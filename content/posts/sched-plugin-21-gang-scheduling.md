---
title: 'scheduler-plugins 21 [in-tree] GangScheduling：整组调度的门禁与计数器'
date: 2026-11-19T11:30:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "gang-scheduling", "podgroup", "workload"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "GangScheduling 是 kube-scheduler 内置的 all-or-nothing 插件。本文从 Pod 的 spec.schedulingGroup 与 PodGroup API 讲起，拆解它在 PreEnqueue 上的 quorum 门禁、HierarchyTracker 计数树、PlacementFeasible 的三种返回值，以及它所依托的 PodGroup 调度周期：整组出队、逐 Pod 试放、在 snapshot 里 assume、失败整体回滚。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `GangScheduling` 实现的扩展点（PreEnqueue、PlacementFeasible）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/21.svg" link="/images/posts/sched-plugins/callstack/21.svg" caption="GangScheduling 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇的 [ImageLocality](/posts/sched-plugin-20-image-locality/) 是默认 Profile 里最后一个打分插件。到这里，一个普通 Pod 的调度周期已经走完：过滤、打分、选出节点，接下来就是 Reserve、Permit 和绑定。

但有一类负载不能这样一个一个地放。一个 8 卡 × 4 机的分布式训练任务，放上去 3 个 worker 和一个都不放没有区别，3 个 worker 会拿着 GPU 空等第 4 个，而第 4 个可能永远等不到资源。这就是 gang scheduling 要解决的问题：要么一组 Pod 同时拿到位置，要么谁都不占。

从这一篇开始的三篇（21~23）讲 kube-scheduler 内置的成组调度。它们不是三个互不相干的插件，而是同一条新调度路径上的三个角色：

- **GangScheduling**（本篇）：决定一组 Pod「什么时候可以开始调度」和「这次尝试算不算成功」；
- **TopologyPlacementGenerator**（[第 22 篇](/posts/sched-plugin-22-topology-placement/)）：把节点按拓扑域切成若干候选 placement，让整组只在一个域里试放；
- **PodGroupPodsCount**（[第 23 篇](/posts/sched-plugin-23-podgroup-pods-count/)）：在多个可行的 placement 之间，偏向能放下更多 Pod 的那个。

本文基于 Kubernetes master 提交 `aabdf5a2`。这部分代码变化很快，和 v1.36 相比，GangScheduling 已经从「Permit 等待」改成了「PlacementFeasible 判定」，后文会专门对比。

## 一、它在哪里，什么时候生效

### 1.1 由 GenericWorkload 一个开关带出来

GangScheduling 不在默认插件列表的字面量里，而是由 feature gate 追加：

```go
// pkg/scheduler/apis/config/v1/default_plugins.go
func applyFeatureGates(config *v1.Plugins) {
	// ...
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		applyGangScheduling(config)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) {
		applyTopologyAwareWorkloadScheduling(config)
	}
}

func applyGangScheduling(config *v1.Plugins) {
	config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.GangScheduling})
}
```

它被追加在 `MultiPoint` 列表的末尾（`DefaultBinder` 之后），没有权重，因为它不打分。相关的几个 feature gate 在 `pkg/features/kube_features.go` 里是这样定义的：

| feature gate | 版本记录 | 作用 |
|---|---|---|
| `GenericWorkload` | 1.35 Alpha，1.37 Beta，默认关闭 | Workload/PodGroup API、Pod 的 `spec.schedulingGroup`、PodGroup 调度周期、GangScheduling 插件 |
| `TopologyAwareWorkloadScheduling` | 1.36 Alpha，1.38 Beta，默认关闭；依赖 `GenericWorkload` | placement 生成与打分（第 22、23 篇） |
| `CompositePodGroup` | 1.37 Alpha，1.38 Beta，默认关闭；依赖前两者 | 多层 PodGroup 树 |
| `PodGroupPreemptionPolicy` | 1.37 Alpha，默认关闭 | PodGroup 级别的 `preemptionPolicy` |
| `WorkloadWithJob` | 1.36 Alpha，默认关闭 | Job 控制器自动创建 Workload 与 PodGroup |

也就是说，在这份代码里，所有 gang 相关的能力默认都是关的。要试用，至少要在 kube-apiserver、kube-scheduler（以及要用 Job 集成时的 kube-controller-manager）上同时打开 `GenericWorkload`，并让 `scheduling.k8s.io/v1beta1` 这组 API 可用。

### 1.2 它挂在哪些扩展点

插件结构体只有两个字段，实现了三个接口：

```go
// pkg/scheduler/framework/plugins/gangscheduling/gangscheduling.go
type GangScheduling struct {
	isCompositePodGroupEnabled bool
	hierarchyTracker           fwk.PodGroupHierarchyTracker
}

var _ fwk.EnqueueExtensions = &GangScheduling{}
var _ fwk.PreEnqueuePlugin = &GangScheduling{}
var _ fwk.PlacementFeasiblePlugin = &GangScheduling{}

// New initializes a new plugin and returns it.
func New(_ context.Context, _ runtime.Object, fh fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &GangScheduling{
		isCompositePodGroupEnabled: fts.EnableCompositePodGroup,
		hierarchyTracker:           fh.SharedPodGroupHierarchyTracker(),
	}, nil
}
```

两个扩展点分别在调度流程的两头：

- **PreEnqueue**：Pod 进入 activeQ 之前。组里的 Pod 没到齐，整组都不许出队；
- **PlacementFeasible**：这是 PodGroup 调度周期专有的扩展点。整组 Pod 逐个试放时，每放完一个就问一次「这组现在算成功、还要继续、还是已经没希望」。

它没有 Args，没有 CycleState，也没有 Permit。它的状态全在 `hierarchyTracker` 里，而这个 tracker 不属于插件本身，是调度器在启动时创建、通过 `Handle` 共享给插件的。

## 二、它管的是什么样的 Pod

### 2.1 Pod、PodGroup、Workload 三层对象

一个 Pod 通过 `spec.schedulingGroup.podGroupName` 声明自己属于哪个 PodGroup：

```go
// staging/src/k8s.io/api/core/v1/types.go
	// WorkloadRef is tombstoned to show why 42 is a reserved protobuf tag.
	// This field got replaced with the SchedulingGroup field in Kubernetes 1.36.
	// WorkloadRef *WorkloadReference `json:"workloadRef,omitempty" protobuf:"bytes,42,opt,name=workloadRef"`

	// schedulingGroup provides a reference to the immediate scheduling runtime
	// grouping object that this Pod belongs to.
	// ...
	// This field is immutable. The scheduler cannot distinguish PodGroup runtime
	// instances that reuse the same namespace and name. Controllers should use a
	// new PodGroup name for each runtime instance. Reusing a name for a PodGroup
	// with different policies may result in Pod placement not conforming to the
	// expected policies.
	//
	// +featureGate=GenericWorkload
	// +optional
	SchedulingGroup *PodSchedulingGroup `json:"schedulingGroup,omitempty" protobuf:"bytes,43,opt,name=schedulingGroup"`
```

这段注释交代了一段演进：1.35 的第一版 API 让 Pod 直接引用 `Workload`（`spec.workloadRef`），1.36 改成了引用运行时实例 `PodGroup`，原来的 protobuf tag 42 作废保留。现在的分工是：

- **Workload**（`scheduling.k8s.io/v1beta1`）：模板，描述一个工作负载由哪些 PodGroup 模板组成，最多 8 个模板，树深最多 4 层；
- **PodGroup**：运行时实例，由 Job、JobSet、LWS 这类控制器从模板创建，带 `schedulingPolicy`；
- **Pod**：通过 `schedulingGroup` 指向 PodGroup。

调度器只关心 PodGroup 和 Pod，Workload 只在校验时用来确认一棵树里的组都来自同一个 Workload（`GetWorkloadName`）。

`schedulingPolicy` 是一个二选一的 union：

```go
// staging/src/k8s.io/api/scheduling/v1beta1/types.go
type PodGroupSchedulingPolicy struct {
	// ...
	Basic *BasicSchedulingPolicy `json:"basic,omitempty" protobuf:"bytes,1,opt,name=basic"`

	// gang specifies that the pods in this group should be scheduled using
	// all-or-nothing semantics. Setting this field at group creation time
	// opts this group to gang scheduling; this field cannot be set or unset afterward.
	// The minCount field within Gang scheduling policy remains mutable after group creation.
	// ...
	Gang *GangSchedulingPolicy `json:"gang,omitempty" protobuf:"bytes,2,opt,name=gang"`
}

// ...

type GangSchedulingPolicy struct {
	// minCount is the minimum number of pods that must be schedulable or scheduled
	// at the same time for the scheduler to admit the entire group.
	// It must be a positive integer. This field is mutable to support workload scaling.
	// ...
	MinCount int32 `json:"minCount" protobuf:"varint,1,opt,name=minCount"`
}
```

注意 `minCount` 的措辞：「schedulable **or scheduled** at the same time」。已经在跑的同组 Pod 也算数，这一点在后面的 PreEnqueue 和 PlacementFeasible 里都能看到。

### 2.2 一个最小的例子

```yaml
apiVersion: scheduling.k8s.io/v1beta1
kind: PodGroup
metadata:
  name: train-0
spec:
  schedulingPolicy:
    gang:
      minCount: 4
---
apiVersion: v1
kind: Pod
metadata:
  name: train-0-worker-0
spec:
  schedulingGroup:
    podGroupName: train-0
  containers:
  - name: trainer
    image: registry.example.com/trainer:v1
    resources:
      requests:
        nvidia.com/gpu: 8
# 另外 3 个 Pod 同理，podGroupName 都是 train-0
```

这组 Pod 会经历三种「被拦下」：

1. 只创建了 3 个 Pod 时，在 PreEnqueue 被拦，`kubectl describe` 能看到 `waiting for podgroup/default/train-0 to meet its quorum`；
2. 4 个都到了但集群里只放得下 3 个，在 PodGroup 调度周期里被 PlacementFeasible 判为 Unschedulable，4 个 Pod 一个都不绑定；
3. 先创建 Pod、后创建 PodGroup，Pod 停在队列的 `incompletePodGroupPods` 里，PreEnqueue 返回 `pod group "train-0" or one of its composite pod group ancestors has not been created yet`。

要理解这三种情况，得先把 PodGroup 调度周期整个看一遍。

## 三、PodGroup 调度周期：GangScheduling 依托的那条路径

{{< figure src="/images/posts/sched-plugins/21-podgroup-cycle.svg" link="/images/posts/sched-plugins/21-podgroup-cycle.svg" caption="PodGroup 调度周期全景：左侧队列侧负责凑齐与放行，右侧 scheduleOnePodGroup 负责整组试放与提交（点击查看大图）" >}}

### 3.1 队列里的实体不再只是 Pod

调度主循环每次从队列里取出的是一个「实体」，可能是 Pod，也可能是 PodGroup：

```go
// pkg/scheduler/schedule_one.go
func (sched *Scheduler) ScheduleOne(ctx context.Context) {
	logger := klog.FromContext(ctx)
	entity, err := sched.NextEntity(logger)
	// ...
	switch specificEntity := entity.(type) {
	case *framework.QueuedPodGroupInfo:
		sched.scheduleOnePodGroup(ctx, specificEntity)
	case *framework.QueuedPodInfo:
		if specificEntity.Pod == nil {
			return
		}
		sched.scheduleOnePod(ctx, specificEntity)
	// ...
	}
}
```

一个 Pod 只要设置了 `schedulingGroup` 且 `GenericWorkload` 打开，就不会作为单个 Pod 入队：

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) isPodGroupMember(pod *v1.Pod) bool {
	if p.isInPlacePodVerticalScalingSchedulerPreemptionEnabled {
		// Deferred resize pods must be queued and evaluated individually rather than as a pod group because they are
		// already running on assigned nodes and undergo atomic individual resource adjustments rather than group gang-scheduling.
		// ...
		if resource.IsPodResizeDeferred(pod) {
			return false
		}
	}
	return p.isGenericWorkloadEnabled && pod.Spec.SchedulingGroup != nil
}
```

`PriorityQueue` 为此多了三个结构，类型注释里写清了它们之间的不变式：

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
// For handling PodGroups, the PriorityQueue is extended with following structures:
//   - incompletePodGroupPods: stores pod infos that wait for their corresponding PodGroup or the parent CompositePodGroup object to be observed.
//   - pendingPodGroupPods: stores all pending pods that wait for their corresponding in-flight pod group to be requeued.
//   - workloadForest: workloadForest maintains a consistent view of observed GenericPodGroup objects (either PodGroup or CompositePodGroup).
//
// These additional structures ensure the following lifecycle invariants for a Pod and its corresponding PodGroup are met:
//  1. Incomplete: A Pod is placed in `incompletePodGroupPods` iff its root PodGroup is NOT present in the `workloadForest`.
//  2. Queued: Otherwise, if the PodGroup is not currently in-flight, the Pod is encapsulated within a QueuedPodGroupInfo,
//     residing in `activeQ`, `backoffQ`, or `unschedulableEntities`.
//  3. In-flight: If the PodGroup is currently in-flight (popped from the queue and being evaluated),
//     newly arriving Pods are placed in `pendingPodGroupPods` to wait for the PodGroup to be requeued.
```

`addPodGroupMember` 把这三条不变式落实到代码里：根组没被观察到就进 `incomplete`；根组已经在某个队列里就并进去；根组刚被 Pop 出去正在调度，就先放 `pending`；否则新建一个 `QueuedPodGroupInfo` 送进 activeQ。

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) addPodGroupMember(logger klog.Logger, pInfo *framework.QueuedPodInfo) {
	rootInfoLookup, hasRoot := p.workloadForest.getRootLookupInfoForPod(logger, pInfo.Pod)
	if !hasRoot {
		// ...
		p.incompletePodGroupPods.add(pInfo)
		// ...
		return
	}

	if added := p.addToPodGroupIfExists(logger, pInfo, rootInfoLookup); added {
		return
	}

	if p.activeQ.isLastPoppedEntity(rootInfoLookup) {
		// ...
		p.pendingPodGroupPods.add(pInfo)
		// ...
		return
	}

	// Create a new group as it's the first member pod in the queue.
	rootInfo := p.newQueuedPodGroupInfoFromRootLookup(logger, pInfo, rootInfoLookup)
	// ...
	if added := p.moveToActiveQ(logger, rootInfo, framework.EventUnscheduledPodAdd.Label(), false, nil); added {
		p.activeQ.broadcast()
	}
	// ...
}
```

`QueuedPodGroupInfo` 以**根组**为单位：没有 CompositePodGroup 时根组就是 PodGroup 自己；有的话，整棵树上所有叶子 PodGroup 的 Pod 合成一个实体出队。排序仍然交给 QueueSort 插件，`PrioritySort.Less` 现在比较的是 `fwk.QueuedEntityInfo`，组的优先级和时间戳与单个 Pod 同等参与排序（见[第 02 篇](/posts/sched-plugin-02-priority-sort/)）。组内 Pod 的顺序由 `PodGroupMemberPodsOrderingFunc` 决定：优先级高的在前，其次尝试次数多的在前，再按时间戳、最后按 Pod 名，保证每轮试放的顺序是确定的。

### 3.2 scheduleOnePodGroup：开场的四步

```go
// pkg/scheduler/schedule_one_podgroup.go
func (sched *Scheduler) scheduleOnePodGroup(ctx context.Context, podGroupInfo *framework.QueuedPodGroupInfo) {
	// ...
	if err := sched.Cache.UpdateSnapshot(logger, sched.nodeInfoSnapshot); err != nil {
		// ...
		sched.handlePodGroupFailureBeforeScheduling(ctx, podGroupInfo, err)
		return
	}

	// PodGroupInfo popped from the queue can have older (Composite)PodGroup objects.
	// Override it here with the snapshotted version to ensure consistency throughout the cycle.
	if err := sched.reconcilePodGroupWithSnapshot(podGroupInfo.PodGroupInfo); err != nil {
		// ...
		sched.handlePodGroupFailureBeforeScheduling(ctx, podGroupInfo, err)
		return
	}
	if err := sched.validatePodGroupHierarchy(podGroupInfo); err != nil {
		sched.handlePodGroupFailureBeforeScheduling(ctx, podGroupInfo, err)
		return
	}

	schedFwk := sched.frameworkForPodGroup(podGroupInfo)
	sched.skipPodGroupPodSchedule(ctx, schedFwk, podGroupInfo)
	// ...
	if !podGroupInfo.HasQueuedPodInfos() {
		// ...
		return
	}
	// ...
	sched.podGroupCycle(ctx, schedFwk, framework.NewCycleState(), podGroupInfo, start)
}
```

`validatePodGroupHierarchy` 值得单独说，因为它会把配置错误直接变成 PodGroup 上的 condition（reason 是 `PodGroupError`）。它检查的是：

- 树深不超过 `WorkloadMaxTreeDepth`（4），且第 4 层不能是 CompositePodGroup；
- 树上所有组引用同一个 Workload；
- gang 父组不能有 basic 子组；`All` 中断模式的父组不能有 `Single` 子组；
- 所有 Pod（包括已经调度的）`schedulerName` 相同；
- 所有 Pod 和子组的**优先级等于根组的优先级**；
- 抢占策略一致。

优先级那一条最容易踩：PodGroup 自己有 `priorityClassName`/`priority` 字段，没设置时 `PodGroupPriority` 返回 0。如果 Pod 设了 `priorityClassName: high`，而 PodGroup 没设，整组会以 `all pods in a pod group hierarchy should have the same priority as the root pod group's priority` 失败，反复重试也不会好，必须把优先级同时写到 PodGroup 上。

### 3.3 核心算法：逐个试放，每步问一次 PlacementFeasible

没有开启拓扑感知时，整组在一个「隐式 placement」里跑默认算法：

```go
// pkg/scheduler/schedule_one_podgroup.go
func (sched *Scheduler) podGroupSchedulingAlgorithm(ctx context.Context, schedFwk framework.Framework, podGroupCycleState *framework.CycleState, podGroupInfo *framework.PodGroupInfo, queuedPodGroupInfo *framework.QueuedPodGroupInfo) (*podGroupAlgorithmResult, revertFns) {
	// ...
	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) {
		// ...
		return sched.podGroupSchedulingPlacementAlgorithm(podGroupCycleCtx, schedFwk, podGroupCycleState, podGroupInfo, queuedPodGroupInfo), nil
	}
	// The non-TAS default algorithm does not evaluate placement candidates, but it
	// still runs in a single implicit placement context so placement-scoped
	// extension points can use the same state plumbing as TAS.
	placementCycleState := framework.NewCycleState()
	placementCycleState.SetPodGroupCycleState(podGroupCycleState)
	return sched.podGroupSchedulingDefaultAlgorithm(podGroupCycleCtx, schedFwk, placementCycleState, podGroupInfo, queuedPodGroupInfo)
}
```

开启拓扑感知时，`podGroupSchedulingPlacementAlgorithm` 会先生成多个 placement，再对每个 placement 调用同一个 `podGroupSchedulingDefaultAlgorithm`，这部分留给第 22 篇。无论哪条路，最终都落到下面这个循环：

```go
// pkg/scheduler/schedule_one_podgroup.go
func (sched *Scheduler) podGroupSchedulingDefaultAlgorithm(ctx context.Context, schedFwk framework.Framework, placementCycleState *framework.CycleState, podGroupInfo *framework.PodGroupInfo, queuedPodGroupInfo *framework.QueuedPodGroupInfo) (result *podGroupAlgorithmResult, revertFns revertFns) {
	defer func() {
		if !result.status.IsSuccess() {
			revertFns.revert()
			result.anyScheduled = false
		}
	}()
	// ...
	podGroupState, err := sched.nodeInfoSnapshot.PodGroupStates().Get(podGroupInfo.GetNamespace(), podGroupInfo.GetName())
	// ...
	// Run PlacementFeasible plugins to check if the pod group can meet its constraints
	// before even attempting to schedule any pods.
	placementProgress := fwk.PlacementProgress{
		Remaining: len(queuedPodInfos),
		Scheduled: podGroupState.ScheduledPodsCount(),
	}
	proceed, placementFeasibleStatus := podGroupPotentiallyFeasible(ctx, schedFwk, placementCycleState, podGroupInfo, placementProgress)
	result.status = placementFeasibleStatus
	if !proceed {
		// ...
		return result, nil
	}

	anyScheduled := false
	for _, podInfo := range queuedPodInfos {
		podResult, revertFn := sched.podGroupPodSchedulingAlgorithm(ctx, schedFwk, placementCycleState, podGroupInfo, podInfo)
		result.podResults = append(result.podResults, podResult)
		if revertFn != nil {
			revertFns.append([]func(){revertFn})
		}
		// ...
		// Check if the pod group can still meet its constraints after scheduling the current pod.
		placementProgress.Remaining--
		if podResult.status.IsSuccess() {
			placementProgress.Scheduled++
		} else {
			placementProgress.Unschedulable++
		}
		proceed, placementFeasibleStatus := podGroupPotentiallyFeasible(ctx, schedFwk, placementCycleState, podGroupInfo, placementProgress)
		result.status = placementFeasibleStatus
		if !proceed {
			// ...
			break
		}
		anyScheduled = anyScheduled || podResult.status.IsSuccess()
	}

	result.anyScheduled = anyScheduled
	return result, revertFns
}
```

这段代码有三处设计值得停下来看：

**第一，`Scheduled` 的初值来自 snapshot 里的 `ScheduledPodsCount()`。** 它数的是这个组里已经 assume 或已经绑定的 Pod。一个 `minCount: 4` 的组如果已经有 3 个 Pod 在跑，第 4 个新来的 Pod 单独就能满足条件，不必等其他人。

**第二，每个 Pod 成功后只在 snapshot 里 assume，不进 cache。** `podGroupPodSchedulingAlgorithm` 里调用的是 `AssumeAndReserveInSnapshot`，注释写得很直白：

```go
// pkg/scheduler/schedule_one_podgroup.go
// Pods of a pod group are assumed into the snapshot rather than the cache: the group's placement
// stays tentative until the whole group is submitted, and a snapshot assume is dropped by the
// next UpdateSnapshot instead of outliving the cycle.
```

后一个 Pod 能「看见」前一个 Pod 占掉的资源，因为它们共享同一个 snapshot；而一旦整组失败，`defer` 里的 `revertFns.revert()` 会对每个已 assume 的 Pod 依次跑 Unreserve 并从 snapshot 里 Forget。即使漏掉了，下一次 `UpdateSnapshot` 也会把这些临时状态冲掉，不会污染 cache。这是「all-or-nothing」在调度周期内的实现方式。

**第三，单个 Pod 失败时不跑 PostFilter。** `schedulingAlgorithm` 里有一个分支专门处理这种情况：

```go
// pkg/scheduler/schedule_one.go
		// We do not want to run PostFilters for single pods in PodGroup cycle.
		// Instead we will run PodGroupPostFilter at the end of PodGroup cycle.
		if state.IsPodGroupSchedulingCycle() {
			// Return nil as NominatingInfo. It can be overridden by the PodGroupPostFilter later
			// and the final decision whether to clear NominatingInfo will be done in submitPodGroupAlgorithmResult.
			return ScheduleResult{nominatingInfo: nil}, fwk.NewStatus(fwk.Unschedulable).WithError(err)
		}
```

为一个 Pod 抢占出位置没有意义，组里其他 Pod 可能还放不下。抢占要在整组层面做，由 `PodGroupPostFilter` 扩展点负责，默认实现是 `DefaultPreemption.PodGroupPostFilter`（详见[第 18 篇](/posts/sched-plugin-18-default-preemption/)）。

## 四、PlacementFeasible：三种返回值

### 4.1 代码只有十几行

```go
// pkg/scheduler/framework/plugins/gangscheduling/gangscheduling.go
func (pl *GangScheduling) PlacementFeasible(ctx context.Context, placementCycleState fwk.PlacementCycleState, podGroupInfo fwk.PodGroupInfo, args fwk.PlacementProgress) *fwk.Status {
	minCount := podGroupInfo.GetMinCount()
	remaining := args.Remaining
	scheduled := args.Scheduled
	unschedulable := args.Unschedulable

	if remaining+scheduled < minCount {
		// minCount can't be satisfied because there are not enough remaining pods.
		return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("scheduling policy's minimum threshold (%d) cannot be satisfied: %d scheduled, %d unschedulable, %d not-evaluated", minCount, scheduled, unschedulable, remaining))
	}

	if scheduled < minCount {
		// minCount might be satisfied once more remaining pods are evaluated.
		return fwk.NewStatus(fwk.Wait, fmt.Sprintf("scheduling policy's minimum threshold (%d) is not yet satisfied: %d scheduled, %d unschedulable, %d not-evaluated", minCount, scheduled, unschedulable, remaining))
	}

	// minCount is satisfied.
	return nil
}
```

`GetMinCount()` 对不同策略的取值是：

```go
// staging/src/k8s.io/kube-scheduler/framework/types.go
func (gpg *GenericPodGroup) GetMinCount() int {
	if cpg := gpg.CompositePodGroup; cpg != nil {
		if cpg.Spec.SchedulingPolicy.Gang != nil {
			return int(cpg.Spec.SchedulingPolicy.Gang.MinGroupCount)
		}
		return 1
	}
	if gpg.PodGroup.Spec.SchedulingPolicy.Gang != nil {
		return int(gpg.PodGroup.Spec.SchedulingPolicy.Gang.MinCount)
	}
	return 1
}
```

Basic 策略的 minCount 是 1。这意味着 GangScheduling 对 basic 组同样生效，只是条件放宽成「这一轮至少放上一个」。如果 basic 组里的 Pod 一个都放不下，整组也是 Unschedulable，会走整组抢占的路径。

### 4.2 框架怎么解读这三种状态

插件返回 `Wait` 并不意味着最终结果是「等待」。`podGroupPotentiallyFeasible` 同时返回「要不要继续」和「如果现在结束，结果是什么」两个值：

```go
// pkg/scheduler/schedule_one_podgroup.go
func podGroupPotentiallyFeasible(ctx context.Context, schedFwk framework.Framework, placementCycleState *framework.CycleState, podGroupInfo *framework.PodGroupInfo, placementProgress fwk.PlacementProgress) (bool, *fwk.Status) {
	status := schedFwk.RunPlacementFeasiblePlugins(ctx, placementCycleState, podGroupInfo, placementProgress)
	switch status.Code() {
	case fwk.Error:
		// ...
		return false, fwk.AsStatus(fmt.Errorf("failed to evaluate placement feasibility: %w", status.AsError()))
	case fwk.Unschedulable:
		// ...
		fitError := newPodGroupFitError(status)
		return false, fwk.NewStatus(fwk.Unschedulable).WithError(fitError)
	case fwk.Wait:
		// Wait status indicates the scheduling should proceed,
		// while at the same time, if it's a terminal status, the outcome should be Unschedulable.
		fitError := newPodGroupFitError(status)
		return true, fwk.NewStatus(fwk.Unschedulable).WithError(fitError)
	default:
		return true, status
	}
}
```

| 插件返回 | 是否继续评估后面的 Pod | 如果这是最后一步，组的结果 |
|---|---|---|
| `Unschedulable` | 否，立刻停 | Unschedulable |
| `Wait` | 是 | Unschedulable |
| `Success`（nil） | 是 | Success |

`Success` 之后仍然继续评估剩余的 Pod，因为 minCount 只是下限，能多放就多放；接口注释也要求插件一旦返回 Success，之后都应继续返回 Success。`RunPlacementFeasiblePlugins` 在多个插件之间的合并规则是：遇到 Unschedulable 或 Error 立即返回，遇到 Wait 记下并继续问下一个插件，全部 Success 才是 Success。框架允许 PlacementFeasible 返回的只有这四种码，`UnschedulableAndUnresolvable` 会被当成「unexpected status code」变成 Error。

### 4.3 一个算例

{{< figure src="/images/posts/sched-plugins/21-feasible-trace.svg" link="/images/posts/sched-plugins/21-feasible-trace.svg" caption="minCount=4、队列里 6 个 Pod 时 PlacementProgress 的变化：场景 A 在第 5 个 Pod 处转为 Success，场景 B 在第 4 个 Pod 处提前判定不可调度（点击查看大图）" >}}

场景 B 里，第 4 个 Pod 失败后 `Remaining + Scheduled = 2 + 1 = 3 < 4`，剩下两个 Pod 就算都成功也不够，于是不再评估 p5、p6。这个提前退出在大组上很有用：一个有 1000 个 Pod、`minCount: 900` 的组，只要失败数超过 100，剩下的 Pod 就不必再逐个跑 Filter/Score。

没被评估的 p5、p6 怎么办？`completePodGroupAlgorithmResult` 会给它们补上组的状态，保证每个 Pod 都有一个 `algorithmResult`，后续 FailureHandler 要靠这些状态决定每个 Pod 的 `UnschedulablePlugins`。

### 4.4 「至少放上一个」这条框架级约束

PlacementFeasible 之外，`runRootSchedulingAlgorithm` 还加了一道检查：

```go
// pkg/scheduler/schedule_one_podgroup.go
	if result.status.IsSuccess() && !result.anyScheduled {
		// The framework requires at least 1 pod to be scheduled in order to return a success status.
		fitError := newPodGroupFitError(fwk.NewStatus(fwk.Unschedulable, "no pods were schedulable"))
		result.status = fwk.NewStatus(fwk.Unschedulable).WithError(fitError)
	}
```

考虑 `minCount: 3`、已经有 3 个 Pod 在跑、又扩容了 2 个新 Pod 的组：PlacementFeasible 一开始就返回 Success（Scheduled=3），但两个新 Pod 都放不下。如果没有这道检查，组的结果会是 Success，两个 Pod 却一个都没动。改成 Unschedulable 之后，它们会进入整组抢占的流程。

## 五、PreEnqueue：凑齐之前不许出队

### 5.1 HierarchyTracker 是谁、在哪更新

PlacementFeasible 解决的是「放不放得下」，PreEnqueue 解决的是「人到齐了没有」。后者用的数据结构是 `HierarchyTracker`，它和插件放在同一个包里，但由调度器在 `framework_init.go` 里创建：

```go
// pkg/scheduler/framework_init.go
	var podGroupHierarchyTracker *gangscheduling.HierarchyTracker
	if feature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		podGroupHierarchyTracker = gangscheduling.NewHierarchyTracker(feature.DefaultFeatureGate.Enabled(features.CompositePodGroup))
	}
```

调度器的 informer 事件处理函数直接调用它，未调度 Pod、已调度 Pod、PodGroup、CompositePodGroup 的增删改都会更新计数。以未调度 Pod 的 Add 为例：

```go
// pkg/scheduler/eventhandlers.go
func (sched *Scheduler) addPodToSchedulingQueue(pod *v1.Pod) {
	// ...
	sched.Cache.AddPodGroupMember(pod)
	if sched.podGroupHierarchyTracker != nil {
		sched.podGroupHierarchyTracker.AddPod(logger, pod)
	}
	sched.SchedulingQueue.Add(klog.NewContext(context.Background(), logger), pod)
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventUnscheduledPodAdd, nil, pod, nil)
	}
}
```

顺序很关键：先更新 tracker，再把 Pod 交给队列。队列在 `moveToActiveQ` 里跑 PreEnqueue 时，看到的计数已经包含这个新 Pod。已调度 Pod 的 Add（`addAssignedPodToCache`）同样会 `AddPod`，所以 tracker 里数的是这个组「所有已知的 Pod」，不区分是否已经绑定。

### 5.2 计数树的结构

```go
// pkg/scheduler/framework/plugins/gangscheduling/hierarchy_tracker.go
type hierarchyGroup struct {
	key           fwk.EntityKey
	genericGroup  *fwk.GenericPodGroup
	parentKey     *fwk.EntityKey
	minCount      int
	allPods       sets.Set[types.UID]
	readyChildren int
}

// ...

// readyCount returns the number of pods for a PodGroup or ready child groups for a CompositePodGroup.
func (g *hierarchyGroup) readyCount() int {
	if g.key.Type == fwk.PodGroupKeyType {
		return len(g.allPods)
	}
	return g.readyChildren
}

// ...
type HierarchyTracker struct {
	lock                     sync.RWMutex
	compositePodGroupEnabled bool
	groups                   map[fwk.EntityKey]*hierarchyGroup
}
```

叶子 PodGroup 用一个 UID 集合数 Pod，CompositePodGroup 用一个整数数「已就绪的子组」。一个组就绪的条件统一成 `readyCount() >= minCount`。用 UID 集合而不是计数器，是为了让重复的 Add 事件幂等（`addPod` 开头先 `Has` 检查）。

CompositePodGroup 是更高一层的组：比如一个训练任务由 `worker-a`、`worker-b`、`ps` 三个 PodGroup 组成，外面套一个 CompositePodGroup，用 `minGroupCount` 表示至少几个子组同时就绪。不开 `CompositePodGroup` 时 `parentKey` 一律被忽略，每个 PodGroup 自己就是根。

{{< figure src="/images/posts/sched-plugins/21-hierarchy-tracker.svg" link="/images/posts/sched-plugins/21-hierarchy-tracker.svg" caption="HierarchyTracker 的计数树：子组跨过 minCount 阈值时向父组传播 +1，根组达到 minGroupCount 后整棵树放行（点击查看大图）" >}}

### 5.3 只在跨越阈值时传播

```go
// pkg/scheduler/framework/plugins/gangscheduling/hierarchy_tracker.go
func (ht *HierarchyTracker) addPod(logger klog.Logger, pod *v1.Pod) {
	key := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	group := ht.getOrCreateGroup(key)
	if group.allPods.Has(pod.UID) {
		return
	}
	wasReady := ht.isGroupReady(group)
	group.allPods.Insert(pod.UID)
	if !wasReady && ht.isGroupReady(group) {
		ht.propagateReadinessDelta(logger, group.parentKey, 1)
	}
}

func (ht *HierarchyTracker) propagateReadinessDelta(logger klog.Logger, parentKey *fwk.EntityKey, delta int) {
	var visited []fwk.EntityKey
	for parentKey != nil && delta != 0 {
		// ... cycle detection ...
		parent := ht.getOrCreateGroup(*parentKey)
		wasReady := ht.isGroupReady(parent)
		parent.readyChildren += delta
		isNowReady := ht.isGroupReady(parent)

		nextKey, nextDelta := parent.parentKey, 0
		if wasReady != isNowReady {
			nextDelta = -1
			if isNowReady {
				nextDelta = 1
			}
		}
		if delta < 0 && parent.empty() {
			delete(ht.groups, *parentKey)
		}

		parentKey, delta = nextKey, nextDelta
	}
}
```

一个组从 3 个 Pod 涨到 4 个，跨过了 minCount=4，向父组传 +1；涨到 5 个时 `wasReady` 已经是 true，什么都不传。父组 `readyChildren` 的变化也只在它自己跨过阈值时才继续往上走。这样每次 Pod 事件的代价是 O(树深)，而树深被 API 限制在 4 以内。

`getOrCreateGroup` 有一个容易忽略的默认值：

```go
// pkg/scheduler/framework/plugins/gangscheduling/hierarchy_tracker.go
// We default minCount to 1 so that out-of-order informer events (e.g. a Pod arriving before its PodGroup)
// still enforce a baseline quorum until the authoritative policy definition arrives.
func (ht *HierarchyTracker) getOrCreateGroup(key fwk.EntityKey) *hierarchyGroup {
	group, exists := ht.groups[key]
	if !exists {
		group = &hierarchyGroup{
			key:      key,
			minCount: 1,
			allPods:  sets.New[types.UID](),
		}
		ht.groups[key] = group
	}
	return group
}
```

Pod 先于 PodGroup 到达时，tracker 先建一个占位节点（`genericGroup == nil`）。PodGroup 到达后 `AddGenericPodGroup` 写入真实的 minCount 和 parentKey，`updateGroupReadiness` 比较前后的就绪状态并补发传播；如果父链从 nil 变成了某个 CompositePodGroup，还会先从旧父撤销、再向新父传播。PodGroup 被删除时，只要还有 Pod 引用它，节点作为占位保留下来。

### 5.4 PreEnqueue 本身

```go
// pkg/scheduler/framework/plugins/gangscheduling/gangscheduling.go
func (pl *GangScheduling) PreEnqueue(ctx context.Context, pod *v1.Pod) *fwk.Status {
	if pod.Spec.SchedulingGroup == nil {
		return nil
	}

	pgKey := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	rootReadiness, err := pl.hierarchyTracker.FindRootGroupReadiness(pgKey)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if rootReadiness == nil || rootReadiness.RootGroup == nil {
		// The pod is unschedulable until its pod group and all parent groups in its hierarchy are created.
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("pod group %q or one of its composite pod group ancestors has not been created yet", *pod.Spec.SchedulingGroup.PodGroupName))
	}

	if rootReadiness.IsReady {
		return nil
	}

	rootGroup := rootReadiness.RootGroup
	if rootGroup.IsBasic() {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("waiting for %s tree to have at least one ready child group", rootGroup.GetKey()))
	}
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("waiting for %s to meet its quorum", rootGroup.GetKey()))
}
```

几个要点：

- 判断的是**根组**是否就绪，而不是 Pod 自己所在的叶子组。一个 CompositePodGroup 下有 3 个子组、`minGroupCount: 2`，只要两个子组凑齐，第三个子组的 Pod 也一起放行，它们会在 PlacementFeasible 那一关再被检验；
- `findRootGroup` 沿父链往上找，链上任何一个组对象缺失（`genericGroup == nil`）都返回 nil，于是得到「has not been created yet」；
- 所有失败都是 `UnschedulableAndUnresolvable`。对 PreEnqueue 来说这只意味着「不进 activeQ、等事件」，不会触发抢占，和 Filter 阶段的语义略有不同；
- `rootGroup.GetKey()` 是 `EntityKey`，它的 `String()` 格式是 `类型/命名空间/名字`，所以事件里看到的是 `podgroup/default/train-0` 这样的字符串。

队列侧还有一条规则让 PreEnqueue 的效果放大到整组：

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) runPreEnqueuePlugins(ctx context.Context, entity framework.QueuedEntityInfo) {
	// Run PreEnqueue plugins for each pod until the first gets gated.
	// ...
	for pInfo := range entity.ForEachPodInfo() {
		p.runPreEnqueuePluginsForPod(ctx, pInfo)
		if pInfo.Gated() {
			// If any pod is gated, the whole entity is gated.
			// ...
			entity.SetGatingPlugin(pInfo.QueueingParams.GatingPlugin, pInfo.QueueingParams.GatingPluginEvents)
			return
		}
	}
	// If no pod is gated, clear the gating plugin info from the entity.
	entity.SetGatingPlugin("", nil)
}
```

任何一个 Pod 被拦，整个实体都不出队。这条规则对 [SchedulingGates](/posts/sched-plugin-01-scheduling-gates/) 也成立：组里只要有一个 Pod 带着 scheduling gate，整组都停在 gated 状态。

## 六、EventsToRegister：谁能把被拦下的组叫回来

```go
// pkg/scheduler/framework/plugins/gangscheduling/gangscheduling.go
func (pl *GangScheduling) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	events := []fwk.ClusterEventWithHint{
		// A new pod (either unscheduled or pre-bound) being added might be the one that completes a gang, meeting its MinCount requirement.
		// PodSchedulingGroup field is immutable, so there is no need to subscribe on Pod/Update event.
		{Event: fwk.ClusterEvent{Resource: fwk.UnscheduledPod, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterPodAdded},
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterPodAdded},
		// A PodGroup being added can be making a waiting gang schedulable.
		{Event: fwk.ClusterEvent{Resource: fwk.PodGroup, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterPodGroupAdded},
		// A PodGroup update to MinCount may make it schedulable
		{Event: fwk.ClusterEvent{Resource: fwk.PodGroup, ActionType: fwk.Update}, QueueingHintFn: pl.isSchedulableAfterPodGroupUpdated},
	}

	if pl.isCompositePodGroupEnabled {
		// ... CompositePodGroup Add / Update ...
	}

	return events, nil
}
```

四个 hint 函数的判断逻辑可以归成一张表：

| 事件 | 返回 `Queue` 的条件 | 其他情况 |
|---|---|---|
| 未调度 Pod Add / 已调度 Pod Add | 两个 Pod 都有 `schedulingGroup`，且新 Pod 的组与等待 Pod 的组在同一棵树（同一个根） | `QueueSkip` |
| PodGroup Add | 新 PodGroup 与等待 Pod 的组在同一棵树 | `QueueSkip` |
| PodGroup Update | 新旧都是 gang、`minCount` **变小**、且在同一棵树 | 变大或不变、或策略不是 gang：`QueueSkip` |
| CompositePodGroup Add / Update | 同上，比较的是 `minGroupCount` | `QueueSkip` |

「同一棵树」由 `areSameHierarchy` 判断：不开 CompositePodGroup 时就是 key 相等；开了以后用 tracker 分别找两边的根组，根相同即可。这让一个兄弟子组的 Pod 到达也能唤醒整棵树。

为什么没有订阅 Pod/Delete 和 PodGroup/Delete？删除只会让组离就绪更远，不可能让一个等待中的组变得可调度。Pod/Update 不订阅的理由注释里写了：`schedulingGroup` 不可变。

这些 hint 会在两种情况下被用到：一是组在 PreEnqueue 被拦，`GangScheduling` 记为 gating plugin；二是 PlacementFeasible 返回 Wait/Unschedulable，`podGroupFitError.addPluginStatus` 把 `GangScheduling` 记进 `unschedulablePlugins`，回队时复制到每个 Pod 的 `UnschedulablePlugins`。队列评估一个组时会对组内每个 Pod 跑 hint，只要有一个 Pod 得到 `Queue`，整组就被移出 unschedulableEntities：

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
// isEntityWorthRequeuing calls isPodWorthRequeuing for all pods belonging to the entity and returns the highest queueing strategy.
func (p *PriorityQueue) isEntityWorthRequeuing(logger klog.Logger, entity framework.QueuedEntityInfo, event fwk.ClusterEvent, oldObj, newObj interface{}, hintKeys *preQueueingHintPodKeys) queueingStrategy {
	// For pod groups, if any pod is worth requeuing, the whole group is worth it.
	// ...
}
```

这里有一个和单 Pod 调度不同的地方：组内各 Pod 的 `UnschedulablePlugins` 并不相同。在场景 A 里，p2、p6 是自己 Filter 失败，带的是 `NodeResourcesFit` 之类的插件；p1/p3/p4/p5 成功了不回队。在场景 B 里，p2~p4 带的是各自的 Filter 插件，p1、p5、p6 带的是组的 `podGroupFitError`，里面有 `GangScheduling`。于是节点变化（唤醒 p2~p4）和新 Pod 加入（唤醒 p1、p5、p6）都能让这个组重新出队。

## 七、提交结果与回队

### 7.1 submitPodGroupResult

```go
// pkg/scheduler/schedule_one_podgroup.go
		if podResult.status.IsSuccess() {
			switch {
			case podGroupResult.status.IsSuccess():
				// Disable pod group scheduling in cycle state before binding.
				podCtx.state.SetPodGroupCycleState(nil)
				podCtx.state.SetPlacementCycleState(nil)
				// Schedule result is applied for pod and its binding cycle executes.
				assumedPodInfo, status := sched.prepareForBindingCycle(ctx, podCtx.state, schedFwk, pInfo, podCtx.podsToActivate, podResult.scheduleResult)
				// ...
				go sched.runBindingCycle(ctx, podCtx.state, schedFwk, podResult.scheduleResult, assumedPodInfo, podSchedulingStart, podCtx.podsToActivate)
				scheduledPods++
			case podGroupResult.status.IsRejected():
				if podGroupResult.waitingOnPreemption {
					// Pod has to come back to the scheduling queue as unschedulable, waiting for preemption to complete.
					sched.FailureHandler(ctx, schedFwk, pInfo, podGroupResult.status.Clone(), podResult.scheduleResult.nominatingInfo, podSchedulingStart)
				} else {
					// ...
					sched.FailureHandler(ctx, schedFwk, pInfo, podGroupResult.status.Clone(), clearNominatedNode, podSchedulingStart)
				}
				unschedulablePods++
			// ...
			}
		} else {
			// ...
			sched.FailureHandler(ctx, schedFwk, pInfo, podResult.status, podResult.scheduleResult.nominatingInfo, podSchedulingStart)
			unschedulablePods++
		}
```

组成功时，每个成功的 Pod 才真正 `AssumeAndReserveInCache`，跑 Permit，然后启动各自的绑定 goroutine。组失败时，即使某个 Pod 自己试放成功，也带着组的状态回队。

最后，调度器把结果写到 PodGroup 的 `status.conditions` 上，类型是 `PodGroupInitiallyScheduled`，reason 有 `Scheduled`、`Unschedulable`、`SchedulerError`、`PodGroupError` 四种。`updatePodGroupCondition` 有一条保护：condition 一旦是 True，后续轮次不会把它改回 False，避免扩容时新 Pod 的失败「污染」一个已经成功启动的组。

### 7.2 AddAttemptedPodGroupIfNeeded

回队分两步。`FailureHandler` 把每个失败的 Pod 交给队列，此时根组仍是「最近一次 Pop 出来的实体」，于是这些 Pod 被放进 `pendingPodGroupPods`。周期末尾 `AddAttemptedPodGroupIfNeeded` 把它们重新组装成一个 `QueuedPodGroupInfo`：

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
	if status.IsSuccess() {
		// If the scheduling attempt of the pod group succeeded, requeue remaining pods directly to active queue.
		requeueWithoutBackoff = true
		// If there are remaining unschedulable or error pods, preserve timestamp, so that subsequent
		// scheduling attempt triggers preemption immediately for the podgroup.
		preserveTimestamp = hadUnschedulableOrErrorPods
		// ...
	} else if status.IsRejected() {
		// This Pod group is rejected by some plugins, not coming back due to unexpected errors (e.g., a network issue).
		pgInfo.UnschedulableCount++
		// ...
	} else {
		// ...
		pgInfo.ConsecutiveErrorsCount++
		// ...
	}
```

组成功但还有剩余 Pod 时（场景 A 的 p2、p6），组直接回 activeQ，不走 backoff，并保留原时间戳排在前面，让下一轮立刻为这些 Pod 尝试抢占。组失败时 `UnschedulableCount` 加一，按 backoff 规则进 backoffQ 或 unschedulableEntities，等 hint 唤醒。

## 八、边界情况与容易误解的地方

**all-or-nothing 只保证到「调度决定」，不保证到「绑定」。** 组成功之后，每个 Pod 的绑定周期是独立的 goroutine。某个 Pod 的 PreBind（比如卷绑定）失败了，它自己回队重试，已经绑上的兄弟 Pod 不会被撤回。调度器保证的是「同一时刻做出的决定满足 minCount」，后续的执行失败需要控制器或下一轮调度来补救。

**minCount 是会变的。** `minCount` 可变，为的是支持扩缩容。调大 minCount 不会触发任何重新入队（hint 返回 QueueSkip），已经运行的 Pod 也不受影响；调小会唤醒同一棵树里等待的组。API 注释也写明了：一轮调度进行中修改 minCount，可能要到下一轮才生效。

**同名 PodGroup 的复用。** `schedulingGroup` 的注释特意警告：调度器无法区分同名同命名空间的两个 PodGroup 运行时实例。HierarchyTracker 的 key 是 `(类型, 命名空间, 名字)`，删掉一个 PodGroup 再用同名重建，旧 Pod 和新 Pod 会被算进同一个组。控制器应该为每个运行时实例生成新名字。

**PreEnqueue 插件的执行顺序不固定。** `preEnqueuePluginMap` 是 map，除了上一次的 gating plugin 会被优先执行，其余插件按 map 迭代顺序执行。`SchedulingGates` 和 `GangScheduling` 谁先拦下 Pod 是不确定的，排查时看到的 gating plugin 可能在两者之间变化。

**Deferred resize 的 Pod 不走组调度。** 开启 `InPlacePodVerticalScalingSchedulerPreemption` 时，已经在节点上、只是 resize 被推迟的 Pod 会被 `isPodGroupMember` 排除，作为单个 Pod 处理（第 17 篇）。

**validatePodGroupHierarchy 看的是所有 Pod。** 包括已经调度的 Pod。一个组跑到一半，有人改了某些 Pod 的优先级类，后续扩容出来的 Pod 也会因为整组校验失败而进不去。

## 九、发散

### 9.1 v1.36 的 GangScheduling 是怎么做的

在 v1.36.4 的源码里，同一个插件长这样：

```go
// pkg/scheduler/framework/plugins/gangscheduling/gangscheduling.go (Kubernetes v1.36.4)
const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = names.GangScheduling
	// permitTimeoutDuration defines how long the gang pods should
	// wait at the permit stage for a quorum before being rejected.
	permitTimeoutDuration = 5 * time.Minute
)
// ...
var _ fwk.EnqueueExtensions = &GangScheduling{}
var _ fwk.PreEnqueuePlugin = &GangScheduling{}
var _ fwk.PermitPlugin = &GangScheduling{}
```

那时它由独立的 `GangScheduling` feature gate（1.35 Alpha）控制。PreEnqueue 直接用 PodGroup lister 和 `PodGroupStates().AllPodsCount()` 判断人数；Permit 则统计 `ScheduledPodsCount()`，不够 minCount 就返回 `Wait` 并带 5 分钟超时，够了就遍历 `AssumedPods()` 逐个 `Allow`。这和 scheduler-plugins 里 Coscheduling 的思路一致：一个个 Pod 先占住资源，在 Permit 处等齐再一起放行。

master 上的版本把这套机制拆掉了：

- 不再有 Permit，也就没有 5 分钟超时。等待发生在**调度周期之内**（逐个试放、snapshot 里 assume），而不是绑定周期之前（cache 里 assume、占着资源等）；
- 等待的 Pod 不再占用 cache 里的资源，也就不会出现「两个组各占一半资源、互相等到超时」的死锁；
- 原来分散在每个 Pod 的 Permit 判断，集中到了 PodGroup 调度周期的 `PlacementFeasible` 一个扩展点；
- 独立的 `GangScheduling` gate 被去掉，插件跟随 `GenericWorkload` 启用；
- PreEnqueue 的计数改成增量维护的 HierarchyTracker，以支持 CompositePodGroup 的多层 quorum。

out-of-tree 的 Coscheduling 至今仍是 Permit 方案，第 27 篇会把两者放在一起对照。

### 9.2 一个组的调度代价

PodGroup 调度周期是串行的：主循环拿到一个组之后，要把组里每个 Pod 的 Filter、Score 都跑完才会处理下一个实体。一个 512 Pod 的组，相当于在一个调度周期里做了 512 次单 Pod 调度，期间其他 Pod 都在排队。开启拓扑感知后还要乘以被评估的 placement 数（第 22 篇讲它的上限是怎么算的）。代码里也留了 TODO（#142269），提到组状态的 API 更新目前是同步串行的，大层级结构下会阻塞队列。

可观测性方面，调度器为组调度单独加了一批指标：`scheduler_podgroup_schedule_attempts_total`（按 scheduled/unschedulable/error 等结果计数）、`scheduler_podgroup_scheduling_algorithm_duration_seconds`、`scheduler_podgroup_scheduling_attempt_duration_seconds`，以及整组抢占的 `scheduler_workload_preemption_attempts_total`。上线 gang 调度后，前两个指标的 P99 是判断「组太大、拖慢全局」的直接依据。

### 9.3 谁来创建 PodGroup

调度器只消费 PodGroup，不创建它。`WorkloadWithJob` feature gate 让 Job 控制器为符合条件的 Job 自动创建 Workload 和 PodGroup（`pkg/controller/job/job_scheduling_manager.go`），并在 Job 规格变化时同步 gang 的 `minCount`。PodGroup 的 API 注释也写明，它由 Job、LWS、JobSet 这类工作负载控制器从 `Workload.podGroupTemplates` 创建。对使用者来说，理想状态是只写一个 Job，成组语义由控制器翻译成 PodGroup，调度器只负责执行。

## 十、小结

- GangScheduling 只在 `GenericWorkload` 打开时启用，挂在 PreEnqueue 和 PlacementFeasible 两个扩展点，没有参数、没有 CycleState，状态在调度器共享的 `HierarchyTracker` 里。
- PreEnqueue 看的是根组是否凑够人：叶子 PodGroup 数 Pod（含已调度的），CompositePodGroup 数就绪子组，只在跨越阈值时向上传播；失败一律 `UnschedulableAndUnresolvable`，且一个 Pod 被拦整组被拦。
- PlacementFeasible 在 PodGroup 调度周期里每试放一个 Pod 调一次：`Remaining + Scheduled < minCount` 立即 Unschedulable，`Scheduled < minCount` 返回 Wait 继续，否则 Success；Basic 策略的 minCount 视为 1。
- 整组的 all-or-nothing 靠「snapshot 里 assume、失败统一 revert」实现，单个 Pod 不跑 PostFilter，抢占交给 `PodGroupPostFilter`；绑定仍然是每个 Pod 独立进行。
- 相比 v1.36 的 Permit + 5 分钟超时，master 把等待挪进了调度周期内部，不再提前占用资源。

开启 `TopologyAwareWorkloadScheduling` 后，上面第 ③ 步之前会多出一个「生成候选 placement」的环节，整组只在一个拓扑域里试放。下一篇讲负责生成 placement 的 [TopologyPlacementGenerator](/posts/sched-plugin-22-topology-placement/)。
