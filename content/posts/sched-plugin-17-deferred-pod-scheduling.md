---
title: 'scheduler-plugins [in-tree] DeferredPodScheduling：已经在跑的 Pod 为什么又回到调度队列'
date: 2026-11-19T10:50:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "DeferredPodScheduling", "原地扩缩容", "抢占"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "原地扩容放不下时会被 kubelet 标记为 Deferred，新 gate 让这类已绑定的 Pod 回到调度器，借用抢占腾出资源。本文拆解 DeferredPodScheduling 的 Filter、Permit、QueueingHint 与插件顺序，以及主循环的配套改动。"
---

[上一篇](/posts/sched-plugin-16-node-declared-features/)的 `NodeDeclaredFeatures` 在 PreFilter 和 Filter 开头都有一行看起来与它无关的判断：`resource.IsPodResizeDeferred(pod)` 为真时直接跳过。往前翻，`PodTopologySpread`、`InterPodAffinity`、`NodeAffinity`、`NodePorts`、`VolumeBinding`……几乎每个 Filter 插件都有同样的一行。这些判断服务于同一个功能：让一个**已经绑定、正在运行**的 Pod 重新走一遍调度周期。`DeferredPodScheduling` 是这个功能在插件层面的主体。

本文基于 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期）。插件由 feature gate `InPlacePodVerticalScalingSchedulerPreemption` 控制，对应 [KEP-5836](https://kep.k8s.io/5836)，1.37 新增，Alpha，默认关闭，依赖 `InPlacePodVerticalScaling`（1.35 已 GA）。gate 开启时，`applyDeferredPodScheduling` 把插件插入默认 MultiPoint 列表中 `NodeResourcesFit` 的**前面**。它实现了 PreFilter、Filter、Permit 和 `EnqueueExtensions`，没有参数、没有 Score，也没有实现 `SignPlugin`。

## 一、问题：原地扩容放不下怎么办

原地扩缩容允许直接修改运行中 Pod 的 `resources`，不用重建 Pod。kubelet 收到修改后先做准入：节点剩余资源够，就分配并让容器运行时调整 cgroup；不够，就把这次 resize 标记为**推迟**：

```go
// staging/src/k8s.io/api/core/v1/types.go
	// PodReasonDeferred reason in PodResizePending pod condition indicates the proposed resize is feasible in
	// theory (it fits on this node) but is not possible right now.
	PodReasonDeferred = "Deferred"

	// PodReasonInfeasible reason in PodResizePending pod condition indicates the proposed resize is not
	// feasible and is rejected; it may not be re-evaluated
	PodReasonInfeasible = "Infeasible"
```

`Infeasible` 是节点总容量都放不下，再等也没用；`Deferred` 是「理论上放得下，但现在被其它 Pod 占着」。被推迟的 resize 会留在 kubelet 的 pending 列表里，等节点上有 Pod 退出、资源被释放时重试。

问题在于：资源可能永远不会被释放。节点上的其它 Pod 都在正常运行，没有理由退出。一个高优先级的数据库 Pod 想把内存从 8Gi 扩到 16Gi，节点上挤满了低优先级的批处理任务，它就会一直卡在 `Deferred`。

kubelet 本身有一个抢占机制，但只服务于关键 Pod（critical pod）的**准入**，而且只看本节点。KEP-5836 的选择是让调度器来做这件事：调度器本来就有完整的抢占实现（优先级比较、PDB、最少驱逐），把 Deferred 的 Pod 当作「一个只能放在当前节点上、需要额外资源的 Pod」送进调度周期，资源不足时自然会触发 `DefaultPreemption`。gate 开启后，kubelet 侧明确放弃了 resize 场景下的抢占：

```go
// pkg/kubelet/preemption/preemption.go
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) {
		if operation == lifecycle.ResizeOperation {
			// For in-place pod resizes, the scheduler owns all preemption decisions.
			// When a resize cannot be accommodated immediately on the node, Kubelet defers the
			// resize request and relies on the scheduler to handle preemption asynchronously.
			return failureReasons, nil
		}
	}
```

判断一个 Pod 是否处于这种状态，全系统用的都是同一个辅助函数：

```go
// staging/src/k8s.io/component-helpers/resource/helpers.go
func IsPodResizeDeferred(pod *v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodResizePending {
			return condition.Reason == v1.PodReasonDeferred
		}
	}
	return false
}
```

## 二、调度器主循环的配套改动

在看插件之前，先看已绑定的 Pod 是怎么回到调度队列的。正常情况下调度器只把**未绑定**的 Pod 放进队列，已绑定的 Pod 只进缓存。gate 开启后，事件处理器多了几条分支：

```go
// pkg/scheduler/eventhandlers.go
	if assignedPod(oldPod) {
		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) && responsibleForPod(newPod, sched.Profiles) {
			oldDeferred := resource.IsPodResizeDeferred(oldPod)
			newDeferred := resource.IsPodResizeDeferred(newPod)

			if !oldDeferred && newDeferred {
				// When an assigned pod transitions to a deferred resize,
				// we must add it to the scheduling queue so the scheduler can evaluate feasibility and run preemption on the assigned node.
				logger.V(3).Info("Assigned pod transitioned to deferred resize, adding to scheduling queue", "pod", klog.KObj(newPod))
				sched.addPodToSchedulingQueue(newPod)
			} else if oldDeferred && !newDeferred {
				// When an assigned pod is no longer deferred,
				// remove it from the scheduling queue so the scheduler stops evaluating preemption for it.
				logger.V(3).Info("Assigned pod is no longer deferred, removing from scheduling queue", "pod", klog.KObj(newPod))
				sched.deletePodFromSchedulingQueue(newPod, false)
			} else if oldDeferred && newDeferred {
				// If the pod's spec mutates while queued for a deferred resize (e.g., resource requests updated), update its queue representation.
				logger.V(3).Info("Assigned pod spec changed while deferred, updating scheduling queue", "pod", klog.KObj(newPod))
				sched.SchedulingQueue.Update(klog.NewContext(context.Background(), logger), oldPod, newPod)
			}
		}
		sched.updateAssignedPodInCache(oldPod, newPod)
	}
```

三个状态转换对应三个动作：进入 Deferred 时入队，离开 Deferred（kubelet 完成了 resize，或者用户撤回了修改）时出队，持续 Deferred 期间 spec 再变（比如用户把目标值又改大了）时更新队列里的副本。`addPod`（调度器重启后 informer 首次同步）和 `deletePod` 也有对应处理。注意 `updateAssignedPodInCache` 照常执行：这个 Pod 同时存在于缓存和队列中，这一点后面会再提到。

队列侧，Deferred 的 Pod 被强制当作单个 Pod 处理，即使它属于某个 PodGroup：

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) isPodGroupMember(pod *v1.Pod) bool {
	if p.isInPlacePodVerticalScalingSchedulerPreemptionEnabled {
		// Deferred resize pods must be queued and evaluated individually rather than as a pod group because they are
		// already running on assigned nodes and undergo atomic individual resource adjustments rather than group gang-scheduling.
		// TODO: This implementation will be reevaluated in beta.
		if resource.IsPodResizeDeferred(pod) {
			return false
		}
	}
	return p.isGenericWorkloadEnabled && pod.Spec.SchedulingGroup != nil
}
```

失败处理侧，`handleSchedulingFailure` 原本有一个保护：如果 informer 里的 Pod 已经有了 `spec.nodeName`（比如 extender 绑定超时但实际成功了），就不再放回队列。Deferred 的 Pod 天生有 `nodeName`，所以这个保护要绕开；同时它也不应该被设置 `nominatedNodeName`、不应该改 `PodScheduled` 条件，函数在入队之后直接返回：

```go
// pkg/scheduler/schedule_one.go
	// Deferred resize pods are already bound to and running on their assigned node. The scheduler only
	// evaluates resize feasibility and executes preemption; Kubelet actuates the resize in-place.
	// Return early to avoid binding operations or setting nominated node status.
	if isDeferredResize {
		return
	}
```

## 三、绝大多数插件让路

调度周期开始后，Deferred 的 Pod 在 PreFilter 阶段被几乎所有插件 `Skip`。`schedule_one_test.go` 中的 `TestScheduler_DeferredResizePluginSkipping` 把这个分工写成了断言：注册十几个 Filter 插件，跑一次 `SchedulePod`，检查 `state.GetSkipFilterPlugins()`：

```go
// pkg/scheduler/schedule_one_test.go
	// Irrelevant plugins should be skipped
	irrelevant := []string{
		nodeaffinity.Name,
		podtopologyspread.Name,
		nodevolumelimits.CSIName,
		nodeports.Name,
		volumebinding.Name,
		volumezone.Name,
		volumerestrictions.Name,
		interpodaffinity.Name,
		nodedeclaredfeatures.Name,
		dynamicresources.Name,
		tainttoleration.Name,
		nodeunschedulable.Name,
	}
	// ...
	// Relevant plugins must NOT be skipped
	relevant := []string{
		nodename.Name,
		noderesources.Name,
	}
```

道理很直接：Pod 不换节点，节点亲和、污点、端口、卷、拓扑分布、Pod 间亲和在它绑定时都已经检查过了，现在唯一变化的是资源需求。`NodeName` 没有 PreFilter，不在 Skip 之列，它的 Filter 负责把候选范围收缩到 Pod 当前所在的节点（其它节点返回 `UnschedulableAndUnresolvable`）。`NodeResourcesFit` 负责资源判断。再加上本文的主角，这次调度周期里真正干活的只有三个 Filter。

PostFilter 也做了同样的分工：`DynamicResources` 的 PostFilter（释放闲置 ResourceClaim）对 Deferred Pod 直接返回 `Unschedulable`，只让 `DefaultPreemption` 工作。

## 四、`NodeResourcesFit` 的增量判断

Deferred Pod 的资源判断有一个特殊之处：它已经在节点上了，节点的 `requested` 里已经算了它一份。算的是多少？调度器缓存计算 Pod 资源时取 `max(spec, 已分配, 实际)`（`resource.PodRequests` 在 `UseStatusResources` 时如此），resize 中的 Pod 按新的、更大的 spec 计入。如果 Filter 再按常规把 Pod 的请求量加上去，就重复计算了。

`helper/resource_fit.go` 为此加了一个调整函数：

```go
// pkg/scheduler/framework/plugins/helper/resource_fit.go
func adjustDeltasToAccomodateCacheDiscrepancy(opts ResourceRequestsOptions, podRequest *framework.Resource, nodeInfo fwk.NodeInfo, pod *v1.Pod) (int64, int64, int64, map[v1.ResourceName]int64) {
	deltaMilliCPU := podRequest.MilliCPU
	// ...
	if !opts.EnableInPlacePodVerticalScalingSchedulerPreemption || pod == nil || len(pod.Spec.NodeName) == 0 || pod.Spec.NodeName != nodeInfo.Node().Name {
		return deltaMilliCPU, deltaMemory, deltaEphemeralStorage, deltaScalarResources
	}

	var cachedPodInfo fwk.PodInfo
	for _, pInfo := range nodeInfo.GetPods() {
		if pInfo.GetPod().UID == pod.UID {
			cachedPodInfo = pInfo
			break
		}
	}
	// ...
	cachedRes := cachedPodInfo.CalculateResource().Resource
	// We take max(0, ...) to prevent negative deltas when podRequest < cachedRes (e.g., during scale-down
	// or asynchronous cache lag). Allowing a negative delta would improperly reduce the node's requested
	// usage before the Kubelet has actually freed the resources. If a stale larger cachedRes causes
	// preemption to fail, eventual cache convergence will emit a scale-down event to wake up the pod.
	deltaMilliCPU = max(0, podRequest.MilliCPU-cachedRes.GetMilliCPU())
	// ...
}
```

`delta = max(0, 队列中 Pod 的请求 − 缓存中同一 Pod 的值)`。缓存与队列中的 Pod 同步时 delta 为 0，判断式 `delta > allocatable − requested` 实际上变成了「节点是否已经超卖」。函数名里的 `CacheDiscrepancy` 说明了它的另一个用途：队列里的 Pod 对象与缓存里的可能有时间差（用户刚又改了一次 spec），delta 吸收这个差值。

用集成测试 `TestDeferredResizePodPreemption` 的第一个用例算一遍：节点 cpu allocatable 300m；victim-1 低优先级，请求 200m；preemptor 已分配 100m，spec 改成 300m。缓存里 preemptor 按 300m 计，节点 requested = 500m。Filter 时 delta = 300 − 300 = 0，`0 > 300 − 500` 成立，返回 `Insufficient cpu`。抢占模拟中移除 victim-1，requested 降到 300m，`0 > 0` 不成立，放得下。于是 victim-1 被驱逐。

`NodeResourcesFit` 的 QueueingHint 也做了相应调整：对 Deferred Pod，其它节点上的 Pod 删除、缩容、节点变化一律 `QueueSkip`，只有同一节点上的事件才可能 `Queue`。gate 开启时它还多注册了两个扩容事件：同节点其它 Pod 扩容（抢走了 Deferred Pod 等待的余量）和目标 Pod 自身再扩容，都需要重新评估是否要抢占。

## 五、插件源码

整个插件不到 160 行。

### 5.1 PreFilter

```go
// pkg/scheduler/framework/plugins/deferredpodscheduling/deferred_pod_scheduling.go
func (pl *DeferredPodScheduling) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if !pl.enableInPlacePodVerticalScalingSchedulerPreemption || !resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, nil
}
```

与其它插件正好相反：普通 Pod `Skip`，Deferred Pod 才参与。没有写 CycleState，`PreFilterExtensions` 返回 nil。

### 5.2 Filter

```go
// pkg/scheduler/framework/plugins/deferredpodscheduling/deferred_pod_scheduling.go
func (pl *DeferredPodScheduling) Filter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if !pl.enableInPlacePodVerticalScalingSchedulerPreemption || !resource.IsPodResizeDeferred(pod) {
		return nil
	}

	node := nodeInfo.Node()
	if node == nil {
		return fwk.NewStatus(fwk.Error, "node not found")
	}

	if pod.Spec.NodeName != "" && pod.Spec.NodeName != node.Name {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "pod assigned to different node")
	}

	if node.Spec.PodPreemptionPolicy != nil && len(node.Spec.PodPreemptionPolicy.DisableResizePreemption) > 0 {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonNodeDisablesResizePreemption)
	}
	return nil
}
```

两个检查。第一个与 `NodeName` 插件重复，作为防御。第二个读的是 KEP-5836 在 Node 上新增的 spec 字段：

```go
// pkg/apis/core/types.go
	// PodPreemptionPolicy controls the node-level preemption behaviors for pods on this node.
	// This is an alpha field and requires enabling the InPlacePodVerticalScalingSchedulerPreemption feature gate.
	// +featureGate=InPlacePodVerticalScalingSchedulerPreemption
	// +optional
	PodPreemptionPolicy *NodePodPreemptionPolicy
}

// NodePodPreemptionPolicy defines the node-level policies governing preemption for pods on this node.
type NodePodPreemptionPolicy struct {
	// DisableResizePreemption lists the owners that have requested to disable scheduler and Kubelet preemption for in-place pod resize on this node.
	// This is an alpha field and requires enabling the InPlacePodVerticalScalingSchedulerPreemption feature gate.
	// +optional
	DisableResizePreemption []string
}
```

`DisableResizePreemption` 是一个「所有者」列表而不是布尔值。多个组件（运维工具、节点维护控制器、某个 operator）可能各自出于不同原因要求临时禁止这个节点上的 resize 抢占，每个组件把自己的名字加进去、用完再删掉，列表为空时才恢复。用布尔值的话，两个组件先后设置、其中一个提前清除，就会覆盖另一个的意图。

节点禁止了 resize 抢占时，Filter 返回 `UnschedulableAndUnresolvable`，抢占不会把这个节点列为候选——而这是 Deferred Pod 唯一可能的节点，所以抢占直接放弃，Pod 留在队列里等待。

apiserver 侧，`nodeStrategy` 在 gate 关闭且旧对象没有用到这个字段时会清空 `spec.podPreemptionPolicy`，这是 Alpha 字段的标准处理。

### 5.3 为什么要排在 `NodeResourcesFit` 之前

`default_plugins.go` 中插入它的函数带着一段注释：

```go
// pkg/scheduler/apis/config/v1/default_plugins.go
// applyDeferredPodScheduling inserts the DeferredPodScheduling plugin.
// It must be placed before NodeResourcesFit so that if a node has preemption disabled,
// the pod fails on DeferredPodScheduling first. This ensures DeferredPodScheduling is recorded
// as the failing plugin, allowing its Queueing Hint to trigger a retry when the policy is enabled.
// It must run after basic filters like NodeName to avoid useless retries on irrelevant node events.
func applyDeferredPodScheduling(config *v1.Plugins) {
	idx := slices.IndexFunc(config.MultiPoint.Enabled, func(p v1.Plugin) bool {
		return p.Name == names.NodeResourcesFit
	})
	if idx != -1 {
		config.MultiPoint.Enabled = slices.Insert(config.MultiPoint.Enabled, idx, v1.Plugin{Name: names.DeferredPodScheduling})
	} else {
		config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.DeferredPodScheduling})
	}
}
```

`slices.Insert(..., idx, ...)` 把它放在 `NodeResourcesFit` 原来的位置上，`NodeResourcesFit` 及之后的插件后移一位，即紧挨在 `NodeResourcesFit` **之前**。顺序之所以重要，要从两个机制说起：

1. `RunFilterPlugins` 遇到第一个失败的插件就返回，并把这个插件名记进 Status；
2. 失败的 Pod 进入 unschedulable 队列后，只有**记录在案的失败插件**注册的事件和 QueueingHint 才能把它唤醒。

假设一个节点既禁止了 resize 抢占、又资源不足。如果 `NodeResourcesFit` 在前，失败记在 `NodeResourcesFit` 名下，状态码是 `Unschedulable`，于是会走一遍注定失败的抢占模拟（`SelectVictimsOnNode` 移除受害者后重跑 Filter，被 `DeferredPodScheduling` 拒绝）。之后运维把禁止策略撤掉，发出的是 `Node/UpdateNodePreemptionPolicy` 事件，`NodeResourcesFit` 并不关心这个事件，Pod 不会被唤醒，只能等 unschedulable 队列的定期刷新（默认 5 分钟）。`DeferredPodScheduling` 在前，失败就记在它名下，它的 hint 正好监听这个事件。

注释的后半句说明了另一侧的边界：它必须排在 `NodeName` 之后。对其它节点，失败应该记在 `NodeName` 名下，而不是 `DeferredPodScheduling`（它的 Filter 也会对其它节点返回 `pod assigned to different node`），这样无关节点的事件不会经过它的 hint。默认列表里 `NodeName` 排在第 3 位，插入位置在 `NodeResourcesFit`（第 8 位）前面，两个条件同时满足。

{{< figure src="/images/posts/sched-plugins/17-deferred-flow.svg" caption="一个 Deferred resize 在调度器里的完整路径，右侧是调度器缓存中的 cpu 账本，以及插件顺序的理由（点击查看大图）" link="/images/posts/sched-plugins/17-deferred-flow.svg" >}}

### 5.4 Permit：放得下反而拒绝

```go
// pkg/scheduler/framework/plugins/deferredpodscheduling/deferred_pod_scheduling.go
// Permit invoked at the permit extension point.
// It rejects deferred resize pods that successfully fit in order to keep them in the Unschedulable queue until
// Kubelet can successfully actuate the resize.
func (pl *DeferredPodScheduling) Permit(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) (*fwk.Status, time.Duration) {
	if !pl.enableInPlacePodVerticalScalingSchedulerPreemption || !resource.IsPodResizeDeferred(p) {
		return nil, 0
	}
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "pod resize fits, waiting for Kubelet actuation"), 0
}
```

Filter 全部通过，意味着节点现在放得下扩容后的 Pod。这个 Pod 已经绑定了，不需要也不能再 Bind。调度器在这里的使命已经完成，剩下的事是 kubelet 重试 pending resize。Permit 返回 `UnschedulableAndUnresolvable`，让 Pod 以「被 DeferredPodScheduling 拒绝」的身份回到 unschedulable 队列，原因字符串说明它在等什么。集成测试把这称为 parking strategy：Pod 停在队列里，`nominatedNodeName` 保持为空，`PodScheduled` 条件保持 True。等 kubelet 完成 resize、清掉 `Deferred` 条件，事件处理器的 `oldDeferred && !newDeferred` 分支把它从队列里删除。

为什么不在 Filter 通过后直接让 Pod 离开队列？因为「放得下」只是调度器基于自己缓存的判断，kubelet 用自己的分配账本做准入，两者之间有时间差，实际执行也可能再次推迟。Pod 留在队列里，同节点再出现资源竞争（其它 Pod 扩容抢走余量）时，`NodeResourcesFit` 的扩容 hint 会把它重新拉回来评估。

### 5.5 一个值得留意的实现细节

按 `prepareForBindingCycle` 的顺序，Permit 之前先执行 `AssumeAndReserveInCache`，第一步是把 Pod「假定」进调度器缓存：

```go
// pkg/scheduler/backend/cache/cache.go
func (cache *cacheImpl) AssumePod(logger klog.Logger, pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.podStates[key]; ok {
		return fmt.Errorf("pod %v(%v) is in the cache, so can't be assumed", key, klog.KObj(pod))
	}

	return cache.addPod(logger, pod, true)
}
```

而 Deferred Pod 是已绑定的 Pod，早已通过 `addAssignedPodToCache` 进入了缓存（第二节提到它同时存在于缓存和队列中）。在 `aabdf5a2` 上，用 `go test -overlay` 注入一个临时测试（不修改源码），以默认插件顺序（`NodeName`、`DeferredPodScheduling`、`NodeResourcesFit`、Permit 阶段的 `DeferredPodScheduling`）对一个「已在缓存中、资源放得下」的 Deferred Pod 跑一次 `schedulingCycle`，得到的是：

```text
Scheduler cache AssumePod failed err="pod pod1(ns1/pod1) is in the cache, so can't be assumed"
status code=Error msg="pod pod1(ns1/pod1) is in the cache, so can't be assumed" plugin=""
```

也就是说，在这个提交上，「放得下」的分支在到达 Permit 之前就以 `Error` 结束，Pod 作为调度错误进入 backoff 队列（`ConsecutiveErrorsCount` 递增），反复重试直到 kubelet 完成 resize、事件处理器把它移出队列。最终结果（不驱逐、不绑定、等待 kubelet）与设计一致，但路径不是 Permit 描述的 parking：错误日志会周期性出现，重试间隔按错误退避增长。插件自己的单元测试直接调用 `Permit`，集成测试的 parking 用例只断言「没有驱逐、Pod 仍在队列、`PodScheduled` 仍为 True」，都覆盖不到这一点。功能还在 Alpha，这类细节很可能在后续版本中调整，读者在自己的版本上验证时可以关注 kube-scheduler 日志里是否有这条 `AssumePod failed`。

## 六、EventsToRegister 与 QueueingHint

```go
// pkg/scheduler/framework/plugins/deferredpodscheduling/deferred_pod_scheduling.go
func (pl *DeferredPodScheduling) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{
			Event:          fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.UpdateNodePreemptionPolicy},
			QueueingHintFn: pl.isSchedulableAfterNodeChange,
		},
		{
			Event:          fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add},
			QueueingHintFn: pl.isSchedulableAfterNodeAdd,
		},
	}, nil
}
```

`UpdateNodePreemptionPolicy` 是为此新增的节点事件类型，事件处理器用 `equality.Semantic.DeepEqual` 比较新旧节点的 `spec.podPreemptionPolicy` 来识别它。两个 hint 的逻辑：

```go
// pkg/scheduler/framework/plugins/deferredpodscheduling/deferred_pod_scheduling.go
	if pod.Spec.NodeName != newNode.Name {
		return fwk.QueueSkip, nil
	}

	oldNodeDisabled := oldNode.Spec.PodPreemptionPolicy != nil && len(oldNode.Spec.PodPreemptionPolicy.DisableResizePreemption) > 0
	newNodeDisabled := newNode.Spec.PodPreemptionPolicy != nil && len(newNode.Spec.PodPreemptionPolicy.DisableResizePreemption) > 0

	// If resize preemption was disabled and is now enabled, the pod may be schedulable.
	if oldNodeDisabled && !newNodeDisabled {
		logger.V(5).Info("Node preemption policy changed from disabled to enabled, queueing pod", "pod", klog.KObj(pod), "node", newNode.Name)
		return fwk.Queue, nil
	}

	return fwk.QueueSkip, nil
```

- 节点更新：只关心 Pod 所在节点，只在「禁止 → 允许」时 `Queue`。「允许 → 禁止」和其它字段的变化都 `QueueSkip`。所有者列表从两个变成一个仍然是「禁止」，不会唤醒。
- 节点新增：只有名字等于 Pod 的 `nodeName`、且没有禁止 resize 抢占时 `Queue`。正常情况下 Pod 所在的节点不会「新增」，这对应的是节点对象被删除后又以同名重建的情况。

两个 hint 的开头都先检查 gate 和 `IsPodResizeDeferred`，对普通 Pod 一律 `QueueSkip`，因为普通 Pod 不可能被这个插件拒绝。

集成测试 `TestDeferredResizeNodePreemptionPolicy` 的三个子用例对应三种情况：禁止策略生效不会唤醒 Pod，撤掉禁止策略会让 Pod 的调度尝试次数增加，其它节点的策略变化不会唤醒 Pod。

## 七、边界情况与坑

**`PreemptNever` 依然有效。** Deferred Pod 走的是标准的 `DefaultPreemption`，`PodEligibleToPreemptOthers` 照常检查 `preemptionPolicy`。集成测试的「no preemption when preemptor policy is PreemptNever」用例确认了这一点：Pod 的 `preemptionPolicy: Never` 时不会驱逐任何 Pod，resize 会一直 Deferred。

**只能驱逐更低优先级的 Pod。** 受害者的优先级必须低于 Deferred Pod 自己。节点上都是同优先级的 Pod 时，抢占找不到受害者。Deferred Pod 自己在节点上，优先级等于自身，不会被选为受害者。

**会关掉整个 Profile 的 opportunistic batching。** `DeferredPodScheduling` 实现了 PreFilter/Filter，却没有实现 `SignPlugin`。框架初始化时发现这种插件，会对整个 Profile 关闭签名（日志 `Disabling signatures for profile because plugins do not support it. plugins=["DeferredPodScheduling"]`）。在默认配置下 `PodTopologySpread` 的系统默认约束已经让所有 Pod 拒签（见[第 13 篇](/posts/sched-plugin-13-pod-topology-spread/)），影响不明显；但对专门调过配置以启用批量调度的集群，开启这个 gate 会让批量调度整体失效。

**驱逐是异步完成的。** 受害者先进入 Terminating，调度器缓存要等它的删除事件到达才把它的资源从节点上减掉；kubelet 重试 pending resize 有自己的触发时机，用的是自己的分配账本。两边存在时间差，调度器判断「放得下」时 kubelet 未必已经执行，这也是 Pod 被设计成留在队列里等待、而不是立即出队的原因之一。

**只覆盖 Deferred，不覆盖 Infeasible。** 节点总容量不够时 kubelet 标记 `Infeasible`，调度器不介入。原地扩容不会把 Pod 迁到更大的节点上，这需要上层（比如 VPA 的重建模式）处理。

## 八、发散

### 8.1 调度器的角色在扩大

传统上调度器只做一件事：给 `nodeName` 为空的 Pod 选一个节点。这个 gate 让调度器第一次以「已绑定 Pod 的资源仲裁者」的身份出现。为此几乎每个 Filter 插件都要加一行 `IsPodResizeDeferred` 的判断，`NodeResourcesFit` 要理解「缓存里已经算过一次」，队列、失败处理、事件处理器都要识别这类 Pod。这些分散在各处的 `if` 是这个设计目前的主要代价，KEP 和队列代码里的 `TODO: This implementation will be reevaluated in beta` 也说明它还在演进。

另一种可能的设计是让 kubelet 自己完成抢占：kubelet 已经有为关键 Pod 腾地方的代码。但 kubelet 只看本节点，不知道 PDB，没有「最少驱逐」的优化，抢占策略和调度器的抢占也会出现两套标准。把决策收拢到调度器，用户只需要理解一套优先级与抢占规则。

### 8.2 和 VPA 的关系

Vertical Pod Autoscaler 的 `InPlaceOrRecreate` 更新模式会先尝试原地修改 resources，失败再重建 Pod。没有这个 gate 时，原地修改在资源紧张的节点上大概率停在 Deferred，VPA 只能在等待一段时间后回退到重建，重建意味着中断。有了调度器抢占，高优先级的工作负载在原地扩容时可以把低优先级任务挤走，减少回退到重建的次数。对优先级设计清晰的集群（在线服务高、批处理低），这让原地扩缩容真正可用；对所有 Pod 都是默认优先级的集群，它几乎没有效果。

### 8.3 运维侧的开关

`DisableResizePreemption` 给了运维一个节点级的「不要因为扩容而驱逐」的开关。适合的场景包括：节点上跑着不能被打断的任务（比如正在做检查点的训练作业）、节点即将下线维护时不希望产生额外的驱逐。由于它是所有者列表，配合自动化工具使用时，每个工具只管理自己的那一项，不会互相覆盖。

## 小结

- kubelet 把放不下的原地扩容标记为 `PodResizePending/Deferred`；gate `InPlacePodVerticalScalingSchedulerPreemption`（1.37 Alpha，默认关闭）开启后，kubelet 不再为 resize 做抢占，事件处理器把这类已绑定的 Pod 重新送进调度队列。
- 调度周期中几乎所有插件对它 `Skip`，只有 `NodeName`（锁定当前节点）、`DeferredPodScheduling` 和 `NodeResourcesFit`（按缓存差值做增量判断）工作，资源不足时由 `DefaultPreemption` 驱逐同节点的低优先级 Pod。
- `DeferredPodScheduling` 的 Filter 检查节点的 `spec.podPreemptionPolicy.disableResizePreemption`，非空即 `UnschedulableAndUnresolvable`；Permit 对放得下的 Pod 返回 `UnschedulableAndUnresolvable`，意在让它停在队列里等 kubelet 执行。
- 它被插在 `NodeResourcesFit` **之前**、`NodeName` 之后：保证禁止策略导致的失败记在它名下，策略撤销时它的 `UpdateNodePreemptionPolicy` hint 能唤醒 Pod。
- 在 `aabdf5a2` 上，「放得下」的路径会先在 `AssumePod` 处以 `Error` 结束，到不了 Permit；它没有实现 `SignPlugin`，开启 gate 会关闭整个 Profile 的批量调度。

下一篇进入 PostFilter，拆解本文反复出现的 [DefaultPreemption](/posts/sched-plugin-18-default-preemption/)。
