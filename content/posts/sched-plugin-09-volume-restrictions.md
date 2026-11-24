---
title: 'scheduler-plugins 09 [in-tree] VolumeRestrictions：同一块盘不能被两个 Pod 抢着写'
date: 2026-11-19T09:30:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "存储", "ReadWriteOncePod", "抢占"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "VolumeRestrictions 是默认 Profile 里第一个存储插件，管两件事：几种 in-tree 内联卷在同一节点上的读写冲突，以及 ReadWriteOncePod PVC 在全集群范围内只能被一个 Pod 使用。本文拆解它的 PreFilter 计数、Filter 判定、AddPod/RemovePod 如何让 RWOP 冲突可以被抢占解决，以及几个 QueueingHint 的判断依据。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `VolumeRestrictions` 实现的扩展点（PreFilter、Filter）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/09.svg" link="/images/posts/sched-plugins/callstack/09.svg" caption="VolumeRestrictions 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇 [NodeResourcesFit](/posts/sched-plugin-08-node-resources-fit/) 把 CPU、内存、扩展资源算完之后，Filter 链进入存储段。默认 Profile 里连着四个存储插件：`VolumeRestrictions`、`NodeVolumeLimits`、`VolumeBinding`、`VolumeZone`，各管一段，本篇是第一个。

本文基于 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期）。`VolumeRestrictions` 在 `pkg/scheduler/apis/config/v1/default_plugins.go` 的 MultiPoint 列表中排在 `NodeResourcesFit` 之后、`NodeVolumeLimits` 之前，默认启用，不受任何 feature gate 控制。它实现了 PreFilter（带 `PreFilterExtensions`）、Filter、`EnqueueExtensions` 和 `SignPlugin`，没有 Score，所以也没有权重。

## 一、它拦的是哪两类 Pod

插件名字很宽泛，实际只检查两件事，彼此几乎没有关系。

**第一类是内联卷的读写冲突。** Pod 直接在 `spec.volumes` 里写 GCE PD、AWS EBS、iSCSI 或 Ceph RBD 卷（不经过 PVC），而同一节点上已有 Pod 以冲突的方式用着同一块盘：

```yaml
# 节点上已有 Pod 使用了同一块 EBS 卷
volumes:
- name: data
  awsElasticBlockStore:
    volumeID: vol-0abc123
    fsType: ext4
```

再来一个声明同一 `volumeID` 的 Pod，这个节点就会被过滤掉，原因是 `node(s) had no available disk`。这是调度器里年头最久的规则之一，前身是 `NoDiskConflict` predicate。

**第二类是 ReadWriteOncePod（RWOP）冲突。** PVC 的 `accessModes` 里有 `ReadWriteOncePod`，表示整个集群里同一时间只能有一个 Pod 使用它：

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: single-writer
spec:
  accessModes: ["ReadWriteOncePod"]
  resources:
    requests:
      storage: 10Gi
```

已经有一个 Pod 挂着 `single-writer`，第二个引用它的 Pod 会在所有节点上失败，原因是 `node(s) unavailable due to PersistentVolumeClaim with ReadWriteOncePod access mode already in-use by another pod`。这条约束和 `ReadWriteOnce` 不同：RWO 限制的是「一个节点」，同节点多个 Pod 可以共享；RWOP 限制的是「一个 Pod」。

两类冲突都返回 `Unschedulable`，都可以被抢占解决，但解决的方式不一样，后面会细说。

## 二、源码拆解

### 2.1 结构体与工厂

```go
// pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go
type VolumeRestrictions struct {
	pvcLister                                          corelisters.PersistentVolumeClaimLister
	sharedLister                                       fwk.SharedLister
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

const (
	// ...
	preFilterStateKey = "PreFilter" + Name

	// ErrReasonDiskConflict is used for NoDiskConflict predicate error.
	ErrReasonDiskConflict = "node(s) had no available disk"
	// ErrReasonReadWriteOncePodConflict is used when a PVC with ReadWriteOncePod access mode is already in-use by another pod.
	ErrReasonReadWriteOncePodConflict = "node(s) unavailable due to PersistentVolumeClaim with ReadWriteOncePod access mode already in-use by another pod"
)
```

插件没有 Args。`New` 只做两件事：从 informer 拿 PVC lister，从 `handle.SnapshotSharedLister()` 拿调度快照。后者是 RWOP 检查的关键，它提供 `StorageInfos().IsPVCUsedByPods(key)`，直接回答「这个 PVC 当前有没有被某个已调度的 Pod 用着」。

第三个字段对应 `InPlacePodVerticalScalingSchedulerPreemption`（1.37 alpha，默认关闭）。开启后，因原地扩容被推迟（resize deferred）而重新进入调度的 Pod，在 PreFilter 直接返回 `Skip`、Filter 直接放行。这类 Pod 已经在节点上跑着，卷早就挂好了，再检查一遍卷冲突只会和自己冲突。同样的判断在后面三个存储插件里都有，具体机制留给 [DeferredPodScheduling](/posts/sched-plugin-17-deferred-pod-scheduling/) 一篇。

### 2.2 CycleState：只为 RWOP 服务

```go
// pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go
// preFilterState computed at PreFilter and used at Filter.
type preFilterState struct {
	// Names of the pod's volumes using the ReadWriteOncePod access mode.
	readWriteOncePodPVCs sets.Set[string]
	// The number of references to these ReadWriteOncePod volumes by scheduled pods.
	conflictingPVCRefCount int
}

func (s *preFilterState) updateWithPod(podInfo fwk.PodInfo, multiplier int) {
	s.conflictingPVCRefCount += multiplier * s.conflictingPVCRefCountForPod(podInfo)
}

// Clone the prefilter state.
func (s *preFilterState) Clone() fwk.StateData {
	if s == nil {
		return nil
	}
	return &preFilterState{
		readWriteOncePodPVCs:   s.readWriteOncePodPVCs,
		conflictingPVCRefCount: s.conflictingPVCRefCount,
	}
}
```

状态里只有两样东西：本 Pod 引用的 RWOP PVC 名字集合，以及「这些 PVC 已经被别的 Pod 引用了几次」。内联卷冲突完全不进 CycleState，Filter 时直接从 `nodeInfo.GetPods()` 现算。

`Clone` 的写法值得注意：集合是浅拷贝，计数是值拷贝。集合在 PreFilter 之后只读，共享没有问题；计数会被 `AddPod`/`RemovePod` 修改，而抢占时 DefaultPreemption 会为每个候选节点克隆一份 CycleState 并行模拟，每份必须有自己的计数。这正好是「只复制会变的部分」。

### 2.3 PreFilter：查 PVC、数引用、决定要不要 Skip

```go
// pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go
func (pl *VolumeRestrictions) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// ...
	needsCheck := false
	for i := range pod.Spec.Volumes {
		if needsRestrictionsCheck(pod.Spec.Volumes[i]) {
			needsCheck = true
			break
		}
	}

	pvcs, err := pl.readWriteOncePodPVCsForPod(ctx, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
		}
		return nil, fwk.AsStatus(err)
	}

	s, err := pl.calPreFilterState(ctx, pod, pvcs)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}

	if !needsCheck && s.conflictingPVCRefCount == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	cycleState.Write(preFilterStateKey, s)
	return nil, nil
}
```

`needsRestrictionsCheck` 只认四种内联卷：`GCEPersistentDisk`、`AWSElasticBlockStore`、`RBD`、`ISCSI`。`readWriteOncePodPVCsForPod` 遍历 Pod 的 `persistentVolumeClaim` 卷，逐个从 lister 取 PVC，挑出 `accessModes` 含 `ReadWriteOncePod` 的。注意它对**每个** PVC 都做 Get，不管是不是 RWOP，所以 PVC 不存在时这里第一个发现。

这带来一个排障时常见的现象：Pod 引用了一个还没创建的 PVC，事件里看到的失败插件是 `VolumeRestrictions`，信息是 `persistentvolumeclaim "xxx" not found`，状态码 `UnschedulableAndUnresolvable`。框架在 PreFilter 阶段遇到 `UnschedulableAndUnresolvable` 会立刻返回，不再跑后面的 PreFilter（`RunPreFilterPlugins` 里的注释说得很清楚：这种情况下本轮不会抢占，没必要继续），所以排在后面的 `VolumeBinding` 根本没有机会报出自己的错误信息。

计数逻辑在 `calPreFilterState`：

```go
// pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go
func (pl *VolumeRestrictions) calPreFilterState(ctx context.Context, pod *v1.Pod, pvcs sets.Set[string]) (*preFilterState, error) {
	conflictingPVCRefCount := 0
	for pvc := range pvcs {
		key := framework.GetNamespacedName(pod.Namespace, pvc)
		if pl.sharedLister.StorageInfos().IsPVCUsedByPods(key) {
			// There can only be at most one pod using the ReadWriteOncePod PVC.
			conflictingPVCRefCount += 1
		}
	}
	// ...
}
```

`IsPVCUsedByPods` 背后是快照里的 `usedPVCRefCounts`，由每个 `NodeInfo.PVCRefCounts` 汇总而来（`pkg/scheduler/backend/cache/snapshot.go` 的 `createUsedPVCRefCounts`）。`NodeInfo` 在 AddPod 时通过 `PodPVCKeys` 给 Pod 引用的每个 PVC 计数加一。这里有两个细节：

- 快照包含 **assumed Pod**。一个 Pod 刚在 Reserve 后被 assume、还在异步绑定，下一个引用同一 RWOP PVC 的 Pod 就已经能看到冲突，不会出现两个 Pod 前后脚都「抢到」的竞态。
- 这是**集群级**的查询，与节点无关。所以 RWOP 冲突一旦存在，Filter 对每个节点给出的结论都一样。

最后的 `Skip` 判断：既没有四种内联卷，也没有 RWOP 冲突，就跳过 Filter。注意条件是「冲突计数为 0」而不是「没有 RWOP PVC」，即使 Pod 用了 RWOP PVC，只要现在没人占着，同样 Skip。

这个 Skip 有一个副作用。框架在 Filter 前会把「已提名到该节点、优先级不低于当前 Pod」的 Pod 通过 `AddPod` 临时加进状态（`addGENominatedPods`），但被 Skip 的插件连 `AddPod` 也不会被调用（`RunPreFilterExtensionAddPod` 里检查了 `GetSkipFilterPlugins()`）。被提名的 Pod 还没进调度缓存，不在 `usedPVCRefCounts` 里。所以「Pod A 刚抢占了 RWOP 占用者、正等着受害者退出，Pod B 引用同一个 PVC 进来」时，B 的计数是 0，直接 Skip。即使不 Skip，`AddPod` 也只在 A 被提名的那个节点上执行，B 在其它节点仍然能过。RWOP 的唯一性对「已提名未绑定」的 Pod 是不设防的，最终靠 assume 之后的快照计数和驱动侧的 `SINGLE_NODE_SINGLE_WRITER` 兜底。

### 2.4 Filter：两段检查

```go
// pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go
func (pl *VolumeRestrictions) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	// ...
	if !satisfyVolumeConflicts(pod, nodeInfo) {
		return fwk.NewStatus(fwk.Unschedulable, ErrReasonDiskConflict)
	}
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	return satisfyReadWriteOncePod(ctx, state)
}

func satisfyReadWriteOncePod(ctx context.Context, state *preFilterState) *fwk.Status {
	if state == nil {
		return nil
	}
	if state.conflictingPVCRefCount > 0 {
		return fwk.NewStatus(fwk.Unschedulable, ErrReasonReadWriteOncePodConflict)
	}
	return nil
}
```

`satisfyVolumeConflicts` 对 Pod 的每个需要检查的内联卷，与节点上每个已有 Pod 做两两比较，核心是 `isVolumeConflict`：

```go
// pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go
func isVolumeConflict(volume *v1.Volume, pod *v1.Pod) bool {
	for _, existingVolume := range pod.Spec.Volumes {
		// Same GCE disk mounted by multiple pods conflicts unless all pods mount it read-only.
		if volume.GCEPersistentDisk != nil && existingVolume.GCEPersistentDisk != nil {
			disk, existingDisk := volume.GCEPersistentDisk, existingVolume.GCEPersistentDisk
			if disk.PDName == existingDisk.PDName && !(disk.ReadOnly && existingDisk.ReadOnly) {
				return true
			}
		}

		if volume.AWSElasticBlockStore != nil && existingVolume.AWSElasticBlockStore != nil {
			if volume.AWSElasticBlockStore.VolumeID == existingVolume.AWSElasticBlockStore.VolumeID {
				return true
			}
		}
		// ... ISCSI: 同 IQN 且不是双方都只读
		// ... RBD: monitor 有交集、pool 和 image 相同，且不是双方都只读
	}

	return false
}
```

四种卷的「同一块盘」判定和冲突条件各不相同：

| 卷类型 | 判定为同一块盘 | 何时冲突 |
| --- | --- | --- |
| GCE PD | `pdName` 相同 | 除非双方都 `readOnly` |
| AWS EBS | `volumeID` 相同 | 总是冲突，只读也不行 |
| iSCSI | `iqn` 相同 | 除非双方都 `readOnly` |
| Ceph RBD | `monitors` 至少一个相同，且 `pool`、`image` 相同 | 除非双方都 `readOnly` |

EBS 的规则最严，只读也冲突，对应的是 EBS 卷（在不考虑 multi-attach 的前提下）一次只能挂到一台实例的一个设备上。RBD 的 monitor 用「有交集」而不是「完全相等」判断，因为同一个 Ceph 集群可以用不同的 monitor 列表来描述，`haveOverlap` 先把较短的一侧放进 set，再遍历较长的一侧。

几个容易误解的地方：

- 这段检查只看 **Pod spec 里直接写的内联卷**。通过 PVC 使用的 GCE PD、EBS 卷（PV 的 `spec.gcePersistentDisk` 等）不在检查范围内。
- 只和**同一节点**上的 Pod 比较。EBS 卷被另一个节点上的 Pod 占着，这里不会拦，冲突会在 attach 阶段暴露。
- 源码里 `Filter` 的注释写着 RBD 和 iSCSI「forbids if ... and the image is read-only」，与代码逻辑正好相反。以代码为准：双方都只读时**不**冲突。

### 2.5 AddPod / RemovePod：让 RWOP 冲突能被抢占解开

```go
// pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go
func (pl *VolumeRestrictions) AddPod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	state.updateWithPod(podInfoToAdd, 1)
	return nil
}

func (pl *VolumeRestrictions) RemovePod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	state.updateWithPod(podInfoToRemove, -1)
	return nil
}
```

`updateWithPod` 统计被加入/移除的 Pod 引用了几个「本 Pod 的 RWOP PVC」，乘以 ±1 加到计数上。两段检查在抢占时的行为因此不同：

- **内联卷冲突**：Filter 每次都从 `nodeInfo.GetPods()` 现算。抢占模拟把受害者从 NodeInfo 副本里删掉，Filter 自然看不到它，不需要额外支持。
- **RWOP 冲突**：计数是 PreFilter 算好的，与 NodeInfo 无关。如果没有 `RemovePod`，抢占模拟删掉占用者之后，计数仍然是 1，Filter 照样失败，抢占永远找不到候选节点。

{{< figure src="/images/posts/sched-plugins/09-rwop-preemption.svg" caption="RWOP 冲突在抢占模拟中的变化：计数是集群级的，只有在占用者所在节点上执行 RemovePod，计数才会归零（点击查看大图）" link="/images/posts/sched-plugins/09-rwop-preemption.svg" >}}

把这个过程放到 DefaultPreemption 里看（细节见 [DefaultPreemption](/posts/sched-plugin-18-default-preemption/) 一篇）：

1. 所有节点的 Filter 都返回 `Unschedulable`，于是每个节点都进入候选。
2. 对每个候选节点，克隆 CycleState，把该节点上优先级更低的 Pod 全部 `RemovePod`，再跑一次 Filter。
3. 只有占用者所在的那个节点（图中的 node-2），移除占用者后计数从 1 变成 0，Filter 通过；其它节点上移除的 Pod 都不引用这个 PVC，计数不变，仍然失败。
4. 之后的「赦免」阶段会尝试把受害者逐个 `AddPod` 回去，加回占用者时计数又变成 1，所以占用者一定留在受害者名单里。

结果是：抢占只会落在占用者所在的节点，并且一定驱逐占用者。RWOP 卷正被那个 Pod 用着，要让新 Pod 用上，只能请它离开。不过新 Pod 并不一定最终落在那个节点：抢占只决定 `nominatedNodeName`，受害者删除后 Pod 重新调度，此时计数归零，任何节点都能通过 VolumeRestrictions，最终落点还要看其它插件（通常调度器会优先尝试提名节点）。

前提当然是占用者的优先级更低。两个优先级相同的 Pod 争同一个 RWOP PVC，后来者只能等。

## 三、EventsToRegister 与 QueueingHint

```go
// pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go
func (pl *VolumeRestrictions) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// Pods may fail to schedule because of volumes conflicting with other pods on same node.
		// Once running pods are deleted and volumes have been released, the unschedulable pod will be schedulable.
		// Due to immutable fields `spec.volumes`, pod update events are ignored.
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodDeleted},
		// A new Node may make a pod schedulable.
		// We intentionally don't set QueueingHint since all Node/Add events could make Pods schedulable.
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}},
		// Pods may fail to schedule because the PVC it uses has not yet been created.
		// This PVC is required to exist to check its access modes.
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add},
			QueueingHintFn: pl.isSchedulableAfterPersistentVolumeClaimAdded},
	}, nil
}
```

三个事件分别对应三种失败原因：

- **已调度 Pod 被删除**：释放内联盘或 RWOP PVC。Pod 的 `spec.volumes` 不可变，所以不关心 Pod 更新事件。
- **新增节点**：内联卷冲突是节点级的，换个干净的节点就能过。不设 hint，所有 Node/Add 一律 `Queue`。
- **新增 PVC**：对应 PreFilter 里 PVC 找不到的情况。

`isSchedulableAfterPersistentVolumeClaimAdded` 逻辑简单：命名空间相同，且名字是 Pod 引用的某个 PVC，返回 `Queue`，否则 `QueueSkip`。

`isSchedulableAfterAssignedPodDeleted` 则做了一个有意思的折中：

```go
// pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go
	if deletedPod.Namespace != pod.Namespace {
		return fwk.QueueSkip, nil
	}

	nodeInfo := framework.NewNodeInfo(deletedPod)
	if !satisfyVolumeConflicts(pod, nodeInfo) {
		// ...
		return fwk.Queue, nil
	}

	// Return Queue if a deleted pod uses the same PVC since the pod may be unschedulable due to the ReadWriteOncePod access mode of the PVC.
	//
	// For now, we don't actually fetch PVC and check the access mode because that operation could be expensive.
	// ...
	claims := sets.New[string]()
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			claims.Insert(volume.PersistentVolumeClaim.ClaimName)
		}
	}
	for _, volume := range deletedPod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && claims.Has(volume.PersistentVolumeClaim.ClaimName) {
			// ...
			return fwk.Queue, nil
		}
	}
```

它把被删的 Pod 包成一个只含它自己的 `NodeInfo`，复用 `satisfyVolumeConflicts` 判断两者的内联卷是否冲突。这个复用很巧：不用关心被删的 Pod 原来在哪个节点，只看「它和我有没有冲突关系」。RWOP 部分则只比较 PVC 名字，不去查 PVC 的 access mode，注释解释了原因：QueueingHint 在事件处理路径上同步执行，多一次 lister 查询会拖慢所有事件的处理，宁可多放几个 Pod 回队列。

有一处值得留意：命名空间不同直接 `QueueSkip`。内联 EBS、GCE PD 是按卷 ID 判冲突的，跨命名空间的两个 Pod 完全可能用同一块盘；按这个 hint，其他命名空间里占盘 Pod 的删除不会唤醒等待者，只能等队列的周期性刷新（`DefaultPodMaxInUnschedulablePodsDuration`，5 分钟）。不过内联云盘本来就是遗留用法，实际影响不大。

## 四、边界与坑

**失败码并不只有 `Unschedulable`。** 常见总结说 VolumeRestrictions「只返回 Unschedulable」，指的是 Filter。PreFilter 在 PVC 不存在时返回的是 `UnschedulableAndUnresolvable`，这也让它成了四个存储插件里第一个报「PVC 不存在」的。

**generic ephemeral volume 不在 RWOP 检查范围内。** `readWriteOncePodPVCsForPod` 和 `PodPVCKeys` 都只认 `volume.PersistentVolumeClaim`，不认 `volume.Ephemeral`。好在 ephemeral 卷生成的 PVC 本来就归属于单个 Pod（`ephemeral.VolumeIsForPod` 会校验 owner），不存在被第二个 Pod 引用的问题。

**绕过调度器的 Pod 不受约束。** 直接写 `spec.nodeName` 的 Pod 不经过调度器，RWOP 的「一个 Pod」约束在调度层面就失效了。kubelet 把 RWOP 翻译成 CSI 的 `SINGLE_NODE_SINGLE_WRITER`（驱动声明支持 `SINGLE_NODE_MULTI_WRITER` 能力时，见 `pkg/volume/csi/csi_client.go`），由驱动在节点上兜底，但跨节点的唯一性只有调度器保证。

**RWOP 冲突会让抢占在所有节点上做一遍模拟。** 由于 Filter 对每个节点都返回 `Unschedulable`，DefaultPreemption 会把所有节点都当候选。集群大的时候这是一笔可观的开销，结果却注定只有一个节点能成。

## 五、发散：这个插件里的两个时代

把 VolumeRestrictions 的两段逻辑并排看，正好是 Kubernetes 存储模型的两个时代。

内联云盘冲突是 in-tree 卷插件时代的产物。那时 Pod 可以直接写 `awsElasticBlockStore`、`gcePersistentDisk`，调度器只能靠逐个比较卷 ID 防止两个 Pod 在同一节点上争同一块盘。CSI 迁移完成后，这些字段仍在 API 里，kubelet 会通过 csi-translation-lib 把它们翻译成对应的 CSI 驱动去挂载，但调度器这段比较逻辑没有跟着改成「按 CSI volumeHandle 比较」。它仍然只认四种内联字段，PVC 路径和原生 CSI 卷都不检查，对这类冲突的保护交给了驱动和 attach/detach 控制器。

RWOP 是 PV 时代的约束。它不关心卷是什么类型，只关心 PVC 被几个 Pod 引用，用的也是调度器快照里现成的 `usedPVCRefCounts` 索引，代价是一次 map 查询。PreFilter 计数加 `AddPod`/`RemovePod` 的组合，是框架里「集群级约束 + 抢占可解」的标准写法，`InterPodAffinity` 的反亲和计数也是同一个模式。写自定义插件时，只要 PreFilter 预计算了依赖其它 Pod 的状态，就要实现这两个方法，否则抢占模拟拿到的是过期状态，要么误判可以抢占，要么永远抢不动。

如果确实要在生产中使用 RWOP，有两点建议。一是给真正需要「独占写」的工作负载用，比如单副本数据库；RWO 已经能保证单节点挂载，RWOP 额外保证的是同一节点上的第二个 Pod 也进不来。二是滚动更新要配合 `maxSurge: 0` 或 `Recreate` 策略，否则新 Pod 会一直因为 RWOP 冲突处于 Pending，直到旧 Pod 删除。新 Pod 优先级不比旧 Pod 高，抢占也帮不上忙。

## 六、小结

- `VolumeRestrictions` 检查两件事：四种 in-tree 内联卷（GCE PD、EBS、iSCSI、RBD）在同一节点上的读写冲突，以及 ReadWriteOncePod PVC 在全集群只能被一个 Pod 使用。
- 两类冲突在 Filter 都返回 `Unschedulable`；PVC 不存在时 PreFilter 返回 `UnschedulableAndUnresolvable`，并且因为它在 PreFilter 链上靠前，常常是第一个报 PVC 缺失的插件。
- RWOP 冲突计数来自调度快照的 `usedPVCRefCounts`，包含 assumed Pod；它是集群级的，所以对每个节点结论一样。
- `AddPod`/`RemovePod` 让计数跟随抢占模拟变化，抢占只会在占用者所在节点成立，并且一定驱逐占用者；内联卷冲突则靠 Filter 现算 NodeInfo 天然支持抢占。
- QueueingHint 对已调度 Pod 删除的判断复用了 `satisfyVolumeConflicts`，RWOP 部分只比 PVC 名不查 access mode，以多唤醒换取事件处理速度。

卷有没有冲突检查完了，下一个问题是节点还能不能再挂一块盘。下一篇 [NodeVolumeLimits](/posts/sched-plugin-10-node-volume-limits/) 讲 CSI 卷数量上限是怎么数的。
