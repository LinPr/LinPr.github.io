---
title: 'scheduler-plugins 11 [in-tree] VolumeBinding：把选 PV 推迟到选节点之后'
date: 2026-11-19T09:50:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "存储", "csi", "WaitForFirstConsumer"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "VolumeBinding 让 PVC 的绑定与动态供给等到 Pod 选定节点之后再做，是唯一横跨调度周期和绑定周期的存储插件。本文按 PreFilter、Filter、Score、Reserve、PreBind、Unreserve 的顺序拆解它的源码：PVC 分类、静态 PV 匹配、selected-node 注解、CSIStorageCapacity 容量检查、Score 的 shape、assume cache 的版本号语义，以及它与 PV 控制器、external-provisioner 的接力。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `VolumeBinding` 实现的扩展点（PreFilter、Filter、PreScore、Score、Reserve、PreBind）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/11.svg" link="/images/posts/sched-plugins/callstack/11.svg" caption="VolumeBinding 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇 [NodeVolumeLimits](/posts/sched-plugin-10-node-volume-limits/) 回答了「这个节点还能不能再挂一块盘」。但一个 PVC 到底对应哪块盘，可能还没有定下来：它也许尚未绑定，要由调度器挑一块合适的 PV，或者通知外部供给器在选定的节点附近新建一块。这件事由 `VolumeBinding` 负责。

本文基于 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期）。`VolumeBinding` 在默认 Profile 的 MultiPoint 列表里排在 `NodeVolumeLimits` 之后、`VolumeZone` 之前，默认启用。

它实现的扩展点比任何其它 in-tree 插件都多：PreFilter、Filter、PreScore、Score、Reserve（含 Unreserve）、PreBind（含 `PreBindPreFlight`），外加 `EnqueueExtensions` 和 `SignPlugin`。

MultiPoint 配置里它没有写权重。框架的 `getValidScoreWeights` 会把未设置或为 0 的权重改成 1，所以只要 Score 生效，权重就是 1。

Score 是否生效取决于 `StorageCapacityScoring` feature gate：1.33 alpha，**1.37 升为 beta 并默认开启**。也就是说从 1.37 起，VolumeBinding 的打分默认参与排序，这是与很多旧资料不同的地方。

## 一、为什么调度器要管 PV 绑定

PV 控制器（kube-controller-manager 的一部分）本来就会给 PVC 找 PV、触发动态供给。问题在于它不知道 Pod 会落在哪里。

设想一个三可用区的集群，StorageClass 是 `Immediate` 模式：PVC 一创建，PV 控制器就找一块 PV 绑上，或者让供给器随便挑一个可用区建盘。假如盘建在 `zone-a`，而 Pod 因为 CPU 不足、反亲和等原因只能去 `zone-b`，这个 Pod 就永远调度不上去了。本地盘（local PV）更极端，PV 的 `nodeAffinity` 写死了某一台机器。

`WaitForFirstConsumer` 模式就是为此设计的：

```yaml {linenos=inline}
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-storage
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: local-pv-node1
spec:
  capacity:
    storage: 100Gi
  accessModes: ["ReadWriteOnce"]
  storageClassName: local-storage
  local:
    path: /mnt/disks/ssd1
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values: ["node-1"]
```

PV 控制器看到 `WaitForFirstConsumer` 的 PVC 时只发事件「waiting for first consumer to be created before binding」，不做绑定（`pkg/controller/volume/persistentvolume/pv_controller.go` 的 `syncUnboundClaim`）。

真正的决策推迟到调度器，分两步：

1. 在 Filter 里，对每个候选节点检查「有没有一块 PV 的 `nodeAffinity` 允许这个节点，或者能不能在这个节点上动态供给」；
2. 选定节点之后，再把决定写回 API，让 PV 控制器和供给器去执行。

这样，存储的拓扑约束就和 CPU、亲和性、污点等约束放在同一次决策里了。代价是调度器必须在调度周期里「记住」自己为每个节点设想的绑定方案，在绑定周期里把它落地，失败了还要撤销。下面按扩展点顺序拆。

## 二、结构体、参数与 Binder

扩展点虽多，插件里保存的东西却很少（第 2–8 行），先看它手里有什么：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
type VolumeBinding struct {
	Binder      SchedulerVolumeBinder
	PVCLister   corelisters.PersistentVolumeClaimLister
	classLister storagelisters.StorageClassLister
	scorer      volumeCapacityScorer
	fts         feature.Features
}
```

插件本身很薄，重活都在 `Binder`（`binder.go` 里的 `volumeBinder`）。插件负责和框架打交道，Binder 负责和 PV/PVC 打交道。

参数 `VolumeBindingArgs` 有两个字段，默认值在 `pkg/scheduler/apis/config/v1/defaults.go`：

```go {linenos=inline}
// pkg/scheduler/apis/config/v1/defaults.go
func SetDefaults_VolumeBindingArgs(obj *configv1.VolumeBindingArgs) {
	if obj.BindTimeoutSeconds == nil {
		obj.BindTimeoutSeconds = ptr.To[int64](600)
	}
	if len(obj.Shape) == 0 && feature.DefaultFeatureGate.Enabled(features.StorageCapacityScoring) {
		obj.Shape = []configv1.UtilizationShapePoint{
			{
				Utilization: 0,
				Score:       int32(config.MaxCustomPriorityScore),
			},
			{
				Utilization: 100,
				Score:       0,
			},
		}
	}
}
```

- `bindTimeoutSeconds` 默认 600 秒（第 3–5 行），是 PreBind 等待 PV 控制器完成绑定的最长时间。校验只要求非负，0 表示不等待。
- `shape` 是打分曲线，默认两点：利用率 0 得 10 分，利用率 100 得 0 分（第 6–17 行）。校验（`ValidateVolumeBindingArgsWithOptions`）在 gate 开启时检查利用率在 0~100 且严格递增、分数在 0~10；gate 关闭时如果配置了 `shape` 直接报错，防止将来开启 gate 时意外生效。

`New` 里构造 Binder 时会拿到八个 informer：Pod、Node、CSINode、PVC、PV、StorageClass，以及 `CapacityCheck` 里的 CSIDriver 和 CSIStorageCapacity。其中 PVC 和 PV 不是直接用 lister，而是包成了 assume cache（第 11–12 行的 `pvcCache`、`pvCache`）：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
type volumeBinder struct {
	kubeClient                  clientset.Interface
	enableVolumeAttributesClass bool

	classLister   storagelisters.StorageClassLister
	podLister     corelisters.PodLister
	nodeLister    corelisters.NodeLister
	csiNodeLister storagelisters.CSINodeLister

	pvcCache PVCAssumeCache
	pvCache  PVAssumeCache

	// Amount of time to wait for the bind operation to succeed
	bindTimeout time.Duration

	translator InTreeToCSITranslator

	csiDriverLister          storagelisters.CSIDriverLister
	csiStorageCapacityLister storagelisters.CSIStorageCapacityLister
}
```

`pvCache` 额外按 StorageClass 建了索引（`assume_cache.go` 的 `storageClassIndex`），Filter 找候选 PV 时按 class 取，不必遍历全部 PV。assume cache 是整个插件能工作的关键，7.2 节单独讲。

数据源和缓存都齐了，下一个问题是它们怎么在调度周期里被共享。

## 三、CycleState：一个被并发写的状态

这个插件的状态和前几个都不一样：它会在调度过程中被并发写入，而不是 PreFilter 写好后只读。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
// the state is initialized in PreFilter phase. because we save the pointer in
// fwk.CycleState, in the later phases we don't need to call Write method
// to update the value
type stateData struct {
	allBound bool
	// podVolumesByNode holds the pod's volume information found in the Filter
	// phase for each node
	// it's initialized in the PreFilter phase
	podVolumesByNode map[string]*PodVolumes
	podVolumeClaims  *PodVolumeClaims
	// hasStaticBindings declares whether the pod contains one or more StaticBinding.
	// If not, vloumeBinding will skip score extension point.
	hasStaticBindings bool
	sync.Mutex
}

func (d *stateData) Clone() fwk.StateData {
	return d
}
```

大多数插件的 CycleState 在 PreFilter 写好后只读，这个不同：Filter 在多个 goroutine 里对不同节点并行执行，每个都要把自己算出的 `PodVolumes` 写进 `podVolumesByNode`。所以结构体内嵌了一把 `sync.Mutex`（第 15 行）。

`Clone`（第 18–20 行）直接返回自身指针，意味着克隆出的 CycleState 和原件共享同一份数据。抢占模拟、被提名 Pod 的 Filter 都会克隆 CycleState，它们写进来的 `podVolumesByNode` 条目也落在同一个 map 上，所以锁必须放在 `stateData` 里而不能依赖 CycleState 自己的同步（Filter 里的注释原话：「the `CycleState` may be duplicated, so we must use a local lock here」）。

共享是安全的，因为同一节点无论在哪条路径上算，结果都一样，后写覆盖先写不改变语义。

`PodVolumes` 就是「对某个节点的绑定方案」（第 5、7 行两个字段）：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
type PodVolumes struct {
	// StaticBindings are binding decisions for PVCs which can be bound to
	// pre-provisioned static PVs.
	StaticBindings []*BindingInfo
	// DynamicProvisions are PVCs that require dynamic provisioning
	DynamicProvisions []*DynamicProvision
}
```

`BindingInfo` 是一对 `pvc`/`pv`；`DynamicProvision` 是一个 PVC 加上它在这个节点上匹配到的 `CSIStorageCapacity`（可能为 nil）。

状态的结构决定了后面每个阶段要往里写什么。第一个写它的是 PreFilter。

## 四、PreFilter：先把 PVC 分好类

PreFilter 做两件事：

1. 把 Pod 引用的 PVC 分成三类（第 11 行调用 `GetPodVolumeClaims`）；
2. 把这个 Pod 可能用得上的 PV 提前取出来。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
func (pl *VolumeBinding) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// ...
	// If pod does not reference any PVC, we don't need to do anything.
	if hasPVC, err := pl.podHasPVCs(pod); err != nil {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
	} else if !hasPVC {
		state.Write(stateKey, &stateData{})
		return nil, fwk.NewStatus(fwk.Skip)
	}
	podVolumeClaims, err := pl.Binder.GetPodVolumeClaims(logger, pod)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	if len(podVolumeClaims.unboundClaimsImmediate) > 0 {
		// Return UnschedulableAndUnresolvable error if immediate claims are
		// not bound. Pod will be moved to active/backoff queues once these
		// claims are bound by PV controller.
		status := fwk.NewStatus(fwk.UnschedulableAndUnresolvable)
		status.AppendReason("pod has unbound immediate PersistentVolumeClaims")
		return nil, status
	}
	state.Write(stateKey, &stateData{
		podVolumesByNode: make(map[string]*PodVolumes),
		podVolumeClaims: &PodVolumeClaims{
			boundClaims:                podVolumeClaims.boundClaims,
			unboundClaimsDelayBinding:  podVolumeClaims.unboundClaimsDelayBinding,
			unboundVolumesDelayBinding: podVolumeClaims.unboundVolumesDelayBinding,
		},
	})
	return nil, nil
}
```

### 4.1 podHasPVCs：几种立即失败的情况

`podHasPVCs` 遍历 `persistentVolumeClaim` 和 `ephemeral` 两类卷，从 PVCLister 取 PVC，遇到以下情况返回错误，PreFilter 包成 `UnschedulableAndUnresolvable`：

- PVC 不存在。generic ephemeral 卷的 PVC 由 ephemeral 控制器在 Pod 创建后才建，这时错误被改写成 `waiting for ephemeral volume controller to create the persistentvolumeclaim "..."`，让人一眼看出是正常的等待；
- PVC 的 `status.phase` 是 `Lost`（绑定的 PV 不存在了）；
- PVC 带 `deletionTimestamp`，正在删除；
- ephemeral 卷的 PVC 不归这个 Pod 所有（`ephemeral.VolumeIsForPod`）。

普通 PVC 不存在时，上一篇说过 `VolumeRestrictions` 在 PreFilter 链上更早，会先报错并终止 PreFilter，所以实际看到这段 VolumeBinding 报错的，主要是 ephemeral 卷。

Pod 没有任何 PVC 时，PreFilter 写入一个**空的** `stateData` 再返回 `Skip`（第 7–10 行）。为什么 Skip 了还要写状态？因为 `Skip` 只让框架跳过本插件的 Filter（以及 PreFilterExtensions），Reserve、PreBind、Unreserve 仍然会被调用，它们第一步都是 `getStateData`，读不到就会返回错误，让 Pod 绑定失败。

空状态里 `podVolumesByNode` 为 nil，Reserve 查不到节点条目，会把 `allBound` 设成 true，后面一路放行。

### 4.2 GetPodVolumeClaims：三类 PVC

分类只围绕一个问题：这个 PVC 该由谁来负责绑定。分不清这一点的 PVC 会直接被拒在 PreFilter。

{{< figure src="/images/posts/sched-plugins/11-classify-filter.svg" caption="左：PreFilter 把 PVC 分成三类；右：Filter 对每个节点调用 FindPodVolumes 的决策路径与失败原因（点击查看大图）" link="/images/posts/sched-plugins/11-classify-filter.svg" >}}

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
	for _, vol := range pod.Spec.Volumes {
		volumeBound, pvc, err := b.isVolumeBound(logger, pod, &vol)
		// ...
		if volumeBound {
			podVolumeClaims.boundClaims = append(podVolumeClaims.boundClaims, pvc)
		} else {
			delayBindingMode, err := volume.IsDelayBindingMode(pvc, b.classLister)
			if err != nil {
				return podVolumeClaims, err
			}
			// Prebound PVCs are treated as unbound immediate binding
			if delayBindingMode && pvc.Spec.VolumeName == "" {
				// Scheduler path
				podVolumeClaims.unboundClaimsDelayBinding = append(podVolumeClaims.unboundClaimsDelayBinding, pvc)
			} else {
				// !delayBindingMode || pvc.Spec.VolumeName != ""
				// Immediate binding should have already been bound
				podVolumeClaims.unboundClaimsImmediate = append(podVolumeClaims.unboundClaimsImmediate, pvc)
			}
		}
	}

	podVolumeClaims.unboundVolumesDelayBinding = map[string][]*v1.PersistentVolume{}
	for _, pvc := range podVolumeClaims.unboundClaimsDelayBinding {
		// Get storage class name from each PVC
		storageClassName := volume.GetPersistentVolumeClaimClass(pvc)
		pvs, err := b.pvCache.ListPVs(storageClassName)
		// ...
		podVolumeClaims.unboundVolumesDelayBinding[storageClassName] = pvs
	}
```

「已绑定」的判定是 `isPVCFullyBound`：`spec.volumeName` 非空且带有 `pv.kubernetes.io/bind-completed` 注解。注意这里取 PVC 用的是 `pvcCache`，会看到 assume 过的版本。

未完全绑定的 PVC 再按 StorageClass 的 `volumeBindingMode` 分：

- `WaitForFirstConsumer` 且 `volumeName` 为空：进 `unboundClaimsDelayBinding`（第 13–15 行），这是调度器要接手的；
- 其它情况，包括 `Immediate` 模式，以及 `volumeName` 已填但还没 bind-completed 的预绑定 PVC：进 `unboundClaimsImmediate`（第 16–20 行），意思是「这应该由 PV 控制器完成，调度器只等」。

只要有一个 `unboundClaimsImmediate`，PreFilter 就返回 `UnschedulableAndUnresolvable`（前面 PreFilter 那个块的第 15–22 行），原因 `pod has unbound immediate PersistentVolumeClaims`。

`IsDelayBindingMode` 有个容易踩的细节：PVC 的 StorageClass **不存在**时它返回 false，没有报错。于是「StorageClass 名字写错了」的 WaitForFirstConsumer PVC 会被当成 Immediate 处理，报的也是 unbound immediate。

而 StorageClass 存在但 `volumeBindingMode` 为 nil 时才会返回错误（正常情况下 API 默认值会填上 `Immediate`）。

最后，对每个延迟绑定 PVC 的 StorageClass，从 `pvCache` 按索引取出同 class 的所有 PV，存进 `unboundVolumesDelayBinding`（第 24–31 行）。这一步放在 PreFilter 而不是 Filter，是因为 Filter 要对每个节点执行，候选 PV 列表与节点无关，取一次就够了。

候选 PV 拿到之后，Filter 就可以逐个节点判断了。

## 五、Filter：为每个节点设计一份绑定方案

Filter 对每个候选节点做两步：

1. 算一份方案（第 4 行的 `FindPodVolumes`）；
2. 写进状态里留给 Reserve 使用（第 19–22 行）。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
func (pl *VolumeBinding) Filter(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	// ...
	podVolumes, reasons, err := pl.Binder.FindPodVolumes(logger, pod, state.podVolumeClaims, node)

	if err != nil {
		return fwk.AsStatus(err)
	}

	if len(reasons) > 0 {
		status := fwk.NewStatus(fwk.UnschedulableAndUnresolvable)
		for _, reason := range reasons {
			status.AppendReason(string(reason))
		}
		return status
	}

	// multiple goroutines call `Filter` on different nodes simultaneously and the `CycleState` may be duplicated, so we must use a local lock here
	state.Lock()
	state.podVolumesByNode[node.Name] = podVolumes
	state.hasStaticBindings = state.hasStaticBindings || (podVolumes != nil && len(podVolumes.StaticBindings) > 0)
	state.Unlock()
	return nil
}
```

所有失败原因一律 `UnschedulableAndUnresolvable`（第 11 行）。存储不匹配不是驱逐 Pod 能解决的，DefaultPreemption 会直接跳过这些节点。

`FindPodVolumes` 的结果分两层处理：

- 用四个布尔变量记录结果（第 2–5 行）；
- 在 defer 里统一翻译成原因字符串（第 6–22 行）。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
	unboundVolumesSatisfied := true
	boundVolumesSatisfied := true
	sufficientStorage := true
	boundPVsFound := true
	defer func() {
		if err != nil {
			return
		}
		if !boundVolumesSatisfied {
			reasons = append(reasons, ErrReasonNodeConflict)
		}
		if !unboundVolumesSatisfied {
			reasons = append(reasons, ErrReasonBindConflict)
		}
		if !sufficientStorage {
			reasons = append(reasons, ErrReasonNotEnoughSpace)
		}
		if !boundPVsFound {
			reasons = append(reasons, ErrReasonPVNotExist)
		}
	}()
```

| 变量 | 原因字符串 | 典型场景 |
| --- | --- | --- |
| `boundVolumesSatisfied` | `node(s) didn't match PersistentVolume's node affinity` | 已绑定的 PV 是 zone-a 的盘，节点在 zone-b |
| `unboundVolumesSatisfied` | `node(s) didn't find available persistent volumes to bind` | 找不到可用 PV 且不能动态供给 |
| `sufficientStorage` | `node(s) did not have enough free storage` | CSIStorageCapacity 显示容量不足 |
| `boundPVsFound` | `node(s) unavailable due to one or more pvc(s) bound to non-existent pv(s)` | PVC 指向的 PV 已被删除 |

### 5.1 已绑定的 PVC：只检查 PV 的 nodeAffinity

已绑定的 PVC 只需要回答一个问题：这块 PV 的 `nodeAffinity` 允许这个节点吗？对应两处代码：

1. 第 17 行用 `CheckNodeAffinity` 检查；
2. 不允许时在第 20 行返回失败。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
	for _, pvc := range claims {
		pvName := pvc.Spec.VolumeName
		pv, err := b.pvCache.Get(pvName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				err = nil
			}
			return true, false, err
		}

		pv, err = b.tryTranslatePVToCSI(logger, pv, csiNode)
		if err != nil {
			return false, true, err
		}

		err = volume.CheckNodeAffinity(pv, node.Labels)
		if err != nil {
			// ...
			return false, true, nil
		}
		// ...
	}
```

这是调度器里**唯一**检查 PV `spec.nodeAffinity` 的地方。`NodeAffinity` 插件只看 Pod 自己的亲和性，不看 PV；`VolumeZone` 只看 PV 的 label。很多人以为 local PV 的节点约束是 NodeAffinity 插件处理的，其实是这里。

`tryTranslatePVToCSI` 在三个条件都满足时把 in-tree PV 翻译成 CSI 形式：

- PV 可迁移；
- 插件属于写死的五个块存储插件之一（`isCSIMigrationOnForPlugin`）；
- 节点 CSINode 的 `storage.alpha.kubernetes.io/migrated-plugins` 注解里包含这个插件。

翻译会改写拓扑，比如 GCE PD 的 `topology.kubernetes.io/zone` 被替换成驱动自己的 `topology.gke.io/zone`（`csi-translation-lib/plugins/in_tree_volume.go` 的 `translateTopologyFromInTreeToCSI`）。节点上的 CSI 驱动注册时会给节点打上驱动的拓扑标签，所以翻译后的 nodeAffinity 匹配的是驱动视角的拓扑。

### 5.2 未绑定的 PVC：先看 selected-node，再找静态 PV，最后看能否供给

未绑定的 PVC 走三步：

1. 选中节点的快速路径（第 3–14 行）；
2. 静态 PV 匹配（第 17–24 行）；
3. 动态供给检查（第 28–33 行）。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
		// Filter out claims to provision
		for _, claim := range podVolumeClaims.unboundClaimsDelayBinding {
			if selectedNode, ok := claim.Annotations[volume.AnnSelectedNode]; ok {
				if selectedNode != node.Name {
					// Fast path, skip unmatched node.
					unboundVolumesSatisfied = false
					return
				}
				claimsToProvision = append(claimsToProvision, claim)
			} else {
				claimsToFindMatching = append(claimsToFindMatching, claim)
			}
		}

		// Find matching volumes
		if len(claimsToFindMatching) > 0 {
			var unboundClaims []*v1.PersistentVolumeClaim
			unboundVolumesSatisfied, staticBindings, unboundClaims, err = b.findMatchingVolumes(logger, pod, claimsToFindMatching, podVolumeClaims.unboundVolumesDelayBinding, node)
			if err != nil {
				return
			}
			claimsToProvision = append(claimsToProvision, unboundClaims...)
		}

		// Check for claims to provision. This is the first time where we potentially
		// find out that storage is not sufficient for the node.
		if len(claimsToProvision) > 0 {
			unboundVolumesSatisfied, sufficientStorage, dynamicProvisions, err = b.checkVolumeProvisions(logger, pod, claimsToProvision, node)
			if err != nil {
				return
			}
		}
```

第一步看 PVC 上有没有 `volume.kubernetes.io/selected-node` 注解（第 4–9 行）。这个注解是调度器在上一次尝试中写上去的（见第七节），表示「已经决定在某节点供给」。有注解的 PVC 只能去那个节点，其它节点直接判失败。这条快速路径的后果在第十节讨论。

第二步 `findMatchingVolumes` 找静态 PV：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
	// Sort all the claims by increasing size request to get the smallest fits
	sort.Sort(byPVCSize(claimsToBind))

	chosenPVs := map[string]*v1.PersistentVolume{}

	foundMatches = true

	for _, pvc := range claimsToBind {
		// Get storage class name from each PVC
		storageClassName := volume.GetPersistentVolumeClaimClass(pvc)
		pvs := unboundVolumesDelayBinding[storageClassName]

		// Find a matching PV
		pv, err := volume.FindMatchingVolume(pvc, pvs, node, chosenPVs, true, b.enableVolumeAttributesClass)
		if err != nil {
			return false, nil, nil, err
		}
		if pv == nil {
			// ...
			unboundClaims = append(unboundClaims, pvc)
			foundMatches = false
			continue
		}

		// matching PV needs to be excluded so we don't select it again
		chosenPVs[pv.Name] = pv
		bindings = append(bindings, &BindingInfo{pv: pv, pvc: pvc})
		// ...
	}
```

挑选过程有两个要点：

- PVC 按请求容量从小到大排序（第 3 行），依次挑选；
- `chosenPVs`（第 5 行声明，第 27 行写入）保证同一个 Pod 的两个 PVC 不会挑到同一块 PV。

从小到大排是贪心策略：小 PVC 先挑走小 PV，大 PV 留给大 PVC，降低「大 PV 被小 PVC 占了、大 PVC 无盘可用」的概率。

`FindMatchingVolume`（`component-helpers/storage/volume/pv_helpers.go`）是 PV 控制器和调度器共用的匹配函数，条件依次是：

- 不在排除列表；
- `claimRef` 为空或者就是这个 PVC；
- 容量不小于请求，`volumeMode` 和 `volumeAttributesClassName` 一致；
- 没有在删除，`phase` 为 `Available`；
- PV 的 `nodeAffinity` 允许这个节点；
- label 满足 PVC 的 `selector`；
- StorageClass 相同，access mode 满足。

满足条件的 PV 里取容量最小的一块。

如果有一块 PV 的 `claimRef` 恰好指向这个 PVC（人为预先指定的绑定），它直接胜出，但只要它的 nodeAffinity 不允许当前节点，整个匹配就返回 nil。

注意 `foundMatches = false`（第 21–23 行）之后循环并没有中断，没匹配上的 PVC 被收进 `unboundClaims`，接着进入第三步。`unboundVolumesSatisfied` 会被 `checkVolumeProvisions` 的返回值覆盖。所以「静态 PV 不够」并不立即失败，只要剩下的 PVC 能动态供给，节点依然可行。

同一个 Pod 的多个 PVC 可以一部分绑静态 PV、一部分动态供给。

第三步 `checkVolumeProvisions` 判断剩下的 PVC 能否在这个节点上动态供给：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
	for _, claim := range claimsToProvision {
		pvcName := getPVCName(claim)
		className := volume.GetPersistentVolumeClaimClass(claim)
		if className == "" {
			return false, false, nil, fmt.Errorf("no class for claim %q", pvcName)
		}

		class, err := b.classLister.Get(className)
		if err != nil {
			return false, false, nil, fmt.Errorf("failed to find storage class %q", className)
		}
		provisioner := class.Provisioner
		if provisioner == "" || provisioner == volume.NotSupportedProvisioner {
			// ...
			return false, true, nil, nil
		}

		// Check if the node can satisfy the topology requirement in the class
		if !v1helper.MatchTopologySelectorTerms(class.AllowedTopologies, labels.Set(node.Labels)) {
			// ...
			return false, true, nil, nil
		}

		// Check storage capacity.
		sufficient, capacity, err := b.hasEnoughCapacity(logger, provisioner, claim, class, node)
		if err != nil {
			return false, false, nil, err
		}
		if !sufficient {
			// hasEnoughCapacity logs an explanation.
			return true, false, nil, nil
		}

		dynamicProvisions = append(dynamicProvisions, &DynamicProvision{
			PVC:          claim,
			NodeCapacity: capacity,
		})
	}
```

三道关卡：

1. StorageClass 必须有 provisioner，且不能是 `kubernetes.io/no-provisioner`（local PV 常用的「不支持动态供给」标记，第 13–17 行）；
2. 节点标签要满足 StorageClass 的 `allowedTopologies`（第 20–23 行）；
3. 容量检查（第 26–33 行，下一小节）。

失败时的原因分两种：

- 前两关失败对应 `didn't find available persistent volumes to bind`；
- 第三关失败返回 `(true, false)`，只报 `did not have enough free storage`，不报 BindConflict。

StorageClass 不存在、PVC 没有 class 则作为 error 返回（第 5–12 行），Filter 给出的是 `Error` 状态，Pod 进 backoff 队列重试，而不是 unschedulable。

### 5.3 CSIStorageCapacity：容量感知的动态供给

第三关的容量检查依赖 `CSIStorageCapacity` 对象，它只在驱动主动开启时才起作用。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
func (b *volumeBinder) hasEnoughCapacity(logger klog.Logger, provisioner string, claim *v1.PersistentVolumeClaim, storageClass *storagev1.StorageClass, node *v1.Node) (bool, *storagev1.CSIStorageCapacity, error) {
	quantity, ok := claim.Spec.Resources.Requests[v1.ResourceStorage]
	if !ok {
		// No capacity to check for.
		return true, nil, nil
	}

	// Only enabled for CSI drivers which opt into it.
	driver, err := b.csiDriverLister.Get(provisioner)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// ...
			return true, nil, nil
		}
		return false, nil, err
	}
	if driver.Spec.StorageCapacity == nil || !*driver.Spec.StorageCapacity {
		return true, nil, nil
	}

	// Look for a matching CSIStorageCapacity object(s).
	// TODO (for beta): benchmark this and potentially introduce some kind of lookup structure (https://github.com/kubernetes/enhancements/issues/1698#issuecomment-654356718).
	capacities, err := b.csiStorageCapacityLister.List(labels.Everything())
	if err != nil {
		return false, nil, err
	}

	for _, capacity := range capacities {
		if capacity.StorageClassName == storageClass.Name &&
			capacitySufficient(capacity, quantity) &&
			b.nodeHasAccess(logger, node, capacity) {
			// Enough capacity found.
			return true, capacity, nil
		}
	}
	// ...
	return false, nil, nil
}
```

容量检查是驱动 opt-in 的：CSIDriver 对象的 `spec.storageCapacity: true` 才启用（第 18–20 行）。启用后由 external-provisioner 的容量控制器按「StorageClass × 拓扑段」周期调用驱动的 `GetCapacity`，发布 `CSIStorageCapacity` 对象：

```yaml {linenos=inline}
apiVersion: storage.k8s.io/v1
kind: CSIStorageCapacity
metadata:
  name: csisc-7xk2p
  namespace: kube-system
storageClassName: topolvm-ssd
nodeTopology:
  matchLabels:
    topology.topolvm.io/node: node-1
capacity: 500Gi
maximumVolumeSize: 200Gi
```

匹配条件三个，就是上面 `hasEnoughCapacity` 那个块第 30–32 行的三个条件：

- `storageClassName` 相同；
- `volumeLimit` 不小于请求，其中 `maximumVolumeSize` 优先，没有才用 `capacity`（总剩余空间不等于能建的最大单卷，比如 LVM 卷组碎片化之后）；
- `nodeTopology` 这个 label selector 匹配节点标签，`nodeTopology` 为 nil 视为不可访问。

只要找到一个满足的对象就返回，这个对象会作为 `NodeCapacity` 记进 `DynamicProvision`，供打分使用。

实现上是在 `hasEnoughCapacity` 里对全体 `CSIStorageCapacity` 线性扫描（同一块第 24–36 行），源码里关于「beta 前做基准测试、考虑加索引」的 TODO 一直还在。对象数量是 StorageClass 数 × 拓扑段数，节点级拓扑（如 TopoLVM）的大集群里，这会让每个节点的 Filter 都扫描一遍上千个对象。

还要注意它只判断单个 PVC 能否放下，**不累加**同一 Pod 的多个 PVC，也不考虑其它正在供给中的 PVC。容量数据本身也是周期刷新的快照，所以它是「降低失败率」而不是「保证成功」，真正的兜底是供给失败后的重调度（第八节）。

到这里，「这个节点能不能放」已经有了答案。接下来是在全部可行的节点之间排序。

## 六、Score：存储利用率打分

Score 只在 `StorageCapacityScoring` 打开时才生效，衡量的是节点上的存储余量。

### 6.1 PreScore 与 Score

PreScore 只判断要不要进入打分，Score 才真正计算。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
func (pl *VolumeBinding) PreScore(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	if pl.scorer == nil {
		return fwk.NewStatus(fwk.Skip)
	}
	// ...
	if state.hasStaticBindings || pl.fts.EnableStorageCapacityScoring {
		return nil
	}
	return fwk.NewStatus(fwk.Skip)
}
```

`scorer` 只在 `New` 里 gate 开启时才构造，所以 PreScore 的行为只取决于 gate：

- gate 关闭时 PreScore 直接 Skip（第 3–5 行）；
- gate 开启时第二个条件恒为真，PreScore 总是放行。

`hasStaticBindings` 这个条件（第 7 行）是 alpha 时期「只给静态绑定打分」的遗留，现在已经不起作用。没有 PVC 的 Pod 也会进入 Score，但 `podVolumesByNode` 里没有任何节点，每个节点都得 0 分，不影响排序。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
	classResources := make(classResourceMap)
	if len(podVolumes.StaticBindings) != 0 || !pl.fts.EnableStorageCapacityScoring {
		// group static binding volumes by storage class
		for _, staticBinding := range podVolumes.StaticBindings {
			class := staticBinding.StorageClassName()
			storageResource := staticBinding.StorageResource()
			// ...
			classResources[class].Requested += storageResource.Requested
			classResources[class].Capacity += storageResource.Capacity
		}
	} else {
		// group dynamic binding volumes by storage class
		for _, provision := range podVolumes.DynamicProvisions {
			if provision.NodeCapacity == nil {
				continue
			}
			class := *provision.PVC.Spec.StorageClassName
			// ...
			// The following line cannot be +=. For example, if a Pod requests two 50GB volumes from
			// a StorageClass with 100GB of capacity on a node, this part of the code will be executed twice.
			// In that case, using += would incorrectly set classResources[class].Capacity to 200GB.
			classResources[class].Capacity = provision.NodeCapacity.Capacity.Value()
			requestedQty := provision.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
			classResources[class].Requested += requestedQty.Value()
		}
	}

	return pl.scorer(classResources), nil
```

按 StorageClass 分组，计算每组的「请求 / 容量」。第 3 行按两种模式分流，算完后在第 29 行交给 `pl.scorer`：

- 有静态绑定时只看静态绑定（第 5–11 行）：请求是 PVC 请求之和，容量是被选中 PV 的容量之和。动态供给部分被忽略。
- 没有静态绑定时看动态供给（第 14–26 行）：容量取 `CSIStorageCapacity.capacity`（注意是 `capacity` 不是 `maximumVolumeSize`），用赋值（第 23 行）而非累加，因为同一个 class 的多个 PVC 匹配到的是同一份节点容量；请求累加（第 25 行）。`NodeCapacity` 为 nil（驱动没开容量跟踪）的 PVC 不参与。

### 6.2 shape 与打分函数

打分函数把每组的「请求 / 容量」换算成利用率，再走一遍 shape 折线。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/scorer.go
func buildScorerFunction(scoringFunctionShape helper.FunctionShape) volumeCapacityScorer {
	rawScoringFunction := helper.BuildBrokenLinearFunction(scoringFunctionShape)
	f := func(requested, capacity int64) int64 {
		if capacity == 0 || requested > capacity {
			return rawScoringFunction(maxUtilization)
		}

		return rawScoringFunction(requested * maxUtilization / capacity)
	}
	return func(classResources classResourceMap) int64 {
		var nodeScore int64
		// in alpha stage, all classes have the same weight
		weightSum := len(classResources)
		if weightSum == 0 {
			return 0
		}
		for _, resource := range classResources {
			classScore := f(resource.Requested, resource.Capacity)
			nodeScore += classScore
		}
		return int64(math.Round(float64(nodeScore) / float64(weightSum)))
	}
}
```

`New` 构造 shape 时把配置里 0~10 的分数乘以 `MaxNodeScore / MaxCustomPriorityScore`（100 / 10 = 10），映射到 0~100。

计算分三步：

1. 利用率 = `requested * 100 / capacity`（第 9 行），整数除法向下取整；
2. 在 shape 的折线上线性插值得到 class 分；
3. 多个 class 取算术平均再四舍五入（第 22 行）。

插件没有 `ScoreExtensions`，不做 NormalizeScore，分数本身已经在 0~100。

算例一：默认 shape、动态供给。默认 shape 映射后是 (0, 100) → (100, 0)，即「分数 = 100 − 利用率」。Pod 有一个 10Gi 的 PVC，三个节点的 `CSIStorageCapacity.capacity` 分别为 100Gi、50Gi、10Gi：

| 节点 | 利用率 | 分数 |
| --- | --- | --- |
| node-a | 10 × 100 / 100 = 10 | 100 − 10 = 90 |
| node-b | 10 × 100 / 50 = 20 | 100 − 20 = 80 |
| node-c | 10 × 100 / 10 = 100 | 0 |

剩余空间越多的节点分越高，这是在做分散：把新卷放到更空的存储池，给后续的大卷留余地，也平衡各节点的 IO。这组数字与 `volume_binding_test.go` 中「storage capacity score」用例的期望一致。

算例二：自定义 shape、静态绑定。配置 shape 为 (0, 0)、(50, 3)、(100, 5)，映射后为 (0, 0)、(50, 30)、(100, 50)。Pod 请求 50Gi，zone-a 节点能匹配的最小 PV 是 200Gi，zone-b 节点是 100Gi：

- zone-a：利用率 50 × 100 / 200 = 25，落在 (0, 0)~(50, 30) 之间，插值 0 + (30 − 0) × (25 − 0) / (50 − 0) = 15；
- zone-b：利用率 50 × 100 / 100 = 50，正好在折点上，30 分。

PV 越贴合请求的节点分越高，这是紧凑策略，避免小 PVC 占用大 PV 造成浪费。它与测试用例「static binding」期望的 15、15、30、30 一致。

**两个例子说明 shape 的方向决定了策略**。默认 shape 对动态供给合理，但它同样作用于静态绑定，会让调度器偏好「PV 比请求大得多」的节点，这与 `findMatchingVolumes` 在单个节点内「挑最小 PV」的思路正好相反。大量使用 local PV 的集群，值得显式配置一个递增的 shape。

源码里还有一处需要留意：内部类型 `pkg/scheduler/apis/config/types_pluginargs.go` 中 `Shape` 字段的注释写着默认值是「0 for 0 utilization, 10 for 100 utilization」，这是 alpha 时代的说法。

对外的 v1 类型（`staging/src/k8s.io/kube-scheduler/config/v1/types_pluginargs.go`）注释和 `defaults.go` 都是「10 for 0 utilization, 0 for 100 utilization」，以后者为准。

到这里，调度周期里的判定和打分都完成了。剩下的三个阶段都在绑定周期里。

## 七、Reserve、PreBind、Unreserve：把方案落地

调度周期在 Score 之后选出一个节点，接下来就要兑现 Filter 为它设计的那份 `PodVolumes`。

{{< figure src="/images/posts/sched-plugins/11-bind-flow.svg" caption="从 Reserve 到 Pod 绑定：调度器只写意图（PV.claimRef、PVC 的 selected-node），由 PV 控制器和 external-provisioner 执行，PreBind 轮询直到完成（点击查看大图）" link="/images/posts/sched-plugins/11-bind-flow.svg" >}}

### 7.1 Reserve：在内存里假装已经绑好

Reserve 分两种情况：

- 有方案时，把 Filter 为选中节点算出的那份方案（第 5 行）变成内存里的假设（第 7 行的 `AssumePodVolumes`）；
- 没有方案（Pod 不引用 PVC）时直接置 `allBound`（第 12–14 行）。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
func (pl *VolumeBinding) Reserve(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	// ...
	// we don't need to hold the lock as only one node will be reserved for the given pod
	podVolumes, ok := state.podVolumesByNode[nodeName]
	if ok {
		allBound, err := pl.Binder.AssumePodVolumes(klog.FromContext(ctx), pod, nodeName, podVolumes)
		if err != nil {
			return fwk.AsStatus(err)
		}
		state.allBound = allBound
	} else {
		// may not exist if the pod does not reference any PVC
		state.allBound = true
	}
	return nil
}
```

`AssumePodVolumes` 先用 `arePodVolumesBound` 检查一遍：如果此刻所有 PVC 都已经完全绑定（可能在调度期间被别人绑好了），直接返回 `allBound = true`。否则：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
	// Assume PV
	newBindings := []*BindingInfo{}
	for _, binding := range podVolumes.StaticBindings {
		newPV, dirty, err := volume.GetBindVolumeToClaim(binding.pv, binding.pvc)
		// ...
		if dirty {
			err = b.pvCache.Assume(newPV)
			if err != nil {
				b.revertAssumedPVs(newBindings)
				return false, err
			}
		}
		newBindings = append(newBindings, &BindingInfo{pv: newPV, pvc: binding.pvc})
	}

	// Assume PVCs
	newProvisionedPVCs := []*DynamicProvision{}
	for _, dynamicProvision := range podVolumes.DynamicProvisions {
		// The claims from method args can be pointing to watcher cache. We must not
		// modify these, therefore create a copy.
		claimClone := dynamicProvision.PVC.DeepCopy()
		metav1.SetMetaDataAnnotation(&claimClone.ObjectMeta, volume.AnnSelectedNode, nodeName)
		err = b.pvcCache.Assume(claimClone)
		if err != nil {
			pvcs := convertDynamicProvisionsToPVCs(newProvisionedPVCs)
			b.revertAssumedPVs(newBindings)
			b.revertAssumedPVCs(pvcs)
			return
		}

		newProvisionedPVCs = append(newProvisionedPVCs, &DynamicProvision{PVC: claimClone})
	}

	podVolumes.StaticBindings = newBindings
	podVolumes.DynamicProvisions = newProvisionedPVCs
	return
```

- 静态绑定（第 3–15 行）：`GetBindVolumeToClaim` 复制 PV，把 `spec.claimRef` 指向 PVC，并加上 `pv.kubernetes.io/bound-by-controller: "yes"` 注解，再放进 `pvCache`；
- 动态供给（第 18–33 行）：复制 PVC，加上 `volume.kubernetes.io/selected-node: <节点名>` 注解，再放进 `pvcCache`。

两步都只改内存，没有任何 API 调用。任何一步失败都会把之前 assume 过的东西还原，保证 Reserve 要么全成功、要么不留痕迹。最后把修改后的对象写回 `podVolumes`，PreBind 会拿它们去发 API 请求。

Reserve 是串行的，这是 assume 的前提：下一个 Pod 的调度周期开始之前，这个 Pod 的 assume 已经完成。

### 7.2 assume cache：版本号决定谁说了算

{{< figure src="/images/posts/sched-plugins/11-assume-cache.svg" caption="assume cache 的读写规则：informer store 是真相，assumed 只在 ResourceVersion 相同时生效，informer 一旦送来新版本就自动作废（点击查看大图）" link="/images/posts/sched-plugins/11-assume-cache.svg" >}}

实现在 `staging/src/k8s.io/component-helpers/storage/volume/assumecache/assume_cache.go`，是一层叠在 informer 之上的覆盖层：

```go {linenos=inline}
// staging/src/k8s.io/component-helpers/storage/volume/assumecache/assume_cache.go
type AssumeCache[T v1.Object] struct {
	// ...
	rwMutex sync.RWMutex

	// Objects from informer
	store   cache.Indexer
	assumed map[string]assumedObject[T]
}

type assumedObject[T v1.Object] struct {
	object T
	// Whether object was already written to the apiserver.
	persisted bool
}

func (a assumedObject[T]) preferOver(storeRV string) bool {
	assumedRV := a.object.GetResourceVersion()
	if !a.persisted {
		// Keep the optimistic object, skip resync.
		return assumedRV == storeRV
	}
	// ...
	// Keep the written object until the informer reaches its version.
	return cmp > 0
}
```

规则可以归纳成四条：

1. Assume 要求版本一致。`Assume(obj)` 先从 informer store 取当前对象，ResourceVersion 不同就报错 `"%q is out of sync (stored: %s, assume: %s)"`。也就是说，只能在「最新版本」上做假设，Filter 时看到的 PV 如果在 Reserve 前被别人改过，Reserve 会失败，Pod 带着 Error 重新排队。
2. 读的时候覆盖。`Get`、`ByIndex` 先取 store 里的对象，若 `assumed` 里有同名条目且 `preferOver(storeRV)` 为真，就返回假设版本。scheduler 用的 `Assume` 写入的是 `persisted: false`，条件是「假设版本号 == store 版本号」（第 19–22 行）。
3. informer 一来就作废。cache 在 informer 上注册了 Add/Update/Delete 回调，每次事件都调用 `mayExpire`，store 里的版本号变了（或者对象被删），假设就被丢弃。之后读到的是 informer 送来的真实对象，正常情况下它已经包含了 PreBind 写进去的改动。
4. Restore 只撤销自己那一版。`Restore(obj)` 只在 `assumed` 中条目的版本号与传入对象相同时才删除，防止撤销掉别人后来写入的假设。

注意注释里专门写了：索引是按 store 里的对象计算的，假设不会改变索引。PV 的 StorageClass 在调度器里从不被修改，所以按 class 索引没有问题（`ListPVs` 的注释也说了这一点）。

assume 解决的是**相邻两个调度周期之间的竞态**。Pod A 在 Reserve 中假设 `pv-1` 绑给了 `pvc-a`，进入异步的绑定周期；PreBind 的 API 请求可能还没发出，Pod B 的调度周期就开始了。B 在 PreFilter 里调用 `ListPVs`，拿到的 `pv-1` 已经带着指向 `pvc-a` 的 `claimRef`，`FindMatchingVolume` 里「`claimRef` 非空且不是我」这一条会把它排除。

动态供给同理，第二个使用同一 PVC 的 Pod 会从 `pvcCache` 读到带 `selected-node` 的版本，只能被调度到同一个节点。没有这层 cache，两个 Pod 会在 informer 追上之前抢到同一块 PV，其中一个必然在 PreBind 失败。

这和调度器自己的 Pod assume（`backend/cache` 里的 `AssumePod`）是同一个思想，只是作用对象从 Pod 换成了 PV 和 PVC。

DynamicResources 插件对 ResourceClaim 做的是同样的事，但用的是另一份实现 `pkg/scheduler/util/assumecache`。component-helpers 版本的类型注释写明了两者的区别：这一份不派发事件，并且总是与 informer 保持一致。

### 7.3 PreBindPreFlight 与 PreBind：发请求、等结果

这两个扩展点是真正和 API 打交道的两步。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
func (pl *VolumeBinding) PreBindPreFlight(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	result := &fwk.PreBindPreFlightResult{AllowParallel: true}
	s, err := getStateData(state)
	if err != nil {
		return result, fwk.AsStatus(err)
	}
	if s.allBound {
		// no need to bind volumes
		return result, fwk.NewStatus(fwk.Skip)
	}
	// ...
	return result, nil
}
```

`PreBindPreFlight` 分两步：

1. 第 3–7 行准备好 `result` 并取出状态；
2. 第 8–11 行对已全部绑定的情况返回 `Skip`。

它在绑定周期的开头调用（`NominatedNodeNameForExpectation` gate 开启时，1.35 起 beta 默认开启）。它不做实际操作，只告诉框架两件事：

- 返回 `Skip`（所有卷已绑定）时，框架跳过本插件的 PreBind；返回 Success 时，调度器会先把 `nominatedNodeName` 写到 Pod 上，告诉 cluster-autoscaler 等外部组件「这个 Pod 马上要绑到这个节点，只是还在等卷」，避免它们在 PreBind 漫长的等待期里误判 Pod 无处可去；
- `AllowParallel: true`（第 3 行）表示它的 PreBind 可以和其它同样声明并行的 PreBind 插件（比如 DynamicResources）一起跑。

PreBind 本身：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/binder.go
func (b *volumeBinder) BindPodVolumes(ctx context.Context, assumedPod *v1.Pod, podVolumes *PodVolumes) (err error) {
	// ...
	bindings := podVolumes.StaticBindings
	claimsToProvision := convertDynamicProvisionsToPVCs(podVolumes.DynamicProvisions)

	// Start API operations
	err = b.bindAPIUpdate(ctx, assumedPod, bindings, claimsToProvision)
	if err != nil {
		return err
	}

	err = wait.PollUntilContextTimeout(ctx, time.Second, b.bindTimeout, false, func(ctx context.Context) (bool, error) {
		b, err := b.checkBindings(logger, assumedPod, bindings, claimsToProvision)
		return b, err
	})
	if err != nil {
		return fmt.Errorf("binding volumes: %w", err)
	}
	return nil
}
```

分两段。`bindAPIUpdate`（第 8 行）把 Reserve 准备好的对象逐个 `Update` 到 API server，顺序是：

1. 先更新 PV（写 `claimRef`）；
2. 再更新 PVC（写 `selected-node`）。

用 `lastProcessedBinding`、`lastProcessedProvisioning` 两个计数记录已经成功的数量，出错时 defer 只撤销**还没写成功**的那些假设；已经写进 API 的不回滚，注释直白地写着「There is no API rollback if the actual binding fails」。

然后 `checkBindings` 每秒轮询一次（第 13–16 行），最长 `bindTimeout`（默认 600 秒）。它读的是 `GetAPIObj`，即 informer store 里的真实版本而不是假设版本，原因注释里也说了：假设版本可能在主调度循环里被再次覆盖，只有 API 状态才和 PV 控制器、供给器共享。检查项包括：

- Pod 还在不在（被删了立即报错退出，不必再等）；
- informer 里的对象版本是否已经追上刚才的写入（`CompareResourceVersion`，没追上就继续等）；
- 静态绑定：PV 的 `claimRef` 是否被 PV 控制器清掉（清掉说明绑定被拒绝），PV 的 nodeAffinity 是否仍匹配节点，PVC 是否已经完全绑定；
- 动态供给：PVC 的 `selected-node` 是否仍等于 Pod 所在节点。**供给器放弃时会删掉这个注解**，这里看到注解消失就报 `provisioning failed for PVC`；PVC 绑上 PV 后，再检查新 PV 的 nodeAffinity 是否匹配。

全部 PVC 完全绑定才返回 true。PreBind 失败（包括超时）返回 `Error` 状态，按 `schedule_one.go` 里 `bindingCycle` 的约定，Permit 之后的失败一律不算 unschedulable，Pod 进 backoff 队列重试。

### 7.4 Unreserve

Unreserve 是 PreBind 的反向操作，撤销这份方案里所有没落地的假设。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
func (pl *VolumeBinding) Unreserve(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) {
	s, err := getStateData(cs)
	if err != nil {
		return
	}
	// we don't need to hold the lock as only one node may be unreserved
	podVolumes, ok := s.podVolumesByNode[nodeName]
	if !ok {
		return
	}
	pl.Binder.RevertAssumedPodVolumes(podVolumes)
}
```

Reserve 之后的任何一步失败（Permit 拒绝、PreBind 出错、Bind 出错），框架都会调用 Unreserve，它把这份方案里所有 PV、PVC 的假设撤销。

结合「Restore 只撤销版本号相同的那一版」，撤销结果分两种：

- 已经成功写进 API 的 PV，`binding.pv` 已被替换成 API 返回的新对象，版本号与假设版本不同，Restore 不会动它，等 informer 送来新版本自然覆盖；
- 还没写进 API 的，假设被删除，cache 回到 informer 的视图。

`handleBindingCycleError` 在调用 Unreserve 之后，还会以 `AssignedPodDelete` 事件触发一次 `MoveAllToActiveOrBackoffQueue`，因为被 forget 的 assumed Pod 占用的资源释放了。

到这里，插件在调度器内部的工作就结束了。真正把卷绑上去的，是调度器之外的两个组件。

## 八、与 PV 控制器、external-provisioner 的接力

调度器从头到尾只写了两样东西：PV 的 `claimRef` 和 PVC 的 `selected-node` 注解。这两样都是「意图」，真正的执行在别处。

**静态绑定。** PV 的 `claimRef` 被写上之后，这块 PV 就处于「预绑定」状态。

PV 控制器的 `syncUnboundClaim` 调用同一个 `FindMatchingVolume`，在 `node == nil && delayBinding` 的情况下会跳过普通候选，但「`claimRef` 恰好指向我」的 PV 会被直接选中。

随后控制器执行 `bind`，做三件事：

1. 设置 PVC 的 `spec.volumeName`；
2. 加上 `pv.kubernetes.io/bind-completed` 注解；
3. 更新双方的 `status.phase`。

PreBind 的轮询看到 bind-completed 就结束。

**动态供给。** PVC 带上 `selected-node` 之后，流程分四步：

1. PV 控制器的 `syncUnboundClaim` 发现这是延迟绑定 PVC 且 `IsDelayBindingProvisioning` 为真，走 `provisionClaim`。对外部供给器，它的工作只是给 PVC 加上 `volume.kubernetes.io/storage-provisioner: <驱动名>` 注解（`setClaimProvisioner`），in-tree provisioner 名会换算成迁移后的 CSI 驱动名；
2. external-provisioner 里的 sig-storage-lib-external-provisioner 在 `shouldProvision` 中检查：provisioner 注解是自己、StorageClass 是 `WaitForFirstConsumer` 时，**必须**有 `selected-node` 才会动手；
3. 供给时它读取 selected node 的 `Node` 对象和 CSINode 的 `topologyKeys`，拼出 `CreateVolume` 请求的 `accessibility_requirements`：默认模式下选中节点的拓扑排在 preferred 首位，开启 `--strict-topology` 时 requisite 只包含选中节点的拓扑（external-provisioner 的 `pkg/controller/topology.go`）；
4. 驱动建好卷后，供给器创建 PV（带驱动返回的拓扑作为 `nodeAffinity`，`claimRef` 指向 PVC），PV 控制器完成绑定。

**失败回退。** 供给器调用 `CreateVolume` 失败时，用 `checkError` 判断要不要重调度。

gRPC 返回 `ResourceExhausted`（CSI 规范里的「在指定拓扑无法供给」）且 PVC 有 selected node 时，结果是 `ProvisioningReschedule`，供给器调用 `rescheduleProvisioning` **删掉 `selected-node` 注解**；selected node 对应的 Node 不存在时也一样。

调度器这边，PreBind 轮询看到注解消失，报 `provisioning failed for PVC`，Unreserve，Pod 退避后重新调度，此时 PVC 已经没有 selected-node，可以重新选节点。

其它错误码（如 `DeadlineExceeded`、`Unavailable`、`Aborted`）被视为「可能还在后台进行」，供给器保留注解继续重试；调度器则在 600 秒后超时。超时之后 Pod 重新调度，但 PVC 上的 `selected-node` 已经写进 API，**Filter 的快速路径会让这个 Pod 只能去原来那个节点**。这是一个有意的设计：供给可能已经在那个节点的拓扑里建了一半，换节点会留下孤儿卷。

这套协议的有趣之处是，调度器和供给器之间没有任何直接调用，全靠 PVC 上的一个注解：写上是请求，删掉是拒绝。[CSI 工作流程](/posts/k8s-csi-workflow/) 一文从供给器一侧讲了 `CreateVolume` 的细节，可以对照阅读。

链条走完，最后回到调度器内部：按这些规则被拒绝的 Pod 什么时候能重试。

## 九、EventsToRegister 与 QueueingHint

七类事件里有五个带 hint（第 2–9 行），判断依据都是「这个变化会不会让某个节点的结论翻转」：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumebinding/volume_binding.go
	events := []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.StorageClass, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterStorageClassChange},
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterPersistentVolumeClaimChange},
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolume, ActionType: fwk.Add | fwk.Update}},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel}},
		{Event: fwk.ClusterEvent{Resource: fwk.CSINode, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSINodeChange},
		{Event: fwk.ClusterEvent{Resource: fwk.CSIDriver, ActionType: fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSIDriverChange},
		{Event: fwk.ClusterEvent{Resource: fwk.CSIStorageCapacity, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSIStorageCapacityChange},
	}
```

七类事件，五个带 hint：

| 事件 | hint 的判断 |
| --- | --- |
| StorageClass 新增/更新 | 新增一律 `Queue`；更新时只有 `allowedTopologies` 变化才 `Queue`。`volumeBindingMode` 不可变，不用考虑 |
| PVC 新增/更新 | 同命名空间，且名字是 Pod 引用的 PVC（含 ephemeral 卷按 `<pod>-<卷名>` 算出的名字）就 `Queue`。几乎所有 PVC 更新都可能有用（绑定完成、注解被删），所以不再细分 |
| PV 新增/更新 | 无 hint，一律 `Queue` |
| Node 新增/标签更新 | 无 hint，一律 `Queue`。标签决定 PV nodeAffinity、`allowedTopologies`、`nodeTopology` 是否匹配 |
| CSINode 新增/更新 | 新增 `Queue`；更新时只有 `migrated-plugins` 注解变化才 `Queue`，它决定 in-tree PV 是否翻译 |
| CSIDriver 更新 | Pod 有内联 CSI 卷用到这个驱动，且 `storageCapacity` 从 true 变成 false（不再做容量检查）才 `Queue` |
| CSIStorageCapacity 新增/更新 | 新增 `Queue`；更新时 `volumeLimit`（`maximumVolumeSize` 优先，否则 `capacity`）变大才 `Queue` |

CSIDriver 的 hint 有一处值得推敲：它只遍历 `vol.CSI`（CSI 内联临时卷），而容量检查针对的是 PVC 的 StorageClass provisioner。对一个因容量不足被拒、使用普通 PVC 的 Pod，驱动关闭 `storageCapacity` 的事件会被这个 hint 判成 `QueueSkip`。好在这种操作很少见，Pod 最终也会被周期性刷新捞回来。

PV 事件没有 hint 是有代价的：PV 控制器、供给器、attach/detach 控制器都会频繁更新 PV 的 status 和注解，每次都会把所有被 VolumeBinding 拒绝的 Pod 放回队列。

事件讲完，剩下的是这套机制在真实集群里容易出问题的地方。

## 十、边界与坑

**被 selected-node 钉住的 PVC。** PreBind 超时、Pod 被删除重建、或调度器重启，只要 PVC 上的注解还在，新 Pod 就只能去那个节点。如果那个节点后来不满足 CPU、污点等条件，Pod 会在所有其它节点上报 `didn't find available persistent volumes to bind`，这条信息很容易误导人去查 PV。

排查时先 `kubectl get pvc -o yaml` 看有没有 `volume.kubernetes.io/selected-node`。供给器只会在节点不存在或 `ResourceExhausted` 时主动删注解，其它情况需要人工判断后手动删除。

**存储失败不会触发抢占。** Filter 和 PreFilter 的所有拒绝都是 `UnschedulableAndUnresolvable`。高优先级 Pod 因为本地盘被低优先级 Pod 占着而无法调度时，抢占不会发生。对 local PV 而言，PV 绑定后就不会因为 Pod 删除而释放（PVC 还在），驱逐 Pod 本来也没用。

绑定周期最长阻塞 10 分钟。默认 600 秒的 `bindTimeout` 对 Pod 来说意味着最坏情况下 10 分钟后才会重新调度。供给慢的存储（某些云盘、网络存储）需要这么长；但如果供给器根本不在跑（没部署、选举失败），Pod 也要白等 10 分钟。可以根据存储类型调小，但要大于正常供给的最长耗时，否则会反复超时、反复重调度，每次都可能在 API 里留下半成品。

Filter 失败原因的叠加。一个节点可能同时报 NodeConflict 和 BindConflict，因为已绑定 PVC 与未绑定 PVC 是分别检查的；但如果某个未绑定 PVC 走了 selected-node 快速路径，函数直接返回，后面的检查不会执行，原因只有一条。

ephemeral 卷与 VolumeZone 的不对称。VolumeBinding 处理 generic ephemeral 卷，下一篇的 VolumeZone 只处理 `persistentVolumeClaim` 卷。ephemeral 卷绑定的 PV 若带 zone 标签，只有 VolumeBinding 检查它的 `nodeAffinity`。

多调度器、多 Profile。每个 Profile 都会构造自己的 `VolumeBinding` 实例和 assume cache（`NewPVAssumeCache` 里专门处理了索引重复注册的情况）。两个 Profile 并发调度引用相同 PV 的 Pod 时，彼此看不到对方的假设，只能靠 Assume 的版本号检查和 PV 控制器兜底。

边界列完，最后补三段背景：这套机制是怎么演化来的、它和 DRA 的关系，以及生产上怎么配。

## 十一、发散

### 11.1 从 VolumeScheduling 到调度框架

延迟绑定最早以 `VolumeScheduling` feature gate 的形式在 1.9 引入，1.13 GA。那时它是调度器核心代码里的一个特殊模块，分三个阶段调用：

1. predicate 阶段调用 `FindPodVolumes`；
2. assume 阶段调用 `AssumePodVolumes`；
3. 绑定阶段调用 `BindPodVolumes`。

调度框架出现后，它被拆成今天这个插件，`SchedulerVolumeBinder` 接口上的那段长注释（`binder.go` 第 127~152 行）仍然按旧的流程描述各个方法的调用时机，里面的「Invokes all score plugins. Future/TBD」就是那个年代的痕迹。

`CSIStorageCapacity` 在 1.24 GA，打分则经历了两轮：

1. 先有 `VolumeCapacityPriority`，只给静态 PV 打分，默认 shape 偏向「PV 越满越好」的紧凑策略；
2. 后来 KEP-4049 以 `StorageCapacityScoring` 取而代之，加入了按 `CSIStorageCapacity` 给动态供给打分，默认 shape 反过来偏向剩余空间多的节点。

内部类型里那句过时的注释，就是第一轮留下的。

### 11.2 与 DynamicResources 的对照

[DynamicResources](/posts/sched-plugin-15-dynamic-resources/) 插件处理 ResourceClaim 的方式与 VolumeBinding 几乎是同构的：PreFilter 收集 claim，Filter 为每个节点算分配方案，Reserve 用 assume cache 假设分配结果，PreBind 写 API 并等待。

区别在于 DRA 的分配由调度器自己完成（结构化参数），PreBind 只需要写回 `status.allocation`；VolumeBinding 的执行者在调度器之外，所以必须轮询。这也是 VolumeBinding 需要 `bindTimeout`、DRA 只在等待设备绑定条件时才需要超时的原因。

### 11.3 生产建议

- 拓扑相关的存储（云盘、本地盘、TopoLVM、OpenEBS LocalPV）一律使用 `WaitForFirstConsumer`。`Immediate` 只适合 NFS、CephFS 这类任意节点都能访问的存储。
- 节点级存储想要容量感知，驱动必须部署 external-provisioner 的容量控制器并在 CSIDriver 上声明 `storageCapacity: true`。否则 Filter 不做容量检查，Score 对动态供给全部给 0 分。
- 1.37 之后 Score 默认生效。如果集群以静态 local PV 为主，默认 shape 会让调度器偏好「PV 比请求大很多」的节点，考虑配置递增 shape，比如 `[{utilization: 0, score: 0}, {utilization: 100, score: 10}]`。
- 监控 `scheduler_volume_scheduling_stage_error_total`（`volumebinding/metrics`），它按 `predicate`、`assume`、`bind` 三个阶段计数，`bind` 阶段的错误通常意味着供给器或 PV 控制器出了问题。

## 十二、小结

- `VolumeBinding` 让 `WaitForFirstConsumer` 的 PVC 在调度时才决定绑哪块 PV 或在哪供给，是调度器里唯一检查 PV `nodeAffinity` 的插件。
- PreFilter 把 PVC 分成已绑定、延迟绑定、立即绑定三类，存在未绑定的立即绑定 PVC（包括预绑定和 StorageClass 不存在的情况）就以 `UnschedulableAndUnresolvable` 拒绝。
- Filter 为每个节点生成一份 `PodVolumes`：已绑定 PVC 查 nodeAffinity；未绑定 PVC 先看 selected-node，再按容量从小到大匹配静态 PV，剩下的检查 provisioner、`allowedTopologies` 和 `CSIStorageCapacity`。所有失败都是 `UnschedulableAndUnresolvable`。
- Score 按 StorageClass 计算「请求 / 容量」并套 shape 折线，1.37 起 `StorageCapacityScoring` 默认开启、权重 1，默认 shape 偏向剩余空间多的节点。
- Reserve 通过 assume cache 在内存中写入 `claimRef` 和 `selected-node`，版本号规则让假设在 informer 追上后自动失效；PreBind 把它们写进 API 并每秒轮询，默认最多等 600 秒；Unreserve 撤销未落地的假设。
- 调度器与供给器之间只靠 `selected-node` 注解通信：写上是请求，供给器删掉是拒绝。

最后一个存储插件 [VolumeZone](/posts/sched-plugin-12-volume-zone/) 很短，它处理的是 PV 上那几个老旧的 zone/region 标签。
