---
title: 'scheduler-plugins 12 [in-tree] VolumeZone：PV 标签上的可用区约束'
date: 2026-11-19T10:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "存储", "topology"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "VolumeZone 读取已绑定 PV 上的 zone/region 标签，要求节点的同名标签取值落在其中。本文拆解它如何解析多可用区标签、如何兼容 failure-domain.beta 与 topology.kubernetes.io 两套标签、为什么失败一律是 UnschedulableAndUnresolvable，以及它与 VolumeBinding 检查 PV nodeAffinity 的分工和历史由来。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `VolumeZone` 实现的扩展点（PreFilter、Filter）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/12.svg" link="/images/posts/sched-plugins/callstack/12.svg" caption="VolumeZone 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇 [VolumeBinding](/posts/sched-plugin-11-volume-binding/) 是存储插件里最重的一个，它通过 PV 的 `spec.nodeAffinity` 处理拓扑约束。存储段的最后一个插件 `VolumeZone` 也管拓扑，但看的是另一个地方：PV 的 **label**。

本文基于 Kubernetes master 提交 `aabdf5a2`。`VolumeZone` 在默认 Profile 的 MultiPoint 列表里排在 `VolumeBinding` 之后、`PodTopologySpread` 之前，默认启用，不受 feature gate 控制，没有 Args。它实现 PreFilter、Filter、`EnqueueExtensions` 和 `SignPlugin`，没有 Score，没有 `PreFilterExtensions`。

## 一、它检查什么

判断依据只有一样东西：PV 上的 label。下面这块 PV 用两个拓扑标签声明了自己的位置：

```yaml {linenos=inline}
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pd-data-1
  labels:
    topology.kubernetes.io/region: us-west1
    topology.kubernetes.io/zone: us-west1-a__us-west1-b
spec:
  capacity:
    storage: 200Gi
  accessModes: ["ReadWriteOnce"]
  csi:
    driver: pd.csi.storage.gke.io
    volumeHandle: projects/p/regions/us-west1/disks/data-1
```

规则是：PV 带有的每一个拓扑标签，节点都必须有对应的标签，且节点的取值必须在 PV 的取值集合里。

上面这块 PV 对节点有两条要求：

1. `topology.kubernetes.io/region` 为 `us-west1`；
2. `topology.kubernetes.io/zone` 为 `us-west1-a` 或 `us-west1-b`（区域盘可以跨两个可用区挂载，多个可用区用双下划线 `__` 连接）。

不满足的节点返回 `UnschedulableAndUnresolvable`，原因 `node(s) had no available volume zone`。

节点一侧要比较的是同名的两个标签，通常由 kubelet 或 cloud-controller-manager 打上：

```yaml {linenos=inline}
# Node：位于 us-west1-b 的节点（只列出拓扑标签）
apiVersion: v1
kind: Node
metadata:
  name: node-b1
  labels:
    topology.kubernetes.io/region: us-west1
    topology.kubernetes.io/zone: us-west1-b
    # ...
```

拿它和上面的 `pd-data-1` 逐条比对：

1. PV 的 region 要求 `us-west1`，节点第 7 行相同，通过；
2. PV 的 zone 取值集合是 `{us-west1-a, us-west1-b}`，节点第 8 行的 `us-west1-b` 在集合里，通过；
3. 同一份 PV 遇到 zone 为 `us-west1-c` 的节点，第 2 条不满足，返回 `node(s) had no available volume zone`。

PV 的标签值里可以有 `__`，节点的标签值只能是单个可用区，所以比较永远是「节点的一个值是否落在 PV 的集合里」。

参与比较的标签只有四个：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumezone/volume_zone.go
var topologyLabels = []string{
	v1.LabelFailureDomainBetaZone,
	v1.LabelFailureDomainBetaRegion,
	v1.LabelTopologyZone,
	v1.LabelTopologyRegion,
}
```

这四个标签分成两组：

1. `failure-domain.beta.kubernetes.io/zone`、`failure-domain.beta.kubernetes.io/region` 两个旧标签（1.17 起废弃，第 3–4 行）；
2. `topology.kubernetes.io/zone`、`topology.kubernetes.io/region` 两个 GA 标签。

PV 上的其它标签一律不管。

判定对象和判定规则都清楚了，源码要做的只是解析标签、取 PV 拓扑、逐条比对。

## 二、源码拆解

### 2.1 结构体与 CycleState

代码分三部分：

1. 结构体保存四个 lister 和一个 feature gate 开关（第 2–7 行）；
2. `pvTopology` 描述一条展开后的 PV 拓扑（第 10–14 行）；
3. `stateData` 是状态，里面只有展开后的 PV 拓扑（第 16–20 行）。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumezone/volume_zone.go
type VolumeZone struct {
	pvLister                                           corelisters.PersistentVolumeLister
	pvcLister                                          corelisters.PersistentVolumeClaimLister
	scLister                                           storagelisters.StorageClassLister
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

// pvTopology holds the value of a pv's topologyLabel
type pvTopology struct {
	pvName string
	key    string
	values sets.Set[string]
}

type stateData struct {
	// podPVTopologies holds the pv information we need
	// it's initialized in the PreFilter phase
	podPVTopologies []pvTopology
}

func (d *stateData) Clone() fwk.StateData {
	return d
}
```

PreFilter 把 Pod 所有 PV 的拓扑标签展开成一个扁平列表，每个元素是「哪块 PV、哪个标签键、允许的取值集合」。Filter 只读这个列表，所以 `Clone` 直接返回自身，不需要复制。

### 2.2 PreFilter：从 Pod 走到 PV 的标签

PreFilter 的目标是把 Pod 用到的每块 PV 的拓扑标签收集成一个列表，分三步：

1. 第 4 行调用 `getPVbyPod`；
2. 第 8–10 行发现没有拓扑就 `Skip`；
3. 第 11 行写进 CycleState。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumezone/volume_zone.go
func (pl *VolumeZone) PreFilter(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// ...
	podPVTopologies, status := pl.getPVbyPod(logger, pod)
	if !status.IsSuccess() {
		return nil, status
	}
	if len(podPVTopologies) == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	cs.Write(preFilterStateKey, &stateData{podPVTopologies: podPVTopologies})
	return nil, nil
}
```

`getPVbyPod` 遍历 Pod 的 `persistentVolumeClaim` 卷（不含 generic ephemeral 卷），每个 PVC 走一遍下面的判断：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumezone/volume_zone.go
		pvName := pvc.Spec.VolumeName
		if pvName == "" {
			scName := storagehelpers.GetPersistentVolumeClaimClass(pvc)
			if len(scName) == 0 {
				return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "PersistentVolumeClaim had no pv name and storageClass name")
			}

			class, err := pl.scLister.Get(scName)
			if s := getErrorAsStatus(err); !s.IsSuccess() {
				return nil, s
			}
			if class.VolumeBindingMode == nil {
				return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("VolumeBindingMode not set for StorageClass %q", scName))
			}
			if *class.VolumeBindingMode == storage.VolumeBindingWaitForFirstConsumer {
				// Skip unbound volumes
				continue
			}

			return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "PersistentVolume had no name")
		}

		pv, err := pl.pvLister.Get(pvName)
		if s := getErrorAsStatus(err); !s.IsSuccess() {
			return nil, s
		}
		podPVTopologies = append(podPVTopologies, pl.getPVTopologies(logger, pv)...)
```

`getErrorAsStatus` 按错误类型分两种：

1. NotFound 转成 `UnschedulableAndUnresolvable`；
2. 其它错误转成 `Error`。

整理一下各种情况：

| PVC 状态 | 结果 |
| --- | --- |
| 已填 `volumeName`，PV 存在 | 收集 PV 的拓扑标签 |
| 已填 `volumeName`，PV 不存在 | `UnschedulableAndUnresolvable`，NotFound 信息 |
| 未绑定，StorageClass 为 `WaitForFirstConsumer` | 跳过，交给 VolumeBinding |
| 未绑定，StorageClass 为 `Immediate` | `UnschedulableAndUnresolvable`，`PersistentVolume had no name` |
| 未绑定，没有 StorageClass 名 | `UnschedulableAndUnresolvable` |
| 未绑定，StorageClass 不存在 | `UnschedulableAndUnresolvable`，NotFound 信息 |

延迟绑定的 PVC 被跳过（第 16–19 行），是因为它还没有 PV，没有标签可看；等 VolumeBinding 在 PreBind 里完成绑定后，供给出来的 PV 用 `nodeAffinity` 表达拓扑，由 VolumeBinding 自己在下次调度时检查。

表里后几种拒绝和 VolumeBinding 的 PreFilter 是重叠的。VolumeBinding 在 PreFilter 链上更靠前，遇到未绑定的 Immediate PVC 会先返回 `UnschedulableAndUnresolvable`。

框架的 `RunPreFilterPlugins`（`pkg/scheduler/framework/runtime/framework.go`）遇到这个 code 立即返回，排在后面的 VolumeZone 的 PreFilter 不会执行，所以实际很少看到 VolumeZone 报出这几条信息。它们在 VolumeBinding 被禁用的自定义 Profile 里才有意义。

注意 PVC 和 PV 取自普通 lister，不是 VolumeBinding 的 assume cache，看不到其它 Pod 在 Reserve 里做的假设。对 VolumeZone 没有影响，它只关心已经写进 API 的 `volumeName`。

### 2.3 getPVTopologies：解析多可用区标签

单个 PV 的标签在这里被展开成若干条 `pvTopology`：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumezone/volume_zone.go
// getPVTopologies retrieves pvTopology from a given PV and returns the array
// This function doesn't check spec.nodeAffinity
// because it's read-only after creation and thus cannot be updated
// and nodeAffinity is being handled in node affinity plugin
func (pl *VolumeZone) getPVTopologies(logger klog.Logger, pv *v1.PersistentVolume) []pvTopology {
	podPVTopologies := make([]pvTopology, 0)
	for _, key := range topologyLabels {
		if value, ok := pv.ObjectMeta.Labels[key]; ok {
			labelZonesSet, err := volumehelpers.LabelZonesToSet(value)
			if err != nil {
				logger.V(5).Info("failed to parse PV's topology label, ignoring the label", "label", fmt.Sprintf("%s:%s", key, value), "err", err)
				continue
			}
			podPVTopologies = append(podPVTopologies, pvTopology{
				pvName: pv.Name,
				key:    key,
				values: sets.Set[string](labelZonesSet),
			})
		}
	}
	return podPVTopologies
}
```

`LabelZonesToSet`（`k8s.io/cloud-provider/volume/helpers`，第 10 行）按 `cloudvolume.LabelMultiZoneDelimiter`，也就是 `"__"` 切分标签值。

为什么不用逗号？label 值的字符集只允许字母数字、`-`、`_`、`.`，逗号不合法，双下划线是能想到的最不容易与可用区名冲突的分隔符。解析失败的标签被忽略而不是报错（第 11–14 行），宁可少一条约束也不让 Pod 卡住。

函数注释的后半句不准确：PV 的 `nodeAffinity` 并不是由 NodeAffinity 插件处理的，NodeAffinity 插件只看 Pod 的 `spec.affinity.nodeAffinity` 和 `nodeSelector`；检查 PV `nodeAffinity` 的是 VolumeBinding 的 `checkBoundClaims`（见上一篇 5.1 节）。

前半句「read-only after creation」也在松动：1.35 引入了 alpha 的 `MutablePVNodeAffinity` gate，允许修改 PV 的 `nodeAffinity`。

### 2.4 Filter：逐条匹配，GA 标签单向兼容

Filter 拿 PreFilter 准备好的列表，逐条和节点标签比对。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumezone/volume_zone.go
func (pl *VolumeZone) Filter(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	// ...
	if len(pod.Spec.Volumes) == 0 {
		return nil
	}
	var podPVTopologies []pvTopology
	state, err := getStateData(cs)
	if err != nil {
		// Fallback to calculate pv list here
		var status *fwk.Status
		podPVTopologies, status = pl.getPVbyPod(logger, pod)
		if !status.IsSuccess() {
			return status
		}
	} else {
		podPVTopologies = state.podPVTopologies
	}

	node := nodeInfo.Node()
	hasAnyNodeConstraint := false
	for _, topologyLabel := range topologyLabels {
		if _, ok := node.Labels[topologyLabel]; ok {
			hasAnyNodeConstraint = true
			break
		}
	}

	if !hasAnyNodeConstraint {
		// The node has no zone constraints, so we're OK to schedule.
		// This is to handle a single-zone cluster scenario where the node may not have any topology labels.
		return nil
	}

	for _, pvTopology := range podPVTopologies {
		v, ok := node.Labels[pvTopology.key]
		if !ok {
			// if we can't match the beta label, try to match pv's beta label with node's ga label
			v, ok = node.Labels[translateToGALabel(pvTopology.key)]
		}
		if !ok || !pvTopology.values.Has(v) {
			// ...
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonConflict)
		}
	}

	return nil
}
```

逻辑分三步：

1. 节点没有任何拓扑标签就放行（第 29–33 行）。注释说这是为单可用区集群准备的：自建集群的节点往往没有 zone 标签，此时 PV 上的标签无从比较，强行比较只会让所有带标签的 PV 都无法使用。
2. 节点有任意一个拓扑标签，就要求 PV 的每一条标签都满足（第 35–45 行）。只要有一条不满足就失败。
3. beta 到 GA 的单向兼容。节点上找不到 PV 的那个标签键时，若 PV 用的是 beta 标签，就用 `translateToGALabel` 换成 GA 标签再找一次（第 37–40 行）。反过来不成立：PV 用 GA 标签、节点只有 beta 标签，直接判失败。

**概括起来是：节点没有拓扑标签就全部放行；有的话逐条匹配；回退方向只有 beta 到 GA。**

{{< figure src="/images/posts/sched-plugins/12-zone-match.svg" caption="VolumeZone 的匹配规则：节点无拓扑标签直接通过；否则 PV 的每个拓扑标签都要在节点上找到取值，beta 标签可以回退到 GA 标签，反向不行（点击查看大图）" link="/images/posts/sched-plugins/12-zone-match.svg" >}}

`volume_zone_test.go` 里有几组对应的用例，正好说明这几条规则：

| 用例名 | PV 标签 | 节点标签 | 结果 |
| --- | --- | --- | --- |
| pv with beta label, node with ga label, matched | beta zone = `us-west1-a` | GA zone = `us-west1-a` | 通过 |
| pv with zone, node with beta zone | GA zone = `us-west1-a` | beta zone = `us-west1-a` | 失败 |
| pv with zone and region, node with only zone | GA zone + GA region | 只有 GA zone | 失败 |
| node without labels | 任意 | 无拓扑标签 | 通过 |

为什么只做单向翻译？节点标签由 kubelet 和 cloud-controller-manager 的节点控制器维护，后者会同时打上 beta 和 GA 两套标签（`staging/src/k8s.io/cloud-provider/controllers/node/node_controller.go`），新版本的节点一定有 GA 标签。只有 beta 标签的节点属于很老的版本，代码没有为它们考虑。

而 PV 是持久对象，几年前创建的 PV 带着 beta 标签很常见，必须兼容。

Filter 里还保留了一个兜底：CycleState 里读不到状态时，现场调用 `getPVbyPod` 再算一遍（第 9–18 行）。正常调度路径上 PreFilter 返回 `Skip` 时 Filter 根本不会被调用：`RunPreFilterPlugins` 把返回 `Skip` 的插件记进 CycleState 的 SkipFilterPlugins 集合，`RunFilterPlugins` 遇到集合里的插件直接跳过（开篇 3.2 节）。读不到状态只可能出现在直接调用 Filter 的外部代码里，例如把调度器插件嵌进去做模拟的组件。

### 2.5 为什么是 UnschedulableAndUnresolvable

节点的可用区标签和 PV 的可用区标签都与节点上运行着哪些 Pod 无关，驱逐任何 Pod 都不会改变比较结果。

返回 `UnschedulableAndUnresolvable` 让抢占直接排除这些节点：`pkg/scheduler/framework/preemption/preemption.go` 只用 `NodesForStatusCode(..., fwk.Unschedulable)` 挑出状态为 `Unschedulable` 的节点做模拟，省掉无意义的计算。PreFilter 里的拒绝也是同一个码，并且会终止整个 PreFilter 链。

这和前两篇形成了对照：

1. VolumeRestrictions、NodeVolumeLimits 的失败与节点上的 Pod 有关，用 `Unschedulable`；
2. VolumeBinding、VolumeZone 的失败与对象属性有关，用 `UnschedulableAndUnresolvable`。

到这里，判定逻辑就讲完了。剩下的是它订阅了哪些事件。

## 三、EventsToRegister 与 QueueingHint

四类事件覆盖了所有可能让结论翻转的变化（第 6–13 行）：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/volumezone/volume_zone.go
func (pl *VolumeZone) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// New storageClass with bind mode `VolumeBindingWaitForFirstConsumer` will make a pod schedulable.
		// Due to immutable field `storageClass.volumeBindingMode`, storageClass update events are ignored.
		{Event: fwk.ClusterEvent{Resource: fwk.StorageClass, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterStorageClassAdded},
		// A new node or updating a node's volume zone labels may make a pod schedulable.
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel}},
		// A new pvc may make a pod schedulable.
		// Also, if pvc's VolumeName is filled, that also could make a pod schedulable.
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterPersistentVolumeClaimChange},
		// A new pv or updating a pv's volume zone labels may make a pod schedulable.
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolume, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: pl.isSchedulableAfterPersistentVolumeChange},
	}, nil
}
```

1. StorageClass 新增：只有 `volumeBindingMode` 为 `WaitForFirstConsumer` 的新 class 才 `Queue`。对应 PreFilter 中「StorageClass 不存在」的拒绝：新建一个 Immediate 的 class 也无济于事，因为未绑定的 Immediate PVC 照样会被拒。`volumeBindingMode` 不可变，所以不监听更新。
2. Node 新增/标签更新：无 hint。`UpdateNodeLabel` 本身已经是细分过的事件类型：调度器的 Node 更新回调（`pkg/scheduler/eventhandlers.go`）调用 `NodeSchedulingPropertiesChange`（`pkg/scheduler/framework/events.go`）逐项比较新旧 Node，只有 labels 不同才产生 `UpdateNodeLabel`，节点其它字段的变化不会触发。
3. PVC 新增/更新：同命名空间、名字被 Pod 引用就 `Queue`。最典型的是 PVC 的 `volumeName` 被填上。
4. PV 新增/更新：新增一律 `Queue`；更新时用 `getPVTopologies` 分别算出新旧 PV 的拓扑列表，`reflect.DeepEqual` 不同才 `Queue`。PV 的 status、注解频繁变化，这个 hint 能过滤掉绝大多数无关更新。

PV 的 hint 没有检查「这块 PV 是不是 Pod 要用的」，任何 PV 的拓扑标签变化都会唤醒所有被 VolumeZone 拒绝的 Pod。拓扑标签很少变，影响不大。

事件讲完，剩下的是几种容易踩空的情况。

## 四、边界与坑

这个插件的坑主要来自它和其它存储插件的分工边界。

**只看 `persistentVolumeClaim` 卷。** generic ephemeral 卷生成的 PVC 绑定的 PV 即使带有 zone 标签，VolumeZone 也不检查。好在 ephemeral 卷基本都走动态供给，拓扑写在 `nodeAffinity` 里，由 VolumeBinding 负责。

节点只有 region 没有 zone。`hasAnyNodeConstraint` 为真，PV 上的 zone 标签在节点上找不到，失败。这在只给节点打了 region 标签的自建集群里会出现。

**手工给 PV 打标签。** 有人为了「让 Pod 只调度到某个可用区」给静态 PV 加 `topology.kubernetes.io/zone`，这是有效的。但更规范的做法是写 PV 的 `nodeAffinity`，那样 VolumeBinding 在绑定前的匹配阶段就会考虑它，而 label 只在 PVC 已绑定之后才被 VolumeZone 检查。

对 `WaitForFirstConsumer` 的静态 PV，label 约束在首次调度时完全不起作用，问题分两步暴露：

1. VolumeBinding 的 `FindMatchingVolume` 不看 PV 的 label（除非 PVC 写了 `selector`），可能为 Pod 选中一块 label 与节点不符的 PV；
2. 绑定完成后，Pod 一旦重建，VolumeZone 就会把它限制在 PV 标签写的可用区里，可能与首次调度的位置不一致。

以一块手工打了标签的静态 PV 为例：`manual-pv-1` 带 `topology.kubernetes.io/zone: us-west1-a`，没有 `nodeAffinity`，StorageClass 为 `WaitForFirstConsumer`。StatefulSet 的 Pod `app-0` 通过 PVC `data-app-0` 使用它；node-a1 在 `us-west1-a`，node-b1 在 `us-west1-b`。过程分四步：

1. 首次调度时 `data-app-0` 还没有 `volumeName`，`getPVbyPod` 在 2.2 节代码块第 16–19 行跳过它，`podPVTopologies` 为空，VolumeZone 的 PreFilter 返回 `Skip`；
2. VolumeBinding 的 `FindMatchingVolume` 在两个节点上都能匹配 `manual-pv-1`：PV 没有 `nodeAffinity`，PVC 没有 `selector`，标签不参与比较。假设打分选中 node-b1，PreBind 之后 PV 控制器完成绑定，`volumeName` 写成 `manual-pv-1`；
3. `app-0` 被删除重建。这次 `getPVbyPod` 能取到 PV，`getPVTopologies` 展开出一条 `{key: topology.kubernetes.io/zone, values: {us-west1-a}}`；
4. Filter 中 node-b1 的 zone 值 `us-west1-b` 不在集合里，返回 `UnschedulableAndUnresolvable`；node-a1 通过。

同一个 Pod、同一块 PV，两次调度被放到了不同的可用区。如果 Pod 自己还用 `nodeSelector` 限定在 `us-west1-b`，第二次调度时 node-a1 被 NodeAffinity 拒绝、node-b1 被 VolumeZone 拒绝，就没有任何节点可选。

同样的约束改写进 PV 的 `nodeAffinity`，两次调度的结论就一致了：

```yaml {linenos=inline}
# PV：manual-pv-1 用 nodeAffinity 而不是 label 声明可用区
apiVersion: v1
kind: PersistentVolume
metadata:
  name: manual-pv-1
spec:
  capacity:
    storage: 100Gi
  accessModes: ["ReadWriteOnce"]
  storageClassName: manual-wffc
  csi:
    driver: pd.csi.storage.gke.io
    volumeHandle: projects/p/zones/us-west1-a/disks/manual-1
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: topology.kubernetes.io/zone
          operator: In
          values: ["us-west1-a"]
```

和打标签的版本相比，变化在两处：

1. `metadata` 里不再有 `topology.kubernetes.io/zone` 标签，VolumeZone 的 `getPVTopologies` 展开结果为空，两次调度的 PreFilter 都返回 `Skip`；
2. 第 14–20 行的 `nodeAffinity` 由 VolumeBinding 检查：首次调度时 `FindMatchingVolume` 的第 5 个条件让 node-b1 匹配不到这块 PV（上一篇 5.2 节），第 10 行的 `manual-wffc` 是 provisioner 为 `kubernetes.io/no-provisioner` 的静态 class，不能改走动态供给，`app-0` 只能落到 node-a1；重建后 `checkBoundClaims` 用 `CheckNodeAffinity` 再查一次，结论相同。

`nodeSelectorTerms` 里只能写 `matchExpressions` 和 `matchFields`，没有 `matchLabels`；这里用 `In` 加一个可用区，等价于原来标签的取值集合。

重复检查。早期由云厂商 admission 创建的 in-tree PV，往往同时带有 zone 标签和对应的 `nodeAffinity`，两者会被 VolumeZone 和 VolumeBinding 各检查一次，结论一致，只是多花一点时间。

这几条都指向同一个事实：在 CSI 时代，PV 的 label 已经不是拓扑的主要载体了。

## 五、发散：一个靠惯性存在的插件

VolumeZone 的前身是 `NoVolumeZoneConflict` predicate，它出现在 PV 还没有 `nodeAffinity` 字段的年代。那时 GCE PD、AWS EBS 这类云盘的可用区信息只能放在 label 上，由 `PersistentVolumeLabel` admission controller（后来迁到 cloud-controller-manager 的 PV labeler）在 PV 创建时调用云厂商 API 查询磁盘所在可用区，自动打上 `failure-domain.beta.kubernetes.io/zone`。

调度器则用这个 predicate 保证 Pod 不会被调度到磁盘所在可用区之外。

后来 PV 有了 `spec.nodeAffinity`，表达能力远强于几个固定标签，可以写：

1. 任意标签键；
2. `In`/`NotIn` 等操作符；
3. 多个 term 的「或」关系。

CSI 驱动供给出来的 PV 用驱动自己的拓扑键（如 `topology.gke.io/zone`、`topology.ebs.csi.aws.com/zone`）写在 `nodeAffinity` 里，根本不带 `topology.kubernetes.io/zone` 标签。在本文所用的源码树里，`PersistentVolumeLabel` admission 和 PV labeler 都已经不存在了，没有任何组件会自动给 PV 打这几个标签。

所以今天的 VolumeZone 只对两类 PV 起作用：

1. 历史遗留的 in-tree PV；
2. 有人手工打了标签的 PV。

对于新建集群，它几乎总是在 PreFilter 返回 `Skip`，开销只是遍历一次 Pod 的 PVC、从 lister 取 PV 看标签。

它仍然留在默认 Profile 里，原因和 VolumeRestrictions 的内联卷检查一样：老集群里那些 PV 还在，删掉插件意味着 Pod 可能被调度到磁盘所在可用区之外，attach 失败后卡在 `ContainerCreating`，而这种失败不会触发重新调度：Pod 已经写入 `spec.nodeName`，调度器把它当作已调度的 Pod，不再放进调度队列（第 3 篇 2.1 节），attach 和挂载只会在这个节点上反复重试。

保留的成本几乎为零，删除的风险却真实存在。如果确定集群里没有带拓扑标签的 PV，在自定义 Profile 中禁用它不会有任何影响。

对比四个存储插件的分工，可以看出它们是按「看哪个字段」切分的：

| 插件 | 看什么 | 失败码 |
| --- | --- | --- |
| VolumeRestrictions | Pod 内联卷 ID、PVC access mode | `Unschedulable` |
| NodeVolumeLimits | CSINode allocatable、节点上已挂的卷 | `Unschedulable` |
| VolumeBinding | PVC 绑定状态、PV `nodeAffinity`、StorageClass、CSIStorageCapacity | `UnschedulableAndUnresolvable` |
| VolumeZone | PV 的 zone/region 标签 | `UnschedulableAndUnresolvable` |

## 六、小结

1. `VolumeZone` 只检查已绑定 PV 上的四个拓扑标签（beta/GA 的 zone、region），要求节点的同名标签取值落在 PV 的取值集合中，多个可用区用 `__` 分隔。
2. 节点没有任何拓扑标签时直接通过；PV 的 beta 标签可以匹配节点的 GA 标签，反向不行。
3. 未绑定的 `WaitForFirstConsumer` PVC 被跳过；未绑定的 Immediate PVC、PV 或 StorageClass 不存在都在 PreFilter 返回 `UnschedulableAndUnresolvable`，但通常会先被 VolumeBinding 拦下。
4. 所有失败都是 `UnschedulableAndUnresolvable`，标签不会因为驱逐 Pod 而改变，抢占直接跳过这些节点。
5. PV 的 `nodeAffinity` 由 VolumeBinding 检查而不是 NodeAffinity 插件，源码注释里的说法有误；CSI 时代的 PV 基本不带这些标签，VolumeZone 主要服务于历史遗留 PV。

存储段到此结束。下一篇 [PodTopologySpread](/posts/sched-plugin-13-pod-topology-spread/) 回到 Pod 之间的关系：如何把副本均匀地打散到各个拓扑域。
