---
title: 'scheduler-plugins [in-tree] NodeDeclaredFeatures：让调度器知道节点「会什么」'
date: 2026-11-19T10:40:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "NodeDeclaredFeatures", "版本偏差", "调度插件"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NodeDeclaredFeatures 让 kubelet 声明自己支持的特性，调度器从 Pod spec 推断所需特性并做子集判断，避免版本偏差期间把新特性 Pod 放到旧节点。本文拆解特性注册表与位图、MaxVersion，以及配套的 kubelet 与 apiserver 准入检查。"
---

[上一篇](/posts/sched-plugin-15-dynamic-resources/)的 `DynamicResources` 处理的是「节点上有没有某种设备」。`NodeDeclaredFeatures` 处理的问题更抽象：节点上的 **kubelet 会不会**某个功能。它是 Filter 链上最后一个插件，也是 in-tree 插件里最年轻的之一。

本文基于 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期）。插件由 feature gate `NodeDeclaredFeatures` 控制，对应 [KEP-5328](https://kep.k8s.io/5328)：1.35 Alpha，1.36 Beta 默认开启，1.37 GA 并锁定开启。gate 开启时 `applyFeatureGates` 把它**追加**到默认 MultiPoint 列表末尾（排在 `DefaultBinder` 之后），所以在 Filter 链上它最后执行。它实现了 PreFilter、Filter、`EnqueueExtensions` 和 `SignPlugin`，没有 Score，没有参数。

## 一、问题：控制面先升级，节点后升级

Kubernetes 的版本偏差策略允许 kubelet 比 kube-apiserver 落后若干个小版本。升级通常是先控制面、后节点，一个大集群的节点滚动升级可能持续数周。这期间会出现这样的情况：

- apiserver 已经认识某个新字段（比如容器的 `restartPolicyRules` 里 `action: RestartAllContainers`），也接受了带这个字段的 Pod；
- 调度器把 Pod 放到了一个还在跑旧版本 kubelet、或者没开对应 feature gate 的节点上；
- 旧 kubelet 不认识这个字段，要么静默忽略（行为与用户预期不符），要么在准入时拒绝，Pod 进入 `Failed`，控制器再建一个新 Pod，新 Pod 可能又被调度到同一类节点上。

在这个插件出现之前，常见的应对方式是运维手工给新节点打标签，再让工作负载写 `nodeSelector`。这要求每个用户都知道「哪个字段需要哪个版本的 kubelet」，而这正是 Kubernetes 自己最清楚的事情。

`NodeDeclaredFeatures` 的思路是把这份知识写进一个共享库：

- kubelet 启动时，根据自己的 feature gate、容器运行时能力和版本，算出自己支持的特性列表，写进 `Node.status.declaredFeatures`；
- 调度器从 Pod spec **推断**出它需要哪些特性，过滤掉没有声明这些特性的节点；
- 用户不需要写任何东西，Pod spec 本身就是需求声明。

## 二、共享库：特性注册表

### 2.1 `Feature` 接口

推断和发现的逻辑都不在调度器里，而在 `k8s.io/component-helpers/nodedeclaredfeatures`。每个特性实现一个接口：

```go
// staging/src/k8s.io/component-helpers/nodedeclaredfeatures/types/types.go
type Feature interface {
	// Name returns the feature's well-known name.
	Name() string

	// Discover checks if a node provides the feature based on its configuration.
	Discover(cfg *NodeConfiguration) bool

	// Requirements returns the feature's feature gate and static config dependencies.
	Requirements() *FeatureRequirements

	// InferForScheduling checks if pod scheduling requires the feature.
	InferForScheduling(podInfo *PodInfo) bool

	// InferForUpdate checks if a pod update requires the feature.
	InferForUpdate(oldPodInfo, newPodInfo *PodInfo) bool

	// MaxVersion specifies the upper bound Kubernetes version (inclusive) for this feature's relevance
	// as a scheduling factor. Should be set based on the feature's GA version
	// and the cluster's version skew policy. Nil means no upper version bound.
	// Comparisons use the full semantic versioning scheme.
	MaxVersion() *version.Version
}
```

一个特性同时回答两侧的问题：节点侧的 `Discover`（我有没有），Pod 侧的 `InferForScheduling`/`InferForUpdate`（你要不要）。两侧逻辑写在同一个文件里，由同一个特性的作者维护，不会出现「kubelet 声明的名字和调度器检查的名字对不上」。

以 `RestartAllContainersOnContainerExits` 为例：

```go
// staging/src/k8s.io/component-helpers/nodedeclaredfeatures/features/restartallcontainers/restart_all_containers.go
func (f *restartAllContainersFeature) Discover(cfg *types.NodeConfiguration) bool {
	return cfg.FeatureGates.Enabled(RestartAllContainersOnContainerExits)
}

func (f *restartAllContainersFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	for _, c := range podInfo.Spec.Containers {
		for _, rule := range c.RestartPolicyRules {
			if rule.Action == v1.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
	}
	for _, c := range podInfo.Spec.InitContainers {
		for _, rule := range c.RestartPolicyRules {
			if rule.Action == v1.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
	}
	return false
}

func (f *restartAllContainersFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	// container.restartPolicy and container.restartPolicyRules are not mutable.
	return false
}
```

节点开了这个 gate 就声明；Pod 只要有一条容器重启规则用了 `RestartAllContainers` 就需要它。字段不可变，所以更新时永远不需要。

有的特性的 `Discover` 还依赖容器运行时，比如 `UserNamespacesHostNetworkSupport` 要求 gate 开启**并且** CRI 报告支持「user namespace + host network」；它的 `InferForScheduling` 检查 `hostNetwork: true` 且 `hostUsers: false`。

### 2.2 当前注册表

注册表是代码里写死的切片：

```go
// staging/src/k8s.io/component-helpers/nodedeclaredfeatures/features/registry.go
var AllFeatures = []types.Feature{
	restartallcontainers.Feature,
	inplacepodresize.PodLevelResourcesResizeFeature,
	extendwebsocketstokubelet.Feature,
	inplacepodresize.NonSidecarInitContainerResizeFeature,
	inplacepodresize.MemoryBackedVolumesResizeFeature,
	usernamespaceshostnetwork.Feature,
	dranodeallocatableresources.Feature,
	volumebindmountoptions.Feature,
	draoptionalnodeoperations.Feature,
	podcertificatesmldsa.Feature,
}
```

按「对调度是否有影响」可以分成三类：

| 特性 | `InferForScheduling` | `InferForUpdate` |
| --- | --- | --- |
| `RestartAllContainersOnContainerExits` | 有 `RestartAllContainers` 重启规则 | 否（字段不可变） |
| `UserNamespacesHostNetworkSupport` | `hostNetwork` 且 `hostUsers: false` | 否（字段不可变） |
| `VolumeBindMountOptions` | 任一容器的 volumeMount 设置了 `bindMountOptions` | 否 |
| `PodCertificateMLDSA` | projected 卷里有 ML-DSA 类型的 podCertificate | 新增了这类卷 |
| `InPlacePodLevelResourcesVerticalScaling` | 否 | Pod 级 `resources` 变化 |
| `InPlacePodVerticalScalingInitContainers` | 否 | 非 sidecar init 容器的 resources 变化 |
| `InPlacePodVerticalScalingMemoryBackedVolumes` | 否 | 内存型 emptyDir 的 `sizeLimit` 变化 |
| `ExtendWebSocketsToKubelet`、`DRANodeAllocatableResources`、`DRAOptionalNodeOperations` | 否 | 否 |

第二类是「原地扩缩容」相关的特性：调度时 Pod 还没有被修改过，只有运行中修改 resources 时才需要节点支持，所以它们只在 Pod **更新**的检查里起作用（见第五节）。第三类对 Pod 的调度和更新都不施加约束，声明它们是为了让其它组件读取：`DRAOptionalNodeOperations` 的注释说明，ResourceClaim 的分配结果要到 PreBind 才确定，PreFilter 推断不出来，这类过滤由 `DynamicResources` 插件自己做；`DRANodeAllocatableResources` 则被 apiserver 的准入插件用来校验 ResourceSlice。

### 2.3 从名字到位图

`Framework` 在构造时把注册表按名字排序，每个特性的下标就是它在位图中的位置：

```go
// staging/src/k8s.io/component-helpers/nodedeclaredfeatures/framework.go
func New(registry []types.Feature) *Framework {
	// Ensure the features are sorted.
	slices.SortFunc(registry, func(a, b types.Feature) int {
		return strings.Compare(a.Name(), b.Name())
	})
	featureNames := make([]string, len(registry))
	for i, f := range registry {
		featureNames[i] = f.Name()
	}
	return &Framework{
		registry:      registry,
		FeatureMapper: NewFeatureMapper(featureNames),
	}
}
```

`FeatureSet` 就是一个 `[]uint64` 位图，子集判断是逐字的按位运算：

```go
// staging/src/k8s.io/component-helpers/nodedeclaredfeatures/bitmap.go
func (b bitmap) IsSubset(other bitmap) (bool, error) {
	if b.size != other.size {
		return false, fmt.Errorf("bitmap size mismatch; %d != %d", b.size, other.size)
	}
	for i := range b.words {
		if b.words[i]&^other.words[i] != 0 {
			return false, nil
		}
	}
	return true, nil
}
```

`b &^ other`（b 与非 other）留下「b 有而 other 没有」的位，全为 0 即是子集。当前 10 个特性只占一个 `uint64` 的低 10 位，一次 Filter 的判断就是一次按位运算。

名字列表到位图的映射在 `mapSorted` 中，它假定输入是有序的，用双指针一次扫描完成。有两个变体：`MustMapSorted` 遇到未知名字就 panic，`TryMap` 则跳过未知名字。

{{< figure src="/images/posts/sched-plugins/16-ndf-bitmap.svg" caption="注册表排序后每个特性占固定的一位；节点声明、Pod 需求都变成同样长度的位图，Filter 只做一次子集判断。下方是同一个库在三个组件里的用法（点击查看大图）" link="/images/posts/sched-plugins/16-ndf-bitmap.svg" >}}

## 三、节点侧：kubelet 声明

kubelet 在初始化时（`NewMainKubelet`）计算一次，之后不再变化：

```go
// pkg/kubelet/kubelet.go
	if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) {
		if status, err := klet.containerRuntime.Status(ctx); err == nil && status != nil {
			klet.runtimeState.setRuntimeFeatures(status.Features)
		} else if err != nil {
			logger.V(4).Info("Unable to prefetch container runtime features for node declared features", "err", err)
		}
		v, err := versionutil.Parse(version.Get().String())
		if err != nil {
			return nil, fmt.Errorf("failed to parse version: %w", err)
		}
		framework := ndf.DefaultFramework
		klet.version = v
		klet.nodeDeclaredFeaturesFramework = framework
		klet.nodeDeclaredFeatures = klet.discoverNodeDeclaredFeatures()
		klet.nodeDeclaredFeaturesSet = framework.MustMapSorted(klet.nodeDeclaredFeatures)
	}
```

`discoverNodeDeclaredFeatures` 把 kubelet 的 feature gate、CRI 报告的运行时能力（目前是 `UserNamespacesHostNetwork` 和 `MountOptions` 两项）和 kubelet 版本打包成 `NodeConfiguration`，交给 `DiscoverNodeFeatures`。结果写进节点状态（`kubelet_node_status.go` 中 `node.Status.DeclaredFeatures = kl.nodeDeclaredFeatures`）。

kubelet 用 `MustMapSorted` 而不是 `TryMap`：它声明的名字本来就来自同一个注册表，出现未知名字说明程序有 bug，应该立刻暴露。

API 字段的定义是一个 `listType=atomic` 的字符串列表：

```go
// staging/src/k8s.io/api/core/v1/types.go
	// DeclaredFeatures represents the features related to feature gates that are declared by the node.
	// +featureGate=NodeDeclaredFeatures
	// +optional
	// +listType=atomic
	DeclaredFeatures []string `json:"declaredFeatures,omitempty" protobuf:"bytes,14,rep,name=declaredFeatures"`
```

与之相比，同在 `NodeStatus` 里的 `features` 字段（`NodeFeatures` 结构体，目前只有一个 `supplementalGroupsPolicy` 布尔值）每增加一个特性都要改 API 类型。`declaredFeatures` 用字符串列表承载，增加特性只需要往注册表里加一个实现，不涉及 API 变更。

## 四、调度器侧：插件源码

### 4.1 `NodeInfo` 里预先转好位图

调度器缓存在节点对象写入 `NodeInfo` 时就完成转换，不需要每个调度周期重复：

```go
// pkg/scheduler/framework/types.go
func (n *NodeInfo) SetNode(node *v1.Node) {
	n.node = node
	n.Allocatable = NewResource(node.Status.Allocatable)
	if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) {
		// Use TryMap rather than Map here in case the node has unknown features. Since we're only
		// concerned with matching known features to the node, there is no risk to discarding
		// unknown features.
		n.DeclaredFeatures = ndf.DefaultFramework.TryMap(node.Status.DeclaredFeatures)
	}
	n.Generation = nextGeneration()
}
```

这里用 `TryMap`。kubelet 有可能比调度器新：版本偏差策略允许 kube-scheduler 比 apiserver 落后一个小版本，而 kubelet 不超过 apiserver，于是在控制面升级过程中，可能出现 kubelet 已是新版本、调度器还是旧版本的窗口。新 kubelet 声明了旧调度器不认识的特性名，旧调度器也不会推断出 Pod 需要这个特性（它的注册表里没有），丢弃它不会造成误判。

### 4.2 结构体与 `New`

```go
// pkg/scheduler/framework/plugins/nodedeclaredfeatures/nodedeclaredfeatures.go
type NodeDeclaredFeatures struct {
	ndfFramework                                       *ndf.Framework
	version                                            *versionutil.Version
	enabled                                            bool
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

func New(ctx context.Context, plArgs runtime.Object, fh fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	if !fts.EnableNodeDeclaredFeatures {
		// Disabled, won't do anything.
		return &NodeDeclaredFeatures{}, nil
	}
	ndfFramework := ndf.DefaultFramework
	ver, err := versionutil.Parse(version.Get().String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse version: %w", err)
	}
	return &NodeDeclaredFeatures{ndfFramework: ndfFramework, version: ver, enabled: true, enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption}, nil
}
```

gate 关闭时返回一个空壳插件（`enabled` 为 false），所有扩展点直接放行或 `Skip`，`EventsToRegister` 返回空。这样即使用户在自定义配置里显式启用了它，也不会出错。`version` 是**调度器自身**的版本，下一节会看到它的作用。

### 4.3 PreFilter

```go
// pkg/scheduler/framework/plugins/nodedeclaredfeatures/nodedeclaredfeatures.go
func (pl *NodeDeclaredFeatures) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	if !pl.enabled {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	// Pod status is not updated yet, we just pass the spec to node declared features library
	podInfo := &ndf.PodInfo{Spec: &pod.Spec}
	reqs, err := pl.ndfFramework.InferForPodScheduling(podInfo, pl.version)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	if reqs.IsEmpty() {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	cycleState.Write(preFilterStateKey, &preFilterState{reqs: reqs})
	return nil, nil
}
```

三个 `Skip` 出口：原地扩容被推迟的已绑定 Pod（它不换节点，见[第 17 篇](/posts/sched-plugin-17-deferred-pod-scheduling/)）；插件未启用；Pod 不需要任何特性。第三个是绝大多数 Pod 的路径，对它们来说插件的全部开销就是十次 `InferForScheduling` 调用，每次只是遍历容器或卷列表。

推断时只传 `Spec`，`Status` 为 nil：调度阶段的 Pod 还没有运行时状态。

`preFilterState` 只有一个位图，`Clone` 直接返回自身：

```go
// pkg/scheduler/framework/plugins/nodedeclaredfeatures/nodedeclaredfeatures.go
type preFilterState struct {
	reqs ndf.FeatureSet
}

// Clone implements StateData.
func (s *preFilterState) Clone() fwk.StateData {
	return s
}
```

需求只取决于 Pod 自身，抢占模拟中增删其它 Pod 不会改变它，所以没有 `PreFilterExtensions`（`PreFilterExtensions()` 返回 nil），也不需要深拷贝。

### 4.4 Filter

```go
// pkg/scheduler/framework/plugins/nodedeclaredfeatures/nodedeclaredfeatures.go
func (pl *NodeDeclaredFeatures) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	if !pl.enabled {
		return nil
	}
	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	// Don't use ndf.MatchNodeFeatureSet(...) here since we don't want to pay the cost of computing the feature diff.
	isMatch, err := s.reqs.IsSubset(nodeInfo.GetNodeDeclaredFeatures())
	if err != nil {
		return fwk.AsStatus(err)
	}
	if !isMatch {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, errReasonUnsatisfiedRequirements)
	}
	return nil
}
```

注释说明了一个取舍：库里的 `MatchNodeFeatureSet` 在不匹配时会计算差集并把位图反解成名字列表，用于生成可读的错误信息（kubelet 和 apiserver 准入就用它）。Filter 在每个候选节点上都要跑，几千个节点各算一次差集、分配一个字符串切片是不必要的开销，所以只做 `IsSubset`，原因字符串是固定的 `node(s) didn't match Pod's required features`。代价是调度失败事件里看不出具体缺哪个特性，排查时需要自己比对 Pod spec 和节点的 `status.declaredFeatures`。

失败返回 `UnschedulableAndUnresolvable`：节点会不会某个功能取决于 kubelet 版本和配置，驱逐 Pod 改变不了它，抢占不应该考虑这些节点。

### 4.5 MaxVersion：特性「毕业」之后

```go
// staging/src/k8s.io/component-helpers/nodedeclaredfeatures/framework.go
func (f *Framework) InferForPodScheduling(podInfo *types.PodInfo, targetVersion *version.Version) (FeatureSet, error) {
	if targetVersion == nil {
		return FeatureSet{}, fmt.Errorf("target version cannot be nil")
	}
	reqs := f.NewFeatureSet()
	for i, f := range f.registry {
		if f.MaxVersion() != nil && targetVersion.GreaterThan(f.MaxVersion()) {
			// If target version is greater than the feature's max version, no need to require the feature
			continue
		}
		if f.InferForScheduling(podInfo) {
			reqs.Set(i)
		}
	}
	return reqs, nil
}
```

```go
// staging/src/k8s.io/component-helpers/nodedeclaredfeatures/framework.go
func (f *Framework) DiscoverNodeFeatures(cfg *types.NodeConfiguration) []string {
	var enabledFeatures []string
	for _, f := range f.registry {
		if f.Discover(cfg) {
			if cfg.Version != nil && f.MaxVersion() != nil && cfg.Version.GreaterThan(f.MaxVersion()) {
				continue
			}
			enabledFeatures = append(enabledFeatures, f.Name())
		}
	}
	return enabledFeatures
}
```

一个特性 GA 之后，最终所有受支持版本的 kubelet 都具备它，再声明、再检查就没有意义了。`MaxVersion` 就是这个「毕业版本」：

- 推断侧用**调度器的版本**比较：调度器版本超过 `MaxVersion`，说明按版本偏差策略，所有合法的 kubelet 都已经支持，不再把它列为需求；
- 发现侧用 **kubelet 的版本**比较：kubelet 超过 `MaxVersion` 后不再声明它，`declaredFeatures` 不会无限增长。

两侧必须同时停止，否则会出现「调度器还在要求、新 kubelet 已经不声明」的情况。单元测试 `Feature not required, version > MaxVersion` 覆盖了推断侧：调度器版本 1.34.0、特性 `MaxVersion` 1.33.0 时，即使 `InferForScheduling` 返回 true，PreFilter 也返回 `Skip`。

`Feature` 接口的注释要求按「GA 版本加上版本偏差窗口」来设置 `MaxVersion`。当前注册表里所有特性的 `MaxVersion()` 都返回 nil，即都还处于需要检查的阶段。

### 4.6 SignPod

```go
// pkg/scheduler/framework/plugins/nodedeclaredfeatures/nodedeclaredfeatures.go
func (pl *NodeDeclaredFeatures) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	podInfo := &ndf.PodInfo{Spec: &pod.Spec}
	fs, err := pl.ndfFramework.InferForPodScheduling(podInfo, pl.version)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	return []fwk.SignFragment{
		{Key: fwk.FeaturesSignerName, Value: fs.String()},
	}, nil
}
```

与前两篇的插件不同，它总是愿意签名：需求只取决于 Pod spec，两个推断结果相同的 Pod 在这个插件上的可行节点集合必然相同。签名片段是位图的字符串形式（如 `0000000100`），键是 `v1.Pod.Spec.RequiredFeatures()`。

## 五、EventsToRegister 与 QueueingHint

```go
// pkg/scheduler/framework/plugins/nodedeclaredfeatures/nodedeclaredfeatures.go
	return []fwk.ClusterEventWithHint{
		{
			Event:          fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeDeclaredFeature},
			QueueingHintFn: pl.isSchedulableAfterNodeChange,
		},
		{
			Event:          fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.Update},
			QueueingHintFn: pl.isSchedulableAfterTargetPodUpdate,
		},
	}, nil
```

`UpdateNodeDeclaredFeature` 是专门为这个插件新增的节点事件类型。调度器的事件处理器在 gate 开启时比较新旧节点的 `declaredFeatures`：

```go
// pkg/scheduler/framework/events.go
func extractNodeFeaturesChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	// DeclaredFeatures is maintained in a sorted order, so slice comparison is safe.
	if !slices.Equal(oldNode.Status.DeclaredFeatures, newNode.Status.DeclaredFeatures) {
		return fwk.UpdateNodeDeclaredFeature
	}
	return fwk.None
}
```

没有这个专门的事件类型，节点 status 的变化（包括每次心跳）只能被粗粒度地归类，被这个插件拒绝的 Pod 要么收不到通知，要么被大量无关的节点更新反复唤醒。

hint 的判断都很直接：

- `isSchedulableAfterNodeChange`：新增节点一律 `Queue`（并不检查新节点是否满足需求）；节点更新时，`declaredFeatures` 不变返回 `QueueSkip`，变了返回 `Queue`。典型场景是节点 kubelet 升级重启后重新上报。
- `isSchedulableAfterTargetPodUpdate`：对新旧 Pod spec 各推断一次，需求位图相同返回 `QueueSkip`，不同返回 `Queue`。

## 六、另外两道检查：kubelet 准入与 apiserver 准入

调度器只在 Pod 选节点时检查一次。同一个库还被另外两处调用，覆盖调度器管不到的环节。

### 6.1 kubelet 准入

kubelet 注册了一个 `declaredFeaturesAdmitHandler`，Pod 到达节点时用**自己**的位图再检查一次：

```go
// pkg/kubelet/lifecycle/handlers.go
func (c *declaredFeaturesAdmitHandler) Admit(_ context.Context, attrs *PodAdmitAttributes) PodAdmitResult {
	pod := attrs.Pod

	podInfo := &ndf.PodInfo{Spec: &pod.Spec, Status: &pod.Status}
	reqs, err := c.ndfFramework.InferForPodScheduling(podInfo, c.version)
	// ...
	matchResult, err := c.ndfFramework.MatchNodeFeatureSet(reqs, c.ndfSet)
	// ...
	if !matchResult.IsMatch {
		return PodAdmitResult{
			Admit:   false,
			Reason:  PodFeatureUnsupported,
			Message: fmt.Sprintf("Pod requires node features that are not available: %s", strings.Join(matchResult.UnsatisfiedRequirements, ", ")),
		}
	}

	return PodAdmitResult{Admit: true}
}
```

这道检查兜住了绕过调度器的路径：直接写 `spec.nodeName` 的 Pod、静态 Pod，以及使用不带此插件的第三方调度器。被拒绝的 Pod 会被 kubelet 置为 `Failed`，原因 `PodFeatureUnsupported`，这里用的是会计算差集的 `MatchNodeFeatureSet`，消息里列出缺少的特性名。

### 6.2 apiserver 准入：Pod 更新

原地扩缩容这类特性在调度时不需要，在**运行中修改**时才需要。负责这一环的是 apiserver 的准入插件 `NodeDeclaredFeatureValidator`，它在默认开启的准入插件列表里，只在 gate 开启时生效：

```go
// plugin/pkg/admission/nodedeclaredfeatures/admission.go
func (p *Plugin) validatePod(a admission.Attributes) error {
	if a.GetOperation() != admission.Update {
		return nil
	}

	// Only validate updates to the main pod spec (subresource == "")
	// or the custom "resize" subresource.
	subresource := a.GetSubresource()
	if subresource != "" && subresource != "resize" {
		return nil
	}
	// ...
	// We only care about pods that are already bound to a node.
	if len(pod.Spec.NodeName) == 0 {
		return nil
	}
	// ...
	if oldPod.Generation == pod.Generation {
		// since generation is only incremented when the spec changes, we can skip validation if it doesnt.
		return nil
	}
	// ...
	return p.validatePodUpdate(pod, oldPod, a)
}
```

只处理已绑定 Pod 的 spec 更新和 `resize` 子资源，用 `generation` 是否变化快速跳过 status 更新。`validatePodUpdate` 调用 `InferForPodUpdate(old, new)`，从 node lister 取出 Pod 所在节点做 `MatchNode`，不满足就返回 `Forbidden`：`pod update requires features ... which are not available on node ...`。这样一个跑在旧节点上的 Pod 发起 Pod 级资源的原地扩容，请求在 API 层就被拒绝，而不是写进去之后 kubelet 处理不了。

kubelet 收到 Pod 更新时也会做一次同样的推断（`HandlePodUpdates` 中），不满足时只记录一个 `FailedNodeDeclaredFeaturesCheck` 事件，作为准入插件之外的可观测性补充。

同一个准入插件还顺带校验 `ResourceSlice`：设备声明了 node allocatable 资源时，要求它对应的节点声明了 `DRANodeAllocatableResources`。这是注册表里「对调度和更新都不施加约束」的特性被消费的地方之一。

## 七、边界情况与坑

**注册表是编译进二进制的。** 用户不能声明自定义特性，也不能给已有特性改推断规则。第三方调度器如果要复用这个插件，需要 vendor 与集群版本匹配的 `component-helpers`，否则注册表长度不同，位图大小不一致——`IsSubset` 遇到大小不一致会返回错误。不过调度器的位图（`NodeInfo.DeclaredFeatures` 和 `reqs`）都来自同一个 `DefaultFramework`，大小必然一致，这个错误分支只在误用时触发。

**一个 PreFilter 错误会让 Pod 进入退避。** `InferForPodScheduling` 只在版本为 nil 时返回错误，正常不会发生；但如果发生，PreFilter 返回的是 `Error` 而不是 `Unschedulable`，Pod 走错误退避。

**节点没有声明任何特性。** gate 关闭的旧 kubelet 不会写 `declaredFeatures`，`TryMap` 得到全 0 的位图。Pod 只要需要任何一个特性，这类节点就全部不可行。这是期望的行为，但在「控制面已开 gate、所有节点都还是旧版本」的过渡期，带新字段的 Pod 会全部 Pending，`kubectl describe` 里看到的是 `node(s) didn't match Pod's required features`。

**特性需求不等于字段可用。** apiserver 接受带新字段的 Pod，前提是 apiserver 侧对应的 gate 开启；`NodeDeclaredFeatures` 只负责「放到能处理它的节点上」。如果 apiserver 侧对应的 gate 关闭，这类新字段通常在写入时就被丢弃，推断结果为空，插件也就无从起作用。

## 八、发散

### 8.1 与 Node Feature Discovery 的对比

社区项目 Node Feature Discovery（NFD）也是「节点声明能力、Pod 按能力选节点」，但路径完全不同：NFD 探测硬件和内核特性（CPU 指令集、PCI 设备、内核模块），写成节点**标签**，Pod 用 `nodeSelector`/`nodeAffinity` **显式**选择。`NodeDeclaredFeatures` 只管 Kubernetes 自身功能，写在节点 **status** 里，需求从 Pod spec **隐式**推断。前者面向工作负载的硬件需求，后者面向控制面与节点之间的版本兼容，两者互不替代。

用 status 而不是标签有一个实际原因：标签可以被任何有 node 写权限的人修改，status 只由 kubelet 上报。一个由 kubelet 版本和配置决定的事实，放在只有 kubelet 写的地方更可靠。

### 8.2 与 RuntimeClass 的对比

`RuntimeClass` 的 `scheduling.nodeSelector` 解决的是「这个运行时只装在部分节点上」，同样是把节点能力和 Pod 需求关联起来。但它要求运维为每个 RuntimeClass 维护节点标签，`NodeDeclaredFeatures` 则完全自动。可以预见，未来依赖 CRI 能力的新功能（例如 `UserNamespacesHostNetworkSupport` 对运行时的要求）会更多地走 `declaredFeatures`。

### 8.3 对升级流程的意义

有了这个插件，「控制面先升级、开 gate，节点慢慢滚动」的流程不再需要额外的协调：新功能的 Pod 会自动落到已升级的节点上，旧节点只接收不需要新功能的 Pod。代价是新功能的 Pod 在升级早期可调度节点很少，需要关注它们是否因为容量不足而 Pending。另一方面，如果某个节点因为回滚 kubelet 失去了某个特性，已经在上面运行的 Pod 不会被驱逐（调度器只在调度时检查，kubelet 准入只在 Pod 到达时检查），但它们之后的原地更新会被 apiserver 准入拒绝。

## 小结

- `NodeDeclaredFeatures` 解决版本偏差期间「新功能 Pod 被调度到不支持它的 kubelet」的问题，需求从 Pod spec 自动推断，不需要用户写任何选择器。
- 推断和发现的逻辑都在 `component-helpers/nodedeclaredfeatures` 的注册表里，每个特性同时实现 `Discover` 和 `InferForScheduling`/`InferForUpdate`；排序后的注册表把名字映射成位图，Filter 只做一次 `&^` 运算。
- kubelet 启动时声明一次，写进 `status.declaredFeatures`；调度器在 `NodeInfo.SetNode` 时用 `TryMap` 转成位图，未知名字丢弃。
- 不满足返回 `UnschedulableAndUnresolvable`；为了性能，Filter 不计算缺少的特性名，原因字符串是固定的。
- `MaxVersion` 让特性在 GA 并度过版本偏差窗口后同时退出声明和检查；推断侧用调度器版本，发现侧用 kubelet 版本。
- 同一个库还被 kubelet 准入（兜住绕过调度器的 Pod）和 apiserver 准入插件 `NodeDeclaredFeatureValidator`（拦截已绑定 Pod 的不兼容更新）使用。

下一篇的 [DeferredPodScheduling](/posts/sched-plugin-17-deferred-pod-scheduling/) 也和原地扩缩容有关，但它处理的是另一个问题：节点资源不够时，谁来腾地方。
