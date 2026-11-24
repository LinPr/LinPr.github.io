---
title: 'scheduler-plugins 08 [in-tree] NodeResourcesFit：资源够不够与该往哪儿放'
date: 2026-11-19T09:20:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "NodeResourcesFit", "资源管理", "源码分析"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NodeResourcesFit 是默认调度链里计算量最大的插件：它先回答 Pod 到底请求了多少资源（init 容器、sidecar、pod 级 resources、overhead、扩展资源），再判断节点放不放得下并决定失败能否靠抢占解决，最后用 LeastAllocated、MostAllocated、RequestedToCapacityRatio 三种策略打分。本文按源码逐层拆解，附算例，并讲清 QueueingHint 如何识别节点变大、Pod 缩小以及 in-place resize 带来的新逻辑。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `NodeResourcesFit` 实现的扩展点（PreFilter、Filter、PreScore、Score、PlacementScore）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/08.svg" link="/images/posts/sched-plugins/callstack/08.svg" caption="NodeResourcesFit 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-07-node-ports/)的 `NodePorts` 查的是一张端口表，节点上的每个端口只有「占用」和「空闲」两种状态。`NodeResourcesFit` 面对的是连续的量：CPU、内存、临时存储、hugepages、GPU 等扩展资源。它要回答三个问题：

1. 这个 Pod 到底请求了多少资源？
2. 节点放得下吗？放不下的话，驱逐一些 Pod 能不能放下？
3. 能放下的节点里，放哪个更好？

第一个问题看起来最简单，实际最容易出错。本文基于 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期），先把第一个问题讲透，再按扩展点顺序拆后两个。

## 一、在调度链中的位置

`NodeResourcesFit` 在默认 Profile 的 `MultiPoint` 中排第八，紧跟 `NodePorts`，打分权重为 1：

```go
// pkg/scheduler/apis/config/v1/default_plugins.go
{Name: names.NodePorts},
{Name: names.NodeResourcesFit, Weight: ptr.To[int32](1)},
{Name: names.VolumeRestrictions},
```

它实现的接口是默认插件里最多的一批：

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
var _ fwk.PreFilterPlugin = &Fit{}
var _ fwk.FilterPlugin = &Fit{}
var _ fwk.EnqueueExtensions = &Fit{}
var _ fwk.PreScorePlugin = &Fit{}
var _ fwk.ScorePlugin = &Fit{}
var _ fwk.SignPlugin = &Fit{}
var _ fwk.PlacementScorePlugin = &Fit{}
```

插件本身没有 feature gate，默认启用，但它的行为受一串 gate 影响，后文会逐个碰到：

| feature gate | 在 aabdf5a2 的状态 | 影响 |
| --- | --- | --- |
| `PodLevelResources` | Beta，默认开 | Pod 级 `spec.resources` 参与请求量计算 |
| `InPlacePodVerticalScaling` | GA，锁定开 | 节点上已有 Pod 的请求量取 spec/allocated/actuated 的最大值；订阅 Pod 缩容事件 |
| `InPlacePodLevelResourcesVerticalScaling` | Beta，默认开 | Pod 级 resources 的原地扩缩容 |
| `DRAExtendedResource` | GA，锁定开 | 由 DRA 提供的扩展资源交给 `DynamicResources` 处理 |
| `InPlacePodVerticalScalingSchedulerPreemption` | Alpha，默认关 | resize 被推迟的 Pod 重新进入调度器，在原节点上抢占 |
| `TopologyAwareWorkloadScheduling` | Alpha，默认关 | 启用 `PlacementScore`，为 PodGroup 的放置方案打分 |

同一个包里还有 `NodeResourcesBalancedAllocation`，两者共用一个打分骨架 `resourceAllocationScorer`。BalancedAllocation 只打分，按故事顺序放在抢占之后的 [第 19 篇](/posts/sched-plugin-19-balanced-allocation/)。

## 二、Pod 的请求量怎么算

`kubectl describe pod` 里的 `0/10 nodes are available: 10 Insufficient cpu`，比较的是「Pod 的请求量」和「节点剩余量」。前者不是把所有容器的 `requests` 加起来那么简单。

### 2.1 入口：ComputePodResourceRequest

```go
// pkg/scheduler/framework/plugins/helper/resource_fit.go
func ComputePodResourceRequest(pod *v1.Pod, opts ResourceRequestsOptions) framework.Resource {
	// pod hasn't scheduled yet so we don't need to worry about InPlacePodVerticalScalingEnabled
	reqs := resource.PodRequests(pod, resource.PodResourcesOptions{
		// SkipPodLevelResources is set to false when PodLevelResources feature is enabled.
		SkipPodLevelResources:                    !opts.EnablePodLevelResources,
		UseDRANodeAllocatableResourceClaimStatus: opts.EnableDRANodeAllocatableResources,
	})
	var result framework.Resource
	result.SetMaxResource(reqs)
	return result
}
```

这个函数放在 `plugins/helper` 而不是插件包里，是因为 kubelet 准入也要用它（源码注释说明了理由：让 kubelet 不必引入插件的 DRA 依赖）。真正的计算在 `k8s.io/component-helpers/resource` 的 `PodRequests`，这是 apiserver、kubelet、调度器、ResourceQuota 共用的一份实现，函数注释写着 `The computation is part of the API and must be reviewed as an API change`。

```go
// staging/src/k8s.io/component-helpers/resource/helpers.go
func PodRequests(pod *v1.Pod, opts PodResourcesOptions) v1.ResourceList {
	reqs := v1.ResourceList{}
	if !opts.SkipContainerLevelResources {
		reqs = AggregateContainerRequests(pod, opts)
	}

	if !opts.SkipPodLevelResources && IsPodLevelRequestsSet(pod) {
		effectiveReqs := pod.Spec.Resources.Requests
		// ...（in-place resize 时取 spec 与 status 的最大值）
		applyPodLevelResources(reqs, effectiveReqs)
	}

	// Add overhead for running a pod to the sum of requests if requested:
	if !opts.ExcludeOverhead && pod.Spec.Overhead != nil {
		addResourceList(reqs, pod.Spec.Overhead)
	}

	return reqs
}
```

三步：容器级聚合，Pod 级覆盖，加 overhead。

### 2.2 容器级聚合：sidecar 与 init 容器

```go
// staging/src/k8s.io/component-helpers/resource/helpers.go
	result := v1.ResourceList{}
	for _, container := range pod.Spec.Containers {
		// ...
		addResourceList(result, containerResources)
	}

	restartableInitContainerResources := v1.ResourceList{}
	initContainerResources := v1.ResourceList{}
	// init containers define the minimum of any resource
	//
	// Let's say `InitContainerUse(i)` is the resource requirements when the i-th
	// init container is initializing, then
	// `InitContainerUse(i) = sum(Resources of restartable init containers with index < i) + Resources of i-th init container`.
	for _, container := range pod.Spec.InitContainers {
		// ...
		containerResources := getResourceList(&container, cs, isResizeInfeasible)
		// Is the init container marked as a restartable init container?
		if isRestartableInitContainer(&container) {
			// and add them to the resulting cumulative container requests
			addResourceList(result, containerResources)

			// track our cumulative restartable init container resources
			addResourceList(restartableInitContainerResources, containerResources)
			containerResources = restartableInitContainerResources
		} else {
			combinedResources := v1.ResourceList{}
			addResourceList(combinedResources, containerResources)
			addResourceList(combinedResources, restartableInitContainerResources)
			containerResources = combinedResources
		}
		maxResourceList(initContainerResources, containerResources)
	}
	maxResourceList(result, initContainerResources)
	return result
```

把它翻译成 Pod 的生命周期：

- **稳态**：所有应用容器和所有 sidecar（`restartPolicy: Always` 的 init 容器）同时运行，资源相加，这就是 `result`。
- **初始化阶段**：init 容器按顺序执行。第 i 个普通 init 容器运行时，排在它前面的 sidecar 已经启动并常驻，所以这一刻的占用是「它自己 + 前面所有 sidecar」。sidecar 本身启动的那一刻，占用是「它和前面所有 sidecar 之和」。
- 每个阶段都算出一个资源向量，最后**逐维度**取最大值。

{{< figure src="/images/posts/sched-plugins/08-request-composition.svg" caption="一个带 sidecar、普通 init 容器和 overhead 的 Pod：CPU 的峰值出现在 init 阶段，内存的峰值出现在稳态，两者各取各的（点击查看大图）" link="/images/posts/sched-plugins/08-request-composition.svg" >}}

图里的 Pod 对应的 YAML 是：

```yaml
spec:
  runtimeClassName: kata          # RuntimeClass 定义的 overhead: cpu 100m, memory 120Mi
  initContainers:
  - name: log-agent
    restartPolicy: Always         # sidecar
    resources: {requests: {cpu: 200m, memory: 64Mi}}
  - name: migrate
    resources: {requests: {cpu: "1", memory: 512Mi}}
  containers:
  - name: app
    resources: {requests: {cpu: 500m, memory: 512Mi}}
  - name: metrics
    resources: {requests: {cpu: 300m, memory: 128Mi}}
```

算一遍：

| 阶段 | CPU | 内存 |
| --- | --- | --- |
| log-agent 启动 | 200m | 64Mi |
| migrate 运行（+ 已启动的 log-agent） | 1200m | 576Mi |
| 稳态（app + metrics + log-agent） | 1000m | 704Mi |
| 逐维度取最大 | 1200m | 704Mi |
| 加 overhead | **1300m** | **824Mi** |

CPU 的峰值来自 init 阶段，内存的峰值来自稳态，结果向量 `(1300m, 824Mi)` 在 Pod 生命周期里任何一刻都没有同时出现过。这是有意的保守：调度器不知道各阶段的持续时间，只能保证每个维度在最坏时刻都放得下。

如果把 `log-agent` 写成普通 init 容器，它跑完就退出，稳态只剩 800m/640Mi，init 阶段的峰值变成 `max(200m, 1000m)` = 1000m 和 `max(64Mi, 512Mi)` = 512Mi，最终是 1100m/760Mi。sidecar 的成本在于它在 init 阶段和稳态都要算。

### 2.3 Pod 级 resources 覆盖

`PodLevelResources`（Beta，默认开）允许在 `spec.resources` 上直接写 Pod 的总请求：

```go
// staging/src/k8s.io/component-helpers/resource/helpers.go
var supportedPodLevelResources = sets.New(v1.ResourceCPU, v1.ResourceMemory)

func applyPodLevelResources(result, effectiveResources v1.ResourceList) {
	for resourceName, quantity := range effectiveResources {
		if IsSupportedPodLevelResource(resourceName) {
			result[resourceName] = quantity
		}
	}
}
```

Pod 级的值**覆盖**（不是相加）容器级聚合的结果，而且只对 CPU、内存和 hugepages 生效。写在 Pod 级的扩展资源或临时存储会被忽略，这些维度仍然以容器级聚合为准。

### 2.4 overhead

`spec.overhead` 由 RuntimeClass 准入控制器在创建 Pod 时根据 `runtimeClassName` 填入，代表沙箱运行时（Kata、gVisor 等）自身的开销。它在最后**相加**，并且在 Pod 级覆盖之后，所以 Pod 级 resources 写 2 核，overhead 100m，最终请求是 2100m。

### 2.5 落到 Resource 结构体

`PodRequests` 返回的是 `v1.ResourceList`（`map[ResourceName]Quantity`），调度器把它转成定长的结构体，避免在热路径上做 map 查找和 Quantity 运算：

```go
// pkg/scheduler/framework/types.go
func (r *Resource) SetMaxResource(rl v1.ResourceList) {
	if r == nil {
		return
	}

	for rName, rQuantity := range rl {
		switch rName {
		case v1.ResourceMemory:
			r.Memory = max(r.Memory, rQuantity.Value())
		case v1.ResourceCPU:
			r.MilliCPU = max(r.MilliCPU, rQuantity.MilliValue())
		case v1.ResourceEphemeralStorage:
			r.EphemeralStorage = max(r.EphemeralStorage, rQuantity.Value())
		default:
			if schedutil.IsScalarResourceName(rName) {
				r.SetScalar(rName, max(r.ScalarResources[rName], rQuantity.Value()))
			}
		}
	}
}
```

CPU 用毫核，内存和存储用字节。其他资源只要满足 `IsScalarResourceName` 就放进 `ScalarResources`：

```go
// pkg/scheduler/util/utils.go
func IsScalarResourceName(name v1.ResourceName) bool {
	return v1helper.IsExtendedResourceName(name) || v1helper.IsHugePageResourceName(name) ||
		v1helper.IsPrefixedNativeResource(name) || v1helper.IsAttachableVolumeResourceName(name)
}
```

扩展资源（`nvidia.com/gpu`、`example.com/foo`）、hugepages（`hugepages-2Mi`）、带 `kubernetes.io/` 前缀的原生资源、`attachable-volumes-*` 都在这里。不符合任何一类的资源名会被静默丢弃，调度器既不检查也不报错。

### 2.6 节点上已有 Pod：用的是另一套口径

待调度 Pod 的请求在 PreFilter 里算一次。节点上已有 Pod 的请求由 `NodeInfo` 在 Pod 加入时累加到 `Requested` 和 `NonZeroRequested`，计算入口是 `PodInfo.CalculateResource`：

```go
// pkg/scheduler/framework/types.go
func (pi *PodInfo) CalculateResource() fwk.PodResource {
	if pi.cachedResource != nil {
		return *pi.cachedResource
	}
	inPlacePodVerticalScalingEnabled := utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling)
	// ...
	requests := resourcehelper.PodRequests(pi.Pod, resourcehelper.PodResourcesOptions{
		UseStatusResources: inPlacePodVerticalScalingEnabled,
		// ...
	})
	isPodLevelResourcesSet := podLevelResourcesEnabled && resourcehelper.IsPodLevelRequestsSet(pi.Pod)
	nonMissingContainerRequests := getNonMissingContainerRequests(requests, isPodLevelResourcesSet)
	non0Requests := requests
	if len(nonMissingContainerRequests) > 0 {
		non0Requests = resourcehelper.PodRequests(pi.Pod, resourcehelper.PodResourcesOptions{
			// ...
			NonMissingContainerRequests:              nonMissingContainerRequests,
		})
	}
	// ...
	podResource := fwk.PodResource{
		Resource: &res,
		Non0CPU:  non0CPU.MilliValue(),
		Non0Mem:  non0Mem.Value(),
	}
	pi.cachedResource = &podResource
	return podResource
}
```

两个差别：

- **`UseStatusResources: true`。** 已经在节点上运行的 Pod 可能正在原地扩缩容，`spec` 里写的是期望值，`status.containerStatuses[].allocatedResources` 是 kubelet 已经接受的值，`status.containerStatuses[].resources` 是实际生效的值。`AggregateContainerRequests` 在这种模式下取三者的最大值（resize 被判定为 Infeasible 时不看 spec）。调度器按最大值记账，宁可高估也不低估。
- **同时算出一个「非零」版本。** 这个版本只给打分用，见第六节。

结果被缓存在 `PodInfo.cachedResource` 里，Pod 更新时清空。

## 三、结构体、工厂与参数

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
type Fit struct {
	ignoredResources                                   sets.Set[string]
	ignoredResourceGroups                              sets.Set[string]
	enableInPlacePodVerticalScaling                    bool
	enablePodLevelResources                            bool
	enableDRAExtendedResource                          bool
	enableInPlacePodLevelResourcesVerticalScaling      bool
	enableInPlacePodVerticalScalingSchedulerPreemption bool
	handle                                             fwk.Handle
	*resourceAllocationScorer
	placementScorer *resourceAllocationScorer
}
```

参数类型：

```go
// pkg/scheduler/apis/config/types_pluginargs.go
type NodeResourcesFitArgs struct {
	metav1.TypeMeta

	// IgnoredResources is the list of resources that NodeResources fit filter
	// should ignore.
	IgnoredResources []string
	// IgnoredResourceGroups defines the list of resource groups that NodeResources fit filter should ignore.
	// e.g. if group is ["example.com"], it will ignore all resource names that begin
	// with "example.com", such as "example.com/aaa" and "example.com/bbb".
	// A resource group name can't contain '/'.
	IgnoredResourceGroups []string

	// ScoringStrategy selects the node resource scoring strategy.
	ScoringStrategy *ScoringStrategy
}
```

默认值只填打分策略：

```go
// pkg/scheduler/apis/config/v1/defaults.go
var defaultResourceSpec = []configv1.ResourceSpec{
	{Name: string(v1.ResourceCPU), Weight: 1},
	{Name: string(v1.ResourceMemory), Weight: 1},
}

func SetDefaults_NodeResourcesFitArgs(obj *configv1.NodeResourcesFitArgs) {
	if obj.ScoringStrategy == nil {
		obj.ScoringStrategy = &configv1.ScoringStrategy{
			Type:      configv1.ScoringStrategyType(config.LeastAllocated),
			Resources: defaultResourceSpec,
		}
	}
	if len(obj.ScoringStrategy.Resources) == 0 {
		// If no resources specified, use the default set.
		obj.ScoringStrategy.Resources = append(obj.ScoringStrategy.Resources, defaultResourceSpec...)
	}
	for i := range obj.ScoringStrategy.Resources {
		if obj.ScoringStrategy.Resources[i].Weight == 0 {
			obj.ScoringStrategy.Resources[i].Weight = 1
		}
	}
}
```

默认是 `LeastAllocated`，CPU 和内存权重各 1。`ValidateNodeResourcesFitArgs` 的校验规则：

- `ignoredResources` 每项必须是合法的标签名；`ignoredResourceGroups` 不能包含 `/`。
- `scoringStrategy.type` 只能是三种之一；每个资源的权重必须在 `(0, 100]`。
- `type` 为 `RequestedToCapacityRatio` 时必须给 `shape`，否则必须**不**给。`shape` 的点按利用率严格递增，利用率在 0 到 100，分数在 0 到 10（`MaxCustomPriorityScore`）。

`NewFit` 根据策略类型从一张表里取出打分器：

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
var nodeResourceStrategyTypeMap = map[config.ScoringStrategyType]scorer{
	config.LeastAllocated: func(strategy *config.ScoringStrategy) *resourceAllocationScorer {
		resources := strategy.Resources
		return &resourceAllocationScorer{
			Name:      string(config.LeastAllocated),
			scorer:    leastResourceScorer(resources),
			resources: resources,
		}
	},
	// MostAllocated、RequestedToCapacityRatio 同理
}
```

`TopologyAwareWorkloadScheduling` 打开时，还会额外构造一个固定为 `MostAllocated` 的 `placementScorer`，供 `ScorePlacement` 使用（见 6.6 节）。

## 四、PreFilter：只算不判

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
// preFilterState computed at PreFilter and used at Filter.
type preFilterState struct {
	framework.Resource
}

// Clone the prefilter state.
func (s *preFilterState) Clone() fwk.StateData {
	return s
}

func (f *Fit) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	result := computePodResourceRequest(pod, ResourceRequestsOptions{EnablePodLevelResources: f.enablePodLevelResources})

	cycleState.Write(preFilterStateKey, result)
	return nil, nil
}
```

和前几个插件不同，`NodeResourcesFit` 的 PreFilter **从不返回 `Skip`**。即使 Pod 一点资源都不请求，Filter 也要检查节点上的 Pod 数量是否已达上限（第五节）。

CycleState 的 key 是 `"PreFilterNodeResourcesFit"`，值是待调度 Pod 的 `Resource`。`Clone` 返回自身，`PreFilterExtensions()` 返回 `nil`，理由和 `NodePorts` 一样：节点那一侧的 `Requested` 存在 `NodeInfo` 里，抢占模拟调用 `NodeInfo.RemovePod` 时会自动减掉受害者的请求量，插件不需要维护任何跨节点的预计算状态。

## 五、Filter：比剩余，判可解

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
func (f *Fit) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	var draManager fwk.SharedDRAManager
	if f.enableDRAExtendedResource {
		draManager = f.handle.SharedDRAManager()
	}

	opts := ResourceRequestsOptions{
		EnablePodLevelResources:                            f.enablePodLevelResources,
		EnableDRAExtendedResource:                          f.enableDRAExtendedResource,
		EnableInPlacePodVerticalScalingSchedulerPreemption: f.enableInPlacePodVerticalScalingSchedulerPreemption,
	}

	insufficientResources := helper.FitsRequest(&s.Resource, nodeInfo, f.ignoredResources, f.ignoredResourceGroups, draManager, opts, pod)

	if len(insufficientResources) != 0 {
		// We will keep all failure reasons.
		failureReasons := make([]string, 0, len(insufficientResources))
		statusCode := fwk.Unschedulable
		for i := range insufficientResources {
			failureReasons = append(failureReasons, insufficientResources[i].Reason)

			if insufficientResources[i].Unresolvable {
				statusCode = fwk.UnschedulableAndUnresolvable
			}
		}

		return fwk.NewStatus(statusCode, failureReasons...)
	}

	return nil
}
```

真正的判断在 `helper.FitsRequest`。

{{< figure src="/images/posts/sched-plugins/08-fit-filter.svg" caption="Filter 的两条分界线：请求超过剩余量就失败，请求超过节点 allocatable 才是不可解决；下半部分是 FitsRequest 的检查顺序（点击查看大图）" link="/images/posts/sched-plugins/08-fit-filter.svg" >}}

### 5.1 Pod 数量

```go
// pkg/scheduler/framework/plugins/helper/resource_fit.go
	allowedPodNumber := nodeInfo.GetAllocatable().GetAllowedPodNumber()
	if len(nodeInfo.GetPods())+1 > allowedPodNumber {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourcePods,
			Reason:       "Too many pods",
			Requested:    1,
			Used:         int64(len(nodeInfo.GetPods())),
			Capacity:     int64(allowedPodNumber),
		})
	}
```

`allowedPodNumber` 来自 `node.status.allocatable.pods`，即 kubelet 的 `maxPods`（默认 110）。注意 `Unresolvable` 没有设置，恒为 `false`：Pod 数满了，驱逐一个就能腾出位置，可以抢占。

### 5.2 全零请求的捷径

```go
// pkg/scheduler/framework/plugins/helper/resource_fit.go
	if podRequest.MilliCPU == 0 &&
		podRequest.Memory == 0 &&
		podRequest.EphemeralStorage == 0 &&
		len(podRequest.ScalarResources) == 0 {
		return insufficientResources
	}
```

BestEffort Pod（所有容器都不写 requests）在这里直接返回，只受 Pod 数量约束。这意味着从 Filter 的角度看，一个已经被请求到 100% 的节点仍然可以无限地接收 BestEffort Pod，直到 `maxPods`。

### 5.3 CPU、内存、临时存储

```go
// pkg/scheduler/framework/plugins/helper/resource_fit.go
	deltaMilliCPU, deltaMemory, deltaEphemeralStorage, deltaScalarResources := adjustDeltasToAccomodateCacheDiscrepancy(opts, podRequest, nodeInfo, pod)

	if podRequest.MilliCPU > 0 && deltaMilliCPU > (nodeInfo.GetAllocatable().GetMilliCPU()-nodeInfo.GetRequested().GetMilliCPU()) {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourceCPU,
			Reason:       "Insufficient cpu",
			Requested:    podRequest.MilliCPU,
			Used:         nodeInfo.GetRequested().GetMilliCPU(),
			Capacity:     nodeInfo.GetAllocatable().GetMilliCPU(),
			Unresolvable: podRequest.MilliCPU > nodeInfo.GetAllocatable().GetMilliCPU(),
		})
	}
	// 内存、临时存储完全同构
```

判定式是 `delta > allocatable − requested`。对普通的待调度 Pod，`delta` 就是 Pod 的请求量（`adjustDeltas…` 只对 in-place resize 的特殊 Pod 起作用，见第八节）。

有三点值得注意：

- 用的是 `allocatable`，不是 `capacity`。`allocatable = capacity − kube-reserved − system-reserved − 驱逐阈值`，由 kubelet 计算并上报。
- 用的是 `Requested`，不是 `NonZeroRequested`。没写 requests 的容器在这里按 0 计算。
- 「已请求」包括 assumed Pod：调度器在 Bind 完成前就把 Pod 假定放进了缓存，后续 Pod 看到的是扣除之后的剩余量。

`Unresolvable` 的判定是 `podRequest > allocatable`：请求量比整个节点还大，清空节点也放不下，抢占没有意义。注意这里比较的是 Pod 的完整请求量 `podRequest`，而不是 `delta`。

### 5.4 标量资源与 ignoredResources

```go
// pkg/scheduler/framework/plugins/helper/resource_fit.go
	for rName, rQuant := range deltaScalarResources {
		// Skip in case request quantity is zero
		if rQuant == 0 {
			continue
		}

		if v1helper.IsExtendedResourceName(rName) {
			// If this resource is one of the extended resources that should be ignored, we will skip checking it.
			// rName is guaranteed to have a slash due to API validation.
			var rNamePrefix string
			if ignoredResourceGroups.Len() > 0 {
				rNamePrefix = strings.Split(string(rName), "/")[0]
			}
			if ignoredExtendedResources.Has(string(rName)) || ignoredResourceGroups.Has(rNamePrefix) {
				continue
			}
		}

		if ShouldDelegateResourceToDRA(rName, nodeInfo, draManager, opts) {
			continue
		}
		if podRequest.ScalarResources[rName] > 0 && rQuant > (nodeInfo.GetAllocatable().GetScalarResources()[rName]-nodeInfo.GetRequested().GetScalarResources()[rName]) {
			insufficientResources = append(insufficientResources, InsufficientResource{
				ResourceName: rName,
				Reason:       fmt.Sprintf("Insufficient %v", rName),
				// ...
				Unresolvable: rQuant > nodeInfo.GetAllocatable().GetScalarResources()[rName],
			})
		}
	}
```

`ignoredResources` 和 `ignoredResourceGroups` **只对扩展资源生效**，外面包着 `IsExtendedResourceName` 判断。把 `cpu` 或 `hugepages-2Mi` 写进 `ignoredResources` 没有任何效果（`cpu` 根本不走这个循环，hugepages 不是扩展资源）。

这个参数的典型用途是配合调度器扩展（extender）：某种扩展资源由外部组件管理，节点 `allocatable` 里不上报，调度器默认会因为「节点有 0 个」而拒绝所有请求它的 Pod，把它加入 `ignoredResources` 就能跳过检查。

节点根本没有某种扩展资源时，`allocatable` 取到 0，Pod 请求任何正数都会失败，而且 `Unresolvable` 为真（请求 > 0）。所以请求了 `nvidia.com/gpu` 的 Pod 在没有 GPU 的节点上返回 `UnschedulableAndUnresolvable`，抢占不会去这些节点上找受害者。

`ShouldDelegateResourceToDRA` 处理的是 `DRAExtendedResource`（1.37 GA）：节点 `allocatable` 里没有这种资源，但集群里存在一个 `DeviceClass` 声明了 `extendedResourceName` 与之对应，说明这种资源由 DRA 驱动通过 ResourceSlice 提供。这时 `NodeResourcesFit` 跳过检查，交给 [DynamicResources](/posts/sched-plugin-15-dynamic-resources/) 处理。同一个资源名在设备插件节点上走老路径，在 DRA 节点上走新路径，工作负载不需要改 YAML。

### 5.5 失败原因与 kubelet 的视角

所有不足项都被收集，每一项的 `Reason` 都进入状态消息。一个节点可能同时报 `Insufficient cpu` 和 `Insufficient memory`，调度器汇总所有节点后得到常见的那条消息：

```text
0/5 nodes are available: 2 Insufficient cpu, 3 Insufficient memory, 1 Too many pods.
preemption: 0/5 nodes are available: 5 No preemption victims found for incoming pod.
```

kubelet 准入也会跑同样的检查（`helper.Fits`），对应的 Pod 失败 reason 是 `OutOfcpu`、`OutOfmemory`、`OutOfpods`，扩展资源是 `OutOf<资源名>`。在调度器正常工作时，kubelet 层面的拒绝通常意味着调度器缓存与 kubelet 的视角出现了偏差，例如节点 allocatable 刚刚变小、或者有绕过调度器直接写 `nodeName` 的 Pod。

## 六、Score：三种策略

### 6.1 PreScore 与「非零」请求

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
func (f *Fit) PreScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	podRequests := f.calculatePodResourceRequestList(pod, f.resources)
	state := &preScoreState{
		podRequests: podRequests,
	}
	// ...（DRA 扩展资源的预计算）
	cycleState.Write(preScoreStateKey, state)
	return nil
}
```

`podRequests` 是一个和 `ScoringStrategy.Resources` 顺序一一对应的 `[]int64`。计算用的是 `resourceAllocationScorer` 的方法：

```go
// pkg/scheduler/framework/plugins/noderesources/resource_allocation.go
func (r *resourceAllocationScorer) calculatePodResourceRequest(pod *v1.Pod, resourceName v1.ResourceName) int64 {

	opts := resourcehelper.PodResourcesOptions{
		UseStatusResources: r.enableInPlacePodVerticalScaling,
		InPlacePodLevelResourcesVerticalScalingEnabled: r.enableInPlacePodLevelResourcesVerticalScaling,
		// SkipPodLevelResources is set to false when PodLevelResources feature is enabled.
		SkipPodLevelResources: !r.enablePodLevelResources,
	}

	if !r.useRequested {
		opts.NonMissingContainerRequests = v1.ResourceList{
			v1.ResourceCPU:    *resource.NewMilliQuantity(schedutil.DefaultMilliCPURequest, resource.DecimalSI),
			v1.ResourceMemory: *resource.NewQuantity(schedutil.DefaultMemoryRequest, resource.DecimalSI),
		}
	}

	requests := resourcehelper.PodRequests(pod, opts)
	// ...
}
```

`useRequested` 对 `NodeResourcesFit` 为 `false`，于是 `NonMissingContainerRequests` 生效：**没有写 CPU request 的容器按 100m 计，没有写内存 request 的按 200MiB 计**。

```go
// pkg/scheduler/util/pod_resources.go
	// DefaultMilliCPURequest defines default milli cpu request number.
	DefaultMilliCPURequest int64 = 100 // 0.1 core
	// DefaultMemoryRequest defines default memory request size.
	DefaultMemoryRequest int64 = 200 * 1024 * 1024 // 200 MB
```

`applyNonMissing` 只替换「缺失」的请求，显式写了 `cpu: 0` 的容器保持 0。节点一侧对应的是 `NodeInfo.NonZeroRequested`，由 2.6 节里同样的规则累加。

为什么打分要用这套「虚构」的数字？因为 Filter 对 BestEffort Pod 只检查 Pod 数量（5.2 节），如果打分也按 0 计算，所有节点对 BestEffort Pod 来说利用率都没变，`LeastAllocated` 会持续给同一个最空的节点打最高分，大量 BestEffort Pod 会堆到一个节点上。按 100m/200MiB 计，每放一个，那个节点的分数就降一点，Pod 自然被摊开。

### 6.2 从节点取三个向量

```go
// pkg/scheduler/framework/plugins/noderesources/resource_allocation.go
func (r *resourceAllocationScorer) calculateNodeAllocatableRequest(
	ctx context.Context,
	nodeInfo fwk.NodeInfo,
	podRequests []int64,
	draPreScoreState *draPreScoreState,
) (requested []int64, allocated []int64, allocatable []int64) {
	requested = make([]int64, len(r.resources))
	allocated = make([]int64, len(r.resources))
	allocatable = make([]int64, len(r.resources))
	for i := range r.resources {
		resource := v1.ResourceName(r.resources[i].Name)
		// If it's an extended resource, and the pod doesn't request it.
		// We don't fill the resource entry as an implication to bypass scoring on it.
		if podRequests[i] == 0 && schedutil.IsScalarResourceName(resource) {
			continue
		}
		nodeAllocatable, nodeAllocated := r.calculateResourceAllocatableRequest(ctx, nodeInfo, resource, draPreScoreState)
		// Only fill the extended resource entry when it's non-zero.
		if nodeAllocatable == 0 {
			continue
		}
		allocatable[i] = nodeAllocatable
		allocated[i] = nodeAllocated
		requested[i] = allocated[i] + podRequests[i]
	}
	return requested, allocated, allocatable
}
```

- `allocated`：节点上已有的请求量。CPU/内存取 `NonZeroRequested`（因为 `useRequested` 为 false），临时存储和标量资源取 `Requested`。
- `requested`：放上这个 Pod 之后的请求量。
- `allocatable`：节点总量。

两种情况下某一项保持为 0、在打分时被跳过：Pod 不请求的标量资源（例如在 `resources` 里配了 GPU，但这个 Pod 不要 GPU），以及节点上 `allocatable` 为 0 的资源。前者很关键：如果不跳过，不要 GPU 的 Pod 在 `MostAllocated` 下会被 GPU 已满的节点吸引过去，把宝贵的 GPU 节点 CPU 也占掉。

### 6.3 LeastAllocated（默认）

```go
// pkg/scheduler/framework/plugins/noderesources/least_allocated.go
func leastResourceScorer(resources []config.ResourceSpec) func([]int64, []int64, []int64) int64 {
	return func(requested, _, allocable []int64) int64 {
		var nodeScore, weightSum int64
		for i := range requested {
			if allocable[i] == 0 {
				continue
			}
			weight := resources[i].Weight
			resourceScore := leastRequestedScore(requested[i], allocable[i])
			nodeScore += resourceScore * weight
			weightSum += weight
		}
		if weightSum == 0 {
			return 0
		}
		return nodeScore / weightSum
	}
}

func leastRequestedScore(requested, capacity int64) int64 {
	if capacity == 0 {
		return 0
	}
	if requested > capacity {
		return 0
	}

	return ((capacity - requested) * fwk.MaxNodeScore) / capacity
}
```

单项分数是剩余比例乘以 100，多项按权重加权平均，全程整数除法。越空的节点分越高，效果是把 Pod 摊开。

### 6.4 MostAllocated

```go
// pkg/scheduler/framework/plugins/noderesources/most_allocated.go
func mostRequestedScore(requested, capacity int64) int64 {
	if capacity == 0 {
		return 0
	}
	if requested > capacity {
		// `requested` might be greater than `capacity` because pods with no
		// requests get minimum values.
		requested = capacity
	}

	return (requested * fwk.MaxNodeScore) / capacity
}
```

和 `LeastAllocated` 镜像，越满分越高，用于装箱。注释点出了一个只在打分时才会出现的情况：因为非零默认值的存在，`NonZeroRequested` 可以超过 `allocatable`（Filter 用的是真实 `Requested`，所以放得进去），这时按满分处理。

### 6.5 RequestedToCapacityRatio

```go
// pkg/scheduler/framework/plugins/noderesources/requested_to_capacity_ratio.go
const maxUtilization = 100

func buildRequestedToCapacityRatioScorerFunction(scoringFunctionShape helper.FunctionShape, resources []config.ResourceSpec) func([]int64, []int64, []int64) int64 {
	rawScoringFunction := helper.BuildBrokenLinearFunction(scoringFunctionShape)
	resourceScoringFunction := func(requested, capacity int64) int64 {
		if capacity == 0 || requested > capacity {
			return rawScoringFunction(maxUtilization)
		}

		return rawScoringFunction(requested * maxUtilization / capacity)
	}
	return func(requested, _, allocable []int64) int64 {
		var nodeScore, weightSum int64
		for i := range requested {
			if allocable[i] == 0 {
				continue
			}
			weight := resources[i].Weight
			resourceScore := resourceScoringFunction(requested[i], allocable[i])
			if resourceScore > 0 {
				nodeScore += resourceScore * weight
				weightSum += weight
			}
		}
		if weightSum == 0 {
			return 0
		}
		return int64(math.Round(float64(nodeScore) / float64(weightSum)))
	}
}

func requestedToCapacityRatioScorer(resources []config.ResourceSpec, shape []config.UtilizationShapePoint) func([]int64, []int64, []int64) int64 {
	shapes := make([]helper.FunctionShapePoint, 0, len(shape))
	for _, point := range shape {
		shapes = append(shapes, helper.FunctionShapePoint{
			Utilization: int64(point.Utilization),
			// MaxCustomPriorityScore may diverge from the max score used in the scheduler and defined by MaxNodeScore,
			// therefore we need to scale the score returned by requested to capacity ratio to the score range
			// used by the scheduler.
			Score: int64(point.Score) * (fwk.MaxNodeScore / config.MaxCustomPriorityScore),
		})
	}

	return buildRequestedToCapacityRatioScorerFunction(shapes, resources)
}
```

通过 `shape` 定义一条分段线性函数：横轴利用率（0 到 100），纵轴分数（0 到 10，内部乘以 10 换算到 0 到 100）。插值函数：

```go
// pkg/scheduler/framework/plugins/helper/shape_score.go
func BuildBrokenLinearFunction(shape FunctionShape) func(int64) int64 {
	return func(p int64) int64 {
		for i := 0; i < len(shape); i++ {
			if p <= int64(shape[i].Utilization) {
				if i == 0 {
					return shape[0].Score
				}
				return shape[i-1].Score + (shape[i].Score-shape[i-1].Score)*(p-shape[i-1].Utilization)/(shape[i].Utilization-shape[i-1].Utilization)
			}
		}
		return shape[len(shape)-1].Score
	}
}
```

第一个点之前取第一个点的分数，最后一个点之后取最后一个点的分数，中间线性插值，同样是整数运算。`shape: [{0,0},{100,10}]` 等价于 `MostAllocated`，`[{0,10},{100,0}]` 等价于 `LeastAllocated`。它真正的价值在于非单调的形状，比如「装到 70% 最好，再满就开始扣分」，给突发流量留余量。

它和前两种策略有两处实现差异：

- 最终平均用 `math.Round` 四舍五入，前两种是整数除法向下取整。
- **只有得分大于 0 的资源计入加权平均**（`if resourceScore > 0`）。用 `[{0,10},{100,0}]` 模拟 `LeastAllocated` 时，一个 CPU 已满（得 0 分）、内存用了 20%（得 80 分）的节点，`LeastAllocated` 给 `(0 + 80) / 2 = 40`，`RequestedToCapacityRatio` 给 `80 / 1 = 80`。某种资源被打成 0 分，等于从平均里消失，节点反而显得更好。用它模拟 `LeastAllocated` 时这个差异影响很大；用作装箱时（低利用率得低分），影响是「完全空闲的资源不拉低平均分」。

### 6.6 算例

{{< figure src="/images/posts/sched-plugins/08-scoring-strategies.svg" caption="三种策略的单资源打分曲线，以及两个节点在三种策略下的得分对比（点击查看大图）" link="/images/posts/sched-plugins/08-scoring-strategies.svg" >}}

待调度 Pod 请求 500m CPU、1Gi 内存。两个节点（数字已是 `NonZeroRequested`）：

| 节点 | allocatable | 已请求 | 加上 Pod 后 |
| --- | --- | --- | --- |
| A | 4000m / 8Gi | 1000m / 2Gi | 1500m / 3Gi |
| B | 8000m / 16Gi | 6000m / 4Gi | 6500m / 5Gi |

**LeastAllocated**：

- A：CPU `(4000−1500)×100/4000 = 62`（62.5 取整），内存 `(8−3)×100/8 = 62`，平均 `(62+62)/2 = 62`。
- B：CPU `(8000−6500)×100/8000 = 18`，内存 `(16−5)×100/16 = 68`，平均 `(18+68)/2 = 43`。

选 A。

**MostAllocated**：

- A：CPU `1500×100/4000 = 37`，内存 `3×100/8 = 37`，平均 37。
- B：CPU `6500×100/8000 = 81`，内存 `5×100/16 = 31`，平均 `(81+31)/2 = 56`。

选 B。

**RequestedToCapacityRatio**，`shape` 为 `[{0,0},{70,10},{100,3}]`，换算后是 (0,0)、(70,100)、(100,30)：

- A：CPU 利用率 37，落在第一段，`0 + 100×37/70 = 52`；内存同为 37，得 52；平均 `round(52) = 52`。
- B：CPU 利用率 81，落在第二段，`100 + (30−100)×(81−70)/30 = 100 + (−770/30) = 100 − 25 = 75`（Go 的整数除法向零截断）；内存利用率 31，`100×31/70 = 44`；平均 `round((75+44)/2) = round(59.5) = 60`。

选 B，但理由和 `MostAllocated` 不同：B 的 CPU 超过了 70% 的「甜区」被扣分，它赢在内存。

`NodeResourcesFit` 的 `ScoreExtensions()` 返回 `nil`，没有 NormalizeScore：三种策略的输出天然在 0 到 100 之间，乘以权重 1 直接进入总分。

### 6.7 PlacementScore

`TopologyAwareWorkloadScheduling` 打开后，`Fit` 还要为 PodGroup 的整套放置方案打分：

```go
// pkg/scheduler/framework/plugins/noderesources/resource_allocation.go
func (r *resourceAllocationScorer) scorePlacement(
	ctx context.Context,
	podGroupInfo fwk.PodGroupInfo,
	podGroupAssignments *fwk.PodGroupAssignments,
) (int64, *fwk.Status) {
	requested := make([]int64, len(r.resources))
	// Calculate requests for the pod group pods scheduled for this placement
	for _, assignment := range podGroupAssignments.ProposedAssignments {
		// ...
			requested[i] += podRequests[i]
	}
	// ...
	// Calculate resources on all nodes in this placement
	for _, node := range podGroupAssignments.Nodes {
		_, nodeAllocated, nodeAllocatable := r.calculateNodeAllocatableRequest(ctx, node, requested, nil)
		for i := range r.resources {
			allocatable[i] += nodeAllocatable[i]
			allocated[i] += nodeAllocated[i]
			requested[i] += nodeAllocated[i]
		}
	}

	score := r.scorer(requested, allocated, allocatable)
	// ...
}
```

把整个放置方案（比如一个机架）里的所有节点当成一个大节点，资源加总后用 `MostAllocated` 打分。这里固定用 `MostAllocated` 而不跟随 `ScoringStrategy.Type`：一组 Pod 应该尽量挤进已经比较满的拓扑域，把完整的空域留给下一个大作业。细节在 [TopologyPlacementGenerator](/posts/sched-plugin-22-topology-placement/) 一篇展开。

## 七、EventsToRegister 与 QueueingHint

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
func (f *Fit) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	events := []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: f.isSchedulableAfterAssignedPodDelete},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeAllocatable}, QueueingHintFn: f.isSchedulableAfterNodeChange},
	}
	if f.enableDRAExtendedResource {
		events = append(events,
			// A pod might be waiting for an exteneded resurce from a class to get created or modified.
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.DeviceClass, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: f.isSchedulableAfterDeviceClassEvent})
	}
	if f.enableInPlacePodVerticalScaling {
		events = append(events,
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.UpdatePodScaleDown}, QueueingHintFn: f.isSchedulableAfterAssignedPodScaleDown},
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodScaleDown}, QueueingHintFn: f.isSchedulableAfterTargetPodScaleDown})
	}
	if f.enableInPlacePodVerticalScalingSchedulerPreemption {
		events = append(events,
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.UpdatePodScaleUp}, QueueingHintFn: f.isSchedulableAfterAssignedPodScaleUp},
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodScaleUp}, QueueingHintFn: f.isSchedulableAfterTargetPodScaleUp})
	}
	return events, nil
}
```

在默认配置下（`InPlacePodVerticalScaling` 已 GA，`DRAExtendedResource` 已 GA），前五个事件全部生效。

### 7.1 已调度 Pod 被删除

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
	if deletedPod.Spec.NodeName == "" && deletedPod.Status.NominatedNodeName == "" {
		// ...
		return fwk.QueueSkip, nil
	}

	if f.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		if deletedPod.Spec.NodeName != pod.Spec.NodeName {
			// ...
			return fwk.QueueSkip, nil
		}
	}

	// any deletion event to a scheduled pod could make the unscheduled pod schedulable.
	// ...
	return fwk.Queue, nil
```

和 `NodePorts` 的精细判断相比，这里几乎是「有删除就重试」。原因在于：任何 Pod 的删除至少会释放一个 Pod 数名额，「Too many pods」就可能被解决；再去比较被删 Pod 请求了哪些资源，收益不大，反而容易漏判（比如被删 Pod 没写 requests，但它占着最后一个 Pod 名额）。

### 7.2 节点新增或 allocatable 变化

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
	// Leaving in the queue, since the pod won't fit into the modified node anyway.
	if !isFit(pod, modifiedNode, draManager, opts) {
		// ...
		return fwk.QueueSkip, nil
	}
	// The pod will fit, so since it's add, unblock scheduling.
	if originalNode == nil {
		// ...
		return fwk.Queue, nil
	}
	// The pod will fit, but since there was no increase in available resources, the change won't make the pod schedulable.
	if !haveAnyRequestedResourcesIncreased(pod, originalNode, modifiedNode, draManager, opts) {
		// ...
		return fwk.QueueSkip, nil
	}
	// ...
	return fwk.Queue, nil
```

`isFit` 构造一个**空的** `NodeInfo`，只设置节点、不放任何 Pod：

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
func isFit(pod *v1.Pod, node *v1.Node, draManager fwk.SharedDRAManager, opts ResourceRequestsOptions) bool {
	if node == nil {
		return false
	}
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)

	return len(Fits(pod, nodeInfo, draManager, opts)) == 0
}
```

所以它判断的是「这个节点的总量放不放得下这个 Pod」，不考虑节点上已经有什么。hint 运行在事件处理路径上，拿不到调度快照，只能做这种粗筛：一个 4 核节点不可能放下 8 核的 Pod，直接 `QueueSkip`；能放下的，交给下一个调度周期去看实际剩余量。

节点更新时还要求「Pod 关心的资源变大了」：

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
func haveAnyRequestedResourcesIncreased(pod *v1.Pod, originalNode, modifiedNode *v1.Node, draManager fwk.SharedDRAManager, opts ResourceRequestsOptions) bool {
	podRequest := computePodResourceRequest(pod, opts)
	// ...
	if modifiedNodeInfo.Allocatable.GetAllowedPodNumber() > originalNodeInfo.Allocatable.GetAllowedPodNumber() {
		return true
	}
	// ...
	if (podRequest.MilliCPU > 0 && modifiedNodeInfo.Allocatable.GetMilliCPU() > originalNodeInfo.Allocatable.GetMilliCPU()) ||
		(podRequest.Memory > 0 && modifiedNodeInfo.Allocatable.GetMemory() > originalNodeInfo.Allocatable.GetMemory()) ||
		(podRequest.EphemeralStorage > 0 && modifiedNodeInfo.Allocatable.GetEphemeralStorage() > originalNodeInfo.Allocatable.GetEphemeralStorage()) {
		return true
	}

	for rName, rQuant := range podRequest.ScalarResources {
		// ...
		if modifiedNodeInfo.Allocatable.GetScalarResources()[rName] > originalNodeInfo.Allocatable.GetScalarResources()[rName] {
			return true
		}
		// ...
	}
	return false
}
```

Pod 名额增加总是值得重试；其余资源只看 Pod 请求了的那几种。只请求 CPU 的 Pod 不会因为节点内存变大而被唤醒。

事件本身由框架过滤：`UpdateNodeAllocatable` 只在 `status.allocatable` 语义上发生变化时才产生（`extractNodeAllocatableChange` 用 `equality.Semantic.DeepEqual` 比较），kubelet 定期上报节点状态、更新 condition 都不会触发它。

### 7.3 Pod 原地缩容

`InPlacePodVerticalScaling` 让运行中的 Pod 可以修改 requests。框架在 Pod 更新时比较新旧请求量：

```go
// pkg/scheduler/framework/events.go
func extractPodScaleDown(newPod, oldPod *v1.Pod) fwk.ActionType {
	// ...
	newPodRequests := resource.PodRequests(newPod, opt)
	oldPodRequests := resource.PodRequests(oldPod, opt)

	for rName, oldReq := range oldPodRequests {
		newReq, ok := newPodRequests[rName]
		if !ok {
			// The resource request of rName is removed.
			return fwk.UpdatePodScaleDown
		}

		if oldReq.MilliValue() > newReq.MilliValue() {
			// The resource request of rName is scaled down.
			return fwk.UpdatePodScaleDown
		}
	}

	return fwk.None
}
```

有两类 `UpdatePodScaleDown`：

- `AssignedPod` 缩容：别的已调度 Pod 变小了，可能腾出资源。hint 是 `haveEnoughRelevantResourcesDecreased`，它对两边都用 `UseStatusResources` 口径算请求量（与 `NodeInfo` 记账一致），只在待调度 Pod 请求的资源里有某一项确实变小时才 `Queue`。
- `TargetPod` 缩容：被拒绝的 Pod **自己**变小了，直接 `Queue`。

注意 `haveEnoughRelevantResourcesDecreased` 用 `max(spec, allocated, actuated)` 计算缩容前后的请求量。把 spec 从 2 核改到 1 核，在 kubelet 真正完成缩容之前，`allocated` 和 `actuated` 还是 2 核，最大值不变，hint 返回 `QueueSkip`。等 kubelet 更新了 status，再来一次 Pod 更新事件，这时最大值才下降，Pod 才被唤醒。这和 `NodeInfo` 的记账时机是一致的，避免在资源真正释放之前就把等待的 Pod 放进来，结果又失败一次。

### 7.4 DeviceClass 事件

`DRAExtendedResource` 下，一个请求 `example.com/gpu` 的 Pod 可能在等一个尚未创建的 `DeviceClass`。hint 检查新增或修改的 `DeviceClass` 的 `extendedResourceName`（或隐式名字 `deviceclass.resource.kubernetes.io/<class 名>`）是否出现在 Pod 的请求里。

## 八、in-place resize 与调度器抢占

最后一组逻辑只在 `InPlacePodVerticalScalingSchedulerPreemption`（1.37 Alpha，默认关）打开时生效，但它展示了 `NodeResourcesFit` 设计上的一个新维度：**被评估的 Pod 已经在节点上了**。

场景是：一个运行中的 Pod 申请原地扩容，节点剩余资源不够，kubelet 把 resize 标记为 `Deferred`（`PodResizePending` condition 的 reason 为 `Deferred`）。打开这个 gate 后，调度器会把这样的 Pod 当作一个「需要调度」的对象重新入队，目的不是换节点，而是看能否在**原节点**上抢占低优先级 Pod 腾出空间。

这个 Pod 已有 `spec.nodeName`，[NodeName](/posts/sched-plugin-03-node-name/) 的 PreFilter 会把候选集缩到这一个节点；`NodeAffinity`、`NodePorts` 等插件对它返回 `Skip`；`NodeResourcesFit` 是少数要认真算的插件，问题在于：这个 Pod 自己的旧请求量已经记在节点的 `Requested` 里了，再把新请求量整个加上去，就重复计算了。

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

只检查**增量**：节点缓存里这个 Pod 已经按 `max(spec, allocated, actuated)` 记了账，新请求只需比较超出的部分。负增量截断为 0，理由写在注释里：kubelet 还没真正释放的资源，不能提前算作空闲。

`Unresolvable` 的判定仍然用 `podRequest`（完整的新请求量）与 `allocatable` 比较：Pod 想扩到比整个节点还大，抢占也无济于事。

QueueingHint 也为这类 Pod 加了节点维度的过滤：已调度 Pod 被删除、节点 allocatable 变化、其他 Pod 缩容，只有发生在**同一个节点**上才 `Queue`。还有两个 `UpdatePodScaleUp` 事件：

- `TargetPod` 扩容：这个 Pod 又往上加了请求，可能原来不需要抢占、现在需要了，所以只要它是 deferred 状态就 `Queue`。
- 同节点上别的 Pod 扩容：它可能吃掉了这个 Pod 正在等的余量，需要重新评估抢占。

完整的流程在 [DeferredPodScheduling](/posts/sched-plugin-17-deferred-pod-scheduling/) 一篇。

## 九、边界情况与坑

**Filter 和 Score 用的是两套数。** Filter 用真实 `Requested`，BestEffort 容器按 0；Score 用 `NonZeroRequested`，缺失的请求按 100m/200MiB。所以会出现「节点 Filter 通过、但 `MostAllocated` 已经把它打成满分」的情况，也会出现「`LeastAllocated` 下得分为 0、但 Filter 照样能放」的情况。

**BestEffort Pod 只受 maxPods 约束。** 一个已经被 requests 占满的节点，Filter 依然会放 BestEffort Pod 进来。能挡住它们的只有打分（非零默认值让满节点分数低）和 kubelet 的驱逐。

**`ignoredResources` 不能忽略 CPU、内存、hugepages。** 只有扩展资源（带域名前缀、不在 `kubernetes.io` 命名空间下的资源）受它影响。

**请求了节点上不存在的扩展资源，是不可抢占的失败。** 拼错资源名（`nvidia.com/gpus`）的 Pod 会得到 `Insufficient nvidia.com/gpus`，而且是 `UnschedulableAndUnresolvable`，不会触发任何抢占。

**Pod 级 resources 在 Fit 打分里可能不生效。** `NewFit` 通过 `getScorer` 构造的 `resourceAllocationScorer` 只设置了 `Name`、`scorer`、`resources` 和 DRA 相关字段，`enablePodLevelResources` 留在了零值 `false`；而 `calculatePodResourceRequest` 读的正是打分器自己的这个字段（`Fit` 结构体上的同名字段被嵌入结构体的方法看不到）。从代码看，`NodeResourcesFit` 打分时会以 `SkipPodLevelResources: true` 计算待调度 Pod 的请求：只写了 Pod 级 requests、容器不写 requests 的 Pod，在打分阶段会被当作每个容器 100m/200MiB。Filter 不受影响（PreFilter 显式传入了 `f.enablePodLevelResources`），节点侧的 `NonZeroRequested` 也不受影响（`CalculateResource` 直接读 feature gate）。`NodeResourcesBalancedAllocation` 的构造函数则正确设置了这个字段。

**临时存储不进默认打分。** 默认 `ScoringStrategy.Resources` 只有 CPU 和内存。临时存储在 Filter 中检查，但不影响节点选择，除非显式加进 `resources`。

**`maxPods` 用满时提示的是 `Too many pods`。** 在 Pod 密度高的节点上（大量 sidecar、小 Pod），这经常比 CPU/内存先耗尽。它是可抢占的失败。

**节点缩容 allocatable 不会驱逐 Pod。** 管理员调大 `kube-reserved` 导致 allocatable 变小，已经运行的 Pod 不受影响，节点的 `Requested` 可能超过 `allocatable`，这时任何新 Pod 都放不进来，`allocatable − requested` 是负数。

## 十、发散

### 10.1 从 predicates/priorities 到一个插件

在 scheduler framework 之前，资源检查是 `PodFitsResources` 谓词，打分是 `LeastRequestedPriority`、`MostRequestedPriority`、`RequestedToCapacityRatioPriority` 三个独立的优先级函数。迁移到 framework 后，它们一度是四个独立插件（`NodeResourcesFit`、`NodeResourcesLeastAllocated`、`NodeResourcesMostAllocated`、`RequestedToCapacityRatio`），后来在 `v1beta2` 配置 API 中合并为 `NodeResourcesFit` 加 `scoringStrategy` 参数。合并的好处是三种互斥的策略不能再被同时启用，也让 Filter 和 Score 共享同一套请求量计算。

`resourceAllocationScorer` 就是合并留下的骨架：它把「从 Pod 和节点取出三个向量」和「怎么把向量变成分数」分开，前者由 `NodeResourcesFit` 和 `NodeResourcesBalancedAllocation` 共享，后者是一个 `func(requested, allocated, allocatable []int64) int64`。

### 10.2 requests 不是用量

`NodeResourcesFit` 的全部计算基于 requests，与节点的真实负载无关。一个 requests 很小、实际吃满 CPU 的 Pod，在调度器眼里节点依然很空。这是 Kubernetes 资源模型的基本假设：requests 是 Pod 与调度器之间的契约，超出部分由 cgroup 在节点上兜底。

想按真实负载调度，需要借助 out-of-tree 插件：scheduler-plugins 里的 Trimaran 系列（[TargetLoadPacking](/posts/sched-plugin-37-target-load-packing/)、[LoadVariationRiskBalancing](/posts/sched-plugin-38-load-variation-risk-balancing/) 等）从指标系统读取节点利用率打分；[NodeResourcesAllocatable](/posts/sched-plugin-33-node-resources-allocatable/) 则换了个角度，按节点的 allocatable 大小而不是剩余比例打分。

### 10.3 生产配置建议

- **装箱配合集群自动扩缩容。** 云上按节点计费的集群，用 `MostAllocated`（或一条不过分激进的 `RequestedToCapacityRatio` 曲线）把 Pod 集中起来，让 Cluster Autoscaler 能回收空节点。注意 `MostAllocated` 会让节点长期处于高 requests 状态，突发扩容时更容易触发抢占。
- **GPU 集群把 GPU 加进打分资源。** 在 `scoringStrategy.resources` 里加上 `nvidia.com/gpu` 并给较高权重，配合 `MostAllocated`，可以减少 GPU 碎片。由于 6.2 节的跳过逻辑，不请求 GPU 的 Pod 不会被这条配置影响。
- **多 Profile 区分工作负载。** 在线服务用 `LeastAllocated` 摊开以降低干扰，批处理用 `MostAllocated` 装箱，各自用不同的 `schedulerName`。
- **调权重之前算一遍。** `NodeResourcesFit` 默认权重只有 1，而 `TaintToleration` 是 3、`NodeAffinity` 和 `PodTopologySpread` 是 2。装箱效果不明显时，往往不是策略不对，而是它在总分里的份量太小。

## 十一、小结

- Pod 的请求量 = max(稳态：应用容器 + sidecar，init 阶段：每个 init 容器 + 它之前的 sidecar)，逐维度取最大；Pod 级 resources 覆盖 CPU/内存/hugepages；最后加 overhead。节点上已有 Pod 按 spec/allocated/actuated 的最大值记账。
- PreFilter 只计算不跳过；Filter 依次检查 Pod 数、CPU/内存/临时存储、标量资源，收集全部不足项；请求超过节点 allocatable 才是 `UnschedulableAndUnresolvable`，否则可以抢占。
- `ignoredResources` 只作用于扩展资源；由 DRA 提供的扩展资源交给 `DynamicResources`。
- 打分用非零请求（缺失按 100m/200MiB），三种策略：`LeastAllocated` 打散，`MostAllocated` 装箱，`RequestedToCapacityRatio` 自定义曲线（四舍五入，0 分资源不计入平均）。
- QueueingHint：已调度 Pod 删除几乎总是重试；节点事件先用空 `NodeInfo` 粗筛总量，再要求 Pod 关心的资源确实变大；Pod 缩容以 status 口径为准。

资源够了，接下来轮到存储。[VolumeRestrictions](/posts/sched-plugin-09-volume-restrictions/) 检查的是卷的独占访问：同一块盘能不能被两个 Pod 同时挂载。
