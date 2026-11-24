---
title: 'scheduler-plugins 19 [in-tree] NodeResourcesBalancedAllocation：看的是放上去之后均衡度变好还是变坏'
date: 2026-11-19T11:10:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "NodeResourcesBalancedAllocation", "打分", "源码分析"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NodeResourcesBalancedAllocation 让各种资源在节点上被均匀地请求，避免 CPU 用满而内存闲置。本文从源码拆解它的标准差算法、为什么当前实现比较的是放上 Pod 前后的均衡度改善量而不是放上之后的绝对值、分数如何被压回 50 到 100、与 NodeResourcesFit 共享的 resourceAllocationScorer 有哪些差异，以及 BestEffort Pod 为什么直接跳过。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `NodeResourcesBalancedAllocation` 实现的扩展点（PreScore、Score）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/19.svg" link="/images/posts/sched-plugins/callstack/19.svg" caption="NodeResourcesBalancedAllocation 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-18-default-preemption/)的 `DefaultPreemption` 是 Filter 全军覆没之后的补救。如果 Filter 留下了可行节点，调度周期就进入打分阶段。前面几篇里，`TaintToleration`、`NodeAffinity`、`NodeResourcesFit`、`PodTopologySpread`、`InterPodAffinity` 等插件已经各自讲过自己的打分逻辑，它们都是「既过滤又打分」。默认 Profile 里还剩两个**只打分、从不拒绝 Pod** 的插件，本篇讲第一个：`NodeResourcesBalancedAllocation`。

本文基于 Kubernetes master 提交 `aabdf5a2`。它在默认 Profile 的 `MultiPoint` 里排在 `DefaultPreemption` 之后，权重 1，只实现 PreScore 和 Score，没有 feature gate，默认启用：

```go
// pkg/scheduler/apis/config/v1/default_plugins.go
{Name: names.DefaultPreemption},
{Name: names.NodeResourcesBalancedAllocation, Weight: ptr.To[int32](1)},
{Name: names.ImageLocality, Weight: ptr.To[int32](1)},
```

它和 [NodeResourcesFit](/posts/sched-plugin-08-node-resources-fit/) 在同一个 `noderesources` 包里，共用打分骨架 `resourceAllocationScorer`，建议先读那一篇的第六节。

## 一、它要解决什么问题

设想一个 16 核 64Gi 的节点，已经被请求了 15 核、16Gi。CPU 几乎用满，内存还剩 48Gi，但任何请求超过 1 核的 Pod 都放不上来，这 48Gi 内存实际上被浪费了。反过来，内存被请求满、CPU 闲置也一样。

`NodeResourcesFit` 的 `LeastAllocated` 只看每种资源的剩余比例的平均值，它不在乎资源之间是否失衡：CPU 剩 10%、内存剩 90% 和两者都剩 50%，平均下来一样。`NodeResourcesBalancedAllocation` 补上这个维度：**让同一个节点上各种资源的请求占比尽量接近**。偏 CPU 的 Pod 被引向内存紧张的节点，偏内存的 Pod 被引向 CPU 紧张的节点，整个集群的资源碎片因此减少。

## 二、结构体与参数

```go
// pkg/scheduler/framework/plugins/noderesources/balanced_allocation.go
type BalancedAllocation struct {
	handle fwk.Handle
	resourceAllocationScorer
}

var _ fwk.PreScorePlugin = &BalancedAllocation{}
var _ fwk.ScorePlugin = &BalancedAllocation{}
var _ fwk.SignPlugin = &BalancedAllocation{}
```

注意它没有实现 `EnqueueExtensions`。纯打分插件不会让 Pod 变得不可调度，也就不需要声明「什么事件能让被我拒绝的 Pod 重新入队」。

参数只有一个字段：

```go
// pkg/scheduler/apis/config/types_pluginargs.go
type NodeResourcesBalancedAllocationArgs struct {
	metav1.TypeMeta

	// Resources to be considered when scoring.
	// ...
	Resources []ResourceSpec
}
```

默认值是 CPU 和内存：

```go
// pkg/scheduler/apis/config/v1/defaults.go
func SetDefaults_NodeResourcesBalancedAllocationArgs(obj *configv1.NodeResourcesBalancedAllocationArgs) {
	if len(obj.Resources) == 0 {
		obj.Resources = defaultResourceSpec
		return
	}
	// If the weight is not set or it is explicitly set to 0, then apply the default weight(1) instead.
	for i := range obj.Resources {
		if obj.Resources[i].Weight == 0 {
			obj.Resources[i].Weight = 1
		}
	}
}
```

`ResourceSpec` 有 `Weight` 字段，但校验规则要求它**必须为 1**：

```go
// pkg/scheduler/apis/config/validation/validation_pluginargs.go
		if resource.Weight != 1 {
			allErrs = append(allErrs, field.Invalid(path.Child("resources").Index(i).Child("weight"), resource.Weight, "must be 1"))
		}
```

标准差是对称的统计量，给某种资源加权没有清晰的含义，所以干脆禁止。这个类型复用了 `NodeResourcesFit` 的 `ResourceSpec`，字段保留了，语义被校验封死。资源名也不能重复。

工厂函数：

```go
// pkg/scheduler/framework/plugins/noderesources/balanced_allocation.go
	return &BalancedAllocation{
		handle: h,
		resourceAllocationScorer: resourceAllocationScorer{
			Name:                            BalancedAllocationName,
			enableInPlacePodVerticalScaling: fts.EnableInPlacePodVerticalScaling,
			enablePodLevelResources:         fts.EnablePodLevelResources,
			enableDRAExtendedResource:       fts.EnableDRAExtendedResource,
			scorer:                          balancedResourceScorer,
			useRequested:                    true,
			resources:                       args.Resources,
			enableInPlacePodLevelResourcesVerticalScaling: fts.EnableInPlacePodLevelResourcesVerticalScaling,
			draManager: h.SharedDRAManager(),
		},
	}, nil
```

和 `NodeResourcesFit` 的打分器相比有两处关键差异：

- **`useRequested: true`**。节点一侧用 `NodeInfo.Requested` 而不是 `NonZeroRequested`；Pod 一侧计算请求量时不设置 `NonMissingContainerRequests`，没写 requests 的容器就按 0 算，不会被替换成 100m/200MiB。
- **feature 开关全部传进了打分器**。`enablePodLevelResources` 等字段在这里被正确设置，Pod 级 resources 会参与打分计算。（`NodeResourcesFit` 通过 `getScorer` 构造的打分器没有设置这些字段，第 08 篇的边界情况里讨论过这个差异。）

为什么要用真实请求而不是非零请求？`NodeResourcesFit` 用非零默认值，是为了让 BestEffort Pod 在 `LeastAllocated` 下被摊开。均衡度关心的是比例关系，凭空给每个容器加上 100m CPU 和 200MiB 内存会扭曲这个比例：一个只请求 4Gi 内存、没写 CPU 的 Pod，在非零口径下变成了「100m CPU + 4Gi」，均衡度的判断就偏了。

## 三、PreScore：BestEffort 直接跳过

```go
// pkg/scheduler/framework/plugins/noderesources/balanced_allocation.go
func (ba *BalancedAllocation) PreScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	podRequests := ba.calculatePodResourceRequestList(pod, ba.resources)
	if ba.isBestEffortPod(podRequests) {
		// Skip BalancedAllocation scoring for best-effort pods to
		// prevent a large number of pods from being scheduled to the same node.
		// See https://github.com/kubernetes/kubernetes/issues/129138 for details.
		return fwk.NewStatus(fwk.Skip)
	}
	state := &balancedAllocationPreScoreState{
		podRequests: podRequests,
	}
	if ba.enableDRAExtendedResource {
		draPreScoreState, status := getDRAPreScoredParams(ba.draManager, ba.resources)
		if status != nil {
			return status
		}
		if draPreScoreState != nil {
			state.draPreScoreState = draPreScoreState
		}
	}
	cycleState.Write(balancedAllocationPreScoreStateKey, state)
	return nil
}
```

`isBestEffortPod` 检查的是 `podRequests` 是否全为 0，而 `podRequests` 只覆盖配置的资源（默认 CPU 和内存）。所以这里的「BestEffort」不是 QoS 意义上的 BestEffort：一个只请求了 GPU、没写 CPU 和内存的 Pod，在默认配置下同样会被跳过。

跳过的理由写在注释里指向的 issue：请求量为 0 的 Pod 放到哪里都不改变节点的资源占比，如果参与打分，均衡度最好的那个节点每次都会胜出，而 BestEffort Pod 放上去之后它的均衡度并不变，于是下一个 BestEffort Pod 还会选它，大量 Pod 堆到同一个节点上。返回 `Skip` 后，框架把插件名记进 `SkipScorePlugins`，这个周期里它的 Score 不会被调用，在总分中贡献 0（对所有节点都是 0，不影响排序）。

CycleState 里存的是 `balancedAllocationPreScoreState`，key 为 `"PreScoreNodeResourcesBalancedAllocation"`，`Clone` 返回自身。打分阶段没有抢占模拟，这份状态也不会被修改。

## 四、Score：均衡度的改善量

```go
// pkg/scheduler/framework/plugins/noderesources/balanced_allocation.go
func (ba *BalancedAllocation) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	s, err := getBalancedAllocationPreScoreState(state)
	if err != nil {
		s = &balancedAllocationPreScoreState{podRequests: ba.calculatePodResourceRequestList(pod, ba.resources)}
		if ba.isBestEffortPod(s.podRequests) {
			return 0, nil
		}
		// ...
	}

	// ba.score favors nodes with balanced resource usage rate.
	// It calculates the standard deviation for those resources and prioritizes the node based on how close the usage of those resources is to each other.
	// Detail: score = (1 - std) * MaxNodeScore, where std is calculated by the root square of Σ((fraction(i)-mean)^2)/len(resources)
	// The algorithm is partly inspired by:
	// "Wei Huang et al. An Energy Efficient Virtual Machine Placement Algorithm with Balanced Resource Utilization"
	return ba.score(ctx, pod, nodeInfo, s.podRequests, s.draPreScoreState)
}
```

`ba.score` 是共享的 `resourceAllocationScorer.score`：先由 `calculateNodeAllocatableRequest` 从节点取出三个向量 `requested`（放上 Pod 之后）、`allocated`（放之前）、`allocatable`，再交给 `scorer` 函数。`NodeResourcesFit` 的三种策略只用了 `requested` 和 `allocatable`，`allocated` 这个参数就是为均衡度打分准备的：

```go
// pkg/scheduler/framework/plugins/noderesources/balanced_allocation.go
func balancedResourceScorer(requested, allocated, allocatable []int64) int64 {
	// Compute the score for the node with pending pod requests
	scoreWithPod := balancedResourceScore(requested, allocatable)
	// Compute the score for the node without pending pod requests
	scoreWithoutPod := balancedResourceScore(allocated, allocatable)
	// Score based on the improvement to the node balance (before vs after adding the pod).
	// Scores with/without pod are in range [MaxNodeScore/2, MaxNodeScore].
	// Therefore, their difference will be in range [-MaxNodeScore/2, MaxNodeScore/2].
	// This operation brings the range back to [MaxNodeScore/2, MaxNodeScore]
	// to be closer to the original implementation that only considered the score after adding the pod.
	// If the difference to the balance is small, the score will be around 75.
	// If the balance is improved, the score will move towards 100.
	// If the balance is decreased, the score will move towards 50.
	return fwk.MaxNodeScore/2 + (fwk.MaxNodeScore/2+scoreWithPod-scoreWithoutPod)/2
}
```

### 4.1 单次均衡分：标准差

```go
// pkg/scheduler/framework/plugins/noderesources/balanced_allocation.go
func balancedResourceScore(requested, allocatable []int64) int64 {
	var resourceToFractions []float64
	var totalFraction float64
	for i := range requested {
		if allocatable[i] == 0 {
			continue
		}
		fraction := float64(requested[i]) / float64(allocatable[i])
		if fraction > 1 {
			fraction = 1
		}
		totalFraction += fraction
		resourceToFractions = append(resourceToFractions, fraction)
	}

	std := 0.0

	// For most cases, resources are limited to cpu and memory, the std could be simplified to std := (fraction1-fraction2)/2
	// len(fractions) > 2: calculate std based on the well-known formula - root square of Σ((fraction(i)-mean)^2)/len(fractions)
	// Otherwise, set the std to zero is enough.
	if len(resourceToFractions) == 2 {
		std = math.Abs((resourceToFractions[0] - resourceToFractions[1]) / 2)
	} else if len(resourceToFractions) > 2 {
		mean := totalFraction / float64(len(resourceToFractions))
		var sum float64
		for _, fraction := range resourceToFractions {
			sum = sum + (fraction-mean)*(fraction-mean)
		}
		std = math.Sqrt(sum / float64(len(resourceToFractions)))
	}

	// STD (standard deviation) is always a positive value. 1-deviation lets the score to be higher for node which has least deviation and
	// multiplying it with `MaxNodeScore` provides the scaling factor needed.
	return int64((1 - std) * float64(fwk.MaxNodeScore))
}
```

每种资源算一个请求占比，超过 1 截断为 1（节点被超额请求时）。然后计算这些占比的总体标准差：

- 两种资源时，均值是 `(f₁+f₂)/2`，每个点到均值的距离都是 `|f₁−f₂|/2`，标准差就是 `|f₁−f₂|/2`。代码把这个特例单独写出来，省掉开方。
- 三种及以上，按定义算。
- 只有一种资源（比如节点上其他资源的 allocatable 都是 0），标准差为 0，满分。

占比都在 [0, 1] 内时，总体标准差最大是 0.5（一半在 0，一半在 1），所以 `(1 − std) × 100` 落在 [50, 100]。

两种资源时可以画在平面上：横轴 CPU 占比，纵轴内存占比，对角线是完全均衡，点到对角线的距离与标准差成正比。

{{< figure src="/images/posts/sched-plugins/19-balance-geometry.svg" caption="两个节点放上同一个 Pod 前后的位置：X 被拉向对角线，Y 被推离对角线（点击查看大图）" link="/images/posts/sched-plugins/19-balance-geometry.svg" >}}

### 4.2 改善量与映射

如果只看「放上之后」的均衡分，图中的节点 Y（放之后 92）会胜过节点 X（放之后 85）。但 Y 原本是完美均衡的 100 分，这个偏内存的 Pod 让它变差了；X 原本 CPU 偏高、内存闲置，这个 Pod 正好把它往均衡方向拉了一把。当前实现比较的是改善量：

```text
Δ     = scoreWithPod − scoreWithoutPod        ∈ [−50, 50]
score = 50 + (50 + Δ) / 2                     ∈ [50, 100]
```

{{< figure src="/images/posts/sched-plugins/19-score-mapping.svg" caption="从占比到最终分的四步，以及改善量 Δ 到最终分的线性映射（点击查看大图）" link="/images/posts/sched-plugins/19-score-mapping.svg" >}}

算例：Pod 请求 1 核 CPU、8Gi 内存。

| | 节点 X（10 核 / 20Gi） | 节点 Y（20 核 / 40Gi） |
| --- | --- | --- |
| 放之前 | CPU 7 核（0.70），内存 2Gi（0.10） | CPU 10 核（0.50），内存 20Gi（0.50） |
| scoreWithoutPod | std = 0.30，**70** | std = 0，**100** |
| 放之后 | CPU 8 核（0.80），内存 10Gi（0.50） | CPU 11 核（0.55），内存 28Gi（0.70） |
| scoreWithPod | std = 0.15，**85** | std = 0.075，`int(92.5)` = **92** |
| Δ | +15 | −8 |
| 最终分 | 50 + (50 + 15) / 2 = 50 + 32 = **82** | 50 + (50 − 8) / 2 = 50 + 21 = **71** |

注意 `(50 + 15) / 2` 是 Go 的整数除法，65 / 2 = 32。

这样设计还有一个直观的好处：Pod 对节点均衡度的影响越小（大节点、小 Pod），Δ 越接近 0，分数越接近 75，这个插件就越「中立」；只有当 Pod 相对节点足够大、确实会改变均衡度时，它才会拉开差距。旧的「只看放上之后」的做法则会持续偏爱本来就均衡的大节点，不管放的是什么 Pod。

把分数压回 [50, 100] 而不是直接用 [0, 100] 表示 Δ，代码注释给出的理由是「更接近只考虑放上之后分数的原始实现」：插件的分数区间不变，它在总分里的份量也就不变，升级后不会突然变得比其他插件更重或更轻。

### 4.3 哪些资源会被计入

`calculateNodeAllocatableRequest` 是两个插件共用的：

```go
// pkg/scheduler/framework/plugins/noderesources/resource_allocation.go
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
```

被跳过的资源在三个向量里都保持 0，`balancedResourceScore` 看到 `allocatable[i] == 0` 就不把它算进标准差。所以：

- 在 `resources` 里配了 `nvidia.com/gpu`，不请求 GPU 的 Pod 打分时只看 CPU 和内存，GPU 维度不影响结果。
- 节点没有 GPU（allocatable 为 0），这一维也被跳过，GPU 节点和非 GPU 节点按不同的维度数计算标准差。
- CPU 和内存**不受**第一条规则影响：Pod 只请求内存、CPU 写 0，CPU 这一维依然参与，Pod 放上去之后 CPU 占比不变、内存占比上升。

`balanced_allocation_test.go` 里有一个三种资源的用例：CPU 占比 85.7%，内存 12.5%，Pod 只请求 1 个 GPU，GPU 从 3/8 变成 4/8。标准差从 0.3039 降到 0.2989，均衡分从 69 变成 70，最终分 `50 + (50 + 1) / 2 = 75`。GPU 占比向均值靠拢了一点，均衡度略有改善。

在 `DRAExtendedResource` 下，如果某个扩展资源节点 `allocatable` 为 0 但由 DRA 驱动提供，`calculateResourceAllocatableRequest` 会改为统计节点上匹配 `DeviceClass` 的设备总数和已分配数，作为这一维的 allocatable 和 allocated。这部分计算在 PreScore 里预取了 ResourceClaim 的分配状态和 ResourceSlice 列表，只在配置的资源里确实有 DRA 托管的扩展资源时才做。

## 五、与 NodeResourcesFit 的配合

两个插件同时启用，是默认配置下打分的主力：

| | NodeResourcesFit（LeastAllocated） | NodeResourcesBalancedAllocation |
| --- | --- | --- |
| 关心什么 | 各资源剩余比例的加权平均 | 各资源占比之间的离散程度 |
| 请求口径 | 非零（缺失按 100m / 200MiB） | 真实 requests |
| BestEffort Pod | 正常打分 | PreScore 跳过 |
| 资源权重 | 1~100，可调 | 必须为 1 |
| 分数区间 | 0~100 | 50~100 |
| 默认插件权重 | 1 | 1 |

`BalancedAllocation` 的分数区间只有 50 宽，实际场景中 Δ 通常很小，节点之间的差距往往只有几分，而 `LeastAllocated` 的差距可以是几十分。所以在默认配置下，它更像一个「平手时的裁判」：剩余量差不多的节点之间，挑一个放上去更均衡的。

在装箱场景下（`NodeResourcesFit` 用 `MostAllocated`），两者的配合更有意义：`MostAllocated` 把 Pod 往满的节点上推，`BalancedAllocation` 防止节点被单一资源塞满。如果只装箱不均衡，最终节点会「CPU 满、内存空」，下一个 Pod 放不进去，Cluster Autoscaler 又加一个节点。

## 六、边界情况与坑

**它不能阻止任何调度。** 纯打分插件，分数再低也只是排名靠后。想要「节点 CPU 超过 80% 不再接收 Pod」这样的硬约束，需要别的手段。

**超额请求被截断。** 占比超过 1 时按 1 处理。节点的 `Requested` 可能超过 `allocatable`（比如 allocatable 被调小），这时所有超额的维度都按 100% 计，彼此之间看起来是均衡的。

**Pod 级 resources 生效，但只覆盖 CPU 和内存。** 由于工厂函数传入了 `enablePodLevelResources`，Pod 级 requests 会参与计算；扩展资源仍然只看容器级。

**只请求 GPU 的 Pod 在默认配置下被跳过。** 默认 `resources` 只有 CPU 和内存，`isBestEffortPod` 只看这两项。

**分数「中立值」是 75，不是 0 或 100。** 看调度日志里的插件分数时，75 表示这个 Pod 对节点均衡度几乎没有影响，而不是「中等均衡」。

## 七、发散：这个插件的公式演进

`BalancedAllocation` 的公式变过几次，每次都对应一个真实的问题：

1. **差值版本。** 早期实现只支持 CPU 和内存，分数是 `(1 − |cpuFraction − memoryFraction|) × MaxNodeScore`，任何一项占比达到 1 时直接给 0 分。两种资源时这与标准差版本只差一个系数 2，但它无法扩展到第三种资源。
2. **标准差版本。** 为了支持扩展资源（GPU 等），改为对任意数量的资源计算标准差，占比超过 1 时截断。这就是今天的 `balancedResourceScore`，两种资源的特例保留了下来。
3. **真实请求口径与 BestEffort 跳过。** 这个插件改为用真实 requests（`useRequested: true`），不再沿用 `NodeResourcesFit` 的非零默认值；源码注释引用的 issue #129138 讨论了大量 BestEffort Pod 被调度到同一个节点的问题，对应的处理是 PreScore 对零请求的 Pod 返回 `Skip`。
4. **改善量版本。** 当前的 `balancedResourceScorer` 同时计算放上前后的均衡分，用差值打分。代码注释明确写了这是为了「接近只考虑放上之后分数的原始实现」而把区间压回 [50, 100]。

这条演进路线反映了打分插件设计上一个反复出现的主题：一个只看「结果状态」的分数，会持续偏爱某一类节点（这里是本来就均衡的大节点），而看「变化量」的分数能区分不同的 Pod 对不同节点的影响。

从更宏观的角度看，`BalancedAllocation` 只在单个节点的维度上做均衡，并不知道集群整体的资源配比。如果集群里所有节点的 CPU 内存比都是 1:4，而工作负载整体是 1:8，任何调度策略都无法避免 CPU 闲置，这需要在节点规格选型上解决。

## 八、小结

- `NodeResourcesBalancedAllocation` 只实现 PreScore 和 Score，权重 1，默认考虑 CPU 和内存，资源权重必须为 1。
- 它用真实 requests（`useRequested: true`），配置的资源全部为 0 请求的 Pod 在 PreScore 返回 `Skip`。
- 单次均衡分是 `(1 − std) × 100`，两种资源时 `std = |f₁ − f₂| / 2`，占比超过 1 截断，范围 [50, 100]。
- 最终分是 `50 + (50 + scoreWithPod − scoreWithoutPod) / 2`：Pod 改善均衡度的节点趋向 100，恶化的趋向 50，影响很小时约为 75。
- 与 `NodeResourcesFit` 共用 `resourceAllocationScorer`，Pod 不请求的标量资源和节点上 allocatable 为 0 的资源都不参与计算。

下一篇是另一个纯打分插件 [ImageLocality](/posts/sched-plugin-20-image-locality/)：它不看资源，而是看节点上有没有现成的镜像，以及这个镜像在集群里铺开了多少。
