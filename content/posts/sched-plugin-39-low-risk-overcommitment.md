---
title: 'scheduler-plugins 39 [out-of-tree] LowRiskOverCommitment：用 limit 和 Beta 分布估计超卖风险'
date: 2026-11-19T14:30:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "trimaran", "overcommitment"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "limit 之和超过节点容量，就是超卖。LowRiskOverCommitment 把风险拆成两部分：limit 超出容量的比例，以及实际负载越过已分配 request 的概率（用 Beta 分布矩匹配求得），再按权重合成、取 CPU 与内存中较差者。本文逐行拆解 PreScore、computeRisk 与 beta.go，推导公式，给出两节点算例，并讨论平滑窗口、条件概率和各种退化情形。"
---

上一篇[《LoadVariationRiskBalancing》](/posts/sched-plugin-38-load-variation-risk-balancing/)把节点负载的均值和标准差合成一个线性风险，但它只用 request 估计新 Pod 的负载。在超卖的集群中，request 和 limit 往往差好几倍。一个节点上所有 Pod 的 limit 加起来可能是容量的两三倍，这些 Pod 一旦同时冲向 limit，CPU 会被节流，内存会触发 OOM。`LowRiskOverCommitment`（下文简称 LROC）把 limit 纳入模型，并用 Beta 分布代替上一篇的「μ + σ」经验公式，直接估计一个概率。

LROC 实现了 PreScore 和 Score（`NormalizeScore` 为空实现）。它不在默认 Profile 里，没有 feature gate，在 scheduler-plugins 的调度器中启用。和前两篇一样，它不实现 `EnqueueExtensions`，因为 Score 插件不会拒绝 Pod。与前两个插件不同，它**完全不创建 `PodAssignEventHandler`**。原因在后面：它用来算 request/limit 的数据来自调度器自己的 `NodeInfo` 快照，本来就没有滞后。源码基线为 scheduler-plugins `cfeb3aa`。

## 一、什么是「超卖风险」

先看一个典型的 Burstable Pod：

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: api
spec:
  schedulerName: trimaran
  containers:
  - name: app
    image: example/api:1.0
    resources:
      requests: {cpu: 500m, memory: 512Mi}
      limits:   {cpu: "1",  memory: 1Gi}
```

`NodeResourcesFit` 只保证 request 之和不超过 Allocatable，limit 之和可以超过，这就是超卖。超卖不一定出问题，只要这些 Pod 不同时用到 limit。所以风险取决于两个因素：

1. **limit 超出容量多少**。这是静态的「最坏情况」，可以从 Pod spec 里直接算出来。
2. **实际负载有多大概率超过已经分配出去的 request**。这是动态的统计量，要用 load-watcher 的指标来估计。

LROC 的 README 把这两项称为 limit risk 和 load risk，按权重相加后，选风险最低的节点。

## 二、参数

```go
// apis/config/types.go
// LowRiskOverCommitmentArgs holds arguments used to configure LowRiskOverCommitment plugin.
type LowRiskOverCommitmentArgs struct {
	metav1.TypeMeta

	// Common parameters for trimaran plugins
	TrimaranSpec
	// The number of windows over which usage data metrics are smoothed
	SmoothingWindowSize int64
	// Resources fractional weight of risk due to limits specification [0,1]
	RiskLimitWeights map[v1.ResourceName]float64
}
```

```go
// apis/config/v1/defaults.go
// SetDefaults_LowRiskOverCommitmentArgs sets the default parameters for LowRiskOverCommitment plugin
func SetDefaults_LowRiskOverCommitmentArgs(args *LowRiskOverCommitmentArgs) {
	SetDefaultTrimaranSpec(&args.TrimaranSpec)
	if args.SmoothingWindowSize == nil || *args.SmoothingWindowSize <= 0 {
		args.SmoothingWindowSize = &DefaultSmoothingWindowSize
	}
	if args.RiskLimitWeights == nil || len(args.RiskLimitWeights) == 0 {
		args.RiskLimitWeights = DefaultRiskLimitWeights
	} else {
		for r, w := range args.RiskLimitWeights {
			if w < 0 || w > 1 {
				(args.RiskLimitWeights)[r] = DefaultRiskLimitWeight
			}
		}
	}
}
```

| 参数 | 默认值 | 含义 |
|---|---|---|
| `smoothingWindowSize` | 5（≤0 时回落到 5） | 指标被平滑过的窗口数，用来把标准差还原回去 |
| `riskLimitWeights` | `cpu: 0.5, memory: 0.5`，越界值替换为 0.5 | 每种资源的总风险中，limit risk 所占的权重 w；load risk 的权重是 1 − w |

`New` 把配置里给的权重叠加到一个预填了 CPU、内存默认值的新 map 上：

```go
// pkg/trimaran/lowriskovercommitment/lowriskovercommitment.go
	// create map of resource risk limit weights
	m := make(map[v1.ResourceName]float64)
	m[v1.ResourceCPU] = pluginv1.DefaultRiskLimitWeight
	m[v1.ResourceMemory] = pluginv1.DefaultRiskLimitWeight
	for r, w := range args.RiskLimitWeights {
		m[r] = w
	}
```

所以只写 `cpu: 0.8` 时，内存仍是 0.5。写了其它资源名也不会报错，但 `computeRank` 只计算 CPU 和内存，其它资源名不会生效。

## 三、PreScore 与 CycleState

```go
// pkg/trimaran/lowriskovercommitment/lowriskovercommitment.go
const (
	// Name : name of plugin
	Name = "LowRiskOverCommitment"

	// MaxVarianceAllowance : allowed value from the maximum variance (to avoid zero divisions)
	MaxVarianceAllowance = 0.99

	// PodResourcesKey State key used in CycleState
	PodResourcesKey = Name + ".PodResources"
)

// PreScore : calculate pod requests and limits and store as plugin state data to be used during scoring
func (pl *LowRiskOverCommitment) PreScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	// ...
	podResourcesStateData := CreatePodResourcesStateData(pod)
	cycleState.Write(PodResourcesKey, podResourcesStateData)
	return nil
}
```

```go
// pkg/trimaran/lowriskovercommitment/lowriskovercommitment.go
// CreatePodResourcesStateData : calculate pod resource requests and limits and store as plugin state data
func CreatePodResourcesStateData(pod *v1.Pod) *PodResourcesStateData {
	requests := trimaran.GetResourceRequested(pod)
	limits := trimaran.GetResourceLimits(pod)
	// make sure limits not less than requests
	trimaran.SetMaxLimits(requests, limits)
	return &PodResourcesStateData{
		podRequests: *requests,
		podLimits:   *limits,
	}
}

// PodResourcesStateData : computed at PreScore and used at Score
type PodResourcesStateData struct {
	podRequests framework.Resource
	podLimits   framework.Resource
}

// Clone : clone the pod resource state data
func (s *PodResourcesStateData) Clone() fwk.StateData {
	return s
}
```

PreScore 每个调度周期只执行一次：计算 Pod 的 request 和 limit（口径与上一篇的 `GetEffectiveResource` 相同：容器求和、与 init 容器取 max、加 overhead），再用 `SetMaxLimits` 保证每种资源的 limit 不小于 request。没写 CPU limit 的容器，limit 被视为等于 request，而不是无穷大。

`Clone` 直接返回自身。这样做是安全的，因为数据写入后只读不改，抢占模拟时复制 CycleState 也不需要深拷贝。Score 读不到这个 key 时（比如 PreScore 被跳过），会现场重新计算一遍，不会报错。

## 四、Score 与 computeRank

```go
// pkg/trimaran/lowriskovercommitment/lowriskovercommitment.go
	// exclude scoring for best effort pods; this plugin is not concerned about best effort pods
	podRequests := &podResources.podRequests
	podLimits := &podResources.podLimits
	if podRequests.MilliCPU == 0 && podRequests.Memory == 0 &&
		podLimits.MilliCPU == 0 && podLimits.Memory == 0 {
		logger.V(6).Info("Skipping scoring best effort pod; using minimum score", "nodeName", nodeName, "pod", klog.KObj(pod))
		return score, nil
	}
	// get node metrics
	metrics, _ := pl.collector.GetNodeMetrics(logger, nodeName)
	if metrics == nil {
		logger.Info("Failed to get metrics for node; using minimum score", "nodeName", nodeName)
		return score, nil
	}
	// calculate score
	totalScore := pl.computeRank(logger, metrics, nodeInfo, pod, podRequests, podLimits) * float64(fwk.MaxNodeScore)
	score = int64(math.Round(totalScore))
	return score, fwk.NewStatus(fwk.Success, "")
```

Score 有两个提前返回 0 分的出口：BestEffort Pod，以及节点没有指标。前者对所有节点都给 0 分，相当于这个插件对 BestEffort Pod **保持中立**，不影响排序；后者是让这个节点在这一维垫底。

```go
// pkg/trimaran/lowriskovercommitment/lowriskovercommitment.go
func (pl *LowRiskOverCommitment) computeRank(logger klog.Logger, metrics []watcher.Metric, nodeInfo fwk.NodeInfo, pod *v1.Pod,
	podRequests *framework.Resource, podLimits *framework.Resource) float64 {
	node := nodeInfo.Node()
	// calculate risk based on requests and limits
	nodeRequestsAndLimits := trimaran.GetNodeRequestsAndLimits(logger, nodeInfo.GetPods(), node, pod, podRequests, podLimits)
	riskCPU := pl.computeRisk(logger, metrics, v1.ResourceCPU, watcher.CPU, node, nodeRequestsAndLimits)
	riskMemory := pl.computeRisk(logger, metrics, v1.ResourceMemory, watcher.Memory, node, nodeRequestsAndLimits)
	rank := 1 - max(riskCPU, riskMemory)
	// ...
	return rank
}
```

`GetNodeRequestsAndLimits` 遍历 `NodeInfo` 里的所有 Pod，最后把待调度 Pod 也加进去，在加它之前保存一份「不含本 Pod」的快照：

```go
// pkg/trimaran/resourcestats.go
	for _, p := range append(podsOnNode, pod) {
		var requested *framework.Resource
		var limits *framework.Resource
		// pending pod is last in sequence
		if p == pod {
			*nodeRequestMinusPod = *nodeRequest
			*nodeLimitMinusPod = *nodeLimit
			requested = podRequests
			limits = podLimits
		} else {
			// get requests and limits for pod
			requested = GetResourceRequested(p)
			limits = GetResourceLimits(p)
			// make sure limits not less than requests
			SetMaxLimits(requested, limits)
		}

		// accumulate
		nodeRequest.MilliCPU += requested.MilliCPU
		nodeRequest.Memory += requested.Memory
		nodeLimit.MilliCPU += limits.MilliCPU
		nodeLimit.Memory += limits.Memory
	}
	// cap requests by node capacity
	setMin(&nodeRequest.MilliCPU, capCpu)
	setMin(&nodeRequest.Memory, capMem)
	setMin(&nodeRequestMinusPod.MilliCPU, capCpu)
	setMin(&nodeRequestMinusPod.Memory, capMem)
```

得到五个量，下文记为：C = Allocatable，R / L = 含本 Pod 的 request / limit 之和，R⁻ / L⁻ = 不含本 Pod 的版本。R 和 R⁻ 被截到不超过 C，L 和 L⁻ 不截断。

`NodeInfo` 来自调度器的快照，**包含已经 assume 但还没绑定完成的 Pod**。所以这一半数据没有 TLP 那种滞后问题，上一个刚被放到这个节点的 Pod，下一个周期就能看到。这也是 LROC 不需要 `PodAssignEventHandler` 的原因。

{{< figure src="/images/posts/sched-plugins/39-lroc-risk-flow.svg" caption="LROC 的风险合成：PreScore 准备 Pod 的 request/limit，Score 中每种资源分别计算 riskLimit 和 riskLoad，按权重合成后取 CPU 与内存的较大值（点击查看大图）" link="/images/posts/sched-plugins/39-lroc-risk-flow.svg" >}}

## 五、computeRisk：两部分风险

### 5.1 riskLimit：limit 超出容量的比例

```go
// pkg/trimaran/lowriskovercommitment/lowriskovercommitment.go
	// (1) riskLimit : calculate overcommit potential load
	if limit > capacity {
		riskLimit = float64(limit-capacity) / float64(limit-request)
	}
```

```text
riskLimit = (L − C) / (L − R)    当 L > C
          = 0                     当 L ≤ C
```

分母 L − R 是「所有 Pod 能在 request 之上额外突发的总量」，分子 L − C 是「其中节点容量兜不住的部分」。比值在 (0, 1] 内：limit 刚超过容量时接近 0；R = C，也就是 request 已经占满时，突发量全部兜不住，值为 1。因为 R ≤ C < L，分母恒为正，不会出现除零。

这个量只依赖 Pod spec，含义是**最坏情况下突发量的落空比例**。注意它用的是含本 Pod 的 R 和 L，衡量的是「放上去之后」的状态。

### 5.2 riskLoad：实际负载越过已分配 request 的概率

```go
// pkg/trimaran/lowriskovercommitment/lowriskovercommitment.go
	// (2) riskLoad : calculate measured overcommitment
	zeroRequest := &framework.Resource{}
	stats, ok := trimaran.CreateResourceStats(logger, metrics, node, zeroRequest, resourceName, resourceType)
	if ok {
		// fit a beta distribution to the measured load stats
		mu, sigma := trimaran.GetMuSigma(stats)
		// adjust standard deviation due to data smoothing
		sigma *= math.Pow(float64(pl.args.SmoothingWindowSize), 0.5)
		// limit the standard deviation close to the allowed maximum for the beta distribution
		sigma = min(sigma, math.Sqrt(GetMaxVariance(mu)*MaxVarianceAllowance))

		// calculate area under beta probability curve beyond total allocated, as overuse risk measure
		allocThreshold := float64(requestMinusPod) / float64(capacity)
		allocThreshold = min(max(allocThreshold, 0), 1)
		allocProb, fitDistribution := ComputeProbability(mu, sigma, allocThreshold)
		// ...
		// condition the probability in case total limit is less than capacity
		if limitMinusPod < capacity && requestMinusPod <= limitMinusPod {
			limitThreshold := float64(limitMinusPod) / float64(capacity)
			if limitThreshold == 0 {
				allocProb = 1 // zero over zero
			} else if fitDistribution != nil {
				limitProb := fitDistribution.DistributionFunction(limitThreshold)
				if limitProb > 0 {
					allocProb /= limitProb
					allocProb = min(max(allocProb, 0), 1)
				}
			}
		}

		// calculate risk
		riskLoad = 1 - allocProb
		// ...
	}
```

分步来看。

**第一步，μ 和 σ。** 用 `CreateResourceStats` 时传入的是**零 request**，所以 μ 就是节点实际平均用量占 Allocatable 的比例，不加本 Pod。σ 先乘以 `√smoothingWindowSize`。原因是：如果观测到的序列是对 n 个独立样本做滑动平均得到的，平均值的方差是原始方差的 1/n，标准差是 1/√n；乘以 √n 可以把标准差还原到原始尺度。Prometheus 的 `instance:node_cpu:ratio` 这类 recording rule 本身就是在若干分钟窗口上算出的速率，默认值 5 对应的就是这种平滑。

**第二步，限制 σ 不超过 Beta 分布允许的最大值。** [0, 1] 上均值为 μ 的分布，方差最大是 μ(1 − μ)，此时概率质量全部集中在 0 和 1 两点。`GetMaxVariance` 返回这个上界，代码再乘以 0.99 后开方，作为 σ 的上限，保证下一步矩匹配一定有解（α、β > 0）。

**第三步，矩匹配出 Beta(α, β)：**

```go
// pkg/trimaran/lowriskovercommitment/beta.go
// MatchMoments : Match the first two moments: m1 and m2
func (b *BetaDistribution) MatchMoments(m1, m2 float64) bool {
	variance := m2 - m1*m1
	if m1 < 0 || m1 > 1 || variance < 0 || variance >= m1*(1-m1) {
		return false
	}
	temp := (m1 * (1 - m1) / variance) - 1
	temp = max(temp, math.SmallestNonzeroFloat64)
	b.alpha = m1 * temp
	b.beta = (1 - m1) * temp
	return b.computeMoments()
}
```

推导过程：Beta(α, β) 的均值是 `α/(α+β)`，方差是 `αβ / ((α+β)²(α+β+1))`。令 ν = α + β，则均值 μ = α/ν，方差 = μ(1−μ)/(ν+1)。反解得到：

```text
ν = μ(1 − μ) / σ² − 1
α = μ · ν
β = (1 − μ) · ν
```

这正是代码里的 `temp`（即 ν）。ν > 0 等价于 σ² < μ(1−μ)，也就是第二步保证的条件。

**第四步，计算 allocProb：**

```go
// pkg/trimaran/lowriskovercommitment/beta.go
// ComputeProbability : The probability that the resource utilization is less than or equal to a given threshold value
func ComputeProbability(mu, sigma, threshold float64) (float64, *BetaDistribution) {
	if mu == 0 || (sigma == 0 && mu <= threshold) {
		return 1, nil
	}
	if sigma == 0 && mu > threshold {
		return 0, nil
	}
	m1 := mu
	m2 := (sigma * sigma) + (mu * mu)
	betaDist := NewBetaDistribution(1, 1)
	if !betaDist.MatchMoments(m1, m2) {
		return 0, nil
	}
	belowLimit := betaDist.DistributionFunction(threshold)
	if math.IsNaN(belowLimit) {
		return 1, betaDist
	}
	return belowLimit, betaDist
}
```

`DistributionFunction` 是 Beta 分布的累积分布函数，调用 gonum 的正则化不完全 Beta 函数 `mathext.RegIncBeta(α, β, x)`。阈值是 `R⁻ / C`，即**已有 Pod 的 request 之和占容量的比例**。allocProb = P(X ≤ R⁻/C)，是「已有 Pod 的实际用量没有超出它们的 request」的概率。

为什么用这个阈值？新 Pod 被调度上来后，它的 request 是要被保证的。如果已有 Pod 的实际用量已经超过了它们自己的 request 总和，超出的那部分就是在借用「未分配」的容量，而新 Pod 的 request 正好要占用这部分容量。所以 1 − allocProb 就是「新 Pod 会和已有 Pod 的突发负载争抢」的概率。

几个特殊情况：μ = 0 时直接认为概率为 1；σ = 0 时退化成确定值，μ ≤ 阈值取 1，否则取 0；矩匹配失败时返回 0（风险为 1）。

**第五步，按 limit 做条件化。** 如果已有 Pod 的 limit 之和 L⁻ 小于容量，那么它们的用量不可能超过 L⁻/C（CPU 会被节流，内存会被 OOM kill）。这时实际用量的分布应该在 L⁻/C 处被截断：

```text
P(X ≤ R⁻/C | X ≤ L⁻/C) = P(X ≤ R⁻/C) / P(X ≤ L⁻/C)      （R⁻ ≤ L⁻）
```

代码就是做了这个除法，然后截到 [0, 1]。L⁻ = 0 时（节点上只有 BestEffort Pod，或者没有 Pod），代码注释写的是「zero over zero」，直接令 allocProb = 1。

**第六步**，`riskLoad = 1 − allocProb`。

{{< figure src="/images/posts/sched-plugins/39-lroc-beta-tail.svg" caption="节点 A 的 CPU：μ=0.40，σ 经 √5 放大后拟合出 Beta(11.6, 17.4)，越过 R⁻/C=0.5 的阴影部分就是 riskLoad；虚线是不放大 σ 的情形" >}}

### 5.3 合成

```go
// pkg/trimaran/lowriskovercommitment/lowriskovercommitment.go
	// combine two components of risk into a total risk as a weighted sum
	w := pl.riskLimitWeightsMap[resourceName]
	totalRisk = w*riskLimit + (1-w)*riskLoad
	totalRisk = min(max(totalRisk, 0), 1)
	return totalRisk
```

完整公式：

```text
totalRisk_r = clamp(w_r · riskLimit_r + (1 − w_r) · riskLoad_r, 0, 1)    r ∈ {cpu, memory}
rank  = 1 − max(totalRisk_cpu, totalRisk_memory)
score = round(100 × rank)
```

## 六、算例

两个节点，Allocatable 都是 4 核、16Gi，`smoothingWindowSize = 5`，权重取默认 0.5。待调度 Pod 就是第一节那个：CPU request 500m / limit 1000m，内存 request 512Mi / limit 1Gi。

| | 节点 A | 节点 B |
|---|---|---|
| 已有 Pod CPU request / limit（R⁻ / L⁻） | 2000m / 3000m | 1500m / 6000m |
| 加上本 Pod 后（R / L） | 2500m / 4000m | 2000m / 7000m |
| CPU 均值 / 标准差（load-watcher） | 40% / 4% | 30% / 3% |
| 已有 Pod 内存 request / limit | 8Gi / 10Gi | 8Gi / 10Gi |
| 内存均值 / 标准差 | 30% / 2% | 30% / 2% |

只看负载，B 更空（30% 对 40%），request 也更少。上一篇的 LVRB 会选 B。

**节点 A 的 CPU：**

- riskLimit：L = 4000，不大于 C = 4000，得 0。
- riskLoad：μ = 0.40，σ = 0.04 × √5 = 0.0894，上限 √(0.99 × 0.24) = 0.487，不触发。ν = 0.24 / 0.008 − 1 = 29，α = 11.6，β = 17.4。阈值 R⁻/C = 2000/4000 = 0.5，P(X ≤ 0.5) = 0.8636。L⁻ = 3000 < C，触发条件化：P(X ≤ 0.75) ≈ 1.0000，allocProb 不变。riskLoad = 0.1364。
- totalRisk = 0.5 × 0 + 0.5 × 0.1364 = **0.0682**

**节点 B 的 CPU：**

- riskLimit：L = 7000 > 4000，(7000 − 4000) / (7000 − 2000) = **0.6**。
- riskLoad：μ = 0.30，σ = 0.03 × √5 = 0.0671，ν = 0.21 / 0.0045 − 1 = 45.67，α = 13.70，β = 31.97。阈值 1500/4000 = 0.375，P(X ≤ 0.375) = 0.8644。L⁻ = 6000 ≥ C，不做条件化。riskLoad = 0.1356。
- totalRisk = 0.5 × 0.6 + 0.5 × 0.1356 = **0.3678**

**内存（两节点相同）：** L = 11Gi < 16Gi，riskLimit = 0。μ = 0.30，σ = 0.02 × √5 = 0.0447，Beta(31.2, 72.8)，阈值 8/16 = 0.5，P(X ≤ 0.5) 几乎为 1，riskLoad ≈ 0，totalRisk ≈ 0。

**结果：**

- A：rank = 1 − max(0.0682, 0) = 0.9318 → **93**
- B：rank = 1 − max(0.3678, 0) = 0.6322 → **63**

LROC 选 A。B 的负载更低、request 更少，但已有 Pod 的 limit 之和已经是容量的 1.5 倍（6000m 对 4000m），再放一个 limit 1 核的 Pod 后达到 1.75 倍，最坏情况下有 60% 的突发量没有着落。A 的负载更高，但 limit 没有超卖，唯一的风险来自「当前负载有 13.6% 的概率越过已分配 request」。

**平滑窗口的影响。** 如果把 A 的 `smoothingWindowSize` 设成 1，σ = 0.04，Beta(59.6, 89.4)，P(X ≤ 0.5) = 0.9930，riskLoad 只有 0.0070，是默认值下的约二十分之一。尾部概率对 σ 非常敏感，这个参数要按指标的实际平滑方式来设，不能随便填。

单测 `TestLowRiskOverCommitment_computeRisk` 的 `test-cpu-1` 用例可以用来检验对公式的理解：C = 4000、L = 3000，riskLimit = 0；μ = 0.8、σ = 0，阈值 R⁻/C = 0.25，μ 大于阈值，allocProb = 0；此时虽然 L⁻ < C，但 σ = 0 时 `fitDistribution` 为 nil，不做条件化。riskLoad = 1，totalRisk = 0.5 × 0 + 0.5 × 1 = 0.5，与断言一致。

## 七、边界情况与坑

**缺少某种资源的指标时，该资源看起来更安全。** `CreateResourceStats` 返回 `ok == false` 时，riskLoad 保持初始值 0，totalRisk 只剩 `w × riskLimit`。比如 SignalFx 只配置了 CPU 指标时，内存风险最多只有 0.5 × riskLimit。而节点完全没有指标时，Score 直接返回 0 分。两种「缺数据」的处理方向相反。

**metrics-server 下 σ = 0，riskLoad 只能是 0 或 1。** 只有 `Latest` 没有 `STD`，`ComputeProbability` 走 σ = 0 的分支：当前瞬时用量不超过 R⁻/C 时 riskLoad = 0，否则为 1。没有中间值，打分会在两档之间跳变。`smoothingWindowSize` 乘的是 0，也不起作用。

**request 被截断，limit 不截断。** 静态 Pod、或绕过调度器直接指定 `nodeName` 的 Pod，可能让 request 之和超过 Allocatable。代码把 R 截到 C，所以 riskLimit 的分母 L − R 不会因为 request 超额而变小或变成负数，风险最多是 1。

**R⁻ > L⁻ 的情况不存在。** 每个 Pod 都经过 `SetMaxLimits`，limit 不小于 request，所以累加后 L⁻ ≥ 未截断的 R⁻ ≥ R⁻。代码里的 `requestMinusPod <= limitMinusPod` 只是防御性判断。

**Pod 级资源和 sidecar 不在计算范围内。** 和上一篇一样，`GetEffectiveResource` 对 init 容器取 max，不区分 restartable sidecar，也不读 `spec.resources`。使用 Pod 级资源的 Pod，request 和 limit 可能被算成 0，进而被当作 BestEffort 跳过。

**计算开销。** 每个节点每次 Score 都要遍历该节点上所有 Pod 的容器，重新计算 request 和 limit，没有缓存。KEP 217（limit 感知打分的另一个提案）在 Known Limitations 里也提到了同样的 O(节点上 Pod 数) 开销。节点上 Pod 较多、集群较大时，这一部分的耗时不能忽略。

**包注释是从 LVRB 复制过来的。** `lowriskovercommitment.go` 文件顶部的注释还写着「Package loadvariationriskbalancing plugin attempts to balance the risk…」。阅读源码时不要被误导。

## 八、发散

**Beta 和正态分布的选择。** 上一篇的 LVRB 借用正态分布的 68–95–99.7 规则解释 margin，但利用率是有界的，均值接近 0 或 1 时，正态分布会给区间外分配不小的概率。Beta 分布天然定义在 [0, 1] 上，两个参数可以由均值和方差唯一确定，而且偏度会随均值自动变化：μ < 0.5 时右偏，尾部向高利用率延伸，这正是我们关心的方向。代价是需要计算不完全 Beta 函数。gonum 的 `RegIncBeta` 直接调用移植自 Cephes 的 `Incbet`（幂级数加连分式），单次调用是微秒级，每个节点、每种资源最多调用两次，开销不大。

**w 的选择因资源而异。** CPU 是可压缩资源，超卖的后果是节流和延迟升高；内存不可压缩，超卖的后果是 OOM kill 或驱逐。对内存来说，「limit 超出容量」这类静态风险更值得重视，因为一旦触发就是 Pod 被杀。所以可以考虑给内存更大的 `riskLimitWeights`（比如 0.7），CPU 保持 0.5 或更低。权重是按资源分别配置的，这在设计上就预留了这种差异。

**和 KEP 217 的对照。** `kep/217-resource-limit-aware-scoring` 提出的是一个只看 limit 的打分插件：`RawScore = (allocatable − limit) × MaxNodeScore / allocatable`，再做 min–max 归一化。当前代码树里没有对应的插件。LROC 可以看作它的「负载感知」版本：riskLimit 对应静态的 limit 超卖程度，riskLoad 用实际负载判断这种超卖是否真的会被触发。一个 limit 严重超卖但 Pod 实际都很闲的节点，在 KEP 217 下会被避开，在 LROC 下则只被扣掉 w × riskLimit。

**为什么只用 R⁻ 而不用 R 做阈值。** 直觉上，「放上去之后」的 request 总和 R 才是新的保证线。但 μ 是不含新 Pod 的实测负载，阈值也必须是不含新 Pod 的 R⁻，两者口径才一致：比较的是「已有 Pod 的负载」和「已有 Pod 的保证量」。新 Pod 的影响已经通过 riskLimit 中的 R、L 体现了。

## 九、小结

- LROC 在 PreScore 中把 Pod 的 request/limit（limit 至少等于 request）写入 CycleState，`Clone` 返回自身；Score 中对 BestEffort Pod 一律给 0 分以保持中立。
- 每种资源分别计算两部分风险：riskLimit = (L − C)/(L − R)，是突发量落空的比例；riskLoad = 1 − P(X ≤ R⁻/C)，用 μ 和经 √n 还原的 σ 矩匹配出 Beta 分布，在 L⁻ < C 时按 limit 做条件化。
- totalRisk = w·riskLimit + (1 − w)·riskLoad，rank 取 CPU 与内存的较差者，score = round(100 × rank)。
- request/limit 部分来自包含 assumed Pod 的 NodeInfo，没有滞后；负载部分仍依赖快照。只接 metrics-server 时，riskLoad 只能是 0 或 1。

下一篇是整个系列的最后一篇：[《Peaks》](/posts/sched-plugin-40-peaks/)换了一个优化目标，它不关心性能风险，而是按功耗模型选择让集群功耗增加最少的节点。
