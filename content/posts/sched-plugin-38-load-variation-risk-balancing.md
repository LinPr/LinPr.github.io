---
title: 'scheduler-plugins [out-of-tree] LoadVariationRiskBalancing：把均值和波动一起算进风险'
date: 2026-11-19T14:20:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "trimaran", "load-aware"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "只按平均负载做均衡，看不到负载的抖动。LoadVariationRiskBalancing 用「均值 + margin × 标准差开 sensitivity 次方」定义节点风险，CPU 与内存取较差者。本文从 ResourceStats 的构造讲到 computeScore 的每一次截断，推导等分线，给出 sensitivity 取 1 和 2 的算例，并指出 metrics-server 下模型退化、满载节点仍得 50 分等细节。"
---

上一篇[《TargetLoadPacking》](/posts/sched-plugin-37-target-load-packing/)讲了 Trimaran 的公共数据链路：load-watcher、30 秒一次的 Collector 快照，以及 `PodAssignEventHandler` 的滞后补偿。TLP 的目标是装箱，只看 CPU 均值。这一篇的 `LoadVariationRiskBalancing`（下文简称 LVRB）方向相反，它做**均衡**，而且把负载的**波动**也算进去。

LVRB 只实现 Score（外加一个空的 `NormalizeScore`），没有 PreScore，也不往 CycleState 写任何东西。它不在默认 Profile 里，需要在 scheduler-plugins 的调度器里显式启用，没有 feature gate。和其它 Trimaran 插件一样，它不实现 `EnqueueExtensions`，因为 Score 插件不会拒绝 Pod。源码基线仍是 scheduler-plugins `cfeb3aa`。

## 一、平均值相同，风险不同

两个 4 核节点，过去 15 分钟的平均 CPU 利用率都是 50%：

- 节点甲上跑的是稳定的在线服务，利用率一直在 48% 到 52% 之间；
- 节点乙上跑着几个批处理任务，利用率在 20% 和 80% 之间来回跳。

只看均值，两者一样。但再往节点乙放一个 Pod，它在峰值时段被 CPU 节流的概率明显更高。KEP（`kep/61-Trimaran-real-load-aware-scheduling`）把这个问题画在 μ–σ 平面上：横轴是均值 M，纵轴是标准差 V。LVRB 的目标是让各节点落在 `M + V = c` 这条对角线附近，也就是「均值加一个标准差」在各节点之间持平。

启用方式：

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: trimaran
  plugins:
    score:
      enabled:
      - name: LoadVariationRiskBalancing
  pluginConfig:
  - name: LoadVariationRiskBalancing
    args:
      safeVarianceMargin: 1
      safeVarianceSensitivity: 2
      metricProvider:
        type: Prometheus
        address: http://prometheus-k8s.monitoring.svc.cluster.local:9090
```

## 二、参数

```go
// apis/config/types.go
// LoadVariationRiskBalancingArgs holds arguments used to configure LoadVariationRiskBalancing plugin.
type LoadVariationRiskBalancingArgs struct {
	metav1.TypeMeta

	// Common parameters for trimaran plugins
	TrimaranSpec
	// Multiplier of standard deviation in risk value
	SafeVarianceMargin float64
	// Root power of standard deviation in risk value
	SafeVarianceSensitivity float64
}
```

```go
// apis/config/v1/defaults.go
// SetDefaults_LoadVariationRiskBalancingArgs sets the default parameters for LoadVariationRiskBalancing plugin
func SetDefaults_LoadVariationRiskBalancingArgs(args *LoadVariationRiskBalancingArgs) {
	SetDefaultTrimaranSpec(&args.TrimaranSpec)
	if args.SafeVarianceMargin == nil || *args.SafeVarianceMargin < 0 {
		args.SafeVarianceMargin = &DefaultSafeVarianceMargin
	}
	if args.SafeVarianceSensitivity == nil || *args.SafeVarianceSensitivity < 0 {
		args.SafeVarianceSensitivity = &DefaultSafeVarianceSensitivity
	}
}
```

两个参数的默认值都是 1.0。负数不会报错，而是在 defaulting 阶段被**静默替换**成默认值。`validation` 包里没有这个插件的校验函数，所以写了 `-2` 也能正常启动，只是行为和预期不同。插件 README 推荐 `margin=1、sensitivity=2`，和代码默认值不一样，下文会算出两者的差别。

`TrimaranSpec` 的含义和默认值见上一篇 2.3 节：不写 `watcherAddress` 时默认用 metrics-server；用 Prometheus 时默认跳过 TLS 校验。

`New` 和 TLP 几乎一样：创建 Collector，创建 `PodAssignEventHandler` 并挂到 Pod informer 上。

```go
// pkg/trimaran/loadvariationriskbalancing/loadvariationriskbalancing.go
	collector, err := trimaran.NewCollector(logger, &args.TrimaranSpec)
	if err != nil {
		return nil, err
	}
	// ...
	podAssignEventHandler := trimaran.New()
	podAssignEventHandler.AddToHandle(handle)

	pl := &LoadVariationRiskBalancing{
		logger:       logger,
		handle:       handle,
		eventHandler: podAssignEventHandler,
		collector:    collector,
		args:         args,
	}
```

但在整个包里，`pl.eventHandler` 只在这里被赋值，**之后再也没有被读取**。也就是说，LVRB 维护着一份「刚调度 Pod」缓存（包括 informer 回调和每 5 分钟一次的清理 goroutine），打分时却完全不用它，没有任何滞后补偿。

## 三、从指标到 ResourceStats

### 3.1 Score 的主流程

```go
// pkg/trimaran/loadvariationriskbalancing/loadvariationriskbalancing.go
func (pl *LoadVariationRiskBalancing) Score(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	// ...
	score := fwk.MinNodeScore
	// get node metrics
	metrics, _ := pl.collector.GetNodeMetrics(logger, nodeName)
	if metrics == nil {
		logger.Info("Failed to get metrics for node; using minimum score", "nodeName", nodeName)
		return score, nil
	}
	podRequest := trimaran.GetResourceRequested(pod)
	node := nodeInfo.Node()

	// calculate CPU score
	var cpuScore float64 = 0
	cpuStats, cpuOK := trimaran.CreateResourceStats(logger, metrics, node, podRequest, v1.ResourceCPU, watcher.CPU)
	if cpuOK {
		cpuScore = computeScore(logger, cpuStats, pl.args.SafeVarianceMargin, pl.args.SafeVarianceSensitivity)
	}
	// ...
	// calculate Memory score
	var memoryScore float64 = 0
	memoryStats, memoryOK := trimaran.CreateResourceStats(logger, metrics, node, podRequest, v1.ResourceMemory, watcher.Memory)
	if memoryOK {
		memoryScore = computeScore(logger, memoryStats, pl.args.SafeVarianceMargin, pl.args.SafeVarianceSensitivity)
	}
	// ...
	// calculate total score
	var totalScore float64 = 0
	if memoryOK && cpuOK {
		totalScore = min(memoryScore, cpuScore)
	} else {
		totalScore = max(memoryScore, cpuScore)
	}
	score = int64(math.Round(totalScore))
	// ...
	return score, fwk.NewStatus(fwk.Success, "")
}
```

流程是：节点没有指标时得 0 分；有指标时，CPU 和内存各算一个分。两个都有就取**较小值**，也就是瓶颈资源决定得分；只有一个时，`max(x, 0)` 就等于那一个；两个都没有时得 0 分。

Pod 的资源量由 `GetResourceRequested` 计算：

```go
// pkg/trimaran/resourcestats.go
// GetEffectiveResource : calculate effective resources of a pod (CPU and Memory)
func GetEffectiveResource(pod *v1.Pod, fn func(container *v1.Container) v1.ResourceList) *framework.Resource {
	result := &framework.Resource{}
	// add up resources of all containers
	for _, container := range pod.Spec.Containers {
		result.Add(fn(&container))
	}
	// take max(sum_pod, any_init_container)
	for _, container := range pod.Spec.InitContainers {
		for rName, rQuantity := range fn(&container) {
			switch rName {
			case v1.ResourceCPU:
				setMax(&result.MilliCPU, rQuantity.MilliValue())
			case v1.ResourceMemory:
				setMax(&result.Memory, rQuantity.Value())
			}
		}
	}
	// add any pod overhead
	if pod.Spec.Overhead != nil {
		result.Add(pod.Spec.Overhead)
	}
	return result
}
```

这是旧版 kube-scheduler 的口径：容器求和，与每个 init 容器取 max，再加 overhead。它没有处理 sidecar（`restartPolicy: Always` 的 init 容器应该累加，而不是取 max），也不读 Pod 级 `spec.resources`。与 TLP 不同，这里用的是 **request**，不是 limit。

### 3.2 GetResourceData：从一组 Metric 里挑出均值和标准差

```go
// pkg/trimaran/resourcestats.go
// GetResourceData : get data from measurements for a given resource type
func GetResourceData(metrics []watcher.Metric, resourceType string) (avg float64, stDev float64, isValid bool) {
	// for backward compatibility of LoadWatcher:
	// average data metric without operator specified
	avgFound := false
	for _, metric := range metrics {
		if metric.Type == resourceType {
			if metric.Operator == watcher.Average {
				avg = metric.Value
				avgFound = true
			} else if metric.Operator == watcher.Std {
				stDev = metric.Value
			} else if (metric.Operator == "" || metric.Operator == watcher.Latest) && !avgFound {
				avg = metric.Value
			}
			isValid = true
		}
	}
	return avg, stDev, isValid
}
```

`AVG` 优先；没有 `AVG` 时，用 `Latest` 或空算子的值当均值；`STD` 单独取。只要出现过该类型的任意一个指标，`isValid` 就为 true。所以，**只有 STD 没有均值时，均值按 0 处理**。

### 3.3 CreateResourceStats 与 GetMuSigma

```go
// pkg/trimaran/resourcestats.go
	// get resource capacity
	rs = &ResourceStats{}
	allocatableResources := node.Status.Allocatable
	am := allocatableResources[resourceName]

	if resourceName == v1.ResourceCPU {
		rs.Capacity = float64(am.MilliValue())
		rs.Req = float64(podRequest.MilliCPU)
	} else {
		rs.Capacity = float64(am.Value())
		rs.Capacity *= MegaFactor
		rs.Req = float64(podRequest.Memory) * MegaFactor
	}

	// calculate absolute usage statistics
	rs.UsedAvg = nodeUtil * rs.Capacity / 100
	rs.UsedStdev = nodeStd * rs.Capacity / 100
```

容量取 **`Allocatable`**。CPU 以毫核为单位，内存换算成 MiB。百分比形式的均值和标准差乘以容量，得到绝对量。

```go
// pkg/trimaran/resourcestats.go
// GetMuSigma : get average and standard deviation from statistics
func GetMuSigma(rs *ResourceStats) (float64, float64) {
	if rs.Capacity <= 0 {
		return 0, 0
	}
	mu := (rs.UsedAvg + rs.Req) / rs.Capacity
	mu = max(min(mu, 1), 0)
	sigma := rs.UsedStdev / rs.Capacity
	sigma = max(min(sigma, 1), 0)
	return mu, sigma
}
```

再除以容量，回到 [0, 1] 区间的比例。**Pod 的 request 加在均值上，不加在标准差上**。README 的解释是：待调度 Pod 的未来负载未知，假设它的均值等于 request、方差为 0。

这里绕了一圈：百分比 × Allocatable 得到绝对量，再除以 Allocatable 变回比例，`ResourceStats` 只在加 request 的时候需要绝对量。但百分比本身的分母是什么，取决于指标源：metrics-server provider 用的是 `Capacity`。所以，在 Capacity 明显大于 Allocatable 的节点上（预留了较多 system/kube reserved），μ 中「已用」的部分和「request」的部分口径不完全一致。

## 四、computeScore：风险公式

```go
// pkg/trimaran/loadvariationriskbalancing/analysis.go
// computeScore : compute score given usage statistics
// - risk = [ average + margin * stDev^{1/sensitivity} ] / 2
// - score = ( 1 - risk ) * maxScore
func computeScore(logger klog.Logger, rs *trimaran.ResourceStats, margin float64, sensitivity float64) float64 {
	if rs.Capacity <= 0 {
		logger.Error(nil, "Invalid resource capacity", "capacity", rs.Capacity)
		return 0
	}

	// make sure values are within bounds
	rs.Req = max(rs.Req, 0)
	rs.UsedAvg = max(min(rs.UsedAvg, rs.Capacity), 0)
	rs.UsedStdev = max(min(rs.UsedStdev, rs.Capacity), 0)

	// calculate average and deviation factors
	mu, sigma := trimaran.GetMuSigma(rs)

	// apply root power
	if sensitivity >= 0 {
		sigma = math.Pow(sigma, 1/sensitivity)
	}
	// apply multiplier
	sigma *= margin
	sigma = max(min(sigma, 1), 0)

	// evaluate overall risk factor
	risk := (mu + sigma) / 2
	logger.V(6).Info("Evaluating risk factor", "mu", mu, "sigma", sigma, "margin", margin, "sensitivity", sensitivity, "risk", risk)
	return (1. - risk) * float64(fwk.MaxNodeScore)
}
```

### 4.1 公式

记 m = margin，s = sensitivity，U 为平均用量，R 为 Pod request，D 为用量标准差，Cap 为可分配量：

```text
μ  = clamp((U + R) / Cap, 0, 1)
σ  = clamp(D / Cap, 0, 1)
σ' = clamp(m × σ^(1/s), 0, 1)
risk  = (μ + σ') / 2                 ∈ [0, 1]
score = 100 × (1 − risk) = 100 − 50 × (μ + σ')
```

每一项都被截断到 [0, 1]，两项相加再除以 2，所以 risk 也在 [0, 1] 内，得分必然在 [0, 100] 内，`NormalizeScore` 可以什么都不做。

### 4.2 margin 和 sensitivity 分别做什么

**margin（m）是 σ 的系数。** KEP 用正态分布解释它：如果节点用量近似服从正态分布，「μ + η·σ ≤ 100%」对应用量超出容量的概率约为 16%（η=1）、2.5%（η=2）、0.15%（η=3）。η 越大越保守。不过代码里并没有计算概率，m 只是一个线性权重，并且 `m·σ'` 超过 1 后会被截断。margin 设得太大时，σ 稍大的节点都会被截在 1，彼此之间就没有区别了。

**sensitivity（s）是开方次数。** 因为 σ ∈ [0, 1]，开 s 次方（s > 1）会**放大小的 σ**：σ = 0.05 时，s=1 得 0.05，s=2 得 0.224，s=3 得 0.368。在实际集群中，节点 CPU 标准差多在 0 到 10% 之间，s=1 时它对风险的贡献很小，几乎被均值淹没；s=2 时，标准差的差异才能真正影响排序。这就是 README 推荐 s=2 的原因。

两个边界值值得单独说明：

- **s = 0**：defaulting 只替换负数，0 会原样保留。此时 `1/s = +Inf`，`math.Pow(σ, +Inf)` 对 σ < 1 得 0、对 σ = 1 得 1，相当于**关掉了波动项**。单测 `zero sensitivity` 用 μ=0.5、σ=0.36 断言得 75 分，正好是 `100 − 50 × 0.5`。
- **s < 0**：只有在绕过 defaulting、直接构造内部参数时才会出现（单测里就是这样）。此时 `if sensitivity >= 0` 不成立，不开方，等同于 s=1。

### 4.3 等分线

令 score = S，则 `μ + m·σ^(1/s) = 2(1 − S/100)`。当 s=1、m=1 时，等分线是 μ–σ 平面上斜率为 −1 的直线，正好是 KEP 里的 `M + V = c` 对角线；当 s=2 时，等分线变成 `μ = c − √σ`，在 σ 接近 0 处很陡。也就是说，σ 从 0 增加到 0.05 带来的扣分，和 μ 增加 0.22 相当。

{{< figure src="/images/posts/sched-plugins/38-lvrb-iso-risk.svg" caption="μ–σ 平面上的等分线：实线 sensitivity=1，虚线 sensitivity=2；右侧是 4.4 节算例（点击查看大图）" link="/images/posts/sched-plugins/38-lvrb-iso-risk.svg" >}}

### 4.4 算例

两个节点，都是 4 核、8Gi，margin = 1。待调度 Pod 的 request 为 `cpu: 500m`、`memory: 1Gi`，各占容量的 0.125。指标来自 Prometheus（有 AVG 和 STD）：

| 节点 | CPU 均值 / 标准差 | 内存均值 / 标准差 |
|---|---|---|
| N1 | 50% / 5% | 40% / 5% |
| N2 | 40% / 25% | 40% / 10% |

**sensitivity = 1（默认）：**

- N1 CPU：μ = 0.5 + 0.125 = 0.625，σ' = 0.05，risk = 0.3375，得 66.25
- N1 内存：μ = 0.4 + 0.125 = 0.525，σ' = 0.05，risk = 0.2875，得 71.25
- N1 = min(66.25, 71.25) = 66.25 → **66**
- N2 CPU：μ = 0.525，σ' = 0.25，risk = 0.3875，得 61.25
- N2 内存：μ = 0.525，σ' = 0.10，risk = 0.3125，得 68.75
- N2 = min(61.25, 68.75) = 61.25 → **61**

**sensitivity = 2：**

- N1 CPU：σ' = √0.05 = 0.2236，risk = (0.625 + 0.2236) / 2 = 0.4243，得 57.57
- N1 内存：risk = (0.525 + 0.2236) / 2 = 0.3743，得 62.57
- N1 → 57.57 → **58**
- N2 CPU：σ' = √0.25 = 0.5，risk = 0.5125，得 48.75
- N2 内存：σ' = √0.1 = 0.3162，risk = 0.4206，得 57.94
- N2 → 48.75 → **49**（`math.Round` 对 .5 向远离 0 的方向取整）

如果只看 CPU 均值，N2（40%）比 N1（50%）空，`LeastAllocated` 类的策略会选 N2。LVRB 在两种配置下都选了更稳定的 N1。s=2 时，两者的差距从 5 分拉大到 9 分，风险项在总分中的权重更大。

单测 `pkg/trimaran/loadvariationriskbalancing/analysis_test.go` 的 `valid data` 用例可以用来验证公式：Capacity=100、Req=10、UsedAvg=40、UsedStdev=36、m=s=1，μ = 0.5，σ' = 0.36，risk = 0.43，得 57。`loadvariationriskbalancing_test.go` 的 `CPU and Memory metrics` 用例则演示了取 min：CPU 得 67，内存 μ = 0.5 + 0.5 = 1.0、σ = 0.1，得 45，最终得 45。

## 五、边界情况与坑

**只用 metrics-server 时，模型退化成「按实际用量最少优先」。** metrics-server provider 只给 `Latest`，没有 `STD`，σ 恒为 0，公式变成 `score = 100 − 50 × (u + r)`，其中 u 是当前瞬时利用率。margin 和 sensitivity 都不起作用，μ 也是一个瞬时值，没有经过时间平均。要让 LVRB 发挥设计意图，应该接 Prometheus。

**满载但稳定的节点仍有 50 分。** 因为除以 2，μ = 1、σ = 0 的节点 risk 只有 0.5。测试 `hot node`（CPU 均值 100%，Pod 为空）断言的就是 50 分。LVRB 实际的分数区间往往只有 [50, 100] 左右，和其它满幅 [0, 100] 的打分插件放在一起时，等效权重大约只有配置值的一半。KEP 的原始算法是 `S = min(M + r + V, 1)`、`U = (1 − S) × 100`，没有除以 2，满载节点得 0 分。实现改成了两项各自截断后取平均，好处是 μ 和 σ' 各自饱和之前都能区分节点，代价是分值区间被压缩。

**没有滞后补偿，批量调度会扎堆。** 如第二节所述，`eventHandler` 只创建、不读取。连续调度一批 Pod 时，在下一次快照刷新之前（最长 30 秒，Prometheus 15 分钟均值下更久），所有 Pod 看到的都是同一份 μ 和 σ，会落到同一个「风险最低」的节点上。区别只在于每个 Pod 自己的 request 都会加到 μ 上，但这只计入了**当前**这个 Pod，之前刚调度的 Pod 不会被计入。

**标准差是对平滑过的序列算的。** Prometheus provider 发的是 `stddev_over_time(instance:node_cpu:ratio[15m])`。而 `instance:node_cpu:ratio` 这类 recording rule 本身就是在几分钟窗口上算出的速率，是已经平滑过的序列，它的标准差会小于原始秒级用量的标准差。LVRB 直接使用这个值，没有做修正。下一篇的 `LowRiskOverCommitment` 用 `smoothingWindowSize` 专门处理了这个问题。

**BestEffort Pod。** request 为 0，μ 中只剩节点自身用量，LVRB 退化为按当前风险选节点，不会因为这个 Pod 有任何区别。

**与 TLP 同时启用。** Collector 的注释明确说明各插件「目标可能冲突，不建议同时启用」。TLP 装箱，LVRB 摊平，两者的分数会相互抵消。再加上上一篇提到的库模式重复注册 `/watcher` 会导致 panic，同时启用时至少要切到服务模式。

## 六、发散

**这是均值–方差思路在调度上的应用。** 把节点看成一个资产组合，μ 是期望收益（这里是期望负载），σ 是波动，LVRB 优化的是一个线性的风险度量 `μ + m·σ'`。它没有把风险换算成「超出容量的概率」，而是把正态分布的 68–95–99.7 规则当作选择 margin 的经验依据。下一篇的 `LowRiskOverCommitment` 走得更远：用 Beta 分布拟合 [0, 1] 区间上的利用率，直接计算「超出某个阈值的概率」。Beta 分布天然有界，不会出现正态分布给利用率分配负值或超过 100% 的问题。

**和 in-tree 均衡类插件的对照。** `NodeResourcesFit` 的 `LeastAllocated` 按 request 选最空的节点；`NodeResourcesBalancedAllocation` 让同一节点上 CPU 与内存的 request 占比接近（详见[《BalancedAllocation》](/posts/sched-plugin-19-balanced-allocation/)）。LVRB 是 `LeastAllocated` 的「实际用量 + 波动」版本。它对多种资源取 min，是瓶颈思路，和 BalancedAllocation 的「让各资源占比接近」并不相同：LVRB 不在乎 CPU 和内存是否均衡，只在乎最危险的那个资源。

**参数怎么选。** 一个可操作的思路：先看集群中节点 CPU 标准差的分布（直接在 Prometheus 上查 `stddev_over_time(instance:node_cpu:ratio[15m])`）。如果大多数节点在 2% 到 8% 之间，s=1 时 σ' 的影响不超过 4 分，几乎可以忽略；s=2 时 σ' 落在 0.14 到 0.28 之间，影响 7 到 14 分，足以改变排序。margin 用来整体调节「波动和均值谁更重要」，但要注意 `m·σ'` 有上限 1。

## 七、小结

- LVRB 只有 Score。CPU 和内存各算一次 `risk = (μ + m·σ^(1/s)) / 2`，其中 μ 包含 Pod 的 request，σ 只来自节点历史；`score = 100 × (1 − risk)`，两种资源都有数据时取较小值。
- `margin` 是 σ 的线性系数，`sensitivity` 是开方次数，后者用于放大小的 σ；负数被静默替换成默认值 1，0 会关掉波动项。
- 风险除以 2 使分值区间压缩到大约 [50, 100]；只接 metrics-server 时 σ 恒为 0，模型退化。
- 插件创建了 `PodAssignEventHandler` 却从不读取，没有滞后补偿。

LVRB 关心的是「已有负载会不会冲高」，它用 request 估计新 Pod 的负载。但很多 Pod 的 limit 远大于 request，节点上 limit 之和可能早就超过了容量。下一篇[《LowRiskOverCommitment》](/posts/sched-plugin-39-low-risk-overcommitment/)把 limit 引进来，用 Beta 分布估计超卖的风险。
