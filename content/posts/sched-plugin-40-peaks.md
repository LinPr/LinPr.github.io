---
title: 'scheduler-plugins 40 [out-of-tree] Peaks：按功耗模型选边际功耗最小的节点'
date: 2026-11-19T14:40:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "trimaran", "energy"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "Peaks 是 Trimaran 中唯一以功耗为目标的插件：用 P(x)=K0+K1·e^(K2·x) 的功耗模型计算把 Pod 放上节点后的功耗跳变，跳变越小分越高。本文拆解功耗模型的加载、Score 的放大取整与反向归一化，推导曲线凹凸性决定装箱还是分散，并指出无模型、无指标、预测超载的节点在归一化后反而得满分。最后回顾整个系列。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `Peaks` 实现的扩展点（Score、NormalizeScore）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/40.svg" link="/images/posts/sched-plugins/callstack/40.svg" caption="Peaks 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇[《LowRiskOverCommitment》](/posts/sched-plugin-39-low-risk-overcommitment/)用 limit 和 Beta 分布估计超卖风险。到这里，Trimaran 的前三个插件优化的都是**性能**：装到目标水位、平衡波动风险、降低超卖风险。最后一个插件 `Peaks`（Power Efficiency Aware Kubernetes Scheduler）换了目标：它要让集群的**总功耗**增加得最少。

`Peaks` 实现了 Score 和 `NormalizeScore`。它是 Trimaran 四个插件里唯一一个 `NormalizeScore` 不为空的，这一点在后面很关键。它没有 PreScore，不写 CycleState，不实现 `EnqueueExtensions`，也不创建 `PodAssignEventHandler`。它不在默认 Profile 里，没有 feature gate，需要在 scheduler-plugins 的调度器（`cmd/scheduler/main.go` 中 `app.WithPlugin(peaks.Name, peaks.New)`）里启用。设计文档在 `kep/tbd-peaks-power-aware-scheduling`，KEP 编号还是 TBD。源码基线为 scheduler-plugins `cfeb3aa`，文件头的版权年份是 2024，是四个插件里最新的一个。

## 一、问题：同样一个 Pod，放在哪里更省电

服务器功耗和 CPU 利用率的关系通常不是线性的。KEP 的两个 user story 分别是：

1. **异构节点**：不同 CPU 架构、虚拟机和裸金属混部、配置不同，每台机器的「利用率–功耗」曲线不同，同样增加 10% 的利用率，耗电增加量也不同。
2. **同构但非线性**：即便所有节点的曲线一样，只要曲线是非线性的，把 Pod 放在利用率不同的节点上，功耗增量也不同。

Peaks 的做法是：对每个节点，用它的功耗模型分别计算「当前利用率」和「放上 Pod 之后的利用率」对应的功耗，两者之差（功耗跳变）越小，得分越高。模型的训练不在插件职责内，插件只做推理。KEP 里的实验用的是 Kepler 采集的能耗数据，训练出指数形式的模型。

## 二、参数与功耗模型的加载

Peaks 的参数比前三个插件都简单，没有 `TrimaranSpec`，只有一个地址和一个模型表。它的不少坑也正出在这么少的东西上。

### 2.1 PeaksArgs

`PeaksArgs` 只有两个配置字段（`TypeMeta` 之外）：`WatcherAddress` 与 `NodePowerModel`（第 7–8 行）。

```go {linenos=inline}
// apis/config/types.go
// PeaksArgs holds arguments used to configure the Peaks plugin
type PeaksArgs struct {
	metav1.TypeMeta

	// Address of load watcher service
	WatcherAddress string
	NodePowerModel map[string]PowerModel // Node power model where key is node name and value is power model
}

type PowerModel struct {
	K0 float64 `json:"k0"`
	K1 float64 `json:"k1"`
	K2 float64 `json:"k2"`
	// Power = K0 + K1 * e ^(K2 * x) : where x is utilisation
	// Idle power of node will be K0 + K1
}
```

和其它三个插件比，`PeaksArgs` **没有内嵌 `TrimaranSpec`**（第 3–9 行的结构体里没有这个字段），只有 `WatcherAddress` 一个字符串（第 7 行）。`New` 是这样创建 Collector 的：

```go {linenos=inline}
// pkg/trimaran/peaks/peaks.go
	collector, err := trimaran.NewCollector(logger, &config.TrimaranSpec{WatcherAddress: args.WatcherAddress})
	if err != nil {
		return nil, err
	}
```

因此 Peaks **只能用服务模式**。不写 `WatcherAddress` 时，`checkSpecs` 会发现 `MetricProvider.Type` 为空，`New` 返回 `invalid MetricProvider.Type, got ` 错误，调度器启动失败。另外，v1 版本的参数类型里两个字段的 json tag 写坏了：

```go {linenos=inline}
// apis/config/v1/types.go
	// Address of load watcher service
	WatcherAddress string `json:watcherAddress",inline"`
	// Associating power models to nodes using node labels will be added as a functionality in a future PR.
	NodePowerModel map[string]PowerModel `json:nodePowerModel",inline"`
```

`json:` 后面缺了左引号（第 3、5 行），`reflect.StructTag` 解析不出 json 键，序列化库会退回使用字段名本身。而 apimachinery 的 JSON 反序列化（`kjson.UnmarshalCaseSensitivePreserveInts`）是区分大小写的，所以配置文件里必须写 `WatcherAddress`、`NodePowerModel`，首字母大写，和其它插件的驼峰写法不同。仓库里的 `pkg/trimaran/peaks/deployment/scheduler-config.yaml` 正是这样写的：

```yaml {linenos=inline}
  pluginConfig:
    - name: Peaks
      args:
        WatcherAddress: http://<Replace with Watcher Address>:2020
        NodePowerModel: {Replace with Power Model Config}
```

v1 包里也没有 `SetDefaults_PeaksArgs`，两个字段都没有默认值，也没有校验。

### 2.2 功耗模型的两种来源

模型可以从插件参数里来，也可以从文件里来，`initNodePowerModels` 负责二选一。

```go {linenos=inline}
// pkg/trimaran/peaks/peaks.go
func initNodePowerModels(powerModel map[string]config.PowerModel) error {
	fmt.Printf("args power model : %+v\n", powerModel)
	if len(powerModel) > 0 {
		return nil
	}
	data, err := os.ReadFile(os.Getenv("NODE_POWER_MODEL"))
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&powerModel); err != nil {
		return err
	}
	return nil
}
```

逻辑分两个分支：

1. 配置里有 `NodePowerModel` 就直接用（第 4–6 行）；
2. 没有就读环境变量 `NODE_POWER_MODEL` 指向的 JSON 文件（第 7 行），格式是 `{"节点名": {"k0": …, "k1": …, "k2": …}}`。

这里有一个 Go 语义上的问题。`powerModel` 是值传递进来的 map。`Decode(&powerModel)`（第 13 行）的行为分两种：

1. 对 nil map 会新分配一个 map，并赋给函数内的局部变量；
2. 对非 nil 的空 map，则把键值写进原来那个 map。

调用方传入的是 `args.NodePowerModel`：

1. 配置里完全不写 `NodePowerModel`：经过 v1 → 内部类型的转换（`autoConvert_v1_PeaksArgs_To_config_PeaksArgs` 是直接按指针转换的），它是 nil。文件被成功解析，但结果只存在局部变量里，函数返回后就丢了。插件拿到的仍是 nil map，所有节点都使用零值模型。
2. 配置里写 `NodePowerModel: {}`：它是一个非 nil 的空 map，文件内容会写进这个 map，插件能正常使用。单测 `TestPeaksScore` 走的就是这条路径（`NodePowerModel: map[string]pluginConfig.PowerModel{}`）。

所以，用文件提供模型时有三处要注意：

1. 如果打算用 ConfigMap 挂载模型文件、再通过环境变量引用，配置里要显式写一个空的 `NodePowerModel: {}`，否则文件被读了，但不会生效；
2. `deployment/README.md` 里给的 JSON 模板把系数写成了带引号的字符串（`"k0":"<replace with node 1 k0>"`），而 `PowerModel` 的字段是 `float64`，替换时必须写成数字，否则解码报错，`New` 失败；
3. 同目录下的 `deployment.yaml` 既没有挂载这个 ConfigMap，也没有设置 `NODE_POWER_MODEL`，需要自己补上。

找不到节点对应的模型时，使用零值：

```go {linenos=inline}
// pkg/trimaran/peaks/peaks.go
func getPowerModel(nodeName string, powerModelMap map[string]config.PowerModel) config.PowerModel {
	powerModel, ok := powerModelMap[nodeName]
	if ok {
		return powerModel
	}
	return config.PowerModel{0, 0, 0}
}
```

模型按**节点名**索引（第 3 行的 `powerModelMap[nodeName]`）。v1 类型的注释写着「按节点标签关联功耗模型会在后续 PR 中加入」，目前还没有实现。每新增一个节点都要更新配置，并重启调度器（模型只在 `New` 时加载一次）。

## 三、Score：计算功耗跳变

模型加载好之后，Score 对每个节点做的事只有一件：算出「放上这个 Pod，功耗会增加多少」，再把这个增量当作代价返回。

```go {linenos=inline}
// pkg/trimaran/peaks/peaks.go
func (pl *Peaks) Score(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	// ...
	score := fwk.MinNodeScore
	nodeName := nodeInfo.Node().Name

	metrics, _ := pl.collector.GetNodeMetrics(logger, nodeName)
	if metrics == nil {
		logger.Error(nil, "Failed to get metrics for node; using minimum score", "nodeName", nodeName)
		return score, nil
	}

	quantity := resource.GetResourceRequestQuantity(pod, v1.ResourceCPU)
	curPodCPUUsage := quantity.MilliValue()

	var nodeCPUUtilPercent float64
	var cpuMetricFound bool
	for _, metric := range metrics {
		if metric.Type == watcher.CPU {
			if metric.Operator == watcher.Average || metric.Operator == watcher.Latest {
				nodeCPUUtilPercent = metric.Value
				cpuMetricFound = true
				break
			}
		}
	}
	// ...
	nodeCPUCapMillis := float64(nodeInfo.Node().Status.Capacity.Cpu().MilliValue())
	nodeCPUUtilMillis := (nodeCPUUtilPercent / 100) * nodeCPUCapMillis

	var predictedCPUUsage float64
	if nodeCPUCapMillis != 0 {
		predictedCPUUsage = 100 * (nodeCPUUtilMillis + float64(curPodCPUUsage)) / nodeCPUCapMillis
	}
	if predictedCPUUsage > 100 {
		return score, fwk.NewStatus(fwk.Success, "")
	} else {
		// ...
		jumpInPower := getPowerJumpForUtilisation(nodeCPUUtilPercent, predictedCPUUsage, getPowerModel(nodeName, pl.args.NodePowerModel))
		return int64(jumpInPower * math.Pow(10, 15)), fwk.NewStatus(fwk.Success, "")
	}
}
```

和 TLP（第 37 篇）的 Score 很像，但有三处不同：

1. **Pod 的 CPU 用量取 request**（第 13–14 行的 `GetResourceRequestQuantity` 与 `MilliValue()`），而且用的是 in-tree 的 `resource.GetResourceRequestQuantity`。它支持 Pod 级资源（在 `PodLevelResources` feature gate 开启且 Pod 设置了 `spec.resources` 时）和 sidecar 语义，overhead 只在 request 非零时才加。TLP 用的是 limit，并且是自己写的累加逻辑。
2. **没有滞后补偿**。刚调度、还没体现在指标里的 Pod 不会被计入。
3. 找到第一个 CPU 的 AVG/Latest 指标就 `break`（第 23 行），而 TLP 取的是最后一个。

功耗跳变的计算（第 3 行）：

```go {linenos=inline}
// pkg/trimaran/peaks/peaks.go
func getPowerJumpForUtilisation(x, p float64, m config.PowerModel) float64 {
	return m.K1 * (math.Exp(m.K2*p) - math.Exp(m.K2*x))
}
```

### 3.1 公式

记当前利用率为 x（%），Pod request 占容量的百分比为 b，放上 Pod 后的利用率为 p = x + b：

```text {linenos=inline}
P(u)  = K0 + K1 · e^(K2·u)
ΔP    = P(p) − P(x) = K1 · (e^(K2·p) − e^(K2·x)) = K1 · e^(K2·x) · (e^(K2·b) − 1)
raw   = int64(ΔP × 10^15)        当 p ≤ 100
      = 0                        当 p > 100、没有指标、或没有 CPU 指标
```

有几点需要注意：

1. **x 的单位是百分数**，不是 [0, 1] 的小数，所以 K2 是「每 1 个百分点」的指数系数。用 [0, 1] 区间训练出来的模型，K2 要除以 100 才能用在这里。
2. **K0 被消掉了**。K0 + K1 是空载功耗，K0 代表与负载无关的基础功耗，它不影响选择。也就是说，Peaks 不会因为一台机器的空载功耗高就避开它，只看边际功耗。
3. 乘以 10¹⁵ 再转 `int64`（Score 第 40 行的 `int64(jumpInPower * math.Pow(10, 15))`），是因为 Score 必须返回整数，而 ΔP 可能是零点几瓦这样的小数，直接取整会丢失全部区分度。代价是上限：`int64` 最大约 9.22×10¹⁸，ΔP 超过约 9223 时就会溢出。如果模型用毫瓦或微瓦作单位（load-watcher 采集的 Scaphandre 指标 `scaph_host_power_microwatts` 就是微瓦），ΔP 很容易超过这个值。Go 规范规定浮点转整数越界时结果由实现决定，在 amd64 上会得到 `math.MinInt64`，这个节点在下面的归一化中会变成最小值，从而得满分。所以模型要用瓦作单位。

### 3.2 凹凸性决定装箱还是分散

对同一个节点模型、同一个 Pod（b 固定），ΔP = K1·(e^(K2·b) − 1)·e^(K2·x)，它随当前利用率 x 单调变化：

1. **K1 > 0、K2 > 0**（凸、递增，最常见的指数模型）：x 越大，ΔP 越大。在同构节点之间，Peaks 会把 Pod 放到利用率最低的节点，效果是分散。
2. **K1 < 0、K2 < 0**（同样递增，但是凹的）：e^(K2·x) 随 x 减小，ΔP 随 x 减小。Peaks 偏好利用率高的节点，效果是装箱。

KEP 的 Story 2 说「同构节点、只要曲线非线性就能省电」，指的就是这两种情况，它们的效果方向相反。

KEP 实验里观察到的「Peaks 装箱、默认调度器分散」，来自异构节点、高能效节点的 K1 较小的场景。对于常见的凸模型，在同构节点之间，Peaks 的行为接近按实际 CPU 负载做 `LeastAllocated`。这时想省电，更好的办法是装箱后让空节点被 Cluster Autoscaler 回收，因为空载功耗（K0 + K1）只有关机才能省掉，KEP 中「Shutting down of a cluster node」这个用例也说明了这一点。

## 四、NormalizeScore：反向归一化

Score 返回的是代价，量级在 10¹⁶ 左右；框架要求的是 0~100 的分数。两者的方向和量纲都不对，转换由 `NormalizeScore` 完成，这也是 Trimaran 四个插件里唯一一个非空实现。

```go {linenos=inline}
// pkg/trimaran/peaks/peaks.go
func (pl *Peaks) NormalizeScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	minCost, maxCost := getMinMaxScores(scores)
	if minCost == 0 && maxCost == 0 {
		return fwk.NewStatus(fwk.Success, "")
	}
	var normCost float64
	for i := range scores {
		if maxCost != minCost {
			normCost = float64(fwk.MaxNodeScore) * float64(scores[i].Score-minCost) / float64(maxCost-minCost)
			scores[i].Score = fwk.MaxNodeScore - int64(normCost)
		} else {
			normCost = float64(scores[i].Score - minCost)
			scores[i].Score = fwk.MaxNodeScore - int64(normCost)
		}
	}
	return fwk.NewStatus(fwk.Success, "")
}
```

Score 返回的是「代价」（ΔP × 10¹⁵），量级在 10¹⁶ 左右，远远超出 [0, 100]。框架在调用完 `NormalizeScore` 之后才检查分数是否越界（`plugin %q returns an invalid score %v, it should in the range of [%v, %v] after normalizing`），所以这里必须把代价映射回 [0, 100]，并且取反（第 10–11 行）：代价最小的得 100，最大的得 0。

```text {linenos=inline}
score_i = 100 − ⌊100 × (raw_i − min) / (max − min)⌋     （max ≠ min）
score_i = 100                                            （max = min ≠ 0）
不变（全为 0）                                            （max = min = 0）
```

单测 `TestPeaksNormalizeScore` 覆盖了三种情况：

1. `{0, 100}` → `{100, 0}`；
2. `{0, 0}` → 原样不变（`NormalizeScore` 第 4–5 行提前返回）；
3. `{100, 100}` → 都是 100（第 13–14 行）。

注意第二、三种的不对称：所有节点代价都为 0 时全部得 0 分，代价相同但不为 0 时全部得 100 分。不过两者对排序都没有影响。

### 4.1 「代价为 0」的含义被混在一起

Score 在以下五种情况下都返回 0：

1. 节点没有指标（`metrics == nil`）；
2. 有指标但没有 CPU 的 AVG/Latest；
3. 预测利用率超过 100%；
4. 节点没有功耗模型（零值模型，K1 = 0）；
5. 模型的 K1 或 K2 为 0。

在 Score 阶段，0 对其它三个 Trimaran 插件表示「最差」。但到了 Peaks 的反向归一化里，0 是**最小的代价**，会被映射成 **100 分**。也就是说，在 Peaks 看来，没有数据、没有模型、甚至预测会超载的节点，都是「最省电」的节点。

{{< figure src="/images/posts/sched-plugins/40-peaks-power-jump.svg" caption="两台 8 核节点的功耗曲线与功耗跳变：n1 绝对功耗更低但边际功耗更大，Peaks 选 n2；预测超载的 n3 原始值为 0，归一化后反而得 100 分（点击查看大图）" link="/images/posts/sched-plugins/40-peaks-power-jump.svg" >}}

## 五、算例

三台 8 核节点（Capacity 8000m），待调度 Pod 的 CPU request 为 2 核，b = 25%。功耗模型（K0 不影响结果，省略）：

| 节点 | K1 | K2 | 当前 x | p = x + 25 |
|---|---|---|---|---|
| n1 | 30 | 0.02 | 30% | 55% |
| n2 | 60 | 0.015 | 10% | 35% |
| n3 | 30 | 0.02 | 90% | 115% |

**n1**：ΔP = 30 × (e^1.1 − e^0.6) = 30 × (3.00417 − 1.82212) = 35.46 W，原始分 35461416706677740。

**n2**：ΔP = 60 × (e^0.525 − e^0.15) = 60 × (1.69046 − 1.16183) = 31.72 W，原始分 31717476339048504。

**n3**：p = 115 > 100，原始分 0。

只有 n1、n2 参与时：min = 3.172×10¹⁶，max = 3.546×10¹⁶。

1. n1 → 100 − 100 = 0
2. n2 → 100 − 0 = 100

Peaks 选 n2。

这个结果反直觉。n1 在 0% 到 100% 的整个范围内，功耗都比 n2 低（30·e^(0.02x) < 60·e^(0.015x) 等价于 x < 138.6），按直觉它是「更节能」的机器。但两者所处的曲线位置不同：

1. n1 已经运行在 30%，处在自己指数曲线较陡的一段；
2. n2 只有 10%，还在平缓段。

所以同样 25 个百分点，n2 多耗的电更少。Peaks 优化的是**这一次放置的边际功耗**，不是机器的能效排名。

三台都参与时：min = 0（n3），max = 3.546×10¹⁶。

1. n1 → 100 − ⌊100 × 1⌋ = 0
2. n2 → 100 − ⌊100 × 0.8944⌋ = 100 − 89 = 11
3. n3 → 100 − 0 = 100

预测会超载的 n3 拿到满分。`NodeResourcesFit` 只按 request 过滤，n3 的实际负载 90% 不会让它被过滤掉，只要 request 还放得下，它就会出现在打分列表里。如果 Peaks 是 Profile 里权重最高的打分插件，Pod 就会被送到这台最热的机器上。

## 六、边界情况与坑

**新节点、采集失败的节点、模型没覆盖的节点都会被优先选择。** 这是 4.1 节的直接结论。在仓库给出的示例配置（`deployment/scheduler-config.yaml`）里，score 只启用了 Peaks，其它打分插件全部禁用（`disabled: '*'`），Peaks 的分数就是全部依据。

一个刚加入集群、还没有被 load-watcher 采集、也没有配置模型的节点，会持续得到满分，接收所有新 Pod，直到指标出现。缓解办法有两条：

1. 保证模型覆盖所有节点；
2. 同时启用 TLP 或 `NodeResourcesFit` 等打分插件来稀释 Peaks 的权重，或者用 nodeAffinity 把 Peaks 调度器的 Pod 限制在已建模的节点上。

**只考虑 CPU。** 内存、网络的功耗贡献都不在模型里，这在 KEP 的 Non-Goals 中写明了。load-watcher 的 Prometheus provider 虽然会拉取 Kepler、Scaphandre 的能耗指标（类型为 `Energy`），但 Peaks 没有使用这些实测值，只用 CPU 利用率去推理模型。

**没有滞后补偿，批量调度会扎堆。** 和 LVRB 一样，在下一次快照刷新之前，连续调度的 Pod 看到的是同一组 x。对于凸模型，最空的节点会连续接收多个 Pod。

**用 Capacity 做分母。** 和 TLP 一样。

**日志写法。** `logger.V(4).Info("Node :", nodeName, ", Node cpu usage current :", …)` 把格式化字符串当成了结构化日志的键值对，输出会很难读；`initNodePowerModels` 里的 `fmt.Printf` 直接写到标准输出，绕过了 klog。排查问题时，要到容器的 stdout 里找「args power model」这一行，来确认模型是否加载成功。

## 七、发散

**功耗感知调度的几个层次。** 从粗到细大致有四层：

1. 选择能效更高的机型（离线规划）；
2. 关掉空闲节点（Cluster Autoscaler / Karpenter 的缩容）；
3. 调度时选择边际功耗低的节点（Peaks）；
4. 运行时调频调压（DVFS，由内核和硬件完成）。

按 KEP 自己的数据，节点空载功耗占总功耗的比例很大，所以第二层的收益通常最大。

Peaks 在第三层，它真正的价值在于异构集群：当能效差异来自机型而不是利用率时，它能把负载引导到高能效机器上。对于同构集群和常见的凸模型，Peaks 实际上会分散负载，和「腾空节点再关掉」的目标相反，这一点需要在启用前想清楚。

**为什么模型选 K0 + K1·e^(K2·x)。** 三个参数刚好能表达「空载功耗 + 随负载上升、斜率逐渐变大」的形状。这和现代 CPU 在高负载时升频、电压随之升高，功耗按 f·V² 增长的现象一致。

但实测曲线也常见分段线性，甚至在中段呈凹形。插件把模型形式写死在 `getPowerJumpForUtilisation` 里，要换成分段线性或查表模型，需要修改代码。

**和其它 Trimaran 插件一起用。** Peaks 不创建 `PodAssignEventHandler`，也不支持库模式，所以它和 TLP、LVRB 同时启用时不会遇到第 37 篇提到的 `/watcher` 重复注册问题（前提是那几个也用服务模式）。但目标是否冲突要单独判断：Peaks（凸模型）倾向分散，TLP 倾向装箱。

## 八、小结

1. Peaks 只用服务模式的 load-watcher，参数键名必须写成大写开头的 `WatcherAddress`、`NodePowerModel`；用环境变量文件提供模型时，配置里要写空的 `NodePowerModel: {}`，否则文件内容不会生效。
2. Score 计算 ΔP = K1·(e^(K2·p) − e^(K2·x))，x、p 都是百分数，K0 被消掉；结果乘以 10¹⁵ 转为 int64，模型单位要用瓦，避免溢出。
3. `NormalizeScore` 把代价反向映射到 [0, 100]。没有指标、没有模型、预测超载都返回原始值 0，归一化后得 100 分，这是部署时最需要防范的问题。
4. 对同一个凸模型，ΔP 随当前利用率增大，Peaks 在同构节点间是分散的；凹模型才会装箱。它优化的是单次放置的边际功耗。

## 九、写在系列的最后

[开篇](/posts/sched-plugin-00-journey/)跟着一个 Pod 走过了调度的全过程，之后的四十篇按顺序拆开了每一站。回头看，这些插件可以按「判断依据从哪里来」分成三层，每往下一层，插件作者要回答的问题就多一些。

**第一层，数据就在 Pod 和 Node 对象上。** [SchedulingGates](/posts/sched-plugin-01-scheduling-gates/) 和 [PrioritySort](/posts/sched-plugin-02-priority-sort/) 决定 Pod 能否出队、按什么顺序出队；[NodeName](/posts/sched-plugin-03-node-name/)、[NodeUnschedulable](/posts/sched-plugin-04-node-unschedulable/)、[TaintToleration](/posts/sched-plugin-05-taint-toleration/)、[NodeAffinity](/posts/sched-plugin-06-node-affinity/)、[NodePorts](/posts/sched-plugin-07-node-ports/)、[NodeResourcesFit](/posts/sched-plugin-08-node-resources-fit/) 只需要比较两个对象的字段。

这一层最需要弄清楚的是 Status code：`Unschedulable` 还是 `UnschedulableAndUnresolvable`，决定了 [DefaultPreemption](/posts/sched-plugin-18-default-preemption/) 会不会为它去尝试抢占；QueueingHint 决定了哪些集群事件能把被拒绝的 Pod 重新放回 activeQ。

**第二层，数据在其它 API 对象和调度器自己的缓存里。** 卷相关的插件（[VolumeBinding](/posts/sched-plugin-11-volume-binding/) 等）要看 PV、PVC、StorageClass；[PodTopologySpread](/posts/sched-plugin-13-pod-topology-spread/) 和 [InterPodAffinity](/posts/sched-plugin-14-inter-pod-affinity/) 要统计全集群的 Pod 分布；[DynamicResources](/posts/sched-plugin-15-dynamic-resources/) 要和 ResourceClaim 的分配状态打交道；[GangScheduling](/posts/sched-plugin-21-gang-scheduling/) 和 out-of-tree 的 [Coscheduling](/posts/sched-plugin-27-coscheduling/)、[CapacityScheduling](/posts/sched-plugin-28-capacity-scheduling/)、[NodeResourceTopologyMatch](/posts/sched-plugin-30-node-resource-topology/) 引入了 PodGroup、ElasticQuota、NodeResourceTopology 等 CRD。

这一层的难点在于一致性：

1. CycleState 怎么存；
2. `Clone` 怎么写；
3. PreFilterExtensions 的 AddPod/RemovePod 怎么让抢占模拟看到正确的状态；
4. Reserve 失败时怎么回滚。

**第三层，数据在集群之外。** Trimaran 四个插件的输入来自 Prometheus、metrics-server 和人工训练的功耗模型。这些数据有滞后、会缺失、会过期，而且调度器自己的 assume 机制看不到它们。

这四篇反复遇到同一类问题：

1. 缺数据时给几分；
2. 补偿逻辑和数据窗口是否匹配；
3. 分数为 0 在归一化之后是什么含义；
4. 多个实例能否共存。

它们都只做 Score，不做 Filter，原因也在这里：外部数据不可靠时，最多是放得不够好，而不是让 Pod 一直 Pending。

整个调度框架的设计，就是在这三层之间划清责任：第一、二层插件负责「能不能放」，用 Status code 和 QueueingHint 跟队列协作；所有层的插件都可以参与「放哪里更好」，最后由 [DefaultBinder](/posts/sched-plugin-24-default-binder/) 写出一个 Binding。读完全部插件之后再看 `schedule_one.go`，每一个扩展点调用背后对应的是哪些问题，应该都能对上号了。
