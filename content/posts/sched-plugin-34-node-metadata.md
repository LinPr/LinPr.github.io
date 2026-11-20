---
title: 'scheduler-plugins [out-of-tree] NodeMetadata：把一个节点 label 变成打分依据'
date: 2026-11-19T13:40:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "scoring"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NodeMetadata 读取节点上的一个 label 或 annotation，按数值高低或时间新旧给节点打分，用来表达成本、硬件代际、维护时间之类的软偏好。本文拆解它的参数与校验、数值和时间戳两条解析路径、为无效数据预留 0 分的归一化算法，并给出带数字的算例和只有读代码才能发现的截断、溢出与 label 格式限制。"
---

上一篇 [NodeResourcesAllocatable](/posts/sched-plugin-33-node-resources-allocatable/) 用节点规格给节点打一个静态的分。规格只是节点众多属性中的一个，运维常常还想表达「便宜的节点优先」「新上架的硬件优先」「最近做过维护的节点优先」这类偏好。这些信息通常已经以 label 或 annotation 的形式挂在 Node 上，`NodeMetadata` 就是把它们直接变成分数的插件。

它只实现 Score 扩展点（含 `NormalizeScore`），通过 `app.WithPlugin(nodemetadata.Name, nodemetadata.New)` 注册到 scheduler-plugins 的调度器二进制中，需要在 Profile 中显式启用，没有 feature gate。设计文档是 `kep/942-node-metadata-scoring`。本文基于 scheduler-plugins 提交 `cfeb3aa`（依赖 Kubernetes v1.36.4）。

## 一、使用方式

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: cost-aware-scheduler
    plugins:
      score:
        enabled:
          - name: NodeMetadata
            weight: 10
    pluginConfig:
      - name: NodeMetadata
        args:
          metadataKey: "node.example.com/hourly-cost"
          metadataSource: "Annotation"
          metadataType: "Number"
          scoringStrategy: "Lowest"
```

这是 `manifests/nodemetadata/scheduler-config.yaml` 中的第二个示例：节点用 annotation 标注每小时成本，越便宜的节点得分越高。Pod 通过 `schedulerName: cost-aware-scheduler` 选择这个 Profile。数据来源就是调度器 snapshot 里的 Node 对象，不需要额外的 informer 或 CRD，外部系统更新 annotation 后，下一个调度周期就能看到。

## 二、参数与校验

```go
// apis/config/types.go
type NodeMetadataArgs struct {
	metav1.TypeMeta

	// MetadataKey is the name of the label or annotation to use for scoring
	MetadataKey string `json:"metadataKey"`

	// MetadataSource indicates whether to read from labels or annotations
	// Valid values: "Label", "Annotation"
	MetadataSource MetadataSourceType `json:"metadataSource"`

	// MetadataType indicates the type of value in the metadata
	// Valid values: "Number", "Timestamp"
	MetadataType MetadataValueType `json:"metadataType"`

	// ScoringStrategy defines how nodes should be scored
	// For Number type: "Highest" or "Lowest"
	// For Timestamp type: "Newest" or "Oldest"
	ScoringStrategy MetadataScoringStrategy `json:"scoringStrategy"`

	// TimestampFormat is the Go time format string for parsing timestamps
	// Only used when MetadataType is "Timestamp"
	// Default: time.RFC3339 ("2006-01-02T15:04:05Z07:00")
	// ...
	TimestampFormat string `json:"timestampFormat,omitempty"`
}
```

前四个字段都没有默认值，唯一的默认值是 `MetadataType` 为 `Timestamp` 时 `TimestampFormat` 取 `time.RFC3339`，v1 的 `SetDefaults_NodeMetadataArgs` 和 `New` 里各设了一遍。`ValidateNodeMetadataArgs` 检查：

- `metadataKey` 不能为空；
- `metadataSource` 只能是 `Label` 或 `Annotation`；
- `metadataType` 只能是 `Number` 或 `Timestamp`；
- `scoringStrategy` 只能是四个值之一，且 `Number` 只能配 `Highest`/`Lowest`，`Timestamp` 只能配 `Newest`/`Oldest`。

校验失败时 `New` 返回错误，调度器启动失败。所以只在 `plugins.score.enabled` 里写了 `NodeMetadata`、却忘了 `pluginConfig`，调度器起不来，错误信息是 `metadataKey cannot be empty`。

一个 Profile 里同一个插件只能有一个实例，也就只能用一个 key。KEP 的 Non-Goals 明确不支持多 key 组合，想同时考虑成本和代际，只能先由外部系统把它们合成一个数字。

## 三、源码

### 3.1 结构与 Score

```go
// pkg/nodemetadata/node_metadata.go
type NodeMetadata struct {
	logger klog.Logger
	handle framework.Handle
	args   *config.NodeMetadataArgs
}

// Name is the name of the plugin used in the Registry and configurations.
const Name = "NodeMetadata"
const invalidScore int64 = math.MinInt64

func (nm *NodeMetadata) Score(ctx context.Context, state framework.CycleState, pod *v1.Pod, nodeInfo framework.NodeInfo) (int64, *framework.Status) {
	logger := klog.FromContext(klog.NewContext(ctx, nm.logger)).WithValues("ExtensionPoint", "Score")
	node := nodeInfo.Node()
	if node == nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("node %q not found", nodeInfo.Node().Name))
	}

	score, err := nm.calculateScore(node)
	if err != nil {
		logger.V(5).Info("Failed to calculate score for node", "node", node.Name, "error", err, "pod", pod.Name)
		// Invalid or missing metadata should sort after valid nodes.
		return invalidScore, nil
	}

	logger.V(10).Info("Score: ", "score", score, "node", node.Name, "pod", pod.Name)
	return score, nil
}
```

两个设计点：

- key 不存在、值解析失败，都**不报错**，而是返回哨兵值 `math.MinInt64`。如果这里返回 `Error` 状态，框架会让整个 Pod 的本轮调度失败，一个节点的 annotation 写错就会让所有 Pod 都调度不了。用哨兵值把错误降级为「这个节点在本插件里得最低分」，符合 KEP 中「缺失或格式错误的元数据不应导致调度失败」的要求。
- `node == nil` 分支里调用了 `nodeInfo.Node().Name`，这本身就是对 nil 指针取字段，会 panic。好在框架传进来的 `NodeInfo` 都来自 snapshot，节点不为空，这个分支实际走不到。

### 3.2 两条解析路径

```go
// pkg/nodemetadata/node_metadata.go
func (nm *NodeMetadata) parseNumericValue(value string) (int64, error) {
	numValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse numeric value %q: %w", value, err)
	}

	// Convert to int64 with optional inversion based on scoring strategy
	score := int64(numValue)
	if nm.args.ScoringStrategy == config.ScoringStrategyLowest {
		// Invert: lower values should get higher scores
		score = -score
	}
	// For ScoringStrategyHighest, higher values naturally get higher scores

	return score, nil
}

func (nm *NodeMetadata) parseTimestampValue(value string) (int64, error) {
	// Try parsing with the configured format
	timestamp, err := time.Parse(nm.args.TimestampFormat, value)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp %q with format %q: %w", value, nm.args.TimestampFormat, err)
	}

	// Calculate age in seconds
	age := time.Since(timestamp).Seconds()

	var score int64
	if nm.args.ScoringStrategy == config.ScoringStrategyNewest {
		// Newer timestamps (smaller age) should get higher scores
		// Use negative age so newer = less negative = higher after normalization
		score = -int64(age)
	} else {
		// Older timestamps (larger age) should get higher scores
		score = int64(age)
	}

	return score, nil
}
```

两条路径的共同思路是：把「偏好方向」编码到原始分的符号里，让归一化永远是「原始分越大越好」。`Lowest` 取负，`Newest` 用负的年龄，于是归一化代码不需要知道策略是什么。

数值路径用 `ParseFloat` 接受小数，再用 `int64(numValue)` 转换。Go 的浮点到整数转换是**向零截断**，不是四舍五入。KEP 的 Known limitations 写的是「numeric values are rounded before scoring」，与代码不符，以代码为准：1.2 和 1.7 都变成 1。

时间戳路径算的是 Score 时刻距离时间戳的秒数，未来的时间戳会得到负的年龄，`Newest` 下反而得分最高。每个节点调用一次 `time.Since`，同一轮打分里各节点的「现在」相差微秒级，不影响秒级精度。

### 3.3 NormalizeScore：为无效节点预留 0 分

```go
// pkg/nodemetadata/node_metadata.go
func (nm *NodeMetadata) NormalizeScore(ctx context.Context, state framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// ...
	minScore, maxScore := int64(math.MaxInt64), int64(math.MinInt64)

	for i := range scores {
		if scores[i].Score == invalidScore {
			continue
		}
		if scores[i].Score < minScore {
			minScore = scores[i].Score
		}
		if scores[i].Score > maxScore {
			maxScore = scores[i].Score
		}
	}

	// The bounds stay crossed only when no valid score was seen.
	if minScore > maxScore {
		for i := range scores {
			scores[i].Score = framework.MinNodeScore
		}
		return nil
	}

	validLowerBound := framework.MinNodeScore + 1

	// Reserve MinNodeScore for invalid nodes, so valid nodes always score above it.
	if maxScore == minScore {
		for i := range scores {
			if scores[i].Score == invalidScore {
				scores[i].Score = framework.MinNodeScore
				continue
			}
			scores[i].Score = validLowerBound
		}
		return nil
	}

	// Normalize scores to [MinNodeScore, MaxNodeScore] range
	oldRange := maxScore - minScore
	newRange := framework.MaxNodeScore - validLowerBound

	for i := range scores {
		if scores[i].Score == invalidScore {
			scores[i].Score = framework.MinNodeScore
			continue
		}
		scores[i].Score = ((scores[i].Score-minScore)*newRange)/oldRange + validLowerBound
	}
	// ...
	return nil
}
```

和 [NodeResourcesAllocatable](/posts/sched-plugin-33-node-resources-allocatable/) 的 min-max 相比，这里多了一层「保留 0 分」的设计：

- 求极值时跳过哨兵值，否则 `MinInt64` 会把区间撑到天文数字，所有有效节点都挤在同一个分数上；
- 无效节点固定为 `MinNodeScore`（0），有效节点映射到 `[1, 100]`，`newRange` 是 99 而不是 100；
- 所有有效节点值相同时，它们都得 1 分而不是 0 分，依然和无效节点拉开差距；
- 全部无效时都是 0 分，插件不起作用。

这样做的意义是：「没有数据」和「数据最差」被明确区分开。如果不预留，`Lowest` 策略下成本最高的节点会得 0 分，与没有标注成本的节点并列，而运维通常希望先用已知成本的节点。

## 四、算例

{{< figure src="/images/posts/sched-plugins/34-nodemetadata-normalize.svg" caption="Number + Lowest 策略下，五个节点从 annotation 字符串到最终分数的全过程；注意 1.2 与 1.7 被截断成同一个值（点击查看大图）" link="/images/posts/sched-plugins/34-nodemetadata-normalize.svg" >}}

数值场景，`Lowest`：五个节点的 `hourly-cost` 分别是 `"2.5"`、`"1.2"`、`"1.7"`、`"0.8"` 和缺失。

1. 截断：2、1、1、0；取负：−2、−1、−1、0；缺失的节点为 `MinInt64`。
2. 跳过哨兵求极值：min = −2，max = 0，`oldRange = 2`，`newRange = 99`。
3. 映射：node-a `0 × 99 / 2 + 1 = 1`；node-b、node-c `1 × 99 / 2 + 1 = 49 + 1 = 50`；node-d `2 × 99 / 2 + 1 = 100`；node-e 0。

1.2 元和 1.7 元的节点得分完全相同。如果成本差异主要在小数部分，应该先把值放大成整数再写进 annotation，比如以「分」为单位写 `120` 和 `170`。

时间戳场景，`Newest`：三个节点的 `last-maintenance` 分别是 2 小时前、1 天前、10 天前。

1. 年龄：7200、86400、864000 秒；取负：−7200、−86400、−864000。
2. min = −864000，max = −7200，`oldRange = 856800`。
3. 2 小时前：`856800 × 99 / 856800 + 1 = 100`；1 天前：`777600 × 99 / 856800 = 89.85`，截断为 89，加 1 得 90；10 天前：1。

分数是线性的，1 天前和 2 小时前只差 10 分，10 天前则掉到最低。线性归一化放大的是候选集合中的极差，一个特别老的节点会把其余节点都推向高分端。

## 五、边界与坑

**label 值的格式限制**。label 值最长 63 个字符，只能包含字母数字、`-`、`_`、`.`，且必须以字母数字开头和结尾。这带来两个后果：RFC3339 时间戳含有 `:`，不能作为 label 值，`metadataSource: Label` 配 `Timestamp` 时必须换成 `2006-01-02` 这类格式（`manifests/nodemetadata/scheduler-config.yaml` 第四个示例正是这么做的）；负数以 `-` 开头，也不能写进 label。annotation 没有这些限制，需要时间精度或负数时用 annotation。

**ParseFloat 接受的写法比想象的多**。`"1e3"`、`"0x1p-2"`、`"Inf"`、`"NaN"` 都能解析成功。超出 int64 范围的浮点数（包括 `Inf`、`NaN`）转成 int64 的结果在 Go 规范里是实现相关的，在 amd64 上通常得到 `MinInt64`，恰好与哨兵值相同而被当作无效，但不应依赖这个巧合。KEP 也把「值恰好等于 `math.MinInt64` 时会被当成无效」列为已知限制。

**归一化乘法可能溢出**。`(score − min) × 99` 是 int64 乘法，有效值的极差超过约 9.3e16 时会溢出，产生负数或超过 100 的分数。框架在汇总分数前会检查每个插件的分数是否落在 `[0, 100]`，越界则返回错误，这个 Pod 本轮调度失败。秒级时间戳和常见的业务数字远到不了这个量级，但用纳秒时间戳当 `Number` 就有可能碰到。

**它只是偏好**。KEP 强调这个插件是 advisory 的：它和其他打分插件按权重相加，不能保证 Pod 一定落在分数最高的节点上，也不会在 annotation 变化后迁移已运行的 Pod。需要硬约束时应使用 NodeAffinity。

## 六、发散：和 NodeAffinity 加权偏好的对比

同样是「按节点 label 表达偏好」，NodeAffinity 的 `preferredDuringSchedulingIgnoredDuringExecution` 也能做到，但两者的表达能力不同：

| | NodeAffinity preferred | NodeMetadata |
| --- | --- | --- |
| 配置位置 | Pod spec，每个工作负载自己声明 | 调度器 Profile，集群级统一策略 |
| 值的使用 | 匹配表达式（`In`、`Gt`、`Lt` 等），命中即得该项权重 | 把值本身当成连续的分数 |
| 档位 | 想要五档就要写五条 term | 自动线性映射，任意多档 |
| 数据来源 | 只能是 label | label 或 annotation |
| 时间语义 | 无 | 支持按时间新旧打分 |

当偏好是「越便宜越好」这种连续量时，NodeMetadata 不需要在每个工作负载里罗列档位；当偏好只对部分工作负载生效时，NodeAffinity 更灵活。两者也可以组合：用 Profile 区分工作负载类别，再在对应 Profile 里启用 NodeMetadata。

这个插件的另一个价值是解耦：成本系统、资产管理系统只需要往 Node 上写 annotation，就能影响调度，而不必改动调度器或工作负载定义。代价是数据质量完全依赖这些外部系统，元数据过期了，调度器无从知晓。

## 小结

- `NodeMetadata` 读取一个 label 或 annotation，`Number` 按 `Highest`/`Lowest`、`Timestamp` 按 `Newest`/`Oldest` 打分，偏好方向编码在原始分的符号里。
- 缺失或解析失败的节点返回 `MinInt64` 哨兵值，归一化时被固定为 0 分；有效节点映射到 `[1, 100]`，保证有数据的节点排在前面。
- 数值经 `int64()` 向零截断（KEP 说「四舍五入」与代码不符），小数差异会被抹平；label 值不能包含 `:` 也不能以 `-` 开头。
- 所有参数都没有默认值（时间格式除外），未配置 `pluginConfig` 会导致调度器启动失败。

下一篇 [PodState](/posts/sched-plugin-35-pod-state/) 同样是一个小型打分插件，它看的不是节点的属性，而是节点上 Pod 的状态。
