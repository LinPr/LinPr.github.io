---
title: 'scheduler-plugins 33 [out-of-tree] NodeResourcesAllocatable：按节点规格而不是按余量打分'
date: 2026-11-19T13:30:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "scoring"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NodeResourcesAllocatable 只根据节点 allocatable 的加权和打分，Least 模式让小 Pod 先占小节点、把大节点留给大 Pod，Most 模式则相反。本文拆解它的参数与默认权重、打分与 min-max 归一化、带数字的算例，以及它和 in-tree NodeResourcesFit 打分策略在语义、量纲和组合使用上的区别。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `NodeResourcesAllocatable` 实现的扩展点（Score、NormalizeScore）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/33.svg" link="/images/posts/sched-plugins/callstack/33.svg" caption="NodeResourcesAllocatable 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇 [CrossNodePreemption](/posts/sched-plugin-32-cross-node-preemption/) 结束了抢占相关的几个插件，故事回到 Score 阶段。接下来四篇都是 scheduler-plugins 里的小型打分插件，本篇的 `NodeResourcesAllocatable` 是其中最「资源化」的一个。

它只实现了 Score 扩展点（含 `NormalizeScore`），通过 `app.WithPlugin(noderesources.AllocatableName, noderesources.NewAllocatable)` 注册到 scheduler-plugins 的调度器二进制中，需要在 Profile 里显式启用，没有 feature gate，也没有 Filter、EventsToRegister 这些部分。本文基于 scheduler-plugins 提交 `cfeb3aa`（依赖 Kubernetes v1.36.4），对照的 in-tree 代码取自 Kubernetes master 提交 `aabdf5a2`。

## 一、它解决什么问题

设计文档 `kep/48-node-resources-allocatable-scoring` 描述了一个异构集群的场景：集群里有 10 单位和 200 单位两种节点，依次来了请求为 5、5、100、100 的四个 Pod。

用 in-tree 的 `LeastAllocated` 打分，过程是这样的：

1. 两个节点都空，第一个 5 可能落在任一节点；
2. 第二个 5 会去「已分配比例更低」的节点，大概率是大节点；
3. 最终可能是 `5/10` 和 `105/200`，第二个 100 放不下了。

所有基于「已分配比例」的打分都有这个问题：一个小 Pod 放在大节点上，比例变化很小，看起来很划算。

`NodeResourcesAllocatable` 换了一个维度：只看节点本身有多大。`mode: Least` 时小节点得分高，5 和 5 先填满小节点，两个 100 都能落到大节点。KEP 里的第二个场景是 `mode: Most`：少量常驻大节点加上按需伸缩的小节点，优先把 Pod 放到大节点上，小节点空闲时可以被缩容。

两个场景对应同一个开关：`mode`。插件对外的可配置项也只有它和资源权重。

## 二、参数与默认值

`NodeResourcesAllocatableArgs` 只有两个配置字段（`TypeMeta` 之外），一个决定看哪些资源（`Resources`，第 10 行），一个决定偏好哪一头（`Mode`，第 13 行）：

```go {linenos=inline}
// apis/config/types.go
type NodeResourcesAllocatableArgs struct {
	metav1.TypeMeta `json:",inline"`

	// Resources to be considered when scoring.
	// Allowed weights start from 1.
	// An example resource set might include "cpu" (millicores) and "memory" (bytes)
	// with weights of 1<<20 and 1 respectfully. That would mean 1 MiB has equivalent
	// weight as 1 millicore.
	Resources []schedconfig.ResourceSpec `json:"resources,omitempty"`

	// Whether to prioritize nodes with least or most allocatable resources.
	Mode ModeType `json:"mode,omitempty"`
}
```

默认值和校验分别在两个函数里：

1. 默认值在 v1 的 `SetDefaults_NodeResourcesAllocatableArgs`：`resources` 为空时用 `cpu: 1<<20, memory: 1`，`mode` 为空时用 `Least`；
2. 校验在 `ValidateNodeResourcesAllocatableArgs`：每个资源的权重必须大于 0（没有上限），`mode` 只能是 `Least` 或 `Most`。

`allocatable.go` 里还有一个同名的 `validateResources` 函数，是没有被调用的遗留代码，实际生效的是 `apis/config/validation` 中的版本。

默认权重的含义写在 `resource_allocation.go` 的注释里：CPU 以毫核计、内存以字节计，`1<<20` 让 1 毫核与 1 MiB 内存等价。换句话说，1 核约等于 1000 MiB，即约 1GiB 内存。

这是一个大致符合通用机型 CPU/内存配比（1:4 左右）量级的折算，但不意味着两者的贡献相等，下面的算例会看到内存往往占主导。

#### 在调度器配置里的写法

上面两个字段落到 `KubeSchedulerConfiguration` 里，是 `pluginConfig` 下的一段 `args`；插件本身还要在 `plugins.score.enabled` 里点名，框架才会调用它的工厂：

```yaml {linenos=inline}
# KubeSchedulerConfiguration：单独一个 Profile 启用 NodeResourcesAllocatable，参数写成默认值
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: allocatable-scheduler
  plugins:
    score:
      enabled:
      - name: NodeResourcesAllocatable
        weight: 1
  pluginConfig:
  - name: NodeResourcesAllocatable
    args:
      mode: Least
      resources:
      - name: cpu
        weight: 1048576
      - name: memory
        weight: 1
```

对照前面的结构体，这段配置分三块：

1. 第 7–10 行把插件挂到 Score 扩展点，`weight: 1` 是插件分数在加权求和时的系数，和第 17、19 行的资源权重是两回事；
2. 第 14 行对应 `Mode`，第 15–19 行对应 `Resources`，`1048576` 就是 `1<<20`，两者都与 `SetDefaults_NodeResourcesAllocatableArgs` 填的默认值相同；
3. 第 5 行的 `schedulerName` 决定哪些 Pod 走这个 Profile，Pod 的 `spec.schedulerName` 要写同一个名字。

写错时调度器直接起不来：`weight: 0` 不满足「大于 0」，`mode: least`（小写）也不在 `Least`、`Most` 之内，两者都会让 `ValidateNodeResourcesAllocatableArgs` 返回错误，`NewAllocatable` 失败。

## 三、源码

整条链路只有三块：工厂把参数变成闭包，`score` 算出原始分，`NormalizeScore` 把它压到 0~100。

### 3.1 工厂

工厂要做的事只有一件：把 Profile 里的 args 和默认值合并，装进 `resourceAllocationScorer`。具体分三步：

1. 默认值先在 `var mode config.ModeType` 和 `resToWeightMap := defaultResourcesToWeightMap`（第 5–6 行）里铺好；
2. `allocArgs != nil` 时才被参数覆盖；
3. 最后连同闭包一起装进结构体（第 29–37 行）。

```go {linenos=inline}
// pkg/noderesources/allocatable.go
func NewAllocatable(ctx context.Context, allocArgs runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
	logger := klog.FromContext(ctx).WithValues("plugin", AllocatableName)
	// Start with default values.
	var mode config.ModeType
	resToWeightMap := defaultResourcesToWeightMap

	// Update values from args, if specified.
	if allocArgs != nil {
		args, ok := allocArgs.(*config.NodeResourcesAllocatableArgs)
		if !ok {
			return nil, fmt.Errorf("want args to be of type NodeResourcesAllocatableArgs, got %T", allocArgs)
		}
		if args.Mode == "" {
			args.Mode = config.Least
		}
		if err := validation.ValidateNodeResourcesAllocatableArgs(args, nil); err != nil {
			return nil, err
		}
		if len(args.Resources) > 0 {
			resToWeightMap = make(resourceToWeightMap)
			for _, resource := range args.Resources {
				resToWeightMap[v1.ResourceName(resource.Name)] = resource.Weight
			}
		}
		mode = args.Mode
	}

	return &Allocatable{
		logger: logger,
		handle: h,
		resourceAllocationScorer: resourceAllocationScorer{
			Name:                AllocatableName,
			scorer:              resourceScorer(logger, resToWeightMap, mode),
			resourceToWeightMap: resToWeightMap,
		},
	}, nil
}
```

插件没有 CycleState、没有 PreScore，所有计算都在 Score 里按节点完成。

`resourceAllocationScorer` 这个结构来自 Kubernetes 1.19 的 in-tree `noderesources` 包（文件头注释写明是拷贝过来的），当年 in-tree 的 `NodeResourcesLeastAllocated`、`NodeResourcesMostAllocated` 还是独立插件，后来才合并进 `NodeResourcesFit` 的 `scoringStrategy`。

### 3.2 打分

打分分两层：

1. 外层的 `score` 负责取数（第 15–17 行）；
2. 真正加权的是构造时传进来的闭包（第 19 行）。

```go {linenos=inline}
// pkg/noderesources/resource_allocation.go
func (r *resourceAllocationScorer) score(
	logger klog.Logger,
	pod *v1.Pod,
	nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	node := nodeInfo.Node()
	if node == nil {
		return 0, fwk.NewStatus(fwk.Error, "node not found")
	}
	if r.resourceToWeightMap == nil {
		return 0, fwk.NewStatus(fwk.Error, "resources not found")
	}
	requested := make(resourceToValueMap, len(r.resourceToWeightMap))
	allocatable := make(resourceToValueMap, len(r.resourceToWeightMap))
	for resource := range r.resourceToWeightMap {
		allocatable[resource], requested[resource] = calculateResourceAllocatableRequest(logger, nodeInfo, pod, resource)
	}

	score := r.scorer(requested, allocatable)
	// ...
	return score, nil
}
```

`calculateResourceAllocatableRequest` 会算出节点的 allocatable 和「已请求 + 本 Pod 请求」两个值，请求量按资源类型分两种算法：

1. CPU/内存用 `GetNonZeroRequested`（未声明 request 的容器按 100m/200Mi 计）；
2. ephemeral-storage 和扩展资源用 `GetRequested`。

但真正打分的闭包根本不看 `requested`：

```go {linenos=inline}
// pkg/noderesources/allocatable.go
func resourceScorer(logger klog.Logger, resToWeightMap resourceToWeightMap, mode config.ModeType) func(resourceToValueMap, resourceToValueMap) int64 {
	return func(requested, allocable resourceToValueMap) int64 {
		// TODO: consider volumes in scoring.
		var nodeScore, weightSum int64
		for resource, weight := range resToWeightMap {
			resourceScore := score(logger, allocable[resource], mode)
			nodeScore += resourceScore * weight
			weightSum += weight
		}
		return nodeScore / weightSum
	}
}

func score(logger klog.Logger, capacity int64, mode config.ModeType) int64 {
	switch mode {
	case config.Least:
		return -1 * capacity
	case config.Most:
		return capacity
	}

	logger.V(10).Info("No match for mode", "mode", mode)
	return 0
}
```

所以原始分是 `±Σ(allocatable_i × w_i) / Σw_i`，分两步得到：

1. 按权重累加各资源的 allocatable（第 7–11 行）；
2. 定符号，`Least` 为负、`Most` 为正（第 15–20 行）。

**它只跟节点自己的 allocatable 有关，与节点上已经运行了什么、与本 Pod 请求多少都无关**，`requested` 的计算是从 in-tree 结构继承来的、在这里没有用处的开销。

测试用例 `resources requested with more than the node` 也印证了这一点：Pod 请求超过节点容量，分数照样按规格给。插件不管能不能放下，那是 Filter 阶段 `NodeResourcesFit` 的事。

权重表里出现的资源，如果节点没有（例如在没有 GPU 的节点上配置了 `nvidia.com/gpu`），allocatable 取 0，不会报错。

### 3.3 NormalizeScore

原始分的量纲是字节当量，必须先映射到框架约定的 0~100，框架才会拿去和其他插件加权。这一步每个插件每轮只做一次，位于所有节点的 `Score` 返回之后、加权汇总之前（`pkg/scheduler/framework/runtime/framework.go` 的 `RunScorePlugins`）：

```go {linenos=inline}
// pkg/noderesources/allocatable.go
func (alloc *Allocatable) NormalizeScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	// Find highest and lowest scores.
	var highest int64 = -math.MaxInt64
	var lowest int64 = math.MaxInt64
	for _, nodeScore := range scores {
		if nodeScore.Score > highest {
			highest = nodeScore.Score
		}
		if nodeScore.Score < lowest {
			lowest = nodeScore.Score
		}
	}

	// Transform the highest to lowest score range to fit the framework's min to max node score range.
	oldRange := highest - lowest
	newRange := fwk.MaxNodeScore - fwk.MinNodeScore
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = fwk.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - lowest) * newRange / oldRange) + fwk.MinNodeScore
		}
	}

	return nil
}
```

归一化有两种情况：

1. 标准的 min-max 线性映射（第 22 行）：本轮可行节点中原始分最高的得 100，最低的得 0；
2. 所有节点规格相同时 `oldRange == 0`（第 19–20 行），全部 0 分，插件对同构集群不起作用，这是合理的。

## 四、算例

{{< figure src="/images/posts/sched-plugins/33-allocatable-score.svg" caption="四种规格的空节点在 Least、Most 两种模式下的得分，与 in-tree NodeResourcesFit 默认 LeastAllocated 的对照（点击查看大图）" link="/images/posts/sched-plugins/33-allocatable-score.svg" >}}

四个节点，默认权重 `cpu: 1048576, memory: 1`，权重和 `1048577`：

| 节点 | allocatable | CPU 项 | 内存项 | 原始分绝对值 |
| --- | --- | --- | --- | --- |
| small | 4C / 16Gi | 4000 × 1048576 | 17179869184 | 20383 |
| medium | 16C / 64Gi | 16000 × 1048576 | 68719476736 | 81535 |
| memheavy | 8C / 128Gi | 8000 × 1048576 | 137438953472 | 139071 |
| large | 64C / 256Gi | 64000 × 1048576 | 274877906944 | 326143 |

以 small 为例，计算分两步：

1. `(4000 × 1048576 + 16 × 2^30) / 1048577 = 21374173184 / 1048577 ≈ 20383.98`；
2. Go 整数除法向零截断，`Least` 模式下是 −20383。

原始分近似等于「毫核数 + 内存 MiB 数」，所以叫它 MiB 当量更直观：small 是 4000 + 16384，内存贡献了八成。

`Least` 模式归一化：最高 −20383，最低 −326143，区间 305760。

1. small：`(−20383 + 326143) × 100 / 305760 = 100`
2. medium：`244608 × 100 / 305760 = 80`
3. memheavy：`187072 × 100 / 305760 = 61`
4. large：0

`Most` 模式正好镜像：small 0、medium 20、memheavy 38、large 100。

memheavy 只有 8 核，却排在 16 核的 medium 后面（Least 模式下 61 < 80），因为它的 128Gi 内存折合成了 13 万多的当量。如果集群里 CPU 才是稀缺资源，应该调大 CPU 权重。README 和 `manifests/noderesources/scheduler-config.yaml` 示例用的是 `cpu: 1000000`，与默认值同量级。

到这里，插件自身的行为已经完整：它只看节点规格。但 scheduler-plugins 的调度器默认还带着 in-tree 的 `NodeResourcesFit`，两者会一起参与打分，真正的区别要在组合里才看得出来。

## 五、与 NodeResourcesFit 打分策略的区别

差别集中在三处：打分语义、组合时的方向、以及归一化带来的副作用。

### 5.1 语义不同

in-tree [NodeResourcesFit](/posts/sched-plugin-08-node-resources-fit/) 的打分策略（`LeastAllocated` 默认、`MostAllocated`、`RequestedToCapacityRatio`）都基于「本 Pod 放上去之后的已请求量 / allocatable」这个比例：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/noderesources/least_allocated.go
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

它回答的是「这个节点还有多空」（`(capacity - requested) * fwk.MaxNodeScore / capacity`，第 10 行），`NodeResourcesAllocatable` 回答的是「这个节点有多大」。两者正交：

| | NodeResourcesFit 打分 | NodeResourcesAllocatable |
| --- | --- | --- |
| 输入 | 已请求 + 本 Pod 请求、allocatable | 只有 allocatable |
| 随调度推进变化 | 是，每放一个 Pod 都变 | 否，节点规格不变就不变 |
| 单资源分数 | 先在每种资源上算出 0~100 的比例分，再加权平均 | 直接对原始量加权平均，量纲由权重换算 |
| 归一化 | 不需要，已在 0~100 | min-max，依赖本轮候选节点集合 |
| 默认是否启用 | 是，权重 1 | 否 |

`Most` 模式和 Fit 的 `MostAllocated` 名字相近，含义完全不同：前者偏好大节点，后者偏好已经比较满的节点（装箱）。一个刚加入集群的空的大节点，在前者里得高分，在后者里得低分。

### 5.2 组合使用时方向会打架

scheduler-plugins 的调度器二进制里，Profile 依然包含 in-tree 的默认插件，原因有两层：

1. `cmd/scheduler/main.go` 调用的是 in-tree 的 `app.NewSchedulerCommand`，`app.WithPlugin` 只往 registry 里注册插件工厂，不改动任何 Profile；
2. 加载配置时，`setDefaults_KubeSchedulerProfile`（`pkg/scheduler/apis/config/v1/defaults.go`）用 `mergePlugins(logger, getDefaultPlugins(), prof.Plugins)` 把配置里写的插件合并进默认列表：`enabled` 里的插件追加进去（与默认插件同名时就地替换），只有写进 `disabled` 的默认插件才会被去掉。

[开篇](/posts/sched-plugin-00-journey/) 1.1 节列过这份默认列表，`NodeResourcesFit` 在其中以权重 1 参与打分，策略默认 `LeastAllocated`。图中最右一列是一个 1 CPU / 2Gi 的 Pod 在四个空节点上的 Fit 得分：small 81、medium 94、memheavy 92、large 98，Fit 偏好大节点；`Least` 模式的 Allocatable 偏好小节点，两者方向相反。

好在两者的区分度不一样。Fit 在空节点之间的差异只有十几分（81 到 98），Allocatable 经过 min-max 后总是拉满 0 到 100。权重都为 1 时 small 得 `81 + 100 = 181`，large 得 `98 + 0 = 98`，Allocatable 胜出。

但每在小节点上放一个 Pod，Fit 给它的分数就快速下降，两者的合力很快会翻转，Pod 开始流向其他节点。KEP 想要的效果是「先用小节点，把大节点留给大 Pod」，下面的例子会看到，默认权重下实际达到的是后半句：large 一直被留着，但 small 并不会被单独填满。

#### 举个例子：连续调度五个 1 CPU / 2Gi 的 Pod

沿用第四节的四个空节点，连续来五个 1 CPU / 2Gi 的 Pod。两个插件权重都为 1，Allocatable 用 `Least` 模式，其余默认打分插件不计入。

五轮里四个节点都放得下，候选集合不变，Allocatable 的分数固定为 small 100、medium 80、memheavy 61、large 0。变的只有 Fit：`leastRequestedScore` 对 CPU、内存各算一次 `(capacity - requested) * fwk.MaxNodeScore / capacity`，`requested` 包含节点上已有的 Pod 和本 Pod，两项再按 1:1 权重做整数平均。下表每格是「Fit + Allocatable = 合计」：

| Pod | small | medium | memheavy | large | 落点 |
| --- | --- | --- | --- | --- | --- |
| 第 1 个 | 81 + 100 = 181 | 94 + 80 = 174 | 92 + 61 = 153 | 98 + 0 = 98 | small |
| 第 2 个 | 62 + 100 = 162 | 94 + 80 = 174 | 92 + 61 = 153 | 98 + 0 = 98 | medium |
| 第 3 个 | 62 + 100 = 162 | 90 + 80 = 170 | 92 + 61 = 153 | 98 + 0 = 98 | medium |
| 第 4 个 | 62 + 100 = 162 | 85 + 80 = 165 | 92 + 61 = 153 | 98 + 0 = 98 | medium |
| 第 5 个 | 62 + 100 = 162 | 81 + 80 = 161 | 92 + 61 = 153 | 98 + 0 = 98 | small |

逐轮看：

1. 第 1 个 Pod：各节点都空，Fit 分与图中最右一列相同，small 以 181 领先。
2. 第 2 个 Pod：small 上已有 1 核 / 2Gi，加上本 Pod 是 2 核 / 4Gi。CPU 项 `(4000 − 2000) × 100 / 4000 = 50`，内存项 `(16 − 4) × 100 / 16 = 75`，Fit 为 `(50 + 75) / 2 = 62`，合计 162，低于 medium 的 174，Pod 落到 medium。
3. 第 3、4 个 Pod：medium 的 Fit 依次降到 90、85，合计 170、165，仍高于 small 的 162。
4. 第 5 个 Pod：medium 上已有 3 个 Pod，加上本 Pod 是 4 核 / 8Gi，CPU 项 75、内存项 87，Fit 为 81，合计 161，small 以 162 重新领先。

翻转比直觉来得早：small 和 medium 在 Allocatable 上只差 20 分，small 上放一个 Pod 就让它的 Fit 掉 19 分（81 → 62），第 2 个 Pod 就转向了 medium，此后两者交替接收 Pod。large 的合计一直停在 98，远低于另外三个节点，留给了后来的大 Pod。所以在默认权重下，「先填小的」实际是「先填 large 以外的节点」，small 并不会被单独填满。

若希望 Allocatable 的语义更占主导，可以给它更高的 `weight`，或者把 Fit 的打分策略改成 `MostAllocated`。后者在同规格节点之间也做装箱，两者的方向就一致了。

#### 两种调整在配置里的位置

两种做法都只改 Profile，可以单独用，也可以一起用：

```yaml {linenos=inline}
# KubeSchedulerConfiguration（节选）：提高 Allocatable 的权重，并把 Fit 改成 MostAllocated
profiles:
- schedulerName: allocatable-scheduler
  plugins:
    score:
      enabled:
      - name: NodeResourcesAllocatable
        weight: 2
  pluginConfig:
  - name: NodeResourcesFit
    args:
      scoringStrategy:
        type: MostAllocated
        resources:
        - name: cpu
          weight: 1
        - name: memory
          weight: 1
```

两处改动分别对应上一段的两种做法：

1. 第 8 行的 `weight: 2` 让 Allocatable 的分数在合计里翻倍。代回上面的例子：第 2 个 Pod 时 small 合计 `62 + 200 = 262`，仍高于 medium 的 `94 + 160 = 254`，留在 small；第 3 个 Pod 时 small 上是 3 核 / 6Gi，CPU 项 25、内存项 `(16 − 6) × 100 / 16` 截断为 62，Fit 为 43，合计 243，低于 medium 的 254，才转向 medium。翻转推迟了一个 Pod。
2. 第 10–18 行改的是 in-tree `NodeResourcesFit` 的参数，`NodeResourcesFit` 本来就在默认插件里，这里只替换它的 `args`。`MostAllocated` 按 `requested * MaxNodeScore / capacity` 打分，第 1 个 Pod 在 small 上的 Fit 分是 `(25 + 12) / 2 = 18`，在 large 上是 `(1 + 0) / 2 = 0`，和 Allocatable 一样偏向 small，而且 small 越满分越高。

仓库自带的 `manifests/noderesources/scheduler-config.yaml` 走的是第三条路：在 `score` 下写 `disabled: - name: "*"`。`expandMultiPointPlugins`（`pkg/scheduler/framework/runtime/framework.go`）看到某个扩展点禁用了 `*`，就不再把默认插件展开到这个扩展点，于是 Score 阶段只剩 Allocatable。**代价是 `TaintToleration`、`NodeAffinity`、`PodTopologySpread`、`InterPodAffinity` 的打分也一起关掉了**，Pod 里写的 `preferredDuringSchedulingIgnoredDuringExecution` 和 `ScheduleAnyway` 拓扑分布不再起作用；Filter 阶段不受影响。

### 5.3 min-max 归一化的副作用

min-max 依赖本轮的候选集合。同一个 medium 节点，候选集合里有 large 时得 80，large 被 Filter 过滤掉后，medium 变成最大，`Least` 模式下得 0。**分数不是节点的固有属性，跨 Pod 比较分数没有意义。**

另外 `percentageOfNodesToScore` 小于 100% 时，候选节点只是全部可行节点的一个子集，极值随采样变化，对大集群的打分会有一些抖动。

子集来自 Filter 阶段的提前停止（[开篇](/posts/sched-plugin-00-journey/) 3.2 节）：`numFeasibleNodesToFind` 算出要找几个可行节点，找够就停，下一个 Pod 从 `nextStartNodeIndex` 接着往后找。节点数不到 100 时会检查全部节点，这种抖动只出现在更大的集群里。

## 六、边界与坑

打分逻辑本身没有分支，容易踩的都在量纲和边界上。

1. 不看是否放得下：插件对放不下的节点也照常打分，依赖 Filter 先把它们排除。
2. 权重无上限，可能溢出：`allocatable × weight` 是 int64 乘法。内存按字节计，1TiB 约 1.1e12，再乘一个 1e7 的权重就超过了 int64 上限（约 9.2e18），**分数会回绕成负数**。CPU 权重设到百万量级没问题，内存权重保持 1 即可。
3. 扩展资源按原始数量参与：GPU 数量是个位数，不配大权重几乎不起作用；配了大权重又可能压过 CPU/内存。混用时最好单独算一下各项贡献。
4. `requested` 的计算被浪费：每个节点每次打分都会遍历 Pod 的容器算请求量，结果不用。开销不大，但如果在超大集群上做性能剖析时看到它，不必奇怪。

## 七、发散：按规格打分的适用边界

这一节换到集群整体来看：这种按规格打的分适合什么样的节点拓扑，代价又在哪里。

它是一个静态偏好。节点规格在节点生命周期内几乎不变，这个插件的分数可以看成给每个节点贴了一个固定的「优先级」，只是由 allocatable 自动算出。同样的效果可以用 [NodeMetadata](/posts/sched-plugin-34-node-metadata/) 读一个人工维护的数字 label 实现，或者用 NodeAffinity 的 `preferredDuringSchedulingIgnoredDuringExecution` 加权，区别在于这里不需要人工维护标签，新机型上线自动生效。

和集群自动伸缩的关系。`Most` 模式适合「常驻大节点 + 弹性小节点」的拓扑，让 Pod 优先挤在大节点上，Cluster Autoscaler 才能找到空闲的小节点缩容。反过来，如果弹性节点是大规格（比如 GPU 机型），`Least` 模式能避免普通 Pod 占用它们，配合 taint 效果更好。无论哪种模式，它都只影响新调度的 Pod，**已经运行的 Pod 不会因为打分变化而迁移**，需要 descheduler 之类的组件配合。

碎片问题的另一面。按规格打分减少的是「大 Pod 找不到大节点」这一种碎片，代价是小节点被优先填满，小节点上的资源余量变少，突发扩容时更容易需要新节点。这和 in-tree 的 `MostAllocated` 装箱面临同样的取舍：利用率和弹性余量此消彼长。

## 小结

1. `NodeResourcesAllocatable` 只实现 Score，原始分为 `±Σ(allocatable × 权重)/Σ权重`，与节点上已运行的 Pod 和本 Pod 的请求都无关，再用 min-max 映射到 `[0, 100]`。
2. 默认 `mode: Least`、权重 `cpu: 1<<20, memory: 1`，1 毫核约等于 1 MiB，内存通常占主导，CPU 稀缺的集群应调大 CPU 权重。
3. 它与 `NodeResourcesFit` 的比例型打分正交，默认组合下两者方向相反；Allocatable 的区分度更大，会在小节点填满之前占上风。
4. 分数依赖本轮候选集合，权重无上限可能导致 int64 溢出。

下一篇 [NodeMetadata](/posts/sched-plugin-34-node-metadata/) 把这种「静态偏好」推到更一般的形式：任意一个节点 label 或 annotation 都可以变成打分依据。
