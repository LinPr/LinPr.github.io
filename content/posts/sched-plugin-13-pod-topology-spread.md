---
title: 'scheduler-plugins 13 [in-tree] PodTopologySpread：按拓扑域数数的打散器'
date: 2026-11-19T10:10:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "PodTopologySpread", "拓扑分布", "调度插件"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "PodTopologySpread 按 zone、节点等拓扑域打散副本。本文从源码拆解 PreFilter 计数表与关键路径、skew 判定、minDomains 与节点包含策略、matchLabelKeys、默认约束、Score 算例和 AddPod/RemovePod。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `PodTopologySpread` 实现的扩展点（PreFilter、Filter、PreScore、Score、NormalizeScore）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/13.svg" link="/images/posts/sched-plugins/callstack/13.svg" caption="PodTopologySpread 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-12-volume-zone/)的 `VolumeZone` 只关心「Pod 用到的卷在哪个 zone」，它看的是存储对象。从这一篇开始，Filter 链进入另一类插件：判断一个节点是否可行，不仅取决于节点本身，还取决于**集群里其它 Pod 分布在哪里**。`PodTopologySpread` 是这类插件的第一个，紧接着的 `InterPodAffinity` 是第二个。

本文基于 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期）。`PodTopologySpread` 默认启用，在默认 Profile 的 MultiPoint 列表里排在 `VolumeZone` 之后、`InterPodAffinity` 之前，打分权重为 2。它实现了 PreFilter（带 `PreFilterExtensions`）、Filter、PreScore、Score（带 `NormalizeScore`）、`EnqueueExtensions` 和 `SignPlugin`。相关的 feature gate 有三个：

| gate | 状态（1.37） | 作用 |
| --- | --- | --- |
| `NodeInclusionPolicyInPodTopologySpread` | GA，锁定开启 | 让 `nodeAffinityPolicy`、`nodeTaintsPolicy` 字段生效 |
| `MatchLabelKeysInPodTopologySpread` | Beta，默认开启 | 调度器把 `matchLabelKeys` 合并进 selector |
| `MatchLabelKeysInPodTopologySpreadSelectorMerge` | Beta，默认开启 | apiserver 在创建 Pod 时就把 `matchLabelKeys` 写进 `labelSelector` |

## 一、它解决什么问题

副本数一多，「别把鸡蛋放在一个篮子里」就成了硬需求。下面这个 Deployment 要求副本在 zone 之间严格均衡，在节点之间尽量均衡：

```yaml {linenos=inline}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 6
  selector:
    matchLabels: {app: web}
  template:
    metadata:
      labels: {app: web}
    spec:
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule      # 硬约束：进 Filter
        labelSelector:
          matchLabels: {app: web}
        matchLabelKeys: ["pod-template-hash"] # 只和同一版本的副本比较
        minDomains: 3
      - maxSkew: 2
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: ScheduleAnyway     # 软约束：进 Score
        labelSelector:
          matchLabels: {app: web}
      containers:
      - name: web
        image: nginx
```

两条约束分别走两条路：

1. `whenUnsatisfiable: DoNotSchedule` 的约束只在 PreFilter/Filter 中使用，节点不满足就被过滤掉；
2. `ScheduleAnyway` 的约束只在 PreScore/Score 中使用，不满足也能调度，只是分数低。

插件内部用同一个函数 `filterTopologySpreadConstraints` 按 `whenUnsatisfiable` 把约束分拣成两组，两组之间没有任何交叉。

`PodTopologySpread` 的核心概念是 skew：某个拓扑域（一个 zone、一个节点……）里匹配 selector 的 Pod 数，减去所有合格域中的最小值。Filter 要求「把新 Pod 放进去之后」的 skew 不超过 `maxSkew`。

整篇文章基本就是在回答三个问题：

1. 哪些 Pod 被计数；
2. 哪些域算「合格域」；
3. 最小值怎么取。

要回答它们，得先看一条约束进入插件之后被解析成什么。

## 二、约束的内部表示与解析

### 2.1 `topologySpreadConstraint`

约束进入插件后先做一次解析。API 里的 `v1.TopologySpreadConstraint` 不直接参与计算，插件把它转换成内部结构（第 2–9 行），顺带把 `labelSelector` 解析成 `labels.Selector`，给可选字段填上默认值：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/common.go
type topologySpreadConstraint struct {
	MaxSkew            int32
	TopologyKey        string
	Selector           labels.Selector
	MinDomains         int32
	NodeAffinityPolicy v1.NodeInclusionPolicy
	NodeTaintsPolicy   v1.NodeInclusionPolicy
}
```

转换逻辑在 `filterTopologySpreadConstraints` 中，第 6 行把 `labelSelector` 解析成 `labels.Selector`：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/common.go
func (pl *PodTopologySpread) filterTopologySpreadConstraints(constraints []v1.TopologySpreadConstraint, podLabels map[string]string, action v1.UnsatisfiableConstraintAction) ([]topologySpreadConstraint, error) {
	var result []topologySpreadConstraint
	for _, c := range constraints {
		if c.WhenUnsatisfiable == action {
			selector, err := metav1.LabelSelectorAsSelector(c.LabelSelector)
			if err != nil {
				return nil, err
			}

			if pl.enableMatchLabelKeysInPodTopologySpread && len(c.MatchLabelKeys) > 0 {
				matchLabels := make(labels.Set)
				for _, labelKey := range c.MatchLabelKeys {
					if value, ok := podLabels[labelKey]; ok {
						matchLabels[labelKey] = value
					}
				}
				if len(matchLabels) > 0 {
					selector = mergeLabelSetWithSelector(matchLabels, selector)
				}
			}

			tsc := topologySpreadConstraint{
				MaxSkew:            c.MaxSkew,
				TopologyKey:        c.TopologyKey,
				Selector:           selector,
				MinDomains:         ptr.Deref(c.MinDomains, 1),   // If MinDomains is nil, we treat MinDomains as 1.
				NodeAffinityPolicy: v1.NodeInclusionPolicyHonor,  // If NodeAffinityPolicy is nil, we treat NodeAffinityPolicy as "Honor".
				NodeTaintsPolicy:   v1.NodeInclusionPolicyIgnore, // If NodeTaintsPolicy is nil, we treat NodeTaintsPolicy as "Ignore".
			}
			if pl.enableNodeInclusionPolicyInPodTopologySpread {
				// ...
			}
			result = append(result, tsc)
		}
	}
	return result, nil
}
```

三个默认值决定了统计口径：

1. `minDomains` 缺省按 1 处理（第 27 行）。1 的效果等于「不限制」，但它顺带处理了一个边界：一个合格域都没有时，最小值会被当作 0（见第四节）。API 校验要求 `minDomains` 只能和 `DoNotSchedule` 搭配。
2. `nodeAffinityPolicy` 缺省是 `Honor`（第 28 行）：Pod 自己的 nodeSelector/required nodeAffinity 选不中的节点，不参与统计。
3. `nodeTaintsPolicy` 缺省是 `Ignore`（第 29 行）：节点有污点照样统计。**两个默认值的方向正好相反。** 这是为了兼容这两个字段出现之前的行为——老版本只按 nodeAffinity 过滤节点，从不看污点。

### 2.2 `matchLabelKeys`：在调度器和 apiserver 两边各合并一次

`matchLabelKeys` 解决的是滚动更新时的计数污染。Deployment 滚动更新期间，新旧两个 ReplicaSet 的 Pod 都带 `app: web`，如果 selector 只写 `app: web`，旧版本副本也会被计数，新版本副本的分布会被旧副本「带偏」，等旧副本缩容后分布就不均了。把 `pod-template-hash` 写进 `matchLabelKeys`，插件会读出新 Pod 自己这个标签的值，合并成 `pod-template-hash=<hash>` 的等值条件加到 selector 上，计数范围就缩小到同一版本。

合并发生在两个地方：

1. 调度器侧是上面的 `mergeLabelSetWithSelector`（第 18–19 行）；
2. apiserver 侧，在 `MatchLabelKeysInPodTopologySpreadSelectorMerge` 开启时，Pod 创建时就把它写进 `labelSelector`：第 3 行判断两个 gate，第 7–9 行逐条约束合并。

```go {linenos=inline}
// pkg/registry/core/pod/strategy.go
func mutateTopologySpreadConstraints(pod *api.Pod) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodTopologySpread) || !utilfeature.DefaultFeatureGate.Enabled(features.MatchLabelKeysInPodTopologySpreadSelectorMerge) || pod.Spec.TopologySpreadConstraints == nil {
		return
	}
	topologySpreadConstraints := pod.Spec.TopologySpreadConstraints
	for i := range topologySpreadConstraints {
		applyMatchLabelKeys(&topologySpreadConstraints[i], pod.Labels)
	}
}
```

两边都合并，selector 里会出现一条重复的等值条件，但在「与」运算下没有副作用。apiserver 侧合并的意义在于让 `labelSelector` 自身完整：**其它读 Pod spec 的组件不需要理解 `matchLabelKeys`，也能得到正确的 selector**。这类组件包括 descheduler、cluster-autoscaler 的模拟器。

### 2.3 默认约束：System 与 List

Pod 没写 `topologySpreadConstraints` 时，`getConstraints` 走另一条分支：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/plugin.go
func (pl *PodTopologySpread) getConstraints(pod *v1.Pod) ([]topologySpreadConstraint, error) {
	var constraints []topologySpreadConstraint
	var err error
	if len(pod.Spec.TopologySpreadConstraints) > 0 {
		// We have feature gating in APIServer to strip the spec
		// so don't need to re-check feature gate, just check length of Constraints.
		constraints, err = pl.filterTopologySpreadConstraints(
			pod.Spec.TopologySpreadConstraints,
			pod.Labels,
			v1.DoNotSchedule,
		)
		// ...
	} else {
		constraints, err = pl.buildDefaultConstraints(pod, v1.DoNotSchedule)
		// ...
	}
	return constraints, nil
}
```

**显式约束和默认约束是二选一的**：只要 Pod 写了一条约束，默认约束就完全不生效。对应两个分支：

1. 第 5–13 行，Pod 写了约束，走 `filterTopologySpreadConstraints`；
2. 第 14–17 行，Pod 没写约束，走 `buildDefaultConstraints`。

默认约束来自插件参数 `PodTopologySpreadArgs`：

```go {linenos=inline}
// pkg/scheduler/apis/config/types_pluginargs.go
type PodTopologySpreadArgs struct {
	metav1.TypeMeta

	// DefaultConstraints defines topology spread constraints to be applied to
	// Pods that don't define any in `pod.spec.topologySpreadConstraints`.
	// `.defaultConstraints[*].labelSelectors` must be empty, as they are
	// deduced from the Pod's membership to Services, ReplicationControllers,
	// ReplicaSets or StatefulSets.
	// When not empty, .defaultingType must be "List".
	DefaultConstraints []v1.TopologySpreadConstraint

	// DefaultingType determines how .defaultConstraints are deduced. Can be one
	// of "System" or "List".
	// ...
	DefaultingType PodTopologySpreadConstraintsDefaulting
}
```

`DefaultingType` 的默认值是 `System`（`SetDefaults_PodTopologySpreadArgs`）。校验函数 `validateDefaultingType` 按模式区分：

1. `System` 模式下 `DefaultConstraints` 必须为空；
2. `List` 模式下用户自己给列表，每条约束不能带 `labelSelector`，`maxSkew` 必须大于 0，同一 `topologyKey`+`whenUnsatisfiable` 组合不能重复。

`New` 中 `System` 模式会换成写死的系统约束：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/plugin.go
var systemDefaultConstraints = []v1.TopologySpreadConstraint{
	{
		TopologyKey:       v1.LabelHostname,
		WhenUnsatisfiable: v1.ScheduleAnyway,
		MaxSkew:           3,
	},
	{
		TopologyKey:       v1.LabelTopologyZone,
		WhenUnsatisfiable: v1.ScheduleAnyway,
		MaxSkew:           5,
	},
}
```

两条都是 `ScheduleAnyway`（第 5、10 行），所以系统默认约束**只影响打分，永远不会让 Pod 调度失败**。

selector 不在配置里，而是由 `buildDefaultConstraints` 现场推导，见下面的代码：

1. 第 7 行推导 selector；
2. 推导结果为空时，第 8–10 行直接放弃。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/common.go
func (pl *PodTopologySpread) buildDefaultConstraints(p *v1.Pod, action v1.UnsatisfiableConstraintAction) ([]topologySpreadConstraint, error) {
	constraints, err := pl.filterTopologySpreadConstraints(pl.defaultConstraints, p.Labels, action)
	if err != nil || len(constraints) == 0 {
		return nil, err
	}
	selector := helper.DefaultSelector(p, pl.services, pl.replicationCtrls, pl.replicaSets, pl.statefulSets)
	if selector.Empty() {
		return nil, nil
	}
	for i := range constraints {
		constraints[i].Selector = selector
	}
	return constraints, nil
}
```

`helper.DefaultSelector` 把选中这个 Pod 的所有 Service 的 selector 合并，再加上控制器（RC、ReplicaSet、StatefulSet）的 selector。推导结果为空——比如一个不属于任何 Service、也没有控制器的裸 Pod——默认约束就不生效。

这也是 `New` 在默认约束非空时必须拿到 `SharedInformerFactory` 并建立四个 lister 的原因。注意这里只认 RC、ReplicaSet、StatefulSet 三种 owner，DaemonSet、Job 创建的 Pod 只能靠 Service 推导出 selector。

约束的形态到这里就固定了：已经按 `whenUnsatisfiable` 分好组，selector 已经解析，默认值也填好了。下一节开始用它们去数 Pod。

## 三、PreFilter：一次遍历，按约束计数

### 3.1 `preFilterState`

PreFilter 要做的事只有一件：把集群现有的 Pod 按拓扑域数清楚，供 Filter 反复查表。数出来的结果写进 CycleState 的 `preFilterState`：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/filtering.go
type preFilterState struct {
	Constraints []topologySpreadConstraint
	// CriticalPaths is a slice indexed by constraint index.
	// Per each entry, we record 2 critical paths instead of all critical paths.
	// CriticalPaths[i][0].MatchNum always holds the minimum matching number.
	// CriticalPaths[i][1].MatchNum is always greater or equal to CriticalPaths[i][0].MatchNum, but
	// it's not guaranteed to be the 2nd minimum match number.
	CriticalPaths []*criticalPaths
	// TpValueToMatchNum is a slice indexed by constraint index.
	// Each entry is keyed with topology value, and valued with the number of matching pods.
	TpValueToMatchNum []map[string]int
}
```

`TpValueToMatchNum[i]` 是第 i 条约束的「拓扑值 → 匹配 Pod 数」表。较早的实现用一张以 `topologyPair{key, value}` 为键的大表（字段名 `TpPairToMatchNum`）把所有约束混在一起，现在按约束下标拆成切片：每条约束一张小 map，键只需要拓扑值。两条约束用同一个 `topologyKey` 但 selector 不同的情况，也就不会互相串号。

`CriticalPaths[i]`（第 9 行）只记两条「关键路径」，即匹配数最小的两个域。**Filter 要的只是最小值，没必要每次扫全部域。** 之所以多记一条，是为了 `AddPod`/`RemovePod` 增量更新时最小值所在的域被加一后，仍能找到新的最小值。

### 3.2 `calPreFilterState`

`calPreFilterState` 是 PreFilter 的主体，分三步：

1. 并行扫一遍节点，把每个节点上按约束分好的计数收进 `tpCountsByNode`（第 4–34 行）；
2. 串行合并（第 36–41 行）；
3. 求出每条约束的两个最小域（第 44–50 行）。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/filtering.go
	tpCountsByNode := make([][]topologyCount, len(allNodes))
	requiredNodeAffinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	processNode := func(n int) {
		nodeInfo := allNodes[n]
		node := nodeInfo.Node()

		if !pl.enableNodeInclusionPolicyInPodTopologySpread {
			// ...
		}

		// Ensure current node's labels contains all topologyKeys in 'Constraints'.
		if !nodeLabelsMatchSpreadConstraints(node.Labels, constraints) {
			return
		}

		tpCounts := make([]topologyCount, 0, len(constraints))
		for i, c := range constraints {
			if pl.enableNodeInclusionPolicyInPodTopologySpread &&
				!c.matchNodeInclusionPolicies(logger, pod, node, requiredNodeAffinity, pl.enableTaintTolerationComparisonOperators) {
				continue
			}

			value := node.Labels[c.TopologyKey]
			count := countPodsMatchSelector(nodeInfo.GetPods(), c.Selector, pod.Namespace)
			tpCounts = append(tpCounts, topologyCount{
				topologyValue: value,
				constraintID:  i,
				count:         count,
			})
		}
		tpCountsByNode[n] = tpCounts
	}
	pl.parallelizer.Until(ctx, len(allNodes), processNode, pl.Name())

	for _, tpCounts := range tpCountsByNode {
		// tpCounts might not hold all the constraints, so index can't be used here as constraintID.
		for _, tpCount := range tpCounts {
			s.TpValueToMatchNum[tpCount.constraintID][tpCount.topologyValue] += tpCount.count
		}
	}

	// calculate min match for each constraint and topology value
	for i := 0; i < len(constraints); i++ {
		s.CriticalPaths[i] = newCriticalPaths()

		for value, num := range s.TpValueToMatchNum[i] {
			s.CriticalPaths[i].update(value, num)
		}
	}
```

这段代码决定了统计口径，有四个细节：

1. **遍历的是全部节点**。`allNodes` 是 `RunPreFilterPlugins` 从快照里取出的完整节点列表，不是「通过了前面 Filter 的节点」。PreFilter 在所有 Filter 之前运行，此时还不知道哪些节点会被 `TaintToleration`、`NodeResourcesFit` 等插件淘汰。
2. 节点必须带齐所有约束的 `topologyKey`。缺任何一个标签，这个节点对所有约束都不计数。
3. **计数为 0 也会产生一个域**。只要节点通过了标签检查和包含策略，`tpCounts` 就会追加一条记录，哪怕 `count` 为 0。一个没有任何匹配 Pod 的 zone 也是「合格域」，它会把最小值拉到 0。
4. 只数同 namespace、未在删除中的 Pod（第 9–12 行）：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/common.go
func countPodsMatchSelector(podInfos []fwk.PodInfo, selector labels.Selector, ns string) int {
	if labels.MatchesNothing(selector) {
		return 0
	}
	count := 0
	for _, p := range podInfos {
		// Bypass terminating Pod (see #87621).
		if p.GetPod().DeletionTimestamp != nil || p.GetPod().Namespace != ns {
			continue
		}
		if selector.Matches(labels.Set(p.GetPod().Labels)) {
			count++
		}
	}
	return count
}
```

并行部分靠下标分区免掉加锁：`processNode` 只写 `tpCountsByNode[n]` 这一个槽位（第 32 行），`pl.parallelizer.Until` 按下标把节点分给多个 goroutine（第 34 行），合并留给之后的单线程循环（第 36–41 行）。

PreFilter 最后的分支决定是否跳过整个 Filter：

1. 没有任何 `DoNotSchedule` 约束（显式或默认）时返回 `Skip`，Filter 整个被跳过；
2. 否则写入状态。

默认 System 模式下，没有显式约束的 Pod 在 PreFilter 阶段就会 `Skip`，因为两条系统约束都是 `ScheduleAnyway`。

### 3.3 `criticalPaths` 的增量更新

`criticalPaths` 只有两个槽位。新增一个域、或某个已经登记过的域计数变了，`update` 要在一次比较里决定这两个槽位要不要换人，分两种情况：

1. 第 11–18 行处理已登记的域；
2. 第 19–29 行插入新域。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/filtering.go
func (p *criticalPaths) update(tpVal string, num int) {
	// first verify if `tpVal` exists or not
	i := -1
	if tpVal == p[0].TopologyValue {
		i = 0
	} else if tpVal == p[1].TopologyValue {
		i = 1
	}

	if i >= 0 {
		// `tpVal` exists
		p[i].MatchNum = num
		if p[0].MatchNum > p[1].MatchNum {
			// swap paths[0] and paths[1]
			p[0], p[1] = p[1], p[0]
		}
	} else {
		// `tpVal` doesn't exist
		if num < p[0].MatchNum {
			// update paths[1] with paths[0]
			p[1] = p[0]
			// update paths[0]
			p[0].TopologyValue, p[0].MatchNum = tpVal, num
		} else if num < p[1].MatchNum {
			// update paths[1]
			p[1].TopologyValue, p[1].MatchNum = tpVal, num
		}
	}
}
```

只记两条路径能成立，依赖代码注释里写明的两个前提：

1. 抢占只驱逐同一个节点上的 Pod，每次只有一个域的计数在变；
2. 每个候选节点的抢占模拟都在独立的 state 副本上进行。

在这两个前提下，「最小值所在的域加一」时，与第二小的域交换就能得到正确的新最小值。**如果抢占算法改成跨多个节点驱逐，这个结构就要重新审视**——注释里的 CAVEAT 正是在提醒这一点。

当前 `DefaultPreemption` 的受害者已经是 `DomainVictim`，以 PodGroup 为单位时一个受害者可能跨多个节点，`SelectVictimsOnNode` 会对远端节点上的 Pod 也调用 `RunPreFilterExtensionRemovePod`/`AddPod`；注释中的前提 1 写于这一能力出现之前。

### 3.4 `Clone`

PreFilter 的结果会被复制：抢占要为每个候选节点各跑一次模拟，nominated Pod 的处理也是在副本上做的。`Clone` 决定哪些字段共享、哪些必须深拷贝：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/filtering.go
func (s *preFilterState) Clone() fwk.StateData {
	if s == nil {
		return nil
	}
	copy := preFilterState{
		// Constraints are shared because they don't change.
		Constraints:       s.Constraints,
		CriticalPaths:     make([]*criticalPaths, len(s.CriticalPaths)),
		TpValueToMatchNum: make([]map[string]int, len(s.TpValueToMatchNum)),
	}
	for i, paths := range s.CriticalPaths {
		copy.CriticalPaths[i] = &criticalPaths{paths[0], paths[1]}
	}
	for i, tpMap := range s.TpValueToMatchNum {
		copy.TpValueToMatchNum[i] = maps.Clone(tpMap)
	}
	return &copy
}
```

只读的 `Constraints` 共享，会被 `AddPod`/`RemovePod` 修改的两部分深拷贝，分两步：

1. 第 8–10 行建好新切片；
2. 第 12–17 行逐个复制。

CycleState 的 `Clone` 发生在两个地方：

1. `RunFilterPluginsWithNominatedPods` 把高优先级的 nominated Pod 加进来做第一轮 Filter 时；
2. 抢占为每个候选节点调用 `SelectVictimsOnNode(ctx, state.Clone(), ...)` 时。

浅拷贝 map 会让不同候选节点的模拟互相污染。

{{< figure src="/images/posts/sched-plugins/13-pts-filter-skew.svg" caption="PreFilter 在全部节点上计数，写出每条约束的 TpValueToMatchNum 和两条关键路径；Filter 对每个节点只查表计算 skew（点击查看大图）" link="/images/posts/sched-plugins/13-pts-filter-skew.svg" >}}

到这里 PreFilter 结束：每条约束都有了一张「拓扑值 → 匹配数」表，外带两个最小域，剩下的工作只是查表。

## 四、Filter：skew 判定

### 4.1 判定公式

Filter 对每个候选节点跑一次，读的全是 PreFilter 留下的状态，不再遍历节点上的 Pod。判定只有一条不等式：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/filtering.go
	podLabelSet := labels.Set(pod.Labels)
	for i, c := range s.Constraints {
		tpKey := c.TopologyKey
		tpVal, ok := node.Labels[tpKey]
		if !ok {
			logger.V(5).Info("Node doesn't have required topology label for spread constraint", "node", klog.KObj(node), "topologyKey", tpKey)
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonNodeLabelNotMatch)
		}

		// judging criteria:
		// 'existing matching num' + 'if self-match (1 or 0)' - 'global minimum' <= 'maxSkew'
		minMatchNum, err := s.minMatchNum(i, c.MinDomains)
		if err != nil {
			// ...
			continue
		}

		selfMatchNum := 0
		if c.Selector.Matches(podLabelSet) {
			selfMatchNum = 1
		}

		matchNum := s.TpValueToMatchNum[i][tpVal]
		skew := matchNum + selfMatchNum - minMatchNum
		if skew > int(c.MaxSkew) {
			// ...
			return fwk.NewStatus(fwk.Unschedulable, ErrReasonConstraintsNotMatch)
		}
	}
```

公式是 `skew = 该域现有匹配数 + 新 Pod 是否匹配自己的 selector − 全局最小值`（第 24–25 行）。三个推论：

1. **判定的是放上去之后的状态**，所以加上 `selfMatchNum`。如果新 Pod 的标签不满足它自己约束里的 selector（这种写法通常是配置错误），`selfMatchNum` 为 0，约束退化成「不要放到已经比最少的域多出 maxSkew 个以上的域」。
2. 「全局最小值」不加新 Pod。当所有域计数相同时，放到任何一个域 skew 都是 1，`maxSkew: 1` 刚好允许。
3. 节点所在的域如果没被统计进 `TpValueToMatchNum`（比如它被包含策略排除了），`matchNum` 读出来是 0。

两种失败的 Status code 不同，原因字符串也不同：

1. 缺标签在第 8 行；
2. skew 超限在第 28 行。

对照如下：

| 失败原因 | Status | reason |
| --- | --- | --- |
| 节点缺少约束要求的拓扑标签 | `UnschedulableAndUnresolvable` | `node(s) didn't match pod topology spread constraints (missing required label)` |
| skew 超过 `maxSkew` | `Unschedulable` | `node(s) didn't match pod topology spread constraints` |

**缺标签靠驱逐 Pod 补不上，所以是不可解决；skew 超限则可能通过驱逐该域内匹配的 Pod 降下来。** 后一种情况留给 `DefaultPreemption` 去尝试。

### 4.2 `minDomains` 与全局最小值

skew 里的「全局最小值」不是直接取 `CriticalPaths[0]`，中间还隔着 `minDomains` 一道修正，分两步：

1. 第 5–6 行取最小值与域数；
2. 第 8–12 行修正。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/filtering.go
func (s *preFilterState) minMatchNum(constraintID int, minDomains int32) (int, error) {
	paths := s.CriticalPaths[constraintID]

	minMatchNum := paths[0].MatchNum
	domainsNum := len(s.TpValueToMatchNum[constraintID])

	if domainsNum < int(minDomains) {
		// When the number of eligible domains with matching topology keys is less than `minDomains`,
		// it treats "global minimum" as 0.
		minMatchNum = 0
	}

	return minMatchNum, nil
}
```

`domainsNum` 是这条约束的 map 长度，即合格域的个数。**合格域少于 `minDomains` 时，最小值按 0 处理**，相当于假装还存在一个空域。

这改变了 `maxSkew` 的语义：它成了每个域的容量上限。`maxSkew: 1, minDomains: 3` 时，在只有两个 zone 的集群里每个 zone 最多放 1 个副本，第 3 个副本 Pending。

这个 Pending 是有意为之。cluster-autoscaler 会为 Pending 的 Pod 模拟扩容，在新 zone 扩出节点后，域数达到 3，新 zone 计数为 0，副本就被引导过去。没有 `minDomains` 时，调度器只会在现有的两个 zone 之间均分，永远不会触发跨 zone 扩容。

`minDomains` 缺省为 1 还顺带解决了一个边界：没有任何合格域时（比如所有节点都缺 `topologyKey`），`paths[0].MatchNum` 是初始值 `math.MaxInt32`，而 `domainsNum = 0 < 1`，最小值被置为 0，不会算出一个巨大的负 skew。当然，此时节点都缺标签，Filter 会先因为标签检查返回 `UnschedulableAndUnresolvable`。

### 4.3 包含策略：哪些节点算数

`nodeAffinityPolicy` 和 `nodeTaintsPolicy` 控制的是 PreFilter 的统计口径，判断逻辑在 `matchNodeInclusionPolicies`，分两部分：

1. nodeAffinity 部分在第 3–8 行；
2. nodeTaints 部分在第 10–14 行。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/common.go
func (tsc *topologySpreadConstraint) matchNodeInclusionPolicies(logger klog.Logger, pod *v1.Pod, node *v1.Node, require nodeaffinity.RequiredNodeAffinity, enableComparisonOperators bool) bool {
	if tsc.NodeAffinityPolicy == v1.NodeInclusionPolicyHonor {
		// We ignore parsing errors here for backwards compatibility.
		if match, _ := require.Match(node); !match {
			return false
		}
	}

	if tsc.NodeTaintsPolicy == v1.NodeInclusionPolicyHonor {
		if _, untolerated := v1helper.FindMatchingUntoleratedTaint(logger, node.Spec.Taints, pod.Spec.Tolerations, helper.DoNotScheduleTaintsFilterFunc(), enableComparisonOperators); untolerated {
			return false
		}
	}
	return true
}
```

`Honor` 的意思是「Pod 本来就去不了的节点，不参与计数」。`nodeTaintsPolicy: Honor` 时，判断污点用的过滤函数只看 `NoSchedule` 和 `NoExecute`，与 `TaintToleration` 的 Filter 口径一致。

为什么默认 `Ignore` 污点会出问题，下图的右半边是一个典型场景：zone-c 只剩一个带 `NoSchedule` 污点的节点（比如被 cordon 了，节点生命周期控制器会给 `spec.unschedulable` 的节点加上 `node.kubernetes.io/unschedulable:NoSchedule`）。默认口径下 zone-c 仍以计数 0 参与统计，最小值被拉到 0，zone-a、zone-b 都超出 maxSkew，而 zone-c 的节点又被 `TaintToleration` 过滤掉，Pod 无处可去。

{{< figure src="/images/posts/sched-plugins/13-pts-inclusion.svg" caption="左：minDomains 让「域不够」时最小值按 0 计，迫使副本等待新域；右：nodeTaintsPolicy 决定带污点的节点是否贡献计数为 0 的域（点击查看大图）" link="/images/posts/sched-plugins/13-pts-inclusion.svg" >}}

`NodeInclusionPolicyInPodTopologySpread` 在 1.33 已经 GA 且锁定开启，代码里 `!pl.enableNodeInclusionPolicyInPodTopologySpread` 的分支（直接用 `requiredNodeAffinity.Match` 过滤）只是保留的旧行为，实际不会再走到。

至此 Filter 这条线走完：PreFilter 数数，Filter 查表判定。剩下的是它的补充——当有 Pod 被拿走或放回时，那张计数表怎么跟着变。

## 五、AddPod / RemovePod：给抢占和 nominated Pod 用

`PreFilterExtensions` 返回插件自身，`AddPod` 和 `RemovePod` 都落到 `updateWithPod`，只是 `delta` 分别为 +1 和 -1：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/filtering.go
func (pl *PodTopologySpread) updateWithPod(logger klog.Logger, s *preFilterState, updatedPod, preemptorPod *v1.Pod, node *v1.Node, delta int) {
	if s == nil || updatedPod.Namespace != preemptorPod.Namespace || node == nil {
		return
	}
	if !nodeLabelsMatchSpreadConstraints(node.Labels, s.Constraints) {
		return
	}

	requiredSchedulingTerm := nodeaffinity.GetRequiredNodeAffinity(preemptorPod)
	// ...

	podLabelSet := labels.Set(updatedPod.Labels)
	for i, constraint := range s.Constraints {
		if !constraint.Selector.Matches(podLabelSet) {
			continue
		}

		if pl.enableNodeInclusionPolicyInPodTopologySpread &&
			!constraint.matchNodeInclusionPolicies(logger, preemptorPod, node, requiredSchedulingTerm, pl.enableTaintTolerationComparisonOperators) {
			continue
		}

		v := node.Labels[constraint.TopologyKey]
		s.TpValueToMatchNum[i][v] += delta
		s.CriticalPaths[i].update(v, s.TpValueToMatchNum[i][v])
	}
}
```

它和 `calPreFilterState` 用的是同一套口径：

1. namespace 必须相同（第 3 行）；
2. 节点必须带齐标签（第 6–8 行）；
3. 节点必须通过包含策略（第 19–22 行）；
4. 被增减的 Pod 必须匹配 selector（第 15–17 行）。

**两处口径一旦不一致，抢占模拟的结论就会和真实调度不符。**

调用方有两类：

1. `RunFilterPluginsWithNominatedPods`：节点上有优先级不低于当前 Pod 的 nominated Pod 时，框架在 state 副本上逐个 `AddPod`，让它们在计数里「占位」，再跑一遍 Filter。
2. `DefaultPreemption` 的 `SelectVictimsOnNode`：先把所有低优先级 Pod `RemovePod`，看抢占者能否放下，再按优先级从高到低逐个 `AddPod` 回去，尽量少驱逐。

skew 超限之所以是 `Unschedulable`，前提就是这里能正确模拟「驱逐某个域里的匹配 Pod 后 skew 下降」。

用一次抢占模拟看这套增减怎么落到 `TpValueToMatchNum` 和 `CriticalPaths` 上。新 Pod P 带 `app: web`，只有一条约束：zone、`maxSkew: 1`、`DoNotSchedule`、selector `app: web`。集群状态如下：

1. zone-a 的节点上共有 3 个 `app: web` 副本；
2. zone-b 只有节点 b1，上面 1 个副本，b1 的 CPU 已经放不下 P；
3. zone-c 只有节点 c1，上面 2 个副本，优先级都低于 P，按 `MoreImportantVictim` 排序后记作 web-5、web-6。

PreFilter 得到 `TpValueToMatchNum[0] = {zone-a: 3, zone-b: 1, zone-c: 2}`，`CriticalPaths[0] = [{zone-b, 1}, {zone-c, 2}]`。c1 上 skew 为 2 + 1 − 1 = 2 > 1，返回 `Unschedulable`，c1 进入抢占候选。`SelectVictimsOnNode` 在 c1 上的过程：

| 步骤 | zone-c 计数 | `CriticalPaths[0]` | c1 上的 Filter |
| --- | --- | --- | --- |
| 移除第一个副本 | 1 | `[{zone-b, 1}, {zone-c, 1}]` | — |
| 移除第二个副本 | 0 | `[{zone-c, 0}, {zone-b, 1}]` | 0 + 1 − 0 = 1，通过 |
| 加回 web-5 | 1 | `[{zone-c, 1}, {zone-b, 1}]` | 1 + 1 − 1 = 1，通过，web-5 保留 |
| 加回 web-6 | 2 | `[{zone-b, 1}, {zone-c, 2}]` | 2 + 1 − 1 = 2 > 1，失败 |
| 再次移除 web-6 | 1 | `[{zone-b, 1}, {zone-c, 1}]` | web-6 记为受害者 |

表里每一行都对应 `updateWithPod` 的第 25–26 行：先改计数，再调 `update`。三处结果对应前文的结论：

1. 第二行和第四行，被改动的域越过了另一个槽位，`update` 的交换（3.3 节代码块第 14–17 行）把新的最小值换到 `p[0]`，这就是只记两条路径也够用的原因；
2. b1 因为 CPU 不足根本放不下 P，zone-b 的计数 1 却一直是最小值，被别的插件淘汰的节点照样参与统计（第八节第一条）；
3. 最终只驱逐 web-6，c1 上 zone-c 的计数回到 1，放入 P 后为 2，与 zone-b 的差正好是 `maxSkew`。

还有一点：`RunPreFilterExtensionAddPod` 会跳过在 PreFilter 返回 `Skip` 的插件。没有 `DoNotSchedule` 约束的 Pod，`PodTopologySpread` 在抢占和 nominated 处理中完全不参与，这是正确的——它本来就没有硬约束。

硬约束这条线到此完整了。同一套计数逻辑还要服务软约束，但软约束的入口不在 Filter，而在 PreScore 和 Score。

## 六、PreScore 与 Score：对数权重

### 6.1 `preScoreState`

打分阶段另有一份状态 `preScoreState`。它不需要关键路径，却多了两样东西：缺标签节点的名单，和每条约束的归一化权重。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/scoring.go
type preScoreState struct {
	Constraints []topologySpreadConstraint
	// IgnoredNodes is a set of node names which miss some Constraints[*].topologyKey.
	IgnoredNodes sets.Set[string]
	// TopologyValueToPodCounts is a slice indexed by constraint index.
	// Each entry is keyed with topology value, and valued with the number of matching pods.
	TopologyValueToPodCounts []map[string]*int64
	// TopologyNormalizingWeight is the weight we give to the counts per topology.
	// This allows the pod counts of smaller topologies to not be watered down by
	// bigger ones.
	TopologyNormalizingWeight []float64
}

// Clone implements the mandatory Clone interface. We don't really copy the data since
// there is no need for that.
func (s *preScoreState) Clone() fwk.StateData {
	return s
}
```

这份状态的两处写法都服务于「并行只改值、不改结构」：

1. Score 阶段的状态不会被 `AddPod`/`RemovePod` 修改，`Clone`（第 17–19 行）直接返回自身。
2. 计数用 `*int64`（第 8 行的 `map[string]*int64`）是为了在并行遍历节点时用 `atomic.AddInt64` 累加：map 的结构（有哪些键）在 `initPreScoreState` 里单线程建好，并行阶段只修改指针指向的值，不修改 map 本身。

### 6.2 两轮遍历：域来自候选节点，计数来自全部节点

`initPreScoreState` 遍历 Filter 之后剩下的候选节点，做两件事：

1. 把它们所在的域登记进 `TopologyValueToPodCounts`（第 2–21 行）；
2. 数出每条约束有多少个域，算出归一化权重（第 23–29 行）。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/scoring.go
	topoSize := make([]int, len(s.Constraints))
	for _, node := range filteredNodes {
		if requireAllTopologies && !nodeLabelsMatchSpreadConstraints(node.Node().Labels, s.Constraints) {
			// Nodes which don't have all required topologyKeys present are ignored
			// when scoring later.
			s.IgnoredNodes.Insert(node.Node().Name)
			continue
		}
		for i, constraint := range s.Constraints {
			// per-node counts are calculated during Score.
			if constraint.TopologyKey == v1.LabelHostname {
				continue
			}
			value := node.Node().Labels[constraint.TopologyKey]
			if s.TopologyValueToPodCounts[i][value] == nil {
				s.TopologyValueToPodCounts[i][value] = new(int64)
				topoSize[i]++
			}
		}
	}

	s.TopologyNormalizingWeight = make([]float64, len(s.Constraints))
	for i, c := range s.Constraints {
		sz := topoSize[i]
		if c.TopologyKey == v1.LabelHostname {
			sz = len(filteredNodes) - len(s.IgnoredNodes)
		}
		s.TopologyNormalizingWeight[i] = topologyNormalizingWeight(sz)
	}
```

然后 `PreScore` 再遍历全部节点，只给已登记的域累加计数（`tpCount == nil` 的域直接跳过）。**域的范围由候选节点决定，计数的范围仍是全部节点。** 这样，一个没有候选节点的 zone 里的 Pod 不会被白白统计，而候选节点所在 zone 里那些不可行节点上的 Pod 仍然会被计入。

`kubernetes.io/hostname` 约束有专门的优化：每个节点自成一个域，没有必要在 PreScore 建一张节点数大小的 map，计数推迟到 Score 中直接数这个节点上的 Pod。

`requireAllTopologies`（第 5 行）决定缺标签的节点怎么处理：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/scoring.go
	// Only require that nodes have all the topology labels if using
	// non-system-default spreading rules. This allows nodes that don't have a
	// zone label to still have hostname spreading.
	requireAllTopologies := len(pod.Spec.TopologySpreadConstraints) > 0 || !pl.systemDefaulted
```

分两种情况：

1. 显式约束或 List 模式下，缺标签的节点进入 `IgnoredNodes`，最后得 0 分；
2. 只有 System 模式放宽了要求，没有 zone 标签的节点（单 zone 的自建集群很常见）仍然可以参与 hostname 维度的打散。

### 6.3 打分公式

Score 对每个候选节点算一次，分两步：

1. 把节点所在的每个域代入同一个函数（第 4–11 行）；
2. 求和（第 14 行取整返回）。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/scoring.go
	var score float64
	for i, c := range s.Constraints {
		if tpVal, ok := node.Labels[c.TopologyKey]; ok {
			var cnt int64
			if c.TopologyKey == v1.LabelHostname {
				cnt = int64(countPodsMatchSelector(nodeInfo.GetPods(), c.Selector, pod.Namespace))
			} else {
				cnt = *s.TopologyValueToPodCounts[i][tpVal]
			}
			score += scoreForCount(cnt, c.MaxSkew, s.TopologyNormalizingWeight[i])
		}
	}
	return int64(math.Round(score)), nil
```

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/scoring.go
func topologyNormalizingWeight(size int) float64 {
	return math.Log(float64(size + 2))
}

func scoreForCount(cnt int64, maxSkew int32, tpWeight float64) float64 {
	return float64(cnt)*tpWeight + float64(maxSkew-1)
}
```

公式由两部分组成：

1. 原始分是各约束「域内匹配数 × 权重 + (maxSkew − 1)」之和（第 6–8 行），越小越好；
2. 权重 `ln(域数 + 2)`（第 2–4 行）随域数缓慢增长，注释给出的区间是 1.09 到 8.52（对应 1 个到 5000 个域）。

权重的用意是：hostname 约束有几百个域，zone 约束只有三五个域，单个节点上多一个 Pod 的「不均衡程度」要比单个 zone 多一个 Pod 更显著，对数权重让域多的约束在总分里占更大比重，同时又不至于线性放大到压倒一切。

### 6.4 `NormalizeScore`

框架的约定是分数越高越好，而这里的原始分越小越好，`NormalizeScore` 负责翻转并归一到 0~100：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/scoring.go
	for i := range scores {
		if scores[i].Score == invalidScore {
			scores[i].Score = 0
			continue
		}
		if maxScore == 0 {
			scores[i].Score = fwk.MaxNodeScore
			continue
		}
		s := scores[i].Score
		scores[i].Score = fwk.MaxNodeScore * (maxScore + minScore - s) / maxScore
	}
```

节点分两类处理：

1. `IgnoredNodes` 中的节点先被标成哨兵值 `-1`（`invalidScore`），不参与求最小/最大值，最后置 0（第 2–6 行）；
2. 其余节点用 `100 × (max + min − s) / max` 翻转方向（第 12 行），全部为整数运算。

这个公式不是 min-max 归一化：原始分最高（最不均衡）的节点得到的是 `100 × min / max`，而不是 0。分母是 `maxScore` 而非 `max − min`，意味着所有节点原始分加上同一个常数后，归一化结果会整体向 100 靠拢、差距缩小。`scoreForCount` 里的 `maxSkew − 1` 正是这样一个常数：**`maxSkew` 越大，打分对偏差越宽容。** 软约束的 `maxSkew` 不是「容许多少偏差」的硬阈值，而是调节打分敏感度的旋钮。

下面用系统默认约束算一个完整的例子。输入是 4 个候选节点，同一个 ReplicaSet 已有 4 个副本：

1. n1 上有 2 个副本，n2、n3 各 1 个，n4 上没有；
2. 拓扑归属：n1、n2 在 zone-a，n3 在 zone-b，n4 在 zone-c。

{{< figure src="/images/posts/sched-plugins/13-pts-score.svg" caption="系统默认约束（hostname maxSkew=3、zone maxSkew=5）下的完整打分算例，包括对数权重、取整和 NormalizeScore（点击查看大图）" link="/images/posts/sched-plugins/13-pts-score.svg" >}}

按 6.3 的公式逐步算：

1. n4 所在的 zone 和节点都没有副本，原始分最低，是 6；
2. n1 所在的两个域计数都最高，原始分是 14；
3. 归一化后 n4 得 100，n1 仍有 42；
4. 再乘默认权重 2，n4 最终是 200。

也就是说，这个算例里 `PodTopologySpread` 在最好和最差的节点之间拉开的分差是 `(100 − 42) × 2 = 116` 分，其它插件（比如 `NodeResourcesFit` 的资源打分）完全可能把这个差距抹平——**这也是「默认会打散，但不保证打散」的原因**。

分数出来之后，还剩一个问题没回答：这个 Pod 被拒绝或被打低分之后，什么事件值得让它重新排队？

## 七、EventsToRegister 与 QueueingHint

被 `PodTopologySpread` 拒绝的 Pod，能被三类事件唤醒：

1. 已绑定 Pod，第 2 行；
2. 目标 Pod 自身，第 4 行；
3. 节点，第 6 行。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/plugin.go
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Add | fwk.UpdatePodLabel | fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodChange},
		// ...
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodToleration | fwk.UpdatePodLabel}, QueueingHintFn: pl.isSchedulableAfterTargetPodChange},
		// ...
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.Delete | fwk.UpdateNodeLabel | fwk.UpdateNodeTaint}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
```

各个 hint 的判断都围绕「这次变化是否改变了某条约束的计数或域」：

1. 已绑定 Pod 的增、删、改标签（`isSchedulableAfterAssignedPodChange`）：变化的 Pod 必须已经绑定或已被 nominate，并且与目标 Pod 同 namespace（`involvedInTopologySpreading`），否则 `QueueSkip`。新增 Pod 匹配任一约束的 selector，或被删除的 Pod 匹配任一约束的 selector，返回 `Queue`。标签更新时只在「更新前后对某条约束的匹配结果发生翻转」时返回 `Queue`。注意新增一个匹配的 Pod 也会 `Queue`：它可能填满了一个空域，提高了全局最小值。
2. 目标 Pod 自身更新（`isSchedulableAfterTargetPodChange`）：容忍度变化且有 `nodeTaintsPolicy: Honor` 的约束时 `Queue`，因为统计口径变了；否则按标签变化判断，因为 `matchLabelKeys` 读的是 Pod 自己的标签，`selfMatchNum` 也取决于自身标签。
3. 节点增、删、改标签/污点（`isSchedulableAfterNodeChange`）：只关心带齐所有 `topologyKey` 的节点。新增或删除这样的节点返回 `Queue`；更新时，带齐与否的状态发生翻转，或者仍然带齐但拓扑标签的值变了、污点变了，返回 `Queue`。

这里的约束是用 `getConstraints` 取的，也就是只看 `DoNotSchedule` 约束（显式或默认）。软约束不会导致 Pod 不可调度，自然不需要为它注册唤醒条件。

主流程到这里走完了：解析、计数、判定、增量更新、打分、唤醒条件。剩下的是几处不看源码容易踩到的边界。

## 八、边界情况与坑

**被别的插件过滤掉的节点也在统计里。** PreFilter 统计的是全部节点，Filter 阶段被 `NodeResourcesFit` 淘汰的节点照样贡献计数和域。

一个 zone 的节点全都资源不足，这个 zone 的计数仍然是「最小值候选」，它会让其它 zone 的节点 skew 超限，结果是整体 Pending，而错误信息里只有一部分节点显示 `didn't match pod topology spread constraints`，另一部分显示 `Insufficient cpu`。排查这类问题时要把所有节点的失败原因合起来看。污点的情况可以用 `nodeTaintsPolicy: Honor` 解决，资源不足没有对应的策略。

**`nodeAffinityPolicy: Honor` 只看 required 部分。** `GetRequiredNodeAffinity` 合并的是 `nodeSelector` 和 `requiredDuringSchedulingIgnoredDuringExecution`，preferred nodeAffinity 不影响统计口径。

**namespace 是硬边界。** 只有与新 Pod 同 namespace 的 Pod 会被计数，selector 写得再宽也不会跨 namespace。需要跨 namespace 约束分布时，`PodTopologySpread` 做不到，只能用带 `namespaceSelector` 的 `InterPodAffinity`。

**删除中的 Pod 不计数。** 滚动更新时，旧 Pod 一旦进入 `Terminating`（`DeletionTimestamp` 非空）就从计数中消失，即使它的容器还在运行。这是 [#87621](https://github.com/kubernetes/kubernetes/issues/87621) 的修复，避免新副本因为「将死的旧副本」而被挤到别的域。

**约束只在调度时生效。** 和 nodeAffinity 的 `IgnoredDuringExecution` 一样，Pod 调度之后分布被打破（节点下线、缩容删掉了某个 zone 的副本），调度器不会主动纠正。缩容时 ReplicaSet 控制器挑选删除对象，也不会读取 `topologySpreadConstraints`。

它的 `getPodsToDelete`（`pkg/controller/replicaset/replica_set.go`）按 `ActivePodsWithRanks` 排序挑选，依据依次是未调度、Pending、未就绪、`pod-deletion-cost`、同节点上同 ReplicaSet 的就绪副本数、就绪时长等。和位置有关的只有「同一节点上副本多的先删」，不看 zone。需要持续维持分布时，要配合 descheduler 的 `RemovePodsViolatingTopologySpreadConstraint` 策略。

**Deferred resize 的 Pod 直接跳过。** `InPlacePodVerticalScalingSchedulerPreemption` 开启时，已绑定且原地扩容被 kubelet 推迟的 Pod 会重新进入调度队列（详见[第 17 篇](/posts/sched-plugin-17-deferred-pod-scheduling/)），`PreFilter` 对它返回 `Skip`，`Filter` 直接放行。这种 Pod 不换节点，拓扑分布与它无关。

**默认约束会关掉 opportunistic batching。** `PodTopologySpread` 实现了 `SignPlugin`，但签名逻辑是：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/podtopologyspread/plugin.go
func (pl *PodTopologySpread) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	if len(pod.Spec.TopologySpreadConstraints) > 0 {
		return nil, fwk.NewStatus(fwk.Unschedulable, "pods with topology constraints are not signable")
	}

	if len(pl.defaultConstraints) > 0 {
		return nil, fwk.NewStatus(fwk.Unschedulable, "pods with default topology constraints are not signable")
	}

	return nil, nil
}
```

判断默认约束时看的是插件配置而不是 Pod（第 7–9 行）：默认 System 模式下 `pl.defaultConstraints` 永远非空，于是**每一个 Pod** 都拒签。KEP-5598 的 opportunistic batching（feature gate `OpportunisticBatching`，1.35 起 Beta 且默认开启）依赖 Pod 签名识别「等价」的 Pod、复用上一个 Pod 的过滤与打分结果。

**gate 虽然默认开着，在默认插件配置下它对任何 Pod 都不会生效。** 想让它工作，需要把 `defaultingType` 设为 `List` 并给一个空的 `defaultConstraints`——代价是失去默认打散。

这样设计的原因是拓扑分布依赖集群中其它 Pod 的位置，同一个 Deployment 的两个副本在调度第一个之后，第二个的可行性和打分就变了，不能复用前一个的结果。

**PreScore 没有显式约束时的空 zone 标签。** System 模式下 `requireAllTopologies` 为 false，没有 zone 标签的节点在 `initPreScoreState` 里会被登记成值为空字符串的「域」，这会让 zone 约束的 `topoSize` 多算 1。Score 时这类节点因为没有 zone 标签而跳过 zone 项，所以影响只体现在权重上，一般可以忽略。

## 九、发散

### 9.1 从 SelectorSpread 到系统默认约束

`PodTopologySpread` 不是凭空出现的，它接替的是更早的一套做法，而这段历史解释了插件里几个看起来奇怪的设计。

早期的 kube-scheduler 用 `SelectorSpread` 插件（以及更早的 `SelectorSpreadPriority` 优选函数）实现「同一个 Service/控制器的 Pod 自动打散」。它的计算方式与 `PodTopologySpread` 不同，也不能配置。

引入 `PodTopologySpread` 之后，这件事被统一到 System 模式的两条默认约束上，`SelectorSpread` 最终被移出了 in-tree 插件，当前的 `names` 包里已经没有它。默认约束里 hostname `maxSkew: 3`、zone `maxSkew: 5` 都是比较宽松的取值，配合 `ScheduleAnyway`，它表达的是和旧插件类似的「尽量分散」的软偏好，而不是硬性均衡。

这一历史也解释了 `buildDefaultConstraints` 里推导 selector 的方式为什么和 `SelectorSpread` 一模一样（Service + RC/RS/StatefulSet），以及为什么 System 模式要放宽「节点必须带齐所有拓扑标签」的要求：要兼容没有 zone 标签的集群。

### 9.2 集群级默认约束的配置

在 KubeSchedulerConfiguration 里改成 `List` 模式，可以给所有没写约束的 Pod 套上集群统一的规则：

```yaml {linenos=inline}
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: default-scheduler
  pluginConfig:
  - name: PodTopologySpread
    args:
      defaultingType: List
      defaultConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: ScheduleAnyway
```

可以在默认约束里用 `DoNotSchedule`，但要谨慎：它会作用于所有通过 Service 或控制器推导出 selector 的 Pod，一旦某个 zone 容量不足，整个集群的工作负载都可能被卡住。`defaultConstraints` 里不能写 `labelSelector`，但可以写 `matchLabelKeys`，`filterTopologySpreadConstraints` 会照常合并。

### 9.3 与 InterPodAffinity 的分工

两个插件都依赖「其它 Pod 在哪里」，能力有重叠：`podAntiAffinity` + `topologyKey: kubernetes.io/hostname` 能实现「每个节点最多一个副本」，`PodTopologySpread` 的 `maxSkew: 1` 只能实现「节点之间最多差一个」。区别在于：

1. 反亲和是「有或无」的判断，没法表达「每个 zone 最多 2 个」；`PodTopologySpread` 是数量差的判断，能表达均衡但不能表达绝对上限（`minDomains` 场景除外）。
2. 反亲和支持 `namespaceSelector` 跨 namespace；`PodTopologySpread` 只看本 namespace。
3. 反亲和对称：已有 Pod 的反亲和规则也会拒绝新 Pod。`PodTopologySpread` 不对称：已有 Pod 的约束不影响新 Pod 的调度，新 Pod 只受自己约束的限制。

性能上，`PodTopologySpread` 的 PreFilter 每个节点只做一次 selector 匹配循环，复杂度是 O(约束数 × Pod 总数)，比 `InterPodAffinity` 便宜，后者在下一篇会看到。

### 9.4 打分权重与其它插件的博弈

默认 Profile 中 `PodTopologySpread` 与 `InterPodAffinity`、`NodeAffinity` 权重都是 2，`TaintToleration` 是 3，资源类打分插件是 1。第六节的算例里它在最好和最差的节点之间只拉开了 116 分，而 `NodeResourcesFit` 的 LeastAllocated 在资源利用率差异较大的节点之间也能拉开接近 100 分的差距（乘以权重 1）。生产中如果发现软约束的打散效果不理想，先检查分数构成（`-v=10` 下调度器会打印每个插件的分数），再决定是提高权重还是改成 `DoNotSchedule`。

## 小结

1. `PodTopologySpread` 按 `whenUnsatisfiable` 把约束分成两组：`DoNotSchedule` 进 PreFilter/Filter，`ScheduleAnyway` 进 PreScore/Score；显式约束与默认约束二选一，默认 System 模式的两条约束都是软约束。
2. PreFilter 在**全部节点**上按约束计数，`TpValueToMatchNum` 每条约束一张「拓扑值 → 匹配数」表，计数为 0 的域也算合格域；`criticalPaths` 只记两个最小域，依赖「抢占只动一个节点、每个节点独立副本」两个前提。
3. Filter 的判定是 `matchNum + selfMatch − minMatchNum ≤ maxSkew`；缺标签返回 `UnschedulableAndUnresolvable`，skew 超限返回可抢占的 `Unschedulable`。
4. `minDomains` 在域不足时把最小值按 0 计，把 `maxSkew` 变成每个域的上限，用来驱动跨域扩容；`nodeAffinityPolicy`（默认 Honor）和 `nodeTaintsPolicy`（默认 Ignore）决定哪些节点参与统计。
5. Score 用 `cnt × ln(域数+2) + (maxSkew−1)` 计算原始分，`NormalizeScore` 用 `100 × (max+min−s)/max` 翻转，不是 min-max；`maxSkew` 在软约束里是灵敏度旋钮。
6. 默认配置下 `SignPod` 对所有 Pod 拒签，opportunistic batching 不会生效。

下一篇进入同样依赖「其它 Pod 在哪里」、但开销大得多的 [InterPodAffinity](/posts/sched-plugin-14-inter-pod-affinity/)。
