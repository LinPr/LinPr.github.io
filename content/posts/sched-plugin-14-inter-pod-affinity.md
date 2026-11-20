---
title: 'scheduler-plugins InterPodAffinity：三张拓扑计数表与一条快速路径'
date: 2026-11-19T10:20:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "InterPodAffinity", "Pod亲和", "调度插件"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "InterPodAffinity 实现 Pod 间亲和与反亲和。本文从源码拆解三张拓扑计数表、Filter 的三道检查与状态码、亲和不可抢占的原因、hostname 快速路径、Score 的双向累加与 hardPodAffinityWeight，以及它的性能开销。"
---

[上一篇](/posts/sched-plugin-13-pod-topology-spread/)的 `PodTopologySpread` 数的是「一个拓扑域里有多少个匹配的 Pod」。`InterPodAffinity` 问的问题更朴素，也更昂贵：「这个拓扑域里**有没有**某种 Pod」。有，亲和满足；有，反亲和就不满足。而且规则是双向的——不光新 Pod 的规则要满足，已经在跑的 Pod 声明的反亲和也会把新 Pod 挡在门外。

本文基于 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期）。`InterPodAffinity` 默认启用，在默认 Profile 的 MultiPoint 列表里紧跟 `PodTopologySpread`，打分权重为 2。它实现了 PreFilter（带 `PreFilterExtensions`）、Filter、PreScore、Score（带 `NormalizeScore`）、`EnqueueExtensions` 和 `SignPlugin`。插件本身没有 feature gate，但有一个 1.37 新增的 Alpha gate `InterPodAffinityHostnameFastPath`（默认关闭）改变了 PreFilter/Filter 的内部实现，本文第五节单独讲。

## 一、规则长什么样

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: web-0
  namespace: shop
  labels: {app: web, tier: frontend}
spec:
  affinity:
    podAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels: {app: cache}
        topologyKey: topology.kubernetes.io/zone      # 必须和 cache 在同一个 zone
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels: {app: web}
        topologyKey: kubernetes.io/hostname           # 同一节点上不能有两个 web
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 50
        podAffinityTerm:
          labelSelector:
            matchLabels: {team: payments}
          namespaceSelector:                          # 跨 namespace 匹配
            matchLabels: {env: prod}
          topologyKey: topology.kubernetes.io/zone
  containers:
  - name: web
    image: nginx
```

一条规则（`PodAffinityTerm`）由三部分组成：用 `labelSelector` 圈定「哪些 Pod」，用 `namespaces`/`namespaceSelector` 圈定「在哪些 namespace 里找」，用 `topologyKey` 定义「什么叫在一起」。`required` 的规则进 Filter，`preferred` 的规则进 Score。

语义上有一个不对称值得先记住：**多条 required 亲和项是「同一个 Pod 同时满足全部项」**，而不是「每一项各自有 Pod 满足」；**多条 required 反亲和项是「任何一项被违反就拒绝」**。前者由 `podMatchesAllAffinityTerms` 实现，后者由逐项检查实现，后面的数据结构和快速路径都建立在这个不对称上。

## 二、规则的预处理：AffinityTerm 与 namespace

### 2.1 `AffinityTerm`

规则在进入调度器缓存时就被解析好了。`framework.NewPodInfo` 会把 Pod 的四类规则（required/preferred × 亲和/反亲和）解析成 `fwk.AffinityTerm`，存在 `PodInfo` 里，此后每个调度周期都直接用，不再重复解析 selector：

```go
// staging/src/k8s.io/kube-scheduler/framework/types.go
type AffinityTerm struct {
	Namespaces        sets.Set[string]
	Selector          labels.Selector
	TopologyKey       string
	NamespaceSelector labels.Selector
}

// Matches returns true if the pod matches the label selector and namespaces or namespace selector.
func (at *AffinityTerm) Matches(pod *v1.Pod, nsLabels labels.Set) bool {
	if at.Namespaces.Has(pod.Namespace) || at.NamespaceSelector.Matches(nsLabels) {
		return at.Selector.Matches(labels.Set(pod.Labels))
	}
	return false
}
```

namespace 的默认值在 `getNamespacesFromPodAffinityTerm` 中确定：`namespaces` 为空且 `namespaceSelector` 为 nil 时，取规则所属 Pod 自己的 namespace。`namespaceSelector` 的 nil 与空对象含义完全不同——`metav1.LabelSelectorAsSelector(nil)` 返回 `labels.Nothing()`，空的 `{}` 返回 `labels.Everything()`，即所有 namespace。

`NodeInfo` 还按规则类型维护了几个子集：`PodsWithAffinity`（有任何 Pod 亲和/反亲和的 Pod）、`PodsWithRequiredAntiAffinity`，以及快速路径用到的 `PodsWithRequiredNonHostScopedAntiAffinity`。快照（`Snapshot`）再据此维护「有这类 Pod 的节点列表」，PreFilter 和 PreScore 用它们缩小遍历范围。

### 2.2 新 Pod 的 `namespaceSelector` 被展开成 namespace 列表

`Matches` 需要被匹配 Pod 的 namespace 标签才能判断 `namespaceSelector`。对新 Pod 的规则，插件在 PreFilter/PreScore 中先做一次展开：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/plugin.go
func (pl *InterPodAffinity) mergeAffinityTermNamespacesIfNotEmpty(at *fwk.AffinityTerm) error {
	if at.NamespaceSelector.Empty() {
		return nil
	}
	ns, err := pl.nsLister.List(at.NamespaceSelector)
	if err != nil {
		return err
	}
	for _, n := range ns {
		at.Namespaces.Insert(n.Name)
	}
	at.NamespaceSelector = labels.Nothing()
	return nil
}
```

用 namespace lister 把 selector 能选中的 namespace 全部列出来并入 `Namespaces`，然后把 selector 置成 `Nothing()`。之后拿新 Pod 的规则去匹配成千上万个已有 Pod 时，传入的 `nsLabels` 一律是 `nil`，只查集合，不再去取每个已有 Pod 的 namespace 标签。代码里反复出现的注释「The incoming pod's terms have the namespaceSelector merged into the namespaces」说的就是这件事。

`Empty()` 返回 true 的是 `Everything()`（空 selector），这种情况不展开，保持「匹配所有 namespace」。`Nothing()` 的 `Empty()` 返回 false，会走一遍 `List`，结果为空，没有副作用。

反方向——拿**已有 Pod 的规则**去匹配新 Pod——就不能这样做了，已有 Pod 的规则成千上万条，不可能每个周期都展开。插件改为只取一次新 Pod 所在 namespace 的标签：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/plugin.go
func GetNamespaceLabelsSnapshot(logger klog.Logger, ns string, nsLister listersv1.NamespaceLister) (nsLabels labels.Set) {
	podNS, err := nsLister.Get(ns)
	if err == nil {
		// Create and return snapshot of the labels.
		return labels.Merge(podNS.Labels, nil)
	}
	logger.V(3).Info("getting namespace, assuming empty set of namespace labels", "namespace", ns, "err", err)
	return
}
```

两个方向各自选择了开销更小的展开方式：新 Pod 的规则少，展开 selector；已有 Pod 多，只取新 Pod 一个 namespace 的标签。

`matchLabelKeys`/`mismatchLabelKeys` 不在调度器里处理。apiserver 的 `mutatePodAffinity` 在创建 Pod 时就把它们转换成 `labelSelector` 中的 `In`/`NotIn` 条件，调度器看到的已经是合并后的 selector。

## 三、PreFilter：三张拓扑计数表

### 3.1 数据结构

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
type topologyPair struct {
	key   string
	value string
}
type topologyToMatchedTermCount map[topologyPair]int64
```

计数表的键是 `topologyPair{key, value}`，例如 `{topology.kubernetes.io/zone, zone-a}`。和 `PodTopologySpread` 按约束下标拆表不同，这里所有规则的计数混在一张表里，因为 Filter 只需要回答「这个拓扑对上有没有」，不需要区分是哪条规则贡献的。

`preFilterState` 里有三张这样的表（gate 关闭时的含义）：

| 字段 | 记录什么 | 来源 |
| --- | --- | --- |
| `existingClusterWideAntiAffinityCounts` | 已有 Pod 的 required 反亲和项匹配新 Pod 的次数 | 只扫有 required 反亲和 Pod 的节点 |
| `clusterWideAffinityCounts` | 匹配新 Pod 全部 required 亲和项的已有 Pod 数 | 扫全部节点的全部 Pod |
| `clusterWideAntiAffinityCounts` | 匹配新 Pod 某条 required 反亲和项的已有 Pod 数 | 扫全部节点的全部 Pod |

表的更新函数有一个细节：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
func (m topologyToMatchedTermCount) update(node *v1.Node, tk string, value int64) {
	if tv, ok := node.Labels[tk]; ok {
		pair := topologyPair{key: tk, value: tv}
		m[pair] += value
		// value could be negative, hence we delete the entry if it is down to zero.
		if m[pair] == 0 {
			delete(m, pair)
		}
	}
}
```

计数降到 0 就删除键。这不是为了省内存，而是语义需要：Filter 的自亲和判断用 `len(clusterWideAffinityCounts) == 0` 表示「全集群没有任何匹配 Pod」，`RemovePod` 把最后一个匹配 Pod 减掉后，必须让表真正变空。

另外节点没有 `topologyKey` 标签时，这个 Pod 不被记账。对反亲和，这意味着「没有这个标签的节点上的 Pod 不会排斥任何人」。

### 3.2 两次并行扫描

已有 Pod 的反亲和只需要看「有 required 反亲和 Pod 的节点」，这个列表由快照维护：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
	processNode := func(i int) {
		nodeInfo := nodes[i]
		node := nodeInfo.Node()

		clusterWideAntiAffinityCounts := make(topologyToMatchedTermCountList, 0)
		hasHostScoped := false
		for _, existingPod := range nodeInfo.GetPodsWithRequiredAntiAffinity() {
			for _, term := range existingPod.GetRequiredAntiAffinityTerms() {
				if pl.enableInterPodAffinityHostnameFastPath && term.TopologyKey == v1.LabelHostname {
					hasHostScoped = true
				} else if term.Matches(incomingPod, nsLabels) {
					clusterWideAntiAffinityCounts.recordMatch(node, term.TopologyKey, 1)
				}
			}
		}
		// ...
		if len(clusterWideAntiAffinityCounts) != 0 {
			antiAffinityCountsList[atomic.AddInt32(&index, 1)] = clusterWideAntiAffinityCounts
		}
	}
	pl.parallelizer.Until(ctx, len(nodes), processNode, pl.Name())
```

新 Pod 自己的规则则必须扫**全部节点的全部 Pod**，因为任何 Pod 都可能被新 Pod 的 selector 选中：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
	processNode := func(i int) {
		nodeInfo := allNodes[i]
		node := nodeInfo.Node()

		affinity := make(topologyToMatchedTermCountList, 0)
		antiAffinity := make(topologyToMatchedTermCountList, 0)
		for _, existingPod := range nodeInfo.GetPods() {
			affinity.appendWithAffinityTerms(affinityTerms, existingPod.GetPod(), node, 1)
			// The incoming pod's terms have the namespaceSelector merged into the namespaces, and so
			// here we don't lookup the existing pod's namespace labels, hence passing nil for nsLabels.
			antiAffinity.appendWithAntiAffinityTerms(antiAffinityTerms, existingPod.GetPod(), nil, node, 1)
		}

		if len(affinity) > 0 || len(antiAffinity) > 0 {
			k := atomic.AddInt32(&index, 1)
			affinityCountsList[k] = affinity
			antiAffinityCountsList[k] = antiAffinity
		}
	}
```

并行阶段每个节点产出一个 `topologyToMatchedTermCountList`（切片，不是 map），用原子自增的下标紧凑地写入结果数组，没有匹配的节点不占位；并行结束后单线程合并成 map。源码注释说明了用切片的原因：合并时遍历切片比遍历 map 快。在几千个节点、每个节点都要产出局部结果的场景里，局部结果用 map 会带来大量小 map 的分配和哈希开销。

亲和项的记账规则是「全匹配才记」：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
func (m *topologyToMatchedTermCountList) appendWithAffinityTerms(
	terms []fwk.AffinityTerm, pod *v1.Pod, node *v1.Node, value int64) {
	if podMatchesAllAffinityTerms(terms, pod) {
		for _, t := range terms {
			m.recordMatch(node, t.TopologyKey, value)
		}
	}
}
```

一个已有 Pod 同时满足全部亲和项，才会在每一项的拓扑对上各记一次。这正是「同一个 Pod 满足全部项」的语义：如果 Pod A 满足项 1、Pod B 满足项 2，它们都不会被记账。

### 3.3 Skip 条件与 Clone

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
	incomingPodHasNoTerms := len(s.podInfo.GetRequiredAffinityTerms()) == 0 && len(s.podInfo.GetRequiredAntiAffinityTerms()) == 0
	if len(s.existingClusterWideAntiAffinityCounts) == 0 && incomingPodHasNoTerms && !mustEvaluateHostScopedAntiAffinity {
		return nil, fwk.NewStatus(fwk.Skip)
	}
```

新 Pod 没有 required 规则、也没有任何已有 Pod 的反亲和匹配它时，整个 Filter 被跳过。绝大多数 Pod 走的是这条路：它们不写 Pod 亲和，集群里写了 required 反亲和的 Pod 也只针对特定标签。此时 PreFilter 的开销只是扫一遍「有 required 反亲和 Pod 的节点」。

另一个早退出口：Pod 的规则无法解析时（例如 selector 写法非法），`NewPodInfo` 报错，PreFilter 返回 `UnschedulableAndUnresolvable`，原因 `parsing pod: ...`。

`Clone` 深拷贝三张表，其余字段（规则切片、`podInfo`、namespace 标签）共享。理由和 `PodTopologySpread` 一样：抢占模拟会在副本上 `AddPod`/`RemovePod`，三张表会被改，规则不会。

## 四、Filter：三道检查与两种状态码

{{< figure src="/images/posts/sched-plugins/14-ipa-filter-maps.svg" caption="一个具体例子：PreFilter 产出三张拓扑计数表，Filter 按「新 Pod 亲和 → 新 Pod 反亲和 → 已有 Pod 反亲和」的顺序逐节点查表（点击查看大图）" link="/images/posts/sched-plugins/14-ipa-filter-maps.svg" >}}

gate 关闭时，Filter 的主体就是三次查表：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
	} else {
		if !nodeTopologyMatchesAffinity(state, nodeInfo) {
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonAffinityRulesNotMatch)
		}
	}
	// ...
	} else {
		if !satisfyPodAntiAffinity(state, nodeInfo) {
			return fwk.NewStatus(fwk.Unschedulable, ErrReasonAntiAffinityRulesNotMatch)
		}
	}

	if !satisfyExistingPodsAntiAffinity(state, nodeInfo) {
		return fwk.NewStatus(fwk.Unschedulable, ErrReasonExistingAntiAffinityRulesNotMatch)
	}

	return nil
```

| 检查 | 失败时 Status | reason |
| --- | --- | --- |
| 新 Pod 的 required 亲和 | `UnschedulableAndUnresolvable` | `node(s) didn't match pod affinity rules` |
| 新 Pod 的 required 反亲和 | `Unschedulable` | `node(s) didn't match pod anti-affinity rules` |
| 已有 Pod 的 required 反亲和 | `Unschedulable` | `node(s) didn't satisfy existing pods anti-affinity rules` |

### 4.1 亲和检查与「第一个副本」问题

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
func nodeTopologyMatchesAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.clusterWideAffinityTerms) > 0 {
		podsExist := true
		for _, term := range state.clusterWideAffinityTerms {
			if topologyValue, ok := nodeInfo.Node().Labels[term.TopologyKey]; ok {
				tp := topologyPair{key: term.TopologyKey, value: topologyValue}
				if state.clusterWideAffinityCounts[tp] <= 0 {
					podsExist = false
				}
			} else {
				// All topology labels must exist on the node - also in case of self-affinity.
				return false
			}
		}

		if !podsExist {
			return len(state.clusterWideAffinityCounts) == 0 && satisfiesSelfAffinityCheck(state, nodeInfo)
		}
		return true
	}
	return true
}
```

节点必须带齐所有亲和项的 `topologyKey`，并且每一项对应的拓扑对上计数都大于 0。

`!podsExist` 时有一个特例：如果**整个集群**都没有匹配的 Pod（表为空），而新 Pod 自己满足自己的全部亲和项，就放行。典型场景是一个 Deployment 要求「所有副本在同一个 zone」——亲和项选中的就是自己这类 Pod。没有这个特例，第一个副本永远找不到可以亲近的对象，整个 Deployment 卡死。注意条件是「全集群为空」而不是「本域为空」：只要别的 zone 已经有了匹配的 Pod，新副本就必须去那个 zone。

### 4.2 反亲和检查

新 Pod 的反亲和只看它自己每条规则对应的拓扑对：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
func satisfyPodAntiAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.clusterWideAntiAffinityCounts) > 0 {
		for _, term := range state.podInfo.GetRequiredAntiAffinityTerms() {
			if topologyValue, ok := nodeInfo.Node().Labels[term.TopologyKey]; ok {
				tp := topologyPair{key: term.TopologyKey, value: topologyValue}
				if state.clusterWideAntiAffinityCounts[tp] > 0 {
					return false
				}
			}
		}
	}
	return true
}
```

已有 Pod 的反亲和则不知道是哪条规则、用的哪个 `topologyKey`，只能拿节点的**每一个标签**去表里查：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
func satisfyExistingPodsAntiAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.existingClusterWideAntiAffinityCounts) > 0 {
		// Iterate over topology pairs to get any of the pods being affected by
		// the scheduled pod anti-affinity terms
		for topologyKey, topologyValue := range nodeInfo.Node().Labels {
			tp := topologyPair{key: topologyKey, value: topologyValue}
			if state.existingClusterWideAntiAffinityCounts[tp] > 0 {
				return false
			}
		}
	}
	// ...
	return true
}
```

云厂商的节点常常带几十个标签，这个循环在每个候选节点上都要跑一遍。表为空时直接跳过，所以只有确实存在「排斥新 Pod 的已有 Pod」时才付出这个代价。

### 4.3 为什么亲和不可抢占，反亲和可以

抢占的手段只有一个：**删除**节点上的低优先级 Pod。

亲和不满足，意味着目标拓扑域里**缺少**某个 Pod。删除 Pod 只会让缺口更大，不可能让它满足，所以返回 `UnschedulableAndUnresolvable`，`DefaultPreemption` 在挑选候选节点时会直接排除这类节点，省掉一次注定失败的模拟。

反亲和不满足，意味着目标拓扑域里**多了**某个 Pod，删掉它就可能解决，所以返回 `Unschedulable`。但「可能」二字很重要：抢占只驱逐**候选节点本身**上的 Pod。拓扑键是 zone 时，冲突的 Pod 很可能在同 zone 的另一个节点上，在当前节点上驱逐什么都无济于事——上图中的 n4 就是这种情况。这类节点会进入抢占候选，在 `SelectVictimsOnNode` 把本节点所有低优先级 Pod 移除后再跑 Filter 时失败，被淘汰。

`DefaultPreemption` 的源码里还有一段注释专门说明了一个不支持的情况：

```go
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
	// If the preemptor does not fit after removing all the eligible victims,
	// we are almost done and this node is not suitable for preemption. The only
	// condition that we could check is if the preemptor is failing to schedule due to
	// inter-pod affinity to one or more victims, but we have decided not to
	// support this case for performance reasons. Having affinity to lower
	// importance (priority) pods is not a recommended configuration anyway.
```

如果抢占者对某个低优先级 Pod 有亲和，「先全部移除再逐个加回」的算法在第一步就会因为亲和不满足而失败，即使只驱逐别的 Pod 就能放下。社区认为对低优先级 Pod 声明亲和本身就不是推荐配置，没有为此增加复杂度。

## 五、InterPodAffinityHostnameFastPath

### 5.1 为什么需要它

gate 关闭时，新 Pod 只要有一条 required 规则，PreFilter 就要扫全集群所有 Pod；hostname 拓扑的规则也不例外。可 `topologyKey: kubernetes.io/hostname` 的规则只关心候选节点**本机**上的 Pod，为它在全集群建一张「每个节点一个键」的大表，再在 Filter 里查这张表，是在绕远路：Filter 阶段手里本来就有这个节点的 `NodeInfo`，直接看它上面的 Pod 就够了。

`preFilterState` 上方有一大段注释说明这个优化的设计，核心思路是按拓扑键把规则分成「全局」和「本机」两类：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
func (s *preFilterState) classifyTermsBasedOnScope() {
	if s.allHostScoped() {
		s.hostScopedAffinityTerms = s.podInfo.GetRequiredAffinityTerms()
	} else {
		s.clusterWideAffinityTerms = s.podInfo.GetRequiredAffinityTerms()
	}

	for _, t := range s.podInfo.GetRequiredAntiAffinityTerms() {
		if t.TopologyKey == v1.LabelHostname {
			s.hostScopedAntiAffinityTerms = append(s.hostScopedAntiAffinityTerms, t)
		} else {
			s.clusterWideAntiAffinityTerms = append(s.clusterWideAntiAffinityTerms, t)
		}
	}
}
```

{{< figure src="/images/posts/sched-plugins/14-ipa-fastpath.svg" caption="开启 InterPodAffinityHostnameFastPath 后，三类规则各自如何分流：hostname 规则跳过全局表，改在 Filter 中只看本节点的 Pod（点击查看大图）" link="/images/posts/sched-plugins/14-ipa-fastpath.svg" >}}

### 5.2 亲和与反亲和的分法不同

反亲和项**逐条**分类：违反任何一条就拒绝，各条之间独立，hostname 项完全可以单独在本机检查。

亲和项**整体**分类：只有当全部项都是 hostname 时，才整体归入 `hostScopedAffinityTerms`；只要有一条不是，就**全部**（包括其中的 hostname 项）留在 `clusterWideAffinityTerms` 里走全局表。原因还是「同一个 Pod 满足全部项」：如果拆开，hostname 项在本机找到一个 Pod A，zone 项在全局表里找到一个 Pod B，无法判断 A 和 B 是不是同一个 Pod。

全部是 hostname 的亲和项在 Filter 中这样检查：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
func nodePodsSatisfyAffinity(state *preFilterState, nodeInfo fwk.NodeInfo) bool {
	if len(state.hostScopedAffinityTerms) == 0 {
		return true
	}
	if _, ok := nodeInfo.Node().Labels[v1.LabelHostname]; !ok {
		// Topology label kubernetes.io/hostname must be present on the node for the terms to match.
		return false
	}

	for _, existingPod := range nodeInfo.GetPods() {
		if podMatchesAllAffinityTerms(state.hostScopedAffinityTerms, existingPod.GetPod()) {
			return true
		}
	}
	// existing matching pod not found, check if self-affinity is satisfied
	return satisfiesSelfAffinityCheck(state, nodeInfo)
}
```

这里丢失了全局表，自亲和特例需要的「全集群是否有匹配 Pod」就没有了。补救办法是 PreFilter 里多做一次轻量的全集群计数（`countMatchingHostScopedAffinityPodsGlobally`），只累加一个 `int64`，不建 map。这一步仍然要遍历所有 Pod，所以对亲和项来说，快速路径省掉的主要是 map 的构建和 Filter 中的查表，而不是遍历本身。

### 5.3 已有 Pod 的 hostname 反亲和

快照新增了一个节点列表：只包含「有**非** hostname required 反亲和 Pod」的节点。PreFilter 只扫这些节点建 `existingClusterWideAntiAffinityCounts`。hostname 项在 Filter 中就地检查：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
	// Local check for host-scoped anti-affinity
	if state.enableInterPodAffinityHostnameFastPath {
		// Topology label kubernetes.io/hostname must be present on the node for the terms to match.
		if _, ok := nodeInfo.Node().Labels[v1.LabelHostname]; ok {
			for _, existingPodInfo := range nodeInfo.GetPodsWithRequiredAntiAffinity() {
				for _, term := range existingPodInfo.GetRequiredAntiAffinityTerms() {
					if term.TopologyKey == v1.LabelHostname {
						if term.Matches(state.podInfo.GetPod(), state.namespaceLabels) {
							return false
						}
					}
				}
			}
		}
	}
```

这带来一个新问题：表为空时 PreFilter 原本会返回 `Skip`，但现在表为空不代表没有 hostname 反亲和需要检查。于是 PreFilter 多算了一个布尔值：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
		var nonHostScopedNodesContainHostScopedTerms bool
		s.existingClusterWideAntiAffinityCounts, nonHostScopedNodesContainHostScopedTerms = pl.getExistingAntiAffinityCounts(ctx, pod, s.namespaceLabels, nodesWithNonHostScopedAntiAffinityPods)
		nodeExistsWithPodsWithOnlyHostScopedAntiAffinityTerms := len(nodesWithAnyRequiredAntiAffinityPods) > len(nodesWithNonHostScopedAntiAffinityPods)
		mustEvaluateHostScopedAntiAffinity = nonHostScopedNodesContainHostScopedTerms || nodeExistsWithPodsWithOnlyHostScopedAntiAffinityTerms
```

判断方式很巧：「有任何 required 反亲和 Pod 的节点数」大于「有非 hostname 反亲和 Pod 的节点数」，说明存在只带 hostname 反亲和 Pod 的节点；再加上扫描过程中顺带发现的 hostname 项，两者任一为真就不能 `Skip`。两个列表都是快照预先维护的，比较长度是 O(1)。

### 5.4 代价与收益

快速路径的收益主要在两类工作负载上：只用 hostname 反亲和做「一节点一副本」的 Deployment（最常见的用法），以及集群里大量存在 hostname 反亲和 Pod 的情况。前者的新 Pod 规则全部是本机规则，`getIncomingAffinityAntiAffinityCounts` 直接返回，PreFilter 不再扫全集群 Pod；后者的已有 Pod 反亲和扫描只覆盖「有非 hostname 反亲和 Pod」的节点，匹配新 Pod 的 hostname 规则也不再往 `existingClusterWideAntiAffinityCounts` 里写「每个节点一个」的键。

代价是 Filter 阶段的工作量上升：每个候选节点都要遍历本机上的 Pod。由于每个节点上的 Pod 数有上限（默认 110），这个代价是有界的。gate 在 1.37 是 Alpha、默认关闭，`NodeInfo.PodsWithRequiredNonHostScopedAntiAffinity` 的注释也写明了 gate 关闭时这个切片必须为空。

## 六、AddPod / RemovePod

`AddPod` 和 `RemovePod` 都调用 `preFilterState.updateWithPod`，用 `multiplier` 区分正负：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/filtering.go
	} else {
		s.existingClusterWideAntiAffinityCounts.updateWithAntiAffinityTerms(pInfo.GetRequiredAntiAffinityTerms(), s.podInfo.GetPod(), s.namespaceLabels, node, multiplier)
	}
	// Process the incoming pod's affinity and anti-affinity terms.
	if s.enableInterPodAffinityHostnameFastPath {
		// ...
		if len(s.hostScopedAffinityTerms) > 0 {
			// ...
			if podMatchesAllAffinityTerms(s.hostScopedAffinityTerms, pInfo.GetPod()) {
				s.matchingHostScopedAffinityPodsCount += multiplier
			}
		}
	} else {
		s.clusterWideAffinityCounts.updateWithAffinityTerms(s.podInfo.GetRequiredAffinityTerms(), pInfo.GetPod(), node, multiplier)
		// The incoming pod's terms have the namespaceSelector merged into the namespaces, and so
		// here we don't lookup the updated pod's namespace labels, hence passing nil for nsLabels.
		s.clusterWideAntiAffinityCounts.updateWithAntiAffinityTerms(s.podInfo.GetRequiredAntiAffinityTerms(), pInfo.GetPod(), nil, node, multiplier)
	}
```

一个被增减的 Pod 同时影响三张表：它自己的反亲和规则可能排斥新 Pod，它也可能被新 Pod 的亲和/反亲和规则选中。快速路径下 hostname 项不在表里，只需维护自亲和计数器；本机的 hostname 检查在 Filter 中直接读 `NodeInfo`，而抢占模拟时 `NodeInfo` 本身已经被 `RemovePod`/`AddPodInfo` 修改过，自然是最新的。

调用场景与上一篇相同：nominated Pod 的占位，以及抢占的「全部移除、逐个加回」。这里要注意 PreFilter 返回 `Skip` 的后果：框架的 `RunPreFilterExtensionAddPod` 会跳过被 Skip 的插件，Filter 也整个跳过。如果新 Pod 自己没有 required 规则、集群里也没有任何**已绑定**的 Pod 用反亲和排斥它，而某个高优先级的 **nominated** Pod 恰好带有排斥新 Pod 的反亲和，`RunFilterPluginsWithNominatedPods` 第一轮（加入 nominated Pod 的那一轮）也不会检查到它。nominated Pod 真正绑定后，下一个 Pod 的 PreFilter 才会看到它的反亲和。

## 七、PreScore 与 Score：双向累加

### 7.1 五类贡献

PreScore 产出一张「拓扑键 → 拓扑值 → 分数」的表：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/scoring.go
type scoreMap map[string]map[string]int64

type preScoreState struct {
	topologyScore scoreMap
	podInfo       fwk.PodInfo
	// A copy of the incoming pod's namespace labels.
	namespaceLabels labels.Set
}
```

对每个已有 Pod，`processExistingPod` 累加五类贡献，记账位置都是「已有 Pod 所在节点」的拓扑值：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/scoring.go
	topoScore.processTerms(state.podInfo.GetPreferredAffinityTerms(), existingPod.GetPod(), nil, existingPodNode, 1)

	topoScore.processTerms(state.podInfo.GetPreferredAntiAffinityTerms(), existingPod.GetPod(), nil, existingPodNode, -1)

	if pl.args.HardPodAffinityWeight > 0 && len(existingPodNode.Labels) != 0 {
		for _, t := range existingPod.GetRequiredAffinityTerms() {
			topoScore.processTerm(&t, pl.args.HardPodAffinityWeight, incomingPod, state.namespaceLabels, existingPodNode, 1)
		}
	}

	topoScore.processTerms(existingPod.GetPreferredAffinityTerms(), incomingPod, state.namespaceLabels, existingPodNode, 1)

	topoScore.processTerms(existingPod.GetPreferredAntiAffinityTerms(), incomingPod, state.namespaceLabels, existingPodNode, -1)
```

前两类是新 Pod 的 preferred 规则匹配已有 Pod（亲和加分、反亲和减分），后三类是已有 Pod 的规则匹配新 Pod。这就是 Score 的**对称性**：A 想靠近 B 时，不仅 A 被调度时会靠向 B，B 被调度时也会被吸引到 A 附近；A 想远离 B 时同理。

### 7.2 `hardPodAffinityWeight`

第三类贡献最特别：已有 Pod 的 **required** 亲和规则在打分时被当作权重为 `hardPodAffinityWeight` 的 preferred 规则。

为什么 Filter 里没有「已有 Pod 的亲和」这一项检查？因为 required 亲和是已有 Pod 在**它自己调度时**的要求，那时已经满足了（`IgnoredDuringExecution`），新 Pod 来不来、放哪里都不会违反它。但从集群整体看，如果 db 要求和 web 同 zone，web 的新副本也放到同 zone 显然更合理。`hardPodAffinityWeight` 就是这种「对称的善意」的强度。

`InterPodAffinityArgs` 的默认值和校验：

| 参数 | 默认值 | 范围 | 说明 |
| --- | --- | --- | --- |
| `hardPodAffinityWeight` | 1 | 0～100 | 设为 0 关闭第三类贡献 |
| `ignorePreferredTermsOfExistingPods` | false | — | 新 Pod 没有 preferred 规则时直接 `Skip` 整个打分 |

默认值 1 相对于 preferred 规则常用的几十的权重是很小的，它更像是「平局时的倾向」。

### 7.3 遍历范围的优化

```go
// pkg/scheduler/framework/plugins/interpodaffinity/scoring.go
	// Optionally ignore calculating preferences of existing pods' affinity rules
	// if the incoming pod has no inter-pod affinities.
	if pl.args.IgnorePreferredTermsOfExistingPods && !hasConstraints {
		return fwk.NewStatus(fwk.Skip)
	}

	// Unless the pod being scheduled has preferred affinity terms, we only
	// need to process nodes hosting pods with affinity.
	var allNodes []fwk.NodeInfo
	var err error
	if hasConstraints {
		allNodes, err = pl.sharedLister.NodeInfos().List()
		// ...
	} else {
		allNodes, err = pl.sharedLister.NodeInfos().HavePodsWithAffinityList()
		// ...
	}
```

新 Pod 没有 preferred 规则时，只有后三类贡献可能非零，而它们都来自「有亲和规则的已有 Pod」，所以只需遍历 `HavePodsWithAffinityList` 里的节点、每个节点只看 `GetPodsWithAffinity()`。新 Pod 有 preferred 规则时，任何 Pod 都可能被它选中，必须遍历全部节点的全部 Pod。并行结束后 `index == -1`（没有任何贡献）时返回 `Skip`。

注意 `hasConstraints` 只看 preferred 规则。新 Pod 只有 required 规则时，打分阶段并不关心它们——required 规则已经在 Filter 里决定了可行性，候选节点在这些规则上都是满足的。

### 7.4 Score 与 NormalizeScore

```go
// pkg/scheduler/framework/plugins/interpodaffinity/scoring.go
	var score int64
	for tpKey, tpValues := range s.topologyScore {
		if v, exist := node.Labels[tpKey]; exist {
			score += tpValues[v]
		}
	}
```

Score 只是按节点标签把表里对应的值加起来，可以为负。`NormalizeScore` 是标准的 min-max：

```go
// pkg/scheduler/framework/plugins/interpodaffinity/scoring.go
	maxMinDiff := maxCount - minCount
	for i := range scores {
		fScore := float64(0)
		if maxMinDiff > 0 {
			fScore = float64(fwk.MaxNodeScore) * (float64(scores[i].Score-minCount) / float64(maxMinDiff))
		}

		scores[i].Score = int64(fScore)
	}
```

与 `PodTopologySpread` 的翻转公式不同，这里最低分的节点得 0，最高分的得 100，中间按比例截断取整。所有节点原始分相同时全部得 0。

{{< figure src="/images/posts/sched-plugins/14-ipa-score.svg" caption="打分算例：五类贡献按已有 Pod 所在节点的拓扑值记账，Score 按候选节点标签求和，NormalizeScore 做 min-max（点击查看大图）" link="/images/posts/sched-plugins/14-ipa-score.svg" >}}

算例中 n2 与 n1 同 zone，都拿到 cache 带来的 +80，但 n2 上已经有一个 web，被新 Pod 自己的反亲和扣了 50；n3 同时受 db-1 的硬亲和（+1）和 log-1 的软反亲和（−30）影响，成为最低分。min-max 之后分差被拉满到 0～100，再乘以权重 2。

## 八、EventsToRegister 与 QueueingHint

```go
// pkg/scheduler/framework/plugins/interpodaffinity/plugin.go
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Add | fwk.UpdatePodLabel | fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodChange},
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodLabel}, QueueingHintFn: pl.isSchedulableAfterTargetPodUpdateLabel},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
```

`isSchedulableAfterAssignedPodChange` 的判断与三张表一一对应：

- 变化的 Pod 既没绑定也没 nominate：`QueueSkip`。
- **更新**：更新前不满足、更新后满足新 Pod 的全部亲和项，`Queue`；更新前匹配、更新后不再匹配新 Pod 的任一反亲和项，`Queue`。
- **新增**：新 Pod 满足新 Pod 的全部亲和项，`Queue`。新增 Pod 不可能让反亲和变得更容易满足，所以不检查反亲和。
- **删除**：被删的 Pod 匹配新 Pod 的反亲和，或者新 Pod 匹配被删 Pod 的反亲和，`Queue`。删除只可能让反亲和变得容易满足，两个方向都要看。

节点事件（`isSchedulableAfterNodeChange`）只注册了 Add 和 UpdateNodeLabel。亲和项方面，新节点带了亲和项的 `topologyKey`、或者节点新获得了这个标签、或者标签值变了，返回 `Queue`。反亲和项方面，**任何新节点**都返回 `Queue`（没有拓扑标签的节点天然不受反亲和约束），节点失去反亲和的 `topologyKey` 或标签值变化也返回 `Queue`。

目标 Pod 自身改标签无条件返回 `Queue`：已有 Pod 的反亲和匹配的是新 Pod 的标签，标签变了，被排斥的状态可能就解除了。

## 九、边界情况与坑

**节点缺少拓扑标签时，亲和与反亲和的结论相反。** 亲和要求节点带齐所有亲和项的 `topologyKey`，否则直接失败；反亲和在节点缺标签时视为满足（查不到拓扑对）。一个没有 zone 标签的节点能放下所有带 zone 反亲和的 Pod，这在混合了不同来源节点的集群里可能出乎意料。

**`IgnoredDuringExecution` 的两层含义。** 已有 Pod 的 required 亲和只在打分时以 `hardPodAffinityWeight` 体现，不会阻止新 Pod 放在任何地方；Pod 调度后标签变化导致的规则违反，调度器也不会处理。API 中预留的 `RequiredDuringSchedulingRequiredDuringExecution` 至今没有实现，`GetPodAffinityTerms` 里还留着注释掉的代码块。

**Deferred resize 的 Pod 直接跳过。** 与 `PodTopologySpread` 一样，原地扩容被推迟的已绑定 Pod 重新进入调度时，PreFilter 返回 `Skip`、Filter 放行（[第 17 篇](/posts/sched-plugin-17-deferred-pod-scheduling/)）。

**带 Pod 亲和的 Pod 不能批量调度。** `SignPod` 对有 `podAffinity`/`podAntiAffinity` 的 Pod 返回 `Unschedulable`（拒签）。没有规则的 Pod 也不是无条件可签：默认配置下签名里要包含 Pod 的全部标签（`LabelsSignerName`），因为已有 Pod 的规则可能匹配这些标签；只有设置了 `ignorePreferredTermsOfExistingPods` 才省掉这一项。

**namespace 不存在时按空标签处理。** `GetNamespaceLabelsSnapshot` 取不到 namespace 时返回 nil，已有 Pod 用 `namespaceSelector` 写的规则就匹配不到新 Pod。正常情况下 Pod 所在的 namespace 必然存在，这只在 informer 延迟时短暂出现。

## 十、发散：开销从哪里来

把各阶段的工作量放在一起看（N 为节点数，P 为 Pod 总数，T 为规则条数）：

| 阶段 | gate 关闭时的工作量 | 触发条件 |
| --- | --- | --- |
| PreFilter：已有 Pod 反亲和 | 有 required 反亲和 Pod 的节点上，这些 Pod 的规则 × 1 次匹配 | 总是执行 |
| PreFilter：新 Pod 规则 | O(P × T)，全部节点全部 Pod | 新 Pod 有 required 规则 |
| Filter：已有 Pod 反亲和 | 每个候选节点 O(节点标签数) | 表非空 |
| Filter：新 Pod 规则 | 每个候选节点 O(规则数) | 新 Pod 有 required 规则 |
| PreScore | O(P × T) 或只看有亲和的 Pod | 新 Pod 有 preferred 规则 / 否则 |

真正昂贵的是两个 O(P × T) 的全集群扫描。一个 5000 节点、15 万 Pod 的集群里，一个带 required 亲和的 Pod 每个调度周期要做数十万次 selector 匹配；`ParallelizeUntil` 把它摊到 16 个 worker（默认并行度）上，依然是调度延迟里最显眼的一项。Kubernetes 官方文档长期写着「不建议在超过数百个节点的集群中使用 Pod 间亲和」，原因就在这里。

几条实际的建议：

- 能用 `PodTopologySpread` 表达的需求（均衡分布、每个域一个）优先用它，PreFilter 的匹配次数是 O(约束数 × P)，并且没有「已有 Pod 规则」这一半的工作。
- 只需要「一节点一副本」时，用 hostname 反亲和，并关注 `InterPodAffinityHostnameFastPath` 的进展。
- 集群里大量 Pod 只是为了「打分时靠近某些服务」而写 preferred 亲和时，它们会让**所有**新 Pod 的 PreScore 都要遍历这些 Pod。如果这种对称性不是必需的，可以开启 `ignorePreferredTermsOfExistingPods`，让没有亲和规则的 Pod 完全跳过打分。
- 避免在 required 亲和里使用过宽的 selector（比如匹配整个 namespace 的 Pod），它会让 `clusterWideAffinityCounts` 在几乎所有拓扑对上都有计数，失去筛选意义，开销却一点不少。

## 小结

- 规则的 namespace 处理分两个方向：新 Pod 的 `namespaceSelector` 被展开成 namespace 列表，已有 Pod 的规则只配合新 Pod 一个 namespace 的标签做匹配。
- PreFilter 产出三张以 `topologyPair` 为键的计数表：已有 Pod 反亲和、新 Pod 亲和、新 Pod 反亲和；并行阶段用切片收集局部结果，最后合并成 map，计数归零即删除键。
- Filter 按「新 Pod 亲和 → 新 Pod 反亲和 → 已有 Pod 反亲和」检查。亲和失败是 `UnschedulableAndUnresolvable`（删 Pod 补不出缺的 Pod），反亲和失败是 `Unschedulable`，但只有冲突 Pod 恰好在候选节点上时抢占才有用。
- 自亲和特例只在「全集群没有任何匹配 Pod」时放行第一个副本。
- `InterPodAffinityHostnameFastPath`（1.37 Alpha）把 hostname 规则从全局表中拆出来，在 Filter 里只看本节点 Pod；亲和项必须全部是 hostname 才能拆，反亲和项逐条拆。
- Score 是对称的：已有 Pod 的 preferred 规则和 required 亲和（以 `hardPodAffinityWeight`，默认 1）也会影响新 Pod 的落点；归一化是标准 min-max。

下一篇是另一类「不看 requests 的资源」——[DynamicResources](/posts/sched-plugin-15-dynamic-resources/)。
