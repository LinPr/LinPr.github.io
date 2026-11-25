---
title: 'scheduler-plugins 06 [in-tree] NodeAffinity：三段 AND 与一个能缩小候选集的 PreFilter'
date: 2026-11-19T09:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "NodeAffinity", "nodeSelector", "源码分析"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NodeAffinity 把 nodeSelector、required/preferred 节点亲和和调度器级的 addedAffinity 合在一个插件里。本文按源码拆解它的 AND/OR 语义、PreFilter 如何从 metadata.name 推导 PreFilterResult、Filter 为什么一律返回不可抢占的状态码、Score 的权重求和与归一化，以及只在标签从不匹配变为匹配时才唤醒 Pod 的 QueueingHint。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `NodeAffinity` 实现的扩展点（PreFilter、Filter、PreScore、Score、NormalizeScore）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/06.svg" link="/images/posts/sched-plugins/callstack/06.svg" caption="NodeAffinity 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-05-taint-toleration/)的 `TaintToleration` 是从节点的角度排斥 Pod：节点打了污点，Pod 没有容忍就进不来。`NodeAffinity` 方向相反，是 Pod 自己挑节点：`nodeSelector` 和 `affinity.nodeAffinity` 写明了要什么样的标签，不满足的节点直接出局。

本文基于 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期）。`NodeAffinity` 在默认 Profile 的 `MultiPoint` 里排第六，紧跟 `TaintToleration`，打分权重为 2。

它同时实现了 PreFilter、Filter、PreScore、Score 四个扩展点，外加 `EnqueueExtensions` 和用于批量调度的 `SignPlugin`。它不受任何 feature gate 控制，默认始终启用，只有 in-place 扩缩容相关的 `InPlacePodVerticalScalingSchedulerPreemption` 会让它对一类特殊 Pod 直接跳过（见 3.6 节）。

```go {linenos=inline}
// pkg/scheduler/apis/config/v1/default_plugins.go
{Name: names.TaintToleration, Weight: ptr.To[int32](3)},
{Name: names.NodeAffinity, Weight: ptr.To[int32](2)},
{Name: names.NodePorts},
```

## 一、它处理哪些约束

一个 Pod 能对节点标签提出三类要求，`NodeAffinity` 把它们都收进来：

```yaml {linenos=inline}
apiVersion: v1
kind: Pod
metadata:
  name: web
spec:
  nodeSelector:                 # ① 精确匹配，所有键值都要满足
    kubernetes.io/os: linux
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:   # ② 硬约束，term 之间 OR
        nodeSelectorTerms:
        - matchExpressions:
          - {key: topology.kubernetes.io/zone, operator: In, values: [cn-a, cn-b]}
          - {key: node.example.com/gpu, operator: DoesNotExist}
        - matchExpressions:
          - {key: node-role.example.com/edge, operator: Exists}
      preferredDuringSchedulingIgnoredDuringExecution:  # ③ 软偏好，按权重加分
      - weight: 80
        preference:
          matchExpressions:
          - {key: disktype, operator: In, values: [ssd]}
      - weight: 20
        preference:
          matchExpressions:
          - {key: topology.kubernetes.io/zone, operator: In, values: [cn-a]}
  containers:
  - name: app
    image: nginx:1.27
```

翻译成文字：节点必须是 Linux，并且「位于 cn-a/cn-b 且没有 GPU 标签」或者「是边缘节点」；在满足这些的节点中，SSD 节点加 80，cn-a 节点加 20。

此外还有第四个来源：调度器配置里的插件参数 `addedAffinity`，管理员可以用它给某个 Profile 下的所有 Pod 追加一层节点亲和，Pod 自己看不到这层约束。

`IgnoredDuringExecution` 这个后缀要当真：**调度器只在调度时检查一次，Pod 运行后节点标签被改掉，不会有任何组件因此驱逐它。**

几类约束怎么组合成一次判断，是一套固定的 AND/OR 规则。先看语义，再看代码。

## 二、语义：谁和谁是 AND，谁和谁是 OR

真正的匹配逻辑不在插件包里，而在 `k8s.io/component-helpers/scheduling/corev1/nodeaffinity`。kubelet 准入、DaemonSet 控制器、调度器用的是同一份代码，这保证了「调度器认为能放」和「kubelet 认为能跑」的判断一致。

{{< figure src="/images/posts/sched-plugins/06-node-affinity-semantics.svg" caption="required 语义的三段 AND、term 间 OR，以及 PreFilter 从 metadata.name 推导候选集合的三种结果（点击查看大图）" link="/images/posts/sched-plugins/06-node-affinity-semantics.svg" >}}

### 2.1 nodeSelector 与 required 亲和：AND

`GetRequiredNodeAffinity` 把 Pod 的两个字段解析成一个结构：

```go {linenos=inline}
// staging/src/k8s.io/component-helpers/scheduling/corev1/nodeaffinity/nodeaffinity.go
type RequiredNodeAffinity struct {
	labelSelector labels.Selector
	nodeSelector  *LazyErrorNodeSelector
}

func NewRequiredNodeAffinity(nodeSelector map[string]string, affinity *v1.Affinity) RequiredNodeAffinity {
	var selector labels.Selector
	if len(nodeSelector) > 0 {
		selector = labels.SelectorFromSet(nodeSelector)
	}
	// Use LazyErrorNodeSelector for backwards compatibility of parsing errors.
	var lazyNodeSelector *LazyErrorNodeSelector
	if affinity != nil &&
		affinity.NodeAffinity != nil &&
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		lazyNodeSelector = NewLazyErrorNodeSelector(affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
	}
	return RequiredNodeAffinity{labelSelector: selector, nodeSelector: lazyNodeSelector}
}

func (s RequiredNodeAffinity) Match(node *v1.Node) (bool, error) {
	if s.labelSelector != nil {
		if !s.labelSelector.Matches(labels.Set(node.Labels)) {
			return false, nil
		}
	}
	if s.nodeSelector != nil {
		return s.nodeSelector.Match(node)
	}
	return true, nil
}
```

**`nodeSelector` 和 required 亲和是 AND。** `Match`（第 22–32 行）分两步：

1. 先查 `nodeSelector`，不满足立刻返回；
2. 满足了再查 required 亲和。

`nodeSelector` 被转成 `labels.SelectorFromSet`（第 9–11 行），map 里每个键值都是一个等值条件，彼此也是 AND。

`nodeSelector` 只是这一层里的一个因子。required 亲和内部还有两层 AND/OR，下一节拆开。

### 2.2 term 之间 OR，term 内部 AND

```go {linenos=inline}
// staging/src/k8s.io/component-helpers/scheduling/corev1/nodeaffinity/nodeaffinity.go
func (ns *LazyErrorNodeSelector) Match(node *v1.Node) (bool, error) {
	if node == nil {
		return false, nil
	}
	nodeLabels := labels.Set(node.Labels)
	nodeFields := extractNodeFields(node)

	var errs []error
	for _, term := range ns.terms {
		match, tErrs := term.match(nodeLabels, nodeFields)
		if len(tErrs) > 0 {
			errs = append(errs, tErrs...)
			continue
		}
		if match {
			return true, nil
		}
	}
	return false, errors.Flatten(errors.NewAggregate(errs))
}

func (t *nodeSelectorTerm) match(nodeLabels labels.Set, nodeFields fields.Set) (bool, []error) {
	if t.parseErrs != nil {
		return false, t.parseErrs
	}
	if t.matchLabels != nil && !t.matchLabels.Matches(nodeLabels) {
		return false, nil
	}
	if t.matchFields != nil && len(nodeFields) > 0 && !t.matchFields.Matches(nodeFields) {
		return false, nil
	}
	return true, nil
}
```

匹配分两层：

1. term 之间是 OR：遍历 term（第 10–19 行），任何一个命中就返回 `true`；
2. 单个 term 内是 AND：`matchExpressions` 被转成一个 `labels.Selector`（多条 requirement 叠加即 AND），`matchFields` 被转成 `fields.AndSelectors`，两者又是 AND（第 23–34 行）。

三个细节决定了实际行为：

1. 空 term 不匹配任何节点。`NewLazyErrorNodeSelector` 解析时会跳过 `matchExpressions` 和 `matchFields` 都为空的 term（注释写的是 `nil or empty term selects no objects`）。所以写一个空 term 想表达「任意节点」是行不通的，它等于什么都没写，如果这是唯一的 term，Pod 哪儿也去不了。
2. `matchFields` 只认 `metadata.name`。`extractNodeFields` 只往 `fields.Set` 里放了这一个键；API 校验也只允许 `In`/`NotIn`，并且必须恰好一个值。
3. 解析错误是「懒」的。名字里的 `Lazy` 指只有在没有任何 term 命中时才把解析错误返回。这是为了兼容历史上 apiserver 校验不严时存进 etcd 的非法值（校验代码里至今有 `allowInvalidLabelValueInRequiredNodeAffinity` 这样的开关）。**一个 term 写错了，不影响其他 term 正常匹配。**

操作符方面：

1. `In`/`NotIn`/`Exists`/`DoesNotExist` 对应 `selection` 包里的同名操作；
2. `Gt`/`Lt` 会把标签值当整数比较；
3. `NotIn` 和 `DoesNotExist` 对没有这个标签的节点返回 true，这经常被用来「排除某类节点」。

这些规则都在 component-helpers 的 `nodeaffinity` 包里实现，插件在它外面加上 PreFilterResult 的推导和打分的归一化。下面按扩展点顺序看插件这一层。

## 三、源码按扩展点拆

重点在两处：PreFilter 怎么缩小候选集，Score 怎么归一化。

### 3.1 结构体、工厂与参数

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
type NodeAffinity struct {
	handle                                             fwk.Handle
	addedNodeSelector                                  *nodeaffinity.NodeSelector
	addedPrefSchedTerms                                *nodeaffinity.PreferredSchedulingTerms
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}
```

插件自身只持有 `addedAffinity` 的两半，Pod 的约束每个调度周期现算。`New` 在启动时把参数解析好：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
func New(_ context.Context, plArgs runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	args, err := getArgs(plArgs)
	if err != nil {
		return nil, err
	}
	pl := &NodeAffinity{
		handle: h,
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
	}
	if args.AddedAffinity != nil {
		if ns := args.AddedAffinity.RequiredDuringSchedulingIgnoredDuringExecution; ns != nil {
			pl.addedNodeSelector, err = nodeaffinity.NewNodeSelector(ns)
			// ...
		}
		// TODO: parse requiredDuringSchedulingRequiredDuringExecution when it gets added to the API.
		if terms := args.AddedAffinity.PreferredDuringSchedulingIgnoredDuringExecution; len(terms) != 0 {
			pl.addedPrefSchedTerms, err = nodeaffinity.NewPreferredSchedulingTerms(terms)
			// ...
		}
	}
	return pl, nil
}
```

注意这里用的解析函数：

1. required 部分是 `NewNodeSelector`（第 13 行），不是 Pod 用的 `NewLazyErrorNodeSelector`；
2. preferred 部分用的是 `NewPreferredSchedulingTerms`（第 18 行）。

**管理员写的配置没有历史包袱，任何解析错误都会在 `ValidateNodeAffinityArgs` 阶段直接让调度器启动失败。** `NodeAffinityArgs` 只有一个字段 `AddedAffinity`，默认为空，表示所有节点都满足。

一个典型用法是让第二个 Profile 只调度到特定节点池：

```yaml {linenos=inline}
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: batch-scheduler
  pluginConfig:
  - name: NodeAffinity
    args:
      addedAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - {key: pool, operator: In, values: [batch]}
```

`schedulerName: batch-scheduler` 的 Pod 都会多一道看不见的约束。类型定义的注释里专门提醒：DaemonSet 这类亲和到具体节点的 Pod，如果目标节点不满足 `addedAffinity`，会一直 Pending。

参数在启动时就准备好了。接下来是每个调度周期都会走的 PreFilter。

### 3.2 PreFilter：解析一次，顺便缩小候选集

PreFilter 要做三件事：判断能不能跳过、把约束解析一次、顺便缩小候选集。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
func (pl *NodeAffinity) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	affinity := pod.Spec.Affinity
	noNodeAffinity := (affinity == nil ||
		affinity.NodeAffinity == nil ||
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil)
	if noNodeAffinity && pl.addedNodeSelector == nil && pod.Spec.NodeSelector == nil {
		// NodeAffinity Filter has nothing to do with the Pod.
		return nil, fwk.NewStatus(fwk.Skip)
	}

	state := &preFilterState{requiredNodeSelectorAndAffinity: nodeaffinity.GetRequiredNodeAffinity(pod)}
	cycleState.Write(preFilterStateKey, state)

	if noNodeAffinity || len(affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
		return nil, nil
	}
	// ...
}
```

前半段做两件事。

第一，能跳就跳。没有 required 亲和、没有 `nodeSelector`、也没有 `addedAffinity` 的 Pod 返回 `Skip`（第 7–13 行）。框架会把插件名记进 CycleState 的 `skipFilterPlugins`，这个周期里它的 Filter 不会在任何节点上被调用。**集群里大多数 Pod 不写节点亲和，这条路径省掉了「节点数 × 一次空函数调用」。**

第二，解析结果放进 CycleState。key 是 `"PreFilterNodeAffinity"`，值是：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
type preFilterState struct {
	requiredNodeSelectorAndAffinity nodeaffinity.RequiredNodeAffinity
}

// Clone just returns the same state because it is not affected by pod additions or deletions.
func (s *preFilterState) Clone() fwk.StateData {
	return s
}
```

`Clone` 直接返回自身。CycleState 在抢占模拟、nominated Pod 二次过滤时会被克隆，但这份状态只取决于待调度 Pod 和节点标签，与节点上有哪些 Pod 无关，没有必要深拷贝。同理 `PreFilterExtensions()` 返回 `nil`：抢占时往节点上增删 Pod，不会改变节点亲和的结论。

后半段从 required 亲和里推导候选节点：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
	// Check if there is affinity to a specific node and return it.
	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	var nodeNames sets.Set[string]
	for _, t := range terms {
		var termNodeNames sets.Set[string]
		for _, r := range t.MatchFields {
			if r.Key == metav1.ObjectNameField && r.Operator == v1.NodeSelectorOpIn {
				// The requirements represent ANDed constraints, and so we need to
				// find the intersection of nodes.
				s := sets.New(r.Values...)
				if termNodeNames == nil {
					termNodeNames = s
				} else {
					termNodeNames = termNodeNames.Intersection(s)
				}
			}
		}
		if termNodeNames == nil {
			// If this term has no node.Name field affinity,
			// then all nodes are eligible because the terms are ORed.
			return nil, nil
		}
		nodeNames = nodeNames.Union(termNodeNames)
	}
	// If nodeNames is not nil, but length is 0, it means each term have conflicting affinity to node.Name;
	// therefore, pod will not match any node.
	if nodeNames != nil && len(nodeNames) == 0 {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, errReasonConflict)
	} else if len(nodeNames) > 0 {
		return &fwk.PreFilterResult{NodeNames: nodeNames}, nil
	}
	return nil, nil
```

它把 required 亲和里的 `metadata.name In [...]` 提取出来，和语义保持一致：

1. term 内取交集（第 7–17 行，AND）；
2. term 间取并集（第 24 行，OR）。

**只要有一个 term 不含节点名条件，那个 term 可能被任何节点命中，就不能缩小，直接返回 `nil`（第 19–23 行）。** 注意 `nodeNames` 有 `nil` 和「非 nil 但为空」两种状态，最后的返回分两种情况：

1. 「非 nil 但为空」说明每个 term 的节点名条件内部自相矛盾，返回 `UnschedulableAndUnresolvable`（第 28–29 行），原因串 `pod affinity terms conflict`；
2. 能确定集合时返回 `PreFilterResult`（第 30–31 行）。

`NotIn` 不参与推导：排除一个节点名并不能把候选集缩小到一个可枚举的集合。

这个优化的主要受益者是 DaemonSet。DaemonSet 控制器为每个节点创建 Pod 时，会把 Pod 的 required 亲和整个替换成指向单个节点的 term：

```go {linenos=inline}
// pkg/controller/daemon/util/daemonset_util.go
func ReplaceDaemonSetPodNodeNameNodeAffinity(affinity *v1.Affinity, nodename string) *v1.Affinity {
	nodeSelReq := v1.NodeSelectorRequirement{
		Key:      metav1.ObjectNameField,
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{nodename},
	}
	// ...
	// Replace node selector with the new one.
	nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = []v1.NodeSelectorTerm{
		{
			MatchFields: []v1.NodeSelectorRequirement{nodeSelReq},
		},
	}
	return affinity
}
```

5000 个节点的集群里，一个 DaemonSet Pod 原本要在 5000 个节点上跑一遍全部 Filter 插件，有了 `PreFilterResult` 只需跑一个。框架在 `RunPreFilterPlugins` 里把各插件的结果用 `Merge` 求交集，`getCandidateNodes` 再把不在集合里的节点统一标记为：

```go {linenos=inline}
// pkg/scheduler/algorithm.go
diagnosis.NodeToStatus.SetAbsentNodesStatus(fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node(s) didn't satisfy plugin(s) %v", sets.List(diagnosis.UnschedulablePlugins))))
```

被排除的节点是「不可解决」的，抢占不会在它们上面尝试。这也是 DaemonSet Pod 因资源不足而 Pending 时，抢占只会在目标节点上找受害者的原因。

抢占挑节点的方式决定了这一点：DefaultPreemption 的 `findCandidates`（`pkg/scheduler/framework/preemption/preemption.go`）只通过 `NodesForStatusCode(..., fwk.Unschedulable)` 取状态为 `Unschedulable` 的节点做模拟，被标成 `UnschedulableAndUnresolvable` 的节点进不了这个列表（[NodeName](/posts/sched-plugin-03-node-name/) 一篇 3.2 节有完整代码）。

候选集缩完之后，才轮到 Filter 逐个节点回答。

### 3.3 Filter：两种原因，同一个状态码

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
func (pl *NodeAffinity) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	node := nodeInfo.Node()

	if pl.addedNodeSelector != nil && !pl.addedNodeSelector.Match(node) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, errReasonEnforced)
	}

	s, err := getPreFilterState(state)
	if err != nil {
		// Fallback to calculate requiredNodeSelector and requiredNodeAffinity
		// here when PreFilter is disabled.
		s = &preFilterState{requiredNodeSelectorAndAffinity: nodeaffinity.GetRequiredNodeAffinity(pod)}
	}

	// Ignore parsing errors for backwards compatibility.
	match, _ := s.requiredNodeSelectorAndAffinity.Match(node)
	if !match {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonPod)
	}

	return nil
}
```

先查管理员的 `addedAffinity`（第 8–10 行），再查 Pod 自己的约束（第 20–23 行），两种失败的原因串不同：

| 失败来源 | 状态码 | 原因串 |
| --- | --- | --- |
| `addedAffinity` 不满足 | `UnschedulableAndUnresolvable` | `node(s) didn't match scheduler-enforced node affinity` |
| `nodeSelector` / required 亲和不满足 | `UnschedulableAndUnresolvable` | `node(s) didn't match Pod's node affinity/selector` |

**状态码都是 `UnschedulableAndUnresolvable`。** 道理很直接：节点标签是节点的属性，驱逐节点上的任何 Pod 都改变不了它，所以抢占（[DefaultPreemption](/posts/sched-plugin-18-default-preemption/)）在挑候选节点时会直接跳过这些节点（3.2 节讲过，它只在 `Unschedulable` 的节点上找受害者）。

Filter 里还有一个兜底：读不到 PreFilter 状态时现场解析。这是为「在配置里禁用了 NodeAffinity 的 PreFilter 但保留 Filter」这种组合准备的。正常情况下 PreFilter 返回 `Skip` 时 Filter 根本不会被调用，走不到这条分支。

评分阶段换了一套输入：只看 preferred 亲和，不再看能不能去。

### 3.4 PreScore 与 Score：权重求和

打分只关心 preferred 亲和和 `addedAffinity` 里的 preferred 部分，两者的处理方式都是权重求和。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
func (pl *NodeAffinity) PreScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	preferredNodeAffinity, err := getPodPreferredNodeAffinity(pod)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if preferredNodeAffinity == nil && pl.addedPrefSchedTerms == nil {
		// NodeAffinity Score has nothing to do with the Pod.
		return fwk.NewStatus(fwk.Skip)
	}
	state := &preScoreState{
		preferredNodeAffinity: preferredNodeAffinity,
	}
	cycleState.Write(preScoreStateKey, state)
	return nil
}

func (pl *NodeAffinity) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	node := nodeInfo.Node()

	var count int64
	if pl.addedPrefSchedTerms != nil {
		count += pl.addedPrefSchedTerms.Score(node)
	}
	// ...（读 preScoreState，读不到时现场计算）
	if s.preferredNodeAffinity != nil {
		count += s.preferredNodeAffinity.Score(node)
	}

	return count, nil
}
```

没有 preferred 亲和的 Pod 在 PreScore 返回 `Skip`（第 7–10 行），Score 和 NormalizeScore 都不会执行，这个插件在总分里贡献 0。和 PreFilter 不同，PreScore 遇到解析错误是直接报错的（`fwk.AsStatus(err)`）。`NewPreferredSchedulingTerms` 不是 lazy 的，对 term 的处理分两种：

1. 权重为 0 或 preference 为空的 term 会被静默跳过；
2. **有解析错误的 term 会让整个调度周期失败**。

单个节点的原始分是命中 term 的权重之和（第 6–11 行逐个 term 判断，命中就把 `weight` 累加）：

```go {linenos=inline}
// staging/src/k8s.io/component-helpers/scheduling/corev1/nodeaffinity/nodeaffinity.go
func (t *PreferredSchedulingTerms) Score(node *v1.Node) int64 {
	var score int64
	nodeLabels := labels.Set(node.Labels)
	nodeFields := extractNodeFields(node)
	for _, term := range t.terms {
		// parse errors are reported in NewPreferredSchedulingTerms.
		if ok, _ := term.match(nodeLabels, nodeFields); ok {
			score += int64(term.weight)
		}
	}
	return score
}
```

API 限制每个 term 的 `weight` 在 1 到 100 之间，多个 term 加起来很容易超过 100，所以必须归一化（`reverse=false`，第 3 行）：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
func (pl *NodeAffinity) NormalizeScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	return helper.DefaultNormalizeScore(fwk.MaxNodeScore, false, scores)
}
```

`DefaultNormalizeScore` 的做法是 `score = 100 × score / max(scores)`。用第一节的 Pod 算一遍，假设 Filter 之后剩四个节点：

| 节点 | 标签 | 原始分 | 归一化 | ×权重 2 |
| --- | --- | --- | --- | --- |
| n1 | ssd、cn-a | 80 + 20 = 100 | 100 × 100 / 100 = 100 | 200 |
| n2 | ssd、cn-b | 80 | 80 | 160 |
| n3 | hdd、cn-a | 20 | 20 | 40 |
| n4 | hdd、cn-b | 0 | 0 | 0 |

如果把权重换成 30 和 10，原始分变成 40、30、10、0，最大值 40 被放大到 100，结果是 100、75、25、0。可以看出两点：

1. **归一化之后只有相对权重有意义**：30/10 和 75/25 打出的分完全一样，把 preferred 权重全部乘以 2 不会让节点亲和在总分里变得更重要。要调整它和其他插件的相对重要性，只能改插件权重。
2. 归一化以「当前这批可行节点里的最大值」为基准。如果所有可行节点都命中同样的 term，它们都得 100，这个插件对排序没有任何贡献；如果都不命中，都得 0。

### 3.5 EventsToRegister 与 QueueingHint

被 `NodeAffinity` 拒绝的 Pod，只有节点变了才有希望：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
func (pl *NodeAffinity) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
	}, nil
}
```

只订阅节点新增和标签变更两类事件（第 4 行）。框架在 `extractNodeLabelsChange` 里用 `equality.Semantic.DeepEqual` 比较新旧标签，只有标签真的变了才产生 `UpdateNodeLabel`。节点心跳、condition、allocatable 变化都不会触发这个插件的 hint。

hint 函数按顺序判断四步：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
func (pl *NodeAffinity) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalNode, modifiedNode, err := util.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if pl.addedNodeSelector != nil && !pl.addedNodeSelector.Match(modifiedNode) {
		// ...
		return fwk.QueueSkip, nil
	}

	requiredNodeAffinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	isMatched, err := requiredNodeAffinity.Match(modifiedNode)
	if err != nil {
		return fwk.Queue, err
	}
	if !isMatched {
		// ...
		return fwk.QueueSkip, nil
	}
	// Since the node was added and it matches the pod's affinity criteria, we can unblock it.
	if originalNode == nil {
		// ...
		return fwk.Queue, nil
	}
	// At this point we know the operation is update so we can narrow down the criteria to unmatch -> match changes only
	// (necessary affinity label was added to the node in this case).
	wasMatched, err := requiredNodeAffinity.Match(originalNode)
	if err != nil {
		return fwk.Queue, err
	}
	if wasMatched {
		// ...
		return fwk.QueueSkip, nil
	}
	// ...
	return fwk.Queue, nil
}
```

1. 新节点不满足 `addedAffinity`，`QueueSkip`（第 8–11 行）。
2. 新节点不满足 Pod 的 required 约束，`QueueSkip`（第 13–21 行）。
3. 节点是新增的，并且满足，`Queue`（第 23–26 行）。
4. 节点是更新的，且更新前就已经满足，`QueueSkip`（第 29–36 行）。

第 4 步是这个 hint 精确的地方。更新前就满足，说明 Pod 上次失败时 `NodeAffinity` 并没有拒绝这个节点，拒绝它的是别的插件。别的插件会通过自己注册的事件唤醒 Pod，`NodeAffinity` 不必凑热闹。**只有「不匹配 → 匹配」这一种转变才值得重试**（第 37–38 行）。

hint 里调用的是 `GetRequiredNodeAffinity(pod)`，不是 CycleState 里的缓存：hint 运行在事件处理路径上，那时没有调度周期，也就没有 CycleState。解析错误会让 hint 返回 `Queue`，这是 hint 的通用约定：拿不准就放行，宁可多调度一次，也不要让 Pod 卡死。

### 3.6 SignPod 与 deferred resize

插件实现了 `SignPlugin`：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go
func (pl *NodeAffinity) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	aff, err := fwk.NodeAffinitySigner(pod)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	return []fwk.SignFragment{
		{Key: fwk.NodeAffinitySignerName, Value: aff},
		{Key: fwk.NodeSelectorSignerName, Value: pod.Spec.NodeSelector},
	}, nil
}
```

签名用于批量调度（KEP-5598）：若干 Pod 在所有参与过滤和打分的插件看来「长得一样」，调度器可以复用上一个 Pod 的节点排序结果。`NodeAffinity` 告诉框架，它的结论只取决于节点亲和与 `nodeSelector` 两个字段（第 7–10 行的两个 `SignFragment`）。框架的约定是，**只要有一个启用了 Filter/Score 的插件没实现 `SignPlugin`，所有 Pod 都关闭批量优化**，所以每个 in-tree 插件都要老老实实交出自己的签名片段。

PreFilter 和 Filter 开头都有一段 `IsPodResizeDeferred` 判断。`InPlacePodVerticalScalingSchedulerPreemption`（1.37 Alpha，默认关闭）打开后，已经运行在节点上、原地扩容被 kubelet 以 `Deferred` 推迟的 Pod 会重新进入调度器（调度器的 Pod 更新回调发现 resize 刚变成 `Deferred` 时把它放回调度队列，见 [NodeName](/posts/sched-plugin-03-node-name/) 一篇 2.2 节），目的是在它自己所在的节点上通过抢占腾出资源。

这种 Pod 已经在节点上了，节点亲和没有重新判断的意义，所以 PreFilter 返回 `Skip`，Filter 直接放行。相关的资源计算在 [NodeResourcesFit](/posts/sched-plugin-08-node-resources-fit/) 和 [DeferredPodScheduling](/posts/sched-plugin-17-deferred-pod-scheduling/) 里。

扩展点到 `SignPod` 为止。剩下的是不走正常路径的组合。

## 四、边界情况与坑

**`spec.nodeName` 绕过的是调度器，不是节点亲和。** 直接写了 `nodeName` 的 Pod 不进调度队列，`NodeAffinity` 的 Filter 不会执行：调度器的 Pod 事件回调用 `assignedPod` 判断 `spec.nodeName` 是否非空，非空的只记进缓存（见 [NodeName](/posts/sched-plugin-03-node-name/) 一篇 2.1 节）。

但 kubelet 准入会再查一遍：

```go {linenos=inline}
// pkg/kubelet/lifecycle/predicate.go
	if matches, _ := corev1nodeaffinity.GetRequiredNodeAffinity(pod).Match(nodeInfo.Node()); !matches {
		reasons = append(reasons, &PredicateFailureError{nodeaffinity.Name, nodeaffinity.ErrReasonPod})
	}
```

不满足时 Pod 会被 kubelet 拒绝，状态为 `Failed`，reason 是 `NodeAffinity`。kubelet 在这里还有一个特殊处理：如果唯一的失败原因是节点亲和，它会同步拉一次最新的 Node 对象再判断一遍，避免因为本地缓存的标签过期而误拒。

**`metadata.name` 亲和和 `nodeName` 不是一回事。**

1. 前者仍然经过完整的调度流程，资源、端口、污点都会检查，失败了会 Pending 并且可以抢占；
2. 后者完全不经过调度器。

DaemonSet 早年用 `nodeName`，后来改为节点亲和，就是为了让 DaemonSet Pod 也能享受抢占和统一的资源判定。

**节点亲和失败不能靠抢占解决，但 PreFilterResult 会把抢占圈得更小。** `kubectl describe` 里看到 `0/100 nodes are available: 99 node(s) didn't satisfy plugin(s) [NodeAffinity], 1 Insufficient cpu`，说明 PreFilter 已经把候选集缩到一个节点，抢占也只会在这一个节点上尝试。

**`nodeSelector` 写了不存在的标签，Pod 会安静地 Pending。** 调度器不会校验标签在集群里是否存在。由于 QueueingHint 只在相关节点出现或标签变更时唤醒 Pod，给一个节点补上标签之后，Pod 会被立即重新调度，不需要等定期刷新。

**修改 Pod 的亲和不会触发这个插件的 hint。** `EventsToRegister` 里没有 Pod 事件。正常 Pod 的亲和是不可变的；带 `schedulingGates` 的 Pod 允许放宽 `nodeSelector` 和亲和，但那时它还被 [SchedulingGates](/posts/sched-plugin-01-scheduling-gates/) 挡在队列外面，不需要 hint。

**preferred 和 `TaintToleration` 的打分是两套归一化。**

1. `TaintToleration` 用反向归一化（软污点越多越差）；
2. `NodeAffinity` 用正向归一化（命中权重越多越好）。

两者权重分别是 3 和 2，一个节点同时有一个 `PreferNoSchedule` 污点且命中全部 preferred term，两项在总分里会部分抵消。

这些问题各有出处：有的在 API 校验，有的在 kubelet 准入，有的在打分公式。下面三节分别讲匹配代码放在哪里、PreFilterResult 的边界，以及一点实践建议。

## 五、发散

### 5.1 一份匹配代码，四个使用者

`component-helpers/scheduling/corev1/nodeaffinity` 是一个只依赖 `k8s.io/api` 和 `apimachinery` 的小包，使用者包括：

1. kube-scheduler 的 `NodeAffinity` 插件；
2. kubelet 准入 `generalFilter`，Pod 被直接写了 `nodeName` 时它是唯一的把关者；
3. DaemonSet 控制器判断「这个节点要不要跑 DaemonSet Pod」；
4. `NewRequiredNodeAffinity` 的注释还点名了「工作负载控制器评估 Pod 模板」这类没有 Pod 对象的调用方。

调度器的语义和其他组件不一致，会导致 Pod 被调度过去后被 kubelet 拒绝，然后控制器再创建、再调度、再被拒绝，形成循环。把匹配逻辑抽到 staging 仓库里由各组件共用，是从根上消除这种不一致。读节点亲和问题时，先确认问题出在「匹配语义」还是「调度器流程」，前者去 component-helpers 找，后者才看插件。

### 5.2 PreFilterResult 的代价与边界

`PreFilterResult` 是一个通用机制，任何插件都可以用它缩小候选集，但在 in-tree 插件里只有两个会返回非空结果：

1. `NodeName`：Pod 已经带着 `spec.nodeName` 时返回这一个节点；
2. `NodeAffinity`：日常最常命中的。

它的局限也很明显：只能从「枚举节点名」的条件里推导，`zone In [cn-a]` 这种标签条件即使只命中一个节点，也不会被推导出来。原因是调度器要从标签反查节点需要一个标签索引，而 `NodeInfo` 快照里没有这种索引，构造它的成本比直接跑 Filter 还高。

### 5.3 生产建议

1. 把「必须」和「最好」分开写：硬性的放 required，能退让的放 preferred。required 写得过严，扩容时新节点忘记打标签就会出现大面积 Pending。
2. 多 Profile 场景下，用 `addedAffinity` 比在每个工作负载上写亲和更不容易遗漏，但要记得给 DaemonSet 留出口：DaemonSet 的 Pod 默认用的是 `default-scheduler`，一般不受其他 Profile 的 `addedAffinity` 影响；如果把 DaemonSet 也指向了带 `addedAffinity` 的 Profile，节点池外的节点上那份 Pod 会一直 Pending。
3. 排查「为什么没去我想要的节点」时，先看 preferred 的绝对权重有没有意义：如果所有可行节点都命中了同样的 term，这个插件在这次调度里等于没打分。

## 六、小结

1. `NodeAffinity` 的 required 语义是 `addedAffinity` AND `nodeSelector` AND（term₁ OR term₂ …），term 内部 `matchExpressions` 与 `matchFields` 全部 AND；空 term 不匹配任何节点。
2. PreFilter 对无约束的 Pod 返回 `Skip`，并从 `metadata.name In` 条件推导 `PreFilterResult`，DaemonSet Pod 因此只在目标节点上跑 Filter；节点名条件互相矛盾时返回 `UnschedulableAndUnresolvable`。
3. Filter 的两种失败都是 `UnschedulableAndUnresolvable`，抢占不会考虑这些节点。
4. Score 是命中 preferred term 的权重和，按当前最大值正向归一化到 0 到 100，再乘以插件权重 2；只有相对权重有意义。
5. QueueingHint 只在节点新增且匹配、或标签从不匹配变为匹配时返回 `Queue`。

节点标签对上了，下一关是节点上的端口：[NodePorts](/posts/sched-plugin-07-node-ports/) 检查 `hostPort` 有没有被占用，它是默认 Filter 链里少数返回可抢占状态码的插件之一。
