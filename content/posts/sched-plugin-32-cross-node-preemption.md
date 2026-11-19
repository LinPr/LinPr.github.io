---
title: '调度插件逐个读（32）：CrossNodePreemption，一份被注释掉的跨节点抢占示例'
date: 2026-11-19T13:20:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "preemption", "PodTopologySpread"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "CrossNodePreemption 想解决 in-tree 抢占无法跨节点选受害者的问题，典型场景是 PodTopologySpread 和 Pod 反亲和。本文拆解这份已被整体注释掉的示例代码：它如何用 DFS 枚举受害者子集、为什么复杂度是 2ⁿ、实现上的若干缺陷，以及 preemption.Evaluator 的扩展点为什么容纳不下它，最后看 Kubernetes 主干如何以 PodGroup 为单位引入跨节点受害者。"
---

上一篇的 [PreemptionToleration](/posts/sched-plugin-31-preemption-toleration/) 在 in-tree 抢占的骨架上改了「谁可以被抢」。本篇的 `CrossNodePreemption` 想改的是另一个更根本的约束：受害者必须和抢占者在同一个节点上。

它在 scheduler-plugins 里的处境很特殊。本文基于 scheduler-plugins 提交 `cfeb3aa`（依赖 Kubernetes v1.36.4），在这个版本里，`cmd/scheduler/main.go` 中它的注册是注释掉的：

```go
// cmd/scheduler/main.go
		app.WithPlugin(peaks.Name, peaks.New),
		// Sample plugins below.
		// app.WithPlugin(crossnodepreemption.Name, crossnodepreemption.New),
		app.WithPlugin(podstate.Name, podstate.New),
		app.WithPlugin(qos.Name, qos.New),
```

`pkg/crossnodepreemption/` 下的 `cross_node_preemption.go`、`candidate.go` 和测试文件，正文全部包在 `/* ... */` 里，编译出来只剩一行 `package crossnodepreemption`。README 标注的成熟度是 Sample（「用于演示和启发」）。也就是说，它不会出现在任何可运行的调度器里，没有扩展点、没有默认权重、没有 feature gate 可言。

但它值得读。它清楚地暴露了 in-tree 抢占模型的边界，而 Kubernetes 主干最近几个版本正在以另一种方式跨过这条边界。本文涉及主干的部分以 master 提交 `aabdf5a2` 为准。

## 一、它要解决什么问题

[DefaultPreemption](/posts/sched-plugin-18-default-preemption/) 的模型是：对每个候选节点，只在这个节点上找低优先级 Pod，移除它们，看抢占者能否放下。这个模型隐含了一个假设：抢占者放不下，原因在这个节点本身，资源不够、端口冲突之类。

有一类约束不满足这个假设。PodTopologySpread 的 skew 是按拓扑域计数的，一个 zone 里的匹配 Pod 分布在多个节点上；Pod 间反亲和以 `topologyKey` 为单位，同一 zone 里任何节点上的匹配 Pod 都会导致冲突。这类约束失败时，「罪魁祸首」可能在别的节点上。

README 引用了早期抢占设计文档里「Supporting cross-node preemption」一节，并给出了目标：

```text
// pkg/crossnodepreemption/README.md
In contrast to the "same-node-preemption" strategy, we can come up with a "cross-node-preemption"
strategy to preempt Pods across multiple nodes, which is useful when a Pod cannot be
scheduled due to "cross node" constraints such as PodTopologySpread and PodAntiAffinity.
```

被注释掉的测试给了一个具体场景：

```go
// pkg/crossnodepreemption/cross_node_preemption_test.go（已注释）
			name: "resolve PodTopologySpread constraint",
			pod: st.MakePod().Name("p").UID("p").Label("foo", "").Priority(highPriority).
				SpreadConstraint(1, "zone", v1.DoNotSchedule, fooSelector).Obj(),
			pods: []*v1.Pod{
				st.MakePod().Name("pod-a").UID("pod-a").Node("node-a").Label("foo", "").Obj(),
				st.MakePod().Name("pod-b").UID("pod-b").Node("node-b").Label("foo", "").Obj(),
				st.MakePod().Name("pod-x").UID("pod-x").Node("node-x").Priority(highPriority).Obj(),
			},
			nodes: []*v1.Node{
				st.MakeNode().Name("node-a").Label("zone", "zone1").Label("node", "node-a").Obj(),
				st.MakeNode().Name("node-b").Label("zone", "zone1").Label("node", "node-b").Obj(),
				st.MakeNode().Name("node-x").Label("zone", "zone2").Label("node", "node-x").Capacity(onePodRes).Obj(),
			},
```

用 YAML 的语言说：抢占者 `p` 带 `foo` 标签、优先级 100，要求按 zone 分布、`maxSkew: 1`、`whenUnsatisfiable: DoNotSchedule`。zone1 的 node-a、node-b 各有一个带 `foo` 的低优先级 Pod；zone2 只有 node-x，它的 `pods` 容量是 1，已经被一个优先级同为 100 的 `pod-x` 占满。

{{< figure src="/images/posts/sched-plugins/32-why-cross-node.svg" link="/images/posts/sched-plugins/32-why-cross-node.svg" caption="zone1 的 foo 计数来自两个节点，单节点抢占每次只能移除其中一个；只有同时移除 pod-a 与 pod-b，p 才能放进 zone1（点击查看大图）" >}}

逐个节点看 in-tree 抢占能做什么：

- node-x：唯一的 Pod 是 `pod-x`，优先级不低于 `p`，不是候选受害者，`SelectVictimsOnNode` 返回 `UnschedulableAndUnresolvable`。
- node-a：候选受害者只有 `pod-a`。移除后 zone1 还剩 `pod-b`，把 `p` 放进来，zone1 计数 2、zone2 计数 0，skew 为 2，超过 1，Filter 失败。
- node-b：与 node-a 对称，同样失败。

三个节点都失败，`p` 无法调度。而只要同时驱逐 `pod-a` 和 `pod-b`，zone1 计数变为 1，skew 为 1，`p` 放在 node-a 或 node-b 上都满足约束。测试期望的结果正是这两个候选。

反亲和的第二个测试用例结构相同：`p` 对带 `foo` 的 Pod 有 zone 级别的 required 反亲和，zone1 里两个节点上都有 `foo`，必须两个一起驱逐，`p` 才能进入 zone1。

## 二、实现：枚举所有受害者组合

### 2.1 PostFilter 的骨架

```go
// pkg/crossnodepreemption/cross_node_preemption.go（已注释）
// PostFilter invoked at the postFilter extension point.
func (pl *CrossNodePreemption) PostFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, m framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	nnn, status := pl.preempt(ctx, state, pod, m)
	if !status.IsSuccess() {
		return nil, status
	}
	// This happens when the pod is not eligible for preemption or extenders filtered all candidates.
	if nnn == "" {
		return nil, framework.NewStatus(framework.Unschedulable)
	}
	return &framework.PostFilterResult{NominatedNodeName: nnn}, framework.NewStatus(framework.Success)
}
```

`preempt` 的五个步骤和 in-tree 当年的 `DefaultPreemption.preempt` 一一对应：

```go
// pkg/crossnodepreemption/cross_node_preemption.go（已注释）
	// 1) Ensure the preemptor is eligible to preempt other pods.
	if !dp.PodEligibleToPreemptOthers(pod, nodeLister, m[pod.Status.NominatedNodeName]) {
		klog.V(5).InfoS("Pod is not eligible for more preemption", "pod", klog.KObj(pod))
		return "", nil
	}

	// 2) Find all preemption candidates.
	candidates, status := FindCandidates(ctx, state, pod, m, pl.fh, nodeLister)
	if !status.IsSuccess() {
		return "", status
	}
	// ...
	// 3) Interact with registered Extenders to filter out some candidates if needed.
	candidates, status = dp.CallExtenders(pl.fh.Extenders(), pod, nodeLister, candidates)
	// ...
	// 4) Find the best candidate.
	bestCandidate := dp.SelectCandidate(candidates)
	// ...
	// 5) Perform preparation work before nominating the selected candidate.
	if status := dp.PrepareCandidate(bestCandidate, pl.fh, cs, pod, pl.Name()); !status.IsSuccess() {
		return "", status
	}
```

除了第二步换成了自己的 `FindCandidates`，其余都直接调用 `defaultpreemption` 包当时导出的包级函数。这一点决定了它为什么现在编译不过，第四节再说。

### 2.2 FindCandidates：先收集，再暴力搜索

```go
// pkg/crossnodepreemption/cross_node_preemption.go（已注释）
func FindCandidates(ctx context.Context, state *framework.CycleState, pod *v1.Pod, m framework.NodeToStatusMap,
	fh framework.Handle, nodeLister framework.NodeInfoLister) ([]dp.Candidate, *framework.Status) {
	allNodes, err := nodeLister.List()
	// ...
	potentialNodes := nodesWherePreemptionMightHelp(allNodes, m)

	// A brute-force algorithm to try ALL possible pod combinations.
	// CAVEAT: don't use this in production env.
	return bruteForceDryRunPreemption(ctx, fh, state, pod, potentialNodes, nodeLister), nil
}
```

`nodesWherePreemptionMightHelp` 排除 Filter 状态为 `UnschedulableAndUnresolvable` 的节点，其余都算潜在节点。然后在所有潜在节点上收集优先级低于抢占者的 Pod，交给 DFS：

```go
// pkg/crossnodepreemption/cross_node_preemption.go（已注释）
func bruteForceDryRunPreemption(ctx context.Context, fh framework.Handle, state *framework.CycleState,
	pod *v1.Pod, potentialNodes []*framework.NodeInfo, nodeLister framework.NodeInfoLister) []dp.Candidate {
	// Loop over <potentialNodes> and collect the pods that has lower priority than <pod>.
	priority := corev1helpers.PodPriority(pod)
	var pods []*v1.Pod
	for _, node := range potentialNodes {
		for i := range node.Pods {
			p := node.Pods[i].Pod
			if corev1helpers.PodPriority(p) < priority {
				pods = append(pods, p)
			}
		}
	}

	var path []*v1.Pod
	var result []dp.Candidate
	// We have 2^len(pods) choices in total.
	f := func(_pods []*v1.Pod) []dp.Candidate {
		return dryRunOnePass(ctx, pod, _pods, nodeLister, fh, state)
	}
	// Pass a slice pointer (&result) so as to change its elements in dfs().
	dfs(pods, 0, path, &result, f)

	return result
}

func dfs(pods []*v1.Pod, i int, path []*v1.Pod, result *[]dp.Candidate, f dryRunFunc) {
	if i >= len(pods) {
		*result = append(*result, f(path)...)
		return
	}

	// Pick, or not pick current pod.
	dfs(pods, i+1, append(path, pods[i]), result, f)
	dfs(pods, i+1, path, result, f)
}
```

`dfs` 是最朴素的子集枚举：每个 Pod 选或不选，递归到底时对当前子集调用一次 `dryRunOnePass`。注释写得很坦白：`We have 2^len(pods) choices in total.`

`append(path, pods[i])` 这种写法在 Go 里有一个经典隐患：如果 `path` 的底层数组还有空余容量，两个分支会共享底层数组，后一个分支的 `append` 可能覆盖前一个分支已经保存的元素。这里之所以没出问题，是因为 `dryRunOnePass` 在拿到 `path` 之后立即用完，生成的 candidate 里保存的是 `path` 本身（`victims: pods`），而这个切片之后可能被兄弟分支改写。在测试的两 Pod 场景里，`path` 每次 `append` 都会因容量不足而重新分配，所以测试通过；Pod 数变多后，已保存的 candidate 的受害者列表有可能被后续分支篡改。

### 2.3 dryRunOnePass：在一个子集上模拟

```go
// pkg/crossnodepreemption/cross_node_preemption.go（已注释）
func dryRunOnePass(ctx context.Context, preemptor *v1.Pod, pods []*v1.Pod, nodeLister framework.NodeInfoLister,
	fh framework.Handle, state *framework.CycleState) []dp.Candidate {
	stateCopy := state.Clone()
	var nodeCopies []*framework.NodeInfo
	// Remove all victim pods.
	for i := range pods {
		pod := pods[i]
		nodeInfo, _ := nodeLister.Get(pod.Spec.NodeName)
		nodeCopy := nodeInfo.Clone()
		nodeCopies = append(nodeCopies, nodeCopy)
		nodeCopy.RemovePod(pod)
		pInfo := framework.NewPodInfo(pod)
		fh.RunPreFilterExtensionRemovePod(ctx, stateCopy, preemptor, pInfo, nodeCopy)
	}
	// See if all Filter plugins passed.
	// NOTE: a complete search space is ALL nodes, but that would be expensive.
	var candidates []dp.Candidate
	for _, nodeInfo := range nodeCopies {
		if s := fh.RunFilterPluginsWithNominatedPods(ctx, stateCopy, preemptor, nodeInfo); s.IsSuccess() {
			candidates = append(candidates, &candidate{victims: pods, name: nodeInfo.Node().Name})
		}
	}
	return candidates
}
```

核心机制和 in-tree 一样依赖 `PreFilterExtensions`：移除受害者时调用 `RunPreFilterExtensionRemovePod`，让 PodTopologySpread、InterPodAffinity 等插件更新它们存在 CycleState 里的计数。不同的是，这里会对**不同节点**上的受害者调用 RemovePod，于是 zone1 的 foo 计数可以一次减掉两个。这正是跨节点抢占能解决拓扑约束的原因：拓扑类插件的跨节点状态都在 CycleState 里，只要对每个受害者都调用一次 RemovePod，计数就是对的。

{{< figure src="/images/posts/sched-plugins/32-brute-force.svg" link="/images/posts/sched-plugins/32-brute-force.svg" caption="两个低优先级 Pod 产生 4 个子集，只有 {pod-a, pod-b} 能让 p 放下；子集数随低优先级 Pod 数指数增长（点击查看大图）" >}}

回到测试场景，低优先级 Pod 只有 `pod-a`、`pod-b`（`pod-x` 优先级不低），DFS 依次访问四个子集：

| 子集 | 被克隆的节点 | Filter 结果 | 产出 |
| --- | --- | --- | --- |
| {pod-a, pod-b} | node-a、node-b | 两个节点上 skew 都为 1，通过 | 2 个 candidate |
| {pod-a} | node-a | zone1 剩 pod-b，skew 2，失败 | 无 |
| {pod-b} | node-b | 同上 | 无 |
| {} | 无 | 没有节点可试 | 无 |

结果与测试期望一致：两个 candidate，受害者都是 `{pod-a, pod-b}`，节点分别是 node-a 和 node-b。

### 2.4 candidate：受害者不在提名节点上

```go
// pkg/crossnodepreemption/candidate.go（已注释）
type candidate struct {
	victims []*v1.Pod
	name    string
}

// Victims returns s.victims.
func (s *candidate) Victims() *extenderv1.Victims {
	return &extenderv1.Victims{
		Pods:             s.victims,
		NumPDBViolations: 0,
	}
}
```

`name` 是提名节点，`victims` 是一组分布在多个节点上的 Pod。这在语义上已经超出了 `Victims` 结构原本的含义，它在 extender 协议里表示「这个节点上要驱逐的 Pod」。`NumPDBViolations` 固定为 0，PDB 完全没有参与。

## 三、为什么只是示例

### 3.1 复杂度

叶子数是 2ⁿ，n 是所有潜在节点上低优先级 Pod 的总数。每个叶子还要克隆若干个 `NodeInfo`、克隆一次 CycleState、对每个受害者跑一遍所有插件的 RemovePod 回调、再对每个受影响节点跑一遍完整的 Filter 链。

n 的规模不是节点内的 Pod 数，而是整个集群（排除 `UnschedulableAndUnresolvable` 节点后）的低优先级 Pod 数。n = 20 时子集数是 1,048,576；n = 30 时是 1,073,741,824。一个几十个节点、跑着批处理作业的集群，n 轻易就是几百。PostFilter 运行在调度主循环里，调度周期是串行的，一次跨节点抢占就足以让整个调度器停摆。

更糟的是，DFS 没有任何剪枝。无论前缀子集是否已经可行，都会完整展开到叶子；而且每个可行的子集都会产出 candidate。对资源类约束来说，一个可行子集的超集通常也可行，于是大量「多驱逐了几个无关 Pod」的 candidate 被生成出来交给 `SelectCandidate` 去挑。拓扑分布则更微妙：多驱逐其他 zone 的匹配 Pod 会拉低最小计数，超集反而可能不可行，这也说明简单的「找到可行解就停」并不是一个正确的剪枝条件，需要针对具体约束设计。

README 的说法是：

```text
// pkg/crossnodepreemption/README.md
> ⚠️ CAVEAT: Current implementation doesn't do any branch cutting, but uses a DFS algorithm
> to iterate all possible preemption strategies. DO NOT use it in your production env.
```

### 3.2 实现上的缺陷

即使不考虑性能，这份代码也只是「演示思路」的水平：

- **同一节点上的多个受害者。** 循环里每个受害者都 `nodeLister.Get` 再 `Clone` 一次自己的节点。如果 `pod-a1`、`pod-a2` 都在 node-a，会产生两份 node-a 副本，每份只少一个 Pod，Filter 在任何一份上都看不到「两个都移除」的效果。资源类的抢占（一个节点要腾出两个 Pod 的空间）在这里会被误判为失败。
- **只在受害者所在节点上检查。** 注释承认了这一点：`a complete search space is ALL nodes, but that would be expensive`。驱逐 zone1 的 Pod 之后，抢占者也许放在 zone1 的第三个节点上最合适，但那个节点没有受害者，不会被检查。
- **忽略 PDB。** `NumPDBViolations` 恒为 0，`SelectCandidate` 的第一条规则（PDB 违反最少）失效。
- **受害者优先级的判定过于宽松。** 只要求「低于抢占者」，没有 `PodEligibleToPreemptOthers` 之外的任何约束，也没有赦免步骤，产出的受害者集合不保证是极小的。
- **`CandidatesToVictimsMap` 的前提被打破。** 选最优 candidate 时，in-tree 的实现会把 candidate 列表转成 `map[节点名]*Victims`，同一节点的多个 candidate 只保留最后一个。v1.36.4 的源码对此有明确说明：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/framework/preemption/preemption.go
	// Same as candidatesToVictimsMap, this logic is not applicable for out-of-tree
	// preemption plugins that exercise different candidates on the same nominated node.
	if victims := victimsMap[candidateNode]; victims != nil {
```

  DFS 会为同一个节点产出很多 candidate（每个可行的超集一个），选择时它们会相互覆盖，最终被选中的未必是受害者最少的那个。

### 3.3 它为什么编译不过

代码用的是很早期的框架 API，在 v1.36.4 里几乎都不存在了：

| 示例代码中的用法 | v1.36.4 中的现状 |
| --- | --- |
| `framework.Handle`、`*framework.CycleState`、`framework.Status` | 移到 `k8s.io/kube-scheduler/framework`（代码里的 `fwk` 包），CycleState 变成接口 `fwk.CycleState` |
| `framework.NodeToStatusMap`（一个 map） | 换成接口 `fwk.NodeToStatusReader`，带 `AbsentNodesStatus` 语义 |
| `dp.PodEligibleToPreemptOthers`、`dp.CallExtenders`、`dp.SelectCandidate`、`dp.PrepareCandidate` 等包级函数 | 收进 `preemption.Evaluator` 的方法，执行部分拆到 `preemption.Executor` |
| `New(_ runtime.Object, fh framework.Handle)` | 工厂签名增加了 `context.Context` 参数 |
| `node.Pods[i].Pod` | `NodeInfo` 变成接口，用 `GetPods()` 与 `GetPod()` |

在仓库里，这类无法跟上依赖升级的示例插件，通常的处理方式就是注释掉，保留思路，不再维护。

## 四、Evaluator 的扩展点为什么容纳不下它

今天如果要写一个跨节点抢占插件，自然的想法是复用 `preemption.Evaluator`，像 [CapacityScheduling](/posts/sched-plugin-28-capacity-scheduling/) 和 PreemptionToleration 那样只实现 `preemption.Interface`。看一下 v1.36.4 的接口：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/framework/preemption/preemption.go
type Interface interface {
	// GetOffsetAndNumCandidates chooses a random offset and calculates the number of candidates that should be
	// shortlisted for dry running preemption.
	GetOffsetAndNumCandidates(nodes int32) (int32, int32)
	// CandidatesToVictimsMap builds a map from the target node to a list of to-be-preempted Pods and the number of PDB violation.
	CandidatesToVictimsMap(candidates []Candidate) map[string]*extenderv1.Victims
	// PodEligibleToPreemptOthers returns one bool and one string. The bool indicates whether this pod should be considered for
	// preempting other pods or not. The string includes the reason if this pod isn't eligible.
	PodEligibleToPreemptOthers(ctx context.Context, pod *v1.Pod, nominatedNodeStatus *fwk.Status) (bool, string)
	// SelectVictimsOnNode finds minimum set of pods on the given node that should be preempted in order to make enough room
	// for "pod" to be scheduled.
	// Note that both `state` and `nodeInfo` are deep copied.
	SelectVictimsOnNode(ctx context.Context, state fwk.CycleState,
		pod *v1.Pod, nodeInfo fwk.NodeInfo, pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, int, *fwk.Status)
	// OrderedScoreFuncs returns a list of ordered score functions to select preferable node where victims will be preempted.
	// The ordered score functions will be processed one by one iff we find more than one node with the highest score.
	// Default score functions will be processed if nil returned here for backwards-compatibility.
	OrderedScoreFuncs(ctx context.Context, nodesToVictims map[string]*extenderv1.Victims) []func(node string) int64
}
```

五个方法里，真正决定受害者的只有 `SelectVictimsOnNode`。它的输入是「一个节点的深拷贝」，Evaluator 在 `DryRunPreemption` 里为每个潜在节点并行调用一次：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/framework/preemption/preemption.go
	checkNode := func(i int) {
		nodeInfoCopy := potentialNodes[(int(offset)+i)%len(potentialNodes)].Snapshot()
		logger.V(5).Info("Check the potential node for preemption", "node", nodeInfoCopy.Node().Name)

		stateCopy := state.Clone()
		pods, numPDBViolations, status := ev.SelectVictimsOnNode(ctx, stateCopy, pod, nodeInfoCopy, pdbs)
		if status.IsSuccess() && len(pods) != 0 {
			victims := extenderv1.Victims{
				Pods:             pods,
				NumPDBViolations: int64(numPDBViolations),
			}
			c := &candidate{
				victims: &victims,
				name:    nodeInfoCopy.Node().Name,
			}
			// ...
```

技术上，`SelectVictimsOnNode` 返回的 `[]*v1.Pod` 并没有被检查是否都在这个节点上，执行阶段的 `prepareCandidate` 会逐个驱逐列表里的 Pod，不管它们在哪。所以一个插件可以在 `SelectVictimsOnNode` 里从快照中读取其他节点的 Pod，对它们调用 `RunPreFilterExtensionRemovePod`（传入那个节点的 `NodeInfo`，但不修改它），然后把它们一起作为受害者返回。拓扑类插件的跨节点计数都在 CycleState 里，Filter 在目标节点上会看到正确的结果。

但这种做法处处违背接口的约定：

- 每个节点的评估是独立并行的，同一个远端 Pod 会出现在多个节点的候选里，彼此没有协调；
- `CandidatesToVictimsMap` 和 extender 协议都把 `Victims` 理解为「该节点上的受害者」，支持抢占的 extender 收到跨节点受害者时行为不可预期；
- `SelectCandidate` 的默认打分（最高受害者优先级、受害者数量等）照样能用，但「受害者分散在几个节点上」这种代价没有体现；
- 搜索空间问题依然存在：选哪些远端 Pod，接口没有提供任何帮助。

换句话说，Evaluator 的扩展点是为「同节点、换规则」设计的：换候选规则（PreemptionToleration）、换配额语义（CapacityScheduling）都很顺手；要改变「受害者在哪里」这个维度，就只能像 CrossNodePreemption 当年那样绕过 Evaluator，自己写整个 PostFilter。

## 五、发散：主干上的跨节点受害者

跨节点抢占的需求并没有消失，Kubernetes 主干最近是从另一个方向切入的：不是为单个 Pod 搜索跨节点的受害者组合，而是承认某些受害者天然就是跨节点的。

### 5.1 PodGroup 作为受害者

master 提交 `aabdf5a2` 中，`SelectVictimsOnNode` 的签名多了一个参数：

```go
// pkg/scheduler/framework/preemption/preemption.go（master）
	// SelectVictimsOnNode finds the minimum set of pods that should be preempted in order to make enough room
	// for the preemptor pod to be scheduled on the selected node (represented by nodeInfo).
	// The candidate victims (allPossibleVictims) may include pods that span additional nodes (e.g., pod groups).
	// Note that `cycleState` is cloned by the caller, and `nodeInfo` is a snapshot copy.
	SelectVictimsOnNode(ctx context.Context, cycleState fwk.CycleState, preemptor *v1.Pod, nodeInfo fwk.NodeInfo, allPossibleVictims []*DomainVictim, pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, int, *fwk.Status)
```

`DomainVictim` 是一个「不可分割的受害单元」，附带它影响到的所有节点：

```go
// pkg/scheduler/framework/preemption/types.go（master）
// DomainVictim extends Victim to include information about the nodes affected by its eviction.
// It represents a preemption unit within a specific scheduling domain and allows
// the preemption logic to understand the blast radius of the eviction across multiple nodes.
type DomainVictim struct {
	Victim
	// affectedNodes tracks every node hosting at least one pod belonging to this victim,
	// enabling the scheduler to assess node-specific impacts across the entire eviction blast radius.
	affectedNodes map[string]fwk.NodeInfo
}
```

当一个 Pod 属于 disruption mode 为 `All` 的 PodGroup 时，驱逐它就意味着驱逐整个组，组内其他成员可能在别的节点上。`DefaultPreemption.SelectVictimsOnNode` 处理这种受害者的方式，正好就是第四节描述的那条「违背约定」的路，只不过现在写进了约定：

```go
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go（master）
	// For remote nodes that are only affected because a PodGroup victim has members there, we deliberately do NOT mutate their NodeInfo.
	// Instead we invoke PreFilterExtension Remove/AddPod with the remote NodeInfo so that plugins can update any CycleState they keep
	// about those pods (e.g. topology spread skew, pod affinity counters). Plugins that read remote NodeInfo directly during Filter
	// (rather than CycleState) will observe stale state for those nodes; this is acceptable because we only run the Filter pass
	// against mainNode here.
```

目标节点仍然只有一个，远端节点只通过 `PreFilterExtensions` 更新 CycleState，不修改 `NodeInfo`。这和 CrossNodePreemption 的 `dryRunOnePass` 是同一个机制，区别在于受害者集合不是搜出来的，而是由 PodGroup 的成员关系决定的，搜索空间仍然是线性的。

### 5.2 整组抢占：以整个集群为域

另一条线是抢占者本身是一个组。v1.36.4 里已经有 `PodGroupEvaluator`，注释写明它的域是整个集群：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/framework/preemption/podgrouppreemption.go
// PodGroupEvaluator is a preemption evaluator that knows how to run
// preemption where a preemptor is a pod group and the domain is the whole cluster.
```

它的算法值得和 CrossNodePreemption 对照：先把**全集群**所有可抢的受害者一次性移除，调用整组调度函数确认能放下；然后按重要性从高到低逐个加回，加回后整组仍能放下就赦免，否则确定为受害者。这是一个贪心的「全部移除、逐个赦免」，复杂度是 O(n) 次整组调度尝试，而不是 2ⁿ。它得到的不一定是全局最少的受害者集合，但是一个「去掉任何一个都不行」的极小集合，在实践中足够好。

这个功能在 v1.36.4 里受 alpha 特性门控 `WorkloadAwarePreemption` 控制（默认关闭）；master 中该门控已经移除，`DefaultPreemption` 在 `GenericWorkload` 开启时才创建 `podGroupEvaluator`（`GenericWorkload` 在 1.37 进入 Beta，默认仍关闭）。

### 5.3 如果真要做单 Pod 的跨节点抢占

把上面两点合起来，一个可用的单 Pod 跨节点抢占大致应该是：

- 受害者候选限定在与失败约束相关的拓扑域内，而不是全集群，例如 PodTopologySpread 失败时只看相关 zone 内匹配 `labelSelector` 的低优先级 Pod；
- 用「全部移除、逐个赦免」代替子集枚举，把复杂度降到线性；
- 对同一节点上的多个受害者只克隆一次节点，对远端节点只走 `PreFilterExtensions`；
- 认真计算 PDB 违反数，并让选择逻辑感知受害者跨越的节点数。

即使做到这些，它仍然改变了抢占的既有语义：一个 Pod 的调度失败，可能导致别的节点、别的 zone 上的 Pod 被驱逐。这种「爆炸半径」正是上游迟迟没有在单 Pod 层面引入跨节点抢占的原因，主干选择先在 PodGroup 这种语义上本来就跨节点的场景里打开这扇门。

## 小结

- `CrossNodePreemption` 在 `cfeb3aa` 中整体被注释，`main.go` 也没有注册它，它只是一份 Sample 级别的思路演示，基于早已不存在的框架 API。
- 它要解决的是拓扑类约束（PodTopologySpread、zone 级反亲和）下单节点抢占无能为力的问题：zone 的计数来自多个节点，单节点抢占每次只能移除其中一部分。
- 实现是对全部低优先级 Pod 做 2ⁿ 子集枚举，每个子集克隆受害者所在节点、通过 `RunPreFilterExtensionRemovePod` 更新 CycleState、再在这些节点上跑 Filter；没有剪枝，忽略 PDB，同节点多受害者的处理也有错误。
- v1.36.4 的 `preemption.Interface` 以「单节点副本」为评估单位，适合换规则，不适合换受害者的范围。
- 主干的方向是让受害者以 PodGroup 为单位跨节点（`DomainVictim`，远端节点只走 `PreFilterExtensions`），并用「全部移除、逐个赦免」的线性算法代替暴力搜索。

下一篇进入打分插件 [NodeResourcesAllocatable](/posts/sched-plugin-33-node-resources-allocatable/)：它和 in-tree 的 NodeResourcesFit 都按可分配资源打分，但偏好的方向可以反过来。
