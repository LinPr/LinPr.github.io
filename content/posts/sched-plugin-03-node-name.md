---
title: 'scheduler-plugins 03 [in-tree] NodeName：一个几乎从不生效的过滤器'
date: 2026-11-19T08:30:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "NodeName", "PreFilterResult"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NodeName 检查 spec.nodeName 是否等于当前节点。但写了 nodeName 的 Pod 通常根本不进调度队列，它在调度器里真正生效的场景只有原地扩容被推迟的 Pod。本文拆开它的 PreFilterResult 如何缩小候选节点、被排除的节点为什么不能参与抢占、为什么有了 PreFilter 还要保留 Filter，以及它和 DaemonSet、kubelet 准入的关系。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `NodeName` 实现的扩展点（PreFilter、Filter）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/03.svg" link="/images/posts/sched-plugins/callstack/03.svg" caption="NodeName 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

Pod 从 `activeQ` 出队后（[上一篇](/posts/sched-plugin-02-priority-sort/)讲了出队顺序），进入调度周期。第一步是 PreFilter，第一个被调用的 PreFilter 插件是 `NodeName`。它在默认 Profile 的 `MultiPoint` 列表里排第三，同时挂在 PreFilter 和 Filter 两个扩展点上，没有打分，没有参数，默认启用，不受 feature gate 控制。本文基于 Kubernetes master 提交 `aabdf5a2`。

它的语义一句话就能说完：Pod 写了 `spec.nodeName`，就只能放在这个节点上。但这句话背后有一个反直觉的事实：**写了 `spec.nodeName` 的 Pod，调度器通常根本看不到**。读懂这个插件，关键是弄清它到底在什么时候生效。

## 一、插件代码

```go
// pkg/scheduler/framework/plugins/nodename/node_name.go
// NodeName is a plugin that checks if a pod spec node name matches the current node.
type NodeName struct{}

var _ fwk.PreFilterPlugin = &NodeName{}
var _ fwk.FilterPlugin = &NodeName{}
var _ fwk.EnqueueExtensions = &NodeName{}
var _ fwk.SignPlugin = &NodeName{}

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = names.NodeName

	// ErrReason returned when node name doesn't match.
	ErrReason = "node(s) didn't match the requested node name"
)

// PreFilter invoked at the prefilter extension point.
func (pl *NodeName) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pod.Spec.NodeName != "" {
		// Pod already has a target node specified in spec.nodeName (e.g. assigned pod with deferred resize or static node assignment).
		// We restrict evaluation to that node - other nodes will be considered as UnschedulableAndUnresolvable and therefore won't be a target of preemption.
		return &fwk.PreFilterResult{
			NodeNames: sets.New(pod.Spec.NodeName),
		}, nil
	}
	return nil, nil
}

// PreFilterExtensions returns prefilter extensions, which is nil for this plugin.
func (pl *NodeName) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// Filter invoked at the filter extension point.
func (pl *NodeName) Filter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {

	if !Fits(pod, nodeInfo) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReason)
	}
	return nil
}

// Fits actually checks if the pod fits the node.
func Fits(pod *v1.Pod, nodeInfo fwk.NodeInfo) bool {
	return len(pod.Spec.NodeName) == 0 || pod.Spec.NodeName == nodeInfo.Node().Name
}
```

拆开看：

- **没有 CycleState**。PreFilter 不写任何状态，Filter 直接读 `pod.Spec.NodeName`。两次读同一个字段，开销可以忽略，没必要缓存。
- **PreFilter 不返回 `Skip`**。`nodeName` 为空时返回 `nil, nil`，表示「不缩小范围」，但 Filter 仍会在每个节点上被调用一次，每次都走 `len(...) == 0` 的短路返回成功。返回 `Skip` 可以省掉这些调用，但它没这么做，原因在第四节。
- **Filter 失败返回 `UnschedulableAndUnresolvable`**，原因字符串是 `node(s) didn't match the requested node name`。驱逐任何 Pod 都不能改变节点名，所以这个节点不应进入抢占候选。
- **`Fits` 是导出函数**。它被 kubelet 的准入检查直接调用，第五节会讲。
- **`SignPod`** 把 `spec.nodeName` 作为签名片段，供 `OpportunisticBatching`（1.35 Beta，默认开启）判断两个 Pod 能否复用同一份打分结果。没有实现 `SignPlugin` 的 Filter/Score 插件会让批处理整体关闭，所以连这么小的插件也要实现它。

## 二、谁会把写了 nodeName 的 Pod 交给调度器

{{< figure src="/images/posts/sched-plugins/03-nodename-prefilter.svg" link="/images/posts/sched-plugins/03-nodename-prefilter.svg" caption="左：带 spec.nodeName 的 Pod 通常只进调度器缓存，由 kubelet 准入把关；右：进入调度周期后，各插件的 PreFilterResult 取交集，决定哪些节点进入 Filter（点击查看大图）" >}}

### 2.1 正常情况：不进队列

调度器的 Pod informer 回调是这样分流的：

```go
// pkg/scheduler/eventhandlers.go
func (sched *Scheduler) addPod(obj interface{}) {
	// ...
	isPodDeferred := utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) && resource.IsPodResizeDeferred(pod)
	if responsibleForPod(pod, sched.Profiles) && (!assignedPod(pod) || isPodDeferred) {
		sched.addPodToSchedulingQueue(pod)
	}
	if assignedPod(pod) {
		sched.addAssignedPodToCache(pod)
	}
}

// assignedPod selects pods that are assigned (scheduled and running).
func assignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}
```

`spec.nodeName` 非空，调度器就认为这个 Pod「已经调度完了」，只把它加进缓存，用于计算节点上的资源占用、端口占用和 Pod 间亲和。它不进调度队列，也就不会经过任何插件，包括 `NodeName` 本身。

所以手写一个带 `nodeName` 的 Pod，是完全绕过调度器的：节点资源够不够、污点容不容忍、亲和满不满足，调度器一概不管。真正的把关者是目标节点上的 kubelet（第五节）。

### 2.2 唯一的入口：被推迟的原地扩容

`addPod` 里那个 `isPodDeferred` 条件是 `NodeName` PreFilter 存在的理由。`InPlacePodVerticalScalingSchedulerPreemption`（1.37 Alpha，默认关闭）这个 feature 处理的场景是：一个已经在运行的 Pod 申请原地扩容（修改 resources），kubelet 发现节点当前放不下，把 resize 标记为 Deferred（`PodResizePending` 条件的 reason 为 `Deferred`）。打开这个 feature 后，调度器会把这种 Pod 重新放进调度队列，评估「能不能在它所在的节点上通过抢占腾出空间」：

```go
// pkg/scheduler/eventhandlers.go
	if assignedPod(oldPod) {
		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) && responsibleForPod(newPod, sched.Profiles) {
			oldDeferred := resource.IsPodResizeDeferred(oldPod)
			newDeferred := resource.IsPodResizeDeferred(newPod)

			if !oldDeferred && newDeferred {
				// When an assigned pod transitions to a deferred resize,
				// we must add it to the scheduling queue so the scheduler can evaluate feasibility and run preemption on the assigned node.
				logger.V(3).Info("Assigned pod transitioned to deferred resize, adding to scheduling queue", "pod", klog.KObj(newPod))
				sched.addPodToSchedulingQueue(newPod)
			} else if oldDeferred && !newDeferred {
				// ...
				sched.deletePodFromSchedulingQueue(newPod, false)
			}
			// ...
		}
		sched.updateAssignedPodInCache(oldPod, newPod)
	}
```

这种 Pod 不能换节点，只能在原节点上想办法。`NodeName.PreFilter` 的注释说得很直白：把评估限制在这一个节点上，其他节点被视为 `UnschedulableAndUnresolvable`，不会成为抢占目标。与此同时，`NodeUnschedulable`、`TaintToleration`、`NodeAffinity` 对这种 Pod 的 PreFilter 都返回 `Skip`：Pod 已经在节点上跑着，节点被 cordon、加了新污点、标签变了，都不影响它扩容。剩下真正有意义的检查是资源，由 `NodeResourcesFit` 和第 17 篇的 `DeferredPodScheduling` 完成。`default_plugins.go` 里 `applyDeferredPodScheduling` 的注释特意要求把 `DeferredPodScheduling` 放在 `NodeName` 这类基础过滤之后，也是这个原因。

除了这条路，`spec.nodeName` 非空的 Pod 不会出现在调度周期里。PreFilter 注释里提到的「static node assignment」，指的就是这类 Pod 在调度器框架里被评估的情况（比如其他复用调度框架做模拟的组件），框架给出一个一致的语义。

## 三、PreFilterResult 怎么缩小候选节点

### 3.1 合并规则

`PreFilterResult` 只有一个字段：

```go
// staging/src/k8s.io/kube-scheduler/framework/interface.go
// PreFilterResult wraps needed info for scheduler framework to act upon PreFilter phase.
type PreFilterResult struct {
	// The set of nodes that should be considered downstream; if nil then
	// all nodes are eligible.
	NodeNames sets.Set[string]
}

func (p *PreFilterResult) AllNodes() bool {
	return p == nil || p.NodeNames == nil
}

func (p *PreFilterResult) Merge(in *PreFilterResult) *PreFilterResult {
	if p.AllNodes() && in.AllNodes() {
		return nil
	}

	r := PreFilterResult{}
	if p.AllNodes() {
		r.NodeNames = in.NodeNames.Clone()
		return &r
	}
	if in.AllNodes() {
		r.NodeNames = p.NodeNames.Clone()
		return &r
	}

	r.NodeNames = p.NodeNames.Intersection(in.NodeNames)
	return &r
}
```

`nil` 表示「全部节点」，非 nil 的集合之间取交集。注意 `nil` 和空集合的区别：`NodeNames == nil` 是全部，`NodeNames` 是一个长度为 0 的集合则表示一个都不行。

框架在 `RunPreFilterPlugins` 里按链上顺序逐个合并，交集一旦为空就立刻终止：

```go
// pkg/scheduler/framework/runtime/framework.go
		if !r.AllNodes() {
			pluginsWithNodes.Insert(pl.Name())
		}
		result = result.Merge(r)
		if !result.AllNodes() && len(result.NodeNames) == 0 {
			msg := fmt.Sprintf("node(s) didn't satisfy plugin(s) %v simultaneously", sets.List(pluginsWithNodes))
			if len(pluginsWithNodes) == 1 {
				msg = fmt.Sprintf("node(s) didn't satisfy plugin %v", sets.List(pluginsWithNodes)[0])
			}

			// When PreFilterResult filters out Nodes, the framework considers Nodes that are filtered out as getting "UnschedulableAndUnresolvable".
			return result, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, msg), pluginsWithNodes
		}
```

`pluginsWithNodes` 记录了哪些插件缩小过范围，它会被写进 `diagnosis.UnschedulablePlugins`。这一点在第六节讲事件时还会用到。

默认插件里能返回非 nil `PreFilterResult` 的只有两个：`NodeName`（`spec.nodeName` 非空）和 `NodeAffinity`（required 节点亲和里用了 `matchFields: metadata.name In [...]`，第 06 篇细讲）。

### 3.2 被排除的节点：不进 Filter，也不参与抢占

PreFilter 结束后，`getCandidateNodes` 把结果变成真正要过 Filter 的节点列表：

```go
// pkg/scheduler/algorithm.go
func (a *SchedulingAlgorithm) getCandidateNodes(preFilterOutcome preFilterOutcome) []fwk.NodeInfo {
	preRes := preFilterOutcome.result
	if preRes.AllNodes() {
		return preFilterOutcome.allNodes
	}

	diagnosis := preFilterOutcome.diagnosis
	nodes := make([]fwk.NodeInfo, 0, len(preRes.NodeNames))
	for nodeName := range preRes.NodeNames {
		// PreRes may return nodeName(s) which do not exist; we verify
		// node exists in the Snapshot within the selected placement.
		if nodeInfo, err := a.nodeInfoSnapshot.GetNodeInPlacement(nodeName); err == nil {
			nodes = append(nodes, nodeInfo)
		}
	}
	diagnosis.NodeToStatus.SetAbsentNodesStatus(fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node(s) didn't satisfy plugin(s) %v", sets.List(diagnosis.UnschedulablePlugins))))

	return nodes
}
```

三个细节：

1. **不存在的节点被静默丢弃**。`spec.nodeName` 指向一个快照里没有的节点，候选列表就是空的，Pod 本周期失败。
2. **被排除的节点统一标为 `UnschedulableAndUnresolvable`**。`SetAbsentNodesStatus` 给所有没出现在 `NodeToStatus` 里的节点设置一个默认状态。PostFilter 阶段的抢占只会在 `Unschedulable` 的节点上找受害者，所以这些节点从一开始就不在抢占范围内。对被推迟扩容的 Pod 来说，这正是想要的：只能在自己的节点上抢占。
3. **Filter 的预算按候选列表计算**。`numNodesToFind` 的入参是 `len(nodes)`，候选只有一个节点时最多找 1 个可行节点。如果直接遍历全部节点、靠 Filter 淘汰，5000 个节点的集群要并行跑 5000 次 Filter 链；有了 PreFilterResult，只跑 1 次。

## 四、为什么有了 PreFilter 还要 Filter

既然 PreFilter 已经把候选缩成一个节点，那个节点一定满足 `Fits`，Filter 似乎是多余的。它没有被删掉，是因为调度周期里有不经过 `getCandidateNodes` 的路径。

回看 `findNodesThatFitPod`：

```go
// pkg/scheduler/algorithm.go
	// "NominatedNodeName" can potentially be set in a previous scheduling cycle as a result of preemption.
	// This node is likely the only candidate that will fit the pod, and hence we try it first before iterating over all nodes.
	// We take the same tack for hinted nodes from the batch module.
	if len(pod.Status.NominatedNodeName) > 0 || len(nodeHint) > 0 {
		feasibleNodes, err := a.evaluateNominatedNode(ctx, pod, schedFramework, state, nodeHint, diagnosis)
		// ...
		if len(feasibleNodes) != 0 {
			return feasibleNodes, diagnosis, nodeHint, nil
		}
	}

	nodes := a.getCandidateNodes(preFilterOutcome)
```

`evaluateNominatedNode` 拿 `status.nominatedNodeName`（或者批处理给出的 `nodeHint`）直接从快照里取节点、跑 Filter，完全不看 `PreFilterResult`。如果提名节点和 `spec.nodeName` 不一致，唯一能拦住它的就是 `NodeName.Filter`。同理，`NodeAffinity` 也同时保留了 PreFilterResult 和 Filter 两份检查。

这是一个通用的设计原则：**PreFilterResult 是优化，不是语义**。插件的 Filter 必须在任何节点上单独给出正确答案，PreFilterResult 只负责让框架少问几次。自己写插件时如果用了 PreFilterResult，也要保证 Filter 能独立成立。

这也解释了 PreFilter 不返回 `Skip` 的原因：返回 `Skip` 会让框架在本周期跳过 `NodeName.Filter`，而 `nodeName` 为空时跳过虽然无害，代码选择了不区分两种情况，保持 Filter 始终在场。对于空 `nodeName`，每个节点多一次函数调用和一次长度判断，代价可以忽略。

## 五、和 kubelet、DaemonSet 的关系

### 5.1 kubelet 准入复用了 Fits

绕过调度器的 Pod 由 kubelet 把关。kubelet 在接纳 Pod 之前跑一组「通用谓词」，其中直接调用了调度器插件导出的函数：

```go
// pkg/kubelet/lifecycle/predicate.go
func generalFilter(logger klog.Logger, pod *v1.Pod, nodeInfo *schedulerframework.NodeInfo) []PredicateFailureReason {
	var reasons []PredicateFailureReason
	// ... 资源检查 ...
	if matches, _ := corev1nodeaffinity.GetRequiredNodeAffinity(pod).Match(nodeInfo.Node()); !matches {
		reasons = append(reasons, &PredicateFailureError{nodeaffinity.Name, nodeaffinity.ErrReasonPod})
	}
	if !nodename.Fits(pod, nodeInfo) {
		reasons = append(reasons, &PredicateFailureError{nodename.Name, nodename.ErrReason})
	}
	if !nodeports.Fits(pod, nodeInfo) {
		reasons = append(reasons, &PredicateFailureError{nodeports.Name, nodeports.ErrReason})
	}

	// Check taint/toleration except for static pods
	if !types.IsStaticPod(pod) {
		_, isUntolerated := corev1.FindMatchingUntoleratedTaint(logger, nodeInfo.Node().Spec.Taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
			// Kubelet is only interested in the NoExecute taint.
			return t.Effect == v1.TaintEffectNoExecute
		}, utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators))
		// ...
	}

	return reasons
}
```

kubelet 只接收 `spec.nodeName` 等于自己的 Pod，所以 `nodename.Fits` 在这里几乎总是为真，它主要是一个防御性检查。更值得注意的是这份清单**没有什么**：没有 `NodeUnschedulable`，没有 `NoSchedule` 污点，没有 Pod 间亲和，没有拓扑分布。所以：

- 给 cordon 掉的节点手写 `nodeName`，Pod 照样能跑起来；
- 节点上有 `NoSchedule` 污点，手写 `nodeName` 的 Pod 也能跑，只有 `NoExecute` 污点会被拒绝（第 05 篇细讲两者的分工）。

准入失败时 Pod 的 phase 变成 `Failed`，`status.reason` 是 `PredicateFailureError` 里的插件名，比如 `NodeAffinity`，message 是 `Predicate NodeAffinity failed: ...`。这时 Pod 不会被重新调度，因为它已经有 `nodeName`，调度器不会接手。

如果 `nodeName` 指向一个根本不存在的节点，没有 kubelet 会认领它，Pod 会一直 Pending。PodGC 控制器会处理这种「孤儿」：节点不存在且持续 40 秒（`quarantineTime`）后，Pod 被标记 `DisruptionTarget`（reason `DeletionByPodGC`）并删除。

### 5.2 DaemonSet 不写 nodeName

直觉上 DaemonSet 的 Pod 应该写 `nodeName`，毕竟每个 Pod 的目标节点是确定的。早期的 DaemonSet 控制器也确实这么做，但那样 Pod 完全绕过了调度器，抢占、资源检查、PriorityClass 都用不上。现在的控制器改写的是节点亲和：

```go
// pkg/controller/daemon/daemon_controller.go
				podTemplate := template.DeepCopy()
				// The pod's NodeAffinity will be updated to make sure the Pod is bound
				// to the target node by default scheduler. It is safe to do so because there
				// should be no conflicting node affinity with the target node.
				podTemplate.Spec.Affinity = util.ReplaceDaemonSetPodNodeNameNodeAffinity(
					podTemplate.Spec.Affinity, nodesNeedingDaemonPods[ix])
```

```go
// pkg/controller/daemon/util/daemonset_util.go
func ReplaceDaemonSetPodNodeNameNodeAffinity(affinity *v1.Affinity, nodename string) *v1.Affinity {
	nodeSelReq := v1.NodeSelectorRequirement{
		Key:      metav1.ObjectNameField,
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{nodename},
	}
	// ...
}
```

`metav1.ObjectNameField` 就是 `metadata.name`。这正好落在 `NodeAffinity.PreFilter` 能识别的形式上，它会返回 `PreFilterResult{NodeNames: {nodename}}`。所以 DaemonSet Pod 在调度器里享受的「只看一个节点」的优化，来自 `NodeAffinity` 而不是 `NodeName`。效果相同，但 Pod 是经过调度器的：资源不够时可以触发抢占（目标节点被标为 `Unschedulable` 而不是 `UnschedulableAndUnresolvable`），PriorityClass 生效，失败会被记录成调度事件。

DaemonSet 的模板里如果真写了 `spec.nodeName`，控制器的 `nodeShouldRunDaemonPod` 会只在这一个节点上创建 Pod，而创建出的 Pod 带着 `nodeName`，又回到了 5.1 的绕过路径。

## 六、QueueingHint：只关心节点新增

```go
// pkg/scheduler/framework/plugins/nodename/node_name.go
func (pl *NodeName) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// We don't need the QueueingHintFn here because the scheduling of Pods will be always retried with backoff when this Event happens.
		// (the same as Queue)
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}},
	}, nil
}
```

只注册了 `Node/Add`，而且没有 hint 函数，框架会用 `defaultQueueingHintFn`，等价于总是返回 `Queue`。逻辑是：节点名不会被修改，节点删除也不会让 Pod 变得可调度，唯一能改变 `NodeName` 判定的是「目标节点出现了」，对应 3.2 里「不存在的节点被静默丢弃」的情况。

`NodeName` 怎么进入 Pod 的 `UnschedulablePlugins`？有两条路：Filter 在某个节点上返回失败（只可能发生在提名节点路径上），或者它在 PreFilter 缩小过范围，被 `pluginsWithNodes` 记了下来（3.1）。后一条意味着：只要 Pod 带着 `nodeName` 失败了，哪怕真正的原因是资源不足，之后任何新节点加入都会把它唤醒一次。这是一个宁可多唤醒的选择，代价不大，因为这类 Pod 本身就很少。

## 七、边界情况

**`nodeName` 与 `schedulingGates` 不能共存。** API Server 在创建时校验（第 01 篇），带 gate 的 Pod 不可能同时写 `nodeName`。

**`nodeName` 创建后不可修改。** `spec.nodeName` 不在 Pod 更新的白名单里，只能通过 `pods/binding` 子资源从空写成非空，而且只能写一次（binding 时检查 `pod.Spec.NodeName != ""` 就报 `already assigned`）。所以 `NodeName` 插件不需要关心 Pod 更新事件。

**提名节点与 `nodeName` 不一致。** 正常流程里不会出现，但第四节说明了即使出现，Filter 也会挡住。

**大规模集群里的影响。** 对普通 Pod，`NodeName` 的开销是每个候选节点一次 Filter 调用加一次字符串长度判断，在所有 Filter 插件里最小，这也是它排在 Filter 链最前面的原因：开销小的放前面，即使它几乎不淘汰节点。

## 八、发散：PreFilterResult 的设计取舍

`PreFilterResult` 是后来加进框架的。在它出现之前，所有 Filter 插件都只能「逐个节点回答是否可行」，即使插件在 PreFilter 阶段就已经知道答案只可能是某一个节点。一个只能去 `node-a` 的 DaemonSet Pod，在 5000 节点的集群里要跑 5000 次完整的 Filter 链（每次都在 `NodeAffinity` 处失败），再根据 `percentageOfNodesToScore` 提前停止也无济于事，因为可行节点只有一个，提前停止的条件永远达不到。

`PreFilterResult` 的设计有三个约束，读完 `NodeName` 和 `getCandidateNodes` 就能理解：

1. **只能缩小，不能扩大**。合并是交集，任何一个插件都不能把别人排除的节点加回来。
2. **被排除等同于 `UnschedulableAndUnresolvable`**。插件只能用它表达「这个节点无论如何都不行」，不能表达「这个节点抢占后也许行」。需要后者语义的插件不能用 PreFilterResult，只能老老实实在 Filter 里返回 `Unschedulable`。
3. **Filter 仍要独立正确**，因为提名节点、批处理 hint 这些路径不经过它。

这三条让 PreFilterResult 成为一个纯粹的性能优化，而不是第二套语义。抢占代码里也能看到第 2 条的落点：`DefaultPreemption` 用 `NodesForStatusCode(..., fwk.Unschedulable)` 挑选潜在节点，状态是 `UnschedulableAndUnresolvable` 的节点从一开始就不在名单里。顺带一提，scheduler-plugins 里实现 PreFilter 的插件（`Coscheduling`、`CapacityScheduling`、`NetworkOverhead`、`NodeResourceTopologyMatch` 等）目前都只返回 `nil` 的 `PreFilterResult`，没有利用这个优化。

## 九、小结

- `NodeName` 的 PreFilter 在 `spec.nodeName` 非空时返回只含这一个节点的 `PreFilterResult`，Filter 在节点名不匹配时返回 `UnschedulableAndUnresolvable`。
- 带 `nodeName` 的 Pod 正常情况下不进调度队列，由 kubelet 准入把关；kubelet 只检查资源、节点亲和、节点名、端口和 `NoExecute` 污点。调度器里唯一的入口是 `InPlacePodVerticalScalingSchedulerPreemption`（Alpha，默认关）下被推迟扩容的 Pod。
- `PreFilterResult` 之间取交集，被排除的节点一律视为 `UnschedulableAndUnresolvable`，不参与抢占；Filter 必须保留，因为提名节点路径不经过 PreFilterResult。
- DaemonSet Pod 不写 `nodeName`，而是写 `matchFields: metadata.name` 节点亲和，由 `NodeAffinity` 的 PreFilterResult 实现同样的单节点优化。
- 只注册 `Node/Add` 事件，没有 hint 函数，相当于总是 `Queue`。

[下一篇](/posts/sched-plugin-04-node-unschedulable/)读 Filter 链上的第二个插件 `NodeUnschedulable`，看 `kubectl cordon` 在调度器里到底改变了什么。
