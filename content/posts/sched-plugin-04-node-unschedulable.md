---
title: 'scheduler-plugins 04 [in-tree] NodeUnschedulable：cordon 在调度器里的样子'
date: 2026-11-19T08:40:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "NodeUnschedulable", "cordon", "污点"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "kubectl cordon 只改了 Node 的 spec.unschedulable 一个字段，NodeUnschedulable 插件负责让调度器尊重它，而容忍 node.kubernetes.io/unschedulable 污点的 Pod 可以例外。本文拆开插件的 Filter 判定、QueueingHint 如何识别 uncordon、spec 字段与 node-lifecycle-controller 补上的同名污点之间的时间差，以及 kubelet 为什么不检查这个字段。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `NodeUnschedulable` 实现的扩展点（PreFilter、Filter）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/04.svg" link="/images/posts/sched-plugins/callstack/04.svg" caption="NodeUnschedulable 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-03-node-name/)的 `NodeName` 是 Filter 链上的第一个插件，它几乎不淘汰节点。第二个插件 `NodeUnschedulable` 处理的是运维最常用的操作之一：`kubectl cordon`。它在默认 Profile 的 `MultiPoint` 列表里排第四，挂在 PreFilter 和 Filter 上，没有打分，没有参数，默认启用。本文基于 Kubernetes master 提交 `aabdf5a2`。

## 一、cordon 改了什么

`kubectl cordon n1` 只做一件事：把 Node 的 `spec.unschedulable` 设为 `true`。kubectl 的实现是一个 patch（第 4 行）：

```go {linenos=inline}
// staging/src/k8s.io/kubectl/pkg/drain/cordon.go
func (c *CordonHelper) PatchOrReplaceWithContext(clientCtx context.Context, clientset kubernetes.Interface, serverDryRun bool) (error, error) {
	// ...
	desiredNode.Spec.Unschedulable = c.desired
	// ...
}
```

`kubectl drain` 是 cordon 之后再逐个驱逐节点上的 Pod。**cordon 本身不影响已经在节点上运行的 Pod**，它只告诉调度器：别再往这里放新 Pod。「告诉调度器」这件事，就是 `NodeUnschedulable` 插件负责的。

## 二、插件代码

结构体里只有两个 feature gate 快照，判定分 PreFilter 和 Filter 两段。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeunschedulable/node_unschedulable.go
// NodeUnschedulable plugin filters nodes that set node.Spec.Unschedulable=true unless
// the pod tolerates {key=node.kubernetes.io/unschedulable, effect:NoSchedule} taint.
type NodeUnschedulable struct {
	enableInPlacePodVerticalScalingSchedulerPreemption bool
	enableTaintTolerationComparisonOperators           bool
}

const (
	// ErrReasonUnknownCondition is used for NodeUnknownCondition predicate error.
	ErrReasonUnknownCondition = "node(s) had unknown conditions"
	// ErrReasonUnschedulable is used for NodeUnschedulable predicate error.
	ErrReasonUnschedulable = "node(s) were unschedulable"
)

// PreFilter invoked at the prefilter extension point.
func (pl *NodeUnschedulable) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, nil
}

// Filter invoked at the filter extension point.
func (pl *NodeUnschedulable) Filter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	node := nodeInfo.Node()

	if !node.Spec.Unschedulable {
		return nil
	}

	logger := klog.FromContext(ctx)
	// If pod tolerate unschedulable taint, it's also tolerate `node.Spec.Unschedulable`.
	podToleratesUnschedulable := v1helper.TolerationsTolerateTaint(logger, pod.Spec.Tolerations, &v1.Taint{
		Key:    v1.TaintNodeUnschedulable,
		Effect: v1.TaintEffectNoSchedule,
	}, pl.enableTaintTolerationComparisonOperators)
	if !podToleratesUnschedulable {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonUnschedulable)
	}

	return nil
}

// New initializes a new plugin and returns it.
func New(_ context.Context, _ runtime.Object, _ fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &NodeUnschedulable{
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
		enableTaintTolerationComparisonOperators:           fts.EnableTaintTolerationComparisonOperators,
	}, nil
}
```

结构体里的两个布尔值（第 4–7 行）是 feature gate 的快照，在 `New`（第 49–54 行）里通过 `feature.Features` 传入，运行期不再查询全局 gate：

1. `InPlacePodVerticalScalingSchedulerPreemption`（1.37 Alpha，默认关）：原地扩容被推迟的 Pod 已经在节点上运行，节点被 cordon 不影响它扩容，所以 PreFilter 返回 `Skip`（第 18–20 行），Filter 也直接放行（第 26–28 行）。上一篇讲过这类 Pod 是唯一带着 `nodeName` 进入调度周期的 Pod。
2. `TaintTolerationComparisonOperators`（1.35 Alpha，默认关）：允许 toleration 使用 `Lt`/`Gt` 数值比较，这里只是透传给容忍判断函数，第 05 篇细讲。

对普通 Pod，PreFilter 返回 `nil, nil`（第 21 行）：既不缩小节点范围，也不 `Skip`。插件没有 CycleState，Filter 每次直接读 Node 对象。

判定只有三步：

1. 节点没被 cordon，放行（第 31–33 行）；
2. 节点被 cordon 了，看 Pod 是否容忍一个构造出来的污点 `node.kubernetes.io/unschedulable:NoSchedule`（第 37–40 行），容忍就放行；
3. 否则返回 `UnschedulableAndUnresolvable`，原因 `node(s) were unschedulable`（第 41–43 行）。

**驱逐节点上的 Pod 不会让节点取消 cordon，所以这个节点不会成为抢占候选。** 抢占在 `pkg/scheduler/framework/preemption/preemption.go` 里用 `NodesForStatusCode` 只挑出状态为 `Unschedulable` 的节点作为潜在候选，返回 `UnschedulableAndUnresolvable` 的节点一开始就被排除（开篇 3.3 节）。

`ErrReasonUnknownCondition` 在整个代码库里已经没有任何引用，它是早年 `CheckNodeUnknownCondition` 谓词留下的常量，这个谓词在节点条件全部改用污点表达之后被删掉了。

## 三、spec 字段和同名污点

Filter 里那句「容忍一个构造出来的污点」还留下一个问题：cordon 明明只改了一个 spec 字段，为什么例外规则要用污点来写？因为这个状态在集群里有两份表示。

### 3.1 一个状态，两份表示

Filter 用「是否容忍某个污点」来决定是否忽略一个 spec 字段，这看起来有点绕。原因是 cordon 在集群里确实有两份表示。kube-controller-manager 里的 node-lifecycle-controller 会把 `spec.unschedulable` 镜像成一个污点：

```go {linenos=inline}
// pkg/controller/nodelifecycle/node_lifecycle_controller.go
func (nc *Controller) doNoScheduleTaintingPass(ctx context.Context, nodeName string) error {
	// ...
	// Map node's condition to Taints.
	var taints []v1.Taint
	for _, condition := range node.Status.Conditions {
		if taintMap, found := nodeConditionToTaintKeyStatusMap[condition.Type]; found {
			if taintKey, found := taintMap[condition.Status]; found {
				taints = append(taints, v1.Taint{
					Key:    taintKey,
					Effect: v1.TaintEffectNoSchedule,
				})
			}
		}
	}
	if node.Spec.Unschedulable {
		// If unschedulable, append related taint.
		taints = append(taints, v1.Taint{
			Key:    v1.TaintNodeUnschedulable,
			Effect: v1.TaintEffectNoSchedule,
		})
	}
	// ...
	taintsToAdd, taintsToDel := taintutils.TaintSetDiff(taints, nodeTaints)
	// ...
}
```

这个函数把节点的状态统一翻译成 `NoSchedule` 污点：

1. 节点条件：`Ready=False` 变成 `not-ready`，`MemoryPressure=True` 变成 `memory-pressure`（第 6–15 行的映射）；
2. cordon：`spec.unschedulable=true` 变成 `unschedulable`（第 16–22 行）。

**这样做的好处是只需要一种机制（污点与容忍）就能表达「哪些 Pod 可以无视节点的某种状态」。** DaemonSet 控制器给它创建的每个 Pod 都加上了一组默认容忍，其中就有这一条（第 2–6 行）：

```go {linenos=inline}
// pkg/controller/daemon/util/daemonset_util.go
	v1helper.AddOrUpdateTolerationInPodSpec(spec, &v1.Toleration{
		Key:      v1.TaintNodeUnschedulable,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})
```

所以 DaemonSet 的 Pod 能调度到 cordon 掉的节点上。`kubectl drain` 遇到 DaemonSet 管理的 Pod 会直接报错（`DaemonSet-managed Pods (use --ignore-daemonsets to ignore)`），要求显式跳过它们：这类 Pod 容忍 cordon，驱逐了也会被控制器在原节点上重建。

从 Pod 的角度看，cordon 完全可以只用污点表达。调度器却不这么看。

### 3.2 为什么调度器还要单独看 spec

既然污点会被补上，`TaintToleration` 插件自然会处理它，`NodeUnschedulable` 似乎是多余的。它存在是因为两份表示之间有时间差：`spec.unschedulable` 是 kubectl 同步写入的，污点是控制器异步补上的，中间要经过三步：

1. node-lifecycle-controller 的 Node informer 收到更新，把节点名放进 `nodeUpdateQueue`；
2. worker 取出节点名，调用 3.1 节的 `doNoScheduleTaintingPass` 算出要增删的污点；
3. 再用 `SwapNodeControllerTaint` 发一次 API 请求，把污点写回 Node。

{{< figure src="/images/posts/sched-plugins/04-cordon-timeline.svg" link="/images/posts/sched-plugins/04-cordon-timeline.svg" caption="cordon 与 uncordon 的时间线：spec 字段立即生效，污点由 node-lifecycle-controller 稍后补上或删除，调度器两边都检查（点击查看大图）" >}}

图中 ① 和 ② 之间，节点已经 cordon，但污点还没出现。如果调度器只看污点，这段时间里新 Pod 仍可能被放到 n1 上。**`NodeUnschedulable` 直接读 spec，cordon 一写入（调度器的 Node informer 一收到更新）就生效，不依赖控制器。** 即使 kube-controller-manager 没有运行，或者 node-lifecycle-controller 被禁用，cordon 依然有效。

而 Filter 里用「容忍那个污点」作为例外条件，是为了让两份表示的语义一致：一个 Pod 如果能容忍 `unschedulable` 污点，那么在 ② 之后它能通过 `TaintToleration`，在 ② 之前它也应该能通过 `NodeUnschedulable`。注释写的就是这个意思：「If pod tolerate unschedulable taint, it's also tolerate `node.Spec.Unschedulable`」。

构造的污点没有 `Value`，所以以下三种容忍都能生效：

1. `operator: Exists` 且 key 为 `node.kubernetes.io/unschedulable`；
2. `operator: Equal`、key 相同、`value` 为空；
3. key 为空、`operator: Exists` 的「容忍一切」。

effect 必须为空或 `NoSchedule`，写成 `NoExecute` 的容忍不算。

两份表示、一段异步窗口，解释了 Filter 为什么两边都要看。接下来看反向的动作：uncordon 之后，被拒绝的 Pod 是怎么被叫回队列的。

## 四、QueueingHint：只关心「变得可调度」

被 `NodeUnschedulable` 拒绝的 Pod，要等到某个节点不再「不可调度」，或者自己补上容忍，才有希望。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeunschedulable/node_unschedulable.go
func (pl *NodeUnschedulable) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// When QueueingHint is enabled, we don't use preCheck and we don't need to register UpdateNodeLabel event.
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeTaint}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
		// When the QueueingHint feature is enabled,
		// the scheduling queue uses Pod/Update Queueing Hint
		// to determine whether a Pod's update makes the Pod schedulable or not.
		// https://github.com/kubernetes/kubernetes/pull/122234
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodToleration}, QueueingHintFn: pl.isSchedulableAfterTargetPodTolerationChange},
	}, nil
}
```

这段注册里有两处订阅、一处显式不订阅，共三条：

1. `Node` 的 `Add | UpdateNodeTaint`（第 5 行）：节点新增或污点变化，cordon、uncordon 和 `kubectl taint` 都算，hint 是 `isSchedulableAfterNodeChange`；
2. `TargetPod` 的 `UpdatePodToleration`（第 10 行）：Pod 自己的容忍发生变化，hint 是 `isSchedulableAfterTargetPodTolerationChange`；
3. 不注册 `UpdateNodeLabel`：注释说明启用 QueueingHint 后不再使用 preCheck，而节点标签变化也不改变 `spec.unschedulable` 的值。

### 4.1 uncordon 被翻译成 UpdateNodeTaint

插件注册的是 `UpdateNodeTaint`，而 uncordon 改的是 `spec.unschedulable`，不是污点。两者的连接点在调度器把 Node 更新翻译成事件的地方：

```go {linenos=inline}
// pkg/scheduler/framework/events.go
func extractNodeSpecUnschedulableChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	if newNode.Spec.Unschedulable != oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable {
		// TODO: create UpdateNodeSpecUnschedulable ActionType
		return fwk.UpdateNodeTaint
	}
	return fwk.None
}
```

`spec.unschedulable` 从 `true` 变为 `false` 时，产生一个 `UpdateNodeTaint` 事件（第 3–5 行）。**反方向（cordon）不产生事件**：节点变得更不可调度，不可能让任何等待中的 Pod 变得可调度，没必要唤醒谁。TODO 注释说明这是一个借用：没有专门的 ActionType，就复用了语义最接近的污点变化。

### 4.2 hint 函数

事件进来了，hint 要回答的是：这一次变化，有没有让这个 Pod 从「不行」变成「行」。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeunschedulable/node_unschedulable.go
func (pl *NodeUnschedulable) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalNode, modifiedNode, err := util.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	// We queue this Pod when -
	// 1. the node is updated from unschedulable to schedulable.
	// 2. the node is added and is schedulable.
	if (originalNode != nil && originalNode.Spec.Unschedulable && !modifiedNode.Spec.Unschedulable) ||
		(originalNode == nil && !modifiedNode.Spec.Unschedulable) {
		logger.V(5).Info("node was created or updated, pod may be schedulable now", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.Queue, nil
	}

	logger.V(5).Info("node was created or updated, but it doesn't make this pod schedulable", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
	return fwk.QueueSkip, nil
}
```

**只有两种情况返回 `Queue`**（第 11–15 行的两个条件）：

1. 节点从 cordon 变为 uncordon；
2. 新增了一个没被 cordon 的节点。

其他的污点变化（比如有人给节点加了别的污点）返回 `QueueSkip`。hint 只看事件里的节点，不看 Pod 被哪个节点拒绝过，所以集群里任何一个节点 uncordon，所有被 `NodeUnschedulable` 拒绝过的 Pod 都会被唤醒一次。

`util.As` 转换失败时返回 `Queue` 加错误，这是所有 hint 函数的惯例：拿不准就唤醒，宁可多跑一次调度周期，也不让 Pod 卡死。

Pod 容忍变化的 hint：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeunschedulable/node_unschedulable.go
func (pl *NodeUnschedulable) isSchedulableAfterTargetPodTolerationChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, modifiedPod, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	// Note: we don't need to check oldPod tolerations the taint because:
	// - Taint can be added, but can't be modified nor removed.
	// - If the Pod already has the toleration, it shouldn't have rejected by this plugin in the first place.
	//   Meaning, here this Pod has been rejected by this plugin, and hence it shouldn't have the toleration yet.
	if v1helper.TolerationsTolerateTaint(logger, modifiedPod.Spec.Tolerations, &v1.Taint{
		Key:    v1.TaintNodeUnschedulable,
		Effect: v1.TaintEffectNoSchedule,
	}, pl.enableTaintTolerationComparisonOperators) {
		// This update makes the pod tolerate the unschedulable taint.
		// ...
		return fwk.Queue, nil
	}
	// ...
	return fwk.QueueSkip, nil
}
```

注释里的「Taint can be added, but can't be modified nor removed」说的其实是 toleration（API 校验 `validateOnlyAddedTolerations` 只允许给 Pod 追加容忍）。因为只能追加，事件提取函数 `extractPodTolerationChange` 只比较新旧容忍的数量；hint 也只需要检查新 Pod 是否容忍（第 12–19 行），不必比较旧 Pod：**旧 Pod 如果已经容忍，根本不会被这个插件拒绝。**

### 4.3 uncordon 之后的两次唤醒

把图里的 ④⑤ 串起来看。一个不容忍 `unschedulable` 污点的 Pod，在 n1 被 cordon 时调度失败。Filter 链里 `NodeUnschedulable` 排在 `TaintToleration` 前面，Filter 遇到第一个失败就停，所以对 n1 记录的拒绝者是 `NodeUnschedulable`。

④ uncordon：`UpdateNodeTaint` 事件，`NodeUnschedulable` 的 hint 返回 `Queue`，Pod 回到队列。如果它在 ⑤ 之前就被调度，n1 的 `spec.unschedulable` 已经是 `false`，`NodeUnschedulable` 放行，但污点还在，`TaintToleration` 拒绝。这一次的拒绝者变成了 `TaintToleration`。

⑤ 控制器删掉污点：又一个 `UpdateNodeTaint` 事件。`NodeUnschedulable` 的 hint 会返回 `QueueSkip`（spec 没变），但 Pod 现在的拒绝者是 `TaintToleration`，它的 hint 发现「之前有不容忍的污点，现在没有了」，返回 `Queue`。Pod 第二次被唤醒，这次通过。

两个插件各自只对自己负责的那份状态作出反应，**组合起来正好覆盖了异步的时间差**。这也是 QueueingHint 设计的一个隐含前提：Pod 的 `UnschedulablePlugins` 在每次失败后都会被刷新（开篇第五节：`handleSchedulingFailure` 用本轮 `Diagnosis` 里的拒绝者覆盖这个字段），只要每个插件都正确地声明「什么变化能让我改判」，Pod 就不会卡在一个已经过时的拒绝者上。

两次唤醒解释了 uncordon 之后会发生什么。剩下几个情况不在正常流程里。

## 五、边界情况

**kubelet 不检查 `spec.unschedulable`。** 上一篇列出了 kubelet 准入时跑的谓词：资源、节点亲和、节点名、端口、`NoExecute` 污点。没有 cordon，也没有 `NoSchedule` 污点。直接写 `spec.nodeName` 的 Pod、静态 Pod，都能在 cordon 掉的节点上运行。cordon 是一个纯调度层面的约定。

**节点新增时就是 cordon 状态。** 有些集群的节点注册流程是先 cordon、初始化完成后再 uncordon。新增事件到来时 hint 看到 `modifiedNode.Spec.Unschedulable` 为 `true`，返回 `QueueSkip`；等 uncordon 时才唤醒等待的 Pod。

**cordon 不影响打分。** 插件没有 Score，被 cordon 的节点要么被 Filter 排除，要么（对容忍的 Pod）和其他节点同等对待。不存在「尽量不去 cordon 的节点」这种软语义，需要软语义时应该使用 `PreferNoSchedule` 污点。

**`NodeUnschedulable` 不实现 `PreFilterExtensions`。** 它的判定只依赖 Node 和待调度 Pod 自身，与节点上有哪些 Pod 无关，抢占模拟时增删 Pod 不会改变结果，不需要 `AddPod`/`RemovePod`。

这个字段之所以还在，要从 cordon 从独立谓词演变成现在这个结构的历史说起。

## 六、发散：从 spec 字段到污点的演进

`spec.unschedulable` 是 Node API 最早的字段之一，比污点机制早得多。早期调度器有一组独立的谓词，分别检查节点是否 Ready、是否有内存压力、磁盘压力、是否被 cordon，每加一种节点状态就多一个谓词，而且「哪些 Pod 可以例外」要在每个谓词里单独硬编码，使用方无法自己声明例外。

污点与容忍出现后，社区做了一次统一（TaintNodesByCondition）：节点状态全部由 node-lifecycle-controller 翻译成 `NoSchedule` 污点，例外规则全部改成容忍，调度器只需要一个 `TaintToleration` 插件就能处理所有情况。上面 `doNoScheduleTaintingPass` 里的映射表就是这次统一的产物，那批按条件过滤的谓词随之删除，`ErrReasonUnknownCondition` 就是那时的遗留。

**`spec.unschedulable` 没有被同样删除，因为它是 kubectl cordon、各种运维工具和云厂商节点池管理的公共接口，改不动。** 于是留下了现在的结构：

1. 运维操作写 spec；
2. 控制器补污点；
3. 调度器用 `NodeUnschedulable` 守住异步窗口，并用同一个容忍作为例外条件，保持两边语义一致。

对生产环境的启示是：

1. 想让某类 Pod 能进入 cordon 的节点（比如节点维护时仍需要运行的诊断工具），给它加 `node.kubernetes.io/unschedulable:NoSchedule` 的容忍，不需要别的手段；
2. 想要「尽量别去」而不是「绝对别去」，用自定义的 `PreferNoSchedule` 污点，不要用 cordon；
3. 排查「节点 uncordon 了 Pod 却还 Pending」时，看事件里最近一次失败的插件：如果已经从 `NodeUnschedulable` 变成 `TaintToleration`，说明在等控制器删污点，检查 kube-controller-manager 是否正常。

## 七、小结

1. `NodeUnschedulable` 的 Filter：`spec.unschedulable` 为真且 Pod 不容忍 `node.kubernetes.io/unschedulable:NoSchedule` 时返回 `UnschedulableAndUnresolvable`，原因 `node(s) were unschedulable`，不会触发抢占。
2. 对普通 Pod，PreFilter 返回 `nil, nil`；只有被推迟扩容的 Pod（Alpha gate）返回 `Skip`。插件没有 CycleState、没有 Score、没有 `PreFilterExtensions`。
3. uncordon 在调度器里被翻译成 `UpdateNodeTaint` 事件，hint 只在节点从 cordon 变为可调度、或新增一个可调度节点时返回 `Queue`；Pod 新增容忍时，只有新容忍覆盖了 unschedulable 污点才返回 `Queue`。
4. node-lifecycle-controller 会异步补上同名污点，调度器直接读 spec 来覆盖这段时间差；uncordon 后 Pod 可能先后被两个插件拒绝和唤醒。
5. kubelet 不检查 cordon，绕过调度器的 Pod 可以运行在 cordon 掉的节点上。

[下一篇](/posts/sched-plugin-05-taint-toleration/)读 `TaintToleration`：它同时有 Filter 和 Score，三种污点效果在调度器、kubelet、taint-eviction-controller 之间各有分工。
