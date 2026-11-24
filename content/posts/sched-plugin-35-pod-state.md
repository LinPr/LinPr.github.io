---
title: 'scheduler-plugins 35 [out-of-tree] PodState：用终止中和被提名的 Pod 数给节点打分'
date: 2026-11-19T13:50:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "preemption"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "PodState 是 scheduler-plugins 里的示例打分插件：节点上终止中的 Pod 越多分越高，被提名到该节点的 Pod 越多分越低。本文拆解它从 NodeInfo 和调度队列 nominator 两处取数的方式、差值加 min-max 的计分，解释这样设计背后与抢占流程的关系，以及只数个数、不看优先级、会把自己算进去等局限。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `PodState` 实现的扩展点（Score、NormalizeScore）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/35.svg" link="/images/posts/sched-plugins/callstack/35.svg" caption="PodState 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇 [NodeMetadata](/posts/sched-plugin-34-node-metadata/) 看的是节点自身的属性。本篇的 `PodState` 看的是节点上 Pod 的「状态」：有多少 Pod 正在终止，有多少 Pod 被提名要来这个节点。这两类 Pod 都是 [DefaultPreemption](/posts/sched-plugin-18-default-preemption/) 的产物，所以它和抢占的关系比名字看起来紧密得多。

`PodState` 只实现 Score 扩展点（含 `NormalizeScore`），没有参数、没有 feature gate。它在 `cmd/scheduler/main.go` 的「Sample plugins」注释之后注册，README 里的成熟度勾选的是 Sample：「for demonstrating and inspiring purpose」。本文基于 scheduler-plugins 提交 `cfeb3aa`（依赖 Kubernetes v1.36.4）。

## 一、全部源码

插件不到一百行，核心是一个函数：

```go {linenos=inline}
// pkg/podstate/pod_state.go
type PodState struct {
	handle fwk.Handle
}

func (ps *PodState) score(nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	var terminatingPodNum, nominatedPodNum int64
	// get nominated Pods for node from nominatedPodMap
	nominatedPodNum = int64(len(ps.handle.NominatedPodsForNode(nodeInfo.Node().Name)))
	for _, p := range nodeInfo.GetPods() {
		// Pod is terminating if DeletionTimestamp has been set
		if p.GetPod().DeletionTimestamp != nil {
			terminatingPodNum++
		}
	}
	return terminatingPodNum - nominatedPodNum, nil
}
```

原始分是「终止中的 Pod 数 − 被提名的 Pod 数」（第 16 行），可以为负。`NormalizeScore` 是标准的 min-max：

```go {linenos=inline}
// pkg/podstate/pod_state.go
func (ps *PodState) NormalizeScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	// Find highest and lowest scores.
	var highest int64 = -math.MaxInt64
	var lowest int64 = math.MaxInt64
	// ...
	// Transform the highest to the lowest score range to fit the framework's min to max node score range.
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

所有节点原始分相同时（最常见的情况：没有终止中的 Pod，也没有被提名的 Pod）`oldRange == 0`（第 11–12 行），全部 0 分，插件不影响结果。这段归一化与 [NodeResourcesAllocatable](/posts/sched-plugin-33-node-resources-allocatable/) 的逐字相同。

公式部分到此为止，剩下的是两个数从哪里来。

## 二、两个数据源

`score` 的两个输入来自完全不同的地方：一个来自本轮调度周期的快照，一个来自调度队列的实时状态。

{{< figure src="/images/posts/sched-plugins/35-podstate-score.svg" caption="终止中的 Pod 来自调度周期的 NodeInfo 快照，被提名的 Pod 来自调度队列的 nominator；算例取自 pod_state_test.go 的最后一个用例" >}}

终止中的 Pod 来自 `nodeInfo.GetPods()`（第 10–14 行），也就是本轮调度周期的 snapshot。一个 Pod 被删除后，API Server 设置 `deletionTimestamp`，kubelet 开始优雅终止，直到容器全部退出、kubelet 确认删除之前，这个 Pod 都还在 NodeInfo 里，它的 request 也仍然计入节点的已请求量。

被提名的 Pod 来自 `handle.NominatedPodsForNode`（第 9 行），调度队列内部的 nominator：

```go {linenos=inline}
// kubernetes@v1.36.4/pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) NominatedPodsForNode(nodeName string) []fwk.PodInfo {
	p.lock.RLock()
	defer p.lock.RUnlock()
	nominatedPods := p.nominator.nominatedPodsForNode(nodeName)

	pods := make([]fwk.PodInfo, len(nominatedPods))
	p.activeQ.underRLock(func(unlockedActiveQ unlockedActiveQueueReader) {
		for i, np := range nominatedPods {
			pods[i] = p.nominatedPodToInfo(np, unlockedActiveQ).DeepCopy()
		}
	})
	return pods
}
```

被提名的 Pod 是那些已经通过抢占在某个节点上「订了位置」、但还没有绑定的 Pod，它们带着 `status.nominatedNodeName`，不在 NodeInfo 里。

注意这里每个元素都做了 `DeepCopy`（第 10 行），而插件只需要长度，每个节点每次打分都要为被提名的 Pod 各拷贝一份 Pod 对象。被提名的 Pod 通常不多，开销可以接受。

## 三、算例

把上面的公式代进测试里的用例。`pod_state_test.go` 的最后一个用例有四个节点，（终止中，被提名）分别为 (5, 0)、(3, 1)、(2, 1)、(0, 1)。

1. 原始分：5、2、1、−1。
2. 最高 5，最低 −1，区间 6。
3. node1：`(5 + 1) × 100 / 6 = 100`；node2：`(2 + 1) × 100 / 6 = 50`；node3：`(1 + 1) × 100 / 6 = 33`；node4：0。

测试里的每个节点还各有 10 个普通 Pod，它们不影响分数，说明插件只关心这两类状态，与节点的负载无关。

## 四、为什么这样设计

两项一加一减不是随手定的，各自对应抢占流程里的一个状态。先看加号那一项。

### 4.1 终止中的 Pod 是「好消息」

README 的解释是：终止中的 Pod 最终会从节点上物理移除。结合 Filter 阶段看会更清楚：终止中的 Pod 的 request 仍然计入 NodeInfo，`NodeResourcesFit` 等插件把它们当成正常 Pod 对待。

**一个节点能进入 Score 阶段，说明即使这些 Pod 还没退出，新 Pod 也已经放得下。** 等它们真正退出后，节点会比现在更空。所以在其他条件相同时，偏好终止中 Pod 多的节点，相当于偏好「马上会变空」的节点。

这种节点通常出现在哪里？滚动更新时旧版本 Pod 正在退出的节点、缩容中的工作负载所在的节点、以及抢占的受害者所在的节点。

### 4.2 被提名的 Pod 是「坏消息」

被提名的 Pod 是抢占者：DefaultPreemption 为它驱逐了某个节点上的低优先级 Pod，并把它提名到这个节点，等受害者退出后它会在下一轮调度中落位。如果此时另一个 Pod 抢先占用了刚腾出的资源，抢占者可能又放不下，前一轮的驱逐就白做了。

框架本身对此有一层硬保护：Filter 阶段的 `RunFilterPluginsWithNominatedPods` 会把优先级不低于当前 Pod、且 UID 不是自己的被提名 Pod 临时加进 NodeInfo 再跑一遍 Filter（第 3 行是这两个条件）：

```go {linenos=inline}
// kubernetes@v1.36.4/pkg/scheduler/framework/runtime/framework.go
	for _, pi := range nominatedPodInfos {
		if corev1.PodPriority(pi.GetPod()) >= corev1.PodPriority(pod) && pi.GetPod().UID != pod.UID {
			nodeInfoOut.AddPodInfo(pi)
			// ...
		}
	}
```

`PodState` 是一个更粗的软约束：不管优先级，被提名的 Pod 越多，节点分越低，让调度器在多个可行节点中尽量绕开正在「等位」的节点。

### 4.3 抢占目标节点上两项会相互抵消

DefaultPreemption 选中一个节点后，会同时制造两件事：受害者被删除（变成终止中），抢占者被提名到这个节点。在 `PodState` 眼里，这个节点一边 +k（k 个受害者），一边 −1（一个抢占者）。

**驱逐了多个受害者时，节点的原始分反而是正的，会被偏好。** 这和插件「避开被提名节点」的初衷恰好相反。框架的硬保护会在 Filter 时把优先级不低于当前 Pod 的抢占者计入节点，资源不够时新 Pod 会被直接拒绝；但只要还放得下，`PodState` 就会把新 Pod 吸引到这个正在为抢占者腾位置的节点上，进一步压缩它的余量。

in-tree 也意识到了终止中受害者的意义：DefaultPreemption 的 `PodEligibleToPreemptOthers` 在 Pod 已有提名节点、且该节点上有因抢占而终止中的低优先级 Pod 时，拒绝再次抢占（第 2–7 行），让它等这些 Pod 退出：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
		if nodeInfo, _ := nodeInfos.Get(nomNodeName); nodeInfo != nil {
			for _, p := range nodeInfo.GetPods() {
				victim := preemption.NewPodVictim(p)
				if pl.isPreemptionAllowed(nodeInfo, victim, pod) && preemption.PodTerminatingByPreemption(p.GetPod()) {
					// There is a terminating pod on the nominated node.
					return false, "not eligible due to a terminating pod on the nominated node."
				}
			}
		}
```

（这段引自 Kubernetes master 提交 `aabdf5a2`。）它区分了「因抢占而终止」（`PodTerminatingByPreemption` 要求 `DisruptionTarget` condition 为 True 且 reason 为 `PreemptionByScheduler`，第 5 行）和一般的终止，而 `PodState` 只看 `DeletionTimestamp`，不做区分。

## 五、局限

五条局限，前三条来自实现，后两条来自数据本身能提供的范围。

- 只数个数：一个请求 64 核的终止中 Pod 和一个请求 0.1 核的终止中 Pod 贡献相同；一个大的抢占者和一个小的抢占者扣分也相同。如果目标是「哪个节点很快会腾出足够的资源」，需要累加 request 而不是计数。
- 不看优先级：高优先级 Pod 不必在乎低优先级的被提名 Pod（框架的 Filter 都不把它们算进来），但在 `PodState` 里同样被扣分。
- 会把自己算进去：nominator 中的提名要到 assume 时才通过 `DeleteNominatedPodIfExists` 删除。一个之前抢占成功、带着 `nominatedNodeName` 的 Pod 重新进入调度时，它自己还在 nominator 里，提名节点因此被扣一分。框架会先单独评估提名节点（`evaluateNominatedNode`），通过就只剩一个可行节点、跳过打分，所以只有提名节点 Filter 不通过、需要对全部节点打分时，这个问题才会出现，而那时提名节点本来也不在候选里。**影响很小，但它说明插件没有像框架那样排除 `pod.UID`**（对比上一节框架代码第 3 行的 `pi.GetPod().UID != pod.UID`）。
- 终止时间不可知：`deletionTimestamp` 只表示开始终止，Pod 可能有很长的 `terminationGracePeriodSeconds`，也可能因为 finalizer 卡住很久。偏好这样的节点并不总是有收益。
- 数据范围：终止中的 Pod 必须仍在 NodeInfo 里，kubelet 确认删除后就消失了；被提名的 Pod 只来自本调度器的队列，其他调度器提名的 Pod 不在其中。

## 六、发散：作为示例插件的价值

局限归局限，它作为示例的价值在于：一个打分插件可以同时用到两种完全不同来源的数据。

- snapshot 中的 NodeInfo：本轮调度周期开始时冻结的集群视图，并发打分时只读，线程安全；
- 通过 `fwk.Handle` 访问的调度器内部状态：这里是 `PodNominator` 接口，它读的是调度队列的实时状态，带锁，在本轮周期中可能变化。

后者是 out-of-tree 插件常被忽视的能力。`fwk.Handle` 还暴露了 `SnapshotSharedLister`、`SharedInformerFactory`、`ClientSet`、`IterateOverWaitingPods` 等入口（Coscheduling 就靠 `IterateOverWaitingPods` 管理 Permit 等待中的 Pod）。写自定义插件时，先看 Handle 能提供什么，常常比自己再建一套 informer 更省事，也更容易和调度器内部状态保持一致。

另一个值得注意的点是它没有 `EventsToRegister`。纯 Score 插件不会让 Pod 变成不可调度，也就不需要声明重新入队的事件；终止中、被提名的状态变化只影响下一次打分，不需要通知队列。

## 小结

- `PodState` 的原始分是「终止中的 Pod 数 − 被提名的 Pod 数」，前者来自 NodeInfo 快照，后者来自调度队列的 nominator，再做 min-max 归一化，所有节点相同时全部 0 分。
- 设计意图是偏好即将变空的节点、避开为抢占者预留的节点，是框架 `RunFilterPluginsWithNominatedPods` 硬保护之外的一层软偏好。
- 它只数个数，不看资源量和优先级，抢占目标节点上两项会相互抵消甚至变成加分；在示例之外直接用于生产需要谨慎。

下一篇 [SySched](/posts/sched-plugin-36-sysched/) 换一个完全不同的视角：不看资源，而是看节点上的 Pod 一共打开了多少系统调用。
