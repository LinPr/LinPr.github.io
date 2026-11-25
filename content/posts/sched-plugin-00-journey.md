---
title: 'scheduler-plugins 00 开篇：一个 Pod 的调度旅程'
date: 2026-11-19T08:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "kube-scheduler", "scheduler-plugins"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "系列开篇。沿着 ScheduleOne 的源码走一遍：Pod 怎么进入队列、怎么出队，调度周期里 PreFilter、Filter、PostFilter、Score、Reserve、Permit 依次发生什么，绑定周期为什么是异步的，失败后又是谁把它叫回来。后面 40 篇每个插件各讲一篇，这一篇是它们共用的地图。"
---

一个 Pod 被创建出来，`spec.nodeName` 为空。从这一刻开始，到它被写上节点名、由 kubelet 拉起，中间所有的决定都由 kube-scheduler 的插件做出：它能不能进入调度队列、什么时候出队、哪些节点放得下、放在哪个节点最好、要不要为它赶走别的 Pod、是否要等同组的 Pod 一起出发。

这个系列按这条路线，把插件一个一个读下来。前 24 篇是随 Kubernetes 发布的 in-tree 插件，后 16 篇是 [scheduler-plugins](https://github.com/kubernetes-sigs/scheduler-plugins) 里的 out-of-tree 插件。每篇只讲一个插件，但都会落到同一套框架上。所以开篇先把这套框架走一遍：插件在什么时候被调用，被调用时能看到什么，返回的结果会被谁怎么处理。

本系列的 in-tree 代码基于 Kubernetes master 提交 `aabdf5a2`（2026-10 下旬，1.38 开发周期），out-of-tree 代码基于 scheduler-plugins 提交 `cfeb3aa`（依赖 Kubernetes v1.36.4）。框架层面的设计讨论可以参考之前的[《从 OpenResty phase engine 到 scheduler framework》](/posts/scheduler-framework/)，这里只讲后续文章需要用到的部分。

调度器并不是孤立工作的。Pod 进入调度器之前，kube-apiserver 的准入链已经改写过它；调度器做完决定之后，还要经过 apiserver 的 binding 子资源和 kubelet 的准入才能真正运行；中间还有好几个控制器在改它依赖的对象。下面这张全景图把这条完整链路画了出来，后面 40 篇讲的插件都在图中间的 kube-scheduler 里。

{{< figure src="/images/posts/sched-plugins/00-architecture.svg" link="/images/posts/sched-plugins/00-architecture.svg" caption="Pod 从提交到运行的全景：kube-apiserver 的准入链、kube-scheduler 的队列与两个周期、binding 子资源、kubelet，虚线框是调度器之外的组件，画在它们介入主流程的位置（点击查看大图）" >}}

读图时注意三种标记：

1. ①~㉔ 是一个 Pod 正常走完的主路径，从 kubectl 提交一直到容器运行；
2. 橙色是会重试的失败，Pod 回到调度队列等下一次机会；红色是这个 Pod 到此为止，只能由控制器新建一个 Pod 重来；
3. Ⓐ~Ⓖ 虚线框是调度器之外的组件，直接画在它们介入的步骤旁边，用虚线箭头连上。同一个组件可能出现两次，比如 Ⓑ：顶部的 webhook 在 ③ 给 Pod 加上 gate，调度器上方的 gate 控制器删掉 gate 后经 ⑦ 唤醒 Pod。最底部的 Ⓐ 沿左侧虚线回到 ①，表示失败或被删除的 Pod 由控制器重建、重新走一遍。

**很多「调度器为什么这样做」的问题，答案其实在调度器之外**。图中跨组件的几条线索，后面的文章会反复用到：

1. `spec.priority` 由 ③ 里的 Priority 准入插件按 `priorityClassName` 写入，之后一直保存在 Pod 上。调度期间 activeQ 的 QueueSort 用它排队、⑫ 用它判断提名 Pod 是否占位、PostFilter 用它挑 victims；Pod 运行之后，㉔ kubelet 的节点压力驱逐排序也会用到它（第 02、18 篇）。
2. `spec.schedulingGates` 只能在创建时加（③ 的 webhook），创建后 ④ 的校验只允许删除；gate 没删干净时，㉑ 的 binding 子资源会直接拒绝绑定（第 01 篇）。
3. 调度器把决定写回集群只有一个出口：⑲ 调用 binding 子资源，由 apiserver 写入 `spec.nodeName`。除此之外它只会写 `PodScheduled` 条件、`status.nominatedNodeName`，以及在抢占时删除 victims。
4. `status.nominatedNodeName` 有两个来源：抢占成功后由失败处理写入；开启 `NominatedNodeNameForExpectation` 时，绑定周期在 PreBind 或 WaitOnPermit 之前也会写入，告诉 Cluster Autoscaler 这个 Pod 即将落在哪个节点。绑定成功时 ㉑ 把它清空（第 18 篇）。
5. ㉒ kubelet 会用同一套判断（资源、nodeAffinity、hostPort、NoExecute 污点）再检查一次。能做到「同一套」，是因为 kubelet 的准入函数 `generalFilter`（`pkg/kubelet/lifecycle/predicate.go`）直接 import 调度器的插件包，调用 `helper.Fits`、`nodename.Fits`、`nodeports.Fits`，污点和节点亲和也用的是调度器同样依赖的 `component-helpers` 函数（第 03 篇第五节）。这里失败的 Pod 直接变成 `Failed`，调度器不会再管它。
6. 被判为 unschedulable 的 Pod，要等外部对象变化才会被唤醒：Ⓔ 扩出新节点、Ⓒ 供给好 PV、Ⓑ 删掉 gate，这些事件经 ⑦ 的 QueueingHint 判断后才把它移出 `unschedulableEntities`（第五节）。

{{< figure src="/images/posts/sched-plugins/00-journey.svg" link="/images/posts/sched-plugins/00-journey.svg" caption="一个 Pod 的调度旅程：左侧是扩展点，右侧是本系列在该位置重点讲的插件（点击查看大图）" >}}

上面这张图是按扩展点画的。落到代码里，这些扩展点分别由哪个函数触发，可以看下面这张调用栈。它一直画到真正执行插件方法的那一层，读图时注意四种标记：

1. 黄色底的行：直接调用插件钩子的函数（框架里统一命名为 `runXxxPlugin`）；
2. 右侧黄色标签：它调用的方法，比如 `runFilterPlugin` 里的 `pl.Filter()`；
3. 缩进：表示谁调用了谁；
4. 右侧加粗：每一步的关键决策，比如 Filter 的失败码决定能否抢占、抢占成功本轮也算失败、assume 让绑定可以异步。

这张图可以当作全文的提纲，后文各节就是对这些结论的展开：

{{< figure src="/images/posts/sched-plugins/00-call-stack.svg" link="/images/posts/sched-plugins/00-call-stack.svg" caption="从入队到绑定的调用栈与关键决策：黄色底的行直接执行插件钩子，右侧加粗的是这一步的关键结论；左侧色条紫色是队列，蓝色是调度周期，绿色是绑定周期的 goroutine，橙色只在失败时执行（点击查看大图）" >}}

先记住几处容易看错的地方：

1. `NextEntity` 不是普通方法，它在 `scheduler.go` 里被赋成 `podQueue.Pop`，出队实际发生在 `PriorityQueue` 里，QueueSort 插件就在这一步起作用（第二节）。
2. `findNodesThatFitPod` 下面依次调用的几个函数是按顺序尝试的：先 PreFilter，再单独试提名节点，最后才遍历候选节点跑 Filter；提名节点通过就直接返回（3.2 节）。
3. 框架层分两级：`RunXxxPlugins` 负责遍历插件、处理 Skip 和失败码、记录指标，真正调用插件方法的是它下面的 `runXxxPlugin`，两级都在 `framework/runtime/framework.go`。打分多一步：`RunScorePlugins` 先按节点并行跑 `runScorePlugin`，再按插件并行跑 `runScoreExtension` 做归一化（3.5 节）。
4. PreEnqueue 和 QueueSort 不在调度循环里执行：PreEnqueue 在 Pod 进入 `activeQ` 时由队列的 `moveToActiveQ` 调用，QueueSort 的 `Less` 则是堆每次调整时的比较函数（第二节）。
5. `runBindingCycle` 和 `handleSchedulingFailure` 都挂在 `scheduleOnePod` 下，但只会走一个：调度周期成功就起 goroutine 绑定（第四节），失败就进失败处理（第五节）。绑定周期和调度周期共用同一个 `CycleState`，所以 PreBind 还能读到 Filter 阶段写下的数据。
6. 最下面青色的一段是 PodGroup 调度周期：整组 Pod 作为一个 entity 出队，在 `scheduleOnePodGroup` 里逐个试放，每个 Pod 走的仍是上面同一条 `schedulingAlgorithm` 路径，额外多了 PlacementGenerate、PlacementFeasible、PlacementScore、PodGroupPostFilter 四个扩展点（第 21~23 篇）。

## 一、出发之前：Profile 决定了这趟旅程有哪些站

在 Pod 出现之前，调度器已经按 Profile 装配好了自己的插件链。这一节先看这份名单从哪来、里面都有谁，后面的每一站都在这份名单上。

### 1.1 插件链来自 Profile

调度器启动时读取 `KubeSchedulerConfiguration`，每个 profile 对应一个 `schedulerName` 和一套插件配置。Pod 通过 `spec.schedulerName` 选择 profile，没写就是 `default-scheduler`。

默认 profile 的插件列表在 `default_plugins.go` 里，写在 `MultiPoint` 下：

```go {linenos=inline}
// pkg/scheduler/apis/config/v1/default_plugins.go
func getDefaultPlugins() *v1.Plugins {
	plugins := &v1.Plugins{
		MultiPoint: v1.PluginSet{
			Enabled: []v1.Plugin{
				{Name: names.SchedulingGates},
				{Name: names.PrioritySort},
				{Name: names.NodeName},
				{Name: names.NodeUnschedulable},
				{Name: names.TaintToleration, Weight: ptr.To[int32](3)},
				{Name: names.NodeAffinity, Weight: ptr.To[int32](2)},
				{Name: names.NodePorts},
				{Name: names.NodeResourcesFit, Weight: ptr.To[int32](1)},
				{Name: names.VolumeRestrictions},
				{Name: names.NodeVolumeLimits},
				{Name: names.VolumeBinding},
				{Name: names.VolumeZone},
				{Name: names.PodTopologySpread, Weight: ptr.To[int32](2)},
				{Name: names.InterPodAffinity, Weight: ptr.To[int32](2)},
				// DynamicResources should come before DefaultPreemption because if
				// there is a problem with a Pod and PostFilter gets called to
				// resolve the problem, it is better to first deallocate an
				// idle ResourceClaim than it is to evict some Pod that might
				// be doing useful work.
				{Name: names.DynamicResources, Weight: ptr.To[int32](2)},
				{Name: names.DefaultPreemption},
				{Name: names.NodeResourcesBalancedAllocation, Weight: ptr.To[int32](1)},
				{Name: names.ImageLocality, Weight: ptr.To[int32](1)},
				{Name: names.DefaultBinder},
			},
		},
	}
	applyFeatureGates(plugins)

	return plugins
}
```

`MultiPoint` 的意思是：列表里的插件实现了哪些扩展点接口，就自动注册到哪些扩展点上，列表顺序就是每个扩展点内部的执行顺序（`Enabled` 列表在第 5–30 行）。`TaintToleration` 同时实现了 `FilterPlugin` 和 `ScorePlugin`，它就同时出现在 Filter 链和 Score 链上，打分权重是 3。

这份列表本身就有语义。注释里写明 `DynamicResources` 要排在 `DefaultPreemption` 前面（第 20–26 行）：两者都实现了 PostFilter，Pod 调度失败时，先尝试释放一个闲置的 ResourceClaim，比驱逐一个正在干活的 Pod 代价小。本系列的文章顺序基本就是这份列表的顺序。

### 1.2 由 feature gate 追加的插件

另外 5 个插件在对应 feature gate 打开时才追加进来。其中 `NodeDeclaredFeatures` 的 gate 在 1.37 已经 GA 并锁定开启，所以实际的默认 profile 是 20 个插件；其余几个 gate 默认关闭：

```go {linenos=inline}
// pkg/scheduler/apis/config/v1/default_plugins.go
func applyFeatureGates(config *v1.Plugins) {
	if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) {
		config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.NodeDeclaredFeatures})
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) {
		applyDeferredPodScheduling(config)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		applyGangScheduling(config)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) {
		applyTopologyAwareWorkloadScheduling(config)
	}
}
```

`DeferredPodScheduling` 不是追加在末尾，而是用 `slices.Insert` 插到 `NodeResourcesFit` 前面（这个 gate 的分支在第 6–8 行）。注释给了两条理由：

1. 要排在 `NodeName` 这类基础过滤之后，避免对无关的节点事件做无用重试；
2. 又要排在 `NodeResourcesFit` 之前，这样节点禁止扩容抢占时失败会记在它名下，策略放开时它自己的 QueueingHint 才能把 Pod 叫回来。

第 17 篇会讲它的来龙去脉。`GangScheduling`、`TopologyPlacementGenerator`、`PodGroupPodsCount` 这三个插件服务于 PodGroup 成组调度，在第 21~23 篇。

out-of-tree 插件不在这份列表里。scheduler-plugins 编译出的是另一个调度器二进制，它用 `app.WithPlugin` 把自己的插件工厂加进 Registry，再由用户在 profile 里显式启用。第 25 篇开始讲这一部分。

到这里，谁在名单上已经清楚了。接下来按一个 Pod 的真实路径往下走，第一站是队列——在它被任何人打分之前，先得排上队。

## 二、第一站：调度队列

### 2.1 三个子队列

调度器靠 Pod informer 发现新 Pod，但不是每个 Pod 都会入队。这个 informer 由 `newPodInformer`（`pkg/scheduler/scheduler.go`）创建，字段选择器 `status.phase!=Succeeded,status.phase!=Failed` 让 apiserver 只推送未结束的 Pod。收到新增事件后，`addPod`（`pkg/scheduler/eventhandlers.go`）按两个判断分流：

1. `spec.nodeName` 为空（`assignedPod` 为假），且 `spec.schedulerName` 是本调度器某个 profile 的名字（`responsibleForPod` 为真），放进调度队列；
2. `spec.nodeName` 已经有值的 Pod 不入队，只记进调度器缓存，用来计算节点上已有的占用。原地扩容被推迟的 Pod 是唯一的例外，第 03 篇会讲。

入队的 Pod 进的是调度队列 `PriorityQueue`。它内部有三个地方可以待：

1. `activeQ`：一个堆，等待被调度的 Pod 在这里排队；
2. `backoffQ`：调度失败、正在退避的 Pod，退避时间从 1 秒起（`DefaultPodInitialBackoffDuration`），每失败一次翻倍，上限 10 秒；
3. `unschedulableEntities`：被插件判定为不可调度、正在等待某个集群事件的 Pod 和 PodGroup。旧版本里它叫 `unschedulablePods`，现在因为 PodGroup 也会在这里等待而改了名，很多资料仍沿用旧名字。

进入 `activeQ` 之前先过 **PreEnqueue** 插件。任何一个 PreEnqueue 插件拒绝，Pod 就留在 `unschedulableEntities`，不会消耗调度周期。in-tree 里实现了 PreEnqueue 的插件有四个：

1. `SchedulingGates`：Pod 的 `spec.schedulingGates` 非空时拦住它；
2. `DynamicResources`：Pod 引用的 ResourceClaim 还不存在时拦住它；
3. `DefaultPreemption`：开启异步抢占（`SchedulerAsyncPreemption`）后，为这个 Pod 发起的抢占还没完成时拦住它；
4. `GangScheduling`：Pod 所属的 PodGroup 还没创建、或者组内 Pod 数还没达到法定数量时拦住它。

其中最直接、也最常被用到的是 `SchedulingGates`，第 01 篇从它开始。

`activeQ` 的出队顺序由唯一的 **QueueSort** 插件决定。限制比「每个 profile 一个」更严：整个调度器进程只有一个调度队列，堆只能有一种比较函数，所以 `validateCommonQueueSort` 要求所有 profile 的 QueueSort 插件及其参数完全相同，队列直接用第一个 profile 的比较函数。

默认的 `PrioritySort` 按优先级、再按入队时间排序（第 02 篇）。out-of-tree 的 `QOSSort`、`TopologicalSort`、`Coscheduling` 也都实现了 QueueSort，所以它们三者互斥，也不能和 `PrioritySort` 同时启用。

### 2.2 出队的不一定是单个 Pod

调度循环的入口是 `ScheduleOne`，现在从队列里取出来的是一个 entity：

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
func (sched *Scheduler) ScheduleOne(ctx context.Context) {
	logger := klog.FromContext(ctx)
	entity, err := sched.NextEntity(logger)
	// ...
	switch specificEntity := entity.(type) {
	case *framework.QueuedPodGroupInfo:
		sched.scheduleOnePodGroup(ctx, specificEntity)
	case *framework.QueuedPodInfo:
		if specificEntity.Pod == nil {
			return
		}
		sched.scheduleOnePod(ctx, specificEntity)
	// ...
	}
}
```

entity 可能是一个 Pod，也可能是一个 PodGroup，这段循环分两步：

1. `NextEntity` 取出 entity（第 4 行）；
2. `switch` 按类型分流（第 6–15 行）。

PodGroup 是 `GenericWorkload` 系列 feature 引入的成组调度路径（`schedule_one_podgroup.go`），整组 Pod 在一次周期里一起评估放置方案。第 21 篇会单独讲这条路径，这里先沿着单个 Pod 走。

## 三、调度周期：串行地选出一个节点

队列只管「谁先来」。被取出之后，Pod 进入调度周期，在这里决定落到哪个节点上。这一段是整篇文章的主干，也是后面所有插件真正工作的位置。

### 3.1 先拍快照

调度周期的第一件事不是挑节点，而是固定一份视角。

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
func (sched *Scheduler) scheduleOnePod(ctx context.Context, podInfo *framework.QueuedPodInfo) {
	// ...
	schedFramework, err := sched.frameworkForPod(pod)
	// ...
	if sched.skipPodSchedule(ctx, schedFramework, pod) {
		// ...
		return
	}
	// ...
	state := framework.NewCycleState()
	// ...
	podsToActivate := framework.NewPodsToActivate()
	state.Write(framework.PodsToActivateKey, podsToActivate)

	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduleResult, assumedPodInfo, status := sched.schedulingCycle(schedulingCycleCtx, state, schedFramework, podInfo, start, podsToActivate)
	if !status.IsSuccess() {
		sched.FailureHandler(schedulingCycleCtx, schedFramework, assumedPodInfo, status, scheduleResult.nominatingInfo, start)
		return
	}

	// bind the pod to its host asynchronously (we can do this b/c of the assumption step above).
	go sched.runBindingCycle(ctx, state, schedFramework, scheduleResult, assumedPodInfo, start, podsToActivate)
}
```

这段代码里有三个后面每篇都会用到的东西。

1. CycleState：每个调度周期新建一个（第 11 行），插件之间靠它传递数据。常见用法是 PreFilter 把计算好的结果写进去，Filter 和 Score 读出来。它是按 key 存取的键值表，值要实现 `Clone`，因为抢占模拟时需要复制一份状态在副本上试算。几乎每篇文章都会讲「这个插件在 CycleState 里存了什么」。
2. 快照：`schedulingCycle` 第一件事是 `sched.Cache.UpdateSnapshot`，把调度器缓存里的节点和 Pod 信息拷贝成一份快照。整个调度周期里插件看到的 `NodeInfo` 都来自这份快照，不会被并发的 informer 事件改动。
3. 异步绑定：调度周期成功后，调度失败的走 `FailureHandler`（第 20–23 行），成功的走到最后一行，用 `go` 启动绑定周期（第 25–26 行）。调度周期是串行的，一次只处理一个 Pod，绑定周期是并发的。这样做的前提是 assume：在发起绑定之前，调度器已经在缓存里把 Pod 当作「已经在这个节点上」，下一个 Pod 的调度周期立刻就能看到这部分资源被占用了。

### 3.2 PreFilter 与 Filter：找到放得下的节点

`schedulingAlgorithm` 调用 `SchedulePod`，后者先调用 `findNodesThatFitPod`：

```go {linenos=inline}
// pkg/scheduler/algorithm.go
func (a *SchedulingAlgorithm) findNodesThatFitPod(ctx context.Context, schedFramework framework.Framework, state fwk.CycleState, podInfo *framework.QueuedPodInfo) ([]fwk.NodeInfo, *framework.Diagnosis, string, error) {
	pod := podInfo.Pod
	preFilterOutcome, status := a.prefilterNodes(ctx, schedFramework, state, pod)
	// ...
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
	// ...
	numNodesToFind := a.numNodesToFind(schedFramework, int32(len(nodes)))
	feasibleNodes, err := a.findNodesThatPassFilters(ctx, schedFramework, state, pod, diagnosis, nodes, numNodesToFind)
	// ...
	feasibleNodes, err = a.filterWithExtenders(ctx, schedFramework, pod, feasibleNodes, diagnosis)
	// ...
}
```

顺序是：

1. PreFilter：每个插件对 Pod 做一次预计算（`prefilterNodes`，第 4 行），可以返回 `PreFilterResult` 把候选节点缩小到一个子集（`NodeName`、`NodeAffinity` 都这样做），也可以返回 `Skip` 表示「这个 Pod 和我无关，我的 Filter 也不用跑了」。
2. 提名节点快速路径：如果 Pod 之前触发过抢占，`pod.Status.NominatedNodeName` 里记着为它腾出空间的节点，先单独试这一个节点（第 9–15 行），通过就直接用它。
3. Filter：在候选节点上并行执行（`findNodesThatPassFilters`，第 20 行），每个节点上的插件串行执行，一个插件失败，这个节点就出局。
4. Extender：如果配置了 HTTP extender，再过一遍（`filterWithExtenders`，第 22 行）。

Filter 并不一定检查所有节点。`numFeasibleNodesToFind` 决定找到多少个可行节点就停：

```go {linenos=inline}
// pkg/scheduler/algorithm.go
func (a *SchedulingAlgorithm) numFeasibleNodesToFind(percentageOfNodesToScore *int32, numAllNodes int32) (numNodes int32) {
	if numAllNodes < minFeasibleNodesToFind {
		return numAllNodes
	}
	// ...
	if percentage == 0 {
		percentage = max(int32(50)-numAllNodes/125, minFeasibleNodesPercentageToFind)
	}

	numNodes = numAllNodes * percentage / 100
	if numNodes < minFeasibleNodesToFind {
		return minFeasibleNodesToFind
	}

	return numNodes
}
```

`minFeasibleNodesToFind` 是 100，`minFeasibleNodesPercentageToFind` 是 5，`percentageOfNodesToScore` 默认为 0，表示自适应。按集群规模分三种情况：

1. 节点数不到 100 时直接返回全部（第 3–5 行），所以 100 个节点以下的集群会检查全部节点；
2. 1000 个节点的集群，比例是 50 − 1000/125 = 42%（第 7–9 行算出来），找到 420 个可行节点就停；
3. 5000 个节点的集群比例是 50 − 40 = 10%，找到 500 个就停（第 11–14 行再对结果做一次下限保护）。

下一个 Pod 从上次停下的位置（`nextStartNodeIndex`）接着往后找，保证每个节点都有机会被看到。

这意味着大集群里打分阶段看到的只是一部分节点，「全局最优」只是局部最优，后面讲打分插件时会反复遇到这个前提。

### 3.3 Status code 决定了失败之后的路

插件返回的不是布尔值，而是 `Status`。它的 code 决定了调度器下一步做什么：

| Code | 含义 | 之后会发生什么 |
| --- | --- | --- |
| `Success` | 通过 | 继续 |
| `Unschedulable` | 这个节点不行，但抢占也许能解决 | 运行 PostFilter（抢占） |
| `UnschedulableAndUnresolvable` | 这个节点不行，抢占也没用 | 抢占时直接跳过这个节点 |
| `Error` | 插件内部错误 | Pod 很快重新入队 |
| `Wait` | Permit 阶段：先等等 | Pod 停在等待列表里 |
| `Skip` | PreFilter/PreScore：和我无关 | 跳过同一插件后面的 Filter/Score |
| `Pending` | 结果可用，但要等外部组件 | 重新入队时不受退避惩罚 |

`Unschedulable` 和 `UnschedulableAndUnresolvable` 的区别贯穿整个系列，因为**返回哪个 code，决定了抢占能不能解决它**。

两种 code 的典型场景：

1. 端口冲突、资源不足返回前者，因为赶走一个 Pod 就能腾出端口或资源；
2. 节点亲和不匹配、存储区域不对返回后者，因为赶走谁都改变不了节点的标签。

每篇文章都会说明插件在什么情况下返回哪一种。

### 3.4 PostFilter：没有节点时的补救

所有节点都过不了 Filter，`SchedulePod` 返回 `FitError`，`schedulingAlgorithm` 接着运行 PostFilter：

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
		// Run PostFilter plugins to attempt to make the pod schedulable in a future scheduling cycle.
		result, status := schedFramework.RunPostFilterPlugins(ctx, state, pod, fitError.Diagnosis.NodeToStatus)
		// ...
		var nominatingInfo *fwk.NominatingInfo
		if result != nil {
			nominatingInfo = result.NominatingInfo
		}
		return ScheduleResult{nominatingInfo: nominatingInfo}, fwk.NewStatus(fwk.Unschedulable).WithError(err)
```

注意返回值：**即使抢占成功，这一次调度周期仍然算失败**，返回 `Unschedulable`（第 9 行）。抢占只做了两件事：

1. 删除受害者 Pod；
2. 把节点名写进 Pod 的 `nominatedNodeName`（第 5–8 行取出 `Result` 里的 `NominatingInfo`）。

Pod 要等下一个周期才能真正放上去，那时就会走上面的「提名节点快速路径」。`DefaultPreemption` 在第 18 篇，scheduler-plugins 里改写抢占逻辑的三个插件在第 28、31、32 篇。

### 3.5 Score：在可行节点里挑一个

可行节点只有一个时直接选它，不打分。多于一个时进入 `prioritizeNodes`：PreScore 做预计算，Score 对每个节点打分，NormalizeScore 把各插件的分数归一化到 0~100（`MaxNodeScore`），再乘以 profile 里的权重求和，取总分最高的节点。

权重是理解打分的关键。默认 profile 里 `TaintToleration` 是 3，`NodeAffinity`、`PodTopologySpread`、`InterPodAffinity`、`DynamicResources` 是 2，`NodeResourcesFit`、`NodeResourcesBalancedAllocation`、`ImageLocality` 是 1。**一个插件归一化后的满分差距乘上权重，才是它对最终结果的真实影响力。**

### 3.6 Reserve 与 Permit：调度周期的最后两步

选出节点后，`prepareForBindingCycle` 做两件事：

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
	assumedPodInfo, status := sched.algorithm.AssumeAndReserveInCache(ctx, state, schedFramework, podInfo, scheduleResult)
	// ...
	// Run "permit" plugins.
	pluginsWaitTime, runPermitStatus := schedFramework.RunPermitPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)
	if runPermitStatus.IsWait() {
		schedFramework.AddWaitingPod(assumedPod, pluginsWaitTime)
	} else if !runPermitStatus.IsSuccess() {
		// trigger un-reserve plugins to clean up state associated with the reserved Pod
		err := sched.algorithm.UnreserveAndForgetFromCache(ctx, state, schedFramework, assumedPodInfo, scheduleResult.SuggestedHost)
		// ...
	}
```

**Assume + Reserve** 分两步：

1. 在缓存里把 Pod 记到目标节点上（`AssumeAndReserveInCache`，第 2 行）；
2. 调用各插件的 Reserve。

有状态的插件在这里记录状态，比如 `VolumeBinding` 把选中的 PV 假定绑定，`CapacityScheduling` 把用量计入配额。之后任何一步失败，都会调用 Unreserve 把记录的状态退回去。

**Permit**：最后一道关卡，可以批准、拒绝或让 Pod 等待（第 5–7 行）。等待的 Pod 已经占着资源，但不会进入绑定。成组调度最早就靠这个机制：一组 Pod 逐个通过调度周期，在 Permit 处等齐了再一起放行。三个实现的做法：

1. scheduler-plugins 的 `Coscheduling` 用的就是这种方式（第 27 篇）；
2. v1.36 的 in-tree `GangScheduling` 也是 PreEnqueue 加 Permit 等待；
3. master 上的 `GangScheduling` 已经换了做法：整组作为一个 entity 出队，在专门的 PodGroup 调度周期里一起试放，不再依赖 Permit 等待（第 21 篇）。

走到这里，调度周期该做的都做完了：节点选好了，状态记好了，Permit 也过了（或者正在等）。剩下的事交给另一个 goroutine。

## 四、绑定周期：异步地写回 API Server

绑定周期由 3.1 节最后那个 `go` 启动，负责把调度结果真正写回 API Server。

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
	// Run "permit" plugins.
	if status := schedFramework.WaitOnPermit(ctx, assumedPod); !status.IsSuccess() {
		// ...
	}

	// Any failures after this point cannot lead to the Pod being considered unschedulable.
	// We define the Pod as "unschedulable" only when Pods are rejected at specific extension points, and Permit is the last one in the scheduling/binding cycle.
	// If a Pod fails on PreBind or Bind, it should be moved to BackoffQ for retry.
	// ...
	sched.SchedulingQueue.Done(assumedPod.UID)
	// ...
	// Run "prebind" plugins.
	if status := schedFramework.RunPreBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost); !status.IsSuccess() {
		return status
	}
	// ...
	// Run "bind" plugins.
	if status := sched.bind(ctx, schedFramework, assumedPod, scheduleResult.SuggestedHost, state); !status.IsSuccess() {
		return status
	}
	// ...
	// Run "postbind" plugins.
	schedFramework.RunPostBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost)
```

绑定周期在自己的 goroutine 里依次执行：

1. **WaitOnPermit**：如果 Permit 返回了 Wait，在这里阻塞（第 3–5 行），直到被批准、被拒绝或超时；
2. **PreBind**：绑定前需要完成的 API 操作（第 14–16 行），比如 `VolumeBinding` 在这里真正绑定 PVC、等待动态供给的 PV 就绪，`DynamicResources` 在这里写回 ResourceClaim 的分配结果；
3. **Bind**：`DefaultBinder` 调用 Pod 的 `binding` 子资源，把 `spec.nodeName` 写进去（第 19–21 行；绑定插件本身见第 24 篇）；
4. **PostBind**：通知性质的收尾（第 23–24 行）。

源码注释里有一条重要的规则：Permit 之后的失败不会让 Pod 被视为「不可调度」，而是进入 `backoffQ` 重试。因为节点已经选好，失败原因是 API 调用之类的偶发问题，不是集群状态不满足。

这一节走的是顺利路径。但调度周期失败时，Pod 不会留在原地：下一节看它被拒绝之后去了哪里，又是靠什么回到队列。

## 五、失败之后：谁把 Pod 叫回来

调度周期失败，`handleSchedulingFailure` 记下是哪些插件拒绝了这个 Pod（第 2–4 行）：

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
	} else if fitError, ok := errors.AsType[*framework.FitError](err); ok { // Inject UnschedulablePlugins to PodInfo, which will be used later for moving Pods between queues efficiently.
		podInfo.UnschedulablePlugins = fitError.Diagnosis.UnschedulablePlugins
		podInfo.PendingPlugins = fitError.Diagnosis.PendingPlugins
		logger.V(2).Info("Unable to schedule pod; no fit; waiting", "pod", klog.KObj(pod), "err", errMsg)
	}
```

然后 `AddUnschedulablePodIfNotPresent` 决定 Pod 去哪里。它先检查这个 Pod 在被调度的这段时间里，集群有没有发生过拒绝它的插件所关心的事件（`determineSchedulingHintForInFlightPod`）。

队列能回答「这段时间里发生过什么」，是因为它从 Pod 出队那一刻就开始记录：`activeQ` 出队时把 Pod 记进 `inFlightPods`，此后只要还有 Pod 在调度中，收到的集群事件都追加到 `inFlightEvents` 列表，直到这个 Pod 调用 `Done`（失败时由 `AddUnschedulablePodIfNotPresent` 自己调用，成功时在绑定周期里调用，即第四节代码第 11 行）。`clusterEventsForPod` 取出的就是这段时间内的事件，判断结果分两种：

1. 发生过：说明它失败所依据的状态可能已经过时，直接进 `backoffQ`；
2. 没发生过：进 `unschedulableEntities` 等待。

之后它什么时候回到 `activeQ`，取决于这些插件的 `EventsToRegister` 与 QueueingHint。每个插件声明自己关心哪些集群事件，比如 `NodeResourcesFit` 关心节点资源变化和 Pod 删除，`TaintToleration` 关心节点污点变化。

事件到来时，队列只对那些「被这个插件拒绝过」的 Pod 调用插件的 hint 函数，hint 返回 `Queue` 才把 Pod 移出去，返回 `QueueSkip` 就继续等。比如一个节点只是更新了一个无关的 label，`NodeResourcesFit` 的 hint 会判断出可分配资源没有变大，返回 `QueueSkip`，被资源不足拒绝的 Pod 就不会白白重试。

这套机制决定了「Pod 一直 Pending，集群明明已经变了却不重新调度」这类问题的答案，所以本系列每篇都会讲插件注册了哪些事件、hint 怎么判断。

兜底机制是 `DefaultPodMaxInUnschedulablePodsDuration`（5 分钟）：在 `unschedulableEntities` 里待满 5 分钟的 Pod 会被强制刷回队列，即使没有等到它关心的事件。

## 六、几个容易忽略的新变化

读最新代码时，有几处和旧资料不同，后续文章会碰到：

1. **PodGroup 调度周期**：除了单个 Pod，队列里还可能出来一个 PodGroup，走 `scheduleOnePodGroup`。在这种周期里，单个 Pod 的 PostFilter 不运行，改为整组结束后运行 PodGroup 级别的 PostFilter（`schedulingAlgorithm` 中 `state.IsPodGroupSchedulingCycle()` 的分支）。
2. **Opportunistic batching**：`OpportunisticBatching`（1.35 Beta，默认开启）让签名相同、连续出队的 Pod 复用上一个 Pod 的打分结果，`findNodesThatFitPod` 里的 `nodeHint` 就来自它，和提名节点走同一条快速路径。它要求 profile 里所有相关插件都能给 Pod 签名，任何一个插件拒签，这个 Pod 就不参与批量。默认配置下 `PodTopologySpread` 的系统默认约束（`systemDefaultConstraints`，按 hostname 和 zone 打散的两条 `ScheduleAnyway` 约束）总是非空，而它的 `SignPod` 只要 Pod 自带约束或默认约束非空就拒签，因此所有 Pod 都会被它拒签。于是这个 gate 虽然开着，默认 profile 里实际上却用不上，第 13 篇会讲到这一点。
3. **NominatedNodeName 的新用途**：`bindingCycle` 里，如果 PreBind 有实际工作要做或 Pod 要在 Permit 等待，调度器会提前把目标节点写进 `nominatedNodeName`，告诉 cluster autoscaler 之类的外部组件「这个 Pod 马上要绑到这个节点」。它不再只是抢占的副产品。

## 七、阅读路线

后面 40 篇按下面的顺序排列。每篇开头会说明插件在旅程中的位置，结尾链接到下一篇，可以顺着读，也可以按需跳读：

| 段落 | 篇目 |
| --- | --- |
| 入队与排序 | [01 SchedulingGates](/posts/sched-plugin-01-scheduling-gates/) · [02 PrioritySort](/posts/sched-plugin-02-priority-sort/) |
| 节点基本属性 | [03 NodeName](/posts/sched-plugin-03-node-name/) · [04 NodeUnschedulable](/posts/sched-plugin-04-node-unschedulable/) · [05 TaintToleration](/posts/sched-plugin-05-taint-toleration/) · [06 NodeAffinity](/posts/sched-plugin-06-node-affinity/) |
| 端口与资源 | [07 NodePorts](/posts/sched-plugin-07-node-ports/) · [08 NodeResourcesFit](/posts/sched-plugin-08-node-resources-fit/) |
| 存储 | [09 VolumeRestrictions](/posts/sched-plugin-09-volume-restrictions/) · [10 NodeVolumeLimits](/posts/sched-plugin-10-node-volume-limits/) · [11 VolumeBinding](/posts/sched-plugin-11-volume-binding/) · [12 VolumeZone](/posts/sched-plugin-12-volume-zone/) |
| Pod 之间的关系 | [13 PodTopologySpread](/posts/sched-plugin-13-pod-topology-spread/) · [14 InterPodAffinity](/posts/sched-plugin-14-inter-pod-affinity/) |
| 设备与新特性 | [15 DynamicResources](/posts/sched-plugin-15-dynamic-resources/) · [16 NodeDeclaredFeatures](/posts/sched-plugin-16-node-declared-features/) · [17 DeferredPodScheduling](/posts/sched-plugin-17-deferred-pod-scheduling/) |
| 抢占 | [18 DefaultPreemption](/posts/sched-plugin-18-default-preemption/) |
| 打分 | [19 NodeResourcesBalancedAllocation](/posts/sched-plugin-19-balanced-allocation/) · [20 ImageLocality](/posts/sched-plugin-20-image-locality/) |
| 成组调度 | [21 GangScheduling](/posts/sched-plugin-21-gang-scheduling/) · [22 TopologyPlacementGenerator](/posts/sched-plugin-22-topology-placement/) · [23 PodGroupPodsCount](/posts/sched-plugin-23-podgroup-pods-count/) |
| 绑定 | [24 DefaultBinder](/posts/sched-plugin-24-default-binder/) |
| out-of-tree：排序 | [25 QOSSort](/posts/sched-plugin-25-qos-sort/) · [26 TopologicalSort](/posts/sched-plugin-26-topological-sort/) · [27 Coscheduling](/posts/sched-plugin-27-coscheduling/) |
| out-of-tree：配额与网络 | [28 CapacityScheduling](/posts/sched-plugin-28-capacity-scheduling/) · [29 NetworkOverhead](/posts/sched-plugin-29-network-overhead/) · [30 NodeResourceTopologyMatch](/posts/sched-plugin-30-node-resource-topology/) |
| out-of-tree：抢占 | [31 PreemptionToleration](/posts/sched-plugin-31-preemption-toleration/) · [32 CrossNodePreemption](/posts/sched-plugin-32-cross-node-preemption/) |
| out-of-tree：打分 | [33 NodeResourcesAllocatable](/posts/sched-plugin-33-node-resources-allocatable/) · [34 NodeMetadata](/posts/sched-plugin-34-node-metadata/) · [35 PodState](/posts/sched-plugin-35-pod-state/) · [36 SySched](/posts/sched-plugin-36-sysched/) |
| out-of-tree：Trimaran 负载感知 | [37 TargetLoadPacking](/posts/sched-plugin-37-target-load-packing/) · [38 LoadVariationRiskBalancing](/posts/sched-plugin-38-load-variation-risk-balancing/) · [39 LowRiskOverCommitment](/posts/sched-plugin-39-low-risk-overcommitment/) · [40 Peaks](/posts/sched-plugin-40-peaks/) |

读每篇时可以带着同样的四个问题：

1. 插件挂在哪些扩展点上；
2. CycleState 里存了什么；
3. 失败时返回哪种 code；
4. 被它拒绝的 Pod 靠什么事件回来。

四个问题都答得上来，这个插件就算读懂了。

旅程从 Pod 进入队列之前开始：[下一篇](/posts/sched-plugin-01-scheduling-gates/)讲 `SchedulingGates`，一个让 Pod 连排队资格都没有的插件。
