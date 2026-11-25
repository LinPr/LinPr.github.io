---
title: 'scheduler-plugins 24 [in-tree] DefaultBinder：一次 POST 收尾整条调度链'
date: 2026-11-19T12:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "binding", "assume cache"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "DefaultBinder 只有一个 Bind 方法：构造 v1.Binding，POST 到 pods/binding 子资源。真正值得读的是它所在的绑定周期：assume 如何让调度与绑定解耦、绑定 goroutine 里各步的失败如何回滚、API Server 端 binding 子资源做了哪些检查、SchedulerAsyncAPICalls 的 API dispatcher 如何合并同一 Pod 的调用，以及怎样用 extender 或自定义 Bind 插件替换它。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `DefaultBinder` 实现的扩展点（Bind）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/24.svg" link="/images/posts/sched-plugins/callstack/24.svg" caption="DefaultBinder 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇的 PodGroupPodsCount](/posts/sched-plugin-23-podgroup-pods-count/) 为 PodGroup 的放置方案打分，到这里，一个 Pod（或一组 Pod）已经确定了要去哪个节点。默认 Profile 里剩下的最后一个插件是 `DefaultBinder`，它负责把这个决定写进 API Server。本文基于 Kubernetes master 提交 `aabdf5a2`，这也是本系列 in-tree 部分的最后一篇。

`DefaultBinder` 是默认 Profile 里**唯一的 Bind 插件**，排在 MultiPoint 列表最后，没有权重，不受任何 feature gate 控制。它在注册表里直接注册 `New`，没有像大多数插件那样套一层 `runtime.FactoryAdapter(fts, …)`，因为它根本不读 feature gate：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/registry.go
		defaultbinder.Name:                   defaultbinder.New,
```

## 一、插件本体

整个插件 76 行，核心是一个方法：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultbinder/default_binder.go
// Bind binds pods to nodes using the k8s client.
func (b DefaultBinder) Bind(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) *fwk.Status {
	logger := klog.FromContext(ctx)
	binding := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name, UID: p.UID},
		Target:     v1.ObjectReference{Kind: "Node", Name: nodeName},
	}
	if b.handle.APICacher() != nil {
		// When API cacher is available, use it to bind the pod.
		onFinish, err := b.handle.APICacher().BindPod(binding)
		if err != nil {
			return fwk.AsStatus(err)
		}
		err = b.handle.APICacher().WaitOnFinish(ctx, onFinish)
		if err != nil {
			return fwk.AsStatus(err)
		}
		return nil
	}
	logger.V(3).Info("Attempting to bind pod to node", "pod", klog.KObj(p), "node", klog.KRef("", nodeName))
	err := util.BindPod(ctx, b.handle.ClientSet(), binding)
	if err != nil {
		return fwk.AsStatus(err)
	}
	return nil
}
```

有两条路径：

1. `APICacher()` 非 nil（即 `SchedulerAsyncAPICalls` 开启）时，把调用交给 API dispatcher 排队执行，自己阻塞等结果（第 9–20 行）；
2. 否则直接用 clientset 同步调用（第 21–26 行）。

两条路径的失败都包成 `Error` 状态返回，**从不返回 `Unschedulable`**。

`Binding` 里带了 Pod 的 `UID`，这不是装饰，第二节会看到它被 API Server 当作前置条件。

### 1.1 同步路径：带重试的 POST

```go {linenos=inline}
// pkg/scheduler/util/utils.go
func Retriable(err error) bool {
	return apierrors.IsInternalError(err) || apierrors.IsServiceUnavailable(err) ||
		net.IsConnectionRefused(err)
}
// ...
// BindPod binds a pod to a node with retry.
func BindPod(ctx context.Context, cs kubernetes.Interface, binding *v1.Binding) error {
	bindFn := func() error {
		return cs.CoreV1().Pods(binding.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
	}

	return retry.OnError(retry.DefaultBackoff, Retriable, bindFn)
}
```

`Pods(ns).Bind` 发出的是 `POST /api/v1/namespaces/{ns}/pods/{name}/binding`。`retry.DefaultBackoff` 是 `Steps: 4, Duration: 10ms, Factor: 5.0, Jitter: 0.1`，即最多 4 次尝试，间隔约 10ms、50ms、250ms。**只有 500、503 和连接被拒三类错误会重试**（`Retriable`，第 2–5 行）。

注意 `Conflict` 不在重试列表里。绑定的冲突几乎都是确定性的（Pod 已经被绑定、UID 已经变了），重试没有意义。异步路径里的 `PodBindingCall.Execute` 调用的也是同一个 `util.BindPod`，所以两条路径的重试语义一致。

## 二、API Server 端：binding 子资源做了什么

调度器的「绑定」在 API Server 侧是一次带前置条件的原子更新：

```go {linenos=inline}
// pkg/registry/core/pod/storage/storage.go
	var preconditions *storage.Preconditions
	if podUID != "" || podResourceVersion != "" {
		preconditions = &storage.Preconditions{}
		if podUID != "" {
			preconditions.UID = &podUID
		}
		// ...
	}

	finalPod = &api.Pod{}
	err = r.store.Storage.GuaranteedUpdate(ctx, podKey, finalPod, false, preconditions, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
		// ...
		if pod.DeletionTimestamp != nil {
			return nil, fmt.Errorf("pod %s is being deleted, cannot be assigned to a host", pod.Name)
		}
		if pod.Spec.NodeName != "" {
			return nil, fmt.Errorf("pod %v is already assigned to node %q", pod.Name, pod.Spec.NodeName)
		}
		// Reject binding to a scheduling un-ready Pod.
		if len(pod.Spec.SchedulingGates) != 0 {
			return nil, fmt.Errorf("pod %v has non-empty .spec.schedulingGates", pod.Name)
		}
		pod.Spec.NodeName = machine
		// Clear nomination hint to prevent stale information affecting external components.
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.ClearingNominatedNodeNameAfterBinding) {
			pod.Status.NominatedNodeName = ""
		}
		// ...
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.PodTopologyLabelsAdmission) {
			copyLabelsWithOverwriting(pod, labels)
		}
		podutil.UpdatePodCondition(&pod.Status, &api.PodCondition{
			Type:   api.PodScheduled,
			Status: api.ConditionTrue,
		})
		// ...
		return pod, nil
	}), dryRun, nil)
```

这一段决定了绑定的语义：

1. UID 前置条件（第 2–9 行）：如果 Pod 在调度期间被删除并以同名重建，UID 不同，绑定失败。调度器不会把为旧 Pod 做的决定套到新 Pod 上。
2. 拒绝条件（第 14–23 行）：Pod 正在删除（第 14–16 行）、已经有 `nodeName`（第 17–19 行）、还有 `schedulingGates`（第 21–23 行，[第 1 篇](/posts/sched-plugin-01-scheduling-gates/)的 gate 在这里有最后一道防线）。失败都被 `assignPod` 转成 `Conflict`。
3. **`spec.nodeName` 只能写一次，这就是多个调度器同时调度同一个 Pod 时的仲裁点**：谁的 binding 先到，谁赢，后到的拿到 `already assigned` 冲突。
4. 顺带写入的字段（第 24–36 行）：`PodScheduled=True` condition；`ClearingNominatedNodeNameAfterBinding`（1.35 起 Beta，默认开）开启时清空 `nominatedNodeName`；`PodTopologyLabelsAdmission`（1.35 起 Beta，默认开）开启时把 Binding 上的 labels 拷到 Pod 上。Binding 上的这些 labels 来自 `PodTopologyLabels` 准入插件（`plugin/pkg/admission/podtopologylabels`）：binding 请求经过准入时，它按 `binding.Target.Name` 查到节点，把节点的拓扑标签合并进 `binding.Labels`。

`spec.nodeName` 一旦写入，后续就是 kubelet 的事了。kubelet 在 `pkg/kubelet/config/apiserver.go` 的 `NewSourceApiserver` 里建 ListWatch 时带了字段选择器 `spec.nodeName=<本节点名>`，过滤在 apiserver 端完成（[第 3 篇](/posts/sched-plugin-03-node-name/) 5.1 节）。`spec.nodeName` 为空时这个 Pod 不匹配任何节点的选择器；binding 写入之后，它才第一次出现在目标节点 kubelet 的 watch 里。

## 三、绑定周期：DefaultBinder 被调用之前

`DefaultBinder.Bind` 只是绑定周期的倒数第二步。要理解它的失败意味着什么，需要把整个周期看一遍。

{{< figure src="/images/posts/sched-plugins/24-binding-cycle.svg" caption="调度周期在 Permit 之后起一个 goroutine 跑绑定周期；Pod 在 cache 中从 Assumed 变为 Added，任何一步失败都走左下角的回滚（点击查看大图）" link="/images/posts/sched-plugins/24-binding-cycle.svg" >}}

### 3.1 assume：让调度周期不等 API

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
	scheduleResult, assumedPodInfo, status := sched.schedulingCycle(schedulingCycleCtx, state, schedFramework, podInfo, start, podsToActivate)
	if !status.IsSuccess() {
		sched.FailureHandler(schedulingCycleCtx, schedFramework, assumedPodInfo, status, scheduleResult.nominatingInfo, start)
		return
	}

	// bind the pod to its host asynchronously (we can do this b/c of the assumption step above).
	go sched.runBindingCycle(ctx, state, schedFramework, scheduleResult, assumedPodInfo, start, podsToActivate)
```

注释里的「because of the assumption step above」说明了整个设计的关键。`schedulingCycle` 末尾的 `prepareForBindingCycle` 先调用 `AssumeAndReserveInCache`：

```go {linenos=inline}
// pkg/scheduler/algorithm.go
	assumedPodInfo := a.prepareAssumedPod(logger, state, podInfo, host)
	if err := a.cache.AssumePod(logger, assumedPodInfo.Pod); err != nil {
		logger.Error(err, "Scheduler cache AssumePod failed")
		return assumedPodInfo, fwk.AsStatus(err)
	}
	schedFramework.DeleteNominatedPodIfExists(assumedPodInfo.Pod)

	if status := a.reserve(ctx, state, schedFramework, assumedPodInfo, podInfo.Pod, host); !status.IsSuccess() {
		if err := a.UnreserveAndForgetFromCache(ctx, state, schedFramework, assumedPodInfo, host); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "UnreserveAndForgetFromCache failed")
		}
		return assumedPodInfo, status
	}
```

assume 分三步生效：

1. `prepareAssumedPod` 深拷贝 Pod，把 `spec.nodeName` 设为选中的节点；
2. `cache.AssumePod` 把这个副本加进 cache，节点的 NodeInfo 立刻计入它的资源；
3. 下一个 Pod 的调度周期开头 `UpdateSnapshot` 时，就能看到这个节点少了这些资源。

**于是调度主循环不必等 API 往返。** 一次 binding 请求通常在几毫秒到几十毫秒，如果串行等待，调度吞吐会被 API 延迟直接卡住；有了 assume，主循环处理完 Permit 就去取下一个 Pod，绑定在后台 goroutine 里并行进行。

assume 也有顺序要求：先 `AssumePod`，再跑 Reserve 插件。Reserve 失败时，`UnreserveAndForgetFromCache` 按相反顺序回滚：

1. 对所有 Reserve 插件调用 Unreserve（包括没有执行到的那些，所以 Unreserve 必须幂等）；
2. 再 `ForgetPod`。

### 3.2 绑定 goroutine 里的各步

assume 之后，绑定 goroutine 按固定顺序走完剩下几步。

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
	// Run "permit" plugins.
	if status := schedFramework.WaitOnPermit(ctx, assumedPod); !status.IsSuccess() {
		// ...
	}

	// Any failures after this point cannot lead to the Pod being considered unschedulable.
	// We define the Pod as "unschedulable" only when Pods are rejected at specific extension points, and Permit is the last one in the scheduling/binding cycle.
	// If a Pod fails on PreBind or Bind, it should be moved to BackoffQ for retry.
	//
	// We can call Done() here because
	// we can free the cluster events stored in the scheduling queue sooner, which is worth for busy clusters memory consumption wise.
	sched.SchedulingQueue.Done(assumedPod.UID)

	// If we are going to run prebind plugins we put the pod in binding map to optimize preemption.
	if preFlightStatus.IsSuccess() {
		var podInPreBindCancel context.CancelCauseFunc
		ctx, podInPreBindCancel = context.WithCancelCause(ctx)
		defer podInPreBindCancel(nil)
		defer schedFramework.RemovePodInPreBind(assumedPod.UID)
		schedFramework.AddPodInPreBind(assumedPod.UID, podInPreBindCancel)
	}
	// Run "prebind" plugins.
	if status := schedFramework.RunPreBindPlugins(ctx, state, assumedPod, scheduleResult.SuggestedHost); !status.IsSuccess() {
		return status
	}

	// Verify that pod was not preempted during prebinding.
	bindingPod := schedFramework.GetPodInPreBind(assumedPod.UID)
	if bindingPod != nil && !bindingPod.MarkPrebound() {
		err := context.Cause(ctx)
		return fwk.AsStatus(err)
	}

	// Run "bind" plugins.
	if status := sched.bind(ctx, schedFramework, assumedPod, scheduleResult.SuggestedHost, state); !status.IsSuccess() {
		return status
	}
```

按顺序：

1. PreBindPreFlight（在上面省略的开头部分）：问每个 PreBind 插件「你这次要不要干活」。只要有插件要干活，或者 Pod 会在 Permit 等待，就先把 `nominatedNodeName` 设为目标节点。`NominatedNodeNameForExpectation`（1.35 起 Beta，默认开）引入这一步，是为了让 Cluster Autoscaler 这类外部组件在漫长的 PreBind（比如等卷挂载、等 DRA 设备就绪）期间知道「这个 Pod 马上就要落到这个节点了」，不要误判为 Pending 而去扩容。
2. WaitOnPermit（第 2–5 行）：如果 Permit 插件返回了 `Wait`（典型是 Gang 调度），在这里阻塞，直到被 Allow、Reject 或超时。**这是最后一个可能产生 `Unschedulable` 的地方。**
3. `SchedulingQueue.Done`（第 13 行）：通知队列这个 Pod 的「in-flight」阶段结束，释放这期间为它记录的集群事件。注释写得很清楚：从这里往后，失败只会进 backoffQ，不会被视为 unschedulable。
4. PreBind（第 15–26 行）：`VolumeBinding` 和 `DynamicResources` 在这里写 API。Pod 经 `AddPodInPreBind` 登记进 PodInPreBind 表，这样[抢占](/posts/sched-plugin-18-default-preemption/)选中它当受害者时，可以直接取消它的 context，不必等它绑定后再删。
5. MarkPrebound（第 28–33 行）：PreBind 结束后标记「已过 PreBind」。如果抢占在这之前取消了它，这里返回取消原因；标记之后，抢占就只能走 API 删除了。这是一个小的状态机，保证「内存中取消」和「绑定继续」不会同时发生。
6. `bind`（第 36 行）：extender 优先，然后是 Bind 插件。

### 3.3 bind 与 RunBindPlugins

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
func (sched *Scheduler) bind(ctx context.Context, schedFramework framework.Framework, assumed *v1.Pod, targetNode string, state fwk.CycleState) (status *fwk.Status) {
	logger := klog.FromContext(ctx)
	defer func() {
		sched.finishBinding(logger, schedFramework, assumed, targetNode, status)
	}()

	bound, err := sched.extendersBinding(logger, schedFramework, assumed, targetNode)
	if bound {
		return fwk.AsStatus(err)
	}
	return schedFramework.RunBindPlugins(ctx, state, assumed, targetNode)
}
```

`extendersBinding` 遍历 extender，第一个 `IsBinder()` 且 `IsInterested(pod)` 的 extender 负责绑定。它出错时有两种处理：

1. 标记了 `ignorable`，就跳过，换下一个；
2. 否则返回错误。

只要有 extender 接手，Bind 插件就不会被调用。源码上方挂着一个 TODO：`Move this to a Plugin`，extender 绑定是 framework 之前的遗留设计。

```go {linenos=inline}
// pkg/scheduler/framework/runtime/framework.go
	if len(f.bindPlugins) == 0 {
		return fwk.NewStatus(fwk.Skip, "")
	}
	// ...
	for _, pl := range f.bindPlugins {
		// ...
		status = f.runBindPlugin(ctx, pl, state, pod, nodeName)
		if status.IsSkip() {
			continue
		}
		if !status.IsSuccess() {
			if status.IsRejected() {
				// ...
				status.SetPlugin(pl.Name())
				return status
			}
			err := status.AsError()
			logger.Error(err, "Plugin Failed", "plugin", pl.Name(), "pod", klog.KObj(pod), "node", nodeName)
			return fwk.AsStatus(fmt.Errorf("running Bind plugin %q: %w", pl.Name(), err))
		}
		return status
	}
	return status
```

Bind 扩展点的语义是「**第一个不返回 Skip 的插件说了算**」。和 Filter 的「全部通过」、Score 的「加权求和」都不同，它是一个责任链。如果所有插件都返回 `Skip`，`RunBindPlugins` 返回 `Skip`，而 `Skip` 不是 `Success`，绑定周期把它当作失败处理，Pod 进 backoffQ。

绑定成功后 `finishBinding` 记录 `Scheduled` 事件（`kubectl describe pod` 里的 `Successfully assigned ns/name to node`），然后执行 PostBind 插件，激活 `podsToActivate` 里登记的 Pod。

## 四、assume 的确认与撤销

### 4.1 informer 确认：Assumed → Added

绑定成功后，Pod 的更新事件经 informer 到达调度器。调度器的 Pod informer（`pkg/scheduler/scheduler.go` 的 `newPodInformer`）只用字段选择器 `status.phase!=Succeeded,status.phase!=Failed` 排除已终止的 Pod，不按节点过滤，所以 binding 改写 `spec.nodeName` 产生的 Update 事件一定会推送过来。

`updatePod` 事件处理器发现 `oldPod` 未分配、`newPod` 已分配，就当作一次添加：

```go {linenos=inline}
// pkg/scheduler/eventhandlers.go
	} else if assignedPod(newPod) {
		// This update means binding operation. We can treat it as adding the pod to a cache
		// (addition to the cache will handle this binding appropriately).
		sched.addAssignedPodToCache(newPod)
```

cache 的 `AddPod` 识别出这是一个 assumed Pod：

```go {linenos=inline}
// pkg/scheduler/backend/cache/cache.go
	currState, ok := cache.podStates[key]
	switch {
	case ok && cache.assumedPods.Has(key):
		// When assuming, we've already added the Pod to cache,
		// Just update here to make sure the Pod's status is up-to-date.
		if err = cache.updatePod(logger, currState.pod, pod); err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Error occurred while updating pod")
		}
		if currState.pod.Spec.NodeName != pod.Spec.NodeName {
			// The pod was added to a different node than it was assumed to.
			logger.Info("Pod was added to a different node than it was assumed", "podKey", key, "pod", klog.KObj(pod), "assumedNode", klog.KRef("", pod.Spec.NodeName), "currentNode", klog.KRef("", currState.pod.Spec.NodeName))
			return nil
		}
	case !ok:
		if err = cache.addPod(logger, pod, false); err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Error occurred while adding pod")
		}
	default:
		return fmt.Errorf("pod %v(%v) was already in added state", key, klog.KObj(pod))
	}
```

**`updatePod`（第 7 行）内部是 `removePod` + `addPod(…, false)`，`removePod` 会把 key 从 `assumedPods` 里删掉，于是状态从 Assumed 变成 Added。** 之后这个 Pod 的更新和删除都走普通路径。`UpdatePod` 遇到仍处于 assumed 状态的 Pod 会报错：assumed Pod 必须先经过 Add 才能被 Update。

### 4.2 没有超时

**当前代码里，cache 没有任何过期机制。** `cacheImpl` 里与 assumed 状态相关的字段只有 `assumedPods sets.Set[string]` 和 `podStates map[string]*podState`，`podState` 里只剩一个 `pod` 字段。早期版本的 cache 有 `FinishBinding` 和一个后台清理循环：绑定结束后开始计时，超过 `durationToExpireAssumedPod` 仍未被 informer 确认，就把 assumed Pod 清掉，防止「绑定其实失败了、但调度器没收到事件」时资源被永久占用。现在整套机制已经从代码中移除，assumed 状态只会被 informer 的 AddPod 确认，或被 ForgetPod 撤销。

**不需要超时的原因是绑定周期本身已经闭环**：`util.BindPod` 要么成功（informer 一定会带来确认事件，见 4.1 节），要么返回错误（`handleBindingCycleError` 一定会 `ForgetPod`）。唯一的灰色地带是「API Server 已经写入，但响应在路上丢了」，调度器拿到超时错误而 Pod 实际已绑定。这种情况的处理在下一节。

## 五、失败回滚

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
func (sched *Scheduler) handleBindingCycleError(
	// ...
	status *fwk.Status) {
	logger := klog.FromContext(ctx)

	assumedPod := podInfo.Pod
	// trigger un-reserve plugins to clean up state associated with the reserved Pod
	if forgetErr := sched.algorithm.UnreserveAndForgetFromCache(ctx, state, fwk, podInfo, scheduleResult.SuggestedHost); forgetErr != nil {
		utilruntime.HandleErrorWithContext(ctx, forgetErr, "ForgetPod failed")
	} else {
		// "Forget"ing an assumed Pod in binding cycle should be treated as a PodDelete event,
		// as the assumed Pod had occupied a certain amount of resources in scheduler cache.
		//
		// Avoid moving the assumed Pod itself as it's always Unschedulable.
		// It's intentional to "defer" this operation; otherwise MoveAllToActiveOrBackoffQueue() would
		// add this event to in-flight events and thus move the assumed pod to backoffQ anyways if the plugins don't have appropriate QueueingHint.
		if status.IsRejected() {
			defer sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, assumedPod, nil, getDifferentUIDPreCheck(assumedPod.UID))
		} else {
			sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, assumedPod, nil, nil)
		}
	}

	sched.FailureHandler(ctx, fwk, podInfo, status, clearNominatedNode, start)
}
```

回滚分三步，顺序和 assume 正好相反：

1. Unreserve + ForgetPod（第 9–12 行）：所有 Reserve 插件撤销各自的内存登记（`VolumeBinding` 还原 PV/PVC 的 assume、`DynamicResources` 撤 in-flight 表并 patch `reservedFor`），cache 删除 assumed Pod，节点资源回到 assume 之前。
2. 广播一次「Pod 删除」（第 18–22 行）：被 assume 的 Pod 曾经占着资源，这期间可能有别的 Pod 因为资源不足被判为不可调度。**现在资源释放了，用 `EventAssignedPodDelete` 通知队列，让那些 Pod 的 QueueingHint 有机会把它们捞回来。** 被拒绝的情况（`IsRejected`，只可能来自 Permit）用 `defer` 并排除自身，原因写在注释里：否则这个事件会被记进自身的 in-flight 事件，让自己也被移到 backoffQ。
3. FailureHandler（第 25 行）：传入 `clearNominatedNode`，清空提名。`Error` 状态的 Pod 没有 `UnschedulablePlugins`，按队列规则进 backoffQ 等待重试。

### 5.1 「其实已经绑定成功了」

`handleSchedulingFailure` 里有一段专门处理这种情况：

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
		// In the case of extender, the pod may have been bound successfully, but timed out returning its response to the scheduler.
		// It could result in the live version to carry .spec.nodeName, and that's inconsistent with the internal-queued version.
		// For deferred resize pods, being assigned to a node in the cache is expected and not an inconsistent extender binding timeout.
		if len(cachedPod.Spec.NodeName) != 0 && !isDeferredResize {
			logger.Info("Pod has been assigned to node. Abort adding it back to queue.", "pod", klog.KObj(pod), "node", cachedPod.Spec.NodeName)
			// We need to call DonePod here because we don't call AddUnschedulablePodIfNotPresent in this case.
		} else {
```

**如果 informer 里的 Pod 已经有 `nodeName`（第 5 行），就不再放回队列。** 此时 cache 里 assumed 的副本已经被 Forget 了，等 informer 的 Update 事件到达时，`AddPod` 走 `!ok` 分支，把它当作一个普通的已分配 Pod 加进来。资源统计在这个短暂窗口里少算了一次，但很快会自我修正。

条件里的 `isDeferredResize` 是这条规则的一个例外：原地扩缩容的 Pod 在 cache 里带 `nodeName` 是预期状态，不能当成「extender 绑定成功但响应丢了」的证据，所以这类 Pod 照样回队。

如果事件比 Forget 先到呢？过程分三步：

1. `AddPod` 走 assumed 分支，状态变成 Added；
2. 随后 `ForgetPod` 发现它已经不在 `assumedPods` 里，`validateAssumedPod` 报错；
3. `handleBindingCycleError` 记录 `ForgetPod failed` 并跳过广播。

cache 的状态仍然是对的。

## 六、SchedulerAsyncAPICalls：API dispatcher

`SchedulerAsyncAPICalls` 在 1.34 以 Beta 引入，**默认关闭**。开启后，调度器里几类 Pod 写操作不再由调用方直接发请求，而是交给一个统一的调度器：

```go {linenos=inline}
// pkg/scheduler/framework/api_calls/api_calls.go
var Relevances = fwk.APICallRelevances{
	PodStatusPatch: 1,
	PodBinding:     2,
}
```

目前只有两种调用：Pod status patch（失败时写 condition 和 `nominatedNodeName`、清理低优先级 Pod 的提名）和 Pod binding。

{{< figure src="/images/posts/sched-plugins/24-api-dispatcher.svg" caption="API dispatcher 的数据结构与合并规则：同一个 Pod UID 在队列里最多只有一个调用，binding 会覆盖尚未发出的 status patch（点击查看大图）" link="/images/posts/sched-plugins/24-api-dispatcher.svg" >}}

### 6.1 callQueue：按对象合并

```go {linenos=inline}
// pkg/scheduler/backend/api_dispatcher/call_queue.go
func (cq *callQueue) merge(oldAPICall, apiCall *queuedAPICall) error {
	if cq.isLessRelevant(oldAPICall.CallType(), apiCall.CallType()) {
		// The new API call is less relevant than the existing one, so skip it.
		err := fmt.Errorf("a more relevant call is already enqueued for this object: %w", fwk.ErrCallSkipped)
		apiCall.sendOnFinish(err)
		return err
	}
	if oldAPICall.CallType() != apiCall.CallType() {
		if cq.inFlightEntities.Has(oldAPICall.UID()) {
			// Old API call is in-flight. The new call needs to wait for the old one to finish.
			return nil
		}
		// API call types don't match, so we overwrite the old one.
		// ...
		oldAPICall.sendOnFinish(fmt.Errorf("a more relevant call was enqueued for this object: %w", fwk.ErrCallOverwritten))
		return nil
	}

	// Send a concrete APICall object to allow for a type assertion.
	err := apiCall.Merge(oldAPICall.APICall)
	// ...
	oldAPICall.sendOnFinish(fmt.Errorf("a call of the same type was enqueued for this object: %w", fwk.ErrCallOverwritten))

	// If we still need the API call or the previous one is in-flight, we proceed.
	return nil
}
```

`merge` 只处理「同一个对象已经有一个调用在队列里」时新来的这个，三条路：

1. 新调用的 relevance 更低：直接丢弃，新调用收到 `ErrCallSkipped`（第 3–8 行）；
2. 两者类型不同：旧调用正在执行就让新调用排队等它结束，否则用新调用覆盖旧的，旧的收到 `ErrCallOverwritten`（第 9–18 行）；
3. 类型相同：调用各自的 `Merge` 把两个请求合成一个，旧的同样收到 `ErrCallOverwritten`（第 20–23 行）。

callQueue 维护三样东西：`apiCalls` 按 UID 存当前有效的调用，`callsQueue` 是等待执行的 UID 的 FIFO，`inFlightEntities` 是正在执行的 UID 集合。不变量是：**同一个对象同一时刻最多一个调用在执行**，后来的调用要么合并进去，要么覆盖，要么被丢弃。

这个设计针对的是一个具体问题：绑定周期开头会 patch `nominatedNodeName`（3.2 节第 1 步），紧接着又要 binding。两种模式的处理不同：

1. 同步模式下，这是两次串行的 API 请求；
2. 在 dispatcher 里，如果 patch 还没来得及发出，binding 到达时直接覆盖它。

**binding 会在 API Server 端清空 `nominatedNodeName`，先写再清的那次 patch 本来就是多余的。**

relevance 的含义因此是「谁的效果能涵盖谁」。binding 涵盖了 status patch，所以两个方向的处理是：

1. binding 到达时，覆盖排队中的 patch；
2. patch 到达时，如果 binding 已在队列或执行中，patch 直接被跳过。

### 6.2 执行与等待

```go {linenos=inline}
// pkg/scheduler/backend/api_dispatcher/api_dispatcher.go
			apiCall, err := ad.callQueue.pop()
			// ...
			go func() {
				startTime := time.Now()

				err := apiCall.Execute(ctx, ad.client)
				// ...
				ad.callQueue.finalize(apiCall)
				apiCall.sendOnFinish(err)
			}()
```

`Run` 的主循环每弹出一个调用就起一个 goroutine 执行（第 4–11 行）。`finalize` 比较 map 里的 `callID`，有两种结果：

1. 没变，就删除记录；
2. 变了，说明执行期间有新调用登记进来，把 UID 重新放回队列。

`DefaultBinder` 在另一端调用 `WaitOnFinish`：

```go {linenos=inline}
// pkg/scheduler/backend/api_cache/api_cache.go
func (c *APICache) WaitOnFinish(ctx context.Context, onFinish <-chan error) error {
	select {
	case err := <-onFinish:
		if fwk.IsUnexpectedError(err) {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
```

**`ErrCallSkipped` 和 `ErrCallOverwritten` 都被当作成功。** 对 binding 来说，被覆盖只可能是被另一个 binding 覆盖，这在正常流程中不会发生。

`apidispatcher.New` 接收一个 `parallelization` 参数，包里也有一个 `goroutinesLimiter` 类型，但在本提交里 `New` 并没有使用这个参数，`Run` 对每个调用都直接起 goroutine。并发度实际上受上游约束：同一个 Pod 最多一个调用在执行，而需要 API 调用的 Pod 数取决于调度吞吐。

## 七、替换 DefaultBinder

### 7.1 extender binder

在 `KubeSchedulerConfiguration` 的 `extenders` 里配置 `bindVerb`，extender 就成为 binder。**它优先于所有 Bind 插件。** 这种方式需要一个 HTTP 服务，延迟和可用性都要自己保证，`ignorable: true` 可以让它失败时退回到插件绑定。

### 7.2 自定义 Bind 插件

更推荐的方式是写一个 Bind 插件，对自己关心的 Pod 处理，其余返回 `Skip`。配置时有一个顺序陷阱：`DefaultBinder` 是通过 MultiPoint 启用的，而框架展开插件时的顺序是：

```go {linenos=inline}
// pkg/scheduler/framework/runtime/framework.go
		// Reorder plugins. Here is the expected order:
		// - part 1: overridePlugins. Their order stay intact as how they're specified in regular extension point.
		// - part 2: multiPointEnabled - i.e., plugin defined in multipoint but not in regular extension point.
		// - part 3: other plugins (excluded by part 1 & 2) in regular extension point.
```

注释把最终的顺序分成三段，段的先后就是执行顺序：

1. part 1 是 `overridePlugins`：在常规扩展点里有显式声明的插件，顺序按常规扩展点里的写法保持不变，排在最前面；
2. part 2 是只在 `multiPoint` 里出现过、常规扩展点没列的插件；
3. part 3 是剩下的、只在常规扩展点里出现的插件。

只在 `bind.enabled` 里加一个 `MyBinder`，它属于 part 3，排在 MultiPoint 带进来的 `DefaultBinder`（part 2）之后。**`DefaultBinder` 从不返回 `Skip`，所以 `MyBinder` 永远轮不到。** 要让它先执行，需要把 `DefaultBinder` 从 MultiPoint 里拿掉，再在 Bind 扩展点上显式排序：

```yaml {linenos=inline}
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: default-scheduler
  plugins:
    multiPoint:
      disabled:
      - name: DefaultBinder
    bind:
      enabled:
      - name: MyBinder          # 不处理的 Pod 返回 Skip
      - name: DefaultBinder     # 兜底
```

如果要彻底替换，`bind.enabled` 里只写 `MyBinder` 即可。

自定义 binder 的典型用途：绑定前在 Pod 上补充注解（Binding 对象的 annotations 会被合并到 Pod 上）、把 Pod 绑定到虚拟节点后再转发到别的集群、在多集群调度器里用别的客户端写目标集群。无论哪种，都要遵守两条约定：

1. 失败要返回 `Error`，不要返回 `Unschedulable`，绑定阶段的失败框架会按 backoff 处理；
2. 不要忽略 `UID`，否则可能把决定套到同名的新 Pod 上。

## 八、边界情况与坑

1. **Bind 失败不会让 Pod 显示为 Unschedulable。** 事件里是 `FailedScheduling`，但 `PodScheduled` condition 的 reason 是 `SchedulerError` 而不是 `Unschedulable`，Pod 走 backoffQ 而不是不可调度池。排障时这是区分「放不下」和「写不进去」的标志。
2. 多个调度器竞争同一个 Pod。两个调度器各自 assume、各自发 binding，API Server 只接受第一个。输掉的那个拿到 `Conflict`，回滚后 Pod 进 backoffQ；随后 informer 带来已绑定的 Pod，`handleSchedulingFailure` 发现 `nodeName` 非空，不再放回队列。正常情况下 `schedulerName` 不同，不会出现这种竞争：每个调度器的事件处理函数都先用 `responsibleForPod` 检查 `pod.Spec.SchedulerName` 是否属于自己的某个 Profile，不属于的 Pod 不进它的调度队列。
3. **PodGroup 的绑定也是逐个进行的。** 组调度成功后，`submitPodGroupResult` 逐个处理成功的成员：先调 `prepareForBindingCycle`（assume 进 cache 并跑 Permit），紧接着为它起一个 `runBindingCycle` goroutine，再处理下一个成员，并不等全组通过 Permit。组内的绑定没有事务性：可能部分成功、部分失败。Gang 语义由 PodGroup 调度周期里的 `PlacementFeasible` 保证（[第 21 篇](/posts/sched-plugin-21-gang-scheduling/)），绑定阶段只能尽力而为。
4. Pod 在绑定期间被删除。UID 前置条件或「正在删除」检查让 binding 失败，回滚后 `handleSchedulingFailure` 发现 informer 里找不到这个 Pod，直接调用 `Done` 结束，不再重试。
5. DefaultBinder 不读 CycleState。它不依赖任何插件的状态，所以即使 PreFilter 都被 Skip 了，它也照常工作。

## 九、发散

### 9.1 为什么 Bind 也是一个插件

在调度器的扩展点设计里，Bind 是唯一一个默认实现「看起来不需要扩展」的点：写 `nodeName` 只有一种方法。把它做成插件，是为了把「怎么让 Pod 到达节点」这件事留给不同的部署形态：虚拟 kubelet 方案、多集群调度、需要在绑定前同步外部系统的场景。责任链语义让多个 binder 可以共存，每个只处理自己的那部分 Pod。

### 9.2 调度器里所有写 API 的地方

读完 in-tree 部分，可以把调度器对 API Server 的写操作列出来：

| 位置 | 写什么 | 时机 |
| --- | --- | --- |
| `DefaultBinder` | `pods/binding` | 绑定周期末尾 |
| `handleSchedulingFailure` | Pod status：`PodScheduled=False` 和 `nominatedNodeName` | 调度失败 |
| 绑定周期 PreFlight | `nominatedNodeName` | PreBind 前 |
| `DefaultPreemption` Executor | 受害者的 `DisruptionTarget` condition、DELETE 受害者、清理低优先级 Pod 的提名 | PostFilter |
| `VolumeBinding` | PV/PVC 绑定 | PreBind |
| `DynamicResources` | ResourceClaim 的 finalizer、status、删除扩展资源 claim | PreBind / Unreserve / PostFilter |

`SchedulerAsyncAPICalls` 开启后，这些写操作分成两类：

1. binding、失败时的 status patch、提名的写入与清理，都走 API dispatcher；
2. 受害者的 condition patch 与删除、插件 PreBind 里的写操作，仍是直接调用 clientset。

这张表也说明了为什么调度器需要对这么多资源的写权限。

### 9.3 吞吐与延迟

调度吞吐由串行的调度周期决定，绑定周期是并行的，理论上不影响吞吐。但绑定 goroutine 数量会随 API 延迟增长：API Server 变慢时，`scheduler_goroutines{operation="binding"}` 会升高，assumed Pod 在 cache 里停留得更久。此时调度器仍在按 assume 后的视图继续调度，决定本身不会出错，只是 Pod 从「Scheduled 事件」到「kubelet 看到」的延迟变长。

## 小结

1. `DefaultBinder` 构造一个带 UID 的 `v1.Binding`，POST 到 `pods/{name}/binding`；同步路径对 500/503/连接拒绝最多尝试 4 次，异步路径交给 API dispatcher。
2. API Server 用 UID 前置条件和「`nodeName` 只能写一次」保证绑定的原子性，同时写入 `PodScheduled=True`、清空提名、复制拓扑标签。
3. assume 让调度周期不必等 API；绑定周期在独立 goroutine 中依次执行 PreFlight、WaitOnPermit、PreBind、MarkPrebound、Bind，Permit 之后的失败只进 backoffQ。
4. 回滚是 Unreserve + ForgetPod + 广播一次 Pod 删除事件；cache 已没有过期机制，绑定周期本身闭环。
5. API dispatcher 以对象 UID 合并调用，binding（relevance 2）覆盖排队中的 status patch（relevance 1）。
6. Bind 扩展点是责任链：extender binder 优先，然后第一个不返回 `Skip` 的插件负责绑定。

## in-tree 部分收束

到这里，一个 Pod 在默认 Profile 里的旅程走完了。回头看[开篇的总地图](/posts/sched-plugin-00-journey/)，可以把 24 篇的要点压缩成三条主线：

1. 入队与排序：`SchedulingGates` 和各插件的 PreEnqueue 决定 Pod 能不能进 activeQ，`PrioritySort` 决定谁先出队，QueueingHint 决定被拒绝的 Pod 什么时候值得重试。
2. 筛选与打分：PreFilter 预计算并可能缩小节点集合，Filter 返回的状态码决定了抢占的范围（`Unschedulable` 才可抢占），Score 的权重和归一化决定了偏好之间的取舍。`DefaultPreemption` 和 `DynamicResources` 的 PostFilter 是两种不同的「腾地方」方式。
3. 预留与绑定：Reserve 在内存里占位，Permit 实现等待与成组，PreBind 写依赖资源，Bind 写 `nodeName`。assume 把调度和绑定解耦，Unreserve 和 Forget 负责全部回滚。

所有这些插件都编译在 kube-scheduler 二进制里，由 `NewInTreeRegistry` 注册。下一部分换一个二进制：[scheduler-plugins](/posts/sched-plugin-25-qos-sort/) 项目通过 `app.WithPlugin` 把 out-of-tree 插件注册进同一套 framework，构建出另一个调度器。第一个要读的是 `QOSSort`，它和 `PrioritySort` 争夺的是同一个位置：一个 Profile 只能有一个 QueueSort 插件。
