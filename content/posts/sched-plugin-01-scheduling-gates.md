---
title: 'scheduler-plugins 01 [in-tree] SchedulingGates：在排队之前拦住 Pod'
date: 2026-11-19T08:10:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "SchedulingGates", "PreEnqueue", "调度队列"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "SchedulingGates 是默认 Profile 里第一个插件，只实现 PreEnqueue：spec.schedulingGates 非空的 Pod 连 activeQ 都进不去。本文从插件的 20 行代码出发，拆开调度队列里 PreEnqueue 的调用位置、gated entity 在 unschedulableEntities 里如何被跳过、gate 删光后 Pod 怎么回到队列，以及 API Server 对 schedulingGates 只减不增的校验规则。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `SchedulingGates` 实现的扩展点（PreEnqueue）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/01.svg" link="/images/posts/sched-plugins/callstack/01.svg" caption="SchedulingGates 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-00-journey/)画了一张总地图：Pod 被 informer 发现后进入调度队列 `PriorityQueue`，经过 PreEnqueue 才能进入 `activeQ`，出队后跑一个调度周期，失败了再由 QueueingHint 决定什么时候回来。这一篇从地图的最左端开始，读默认 Profile 里排在第一位的插件 `SchedulingGates`。

它在 `default_plugins.go` 的 `MultiPoint` 列表里排第一，默认启用，不受任何 feature gate 控制（对应的 `PodSchedulingReadiness` 已经 GA 并从 `kube_features.go` 中移除）。

它只实现了两个接口：`PreEnqueuePlugin` 和 `EnqueueExtensions`，没有 Filter，没有 Score，也不参与调度周期。本文基于 Kubernetes master 提交 `aabdf5a2`。

## 一、它解决什么问题

调度器默认假设：Pod 一创建出来就可以调度。但很多场景里，Pod 创建时还缺东西：

- 批处理队列系统（如 Kueue）要先确认配额，再放 Pod 去调度；
- 外部控制器要先根据集群状态补上 `nodeSelector` 或节点亲和，再允许调度；
- 一组 Pod 要等全部创建完、或者某个外部资源（网络、存储）就绪后再一起出发。

在 `schedulingGates` 出现之前，这类需求只能靠「让 Pod 调度失败」来实现，比如先写一个不存在的 `schedulerName`，或者加一个所有节点都不满足的亲和条件。前者让 Pod 完全不被任何调度器看到，后者让 Pod 每次被唤醒都完整地跑一遍调度周期再失败，白白消耗调度吞吐。

`schedulingGates` 给了一个显式的开关：

```yaml {linenos=inline}
apiVersion: v1
kind: Pod
metadata:
  name: job-worker-0
spec:
  schedulingGates:
  - name: example.com/quota-check
  - name: example.com/network-ready
  containers:
  - name: main
    image: busybox
```

**只要列表非空，调度器就不会尝试调度它。** `kubectl get pod` 看到的状态是 `SchedulingGated`。两个外部控制器各自完成工作后，各自删掉自己那一项；列表删空，Pod 才开始排队。

调度器里承载这个语义的只有一个二十行的 PreEnqueue 插件，先看它本身。

## 二、插件本身：20 行代码

插件只做一件事：看到 gate 就拒绝。全部代码一次可以读完。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/schedulinggates/scheduling_gates.go
// SchedulingGates checks if a Pod carries .spec.schedulingGates.
type SchedulingGates struct{}

var _ fwk.PreEnqueuePlugin = &SchedulingGates{}
var _ fwk.EnqueueExtensions = &SchedulingGates{}

func (pl *SchedulingGates) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	if len(p.Spec.SchedulingGates) == 0 {
		return nil
	}
	gates := make([]string, 0, len(p.Spec.SchedulingGates))
	for _, gate := range p.Spec.SchedulingGates {
		gates = append(gates, gate.Name)
	}
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("waiting for scheduling gates: %v", gates))
}

// EventsToRegister returns the possible events that may make a Pod
// failed by this plugin schedulable.
func (pl *SchedulingGates) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// Pods can be more schedulable once it's gates are removed
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodSchedulingGatesEliminated}, QueueingHintFn: pl.isSchedulableAfterUpdateTargetPodSchedulingGatesEliminated},
	}, nil
}

// New initializes a new plugin and returns it.
func New(_ context.Context, _ runtime.Object, _ fwk.Handle, _ feature.Features) (fwk.Plugin, error) {
	return &SchedulingGates{}, nil
}

func (pl *SchedulingGates) isSchedulableAfterUpdateTargetPodSchedulingGatesEliminated(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	return fwk.Queue, nil
}
```

插件是空结构体，没有参数，没有状态，`New`（第 29–31 行）忽略了所有入参。`PreEnqueue`（第 8–16 行）只做一件事：`p.Spec.SchedulingGates` 为空时返回 `nil`（第 9–11 行）；非空时把每个 gate 的 `Name` 收集到 `gates` 切片（第 12–15 行），返回 `UnschedulableAndUnresolvable`（第 16 行），消息里用 `%v` 列出这些名字。

两处值得说明：

- 状态码选 `UnschedulableAndUnresolvable`。对 PreEnqueue 来说，任何非 Success 的状态效果都一样：Pod 不进 `activeQ`。这里没有抢占的问题，因为 Pod 根本走不到 PostFilter。选这个 code 只是在语义上表达「调度器自己解决不了，要等外部」。
- QueueingHint 无条件返回 `Queue`。它只注册了一个事件 `TargetPod/UpdatePodSchedulingGatesEliminated`，而这个事件本身已经足够精确（见第四节），hint 函数没必要再做判断。

真正让这个插件起作用的是它在队列里的位置，下面看队列这一侧。

## 三、PreEnqueue 在队列里的位置

{{< figure src="/images/posts/sched-plugins/01-queue-preenqueue.svg" link="/images/posts/sched-plugins/01-queue-preenqueue.svg" caption="PreEnqueue 是进入 activeQ（以及开启 SchedulerPopFromBackoffQ 时进入 backoffQ）之前的 gate，被拒绝的 entity 以 gated 状态留在 unschedulableEntities（点击查看大图）" >}}

队列这一侧要回答两个问题：gate 架在哪些路径上，被拦下的 entity 又停在哪里。

### 3.1 三个子队列

`PriorityQueue` 的注释把三个子队列写得很清楚：

```go {linenos=inline}
// pkg/scheduler/backend/queue/scheduling_queue.go
// PriorityQueue implements a scheduling queue.
// The head of PriorityQueue is the highest priority pending entity (pod or pod group). This structure
// has two sub queues and an additional data structure, namely: activeQ,
// backoffQ and unschedulableEntities.
//   - activeQ holds pods or pod groups that are being considered for scheduling.
//   - backoffQ holds pods or pod groups that moved from unschedulableEntities and will move to
//     activeQ when their backoff periods complete.
//   - unschedulableEntities holds pods or pod groups that were already attempted for scheduling and
//     are currently determined to be unschedulable.
```

注意注释的措辞：「two sub queues and an additional data structure」。`activeQ` 和 `backoffQ` 是堆，`unschedulableEntities` 只是一个 map，里面的 entity 没有顺序，因为它们本来就不会被 Pop，只会被事件「捞」出来：

```go {linenos=inline}
// pkg/scheduler/backend/queue/unschedulable_entities.go
// unschedulableEntities holds pods and pod groups that cannot be scheduled.
type unschedulableEntities struct {
	// entityInfoMap is a map keyed by an entity's key and the value is the QueuedEntityInfo.
	entityInfoMap map[string]framework.QueuedEntityInfo
	keyFunc       func(framework.QueuedEntityInfo) string
	// unschedulableRecorder and gatedRecorder track the number of entities in the unschedulable queue.
	// unschedulableRecorder tracks standard unschedulable entities, while gatedRecorder tracks entities
	// that are specifically blocked by scheduling gates.
	unschedulableRecorder, gatedRecorder metrics.MetricRecorder
}
```

**被 PreEnqueue 拦下的 entity 和调度失败的 entity 住在同一个 map 里，只是指标分开统计**：前者计入 `scheduler_pending_pods{queue="gated"}`，后者计入 `queue="unschedulable"`。排查「Pod 为什么没被调度」时，先看这两个指标就能分清是没排上队还是排上了但调度失败。

三个子队列的分工清楚了，下一个问题是 entity 从哪条路径被放进 `activeQ`。

### 3.2 两个入口都会经过 gate

既然 gated entity 停在 `unschedulableEntities`，gate 就必须架在 entity 进入 `activeQ` 的唯一入口上。这个入口是 `moveToActiveQ`，它先跑 PreEnqueue：

```go {linenos=inline}
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) moveToActiveQ(logger klog.Logger, entity framework.QueuedEntityInfo, event string, movesFromBackoffQ bool, strategy *queueingStrategy) bool {
	gatedBefore := entity.Gated()
	// If SchedulerPopFromBackoffQ feature gate is enabled,
	// PreEnqueue plugins were called when the entity was added to the backoffQ.
	// Don't need to repeat it here when the entity is directly moved from the backoffQ.
	skipPreEnqueue := p.isPopFromBackoffQEnabled && movesFromBackoffQ
	if !skipPreEnqueue {
		p.runPreEnqueuePlugins(context.Background(), entity)
	}

	if entity.Gated() {
		// ...
		entity.SetWasFlushedFromUnschedulable(false)
		p.unschedulableEntities.addOrUpdate(entity, gatedBefore, event, strategy)
		// Entity not moved to activeQ.
		return false
	}
	now := p.clock.Now()
	entity.SetInitialAttemptTimestamp(now)
	p.unschedulableEntities.delete(entity, gatedBefore)

	p.activeQ.add(logger, entity, event, strategy)
	// Pod successfully moved to activeQ.
	return true
}
```

`SchedulerPopFromBackoffQ`（1.33 Beta，默认开启）打开后，`activeQ` 空闲时调度器会直接从 `backoffQ` 里 Pop，这样 `backoffQ` 里的 entity 也可能被直接调度。`moveToActiveQ` 里的 `skipPreEnqueue`（第 7–10 行）就是为此准备的：从 `backoffQ` 直接过来的 entity 不再重复跑 PreEnqueue；被拒绝的 entity 走第 12–18 行的分支，放回 `unschedulableEntities`，不进入 `activeQ`。

gate 因此必须前移：`moveToBackoffQ` 在入队时就跑 PreEnqueue，被拒绝的同样放回 `unschedulableEntities`；之后从 `backoffQ` 移到 `activeQ` 时（`movesFromBackoffQ=true`）就不再重复跑。

```go {linenos=inline}
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) moveToBackoffQ(logger klog.Logger, entity framework.QueuedEntityInfo, event string, strategy *queueingStrategy) bool {
	gatedBefore := entity.Gated()
	// If SchedulerPopFromBackoffQ feature gate is enabled,
	// PreEnqueue plugins are called on inserting entities to the backoffQ,
	// not to call them again on popping out.
	if p.isPopFromBackoffQEnabled {
		p.runPreEnqueuePlugins(context.Background(), entity)
		if entity.Gated() {
			// ...
			p.unschedulableEntities.addOrUpdate(entity, gatedBefore, event, strategy)
			return false
		}
	}
	p.unschedulableEntities.delete(entity, gatedBefore)

	p.backoffQ.add(logger, entity, event, strategy)
	return true
}
```

**无论 entity 从哪个方向往「可以被 Pop 的地方」走，都要先过 PreEnqueue。** 这也解释了 PreEnqueue 插件的一个约束：它会在队列锁内被频繁调用，必须是 O(1) 的纯检查，不能发 API 请求。`SchedulingGates` 只看一个切片长度，符合这个要求。

### 3.3 gate 内部：gating 插件先跑，其余顺序不固定

两个入口调的是同一套检查，而这套检查面对的是一个插件集合。什么时候问哪个插件，决定了上一次的拦截结果能不能被复用。

```go {linenos=inline}
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) runPreEnqueuePluginsForPod(ctx context.Context, pInfo *framework.QueuedPodInfo) {
	// ...
	gatingPlugin := pInfo.GatingPlugin
	if gatingPlugin != "" {
		// Run the gating plugin first
		s := p.runPreEnqueuePlugin(ctx, p.preEnqueuePluginMap[pod.Spec.SchedulerName][gatingPlugin], pInfo, shouldRecordMetric)
		if !s.IsSuccess() {
			// No need to iterate other plugins
			return
		}
	}

	for _, pl := range p.preEnqueuePluginMap[pod.Spec.SchedulerName] {
		if gatingPlugin != "" && pl.Name() == gatingPlugin {
			// should be run already above.
			continue
		}
		s := p.runPreEnqueuePlugin(ctx, pl, pInfo, shouldRecordMetric)
		if !s.IsSuccess() {
			// No need to iterate other plugins
			return
		}
	}
	// all plugins passed
	pInfo.GatingPlugin = ""
}
```

`preEnqueuePluginMap` 的类型是 `map[string]map[string]fwk.PreEnqueuePlugin`，按 profile 名和插件名索引。Go 的 map 遍历顺序是随机的，所以**PreEnqueue 插件之间没有固定的执行顺序**，这和 Filter 按配置顺序执行不同。唯一的确定性是：上次拦住这个 Pod 的插件（`GatingPlugin`）先跑（第 4–12 行），它仍然拒绝就直接返回，其他插件不用再跑。

拒绝时，`runPreEnqueuePlugin` 记下三样东西（第 2–4 行）：

```go {linenos=inline}
// pkg/scheduler/backend/queue/scheduling_queue.go
	pInfo.UnschedulablePlugins.Insert(pl.Name())
	pInfo.GatingPlugin = pl.Name()
	pInfo.GatingPluginEvents = p.pluginToEventsMap[pInfo.GatingPlugin]
```

`GatingPlugin` 非空就是「gated」的定义（`QueuedPodInfo.Gated()` 只判断这个字段）；`GatingPluginEvents` 是这个插件在 `EventsToRegister` 里声明的事件，对 `SchedulingGates` 来说只有一个 `TargetPod/UpdatePodSchedulingGatesEliminated`。

默认 Profile 里其实有三个插件实现了 PreEnqueue：`SchedulingGates`、`DynamicResources`（Pod 引用的 ResourceClaim 还不存在时拦住）、`DefaultPreemption`（开启 `SchedulerAsyncPreemption` 后，Pod 的异步抢占还在进行时拦住）。`GenericWorkload` 开启后还会加上 `GangScheduling`。

它们共用这套机制，只是拦截条件不同。gate 架在哪、谁先跑，到这里都清楚了。剩下的是被拦住的 entity 怎么被放回来。

## 四、gated 的 Pod 怎么回来

这一节把 ungate 到重新可调度的路径走一遍：先看哪些事件能唤醒它，再看这个事件怎么产生、退避怎么算，最后看两个容易忽略的角落。

### 4.1 只有一种事件能唤醒它

集群里每时每刻都有节点更新、Pod 删除之类的事件，每个事件都会调用 `MoveAllToActiveOrBackoffQueue`，遍历 `unschedulableEntities`。如果被 gate 住的 Pod 有成千上万个，每次都给它们跑一遍 QueueingHint 是浪费。队列在这里做了一个短路：

```go {linenos=inline}
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) moveEntitiesToActiveOrBackoffQueue(logger klog.Logger, entityInfoList []framework.QueuedEntityInfo, event fwk.ClusterEvent, oldObj, newObj interface{}, hintKeys *preQueueingHintPodKeys) {
	// ...
	for _, entity := range entityInfoList {
		// As an optimization, we avoid re-evaluating gated entities for events unrelated to their gating plugin.
		// However, wildcard events (e.g., periodic flushes) always trigger re-evaluation to entities pods don't
		// get stuck due to incomplete or incorrect queueing hints.
		if entity.Gated() && !framework.ClusterEventIsWildCard(event) && !framework.MatchAnyClusterEvent(event, entity.GetGatingPluginEvents()) {
			// This event doesn't interest the gating plugin of this Pod,
			// which means this event never moves this Pod to activeQ.
			continue
		}
		// ...
	}
	// ...
}
```

**gated entity 只对 `GatingPluginEvents` 里的事件和通配事件有反应**（第 8–12 行）：节点变化、别的 Pod 删除，一律跳过，连 hint 函数都不调。

### 4.2 「全部删光」才产生事件

`UpdatePodSchedulingGatesEliminated` 来自 Pod 更新时的事件提取函数：

```go {linenos=inline}
// pkg/scheduler/framework/events.go
func extractPodSchedulingGateEliminatedChange(newPod *v1.Pod, oldPod *v1.Pod) fwk.ActionType {
	if len(newPod.Spec.SchedulingGates) == 0 && len(oldPod.Spec.SchedulingGates) != 0 {
		// A scheduling gate on the pod is completely removed.
		return fwk.UpdatePodSchedulingGatesEliminated
	}

	return fwk.None
}
```

提取条件很严（第 3–6 行）：旧 Pod 有 gate、新 Pod 一个都没有。两个 gate 删掉一个，不产生这个事件；如果这次更新也没有触发其他提取函数，产生的是通用的 `TargetPod/Update`。

后者也匹配不上。`MatchClusterEvents` 要求 `incomingEvent.ActionType <= ce.ActionType`，`Update` 是所有更新位的并集，大于 `UpdatePodSchedulingGatesEliminated`，所以 Pod 继续留在原地。**这正是「hint 无条件返回 `Queue`」敢这么写的原因：能走到 hint 的事件，一定是 gate 已经删光了。**

完整的唤醒路径在 `PriorityQueue.Update` 里：Pod 在 `unschedulableEntities` 中，就对每个提取出来的事件调用 `isEntityWorthRequeuing`，`SchedulingGates` 在 `UnschedulablePlugins` 里、事件匹配、hint 返回 `Queue`，策略是 `queueAfterBackoff`；然后 `requeueEntityWithQueueingStrategy` 检查退避：

```go {linenos=inline}
// pkg/scheduler/backend/queue/backoff_queue.go
	count := entity.GetUnschedulableCount()
	if entity.GetConsecutiveErrorsCount() > 0 {
		// ...
		count = entity.GetConsecutiveErrorsCount()
	}

	if count == 0 {
		// When the entity hasn't experienced any scheduling attempts,
		// they don't have to get a backoff.
		return time.Time{}
	}
```

被 gate 住的 Pod 从未进过调度周期，`UnschedulableCount` 为 0（`count` 在第 2 行取出，第 8–12 行判断为零时直接返回零值时间），没有退避，直接走 `moveToActiveQ`。PreEnqueue 再跑一遍，`SchedulingGates` 放行，Pod 进入 `activeQ`。

**从 gate 删光到 Pod 可以被 Pop，中间没有任何人为延迟。**

### 4.3 兜底刷新不会把它放出来

上面的路径依赖事件和 hint 都正确。万一插件把 hint 写错、事件没产生，队列还有一层定时兜底：每 30 秒执行一次 `flushUnschedulableEntitiesLeftover`，把在 `unschedulableEntities` 里待超过 5 分钟的 entity 以通配事件 `UnschedulableTimeout` 重新评估。

通配事件不会被 4.1 的短路跳过，所以 gated 的 Pod 大约每 5 分钟会被拿出来重跑一次 PreEnqueue，`moveToActiveQ` 发现它仍然 gated，原样放回。这个兜底是为了防止插件的 QueueingHint 写错导致 Pod 卡死；对 `SchedulingGates` 来说，**它没有实际作用，只是一次很便宜的空转**。

### 4.4 一个副作用：排队时间从创建时算起

`QueuedPodInfo.Timestamp` 只在两个时刻赋值：Pod 第一次加入队列时（`newQueuedPodInfo`），和调度失败回队时（`AddUnschedulablePodIfNotPresent`）。gated 的 Pod 没有失败过，ungate 时走的 `entity.Update(newPod)` 只替换 Pod 对象，不动 `QueueingParams`。

**所以一个被 gate 了一小时的 Pod，ungate 后的 `Timestamp` 仍是一小时前。** 下一篇会讲到 `PrioritySort` 在同优先级内按 `Timestamp` 排序，这意味着它会排在这一小时内新建的同优先级 Pod 前面。对队列系统来说这通常是期望的行为：先提交的作业先调度。

## 五、API Server 一侧：只能删，不能加

前面几节都在调度器内部：gate 在哪、事件怎么来。`schedulingGates` 的语义能成立，还靠 API Server 一侧的校验，而不是调度器。

### 5.1 只能在创建时设置

gate 只能删不能加，这条规则由更新时的校验保证：

```go {linenos=inline}
// pkg/apis/core/validation/validation.go
func validateOnlyDeletedSchedulingGates(newGates, oldGates []core.PodSchedulingGate, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(newGates) == 0 {
		return allErrs
	}

	additionalGates := make(map[string]int)
	for i, newGate := range newGates {
		additionalGates[newGate.Name] = i
	}

	for _, oldGate := range oldGates {
		delete(additionalGates, oldGate.Name)
	}

	for gate, i := range additionalGates {
		allErrs = append(allErrs, field.Forbidden(fldPath.Index(i).Child("name"), fmt.Sprintf("only deletion is allowed, but found new scheduling gate '%s'", gate)))
	}

	return allErrs
}
```

更新时出现旧列表里没有的名字就拒绝（第 8–18 行）。**这保证了 gate 是单向的：Pod 一旦开始调度，就不可能被重新 gate 回去**，调度器不需要处理「调度到一半被 gate」的情况。创建时还有两条约束：gate 名必须是合法的 qualified name 且不能重复（`validateSchedulingGates`）；`spec.nodeName` 和 `schedulingGates` 不能同时设置：

```go {linenos=inline}
// pkg/apis/core/validation/validation.go
	// A Pod cannot be assigned a Node if there are remaining scheduling gates.
	if pod.Spec.NodeName != "" && len(pod.Spec.SchedulingGates) != 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("nodeName"), "cannot be set until all schedulingGates have been cleared"))
	}
```

绕过调度器的路径也被堵上了。`pods/binding` 子资源在写入节点名之前检查 gate：

```go {linenos=inline}
// pkg/registry/core/pod/storage/storage.go
		// Reject binding to a scheduling un-ready Pod.
		if len(pod.Spec.SchedulingGates) != 0 {
			return nil, fmt.Errorf("pod %v has non-empty .spec.schedulingGates", pod.Name)
		}
```

所以无论是默认调度器、第二个调度器，还是自己写的绑定脚本，只要 gate 没删光，Pod 都不可能被绑定到节点上。

### 5.2 gated 期间放宽的字段

Pod 的 spec 大部分字段创建后不可变。`ValidatePodUpdate` 的白名单里有两项只在 gated 期间生效：

```go {linenos=inline}
// pkg/apis/core/validation/validation.go
var updatablePodSpecFields = []string{
	// ...
	"`spec.schedulingGates` (only deletions of existing scheduling gates)",
	"`spec.nodeSelector` (only additions, and only while the pod has scheduling gates)",
	"`spec.affinity.nodeAffinity` (only while the pod has scheduling gates)",
}
```

判断条件是 `podIsGated := len(oldPod.Spec.SchedulingGates) > 0`，看的是旧对象，所以可以在同一次更新里补上 `nodeSelector` 并删掉最后一个 gate。

**规则是「只能收紧」**，具体有两条：

- `nodeSelector` 只能新增键；
- `requiredDuringSchedulingIgnoredDuringExecution` 原来为空时可以随便设，原来非空时不能增删 term，每个 term 里只能追加 requirement（`validateNodeAffinityMutation`）。

只能收紧的原因是，外部控制器可能基于原来的约束做过决策（比如按可选节点预留了配额），放宽约束会让那些决策失效。

`podAffinity`/`podAntiAffinity` 不在白名单里。源码注释给出的原因是 `matchLabelKeys`/`mismatchLabelKeys` 会在创建时被展开进 `labelSelector`，允许修改就要处理展开结果和后续人为修改之间的冲突。

### 5.3 SchedulingGated 状态是谁写的

**`kubectl get pod` 显示的 `SchedulingGated` 不是调度器写的。** 调度器从未处理过这个 Pod，自然不会更新它的状态。条件是 API Server 在创建时加上的（第 3–5 行判断 gate 是否非空，第 7–12 行写入条件）：

```go {linenos=inline}
// pkg/registry/core/pod/strategy.go
func applySchedulingGatedCondition(pod *api.Pod) {
	if len(pod.Spec.SchedulingGates) == 0 {
		return
	}
	// ...
	podutil.UpdatePodCondition(&pod.Status, &api.PodCondition{
		Type:    api.PodScheduled,
		Status:  api.ConditionFalse,
		Reason:  apiv1.PodReasonSchedulingGated,
		Message: "Scheduling is blocked due to non-empty scheduling gates",
	})
}
```

kubectl 的打印逻辑看到 `PodScheduled` 条件的 reason 是 `SchedulingGated`，就把 STATUS 列显示成它。gate 删光之后，这个条件要等调度器第一次处理该 Pod 时才会被覆盖（调度失败写成 `Unschedulable`，成功则绑定）。

字段语义、校验规则和状态展示到这里都交代完了。接下来是几个不按正路走的组合。

## 六、边界情况

**gate 删光了，Pod 却仍然没进 activeQ。** 先看 `scheduler_pending_pods{queue="gated"}` 有没有下降。如果没有，说明还有别的 PreEnqueue 插件在拦它，比如 Pod 引用的 ResourceClaim 还没创建，`DynamicResources` 的 PreEnqueue 会拦住。这时 `GatingPlugin` 已经从 `SchedulingGates` 变成了 `DynamicResources`，Pod 等的是 ResourceClaim 的事件，和 gate 无关了。调度器在 V4 日志里会打印 `Status after running PreEnqueue plugin` 和插件名。

**Pod 指定了不存在的 `schedulerName`。** `addPod` 先用 `responsibleForPod` 判断这个 Pod 是不是自己的，不是就不入队，PreEnqueue 也就不会跑。这种 Pod 不会出现在 `pending_pods` 任何一个 queue 标签下。

**PodGroup 成员被 gate。** `runPreEnqueuePlugins` 对 PodGroup entity 逐个检查成员 Pod，遇到第一个被 gate 的就把整个组标记为 gated（第 2–9 行）：

```go {linenos=inline}
// pkg/scheduler/backend/queue/scheduling_queue.go
	for pInfo := range entity.ForEachPodInfo() {
		p.runPreEnqueuePluginsForPod(ctx, pInfo)
		if pInfo.Gated() {
			// If any pod is gated, the whole entity is gated.
			// ...
			entity.SetGatingPlugin(pInfo.QueueingParams.GatingPlugin, pInfo.QueueingParams.GatingPluginEvents)
			return
		}
	}
```

一个成员没 ungate，整组都不会进入调度周期。这和成组调度「要么一起上要么都不上」的语义一致，第 21 篇会细讲。

**gate 名字没有任何语义。** 调度器只看列表长度，不解析名字。约定俗成用带域名前缀的名字（如 `example.com/quota-check`），是为了让不同控制器各管各的、不误删别人的 gate。

这些边界情况的共同点是：gate 的语义完全落在队列这一侧，调度周期里没有任何一处代码认识它。下一节对比的正是这两种实现方式。

## 七、发散：为什么做成 PreEnqueue，而不是 Filter

一个外部控制器想「暂时不让 Pod 调度」，最直观的实现是写一个 Filter 插件，条件不满足就对所有节点返回失败。对比两者的成本：

| | Filter 实现 | PreEnqueue 实现 |
| --- | --- | --- |
| Pod 被唤醒时 | 完整跑一遍 PreFilter，对候选节点并行跑 Filter，再跑 PostFilter 尝试抢占 | 一次切片长度检查 |
| 占用调度周期 | 是，调度周期是串行的，会挡住后面的 Pod | 否 |
| 退避 | 每失败一次翻倍，ungate 后可能还要等最长 10 秒 | 从未失败，ungate 后立即入队 |
| 受哪些事件唤醒 | 插件关心的所有事件 | 只有 gate 删光 |

**调度器的吞吐上限由串行的调度周期决定。** 大批量提交作业的场景下，几千个暂时不该调度的 Pod 如果每隔几秒就来占一次调度周期，会明显拖慢真正可以调度的 Pod。PreEnqueue 把这部分成本降到了接近零。

另一个对比对象是 `schedulerName`。把 Pod 的 `schedulerName` 设成一个不存在的调度器，同样能让默认调度器不处理它。但那是一个创建后不可修改的字段，没法「放行」，只能删掉重建；而且这样的 Pod 不会被任何调度器的指标统计到。**gate 是可撤销的，同时让 Pod 在调度器的队列里有一个可观测的位置。**

从机制上看，`SchedulingGates` 也是 PreEnqueue 扩展点的第一个使用者。这个扩展点后来被 `DynamicResources`、异步抢占、成组调度复用，做的都是同一件事：在条件明显不满足时，别让 Pod 去占用串行的调度周期。

## 八、小结

- `SchedulingGates` 只实现 PreEnqueue：`spec.schedulingGates` 非空就返回 `UnschedulableAndUnresolvable`，Pod 留在 `unschedulableEntities`，被标记为 gated，不进 `activeQ`，也不进 `backoffQ`（开启 `SchedulerPopFromBackoffQ` 时）。
- PreEnqueue 在 `moveToActiveQ` 和 `moveToBackoffQ` 里调用，多个 PreEnqueue 插件之间没有固定顺序，上次拦住 Pod 的插件先跑。
- gated 的 Pod 只响应 `UpdatePodSchedulingGatesEliminated`（gate 从有到全无）和通配事件，其他集群事件直接跳过；ungate 后没有退避，立即进入 `activeQ`，并保留原来的入队时间戳。
- API Server 保证 gate 只减不增，`nodeName` 与 gate 不能共存，binding 子资源拒绝 gated 的 Pod；gated 期间允许收紧 `nodeSelector` 和节点亲和。

Pod 进入 `activeQ` 之后，谁先出队由唯一的 QueueSort 插件决定。[下一篇](/posts/sched-plugin-02-priority-sort/)读 `PrioritySort`。
