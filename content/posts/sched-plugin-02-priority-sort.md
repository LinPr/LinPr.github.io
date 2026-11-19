---
title: '调度插件逐个读（02）：PrioritySort，一行比较函数决定谁先出队'
date: 2026-11-19T08:20:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "PrioritySort", "QueueSort", "PriorityClass"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "PrioritySort 是默认的 QueueSort 插件，只有一个 Less 方法：优先级高的先出队，相同优先级按入队时间先后。本文拆开 Less 的两个输入从哪里来：priority 由 Priority 准入插件根据 PriorityClass 在创建时写死，Timestamp 是入队时间而且会在失败后刷新；再看 Less 如何驱动 activeQ 和 backoffQ 两个堆、为什么所有 profile 必须共用一个 QueueSort，以及 preemptionPolicy: Never 的「插队不抢占」语义。"
---

[上一篇](/posts/sched-plugin-01-scheduling-gates/)讲的是 Pod 能不能进入 `activeQ`。进来之后，`activeQ` 里可能同时排着成百上千个 Pod，调度周期是串行的，一次只取一个。取哪一个，由 QueueSort 扩展点决定。默认 Profile 里这个位置只有一个插件：`PrioritySort`。

它在 `default_plugins.go` 的 `MultiPoint` 列表里排第二，默认启用，不受 feature gate 控制，也没有参数。和其他插件不同，它在 `registry.go` 里直接注册 `queuesort.New`，没有经过 `runtime.FactoryAdapter` 包装，因为它不需要读取任何 feature gate。本文基于 Kubernetes master 提交 `aabdf5a2`。

## 一、插件本身

```go
// pkg/scheduler/framework/plugins/queuesort/priority_sort.go
// PrioritySort is a plugin that implements Priority based sorting.
type PrioritySort struct{}

var _ fwk.QueueSortPlugin = &PrioritySort{}

// Less is the function used by the activeQ heap algorithm to sort entities.
// It sorts entities based on their priority. When priorities are equal, it uses
// the entity timestamp.
func (pl *PrioritySort) Less(entity1, entity2 fwk.QueuedEntityInfo) bool {
	p1 := entity1.GetPriority()
	p2 := entity2.GetPriority()
	return (p1 > p2) || (p1 == p2 && entity1.GetTimestamp().Before(entity2.GetTimestamp()))
}

// New initializes a new plugin and returns it.
func New(_ context.Context, _ runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	return &PrioritySort{}, nil
}
```

整个插件就是一个严格弱序：`Less(a, b)` 为真表示 a 应该排在 b 前面。先比优先级，数值大的在前；优先级相等再比时间戳，早的在前。两者都相等时 `Less(a,b)` 和 `Less(b,a)` 都为假，堆不保证它们的先后。

`QueueSortPlugin` 接口只要求这一个方法：

```go
// staging/src/k8s.io/kube-scheduler/framework/interface.go
// LessFunc is the function to sort entity info
type LessFunc func(entity1, entity2 QueuedEntityInfo) bool

// QueueSortPlugin is an interface that must be implemented by "QueueSort" plugins.
// These plugins are used to sort entities in the scheduling queue. Only one queue sort
// plugin may be enabled at a time.
type QueueSortPlugin interface {
	Plugin
	// Less are used to sort entities in the scheduling queue.
	Less(QueuedEntityInfo, QueuedEntityInfo) bool
}
```

参数类型是 `QueuedEntityInfo`，不是 `QueuedPodInfo`。开启 `GenericWorkload` 后，队列里排队的除了单个 Pod，还有整个 PodGroup（`QueuedPodGroupInfo`），两种实体放在同一个堆里比较。Less 只依赖接口上的 `GetPriority()` 和 `GetTimestamp()`，不关心实体是什么类型。

这个插件值得一读的地方不在 Less 本身，而在它的两个输入：priority 从哪里来，timestamp 从哪里来。

## 二、priority 从哪里来

### 2.1 Pod 的 priority 在创建时写死

单个 Pod 的 `GetPriority()` 读的是 `spec.priority`：

```go
// pkg/scheduler/framework/types.go
func (pqi *QueuedPodInfo) GetPriority() int32 {
	return corev1helpers.PodPriority(pqi.GetPod())
}
```

```go
// staging/src/k8s.io/component-helpers/scheduling/corev1/helpers.go
func PodPriority(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	// When priority of a running pod is nil, it means it was created at a time
	// that there was no global default priority class and the priority class
	// name of the pod was empty. So, we resolve to the static default priority.
	return 0
}
```

`spec.priority` 不是创建者填的，而是 API Server 的 `Priority` 准入插件在创建 Pod 时根据 `priorityClassName` 解析出来的：

```go
// plugin/pkg/admission/priority/admission.go
	if operation == admission.Create {
		var priority int32
		var preemptionPolicy *apiv1.PreemptionPolicy
		pcName, priority, preemptionPolicy, err := p.establishPriority(a, &pod.Spec.PriorityClassName)
		if err != nil {
			return err
		}
		pod.Spec.PriorityClassName = pcName
		// if the pod contained a priority that differs from the one computed from the priority class, error
		if pod.Spec.Priority != nil && *pod.Spec.Priority != priority {
			return admission.NewForbidden(a, fmt.Errorf("the integer value of priority (%d) must not be provided in pod spec; priority admission controller computed %d from the given PriorityClass name", *pod.Spec.Priority, priority))
		}
		pod.Spec.Priority = &priority
		// ...
	}
```

解析规则在 `establishPriority` 和 `getDefaultPriority` 里：

- 写了 `priorityClassName`：查这个 PriorityClass，不存在就拒绝创建（`no PriorityClass with name ... was found`）；
- 没写：找 `globalDefault: true` 的 PriorityClass。如果因为竞争出现了多个，取 `value` 最小的那个；
- 一个 globalDefault 都没有：priority 为 0，`preemptionPolicy` 为 `PreemptLowerPriority`。

Pod 更新时准入插件只是把新对象里缺失的 `priority` 和 `preemptionPolicy` 从旧对象补回来。`spec.priority` 和 `spec.priorityClassName` 都不在 `ValidatePodUpdate` 的可变字段白名单里，所以 **Pod 的优先级在整个生命周期内不变**。PriorityClass 本身的 `value` 和 `preemptionPolicy` 也不可修改（`ValidatePriorityClassUpdate`）。修改 PriorityClass 的唯一办法是删了重建，而这只影响之后创建的 Pod，已有 Pod 的 `spec.priority` 是创建时拷贝的整数，不会跟着变。

这个不可变性对队列很重要：堆里元素的排序键如果会变，每次变化都要重新调整堆。priority 不变，`Update` 时 `heap.AddOrUpdate` 里的 `heap.Fix` 基本上不会移动元素。

### 2.2 优先级的取值范围

```go
// pkg/apis/scheduling/types.go
	// HighestUserDefinablePriority is the highest priority for user defined priority classes. Priority values larger than 1 billion are reserved for Kubernetes system use.
	HighestUserDefinablePriority = int32(1000000000)
	// SystemCriticalPriority is the beginning of the range of priority values for critical system components.
	SystemCriticalPriority = 2 * HighestUserDefinablePriority
```

自定义的 PriorityClass 最大是 10 亿。名字以 `system-` 开头的被保留，只能是两个内置类：`system-cluster-critical`（20 亿）和 `system-node-critical`（20 亿 + 1000）。所以无论怎么配置，CoreDNS、kube-proxy 这类用内置类的 Pod 总是排在所有业务 Pod 前面。

一个常见的配置：

```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: batch-high
value: 1000
globalDefault: false
preemptionPolicy: PreemptLowerPriority   # 不写也会被默认为这个值
description: "批处理高优先级作业"
```

### 2.3 PodGroup 的 priority

`QueuedPodGroupInfo` 的优先级来自 PodGroup 对象自己的 `spec.priority`，同样由 `Priority` 准入插件在创建 PodGroup 时根据它的 `priorityClassName` 解析（`admitPodGroup`）。组内各 Pod 的 priority 不参与组的排序。这意味着一个 PodGroup 的排队位置由组的 PriorityClass 决定，而不是组里优先级最高的那个 Pod。

## 三、timestamp 从哪里来

`GetTimestamp()` 返回 `QueueingParams.Timestamp`，字段注释写的是「The time entity added to the scheduling queue」，入队时间，不是 Pod 的 `creationTimestamp`。它在两个地方被赋值。

新 Pod 第一次进入队列：

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) newQueuedPodInfo(ctx context.Context, pod *v1.Pod, plugins ...string) *framework.QueuedPodInfo {
	now := p.clock.Now()
	// ...
	return &framework.QueuedPodInfo{
		PodInfo: podInfo,
		QueueingParams: framework.QueueingParams{
			Timestamp:               now,
			InitialAttemptTimestamp: nil,
			UnschedulablePlugins:    sets.New(plugins...),
		},
		PodSignature: p.signPod(ctx, pod),
	}
}
```

调度失败、回到队列：

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
func (p *PriorityQueue) AddUnschedulablePodIfNotPresent(ctx context.Context, pInfo *framework.QueuedPodInfo, podSchedulingCycle int64) error {
	// ...
	// Refresh the timestamp since the pod is re-added.
	pInfo.Timestamp = p.clock.Now()
	// We changed ConsecutiveErrorsCount or UnschedulableCount plus Timestamp, and now the calculated backoff time should be different,
	// removing the cached backoff time.
	pInfo.BackoffExpiration = time.Time{}
	// ...
}
```

除此之外，实体在 `unschedulableEntities`、`backoffQ`、`activeQ` 之间移动，Pod 对象被更新，周期性刷新（`flushUnschedulableEntitiesLeftover` 改的是另一个字段 `FlushTimestamp`），都不会改 `Timestamp`。

刷新的效果是：**同优先级内，失败过的 Pod 排到后面去**。下图是一个例子。

{{< figure src="/images/posts/sched-plugins/02-activeq-heap.svg" link="/images/posts/sched-plugins/02-activeq-heap.svg" caption="五个 Pod 在 activeQ 堆里的位置：先按 priority，再按 Timestamp；e 因为失败过一次，Timestamp 被刷新，排到了同优先级的 a 后面（点击查看大图）" >}}

`e` 原本 10:00:00 入队，比 `a` 早。它调度失败一次，回队时 `Timestamp` 被刷新为 10:00:06，等它退避结束重新进入 `activeQ`，已经排在 `a` 后面了。这是一种隐式的公平：一个反复失败的 Pod 不会因为「来得早」一直占着同优先级的队头。

`Timestamp` 还决定退避时长。`getBackoffTime` 用 `entity.GetTimestamp().Add(duration)` 计算退避到期时间，失败时刷新 `Timestamp`，相当于从这次失败的时刻开始计算退避。

上一篇提到的 gated Pod 是另一种情况：它从未失败，`Timestamp` 停在第一次入队的时刻，ungate 之后带着这个旧时间进入 `activeQ`，排在这期间新来的同优先级 Pod 前面。

## 四、Less 驱动的两个堆

### 4.1 activeQ

调度器启动时，用第一个 profile 的 `QueueSortFunc()` 创建队列：

```go
// pkg/scheduler/scheduler.go
	podQueue := internalqueue.NewSchedulingQueue(
		profiles[comps.options.profiles[0].SchedulerName].QueueSortFunc(),
		informerFactory,
		// ...
	)
```

```go
// pkg/scheduler/framework/runtime/framework.go
func (f *frameworkImpl) QueueSortFunc() fwk.LessFunc {
	// ...
	if len(f.queueSortPlugins) == 0 {
		panic("No QueueSort plugin is registered in the frameworkImpl.")
	}

	// Only one QueueSort plugin can be enabled.
	return f.queueSortPlugins[0].Less
}
```

`NewPriorityQueue` 把它包成堆的比较函数：

```go
// pkg/scheduler/backend/queue/scheduling_queue.go
	pq.activeQ = newActiveQueue(heap.NewWithRecorder(queuedEntityKeyFunc, heap.LessFunc[framework.QueuedEntityInfo](lessConverted), metrics.NewActiveEntitiesRecorder()), options.metricsRecorder, backoffQPopper)
```

堆的实现在 `pkg/scheduler/backend/heap`，是对 `container/heap` 的封装，额外维护了一个 `keyIndex map[string]int`，从实体 key 映射到数组下标，这样按 key 删除或更新时不用线性查找：

```go
// pkg/scheduler/backend/heap/heap.go
func (h *Heap[T]) AddOrUpdate(obj T) {
	key := h.data.keyFunc(obj)
	if idx, exists := h.data.keyIndex[key]; exists {
		// ...
		h.data.queue[idx].obj = obj
		heap.Fix(h.data, idx)
	} else {
		heap.Push(h.data, &heapItem[T]{obj: obj, key: key})
		// ...
	}
}
```

Push、Pop、Fix 都是 O(log n)，Less 在每次上浮或下沉时被调用 O(log n) 次。`PrioritySort.Less` 只做两次整数比较和一次时间比较，即使 `activeQ` 里有上万个 Pod，排序开销也可以忽略。自己写 QueueSort 插件时要记住这一点：Less 会被高频调用，而且是在 `activeQ` 的锁里调用，不能在里面做任何 I/O。

### 4.2 backoffQ

`backoffQ` 也是堆，主排序键是退避到期时间。开启 `SchedulerPopFromBackoffQ`（1.33 Beta，默认开启）后，它把同一个 Less 用作第二排序键：

```go
// pkg/scheduler/backend/queue/backoff_queue.go
// lessBackoffCompletedWithPriority is a less function of entityBackoffQ if PopFromBackoffQ feature is enabled.
// It orders the entities in the same BackoffOrderingWindow the same as the activeQ will do to improve popping order from backoffQ when activeQ is empty.
func (bq *backoffQueue) lessBackoffCompletedWithPriority(entity1, entity2 framework.QueuedEntityInfo) bool {
	bo1 := bq.getBackoffTime(entity1)
	bo2 := bq.getBackoffTime(entity2)
	if !bo1.Equal(bo2) {
		return bo1.Before(bo2)
	}
	// If the backoff time is the same, sort the entity in the same manner as activeQ does.
	return bq.activeQLessFn(entity1, entity2)
}
```

退避到期时间会被 `alignToWindow` 截断到 1 秒的窗口（`backoffQOrderingWindowDuration`），所以同一秒内到期的实体之间按 Less 排序。这个 feature 打开后，`activeQ` 为空时 `Pop` 会直接从 `backoffQ` 取，取出的顺序因此也基本符合优先级。只有 `entityErrorBackoffQ`（因 `Error` 状态退避的实体）仍然只按到期时间排序。

## 五、为什么只能有一个 QueueSort

框架在三个层面上保证只有一个 QueueSort：

1. 单个 profile 内，`NewFramework` 检查 `len(f.queueSortPlugins) != 1` 就报错：`only one queue sort plugin required for profile with scheduler name ...`；
2. 多个 profile 之间，配置校验 `validateCommonQueueSort` 要求所有 profile 的 QueueSort 插件和参数完全相同，否则报 `must be the same for all profiles`；
3. 运行时 `profile.NewMap` 里再检查一次，不同就报 `different queue sort plugins for profile`。

原因在 4.1 的那段代码里：整个调度器只有一个 `PriorityQueue`，所有 profile 的 Pod 都排在同一个 `activeQ` 里，而这个堆只能有一种比较函数。如果两个 profile 的排序规则不同，同一个堆里的元素就无法比较。

这也是第 25~27 篇的三个 out-of-tree QueueSort 插件（`QOSSort`、`TopologicalSort`、`Coscheduling`）互斥的原因，而且一旦启用，同一个调度器进程里所有 profile 都要换成同一个。

## 六、优先级在队列之外的作用

`PrioritySort` 只决定出队顺序。同一个 `spec.priority` 还在调度周期的其他地方被使用，理解了这些才知道「高优先级」到底意味着什么。

### 6.1 提名 Pod 的占位

被抢占提名了节点（`status.nominatedNodeName`）、但还没真正绑定的 Pod，会在 Filter 阶段被当作「已经在那个节点上」。不过只对优先级不低于当前 Pod 的提名 Pod 这样做：

```go
// pkg/scheduler/framework/runtime/framework.go
	for _, pi := range nominatedPodInfos {
		if corev1.PodPriority(pi.GetPod()) >= corev1.PodPriority(pod) && pi.GetPod().UID != pod.UID {
			nodeInfoOut.AddPodInfo(pi)
			status := fh.RunPreFilterExtensionAddPod(ctx, stateOut, pod, pi, nodeInfoOut)
			// ...
		}
	}
```

高优先级 Pod 抢占腾出的位置，低优先级 Pod 不能趁它还没绑定时占走；反过来，低优先级 Pod 的提名不会挡住高优先级 Pod。

### 6.2 抢占

`DefaultPreemption` 只会驱逐优先级严格低于抢占者的 Pod，第 18 篇会细讲。这里只说和 PriorityClass 直接相关的一个字段：`preemptionPolicy`。

```go
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
func (pl *DefaultPreemption) PodEligibleToPreemptOthers(_ context.Context, pod *v1.Pod, nominatedNodeStatus *fwk.Status) (bool, string) {
	if pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == v1.PreemptNever {
		return false, "not eligible due to preemptionPolicy=Never."
	}
	// ...
}
```

`preemptionPolicy` 同样由准入插件从 PriorityClass 拷贝到 Pod，Pod 上不能直接写一个与之不同的值。设为 `Never` 的 PriorityClass 就是常说的「非抢占优先级」：它的 Pod 在队列里照样按高优先级插队（`PrioritySort` 根本不看这个字段），但调度失败时不会驱逐别人，只能等。适合那些「希望尽快被调度、但不值得为它杀掉正在运行的任务」的作业，比如数据分析类的批处理。

### 6.3 kubelet 的节点压力驱逐

节点内存、磁盘、PID 紧张时，kubelet 选择驱逐对象也用 priority 作排序键之一：

```go
// pkg/kubelet/eviction/helpers.go
func rankMemoryPressure(pods []*v1.Pod, stats statsFunc) {
	orderedBy(exceedMemoryRequests(stats), priority, memory(stats)).Sort(pods)
}
```

内存压力下先看是否超出 request，再看 priority，再看实际用量。一个 PriorityClass 同时影响了「先调度谁」「可以赶走谁」「节点紧张时先赶走谁」三件事，设计 PriorityClass 体系时要一起考虑。

## 七、边界情况与常见误解

**高优先级不等于一定先调度成功。** `PrioritySort` 只保证高优先级的 Pod 先出队、先跑调度周期。它调度失败后离开 `activeQ`，进入 `backoffQ` 或 `unschedulableEntities`，后面的低优先级 Pod 照常出队。所以一个放不下的大 Pod 不会造成队头阻塞，代价是低优先级的小 Pod 可能先把碎片资源占了，大 Pod 要靠抢占才能挤进去。

**同优先级不是严格的 FIFO。** 刷新 `Timestamp` 的规则让失败过的 Pod 排到后面；调度器重启（或 leader 切换）后，队列由 informer 的全量 List 重建，所有 Pending Pod 的 `Timestamp` 都变成重建时刻，原来的先后关系就丢了。依赖「先创建先调度」的业务逻辑不可靠，需要严格顺序应当用 gate 或 `TopologicalSort` 之类的插件显式控制。

**改 PriorityClass 不影响已有 Pod。** 删掉重建一个同名、`value` 不同的 PriorityClass，已经存在的 Pod 仍保留旧的 `spec.priority`，Pending 的 Pod 也一样。要让它们按新优先级排队，只能重建 Pod。

**globalDefault 只影响之后创建的 Pod。** 集群运行一段时间后再创建 `globalDefault: true` 的 PriorityClass，已有 Pod 的 priority 仍然是 0。

**Less 的严格弱序。** 两个优先级相同、`Timestamp` 也完全相同的实体，Less 双向都为假，出队先后取决于堆的内部结构。测试里用假时钟时容易遇到这种「看起来随机」的顺序。

## 八、发散：排序之外还能怎么排

`PrioritySort` 的规则非常保守，只有两个维度。常见的扩展需求和对应的做法：

- **按 QoS 排序**：scheduler-plugins 的 `QOSSort` 在优先级相同时先比 QoS 等级，再比 `Timestamp`，见[第 25 篇](/posts/sched-plugin-25-qos-sort/)。注意 scheduler-plugins 依赖的是 v1.36，那里的 `Less` 签名还是 `Less(fwk.QueuedPodInfo, fwk.QueuedPodInfo)`，master 上已经改成了 `QueuedEntityInfo`。
- **按依赖关系排序**：`TopologicalSort` 让同一个应用内被依赖的 Pod 先出队（[第 26 篇](/posts/sched-plugin-26-topological-sort/)）。
- **成组出队**：`Coscheduling` 在优先级相同时比较的是 PodGroup 的创建时间而不是 Pod 的入队时间，于是同一组的 Pod 在队列里挨在一起（[第 27 篇](/posts/sched-plugin-27-coscheduling/)）。in-tree 的 `GenericWorkload` 走了另一条路，直接把 PodGroup 作为一个实体放进堆里，仍然由 `PrioritySort` 排序。
- **租户间公平**：`PrioritySort` 没有租户概念，一个租户提交大量同优先级作业会排在其他租户前面。这类需求通常在队列之外解决，比如 Kueue 用 `schedulingGates` 控制每个租户放进调度器的 Pod 数量，调度器内部仍然只看优先级。

从数据结构的角度看，QueueSort 插件能控制的只是一个全序关系，而且必须是便宜的、只依赖实体自身字段的比较。凡是需要全局视角的排序（比如「这个租户已经占用了多少资源」），都不适合放在 Less 里，因为堆不会在外部状态变化时重新排序。这也是为什么配额、公平共享这类功能更多地在 PreEnqueue 或者调度器外部实现。

## 九、小结

- `PrioritySort.Less`：priority 大的在前，相等时 `Timestamp` 早的在前；它是 `activeQ` 堆的比较函数，开启 `SchedulerPopFromBackoffQ` 后也是 `backoffQ` 在同一 1 秒窗口内的第二排序键。
- priority 由 `Priority` 准入插件在创建时根据 PriorityClass 写进 `spec.priority`，之后不可变；自定义类最大 10 亿，`system-cluster-critical`/`system-node-critical` 是 20 亿和 20 亿 + 1000。
- `Timestamp` 是入队时间，调度失败回队时刷新，所以同优先级内失败过的 Pod 会排到后面；gated 的 Pod 保留最初的入队时间。
- 整个调度器只有一个 `PriorityQueue`，所以所有 profile 必须使用同一个 QueueSort 插件和参数。
- `preemptionPolicy: Never` 不影响排队，只取消抢占资格。

出队之后，Pod 进入调度周期的第一站 PreFilter。[下一篇](/posts/sched-plugin-03-node-name/)读 Filter 链上排第一的 `NodeName`，看它如何用 `PreFilterResult` 把候选节点直接缩到一个。
