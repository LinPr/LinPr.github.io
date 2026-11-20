---
title: 'scheduler-plugins QOSSort：优先级打平时让 Guaranteed 先出队'
date: 2026-11-19T12:10:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "queuesort", "qos"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "QOSSort 是 out-of-tree 部分的第一个插件，在 PrioritySort 的「优先级、入队时间」两级比较之间插入 QoS 等级。本文先交代 scheduler-plugins 的插件如何编译进另一个调度器、如何由 Profile 启用，以及为什么一个进程只能有一个 QueueSort，再拆 Less 与 compQOS 的实现和它与 PrioritySort 的差异。"
---

[上一篇](/posts/sched-plugin-24-default-binder/)的 DefaultBinder 把 `Binding` 写进 API Server，一个 Pod 在默认 Profile 里的旅程到此走完。从这一篇开始，系列转向 [kubernetes-sigs/scheduler-plugins](https://github.com/kubernetes-sigs/scheduler-plugins)：这些插件不随 Kubernetes 发布，要自己编译、部署、配置才会生效。本文基于 scheduler-plugins 提交 `cfeb3aa`，它依赖的 Kubernetes 版本是 v1.36.4，框架代码的引用以 v1.36.4 为准；和 Kubernetes master（提交 `aabdf5a2`）有差异的地方会单独指出。

QOSSort 是 out-of-tree 插件里最小的一个：只实现 QueueSort 扩展点，三十行代码，README 里把成熟度标成 Sample（示范用途）。它适合作为第一篇，因为在读它之前，需要先把「out-of-tree 插件到底跑在哪里」讲清楚，而这件事对后面每一篇都适用。

## 一、out-of-tree 插件怎么跑起来

### 1.1 编译出另一个调度器二进制

scheduler-plugins 不提供动态加载机制，插件是在编译期链接进二进制的。仓库的入口 `cmd/scheduler/main.go` 直接复用了 kube-scheduler 的命令行：

```go
// cmd/scheduler/main.go
func main() {
	// Register custom plugins to the scheduler framework.
	// Later they can consist of scheduler profile(s) and hence
	// used by various kinds of workloads.
	command := app.NewSchedulerCommand(
		app.WithPlugin(capacityscheduling.Name, capacityscheduling.New),
		app.WithPlugin(coscheduling.Name, coscheduling.New),
		// ...
		app.WithPlugin(networkoverhead.Name, networkoverhead.New),
		app.WithPlugin(topologicalsort.Name, topologicalsort.New),
		// ...
		// Sample plugins below.
		// app.WithPlugin(crossnodepreemption.Name, crossnodepreemption.New),
		app.WithPlugin(podstate.Name, podstate.New),
		app.WithPlugin(qos.Name, qos.New),
	)

	code := cli.Run(command)
	os.Exit(code)
}
```

`app` 是 `k8s.io/kubernetes/cmd/kube-scheduler/app`，也就是官方 kube-scheduler 的同一个包。`WithPlugin` 只做一件事：把插件名和工厂函数登记进一张 Registry。

```go
// cmd/kube-scheduler/app/server.go（k8s v1.36.4）
// WithPlugin creates an Option based on plugin name and factory. Please don't remove this function: it is used to register out-of-tree plugins,
// hence there are no references to it from the kubernetes scheduler code base.
func WithPlugin(name string, factory runtime.PluginFactory) Option {
	return func(registry runtime.Registry) error {
		return registry.Register(name, factory)
	}
}
```

`Setup` 把这些 Option 依次作用在一张空的 `outOfTreeRegistry` 上，再通过 `scheduler.WithFrameworkOutOfTreeRegistry` 传给 `scheduler.New`，后者把它合并进 in-tree Registry：

```go
// pkg/scheduler/scheduler.go（k8s v1.36.4）
	registry := frameworkplugins.NewInTreeRegistry()
	if err := registry.Merge(options.frameworkOutOfTreeRegistry); err != nil {
		return nil, err
	}
```

`Merge` 内部逐个调用 `Register`，同名插件会返回 `a plugin named ... already exists` 错误，所以 out-of-tree 插件不能顶替 in-tree 插件的名字。合并的结果是：scheduler-plugins 编译出的二进制包含全部 in-tree 插件加上 15 个 out-of-tree 插件（`crossnodepreemption` 在 main.go 里被注释掉了），默认 Profile 不做任何配置也能照常工作。

这个二进制有两种部署方式（见仓库 `doc/install.md`）：作为第二个调度器与默认调度器并存，或者直接替换控制面上的 kube-scheduler。

### 1.2 Profile 启用，schedulerName 路由

插件编译进去不代表生效，还要在 `KubeSchedulerConfiguration` 的某个 Profile 里启用。QOSSort 的示例配置是这样的（仓库 `manifests/qos/scheduler-config.yaml` 仍写着 `v1beta3`，这个版本早已从 kube-scheduler 移除，scheduler-plugins 的 `apis/config` 下也只剩 `v1`，照抄时要改成 `v1`）：

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: default-scheduler
  plugins:
    queueSort:
      enabled:
      - name: QOSSort
      disabled:
      - name: "*"
```

每个 Profile 有一个 `schedulerName`，调度器只处理 `spec.schedulerName` 命中自己某个 Profile 的 Pod：

```go
// pkg/scheduler/eventhandlers.go（k8s v1.36.4）
func responsibleForPod(pod *v1.Pod, profiles profile.Map) bool {
	return profiles.HandlesSchedulerName(pod.Spec.SchedulerName)
}
```

出队之后，`frameworkForPod` 再按同一个字段挑出对应 Profile 的 framework 实例，后面的 PreFilter、Filter、Score、Bind 都在这个实例里跑。作为第二调度器部署时，Pod 必须显式写 `schedulerName: scheduler-plugins-scheduler`（helm chart 的默认名字），否则会被默认调度器接走。

### 1.3 一个进程只有一个排序函数

`schedulerName` 能把 Pod 分到不同的插件链，但分不开队列。整个调度器进程只有一个 `SchedulingQueue`，它的比较函数在构造时就定死了：

```go
// pkg/scheduler/scheduler.go（k8s v1.36.4）
	podQueue := internalqueue.NewSchedulingQueue(
		profiles[options.profiles[0].SchedulerName].QueueSortFunc(),
		informerFactory,
		// ...
	)
```

取的是第一个 Profile 的 `QueueSortFunc()`。为了让这个选择不产生歧义，配置校验要求所有 Profile 的 queueSort 完全一致，连插件参数也要一致：

```go
// pkg/scheduler/apis/config/validation/validation.go（k8s v1.36.4）
	for i := 1; i < len(profiles); i++ {
		var curr config.PluginSet
		if profiles[i].Plugins != nil {
			curr = profiles[i].Plugins.QueueSort
		}
		if !apiequality.Semantic.DeepEqual(canon, curr) {
			errs = append(errs, field.Invalid(path.Index(i).Child("plugins", "queueSort"), curr, "must be the same for all profiles"))
		}
		for _, cfg := range profiles[i].PluginConfig {
			if cfg.Name == queueSortName && !apiequality.Semantic.DeepEqual(queueSortArgs, cfg.Args) {
				errs = append(errs, field.Invalid(path.Index(i).Child("pluginConfig", "args"), cfg.Args, "queueSort must be the same for all profiles"))
			}
		}
	}
```

在单个 Profile 内部，`newFramework` 还会检查 QueueSort 插件恰好一个：

```go
// pkg/scheduler/framework/runtime/framework.go（k8s v1.36.4）
	if len(f.queueSortPlugins) != 1 {
		return nil, fmt.Errorf("only one queue sort plugin required for profile with scheduler name %q, but got %d", profile.SchedulerName, len(f.queueSortPlugins))
	}
```

于是 scheduler-plugins 里的三个 QueueSort 插件——本篇的 QOSSort、[下一篇](/posts/sched-plugin-26-topological-sort/)的 TopologicalSort、再下一篇的 [Coscheduling](/posts/sched-plugin-27-coscheduling/)——在同一个调度器进程里只能选一个，而且不是「每个 Profile 选一个」，是整个进程选一个。想同时用 QOSSort 和 Coscheduling，只能部署两个调度器进程。

{{< figure src="/images/posts/sched-plugins/25-out-of-tree-runtime.svg" caption="out-of-tree 插件的三个阶段：编译期注册工厂、启动时合并 Registry、配置期由 Profile 启用；排序队列全进程共享，所以 QueueSort 只能四选一（点击查看大图）" link="/images/posts/sched-plugins/25-out-of-tree-runtime.svg" >}}

配置里那行 `disabled: "*"` 不是可有可无的。v1 默认插件全部放在 `multiPoint` 里，其中包括 `PrioritySort`。`expandMultiPointPlugins` 展开每个扩展点时，如果该扩展点的 `disabled` 含 `"*"`，就跳过 multiPoint 展开：

```go
// pkg/scheduler/framework/runtime/framework.go（k8s v1.36.4）
		if disabledSet.Has("*") {
			logger.V(4).Info("Skipped MultiPoint expansion because all plugins are disabled for extension point", "extension", pluginType)
			continue
		}
```

不写这一行，`queueSort.enabled` 里的 QOSSort 和 multiPoint 展开出来的 PrioritySort 会同时进入 `queueSortPlugins`，启动时报上面那条 `but got 2`。另一种写法是 helm chart 采用的：在 `multiPoint.enabled` 里启用插件，同时在 `multiPoint.disabled` 里禁用 `PrioritySort`。

## 二、它解决什么问题

PrioritySort 的排序键是「优先级降序、入队时间升序」。在很多集群里，绝大多数业务 Pod 没有设置 PriorityClass，优先级都是 0，排序实际上退化成 FIFO。设想集群资源紧张时同时提交了下面两个 Pod：

```yaml
# batch-probe：BestEffort，不声明任何 requests/limits
containers:
- name: probe
  image: busybox
---
# payment-api：Guaranteed，requests == limits
containers:
- name: api
  image: payment:1.4
  resources:
    requests: {cpu: "2", memory: 4Gi}
    limits:   {cpu: "2", memory: 4Gi}
```

如果 `batch-probe` 先入队，PrioritySort 会先调度它。QOSSort 的想法是：优先级相同时，先调度 QoS 等级高的 Pod，也就是声明了确定资源量、对运行质量要求更高的那一类。它不过滤节点、不打分，只改变 activeQ 里的出队顺序。

## 三、源码

### 3.1 结构体与工厂

```go
// pkg/qos/queue_sort.go
// Name is the name of the plugin used in the plugin registry and configurations.
const Name = "QOSSort"

// Sort is a plugin that implements QoS class based sorting.
type Sort struct{}

var _ fwk.QueueSortPlugin = &Sort{}

// New initializes a new plugin and returns it.
func New(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &Sort{}, nil
}
```

空结构体，不读参数、不持有 `Handle`，也没有对应的 Args 类型和默认值。QueueSort 插件运行在队列里，此时还没有调度周期，因此也谈不上 CycleState。

### 3.2 Less：三级比较

```go
// pkg/qos/queue_sort.go
// Less is the function used by the activeQ heap algorithm to sort pods.
// It sorts pods based on their priorities. When the priorities are equal, it uses
// the Pod QoS classes to break the tie. If both the priority and QoS class are equal,
// it uses PodQueueInfo.timestamp to determine the order.
func (*Sort) Less(pInfo1, pInfo2 fwk.QueuedPodInfo) bool {
	p1 := corev1helpers.PodPriority(pInfo1.GetPodInfo().GetPod())
	p2 := corev1helpers.PodPriority(pInfo2.GetPodInfo().GetPod())

	if p1 != p2 {
		return p1 > p2
	}
	qosResult := compQOS(pInfo1.GetPodInfo().GetPod(), pInfo2.GetPodInfo().GetPod())
	if qosResult != 0 {
		return qosResult > 0
	}
	return pInfo1.GetTimestamp().Before(pInfo2.GetTimestamp())
}
```

`Less(a, b)` 返回 true 表示 a 应排在 b 前面。三级依次是：

1. `PodPriority` 读 `spec.priority`，为 nil 时返回 0。这个字段由 Priority 准入插件根据 `priorityClassName` 填好，调度器不再解析 PriorityClass。
2. 优先级相同，比较 QoS 等级。
3. QoS 也相同，比较 `Timestamp`。这个时间戳不是 Pod 的创建时间，而是进入队列的时间：`AddUnschedulableIfNotPresent` 在 Pod 调度失败、重新放回队列时会执行 `pInfo.Timestamp = p.clock.Now()`。所以一个失败过的 Pod，重新回到 activeQ 后会排到同优先级、同 QoS 的新来者后面。

这三级构成一个字典序比较，键是 `(-priority, -qosRank, timestamp)`，满足严格弱序：`Less(a, a)` 为 false，传递性成立。这一点看似平常，[下一篇](/posts/sched-plugin-26-topological-sort/)的 TopologicalSort 就没有做到。

### 3.3 compQOS：QoS 等级从哪里来

```go
// pkg/qos/queue_sort.go
func compQOS(p1, p2 *v1.Pod) int {
	p1QOS, p2QOS := v1qos.GetPodQOS(p1), v1qos.GetPodQOS(p2)

	// Define the precedence order of QoS classes using a map
	qosOrder := map[v1.PodQOSClass]int{
		v1.PodQOSBestEffort: 1,
		v1.PodQOSBurstable:  2,
		v1.PodQOSGuaranteed: 3,
	}

	if qosOrder[p1QOS] > qosOrder[p2QOS] {
		return 1
	} else if qosOrder[p1QOS] < qosOrder[p2QOS] {
		return -1
	}

	return 0
}
```

`GetPodQOS` 来自 `k8s.io/kubernetes/pkg/apis/core/v1/helper/qos`，它优先读 `status.qosClass`：

```go
// pkg/apis/core/v1/helper/qos/qos.go（k8s v1.36.4）
func GetPodQOS(pod *v1.Pod) v1.PodQOSClass {
	if pod.Status.QOSClass != "" {
		return pod.Status.QOSClass
	}
	return ComputePodQOS(pod)
}
```

`status.qosClass` 由 API Server 在创建 Pod 时写入（`pkg/registry/core/pod/strategy.go` 的 `PrepareForCreate` 里 `pod.Status.QOSClass = qos.GetPodQOS(pod)`），之后不会变化：原地扩缩容的校验明确拒绝改变 QoS 的 resize（`Pod QOS Class may not change as a result of resizing`）。所以队列里的 Pod 几乎都走第一个分支，比较成本只是两次字段读取。只有绕过 API Server 构造的 Pod（比如单元测试）才会走 `ComputePodQOS`。

`ComputePodQOS` 的规则只看 cpu 和 memory 两种资源：所有容器（含 init 容器）都没有声明 cpu/memory 的 requests 和 limits 是 BestEffort；每个容器的 cpu 和 memory 都设置了 limits 且汇总后 requests 等于 limits 是 Guaranteed；其余是 Burstable。开启 `PodLevelResources` 且 Pod 声明了 `spec.resources` 时，改用 Pod 级资源判断。GPU 之类的扩展资源不影响 QoS，一个只申请了 `nvidia.com/gpu` 的 Pod 仍然是 BestEffort。

`qosOrder` 每次比较都新建一个 map，严格说是一次小的堆分配。activeQ 每次 Push/Pop 做 O(log n) 次比较，这点开销在调度周期面前可以忽略，但它说明这份代码定位是示范，没有为热路径优化。

{{< figure src="/images/posts/sched-plugins/25-qos-less.svg" caption="QOSSort.Less 的三级判定，以及同一批 Pod 在 PrioritySort 与 QOSSort 下的出队顺序：同为优先级 100 时 e（Guaranteed）超过先入队的 a（BestEffort）；优先级 0 的四个 Pod 按 Guaranteed、Burstable、BestEffort 分层，层内仍按入队时间（点击查看大图）" link="/images/posts/sched-plugins/25-qos-less.svg" >}}

### 3.4 没有的东西

QOSSort 不实现任何其它扩展点，也不需要 `EventsToRegister`：框架只为 PreEnqueue、PreFilter、Filter、Reserve、Permit 插件收集 EnqueueExtensions（`shouldHaveEnqueueExtensions`），QueueSort 插件不会拒绝 Pod，自然没有「什么事件让它重新入队」的问题。它影响的只是 Pod 在 activeQ 里的位置。

## 四、与 PrioritySort 对照

v1.36.4 的 PrioritySort：

```go
// pkg/scheduler/framework/plugins/queuesort/priority_sort.go（k8s v1.36.4）
func (pl *PrioritySort) Less(pInfo1, pInfo2 fwk.QueuedPodInfo) bool {
	p1 := corev1helpers.PodPriority(pInfo1.GetPodInfo().GetPod())
	p2 := corev1helpers.PodPriority(pInfo2.GetPodInfo().GetPod())
	return (p1 > p2) || (p1 == p2 && pInfo1.GetTimestamp().Before(pInfo2.GetTimestamp()))
}
```

两者的差别只有中间那一层 QoS，优先级仍然是第一排序键。QOSSort 不会让一个优先级 0 的 Guaranteed Pod 超过优先级 1 的 BestEffort Pod。

这个比较函数不只用于 activeQ。v1.36.4 的 backoffQ 在 `SchedulerPopFromBackoffQ`（1.33 起 Beta，默认开启）打开时，用 `lessBackoffCompletedWithPriority` 排序：先比退避到期时间，到期时间落在同一个对齐窗口时，再调用 activeQ 的 Less：

```go
// pkg/scheduler/backend/queue/backoff_queue.go（k8s v1.36.4）
func (bq *backoffQueue) lessBackoffCompletedWithPriority(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	bo1 := bq.getBackoffTime(pInfo1)
	bo2 := bq.getBackoffTime(pInfo2)
	if !bo1.Equal(bo2) {
		return bo1.Before(bo2)
	}
	// If the backoff time is the same, sort the pod in the same manner as activeQ does.
	return bq.activeQLessFn(pInfo1, pInfo2)
}
```

所以换成 QOSSort 后，activeQ 为空、调度器直接从 backoffQ 取 Pod 时，同一窗口内的 Pod 也会按 QoS 分层。

到了 Kubernetes master，QueueSort 接口本身变了。`aabdf5a2` 中的定义是：

```go
// staging/src/k8s.io/kube-scheduler/framework/interface.go（k8s master aabdf5a2）
type QueueSortPlugin interface {
	Plugin
	// Less are used to sort entities in the scheduling queue.
	Less(QueuedEntityInfo, QueuedEntityInfo) bool
}
```

`QueuedEntityInfo` 可以是单个 Pod，也可以是 PodGroup（`EntityKeyType` 取值 `pod`、`podgroup`、`compositepodgroup`），PrioritySort 相应地改成了 `entity1.GetPriority()`。QOSSort 现在拿到的是 `QueuedPodInfo`，升级到这套接口后需要先按实体类型分流：PodGroup 没有天然的 QoS，要么取成员的某种聚合，要么跳过 QoS 这一层。这是 scheduler-plugins 跟进新版本时 QueueSort 类插件都要面对的改动。

## 五、边界与坑

**QoS 只影响出队顺序，不影响抢占。** 一个优先级 0 的 Guaranteed Pod 调度失败后，[DefaultPreemption](/posts/sched-plugin-18-default-preemption/) 只会考虑优先级更低的 Pod 作为受害者，同优先级的 BestEffort Pod 不会被它驱逐。想让关键负载能抢占，还是要用 PriorityClass。

**先出队不等于先占到资源。** 排序只决定谁先进入调度周期。如果排在前面的 Guaranteed Pod 找不到节点，它会进入 unschedulablePods，后面的 BestEffort Pod 继续调度，队列不会被它堵住。QOSSort 在资源紧张、多个 Pod 同时竞争同一批空闲资源时才有意义；集群空闲时，每个 Pod 出队就能落位，顺序差异几乎看不出来。

**BestEffort 排在最后，代价通常不大。** BestEffort Pod 的 requests 为 0，NodeResourcesFit 的过滤基本拦不住它（除非节点 Pod 数达到上限），晚一点出队一般不会让它调度失败。反过来，Guaranteed Pod 的 requests 往往较大，越早出队越容易拿到完整的空闲资源。

**和 TopologicalSort、Coscheduling 不能共存。** 需要 Gang 语义又希望按 QoS 排序，只能部署两个调度器进程，或者在 Coscheduling 的排序规则之外另想办法；配置里同时启用两个 QueueSort 会直接启动失败。

## 六、发散：QoS 在调度之后还意味着什么

调度器里 QoS 只用来排序，节点上它的作用更具体。kubelet 根据 QoS 设置容器的 `oom_score_adj`：

```go
// pkg/kubelet/qos/policy.go（k8s master aabdf5a2）
const (
	// ...
	guaranteedOOMScoreAdj int = -997
	besteffortOOMScoreAdj int = 1000
)

func GetContainerOOMScoreAdjust(pod *v1.Pod, container *v1.Container, memoryCapacity int64) int {
	// ...
	switch v1qos.GetPodQOS(pod) {
	case v1.PodQOSGuaranteed:
		// Guaranteed containers should be the last to get killed.
		return guaranteedOOMScoreAdj
	case v1.PodQOSBestEffort:
		return besteffortOOMScoreAdj
	}
	// ...
	oomScoreAdjust = 1000 - (1000*containerMemReq)/memoryCapacity
	// ...
```

Guaranteed 是 -997，BestEffort 是 1000，Burstable 按内存 request 占节点容量的比例落在两者之间（并被限制在 3 到 999）。节点内存耗尽、内核 OOM killer 介入时，BestEffort 容器最先被杀。

但 kubelet 自己的驱逐（node-pressure eviction）并不直接按 QoS 排序。内存压力下的排序是：

```go
// pkg/kubelet/eviction/helpers.go（k8s master aabdf5a2）
func rankMemoryPressure(pods []*v1.Pod, stats statsFunc) {
	orderedBy(exceedMemoryRequests(stats), priority, memory(stats)).Sort(pods)
}
```

先看用量是否超过 requests，再看优先级，最后看超出 requests 的量。BestEffort 的 requests 为 0，只要有内存用量就「超过 requests」，所以实际效果和按 QoS 驱逐相近，但依据是 requests 而不是 QoS 标签。

把这两件事和 QOSSort 放在一起看：QoS 在 Kubernetes 里是一个由 requests/limits 推导出来的派生属性，节点侧用它（或它的来源 requests）决定谁先牺牲；QOSSort 把同样的信号用到了调度入口，让「要求确定资源」的 Pod 先拿到资源。它与 PriorityClass 不冲突：PriorityClass 是显式声明的业务重要性，QoS 是资源声明方式的副产品。优先级体系划分得足够细的集群，QOSSort 几乎没有发挥空间；大多数 Pod 都是优先级 0 的集群，它是一个不需要改任何工作负载就能生效的调整。

## 七、小结

- scheduler-plugins 通过 `app.WithPlugin` 把插件编译进一个基于 kube-scheduler 的新二进制，in-tree 插件全部保留；插件由 Profile 启用，Pod 通过 `spec.schedulerName` 选择 Profile。
- 一个调度器进程只有一个调度队列，比较函数取自第一个 Profile，所有 Profile 的 queueSort 必须相同，因此 QOSSort、TopologicalSort、Coscheduling 三者互斥。
- QOSSort 的排序键是 `(优先级降序, QoS 等级降序, 入队时间升序)`，QoS 来自 API Server 写入的 `status.qosClass`；它满足严格弱序，也通过 backoffQ 的同窗口排序生效。
- 它只改变出队顺序，不影响过滤、打分和抢占。

QOSSort 在 PrioritySort 之上加了一层通用信号。[下一篇](/posts/sched-plugin-26-topological-sort/)的 TopologicalSort 也是 QueueSort 插件，它依据的是应用自身的依赖拓扑：同一个应用内，按 AppGroup controller 算好的拓扑顺序出队。
