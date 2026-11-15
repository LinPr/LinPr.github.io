---
title: 'Kubernetes 编程（四）：控制器与 informer'
date: 2026-11-15T21:30:00+08:00
draft: false
tags: ["kubernetes", "client-go", "informer", "controller", "源码阅读"]
categories: ["Kubernetes"]
series: ["kubernetes-programming"]
author: "LinPr"
description: "控制器是 Kubernetes 的灵魂，informer 是控制器的基石。本文先用闭环控制理解控制器，理清 controller、informer、operator 三个概念的关系；再沿着 sample-controller 那张经典的图，结合 client-go 源码逐步拆解 Reflector、DeltaFIFO、Indexer、事件分发和 workqueue，最后给出一个最小控制器骨架。"
---

前两篇用 client-go 做的都是「主动」的事：发请求，拿结果。但 Kubernetes 里大部分组件都不是这么工作的。Deployment 控制器不会每秒去问一遍「有没有新的 Deployment」，kube-proxy 也不会轮询 EndpointSlice。它们靠的是**控制器模式**，而控制器的底层是 client-go 的 **informer**。

## 一、什么是控制器

网上关于控制器的说法大多是：控制器把用户在 YAML 里声明的期望状态，通过不断执行操作，让实际状态与之一致。这当然没错，我想换个角度理解它。

自动化专业有一门课叫《自动控制原理》，核心概念之一是开环控制和闭环控制。**闭环控制系统的特点是：系统的输出会通过反馈重新回到输入端。** 以空调为例：设定 26 度，传感器测得室温 30 度，差值大，压缩机全力制冷；几分钟后温度下降，新的温度又作为反馈输入传感器，差值变小，压缩机降低功率，最终室温稳定在 26 度。

Kubernetes 的控制器就是一个闭环控制系统：

| 闭环控制 | Kubernetes 控制器 |
| --- | --- |
| 设定值 | 对象的 `spec`（期望状态） |
| 传感器 | informer，持续观察集群的实际状态 |
| 控制器 | 调谐逻辑（reconcile），比较期望和实际 |
| 执行器 | 调用 API 创建、更新、删除资源 |
| 反馈 | 执行结果改变了集群状态，又被 informer 观察到，进入下一轮 |

一个关键的设计选择是：**控制器是水平触发（level-triggered）的，不是边沿触发（edge-triggered）的。** 它不关心「发生了什么事件」，只关心「现在是什么状态、应该是什么状态」。事件只是一个提醒：这个对象可能需要看一眼了。即使丢了几个事件、控制器重启过、同一个对象短时间内改了十次，只要最后拿最新状态对比一次，结果都是对的。后面会看到，informer 和 workqueue 的很多设计都是为这一点服务的。

## 二、controller、informer、operator 是什么关系

在 Kubernetes 世界里经常看到这几个词：kube-controller-manager、各种 xxx-operator、informer。它们描述的东西关系很紧密，但侧重点不同：

- **informer**：client-go 提供的一个组件，负责持续地从 API Server 获取某类资源的变化，在本地维护一份缓存，并在变化时通知你。它解决的是「怎么高效地观察集群」。
- **controller**：一种编程模式，即上一节说的控制循环。它用 informer 观察状态，用 workqueue 排队待处理的对象，用 reconcile 逻辑做调谐。kube-controller-manager 里打包了几十个这样的控制器。
- **operator**：本质上也是 controller，只是通常和 CRD 一起出现，把某个领域的运维知识（怎么部署、扩容、备份、升级一个数据库）编码进调谐逻辑里。Kubernetes 的内置资源和 CRD 在机制上没有本质区别，所以 controller 和 operator 在实现上几乎一样。

如果把 informer 类比成 Java 世界里的 HTTP 标准库，那么 controller-runtime（Kubebuilder 和 Operator SDK 的底座）就像 Spring Boot：它把 informer、workqueue、调谐循环这些细节封装起来，让你只写 `Reconcile` 方法。不同场景里大家会用不同的词，但它们讲的是同一套机制的不同层次。

要理解上层，就要自底向上，先把 informer 搞清楚。

## 三、informer 全景

client-go 里设计最精巧的部分就是 informer，它也是让 Kubernetes 这个庞然大物稳定运行的重要基石。[sample-controller](https://github.com/kubernetes/sample-controller/blob/master/docs/controller-client-go.md) 仓库里有一张流传很广的图，把它的原理画得很清楚：

![client-go 与自定义控制器的交互](/images/posts/k8s-programming/client-go-controller.jpeg)

图分成上下两半：

- **上半部分 client-go**：这些组件 client-go 都已经实现好了，源码在 `k8s.io/client-go/tools/cache`；
- **下半部分 Custom Controller**：黄色部分是写控制器的人要实现的，可以理解为几个待填的钩子。

图里的序号是事件发生的顺序，共 9 步：

| 步骤 | 发生了什么 | 组件 |
| --- | --- | --- |
| 1 | List & Watch：先全量列出，再从那个版本开始监听变化 | Reflector |
| 2 | 把对象变化放进队列 | Reflector → DeltaFIFO |
| 3 | 从队列里弹出对象 | informer（controller） |
| 4 | 把对象交给 Indexer | informer → Indexer |
| 5 | 以 key 为索引存进线程安全的本地缓存 | Indexer → ThreadSafeStore |
| 6 | 把对象分发给注册的事件处理函数 | informer → ResourceEventHandler |
| 7 | 处理函数算出对象的 key，放进工作队列 | 自定义控制器 |
| 8 | worker 从工作队列取出 key | 自定义控制器 |
| 9 | 用 key 从本地缓存取出最新对象，执行调谐 | 自定义控制器 |

下面逐步对照源码。

## 四、上半部分：client-go 做了什么

### 4.1 Reflector：List & Watch（步骤 1、2）

Reflector 负责和 API Server 打交道。它的工作分两段：

1. **List**：先发一个 GET 请求列出全部对象，记下响应里的 `resourceVersion`；
2. **Watch**：从这个 `resourceVersion` 开始发起 `?watch=true` 请求，这是一个长连接，API Server 会把之后的每一次增、改、删以事件流的形式推过来。

Watch 连接断了就重连，并从最后看到的 `resourceVersion` 继续。如果这个版本太旧，etcd 已经压缩掉了，API Server 返回 410 Gone，Reflector 就回到第一步重新 List（源码中 `isExpiredError` 的分支）。新版本 client-go 引入了 WatchList（流式 List，client-go v0.35 起默认开启），把首次 List 也改成通过 watch 流返回，以减轻 API Server 构造大列表时的内存压力。

Reflector 不直接调用你的代码，它只是把变化写进一个队列：DeltaFIFO。

### 4.2 DeltaFIFO：按对象聚合的变化队列（步骤 2、3）

DeltaFIFO 的名字说明了它的两个特点：

- **Delta**：它存的不是对象本身，而是对象的变化，类型包括 `Added`、`Updated`、`Deleted`、`Replaced`（重新 List 时）、`Sync`（定期 resync 时）；
- **FIFO**：同一个对象（以 `namespace/name` 为 key）的多个变化会合并到同一个队列项里，按顺序排好，弹出时整组一起处理。

这样，同一个对象在短时间内被改了很多次，也只占一个队列位置；处理时能看到完整的变化序列，最后落到的是最新状态。

### 4.3 弹出、更新缓存、分发事件（步骤 3～6）

informer 内部的循环不断从 DeltaFIFO 弹出一组 delta，交给 `processDeltas`：

```go
// k8s.io/client-go/tools/cache/controller.go（节选）
// from oldest to newest
for _, d := range deltas {
    obj := d.Object
    switch d.Type {
    case Sync, Replaced, Added, Updated:
        if old, exists, err := clientState.Get(obj); err == nil && exists {
            if err := clientState.Update(obj); err != nil {
                return err
            }
            handler.OnUpdate(old, obj)
        } else {
            if err := clientState.Add(obj); err != nil {
                return err
            }
            handler.OnAdd(obj, isInInitialList)
        }
    case Deleted:
        if err := clientState.Delete(obj); err != nil {
            return err
        }
        handler.OnDelete(obj)
    }
}
```

这段代码完成了图中的步骤 4、5、6，顺序很重要：**先更新本地缓存（`clientState`，即 Indexer），再通知处理函数。** 所以处理函数被调用时，缓存里已经是新数据了，之后从缓存里取一定能拿到不比这个事件更旧的对象。

注意 `Added` 和 `Updated` 没有被区别对待：有没有这个对象，以缓存里是否存在为准，而不是以事件类型为准。这也是水平触发的体现。

### 4.4 Indexer：带索引的本地缓存（步骤 4、5）

Indexer 是一个线程安全的内存存储（ThreadSafeStore），主键是 `namespace/name`（由 `MetaNamespaceKeyFunc` 生成）。此外还可以注册二级索引，最常用的是按命名空间索引，这样「列出某个命名空间下的所有 Pod」就不用遍历全部对象。

控制器里通过 **Lister** 读这个缓存，比如 `podLister.Pods("default").Get("foo")`。**所有读操作都走本地内存，不访问 API Server。** 这是 informer 最大的价值：几十个控制器、几千个节点上的 kubelet 和 kube-proxy 如果都直接查询 API Server，控制面早就扛不住了。

### 4.5 共享与分发

实际使用时，很少直接创建单个 informer，而是用 `SharedInformerFactory`：

```go
factory := informers.NewSharedInformerFactory(clientset, 30*time.Second)
podInformer := factory.Core().V1().Pods()
```

同一个进程里，对同一种资源的所有使用者共享**一个** Reflector、**一个**缓存、**一条** watch 连接。每个注册进来的事件处理函数，会得到一个独立的 `processorListener`，它内部有一个无界的环形缓冲区（`pendingNotifications buffer.RingGrowing`）：

```go
// pendingNotifications is an unbounded ring buffer that holds all notifications not yet distributed.
// There is one per listener, but a failing/stalled listener will have infinite pendingNotifications
```

这样一个处理函数慢了，不会阻塞其他处理函数，也不会阻塞缓存更新。代价是这个慢处理函数的缓冲区会无限增长，所以**处理函数里不能做耗时操作**，这正是下一节 workqueue 存在的原因。

工厂创建时传入的 `30*time.Second` 是 resync 周期。到时间后，informer 会把**本地缓存里**的所有对象以 `Sync` 类型重新走一遍分发流程，相当于对每个对象都触发一次 `OnUpdate`。它不会访问 API Server，作用是给控制器一个定期重新调谐的机会，修复那些因外部原因偏离期望状态、却没有产生事件的情况。

## 五、下半部分：控制器做什么

### 5.1 事件处理函数：只入队 key（步骤 6、7）

事件处理函数拿到的是对象本身，但它通常只做一件事：算出对象的 key，放进工作队列。

```go
podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc: func(obj interface{}) {
        if key, err := cache.MetaNamespaceKeyFunc(obj); err == nil {
            queue.Add(key)
        }
    },
    UpdateFunc: func(old, new interface{}) {
        if key, err := cache.MetaNamespaceKeyFunc(new); err == nil {
            queue.Add(key)
        }
    },
    DeleteFunc: func(obj interface{}) {
        // 删除事件可能拿到 DeletedFinalStateUnknown，需要专门的 key 函数
        if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err == nil {
            queue.Add(key)
        }
    },
})
```

为什么入队的是 key 而不是对象？因为对象会过期。从入队到被处理之间，这个对象可能又被改了好几次。只存 key，处理时再去缓存里取，拿到的永远是那一刻的最新状态。这也是水平触发的直接体现：控制器关心的是「这个对象现在应该是什么样」，不关心「中间经历了哪些事件」。

### 5.2 workqueue：去重、并发控制、限流（步骤 7、8）

client-go 的 `workqueue` 不是一个普通的队列，它有三个关键特性：

**1. 去重。** 同一个 key 入队多次，在被处理前只会存在一份。源码里用两个集合实现：

```go
// k8s.io/client-go/util/workqueue/queue.go
// dirty defines all of the items that need to be processed.
dirty sets.Set[t]

// Things that are currently being processed are in the processing set.
// These things may be simultaneously in the dirty set. When we finish
// processing something and remove it from this set, we'll check if
// it's in the dirty set, and if so, add it to the queue.
processing sets.Set[t]
```

**2. 同一个 key 不会被并发处理。** 一个 key 正在处理时（在 `processing` 集合里），又有新事件把它加进来，它只会被标记在 `dirty` 里，不会再次出队。等当前处理结束调用 `Done(key)` 后，才重新入队。所以即使开了多个 worker，同一个对象也只会被一个 worker 处理，调谐逻辑里不用担心同一个对象的并发冲突。

**3. 失败重试带限流。** 默认的限流器是两种的组合：

```go
// k8s.io/client-go/util/workqueue/default_rate_limiters.go
func DefaultTypedControllerRateLimiter[T comparable]() TypedRateLimiter[T] {
    return NewTypedMaxOfRateLimiter(
        NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, 1000*time.Second),
        // 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
        &TypedBucketRateLimiter[T]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
    )
}
```

单个 key 每失败一次，等待时间翻倍，从 5 毫秒到最长 1000 秒；同时整体重试速度不超过每秒 10 次。这样一个一直失败的对象不会把控制器和 API Server 拖垮。

### 5.3 worker：取 key，读缓存，调谐（步骤 8、9）

```go
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
    key, shutdown := c.queue.Get()            // 步骤 8：阻塞直到有 key
    if shutdown {
        return false
    }
    defer c.queue.Done(key)                   // 处理完必须调用，否则这个 key 永远不会再出队

    err := c.syncHandler(ctx, key)
    if err == nil {
        c.queue.Forget(key)                   // 成功：清空这个 key 的失败计数
        return true
    }
    c.queue.AddRateLimited(key)               // 失败：按退避时间重新入队
    return true
}

func (c *Controller) syncHandler(ctx context.Context, key string) error {
    namespace, name, _ := cache.SplitMetaNamespaceKey(key)
    foo, err := c.fooLister.Foos(namespace).Get(name)   // 步骤 9：从本地缓存读
    if errors.IsNotFound(err) {
        return nil                                       // 对象已被删除，清理后返回
    }
    if err != nil {
        return err
    }
    // 比较 foo.Spec 和实际状态，创建、更新、删除下游资源
    // 最后更新 foo.Status
    return nil
}
```

`Done`、`Forget`、`AddRateLimited` 这三个调用是控制器里最容易写错的地方：漏了 `Done`，这个 key 会永远卡在 `processing` 里；成功后不调 `Forget`，下次失败的退避时间会从一个很大的值开始。

### 5.4 启动

```go
factory.Start(ctx.Done())                          // 启动所有 informer
if !cache.WaitForCacheSync(ctx.Done(), podInformer.Informer().HasSynced) {
    return fmt.Errorf("failed to sync caches")
}
for i := 0; i < workers; i++ {
    go wait.UntilWithContext(ctx, c.runWorker, time.Second)
}
```

一定要等缓存首次同步完成（`WaitForCacheSync`）再启动 worker，否则 worker 会在缓存还不完整时开始调谐，可能误以为某些对象不存在，进而误删资源。[服务发现那篇](/posts/k8s-service-discovery/)里 kube-proxy 要等 Service 和 EndpointSlice 都同步完才开始写规则，用的是同一个道理。

## 六、总结

- 控制器是一个闭环控制系统：informer 观察实际状态，reconcile 对比期望状态，调用 API 去执行，执行结果又被观察到。它是**水平触发**的，只关心当前状态，不关心事件序列。
- informer 是 client-go 的组件，controller 是控制循环这种模式，operator 是配合 CRD、编码了领域运维知识的 controller；controller-runtime 是在 client-go 之上封装的框架。
- informer 的数据通路：Reflector 做 List & Watch → DeltaFIFO 按对象聚合变化 → 先更新 Indexer 缓存，再分发事件 → 每个处理函数有独立的缓冲区。所有读操作都走本地缓存，同一种资源在一个进程里只有一条 watch 连接。
- 控制器的套路：事件处理函数只入队 key；workqueue 负责去重、保证同一个 key 不被并发处理、失败指数退避；worker 用 key 从缓存取最新对象做调谐，然后正确调用 `Done`/`Forget`/`AddRateLimited`。
- 启动时先等缓存同步完成，再启动 worker。

每一个步骤都有很多细节值得反复读源码体会，文字只能管中窥豹。理解了这套机制，再看 CRD 和 Operator 就会轻松很多：CRD 只是让 API Server 多了一种资源，Operator 只是用这套控制器机制去管理它。下一篇就从 CRD 开始。
