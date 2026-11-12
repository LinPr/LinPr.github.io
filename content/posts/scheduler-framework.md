---
title: '从 OpenResty phase engine 到 scheduler framework：看懂可插拔的插件式架构的设计'
date: 2026-11-09T16:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "openresty", "nginx", "架构"]
series: ["kube-scheduler"]
weight: 1
author: "LinPr"
description: "一个 HTTP 请求和一个 Pod，用的都是「阶段 + 钩子」的结构，却在实现上走了两条路。本文对照 nginx phase engine 与 kube-scheduler scheduling framework，拆解可插拔插件架构的三个核心问题。"
---

最近在读 kube-scheduler 的 framework 包。

读之前我以为调度器的代码是一整块紧耦合的逻辑。打开 `pkg/scheduler/framework/plugins/` 才发现，目录下是二十多个独立插件包：`nodename`、`nodeports`、`tainttoleration`、`imagelocality`……每个包只实现一个很窄的判定。framework 本身不定义任何调度语义——它不知道什么是「资源充足」，也不知道什么是「污点」，只负责在确定的时机调用确定的插件。

同样的组织方式，nginx 已经用了二十年。

把 OpenResty 和 scheduler 放在一起读之后，我认为可插拔的插件架构最终都要回答三个问题：

1. **控制流归谁？** 由插件决定下一步，还是由框架决定？
2. **顺序由谁确定？** 同一阶段的多个插件如何排序，跨阶段又如何排序？
3. **失败如何处理？** 插件返回失败后，框架是终止、跳过，还是补偿？

这三个问题的答案一旦不同，架构的行为就会完全不同。下面先讲 OpenResty，它的答案最简洁；再讲 scheduler，它的题目更难；最后回到这三个问题上做对照。

## 一、OpenResty：阶段固定，执行由 checker 驱动

### 1.1 阶段顺序由枚举定义

nginx 处理一个 HTTP 请求走的是固定流水线。`ngx_http_core_module.h` 用枚举定义了全部阶段：

```c
typedef enum {
    NGX_HTTP_POST_READ_PHASE = 0,
    NGX_HTTP_SERVER_REWRITE_PHASE,
    NGX_HTTP_FIND_CONFIG_PHASE,
    NGX_HTTP_REWRITE_PHASE,
    NGX_HTTP_POST_REWRITE_PHASE,
    NGX_HTTP_PREACCESS_PHASE,
    NGX_HTTP_ACCESS_PHASE,
    NGX_HTTP_POST_ACCESS_PHASE,
    NGX_HTTP_PRECONTENT_PHASE,
    NGX_HTTP_CONTENT_PHASE,
    NGX_HTTP_LOG_PHASE
} ngx_http_phases;
```

枚举值同时是数组下标，顺序在编译期固定。插件不能新增阶段，只能在已有阶段内注册 handler。完整的阶段顺序如下：

```
一个 HTTP 请求的 phase 顺序

  POST_READ            读取并预处理请求（realip 等）
     |
  SERVER_REWRITE       server_rewrite_by_lua*
     |
  FIND_CONFIG          按 URI 匹配 location
     |
  REWRITE              rewrite_by_lua*
     |
  POST_REWRITE         检测 URI 是否变更，变更则跳回 FIND_CONFIG
     |
  PREACCESS            limit_req / limit_conn
     |
  ACCESS               access_by_lua* / auth_basic / allow、deny
     |
  POST_ACCESS          satisfy any 的收尾
     |
  PRECONTENT           try_files / mirror
     |
  CONTENT              content_by_lua*
     |
  LOG                  log_by_lua*
```

### 1.2 执行器是 checker，插件只返回状态码

驱动这套阶段的是 `ngx_http_core_run_phases()`，核心逻辑只有几行：

```c
while (ph[r->phase_handler].checker) {
    rc = ph[r->phase_handler].checker(r, &ph[r->phase_handler]);
    if (rc == NGX_OK) {
        return;
    }
}
```

这里被调用的是 `checker`，不是插件。`checker` 是阶段的执行函数，由它决定是否调用、以及按什么顺序调用本阶段的 handler。插件返回的整数由 `checker` 解释，同一个返回值在不同阶段含义不同：

| 返回值 | `generic` checker 中的语义 | `rewrite` checker 中的语义 |
|---|---|---|
| `NGX_OK` | 本阶段完成，跳到 `next` | 结束整个请求 |
| `NGX_DECLINED` | 本 handler 不处理，继续下一个 | 本 handler 不处理，继续下一个 |
| `NGX_AGAIN` | 挂起请求，让出执行权 | 结束整个请求 |
| `NGX_DONE` | 挂起请求，让出执行权 | 挂起请求，让出执行权 |
| HTTP 状态码 | 以该状态码结束请求 | 以该状态码结束请求 |

`rewrite` checker 的代码只有三个分支，其余返回值全部落到最后一行：

```c
if (rc == NGX_DECLINED) { r->phase_handler++; return NGX_AGAIN; }
if (rc == NGX_DONE)     { return NGX_OK; }

/* NGX_OK, NGX_AGAIN, NGX_ERROR, NGX_HTTP_... */
ngx_http_finalize_request(r, rc);
return NGX_OK;
```

所以 `NGX_OK` 在 `generic` 里是「继续」，在 `rewrite` 里是「终止」，`NGX_AGAIN` 在 `generic` 里是「让出」，在 `rewrite` 里也是「终止」。

因此插件不持有控制流，它只返回一个整数；控制流的语义由框架侧的 `checker` 定义。

### 1.3 阶段内顺序等于注册顺序，实现方式是数组摊平

模块在 `postconfiguration` 回调里把自己的 handler 追加到 `cmcf->phases[阶段].handlers`。所有模块注册完毕后，`ngx_http_init_phase_handlers()` 把 11 个阶段的 handler 数组摊平成一条 `ngx_http_phase_handler_t` 数组：

```c
typedef struct {
    ngx_http_phase_handler_pt  checker;
    ngx_http_handler_pt        handler;
    ngx_uint_t                 next;
} ngx_http_phase_handler_t;
```

摊平过程中有两个关键实现细节：

1. **handler 倒序写入。** `for (j = nelts - 1; j >= 0; j--)` 倒序遍历、正序写入，最终数组里保持注册顺序。所谓「同阶段按注册顺序执行」就是这样实现的，运行时没有任何排序动作。
2. **同阶段共享同一个 `next`。** 跳转目标按阶段计算，不按 handler 计算。

摊平后的结构大致如下：

```
注册阶段，各模块追加到自己所属阶段的数组

  phases[REWRITE].handlers = [ ngx_http_rewrite_handler, m2, m3 ]

配置加载末尾，摊平为一维数组

  phase_engine.handlers =
    [0] checker=rewrite        handler=...SERVER_REWRITE...
    [1] checker=find_config
    [2] checker=rewrite        handler=ngx_http_rewrite_handler
    [3] checker=rewrite        handler=m2
    [4] checker=rewrite        handler=m3
    [5] checker=post_rewrite   next -> [1]，URI 变更时跳回
    ...
```

补充一点：`LOG` 阶段不在这条数组里。它由 `ngx_http_log_request()` 单独遍历 `phases[NGX_HTTP_LOG_PHASE].handlers` 执行，所以 `init_phase_handlers()` 的循环条件是 `i < NGX_HTTP_LOG_PHASE`。

### 1.4 OpenResty 的注册方式

OpenResty 是一组普通的 nginx 模块，区别只在于 handler 中执行的是 Lua。官方文档的指令位置图列出了全部 Lua 入口：

{{< figure src="/images/posts/scheduler-framework/openresty-phases-official.png" caption="lua-nginx-module 文档中的 Lua 指令位置图：一个请求从左到右流过，每一行对应一类 Lua 入口" >}}

这张图回答的是「一段 Lua 应该写在哪个指令里」，不是「框架按什么顺序调用插件」。图中 `init_by_lua*`、`ssl_*_by_lua*`、`header_filter_by_lua*`、`body_filter_by_lua*` 这些入口不属于 phase，它们的实现机制是函数指针挂载，与 phase handler 完全不同。之所以画在同一条轴上，是因为从使用者角度看，它们都属于「可插入代码的位置」。

真正注册进 phase engine 的只有四个阶段：

| 阶段 | Lua 指令 | 注册条件 |
|---|---|---|
| `SERVER_REWRITE` | `server_rewrite_by_lua*` | 配置中出现该指令 |
| `REWRITE` | `rewrite_by_lua*` | 配置中出现该指令 |
| `ACCESS` | `access_by_lua*` | 配置中出现该指令 |
| `LOG` | `log_by_lua*` | 配置中出现该指令 |

`content_by_lua*` 不参与 phase 注册，它直接设置 location 的 content handler：

```c
clcf->handler = ngx_http_lua_content_handler;
```

注册是有条件的：只有配置里出现对应指令时，`requires_rewrite`、`requires_access` 这类标志才会置位，才会执行 `ngx_array_push`。这一点与 nginx 内置模块不同——内置模块只要编译进来就无条件注册。

### 1.5 postpone：运行期重排 phase 数组

`rewrite_by_lua*` 和 `access_by_lua*` 的文档写的是「默认在该阶段末尾执行」。这不是排序语义标记，而是一次运行期的数组重排：

```c
last_ph = &ph[cur_ph->next - 1];

if (cur_ph < last_ph) {
    tmp = *cur_ph;
    memmove(cur_ph, cur_ph + 1,
            (last_ph - cur_ph) * sizeof(ngx_http_phase_handler_t));
    *last_ph = tmp;

    r->phase_handler--;   /* 重做当前这一格 */
    return NGX_DECLINED;
}
```

这段代码有三个要点：

1. **修改的是 `cmcf->phase_engine.handlers`。** 这是进程内共享的配置结构，不是请求私有的，整个配置生命周期只重排一次。
2. **`postponed_to_rewrite_phase_end` 置位后不再复位。** 第一个执行到这里的请求承担重排开销，后续请求看到的数组已经排好序。
3. **`r->phase_handler--` 配合 `NGX_DECLINED` 表示重做当前项。** 重做时 Lua handler 已在阶段末尾，条件不再成立，正常向下执行。

这个实现说明了一件事：框架没有提供「声明本插件应排在阶段末尾」的接口，模块只能直接修改 phase 数组。这种做法成立的前提是单线程、重排幂等、且没有并发读写该数组。

### 1.6 小结

- **控制流**：由框架持有。插件返回整数，`checker` 决定如何解释。
- **顺序**：由注册顺序决定。同阶段按注册顺序，跨阶段由枚举固定。
- **失败**：硬编码在各个 `checker` 中，按阶段分别定义。

## 二、scheduler：阶段是函数调用，插件在扩展点注册

转到 kube-scheduler，首先要换掉一个前提：这里处理的不是「一个请求」，而是一次决策。

HTTP 请求的结果是二元的——成功响应或错误响应。一个 Pod 进来面对的却是数百上千个候选节点，最终只能选择一个。这个差别决定了后续大部分设计。

{{< figure src="/images/posts/scheduler-framework/scheduler-framework-official.png" caption="Kubernetes 文档中的 scheduling framework 图：调度队列、调度周期、绑定周期，以及节点维度的扇出" >}}

整体流程如下：

```
一个 Pod 的调度流程

  队列  activeQ / backoffQ / unschedulableQ
    |
    v
  PreEnqueue --> QueueSort            决定下一个处理哪个 Pod
    |
  +---------------------------------------------------------- 调度周期（串行）
  |   PreFilter --> Filter --> [PostFilter]
  |                   |             ^
  |                   |             |
  |                   |        所有节点均不通过时才进入
  |                   |        默认实现为 DefaultPreemption
  |                   |
  |                   +-- 每个候选节点调用一次
  |
  |   PreScore --> Score --> Reserve --> Permit
  |                  |
  |              每个候选节点调用一次，
  |              之后还有归一化、权重计算和求和
  |
  +----------------------------------------------------------
                               |
                               v
  +---------------------------------------------------------- 绑定周期（异步）
  |   PreBind --> Bind --> PostBind
  +----------------------------------------------------------
```

与 nginx 相比，有七处不同。

### 2.1 阶段顺序由函数调用表达

scheduler 没有 `init_phase_handlers()`，没有 `checker`，也没有 `next` 下标。阶段顺序就是 `schedule_one.go` 中的调用先后：

```go
a.findNodesThatFitPod(ctx, fwk, state, podInfo)   // PreFilter + Filter
a.prioritizeNodes(...)                            // PreScore + Score
a.reserve(...)                                    // Reserve
schedFramework.RunPermitPlugins(...)              // Permit
```

代价是缺少统一执行抽象，收益是每个扩展点可以定义自己的失败语义。举三个例子：

- `RunFilterPlugins`：第一个返回非 `Success` 的插件立即返回，后续插件不再执行。
- `RunPreFilterPlugins`：遇到 `Unschedulable` 时记录状态并继续执行剩余插件，因为后续抢占需要全部插件的评估结果。
- `RunBindPlugins`：返回 `Skip` 的插件让给下一个；第一个不返回 `Skip` 的插件认领这个 Pod，无论成功还是失败，剩余插件都不再调用。

这三套逻辑无法用同一个 `checker` 表达，因此 scheduler 不做统一抽象。

### 2.2 插件顺序由配置决定

nginx 的插件顺序取决于模块链接顺序，从配置文件里看不出来。scheduler 把它显式化为配置项：

```go
type PluginSet struct {
    // Enabled specifies plugins that should be enabled in addition to default plugins.
    // These are called after default plugins and in the same order specified here.
    Enabled []Plugin
}
```

默认插件在前，`Enabled` 中的插件在后，各自按列表顺序执行。字段注释明确写了执行顺序就是列表顺序。

需要注意：使用 `multiPoint` 写法时，`expandMultiPointPlugins()` 会按三段重新排列：

1. `overridePlugins`：在具体扩展点中显式写过的插件，顺序按其在扩展点中的位置。
2. `multiPointEnabled`：只在 multiPoint 中出现、未在扩展点中出现的插件。
3. `enabledSet` 中的其余插件。

也就是说，YAML 中书写的顺序不一定等于最终执行顺序。

### 2.3 失败语义由 Status 表达

nginx 的失败语义分散在各个 `checker` 中，同一个返回值在不同阶段含义不同。scheduler 把它收敛为一个值：`Status`。

插件返回 `fwk.NewStatus(code, msg)`，框架依据 code 决定后续动作。但同一个 code 落在不同扩展点上，框架的处理并不相同：

| code | 在 PreFilter 返回 | 在 Filter 返回 |
|---|---|---|
| `Success` | 继续下一个插件 | 继续下一个插件；全部通过则节点可行 |
| `Skip` | 记录插件名，本周期跳过它的 Filter | 不允许，被当作 `Error` |
| `Unschedulable` | 记下失败，**继续**执行剩余插件 | 该节点不可行，但可以作为抢占候选 |
| `UnschedulableAndUnresolvable` | **立即终止**，所有节点都视为不可解决 | 该节点不可行，且不参与抢占 |
| `Error` | 本轮失败，Pod 进入退避 | 本轮失败，Pod 进入退避 |

还有两个只在特定扩展点使用的 code：`Wait` 只能由 Permit 返回，表示「先别绑定，等我的信号」；`Pending` 用于等待外部条件的场景，与 `Unschedulable` 一样被视为拒绝。

关键不在分类数量，而在于**同一个 code 在不同扩展点上有不同处理**。`RunPreFilterPlugins` 是最清楚的例子：

```go
if s.Code() == fwk.UnschedulableAndUnresolvable {
    // 抢占无法解决，剩余插件不必执行
    return nil, s, nil
}
if s.Code() == fwk.Unschedulable {
    // 仍可能通过抢占解决，而抢占需要全部插件的评估结果
    returnStatus = s
    continue
}
```

同样表示不可行，一个立即返回，一个记录后继续。nginx 的返回码只能表达「这个阶段怎么走」，scheduler 的 `Status` 能表达「这次决策怎么走」。

实践上有一个容易犯的错误：在 PreFilter 中对所有失败都返回 `UnschedulableAndUnresolvable`，等价于关闭抢占，而且不会有任何日志提示。

### 2.4 调用次数是扇出

这是两边最根本的差异。

- **nginx 的阶段**：请求处理流程上的一个位置，每个请求调用一次。
- **scheduler 的扩展点**：阶段与对象的二维组合，`Filter` 和 `Score` 对每个候选节点各调用一次。

{{< figure src="/images/posts/scheduler-framework/scheduler-fanout-score.svg" caption="Filter 与 Score 的扇出：外层在节点维度并行，内层在插件维度串行；Score 之后还有归一化和加权汇总两轮" >}}

`Score` 的执行分三轮：

1. **每个节点并行执行所有 Score 插件**，插件之间串行，得到原始分数。
2. **每个插件并行执行 `NormalizeScore`**，将分数归一化到 `[MinScore, MaxScore]`；`ScoreExtensions()` 返回 nil 的插件跳过这一轮。
3. **每个节点并行执行范围检查、乘权重、求和**，`TotalScore = Σ(score × weight)`。

范围检查放在归一化之后。实现了 `NormalizeScore` 的插件，原始分数可以不在 `[0, 100]` 内；未实现的插件，原始分数必须自己落在 `[0, 100]` 内。越界不会导致「分数不准」，而是让整个调度周期返回错误：

```go
err := fmt.Errorf("plugin %q returns an invalid score %v, it should in the range of [%v, %v] after normalizing",
    pl.Name(), score, fwk.MinScore, fwk.MaxScore)
```

另一点容易被忽略：**候选节点不等于全部节点**。`numFeasibleNodesToFind()` 会对节点抽样：

- 节点总数小于 `minFeasibleNodesToFind`（100）时，返回全部节点。
- 否则按百分比抽样，默认比例为 `max(50 - numAllNodes/125, 5)`，即从 50% 起随规模递减，最低 5%。
- 计算结果低于 100 时按 100 计。

此外，如果当前 profile 既没有打分插件、也没有 extender 过滤，`numFeasibleNodesToFind()` 直接返回 1：不需要比较，找到第一个可行节点就够了。

### 2.5 跨阶段状态由 CycleState 承载

两边采用同一个模式：阶段之间不通过返回值传递数据，而是共享一个生命周期与本次处理绑定的容器。

- OpenResty：`ngx.ctx`，每个请求一份。
- scheduler：`CycleState`，每个调度周期一份。

`CycleState` 的接口是：

```go
Read(key StateKey) (StateData, error)
Write(key StateKey, val StateData)
Delete(key StateKey)
Clone() CycleState
```

用法与 `ngx.ctx` 基本一致：`PreFilter` 中 `Write`，`Filter` 和 `Score` 中 `Read`。但实现上有一个 `ngx.ctx` 不需要考虑的问题：并发。

```go
// pkg/scheduler/framework/cycle_state.go
type CycleState struct {
    // storage is keyed with StateKey, and valued with StateData.
    storage sync.Map
    // ...
    skipFilterPlugins        sets.Set[string]
    skipScorePlugins         sets.Set[string]
    skipAllPostFilterPlugins bool
    // ...
}
```

nginx 的一个 worker 是单线程的，同一个请求的 `ngx.ctx` 不会被并发访问。scheduler 的 Filter 和 Score 却是在多个节点上并行执行的，它们读的是**同一份** `CycleState`，所以底层用 `sync.Map`。这带来三条约束：

1. PreFilter 写入的数据，在 Filter 阶段应当只读。并行的 Filter 如果修改共享数据，需要插件自己加锁（`VolumeBinding` 的状态里就嵌了一个 `sync.Mutex`）。
2. 抢占模拟要在节点上「假装移除」Pod，会修改状态。所以它先调用 `Clone()` 复制一份，在副本上做，不影响正式的调度周期。
3. 生命周期是单次调度周期。Pod 失败重新入队后是一份新的状态，不能当作跨周期缓存；key 冲突时后写覆盖先写，自定义插件应使用包级常量作为 key。

与 `ngx.ctx` 的差别在于，`CycleState` 除了承载业务数据，还承载流程控制标记：`GetSkipFilterPlugins()`、`GetSkipScorePlugins()`、`ShouldSkipAllPostFilterPlugins()` 都存在其中。框架把「本次是否跳过某个插件」也作为跨阶段传递的状态。

### 2.6 失败之后还有补偿

nginx 的失败是终点：请求终止，没有资源需要回收。scheduler 不同，因为**它操作的资源有状态、有生命周期**。

具体体现在两处：

1. **`PostFilter` 是失败后的第二个入口。** `Filter` 淘汰所有节点时，调度周期并不结束，而是进入 `PostFilter`（默认实现 `DefaultPreemption`，尝试驱逐低优先级 Pod 腾出位置）。代码中对该分支有明确的前置判断：

    ```go
    if !schedFramework.HasPostFilterPlugins() {
        logger.V(3).Info("No PostFilter plugins are registered, so no preemption will be performed")
    }
    ```

    没有注册 PostFilter 插件，就不会执行抢占。这行日志在排查「Pod 长期 Pending 但没有任何 Pod 被驱逐」时很有用。

2. **`Unreserve` 是 `Reserve` 的逆操作。** `Reserve` 成功后，如果 `Permit` 拒绝、等待超时，或者 `PreBind` / `Bind` 失败，框架会按 **Reserve 执行的逆序**调用所有 `Unreserve`：

    ```go
    // Execute the Unreserve operation of each reserve plugin in the
    // *reverse* order in which the Reserve operation was executed.
    for i := len(f.reservePlugins) - 1; i >= 0; i-- {
    ```

    逆序是为了让回收顺序与分配顺序严格对称。文档同时要求 `Unreserve` 必须幂等，因为它可能在 `Reserve` 从未被调用的情况下也被调用。

### 2.7 调度周期串行，绑定周期异步

nginx 处理 I/O 的方式是「让出」：handler 返回 `NGX_AGAIN` 或 `NGX_DONE`，请求挂起，worker 去处理别的事件，I/O 就绪后再从当前 phase 继续。一个 worker 可以同时推进成千上万个请求。

scheduler 面对的是同样的问题：绑定要写 API Server，挂载卷要等外部控制器，都是慢操作。它的做法是把一次调度拆成两段：

```go
// pkg/scheduler/schedule_one.go
scheduleResult, assumedPodInfo, status := sched.schedulingCycle(schedulingCycleCtx, state, schedFramework, podInfo, start, podsToActivate)
if !status.IsSuccess() {
    sched.FailureHandler(schedulingCycleCtx, schedFramework, assumedPodInfo, status, scheduleResult.nominatingInfo, start)
    return
}

// bind the pod to its host asynchronously (we can do this b/c of the assumption step above).
go sched.runBindingCycle(ctx, state, schedFramework, scheduleResult, assumedPodInfo, start, podsToActivate)
```

调度周期（PreFilter 到 Permit）是串行的，一次只处理一个 Pod；绑定周期（PreBind 到 PostBind）放进 goroutine 异步执行，调度器马上开始处理下一个 Pod。

注释里的「assumption step」是这个设计能成立的关键。调度周期结束前，调度器先把 Pod「假定」写进自己的缓存：

```go
// pkg/scheduler/algorithm.go
// AssumeAndReserveInCache assumes the pod into the scheduler cache and runs Reserve plugins.
// If Reserve plugins fail, the assumption is rolled back from the cache.
func (a *SchedulingAlgorithm) AssumeAndReserveInCache(...) (*framework.QueuedPodInfo, *fwk.Status) {
    assumedPodInfo := a.prepareAssumedPod(logger, state, podInfo, host)
    if err := a.cache.AssumePod(logger, assumedPodInfo.Pod); err != nil {
        return assumedPodInfo, fwk.AsStatus(err)
    }
    // ...
    if status := a.reserve(ctx, state, schedFramework, assumedPodInfo, podInfo.Pod, host); !status.IsSuccess() {
        a.UnreserveAndForgetFromCache(ctx, state, schedFramework, assumedPodInfo, host)
        return assumedPodInfo, status
    }
    return assumedPodInfo, nil
}
```

下一个 Pod 的调度周期开始时，会用缓存更新节点快照（`UpdateSnapshot`），这时它已经能看到上一个 Pod 占用的资源，即使那个 Pod 的绑定 API 调用还没返回。这是一种**乐观执行**：先假设绑定会成功，让后续决策基于这个假设；绑定真的失败时，由 `Unreserve` 和 `ForgetPod` 把假设撤回。

Permit 的 `Wait` 也建立在这个结构上。插件返回 `Wait` 后，Pod 被放进 waiting pods 表，调度周期照常结束；绑定周期的 goroutine 一开始就调用 `WaitOnPermit` 阻塞，直到插件发出 `Allow` 或 `Reject`，或者超时。等待只占用一个 goroutine，不占用调度周期。gang 调度就是利用这一点，让一组 Pod 在 Permit 处汇合。

两边对照：

| | nginx | scheduler |
|---|---|---|
| 慢操作 | 网络 I/O | API 写入、卷挂载、等待同组 Pod |
| 让出方式 | 返回 `NGX_AGAIN`，事件就绪后重入 phase | 拆成两个周期，绑定在 goroutine 中执行 |
| 后续处理看到什么 | 不涉及共享资源 | 通过 assume 看到「假定已绑定」的结果 |
| 失败回滚 | 不需要 | `Unreserve` + `ForgetPod` |

nginx 的请求之间不共享资源，挂起一个请求不影响别的请求。调度器的 Pod 之间共享节点资源，异步之后必须有一个机制让后来者看到先来者的决定，assume 就是这个机制。

## 三、逐项对照

| 维度 | OpenResty / nginx | kube-scheduler |
|---|---|---|
| 处理对象 | 一个 HTTP 请求 | 一个 Pod × 一批候选节点 |
| 阶段定义 | 11 个，编译期枚举 | 12 个经典扩展点，Go 接口 |
| 阶段顺序 | 摊平后的数组 + `checker` | 函数调用顺序 |
| 阶段内顺序 | 模块注册顺序 | `Enabled` 列表（经 MultiPoint 重排） |
| 失败语义 | `checker` 中硬编码 | `Status` code |
| 跨阶段状态 | `ngx.ctx` | `CycleState`（含流程控制标记） |
| 失败后补偿 | 无 | `PostFilter` / `Unreserve` |
| 慢操作 | `NGX_AGAIN` 挂起，事件驱动重入 | 绑定周期异步，assume 乐观写缓存 |
| 跨阶段状态的并发 | 单线程，无并发 | Filter/Score 并行读，`sync.Map` |
| 运行期能否调整顺序 | 能，postpone 直接重排数组 | 不能，初始化后为只读切片 |
| 插件加载方式 | 编译进 nginx，或 Lua 文件 | 编译进 kube-scheduler 二进制 |

表中「12 个经典扩展点」指 `PreEnqueue`、`QueueSort`、`PreFilter`、`Filter`、`PostFilter`、`PreScore`、`Score`、`Reserve`、`Permit`、`PreBind`、`Bind`、`PostBind`，也就是官方框架图覆盖的那一组。`v0.36.4` 的接口定义中另有 `Sign`（KEP-5598，批处理签名），以及服务于 PodGroup 调度的 `PlacementGenerate`、`PlacementScore`、`PlacementFeasible`、`PodGroupPostFilter` 等新增扩展点。

表格里都是现象。再追一层，根本原因只有一句：**nginx 处理的是无状态的短生命周期请求，scheduler 处理的是有状态的长期资源。**

请求失败后丢弃即可，没有已占用的资源需要归还，所以 nginx 采用「终止」这一最简单的失败模型就够了。

Pod 不同。`Reserve` 已经占用了节点资源，`PreBind` 可能已经挂载存储卷，后续任何一步失败都必须由框架还原。因此它必须有 `Unreserve`，而且必须逆序；因此它必须有 `PostFilter` 这个失败后的第二个入口。

这些差异不来自设计水平的高低，而来自领域模型本身。

## 四、设计启示

综合两边的做法，如果自己设计一个插件架构，我会按下面的顺序考虑。

1. **先固定阶段，再开放钩子。** 阶段必须是少数、稳定、可枚举的。OpenResty 真正注册进 phase engine 的只有 4 个阶段，scheduler 只有 12 个经典扩展点。插件只能填充阶段内部，不能新增阶段——这是框架能够对执行顺序作出承诺的前提。

2. **顺序要显式，不要依赖加载顺序。** nginx 的顺序取决于模块链接顺序，这是历史包袱。scheduler 用 `Enabled` 列表把它显式化，方向是对的。只要顺序会影响结果，就应该让使用者能看到、能修改。

3. **失败语义要做成数据。** `NGX_OK` 在不同 `checker` 中含义不同，属于「返回码语义依赖上下文」的典型问题。scheduler 的 `Status` 也没有完全摆脱上下文——`Skip` 只在部分扩展点有意义——但它至少把「有哪几种失败、各自意味着什么」定义清楚了。

4. **状态放在显式容器里。** `ngx.ctx` 和 `CycleState` 都是这个思路，天然按请求或调度周期隔离。代价有两个：一是隐式依赖，读取一个 key 时，你不知道是谁写的、什么时候写的；二是一旦阶段内部有并行，容器就必须并发安全，写入方和读取方还要约定好谁能改。scheduler 的 Filter 并行执行，所以 `CycleState` 用 `sync.Map`，并且约定 Filter 阶段只读。

5. **有副作用的操作必须配一个反操作。** `Reserve` / `Unreserve` 是最好的例子。只要一个接口有「占用资源」的动作，就应该同时定义「如何归还」，并保证它可以被重复调用，因为调用方无法确定它是否真的执行过。

6. **慢操作移出主循环，用乐观假设保持一致。** nginx 用事件驱动挂起请求，scheduler 把绑定拆成异步周期。后者多出来的一步是 assume：既然后续决策依赖前一个决策的结果，就先假定它成功，失败再撤回。

前四条在两边都能找到对应实现，后两条只有 scheduler 需要，原因仍然是那个领域差异。

## 五、结语

可插拔的插件架构，本质上是在划分一条边界：

> **「什么时候执行」归框架，「具体如何执行」归插件。**

OpenResty 把这条边界画在阶段上：你写 Lua，框架负责在正确的时机调用。scheduler 把边界画在扩展点上，并且因为要做决策、要处理有状态的资源，这条边界被切得更细——一次决策过程中，数据要在框架和插件之间往返十几个步骤。

读完这两套代码，我最大的收获是发现**同一套结构在不同领域下会生长出完全不同的细节**。与其记住每个阶段的名字，不如记住那三个问题：控制流归谁、顺序由谁确定、失败如何处理。把这三个问题问清楚，一个插件架构的行为基本也就清楚了。

框架讲完了，下一篇[《kube-scheduler in-tree 插件源码拆解》](/posts/kubernetes-scheduler-in-tree-plugins/)看框架里实际装了什么：24 个内置插件分别在这套规则下做了哪些决定。

---

*nginx 部分依据 nginx 1.20.2 源码（`src/http/ngx_http.c`、`src/http/ngx_http_core_module.c`、`src/http/ngx_http_request.c`）与 lua-nginx-module（commit c9a0808）的 phase 注册和 postpone 逻辑；scheduler 部分依据 `k8s.io/kube-scheduler@v0.36.4` 与 `pkg/scheduler/framework/runtime/framework.go`、`pkg/scheduler/algorithm.go`、`pkg/scheduler/schedule_one.go` 的实现。`PRECONTENT` 是 nginx 1.13.4 引入的名称，更早的版本叫 `TRY_FILES`。*
