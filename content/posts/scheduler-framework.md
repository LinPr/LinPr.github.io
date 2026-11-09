---
title: '从 OpenResty phase engine 到 scheduler framework：看懂可插拔的插件式架构的设计'
date: 2026-11-09T16:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "openresty", "nginx", "架构"]
author: "LinPr"
description: "一个请求和一个 Pod，同一套「阶段 + 钩子」的骨架，长出了两种完全不同的细节。这篇是我把 OpenResty 和 kube-scheduler 摆在一起读的笔记。"
---

最近在啃 kube-scheduler 的 framework 包。

读之前我有点心理准备，觉得调度器这种东西，逻辑总该是硬的。结果翻开 `pkg/scheduler/framework/plugins/` 目录，看到的是一串小包：`nodename`、`nodeports`、`tainttoleration`、`imagelocality`……每个都只回答一个很窄的问题。框架本身反而对调度几乎一无所知——它不知道什么叫「资源够用」，也不知道什么叫「污点」，它只知道**什么时候该叫谁**。

这种写法我熟。写 OpenResty 的时候，nginx 也是这么干的。

于是我把两边摆在一起看。看久了发现，所谓可插拔的插件架构，要回答的其实就三个问题：

- **控制流归谁？** 是插件自己决定下一步，还是框架说了算？
- **顺序从哪来？** 同一个阶段里有好几个插件，谁先谁后，由谁定？
- **搞砸了怎么办？** 插件返回一个失败，是终止、跳过，还是补救？

这三个问题听着琐碎，可答案一不同，整个架构的气质就变了。下面先说 OpenResty——它的答案最短，也最经典——再说 scheduler。最后回头聊几句，这三问对平时写程序有什么用。

## 一、OpenResty：先有流水线，才谈得上插件

nginx 处理一个 HTTP 请求，走的是一条固定的流水线。源码里就是一个枚举，从 `POST_READ` 一路排到 `LOG`，中间不带商量的：

```
一个请求在 nginx 里的时间轴

  POST_READ            realip 之类，改改请求本身
     |
  SERVER_REWRITE       server_rewrite_by_lua*
     |
  FIND_CONFIG          按 URI 找到 location
     |
  REWRITE              rewrite_by_lua*
     |
  POST_REWRITE         URI 变了就跳回 FIND_CONFIG
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

这张图好懂，但它藏着一个前提：**这些阶段是写死在枚举里的。** 插件没办法新增一个阶段，只能往已有的格子里插队。

那「插队」这件事是谁在管？看驱动这套流水线的代码，只有这么几行（`ngx_http_core_module.c`）：

```c
while (ph[r->phase_handler].checker) {
    rc = ph[r->phase_handler].checker(r, &ph[r->phase_handler]);
    if (rc == NGX_OK) {
        return;
    }
}
```

注意，这里没有「调用插件」这回事，只有「调用 checker」。checker 是阶段自己的执行器，由它来决定要不要、以及按什么顺序调用这一阶段的 handler。插件返回的那个整数，是被 checker 解读的：

- `NGX_DECLINED`：这一步我没意见，往下走；
- `NGX_OK`：这一格我办完了（在 rewrite 阶段，这甚至是「结束请求」）；
- 一个 HTTP 状态码：请求到此为止。

所以插件其实什么都没掌控，它只交出**一个信号**。怎么理解这个信号，是阶段的事。

顺序也差不多。模块在配置加载时（`postconfiguration` 回调）把自己的 handler 塞进 `cmcf->phases[阶段].handlers`，等所有模块都注册完，nginx 把 11 个阶段的数组摊平成一条一维数组：

```
注册时，各模块往自己的阶段里 append

  phases[REWRITE].handlers = [ ngx_http_rewrite_handler , m2 , m3 ]

配置加载末尾，摊平

  phase_engine.handlers =
    [0] checker=rewrite        handler=...SERVER_REWRITE...
    [1] checker=find_config
    [2] checker=rewrite        handler=ngx_http_rewrite_handler
    [3] checker=rewrite        handler=m2
    [4] checker=rewrite        handler=m3
    [5] checker=post_rewrite               <- URI 变了就跳回 [1]
    ...
```

摊平的时候是倒着拷的，所以数组里保持了注册顺序；而同一个阶段的所有 handler 共用**同一个 `next`**——跳转目标是按阶段算的，不是按 handler 算的。这就是「同阶段按注册顺序执行、跨阶段按枚举位置执行」的全部实现。

OpenResty 就是一堆普通的 nginx 模块，只是它的 handler 里跑的是 Lua。官方文档里那张图，画的就是它能挂的位置：

{{< figure src="/images/posts/scheduler-framework/openresty-phases-official.png" caption="lua-nginx-module 文档里的指令位置图：一个请求从左到右流过，每一行是一种 Lua 入口" >}}

这张图有个容易看岔的地方：它画的不是「框架会按什么顺序调用你」，而是「**我应该把这段 Lua 写在哪**」。里面 `init_by_lua`、`ssl_*`、`header_filter` 这些根本不是一个 request phase，实现机制也完全不同（它们挂的是函数指针，不是 phase handler），可站在使用者角度，它们都只是「能插代码的地方」，于是就画在同一根轴上了。

那顺序到底重不重要？我觉得最能说明问题的，是一个很小的细节。

`rewrite_by_lua*` 和 `access_by_lua*` 的文档写着「默认在阶段末尾执行」。听起来像个排序标记，实际上它是一次**运行时的数组重排**：第一次执行到这一格时，lua 直接把自己的那格用 `memmove` 挪到这个阶段的最后一格。

```c
last_ph = &ph[cur_ph->next - 1];

if (cur_ph < last_ph) {
    tmp = *cur_ph;
    memmove(cur_ph, cur_ph + 1,
            (last_ph - cur_ph) * sizeof(ngx_http_phase_handler_t));
    *last_ph = tmp;

    r->phase_handler--;   /* 重做当前这格 */
    return NGX_DECLINED;
}
```

它改的是 `cmcf` 里那份**共享**的数组，不是请求私有的。所以整个进程只挪一次：第一个走到这里的请求付这个代价，后面的请求拿到手时数组已经排好了。

这件事有意思的地方不在代码，在**动机**。框架没提供「我想排在后面」的接口，插件于是自己动手改了框架的数据结构。这在 C 里能做，因为那个数组就是个裸指针；但代价也明摆着——它只在单线程、重排幂等、且没人并发读写的前提下成立。

可它至少证明了一件事：**「我这个插件必须排在别人后面」，是真实存在、而且躲不开的需求。**

小结一下 OpenResty 的答案：

- 控制流：**框架的**。插件只交一个返回码。
- 顺序：**注册的**。同阶段看注册先后，跨阶段看枚举写死。
- 失败：**硬编码的**。每个 checker 自己写好了失败之后干什么。

这三条都很朴素，但撑住了 nginx 二十年的扩展生态。

## 二、scheduler：同一个骨架，换了一道更难的题

带着这三个问题去看 kube-scheduler，第一个要换掉的观念是：这里处理的不是「一个请求」，而是「**一次决策**」。

一个 HTTP 请求只对应一个响应，要么成，要么败。可一个 Pod 进来，面对的是几百上千个节点，最后只能挑一个。这个差别，几乎决定了后面所有的细节。

{{< figure src="/images/posts/scheduler-framework/scheduler-framework-official.png" caption="Kubernetes 文档里的 scheduling framework 图：队列、调度周期、绑定周期，以及节点维度的扇出" >}}

骨架还是那个骨架——一串阶段，每段留钩子，插件挂上去：

```
一个 Pod 的一生

  队列  activeQ / backoffQ / unschedulableQ
    |
    v
  PreEnqueue --> QueueSort          决定下一个处理谁
    |
  +----------------------------------------------------------
  | 调度周期（串行，一次只处理一个 Pod）
  |
  |   PreFilter --> Filter --> [PostFilter]
  |                   |             ^
  |                   |             |
  |                   |             全部淘汰才进，通常做抢占
  |                   |
  |                   +-- 每个候选节点跑一次
  |
  |   PreScore --> Score --> Reserve --> Permit
  |                  |
  |                  每节点一次，之后还要归一化、乘权重、求和
  |
  +----------------------------------------------------------
                               |
                               v
  +----------------------------------------------------------
  | 绑定周期（异步执行，可以和下一个 Pod 的调度周期重叠）
  |
  |   PreBind --> Bind --> PostBind
  |
  +----------------------------------------------------------
```

粗看还是那些东西。但细看会发现几处 nginx 没有的复杂度——每一处，我都能追到「一次决策」这四个字上。

### 1. 阶段顺序：没有数组了，只有函数调用

scheduler 里没有 `init_phase_handlers` 那样的摊平，也没有 checker。阶段顺序就是 `schedule_one.go` 里函数调用的先后：

```go
a.findNodesThatFitPod(ctx, fwk, state, podInfo)   // PreFilter + Filter
a.prioritizeNodes(...)                            // PreScore + Score
a.reserve(...)                                    // Reserve
schedFramework.RunPermitPlugins(...)              // Permit
```

少了一层抽象，换来的是**每个扩展点可以有自己的脾气**。Filter 是「第一个失败就返回」；PreFilter 却要「遇到 Unschedulable 还得把剩下的跑完」；Bind 是「第一个认领的说了算，后面的全部跳过」。这些语义没法统一，nginx 那种通用 checker 在这里根本写不出来。

### 2. 插件顺序：从链接顺序变成配置文件

nginx 的顺序取决于模块链接的先后，用户改起来很别扭——你在配置里看不出谁先跑，得翻源码。scheduler 把它做成了配置的一部分：

```go
type PluginSet struct {
    // Enabled specifies plugins that should be enabled in addition to default plugins.
    // These are called after default plugins and in the same order specified here.
    Enabled []Plugin
```

默认插件在前，用户加的在后，各按自己列表里的顺序。直白得让人放心。

不过有个坑：如果你用的是 `multiPoint` 写法，`expandMultiPointPlugins()` 会按三段重新拆开——显式写在具体扩展点里的排最前，只写在 multiPoint 里的其次，其余垫底。**你 YAML 里写的顺序，不等于实际执行的顺序。** 这一点我读源码之前完全没想到。

### 3. 失败：从硬编码变成数据

这是我觉得 scheduler 最现代的一处设计。

nginx 的失败语义，是每个 checker 各自写死的——`NGX_OK` 在 generic 阶段是「往下走」，在 rewrite 阶段却是「结束请求」，同一个返回值两种含义。scheduler 把它提炼成了一个值：`Status`。

插件返回 `framework.NewStatus(code, msg)`，框架根据 code 决定下一步：

| code | 框架的反应 |
|---|---|
| `Success` | 继续下一个插件 |
| `Skip` | 记下名字，配对的扩展点（PreFilter ↔ Filter）也一起跳过 |
| `Unschedulable` | 这个节点不行，换下一个 |
| `UnschedulableAndUnresolvable` | 连抢占都救不了，别再试了 |
| `Error` | 插件自己挂了，Pod 退避重试 |

强的地方不在于种类多，而在于**同一个「失败」，在不同扩展点上可以被分别定义**。最能说明问题的是 PreFilter：

```go
if s.Code() == fwk.UnschedulableAndUnresolvable {
    // 抢占也没用，剩下的不用跑了
    return nil, s, nil
}
if s.Code() == fwk.Unschedulable {
    // 还有机会靠抢占救回来，而抢占需要所有插件的信息
    returnStatus = s
    continue
}
```

同样是「不行」，一个是「立刻收工」，一个是「把活干完再说」。nginx 表达不了这个区别，因为它的返回码是对**阶段**说的，而这里的 `Status` 是对**决策**说的。

顺带说一个实践上的坑：在 PreFilter 里图省事，所有情况都返回 `UnschedulableAndUnresolvable`，等于把抢占静悄悄地关掉了。Pod 会一直 Pending，而且没人告诉你为什么。

### 4. 一个阶段跑几次：扇出

这是两边最根本的不对称，也是那张官方图唯一试图表达、却最容易被划过去的东西。

nginx 的阶段，是**一维时间轴上的一个点**：一个请求，一次调用。
scheduler 的阶段，是**一个二维格子**：阶段 × 对象。

{{< figure src="/images/posts/scheduler-framework/scheduler-fanout-score.svg" caption="Filter 与 Score 的扇出：外层在节点维度并行，内层在插件维度串行；Score 之后还有两趟收尾" >}}

Filter 是每个候选节点跑一次，节点之间并行、节点内部的插件串行。Score 更夸张，跑三趟：先拿原始分，再让每个插件把分数归一到 `[0, 100]`，最后做范围检查、乘权重、求和。

范围检查放在归一化**之后**，这点很重要：实现了 `NormalizeScore` 的插件，原始分可以随便给；没实现的，原始分自己就必须落在 `[0, 100]` 里。选错了不会「分数不准」，会直接让整个调度周期报错。

还有一种更隐蔽的扇出在前头：**「每个节点」并不等于「全部节点」**。默认情况下 `numFeasibleNodesToFind()` 会按总量抽样——50% 起步，节点越多比例越低，最低 5%，但至少 100 个。也就是说，你的 Score 插件不一定会看到每一个节点。如果打分逻辑依赖全局视角，这一点得先想清楚。

### 5. 状态：`CycleState`，以及它多装的那点东西

两边都用了同一个套路：阶段之间不靠返回值传数据，而是共享一个短命的容器。

OpenResty 是 `ngx.ctx`，每个请求一份；scheduler 是 `CycleState`，每个调度周期一份。PreFilter 里 `Write`，Filter 和 Score 里 `Read`，写法几乎是照抄的。

但 `CycleState` 多装了一样东西：**流程控制标记**。`GetSkipFilterPlugins()`、`GetSkipScorePlugins()` 这些也放在里面——框架把「这个插件这次要不要跳过」也当成了要跨阶段传递的状态。

这其实是在说：在 scheduler 里，**控制流本身也是数据**。nginx 里对应的东西是 `r->phase_handler` 和 `r->uri_changed`，都直接挂在请求结构上，插件只读不写。

### 6. 有失败，还有「失败之后」

nginx 的失败是终点：请求扔了，没有东西需要善后。scheduler 不是——**它操作的资源是有状态的、有生命周期的**。

所以它有两样 nginx 里根本不存在的东西：

- `Filter` 把所有节点都淘汰了，调度周期**不算结束**，会进 `PostFilter`（默认实现是 `DefaultPreemption`，驱逐低优先级的 Pod 来腾位置）。「这个 Pod 现在放不下」不等于「这个 Pod 没救了」。
- `Reserve` 一旦占了节点上的资源，后面 `Permit` 拒绝、等待超时、或者 `PreBind` / `Bind` 失败，框架都会把所有 `Reserve` 插件的 `Unreserve` **逆序**调一遍。逆序是为了和分配严格对称，像压栈出栈；而且文档明确要求 `Unreserve` 幂等，因为它可能在 `Reserve` 根本没被调用的情况下也被调用。

## 三、三个问题，两个答案

把两边的答案并排放在一张表里：

| | OpenResty / nginx | kube-scheduler |
|---|---|---|
| 执行对象 | 一个请求 | 一个 Pod × 一批节点 |
| 阶段 | 11 个，编译期枚举 | 12 个扩展点，接口方法 |
| 阶段顺序 | 摊平后的数组 + checker | 函数调用顺序 |
| 阶段内顺序 | 模块注册先后 | `Enabled` 列表（经 MultiPoint 重排） |
| 失败语义 | checker 里写死 | `Status` code，数据 |
| 跨阶段状态 | `ngx.ctx` | `CycleState`（还装着控制标记） |
| 失败后补偿 | 无 | `PostFilter` / `Unreserve` |
| 运行时能否改顺序 | 能（postpone 直接改数组） | 不能（初始化后只读） |
| 扩展方式 | 加载 .so / Lua 文件 | 编进二进制 |

上半张表都是现象。再往下追一层，根子只有一句话：**nginx 处理的是无状态的短命请求，scheduler 处理的是有状态的长期资源。**

请求失败了，丢掉就行，没有东西需要撤销，所以 nginx 的失败模型可以是「终止」——最简单的那个。

可 Pod 不是。`Reserve` 占了资源，`PreBind` 挂了存储卷，后面任何一步失败，都必须有人把这些东西还原。所以它必须有 `Unreserve`，而且必须逆序；所以它必须有 `PostFilter` 这个「失败之后的第二个入口」。

这些不是 scheduler 的设计师比 nginx 的设计师想得多，是**题目本身变难了**。

## 四、如果让我去设计一个插件架构

看了这两套东西，我大致会按这个顺序想问题。

**先把阶段定死，再开放钩子。** 阶段是骨架，必须是少数、稳定、可枚举的。OpenResty 真正挂进 phase 引擎的只有四个阶段（server_rewrite、rewrite、access、log），`content_by_lua` 连阶段都不占，直接写 location 的 handler；scheduler 也不过 12 个扩展点。插件只能往格子里填，不能加格子——这不是吝啬，是保护。格子一多，框架就什么都不敢保证了。

**顺序要显式，别让它取决于加载顺序。** nginx 的顺序取决于模块链接的先后，这是个历史包袱：配置里看不出来谁先跑，得翻源码。scheduler 用 `Enabled` 列表把这层显式化，虽然 MultiPoint 又加了一层隐式重排，但方向是对的。**任何「顺序影响结果」的系统，都该让顺序在用户手里可见、可写。**

**失败语义要做成数据。** `NGX_DECLINED`、`NGX_OK`、`NGX_DONE` 在不同 checker 里含义不同，这是「返回码语义取决于上下文」的经典反面教材。scheduler 的 `Status` 也没完全摆脱上下文（`Skip` 只在部分扩展点有意义），但它至少把「有哪几种失败」「每种意味着什么」摆到了明面上。

**状态放显式容器，别靠返回值传。** `ngx.ctx` 和 `CycleState` 都是这个思路，天生请求隔离，不用考虑并发。代价是隐式：你读一个 key 的时候，不知道是谁写的、什么时候写的、还在不在。所以 scheduler 才需要专门写清楚「只活一次调度周期」「后写的赢」这种注意事项——**显式容器的代价，是隐式的数据依赖。**

**有副作用的操作，必须配一个反操作。** `Reserve` / `Unreserve` 是最好的例子。只要一个 API 有「占资源」的动作，就应该同时定义「怎么还回去」，而且要保证它可以被重复调用，因为调用方不知道它到底有没有真的执行过。

这五条里，前四条在两边都能看到影子，第五条只有 scheduler 需要——又绕回了那句话。

## 最后

回到标题。可插拔的插件式架构，说到底就是把两件事分开：

> **「什么时候做」归框架，「具体怎么做」归插件。**

OpenResty 的分界线画在阶段上：你写 Lua，框架负责把你叫醒。
scheduler 的分界线画在扩展点上，而且因为要做决策、要处理有状态的资源，这条线画得精细得多——一份数据要在框架和插件之间来回走十几个回合。

读这两套代码最大的收获，倒不是什么新范式。是发现**同一个骨架，在两种题目下会长出完全不同的细节**。

所以下次再看到「把流程拆成阶段、每段留钩子」的设计，我大概会先把那三个问题问一遍：控制流在谁手里？顺序从哪来？失败了谁收场？问完，架构的形状基本就出来了。

---

*nginx 部分依据 nginx 1.20.2（`src/http/ngx_http.c`、`src/http/ngx_http_core_module.c`）与 lua-nginx-module（commit c9a0808）的 phase 注册和 postpone 逻辑；scheduler 部分依据 `k8s.io/kube-scheduler@v0.36.4` 的 framework 接口与 `pkg/scheduler` 下的运行时实现。`PRECONTENT` 这个名字是 nginx 1.13.4 才有的，更早的版本叫 `TRY_FILES`。*
