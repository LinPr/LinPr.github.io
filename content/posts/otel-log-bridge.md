---
title: '不换日志库也能接入 OTel：Go 日志桥接模式的原理与实战'
date: 2026-11-11T23:00:00+08:00
draft: false
tags: ["go", "opentelemetry", "可观测性", "日志", "slog"]
author: "LinPr"
description: "Go 的 OpenTelemetry 日志走的是桥接而不是接管：实现日志库自己的扩展点，把记录翻译成 OTel 类型。这一个翻译点，把可扩展性切成了 handler 和 processor 两层。"
---

给一个已经上线的 Go 服务接 OpenTelemetry，最麻烦的往往不是 trace 和 metric，而是日志。

trace 和 metric 好办，因为它们的 API 是 OTel 自己定义的。加一个 `tracer.Start`、注册一个 `Int64Counter`，改动是增量的。日志不一样：服务里已经有一整套 `slog` 或 `zap` 的调用，有固定的字段约定、有日志轮转、有下游的检索规则。让业务把所有 `logger.Info` 改成 `otelLogger.Emit`，几乎不可能推得动。

OTel 在 Go 这里没有硬推。它选的是另一条路：

> **不接管日志库，而是实现日志库自己的扩展点，把每条日志翻译成 OTel 的 `log.Record`。**

这就是桥接（bridge）模式。`go.opentelemetry.io/contrib/bridges/` 下面那几个包——`otelslog`、`otelzap`、`otellogrus`、`otellogr`——各自只做一件事：把宿主日志库的一条记录，转成 OTel 的记录。

这件事有意思的地方在于，翻译点本身是一个分界：

- 翻译**之前**，你拿到的是日志库的原始类型，字段还是 Go 值，但看不到 trace 上下文。
- 翻译**之后**，你拿到的是 OTel 的 `log.Record`，trace_id 已经填好，但结构体信息已经丢了。

于是"扩展"自然分成两层：桥接前的 `handler` 包装，和桥接后的 `processor` 包装。两者不是可以互相替代的两种写法，它们能做的事有明确交集，也有明确的分工。

下面按三个问题展开：

1. 桥接到底发生在哪一层，翻译过程中丢了什么？
2. handler 包装能做什么，边界在哪里？
3. processor 包装能做什么，什么时候必须用它？

最后用一个真实工程 `go-bootstrap/otel` 的初始化流程收尾，把顺序和几个坑说清楚。

## 一、原理：一条 record 的旅程

### 1.1 四层结构

一条日志从业务代码到后端，中间会经过四层：

{{< figure src="/images/posts/otel-log-bridge/two-wrappers.svg" caption="桥接点把链路切成两段，也把扩展性切成两层。虚线框内是可包装的位置" >}}

- **宿主日志库**：`log/slog`、`zap`、`logrus`、`logr`，负责格式、字段、级别、输出。
- **桥接层**：`otelslog` 等 contrib 包，负责翻译。
- **Logs SDK**：`go.opentelemetry.io/otel/sdk/log`，负责 resource、processor 链、批量与导出。
- **Exporter**：stdout、文件、OTLP HTTP/gRPC。

一个容易被忽略的事实：桥接层不在 SDK 里。`otelslog` 是 `go.opentelemetry.io/contrib/bridges/` 下的独立 module，而 `sdk/log` 是 `go.opentelemetry.io/otel/sdk/log`。这个拆分不是随意的——它意味着 SDK 不认识任何日志库，桥接实现可以独立演进、独立发版，也意味着你可以自己写一个桥接。

### 1.2 翻译表

以 `otelslog` 为例，`slog.Record` 到 `log.Record` 的映射是固定的：

| `slog.Record` 字段 | `log.Record` 字段 | 说明 |
|---|---|---|
| `Time` | `Timestamp` | 原样 |
| `Message` | `Body` | `attribute.StringValue` |
| `Level` | `Severity` + `SeverityText` | 加固定偏移，见下 |
| `PC` | `code.file.path` 等三个属性 | 仅 `WithSource(true)` 时 |
| `Attrs` | `Attributes` | 按 `slog.Kind` 映射 |
| error 类型的 Attr | `Record.SetErr` | 不进 Attributes，SDK 再派生 `exception.*` |

级别的换算只有一行：

```go
const sevOffset = slog.Level(log.SeverityDebug) - slog.LevelDebug
record.SetSeverity(log.Severity(r.Level + sevOffset))
```

`slog.LevelDebug` 是 -4，`log.SeverityDebug` 是 5，偏移量是 9。于是 `Info` (0) 落到 9，正好是 `SeverityInfo`；`Warn` (4) 落到 13，`Error` (8) 落到 17。OTel 的 severity 每档占 4 个值（DEBUG 是 5–8，INFO 是 9–12），所以 `slog.LevelDebug+1` 这种自定义中间级别也不会被压平，落在 6，仍然属于 DEBUG 区间。这个设计是线性映射，不是查表。

`EnabledParameters` 里的级别用的是同一个偏移，所以过滤器在两边的判断一致。

### 1.3 翻译的边界：`slog.KindAny` 里放的是什么

真正决定后面一切的是属性的转换。`otelslog` 按 `slog.Kind` 分派：

| `slog.Kind` | 转换结果 |
|---|---|
| Bool / Int64 / Float64 / String | 同类型 |
| Uint64 | 装得下 int64 就转 `Int64Value`，否则 `Float64Value` |
| Duration / Time | `Int64Value`，纳秒 |
| Group | `MapValue`，递归 |
| Any（slice / array） | `SliceValue`，逐元素递归 |
| Any（map） | `MapValue`，逐项递归 |
| Any（struct） | **`StringValue`，内容是 `fmt.Sprintf("%+v", v)`** |
| Any（error） | 被提升为 `Record.SetErr` |

倒数第二行是全篇的技术支点，它在 `convert.go` 里只有一句：

```go
case reflect.Struct:
    return attribute.StringValue(fmt.Sprintf("%+v", v))
```

也就是说：**一个 struct 类型的日志字段，在穿过桥接点之后就变成了一段不可逆的文本。**

这里没有"以后再解析回来"的余地。`attribute.Value` 的类型集合是固定的：四种标量（bool、int64、float64、string），加上它们对应的切片、`[]byte` 和 map。没有"任意 Go 值"这一项。struct 不在里面，指针会被解引用，接口会被拆开，最终能放进去的只有这些。

这解释了很多现象。比如为什么 `slog.Any("user", user)` 在 Grafana 里看到的是一坨文本，而不是可以展开的字段；比如为什么想保留结构，只能在桥接之前动手。

还有一个细节值得注意：error 被单独拎出来，走的是 `SetErr` 而不是 Attributes。这样 SDK 的 `newRecord` 才能按 Logs 规范派生出 `exception.type` 和 `exception.message`，后端的错误聚合才有东西可看。后面讲 `jsonHandler` 时会看到，它特意跳过了 error，就是为了不破坏这条路径。

### 1.4 为什么 OTel 不自己做一个日志库

不是因为做不出来，而是因为做出来也没人用。Go 生态里没有 SLF4J 那样的门面，`log/slog` 到 Go 1.21 才进标准库，在此之前每个项目都有自己的选择。OTel 如果要求所有人换库，等于要求所有依赖库也跟着换。

另一个信号写在版本号里。这个工程的 `go.mod` 里：

- `go.opentelemetry.io/otel` 是 `v1.46.0`
- `go.opentelemetry.io/otel/log` 是 `v0.22.0`
- `go.opentelemetry.io/otel/sdk/log` 是 `v0.22.0`

trace 和 metric 的 API 在 v1，日志还在 v0。稳定的是 **Logs Bridge API**（桥接方用的那部分），而 **Logs API**（应用直接调用的那部分）还在演进。`provider.go` 里甚至留了一行 TODO：

```go
// TODO: update this to use logs api not ask when otel 1.47 version released
global.SetLoggerProvider(p.logProvider)
```

一个还在 v0 的信号，不适合要求业务代码直接依赖它。桥接是更现实的选择，也是更低风险的接入方式。

## 二、handler 包装：在翻译之前动手

### 2.1 `slog.Handler` 的四个方法就是扩展契约

要在 `slog` 这一侧做包装，只需要实现 `slog.Handler`：

```go
type Handler interface {
    Enabled(context.Context, Level) bool
    Handle(context.Context, Record) error
    WithAttrs(attrs []Attr) Handler
    WithGroup(name string) Handler
}
```

包装一个已有的 handler，只需要内嵌它，覆写关心的那一个方法：

```go
type myHandler struct {
    slog.Handler
}

func (h myHandler) Handle(ctx context.Context, r slog.Record) error {
    // 改一改 r
    return h.Handler.Handle(ctx, r)
}
```

内嵌让 `Enabled`、`WithAttrs`、`WithGroup` 自动透传，看起来只需要写一个方法。

但这里有个必须处理的细节：**`WithAttrs` 和 `WithGroup` 也必须覆写**，否则包装会在 `logger.With(...)` 时丢失。

原因在 `slog.Logger.With` 里：

```go
func (l *Logger) With(args ...any) *Logger {
	c := l.clone()
	c.handler = l.handler.WithAttrs(argsToAttrSlice(args))
	return c
}
```

它调的是 handler 的 `WithAttrs`，然后用返回值构造新的 logger。如果包装类型没有覆写这个方法，方法提升会把调用直接交给内层 handler，返回的是**内层**的类型。下一次 `Handle` 就走不到你的包装了。

这是一个很安静的 bug：单条日志正常，带 `With` 的日志突然开始丢字段。

正确的写法是每一层都把自己包回去。工程里的 `jsonHandler` 就是这么写的：

```go
func (h jsonHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
    jsonified := make([]slog.Attr, len(attrs))
    for i, a := range attrs {
        jsonified[i] = jsonifyAttr(a, h.pretty)
    }
    return jsonHandler{Handler: h.Handler.WithAttrs(jsonified)}
}

func (h jsonHandler) WithGroup(name string) slog.Handler {
    return jsonHandler{Handler: h.Handler.WithGroup(name)}
}
```

注意 `WithAttrs` 里先做了一次 `jsonifyAttr`。这一步是必要的：`With` 传进来的属性会存在内层 handler 里，如果你只处理 `Handle` 时的属性，这些预先挂载的字段就会漏掉转换。

### 2.2 例一：`jsonHandler`，保住结构

`jsonHandler` 解决的就是 1.3 里那个问题。

它做的事很简单：在 `Handle` 里遍历属性，遇到 `KindAny` 且不是 error 的值，先 `json.Marshal` 成字符串，再交给 `otelslog`。

```go
func (h jsonHandler) Handle(ctx context.Context, record slog.Record) error {
    newRecord := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
    record.Attrs(func(attr slog.Attr) bool {
        newRecord.AddAttrs(jsonifyAttr(attr, h.pretty))
        return true
    })
    return h.Handler.Handle(ctx, newRecord)
}
```

有三个实现上的考虑：

1. **必须重建 record。** `slog.Record` 的属性可以追加、可以遍历，但不能替换或删除，所以想改写只能造一个新的。
2. **必须跳过 error。** 如果 error 也被序列化成字符串，`otelslog` 就认不出它是 error，`SetErr` 不会被调用，`exception.type` / `exception.message` 全部消失。这个判断在 `jsonifyAttr` 里：

   ```go
   a := v.Any()
   if _, ok := a.(error); ok {
       return attr
   }
   ```

3. **它只能放在 slog 这一层。** 放到 processor 里是无效的，因为 processor 拿到的 attribute 已经是 `attribute.Value`，struct 早在 `convertValue` 里被 `%+v` 掉了。信息在桥接处丢失，下游无法还原。

这个例子说明了 handler 层的核心价值：**它是唯一还能看到 Go 原始值的位置。**

代价也要说清楚。JSON 化之后，字段类型从结构变成了字符串，后端不能再按字段做嵌套查询，只能全文检索。如果下游需要结构化查询，更好的做法是让 `otelslog` 的 `Group` 机制去处理——用 `slog.Group("user", "name", u.Name, "age", u.Age)` 显式展开，而不是塞一个 struct 进去。`jsonHandler` 是个通用的兜底，不是首选。

### 2.3 例二：`baggageHandler`，把上下文带进来

第二个例子解决另一类问题：信息在 `context.Context` 里，日志库不知道要输出它。

```go
func (bh *baggageHandler) Handle(ctx context.Context, record slog.Record) error {
    members := baggage.FromContext(ctx).Members()
    for _, member := range members {
        if _, ok := bh.baggageMembers[member.Key()]; ok {
            record.AddAttrs(slog.String(member.Key(), member.Value()))
        }
    }
    return bh.handler.Handle(ctx, record)
}
```

这里有个小细节：`Record` 的副本共享底层属性数组，`AddAttrs` 把新属性写进这份共享存储，所以不需要像 `jsonHandler` 那样重建，直接改完往下传就行。前提是没有别的地方同时持有这个 record。

这个 handler 通常包在 `otelslog` 外面，成为最外层：

```go
slog.SetDefault(slog.New(
    slogbaggage.NewBaggageHandler(
        slog.Default().Handler(),   // 此时已经是 otelslog 的 handler
        slogbaggage.WithBaggageMembers("x-request-id", "x-tenant-id"),
    ),
))
```

顺序不能反。`NewOtelProviders` 内部已经调过 `slog.SetDefault`，这里是在它的结果上再包一层；如果先包再 `SetDefault`，前面那层会被直接覆盖掉。

### 2.4 handler 层能做什么，不能做什么

能做的：

- 改写属性：补字段、脱敏、把结构体编码成 JSON。
- 丢弃记录：`Handle` 里直接 `return nil`，不调内层。
- 改写级别：重建 record 时换一个 `Level`。
- 读 `ctx`：baggage、请求 ID、业务上下文都在这里。

不能做的：

- **拿不到 trace_id / span_id。** 这两个字段是 SDK 的 `newRecord` 从 `ctx` 里取 `SpanContext` 填进去的，发生在桥接之后。handler 阶段它们还不存在。
- **改不了 resource。** resource 是 provider 级别的，handler 看不到。
- **影响不了导出。** 重试、批量、超时都是 SDK 的事。

还有一个性能上的约束：`Handle` 在调用方 goroutine 上同步执行。`json.Marshal` 每个 struct 都要走一次反射，高频日志路径上要留意。

### 2.5 四种桥接，四种包装点

这一节是全文我觉得最有意思的观察。

同样的"桥接"，在四个日志库上的实现形态完全不同：

| 日志库 | 桥接实现 | 宿主扩展点 | 包装形态 | 能否与原有输出并存 |
|---|---|---|---|---|
| `log/slog` | `otelslog` | `slog.Handler` | 实现四个方法，包住内层 | 能，handler 可以扇出 |
| `zap` | `otelzap` | `zapcore.Core` | `NewCore`，或 `zap.WrapCore` | 能，`WrapCore` 叠加 |
| `logrus` | `otellogrus` | `logrus.Hook` | `AddHook` | 能，Hook 是旁路 |
| `logr` | `otellogr` | `logr.LogSink` | `NewLogSink` | 能，Sink 由调用方组装 |

桥接的形态，取决于宿主库自己提供了什么扩展点，而不是 OTel 想让它长什么样。

- `slog` 的扩展点是 handler，所以桥接是一个 `slog.Handler`，包装就是再套一个 handler。
- `zap` 的扩展点是 `zapcore.Core`，所以桥接是一个 `Core`。`zap.WrapCore` 能在不替换原有 core 的前提下叠一层，这意味着同一份日志可以同时写文件和发 OTLP。
- `logrus` 的扩展点是 `Hook`，Hook 本来就是旁路，所以桥接天然不影响原有输出。
- `logr` 的扩展点是 `LogSink`，桥接是一个 sink。

这也是为什么 OTel 不提供一个统一的"日志适配器"：四个库的寄生方式根本不一样，统一不了。真正统一的部位在桥接之后，也就是下面要讲的 `Processor`。

顺带一提，`go-bootstrap/otel` 里 zap 和 slog 的级别过滤走的是两条不同的路。zap 用 `zap.IncreaseLevel(zapLevel)` 在 Core 层就地过滤；slog 没有这个选项，`otelslog` 也没有提供 `WithLevel`，所以只能在 SDK 的 processor 里加一层 `severityProcessor`。同样的需求，因为宿主库的能力不同，落到了两个不同的层次上。

## 三、processor 包装：在翻译之后动手

### 3.1 `Processor` 的四个方法

SDK 侧的扩展点是 `log.Processor`：

```go
type Processor interface {
    Enabled(ctx context.Context, param EnabledParameters) bool
    OnEmit(ctx context.Context, record *Record) error
    Shutdown(ctx context.Context) error
    ForceFlush(ctx context.Context) error
}
```

和 handler 最大的区别是输入：这里是 `*log.Record`，完全是 OTel 的类型，和 `slog`、`zap` 没有关系。

所以**一个 processor 能同时服务所有桥接实现的输出**。这是跨日志库统一处理的唯一位置。

### 3.2 Record 是共享且可变的

`Processor` 的文档里有两句话，决定了整个链的行为：

> The SDK invokes the processors sequentially in the same order as they were registered with `WithProcessor`. Implementations may synchronously modify the record so that the changes are visible in the next registered processor.

也就是说，processor 链是有序的，而且后一个能看到前一个的修改。这跟 HTTP middleware 的结构是一样的：注册顺序就是执行顺序，责任链上传递的是同一个对象。

由此推出两条规则：

1. **顺序有意义。** 过滤器应该放在批处理之前，否则记录先入队再被丢弃，白占队列配额。
2. **异步处理必须 `Clone()`。** `Record` 不是并发安全的，文档明确要求异步场景用 `Record.Clone` 复制一份。

对应的代码在 `logger.go` 里，`newRecord` 只构造一次，然后整条链共用同一个指针：

```go
newRecord := l.newRecord(ctx, r)
for _, processor := range processors {
    if err := processor.OnEmit(ctx, &newRecord); err != nil {
        errs = append(errs, err)
    }
}
```

### 3.3 `Enabled` 和 `OnEmit` 是两条独立的路径

这是最容易写错的地方。文档里写得很直白：

> `OnEmit` will be called independently of `Enabled`.

`Enabled` 不是拦截器，它只是一个提示。SDK 的 `Logger.Enabled` 实现是"任意一个 processor 说 true 就是 true"：

```go
for _, processor := range processors {
    if processor.Enabled(ctx, p) {
        return true
    }
}
return false
```

注意这是**聚合**判断，不是逐个协商。由此有两个推论：

1. 如果要在 processor 里做过滤，必须同时实现 `Enabled`（让上层少构造 record）和 `OnEmit`（保证正确性）。只实现一个都会出问题：只实现 `OnEmit` 会白构造；只实现 `Enabled` 会漏过滤。
2. 注册多个 processor 时，只要有一个返回 true，上层就会构造 record。你的过滤只对自己那条链路生效，别的 processor 该收还是收。

`severityProcessor` 正是按这个规则写的：

```go
func (p *severityProcessor) OnEmit(ctx context.Context, record *log.Record) error {
    if record.Severity() >= p.minSeverity {
        return p.Processor.OnEmit(ctx, record)
    }
    return nil
}

func (p *severityProcessor) Enabled(ctx context.Context, param log.EnabledParameters) bool {
    return param.Severity >= p.minSeverity && p.Processor.Enabled(ctx, param)
}
```

`OnEmit` 里再比一次不是冗余。`Enabled` 只是上层的短路优化，SDK 并不保证调用方一定先问过 `Enabled`。

### 3.4 只写两个方法：内嵌接口的用处

`severityProcessor` 只写了一个字段和两个方法：

```go
type severityProcessor struct {
    log.Processor
    minSeverity otellog.Severity
}
```

`Shutdown` 和 `ForceFlush` 没有实现，但类型仍然满足 `log.Processor`——内嵌的接口把这两个方法提升上来了，调用会直接透传给下游。

这是一种很好用的 Go 写法：**内嵌接口而不是内嵌结构体**。前者只要求"能转发"，不关心下游的具体类型；后者会把实现细节带进来。

同样的手法在 `jsonHandler` 里也出现了，只不过那边内嵌的是 `slog.Handler`。

### 3.5 processor 层能做什么，不能做什么

能做的：

- 过滤：按 severity、按 attribute、按 scope。
- 改写：脱敏、补字段、截断超长值、限制属性数量。
- 采样：按 trace_id 或业务 key 决定留不留。
- 路由：按 severity 或 scope 分发到不同 exporter。
- 聚合：在 `OnEmit` 里计数，在 `Shutdown` 时输出汇总。

不能做的：

- **恢复被翻译丢失的结构。** 见 2.2，struct 已经变成字符串了。
- **改 resource 和 instrumentation scope。** `Record` 上只有读取方法（`Resource()`、`InstrumentationScope()`），没有对应的 setter。
- **拿到日志库特有的信息。** 有些东西桥接没有对应的表达方式，就直接丢了。最典型的是 `slog.Record` 的 `PC`：不带 `WithSource(true)` 时，它不会变成 `code.*` 属性。再比如自定义的 `zapcore.Encoder`：桥接实现的是 `Core`，根本不经过你的 Encoder。

能做 trace 相关的事，是 processor 相对 handler 最实用的一点：`newRecord` 在 processor 之前执行，`trace_id`、`span_id`、`trace_flags` 都已经在 `Record` 上了。想按 trace 采样、想给同一条 trace 的日志打统一的标记，只能在这一层做。

## 四、两层的分工

把上面的结论收成一张表：

| 维度 | handler 包装 | processor 包装 |
|---|---|---|
| 位置 | 桥接之前 | 桥接之后 |
| 输入类型 | `slog.Record`（原始 Go 值） | `log.Record`（OTel 类型） |
| 能保住 struct 吗 | 能 | 不能 |
| 跨日志库 | 不能，每个库写一份 | 能，一份服务所有桥接 |
| 影响范围 | 这一条 handler 链 | 所有进入 SDK 的记录 |
| 能看到 trace_id 吗 | 不能 | 能 |
| 执行位置 | 调用方 goroutine | 调用方 goroutine（Batch 之前） |
| 典型用途 | 结构编码、ctx 增强、格式适配 | 过滤、脱敏、采样、路由 |

判断标准可以压成一句话：**看这个信息在翻译之后还在不在。**

- 还在的（级别、属性、时间），两层都能做，优先选 processor，因为跨库通用。
- 不在的（struct 的原始类型、`ctx` 里还没被提取的东西），只能在 handler 层做。
- 翻译之后才出现的（trace_id、span_id、resource），只能在 processor 层做。

## 五、实战：初始化顺序与几个坑

### 5.1 初始化顺序

`go-bootstrap/otel` 的 `initLog` 把所有部件按依赖顺序串了起来（省略了错误处理）：

```go
logExporter, err := p.createLogExporter(logConfig)

var processor log.Processor
processor = log.NewBatchProcessor(logExporter)
if logConfig.Logger == LoggerTypeSlog {
    slogSeverity, _, err := parseLogLevel(logConfig.Level)
    processor = newSeverityProcessor(processor, slogSeverity)
}

p.logProvider = log.NewLoggerProvider(
    log.WithProcessor(processor),
    log.WithResource(res),
    log.WithAttributeCountLimit(logConfig.AttributeCountLimit),
)

global.SetLoggerProvider(p.logProvider)

if err := p.setupLoggerBridge(logConfig); err != nil { ... }
```

顺序是固定的，每一步都在满足下一步的前置条件：

1. **先建 exporter。** processor 需要它。
2. **再建 processor，需要过滤就包一层。** provider 需要它。
3. **再建 provider。** 这时才把 resource 和属性上限绑定上去。
4. **设为全局。** 其他 contrib 组件（比如没显式传 provider 的 `otelzap`）会从这里取。
5. **最后建桥接 handler，`slog.SetDefault`。** 因为 `otelslog.NewHandler` 在构造时就会调用 `provider.Logger(name)` 拿到 logger 实例。

第 5 步的顺序最容易被写错。`NewHandler` 里的 `cfg.logger(name)` 是**构造时求值**，不是每次 `Handle` 时再取。所以 provider 必须已经存在，否则拿到的是一个空实现。

包 processor 的位置也有讲究。`newSeverityProcessor` 包在 `BatchProcessor` 外面，所以过滤发生在入队之前。反过来写的话，被过滤掉的记录会先占队列位置（默认 2048 条）、再被丢掉，高峰期可能把队列挤满。

### 5.2 完整调用链

把 handler 链、桥接、processor 链串起来，一次 `slog.InfoContext` 的实际路径是：

```
slog.InfoContext(ctx, "msg", "k", v)
  |
  v
baggageHandler.Handle          app-level wrapper
  |
  v
jsonHandler.Handle             optional, Pretty=true
  |
  v
otelslog.Handler.Handle        bridge
  |  convertRecord()
  v
sdk/log.(*logger).Emit
  |  newRecord(ctx) -> trace_id, span_id
  v
severityProcessor.OnEmit       SDK-level wrapper
  |
  v
BatchProcessor.OnEmit -> queue(2048) -> Export
  |
  v
Exporter: stdout / file / OTLP
```

`BatchProcessor` 的默认参数是：队列 2048 条、导出间隔 1 秒、批量上限 512 条、单次导出超时 30 秒。这些都可以用 `WithMaxQueueSize`、`WithExportInterval`、`WithExportMaxBatchSize`、`WithExportTimeout` 调整。

队列满时的行为值得注意：**丢弃最旧的记录，保留最新的**，同时累加一个 dropped 计数。它不会阻塞调用方，也不会把新记录扔掉。日志路径上不阻塞是对的，但意味着高峰期丢的是更早的现场。

另外，`BatchProcessor.OnEmit` 入队前会先 `r.Clone()`。注释写得很直白：后续 processor 对 record 的修改不能和数据竞争。这也从侧面印证了 3.2 那条规则——异步持有 record 之前必须复制。

### 5.3 对应的 `Enabled` 链

`Handle` 之前还有一次 `Enabled` 判断，路径是另一条：

```
slog.Logger.Info
  -> baggageHandler.Enabled      pass through
     -> jsonHandler.Enabled      embedded, pass through
        -> otelslog.Handler.Enabled
           -> sdk logger.Enabled
              -> any(processor.Enabled) == true ?
                 -> severityProcessor.Enabled
                    severity >= floor && downstream.Enabled
```

`slog.Logger.log` 在开头就做了这个判断：

```go
if !l.Enabled(ctx, level) {
    return
}
```

所以级别被过滤掉时，`convertRecord` 根本不会执行，record 也不会分配。

这就是 `severityProcessor` 必须实现 `Enabled` 的实际收益：省掉的不只是导出，还有整条转换链。

### 5.4 五个真实的坑

**一、`slog.Info` 没有 trace_id。**

`sdk/log` 的 `newRecord` 只从 `ctx` 里取 `SpanContext`：

```go
sc := trace.SpanContextFromContext(ctx)
```

不带 `Context` 的日志方法传的是 `context.Background()`，取出来是空的 `SpanContext`，`trace_id` 和 `span_id` 都是零值。后端拿不到关联，这条日志在 trace 视图里就是孤立的。

规则很简单：所有请求路径上的日志都用 `InfoContext`、`ErrorContext`，不要用 `Info`、`Error`。

**二、初始化期间的日志会丢。**

`slog.SetDefault` 之前，默认 logger 是标准库的 text handler，输出到 stderr，和 OTel 没有关系。所以 `setupLoggerBridge` 执行前打的日志不会进入管道。

这也解释了一个细节：`initLog` 最后那行 `slog.Info("log provider initialized", ...)` 在 `SetDefault` 之后执行，所以它自己也会变成一条 OTel 记录。在 `test/filelog` 的 golden 文件里能看到它，`scope.name` 是 `global`，`code.file.path` 指向 `provider.go`。用这个来验证整条链路是否打通，比看文档快。

**三、Shutdown 的 ctx 必须有 deadline，而且要先 Flush。**

`BatchProcessor` 的最后一轮导出依赖 `Shutdown` 或 `ForceFlush` 触发。如果 ctx 已经取消，队列里剩下的记录就丢了。

`OtelProviders.Shutdown` 的做法是每个 provider 先 `ForceFlush` 再 `Shutdown`，调用方给一个独立的超时 ctx：

```go
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
slog.InfoContext(ctx, "shutting down observability")
return providers.Shutdown(ctx)
```

注意最后那条日志是在 `Shutdown` 之前打的，它会进入队列，由 `Shutdown` 内部的 `ForceFlush` 送出。如果放在 `Shutdown` 之后，就写不出去了。

**四、导出错误会回流进日志管道，但是以 INFO 级别。**

先说结论：**用 slog 的时候，不设置 `otel.SetErrorHandler` 也能看到 OTel 内部的错误。** 原因是默认的 ErrorHandler 并没有自己往 stderr 写，而是调用了标准库的 `log.Print`：

```go
func (d *ErrDelegator) Handle(err error) {
    if eh := d.delegate.Load(); eh != nil {
        (*eh).Handle(err)
        return
    }
    log.Print(err)
}
```

而 `slog.SetDefault` 会把标准库 `log` 的默认输出替换掉：

```go
func SetDefault(l *Logger) {
    defaultLogger.Store(l)
    if _, ok := l.Handler().(*defaultHandler); !ok {
        capturePC := log.Flags()&(log.Lshortfile|log.Llongfile) != 0
        log.SetOutput(&handlerWriter{l.Handler(), &logLoggerLevel, capturePC})
        log.SetFlags(0)
    }
}
```

于是 `otel.Handle` → `log.Print` → `handlerWriter` → `otelslog.Handler` → SDK，这条链路是通的。初始化顺序也刚好对得上：`setupLoggerBridge` 里先 `slog.SetDefault`，之后才可能产生导出错误。

这是 slog 分支独有的现象。换成 zap 或 logrus 的桥接，没有人去动标准库 `log` 的输出，同样一句 `log.Print` 就直接写到 stderr，不会进 OTel 管道。

这个结论有三个前提，值得确认一下：

1. **级别是 INFO。** `handlerWriter` 用的是 `logLoggerLevel`，零值是 `LevelInfo`。如果配置里的 `Level` 是 `warn` 或 `error`，`severityProcessor` 会把这条记录过滤掉，等于什么都没看到。默认配置是 `info`，所以默认情况下能看到。
2. **记录本身是 INFO，不是 ERROR。** 按 `severity=ERROR` 配的告警不会因为 collector 挂掉而响，它们会安静地躺在 INFO 里。
3. **导出失败会形成一条自引用回路。** `BatchProcessor.exportBatch` 在导出失败时调用 `otel.Handle(err)`；这条错误变成一条 INFO 日志重新入队，下个周期再导出、再失败。collector 挂掉期间，大约每个导出周期就会稳定地产生一条这样的记录，并且和业务日志抢那 2048 条的队列。

所以更准确的说法是：**用 slog 时默认就能看到这些错误，但看到的方式不一定是你想要的。** 只是排查用，保持默认就够了；如果要让它们带上 ERROR 级别、避免回流、或者写到独立的目标，再设置 `otel.SetErrorHandler`。这个工程目前用的是默认行为，不算缺陷，但值得知道它是怎么走的。

**五、双写不要靠业务代码调用两次。**

如果既要本地文件又要 OTLP，正确做法是在 handler 层扇出：写一个把 record 分发给多个子 handler 的 `slog.Handler`，或者在 zap 里用 `zap.WrapCore` 叠加。业务代码里调两次 `logger.Info` 会导致字段重复、时间戳不一致，而且没人记得住要写两次。

## 六、结语

桥接模式的本质，是**把扩展点建在别人的扩展点上**。

这个选择带来三个直接的好处：

1. **零迁移。** 已有的 `logger.Info` 一行不用改，接上桥接就进了 OTel。
2. **可并存。** 原来的文件输出、轮转、格式化都保留，OTel 只是多出来的一条支路。
3. **跨库统一。** 无论上游是 `slog`、`zap` 还是 `logrus`，进入 SDK 之后都是同一种 `log.Record`，processor 写一份就够了。

代价也很清楚：

1. **翻译是不可逆的降级。** struct 变字符串、自定义类型被压平，桥接之后无法还原。
2. **能力上限由桥接实现决定。** 宿主库有而 OTel 没有的概念（zap 的 namespace、logrus 的 formatter），桥接表达不了，就丢了。
3. **错误处理的默认路径要心里有数。** SDK 默认把错误交给标准库的 `log.Print`；用 slog 时它会被 `SetDefault` 接到 OTel 管道上，但记录是 INFO 级别，而且导出失败会回流。要精确控制，仍然需要 `SetErrorHandler`。

回到文章开头那三个问题：

- **桥接发生在哪一层？** 在日志库和 SDK 之间，由 contrib 里的独立模块承担。
- **handler 能做什么？** 在翻译之前动手，是唯一还能看到 Go 原始值的位置。
- **processor 能做什么？** 在翻译之后动手，是唯一能跨日志库统一、也是唯一能看到 trace 上下文的位置。

如果哪天要自己设计一个"接入已有生态"的 SDK，这套结构可以照搬，但有两点值得记住：

- **桥接比接管现实。** 接管要求整个生态迁移，桥接只要求实现一个接口。
- **稳定接口应该定义在翻译之后的类型上。** `Processor` 只用 `log.Record`，不认识 `slog` 也不认识 `zap`，所以四个桥接实现能共用同一条管道。反过来，如果 `Processor` 接收的是宿主类型，每加一个日志库就要改一次 SDK。

---

*代码依据 `go-bootstrap/otel`（`provider.go`、`severity_processor.go`、`json_handler.go`、`slogbaggage/baggage_handler.go`）与 `go.opentelemetry.io/contrib/bridges/otelslog@v0.20.1`、`otelzap@v0.20.1`、`otellogrus@v0.20.1`、`go.opentelemetry.io/otel/sdk/log@v0.22.0` 的源码。`otelslog` 的转换规则来自包文档与 `handler.go`、`convert.go`；`BatchProcessor` 的默认值取自 `sdk/log/batch.go` 中的常量。*
