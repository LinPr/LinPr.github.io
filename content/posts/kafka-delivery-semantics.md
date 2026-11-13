---
title: 'Kafka 投递语义：至多一次、至少一次与恰好一次是怎么组合出来的'
date: 2026-11-13T00:20:00+08:00
draft: false
tags: ["kafka", "消息队列", "分布式", "exactly-once"]
categories: ["Kafka"]
author: "LinPr"
description: "at-most-once、at-least-once、exactly-once 不是 topic 的开关，而是确认、重试、幂等、位点提交与事务五个机制组合出来的结果。本文结合 franz-go 客户端源码，拆解每个机制到底消除了哪一类丢失或重复，又留下了什么。"
---

同一个 topic，同一套生产者配置，挂了两个消费者组。一个组在 rebalance 之后少处理了一批消息，另一个组在重启之后把一批消息处理了两遍。topic 没变，broker 没变，结果却一个丢、一个重。

原因在于，Kafka 里所说的投递语义：**at-most-once（至多一次）**、**at-least-once（至少一次）**、**exactly-once（恰好一次）**，都不是 topic 的属性，也不是某个能打开的开关。一条消息从生产到被处理，中间有几个决策点，每个点上的选择决定了这里会丢、会重，还是两者都能避免。端到端的结果，就是这几个选择叠加的结果。

所以本文不按「三种语义各怎么配」来讲，而是反过来，沿着消息的路径回答三个问题：

1. **消息在哪里会丢？** 每个丢失点由哪个机制堵住？
2. **重复从哪里来？** 每个机制能消除哪一类重复，消除不了哪一类？
3. **机制本身的边界在哪？** 幂等和事务各自在什么情况下会失效？

前两个问题决定了三种语义各需要什么配置，第三个问题决定了这些配置在生产上能不能兑现。

> 文中客户端侧的实现细节引用自 Go 客户端 [franz-go](https://github.com/twmb/franz-go)（commit `8363ba9`）的源码，主要在 `pkg/kgo/` 下。协议层面的行为各客户端一致，但默认值不同，涉及默认值时会分别说明。

## 一、三种语义与四个决策点

### 1.1 定义

| 语义 | 保证 | 允许丢 | 允许重 |
| --- | --- | --- | --- |
| 至多一次（at-most-once） | 最多处理一次 | 是 | 否 |
| 至少一次（at-least-once） | 至少处理一次 | 否\* | 是 |
| 恰好一次（exactly-once） | 效果上处理一次 | 否\* | 否\*\* |

\* 前提是副本数、`acks`、`min.insync.replicas` 配置正确，且没有发生超出副本冗余的故障。
\*\* 只在 Kafka 内部成立：读 Kafka、写 Kafka。

exactly-once 常被简称为 EOS（exactly-once semantics），更准确的说法是「有效一次」（effectively once）。底层的网络重试、重复写入依然会发生，只是通过幂等和事务被识别出来、挡在了可见范围之外，而不是物理上只发生了一次。

### 1.2 四个决策点

{{< figure src="/images/posts/kafka-delivery-semantics/decision-points.svg" caption="一条消息的四个决策点：生产确认、副本选举、提交时机、事务边界" >}}

1. **生产端确认**：要不要等 broker 确认，等几个副本确认（`acks`）；
2. **生产端重试**：失败后要不要重发，重发会不会被去重（`retries` + 幂等）；
3. **消费端提交时机**：先处理还是先提交位点；
4. **事务边界**：输出和位点提交是不是一起生效。

前两个决定消息能不能可靠地**进入** Kafka，后两个决定消息被**处理**几次。

### 1.3 端到端取最弱的一环

| 生产端 | 消费端 | 端到端语义 |
| --- | --- | --- |
| 不确认或不重试 | 任意 | at-most-once：可能丢 |
| `acks=all` + 重试 | 处理后再提交 | at-least-once：可能重 |
| `acks=all` + 重试 | 处理后再提交 + 幂等消费 | 业务上有效一次 |
| 事务生产 | `read_committed` + 位点随事务提交 | exactly-once（Kafka 内） |

第三行是工程上最常用的组合：Kafka 层面依然可能重复，但业务结果只生效一次。它比事务便宜，也适用于输出不在 Kafka 里的场景。

---

## 二、生产端：丢失与重复从哪来

### 2.1 `acks`：确认等到哪一步

| `acks` | broker 何时回复 | 丢失窗口 |
| --- | --- | --- |
| `0` | 不回复 | 发送失败、broker 拒绝写入，客户端都不知道 |
| `1` | leader 写入本地日志后 | leader 崩溃时，follower 还没复制到的数据 |
| `all`（`-1`） | ISR 中所有副本都写入后 | ISR 只剩 leader 一个时，退化为 `acks=1` |

**`acks=0` 时，客户端连响应都不读。** franz-go 在请求写到网络上之后立刻把它当作完成：

```go
// pkg/kgo/broker.go
isNoResp = r.acks == 0
// ...
if isNoResp {
    pr.promise(noResp, nil)
```

随后用基准位点 0 结束这一批记录：

```go
// pkg/kgo/sink.go，handleReqRespNoack
s.cl.finishBatch(batch.recBatch, req.producerID, req.producerEpoch, 0, nil)
```

所以 `acks=0` 时，回调拿到的 `err` 永远是 nil（除非连网络写都失败了），`Record.Offset` 也不是真实的日志位点，而是批内序号 0、1、2……broker 后来拒绝了这批数据，客户端也不会知道。

**`acks=1` 的丢失不需要 unclean 选举。** leader 写入本地后立刻回复，此时 follower 还没来得及拉取。leader 崩溃，ISR 中的另一个副本当选新 leader，它的日志里没有这条消息；旧 leader 恢复后以 follower 身份加入，会按新 leader 的日志截断自己。这条已经被确认过的消息就消失了，整个过程都是正常的 leader 切换。

**`acks=all` 要配合 `min.insync.replicas`。** `all` 的含义是「当前 ISR 里的所有副本」，而 ISR 是动态的。三副本的 topic，如果两个 follower 都因为落后被移出 ISR，`acks=all` 就只等 leader 自己，和 `acks=1` 没有区别。`min.insync.replicas=2` 规定 ISR 少于 2 个时拒绝写入，生产者收到 `NOT_ENOUGH_REPLICAS`。写入失败是可见的，比静默降级好。

### 2.2 重试是重复的来源

`acks=all` 堵住了丢失，但带来了新问题。考虑这个时序：

```text
producer                       broker (leader)
   |                                 |
   |----- produce(batch) ----------->|  写入日志，复制到 ISR
   |                                 |
   |      x<------ response ---------|  响应在网络上丢失
   |                                 |
   |  (timeout)                      |
   |----- produce(batch) ----------->|  重试：没有幂等时，再写一次
   |                                 |
```

broker 已经写入，确认却丢了。生产者只能重发，同一条消息就在日志里出现了两次。**只要允许重试，就可能重复；不允许重试，就可能丢失。** 这是生产端的基本矛盾，幂等生产者就是为了打破它。

### 2.3 幂等生产者：给每个批次编号

幂等的思路很直接：每个生产者有一个身份，每个分区上的每个批次有一个递增序号，broker 记住每个生产者在每个分区上最近写入的序号，看到重复的就丢弃。

**身份：PID 与 epoch。** 生产者第一次发送数据前，向 broker 请求 `InitProducerID`，得到一个 producer ID 和 epoch。franz-go 是懒加载的，第一次真正要发送时才请求：

```go
// pkg/kgo/sink.go
// NOTE: we create the req AFTER getting our producer ID!
// If a prior response caused errReloadProducerID, then calling
// producerID() sets needSeqReset, and creating the request resets
// sequence numbers. ...
```

**序号：每个分区一个计数器。** 序号存在每个分区的发送缓冲 `recBuf` 上：

```go
// pkg/kgo/sink.go
// seq is used for the seq in each record batch. It is incremented when
// produce requests are made and can be reset on errors to batch0Seq.
seq int32
// batch0Seq is the seq of the batch at batchDrainIdx 0. If we reset
// the drain index, we reset seq with this number. If we successfully
// finish batch 0, we bump this.
batch0Seq int32
```

两个计数器分工明确：

- `seq` 在构造请求时增加，增量是这一批的记录条数；
- `batch0Seq` 只在批次被确认成功后才前进。

**重试时复用原来的序号。** 需要重发时，`seq` 被重置回 `batch0Seq`：

```go
// pkg/kgo/sink.go，resetBatchDrainIdx
recBuf.seq = recBuf.batch0Seq
recBuf.batchDrainIdx = 0
```

重发的批次带着和第一次完全相同的序号。broker 发现这个序号已经写过，就不会再写一次。

**broker 回复「重复」时，客户端视为成功。**

```go
// pkg/kgo/sink.go
case err == kerr.DuplicateSequenceNumber: // ignorable, but we should not get
    s.cl.cfg.logger.Log(LogLevelInfo, "received unexpected duplicate sequence number, ignoring and treating batch as successful", ...)
    err = nil
    fallthrough
```

回到 2.2 的时序：第二次 produce 会被 broker 识别为重复，返回成功，日志里只有一份。

### 2.4 幂等如何保证分区内顺序

允许多个请求同时在途，是提高吞吐的常用手段，但它会打乱顺序：请求 1 失败重试、请求 2 已经成功，重试后的请求 1 就排到了请求 2 后面。

有了序号，broker 可以检查连续性：收到的序号必须紧接着上一次写入的序号，否则返回 `OUT_OF_ORDER_SEQUENCE_NUMBER`，拒绝写入。客户端按顺序重发，日志里的顺序就和发送顺序一致。Java 客户端据此规定，开启幂等时 `max.in.flight.requests.per.connection` 不能超过 5。

franz-go 在这里更保守，它的在途请求数从 1 开始，收到第一个成功响应后才放宽到 4：

```go
// pkg/kgo/sink.go
// Starting on version 4, Kafka allowed five inflight requests while
// maintaining idempotency. Before, only one was allowed.
// ...
if idempotent && version >= 4 { s.inflightSem.Store(make(chan struct{}, 4)) }
```

原因写在另一段注释里：

```go
// ... Kafka will accept the first produce request for a pid/epoch
// with *any* sequence number. Say we sent two requests inflight. The
// first request Kafka replies to with NOT_LEADER_FOR_PARTITION, the
// second, the broker finished setting up and accepts. The broker now
// has the second request but not the first, we will retry both
// requests and receive OOOSN, and the broker has logs out of order.
// By only allowing more than one inflight if we have seen an ok
// response, we largely eliminate risk of this problem.
```

broker 对一个新 PID/epoch 的**第一个**请求不检查序号连续性。如果一开始就并发发送，第一个请求失败、第二个成功，第二个就会被当成起点写进去，顺序已经乱了。先串行发一个，确认 broker 已经建立了这个生产者的状态，再放开并发，就能避开这个窗口。

### 2.5 幂等的四条边界

**1. 只在一个生产者会话内有效。** 进程重启后会拿到新的 PID（或者新的 epoch），broker 无法把新会话的消息和旧会话的消息对应起来。重启前发出、结果未知的消息，重启后由业务重发，就会重复。

**2. 只去重「生产者重试」造成的重复。** 业务代码自己调用了两次 `Produce`，上游系统重发，消费端重复处理，幂等生产者都管不到。

**3. 结果未知时，客户端只能等。** 有些错误让客户端无法判断数据是否已经写入：

```go
// pkg/kgo/sink.go
// If we receive a response, but the error code is REQUEST_TIMED_OUT or
// NOT_ENOUGH_REPLICAS_AFTER_APPEND, we actually do not know the state
// of producing this on the broker. ... Once we do not know the state, we need to block cancelation
// until we definitively produce or definitively fail.
unsureIfProduced bool
```

这种状态下，franz-go 会绕过重试次数上限，一直重试到得出确定结果为止。因为此时放弃，就不知道该告诉业务「成功」还是「失败」；如果告诉业务失败、业务又重发，就会产生幂等也挡不住的重复。它提供了一个选项 `AllowIdempotentProduceCancellation` 允许强行取消，文档写得很直白：

> When a record is in-flight, the client cannot tell "the broker never received it" from "the broker wrote it but the reply was lost". ... any cancelled record that the application re-produces may land on the broker twice.

**4. 检测到序号断档，说明可能已经丢了数据。** `OUT_OF_ORDER_SEQUENCE_NUMBER` 意味着 broker 上缺了一段序号：

```go
// pkg/kgo/sink.go
// OOOSN always means data loss 1.0+ and is ambiguous prior.
// We assume the worst and only continue if requested.
```

franz-go 的处理分三种情况：

- 如果是因为日志被 retention 截断了开头，不是真的丢失，直接递增 epoch、重置序号，继续发送；
- 如果是事务生产者，或者配置了 `StopProducerOnDataLossDetected`，让这个分区的记录全部失败，把问题交给业务；
- 普通幂等生产者（默认）：触发 `ProducerOnDataLossDetected` 回调，然后递增 epoch、序号从 0 开始继续发送。

第三种情况意味着：**默认配置下，幂等生产者检测到可能的数据丢失后会继续工作。** 如果业务不能容忍这种情况，要么注册回调告警，要么打开 `StopProducerOnDataLossDetected`。

### 2.6 不同客户端的默认值

| 客户端 | `acks` 默认 | 幂等默认 | 重试 |
| --- | --- | --- | --- |
| Java（3.0 起） | `all` | 开启 | 受 `delivery.timeout.ms`（默认 120 秒）约束 |
| franz-go | `all` | 开启 | 次数和总时长默认都不限 |
| Sarama | `WaitForLocal`（即 `1`） | 关闭 | 默认 3 次 |

franz-go 的配置文档里有一处写着默认是 `LeaderAck`，与实际代码 `acks: AllISRAcks()` 不符，以代码为准。看默认值时，以自己所用客户端的代码为准，不要假设和 Java 客户端一致。

---

## 三、消费端：位点提交决定丢还是重

### 3.1 位点是「下一条要读的位置」

消费者对 Kafka 的确认方式是提交位点（offset）。提交的值是**下一条要读的消息的位点**，不是最后处理完的那条：

```go
// pkg/kgo/consumer_group.go
// Offset is the offset of a record. If committing, this should be one AFTER a record's offset.
// Clients start consuming at the offset that is committed.
```

所以处理完 offset=104 的消息后，要提交 105。手动提交时写成 104，重启后会重复处理 104；写成 106，会跳过 105。

位点是单调推进的：提交了 105，就等于声明 105 之前的消息全部处理完了。没有「第 103 条失败、其余成功」这种表达。

### 3.2 提交时机

| 提交时机 | 崩溃发生在 | 重启后 | 语义 |
| --- | --- | --- | --- |
| 处理前提交 | 提交后、处理完成前 | 从已提交位点继续，这批消息丢失 | 至多一次 |
| 处理后提交 | 处理完成后、提交前 | 从上次的位点重读，这批消息重复 | 至少一次 |
| 与输出在同一事务中 | 任意位置 | 要么都生效，要么都没有 | 恰好一次 |

### 3.3 自动提交到底提交了什么

「自动提交」最常见的误解是：处理完会自动提交。实际上客户端根本不知道你有没有处理完，它只知道你**拉取**了哪些消息。

franz-go 的实现能很清楚地说明这一点。每次 `PollFetches` 返回记录时，它把「最后一条 + 1」记为 dirty；**下一次** poll 开始时，再把 dirty 升级为可提交的 head；后台每 5 秒提交一次 head：

```go
// pkg/kgo/consumer_group.go
// Under DEFAULT autocommit (neither disabled nor greedy) we set only
// dirty here, and undirtyUncommitted promotes dirty->head at the START
// of the next poll. This one-poll lag is INTENTIONAL and is what makes
// default autocommit at-least-once: defaultRevoke commits head, so a
// revoke between poll N and N+1 commits none of poll N's records and
// the new owner re-reads them. Committing dirty at revoke instead would
// mark records consumed while the app may still be mid-processing them -
// a loss window. ...
```

「下一次 poll」被当作「上一批已经处理完」的隐式信号。只要处理是在两次 poll 之间同步完成的，这个假设成立，自动提交就是 at-least-once。Java 客户端的自动提交同样发生在 `poll()` 内部，提交的也是上一轮已经返回的记录的位点，逻辑一致。

这个假设在三种情况下会被打破：

- **异步处理**：poll 拿到消息后丢给线程池就返回，接着再次 poll。此时消息还没处理，位点已经可以提交了，等价于「先提交后处理」；
- **贪婪模式**：franz-go 的 `GreedyAutoCommit` 直接提交所有已拉取的位点，文档写明 `may result in message loss if your application crashes`；
- **处理很慢**：Java 客户端里，单次处理超过 `max.poll.interval.ms`，消费者被移出组，分区被分给别人，重复由此产生。

### 3.4 rebalance：重复最主要的来源

分区随时可能在 rebalance 中被分配给另一个消费者。交接时，旧消费者已经处理、但还没提交的消息，会被新消费者重新处理。

franz-go 的默认处理是：分区被回收（revoke）前，同步提交一次 head：

```go
// pkg/kgo/consumer_group.go
// defaultRevoke commits the last fetched offsets and waits for the commit to
// finish. This is the default onRevoked function which, when combined with the
// default autocommit, ensures we never miss committing everything.
```

但分区被判定为丢失（lost，例如会话超时）时，什么也不提交，因为此时这个消费者已经不再是分区的合法持有者了。

关闭自动提交、自己管理位点时，`DisableAutoCommit` 的文档给了一个具体的例子：消费者 A 提交到 4，处理到 30，这时 rebalance，分区给了 B，B 从 4 开始读，5 到 30 被处理两遍。文档的结论是：

> a simple group consumer is not EOS / transactional, only at-least-once.

还有一种更隐蔽的情况：

> If you poll records, and then a rebalance happens, and then you commit, you may be committing to partitions you no longer own. This will result in duplicates. In the worst case, you could rewind commits that a different member has already made.

rebalance 之后拿着旧的位点去提交，可能把新持有者已经推进的位点**往回拨**。这就是为什么手动提交时，要在分区回收回调里完成提交，而不是在处理循环的任意位置提交。

### 3.5 批量处理时怎么提交

位点只能表达「这之前都完成了」，所以批量处理时要保证提交的位点之前没有空洞：

```text
for each batch:                     # 同一分区的一批记录
    for each record in batch:       # 按位点顺序
        handle(record)              # 任一条失败，整批不提交
    commit(batch.lastOffset + 1)    # 全部成功才推进
```

多分区时位点按分区各自推进，一个分区的失败不影响其他分区。

消费端多线程处理时，同一分区内的消息可能乱序完成。这时要么按 key 把消息分发给固定的线程，保证同一分区单线程；要么维护一个「已完成位点」的水位，只提交连续完成的那一段。franz-go 的 `AutoCommitMarks` + `MarkCommitRecords` 就是为这种场景准备的：只有被业务显式标记过的记录才会被自动提交。

### 3.6 重复来源汇总

| 来源 | 触发条件 | 幂等生产者能消除吗 | 事务能消除吗 |
| --- | --- | --- | --- |
| 生产端网络重试 | broker 已写入，确认丢失 | 能 | 能 |
| 生产者重启 | 结果未知的消息被业务重发 | 不能 | 能（`transactional.id` 跨会话） |
| 消费端崩溃 | 处理后、提交前崩溃 | 不能 | 能（输出在 Kafka 内时） |
| rebalance | 未提交的位点交给新消费者 | 不能 | 能（输出在 Kafka 内时） |
| 业务或上游重发 | 应用逻辑 | 不能 | 不能 |

最后一行是所有 Kafka 机制都覆盖不到的。这也说明了为什么幂等消费是绕不开的：无论 Kafka 层面配置得多严格，业务层面的重复都要靠业务自己处理。

---

## 四、at-most-once：省掉确认和重试

### 4.1 什么时候可以丢

判断标准不是「这个数据重不重要」，而是**丢了能不能补**：

- 指标、日志、埋点：采样和丢失本来就在设计之内；
- 缓存刷新、失效通知：丢了下一轮还会来，最坏是数据多旧一会儿；
- 在线状态、心跳：过时即无效，重发没有意义。

订单、支付、审计日志不行，不是因为它们更重要，而是因为丢了以后没有任何机制能补回来。

### 4.2 配置

| 位置 | 配置 | 取值 | 说明 |
| --- | --- | --- | --- |
| Producer | `acks` | `0` 或 `1` | `0` 完全不知道失败，`1` 至少知道 leader 写没写 |
| Producer | `retries` | `0` | 不重试，就不会重复 |
| Producer | `enable.idempotence` | `false` | 幂等要求 `acks=all`，与 `acks=0/1` 冲突 |
| Consumer | 提交时机 | 处理前提交 | 或自动提交 + 异步处理 |

### 4.3 注意事项

- **幂等与 `acks=0/1` 互斥。** franz-go 会在配置校验阶段直接报错 `idempotency requires acks=all`。Java 3.0 以后幂等默认开启，显式配置 `acks=1` 时要同时关掉幂等，否则同样会报配置错误。
- **至少把发送失败暴露成指标。** `acks=0` 下客户端无法感知失败，但可以监控发送缓冲占用、超时数、连接错误。否则「丢了多少」永远是个未知数。

---

## 五、at-least-once + 幂等消费：默认答案

### 5.1 为什么它是最常见的正确选择

面对丢和重，大多数业务的判断一致：**丢了无法补救，重了可以消除。** 重复的订单消息可以在消费端被唯一键挡住；丢失的订单消息只能靠人工对账发现。

所以把复杂度放在「让重复无害」上，而不是放在「保证绝不重复」上。前者只需要在业务写入时加一个约束，后者需要全链路事务。

### 5.2 配置

| 位置 | 配置 | 取值 | 作用 |
| --- | --- | --- | --- |
| Broker/Topic | `replication.factor` | `3` | 三份副本 |
| Broker/Topic | `min.insync.replicas` | `2` | ISR 不足 2 时拒绝写入，防止 `acks=all` 退化 |
| Broker/Topic | `unclean.leader.election.enable` | `false` | 不允许落后副本当选 leader |
| Producer | `acks` | `all` | 写入全部 ISR 才算成功 |
| Producer | `enable.idempotence` | `true` | 消除重试产生的重复 |
| Producer | `delivery.timeout.ms` | 按业务 SLA | 重试的总时长上限 |
| Consumer | 提交方式 | 处理完成后手动提交 | 或同步处理 + 默认自动提交 |
| Consumer | `max.poll.interval.ms` | 大于最长处理时间 | 避免处理中被踢出消费组 |

### 5.3 幂等消费的几种做法

| 做法 | 适用 | 要点 |
| --- | --- | --- |
| 唯一键 + upsert | 有业务主键的实体写入 | 依赖唯一索引，不能「先查后写」 |
| 去重表 | 需要精确去重 | 去重记录与业务写入在同一个本地事务 |
| 状态机 | 订单、工单等状态流转 | 只允许合法的状态迁移，重复消息自然被跳过 |
| 覆盖写 | 余额快照、配置下发 | 写最终值，不写增量 |
| 位点入库 | 写数据库的消费者 | 业务数据和位点在同一个数据库事务里提交 |

几个常见错误：

- **先查再写不是幂等。** 两个消费者同时查到「不存在」，然后都插入。只有唯一索引或 `INSERT ... ON CONFLICT DO NOTHING` 能兜底；
- **去重记录和业务写入分开提交。** 业务写成功、去重记录没写成功，下次重放还会再写一次；
- **去重窗口有限。** 按时间清理去重表后，很久以前的消息被重放，仍然会重复。这个边界需要业务明确接受；
- **处理失败仍然提交位点。** 在 `defer` 里提交，或者把错误吞掉后继续，都等价于 at-most-once；
- **不可重试的错误无限重试。** 一条格式错误的消息会卡住整个分区。应该设置重试上限，超过后转入死信队列。

### 5.4 最后一种做法值得单独说

「位点入库」把消费位点和业务数据写进同一个数据库事务：

```text
BEGIN;
  INSERT INTO orders ...;
  UPDATE consumer_offsets SET next_offset = 105 WHERE topic = 'orders' AND partition = 3;
COMMIT;
```

消费者启动时从数据库读位点，而不是从 Kafka 读。业务数据和位点要么一起生效，要么都没有。这实际上是在 Kafka 之外实现了恰好一次，而且不依赖 Kafka 事务。代价是每次 rebalance 拿到新分区时，都要从数据库加载位点并手动 seek。

---

## 六、exactly-once：Kafka 事务的实现与边界

### 6.1 它保证的是什么

> **消费、处理、生产三个动作在 Kafka 内部原子生效：输出写入和消费位点提交，要么一起成功，要么一起失败。**

它覆盖的路径是 **Kafka → 处理 → Kafka**。写数据库、调 HTTP 接口这类外部副作用不在事务里。事务回滚时，已经写进数据库的数据不会被撤销。

### 6.2 事务的执行过程

一次事务涉及三类请求，franz-go 的实现可以看清每一步什么时候发生：

**1. `BeginTransaction` 不发送任何请求。** 它只是在本地设置「正在事务中」的标记：

```go
// pkg/kgo/txn.go（节选）
func (cl *Client) BeginTransaction() error {
    // ...
    if cl.producer.inTxn {
        return errors.New("invalid attempt to begin a transaction while already in a transaction")
    }
    // ...
    cl.producer.inTxn = true
    cl.producer.producedInTxn.Store(false)
    // ...
    cl.producer.producingTxn.Store(true) // allow produces for txns now
    return nil
}
```

**2. 第一次写某个分区时，登记这个分区。** 在发送 Produce 请求之前，先向事务协调器发送 `AddPartitionsToTxn`，告诉它「这个事务涉及这个分区」。每个分区每个事务只登记一次：

```go
// pkg/kgo/sink.go，txnReqBuilder.add
if t.txnID == nil || t.pv12 || rb.addedToTxn.Swap(true) {
    return
}
```

协调器需要知道事务涉及哪些分区，提交时才能给每个分区写结束标记。

**3. 把消费位点并入事务。** 发送 `AddOffsetsToTxn` 登记消费者组，再用 `TxnOffsetCommit` 提交位点。位点此时只是「待定」状态，事务提交前对其他消费者不可见。请求中带上了消费者组的 generation 和 member ID：

```go
// pkg/kgo/txn.go
// Since this function is a gigantic footgun if not done properly, we hide this
// and only allow transaction sessions to commit.
```

带 generation 是 KIP-447 引入的：如果这个消费者在事务期间已经被 rebalance 踢出了，它的 generation 是过期的，位点提交会被拒绝，避免旧消费者覆盖新消费者的进度。

**4. `EndTxn` 提交或中止。** 协调器收到 `EndTxn` 后，先把事务状态写入内部 topic `__transaction_state`，再给每个登记过的分区写一条 **控制记录**（COMMIT 或 ABORT 标记）。

KIP-890（Kafka 4.0 的 transaction v2）简化了这个流程：Produce v12 起不再需要单独的 `AddPartitionsToTxn`，由 broker 在收到写入时隐式登记；每次 `EndTxn` 都会递增 epoch，进一步防止上一个事务的迟到请求混入下一个事务。franz-go 两套协议都实现了，按 broker 能力自动选择。

### 6.3 读端：`read_committed` 怎么过滤

事务中止后，数据并不会从日志里删除，只是多了一条 ABORT 标记。过滤工作分在 broker 和客户端两边完成。

**broker 端用 LSO 挡住未决的数据。** LSO（last stable offset）是「这个位置之前的所有事务都已经有结果」的位点。`read_committed` 的消费者最多只能读到 LSO，还在进行中的事务的数据对它不可见。

**客户端过滤已中止的数据。** broker 在 Fetch 响应里附带这一段内被中止的事务列表 `AbortedTransactions`，客户端据此跳过对应批次：

```go
// pkg/kgo/source.go，shouldAbortBatch
if len(a) == 0 || b.Attributes&0b0001_0000 == 0 {   // 不是事务批次
    return false
}
pidAborts := a[b.ProducerID]
// ...
if b.FirstOffset < pidAborts[0] { return false }
return true
```

批次属性的第 4 位标识「事务批次」，第 5 位标识「控制记录」。遇到 ABORT 控制记录时，客户端把对应生产者的中止条目弹出，后面这个生产者的数据恢复正常读取。控制记录本身默认不返回给业务。

这带来两个直接后果：

- **长事务会挡住下游。** 一个事务不结束，LSO 就停在它的起点，这个分区上之后所有的数据，包括其他生产者写入的、已经提交的，都读不到；
- **中止的数据占用空间。** 被中止的记录照样占位点和磁盘，直到 retention 清理。频繁中止会浪费存储和带宽。

消费者的隔离级别默认是 `read_uncommitted`，必须显式设置为 `read_committed`，否则事务的原子可见性没有意义。

### 6.4 rebalance 期间的事务

消费 → 处理 → 生产的事务，最棘手的是事务进行到一半时发生了 rebalance。franz-go 的 `GroupTransactSession` 注释把两种选择讲得很清楚：

> When ETLing in a group in a transaction, if a rebalance happens before the transaction is ended, you either (a) must block the rebalance from finishing until you are done producing, and then commit before unblocking, or (b) allow the rebalance to happen, but abort any work you did. ... Instead, for safety, a GroupTransactSession favors (b). If a rebalance occurs at any time before ending a transaction with a commit, this will abort the transaction.

选项 (a) 的问题是：处理慢的话，会超过 rebalance 超时，消费者被踢出组；被踢出后再提交，在旧版本 Kafka 上甚至可能「错误地成功」，导致重复。

所以只要事务期间发生了分区回收，就中止事务，并把消费位置回退到已提交的位点，重新处理：

```go
// pkg/kgo/txn.go
tryCommit := !s.failed() && commitErr == nil && !hasAbortableCommitErr && (okHeartbeat || canCommitDespiteRebalance)
```

提交前还会主动发一次心跳，确认自己仍然在组里。心跳成功，说明在 rebalance 超时之前，还有时间安全地完成 `EndTxn`。

### 6.5 `transactional.id` 与 fencing

幂等生产者的 PID 在重启后会变，无法跨会话去重。事务生产者用一个固定的 `transactional.id` 解决这个问题：

- 同一个 `transactional.id` 重启后调用 `InitProducerID`，协调器返回同一个 PID、递增后的 epoch，并中止旧实例未完成的事务；
- 旧实例如果还活着（比如只是网络分区），它之后的请求带着旧 epoch，会被拒绝，返回 `PRODUCER_FENCED` 或 `INVALID_PRODUCER_EPOCH`。

这就是 fencing：保证同一个逻辑生产者在任一时刻只有一个实例能提交事务，防止「僵尸实例」提交过期的数据。滚动发布时尤其关键。

franz-go 把 `PRODUCER_FENCED` 视为不可恢复的错误：

```go
// pkg/kgo/txn.go
// PRODUCER_FENCED from coordinator paths is not recoverable.
```

客户端被 fence 之后，唯一正确的处理是关闭它，创建新的客户端。

`transactional.id` 要满足两个条件：**稳定**，同一个处理单元重启后使用相同的值；**唯一**，同时运行的实例不能共用。常见做法是用「应用名 + 实例序号」，或者在 StatefulSet 中用 Pod 名。

### 6.6 提交结果未知时只能中止

`EndTxn` 发出后，如果连接断开或响应超时，客户端不知道事务到底提交了没有。franz-go 此时把状态标记为「未确认」，之后只允许用中止来收尾：

```go
// pkg/kgo/txn.go
if unconfirmed {
    if commit {
        return errors.New("cannot retry a commit whose outcome is unconfirmed: the transaction may already be aborted; retry with TryAbort")
    }
    // ...
}
```

这里的「中止」并不是再发一次 `EndTxn(abort)`，而是用当前的 PID 和 epoch 重新执行一次 `InitProducerID`。协调器收到后会递增 epoch，并中止这个 `transactional.id` 下仍在进行中的事务。源码注释的说法是：

> That reload IS the abort.

两种结果都能被正确收敛：

- 原来的 `EndTxn` 其实没生效：事务还在进行中，被这次 epoch 递增中止；
- 原来的 `EndTxn` 已经生效：事务早已结束，这次重新初始化不会产生副作用。

不允许重试提交的原因是：上面那次重新初始化可能刚刚把事务中止了，这时再告诉调用方「提交成功」就是错误的。调用方只能按失败处理，回退位点重新消费。

### 6.7 配置与注意事项

| 位置 | 配置 | 取值 |
| --- | --- | --- |
| Producer | `transactional.id` | 稳定且实例唯一 |
| Producer | `enable.idempotence` | `true`（事务的硬性前提） |
| Producer | `acks` | `all`（事务的硬性前提） |
| Producer | `transaction.timeout.ms` | 小于 broker 的 `transaction.max.timeout.ms`（默认 15 分钟） |
| Consumer | `isolation.level` | `read_committed` |
| Consumer | 自动提交 | 关闭，位点随事务提交 |

事务超时时间的默认值，Java 客户端是 60 秒，franz-go 是 40 秒。franz-go 的文档特别注明：超时从事务中第一条记录被发送时开始计算，而不是从 `BeginTransaction` 开始。处理时间超过超时，协调器会主动中止事务，表现为事务反复失败。

其他注意事项：

- **性能成本是实打实的。** 每个事务都有协调器往返，提交时要给每个分区写控制标记。事务越小，开销占比越高；事务越大，下游等待 LSO 的时间越长。通常按批次或时间窗口提交；
- **Kafka Streams 不需要手写。** 设置 `processing.guarantee=exactly_once_v2` 即可，它在内部完成了上面所有步骤；
- **输出到外部系统时退回 at-least-once。** 写数据库用 5.4 节的位点入库；调 HTTP 用接口幂等；需要「写库 + 发消息」原子时，用 outbox 模式：业务数据和待发消息写进同一个本地事务，再由独立进程投递到 Kafka。

### 6.8 适用场景

- **流式聚合**：窗口统计、状态计算，重复会让指标翻倍；
- **Kafka 到 Kafka 的 ETL**：清洗、转换、路由到下游 topic；
- **事件溯源、状态复制**：事件流要求不重不漏。

反过来，最终写入数据库或调用外部接口的链路，Kafka 事务帮不上忙。

---

## 七、怎么选，怎么验证

### 7.1 选择顺序

按顺序回答四个问题：

1. **丢了会怎样？** 业务无感知、或下一轮能自动补上 → at-most-once。否则继续；
2. **重复了会怎样？** 能用唯一键、状态机、覆盖写消掉 → at-least-once + 幂等消费；
3. **输出是否只在 Kafka 内？** 是 → 可以考虑事务；否 → 位点入库或外部幂等；
4. **能否接受事务带来的延迟和吞吐损失？** 不能 → at-least-once + 幂等消费仍然是最优解。

### 7.2 场景对照

| 场景 | 推荐 | 关键配置 |
| --- | --- | --- |
| 日志、指标、埋点 | 至多一次 | `acks=0/1`，关重试与幂等 |
| 缓存失效通知 | 至多一次 | 短超时，接受丢失 |
| 订单、支付、库存 | 至少一次 + 幂等消费 | `acks=all`，`min.insync.replicas=2`，手动提交，唯一键 |
| Kafka 到数据库 | 至少一次 + 位点入库 | 业务数据与位点同事务 |
| Kafka 到 Kafka 的 ETL | 恰好一次 | `transactional.id`，`read_committed` |
| 实时聚合 | 恰好一次 | Kafka Streams `exactly_once_v2` |

### 7.3 验证

语义是配置出来的，也要测出来：

- **故障注入**：在处理完成后、提交前 `kill -9` 消费者，确认重启后消息重投且只生效一次；在 broker 写入后、响应返回前断网，确认幂等生产者没有写入重复数据；在事务提交前触发 rebalance，确认事务被中止、数据被重新处理；
- **对账**：统计生产者发送数、消费端处理数、去重命中数，三者对得上；
- **监控**：消费延迟、提交失败数、重复命中率、事务中止率、死信队列长度。幂等生产者的数据丢失回调（franz-go 的 `ProducerOnDataLossDetected`）要接入告警。

---

## 八、结语

回到开头的三个问题，每个机制消除的东西都是有限的：

- **`acks=all` + `min.insync.replicas`** 消除了 broker 侧的丢失，但引入了重试；
- **幂等生产者** 消除了单个会话内重试造成的重复，但跨会话、跨分区、业务层的重复它管不到；
- **处理后提交** 消除了消费端的丢失，但崩溃和 rebalance 会带来重复；
- **事务** 把输出和位点绑在一起，消除了 Kafka 内的所有重复，但只到 Kafka 的边界为止。

最后剩下的那一类重复，业务层的重发、外部系统的副作用，没有任何 Kafka 配置能覆盖。所以工程上最常见的组合不是 exactly-once，而是 **at-least-once + 幂等消费**：生产端用 `acks=all` 和幂等生产者保证不丢，消费端用唯一键、状态机或位点入库保证重复只生效一次。

Kafka 提供的是一组机制，语义是这些机制组合出来的结果。先想清楚丢了会怎样、重了会怎样，再决定打开哪几个开关。
