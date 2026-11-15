---
title: 'Promtail 与 Grafana Alloy：两代日志采集 agent 的源码对比'
date: 2026-11-16T00:10:00+08:00
draft: false
tags: ["loki", "promtail", "alloy", "日志", "源码阅读"]
categories: ["可观测性"]
author: "LinPr"
description: "Promtail 已经功能冻结，Grafana 的日志采集转向 Alloy。两者都靠 Prometheus 的服务发现找到 Pod，但一个读节点上的日志文件，一个可以走 Kubernetes API。本文对照源码，拆解两者的发现、打标签、读取和断点续传是怎么做的，以及迁移时要注意什么。"
---

在 Kubernetes 里收集容器日志，最常见的组合是 Loki 加一个采集 agent。很长一段时间里，这个 agent 是 Promtail。现在 Loki 文档里 Promtail 页面顶部写着：

> Promtail is feature complete. All future feature development will occur in Grafana Alloy.

Promtail 已经不再加新功能，Grafana 把日志、指标、链路、Profile 的采集统一到了 Alloy 里。

但 Promtail 依然值得一读：它功能单一、代码量小，日志采集 agent 该有的几块（服务发现、打标签、读日志、记位置、发送）都能在里面看清楚。很多更新的 agent 身上都有它的影子。读完它再看 Alloy，就能分辨出哪些是共通的骨架，哪些是 Alloy 新加的东西。

本文先用 Promtail 把骨架讲清楚，再看 Alloy 在同一套骨架上换了什么，最后做对比并给出迁移建议。

> 源码版本：Loki `b7fe6bf6`（Promtail 位于 `clients/pkg/promtail`），Alloy `89d92b1`。

## 一、日志采集 agent 的骨架

无论 Promtail 还是 Alloy，采 Pod 日志都要回答五个问题：

| 问题 | 做什么 |
| --- | --- |
| 采哪些 | 服务发现：集群里有哪些 Pod、哪些容器 |
| 打什么标签 | relabel：把 Pod 元数据变成 Loki 的 stream label |
| 从哪读 | 日志文件路径，或者 Kubernetes API |
| 读到哪了 | 位置记录（positions），重启后能接着读 |
| 发到哪 | 批量推送到 Loki 的 `/loki/api/v1/push` |

两者在第一、二步几乎一样，因为都直接复用了 Prometheus 的服务发现和 relabel 实现；差异主要集中在第三、四步。

## 二、Promtail：发现 Pod，再去读文件

### 2.1 一份典型配置

```yaml
server:
  log_level: info
  http_listen_port: 3101

clients:
  - url: http://loki.components.svc.cluster.local:3100/loki/api/v1/push

positions:
  filename: /run/promtail/positions.yaml

scrape_configs:
  - job_name: kubernetes-pods
    pipeline_stages:
      - cri: {}
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - action: replace
        source_labels: [__meta_kubernetes_pod_node_name]
        target_label: node_name
      - action: replace
        source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - action: replace
        source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - action: replace
        source_labels: [__meta_kubernetes_pod_container_name]
        target_label: container
      # 关键一条：拼出日志文件路径
      - action: replace
        replacement: /var/log/pods/*$1/*.log
        separator: /
        source_labels:
          - __meta_kubernetes_pod_uid
          - __meta_kubernetes_pod_container_name
        target_label: __path__
      # 静态 Pod 的目录名里用的是 config hash，不是 UID
      - action: replace
        regex: true/(.*)
        replacement: /var/log/pods/*$1/*.log
        separator: /
        source_labels:
          - __meta_kubernetes_pod_annotationpresent_kubernetes_io_config_hash
          - __meta_kubernetes_pod_annotation_kubernetes_io_config_hash
          - __meta_kubernetes_pod_container_name
        target_label: __path__
```

这份配置里藏着 Promtail 的全部设计：

- `kubernetes_sd_configs` 就是 Prometheus 的服务发现配置，写法完全一样；
- `relabel_configs` 也是 Prometheus 的 relabel 语法，最终 `__path__` 这个 label 决定了读哪个文件；
- `pipeline_stages` 里的 `cri: {}` 用来解析 containerd/CRI-O 的日志行格式（`时间戳 stdout F 内容`），把真正的日志内容和时间戳拆出来；
- `positions` 记录每个文件读到了哪个字节。

kubelet 把容器日志写在 `/var/log/pods/<namespace>_<pod>_<uid>/<container>/<n>.log`。第一条 `__path__` 规则拼出的是 `/var/log/pods/*<uid>/<container>/*.log`，用通配符跳过前面的 namespace 和 pod 名，用 UID 精确定位。静态 Pod（比如 kube-apiserver）的目录名里那一段是 `kubernetes.io/config.hash` 注解的值，所以需要第二条规则。

### 2.2 服务发现：直接用 Prometheus 的 Manager

`FileTargetManager` 创建时，直接 new 了一个 Prometheus 的 `discovery.Manager`：

```go
// clients/pkg/promtail/targets/file/filetargetmanager.go
manager: discovery.NewManager(
    ctx,
    log.With(logger, "component", "discovery"),
    noopRegistry,
    noopSdMetrics,
),
```

每个 `scrape_config` 的 `kubernetes_sd_configs` 交给它。Prometheus 的 Kubernetes 发现内部就是一组 informer（`role: pod` 时 watch Pod），Pod 增删改时把它们变成 `targetgroup.Group`，每个容器一个 target，带着 `__meta_kubernetes_*` 一系列元数据 label。

Promtail 的主循环只是从 `SyncCh()` 里收这些 target group：

```go
func (tm *FileTargetManager) run(ctx context.Context) {
    defer tm.wg.Done()
    for {
        select {
        case targetGroups := <-tm.manager.SyncCh():
            for jobName, groups := range targetGroups {
                tm.syncers[jobName].sync(groups, tm.targetEventHandler)
            }
        case <-ctx.Done():
            return
        }
    }
}
```

### 2.3 从 label 到文件：`targetSyncer.sync`

每个 target 在 `sync` 里经过这几步（节选）：

```go
// clients/pkg/promtail/targets/file/filetargetmanager.go
processedLabels, keep := relabel.Process(labels.FromMap(labelMap), s.relabelConfig...)
// Drop targets if instructed to drop in relabeling.
if !keep {
    // ...
    continue
}

host, ok := labels[hostLabel]                       // "__host__"
if ok && string(host) != s.hostname {
    // ignoring target, wrong host
    continue
}

path, ok := labels[pathLabel]                       // "__path__"
if !ok {
    // no path for target
    continue
}

for k := range labels {
    if strings.HasPrefix(string(k), "__") {         // 双下划线开头的 label 用完即丢
        delete(labels, k)
    }
}
```

1. **relabel**：执行配置里的规则，`drop` 掉的 target 直接跳过；
2. **只采本机**：如果有 `__host__` label 且不等于本机 hostname，跳过。Promtail 以 DaemonSet 运行，每个节点上的实例只该读本节点的文件；
3. **取路径**：没有 `__path__` 的 target 无法采集；
4. **清理内部 label**：所有 `__` 开头的 label 在发往 Loki 前删掉，剩下的就是 stream label。

这里有个容易忽略的性能问题：Prometheus 的 Pod 发现默认 watch 全集群的 Pod，每个 Promtail 实例都会收到所有节点的 Pod，再靠 `__host__` 丢掉别的节点的。大集群里通常在 `kubernetes_sd_configs` 里加一个 `selectors`，用 `spec.nodeName=$HOSTNAME` 让 API Server 只返回本节点的 Pod。

### 2.4 读文件：glob、fsnotify 和 tail

每个 target 对应一个 `FileTarget`。它定期（`target.sync-period`，默认 10 秒）用 `doublestar.FilepathGlob` 展开 `__path__` 里的通配符，对新出现的文件开始 tail，对消失的文件停止并删除位置记录；同时用 fsnotify 监听目录，文件一创建就立刻开始读，不必等下一次同步。

真正读文件的是 `tailer`，底层是 `tail.TailFile`：

```go
// clients/pkg/promtail/targets/file/tailer.go
tail, err := tail.TailFile(path, tail.Config{
    Follow:    true,
    Poll:      true,
    ReOpen:    true,
    // ...
    Location: &tail.SeekInfo{ /* 从 positions 里记录的字节偏移开始 */ },
    PollOptions: pollOptions,
})
```

`ReOpen: true` 保证日志轮转（文件被重命名、新建同名文件）后能继续读新文件。

这就是 Promtail 读日志的本质：**打开节点上的文件，从记录的位置开始顺序读字节流。** 所以它必须以 DaemonSet 运行，并且要把宿主机的 `/var/log/pods`（以及符号链接指向的 `/var/lib/docker/containers` 或 containerd 的目录）挂进容器。

### 2.5 断点续传：positions 文件

每个文件读到的字节偏移记在 `positions.yaml` 里，默认每 10 秒落盘一次（`positions.sync-period`）。Promtail 重启后从这个偏移继续读。两次落盘之间如果进程被杀，最多会重复发送这 10 秒内已经发过的日志。这是至少一次（at-least-once）的语义：可能重复，不会丢失（前提是文件还没被轮转删掉）。

positions 文件要放在宿主机目录（hostPath）上，否则 Pod 重建后位置记录丢失，会从头重新发送整个文件。

## 三、Alloy：同一副骨架，换了一套编程模型

### 3.1 配置变成了组件图

Alloy 不再用「一个大 YAML，按功能分段」的方式，而是用自己的配置语言（原名 River，现在叫 Alloy syntax）声明一组**组件**，组件之间通过引用彼此的导出值连接起来。同样采集 Pod 日志：

```alloy
// 1. 服务发现：发现 Pod，导出 targets
discovery.kubernetes "pod" {
  role = "pod"
}

// 2. relabel：输入上一个组件的 targets，导出 output
discovery.relabel "pod_logs" {
  targets = discovery.kubernetes.pod.targets

  rule {
    source_labels = ["__meta_kubernetes_namespace"]
    target_label  = "namespace"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_name"]
    target_label  = "pod"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_container_name"]
    target_label  = "container"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_label_app_kubernetes_io_name"]
    target_label  = "app"
  }
  rule {
    source_labels = ["__meta_kubernetes_namespace", "__meta_kubernetes_pod_container_name"]
    separator     = "/"
    target_label  = "job"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_container_id"]
    regex         = "^(\\S+):\\/\\/.+$"
    target_label  = "container_runtime"
  }
}

// 3. 读日志：这里用的是 Kubernetes API
loki.source.kubernetes "pod_logs" {
  targets    = discovery.relabel.pod_logs.output
  forward_to = [loki.process.pod_logs.receiver]
}

// 4. 处理：加静态 label
loki.process "pod_logs" {
  stage.static_labels {
    values = { cluster = "<CLUSTER_NAME>" }
  }
  forward_to = [loki.write.default.receiver]
}

// 5. 发送
loki.write "default" {
  endpoint {
    url = "http://loki.components.svc.cluster.local:3100/loki/api/v1/push"
  }
}
```

和 Promtail 配置逐段对照：`kubernetes_sd_configs` 变成 `discovery.kubernetes`，`relabel_configs` 变成 `discovery.relabel`，读取、处理、发送各自成为一个组件。数据流在配置里是显式的：`discovery.kubernetes.pod.targets → discovery.relabel → loki.source.kubernetes → loki.process → loki.write`。

### 3.2 配置的解析与求值

Alloy 自己实现了一个小型语言：`syntax/` 目录下有完整的 scanner、parser、AST、类型检查和求值器（`vm`）。启动时把所有 `.alloy` 文件合并解析：

![ParseSources：多个配置文件按名字排序后逐个解析，合并成一个 Source](/images/posts/promtail-vs-alloy/img-00.png)

排序是为了保证无论文件读取顺序如何，解析结果都一样。

解析出 AST 之后，`internal/runtime/internal/controller/loader.go` 里的 `Loader` 会把每个组件块变成图上的一个节点，组件之间的引用（比如 `discovery.kubernetes.pod.targets`）变成边，构成一张有向无环图（`dag.Graph`）。求值按拓扑顺序进行：一个组件的导出值变了，只重新求值依赖它的下游组件（`EvaluateDependants`）。

这就是 Alloy 比 Promtail 复杂得多的原因。Promtail 的数据流是写死在代码里的，Alloy 则是一个通用的组件运行时，日志采集只是其中一种用法。同一个进程里还可以同时跑 `prometheus.scrape`、`otelcol.*`、`pyroscope.*` 等组件。

### 3.3 服务发现：同一份 Prometheus 代码

`discovery.kubernetes` 组件内部用的仍然是 Prometheus 的 Kubernetes 发现。下图截自 Alloy 依赖的 Prometheus 模块，`role: pod` 时启动 Pod informer：

![Prometheus Kubernetes discovery 的 RolePod 分支：为每个命名空间建一个 Pod informer](/images/posts/promtail-vs-alloy/img-01.png)

`ListFunc` 和 `WatchFunc` 都带上了 `d.selectors.pod.field` 和 `d.selectors.pod.label`，这就是配置里 `selectors` 的作用位置：把过滤条件下推到 API Server，而不是拿回全量数据再在本地丢弃。如果 `attach_metadata { node = true }`，还会额外启动 Node informer，给 target 附上节点的 label。

这一层和 Promtail 完全相同，所以两者的 `__meta_kubernetes_*` label 名字是一样的，relabel 规则基本可以照搬。

### 3.4 `loki.source.kubernetes`：用 API 读日志

拿到 target 后，`kubetail` 包为每个容器构造一个 `Target`，用 `namespace/pod` 加容器名作为 ID：

![NewTarget：从 label 里取 namespace、pod 名、容器名](/images/posts/promtail-vs-alloy/img-02.png)

然后 `tailer` 调用的是 Pod 的日志接口，而不是打开文件：

![tailer.tail：调用 Pods(ns).GetLogs(...).Stream()](/images/posts/promtail-vs-alloy/img-03.png)

```go
// internal/component/loki/source/kubernetes/kubetail/tailer.go
req := t.opts.Client.CoreV1().Pods(key.Namespace).GetLogs(key.Name, &corev1.PodLogOptions{
    Follow:     true,
    Container:  containerName,
    SinceTime:  offsetTime,
    Timestamps: true, // Should be forced to true so we can parse the original timestamp back out.
})
stream, err := req.Stream(ctx)
```

这相当于对每个容器执行一次 `kubectl logs -f --timestamps --since-time=...`。请求路径是 **Alloy → API Server → 目标节点的 kubelet → 读日志文件 → 原路流式返回**。

这条路径带来了几处和 Promtail 不同的设计：

**断点续传记的是时间，不是字节。** API 不支持按字节偏移读取，只支持 `SinceTime`。所以 positions 里存的是最后一条日志的时间戳（`UnixMicro`），重启后用 `SinceTime` 从那个时间点继续：

```go
if offset, err := t.opts.Positions.Get(positionsEnt.Path, positionsEnt.Labels); err != nil {
    level.Warn(t.log).Log("msg", "failed to load last read offset", "err", err)
} else if offset != 0 {
    lastReadTime = time.UnixMicro(offset)
}
// ...
t.opts.Positions.Put(positionsEnt.Path, positionsEnt.Labels, entryTimestamp.UnixMicro())
```

`SinceTime` 只精确到秒，重新打开流时会把最后一秒内的日志再收到一遍。读取循环用时间戳去重：

```go
entryTimestamp, entryLine := parseKubernetesLog(line)
if !entryTimestamp.After(lastReadTime) {
    continue
}
lastReadTime = entryTimestamp
```

不晚于上一行时间戳的行一律跳过，而且 `lastReadTime` 每读一行就更新一次。这避免了重连带来的重复，但也意味着：只要两行日志的时间戳完全相同（kubelet 记录到纳秒），后一行就会被丢弃，不只发生在重连的边界上。绝大多数情况下时间戳不会撞，但高频打日志的容器值得留意。文件方式按字节偏移读取，没有这个问题。

**连接会定期重建。** API 的日志流是一个长连接，可能被中间的负载均衡器或 API Server 断开，而且断开后不一定能察觉。tailer 设了两个保护：每个流最多存活 `maxTailerLifetime = 1 * time.Hour`；同时维护一个日志间隔的滑动平均，超过平均间隔 3 倍还没收到新日志，就主动关闭并重新打开流。

**可以不以 DaemonSet 运行。** 读 API 不需要宿主机目录，也不需要特权，所以 Alloy 可以作为 Deployment 运行。`loki.source.kubernetes` 支持 `clustering { enabled = true }`，多个 Alloy 实例通过一致性哈希分摊 target，每个容器只被一个实例采集。

### 3.5 Alloy 也能读文件

参考资料里常说「Alloy 通过 API 读日志，Promtail 读文件」，这只说对了一半。上面的 API 方式只是 Alloy 官方 Kubernetes 日志指南里推荐的一种，因为它不需要特权。Alloy 同样提供读文件的方式：

```alloy
local.file_match "pods" {
  path_targets = discovery.relabel.pod_logs.output   // relabel 里拼好 __path__
}

loki.source.file "pods" {
  targets    = local.file_match.pods.targets
  forward_to = [loki.process.pod_logs.receiver]
}
```

`loki.source.file` 和 Promtail 一样按字节偏移记录位置（`tailer.go` 里 `positions.Put(t.path, t.labelsStr, pos)`），需要以 DaemonSet 运行并挂载 `/var/log/pods`。从 Promtail 迁过来时，这是行为最接近的方案。

## 四、对比

| 维度 | Promtail | Alloy `loki.source.file` | Alloy `loki.source.kubernetes` |
| --- | --- | --- | --- |
| 状态 | 功能冻结，只修 bug | 持续开发 | 持续开发 |
| 配置 | YAML，按功能分段 | Alloy syntax，组件图 | 同左 |
| 服务发现 | Prometheus SD | Prometheus SD | Prometheus SD |
| 读取方式 | 节点上的日志文件 | 节点上的日志文件 | API Server → kubelet |
| 位置记录 | 字节偏移 | 字节偏移 | 时间戳（同一时间戳的多行只保留第一行） |
| 部署形态 | DaemonSet | DaemonSet | Deployment / DaemonSet，可集群分片 |
| 需要宿主机挂载/特权 | 是 | 是 | 否 |
| 对 API Server 的压力 | 只有 watch Pod | 只有 watch Pod | 所有日志流经 API Server |
| 能否采节点上的非容器日志 | 能（syslog、journal 等） | 能 | 不能 |
| 同进程采指标、链路 | 不能 | 能 | 能 |

两种 Alloy 方式怎么选：

- **日志量大、节点多：用文件方式。** API 方式下，每一行日志都要经过 API Server 和 kubelet 转发。全集群日志量一大，API Server 的带宽和 CPU 会成为瓶颈，连带影响整个控制面。
- **没有宿主机权限，或只采少量命名空间：用 API 方式。** 比如托管集群禁止特权 DaemonSet，或者只需要采某个业务的几十个 Pod。
- **节点上还有 journal、`/var/log/*.log` 等非容器日志：只能用文件方式。**

## 五、从 Promtail 迁移

1. **自动转换配置。** Alloy 自带 `alloy convert --source-format=promtail`，能把 Promtail 的 YAML 转成 Alloy 配置，转换出来的就是 `loki.source.file` 方案，行为最接近原来。
2. **positions 不能直接复用。** 字段格式不同，迁移时要么接受从头读一次（可能产生重复），要么先停掉 Promtail，再用 `tail_from_end = true` 让 Alloy 从文件末尾开始读（会丢掉切换窗口内的日志）。
3. **stream label 保持一致。** Loki 的查询、告警、看板都依赖 label。迁移时先把 relabel 规则逐条对齐，确认新旧 agent 产出的 label 集合相同，再切换；否则 Grafana 面板会在切换点断档。
4. **注意 label 基数。** 迁移时容易顺手加上 `pod`、`container_id` 这类高基数 label。Loki 按 label 组合建 stream，基数太高会拖垮写入和查询。像 `container_runtime` 这种取值很少的 label 没问题，`pod` 名则要权衡。

## 六、总结

- 日志采集 agent 的骨架是五步：发现、打标签、读取、记位置、发送。Promtail 和 Alloy 在前两步都直接复用 Prometheus 的服务发现和 relabel，所以 `__meta_kubernetes_*` label 和 relabel 写法是通用的。
- Promtail 用 `__path__` label 把 target 变成文件路径，按字节读节点上的日志文件，只能以 DaemonSet 运行。代码结构简单，适合用来理解日志采集的全过程。
- Alloy 把数据流做成了可组合的组件图：自研配置语言，解析成 AST 后按依赖关系构成有向无环图并增量求值。它的复杂度主要来自这个通用运行时，而不是日志采集本身。
- Alloy 读 Pod 日志有两种方式：`loki.source.file` 读文件，和 Promtail 一致；`loki.source.kubernetes` 走 API，免特权、可分片，但日志流量都会经过 API Server，位置记录也从字节偏移变成了时间戳。
- 新项目直接用 Alloy。日志量大时优先用文件方式，受权限限制时再考虑 API 方式。

*原笔记中 Promtail 部分的源码截图已经丢失，本文改为直接引用 Loki 仓库 `clients/pkg/promtail` 下的源码。*
