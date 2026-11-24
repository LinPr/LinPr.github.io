---
title: 'scheduler-plugins 37 [out-of-tree] TargetLoadPacking：按真实 CPU 负载把节点装到目标水位'
date: 2026-11-19T14:10:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "trimaran", "load-aware"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "Trimaran 四个插件的第一篇。先拆共用的数据链路：load-watcher 如何从 Prometheus、metrics-server、SignalFx 拉指标，Collector 的 30 秒快照，PodAssignEventHandler 怎样补偿「刚调度、指标还没反映」的 Pod；再推导 TargetLoadPacking 在目标利用率处取尖顶的打分曲线，并用算例演示补偿如何改变排序。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `TargetLoadPacking` 实现的扩展点（Score、NormalizeScore）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/37.svg" link="/images/posts/sched-plugins/callstack/37.svg" caption="TargetLoadPacking 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇[《SySched》](/posts/sched-plugin-36-sysched/)按系统调用集合给节点打分，数据来自 Security Profiles Operator 的 CRD，仍然是集群里的 API 对象。从这一篇开始的四篇讲 Trimaran，它们打分用的是**节点的实际负载**。这些数据不在 API Server 里，而是从 Prometheus、metrics-server 这类监控系统中拉取的。

Trimaran 是 scheduler-plugins 里 `pkg/trimaran/` 下四个插件的统称：`TargetLoadPacking`、`LoadVariationRiskBalancing`、`LowRiskOverCommitment`、`Peaks`。本文基于 scheduler-plugins 提交 `cfeb3aa`（依赖 Kubernetes v1.36.4）和它锁定的 `github.com/paypal/load-watcher v0.2.4`。四个插件在 Pod 调度过程中的位置相同：

- 都只实现 Score，不实现 PreFilter/Filter，因此**不会让任何 Pod 变成 Unschedulable**；`LowRiskOverCommitment` 另外实现了 PreScore。
- 都不在 in-tree 默认 Profile 里，没有 feature gate，需要运行 scheduler-plugins 自己的调度器二进制（`cmd/scheduler/main.go` 里通过 `app.WithPlugin` 注册），并在 `profiles[].plugins.score.enabled` 里显式启用。没有写 weight 时，框架会把 0 改成 1（`pkg/scheduler/framework/runtime/framework.go` 里的 `getValidScoreWeights`）。
- 都没有实现 `EnqueueExtensions`。这不是遗漏：框架只为 PreEnqueue、PreFilter、Filter、Reserve、Permit、PlacementFeasible 插件收集入队事件（`shouldHaveEnqueueExtensions`），Score 插件不会拒绝 Pod，也就不存在「被它拒绝后什么事件能让 Pod 重新入队」的问题。

本文先把四个插件共用的数据链路讲清楚，再看 `TargetLoadPacking`（下文简称 TLP）的打分曲线。

## 一、request 和真实用量之间的差距

默认的 `NodeResourcesFit` 只看 request。一台 8 核节点上已经有 Pod 申请了 6 核，调度器就认为它「用了 75%」，即便这些 Pod 实际只用了 1 核。反过来，一个没写 request 的 BestEffort Pod 在调度器眼里占 0 核，但它可能长期吃满 2 核。

Trimaran 的 KEP（`kep/61-Trimaran-real-load-aware-scheduling`）给出的动机是：应用的使用者很难估准 request，有的干脆不写；而所有 in-tree 打分插件都不看实际利用率。于是出现两种浪费：request 偏大的节点实际很空，却被当作满载；request 偏小的节点实际很忙，却继续接收 Pod。

TLP 的目标是让每个被使用的节点的实际 CPU 利用率都接近一个目标值 T（默认 40%）。低于 T 的节点优先装满，超过 T 的节点开始被「惩罚」。下面这个 Pod，在 TLP 看来会占用 1 核，而不是它的 request 500m，原因在 3.2 节：

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: web
spec:
  schedulerName: trimaran
  containers:
  - name: app
    image: nginx
    resources:
      requests:
        cpu: 500m
      limits:
        cpu: "1"
```

## 二、共用基础：指标从哪里来

打分依赖的数据不在 API Server 里。这一节先把四个插件共用的数据通路交代清楚：指标从哪来、多久刷新一次、刚调度的 Pod 怎么补，然后再看 TLP 自己的逻辑。

{{< figure src="/images/posts/sched-plugins/37-trimaran-data-path.svg" caption="Trimaran 的两条数据通路：上面是周期刷新的指标快照，下面是 informer 实时维护的「刚调度 Pod」缓存（点击查看大图）" link="/images/posts/sched-plugins/37-trimaran-data-path.svg" >}}

### 2.1 load-watcher 的数据模型

Trimaran 不直接查询 Prometheus，而是通过 PayPal 开源的 load-watcher 中转。load-watcher 返回的数据结构如下：

```go
// github.com/paypal/load-watcher@v0.2.4/pkg/watcher/watcher.go
const (
	BaseUrl         = "/watcher"
	// ...
	FifteenMinutes  = "15m"
	TenMinutes      = "10m"
	FiveMinutes     = "5m"
	CPU             = "CPU"
	Memory          = "Memory"
	// ...
	Average         = "AVG"
	Std             = "STD"
	Latest          = "Latest"
	// ...
)

type Window struct {
	Duration string `json:"duration"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
}

type Metric struct {
	Name     string  `json:"name"`             // Name of metric at the provider
	Type     string  `json:"type"`             // CPU or Memory
	Operator string  `json:"operator"`         // STD or AVE or SUM, etc.
	Rollup   string  `json:"rollup,omitempty"` // Rollup used for metric calculation
	Value    float64 `json:"value"`            // Value is expected to be in %
}

type WatcherMetrics struct {
	Timestamp int64  `json:"timestamp"`
	Window    Window `json:"window"`
	Source    string `json:"source"`
	Data      Data   `json:"data"`
}
```

一个节点对应一组 `Metric`，用 `Type`（CPU/Memory/…）和 `Operator`（AVG/STD/Latest）区分。`Value` 是**百分比**。`Window.End` 是这份数据的采集时刻，后面的滞后补偿会用到它。

`Watcher` 内部维护 15m、10m、5m 三个窗口，每个窗口一个 goroutine，每分钟拉一次，每个窗口只保留最近 5 份结果。对外的 `GetLatestWatcherMetrics("15m")` 优先返回 15 分钟窗口的最新一份，没有时再依次退到 10 分钟、5 分钟。

### 2.2 三种 metrics provider

load-watcher v0.2.4 支持三种指标源，它们给出的数据差别很大，直接影响后面几个插件的行为。

Prometheus provider 对每个窗口发 `avg_over_time` 和 `stddev_over_time` 两类查询，指标名固定：

```go
// github.com/paypal/load-watcher@v0.2.4/pkg/watcher/internal/metricsprovider/prometheus.go
	promStd                      = "stddev_over_time"
	promAvg                      = "avg_over_time"
	promCpuMetric                = "instance:node_cpu:ratio"
	promMemMetric                = "instance:node_memory_utilisation:ratio"
	// ...
	hostMetricKey                = "instance"
```

```go
// github.com/paypal/load-watcher@v0.2.4/pkg/watcher/internal/metricsprovider/prometheus.go
	if host == allHosts {
		promQuery = fmt.Sprintf("%s(%s[%s])", method, metric, rollup)
	} else {
		// ...
	}
```

也就是 `avg_over_time(instance:node_cpu:ratio[15m])` 这样的查询。结果按 `instance` 标签归属到节点，值乘以 100 变成百分比。

这里有两个前提：集群里要有 `instance:node_cpu:ratio` 这类 recording rule（来自 node-exporter 的配套规则），并且 `instance` 标签的值必须等于 Kubernetes Node 的名字，否则插件按节点名查不到数据。

另外，它一轮会查询 17 个指标（CPU、内存、网络、磁盘、Kepler/Scaphandre 能耗）× 2 种算子，三个窗口每分钟各一轮，大约每分钟 100 次查询，Trimaran 只用得到其中 CPU 和内存这几个。

KubernetesMetricsServer（默认）。读 `metrics.k8s.io` 的 `NodeMetrics`，用节点 `Capacity` 换算成百分比，`Operator` 是 `Latest`，**没有标准差**：

```go
// github.com/paypal/load-watcher@v0.2.4/pkg/watcher/internal/metricsprovider/k8s.go
		var cpuFetchedMetric watcher.Metric
		cpuFetchedMetric.Type = watcher.CPU
		cpuFetchedMetric.Operator = watcher.Latest
		// ...
		cpuFetchedMetric.Value = float64(100*host.Usage.Cpu().MilliValue()) / float64(cpuNodeCapacityMap[host.Name])
```

SignalFx provider 走 SignalFx 的 HTTP API，`addMetadata` 把 `Operator` 统一标成 `AVG`。

第三、四篇会看到，`LoadVariationRiskBalancing` 和 `LowRiskOverCommitment` 依赖标准差。只配 metrics-server 时，它们的「波动」项恒为 0，模型会退化成只看均值。

### 2.3 两种接入方式与参数

四个插件的参数里都内嵌了 `TrimaranSpec`（`Peaks` 例外，第 40 篇再说）：

```go
// apis/config/types.go
// Denote the spec of the metric provider
type MetricProviderSpec struct {
	// Types of the metric provider
	Type MetricProviderType
	// The address of the metric provider
	Address string
	// The authentication token of the metric provider
	Token string
	// Whether to enable the InsureSkipVerify options for https requests on Metric Providers.
	InsecureSkipVerify bool
}

// TrimaranSpec holds common parameters for trimaran plugins
type TrimaranSpec struct {
	// Metric Provider to use when using load watcher as a library
	MetricProvider MetricProviderSpec
	// Address of load watcher service
	WatcherAddress string
}
```

默认值逻辑：

```go
// apis/config/v1/defaults.go
// SetDefaultTrimaranSpec sets the default parameters for common Trimaran plugins
func SetDefaultTrimaranSpec(args *TrimaranSpec) {
	if args.WatcherAddress == nil && args.MetricProvider.Type == "" {
		args.MetricProvider.Type = DefaultMetricProviderType
	}
	if args.MetricProvider.Type == Prometheus && args.MetricProvider.InsecureSkipVerify == nil {
		args.MetricProvider.InsecureSkipVerify = &DefaultInsecureSkipVerify
	}
}
```

`DefaultMetricProviderType` 是 `KubernetesMetricsServer`，`DefaultInsecureSkipVerify` 是 `true`。也就是说，**用 Prometheus 时默认跳过 TLS 证书校验**。如果 Prometheus 走 HTTPS，并且你在意中间人风险，需要显式写 `insecureSkipVerify: false`。

选择哪种接入方式只看有没有 `watcherAddress`：

```go
// pkg/trimaran/collector.go
	if trimaranSpec.WatcherAddress != "" {
		client, err = loadwatcherapi.NewServiceClient(trimaranSpec.WatcherAddress)
	} else {
		opts := watcher.MetricsProviderOpts{
			Name:               string(trimaranSpec.MetricProvider.Type),
			Address:            trimaranSpec.MetricProvider.Address,
			AuthToken:          trimaranSpec.MetricProvider.Token,
			InsecureSkipVerify: trimaranSpec.MetricProvider.InsecureSkipVerify,
		}
		client, err = loadwatcherapi.NewLibraryClient(opts)
	}
```

- 服务模式：load-watcher 作为独立 Deployment 运行，插件每次刷新发一个 `GET <watcherAddress>/watcher`，超时 55 秒。
- 库模式：`NewLibraryClient` 在调度器进程里 new 一个 `Watcher` 并调用 `StartWatching()`。需要注意，`StartWatching` 不只是启动拉取 goroutine，它还会在 `:2020` 端口起一个 HTTP server，并把 `/watcher`、`/watcher/health` 注册到 `http.DefaultServeMux`：

```go
// github.com/paypal/load-watcher@v0.2.4/pkg/watcher/watcher.go
	http.HandleFunc(BaseUrl, w.handler)
	http.HandleFunc(HealthCheckUrl, w.healthCheckHandler)
	server := &http.Server{
		Addr:    ":2020",
		Handler: http.DefaultServeMux,
	}
```

所以库模式下，调度器 Pod 会多监听一个 2020 端口。另外，`net/http` 在同一个 `ServeMux` 上重复注册相同的 pattern 会 panic，而每个插件实例都有自己的 Collector（见下文）。因此在库模式下，同一个进程里出现第二个 Trimaran 插件实例（比如两个 Profile 都启用 TLP，或同时启用 TLP 和 LVRB）时，第二次 `StartWatching` 就会因为重复注册 `/watcher` 而 panic。要同时运行多个实例，应该改用服务模式。

`checkSpecs` 只在没有 `watcherAddress` 时校验 provider 类型，合法值只有上面三种，否则 `New` 返回 `invalid MetricProvider.Type` 错误，调度器启动失败。

### 2.4 Collector：每 30 秒一份快照

指标在 load-watcher 里是每分钟刷新的，插件不能每次打分都去问一遍，于是每个插件实例各持有一个 Collector，把结果缓存在本地。

```go
// pkg/trimaran/collector.go
const (
	metricsUpdateIntervalSeconds = 30
)

// Collector : get data from load watcher, encapsulating the load watcher and its operations
//
// Trimaran plugins have different, potentially conflicting, objectives. Thus, it is recommended not
// to enable them concurrently. As such, they are currently designed to each have its own Collector.
// ...
type Collector struct {
	// load watcher client
	client loadwatcherapi.Client
	// data collected by load watcher
	metrics watcher.WatcherMetrics
	// for safe access to metrics
	mu sync.RWMutex
}
```

`NewCollector` 先同步调用一次 `updateMetrics`。这次失败只打日志，不会让插件创建失败。之后起一个 ticker，每 30 秒刷新一次。`updateMetrics` 只在成功时替换快照，失败时**保留旧快照**。快照本身没有过期判断：如果 load-watcher 或 Prometheus 挂了一小时，插件会一直用一小时前的数据打分，日志里只有「Load watcher client failed」。

Score 时通过 `GetNodeMetrics` 读取：

```go
// pkg/trimaran/collector.go
func (collector *Collector) GetNodeMetrics(logger klog.Logger, nodeName string) ([]watcher.Metric, *watcher.WatcherMetrics) {
	allMetrics := collector.getAllMetrics()
	// This happens if metrics were never populated since scheduler started
	if allMetrics.Data.NodeMetricsMap == nil {
		logger.Error(nil, "Metrics not available from watcher")
		return nil, nil
	}
	// Check if node is new (no metrics yet) or metrics are unavailable due to 404 or 500
	if _, ok := allMetrics.Data.NodeMetricsMap[nodeName]; !ok {
		logger.Error(nil, "Unable to find metrics for node", "nodeName", nodeName)
		return nil, allMetrics
	}
	return allMetrics.Data.NodeMetricsMap[nodeName].Metrics, allMetrics
}
```

`getAllMetrics` 在读锁下把结构体复制一份再返回。这里是浅拷贝，`NodeMetricsMap` 仍指向同一个 map，但 `updateMetrics` 每次都是整体替换 `collector.metrics`，不会原地修改旧 map，所以读到的旧 map 不会被并发写。四个插件对 `metrics == nil` 的处理一致：直接返回 `fwk.MinNodeScore`（0）。新加入、还没被采集到的节点会在这一维得 0 分。

### 2.5 PodAssignEventHandler：补偿「刚调度、指标还没反映」

指标总是滞后的。一个 Pod 刚绑定到节点，要等容器启动、kubelet 上报、Prometheus 抓取、load-watcher 拉取、Collector 刷新这几步都完成，它的用量才会出现在快照里。在这段时间内调度器会认为节点仍然空闲，一批 Pod 可能连续落到同一个「看起来最合适」的节点上。

`PodAssignEventHandler` 用 informer 实时记录「哪些 Pod 刚被绑定到哪个节点」：

```go
// pkg/trimaran/handler.go
const (
	// This is the maximum staleness of metrics possible by load watcher
	cacheCleanupIntervalMinutes = 5
	// Time interval in seconds for each metrics agent ingestion.
	metricsAgentReportingIntervalSeconds = 60
)

// This event handler watches assigned Pod and caches them locally
type PodAssignEventHandler struct {
	// Maintains the node-name to podInfo mapping for pods successfully bound to nodes
	ScheduledPodsCache map[string][]podInfo
	sync.RWMutex
}

// Stores Timestamp and Pod spec info object
type podInfo struct {
	// This timestamp is initialised when adding it to ScheduledPodsCache after successful binding
	Timestamp time.Time
	Pod       *v1.Pod
}
```

注册方式是在调度器共享的 Pod informer 上挂一个 `FilteringResourceEventHandler`，过滤条件是 `isAssigned`（`Spec.NodeName` 非空）。这里需要先了解 client-go 的语义：当一个对象从「不满足过滤条件」变成「满足」时，`FilteringResourceEventHandler.OnUpdate` 会把它转换成对内层 handler 的 `OnAdd` 调用：

```go
// k8s.io/client-go@v0.36.4/tools/cache/controller.go
func (r FilteringResourceEventHandler) OnUpdate(oldObj, newObj interface{}) {
	newer := r.FilterFunc(newObj)
	older := r.FilterFunc(oldObj)
	switch {
	case newer && older:
		r.Handler.OnUpdate(oldObj, newObj)
	case newer && !older:
		r.Handler.OnAdd(newObj, false)
	// ...
	}
}
```

Pod 被绑定就是 `NodeName` 从空变成非空，所以实际走的是 `OnAdd → updateCache`。`PodAssignEventHandler.OnUpdate` 里「`NodeName` 变化才更新」的分支几乎不会触发，因为 `NodeName` 一旦设置就不能再修改。

```go
// pkg/trimaran/handler.go
func (p *PodAssignEventHandler) updateCache(pod *v1.Pod) {
	if pod.Spec.NodeName == "" {
		return
	}
	p.Lock()
	p.ScheduledPodsCache[pod.Spec.NodeName] = append(p.ScheduledPodsCache[pod.Spec.NodeName],
		podInfo{Timestamp: time.Now(), Pod: pod})
	p.Unlock()
}
```

时间戳取的是 informer 收到事件时的 `time.Now()`，不是 Pod 的创建时间或启动时间。由于是按时间追加，每个节点的切片天然按时间升序排列，`cleanupCache` 因此可以直接二分查找：

```go
// pkg/trimaran/handler.go
// Deletes podInfo entries that are older than metricsAgentReportingIntervalSeconds. Also deletes node entry if empty
func (p *PodAssignEventHandler) cleanupCache() {
	p.Lock()
	defer p.Unlock()
	for nodeName := range p.ScheduledPodsCache {
		cache := p.ScheduledPodsCache[nodeName]
		curTime := time.Now()
		idx := sort.Search(len(cache), func(i int) bool {
			return cache[i].Timestamp.Add(metricsAgentReportingIntervalSeconds * time.Second).After(curTime)
		})
		if idx == len(cache) {
			continue
		}
		n := copy(cache, cache[idx:])
		// ...
	}
}
```

清理每 5 分钟跑一次，删除 60 秒以前的条目。注意 `idx == len(cache)` 表示**所有条目都已过期**，此时代码却直接 `continue`，一条都没删。结果是，一个节点只要在最近 60 秒内没有新 Pod，它的旧条目就会一直留着，直到将来某次清理时它恰好有新条目，或者旧 Pod 被删除（`OnDelete` 按 UID 移除）。这些残留条目不影响打分的正确性，因为 Score 读取时还会按时间窗口再过滤一次（见 3.3 节），但缓存会比设计意图大一些。

`OnDelete` 里直接写了 `pod := obj.(*v1.Pod)`。而 `AddToHandle` 的 `FilterFunc` 会放行 `DeletedFinalStateUnknown`（informer 断线期间漏掉删除事件时产生的墓碑对象），所以墓碑会原样传进 `OnDelete`，类型断言失败会 panic。这条路径很少触发，但确实存在。

这份缓存里**不只有 Trimaran 调度器自己调度的 Pod**，所有已绑定的 Pod 都会进来，包括默认调度器调度的 Pod 和 DaemonSet Pod。这是合理的：无论谁调度的，它们的负载同样还没体现在指标里。

{{< figure src="/images/posts/sched-plugins/37-lag-window.svg" caption="以 Window.End 为基准：End 之后或 End 前 60 秒内绑定的 Pod 被视为指标里还没有它们，按预测用量补加" >}}

数据通路到这里就完整了：一份每 30 秒刷新的快照，加上一份实时维护的补偿缓存。下面看 TLP 怎么用它们打分。

## 三、TargetLoadPacking 源码

### 3.1 参数与 New

TLP 的参数在公共的 `TrimaranSpec` 之外还有三个字段。

```go
// apis/config/types.go
// TargetLoadPackingArgs holds arguments used to configure TargetLoadPacking plugin.
type TargetLoadPackingArgs struct {
	metav1.TypeMeta

	// Common parameters for trimaran plugins
	TrimaranSpec
	// Default requests to use for best effort QoS
	DefaultRequests v1.ResourceList
	// Default requests multiplier for busrtable QoS
	DefaultRequestsMultiplier string
	// Node target CPU Utilization for bin packing
	TargetUtilization int64
}
```

| 参数 | 默认值 | 作用 |
|---|---|---|
| `targetUtilization` | 40（≤0 时也回落到 40） | 目标 CPU 利用率 T，单位 % |
| `defaultRequests` | `cpu: 1000m` | 容器既没有 limit 也没有 request 时的预测用量 |
| `defaultRequestsMultiplier` | `"1.5"`（字符串） | 只有 request 时，预测用量 = request × 倍数 |

`defaultRequestsMultiplier` 被定义成字符串，是因为 component config 的 API 规范不鼓励使用浮点字段。`New` 里用 `strconv.ParseFloat` 解析，解析失败时返回错误，调度器启动失败。这三个参数没有在 `apis/config/validation` 里做校验。

`New` 的关键部分：

```go
// pkg/trimaran/targetloadpacking/targetloadpacking.go
var (
	requestsMilliCores           = cfgv1.DefaultRequestsMilliCores
	hostTargetUtilizationPercent = cfgv1.DefaultTargetUtilizationPercent
	requestsMultiplier           float64
)

func New(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	// ...
	collector, err := trimaran.NewCollector(logger, &args.TrimaranSpec)
	if err != nil {
		return nil, err
	}

	hostTargetUtilizationPercent = args.TargetUtilization
	requestsMilliCores = args.DefaultRequests.Cpu().MilliValue()
	requestsMultiplier, err = strconv.ParseFloat(args.DefaultRequestsMultiplier, 64)
	// ...
	podAssignEventHandler := trimaran.New()
	podAssignEventHandler.AddToHandle(handle)
	// ...
}
```

三个参数被写进了**包级变量**，而不是插件结构体。调度器为每个 Profile 各创建一个插件实例，如果两个 Profile 都启用了 TLP 且参数不同，后创建的实例会覆盖前一个的 T 和倍数，两个 Profile 实际上会用同一套参数。（这种配置在库模式下本来就会因为 2.3 节的端口注册冲突而起不来，服务模式下则会遇到这个参数覆盖问题。）

### 3.2 预测一个容器会用多少 CPU

打分的第一步是估计一个 Pod 会吃掉多少 CPU。这个估计只依赖容器的 request 和 limit，规则是三层兜底：

```go
// pkg/trimaran/targetloadpacking/targetloadpacking.go
// PredictUtilisation predict utilization for a container based on its requests/limits
func PredictUtilisation(container *v1.Container) int64 {
	if _, ok := container.Resources.Limits[v1.ResourceCPU]; ok {
		return container.Resources.Limits.Cpu().MilliValue()
	} else if _, ok := container.Resources.Requests[v1.ResourceCPU]; ok {
		return int64(math.Round(float64(container.Resources.Requests.Cpu().MilliValue()) * requestsMultiplier))
	}
	return requestsMilliCores
}
```

三层兜底按优先级是：limit、request × 1.5、1000m。用 limit 而不用 request 的原因在 KEP 里有说明：limit 是更强的上界，按上界预测，利用率更不容易冲过目标值。代价是 limit 远大于 request 的 Pod 会被视为「很重」。

Pod 级的预测只累加 `Spec.Containers`，再加上 `Spec.Overhead` 里的 CPU。**init 容器不参与预测**，Pod 级资源（`spec.resources`）也不会被读取。

### 3.3 Score

有了单个 Pod 的预测用量，Score 把节点当前指标、补偿缓存和这个预测值合起来，算出节点的分数。整个过程分为四步。

```go
// pkg/trimaran/targetloadpacking/targetloadpacking.go
func (pl *TargetLoadPacking) Score(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	// ...
	score := fwk.MinNodeScore
	nodeName := nodeInfo.Node().Name

	// get node metrics
	metrics, allMetrics := pl.collector.GetNodeMetrics(logger, nodeName)
	if metrics == nil {
		klog.InfoS("Failed to get metrics for node; using minimum score", "nodeName", nodeName)
		// Avoid the node by scoring minimum
		return score, nil
		// TODO(aqadeer): If this happens for a long time, fall back to allocation based packing. This could mean maintaining failure state across cycles if scheduler doesn't provide this state
	}

	var curPodCPUUsage int64
	for _, container := range pod.Spec.Containers {
		curPodCPUUsage += PredictUtilisation(&container)
	}
	// ...
	if pod.Spec.Overhead != nil {
		curPodCPUUsage += pod.Spec.Overhead.Cpu().MilliValue()
	}

	var nodeCPUUtilPercent float64
	var cpuMetricFound bool
	for _, metric := range metrics {
		if metric.Type == watcher.CPU {
			if metric.Operator == watcher.Average || metric.Operator == watcher.Latest {
				nodeCPUUtilPercent = metric.Value
				cpuMetricFound = true
			}
		}
	}

	if !cpuMetricFound {
		logger.Error(nil, "Cpu metric not found in node metrics", "nodeName", nodeName, "nodeMetrics", metrics)
		return score, nil
	}
	nodeCPUCapMillis := float64(nodeInfo.Node().Status.Capacity.Cpu().MilliValue())
	nodeCPUUtilMillis := (nodeCPUUtilPercent / 100) * nodeCPUCapMillis
```

第一步，取节点当前 CPU 利用率。接受 `AVG` 或 `Latest`，循环里没有 `break`，所以多个匹配时**最后一个生效**。Prometheus 下 CPU 只有一个 AVG，不会出现歧义。然后用节点 **`Capacity`**（不是 `Allocatable`）把百分比换算成毫核。

第二步，计算缓存里「指标还没反映」的 Pod 用量：

```go
// pkg/trimaran/targetloadpacking/targetloadpacking.go
	var missingCPUUtilMillis int64 = 0
	pl.eventHandler.RLock()
	for _, info := range pl.eventHandler.ScheduledPodsCache[nodeName] {
		// If the time stamp of the scheduled pod is outside fetched metrics window, or it is within metrics reporting interval seconds, we predict util.
		// Note that the second condition doesn't guarantee metrics for that pod are not reported yet as the 0 <= t <= 2*metricsAgentReportingIntervalSeconds
		// t = metricsAgentReportingIntervalSeconds is taken as average case and it doesn't hurt us much if we are
		// counting metrics twice in case actual t is less than metricsAgentReportingIntervalSeconds
		if info.Timestamp.Unix() > allMetrics.Window.End || info.Timestamp.Unix() <= allMetrics.Window.End &&
			(allMetrics.Window.End-info.Timestamp.Unix()) < metricsAgentReportingIntervalSeconds {
			for _, container := range info.Pod.Spec.Containers {
				missingCPUUtilMillis += PredictUtilisation(&container)
			}
			missingCPUUtilMillis += info.Pod.Spec.Overhead.Cpu().MilliValue()
			// ...
		}
	}
	pl.eventHandler.RUnlock()
```

判断条件用时间戳 t 和快照的 `Window.End` 比较：t 晚于 End，或者 t 在 End 之前 60 秒以内，就认为指标里还没有这个 Pod，按 3.2 节的规则补加它的预测用量。注释里说得很直白：指标代理的上报周期假设为 60 秒，一个 Pod 真正体现在指标里可能需要 0 到 120 秒，这里取平均值 60 秒，偶尔重复计算一次「问题不大」。

`info.Pod.Spec.Overhead.Cpu()` 在 `Overhead` 为 nil 时也是安全的，因为 `ResourceList` 是 map 类型，对 nil map 调用 `Cpu()` 会返回零值 Quantity。

第三、四步，算出预测利用率并套用分段函数：

```go
// pkg/trimaran/targetloadpacking/targetloadpacking.go
	var predictedCPUUsage float64
	if nodeCPUCapMillis != 0 {
		predictedCPUUsage = 100 * (nodeCPUUtilMillis + float64(curPodCPUUsage) + float64(missingCPUUtilMillis)) / nodeCPUCapMillis
	}
	if predictedCPUUsage > float64(hostTargetUtilizationPercent) {
		if predictedCPUUsage > 100 {
			return score, fwk.NewStatus(fwk.Success, "")
		}
		penalisedScore := int64(math.Round(float64(hostTargetUtilizationPercent) * (100 - predictedCPUUsage) / (100 - float64(hostTargetUtilizationPercent))))
		// ...
		return penalisedScore, fwk.NewStatus(fwk.Success, "")
	}

	score = int64(math.Round((100-float64(hostTargetUtilizationPercent))*
		predictedCPUUsage/float64(hostTargetUtilizationPercent) + float64(hostTargetUtilizationPercent)))
	// ...
	return score, fwk.NewStatus(fwk.Success, "")
}
```

所有路径都返回 `Success`（或 nil），即使拿不到指标也不会让调度失败，最多给 0 分。`NormalizeScore` 是空实现，原始分数就是最终分数，再乘以权重。

### 3.4 打分曲线的推导

记节点当前利用率为 A（%），Pod 预测用量为 B，滞后补偿为 M，容量为 Cap，目标为 T：

```text
U = (A × Cap / 100 + B + M) / Cap × 100

            ⎧ (100 − T) × U / T + T        0 ≤ U ≤ T
score(U) =  ⎨ T × (100 − U) / (100 − T)    T < U ≤ 100
            ⎩ 0                            U > 100
```

（实现里对两段都做了 `math.Round`。）

这两条直线是怎么定出来的，看端点就清楚：

- 上升段过 `(0, T)` 和 `(T, 100)`，斜率是 `(100−T)/T`。空节点得 T 分而不是 0 分。KEP 的解释是：被看好的节点要拿到足够高的分数，才不会被其它打分插件轻易抵消。
- 下降段过 `(T, T)` 和 `(100, 0)`，斜率是 `−T/(100−T)`。
- 在 U = T 处，左极限是 100，右极限是 T，**曲线在这里断开，落差为 100 − T**。T 越小，落差越大。

这个形状意味着：所有节点都低于 T 时，**最接近 T 的节点得分最高**，Pod 往已经有一定负载的节点上堆，这就是装箱（best fit）。一旦某个节点会被推过 T，它的分数立即跌到 T 以下，而且越热分越低，这时在过热节点之间是分散（least fit）。下降段的最高分 T 恰好等于上升段的最低分 T（空节点的分数），所以「把节点推过目标」不会比「放到空节点」更好。这一点保证了装箱不会越过目标值。

{{< figure src="/images/posts/sched-plugins/37-tlp-score-curve.svg" caption="TLP 的分段打分曲线（T=40 实线、T=70 虚线），以及 3.5 节算例中 A、B、C 和补偿后的 B' 所在位置（点击查看大图）" link="/images/posts/sched-plugins/37-tlp-score-curve.svg" >}}

### 3.5 算例

三个 8 核节点（Capacity 8000m），T = 40。待调度 Pod 就是第一节那个 `limit: 1`，所以 B = 1000m，占容量的 12.5%。

| 节点 | 当前利用率 A | U = A + 12.5 | 落在哪一段 | 分数 |
|---|---|---|---|---|
| A | 10% | 22.5 | 上升段 | 60 × 22.5 / 40 + 40 = 73.75 → 74 |
| B | 25% | 37.5 | 上升段 | 60 × 37.5 / 40 + 40 = 96.25 → 96 |
| C | 45% | 57.5 | 下降段 | 40 × 42.5 / 60 = 28.33 → 28 |

**B 最接近目标，胜出**。C 已经超过目标，被惩罚。

再看补偿的作用。假设 20 秒前，另一个 Pod 刚被绑定到 B，它只有 `requests.cpu: 500m`、没有 limit。它绑定的时刻在当前快照的 `Window.End` 之后（或在 End 前 60 秒以内），所以会被计入 `missingCPUUtilMillis`：

- 预测用量 = round(500 × 1.5) = 750m，占 9.375%；
- U = 25 + 12.5 + 9.375 = 46.875，越过了 T；
- 分数 = 40 × (100 − 46.875) / 60 = 35.42 → 35。

B 从 96 分跌到 35 分，A 以 74 分胜出。如果没有补偿，接下来几十秒内的 Pod 都会挤到 B 上，直到新的指标反映出真实负载，B 可能已经远超目标。

## 四、边界情况与坑

**assumed Pod 在补偿缓存里看不到。** 调度器在 Reserve 阶段把 Pod「假定」到节点上（assume），之后异步执行 PreBind/Bind。补偿缓存依赖 informer 收到「NodeName 已设置」的更新，也就是要等 Bind 写入 API Server、watch 事件回来之后，条目才会出现。

在这段时间里（通常几十到几百毫秒，有卷绑定的 PreBind 时会更长），下一个 Pod 的打分看不到上一个 Pod。`NodeResourcesFit` 没有这个问题，因为它读的 `NodeInfo` 快照包含 assumed Pod。高并发提交一批 Pod 时，TLP 仍会出现小范围扎堆。

**Prometheus 的 15 分钟均值和 60 秒补偿不匹配。** 补偿逻辑假设一个 Pod 在绑定 60 秒后就「完整地」体现在指标里。这个假设对 metrics-server 的 `Latest` 值大体成立。

但 Prometheus 模式下取的是 `avg_over_time(...[15m])`：一个两分钟前启动的 Pod，在 15 分钟均值里只占约 2/15 的权重，补偿却已经不再计入它。所以 Prometheus 模式下，新 Pod 的负载有十几分钟处于「低估」状态，批量扩容时节点会被装过目标值。目标值设得比期望值低 10 个百分点（KEP 的建议），一部分原因就在这里。

**调度器重启后会短暂高估。** informer 启动时的全量 List 会对每个已绑定的 Pod 触发 `OnAdd`，时间戳都是启动时刻。

在启动后第一份快照的 `Window.End` 前后约 60 秒内，集群里**所有**正在运行的 Pod 都会被当成「刚调度」，按 limit 补加一遍。这段时间内，几乎每个节点的 U 都会被高估甚至超过 100，TLP 给出的分数接近 0，排序基本由其它插件决定。

**指标长时间停更时补偿会失效。** Collector 失败时保留旧快照，`Window.End` 停在过去某个时刻。按 3.3 节的条件，End 之后绑定的 Pod 都应该被补加，但 `cleanupCache` 判断过期用的是**当前时间**，不是 `Window.End`。只要清理时该节点还有 60 秒内的新条目，更早的条目就会被一并删除。这些 Pod 既没有出现在旧指标里，也不再被补偿。

**新节点和采集失败的节点得 0 分。** 代码里的 TODO 写着「长时间拿不到指标就退回按 allocation 装箱」，但目前没有实现。新扩容的节点在被 load-watcher 采集到之前（库模式下最多约 1.5 分钟：Watcher 每分钟拉一次，Collector 每 30 秒刷新一次），在 TLP 这一维上一直垫底。

**容量口径。** TLP 用 `Capacity` 做分母；metrics-server provider 也用 `Capacity` 换算百分比，两者一致。第 38、39 篇的插件则用 `Allocatable` 乘以同一个百分比来还原绝对用量，口径略有不同。

**和其它打分插件的冲突。** TLP 的 README 和 `manifests/trimaran/scheduler-config.yaml` 里的示例配置还是 `v1beta3` 写法，禁用的 `NodeResourcesLeastAllocated` 在当前版本里已经不是独立插件，而是 `NodeResourcesFit` 的 `scoringStrategy`。默认的 `LeastAllocated` 策略按 request「越空越好」，与 TLP 的装箱方向相反。

按当前 API，应在 Profile 里禁用 `NodeResourcesBalancedAllocation`，并把 `NodeResourcesFit` 的打分策略改成 `MostAllocated`，或者给它很低的权重：

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: trimaran
  plugins:
    score:
      disabled:
      - name: NodeResourcesBalancedAllocation
      enabled:
      - name: TargetLoadPacking
        weight: 3
  pluginConfig:
  - name: NodeResourcesFit
    args:
      scoringStrategy:
        type: MostAllocated
  - name: TargetLoadPacking
    args:
      targetUtilization: 60
      defaultRequestsMultiplier: "1.5"
      watcherAddress: http://load-watcher.kube-system.svc:2020
```

## 五、发散

前面几节讲的都是「怎么把节点装到某个水位」，这一节讨论几个设计上的取舍：为什么不做硬约束、和集群级的伸缩组件怎么配合、以及目标值到底该怎么定。

**为什么是 Score 而不是 Filter。** KEP 的 Non-Goals 第一条就是「不把『利用率不超过 X%』做成 Filter」。真实负载是随时间波动的统计量，用它做硬约束，在指标抖动或缺失时会让 Pod 大面积 Pending；做成 Score 则最多是放得不够好。

代价是目标值只是一个**软目标**：如果所有节点都已经超过 T，TLP 只会挑最不热的那个，不会拒绝调度。真正的硬约束仍然要靠 request 和 `NodeResourcesFit`。

**与 Cluster Autoscaler 配合。** TLP 把负载往少数节点上集中，空出来的节点在实际负载上很空。但 Cluster Autoscaler 判断能否缩容，看的是节点上 Pod 的 **request** 之和占可分配量的比例（`--scale-down-utilization-threshold`，默认 0.5），而且节点上的 Pod 要能被重新安置到别处。

TLP 只影响新 Pod 落在哪里，不会迁移老 Pod。要让节点真正被回收，需要配合 descheduler 之类的组件把低负载节点上的 Pod 驱逐，让它们重新调度到 TLP 偏好的节点上。

**和 Opportunistic Batching 的关系。** Kubernetes 1.35 起，`OpportunisticBatching` 进入 Beta 并默认开启：如果 Profile 里所有 PreFilter/Filter/PreScore/Score 插件都实现了 `SignPlugin`，连续到来的「签名相同」的 Pod 可以复用上一次的过滤和打分结果。

四个 Trimaran 插件都没有实现 `SignPlugin`，`computeBatchablePlugins` 会因此关闭整个 Profile 的签名优化，日志里能看到「Disabling signatures for profile because plugins do not support it」。这其实是正确的：Trimaran 的分数依赖外部快照和随时间变化的补偿缓存，同样的 Pod 隔几秒打分，结果就可能不同，不适合复用。

**目标值怎么选。** KEP 建议「想达到 X%，就配 X − 10」。结合上面的分析，这 10 个百分点要覆盖三部分：assumed Pod 不可见导致的扎堆、Prometheus 均值窗口带来的低估、负载本身的波动。如果负载波动大，单靠调小 T 并不精确。下一个插件 `LoadVariationRiskBalancing` 把标准差直接放进了模型。

## 六、小结

- Trimaran 的数据链路是「指标源 → load-watcher → Collector 每 30 秒一份快照」，再加上「informer → `PodAssignEventHandler` 实时记录刚绑定的 Pod」。前者提供历史负载，后者弥补滞后。四个插件里只有 TLP 真正读取了后者。
- 库模式会在调度器进程里起 `:2020` 的 HTTP server，同一进程内的多个 Trimaran 实例会因重复注册 `/watcher` 而 panic；Prometheus 下默认跳过 TLS 校验。
- TLP 按 limit → request×1.5 → 1000m 预测 Pod 用量，用 Capacity 换算预测利用率 U，分段打分：U ≤ T 时线性上升到 100，超过 T 立刻跌到 T 以下再线性下降，超过 100 得 0。效果是先装箱到 T，再在热节点之间分散。
- 补偿条件以 `Window.End` 为基准，覆盖 End 之后和 End 前 60 秒内绑定的 Pod。它覆盖不了 assumed Pod，也覆盖不了 Prometheus 长窗口均值的稀释效应。
- 拿不到指标的节点得 0 分，所有路径都返回 Success，TLP 永远不会让 Pod 调度失败。

TLP 只看 CPU 均值。下一篇[《LoadVariationRiskBalancing》](/posts/sched-plugin-38-load-variation-risk-balancing/)把 CPU 和内存的均值、标准差都放进「风险」模型，目标从「装到某个水位」变成「让各节点风险持平」。
