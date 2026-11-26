---
title: 'scheduler-plugins 07 [in-tree] NodePorts：一张端口表和一个可以抢占的失败'
date: 2026-11-19T09:10:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "NodePorts", "hostPort", "源码分析"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NodePorts 检查 Pod 的 hostPort 是否和节点上已有 Pod 冲突。本文拆解它从哪些容器收集端口、HostPortInfo 如何用 (hostIP, protocol, port) 三元组判冲突以及 0.0.0.0 的特殊语义、为什么冲突返回可被抢占解决的 Unschedulable，以及只在占用相同端口的 Pod 被删除时才唤醒的 QueueingHint。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `NodePorts` 实现的扩展点（PreFilter、Filter）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/07.svg" link="/images/posts/sched-plugins/callstack/07.svg" caption="NodePorts 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-06-node-affinity/)的 `NodeAffinity` 看的是节点的标签，失败了谁也改变不了。`NodePorts` 看的是节点上已经被占用的 `hostPort`，失败的原因是「别的 Pod 先占了」，这就有了商量的余地。

本文基于 Kubernetes master 提交 `aabdf5a2`。`NodePorts` 在默认 Profile 的 `MultiPoint` 里排第七，只实现 PreFilter 和 Filter，不参与打分，没有插件参数，默认启用。它和 `NodeAffinity` 一样只在 `InPlacePodVerticalScalingSchedulerPreemption`（1.37 Alpha，默认关）打开时，对 resize 被推迟的 Pod 直接跳过。整个插件不到 200 行，但它是默认 Filter 链里少数返回 `Unschedulable` 而不是 `UnschedulableAndUnresolvable` 的插件，抢占能不能帮上忙就取决于这个区别。

## 一、什么样的 Pod 会被它拦下

先划清范围：`NodePorts` 只关心那些会真正占住节点主机端口的 Pod。

`hostPort` 把节点上的一个端口映射到容器端口。同一个节点上，同一个 IP、同一个协议、同一个端口只能给一个 Pod：

```yaml {linenos=inline}
apiVersion: v1
kind: Pod
metadata:
  name: ingress-a
spec:
  containers:
  - name: nginx
    image: nginx:1.27
    ports:
    - containerPort: 80
      hostPort: 80          # hostIP 不写即 0.0.0.0，protocol 不写即 TCP
```

第二个同样声明 `hostPort: 80` 的 Pod 在这个节点上会被拒绝，`kubectl describe pod` 里看到的是：

```text {linenos=inline}
0/3 nodes are available: 3 node(s) didn't have free ports for the requested pod ports.
```

还有一类 Pod 没写 `hostPort` 也会被它检查：`hostNetwork: true` 的 Pod。API 默认值逻辑会把 `containerPort` 原样填进 `hostPort`（第 2–4 行）：

```go {linenos=inline}
// pkg/apis/core/v1/defaults.go
	if obj.Spec.HostNetwork {
		defaultHostNetworkPorts(&obj.Spec.Containers)
		defaultHostNetworkPorts(&obj.Spec.InitContainers)
	}
```

所以 `hostNetwork` 的 Pod 只要声明了 `ports`，就会被当作占用了同号的主机端口。这正是想要的效果：hostNetwork 容器直接在主机网络命名空间里监听，端口冲突是真实存在的。

落到清单上，下面这个 Pod 一个 `hostPort` 都没写，`NodePorts` 照样会检查它：

```yaml {linenos=inline}
# Pod：hostNetwork 模式，只写了 containerPort
apiVersion: v1
kind: Pod
metadata:
  name: node-exporter-x
spec:
  hostNetwork: true
  containers:
  - name: exporter
    image: quay.io/prometheus/node-exporter:v1.8.2
    ports:
    - containerPort: 9100     # 默认值逻辑补成 hostPort: 9100
```

这个清单在调度器眼里分三步变成一个端口占用：

1. 第 7 行的 `hostNetwork: true` 让 `defaultHostNetworkPorts` 生效；
2. 第 12 行只写了 `containerPort`，写入 etcd 之前 `hostPort` 被填成 9100，`protocol` 被填成 `TCP`；
3. `GetHostPorts` 收集到的是 `0.0.0.0:9100/TCP`，和第一个例子里显式写 `hostPort` 没有区别。

反过来，hostNetwork Pod 不能把 `hostPort` 写成和 `containerPort` 不同的值，`validatePodHostNetworkDeps`（`pkg/apis/core/validation/validation.go`）会以 ``must match `containerPort` when `hostNetwork` is true`` 拒绝。

## 二、从 Pod 里收集端口

确认了谁来查，接下来看它查什么。端口清单由 `GetHostPorts` 从 Pod spec 里提取：

```go {linenos=inline}
// pkg/scheduler/util/utils.go
func GetHostPorts(pod *v1.Pod) []v1.ContainerPort {
	var ports []v1.ContainerPort
	if pod == nil {
		return ports
	}

	hostPort := func(p v1.ContainerPort) bool {
		return p.HostPort > 0
	}

	for _, c := range pod.Spec.InitContainers {
		// Only consider initContainers that will be running the entire
		// duration of the Pod.
		if c.RestartPolicy == nil || *c.RestartPolicy != v1.ContainerRestartPolicyAlways {
			continue
		}
		for _, p := range c.Ports {
			if !hostPort(p) {
				continue
			}
			ports = append(ports, p)
		}
	}
	for _, c := range pod.Spec.Containers {
		for _, p := range c.Ports {
			// ...
			ports = append(ports, p)
		}
	}
	return ports
}
```

清单里有两类容器，取舍不同：

1. init 容器只收集 `restartPolicy: Always` 的那种（第 12–24 行），也就是 sidecar。普通 init 容器跑完就退出，调度器认为它不长期占用端口。
2. 应用容器只保留 `hostPort > 0` 的声明（第 25–30 行）。

**所以清单上只有「会长期占端口」的容器。**

这个函数有两个调用方：

1. 插件的 PreFilter：收集待调度 Pod 想要的端口；
2. `NodeInfo.update`：Pod 加入或离开节点时维护节点的已用端口。

两边用同一个函数，保证「想要」和「已占」的口径一致。

下面这个 Pod 三个容器都声明了端口，`GetHostPorts` 只收集其中一个：

```yaml {linenos=inline}
# Pod：sidecar 的 hostPort 会被收集，普通 init 容器的不会
apiVersion: v1
kind: Pod
metadata:
  name: proxy-with-sidecar
spec:
  initContainers:
  - name: fetch-config            # 普通 init 容器，跑完退出
    image: busybox:1.36
    command: ["sh", "-c", "echo fetch"]
    ports:
    - containerPort: 8080
      hostPort: 8080
  - name: envoy                   # sidecar
    image: envoyproxy/envoy:v1.31.0
    restartPolicy: Always
    ports:
    - containerPort: 15090
      hostPort: 15090
  containers:
  - name: app
    image: nginx:1.27
    ports:
    - containerPort: 80           # 没有 hostPort
```

三个容器对应 `GetHostPorts` 的三条路径：

1. `fetch-config` 没有 `restartPolicy`，在 `GetHostPorts` 的 init 容器循环里直接 `continue`，它第 13 行的 8080 不进清单；
2. `envoy` 第 16 行是 `restartPolicy: Always`，第 19 行的 15090 被收集；
3. `app` 第 24 行只有 `containerPort`，`hostPort` 为 0，不满足 `hostPort > 0`。

最终清单只有 `0.0.0.0:15090/TCP`，节点上别的 Pod 占着 8080 也不影响这个 Pod 的调度。

端口从哪里收集已经清楚，剩下的是各扩展点分别怎么用它。

## 三、源码按扩展点拆

下面按一次调度里的调用顺序拆开：工厂造出实例，PreFilter 准备状态，Filter 拿状态去查节点表，最后失败码决定抢占能不能介入。

### 3.1 结构体与工厂

插件结构体只有一个字段，用来存一个 feature gate 的开关：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeports/node_ports.go
type NodePorts struct {
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

func New(_ context.Context, _ runtime.Object, _ fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &NodePorts{enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption}, nil
}
```

没有参数，连 `Handle` 都不保存。**节点已用端口不从 informer 或 lister 里查，而是直接从 Filter 传进来的 `NodeInfo` 上读。**

### 3.2 PreFilter：没有端口就 Skip

PreFilter 判断这个 Pod 有没有端口，有就写进 CycleState，没有就返回 `Skip`。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeports/node_ports.go
type preFilterState []v1.ContainerPort

// Clone the prefilter state.
func (s preFilterState) Clone() fwk.StateData {
	// The state is not impacted by adding/removing existing pods, hence we don't need to make a deep copy.
	return s
}

func (pl *NodePorts) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	s := util.GetHostPorts(pod)
	// Skip if a pod has no ports.
	if len(s) == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	cycleState.Write(preFilterStateKey, preFilterState(s))
	return nil, nil
}
```

写进 CycleState 的这一项（第 14–19 行）：

1. key 是 `"PreFilterNodePorts"`；
2. 值就是待调度 Pod 的端口切片。

绝大多数 Pod 没有 `hostPort`，在这里返回 `Skip`（第 16–18 行），整个调度周期里 `NodePorts` 的 Filter 一次都不会被调用。

`Clone` 返回自身，`PreFilterExtensions()` 返回 `nil`。**这两个选择背后是同一个设计：CycleState 里只放待调度 Pod 自己的端口，节点的已用端口不在状态里**。抢占模拟时，`DefaultPreemption` 在克隆出来的 `NodeInfo` 上删除受害者：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
			if pi.GetPod().Spec.NodeName == mainNodeName {
				if err := nodeInfo.RemovePod(logger, pi.GetPod()); err != nil {
					return err
				}
			}
			status := pl.fh.RunPreFilterExtensionRemovePod(ctx, cycleState, preemptor, pi, nodeInfo)
```

`NodeInfo.RemovePod`（第 2–6 行）会顺带调用 `updateUsedPorts` 把受害者的端口从表里拿掉，再跑一次 Filter 就能看到端口被释放。插件不需要在 `RemovePod` 扩展里维护任何东西。对比后面的 `InterPodAffinity`、`PodTopologySpread`，它们在 PreFilter 里预计算了跨节点的计数，就必须实现 `AddPod`/`RemovePod` 来同步这些计数。

### 3.3 Filter：查表

Filter 假定 PreFilter 已经把状态写好，它只负责拿状态和节点表做一次比对。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeports/node_ports.go
func (pl *NodePorts) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	wantPorts, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	fits := fitsPorts(wantPorts, nodeInfo.GetUsedPorts())
	if !fits {
		return fwk.NewStatus(fwk.Unschedulable, ErrReason)
	}

	return nil
}

func fitsPorts(wantPorts []v1.ContainerPort, portsInUse fwk.HostPortInfo) bool {
	// try to see whether portsInUse and wantPorts will conflict or not
	for _, cp := range wantPorts {
		if portsInUse.CheckConflict(cp.HostIP, string(cp.Protocol), cp.HostPort) {
			return false
		}
	}
	return true
}
```

和 `NodeAffinity` 不同，读不到 PreFilter 状态时这里不兜底，直接返回 `Error`（第 6–9 行）。

原因是 PreFilter 已经对「没有端口」的 Pod 返回了 `Skip`，而[上一篇](/posts/sched-plugin-06-node-affinity/) 3.2 节讲过，返回 `Skip` 的插件会被框架记进 CycleState 的 `skipFilterPlugins`，这个周期里它的 Filter 不会被调用。所以 Filter 被调用就意味着 PreFilter 一定写过状态。

**失败的状态码是 `Unschedulable`，而不是 `UnschedulableAndUnresolvable`**（第 11–14 行）；原因串是 `node(s) didn't have free ports for the requested pod ports`。

### 3.4 HostPortInfo：三元组与 0.0.0.0

冲突怎么判，完全由已用端口的数据结构决定。它是一个以 `hostIP` 为外层 key 的两层 map：

```go {linenos=inline}
// staging/src/k8s.io/kube-scheduler/framework/types.go
// DefaultBindAllHostIP defines the default ip address used to bind to all host.
const DefaultBindAllHostIP = "0.0.0.0"

// ProtocolPort represents a protocol port pair, e.g. tcp:80.
type ProtocolPort struct {
	Protocol string
	Port     int32
}

// HostPortInfo stores mapping from ip to a set of ProtocolPort
type HostPortInfo map[string]map[ProtocolPort]struct{}
```

外层 key 是 `hostIP`，内层是 `{protocol, port}` 的集合。冲突判定：

```go {linenos=inline}
// staging/src/k8s.io/kube-scheduler/framework/types.go
func (h HostPortInfo) CheckConflict(ip, protocol string, port int32) bool {
	if port <= 0 {
		return false
	}

	h.sanitize(&ip, &protocol)

	pp := NewProtocolPort(protocol, port)

	// If ip is 0.0.0.0 check all IP's (protocol, port) pair
	if ip == DefaultBindAllHostIP {
		for _, m := range h {
			if _, ok := m[*pp]; ok {
				return true
			}
		}
		return false
	}

	// If ip isn't 0.0.0.0, only check IP and 0.0.0.0's (protocol, port) pair
	for _, key := range []string{DefaultBindAllHostIP, ip} {
		if m, ok := h[key]; ok {
			if _, ok2 := m[*pp]; ok2 {
				return true
			}
		}
	}

	return false
}

// sanitize the parameters
func (h HostPortInfo) sanitize(ip, protocol *string) {
	if len(*ip) == 0 {
		*ip = DefaultBindAllHostIP
	}
	if len(*protocol) == 0 {
		*protocol = string(v1.ProtocolTCP)
	}
}
```

{{< figure src="/images/posts/sched-plugins/07-hostport-conflict.svg" caption="同一张 UsedPorts 表上五个查询的结果：0.0.0.0 查全部 IP，具体 IP 只查自己和 0.0.0.0，协议不同不冲突（点击查看大图）" link="/images/posts/sched-plugins/07-hostport-conflict.svg" >}}

规则可以归纳为三条：

1. 空值补全。`hostIP` 为空按 `0.0.0.0`，`protocol` 为空按 `TCP`（第 34–41 行）。`Add` 和 `CheckConflict` 都先 `sanitize`（第 7 行），所以「不写」和「写 0.0.0.0」在调度器眼里完全等价。
2. 0.0.0.0 是通配。想要 `0.0.0.0:80/TCP`，就得在所有 IP 下都没人占 `80/TCP`（第 12–19 行遍历整张表）；反过来，已经有人占了 `0.0.0.0:80/TCP`，任何具体 IP 的 `80/TCP` 都会冲突（第 22–28 行）。这对应操作系统里 `INADDR_ANY` 的行为：绑定了通配地址，就不能再在某个具体地址上绑定同一端口。
3. 具体 IP 之间互不影响。`10.0.0.5:80` 和 `10.0.0.6:80` 可以共存在同一个节点上（第 22–28 行只查 `ip` 和 `DefaultBindAllHostIP` 两个 key），适合多网卡节点上每个 IP 跑一个实例。

**三条合起来只有一句：判冲突看的是「地址 + 协议 + 端口」三元组，而 `0.0.0.0` 在地址这一维上匹配一切。**

协议是三元组的一部分，`TCP 53` 和 `UDP 53` 不冲突，这正是 DNS 服务同时监听两种协议的方式。

把三条规则放到清单上。假设一个节点有两块网卡，地址是 `10.0.0.5` 和 `10.0.0.6`，下面三个 Pod 可以同时调度到它上面：

```yaml {linenos=inline}
# 三个 Pod 的端口声明，可以共存在同一个节点上
# Pod lb-a
spec:
  containers:
  - name: lb
    # ...
    ports:
    - containerPort: 80
      hostIP: 10.0.0.5
      hostPort: 80
---
# Pod lb-b
spec:
  containers:
  - name: lb
    # ...
    ports:
    - containerPort: 80
      hostIP: 10.0.0.6
      hostPort: 80
---
# Pod dns
spec:
  containers:
  - name: dns
    # ...
    ports:
    - containerPort: 53
      hostPort: 53
      protocol: UDP
    - containerPort: 53
      hostPort: 53
      protocol: TCP
```

三个 Pod 依次落到节点上，`UsedPorts` 的变化和判断如下：

1. `lb-a` 第 9–10 行占住 `10.0.0.5` 下的 `TCP 80`；
2. `lb-b` 第 19 行的 `hostIP` 是 `10.0.0.6`，`CheckConflict` 只查 `0.0.0.0` 和 `10.0.0.6` 两个 key（规则 3），都没有 `TCP 80`，放行；
3. `dns` 没写 `hostIP`，按 `0.0.0.0` 处理，第 28–33 行要的是 `UDP 53` 和 `TCP 53`，遍历整张表都查不到，放行；两项协议不同，彼此也不冲突。

反例：再来一个 Pod 只写 `hostPort: 80`、不写 `hostIP`，它按 `0.0.0.0:80/TCP` 查询，遍历整张表时在 `10.0.0.5` 下查到 `TCP 80`（规则 2），这个节点被拒绝。

`IPv6` 的通配地址 `::` 没有被特殊处理，它只是一个普通的 IP 字符串，只会和 `0.0.0.0` 以及自己冲突。在 IPv6 节点上用 `hostIP: "::"` 时要意识到这一点。

### 3.5 为什么是 Unschedulable

**失败码决定了 `DefaultPreemption` 会不会在这个节点上找受害者**。两种状态码的态度完全不同：

| 状态码 | 抢占的态度 |
| --- | --- |
| `UnschedulableAndUnresolvable` | 驱逐什么都没用，跳过这个节点 |
| `Unschedulable` | 也许驱逐一些低优先级 Pod 就能放下，值得模拟 |

端口被占是一个「资源被别人用了」的问题，和 CPU 不够性质一样：只要把占端口的 Pod 赶走，端口就释放了。所以 `NodePorts` 返回 `Unschedulable`。

一个实际场景：节点上有一个低优先级的调试 Pod 占了 `hostPort: 9100`，现在要部署高优先级的 node-exporter DaemonSet，它也要 9100。DaemonSet Pod 通过节点亲和被锁定到这个节点（见上一篇的 `PreFilterResult`），之后分三步：

1. `NodePorts` 返回 `Unschedulable`；
2. `DefaultPreemption` 在这个节点上模拟删除调试 Pod，`RemovePod` 释放 9100；
3. Filter 通过，调试 Pod 被驱逐，node-exporter 被提名到这个节点。

如果 `NodePorts` 返回的是 `UnschedulableAndUnresolvable`，这个 DaemonSet Pod 会永远 Pending。

这个场景对应的清单如下：

```yaml {linenos=inline}
# PriorityClass + DaemonSet：高优先级的 node-exporter
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: node-agent-high
value: 1000000
description: "node agents that must run on every node"
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: node-exporter
spec:
  selector:
    matchLabels:
      app: node-exporter
  template:
    metadata:
      labels:
        app: node-exporter
    spec:
      priorityClassName: node-agent-high
      containers:
      - name: exporter
        image: quay.io/prometheus/node-exporter:v1.8.2
        ports:
        - containerPort: 9100
          hostPort: 9100
---
# 低优先级的调试 Pod，先占了 9100
apiVersion: v1
kind: Pod
metadata:
  name: debug-0
spec:
  containers:
  - name: debug
    image: busybox:1.36
    command: ["sleep", "infinity"]
    ports:
    - containerPort: 9100
      hostPort: 9100
```

三处字段共同决定了抢占能成立：

1. 优先级：第 6 行定义 `value: 1000000`，第 22 行通过 `priorityClassName` 引用，Priority 准入插件在 Pod 创建时把它写进 `spec.priority`；`debug-0` 没写 `priorityClassName`，集群里没有 `globalDefault` 的 PriorityClass 时它的优先级是 0。
2. 端口：第 27–28 行和第 41–42 行都没写 `hostIP` 和 `protocol`，按 `sanitize` 都是 `0.0.0.0:9100/TCP`，`NodePorts` 在 `debug-0` 所在节点返回 `Unschedulable`。
3. 抢占策略：PriorityClass 没写 `preemptionPolicy`，默认是 `PreemptLowerPriority`。如果写成 `preemptionPolicy: Never`，`DefaultPreemption` 不会为它驱逐任何 Pod，即便失败码是 `Unschedulable`，它也只能等 `debug-0` 自己被删掉。

对比上一篇的 `NodeAffinity` 和下一篇的 [NodeResourcesFit](/posts/sched-plugin-08-node-resources-fit/)：

1. `NodeAffinity` 一律不可解决；
2. `NodeResourcesFit` 视情况而定（请求量超过节点总量时不可解决）；
3. `NodePorts` 则一律可解决。

它不需要区分，因为端口表里的每一项都对应某个可以驱逐的 Pod。

被拒绝的 Pod 不会立刻重试，它要等一个「情况可能已经变了」的信号。

## 四、EventsToRegister 与 QueueingHint

这个信号由两样东西构成：`EventsToRegister` 声明插件关心哪些事件，QueueingHint 判断某个具体事件值不值得把 Pod 放回队列。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeports/node_ports.go
func (pl *NodePorts) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// Due to immutable fields `spec.containers[*].ports`, pod update events are ignored.
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodDeleted},
		// We don't need the QueueingHintFn here because the scheduling of Pods will be always retried with backoff when this Event happens.
		// (the same as Queue)
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add}},
	}, nil
}
```

两类事件能让被端口卡住的 Pod 有新机会：

1. 已调度的 Pod 被删除（第 5 行），可能释放端口。容器的 `ports` 字段不可变，所以不需要订阅 Pod 更新。
2. 新节点加入（第 8 行），新节点上端口全空。这里没有 hint 函数，等价于总是 `Queue`。节点更新不订阅，节点的任何属性变化都不会影响端口占用。

删除事件几乎在每个节点上都在发生，所以它单独配了 hint。这个函数要把「删了也没用」的情况提前挡掉：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/nodeports/node_ports.go
func (pl *NodePorts) isSchedulableAfterAssignedPodDeleted(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	deletedPod, _, err := util.As[*v1.Pod](oldObj, nil)
	if err != nil {
		return fwk.Queue, err
	}

	// If the deleted pod is unscheduled, it doesn't make the target pod schedulable.
	if deletedPod.Spec.NodeName == "" && deletedPod.Status.NominatedNodeName == "" {
		// ...
		return fwk.QueueSkip, nil
	}

	// If the deleted pod doesn't use any host ports, it doesn't make the target pod schedulable.
	ports := util.GetHostPorts(deletedPod)
	if len(ports) == 0 {
		return fwk.QueueSkip, nil
	}

	// Verify that `pod` and the deleted pod don't have any common port(s).
	// So, deleting that pod couldn't make `pod` schedulable.
	portsInUse := make(fwk.HostPortInfo, len(ports))
	for _, p := range ports {
		portsInUse.Add(p.HostIP, string(p.Protocol), p.HostPort)
	}
	if fitsPorts(util.GetHostPorts(pod), portsInUse) {
		// ...
		return fwk.QueueSkip, nil
	}

	// ...
	return fwk.Queue, nil
}
```

依次排除三种「删了也没用」的情况：

1. 被删的 Pod 既没有调度到节点，也没有被提名到节点，它从没占用过任何节点的端口（第 9–12 行）。
2. 被删的 Pod 根本没有 `hostPort`（第 15–18 行）。
3. 被删 Pod 的端口和当前 Pod 想要的没有交集（第 22–29 行）。

第 1 条里还要多看一眼 `NominatedNodeName`：Filter 阶段框架会把提名到该节点、优先级不低于当前 Pod 的 Pod 临时加进 `NodeInfo` 再跑一遍（`RunFilterPluginsWithNominatedPods`），被提名的 Pod 实际上「预占」了端口，它被删除同样可能让端口空出来。

第 3 条的实现很巧妙，分两步：

1. 把被删 Pod 的端口装进一张临时的 `HostPortInfo`；
2. 用 `fitsPorts` 检查当前 Pod 能不能「放进」这张表。

能放进就说明没有交集，用的是和 Filter 完全一样的冲突语义，`0.0.0.0` 的通配规则自动生效。

hint 没有检查被删 Pod 在哪个节点。只要端口有交集就 `Queue`，哪怕那个节点因为别的原因（比如节点亲和）根本不在当前 Pod 的候选范围里。**hint 的设计原则是宁可多唤醒，不可漏唤醒**，节点维度的精确判断留给下一次调度周期本身。

集群里大多数 Pod 删除事件都发生在没有 `hostPort` 的 Pod 上，第 2 步就能把它们挡掉。这意味着一个等端口的 Pod 不会被每次滚动更新反复唤醒。

#### 举个例子

等待中的 Pod `exporter` 声明 `hostPort: 9100`，`hostIP` 和 `protocol` 都没写，按 `sanitize` 就是 `0.0.0.0:9100/TCP`。集群里先后删除了四个 Pod，hint 的判断如下（行号对应上面的 `isSchedulableAfterAssignedPodDeleted`）：

| 被删的 Pod | 它的端口 | 走到哪一步 | 结果 |
| --- | --- | --- | --- |
| `pending-x` | 从未调度，`spec.nodeName` 和 `NominatedNodeName` 都为空 | 第 9–12 行 | `QueueSkip` |
| `web-7` | 没有 `hostPort` | 第 15–18 行，`ports` 为空 | `QueueSkip` |
| `dns-1` | `hostPort: 9100`，`protocol: UDP` | 临时表是 `{0.0.0.0: {UDP 9100}}`，查 `TCP 9100` 查不到，第 26–29 行 | `QueueSkip` |
| `debug-1` | `hostIP: 10.0.0.5`，`hostPort: 9100` | 临时表是 `{10.0.0.5: {TCP 9100}}`；`exporter` 要的是 `0.0.0.0`，`CheckConflict` 遍历整张表，在 `10.0.0.5` 下查到 `TCP 9100`，`fitsPorts` 为假 | `Queue`（第 32 行） |

把 `exporter` 换成声明 `hostIP: 10.0.0.6` 的 Pod，`debug-1` 的删除就变成 `QueueSkip`：这时 `CheckConflict` 只查 `0.0.0.0` 和 `10.0.0.6` 两个 key，临时表里只有 `10.0.0.5`。

四次删除只有一次唤醒了 `exporter`，而且是否唤醒和 Filter 判冲突的结论一致，这正是 hint 复用 `fitsPorts` 的效果。

到这里，判定逻辑、失败码和唤醒条件都齐了。剩下几种情况是插件判不到的，下一节把它们集中列出来。

## 五、边界情况与坑

前面讲的是它怎么判，这一节讲它判不到什么。下面几种情况都会让 `NodePorts` 的结论和节点上的真实状态对不上。

Pod 内部的端口冲突，调度器不管。`fitsPorts` 只拿待调度 Pod 的端口和节点表比较，不检查 Pod 自己的端口之间是否冲突。这件事由 API 校验负责，但校验的口径和调度器不一样：

```go {linenos=inline}
// pkg/apis/core/validation/validation.go
			str := fmt.Sprintf("%s/%s/%d", ctr.Ports[pi].Protocol, ctr.Ports[pi].HostIP, port)
			if accumulator.Has(str) {
				allErrs = append(allErrs, field.Duplicate(idxPath.Child("hostPort"), str))
			}
```

API 校验按字面拼字符串去重（第 2–4 行），`TCP//80`（hostIP 为空）和 `TCP/0.0.0.0/80` 是两个不同的串，能通过校验；在调度器的 `sanitize` 之后它们是同一个三元组。

下面这个 Pod 就属于这种情况：

```yaml {linenos=inline}
# Pod：能通过 API 校验，调度器却视为同一个三元组
apiVersion: v1
kind: Pod
metadata:
  name: dup-port
spec:
  containers:
  - name: web
    image: nginx:1.27
    ports:
    - containerPort: 80
      hostPort: 80               # 校验时拼成 TCP//80
    - containerPort: 8080
      hostIP: 0.0.0.0
      hostPort: 80               # 校验时拼成 TCP/0.0.0.0/80
```

它在 API 和调度器两侧的遭遇不同：

1. API 校验：`protocol` 先被默认值填成 `TCP`，第 11–12 行和第 13–15 行拼出两个不同的串，`accumulator` 里不重复，创建成功；
2. 调度器：`GetHostPorts` 收集到两项，`sanitize` 后都是 `0.0.0.0:80/TCP`，但 `fitsPorts` 只拿它们和节点表比，不会发现自相冲突；
3. 节点上：两个容器端口要映射到同一个主机端口，结果取决于容器运行时和 CNI。

另外，init 容器（包括 sidecar）是逐个单独校验的（源码注释是 `init containers run one-by-one`），sidecar 和应用容器之间的 `hostPort` 重复也不会被 API 拦下。这样的 Pod 能被调度，到了节点上由容器运行时或 CNI 报错。

**调度器不知道主机进程占用的端口。** `UsedPorts` 只来自 Pod 的声明。节点上的 sshd、kubelet（10250）、节点本地的 DNS 缓存等主机进程占用的端口，调度器一概不知。

由此带来的后果是：一个 `hostNetwork` Pod 声明了 `containerPort: 10250`，调度器照样会放行，结果是容器起不来。

**`hostPort` 不是真的在主机上 bind。** 对非 hostNetwork 的 Pod，`hostPort` 通常由 CNI 的 portmap 插件用 iptables/nftables DNAT 规则实现，主机上并没有进程监听这个端口。

调度器的三元组模型是对这种 DNAT 语义的抽象：`0.0.0.0` 的规则匹配所有目的地址，所以和任何具体 IP 冲突。如果使用的 CNI 不支持 portmap，`hostPort` 会静默失效，而调度器依然认为端口被占用。

端口表是集合，不是计数。`HostPortInfo.Remove` 直接从集合里删掉三元组。正常调度下同一个三元组不可能在一个节点上出现两次：第二个想占同一三元组的 Pod 在 Filter 里就被 `CheckConflict` 挡下，进不了这个节点的 `NodeInfo`。所以没有问题。

kubelet 会再查一遍。kubelet 准入的 `generalFilter` 复用了插件导出的 `Fits`（第 2–4 行）：

```go {linenos=inline}
// pkg/kubelet/lifecycle/predicate.go
	if !nodeports.Fits(pod, nodeInfo) {
		reasons = append(reasons, &PredicateFailureError{nodeports.Name, nodeports.ErrReason})
	}
```

直接写 `nodeName` 绕过调度器的 Pod 如果端口冲突，会被 kubelet 以 reason `NodePorts` 拒绝。

批量调度的签名只含端口号。`SignPod` 用的 `HostPortsSigner` 只收集 `hostPort` 的数值，不含 `hostIP` 和协议。两个 Pod 只在 `hostIP` 上不同，会得到相同的签名片段。

这不影响正确性：批量调度复用的只是上一个 Pod 的节点排名作为提示，被提示的节点仍然要重新跑一遍 Filter。`findNodesThatFitPod`（`pkg/scheduler/algorithm.go`）把 `GetNodeHint` 返回的节点交给 `evaluateNominatedNode`，和提名节点走同一条路径，在这个节点上调用 `findNodesThatPassFilters`。

这些情况大多不是插件的 bug，而是 `hostPort` 这个机制本身的代价。下一节换个角度：这个代价值不值得付。

## 六、发散：hostPort 的替代与取舍

`hostPort` 让 Pod 与节点端口绑定，带来了调度上的硬约束：同一个节点最多跑一个实例，实际效果接近一个隐式的 Pod 反亲和。很多团队用 `hostPort` 部署 ingress 控制器或监控 agent，本意是「每个节点一个」，这时 DaemonSet 才是正确的工具，`hostPort` 只是端口暴露的手段。

如果只是要从集群外访问服务，`NodePort` 类型的 Service（注意不是这个插件）由 kube-proxy 在所有节点上开放端口，不影响调度；`LoadBalancer` 和 Ingress 更进一步。**只有在需要绕开 kube-proxy、保留客户端源 IP、或者必须让流量落在本节点的 Pod 上时，`hostPort`/`hostNetwork` 才值得它带来的调度约束。**

从调度器的角度看，`NodePorts` 是一个几乎零成本的插件：大部分 Pod 在 PreFilter 就 `Skip` 了；需要检查的 Pod，每个节点也只是几次 map 查找。它用最少的代码展示了几个贯穿全系列的设计：

1. 状态放在 `NodeInfo` 还是 CycleState；
2. 失败码如何决定抢占；
3. hint 如何用和 Filter 相同的语义精确判断「这个事件和我有没有关系」。

## 七、小结

1. `NodePorts` 只检查 `hostPort > 0` 的端口，来源是应用容器和 sidecar；`hostNetwork` Pod 的 `containerPort` 会被默认填进 `hostPort`。
2. PreFilter 对没有端口的 Pod 返回 `Skip`；CycleState 只放待调度 Pod 的端口，节点已用端口在 `NodeInfo.UsedPorts` 里，所以不需要 `PreFilterExtensions`。
3. 冲突判定基于 `(hostIP, protocol, port)`：空 IP 视为 `0.0.0.0`，`0.0.0.0` 与任何 IP 冲突，具体 IP 只与自己和 `0.0.0.0` 冲突，协议不同不冲突。
4. 冲突返回 `Unschedulable`，可以通过抢占占用端口的低优先级 Pod 解决。
5. QueueingHint 只在被删 Pod 曾经占用（或提名占用）过、且与当前 Pod 有端口交集时返回 `Queue`；新节点加入总是重试。

端口之后是调度器里最重的一道检查：[NodeResourcesFit](/posts/sched-plugin-08-node-resources-fit/) 要回答「资源够不够」，而「Pod 到底请求了多少资源」这个问题，比看上去复杂得多。
