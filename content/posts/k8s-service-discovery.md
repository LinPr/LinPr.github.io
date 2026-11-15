---
title: 'Kubernetes 服务发现：从一个域名到一条 IPVS 规则'
date: 2026-11-15T18:50:00+08:00
draft: false
tags: ["kubernetes", "coredns", "kube-proxy", "endpointslice", "服务发现"]
categories: ["Kubernetes"]
author: "LinPr"
description: "Pod 用域名访问一个服务，背后是 CoreDNS、kube-proxy 和 EndpointSlice controller 三个组件各管一段。本文从「让 Pod 用域名访问集群外的 registry」这个需求出发，结合源码拆解每一段做了什么，并纠正几个常见误解。"
---

在做离线部署时遇到一个需求：**让集群内的 Pod 通过域名访问集群外的 registry。**

![Pod 访问同一局域网内、集群外的机器](/images/posts/k8s-service-discovery/img-00.png)

直接写 IP 当然能通，但这样程序就和 registry 的地址绑死了。我们想要的是：程序只认一个固定域名，registry 换了 IP，Pod 里的配置一行都不用改。这正是 DNS 做的事，此时 DNS 就充当了服务注册中心。

这个需求有好几种解法。本文先把解法列出来，再借其中最有代表性的一种（不带 selector 的 Service），把 Kubernetes 服务发现的整条链路拆开看：**域名怎么解析成 IP，IP 怎么转发到后端，后端列表又是谁维护的。**

文中源码出自 `k8s.io/kubernetes`（commit `aabdf5a2`）和 `coredns/coredns`（commit `c3218b2`）。

## 一、四种让 Pod 访问集群外服务的办法

### 1.1 CoreDNS 的 hosts 插件

最直接的办法：在 CoreDNS（或 NodeLocal DNSCache）的 ConfigMap 里加一条静态记录。

```text
hosts {
    172.24.64.122 registry.example.internal
    fallthrough
}
```

`fallthrough` 表示没命中的查询继续交给后面的插件，不写的话其他域名都会解析失败。

改完 ConfigMap 不一定要重启 CoreDNS。kubeadm 默认的 Corefile 里带了 `reload` 插件，它会定期计算 Corefile 的校验和，变化后自动重新加载。ConfigMap 同步到 Pod 里的挂载文件本身还有一段延迟（kubelet 的同步周期，通常在一分钟以内），所以要等一会儿才生效。如果你的 Corefile 没有 `reload`，才需要重启 Pod。

### 1.2 ExternalName Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: registry
spec:
  type: ExternalName
  externalName: registry.example.internal
```

CoreDNS 对 `registry.<ns>.svc.cluster.local` 返回一条 CNAME，指向 `externalName`。它不分配 ClusterIP，kube-proxy 也不会为它建规则。前提是外部服务**本身已经有一个能解析的域名**，它只是起了个集群内的别名。如果外部服务只有 IP，这个办法帮不上忙。

### 1.3 不带 selector 的 Service + 手工 EndpointSlice

官方文档 [Services without selectors](https://kubernetes.io/docs/concepts/services-networking/service/#services-without-selectors) 提到的做法：Service 不写 `selector`，后端地址由你自己创建 EndpointSlice 来提供。文档列举的适用场景有：

1. 生产环境用外部数据库集群，测试环境用集群内的数据库；
2. 把 Service 指向其他命名空间或其他集群里的服务；
3. 迁移到 Kubernetes 的过程中，只有一部分后端跑在集群内。

```yaml
apiVersion: v1
kind: Service
metadata:
  name: my-service
spec:
  ports:
    - name: http
      protocol: TCP
      port: 80
      targetPort: 9376
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: my-service-1            # 约定以 Service 名作前缀
  labels:
    kubernetes.io/service-name: my-service   # 靠这个 label 关联到 Service
addressType: IPv4
ports:
  - name: http                  # 要和 Service 端口的 name 一致
    appProtocol: http
    protocol: TCP
    port: 9376
endpoints:
  - addresses: ["10.4.5.6"]
  - addresses: ["10.1.2.3"]
```

关联 Service 和 EndpointSlice 的不是名字，而是 `kubernetes.io/service-name` 这个 label。访问 `my-service:80` 的流量会被负载均衡到两个外部地址的 9376 端口。实测这个办法能解决最初的问题。

两个要注意的地方：

- **不要给手工创建的 EndpointSlice 打 `endpointslice.kubernetes.io/managed-by: endpointslice-controller.k8s.io`。** EndpointSlice controller 用这个 label 认领自己管理的对象，打上之后可能被它当成自己的东西删掉。
- 如果你创建的是旧式的 `Endpoints` 对象，`endpointslicemirroring-controller` 会自动把它镜像成 EndpointSlice，效果一样。

### 1.4 四种办法对比

| 办法 | 返回给 Pod 的是 | 需要外部服务有域名 | 负载均衡 | 改动范围 |
| --- | --- | --- | --- | --- |
| hosts 插件 | 外部 IP（A 记录） | 不需要 | 无（DNS 轮询） | 改 CoreDNS 配置，影响全集群 |
| ExternalName | CNAME | 需要 | 无 | 一个 Service |
| 无 selector Service | ClusterIP，再由 kube-proxy 转发 | 不需要 | 有，IPVS/iptables | 一个 Service + EndpointSlice |
| headless 无 selector Service | 外部 IP 列表（A 记录） | 不需要 | 无（DNS 轮询） | 同上，`clusterIP: None` |

方案 1.3 最值得细看，因为它和普通 Service 走的是同一条链路，只是 EndpointSlice 由人来写而不是由 controller 生成。看懂了它，也就看懂了 Kubernetes 的服务发现。

## 二、一次请求经过哪些地方

先看一个真实集群里，Pod 用域名发 HTTP 请求时发生了什么。

**第一步：读 `/etc/resolv.conf`，找到 DNS 服务器。**

![Pod 内的 resolv.conf](/images/posts/k8s-service-discovery/img-01.png)

`nameserver` 写的是一个 IP，不是域名。原因很简单：解析域名之前得先知道 DNS 服务器在哪，写成域名就成了先有鸡还是先有蛋。这个文件由 kubelet 在创建 Pod 时生成，IP 来自 kubelet 的 `clusterDNS` 配置。

另外两行也值得注意：

- `search default.svc.cluster.local svc.cluster.local cluster.local`：短域名会依次补上这些后缀去查，所以同命名空间里直接写 `my-service` 就能解析。
- `options ndots:5`：域名里的点少于 5 个时，先按 search 列表补后缀查询，全部失败后才查原始域名。查 `registry.example.com`（2 个点）会先查 3 次 `registry.example.com.default.svc.cluster.local` 之类的名字，每次都 NXDOMAIN，最后才查到真正的记录。访问外部域名多的应用可以在域名末尾加 `.`，或者调低 Pod 的 `dnsConfig.options.ndots`。

**第二步：这个 IP 是 CoreDNS Service 的 ClusterIP。**

![CoreDNS Service 的 ClusterIP 是 10.233.0.3](/images/posts/k8s-service-discovery/img-02.png)

**第三步：内核的 IPVS 规则把发往这个 IP 的包转给 CoreDNS Pod。**

![IPVS 规则：10.233.0.3:53 转发到两个后端](/images/posts/k8s-service-discovery/img-03.png)

`10.233.0.3:53` 下挂了两个 real server，调度算法是 `rr`（轮询），转发模式是 `Masq`（NAT）。截图里 grep 出的是 TCP 规则，53/UDP 也有一组对应的规则，DNS 查询默认走的是 UDP。

**第四步：两个 real server 正是 CoreDNS 的两个 Pod。**

![CoreDNS 的两个 Pod IP](/images/posts/k8s-service-discovery/img-04.png)

CoreDNS 收到查询后，如果域名属于集群域（`cluster.local`），由 `kubernetes` 插件从内存里查 Service 并返回结果；否则按 Corefile 里的 `forward` 规则交给上游 DNS。

**第五步：应用拿到解析结果，向这个 IP 发起 TCP 连接。** 如果解析出来的是普通 Service 的 ClusterIP，这个连接又会经过一次第三步那样的 IPVS 转发，最终落到某个后端 Pod 上。

整条链路里出现了 Service、CoreDNS 和 IPVS，却没看到 EndpointSlice。可理论上「这个 Service 背后有哪些 Pod」必须有人记录。它在哪里起作用？下面按组件逐个看源码。

---

## 三、三个组件各管一段

先给出全景，再分开讲：

| 组件 | 监听的资源 | 产出 | 回答的问题 |
| --- | --- | --- | --- |
| EndpointSlice controller（kube-controller-manager） | Service、Pod、Node、EndpointSlice | EndpointSlice 对象 | 这个 Service 现在有哪些可用后端 |
| kube-proxy（每个节点） | Service、EndpointSlice、Node | 本节点内核里的 IPVS / iptables / nftables 规则 | 发往 ClusterIP 的包转给谁 |
| CoreDNS `kubernetes` 插件 | Service、EndpointSlice、Namespace（可选 Pod） | DNS 应答 | 这个域名对应哪个 IP |

EndpointSlice 是中间那张表：controller 写它，kube-proxy 和 CoreDNS 读它。三者之间没有直接调用，全靠 API Server 里的对象和 informer 的事件串起来。

## 四、CoreDNS：把域名翻译成 ClusterIP

域名规则可以先看官方文档 [Service 与 Pod 的 DNS](https://kubernetes.io/zh-cn/docs/concepts/services-networking/dns-pod-service/)。CoreDNS 里负责集群域名的是 `plugin/kubernetes`，插件说明在 [coredns.io/plugins/kubernetes](https://coredns.io/plugins/kubernetes/)。

### 4.1 启动时：用 informer 把资源搬进内存

CoreDNS 不会在每次查询时去问 API Server。插件初始化时调用 `InitKubeCache`，创建一个 `dnsControl`，再用一个 goroutine 在后台运行它。

![InitKubeCache 创建并在后台启动 controller](/images/posts/k8s-service-discovery/img-05.png)

`newdnsController` 里为每种资源建一个 informer，带上自定义的索引（按名字、按 IP），收到增删改事件后更新本地缓存。

![newdnsController 为 Service、Pod、EndpointSlice、Namespace 各建一个 informer](/images/posts/k8s-service-discovery/img-06.png)

这里有两个开关：

- Pod informer 只在 `pods verified` 模式下才真正启动（`initPodCache`）。kubeadm 默认配置是 `pods insecure`，不需要 Pod 缓存。
- EndpointSlice informer 默认启动，在 Corefile 里写 `noendpoints` 可以关掉，代价是 headless Service 和按端点名查询都不可用。

所以「CoreDNS 也读 EndpointSlice」是对的，但它读 EndpointSlice 主要是为了 headless Service，普通 ClusterIP Service 用不到。下面会看到这一点。

### 4.2 查询时：按记录类型分发，最后都落到 `findServices`

插件的 `ServeDNS` 先判断域名是否属于自己负责的 zone，不属于就交给下一个插件。属于的话，按查询类型分发到 `plugin.A`、`plugin.SRV` 等通用函数。

![etcd 插件的 ServeDNS 按 QType 分发](/images/posts/k8s-service-discovery/img-07.png)

截图是 `etcd` 插件的版本，`kubernetes` 插件的 `ServeDNS`（`plugin/kubernetes/handler.go`）结构完全相同。区别在于，`kubernetes` 插件对 AXFR/IXFR 直接返回 `REFUSED`，因为它不支持区域传送。

![同一段分发逻辑](/images/posts/k8s-service-discovery/img-08.png)

`plugin.A` 这些通用函数会回调插件的 `Services()` 方法，`kubernetes` 插件的实现再经过 `Records()` 调到 `findServices()`。这个函数按 Service 类型分了四条路：

![findServices 中的 ClusterIP 分支](/images/posts/k8s-service-discovery/img-09.png)

| Service 类型 | 返回什么 | 是否看 EndpointSlice |
| --- | --- | --- |
| ExternalName | 一条 CNAME，指向 `spec.externalName` | 否 |
| headless（`clusterIP: None`），或查询带了端点名 | 每个就绪端点的 IP | 是 |
| ClusterIP | `svc.ClusterIPs` 里的每个 IP（双栈时有两个） | 默认否 |
| 不存在 | NXDOMAIN | — |

ClusterIP 分支（截图红框）只做两件事：按端口和协议过滤 `svc.Ports`（SRV 查询会用到），然后为每个 ClusterIP 生成一条记录。**它返回的就是 Service 自己的 IP，和背后有没有 Pod 无关。**

### 4.3 「没有后端就不解析」是可选行为，不是默认行为

截图上方折叠的那段 `podsCount` 逻辑，容易让人以为 CoreDNS 会检查 Service 后面有没有 Pod，没有的话就直接解析失败。看完整代码：

```go
// plugin/kubernetes/kubernetes.go
if k.opts.ignoreEmptyService && svc.Type != api.ServiceTypeExternalName && !svc.Headless() {
    // serve NXDOMAIN if no endpoint is able to answer
    podsCount := 0
    for _, ep := range endpointsListFunc() {
        for _, eps := range ep.Subsets {
            podsCount += len(eps.Addresses)
        }
    }
    if podsCount == 0 {
        continue
    }
}
```

外层条件是 `ignoreEmptyService`，默认值是 `false`（`setup.go` 里初始化为 `false`），只有在 Corefile 里写了 `ignore empty_service` 才会打开。默认配置下，没有任何后端的 ClusterIP Service 照样能解析出 ClusterIP，请求发出去之后才会失败（连接被拒或超时）。

对 headless Service 则不同：它本来就要返回端点 IP，没有就绪端点自然就没有记录。CoreDNS 判断「就绪」的方式和 API 文档一致，`ready` 字段为 nil 时按就绪处理：

```go
// plugin/kubernetes/object/endpoint.go
func endpointsliceReady(ready *bool) bool {
    // Per API docs: a nil value indicates an unknown state. In most cases consumers
    // should interpret this unknown state as ready.
    if ready == nil {
        return true
    }
    return *ready
}
```

手工写 EndpointSlice 时不填 `conditions`，CoreDNS 和 kube-proxy 都会把端点当成就绪的，这也是 1.3 节的例子能直接工作的原因。

### 4.4 小结

CoreDNS 只负责把 `<service>.<namespace>.svc.cluster.local` 翻译成一个 IP。对普通 Service，这个 IP 是 ClusterIP。但 ClusterIP 是虚拟的，没有任何进程监听它，包要能到达 Pod，还需要有人在每个节点上把它转出去。这是 kube-proxy 的工作。

---

## 五、kube-proxy：把 ClusterIP 落成内核规则

kube-proxy 在每个节点上运行，本身不转发流量（userspace 模式早已删除）。它做的是把 Service 和 EndpointSlice 翻译成内核规则，真正转发包的是内核。下面以 IPVS 模式为例，iptables 和 nftables 模式的结构相同，只是产出的规则不一样。

### 5.1 事件回调：只记账，不立即同步

IPVS 模式的 `Proxier` 实现了 Service、EndpointSlice、Node 三类资源的事件回调：

![Proxier 实现的事件回调方法](/images/posts/k8s-service-discovery/img-10.png)

以 EndpointSlice 为例，三个回调都是同一个模式：先把变化记进 `endpointsChanges`，如果确实有变化、并且 Proxier 已经初始化，就调用 `Sync()`。

![OnEndpointSliceAdd/Update/Delete 都调用 Sync](/images/posts/k8s-service-discovery/img-11.png)

`Sync()` 并不直接改规则：

```go
// pkg/proxy/ipvs/proxier.go
func (proxier *Proxier) Sync() {
    if proxier.healthzServer != nil {
        proxier.healthzServer.QueuedUpdate(proxier.ipFamily)
    }
    metrics.SyncProxyRulesLastQueuedTimestamp.WithLabelValues(string(proxier.ipFamily)).SetToCurrentTime()
    proxier.syncRunner.Run()
}
```

`syncRunner` 是一个 `BoundedFrequencyRunner`，`Run()` 只是请求执行一次，真正执行时调用的函数是 `syncProxyRules`。这个 runner 有两个限制：

- **最小间隔 `minSyncPeriod`**：两次同步之间至少隔这么久，短时间内的多次变化会合并成一次。集群里 Pod 频繁滚动时，这能避免每个事件都重写一遍规则。
- **最大间隔 `syncPeriod`**：即使没有任何事件，也会定期全量同步一次（IPVS 默认 30 秒），用来修复被人手工改掉的规则。

另外，`OnEndpointSlicesSynced` 和 `OnServiceSynced` 会在 informer 首次同步完成后各调用一次。只有两类资源都同步完，`isInitialized()` 才为真。这样可以保证 kube-proxy 不会拿着不完整的数据去写规则，把还没加载到的 Service 误删掉。

### 5.2 `syncProxyRules`：对比期望与现状，补齐差异

![syncProxyRules：所有 IPVS 调用都发生在这里](/images/posts/k8s-service-discovery/img-12.png)

这个函数比较长，主要做这几件事：

1. 把 `endpointsChanges`、`serviceChanges` 合并进内存里的 `svcPortMap` 和 `endpointsMap`；
2. 确保节点上存在一块名为 `kube-ipvs0` 的 dummy 网卡，把所有 ClusterIP 绑到这块网卡上，这样内核才会接收发往这些 IP 的包；
3. 为每个 Service 端口创建 IPVS 虚拟服务，为每个可用端点创建 real server；
4. 维护 ipset 和少量 iptables 规则，用于 SNAT、NodePort、`externalTrafficPolicy` 等场景；
5. 删除已经不存在的虚拟服务和 real server。

第 3 步「可用端点」是怎么选出来的？`pkg/proxy/topology.go` 里会先过滤掉 `IsReady()` 为假的端点，再按拓扑感知路由（Topology Aware Routing）和 `internalTrafficPolicy` 做二次筛选。只有当一个 Service 完全没有就绪端点时，才会退而使用「serving 且 terminating」的端点，保证滚动更新期间的连接能平滑结束。

### 5.3 关于 IPVS 模式的现状

源码里已经有明确的弃用提示：

```go
// cmd/kube-proxy/app/server_linux.go
message := "The ipvs proxier has been deprecated and will be disabled by default in Kubernetes 1.40 and removed in Kubernetes 1.43. Migrate to the 'nftables' proxier instead."
```

新集群建议直接用 `nftables` 模式（1.33 GA）。三种模式读取的数据完全相同，监听 Service 和 EndpointSlice、合并变化、限频同步这套结构也一样，本文讲的链路不受影响。

---

## 六、EndpointSlice controller：维护后端列表

前两节都在读 EndpointSlice。写它的是 kube-controller-manager 里的 EndpointSlice controller。

### 6.1 它监听什么

启动代码一眼就能看出它的输入：

![startEndpointSliceController 传入 Pod、Service、Node、EndpointSlice 四个 informer](/images/posts/k8s-service-discovery/img-13.png)

- **Service**：有了 selector，才知道要找哪些 Pod；
- **Pod**：Pod 的 IP、就绪状态、所在节点；
- **Node**：用于填写端点的 `zone`，支撑拓扑感知路由；
- **EndpointSlice**：自己写出去的对象被别人改了，要能感知并纠正。

下面那个 `startEndpointSliceMirroringController` 就是 1.3 节提到的镜像 controller，它监听的是旧式 Endpoints 对象。

截图来自较早的版本。当前源码里，controller 的核心逻辑已经拆到了独立的 `k8s.io/endpointslice` 库（`staging/src/k8s.io/endpointslice/`），以便其他项目复用，但结构没有变。

### 6.2 Pod 变化什么时候算「端点变了」

不是 Pod 的每次更新都会触发同步。`podEndpointsChanged` 只关心会影响端点的字段：

![podEndpointsChanged：就绪状态变化视为端点变化](/images/posts/k8s-service-discovery/img-14.png)

| 变化 | 影响 |
| --- | --- |
| labels、hostname、subdomain | 可能改变它属于哪些 Service |
| `DeletionTimestamp` 被设置 | 端点进入 terminating |
| 就绪状态（`Ready` condition） | 端点的 `ready` 条件翻转 |
| Pod IP | 端点地址变化 |

其他字段（比如容器重启次数）不会触发。这里也解释了 readinessProbe 的作用路径：**探针失败 → kubelet 把 Pod 的 `Ready` condition 置为 False → controller 更新 EndpointSlice → kube-proxy 从规则里摘掉这个端点。** 探针本身不直接和 kube-proxy 打交道。

### 6.3 `syncService` 与 `reconcile`

每个变化最终会把对应的 Service 放进工作队列，由 `syncService` 处理：

![syncService 列出相关对象后调用 reconcile](/images/posts/k8s-service-discovery/img-15.png)

`syncService` 先取出 Service 和它 selector 选中的 Pod，再取出这个 Service 名下、由本 controller 管理的 EndpointSlice，交给 `reconcile` 计算差异，输出要创建、更新和删除的 EndpointSlice。

两个细节和本文主题直接相关：

**第一，没有 selector 的 Service，controller 直接跳过。**

```go
// pkg/controller/endpointslice/endpointslice_controller.go
if service.Spec.Selector == nil {
    // services without a selector receive no endpoint slices from this controller;
    // these services will receive endpoint slices that are created out-of-band via the REST API.
    return nil
}
```

这就是 1.3 节的方案能工作的原因：controller 不会去覆盖你手写的 EndpointSlice。下游的 kube-proxy 和 CoreDNS 不区分 EndpointSlice 是谁写的，只看 `kubernetes.io/service-name` 这个 label。

**第二，没就绪的 Pod 也会写进 EndpointSlice，只是 `ready: false`。**

```go
// staging/src/k8s.io/endpointslice/utils.go
serving := endpointutil.IsPodReady(pod)
terminating := pod.DeletionTimestamp != nil
// For compatibility reasons, "ready" should never be "true" if a pod is terminatng, unless
// publishNotReadyAddresses was set.
ready := service.Spec.PublishNotReadyAddresses || (serving && !terminating)
```

被排除在外的只有已经结束的 Pod（`Succeeded`/`Failed`）和还没分到 IP 的 Pod（`ShouldPodBeInEndpoints`）。把就绪判断留给消费方，是 EndpointSlice 比旧版 Endpoints 好的地方：kube-proxy 可以据此在滚动更新时使用 terminating 端点，CoreDNS 也可以通过 `publishNotReadyAddresses` 让 StatefulSet 的 Pod 在就绪前互相发现。

另外，每个 EndpointSlice 默认最多放 100 个端点（`--max-endpoints-per-slice`），大 Service 会拆成多个 slice。Pod 变化时只需要更新其中一个，不用像旧版 Endpoints 那样每次重写整个对象并推给所有节点。这也是 `Endpoints` API 被标记为 legacy 的原因，源码注释写得很直接：

```go
// Endpoints is a legacy API and does not contain information about all Service features.
// Use discoveryv1.EndpointSlice for complete information about Service endpoints.
```

---

## 七、把三段串起来

回到第二节那个请求，现在可以把每一步对应到组件上：

```text
Pod 创建 / 就绪状态变化
  └─ EndpointSlice controller   监听 Pod/Service/Node → 写 EndpointSlice（含 ready 条件）

Service 创建
  ├─ kube-apiserver              分配 ClusterIP
  ├─ kube-proxy（每个节点）      监听 Service/EndpointSlice → kube-ipvs0 绑 IP + IPVS 虚拟服务/real server
  └─ CoreDNS                     监听 Service/EndpointSlice → 内存缓存

应用发起请求 http://my-service/
  1. 读 /etc/resolv.conf → nameserver 是 CoreDNS 的 ClusterIP
  2. DNS 查询包 → 本节点 IPVS → 某个 CoreDNS Pod
  3. CoreDNS kubernetes 插件 → findServices → 返回 my-service 的 ClusterIP
  4. TCP 连接 ClusterIP:80 → 本节点 IPVS → 某个就绪的后端 IP:9376
```

几个由此得出的结论：

- **DNS 解析成功不代表服务可用。** 普通 Service 解析到的是 ClusterIP，即使后端为零也照样能解析。排查时要分开看：`nslookup` 通不通是 CoreDNS 的问题，`curl` 通不通才是后端和 kube-proxy 的问题。
- **两次负载均衡。** DNS 查询本身经过一次 IPVS 转发（到某个 CoreDNS Pod），业务请求又经过一次（到某个业务 Pod）。业务请求的负载均衡发生在内核里，和 DNS 无关，所以客户端缓存 DNS 结果不会让流量全落到一个 Pod 上。headless Service 则相反，它的负载均衡完全依赖客户端怎么用 DNS 返回的多个 IP。
- **这是最终一致的。** 从 Pod 就绪到流量真正进来，要经过 controller 写 EndpointSlice、kube-proxy 被 `minSyncPeriod` 限频同步规则两段延迟。Pod 下线时同理，所以通常会在 `preStop` 里 sleep 几秒，让各节点先把它从规则里摘掉。
- **手工 EndpointSlice 没有健康检查。** 1.3 节的方案里，没有人会替你探测外部地址是否存活。外部 registry 挂了，kube-proxy 仍会把一部分请求转过去。需要的话，可以写一个小 controller 定期探测并更新 `conditions.ready`。

---

## 八、总结

- 让 Pod 用域名访问集群外服务有四种办法：hosts 插件、ExternalName、无 selector Service 加手工 EndpointSlice、以及它的 headless 变体。外部只有 IP、又想要负载均衡时，选无 selector Service。
- Kubernetes 服务发现由三段组成：**CoreDNS 把域名翻译成 IP，kube-proxy 把 ClusterIP 翻译成内核转发规则，EndpointSlice controller 维护每个 Service 的后端列表。** 三者通过 API Server 里的对象解耦，彼此不直接调用。
- CoreDNS 对普通 Service 只返回 ClusterIP，不检查后端；只有 headless Service 和打开 `ignore empty_service` 时才看 EndpointSlice。
- EndpointSlice 里同时包含就绪和未就绪的端点，就绪与否写在 `conditions` 里，由消费方决定怎么用。
- kube-proxy 的 IPVS 模式已被弃用，新集群用 nftables。模式换了，上面这条链路不变。

| 问题 | 先看哪里 |
| --- | --- |
| 域名解析失败 | Pod 的 `/etc/resolv.conf`、CoreDNS 日志、Service 是否存在 |
| 能解析但连不上 | `kubectl get endpointslice -l kubernetes.io/service-name=<svc>` 是否有 `ready: true` 的端点 |
| 有就绪端点但仍不通 | 节点上的 `ipvsadm -Ln` / `nft list ruleset`、kube-proxy 日志 |
| 外部域名解析慢 | `ndots:5` 带来的多次 search 查询 |
| Pod 下线时出现少量 5xx | 规则同步延迟，加 `preStop` 等待 |

*本文源码依据 `k8s.io/kubernetes`（commit `aabdf5a2`）与 `coredns/coredns`（commit `c3218b2`）。部分截图来自更早的版本，函数位置可能有变化，逻辑以文中引用的源码为准。*
