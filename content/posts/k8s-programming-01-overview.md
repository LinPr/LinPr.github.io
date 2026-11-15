---
title: 'Kubernetes 编程（一）：Kubernetes 是一个可以编程的平台'
date: 2026-11-15T21:00:00+08:00
draft: false
tags: ["kubernetes", "client-go", "扩展"]
categories: ["Kubernetes"]
series: ["kubernetes-programming"]
author: "LinPr"
description: "大多数人只把 Kubernetes 当成一个用 kubectl 操作的容器编排系统。本文是 Kubernetes 编程系列的开篇：什么是 Kubernetes 编程，Kubernetes 在哪些层面留了扩展点，以及这个系列打算按什么顺序讲。"
---

开发和运维同学平时使用 Kubernetes，大部分时间是在用 `kubectl` 操作资源：`apply` 一份 YAML，`get` 看看状态，`logs` 查一下问题。这个用法没有任何问题，但只看到了 Kubernetes 的一面。

Kubernetes 从一开始就被设计成一个**可以被编程、被扩展的平台**。它的核心只定义接口和机制，具体能力尽量交给插件和外部组件实现。这个系列想从这个角度重新认识它。

## 一、厂商中立：为什么 Kubernetes 到处都是接口

CNCF 对 Kubernetes 的定位是一个**厂商中立**的容器编排系统。所谓厂商中立，最典型的例子是存储。

早期 Kubernetes 支持 AWS EBS、GCE PD、Ceph RBD 等存储，是把这些存储的代码直接写在 Kubernetes 主仓库里的（所谓 in-tree volume plugin）。这样每接入一种存储，都要改 Kubernetes 本身、等它发版；存储厂商的 bug 也会变成 Kubernetes 的 bug。

后来社区定义了 CSI（Container Storage Interface）。Kubernetes 只负责按规范调用 gRPC 接口，具体怎么创建卷、怎么挂载，交给各家存储实现的驱动。in-tree 插件逐步迁移到 CSI，随后从主仓库中移除。

存储不是孤例：

| 领域 | 接口 | Kubernetes 只负责 | 由谁实现 |
| --- | --- | --- | --- |
| 容器运行时 | CRI | 通过 gRPC 让运行时创建、启动容器 | containerd、CRI-O |
| 网络 | CNI | 在 Pod 创建、删除时调用插件 | Calico、Cilium、Flannel |
| 存储 | CSI | 在卷的生命周期各阶段调用 gRPC | 各存储厂商的驱动 |

这种「核心只管调用，实现交给插件」的模式，贯穿了 Kubernetes 的各个层面。

## 二、什么是 Kubernetes 编程

在我看来，可以从两个层面理解。

**狭义上：用程序操作 Kubernetes 资源。** 通过官方客户端库（Go 语言里是 client-go），在程序里对 Pod、Deployment、Service 做增删改查。这和 `kubectl` 做的事本质上一样，只是换成了代码，可以和业务逻辑结合：比如发布系统按灰度比例调整副本数，或者巡检程序扫描所有命名空间里没设资源限制的 Pod。

**广义上：利用 Kubernetes 的设计理念，在各个层面扩展它。** Kubernetes 在几乎每个环节都预留了扩展点：

| 想扩展的东西 | 扩展点 | 典型例子 |
| --- | --- | --- |
| 操作资源 | client-go、dynamic client | 发布系统、巡检工具 |
| 命令行工具 | kubectl 插件 | `kubectl ns`、krew 上的各种插件 |
| 自动化运维 | 控制器 / Operator | 各类数据库 Operator、cert-manager |
| 资源类型 | CRD（CustomResourceDefinition） | Prometheus Operator 的 `ServiceMonitor` |
| API Server 本身 | API Aggregation | metrics-server 提供的 `metrics.k8s.io` |
| 准入控制 | Admission Webhook、ValidatingAdmissionPolicy | Kyverno、OPA Gatekeeper |
| 调度 | Scheduling Framework 插件、多调度器 | scheduler-plugins |
| 运行时、网络、存储 | CRI、CNI、CSI | containerd、Cilium、各类 CSI 驱动 |

越往下越硬核。CRI、CNI 很少需要自己实现，但理解它们的工作方式，在排查问题和评估开源项目时会很有用。

两个层面并不冲突。日常工作中接触最多的可能还是增删改查，但如果理解了这些扩展机制，在调研或引入云原生开源项目时，就能看清它在 Kubernetes 体系里处在哪个位置、靠哪种机制工作。读这些项目的源码，也能学到很多值得借鉴的设计。

## 三、这些扩展点有一个共同的底座

上面的扩展点看起来五花八门，但除了 CRI、CNI、CSI 这几个运行在节点上的接口，几乎都建立在同两件事上：

1. **一切皆资源，资源通过 RESTful API 暴露。** `kubectl`、client-go、控制器、Operator，最终都是在调 API Server 的 HTTP 接口。CRD 能扩展 Kubernetes，正是因为新类型的资源也会得到一套同样风格的 API。
2. **控制器模式。** 一个程序监听资源变化，对比期望状态和实际状态，然后采取行动，让两者趋于一致。Deployment、Service 背后是控制器，Operator 也是控制器，调度器本质上也是一个特殊的控制器。

所以这个系列会先把这两块打扎实：先讲资源和客户端库，再讲控制器，后面的 CRD、Operator、webhook 就是在这个底座上加东西。

## 四、系列规划

这个系列会按循序渐进的顺序写，目前规划如下（后续可能调整）：

1. 概论（本文）
2. 使用 client-go 操作 Kubernetes 资源
3. 编写 kubectl 插件
4. 理解控制器与 informer
5. 用 CRD 扩展 Kubernetes 资源
6. 用 Operator 实现运维自动化
7. 编写准入 webhook
8. 编写自定义调度器
9. 编写 CSI 存储驱动

部分主题已经有单独的文章，可以先看：

- 调度器：[kube-scheduler 系列](/series/kube-scheduler/)，从 scheduling framework 讲到 in-tree 和 out-of-tree 插件；
- 存储：[Kubernetes CSI：一个 PVC 是怎么变成容器里的目录的](/posts/k8s-csi-workflow/)；
- 控制器的一个完整实例：[Kubernetes 服务发现：从一个域名到一条 IPVS 规则](/posts/k8s-service-discovery/)，里面的 EndpointSlice controller、kube-proxy、CoreDNS 都是控制器模式的应用。

由于经验有限，有些地方的理解不一定深入。这个系列的目标是先把目前的理解系统地整理出来，以后认识更深了，再回来修正。学习本来就是一个不断推翻旧认知、再建立新认知的过程，就像软件开发里迭代往往比瀑布更有效：先有一个不完美的开始，再一点点改进。
