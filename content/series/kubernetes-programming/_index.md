---
title: Kubernetes 编程
description: 从 client-go 操作资源开始，逐步扩展到 kubectl 插件、控制器、CRD 与 Operator、准入 webhook、自定义调度器和 CSI 驱动。
---

大多数人接触 Kubernetes 是从 `kubectl` 开始的。这个系列换一个角度：把 Kubernetes 当成一个可以编程、可以扩展的平台，按「操作资源 → 扩展命令行 → 理解控制器 → 扩展资源与控制面」的顺序循序渐进，每一篇都尽量落到源码上。系列会持续更新。
