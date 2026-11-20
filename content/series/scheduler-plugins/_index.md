---
title: scheduler-plugins 逐个拆解
description: 跟着一个 Pod 的调度旅程，按执行顺序逐个读完 kube-scheduler 的 24 个 in-tree 插件和 scheduler-plugins 的 16 个 out-of-tree 插件。
---

kube-scheduler 的调度语义全部写在插件里。这个系列不再按「过滤类、打分类」分组概述，而是跟着一个 Pod 从入队到绑定的顺序，每个插件单独一篇，读到源码层面：CycleState 里存了什么、返回哪种失败码、分数怎么算、什么事件会让被拒绝的 Pod 重新入队。前 24 篇是随 Kubernetes 发布的 in-tree 插件，后 16 篇是 sigs.k8s.io/scheduler-plugins 中的 out-of-tree 插件。
