---
title: kube-scheduler 调度器源码笔记
description: 从插件式架构讲起，逐层拆开 kube-scheduler 的框架、内置插件与社区 out-of-tree 扩展。
---

一个调度器要回答三个问题：什么时候调用插件、插件看到什么数据、插件说完之后谁来兜底。这个系列按这个顺序读下来，最后能对着源码说清楚一个 Pod 是怎么被放到节点上的。
