---
title: 'csi 09 两份驱动放在一起看：CSI 的共性与取舍'
date: 2026-11-28T18:50:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "csi-driver-host-path", "csi-driver-nfs", "架构"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "同一个规范，两份驱动的接口数量差了三分之一，状态管理方式完全相反，部署形态也一个是单 Pod、一个是标准的两工作负载。把它们并排放，才能分清哪些是规范强制的、哪些是实现自由，以及写自己的驱动时哪些必须抄、哪些不能抄。"
---

前八篇一直在两份驱动之间来回切换。这一篇做总结：**把 hostpath 和 nfs 放在一张表里，逐层看它们的差异，再反推 CSI 规范到底固定了什么。**

## 一、三层差异

差异从外到内分三层，越往里越接近规范的本质。

### 1.1 部署层

| 对比项 | hostpath | nfs |
| --- | --- | --- |
| 工作负载 | 一个 `StatefulSet`，`replicas: 1` | `Deployment`（controller）+ `DaemonSet`（node） |
| 容器数 | 7 个（驱动 + 6 个 sidecar） | controller 5 个、node 3 个 |
| 有没有 attacher | 有，但不工作（驱动没声明能力） | 没有部署 |
| 数据目录 | 宿主机 `/var/lib/csi-hostpath-data/` | NFS 服务端的共享子目录 |
| 可扩展性 | 单节点 | controller 可扩副本 |

hostpath 把 controller 和 node 塞进一个 Pod，原因是数据在本地：**只有所有东西都在同一个节点上才成立**。nfs 的数据在远端，controller 和 node 天然可以分开，部署就回到标准形态。

### 1.2 接口层

| 接口 | hostpath | nfs |
| --- | --- | --- |
| `CreateVolume` / `DeleteVolume` | 实现 | 实现 |
| `ControllerPublishVolume` / `ControllerUnpublishVolume` | 实现，默认关 | `Unimplemented` |
| `CreateSnapshot` / `DeleteSnapshot` / `ListSnapshots` | 实现 | 实现 |
| `ControllerExpandVolume` | 实现 | 只回话 |
| `GetCapacity` | 实现 | `Unimplemented` |
| `NodeStageVolume` / `NodeUnstageVolume` | 实现 | `Unimplemented` |
| `NodePublishVolume` / `NodeUnpublishVolume` | 实现 | 实现 |
| `NodeExpandVolume` | 实现 | `Unimplemented` |
| `NodeGetVolumeHealth` / `NodeGetStorageHealth` | 实现 | 无 |
| 没有实现的方法 | 无（全部实现） | 附着、容量查询、Stage、节点扩容、卷健康 |

**这些 `Unimplemented` 每一个都能从存储模型推出原因**：附着（网络文件系统没有「挂到某台机器」的概念）、Stage（不需要两级挂载）、容量（共享目录没有剩余量）、节点扩容（目录没有配额）、卷健康（共享目录没有单卷故障）。

### 1.3 状态层

| 对比项 | hostpath | nfs |
| --- | --- | --- |
| 状态存在哪 | 本地 `state.json` | 不存，编码在 volume ID 里 |
| 并发控制 | 全局 `sync.Mutex` | 按卷 ID 的 `VolumeLocks`，冲突返回 `Aborted` |
| 幂等实现 | 查状态 + 一把锁 | 查 ID + 细粒度锁 |
| 崩溃恢复 | 读文件 | 无需恢复 |

这是两份驱动最本质的区别。它决定了：**hostpath 只能单副本，nfs 可以多副本。**

## 二、规范固定了什么

把所有差异排掉，剩下的是规范真正强制的东西：

### 2.1 三个 service 必须都存在

即使某个方法不实现，也要返回 `Unimplemented` 而不是让方法不存在——**gRPC 服务必须完整注册**。所以 nfs 里有一堆空实现：

```go {linenos=inline}
// pkg/nfs/nodeserver.go
func (ns *NodeServer) NodeStageVolume(_ context.Context, _ *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
```

这些空实现不是偷懒（第 2–4 行），是「我不做这件事」的明确声明。

### 2.2 能力表必须和实现对齐

三条硬约束：

1. **声明了就必须实现**。声明 `CREATE_DELETE_VOLUME` 却让 `CreateVolume` 返回 `Unimplemented`，provisioner 会一直重试；
2. **没声明就不能被调用**。hostpath 没声明 `PUBLISH_UNPUBLISH_VOLUME`，attacher 就不会调 `ControllerPublishVolume`，返回什么都不影响；
3. **能力决定路径**。`STAGE_UNSTAGE_VOLUME` 决定挂载分几步，`GET_VOLUME_STATS` 决定 kubelet 会不会问用量，`EXPAND_VOLUME` 决定扩容能不能落地。

### 2.3 幂等是行为契约

规范不规定怎么实现幂等，只规定结果：

| 场景 | 必须返回 |
| --- | --- |
| 重复创建同名同参的卷 | 成功 + 同一个卷 |
| 重复创建同名不同参的卷 | `ALREADY_EXISTS` |
| 删除不存在的卷 | 成功 |
| 重复附着到同一节点 | 成功 |
| 重复挂载到同一目标路径 | 成功 |
| 重复扩容到目标容量 | 成功 |

hostpath 和 nfs 用完全不同的方式满足（锁 + 状态 vs ID + 细粒度锁），但**对调用方来说行为一致**。这就是规范的作用。

### 2.4 错误码决定重试策略

| 错误码 | 调用方反应 | 用在什么情况 |
| --- | --- | --- |
| `ALREADY_EXISTS` | 不重试 | 参数冲突，重试也没用 |
| `ABORTED` | 立刻重试 | 并发冲突，稍后就好了 |
| `RESOURCE_EXHAUSTED` | 退避重试 | 容量不足，可能释放 |
| `INVALID_ARGUMENT` | 不重试 | 请求本身有问题 |
| `NOT_FOUND` | 看上下文 | 删除时**不该**返回 |
| `UNIMPLEMENTED` | 不调用 | 能力没声明 |
| `INTERNAL` | 退避重试 | 驱动内部错误 |

**选错错误码的后果不是「信息不好看」，而是重试行为完全相反。** 这是 CSI 驱动最容易出错的地方，也是 `csi-sanity` 检查最严的地方（[10 篇](/posts/csi-10-sanity/)）。

## 三、实现自由在哪

规范留白的地方，两份驱动做了不同选择：

| 自由度 | hostpath 的选择 | nfs 的选择 | 更好的做法 |
| --- | --- | --- | --- |
| 状态存哪 | 本地 JSON | volume ID 编码 | 写进 PV 的 `volumeAttributes` 或后端 |
| 并发控制 | 全局锁 | 按卷锁 | 按卷锁 + 重试 |
| 快照实现 | `tar` / `cp` | `tar` 打包 | 后端原生快照 |
| 统计缓存 | 无 | 10 分钟 | 按后端开销决定 |
| 卷健康 | 标记文件 | 不实现 | 查后端 |
| 挂载跳数 | 两步（名义 Stage） | 一步 | 按数据源决定 |

**判断一个选择好不好，标准是「它和后端的性质匹配吗」**：

1. 后端是本地磁盘 → 状态存本地、单副本、两步挂载，都合理；
2. 后端是网络存储 → 无状态、可扩副本、一步挂载，也合理；
3. 后端是分布式块存储 → 需要 attach、需要 Stage、状态写后端，又是另一套。

## 四、不能照抄的地方

hostpath 是官方示例，注释里自己写了「不是生产最佳实践」。具体有五处：

1. **`state.json` 全量覆盖写**（[03 篇](/posts/csi-03-state/)）。写盘过程中崩溃会损坏文件，真实驱动要用原子写或后端 API；
2. **全局 `sync.Mutex`**。所有修改串行，吞吐上不去；
3. **`createVolume` 的容量取整不检查上限**。源码注释承认：`a more robust driver would ensure any rounding does not exceed limit`；
4. **`CheckVolumeLifecycle` 默认关闭**。卷还在用也能删，靠上游 issue 挡着；
5. **`NodeStageVolume` 不挂载**。staging 路径只是记账，`mount | grep globalmount` 什么也没有。

**每一处都在文章里标过「真实驱动会怎么做」**，写自己驱动时对着改。

## 五、可以照抄的地方

反过来，有些结构值得直接学：

1. **`logGRPC` 拦截器**（[02 篇](/posts/csi-02-process/)）。一行日志定位请求，`Probe` 降级、错误无条件打，是排查的基础；
2. **能力检查放在方法第一行**。每个方法开头 `validateXxxServiceRequest`，一眼看出它属于哪条能力；
3. **错误码逐条对照规范**。hostpath 的每个 `status.Error` 都能在规范里找到依据，这份对照关系本身就是文档；
4. **`endpoint.Listen` 先删旧 socket**。容器重启时不做这一步会 `address already in use`；
5. **`VolumeLocks` 的 `TryAcquire` + `defer Release` 模式**。nfs 用二十行实现的按卷锁，可以直接搬。

## 六、选型建议

最后落到实际问题上：**如果我要写一个 CSI 驱动，该从哪份代码开始？**

| 你的后端 | 起点 | 参考哪份 |
| --- | --- | --- |
| 本地磁盘、单节点 | hostpath | 全部 |
| NFS / SMB 等网络文件系统 | nfs | 状态层 + Publish |
| 云厂商块存储 | nfs 的骨架 + hostpath 的 attach | Controller 侧两份都看 |
| 分布式文件系统 | nfs | 状态层 + VolumeLocks |
| 对象存储（用 FUSE 挂载） | nfs + hostpath 的统计 | 两份都要 |

**共同的第一步是同一个**：先把能力表定下来。声明哪些能力，就决定了你要实现哪些方法、哪些 sidecar 会工作、kubelet 走哪条路径。**能力表定了，代码结构基本就定了**——这也是为什么[01 篇](/posts/csi-01-spec/)要把它放在最前面。

下一节用 `csi-sanity` 验收这两份驱动，看规范检查器会挑出哪些毛病。
