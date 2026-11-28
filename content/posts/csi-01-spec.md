---
title: 'csi 01 CSI 规范：能力协商决定了谁在什么时候调用你'
date: 2026-11-28T17:30:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "csi-spec"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "CSI 规范把驱动要提供的能力分成三个 service，又用三张能力表让驱动自己声明实现了哪些。本文列出每个 service 的方法与调用方，逐张拆开 Plugin、Controller、Node 能力表，并说明 hostpath 驱动默认声明了什么、打开某个开关后谁会开始工作，以及哪些方法必须幂等。"
---

[上一篇](/posts/csi-00-journey/)的时序图里，驱动只做一件事：响应 gRPC 请求。但请求从哪来、有哪些、什么时候来，全由规范里的两张东西决定：**方法的归属**，和**驱动自己声明的能力**。

这一篇把规范里读代码需要的那部分抽出来：三个 service 各有哪些方法、谁调用它们、三张能力表各自控制什么、哪些方法必须幂等。后面每一篇讲到某个接口时，都会回指这里的表。

## 一、三个 service，一句话说清各自管什么

| service | 管什么 | 部署在哪 |
| --- | --- | --- |
| `Identity` | 我是谁、我还活着吗、我支持哪些能力 | 每个驱动进程都要实现 |
| `Controller` | 建卷、删卷、快照、扩容、附着 | 集群里只需要一份 |
| `Node` | 把卷挂到本节点、挂进容器、报告用量 | 每个节点各一份 |

`Identity` 是唯一一个「必须实现、但没有业务逻辑」的 service。它存在的意义全在 `GetPluginCapabilities`：kubelet 里的 CSI 插件在启动时会先问一遍，拿到答案才知道这个驱动是完整的还是只有 node 侧。

## 二、方法清单与调用方

规范里的方法并不都需要实现，缺少的那部分通过能力表说明。下表按 service 分组，标出每个方法由谁调用：

| service | 方法 | 调用方 |
| --- | --- | --- |
| Identity | `GetPluginInfo`、`GetPluginCapabilities`、`Probe` | kubelet、sidecar、liveness-probe |
| Controller | `CreateVolume`、`DeleteVolume` | external-provisioner |
| | `ControllerPublishVolume`、`ControllerUnpublishVolume` | external-attacher |
| | `CreateSnapshot`、`DeleteSnapshot`、`ListSnapshots`、`GetSnapshot` | external-snapshotter |
| | `ControllerExpandVolume` | external-resizer |
| | `ControllerGetVolume`、`ListVolumes`、`GetCapacity` | 运维工具、调度器（配合容量追踪） |
| | `ValidateVolumeCapabilities` | kubelet，在挂载前确认卷支持请求的访问模式 |
| | `ControllerModifyVolume` | external-resizer（可变参数特性） |
| | `ControllerGetCapabilities` | 所有 controller 侧 sidecar，启动时问一遍 |
| Node | `NodeStageVolume`、`NodeUnstageVolume` | kubelet |
| | `NodePublishVolume`、`NodeUnpublishVolume` | kubelet |
| | `NodeGetInfo`、`NodeGetCapabilities` | kubelet，以及 node-driver-registrar |
| | `NodeGetVolumeStats` | kubelet，定期收集用量 |
| | `NodeExpandVolume` | kubelet，扩容后调整文件系统 |
| | `NodeGetVolumeHealth`、`NodeGetStorageHealth` | kubelet，配合卷健康特性 |

表里能看出一条规律：**controller 侧的方法几乎都由 sidecar 调用，node 侧的方法几乎都由 kubelet 调用。** 记住这条，就不会再问「驱动为什么不自己 watch PVC」——它没有客户端身份，也没有 watch 的代码。

## 三、三张能力表，各自控制谁

规范用三张独立的表描述「这个驱动支持什么」，它们回答的问题完全不同：

| 能力表 | 返回方法 | 控制谁 |
| --- | --- | --- |
| `PluginCapability` | `GetPluginCapabilities` | kubelet、所有 sidecar；决定这个驱动有没有 controller 侧 |
| `ControllerServiceCapability` | `ControllerGetCapabilities` | 各个 controller sidecar；决定自己要不要工作 |
| `NodeServiceCapability` | `NodeGetCapabilities` | kubelet；决定挂载流程走几步 |

三张表里最关键的是第一张。`CONTROLLER_SERVICE` 没有声明，kubelet 会把整个驱动当成「只有 node 侧」，controller 的 sidecar 全部不工作。`VOLUME_ACCESSIBILITY_CONSTRAINTS` 没有声明，调度器就不会把拓扑信息传给驱动，`CreateVolume` 里的 `requisite` 也会是空的。

第二张表的作用更直接：sidecar 启动时先调 `ControllerGetCapabilities`，发现自己需要的能力不在列表里就不启动对应的处理逻辑。`external-attacher` 就是这么判断的——驱动没声明 `PUBLISH_UNPUBLISH_VOLUME`，它连 `VolumeAttachment` 都不会去 watch。

第三张表决定挂载分几步。声明了 `STAGE_UNSTAGE_VOLUME`，kubelet 会先 `NodeStageVolume` 再 `NodePublishVolume`；没声明，就只有 `NodePublishVolume` 一步。

## 四、hostpath 声明了什么

把这三张表和代码对上，先看 `GetPluginCapabilities`：

```go {linenos=inline}
// pkg/hostpath/identityserver.go
func (hp *hostPath) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	klog.V(5).Infof("Using default capabilities")
	caps := []*csi.PluginCapability{
		{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
				},
			},
		},
		{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_GROUP_CONTROLLER_SERVICE,
				},
			},
		},
	}
	if hp.config.EnableTopology {
		// 追加 VOLUME_ACCESSIBILITY_CONSTRAINTS
	}
	if hp.config.EnableSnapshotMetadata {
		// 追加 SNAPSHOT_METADATA_SERVICE
	}
	return &csi.GetPluginCapabilitiesResponse{Capabilities: caps}, nil
}
```

无条件声明的只有两条（第 6–18 行）：

1. `CONTROLLER_SERVICE`：这个驱动有 controller 侧，sidecar 可以工作；
2. `GROUP_CONTROLLER_SERVICE`：支持卷组快照，这是[规范 1.13](https://github.com/container-storage-interface/spec) 新增的一组接口。

`VOLUME_ACCESSIBILITY_CONSTRAINTS` 要看 `EnableTopology`，`SNAPSHOT_METADATA_SERVICE` 要看 `EnableSnapshotMetadata`。**声明和实现必须对齐**：没声明拓扑能力却在 `NodeGetInfo` 里返回拓扑键，kubelet 会忽略它们。

再看 controller 侧和 node 侧实际声明的能力，以及它们带来的连锁反应：

| 能力 | 声明后谁开始工作 |
| --- | --- |
| `CREATE_DELETE_VOLUME` | external-provisioner 处理 PVC |
| `PUBLISH_UNPUBLISH_VOLUME` | external-attacher 处理 `VolumeAttachment`；**hostpath 默认不声明**（`--enable-attach` 默认 false） |
| `CREATE_DELETE_SNAPSHOT` | external-snapshotter 处理 `VolumeSnapshot` |
| `EXPAND_VOLUME` | external-resizer 处理扩容；`--enable-volume-expansion` 默认 true |
| `GET_CAPACITY`、`GET_VOLUME`、`LIST_VOLUMES` | 供调度器和运维工具查询 |
| `STAGE_UNSTAGE_VOLUME`（node 侧） | kubelet 先 Stage 再 Publish，分两步 |
| `GET_VOLUME_STATS`（node 侧） | kubelet 定期调 `NodeGetVolumeStats` |

这张表最好和 00 篇的部署清单对着看：`csi-attacher` 容器在清单里，但因为驱动没有声明对应能力，它实际什么都不做。**清单里的 sidecar 未必都在干活，要看能力表。**

## 五、哪些方法必须幂等

规范对修改类的方法是硬要求：**调用方可能重试，驱动必须保证重复调用不产生第二份副作用。** 具体到几个最容易写错的：

| 方法 | 幂等要求 |
| --- | --- |
| `CreateVolume` | 同名、同参数重复调用返回同一个卷；同名但参数不同返回 `ALREADY_EXISTS` |
| `DeleteVolume` | 卷不存在时返回成功，不是 `NOT_FOUND` |
| `ControllerPublishVolume` | 已附着到同一节点时返回成功 |
| `NodeStageVolume`、`NodePublishVolume` | 已经挂载在同一目标路径时返回成功 |
| `CreateSnapshot`、`DeleteSnapshot` | 同 `CreateVolume`、`DeleteVolume` |
| `ControllerExpandVolume`、`NodeExpandVolume` | 已经扩到目标容量时返回成功 |

幂等不是靠「查一下再决定」就能完成的：两个请求可能同时到达。hostpath 用一把全局锁把建卷、删卷串起来，第 03、04 篇会看具体做法。

## 六、规范怎么被验证

规范里的这些要求没法靠读文档确认自己写对了。官方的做法是 `csi-sanity`：它对着驱动的 socket 跑一遍，检查必填字段、错误码、幂等行为，比如「重复创建同名卷应该返回什么」「删除不存在的卷应该成功还是报错」。

第 10 篇会用它跑一遍 hostpath，看它到底检查了哪些规则。现在只需要记住：**这份规范是可以被机械验证的**，写驱动时不要凭印象。

下一节开始进代码：先看驱动进程怎么启动、怎么把三个 service 挂到同一个 socket 上。
