---
title: 'csi 04 CreateVolume 与 DeleteVolume：幂等、容量和错误码'
date: 2026-11-28T18:00:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "CreateVolume", "csi-driver-host-path", "csi-driver-nfs"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "CreateVolume 是 sidecar 调用的第一个有副作用的接口，也是幂等和错误码最集中的地方。本文拆开 hostpath 的容量协商、重名判断与回滚，对照 nfs 的参数校验和按卷加锁，给出每个 gRPC 错误码在什么情况下返回，并说明 ValidateVolumeCapabilities 为什么常被忽略却绕不过去。"
---

[上一篇](/posts/csi-03-state/)看了驱动怎么记住卷。这一篇开始看它怎么建卷：`CreateVolume` 是第一个有副作用的接口，也是规范里幂等要求最重、错误码用得最多的地方。

先把两份实现放在一起看整体节奏：**hostpath 先查重名再建卷，nfs 先按卷名加锁再建目录**。两种顺序都满足规范，但失败路径完全不同。

## 一、hostpath 的 CreateVolume：五段

函数接近两百行，按动作切是五段：

| 段落 | 做什么 |
| --- | --- |
| 能力检查 | `validateControllerServiceRequest` 确认驱动声明了 `CREATE_DELETE_VOLUME` |
| 参数校验 | 名字、access type、容量范围 |
| 加锁 | 进入全局临界区 |
| 重名判断 | 已存在就比较容量和内容源，直接返回 |
| 建卷 | 生成 ID、建目录或块设备、写状态、克隆内容 |

### 1.1 参数校验：三条硬规则

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	// Check arguments
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	caps := req.GetVolumeCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}
```

`Name` 和 `VolumeCapabilities` 都是必填（第 3–9 行），缺一个就 `InvalidArgument`。接着从 access type 里推出两件事：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	if accessTypeBlock && accessTypeMount {
		return nil, status.Error(codes.InvalidArgument, "cannot have both block and mount access type")
	}
```

一个卷要么是块设备、要么是文件系统，不能同时是两者（第 2–4 行）。然后处理容量：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	capacity := int64(req.GetCapacityRange().GetRequiredBytes())
	limit := int64(req.GetCapacityRange().GetLimitBytes())
	if capacity < 0 || limit < 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot have negative capacity")
	}
	if limit > 0 && capacity > limit {
		return nil, status.Error(codes.InvalidArgument, "capacity cannot exceed limit")
	}
	if capacity == 0 && limit > 0 {
		capacity = limit
	}
```

这一段（第 2–11 行）对应规范里 `CapacityRange` 的语义：`required_bytes` 是下限，`limit_bytes` 是上限，0 表示没指定。三个分支分别是负数、上下限矛盾、只给了上限。**`capacity == 0 && limit > 0` 时把上限当需求**，这样只写 `limit` 的调用方也能建出卷。

### 1.2 重名判断：幂等的核心

加锁之后第一件事是按名字查：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	if exVol, err := hp.state.GetVolumeByName(req.GetName()); err == nil {
		// Since err is nil, it means the volume with the same name already exists
		// need to check if the size of existing volume is the same as in new
		// request
		if exVol.VolSize < capacity || (limit > 0 && exVol.VolSize > limit) {
			return nil, status.Errorf(codes.AlreadyExists, "Volume with the same name: %s but with different size already exist", req.GetName())
		}
```

容量不匹配返回 `AlreadyExists`（第 7 行），不是 `InvalidArgument`——**这是规范里唯一一次用「资源已存在」表达「参数冲突」**。为什么不用 `InvalidArgument`？因为对调用方来说，这不是请求格式错了，而是「你要的东西和已有的不一样」，重试同样的请求永远不会成功。

内容源也要检查：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
		if req.GetVolumeContentSource() != nil {
			volumeSource := req.VolumeContentSource
			switch volumeSource.Type.(type) {
			case *csi.VolumeContentSource_Snapshot:
				if volumeSource.GetSnapshot() != nil && exVol.ParentSnapID != "" && exVol.ParentSnapID != volumeSource.GetSnapshot().GetSnapshotId() {
					return nil, status.Error(codes.AlreadyExists, "existing volume source snapshot id not matching")
				}
			case *csi.VolumeContentSource_Volume:
				if volumeSource.GetVolume() != nil && exVol.ParentVolID != volumeSource.GetVolume().GetVolumeId() {
					return nil, status.Error(codes.AlreadyExists, "existing volume source volume id not matching")
				}
			default:
				return nil, status.Errorf(codes.InvalidArgument, "%v not a proper volume source", volumeSource)
			}
		}
```

从快照或卷克隆时，**来源 ID 也必须一致**（第 5–13 行），否则同样 `AlreadyExists`。全部检查通过，才返回已有卷（第 17 行之后），并且用**已有的** `VolID` 和 `VolSize`，不是请求里的值。

这一段就是幂等的全部实现：**同一个请求重复到达，第二次一定走进这个分支，返回第一份的结果。**

### 1.3 建卷与回滚

重名判断之后才真正创建：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	volumeID := uuid.NewUUID().String()
	kind := req.GetParameters()[storageKind]
	vol, err := hp.createVolume(volumeID, req.GetName(), capacity, requestedAccessType, false /* ephemeral */, kind)
	if err != nil {
		return nil, err
	}
```

`volumeID` 是新生成的 UUID（第 1 行），**不是复用名字**。名字只用来查重，ID 才是身份的来源。`createVolume` 负责建目录或块设备、写状态，失败直接返回。

从快照克隆时，任何一步失败都要把刚建的卷删掉：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
		if err != nil {
			klog.V(4).Infof("VolumeSource error: %v", err)
			if delErr := hp.deleteVolume(volumeID); delErr != nil {
				klog.V(2).Infof("deleting hostpath volume %v failed: %v", volumeID, delErr)
			}
			return nil, err
		}
```

这是「要么全成、要么全不成」的手写版本（第 4–7 行）。没有回滚，失败后会留下一个空目录和一个指向它的状态记录，下次同名请求会以为卷已存在。

## 二、hostpath 的 DeleteVolume：三条边界

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	volId := req.GetVolumeId()
	vol, err := hp.state.GetVolumeByID(volId)
	if err != nil {
		// Volume not found: might have already deleted
		return &csi.DeleteVolumeResponse{}, nil
	}

	if vol.Attached || !vol.Published.Empty() || !vol.Staged.Empty() {
		msg := fmt.Sprintf("Volume '%s' is still used (attached: %v, staged: %v, published: %v) by '%s' node",
			vol.VolID, vol.Attached, vol.Staged, vol.Published, vol.NodeID)
		if hp.config.CheckVolumeLifecycle {
			return nil, status.Error(codes.Internal, msg)
		}
		klog.Warning(msg)
	}

	if err := hp.deleteVolume(volId); err != nil {
		return nil, fmt.Errorf("failed to delete volume %v: %w", volId, err)
	}
```

三条边界：

1. **卷不存在返回成功**（第 3–6 行）。这是规范里最容易被写错的一条，很多驱动下意识返回 `NOT_FOUND`，导致 sidecar 反复重试；
2. **卷还在用只记日志**（第 8–14 行）。默认 `CheckVolumeLifecycle` 关闭，因为上游有个已知问题会让正常删除流程误报（`--check-volume-lifecycle` 的注释里链了 issue）。打开后返回 `Internal`；
3. **ID 为空返回 `InvalidArgument`**（函数开头）。空 ID 不是「不存在」，是请求本身不合法。

## 三、nfs：顺序不同，错误码也不同

nfs 的 `CreateVolume` 先做参数白名单校验，再按卷名加锁：

```go {linenos=inline}
// pkg/nfs/controllerserver.go
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case paramServer:
		case paramShare:
		case paramSubDir:
		case paramOnDelete:
		case pvcNamespaceKey:
		case pvcNameKey:
		case pvNameKey:
			// no op
		case mountPermissionsField:
			// ...
		case paramUID:
			// ...
		case paramGID:
			// ...
		default:
			return nil, status.Errorf(codes.InvalidArgument, "invalid parameter %q in storage class", k)
		}
	}

	if acquired := cs.Driver.volumeLocks.TryAcquire(name); !acquired {
		return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, name)
	}
	defer cs.Driver.volumeLocks.Release(name)
```

两个和 hostpath 不同的地方：

1. **参数是白名单**（第 21 行）。StorageClass 里写错一个 key 名，直接 `InvalidArgument`，不会静默忽略。hostpath 把 `parameters` 原样塞进 `VolumeContext`，写错了只有到挂载时才报错；
2. **并发冲突返回 `Aborted` 而不是排队**（第 23–25 行）。`TryAcquire` 拿不到锁立刻返回，调用方（provisioner）会重试。这比 hostpath 的全局锁更细：不同卷的创建可以并行。

nfs 没有「重名判断」这一段，因为它靠 ID 定位卷，同名就是同一个 `subDir`，而不是靠状态文件查。

`ValidateVolumeCapabilities` 两边都实现得很简单：

```go {linenos=inline}
// pkg/nfs/controllerserver.go
func (cs *ControllerServer) ValidateVolumeCapabilities(_ context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if err := isValidVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
		Message: "",
	}, nil
}
```

它只校验格式（第 2–7 行），然后**把请求里的 capabilities 原样确认回去**（第 9–13 行）。hostpath 多做了一步「卷必须存在」，nfs 连这一步都省了。**kubelet 在挂载前会调用它**，返回不确认，Pod 就挂不上；返回未实现（`Unimplemented`）也有同样效果。

## 四、错误码对照

两个接口用到的错误码汇总如下。**选错错误码的后果不是「报错信息不好看」，而是 sidecar 的行为完全相反**：

| 错误码 | 什么时候返回 | sidecar 的反应 |
| --- | --- | --- |
| `InvalidArgument` | 必填字段为空、容量为负、上下限矛盾、参数不在白名单 | 不重试，把错误记到事件里 |
| `AlreadyExists` | 同名卷容量或内容源不一致 | 不重试，等人工处理 |
| `Aborted` | 同一个卷已有操作在进行 | **立刻重试**（指数退避） |
| `NotFound` | 查询一个不存在的卷 | 对 `DeleteVolume` 来说**不该出现**，出现了会一直重试 |
| `ResourceExhausted` | 容量超出剩余配额（hostpath 的 `--capacity` 模式） | 退避重试，可能在容量释放后成功 |
| `OutOfRange` | 容量超过 `--max-volume-size` | 不重试 |
| `Internal` | 建目录、写状态等失败 | 退避重试 |
| `Unimplemented` | 驱动没有声明对应能力 | 对应 sidecar 不启动 |

最容易踩的两个坑：

1. **`DeleteVolume` 对不存在的卷返回 `NotFound`**。规范要求返回成功，返回 `NotFound` 会让 provisioner 认为「删除失败」，反复重试；
2. **并发冲突返回 `Internal`**。`Internal` 表示「驱动出错了」，而并发冲突是正常的竞争，`Aborted` 才表示「请重试」。hostpath 用锁排队不会有这个问题，nfs 的 `TryAcquire` 失败必须用 `Aborted`。

## 五、两份实现的选择说明了什么

| 对比项 | hostpath | nfs |
| --- | --- | --- |
| 并发控制 | 全局互斥锁，排队 | 按卷 ID 加锁，失败返回 `Aborted` |
| 重名处理 | 查状态文件，比对容量与内容源 | 不查，靠 ID 定位 |
| 参数校验 | 几乎不校验，原样透传 | 白名单校验 |
| 回滚 | 手写 `deleteVolume` | 依赖挂载与目录操作本身 |
| `ValidateVolumeCapabilities` | 检查卷存在 | 只检查格式 |

**规范给的是行为契约，不是实现模板。** hostpath 把所有并发请求串行化，代价是吞吐，换来的是「不用想并发」；nfs 用细粒度锁，代价是要处理 `Aborted` 重试，换来的是可扩展。

下一篇看控制器侧剩下的三件事：附着、快照、扩容。附着这一项，两份驱动的差别最大——hostpath 实现了但默认关闭，nfs 根本没有。
