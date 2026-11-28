---
title: 'csi 05 附着、快照与扩容：控制器侧另外三件事'
date: 2026-11-28T18:10:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "VolumeAttachment", "快照", "扩容", "csi-driver-nfs"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "控制器侧除了建卷删卷，还有附着、快照、扩容和容量查询四组接口。hostpath 全实现但默认关掉附着，nfs 则连附着和 Node 侧扩容都直接返回 Unimplemented。本文把两组实现放在一起看：能力表决定了 sidecar 会不会调用，而实现里的每个错误码都对应一种重试策略。"
---

[上一篇](/posts/csi-04-create-delete/)看完了建卷和删卷。控制器侧还剩四组接口：附着、快照、扩容、容量查询。这一篇把它们放在一起看，因为**它们的实现长度和错误码选择，几乎完全由能力表决定**——声明了才有人调用，没声明就返回 `Unimplemented`。

## 一、附着：hostpath 实现了但默认关闭

`ControllerPublishVolume` 的第一行就是能力检查：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
func (hp *hostPath) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if !hp.config.EnableAttach {
		return nil, status.Error(codes.Unimplemented, "ControllerPublishVolume is not supported")
	}

	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}
```

`EnableAttach` 来自 `--enable-attach`，默认 `false`（[02 篇](/posts/csi-02-process/)的参数表）。所以默认配置下，这个方法永远返回 `Unimplemented`，而 attacher 因为驱动没声明 `PUBLISH_UNPUBLISH_VOLUME`，根本不会调用它——**两边的默认值是一致的，不会出现「调用了但没实现」**。

打开之后，逻辑很短：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	if req.NodeId != hp.config.NodeID {
		return nil, status.Errorf(codes.NotFound, "Not matching Node ID %s to hostpath Node ID %s", req.NodeId, hp.config.NodeID)
	}

	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	vol, err := hp.state.GetVolumeByID(req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Check to see if the volume is already published.
	if vol.Attached {
		// Check if readonly flag is compatible with the publish request.
		if req.GetReadonly() != vol.ReadOnlyAttach {
			return nil, status.Error(codes.AlreadyExists, "Volume published but has incompatible readonly flag")
		}

		return &csi.ControllerPublishVolumeResponse{
			PublishContext: map[string]string{},
		}, nil
	}
```

四个判断，每个都对应规范里的一条：

1. **节点必须是自己**（第 2–3 行）。hostpath 只有一个节点，`NodeId` 不匹配就是 `NotFound`——不是 `InvalidArgument`，因为请求本身合法，只是这个驱动没有那个节点；
2. **卷必须存在**（第 8–11 行）。不存在返回 `NotFound`；
3. **重复附着要幂等**（第 14–22 行）。已经附着且只读标志一致，返回成功和空的 `PublishContext`；
4. **只读标志不一致返回 `AlreadyExists`**（第 16–18 行）。和建卷时「同名不同参数」是同一个思路：重试永远不会成功，但也不是请求格式错误。

最后还有一条容量上限：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	// Check attach limit before publishing.
	if hp.config.AttachLimit > 0 && hp.getAttachCount() >= hp.config.AttachLimit {
		return nil, status.Errorf(codes.ResourceExhausted, "Cannot attach any more volumes to this node ('%s')", hp.config.NodeID)
	}

	vol.Attached = true
	vol.ReadOnlyAttach = req.GetReadonly()
	if err := hp.state.UpdateVolume(vol); err != nil {
		return nil, err
	}
```

`--attach-limit` 控制单节点最多附着多少个卷（第 2–4 行），超了返回 `ResourceExhausted`——这是「暂时不行，等别的卷卸载后重试」的语义，attacher 会退避重试。**真正的 attach 动作就是改两个字段**（第 6–7 行），因为 hostpath 没有后端设备可操作。

`ControllerUnpublishVolume` 的边界更宽松：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	// Empty node id is not a failure as per Spec
	if req.NodeId != "" && req.NodeId != hp.config.NodeID {
		return nil, status.Errorf(codes.NotFound, "Node ID %s does not match to expected Node ID %s", req.NodeId, hp.config.NodeID)
	}
```

**空的 `NodeId` 是合法的**（第 2–3 行）。规范允许调用方不指定节点就解除附着，意思是「不管挂在哪都卸掉」。这条很容易漏，漏了会让 attacher 在节点已删除时一直重试。

### 1.1 nfs：根本没有这个方法

```go {linenos=inline}
// pkg/nfs/controllerserver.go
func (cs *ControllerServer) ControllerPublishVolume(_ context.Context, _ *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ControllerUnpublishVolume(_ context.Context, _ *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
```

两个方法都是空实现（第 2–7 行）。原因在存储模型：**NFS 是网络文件系统，没有「附着」这个动作**。挂载就是 `mount`，任何节点都能随时挂同一个共享目录，不需要先在服务器上登记。

所以 nfs 的能力表里没有 `PUBLISH_UNPUBLISH_VOLUME`，attacher 部署了也不会工作。这也是为什么 nfs 的部署清单里通常没有 `csi-attacher`。

## 二、快照：复制还是打包

hostpath 的快照是**复制**。先查重名：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	if exSnap, err := hp.state.GetSnapshotByName(req.GetName()); err == nil {
		// Since err is nil, it means the snapshot with the same name already exists need
		// to check if the sourceVolumeId of existing snapshot is the same as in new request.
		if exSnap.VolID == req.GetSourceVolumeId() {
			// same snapshot has been created.
			return &csi.CreateSnapshotResponse{ /* ... */ }, nil
		}
		return nil, status.Errorf(codes.AlreadyExists, "snapshot with the same name: %s but with different SourceVolumeId already exist", req.GetName())
	}
```

和 `CreateVolume` 的结构完全一样（第 1–9 行）：同名同源返回已有快照，同名不同源返回 `AlreadyExists`。**快照的幂等规则和卷是同一条。**

真正的复制只有两个命令：

```go {linenos=inline}
// pkg/hostpath/hostpath.go
func (hp *hostPath) createSnapshotFromVolume(vol state.Volume, file string, opts ...string) error {
	var args []string
	var cmdName string
	if vol.VolAccessType == state.BlockAccess {
		klog.V(4).Infof("Creating snapshot of Raw Block Mode Volume")
		cmdName = "cp"
		args = []string{vol.VolPath, file}
	} else {
		klog.V(4).Infof("Creating snapshot of Filesystem Mode Volume")
		cmdName = "tar"
		opts = append(
			[]string{"czf", file, "-C", vol.VolPath},
			opts...,
		)
		args = []string{"."}
	}
	executor := utilexec.New()
	optsAndArgs := append(opts, args...)
	out, err := executor.Command(cmdName, optsAndArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed create snapshot: %w: %s", err, out)
	}

	return nil
}
```

块设备用 `cp`（第 4–8 行），文件系统用 `tar czf`（第 9–16 行）。**快照是同步完成的**，所以 `CreateSnapshot` 直接写 `ReadyToUse = true`（`controllerserver.go` 里构造 `state.Snapshot` 的地方）。

规范允许异步：当后端打快照需要几分钟时，可以先返回 `ReadyToUse: false`，snapshotter 会轮询 `GetSnapshot` 或 `ListSnapshots` 等它就绪。hostpath 不需要，因为它只是复制文件。

nfs 的快照同样用 tar，但走的是「挂载源卷 → 打包到快照目录」的流程：

```go {linenos=inline}
// pkg/nfs/controllerserver.go
	srcPath, err := getInternalVolumePath(cs.Driver.workingMountDir, srcVol)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	dstPath := filepath.Join(snapInternalVolPath, snapshot.archiveName(cs.Driver.enableSnapshotCompression))

	klog.V(2).Infof("tar %v -> %v", srcPath, dstPath)
	limits := cs.Driver.snapshotTarLimits()
	if cs.useTarCommandForSnapshot(limits) {
		var tarArgs []string
		if cs.Driver.enableSnapshotCompression {
			tarArgs = []string{"-C", srcPath, "-czvf", dstPath, "."}
		} else {
			tarArgs = []string{"-C", srcPath, "-cvf", dstPath, "."}
		}
		if out, err := exec.Command("tar", tarArgs...).CombinedOutput(); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create archive for snapshot: %v: %v", err, string(out))
		}
	} else {
		if err := TarPack(srcPath, dstPath, cs.Driver.enableSnapshotCompression, limits); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create archive for snapshot: %v", err)
		}
	}
```

三个和 hostpath 不同的地方：

1. **先拿到源卷的本地路径**（第 1 行）。nfs 的卷在远端，必须 mount 到本地临时目录才能读；
2. **两条打包路径**（第 9–23 行）。设了大小限制时用内置的 `TarPack`，没有限制时直接调系统的 `tar` 命令——因为内置实现无法在打包过程中中断超大归档；
3. **压缩是可选的**（第 11–15 行的 `enableSnapshotCompression`）。hostpath 写死 `czf`，nfs 可以选 `cvf`。

## 三、扩容：hostpath 真扩，nfs 只回话

hostpath 的扩容就是改一个数字：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	capacity := int64(capRange.GetRequiredBytes())
	if capacity > hp.config.MaxVolumeSize {
		return nil, status.Errorf(codes.OutOfRange, "Requested capacity %d exceeds maximum allowed %d", capacity, hp.config.MaxVolumeSize)
	}

	// Lock before acting on global state. A production-quality
	// driver might use more fine-grained locking.
	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	exVol, err := hp.state.GetVolumeByID(volID)
	if err != nil {
		return nil, err
	}

	if exVol.VolSize < capacity {
		exVol.VolSize = capacity
		if err := hp.state.UpdateVolume(exVol); err != nil {
			return nil, err
		}
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         exVol.VolSize,
		NodeExpansionRequired: !hp.config.DisableNodeExpansion,
	}, nil
}
```

三个细节：

1. **只会变大**（第 16 行）。请求的容量小于现状时直接返回现状，不报错——规范要求扩容是幂等的，重复请求同一个目标容量不该失败；
2. **返回实际容量**（第 24 行）。可能是 `MaxVolumeSize` 截断后的值，调用方以它为准；
3. **`NodeExpansionRequired`**（第 25 行）。为 `true` 时 kubelet 还要再调 `NodeExpandVolume` 去扩文件系统，hostpath 默认 `true`，但它的 `NodeExpandVolume` 只做 `resize2fs` 之外的事（[07 篇](/posts/csi-07-node-rest/)会讲）。

nfs 的扩容**什么都不做**：

```go {linenos=inline}
// pkg/nfs/controllerserver.go
	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	klog.V(2).Infof("ControllerExpandVolume(%s) successfully, currentQuota: %d bytes", req.VolumeId, volSizeBytes)

	return &csi.ControllerExpandVolumeResponse{CapacityBytes: req.GetCapacityRange().GetRequiredBytes()}, nil
}
```

只是把请求的容量原样返回（第 4 行）。因为 NFS 目录没有配额概念，PV 的大小只影响调度，不影响实际能写多少。它的 `NodeExpandVolume` 也直接 `Unimplemented`（`pkg/nfs/nodeserver.go`），而 node 侧的能力表里确实没有 `EXPAND_VOLUME`。

**能力表在这里又一次把「实现」和「调用」对齐了**：controller 侧声明了 `EXPAND_VOLUME`，所以 `ControllerExpandVolume` 会被调用；node 侧没声明，所以 kubelet 不会调 `NodeExpandVolume`。

## 四、容量查询：只在配置了才准

`GetCapacity` 返回某个存储类的剩余容量，调度器（配合容量追踪特性）和运维工具会用它：

```go {linenos=inline}
// pkg/hostpath/controllerserver.go
	available := hp.config.MaxVolumeSize
	if hp.config.Capacity.Enabled() {
		// Empty "kind" will return "zero capacity". There is no fallback
		// to some arbitrary kind here because in practice it always should
		// be set.
		kind := req.GetParameters()[storageKind]
		quantity := hp.config.Capacity[kind]
		allocated := hp.sumVolumeSizes(kind)
		available = quantity.Value() - allocated
	}
	maxVolumeSize := hp.config.MaxVolumeSize
	if maxVolumeSize > available {
		maxVolumeSize = available
	}
```

两个分支（第 2–9 行）：

1. **没配 `--capacity`**：剩余容量直接报 `MaxVolumeSize`，等于告诉调用方「有多少给多少」，这个数字没有实际意义；
2. **配了 `kind=quantity`**：用 `quantity - 已分配` 算剩余，`kind` 从 StorageClass 参数里取。**空的 `kind` 返回 0**，注释解释了原因——真实使用里一定会带，不做兜底反而能暴露配置错误。

nfs 的 `GetCapacity` 是 `Unimplemented`。原因和扩容一样：NFS 服务器不知道「剩余多少」，整个共享是无限的，只受服务器磁盘限制。

## 五、两组实现的对照

| 接口 | hostpath | nfs | 差异根源 |
| --- | --- | --- | --- |
| `ControllerPublishVolume` | 实现，默认关闭 | `Unimplemented` | 网络文件系统没有附着概念 |
| `ControllerUnpublishVolume` | 实现，空 `NodeId` 合法 | `Unimplemented` | 同上 |
| `CreateSnapshot` | 复制，同步就绪 | tar 打包，可压缩、可限流 | 块/文件卷 vs 共享目录 |
| `DeleteSnapshot` | 删文件 | 删归档 | 同上 |
| `ControllerExpandVolume` | 改 `VolSize`，只增不减 | 原样回话 | NFS 无配额 |
| `NodeExpandVolume` | 实现 | `Unimplemented` | node 侧没声明能力 |
| `GetCapacity` | 按 `kind` 算剩余 | `Unimplemented` | 共享目录没有「剩余」 |

三条规律值得记住：

1. **没声明的能力返回 `Unimplemented` 是正确做法**，不是偷懒。返回 `InvalidArgument` 或 `NotFound` 会让 sidecar 以为出了别的问题；
2. **幂等体现在「只增不减」「已附着就返回成功」「同名同源返回已有快照」这些地方**，每一条都对应一次可能的重试；
3. **错误码决定重试策略**：`AlreadyExists` 不重试、`Aborted` 立刻重试、`ResourceExhausted` 退避重试、`Unimplemented` 不调用。选错一个，行为就完全相反。

控制器侧到此结束。下一篇转到节点侧：`NodeStageVolume` 和 `NodePublishVolume` 这两步，是怎么把一个卷变成容器里的挂载点的。
