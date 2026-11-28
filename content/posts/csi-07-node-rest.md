---
title: 'csi 07 节点侧其余接口：块设备、能力声明、拓扑与卷健康'
date: 2026-11-28T18:30:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "NodeGetInfo", "NodeGetVolumeStats", "卷健康", "csi-driver-nfs"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "节点侧除了 Stage 和 Publish，还有五组接口：块设备发布、能力声明、拓扑上报、用量统计和卷健康。它们大多只被 kubelet 定期调用，返回值直接决定调度和驱逐行为。本文拆开 hostpath 的实现，对照 nfs 的极简版本，说明每条能力声明分别被谁消费。"
---

[上一篇](/posts/csi-06-node-mount/)讲完了挂载的两步。节点侧还有五组接口，它们平时不参与挂载，但每一个都在影响集群行为：**能力表决定 kubelet 走哪条路径，拓扑决定 Pod 能被调度到哪个节点，统计和健康决定 kubelet 什么时候驱逐 Pod。**

这一篇把它们放在一起看，因为实现都很短，值得花时间的是「谁在消费返回值」。

## 一、块设备：另一条发布分支

[06 篇](/posts/csi-06-node-mount/)只看了文件系统分支。块设备卷走的是另一条路：

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
	if req.GetVolumeCapability().GetBlock() != nil {
		if vol.VolAccessType != state.BlockAccess {
			return nil, status.Error(codes.InvalidArgument, "cannot publish a non-block volume as block volume")
		}

		volPathHandler := volumepathhandler.VolumePathHandler{}

		// Get loop device from the volume path.
		loopDevice, err := volPathHandler.GetLoopDevice(vol.VolPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get the loop device: %w", err)
		}

		// Check if the target path exists. Create if not present.
		_, err = os.Lstat(targetPath)
		if os.IsNotExist(err) {
			if err = makeFile(targetPath); err != nil {
				return nil, fmt.Errorf("failed to create target path: %w", err)
			}
		}
```

四个差别：

1. **类型必须匹配**（第 2–3 行）。建卷时是 `MountAccess`，就不能当块设备发布；
2. **源是 loop 设备**（第 9 行），不是目录。`createVolume` 建块卷时用 `fallocate` 造了个文件，再 `AttachFileDevice` 把它关联到 loop 设备；
3. **target 是文件**（第 14–19 行的 `makeFile`），不是目录；
4. **挂载选项仍然是 `bind`**（下一段），因为 loop 设备本身已经是一个块设备，bind 到 Pod 的 `/dev/xxx` 即可。

块设备在 Pod 里长这样：

```yaml {linenos=inline}
# Pod：把块设备直接交给容器
apiVersion: v1
kind: Pod
metadata:
  name: block-consumer
spec:
  containers:
  - name: app
    image: busybox
    command: ["sh", "-c", "mkfs.ext4 /dev/xvda && mount /dev/xvda /data && sleep 1d"]
    volumeDevices:          # 注意不是 volumeMounts
    - name: data
      devicePath: /dev/xvda
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: block-pvc
```

`volumeDevices`（第 11–13 行）和 `volumeMounts` 是两种不同的挂载方式：前者要求 PVC 的 `volumeMode: Block`，后者要求 `Filesystem`。**用错了 apiserver 会拒绝 Pod**，错误信息是 `volumeMode` 不匹配。

## 二、能力声明：每一条被谁消费

`NodeGetCapabilities` 返回的五条加上条件追加的一条，各自有明确的消费方：

| 能力 | 消费方 | 不开会怎样 |
| --- | --- | --- |
| `STAGE_UNSTAGE_VOLUME` | kubelet | 不调 `NodeStageVolume`，直接 Publish |
| `GET_VOLUME_STATS` | kubelet | 不调 `NodeGetVolumeStats`，Pod 的磁盘用量一直显示为 0 |
| `SINGLE_NODE_MULTI_WRITER` | kubelet | 拒绝 `ReadWriteOncePod` 之外的多写者访问模式 |
| `GET_VOLUME_HEALTH` | kubelet | 不调 `NodeGetVolumeHealth`，卷故障不会反映到 Pod 状态 |
| `GET_STORAGE_HEALTH` | kubelet | 不调 `NodeGetStorageHealth`，存储后端故障不可见 |
| `EXPAND_VOLUME` | kubelet | 不调 `NodeExpandVolume`，扩容只停在这一侧 |

hostpath 除了 `EXPAND_VOLUME` 是条件声明（`--enable-volume-expansion`，默认开），其余五条**无条件返回**。

nfs 只声明了三条：

```go {linenos=inline}
// pkg/nfs/nfs.go
	n.AddNodeServiceCapabilities([]csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
		csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.NodeServiceCapability_RPC_UNKNOWN,
	})
```

**`UNKNOWN` 是占位符，不是能力**（第 4 行）。规范里它表示「未指定」，正常驱动不该返回它——nfs 这里大概是从模板继承下来的，kubelet 会忽略。

关键是少了 `STAGE_UNSTAGE_VOLUME`（所以 nfs 只有一步挂载）和两条健康接口（所以 NFS 后端的故障不会通过 CSI 传到 Pod 状态）。

## 三、拓扑：告诉调度器「我在哪」

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
func (hp *hostPath) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	resp := &csi.NodeGetInfoResponse{
		NodeId:            hp.config.NodeID,
		MaxVolumesPerNode: hp.config.MaxVolumesPerNode,
	}

	if hp.config.EnableTopology {
		resp.AccessibleTopology = &csi.Topology{
			Segments: map[string]string{TopologyKeyNode: hp.config.NodeID},
		}
	}

	if hp.config.AttachLimit > 0 {
		resp.MaxVolumesPerNode = hp.config.AttachLimit
	}
```

三个字段：

1. **`NodeId`**（第 3 行）。就是 `--nodeid` 的值，kubelet 用它匹配 `CSINode` 对象；
2. **`AccessibleTopology`**（第 7–11 行）。拓扑段，`TopologyKeyNode` 是 `kubernetes.io/hostname`。声明了 `VOLUME_ACCESSIBILITY_CONSTRAINTS` 才会有这个字段；
3. **`MaxVolumesPerNode`**（第 13–15 行）。单节点最多挂多少个卷，kubelet 据此限制 Pod 数量。

拓扑的作用链是：`NodeGetInfo` 返回段 → kubelet 写进 `CSINode.spec.drivers[].topologyKeys` → 调度器的 `VolumeBinding` 插件用它生成节点的拓扑信息 → `CreateVolume` 的 `requisite` 里出现。

`--attach-limit -1` 那个分支容易看漏：

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
	// if attach limit is -1, set a random MaxVolumesPerNode between 1 and 10
	if hp.config.AttachLimit == -1 {
		resp.MaxVolumesPerNode = int64(rand.Intn(10) + 1)
	}
```

**每次调用返回不同的随机数**（第 2–4 行）。这是给 E2E 测试用的：`maxVolumesPerNode` 会随每次 `NodeGetInfo` 抖动，用来验证 kubelet 不会把它缓存成固定值。

nfs 的 `NodeGetInfo` 只返回 `NodeId`：

```go {linenos=inline}
// pkg/nfs/nodeserver.go
func (ns *NodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.Driver.nodeID,
	}, nil
}
```

没有拓扑（第 2–4 行）。因为 NFS 的卷在所有节点上都能访问，不需要按拓扑限制调度。

## 四、用量统计：kubelet 的驱逐依据

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
	available, capacity, used, inodes, inodesFree, inodesUsed, err := getPVStats(in.GetVolumePath())
	if err != nil {
		return nil, fmt.Errorf("get volume stats failed: %w", err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: available,
				Used:      used,
				Total:     capacity,
				Unit:      csi.VolumeUsage_BYTES,
			}, {
				Available: inodesFree,
				Used:      inodesUsed,
				Total:     inodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}
```

两组用量（第 6–20 行）：字节数和 inode 数。**inode 这一组不能省**——很多「磁盘没满但写不进去」的故障，就是 inode 用完了；kubelet 靠这个数值触发驱逐。

底层是 `fs.Info`：

```go {linenos=inline}
// pkg/hostpath/healthcheck.go
func getPVStats(volumePath string) (available int64, capacity int64, used int64, inodes int64, inodesFree int64, inodesUsed int64, err error) {
	return fs.Info(volumePath)
}
```

直接调 `k8s.io/kubernetes/pkg/volume/util/fs`（第 2 行）。**这是 kubelet 自己的统计实现**，所以 hostpath 报出来的数字和 `kubectl describe node` 里的一致。真实驱动通常也复用它，避免统计口径不一致。

nfs 在调 `fs.Info` 之前多了一层缓存：

```go {linenos=inline}
// pkg/nfs/nodeserver.go
	// check if the volume stats is cached
	cache, err := ns.Driver.volStatsCache.Get(req.VolumeId, CacheReadTypeDefault)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	if cache != nil {
		resp := cache.(*csi.NodeGetVolumeStatsResponse)
		klog.V(6).Infof("NodeGetVolumeStats: volume stats for volume %s path %s is cached", req.VolumeId, req.VolumePath)
		return resp, nil
	}
```

**缓存 `volStatsCache`**（第 2–10 行），过期时间默认 10 分钟（`--vol-stats-cache-expire`）。原因很实际：NFS 上跑 `statfs` 要发网络请求，kubelet 每十几秒调一次，不缓存会给服务端造成明显压力。

代价是**用量数据有延迟**：Pod 刚写完大量数据，kubelet 可能十分钟后才看到。这是文件存储驱动的常见取舍。

## 五、卷健康：用标记文件传递状态

这是 hostpath 相对老版本新增的能力，也是它对「驱动怎么把后端故障告诉 Kubernetes」这个问题的回答。

接口本身很薄：

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
func (hp *hostPath) NodeGetVolumeHealth(ctx context.Context, req *csi.NodeGetVolumeHealthRequest) (*csi.NodeGetVolumeHealthResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	if _, err := hp.state.GetVolumeByID(req.GetVolumeId()); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: %v", req.GetVolumeId(), err)
	}

	return &csi.NodeGetVolumeHealthResponse{
		VolumeHealth: &csi.VolumeHealth{
			VolumeId:       req.GetVolumeId(),
			HealthStatuses: hp.getVolumeHealthEntries(req.GetVolumeId(), ScopeNode),
		},
	}, nil
}
```

真正的设计在**状态怎么被写进去**：

```go {linenos=inline}
// pkg/hostpath/healthctl.go
const (
	volumeHealthSuffix             = ".health"
	volumeHealthControllerSuffix   = ".controller.health"
	volumeHealthNodeSuffix         = ".node.health"
	storageHealthFile              = "storage.health"
)
```

三种作用域对应三种文件（第 2–5 行）：

1. `<volID>.health`：controller 和 node 都能读的兜底标记；
2. `<volID>.controller.health`：只有 controller 侧读，优先级高于兜底；
3. `<volID>.node.health`：只有 node 侧读，同样优先。

读取时先看侧别专用的，没有再退回通用：

```go {linenos=inline}
// pkg/hostpath/healthctl.go
func ReadEffectiveVolumeHealthMarker(stateDir, volID string, scope VolumeHealthScope) (*VolumeHealthMarker, error) {
	if scope != ScopeBoth {
		m, err := ReadVolumeHealthMarker(stateDir, volID, scope)
		if err != nil {
			return nil, err
		}
		if m != nil {
			return m, nil
		}
	}
	return ReadVolumeHealthMarker(stateDir, volID, ScopeBoth)
}
```

**为什么用文件而不是直接返回**：hostpath 没有真正的后端可查，它需要一种「外部能写、驱动能读」的通道来模拟故障。标记文件就是这个通道，仓库里配了 `cmd/healthctl` 这个命令行工具，可以手工写入一条健康标记：

```bash {linenos=inline}
# 给 volume-1 在 node 侧写一条 DEGRADED 标记
healthctl -statedir /csi-data-dir mark-volume-unhealthy volume-1 \
  --scope node --status DEGRADED --reason IOError \
  --message "backend disk failing"
```

写完之后，kubelet 下次调 `NodeGetVolumeHealth`（`--node-status-update-frequency`，默认 10 秒）就能看到，进而影响 Pod 的 `VolumeHealth` 状况。**真实驱动不会用文件**——它们直接查后端，但接口形态是一样的。

还有一层 node 级的存储健康：

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
func (hp *hostPath) NodeGetStorageHealth(ctx context.Context, req *csi.NodeGetStorageHealthRequest) (*csi.NodeGetStorageHealthResponse, error) {
	return &csi.NodeGetStorageHealthResponse{
		BackendHealth: hp.getStorageBackendHealth(),
	}, nil
}
```

区别在于**粒度**：`NodeGetVolumeHealth` 针对单个卷，`NodeGetStorageHealth` 针对整个存储后端。后者对应 `GET_STORAGE_HEALTH` 能力，一个后端坏了会影响该节点上所有卷。

清除标记有两个时机：

1. 卷被删除时，`deleteVolume` 会调 `ClearAllVolumeHealthMarkers`（[03 篇](/posts/csi-03-state/)看过这段），避免卷 ID 复用时残留旧标记；
2. 后端恢复后由 `healthctl` 手工清除。

## 六、NodeExpandVolume：只做类型检查

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
	info, err := os.Stat(volPath)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Could not get file information from %s: %v", volPath, err)
	}

	switch m := info.Mode(); {
	case m.IsDir():
		if vol.VolAccessType != state.MountAccess {
			return nil, status.Errorf(codes.InvalidArgument, "Volume %s is not a directory", volID)
		}
	case m&os.ModeDevice != 0:
		if vol.VolAccessType != state.BlockAccess {
			return nil, status.Errorf(codes.InvalidArgument, "Volume %s is not a block device", volID)
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "Volume %s is invalid", volID)
	}

	return &csi.NodeExpandVolumeResponse{}, nil
}
```

**除了校验，什么都不做**（第 1–17 行）。因为 hostpath 的「扩容」在 controller 侧只是改了个数字，文件系统本来就没限制——`resize2fs` 那一步在目录卷上没有意义。

真实驱动的这一步是必需的：块设备扩容后必须 `resize2fs` 或 `xfs_growfs`，否则容器看到的还是旧大小。**`ControllerExpandVolume` 返回 `NodeExpansionRequired: true` 时，kubelet 才会走到这里**（[05 篇](/posts/csi-05-attach-snapshot-expand/)看过这个返回值）。

## 七、两份实现的对照

| 接口 | hostpath | nfs | 差异原因 |
| --- | --- | --- | --- |
| 块设备 | 支持，走 loop 设备 | 不支持 | 文件系统没有块设备 |
| `STAGE_UNSTAGE_VOLUME` | 声明 | 不声明 | 网络路径不需要两级挂载 |
| `NodeGetInfo` 拓扑 | `kubernetes.io/hostname` | 无 | NFS 卷所有节点可达 |
| `NodeGetVolumeStats` | 直接 `fs.Info` | 带 10 分钟缓存 | 网络调用比本地贵 |
| 卷健康 | 标记文件 + `healthctl` | 无 | 共享目录没有单卷故障概念 |
| `NodeExpandVolume` | 只校验类型 | `Unimplemented` | 目录没有配额 |

三条规律：

1. **能力表是消费方的开关**，不是文档。少声明一条，对应的 kubelet 逻辑就整个不启用，返回什么都无所谓；
2. **统计和健康是「被轮询」的接口**。它们的实现要考虑调用频率（kubelet 每 10 秒左右一次），所以 nfs 才需要缓存；
3. **节点侧的实现长度取决于后端能力**。hostpath 要模拟块设备、拓扑、健康，所以代码长；nfs 只有目录，所以一半接口是 `Unimplemented`，这是正确的。

控制器和节点侧的接口都过完了。下一篇换个视角：跟着一个 5Gi 的 PVC，从创建到删除走完全程，看每一步是谁触发的、两份驱动分别做了什么。
