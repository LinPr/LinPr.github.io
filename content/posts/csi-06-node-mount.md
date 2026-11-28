---
title: 'csi 06 NodeStage 与 NodePublish：一个卷怎么变成容器里的挂载点'
date: 2026-11-28T18:20:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "NodeStageVolume", "NodePublishVolume", "csi-driver-nfs"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "kubelet 给同一个卷准备两个路径：staging 路径每节点一份，target 路径每个 Pod 一份。声明了 STAGE_UNSTAGE_VOLUME 的驱动要走两步，没声明的只走一步。本文拆开 hostpath 的两步挂载、bind mount 的参数与目标路径怎么拼，对照 nfs 的一步挂载，并说明 readOnly、fsGroup、mount propagation 分别在哪个环节生效。"
---

[上一篇](/posts/csi-05-attach-snapshot-expand/)结束时，卷已经建好，可能在某个节点上「附着」了。这一篇是最后一步：**让它出现在容器的文件系统里**。

挂载分几步，由 [01 篇](/posts/csi-01-spec/)那张能力表决定。hostpath 声明了 `STAGE_UNSTAGE_VOLUME`，kubelet 会先 Stage 再 Publish；nfs 没声明，只有 Publish 一步。

{{< figure src="/images/posts/csi/06-mount-paths.svg" link="/images/posts/csi/06-mount-paths.svg" caption="两步挂载与一步挂载：staging 路径每节点一份、由 Stage 写入状态；target 路径每个 Pod 一份，靠 bind mount 把数据目录挂进去（点击查看大图）" >}}

## 一、kubelet 给的三个路径

理解挂载之前，先分清 kubelet 传给驱动的三个路径字段：

| 字段 | 谁生成 | 粒度 | 用途 |
| --- | --- | --- | --- |
| `staging_target_path` | kubelet | 每个节点、每个卷一份 | Stage 之后的数据落点 |
| `target_path` | kubelet | 每个 Pod、每个卷一份 | 容器最终看到的位置 |
| `volume_path`（只在 Publish 里） | kubelet | 同 staging | 没 Stage 时直接指向数据 |

三者都是 kubelet 算好的绝对路径，驱动不能改：

1. staging 形如 `/var/lib/kubelet/plugins/kubernetes.io/csi/<driver>/<hash>/globalmount`；
2. target 形如 `/var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~csi/<pv-name>/mount`。

**为什么要有两级**：同一个卷可以被同一个节点的多个 Pod 使用（`ReadWriteOnce` 只限制节点数，不限制 Pod 数）。先挂一次到 staging 路径，再给每个 Pod 各 bind mount 一次，底层设备只挂一次。

## 二、hostpath 的 Stage：只记账，不动文件系统

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
func (hp *hostPath) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capability missing in request")
	}
```

三个必填项（第 4–13 行）。缺任何一个都返回 `InvalidArgument`，因为后面所有的路径判断都依赖它们。

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
	if hp.config.EnableAttach && !vol.Attached {
		return nil, status.Errorf(codes.Internal, "ControllerPublishVolume must be called on volume '%s' before staging on node",
			vol.VolID)
	}

	if vol.Staged.Has(stagingTargetPath) {
		klog.V(4).Infof("Volume %q is already staged at %q, nothing to do.", req.VolumeId, stagingTargetPath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	if !vol.Staged.Empty() {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %q is already staged at %v", req.VolumeId, vol.Staged)
	}

	vol.Staged.Add(stagingTargetPath)
	if err := hp.state.UpdateVolume(vol); err != nil {
		return nil, err
	}
```

四段判断（第 1–18 行），每一段都对应一条规范要求：

1. **开了 attach 就必须先附着**（第 1–4 行）。这是两步之间的顺序约束，返回 `Internal` 是因为它属于调用方用错了流程，不是环境问题；
2. **同一个 staging 路径重复 Stage 是幂等的**（第 6–9 行），直接返回成功；
3. **已经 staged 到别的路径返回 `FailedPrecondition`**（第 11–13 行）。一个卷在一个节点上只能有一个 staging 路径；
4. **最后只加一行状态**（第 15–18 行）。

`NodeStageVolume` 到此结束——**它完全没有碰文件系统**。因为数据目录 `/var/lib/csi-hostpath-data/<volume-id>` 创建时就已经存在，不需要在这里做任何事。

真实驱动这一步做的事就多了：格式化设备（`mkfs`）、挂到 staging 路径、设置属主。hostpath 之所以能不做事，是因为它的「设备」本来就是一个目录。

## 三、hostpath 的 Publish：一次 bind mount

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
	targetPath := req.GetTargetPath()
	ephemeralVolume := req.GetVolumeContext()["csi.storage.k8s.io/ephemeral"] == "true" ||
		req.GetVolumeContext()["csi.storage.k8s.io/ephemeral"] == "" && hp.config.Ephemeral // Kubernetes 1.15 doesn't have csi.storage.k8s.io/ephemeral.

	if req.GetVolumeCapability().GetBlock() != nil &&
		req.GetVolumeCapability().GetMount() != nil {
		return nil, status.Error(codes.InvalidArgument, "cannot have both block and mount access type")
	}
```

开头的两个标志位（第 1–8 行）：`ephemeralVolume` 判断这个卷是不是内联卷（生命周期跟 Pod 走），block 和 mount 不能同时指定。

stage 检查在中间：

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
	if !ephemeralVolume {
		if vol.Staged.Empty() {
			return nil, status.Errorf(codes.FailedPrecondition, "volume %q must be staged before publishing", vol.VolID)
		}
		if !vol.Staged.Has(req.GetStagingTargetPath()) {
			return nil, status.Errorf(codes.InvalidArgument, "volume %q was staged at %v, not %q", vol.VolID, vol.Staged, req.GetStagingTargetPath())
		}
	}
```

**Publish 必须带 Stage 时的那个路径**（第 5–7 行），不一致返回 `InvalidArgument`。内联卷例外（第 1 行），因为它不走 Stage。

真正的挂载在文件系统分支里：

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
		notMnt, err := mount.IsNotMountPoint(mounter, targetPath)
		if err != nil {
			if os.IsNotExist(err) {
				if err = os.Mkdir(targetPath, 0750); err != nil {
					return nil, fmt.Errorf("create target path: %w", err)
				}
				notMnt = true
			} else {
				return nil, fmt.Errorf("check target path: %w", err)
			}
		}

		if !notMnt {
			return &csi.NodePublishVolumeResponse{}, nil
		}

		options := []string{"bind"}
		if readOnly {
			options = append(options, "ro")
		}
		path := hp.getVolumePath(volumeId)

		if err := mounter.Mount(path, targetPath, "", options); err != nil {
```

四件事：

1. **target 路径不存在就建**（第 3–7 行），权限 `0750`；
2. **已经挂载过就返回成功**（第 13–15 行）。这是 Publish 的幂等实现，靠的是「查内核挂载表」而不是查 `state.json`；
3. **挂载选项只有两个**（第 17–20 行）：`bind` 和可选的 `ro`。`fsType` 是空的，bind mount 不需要；
4. **源路径是卷的数据目录**（第 21 行），不是 staging 路径。

第 4 点容易看错：**hostpath 的 bind mount 直接从数据目录挂到 target，绕过了 staging 路径**。所以在 hostpath 里 staging 路径是个「记账用的名义路径」，`state.json` 记着它，但文件系统上并没有东西挂在那里。

这是示例驱动的取舍，不是规范要求。规范的本意是「Stage 把设备挂到 staging 路径，Publish 再 bind 到 target」。真实驱动会这么写；hostpath 因为数据本来就在本地目录，省掉了第一跳，代价是**它的 staging 路径对排查问题没有帮助**——`mount | grep globalmount` 什么也看不到。

## 四、readOnly、fsGroup 与 mount propagation

三个经常被问到「在哪一步生效」的东西：

| 项目 | 生效环节 | 谁负责 |
| --- | --- | --- |
| `readOnly: true` | Publish 的 `ro` 挂载选项 | 驱动（hostpath 第 18–20 行） |
| `fsGroup` | Publish 之前，kubelet 调 `NodeStageVolume` 后自己改属主 | kubelet，不是驱动 |
| mount propagation | Pod 的 `volumeMounts[].mountPropagation` | kubelet，通过容器运行时设置 |

补充两点：

1. **`fsGroup` 不是 CSI 接口的一部分**。kubelet 在挂载后会递归 `chown`，CSI 驱动可以通过 `CSIDriver.spec.fsGroupPolicy` 声明支持方式（`ReadWriteOnceWithFSType`、`File`、`None`），但驱动本身不负责改权限；
2. **mount propagation 的 `Bidirectional` 是给驱动容器自己用的**，写在部署清单的 `volumeMounts` 上（[02 篇](/posts/csi-02-process/)的 `mountpoint-dir`），目的是让驱动在容器里做的挂载能传播到宿主机的 kubelet 视图。**驱动容器不挂载 `/var/lib/kubelet/pods` 并开 `Bidirectional`，它做的 bind mount 宿主机看不到，Pod 就挂不上**——这是写自己驱动时最常见的坑之一。

## 五、nfs：没有 Stage 这一步

```go {linenos=inline}
// pkg/nfs/nodeserver.go
func (ns *NodeServer) NodeStageVolume(_ context.Context, _ *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
```

`NodeStageVolume` 和 `NodeUnstageVolume` 都返回 `Unimplemented`（第 2–3 行），kubelet 也不会调用它们，因为 node 侧能力表里没有 `STAGE_UNSTAGE_VOLUME`。

它的 Publish 直接 mount 网络路径：

```go {linenos=inline}
// pkg/nfs/nodeserver.go
	server = getServerFromSource(server)
	source := fmt.Sprintf("%s:%s", server, baseDir)
	if subDir != "" {
		// replace pv/pvc name namespace metadata in subDir
		subDir = replaceWithMap(subDir, subDirReplaceMap)

		source = strings.TrimRight(source, "/")
		source = fmt.Sprintf("%s/%s", source, subDir)
	}
```

从 `VolumeContext` 里拼出源路径（第 1–9 行）：`server:/share/subdir`。这和 hostpath 的本地目录是本质区别——**源路径可以来自任意节点，所以每个节点各自 mount 一次就能共享**，不需要 staging 把设备「先挂到本机」。

挂载前还要处理一个 NFS 特有的情况：

```go {linenos=inline}
// pkg/nfs/nodeserver.go
	if !notMnt {
		// check if the existing mount is stale (e.g. after NFS server restart)
		if _, err := lstatFunc(targetPath); err != nil && os.IsPermission(err) {
			return &csi.NodePublishVolumeResponse{}, nil
		} else if err != nil && isStaleFileHandle(err) {
			klog.Warningf("NodePublishVolume: detected stale mount at %s, attempting remount", targetPath)
			if unmountErr := ns.mounter.Unmount(targetPath); unmountErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to unmount stale mount %s: %v", targetPath, unmountErr)
			}
			// fall through to remount
		} else {
			// 已经正常挂载，改属主后返回
		}
	}
```

已经挂载过时不能直接返回成功，要先探一下**挂载是否已经失效**（第 2–10 行）。NFS 服务端重启后，本地的挂载点还在，但访问会报 `ESTALE`；这时必须卸载重挂，否则 Pod 里读写会一直失败。

**这一步是文件存储驱动必须处理的**，块设备驱动没有这个问题。hostpath 也检查了「已挂载就直接返回」（第三节），因为它没有远端可能失联。

## 六、卸载：顺序不能反

`NodeUnpublishVolume` 先解绑 target，`NodeUnstageVolume` 再清 staging。hostpath 的 Unstage 有一道保护：

```go {linenos=inline}
// pkg/hostpath/nodeserver.go
	if !vol.Published.Empty() {
		return nil, status.Errorf(codes.Internal, "volume %q is still published at %q on node %q", vol.VolID, vol.Published, vol.NodeID)
	}
	vol.Staged.Remove(stagingTargetPath)
```

**还有 Pod 在用就不能 Unstage**（第 1–4 行）。这防的是 kubelet 在 Pod 没删干净时过早卸载卷——真实驱动如果先卸了设备，正在运行的 Pod 会立刻 IO 失败。

同一卷被多个 Pod 挂载时，`Published` 是一个集合，最后一个 Pod 卸载后才允许 Unstage。

## 七、两份实现的对照

| 对比项 | hostpath | nfs |
| --- | --- | --- |
| Stage | 实现，只写状态，不碰文件系统 | `Unimplemented` |
| Publish 的源 | 本地数据目录 | `server:/share/subdir` |
| bind mount 选项 | `bind` + 可选 `ro` | 网络挂载选项 + `ro` |
| 幂等判断 | 查内核挂载表 | 查挂载表 + 探测 `ESTALE` |
| 卸载顺序 | Unpublish → Unstage，带「仍被使用」保护 | 只有 Unpublish |
| 并发控制 | 全局 `mutex` | 按 `volumeID-targetPath` 的 `VolumeLocks` |

结论很清楚：**Stage 这一步存不存在，取决于数据源是「本机设备」还是「网络路径」**。本机设备需要「先挂到节点、再分给 Pod」两级；网络路径不需要，每个节点自己挂就行。

下一节看节点侧剩下的接口：`NodeGetInfo` 的拓扑、`NodeGetVolumeStats` 的用量上报、`NodeGetCapabilities` 的逐条含义，以及 hostpath 新增的卷健康接口。
