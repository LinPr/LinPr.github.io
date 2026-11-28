---
title: 'csi 03 状态存储：驱动怎么记住自己建过哪些卷'
date: 2026-11-28T17:50:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "csi-driver-host-path", "csi-driver-nfs"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "CSI 规范没有规定驱动把状态存在哪，只要求重复调用时给出相同回答。hostpath 把卷和快照的元数据写进本地 state.json，用一把全局锁串行化所有修改；nfs 则完全不存状态，把卷的位置编码进 volume ID。两种做法各自解决了什么问题、又付出了什么代价，是这一篇要对照的重点。"
---

[上一篇](/posts/csi-02-process/)结束时，驱动已经能启动、能回答「我是谁」了。但 `CreateVolume` 一旦被调用，它就要回答一个绕不开的问题：**上次建过的卷，这次还记得吗**。

规范没有规定状态存在哪，只规定了行为：同名同参数的重复创建要返回同一个卷，删除一个不存在的卷要返回成功。怎么满足这两条，两份驱动走了极端相反的两条路。

## 一、hostpath：一个 JSON 文件加一把全局锁

hostpath 的驱动对象上只有两个字段，第二个就是状态：

```go {linenos=inline}
// pkg/hostpath/hostpath.go
type hostPath struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
	csi.UnimplementedGroupControllerServer
	csi.UnimplementedSnapshotMetadataServer
	config Config

	// gRPC calls involving any of the fields below must be serialized
	// by locking this mutex before starting. Internal helper
	// functions assume that the mutex has been locked.
	mutex sync.Mutex
	state state.State
}
```

注释写得很明确（第 9–11 行）：**凡是要读写 `state` 的 gRPC 调用，进来第一件事都是加锁**。这不是细粒度的锁，而是一把全局互斥锁，整个驱动的所有修改操作排队执行。

状态本身在 `pkg/state` 包里，底层就是一个 JSON 文件：

```go {linenos=inline}
// pkg/state/state.go
type Volume struct {
	VolName        string
	VolID          string
	VolSize        int64
	VolPath        string
	VolAccessType  AccessType
	ParentVolID    string
	ParentSnapID   string
	Ephemeral      bool
	NodeID         string
	Kind           string
	ReadOnlyAttach bool
	Attached       bool
	Staged         Strings
	Published      Strings
}

type Snapshot struct {
	Name            string
	Id              string
	VolID           string
	Path            string
	CreationTime    *timestamppb.Timestamp
	SizeBytes       int64
	ReadyToUse      bool
	GroupSnapshotID string
}
```

`Volume` 里有两类字段值得区分：

1. **创建时确定的**：`VolName`、`VolID`、`VolSize`、`VolPath`、`VolAccessType`、`Kind`，以及克隆来源 `ParentVolID`、`ParentSnapID`；
2. **运行中变化的**：`Staged` 和 `Published` 记录卷当前挂在哪个 staging 路径、被 publish 到哪些目标路径，`Attached`、`ReadOnlyAttach` 记录附着状态。

第一类是 `CreateVolume` 写、之后只读；第二类是每次 `NodeStageVolume`、`NodePublishVolume` 都要更新。**驱动重启后靠这两类字段恢复全部认知**：知道有哪些卷、每个卷多大、当前挂在哪些路径上。

## 二、状态文件的读写只有两个函数

```go {linenos=inline}
// pkg/state/state.go
func (s *state) dump() error {
	data, err := json.Marshal(&s.resources)
	if err != nil {
		return status.Errorf(codes.Internal, "error encoding volumes and snapshots: %v", err)
	}
	if err := os.WriteFile(s.statefilePath, data, 0600); err != nil {
		return status.Errorf(codes.Internal, "error writing state file: %v", err)
	}
	return nil
}

func (s *state) restore() error {
	s.Volumes = nil
	s.Snapshots = nil

	data, err := os.ReadFile(s.statefilePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Nothing to do.
		return nil
	case err != nil:
		return status.Errorf(codes.Internal, "error reading state file: %v", err)
	}
	if err := json.Unmarshal(data, &s.resources); err != nil {
		return status.Errorf(codes.Internal, "error encoding volumes and snapshots from state file %q: %v", s.statefilePath, err)
	}
	return nil
}
```

`dump`（第 2–10 行）在每次修改后调用，把**整个** `resources` 结构序列化后覆盖写盘。`restore`（第 12–27 行）在 `state.New` 里调用一次，把文件读回内存。三个要点：

1. **文件权限是 `0600`**（第 7 行）。这些元数据里有卷的宿主路径，不能让其他用户读到；
2. **文件不存在不算错**（第 17–19 行）。首次启动时没有 `state.json`，恢复成空状态即可；
3. **每次修改都写全量**。`UpdateVolume`、`DeleteVolume`、`UpdateSnapshot` 都是「改内存 → 调 `dump`」，没有增量日志，也没有事务。

全量写的代价是 O(n)：卷多了以后每次修改都要序列化全部卷。hostpath 是测试驱动，卷数量少，这个代价可以忽略；真实驱动不会这么做，第 06 节会讲它们的选择。

## 三、一把锁换来了什么

`CreateVolume` 里加锁的位置很关键：

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

	// Lock before acting on global state. A production-quality
	// driver might use more fine-grained locking.
	hp.mutex.Lock()
	defer hp.mutex.Unlock()
```

注释（第 10–11 行）承认了一把锁不够优雅。但它换来的是整个「查重名 + 建卷 + 写状态」过程的原子性。`CreateVolume` 的核心逻辑是：先按名字查卷存不存在，存在就比对容量，不存在才建：

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

如果没有锁，两个同名的 `CreateVolume` 并发进来，都查不到卷，都去建目录、都往 `state.json` 里追加，**最后会多出一个孤儿目录**。加锁之后，第二个请求一定看到第一个的结果，直接返回已有卷（第 4 行之后的分支），满足规范的幂等要求。

`DeleteVolume` 也在这把锁下执行，所以「删除正在被并发创建的卷」这种交错不会发生。

## 四、崩溃之后：状态能恢复什么、恢复不了什么

驱动进程崩溃、容器重启后，`state.New` 会重新读 `state.json`，卷列表原样回来。这是这套设计的最大价值：**kubelet 停止再启动容器时，驱动不需要重新发现卷**。

但有两件事它救不回来：

1. **写盘过程中崩溃**。`os.WriteFile` 不是原子的，如果正好在截断文件后、写完数据前崩溃，`state.json` 会变成空文件或半截 JSON，下次启动时 `json.Unmarshal` 报错，驱动直接起不来。hostpath 没有处理这个窗口，真实的驱动会用「临时文件 + `rename`」或写 WAL 来避免；
2. **卷数据本身**。`state.json` 里只记元数据，`VolPath` 指向的目录在宿主机上，节点一换就没了。

这两点也是「不适合照抄」的具体理由，第 06 节会展开。

## 五、nfs：完全不存状态

nfs 驱动里没有类似 `state.json` 的东西。它不需要，因为**卷的位置信息全部编码在 volume ID 里**：

```go {linenos=inline}
// pkg/nfs/controllerserver.go
func getVolumeIDFromNfsVol(vol *nfsVolume) string {
	idElements := make([]string, totalIDElements)
	idElements[idServer] = strings.Trim(vol.server, "/")
	idElements[idBaseDir] = strings.Trim(vol.baseDir, "/")
	idElements[idSubDir] = strings.Trim(vol.subDir, "/")
	idElements[idUUID] = vol.uuid
	if strings.EqualFold(vol.onDelete, retain) || strings.EqualFold(vol.onDelete, archive) {
		idElements[idOnDelete] = vol.onDelete
	}

	return strings.Join(idElements, separator)
}
```

`separator` 是 `#`（`pkg/nfs/utils.go` 第 40 行）。一个典型的 volume ID 长这样：

```text {linenos=inline}
# volume ID 的组成：<server>#<baseDir>#<subDir>#<uuid>[#<onDelete>]
nfs-server.default.svc.cluster.local#share#pvc-9f3a1c2e-4b7d-4f1a-9c3e-2d5b8a7c1e4f#9f3a1c2e-4b7d-4f1a-9c3e-2d5b8a7c1e4f
```

反方向的 `getNfsVolFromID` 把它拆回一个 `nfsVolume`。于是：

1. `DeleteVolume` 拿到 ID 就知道卷在哪个服务器、哪个共享目录、哪个子目录，直接去删，**不需要先查本地记录**；
2. 驱动重启、容器重建、甚至换一个节点部署，只要 ID 没变，行为就完全一致；
3. controller 侧是无状态的，可以随意扩副本。

代价是**卷 ID 变了就找不回来**。如果有人手工改了 NFS 上的目录名，或者 ID 里的 `subDir` 被改写，驱动会认为这个卷不存在。规范要求 ID 不可变，nfs 把这个要求当成了实现手段。

## 六、两种做法放在一起

| 对比项 | hostpath | nfs |
| --- | --- | --- |
| 状态存在哪 | 本地 `state.json` | 不存在，编码在 volume ID 里 |
| 恢复机制 | 启动时读文件 | 每次从 ID 反解 |
| 并发控制 | 一把全局 `sync.Mutex` | 按卷 ID 的 `VolumeLocks`（`TryAcquire` 争不到返回 `Aborted`） |
| 写盘 | 每次修改全量覆盖 | 无 |
| 崩溃窗口 | 写文件过程中可能损坏 | 不存在本地写，但 `volDeletionCache` 是内存缓存，重启即失效 |
| 扩展性 | 单副本 | controller 可扩副本 |
| 适用场景 | 单节点、演示 | 依赖后端自身保证一致性的共享文件系统 |

两处细节值得单独说：

1. **nfs 也不是完全没有锁**。它用 `VolumeLocks` 按卷 ID 加锁，同一个卷的并发操作直接返回 `Aborted` 让调用方重试——和 hostpath 的「排队等」是两种策略。规范允许这两种：`ABORTED` 表示「请重试这个操作」；
2. **nfs 的 `volDeletionCache` 是内存缓存**（`pkg/nfs/nfs.go` 里的 `azcache`），只用来避免重复删除时的多余挂载，删掉了不影响正确性，重启后最坏是多删一次——而多删一次正是规范要求的幂等行为。

## 七、这一节的选择说明了什么

把两种做法并排放，能看到 CSI 规范留给实现的自由度有多大：**规范只管行为，不管状态**。

1. 只要满足「同名同参返回同一个卷」「删除不存在的卷返回成功」，状态存在哪都行；
2. hostpath 用本地文件换「实现简单」，代价是单节点、不可扩展；
3. nfs 用 ID 编码换「无状态」，代价是依赖后端本身的一致性，且 ID 一旦被改动就无法恢复。

真实的生产驱动大多在两者之间：要么把状态写回 Kubernetes 对象（如 PV 的 `VolumeAttributes`），要么依赖后端存储自己的元数据服务，很少像 hostpath 这样存本地文件，也很少像 nfs 这样完全无状态。

下一篇开始看第一个真正用上状态的接口：`CreateVolume` 和 `DeleteVolume`。hostpath 的幂等检查、nfs 的参数校验和 `Aborted` 重试，都会在那里对上。
