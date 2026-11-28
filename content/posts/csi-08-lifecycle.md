---
title: 'csi 08 跟着一个 5Gi 的 PVC 走完全程：每一步是谁触发的'
date: 2026-11-28T18:40:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "PersistentVolumeClaim", "sidecar", "csi-driver-nfs"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "前面七篇分别看了驱动的每个接口。这一篇把它们串成一条时间线：同一个 5Gi 的 PVC，从创建到挂载、扩容、删除，每一步由谁触发、调了哪个 RPC、驱动里改了什么状态、两份驱动在哪一步分道扬镳。附上部署对象与 registrar 注册的位置。"
---

前面七篇是按接口拆的：`CreateVolume`、`ControllerPublishVolume`、`NodeStageVolume`……每个接口单独看很清楚，但真正排查问题时需要的是反过来的视角：**一个 PVC 走到某一步了，现在是谁在干活、调的是什么。**

这一篇跟着一个具体的 PVC 走完全程，把前七篇的内容串起来。对象沿用 [00 篇](/posts/csi-00-journey/)的示例，只把容量改成 5Gi。

## 一、出发前的集群状态

先把要用的对象写好。这是 hostpath 的 StorageClass 加一个 PVC：

```yaml {linenos=inline}
# StorageClass 与 PVC：5Gi，绑定模式 Immediate
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: csi-hostpath-sc
provisioner: hostpath.csi.k8s.io
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-5gi
  namespace: demo
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  storageClassName: csi-hostpath-sc
```

`provisioner`（第 6 行）必须和 `CSIDriver` 的名字一致。集群里那个 `CSIDriver` 对象是这样写的：

```yaml {linenos=inline}
# CSIDriver：告诉 kubelet 这个驱动支持什么
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: hostpath.csi.k8s.io
spec:
  volumeLifecycleModes:
  - Persistent
  - Ephemeral
  podInfoOnMount: true
  fsGroupPolicy: File
```

四个字段各自的作用：

1. `volumeLifecycleModes`（第 6–8 行）：支持持久卷和内联卷。没有 `Ephemeral`，Pod 里写 `csi:` 内联卷会被拒绝；
2. `podInfoOnMount`（第 9 行）：为 `true` 时 kubelet 会把 Pod 的信息塞进 `VolumeContext`，供驱动在 Publish 时读取；
3. `fsGroupPolicy: File`（第 10 行）：kubelet 会按 Pod 的 `fsGroup` 递归改卷的属主。设成 `None` 就不改。

**`CSIDriver` 必须存在**。kubelet 启动 CSI 插件时会先找这个对象，找不到就不认这个驱动，Pod 会卡在 `ContainerCreating` 并报 `driver name ... not found in the list of registered CSI drivers`。

## 二、第 1 步：PVC 创建，provisioner 建卷

`volumeBindingMode: Immediate` 意味着不等待 Pod，PVC 一创建就开始建卷。

**触发方**：`external-provisioner` watch 到 PVC，判定「这个 PVC 需要动态供给」，调 `CreateVolume`。

请求里带的关键字段：

1. `name`：`pvc-<PVC 的 UID>`，由 provisioner 拼接，保证集群内唯一；
2. `capacity_range.required_bytes`：`5 * 1024³`；
3. `volume_capabilities`：从 PVC 的 `accessModes` 翻译过来，`ReadWriteOnce` → `SINGLE_NODE_WRITER`；
4. `parameters`：StorageClass 里的参数，这里为空；
5. `requisite` / `preferred`：`volumeBindingMode: Immediate` 时为空；`WaitForFirstConsumer` 时才有。

**hostpath 做了什么**（[04 篇](/posts/csi-04-create-delete/)）：按名字查重 → 生成 UUID → `MkdirAll` 建目录 → 写 `state.json`。返回的 `VolumeId` 是那个 UUID，`CapacityBytes` 是 5Gi。

**nfs 做了什么**（同一篇）：按卷名加锁 → 校验参数 → 挂载 NFS 共享 → `MkdirAll` 建子目录 → 返回由 `server#share#subdir#uuid` 拼成的 ID。

**结果**：PV 被创建，`pv.spec.csi.volumeHandle` 就是驱动返回的 `VolumeId`，PVC 状态变成 `Bound`。

## 三、第 2 步：Pod 创建，调度器挑节点

```yaml {linenos=inline}
# Pod：使用这个 PVC
apiVersion: v1
kind: Pod
metadata:
  name: app-0
  namespace: demo
spec:
  containers:
  - name: app
    image: busybox
    volumeMounts:
    - name: data
      mountPath: /data
    command: ["sh", "-c", "echo hello > /data/a.txt && sleep 1d"]
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: data-5gi
```

**触发方**：调度器的 `VolumeBinding` 插件。它要做三件事：

1. 找出这个 Pod 用到的所有 PVC（第 14–16 行）；
2. 检查这些 PVC 的 PV 有没有拓扑限制。hostpath 声明了 `VOLUME_ACCESSIBILITY_CONSTRAINTS`，并且 `CreateVolume` 返回的 `AccessibleTopology` 只有当前节点，所以 PV 带了 `nodeAffinity`；
3. 把拓扑约束转成节点筛选条件，Pod 只能调度到那个节点。

**结果**：Pod 的 `spec.nodeName` 被写上，`VolumeAttachment` 对象还没创建（hostpath 没开 attach）。

如果 StorageClass 是 `WaitForFirstConsumer`，**顺序反过来**：第 1 步延后到这里，调度器先挑节点，再带着 `requisite` 拓扑去调 `CreateVolume`。

## 四、第 3 步：attacher（只在开了 attach 时）

**触发方**：`external-attacher` watch 到 `VolumeAttachment`。

这一步默认不发生。hostpath 没声明 `PUBLISH_UNPUBLISH_VOLUME`，attacher 不工作，所以没有 `VolumeAttachment` 对象。只有启动时加了 `--enable-attach`，才会出现：

```yaml {linenos=inline}
# VolumeAttachment：attacher 创建，记录卷挂在哪个节点
apiVersion: storage.k8s.io/v1
kind: VolumeAttachment
metadata:
  name: csi-<卷 ID 的哈希>
spec:
  attacher: hostpath.csi.k8s.io
  nodeName: node-1
  source:
    persistentVolumeName: pvc-9f3a1c2e
status:
  attached: true
```

`status.attached`（第 9 行）由 attacher 在 `ControllerPublishVolume` 成功之后写入。nfs 的情况更简单：它连这个方法都没有，部署里通常也不放 attacher。

## 五、第 4 步：kubelet 挂载

**触发方**：kubelet 的 volume manager。它看到 Pod 的 `spec.nodeName` 是自己，开始准备卷。

hostpath 声明了 `STAGE_UNSTAGE_VOLUME`，走两步（[06 篇](/posts/csi-06-node-mount/)）：

| 子步骤 | 调用 | 驱动做了什么 | 状态变化 |
| --- | --- | --- | --- |
| 4.1 | `NodeStageVolume` | 只把 staging 路径加进 `Staged` | `state.json` 的 `staged` 多一项 |
| 4.2 | `NodePublishVolume` | bind mount 数据目录到 target | `published` 多一项，`nodeID` 写入 |

nfs 没声明，只有一步：

| 子步骤 | 调用 | 驱动做了什么 |
| --- | --- | --- |
| 4.1 | `NodePublishVolume` | 直接 mount `server:/share/subdir` 到 target |

完成之后 `state.json` 里这条记录长这样：

```json {linenos=inline}
// /csi-data-dir/state.json 里的一条记录
{
  "VolName": "pvc-9f3a1c2e-4b7d-4f1a-9c3e-2d5b8a7c1e4f",
  "VolID": "a3f1e8c2-7d94-4a1b-8e6f-3c2d9b7a1e5f",
  "VolSize": 5368709120,
  "VolPath": "/csi-data-dir/a3f1e8c2-7d94-4a1b-8e6f-3c2d9b7a1e5f",
  "VolAccessType": 0,
  "NodeID": "node-1",
  "Attached": false,
  "Staged": ["/var/lib/kubelet/plugins/kubernetes.io/csi/.../globalmount"],
  "Published": ["/var/lib/kubelet/pods/<uid>/volumes/kubernetes.io~csi/pvc-.../mount"]
}
```

**结果**：容器里能看到 `/data`，`echo hello` 写进的是节点上的 `/var/lib/csi-hostpath-data/<volume-id>/a.txt`。

## 六、第 5 步：扩容到 8Gi

```bash {linenos=inline}
# 把 PVC 的容量改成 8Gi
kubectl -n demo patch pvc data-5gi -p '{"spec":{"resources":{"requests":{"storage":"8Gi"}}}}'
```

**触发方**：`external-resizer` watch 到 PVC 的 `spec.resources.requests.storage` 变大，且 StorageClass 允许扩容。

| 步骤 | 调用 | hostpath | nfs |
| --- | --- | --- | --- |
| 5.1 | `ControllerExpandVolume` | 改 `VolSize`，返回 `NodeExpansionRequired: true` | 原样回话，什么都不做 |
| 5.2 | `NodeExpandVolume` | 只校验路径类型 | `Unimplemented`，kubelet 不调 |

hostpath 的 `VolSize` 从 `5368709120` 变成 `8589934592`，但**文件系统没有真的变大**——它的 Node 侧什么都不做（[07 篇](/posts/csi-07-node-rest/)）。真实驱动必须在 5.2 里跑 `resize2fs`。

## 七、第 6 步：删除 Pod，卸载

**触发方**：kubelet 在 Pod 被删除后清理卷。

| 步骤 | 调用 | hostpath |
| --- | --- | --- |
| 6.1 | `NodeUnpublishVolume` | 卸载 target，从 `Published` 移除 |
| 6.2 | `NodeUnstageVolume` | 检查 `Published` 为空后，从 `Staged` 移除 |
| 6.3 | `ControllerUnpublishVolume` | 只在开了 attach 时；把 `Attached` 置 false |

**6.2 的那道检查很关键**（[06 篇](/posts/csi-06-node-mount/)）：`Published` 非空时返回 `Internal`，防止 kubelet 在还有 Pod 用着的时候卸载卷。

## 八、第 7 步：删除 PVC，删卷

```bash {linenos=inline}
# 删除 PVC，触发 reclaimPolicy: Delete
kubectl -n demo delete pvc data-5gi
```

**触发方**：`external-provisioner` watch 到 PV 进入 `Released`，且 `reclaimPolicy: Delete`。

调用 `DeleteVolume`：

1. **hostpath**：查 `state.json`，检查是否还在用，`os.RemoveAll` 删目录，从状态里移除记录，再清掉健康标记；
2. **nfs**：从 ID 反解出 `server/share/subdir`，挂载共享，删子目录，往内存里的 `volDeletionCache` 记一笔。

**结果**：PV 对象和底层数据都消失。如果 `reclaimPolicy: Retain`，这一步不发生，PV 留在 `Released` 状态等人处理。

## 九、两条时间线放在一起

| 时刻 | 事件 | hostpath 调的 RPC | nfs 调的 RPC |
| --- | --- | --- | --- |
| t0 | 创建 PVC | `CreateVolume` | `CreateVolume` |
| t1 | 创建 Pod | 无（调度器工作） | 无 |
| t2 | Pod 绑定节点 | 无（默认没开 attach） | 无 |
| t3 | 挂载 | `NodeStageVolume` + `NodePublishVolume` | `NodePublishVolume` |
| t4 | 扩容 | `ControllerExpandVolume` + `NodeExpandVolume` | `ControllerExpandVolume` |
| t5 | 删 Pod | `NodeUnpublishVolume` + `NodeUnstageVolume` | `NodeUnpublishVolume` |
| t6 | 删 PVC | `DeleteVolume` | `DeleteVolume` |

两份驱动的接口数量差别是 **7 比 5**，差在 `NodeStageVolume`、`NodeUnstageVolume` 这两个（nfs 不需要），和 `NodeExpandVolume`（nfs 是 `Unimplemented`）。

## 十、registrar 在这条线上的位置

前面整条时间线有一个前提：**kubelet 得先认识这个驱动**。这件事发生在更早的启动阶段，由 `node-driver-registrar` 完成：

```yaml {linenos=inline}
# registrar 的启动参数
        - name: node-driver-registrar
          args:
            - --csi-address=/csi/csi.sock
            - --kubelet-registration-path=/var/lib/kubelet/plugins/csi-hostpath/csi.sock
```

两个参数（第 3–4 行）：

1. `--csi-address`：驱动自己的 socket，registrar 用它调 `GetPluginInfo` 和 `NodeGetInfo`；
2. `--kubelet-registration-path`：**kubelet 能看到的那个路径**。registrar 把它写到 `/var/lib/kubelet/plugins_registry/<driver>-reg.sock`，kubelet 从这个目录发现插件，再按里面的路径去连驱动。

**这两个路径必须落在同一个宿主目录上**，否则 registrar 注册的路径 kubelet 连不上。部署清单里 `registrar` 和 `hostpath` 两个容器都挂了 `socket-dir`（宿主路径 `/var/lib/kubelet/plugins/csi-hostpath`），就是为了这个。

注册完成后，kubelet 调用 `NodeGetInfo` 拿到 `NodeId`，写进 `CSINode` 对象：

```yaml {linenos=inline}
# CSINode：kubelet 自动维护，记录本节点上的驱动与拓扑键
apiVersion: storage.k8s.io/v1
kind: CSINode
metadata:
  name: node-1
spec:
  drivers:
  - name: hostpath.csi.k8s.io
    nodeID: node-1
    topologyKeys:
    - kubernetes.io/hostname
```

`topologyKeys`（第 8–9 行）来自 `NodeGetInfo` 返回的 `AccessibleTopology`。**调度器读这个对象来判断 PV 能不能落到某个节点**，所以拓扑键声明错了，Pod 会被调度到挂不上卷的节点。

## 十一、这条时间线说明了什么

三步之间没有直接调用关系，全靠 watch 和对象状态传递：

1. **PVC → PV**：provisioner 例外地是「先有请求、再有对象」，但它的输入仍然是 watch 到的 PVC；
2. **PV + Pod → 调度**：调度器读 PV 的 `nodeAffinity` 和 `CSINode` 的拓扑键，不读驱动的任何东西；
3. **Pod → 挂载**：kubelet 拿到 `spec.nodeName` 才开始挂，走的还是同一个 socket。

**只要中间有一个对象没被正确写入，后面的步骤就不会发生**。排查时从 `kubectl describe pvc`、`kubectl describe pod`、`kubectl get volumeattachment`、`kubectl get csinode` 这几个对象往下看，比直接翻驱动日志快得多。

下一篇把两份驱动放在一起做总结：从这条时间线里能提炼出什么 CSI 的共性，以及哪些选择属于实现自由。
