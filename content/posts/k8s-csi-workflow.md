---
title: 'Kubernetes CSI：一个 PVC 是怎么变成容器里的目录的'
date: 2026-11-15T19:30:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "源码阅读", "csi-driver-nfs"]
categories: ["Kubernetes"]
author: "LinPr"
description: "CSI 把 Kubernetes 的存储拆成「社区维护的 sidecar」和「厂商实现的 gRPC 服务」两半。本文按注册、Provision、Attach、Mount 四个阶段，结合 node-driver-registrar、external-provisioner、external-attacher、kubelet 和 csi-driver-nfs 的源码，讲清楚每一步由谁触发、调了哪个接口、在节点上留下了什么。"
---

写一个 PVC，挂进 Pod，容器里就多了一个能读写的目录。这件事看起来很简单，背后却有六七个组件接力：控制面要创建存储卷、把卷挂到某个节点上，节点上要把卷格式化、挂载，最后再挂进容器。

Kubernetes 早期把各家存储的代码直接写在主仓库里（in-tree volume plugin），每接入一种存储都要改 Kubernetes 本身，发版节奏也被绑在一起。后来出现过 FlexVolume，用节点上的可执行文件做扩展，但它只解决了「挂载」，创建、删除、快照都还得另想办法。CSI（Container Storage Interface）是最终的答案：**Kubernetes 只定义接口，具体怎么创建卷、怎么挂载，交给存储厂商实现的 gRPC 服务。**

本文按一个卷的生命周期，把这条链路拆成四个阶段：注册、Provision、Attach、Mount。每一段都对照源码说明谁触发、调了什么接口、在节点上留下了什么。示例驱动用的是 [csi-driver-nfs](https://github.com/kubernetes-csi/csi-driver-nfs)。

> 源码版本：kubernetes `aabdf5a2`，csi-driver-nfs `f774b51`，node-driver-registrar `c103390`，external-provisioner `c7277b7`，external-attacher `edc3ddc`。

## 一、全景：两半拼起来的存储系统

![CSI 架构：Kubernetes 组件、社区 sidecar、厂商实现的 CSI 服务](/images/posts/k8s-csi/img-00.png)

图里分成三块：

- **黄色和蓝色：Kubernetes 自身。** kube-controller-manager 里的 AttachDetach controller，以及每个节点上的 kubelet。它们不认识任何具体存储，只会创建 API 对象，或者通过 gRPC 调用 CSI 接口。
- **绿色：External Components。** 由 Kubernetes 存储兴趣小组（sig-storage）维护的一组 sidecar，跟任何厂商都无关，可以通用。它们监听 API 对象的变化，把变化翻译成对 CSI 接口的调用。
- **灰色：Custom Components。** 存储厂商要写的部分，是三个 gRPC 服务：Identity、Controller、Node。

中间那条竖线就是 CSI 规范本身。线左边是调用方（gRPC client），线右边是被调用方（gRPC server）。**写一个 CSI 驱动，本质上就是实现这三个 gRPC 服务端。**

这些组件之间的分工可以先用一张表概括：

| 阶段 | 触发者 | 监听的对象 | 调用的 CSI 接口 | 结果 |
| --- | --- | --- | --- | --- |
| 注册 | node-driver-registrar + kubelet | 节点上的 socket 文件 | `Identity.GetPluginInfo`、`Node.NodeGetInfo` | kubelet 知道这个驱动、`CSINode` 对象里多一条记录 |
| Provision | external-provisioner | PVC | `Controller.CreateVolume` | 后端创建出存储卷，集群里多一个 PV |
| Attach | AttachDetach controller → external-attacher | VolumeAttachment | `Controller.ControllerPublishVolume` | 卷被挂到目标节点（如云盘挂到虚拟机） |
| Mount | kubelet | 本节点上应运行的 Pod | `Node.NodeStageVolume`、`Node.NodePublishVolume` | 卷出现在 Pod 的目录里，再被挂进容器 |

## 二、三个 gRPC 服务

先看驱动要实现的接口。下面是 CSI 规范生成的 Go 接口（`github.com/container-storage-interface/spec`），所有实现都要内嵌 `Unimplemented*Server` 以保证向前兼容。

### 2.1 Identity：我是谁，我能做什么

```go
type IdentityServer interface {
    GetPluginInfo(context.Context, *GetPluginInfoRequest) (*GetPluginInfoResponse, error)
    GetPluginCapabilities(context.Context, *GetPluginCapabilitiesRequest) (*GetPluginCapabilitiesResponse, error)
    Probe(context.Context, *ProbeRequest) (*ProbeResponse, error)
}
```

- `GetPluginInfo` 返回驱动名（比如 `nfs.csi.k8s.io`）和版本，**驱动名就是 StorageClass 里 `provisioner` 字段要填的值**；
- `GetPluginCapabilities` 声明有没有 Controller 服务、是否支持拓扑感知等；
- `Probe` 是健康检查，配合 livenessprobe sidecar 使用。

### 2.2 Controller：在控制面就能完成的操作

```go
type ControllerServer interface {
    CreateVolume(context.Context, *CreateVolumeRequest) (*CreateVolumeResponse, error)
    DeleteVolume(context.Context, *DeleteVolumeRequest) (*DeleteVolumeResponse, error)
    ControllerPublishVolume(context.Context, *ControllerPublishVolumeRequest) (*ControllerPublishVolumeResponse, error)
    ControllerUnpublishVolume(context.Context, *ControllerUnpublishVolumeRequest) (*ControllerUnpublishVolumeResponse, error)
    ValidateVolumeCapabilities(context.Context, *ValidateVolumeCapabilitiesRequest) (*ValidateVolumeCapabilitiesResponse, error)
    ListVolumes(context.Context, *ListVolumesRequest) (*ListVolumesResponse, error)
    GetCapacity(context.Context, *GetCapacityRequest) (*GetCapacityResponse, error)
    ControllerGetCapabilities(context.Context, *ControllerGetCapabilitiesRequest) (*ControllerGetCapabilitiesResponse, error)
    CreateSnapshot(context.Context, *CreateSnapshotRequest) (*CreateSnapshotResponse, error)
    DeleteSnapshot(context.Context, *DeleteSnapshotRequest) (*DeleteSnapshotResponse, error)
    ListSnapshots(context.Context, *ListSnapshotsRequest) (*ListSnapshotsResponse, error)
    ControllerExpandVolume(context.Context, *ControllerExpandVolumeRequest) (*ControllerExpandVolumeResponse, error)
    ControllerGetVolume(context.Context, *ControllerGetVolumeRequest) (*ControllerGetVolumeResponse, error)
    ControllerModifyVolume(context.Context, *ControllerModifyVolumeRequest) (*ControllerModifyVolumeResponse, error)
}
```

判断一个操作该放在 Controller 还是 Node，看它**需不需要在卷所在的那台机器上执行**。创建云盘、把云盘挂到某台虚拟机、打快照，调云厂商的 API 就行，在哪台机器上调都一样，所以放在 Controller 服务里，通常以 Deployment 形式只跑一两个副本。

接口很多，但不需要全部实现。`ControllerGetCapabilities` 声明了驱动支持哪些能力，sidecar 会先问它，不支持的接口就不会调用。

### 2.3 Node：必须在节点上做的事

```go
type NodeServer interface {
    NodeStageVolume(context.Context, *NodeStageVolumeRequest) (*NodeStageVolumeResponse, error)
    NodeUnstageVolume(context.Context, *NodeUnstageVolumeRequest) (*NodeUnstageVolumeResponse, error)
    NodePublishVolume(context.Context, *NodePublishVolumeRequest) (*NodePublishVolumeResponse, error)
    NodeUnpublishVolume(context.Context, *NodeUnpublishVolumeRequest) (*NodeUnpublishVolumeResponse, error)
    NodeGetVolumeStats(context.Context, *NodeGetVolumeStatsRequest) (*NodeGetVolumeStatsResponse, error)
    NodeExpandVolume(context.Context, *NodeExpandVolumeRequest) (*NodeExpandVolumeResponse, error)
    NodeGetCapabilities(context.Context, *NodeGetCapabilitiesRequest) (*NodeGetCapabilitiesResponse, error)
    NodeGetInfo(context.Context, *NodeGetInfoRequest) (*NodeGetInfoResponse, error)
}
```

格式化块设备、`mount` 到某个目录，只能在那台节点上做，所以 Node 服务以 DaemonSet 形式跑在每个节点上。它的调用方不是 sidecar，而是 **kubelet 本身**。

### 2.4 部署形态

以 csi-driver-nfs 为例，它的部署清单就是上面分工的直接体现：

| 组件 | 形态 | 容器 |
| --- | --- | --- |
| `csi-nfs-controller` | Deployment | `nfsplugin`（Identity + Controller）+ csi-provisioner + csi-resizer + csi-snapshotter + livenessprobe |
| `csi-nfs-node` | DaemonSet | `nfsplugin`（Identity + Node）+ node-driver-registrar + livenessprobe |

同一个 `nfsplugin` 二进制，在两种 Pod 里启用了不同的服务。所有 sidecar 都通过一个共享的 emptyDir 或 hostPath 目录里的 unix socket（`/csi/csi.sock`）和驱动通信，不走网络：

```go
// csi-driver-nfs/pkg/nfs/server.go（节选）
if ids != nil {
    csi.RegisterIdentityServer(server, ids)
}
if cs != nil {
    csi.RegisterControllerServer(server, cs)
}
if ns != nil {
    csi.RegisterNodeServer(server, ns)
}
```

## 三、注册：kubelet 怎么发现一个驱动

参考资料里常见的说法是「插件把可执行文件放到 hostPath 目录里，kubelet 去调用」。这是 FlexVolume 的做法，CSI 不是这样。**CSI 驱动和 kubelet 之间只交换 unix socket，没有可执行文件。**

### 3.1 两个 socket

DaemonSet 里挂了两个关键的 hostPath：

```yaml
# csi-driver-nfs/deploy/csi-nfs-node.yaml（节选）
- name: node-driver-registrar
  args:
    - --csi-address=/csi/csi.sock
    - --kubelet-registration-path=$(DRIVER_REG_SOCK_PATH)
  env:
    - name: DRIVER_REG_SOCK_PATH
      value: /var/lib/kubelet/plugins/csi-nfsplugin/csi.sock
volumes:
  - name: socket-dir
    hostPath:
      path: /var/lib/kubelet/plugins/csi-nfsplugin       # 驱动自己的 csi.sock
  - name: registration-dir
    hostPath:
      path: /var/lib/kubelet/plugins_registry            # kubelet 盯着的注册目录
```

- `/var/lib/kubelet/plugins/<driver>/csi.sock`：驱动的 gRPC 服务监听在这里，kubelet 之后的 Node 调用都发到这个 socket；
- `/var/lib/kubelet/plugins_registry/<driver>-reg.sock`：node-driver-registrar 创建的注册 socket。

### 3.2 registrar 只是一个「名片」服务

node-driver-registrar 启动后，先通过 `csi.sock` 调驱动的 `GetPluginInfo` 拿到驱动名，然后在注册目录里起一个 gRPC 服务：

```go
// node-driver-registrar/cmd/csi-node-driver-registrar/node_register.go（节选）
// as gRPC server which replies to registration requests initiated by kubelet's
// plugins watcher infrastructure. Node labeling is done by kubelet's csi code.
registrar := newRegistrationServer(csiDriverName, *kubeletRegistrationPath, supportedVersions)
socketPath := buildSocketPath(csiDriverName)   // /registration/<driver>-reg.sock
lis, err := net.Listen("unix", socketPath)
grpcServer := grpc.NewServer(opts...)
registerapi.RegisterRegistrationServer(grpcServer, registrar)
```

它只实现两个方法：

```go
type registrationServer struct {
    driverName string
    endpoint   string     // 驱动 csi.sock 在宿主机上的路径
    version    []string
}

func (e registrationServer) GetInfo(ctx context.Context, req *registerapi.InfoRequest) (*registerapi.PluginInfo, error)
func (e registrationServer) NotifyRegistrationStatus(ctx context.Context, status *registerapi.RegistrationStatus) (*registerapi.RegistrationStatusResponse, error)
```

`GetInfo` 告诉 kubelet「我叫什么、我的 csi.sock 在宿主机的哪个位置」。注意 `endpoint` 填的是**宿主机路径**（`--kubelet-registration-path`），而不是 registrar 容器里看到的 `/csi/csi.sock`，因为是 kubelet 要去连它。

### 3.3 kubelet 的 plugin watcher

kubelet 的 plugin watcher 用 fsnotify 监听 `plugins_registry` 目录。一旦出现新的 socket：

1. 调 `GetInfo` 拿到驱动名和 endpoint；
2. 交给 CSI 的 `RegistrationHandler.RegisterPlugin`，把驱动名到 endpoint 的映射存进内存；
3. 通过 endpoint 调驱动的 `NodeGetInfo`，拿到节点 ID 和拓扑信息，写进 `CSINode` 对象和节点 label；
4. 调 `NotifyRegistrationStatus` 告诉 registrar 注册成功。

```go
// kubernetes/pkg/volume/csi/csi_plugin.go（节选）
func (h *RegistrationHandler) RegisterPlugin(ctx context.Context, pluginName string, endpoint string, versions []string, pluginClientTimeout *time.Duration) error {
    klog.Info(log("Register new plugin with name: %s at endpoint: %s", pluginName, endpoint))
    // ...
    // Storing endpoint of newly registered CSI driver into the map, where CSI driver name will be the key
    csiDrivers.Set(pluginName, Driver{
        endpoint:                endpoint,
        highestSupportedVersion: highestSupportedVersion,
    })
    // ...
}
```

注册完成后可以用 `kubectl get csinode <node> -o yaml` 验证，`spec.drivers` 里会出现这个驱动和它的节点 ID。

这套设计的好处是：kubelet 不需要知道任何驱动，也不需要重启，驱动的 DaemonSet 一起来，注册自动完成；驱动 Pod 被删除，socket 消失，kubelet 也能感知到。

## 四、Provision：创建存储卷

### 4.1 external-provisioner 的工作

用户创建 PVC，StorageClass 的 `provisioner` 是 `nfs.csi.k8s.io`：

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: nfs-csi
provisioner: nfs.csi.k8s.io
parameters:
  server: nfs-server.default.svc.cluster.local
  share: /
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

external-provisioner 监听 PVC，发现 PVC 的 StorageClass 指向自己负责的驱动，就构造 `CreateVolumeRequest`（卷名、容量、访问模式、StorageClass 的 `parameters`）调驱动：

```go
// external-provisioner/pkg/controller/controller.go
rep, err := p.csiClient.CreateVolume(createCtx, req)
```

驱动返回 `volume_id` 和 `volume_context` 之后，**是 external-provisioner 而不是驱动**创建 PV 对象（同文件中的 `pv := &v1.PersistentVolume{...}`），把 `volume_id` 填进 `spec.csi.volumeHandle`。随后 Kubernetes 自己的 PV controller 把 PV 和 PVC 绑定起来。

驱动完全不碰 Kubernetes API，只负责「在后端存储上创建一个卷，告诉我它的 ID」。这就是 CSI 的边界：驱动可以被 Kubernetes 以外的编排系统复用（CSI 规范最初就是 Kubernetes、Mesos、Cloud Foundry 一起定的）。

### 4.2 NFS 驱动的 CreateVolume

对云盘来说，`CreateVolume` 是调云厂商 API 买一块盘。NFS 服务器本身已经存在，「创建卷」就变成了「在共享目录下建一个子目录」：

```go
// csi-driver-nfs/pkg/nfs/controllerserver.go（节选）
// Mount nfs base share so we can create a subdirectory
if err = cs.internalMount(ctx, nfsVol, parameters, volCap); err != nil {
    return nil, status.Errorf(codes.Internal, "failed to mount nfs server: %v", err)
}
defer func() {
    if err = cs.internalUnmount(ctx, nfsVol); err != nil {
        klog.Warningf("failed to unmount nfs server: %v", err)
    }
}()

// Create subdirectory under base-dir
internalVolumePath := getInternalVolumePath(cs.Driver.workingMountDir, nfsVol)
if err = os.MkdirAll(internalVolumePath, 0777); err != nil {
    return nil, status.Errorf(codes.Internal, "failed to make subdirectory: %v", err)
}
```

先把 NFS 共享根目录临时挂到 controller Pod 里，`mkdir` 一个以 PV 名命名的子目录，再卸载。返回的 `volume_id` 把服务器、根目录、子目录拼在一起（`getVolumeIDFromNfsVol`），之后所有接口都靠解析这个 ID 找回位置，驱动本身不需要任何数据库，是无状态的。`DeleteVolume` 反过来，按 `onDelete` 参数决定删除、保留还是归档这个子目录。

## 五、Attach：把卷交给节点

### 5.1 并不是所有存储都需要 Attach

Attach 的含义是「让节点能看到这块设备」，比如把一块 EBS 云盘挂到某台 EC2 上，节点上随即出现 `/dev/nvme1n1`。NFS、CephFS 这类网络文件系统，任何节点直接 `mount` 就行，没有这一步。

驱动通过 `CSIDriver` 对象声明自己需不需要 Attach：

```yaml
# csi-driver-nfs/deploy/csi-nfs-driverinfo.yaml
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: nfs.csi.k8s.io
spec:
  attachRequired: false
  fsGroupPolicy: File
```

kubelet 和 AttachDetach controller 都会读这个字段：

```go
// kubernetes/pkg/volume/csi/csi_plugin.go，skipAttach
if csiDriver.Spec.AttachRequired != nil && *csiDriver.Spec.AttachRequired == false {
    return true, nil
}
```

所以 csi-driver-nfs 的部署清单里根本没有 external-attacher。

### 5.2 需要 Attach 时的流程

对需要 Attach 的驱动，Pod 调度到节点后：

1. kube-controller-manager 里的 **AttachDetach controller** 发现「这个卷应该挂到这个节点」，在 `pkg/volume/csi` 里调用 `csiAttacher.Attach`，**创建一个 `VolumeAttachment` 对象**（写明驱动、PV 名、节点名），然后等它的 `status.attached` 变成 true；
2. **external-attacher** 监听到这个 `VolumeAttachment`，调驱动的 `ControllerPublishVolume`；
3. 驱动调云厂商 API 把盘挂到节点，返回 `publish_context`（比如设备路径）；
4. external-attacher 把 `status.attached` 置为 true，`publish_context` 写进 `status.attachmentMetadata`：

```go
// external-attacher/pkg/controller/util.go
func markAsAttached(ctx context.Context, client kubernetes.Interface, va *storage.VolumeAttachment, metadata map[string]string) (*storage.VolumeAttachment, error) {
    // ...
    clone.Status.Attached = true
    // ...
}
```

整个过程中，AttachDetach controller 和 external-attacher 之间没有任何直接调用，只通过 `VolumeAttachment` 对象交接。这和服务发现里 EndpointSlice 的作用一样：**用一个 API 对象，把 Kubernetes 内置组件和外部 sidecar 解耦。**

这一步只是让节点「看到」了设备，还没有任何目录被挂载。参考资料里把 `/var/lib/kubelet/pods/<Pod ID>/volumes/...` 算作 Attach 阶段的产物，其实是混淆了 Attach 和 Mount：那个目录是下一阶段 kubelet 才处理的。

## 六、Mount：从设备到容器里的目录

Mount 必须在节点上执行，由 kubelet 的 **VolumeManager** 驱动。它的 reconciler 循环比较「这个节点上应该挂载的卷」和「实际已挂载的卷」，不一致就调用 `pkg/volume/csi`，后者再通过 `csi.sock` 调驱动的 Node 服务。

### 6.1 两步挂载：Stage 和 Publish

| 步骤 | 接口 | 目标路径 | 粒度 | 典型操作 |
| --- | --- | --- | --- | --- |
| Stage | `NodeStageVolume` | `/var/lib/kubelet/plugins/kubernetes.io/csi/<driver>/<hash>/globalmount` | 每个卷每个节点一次 | 格式化块设备、挂载到全局目录 |
| Publish | `NodePublishVolume` | `/var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~csi/<pv>/mount` | 每个 Pod 一次 | 从全局目录 bind mount 到 Pod 目录 |

为什么要分两步？一块 `ReadWriteOnce` 的盘可能被同一节点上的多个 Pod 使用。格式化和挂载块设备只该做一次，放在 Stage；每个 Pod 只需要一个 bind mount，放在 Publish。

Stage 是可选的，驱动在 `NodeGetCapabilities` 里声明了 `STAGE_UNSTAGE_VOLUME` 才会被调用。csi-driver-nfs 没有声明它，所以只有 Publish 一步。

### 6.2 NFS 的 NodePublishVolume

```go
// csi-driver-nfs/pkg/nfs/nodeserver.go（节选）
server = getServerFromSource(server)
source := fmt.Sprintf("%s:%s", server, baseDir)
if subDir != "" {
    // replace pv/pvc name namespace metadata in subDir
    subDir = replaceWithMap(subDir, subDirReplaceMap)
    source = strings.TrimRight(source, "/")
    source = fmt.Sprintf("%s/%s", source, subDir)
}
// ...
klog.V(2).Infof("NodePublishVolume: volumeID(%v) source(%s) targetPath(%s) mountflags(%v)", volumeID, source, targetPath, mountOptions)
return ns.mounter.Mount(source, targetPath, "nfs", mountOptions)
```

就是一次 `mount -t nfs server:/share/<pv> <targetPath>`。`targetPath` 是 kubelet 传进来的 Pod 卷目录，驱动只管往这里挂。

这里有两个部署上的要求，缺一不可：

```yaml
# csi-driver-nfs/deploy/csi-nfs-node.yaml（节选）
securityContext:
  privileged: true                         # 容器里执行 mount 需要特权
volumeMounts:
  - name: pods-mount-dir
    mountPath: /var/lib/kubelet/pods
    mountPropagation: "Bidirectional"      # 容器里做的挂载要传播回宿主机
```

`mount` 是在驱动容器里执行的。默认情况下，容器内的挂载只在容器自己的 mount namespace 里可见。`mountPropagation: Bidirectional` 让这次挂载传播回宿主机，kubelet 和之后启动的业务容器才能看到。忘了配置这一项是自己写驱动时最常见的问题：驱动日志显示挂载成功，业务容器里却是空目录。

### 6.3 最后一步：交给容器运行时

Publish 完成后，Pod 目录下已经是一个挂好的文件系统。kubelet 通过 CRI 创建容器时，把这个目录作为 bind mount 传给容器运行时，映射到 `volumeMounts.mountPath`。

参考资料里说「调用 CRI 启动带有 volume 参数的 container，把磁盘 mount 到容器目录」，容易让人以为是 CSI 驱动或 CRI 去挂载磁盘。实际上，挂载早在 `NodePublishVolume` 时就完成了，CRI 只是做一次普通的 bind mount，它并不知道这是什么存储。

## 七、串起来

```text
1. 部署驱动
   DaemonSet: nfsplugin 监听 /var/lib/kubelet/plugins/<drv>/csi.sock
   registrar 在 /var/lib/kubelet/plugins_registry/<drv>-reg.sock 起注册服务
   kubelet plugin watcher → GetInfo → RegisterPlugin → NodeGetInfo → CSINode

2. 用户创建 PVC
   external-provisioner → Controller.CreateVolume → 后端建卷
   external-provisioner 创建 PV → PV controller 绑定 PV/PVC

3. Pod 调度到 node-1
   [attachRequired=true]  AD controller 创建 VolumeAttachment
                          external-attacher → Controller.ControllerPublishVolume → status.attached=true
   [attachRequired=false] 跳过

4. kubelet VolumeManager（node-1）
   [支持 STAGE] Node.NodeStageVolume  → .../globalmount
   Node.NodePublishVolume → /var/lib/kubelet/pods/<uid>/volumes/kubernetes.io~csi/<pv>/mount

5. kubelet → CRI 创建容器，bind mount 上一步的目录到 mountPath
```

删除时顺序完全相反：容器退出 → `NodeUnpublishVolume` → `NodeUnstageVolume` → 删除 VolumeAttachment → `ControllerUnpublishVolume` → 删除 PVC 时按 `reclaimPolicy` 决定是否 `DeleteVolume`。

## 八、除了四个阶段，CSI 还管什么

| 能力 | sidecar | CSI 接口 | 用户侧对象 |
| --- | --- | --- | --- |
| 扩容 | external-resizer | `ControllerExpandVolume` + `NodeExpandVolume` | 修改 PVC 的 `spec.resources.requests.storage`，需要 StorageClass `allowVolumeExpansion: true` |
| 快照 | external-snapshotter（另需 snapshot-controller 和 CRD） | `CreateSnapshot` / `DeleteSnapshot` | `VolumeSnapshot`、`VolumeSnapshotClass` |
| 克隆 / 从快照恢复 | external-provisioner | `CreateVolume` 带 `volume_content_source` | PVC 的 `dataSource` |
| 容量感知调度 | external-provisioner（`--enable-capacity`） | `GetCapacity` | `CSIStorageCapacity` |
| 卷监控 | kubelet | `NodeGetVolumeStats` | kubelet 指标 `kubelet_volume_stats_*` |
| 拓扑 | external-provisioner | `NodeGetInfo` 返回的 `accessible_topology` | StorageClass 的 `allowedTopologies`、`volumeBindingMode: WaitForFirstConsumer` |

模式都一样：一个 sidecar 监听一种 API 对象，翻译成一个 CSI 接口调用。新增一项能力，就是新增一个 sidecar 和一组接口，Kubernetes 核心几乎不用改。

拓扑值得多说一句。云盘通常只能挂到同一可用区的节点上。如果 StorageClass 用默认的 `Immediate`，PVC 一创建就会在随便某个可用区建盘，Pod 之后却可能被调度到另一个可用区，永远挂不上。改成 `WaitForFirstConsumer`，provisioner 会等 Pod 调度完、拿到节点所在的拓扑再建盘。

## 九、排查思路

| 现象 | 卡在哪一步 | 先看哪里 |
| --- | --- | --- |
| PVC 一直 `Pending` | Provision | `kubectl describe pvc` 的 Events、csi-provisioner 容器日志 |
| Pod 卡在 `ContainerCreating`，事件里有 `AttachVolume.Attach failed` | Attach | `kubectl get volumeattachment`、csi-attacher 日志 |
| 事件里有 `MountVolume.SetUp failed` | Mount | 该节点上 node DaemonSet 里驱动容器的日志、`journalctl -u kubelet` |
| 事件说 `driver name xxx not found in the list of registered CSI drivers` | 注册 | `kubectl get csinode <node>`、registrar 日志、`/var/lib/kubelet/plugins_registry/` |
| 驱动日志显示挂载成功，容器里是空目录 | Mount 传播 | node 容器的 `mountPropagation: Bidirectional` |
| 删除 PVC 后后端卷还在 | 回收 | PV 的 `reclaimPolicy`、驱动的 `DeleteVolume` 逻辑（NFS 驱动看 `onDelete`） |

## 十、总结

- CSI 把存储拆成两半：**Kubernetes 一侧只负责调用接口，存储厂商一侧实现 Identity、Controller、Node 三个 gRPC 服务。** 中间的 sidecar（registrar、provisioner、attacher、resizer、snapshotter）由社区维护，各家驱动通用。
- 判断一个操作属于 Controller 还是 Node：要不要在那台机器上执行。建卷、挂盘、快照属于 Controller；格式化、mount 属于 Node。
- 组件之间靠 **API 对象**（PVC、PV、VolumeAttachment、CSINode）和 **unix socket** 解耦。kubelet 通过注册目录里的 socket 发现驱动，没有可执行文件。
- 一个卷从 PVC 到容器目录要经过四步：注册、Provision、Attach（可选）、Mount（Stage 可选 + Publish）。Attach 只让节点看到设备，挂载发生在 Mount 阶段，CRI 最后只做一次 bind mount。
- 写驱动时最容易漏的是部署细节：`privileged`、`mountPropagation: Bidirectional`、`CSIDriver.attachRequired`，以及 registrar 的 `--kubelet-registration-path` 要填宿主机路径。

*参考：[CSI 规范](https://github.com/container-storage-interface/spec)、[Kubernetes CSI 开发者文档](https://kubernetes-csi.github.io/docs/)、张磊《深入剖析 Kubernetes》第 30 讲「编写自己的存储插件：FlexVolume 与 CSI」。*
