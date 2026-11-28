---
title: 'csi 00 开篇：一个 PVC 要经过谁，才能变成容器里的目录'
date: 2026-11-28T17:00:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "PersistentVolumeClaim", "csi-driver-host-path"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "CSI 系列开篇。先分清树内卷、local-path-provisioner 和 CSI 驱动这三条链路，再看 CSI 怎么把驱动拆成 controller 与 node 两半、由哪些 sidecar 调用、一个 PVC 从创建到挂载要经过谁。系列读两份驱动——csi-driver-host-path 与 csi-driver-nfs，本篇先摆出两者在能力、部署和状态上的差别，后面每篇都对照着讲。"
---

一个 Pod 的 `volumes` 里写了一个 `persistentVolumeClaim`，从 `kubectl apply` 到容器里能读写 `/data`，中间经过至少六方：apiserver 记下 PVC、external-provisioner 建出卷、调度器挑节点、external-attacher 把卷挂到节点、kubelet 把它挂进容器、CSI 驱动在每一步响应 gRPC 请求。

这六方里只有最后一方是我们自己能写的。这个系列只读一份驱动：[`kubernetes-csi/csi-driver-host-path`](https://github.com/kubernetes-csi/csi-driver-host-path)，Kubernetes CSI 组织维护的示例驱动，驱动本体约 5000 行，CSI 规范里的方法基本都有实现。开篇先把「谁在什么时候调它」讲清楚，后面每一篇都在这张图上找位置。

本文基于 csi-driver-host-path 提交 `bfacf5a`（2026-11），sidecar 版本取自仓库里的 `deploy/kubernetes-latest`。

## 一、三条链路，只有一条是 CSI

讨论 CSI 时最容易混的，是「把本地磁盘给 Pod」这件事其实有四条完全不同的实现路线。它们目的相似，机制毫无关系：

| 路线 | 是不是 CSI | 谁在节点上干活 | 动态供给 | 快照、扩容 |
| --- | --- | --- | --- | --- |
| `hostPath`（树内卷） | 否 | kubelet 内置插件 | 不支持 | 不支持 |
| `local` PV（树内卷） | 否 | kubelet 内置插件 | 不支持，要手工建 PV | 不支持 |
| `local-path-provisioner` | 否 | 独立进程建 PV，kubelet 仍用内置插件挂载 | 支持 | 不支持 |
| CSI 驱动 | 是 | 驱动进程，kubelet 通过 gRPC 调用 | 支持 | 取决于驱动 |

前三条都不经过 gRPC，也没有 sidecar，所以它们不在这个系列的范围内。但有两个名字会反复出现，先把它们钉死：

1. 原生 `hostPath` 卷和原生 `local` 卷都是 kubelet 内置的卷插件。区别在于 `local` 卷会写 `nodeAffinity`，调度器知道它属于哪个节点，Pod 不会被调度到别的节点上去；`hostPath` 没有这层约束。
2. `local-path-provisioner` 常被当成「local PV 的动态供给器」，但它默认创建的是 **hostPath 类型的 PV**（源码里 `defaultVolumeType = "hostPath"`），只是顺手写好了 `nodeAffinity`。想拿到真正的 `local` 类型，得显式配置 `volumeType: local`。

之所以要花一节讲清这些，是因为驱动叫 `csi-driver-host-path`，它的卷目录也在宿主机上。**名字像、底层像，链路完全不一样**：这份驱动走的是完整的 CSI 流程，只是把「后端存储」换成了一个本地目录。

## 二、两份参考实现：hostpath 与 nfs

这个系列读两份驱动，不是一份。它们在同一个规范下，取舍几乎处处相反；对着看，才能分清哪些是规范要求的、哪些是某一份实现自己的选择。

1. [`csi-driver-host-path`](https://github.com/kubernetes-csi/csi-driver-host-path)：后端是宿主机上的一个目录。CSI 的接口它几乎全实现，附着、快照、扩容、卷健康、卷组快照都能打开，价值在「接口覆盖全」。
2. [`csi-driver-nfs`](https://github.com/kubernetes-csi/csi-driver-nfs)：后端是一台 NFS 服务器，一块卷就是服务器上的一个子目录。它没有 attach 的概念，没有 Stage 这一步，快照用 tar 打包，价值在「展示规范里哪些东西可以不要」。

把两者的能力表摆在一起，差别立刻出来：

| 对比项 | hostpath | nfs |
| --- | --- | --- |
| 后端 | 宿主机目录 | NFS 服务端的子目录 |
| Plugin 能力 | controller、卷组快照、拓扑（默认开） | 只有 controller |
| attach | 实现，默认关闭 | 完全没有 |
| Stage、Unstage | 声明了 | 没有声明 |
| 快照 | 复制卷内容 | tar 打包 |
| 状态 | 本地 `state.json` | 无状态，信息编码在 volume ID 里 |

后面每一篇都按同一顺序展开：先看规范要求什么，再看 hostpath 怎么做、nfs 怎么做，最后说差别说明了什么。第一个差别就在下一节：驱动被拆成两半之后，两份实现在部署上走向了两个方向。

## 三、CSI 把驱动拆成两半

{{< figure src="/images/posts/csi/00-architecture.svg" link="/images/posts/csi/00-architecture.svg" caption="任意 CSI 驱动的通用部署形态：controller 侧由 sidecar watch apiserver 的对象来触发调用，node 侧由 kubelet 在挂载时调用，两侧都只是连到驱动 unix socket 的 gRPC 客户端，后端存储由驱动自己决定（点击查看大图）" >}}

CSI 规范把驱动要提供的能力分成三个 gRPC service，每个驱动都要同时实现它们：

1. `Identity`：回答「你是谁、你还活着吗、你支持哪些能力」。所有驱动都必须实现。
2. `Controller`：建卷、删卷、快照、扩容、附着。这些操作管的是集群里的卷，整个集群只需要一份。
3. `Node`：把卷挂到本节点、再挂进容器。它要访问本机的设备和挂载点，每个节点都要有一份。

这两半在真实驱动里通常部署成两个工作负载：controller 侧是一个单副本的 `Deployment`，node 侧是一个跑在每个节点上的 `DaemonSet`。**驱动本身不 watch Kubernetes 对象**，它只是一个被动的 gRPC 服务端；所有「什么时候该建卷」「什么时候该挂载」的判断，都由 sidecar 和 kubelet 完成，它们再回调驱动。

上面这张图画的是**任意 CSI 驱动**都成立的形态，不是某一份驱动的部署。示例驱动 hostpath 在这里有两处偏离，第五节会对着这张图讲清楚。

| 组件 | 监听什么 | 调用驱动的哪个方法 |
| --- | --- | --- |
| `external-provisioner` | `PersistentVolumeClaim` | `CreateVolume`、`DeleteVolume` |
| `external-attacher` | `VolumeAttachment` | `ControllerPublishVolume`、`ControllerUnpublishVolume` |
| `external-snapshotter` | `VolumeSnapshot` | `CreateSnapshot`、`DeleteSnapshot`、`ListSnapshots` |
| `external-resizer` | PVC 的容量变化 | `ControllerExpandVolume` |
| `node-driver-registrar` | 无，启动时执行 | 把驱动的 socket 注册到 kubelet 的插件目录 |
| `liveness-probe` | 无，定期执行 | 调用 `Probe`，失败就重启驱动容器 |

这张表解释了一个新手常见的误解：**驱动并不「知道」有 Pod 要用卷，是 sidecar 把对象变化翻译成了 RPC 调用。**

## 四、一次 PVC 的完整时序

把上面这些组件串起来，一个 PVC 的完整路径如下：

| 步骤 | 谁触发 | 做什么 | 集群里发生了什么 |
| --- | --- | --- | --- |
| 1 | `kubectl apply` | 创建 PVC | apiserver 保存 PVC，状态 `Pending` |
| 2 | `external-provisioner` | `CreateVolume` | 创建 PV，与 PVC 绑定 |
| 3 | 创建引用它的 Pod | 调度器 `VolumeBinding` 检查 | Pod 绑定到某个节点 |
| 4 | `external-attacher` | `ControllerPublishVolume` | 创建 `VolumeAttachment` 并标记为已附着（仅当驱动声明了这个能力） |
| 5 | kubelet | `NodeStageVolume`、`NodePublishVolume` | 容器里出现挂载点 |
| 6 | 删除 Pod | `NodeUnpublishVolume`、`NodeUnstageVolume` | 挂载点消失 |
| 7 | 删除 PVC | `DeleteVolume` | 后端数据删除，PV 回收 |

第 2 步什么时候发生，由 StorageClass 的 `volumeBindingMode` 决定。仓库里的示例清单是这么写的：

```yaml {linenos=inline}
# examples/csi-storageclass.yaml 与 examples/csi-pvc.yaml
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
  name: csi-pvc
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  storageClassName: csi-hostpath-sc
```

`provisioner`（第 6 行）必须等于驱动在 `CSIDriver` 对象里登记的名字 `hostpath.csi.k8s.io`，否则 provisioner 不会认领这个 PVC。`volumeBindingMode: Immediate`（第 8 行）表示 PVC 一创建就建卷；如果改成 `WaitForFirstConsumer`，第 2 步会推迟到 Pod 调度之后，让驱动知道卷要落在哪个节点上。

**hostpath 默认没有开启 attach，所以第 4 步平时不会发生。** 部署清单里虽然有 `csi-attacher`，但驱动启动时没有传 `--enable-attach`，`ControllerGetCapabilities` 就不会返回 `PUBLISH_UNPUBLISH_VOLUME`，attacher 认为这个驱动不需要附着，既不创建 `VolumeAttachment`，也不会调用 `ControllerPublishVolume`。读代码时看到 `ControllerPublishVolume` 实现得很简单，原因就在这里，第 05 篇会展开。

## 五、这份部署为什么只有一个 Pod

前面说 controller 侧和 node 侧通常分成两个工作负载，hostpath 却把它们塞进了同一个 Pod。部署清单里只有一个工作负载，`replicas` 写死为 1：

```yaml {linenos=inline}
# deploy/kubernetes-latest/hostpath/csi-hostpath-plugin.yaml
kind: StatefulSet
apiVersion: apps/v1
metadata:
  name: csi-hostpathplugin
# ...
spec:
  serviceName: "csi-hostpathplugin"
  # One replica only:
  # Host path driver only works when everything runs
  # on a single node.
  replicas: 1
```

注释写得很直白：这个驱动只有所有东西都跑在一个节点上才成立。原因在存储侧：卷的数据放在宿主机的 `/var/lib/csi-hostpath-data/` 下，节点之间没有任何共享，Pod 一旦被调度到别的节点，数据就找不到了。既然只有单节点，controller 和 node 分开部署就没有意义，索性合成一个 Pod。

这个 Pod 里一共七个容器：驱动本体 `hostpathplugin`，加 `node-driver-registrar`、`liveness-probe`、`csi-attacher`、`csi-provisioner`、`csi-resizer`、`csi-snapshotter` 六个 sidecar。驱动本体的启动参数只有四个：

```yaml {linenos=inline}
# deploy/kubernetes-latest/hostpath/csi-hostpath-plugin.yaml
        - name: hostpath
          image: registry.k8s.io/sig-storage/hostpathplugin:v1.18.0
          args:
            - "--drivername=hostpath.csi.k8s.io"
            - "--v=5"
            - "--endpoint=$(CSI_ENDPOINT)"
            - "--nodeid=$(KUBE_NODE_NAME)"
```

`--drivername`（第 5 行）决定 `CSIDriver` 对象和 StorageClass 里写的名字；`--endpoint`（第 7 行）就是那个 unix socket，取值为 `unix:///csi/csi.sock`；`--nodeid`（第 8 行）来自 Pod 的 `spec.nodeName`，`NodeGetInfo` 返回的就是它。

真实驱动不会这么部署。它们的 controller 是 `Deployment`，node 是 `DaemonSet`，两份二进制可以相同，也可以不同；node 侧还要挂载 `/var/lib/kubelet/pods` 并开 `mountPropagation: Bidirectional`，才能把挂载点传播给 kubelet。读这份示例时，**心里要留一份「正常情况下应该是两个工作负载」的对照**，否则容易把单节点的简化当成 CSI 的通用做法。

## 六、这两份代码能教你什么，不能教你什么

值得读的地方：

1. 每个 RPC 的请求和响应长什么样，必填字段有哪些，哪些字段可以空着；
2. 幂等怎么实现：重复创建同名卷、重复删除已删除的卷，分别返回什么；
3. 能力协商的完整链条：`GetPluginCapabilities`、`ControllerGetCapabilities`、`NodeGetCapabilities` 三张表分别影响谁；
4. 一个真正的挂载流程：从 `NodeStageVolume` 的 bind mount 到 `NodePublishVolume` 的目标路径；
5. sidecar 和驱动的配合方式，包括 registrar 怎么让 kubelet 发现驱动。

不适合照抄的地方：

1. 状态存在本地 JSON 文件里，节点一换就丢，真实驱动必须存在后端或 Kubernetes 对象里；
2. 没有跨节点能力，也没有高可用；
3. 没有后端 API 交互、重试、限流、指标这些工程化内容；
4. 错误处理基本只覆盖了正常路径。

把两份驱动放在一起看，才能看到最关键的一条：**规范没有要求实现全部方法，能力表就是用来声明「这部分我不做」的。** hostpath 把接口几乎全实现了一遍，nfs 只保留文件存储真正需要的那些——没有 attach、没有 Stage、没有拓扑。两种都合法，选择取决于后端是什么。

## 七、系列路线

后面九篇都按「规范怎么说 → hostpath 怎么做 → nfs 怎么做」的顺序展开：

1. 01 CSI 规范：能力协商决定了谁在什么时候调用你（两份能力表先对照一遍）
2. 02 驱动进程：从命令行参数到 unix socket，再到自我介绍（两份启动路径的差别）
3. 03 状态存储：hostpath 的 state.json 与 nfs 的无状态
4. 04 CreateVolume 与 DeleteVolume：幂等、容量和错误码（两份实现对照）
5. 05 附着、快照与扩容：nfs 为什么一条都用不上
6. 06 NodeStage 与 NodePublish：两步挂载和一步挂载
7. 07 节点侧其余接口：块设备、能力声明、拓扑与卷健康
8. 08 跟着一个 5Gi 的 PVC 走完全程：两份驱动并排
9. 09 两份驱动放在一起看：CSI 的共性与取舍
10. 10 csi-sanity：用规范测试验收一个驱动

读每一篇时可以带着三个问题：这个接口由谁调用、它必须幂等吗、两份驱动分别选择做还是不做。三个问题答得上来，这个接口就算读懂了。
