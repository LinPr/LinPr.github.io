---
title: 从零读懂一个 CSI 驱动
description: 跟着 csi-driver-host-path 的源码，把 CSI 的 controller 与 node 两半、sidecar 的调用方式、一个 PVC 的完整生命周期走一遍，读懂一份生产级存储驱动的骨架。
---

CSI 驱动不 watch Kubernetes 对象，它只是一个被动的 gRPC 服务端：sidecar 把 PVC、VolumeAttachment 这些对象的变化翻译成 RPC 调用，kubelet 在挂载时也直接连上来。这个系列只读一份驱动——Kubernetes CSI 组织维护的示例驱动 csi-driver-host-path，把它的三个 service、状态存储、幂等与错误码、挂载流程逐个拆开，最后用 csi-sanity 和另一份驱动 csi-driver-nfs 做对照。
