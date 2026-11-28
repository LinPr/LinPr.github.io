---
title: 两份 CSI 驱动对照着读
description: 同一套 CSI 规范，两份取舍几乎相反的驱动：hostpath 把接口几乎全实现，nfs 只保留文件存储真正需要的那些。对着源码逐篇对照，看清哪些是规范要求的、哪些是实现自己的选择。
---

CSI 驱动不 watch Kubernetes 对象，它只是一个被动的 gRPC 服务端：sidecar 把 PVC、VolumeAttachment 这些对象的变化翻译成 RPC 调用，kubelet 在挂载时也直接连上来。这个系列读两份驱动——Kubernetes CSI 组织维护的示例驱动 csi-driver-host-path，和在生产里广泛使用的 csi-driver-nfs——每一篇先看规范要求什么，再看两份实现各自怎么做，最后说差别说明了什么。
