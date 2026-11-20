---
title: 'scheduler-plugins [in-tree] NodeVolumeLimits：一个节点还能再挂几块盘'
date: 2026-11-19T09:40:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "存储", "csi"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "云盘能挂到一台机器上的数量有硬上限，NodeVolumeLimits 负责在调度时把这个上限算进去。本文拆解它如何从 CSINode 读取 allocatable.count、如何把 PVC、generic ephemeral 卷、in-tree 迁移卷统一换算成「驱动名/volumeHandle」去重计数、为什么还要数 VolumeAttachment，以及 VolumeLimitScaling、QueueingHint 和抢占中的几个细节。"
---

上一篇 [VolumeRestrictions](/posts/sched-plugin-09-volume-restrictions/) 检查的是「同一块盘能不能被两个 Pod 用」。接下来的问题是：这个节点还能不能再挂一块盘。

AWS 上一台 Nitro 实例能挂的 EBS 卷数量有限，GCE、Azure 也都有类似的上限。超过之后 attach 会失败，Pod 卡在 `ContainerCreating`。`NodeVolumeLimits` 把这个上限前移到调度阶段。本文基于 Kubernetes master 提交 `aabdf5a2`。它在默认 Profile 的 MultiPoint 列表里紧跟 `VolumeRestrictions`，默认启用，实现了 PreFilter、Filter、`EnqueueExtensions`、`SignPlugin`，没有 Score，没有 Args。插件的启用不受 feature gate 控制，但其中一段逻辑受 `VolumeLimitScaling`（1.37 beta，默认开启）控制。

## 一、名字的来历：插件叫 NodeVolumeLimits，结构体叫 CSILimits

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
// CSILimits is a plugin that checks node volume limits.
type CSILimits struct {
	csiManager      fwk.CSIManager
	pvLister        corelisters.PersistentVolumeLister
	pvcLister       corelisters.PersistentVolumeClaimLister
	scLister        storagelisters.StorageClassLister
	vaLister        storagelisters.VolumeAttachmentLister
	csiDriverLister storagelisters.CSIDriverLister
	vaIndexer       cache.Indexer

	randomVolumeIDPrefix                               string
	enableVolumeLimitScaling                           bool
	enableInPlacePodVerticalScalingSchedulerPreemption bool
	translator                                         InTreeToCSITranslator
}

// CSIName is the name of the plugin used in the plugin registry and configurations.
const CSIName = names.NodeVolumeLimits
```

`nodevolumelimits` 包里曾经并存过好几个插件：`EBSLimits`、`GCEPDLimits`、`AzureDiskLimits`、`CinderLimits` 分别按 in-tree 卷类型计数，`NodeVolumeLimits`（即 `CSILimits`）只管 CSI 卷。随着 CSI 迁移完成，in-tree 卷在节点上实际都由 CSI 驱动挂载，按类型分开数既重复又不准，那几个插件陆续被删掉，只剩下这一个。结构体名和常量名 `CSIName` 是那个年代留下的。代码里还有一句注释「If the volume is not migratable, it will be count in non_csi filter」，所说的 non-CSI filter 已经不存在了：不能迁移的 in-tree 卷现在根本不计数。

字段里有三处与众不同：

- `csiManager` 不是自己建的 lister，而是 `handle.SharedCSIManager()`。它在 `pkg/scheduler/framework_init.go` 里用 CSINode lister 构造，通过 `frameworkruntime.WithSharedCSIManager` 注入，接口定义在 fwk 包的 `CSIManager`，供所有插件共享 CSINode 视图。
- `vaIndexer` 是给 VolumeAttachment informer 加的按 `spec.nodeName` 的索引，键名 `va.spec.nodename`。Filter 每个节点都要查「挂在这个节点上的 VA」，没有索引就得每次全量遍历。
- `randomVolumeIDPrefix` 在 `NewCSI` 里用 `rand.String(32)` 生成，用于给还没绑定 PV 的 PVC 造一个假的 volumeHandle，后面细说。

## 二、上限从哪里来

上限只有一个来源：`CSINode.spec.drivers[].allocatable.count`。

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
func getVolumeLimits(csiNode *storagev1.CSINode) map[string]int64 {
	nodeVolumeLimits := make(map[string]int64)
	if csiNode == nil {
		return nodeVolumeLimits
	}
	for _, d := range csiNode.Spec.Drivers {
		if d.Allocatable != nil && d.Allocatable.Count != nil {
			nodeVolumeLimits[d.Name] = int64(*d.Allocatable.Count)
		}
	}
	return nodeVolumeLimits
}
```

这个值由 kubelet 写入。驱动通过 node-driver-registrar 注册时，kubelet 调用驱动的 `NodeGetInfo`，把响应里的 `max_volumes_per_node` 交给 `nodeInfoManager.InstallCSIDriver`，写进本节点 CSINode 对应驱动的 `allocatable.count`（`pkg/volume/csi/csi_plugin.go`）。驱动返回 0 表示不限制，此时 `allocatable` 不设，`getVolumeLimits` 结果里就没有这个驱动。注册流程本身可以参考 [CSI 工作流程](/posts/k8s-csi-workflow/) 一文的「注册」一节。

一个典型的 CSINode：

```yaml
apiVersion: storage.k8s.io/v1
kind: CSINode
metadata:
  name: ip-10-0-1-23
  annotations:
    storage.alpha.kubernetes.io/migrated-plugins: kubernetes.io/aws-ebs
spec:
  drivers:
  - name: ebs.csi.aws.com
    nodeID: i-0123456789abcdef0
    allocatable:
      count: 25
    topologyKeys: ["topology.ebs.csi.aws.com/zone"]
```

更早的版本把上限放在 `Node.status.allocatable` 里，资源名形如 `attachable-volumes-aws-ebs`、`attachable-volumes-csi-<driver>`，也曾允许用环境变量覆盖。现在调度器完全不读 Node 上的这些字段，只认 CSINode。

`allocatable.count` 原本是不可变的。`MutableCSINodeAllocatableCount` 在 1.36 GA 并锁定开启之后，CSIDriver 可以设置 `nodeAllocatableUpdatePeriodSeconds`，kubelet 会按周期重新调用 `NodeGetInfo` 刷新上限，并在 attach 返回 `ResourceExhausted` 时立即刷新（`pkg/volume/csi/csi_node_updater.go`）。这解决了「节点上被别的系统挂了卷，实际可用槽位比上报的少」的问题，也解释了为什么后面 QueueingHint 要专门比较 CSINode 前后的 count。

## 三、PreFilter：只判断要不要 Skip

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
func (pl *CSILimits) PreFilter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// ...
	volumes := pod.Spec.Volumes
	for i := range volumes {
		vol := &volumes[i]
		if vol.PersistentVolumeClaim != nil || vol.Ephemeral != nil || pl.translator.IsInlineMigratable(vol) {
			return nil, nil
		}
	}

	return nil, fwk.NewStatus(fwk.Skip)
}
```

只有三种卷会被计数：PVC、generic ephemeral 卷、可迁移的 in-tree 内联卷（`IsInlineMigratable` 由 csi-translation-lib 判断，比如内联的 `awsElasticBlockStore`）。ConfigMap、Secret、emptyDir 这类不占挂载槽位的卷，或者 Pod 根本没有卷，PreFilter 就返回 `Skip`，Filter 不会被调用。

PreFilter 不写 CycleState，`PreFilterExtensions` 返回 nil。所有计算都放在 Filter 里按节点现算，原因后面讲。

## 四、Filter：把所有卷换算成同一种身份再数

### 4.1 主流程

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
func (pl *CSILimits) Filter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	// ...
	csiNode, err := pl.csiManager.CSINodes().Get(node.Name)
	if err != nil {
		// TODO: return the error once CSINode is created by default (2 releases)
		logger.V(5).Info("Could not get a CSINode object for the node", "node", klog.KObj(node), "err", err)
	}

	// Count CSI volumes from the new pod
	newVolumes := make(map[string]string)
	if err := pl.filterAttachableVolumes(logger, pod, csiNode, true /* new pod */, newVolumes); err != nil {
		if apierrors.IsNotFound(err) {
			// PVC is not found. This Pod will never be schedulable until PVC is created.
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
		}
		return fwk.AsStatus(err)
	}

	// If the pod doesn't have any new CSI volumes, the predicate will always be true
	if len(newVolumes) == 0 {
		return nil
	}

	if pl.enableVolumeLimitScaling {
		// ... 驱动未安装检查，见第五节
	}

	// If the node doesn't have volume limits, the predicate will always be true
	nodeVolumeLimits := getVolumeLimits(csiNode)
	if len(nodeVolumeLimits) == 0 {
		return nil
	}

	// Count CSI volumes from existing pods
	attachedVolumes := make(map[string]string)
	for _, existingPod := range nodeInfo.GetPods() {
		if err := pl.filterAttachableVolumes(logger, existingPod.GetPod(), csiNode, false /* existing pod */, attachedVolumes); err != nil {
			return fwk.AsStatus(err)
		}
	}

	attachedVolumeCount := map[string]int{}
	for volumeUniqueName, driverName := range attachedVolumes {
		// Don't count single volume used in multiple pods more than once
		delete(newVolumes, volumeUniqueName)
		attachedVolumeCount[driverName]++
	}

	// Count CSI volumes from VolumeAttachments
	volumeAttachments, err := pl.getNodeVolumeAttachmentInfo(logger, node.Name)
	if err != nil {
		return fwk.AsStatus(err)
	}

	for volumeUniqueName, driverName := range volumeAttachments {
		// Avoid double-counting volumes already used by existing pods
		if _, exists := attachedVolumes[volumeUniqueName]; !exists {
			attachedVolumeCount[driverName]++
		}
	}

	// Count the new volumes count per driver
	newVolumeCount := map[string]int{}
	for _, driverName := range newVolumes {
		newVolumeCount[driverName]++
	}

	for driverName, count := range newVolumeCount {
		maxVolumeLimit, ok := nodeVolumeLimits[driverName]
		if ok {
			currentVolumeCount := attachedVolumeCount[driverName]
			// ...
			if currentVolumeCount+count > int(maxVolumeLimit) {
				return fwk.NewStatus(fwk.Unschedulable, ErrReasonMaxVolumeCountExceeded)
			}
		}
	}

	return nil
}
```

整个算法围绕一个 map：键是 `驱动名/volumeHandle`（`getVolumeUniqueName`），值是驱动名。新 Pod 的卷、节点上已有 Pod 的卷、节点上的 VolumeAttachment，三路来源都换算成这个键，靠 map 天然去重：

1. 新 Pod 的卷进 `newVolumes`；
2. 已有 Pod 的卷进 `attachedVolumes`，同时从 `newVolumes` 里删掉相同的键，**与已有 Pod 共享的卷不算新增**；
3. VolumeAttachment 里不在 `attachedVolumes` 中的卷，额外计入已用数；
4. 按驱动比较「已用 + 新增」与上限，超过则 `Unschedulable`，原因 `node(s) exceed max volume count`。

{{< figure src="/images/posts/sched-plugins/10-volume-count.svg" caption="NodeVolumeLimits 的计数过程：三路来源统一成「驱动名/volumeHandle」键，去重后按驱动与 CSINode 上限比较（点击查看大图）" link="/images/posts/sched-plugins/10-volume-count.svg" >}}

### 4.2 算例

节点上 `ebs.csi.aws.com` 的 `allocatable.count` 为 4。

- 已有 Pod A 使用 `pvc-1`（PV 句柄 `vol-1`）、`pvc-2`（`vol-2`）；
- 已有 Pod B 使用 `pvc-2`（同一个 `vol-2`）和一个内联 `awsElasticBlockStore`（`volumeID: vol-3`，经迁移翻译为 `ebs.csi.aws.com/vol-3`）；
- 节点上有四个 VA：`vol-1`、`vol-2`、`vol-4` 指向 CSI PV，另一个是 `vol-3` 内联卷的 VA（`source.inlineVolumeSpec`，没有 `persistentVolumeName`）；
- 新 Pod 使用 `pvc-2` 和一个尚未绑定、StorageClass 为 `WaitForFirstConsumer` 的 `pvc-new`。

计算过程：

| 步骤 | 结果 |
| --- | --- |
| `attachedVolumes` | `{ebs/vol-1, ebs/vol-2, ebs/vol-3}`，`vol-2` 被两个 Pod 共享只算一次，计数 3 |
| `newVolumes` 去重前 | `{ebs/vol-2, ebs/<prefix>-default/pvc-new}` |
| `newVolumes` 去重后 | `vol-2` 已在节点上，删掉，剩 1 个 |
| VA 补充 | `vol-1`、`vol-2` 已计入；`vol-4` 不在 `attachedVolumes`，+1；`vol-3` 的 VA 没有 PV 名被跳过。已用 4 |
| 判定 | 4 + 1 = 5 > 4，返回 `Unschedulable` |

如果 `vol-4` 那条 VA 不存在，已用数是 3，3 + 1 = 4 不超过 4，节点通过。`vol-4` 典型的来源是：用它的 Pod 刚被删除，kubelet 还没卸载完、attach/detach 控制器还没执行 detach。此时卷仍然实实在在地占着实例上的一个挂载槽位，只看 Pod 会少算。这正是 VA 计数被加进来的原因。

### 4.3 filterAttachableVolumes：三种卷的入口

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
		switch {
		case vol.PersistentVolumeClaim != nil:
			// Normal CSI volume can only be used through PVC
			pvcName = vol.PersistentVolumeClaim.ClaimName
		case vol.Ephemeral != nil:
			// ...
			pvcName = ephemeral.VolumeClaimName(pod, &vol)
			isEphemeral = true
		default:
			// Inline Volume does not have PVC.
			// Need to check if CSI migration is enabled for this inline volume.
			// ...
			if err := pl.checkAttachableInlineVolume(logger, &vol, csiNode, pod, result); err != nil {
				return err
			}

			continue
		}
		// ...
		pvc, err := pl.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)

		if err != nil {
			if newPod {
				// The PVC is required to proceed with
				// scheduling of a new pod because it cannot
				// run without it. Bail out immediately.
				return fmt.Errorf("looking up PVC %s/%s: %w", pod.Namespace, pvcName, err)
			}
			// If the PVC is invalid, we don't count the volume because
			// there's no guarantee that it belongs to the running predicate.
			logger.V(5).Info("Unable to look up PVC info", "pod", klog.KObj(pod), "PVC", klog.KRef(pod.Namespace, pvcName))
			continue
		}
```

新 Pod 和已有 Pod 的容错不对称。新 Pod 的 PVC 查不到，错误用 `%w` 包装后上抛，Filter 用 `apierrors.IsNotFound` 识别出来（`IsNotFound` 走 `errors.As`，能穿透包装），返回 `UnschedulableAndUnresolvable`；已有 Pod 的 PVC 查不到只是跳过不计。已有 Pod 已经在节点上，它的 PVC 被删了也不该阻止新 Pod 调度，宁可少算。

generic ephemeral 卷生成的 PVC 名是 `<pod名>-<卷名>`，还要用 `ephemeral.VolumeIsForPod` 校验 PVC 的 owner 确实是这个 Pod，防止同名的无关 PVC 被误用。

### 4.4 getCSIDriverInfo：PVC 到 `驱动/句柄` 的换算

这是整个插件最核心的一步。根据 PVC 的状态分三条路：

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
func (pl *CSILimits) getCSIDriverInfo(logger klog.Logger, csiNode *storagev1.CSINode, pvc *v1.PersistentVolumeClaim) (string, string) {
	pvName := pvc.Spec.VolumeName

	if pvName == "" {
		logger.V(5).Info("Persistent volume had no name for claim", "PVC", klog.KObj(pvc))
		return pl.getCSIDriverInfoFromSC(logger, csiNode, pvc)
	}

	pv, err := pl.pvLister.Get(pvName)
	if err != nil {
		// ...
		return pl.getCSIDriverInfoFromSC(logger, csiNode, pvc)
	}

	csiSource := pv.Spec.PersistentVolumeSource.CSI
	if csiSource == nil {
		// We make a fast path for non-CSI volumes that aren't migratable
		if !pl.translator.IsPVMigratable(pv) {
			return "", ""
		}

		pluginName, err := pl.translator.GetInTreePluginNameFromSpec(pv, nil)
		// ...
		if !isCSIMigrationOn(csiNode, pluginName) {
			logger.V(5).Info("CSI Migration of plugin is not enabled", "plugin", pluginName)
			return "", ""
		}

		csiPV, err := pl.translator.TranslateInTreePVToCSI(logger, pv)
		// ...
		csiSource = csiPV.Spec.PersistentVolumeSource.CSI
	}

	return csiSource.Driver, csiSource.VolumeHandle
}
```

**已绑定的 CSI PV**：直接取 `spec.csi.driver` 和 `spec.csi.volumeHandle`。

**已绑定的 in-tree PV**：比如 `spec.awsElasticBlockStore.volumeID: aws://us-east-1a/vol-0abc`。先确认可迁移，再用 csi-translation-lib 翻译。翻译不只是改字段名，EBS 的 `KubernetesVolumeIDToEBSVolumeID` 会把 `aws://us-east-1a/vol-0abc` 这种 URL 形式规整成 `vol-0abc`，驱动名换成 `ebs.csi.aws.com`。这样同一块盘无论以 in-tree PV、内联卷还是原生 CSI PV 出现，算出来的键都是 `ebs.csi.aws.com/vol-0abc`，去重才成立。

**未绑定的 PVC**：典型场景是 `WaitForFirstConsumer` 的 PVC，要等调度器选定节点后才会供给。此时没有 PV，只能看 StorageClass：

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
	// We use random prefix to avoid conflict with volume IDs. If PVC is bound during the execution of the
	// predicate and there is another pod on the same node that uses same volume, then we will overcount
	// the volume and consider both volumes as different.
	volumeHandle := fmt.Sprintf("%s-%s/%s", pl.randomVolumeIDPrefix, namespace, pvcName)

	provisioner := storageClass.Provisioner
	if pl.translator.IsMigratableIntreePluginByName(provisioner) {
		if !isCSIMigrationOn(csiNode, provisioner) {
			// ...
			return "", ""
		}

		driverName, err := pl.translator.GetCSINameFromInTreeName(provisioner)
		// ...
		return driverName, volumeHandle
	}

	return provisioner, volumeHandle
```

驱动名取 StorageClass 的 `provisioner`（in-tree provisioner 如 `kubernetes.io/aws-ebs` 换算成 CSI 驱动名），句柄则是 `<随机前缀>-<命名空间>/<PVC名>`。这个设计有两层用意：

- 随机前缀保证假句柄不会与任何真实 volumeHandle 撞上；
- 同一个未绑定 PVC 在同一个调度器实例里总是得到相同的假句柄。比如一个 assumed Pod 已经引用了 `pvc-new`、正在等待卷供给，第二个 Pod 也引用 `pvc-new`，两边的键一致，可以正确去重。

代价写在注释里：如果 PVC 恰好在两次计算之间完成绑定，一边用真实句柄、一边用假句柄，同一块盘会被算两次。这是偏保守的误差，只会多拒，不会多放。

StorageClass 名为空或查不到时直接返回空，不计数。注释给出的理由是：没有 StorageClass 的未绑定 PVC 只能是 Immediate 模式，按理应该已经绑定，那是 `VolumeBinding` 要拦的事。

### 4.5 isCSIMigrationOn：比 VolumeBinding 宽松

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/utils.go
func isCSIMigrationOn(csiNode *storagev1.CSINode, pluginName string) bool {
	if csiNode == nil || len(pluginName) == 0 {
		return false
	}

	// In-tree storage to CSI driver migration feature should be enabled,
	// along with the plugin-specific one
	switch pluginName {
	case csilibplugins.AWSEBSInTreePluginName:
		return true
	case csilibplugins.PortworxVolumePluginName:
		return true
	case csilibplugins.GCEPDInTreePluginName:
		return true
	case csilibplugins.AzureDiskInTreePluginName:
		return true
	case csilibplugins.CinderInTreePluginName:
		return true
	default:
		return false
	}
}
```

各插件的迁移 feature gate 早已 GA 并移除，这里写死了五个块存储插件，只要节点有 CSINode 就认为迁移已开启。而下一篇 `VolumeBinding` 的同名逻辑 `isPluginMigratedToCSIOnNode` 还会检查 CSINode 上的 `storage.alpha.kubernetes.io/migrated-plugins` 注解是否包含该插件。两边判断标准不一致，在 CSINode 存在但注解未包含某插件的节点上，NodeVolumeLimits 会把 in-tree 卷按 CSI 计数，而 VolumeBinding 不会翻译它的拓扑。实际集群里 kubelet 会在启动时写好这个注解，两者结果通常一致。

还有一个隐含前提：CSINode 不存在时（节点刚加入、kubelet 还没创建），所有 in-tree 卷都不计数，`getVolumeLimits` 也拿不到任何上限，Filter 直接通过。注释里的 TODO 说等 CSINode 默认创建之后要改成报错，这个 TODO 已经存在了很多个版本。

### 4.6 VolumeAttachment 的计数

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
func (pl *CSILimits) getNodeVolumeAttachmentInfo(logger klog.Logger, nodeName string) (map[string]string, error) {
	volumeAttachments := make(map[string]string)
	vas, err := pl.vaIndexer.ByIndex(vaIndexKey, nodeName)
	// ...
	for _, vao := range vas {
		// ...
			if va.Spec.Source.PersistentVolumeName == nil {
				logger.V(5).Info("VolumeAttachment has no PV name", "VolumeAttachment", klog.KObj(va))
				continue
			}
			pv, err := pl.pvLister.Get(*va.Spec.Source.PersistentVolumeName)
			// ...
			if pv.Spec.CSI == nil {
				logger.V(5).Info("PV is not a CSI volume", "PV", klog.KObj(pv))
				continue
			}
			volumeID := getVolumeUniqueName(va.Spec.Attacher, pv.Spec.CSI.VolumeHandle)
			volumeAttachments[volumeID] = va.Spec.Attacher
		// ...
	}
	return volumeAttachments, nil
}
```

VA 只覆盖「指向原生 CSI PV」的那部分：内联卷的 VA（`source.inlineVolumeSpec`）和 in-tree PV 的 VA 都被跳过，它们只能靠 Pod 那一路计数。只有 `attachRequired: true` 的驱动才会产生 VA，NFS 这类不需要 attach 的驱动不受这一路影响。

## 五、VolumeLimitScaling：驱动没装好就别来

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
	if pl.enableVolumeLimitScaling {
		for _, driverName := range newVolumes {
			driverInstalled, err := pl.checkCSIDriverOnNode(driverName, csiNode)
			if err != nil {
				return fwk.AsStatus(err)
			}
			if !driverInstalled {
				driverNotInstalledMsg := fmt.Sprintf("%s CSI driver is not installed on the node", driverName)
				return fwk.NewStatus(fwk.Unschedulable, driverNotInstalledMsg)
			}
		}
	}
```

`checkCSIDriverOnNode` 先查 CSIDriver 对象：不存在（可能不是 CSI 驱动）或 `spec.preventPodSchedulingIfMissing` 未设为 true，都视为「已安装」放行；驱动明确 opt-in 时，才要求节点的 CSINode 里有这个驱动。

这解决的是节点刚启动的窗口期：DaemonSet 形式的 CSI node 插件还没跑起来，CSINode 里没有这个驱动，也就没有上限，老逻辑会直接放行，Pod 调度上去之后挂载失败。对 cluster-autoscaler 也很重要：它在模拟新节点时调用调度器插件，有了这条检查，模拟节点必须同时模拟出 CSINode，否则模拟结论会和实际不符。API 注释里专门提醒了这一点。

两个值得注意的细节：

- 返回的是 `Unschedulable` 而不是 `UnschedulableAndUnresolvable`。驱动没装好不可能通过驱逐 Pod 解决，DefaultPreemption 会对这些节点做一遍注定失败的模拟。
- 检查发生在 `getVolumeLimits` 之前，所以即使驱动没有声明上限，也会被这条拦住。

## 六、EventsToRegister 与 QueueingHint

```go
// pkg/scheduler/framework/plugins/nodevolumelimits/csi.go
func (pl *CSILimits) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// We don't register any `QueueingHintFn` intentionally
		// because any new CSINode could make pods that were rejected by CSI volumes schedulable.
		{Event: fwk.ClusterEvent{Resource: fwk.CSINode, ActionType: fwk.Add}},
		{Event: fwk.ClusterEvent{Resource: fwk.CSINode, ActionType: fwk.Update}, QueueingHintFn: pl.isSchedulableAfterCSINodeUpdated},
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodDeleted},
		{Event: fwk.ClusterEvent{Resource: fwk.PersistentVolumeClaim, ActionType: fwk.Add}, QueueingHintFn: pl.isSchedulableAfterPVCAdded},
		{Event: fwk.ClusterEvent{Resource: fwk.VolumeAttachment, ActionType: fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterVolumeAttachmentDeleted},
	}, nil
}
```

逐个看 hint 的判断：

- **CSINode 新增**：无条件 `Queue`。
- **CSINode 更新**：把新旧对象里每个驱动的 `allocatable.count` 做成 map 比较，任何驱动的上限**变大**才 `Queue`。不存在或没有 count 的按 0 算。配合 `MutableCSINodeAllocatableCount`，kubelet 周期刷新上限时只有增加才会唤醒 Pod。
- **已调度 Pod 删除**：被删 Pod 没有卷，或者既没有 `nodeName` 也没有 `nominatedNodeName`，返回 `QueueSkip`；只要它有 PVC、ephemeral 卷或可迁移内联卷之一就 `Queue`。不检查驱动是否相同。
- **PVC 新增**：对应新 Pod 的 PVC 找不到的情况，名字和命名空间都匹配才 `Queue`。
- **VA 删除**：Pod 只要有任何一个 PVC 卷就 `Queue`；对可迁移内联卷，翻译后比较驱动名与 VA 的 `attacher`。

CSINode 更新的 hint 有一个缝隙。在 `VolumeLimitScaling` 下被「驱动未安装」拒绝的 Pod，等的是驱动注册到 CSINode。kubelet 启动时就会创建 CSINode（此时可能没有任何驱动），驱动注册是一次 Update。如果这个驱动不上报 `max_volumes_per_node`，新旧 count 都按 0 算，hint 返回 `QueueSkip`，Pod 不会被立刻唤醒，只能等不可调度队列的周期刷新（`DefaultPodMaxInUnschedulablePodsDuration`，5 分钟）或其它事件。驱动上报了上限则 0 → N，正常唤醒。

还要注意 Pod 删除和 VA 删除这两个事件的时间差。Pod 被删时卷往往还没 detach，VA 还在，这时被唤醒的 Pod 重新跑 Filter 还是会失败（VA 一路照样计数），真正腾出槽位的是之后的 VA Delete 事件。两个事件都注册，是为了覆盖「没有 VA 的驱动」和「有 VA 的驱动」两种情况。

## 七、边界与坑

**抢占几乎帮不上忙。** 插件没有 `PreFilterExtensions`，抢占模拟依赖 Filter 从 `nodeInfo.GetPods()` 现算：移除受害者后，它的卷从 `attachedVolumes` 里消失。但 VA 那一路查的是 informer 里的真实 VA，模拟不会动它。受害者的卷只要有 VA，就会在第 3 步以「不在 `attachedVolumes` 中」的身份重新计入，计数纹丝不动。所以对需要 attach 的驱动（有上限的块存储基本都是），因卷数量超限而失败的 Pod 走抢占通常找不到候选。这从语义上也说得通：Pod 删除后卷还要经过 unmount、detach 才真正释放槽位，调度器不该假设驱逐立即见效。

**新 Pod 自己的卷已经 attach 在节点上时会多算一次。** 第 2 步只用已有 Pod 的卷去重 `newVolumes`，不用 VA 去重。如果新 Pod 要用的卷此刻在节点上有 VA、但没有任何 Pod 在用（比如同一个 StatefulSet Pod 删除后立即在原节点重建，旧卷还没 detach），这块盘会在 VA 一路和新 Pod 一路各算一次。上限卡得很紧的节点上，这会造成短暂的误拒，等 VA 删除后自然恢复。

**只按驱动维度比较。** `newVolumeCount` 里没有出现在 `nodeVolumeLimits` 中的驱动（没有上报上限）不做检查。一个 Pod 同时用两个驱动的卷，任意一个驱动超限就失败。

**Filter 的开销与节点上的 Pod 数成正比。** 每个节点都要遍历全部已有 Pod 的全部卷，逐个查 PVC、PV，必要时做 in-tree 翻译，再查一遍 VA。没有 PreFilter 缓存，抢占模拟时每个候选节点还要再算一遍。它选择不缓存，是因为结果依赖节点上的 Pod 集合，缓存就必须实现 `AddPod`/`RemovePod` 并处理 VA，复杂度不划算。VA 按节点建索引是为了把最重的那部分降下来。

## 八、发散：调度器、kubelet、attach/detach 控制器的三方对账

卷数量限制牵涉三个组件，各自看到的「已挂载卷」并不一样：

- **kubelet** 通过 `NodeGetInfo` 知道驱动的硬上限，写进 CSINode；
- **attach/detach 控制器**（kube-controller-manager 里）为每个需要 attach 的卷创建 VolumeAttachment，external-attacher 看到 VA 后调用驱动的 `ControllerPublishVolume`；
- **调度器**在 Pod 落到节点之前，用「Pod 上的卷 ∪ VA」估算已用量。

VA 计数是后来补上的。早期只数 Pod，Pod 删除而卷未 detach 的窗口里调度器会少算，新 Pod 被放上去之后 attach 失败，报的是驱动侧的 `ResourceExhausted` 或云厂商 API 的错误，Pod 卡在 `ContainerCreating`，而且不会被重新调度，因为调度已经结束了。补上 VA 之后，调度器的视图更接近「实例上真实被占用的槽位」。

即便如此，三方之间仍然可能对不上：云厂商还会把系统盘、网卡（某些实例类型的网卡和 EBS 共享 PCIe 槽位）算进同一个上限，而驱动上报的 `max_volumes_per_node` 只是一个静态估计。`MutableCSINodeAllocatableCount` 的周期刷新和 `ResourceExhausted` 触发刷新，就是为了让 kubelet 能把真实可用数修正回 CSINode。生产上建议：

- 块存储 CSI 驱动开启 `nodeAllocatableUpdatePeriodSeconds`，让上限随实例实际情况调整；
- 依赖 cluster-autoscaler 的集群，确认 autoscaler 版本能模拟 CSINode，再开启驱动的 `preventPodSchedulingIfMissing`；
- 排查 `node(s) exceed max volume count` 时，先 `kubectl get volumeattachment` 看节点上是否残留了 detach 失败的 VA，这比看 Pod 列表更接近调度器的视角。

## 九、小结

- `NodeVolumeLimits`（结构体 `CSILimits`）是 in-tree 卷数量插件合并后的唯一幸存者，只按 CSI 驱动计数，上限来自 `CSINode.spec.drivers[].allocatable.count`。
- PVC、generic ephemeral 卷、可迁移内联卷三种来源都换算成 `驱动名/volumeHandle`，与节点上已有 Pod 的卷、VolumeAttachment 合并去重；未绑定 PVC 用「随机前缀 + 命名空间/名字」生成假句柄。
- 超限返回 `Unschedulable`，新 Pod 的 PVC 不存在返回 `UnschedulableAndUnresolvable`；`VolumeLimitScaling` 下驱动未安装且 CSIDriver 声明了 `preventPodSchedulingIfMissing` 时返回 `Unschedulable`。
- VA 一路让调度器看见「Pod 已删、卷未 detach」的槽位占用，代价是抢占模拟基本无法通过驱逐释放槽位。
- QueueingHint 只在 CSINode 上限变大、相关 PVC 创建、带卷 Pod 删除、VA 删除时唤醒；驱动注册但未上报上限这一情况会落到 5 分钟的周期刷新。

数量过关之后，还要回答「这些卷到底绑哪块 PV、在哪供给」。下一篇 [VolumeBinding](/posts/sched-plugin-11-volume-binding/) 是存储插件里最复杂的一个，横跨调度周期和绑定周期。
