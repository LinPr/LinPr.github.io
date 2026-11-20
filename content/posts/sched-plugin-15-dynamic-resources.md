---
title: 'scheduler-plugins 15 [in-tree] DynamicResources：把设备分配搬进调度周期'
date: 2026-11-19T10:30:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "DRA", "ResourceClaim", "设备分配"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "DynamicResources 是 DRA 在调度器里的实现：它在 Filter 里为每个节点跑一遍设备分配器，在 Reserve 把结果挂进 in-flight 表，在 PreBind 把 allocation 和 reservedFor 写回 ResourceClaim。本文按扩展点拆解它的状态、分配器的回溯搜索、assume cache 与 in-flight 表的分工、各个 DRA feature gate，以及它为什么几乎从不触发抢占。"
---

[上一篇](/posts/sched-plugin-14-inter-pod-affinity/)的 `InterPodAffinity` 处理的是「Pod 和 Pod 之间」的约束。默认 Profile 里紧接着它的是 `DynamicResources`，它处理的是 Pod 和**设备**之间的约束：GPU、网卡 VF、FPGA 这类无法用一个整数 `nvidia.com/gpu: 1` 描述清楚的资源。本文基于 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期）。

在 `pkg/scheduler/apis/config/v1/default_plugins.go` 里，它的权重是 2，位置被刻意放在 `DefaultPreemption` 之前：

```go
// pkg/scheduler/apis/config/v1/default_plugins.go
				{Name: names.InterPodAffinity, Weight: ptr.To[int32](2)},
				// DynamicResources should come before DefaultPreemption because if
				// there is a problem with a Pod and PostFilter gets called to
				// resolve the problem, it is better to first deallocate an
				// idle ResourceClaim than it is to evict some Pod that might
				// be doing useful work.
				{Name: names.DynamicResources, Weight: ptr.To[int32](2)},
				{Name: names.DefaultPreemption},
```

在这个提交里，插件本身已经不再受 `DynamicResourceAllocation` gate 控制（这个 gate 已从 `kube_features.go` 中移除），默认 Profile 一定会加载它。它实现的扩展点是整个 in-tree 插件里最多的一个：

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
var _ fwk.PreEnqueuePlugin = &DynamicResources{}
var _ fwk.PreFilterPlugin = &DynamicResources{}
var _ fwk.FilterPlugin = &DynamicResources{}
var _ fwk.PostFilterPlugin = &DynamicResources{}
var _ fwk.PodGroupPostFilterPlugin = &DynamicResources{}
var _ fwk.ScorePlugin = &DynamicResources{}
var _ fwk.ReservePlugin = &DynamicResources{}
var _ fwk.EnqueueExtensions = &DynamicResources{}
var _ fwk.PreBindPlugin = &DynamicResources{}
var _ fwk.SignPlugin = &DynamicResources{}
```

PreEnqueue、PreFilter、Filter、PostFilter、Score、Reserve/Unreserve、PreBind，一个 Pod 从入队到绑定，它几乎全程在场。原因是 DRA 把「设备分配」这件事从 kubelet/驱动侧挪到了调度器里：调度器在选节点的同时就决定用哪几块设备，并把决定写进 API 对象。

## 一、DRA 的对象模型：调度器在读什么、写什么

### 1.1 四类对象

{{< figure src="/images/posts/sched-plugins/15-dra-objects.svg" caption="DRA 的对象关系：Pod 引用 ResourceClaim，claim 的请求按名字指向 DeviceClass，驱动通过 ResourceSlice 发布设备；调度器只写 claim 的 status（点击查看大图）" link="/images/posts/sched-plugins/15-dra-objects.svg" >}}

- **ResourceSlice**：驱动发布的设备清单。一个 slice 属于某个 driver 的某个 pool，带 `nodeName`、`nodeSelector` 或 `allNodes` 之一，说明这些设备从哪些节点可达。每个设备有 `attributes`（如型号、PCIe root）和 `capacity`（如显存）。
- **DeviceClass**：管理员定义的设备类别，用 CEL `selectors` 圈出一类设备，附带下发给驱动的 `config`。
- **ResourceClaim**：工作负载的申请单。`spec.devices.requests[]` 里每个请求要么是 `exactly`（指定 class、selectors、数量），要么是 `firstAvailable`（一组有序的备选子请求）。`constraints` 可以要求多个设备在某个属性上取值相同。
- **ResourceClaimTemplate**：每个 Pod 一份 claim 时用的模板，由 kube-controller-manager 里的 resourceclaim controller 实例化，并把生成的 claim 名字回填到 `pod.status.resourceClaimStatuses`。

一个典型的申请如下，Pod 想要一块 80Gi 以上显存的 GPU，拿不到就退而求其次要两块普通 GPU：

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaimTemplate
metadata:
  name: gpu-tpl
spec:
  spec:
    devices:
      requests:
      - name: gpu
        firstAvailable:
        - name: big
          deviceClassName: gpu.example.com
          selectors:
          - cel:
              expression: device.capacity["gpu.example.com"].memory.compareTo(quantity("80Gi")) >= 0
        - name: small
          deviceClassName: gpu.example.com
          count: 2
---
apiVersion: v1
kind: Pod
metadata:
  name: train-0
spec:
  resourceClaims:
  - name: gpu
    resourceClaimTemplateName: gpu-tpl
  containers:
  - name: train
    image: trainer:latest
    resources:
      claims:
      - name: gpu
```

### 1.2 调度器的写边界

调度器对 claim 只做三件事：加 finalizer `resource.kubernetes.io/delete-protection`，写 `status.allocation`（选中的设备列表和 `nodeSelector`），往 `status.reservedFor` 里追加当前 Pod。反向操作分散在几个地方：Unreserve 和 PostFilter 由调度器自己做；Pod 结束后从 `reservedFor` 移除 Pod、在最后一个使用者离开时清空 `allocation`，由 resourceclaim controller 做：

```go
// pkg/controller/resourceclaim/controller.go
		if len(remaining) == 0 {
			// This is a sanity check. There shouldn't be any claims without this
			// finalizer because there's no longer any other way of allocating claims.
			// Classic DRA was the alternative earlier.
			if builtinControllerFinalizer >= 0 {
				// Allocated by scheduler with structured parameters. We can "deallocate"
				// by clearing the allocation.
				claim.Status.Allocation = nil
			}
		}
```

这一点对理解后面的抢占至关重要：**设备的占用记录在 claim 上，而不是 Pod 上**。删除一个 Pod 并不会立即让它的设备变成空闲，要等 controller 把 claim 的 allocation 清掉。

## 二、插件结构与参数

### 2.1 结构体与 New

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
type DynamicResources struct {
	fts                   feature.Features
	filterTimeout         time.Duration
	bindingTimeout        time.Duration
	fh                    fwk.Handle
	clientset             kubernetes.Interface
	celCache              *cel.Cache
	draManager            fwk.SharedDRAManager
	podIndexer            cache.Indexer
	podResourceClaimIndex string
}
```

几个字段值得单独说：

- `celCache` 是一个容量为 10 的 LRU，缓存编译好的 CEL 表达式。DeviceClass 和 claim 里的 selector 在不同调度周期里高度重复，编译一次复用多次。
- `draManager` 来自 `fh.SharedDRAManager()`，是所有 Profile 共享的 `DefaultDRAManager`，里面装着 claim 的 assume cache、in-flight 表、ResourceSlice tracker 和 DeviceClass lister。插件自己不持有任何跨周期状态，跨周期的东西都在这里。
- `podIndexer` 是 `New` 里给 Pod informer 加的一个索引：按「Pod 引用了哪些 claim」建索引，索引名带 Profile 名后缀，避免多个 Profile 重复注册冲突。它服务于后面要讲的 PreQueueingHint。

### 2.2 DynamicResourcesArgs

两个参数，都由 feature gate 决定是否生效：

```go
// pkg/scheduler/apis/config/v1/defaults.go
func SetDefaults_DynamicResourcesArgs(obj *configv1.DynamicResourcesArgs) {
	if obj.FilterTimeout == nil && feature.DefaultFeatureGate.Enabled(features.DRASchedulerFilterTimeout) {
		obj.FilterTimeout = &metav1.Duration{Duration: configv1.DynamicResourcesFilterTimeoutDefault}
	}

	if obj.BindingTimeout == nil &&
		feature.DefaultFeatureGate.Enabled(features.DRADeviceBindingConditions) &&
		feature.DefaultFeatureGate.Enabled(features.DRAResourceClaimDeviceStatus) {
		obj.BindingTimeout = &metav1.Duration{Duration: configv1.DynamicResourcesBindingTimeoutDefault}
	}
}
```

| 参数 | 默认值 | 生效条件 | 校验 |
| --- | --- | --- | --- |
| `filterTimeout` | 10s | `DRASchedulerFilterTimeout`（Beta，默认开） | 不能为负；设为 0 表示不限时；gate 关闭时设置它会被拒绝 |
| `bindingTimeout` | 600s | `DRADeviceBindingConditions` 与 `DRAResourceClaimDeviceStatus` 同时开启 | 至少 1 秒；gate 关闭时设置它会被拒绝 |

`filterTimeout` 是**每个节点**一次 Filter 的上限，不是整个调度周期的上限。分配器本质是回溯搜索，最坏情况是指数级的，这个超时是兜底。

## 三、CycleState：一个指针、不拷贝

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
type stateData struct {
	claims claimStore

	draExtendedResource draExtendedResource

	// Allocator handles claims with structured parameters, which is all of them nowadays.
	allocator structured.Allocator

	// mutex must be locked while accessing any of the fields below.
	mutex sync.Mutex

	unavailableClaims sets.Set[int]

	// informationsForClaim has one entry for each claim in claims.
	informationsForClaim []informationForClaim

	// nodeAllocations caches the result of Filter for the nodes, its key is node name.
	nodeAllocations map[string]nodeAllocation

	claimHasNodeAllocatableMappedDevice map[types.UID]bool
}

func (d *stateData) Clone() fwk.StateData {
	return d
}
```

`Clone` 直接返回自己。其他插件（比如 `PodTopologySpread`）的 `Clone` 会深拷贝，是因为抢占模拟时要在副本上做 AddPod/RemovePod。`DynamicResources` 没有实现 `PreFilterExtensions`（返回 `nil`），抢占模拟不会修改它的状态，所以共享指针是安全的，还省掉了 `nodeAllocations` 这个可能很大的 map 的拷贝。

几个字段的分工：

- `claims` 是一个 `claimStore`，按 `pod.Spec.ResourceClaims` 的顺序存放 claim 指针，如果 Pod 有「由 DRA 支撑的扩展资源」，末尾再追加一个特殊 claim（第七节讲）。它提供 `all()`、`allUserClaims()`、`toAllocate()` 三个迭代器，`toAllocate()` 跳过已经有 `Status.Allocation` 的 claim，但**返回原始下标**，这样 `informationsForClaim[index]` 始终能对上号。
- `informationsForClaim[i]` 有两个字段：`availableOnNodes` 是已分配 claim 的 `nodeSelector` 编译结果；`allocation` 是 Reserve 选定的分配结果。
- `nodeAllocations` 是 Filter 的产物：每个通过 Filter 的节点一份分配结果。Score 和 Reserve 都从这里取，不重新计算。
- `unavailableClaims` 记录「已分配、但在某些节点上不可用」的 claim 下标，供 PostFilter 挑选释放对象。
- `mutex` 保护后三个字段，因为 Filter 是按节点并行执行的。

## 四、PreEnqueue：claim 还不存在就别排队

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
func (pl *DynamicResources) PreEnqueue(ctx context.Context, pod *v1.Pod) (status *fwk.Status) {
	if err := pl.foreachPodResourceClaim(pod, nil); err != nil {
		return statusUnschedulable(klog.FromContext(ctx), err.Error())
	}
	return nil
}
```

`foreachPodResourceClaim` 对每个 `pod.Spec.ResourceClaims` 做三件事：用 `resourceclaim.Name` 解析出 claim 名（模板生成的 claim 要从 `pod.status.resourceClaimStatuses` 里找）；从 assume cache 取 claim；检查 claim 没在删除中，并且模板生成的 claim 的 owner 确实是这个 Pod。

用模板创建 Pod 时，Pod 先出现，controller 才去建 claim 并回填 status。这中间 Pod 被 PreEnqueue 挡在 activeQ 之外，留在不可调度池里。等 controller 回填了 `resourceClaimStatuses`，`UpdatePodGeneratedResourceClaim` 事件触发 `isSchedulableAfterTargetPodUpdate`，后者再跑一遍 `foreachPodResourceClaim`，全部能找到才返回 `Queue`。

还有一个容易漏掉的出口：`resourceclaim.Name` 遇到模板生成的 claim、而 status 里有条目但 `ResourceClaimName` 为 nil 时返回 `nil`，`foreachPodResourceClaim` 会直接跳过。这是 controller 判断「不需要生成 claim」时的约定，此时 Pod 不需要这个 claim 也能调度。

## 五、PreFilter：收集 claim、构建分配器

PreFilter 是这个插件最重的一步，可以分成五段。

### 5.1 初始化状态，没有 claim 就 Skip

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
	s := &stateData{
		nodeAllocations: make(map[string]nodeAllocation),
	}
	state.Write(stateKey, s)
	// ...
	userClaims, err := pl.podResourceClaims(pod)
	if err != nil {
		return nil, statusUnschedulable(logger, err.Error())
	}
	// ...
	extendedResourceClaim, status := pl.preFilterExtendedResources(pod, logger, s)
	if status != nil {
		return nil, status
	}
	claimBindings, err := pl.podResourceClaimBindings(pod)
	if err != nil {
		return nil, statusUnschedulable(logger, err.Error())
	}
	claims := newClaimStore(userClaims, extendedResourceClaim, claimBindings)

	// This check covers user and extended ResourceClaim.
	if claims.empty() {
		return nil, fwk.NewStatus(fwk.Skip)
	}
```

注意 `state.Write` 发生在 `Skip` 之前：即使 PreFilter 返回 `Skip`、框架跳过了它的 Filter，Reserve、Unreserve、PostFilter 仍然会被调用（PreBind 则由 `PreBindPreFlight` 返回 `Skip` 跳过），它们读到的是一个空 `claims`，靠 `state.claims.empty()` 快速返回。

### 5.2 逐个检查 claim

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
	for index, claim := range claims.all() {
		if claim.Status.Allocation != nil &&
			!resourceclaim.CanBeReserved(claim) &&
			!resourceclaim.IsReservedForPod(pod, claim, pl.fts.EnableDRAWorkloadResourceClaims) {
			// Resource is in use. The pod has to wait.
			return nil, statusUnschedulable(logger, "resourceclaim in use", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
		}

		if claim.Status.Allocation != nil {
			nodeSelector, err := nodeSelectorFromAllocation(claim.Status.Allocation)
			// ...
			s.informationsForClaim[index].availableOnNodes = nodeSelector
		} else {
			numClaimsToAllocate++
			// ...
			// Allocation in flight? Better wait for that
			// to finish, see inFlightAllocations
			// documentation for details.
			if pl.draManager.ResourceClaims().GetPendingAllocation(claim.UID) != nil {
				return nil, statusUnschedulable(logger, fmt.Sprintf("resource claim %s is in the process of being allocated", klog.KObj(claim)))
			}
			// ...
			for _, request := range claim.Spec.Devices.Requests {
				switch {
				case request.Exactly != nil:
					if status := pl.validateDeviceClass(logger, request.Exactly.DeviceClassName, request.Name); status != nil {
						return nil, status
					}
				case len(request.FirstAvailable) > 0:
					if !pl.fts.EnableDRAPrioritizedList {
						return nil, statusUnschedulable(logger, fmt.Sprintf("resource claim %s, request %s: has subrequests, but the DRAPrioritizedList feature is disabled", klog.KObj(claim), request.Name))
					}
					// ...
				default:
					return nil, statusUnschedulable(logger, fmt.Sprintf("resource claim %s, request %s: unknown request type", klog.KObj(claim), request.Name))
				}
			}
		}
	}
```

分两种情况：

**已分配的 claim**：只把它的 `allocation.nodeSelector` 编译出来，Filter 时检查节点是否匹配。第一个条件里的 `CanBeReserved` 目前恒为 `true`（源码里写着 `// Currently no restrictions on sharing...`），所以「resourceclaim in use」这个分支在当前代码里走不到。一个 claim 可以被任意多个 Pod 共享，上限由 API 校验的 `ResourceClaimReservedForMaxSize = 256` 兜底，超过时会在 PreBind 的 `UpdateStatus` 上失败。

**未分配的 claim**：先看 in-flight 表里有没有它。如果另一个 Pod 已经在 Reserve 里为它选好了设备、但 PreBind 还没写回 API，当前 Pod 直接 `UnschedulableAndUnresolvable`，等 claim 的 status 更新事件再来。这是为了避免两个 Pod 同时以为自己是分配者（第八节详细讲）。然后检查每个请求引用的 DeviceClass 是否存在，不存在返回 `device class %s does not exist`，等 DeviceClass 的 Add 事件。

### 5.3 PodGroup 的例外：复用 pending allocation

上面跳过的 `// ...` 里有一段处理 PodGroup 的逻辑。`GenericWorkload` 开启后，一个 PodGroup 里的多个 Pod 在同一个 PodGroup 调度周期里依次走 Reserve，全部成功后才一起绑定。如果这些 Pod 共享一个 claim，第一个 Pod 的 Reserve 选好了设备，第二个 Pod 走到 PreFilter 时发现 claim 在 in-flight 里——按普通逻辑它会被拒绝，整个组永远凑不齐。

所以这里多了一个判断：如果当前周期的 `podGroupStateData.pendingAllocations` 里有这个 claim，就把 in-flight 的分配结果当成「已分配」处理，复用它的 `nodeSelector`。源码注释明确说，这种共享目前**只限于同一个 PodGroup 调度周期**，不跨周期、不跨组。

### 5.4 汇总「已分配设备集合」

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
		var allocatedState *structured.AllocatedState
		err = wait.PollUntilContextTimeout(ctx, time.Microsecond, 5*time.Second, true /* immediate */, func(context.Context) (bool, error) {
			if pl.fts.EnableDRAConsumableCapacity {
				allocatedState, err = pl.draManager.ResourceClaims().GatherAllocatedState()
				if err != nil {
					if errors.Is(err, errClaimTrackerConcurrentModification) {
						logger.V(6).Info("Conflicting modification during GatherAllocatedState, trying again")
						return false, nil
					}
					return false, err
				}
				// ...
				return true, nil
			} else {
				allocatedDevices, err := pl.draManager.ResourceClaims().ListAllAllocatedDevices()
				// ...
			}
		})
```

分配器需要知道「哪些设备已经被别人占了」。这个集合有两个来源：

1. `allocatedDevices`：一个挂在 claim assume cache 上的事件处理器，随 claim 的增删改增量维护一个 `sets.Set[DeviceID]`。源码注释说这比每次 List 所有 claim 再拼字符串便宜得多。
2. in-flight 表：已经 Reserve、还没 PreBind 的分配。

两者不在同一把锁下，存在一个窗口：先读 `allocatedDevices` 时某个 claim 还不在，接着它被 PreBind 写回、从 in-flight 移除并进入 cache，再去读 in-flight 时它也不在了——这个 claim 的设备就被漏掉。`allocatedDevices` 每次修改都会递增一个 `revision`，读完 in-flight 之后再比较一次 revision，变了就返回 `errClaimTrackerConcurrentModification`，外层以 1 微秒间隔轮询重试，最多 5 秒。

`DRAConsumableCapacity`（1.36 起 Beta，默认开）开启时走 `GatherAllocatedState`，额外收集「共享设备」和「已消耗容量」。这是为了支持一块设备按容量切给多个 claim 的场景。

### 5.5 构建分配器

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
		slices, err := pl.draManager.ResourceSlices().ListWithDeviceTaintRules()
		if err != nil {
			return nil, statusError(logger, err)
		}
		features := AllocatorFeatures(pl.fts)
		allocator, err := structured.NewAllocator(ctx, features, *allocatedState, pl.draManager.DeviceClasses(), slices, pl.celCache)
		if err != nil {
			return nil, statusError(logger, err)
		}
		s.allocator = allocator
```

只有 `numClaimsToAllocate > 0` 时才构建分配器；所有 claim 都已分配时 `s.allocator` 为 nil，Filter 只做 nodeSelector 匹配。`ListWithDeviceTaintRules` 返回的是已经叠加了 `DeviceTaintRule` 的 slice 视图。

源码注释里有一段解释了为什么每个 Pod 都重建一次分配器，而不是长期维护一个：长期维护的分配器要和 informer 事件精细同步，Cluster Autoscaler 复用调度器插件做模拟时就很难接入。目前的选择是「每周期从快照重建」，用 `allocatedDevices` 的增量集合把重建成本压下来。

## 六、Filter：每个节点跑一次分配器

### 6.1 已分配 claim 的检查

Filter 先遍历所有 claim，对已分配的 claim 做三项检查，任何一项不通过都把下标记入 `unavailableClaims`：

1. `availableOnNodes.Match(node)`：分配结果的 `nodeSelector` 是否覆盖当前节点。
2. 分配结果里有设备设置了 `SkipNodeOperations`，而节点的 `status.declaredFeatures` 里没有 `DRAOptionalNodeOperations`。
3. 开启 binding conditions 时，claim 已经绑定失败（`BindingFailureConditions` 为真），或者等待超过 `bindingTimeout` 仍未就绪。

最后统一返回：

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
	if len(unavailableClaims) > 0 {
		// ...
		for _, index := range unavailableClaims {
			state.unavailableClaims.Insert(index)
		}

		return statusUnschedulable(logger, "resourceclaim not available on the node", "pod", klog.KObj(pod))
	}
```

### 6.2 未分配 claim：调用 Allocate

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
		if pl.fts.EnableDRASchedulerFilterTimeout && pl.filterTimeout > 0 {
			c, cancel := context.WithTimeout(allocCtx, pl.filterTimeout)
			defer cancel()
			allocCtx = c
		}
		// ...
		allocationResult, err := state.allocator.Allocate(allocCtx, node, claimsToAllocate)
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			// Timeouts are potentially transient. Return Error
			// so the pod retries via backoff instead of sitting in the
			// unschedulable queue waiting for a cluster event.
			// ...
			return statusError(logger, fmt.Errorf("node %s: timed out trying to allocate devices", node.Name), "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaims", klog.KObjSlice(claimsToAllocate))
		case errors.Is(err, structured.ErrFailedAllocationOnNode):
			// Not a fatal error, allocation on other nodes may proceed.
			// The error is only surfaced if allocation fails on all nodes.
			return statusUnschedulable(logger, err.Error(), "pod", klog.KObj(pod), "node", klog.KObj(node))
		case ctx.Err() != nil:
			return statusUnschedulable(logger, fmt.Sprintf("asked by caller to stop allocating devices: %v", context.Cause(ctx)), "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaims", klog.KObjSlice(claimsToAllocate))
		case err != nil:
			// This should only fail if there is something wrong with the claim or class.
			// Return an error to abort scheduling of it.
			// ...
			return statusError(logger, err, "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaims", klog.KObjSlice(claimsToAllocate))
		}
		// Check for exact length just to be sure. In practice this is all-or-nothing.
		if len(allocationResult) != len(claimsToAllocate) {
			return statusUnschedulable(logger, "cannot allocate all claims", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaims", klog.KObjSlice(claimsToAllocate))
		}
```

四种失败方式，三种状态：

| 情况 | 返回 | 后果 |
| --- | --- | --- |
| 超时 `DeadlineExceeded` | `Error` | 整个调度周期中止，Pod 进 backoffQ |
| `ErrFailedAllocationOnNode`（如 pool 无效、要「全部设备」但 pool 正在更新） | `UnschedulableAndUnresolvable` | 只影响这个节点 |
| 调用方取消（比如已经找够了可行节点） | `UnschedulableAndUnresolvable` | 只影响这个节点 |
| 其它错误（CEL 表达式出错等） | `Error` | 整个调度周期中止 |
| 分配器返回 nil（放不下） | `UnschedulableAndUnresolvable`，`cannot allocate all claims` | 只影响这个节点 |

`Error` 会让整个调度周期失败，不管其他节点是否可行。超时选择 `Error` 是有意的：超时可能是暂时的，放进不可调度池等事件不如直接 backoff 重试。这和 `DynamicResourcesArgs.FilterTimeout` 字段注释的描述不一致——注释说超时后「the Pod is considered unschedulable for the node. If filtering succeeds for some other node(s), those are picked instead」，而当前代码是中止整个周期。排障时以代码为准。

成功时，结果连同 pending allocation 一起存进 `state.nodeAllocations[node.Name]`，这一步要加 `state.mutex`。

### 6.3 分配器内部：回溯搜索

`structured.NewAllocator` 不是一个实现，而是三个：

```go
// staging/src/k8s.io/dynamic-resource-allocation/structured/allocator.go
	for _, allocator := range availableAllocators {
		// Disabled?
		if !allocatorEnabled(allocator.name) {
			continue
		}
		enabledAllocators = append(enabledAllocators, allocator.name)

		// All required features supported?
		if allocator.supportedFeatures.Set().IsSuperset(features.Set()) {
			// Use it!
			return allocator.newAllocator(ctx, features, allocatedState, classLister, slices, celCache)
		}
	}
```

`stable`、`incubating`、`experimental` 三个包按顺序尝试，选第一个支持当前全部已启用特性的。各包代码互相复制、不共享，目的是让新特性的代码不影响稳定路径。`stable` 只支持 AdminAccess、PrioritizedList、PartitionableDevices、DeviceTaints 四项；按本提交的默认 gate，`DRADeviceBindingConditions`、`DRAConsumableCapacity` 都是开的，所以默认集群实际跑的是 `incubating`。

三个实现的骨架相同：先 `GatherPools` 收集当前节点可达的 pool，然后对「claim → request → 第 N 个设备」做深度优先搜索：

{{< figure src="/images/posts/sched-plugins/15-dra-allocator.svg" caption="一个带 matchAttribute 约束的 claim 在节点上的搜索过程：gpu0 分支走不通后回溯，gpu1 分支找到 {gpu1, gpu2} 后立即返回（点击查看大图）" link="/images/posts/sched-plugins/15-dra-allocator.svg" >}}

```go
// staging/src/k8s.io/dynamic-resource-allocation/structured/internal/stable/allocator_stable.go
	// We allocate one claim after the other and for each claim, all of
	// its requests. For each individual device we pick one possible
	// candidate after the other, checking constraints as we go.
	// Each chosen candidate is marked as "in use" and the process
	// continues, recursively. This way, all requests get matched against
	// all candidates in all possible orders.
	//
	// The first full solution is chosen.
	//
	// In other words, this is an exhaustive search. This is okay because
	// it aborts early. Once scoring gets added, more intelligence may be
	// needed to avoid trying "equivalent" solutions (two identical
	// requests, two identical devices, two solutions that are the same in
	// practice).
```

`allocateOne` 每次处理一个「设备槽位」。对于普通请求，它按 pool → slice → device 的顺序遍历：

```go
// staging/src/k8s.io/dynamic-resource-allocation/structured/internal/stable/allocator_stable.go
				allocated, deallocate, err := alloc.allocateDevice(r, device, false)
				if err != nil {
					return false, err
				}
				if !allocated {
					// In use or constraint violated...
					alloc.logger.V(7).Info("Device not usable", "device", deviceID)
					continue
				}
				deviceKey := deviceIndices{
					claimIndex:      r.claimIndex,
					requestIndex:    r.requestIndex,
					subRequestIndex: r.subRequestIndex,
					deviceIndex:     r.deviceIndex + 1,
				}
				done, err := alloc.allocateOne(deviceKey, allocateSubRequest)
				// If we found a solution, we can stop.
				if err == nil && done {
					return done, nil
				}

				// Otherwise we didn't find a solution, and we need to deallocate
				// so the temporary allocation is correct for trying other devices.
				deallocate()
```

`allocateDevice` 依次检查：设备是否已被占用（admin access 请求例外）、counter 是否够（可分区设备）、污点是否被容忍、约束（如 `matchAttribute`）能否加入。通过就把设备临时标记为占用，返回一个 `deallocate` 闭包，下一层失败时用它回滚。

`incubating` 在此基础上加了一个剪枝：同一个请求内部，设备的**顺序**不重要，`{gpu1, gpu2}` 和 `{gpu2, gpu1}` 是同一个解。所以它给 `allocateOne` 加了 `startLocation` 参数，同一请求的下一个设备只从当前设备之后找，搜索空间从排列降到组合。源码注释举的例子是设备 `[1, 2, 3]` 只会尝试 `[1] [2] [3] [1,2] [1,3] [2,3] [1,2,3]`。

`firstAvailable` 子请求按列表顺序逐个尝试，第一个能走通的就被采用，并记进 `selectedSubRequestIndex`。这里有一个专门的错误 `errAllocationResultMaxSizeExceeded`：一个 claim 最多 `AllocationResultsMaxSize = 32` 个设备，如果某个子请求因为设备数超限失败，下一个子请求可能要的设备更少，所以继续试；只有所有子请求都因为超限失败时，才把这个错误往上传，让上层做更激进的回溯。

整个搜索**不比较多个解**。拿到第一个完整解就返回，设备选择完全由 ResourceSlice 的发布顺序决定。这也是 Score 阶段只能看「选中了第几个备选」、而不能看「设备有多好」的原因。

## 七、扩展资源与节点可分配资源：两条附加路径

### 7.1 DRAExtendedResource：让 `nvidia.com/gpu: 1` 也走 DRA

`DRAExtendedResource` 在 1.37 已 GA。它让 DeviceClass 声明一个 `extendedResourceName`，Pod 用传统的 `resources.limits` 写法申请，调度器在没有 device plugin 提供该资源的节点上，用 DRA 满足它。

实现方式是一个只存在于内存里的特殊 claim：

```go
// pkg/scheduler/framework/plugins/dynamicresources/extendeddynamicresources.go
	// It's intentionally not a valid ResourceClaim name to avoid conflicts with
	// some actual ResourceClaim in the apiserver.
	specialClaimInMemName = "<extended-resources>"
```

PreFilter 构造一个名为 `<extended-resources>`、带临时 UID、`Spec` 为空的 claim，追加到 `claimStore` 末尾。Filter 里 `filterExtendedResources` 对每个节点判断：节点 `allocatable` 里有这个资源（device plugin 提供）就不走 DRA；没有、但能映射到 DeviceClass，就给这个节点生成一个带具体 `requests` 的 claim 副本交给分配器。PreBind 时才真正在 API Server 里 `Create` 这个 claim（名字由 `GenerateName` 生成），再更新 Pod 的 `status.extendedResourceClaimStatus`，记录容器到请求的映射。

### 7.2 DRANodeAllocatableResources：设备映射到 CPU/内存

`DRANodeAllocatableResources`（1.36 Alpha，默认关）处理另一种设备：它们本身就代表节点的 CPU、内存等可分配资源（`Device.NodeAllocatableResources` 带 `Mapping`）。这种设备分配出去后，`NodeResourcesFit` 那边的账也要跟着变。Filter 里 `calculateAndCheckNodeAllocatableResources` 计算 Pod 通过 DRA 拿到的节点资源足迹并检查是否放得下，结果存进 `nodeAllocation.nodeAllocatableResourceClaimStatuses`；调度器在 assume Pod 时把它拷进 `pod.Status.NodeAllocatableResourceClaimStatuses`，让缓存里的 NodeInfo 记账正确；PreBind 再 patch 到 API。

## 八、Reserve、PreBind、Unreserve：一次分配的落点

这三个扩展点合起来，解决的是一个经典并发问题：调度周期是串行的，绑定周期是并行的，两者之间有时间差。Pod A 在 Reserve 选定了设备，在它的 PreBind 把结果写回 API 之前，Pod B 的调度周期已经开始了。B 必须知道那几块设备「已经名花有主」。

{{< figure src="/images/posts/sched-plugins/15-dra-claim-lifecycle.svg" caption="一次分配在 CycleState、in-flight 表、assume cache、API Server 四处的读写时机（点击查看大图）" link="/images/posts/sched-plugins/15-dra-claim-lifecycle.svg" >}}

### 8.1 Reserve：挂进 in-flight 表

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
		allocIndex := 0
		for index, claim := range state.claims.toAllocate() {
			// ...
			allocation := &allocations.allocationResults[allocIndex]
			state.informationsForClaim[index].allocation = allocation

			if claim == extendedResourceClaim {
				// replace the special claim template for extended
				// resource backed by DRA with the real instantiated claim.
				claim = allocations.extendedResourceClaim
			}
			// ...
			claim = claim.DeepCopy()
			if !slices.Contains(claim.Finalizers, resourceapi.Finalizer) {
				claim.Finalizers = append(claim.Finalizers, resourceapi.Finalizer)
			}
			claim.Status.Allocation = allocation
			err := pl.draManager.ResourceClaims().SignalClaimPendingAllocation(claim.UID, claim)
			// ...
		}
```

Reserve 不写 API、不写 assume cache，只做两件事：把选中节点的分配结果记进 `informationsForClaim[index].allocation`，再把「带上 allocation 的 claim 副本」交给 `SignalClaimPendingAllocation`。后者往 `inFlightAllocations` 里插一条，`sharers` 计数为 1；如果已有条目（PodGroup 内共享），`sharers` 加 1。插入前它还会再查一次 assume cache，如果 claim 其实已经被分配了（CycleState 可能已经过时），就不插。

对于已经分配、只是还没 reserve 给本 Pod 的 claim，Reserve 什么都不做，源码注释说得很直接：先假定预留会成功，到 PreBind 再真正写 `reservedFor`，失败了说明别的 Pod 更快，重新调度即可。

### 8.2 in-flight 表为什么必须由「分配者」本人写回

`claimTracker.inFlightAllocations` 的注释里描述了两个反例，值得完整理解：

```go
// pkg/scheduler/framework/plugins/dynamicresources/dra_manager.go
	// This mechanism avoids the following problem:
	// - Pod A triggers allocation for claim X.
	// - Pod B shares access to that claim and gets scheduled because
	//   the claim is assumed to be allocated.
	// - PreBind for pod B is called first, tries to update reservedFor and
	//   fails because the claim is not really allocated yet.
	//
	// We could avoid the ordering problem by allowing either pod A or pod B
	// to set the allocation. But that is more complicated and leads to another
	// problem:
	// - Pod A and B get scheduled as above.
	// - PreBind for pod A gets called first, then fails with a temporary API error.
	//   It removes the updated claim from the in-flight claims because of that.
	// - PreBind for pod B gets called next and succeeds with adding the
	//   allocation and its own reservedFor entry.
	// - The assume cache is now not reflecting that the claim is allocated,
	//   which could lead to reusing the same resource for some other claim.
```

所以规则是：**claim 在 in-flight 里时，其他 Pod 一律等待**（PreFilter 里的 `is in the process of being allocated`）。唯一的例外是同一 PodGroup 调度周期内的 Pod，它们通过 `sharers` 引用计数共享，in-flight 条目要等所有共享者都 Unreserve 或者有一个 PreBind 成功后才移除。

### 8.3 PreBind：写回 claim

PreBind 在绑定 goroutine 里执行。`PreBindPreFlight` 对没有 claim 的 Pod 返回 `Skip`，有 claim 时返回 `AllowParallel: true`，允许和其他声明可并行的 PreBind 插件（比如 `VolumeBinding`）并发执行。

对每个还没 reserve 给本 Pod 的 claim，调用 `bindClaim`：

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// ...
		// Do we need to store an allocation result from Reserve?
		if allocation != nil {
			if claim.Status.Allocation != nil {
				return fmt.Errorf("claim %s got allocated elsewhere in the meantime", klog.KObj(claim))
			}

			// The finalizer needs to be added in a normal update.
			// If we were interrupted in the past, it might already be set and we simply continue.
			if !slices.Contains(claim.Finalizers, resourceapi.Finalizer) {
				claim.Finalizers = append(claim.Finalizers, resourceapi.Finalizer)
				updatedClaim, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{})
				// ...
				claim = updatedClaim
			}
			claim.Status.Allocation = allocation
		}

		// We can simply try to add the pod here without checking
		// preconditions. The apiserver will tell us with a
		// non-conflict error if this isn't possible.
		claim.Status.ReservedFor = append(claim.Status.ReservedFor, binding)
		// ...
		updatedClaim, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
		// ...
		claim = updatedClaim
		resourceClaimModified = true
		return nil
	})
```

finalizer 和 status 分两次请求，因为 finalizer 在 metadata 里，只能走普通 `Update`；allocation 和 reservedFor 在 status 里，只能走 `UpdateStatus`。先加 finalizer，保证 claim 在「已分配」状态下不会被直接删掉、导致设备泄漏。冲突时 `RetryOnConflict` 重新 Get 最新的 claim 再来一遍，如果发现已经被别人分配了，直接报错。

`binding` 通常是本 Pod 的引用；`DRAWorkloadResourceClaims`（1.37 Beta，默认关）开启时，属于 PodGroup 级 claim 的会写 PodGroup 的引用，这样同组 Pod 共享同一份 `reservedFor` 条目。

写成功后，`bindClaim` 的 defer 调用 `AssumeClaimAfterAPICall(claim)`，把 API Server 返回的新对象放进 assume cache。assume cache 的规则是只接受版本不低于当前的对象，后续 informer 事件到达时自然覆盖。这样下一个 Pod 的 PreFilter 立刻就能看到这个 claim 已分配，不用等 informer。

in-flight 条目的移除被推迟到整个 PreBind 成功之后：

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
	for index, claim := range state.claims.all() {
		if !resourceclaim.IsReservedForPod(pod, claim, pl.fts.EnableDRAWorkloadResourceClaims) {
			claim, successCleanup, err := pl.bindClaim(ctx, state, index, pod, nodeName)
			if err != nil {
				return statusError(logger, err)
			}
			defer func() {
				if retStatus.IsSuccess() && successCleanup != nil {
					successCleanup()
				}
			}()
			// Updated here such that Unreserve can work with patched claim.
			state.claims.set(index, claim)
		}
	}
```

`successCleanup` 以 `forceRemove=true` 调用 `MaybeRemoveClaimPendingAllocation`，不管还有几个 sharer 都删掉，因为 API 里已经有了真实的 allocation。`state.claims.set` 把 claim 换成 API 返回的新对象，这样如果后续 Bind 失败，Unreserve 能看到「这个 claim 已经 reserve 给我了」，知道要去撤销。

### 8.4 binding conditions：PreBind 里的等待

`DRADeviceBindingConditions`（1.36 Beta，默认开）为一类特殊设备设计：设备要先做一些准备（比如通过 fabric 挂到节点上），才能让 Pod 绑定。ResourceSlice 里的设备声明 `bindingConditions` 和 `bindingFailureConditions`，驱动在 `claim.status.devices[].conditions` 里更新它们。

PreBind 写完 claim 后，如果有设备带 binding conditions，就每 5 秒检查一次，直到全部为真、任一失败条件为真、或者超过 `bindingTimeout`（默认 600 秒）：

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, pl.bindingTimeout, true,
		func(ctx context.Context) (bool, error) {
			return pl.isPodReadyForBinding(state)
		})
```

失败或超时返回 `Error`，Pod 进 backoff。下一轮调度时，claim 已经是「已分配」状态，Filter 里的 `isClaimReadyForBinding`/`isClaimTimeout` 会把它记进 `unavailableClaims`，然后 PostFilter 把它释放掉，让分配器有机会换一批设备。超时的起点是 `allocation.allocationTimestamp`，这个字段也是 PreBind 写入的。

### 8.5 Unreserve：撤销

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
	for _, claim := range state.claims.allUserClaims() {
		// ...
		if deleted := pl.draManager.ResourceClaims().MaybeRemoveClaimPendingAllocation(claim.UID, false); deleted {
			// ...
			pl.draManager.ResourceClaims().AssumedClaimRestore(claim.Namespace, claim.Name)
			// ...
		}

		// Ignore claims reserved for the PodGroup because other Pods in the
		// group may still need this claim. The PodGroup may be unreserved and
		// ...
		if claim.Status.Allocation != nil &&
			resourceclaim.IsReservedForPod(pod, claim, false /* acceptPodGroupReservation */) {
			// Remove pod from ReservedFor. A strategic-merge-patch is used
			// because that allows removing an individual entry without having
			// the latest ResourceClaim.
			patch := fmt.Sprintf(`{"metadata": {"uid": %q}, "status": { "reservedFor": [ {"$patch": "delete", "uid": %q} ] }}`,
				claim.UID,
				pod.UID,
			)
			// ...
		}
	}
```

两段撤销对应两种进度：

- **还在 in-flight**（Reserve 之后、PreBind 之前失败，比如 Permit 被拒）：`sharers` 减 1，归零则删除条目，并 `Restore` assume cache 到 informer 版本。
- **已经写进 API**（PreBind 成功、Bind 失败）：用 strategic merge patch 从 `reservedFor` 删掉自己。patch 里带上 `metadata.uid`，防止误改同名的新 claim。`allocation` 本身不清，留给 resourceclaim controller 或下一轮 PostFilter 处理。

## 九、Score：只在 firstAvailable 上打分

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
		for _, req := range claim.Spec.Devices.Requests {
			if req.Exactly != nil {
				continue
			}
			for i, subReq := range req.FirstAvailable {
				subRequestRef := resourceclaim.CreateSubRequestRef(req.Name, subReq.Name)
				if allocatedSubRequests.Has(subRequestRef) {
					score += int64(resourceapi.FirstAvailableDeviceRequestMaxSize - i)
				}
			}
		}
```

`DRAPrioritizedList` 未开启时 Score 直接返回 0（该 gate 1.37 已 GA 且锁定为开）。打分只看 `firstAvailable` 请求：选中第 `i` 个子请求得 `8 - i` 分（`FirstAvailableDeviceRequestMaxSize = 8`，子请求最多 8 个）。`exactly` 请求不参与打分。

算例：Pod 有一个 claim，请求 `gpu` 的备选是 `[big, small]`。

| 节点 | 分配器选中 | 原始分 | `DefaultNormalizeScore(100)` | ×权重 2 |
| --- | --- | --- | --- | --- |
| node-a | `gpu/big`（i=0） | 8 | 8×100/8 = 100 | 200 |
| node-b | `gpu/small`（i=1） | 7 | 7×100/8 = 87 | 174 |
| node-c | 已分配的 claim，无 firstAvailable | 0 | 0 | 0 |

整数除法 700/8 取 87。因为分配器在每个节点上都是「从第一个子请求开始试」，原始分实际反映的是**这个节点能满足的最优备选是第几个**。多个 claim、多个 firstAvailable 请求的分数直接相加。

`computeScore` 里有一个细节：已分配的 claim 从 `claim.Status.Allocation` 取结果，未分配的从 `nodeAllocations[node].allocationResults` 按顺序取，两者用独立的下标 `unallocatedIndex` 对齐。这是因为 `allocationResults` 只包含本周期需要分配的那几个 claim。

## 十、PostFilter：释放一个闲置的 claim

所有节点都失败时，PostFilter 尝试从 `unavailableClaims` 里挑一个 claim 释放：

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
	// Iterating over a map is random. This is intentional here, we want to
	// pick one claim randomly because there is no better heuristic.
	for index := range state.unavailableClaims {
		claim := state.claims.get(index)
		// ...
		if len(claim.Status.ReservedFor) == 0 ||
			(len(claim.Status.ReservedFor) == 1 && claim.Status.ReservedFor[0].UID == pod.UID) {
			claim := claim.DeepCopy()
			claim.Status.ReservedFor = nil
			claim.Status.Allocation = nil
			claim.Status.Devices = nil
			// ...
			if _, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{}); err != nil {
				return true, statusError(logger, err)
			}
			return true, fwk.NewStatus(fwk.Unschedulable, "deallocation of ResourceClaim completed")
		}
	}
```

只有「没人在用」或者「只有我自己在用」的 claim 才能释放。典型场景：claim 是预先手工创建的，上次分配到了 node-a，Pod 后来被删、重建，新 Pod 因为 nodeAffinity 只能去 node-b，这时 claim 的 `nodeSelector` 指向 node-a，在所有节点上都不可用。PostFilter 清空它的 allocation，`isSchedulableAfterClaimChange` 看到「allocation 从有到无」返回 `Queue`，下一轮重新分配。

不管释放与否，PostFilter 都返回 `Unschedulable`，从不返回 `Success`。所以框架会继续调用下一个 PostFilter，也就是 `DefaultPreemption`。一次只释放一个，是因为每次释放都会触发重试，释放一个之后可能就够了。

`PodGroupPostFilter` 是它的 PodGroup 版本：对组内每个未调度 Pod 跑一遍同样的逻辑，如果有 Pod 找不到可释放的 claim，并且组内还没有任何 Pod 被调度成功（`ScheduledPodsCount() == 0`），再尝试释放 PodGroup 级 claim。它不支持 CompositePodGroup 和带拓扑约束的 PodGroup。

## 十一、为什么 DRA 的失败几乎都不可抢占

### 11.1 状态码层面

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
func statusUnschedulable(logger klog.Logger, reason string, kv ...interface{}) *fwk.Status {
	// ...
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, reason)
}
```

函数名叫 `statusUnschedulable`，返回的却是 `UnschedulableAndUnresolvable`。插件里所有「放不下」的分支都走它。抢占只在 Filter 返回 `Unschedulable` 的节点上找受害者（`NodesForStatusCode(..., fwk.Unschedulable)`），PreFilter 失败则所有节点都被标为同一个状态。所以凡是因为 DRA 失败的节点，抢占一律不考虑。

### 11.2 机制层面

即使改成 `Unschedulable`，抢占也帮不上忙，原因有三：

1. **没有 PreFilterExtensions**。抢占模拟靠 `RemovePod` 让插件更新 CycleState。`DynamicResources` 的分配器在 PreFilter 时就固定了「已分配设备集合」，移除一个受害者 Pod 不会让任何设备变成空闲。
2. **设备记账在 claim 上**。真正释放设备要等受害者 Pod 被删除、resourceclaim controller 把它从 `reservedFor` 移除并清空 allocation。这条链路不在调度器的模拟范围内。
3. **共享 claim**。一个 claim 可能被多个 Pod 共享，驱逐一个 Pod 不一定释放 claim。

所以 DRA 的「腾位置」走的是另一条路：PostFilter 释放闲置 claim。这也是 `default_plugins.go` 里把它排在 `DefaultPreemption` 前面的原因。

## 十二、事件与 QueueingHint

```go
// pkg/scheduler/framework/plugins/dynamicresources/dynamicresources.go
	events := []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel | fwk.UpdateNodeAllocatable}},
		{Event: fwk.ClusterEvent{Resource: fwk.ResourceClaim, ActionType: fwk.Add | fwk.Update | fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterClaimChange, PreQueueingHintFn: pl.preQueueingHint},
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodGeneratedResourceClaim}, QueueingHintFn: pl.isSchedulableAfterTargetPodUpdate},
		{Event: fwk.ClusterEvent{Resource: fwk.DeviceClass, ActionType: fwk.Add | fwk.Update}},
		{Event: fwk.ClusterEvent{Resource: fwk.ResourceSlice, ActionType: fwk.Add | fwk.Update}},
	}
```

Node、DeviceClass、ResourceSlice 三类事件没有 hint 函数，一律 `Queue`。ResourceSlice 的 Delete 不在列表里：设备变少不会让 Pod 变得可调度。

ResourceClaim 事件是最频繁的，所以有两层过滤。

**PreQueueingHint**：每个事件只调用一次，在遍历不可调度 Pod 之前先缩小范围：

- claim 的 allocation 从有到无，或者一个已分配的 claim 被删除：设备被释放了，可能任何 Pod 都会受益，返回 `AllPods: true`；
- 其他变化：通过 `New` 里注册的 Pod 索引，只返回引用了这个 claim 的 Pod。

在大集群里，不可调度池可能有成千上万个 Pod，大部分 claim 事件只和一两个 Pod 相关。这一层把 O(Pod 数) 的 hint 调用降到 O(相关 Pod 数)。

**QueueingHint** `isSchedulableAfterClaimChange` 再逐个判断：

| 情况 | 结果 |
| --- | --- |
| 删除事件且最终状态未知（`DeletedFinalStateUnknown`） | `Queue` |
| 删除的 claim 没有 allocation | `QueueSkip` |
| Pod 的 claim 有任何一个还找不到 | `QueueSkip` |
| 任意 claim 的 allocation 被清空 | `Queue` |
| 不是这个 Pod 引用的 claim | `QueueSkip` |
| Pod 引用的 claim 被创建 | `Queue` |
| status 完全没变（比如只加了 finalizer） | `QueueSkip` |
| status 有变化 | `Queue` |

「删除的 claim 没有 allocation 就跳过」依赖一个前提：调度器不会漏掉「allocation 被清空」这个更新事件。普通 informer 可能把「加 allocation」和「清 allocation」合并成一次看起来无关的更新，但这里的事件来自 assume cache，而 allocation 只由调度器自己写入，它一定先看到「加」，也就一定能看到「清」。

## 十三、边界情况与坑

**Filter 的超时会中止整个调度周期。** 设置了 `filterTimeout` 后，只要有一个节点的分配器超时，整个周期返回 `Error`，其他节点的结果作废，Pod 进 backoff。如果集群里某些节点的 ResourceSlice 特别多、请求特别复杂，可能出现「明明别的节点能放下，Pod 却一直 backoff」的现象。看 `scheduler_plugin_execution_duration_seconds{plugin="DynamicResources",extension_point="Filter"}` 是第一步。

**`Unschedulable` 的 PostFilter 消息很容易被误读。** 事件里出现 `deallocation of ResourceClaim completed` 不代表失败，它表示调度器刚释放了一个 claim，Pod 会在 claim 更新事件后重试。

**PreFilter 拒绝时不会写 `unavailableClaims`。** 比如 DeviceClass 不存在，PostFilter 读到的 `unavailableClaims` 是空的，什么都不会释放。

**DRA Pod 不参与 Pod 签名批处理。** `SignPod` 对任何带 `resourceClaims` 的 Pod 返回 `Unschedulable`，即不可签名。`OpportunisticBatching`（1.35 起 Beta）会给签名相同的 Pod 复用上一个 Pod 的节点评估结果，DRA 的分配结果和设备状态强相关，无法简单复用。源码注释承认这是保守做法，并给出了跟踪 issue。

**一个 claim 被多个 Pod 共享时，只有第一个 Pod 是「分配者」。** 后面的 Pod 走的是「已分配」路径，Filter 只检查 nodeSelector，Score 只读 `Status.Allocation`，PreBind 只追加 `reservedFor`。如果第一个 Pod 还在 PreBind 途中，后面的 Pod 会被 `is in the process of being allocated` 拒绝一次，等事件重试。

**与 `NodeResourcesFit` 的分工。** 传统扩展资源（device plugin）由 `NodeResourcesFit` 检查。`DRAExtendedResource` 开启后，同一个资源名在 device plugin 节点上由 `NodeResourcesFit` 负责，在 DRA 节点上由 `DynamicResources` 负责，`filterExtendedResources` 里通过节点 `allocatable` 是否为 0 来区分。

## 十四、发散

### 14.1 DRA 相关 feature gate 一览

以本提交的 `kube_features.go` 为准，与调度器插件相关的 gate：

| gate | 阶段（默认） | 在插件里的作用 |
| --- | --- | --- |
| `DRAPrioritizedList` | GA，锁定开 | 允许 `firstAvailable`；Score 生效 |
| `DRAAdminAccess` | 1.36 GA，锁定开 | admin access 请求可以使用已被占用的设备，不计入已分配集合 |
| `DRADeviceTaints` | 1.37 GA | 设备污点与容忍；slice 视图叠加 `DeviceTaintRule` |
| `DRAExtendedResource` | 1.37 GA，锁定开 | 第七节的特殊 claim |
| `DRAResourceClaimDeviceStatus` | 1.37 GA，锁定开 | `claim.status.devices` |
| `DRAPartitionableDevices` | Beta，开 | 可分区设备的 counter 记账 |
| `DRAConsumableCapacity` | Beta，开 | 一块设备按容量分给多个 claim |
| `DRADeviceBindingConditions` | Beta，开 | PreBind 等待 binding conditions；`bindingTimeout` |
| `DRASchedulerFilterTimeout` | Beta，开 | `filterTimeout` |
| `DRAFractionalCapacityRange` | Beta | 容量请求的区间语义，依赖 `DRAConsumableCapacity` |
| `DRAWorkloadResourceClaims` | 1.37 Beta，关 | PodGroup 级 claim，`reservedFor` 写 PodGroup |
| `DRANodeAllocatableResources` | Alpha，关 | 设备映射为节点 CPU/内存 |
| `DRAListTypeAttributes` | Alpha，关 | 列表类型的设备属性 |
| `DRAOptionalNodeOperations` | 1.37 Alpha，关 | 设备可跳过节点侧准备操作，要求节点声明特性 |
| `DRADerivedAttributes` | 1.37 Alpha，关 | 派生属性 |
| `DRADeviceCompatibilityGroups` | 1.37 Alpha，关 | 设备兼容组 |

这些 gate 通过 `AllocatorFeatures` 汇总成 `structured.Features`，直接决定用哪个分配器实现。打开任何一个 Alpha 特性都会让调度器切换到 `experimental` 分配器，这个切换是整体性的，生产环境开 Alpha gate 时要意识到这一点。

### 14.2 为什么不在 kubelet 侧分配

device plugin 时代，调度器只看一个整数计数，具体哪块设备由 kubelet 在 Admit 时决定。问题是调度器对设备拓扑一无所知：两块 GPU 是否在同一个 PCIe root、是否通过 NVLink 互连，调度时都无法表达。结果是 Pod 被调度到节点上之后，kubelet 才发现凑不出满足拓扑的组合，只能拒绝。

DRA 把选择提前到调度器，代价是调度器要做一次真正的组合搜索，并且引入了 in-flight 表、assume cache、PreBind 写 API 这一整套一致性机制。`filterTimeout` 的存在本身就说明了这个代价的量级：源码注释给的正常耗时是每节点 10 到 200 毫秒，比其他 Filter 插件高出几个数量级。

### 14.3 和 VolumeBinding 的相似之处

`DynamicResources` 的结构和 [VolumeBinding](/posts/sched-plugin-11-volume-binding/) 很像：都在 Filter 里为每个节点算出一个「绑定方案」并缓存在 CycleState，都在 Reserve 里把方案登记到一个跨 Pod 共享的内存结构，让后续 Pod 的调度周期看得见，都在 PreBind 里才真正写 API，失败时都靠 Unreserve 回滚内存登记。不同的是 PV 与 PVC 是一对一绑定，冲突检测相对简单；DRA 的一块设备可能被分区、按容量共享、带污点、带 binding conditions，状态空间大得多，于是有了独立的 `allocatedDevices` 增量集合、revision 重试，以及 in-flight 表的引用计数。

### 14.4 生产配置建议

- 如果集群有大量 ResourceSlice，先观察 Filter 耗时，再决定是否调小 `filterTimeout`。调小超时并不会让 Pod「换个节点试」，而是让整个周期 backoff。
- 不需要 binding conditions 的驱动不必关心 `bindingTimeout`；需要的驱动要确保在超时前更新 `status.devices[].conditions`，否则 claim 会被 PostFilter 释放，设备白准备一遍。
- 预先手工创建的 claim（不是模板生成的）在 Pod 删除后会被 controller 清空 allocation，但这依赖 controller 正常运行。如果看到 Pod 一直报 `resourceclaim not available on the node` 并伴随 `deallocation of ResourceClaim completed`，说明它在依靠调度器自己的 PostFilter 兜底。

## 小结

- `DynamicResources` 的 PreFilter 汇总「已分配设备集合」（informer 增量集合 + in-flight 表，revision 不一致就重试），构建一个每周期新建的分配器；Filter 对每个节点跑一次回溯搜索，第一个完整解即返回，结果缓存在 `nodeAllocations`。
- 默认 gate 下实际使用的是 `incubating` 分配器，它用 `startLocation` 把同一请求内的搜索从排列降到组合。
- Reserve 只写 in-flight 表，PreBind 先加 finalizer、再写 allocation 与 reservedFor，成功后放进 assume cache 并撤掉 in-flight；Unreserve 按进度分别回滚。
- Score 只对 `firstAvailable` 打分，选中第 `i` 个备选得 `8 - i` 分。
- 内部的 `statusUnschedulable` 返回的是 `UnschedulableAndUnresolvable`，加上没有 PreFilterExtensions、设备记账在 claim 上，DRA 的失败几乎不会触发抢占；它用 PostFilter 释放闲置 claim 来代替。

下一篇看一个小得多的插件：[NodeDeclaredFeatures](/posts/sched-plugin-16-node-declared-features/)，它检查的是节点声明的特性集合，DRA 的 `DRAOptionalNodeOperations` 就依赖它。
