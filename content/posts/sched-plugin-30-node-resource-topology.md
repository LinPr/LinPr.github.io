---
title: 'scheduler-plugins [out-of-tree] NodeResourceTopologyMatch：让调度器提前做一次 kubelet 的 NUMA 准入'
date: 2026-11-19T13:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "numa", "topology-manager"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NodeResourceTopologyMatch 读取 NodeResourceTopology CRD，在调度阶段模拟 kubelet Topology Manager 的 single-numa-node 准入，避免 Pod 被调度后又因 TopologyAffinityError 失败。本文拆解 NRT 数据模型、Filter 的位图算法、四种 Score 策略的真实行为、Passthrough/OverReserve/DiscardReserved 三种缓存、外来 Pod 检测、抢占模拟，以及只有读代码才看得到的坑。"
---

上一篇 [NetworkOverhead](/posts/sched-plugin-29-network-overhead/) 用 CRD 描述的网络代价给节点打分。本篇的 `NodeResourceTopologyMatch`（下文简称 NRT 插件）同样依赖一个 CRD，但它关心的不是节点之间，而是节点内部：一台机器上有几个 NUMA 节点、每个 NUMA 节点还剩多少 CPU、内存和设备。

它挂在 PreFilter、Filter、Score、Reserve、PostBind 五个扩展点上，并实现了 `EnqueueExtensions`。和所有 scheduler-plugins 插件一样，它不在 kube-scheduler 的默认 Profile 里，需要用 scheduler-plugins 编译出的调度器二进制（`cmd/scheduler/main.go` 里通过 `app.WithPlugin(noderesourcetopology.Name, noderesourcetopology.New)` 注册）并在 Profile 中显式启用，没有 feature gate。本文基于 scheduler-plugins 提交 `cfeb3aa`（依赖 Kubernetes v1.36.4），涉及 kube-scheduler 框架的部分以同一版本为准。

## 一、问题：节点总量够，kubelet 却拒绝

### 1.1 kubelet 的 Topology Manager

kubelet 里的 Topology Manager 负责让一个 Pod 拿到的 CPU、内存、设备尽量来自同一个 NUMA 节点。它向各个 hint provider（CPU Manager、Memory Manager、Device Manager）收集「哪些 NUMA 组合能满足请求」的提示，合并后按策略决定是否准入：

| 策略 | 合并后找不到满意的对齐时 |
| --- | --- |
| `none`（默认） | 不做任何对齐 |
| `best-effort` | 记下偏好，照样准入 |
| `restricted` | 拒绝准入 |
| `single-numa-node` | 只接受「全部资源来自一个 NUMA 节点」的结果，否则拒绝 |

作用域（`topologyManagerScope`）决定对齐的粒度：`container`（默认）逐个容器对齐，不同容器可以落在不同 NUMA 节点；`pod` 把整个 Pod 当成一个整体，所有容器必须落在同一组 NUMA 节点上。

被拒绝的 Pod 状态是 `TopologyAffinityError`，它已经绑定到节点上，不会再回到调度队列。如果由 ReplicaSet 管理，控制器会新建一个 Pod，调度器又只看节点总量，很可能把它再次放到同一个节点，形成循环。

### 1.2 调度器为什么看不见

kube-scheduler 的 `NodeResourcesFit` 只看 `Node.status.allocatable` 减去已分配的总量。一台双路服务器剩 16 个 CPU，可能是 NUMA 0 剩 6 个、NUMA 1 剩 10 个；剩一张 GPU，可能只挂在 NUMA 1 上。这些信息不在 Node 对象里，调度器无从判断一个「4 CPU + 16Gi 内存 + 1 GPU」的 Guaranteed Pod 能否在单个 NUMA 节点上放下。

NRT 插件的思路是：让节点上的代理把 NUMA 粒度的资源情况写进一个 CRD，调度器在 Filter 阶段用它把 kubelet 的准入逻辑预演一遍。

## 二、NodeResourceTopology CRD

### 2.1 数据模型

CRD 定义在依赖模块 `github.com/k8stopologyawareschedwg/noderesourcetopology-api`（本仓库用 v0.1.2），API 组 `topology.node.k8s.io`，版本 `v1alpha2`，集群级对象，名字等于节点名：

```go
// noderesourcetopology-api@v0.1.2/pkg/apis/topology/v1alpha2/types.go
type NodeResourceTopology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// DEPRECATED (to be removed in v1beta1): use top level attributes if needed
	TopologyPolicies []string `json:"topologyPolicies,omitempty"`

	Zones      ZoneList      `json:"zones"`
	Attributes AttributeList `json:"attributes,omitempty"`
}

type Zone struct {
	Name       string           `json:"name" protobuf:"bytes,1,opt,name=name"`
	Type       string           `json:"type" protobuf:"bytes,2,opt,name=type"`
	Parent     string           `json:"parent,omitempty" protobuf:"bytes,3,opt,name=parent"`
	Costs      CostList         `json:"costs,omitempty" protobuf:"bytes,4,rep,name=costs"`
	Attributes AttributeList    `json:"attributes,omitempty" protobuf:"bytes,5,rep,name=attributes"`
	Resources  ResourceInfoList `json:"resources,omitempty" protobuf:"bytes,6,rep,name=resources"`
}
```

每个 `ResourceInfo` 有 `Capacity`、`Allocatable`、`Available` 三个量，插件判定时只用 `Available`（`Allocatable` 只在抢占模拟里做上限校验）。一个由节点代理上报的对象大致如下（示例）：

```yaml
apiVersion: topology.node.k8s.io/v1alpha2
kind: NodeResourceTopology
metadata:
  name: worker-1                 # 必须等于节点名
attributes:
- name: topologyManagerPolicy
  value: single-numa-node
- name: topologyManagerScope
  value: container
- name: nodeTopologyPodsFingerprint
  value: pfp0v001...             # 节点上 Pod 集合的指纹，缓存重同步要用
zones:
- name: node-0                   # 必须是 node-<数字>
  type: Node
  costs:
  - {name: node-0, value: 10}
  - {name: node-1, value: 21}
  resources:
  - {name: cpu, capacity: "32", allocatable: "30", available: "6"}
  - {name: memory, capacity: 128Gi, allocatable: 120Gi, available: 20Gi}
  - {name: nvidia.com/gpu, capacity: "1", allocatable: "1", available: "0"}
- name: node-1
  type: Node
  # ...
```

这些对象由节点侧的「topology updater」生产，常见实现是 NFD 的 `nfd-topology-updater` 和 `resource-topology-exporter`（RTE）。它们通过 kubelet 的 podresources API 查询哪些 CPU、内存、设备已被独占分配，按 NUMA 归集后写回 NRT。

### 2.2 插件对字段的两个硬性要求

第一，zone 名必须能被 `numanode.NameToID` 解析，它要求前缀是 `node-`：

```go
// pkg/noderesourcetopology/pluginhelpers.go
func createNUMANodeList(lh logr.Logger, zones topologyv1alpha2.ZoneList) NUMANodeList {
	numaIDToZoneIDx := make([]int, maxNUMAId)
	nodes := NUMANodeList{}
	// filter non Node zones and create idToIdx lookup array
	for i, zone := range zones {
		if zone.Type != helper.ZoneTypeNUMANode {
			continue
		}

		numaID, err := numanode.NameToID(zone.Name)
		if err != nil || numaID > maxNUMAId {
			lh.Error(err, "error getting the numaID", "zone", zone.Name, "numaID", numaID)
			continue
		}
		// ...
		nodes = append(nodes, NUMANode{NUMAID: numaID, Resources: resources})
	}
	// ...
}
```

`type` 不是 `Node` 的 zone、名字解析失败的 zone 都被静默跳过。插件自己的 `pkg/noderesourcetopology/README.md` 演示部分写的是 `numa-node-0`，按这段代码会被全部丢弃；`manifests/noderesourcetopology/worker-node-A.yaml` 里用的是 `node-0`，以后者为准。顺带一提，`maxNUMAId` 是 64，判断条件是 `> maxNUMAId`，ID 恰好为 64 时会越界访问长度为 64 的数组，好在现实中没有这么多 NUMA 节点。

第二，Topology Manager 的策略和作用域要从 NRT 里读：

```go
// pkg/noderesourcetopology/nodeconfig/topologymanager.go
func TopologyManagerFromNodeResourceTopology(lh logr.Logger, nodeTopology *topologyv1alpha2.NodeResourceTopology) TopologyManager {
	conf := TopologyManagerDefaults()
	cfg := &conf // shortcut
	// Backward compatibility (v1alpha2 and previous). Deprecated, will be removed when the NRT API moves to v1beta1.
	cfg.updateFromPolicies(lh, nodeTopology.Name, nodeTopology.TopologyPolicies)
	// preferred new configuration source (v1alpha2 and onwards)
	cfg.updateFromAttributes(lh, nodeTopology.Attributes)
	return conf
}
```

默认值是 `scope=container`、`policy=none`、`maxNUMANodes=8`，与 kubelet 一致。先读已废弃的 `topologyPolicies`（如 `SingleNUMANodeContainerLevel`），再用顶层 `attributes` 中的 `topologyManagerPolicy`、`topologyManagerScope`、`topologyManagerMaxNUMANodes` 覆盖。代理没有上报这些属性时，节点会被当成 `policy=none`，Filter 直接放行。这是部署时最常见的「插件装了却不起作用」的原因。

## 三、插件结构与参数

### 3.1 结构体与工厂

```go
// pkg/noderesourcetopology/plugin.go
type TopologyMatch struct {
	logger              klog.Logger
	resourceToWeightMap resourceToWeightMap
	nrtCache            nrtcache.Interface
	scoreStrategyFunc   scoreStrategyFn
	scoreStrategyType   apiconfig.ScoringStrategyType
	preemptionMode      apiconfig.PreemptionMode
}
```

真正的复杂度在 `nrtCache` 里。`New` 校验参数后调用 `initNodeTopologyInformer`，按参数挑一种缓存实现：

```go
// pkg/noderesourcetopology/pluginhelpers.go
func initNodeTopologyInformer(ctx context.Context, lh logr.Logger,
	tcfg *apiconfig.NodeResourceTopologyMatchArgs, handle fwk.Handle) (nrtcache.Interface, error) {
	client, err := ctrlclient.NewWithWatch(handle.KubeConfig(), ctrlclient.Options{Scheme: scheme})
	// ...
	if tcfg.DiscardReservedNodes {
		return nrtcache.NewDiscardReserved(lh.WithName(logging.SubsystemNRTCache), client), nil
	}

	if tcfg.CacheResyncPeriodSeconds <= 0 {
		return nrtcache.NewPassthrough(lh.WithName(logging.SubsystemNRTCache), client), nil
	}

	podSharedInformer, podLister, err := podprovider.NewFromHandle(lh, handle, tcfg.Cache)
	// ...
	nrtCache, err := nrtcache.NewOverReserve(ctx, lh.WithName(logging.SubsystemNRTCache), tcfg.Cache, client, podLister, *tcfg.PreemptionMode)
	// ...
	initNodeTopologyForeignPodsDetection(lh, tcfg.Cache, handle, podSharedInformer, nrtCache)

	resyncPeriod := time.Duration(tcfg.CacheResyncPeriodSeconds) * time.Second
	go wait.Forever(nrtCache.Resync, resyncPeriod)
	// ...
	return nrtCache, nil
}
```

注意这个 `client` 是 controller-runtime 的直连客户端，没有配置 cache reader。后面会看到这对 Passthrough 模式意味着什么。

### 3.2 参数与默认值

`NodeResourceTopologyMatchArgs` 的默认值在 `apis/config/v1/defaults.go` 的 `SetDefaults_NodeResourceTopologyMatchArgs`：

| 字段 | 默认值 | 作用 |
| --- | --- | --- |
| `scoringStrategy.type` | `LeastAllocated` | 打分策略，校验只接受 `MostAllocated`、`BalancedAllocation`、`LeastAllocated`、`LeastNUMANodes` |
| `scoringStrategy.resources` | `cpu:1`、`memory:1` | 打分权重，未列出的资源权重按 1 计 |
| `cacheResyncPeriodSeconds` | 0 | 大于 0 才启用 OverReserve 缓存 |
| `discardReservedNodes` | false | 为 true 时使用 DiscardReserved，优先级最高 |
| `cache.foreignPodsDetect` | `All` | 外来 Pod 检测范围 |
| `cache.resyncMethod` | `Autodetect` | 指纹的计算口径 |
| `cache.informerMode` | `Dedicated` | 缓存用独立的 Pod informer |
| `cache.resyncScope` | 未设置，代码回退为 `All` | 是否监听 NRT 属性变化 |
| `preemptionMode` | `Disabled` | 抢占模拟，需要显式设为 `Enabled` |

推荐用 `multiPoint` 启用，这样 PreFilter、Filter、Score、Reserve、PostBind 一并打开。OverReserve 和 DiscardReserved 都依赖 Reserve；只配 `filter` 和 `score`、却设置了 `cacheResyncPeriodSeconds`，缓存永远收不到预留，等于白开。

## 四、PreFilter：只为抢占服务

```go
// pkg/noderesourcetopology/prefilter.go
const stateVictimPodsKey fwk.StateKey = "stateVictimPods"

// PodsInfo is a map of namespace/podname to pod and is forming the preemption stack, i.e. the victim pods.
type PodsInfo map[string]*v1.Pod

// ...

func (s PodsInfo) Clone() fwk.StateData {
	cloned := make(PodsInfo, len(s))
	for podID, pod := range s {
		cloned[podID] = pod.DeepCopy()
	}
	return cloned
}

// The extension point is implemented only to be able to use AddPod/RemovePod functions during preemption flow
func (tm *TopologyMatch) PreFilter(ctx context.Context, cycleState fwk.CycleState, _ *v1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if tm.preemptionMode == apiconfig.PreemptionDisabled {
		return nil, nil
	}
	cycleState.Write(stateVictimPodsKey, make(PodsInfo))
	return nil, fwk.NewStatus(fwk.Success, "")
}
```

PreFilter 不做任何判定，不缩小节点集合，也不返回 `Skip`。它存在的唯一理由是注释说的：实现 `PreFilterExtensions`，从而在抢占模拟时收到 `AddPod`/`RemovePod` 回调。

DefaultPreemption 在某个节点上模拟「驱逐这些受害者后能否放下」时，会对每个受害者调用 `RemovePod`，插件把它压入 CycleState 里的受害者栈；回滚时调用 `AddPod`，从栈里弹出。`Clone` 做深拷贝，是因为抢占会为每个候选节点克隆一份 CycleState 并发模拟，各节点的受害者集合不能互相污染。抢占关闭时，三个方法都直接返回，CycleState 里没有这个 key。

## 五、Filter：模拟 single-numa-node

### 5.1 入口

```go
// pkg/noderesourcetopology/filter.go
func (tm *TopologyMatch) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if nodeInfo.Node() == nil {
		return fwk.NewStatus(fwk.Error, "node not found")
	}
	qos := v1qos.GetPodQOS(pod)
	if qos == v1.PodQOSBestEffort && !resourcerequests.IncludeNonNative(pod) {
		return nil
	}

	nodeName := nodeInfo.Node().Name
	// ...
	nodeTopology, info := tm.nrtCache.GetCachedNRTCopy(ctx, nodeName, pod)
	lh = lh.WithValues(logging.KeyGeneration, info.Generation)
	if !info.Fresh {
		lh.V(2).Info("invalid topology data")
		return fwk.NewStatus(fwk.Unschedulable, "invalid node topology data")
	}
	if nodeTopology == nil {
		return nil
	}
	// ... 抢占流程，见第八节
	conf := nodeconfig.TopologyManagerFromNodeResourceTopology(lh, nodeTopology)
	// ...
	handler, scope := filterHandlerFromTopologyManager(conf)
	if handler == nil {
		return nil
	}

	numaNodes := createNUMANodeList(lh, nodeTopology.Zones)
	// ...
	fi := filterInfo{
		nodeName:        nodeName,
		node:            nodeInfo,
		topologyManager: conf,
		numaNodes:       numaNodes,
		qos:             qos,
	}
	status := handler(lh, pod, &fi)
	if !isPreemptionFlow && status != nil {
		tm.nrtCache.NodeMaybeOverReserved(nodeName, pod)
	}
	return status
}
```

几个早退分支值得逐个看：

- 不请求扩展资源的 BestEffort Pod 直接放行，因为 kubelet 不会给它任何对齐；
- `Fresh=false`（缓存认为数据不可信，比如节点上有外来 Pod）返回 `Unschedulable`，原因 `invalid node topology data`；
- 节点没有 NRT 对象时**放行**。没部署代理的节点、名字不匹配的 NRT，都会让插件对这个节点完全失效；
- 策略不是 `single-numa-node` 时 `filterHandlerFromTopologyManager` 返回 nil，同样放行。`restricted` 和 `best-effort` 节点不受保护。

所有拒绝都用 `Unschedulable` 而不是 `UnschedulableAndUnresolvable`，所以 DefaultPreemption 会尝试在这些节点上找受害者。拒绝后还会调用 `NodeMaybeOverReserved` 把节点标脏，这是缓存重同步的触发器，第七节细讲。

### 5.2 核心：逐资源求 NUMA 位图

{{< figure src="/images/posts/sched-plugins/30-nrt-filter.svg" caption="Filter 对每种请求资源算出一个「哪些 zone 单独能满足」的位图，逐个取交集；container 作用域逐容器扣减，pod 作用域整体判定（点击查看大图）" link="/images/posts/sched-plugins/30-nrt-filter.svg" >}}

两种作用域最终都调用 `resourcesAvailableInAnyNUMANodes`：

```go
// pkg/noderesourcetopology/filter.go
func resourcesAvailableInAnyNUMANodes(lh logr.Logger, info *filterInfo, resources v1.ResourceList) (int, bool, string) {
	numaID := info.topologyManager.MaxNUMANodes // highest NUMA ID
	bitmask := bm.NewEmptyBitMask()
	// set all bits, each bit is a NUMA node, if resources couldn't be aligned
	// on the NUMA node, bit should be unset
	bitmask.Fill()

	nodeResources := util.ResourceList(info.node.GetAllocatable())

	for resource, quantity := range resources {
		// ...
		if quantity.IsZero() {
			// why bother? everything's fine from the perspective of this resource
			continue
		}

		if _, ok := nodeResources[resource]; !ok {
			// ...
			return -1, false, string(resource)
		}

		hasNUMAAffinity := false
		resourceBitmask := bm.NewEmptyBitMask()
		for _, numaNode := range info.numaNodes {
			numaQuantity, ok := numaNode.Resources[resource]
			if !ok {
				continue
			}

			hasNUMAAffinity = true
			if !isResourceSetSuitable(info.qos, resource, quantity, numaQuantity) {
				continue
			}

			resourceBitmask.Add(numaNode.NUMAID)
		}

		// non-native resources or ephemeral-storage may not expose NUMA affinity,
		// but since they are available at node level, this is fine
		if !hasNUMAAffinity && isHostLevelResource(resource) {
			continue
		}

		bitmask.And(resourceBitmask)
		if bitmask.IsEmpty() {
			lh.V(2).Info("early verdict: cannot find affinity")
			return numaID, false, string(resource)
		}
	}
	// ...
	numaID = bitmask.GetBits()[0]

	// at least one NUMA node is available
	ret := !bitmask.IsEmpty()
	return numaID, ret, "generic"
}
```

这里用的 `bitmask` 包直接来自 kubelet（`k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask`），和 Topology Manager 合并 hint 时用的是同一个数据结构。算法本身很直白：一开始假设所有 zone 都可行，每处理一种资源就把「放不下」的 zone 位清掉，位图一旦为空立刻失败。最终选中的 zone 是剩余位中编号最小的那个，对应 kubelet 在多个等宽可行 hint 中偏好低编号的行为（`IsNarrowerThan` 在位数相同时比较 `IsLessThan`）。

资源分三类处理，对应两个辅助函数：

```go
// pkg/noderesourcetopology/numaresources.go
func isHostLevelResource(resource corev1.ResourceName) bool {
	// ...
	if resource == corev1.ResourceEphemeralStorage {
		return true
	}
	if resource == corev1.ResourceStorage {
		return true
	}
	if !v1helper.IsNativeResource(resource) {
		return true
	}
	return false
}

func isResourceSetSuitable(qos corev1.PodQOSClass, resource corev1.ResourceName, quantity, numaQuantity resource.Quantity) bool {
	if qos != corev1.PodQOSGuaranteed && isNUMAAffineResource(resource) {
		return true
	}
	return numaQuantity.Cmp(quantity) >= 0
}
```

- 节点级都没有的资源：立刻失败，原因是该资源名；
- 所有 zone 都没上报的资源，如果是 ephemeral-storage、storage 或扩展资源（比如没有 NUMA 亲和的设备），视为节点级资源，跳过；如果是 CPU、内存这类原生资源却没有任何 zone 上报，`resourceBitmask` 为空，`And` 之后必然失败；
- 非 Guaranteed Pod 的 CPU、内存、hugepages 在任何 zone 都算「合适」。kubelet 的 CPU Manager 和 Memory Manager 只给 Guaranteed Pod 做独占分配，Burstable Pod 的这些资源不参与对齐，所以对 Burstable Pod 插件实际只检查设备。

### 5.3 container 作用域：逐容器扣减

```go
// pkg/noderesourcetopology/filter.go
func singleNUMAContainerLevelHandler(lh logr.Logger, pod *v1.Pod, info *filterInfo) *fwk.Status {
	// the init containers are running SERIALLY and BEFORE the normal containers.
	// https://kubernetes.io/docs/concepts/workloads/pods/init-containers/#understanding-init-containers
	// therefore, we don't need to accumulate their resources together
	for initContainer := range podutil.ContainerIter(&pod.Spec, podutil.InitContainers) {
		// ...
		_, match, reason := resourcesAvailableInAnyNUMANodes(clh, info, initContainer.Resources.Requests)
		if !match {
			msg := "cannot align " + cntKind + " container"
			// we can't align init container, so definitely we can't align a pod
			clh.V(2).Info(msg, "reason", reason)
			return fwk.NewStatus(fwk.Unschedulable, msg)
		}
	}

	for container := range podutil.ContainerIter(&pod.Spec, podutil.Containers) {
		// ...
		numaID, match, reason := resourcesAvailableInAnyNUMANodes(clh, info, container.Resources.Requests)
		if !match {
			// we can't align container, so definitely we can't align a pod
			clh.V(2).Info("cannot align container", "reason", reason)
			return fwk.NewStatus(fwk.Unschedulable, "cannot align container")
		}

		// subtract the resources requested by the container from the given NUMA.
		// this is necessary, so we won't allocate the same resources for the upcoming containers
		err := subtractResourcesFromNUMANodeList(clh, info.numaNodes, numaID, info.qos, container.Resources.Requests)
		if err != nil {
			// this is an internal error which should never happen
			return fwk.NewStatus(fwk.Error, "inconsistent resource accounting", err.Error())
		}
	}
	return nil
}
```

这段完全对应 kubelet `scope_container.go` 的行为：先 init 容器、后 app 容器逐个准入，每个 app 容器选定 zone 后扣减，后面的容器看到的是扣减后的余量。扣减作用在本次 Filter 新建的 `NUMANodeList` 上（`createNUMANodeList` 每次重新生成），不会污染缓存。

失败原因有三种：`cannot align init container`、`cannot align sidecar container`（`GetInitContainerKind` 根据 `restartPolicy: Always` 区分）、`cannot align container`。这里有一个和 kubelet 不完全一致的地方：sidecar 容器（可重启的 init 容器）会一直运行，kubelet 会把它的独占资源保留到 Pod 结束，而这里对所有 init 容器都不扣减，带独占资源的 sidecar 可能让后续 app 容器的判定偏乐观。

### 5.4 pod 作用域：一次性判定

```go
// pkg/noderesourcetopology/filter.go
func singleNUMAPodLevelHandler(lh logr.Logger, pod *v1.Pod, info *filterInfo) *fwk.Status {
	resources := util.GetPodEffectiveRequest(pod)
	// ...
	numaID, match, reason := resourcesAvailableInAnyNUMANodes(lh, info, resources)
	if !match {
		lh.V(2).Info("cannot align pod", "name", pod.Name, "reason", reason)
		return fwk.NewStatus(fwk.Unschedulable, "cannot align pod")
	}
	// ...
	return nil
}
```

`GetPodEffectiveRequest`（`pkg/util/resource.go`）对 app 容器求和、对 init 容器取最大值，两者再取较大者，最后加上 `Overhead`。上图下半部分是同一个 Pod 在两种作用域下的不同结论：两个各要 4 CPU 的容器，在 6+5 的余量下 container 作用域能放，pod 作用域放不下。这也说明了为什么作用域必须从 NRT 读，而不能由插件假设。

## 六、Score：四种策略与「取最小」

### 6.1 入口与前提

```go
// pkg/noderesourcetopology/score.go
func (tm *TopologyMatch) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	nodeName := nodeInfo.Node().Name
	// ...
	// if it's a non-guaranteed pod, every node is considered to be a good fit
	qos := v1qos.GetPodQOS(pod)
	if qos != v1.PodQOSGuaranteed {
		return fwk.MaxNodeScore, nil
	}

	nodeTopology, info := tm.nrtCache.GetCachedNRTCopy(ctx, nodeName, pod)
	// ...
	if !info.Fresh {
		return 0, nil
	}
	if nodeTopology == nil {
		return 0, nil
	}
	// ...
	conf := nodeconfig.TopologyManagerFromNodeResourceTopology(lh, nodeTopology)
	handler := tm.scoringHandlerFromTopologyManagerConfig(conf)
	if handler == nil {
		return 0, nil
	}
	// ...
}

func (tm *TopologyMatch) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}
```

非 Guaranteed Pod 所有节点 100 分，插件对它不起作用。没有 NRT 的节点、策略不匹配的节点都是 0 分。和 Filter 对比一下：没有 NRT 的节点在 Filter 里被放行，在 Score 里被压到最低，对 Guaranteed Pod 来说，有 NRT 的节点会被优先选择。

插件没有 `NormalizeScore`，各策略直接产出 `[0, 100]` 的分数。权重由 Profile 配置决定，没写 `weight` 时框架的 `getValidScoreWeights` 按 1 处理。

### 6.2 前三种策略：每个 zone 一分，取最小非零值

`MostAllocated`、`LeastAllocated`、`BalancedAllocation` 只在 `single-numa-node` 节点上生效，pod 作用域对 Pod 有效请求打分一次，container 作用域对每个容器（含 init 容器）各打一次再取算术平均。打分的核心在 `scoreForEachNUMANode`：

```go
// pkg/noderesourcetopology/score.go
func scoreForEachNUMANode(lh logr.Logger, requested v1.ResourceList, numaList NUMANodeList, score scoreStrategyFn, resourceToWeightMap resourceToWeightMap) int64 {
	numaScores := make([]int64, len(numaList))
	minScore := int64(0)

	for _, numa := range numaList {
		numaScore := score(requested, numa.Resources, resourceToWeightMap)
		// if NUMA's score is 0, i.e. not fit at all, it won't be taken under consideration by Kubelet.
		if (minScore == 0) || (numaScore != 0 && numaScore < minScore) {
			minScore = numaScore
		}
		numaScores[numa.NUMAID] = numaScore
		// ...
	}
	return minScore
}
```

每种策略对单个 zone 的算法：

```go
// pkg/noderesourcetopology/least_allocated.go
func leastAllocatedScore(requested, numaCapacity resource.Quantity) int64 {
	if numaCapacity.CmpInt64(0) == 0 {
		return 0
	}
	if requested.Cmp(numaCapacity) > 0 {
		return 0
	}
	numaValue := numaCapacity.Value()
	requestedValue := requested.Value()
	return (numaValue - requestedValue) * fwk.MaxNodeScore / numaCapacity.Value()
}
```

`MostAllocated` 是 `requested * 100 / numaCapacity`；`BalancedAllocation` 算各资源 `requested / capacity` 的比例，用 gonum 的 `stat.Variance`（无偏样本方差，除以 n−1）求方差，分数为 `(1 − 方差) × 100`。几处与 in-tree `NodeResourcesFit` 不同：

- 分母是 zone 的 `Available`，不是容量。同样是「剩余比例」，in-tree 算的是 `(allocatable − 已请求 − 本 Pod) / allocatable`，这里算的是 `(available − 本 Pod) / available`，与 zone 已经用了多少无关；
- 只遍历 Pod 请求了的资源，权重表里有但 Pod 没请求的资源不参与；
- `Quantity.Value()` 对 CPU 向上取整到核，500m 按 1 核算。

用一个算例看「取最小非零」带来的后果：

{{< figure src="/images/posts/sched-plugins/30-nrt-score.svg" caption="同一个 Guaranteed Pod 在两个节点上三种策略的打分：每个 zone 单独算分，节点分取非零 zone 分的最小值（点击查看大图）" link="/images/posts/sched-plugins/30-nrt-score.svg" >}}

以 `LeastAllocated` 为例，A 的 node-0：CPU `(8−4)×100/8 = 50`，内存 `(32−8)×100/32 = 75`，权重各 1，`(50+75)/2 = 62`（整数除法）；node-1 两项都是 75。节点 A 取 min(62, 75) = 62。B 的 node-0 恰好装满，两项都是 0，被当作「放不下」跳过；node-1 是 `(86+87)/2 = 86`。B 以 86 胜出。

这个「取最小」的设计意图是保守：节点分反映最差的那个可行 zone。但它和 kubelet 的实际选择脱节：

1. kubelet 会把 Pod 放进编号最小的可行 zone。B 上实际落在 node-0，而 node-0 的分数恰好被丢弃了；
2. 恰好装满（剩余 0）与放不下用同一个值 0 表示，完美贴合的 zone 在 `LeastAllocated` 下被忽略；
3. `MostAllocated` 本意是装箱，B 有一个 100 分的完美贴合 zone，节点分却被另一个 zone 的 12 分决定，输给了 A。

结论是：这三种策略更适合「NUMA 布局相近、负载分布均匀」的集群，用来在节点之间粗粒度地偏好或回避拥挤；不要指望它们在节点内部做精确装箱。README 也明确说明，打分只影响节点选择，zone 的选择永远由 kubelet 决定。

另一个细节：`numaScores[numa.NUMAID]` 用 NUMA ID 做下标，而切片长度是 zone 个数。如果某个节点只上报了 `node-0` 和 `node-2`，这一行会越界 panic。正常硬件的 NUMA 编号是连续的，但手工构造 NRT 测试时要注意。

### 6.3 LeastNUMANodes：偏好跨越最少 NUMA 的节点

`LeastNUMANodes` 不检查策略，适用于所有 Topology Manager 策略（典型搭配是 `restricted` 或 `best-effort`，这时 Pod 允许跨 NUMA，但跨得越少越好）。它对 pod 作用域求有效请求、对 container 作用域逐容器求（同样扣减），算「至少需要几个 zone 才能放下」：

```go
// pkg/noderesourcetopology/least_numa.go
func numaNodesRequired(lh logr.Logger, qos v1.PodQOSClass, numaNodes NUMANodeList, resources v1.ResourceList) (bitmask.BitMask, bool) {
	for bitmaskLen := 1; bitmaskLen <= len(numaNodes); bitmaskLen++ {
		numaNodesCombination := combin.Combinations(len(numaNodes), bitmaskLen)
		suitableCombination, isMinDistance := findSuitableCombination(lh, qos, numaNodes, resources, numaNodesCombination)
		// we have found suitable combination for given bitmaskLen
		if suitableCombination != nil {
			// ...
			return bm, isMinDistance
		}
	}

	return nil, false
}

func normalizeScore(numaNodesCount int, isMinAvgDistance bool, highestNUMAID int) int64 {
	numaNodeScore := fwk.MaxNodeScore / int64(highestNUMAID)
	score := fwk.MaxNodeScore - int64(numaNodesCount)*numaNodeScore
	if isMinAvgDistance {
		// if distance between NUMA domains is optimal add half of numaNodeScore to make this node more favorable
		return score + numaNodeScore/2
	}

	return score
}
```

从 1 个 zone 开始枚举组合（`gonum` 的 `combin.Combinations`），找到能放下的最小组合数；同样组合数下，再用 zone 的 `costs`（即 ACPI SLIT 距离）计算组合内的平均距离，等于所有同规模组合中的最小平均距离时额外加半档分。

算例：`topologyManagerMaxNUMANodes` 没上报时取默认 8，每档 `100/8 = 12` 分。一个 zone 能放下的 Pod 得 `100 − 12 + 6 = 94`（单个 zone 的「平均距离」就是自身距离，所有单 zone 组合都相等，必然拿到加分）；需要两个 zone 时得 `100 − 24 = 76`，若这两个 zone 恰好是距离最近的一对则为 82；放不下时 0 分。container 作用域取所有容器中需要 zone 数最多的那个，而距离加分要求每个容器都是最优距离。

枚举组合的复杂度是 2 的 zone 数次方量级，但现实中节点通常只有 2 到 8 个 NUMA 节点，开销可以接受。

## 七、缓存：为什么数据总是落后

### 7.1 落后从哪里来

NRT 是一个最终一致的分布式视图。一个 Pod 从被调度到反映在 NRT 中，要经过：调度器 Bind → kubelet 收到 Pod → Topology Manager 准入并分配 CPU/内存/设备 → 代理通过 podresources API 观察到变化 → 代理更新 NRT → 调度器读到新 NRT。代理的上报周期通常是 10 到 60 秒（缓存设计文档 `cache/docs/NUMA-aware-scheduler-side-reserve-plugin.md` 的说法）。在这个窗口里，调度器看到的 zone 余量都是旧的。

批量创建 Pod 时问题最明显：十个 Guaranteed Pod 在同一秒内进入调度，都看到 NUMA 0 剩 6 个 CPU，都被放到同一个节点，kubelet 准入第一个之后，后面的依次 `TopologyAffinityError`。in-tree 调度器通过 assume 机制在内存里扣减节点总量，规避了这个问题；但 NUMA 粒度的扣减做不到同样精确，因为调度器不知道 kubelet 会选哪个 zone。设计文档给出的原则是：**宁可让 Pod 多等一会儿，也不要产生 TopologyAffinityError**。

### 7.2 三种实现

`cache.Interface` 有三个实现：

| 实现 | 启用条件 | 读 NRT | Reserve 后的行为 |
| --- | --- | --- | --- |
| `Passthrough` | `cacheResyncPeriodSeconds <= 0`（默认） | 每次 Filter/Score 直接 `client.Get` | 什么都不做 |
| `DiscardReserved` | `discardReservedNodes: true` | 同上 | 节点有未完成的预留时，`GetCachedNRTCopy` 返回 `Fresh=false`，节点被拒绝，直到 PostBind 或 Unreserve 清除 |
| `OverReserve` | `cacheResyncPeriodSeconds > 0` | 读内存快照，再悲观扣减 | 记录预留量，从**所有** zone 扣减 |

Passthrough 有一个容易被忽视的性能问题：前面提到的 `client` 是直连 API Server 的客户端，`GetCachedNRTCopy` 每次都发一个 GET 请求。Filter 和 Score 各调用一次，一个 Pod 的调度周期里，每个被评估的节点就是两次 API 请求。大集群里这会给 API Server 带来可观的压力，同时拖慢调度周期本身。

DiscardReserved 只覆盖「Reserve 到 PostBind」这一小段，也就是绑定还没完成的时间。PostBind 之后预留就被清掉了，此时 NRT 依然没有反映这个 Pod，所以它只能防住同一节点上的并发绑定，防不住上报延迟。源码注释也承认，在代理不工作、网络慢等情况下，它退化成 Passthrough。

### 7.3 OverReserve 的工作方式

{{< figure src="/images/posts/sched-plugins/30-nrt-cache.svg" caption="OverReserve 缓存的状态、写入来源与重同步流水线：读取时悲观扣减，只有指纹证明 NRT 已经追上调度器的认知时才替换快照（点击查看大图）" link="/images/posts/sched-plugins/30-nrt-cache.svg" >}}

启动时 `NewOverReserve` 用一次 `List` 拿到全部 NRT，建立快照。此后**快照内容不会随 NRT 更新而自动刷新**，只有在 `FlushNodes` 时才替换。读取路径：

```go
// pkg/noderesourcetopology/cache/overreserve.go
func (ov *OverReserve) GetCachedNRTCopy(ctx context.Context, nodeName string, pod *corev1.Pod) (*topologyv1alpha2.NodeResourceTopology, CachedNRTInfo) {
	ov.lock.Lock()
	defer ov.lock.Unlock()
	info := CachedNRTInfo{Generation: ov.generation}
	if ov.nodesWithForeignPods.IsSet(nodeName) {
		return nil, info
	}

	info.Fresh = true
	nrt := ov.nrts.GetNRTCopyByNodeName(nodeName)
	if nrt == nil {
		return nil, info
	}
	nodeAssumedResources, ok := ov.assumedResources[nodeName]
	if !ok {
		return nrt, info
	}
	// ...
	nodeAssumedResources.UpdateNRT(nrt, logging.KeyPod, logID)
	// ...
	return nrt, info
}
```

`UpdateNRT` 把这个节点上所有已预留 Pod 的有效请求从每个 zone 的 `Available` 里减掉，不够减时置 0：

```go
// pkg/noderesourcetopology/cache/store.go
func (rs *resourceStore) UpdateNRT(nrt *topologyv1alpha2.NodeResourceTopology, logKeysAndValues ...any) {
	for key, res := range rs.data {
		// We cannot predict on which Zone the workload will be placed.
		// And we should totally not guess. So the only safe (and conservative)
		// choice is to decrement the available resources from *all* the zones.
		// This can cause false negatives, but will never cause false positives,
		// which are much worse.
		for zi := 0; zi < len(nrt.Zones); zi++ {
			// ...
				zr.Available.Sub(qty)
			// ...
		}
	}
}
```

这就是 Over-Reserve 名字的由来：一个 4 核 Pod 在双 NUMA 节点上会让两个 zone 各少 4 核，账面上「占用」了 8 核。随着预留累积，节点在缓存眼里越来越满，最终某个 Pod 被 Filter 拒绝，`NodeMaybeOverReserved` 把节点计入 `nodesMaybeOverreserved`。缓存不区分「真的满了」和「被悲观扣满了」，交给重同步去判断。

预留只在 `UnreserveNodeResources` 和 Flush 时删除。Pod 结束、被删除都不会减少预留，节点只会单调地变「满」，直到 Flush。

### 7.4 重同步与 Pod 指纹

`Resync` 每 `cacheResyncPeriodSeconds` 秒执行一次：

1. `drainNRTEvents` 取出 watcher 发来的事件；
2. `GetDesyncedNodes` 收集脏节点：被拒过的节点和有外来 Pod 的节点归入 `MaybeOverReserved`，属性变化的节点归入 `ConfigChanged`；
3. 对 `MaybeOverReserved` 中的每个节点，GET 最新 NRT，并用 Pod lister 列出该节点上的 Pod；
4. 比较 Pod 指纹，一致才 Flush，否则保持脏状态，下个周期再试。

指纹（podfingerprint，pfp）是代理写进 NRT 属性 `nodeTopologyPodsFingerprint` 的值：对节点上 Pod 的 `namespace/name` 集合做 xxhash 得到的签名，前缀 `pfp0v001`。调度器在本地用 informer 里的 Pod 集合重新计算一遍：

```go
// pkg/noderesourcetopology/cache/store.go
func checkPodFingerprintForNode(lh logr.Logger, objs []podData, nodeName, pfpExpected string, onlyExclRes bool) error {
	st := podfingerprint.MakeStatus(nodeName)
	pfp := podfingerprint.NewTracingFingerprint(len(objs), &st)
	for _, obj := range objs {
		if onlyExclRes && !obj.hasExclusiveResources() {
			continue
		}
		pfp.Add(obj.Namespace, obj.Name)
	}
	pfpComputed := pfp.Sign()
	// ...
	err := pfp.Check(pfpExpected)
	// ...
	return err
}
```

两个指纹相等，说明代理生成这份 NRT 时看到的 Pod 集合，和调度器此刻知道的 Pod 集合一致，NRT 已经「追上」了调度器的认知，这时用它替换快照、清空预留才安全。这个门控就是整个缓存设计的关键：它把「NRT 新不新」这个无法直接判断的问题，转换成了「两边的 Pod 集合是否相同」。

`resyncMethod` 决定指纹的口径：`All` 计入所有 Pod；`OnlyExclusiveResources` 只计入持有独占资源的 Pod（整数核的 Guaranteed CPU、Guaranteed 的内存/hugepages、NRT 中出现的设备）；默认 `Autodetect` 读 NRT 属性 `nodeTopologyPodsFingerprintMethod`，值为 `with-exclusive-resources` 时用后者，否则用前者。两边口径必须一致，否则指纹永远对不上，节点永远无法 Flush。

`FlushNodes` 替换快照后，删除该节点的预留和三个计数器，并把 `generation` 加一。

### 7.5 外来 Pod

调度器只对自己调度的 Pod 做了预留。如果别的调度器（典型场景是 NRT 插件跑在第二调度器里，默认调度器也在往同一批节点放 Pod）把 Pod 放到节点上，缓存的账就错了，而且错的方向是乐观的，这是唯一不能接受的方向。

```go
// pkg/noderesourcetopology/cache/foreign_pods.go
func IsForeignPod(pod *corev1.Pod, nrtResources sets.Set[corev1.ResourceName]) bool {
	if pod.Spec.NodeName == "" {
		// nothing to do yet
		return false
	}
	if schedProfileNames.Has(pod.Spec.SchedulerName) {
		// nothing to do here - we know already about this pod
		return false
	}
	if !onlyExclusiveResources {
		return true
	}
	return resourcerequests.AreExclusiveForPod(pod, nrtResources)
}
```

`schedProfileNames` 是包级全局变量，每个启用了该插件的 Profile 在 `New` 时注册自己的名字。Pod informer 的 Add、Update、Delete 事件中只要出现了外来 Pod，就调用 `NodeHasForeignPods`。被标记的节点在 `GetCachedNRTCopy` 里返回 `nil` 和 `Fresh=false`：Filter 拒绝、Score 记 0，直到重同步通过指纹门控。

默认 `foreignPodsDetect: All` 意味着 DaemonSet 的 Pod（由默认调度器或直接指定 `nodeName` 创建）也算外来 Pod。每次 DaemonSet 滚动，相关节点都会短暂对 NRT 插件「不可用」。如果集群里外来 Pod 只会是 BestEffort/Burstable，可以改成 `OnlyExclusiveResources`，只对持有独占资源的外来 Pod 作出反应。

### 7.6 Pod informer 的两种模式

重同步需要按节点列出 Pod。`podprovider.NewFromHandle` 提供两种来源：

- `Shared`：复用调度器的 Pod informer，加一个 `spec.nodeName` 索引，只保留 `Running` 的 Pod；
- `Dedicated`（默认）：新建一个独立的 Pod informer，保留所有已绑定、非 `Pending` 的 Pod。

区别在于 Pod 集合的口径。代理计算指纹时看到的是 kubelet 已经知道的 Pod，调度器侧需要尽量用同样的集合。调度器自带的 informer 会过滤掉某些终态 Pod，独立 informer 多一条到 API Server 的 watch 连接，但口径更可控（相关讨论见代码注释引用的 scheduler-plugins#599）。

### 7.7 NRT watcher 与一个空缺

`resyncScope` 为 `All`（代码默认）时，`NewOverReserve` 会启动 `Watcher` 监听 NRT 对象，只关心 Topology Manager 配置的变化：对比新旧对象的 `nodeconfig.TopologyManager`，不同就发出 `WatchReasonAttrChanged`，重同步时这些节点走 `nullGate`，不比指纹直接 Flush。这样节点改了 kubelet 配置后，调度器不需要重启就能用上新策略。通道满了的事件会暂存在 `pending` 中，每 2 秒重试一次，同一节点只保留最新一条。

watcher 对从未见过的 NRT 会发出 `WatchReasonNewlyAdded`，但 `processNRTEvent` 只处理 `WatchReasonAttrChanged`，其余原因只记一条 `unsupported NRT event` 日志：

```go
// pkg/noderesourcetopology/cache/overreserve.go
func (ov *OverReserve) processNRTEvent(nrtEv NRTEvent, lh logr.Logger) int {
	switch nrtEv.Reason {
	case WatchReasonAttrChanged:
		ov.nodesWithAttrUpdate.Incr(nrtEv.NodeName)
		return 1
	}
	lh.V(2).Info("unsupported NRT event", "reason", nrtEv.Reason.String(), logging.KeyNode, nrtEv.NodeName)
	return 0
}
```

结合前面的代码可以推出一个后果：调度器启动之后才出现 NRT 的节点（扩容的新节点），不在快照里，`GetCachedNRTCopy` 返回 `nil`，Filter 放行；它被拒绝的机会不存在，自然也不会被标脏，`ReserveNodeResources` 和 `NodeHasForeignPods` 都会因为 `!ov.nrts.Contains(nodeName)` 直接返回。在 OverReserve 模式下，这类节点对 NUMA 对齐不设防，直到调度器重启重新 List。弹性伸缩频繁的集群需要留意这一点。

## 八、抢占模拟

`preemptionMode: Enabled` 时，Filter 中间多了一段：

```go
// pkg/noderesourcetopology/filter.go
	victims, err := getVictimPods(cycleState, tm.preemptionMode)
	// ...
	isPreemptionFlow := len(victims) > 0
	if isPreemptionFlow {
		// ...
		numaPlacementInfo := tm.nrtCache.GetCachedNUMAPlacementInfo(nodeName)
		if numaPlacementInfo != nil && numaPlacementInfo.Containers() != 0 {
			nodeTopology, err = preemption.GetNRTPostPodsEviction(lh, nodeTopology.DeepCopy(), victims, numaPlacementInfo)
			if err != nil {
				// valid NUMA placement info aligns with having a fresh PFP hence use OverReserved signal to resync with the PFP gate
				tm.nrtCache.NodeMaybeOverReserved(nodeName, pod)
				return fwk.NewStatus(fwk.Unschedulable, "eviction simulation in NRT is not possible: "+err.Error())
			}
		}
	}
```

要把受害者的资源「还」给 NRT，必须知道每个受害者容器被 kubelet 放在哪个 zone。这个信息来自代理在 NRT 中额外编码的 NUMA 放置数据（`numaplacement` 库：顶层属性存元数据，每个 zone 的属性存一个向量），缓存在 Flush 时保存节点的 Pod 列表，首次查询时惰性解码出「容器 → NUMA ID」映射。`GetNRTPostPodsEviction` 只归还受害者中独占资源的部分（跳过普通 init 容器，保留 sidecar），按 NUMA 累加回 `Available`，任何一项超过 `Allocatable` 就整体放弃。

这套机制有三个前提：只有 OverReserve 实现了 `GetCachedNUMAPlacementInfo`，Passthrough 和 DiscardReserved 返回 nil；代理必须上报放置数据；只解码 `single-numa-node` 策略的节点。任何一个不满足，Filter 就退化为不考虑受害者的普通判定。抢占关闭时更简单：`RemovePod` 什么都不做，模拟驱逐后 NRT 不变，被 NRT 拒绝的节点永远不会成为抢占候选。

## 九、Reserve、PostBind 与重新入队

Reserve、Unreserve、PostBind 都只是把调用转交给缓存：

```go
// pkg/noderesourcetopology/reserve.go
func (tm *TopologyMatch) Reserve(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) *fwk.Status {
	// ...
	tm.nrtCache.ReserveNodeResources(nodeName, pod)
	// can't fail
	return fwk.NewStatus(fwk.Success, "")
}
```

OverReserve 的 `PostBind` 是空实现，预留一直保留到 Flush；DiscardReserved 在 PostBind 时删除预留；Passthrough 全部为空。

被这个插件拒绝的 Pod 靠下面三类事件重新入队：

```go
// pkg/noderesourcetopology/plugin.go
func (tm *TopologyMatch) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	// ...
	nrtGVK := fmt.Sprintf("noderesourcetopologies.v1alpha2.%v", topologyapi.GroupName)
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Delete}},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeAllocatable}},
		{Event: fwk.ClusterEvent{Resource: fwk.EventResource(nrtGVK), ActionType: fwk.Add | fwk.Update}},
	}, nil
}
```

`noderesourcetopologies.v1alpha2.topology.node.k8s.io` 这种「资源.版本.组」格式的名字会让 kube-scheduler 用 dynamic informer 监听这个 CRD（`eventhandlers.go` 里点号数量不少于 2 的 GVK 走 `dynInformerFactory`）。三个事件都没有提供 QueueingHint 函数，框架使用 `defaultQueueingHintFn`，一律返回 `Queue`。

实际效果：代理通常周期性地更新 NRT，每一次 Update 都会把所有被 NRT 插件拒绝的 Pod 移回活跃队列，不管更新的是哪个节点、资源有没有变化。在 OverReserve 模式下，这些重试多半是无效的，因为快照要等重同步通过指纹门控才会变；一个被拒的 Pod 真正能被放下，往往要等到下一个 Resync 周期之后的某次 NRT 更新。

## 十、生产中的坑

前面散落的问题集中在这里，按发生频率排列：

1. **NRT 没有策略属性**。代理没有上报 `topologyManagerPolicy`，插件按 `none` 处理，Filter 全部放行。部署后先 `kubectl get noderesourcetopologies -o yaml` 检查 `attributes`。
2. **zone 名不是 `node-N`**。zone 被跳过，CPU、内存的位图为空，Guaranteed Pod 在这些节点上一律被拒。
3. **默认 Passthrough**。既有上报延迟导致的批量误判，又有每节点两次 API GET 的开销。生产环境应开启 OverReserve，`cacheResyncPeriodSeconds` README 建议不低于 5 秒，同时确认 Reserve 扩展点已启用。
4. **只覆盖 single-numa-node**。`restricted` 策略同样会拒绝准入，但 Filter 不检查它；这类节点只能靠 `LeastNUMANodes` 打分做软约束。
5. **Guaranteed Pod 的小数 CPU**。kubelet 的 static CPU Manager 只对整数核做独占分配，`1500m` 的 Guaranteed 容器不会产生 CPU 对齐约束；插件的 `isResourceSetSuitable` 只看 QoS，仍要求单个 zone 有 1.5 核余量，会产生假阴性。
6. **外来 Pod 让节点暂时消失**。和默认调度器共用节点时，默认 `foreignPodsDetect: All` 会让每个外来 Pod 都把节点锁住到下一次成功重同步。
7. **指纹口径不一致**。`resyncMethod` 与代理的 `nodeTopologyPodsFingerprintMethod` 不一致、或 informer 模式与代理的 Pod 集合口径不一致，指纹永远不匹配，节点被标脏后就再也出不来，表现为一批节点长期被拒。日志里搜 `podset fingerprint mismatch`。
8. **新节点不受保护**（OverReserve 模式）。见 7.7。
9. **打分与 kubelet 的 zone 选择脱节**。见 6.2。

## 十一、发散

**为什么不直接 import kubelet 的准入代码？** 缓存设计文档讨论过：即使运行完全相同的代码，调度器和 kubelet 也是一个分布式系统，调度器看到的状态永远可能落后，「预测」kubelet 的行为在原理上做不到百分之百。插件选择的是「模拟 + 保守纠偏」：Filter 只模拟最严格也最常用的 `single-numa-node`，缓存用悲观扣减保证只会误拒、不会误放。这与 in-tree 调度器的 assume 机制精神一致，只是粒度做不到 zone 级的精确。

**和 DRA 的关系**。Dynamic Resource Allocation（见 [DynamicResources](/posts/sched-plugin-15-dynamic-resources/)）让设备驱动以 `ResourceSlice` 的形式发布设备及其属性，调度器在调度阶段就确定分配哪个设备，kubelet 只执行，不再「事后裁决」。如果 CPU、内存也最终通过 DRA 或类似机制表达 NUMA 拓扑，调度器就能成为分配的决定者，NRT 这类「上报 + 模拟」的模式会失去存在的必要。在那之前，NRT 插件仍是用 Topology Manager 跑延迟敏感负载时最直接的选择。

**和 NodeResourcesFit 的分工**。NRT 插件不检查节点总量，它假设 `NodeResourcesFit` 已经过滤了总量不足的节点。两者都会运行，Filter 链上 NRT 插件通常排在 in-tree 插件之后。打分上两者也会叠加：in-tree 的 `NodeResourcesFit` 默认 `LeastAllocated` 会偏好总量空闲的节点，NRT 的 `LeastAllocated` 偏好最差 zone 也空闲的节点，方向基本一致。

**一个 Profile 一个插件实例**。`schedProfileNames` 是全局变量，多 Profile 场景下每个 Profile 都会创建自己的缓存实例和独立 Pod informer（`Dedicated` 模式），各自持有一份全量 NRT 快照、各自运行一个 Resync goroutine。两个 Profile 之间互不认为对方是外来调度器，但也互不知道对方的预留。

## 小结

- NRT 插件读取节点代理上报的 `NodeResourceTopology`，在 Filter 中用 kubelet 的 `bitmask` 预演 `single-numa-node` 准入：逐资源求可行 zone 位图再取交集，container 作用域逐容器扣减，pod 作用域整体判定；拒绝时返回 `Unschedulable`，可以触发抢占。
- 没有 NRT、策略不是 `single-numa-node`、BestEffort Pod，都直接放行；策略和作用域必须由代理通过 `attributes` 上报，zone 名必须是 `node-N`。
- Score 只对 Guaranteed Pod 生效；前三种策略每个 zone 单独打分、节点取非零最小值，分母是 `Available`，与 kubelet 的实际 zone 选择并不对应；`LeastNUMANodes` 偏好跨越最少 NUMA 的节点，适用于所有策略。
- NRT 数据天然落后。默认的 Passthrough 每次直接 GET 且不纠偏；OverReserve 在所有 zone 上悲观扣减预留，由 Pod 指纹门控决定何时用新 NRT 替换快照，外来 Pod 会让节点暂时不可用。
- 抢占模拟依赖 OverReserve 和代理上报的 NUMA 放置数据，默认关闭。

下一篇 [PreemptionToleration](/posts/sched-plugin-31-preemption-toleration/) 进入 PostFilter：同样是抢占，它让受害者可以声明「在多长时间内不愿被抢占」。
