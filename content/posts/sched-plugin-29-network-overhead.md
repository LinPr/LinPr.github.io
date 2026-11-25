---
title: 'scheduler-plugins 29 [out-of-tree] NetworkOverhead：按依赖的网络代价过滤与打分'
date: 2026-11-19T12:50:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "network-aware", "score"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "NetworkOverhead 读取 AppGroup 里的服务依赖和 NetworkTopology 里的 region/zone 代价矩阵，在 PreFilter 中为每个节点预先算出依赖的满足/违反数和累计网络代价，Filter 按多数票过滤，Score 把代价反转成分数。本文拆两个 CRD 的结构、weightsName 与代价表的查找、逐对判定规则，并用一个两级拓扑的算例算出每个节点的计数与得分。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `NetworkOverhead` 实现的扩展点（PreFilter、Filter、Score、NormalizeScore）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/29.svg" link="/images/posts/sched-plugins/callstack/29.svg" caption="NetworkOverhead 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-28-capacity-scheduling/)的 CapacityScheduling 在 PreFilter 和 PostFilter 上做多租户配额。NetworkOverhead 同样从 PreFilter 开始介入，但关心的是另一类约束：一个微服务的 Pod 放在哪里，它和它所调用的服务之间的网络代价最小。

它实现 PreFilter（带空的 `PreFilterExtensions`）、Filter、Score 和 NormalizeScore，不在默认 Profile 里，需要通过 scheduler-plugins 编译出的调度器启用，常与[第 26 篇](/posts/sched-plugin-26-topological-sort/)的 TopologicalSort 一起配置。`pkg/networkaware/README.md` 把它的成熟度标为 Sample。

本文基于 scheduler-plugins `cfeb3aa`（依赖 Kubernetes v1.36.4）；两个 CRD 的类型来自依赖模块 `github.com/diktyo-io/appgroup-api v1.0.1-alpha` 和 `github.com/diktyo-io/networktopology-api v1.0.1-alpha`。

## 一、它解决什么问题

默认插件里和「位置」有关的只有 [InterPodAffinity](/posts/sched-plugin-14-inter-pod-affinity/)：可以写「和 app=cart 的 Pod 放在同一个 zone」，但它只有拓扑域相同与否两种状态，表达不了「跨 zone 可以接受，跨 region 不行」，更表达不了「us-west-1 到 us-east-1 的延迟是 z1 到 z2 的四倍」。

NetworkOverhead 把这两件事拆成两份数据：

1. **应用的依赖图**写在 AppGroup 里：frontend 调用 cart，最多容忍代价 8；frontend 调用 catalog，最多容忍代价 15。
2. **基础设施的网络代价**写在 NetworkTopology 里：region 之间多少、同一 region 内 zone 之间多少。

调度一个 Pod 时，插件找出它所依赖的 workload 已经有哪些 Pod 绑定到了节点上，对每个候选节点估算「如果放在这里，到这些依赖 Pod 的网络代价」，据此过滤和打分。

KEP 给出的典型配置把 NetworkOverhead 的权重设为 5，并启用 TopologicalSort 做队列排序：

```yaml {linenos=inline}
# manifests/networktopology/scheduler-config.yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: network-aware-scheduler
  plugins:
    multiPoint:
      enabled:
      - name: NetworkOverhead
        weight: 5 # A higher weight is given to NetworkOverhead to favor allocation schemes with lower latency.
      disabled:
      - name: NodeResourcesFit # Preferably avoid the combination of NodeResourcesFit with NetworkOverhead
    queueSort:
      enabled:
      - name: TopologicalSort
      disabled:
      - name: "*"
  pluginConfig:
  - name: TopologicalSort
    args:
      namespaces:
      - "default"
  - name: NetworkOverhead
    args:
      namespaces:
      - "default"
      weightsName: "UserDefined" # or Dijkstra
      networkTopologyName: "net-topology-test"
```

这份示例在 multiPoint 里禁用了 NodeResourcesFit。要注意这不仅关掉了 NodeResourcesFit 的打分，也关掉了它的 PreFilter/Filter，调度器从此不再检查节点的 CPU、内存是否够用，Pod 可能被放到资源不足的节点上、被 kubelet 以 `OutOfcpu`/`OutOfmemory` 拒绝。

示例的注释只说「尽量避免与 NodeResourcesFit 组合」，没有给出原因；如果担心的是两者的打分互相抵消，更稳妥的做法是只调整 NodeResourcesFit 的 Score 权重，而不是整体禁用。

## 二、两个 CRD

插件要用到的数据分成两半：应用的依赖图写在 AppGroup 里，基础设施的代价写在 NetworkTopology 里。

### 2.1 AppGroup：依赖与 maxNetworkCost

AppGroup 的结构在第 26 篇已经拆过，NetworkOverhead 用到的是 `spec.workloads[*].dependencies`：

```go {linenos=inline}
// github.com/diktyo-io/appgroup-api@v1.0.1-alpha/pkg/apis/appgroup/v1alpha1/types.go
type DependenciesInfo struct {
	// Workload reference Info.
	// +required
	Workload AppGroupWorkloadInfo `json:"workload" protobuf:"bytes,1,opt,name=workload, casttype=AppGroupWorkloadInfo"`

	// MinBandwidth between workloads
	// +optional
	MinBandwidth resource.Quantity `json:"minBandwidth,omitempty" protobuf:"bytes,2,opt,name=minBandwidth"`

	// Max Network Cost between workloads
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Default=0
	// +kubebuilder:validation:Maximum=10000
	MaxNetworkCost int64 `json:"maxNetworkCost,omitempty" protobuf:"bytes,3,opt,name=maxNetworkCost"`
}
```

`MinBandwidth` 在插件里没有被读取，KEP 把带宽约束留给了扩展资源（由 DaemonSet 把带宽注册成节点的扩展资源，交给 NodeResourcesFit 处理）。插件只用 `Workload.Selector` 和 `MaxNetworkCost`。

`maxNetworkCost` 不写时是 0，意味着跨 zone、跨 region 的任何已知代价都算违反。

依赖列表的提取方式：

```go {linenos=inline}
// pkg/networkaware/util/util.go
// GetDependencyList : get workload dependencies established in the AppGroup CR
func GetDependencyList(pod *v1.Pod, ag *agv1alpha1.AppGroup) []agv1alpha1.DependenciesInfo {

	// Check Dependencies of the given pod
	var dependencyList []agv1alpha1.DependenciesInfo

	// Get Labels of the given pod
	podLabels := pod.GetLabels()

	for _, w := range ag.Spec.Workloads {
		if w.Workload.Selector == podLabels[agv1alpha1.AppGroupSelectorLabel] {
			for _, dependency := range w.Dependencies {
				dependencyList = append(dependencyList, dependency)
			}
		}
	}

	// Return the dependencyList
	return dependencyList
}
```

只看当前 Pod 自己这个 workload 的出边。被别人依赖的一方（例如 redis）没有出边，NetworkOverhead 对它不起作用。

### 2.2 NetworkTopology：四层嵌套的代价表

依赖图只说了「谁调用谁、最多容忍多少代价」，代价本身要另外一张表提供。

```go {linenos=inline}
// github.com/diktyo-io/networktopology-api@v1.0.1-alpha/pkg/apis/networktopology/v1alpha1/types.go
type NetworkTopologySpec struct {
	// The manual defined weights of the cluster
	// +required
	Weights WeightList `json:"weights" protobuf:"bytes,1,opt,name=weights,casttype=WeightList"`

	// ConfigmapName to be used for cost calculation
	// +required
	ConfigmapName string `json:"configmapName" protobuf:"bytes,2,opt,name=configmapName"`
}

// WeightInfo contains information about all network costs for a given algorithm.
type WeightInfo struct {
	// Algorithm Name for network cost calculation (e.g., userDefined)
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// TopologyList owns Costs between origins
	// +required
	TopologyList TopologyList `json:"topologyList" protobuf:"bytes,2,opt,name=topologyList,casttype=TopologyList"`
}

// TopologyInfo contains information about network costs for a particular Topology Key.
type TopologyInfo struct {
	// Topology key (e.g., "topology.kubernetes.io/region", "topology.kubernetes.io/zone").
	// +required
	TopologyKey TopologyKey `json:"topologyKey" protobuf:"bytes,1,opt,name=topologyKey"` // add as enum instead of string

	// OriginList for a particular origin.
	// +required
	OriginList OriginList `json:"originList" protobuf:"bytes,2,rep,name=originList,casttype=OriginList"`
}
```

再往下是 `OriginInfo{Origin, CostList}` 和 `CostInfo{Destination, BandwidthCapacity, BandwidthAllocated, NetworkCost}`。层级是：

```text {linenos=inline}
weights[]            按 name 区分代价来源（UserDefined、NetperfCosts……）
└─ topologyList[]    按 topologyKey 区分层级（region、zone）
   └─ originList[]   起点（某个 region 名或 zone 名）
      └─ costList[]  终点与 networkCost
```

`topologyKey` 只认两个值，分别等于节点的标准 label：

```go {linenos=inline}
// github.com/diktyo-io/networktopology-api@v1.0.1-alpha/pkg/apis/networktopology/v1alpha1/types.go
const (
	// NetworkTopologyRegion corresponds to "topology.kubernetes.io/region"
	NetworkTopologyRegion TopologyKey = v1.LabelTopologyRegion

	// NetworkTopologyRegion corresponds to "topology.kubernetes.io/zone"
	NetworkTopologyZone TopologyKey = v1.LabelTopologyZone

	// NetworkTopologyNetperfCosts corresponds to costs defined with measurements via the Netperf Component: "NetperfCosts"
	NetworkTopologyNetperfCosts string = "NetperfCosts"
)
```

代价是有向的：z1→z2 和 z2→z1 是两条记录，要分别写。也没有节点级别的条目：KEP 的假设是同一 zone 内节点之间的网络是均匀的，插件对同 zone 一律记 1，不查表。

`weights` 可以并存多套，插件参数 `weightsName` 选用其中一套。按名字分两类：

1. 名字为 `NetperfCosts` 的那套，约定由 networktopology controller 根据 netperf 测量结果（存放在 `configmapName` 指向的 ConfigMap 里）自动维护；
2. 其它名字（默认 `UserDefined`）视为手工填写。

这个区别会影响插件是否对代价表排序，见 3.3 节。

仓库里的示例 `manifests/networktopology/networkTopology-example.yaml` 用的字段名是 `configMapName`、`costList`、`originCosts`、`costs`，与 CRD 的 `configmapName`、`topologyList`、`originList`、`costList` 对不上。

CRD 把 `configmapName`、`weights[].topologyList` 列为必填，照着这份示例 apply 会被 API Server 拒绝；依赖模块里 `networktopology-api/manifests/example.yaml` 的字段名才是正确的。

## 三、源码

两个 CRD 的结构清楚了，下面看插件怎么把它们用起来。

### 3.1 结构体、参数与默认值

先看插件持有的字段和它固定的几个常量。

```go {linenos=inline}
// pkg/networkaware/networkoverhead/networkoverhead.go
const (
	// Name : name of plugin used in the plugin registry and configurations.
	Name = "NetworkOverhead"

	// MaxCost : MaxCost used in the NetworkTopology for costs between origins and destinations
	MaxCost = 100

	// SameHostname : If pods belong to the same host, then consider cost as 0
	SameHostname = 0

	// SameZone : If pods belong to hosts in the same zone, then consider cost as 1
	SameZone = 1

	// preFilterStateKey is the key in CycleState to NetworkOverhead pre-computed data.
	preFilterStateKey = "PreFilter" + Name
)

// NetworkOverhead : Filter and Score nodes based on Pod's AppGroup requirements: MaxNetworkCosts requirements among Pods with dependencies
type NetworkOverhead struct {
	client.Client
	logger      klog.Logger
	podLister   corelisters.PodLister
	handle      fwk.Handle
	namespaces  []string
	weightsName string
	ntName      string
}
```

三个常量（第 7、10、13 行）定义了代价模型里的固定值：同节点 0、同 zone 1、未知或无法计算时 100。

参数与默认值：

```go {linenos=inline}
// apis/config/v1/types.go
type NetworkOverheadArgs struct {
	metav1.TypeMeta `json:",inline"`

	// Namespaces to be considered by NetworkMinCost plugin
	Namespaces []string `json:"namespaces,omitempty"`

	// Preferred weights (Default: UserDefined)
	WeightsName *string `json:"weightsName,omitempty"`

	// The NetworkTopology CRD name
	NetworkTopologyName *string `json:"networkTopologyName,omitempty"`
}
```

```go {linenos=inline}
// apis/config/v1/defaults.go
func SetDefaults_NetworkOverheadArgs(obj *NetworkOverheadArgs) {
	if len(obj.Namespaces) == 0 {
		obj.Namespaces = []string{metav1.NamespaceDefault}
	}

	if obj.WeightsName == nil {
		obj.WeightsName = &DefaultWeightsName
	}

	if obj.NetworkTopologyName == nil {
		obj.NetworkTopologyName = &DefaultNetworkTopologyName
	}
}
```

`DefaultWeightsName` 是 `"UserDefined"`，`DefaultNetworkTopologyName` 是 `"nt-default"`。注意默认的 NetworkTopology 名字和示例 CR 的 `net-topology-test` 不同，不显式配置 `networkTopologyName` 就找不到示例里的那个对象。三个参数都没有校验逻辑。

`New` 里除了 Pod lister 取自调度器的 `SharedInformerFactory`，AppGroup 和 NetworkTopology 都通过 `util.NewClientWithCachedReader` 构造的 controller-runtime 缓存客户端读取，机制与 TopologicalSort 相同：informer 按类型懒创建，`Get` 返回深拷贝。

### 3.2 CycleState：PreFilter 把所有活都做完

这个插件和前面几个的最大区别，是它把全部计算压在 PreFilter 里，Filter 和 Score 只查表。CycleState 里的结构就是为此设计的。

```go {linenos=inline}
// pkg/networkaware/networkoverhead/networkoverhead.go
// PreFilterState computed at PreFilter and used at Filter and Score.
type PreFilterState struct {
	// boolean that tells the filter and scoring functions to pass the pod since it does not belong to an AppGroup
	scoreEqually bool

	// AppGroup name of the pod
	agName string

	// AppGroup CR
	appGroup *agv1alpha1.AppGroup

	// NetworkTopology CR
	networkTopology *ntv1alpha1.NetworkTopology

	// Dependency List of the given pod
	dependencyList []agv1alpha1.DependenciesInfo

	// Pods already scheduled based on the dependency list
	scheduledList networkawareutil.ScheduledList

	// node map for cost / destinations. Search for requirements faster...
	nodeCostMap map[string]map[networkawareutil.CostKey]int64

	// node map for satisfied dependencies
	satisfiedMap map[string]int64

	// node map for violated dependencies
	violatedMap map[string]int64

	// node map for costs
	finalCostMap map[string]int64
}

// Clone the preFilter state.
func (no *PreFilterState) Clone() fwk.StateData {
	return no
}
```

Filter 和 Score 只是查表：`satisfiedMap`、`violatedMap`、`finalCostMap` 三张以节点名为 key 的 map（第 25–32 行）在 PreFilter 里一次算好。

这些字段按作用分成四组：

1. 开关 `scoreEqually`（第 4–5 行）：Pod 不属于 AppGroup，或者后面任何一条提前返回的条件成立时，它保持初始值 true，Filter 直接放行、Score 给最低分；
2. 输入：Pod 的 AppGroup 名（第 7–8 行）、读到的两个 CRD 对象（第 10–14 行）、当前 Pod 的依赖列表（第 16–17 行）、已经绑定节点的依赖 Pod（第 19–20 行）；
3. 中间结果 `nodeCostMap`（第 22–23 行）：每个候选节点自己的一张代价表，逐对判定时按 `CostKey` 查它；
4. 输出：满足数（第 25–26 行）、违反数（第 28–29 行）、累计代价（第 31–32 行）。

`Clone` 直接返回自身指针（第 35–38 行），没有深拷贝。这在 Filter/Score 只读的前提下是安全的，但也意味着抢占模拟里复制出来的 CycleState 与原状态共享同一份数据；配合下面 AddPod/RemovePod 什么也不做，抢占模拟对这个插件的结果没有任何影响。

### 3.3 PreFilter

PreFilter 的结构是「一串提前返回 + 一段节点循环」。

```go {linenos=inline}
// pkg/networkaware/networkoverhead/networkoverhead.go
func (no *NetworkOverhead) PreFilter(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// Init PreFilter State
	preFilterState := &PreFilterState{
		scoreEqually: true,
	}
	// ...
	// Write initial status
	state.Write(preFilterStateKey, preFilterState)

	// Check if Pod belongs to an AppGroup
	agName := networkawareutil.GetPodAppGroupLabel(pod)
	if len(agName) == 0 { // Return
		return nil, fwk.NewStatus(fwk.Success, "Pod does not belong to an AppGroup, return")
	}

	// Get AppGroup CR
	appGroup := no.findAppGroupNetworkOverhead(ctx, agName)

	// Get NetworkTopology CR
	networkTopology := no.findNetworkTopologyNetworkOverhead(ctx)

	// Sort Costs if manual weights were selected
	no.sortNetworkTopologyCosts(networkTopology)

	// Get Dependencies of the given pod
	dependencyList := networkawareutil.GetDependencyList(pod, appGroup)

	// If the pod has no dependencies, return
	if dependencyList == nil {
		return nil, fwk.NewStatus(fwk.Success, "Pod has no dependencies, return")
	}

	// Get pods from lister
	selector := labels.Set(map[string]string{agv1alpha1.AppGroupLabel: agName}).AsSelector()
	pods, err := no.podLister.List(selector)
	if err != nil {
		return nil, fwk.NewStatus(fwk.Success, "Error while returning pods from appGroup, return")
	}

	// Return if pods are not yet allocated for the AppGroup...
	if len(pods) == 0 {
		return nil, fwk.NewStatus(fwk.Success, "No pods yet allocated, return")
	}

	// Pods already scheduled: Get Scheduled List (Deployment name, replicaID, hostname)
	scheduledList := networkawareutil.GetScheduledList(pods)
	// Check if scheduledList is empty...
	if len(scheduledList) == 0 {
		logger.Error(nil, "Scheduled list is empty, return")
		return nil, fwk.NewStatus(fwk.Success, "Scheduled list is empty, return")
	}

	// Get all nodes
	nodeList, err := no.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, fwk.NewStatus(fwk.Error, fmt.Sprintf("Error getting the nodelist: %v", err))
	}
	// ...
```

前半段是一串提前返回，每一种都以 `Success` 结束，状态里保留开头写入的 `scoreEqually: true`：

| 条件 | 结果 |
| --- | --- |
| Pod 没有 `appgroup.diktyo.x-k8s.io` label | 不参与 |
| 当前 workload 在 AppGroup 里没有依赖（或 workload label 对不上任何 workload） | 不参与 |
| 列出 Pod 出错 | 不参与（注意是 `Success`，不是 `Error`） |
| AppGroup 名下一个 Pod 都没有 / 都还没绑定节点 | 不参与 |

四条分支分别对应上文代码块的第 12–15 行、第 30–32 行、第 37–39 行、第 42–44 行。

`scheduledList` 来自 Pod lister，取的是带 `appgroup.diktyo.x-k8s.io=<agName>` label、`spec.nodeName` 非空的 Pod（第 7 行的判断）：

```go {linenos=inline}
// pkg/networkaware/util/util.go
func GetScheduledList(pods []*v1.Pod) ScheduledList {
	// scheduledList: Deployment name, replicaID, hostname
	scheduledList := ScheduledList{}

	for _, p := range pods {
		if len(p.Spec.NodeName) != 0 {
			scheduledInfo := ScheduledInfo{
				Name:      p.Name,
				Selector:  GetPodAppGroupSelector(p),
				ReplicaID: string(p.GetUID()),
				Hostname:  p.Spec.NodeName,
			}
			scheduledList = append(scheduledList, scheduledInfo)
		}
	}
	// Return the scheduledList
	return scheduledList
}
```

有三处细节：

1. lister 不按 namespace 过滤，任何 namespace 里带同名 AppGroup label 的 Pod 都会被算进来；
2. 它不看 Pod phase，已经 `Succeeded`/`Failed` 但对象还在的 Pod 照样参与计算；
3. 它读的是 informer，即 API Server 已经确认绑定的 Pod，调度器缓存里已 assume、尚未完成绑定的 Pod 不在其中。

后半段对快照里的**每一个节点**（而不是 PreFilter 参数 `nodes`）构造代价表、计算满足/违反数和累计代价：

```go {linenos=inline}
// pkg/networkaware/networkoverhead/networkoverhead.go
	for _, nodeInfo := range nodeList {
		// retrieve region and zone labels
		region := networkawareutil.GetNodeRegion(nodeInfo.Node())
		zone := networkawareutil.GetNodeZone(nodeInfo.Node())
		// ...
		// Create map for cost / destinations. Search for requirements faster...
		costMap := make(map[networkawareutil.CostKey]int64)

		// Populate cost map for the given node
		no.populateCostMap(costMap, networkTopology, region, zone)
		// ...
		// Update nodeCostMap
		nodeCostMap[nodeInfo.Node().Name] = costMap

		// Get Satisfied and Violated number of dependencies
		satisfied, violated, ok := checkMaxNetworkCostRequirements(logger, scheduledList, dependencyList, nodeInfo, region, zone, costMap, no)
		if ok != nil {
			return nil, fwk.NewStatus(fwk.Error, fmt.Sprintf("pod hostname not found: %v", ok))
		}

		// Update Satisfied and Violated maps
		satisfiedMap[nodeInfo.Node().Name] = satisfied
		violatedMap[nodeInfo.Node().Name] = violated
		// ...
		// Get accumulated cost based on pod dependencies
		cost, ok := no.getAccumulatedCost(scheduledList, dependencyList, nodeInfo.Node().Name, region, zone, costMap)
		if ok != nil {
			return nil, fwk.NewStatus(fwk.Error, fmt.Sprintf("getting pod hostname from Snapshot: %v", ok))
		}
		// ...
		finalCostMap[nodeInfo.Node().Name] = cost
	}
```

`PreFilterResult` 始终为 nil，不缩小节点集合。

**代价表的构造。** 每个节点的 `costMap` 只装与它自己相关的条目，key 是 `CostKey{Origin, Destination}`：

```go {linenos=inline}
// pkg/networkaware/networkoverhead/networkoverhead.go
func (no *NetworkOverhead) populateCostMap(
	costMap map[networkawareutil.CostKey]int64,
	networkTopology *ntv1alpha1.NetworkTopology,
	region string,
	zone string) {
	for _, w := range networkTopology.Spec.Weights { // Check the weights List
		if w.Name != no.weightsName { // If it is not the Preferred algorithm, continue
			continue
		}

		if region != "" { // Add Region Costs
			// Binary search through CostList: find the Topology Key for region
			topologyList := networkawareutil.FindTopologyKey(w.TopologyList, ntv1alpha1.NetworkTopologyRegion)

			if no.weightsName != ntv1alpha1.NetworkTopologyNetperfCosts {
				// Sort Costs by origin, might not be sorted since were manually defined
				sort.Sort(networkawareutil.ByOrigin(topologyList))
			}

			// Binary search through TopologyList: find the costs for the given Region
			costs := networkawareutil.FindOriginCosts(topologyList, region)

			// Add Region Costs
			for _, c := range costs {
				costMap[networkawareutil.CostKey{ // Add the cost to the map
					Origin:      region,
					Destination: c.Destination}] = c.NetworkCost
			}
		}
		if zone != "" { // Add Zone Costs
			// ... 同上，topologyKey 换成 zone，origin 换成本节点的 zone
		}
	}
}
```

查找全部是二分：

1. `FindTopologyKey` 在 `topologyList` 上按 key 二分（第 14 行）；
2. `FindOriginCosts` 在 `originList` 上按 origin 二分（第 22 行）。

二分要求有序，手工填写的代价表不一定有序，所以按 `weightsName` 分两种情况：

1. 不是 `NetperfCosts` 时，PreFilter 开头的 `sortNetworkTopologyCosts` 先按 `topologyKey` 排一次，`populateCostMap` 再对 `originList` 按 origin 排序；
2. `NetperfCosts` 则信任 controller 已经排好。

由于 `Get` 返回的是深拷贝，这些排序不会改动缓存里的对象，但会在每次 PreFilter、对每个节点重复一遍：`originList` 的排序发生在节点循环内部，N 个节点就排 N 次。

`weightsName` 在 `weights` 里找不到同名条目时，所有节点的 `costMap` 都是空的。插件不会报错，只是所有跨 zone、跨 region 的查找都落到「查不到」分支，见下一节。

### 3.4 逐对判定：满足、违反，还是不计

`checkMaxNetworkCostRequirements` 和 `getAccumulatedCost` 的外层都是同一个双重循环：遍历 `scheduledList` 里的每个已绑定 Pod，再遍历当前 Pod 的每个依赖，selector 对上的才处理。判定树如下（同一节点上的一对直接算满足，第 11–14 行）：

```go {linenos=inline}
// pkg/networkaware/networkoverhead/networkoverhead.go（checkMaxNetworkCostRequirements）
	for _, podAllocated := range scheduledList { // For each pod already allocated
		if podAllocated.Hostname != "" { // if hostname not empty...
			for _, d := range dependencyList { // For each pod dependency
				// If the pod allocated is not an established dependency, continue.
				if podAllocated.Selector != d.Workload.Selector {
					continue
				}

				// If the Pod hostname is the node being filtered, requirements are checked via extended resources
				if podAllocated.Hostname == nodeInfo.Node().Name {
					satisfied += 1
					continue
				}

				// If Nodes are not the same, get NodeInfo from pod Hostname
				podNodeInfo, err := no.handle.SnapshotSharedLister().NodeInfos().Get(podAllocated.Hostname)
				if err != nil {
					logger.Error(err, "getting pod's NodeInfo from snapshot", "nodeInfo", podNodeInfo)
					return satisfied, violated, err
				}

				// Get zone and region from Pod Hostname
				regionPodNodeInfo := networkawareutil.GetNodeRegion(podNodeInfo.Node())
				zonePodNodeInfo := networkawareutil.GetNodeZone(podNodeInfo.Node())

				if regionPodNodeInfo == "" && zonePodNodeInfo == "" { // Node has no zone and region defined
					violated += 1
				} else if region == regionPodNodeInfo { // If Nodes belong to the same region
					if zone == zonePodNodeInfo { // If Nodes belong to the same zone
						satisfied += 1
					} else { // belong to a different zone, check maxNetworkCost
						cost, costOK := costMap[networkawareutil.CostKey{ // Retrieve the cost from the map (origin: zone, destination: pod zoneHostname)
							Origin:      zone, // Time Complexity: O(1)
							Destination: zonePodNodeInfo,
						}]
						if costOK {
							if cost <= d.MaxNetworkCost {
								satisfied += 1
							} else {
								violated += 1
							}
						}
					}
				} else { // belong to a different region
					cost, costOK := costMap[networkawareutil.CostKey{ // Retrieve the cost from the map (origin: zone, destination: pod zoneHostname)
						Origin:      region, // Time Complexity: O(1)
						Destination: regionPodNodeInfo,
					}]
					if costOK {
						if cost <= d.MaxNetworkCost {
							satisfied += 1
						} else {
							violated += 1
						}
					}
				}
			}
		}
	}
```

`getAccumulatedCost` 的结构完全相同，只是把「满足/违反」换成加代价：同节点加 `SameHostname`（0），对方节点没有 region 和 zone 加 `MaxCost`（100），同 zone 加 `SameZone`（1），查到代价加查到的值，查不到加 `MaxCost`。

{{< figure src="/images/posts/sched-plugins/29-pair-decision.svg" caption="一对（已调度 Pod，依赖）的判定树：Filter 计数与 Score 代价走同一套分支，只在「代价表里查不到」时分道——Filter 不计数，Score 加 100（点击查看大图）" link="/images/posts/sched-plugins/29-pair-decision.svg" >}}

几个由代码结构决定、文档里没有写的性质：

1. 按副本计数，不按依赖计数。cart 有两个副本已调度，frontend 对 cart 的这一条依赖就会被计两次。依赖副本越多，它在多数票里的权重越大。
2. 同 zone 不看 maxNetworkCost。同一 zone 的不同节点一律算满足，即使 `maxNetworkCost` 写的是 0。
3. 两种「没有拓扑信息」的处理不对称。依赖 Pod 所在节点没有 region 和 zone 时，Filter 记违反、Score 加 100；而候选节点自己没有 region、zone 时，`populateCostMap` 跳过两个分支，`costMap` 为空，后续查找全部落空，Filter 什么都不计、Score 加 100。**前者会被过滤，后者在 Filter 阶段总能通过。**
4. **查不到代价时 Filter 放行、Score 惩罚。** 代价表漏写某个方向（比如只写了 z1→z2，没写 z2→z1），从 z2 看过去的这对依赖既不算满足也不算违反。
5. zone 名需要全局唯一。同 region 不同 zone 时，查表的 key 是 `(本节点 zone, 对方 zone)`，没有带上 region。两个 region 下都有叫 `a` 的 zone，代价表就无法区分。

### 3.5 Filter：多数票

PreFilter 算出了每个节点的满足数和违反数，Filter 只做一件事：把它们拿来投票。

```go {linenos=inline}
// pkg/networkaware/networkoverhead/networkoverhead.go
func (no *NetworkOverhead) Filter(ctx context.Context,
	cycleState fwk.CycleState,
	pod *corev1.Pod,
	nodeInfo fwk.NodeInfo) *fwk.Status {
	if nodeInfo.Node() == nil {
		return fwk.NewStatus(fwk.Error, "node not found")
	}
	// ...
	// Get PreFilterState
	preFilterState, err := getPreFilterState(cycleState)
	if err != nil {
		logger.Error(err, "Failed to read preFilterState from cycleState", "preFilterStateKey", preFilterStateKey)
		return fwk.NewStatus(fwk.Error, "not eligible due to failed to read from cycleState")
	}

	// If scoreEqually, return nil
	if preFilterState.scoreEqually {
		logger.V(6).Info("Score all nodes equally, return")
		return nil
	}

	// Get satisfied and violated number of dependencies
	satisfied := preFilterState.satisfiedMap[nodeInfo.Node().Name]
	violated := preFilterState.violatedMap[nodeInfo.Node().Name]
	// ...
	// The pod is filtered out if the number of violated dependencies is higher than the satisfied ones
	if violated > satisfied {
		return fwk.NewStatus(fwk.Unschedulable,
			fmt.Sprintf("Node %v does not meet several network requirements from Workload dependencies: Satisfied: %v Violated: %v", nodeInfo.Node().Name, satisfied, violated))
	}
	return nil
}
```

`maxNetworkCost` 不是硬约束。只有违反数严格多于满足数时才过滤，一半满足一半违反的节点照样通过。返回码是 `Unschedulable`（第 28–31 行），失败信息形如 `Node n-3 does not meet several network requirements from Workload dependencies: Satisfied: 1 Violated: 2`。

`Unschedulable` 意味着 PostFilter 阶段的抢占会把这些节点当作候选。但抢占模拟是通过 `AddPod`/`RemovePod` 在 CycleState 上增删 Pod 再重跑 Filter，而 NetworkOverhead 的这两个方法什么也不做：

```go {linenos=inline}
// pkg/networkaware/networkoverhead/networkoverhead.go
// AddPod from pre-computed data in cycleState.
// no current need for the NetworkOverhead plugin
func (no *NetworkOverhead) AddPod(ctx context.Context,
	cycleState fwk.CycleState,
	podToSchedule *corev1.Pod,
	podToAdd fwk.PodInfo,
	nodeInfo fwk.NodeInfo) *fwk.Status {
	return fwk.NewStatus(fwk.Success, "")
}
```

Filter 读的又是 PreFilter 算好的计数，所以驱逐任何 Pod 都不会改变它的结论。对被它拒绝的节点，[DefaultPreemption](/posts/sched-plugin-18-default-preemption/) 的模拟每次都会失败，只是白算一遍。从语义上说，这里返回 `UnschedulableAndUnresolvable` 更准确。

### 3.6 Score 与 NormalizeScore

过滤只留下「违反不多于满足」的节点，剩下的排序靠累计代价。

```go {linenos=inline}
// pkg/networkaware/networkoverhead/networkoverhead.go
func (no *NetworkOverhead) Score(ctx context.Context,
	cycleState fwk.CycleState,
	pod *corev1.Pod,
	nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	score := fwk.MinNodeScore
	nodeName := nodeInfo.Node().Name
	// ...
	// If scoreEqually, return minScore
	if preFilterState.scoreEqually {
		return score, fwk.NewStatus(fwk.Success, "scoreEqually enabled: minimum score")
	}

	// Return Accumulated Cost as score
	score = preFilterState.finalCostMap[nodeName]
	// ...
	return score, fwk.NewStatus(fwk.Success, "Accumulated cost added as score, normalization ensures lower costs are favored")
}

// NormalizeScore : normalize scores since lower scores correspond to lower latency
func (no *NetworkOverhead) NormalizeScore(ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	scores fwk.NodeScoreList) *fwk.Status {
	// ...
	// Get Min and Max Scores to normalize between fwk.MaxNodeScore and fwk.MinNodeScore
	minCost, maxCost := getMinMaxScores(scores)

	// If all nodes were given the minimum score, return
	if minCost == 0 && maxCost == 0 {
		return nil
	}

	var normCost float64
	for i := range scores {
		if maxCost != minCost { // If max != min
			// node_normalized_cost = MAX_SCORE * ( ( nodeScore - minCost) / (maxCost - minCost)
			// nodeScore = MAX_SCORE - node_normalized_cost
			normCost = float64(fwk.MaxNodeScore) * float64(scores[i].Score-minCost) / float64(maxCost-minCost)
			scores[i].Score = fwk.MaxNodeScore - int64(normCost)
		} else { // If maxCost = minCost, avoid division by 0
			normCost = float64(scores[i].Score - minCost)
			scores[i].Score = fwk.MaxNodeScore - int64(normCost)
		}
	}
	// ...
	return nil
}
```

Score 返回的原始值是累计代价（第 15 行），可以远大于 100，框架只在 NormalizeScore 之后检查分数范围。归一化公式是：

```text {linenos=inline}
score = 100 − int64( 100 × (cost − min) / (max − min) )
```

`min`、`max` 取的是本轮参与打分的节点（即通过所有 Filter 的节点）的代价。代价最低的节点得 100，最高的得 0，`int64` 向零截断。

所有节点代价相同且不为 0 时都得 100；全为 0（包括 `scoreEqually` 的情况）时保持 0，同样不影响相对排序。

## 四、算例

用一个两 region、四 zone 的集群把上面的规则走一遍。先看输入，再逐个节点算满足数、违反数和累计代价，最后统一归一化。

{{< figure src="/images/posts/sched-plugins/29-topology-example.svg" caption="算例集群：us-west-1 下 z1、z2，us-east-1 下 z3、z4；cart 的两个副本在 n-1、n-2，catalog 的一个副本在 n-3；n-6 没有拓扑 label；待调度的是 frontend 的一个副本（点击查看大图）" link="/images/posts/sched-plugins/29-topology-example.svg" >}}

NetworkTopology（`weightsName: UserDefined`）：

```yaml {linenos=inline}
apiVersion: networktopology.diktyo.x-k8s.io/v1alpha1
kind: NetworkTopology
metadata:
  name: nt-default
  namespace: default
spec:
  configmapName: "netperfMetrics"
  weights:
  - name: "UserDefined"
    topologyList:
    - topologyKey: "topology.kubernetes.io/region"
      originList:
      - origin: "us-west-1"
        costList:
        - {destination: "us-east-1", networkCost: 20}
      - origin: "us-east-1"
        costList:
        - {destination: "us-west-1", networkCost: 20}
    - topologyKey: "topology.kubernetes.io/zone"
      originList:
      - origin: "z1"
        costList:
        - {destination: "z2", networkCost: 5}
      - origin: "z2"
        costList:
        - {destination: "z1", networkCost: 5}
      - origin: "z3"
        costList:
        - {destination: "z4", networkCost: 10}
      - origin: "z4"
        costList:
        - {destination: "z3", networkCost: 10}
```

AppGroup `shop` 里 frontend 依赖 cart（`maxNetworkCost: 8`）和 catalog（`maxNetworkCost: 15`）。已绑定的依赖 Pod：cart-a 在 n-1，cart-b 在 n-2，catalog-a 在 n-3。

每个节点要处理三对：(cart-a, cart)、(cart-b, cart)、(catalog-a, catalog)。

**n-1（us-west-1/z1）**：

1. cart-a：就在本节点，满足，+0；
2. cart-b：同 region 的 z2，查 `(z1, z2)` 得 5 ≤ 8，满足，+5；
3. catalog-a：在 us-east-1，查 `(us-west-1, us-east-1)` 得 20 > 15，违反，+20。

满足 2、违反 1，通过；代价 25。

**n-2（us-west-1/z2）**：与 n-1 对称。

1. cart-a：查 `(z2, z1)` 得 5 ≤ 8，满足，+5；
2. cart-b：就在本节点，满足，+0；
3. catalog-a：违反，+20。

满足 2、违反 1，通过；代价 25。

**n-3（us-east-1/z3）**：

1. cart-a、cart-b：都跨 region，20 > 8，各违反一次、各 +20；
2. catalog-a：就在本节点，满足，+0。

满足 1、违反 2，`Unschedulable`；代价 40。

**n-4（us-east-1/z4）**：

1. cart-a、cart-b：各违反 +20；
2. catalog-a：在同 region 的 z3，查 `(z4, z3)` 得 10 ≤ 15，满足，+10。

满足 1、违反 2，`Unschedulable`；代价 50。

**n-5（us-west-1/z1）**：

1. cart-a：在同 zone 的 n-1，满足 +1（`SameZone`）；
2. cart-b：查 `(z1, z2)` 得 5 ≤ 8，满足 +5；
3. catalog-a：违反 +20。

满足 2、违反 1，通过；代价 26。

**n-6（无 label）**：本节点 region、zone 都是空串，`costMap` 为空。

三对依赖都走「region 不同」分支，查 `("", us-west-1)` 或 `("", us-east-1)` 都查不到：Filter 不计数，Score 各 +100。满足 0、违反 0，通过；代价 300。

n-3、n-4 被 Filter 拒绝，参与打分的是 n-1、n-2、n-5、n-6：

1. 含 n-6：min 25，max 300。n-1、n-2 得 100；n-5 得 `100 − int64(100 × 1 / 275)` = `100 − int64(0.36)` = 100；n-6 得 0。
2. 若 n-6 补上了 label 或被其它插件过滤掉：min 25，max 26。n-1、n-2 得 100；n-5 得 `100 − int64(100 × 1 / 1)` = 0。

{{< figure src="/images/posts/sched-plugins/29-result-table.svg" caption="算例结果：多数票让 n-1、n-2、n-5 在各有一次违反的情况下通过；没有 label 的 n-6 逃过 Filter 但代价最高；min-max 归一化让 n-5 的得分随 n-6 是否在场在 100 与 0 之间跳变（点击查看大图）" link="/images/posts/sched-plugins/29-result-table.svg" >}}

这个算例暴露了 min-max 归一化的两个性质。

第一，**分数只反映相对位置，不反映绝对差距**：25 和 26 只差 1 点代价，在没有 n-6 时被放大成 100 分对 0 分，乘上权重 5 就是 500 分的差距，在默认打分插件之间通常足以单独决定结果。

第二，**一个离群节点会压扁其余节点的区分度**：n-6 的 300 让 25 和 26 之间的差距在截断后消失。

结论随候选集合变化：大集群里 Filter 找够 percentageOfNodesToScore 规定数量的可行节点就会停止，哪些节点进入打分不是固定的，同一个节点的 NetworkOverhead 分数也会随之变化。

## 五、EventsToRegister 与重新入队

NetworkOverhead 实现了 PreFilter 和 Filter，却没有实现 `EnqueueExtensions`。v1.36.4 的框架对这种插件注册一个默认的扩展：

```go {linenos=inline}
// pkg/scheduler/framework/runtime/framework.go（k8s v1.36.4）
	ext, ok := p.(fwk.EnqueueExtensions)
	if !ok {
		// If interface EnqueueExtensions is not implemented, register the default enqueue extensions
		// to the plugin because we don't know which events the plugin is interested in.
		// This is to ensure backward compatibility.
		f.enqueueExtensions = append(f.enqueueExtensions, &defaultEnqueueExtension{pluginName: p.Name()})
		return
	}
```

`defaultEnqueueExtension.EventsToRegister` 返回 `framework.UnrollWildCardResource()`：Pod、Node、PV、PVC、CSINode、CSIDriver、CSIStorageCapacity、StorageClass、ResourceClaim、DeviceClass 的所有动作（开启 `GenericWorkload` 时再加 PodGroup），QueueingHint 用 `defaultQueueingHintFn`，一律返回 `Queue`。

所以被 NetworkOverhead 拒绝的 Pod，集群里任何一个 Pod 或节点的变化都会把它放回 activeQ（经过退避），这比实际需要的宽得多，但不会漏掉。

真正会漏掉的是 CRD 的变化。AppGroup、NetworkTopology 不在调度器的集群事件里，修改 `maxNetworkCost` 或代价表不会触发任何 Pod 重新入队。如果一个 Pod 是因为代价表写错被拒绝的，修正代价表之后，它要等到下一个 Pod/Node 事件，或者等 unschedulablePods 的定期刷新（`DefaultPodMaxInUnschedulablePodsDuration` 为 5 分钟）。

## 六、边界与坑

**CR 找不到时会 panic。** 当 Pod 带 AppGroup label 时，PreFilter 依次执行 `findAppGroupNetworkOverhead`、`findNetworkTopologyNetworkOverhead`、`sortNetworkTopologyCosts`、`GetDependencyList`。

出问题的路径是：

1. 两个 find 在所有 namespace 都找不到时返回 `nil`；
2. `sortNetworkTopologyCosts` 在 `weightsName` 不是 `NetperfCosts` 时会 `range networkTopology.Spec.Weights`，对 nil 指针解引用；
3. 即使跳过了排序，`GetDependencyList(pod, nil)` 也会访问 `ag.Spec`。

PreFilter 运行在调度主循环的 goroutine 里，框架不 recover，调度器进程退出。

KEP「Known limitations」一节写的是「没有这两个 CRD 时，NetworkOverhead 对所有节点打同分」，与代码行为不符。常见的触发方式是 `networkTopologyName` 用了默认的 `nt-default` 而集群里的 CR 叫别的名字，或 CR 建在了 `args.namespaces` 之外。

**依赖 Pod 所在节点被删除，会让整组 Pod 的 PreFilter 报错。** 依赖 Pod 的 `spec.nodeName` 指向的节点如果不在调度快照里（节点对象已删除、Pod 对象还没被清理），`NodeInfos().Get` 返回错误，PreFilter 返回 `Error`。

这个 Pod 会进入错误退避，依赖同一个 workload 的所有 Pod 在这段时间里都无法调度，直到那个孤儿 Pod 被垃圾回收。

**连续调度的 Pod 互相看不见。** `scheduledList` 来自 Pod informer，只包含 API Server 已经写入 `nodeName` 的 Pod。同一批 frontend 副本连续出队时，前一个副本只是在调度器缓存里 assume 了，绑定还在异步进行，后一个副本的 PreFilter 看不到它。

对 frontend 自己没有影响（它不依赖 frontend），但如果 cart 和 frontend 同时排队，frontend 很可能看不到刚被调度的 cart。

读调度快照（`SnapshotSharedLister().NodeInfos()` 里的 Pod 包含 assumed Pod）可以避免这个时间差，代价是要遍历节点上的 Pod 而不是按 label 列。

**MaxCost 与真实代价的量纲。** `MaxCost` 写死为 100，而 `networkCost` 在 CRD 里没有上限，`maxNetworkCost` 上限是 10000。如果代价表用毫秒级延迟填写，跨洲的真实代价可能是 150，比「查不到」的惩罚 100 还大，没有 label 的节点在打分上反而优于有完整拓扑信息的远端节点。

代价表的取值范围应当控制在 100 以内，或者把「未知」视为比任何已知代价都差。

**每次 PreFilter 都是全量计算。** 对快照中的全部 N 个节点，各构造一份 costMap（含一次 originList 排序），再对 S 个已调度 Pod 与 D 个依赖做双重循环，每对还要按名字取一次 NodeInfo。复杂度约为 O(N × (S × D + O log O))，其中 O 是 originList 长度。

仓库的基准测试覆盖到 10000 个节点、10 个已调度 Pod 的规模；AppGroup 内已调度 Pod 达到数千时，这部分开销会集中在 PreFilter，而且对每个待调度的 Pod 都要重来一遍。

## 七、发散

### 7.1 与 InterPodAffinity 的对比

InterPodAffinity 的 preferred 规则也能表达「靠近某类 Pod」，它的打分是「拓扑域内匹配 Pod 的数量 × 权重」，只区分同域与不同域。

NetworkOverhead 多出来的是两样东西：分层的代价（同节点 0、同 zone 1、跨 zone 查表、跨 region 查表）和每条依赖各自的容忍上限。代价是它要求维护两个 CRD 和配套 controller，且只覆盖 region/zone 两级，同 zone 内的机架、交换机层级无法表达。如果只是想让 frontend 和 cart 同 zone，InterPodAffinity 更简单、也更可靠。

### 7.2 与 NodeResourceTopologyMatch 同属「拓扑感知」，但关注的层级不同

NetworkOverhead 关心节点之间的距离，[下一篇](/posts/sched-plugin-30-node-resource-topology/)的 NodeResourceTopologyMatch 关心节点内部的拓扑：CPU、内存、设备分布在哪个 NUMA 节点上。

两者都需要一个额外的 CRD 把调度器看不到的拓扑信息喂进来（NetworkTopology、NodeResourceTopology），也都面临同样的问题：CRD 的变化不在调度器的事件体系里，数据的新鲜度取决于外部 controller。

### 7.3 代价从哪里来

手工填写的 `UserDefined` 代价是静态的。KEP 设想的完整链路分三步：

1. netperf 组件定期在每对 zone/region 之间各挑一对节点测量延迟，写入 ConfigMap；
2. networktopology controller 读取 ConfigMap，计算代价（例如对多跳路径求最短路），写回 NetworkTopology 的 `NetperfCosts` 权重；
3. 插件配置 `weightsName: NetperfCosts` 使用它。

这个设计只测 zone/region 之间的代表性节点对，避免在大集群里做 O(N²) 的全量探测，前提是同一 zone 内的网络质量均匀，这也是插件对同 zone 固定记 1 的原因。

## 八、小结

1. NetworkOverhead 依赖两个 CRD：AppGroup 提供依赖与 `maxNetworkCost`，NetworkTopology 提供按 `weightsName` 区分的 region/zone 有向代价表；默认 `weightsName` 为 `UserDefined`、`networkTopologyName` 为 `nt-default`、`namespaces` 为 `["default"]`。
2. 所有计算都在 PreFilter 中完成，结果存进 CycleState 的三张节点 map；Filter、Score 只是查表，`Clone` 共享指针，AddPod/RemovePod 为空，抢占模拟无法改变它的结论。
3. 逐对判定：同节点 0、同 zone 1（不看上限）、跨 zone/region 查表比较上限；依赖节点无 label 记违反加 100；查不到代价时 Filter 不计数、Score 加 100。
4. Filter 是按副本计数的多数票，`violated > satisfied` 才返回 `Unschedulable`；Score 返回累计代价，NormalizeScore 做 min-max 反转，分数对候选集合敏感。
5. 没有 EnqueueExtensions，按所有 Pod/Node 等事件重新入队；CRD 变化不会触发重试；CR 缺失时 PreFilter 解引用 nil 导致调度器退出。

下一篇进入 [NodeResourceTopologyMatch](/posts/sched-plugin-30-node-resource-topology/)，从节点之间的网络拓扑转到节点内部的 NUMA 拓扑。
