---
title: 'scheduler-plugins 22 [in-tree] TopologyPlacementGenerator：让整组 Pod 只在一个拓扑域里试放'
date: 2026-11-19T11:40:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "topology-aware-scheduling", "podgroup", "placement"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "TopologyPlacementGenerator 是 PlacementGenerate 扩展点上唯一的 in-tree 插件：它按 PodGroup 声明的拓扑标签把节点切成若干候选 placement，让整组 Pod 只在同一个 rack 或 zone 里试放。本文拆解 placement 的数据结构、已调度 Pod 如何锁定域、多个生成插件的笛卡尔积合并、snapshot 如何限定节点集合，以及调度器在多个 placement 之间搜索的上限公式。"
---

[上一篇](/posts/sched-plugin-21-gang-scheduling/)讲了 PodGroup 调度周期的骨架：整组出队，逐个 Pod 试放，每放一个就让 GangScheduling 判断一次 `PlacementFeasible`。那条路径默认把整组放在「全部节点」这一个隐式 placement 里。

分布式训练对放在哪里通常有更强的要求：同一个训练任务的所有 worker 最好在同一个 rack 下，走同一台 ToR 交换机；或者至少在同一个 zone，避免跨可用区的流量和延迟。`PodTopologySpread` 和 `InterPodAffinity` 是逐个 Pod 判断的，前一个 Pod 选了 rack-1，后一个 Pod 才被吸引过去，第一个 Pod 选错了，整组就跟着错。

`TopologyAwareWorkloadScheduling`（KEP-5732）换了一种做法：**先决定整组去哪个域，再在这个域里逐个放 Pod**，多个域都试一遍，最后挑最好的。负责「列出候选域」的就是本篇的 TopologyPlacementGenerator，负责「挑最好的」是[下一篇](/posts/sched-plugin-23-podgroup-pods-count/)的 PodGroupPodsCount。

本文基于 Kubernetes master 提交 `aabdf5a2`。

## 一、位置与开关

插件注册名是 `TopologyPlacementGenerator`，代码在 `topologyaware` 包里，结构体叫 `TopologyPlacement`。它和 PodGroupPodsCount 由同一个函数追加到默认 Profile：

```go
// pkg/scheduler/apis/config/v1/default_plugins.go
func applyTopologyAwareWorkloadScheduling(config *v1.Plugins) {
	config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.TopologyPlacementGenerator})
	config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.PodGroupPodsCount, Weight: ptr.To[int32](1)})
}
```

触发条件是 `TopologyAwareWorkloadScheduling` feature gate：1.36 Alpha，1.38 Beta，默认关闭，并且依赖 `GenericWorkload`。它只实现了一个接口：

```go
// pkg/scheduler/framework/plugins/topologyaware/topology_placement.go
// TopologyPlacement is a plugin that generates placements for a pod group based on its topology constraints.
type TopologyPlacement struct {
	handle fwk.Handle
	fts    feature.Features
}

var _ fwk.PlacementGeneratePlugin = &TopologyPlacement{}
```

`PlacementGenerate` 是 PodGroup 调度周期专用的扩展点，单个 Pod 的调度周期完全不会调用它。所以即使打开了 feature gate，普通 Pod 的调度行为也不变。

## 二、API：PodGroup 上的 schedulingConstraints

拓扑约束写在 PodGroup（或 CompositePodGroup）的 `schedulingConstraints.topology` 里：

```go
// staging/src/k8s.io/api/scheduling/v1beta1/types.go
// PodGroupSchedulingConstraints defines scheduling constraints (e.g. topology) for a PodGroup.
type PodGroupSchedulingConstraints struct {
	// topology defines the topology constraints for the pod group.
	// Currently only a single topology constraint can be specified. This may change in the future.
	//
	// +optional
	// +k8s:optional
	// +k8s:maxItems=1
	// ...
	Topology []TopologyConstraint `json:"topology,omitempty" protobuf:"bytes,1,rep,name=topology"`
}

// TopologyConstraint defines a topology constraint for a PodGroup.
type TopologyConstraint struct {
	// key specifies the key of the node label representing the topology domain.
	// All pods within the PodGroup must be colocated within the same domain instance.
	// Different PodGroups can land on different domain instances even if they derive from the same PodGroupTemplate.
	// Nodes where this label is missing or has an empty value do not belong to any domain instance,
	// so pods within the PodGroup cannot be scheduled onto them.
	// Examples: "topology.kubernetes.io/rack"
	//
	// +required
	// ...
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`
}
```

几个约束直接写在 API 里：最多一个 topology 项（`maxItems=1`）；整个 `schedulingConstraints` 字段不可变；没有该标签或标签值为空的节点不属于任何域。一个完整的例子：

```yaml
apiVersion: scheduling.k8s.io/v1beta1
kind: PodGroup
metadata:
  name: llm-train-0
spec:
  schedulingPolicy:
    gang:
      minCount: 16
  schedulingConstraints:
    topology:
    - key: topology.kubernetes.io/rack
```

它的语义是「16 个 Pod 必须同时放进**同一个** rack」，和 `PodTopologySpread` 的「尽量均匀打散」方向相反，更接近一个组级别的 `requiredDuringScheduling` 亲和。

## 三、GeneratePlacements 源码

### 3.1 placement 是什么

```go
// staging/src/k8s.io/kube-scheduler/framework/types.go
// Placement determines the resources to be considered when scheduling a pod group.
// Pod group scheduling cycle can check multiple placements and select the one that results
// in the best pod assignments.
type Placement struct {
	// Name uniquely identifies the placement.
	// This is used for diagnostics and debugability.
	// The choice of the name is up to the PlacementGeneratePlugin.
	// Names returned by one plugin must be unique and must not contain "/",
	// which the framework uses to join names when merging placements.
	Name string
	// Nodes specifies the nodes that are valid for this placement.
	// Scheduler will try to schedule the pod group using only those nodes.
	Nodes []NodeInfo
}
```

一个 placement 就是「一个名字 + 一组节点」。注意 `Nodes` 里存的是 snapshot 里的 `NodeInfo` 指针，不是节点名，这一点在后面 `AssumePlacement` 会用到。

### 3.2 主函数

```go
// pkg/scheduler/framework/plugins/topologyaware/topology_placement.go
func (pl *TopologyPlacement) GeneratePlacements(ctx context.Context, state fwk.PodGroupCycleState, podGroup fwk.PodGroupInfo, parentPlacement *fwk.Placement) (*fwk.GeneratePlacementsResult, *fwk.Status) {
	topologyKey, ok := pl.getTopologyKey(podGroup)
	if !ok {
		// No topology constraints, return a single placement with no constraints.
		return &fwk.GeneratePlacementsResult{Placements: []*fwk.Placement{parentPlacement}}, nil
	}

	var requiredDomain *string
	scheduledPods, err := pl.getScheduledPods(podGroup)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	if len(scheduledPods) > 0 {
		scheduledDomain, err := pl.getScheduledPodsTopologyDomain(topologyKey, scheduledPods)
		if err != nil {
			return nil, fwk.AsStatus(fmt.Errorf("cannot determine domain for already scheduled pods: %w", err))
		}
		requiredDomain = &scheduledDomain
	}

	nodesPerTopologyDomain := make(map[string][]fwk.NodeInfo)
	for _, node := range parentPlacement.Nodes {
		if domain, ok := node.Node().Labels[topologyKey]; ok && domain != "" {
			if requiredDomain == nil || *requiredDomain == domain {
				nodesPerTopologyDomain[domain] = append(nodesPerTopologyDomain[domain], node)
			}
		}
	}

	placements := make([]*fwk.Placement, 0, len(nodesPerTopologyDomain))
	for topologyDomain, nodes := range nodesPerTopologyDomain {
		if len(nodes) > 0 {
			placements = append(placements, &fwk.Placement{
				Name:  topologyDomain,
				Nodes: nodes,
			})
		}
	}

	return &fwk.GeneratePlacementsResult{Placements: placements}, nil
}
```

逻辑分三步：取 topology key；找组里已调度 Pod 所在的域；按标签值把父 placement 的节点分桶。下面这张图把它会遇到的几种情况放在一起：

{{< figure src="/images/posts/sched-plugins/22-generate.svg" link="/images/posts/sched-plugins/22-generate.svg" caption="GeneratePlacements 的五种情况：无约束、首次调度、已有 Pod 锁定域、域冲突报错、父 placement 里没有所需的域（点击查看大图）" >}}

**topology key 只取第一项。** `getTopologyKey` 对 PodGroup 和 CompositePodGroup 分别读 `Spec.SchedulingConstraints.Topology[0].Key`，注释写着「Right now, we only support a single topology constraint on the API level」。

**没有约束就原样返回 `parentPlacement`。** 这是一个约定：框架看到「只返回了一个 placement，而且就是传进来的那个指针」，就知道这个插件不想约束，会把它从合并里剔除（见第四节）。返回一个内容相同的新 placement 是不行的，那会被当成「有约束」。

**节点按标签值分桶，空值和缺失都跳过。** 这和 API 注释一致。一个常见的误配是只给部分节点打了 rack 标签，结果没打标签的节点对这个组完全不可见，看起来像是「集群资源够却调度不上」。

**placement 的 Name 直接用标签值。** 对 rack 标签来说就是 `r1`、`r2`。由于是从 map 里遍历出来的，返回顺序每次不同，接口注释也提醒插件「must not rely on their order」。

### 3.3 已调度 Pod 锁定域

这是这个插件里最有意思的设计。考虑一个 `minCount: 4` 的组有 6 个 Pod，第一轮在 rack r2 放下了 5 个，第 6 个因资源不够回队。下一轮调度这个组时，队列里只剩第 6 个 Pod。如果插件仍然列出所有 rack，调度器可能把它放到 r1，整组就被拆到了两个 rack。

所以插件先取出组里已经调度（assigned 或 assumed）的 Pod，要求它们都在同一个域：

```go
// pkg/scheduler/framework/plugins/topologyaware/topology_placement.go
func (pl *TopologyPlacement) getScheduledPodsTopologyDomain(topologyKey string, scheduledPods []*v1.Pod) (string, error) {
	topologyDomain := ""
	for _, pod := range scheduledPods {
		node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(pod.Spec.NodeName)
		if err != nil {
			return "", fmt.Errorf("getting node for pod %v: %w", klog.KObj(pod), err)
		}
		domain, ok := node.Node().Labels[topologyKey]
		if !ok {
			return "", fmt.Errorf("no topology domain found for pod %v", klog.KObj(pod))
		}
		// An empty label value is treated as a node labeling mistake, not as a domain.
		if domain == "" {
			return "", fmt.Errorf("empty topology domain found for pod %v", klog.KObj(pod))
		}
		if topologyDomain != "" && topologyDomain != domain {
			return "", fmt.Errorf("more than 1 domain found for pod group: %q, %q", topologyDomain, domain)
		}
		topologyDomain = domain
	}
	return topologyDomain, nil
}
```

三种错误都会让 `GeneratePlacements` 返回 `Error` 状态，整个组调度周期以 Error 结束（不是 Unschedulable），PodGroup 的 condition reason 会是 `SchedulerError`。什么时候会走到这里？

- 节点标签被改了：组里的 Pod 已经在 r2 的节点上跑，有人把其中一个节点的标签改成了 r3；
- Pod 不是通过这条路径调度的：比如在打开 feature gate 之前已经调度了一部分，或者有 Pod 直接写了 `nodeName`；
- 已调度的 Pod 落在了没有标签的节点上。

这些都不是「等一等就能好」的状态，所以返回 Error 而非 Unschedulable 是合理的：它会在日志和 PodGroup condition 里留下明确的错误信息，而不是悄无声息地一直重试。

开启 CompositePodGroup 时，`getScheduledPods` 会用 `helper.GetPodGroupStates` 遍历以当前组为根的整棵子树，收集所有叶子 PodGroup 的已调度 Pod。这样一个带 zone 约束的 CompositePodGroup 下，任何一个子组已经落地的 Pod 都会把整棵树锁在同一个 zone。

### 3.4 它不写任何 CycleState

PodGroup 调度周期里有三层状态：`PodGroupCycleState`（整个组一份）、`PlacementCycleState`（每个候选 placement 一份）、以及每个 Pod 自己的 `CycleState`。接口允许生成插件通过 `state.PlacementState(placement).Write(...)` 给某个 placement 挂上数据，后续这个 placement 里每个 Pod 的 Filter/Score 都能通过 `GetPlacementCycleState()` 读到。

TopologyPlacement 没有用这个能力，它产出的信息全在 `Nodes` 列表里。这也说明 placement 机制的设计目标比「按标签分组」更宽：将来的生成插件可以给 placement 附带预计算的数据（比如某个域里可用的 DRA 设备），而不仅仅是节点列表。

## 四、框架如何使用生成结果

### 4.1 RunPlacementGeneratePlugins：剔除不约束的插件，再做笛卡尔积

```go
// pkg/scheduler/framework/runtime/framework.go
func (f *frameworkImpl) RunPlacementGeneratePlugins(ctx context.Context, state fwk.PodGroupCycleState, podGroup fwk.PodGroupInfo, nodes []fwk.NodeInfo) (placements []*fwk.Placement, status *fwk.Status) {
	// ...
	if len(f.placementGeneratePlugins) == 0 {
		return []*fwk.Placement{{Nodes: nodes}}, nil
	}

	// Run each plugin and keep the ones that constrain the input.
	// Each plugin gets its own input placement pointer so per-placement state remains isolated.
	var constrained [][]*fwk.Placement
	for _, plugin := range f.placementGeneratePlugins {
		inputPlacement := &fwk.Placement{Nodes: nodes}
		result, status := f.runPlacementGeneratePlugin(ctx, plugin, state, podGroup, inputPlacement)
		if !status.IsSuccess() {
			return nil, status.WithPlugin(plugin.Name())
		}
		if result == nil || len(result.Placements) == 0 {
			return nil, fwk.NewStatus(fwk.Unschedulable, "no feasible placements found").WithPlugin(plugin.Name())
		}
		if err := validatePlacementNames(result.Placements); err != nil {
			return nil, fwk.AsStatus(err).WithPlugin(plugin.Name())
		}
		if len(result.Placements) == 1 && result.Placements[0] == inputPlacement {
			continue
		}
		constrained = append(constrained, result.Placements)
	}

	if len(constrained) == 0 {
		return []*fwk.Placement{{Nodes: nodes}}, nil
	}

	merged := constrained[0]
	for i := 1; i < len(constrained); i++ {
		merged, status = mergePlacements(merged, constrained[i], state)
		// ...
	}
	return merged, nil
}
```

需要注意的几点：

- 返回空列表的含义是「没有可行的 placement」，框架直接给出 `Unschedulable`，失败插件记为 `TopologyPlacementGenerator`。图里的情况 E 就是这样：组里已有 Pod 在 r2，而父 placement（比如 CompositePodGroup 选中的 zone-a）里没有 r2 的节点；
- 指针比较 `result.Placements[0] == inputPlacement` 就是 3.2 节说的「不约束」约定；
- 多个插件都给出约束时，`mergePlacements` 对它们做笛卡尔积，节点取交集，名字用 `/` 连接，交集为空的组合直接丢掉。这也是 `validatePlacementNames` 禁止名字里出现 `/`、禁止同一插件返回重名的原因。

当前 in-tree 只有这一个生成插件，所以默认配置下不会触发合并。

### 4.2 AssumePlacement：让逐 Pod 算法只看见这些节点

placement 怎么限制后面的 Filter？靠 snapshot：

```go
// pkg/scheduler/backend/cache/snapshot.go
func (s *Snapshot) AssumePlacement(placement *fwk.Placement) error {
	if placement == nil {
		s.ForgetPlacement()
		return nil
	}
	if len(placement.Nodes) == len(s.nodeInfoList) {
		// All nodes in placement, meaning we can treat it the same as no placement and avoid copying the buffer.
		s.ForgetPlacement()
		return nil
	}
	s.placementNodes = &placementNodes{
		placement:   placement,
		nodeInfoSet: sets.New[string](),
	}
	for _, node := range placement.Nodes {
		snapshotNode, ok := s.nodeInfoMap[node.Node().Name]
		// ...
		if snapshotNode != node {
			s.ForgetPlacement()
			return fmt.Errorf("node %s in placement is not the same instance as in the snapshot", node.Node().Name)
		}
		s.placementNodes.nodeInfoSet.Insert(node.Node().Name)
	}
	return nil
}
```

assume 之后，`SchedulePod` 里的 `NumNodesInPlacement`、`prefilterNodes` 里的 `ListNodesInPlacement`、以及 PreFilterResult 和 NominatedNodeName 用到的 `GetNodeInPlacement` 都只看这个 placement 的节点。逐 Pod 算法本身一行没改，就被限定在了一个 rack 里。

两个细节：一是 placement 里的节点必须是 snapshot 里**同一个** `NodeInfo` 实例，生成插件不能自己构造或复制 NodeInfo；二是 placement 覆盖全部节点时等同于没有 placement，省去建集合的开销。

还要注意，placement 限制的是「候选节点」，不是「插件能看到的集群」。`RunPreFilterPlugins` 传给各插件的 `nodes` 参数来自 `SnapshotSharedLister().NodeInfos().List()`，而 `Snapshot.List()` 返回的是全部节点，所以 `PodTopologySpread`、`InterPodAffinity` 在 PreFilter 里统计已有 Pod 分布时，看到的仍是整个集群；被 placement 挡在外面的只是 Filter 和 Score 的候选节点。

### 4.3 在多个 placement 之间搜索

{{< figure src="/images/posts/sched-plugins/22-search.svg" link="/images/posts/sched-plugins/22-search.svg" caption="podGroupSchedulingPlacementAlgorithm 的搜索流程与「找到多少个可行 placement 就停」的上限公式（点击查看大图）" >}}

生成之后，`podGroupSchedulingPlacementAlgorithm` 决定评估哪些 placement、评估到什么时候停。三个设计点：

**NominatedNodeName 快速路径。** 如果组里 Pod 的 `status.nominatedNodeName` 恰好只落在一个 placement 里，先评估它，可行就直接用，不再看别的 placement：

```go
// pkg/scheduler/schedule_one_podgroup.go
	// Try the placement matching the pods' NominatedNodeName first, mirroring pod-by-pod
	// scheduling, which evaluates the nominated node before all others. A nominated node is
	// usually set by a previous preemption cycle and is the placement the pod group is
	// expected to land on, so if the gang is feasible there we use it and skip the rest.
	//
	// This fast path is limited to standalone PodGroups. A PodGroup that is part of a
	// ...
	nominatedFeasible := false
	var nominated *fwk.Placement
	if queuedPodGroupInfo.GetType() == fwk.PodGroupKeyType {
		nominated = nominatedPlacement(placements, podGroupInfo, queuedPodGroupInfo)
	}
```

NNN 一般是上一轮整组抢占留下的。抢占已经在某个 rack 里腾出了位置，这一轮如果又去打分、选到另一个 rack，前面驱逐的 Pod 就白白牺牲了。代码注释承认这个取舍：「we may prefer the nominated placement over one that fits the whole gang」。快速路径还要求 `anyScheduled`，避免「这个 placement 可行只是因为之前已经有足够的 Pod 在跑，新 Pod 一个都没放上」被误当成成功。

**只找够数就停。** 有 PlacementScore 插件时，要找的可行 placement 数由 `numFeasiblePlacementsToFind` 决定；没有打分插件时只找 1 个，找到第一个可行的就用：

```go
// pkg/scheduler/schedule_one_podgroup.go
	// If percentage is 0, linearly interpolate from 100% to 10% as the summed node count
	// across generated placements grows from 0 to 5000, with a 5% floor.
	if percentage == 0 {
		numAllNodes := 0
		for _, placement := range placements {
			// Placements may overlap or omit cluster nodes, so use their summed node count.
			numAllNodes += len(placement.Nodes)
		}
		percentage = max(minFeasiblePlacementsPercentageToFind, 100-numAllNodes*9/500)
	}

	numPlacements = max(minFeasiblePlacementsToFind, numAllPlacements*percentage/100)
```

这和单 Pod 调度的 `percentageOfNodesToScore` 是同一个思路：集群越大，越不追求全局最优。比例可以用 `KubeSchedulerConfiguration` 的 `percentageOfPlacementsToScore` 配置（全局或 profile 级，0~100），这个字段只在开启 `TopologyAwareWorkloadScheduling` 时允许设置，默认值是 0，也就是上面的自适应公式。

**只评估一部分时先打乱。** `placementsToEvaluate` 在 `numToFind < N` 时克隆并随机打乱 placement 顺序，避免总是偏向生成插件先返回的那些域。

每评估一个 placement，都要完整地跑一遍第 21 篇的逐 Pod 算法，然后立刻 `revert`：

```go
// pkg/scheduler/schedule_one_podgroup.go
func (sched *Scheduler) evaluatePlacement(ctx context.Context, schedFwk framework.Framework, podGroupCycleState *framework.CycleState, podGroupInfo *framework.PodGroupInfo, queuedPodGroupInfo *framework.QueuedPodGroupInfo, placement *fwk.Placement) *podGroupAlgorithmResult {
	// ...
	if err := sched.nodeInfoSnapshot.AssumePlacement(placement); err != nil {
		// ...
	}
	placementCycleState := newPlacementCycleState(podGroupCycleState, placement)
	result, placementRevertFns := sched.podGroupSchedulingDefaultAlgorithm(ctx, schedFwk, placementCycleState, podGroupInfo, queuedPodGroupInfo)
	placementRevertFns.revert()
	// ...
}
```

每个 placement 都从同一个干净的 snapshot 出发，结果互不干扰；被选中的那份结果里保存着每个 Pod 的 `scheduleResult`，提交阶段再真正 assume 进 cache。

## 五、边界情况

**不可行的 placement 不计入「够数」。** `numToFind` 数的是可行的 placement。如果 40 个 rack 里只有 2 个放得下，调度器会把 40 个全部试一遍。每次试放能被 PlacementFeasible 提前判死，代价仍然不小。拓扑域很多、资源又紧张的集群，组调度的耗时主要花在这里，可以看 `scheduler_placement_evaluations_total{result="infeasible"}` 和 `scheduler_placement_evaluation_duration_seconds`。

**全部不可行时的报错只反映第一个 placement。** 没有一个可行时，返回的消息是 `0/N placements are available, first placement status: ...`，后半段只是第一个被评估的 placement 的失败原因（打乱后的第一个），不代表所有 rack 都是同一个原因。代码注释也说「Which one we report is otherwise arbitrary」。

**拓扑约束只是「同一个域」，不是「尽量紧凑」。** 插件保证整组在同一个 rack，但 rack 内部 Pod 怎么分布，仍然由逐 Pod 的 Score 插件决定。想要 rack 内也尽量集中，需要配合节点打分策略。

**标签值不能含 `/`，在这里恰好不是问题。** placement 名字禁止 `/`，而名字就是标签值。Kubernetes 的标签值本身不允许 `/`，所以这条校验对 TopologyPlacement 永远不会触发，但第三方生成插件如果用节点名、路径之类做名字就要注意。

**和 PodTopologySpread 同时使用。** 如果 PodGroup 要求同一个 rack，而 Pod 模板里又写了按 rack 打散的 `DoNotSchedule` 约束，两者是矛盾的：placement 已经把候选节点限定在一个 rack 里，PodTopologySpread 却要求 rack 之间的 skew 不超过 maxSkew。结果通常是组里只能放下很少的 Pod，触发 PlacementFeasible 的 Unschedulable。

## 六、发散

### 6.1 嵌套的拓扑：zone 里再选 rack

开启 CompositePodGroup 后，拓扑约束可以分层：外层 CompositePodGroup 要求同一个 zone，内层每个 PodGroup 要求同一个 rack。`compositePodGroupSchedulingPlacementAlgorithm` 先对外层生成 zone 级 placement，`AssumePlacement(zone-a)` 之后递归调度子组；子组再调用 `RunPlacementGeneratePlugins` 时，传进来的 `allNodes` 来自 `ListNodesInPlacement()`，也就是 zone-a 的节点。TopologyPlacement 在这些节点里按 rack 分桶，自然得到「zone-a 里的 rack」。插件本身不需要知道层级，`parentPlacement` 参数就是为此设计的。

子组之间还有一个顺序依赖：前一个子组成功后，`assumeSubtreeWithRevert` 把它的结果 assume 进 snapshot，后一个子组在评估时就能看到前者占用的资源。这让两个子组不会同时把同一个 rack 的资源算成自己的。

### 6.2 与「先过滤再选域」方案的对比

一种更直观的做法是：先对每个 Pod 跑 Filter 得到可行节点，再看哪个域能容纳全部 Pod。这个方案的问题是忽略了 Pod 之间的相互影响，第 1 个 Pod 占掉的资源会影响第 2 个 Pod 的可行性，要准确判断就得模拟。placement 方案本质上就是「在每个候选域里做一次完整模拟」，用 snapshot 的 assume/revert 来保证模拟之间互不干扰。它更准确，代价是乘上了 placement 数，所以才需要第四节那套「找够就停」的机制。

### 6.3 什么时候值得开

PodGroup 不写 `schedulingConstraints` 时，TopologyPlacement 原样返回父 placement，框架视为不约束，于是只有一个 placement，退化成第 21 篇的流程。打开 feature gate 本身不会改变没有拓扑约束的组的行为，额外开销只是多一次插件调用。真正带来成本的是「组写了拓扑约束」且「拓扑域数量多」，这种情况要关注组调度延迟对全局吞吐的影响。

## 七、小结

- TopologyPlacementGenerator 只实现 `PlacementGenerate`，按 PodGroup/CompositePodGroup 的 `schedulingConstraints.topology[0].key` 把父 placement 的节点按标签值分桶，每个非空域一个 placement；无标签或空标签的节点被排除。
- 没有拓扑约束时原样返回父 placement 指针，框架据此识别为「不约束」；返回空列表会让框架给出 `Unschedulable: no feasible placements found`。
- 组里已有调度过的 Pod 时，只返回那个域；已调度 Pod 跨了多个域或在无标签节点上，返回 Error。
- placement 通过 `Snapshot.AssumePlacement` 限定逐 Pod 算法的候选节点；调度器按 `percentageOfPlacementsToScore`（默认自适应）找够若干个可行 placement 后停止，有 NominatedNodeName 时优先评估它所在的 placement。

找到多个可行 placement 之后，靠什么挑出最好的一个？下一篇讲 [PodGroupPodsCount](/posts/sched-plugin-23-podgroup-pods-count/)。
