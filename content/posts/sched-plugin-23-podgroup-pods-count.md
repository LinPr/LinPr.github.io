---
title: 'scheduler-plugins 23 [in-tree] PodGroupPodsCount：在可行的 placement 里挑能放下更多 Pod 的那个'
date: 2026-11-19T11:50:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "podgroup", "placement", "scoring"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "PodGroupPodsCount 是 PlacementScore 扩展点上的打分插件：在多个都满足 minCount 的候选 placement 之间，给能放下更多 Pod 的那个更高分。本文拆解它的打分与归一化公式、为什么分母用最大值而不是 minCount、为什么默认权重是 1、它和 NodeResourcesFit 的 MostAllocated 放置打分如何互相制衡，并给出一个带数字的完整算例。"
---

[上一篇](/posts/sched-plugin-22-topology-placement/)的 TopologyPlacementGenerator 把节点切成了若干 rack 级的候选 placement，调度器在每个 placement 里完整试放一遍整组 Pod，留下那些通过了 GangScheduling `PlacementFeasible` 的。如果可行的 placement 不止一个，就需要打分来挑。

这一步发生在 PodGroup 调度周期的 `PlacementScore` 扩展点。默认 Profile 里有两个插件在这里打分：本篇的 PodGroupPodsCount，以及 NodeResourcesFit 的放置打分。前者回答「哪个 placement 放下的 Pod 多」，后者回答「哪个 placement 装得更满」。

本文基于 Kubernetes master 提交 `aabdf5a2`。

## 一、位置、开关与权重

PodGroupPodsCount 和 TopologyPlacementGenerator 由同一个函数加入默认 Profile，`TopologyAwareWorkloadScheduling` feature gate 控制（1.36 Alpha，1.38 Beta，默认关闭）：

```go
// pkg/scheduler/apis/config/v1/default_plugins.go
func applyTopologyAwareWorkloadScheduling(config *v1.Plugins) {
	config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.TopologyPlacementGenerator})
	config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.PodGroupPodsCount, Weight: ptr.To[int32](1)})
}
```

它是默认插件里少数显式写了 `Weight: 1` 的。插件本身极简，没有参数，也不读写任何 CycleState：

```go
// pkg/scheduler/framework/plugins/podgrouppodscount/podgroup_pods_count.go
// PodGroupPodsCount is a placement score plugin that favors placements that can accommodate more pods from the considered PodGroup.
type PodGroupPodsCount struct {
	handle fwk.Handle
	fts    feature.Features
}

var _ fwk.PlacementScorePlugin = &PodGroupPodsCount{}

const Name = names.PodGroupPodsCount

// New initializes a new plugin and returns it.
func New(_ context.Context, _ runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &PodGroupPodsCount{handle: h, fts: fts}, nil
}
```

## 二、什么时候会被调用，什么时候不会

PlacementScore 插件并不是每次组调度都会跑。回顾第 22 篇的搜索流程，以下几种情况会跳过打分：

- 没开 `TopologyAwareWorkloadScheduling`：组调度走隐式 placement，根本没有「多个候选」；
- PodGroup 没有拓扑约束：TopologyPlacementGenerator 原样返回父 placement，只有一个候选；
- NominatedNodeName 快速路径命中：直接采用，不打分；
- 只找到一个可行 placement：`findBestPodGroupPlacement` 在 `len(successfulResults) == 1` 时直接返回它。

只有在「有多个可行 placement」时，`findBestPlacement` 才会调 `RunPlacementScorePlugins`：

```go
// pkg/scheduler/schedule_one_podgroup.go
func (sched *Scheduler) findBestPlacement(ctx context.Context, schedFwk framework.Framework, podGroupCycleState fwk.PodGroupCycleState, podGroupInfo *framework.PodGroupInfo, placementPodGroupAssignments []*fwk.PodGroupAssignments, placementStates []fwk.PlacementCycleState) (*fwk.Placement, *fwk.Status) {
	scores, status := schedFwk.RunPlacementScorePlugins(ctx, podGroupCycleState, podGroupInfo, placementPodGroupAssignments, placementStates)
	if !status.IsSuccess() {
		return nil, status
	}

	for i := range scores {
		scores[i].Randomizer = rand.Int()
	}
	// ...
	bestScore := &scores[0]
	for _, score := range scores[1:] {
		if score.TotalScore > bestScore.TotalScore ||
			score.TotalScore == bestScore.TotalScore &&
				score.Randomizer > bestScore.Randomizer {
			bestScore = &score
		}
	}
	return bestScore.Placement, nil
}
```

同分时用一个随机数决胜，和单 Pod 调度的 `selectHost` 是同一个目的：避免多个组总是挤进同一个 rack。

打分插件拿到的输入是 `PodGroupAssignments`：

```go
// staging/src/k8s.io/kube-scheduler/framework/types.go
// PodGroupAssignments holds the temporary assignments of pods in a pod group to nodes for a placement.
// Can be used in the pod group scheduling cycle to determine the best placement for a pod group.
type PodGroupAssignments struct {
	*Placement
	// ProposedAssignments associates pods with proposed nodes that were determined for a given placement
	// during the pod group scheduling cycle.
	// The pods are guaranteed to also be present in the PodGroupInfo.
	ProposedAssignments []ProposedAssignment
}
```

`ProposedAssignments` 由 `makeProposedAssignments` 从试放结果里挑出「状态成功且有节点名」的 Pod 构造，也就是这个 placement 里本轮新放下的 Pod。

## 三、打分：已调度数 + 提议数

```go
// pkg/scheduler/framework/plugins/podgrouppodscount/podgroup_pods_count.go
// ScorePlacement calculates a score for a given Placement.
// Both scheduled (assumed/assigned) pods and the proposed assignments are taken into consideration
// when computing the score. This ensures that the relative difference between choices is reduced,
// and small changes to the total count result in small changes to the score.
func (pl *PodGroupPodsCount) ScorePlacement(ctx context.Context, state fwk.PlacementCycleState, podGroup fwk.PodGroupInfo, placement *fwk.PodGroupAssignments) (int64, *fwk.Status) {
	scheduledPodsCount, err := pl.getScheduledPodsCount(podGroup)
	if err != nil {
		return 0, fwk.AsStatus(err)
	}

	return int64(scheduledPodsCount + len(placement.ProposedAssignments)), nil
}
```

原始分数就是「这个组一共会有多少个 Pod 在跑」：snapshot 里已经调度的（assigned + assumed），加上这个 placement 里新放下的。

已调度数从哪来？不开 CompositePodGroup 时读本组的 `PodGroupState`；开了以后用 `helper.GetPodGroupStates` 遍历以当前组为根的整棵子树，把每个叶子 PodGroup 的 `ScheduledPodsCount()` 加起来：

```go
// pkg/scheduler/framework/plugins/podgrouppodscount/podgroup_pods_count.go
func (pl *PodGroupPodsCount) getScheduledPodsCount(podGroup fwk.PodGroupInfo) (int, error) {
	if pl.fts.EnableCompositePodGroup {
		count := 0
		for podGroupState, err := range helper.GetPodGroupStates(pl.handle.SnapshotSharedLister(), podGroup.GetKey()) {
			if err != nil {
				return 0, err
			}
			count += podGroupState.ScheduledPodsCount()
		}
		return count, nil
	}
	pgState, err := pl.handle.SnapshotSharedLister().PodGroupStates().Get(podGroup.GetNamespace(), podGroup.GetName())
	if err != nil {
		return 0, err
	}
	return pgState.ScheduledPodsCount(), nil
}
```

这里不会重复计数：每个 placement 试放结束后，`evaluatePlacement` 立刻调用 `placementRevertFns.revert()` 撤销 snapshot 里的 assume，所以打分时 snapshot 里的「已调度」只包含本轮之前就存在的 Pod，本轮新放的 Pod 只在 `ProposedAssignments` 里出现一次。CompositePodGroup 的情况类似，`makeCompositePodGroupAssignments` 用 `successfulLeafResults` 把子树里所有成功叶子的提议合在一起。

注释里那句「reduce the relative difference」说的是：同样是 2 个和 1 个新 Pod 的差距，如果组里已经有 10 个 Pod 在跑，原始分是 12 对 11，归一化后是 100 对 91；只数新 Pod 则是 2 对 1，归一化后是 100 对 50。前者更符合「整组规模差别不大」的事实。

不过在默认配置下，这一项其实很少真正影响结果。TopologyPlacementGenerator 在组里已经有 Pod 调度时只返回那一个域（第 22 篇 3.3 节），候选只剩一个，打分会被跳过。已调度数大于 0 且又有多个候选 placement 的情况，需要有第三方的 PlacementGenerate 插件参与才会出现。

## 四、归一化：除以最大值，而不是 minCount

```go
// pkg/scheduler/framework/plugins/podgrouppodscount/podgroup_pods_count.go
// NormalizePlacementScore normalizes the scores to a range of [MinScore, MaxScore].
// The normalization is based on the maximum count among all candidate placements.
// We purposely do not consider MinCount (the minimum pods required for the group) during normalization
// to avoid large gaps in scores when there are minimal differences in pod counts.
func (pl *PodGroupPodsCount) NormalizePlacementScore(ctx context.Context, state fwk.PodGroupCycleState, podGroup fwk.PodGroupInfo, scores []fwk.PlacementScore) *fwk.Status {
	maxCount := int64(0)

	for _, score := range scores {
		maxCount = max(maxCount, score.Score)
	}

	if maxCount == 0 {
		// TODO: this plugin assumes that the gang scheduling plugin will not let pod groups with 0 scheduled pods through.
		// This can be solved by making it a core algorithm assumption or handling it gracefully in the plugin.
		return fwk.AsStatus(errors.New("no pods from pod group are assigned to any of the candidate placements"))
	}

	for i := range scores {
		scores[i].Score = fwk.MinScore + scores[i].Score*(fwk.MaxScore-fwk.MinScore)/maxCount
	}

	return nil
}
```

`fwk.MinScore` 是 0，`fwk.MaxScore` 是 100，公式简化为 `score × 100 / maxCount`，整数除法向下取整。

为什么不用 `(score - minCount) / (max - minCount)`？假设 `minCount: 4`，三个候选分别能放 6、5、4 个 Pod：

| 放下的 Pod 数 | 现在的公式 | 以 minCount 为下界的公式 |
|---|---|---|
| 6 | 600/6 = 100 | (6-4)/(6-4) = 100 |
| 5 | 500/6 = 83 | (5-4)/(6-4) = 50 |
| 4 | 400/6 = 66 | (4-4)/(6-4) = 0 |

后者把「只差一个 Pod」放大成 50 分的差距，足以压过 NodeResourcesFit 那一侧的任何差别。测试里专门有一个用例「some minimal score that is far from a group of scores located closely」验证归一化不会把这种分布拉开。现在的公式让 PodGroupPodsCount 的分差和 Pod 数的相对差距成正比，另一个打分插件还有发言权。

`maxCount == 0` 的分支值得多看一眼。被打分的 placement 都是「可行」的，可行意味着 PlacementFeasible 返回了 Success。GangScheduling 只有在 `Scheduled >= minCount` 时才返回 Success，而 minCount 至少是 1（Basic 策略也按 1 计），所以 `scheduled + proposed` 至少是 1，`maxCount` 不可能是 0。TODO 注释承认这是一个跨插件的隐含假设：如果有人在 Profile 里关掉了 GangScheduling，`RunPlacementFeasiblePlugins` 没有插件可跑，所有 placement 都会被视为可行，此时一个 0 个 Pod 的 placement 就可能让这里报错，整个组调度以 Error 结束。

## 五、和 NodeResourcesFit 的放置打分一起看

### 5.1 NodeResourcesFit 也是 PlacementScore 插件

开启 TAS 时，NodeResourcesFit 会额外构造一个固定为 MostAllocated 策略的 scorer：

```go
// pkg/scheduler/framework/plugins/noderesources/fit.go
	if fts.EnableTopologyAwareWorkloadScheduling {
		placementScorer, err := getScorer(&config.ScoringStrategy{
			Type:      config.MostAllocated,
			Resources: args.ScoringStrategy.Resources,
		})
		if err != nil {
			return nil, err
		}
		pl.placementScorer = placementScorer
	}
```

它把一个 placement 里所有节点的可分配量、已分配量和本组提议的请求量加总，算一次 MostAllocated：

```go
// pkg/scheduler/framework/plugins/noderesources/resource_allocation.go
func (r *resourceAllocationScorer) scorePlacement(
	ctx context.Context,
	podGroupInfo fwk.PodGroupInfo,
	podGroupAssignments *fwk.PodGroupAssignments,
) (int64, *fwk.Status) {
	requested := make([]int64, len(r.resources))
	// Calculate requests for the pod group pods scheduled for this placement
	for _, assignment := range podGroupAssignments.ProposedAssignments {
		pod := assignment.GetPod()
		podRequests := r.calculatePodResourceRequestList(pod, r.resources)
		for i := range len(r.resources) {
			requested[i] += podRequests[i]
		}
	}
	allocatable := make([]int64, len(r.resources))
	allocated := make([]int64, len(r.resources))
	// Calculate resources on all nodes in this placement
	for _, node := range podGroupAssignments.Nodes {
		_, nodeAllocated, nodeAllocatable := r.calculateNodeAllocatableRequest(ctx, node, requested, nil)
		for i := range r.resources {
			allocatable[i] += nodeAllocatable[i]
			allocated[i] += nodeAllocated[i]
			// requested includes both the already existing pods
			// and the pod group pods that are being scheduled for this placement
			requested[i] += nodeAllocated[i]
		}
	}

	score := r.scorer(requested, allocated, allocatable)
	// ...
	return score, nil
}
```

注意两点：

- 单 Pod 的 Score 默认用 `LeastAllocated`（倾向空闲节点、打散），而放置打分固定用 `MostAllocated`（倾向更满的域、装箱）。这是有意的：组级别希望把大块完整的域留给后来的大组，域内部的节点选择仍然由单 Pod 打分决定；
- 资源种类和权重沿用 `NodeResourcesFitArgs.scoringStrategy.resources`，默认是 cpu、memory 各权重 1。它没有实现 `NormalizePlacementScore`，MostAllocated 的结果本身就在 0~100。

### 5.2 一个完整算例

{{< figure src="/images/posts/sched-plugins/23-score-example.svg" link="/images/posts/sched-plugins/23-score-example.svg" caption="三个可行 rack 的放置打分：PodGroupPodsCount 让放得下全部 6 个 Pod 的 rack 胜出，NodeResourcesFit 在两者之间选更满的那个（点击查看大图）" >}}

设定：`gang.minCount: 4`，队列里 6 个 Pod，每个请求 8 CPU / 32Gi。三个 rack 各有 4 个节点，每个节点 32 CPU / 128Gi，rack 合计 128 CPU / 512Gi。

- rack-a 已占用 72 CPU / 256Gi，四个节点的 CPU 占用分别是 24、24、8、16，内存各 64Gi。按单节点可放数算（CPU 与内存取小）是 1、1、2、2，正好放下 6 个；
- rack-b 已占用 16 CPU / 64Gi，空间充裕，6 个都放得下；
- rack-c 已占用 96 CPU / 384Gi，每个节点只剩 8 CPU / 32Gi，只能放 4 个，刚好满足 minCount。

PodGroupPodsCount：原始分 6、6、4，最大值 6，归一化后是 `600/6 = 100`、`100`、`400/6 = 66`。

NodeResourcesFit（MostAllocated）：

| | CPU | 内存 | 平均 |
|---|---|---|---|
| rack-a | (72+48)×100/128 = 12000/128 = 93 | (256+192)×100/512 = 87 | (93+87)/2 = 90 |
| rack-b | (16+48)×100/128 = 50 | (64+192)×100/512 = 50 | 50 |
| rack-c | (96+32)×100/128 = 100 | (384+128)×100/512 = 100 | 100 |

两个插件权重都是 1，`TotalScore` 分别是 190、150、166，rack-a 胜出。

如果只有 NodeResourcesFit，rack-c 会以 100 分胜出：它装得最满，但 6 个 Pod 里只放下 4 个，另外 2 个要回队等资源，组的规模被压到了下限。如果只有 PodGroupPodsCount，rack-a 和 rack-b 同为 100 分，只能随机二选一，有一半概率选到更空的 rack-b，把一个大块空闲域切碎。两者都开、权重相同，得到的就是「先保证放得多，再在放得一样多的里面挑更满的」。

### 5.3 为什么默认权重是 1

从源码能看到的理由有三条：

**一是和 NodeResourcesFit 对称。** 默认 Profile 里 NodeResourcesFit 在 `MultiPoint` 下的权重是 1。框架计算放置打分权重时，用的是同一份配置：

```go
// pkg/scheduler/framework/runtime/framework.go
	placementScoreWeights, err := getValidScoreWeights(f, reflect.TypeFor[fwk.PlacementScorePlugin](), append(profile.Plugins.PlacementScore.Enabled, profile.Plugins.MultiPoint.Enabled...))
	if err != nil {
		return nil, fmt.Errorf("placement score plugins: %w", err)
	}
	f.placementScorePluginWeight = placementScoreWeights
```

`getValidScoreWeights` 先看 `plugins.placementScore.enabled` 里单独写的权重，没有再用 `multiPoint` 里的权重，权重为 0 时按 1 处理。所以 NodeResourcesFit 的放置打分权重默认也是 1，两个插件都归一化到 0~100，等权相加。PodGroupPodsCount 的分差和 Pod 数的相对差成正比（少一个 Pod 大约少 `100/max` 分），MostAllocated 的分差和资源利用率差成正比，权重相同时，「少放一个 Pod」和「利用率低十几个百分点」大致相当。

**二是 MultiPoint 权重会同时影响两个扩展点。** 如果有人为了偏向装箱，把 NodeResourcesFit 在 `multiPoint` 里的权重调高，它的单 Pod Score 权重和放置打分权重会一起变。想单独调整放置打分，要在 `plugins.placementScore.enabled` 里写一项带权重的 NodeResourcesFit 或 PodGroupPodsCount，那里的权重优先于 `multiPoint`。

**三是溢出检查。** `getValidScoreWeights` 会检查 `Σ weight × MaxScore` 不超过 `MaxTotalScore`，单 Pod 打分和放置打分各算各的，放置打分目前只有两个插件，权重 1 远不会触及上限。

### 5.4 RunPlacementScorePlugins 的三段并行

```go
// pkg/scheduler/framework/runtime/framework.go
		// Run ScorePlacement method for each placement in parallel.
		f.Parallelizer().Until(ctx, len(podGroupAssignments), func(index int) {
			// ...
			for _, pl := range plugins {
				// ...
				s, status := f.runPlacementScorePlugin(ctx, pl, placementStates[index], podGroupInfo, pga)
				// ...
			}
		}, metrics.PlacementScore)
	// ...
	// Run NormalizePlacementScore method for each PlacementScorePlugin in parallel.
	// ...
	// Apply score weight for each PlacementScorePlugin in parallel,
	// and then, build allPlacementPluginScores.
	f.Parallelizer().Until(ctx, len(podGroupAssignments), func(index int) {
		// ...
		for i, pl := range plugins {
			weight := f.placementScorePluginWeight[pl.Name()]
			placementScoreList := pluginToPlacementScores[pl.Name()]
			score := placementScoreList[index].Score

			if score > fwk.MaxScore || score < fwk.MinScore {
				err := fmt.Errorf("plugin %q returns an invalid score %v, it should in the range of [%v, %v] after normalizing", pl.Name(), score, fwk.MinScore, fwk.MaxScore)
				errCh.SendWithCancel(err, cancel)
				return
			}
			weightedScore := score * int64(weight)
			// ...
			placementPluginScores.TotalScore += weightedScore
		}
		allPlacementPluginScores[index] = placementPluginScores
	}, metrics.PlacementScore)
```

结构和单 Pod 的 `RunScorePlugins` 一致（代码注释也说两者「mostly duplicates」）：按 placement 并行打分、按插件并行归一化、按 placement 并行加权。分数越界会直接让整个组调度以 Error 结束。这就是 PodGroupPodsCount 必须实现归一化的原因：原始分是 Pod 数，一个 200 Pod 的组原始分就是 200，不归一化会触发这个越界错误。

## 六、边界情况

**打分插件看不到 minCount 以外的「组完整度」。** PodGroupPodsCount 只数 Pod，不管 Pod 的角色。一个由 1 个 launcher 和 N 个 worker 组成的组（同一个 PodGroup），「放下 launcher + 3 个 worker」和「放下 4 个 worker」得分相同。需要区分角色时，应该拆成多个 PodGroup，用 CompositePodGroup 组织。

**整组不可行的 placement 不参与打分。** 只有试放结果是 Success 的 placement 才进入 `successfulResults`。PodGroupPodsCount 不会「奖励」一个放了 3 个 Pod 但没达到 minCount=4 的 placement，那个 placement 在 PlacementFeasible 阶段就被淘汰了。

**评估到的 placement 数有限。** 第 22 篇讲过，有打分插件时调度器只找 `numFeasiblePlacementsToFind` 个可行 placement 就停。打分是在这个子集里选最优，而不是全局最优。被随机打乱后没轮到评估的 rack，即使能放下更多 Pod 也不会被选中。

**打分错误会让整组失败。** `ScorePlacement` 里读 `PodGroupStates` 出错（比如 snapshot 里找不到组状态）会返回 Error，`findBestPodGroupPlacement` 把它向上传，组的结果是 Error 而不是降级成随机选一个。

## 七、发散：放置打分和单 Pod 打分的分工

kube-scheduler 现在有两层打分：

| | 单 Pod Score | PlacementScore |
|---|---|---|
| 对象 | 一个 Pod 的候选节点 | 一个组的候选 placement |
| 输入 | `NodeInfo` | `PodGroupAssignments`（placement + 提议的 Pod→节点映射） |
| 默认插件 | TaintToleration、NodeAffinity、NodeResourcesFit（LeastAllocated）等 | PodGroupPodsCount、NodeResourcesFit（MostAllocated） |
| 选择方式 | `selectHost`，同分随机 | `findBestPlacement`，同分随机 |
| 剪枝 | `percentageOfNodesToScore` | `percentageOfPlacementsToScore` |

一个组的最终落点由两层共同决定：PlacementScore 选出 rack，rack 内每个 Pod 的节点由单 Pod Score 决定。默认配置下两层的资源倾向相反，组之间装箱、组内部打散。如果业务希望组内也尽量紧凑（比如减少跨节点通信），需要把单 Pod 的 NodeResourcesFit 策略改成 MostAllocated，但这会影响所有 Pod 的调度行为，最好放在单独的 Profile 里。

另一个值得关注的设计是放置打分对 DRA 的预留：`ProposedAssignment` 接口提供了 `GetCycleState()`，打分插件可以读到每个 Pod 在试放时写下的 CycleState，比如 DynamicResources 为它挑中的设备。将来按「设备拓扑是否连续」给 placement 打分的插件，可以基于这份数据实现，而不需要重新做一遍分配。

## 八、小结

- PodGroupPodsCount 只在 `TopologyAwareWorkloadScheduling` 打开、且有多个可行 placement 时被调用；原始分是「已调度 Pod 数 + 本 placement 提议的 Pod 数」。
- 归一化用 `score × 100 / maxCount`，有意不以 minCount 为下界，避免一个 Pod 的差距被放大成几十分；`maxCount == 0` 依赖 GangScheduling 保证不会发生。
- 默认权重 1，与 NodeResourcesFit 的放置打分（固定 MostAllocated、同样权重 1）等权相加：先保证放得多，再在放得一样多的候选里挑装得更满的。
- 放置打分的权重可以在 `plugins.placementScore.enabled` 里单独设置，优先于 `multiPoint` 里的权重。

一个组的 Pod 选定了节点，接下来就进入各自的绑定周期，由 [DefaultBinder](/posts/sched-plugin-24-default-binder/) 把 `spec.nodeName` 写回 API Server。
