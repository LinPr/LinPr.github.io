---
title: 'scheduler-plugins 18 [in-tree] DefaultPreemption：用模拟换一个提名节点'
date: 2026-11-19T11:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "preemption", "PodDisruptionBudget", "优先级"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "DefaultPreemption 是默认 Profile 里唯一会驱逐别人的插件。本文沿 PostFilter 的完整调用链拆解通用的 preemption.Evaluator：资格检查、只在 Unschedulable 节点上找候选、按比例抽样、SelectVictimsOnNode 的「全移除再逐个放回」、PDB 违反计数、候选节点的五级排序、nominatedNodeName 的写入与清空，以及异步抢占如何用 PreEnqueue 挡住抢占者。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `DefaultPreemption` 实现的扩展点（PreEnqueue、PostFilter、PodGroupPostFilter）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/18.svg" link="/images/posts/sched-plugins/callstack/18.svg" caption="DefaultPreemption 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

前面十几篇读的都是「判断」：节点能不能放、放哪里更好。[上一篇的 DeferredPodScheduling](/posts/sched-plugin-17-deferred-pod-scheduling/) 结尾已经提到，它自己不腾地方，腾地方要靠抢占。本篇读的 `DefaultPreemption` 是默认 Profile 里唯一会改变集群现状的插件：所有节点都没通过 Filter 时，它在内存里模拟驱逐低优先级 Pod，找到一个可行的节点，然后真的去删 Pod，并把这个节点记进抢占者的 `status.nominatedNodeName`。本文基于 Kubernetes master 提交 `aabdf5a2`。

它在默认 Profile 的 MultiPoint 列表里没有权重，紧跟在 `DynamicResources` 之后（[第 15 篇](/posts/sched-plugin-15-dynamic-resources/)解释过为什么 DRA 要排在前面：先释放闲置 claim，再考虑驱逐 Pod）。实现了 PostFilter 和 PreEnqueue 两个扩展点，`GenericWorkload` 开启时还有 PodGroupPostFilter。插件本身不受 gate 控制，默认启用；它的行为由 `SchedulerAsyncPreemption`（1.33 起 Beta，默认开）和 `GenericWorkload` 等 gate 调整。

插件只有 500 行，真正的逻辑在 `pkg/scheduler/framework/preemption/` 这个通用包里。这个包的设计目标就是让 out-of-tree 抢占插件复用同一套流程，只替换其中几步，后面 out-of-tree 部分的 `PreemptionToleration`、`CrossNodePreemption` 都是这么做的。

## 一、什么时候会触发抢占

触发条件有三个，缺一不可：

1. 调度周期里所有节点都失败，`SchedulePod` 返回 `FitError`；
2. Profile 里注册了 PostFilter 插件，并且排在前面的 PostFilter（如 `DynamicResources`）没有返回 `Success`；
3. 至少有一个节点的失败状态是 `Unschedulable`，而不是 `UnschedulableAndUnresolvable`。

第三条是最容易被忽视的。一个典型的能触发抢占的场景：

```yaml {linenos=inline}
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: online
value: 1000
preemptionPolicy: PreemptLowerPriority   # 默认值
---
apiVersion: v1
kind: Pod
metadata:
  name: api-7
spec:
  priorityClassName: online
  containers:
  - name: api
    image: api:latest
    resources:
      requests:
        cpu: "4"
```

如果所有节点都是因为 CPU 不足（`NodeResourcesFit` 返回 `Unschedulable`）而失败，且节点上有优先级低于 1000 的 Pod，抢占就有机会。反过来，如果失败原因是节点亲和不匹配、污点不容忍、DRA 设备分配失败——这些插件返回的都是 `UnschedulableAndUnresolvable`——驱逐谁都没用，`DefaultPreemption` 连模拟都不会做。

所以抢占的第一步不是找受害者，而是筛掉那些「驱逐了也没用」的节点。这一点贯穿下一节的候选筛选。

## 二、插件结构

插件字段不多，但每一个都对应一种扩展方式：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
type DefaultPreemption struct {
	fh   fwk.Handle
	fts  feature.Features
	args config.DefaultPreemptionArgs

	Executor          fwk.PreemptionExecutor
	Evaluator         *preemption.Evaluator
	pgLister          fwk.PodGroupLister
	podGroupEvaluator podGroupEvaluator

	IsEligiblePod IsEligiblePodFunc

	MoreImportantVictim MoreImportantVictimFunc
}
```

几个导出字段说明了它的扩展方式：`New` 返回的是具体类型 `*DefaultPreemption` 而不是 `fwk.Plugin`，源码注释写着「The plugin type is retained to allow modification」。自建调度器可以包一层 `New`，替换下面两个字段，不必复制整个插件：

1. `IsEligiblePod`：额外的受害者资格过滤（第 12 行）；
2. `MoreImportantVictim`：受害者排序（第 14 行）。

`Executor` 来自 `fh.PreemptionManager().Executor()`，是整个调度器共享的一个实例，负责真正的删除动作和异步抢占的簿记。`Evaluator` 是 `preemption.NewEvaluator(Name, fh, &pl, pl.fts)`——注意第三个参数传的是插件自己，`Evaluator` 通过 `preemption.Interface` 回调插件的五个方法：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/preemption.go
type Interface interface {
	GetOffsetAndNumCandidates(nodes int32) (int32, int32)
	CandidatesToVictimsMap(candidates []fwk.PreemptionCandidate) map[string]*extenderv1.Victims
	PodEligibleToPreemptOthers(ctx context.Context, pod *v1.Pod, nominatedNodeStatus *fwk.Status) (bool, string)
	SelectVictimsOnNode(ctx context.Context, cycleState fwk.CycleState, preemptor *v1.Pod, nodeInfo fwk.NodeInfo, allPossibleVictims []*DomainVictim, pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, int, *fwk.Status)
	OrderedScoreFuncs(ctx context.Context, nodesToVictims map[string]*extenderv1.Victims) []func(node string) int64
}
```

流程结构在 `Evaluator` 里，策略在 `Interface` 里。这是一个典型的模板方法模式。

### 2.1 参数

两个参数，都服务于第五节的候选数量计算：

```go {linenos=inline}
// pkg/scheduler/apis/config/v1/defaults.go
func SetDefaults_DefaultPreemptionArgs(obj *configv1.DefaultPreemptionArgs) {
	if obj.MinCandidateNodesPercentage == nil {
		obj.MinCandidateNodesPercentage = ptr.To[int32](10)
	}
	if obj.MinCandidateNodesAbsolute == nil {
		obj.MinCandidateNodesAbsolute = ptr.To[int32](100)
	}
}
```

校验规则：百分比在 `[0, 100]`，绝对值不小于 0，两者不能同时为 0。它们的用法在第五节讲。

到这里，插件的整体结构已经清楚了。下面按一次抢占的实际顺序走：入口、资格检查、找候选、模拟受害者、排序、执行。

## 三、PostFilter 的入口与返回值语义

入口只转发一次调用，其余都在 `Evaluator` 里：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
func (pl *DefaultPreemption) PostFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, m fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	defer func() {
		metrics.PreemptionAttempts.Inc()
	}()

	result, status := pl.Evaluator.Preempt(ctx, state, pod, m)
	msg := status.Message()
	if len(msg) > 0 {
		return result, fwk.NewStatus(status.Code(), "preemption: "+msg)
	}
	return result, status
}
```

插件只是给消息加了一个 `preemption: ` 前缀（第 9–11 行），所以 `kubectl describe pod` 里看到的 `preemption: 0/5 nodes are available: ...` 就是从这里来的。

`Evaluator.Preempt` 的注释列出了四种返回组合，它们决定了 `nominatedNodeName` 的命运：

| 返回 | 含义 | 对 nominatedNodeName |
| --- | --- | --- |
| `<nil, Error>` | 偶发错误 | 保持不变 |
| `<nil, Unschedulable>` | 预期之内，比如在等受害者退出 | 保持不变 |
| `<非 nil, Unschedulable>` | 抢占也救不了 | 按 result 处理，通常是清空 |
| `<非 nil, Success>` | 找到了 | 写入选中的节点 |

返回值经 `schedulingAlgorithm` 包成 `ScheduleResult.nominatingInfo`，最终在 `handleSchedulingFailure` 里写进队列的 nominator 和 API（第 2–9 行）：

```go {linenos=inline}
// pkg/scheduler/schedule_one.go
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.AddNominatedPod(logger, nominatedPodInfo, nominatingInfo)
	}
	// ...
	if err := updatePod(ctx, sched.client, podFwk.APICacher(), pod, &v1.PodCondition{
		Type:               v1.PodScheduled,
		// ...
	}, nominatingInfo); err != nil {
```

注意即使抢占成功，这个调度周期的结果仍然是「失败」，Pod 回到队列。抢占者不会在本周期绑定，它要等受害者真正退出。

{{< figure src="/images/posts/sched-plugins/18-preemption-flow.svg" caption="Evaluator.Preempt 的主路径与提前退出；右列标出每种退出对 nominatedNodeName 的影响（点击查看大图）" link="/images/posts/sched-plugins/18-preemption-flow.svg" >}}

返回值这一层说清了「结果怎么记」，下一层说「谁有资格抢占」。

## 四、资格检查：PodEligibleToPreemptOthers

`evaluate` 的第一步是从 PodLister 重新取一次 Pod，然后问插件「这个 Pod 有没有资格抢占」：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
func (pl *DefaultPreemption) PodEligibleToPreemptOthers(_ context.Context, pod *v1.Pod, nominatedNodeStatus *fwk.Status) (bool, string) {
	if pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == v1.PreemptNever {
		return false, "not eligible due to preemptionPolicy=Never."
	}

	nodeInfos := pl.fh.MutableSnapshotSharedLister().NodeInfos()
	nomNodeName := pod.Status.NominatedNodeName
	if len(nomNodeName) > 0 {
		// If the pod's nominated node is considered as UnschedulableAndUnresolvable by the filters,
		// then the pod should be considered for preempting again.
		if nominatedNodeStatus.Code() == fwk.UnschedulableAndUnresolvable {
			return true, ""
		}

		if nodeInfo, _ := nodeInfos.Get(nomNodeName); nodeInfo != nil {
			for _, p := range nodeInfo.GetPods() {
				victim := preemption.NewPodVictim(p)
				if pl.isPreemptionAllowed(nodeInfo, victim, pod) && preemption.PodTerminatingByPreemption(p.GetPod()) {
					// There is a terminating pod on the nominated node.
					return false, "not eligible due to a terminating pod on the nominated node."
				}
			}
		}
	}
	return true, ""
}
```

两条拒绝规则：

**`preemptionPolicy: Never`**（第 3–5 行）。这种 Pod 在队列里仍然按高优先级排在前面，但永远不驱逐别人。适合「我很重要、想先被调度，但不愿意为此打断别人」的批处理任务。`spec.preemptionPolicy` 由 PriorityClass 的同名字段在准入时填入。

**提名节点上还有因抢占而终止的 Pod**（第 16–25 行）。判断「因抢占而终止」用的是 `PodTerminatingByPreemption`：`DeletionTimestamp` 非空，并且有一个 `DisruptionTarget=True`、`reason=PreemptionByScheduler` 的 condition。这个 condition 正是第九节 Executor 在删除受害者之前打上去的。上一轮抢占的受害者还在优雅退出，此时再抢占一次只会多杀一批 Pod。返回 `<nil, Unschedulable>`，提名保持不变，Pod 等受害者的删除事件。

例外是提名节点在本轮 Filter 中是 `UnschedulableAndUnresolvable`，比如节点被打了 `NoSchedule` 污点。等受害者退出也没用了，允许重新抢占。

资格通过之后，开始找候选节点。

## 五、候选节点：只看 Unschedulable，按比例抽样

候选不是所有失败的节点，只是其中状态为 `Unschedulable` 的那部分（第 5 行）。`findCandidates` 分四步取出候选：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/preemption.go
func (ev *Evaluator) findCandidates(ctx context.Context, state fwk.CycleState, allNodes []fwk.NodeInfo, pod *v1.Pod, m fwk.NodeToStatusReader) ([]fwk.PreemptionCandidate, *framework.NodeToStatus, error) {
	// ...
	// Get a list of nodes with failed predicates (Unschedulable) that may be satisfied by removing pods from the node.
	potentialNodes, err := m.NodesForStatusCode(ev.Handler.MutableSnapshotSharedLister().NodeInfos(), fwk.Unschedulable)
	if err != nil {
		return nil, nil, err
	}
	if len(potentialNodes) == 0 {
		logger.V(3).Info("Preemption will not help schedule pod on any node", "pod", klog.KObj(pod))
		return nil, framework.NewDefaultNodeToStatus(), nil
	}

	pdbs, err := getPodDisruptionBudgets(ev.PdbLister)
	if err != nil {
		return nil, nil, err
	}

	offset, candidatesNum := ev.GetOffsetAndNumCandidates(int32(len(potentialNodes)))
	return ev.DryRunPreemption(ctx, state, pod, potentialNodes, pdbs, offset, candidatesNum)
}
```

1. 用 `NodesForStatusCode` 取出状态为 `Unschedulable` 的节点，只有这些节点上的 Pod 移除后才可能让抢占者放得下（第 5 行）；
2. 这个集合为空时，直接返回空的 `NodeToStatus`，本轮不模拟任何节点（第 9–12 行）；
3. 从 `PdbLister` 读出全部 PDB，供后面的受害者筛选和违反计数使用（第 14–17 行）；
4. 用 `GetOffsetAndNumCandidates` 算出抽样的起点和候选数，把节点、PDB 和这两个数字一起交给 `DryRunPreemption`（第 19–20 行）。

`NodesForStatusCode` 要处理一个细节：`NodeToStatus` 只显式记录了被评估过的节点，没出现在 map 里的节点用 `AbsentNodesStatus` 表示。它的取值有两种情况：

1. PreFilter 失败时，所有节点都是 absent，状态就是 PreFilter 的状态；
2. PreFilter 返回了 `PreFilterResult` 缩小节点集合时，被排除的节点被设为 `UnschedulableAndUnresolvable`。

所以 PreFilter 阶段的拒绝和缩小，都会让抢占范围随之缩小。

候选数量：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
func (pl *DefaultPreemption) calculateNumCandidates(numNodes int32) int32 {
	n := (numNodes * pl.args.MinCandidateNodesPercentage) / 100
	if n < pl.args.MinCandidateNodesAbsolute {
		n = pl.args.MinCandidateNodesAbsolute
	}
	if n > numNodes {
		n = numNodes
	}
	return n
}
```

`numNodes` 是**潜在节点数**，不是集群节点总数。按默认参数：

| 潜在节点数 | N×10/100 | 与 100 取大 | 与 N 取小 |
| --- | --- | --- | --- |
| 60 | 6 | 100 | 60 |
| 400 | 40 | 100 | 100 |
| 3000 | 300 | 300 | 300 |

也就是说：

1. 潜在节点不足 1000 个时，至少要凑齐 100 个候选（或者全部）才停；
2. 超过 1000 个时凑够 10%。

起点 `offset` 是 `rand.Int31n(numNodes)`，从随机位置开始环形遍历，避免每次都在同一批节点上驱逐。

这两个参数名里的「Min」容易引起误解：它们是**提前停止的阈值**，不是上限。模拟找到这么多候选就可以停，但找到多少都会参与第七节的排序，排序结果才决定选谁。调小它们能显著缩短大集群的抢占耗时，代价是选出的节点不一定是全局最优。

候选节点定了，接下来是逐节点模拟。

## 六、DryRunPreemption 与 SelectVictimsOnNode

### 6.1 并行模拟

模拟对每个候选节点并行跑一遍，通过的进候选列表：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/preemption.go
	checkNode := func(i int) {
		nodeInfo := potentialNodes[(int(offset)+i)%len(potentialNodes)]
		// ...
		allPossibleVictims, err := ev.GetVictimsOnNode(ctx, nodeInfo)
		// ...
		pods, numPDBViolations, status := ev.SelectVictimsOnNode(ctx, state.Clone(), pod, nodeInfo.Snapshot(), allPossibleVictims, pdbs)
		// ...
		if status.IsSuccess() && (len(pods) != 0 || ev.hasInterestedPreemptExtender(pod)) {
			victims := extenderv1.Victims{
				Pods:             pods,
				NumPDBViolations: int64(numPDBViolations),
			}
			c := newCandidate(&victims, nodeInfo.Node().Name, nil, nodeInfo.Node().Name)
			if numPDBViolations == 0 {
				nonViolatingCandidates.add(c)
			} else {
				violatingCandidates.add(c)
			}
			nvcSize, vcSize := nonViolatingCandidates.size(), violatingCandidates.size()
			if nvcSize > 0 && nvcSize+vcSize >= candidatesNum {
				cancel()
			}
			return
		}
		// ...
	}
	fh.Parallelizer().Until(ctx, len(potentialNodes), checkNode, ev.PluginName)
```

几个细节：

1. 每个节点拿到的是 `state.Clone()` 和 `nodeInfo.Snapshot()`，互不干扰。这就是各插件 `CycleState.Clone` 必须深拷贝的原因——抢占模拟会在副本上调用 `RemovePod`/`AddPod`。
2. 提前停止的条件是「**至少有一个不违反 PDB 的候选**，且总数达到 `candidatesNum`」（第 20–22 行）。如果找到的全是违反 PDB 的候选，模拟会继续，直到遍历完或找到一个不违反的。
3. 两个候选列表都是容量为 `candidatesNum` 的定长数组，用 `atomic.AddInt32` 分配下标，溢出的直接丢弃。并行执行时可能略微超出阈值后才 cancel。
4. 模拟成功但受害者为空（不驱逐任何 Pod 就放得下）不算候选，状态被改成 `UnschedulableAndUnresolvable`——这种情况说明 Filter 失败的原因与本节点上的 Pod 无关。唯一的例外是存在支持抢占的 extender，它可能自己添加受害者。

`GetVictimsOnNode` 按 gate 分两种情况：

1. `GenericWorkload` 关闭时，把节点上每个 Pod 包成一个 `PodKeyType` 受害者；
2. 开启时，如果 Pod 所属的 PodGroup（或其祖先）的 disruption mode 是 `All`，就把整个组在**全集群**范围内已调度的 Pod 聚成一个受害者，要驱逐就整组驱逐。

### 6.2 SelectVictimsOnNode：先全移除，再逐个放回

这是整个抢占算法的核心。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
	var potentialVictims []*preemption.DomainVictim
	for _, victim := range allPossibleVictims {
		if pl.isPreemptionAllowedAcrossAllVictimNodes(victim, preemptor) {
			potentialVictims = append(potentialVictims, victim)
		}
	}

	// No preemption victims found for incoming preemptor, and so we don't need to evaluate the node again since its state didn't change.
	if len(potentialVictims) == 0 {
		return nil, 0, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "No preemption victims found for incoming pod")
	}

	// As the first step, remove all victims eligible for preemption from the node.
	for _, victim := range potentialVictims {
		// ...
		if err := removeVictim(victim); err != nil {
			return nil, 0, fwk.AsStatus(err)
		}
	}

	// If the preemptor does not fit after removing all the eligible victims,
	// we are almost done and this node is not suitable for preemption. The only
	// ...
	if status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo); !status.IsSuccess() {
		return nil, 0, status
	}
```

第一步（第 2–7 行）：筛出优先级严格低于抢占者、且 `IsEligiblePod` 通过的受害者。**同优先级不会互相抢占**。

第二步：把它们全部从 NodeInfo 移除（第 15–20 行），每移除一个都调用 `RunPreFilterExtensionRemovePod`，让 `PodTopologySpread`、`InterPodAffinity`、`NodePorts` 这类插件更新各自的 CycleState。

第三步：跑一次完整的 Filter（第 25–27 行）。全部移除后还放不下，这个节点就没救了，直接返回。注释里特别说明：如果抢占者对某个受害者有 Pod 亲和，移除受害者反而会让 Filter 失败，这种情况出于性能考虑不支持。

`RunFilterPluginsWithNominatedPods` 不是普通的 `RunFilterPlugins`，它跑两遍，两次都通过才算通过：

1. 把提名到本节点、优先级不低于抢占者的其他 Pod 临时加进来跑一遍；
2. 全部通过后，再不加它们跑一遍。

这保证抢占者不会抢走为更高优先级 Pod 腾出的位置。

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
	sort.Slice(potentialVictims, func(i, j int) bool {
		return pl.MoreImportantVictim(potentialVictims[i], potentialVictims[j])
	})

	violatingVictims, nonViolatingVictims := preemption.FilterVictimsWithPDBViolation(potentialVictims, pdbs)
	var victims []*preemption.DomainVictim
	reprieveVictim := func(v *preemption.DomainVictim) (bool, error) {
		if err := addVictim(v); err != nil {
			return false, err
		}

		status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo)
		fits := status.IsSuccess()
		if !fits {
			if err := removeVictim(v); err != nil {
				return false, err
			}
			victims = append(victims, v)
			// ...
		}

		return fits, nil
	}

	numViolatingVictim := 0
	for _, violatingVictim := range violatingVictims {
		if fits, err := reprieveVictim(violatingVictim.Victim); err != nil {
			return nil, 0, fwk.AsStatus(err)
		} else if !fits {
			numViolatingVictim += violatingVictim.ViolateCount
		}
	}

	// Now we try to reprieve non-violating victims.
	for _, v := range nonViolatingVictims {
		if _, err := reprieveVictim(v); err != nil {
			return nil, 0, fwk.AsStatus(err)
		}
	}
```

第四步：按重要性从高到低排序，按 PDB 分成两组（第 2–6 行），**先尝试放回违反 PDB 的那组**，再放回不违反的。每放回一个跑一次 Filter，结果有两种：

1. 放得下就赦免；
2. 放不下就再移除并记为受害者。

{{< figure src="/images/posts/sched-plugins/18-select-victims.svg" caption="SelectVictimsOnNode 算例：p-b 比 p-a 更重要，但 p-a 受 PDB 保护，先被尝试放回并赦免，最终受害者是 p-b 和 p-c（点击查看大图）" link="/images/posts/sched-plugins/18-select-victims.svg" >}}

这是一个贪心算法，复杂度是 O(受害者数 × Filter 开销)。它得到的不是「最少受害者」的最优解，而是在「尽量保留重要的、尽量保留受 PDB 保护的」这个偏好下的一个可行解。

为什么不直接「从最不重要的开始逐个移除，直到放得下」？因为那样每一步都要判断「还差多少」，而 Filter 插件只回答「放得下/放不下」，不回答「差多少」。全移除再放回的方向只需要二值判断，对任何 Filter 插件都适用。

### 6.3 受害者的重要性

两个受害者谁更该被保留，由这个函数按五个级别依次比较：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/util.go
func MoreImportantVictim(vi1, vi2 Victim) bool {
	if vi1.Priority() != vi2.Priority() {
		return vi1.Priority() > vi2.Priority()
	}

	rank1 := victimRank(vi1)
	rank2 := victimRank(vi2)

	if rank1 != rank2 {
		return rank1 > rank2
	}

	if len(vi1.Pods()) != len(vi2.Pods()) {
		return len(vi1.Pods()) > len(vi2.Pods())
	}

	t1, t2 := vi1.EarliestStartTime(), vi2.EarliestStartTime()
	if t1 != nil && t2 != nil && !t1.Equal(t2) {
		return t1.Before(t2)
	}

	return podIdentityLess(vi1, vi2)
}
```

优先级 → 类型（CompositePodGroup > PodGroup > Pod）→ 成员数 → 最早启动时间（跑得越久越重要）→ UID。最后一级 UID 比较保证排序是确定性的，同样的输入总得到同样的受害者。

### 6.4 PDB 违反计数

PDB 的余量是这样一个个扣下去的：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/util.go
	podIsViolating := func(pod *v1.Pod) bool {
		for i, pdb := range pdbs {
			if pdb.Namespace != pod.Namespace {
				continue
			}
			// ...
			if !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}

			// Existing in DisruptedPods means it has been processed in API server,
			// we don't treat it as a violating case.
			if _, exist := pdb.Status.DisruptedPods[pod.Name]; exist {
				continue
			}
			// Only decrement the matched pdb when it's not in its <DisruptedPods>;
			// otherwise we may over-decrement the budget number.
			pdbsAllowed[i]--
			// We have found a matching PDB.
			if pdbsAllowed[i] < 0 {
				return true
			}
		}

		return false
	}
```

每个 PDB 的余量从 `status.disruptionsAllowed` 开始，受害者按重要性降序逐个扣减，扣到负数的 Pod 才算违反（第 19–22 行）。这意味着，同一个 PDB 下有两个受害者、余量为 1 时：

1. 更重要的那个先扣掉余量，不算违反；
2. 不那么重要的那个被判为违反，于是会被优先尝试赦免。

最终结果倾向于驱逐更重要的那个——这是贪心顺序带来的一个副作用。

已经在 `status.disruptedPods` 里的 Pod 不扣减（第 14–16 行），因为它已经被 Eviction API 处理过、计入过余量了。这个 map 的写入和清理分属两个组件：

1. 写入：apiserver 处理 Eviction 请求时（`pkg/registry/core/pod/storage/eviction.go`），先把 `status.disruptionsAllowed` 减 1，再把 Pod 名和当前时间写进 `disruptedPods`，一起 `UpdateStatus` 到 PDB；
2. 清理：disruption 控制器重算 PDB 时（`pkg/controller/disruption/disruption.go` 的 `buildDisruptedPodMap`），Pod 已开始删除或已不存在就移出 map，超过 `DeletionTimeout`（2 分钟）仍未删除的也移出，并记一条 `NotDeleted` 事件。

所以 Pod 出现在 `disruptedPods` 里，说明它那一份余量已经从 `disruptionsAllowed` 里扣过，调度器再扣就重复了。

PodGroup 受害者只要有一个成员违反，整个组都算违反，`ViolateCount` 是违反成员的个数。

到这一步，每个候选节点都有了一份「要驱逐谁、违反几个 PDB」的答案。如果候选不止一个，就要在这些答案之间做选择。

## 七、选一个节点：五级排序

多个候选节点时，用五级打分逐级淘汰（第 2–14 行的 `scoreFuncs`），第一个分出胜负的级别决定结果：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/preemption.go
		scoreFuncs = []func(string) int64{
			// A node with a minimum number of PDB is preferable.
			minNumPDBViolatingScoreFunc,
			// A node with a minimum highest priority victim is preferable.
			minHighestPriorityScoreFunc,
			// A node with the smallest sum of priorities is preferable.
			minSumPrioritiesScoreFunc,
			// A node with the minimum number of pods is preferable.
			minNumPodsScoreFunc,
			// A node with the latest start time of all highest priority victims is preferable.
			latestStartTimeScoreFunc,
			// If there are still ties, then the first Node in the list is selected.
		}
```

逐级淘汰：每一级只保留得分最高的节点，剩一个就返回，最后仍并列则取第一个。插件的 `OrderedScoreFuncs` 返回 `nil` 时用这套默认规则，out-of-tree 插件可以返回自己的规则列表。

| 级别 | 规则 | 得分 |
| --- | --- | --- |
| 1 | PDB 违反数最少 | `-NumPDBViolations` |
| 2 | 受害者中最高优先级最低 | `-Pods[0] 的优先级` |
| 3 | 受害者优先级之和最小 | `-Σ(priority + MaxInt32 + 1)` |
| 4 | 受害者数量最少 | `-len(Pods)` |
| 5 | 最高优先级受害者中最早启动时间最晚 | `GetEarliestPodStartTime().UnixNano()` |

第 2 级直接取 `Pods[0]`，依赖 `SelectVictimsOnNode` 返回的受害者已按重要性降序排列。函数末尾有一次额外排序：当违反和不违反 PDB 的受害者都存在时，两组拼接后顺序会乱，需要重排。

第 3 级的 `+ MaxInt32 + 1` 看一个算例。设两个节点都通过了前两级，最高优先级都是 -5：

| 节点 | 受害者优先级 | 不加偏移的和 | 加偏移的和 |
| --- | --- | --- | --- |
| X | -5, -10 | -15 | 2×2147483648 − 15 = 4294967281 |
| Y | -5 | -5 | 2147483648 − 5 = 2147483643 |

不加偏移，X 的和更小，会选 X，但 X 要多杀一个 Pod。加了偏移后每个受害者都贡献一个正数，受害者越多和越大，Y 胜出。源码注释原话是：避免「有几个负优先级 Pod 的节点」压过「只有更少同样负优先级 Pod 的节点」。

第 5 级是「尽量驱逐刚启动的 Pod」：节点的分数是最高优先级那批受害者中最早的启动时间，分数越大（越晚）越好。

节点选出来了，接下来是真正动手删。

## 八、extender 的介入

`callExtenders` 在排序之前执行，规则有三条：

1. 只有 `SupportsPreemption()` 且 `IsInterested(pod)` 的 extender 参与；
2. 它们通过 `ProcessPreemption` 拿到 `节点 → 受害者` 的 map，可以删掉节点、改受害者列表；
3. 返回空受害者的节点会被丢弃，除非输入时它本来就是空的占位（为第 6.1 节说的「有支持抢占的 extender 时保留空受害者候选」而设计）。

extender 处理完，剩下的就是执行。

## 九、执行：Executor

### 9.1 删除一个受害者

删除之前，先看受害者处在哪个阶段：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/executor.go
		if waitingPod := e.fh.GetWaitingPod(victim.UID); waitingPod != nil {
			if waitingPod.Preempt(pluginName, "preempted") {
				// ...
				preemptedInMemory = true
			}
		} else if podInPreBind := e.fh.GetPodInPreBind(victim.UID); podInPreBind != nil {
			// If the victim is in the preBind cancel the binding process.
			if podInPreBind.CancelPod(fmt.Sprintf("preempted by %s", pluginName)) {
				// ...
				preemptedInMemory = true
			}
			// ...
		}
		if !preemptedInMemory {
			condition := &v1.PodCondition{
				Type:               v1.DisruptionTarget,
				// ...
				Status:             v1.ConditionTrue,
				Reason:             v1.PodReasonPreemptionByScheduler,
				Message:            fmt.Sprintf("%s: preempting to accommodate a higher priority %s", preemptor.SchedulerName(), preemptor.Type()),
			}
			// ...
			if err := schedutil.DeletePod(ctx, fh.ClientSet(), victim); err != nil {
```

受害者可能处在三种状态：

1. 在 Permit 等待中（比如 Gang 调度还没凑齐）：调用 `waitingPod.Preempt`，它的绑定周期会以拒绝结束，Pod 回队列。不发任何 API 请求。
2. 在 PreBind 中：`CancelPod` 取消它的 PreBind context。绑定周期在 PreBind 之后会调用 `MarkPrebound`，如果已被取消就返回错误，进入失败处理。只有取消成功（还没 `MarkPrebound`）才算内存中抢占。
3. 已经在运行：先 patch 一个 `DisruptionTarget` condition，再 `DELETE`。顺序不能反：condition 是第四节 `PodTerminatingByPreemption` 的判断依据，也让 Job 控制器等组件能区分「被调度器抢占」和普通删除。

删除走的是普通的 Pod DELETE，不是 Eviction API。**PDB 在这里不会拦截**，它只影响第六、七节的选择。

### 9.2 清理低优先级 Pod 的提名

受害者处理完，还要处理被抢占节点上已经存在的提名：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/executor.go
	nominatedPods := getLowerPriorityNominatedPods(logger, fh, preemptor.Priority(), c.Nodes())
	if err := clearNominatedNodeName(ctx, cs, fh.APICacher(), nominatedPods...); err != nil {
```

如果目标节点上已经有更低优先级的 Pod 被提名（它们之前也抢占过），现在这块地方要给更高优先级的抢占者了，它们的提名就失效了。

清空靠 `clearNominatedNodeName` 用 `PatchPodStatus` 把 `status.nominatedNodeName` 改成空串。源码注释说这次更新会把它们移进 activeQ，让它们尽快去找别的节点。但在按 QueueingHint 重新入队的队列里，这次更新的效果要分三种情况看：

1. 调度器收到这次 Pod 更新，`PriorityQueue.Update` 先调用 `UpdateNominatedPod`，nominator 删掉这个 Pod 的提名：它不再在该节点上占位，下一轮调度也不会先评估这个节点；
2. Pod 在 activeQ 或 backoffQ 里时，只更新对象，按原来的节奏出队；
3. Pod 在 unschedulable 池里时，只改 status 的更新被 `PodSchedulingPropertiesChange` 归为通用的 `Pod/Update` 事件。以最常见的、被 `NodeResourcesFit` 拒绝的抢占者为例，它只注册了 `UpdatePodScaleDown` 这类细分的 Pod 更新事件，匹配不上，Pod 不会被移出，仍然等受害者的删除事件或 5 分钟的周期刷新。

所以**清空提名一定会撤掉它的占位，但不保证立刻触发重新调度**。注释还说明这里没有逐个检查它们是否真的放不下，因为那需要大量 NodeInfo 和 PreFilter 状态操作，而每个节点上的提名 Pod 通常很少。

### 9.3 同步与异步

同步还是异步，只差在 `ActuatePodPreemption` 的一行分支（第 4–7 行）：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/executor.go
func (e *Executor) ActuatePodPreemption(ctx context.Context, candidate fwk.PreemptionCandidate, preemptorPod *v1.Pod, pluginName string) *fwk.Status {
	podPreemptor := &podExecutorPreemptor{Pod: preemptorPod}
	if e.fts.EnableAsyncPreemption {
		e.prepareCandidateAsync(candidate, podPreemptor, pluginName)
		return nil
	}
	return e.prepareCandidate(ctx, candidate, podPreemptor, pluginName)
}
```

同步模式下，删除受害者的 API 调用发生在调度周期里。调度周期是串行的，一次抢占删 10 个 Pod、每个请求 50 毫秒，就意味着这期间所有其他 Pod 都在等。`SchedulerAsyncPreemption`（KEP-4832）把这部分挪到 goroutine 里，PostFilter 立刻返回。

## 十、异步抢占与 PreEnqueue

异步带来一个新问题：

1. PostFilter 返回后抢占者回到队列，受害者可能还没开始删除；
2. 如果抢占者此时被某个事件唤醒、再次进入调度，它会看到节点仍然满、受害者都还没带 `DeletionTimestamp`；
3. 于是第四节的「等待」规则不生效，它会**再发起一次抢占**，杀掉另一批 Pod。

{{< figure src="/images/posts/sched-plugins/18-async-preemption.svg" caption="异步抢占的三条时间线：PostFilter 立即返回，goroutine 分两段删除受害者，PreEnqueue 用 preempting 集合挡住抢占者直到最后一个受害者开始删除（点击查看大图）" link="/images/posts/sched-plugins/18-async-preemption.svg" >}}

解决办法是 `preempting` 集合加 PreEnqueue：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
func (pl *DefaultPreemption) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	if !pl.fts.EnableAsyncPreemption {
		return nil
	}
	// ...
	if pl.Executor.IsPodRunningPreemption(p.GetUID()) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "waiting for the preemption for this pod to be finished")
	}
	return nil
}
```

`prepareCandidateAsync` 在启动 goroutine 之前把抢占者 UID 放进 `preempting`，goroutine 结束时删除。goroutine 内部的顺序有讲究：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/executor.go
		preemptLastVictim := true
		if len(victimPods) > 1 {
			// ...
			e.fh.Parallelizer().Until(ctx, len(victimPods)-1, preemptPod, pluginName)
			if err := errCh.Receive(); err != nil {
				// ...
				preemptLastVictim = false
			}
		}

		if preemptLastVictim {
			lastVictim := victimPods[len(victimPods)-1]
			e.mu.Lock()
			e.lastVictimsPendingPreemption[preemptor.UID()] = pendingVictim{namespace: lastVictim.Namespace, name: lastVictim.Name}
			e.mu.Unlock()

			preemptedInMemory, err := e.PreemptPod(ctx, c, preemptor, lastVictim, pluginName)
			// ...
		}
		e.mu.Lock()
		e.preempting.Delete(preemptor.UID())
		delete(e.lastVictimsPendingPreemption, preemptor.UID())
		e.mu.Unlock()
```

前 n-1 个受害者并行删，最后一个单独删，并在删之前登记到 `lastVictimsPendingPreemption`（第 3–16 行）。**这么拆是为了堵住一个删除事件早于簿记的竞态。**

源码注释描述的竞态是：

1. 最后一个 DELETE 请求返回后、goroutine 执行 `preempting.Delete` 之前，受害者的删除事件可能已经到达队列；
2. 此时 PreEnqueue 看到抢占者还在 `preempting` 里，就会把它挡住；
3. 等 `preempting.Delete` 执行时，删除事件已经消费过了，没有新事件来唤醒它，抢占者卡在不可调度池里。

有了 `lastVictimsPendingPreemption`，`isRunningPreemption` 就能区分这种情况：

```go {linenos=inline}
// pkg/scheduler/framework/preemption/executor.go
	victim, ok := e.lastVictimsPendingPreemption[uid]
	if !ok {
		// Since preemptor is in `preempting` but last victim is not registered yet, the async preemption is pending.
		return true
	}
	// Preemptor is waiting for preemption of one last victim. We can check if the victim has already been deleted.
	victimPod, err := e.podLister.Pods(victim.namespace).Get(victim.name)
	if err != nil {
		// Victim already deleted, preemption is done.
		return false
	}
	if victimPod.DeletionTimestamp != nil {
		// Victim deletion has started, preemption is done.
		return false
	}
	// Preemption of the last pod is still ongoing.
	return true
```

只要最后一个受害者已经开始删除（或已消失），就认为抢占完成，不再阻挡（第 13–16 行）。

另外两个兜底，处理方式相同，都是在 goroutine 结束时调用 `fh.Activate` 主动把抢占者移回 activeQ：

1. goroutine 出错时；
2. 最后一个受害者是在内存中被抢占的（不会产生删除事件）。

### 10.1 EventsToRegister：注册了事件，却总是 Skip

异步模式下，它只注册一个事件（第 3–8 行）：

```go {linenos=inline}
// pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
func (pl *DefaultPreemption) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	if pl.fts.EnableAsyncPreemption {
		return []fwk.ClusterEventWithHint{
			// We need to register the event to tell the scheduling queue that the pod could be un-gated after some Pods' deletion.
			{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: pl.isPodSchedulableAfterAssignedPodDeletion},
		}, nil
	}
	return nil, nil
}
```

`isPodSchedulableAfterAssignedPodDeletion` 永远返回 `QueueSkip`。这看起来矛盾，但要结合队列的逻辑看。被 PreEnqueue 挡住的 Pod 处于「gated」状态，队列有一个优化：

```go {linenos=inline}
// pkg/scheduler/backend/queue/scheduling_queue.go
		if entity.Gated() && !framework.ClusterEventIsWildCard(event) && !framework.MatchAnyClusterEvent(event, entity.GetGatingPluginEvents()) {
			// This event doesn't interest the gating plugin of this Pod,
			// which means this event never moves this Pod to activeQ.
			continue
		}

		schedulingHint := p.isEntityWorthRequeuing(logger, entity, event, oldObj, newObj, hintKeys)
```

gated 的 Pod 只会被**gating 插件注册过的事件**考虑。注册 `AssignedPod/Delete` 的用意分两步看：

1. 让受害者删除事件能「穿过」这层过滤；
2. 穿过之后，`isEntityWorthRequeuing`（第 8 行）询问的是 Pod 的 `UnschedulablePlugins`（比如 `NodeResourcesFit`）的 hint，由它们判断删除是否真的腾出了空间。

`DefaultPreemption` 自己不做这个判断，所以返回 Skip。

## 十一、nominatedNodeName 的后续：下一轮怎么用

抢占者带着 `nominatedNodeName` 回到队列。下一轮调度时，`findNodesThatFitPod` 先单独评估提名节点：

```go {linenos=inline}
// pkg/scheduler/algorithm.go
	// "NominatedNodeName" can potentially be set in a previous scheduling cycle as a result of preemption.
	// This node is likely the only candidate that will fit the pod, and hence we try it first before iterating over all nodes.
	// We take the same tack for hinted nodes from the batch module.
	if len(pod.Status.NominatedNodeName) > 0 || len(nodeHint) > 0 {
		feasibleNodes, err := a.evaluateNominatedNode(ctx, pod, schedFramework, state, nodeHint, diagnosis)
		// ...
		if len(feasibleNodes) != 0 {
			return feasibleNodes, diagnosis, nodeHint, nil
		}
	}
```

结果有两种：

1. 提名节点通过 Filter 就直接选它，跳过其他节点和打分（第 5–11 行）；
2. 没通过就走正常流程，可能落到别的节点上——提名只是一个提示，不是承诺。

提名在等待期间也有作用：其他 Pod 调度时，`RunFilterPluginsWithNominatedPods` 会把提名到该节点、优先级不低于自己的 Pod 当作已经在节点上，防止低优先级 Pod 抢走刚腾出的空间。

提名最终会被清除，有两条路径：

1. 抢占者绑定时，`ClearingNominatedNodeNameAfterBinding`（1.35 起 Beta，默认开）让 API Server 在 binding 子资源里清空它；
2. 下一轮抢占发现「Preemption is not helpful」时返回 `NNN=""` 清空。

用第一节的 `api-7`（优先级 1000，请求 4 核 CPU）把提名的一来一回走一遍。node-2 有 8 核，跑着 `web-1`（优先级 1000，4 核）和 `batch-a`（优先级 0，4 核），已经满了；其它节点也都满了，上面的 Pod 优先级都不低于 1000：

1. 第一轮调度：所有节点都在 `NodeResourcesFit` 上返回 `Unschedulable`（Insufficient cpu），进入 PostFilter。
2. `findCandidates`：所有节点都是潜在节点。其它节点在 `SelectVictimsOnNode` 第一步找不到优先级更低的 Pod，返回 `UnschedulableAndUnresolvable`；node-2 上只有 `batch-a` 符合（`web-1` 与 `api-7` 同优先级，不能抢）。移除 `batch-a` 后剩 4 核，放得下；尝试放回 `batch-a` 又放不下，受害者是 `[batch-a]`，node-2 成为唯一候选。
3. 执行：异步模式下 `prepareCandidateAsync` 把 `api-7` 放进 `preempting`，goroutine 给 `batch-a` 打上 `DisruptionTarget` condition 后删除；PostFilter 立刻返回 node-2，`handleSchedulingFailure` 把 `api-7` 加进 nominator，并把 `status.nominatedNodeName` 写成 node-2。本周期仍算失败，`api-7` 回到不可调度池。
4. `batch-a` 优雅退出期间仍在调度器缓存里，node-2 还是满的。它的删除事件到达后，缓存里 node-2 已请求的 CPU 降到 4 核；`NodeResourcesFit` 对 `AssignedPod/Delete` 的 hint 返回 `Queue`，`api-7` 移出不可调度池。
5. 如果 `api-7` 的退避还没到期，第 4 步把它放进的是 backoffQ。这时 activeQ 里优先级为 0、请求 2 核的 `batch-c` 先出队，它在 node-2 上跑 `RunFilterPluginsWithNominatedPods`：`api-7` 提名到 node-2，优先级 1000 不低于 0，被临时加进 NodeInfo，已请求变成 8 核，`batch-c` 放不下。
6. `api-7` 出队：`findNodesThatFitPod` 先用 `evaluateNominatedNode` 单独评估 node-2。提名到 node-2 的只有它自己，按 UID 排除；Filter 看到 4 + 4 = 8 核，通过，直接选中 node-2，不再评估其它节点，也不打分。
7. 绑定时 `ClearingNominatedNodeNameAfterBinding` 让 API Server 清空 `status.nominatedNodeName`。

从第 3 步到第 6 步，受害者删除和抢占者重新调度之间有一个时间窗，node-2 腾出的 4 核在这段时间里由提名占着。第 5 步出队的如果是优先级不低于 1000 的 Pod，提名挡不住它，这就是第十三节说的「空间可能被别人拿走」。

以上都是单个 Pod 的抢占。PodGroup 走的是另一条路径。

## 十二、PodGroup 抢占

`GenericWorkload` 开启后，PodGroup 作为一个整体调度，失败时调用 `PodGroupPostFilter`。它走的是另一个 evaluator：`PodGroupEvaluator` 不再逐节点模拟，而是分四步：

1. 从全集群收集受害者（`GenerateVictims`）；
2. 一次性全部从快照里移除；
3. 用 `podGroupSchedulingFunc` 检查整个组能否放下；
4. 再逐个尝试放回。

思路与单 Pod 版本相同，只是「放得下」的判断变成了「整个组都放得下」，候选也不再是单个节点，而是一组节点（`candidate.name` 为 `"cluster"`）。`preemptionPolicy` 由 `PodGroupPreemptionPolicy` gate 决定读 PodGroup 的字段还是读组内 Pod 的字段。

## 十三、边界情况与坑

**同优先级不抢占。** 条件是 `victim.Priority() < corev1helpers.PodPriority(preemptor)`，严格小于。没设 PriorityClass 的 Pod 优先级都是 0，彼此之间永远不会抢占。

**PDB 不能阻止抢占。** 它只影响受害者选择和节点排序。真正需要防护的工作负载要靠优先级，而不是 PDB。

**抢占成功不等于马上调度。** 受害者有优雅退出期，默认 30 秒。这期间抢占者是 Pending，`kubectl describe` 能看到 `Nominated Node`。如果受害者的 `terminationGracePeriodSeconds` 很长，抢占者就要等很久。

**受害者退出后，空间可能被别人拿走。** 提名只保护不被更低优先级的 Pod 抢走，同优先级或更高优先级的 Pod 仍然可以占用，此时抢占者下一轮要重新抢占。

**`UnschedulableAndUnresolvable` 的节点完全不参与。** 一个 Pod 同时有节点亲和和资源需求时，只有亲和匹配、资源不足的节点会被考虑。排障时先看 `kubectl describe` 里每个节点的失败原因。

**模拟中不考虑抢占者对受害者的亲和。** 如果高优先级 Pod 依赖与低优先级 Pod 的 Pod 亲和，抢占可能在模拟里失败。

**DRA 的设备不会因抢占而在模拟中释放。** 见[第 15 篇](/posts/sched-plugin-15-dynamic-resources/)第十一节。

## 十四、发散

### 14.1 抢占对 CycleState 的要求

抢占是唯一会在一个调度周期内多次「回放」Filter 的地方：每个候选节点一份 CycleState 副本，每移除或放回一个受害者就调用一次 PreFilterExtensions。这就是为什么系列里反复强调各插件的 `Clone` 实现和 `AddPod`/`RemovePod`：

1. `Clone` 浅拷贝，副本之间会互相污染；
2. 没实现 `AddPod`/`RemovePod`，插件在模拟中看不到受害者被移除，可能把一个本来可行的候选判为不可行；
3. 像 `DynamicResources` 这样 `Clone` 返回自己的插件，前提是它在模拟中不修改状态。

### 14.2 抢占的成本

一次抢占的最坏开销约为「潜在节点数 × 每节点受害者数 × 一次 Filter 的开销」，再被 `candidatesNum` 截断。在几千个节点、每节点几十个低优先级 Pod 的集群里，这一步可能是调度周期里最慢的环节。观察 `scheduler_preemption_evaluation_duration_seconds` 和 `scheduler_preemption_attempts_total`；如果评估太慢，调小 `minCandidateNodesAbsolute` 是最直接的手段。

### 14.3 与 out-of-tree 抢占插件的关系

`preemption.Interface` 的五个方法就是扩展点，但 scheduler-plugins 里的两个抢占插件走了不同的路：

1. `PreemptionToleration`（[第 31 篇](/posts/sched-plugin-31-preemption-toleration/)）实现了完整的 `preemption.Interface`，在自己的 PostFilter 里 `NewEvaluator(…, pl, …)` 复用这套结构，主要改写 `SelectVictimsOnNode`：PriorityClass 上声明了容忍策略的 Pod，在容忍期内不会成为受害者。它传入的 Executor 不开任何 feature，所以总是同步抢占。
2. `CrossNodePreemption`（[第 32 篇](/posts/sched-plugin-32-cross-node-preemption/)）没有复用 `Evaluator`，而是复刻了一遍早期的抢占流程，用 DFS 暴力枚举跨节点的受害者组合。它的代码还停留在旧版 framework API 上，在 `cmd/scheduler/main.go` 里的注册语句是注释掉的。

### 14.4 历史演进

抢占最早写在 `generic_scheduler.go` 里，scheduler framework 引入 PostFilter 扩展点后才被拆成 `DefaultPreemption` 插件，后来又把通用部分抽到 `framework/preemption` 包，以支持 out-of-tree 复用。

异步抢占（KEP-4832）是最近的一次大变化，它把 API 调用移出调度周期，引入了 PreEnqueue 阻挡和 `lastVictimsPendingPreemption` 这套簿记。

再往后，`GenericWorkload` 引入了 Victim/DomainVictim 抽象，使得受害者可以是一整个 PodGroup——这也是 `SelectVictimsOnNode` 要处理「远程节点」的原因：一个 PodGroup 受害者的成员可能分布在多个节点，移除它时只修改主节点的 NodeInfo，其他节点只通过 PreFilterExtensions 通知插件更新 CycleState。

## 小结

1. `DefaultPreemption` 只是一个壳，流程在 `preemption.Evaluator`，策略通过 `preemption.Interface` 回调插件，`IsEligiblePod` 和 `MoreImportantVictim` 可以替换。
2. 只有 Filter 返回 `Unschedulable` 的节点才是潜在节点；候选数量 `min(N, max(N×10%, 100))` 是提前停止的阈值，且要求至少一个不违反 PDB 的候选。
3. `SelectVictimsOnNode` 先移除所有低优先级 Pod，放不下就放弃；放得下再按「违反 PDB 的先、重要的先」逐个放回，放不下的才是受害者。
4. 候选节点按 PDB 违反数、最高受害者优先级、优先级和（带偏移）、受害者数、启动时间五级淘汰。
5. 删除前先打 `DisruptionTarget` condition，Permit 等待中和 PreBind 中的受害者在内存里处理；PDB 不阻止删除。
6. 异步抢占用 `preempting` 集合 + PreEnqueue 防止重复抢占，用 `lastVictimsPendingPreemption` 消除「删除事件早于簿记」的竞态。

下一篇回到打分：[NodeResourcesBalancedAllocation](/posts/sched-plugin-19-balanced-allocation/)，看调度器如何让节点上各种资源的使用率保持均衡。
