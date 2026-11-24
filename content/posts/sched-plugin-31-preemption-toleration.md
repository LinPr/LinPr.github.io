---
title: 'scheduler-plugins 31 [out-of-tree] PreemptionToleration：让受害者也能为抢占设条件'
date: 2026-11-19T13:10:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "preemption", "PriorityClass"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "PreemptionToleration 在 PriorityClass 上用两个 annotation 声明「谁能抢我、我被调度后多久内不可抢」，以此改写抢占的受害者筛选。本文拆解策略解析与默认值、ExemptedFromPreemption 的判定顺序、它如何复用 preemption.Evaluator，以及与 in-tree DefaultPreemption 的逐项差异和容忍期到期后的重试时机。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `PreemptionToleration` 实现的扩展点（PostFilter）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/31.svg" link="/images/posts/sched-plugins/callstack/31.svg" caption="PreemptionToleration 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇的 [NodeResourceTopologyMatch](/posts/sched-plugin-30-node-resource-topology/) 处理的是 Filter 和 Score 阶段的 NUMA 对齐。从本篇开始的两篇回到 PostFilter，也就是 Filter 全部失败之后的抢占。

in-tree 的 [DefaultPreemption](/posts/sched-plugin-18-default-preemption/) 已经详细拆过抢占的骨架，这两篇看的是 scheduler-plugins 在这副骨架上做的两种改造：本篇的 `PreemptionToleration` 改的是「谁可以被抢」，下一篇的 `CrossNodePreemption` 试图改的是「受害者可以在哪」。

本文基于 scheduler-plugins 提交 `cfeb3aa`，它依赖 Kubernetes v1.36.4；对比用的 in-tree 代码来自 `k8s.io/kubernetes@v1.36.4`，涉及主干时以 master 提交 `aabdf5a2` 为准。

`PreemptionToleration` 只实现了一个扩展点：`PostFilter`。它不在默认 Profile 里，没有 feature gate，README 标注的成熟度是 Alpha。启用方式是在 scheduler-plugins 编译的调度器里替换掉 `DefaultPreemption`：

```yaml
# pkg/preemptiontoleration/README.md
profiles:
- schedulerName: default-scheduler
  plugins:
    postFilter:
      enabled:
      - name: PreemptionToleration
      disabled:
      - name: DefaultPreemption
```

两者必须二选一。PostFilter 插件依次执行、第一个返回 `Success` 的生效，如果 `DefaultPreemption` 还开着并排在前面，它会无视容忍策略直接驱逐。

## 一、它要解决什么问题

Kubernetes 的优先级抢占只有一条规则：高优先级可以驱逐低优先级。`PriorityClass` 上唯一和抢占有关的字段 `preemptionPolicy` 描述的是抢占者一侧：设为 `Never` 的 Pod 不会去抢别人。**被抢的一方没有任何发言权。**

KEP（`kep/205-preemption-toleration`）给了两个这条规则覆盖不到的场景。

第一个场景：低优先级但不愿被打断。有些作业用的是不支持断点续跑的工具，被驱逐就意味着从头再来。给它一个高优先级可以避免被抢，但它会反过来抢占别人，而这类作业本质上只应该在集群有空闲时运行。管理员想要的是这样一种优先级：调度时排在 `high` 后面，一旦跑起来，`high` 抢不动它，只有 `system-critical` 能抢。

```yaml
  system-critical: 10000
             high:  9000
low-non-preempted:  8000 # this priority class can tolerate preemption from priorities less than 10000
              low:  8000 # normal preemption happens on this priority class
```

第二个场景：保证最短运行时间。机器学习训练按 epoch 推进，如果集群繁忙、作业总在第一个 epoch 跑完之前被抢，它就永远没有进展，消耗的算力全部白费。管理员希望：被调度后的头 10 分钟，`high` 不能抢它；10 分钟后正常可抢。

这两个需求都是「被抢的一方对抢占设条件」。`PreemptionToleration` 用 `PriorityClass` 上的 annotation 表达这些条件，不改动任何公开 API。

## 二、策略：两个 annotation

这两个场景用的是同一套机制：把条件写在 `PriorityClass` 的 annotation 上。先定义 key。

```go
// pkg/preemptiontoleration/preemption_toleration_policy.go
const (
	AnnotationKeyPrefix                     = "preemption-toleration.scheduling.x-k8s.io/"
	AnnotationKeyMinimumPreemptablePriority = AnnotationKeyPrefix + "minimum-preemptable-priority"
	AnnotationKeyTolerationSeconds          = AnnotationKeyPrefix + "toleration-seconds"
)
```

```go
// pkg/preemptiontoleration/preemption_toleration_policy.go
type Policy struct {
	// MinimumPreemptablePriority specifies the minimum priority value that can preempt this priority class.
	// It defaults to the PriorityClass's priority value + 1 if not set, which means pods that have a higher priority value can preempt it.
	MinimumPreemptablePriority int32

	// TolerationSeconds specifies how long this priority class can tolerate preemption
	// by priorities lower than MinimumPreemptablePriority.
	// It defaults to zero if not set. Zero value means the pod will be preempted immediately. i.e., no toleration at all.
	// If it's set to a positive value, the duration will be honored.
	// If it's set to a negative value, the pod can be tolerated forever - i.e., pods with priority
	// lower than MinimumPreemptablePriority won't be able to preempt it.
	// This value affects scheduled pods only (no effect on nominated pods).
	TolerationSeconds int64
}
```

两个字段的含义要合在一起读：优先级低于 `MinimumPreemptablePriority` 的抢占者，需要等受害者被调度满 `TolerationSeconds` 秒之后才能抢它；优先级达到门槛的抢占者不受限制。`TolerationSeconds` 为负数表示永远等不到。

解析函数很直白：

```go
// pkg/preemptiontoleration/preemption_toleration_policy.go
func parsePreemptionTolerationPolicy(
	pc schedulingv1.PriorityClass,
) (*Policy, error) {
	policy := &Policy{}

	minimumPreemptablePriorityStr, ok := pc.Annotations[AnnotationKeyMinimumPreemptablePriority]
	if !ok {
		policy.MinimumPreemptablePriority = pc.Value + 1 // default value
	} else {
		minimumPreemptablePriority, err := strconv.ParseInt(minimumPreemptablePriorityStr, 10, 32)
		if err != nil {
			return nil, err
		}
		policy.MinimumPreemptablePriority = int32(minimumPreemptablePriority)
	}

	tolerationSecondsStr, ok := pc.Annotations[AnnotationKeyTolerationSeconds]
	if !ok {
		policy.TolerationSeconds = 0 // default value
	} else {
		tolerationSeconds, err := strconv.ParseInt(tolerationSecondsStr, 10, 64)
		if err != nil {
			return nil, err
		}
		policy.TolerationSeconds = tolerationSeconds
	}

	return policy, nil
}
```

默认值是 `value + 1` 和 `0`。两个都不写时，任何比受害者优先级高的抢占者都满足门槛，行为与默认抢占完全一致，所以把插件换上去不会改变没有加 annotation 的 `PriorityClass` 的行为。

用 KEP 的两个场景对应：

```yaml
# 不愿被 high 打断：只有 ≥10000 能抢，且永远不过期
kind: PriorityClass
metadata:
  name: low-non-preempted
  annotations:
    preemption-toleration.scheduling.x-k8s.io/minimum-preemptable-priority: "10000"
    preemption-toleration.scheduling.x-k8s.io/toleration-seconds: "-1"
value: 8000
---
# 保证 10 分钟运行时间：high 要等 10 分钟，system-critical 随时可抢
kind: PriorityClass
metadata:
  name: low-non-preempted-10min
  annotations:
    preemption-toleration.scheduling.x-k8s.io/minimum-preemptable-priority: "10000"
    preemption-toleration.scheduling.x-k8s.io/toleration-seconds: "600"
value: 8000
```

注意策略挂在 `PriorityClass` 上，读的是受害者的 `PriorityClass`，而且是在抢占时通过 Lister 实时读取。**修改 annotation 会立即影响所有已经在运行的、使用该 `PriorityClass` 的 Pod，不需要重建 Pod。** `PriorityClass` 的 `value` 字段不可修改，但 annotation 可以随时改。

## 三、ExemptedFromPreemption：逐条判定

策略解析出来之后，判定本身只剩一个问题：给定一个受害者和一个抢占者，前者该不该被保护。回答它的是插件的核心函数，源码注释特意说明导出它是为了让其他 PostFilter 插件复用这套容忍策略：

```go
// pkg/preemptiontoleration/preemption_toleration.go
func ExemptedFromPreemption(
	logger klog.Logger,
	victimCandidate, preemptor *v1.Pod,
	pcLister schedulinglisters.PriorityClassLister,
	now time.Time,
) (bool, error) {

	if victimCandidate.Spec.PriorityClassName == "" {
		return false, nil
	}
	victimPriorityClass, err := pcLister.Get(victimCandidate.Spec.PriorityClassName)
	if err != nil {
		return false, err
	}

	preemptorPreemptionPolicy := v1.PreemptLowerPriority
	if preemptor.Spec.PreemptionPolicy != nil {
		preemptorPreemptionPolicy = *preemptor.Spec.PreemptionPolicy
	}
	if preemptorPreemptionPolicy == v1.PreemptNever {
		return true, nil
	}

	// check it can tolerate the preemption in terms of priority value
	policy, err := parsePreemptionTolerationPolicy(*victimPriorityClass)
	if err != nil {
		// if any error raised, no toleration at all
		logger.Error(err, "Failed to parse preemption toleration policy of victim candidate's priorityclass.  This victim candidate can't tolerate the preemption",
			// ...
		)
		return false, nil
	}
	preemptorPriority := corev1helpers.PodPriority(preemptor)
	if preemptorPriority >= policy.MinimumPreemptablePriority {
		return false, nil
	}

	if policy.TolerationSeconds < 0 {
		return true, nil
	}

	// check it can tolerate the preemption in terms of toleration seconds
	_, scheduledCondition := podutil.GetPodCondition(&victimCandidate.Status, v1.PodScheduled)
	if scheduledCondition == nil || scheduledCondition.Status != v1.ConditionTrue {
		return true, nil
	}
	scheduledAt := scheduledCondition.LastTransitionTime.Time
	tolerationDuration := time.Duration(policy.TolerationSeconds) * time.Second

	return scheduledAt.Add(tolerationDuration).After(now), nil
}
```

返回 `true` 表示豁免，即这个 Pod 不能成为受害者。

{{< figure src="/images/posts/sched-plugins/31-exemption-flow.svg" link="/images/posts/sched-plugins/31-exemption-flow.svg" caption="节点上每个 Pod 的判定顺序：先过优先级比较，再进 ExemptedFromPreemption；绿色为豁免，橙色为成为候选受害者，红色为整个节点的模拟报错（点击查看大图）" >}}

逐条看几个值得注意的分支，它们决定了这个函数在边界上的行为：

- 没有 `priorityClassName`：直接不豁免。只有在集群里没有 `globalDefault: true` 的 `PriorityClass` 时才会出现这种 Pod，否则 Priority 准入插件会给它填上默认的类名。
- Lister 查不到 `PriorityClass`：返回 error，而且这个 error 会一路上抛，让当前节点的 `SelectVictimsOnNode` 以 `Error` 状态结束。`PriorityClass` 被删除、但还有 Pod 引用它的情况下就会触发。测试 `TestExemptedFromPreemptionWithNonExitingPriorityClass` 覆盖了这一路径。
- 抢占者 `preemptionPolicy: Never`：豁免所有人。这个分支在插件内部实际走不到，因为 Evaluator 的第一步 `PodEligibleToPreemptOthers` 已经把这类抢占者拦下了；它的存在是因为函数是导出的，其他插件调用时不一定先做了资格检查。
- 解析失败：不豁免，只打一条错误日志。annotation 写成 `"10k"` 之类的值，策略就等于没写，调度器之外看不到任何提示。
- 门槛比较用 `>=`：抢占者优先级恰好等于 `minimum-preemptable-priority` 时，不豁免，可以直接抢。测试 `when preemptor's priority is equal to MinimumPreemptablePriorityClass, it should return false` 明确了这一点。
- 没有 `PodScheduled=True`：豁免。节点上的 Pod 一般都已经有这个 condition（绑定时由 API server 写入），没有的主要是调度器缓存里被 assume、尚未完成绑定的 Pod。插件选择保护它们：一个刚刚做出的调度决定不应该马上被推翻。
- 时间比较：`scheduledAt` 用的是 `PodScheduled` condition 的 `LastTransitionTime`，不是容器启动时间，也不是 `status.startTime`。Pod 绑定后拉镜像花了 5 分钟，那 10 分钟的容忍期里真正在跑的只有 5 分钟。如果「保证运行时间」是硬需求，`toleration-seconds` 要把镜像拉取和启动时间算进去。

`now` 不是在判定时取的，而是在 PostFilter 开头取一次：

```go
// pkg/preemptiontoleration/preemption_toleration.go
func (pl *PreemptionToleration) PostFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, m fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	defer func() {
		metrics.PreemptionAttempts.Inc()
	}()

	pe := preemption.NewEvaluator(
		pl.Name(),
		pl.fh,
		pl,
		preemption.NewExecutor(pl.fh, feature.Features{}), // no feature enabled, i.e. preemption is synchronous
	)

	pl.curTime = pl.clock.Now()
	return pe.Preempt(ctx, state, pod, m)
}
```

`DryRunPreemption` 会并行模拟多个节点，**所有节点共用同一个 `curTime`，判定结果不会因为模拟的先后而不同。** `clock` 是 `k8s.io/utils/clock` 的接口，测试里可以注入假时钟。

`curTime` 存在插件结构体上而不是 CycleState 里，能这么做是因为调度周期（包括 PostFilter）是串行执行的，同一时刻只有一个 Pod 在跑 PostFilter。

### 3.1 一个反直觉的组合：只写门槛

假设只写了 `minimum-preemptable-priority: "10000"`，没写 `toleration-seconds`。直觉上这应该意味着「低于 10000 的抢不动我」。但 `TolerationSeconds` 默认是 0，走到最后一步：`scheduledAt + 0 > now` 对于一个已经调度过的 Pod 不成立，返回 false，不豁免。也就是说，**只写门槛不写时长，几乎等于什么都没写**，唯一的效果是保护那些还没有 `PodScheduled=True` 的 Pod。

想要「门槛以下永远抢不动」，必须同时写 `toleration-seconds: "-1"`。KEP 里的 `low-non-preempted` 正是这么配的。

反过来，只写 `toleration-seconds` 不写门槛也几乎无效：门槛默认是 `value + 1`，任何能抢它的 Pod（优先级严格更高）都达到了门槛，在第五步就判为不豁免，根本走不到时长判断。

### 3.2 门槛只能往上调

`minimum-preemptable-priority` 设得比 `value` 还低，比如 `value: 8000`、门槛 5000，会不会让优先级 6000 的 Pod 能抢它？不会。在 `ExemptedFromPreemption` 之前，`SelectVictimsOnNode` 已经过滤掉了优先级不低于抢占者的 Pod：

```go
// pkg/preemptiontoleration/preemption_toleration.go
	podPriority := corev1helpers.PodPriority(preemptor)
	for _, pi := range nodeInfo.GetPods() {
		if corev1helpers.PodPriority(pi.GetPod()) >= podPriority {
			continue
		}

		// For a pod with lower priority, check if it can be exempted from the preemption.
		exempted, err := ExemptedFromPreemption(logger, pi.GetPod(), preemptor, pl.priorityClassLister, pl.curTime)
		if err != nil {
			logger.Error(err, "Encountered error while selecting victims on node", "Node", nodeInfo.Node().Name)
			return nil, 0, fwk.AsStatus(err)
		}

		if !exempted {
			potentialVictims = append(potentialVictims, pi)
		}
	}
```

**容忍策略只能在「优先级更低」这个集合里再剔除一部分，不能扩大它。** 这是一个有意的保守设计：受害者一侧的策略只能保护自己，不能让自己更容易被抢。

## 四、SelectVictimsOnNode：与 DefaultPreemption 只差一处

第三节的 `ExemptedFromPreemption` 是逐个 Pod 的判断，它被调用在 `SelectVictimsOnNode` 里，用来把候选受害者集合再筛一遍。整个函数与 in-tree 的对应实现只差这一处，函数注释写得很直接：

```go
// pkg/preemptiontoleration/preemption_toleration.go
// SelectVictimsOnNode finds minimum set of pods on the given node that should
// be preempted in order to make enough room for "preemptor" to be scheduled.
// The algorithm is almost identical to DefaultPreemption plugin's one.
// The only difference is that it takes PreemptionToleration annotations in
// PriorityClass resources into account for selecting victim pods.
```

候选受害者确定之后，剩下的步骤和 in-tree 的实现逐行对应：

```go
// pkg/preemptiontoleration/preemption_toleration.go
	// Remove the victims only once the loop above is done: removePod mutates the
	// very slice that nodeInfo.GetPods() returns.
	for _, pi := range potentialVictims {
		if err := removePod(pi); err != nil {
			return nil, 0, fwk.AsStatus(err)
		}
	}

	// No potential victims are found, and so we don't need to evaluate the node again since its state didn't change.
	if len(potentialVictims) == 0 {
		message := fmt.Sprintf("No victims found on node %v for preemptor pod %v", nodeInfo.Node().Name, preemptor.Name)
		return nil, 0, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, message)
	}

	// If the new pod does not fit after removing all the lower priority pods,
	// we are almost done and this node is not suitable for preemption. ...
	if status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, state, preemptor, nodeInfo); !status.IsSuccess() {
		return nil, 0, status
	}
	var victims []*v1.Pod
	numViolatingVictim := 0
	// Sort potentialVictims by pod priority from high to low, which ensures to
	// reprieve higher priority pods first.
	sort.Slice(potentialVictims, func(i, j int) bool {
		return util.MoreImportantPod(potentialVictims[i].GetPod(), potentialVictims[j].GetPod())
	})
	// Try to reprieve as many pods as possible. We first try to reprieve the PDB
	// violating victims and then other non-violating ones. In both cases, we start
	// from the highest priority victims.
	violatingVictims, nonViolatingVictims := filterPodsWithPDBViolation(potentialVictims, pdbs)
	// ...
```

全部移除候选受害者、跑一遍 Filter 确认放得下、按「先违反 PDB 的、再不违反的」两轮，各自按优先级从高到低逐个赦免。被豁免的 Pod 始终留在节点副本上，Filter 判断时它们占着资源。所以一个节点上如果大部分低优先级 Pod 都处于容忍期，移除剩下的也腾不出足够空间，这个节点会在 Filter 一步失败，不成为候选。

这里有一个值得多想一步的地方：容忍期的判断发生在「全部移除」之前，是一个硬筛选，而不是赦免阶段的偏好。即便驱逐一个容忍期内的 Pod 就能让抢占者放下，插件也不会这么做，而是去别的节点找；所有节点都找不到，抢占就失败。容忍策略的优先级高于「尽量让高优先级 Pod 调度成功」。

## 五、和 DefaultPreemption 的逐项对比

第四节说的「只差一处」仅指受害者筛选，其余部分不是重写，而是从 in-tree 复制来的。源码在文件末尾标了一个分界线：

```go
// pkg/preemptiontoleration/preemption_toleration.go
/* DO NOT EDIT CONTENT BELOW */
/* Copied from k/k#pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go */
```

分界线以下的 `GetOffsetAndNumCandidates`、`CandidatesToVictimsMap`、`calculateNumCandidates`、`PodEligibleToPreemptOthers`、`filterPodsWithPDBViolation` 都是从 in-tree 复制来的。参数类型也是直接复用：

```go
// apis/config/types.go
// PreemptionTolerationArgs reuses DefaultPluginArgs.
type PreemptionTolerationArgs schedconfig.DefaultPreemptionArgs
```

默认值和校验也直接调 in-tree 的：`SetDefaults_PreemptionTolerationArgs` 调用 `SetDefaults_DefaultPreemptionArgs`，得到 `minCandidateNodesPercentage: 10`、`minCandidateNodesAbsolute: 100`；`New` 里调用 `validation.ValidateDefaultPreemptionArgs`。

所以「找多少个候选节点」这件事，`PreemptionToleration` 和 `DefaultPreemption` 完全一样：从随机偏移开始，找到 `max(节点数 × 10%, 100)`（不超过节点数）个候选就停。

「复制」意味着随着 in-tree 演进会逐渐脱节。拿 v1.36.4 的 `DefaultPreemption` 对比：

| 方面 | DefaultPreemption（v1.36.4） | PreemptionToleration |
| --- | --- | --- |
| 候选受害者筛选 | 优先级更低，且 `IsEligiblePod` 返回 true（默认恒真） | 优先级更低，且 `ExemptedFromPreemption` 返回 false |
| 受害者排序 | 可替换的 `MoreImportantPod` 函数字段 | 固定用 `util.MoreImportantPod` |
| 重复抢占检查 | 退出中的 Pod 必须带 `DisruptionTarget` condition、reason 为 `PreemptionByScheduler` | 只看 `DeletionTimestamp`，被手动删除的 Pod 也算 |
| 驱逐执行 | `SchedulerAsyncPreemption` 开启时异步执行（1.33 起默认开启），配合 `PreEnqueue` 挡住抢占中的 Pod | 固定同步执行，没有 `PreEnqueue` |
| EventsToRegister | 异步抢占时注册 Pod 删除事件，hint 恒为 `QueueSkip` | 未实现 |
| PodGroup | 开启相关 gate 时对 PodGroup 内的 Pod 直接返回 `Unschedulable`，交给整组抢占 | 不感知 |
| 返回消息 | 统一加前缀 `preemption: ` | 原样返回 Evaluator 的消息 |

第三行影响最实际。`DefaultPreemption` 认为「提名节点上有正在因抢占而退出的 Pod，就先别再抢」；`PreemptionToleration` 认为「提名节点上有任何正在退出的低优先级 Pod，就先别再抢」。

后者在一个有大量 Pod 正被滚动更新的节点上，会让已提名的抢占者在这段时间里放弃重新评估，即便它的提名节点已经因为别的原因不再合适（`UnschedulableAndUnresolvable` 的情况除外，代码对此单独放行）。

第一行则说明了一个更值得注意的事实：in-tree 现在已经有了 `IsEligiblePod` 这个钩子：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go
// IsEligiblePodFunc is a function which may be assigned to the DefaultPreemption plugin.
// This may implement rules/filtering around preemption eligibility, which is in addition to
// the internal requirement that the victim pod have lower priority than the preemptor pod.
// Any customizations should always allow system services to preempt normal pods, to avoid
// problems if system pods are unable to find space.
type IsEligiblePodFunc func(nodeInfo fwk.NodeInfo, victim fwk.PodInfo, preemptor *v1.Pod) bool
```

`DefaultPreemption` 的 `New` 返回的是具体类型 `*DefaultPreemption`，注释写明「The plugin type is retained to allow modification」。

**今天要实现 PreemptionToleration 的语义，可以不复制任何代码。** 在自己的插件工厂里调用 `defaultpreemption.New`，把 `IsEligiblePod` 换成调用 `ExemptedFromPreemption` 的函数，就同时得到了容忍策略和 in-tree 的异步抢占、PodGroup 处理等全部演进。注释里「Any customizations should always allow system services to preempt normal pods」这句提醒同样适用：容忍策略应该给 `system-cluster-critical` 之类的优先级留出门槛。

## 六、容忍期到期之后：谁来唤醒等待的 Pod

前面讲的都是抢占发生那一刻怎么判断。容忍期还有另一半：它是时间，会自己走完，而走完这件事不产生任何集群事件。

`PreemptionToleration` 没有实现 `EnqueueExtensions`，因为它只在 PostFilter 工作，从不在 PreFilter/Filter 中拒绝 Pod。一个抢占失败的 Pod，记在它身上的失败插件是 Filter 阶段的那些，比如资源不足时的 `NodeResourcesFit`。它会进入 unschedulable 队列，等这些插件注册的事件来唤醒。

问题在于，「容忍期到期」不是一个集群事件。没有任何对象发生变化，调度队列不会知道。

{{< figure src="/images/posts/sched-plugins/31-toleration-timeline.svg" link="/images/posts/sched-plugins/31-toleration-timeline.svg" caption="t=6 时 P1 抢占失败，t=10 容忍期结束但没有事件唤醒 P1，直到 unschedulable 队列的 5 分钟超时把它刷回（点击查看大图）" >}}

图里的场景：victim 在 t=0 被调度，所属 `PriorityClass` 门槛 10000、容忍 600 秒。优先级 9000 的 P1 在 t=6 分钟到来，节点上唯一能腾出空间的就是 victim，它处于容忍期，P1 找不到候选，进入 unschedulable 队列。t=10 时容忍期结束，集群里如果恰好没有 Pod 删除、节点变化之类的事件，P1 不会被唤醒，直到调度队列的兜底机制生效：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/backend/queue/scheduling_queue.go
	DefaultPodMaxInUnschedulablePodsDuration time.Duration = 5 * time.Minute
```

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/backend/queue/scheduling_queue.go
	go wait.Until(func() {
		p.flushUnschedulablePodsLeftover(logger)
	}, 30*time.Second, p.stop)
```

队列每 30 秒检查一次，把待了超过 5 分钟的 Pod 刷回去。P1 在 t=6 入队，大约 t=11 才被刷回、重新调度、抢占成功。这个例子里只晚了 1 分钟，但最坏情况下，抢占者在容忍期结束前一刻失败，要等 5 分钟多才能重试。

对于「保证运行 10 分钟」的语义，这个延迟只会让受害者多跑一会儿，不会违反承诺；但对于期望「10 分钟一到就让路」的高优先级作业，等待时间可能比预期多出几分钟。繁忙的集群里，Pod 删除、节点状态变化等事件很频繁，实际延迟通常远小于这个上限。

## 七、边界情况与坑

主流程之外，还有几处行为只体现在边界上：策略写错、`PriorityClass` 被删、集群里同时跑着别的调度器。

- 策略失效没有提示：解析失败只打日志，`PriorityClass` 上写错 annotation，表现就是和默认抢占一样。上线前可以用一个短的 `toleration-seconds` 和一个测试抢占者验证一次。
- `PriorityClass` 被删除会让抢占报错：节点上只要有一个低优先级 Pod 引用了不存在的 `PriorityClass`，这个节点的模拟就以 `Error` 结束。`DryRunPreemption` 收集这些错误，只有在一个候选都没找到时才把错误作为 PostFilter 的结果返回，这时 `schedule_one.go` 用 `HandleErrorWithContext` 把它记成错误日志，Pod 本身仍按普通的「无法调度」处理。排查时，抢占没有发生而日志里出现 `priorityclass ... not found`，就是这个原因。
- PDB 与容忍策略的关系：PDB 在抢占中是软约束，Evaluator 优先选择不违反 PDB 的候选，但所有候选都违反时仍会执行。**容忍策略是硬约束，被豁免的 Pod 不会出现在任何候选里。** 需要「绝对不被抢」时，容忍策略（`toleration-seconds: -1`）比 PDB 更可靠；但它只对调度器的抢占有效，对节点压力驱逐、`kubectl drain` 等其他驱逐途径没有作用，那些场景仍要靠 PDB。
- 只在调度器层面生效：策略存在于 annotation 中，只有启用了这个插件的调度器才认。集群里还跑着别的调度器（包括默认的 kube-scheduler）时，使用别的 `schedulerName` 的高优先级 Pod 仍会按默认规则抢占这些 Pod。
- `now` 与时钟偏差：`scheduledAt` 来自 API server 写入的 condition 时间，`now` 来自调度器所在节点的时钟。两者不同步时，容忍期的实际长度会有偏差，一般在秒级，对分钟级的容忍期影响不大。

## 八、发散：在受害者一侧表达策略

前七节都在讲一个插件怎么做。这一节回到设计层面：为什么上游最终把这件事交给插件，而不是写进 `PriorityClass`。

Kubernetes 的抢占设计里，抢占者一侧有 `priority` 和 `preemptionPolicy`，受害者一侧几乎只有 PDB。`PreemptionToleration` 补的就是受害者一侧。

社区对这个方向一直有讨论。上游最终没有把 `minimumPreemptablePriority` 之类的字段加进 `PriorityClass`，而是选择了一个更通用的扩展方式：`DefaultPreemption` 的 `IsEligiblePod` 与 `MoreImportantPod` 两个函数字段，由使用者在自己的构建里注入规则。这符合 kube-scheduler 一贯的做法：核心只保留最通用的语义，策略交给插件。代价是这两个钩子不是配置项，要用它们就得自己编译调度器。

批处理系统的做法可以作为参照。Kueue 在 `ClusterQueue` 层面提供 `preemption` 策略（如 `withinClusterQueue`、`reclaimWithinCohort`），在工作负载的粒度上决定谁能抢谁；这和 `PreemptionToleration` 在 Pod 粒度上的判断是互补的。一个 Kueue 管理的训练作业被准入之后，它的 Pod 依然可能被调度器层面的优先级抢占打断，这一层的保护就需要 `PreemptionToleration` 这样的机制。

最后，「保证最短运行时间」本质上是在用调度器弥补作业不能 checkpoint 的缺陷。容忍期越长，高优先级作业的等待越长，集群的响应能力越差。在作业能够定期 checkpoint 的前提下，把 `toleration-seconds` 设成略大于一次 checkpoint 间隔，通常是兼顾两者的取值。

## 小结

- `PreemptionToleration` 只实现 PostFilter，用受害者 `PriorityClass` 上的 `minimum-preemptable-priority` 与 `toleration-seconds` 两个 annotation 剔除候选受害者，默认值 `value + 1` 和 `0` 使未配置的 `PriorityClass` 行为与默认抢占一致。
- 判定顺序：无类名不豁免、查不到类名报错、门槛达到不豁免、时长为负永久豁免、未调度豁免、最后比较 `PodScheduled` 时间加时长与 PostFilter 开始时刻。
- 两个 annotation 必须配合使用：只写门槛或只写时长，几乎没有效果。
- 其余抢占逻辑从 in-tree 复制而来，已经落后于 v1.36.4 的 `DefaultPreemption`（同步驱逐、旧的重复抢占检查）；今天更好的实现方式是给 `DefaultPreemption` 注入 `IsEligiblePod`。
- 容忍期到期不产生集群事件，等待中的抢占者可能要等 unschedulable 队列的 5 分钟超时才重试。

下一篇看被注释掉的 [CrossNodePreemption](/posts/sched-plugin-32-cross-node-preemption/)：受害者为什么必须和抢占者在同一个节点上，以及打破这个限制要付出什么代价。
