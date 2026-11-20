---
title: 'scheduler-plugins [in-tree] TaintToleration：三种污点效果的分工'
date: 2026-11-19T08:50:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "TaintToleration", "污点", "NoExecute"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "TaintToleration 是默认 Profile 里权重最高的打分插件，同时也是硬过滤器：Filter 只看 NoSchedule 和 NoExecute，Score 只数不被容忍的 PreferNoSchedule。本文拆开容忍匹配的四条规则、Filter 与 Score 各自筛选污点和容忍的方式、反向归一化的算例与边界、QueueingHint 的判断，以及 NoExecute 在调度器、kubelet 准入和 taint-eviction-controller 之间的分工。"
---

[上一篇](/posts/sched-plugin-04-node-unschedulable/)看到，`kubectl cordon` 最终会被 node-lifecycle-controller 镜像成一个 `NoSchedule` 污点，由 `TaintToleration` 插件接手。这个插件是默认 Profile 里第一个同时有 Filter 和 Score 的插件：在 `MultiPoint` 列表里排第五，挂在 PreFilter、Filter、PreScore、Score 四个扩展点上，打分权重是 3，在所有默认插件里最高。默认启用，没有插件参数。本文基于 Kubernetes master 提交 `aabdf5a2`。

## 一、污点的三种效果

污点写在 Node 上，由 key、value、effect 组成。effect 有三种：

| effect | 对调度的影响 | 对已运行 Pod 的影响 |
| --- | --- | --- |
| `NoSchedule` | 不容忍的 Pod 不能调度上来 | 无 |
| `PreferNoSchedule` | 尽量不调度上来，实在没别的地方也可以 | 无 |
| `NoExecute` | 不容忍的 Pod 不能调度上来 | 不容忍的 Pod 会被驱逐 |

一个典型的组合：

```yaml
# 节点：GPU 专用，且是抢占式实例
spec:
  taints:
  - key: dedicated
    value: gpu
    effect: NoSchedule
  - key: spot
    value: "true"
    effect: PreferNoSchedule
---
# Pod：能用 GPU 节点，但不想去抢占式实例
spec:
  tolerations:
  - key: dedicated
    operator: Equal
    value: gpu
    effect: NoSchedule
```

`TaintToleration` 插件处理的是表格的第二列。第三列不归调度器管，第六节会讲它由谁负责。

## 二、容忍怎么匹配一个污点

插件本身不实现匹配逻辑，它调用 API 类型上的方法：

```go
// staging/src/k8s.io/api/core/v1/toleration.go
func (t *Toleration) ToleratesTaint(logger klog.Logger, taint *Taint, enableComparisonOperators bool) bool {
	if len(t.Effect) > 0 && t.Effect != taint.Effect {
		return false
	}

	if len(t.Key) > 0 && t.Key != taint.Key {
		return false
	}

	// TODO: Use proper defaulting when Toleration becomes a field of PodSpec
	switch t.Operator {
	// empty operator means Equal
	case "", TolerationOpEqual:
		return t.Value == taint.Value
	case TolerationOpExists:
		return true
	case TolerationOpLt, TolerationOpGt:
		// If comparison operators are disabled, this toleration doesn't match
		if !enableComparisonOperators {
			return false
		}
		return compareNumericValues(logger, t.Value, taint.Value, t.Operator)
	default:
		return false
	}
}
```

四条规则，按顺序：

1. **effect**：toleration 的 effect 为空表示匹配所有 effect，否则必须相等。
2. **key**：toleration 的 key 为空表示匹配所有 key（API 校验要求此时 operator 必须是 `Exists`），否则必须相等。
3. **operator**：空或 `Equal` 比较 value；`Exists` 不看 value。
4. **`Lt`/`Gt`**：把两边的 value 解析成 int64 做数值比较。`Gt` 的含义是「污点的值大于容忍的值」时容忍，`Lt` 相反。解析失败（不是十进制整数）一律视为不容忍。这是 `TaintTolerationComparisonOperators` 引入的新能力（1.35 Alpha，默认关闭），gate 关闭时 API Server 拒绝创建带 `Lt`/`Gt` 的 Pod，调度器也把它们当作不匹配。它的用途是表达「SLA 等级不低于某个值的节点」这类数值阈值，测试里的用例就是用一个数值型的 SLA 污点。

`tolerationSeconds` 不参与匹配，调度器完全忽略它。API 校验要求只有 `effect: NoExecute` 的容忍才能设置 `tolerationSeconds`，它是给驱逐用的（第六节）。

`TolerationsTolerateTaint` 是在容忍列表上的「任一匹配」：

```go
// staging/src/k8s.io/component-helpers/scheduling/corev1/helpers.go
func TolerationsTolerateTaint(logger klog.Logger, tolerations []v1.Toleration, taint *v1.Taint, enableComparisonOperators bool) bool {
	for i := range tolerations {
		if tolerations[i].ToleratesTaint(logger, taint, enableComparisonOperators) {
			return true
		}
	}
	return false
}
```

## 三、插件结构与 Filter

```go
// pkg/scheduler/framework/plugins/tainttoleration/taint_toleration.go
// TaintToleration is a plugin that checks if a pod tolerates a node's taints.
type TaintToleration struct {
	handle                                             fwk.Handle
	enableTaintTolerationComparisonOperators           bool
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = names.TaintToleration
	// preScoreStateKey is the key in CycleState to TaintToleration pre-computed data for Scoring.
	preScoreStateKey = "PreScore" + Name
	// ErrReasonNotMatch is the Filter reason status when not matching.
	ErrReasonNotMatch = "node(s) had taints that the pod didn't tolerate"
)
```

两个 feature gate 快照和前两篇一样：`InPlacePodVerticalScalingSchedulerPreemption` 打开时，被推迟扩容的 Pod 在 PreFilter 返回 `Skip`、Filter 直接放行，已经在节点上运行的 Pod 不受新污点影响；`TaintTolerationComparisonOperators` 透传给匹配函数。`handle` 字段被保存了但没有被使用。对普通 Pod，PreFilter 返回 `nil, nil`。

Filter：

```go
// pkg/scheduler/framework/plugins/tainttoleration/taint_toleration.go
func (pl *TaintToleration) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	logger := klog.FromContext(ctx)
	node := nodeInfo.Node()

	taint, isUntolerated := v1helper.FindMatchingUntoleratedTaint(logger, node.Spec.Taints, pod.Spec.Tolerations,
		helper.DoNotScheduleTaintsFilterFunc(),
		pl.enableTaintTolerationComparisonOperators)
	if !isUntolerated {
		return nil
	}

	logger.V(4).Info("node had untolerated taints", "node", klog.KObj(node), "pod", klog.KObj(pod), "untoleratedTaint", taint)
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "node(s) had untolerated taint(s)")
}
```

```go
// pkg/scheduler/framework/plugins/helper/taint.go
func DoNotScheduleTaintsFilterFunc() func(t *v1.Taint) bool {
	return func(t *v1.Taint) bool {
		// PodToleratesNodeTaints is only interested in NoSchedule and NoExecute taints.
		return t.Effect == v1.TaintEffectNoSchedule || t.Effect == v1.TaintEffectNoExecute
	}
}
```

```go
// staging/src/k8s.io/component-helpers/scheduling/corev1/helpers.go
func FindMatchingUntoleratedTaint(logger klog.Logger, taints []v1.Taint, tolerations []v1.Toleration, inclusionFilter taintsFilterFunc, enableComparisonOperators bool) (v1.Taint, bool) {
	filteredTaints := getFilteredTaints(taints, inclusionFilter)
	for _, taint := range filteredTaints {
		if !TolerationsTolerateTaint(logger, tolerations, &taint, enableComparisonOperators) {
			return taint, true
		}
	}
	return v1.Taint{}, false
}
```

流程是：先按 effect 过滤，只留下 `NoSchedule` 和 `NoExecute`；再逐个检查是否被 Pod 的某个容忍覆盖；遇到第一个不被容忍的就返回。时间复杂度是 O(污点数 × 容忍数)，两者通常都是个位数，没有任何预计算，所以插件的 PreFilter 不往 CycleState 里写东西。

{{< figure src="/images/posts/sched-plugins/05-taint-filter-score.svg" link="/images/posts/sched-plugins/05-taint-filter-score.svg" caption="同一组节点污点，按 effect 分给 Filter 和 Score：Filter 只看 NoSchedule/NoExecute，Score 只数不被容忍的 PreferNoSchedule（点击查看大图）" >}}

三点值得注意：

**失败码是 `UnschedulableAndUnresolvable`。** 驱逐节点上的 Pod 不会让污点消失，所以被污点拒绝的节点不会成为抢占候选。如果一个高优先级 Pod 在整个集群只被污点挡住，抢占找不到任何候选节点，调度失败事件里会带上 `Preemption is not helpful for scheduling`。

**`NoSchedule` 和 `NoExecute` 在调度时没有区别。** 两者都会淘汰节点，且失败码相同。区别只在 Pod 已经运行之后。

**失败原因字符串和常量不一致。** Filter 返回的是字面量 `node(s) had untolerated taint(s)`，而导出的常量 `ErrReasonNotMatch` 是 `node(s) had taints that the pod didn't tolerate`。在整个代码库里，这个常量只被 kubelet 使用（`pkg/kubelet/lifecycle/predicate.go` 的准入检查和 `kubelet.go` 里的已知拒绝原因列表）。所以用 `didn't tolerate` 去搜调度器事件是搜不到的，调度器的事件里是 `untolerated taint(s)`；后者出现在 kubelet 拒绝 Pod 的消息里。

## 四、PreScore 与 Score：数软污点

### 4.1 PreScore：先挑出有用的容忍

```go
// pkg/scheduler/framework/plugins/tainttoleration/taint_toleration.go
// preScoreState computed at PreScore and used at Score.
type preScoreState struct {
	tolerationsPreferNoSchedule []v1.Toleration
}

// Clone implements the mandatory Clone interface. We don't really copy the data since
// there is no need for that.
func (s *preScoreState) Clone() fwk.StateData {
	return s
}

// getAllTolerationEffectPreferNoSchedule gets the list of all Tolerations with Effect PreferNoSchedule or with no effect.
func getAllTolerationPreferNoSchedule(tolerations []v1.Toleration) (tolerationList []v1.Toleration) {
	for _, toleration := range tolerations {
		// Empty effect means all effects which includes PreferNoSchedule, so we need to collect it as well.
		if len(toleration.Effect) == 0 || toleration.Effect == v1.TaintEffectPreferNoSchedule {
			tolerationList = append(tolerationList, toleration)
		}
	}
	return
}

// PreScore builds and writes cycle state used by Score and NormalizeScore.
func (pl *TaintToleration) PreScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	tolerationsPreferNoSchedule := getAllTolerationPreferNoSchedule(pod.Spec.Tolerations)
	state := &preScoreState{
		tolerationsPreferNoSchedule: tolerationsPreferNoSchedule,
	}
	cycleState.Write(preScoreStateKey, state)
	return nil
}
```

CycleState 的 key 是 `PreScoreTaintToleration`，值是 Pod 容忍里 effect 为空或 `PreferNoSchedule` 的子集。effect 为 `NoSchedule` 的容忍反正匹配不上 `PreferNoSchedule` 污点（规则 1），提前剔除只是为了让每个节点的 Score 少做几次比较：PreScore 每个调度周期跑一次，Score 每个可行节点跑一次。

`Clone` 直接返回自身，不做拷贝。CycleState 在抢占模拟时会被克隆，共享同一份数据安全的前提是 PreScore 写入之后再也没人修改它。这个切片只依赖 Pod 本身，不依赖节点上的 Pod，确实是不变的。

PreScore 从不返回 `Skip`。即使 Pod 没有任何相关容忍、集群里也没有任何 `PreferNoSchedule` 污点，Score 仍会在每个可行节点上跑一遍，只是每次都得 0。

### 4.2 Score 与 NormalizeScore

```go
// pkg/scheduler/framework/plugins/tainttoleration/taint_toleration.go
// CountIntolerableTaintsPreferNoSchedule gives the count of intolerable taints of a pod with effect PreferNoSchedule
func (pl *TaintToleration) countIntolerableTaintsPreferNoSchedule(logger klog.Logger, taints []v1.Taint, tolerations []v1.Toleration) (intolerableTaints int) {
	for _, taint := range taints {
		// check only on taints that have effect PreferNoSchedule
		if taint.Effect != v1.TaintEffectPreferNoSchedule {
			continue
		}

		if !v1helper.TolerationsTolerateTaint(logger, tolerations, &taint, pl.enableTaintTolerationComparisonOperators) {
			intolerableTaints++
		}
	}
	return
}

// Score invoked at the Score extension point.
func (pl *TaintToleration) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	// ...
	score := int64(pl.countIntolerableTaintsPreferNoSchedule(logger, node.Spec.Taints, s.tolerationsPreferNoSchedule))
	return score, nil
}

// NormalizeScore invoked after scoring all nodes.
func (pl *TaintToleration) NormalizeScore(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	return helper.DefaultNormalizeScore(fwk.MaxNodeScore, true, scores)
}
```

Score 的原始分是「不被容忍的 `PreferNoSchedule` 污点个数」，越大越差。框架要求最终分数越大越好，所以 NormalizeScore 用 `reverse=true` 翻转：

```go
// pkg/scheduler/framework/plugins/helper/normalize_score.go
func DefaultNormalizeScore(maxPriority int64, reverse bool, scores fwk.NodeScoreList) *fwk.Status {
	var maxCount int64
	for i := range scores {
		if scores[i].Score > maxCount {
			maxCount = scores[i].Score
		}
	}

	if maxCount == 0 {
		if reverse {
			for i := range scores {
				scores[i].Score = maxPriority
			}
		}
		return nil
	}

	for i := range scores {
		score := scores[i].Score

		score = maxPriority * score / maxCount
		if reverse {
			score = maxPriority - score
		}

		scores[i].Score = score
	}
	return nil
}
```

公式是 `100 − 100 × raw / max(raw)`，整数除法向下取整。框架随后乘以权重 3。

{{< figure src="/images/posts/sched-plugins/05-taint-score-and-roles.svg" link="/images/posts/sched-plugins/05-taint-score-and-roles.svg" caption="左：四个可行节点的软污点打分算例；右：NoSchedule、PreferNoSchedule、NoExecute 在调度器、kubelet、taint-eviction-controller 之间的分工（点击查看大图）" >}}

算例：4 个通过 Filter 的节点，不被容忍的软污点数分别是 0、1、2、4。`max = 4`：

| 节点 | raw | 归一化 | × 3 |
| --- | --- | --- | --- |
| n1 | 0 | 100 − 0 = 100 | 300 |
| n2 | 1 | 100 − 100×1/4 = 75 | 225 |
| n3 | 2 | 100 − 100×2/4 = 50 | 150 |
| n4 | 4 | 100 − 100×4/4 = 0 | 0 |

如果最大值是 3，raw 为 1 的节点得 `100 − 33 = 67`，raw 为 2 的得 `100 − 66 = 34`，取整误差在这里体现出来。

### 4.3 归一化的两个反直觉之处

**它不是 min-max 归一化。** 分母是最大值，下界固定为 0。所有可行节点都有 1 个不被容忍的软污点时，`max = 1`，每个节点都是 `100 − 100 = 0` 分；所有节点都没有软污点时，走 `maxCount == 0` 分支，每个节点都是 100 分。两种情况节点之间都没有差别，插件对选择没有影响，这是正确的；但在 V(10) 日志 `Plugin scored node for pod` 里查看各插件的加权分时，会看到同样的「没有差别」对应着 0 分和 300 分两种绝对值，不要据此判断节点好坏。

**分数是相对的，只在本次可行节点里比较。** `max` 只在通过 Filter 的节点里取，而大集群中 Filter 会因为 `percentageOfNodesToScore` 提前停止（第 00 篇讲过），所以同一个节点在两个调度周期里可能因为同批候选不同而得到不同的分数。

### 4.4 权重 3 意味着什么

默认 Profile 里，`NodeResourcesFit` 和 `NodeResourcesBalancedAllocation` 的权重都是 1，满分各 100；`TaintToleration` 一项的满分差距就是 300。只要候选节点之间的软污点个数有差异，它对最终排序的影响通常压过资源打分。这符合 `PreferNoSchedule` 的意图：「除非没有别的选择，否则别来」。如果把它当成一个很弱的偏好来用，比如给半个集群都打上软污点、希望只是「轻微倾向」另一半，实际效果会比预想的强得多。

## 五、QueueingHint

```go
// pkg/scheduler/framework/plugins/tainttoleration/taint_toleration.go
func (pl *TaintToleration) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeTaint}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodToleration}, QueueingHintFn: pl.isSchedulableAfterTargetPodTolerationChange},
	}, nil
}

func (pl *TaintToleration) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalNode, modifiedNode, err := util.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	wasUntolerated := true
	if originalNode != nil {
		_, wasUntolerated = v1helper.FindMatchingUntoleratedTaint(logger, originalNode.Spec.Taints, pod.Spec.Tolerations, helper.DoNotScheduleTaintsFilterFunc(), pl.enableTaintTolerationComparisonOperators)
	}

	_, isUntolerated := v1helper.FindMatchingUntoleratedTaint(logger, modifiedNode.Spec.Taints, pod.Spec.Tolerations, helper.DoNotScheduleTaintsFilterFunc(), pl.enableTaintTolerationComparisonOperators)

	if wasUntolerated && !isUntolerated {
		// ...
		return fwk.Queue, nil
	}
	// ...
	return fwk.QueueSkip, nil
}
```

节点事件的 hint 用的是和 Filter 完全相同的判断，只不过分别在旧节点和新节点上各跑一次：只有「之前这个 Pod 过不去，现在过得去」才返回 `Queue`。新增节点时没有旧节点，`wasUntolerated` 默认为 `true`，于是新节点只要对这个 Pod 没有不被容忍的硬污点就唤醒它。

和上一篇的 `NodeUnschedulable` 对比，这个 hint 是**针对具体 Pod** 的：它拿 Pod 的容忍去比较。一个节点删掉了 `dedicated=gpu:NoSchedule`，只会唤醒那些因为这个污点被拒绝、并且没有别的不被容忍污点的 Pod；对本来就容忍 GPU 污点的 Pod，`wasUntolerated` 为假，返回 `QueueSkip`。

只看硬污点：`PreferNoSchedule` 的变化不会让任何 Pod 从「不可调度」变成「可调度」，所以 hint 根本不考虑它。

hint 只判断事件中的这一个节点。它不知道 Pod 当初是被哪个节点拒绝的，也不知道这个节点是否还有资源。一个节点去掉污点，就会唤醒所有对它「之前不行、现在行」的 Pod，即使这些 Pod 实际上会因为资源不足在下一个周期里再次失败。这是 QueueingHint 的一般取舍：hint 只负责排除明显无关的事件，不负责预测调度结果。

Pod 容忍变化的 hint 无条件返回 `Queue`：

```go
// pkg/scheduler/framework/plugins/tainttoleration/taint_toleration.go
func (pl *TaintToleration) isSchedulableAfterTargetPodTolerationChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	// The updated Pod is the unschedulable Pod.
	logger.V(5).Info("a new toleration is added for the unschedulable Pod, and it may make it schedulable", "pod", klog.KObj(pod))
	return fwk.Queue, nil
}
```

`NodeUnschedulable` 在同样的事件上会检查新容忍是否覆盖 unschedulable 污点，而这里不检查。原因是 `TaintToleration` 关心的污点集合是开放的，要判断新容忍是否有用，得知道 Pod 当初被哪些节点的哪些污点拒绝，而这些信息没有保存在 Pod 上。容忍只能追加、不能修改或删除，这种更新本来就少见，直接唤醒的代价很小。

## 六、NoExecute：调度器只管一半

`NoExecute` 在表格里有两个作用：阻止调度，驱逐已运行的 Pod。调度器只负责前一半，而且是和 `NoSchedule` 完全相同的处理。后一半由另外两个组件完成。

### 6.1 kubelet 准入

绕过调度器（写了 `nodeName`）的 Pod，到了节点上会经过 kubelet 的准入检查。第 03 篇引用过 `generalFilter`，它对污点的检查是：

```go
// pkg/kubelet/lifecycle/predicate.go
	// Check taint/toleration except for static pods
	if !types.IsStaticPod(pod) {
		_, isUntolerated := corev1.FindMatchingUntoleratedTaint(logger, nodeInfo.Node().Spec.Taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
			// Kubelet is only interested in the NoExecute taint.
			return t.Effect == v1.TaintEffectNoExecute
		}, utilfeature.DefaultFeatureGate.Enabled(features.TaintTolerationComparisonOperators))
		if isUntolerated {
			reasons = append(reasons, &PredicateFailureError{tainttoleration.Name, tainttoleration.ErrReasonNotMatch})
		}
	}
```

只看 `NoExecute`，而且跳过静态 Pod。逻辑很直接：`NoSchedule` 是调度层面的约定，Pod 既然没有经过调度器，就不受它约束；`NoExecute` 是「不容忍就不能在这里运行」，kubelet 不接纳这样的 Pod，否则它刚启动就会被驱逐。静态 Pod 由节点本地的清单文件管理，通常是控制面组件，不能因为节点被打了污点就拒绝启动。

### 6.2 taint-eviction-controller

已经运行的 Pod 由 kube-controller-manager 里的 `taint-eviction-controller` 处理。它只关心 `NoExecute`：

```go
// pkg/controller/tainteviction/taint_eviction.go
func getNoExecuteTaints(taints []v1.Taint) []v1.Taint {
	result := []v1.Taint{}
	for i := range taints {
		if taints[i].Effect == v1.TaintEffectNoExecute {
			result = append(result, taints[i])
		}
	}
	return result
}
```

对节点上的每个 Pod，它判断这组 `NoExecute` 污点是否全部被容忍，以及容忍里的 `tolerationSeconds`：

```go
// pkg/controller/tainteviction/taint_eviction.go
	allTolerated, usedTolerations := v1helper.GetMatchingTolerations(logger, taints, tolerations)
	if !allTolerated {
		// ...
		// We're canceling scheduled work (if any), as we're going to delete the Pod right away.
		tc.cancelWorkWithEvent(logger, podNamespacedName)
		tc.taintEvictionQueue.AddWork(ctx, NewWorkArgs(podNamespacedName.Name, podNamespacedName.Namespace), now, now)
		return
	}
	minTolerationTime := getMinTolerationTime(usedTolerations)
	// getMinTolerationTime returns negative value to denote infinite toleration.
	if minTolerationTime < 0 {
		// ...
		tc.cancelWorkWithEvent(logger, podNamespacedName)
		return
	}

	startTime := now
	triggerTime := startTime.Add(minTolerationTime)
	// ...
	tc.taintEvictionQueue.AddWork(ctx, NewWorkArgs(podNamespacedName.Name, podNamespacedName.Namespace), startTime, triggerTime)
```

有不被容忍的 `NoExecute` 污点就立即删除；全部被容忍时，取所有用到的容忍里最小的 `tolerationSeconds` 作为宽限期，到期删除；没有任何容忍设置 `tolerationSeconds`，就永久容忍。`tolerationSeconds` 小于等于 0 视为立即删除。

`DefaultTolerationSeconds` 准入插件（默认启用）会给没有相关容忍的 Pod 加上 `node.kubernetes.io/not-ready:NoExecute` 和 `node.kubernetes.io/unreachable:NoExecute` 两条容忍，各 300 秒。所以节点失联后，普通 Pod 大约 5 分钟才被驱逐；DaemonSet 的 Pod 带的是不设 `tolerationSeconds` 的同名容忍（上一篇的 `AddOrUpdateDaemonPodTolerations`），永远不会因为节点失联被驱逐。

### 6.3 一个推论：not-ready 为什么有两个污点

node-lifecycle-controller 在节点 `Ready=False` 时会打两个同名污点：`doNoScheduleTaintingPass` 打 `node.kubernetes.io/not-ready:NoSchedule`，`doNoExecuteTaintingPass` 打 `node.kubernetes.io/not-ready:NoExecute`。

结合上面的分工就能理解为什么需要两个。每个 Pod 默认都容忍 `not-ready:NoExecute`（300 秒）。如果只有 `NoExecute` 版本，调度器的 Filter 会认为这些 Pod 容忍 not-ready 节点，新 Pod 就可能被调度到一个已经失联的节点上，再在 5 分钟后被驱逐。`NoSchedule` 版本不在默认容忍之内，它让调度器在 Filter 阶段直接排除 not-ready 节点，`NoExecute` 版本只负责已经在上面的 Pod 的宽限期。调度器不看 `tolerationSeconds`，这里正是靠两个 effect 把「能否调度」和「能运行多久」分开表达。

新节点还有一个和上一篇 cordon 类似的异步窗口：节点对象刚创建时 node-lifecycle-controller 还没来得及打 `not-ready:NoSchedule`，调度器可能把 Pod 放到一个还没准备好的节点上。这个窗口是由 API Server 的 `TaintNodesByCondition` 准入插件（`plugin/pkg/admission/nodetaint`，默认启用）在创建 Node 时同步补上这个污点来关闭的：

```go
// plugin/pkg/admission/nodetaint/admission.go
	// Taint node with NotReady taint at creation. This is needed to make sure
	// that nodes are added to the cluster with the NotReady taint. Otherwise,
	// a new node may receive the taint with some delay causing pods to be
	// scheduled on a not-ready node. Node controller will remove the taint
	// when the node becomes ready.
	addNotReadyTaint(node)
```

同一类问题，cordon 靠调度器多一个插件直接读 spec，新节点靠准入同步写污点，两种解法都是为了不依赖控制器的异步补写。

## 七、边界情况与常见误解

**污点只排斥，不吸引。** 上面的 GPU 例子里，Pod 容忍了 `dedicated=gpu`，只说明它**可以**去 GPU 节点，不说明它**会**去。想让它只去 GPU 节点，还要加节点亲和或 `nodeSelector`，那是[下一篇](/posts/sched-plugin-06-node-affinity/)的 `NodeAffinity`。专用节点的标准做法是「污点挡住别人 + 亲和拉来自己」两件套。

**`operator: Exists` 且 key 为空的容忍容忍一切。** 包括 `NoExecute`，包括 unschedulable。一些系统组件这样写是为了在任何节点上都能运行，业务 Pod 这样写会让 cordon、not-ready、专用节点全部失效。

**容忍里的 effect 写错。** 容忍写成 `effect: NoExecute`，节点污点是 `NoSchedule`，规则 1 判定不匹配，Pod 仍会被挡住。排查时注意 effect 是否完全一致，或者干脆留空。

**用 `kubectl taint` 去掉污点后 Pod 很快被调度。** 污点变化触发 `UpdateNodeTaint`，hint 返回 `Queue`。如果 Pod 之前只被这一个插件拒绝过，而且没有退避（第一次失败后退避 1 秒），它会在下一个调度循环就被尝试。

**Score 不看 `NoSchedule` 的容忍。** 一个 Pod 容忍了某个节点的硬污点，并不会因此「更倾向」那个节点。打分只和不被容忍的软污点个数有关。

## 八、发散

**三种 effect 的组合是一个两维矩阵的投影。** 污点要表达的其实是两个维度：调度时是硬约束还是软约束，运行时是否驱逐。`NoSchedule` 是「硬、不驱逐」，`PreferNoSchedule` 是「软、不驱逐」，`NoExecute` 是「硬、驱逐」。缺少的「软、驱逐」组合在语义上不成立（软约束允许调度上来，驱逐却要求不能在上面运行）。理解了这一点，就不会再去找「软的 NoExecute」。

**动态资源的设备污点走了同一条路。** DRA 引入了 `DRADeviceTaints`，设备也可以有污点，ResourceClaim 里可以写容忍，kube-controller-manager 里对应地多了一个 `device-taint-eviction-controller`。模式与节点污点完全一致：调度器负责「能不能分配」，控制器负责「已分配的要不要回收」。第 15 篇讲 `DynamicResources` 时会看到对应代码。

**生产配置建议。** 给节点打 `PreferNoSchedule` 前，先算一下权重 3 带来的影响（4.4 节）；需要表达一个连续的「节点质量」时，`TaintTolerationComparisonOperators` 的 `Gt`/`Lt` 比打一堆离散的污点更合适，但它目前是 Alpha，API Server、调度器、kubelet、controller-manager 的 gate 要一起打开，否则不同组件对同一个容忍的判断会不一致：调度器认为不匹配，API Server 甚至不允许创建。

## 九、小结

- Filter 只检查 `NoSchedule` 和 `NoExecute`，任一不被容忍即返回 `UnschedulableAndUnresolvable`（`node(s) had untolerated taint(s)`），不会触发抢占；调度时两种 effect 没有区别。
- PreScore 把 effect 为空或 `PreferNoSchedule` 的容忍写进 CycleState（`PreScoreTaintToleration`，`Clone` 不拷贝）；Score 数不被容忍的 `PreferNoSchedule` 个数；NormalizeScore 用 `100 − 100×raw/max` 反向归一化，再乘权重 3。
- 节点事件的 hint 对比新旧节点上这个 Pod 是否有不被容忍的硬污点；Pod 新增容忍时无条件 `Queue`。
- `NoExecute` 的驱逐不归调度器：kubelet 准入只看 `NoExecute`（静态 Pod 除外），taint-eviction-controller 按 `tolerationSeconds` 的最小值驱逐已运行的 Pod；not-ready/unreachable 同时有 `NoSchedule` 和 `NoExecute` 两个版本，分别管调度和运行。

污点只能把 Pod 挡在外面。[下一篇](/posts/sched-plugin-06-node-affinity/)读 `NodeAffinity`，看 Pod 怎么主动选择节点，以及它的 PreFilterResult 如何服务于 DaemonSet。
