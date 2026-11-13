---
title: 'kube-scheduler in-tree 插件源码拆解：24 个插件如何分工'
date: 2026-11-13T02:10:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "kube-scheduler", "插件"]
categories: ["Kubernetes"]
series: ["kube-scheduler"]
weight: 2
author: "LinPr"
description: "kube-scheduler 的调度语义全部写在插件里。本文从 NewInTreeRegistry 出发，先讲框架调用插件的四条规则，再逐个拆解 24 个 in-tree 插件：CycleState 里存了什么、Filter 返回哪种失败码、分数怎么算、什么事件会让 Pod 重新入队。"
---

上一篇[《从 OpenResty phase engine 到 scheduler framework》](/posts/scheduler-framework/)讲的是框架：扩展点怎么定义，插件按什么顺序被调用，失败之后怎么补偿。框架本身不包含任何调度语义，它不知道什么是资源不足，也不知道什么是污点。这些判断全部写在 `pkg/scheduler/framework/plugins/` 下的插件里。

这一篇读插件。只看插件的注释，每个插件一句话就能说完：`NodePorts` 检查端口冲突，`TaintToleration` 检查污点。但这样读完，回答不了生产上真正会遇到的问题：

- 为什么这个 Pod 没有触发抢占？
- 为什么节点资源释放了，Pod 还在 Pending？
- 为什么 `PodTopologySpread` 明明没配，Pod 还是被打散了？

这些问题的答案藏在三个细节里：**插件返回哪种失败码**、**插件把状态存在哪**、**插件关心哪些集群事件**。所以本文对每个插件都会尽量回答这三点。

先讲注册和默认配置，再讲框架调用插件的四条规则，这是读懂所有插件的前提；然后按过滤、打分、流程三组逐个拆解；最后用一张矩阵和几条结论收尾。

> 本文基于 `kubernetes` 主仓库 commit `aabdf5a2`。插件实现在 `pkg/scheduler/framework/plugins/`，框架在 `pkg/scheduler/framework/runtime/framework.go` 与 `pkg/scheduler/algorithm.go`，接口定义在 `staging/src/k8s.io/kube-scheduler/framework/`。

## 一、注册与启用是两件事

### 1.1 Registry 是一张名字到工厂的表

```go
// pkg/scheduler/framework/plugins/registry.go
func NewInTreeRegistry() runtime.Registry {
    fts := plfeature.NewSchedulerFeaturesFromGates(feature.DefaultFeatureGate)
    registry := runtime.Registry{
        dynamicresources.Name:  runtime.FactoryAdapter(fts, dynamicresources.New),
        tainttoleration.Name:   runtime.FactoryAdapter(fts, tainttoleration.New),
        nodename.Name:          runtime.FactoryAdapter(fts, nodename.New),
        // ...
        imagelocality.Name:     imagelocality.New,
        queuesort.Name:         queuesort.New,
        defaultbinder.Name:     defaultbinder.New,
    }
    return registry
}
```

`runtime.Registry` 就是 `map[string]PluginFactory`。键是配置里写的插件名，值是构造函数。

多数条目套了一层 `runtime.FactoryAdapter(fts, New)`，它把 feature gate 的快照 `fts` 传进构造函数。插件在构造时就决定了要不要启用某个新行为，运行期不再查询 feature gate。只有 `ImageLocality`、`PrioritySort`、`DefaultBinder` 三个不受任何 gate 影响，直接注册了 `New`。

表里一共 24 个插件。注册只说明「这个二进制里有这个插件」，是否调用由 Profile 决定。

### 1.2 默认 Profile：19 个插件，顺序就是语义

默认配置来自 `pkg/scheduler/apis/config/v1/default_plugins.go`，使用 `MultiPoint` 写法：所有插件列在一个列表里，框架按每个插件实现的接口，把它展开到对应的扩展点。

| 顺序 | 插件 | 权重 | 实现的扩展点 |
| --- | --- | --- | --- |
| 1 | `SchedulingGates` | — | PreEnqueue |
| 2 | `PrioritySort` | — | QueueSort |
| 3 | `NodeName` | — | PreFilter、Filter |
| 4 | `NodeUnschedulable` | — | PreFilter、Filter |
| 5 | `TaintToleration` | 3 | PreFilter、Filter、PreScore、Score |
| 6 | `NodeAffinity` | 2 | PreFilter、Filter、PreScore、Score |
| 7 | `NodePorts` | — | PreFilter、Filter |
| 8 | `NodeResourcesFit` | 1 | PreFilter、Filter、PreScore、Score、PlacementScore |
| 9 | `VolumeRestrictions` | — | PreFilter、Filter |
| 10 | `NodeVolumeLimits` | — | PreFilter、Filter |
| 11 | `VolumeBinding` | — | PreFilter、Filter、PreScore、Score、Reserve、PreBind |
| 12 | `VolumeZone` | — | PreFilter、Filter |
| 13 | `PodTopologySpread` | 2 | PreFilter、Filter、PreScore、Score |
| 14 | `InterPodAffinity` | 2 | PreFilter、Filter、PreScore、Score |
| 15 | `DynamicResources` | 2 | PreEnqueue、PreFilter、Filter、PostFilter、Score、Reserve、PreBind |
| 16 | `DefaultPreemption` | — | PreEnqueue、PostFilter |
| 17 | `NodeResourcesBalancedAllocation` | 1 | PreScore、Score |
| 18 | `ImageLocality` | 1 | Score |
| 19 | `DefaultBinder` | — | Bind |

同一个扩展点上的插件按列表顺序执行。Filter 遇到第一个失败就停止，所以把开销小、淘汰率高的插件放在前面，可以少做很多计算。`NodeName`、`NodeUnschedulable` 排在最前面，因为它们只比较一个字段；`InterPodAffinity` 排在后面，因为它要遍历其他 Pod。

顺序有时直接决定行为。`DynamicResources` 排在 `DefaultPreemption` 前面，源码给了理由：

```go
// DynamicResources should come before DefaultPreemption because if
// there is a problem with a Pod and PostFilter gets called to
// resolve the problem, it is better to first deallocate an
// idle ResourceClaim than it is to evict some Pod that might
// be doing useful work.
```

两者都实现了 PostFilter。PostFilter 遇到第一个返回 `Success` 的插件就停止，所以先尝试释放闲置的设备分配，不行再驱逐 Pod。

权重只对 Score 有意义。没写或写成 0 的权重会被改成 1；框架初始化时还会检查 `Σ 权重 × 100` 是否会溢出 `int64`。

### 1.3 由 feature gate 追加的 5 个

`applyFeatureGates` 根据 gate 往默认列表里追加插件：

| 插件 | 门控 | 追加位置 | 扩展点 |
| --- | --- | --- | --- |
| `NodeDeclaredFeatures` | `NodeDeclaredFeatures` | 末尾 | PreFilter、Filter |
| `DeferredPodScheduling` | `InPlacePodVerticalScalingSchedulerPreemption` | `NodeResourcesFit` 之前 | PreFilter、Filter、Permit |
| `GangScheduling` | `GenericWorkload` | 末尾 | PreEnqueue、PlacementFeasible |
| `TopologyPlacementGenerator` | `TopologyAwareWorkloadScheduling` | 末尾 | PlacementGenerate |
| `PodGroupPodsCount` | `TopologyAwareWorkloadScheduling` | 末尾，权重 1 | PlacementScore |

后三个处理的对象不是单个 Pod，而是 PodGroup，走的是 `PlacementGenerate → PlacementScore → PlacementFeasible` 这条新链路，第五节单独讲。

### 1.4 包名不等于插件名

`noderesources` 包里有两个插件：`NewFit` 注册为 `NodeResourcesFit`，`NewBalancedAllocation` 注册为 `NodeResourcesBalancedAllocation`。配置、日志、指标里用的都是插件名，所有名字的常量定义在 `plugins/names` 包里。

---

## 二、框架调用插件的四条规则

读插件之前，必须先知道框架怎么调用它们。插件返回同一个状态码，在不同扩展点上的后果完全不同。

### 2.1 PreFilter：Skip、提前终止与节点集合求交

PreFilter 在每个调度周期只调用一次，插件按顺序执行。返回值有四种处理方式：

| 返回 | 框架行为 |
| --- | --- |
| `Skip` | 记入 `SkipFilterPlugins`，这个插件的 Filter 和 PreFilterExtensions 本周期都不再调用 |
| `UnschedulableAndUnresolvable` | 立即返回，后续 PreFilter 不再执行 |
| `Unschedulable` | 记住这个失败，**继续执行**剩余 PreFilter，让抢占拿到完整的状态 |
| 其他非成功状态 | 包装成 `Error` |

`Skip` 是性能优化的关键。大多数 Pod 没有 hostPort、没有卷、没有亲和规则，对应插件在 PreFilter 返回 `Skip`，之后在几千个节点上的 Filter 调用就全部省掉了。

PreFilter 还可以返回 `PreFilterResult`，把候选节点缩小到一个集合。多个插件的结果**取交集**：

```go
// pkg/scheduler/framework/runtime/framework.go
if !r.AllNodes() {
    pluginsWithNodes.Insert(pl.Name())
}
result = result.Merge(r)
if !result.AllNodes() && len(result.NodeNames) == 0 {
    msg := fmt.Sprintf("node(s) didn't satisfy plugin(s) %v simultaneously", sets.List(pluginsWithNodes))
    // ...
    return result, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, msg), pluginsWithNodes
}
```

交集为空时直接判为 `UnschedulableAndUnresolvable`，Filter 都不用跑了。不在集合里的节点也会被标记为 `UnschedulableAndUnresolvable`，抢占不会考虑它们。

### 2.2 Filter：节点并行，插件串行，找够就停

对单个节点，Filter 插件按顺序串行执行，遇到第一个失败就停止，这个节点的失败原因就是那个插件的返回值。

对多个节点，框架用 `Parallelizer` 并行执行，默认并发度 16。它不会检查全部节点，找到足够数量的可行节点就取消剩余任务：

```go
// pkg/scheduler/algorithm.go
func (a *Algorithm) numFeasibleNodesToFind(percentageOfNodesToScore *int32, numAllNodes int32) (numNodes int32) {
    if numAllNodes < minFeasibleNodesToFind {          // 100
        return numAllNodes
    }
    // ...
    if percentage == 0 {
        percentage = max(int32(50)-numAllNodes/125, minFeasibleNodesPercentageToFind)   // 下限 5%
    }
    numNodes = numAllNodes * percentage / 100
    if numNodes < minFeasibleNodesToFind {
        return minFeasibleNodesToFind
    }
    return numNodes
}
```

默认的 `percentageOfNodesToScore` 是 0，触发自适应公式：

| 集群节点数 | 比例 | 找够多少个就停 |
| --- | --- | --- |
| < 100 | — | 全部 |
| 500 | 46% | 230 |
| 1000 | 42% | 420 |
| 5000 | 10% | 500 |
| ≥ 5625 | 5% | 节点数 × 5% |

为了让每个节点都有机会被检查，框架记录一个 `nextStartNodeIndex`，下一个 Pod 从上一次停下的位置继续往后找。

另外两条规则：

- 如果 Pod 有 `nominatedNodeName`（之前抢占过），先单独检查这个节点，通过就直接用；
- 检查节点时，会先把优先级不低于当前 Pod 的 nominated Pod 当作已经在节点上运行，跑一遍 Filter；通过后再不带它们跑一遍。两遍都通过才算可行。这样既不会抢走别人预定的资源，也不会因为预定者最终没来而误判。

### 2.3 Score：PreScore 可以 Skip，归一化后必须在 0 到 100 之间

打分分四步：

1. PreScore 按顺序执行，返回 `Skip` 的插件本周期不再打分；
2. 每个节点并行执行所有 Score 插件；
3. 每个插件并行执行 `NormalizeScore`（如果实现了）；
4. 检查范围，乘权重，求和。

```go
// pkg/scheduler/framework/runtime/framework.go
if score > fwk.MaxScore || score < fwk.MinScore {
    err := fmt.Errorf("plugin %q returns an invalid score %v, it should in the range of [%v, %v] after normalizing",
        pl.Name(), score, fwk.MinScore, fwk.MaxScore)
    // ...
}
weightedScore := score * int64(weights[i])
totalScore += weightedScore
```

超出 `[0, 100]` 不是截断，而是让整个调度周期失败。原始分数天然不在这个区间的插件，比如亲和权重之和、镜像大小，都必须实现 `NormalizeScore`。

只有一个可行节点时，Score 阶段整个跳过。

### 2.4 两种失败码，以及 Pod 什么时候被重新调度

所有插件的失败都落在两个状态码上：

| 状态码 | 含义 | 对抢占的影响 |
| --- | --- | --- |
| `Unschedulable` | 当前不满足，驱逐一些 Pod 后可能满足 | 该节点进入抢占候选 |
| `UnschedulableAndUnresolvable` | 驱逐任何 Pod 都无法满足 | 该节点直接排除 |

`DefaultPreemption` 只会在 Filter 返回 `Unschedulable` 的节点上寻找受害者。插件用错失败码，影响的是抢占能否发生：本该 `Unschedulable` 的写成了 `UnschedulableAndUnresolvable`，就等于在这个维度上关闭了抢占。

失败后的 Pod 进入不可调度队列。它什么时候回到活跃队列，由插件的 `EventsToRegister` 决定。每个插件声明自己关心哪些集群事件，并可以附带一个 `QueueingHintFn`：

```go
type ClusterEventWithHint struct {
    Event          ClusterEvent      // 例如 {Resource: Node, ActionType: Add | UpdateNodeTaint}
    QueueingHintFn QueueingHintFn    // 返回 Queue 或 QueueSkip
}
```

事件发生时，框架只调用「导致这个 Pod 失败的那些插件」的 hint 函数。返回 `Queue` 才重新入队，返回 `QueueSkip` 则继续等。这是 Pod 在资源释放后迟迟不被调度的常见原因：释放资源的事件，没有被「让它失败的那个插件」认领。

---

## 三、过滤类插件

### 3.1 `NodeName`：用 PreFilterResult 直接缩小范围

```go
// plugins/nodename/node_name.go
func (pl *NodeName) PreFilter(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
    if len(pod.Spec.NodeName) == 0 {
        return nil, nil
    }
    return &fwk.PreFilterResult{NodeNames: sets.New(pod.Spec.NodeName)}, nil
}
```

设置了 `spec.nodeName` 的 Pod，PreFilter 直接把候选节点缩到这一个。其他节点根本不会进入 Filter，这比逐个节点比较名字高效得多。

注意它没有返回 `Skip`：普通 Pod 的 PreFilter 返回 `nil, nil`，Filter 仍会对每个节点调用一次，只是立刻返回成功。Filter 失败时返回 `UnschedulableAndUnresolvable`，原因是 `node(s) didn't match the requested node name`。

### 3.2 `NodeUnschedulable`：cordon 也是一种污点

处理 `kubectl cordon` 设置的 `node.Spec.Unschedulable`。它的判定方式说明了 cordon 的本质：

- 节点 `spec.unschedulable` 为 false：通过；
- Pod 容忍 `node.kubernetes.io/unschedulable:NoSchedule` 污点：通过；
- 其他情况：`UnschedulableAndUnresolvable`，原因 `node(s) were unschedulable`。

DaemonSet 的 Pod 能被调度到 cordon 掉的节点上，就是因为 DaemonSet controller 自动给它们加了这个容忍。

它注册了两个事件：节点从不可调度变为可调度（或者新增一个可调度节点）；Pod 的容忍被修改为能容忍这个污点。两种情况都返回 `Queue`，其他节点变化一律 `QueueSkip`。

### 3.3 `NodePorts`：端口冲突可以触发抢占

PreFilter 从 Pod 中提取所有 `hostPort`，写进 CycleState：

```go
const preFilterStateKey = "PreFilter" + Name      // "PreFilterNodePorts"
type preFilterState []v1.ContainerPort
```

没有 hostPort 的 Pod 直接 `Skip`，绝大多数 Pod 走这条路径。

Filter 用 `(HostIP, Protocol, HostPort)` 三元组和节点已用端口比较。`0.0.0.0` 表示绑定所有地址，它与同协议同端口的任何 IP 都冲突。冲突时返回 **`Unschedulable`**，因为驱逐占用端口的 Pod 确实能解决问题，所以端口冲突可以触发抢占。

它的 QueueingHint 写得很精确：只有被删除的 Pod 确实占用了当前 Pod 需要的端口，才返回 `Queue`。别的 Pod 删除，不会唤醒等端口的 Pod。

### 3.4 `TaintToleration`：Filter 管硬污点，Score 管软污点

| 阶段 | 处理的污点效果 | 行为 |
| --- | --- | --- |
| Filter | `NoSchedule`、`NoExecute` | 有未容忍的污点即返回 `UnschedulableAndUnresolvable`，原因 `node(s) had untolerated taint(s)` |
| Score | `PreferNoSchedule` | 原始分是未容忍的软污点**数量** |

污点失败是 `UnschedulableAndUnresolvable`：驱逐 Pod 改变不了节点上的污点。

打分的原始值是「坏」的数量，越多越差，所以用反向归一化：

```go
// plugins/helper/normalize_score.go
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

这个辅助函数被很多插件复用。它的含义是「按最大值等比缩放到 0 到 100」，`reverse` 再翻转方向。注意它不是 min-max 归一化：所有节点都有 1 个软污点时，大家都得 0 分，而不是都得 100 分。

`TaintToleration` 的默认权重是 3，在默认插件里最高。

### 3.5 `NodeAffinity`：能缩小范围的亲和

处理三类约束：`nodeSelector`、`requiredDuringScheduling...`、`preferredDuringScheduling...`。此外，管理员可以通过插件参数 `addedAffinity` 给这个 Profile 下的所有 Pod 追加约束。

PreFilter 有一个容易被忽略的优化：如果 required 亲和里用了 `matchFields` 的 `metadata.name In [...]`，它会把节点名提取出来，作为 `PreFilterResult` 返回。同一个 term 内的条件取交集，多个 term 之间取并集；只要有一个 term 不含节点名条件，就不做缩小。

Filter 区分两种失败，都是 `UnschedulableAndUnresolvable`：

- `node(s) didn't match scheduler-enforced node affinity`，不满足 `addedAffinity`；
- `node(s) didn't match Pod's node affinity/selector`，不满足 Pod 自己的约束。

Score 是匹配上的 preferred term 的权重之和，再用 `DefaultNormalizeScore` 正向归一化。

QueueingHint 只在节点标签从「不匹配」变为「匹配」时返回 `Queue`。给节点打一个无关的 label，不会唤醒任何 Pod。

### 3.6 `NodeResourcesFit`：资源检查与三种打分策略

#### Filter：失败码取决于请求量是否超过节点总量

PreFilter 计算 Pod 的有效请求量，规则是 init 容器取最大值，应用容器求和，两者取较大值，再加上 overhead。结果存在 `PreFilterNodeResourcesFit` 键下。

Filter 逐项检查 Pod 数量、CPU、内存、临时存储和扩展资源，**收集所有不足的项**，而不是遇到第一个就返回：

```go
// plugins/noderesources/fit.go
statusCode := fwk.Unschedulable
for i := range insufficientResources {
    failureReasons = append(failureReasons, insufficientResources[i].Reason)
    if insufficientResources[i].Unresolvable {
        statusCode = fwk.UnschedulableAndUnresolvable
    }
}
```

`Unresolvable` 的判定条件是「请求量大于节点 allocatable」。请求 64 核的 Pod 遇到 32 核的节点，驱逐所有 Pod 也放不下，所以这个节点会被排除在抢占之外。只是当前剩余不足时返回 `Unschedulable`，可以触发抢占。

失败原因是 `Insufficient cpu`、`Insufficient memory`、`Too many pods` 这类字符串，`kubectl describe pod` 里看到的 `0/10 nodes are available: 3 Insufficient cpu` 就是把各节点的原因计数汇总后的结果。

#### Score：三种策略

打分时，没写 requests 的容器会被视为 CPU 100m、内存 200MiB。否则 BestEffort Pod 在所有节点上的分数都一样，失去区分度。

| 策略 | 单资源分数 | 偏好 |
| --- | --- | --- |
| `LeastAllocated`（默认） | `(capacity − requested) × 100 / capacity` | 剩余多的节点，打散 |
| `MostAllocated` | `requested × 100 / capacity` | 已分配多的节点，装箱 |
| `RequestedToCapacityRatio` | 按用户定义的分段线性曲线 | 自定义 |

多个资源按权重加权平均，默认 CPU、内存权重各为 1。`NodeResourcesFit` 没有实现 `NormalizeScore`，分数天然落在 0 到 100 之间。

它还实现了 `PlacementScore`，为 PodGroup 的整套放置方案打分。这里固定使用 MostAllocated 语义，把方案内所有节点的资源加总后计算，倾向于把一组 Pod 塞进更满的节点集合。

#### 重新入队的条件

- 一个已调度的 Pod 被删除：返回 `Queue`；
- 节点新增或 allocatable 变化：只有当前 Pod 能放进这个节点，并且某项 Pod 请求的资源确实增加了，才返回 `Queue`；
- 开启原地扩缩容后，Pod 缩容也会触发重新入队。

### 3.7 存储：四个插件各管一段

存储的判定拆成四个插件，是因为它们依赖的对象和生效时机都不同。

#### `VolumeRestrictions`：卷冲突与 ReadWriteOncePod

PreFilter 写入状态：

```go
type preFilterState struct {
    readWriteOncePodPVCs   sets.Set[string]   // Pod 用到的 RWOP PVC
    conflictingPVCRefCount int                // 这些 PVC 已被其他 Pod 使用的次数
}
```

Filter 检查两类问题，都返回 **`Unschedulable`**：

- 磁盘冲突：同一块 GCE PD、AWS EBS、iSCSI、RBD 卷被多个 Pod 以读写方式挂载，原因 `node(s) had no available disk`；
- RWOP 冲突：`conflictingPVCRefCount > 0`。

RWOP 冲突可以通过抢占解决，关键在于它实现了 `PreFilterExtensions`。抢占模拟时，框架每「移除」一个受害者，就调用 `RemovePod`，把计数减一；计数归零，Filter 就通过了。这是 PreFilterExtensions 存在的意义：**让 PreFilter 预计算的状态能够跟随抢占模拟增量更新**，不必重新计算。

#### `NodeVolumeLimits`：CSI 卷数量上限

读取 `CSINode.spec.drivers[].allocatable.count` 作为每个驱动的上限，统计节点上已有 Pod 的卷与 `VolumeAttachment`，去重后加上新 Pod 的卷。超出时返回 `Unschedulable`，原因 `node(s) exceed max volume count`。新 Pod 与已有 Pod 共享的卷不重复计数。

它的事件注册比较完整：`CSINode` 上限增加、`VolumeAttachment` 删除、挂卷的 Pod 删除，都可能唤醒等待中的 Pod。

#### `VolumeBinding`：唯一跨越五个扩展点的插件

| 扩展点 | 行为 |
| --- | --- |
| PreFilter | 检查 PVC 是否存在、是否在删除中；存在未绑定的 `Immediate` 模式 PVC 时直接失败；没有 PVC 则 `Skip` |
| Filter | 对每个节点调用 `FindPodVolumes`：已绑定的 PV 拓扑是否匹配，未绑定的 PVC 能否找到可用 PV 或动态供给 |
| Score | 按存储利用率打分，**只在 `StorageCapacityScoring` gate 开启时启用** |
| Reserve | `AssumePodVolumes`，在调度器缓存中「假定」绑定 |
| PreBind | 真正调用 API 完成 PV/PVC 绑定，等待完成，默认超时 600 秒 |

CycleState 里存的是每个节点的卷决策：

```go
type stateData struct {
    allBound          bool
    podVolumesByNode  map[string]*PodVolumes   // Filter 阶段为每个节点计算的绑定方案
    podVolumeClaims   *PodVolumeClaims
    hasStaticBindings bool
    sync.Mutex                                  // Filter 并行写入，需要加锁
}
```

Filter 的所有失败都是 `UnschedulableAndUnresolvable`：

- `node(s) didn't find available persistent volumes to bind`
- `node(s) didn't match PersistentVolume's node affinity`
- `node(s) did not have enough free storage`
- `node(s) unavailable due to one or more pvc(s) bound to non-existent pv(s)`

`WaitForFirstConsumer` 模式下，PV 的选择推迟到 Pod 选定节点之后。这正是它必须横跨 Filter、Reserve、PreBind 的原因：Filter 为每个节点算方案，Reserve 选定方案，PreBind 落地。

#### `VolumeZone`：未绑定 PV 的区域约束

PreFilter 收集 Pod 所用 PV 的 zone/region 标签，`WaitForFirstConsumer` 模式下尚未绑定的 PVC 跳过，交给 `VolumeBinding` 处理。Filter 要求节点的 zone/region 标签与 PV 一致，同时兼容 beta 和 GA 两套标签名。节点没有任何区域标签时直接通过。不匹配返回 `UnschedulableAndUnresolvable`，原因 `node(s) had no available volume zone`。

### 3.8 `InterPodAffinity`：亲和不可抢占，反亲和可以

PreFilter 预先算好按拓扑域聚合的计数：

```go
type preFilterState struct {
    // 已有 Pod 的反亲和规则匹配了当前 Pod，按拓扑域计数
    existingClusterWideAntiAffinityCounts topologyToMatchedTermCount
    // 当前 Pod 的亲和/反亲和规则匹配到的已有 Pod，按拓扑域计数
    clusterWideAffinityCounts             topologyToMatchedTermCount
    clusterWideAntiAffinityCounts         topologyToMatchedTermCount
    // ...
}
type topologyToMatchedTermCount map[topologyPair]int64    // topologyPair{key, value}
```

Filter 时只需要查表：取节点的拓扑标签值，看对应域的计数是否满足。三项检查按顺序进行：

| 检查 | 失败码 | 原因 |
| --- | --- | --- |
| 当前 Pod 的亲和规则 | `UnschedulableAndUnresolvable` | `node(s) didn't match pod affinity rules` |
| 当前 Pod 的反亲和规则 | `Unschedulable` | `node(s) didn't match pod anti-affinity rules` |
| 已有 Pod 的反亲和规则 | `Unschedulable` | `node(s) didn't satisfy existing pods anti-affinity rules` |

失败码的差异是有道理的：亲和不满足，说明目标域里**缺少**某个 Pod，驱逐 Pod 只会让情况更糟；反亲和不满足，说明目标域里**多了**某个 Pod，驱逐它就能解决。

亲和规则有一个特例：如果 Pod 的亲和规则匹配它自己（例如一个 Deployment 要求所有副本在同一个 zone），而集群里还没有任何匹配的 Pod，第一个副本会被放行。否则这种自引用的规则永远无法满足。

打分阶段的 PreScore 汇总四种贡献：当前 Pod 的 preferred 亲和（加分）与反亲和（减分），已有 Pod 的 preferred 亲和与反亲和，以及已有 Pod 的 required 亲和（加 `hardPodAffinityWeight`，默认 1）。结果是一个按拓扑域的分数表，Score 只需查表，`NormalizeScore` 做 min-max 归一化。

这个插件在大集群里开销最大：PreFilter 需要遍历所有带亲和规则的 Pod。新版本增加了 `InterPodAffinityHostnameFastPath`，以 `kubernetes.io/hostname` 为拓扑键的规则不再构建全局表，改为在 Filter 中按节点就地检查。

### 3.9 `PodTopologySpread`：偏差的计算方式

#### Filter：只看最小的那个域

`DoNotSchedule` 约束参与 Filter，`ScheduleAnyway` 约束参与 Score。PreFilter 计算每个拓扑域中匹配 selector 的 Pod 数，并维护一个 `criticalPaths`，记录匹配数最少的两个域，这样 Filter 不用每次都扫描全部域找最小值。

```go
// plugins/podtopologyspread/filtering.go
// 域的数量少于 MinDomains 时，最小匹配数视为 0
skew := matchNum + selfMatchNum - minMatchNum
if skew > int(c.MaxSkew) {
    return fwk.NewStatus(fwk.Unschedulable, ErrReasonConstraintsNotMatch)
}
```

`selfMatchNum` 是当前 Pod 自己是否匹配 selector，匹配为 1。也就是说，判定的是「放上去之后」的偏差。

两种失败：

- 节点缺少约束要求的拓扑标签：`UnschedulableAndUnresolvable`；
- 偏差超过 `maxSkew`：`Unschedulable`，驱逐 Pod 可以降低偏差。

统计时，哪些节点算在内由 `nodeAffinityPolicy`（默认 `Honor`，只统计满足 Pod 节点亲和的节点）和 `nodeTaintsPolicy`（默认 `Ignore`，不考虑污点）决定。

#### Score：对数加权

```go
// plugins/podtopologyspread/scoring.go
func topologyNormalizingWeight(size int) float64 {
    return math.Log(float64(size + 2))
}
func scoreForCount(cnt int64, maxSkew int32, tpWeight float64) float64 {
    return float64(cnt)*tpWeight + float64(maxSkew-1)
}
```

原始分是「加权后的匹配数」，越小越好。权重是 `log(域数量 + 2)`：域越多的约束，权重越大。同时有 hostname 和 zone 两个约束时，hostname 有几百个域，zone 只有三个，单个节点上多一个 Pod 的影响就比单个 zone 多一个 Pod 大。`NormalizeScore` 把它反转成「越分散越高分」。

#### 默认约束

默认 `defaultingType: System`。Pod 没有声明任何约束时，会自动套用：

```go
// plugins/podtopologyspread/plugin.go
var systemDefaultConstraints = []v1.TopologySpreadConstraint{
    {TopologyKey: v1.LabelHostname, WhenUnsatisfiable: v1.ScheduleAnyway, MaxSkew: 3},
    {TopologyKey: v1.LabelTopologyZone, WhenUnsatisfiable: v1.ScheduleAnyway, MaxSkew: 5},
}
```

selector 从 Pod 所属的 Service、ReplicaSet、StatefulSet 推导。所以同一个 Deployment 的副本，即使没配任何打散规则，打分时也会倾向于分散到不同节点和 zone。都是 `ScheduleAnyway`，只影响打分，不会导致调度失败。

### 3.10 `DynamicResources`：失败几乎都不可抢占

DRA 的调度侧实现。Pod 通过 `ResourceClaim` 申请设备，插件负责为它分配具体设备。

| 扩展点 | 行为 |
| --- | --- |
| PreEnqueue | Pod 引用的 claim 还没被创建时，不进入调度队列 |
| PreFilter | 读取 claim、DeviceClass、ResourceSlice，构建分配器；没有 claim 则 `Skip` |
| Filter | 在当前节点上尝试分配，结果缓存到 `nodeAllocations[node]` |
| PostFilter | 随机挑一个闲置的已分配 claim，释放它的分配 |
| Score | 仅开启 `DRAPrioritizedList` 时有效：选中的候选设备越靠前，分越高 |
| Reserve | 标记 claim 分配进行中 |
| PreBind | 写入分配结果和 `ReservedFor`，等待设备的 binding conditions |

它的失败码有一个特点：内部的 `statusUnschedulable()` 总是返回 **`UnschedulableAndUnresolvable`**。设备分配失败，驱逐普通 Pod 一般腾不出设备，所以默认不走 Pod 抢占，而是走自己的 PostFilter，释放闲置 claim。

另一个特殊处理：分配器计算超时（`filterTimeout`，开启对应 gate 时默认 10 秒）时返回 `Error`，而不是不可调度。这样 Pod 会进入退避，不会因为一次计算超时就被判定为「放不下」。

### 3.11 `NodeDeclaredFeatures`：资源之外的兼容性

节点在 `status` 中声明自己支持的特性，插件从 Pod spec 推断出它需要的特性集合，Filter 检查子集关系。不满足返回 `UnschedulableAndUnresolvable`，原因 `node(s) didn't match Pod's required features`。

它解决的是 requests 表达不了的兼容性问题。某些 Pod 字段只有新版 kubelet 才支持，混合版本集群在升级期间，可以用它避免把 Pod 调度到旧节点。

---

## 四、打分类插件

### 4.1 打分方向与归一化方式

默认 Profile 中，参与打分的插件有 9 个：

| 插件 | 权重 | 原始分含义 | 归一化 |
| --- | --- | --- | --- |
| `TaintToleration` | 3 | 未容忍的软污点数 | `DefaultNormalizeScore` 反向 |
| `NodeAffinity` | 2 | 匹配的 preferred 权重和 | `DefaultNormalizeScore` 正向 |
| `PodTopologySpread` | 2 | 加权匹配数 | 自定义，反向 |
| `InterPodAffinity` | 2 | 拓扑域分数和，可为负 | min-max |
| `DynamicResources` | 2 | 候选设备的排序位置 | `DefaultNormalizeScore` 正向 |
| `NodeResourcesFit` | 1 | 策略决定，已在 0 到 100 | 无 |
| `NodeResourcesBalancedAllocation` | 1 | 均衡度改善量，已在 50 到 100 | 无 |
| `ImageLocality` | 1 | 镜像大小，已缩放到 0 到 100 | 无 |
| `VolumeBinding` | — | 存储利用率 | 无，需 gate 开启 |

权重为 2 和 3 的插件全部和「约束」有关，权重为 1 的全部和「资源」有关。默认配置的倾向很明确：先满足用户声明的偏好，再考虑资源利用。

### 4.2 `NodeResourcesBalancedAllocation`：评估的是改善量

这个插件的公式在近几个版本中变过，读旧资料时要注意：

```go
// plugins/noderesources/balanced_allocation.go
scoreWithPod := balancedResourceScore(requested, allocatable)
scoreWithoutPod := balancedResourceScore(allocated, allocatable)
return fwk.MaxNodeScore/2 + (fwk.MaxNodeScore/2+scoreWithPod-scoreWithoutPod)/2
```

`balancedResourceScore` 计算各资源使用率的标准差，返回 `(1 − std) × 100`。两种资源时，标准差就是两个使用率之差的一半。

新公式比较的是「放上去之后」和「放上去之前」的均衡度：

- 放上去让节点更均衡，分数趋向 100；
- 放上去没有变化，约 75 分；
- 放上去让节点更失衡，分数趋向 50。

旧公式只看放上去之后的绝对均衡度。差别在于：一个本来就很失衡的节点，旧公式下永远低分；新公式下，如果这个 Pod 恰好能补齐短板，它会得到高分。

另外两个细节：它用的是 Pod 的**真实** requests，而不是 `NodeResourcesFit` 那样的默认值替代；BestEffort Pod 在 PreScore 直接 `Skip`。

### 4.3 `ImageLocality`：镜像越大，本地缓存越值钱

```go
// plugins/imagelocality/image_locality.go
const (
    mb                    int64 = 1024 * 1024
    minThreshold          int64 = 23 * mb
    maxContainerThreshold int64 = 1000 * mb
)

func calculatePriority(sumScores int64, imageCount int) int64 {
    maxThreshold := maxContainerThreshold * int64(imageCount)
    if sumScores < minThreshold {
        sumScores = minThreshold
    } else if sumScores > maxThreshold {
        sumScores = maxThreshold
    }
    return fwk.MaxScore * (sumScores - minThreshold) / (maxThreshold - minThreshold)
}
```

`sumScores` 是节点上已存在的镜像大小之和，但每个镜像都乘了一个扩散系数：

```text
镜像贡献 = 镜像大小 × (拥有该镜像的节点数 / 集群节点总数)
```

这个系数用来防止「热点」：如果一个镜像只在一个节点上，不乘系数的话，所有用这个镜像的 Pod 都会挤到那个节点。乘上系数后，只有在较多节点上都存在的镜像，才会产生明显的加分。

23MB 以下不加分，每个镜像来源 1000MB 封顶，中间线性。镜像来源包括普通容器、init 容器和镜像卷。

---

## 五、流程类插件

### 5.1 `SchedulingGates`：在入队之前拦住

```go
func (pl *SchedulingGates) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
    if len(p.Spec.SchedulingGates) == 0 {
        return nil
    }
    gates := make([]string, 0, len(p.Spec.SchedulingGates))
    for _, gate := range p.Spec.SchedulingGates {
        gates = append(gates, gate.Name)
    }
    return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("waiting for scheduling gates: %v", gates))
}
```

PreEnqueue 在 Pod 进入活跃队列之前调用。带有 `schedulingGates` 的 Pod 不会进入活跃队列，不参与任何调度周期，也不消耗调度器的计算资源。外部控制器移除全部 gate 后，`UpdatePodSchedulingGatesEliminated` 事件把它放回队列。

这是一个「队列侧」插件。它和 Filter 的区别在于：Filter 失败的 Pod 每次被唤醒都要完整地跑一遍调度周期，PreEnqueue 拦住的 Pod 一次都不会跑。

### 5.2 `PrioritySort`：唯一的 QueueSort

```go
func (pl *PrioritySort) Less(entity1, entity2 fwk.QueuedEntityInfo) bool {
    p1 := entity1.GetPriority()
    p2 := entity2.GetPriority()
    return (p1 > p2) || (p1 == p2 && entity1.GetTimestamp().Before(entity2.GetTimestamp()))
}
```

优先级高的先出队，相同时入队早的先出队。参数类型是 `QueuedEntityInfo` 而不是 `QueuedPodInfo`，因为队列里的元素现在可以是单个 Pod，也可以是一个 PodGroup。

一个 Profile 只能有一个 QueueSort 插件，而且**所有 Profile 的 QueueSort 必须相同**，因为调度队列是全局共享的。

### 5.3 `DefaultBinder`：最后一步写 API

构造一个 `v1.Binding` 对象（目标为 `Kind: Node`），调用 API Server 的 binding 子资源。Bind 扩展点上的插件按顺序调用，第一个不返回 `Skip` 的插件负责绑定，`DefaultBinder` 排在最后，作为兜底。

### 5.4 `DefaultPreemption`：抢占的完整流程

所有节点都没通过 Filter 时，进入 PostFilter，`DefaultPreemption` 尝试驱逐低优先级 Pod 腾出位置。

#### 第一步：是否有资格抢占

- `preemptionPolicy: Never` 的 Pod 不抢占；
- 如果 Pod 之前已经抢占过（有 `nominatedNodeName`），而那个节点上还有低优先级 Pod 正在因抢占而终止，**本次不再抢占**，等它们退出。避免一个 Pod 反复驱逐。

#### 第二步：确定候选节点

只有 Filter 返回 `Unschedulable` 的节点才会被考虑。这就是 2.4 节说的，失败码决定了抢占范围。

候选节点数量：

```text
numCandidates = max(N × percentage / 100, absolute)
```

`N` 是节点数，`percentage` 与 `absolute` 分别对应参数 `minCandidateNodesPercentage`（默认 10）和 `minCandidateNodesAbsolute`（默认 100），结果不超过节点总数。

从一个随机偏移开始遍历，避免每次都从同一批节点里挑受害者。

#### 第三步：在每个候选节点上模拟

对每个节点并行执行 `SelectVictimsOnNode`：

1. 找出所有优先级低于抢占者的 Pod，作为潜在受害者；
2. 把它们全部从节点上「移除」，并调用各插件的 `RemovePod` 扩展更新 CycleState；
3. 运行 Filter，如果还是放不下，这个节点不可行；
4. 按重要性从高到低排列受害者，依次是优先级、PodGroup 类型、组大小、启动时间；
5. 根据 PDB 把受害者分成「驱逐会违反 PDB」和「不会违反」两组；
6. **尝试赦免**：先处理违反 PDB 的那组，再处理不违反的；每组内从最重要的开始，逐个加回节点，加回后仍能放下就不驱逐。

这样得到的受害者集合是「刚好够用」的，而且尽量保留了重要的 Pod 和受 PDB 保护的 Pod。PDB 在这里是软约束：如果只有违反 PDB 才能腾出位置，仍然会驱逐，只是这个候选在下一步排序中会靠后。

#### 第四步：选一个节点

1. PDB 违反数最少；
2. 受害者中的最高优先级最低；
3. 受害者优先级之和最小；
4. 受害者数量最少；
5. 受害者中最高优先级 Pod 的启动时间最晚，即尽量驱逐刚启动的；
6. 仍然相同，取第一个。

每一步只保留得分最高的节点，剩一个时停止。

#### 第五步：执行

驱逐受害者，把抢占者的 `nominatedNodeName` 设为选中的节点。注意抢占者并不会立刻绑定：它回到队列，等受害者真正退出后再走一次正常的调度周期。

开启 `AsyncPreemption` 后，驱逐在后台 goroutine 中执行，调度器不必等 API 调用完成就能处理下一个 Pod。此时 `DefaultPreemption` 的 PreEnqueue 会拦住这个抢占者，在驱逐完成之前不让它再次进入调度。

### 5.5 `DeferredPodScheduling`：调度器参与原地扩缩容

原地扩缩容（in-place resize）允许修改运行中 Pod 的 resources。如果节点剩余资源不够，kubelet 会把 resize 标记为 deferred，等资源释放后再执行。问题是：资源可能永远不会释放。

`InPlacePodVerticalScalingSchedulerPreemption` 让调度器介入。被 defer 的 Pod 虽然已经运行在节点上，仍会以新的资源需求重新进入调度器：

| 阶段 | 行为 |
| --- | --- |
| 其他 Filter 插件 | 几乎全部对这种 Pod 返回 `Skip`，只有 `NodeName` 和 `NodeResourcesFit` 参与 |
| `NodeResourcesFit` | 只检查当前节点上**新增的那部分**请求量是否放得下 |
| `DeferredPodScheduling` Filter | 不是当前节点则 `UnschedulableAndUnresolvable`；节点关闭了 resize 抢占也是 |
| PostFilter | 放不下时正常走抢占，驱逐同节点的低优先级 Pod |
| `DeferredPodScheduling` Permit | 放得下时**拒绝**：`pod resize fits, waiting for Kubelet actuation` |

最后一步看起来反直觉：能放下为什么要拒绝？因为这个 Pod 已经在节点上了，不需要也不能再 Bind。调度器在这里的作用只是「借用抢占机制腾出空间」，空间够了，剩下的事交给 kubelet。Permit 返回 `UnschedulableAndUnresolvable`，Pod 留在不可调度队列，等 kubelet 完成 resize。

### 5.6 PodGroup 链路：`GangScheduling` 与拓扑放置

传统调度周期处理「一个 Pod 选一个节点」。`GenericWorkload` 引入了 PodGroup，调度对象变成「一组 Pod 选一组节点」。

#### `GangScheduling`

PreEnqueue 检查 PodGroup 是否已创建、成员是否已经到齐（满足 quorum），不满足时 Pod 不入队：

```text
pod group %q or one of its composite pod group ancestors has not been created yet
waiting for %s to meet its quorum
```

PlacementFeasible 判断一个放置方案能否满足 gang 语义：

```go
// plugins/gangscheduling/gangscheduling.go
if remaining+scheduled < minCount {
    return fwk.NewStatus(fwk.Unschedulable, "...cannot be satisfied...")
}
if scheduled < minCount {
    return fwk.NewStatus(fwk.Wait, "...is not yet satisfied...")
}
return nil
```

三种结果：剩余加已调度也凑不够 `minCount`，不可行；够但还没调度完，等待；已满足，通过。

#### `TopologyPlacementGenerator` 与 `PodGroupPodsCount`

- `TopologyPlacementGenerator`（PlacementGenerate）：读取 PodGroup 的拓扑约束键（如 `topology.kubernetes.io/zone`），按标签值把候选节点分组，每组是一个候选方案。如果组内已有 Pod 被调度，只保留它们所在的那个域。
- `PodGroupPodsCount`（PlacementScore）：方案的原始分是「已调度数 + 该方案能放下的数量」，按最大值归一化。能装下更多组员的方案得分更高。
- `NodeResourcesFit` 的 PlacementScore 也参与这一步，倾向于更满的节点集合。

这些插件的方法签名和传统扩展点不同，参数是 `PodGroupInfo` 和 `Placement`，而不是 `*v1.Pod` 和 `NodeInfo`。这部分 API 仍在演进。

---

## 六、插件 × 扩展点矩阵

把前面各节的内容按扩展点重新组织，就是下面这张图。左边一列是扩展点，可以看成一个数组；每一格后面挂着一条链表，节点就是这个扩展点上依次执行的插件，顺序与 1.2 节的默认列表一致。扩展点名字下面的小字，是框架遍历这条链的规则：

{{< figure src="/images/posts/kubernetes-scheduler-in-tree-plugins/extension-chains.svg" link="/images/posts/kubernetes-scheduler-in-tree-plugins/extension-chains.svg" caption="默认 Profile 中每个扩展点上的插件链，虚线为需要 feature gate 开启的插件（点击查看大图）" >}}

同一个插件会出现在多条链上，例如 `VolumeBinding` 出现在 PreFilter、Filter、PreScore、Score、Reserve、PreBind 六条链里，它在每条链上做的是不同的事。遍历规则则决定了顺序有没有意义：Filter 链遇到第一个失败就停，顺序只影响性能；PostFilter 链遇到第一个成功就停，顺序直接决定由谁来补救；Score 链全部执行后求和，顺序无关紧要。

自己往这些链上追加插件时，新插件默认排在链尾。如何把它插到指定位置，下一篇的 [1.2 节](/posts/kubernetes-scheduler-out-of-tree-plugins/#12-与-in-tree-插件共用扩展点时顺序怎么定)结合源码讲了完整的规则。

同样的信息写成表格：

| 扩展点 | 插件 |
| --- | --- |
| `PreEnqueue` | `SchedulingGates`、`DynamicResources`、`DefaultPreemption`（异步抢占）、`GangScheduling` |
| `QueueSort` | `PrioritySort` |
| `PreFilter` | `NodeName`、`NodeUnschedulable`、`TaintToleration`、`NodeAffinity`、`NodePorts`、`NodeResourcesFit`、`VolumeRestrictions`、`NodeVolumeLimits`、`VolumeBinding`、`VolumeZone`、`PodTopologySpread`、`InterPodAffinity`、`DynamicResources`、`NodeDeclaredFeatures`、`DeferredPodScheduling` |
| `Filter` | 与 PreFilter 相同的 15 个 |
| `PostFilter` | `DynamicResources`、`DefaultPreemption` |
| `PreScore` | `TaintToleration`、`NodeAffinity`、`NodeResourcesFit`、`VolumeBinding`、`PodTopologySpread`、`InterPodAffinity`、`NodeResourcesBalancedAllocation` |
| `Score` | `TaintToleration`、`NodeAffinity`、`NodeResourcesFit`、`VolumeBinding`、`PodTopologySpread`、`InterPodAffinity`、`DynamicResources`、`NodeResourcesBalancedAllocation`、`ImageLocality` |
| `Reserve` | `VolumeBinding`、`DynamicResources` |
| `Permit` | `DeferredPodScheduling` |
| `PreBind` | `VolumeBinding`、`DynamicResources` |
| `Bind` | `DefaultBinder` |
| `PostBind` | 无 |
| `PlacementGenerate` | `TopologyPlacementGenerator` |
| `PlacementScore` | `NodeResourcesFit`、`PodGroupPodsCount` |
| `PlacementFeasible` | `GangScheduling` |

另外，绝大多数过滤和打分插件还实现了 `SignPlugin`：为 Pod 生成调度签名片段，签名相同的 Pod 可以复用之前的调度结果，用于批量调度优化。只要有一个启用的过滤或打分插件没实现它，批量优化就会对所有 Pod 关闭。

把失败码也标上，能更清楚地看出抢占的边界：

| 失败码 | 插件 |
| --- | --- |
| 只返回 `UnschedulableAndUnresolvable` | `NodeName`、`NodeUnschedulable`、`TaintToleration`、`NodeAffinity`、`VolumeBinding`、`VolumeZone`、`DynamicResources`、`NodeDeclaredFeatures`、`DeferredPodScheduling` |
| 视情况返回两种 | `NodeResourcesFit`（请求超过 allocatable 时不可解决）、`InterPodAffinity`（亲和不可解决，反亲和可解决）、`PodTopologySpread`（缺标签不可解决，偏差可解决） |
| 只返回 `Unschedulable` | `NodePorts`、`VolumeRestrictions`、`NodeVolumeLimits` |

能触发抢占的维度只有：资源、端口、反亲和、拓扑打散、卷冲突、卷数量。节点身份、污点、节点亲和、存储拓扑、设备，都不会。

---

## 七、几条从源码里得到的结论

**1. Pod 不被抢占，先看是哪个插件拒绝的。**

`kubectl describe pod` 中 FailedScheduling 事件会列出每个节点的失败原因。如果原因来自上面第一组插件，这些节点不会进入抢占候选，再高的优先级也没用。

**2. 资源释放了 Pod 还 Pending，看 QueueingHint。**

Pod 失败后只能被「拒绝它的那些插件」注册的事件唤醒。比如 `NodePorts` 只在占用了相同端口的 Pod 被删除时才唤醒，删掉别的 Pod 没用。最终兜底是不可调度队列的定期刷新，但那需要等待更长时间。

**3. 默认就有打散。**

`PodTopologySpread` 的系统默认约束会让同一个工作负载的副本在打分时倾向于分散，hostname maxSkew 3，zone maxSkew 5。需要把副本集中放置时，要显式关掉 `defaultingType` 或调整权重。

**4. 权重不是都为 1。**

`TaintToleration` 为 3，`NodeAffinity`、`PodTopologySpread`、`InterPodAffinity`、`DynamicResources` 为 2。修改权重前先确认默认值，否则以为是「加一点偏好」，实际可能翻倍。

**5. `percentageOfNodesToScore` 影响的不只是性能。**

大集群中默认只找一部分可行节点就停止。打分只在这部分节点中进行，选出的是「局部最优」。对调度质量要求高、节点数又不多的场景，可以调高这个值。

**6. PreFilterExtensions 决定抢占能否准确模拟。**

插件如果在 PreFilter 中预计算了依赖其他 Pod 的状态（如 `VolumeRestrictions` 的 RWOP 计数、`InterPodAffinity` 的拓扑计数），就必须实现 `AddPod`/`RemovePod`。否则抢占模拟时状态不会更新，模拟结果就是错的。自定义插件时尤其要注意。

---

## 八、结语

24 个插件，单独看都很小：`NodeName` 比较一个字符串，`NodePorts` 查一张端口表，`ImageLocality` 算一个比例。它们能组成一个可用的调度器，是因为框架把「什么时候调用」「失败意味着什么」「什么时候重试」这些问题统一收走了，插件只需要回答一个很窄的问题。

但窄不等于简单。每个插件在回答这个窄问题时，都要同时做出三个决定：

1. **状态放在哪个扩展点算**：放 PreFilter 只算一次，放 Filter 要算 N 次；
2. **失败返回哪个码**：决定了抢占会不会发生；
3. **关心哪些事件**：决定了失败的 Pod 什么时候能再试一次。

读懂这三个决定，就能解释调度器的大部分行为。下一篇[《scheduler-plugins 源码拆解》](/posts/kubernetes-scheduler-out-of-tree-plugins/)看另一侧：当内置插件不够用时，社区插件如何在同一套框架里做这三个决定，以及它们比 in-tree 插件多出来的那个问题：数据从哪来。
