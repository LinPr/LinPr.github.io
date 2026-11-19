---
title: 'scheduler-plugins 源码拆解：15 个 out-of-tree 插件的实现与取舍'
date: 2026-11-13T02:20:00+08:00
lastmod: 2026-11-20T10:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "scheduling-framework"]
categories: ["Kubernetes"]
series: ["kube-scheduler"]
author: "LinPr"
description: "配额、gang、实时负载、NUMA、网络代价、seccomp……sigs.k8s.io/scheduler-plugins 把 in-tree 装不下的调度能力做成了插件。本文从源码出发，拆解每个插件的状态放在哪、公式怎么算、失败返回什么，以及那些只有读代码才看得到的边界情况。"
---

> 本文是总览。每个插件更深入的源码拆解（CycleState、失败码、打分算例、QueueingHint、边界情况）见系列[《调度插件逐个读》](/series/scheduler-plugins/)，从[开篇](/posts/sched-plugin-00-journey/)读起；out-of-tree 部分从第 25 篇 [QOSSort](/posts/sched-plugin-25-qos-sort/) 开始。文中各插件小节也链接到了对应的单篇。

上一篇[《kube-scheduler 的 in-tree 插件》](/posts/kubernetes-scheduler-in-tree-plugins/)读的是随 Kubernetes 发布的内置插件。它们有一个共同点：判定所需的数据全部来自 API 对象，Pod、Node、PV、PVC，调度器自己的 informer 就能拿到。

社区仓库 `sigs.k8s.io/scheduler-plugins` 里的插件不满足这个前提。弹性配额要知道每个租户已经用了多少，gang 调度要知道同组的 Pod 到了几个，负载感知要知道节点**真实**的 CPU 使用率，NUMA 对齐要知道 kubelet 在每个 NUMA 节点上还剩多少资源。这些数据要么不在 API 里，要么需要跨 Pod 聚合，要么需要外部系统采集。

所以读这个仓库时，我关心的不是「每个插件做什么」，那一行注释就能说清；而是三个更具体的问题：

1. **状态放在哪？** CycleState、插件进程内存、还是 CRD？谁负责更新？
2. **公式是什么？** 打分到底在比较什么，边界值怎么处理？
3. **数据缺失时怎么办？** 指标没采到、CRD 不存在、缓存过期时，插件返回什么？

第三个问题最容易被忽略，也最容易在生产上出事。下面先看运行方式，再按配额、成组、负载、拓扑、网络、其他六组逐个拆。

> 本文基于 `scheduler-plugins` commit `cfeb3aa`，依赖 `k8s.io/kubernetes v1.36.4`。只引用 `cmd/`、`pkg/`、`apis/` 下的 Go 源码。与 in-tree 实现对照的部分，以 Kubernetes master 提交 `aabdf5a2`（1.38 开发周期）为准。

## 一、out-of-tree 首先是另一个调度器二进制

### 1.1 `WithPlugin` 只是往 Registry 里多放几个工厂

`cmd/scheduler/main.go` 的全部逻辑是：

```go
command := app.NewSchedulerCommand(
    app.WithPlugin(capacityscheduling.Name, capacityscheduling.New),
    app.WithPlugin(coscheduling.Name, coscheduling.New),
    app.WithPlugin(loadvariationriskbalancing.Name, loadvariationriskbalancing.New),
    app.WithPlugin(networkoverhead.Name, networkoverhead.New),
    // ... 共 15 个
    // app.WithPlugin(crossnodepreemption.Name, crossnodepreemption.New),
)
```

`app.WithPlugin` 把构造函数合并进 out-of-tree registry，启动时再和 `NewInTreeRegistry()` 合并成一张表。之后的流程与原生 kube-scheduler 完全相同：读配置、按 Profile 实例化插件、进入调度循环。

这带来两个直接后果。

**第一，版本被 `go.mod` 锁死。** 这个 commit 依赖 `k8s.io/kubernetes v1.36.4`，编出来的二进制里，in-tree 插件、调度队列、抢占逻辑全部是 1.36.4 的实现。集群升级到 1.37，调度器不会跟着升级，必须换用对应版本的 scheduler-plugins 重新编译。

**第二，插件是否生效只取决于 Profile。** 注册了 15 个，不代表 15 个都在跑：

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: scheduler-plugins-scheduler
  plugins:
    multiPoint:
      enabled:
      - name: Coscheduling
      - name: CapacityScheduling
      disabled:
      - name: PrioritySort        # 整个进程只能有一个 QueueSort，换成 Coscheduling 的
      - name: DefaultPreemption   # 抢占交给 CapacityScheduling，原因见 1.2 节
  pluginConfig:
  - name: Coscheduling
    args:
      permitWaitingTimeSeconds: 30
```

只有 `spec.schedulerName` 等于这个 Profile 名字的 Pod 才会交给它。但 Profile 只能分开插件链，分不开调度队列：整个进程只有一个 `SchedulingQueue`，比较函数取自第一个 Profile，配置校验 `validateCommonQueueSort` 还要求所有 Profile 的 `queueSort` 及其参数完全相同。所以 QOSSort、TopologicalSort、Coscheduling 这三个 QueueSort 插件在一个调度器进程里只能启用一个，想同时用就得部署两个调度器（详见[第 25 篇](/posts/sched-plugin-25-qos-sort/)）。启用方式有两种：像上面这样在 `multiPoint.disabled` 里禁用 `PrioritySort`，或者在 `queueSort` 扩展点上写 `disabled: [{name: "*"}]` 再单独启用。

### 1.2 与 in-tree 插件共用扩展点时，顺序怎么定

out-of-tree 插件不会单独占一个扩展点，而是和 in-tree 默认插件挂在同一条链上。下图把每个扩展点上可能出现的插件都画了出来，灰色是 in-tree 默认插件，彩色是本仓库的插件：

{{< figure src="/images/posts/kubernetes-scheduler-out-of-tree-plugins/extension-chains.svg" link="/images/posts/kubernetes-scheduler-out-of-tree-plugins/extension-chains.svg" caption="out-of-tree 插件挂载的扩展点：灰色为 in-tree 默认插件，彩色插件默认追加在其后（点击查看大图）" >}}

链上的顺序不是插件自己决定的，而是由配置经过两步处理得到的。

**第一步：合并默认配置与用户配置。** `pkg/scheduler/apis/config/v1/default_plugins.go` 中的 `mergePluginSet` 对每个扩展点（包括 `multiPoint`）分别合并：

```go
// pkg/scheduler/apis/config/v1/default_plugins.go（k8s v1.36.4，mergePluginSet）
	var enabledPlugins []v1.Plugin
	if !disabledPlugins.Has("*") {
		for _, defaultEnabledPlugin := range defaultPluginSet.Enabled {
			if disabledPlugins.Has(defaultEnabledPlugin.Name) {
				continue
			}
			// The default plugin is explicitly re-configured, update the default plugin accordingly.
			if customPlugin, ok := enabledCustomPlugins[defaultEnabledPlugin.Name]; ok {
				// ...
				// Update the default plugin in place to preserve order.
				defaultEnabledPlugin = customPlugin.plugin
				replacedPluginIndex.Insert(customPlugin.index)
			}
			enabledPlugins = append(enabledPlugins, defaultEnabledPlugin)
		}
	}

	// Append all the custom plugins which haven't replaced any default plugins.
	// ...
	for index, plugin := range customPluginSet.Enabled {
		if !replacedPluginIndex.Has(index) {
			enabledPlugins = append(enabledPlugins, plugin)
		}
	}
```

得到三条规则：

- 默认插件保持原来的相对顺序，被 `disabled` 的直接去掉，`disabled: [{name: "*"}]` 去掉全部；
- 用户在 `enabled` 中重新写了某个默认插件（比如为了改权重），它**原地替换**，位置不变；
- 其余用户插件按书写顺序**追加在默认插件之后**。

**第二步：把 multiPoint 展开到各扩展点。** `framework/runtime/framework.go` 中的 `expandMultiPointPlugins` 对每个扩展点，把插件分成三段重新排列：

```go
// Reorder plugins. Here is the expected order:
// - part 1: overridePlugins. Their order stay intact as how they're specified in regular extension point.
// - part 2: multiPointEnabled - i.e., plugin defined in multipoint but not in regular extension point.
// - part 3: other plugins (excluded by part 1 & 2) in regular extension point.
```

| 段 | 包含哪些插件 | 段内顺序 |
| --- | --- | --- |
| 第一段 | 既写在 `multiPoint` 里、又写在这个扩展点（如 `filter`）里的插件 | 按扩展点里的书写顺序 |
| 第二段 | 只出现在 `multiPoint` 里的插件，**包括全部 in-tree 默认插件** | 按第一步合并后的 multiPoint 顺序 |
| 第三段 | 只写在这个扩展点里、没写进 `multiPoint` 的插件 | 按扩展点里的书写顺序 |

另外，如果某个扩展点上写了 `disabled: [{name: "*"}]`，这个扩展点不做 multiPoint 展开，只使用该扩展点自己的 `enabled` 列表。

#### 三种常见需求的写法

**1. 只要挂上去，不在乎位置**：写在 `multiPoint.enabled` 里。它会出现在它实现的每个扩展点上，排在所有默认插件之后。上图就是这种情况。

**2. 在某个扩展点上排到默认插件前面**：这一条最容易写错。直觉上会只写在 `filter.enabled` 里，但按上表它落在**第三段**，反而排在全部默认插件之后。正确的写法是两处都写：

```yaml
plugins:
  multiPoint:
    enabled:
    - name: NodeResourceTopologyMatch
  filter:
    enabled:
    - name: NodeResourceTopologyMatch   # 同时出现在 multiPoint 和 filter → 第一段，排在最前
```

这样它在 Filter 上排在最前面，在其他扩展点（Score、Reserve 等）上仍然追加在默认插件之后。

**3. 完全自己决定某个扩展点的顺序**：先禁用这个扩展点上的全部插件，再按需要的顺序把它们逐个写回来：

```yaml
plugins:
  multiPoint:
    enabled:
    - name: NodeResourceTopologyMatch
  filter:
    disabled:
    - name: "*"
    enabled:                            # Filter 上就是这个顺序，一个不多一个不少
    - name: NodeName
    - name: NodeResourceTopologyMatch
    - name: NodeResourcesFit
    # ... 其余需要保留的默认插件
```

这种写法的代价是升级 Kubernetes 时，新版本新增的默认 Filter 插件不会自动出现在这个扩展点上，需要手动补。

#### 顺序在哪些扩展点上真正影响结果

| 扩展点 | 链的遍历规则 | 顺序的影响 |
| --- | --- | --- |
| QueueSort | 整个进程只能有一个 | 不存在顺序，只有替换：启用 out-of-tree 的 QueueSort 必须禁用 `PrioritySort`，且所有 Profile 的 `queueSort` 必须相同（`validateCommonQueueSort`） |
| PostFilter | 遇到第一个 `Success` 即停止 | **决定结果**：`DefaultPreemption` 在前且抢占成功，后面的 `CapacityScheduling` 就不会执行，所以要禁用它 |
| Bind | 第一个不返回 `Skip` 的插件认领 | **决定结果**：谁先认领谁绑定 |
| PreFilter | 遇到 `UnschedulableAndUnresolvable` 立即停止；`Unschedulable` 只记下来，其余插件照常执行（抢占模拟需要它们写的状态） | 不影响结果，只影响记录下来的失败插件 |
| Filter | 每个节点上遇到第一个失败即停止 | 不影响节点是否可行（所有插件都要通过），只影响性能和记录下来的失败原因 |
| Score | 全部执行后加权求和 | 不影响结果，分数是可交换的求和 |
| Reserve / Permit / PreBind | 依次执行，失败则回滚 | 影响副作用的先后，例如先占配额还是先占 NUMA 资源 |

这也解释了 1.1 节示例里为什么要禁用 `DefaultPreemption`：PostFilter 是「第一个成功的说了算」，两个抢占插件同时存在，默认的那个会先执行，按优先级驱逐，完全绕过配额规则。

### 1.3 插件状态的三种归属

in-tree 插件的状态基本只活在一个调度周期的 `CycleState` 里。out-of-tree 插件则普遍在进程内维护长期状态，有的还要靠 controller 写回 CRD：

| 插件 | CycleState | 进程内长期状态 | 依赖的外部数据 |
| --- | --- | --- | --- |
| `CapacityScheduling` | 请求量、配额快照 | 每个 Namespace 的 `Used` | `ElasticQuota` 的 `min`/`max` |
| `Coscheduling` | Permit 是否已激活兄弟 Pod | 已分配 Pod 集合、退避表、失败时间 | `PodGroup` 的 spec |
| Trimaran 四个 | 仅 `LowRiskOverCommitment` 用 | 指标快照、最近调度 Pod 缓存 | load-watcher / metrics 源 |
| `NodeResourceTopologyMatch` | 抢占模拟用的受害者栈 | NRT 缓存与预留量 | `NodeResourceTopology` |
| `NetworkOverhead` | 依赖列表、代价矩阵 | 无 | `AppGroup`、`NetworkTopology` |
| `SySched` | 无 | 节点上 Pod 的系统调用并集 | SPO 的 `SeccompProfile` |

进程内状态意味着两件事：调度器重启会丢失，需要靠 informer 重建；多副本部署时，只有 leader 的状态是有效的。后面读每个插件时，都可以拿这张表对照。

---

## 二、`CapacityScheduling`：调度器自己记账的弹性配额

完整拆解见[第 28 篇](/posts/sched-plugin-28-capacity-scheduling/)。

### 2.1 `ElasticQuota` 只被读了两个字段

```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: ElasticQuota
metadata:
  name: quota-a
  namespace: team-a
spec:
  min: { cpu: "10", memory: 20Gi }
  max: { cpu: "20", memory: 40Gi }
```

语义是「保底 `min`，封顶 `max`，中间的部分向别人借」。CRD 还有一个 `status.used`，由 `cmd/controller` 维护。但读插件代码会发现，**调度器从不读 `status.used`**。它在内存里自己记账：

```go
// pkg/capacityscheduling/elasticquota.go
type ElasticQuotaInfo struct {
    Namespace string
    pods      sets.Set[string]
    Min       *framework.Resource
    Max       *framework.Resource
    Used      *framework.Resource
}
```

`Used` 的更新来源有三处：

- Pod informer 看到已绑定节点的 Pod，加进去；Pod 删除或进入终态时，减掉；
- `Reserve` 时加上当前 Pod 的请求量；
- `Unreserve` 时减回来。

controller 写的 `status.used` 只统计 `Running` 状态的 Pod，而调度器的账本还包括刚 Reserve、尚未运行的 Pod。所以 `kubectl get eq` 看到的用量，和调度器判定时用的用量，在 Pod 启动的这段时间里是不一致的。排查「配额明明没满为什么调度失败」时，要知道这一点。两本账还有两处稳定的差异：controller 计算 Pod 请求量时调用 `quota.Add(result, pod.Spec.Overhead)` 却丢掉了返回值，带 RuntimeClass overhead 的 Pod，overhead 不进 `status.used`，而插件的账本会计入；反过来，先有 Pod、后建 `ElasticQuota` 时，插件新建的条目 `Used` 从 0 开始，Namespace 里已经在跑的 Pod 不会被补记（Pod informer 不会为它们再发 Add 事件），直到这些 Pod 被重建或调度器重启，插件都会少算。

`min`、`max` 的默认值只在**整个字段**缺失时才生效：`min` 为 nil 时各维度视为 0，`max` 为 nil 时视为 `math.MaxInt64`，即不设上限。只要写了其中一项，其余维度就是 0。例如 `max` 只写了 `cpu: 20`，`Max.Memory` 就是 0，任何请求内存的 Pod 都会被判定为「超过 Max」；`min` 只写 CPU 也有同样的问题。所以 `min`、`max` 应当把 Pod 会请求的资源维度都写全。

### 2.2 PreFilter：两道检查，第二道是全局的

```go
// pkg/capacityscheduling/capacity_scheduling.go
	if eq.usedOverMaxWith(nominatedPodsReqInEQWithPodReq) {
		return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("Pod %v/%v is rejected in PreFilter because ElasticQuota %v is more than Max", pod.Namespace, pod.Name, eq.Namespace))
	}

	if elasticQuotaInfos.aggregatedUsedOverMinWith(*nominatedPodsReqWithPodReq) {
		return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("Pod %v/%v is rejected in PreFilter because total ElasticQuota used is more than min", pod.Namespace, pod.Name))
	}
```

写成不等式，`req` 表示本 Pod 的请求量加上相关的 nominated Pod：

```text
检查一：Used(ns) + req > Max(ns)
检查二：ΣUsed + req > ΣMin
```

第一道里的 nominated Pod 只算同 Namespace、优先级不低于本 Pod 的；第二道的求和范围是所有 `ElasticQuota`，nominated Pod 除了第一道那些，还加上其他 Namespace 中用量未超过 `min` 的配额下的 Pod。任一资源维度超出即失败。第一道是常规的单租户上限。第二道更有意思：**全集群已用总量永远不能超过所有 `min` 之和**。所谓「借用」，借的只是别的租户暂时没用掉的 `min`，并不能超出 `min` 的总池子。这意味着如果所有租户的 `min` 加起来小于集群容量，多出来的那部分容量对配额内的 Pod 是不可见的。

把 nominated Pod 计入，是为了避免「抢占已经腾出位置、但被提名的 Pod 还没绑定」时，其他 Pod 把同一份配额再占一次。

两道检查都返回 `Unschedulable` 而不是 `UnschedulableAndUnresolvable`，所以失败后会进入 PostFilter，也就是抢占。

没有 `ElasticQuota` 的 Namespace，PreFilter 直接返回 `Success`，完全不受配额约束。

### 2.3 PostFilter：受害者由「是否超过 min」决定

抢占复用了上游的 `preemption.Evaluator`，只替换了 `SelectVictimsOnNode`。选谁当受害者取决于抢占者自己的状态：

| 抢占者状态 | 可被驱逐的 Pod |
| --- | --- |
| 有 EQ，且 `Used + 请求 > Min`（已经在借） | **同 Namespace** 内优先级更低的 Pod |
| 有 EQ，且仍在 `Min` 以内 | **其他 Namespace** 中 `Used > Min` 的 Pod，即正在借用的租户，**不比较优先级** |
| 没有 EQ | 同样没有 EQ 的 Namespace 中优先级更低的 Pod |

第二行是弹性配额的核心：租户在自己的 `min` 以内申请资源时，有权把借出去的资源收回来，而且不看优先级。一个低优先级的租户，可以驱逐另一个租户的高优先级 Pod，只要后者处于借用状态。

之后的流程与 `DefaultPreemption` 一致：先移除全部候选受害者，运行 Filter 确认 Pod 能放下，再按 PDB 分两轮尝试「赦免」：把受害者逐个加回去，加回后仍然能放下的就不驱逐。区别在于赦免条件多了一项：加回后配额检查也必须仍然满足。

与 in-tree 抢占相比还有两处实现差异。一是候选数量：`GetOffsetAndNumCandidates` 返回 `(0, n)`，从第一个节点开始、要求的候选数等于全部潜在节点数，而 `DefaultPreemption` 默认只要 `max(10% 节点数, 100)` 个候选、找够即停，所以 `CapacityScheduling` 每次抢占都会把所有节点模拟一遍，耗时随集群规模线性增长。二是驱逐方式：插件构造 `preemption.Executor` 时传入的是空的 `feature.Features{}`，异步抢占关闭，驱逐受害者的 API 调用在 PostFilter 里同步完成。

### 2.4 使用时的边界

- 一个 Namespace 只对应一个 `ElasticQuota`，controller 取的是列表里的第一个；
- 它管的是**调度准入**，不是运行时限制。Pod 跑起来后用超了 requests，配额不会感知；
- 没有 EQ 的 Pod 既不受约束，也不会被有 EQ 的 Pod 抢占。混部时，「没配额」反而是最安全的身份。
- `EventsToRegister` 只注册了 Pod 删除和 `ElasticQuota` 的任意变化，都没有 QueueingHint，框架一律按 `Queue` 处理：集群里任何一个 Pod 被删除，所有被它拒绝的 Pod 都会被唤醒重试。

---

## 三、`Coscheduling`：用 Permit 的等待实现 all-or-nothing

完整拆解见[第 27 篇](/posts/sched-plugin-27-coscheduling/)。

### 3.1 PodGroup 与 Pod 的关联

```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: PodGroup
metadata:
  name: training
  namespace: ml
spec:
  minMember: 8
  minResources: { cpu: "32", nvidia.com/gpu: "8" }
  scheduleTimeoutSeconds: 120
```

Pod 通过 label `scheduling.x-k8s.io/pod-group: training` 关联。插件只读 `spec` 和 `creationTimestamp`，**不读 `status`**。`status.phase` 由 controller 维护，供人和上层系统查看。

gang 调度在框架里没有原生支持，调度器一次只处理一个 Pod。`Coscheduling` 的思路是把「一组 Pod 同时成功」拆进五个扩展点：

```text
QueueSort    让同组 Pod 在队列里挨在一起
PreFilter    组的条件不满足，单个 Pod 不必尝试
Permit       Pod 选好节点后先停住，等同组凑齐
PostFilter   有成员失败时，尽快放弃整组
Unreserve    任何一个成员回滚，整组一起回滚
```

它没有实现 PreEnqueue：人数不够的组，Pod 照样出队、进入调度周期，到 PreFilter 才被拒，每次都要消耗一轮调度周期。

### 3.2 QueueSort：用组的时间戳代替 Pod 的时间戳

```go
// pkg/coscheduling/coscheduling.go
func (cs *Coscheduling) Less(podInfo1, podInfo2 fwk.QueuedPodInfo) bool {
	prio1 := corev1helpers.PodPriority(podInfo1.GetPodInfo().GetPod())
	prio2 := corev1helpers.PodPriority(podInfo2.GetPodInfo().GetPod())
	if prio1 != prio2 {
		return prio1 > prio2
	}
	creationTime1 := cs.pgMgr.GetCreationTimestamp(context.TODO(), podInfo1.GetPodInfo().GetPod(), *podInfo1.GetInitialAttemptTimestamp())
	creationTime2 := cs.pgMgr.GetCreationTimestamp(context.TODO(), podInfo2.GetPodInfo().GetPod(), *podInfo2.GetInitialAttemptTimestamp())
	if creationTime1.Equal(creationTime2) {
		return core.GetNamespacedName(podInfo1.GetPodInfo().GetPod()) < core.GetNamespacedName(podInfo2.GetPodInfo().GetPod())
	}
	return creationTime1.Before(creationTime2)
}
```

优先级相同时，比较的不是 Pod 自己的入队时间，而是 `GetCreationTimestamp` 返回的「组时间」：

- 不属于任何组：Pod 的首次尝试时间；
- 组最近失败过：**最近一次失败的时间**；
- 否则：PodGroup 的创建时间。

同组 Pod 共享同一个时间戳，再按名字排，就会在队列里连续出队。第二条规则是为了防止队头阻塞：一个反复失败的组，时间戳会不断后移，把位置让给后面的组。

### 3.3 PreFilter：四道前置条件

| 顺序 | 检查 | 失败原因 |
| --- | --- | --- |
| 1 | 组是否在退避期 | `podGroup %v failed recently` |
| 2 | 带该 label 的 Pod 总数 ≥ `minMember` | `cannot find enough sibling pods, current pods number: %v, minMember of group: %v` |
| 3 | 带 `schedulingGates` 的 Pod 不能多到让剩余人数低于 `minMember`（Pod 数超过 `minMember` 的部分可以抵扣） | `cannot proceed due to gated pods in the same PodGroup` |
| 4 | 集群剩余资源 ≥ `minResources` | `resource gap: %v` |

所有失败都转成 `UnschedulableAndUnresolvable`：组的条件不满足时，抢占也救不了。

第四条要注意它的粒度：`CheckClusterResource` 把所有节点的空闲资源**加总**后与 `minResources` 比较，不是逐节点装箱。8 张 GPU 分散在 8 个节点上，每个只剩 1 张，也能通过这道检查。真正的装箱还是由 Filter 阶段的 `NodeResourcesFit` 决定。检查通过后，组会被记进 `permittedPG` 缓存，一段时间内不再重复计算。

### 3.4 Permit：凑齐之前一直 Wait

```go
// pkg/coscheduling/core/core.go（PodGroupManager.Permit）
	assigned, exist := pgMgr.assignedPodsByPG[pgFullName]
	if !exist {
		assigned = sets.Set[string]{}
		pgMgr.assignedPodsByPG[pgFullName] = assigned
	}
	assigned.Insert(pod.Name)
	// ...
	if len(assigned) >= int(pg.Spec.MinMember) {
		return Success
	}

	if len(assigned) == 1 {
		// ...
		state.Write(permitStateKey, &PermitState{Activate: true})
	}

	return Wait
```

返回 `Success` 时，插件的 `Permit` 会对同组所有等待中的 Pod 调用 `Allow`；返回 `Wait` 时，插件返回 `fwk.Wait` 和超时时间。

`Wait` 是调度框架里很少被用到的状态：Pod 已经选定节点并完成 Reserve，资源已在调度器缓存中占住，但不进入 Bind，挂在 waiting pods 表里等待 `Allow` 或 `Reject`。

超时时间按以下优先级取值：

1. PodGroup 的 `spec.scheduleTimeoutSeconds`；
2. 插件参数 `permitWaitingTimeSeconds`（默认 60）；
3. 兜底常量 60 秒。

第一个进入 Permit 的成员（`len(assigned) == 1`）还会在 CycleState 里打上标记，插件随后把同组其他 Pod 写进 `PodsToActivateKey`，让它们从不可调度队列里提前回到 activeQ，缩短凑齐所需的时间。

### 3.5 失败时尽快放弃整组

只靠 Permit 超时有一个问题：一个注定凑不齐的组，会让已经 Reserve 的成员白白占着资源等 60 秒。`PostFilter` 负责提前止损：

```go
// pkg/coscheduling/coscheduling.go（PostFilter）
	assigned := cs.pgMgr.GetAssignedPodCount(pgName)
	if assigned >= int(pg.Spec.MinMember) {
		// ...
		return &fwk.PostFilterResult{}, fwk.NewStatus(fwk.Unschedulable)
	}

	// If the gap is less than/equal the reject threshold, we may want to try subsequent Pods
	// to see they can satisfy the PodGroup
	notAssignedPercentage := float64(int(pg.Spec.MinMember)-assigned) / float64(pg.Spec.MinMember)
	if notAssignedPercentage <= cs.pgRejectThreshold {
		// ...
		return &fwk.PostFilterResult{}, fwk.NewStatus(fwk.Unschedulable)
	}
	// ...（Reject 同组所有等待中的 Pod，按需退避，标记失败）
```

某个成员 Filter 失败时，先看组是否已经够数：已分配的数量达到 `minMember`，失败的这个是「多出来」的成员，不连累整组。否则计算组内还有多少比例没有分到节点：

- 比例 ≤ `podGroupRejectPercentage`（默认 10%）：只差几个了，不放弃，等它下一轮重试；
- 比例更高：判定这一轮凑不齐，`Reject` 所有等待中的同组 Pod，释放它们 Reserve 的资源。

`Unreserve` 的语义更直接：任何一个成员回滚，整组都回滚。

退避参数 `podGroupBackoffSeconds` 默认是 **0，即关闭**。不开退避时，被整组拒绝的 Pod 会很快重新入队，大组在资源紧张时可能反复空转。生产上建议设一个值。

重新入队靠 `EventsToRegister`：它只注册了 Pod Add 和 PodGroup Add/Update 两类事件，都没有 QueueingHint，框架一律按 `Queue` 处理。集群里任何一个 Pod 被创建，所有因 Coscheduling 被拒的 Pod 都会被唤醒重试，不管新 Pod 是不是同一个组的。

### 3.6 PodGroup 的状态机

controller 根据组内 Pod 的实际状态推进 `status.phase`。每次协调时重新统计带该 label 的 Pod（`r.List` 只带了 label selector，没有限定 namespace，不同 namespace 里同名 PodGroup 的 Pod 会被互相计入），按下表依次判断，后面的条件成立会覆盖前面的结果：

| 条件 | phase |
| --- | --- |
| 刚创建，phase 为空 | `Pending` |
| `Pending` 且组内 Pod 数 ≥ `minMember` | `Scheduling` |
| 组内 Pod 数 < `minMember` | `Pending` |
| `running + succeeded` < `minMember` | `Scheduling` |
| `running + succeeded` ≥ `minMember` | `Running` |
| `failed` > 0 且 `failed + running + succeeded` ≥ `minMember` | `Failed` |
| `succeeded` ≥ `minMember` | `Finished` |

`Failed` 和 `Finished` 是终态：Reconcile 开头遇到这两个 phase 直接返回，即使失败的 Pod 后来被重建、组恢复运行，phase 也会一直停在 `Failed`。类型定义里还有一个 `Unknown`，但没有任何分支会设置它。Reconcile 里另有一段「调度开始时间减创建时间超过 48 小时就不再协调」的超时判断，而整个仓库没有代码给 `ScheduleStartTime` 赋值，零值减出来是负数，这个超时永远不会触发。调度器不读这个状态，所以 phase 停在 `Scheduling` 并不会阻止调度；反过来，phase 显示 `Running` 也不代表调度器的内存状态已经同步。

### 3.7 与 in-tree `GangScheduling` 的关系

Kubernetes 主仓库也在推进原生的 gang 调度。scheduler-plugins 依赖的 v1.36.4 里，in-tree `GangScheduling` 还是 PreEnqueue + Permit 的实现（固定 5 分钟超时），思路和 `Coscheduling` 相近；到了 master，它已经不再实现 Permit，而是实现 PreEnqueue 和 `PlacementFeasible`，独立的 `GangScheduling` feature gate 已删除，插件随 `GenericWorkload`（1.35 Alpha，1.37 Beta，默认关闭）一起启用。调度器为 PodGroup 增加了专门的调度周期（`schedule_one_podgroup.go`）：整组作为一个队列实体出队，在 snapshot 里逐个试放、逐个 assume，每放一个就问一次 `PlacementFeasible`，失败则整体撤销；周期内单个 Pod 不跑 PostFilter，改由 `PodGroupPostFilter` 为整组计算抢占。两种实现的根本差异在于等待发生在哪里：

- `Coscheduling` 逐个调度、在 Permit 处汇合，判定发生在**每个 Pod 已经在调度器缓存里占住资源之后**，靠超时兜底；
- master 上的 in-tree 方案在**一个调度周期内部**完成整组判定，snapshot 里的 assume 只对这一轮可见，要么整组提交、要么全部撤销，没有超时，人数不够时 PreEnqueue 直接让整组不出队。

前者实现简单、兼容所有现有扩展点，代价是可能占着资源等待，两个组各占一半资源时会互相等到超时；后者不会出现部分成员先占资源的中间状态，代价是一个大组会独占调度主循环较长时间，且依赖还在演进中的 API。详细对照见[第 27 篇](/posts/sched-plugin-27-coscheduling/)第十节和 [GangScheduling](/posts/sched-plugin-21-gang-scheduling/) 一篇。

---

## 四、Trimaran：基于真实负载打分

Trimaran 是四个插件的统称：`TargetLoadPacking`、`LoadVariationRiskBalancing`、`LowRiskOverCommitment`、`Peaks`。它们只实现 Score（`LowRiskOverCommitment` 另有一个 PreScore），不做过滤。四个插件的单篇分别是第 [37](/posts/sched-plugin-37-target-load-packing/)、[38](/posts/sched-plugin-38-load-variation-risk-balancing/)、[39](/posts/sched-plugin-39-low-risk-overcommitment/)、[40](/posts/sched-plugin-40-peaks/) 篇。

### 4.1 指标从哪来

四个插件共用 `pkg/trimaran/collector.go` 里的 `Collector`：

- 配置了 `watcherAddress`：通过 HTTP 访问独立部署的 load-watcher 服务；
- 没配置：把 load-watcher 作为库嵌入，直接对接指标源。支持 `KubernetesMetricsServer`、`Prometheus`、`SignalFx`，默认是 `KubernetesMetricsServer`。`Peaks` 例外，它只把 `WatcherAddress` 传给 `Collector`，不写时 `MetricProvider.Type` 为空，`New` 直接报错，所以只能用服务模式。

启动时拉一次，之后每 **30 秒**刷新一次，结果是一份整体快照，打分时读快照，不会实时请求。

**指标缺失时，四个插件的原始分都是 0。** 节点新加入、metrics-server 漏采、Prometheus 查询超时，都会让这个节点在前三个插件的维度上垫底。如果它们是 Profile 里权重最高的打分插件，结果就是这些节点几乎不会被选中。`Peaks` 的原始分是代价，经过反向归一化后方向正好相反，见 4.6。

另外，四个插件都没有实现 `SignPlugin`。Opportunistic Batching 要求 Profile 里所有 PreFilter/Filter/PreScore/Score 插件都能给 Pod 签名，只要启用其中任何一个，整个 Profile 的签名复用就会被关掉。这对 Trimaran 是合理的：分数依赖外部快照和随时间变化的补偿缓存，不适合复用。

### 4.2 指标滞后的补偿

指标有延迟：一个 Pod 刚绑定到节点，它的 CPU 使用量要在下一个采集周期后才会出现在指标里。在此期间，调度器会以为节点仍然空闲，把一批 Pod 都调度过去。

`PodAssignEventHandler` 用来弥补这个时间窗：它监听 Pod 的绑定事件，按节点记录最近调度的 Pod 和时间戳。打分时，时间戳晚于指标窗口结束时间、或在其之前 60 秒内的 Pod，会被视为「指标里还没有它」，把它们的预测用量补加上去。缓存每 5 分钟清理一次。

四个插件里，**只有 `TargetLoadPacking` 实际读取了这份缓存**。另外三个没有这层补偿，在批量调度时更容易扎堆。

### 4.3 `TargetLoadPacking`：在目标利用率附近装箱

目标是把节点 CPU 利用率推到目标值 T（默认 40%）附近，而不是越低越好。

预测利用率的计算分两步。先估计 Pod 的 CPU 用量，逐个容器取值后求和，再加上 overhead：

- 有 limit，取 limit；
- 没有 limit 但有 request，取 request × 1.5（`defaultRequestsMultiplier`）；
- 都没有，取 1000m（`defaultRequests`）。

再估计放上去之后的节点利用率：

```text
predicted = (当前用量 + 滞后补偿 + Pod 用量) / Capacity × 100
```

打分公式：

```go
// pkg/trimaran/targetloadpacking/targetloadpacking.go
	if predictedCPUUsage > float64(hostTargetUtilizationPercent) {
		if predictedCPUUsage > 100 {
			return score, fwk.NewStatus(fwk.Success, "")
		}
		penalisedScore := int64(math.Round(float64(hostTargetUtilizationPercent) * (100 - predictedCPUUsage) / (100 - float64(hostTargetUtilizationPercent))))
		// ...
		return penalisedScore, fwk.NewStatus(fwk.Success, "")
	}

	score = int64(math.Round((100-float64(hostTargetUtilizationPercent))*
		predictedCPUUsage/float64(hostTargetUtilizationPercent) + float64(hostTargetUtilizationPercent)))
```

`score` 的初值是 `fwk.MinNodeScore`，即 0。

以 T = 40 为例，分数曲线是一个尖顶：

```text
score
 100 |              *
     |            *   *
     |          *       *
  40 |*  *  *              *
     |                        *
   0 +----------+-------------*------> 预测利用率 %
     0         40            100
```

- 0% 时得 40 分，线性上升，40% 时满分；
- 超过 40% 后线性下降，100% 时归零；
- 超过 100% 直接 0 分。

这个形状使空节点不如「已经有一些负载、加上这个 Pod 刚好到 40%」的节点受欢迎，从而把负载集中到较少的节点上，空出来的节点可以缩容。

注意它用的分母是 `Capacity` 而不是 `Allocatable`，并且 Pod 的预测量优先取 **limit**。limit 远大于 request 的 Pod，会被认为很「重」。

### 4.4 `LoadVariationRiskBalancing`：均值和波动一起算

只看平均利用率会忽略波动：两个节点平均都是 50%，一个稳定，一个在 20% 和 80% 之间来回跳，后者更危险。这个插件把标准差纳入风险：

```go
// pkg/trimaran/loadvariationriskbalancing/analysis.go
	// calculate average and deviation factors
	mu, sigma := trimaran.GetMuSigma(rs)

	// apply root power
	if sensitivity >= 0 {
		sigma = math.Pow(sigma, 1/sensitivity)
	}
	// apply multiplier
	sigma *= margin
	sigma = max(min(sigma, 1), 0)

	// evaluate overall risk factor
	risk := (mu + sigma) / 2
	// ...
	return (1. - risk) * float64(fwk.MaxNodeScore)
```

`GetMuSigma` 给出 `mu = (节点平均用量 + Pod request) / Allocatable` 和 `sigma = 节点用量标准差 / Allocatable`，都截断到 `[0, 1]`。`margin`（`safeVarianceMargin`，默认 1）放大标准差的权重；`sensitivity`（`safeVarianceSensitivity`，默认 1）大于 1 时，开方会抬高小的标准差，让插件对轻微波动也敏感。

CPU 和内存各算一个分，两者都有效时取**较小值**：任何一个维度有风险，节点就不受欢迎。

代码与 KEP 有两处出入。KEP 的公式是 `S = min(M + r + V, 1)`、得分 `(1 − S) × 100`，满载节点得 0 分；代码把两项各自截断后**除以 2**，μ = 1、σ = 0 的满载但稳定的节点仍有 50 分，实际分值区间大约只有 `[50, 100]`，和满幅打分的插件放在一起时，等效权重约为配置值的一半。另外，metrics-server 只提供瞬时值，没有标准差，σ 恒为 0，`margin` 和 `sensitivity` 都不起作用，插件退化成「按当前实际用量最少优先」；要让它按设计工作，需要接 Prometheus 或 SignalFx。

### 4.5 `LowRiskOverCommitment`：估计「超出 limit」的概率

前两个插件只看 request 和使用量。在超卖的集群里，limit 之和远大于节点容量，真正的风险是**很多 Pod 同时用到接近 limit**。这个插件把风险拆成两部分：

**风险一，limit 超卖的程度：**

```text
riskLimit = (Σlimit - capacity) / (Σlimit - Σrequest)
```

只在 `Σlimit > capacity` 时计算，否则为 0。

limit 总和刚好等于容量时为 0；超出部分占 limit-request 差值的比例越大，风险越高。

**风险二，实际负载超出 request 的概率：**

把节点利用率看成一个 Beta 分布，用测得的均值 μ 和标准差 σ 做矩匹配：

```go
// pkg/trimaran/lowriskovercommitment/beta.go
	temp := (m1 * (1 - m1) / variance) - 1
	temp = max(temp, math.SmallestNonzeroFloat64)
	b.alpha = m1 * temp
	b.beta = (1 - m1) * temp
```

再用这个分布计算「利用率 ≤ request 占容量比例」的概率 P，`riskLoad = 1 - P`。σ 会先乘以 `sqrt(smoothingWindowSize)`（默认窗口 5），把短期波动外推到更长的时间尺度。

两者加权合并：

```go
// pkg/trimaran/lowriskovercommitment/lowriskovercommitment.go
	// combine two components of risk into a total risk as a weighted sum
	w := pl.riskLimitWeightsMap[resourceName]
	totalRisk = w*riskLimit + (1-w)*riskLoad
	// ...
	rank := 1 - max(riskCPU, riskMemory)
```

`riskLimitWeights` 默认 cpu、memory 各 0.5，最终得分是 `rank × 100` 取整。

和上一个插件一样，取 CPU、内存中风险更高的那个。

一个特殊情况：BestEffort Pod 的 request 和 limit 都是 0，直接得 0 分。它的设计目标是保护「有 request、有 limit、但 limit 被超卖」的 Pod，BestEffort 不在讨论范围内。

### 4.6 `Peaks`：最小化功耗跳变

`Peaks` 优化的不是资源，而是功耗。它假设节点功耗与 CPU 利用率呈指数关系：

```go
// pkg/trimaran/peaks/peaks.go
func getPowerJumpForUtilisation(x, p float64, m config.PowerModel) float64 {
    return m.K1 * (math.Exp(m.K2*p) - math.Exp(m.K2*x))
}
```

`x` 是当前利用率，`p` 是加上 Pod request 后的预测利用率，结果是功耗增量。原始分数是**代价**，越小越好，所以 `NormalizeScore` 做了反向的 min-max 归一化：

```go
normCost = float64(fwk.MaxNodeScore) * float64(scores[i].Score-minCost) / float64(maxCost-minCost)
scores[i].Score = fwk.MaxNodeScore - int64(normCost)
```

功耗模型 `{K0, K1, K2}` 按节点名配置在参数 `NodePowerModel` 中，或放在环境变量 `NODE_POWER_MODEL` 指向的 JSON 文件里，需要事先按机型测量。`K0` 在公式里没有用到，是常数项，对增量没有影响。读取文件的代码把结果解码进参数传进来的 map，只有配置里显式写了一个空的 `NodePowerModel: {}` 时，文件内容才真正生效。

配置上还有两处要注意：如 4.1 所述，`Peaks` 只能用服务模式，必须配置 load-watcher 地址；而 v1 参数类型里两个字段的 json tag 写坏了（`json:watcherAddress",inline"`，缺了左引号），解析不出 json 键，只能按字段名匹配，apimachinery 的反序列化又区分大小写，所以配置里必须写首字母大写的 `WatcherAddress`、`NodePowerModel`，和其他插件的驼峰写法不同。

**这里有一个与其他三个插件相反的行为。** 指标缺失、预测超过 100%、节点没有功耗模型时，`Score` 都返回原始值 0。而 0 是最小代价，反向归一化之后会变成 **100 分**。也就是说，在 `Peaks` 下，没有数据的节点反而最受欢迎。功耗模型覆盖不全时，这个行为会让调度结果偏向未建模的节点。

### 4.7 四个插件放在一起看

| 插件 | 用到的数据 | 分母 | 无指标时 | 关键参数与默认值 |
| --- | --- | --- | --- | --- |
| `TargetLoadPacking` | CPU 使用量 + 滞后补偿 | Capacity | 0 分 | `targetUtilization: 40`，`defaultRequestsMultiplier: 1.5` |
| `LoadVariationRiskBalancing` | CPU/内存均值与标准差 | Allocatable | 0 分 | `safeVarianceMargin: 1`，`safeVarianceSensitivity: 1` |
| `LowRiskOverCommitment` | request、limit、均值、标准差 | Allocatable | 0 分 | `smoothingWindowSize: 5`，`riskLimitWeights: 0.5` |
| `Peaks` | CPU 使用量 + 功耗模型 | Capacity | **100 分** | `NodePowerModel` 无默认，只支持服务模式 |

它们的目标互相冲突：`TargetLoadPacking` 想把负载集中，`LoadVariationRiskBalancing` 想把风险分散。同一个 Profile 里通常只启用其中一个。

---

## 五、`NodeResourceTopologyMatch`：调度器看见 NUMA

完整拆解见[第 30 篇](/posts/sched-plugin-30-node-resource-topology/)。

### 5.1 为什么需要它

kubelet 的 Topology Manager 在节点上做 NUMA 对齐：一个 Guaranteed Pod 申请了 4 个 CPU 和 1 张 GPU，`single-numa-node` 策略要求它们来自同一个 NUMA 节点，否则 kubelet 拒绝准入，Pod 进入 `TopologyAffinityError`。

问题是调度器只看节点的总量。节点总共剩 8 个 CPU，但分在两个 NUMA 节点上各 4 个，GPU 只在 NUMA 1 上，调度器会认为能放，kubelet 却会拒绝。Pod 被反复调度到同一个节点，又反复失败。

这个插件读取 `NodeResourceTopology` CRD（由节点上的 exporter 上报每个 NUMA zone 的可用资源），在调度阶段模拟 kubelet 的判定。

### 5.2 Filter 只模拟 `single-numa-node`

```go
// pkg/noderesourcetopology/filter.go
func filterHandlerFromTopologyManager(conf nodeconfig.TopologyManager) (filterFn, string) {
    if conf.Policy != kubeletconfig.SingleNumaNodeTopologyManagerPolicy {
        return nil, ""
    }
    if conf.Scope == kubeletconfig.PodTopologyManagerScope {
        return singleNUMAPodLevelHandler, "pod"
    }
    if conf.Scope == kubeletconfig.ContainerTopologyManagerScope {
        return singleNUMAContainerLevelHandler, "container"
    }
    return nil, ""
}
```

只有节点的 Topology Manager 策略是 `single-numa-node` 时才会过滤。`restricted`、`best-effort`、`none` 策略的节点一律通过。节点的策略和作用域从 NRT 对象的 `attributes` 读取，读不到时默认 `none` + `container`。

两种作用域的判定方式不同：

| 作用域 | 判定方式 | 失败原因 |
| --- | --- | --- |
| `pod` | Pod 的有效请求（init 容器取最大，应用容器求和）必须放进某一个 NUMA zone | `cannot align pod` |
| `container` | 每个容器分别找一个能放下的 zone，找到后**从该 zone 扣除**，再处理下一个容器 | `cannot align container` |

container 作用域下，扣除这一步很关键：两个容器各要 3 个 CPU，节点两个 zone 各剩 4 个，它们会被分到两个 zone，都能通过；如果不扣除，判定会错误地认为两个都能放进同一个 zone。

判定细节里还有一条容易忽略的规则：**非 Guaranteed Pod 的 CPU、内存、hugepages 可以放在任意 zone**，因为 kubelet 只对 Guaranteed Pod 做独占 CPU 和内存绑定。所以对 Burstable Pod，这个插件实际只检查设备类资源。

NRT 数据被判定为不新鲜（见 5.4）时，返回 `Unschedulable`，原因是 `invalid node topology data`。

### 5.3 Score：取最差 zone 的分

非 Guaranteed Pod 一律满分，不参与比较。对 Guaranteed Pod，先对每个 zone 用评分策略算分，**节点得分取所有非零 zone 分数的最小值**。四种策略：

| 策略 | zone 分数 |
| --- | --- |
| `LeastAllocated`（默认） | Σ 权重 × 剩余比例 × 100 / Σ 权重 |
| `MostAllocated` | Σ 权重 × 请求比例 × 100 / Σ 权重 |
| `BalancedAllocation` | (1 − 各资源请求比例的方差) × 100 |
| `LeastNUMANodes` | 100 − 需要的 zone 数 × (100 / MaxNUMANodes)，距离最近的组合再加分 |

前三种只对 `single-numa-node` 策略的节点打分；`LeastNUMANodes` 适用于所有策略，偏好用最少 NUMA 节点就能满足的方案，配合 `restricted` 策略使用。

表里的「比例」与 in-tree `NodeResourcesFit` 的口径不同：分母是 zone 上报的 `Available`，不是容量。`LeastAllocated` 算的是 `(available − 本 Pod 请求) / available`，与 zone 已经用了多少无关。另一个后果来自「取非零最小值」：放不下的 zone 得 0 分被跳过，而恰好装满的 zone（剩余为 0）也得 0 分，同样被跳过，完美贴合的 zone 在 `LeastAllocated` 下反而不计入节点分。

### 5.4 缓存：数据总是落后于真实状态

NRT 由节点上的 exporter 定期上报，天然有延迟。调度器刚把一个 Pod 放到 NUMA 0，下一份 NRT 报告到来之前，调度器看到的 NUMA 0 仍然是空的。插件提供了三种缓存模式应对：

| 模式 | 触发条件 | 行为 |
| --- | --- | --- |
| Passthrough | `cacheResyncPeriodSeconds` 未设置或 ≤ 0（**默认**） | 每次直接向 API Server GET 节点的 NRT，不做任何修正 |
| OverReserve | 设置了 resync 周期 | Reserve 时记下 Pod 的请求，读取时从**每个** zone 都扣掉；每个周期只处理「脏」节点（被 Filter 拒过的、有外来 Pod 的、属性变化的），重新 GET NRT，前两类还要比对 Pod 指纹，通过后才 Flush，用新 NRT 替换快照并清掉该节点的预留 |
| DiscardReserved | `discardReservedNodes: true` | 节点上有任何未完成的预留，就把这个节点的 NRT 视为不新鲜，直接拒绝 |

OverReserve 之所以从每个 zone 都扣，是因为调度器并不知道 kubelet 最终会把 Pod 放在哪个 zone，只能做最悲观的假设。代价是短时间内可能低估可用资源，但不会高估。缓存快照只在 Flush 时整体替换，不会随 NRT 对象的更新自动刷新：watcher 送来的事件里只处理属性变化（`WatchReasonAttrChanged`），新出现的 NRT 对象（`WatchReasonNewlyAdded`）只记一条 `unsupported NRT event` 日志；预留也只在 Unreserve 和 Flush 时删除，节点在缓存眼里只会单调地变「满」，直到被 Filter 拒绝、标脏、再成功 Flush。

默认的 Passthrough 模式有两个问题。一是批量调度时最危险：一批 Pod 可能都看到同一份旧数据，全部调度到同一个 NUMA zone，再被 kubelet 依次拒绝。二是开销：插件构造的是 controller-runtime 的直连客户端，没有缓存 reader，`GetCachedNRTCopy` 每次都发一个 GET，Filter 和 Score 各调用一次，一个 Pod 的调度周期里每个被评估的节点就是两次 API 请求。生产上至少应该打开 OverReserve。

抢占默认关闭（`preemptionMode: Disabled`），打开后插件通过 PreFilterExtensions 的 `AddPod`/`RemovePod` 在 CycleState 里维护一个受害者栈，Filter 时用它模拟驱逐后的 NUMA 可用量。

---

## 六、网络感知：`NetworkOverhead` 与 `TopologicalSort`

这两个插件依赖 diktyo-io 定义的两个 CRD：

- `AppGroup`：描述一组微服务的依赖关系，每个依赖可以声明 `maxNetworkCost`；
- `NetworkTopology`：描述 region 之间、zone 之间的网络代价矩阵。

Pod 通过 label `appgroup.diktyo.x-k8s.io` 标识所属 AppGroup，`appgroup.diktyo.x-k8s.io.workload` 标识自己是其中哪个 workload。节点的 region 和 zone 取自标准 label `topology.kubernetes.io/region` 与 `topology.kubernetes.io/zone`。

### 6.1 `NetworkOverhead`：按依赖计算网络代价

完整拆解见[第 29 篇](/posts/sched-plugin-29-network-overhead/)。

PreFilter 读出当前 Pod 的依赖列表，以及这些依赖已经调度到了哪些节点，并为每个候选节点构造到其他 region/zone 的代价表，写入 CycleState。「已经调度」的依赖 Pod 来自 Pod informer（`spec.nodeName` 非空），调度器缓存里刚 assume、尚未完成绑定的 Pod 不在其中，所以连续出队的 Pod 之间互相看不见。

Filter 对每个「已调度的依赖 Pod」做一次判定：

| 情况 | 结果 |
| --- | --- |
| 在同一节点，或同一 zone | 满足 |
| 跨 zone/region，代价 ≤ `maxNetworkCost` | 满足 |
| 跨 zone/region，代价 > `maxNetworkCost` | 违反 |
| 跨 zone/region，代价表里查不到这一对 | 不计数 |
| 对方节点没有 region/zone label | 违反 |

候选节点**自己**没有 region、zone label 时，代价表为空，跨节点的依赖（对方节点同样没有 label 的除外）都落到「查不到」，既不算满足也不算违反，这样的节点在 Filter 上基本总能通过。

```go
// pkg/networkaware/networkoverhead/networkoverhead.go
	// The pod is filtered out if the number of violated dependencies is higher than the satisfied ones
	if violated > satisfied {
		return fwk.NewStatus(fwk.Unschedulable,
			fmt.Sprintf("Node %v does not meet several network requirements from Workload dependencies: Satisfied: %v Violated: %v", nodeInfo.Node().Name, satisfied, violated))
	}
```

违反的数量多于满足的数量才过滤，不是只要有违反就过滤。这是一个宽松的多数判定。

Score 累加到每个依赖的网络代价：同节点 0，同 zone 1，跨 zone 或跨 region 取代价表中的值；查不到、或对方节点没有 region/zone 时按最大值 100 计算。注意 Filter 与 Score 对「查不到」的处理不对称：Filter 放行，Score 惩罚。原始分是代价，`NormalizeScore` 做反向归一化，代价最小的节点得 100 分。

没有 AppGroup label、没有依赖、或依赖都还没调度的 Pod，所有节点得分相同，插件对它不起作用。但 Pod 带了 AppGroup label、而对应的 `AppGroup` 或 `NetworkTopology` 找不到时，情况完全不同：两个查找函数返回 nil，PreFilter 随后对 nil 指针解引用，框架不 recover，调度器进程直接退出。KEP 里「缺少这两个 CRD 时所有节点同分」的说法与代码不符，`networkTopologyName` 写错、CR 建在 `namespaces` 参数之外都会触发。

插件也没有实现 `EnqueueExtensions`，框架为它注册默认的事件集，被它拒绝的 Pod 会在各类 Pod、Node 事件上被唤醒，`AppGroup`、`NetworkTopology` 的变化反而不会触发重试。

### 6.2 `TopologicalSort`：按 AppGroup 的拓扑序出队

完整拆解见[第 26 篇](/posts/sched-plugin-26-topological-sort/)。它是 QueueSort 插件：

```go
// pkg/networkaware/topologicalsort/topologicalsort.go
	// If pods do not belong to an AppGroup, or being to different AppGroups, follow vanilla QoS Sort
	if p1AppGroup != p2AppGroup || len(p1AppGroup) == 0 {
		// ...
		s := &queuesort.PrioritySort{}
		return s.Less(pInfo1, pInfo2)
	}
	// ...
	appGroup := ts.findAppGroupTopologicalSort(ctx, agName)
	// ...
	// Binary search to find both order index since topology list is ordered by Workload Name
	orderP1 := networkawareutil.FindPodOrder(appGroup.Status.TopologyOrder, labelsP1[agv1alpha.AppGroupSelectorLabel])
	orderP2 := networkawareutil.FindPodOrder(appGroup.Status.TopologyOrder, labelsP2[agv1alpha.AppGroupSelectorLabel])
	// ...
	// Lower is better
	return orderP1 <= orderP2
```

不同 AppGroup 或没有 AppGroup 的 Pod 回退到 `PrioritySort`（代码注释和 KEP 写的都是 QoS Sort，实际调用的是 `PrioritySort`，不比较 QoS）。同一 AppGroup 内，按 controller 计算好的 `status.topologyOrder` 排序。这个数组按 workload 的 selector 字典序存放，拓扑序体现在每个元素的 `index` 上，`FindPodOrder` 在其中二分查找，查不到返回 `-1`；手工填写或自己实现 controller 时没有按 selector 排序，二分会漏掉存在的元素，这些 Pod 都会得到 `-1`，排到同组正常 Pod 的前面。

它本应和 `NetworkOverhead` 配套：`NetworkOverhead` 的判定依赖「已经调度好的依赖 Pod」，只有被调用方先落位，调用方才能被放到离它近的地方。但拓扑序的方向取决于 AppGroup 选的排序算法。依赖边按「调用方 → 被调用方」建图，示例和 KEP 默认用的 `KahnSort` 从入度为 0 的节点开始，排在最前面的是 frontend 这样的**调用方**；整组同时提交时，frontend 先出队，它依赖的服务都还没调度，`NetworkOverhead` 无从参考。想要「被依赖的服务先调度」，需要选 `ReverseKahn`、`ReverseTarjan` 这类反向算法。

注意最后一行用的是 `<=`，两个 Pod 顺序相同时 `Less(a, b)` 和 `Less(b, a)` 都返回 true，不满足严格弱序。影响不只是同序 Pod 之间顺序不稳定：同组按 index、跨组按优先级，两套规则混用时三个 Pod 之间可以构成环（A 在 B 前、B 在 C 前、C 又在 A 前），堆顶不再保证是全局最小，高优先级的普通 Pod 和 AppGroup 里的低优先级 Pod 谁先出队取决于插入历史。

还有一个更直接的风险：`findAppGroupTopologicalSort` 在所有 namespace 都查不到 AppGroup 时返回 nil，`Less` 紧接着访问 `appGroup.Status.TopologyOrder`。Pod 带着 AppGroup label 而 AppGroup 不存在（名字拼错、建在 `namespaces` 参数之外、已被删除）时，调度队列的堆操作里就会发生空指针 panic，调度器进程退出。

---

## 七、四个小型打分插件

### 7.1 `NodeResourcesAllocatable`：按节点规格打分

```go
// pkg/noderesources/allocatable.go
for resource, weight := range resToWeightMap {
    resourceScore := score(logger, allocable[resource], mode)
    nodeScore += resourceScore * weight
    weightSum += weight
}
return nodeScore / weightSum
...
case config.Least: return -1 * capacity
case config.Most:  return capacity
```

只看节点的 `Allocatable`，与节点上已经跑了什么无关：打分函数的签名里有 `requested` 参数，但函数体完全没有用它。`Least` 模式（默认）取负值，即偏好规格小的节点；`Most` 模式偏好规格大的节点。

默认权重 `cpu: 1<<20, memory: 1`，含义是 1 毫核与 1 MiB 内存等价。原始分数再做 min-max 归一化到 `[0, 100]`。

典型场景是异构集群：小 Pod 优先放到小规格节点，把大节点留给大 Pod，降低碎片。它与 in-tree 的 `NodeResourcesFit` 互补：后者看已分配比例，这个看节点本身的规格。详见[第 33 篇](/posts/sched-plugin-33-node-resources-allocatable/)。

### 7.2 `NodeMetadata`：把节点 label/annotation 变成分数

| 参数 | 取值 |
| --- | --- |
| `metadataSource` | `Label` / `Annotation` |
| `metadataKey` | 任意 key |
| `metadataType` | `Number` / `Timestamp` |
| `scoringStrategy` | `Number` 对应 `Highest` / `Lowest`；`Timestamp` 对应 `Newest` / `Oldest` |
| `timestampFormat` | 默认 RFC3339 |

原始分：数字用 `strconv.ParseFloat` 解析后直接转成 `int64`，向零截断（`3.9` 记为 3，`-3.9` 记为 −3），不是四舍五入，小数差异会被抹平；`Lowest` 取负。时间戳换算成「距今秒数」，`Newest` 取负。缺失或解析失败记为 `math.MinInt64`。

归一化时，无效节点得 0 分，有效节点映射到 `[1, 100]`，保证「有数据的节点」一定排在「没数据的节点」前面。这一点和 Peaks 正好相反。

可以用来表达「优先用便宜的机型」「优先用新上架的节点」「优先用维护窗口最远的节点」这类不需要写代码的偏好。详见[第 34 篇](/posts/sched-plugin-34-node-metadata/)。

### 7.3 `PodState`：终止中的 Pod 是好消息，被提名的 Pod 是坏消息

```text
原始分 = 节点上 DeletionTimestamp 非空的 Pod 数 − 被提名到该节点的 Pod 数
```

再做 min-max 归一化。

终止中的 Pod 即将释放资源，这个节点很快会更空；被提名的 Pod 是抢占者，它们已经预定了这个节点上即将腾出的资源，再挤过去很可能和它们冲突。这个插件让调度器往前一种节点去，避开后一种。

它只数个数，不看资源量和优先级，有两个副作用。一是抢占目标节点上两项会相互抵消：抢占成功后，受害者处于终止中（+1），抢占者被提名到这个节点（−1），驱逐了多个受害者时差值甚至为正，节点反而被加分。二是会把自己算进去：带着 `nominatedNodeName` 重新调度的 Pod 仍在 nominator 里，它的提名节点因此被扣一分（只在提名节点 Filter 不通过、需要对全部节点打分时才会体现）。详见[第 35 篇](/posts/sched-plugin-35-pod-state/)。

### 7.4 `SySched`：按系统调用画像放置

出发点是 seccomp 的攻击面。节点上 Pod 允许的系统调用越杂，某个 Pod 被攻破后，能利用的内核接口就越多。`SySched` 的打分是「把这个 Pod 放上来，会额外暴露多少系统调用」：

```text
score = |节点系统调用并集 − 本 Pod 的系统调用|
      + Σ 节点上每个已有 Pod p： |(节点并集 ∪ 本 Pod) − p 的系统调用|
```

第一项是本 Pod 因为和别人共处而多暴露的部分，第二项是已有 Pod 因为本 Pod 的到来而多暴露的部分。`NormalizeScore` 反向归一化，暴露越少分越高。

系统调用集合来自 Security Profiles Operator 的 `SeccompProfile` CR：插件从 Pod 的 `seccompProfile.localhostProfile` 路径中解析出 CR 的 namespace 和名字，取 `action` 为 `SCMP_ACT_ALLOW` 或 `SCMP_ACT_LOG` 的系统调用。

几个需要注意的边界：

- 没有任何 seccomp 配置的 Pod，会改用参数 `defaultProfileNamespace`/`defaultProfileName`（默认 `default/all-syscalls`）指向的全集 profile，被视为暴露了全部系统调用。它放到哪个节点，哪个节点的并集就接近全集，之后受限的 Pod 都会回避这个节点，这正是插件隔离「坏邻居」的方式。仓库示例里的全集 profile 叫 `full-seccomp`，示例配置因此显式改了 `defaultProfileName`；只用默认参数又没有创建 `all-syscalls` 时，这类 Pod 的画像为空，`Score` 对所有节点都返回 `math.MaxInt64`。`DefaultNormalizeScore` 里 `100 × MaxInt64` 在 int64 中溢出，最终每个节点都是 100 分，插件对这类 Pod 失效；
- 节点画像由 informer 维护，并不是「只统计 Running 的 Pod」：调度器启动时已在运行的 Pod 在 Add 事件里加入；新调度的 Pod 在 Update 事件里、旧对象处于 `Pending` 且已有 `HostIP` 时加入，也就是要等 kubelet 上报 `HostIP` 之后的又一次状态更新；Pod 删除或进入终态（调度器的 Pod informer 过滤掉了 `Succeeded`/`Failed`，对插件而言就是删除事件）时移除。插件没有实现 Reserve，刚调度、还没被 kubelet 处理的 Pod 不计入，一批同画像的 Pod 会一起涌向同一个「空」节点；
- `Score` 由框架并发地对多个节点调用，读取的 `HostToPods`、`HostSyscalls` 是普通 map，informer 回调同时在另一个 goroutine 里写，全程没有锁，存在并发读写 map 导致进程 fatal 的风险。

详见[第 36 篇](/posts/sched-plugin-36-sysched/)。

---

## 八、队列与抢占

### 8.1 `QOSSort`：优先级相同时按 QoS 出队

```go
// pkg/qos/queue_sort.go
func (*Sort) Less(pInfo1, pInfo2 fwk.QueuedPodInfo) bool {
    p1 := corev1helpers.PodPriority(pInfo1.GetPodInfo().GetPod())
    p2 := corev1helpers.PodPriority(pInfo2.GetPodInfo().GetPod())
    if p1 != p2 {
        return p1 > p2
    }
    qosResult := compQOS(pInfo1.GetPodInfo().GetPod(), pInfo2.GetPodInfo().GetPod())
    if qosResult != 0 {
        return qosResult > 0
    }
    return pInfo1.GetTimestamp().Before(pInfo2.GetTimestamp())
}
```

排序键依次是：优先级、QoS（`Guaranteed` > `Burstable` > `BestEffort`）、入队时间。

它与 `PrioritySort` 唯一的区别是中间多了 QoS 这一层。适合优先级划分粗、但希望 Guaranteed 负载先拿到资源的集群。和另外两个 QueueSort 插件一样，它在整个调度器进程里只能独占（见 1.1 节）。签名上它接收的还是 `fwk.QueuedPodInfo`，而 Kubernetes master 上 `QueueSortPlugin.Less` 的参数已经改为 `QueuedEntityInfo`（可能是单个 Pod，也可能是整个 PodGroup），scheduler-plugins 升级到新版本时，三个 QueueSort 插件都要适配。详见[第 25 篇](/posts/sched-plugin-25-qos-sort/)。

### 8.2 `PreemptionToleration`：允许被抢占，但要等一会儿

默认抢占只看优先级：高优先级 Pod 可以驱逐任何低优先级 Pod，不管后者刚启动 1 秒还是已经跑了 1 天。这个插件让**受害者**通过自己 `PriorityClass` 上的 annotation 声明容忍策略：

```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: batch-low
  annotations:
    preemption-toleration.scheduling.x-k8s.io/minimum-preemptable-priority: "10000"
    preemption-toleration.scheduling.x-k8s.io/toleration-seconds: "3600"
value: 1000
```

判定一个 Pod 是否**免于**被抢占，`ExemptedFromPreemption` 按如下顺序判断：

1. 受害者没有 `priorityClassName`：不豁免，按默认规则处理；
2. 抢占者的 `preemptionPolicy` 是 `Never`：豁免；
3. 抢占者优先级 ≥ `minimum-preemptable-priority`：不豁免，可以直接抢；
4. `toleration-seconds` < 0：永久豁免；
5. 受害者还没有 `PodScheduled=True` 条件：豁免；
6. 其余情况：从 `PodScheduled` 的时间算起，未满 `toleration-seconds` 秒则豁免。

两个 annotation 的默认值分别是 `PriorityClass.value + 1` 和 0，即不配置时行为与默认抢占完全一致。这也意味着两者必须配合使用：只写 `toleration-seconds` 时，门槛仍是 `value + 1`，任何能抢占它的 Pod（优先级必然更高）都已越过门槛，容忍期不起作用；只写门槛、不写时长，低于门槛的抢占者遇到的容忍期是 0 秒，同样立即可抢。

以上面的配置为例：优先级 ≥ 10000 的 Pod 可以随时抢占它；优先级在 1001 到 9999 之间的 Pod，要等它运行满 1 小时才能抢占。适合「低优先级批任务，但跑到一半被杀代价很高」的场景。

有两点实现上的限制。容忍期到期本身不产生任何集群事件，在容忍期内抢占失败的 Pod 留在不可调度队列里，可能要等队列每 5 分钟一次的刷新才重新尝试。其余抢占逻辑是从早期的 in-tree 代码复制来的，已经落后于 v1.36.4 的 `DefaultPreemption`，驱逐同步执行，没有 `PreEnqueue` 配合异步抢占。详见[第 31 篇](/posts/sched-plugin-31-preemption-toleration/)。

### 8.3 `CrossNodePreemption`：只是一份注释掉的示例

`main.go` 里它的注册被注释掉了，`pkg/crossnodepreemption/` 下的实现也整体包在 `/* ... */` 中，编译后只剩一行 `package crossnodepreemption`。被注释的代码用的是很早期的框架 API（`*framework.CycleState`、`framework.NodeToStatusMap`、`defaultpreemption` 包里早已移除的包级函数、不带 `context.Context` 的工厂签名），在 v1.36.4 上取消注释也编译不过。它的思路是：单个节点腾不出位置时，枚举所有低优先级 Pod 的子集，跨多个节点组合受害者。复杂度是 2ⁿ，源码注释写明 `CAVEAT: don't use this in production env`。

跨节点受害者这件事，Kubernetes master 换了一条路：以 PodGroup 为单位。驱逐 disruption mode 为 `All` 的 PodGroup 的一个成员等于驱逐整组，`preemption` 包用 `DomainVictim` 描述这种「不可分割、附带所有受影响节点」的受害单元，受害者集合由组的成员关系决定，而不是搜出来的，复杂度仍是线性的。详见[第 32 篇](/posts/sched-plugin-32-cross-node-preemption/)。

---

## 九、从源码里读出来的注意事项

把上面散落在各节的边界行为集中列一下。它们都只能从代码里读出来，每条都是「行为 → 影响」：

**`CapacityScheduling`**

- 用量在调度器内存中计算，不读 `status.used` → `kubectl get eq` 看到的用量与调度判定用的不一致（controller 只算 `Running`、漏算 overhead；插件对先于配额存在的 Pod 少算）。
- 全局已用不超过 Σ`min` → `min` 总和小于集群容量时，多出来的容量对配额内的 Pod 不可见。
- `min`/`max` 只写部分资源时，其余维度是 0 而不是不限 → 只写 CPU 的 `max` 会拒绝所有请求内存的 Pod。
- 抢占模拟全部节点、同步驱逐 → 大集群里抢占耗时随节点数线性增长。

**`Coscheduling`**

- `minResources` 检查的是集群总量，不逐节点装箱 → 资源碎片化时，PreFilter 通过而 Filter 失败。
- 退避默认关闭 → 资源不足时，大组会反复空转。
- 没有 PreEnqueue、事件没有 QueueingHint → 人数不够的组照样出队，任何 Pod 创建都会唤醒所有被拒的组。
- PodGroup controller 的 phase 只能粗略参考 → `Failed` 是终态、`Unknown` 从不设置、48 小时超时不触发、列 Pod 不限 namespace。

**Trimaran**

- 无指标的节点得 0 分 → 新节点、漏采的节点被系统性避开。
- `Peaks` 例外：无指标、无功耗模型、预测超过 100% 时，反向归一化后得 100 分 → 未建模的节点反而最受欢迎。
- `TargetLoadPacking` 的预测用量优先取 limit → limit 远大于 request 的 Pod 被视为很重。
- `LoadVariationRiskBalancing` 的风险除以 2 → 满载但稳定的节点仍有 50 分；只接 metrics-server 时没有标准差，模型退化。
- 四个插件都没有实现 `SignPlugin` → 启用任何一个都会关掉整个 Profile 的批量签名复用。

**`NodeResourceTopologyMatch`**

- 只过滤 `single-numa-node` 策略的节点 → 其他策略的节点不受 NUMA 保护。
- 默认 Passthrough，不修正上报延迟，且每次 Filter/Score 都直连 API Server GET → 批量调度时扎堆到同一个 NUMA zone，API Server 压力随节点数增长。

**其他**

- `NetworkOverhead` 只在违反数多于满足数时过滤，代价表查不到时不计数 → 部分违反依赖的节点、没有拓扑 label 的节点仍会通过。
- `NetworkOverhead`、`TopologicalSort` 在 Pod 带 AppGroup label 而 CR 缺失时解引用 nil → 调度器进程 panic 退出。
- `SySched` 没配 seccomp 的 Pod 改用默认全集 profile → 它所在的节点会被受限 Pod 回避；默认 profile 也读不到时，整数溢出让所有节点同分，插件失效。

---

## 十、结语

回头看开头的三个问题，这个仓库的插件可以按答案分成三类：

- **状态在 CRD 里、由 controller 维护**：`Coscheduling`、`NetworkOverhead`、`TopologicalSort`。插件主要读 spec，`TopologicalSort` 还依赖 AppGroup controller 算出的 `status.topologyOrder`；`Coscheduling` 的 controller 只回写供人查看的 `status.phase`，插件不读它。CR 本身则缺一不可，网络感知的两个插件在 CR 缺失时会让调度器退出。
- **状态在插件内存里、自己维护**：`CapacityScheduling`、`NodeResourceTopologyMatch` 的缓存、`SySched` 的画像。它们需要从 informer 重建，重启和多副本时要特别留意。
- **状态在外部系统里**：Trimaran 四个插件。调度质量的上限由指标质量决定。

in-tree 插件不需要回答这些问题，因为 API Server 就是唯一的事实来源。out-of-tree 插件的复杂度，大部分不在算法上，而在「谁来保证我读到的数据是对的」上。引入任何一个之前，先把它依赖的数据链路想清楚，比调参重要得多。
