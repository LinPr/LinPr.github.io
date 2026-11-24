---
title: 'scheduler-plugins 27 [out-of-tree] Coscheduling：用 Permit 把一组 Pod 攒齐再放行'
date: 2026-11-19T12:30:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "gang-scheduling", "podgroup"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "Coscheduling 是 scheduler-plugins 里的 gang 调度插件：用 PodGroup CRD 和 pod-group 标签把 Pod 编组，QueueSort 让同组 Pod 相邻出队，PreFilter 做四道前置检查，Permit 让已占好资源的 Pod 原地等齐 minMember，PostFilter 和 Unreserve 在失败时连坐整组。本文逐个拆解这些扩展点，讲清 PodGroup controller 的状态机，并与 in-tree 的 GangScheduling 做源码级对照。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `Coscheduling` 实现的扩展点（QueueSort、PreFilter、PostFilter、Reserve、Permit）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/27.svg" link="/images/posts/sched-plugins/callstack/27.svg" caption="Coscheduling 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-26-topological-sort/)的 TopologicalSort 用一个 AppGroup CRD 决定同一应用里各个 Pod 的出队顺序。它是 QueueSort 插件，本篇的 Coscheduling 也是。[第 25 篇](/posts/sched-plugin-25-qos-sort/)讲过，一个 Profile 只能有一个 QueueSort 插件，所以 QOSSort、TopologicalSort、Coscheduling 三者在同一个 Profile 里互斥。

Coscheduling 要解决的问题和[第 21 篇](/posts/sched-plugin-21-gang-scheduling/)的 GangScheduling 一样：一组 Pod 要么都拿到位置，要么都不占。两者的实现路线却完全不同。

GangScheduling 把整组作为一个 entity 出队，在一次调度周期里逐个试放、失败统一回滚；Coscheduling 诞生在调度框架还只能一个一个处理 Pod 的年代，它让每个 Pod 照常走完调度周期、真实地占住资源，然后在 Permit 扩展点原地等待，等到同组凑够 `minMember` 个再一起放行。

本文基于 scheduler-plugins 提交 `cfeb3aa`，它依赖 Kubernetes v1.36.4。

Coscheduling 运行在 scheduler-plugins 自己构建的调度器二进制里（`cmd/scheduler/main.go` 中的 `app.WithPlugin(coscheduling.Name, coscheduling.New)`），PodGroup 的状态由另一个二进制 `cmd/controller` 里的 PodGroupReconciler 维护。

## 一、它挂在哪些扩展点

Coscheduling 的代码散在五个扩展点和一个 EnqueueExtensions 上。先看它实现了哪些接口，就能知道后面每一节在读哪一段。

```go {linenos=inline}
// pkg/coscheduling/coscheduling.go
// Coscheduling is a plugin that schedules pods in a group.
type Coscheduling struct {
	logger            klog.Logger
	frameworkHandler  fwk.Handle
	pgMgr             core.Manager
	scheduleTimeout   *time.Duration
	pgBackoff         *time.Duration
	pgRejectThreshold float64
}

var _ fwk.QueueSortPlugin = &Coscheduling{}
var _ fwk.PreFilterPlugin = &Coscheduling{}
var _ fwk.PostFilterPlugin = &Coscheduling{}
var _ fwk.PermitPlugin = &Coscheduling{}
var _ fwk.ReservePlugin = &Coscheduling{}

var _ fwk.EnqueueExtensions = &Coscheduling{}
```

五个扩展点加一个 EnqueueExtensions（第 12–18 行的接口断言）。官方给的配置是这样的：

```yaml {linenos=inline}
# manifests/coscheduling/scheduler-config.yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
# ...（leaderElection、clientConnection 省略）
profiles:
- schedulerName: default-scheduler
  plugins:
    multiPoint:
      enabled:
      - name: Coscheduling
    queueSort:
      enabled:
      - name: Coscheduling
      disabled:
      - name: "*"
  pluginConfig:
  - name: Coscheduling
    args:
      permitWaitingTimeSeconds: 10
```

`multiPoint` 打开它实现的所有扩展点，`queueSort` 把默认的 PrioritySort 换掉。因为是追加在默认插件之后，它在 PostFilter 链上排在 DefaultPreemption 后面，这正是 KEP 要求的「this plugin should be configured as the last one among PostFilter plugins」。

{{< figure src="/images/posts/sched-plugins/27-flow.svg" link="/images/posts/sched-plugins/27-flow.svg" caption="Coscheduling 在单个 Pod 调度旅程中的六处介入：排序、前置检查、整组拒绝、空 Reserve、Permit 等齐、超时连坐（点击查看大图）" >}}

扩展点的分工到此清楚：排序、前置检查、整组拒绝、空 Reserve、Permit 等齐。接下来先看这些逻辑依赖的数据——一个 CRD 和三个参数。

## 二、PodGroup CRD 与插件参数

### 2.1 一个 CRD，一个标签

编组靠一个 CRD 加一个标签，没有改动 Pod 的 API。

```go {linenos=inline}
// apis/scheduling/v1alpha1/types.go
// PodGroupSpec represents the template of a pod group.
type PodGroupSpec struct {
	// MinMember defines the minimal number of members/tasks to run the pod group;
	// if there's not enough resources to start all tasks, the scheduler
	// will not start any.
	// The minimum is 1
	// +kubebuilder:validation:Minimum=1
	MinMember int32 `json:"minMember,omitempty"`

	// MinResources defines the minimal resource of members/tasks to run the pod group;
	// if there's not enough resources to start all tasks, the scheduler
	// will not start any.
	MinResources v1.ResourceList `json:"minResources,omitempty"`

	// ScheduleTimeoutSeconds defines the maximal time of members/tasks to wait before run the pod group;
	ScheduleTimeoutSeconds *int32 `json:"scheduleTimeoutSeconds,omitempty"`
}
```

Pod 和 PodGroup 的关联不是 Pod spec 里的字段，而是一个标签：

```go {linenos=inline}
// apis/scheduling/v1alpha1/types.go
	// PodGroupLabel is the default label of coscheduling
	PodGroupLabel = scheduling.GroupName + "/pod-group"
```

也就是 `scheduling.x-k8s.io/pod-group: <PodGroup 名>`。一个典型用法：

```yaml {linenos=inline}
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: PodGroup
metadata:
  name: nginx
spec:
  scheduleTimeoutSeconds: 10
  minMember: 3
---
apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: nginx
spec:
  replicas: 6
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
        scheduling.x-k8s.io/pod-group: nginx
    spec:
      containers:
      - name: nginx
        image: nginx
        resources:
          requests:
            cpu: 3000m
            memory: 500Mi
```

用标签而不是字段的好处是任何控制器、任何 Pod 模板都能直接用，不需要改 API；代价是标签可以随时改、可以拼错，调度器无从校验。后面会看到，「同组 Pod 有多少个」是靠 label selector 列 Pod 数出来的。

### 2.2 三个参数

插件的行为由三个参数控制，分别管等待时长、失败退避和整组拒绝的门槛。

```go {linenos=inline}
// apis/config/types.go
// CoschedulingArgs defines the parameters for Coscheduling plugin.
type CoschedulingArgs struct {
	metav1.TypeMeta

	// PermitWaitingTimeSeconds is the waiting timeout in seconds.
	PermitWaitingTimeSeconds int64
	// PodGroupBackoffSeconds is the backoff time in seconds before a pod group can be scheduled again.
	PodGroupBackoffSeconds int64
	// PodGroupRejectPercentage is the percentage (0-100) of unassigned pods relative to
	// minMember below which PostFilter will not reject the PodGroup.
	// ...
	PodGroupRejectPercentage int32
}
```

默认值在 `apis/config/v1/defaults.go`：`permitWaitingTimeSeconds` 60，`podGroupBackoffSeconds` 0（不 backoff），`podGroupRejectPercentage` 10。校验要求前两个非负、第三个在 0~100 之间；`New` 里对 `PodGroupBackoffSeconds < 0` 又检查了一遍。

`New` 里有两处初始化值得注意：

```go {linenos=inline}
// pkg/coscheduling/coscheduling.go
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	c, _, err := util.NewClientWithCachedReader(ctx, handle.KubeConfig(), scheme)
	// ...
	// Performance improvement when retrieving list of objects by namespace or we'll log 'index not exist' warning.
	handle.SharedInformerFactory().Core().V1().Pods().Informer().AddIndexers(cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})

	scheduleTimeDuration := time.Duration(args.PermitWaitingTimeSeconds) * time.Second
	pgMgr := core.NewPodGroupManager(
		c,
		handle.SnapshotSharedLister(),
		&scheduleTimeDuration,
		// Keep the podInformer (from frameworkHandle) as the single source of Pods.
		handle.SharedInformerFactory().Core().V1().Pods(),
	)
```

PodGroup 通过一个 controller-runtime 的 client 读取，它的 Reader 是一个启动后等待同步完成的 informer cache，所以插件里看似「每次 Get」的调用其实都是读本地缓存。

Pod 则复用调度器自己的 Pod informer，并加了一个按 namespace 的索引，因为插件频繁地按 namespace + 标签列 Pod。

## 三、PodGroupManager 的状态

插件的大部分逻辑在 `core.PodGroupManager` 里，它持有几份进程内状态：

```go {linenos=inline}
// pkg/coscheduling/core/core.go
type PodGroupManager struct {
	// client is a generic controller-runtime client to manipulate both core resources and PodGroups.
	client client.Client
	// snapshotSharedLister is pod shared list
	snapshotSharedLister fwk.SharedLister
	// scheduleTimeout is the default timeout for podgroup scheduling.
	// If podgroup's scheduleTimeoutSeconds is set, it will be used.
	scheduleTimeout *time.Duration
	// permittedPG stores the podgroup name which has passed the pre resource check.
	permittedPG *gocache.Cache
	// backedOffPG stores the podgorup name which failed scheudling recently.
	backedOffPG *gocache.Cache
	// lastFailedSchedulePG stores the last time a PodGroup's scheduling attempt failed.
	// Used by GetCreationTimestamp to prevent head-of-line blocking.
	lastFailedSchedulePG sync.Map
	// podLister is pod lister
	podLister listerv1.PodLister
	// assignedPodsByPG stores the pods assumed or bound for podgroups
	assignedPodsByPG map[string]sets.Set[string]
	sync.RWMutex
}
```

| 状态 | 键 | 谁写 | 谁读 | 生命周期 |
|---|---|---|---|---|
| `permittedPG` | `ns/pg` | PreFilter 资源检查通过 | PreFilter | TTL = scheduleTimeout（默认 60s），PostFilter/Unreserve 删除 |
| `backedOffPG` | `ns/pg` | PostFilter 整组拒绝时 | PreFilter | TTL = podGroupBackoffSeconds |
| `lastFailedSchedulePG` | `ns/pg` | PostFilter、Unreserve | QueueSort | Permit 成功时删除 |
| `assignedPodsByPG` | `ns/pg` → Pod 名集合 | Permit、Pod informer Add | Permit、PostFilter | Unreserve、Pod informer Delete 删除 |

`assignedPodsByPG` 是 gang 判定的核心计数。它有两个来源：Permit 时把当前 Pod 加进去（此时 Pod 只是 assume，还没绑定）；Pod informer 的 Add 事件里，带 `nodeName` 的 Pod 也会加进去（主要覆盖调度器重启后已经在跑的 Pod）。

删除来源也是两个：Unreserve，以及已绑定 Pod 的 Delete 事件。所以它近似等于「已经过了 Permit 或已经绑定、且还存在的同组 Pod」。

这些状态都在调度器进程的内存里，不写回 API。调度器重启后，`permittedPG`、`backedOffPG`、`lastFailedSchedulePG` 全部清空，`assignedPodsByPG` 由 informer 的初始 List 重建出已绑定的部分。

## 四、QueueSort：让同组 Pod 挨在一起

有了这些状态，排序函数就能解释一件事：同组的 Pod 为什么会在队列里排在一起。

```go {linenos=inline}
// pkg/coscheduling/coscheduling.go
// Less is used to sort pods in the scheduling queue in the following order.
// 1. Compare the priorities of Pods.
// 2. Compare the initialization timestamps of PodGroups or Pods.
// 3. Compare the keys of PodGroups/Pods: <namespace>/<podname>.
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

和 [PrioritySort](/posts/sched-plugin-02-priority-sort/) 相比，关键差别在第二步用的时间戳：

```go {linenos=inline}
// pkg/coscheduling/core/core.go
func (pgMgr *PodGroupManager) GetCreationTimestamp(ctx context.Context, pod *corev1.Pod, ts time.Time) time.Time {
	pgName := util.GetPodGroupLabel(pod)
	if len(pgName) == 0 {
		return ts
	}
	pgFullName := fmt.Sprintf("%v/%v", pod.Namespace, pgName)
	// If PodGroup has failed scheduling recently, use the failure timestamp
	// to prevent head-of-line blocking.
	if lastFailed, exist := pgMgr.lastFailedSchedulePG.Load(pgFullName); exist {
		return lastFailed.(time.Time)
	}
	var pg v1alpha1.PodGroup
	if err := pgMgr.client.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pgName}, &pg); err != nil {
		return ts
	}
	return pg.CreationTimestamp.Time
}
```

同组的所有 Pod 拿到的是同一个时间戳（PodGroup 的创建时间），比较结果相等，于是落到第三步按 `namespace/podname` 排（第 14–15 行）。效果是：同优先级下，同一个组的 Pod 在 activeQ 里连续排列，调度器会连着处理它们。

这是 Coscheduling 的基础假设，KEP 里「Lightweight coscheduling based on back-to-back queue sorting」说的就是这件事。同组的 Pod 被连续调度，它们在 Permit 处等待的时间就短，占着资源空等的时间也短。

`lastFailedSchedulePG` 是后来加的修正。如果一个创建得很早的大组一直放不下，它的 Pod 会因为时间戳最早而一次次排在队首，每次都失败、每次都占掉一轮调度周期，后面能放下的组被饿住。记录失败时刻之后，失败的组时间戳变成「刚才」，排到同优先级其他组的后面。Permit 整组成功时 `ClearPodGroupScheduleFailure` 清除记录，恢复按创建时间排序。

两个细节：

- **优先级比较在时间戳之前。** README 特意提醒「Pods in the same PodGroup with different priorities might lead to unintended behavior」。同组 Pod 优先级不同，会被其他组的 Pod 插在中间，同组相邻的假设就不成立了。
- **Less 在 activeQ 的堆操作里被调用，持有队列锁。** 每次比较都要走一次 `GetCreationTimestamp`，可能要读 `sync.Map` 和 informer cache。读本地缓存很快，但这是 Coscheduling 比 PrioritySort 多出的一份常数开销。

## 五、PreFilter：四道检查

排序只决定谁先出队，真正拦住「凑不齐的组」的是 PreFilter。

```go {linenos=inline}
// pkg/coscheduling/coscheduling.go
func (cs *Coscheduling) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	lh := klog.FromContext(klog.NewContext(ctx, cs.logger)).WithValues("ExtensionPoint", "PreFilter")
	// If PreFilter fails, return framework.UnschedulableAndUnresolvable to avoid
	// any preemption attempts.
	if err := cs.pgMgr.PreFilter(ctx, pod); err != nil {
		lh.Error(err, "PreFilter failed", "pod", klog.KObj(pod))
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, err.Error())
	}
	return nil, fwk.NewStatus(fwk.Success, "")
}
```

所有失败统一返回 `UnschedulableAndUnresolvable`（第 8 行），注释说得很清楚：避免任何抢占尝试。PreFilter 失败时，所有节点都会带上这个状态，DefaultPreemption 只会在 `Unschedulable` 的节点上找候选，于是一个候选都没有。没有 `PreFilterResult`，也不缩小节点集合，`PreFilterExtensions` 返回 nil。

真正的逻辑在 `PodGroupManager.PreFilter`：

```go {linenos=inline}
// pkg/coscheduling/core/core.go
func (pgMgr *PodGroupManager) PreFilter(ctx context.Context, pod *corev1.Pod) error {
	// ...
	pgFullName, pg := pgMgr.GetPodGroup(ctx, pod)
	if pg == nil {
		return nil
	}

	if _, exist := pgMgr.backedOffPG.Get(pgFullName); exist {
		return fmt.Errorf("podGroup %v failed recently", pgFullName)
	}

	pods, err := pgMgr.podLister.Pods(pod.Namespace).List(
		labels.SelectorFromSet(labels.Set{v1alpha1.PodGroupLabel: util.GetPodGroupLabel(pod)}),
	)
	if err != nil {
		return fmt.Errorf("podLister list pods failed: %w", err)
	}

	quorumGap := int(pg.Spec.MinMember) - len(pods)
	if quorumGap > 0 {
		return fmt.Errorf("pre-filter pod %v cannot find enough sibling pods, "+
			"current pods number: %v, minMember of group: %v", pod.Name, len(pods), pg.Spec.MinMember)
	}

	// Extra check to see how many SchedulingGated Pods can be tolerated.
	// quorumGap can be negative if a PodGroup's minMember < a workload's replicas.
	for _, p := range pods {
		if len(p.Spec.SchedulingGates) > 0 {
			quorumGap++
		}
		if quorumGap > 0 {
			return fmt.Errorf("pre-filter pod %v cannot proceed due to gated pods in the same PodGroup", pod.Name)
		}
	}

	if pg.Spec.MinResources == nil {
		return nil
	}

	// TODO(cwdsuzhou): This resource check may not always pre-catch unschedulable pod group.
	// It only tries to PreFilter resource constraints so even if a PodGroup passed here,
	// it may not necessarily pass Filter due to other constraints such as affinity/taints.
	if _, ok := pgMgr.permittedPG.Get(pgFullName); ok {
		return nil
	}

	nodes, err := pgMgr.snapshotSharedLister.NodeInfos().List()
	// ...
	minResources := pg.Spec.MinResources.DeepCopy()
	podQuantity := resource.NewQuantity(int64(pg.Spec.MinMember), resource.DecimalSI)
	minResources[corev1.ResourcePods] = *podQuantity
	err = CheckClusterResource(ctx, nodes, minResources, pgFullName)
	if err != nil {
		lh.Error(err, "Failed to PreFilter", "podGroup", klog.KObj(pg))
		return err
	}
	pgMgr.permittedPG.Add(pgFullName, pgFullName, *pgMgr.scheduleTimeout)
	return nil
}
```

先说「放行」的情况：Pod 没有 pod-group 标签，或者标签指向的 PodGroup 读不到（`GetPodGroup` 在 `client.Get` 出错时返回 nil 的 pg），PreFilter 都直接通过。后一种情况会在 Permit 处被拦下，返回 `PodGroup not found`。

然后是四道检查：

**① backoff 名单。** PostFilter 整组拒绝时可以把组放进 `backedOffPG`（第七节），在 TTL 内，这个组的任何 Pod 进入 PreFilter 都会被直接拒绝。默认 `podGroupBackoffSeconds` 为 0，这道检查形同虚设。

**② 人数。** 用 label selector 在 Pod 的命名空间里列出同组 Pod，数量不足 `minMember` 就拒绝。注意这里数的是**所有**带标签的 Pod，不论是否已经调度、是否处于 Terminating，也不论 `schedulerName` 是不是这个调度器。

**③ scheduling gate。** `quorumGap` 在第二步之后是「minMember 减 Pod 总数」，通常小于等于 0，表示有富余。每发现一个带 `schedulingGates` 的同组 Pod，就把缺口加一，缺口转正就拒绝。比如 `minMember: 3`、5 个 Pod，富余是 2，可以容忍 2 个 gated Pod；有 3 个 gated 就拒绝。

这条检查让 Coscheduling 和 [SchedulingGates](/posts/sched-plugin-01-scheduling-gates/) 配合得上：被 gate 挡住的 Pod 反正进不了调度周期，不应该算作「能参与凑数」的成员。测试里有三个用例分别覆盖「gated 但仍满足 quorum」「gated 导致不足」「全部 gated」。

**④ 集群总资源。** 只有设置了 `minResources` 才做。把 `minResources` 加上 `pods: minMember`，和集群里每个节点的剩余资源逐个相减：

```go {linenos=inline}
// pkg/coscheduling/core/core.go
func CheckClusterResource(ctx context.Context, nodeList []fwk.NodeInfo, resourceRequest corev1.ResourceList, desiredPodGroupName string) error {
	for _, info := range nodeList {
		if info == nil || info.Node() == nil {
			continue
		}

		nodeResource := util.ResourceList(getNodeResource(ctx, info, desiredPodGroupName))
		for name, quant := range resourceRequest {
			quant.Sub(nodeResource[name])
			if quant.Sign() <= 0 {
				delete(resourceRequest, name)
				continue
			}
			resourceRequest[name] = quant
		}
		if len(resourceRequest) == 0 {
			return nil
		}
	}
	return fmt.Errorf("resource gap: %v", resourceRequest)
}
```

这是一个「剩余资源总和是否够」的检查，节点之间的碎片不在考虑之内：三个节点各剩 2 核，总和 6 核，满足 `minResources: cpu: 6`，但一个要 4 核的 Pod 哪里也放不下。TODO 注释也承认它只是一个必要条件。

`getNodeResource` 计算节点剩余量时，会先从节点克隆里删掉本组已经在上面的 Pod，因为这些 Pod 的用量本来就包含在 `minResources` 里，不能重复扣除。

检查通过后，组名被放进 `permittedPG`，TTL 是 scheduleTimeout。在这段时间里同组的后续 Pod 跳过资源检查，避免每个 Pod 都把所有节点扫一遍。

PreFilter 不写 CycleState，四道检查里只有第四道依赖调度周期的 snapshot，其余都读 informer 缓存。

## 六、Filter 之后：Reserve 与 Permit

PreFilter 放行之后，Pod 依次经过 Filter、Score、Reserve。Coscheduling 在前两处没有实现，只是在后两处各占了一个位置——一个空的 Reserve，一个会等待的 Permit。

### 6.1 Reserve 是空的

这个空函数不是遗漏，它的存在有明确目的。

```go {linenos=inline}
// pkg/coscheduling/coscheduling.go
// Reserve is the functions invoked by the framework at "reserve" extension point.
func (cs *Coscheduling) Reserve(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	return nil
}
```

它实现 `ReservePlugin`（第 3–5 行，直接返回 nil）只是为了拿到 `Unreserve` 回调：框架只对实现了 ReservePlugin 接口的插件调用 Unreserve。

### 6.2 Permit：数 assigned，够了就全部放行

真正的等待发生在 Permit。它数一数组里已经占住位置的 Pod，够了就放行，不够就返回 Wait。

```go {linenos=inline}
// pkg/coscheduling/core/core.go
func (pgMgr *PodGroupManager) Permit(ctx context.Context, state fwk.CycleState, pod *corev1.Pod) Status {
	pgFullName, pg := pgMgr.GetPodGroup(ctx, pod)
	if pgFullName == "" {
		return PodGroupNotSpecified
	}
	if pg == nil {
		// A Pod with a podGroup name but without a PodGroup found is denied.
		return PodGroupNotFound
	}

	pgMgr.RWMutex.Lock()
	defer pgMgr.RWMutex.Unlock()
	assigned, exist := pgMgr.assignedPodsByPG[pgFullName]
	if !exist {
		assigned = sets.Set[string]{}
		pgMgr.assignedPodsByPG[pgFullName] = assigned
	}
	assigned.Insert(pod.Name)
	// The number of pods that have been assigned nodes is calculated from the snapshot.
	// The current pod in not included in the snapshot during the current scheduling cycle.
	if len(assigned) >= int(pg.Spec.MinMember) {
		return Success
	}

	if len(assigned) == 1 {
		// Given we've reached Permit(), it's mean all PreFilter checks (minMember & minResource)
		// already pass through, so if len(assigned) == 1, it could be due to:
		// - minResource get satisfied
		// - new pods added
		// In either case, we should and only should use this 0-th pod to trigger activating
		// its siblings.
		// It'd be in-efficient if we trigger activating siblings unconditionally.
		// See https://github.com/kubernetes-sigs/scheduler-plugins/issues/682
		state.Write(permitStateKey, &PermitState{Activate: true})
	}

	return Wait
}
```

插件层把四种结果翻译成框架状态（第 7–33 行的 `switch`）：

```go {linenos=inline}
// pkg/coscheduling/coscheduling.go
func (cs *Coscheduling) Permit(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (*fwk.Status, time.Duration) {
	// ...
	waitTime := *cs.scheduleTimeout
	s := cs.pgMgr.Permit(ctx, state, pod)
	var retStatus *fwk.Status
	switch s {
	case core.PodGroupNotSpecified:
		return fwk.NewStatus(fwk.Success, ""), 0
	case core.PodGroupNotFound:
		return fwk.NewStatus(fwk.Unschedulable, "PodGroup not found"), 0
	case core.Wait:
		// ...
		_, pg := cs.pgMgr.GetPodGroup(ctx, pod)
		if wait := util.GetWaitTimeDuration(pg, cs.scheduleTimeout); wait != 0 {
			waitTime = wait
		}
		retStatus = fwk.NewStatus(fwk.Wait)
		// We will also request to move the sibling pods back to activeQ.
		cs.pgMgr.ActivateSiblings(ctx, pod, state)
	case core.Success:
		pgFullName := util.GetPodGroupFullName(pod)
		cs.pgMgr.ClearPodGroupScheduleFailure(pgFullName)
		cs.frameworkHandler.IterateOverWaitingPods(func(waitingPod fwk.WaitingPod) {
			if util.GetPodGroupFullName(waitingPod.GetPod()) == pgFullName {
				// ...
				waitingPod.Allow(cs.Name())
			}
		})
		// ...
		retStatus = fwk.NewStatus(fwk.Success)
		waitTime = 0
	}

	return retStatus, waitTime
}
```

| `pgMgr.Permit` 结果 | 返回状态 | 等待时间 |
|---|---|---|
| `PodGroupNotSpecified`（没有标签） | Success | 0 |
| `PodGroupNotFound`（有标签但组不存在） | Unschedulable `PodGroup not found` | 0 |
| `Wait`（assigned < minMember） | Wait | `scheduleTimeoutSeconds`，未设置则 `permitWaitingTimeSeconds` |
| `Success`（assigned ≥ minMember） | Success，并 Allow 所有同组 waiting Pod | 0 |

等待时间的优先级在 `util.GetWaitTimeDuration` 里：PodGroup 的 `scheduleTimeoutSeconds` 优先，其次是插件参数，两者都没有用 `DefaultWaitTime`（60s）。框架层面还有一个上限：`RunPermitPlugins` 会把超过 15 分钟的等待时间截断为 15 分钟（`maxTimeout`）。

返回 `Wait` 意味着什么？这个 Pod 已经在调度器 cache 里 assume 到了某个节点，资源被计入了节点占用；框架把它放进 `waitingPods` 表，为每个返回 Wait 的插件起一个 `time.AfterFunc` 计时器；它的绑定 goroutine 阻塞在 `WaitOnPermit`。

在 v1.36 的绑定周期里，`NominatedNodeNameForExpectation`（1.35 起 Beta、默认开启）打开时，即将进入 WaitOnPermit 的 Pod 还会被写上 `status.nominatedNodeName`，告诉 Cluster Autoscaler 等外部组件「这个 Pod 马上要落到这里」，所以 `kubectl get pod -o wide` 能看到 waiting Pod 的 NOMINATED NODE。

### 6.3 ActivateSiblings：第一个到达的 Pod 去叫醒同伴

Pod 在 Wait 里等的时候，同组其他 Pod 可能正躺在 unschedulable 队列里，需要有人把它们挪回来。做这件事的是 `ActivateSiblings`。

```go {linenos=inline}
// pkg/coscheduling/core/core.go
func (pgMgr *PodGroupManager) ActivateSiblings(ctx context.Context, pod *corev1.Pod, state fwk.CycleState) {
	// ...
	// Only proceed if it's explicitly requested to activate sibling pods.
	if c, err := state.Read(permitStateKey); err != nil {
		return
	} else if s, ok := c.(*PermitState); !ok || !s.Activate {
		return
	}

	pods, err := pgMgr.podLister.Pods(pod.Namespace).List(
		labels.SelectorFromSet(labels.Set{v1alpha1.PodGroupLabel: pgName}),
	)
	// ...
	if len(pods) != 0 {
		if c, err := state.Read(framework.PodsToActivateKey); err == nil {
			if s, ok := c.(*framework.PodsToActivate); ok {
				s.Lock()
				for _, pod := range pods {
					namespacedName := GetNamespacedName(pod)
					s.Map[namespacedName] = pod
				}
				s.Unlock()
			}
		}
	}
}
```

这是插件里唯一用到 CycleState 的地方：Permit 在 `assigned` 刚好为 1 时写入 `PermitState{Activate: true}`（`pgMgr.Permit` 的第 26–36 行），`ActivateSiblings` 读到才动手，把同组其他 Pod 塞进框架预留的 `PodsToActivate`。调度周期结束时，框架把这些 Pod 从 backoffQ 或 unschedulable 队列里直接挪进 activeQ。

`PermitState.Clone` 返回一个新对象，满足 `StateData` 接口的要求。

为什么只让第一个 Pod 做？注释引用了 issue #682：如果每个进入 Wait 的 Pod 都去激活兄弟，一个 100 人的组会触发 99 次激活，每次都要列一遍 Pod、改一遍队列。第一个 Pod 能走到 Permit，说明 PreFilter 的人数和资源检查都已通过，这正是「此前因为人数不够而退避的兄弟们现在值得重试」的时刻。

## 七、失败时的连坐：PostFilter 与 Unreserve

前面讲的都是顺利凑齐的情况。凑不齐时有两条收场路径：Filter 阶段就找不到节点，走 PostFilter；Permit 等不到人，走 Unreserve。

{{< figure src="/images/posts/sched-plugins/27-permit-timeline.svg" link="/images/posts/sched-plugins/27-permit-timeline.svg" caption="minMember=3 时 Permit 的三种结局：及时凑齐、第三个 Pod 失败触发 PostFilter 连坐、等待超时后 Unreserve 连坐（点击查看大图）" >}}

### 7.1 PostFilter：差得多才拒绝整组

PostFilter 处理的是「这个 Pod 根本找不到节点」这种情况。

```go {linenos=inline}
// pkg/coscheduling/coscheduling.go
func (cs *Coscheduling) PostFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod,
	filteredNodeStatusReader fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	// ...
	pgName, pg := cs.pgMgr.GetPodGroup(ctx, pod)
	if pg == nil {
		// ...
		return &fwk.PostFilterResult{}, fwk.NewStatus(fwk.Unschedulable, "can not find pod group")
	}

	// This indicates there are already enough Pods satisfying the PodGroup,
	// so don't bother to reject the whole PodGroup.
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

	// It's based on an implicit assumption: if the nth Pod failed,
	// it's inferrable other Pods belonging to the same PodGroup would be very likely to fail.
	cs.frameworkHandler.IterateOverWaitingPods(func(waitingPod fwk.WaitingPod) {
		if waitingPod.GetPod().Namespace == pod.Namespace && util.GetPodGroupLabel(waitingPod.GetPod()) == pg.Name {
			// ...
			waitingPod.Reject(cs.Name(), "optimistic rejection in PostFilter")
		}
	})

	if cs.pgBackoff != nil {
		pods, err := cs.frameworkHandler.SharedInformerFactory().Core().V1().Pods().Lister().Pods(pod.Namespace).List(
			labels.SelectorFromSet(labels.Set{v1alpha1.PodGroupLabel: util.GetPodGroupLabel(pod)}),
		)
		if err == nil && len(pods) >= int(pg.Spec.MinMember) {
			cs.pgMgr.BackoffPodGroup(pgName, *cs.pgBackoff)
		}
	}

	cs.pgMgr.DeletePermittedPodGroup(ctx, pgName)
	cs.pgMgr.MarkPodGroupScheduleFailure(pgName)
	return &fwk.PostFilterResult{}, fwk.NewStatus(fwk.Unschedulable,
		fmt.Sprintf("PodGroup %v gets rejected due to Pod %v is unschedulable even after PostFilter", pgName, pod.Name))
}
```

一个 Pod 在 Filter 阶段找不到节点，而且排在它前面的 DefaultPreemption 也没能通过抢占解决（DefaultPreemption 返回 Success 时 PostFilter 链就结束了，Coscheduling 根本不会被调用），才会走到这里。三条分支：

1. 组已经够数（`assigned ≥ minMember`，第 13–17 行）：这个 Pod 是 minMember 之外的「额外成员」，它放不下不影响组，正常返回 Unschedulable 回队；
2. 差距在阈值内（第 21–25 行）：`(minMember - assigned) / minMember ≤ podGroupRejectPercentage / 100`。默认 10%，`minMember: 10` 时已经有 9 个在等，这个 Pod 失败也不拒绝，让后面的 Pod 再试试。测试用例覆盖了「9/10 不拒绝」「8/10 拒绝」「阈值 0 时总是拒绝」「阈值 100 时从不拒绝」等组合；
3. 差距超过阈值（第 29–34 行）：遍历所有 waiting Pod，同命名空间同标签的全部 `Reject`。这是一个「乐观推断」：第 n 个 Pod 放不下，大概率后面的也放不下，与其让前面的 Pod 占着资源等到超时，不如立刻全部释放。

拒绝之后的三个动作：可选地进入 backoff（只有当同组 Pod 总数已经 ≥ minMember 时才 backoff，人数不够的情况本来就会被 PreFilter 挡住，没必要再加 backoff）；删除 `permittedPG`，让下一轮重新做资源检查；记录失败时刻，影响 QueueSort。

注意 PostFilter 里有两处容易被忽视：

- **assigned 为 0 时也会走到第三条分支。** 比如组里第一个 Pod 就 Filter 失败，`(minMember - 0) / minMember = 100%`，于是标记失败时刻，组的 Pod 整体排到同优先级的其他组后面。
- **PreFilter 失败也会进 PostFilter。** PreFilter 返回 `UnschedulableAndUnresolvable` 后，调度器照样跑 PostFilter 链。DefaultPreemption 找不到候选，返回 Unschedulable，链继续走到 Coscheduling。也就是说「人数不够」被 PreFilter 拦下时，同样会给这个组打上失败时刻，同样会删掉 `permittedPG`。

### 7.2 Unreserve：一个失败，全员失败

Permit 等不到人时走的是另一条路，而这条路最终也落到 Unreserve 上。

```go {linenos=inline}
// pkg/coscheduling/coscheduling.go
// Unreserve rejects all other Pods in the PodGroup when one of the pods in the group times out.
func (cs *Coscheduling) Unreserve(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) {
	// ...
	pgName, pg := cs.pgMgr.GetPodGroup(ctx, pod)
	if pg == nil {
		return
	}
	cs.pgMgr.Unreserve(ctx, pod)
	cs.frameworkHandler.IterateOverWaitingPods(func(waitingPod fwk.WaitingPod) {
		if waitingPod.GetPod().Namespace == pod.Namespace && util.GetPodGroupLabel(waitingPod.GetPod()) == pg.Name {
			// ...
			waitingPod.Reject(cs.Name(), "rejection in Unreserve")
		}
	})
	cs.pgMgr.DeletePermittedPodGroup(ctx, pgName)
	cs.pgMgr.MarkPodGroupScheduleFailure(pgName)
}
```

框架在以下情况会调用 Unreserve：

- Reserve 之后的某个插件失败；
- Permit 拒绝；
- WaitOnPermit 超时或被 Reject；
- PreBind/Bind 失败。

对 Coscheduling 来说最典型的是超时：第一个进入 Wait 的 Pod 的计时器先到期，框架以 `rejected due to timeout after waiting 60s at plugin Coscheduling` 拒绝它，它的绑定周期失败，`handleBindingCycleError` 先跑 Unreserve 再回队。

Coscheduling 的 Unreserve 把它从 `assignedPodsByPG` 里删掉，然后 Reject 所有同组 waiting Pod，后者的绑定周期也依次失败、依次 Unreserve，形成一串连锁反应，最后整组资源全部释放。

被 Reject 的 waiting Pod 回队时，`UnschedulablePlugins` 里是 `Coscheduling`（v1.36 的绑定周期在 `WaitOnPermit` 失败时构造 `FitError`，把 `status.Plugin()` 放进去），于是它们要等 Coscheduling 注册的事件才会被重新激活。

## 八、EventsToRegister

被拒绝的 Pod 不会自己回到调度队列，需要有人告诉它「情况变了，可以再试」。Coscheduling 用 `EventsToRegister` 声明它关心哪些事件。

```go {linenos=inline}
// pkg/coscheduling/coscheduling.go
func (cs *Coscheduling) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	// To register a custom event, follow the naming convention at:
	// https://github.com/kubernetes/kubernetes/pull/101394
	// Please follow: eventhandlers.go#L403-L410
	pgGVK := fmt.Sprintf("podgroups.v1alpha1.%v", scheduling.GroupName)
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Add}},
		{Event: fwk.ClusterEvent{Resource: fwk.EventResource(pgGVK), ActionType: fwk.Add | fwk.Update}},
	}, nil
}
```

两类事件（第 8–9 行）：任何 Pod 的 Add，以及 PodGroup CRD 的 Add/Update。CRD 事件用 `<resource>.<version>.<group>` 形式的字符串注册，调度器看到不认识的资源名会为它建一个动态 informer。

两类事件都**没有** QueueingHintFn，框架会给它们配上 `defaultQueueingHintFn`，一律返回 `Queue`。也就是说，集群里任何一个 Pod 被创建，所有因 Coscheduling 被拒的 Pod 都会被移出 unschedulable 队列，进入 backoffQ 或 activeQ 再试一次，不管新 Pod 是不是同一个组的。

这是 Coscheduling 和 in-tree GangScheduling 在排障体验上最大的区别之一：后者的 hint 会判断新 Pod 是否在同一棵组树里，前者没有任何过滤，繁忙集群里被拒的组会被频繁唤醒、反复在 PreFilter 失败。

另外，Coscheduling 没有实现 PreEnqueue。人数不够的组，它的 Pod 仍然会出队，进入调度周期后才在 PreFilter 被拒，每次都要消耗一轮调度周期。

## 九、PodGroup controller 的状态机

PodGroup 的 `status` 不由调度器写，而由 `cmd/controller` 里的 PodGroupReconciler 维护。它监听 PodGroup 和所有 Pod，Pod 事件通过 `podToPodGroup` 映射到它标签指向的 PodGroup。

```go {linenos=inline}
// pkg/controllers/podgroup_controller.go
	if pg.Status.Phase == schedv1alpha1.PodGroupFinished ||
		pg.Status.Phase == schedv1alpha1.PodGroupFailed {
		return ctrl.Result{}, nil
	}
	// If startScheduleTime - createTime > 2days,
	// do not reconcile again because pod may have been GCed
	if (pg.Status.Phase == schedv1alpha1.PodGroupScheduling || pg.Status.Phase == schedv1alpha1.PodGroupPending) && pg.Status.Running == 0 &&
		pg.Status.ScheduleStartTime.Sub(pg.CreationTimestamp.Time) > 48*time.Hour {
		r.recorder.Event(pg, v1.EventTypeWarning,
			"Timeout", "schedule time longer than 48 hours")
		return ctrl.Result{}, nil
	}

	podList := &v1.PodList{}
	if err := r.List(ctx, podList,
		client.MatchingLabelsSelector{
			Selector: labels.Set(map[string]string{
				schedv1alpha1.PodGroupLabel: pg.Name}).AsSelector(),
		}); err != nil {
		// ...
	}
	pods := podList.Items

	pgCopy := pg.DeepCopy()
	switch pgCopy.Status.Phase {
	case "":
		pgCopy.Status.Phase = schedv1alpha1.PodGroupPending
	case schedv1alpha1.PodGroupPending:
		if len(pods) >= int(pg.Spec.MinMember) {
			pgCopy.Status.Phase = schedv1alpha1.PodGroupScheduling
			if len(pods) > 0 {
				fillOccupiedObj(pgCopy, &pods[0])
			}
		}
	default:
		pgCopy.Status.Running, pgCopy.Status.Succeeded, pgCopy.Status.Failed = getCurrentPodStats(pods)
		if len(pods) < int(pg.Spec.MinMember) {
			pgCopy.Status.Phase = schedv1alpha1.PodGroupPending
			break
		}

		if pgCopy.Status.Succeeded+pgCopy.Status.Running < pg.Spec.MinMember {
			pgCopy.Status.Phase = schedv1alpha1.PodGroupScheduling
		}

		if pgCopy.Status.Succeeded+pgCopy.Status.Running >= pg.Spec.MinMember {
			pgCopy.Status.Phase = schedv1alpha1.PodGroupRunning
		}
		// Final state of pod group
		if pgCopy.Status.Failed != 0 &&
			pgCopy.Status.Failed+pgCopy.Status.Running+pgCopy.Status.Succeeded >= pg.Spec.MinMember {
			pgCopy.Status.Phase = schedv1alpha1.PodGroupFailed
		}
		if pgCopy.Status.Succeeded >= pg.Spec.MinMember {
			pgCopy.Status.Phase = schedv1alpha1.PodGroupFinished
		}
	}

	return r.patchPodGroup(ctx, pg, pgCopy)
```

{{< figure src="/images/posts/sched-plugins/27-controller-state.svg" link="/images/posts/sched-plugins/27-controller-state.svg" caption="PodGroupReconciler 的实际状态转移：Pending 只看 Pod 数，Scheduling 与 Running 共用一段判断，Failed 和 Finished 是终态（点击查看大图）" >}}

照着代码读，有几处和 KEP、类型注释的描述不一致，值得单独列出来：

**`Unknown` 从不被设置。** 类型注释说它表示「部分 Pod 调度了、其余因资源不足等原因调度不上」，但 Reconcile 里没有任何分支会写这个值。

**`Failed` 是终态。** KEP 的 mermaid 图里画了 `Failed --> if_minMember: failed resolved`，即故障恢复后可以回到 Scheduling 或 Running。代码在 Reconcile 开头遇到 `Finished` 或 `Failed` 直接 return，PodGroup 一旦进入 Failed 就不会再变。

对于用 Deployment/ReplicaSet 管理、Pod 失败后会被重建的负载，这意味着 PodGroup 的 phase 会永远停在 Failed，不能再用它判断组的实际状态。好在调度插件完全不读 `status`，Failed 不影响后续调度。

**「失败」的判定很宽。** `Failed != 0 && Failed+Running+Succeeded >= minMember`：只要有一个 Pod 失败，且三类 Pod 加起来够 minMember，就是 Failed。一个 `minMember: 3`、跑了 3 个 Pod 的组，其中一个 Pod 因为 OOM 退出成 Failed，整组就进入终态。

**48 小时超时实际不会触发。** 判断用的是 `ScheduleStartTime - CreationTimestamp`，但整个仓库里没有任何代码给 `ScheduleStartTime` 赋值（只有测试用例里设置过）。它保持零值，减出来是一个很大的负数，永远不大于 48 小时。

**列 Pod 时没有限定命名空间。** `r.List` 只带了 `MatchingLabelsSelector`，没有 `client.InNamespace(pg.Namespace)`。同一个集群里，如果两个命名空间各有一个名为 `nginx` 的 PodGroup，它们的 Pod 会被互相计入对方的 Running/Succeeded/Failed。

对比同目录下的 ElasticQuotaReconciler，它列 Pod 时用了 `client.InNamespace`。调度插件这一侧的 `podLister.Pods(pod.Namespace)` 是带命名空间的，不受影响。

**`OccupiedBy` 只在 Pending → Scheduling 时填一次**，取的是第一个 Pod 的 ownerReferences，格式 `namespace/ownerName`，多个 owner 用逗号连接。

总结一下：PodGroup 的 `status` 更适合当作一个粗略的展示字段，调度逻辑完全不依赖它，监控和自动化也不宜依赖它。

## 十、与 in-tree GangScheduling 的对照

调度插件这一侧读完了，把 Coscheduling 和 in-tree 的 GangScheduling 放在一张表里，差别会看得更清楚。

| | Coscheduling（scheduler-plugins `cfeb3aa`） | GangScheduling（Kubernetes master `aabdf5a2`） |
|---|---|---|
| 组的标识 | Pod 标签 `scheduling.x-k8s.io/pod-group` | Pod 字段 `spec.schedulingGroup.podGroupName`，不可变 |
| 组对象 | CRD `scheduling.x-k8s.io/v1alpha1` PodGroup | 内置 API `scheduling.k8s.io/v1beta1` PodGroup，可嵌套 CompositePodGroup |
| 成组门槛 | `minMember`，可选 `minResources` | `gang.minCount`（或 Basic，视为 1） |
| 调度单位 | 单个 Pod，靠 QueueSort 让同组相邻 | 整组一个队列 entity，`scheduleOnePodGroup` 一次处理 |
| 人数不够时 | Pod 照常出队，在 PreFilter 被拒 | PreEnqueue 拦截，整组不出队 |
| 等待方式 | Pod 在 cache 里 assume，Permit 返回 Wait，占着资源等 | 在 snapshot 里逐个 assume，周期内判定，不提前占用 cache |
| 超时 | `scheduleTimeoutSeconds` / `permitWaitingTimeSeconds`（默认 60s） | 无超时（v1.36 的 Permit 实现是固定 5 分钟） |
| 失败回滚 | PostFilter / Unreserve 遍历 waiting Pod 逐个 Reject | `revertFns` 撤销 snapshot assume |
| 抢占 | 单 Pod 的 DefaultPreemption，不感知组 | `PodGroupPostFilter` 整组抢占 |
| 拓扑感知 | 无（可配 PodTopologySpread 等单 Pod 约束） | TopologyPlacementGenerator 按域生成 placement |
| 重新入队 | Pod Add、PodGroup Add/Update，无 hint，一律 Queue | 带 hint，只在同一组树的事件上 Queue |
| 状态回写 | 独立 controller 写 `status.phase` | 调度器写 `PodGroupInitiallyScheduled` condition |

最本质的差别在「等待发生在哪里」。Coscheduling 的等待在 Permit，Pod 已经在 cache 里占住了资源。两个同时到来的组，如果各占了一半资源、都在 Permit 等另一半，就会互相等到超时，然后同时释放、同时重试，可能反复如此。

Coscheduling 为此加了一系列缓解手段：

- QueueSort 让同组 Pod 连续出队，缩短交错的窗口；
- PreFilter 的 minResources 检查提前排除总量不够的组；
- PostFilter 在第一个 Pod 失败时就整组释放，而不是等到超时；
- 失败时刻参与排序，让反复失败的组让出队首。

这些手段降低了死锁的概率和持续时间，但不能根除。

GangScheduling 的等待发生在一次调度周期内部，snapshot 里的 assume 只对这一轮可见，整组要么提交、要么全部撤销，不存在两组各占一半的中间状态。代价是一个大组会独占调度主循环很长时间。

另一个差别是抢占。Coscheduling 组里的每个 Pod 单独触发 DefaultPreemption，可能为第 1 个 Pod 驱逐了一批 Pod，到第 5 个 Pod 时发现整组还是放不下，前面的驱逐就白费了。in-tree 的 `PodGroupPostFilter` 一次性为整组计算抢占方案。

## 十一、边界情况与运维要点

**`minMember` 应该小于等于副本数。** PreFilter 的第二道检查要求带标签的 Pod 总数 ≥ minMember。ReplicaSet 缩容到 minMember 以下时，剩下的 Pending Pod 永远过不了 PreFilter。反过来，副本数大于 minMember 时，「多出来」的 Pod 在组已经够数后就是普通 Pod，单独调度、单独失败，PostFilter 的第一条分支保证它们不会连坐。

**Terminating 的 Pod 也算人数。** label selector 列出的 Pod 包括正在删除的。滚动更新时，旧 Pod 还没删完、新 Pod 已经创建，人数会被高估，新 Pod 可能在只有部分新成员时就通过了 PreFilter，然后在 Permit 等到超时。

**同组 Pod 的 `schedulerName` 要一致。** 插件只能看到自己这个调度器负责的 Pod 的 Permit，但 PreFilter 数人头时不区分调度器。混用调度器的组可能人数够了却永远凑不齐 Permit。

**permittedPG 的缓存窗口。** 资源检查通过后，同组 Pod 在 scheduleTimeout 时间内都跳过检查。这期间如果别的负载占掉了资源，后续 Pod 会在 Filter 失败，再由 PostFilter 处理。

**等待中的 Pod 占着资源。** waiting Pod 已经在 cache 里 assume，它占用的资源对其他 Pod 不可用。`scheduleTimeoutSeconds` 设得太长（比如几分钟），一个凑不齐的组会长时间压住大块资源；设得太短，大组还没来得及逐个过完调度周期就超时了。

经验上，超时至少要覆盖「一个组的所有 Pod 各走一遍调度周期」的时间，大组需要按组规模估算。

**`kubectl get pg` 的歧义。** 这个 CRD 的 `shortNames` 是 `pg`、`pgs`，复数名 `podgroups`。在同时启用了 in-tree PodGroup API（`scheduling.k8s.io`）的集群里，两个 API 组都有名为 `podgroups` 的资源，命令行里最好写全 `podgroups.scheduling.x-k8s.io`，避免读到另一个 API 组的对象。

## 十二、发散：两条路线会怎样演进

scheduler-plugins 依赖的是 Kubernetes v1.36.4。在那个版本里，in-tree GangScheduling 仍是 PreEnqueue + Permit 的实现（固定 5 分钟超时，`GangScheduling` feature gate 默认关闭），思路和 Coscheduling 相近，只是用内置 API 和 `spec.schedulingGroup` 替换了 CRD 和标签。

到了 master，in-tree 一侧已经转向整组调度周期，等待从 Permit 挪进了调度周期内部（见第 21 篇）。

框架接口的变化也会传导到 Coscheduling。master 上的 `QueueSortPlugin.Less` 参数已经是 `fwk.QueuedEntityInfo`（可能是 Pod，也可能是整组），而 Coscheduling 的 `Less` 签名还是 `fwk.QueuedPodInfo`。

scheduler-plugins 升级到新版本 Kubernetes 时，至少要适配这个接口；更大的问题是，一旦调度器打开 `GenericWorkload`，带 `spec.schedulingGroup` 的 Pod 会走整组路径，而只带标签的 Pod 走单 Pod 路径，两套 gang 机制会在同一个调度器里并存。

在内置 API 稳定之前，Coscheduling 仍是很多批处理平台的现实选择，但新项目值得关注 Workload/PodGroup API 的进展，Job 控制器已经可以在 `WorkloadWithJob` 开启时自动创建 PodGroup。

## 十三、小结

- Coscheduling 用 PodGroup CRD 和 `scheduling.x-k8s.io/pod-group` 标签编组，挂在 QueueSort、PreFilter、PostFilter、Reserve（仅为 Unreserve）、Permit 五个扩展点，是 QueueSort 插件，与 QOSSort、TopologicalSort 互斥。
- QueueSort 按「优先级 → PodGroup 创建时间（最近失败过则用失败时刻）→ namespace/name」排序，让同组 Pod 连续出队，并避免反复失败的组霸占队首。
- PreFilter 做四道检查：backoff 名单、Pod 总数 ≥ minMember、扣除 gated Pod 后仍够数、集群剩余资源总和 ≥ minResources；任一失败返回 `UnschedulableAndUnresolvable`，以避免抢占。
- Permit 把 Pod 计入 `assignedPodsByPG`，够 minMember 就 Allow 所有同组 waiting Pod，否则 Wait（默认 60s），第一个进入 Wait 的 Pod 负责激活兄弟。
- PostFilter 在差距超过 `podGroupRejectPercentage`（默认 10%）时 Reject 整组 waiting Pod；Unreserve 在超时或其他失败时同样连坐整组。
- PodGroup controller 的 phase 以代码为准：Unknown 从不设置，Failed 是终态，48 小时超时不会触发，列 Pod 不限命名空间。
- 与 in-tree GangScheduling 相比，Coscheduling 让 Pod 占着资源在 Permit 等待，存在组间互等的风险，靠排序、预检、乐观拒绝和超时来缓解。

下一篇是另一个依赖 CRD、在调度器里维护进程内用量记录的插件：做多租户弹性配额的 [CapacityScheduling](/posts/sched-plugin-28-capacity-scheduling/)。
