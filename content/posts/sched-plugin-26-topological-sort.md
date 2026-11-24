---
title: 'scheduler-plugins 26 [out-of-tree] TopologicalSort：按应用依赖的拓扑序出队'
date: 2026-11-19T12:20:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "queuesort", "network-aware"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "TopologicalSort 是网络感知框架里的 QueueSort 插件：同一个 AppGroup 的 Pod 按 controller 预先算好的 topologyOrder 出队，其余回退到 PrioritySort。本文拆 AppGroup CRD、拓扑序的产生与存储方式、Less 的比较规则，以及这个比较函数不满足严格弱序、AppGroup 缺失时会 panic 等只有读代码才看得到的问题。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `TopologicalSort` 实现的扩展点（QueueSort）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/26.svg" link="/images/posts/sched-plugins/callstack/26.svg" caption="TopologicalSort 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-25-qos-sort/)讲了 out-of-tree 插件的运行方式，以及一个调度器进程只能有一个 QueueSort 插件。TopologicalSort 是 scheduler-plugins 里第二个 QueueSort 插件，和 QOSSort、Coscheduling 互斥。

它属于「网络感知调度」（KEP 260 Network-Aware Scheduling）的一部分，与[第 29 篇](/posts/sched-plugin-29-network-overhead/)的 NetworkOverhead 配套：NetworkOverhead 决定 Pod 放在哪个节点，TopologicalSort 决定同一个应用里谁先被调度。`pkg/networkaware/README.md` 把两者的成熟度都标为 Sample。

本文代码基于 scheduler-plugins `cfeb3aa`（依赖 Kubernetes v1.36.4），AppGroup 的类型定义来自依赖模块 `github.com/diktyo-io/appgroup-api v1.0.1-alpha`。

## 一、为什么调度顺序会影响放置结果

上一篇的 QOSSort 换掉了排序键，但排序的依据仍然是「Pod 自己身上的属性」。TopologicalSort 的依据是 Pod 之间的关系，所以先要说明这种关系为什么重要。

一个微服务应用通常一次性提交：前端、购物车、商品目录、缓存，每个都是一个 Deployment，几十个 Pod 几乎同时进入调度队列。

NetworkOverhead 的打分依据是「这个 Pod 依赖的那些 Pod 已经被放在了哪里」，它在 PreFilter 里只看已经绑定到节点的依赖 Pod。如果一个 Pod 出队时它的依赖都还没有调度，NetworkOverhead 找不到参照物，只能给所有节点打相同的分。

所以对网络感知调度来说，同一个应用内部的出队顺序会影响最终的放置质量。PrioritySort 只按优先级和入队时间排序，不知道 Pod 之间的依赖关系；TopologicalSort 把应用的依赖图做拓扑排序，按排好的顺序出队。

Pod 通过两个 label 声明自己属于哪个应用、是其中哪个 workload：

```yaml {linenos=inline}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cart
spec:
  template:
    metadata:
      labels:
        appgroup.diktyo.x-k8s.io: shop            # 所属 AppGroup 的名字
        appgroup.diktyo.x-k8s.io.workload: cart   # 在 AppGroup 里的 workload selector
    spec:
      schedulerName: network-aware-scheduler
      # ...
```

两个 label 的 key 是 appgroup-api 里的常量：

```go {linenos=inline}
// github.com/diktyo-io/appgroup-api@v1.0.1-alpha/pkg/apis/appgroup/v1alpha1/types.go
const (
	// AppGroupLabel is the default label of the AppGroup for the network-aware framework
	AppGroupLabel = appgroupapi.GroupName

	// AppGroupSelectorLabel is the default selector label for Pods belonging to a given Workload (e.g., workload = App-A)
	AppGroupSelectorLabel = AppGroupLabel + ".workload"
	// ...
)
```

`GroupName` 是 `appgroup.diktyo.x-k8s.io`。

两个 label 标出了 Pod 属于哪个应用、在其中扮演哪个角色。角色之间的调用关系则集中写在 AppGroup 里，这就是下一节要看的 CRD。

## 二、AppGroup CRD

### 2.1 spec：依赖图

AppGroup 的 `spec` 由用户手写，描述应用由哪些 workload 组成、谁依赖谁。

```go {linenos=inline}
// github.com/diktyo-io/appgroup-api@v1.0.1-alpha/pkg/apis/appgroup/v1alpha1/types.go
type AppGroupSpec struct {
	// NumMembers defines the number of Pods belonging to the App Group
	// +kubebuilder:validation:Minimum=1
	// +required
	NumMembers int32 `json:"numMembers" protobuf:"bytes,1,opt,name=numMembers"`

	// The preferred Topology Sorting Algorithm
	// +required
	TopologySortingAlgorithm string `json:"topologySortingAlgorithm" protobuf:"bytes,2,opt,name=topologySortingAlgorithm"`

	// Workloads defines the workloads belonging to the group
	// +required
	Workloads AppGroupWorkloadList `json:"workloads" protobuf:"bytes,3,rep,name=workloads, casttype=AppGroupWorkloadList"`
}

type AppGroupWorkload struct {
	// Workload reference Info.
	// +required
	Workload AppGroupWorkloadInfo `json:"workload" protobuf:"bytes,1,opt,name=workload, casttype=AppGroupWorkloadInfo"`

	// Dependencies of the Workload.
	// +optional
	Dependencies DependenciesList `json:"dependencies,omitempty" protobuf:"bytes,2,opt,name=dependencies, casttype=DependenciesList"`
}
```

`AppGroupWorkloadInfo` 有 `Kind`、`Name`、`Selector`、`APIVersion`、`Namespace` 五个字段，插件只用 `Selector`，它要和 Pod 上 `appgroup.diktyo.x-k8s.io.workload` 的值对上。

`DependenciesInfo` 除了指向另一个 workload，还带 `MinBandwidth` 和 `MaxNetworkCost`，这两个字段是给 NetworkOverhead 用的，TopologicalSort 不读。

一个四个 workload 的应用写成 AppGroup 是这样：

```yaml {linenos=inline}
apiVersion: appgroup.diktyo.x-k8s.io/v1alpha1
kind: AppGroup
metadata:
  name: shop
  namespace: default
spec:
  numMembers: 4
  topologySortingAlgorithm: KahnSort
  workloads:
  - workload: {kind: Deployment, apiVersion: apps/v1, namespace: default, name: frontend, selector: frontend}
    dependencies:
    - workload: {kind: Deployment, apiVersion: apps/v1, namespace: default, name: cart, selector: cart}
      maxNetworkCost: 8
    - workload: {kind: Deployment, apiVersion: apps/v1, namespace: default, name: catalog, selector: catalog}
      maxNetworkCost: 15
  - workload: {kind: Deployment, apiVersion: apps/v1, namespace: default, name: cart, selector: cart}
    dependencies:
    - workload: {kind: Deployment, apiVersion: apps/v1, namespace: default, name: redis, selector: redis}
  - workload: {kind: Deployment, apiVersion: apps/v1, namespace: default, name: catalog, selector: catalog}
  - workload: {kind: Deployment, apiVersion: apps/v1, namespace: default, name: redis, selector: redis}
```

依赖的方向是「调用方 → 被调用方」：frontend 调用 cart 和 catalog，cart 调用 redis。

### 2.2 status：拓扑序

`spec` 是用户写的，插件的二分查找读的却是 `status` 里的另一份数据。

```go {linenos=inline}
// github.com/diktyo-io/appgroup-api@v1.0.1-alpha/pkg/apis/appgroup/v1alpha1/types.go
type AppGroupStatus struct {
	// The number of actively running workloads (e.g., number of pods).
	// +optional
	// +kubebuilder:validation:Minimum=0
	RunningWorkloads int32 `json:"runningWorkloads,omitempty" protobuf:"bytes,1,opt,name=runningWorkloads"`

	// ScheduleStartTime of the group
	// +optional
	ScheduleStartTime metav1.Time `json:"scheduleStartTime,omitempty" protobuf:"bytes,2,opt,name=scheduleStartTime"`

	// TopologyCalculationTime of the group
	// +optional
	TopologyCalculationTime metav1.Time `json:"topologyCalculationTime,omitempty" protobuf:"bytes,3,opt,name=topologyCalculationTime"`

	// Topology order for TopSort plugin (QueueSort)
	// +optional
	TopologyOrder AppGroupTopologyList `json:"topologyOrder,omitempty" protobuf:"bytes,4,rep,name=topologyOrder,casttype=TopologyList"`
}

// AppGroupTopologyInfo represents the calculated order for a given Workload.
// +protobuf=true
type AppGroupTopologyInfo struct {
	// Workload reference Info.
	Workload AppGroupWorkloadInfo `json:"workload,omitempty" protobuf:"bytes,1,opt,name=workload, casttype=AppGroupWorkloadInfo"`

	// Topology index.
	Index int32 `json:"index,omitempty" protobuf:"bytes,2,opt,name=index"`
}
```

`TopologyOrder` 是一个 `(workload, index)` 列表（第 18 行，元素定义在第 23–29 行），`index` 越小越先调度。插件自己不做拓扑排序，这个列表由 AppGroup controller 计算后写入 status。

controller 不在 scheduler-plugins 仓库里，而是 diktyo-io 社区单独维护的组件，仓库只提供部署清单 `manifests/appgroup/deploy-appgroup-controller.yaml`。可选的算法在 appgroup-api 里列为常量：

```go {linenos=inline}
// github.com/diktyo-io/appgroup-api@v1.0.1-alpha/pkg/apis/appgroup/v1alpha1/types.go
	// Topological Sorting algorithms supported by AppGroup
	AppGroupKahnSort        = "KahnSort"
	AppGroupTarjanSort      = "TarjanSort"
	AppGroupReverseKahn     = "ReverseKahn"
	AppGroupReverseTarjan   = "ReverseTarjan"
	AppGroupAlternateKahn   = "AlternateKahn"
	AppGroupAlternateTarjan = "AlternateTarjan"
```

KEP 用 Online Boutique（11 个 workload，P1 是前端）给出了各算法的结果，摘两行：

| 算法 | 拓扑序 |
| --- | --- |
| Kahn | P1, P10, P9, P8, P7, P6, P5, P4, P3, P2, P11 |
| Reverse Kahn | P11, P2, P3, P4, P5, P6, P7, P8, P9, P10, P1 |

注意方向：按「调用方 → 被调用方」建图，Kahn 算法从入度为 0 的节点开始，排在第一位的是前端 P1，也就是**调用方先出队**；Reverse Kahn 把它倒过来，叶子服务（P11 是 redis-cart）先出队。这一点在第六节还会回到。

### 2.3 一个容易忽略的存储约定

知道了 `TopologyOrder` 存的是什么，还要知道它按什么顺序存，因为插件是用二分查找读它的。

`TopologyOrder` 数组的排列顺序**不是**拓扑序本身，而是按 `workload.selector` 的字典序排好的；拓扑序体现在每个元素的 `index` 上。插件代码的注释写着 `topology list is ordered by Workload Name`，查找时用的是二分：

```go {linenos=inline}
// pkg/networkaware/util/util.go
// FindPodOrder : return the order index of the given pod
func FindPodOrder(t agv1alpha1.AppGroupTopologyList, selector string) int32 {
	low := 0
	high := len(t) - 1

	for low <= high {
		mid := (low + high) / 2
		if t[mid].Workload.Selector == selector {
			return t[mid].Index // Return the index
		} else if t[mid].Workload.Selector < selector {
			low = mid + 1
		} else if t[mid].Workload.Selector > selector {
			high = mid - 1
		}
	}
	return -1
}
```

单元测试也遵守这个约定：KEP 例子里的 `TopologyOrder` 是按 P1、P10、P9 …… 的拓扑序写的，测试在创建 AppGroup 之前先 `sort.Sort(util.ByWorkloadSelector(tt.appGroup.Status.TopologyOrder))` 把它重排成 p1、p10、p11、p2 …… 的字符串字典序。

如果手工写 status 或者自己实现 controller 时没有按 selector 排序，二分查找会漏掉存在的元素，走到第 17 行返回 `-1`。

{{< figure src="/images/posts/sched-plugins/26-topological-order.svg" caption="从依赖图到出队顺序：controller 按 spec.topologySortingAlgorithm 计算拓扑序，把 (selector, index) 按 selector 排好写进 status；Less 读两个 Pod 的 label，同组时二分查出 index 比大小，否则回退 PrioritySort（点击查看大图）" link="/images/posts/sched-plugins/26-topological-order.svg" >}}

## 三、源码

### 3.1 结构体、参数与客户端

到了插件这一侧，要做的事反而简单：拿到 Pod 的 label，读出 index，比大小。先看它手里有哪些东西。

```go {linenos=inline}
// pkg/networkaware/topologicalsort/topologicalsort.go
// TopologicalSort : Sort pods based on their AppGroup and corresponding microservice dependencies
type TopologicalSort struct {
	client.Client
	logger     klog.Logger
	handle     fwk.Handle
	namespaces []string
}
```

参数只有一个 `namespaces`：

```go {linenos=inline}
// apis/config/v1/types.go
type TopologicalSortArgs struct {
	metav1.TypeMeta `json:",inline"`

	// Namespaces to be considered by TopologySort plugin
	Namespaces []string `json:"namespaces,omitempty"`
}
```

```go {linenos=inline}
// apis/config/v1/defaults.go
func SetDefaults_TopologicalSortArgs(obj *TopologicalSortArgs) {
	if len(obj.Namespaces) == 0 {
		obj.Namespaces = []string{metav1.NamespaceDefault}
	}
}
```

默认值是 `["default"]`（第 3–5 行），`apis/config/validation` 里没有针对它的校验。AppGroup 只会在这些 namespace 里找，与 Pod 自己在哪个 namespace 无关。

`New` 里构造了一个 controller-runtime 客户端（第 9 行），读操作走本地缓存：

```go {linenos=inline}
// pkg/networkaware/topologicalsort/topologicalsort.go
func New(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	// ...
	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}

	c, _, err := util.NewClientWithCachedReader(ctx, handle.KubeConfig(), scheme)
	if err != nil {
		return nil, err
	}

	pl := &TopologicalSort{
		Client:     c,
		logger:     logger,
		handle:     handle,
		namespaces: args.Namespaces,
	}
	return pl, nil
}
```

```go {linenos=inline}
// pkg/util/client_util.go
func NewClientWithCachedReader(ctx context.Context, config *rest.Config, scheme *runtime.Scheme) (client.Client, cache.Cache, error) {
	ccache, err := cache.New(config, cache.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, nil, err
	}
	go ccache.Start(ctx)
	if !ccache.WaitForCacheSync(ctx) {
		return nil, nil, fmt.Errorf("failed to sync cache")
	}
	c, err := client.New(config, client.Options{
		Scheme: scheme,
		Cache: &client.CacheOptions{
			Reader: ccache,
		},
	})
	return c, ccache, err
}
```

调度器自己的 `SharedInformerFactory` 只认识内置类型，CRD 只能另起一套缓存。这里的 `cache.New` 启动时还没有任何 informer，controller-runtime（v0.24.1）的 informer 是按类型懒创建的：第一次 `Get` 一个 AppGroup 时才创建 AppGroup 的 informer，并且默认阻塞到它完成首次同步。

调度器的 ServiceAccount 因此需要 `appgroups` 的 get/list/watch 权限。helm chart 的 `rbac.yaml` 只在 `plugins.enabled` 包含 `NetworkOverhead` 时才生成这条规则，单独启用 TopologicalSort 时要自己补上。

### 3.2 Less

前置数据齐了，接下来是比较函数本身。

```go {linenos=inline}
// pkg/networkaware/topologicalsort/topologicalsort.go
// Less is the function used by the activeQ heap algorithm to sort pods.
// 1) Sort Pods based on their AppGroup and corresponding service topology graph.
// 2) Otherwise, follow the strategy of the in-tree QueueSort Plugin (PrioritySort Plugin)
func (ts *TopologicalSort) Less(pInfo1, pInfo2 fwk.QueuedPodInfo) bool {
	p1AppGroup := networkawareutil.GetPodAppGroupLabel(pInfo1.GetPodInfo().GetPod())
	p2AppGroup := networkawareutil.GetPodAppGroupLabel(pInfo2.GetPodInfo().GetPod())
	ctx := context.TODO()
	logger := ts.logger.WithValues("ExtensionPoint", "Less")

	// If pods do not belong to an AppGroup, or being to different AppGroups, follow vanilla QoS Sort
	if p1AppGroup != p2AppGroup || len(p1AppGroup) == 0 {
		logger.V(4).Info("Pods do not belong to the same AppGroup CR", "p1AppGroup", p1AppGroup, "p2AppGroup", p2AppGroup)
		s := &queuesort.PrioritySort{}
		return s.Less(pInfo1, pInfo2)
	}

	// Pods belong to the same appGroup, get the CR
	// ...
	agName := p1AppGroup
	appGroup := ts.findAppGroupTopologicalSort(ctx, agName)

	// Get labels from both pods
	labelsP1 := pInfo1.GetPodInfo().GetPod().GetLabels()
	labelsP2 := pInfo2.GetPodInfo().GetPod().GetLabels()

	// Binary search to find both order index since topology list is ordered by Workload Name
	orderP1 := networkawareutil.FindPodOrder(appGroup.Status.TopologyOrder, labelsP1[agv1alpha.AppGroupSelectorLabel])
	orderP2 := networkawareutil.FindPodOrder(appGroup.Status.TopologyOrder, labelsP2[agv1alpha.AppGroupSelectorLabel])

	// ...
	// Lower is better
	return orderP1 <= orderP2
}
```

分两条路：

- 不在同一个 AppGroup（包括两者都没有 label、只有一个有、属于不同 AppGroup）：直接调用 in-tree 的 `PrioritySort.Less`（第 12–15 行），即优先级降序、入队时间升序。代码注释写的是 `follow vanilla QoS Sort`，KEP 和插件 README 也说回退到 QoS 插件的策略，但实际调用的是 `queuesort.PrioritySort`，**不比较 QoS**。
- 同一个 AppGroup：取出 AppGroup，分别用两个 Pod 的 workload label 在 `TopologyOrder` 里查 index（第 27–29 行），`orderP1 <= orderP2` 时 pInfo1 排前面（第 33 行）。这条路上既不看优先级，也不看入队时间。

取 AppGroup 的函数按 `namespaces` 顺序逐个尝试：

```go {linenos=inline}
// pkg/networkaware/topologicalsort/topologicalsort.go
func (ts *TopologicalSort) findAppGroupTopologicalSort(ctx context.Context, agName string) *agv1alpha.AppGroup {
	logger := ts.logger
	for _, namespace := range ts.namespaces {
		logger.V(6).Info("appGroup CR", "namespace", namespace, "name", agName)
		// AppGroup couldn't be placed in several namespaces simultaneously
		appGroup := &agv1alpha.AppGroup{}
		err := ts.Get(ctx, client.ObjectKey{
			Namespace: namespace,
			Name:      agName,
		}, appGroup)
		if err != nil {
			logger.V(4).Info("Cannot get AppGroup from AppGroupNamespaceLister:", "error", err)
			continue
		}
		if appGroup != nil {
			return appGroup
		}
	}
	return nil
}
```

用单元测试里的数据验证一下，三种情况各走一条分支：

1. `basic` AppGroup 里 p1、p2、p3 的 index 分别是 1、2、3，`Less(p1, p2)` 得 `1 <= 2`，返回 true；
2. Online Boutique 里 p5 的 index 是 7、p1 是 1，`Less(p5, p1)` 得 `7 <= 1`，返回 false；
3. p1 属于 `basic`、p5 属于 `other`，走 PrioritySort：两者优先级都是 0、时间戳都是零值，`Before` 为 false，返回 false。

这三个用例正是 `topologicalsort_test.go` 里 `TestTopologicalSortLess` 的期望值。

### 3.3 没有其它扩展点

TopologicalSort 只实现 `QueueSortPlugin`，没有 `EventsToRegister`，也没有 CycleState。和 QOSSort 一样，它只改变 Pod 在 activeQ 里的位置；通过 v1.36.4 backoffQ 的 `lessBackoffCompletedWithPriority`，它也决定退避到期时间相同的 Pod 之间的先后。

## 四、这个比较函数的性质

实现读完了，回到一个更根本的问题：这个 `Less` 能不能当堆的比较函数用。堆排序要求 `Less` 是严格弱序：非自反（`Less(a, a)` 为 false）、传递、不可比关系也传递。TopologicalSort 有两处不满足。

**第一处是 `<=`（第 33 行）。** 同一个 workload 的两个副本 index 相同，`Less(cart-0, cart-1)` 和 `Less(cart-1, cart-0)` 都返回 true。调度队列的堆基于 `container/heap`，新元素 Push 后上浮的条件是 `h.Less(j, parent)`，对 `<=` 来说相等也成立，于是新加入的 Pod 会越过同 index 的父节点往上走。

结果是同一 workload 内的出队顺序取决于插入顺序和当时的堆形状，既不是 FIFO，也不受优先级影响。一个调度失败、重新入队的副本，可能马上又被排到同组其他副本前面。

**第二处是两套规则的拼接。** 同组 Pod 之间按 index 比，跨组 Pod 之间按优先级比，两套规则放在一个比较函数里，传递性就没了：

- A：`shop/frontend`，index 1，优先级 0
- C：`shop/redis`，index 4，优先级 1000
- B：不属于任何 AppGroup，优先级 500

`Less(A, C)` 为 true（同组，1 <= 4）；`Less(C, B)` 为 true（跨组，1000 > 500）；`Less(B, A)` 为 true（跨组，500 > 0）。A 在 C 前、C 在 B 前、B 又在 A 前，构成一个环。

{{< figure src="/images/posts/sched-plugins/26-less-cycle.svg" caption="同组按 index、跨组按优先级，拼出的 Less 会成环；同 index 的副本互判为「更小」，组内出队顺序取决于插入顺序和堆形状（点击查看大图）" link="/images/posts/sched-plugins/26-less-cycle.svg" >}}

这不会导致 panic 或死循环，二叉堆的上浮、下沉每次只比较父子两个元素，总能结束。代价是「堆顶是全局最小」这一保证失效：堆顶只是相对于它的子节点更小，具体哪个 Pod 先出队取决于插入历史。

混合部署时最直接的表现是：高优先级的普通 Pod 和 AppGroup 里的低优先级 Pod 同时排队，谁先出队不再稳定地由优先级决定。

## 五、边界与坑

**AppGroup 找不到时调度器会 panic。** `findAppGroupTopologicalSort` 在所有 namespace 都查不到时返回 `nil`，`Less` 紧接着访问 `appGroup.Status.TopologyOrder`，空指针解引用。

Pod 带着 `appgroup.diktyo.x-k8s.io` label 却没有对应 AppGroup 的情况并不少见：

- AppGroup 建在了 `args.namespaces` 之外的 namespace；
- 名字拼错；
- 先建了 Deployment 后建 AppGroup；
- AppGroup 被删了而 Pod 还在排队。

只要队列里有两个这样的 Pod 需要比较，panic 就会发生在调度队列的堆操作里，调度器框架没有 recover，进程会退出。同一个查找函数在 NetworkOverhead 里多了 `appGroup.GetUID() != ""` 的判断，但同样会在找不到时返回 `nil`，后果放在第 29 篇里讲。

**status 为空时整组退化。** AppGroup 存在但 controller 没有部署或还没算完，`TopologyOrder` 为空，`FindPodOrder` 对所有 Pod 都返回 `-1`，`-1 <= -1` 恒为 true，整组 Pod 之间互判为「更小」，出队顺序完全由堆的插入历史决定。workload label 写错或缺失时也返回 `-1`，这个 Pod 会排到同组所有正常 Pod 的前面。

**每次比较都读一次缓存，并深拷贝。** `ts.Get` 走 controller-runtime 的缓存读取，默认对返回的对象做 `DeepCopyObject`。activeQ 每次 Push/Pop 要做 O(log n) 次比较，同组 Pod 的每次比较都完整复制一份 AppGroup，包括整个 `spec.workloads` 和 `status.topologyOrder`。

队列里只有几十个 Pod 时无所谓；一个几百个 workload 的 AppGroup 加上几千个排队 Pod，这部分开销会落在持有队列锁的路径上。第一次比较还会触发 AppGroup informer 的懒创建和同步等待。

**namespace 与同名 AppGroup。** 查找只看 `args.namespaces`，与 Pod 的 namespace 无关；两个 namespace 里有同名 AppGroup 时，`namespaces` 列表里靠前的那个生效，另一个 namespace 的 Pod 会拿到别人的拓扑序。

**排序不等于等待。** QueueSort 只决定同时在队列里的 Pod 谁先出队。前一个 Pod 出队后进入调度周期，选好节点就进入异步的绑定周期，下一个 Pod 的调度周期随即开始；如果后者的插件（比如 NetworkOverhead）需要看到前者已经落在哪个节点，而它读的是 informer 里已绑定的 Pod，就未必看得到。

另外，如果依赖方的 Pod 晚几秒才提交，它根本不会和被依赖方同时出现在队列里，拓扑序也就无从发挥。

## 六、发散

### 6.1 拓扑序的方向与 NetworkOverhead 是否对得上

2.2 节末尾留了一句话没展开：Kahn 让调用方先出队，Reverse Kahn 反过来。当时说第六节再回到这一点，现在把它和 NetworkOverhead 对上。

NetworkOverhead 给一个 Pod 打分时，只看**这个 Pod 自己的** `dependencies` 里的 workload 已经调度到了哪里（`GetDependencyList` 按 Pod 的 workload label 取 `spec.workloads[*].dependencies`）。

以 shop 为例：frontend 的放置会参考 cart、catalog 的位置，cart 的放置参考 redis 的位置，redis 和 catalog 没有依赖，NetworkOverhead 对它们给所有节点打同分。

要让 NetworkOverhead 起作用，应当让被调用方先落位，调用方再跟过去，即 redis → cart/catalog → frontend。这正是 ReverseKahn、ReverseTarjan 一类算法给出的顺序。

而示例清单和 KEP 示例默认用的 `KahnSort` 给出的是 frontend 在最前：整组同时提交时，frontend 先出队，此时 cart、catalog 都还没调度，NetworkOverhead 无从参考；cart 再出队，redis 也还没调度。**拓扑序的方向需要按依赖边的语义来选，而不是默认用 KahnSort。**

### 6.2 更强的顺序保证

即使方向选对，拓扑序也只保证「先出队」，不保证「等对方就绪」。需要硬约束时，得换工具。

如果需要「依赖方必须在被依赖方运行之后才开始调度」这样的硬约束，QueueSort 做不到，因为它只调整同时排队的 Pod 的先后。

Kubernetes 原生的工具是 [SchedulingGates](/posts/sched-plugin-01-scheduling-gates/)：Pod 创建时带上 gate，由 controller 在其依赖的 Pod 进入 Running 后移除 gate，Pod 才会进入 activeQ。

这种做法把顺序控制放在 PreEnqueue 阶段，和任何 QueueSort 插件都兼容，也不需要占用进程里唯一的 QueueSort 位置。

### 6.3 如果要改进这份实现

几处改动可以让它更稳妥：

1. 查不到 AppGroup 时回退 PrioritySort，而不是解引用 nil；
2. 把 `<=` 改为 `<`，index 相等时再比较优先级和入队时间，组内就恢复了确定性；
3. 跨组比较时，把同组 Pod 统一视为「优先级相同」也消除不了环。要彻底满足严格弱序，需要给所有 Pod 一个统一的全序键，比如 `(优先级, AppGroup 内 index, 入队时间)`，代价是 AppGroup 内部的拓扑序会被优先级打断。

至于每次比较都读 AppGroup，可以在 Pod 入队时把 index 算好缓存下来，但 QueueSort 插件拿不到入队事件，这需要配合 PreEnqueue 插件或修改框架。

另外，Kubernetes master 已经把 `QueueSortPlugin.Less` 的参数改成 `QueuedEntityInfo`（可能是 Pod，也可能是 PodGroup），TopologicalSort 和 QOSSort 一样，在 scheduler-plugins 升级依赖时需要按 entity 类型分流。

## 七、小结

- TopologicalSort 是 QueueSort 插件，与 QOSSort、Coscheduling 互斥；Pod 通过 `appgroup.diktyo.x-k8s.io` 和 `appgroup.diktyo.x-k8s.io.workload` 两个 label 关联到 AppGroup。
- 拓扑序由外部的 AppGroup controller 计算，存放在 `status.topologyOrder`，数组按 selector 字典序排列，index 才是顺序；插件只做二分查找和比较。
- 同组按 index（`<=`）比较，跨组或无组回退 PrioritySort，不比较 QoS。这个组合不满足严格弱序：同 index 互判为「更小」，跨组可以成环。
- AppGroup 在配置的 namespace 里找不到时，`Less` 会解引用 nil 导致调度器退出；status 为空时整组排序退化。
- 拓扑序的方向要与 NetworkOverhead 的依赖语义配合，被调用方先出队才能让调用方靠近它。

[下一篇](/posts/sched-plugin-27-coscheduling/)是第三个 QueueSort 插件 Coscheduling，它把同一个 PodGroup 的 Pod 排在一起，并在 Permit 阶段实现 all-or-nothing。
