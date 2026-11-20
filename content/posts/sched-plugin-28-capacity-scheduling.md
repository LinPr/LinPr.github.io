---
title: 'scheduler-plugins CapacityScheduling：调度器自己记账的弹性配额'
date: 2026-11-19T12:40:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "ElasticQuota", "preemption"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "CapacityScheduling 用 ElasticQuota 的 min/max 实现租户间的资源借用与回收。本文从源码拆解插件内存账本 ElasticQuotaInfos 的维护方式、PreFilter 的两道配额检查、Reserve/Unreserve 记账、PostFilter 如何借用 in-tree 的 preemption.Evaluator 并改写受害者选择，以及 EventsToRegister 和 controller 写 status.used 的细节与坑。"
---

上一篇的 [Coscheduling](/posts/sched-plugin-27-coscheduling/) 用 Permit 的等待把一组 Pod 捆在一起：要么都上，要么都不上。它解决的是「一组 Pod 之间」的问题。本篇的 `CapacityScheduling` 处理的是「租户之间」的问题：集群资源按 Namespace 分给不同团队，每个团队有一份保底额度，闲着的额度可以借给别人，需要时再收回来。

本文基于 scheduler-plugins 提交 `cfeb3aa`，它依赖 Kubernetes v1.36.4，所以文中引用的 `preemption` 包代码来自 `k8s.io/kubernetes@v1.36.4`；涉及 Kubernetes 主干的地方以 master 提交 `aabdf5a2` 为准，并会单独标出差异。

`CapacityScheduling` 挂在四类扩展点上：`PreFilter`（连同 `PreFilterExtensions` 的 `AddPod`/`RemovePod`）、`PostFilter`、`Reserve`（含 `Unreserve`），另外实现了 `EnqueueExtensions`。它不在任何默认 Profile 里，也没有 feature gate，只能在 scheduler-plugins 编译出的调度器里通过配置启用。仓库自带的配置是这样的：

```yaml
# manifests/capacityscheduling/scheduler-config.yaml
profiles:
- schedulerName: default-scheduler
  plugins:
    multiPoint:
      enabled:
      - name: CapacityScheduling
    postFilter:
      enabled:
      - name: CapacityScheduling
      disabled:
      - name: "*"
```

`multiPoint` 把它注册到它实现的所有扩展点；`postFilter` 一段再把默认的 `DefaultPreemption` 关掉。原因后面会讲：`CapacityScheduling` 自带一套抢占，两套抢占同时开着，`DefaultPreemption` 会按纯优先级去驱逐 Pod，完全不理会配额。插件 README 标注的成熟度是 Beta。

## 一、它要解决什么问题

Kubernetes 自带的 `ResourceQuota` 是准入阶段的硬上限：Namespace 里所有 Pod 的 requests 加起来不能超过 `hard`，超了 Pod 连创建都会被拒。它有两个问题。

第一，额度是死的。团队 A 的额度闲着，团队 B 排着长队，B 也用不了 A 的那一份。第二，记账发生在创建时而不是调度时，一个卡在 Pending 的 Pod 同样占着额度。

`CapacityScheduling` 借用了 Yarn capacity scheduler 的思路，把额度拆成两个数：

```go
// apis/scheduling/v1alpha1/types.go
type ElasticQuotaSpec struct {
	// Min is the set of desired guaranteed limits for each named resource.
	// +optional
	Min v1.ResourceList `json:"min,omitempty" protobuf:"bytes,1,rep,name=min, casttype=ResourceList,castkey=ResourceName"`

	// Max is the set of desired max limits for each named resource. The usage of max is based on the resource configurations of
	// successfully scheduled pods.
	// +optional
	Max v1.ResourceList `json:"max,omitempty" protobuf:"bytes,2,rep,name=max, casttype=ResourceList,castkey=ResourceName"`
}

// ElasticQuotaStatus defines the observed use.
type ElasticQuotaStatus struct {
	// Used is the current observed total usage of the resource in the namespace.
	// +optional
	Used v1.ResourceList `json:"used,omitempty" protobuf:"bytes,1,rep,name=used,casttype=ResourceList,castkey=ResourceName"`
}
```

`min` 是保底：只要你要，就一定能拿回来，哪怕要驱逐别人的 Pod。`max` 是封顶：借别人的也不能超过这个数。`min` 和 `max` 之间的部分，只能在别人有闲置 `min` 的时候借用，并且随时可能被收回。

一个典型的对象：

```yaml
# manifests/capacityscheduling/elasticquota-example.yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: ElasticQuota
metadata:
  name: test
  namespace: test
spec:
  max:
    cpu: 20
    memory: 40Gi
    nvidia.com/gpu: 2
  min:
    cpu: 10
    memory: 20Gi
    nvidia.com/gpu: 1
```

`ElasticQuota` 是 Namespace 级的 CRD，一个 Namespace 只认一个。它约束的是调度，不是创建：Pod 照样能建出来，只是在调度时会因为配额被拦在 PreFilter，然后要么等，要么通过抢占把别人借走的额度收回来。

## 二、三本账

读这个插件，第一件事是分清楚「用量」到底记在哪里。源码里有三处，彼此独立。

{{< figure src="/images/posts/sched-plugins/28-ledgers.svg" link="/images/posts/sched-plugins/28-ledgers.svg" caption="插件内存中的 elasticQuotaInfos 是唯一参与调度判定的账本；CycleState 里的快照只活一个调度周期；status.used 由另一个进程写入，调度器从不读取（点击查看大图）" >}}

### 2.1 插件内存账本：`ElasticQuotaInfos`

```go
// pkg/capacityscheduling/capacity_scheduling.go
type CapacityScheduling struct {
	sync.RWMutex
	logger            klog.Logger
	fh                fwk.Handle
	podLister         corelisters.PodLister
	pdbLister         policylisters.PodDisruptionBudgetLister
	client            client.Client
	elasticQuotaInfos ElasticQuotaInfos
}
```

```go
// pkg/capacityscheduling/elasticquota.go
type ElasticQuotaInfos map[string]*ElasticQuotaInfo

// ElasticQuotaInfo is a wrapper to a ElasticQuota with information.
// Each namespace can only have one ElasticQuota.
type ElasticQuotaInfo struct {
	Namespace string
	pods      sets.Set[string]
	Min       *framework.Resource
	Max       *framework.Resource
	Used      *framework.Resource
}
```

map 的 key 是 Namespace 名，不是 `ElasticQuota` 的名字。`Min`、`Max`、`Used` 都是调度器内部的 `framework.Resource`，也就是 `MilliCPU`、`Memory`、`EphemeralStorage`、`AllowedPodNumber` 四个固定字段加一个 `ScalarResources` map。

`pods` 是这个结构里最关键的字段。它记录已经计入 `Used` 的 Pod 的 `namespace/name`，让「加一个 Pod」和「减一个 Pod」都是幂等的：

```go
// pkg/capacityscheduling/elasticquota.go
func (e *ElasticQuotaInfo) addPodIfNotPresent(pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	if e.pods.Has(key) {
		return nil
	}

	e.pods.Insert(key)
	podRequest := computePodResourceRequest(pod)
	e.reserveResource(*podRequest)

	return nil
}
```

需要幂等，是因为同一个 Pod 会从两条路径进入账本：调度周期里的 `Reserve`，以及绑定成功后 Pod informer 发来的事件。两条路径的先后顺序不确定，有了 `pods` 集合，谁先到都只记一次。

`New` 里注册了两组事件处理函数。`ElasticQuota` 的 informer 来自 controller-runtime 的缓存（`util.NewClientWithCachedReader`），Pod 的 informer 直接复用调度器的 `SharedInformerFactory`，并且只接收已绑定节点的 Pod：

```go
// pkg/capacityscheduling/capacity_scheduling.go
	podInformer := handle.SharedInformerFactory().Core().V1().Pods().Informer()
	podInformer.AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return assignedPod(t)
				// ...
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    c.addPod,
				UpdateFunc: c.updatePod,
				DeleteFunc: c.deletePod,
			},
		},
	)
```

`FilteringResourceEventHandler` 有一个容易忽略的行为：当一次 Update 让对象从「不满足过滤条件」变成「满足」时，它会把这次 Update 转成 Add。Pod 绑定就是这种情况，`spec.nodeName` 从空变成非空，所以绑定后进入账本的是 `addPod`，不是 `updatePod`。

`addPod` 还有一段懒加载：

```go
// pkg/capacityscheduling/capacity_scheduling.go
func (c *CapacityScheduling) addPod(obj interface{}) {
	// ...
	elasticQuotaInfo := c.elasticQuotaInfos[pod.Namespace]
	// If elasticQuotaInfo is nil, try to list ElasticQuotas through elasticQuotaLister
	if elasticQuotaInfo == nil {
		var eqList v1alpha1.ElasticQuotaList
		if err := c.client.List(ctx, &eqList, client.InNamespace(pod.Namespace)); err != nil {
			logger.Error(err, "Failed to get elasticQuota", "elasticQuota", pod.Namespace)
			return
		}

		eqs := eqList.Items
		// If the length of elasticQuotas is 0, return.
		if len(eqs) == 0 {
			return
		}

		if len(eqs) > 0 {
			// only one elasticquota is supported in each namespace
			eq := eqs[0]
			elasticQuotaInfo = newElasticQuotaInfo(eq.Namespace, eq.Spec.Min, eq.Spec.Max, nil)
			c.elasticQuotaInfos[eq.Namespace] = elasticQuotaInfo
		}
	}

	err := elasticQuotaInfo.addPodIfNotPresent(pod)
	// ...
}
```

两个 informer 启动时谁先同步完是不确定的。如果 Pod 的 Add 先到，账本里还没有这个 Namespace 的条目，就主动去 `ElasticQuota` 缓存里查一次，查到就建条目。对应地，`addElasticQuota` 发现条目已存在时直接返回，不覆盖：

```go
// pkg/capacityscheduling/capacity_scheduling.go
func (c *CapacityScheduling) addElasticQuota(obj interface{}) {
	eq := obj.(*v1alpha1.ElasticQuota)
	oldElasticQuotaInfo := c.elasticQuotaInfos[eq.Namespace]
	if oldElasticQuotaInfo != nil {
		return
	}

	elasticQuotaInfo := newElasticQuotaInfo(eq.Namespace, eq.Spec.Min, eq.Spec.Max, nil)

	c.Lock()
	defer c.Unlock()
	c.elasticQuotaInfos[eq.Namespace] = elasticQuotaInfo
}

func (c *CapacityScheduling) updateElasticQuota(oldObj, newObj interface{}) {
	// ...
	oldEQInfo := c.elasticQuotaInfos[oldEQ.Namespace]
	if oldEQInfo != nil {
		newEQInfo.pods = oldEQInfo.pods
		newEQInfo.Used = oldEQInfo.Used
	}
	c.elasticQuotaInfos[newEQ.Namespace] = newEQInfo
}
```

`updateElasticQuota` 只换 `Min`、`Max`，把 `pods` 和 `Used` 原样搬到新对象上。所以调整配额不会丢失已有用量。删除则是整条 `delete`，没有任何残留。

Pod 离开账本有两条路：`deletePod`，以及 `updatePod` 里新状态既不是 `Running` 也不是 `Pending` 的情况。实际上第二条很少走到。调度器的 Pod informer 建立时带了字段选择器：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/scheduler.go
func newPodInformer(cs clientset.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	selector := fmt.Sprintf("status.phase!=%v,status.phase!=%v", v1.PodSucceeded, v1.PodFailed)
	// ...
}
```

Pod 进入 `Succeeded` 或 `Failed` 时就不再匹配这个选择器，informer 看到的是一次删除。插件复用的正是这个 informer，所以「跑完的 Job Pod 释放配额」走的是 `deletePod`。

### 2.2 CycleState 里的快照

PreFilter 一开始就把整本账深拷贝一份写进 CycleState：

```go
// pkg/capacityscheduling/capacity_scheduling.go
func (c *CapacityScheduling) snapshotElasticQuota() *ElasticQuotaSnapshotState {
	c.RLock()
	defer c.RUnlock()

	elasticQuotaInfosDeepCopy := c.elasticQuotaInfos.clone()
	return &ElasticQuotaSnapshotState{
		elasticQuotaInfos: elasticQuotaInfosDeepCopy,
	}
}
```

CycleState 里一共两个 key：

```go
// pkg/capacityscheduling/capacity_scheduling.go
	preFilterStateKey       = "PreFilter" + Name
	ElasticQuotaSnapshotKey = "ElasticQuotaSnapshot"
```

两者的 `Clone` 实现截然不同：

```go
// pkg/capacityscheduling/capacity_scheduling.go
// Clone the preFilter state.
func (s *PreFilterState) Clone() fwk.StateData {
	return s
}

// Clone the ElasticQuotaSnapshot state.
func (s *ElasticQuotaSnapshotState) Clone() fwk.StateData {
	return &ElasticQuotaSnapshotState{
		elasticQuotaInfos: s.elasticQuotaInfos.clone(),
	}
}
```

`PreFilterState` 写入后不再被修改，返回自身就够了。快照则不同：抢占模拟时，`Evaluator.DryRunPreemption` 会为每个候选节点调用一次 `state.Clone()`，然后在副本上反复「移除受害者、加回受害者」。每一次移除和加回都会经过 `RunPreFilterExtensionRemovePod`/`AddPod` 回调到插件，改动快照里对应 Namespace 的 `Used`。不同节点的模拟是并行的，快照必须深拷贝，否则节点之间会相互污染。

为什么不直接在 PreFilter 里读插件的账本，而要拍一份快照？因为抢占模拟需要一份「可以随便改的账」。只读检查用快照还是用原账本差别不大，但 `AddPod`/`RemovePod` 必须改一份不影响真实记账的副本。

快照的代价是每个调度周期都复制所有 `ElasticQuotaInfo`，包括每个 `pods` 集合。配额 Namespace 里的 Pod 越多，每次 PreFilter 的复制量越大。源码在 PreFilter 开头留了一个 TODO，提到可以只复制有变化的部分，目前还没有实现。

### 2.3 `status.used`：另一个进程写的「展示用」数据

`ElasticQuota` 的 `status.used` 不是调度器写的，而是 scheduler-plugins 的 controller 进程（`cmd/controller`）里的 `ElasticQuotaReconciler`：

```go
// pkg/controllers/elasticquota_controller.go
func (r *ElasticQuotaReconciler) computeElasticQuotaUsed(ctx context.Context, namespace string, eq *schedv1alpha1.ElasticQuota) (v1.ResourceList, error) {
	used := newZeroUsed(eq)
	podList := &v1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	for _, p := range podList.Items {
		if p.Status.Phase == v1.PodRunning {
			used = quota.Add(used, computePodResourceRequest(&p))
		}
	}
	return used, nil
}
```

它只统计 `Running` 的 Pod。插件账本统计的是「已经绑定节点、尚未终止」的 Pod，还包括 `Reserve` 之后、绑定之前的 Pod。所以一个 Pod 从被调度到容器真正跑起来的这段时间里（拉镜像可能要几分钟），`kubectl get eq` 看到的用量比调度器判定时用的要少。

更关键的是，调度器根本不读 `status.used`：`addElasticQuota`、`updateElasticQuota`、`addPod` 构造 `ElasticQuotaInfo` 时传入的 `used` 都是 `nil`。不部署 controller，调度完全不受影响，只是看不到 `Used` 列。

controller 计算请求量的函数还有一个实现问题：

```go
// pkg/controllers/elasticquota_controller.go
	// If Overhead is being utilized, add to the total requests for the pod
	if pod.Spec.Overhead != nil {
		quota.Add(result, pod.Spec.Overhead)
	}
```

`k8s.io/apiserver/pkg/quota/v1` 的 `Add` 不修改入参，而是返回一个新的 `ResourceList`。这里丢掉了返回值，所以使用 RuntimeClass overhead 的 Pod，overhead 不会出现在 `status.used` 里。插件自己的 `computePodResourceRequest` 用的是 `result.Add(pod.Spec.Overhead)`，会把 overhead 计入。用了 Kata 之类带 overhead 的 RuntimeClass 时，两本账会出现稳定的差值。

## 三、比较函数：每个维度都要比

配额检查最终都落到一个函数上：

```go
// pkg/capacityscheduling/elasticquota.go
func cmp2(x1, x2, y *framework.Resource, bound int64) bool {
	if x1.MilliCPU+x2.MilliCPU > y.MilliCPU {
		return true
	}

	if x1.Memory+x2.Memory > y.Memory {
		return true
	}

	if x1.EphemeralStorage+x2.EphemeralStorage > y.EphemeralStorage {
		return true
	}

	if x1.AllowedPodNumber+x2.AllowedPodNumber > y.AllowedPodNumber {
		return true
	}

	for rName, rQuant := range x1.ScalarResources {
		yQuant := bound
		if yq, ok := y.ScalarResources[rName]; ok {
			yQuant = yq
		}
		if rQuant+x2.ScalarResources[rName] > yQuant {
			return true
		}
	}

	return false
}
```

语义是「`x1 + x2` 在任一维度上超过 `y`，就返回 true」。四个固定维度总是参与比较；扩展资源（GPU 之类）只比较 `x1` 里出现的，`y` 里没有定义时用 `bound` 兜底。调用方传入的 `bound` 是：

```go
// pkg/capacityscheduling/elasticquota.go
const (
	UpperBoundOfMax = math.MaxInt64
	LowerBoundOfMin = 0
)
```

也就是说，`max` 没写的扩展资源视为不设上限，`min` 没写的扩展资源视为 0。

固定维度的情况不一样。`newElasticQuotaInfo` 只在 `min` 或 `max` **整体为 nil** 时才填默认值：

```go
// pkg/capacityscheduling/elasticquota.go
func newElasticQuotaInfo(namespace string, min, max, used v1.ResourceList) *ElasticQuotaInfo {
	if min == nil {
		min = makeResourceListForBound(LowerBoundOfMin)
	}
	if max == nil {
		max = makeResourceListForBound(UpperBoundOfMax)
	}
	// ...
		Min:       framework.NewResource(min),
		Max:       framework.NewResource(max),
	// ...
}
```

只要 `max` 写了任何一项，比如只写了 `cpu: 6`，`framework.NewResource` 生成的 `Max.Memory` 就是 0。此时一个请求了 1Gi 内存的 Pod，`0 + 1Gi > 0` 成立，PreFilter 会以「超过 Max」拒绝它。`min` 同理：只写了 CPU 的 `min`，任何请求内存的 Pod 都会让该 quota 被判定为「超过 min」。

仓库 README 的演示只配了 CPU，演示用的 nginx 也只请求了 CPU，所以不会踩到。实际使用时，**`min` 和 `max` 应该把 Pod 会请求的固定维度（至少 cpu 和 memory）都写全**。

`AllowedPodNumber` 是个例外：容器的 requests 里不会出现 `pods`，Pod 的请求量在这个维度上永远是 0，`0 > 0` 不成立，所以不写也没关系。

## 四、PreFilter：两道检查

```go
// pkg/capacityscheduling/capacity_scheduling.go
// PreFilter performs the following validations.
// 1. Check if the (pod.request + eq.allocated) is less than eq.max.
// 2. Check if the sum(eq's usage) > sum(eq's min).
func (c *CapacityScheduling) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// ...
	snapshotElasticQuota := c.snapshotElasticQuota()
	podReq := computePodResourceRequest(pod)

	state.Write(ElasticQuotaSnapshotKey, snapshotElasticQuota)

	elasticQuotaInfos := snapshotElasticQuota.elasticQuotaInfos
	eq := snapshotElasticQuota.elasticQuotaInfos[pod.Namespace]
	if eq == nil {
		preFilterState := &PreFilterState{
			podReq: *podReq,
		}
		state.Write(preFilterStateKey, preFilterState)
		return nil, fwk.NewStatus(fwk.Success)
	}
	// ...
```

没有 `ElasticQuota` 的 Namespace 直接放行，返回 `Success`，但仍然写入了快照和 `podReq`，因为后面的抢占逻辑需要它们。

### 4.1 nominated Pod 的两种求和

接下来遍历所有节点上的 nominated Pod，也就是已经抢占成功、等待受害者退出的 Pod：

```go
// pkg/capacityscheduling/capacity_scheduling.go
	for _, node := range nodeList {
		nominatedPods := c.fh.NominatedPodsForNode(node.Node().Name)
		for _, p := range nominatedPods {
			if p.GetPod().UID == pod.UID {
				continue
			}
			ns := p.GetPod().Namespace
			info := c.elasticQuotaInfos[ns]
			if info != nil {
				pResourceRequest := util.ResourceList(computePodResourceRequest(p.GetPod()))
				// If they are subject to the same quota(namespace) and p is more important than pod,
				// p will be added to the nominatedResource and totalNominatedResource.
				// If they aren't subject to the same quota(namespace) and the usage of quota(p's namespace) does not exceed min,
				// p will be added to the totalNominatedResource.
				if ns == pod.Namespace && corev1helpers.PodPriority(p.GetPod()) >= corev1helpers.PodPriority(pod) {
					nominatedPodsReqInEQWithPodReq.Add(pResourceRequest)
					nominatedPodsReqWithPodReq.Add(pResourceRequest)
				} else if ns != pod.Namespace && !info.usedOverMin() {
					nominatedPodsReqWithPodReq.Add(pResourceRequest)
				}
			}
		}
	}

	nominatedPodsReqInEQWithPodReq.Add(util.ResourceList(podReq))
	nominatedPodsReqWithPodReq.Add(util.ResourceList(podReq))
```

得到两个和：

- `nominatedPodsReqInEQWithPodReq`：本 Pod 请求 + 同 Namespace 中优先级不低于本 Pod 的 nominated Pod；
- `nominatedPodsReqWithPodReq`：上面那部分 + 其他 Namespace 中、所属 quota 尚未超过 `min` 的 nominated Pod。

nominated Pod 还没有经过 `Reserve`，账本里没有它们。如果不把它们算进来，就会出现这样的情况：B 抢占了 A 的 Pod 准备回收自己的 `min`，A 的 Pod 还在优雅退出，此时 C 的 Pod 进来，看到账面上还有余量，把这份额度又占了。

第二种求和只计入「所属 quota 未超 `min`」的外部 nominated Pod，原因是：只有这些 Pod 有权利继续拿资源，它们正在回收自己的保底额度；已经超过 `min` 的那些，自己也只是借用者，不需要为它们预留。

这段循环查的是 `c.elasticQuotaInfos`，即插件的实时账本，没有使用刚拍的快照，也没有加读锁。两者在绝大多数时候是一致的，只是一个小小的不严谨之处。

### 4.2 两道检查

```go
// pkg/capacityscheduling/capacity_scheduling.go
	if eq.usedOverMaxWith(nominatedPodsReqInEQWithPodReq) {
		return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("Pod %v/%v is rejected in PreFilter because ElasticQuota %v is more than Max", pod.Namespace, pod.Name, eq.Namespace))
	}

	if elasticQuotaInfos.aggregatedUsedOverMinWith(*nominatedPodsReqWithPodReq) {
		return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("Pod %v/%v is rejected in PreFilter because total ElasticQuota used is more than min", pod.Namespace, pod.Name))
	}

	return nil, fwk.NewStatus(fwk.Success, "")
```

写成不等式：

```text
检查一：Used(ns)  + nominatedInEQ + req > Max(ns)   → Unschedulable
检查二：ΣUsed(全部) + nominatedAll + req > ΣMin(全部) → Unschedulable
```

`aggregatedUsedOverMinWith` 把所有 quota 的 `Used` 和 `Min` 各自加总再比较：

```go
// pkg/capacityscheduling/elasticquota.go
func (e ElasticQuotaInfos) aggregatedUsedOverMinWith(podRequest framework.Resource) bool {
	used := framework.NewResource(nil)
	min := framework.NewResource(nil)

	for _, elasticQuotaInfo := range e {
		used.Add(util.ResourceList(elasticQuotaInfo.Used))
		min.Add(util.ResourceList(elasticQuotaInfo.Min))
	}

	used.Add(util.ResourceList(&podRequest))
	return cmp(used, min, LowerBoundOfMin)
}
```

第一道是常规的单租户上限。第二道决定了这个插件的整体形态：**所有配额内 Pod 的用量总和，永远不能超过所有 `min` 之和**。「借用」借的只是其他租户暂时没用上的 `min`，借不到 `min` 总池子之外的资源。假如所有 `min` 加起来只有集群容量的一半，另一半对配额内的 Pod 不可见，即便 `max` 写得很大也没用；只有没有 `ElasticQuota` 的 Namespace 能用到这部分。

所以使用这个插件时，`ΣMin` 应该接近集群可分配资源。这也是 README 演示特意让两个 `min` 之和等于集群容量的原因。KEP 的「已知限制」一节又建议把 `ΣMin` 设得略小于集群容量，以缓解碎片问题（第八节会讲）。两者的折中是：`ΣMin` 贴近集群可分配量，留出少量余量。

两道检查都返回 `Unschedulable`，而不是 `UnschedulableAndUnresolvable`。这个选择决定了后面的流程。在 v1.36.4 的框架里：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/framework/runtime/framework.go
		if !s.IsSuccess() {
			s.SetPlugin(pl.Name())
			if s.Code() == fwk.UnschedulableAndUnresolvable {
				// In this case, the preemption shouldn't happen in this scheduling cycle.
				// So, no need to execute all PreFilter.
				return nil, s, nil
			}
			if s.Code() == fwk.Unschedulable {
				// In this case, the preemption should happen later in this scheduling cycle.
				// So we need to execute all PreFilter.
				// https://github.com/kubernetes/kubernetes/issues/119770
				returnStatus = s
				continue
			}
			// ...
```

`Unschedulable` 时其余 PreFilter 插件照常执行，它们的状态都会写进 CycleState。抢占模拟需要运行 Filter，而 Filter 依赖 PreFilter 写的状态，这一步必不可少。随后 `schedule_one.go` 把同一个状态设给所有节点：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/schedule_one.go
	preRes, s, unscheduledPlugins := schedFramework.RunPreFilterPlugins(ctx, state, pod)
	diagnosis.UnschedulablePlugins = unscheduledPlugins
	if !s.IsSuccess() {
		if !s.IsRejected() {
			return nil, diagnosis, "", s.AsError()
		}
		// All nodes in NodeToStatus will have the same status so that they can be handled in the preemption.
		diagnosis.NodeToStatus.SetAbsentNodesStatus(s)
		// ...
```

所有节点都是 `Unschedulable`，于是全部成为抢占候选。

{{< figure src="/images/posts/sched-plugins/28-decision.svg" link="/images/posts/sched-plugins/28-decision.svg" caption="左：PreFilter 的两道检查，失败时所有节点都成为抢占候选；右：PostFilter 中按抢占者状态选择候选受害者，再用 Filter 和配额检查逐个赦免（点击查看大图）" >}}

## 五、Reserve / Unreserve：调度周期内的记账

```go
// pkg/capacityscheduling/capacity_scheduling.go
func (c *CapacityScheduling) Reserve(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	c.Lock()
	defer c.Unlock()
	// ...
	elasticQuotaInfo := c.elasticQuotaInfos[pod.Namespace]
	if elasticQuotaInfo != nil {
		err := elasticQuotaInfo.addPodIfNotPresent(pod)
		if err != nil {
			logger.Error(err, "Failed to add Pod to its associated elasticQuota", "pod", klog.KObj(pod))
			return fwk.NewStatus(fwk.Error, err.Error())
		}
	}
	return fwk.NewStatus(fwk.Success, "")
}

func (c *CapacityScheduling) Unreserve(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) {
	c.Lock()
	defer c.Unlock()
	// ...
	elasticQuotaInfo := c.elasticQuotaInfos[pod.Namespace]
	if elasticQuotaInfo != nil {
		err := elasticQuotaInfo.deletePodIfPresent(pod)
		// ...
	}
}
```

`Reserve` 改的是插件的实时账本，不是快照。调度器的调度周期是串行的，但绑定周期是异步的：Pod 1 在 `Reserve` 之后进入异步绑定，调度器马上开始调度 Pod 2。如果不在 `Reserve` 时记账，Pod 2 的 PreFilter 拍快照时看不到 Pod 1，两者会共用同一份配额。这和调度器缓存的 assume 机制同理：在 API 层面确认之前，先在本地「假定」它已经成功。

绑定失败、Permit 拒绝或超时时，框架调用 `Unreserve`，账本回滚。之后如果绑定成功，informer 的 Add 事件再到，`pods` 集合里已有这个 key，不会重复记账。

## 六、PostFilter：借用 Evaluator，换掉受害者选择

### 6.1 Evaluator 与 `preemption.Interface`

in-tree 的抢占流程分两层。`preemption.Evaluator` 负责骨架：检查抢占资格、找候选节点、并行模拟、调用 extender、选最优节点、执行驱逐。随抢占策略变化的部分，抽成了一个接口：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/framework/preemption/preemption.go
type Interface interface {
	// GetOffsetAndNumCandidates chooses a random offset and calculates the number of candidates that should be
	// shortlisted for dry running preemption.
	GetOffsetAndNumCandidates(nodes int32) (int32, int32)
	// CandidatesToVictimsMap builds a map from the target node to a list of to-be-preempted Pods and the number of PDB violation.
	CandidatesToVictimsMap(candidates []Candidate) map[string]*extenderv1.Victims
	// PodEligibleToPreemptOthers returns one bool and one string. The bool indicates whether this pod should be considered for
	// preempting other pods or not. The string includes the reason if this pod isn't eligible.
	PodEligibleToPreemptOthers(ctx context.Context, pod *v1.Pod, nominatedNodeStatus *fwk.Status) (bool, string)
	// SelectVictimsOnNode finds minimum set of pods on the given node that should be preempted in order to make enough room
	// for "pod" to be scheduled.
	// Note that both `state` and `nodeInfo` are deep copied.
	SelectVictimsOnNode(ctx context.Context, state fwk.CycleState,
		pod *v1.Pod, nodeInfo fwk.NodeInfo, pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, int, *fwk.Status)
	// OrderedScoreFuncs returns a list of ordered score functions to select preferable node where victims will be preempted.
	// The ordered score functions will be processed one by one iff we find more than one node with the highest score.
	// Default score functions will be processed if nil returned here for backwards-compatibility.
	OrderedScoreFuncs(ctx context.Context, nodesToVictims map[string]*extenderv1.Victims) []func(node string) int64
}
```

`DefaultPreemption` 是这个接口的一个实现（详见[DefaultPreemption 篇](/posts/sched-plugin-18-default-preemption/)）。`CapacityScheduling` 定义了一个内部类型 `preemptor` 作为另一个实现，在 PostFilter 里临时组装一个 Evaluator：

```go
// pkg/capacityscheduling/capacity_scheduling.go
func (c *CapacityScheduling) PostFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, m fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	defer func() {
		metrics.PreemptionAttempts.Inc()
	}()

	pe := preemption.NewEvaluator(
		c.Name(),
		c.fh,
		&preemptor{
			logger: c.logger,
			fh:     c.fh,
			state:  state,
		},
		preemption.NewExecutor(c.fh, feature.Features{}), // no feature enabled, i.e. preemption is synchronous
	)

	return pe.Preempt(ctx, state, pod, m)
}
```

`feature.Features{}` 是空的，`EnableAsyncPreemption` 为 false，所以驱逐受害者的 API 调用在 PostFilter 里同步完成。`DefaultPreemption` 在 `SchedulerAsyncPreemption` 开启时（1.33 起默认开启）会把这一步放进 goroutine，并配合 `PreEnqueue` 挡住正在抢占的 Pod；`CapacityScheduling` 没有实现 `PreEnqueue`，只能走同步路径。

五个方法里，三个是简单实现：

```go
// pkg/capacityscheduling/capacity_scheduling.go
func (p *preemptor) OrderedScoreFuncs(ctx context.Context, nodesToVictims map[string]*extenderv1.Victims) []func(node string) int64 {
	return nil
}

func (p *preemptor) GetOffsetAndNumCandidates(n int32) (int32, int32) {
	return 0, n
}
```

`OrderedScoreFuncs` 返回 nil，选节点时走 Evaluator 的默认规则：PDB 违反数最少、最高受害者优先级最低、受害者优先级之和最小、受害者数量最少、最高优先级受害者启动最晚。`GetOffsetAndNumCandidates` 返回 `(0, n)`：从第一个节点开始，要求的候选数等于全部潜在节点数。`DefaultPreemption` 默认只要 `max(10% 节点数, 100)` 个候选，并从随机偏移开始，找够就停；`CapacityScheduling` 则会把所有节点都模拟一遍。大集群里，这让一次抢占的耗时随节点数线性增长。

### 6.2 PodEligibleToPreemptOthers：别重复抢

这个方法回答：抢占者上一轮已经抢过一次，受害者还在退出，这一轮还要不要再抢？`DefaultPreemption` 的答案是：提名节点上有正在被抢占而退出的低优先级 Pod，就不再抢。`CapacityScheduling` 把配额考虑了进去：

```go
// pkg/capacityscheduling/capacity_scheduling.go
		podPriority := corev1helpers.PodPriority(pod)
		preemptorEQInfo, preemptorWithEQ := elasticQuotaSnapshotState.elasticQuotaInfos[pod.Namespace]
		if preemptorWithEQ {
			moreThanMinWithPreemptor := preemptorEQInfo.usedOverMinWith(&preFilterState.nominatedPodsReqInEQWithPodReq)
			for _, p := range nodeInfo.GetPods() {
				// Checking terminating pods
				if p.GetPod().DeletionTimestamp != nil {
					eqInfo, withEQ := elasticQuotaSnapshotState.elasticQuotaInfos[p.GetPod().Namespace]
					if !withEQ {
						continue
					}
					if p.GetPod().Namespace == pod.Namespace && corev1helpers.PodPriority(p.GetPod()) < podPriority {
						// ...
						return false, "not eligible due to a terminating pod on the nominated node."
					} else if p.GetPod().Namespace != pod.Namespace && !moreThanMinWithPreemptor && eqInfo.usedOverMin() {
						// ...
						return false, "not eligible due to a terminating pod on the nominated node."
					}
				}
			}
		} else {
			// ...
		}
```

「退出中的 Pod 会腾出空间给我」这个判断，在配额语义下要和受害者选择规则一致：只有那些按规则本来就可以被我驱逐的 Pod 正在退出，才说明上一轮抢占还在生效。规则见下一节。省略的 `else` 分支处理没有 EQ 的抢占者，只看同样没有 EQ、优先级更低的退出中 Pod。这里判断「退出中」只看 `DeletionTimestamp`，不区分是被抢占还是被人删除；v1.36.4 的 `DefaultPreemption` 已经改为检查 `DisruptionTarget` condition 的 reason 是否为 `PreemptionByScheduler`，`CapacityScheduling` 这份逻辑还是旧写法。

### 6.3 SelectVictimsOnNode：受害者由「是否超过 min」决定

这是插件改写的核心。函数在一个节点的副本上运行，先按抢占者的状态挑出候选受害者：

```go
// pkg/capacityscheduling/capacity_scheduling.go
	var potentialVictims []fwk.PodInfo
	if preemptorWithElasticQuota {
		nominatedPodsReqInEQWithPodReq = preFilterState.nominatedPodsReqInEQWithPodReq
		nominatedPodsReqWithPodReq = preFilterState.nominatedPodsReqWithPodReq
		moreThanMinWithPreemptor := preemptorElasticQuotaInfo.usedOverMinWith(&nominatedPodsReqInEQWithPodReq)
		for _, p := range nodeInfo.GetPods() {
			eqInfo, withEQ := elasticQuotaInfos[p.GetPod().Namespace]
			if !withEQ {
				continue
			}

			if moreThanMinWithPreemptor {
				// If Preemptor.Request + Quota.Used > Quota.Min:
				// It means that its guaranteed isn't borrowed by other
				// quotas. So that we will select the pods which subject to the
				// same quota(namespace) with the lower priority than the
				// preemptor's priority as potential victims in a node.
				if p.GetPod().Namespace == pod.Namespace && corev1helpers.PodPriority(p.GetPod()) < podPriority {
					potentialVictims = append(potentialVictims, p)
				}

			} else {
				// If Preemptor.Request + Quota.allocated <= Quota.min: It
				// means that its min(guaranteed) resource is used or
				// `borrowed` by other Quota. Potential victims in a node
				// will be chosen from Quotas that allocates more resources
				// than its min, i.e., borrowing resources from other
				// Quotas.
				if p.GetPod().Namespace != pod.Namespace && eqInfo.usedOverMin() {
					potentialVictims = append(potentialVictims, p)
				}
			}
		}
	} else {
		for _, p := range nodeInfo.GetPods() {
			_, withEQ := elasticQuotaInfos[p.GetPod().Namespace]
			if withEQ {
				continue
			}
			if corev1helpers.PodPriority(p.GetPod()) < podPriority {
				potentialVictims = append(potentialVictims, p)
			}
		}
	}
```

三种情况：

| 抢占者 | 判定 | 候选受害者 |
| --- | --- | --- |
| 有 EQ，`Used + req > Min` | 自己已在借用 | 同 Namespace、优先级更低的 Pod |
| 有 EQ，`Used + req ≤ Min` | 自己的 `min` 被别人占着 | 其他 Namespace 中、所属 quota `Used > Min` 的 Pod，**不比较优先级** |
| 没有 EQ | 不参与配额 | 同样没有 EQ 的 Namespace 中、优先级更低的 Pod |

第二行是弹性配额的语义所在：租户在自己 `min` 以内申请资源，是在拿回本属于自己的东西，所以不看优先级，正在借用的一方哪怕优先级更高也要让出来。第一行则是 KEP 里强调的「受限的单 Namespace 抢占」：已经在借了，就只能和自己人比优先级，不能为了多借而驱逐别的租户。

注意判定用的是 `usedOverMinWith`，任一维度超过 `min` 就算「超」。一个 quota 的 CPU 远低于 `min`、GPU 刚好用满 `min`，再来一个请求 GPU 的 Pod，它会被判为「已在借用」，只能抢同 Namespace 的 Pod，即便它的 CPU 额度被别人借走了。多维资源下，「借」与「还」不是按维度分别计算的。

三组之间互不跨越：有 EQ 的抢占者永远不会驱逐无 EQ 的 Pod，无 EQ 的抢占者也碰不到有 EQ 的 Pod。混部时，没有配额的工作负载反而处在一个受保护的位置。

接下来的流程和 `DefaultPreemption` 同构，只是多了配额检查：

```go
// pkg/capacityscheduling/capacity_scheduling.go
	// No potential victims are found, and so we don't need to evaluate the node again since its state didn't change.
	if len(potentialVictims) == 0 {
		message := fmt.Sprintf("No victims found on node %v for preemptor pod %v", nodeInfo.Node().Name, pod.Name)
		return nil, 0, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, message)
	}

	// If the new pod does not fit after removing all the lower priority pods,
	// we are almost done and this node is not suitable for preemption. ...
	if s := p.fh.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo); !s.IsSuccess() {
		return nil, 0, s
	}

	// If the quota.used + pod.request > quota.max or sum(quotas.used) + pod.request > sum(quotas.min)
	// after removing all the lower priority pods,
	// we are almost done and this node is not suitable for preemption.
	if preemptorWithElasticQuota {
		if preemptorElasticQuotaInfo.usedOverMaxWith(&podReq) ||
			elasticQuotaInfos.aggregatedUsedOverMinWith(podReq) {
			return nil, 0, fwk.NewStatus(fwk.Unschedulable, "global quota max exceeded")
		}
	}
```

`removePod` 在从 `nodeInfo` 副本里删掉 Pod 之后，还会调用 `RunPreFilterExtensionRemovePod`，其中就包括插件自己的 `RemovePod`，它会从快照副本里减掉受害者的用量。所以「全部移除之后再做配额检查」时，`elasticQuotaInfos` 里的 `Used` 已经扣除了这些受害者。`CapacityScheduling` 本身没有 Filter，配额在这里只能显式检查，`RunFilterPluginsWithNominatedPods` 只负责节点层面能否放下。

最后是赦免，把候选受害者按优先级从高到低逐个加回，先处理加回会违反 PDB 的，再处理其余：

```go
// pkg/capacityscheduling/capacity_scheduling.go
	reprievePod := func(pi fwk.PodInfo) (bool, error) {
		if err := addPod(pi); err != nil {
			return false, err
		}
		s := p.fh.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
		fits := s.IsSuccess()
		if !fits {
			if err := removePod(pi); err != nil {
				return false, err
			}
			victims = append(victims, pi.GetPod())
			// ...
		}

		if preemptorWithElasticQuota && (preemptorElasticQuotaInfo.usedOverMaxWith(&nominatedPodsReqInEQWithPodReq) || elasticQuotaInfos.aggregatedUsedOverMinWith(nominatedPodsReqWithPodReq)) {
			if err := removePod(pi); err != nil {
				return false, err
			}
			victims = append(victims, pi.GetPod())
			// ...
		}

		return fits, nil
	}
```

加回后 Filter 不通过，这个 Pod 必须走；Filter 通过但配额检查不通过，也必须走。两者都通过，才赦免。和 `DefaultPreemption` 相比，多出来的正是第二个 `if`。

第二个 `if` 里用的是含 nominated Pod 的两个和，而赦免前的那次检查只用了 `podReq`。这两个检查口径不一致会导致一个边角情况：如果其他 nominated Pod 的请求量使得「移除所有候选后」配额仍然超限，那么第一个被尝试赦免的 Pod 在 Filter 不通过时已经被 `removePod` 一次，接着配额检查又不通过，会对同一个 Pod 再 `removePod` 一次。`NodeInfo.RemovePod` 找不到 Pod 会返回 `no corresponding pod ... in pods of node` 错误，这个节点的模拟会以 `Error` 结束。这种组合在实际中并不常见，但排查抢占报错时值得知道这条路径。

### 6.4 算例：借用与回收

用 README 的演示场景把流程走一遍。两台节点各 4 CPU，共 8 CPU；quota A、B 都是 `min: cpu 4`、`max: cpu 6`，`ΣMin = 8`；所有 Pod 请求 2 CPU、只请求 CPU、优先级相同。

{{< figure src="/images/posts/sched-plugins/28-borrow-reclaim.svg" link="/images/posts/sched-plugins/28-borrow-reclaim.svg" caption="A 先借走 B 的闲置 min，B 在自己的 min 以内申请资源时，触发跨 Namespace 抢占，把 A 压回 min（点击查看大图）" >}}

**① A 借用。** A 的 Deployment 有 4 个副本。a1、a2 让 A 用到 4，正好等于 `min`。a3：检查一 `4 + 2 = 6 ≤ 6`，检查二 `ΣUsed 4 + 2 = 6 ≤ ΣMin 8`，通过，A 开始借用 B 的 `min`。a4：检查一 `6 + 2 = 8 > 6`，`Unschedulable`。进入 PostFilter：`usedOverMinWith` 判定 `6 + 2 > 4`，A 处于借用状态，只能抢同 Namespace 内优先级更低的 Pod，没有，每个节点都返回 `UnschedulableAndUnresolvable`，a4 留在 Pending。

**② B 用掉剩余空间。** b1：检查一 `0 + 2 ≤ 6`，检查二 `6 + 2 = 8 ≤ 8`，通过，调度到 node2。此时集群 8 CPU 全部用完，`ΣUsed` 也等于 `ΣMin`。

**③ b2 触发回收。** b2：检查一 `2 + 2 = 4 ≤ 6` 通过；检查二 `8 + 2 = 10 > 8`，`Unschedulable`，所有节点进入抢占候选。B 的判定：`2 + 2 = 4`，不大于 `min` 4，处于「`min` 被占用」状态，候选受害者是其他 Namespace 中 `Used > Min` 的 Pod，A 的 6 大于 4，A 的所有 Pod 都是候选。

- node1 上有 a1、a2。全部移除后，快照里 A 降到 2，b2 放得下，配额 `2 + 2 + 2 = 6 ≤ 8` 通过。赦免 a1（同优先级时启动更早的更重要，先加回）：节点 `2 + 2 = 4 ≤ 4` 放得下，配额 `4 + 2 + 2 = 8 ≤ 8` 通过，a1 留下。赦免 a2：节点 `6 > 4` 放不下，a2 是受害者。
- node2 上有 a3、b1。候选只有 a3（b1 是自己人）。移除后 b2 放得下；加回 a3 放不下，a3 是受害者。

两个候选节点各有 1 个同优先级的受害者，前四条默认规则都打平，最后按「最高优先级受害者启动时间最晚」选出一个。假设选中 node2：a3 被驱逐，b2 被提名到 node2。

**④ 回到各自的 min。** a3 退出后 A 回到 4，b2 绑定后 B 也是 4，`ΣUsed = 8`。此时 A 的 Pending Pod 有两个：原来的 a4，以及 Deployment 为补 a3 新建的 a3'。它们的检查二都是 `8 + 2 > 8`；PostFilter 中 A 的判定是 `4 + 2 > 4`，只能抢同 Namespace，没有受害者，继续等待。

这个算例还能说明 PreFilter 为什么要计入 nominated Pod。b2 被提名到 node2 之后、绑定之前，假设 a3 已经退出，A 回到 4，B 的账面仍是 2（只有 b1）。此时 a4 进入调度：检查一 `4 + 2 = 6 ≤ 6` 通过；检查二里，b2 属于 B，B 的 `Used` 2 没有超过 `min`，所以 b2 的 2 CPU 计入 `nominatedPodsReqWithPodReq`，结果是 `ΣUsed 6 + b2 的 2 + a4 的 2 = 10 > 8`，a4 被拒。如果不计入 b2，`6 + 2 = 8 ≤ 8` 会放行，a4 就把 B 刚回收、b2 正等着的那份资源抢走了。

### 6.5 为什么需要 AddPod / RemovePod

`PreFilterExtensions` 的存在本身就是为了抢占：PreFilter 预先算好的状态（比如 PodTopologySpread 的拓扑计数）在模拟「移走一个 Pod」时需要同步更新，否则 Filter 看到的仍是旧计数。`CapacityScheduling` 的状态是配额用量，道理相同：

```go
// pkg/capacityscheduling/capacity_scheduling.go
func (c *CapacityScheduling) RemovePod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	// ...
	elasticQuotaSnapshotState, err := getElasticQuotaSnapshotState(cycleState)
	// ...
	elasticQuotaInfo := elasticQuotaSnapshotState.elasticQuotaInfos[podToRemove.GetPod().Namespace]
	if elasticQuotaInfo != nil {
		err = elasticQuotaInfo.deletePodIfPresent(podToRemove.GetPod())
		// ...
	}

	return fwk.NewStatus(fwk.Success, "")
}
```

改的是 CycleState 里快照的副本，不碰实时账本。另外，`RunFilterPluginsWithNominatedPods` 在评估时会把同节点上优先级不低于抢占者的 nominated Pod 临时加入，这也会触发 `AddPod`，于是这些 nominated Pod 的用量在快照里也会被计入。

## 七、EventsToRegister：被拒之后什么时候重试

```go
// pkg/capacityscheduling/capacity_scheduling.go
func (c *CapacityScheduling) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	// To register a custom event, follow the naming convention at:
	// https://github.com/kubernetes/kubernetes/pull/101394
	// Please follow: eventhandlers.go#L403-L410
	eqGVK := fmt.Sprintf("elasticquotas.v1alpha1.%v", scheduling.GroupName)
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Delete}},
		{Event: fwk.ClusterEvent{Resource: fwk.EventResource(eqGVK), ActionType: fwk.All}},
	}, nil
}
```

两类事件，都没有 `QueueingHintFn`。框架对这种情况的处理是：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/scheduler.go
// defaultQueueingHintFn is the default queueing hint function.
// It always returns Queue as the queueing hint.
var defaultQueueingHintFn = func(_ klog.Logger, _ *v1.Pod, _, _ interface{}) (fwk.QueueingHint, error) {
	return fwk.Queue, nil
}
```

也就是只要事件发生就重新入队，不做任何过滤。

**Pod/Delete。** 释放配额的事件。它匹配框架内部的 `AssignedPodDelete`（`fwk.Pod` 会同时匹配 `assignedPod` 和 `unschedulablePod` 两种内部资源）。由于调度器的 Pod informer 用字段选择器排除了终态 Pod，Pod 跑完进入 `Succeeded`/`Failed` 也表现为一次删除，所以 Job 完成同样会唤醒等待配额的 Pod。代价是集群里任何 Pod 删除，包括与配额毫无关系的 Namespace，都会把所有被 `CapacityScheduling` 拒绝的 Pod 重新入队。在 Pod 变动频繁的大集群里，这些 Pod 会被反复拿出来跑一遍 PreFilter（每次都要复制整本账）再被拒绝。一个按 Namespace 是否有 EQ、或按释放量过滤的 hint 可以省下大部分无效重试，目前没有实现。

**`elasticquotas.v1alpha1.scheduling.x-k8s.io` 的全部事件。** 调整 `min`/`max`、新建或删除配额，都可能让被拒的 Pod 变得可调度。这个字符串是框架约定的 `<resource>.<version>.<group>` 格式，调度器在 `addAllEventHandlers` 里检测到至少两个点的自定义资源名时，会用 dynamic informer 去 watch 它：

```go
// k8s.io/kubernetes@v1.36.4/pkg/scheduler/eventhandlers.go
			// GVK is expected to be at least 3-folded, separated by dots.
			// <kind in plural>.<version>.<group>
			// Valid examples:
			// - foos.v1.example.com
			// - bars.v1beta1.a.b.c
			// Invalid examples:
			// - foos.v1 (2 sections)
			// - foo.v1.example.com (the first section should be plural)
			if strings.Count(string(gvk), ".") < 2 {
				utilruntime.HandleErrorWithLogger(logger, nil, "Incorrect event registration", "gvk", gvk)
				continue
			}
			// Fall back to try dynamic informers.
			gvr, _ := schema.ParseResourceArg(string(gvk))
			dynInformer := dynInformerFactory.ForResource(*gvr).Informer()
```

这意味着调度器进程里实际有两个 `ElasticQuota` informer：插件自己通过 controller-runtime 建的那个（用于维护账本），和框架为事件建的 dynamic informer（只用于入队）。两者独立同步，理论上存在「入队事件先到、插件账本后更新」的窗口：Pod 被唤醒、拿到的仍是旧的 `min`/`max`，再次被拒。由于后续的账本更新不再产生新的入队事件，这个 Pod 只能等下一次 Pod 删除，或者 5 分钟的 unschedulable 超时（`DefaultPodMaxInUnschedulablePodsDuration`）把它刷回队列。

没有注册 Pod 的 Add/Update 看起来会漏掉一种情况：PreFilter 的拒绝可能只是因为计入了别人的 nominated Pod，后来那个 Pod 的提名被清除或换了节点，这在 API 上是一次 Pod 更新。框架对此另有处理：`updatePodInSchedulingQueue` 发现 `nominatedNodeName` 变化时，会以 `AssignedPodDelete` 事件触发一次 `MoveAllToActiveOrBackoffQueue`，相当于「旧节点上少了一个 Pod」，正好命中插件注册的 Pod/Delete。

## 八、边界情况与坑

**`min`/`max` 只写部分维度。** 第三节已经分析：只要写了任何一项，没写的 cpu/memory/ephemeral-storage 就是 0。表现为「配额明明很大，Pod 却报 ElasticQuota is more than Max」，或者租户在 CPU 远低于 `min` 时被当作借用者。扩展资源没写则不受影响（`max` 视为无穷、`min` 视为 0）。

**先有 Pod、后建 ElasticQuota。** `addElasticQuota` 新建条目时 `Used` 为 0，`pods` 为空。Namespace 里已经在运行的 Pod 不会被补记，因为 Pod informer 不会为它们再发 Add 事件（调度器 informer 的 resync 周期为 0）。于是新建的配额会少算已有用量，直到这些 Pod 被删除重建或调度器重启。对一个已经有工作负载的 Namespace 启用配额，最好在调度器重启前后完成；删除再重建 `ElasticQuota` 也有同样的问题。

**跨 Namespace 回收不看优先级。** 回收模式下，一个优先级 0 的 Pod 可以驱逐另一个租户优先级 100 万的 Pod，只要后者的租户处于借用状态。重要的服务不应该放在可能借用的配额里，或者它所在的 quota 应该设 `max = min`，从根上不借。

**多维度共同决定「是否在借」。** `usedOverMinWith` 任一维度超出即为真，GPU 用满 `min` 会让整个 quota 失去跨 Namespace 回收 CPU 的能力。

**单节点抢占导致的碎片。** 受害者必须和抢占者在同一个节点上。KEP 的「已知限制」里给了一个例子：A 的两个 1 CPU Pod 分散在两个节点上，B 需要一个 2 CPU 的 Pod 回收自己的 `min`，但任何一个节点单独腾出的空间都不够，B 拿不回额度。这正是本系列第 32 篇 [CrossNodePreemption](/posts/sched-plugin-32-cross-node-preemption/) 想解决的问题。KEP 的建议是把 `ΣMin` 设得比集群容量略小，留出余量吸收碎片。

**和 `DefaultPreemption` 同时启用。** PostFilter 插件按顺序执行，第一个返回 `Success` 的生效。如果 `DefaultPreemption` 排在前面，它会忽略配额，直接按优先级驱逐，被驱逐的可能正是在 `min` 以内、本该受保护的 Pod。这也是官方配置用 `disabled: "*"` 把默认 PostFilter 全部关掉的原因。

**多个 Profile 各有一本账。** 调度器为每个 Profile 调用一次 `NewFramework`，插件工厂也会各调用一次，所以两个 Profile 都启用 `CapacityScheduling` 时，会有两个插件实例、两份 `elasticQuotaInfos`。informer 驱动的那部分最终一致，`Reserve` 记的那部分各记各的，同一瞬间两个 Profile 可能都看到同一份余量。应该只在一个 Profile 里启用它，或保证配额 Namespace 的 Pod 都使用同一个 `schedulerName`。同理，多副本调度器靠 leader election 保证只有一个实例在调度，账本才不会分叉。

**`computePodResourceRequest` 不认识 sidecar。** 插件把所有 init container 当作串行执行、取最大值。原生 sidecar（`restartPolicy: Always` 的 init container）在 Pod 运行期间一直存在，in-tree 的资源计算会把它们累加到常驻请求里，这里没有区分。使用 sidecar 时，配额计算会比 NodeResourcesFit 的口径偏小。

## 九、发散

### 9.1 「借用」在调度器里实现的代价

把配额放进调度器，换来的是精确：记账发生在 `Reserve`，资源真正被占用的时刻；Pending 的 Pod 不消耗额度。代价是所有逻辑都被 kube-scheduler 的扩展点结构约束住了：

- 一次只调度一个 Pod，配额判断无法感知「这一批 Pod 能不能一起进来」，与 Coscheduling 组合时，gang 的一部分可能把配额占住，剩下的部分却进不来；
- 抢占只能在单节点内完成，回收效率受碎片影响；
- 账本是进程内存，调度器重启时依赖 informer 重放来重建，重放期间（以及 `addElasticQuota` 先到的情况下）用量是逐步恢复的。

社区后来的 Kueue 走的是另一条路：在准入层面以 Job 为单位排队，用 `ClusterQueue` 的 nominal quota、cohort 内借用与 borrowing/lending limit 表达类似的语义，配额满足后才把 Job 放给调度器。它不做精确到 Pod 绑定时刻的记账，但天然以作业为粒度，也不受单节点抢占的限制。两者解决的问题高度重叠，选择时主要看工作负载是「一堆独立的 Pod」还是「一个个作业」。

### 9.2 与 in-tree 抢占接口的演进

`CapacityScheduling` 能复用 Evaluator，前提是 `preemption.Interface` 足够稳定。实际上这个接口在 Kubernetes 主干上已经变了。master 提交 `aabdf5a2` 中：

```go
// pkg/scheduler/framework/preemption/preemption.go（master）
	SelectVictimsOnNode(ctx context.Context, cycleState fwk.CycleState, preemptor *v1.Pod, nodeInfo fwk.NodeInfo, allPossibleVictims []*DomainVictim, pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, int, *fwk.Status)
```

```go
// pkg/scheduler/framework/preemption/preemption.go（master）
func NewEvaluator(pluginName string, fh fwk.Handle, i Interface, fts feature.Features) *Evaluator {
```

`SelectVictimsOnNode` 多了 `allPossibleVictims` 参数（受害者可以是一整个 PodGroup，跨多个节点），`CandidatesToVictimsMap` 的参数变成 `fwk.PreemptionCandidate`，`NewEvaluator` 不再接收 `*Executor`，改从 `fh.PreemptionManager()` 获取。scheduler-plugins 升级到下一个 Kubernetes 版本时，`preemptor` 需要跟着改。这类依赖 in-tree 内部包（`k8s.io/kubernetes/pkg/...`）的插件，每次升级都要付出这种适配成本，这也是 out-of-tree 插件的常态。

### 9.3 配置建议

- `ΣMin` 贴近集群可分配资源，留少量余量；
- `min`、`max` 写全 cpu、memory，有 GPU 时写上 GPU；
- 关闭 `DefaultPreemption`，只保留 `CapacityScheduling` 的 PostFilter；
- 只在一个 Profile 里启用；
- controller 可选，部署它只是为了看见 `status.used`，并且要记住它只统计 `Running` 的 Pod、不含 overhead。

## 小结

- `CapacityScheduling` 在调度器内存里维护每个 Namespace 的 `Min`/`Max`/`Used`，`pods` 集合保证 informer 与 `Reserve` 两条路径的记账幂等；`status.used` 由另一个进程写，调度器从不读取。
- PreFilter 两道检查：本 quota 不超过 `max`，所有 quota 的用量之和不超过 `min` 之和；后者意味着借用只能借别人闲置的 `min`。两者都返回 `Unschedulable`，失败后所有节点进入抢占。
- PostFilter 复用 v1.36.4 的 `preemption.Evaluator`，自己实现 `preemption.Interface`：抢占者在 `min` 以内时回收借用者的 Pod 且不看优先级，已经在借时只能抢同 Namespace 的低优先级 Pod；赦免时同时检查节点与配额。
- `min`/`max` 只写部分维度时，未写的 cpu/memory 被当作 0，这是最常见的误配置。
- EventsToRegister 只注册了 Pod 删除和 ElasticQuota 变化，没有 QueueingHint，任何 Pod 删除都会唤醒所有被它拒绝的 Pod。

下一篇看 [NetworkOverhead](/posts/sched-plugin-29-network-overhead/)：它关心的不是资源够不够，而是 Pod 之间的网络距离。
