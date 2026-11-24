---
title: 'scheduler-plugins 36 [out-of-tree] SySched：按系统调用画像把 Pod 放到一起'
date: 2026-11-19T14:00:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "scheduler-plugins", "seccomp", "security"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "SySched 从 Security Profiles Operator 的 SeccompProfile 中读出每个 Pod 允许的系统调用，计算新 Pod 放到某节点后所有 Pod 额外暴露的系统调用数（ExS），让系统调用画像相近的 Pod 聚在一起、缩小节点的内核攻击面。本文拆解它的画像获取、节点并集缓存、ExS 打分与反向归一化，给出算例，并指出并发、缓存时序与 KEP 不一致之处。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `SySched` 实现的扩展点（Score、NormalizeScore）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/36.svg" link="/images/posts/sched-plugins/callstack/36.svg" caption="SySched 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

上一篇 [PodState](/posts/sched-plugin-35-pod-state/) 用 Pod 的生命周期状态给节点打分。本篇的 `SySched` 关心的维度在调度器里很少见：安全。同一节点上的 Pod 共享一个内核，任何一个 Pod 能调用的系统调用，都是这个内核对外暴露的攻击面的一部分。一个只需要几十个系统调用的 Pod，如果和一个什么都能调的 Pod 放在一起，就要和对方一起承担那些它根本用不到的内核接口的漏洞风险。

`SySched` 只实现 Score 扩展点（含 `NormalizeScore`），通过 `app.WithPlugin(sysched.Name, sysched.New)` 注册，需要在 Profile 中显式启用，没有 feature gate。README 中的成熟度是 Alpha，设计文档是 `kep/399-sysched-scoring`。本文基于 scheduler-plugins 提交 `cfeb3aa`（依赖 Kubernetes v1.36.4，Security Profiles Operator API v0.4.0）。

## 一、ExS：多暴露的系统调用

「共享内核的风险」要先变成一个能比较大小的数字，调度器才用得上。KEP 为此定义了一个指标 Extraneous System Call（ExS）：一个 Pod 在某节点上的 ExS，是该节点上所有 Pod 允许的系统调用并集，减去这个 Pod 自己允许的系统调用后剩下的个数。

它衡量的是「这个 Pod 因为和别人住在一起，额外暴露在哪些系统调用之下」。

调度时关心的是节点级的 ExS：假设新 Pod 放到这个节点，算出节点上每个 Pod（含新 Pod）的 ExS 再求和。值越小，说明这个节点上的 Pod 画像越接近，放上去之后节点的「开放」系统调用增加得越少。KEP 还设想了给高危系统调用加权（参考 CVE 历史），代码里留了位置但没有实现：

```go {linenos=inline}
// pkg/sysched/sysched.go
func (sc *SySched) calcScore(syscalls sets.Set[string]) int {
	// ...
	// Currently, score is not adjusted based on critical/cve syscalls.
	// NOTE: weight W is hardcoded for now
	// TODO: add critical/cve syscalls
	// TODO: adjust weight W for critical/cve syscalls
	totCrit := 0
	W := 1

	score := syscalls.Len() - totCrit
	score = score + W*totCrit
	// ...
	return score
}
```

`totCrit` 恒为 0、`W` 恒为 1（第 8–9 行），`calcScore` 就是集合大小（第 11 行）。

指标的定义清楚了，接下来要回答的是：每个 Pod 的系统调用集合从哪里来。

## 二、系统调用画像从哪来

ExS 依赖两份数据——单个 Pod 允许哪些系统调用，以及某节点上所有 Pod 的并集。第一份来自 Security Profiles Operator（SPO）管理的 profile 对象，先看插件怎么把它读到内存里。

### 2.1 SeccompProfile CR

Pod 的系统调用集合来自 Security Profiles Operator（SPO）管理的 `SeccompProfile` 对象：

```yaml {linenos=inline}
# manifests/sysched/examples/nginx.profile.yaml（节选）
apiVersion: security-profiles-operator.x-k8s.io/v1beta1
kind: SeccompProfile
metadata:
  name: nginx-seccomp
  namespace: default
spec:
  defaultAction: SCMP_ACT_LOG
  architectures:
  - SCMP_ARCH_X86_64
  syscalls:
  - action: SCMP_ACT_ALLOW
    names:
    - pivot_root
    # ...
```

SPO 会把它落盘到节点 kubelet 的 seccomp 目录下（相对路径形如 `operator/<namespace>/<name>.json`），Pod 通过 `seccompProfile.type: Localhost` 引用这个相对路径；`ProfileBinding`（示例中的 `nginx.binding.yaml`）可以让 SPO 的 webhook 按镜像自动给 Pod 注入引用。插件反过来，从 Pod 的引用路径中解析出 CR 的 namespace 和名字：

```go {linenos=inline}
// pkg/sysched/sysched.go
func parseNameNS(profilePath string) (string, string) {
	if profilePath == "" {
		return "", ""
	}

	parts := strings.Split(profilePath, "/")
	if len(parts) < 2 {
		return "", ""
	}

	ns := parts[len(parts)-2]

	// get filename without extension
	name := strings.TrimSuffix(parts[len(parts)-1], path.Ext(parts[len(parts)-1]))

	return ns, name
}
```

只看路径的最后两段（`ns := parts[len(parts)-2]` 与 `name := ...`，第 12、15 行）：`localhost/operator/default/nginx-seccomp.json` 和 `operator/default/nginx-seccomp.json` 都解析为 `default/nginx-seccomp`。然后读 CR，只合并 `SCMP_ACT_ALLOW` 和 `SCMP_ACT_LOG` 两类动作的名字（第 9–10 行）：

```go {linenos=inline}
// pkg/sysched/sysched.go
	syscallCategories := seccompProfile.Spec.Syscalls

	// need to merge the syscalls in the syscall categories
	// from multiple relevant actions, e.g., allow, log, notify
	for _, element := range syscallCategories {
		// NOTE: should we consider the other categories, e.g., notify, trace?
		// SCMP_ACT_TRACE --> ActTrace, seccomp.ActNotify
		if element.Action == seccomp.ActAllow || element.Action == seccomp.ActLog {
			syscalls = syscalls.Union(sets.New[string](element.Names...))
		}
	}
```

这是一个白名单模型：KEP 明确只支持 `syscalls` 列出允许集合、`defaultAction` 为 `SCMP_ACT_ERRNO` 或 `SCMP_ACT_LOG` 的 profile，不支持黑名单式的 profile，也不考虑参数级过滤。SPO 的 `SeccompProfileSpec` 里还有 `BaseProfileName` 字段，允许一个 profile 继承另一个 profile 的系统调用，插件不处理它，继承来的系统调用不会计入画像。

### 2.2 一个 Pod 的画像：三处来源取并集

一个 Pod 可能通过多个入口引用 profile，`getSyscalls` 把它们并成一个集合。

```go {linenos=inline}
// pkg/sysched/sysched.go
func (sc *SySched) getSyscalls(pod *v1.Pod) sets.Set[string] {
	// ...
	r := sets.New[string]()

	// read the seccomp profile from the security context of a pod
	podSC := pod.Spec.SecurityContext

	if podSC != nil && podSC.SeccompProfile != nil && podSC.SeccompProfile.Type == "Localhost" {
		// ... 解析 LocalhostProfile，读 CR，并入 r
	}

	// read the seccomp profile from container security context and merge them
	for _, container := range pod.Spec.Containers {
		// ... 同上，逐个容器并入 r
	}

	// SPO seccomp profiles are sometimes automatically annotated to a pod
	if pod.ObjectMeta.Annotations != nil {
		// there could be multiple SPO seccomp profile annotations for a pod
		// merge all profiles to obtain the syscal set for a pod
		for k, v := range pod.ObjectMeta.Annotations {
			// looks for annotation related to the seccomp
			if strings.Contains(k, SPO_ANNOTATION) {
				// ... 解析、读 CR、并入 r
				break
			}
		}
	}

	// if a pod does not have a seccomp profile specified, return the set of all syscalls
	if len(r) == 0 {
		syscalls, err := sc.readSPOProfileCR(sc.DefaultProfileName, sc.DefaultProfileNamespace)
		// ...
		if syscalls.Len() > 0 {
			r = r.Union(syscalls)
		}
	}

	return r
}
```

三处来源：Pod 级 `securityContext`、每个 app 容器的 `securityContext`（不含 init 容器和临时容器，第 9–16 行）、键名包含 `seccomp.security.alpha.kubernetes.io` 的 annotation（早期 SPO 和已废弃的 seccomp annotation 用法）。

annotation 这段的注释说「可能有多个，合并所有」（第 20–21 行），但循环在第一个匹配的 key 之后就 `break` 了（第 26 行）。Go 的 map 遍历顺序是随机的，多容器 Pod 用 `container.seccomp.security.alpha.kubernetes.io/<容器名>` 形式标注时，每次只会随机读到其中一个容器的 profile。好在新版本 Kubernetes 用的是 `securityContext` 字段，这条路径主要影响老集群。

三处都没读到时，用参数指定的默认 profile（第 32–38 行），代表「不受限的 Pod 能调用所有系统调用」。

### 2.3 参数

默认 profile 由插件参数指定，一共只有两个字段（第 6、9 行）。

```go {linenos=inline}
// apis/config/types.go
type SySchedArgs struct {
	metav1.TypeMeta

	// CR namespace of the default profile for all system calls
	DefaultProfileNamespace string

	// CR name of the default profile for all system calls
	DefaultProfileName string
}
```

默认值（`apis/config/v1/defaults.go`）是 `default/all-syscalls`。仓库里提供的全集 profile `manifests/sysched/examples/full.profile.yaml` 却叫 `full-seccomp`（333 个系统调用），`manifests/sysched/scheduler-config.yaml` 因此显式把 `defaultProfileName` 设成了 `full-seccomp`。只用默认参数又没创建 `all-syscalls` 时，未配置 seccomp 的 Pod 画像为空，后果见 4.2。

单个 Pod 的画像有了，但打分要看的是节点上所有 Pod 的并集。每次打分都重算一遍代价很高，插件为此在内存里维护了一份节点级缓存。

## 三、节点画像缓存

### 3.1 状态

这份缓存就是 `SySched` 结构体里的两个 map。

```go {linenos=inline}
// pkg/sysched/sysched.go
type SySched struct {
	logger klog.Logger
	handle fwk.Handle
	client client.Client
	// Maintain state of what pods on each node
	// Cached state from SharedLister does not hold system wide info of pods
	// scheduled by other schedulers
	HostToPods map[string][]*v1.Pod
	// Key: node name
	// Value: set of system call names
	HostSyscalls            map[string]sets.Set[string]
	ExSAvg                  float64
	ExSAvgCount             int64
	DefaultProfileNamespace string
	DefaultProfileName      string
	WeightedSyscallProfile  string
}
```

`HostToPods` 记录每个节点上的 Pod（第 9 行），`HostSyscalls` 是每个节点上所有 Pod 画像的并集（第 12 行）。插件没有用 CycleState，这些都是跨调度周期的插件内部状态。`ExSAvg`、`ExSAvgCount`（第 13–14 行）在每次 Score 时更新一个滑动平均，但除了打日志之外没有任何地方读取它；`WeightedSyscallProfile`（第 17 行）从未被赋值。

`client` 由 `util.NewClientWithCachedReader` 创建，是带 controller-runtime 缓存的客户端，读 `SeccompProfile` 走本地缓存而不是每次请求 API Server。

### 3.2 informer 事件

缓存的内容由 informer 事件维护。`New` 在调度器共享的 Pod informer 上注册了三个回调：

```go {linenos=inline}
// pkg/sysched/sysched.go
func (sc *SySched) podAdded(obj interface{}) {
	// ...
	pod := obj.(*v1.Pod)

	// Add already running pod to map
	// This is for when our scheduler comes up after other pods
	if pod.Status.Phase == v1.PodRunning {
		// ...
		sc.addPod(pod)
	}
}

func (sc *SySched) podUpdated(old, new interface{}) {
	// ...
	pod := old.(*v1.Pod)

	// Pod has been assigned to node, now can add to our map
	if pod.Status.Phase == v1.PodPending && pod.Status.HostIP != "" {
		// ...
		sc.addPod(pod)
	}
}

func (sc *SySched) podDeleted(obj interface{}) {
	// ...
	pod := obj.(*v1.Pod)
	// ...
	sc.removePod(pod)
}
```

注释说之所以不用 SharedLister，是因为它不包含其他调度器调度的 Pod。实际上调度器的 snapshot 包含节点上所有已绑定的 Pod，与调度器无关；自建映射真正的好处是能缓存每个节点的系统调用并集，不必每次重新计算。

`podUpdated` 判断的是旧对象（第 16 行的 `pod := old.(*v1.Pod)`），条件有两个：旧对象处于 `Pending`，且已经带上 `HostIP`（第 19 行）。一个新调度的 Pod 要依次经历：

1. 绑定：`nodeName` 被设置，`HostIP` 仍为空；
2. kubelet 上报 `HostIP`：仍是 `Pending`；
3. 后续的状态更新：容器状态变化、变为 `Running` 等。

只有在第 2 步之后的那次更新里，旧对象才同时满足两个条件，Pod 这时才进入 `HostToPods`。**在那之前，调度器在这个节点上看不到它的系统调用。**

删除这一侧，调度器的 Pod informer 带有字段选择器 `status.phase!=Succeeded,status.phase!=Failed`（`kubernetes@v1.36.4/pkg/scheduler/scheduler.go` 的 `newPodInformer`），Pod 进入终态时对 informer 而言就是删除事件，会触发 `removePod`，节点并集按剩余 Pod 重新计算。

`podDeleted` 直接做 `obj.(*v1.Pod)` 断言（第 27 行），没有处理 informer 在丢失删除事件时投递的 `DeletedFinalStateUnknown`，断言失败会 panic。

`addPod` 和 `removePod` 都只按 `pod.Name` 比较，不看 namespace。两个 namespace 里同名的 Pod 落在同一个节点上时，后一个不会被加入；删除其中一个会把另一个也移除。

## 四、Score 与 NormalizeScore

节点并集维护好了，回到打分本身：Score 用这份缓存算出每个节点的原始分，NormalizeScore 再把原始分折算成 0~100 的最终分。

### 4.1 打分

两个 map 在 Score 里被读取。

```go {linenos=inline}
// pkg/sysched/sysched.go
func (sc *SySched) Score(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	// ...
	node := nodeInfo.Node()
	if node == nil {
		return 0, nil
	}
	nodeName := node.Name

	podSyscalls := sc.getSyscalls(pod)

	// NOTE: this condition is true only when a pod does not
	// have a syscall profile, or the unconfined syscall is
	// not set. We return a large number (INT64_MAX) as score.
	if len(podSyscalls) == 0 {
		return math.MaxInt64, nil
	}

	_, hostSyscalls := sc.getHostSyscalls(node.Name)

	// when a host or node does not have any pods
	// running, the extraneous syscall score is zero
	if hostSyscalls == nil {
		return 0, nil
	}

	diffSyscalls := hostSyscalls.Difference(podSyscalls)
	totalDiffs := sc.calcScore(diffSyscalls)

	// add the difference existing pods will see if new Pod is added into this host
	newHostSyscalls := hostSyscalls.Clone()
	newHostSyscalls = newHostSyscalls.Union(podSyscalls)
	for _, p := range sc.HostToPods[node.Name] {
		podSyscalls = sc.getSyscalls(p)
		diffSyscalls = newHostSyscalls.Difference(podSyscalls)
		totalDiffs += sc.calcScore(diffSyscalls)
	}

	sc.ExSAvg = sc.ExSAvg + (float64(totalDiffs)-sc.ExSAvg)/float64(sc.ExSAvgCount)
	sc.ExSAvgCount += 1
	// ...
	return int64(totalDiffs), nil
}
```

写成公式，设节点并集为 H、新 Pod 画像为 S，节点上已有 Pod 的画像为 S₁…Sₙ：

```text {linenos=inline}
raw = |H − S| + Σᵢ |(H ∪ S) − Sᵢ|
```

第一项是新 Pod 的 ExS（4.1 节 Score 代码第 27–28 行）。严格地说新 Pod 的 ExS 应该是 `|(H ∪ S) − S|`，但这与 `|H − S|` 相等，代码用了更省的写法。第二项是已有 Pod 在新 Pod 到来后的 ExS 之和（4.1 节代码第 33–37 行）。注意第二项包含已有 Pod **原本就有**的 ExS，不只是增量，所以原始分会随节点上 Pod 数量增长：Pod 越多的节点，基数越大。

### 4.2 归一化

原始分只是相对大小，框架要求每个插件给出 0~100 的分数，这一步交给 in-tree 的归一化函数。

```go {linenos=inline}
// pkg/sysched/sysched.go
func (sc *SySched) NormalizeScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	// ...
	ret := helper.DefaultNormalizeScore(fwk.MaxNodeScore, true, scores)
	// ...
	return ret
}
```

in-tree 的 `DefaultNormalizeScore` 在 `reverse=true` 时（第 4 行的第二个实参）计算 `100 − 100 × score / max`，原始分越小越好；所有原始分都是 0 时全部 100。

当 Pod 画像为空（Pod 没有 profile，默认 profile 也读不到），所有节点都返回 `math.MaxInt64`。此时 `max` 也是 `MaxInt64`，`100 × MaxInt64` 在 int64 中溢出，结果恰好是 −100，除以 `MaxInt64` 得 0，反转后每个节点都是 100 分。

也就是说，**这类 Pod 在所有节点上得分相同，插件对它不起作用**。代码注释说返回大数，KEP 的 Alpha 标准写的是「没有 seccomp profile 的 Pod 视为 no-op（返回 0）」，两者的写法不同，但因为溢出，最终效果都是不影响排序。这个结果依赖整数溢出的巧合，读代码时容易误以为这类 Pod 会被排到最后。

## 五、算例

用三个节点上的小集合把上面的公式走一遍，看原始分和归一化后的排序是否一致。

{{< figure src="/images/posts/sched-plugins/36-sysched-exs.svg" caption="同一个 Pod 在三个节点上的 ExS 原始分与反向归一化结果；空节点直接 0 分原始分、拿满分（点击查看大图）" link="/images/posts/sched-plugins/36-sysched-exs.svg" >}}

待调度 Pod X 的画像为 {4, 7, 8}，三个节点：

- node-1 上有 P1 = {1, 2, 3, 5}。X 的 ExS：`{1,2,3,5} − {4,7,8}`，4 个；新并集 {1,2,3,4,5,7,8}，P1 的 ExS：`{4,7,8}`，3 个。原始分 7。
- node-2 上没有任何被记录的 Pod，`HostSyscalls` 中没有这个节点，原始分 0。
- node-3 上有 P3 = {4, 7, 8, 9}。X 的 ExS：`{9}`，1 个；并集不变，P3 的 ExS 为 0。原始分 1。

反向归一化，`max = 7`：node-1 `100 − 100 × 7 / 7 = 0`；node-3 `100 − 100 × 1 / 7 = 100 − 14 = 86`；node-2 `100`。

画像相近的 node-3 比 node-1 好得多，这是插件想要的效果；但空的 node-2 比 node-3 更好。只要集群里还有空节点，`SySched` 就会把 Pod 往空节点上推，相当于一个分散策略；「物以类聚」要等节点都有 Pod 之后才显现。

如果某个节点上跑着一个没配 seccomp 的 Pod，它被赋予 `full-seccomp` 的 333 个系统调用，节点并集几乎是全集。之后任何受限 Pod 放到这个节点，第一项就是两三百。这类节点会被所有受限 Pod 回避，不受限的 Pod 则在各节点之间没有偏好（它自己的 ExS 接近 0，但它会让已有 Pod 的 ExS 增大，所以也会倾向于空节点或已经有不受限 Pod 的节点）。这正是 KEP 的意图：把「坏邻居」隔离开。

## 六、边界与坑

前几节讲的是插件的设计，这一节收拢实现层面的问题。它们大多来自同一件事：在一个会被并发调用的打分函数里，手工维护跨调度周期的状态。

**并发不安全**。框架通过 `Parallelizer` 并发地为多个节点调用 Score，`SySched` 在 Score 里读 `HostToPods`、`HostSyscalls` 两个普通 map，同时 informer 回调在另一个 goroutine 里写它们，全程没有锁。Go 运行时检测到 map 的并发读写会直接 fatal，无法 recover。

`ExSAvg` 的更新也是数据竞争，只是它不影响打分。集群 Pod 变化频繁时，这是一个真实的稳定性风险。

**新绑定的 Pod 看不见**。如 3.2 所述，Pod 要等 kubelet 上报 `HostIP` 之后的又一次状态更新才进入节点画像，插件也没有实现 Reserve 来记录自己刚做出的决定。

由此带来一个批次效应：一批同画像的 Pod 同时调度时，它们都看不到彼此，会一起涌向同一个「空」节点（原始分 0 的节点），然后在下一批调度时才发现那里已经很拥挤。

**每次打分都重新计算已有 Pod 的画像**。`getSyscalls(p)` 对节点上每个已有 Pod 都会重新解析路径、读 CR、做集合并。读 CR 走本地缓存，但集合运算的开销与「节点数 × 每节点 Pod 数 × 系统调用数」成正比，节点并集虽然缓存了，单个 Pod 的画像没有缓存。新 Pod 自己的画像也是每个节点算一次。

**同名 Pod 冲突、annotation 随机取一个、tombstone 断言**这几处，2.2 与 3.2 已具体展开，此处不重复。

**画像不全**。init 容器、临时容器不计入；`BaseProfileName` 继承的系统调用不计入；`SCMP_ACT_TRACE`、`SCMP_ACT_NOTIFY` 等动作不计入。这些都会让画像比实际小，偏向乐观。

这些是工程实现上的问题。更值得讨论的是方法本身：把安全作为打分维度，究竟能换来多少收益，又有什么代价。

## 七、发散

**调度能提供多少安全收益？** KEP 引用的实验数据是：在 Dirty COW、Dirty Pipe 这类可通过系统调用利用的内核漏洞场景下，受影响的受害 Pod 最多减少 40%，受影响节点最多减少三分之一。

它不是隔离机制，同一节点上的 Pod 仍然共享内核，seccomp 本身才是第一道防线；调度只是让每个节点「开放」的系统调用更少、让一个被攻破的 Pod 能波及的邻居更少。要真正隔离不可信工作负载，应该用 gVisor、Kata 这类沙箱运行时，或者用 taint 和 NodeAffinity 把它们赶到专用节点。

**和反亲和性的对比**。用 PodAntiAffinity 也能把「高危」和「低危」工作负载分开，但它需要人为给工作负载打标签、分等级。`SySched` 直接使用 seccomp profile 这份已经存在、且由 SPO 的录制功能自动生成的数据，粒度到单个系统调用，不需要人工分级。

代价是它只是一个打分插件，没有硬约束，权重低了几乎没有效果，权重高了又会和资源均衡类插件冲突。KEP 在 Risks 里也提到：相同画像的 Pod 被聚集到同一批节点上，可能带来性能上的争用。

**画像的生命周期**。seccomp profile 一般随镜像版本变化，SPO 可以录制新版本的 profile 并更新 CR。CR 更新后，`SySched` 读取画像走的是缓存客户端，下一次打分就会用新画像；但节点并集 `HostSyscalls` 只在 Pod 增删时更新，不会因为 CR 变化而重算，节点画像可能长期停留在旧版本 profile 上，直到该节点上有 Pod 被删除触发 `recomputeHostSyscalls`。

## 小结

- `SySched` 从 SPO 的 `SeccompProfile` 读取 Pod 允许的系统调用（`SCMP_ACT_ALLOW`/`SCMP_ACT_LOG`），未配置 seccomp 的 Pod 使用默认的全集 profile。
- 原始分是新 Pod 放上节点后，新 Pod 和已有 Pod 各自 ExS 的总和，反向归一化后越小越好；空节点原始分为 0，总是拿满分。
- 节点画像由 informer 维护在插件内存中，Pod 要等 kubelet 开始上报状态后才计入，没有 Reserve，也没有锁，存在并发读写 map 的风险。
- 代码与 KEP 有出入：画像为空时返回 `MaxInt64`，因 int64 溢出最终所有节点同分；关键系统调用加权尚未实现。

下一篇 [TargetLoadPacking](/posts/sched-plugin-37-target-load-packing/) 进入 Trimaran 系列，打分的依据从 API 对象转向了节点的实时负载指标。
