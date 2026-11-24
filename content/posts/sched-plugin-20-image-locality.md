---
title: 'scheduler-plugins 20 [in-tree] ImageLocality：镜像越大越值得就近，但不能扎堆'
date: 2026-11-19T11:20:00+08:00
draft: false
tags: ["kubernetes", "scheduler", "ImageLocality", "镜像", "源码分析"]
categories: ["Kubernetes"]
series: ["scheduler-plugins"]
author: "LinPr"
description: "ImageLocality 给已经缓存了 Pod 所需镜像的节点加分，减少拉镜像的时间。本文拆解它的打分公式：镜像大小乘以镜像在集群中的铺开比例、23MiB 与 1000MiB×镜像数的上下阈值、为什么要按节点数缩放来避免节点过热，以及它依赖的数据从哪里来——kubelet 只上报最大的 50 个镜像、调度器缓存如何维护镜像到节点的映射。"
---

> 先看位置：下图是[开篇](/posts/sched-plugin-00-journey/)里的调度器调用栈，红框标出了 `ImageLocality` 实现的扩展点（Score）。读的时候可以对照这张图，看清本篇讲的每个函数在一次调度里什么时候被调用。

{{< figure src="/images/posts/sched-plugins/callstack/20.svg" link="/images/posts/sched-plugins/callstack/20.svg" caption="ImageLocality 在调度流程中的位置：红框是本篇插件实现的扩展点（点击查看大图）" >}}

[上一篇](/posts/sched-plugin-19-balanced-allocation/)的 `NodeResourcesBalancedAllocation` 关心资源配比。默认 Profile 里最后一个打分插件 `ImageLocality` 关心的是另一种成本：Pod 被调度到一个没有镜像的节点上，kubelet 要先把镜像拉下来，几 GB 的模型推理镜像可能要拉好几分钟。如果某个可行节点上已经有这个镜像，Pod 可以几乎立刻启动。

本文基于 Kubernetes master 提交 `aabdf5a2`。`ImageLocality` 只实现了 Score 一个扩展点，没有 PreScore、没有 NormalizeScore，权重 1，没有参数，也没有 feature gate。它是少数几个直接注册 `New`、不经过 `runtime.FactoryAdapter` 注入 feature 开关的插件：

```go
// pkg/scheduler/framework/plugins/imagelocality/image_locality.go
func New(_ context.Context, _ runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
	return &ImageLocality{handle: h}, nil
}
```

它在 `MultiPoint` 里排在 `NodeResourcesBalancedAllocation` 之后、`DefaultBinder` 之前。打完这一项，节点总分就出来了，调度器选出最高分的节点进入 Reserve 和 Permit。

## 一、打分公式

整个插件的逻辑是三个函数：

```go
// pkg/scheduler/framework/plugins/imagelocality/image_locality.go
// The two thresholds are used as bounds for the image score range. They correspond to a reasonable size range for
// container images compressed and stored in registries; 90%ile of images on dockerhub drops into this range.
const (
	mb                    int64 = 1024 * 1024
	minThreshold          int64 = 23 * mb
	maxContainerThreshold int64 = 1000 * mb
)

func (pl *ImageLocality) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	nodeInfos, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return 0, fwk.AsStatus(err)
	}
	totalNumNodes := len(nodeInfos)

	imageScores, imageCount := sumImageScores(nodeInfo, pod, totalNumNodes)
	score := calculatePriority(imageScores, imageCount)

	return score, nil
}
```

### 1.1 累加：哪些镜像算数

```go
// pkg/scheduler/framework/plugins/imagelocality/image_locality.go
func sumImageScores(nodeInfo fwk.NodeInfo, pod *v1.Pod, totalNumNodes int) (int64, int) {
	var sum int64

	for _, container := range pod.Spec.InitContainers {
		if state, ok := nodeInfo.GetImageStates()[normalizedImageName(container.Image)]; ok {
			sum += scaledImageScore(state, totalNumNodes)
		}
	}

	for _, container := range pod.Spec.Containers {
		if state, ok := nodeInfo.GetImageStates()[normalizedImageName(container.Image)]; ok {
			sum += scaledImageScore(state, totalNumNodes)
		}
	}

	imageCount := len(pod.Spec.InitContainers) + len(pod.Spec.Containers)

	for _, volume := range pod.Spec.Volumes {
		if volume.Image == nil {
			continue
		}
		if state, ok := nodeInfo.GetImageStates()[normalizedImageName(volume.Image.Reference)]; ok {
			sum += scaledImageScore(state, totalNumNodes)
		}
		imageCount++
	}
	return sum, imageCount
}
```

三类镜像来源：init 容器、应用容器，以及 `image` 类型的卷（`ImageVolume` 已在 1.36 GA，可以把一个 OCI 镜像作为只读卷挂进 Pod，常用于分发模型权重）。只有节点上存在的镜像才会累加分数，但**每一个来源都计入 `imageCount`**，不管节点上有没有。两个容器用同一个镜像，会被累加两次，`imageCount` 也计两次。

### 1.2 按铺开程度缩放

```go
// pkg/scheduler/framework/plugins/imagelocality/image_locality.go
// scaledImageScore returns an adaptively scaled score for the given state of an image.
// The size of the image is used as the base score, scaled by a factor which considers how much nodes the image has "spread" to.
// This heuristic aims to mitigate the undesirable "node heating problem", i.e., pods get assigned to the same or
// a few nodes due to image locality.
func scaledImageScore(imageState *fwk.ImageStateSummary, totalNumNodes int) int64 {
	spread := float64(imageState.NumNodes) / float64(totalNumNodes)
	return int64(float64(imageState.Size) * spread)
}
```

单个镜像的贡献是 `大小 × 拥有它的节点数 / 节点总数`。

### 1.3 夹到阈值并映射到 0~100

```go
// pkg/scheduler/framework/plugins/imagelocality/image_locality.go
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

下限固定是 23MiB（常量名叫 `mb`，实际是 1024×1024 字节），上限是 1000MiB 乘以镜像来源数。夹好之后线性映射到 0~100，整数除法向下取整。

把三步写成一个式子：

```text
sum   = Σ（节点上已有的镜像） size × numNodes / totalNodes
max   = 1000MiB × imageCount
score = 100 × (clamp(sum, 23MiB, max) − 23MiB) / (max − 23MiB)
```

## 二、算例

{{< figure src="/images/posts/sched-plugins/20-image-locality.svg" caption="单镜像和双镜像 Pod 的打分曲线，以及同一个 800MiB 镜像在不同铺开程度下的得分（点击查看大图）" link="/images/posts/sched-plugins/20-image-locality.svg" >}}

集群有 50 个节点，Pod 只有一个容器，镜像 `registry.example.com/app:v3` 大小 800MiB：

| 节点情况 | sum | 得分 |
| --- | --- | --- |
| 节点上有这个镜像，全集群 40 个节点有 | 800 × 40/50 = 640MiB | 100 × (640 − 23) / (1000 − 23) = **63** |
| 节点上有，全集群 50 个节点都有 | 800MiB | 100 × 777 / 977 = **79** |
| 节点上有，全集群只有 2 个节点有 | 800 × 2/50 = 32MiB | 100 × 9 / 977 = **0** |
| 节点上没有 | 0，夹到 23MiB | **0** |

如果 Pod 还带一个 50MiB 的 sidecar 镜像（50 个节点都有），在第一种节点上：`sum = 640 + 50 = 690MiB`，`imageCount = 2`，上限变成 2000MiB，得分 `100 × (690 − 23) / (2000 − 23) = 33`。多了一个小 sidecar，分数从 63 掉到 33。

这几行数字揭示了公式的三个性质：

**稀有镜像几乎不加分。** 只有 2 个节点有的镜像，缩放后只剩 32MiB，刚过下限，得 0 分。这是有意的，见第三节。

**镜像越普及，有它的节点得分越高，但此时有它的节点也越多。** 50 个节点都有时，每个节点都得 79 分，这一项对排序没有任何影响。`ImageLocality` 真正起作用的区间是「多数节点有、少数节点没有」，比如滚动发布新版本的中途，或者新扩容的节点还没拉过镜像。

**容器越多，单个大镜像的影响越被稀释。** 上限按容器数线性增长，但实际业务里通常只有一个大镜像，sidecar、init 容器的镜像都很小，它们抬高了上限，却贡献不了多少 `sum`。一个带 sidecar 和两个 init 容器的 Pod，上限是 4000MiB；主镜像 3GiB 且全集群铺满，只要另外三个镜像合计不到 928MiB，得分就到不了 100。

## 三、为什么要按节点数缩放

源码注释给的理由是 node heating problem：假设一个新版本镜像刚在一个节点上被拉过，如果打分只看镜像大小，这个节点对这个工作负载的所有后续副本都有巨大的加分，副本会一个接一个地被调度到这里，直到资源耗尽。这和 `PodTopologySpread`、`LeastAllocated` 的目标相反，而且把一个 Deployment 的所有副本放到同一个节点上，节点故障时会一起挂掉。

乘以 `numNodes / totalNodes` 之后：

- 镜像只在少数节点上时，它的「本地性」价值被压得很低，Pod 按其他插件的意见分散开。
- 随着副本被调度到更多节点，镜像铺开，本地性的价值逐渐上升，但这时有镜像的节点已经很多，加分不会集中到一个节点上。

这是一个用「全局铺开程度」修正「局部收益」的启发式。代价是：在大集群里，一个只部署在少数节点池上的工作负载，它的镜像永远铺不开，`ImageLocality` 对它几乎不起作用。

注意 `totalNumNodes` 用的是快照里的**全部**节点（`NodeInfos().List()`），不是 Filter 之后的可行节点。一个 1000 节点的集群里，某个工作负载被节点亲和限制在 20 个 GPU 节点上，它的镜像就算在这 20 个节点上全都有，`spread` 也只有 0.02。

## 四、数据从哪里来

### 4.1 kubelet 上报：只报最大的 50 个

调度器知道节点上有哪些镜像，靠的是 `node.status.images`，由 kubelet 定期更新：

```go
// pkg/kubelet/nodestatus/setters.go
// Images returns a Setter that updates the images on the node.
// imageListFunc is expected to return a list of images sorted in descending order by image size.
// nodeStatusMaxImages is ignored if set to -1.
func Images(nodeStatusMaxImages int32,
	imageListFunc func(context.Context) ([]kubecontainer.Image, error), // typically Kubelet.imageManager.GetImageList
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		// ...
		// we expect imageListFunc to return a sorted list, so we just need to truncate
		if int(nodeStatusMaxImages) > -1 &&
			int(nodeStatusMaxImages) < len(containerImages) {
			containerImages = containerImages[0:nodeStatusMaxImages]
		}

		for _, image := range containerImages {
			// make a copy to avoid modifying slice members of the image items in the list
			names := append([]string{}, image.RepoDigests...)
			names = append(names, image.RepoTags...)
			// Report up to MaxNamesPerImageInNodeStatus names per image.
			if len(names) > MaxNamesPerImageInNodeStatus {
				names = names[0:MaxNamesPerImageInNodeStatus]
			}
			imagesOnNode = append(imagesOnNode, v1.ContainerImage{
				Names:     names,
				SizeBytes: image.Size,
			})
		}

		node.Status.Images = imagesOnNode
		return nil
	}
}
```

两个上限：

- **`nodeStatusMaxImages`**，kubelet 配置项，默认 50（`pkg/kubelet/apis/config/v1beta1/defaults.go`），设为 -1 表示不限制。镜像列表按大小降序排列（镜像管理器用 `sliceutils.ByImageSize` 排序），截断后只剩最大的 50 个。
- **每个镜像最多 5 个名字**（`MaxNamesPerImageInNodeStatus = 5`），先放 digest 形式，再放 tag 形式。

这两个上限是为了控制 Node 对象的大小：Node 对象被所有 kube-proxy、CNI 插件、调度器 watch，每次 kubelet 更新状态都要序列化传输。代价是 `ImageLocality` 看不到小镜像。在一个缓存了上百个镜像的节点上，排在 50 名之后的镜像对调度器是不可见的。好在小镜像拉取本来就快，打分阈值下限 23MiB 也决定了它们贡献不了多少分，所以截断的影响主要落在「节点上镜像很多，而目标镜像不在最大的 50 个里」这种情况。

一个镜像如果被打了超过 5 个 tag，或者有多个 digest，后面的名字会被截掉。Pod 用了被截掉的那个名字，调度器就认为节点上没有这个镜像。

### 4.2 名字归一化

```go
// pkg/scheduler/framework/plugins/imagelocality/image_locality.go
// normalizedImageName returns the CRI compliant name for a given image.
// TODO: cover the corner cases of missed matches, e.g,
// 1. Using Docker as runtime and docker.io/library/test:tag in pod spec, but only test:tag will present in node status
// 2. Using the implicit registry, i.e., test:tag or library/test:tag in pod spec but only docker.io/library/test:tag
// in node status; note that if users consistently use one registry format, this should not happen.
func normalizedImageName(name string) string {
	if strings.LastIndex(name, ":") <= strings.LastIndex(name, "/") {
		name = name + ":latest"
	}
	return name
}
```

唯一的归一化是补 `:latest`：最后一个 `:` 出现在最后一个 `/` 之前（或者根本没有 `:`），说明没写 tag。`registry:5000/app` 这种带端口的仓库地址也能正确处理，因为端口里的 `:` 在 `/` 之前。digest 形式 `app@sha256:abc...` 里有 `:` 且在最后一个 `/` 之后，不会被补。

除此之外，调度器只做字符串的精确匹配，不解析 registry 前缀。Pod 写 `nginx:1.27`，节点上报的是 `docker.io/library/nginx:1.27`（containerd 的常见行为），两者匹配不上，TODO 注释里列的就是这类情况。所以 Pod 里统一写完整的镜像名（带 registry 前缀），`ImageLocality` 才能准确工作。

### 4.3 调度器缓存：镜像到节点的反向索引

节点加入或更新时，缓存维护一张全局表：

```go
// pkg/scheduler/backend/cache/cache.go
func (cache *cacheImpl) addNodeImageStates(node *v1.Node, nodeInfo *framework.NodeInfo) {
	newSum := make(map[string]*fwk.ImageStateSummary)

	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			// update the entry in imageStates
			state, ok := cache.imageStates[name]
			if !ok {
				state = &fwk.ImageStateSummary{
					Size:  image.SizeBytes,
					Nodes: sets.New(node.Name),
				}
				cache.imageStates[name] = state
			} else {
				state.Nodes.Insert(node.Name)
			}
			// create the ImageStateSummary for this image
			if _, ok := newSum[name]; !ok {
				newSum[name] = state
			}
		}
	}
	nodeInfo.ImageStates = newSum
}
```

`cache.imageStates` 以镜像名为 key，值里记录大小和拥有它的节点集合。每个 `NodeInfo.ImageStates` 只是指向这些共享条目的指针。一个镜像的每个名字（digest、各个 tag）都是独立的条目，Pod 用哪个名字都能查到。

生成调度快照时，`NodeInfo.SnapshotConcrete` 调用 `ImageStateSummary.Snapshot()`，把节点集合压缩成一个数字：

```go
// staging/src/k8s.io/kube-scheduler/framework/types.go
// Snapshot returns a copy without Nodes field of ImageStateSummary
func (iss *ImageStateSummary) Snapshot() *ImageStateSummary {
	return &ImageStateSummary{
		Size:     iss.Size,
		NumNodes: iss.Nodes.Len(),
	}
}
```

这里有两个读代码才看得到的细节：

- **`Size` 来自第一个上报它的节点。** 条目创建后，后续节点只往 `Nodes` 里加名字，不更新 `Size`。同一个镜像名在不同架构的节点上大小可能不同（多架构镜像），调度器用的是先到者的值。
- **`NumNodes` 是在节点自己的 `NodeInfo` 被重新快照时才刷新的。** 增量快照只复制 `Generation` 变了的 `NodeInfo`。镜像被拉到节点 B 上，B 的快照会更新；但节点 A 快照里的这个镜像的 `NumNodes` 要等 A 自己发生变化（Pod 增减、节点更新）才会刷新。不同节点看到的 `spread` 可能短暂不一致。好在 Pod 的调度、删除频繁地触发节点变化，这种不一致通常很快就会被抹平。

## 五、边界情况与坑

**`imagePullPolicy: Always` 不影响打分。** 调度器只看节点上有没有镜像。`Always` 策略下 kubelet 仍会联系镜像仓库检查 manifest，但已缓存的层不会重新下载，本地性依然有价值。

**`:latest` 的陷阱。** 节点上有一个旧的 `app:latest`，Pod 也用 `app:latest`，调度器会给这个节点加分，尽管两者可能不是同一个镜像内容。这是 `:latest` 的通用问题，在这里表现为加分错误。

**分数与其他插件相比很小。** 权重 1，满分 100。`TaintToleration` 一个软污点的差距（权重 3）就可能抵消它。它的定位是「同等条件下优先选有镜像的节点」，不是硬性的镜像亲和。

**没有 NormalizeScore。** 如果所有可行节点都有镜像，它们得分相同；如果都没有，都是 0。和 `NodeAffinity` 不同，这里不会把最高分拉到 100。

**每个节点都调用一次 `List()`。** `Score` 里每次都取全部节点列表只为了拿长度，看起来浪费，但快照的 `List()` 直接返回内部切片，开销可以忽略。这个值对一个调度周期里的所有节点都相同，放到 PreScore 里算一次会更整洁，当前实现没有这么做。

**批量调度的签名不包含镜像卷。** `SignPod` 只收集了容器和 init 容器的镜像名。镜像卷由 `VolumesSigner` 覆盖（`VolumeRestrictions`、`VolumeBinding` 等卷插件都提交这个片段），两者合起来才是完整的签名。

## 六、发散

### 6.1 让镜像铺开的几种做法

`ImageLocality` 只能利用现有的镜像分布，不能主动改变它。生产上常见的做法是主动预热：

- **预拉取 DaemonSet。** 用一个 DaemonSet 在每个节点上以 init 容器的形式引用大镜像（命令设为 `true` 之类立即退出），把镜像拉到所有节点。这之后所有节点都有镜像，`ImageLocality` 对排序不再有影响，但拉镜像的成本已经提前付掉了。
- **节点镜像预置。** 在节点镜像（AMI 等）里预置常用的大镜像，新扩容的节点开机即有。
- **按需加载。** 镜像懒加载（如 stargz/SOCI 一类的 snapshotter）让容器在镜像未完全下载时就能启动，这类方案普及之后，镜像本地性的价值会下降。

### 6.2 什么时候值得调权重或禁用

镜像大、发布频繁、节点数多（拉镜像容易打满仓库带宽）的集群，可以适当提高 `ImageLocality` 的权重，让发布期间的新副本尽量落在已有新镜像的节点上。反过来，镜像都很小（几十 MB）、或者已经做了全量预热的集群，这个插件几乎不产生区分度，保留它也没有坏处，因为它的计算量很小。

需要警惕的是和 `PodTopologySpread` 的冲突：拓扑分布要求副本分散到不同的 zone，`ImageLocality` 倾向于镜像已经存在的地方。两者都是软约束时，结果取决于权重，通常 `PodTopologySpread`（权重 2）占上风。

### 6.3 与 kubelet 镜像 GC 的联动

kubelet 的镜像垃圾回收在磁盘使用率超过 `imageGCHighThresholdPercent`（默认 85%）时按最近使用时间删除镜像。被删掉的镜像在下一次节点状态更新时从 `status.images` 里消失，调度器缓存的 `removeNodeImageStates`/`addNodeImageStates` 随之更新。磁盘紧张的节点上，大镜像来来去去，`ImageLocality` 看到的分布也随之波动。

## 七、小结

- `ImageLocality` 只实现 Score，权重 1，没有参数，打分看节点上已有的镜像，来源包括 init 容器、应用容器和镜像卷。
- 单个镜像的贡献是 `size × numNodes / totalNodes`；总和夹到 `[23MiB, 1000MiB × 镜像来源数]` 后线性映射到 0~100。
- 按铺开比例缩放是为了避免 node heating：稀有镜像几乎不加分，副本不会因为镜像本地性扎堆。
- 数据来自 `node.status.images`，kubelet 默认只报最大的 50 个镜像、每个镜像最多 5 个名字；名字匹配只补 `:latest`，Pod 与节点上报的写法要一致。
- 调度器缓存维护镜像到节点集合的反向索引，`Size` 取首个上报节点的值，`NumNodes` 随节点快照刷新。

打分结束，调度器选出了节点。接下来 Pod 要经过 Reserve 和 Permit，对于成组调度的 Pod，这里是它们互相等待的地方：下一篇 [GangScheduling](/posts/sched-plugin-21-gang-scheduling/)。
