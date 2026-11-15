---
title: 'Helm upgrade 源码分析：一次升级到底改了什么'
date: 2026-11-15T16:10:00+08:00
draft: false
tags: ["helm", "kubernetes", "源码阅读", "go"]
categories: ["Kubernetes"]
author: "LinPr"
description: "helm upgrade 到底做了哪些事？本文从 release 的存储格式讲起，拆开 prepareUpgrade、performUpgrade、Update 与 hook 执行四段逻辑，说明资源是怎么被创建、更新、删除的，以及 hook 为什么只能控制 Job 和 Pod 的顺序。"
---

用 Helm 的人大多会背几条命令：`helm install`、`helm upgrade`、`helm rollback`、`helm history`。但真出问题时，能说清楚 `upgrade` 到底改了什么的人不多：

- 为什么删掉一个 Deployment，`helm upgrade` 之后它真的没了，而删掉一个 hook 资源，它却一直留在集群里？
- 为什么 `helm upgrade --wait` 会卡住，有时要等几分钟？
- 为什么 Job 能保证在 Deployment 之前跑完，而 Deployment 之间却没有先后顺序？

这些问题都能在 `pkg/action/upgrade.go` 里找到答案。这篇笔记按源码顺序读一遍 `upgrade` 的完整路径，重点放在三个地方：release 怎么存、资源怎么算、hook 怎么执行。

{{< figure src="/images/posts/helm-upgrade-source/img-00.png" caption="全文的主线：一次 upgrade 就是把当前集群里的资源集合（current）和目标集合（target）做比较，产出三种操作" >}}

上面这张图是整篇文章的骨架：`current` 集合（也叫 `original` 集合，源码里两个名字混用）是上一次发布留下的资源，`target` 集合是这次渲染出来的资源。两者的差集决定谁被创建、谁被更新、谁被删除。后面每一段源码，本质上都是在回答「这两个集合从哪来」和「怎么把它们变成 API 调用」。

## 一、先看 Helm 怎么记录一次发布

### 1.1 release 是有版本号的

`helm history` 的输出能直接说明问题：

{{< figure src="/images/posts/helm-upgrade-source/img-01.png" caption="同一个 release 的三次发布，REVISION 从 1 递增到 3" >}}

每次 `install` 或 `upgrade` 都会产生一个新的 revision。旧 revision 不会消失，状态从 `deployed` 变成 `superseded`，最近一次是 `deployed`。这也是 `helm rollback` 能工作的前提：旧版本的完整信息还在。

### 1.2 release 存在哪：一个可替换的 storage

存储位置由 `HELM_DRIVER` 决定。`Configuration.Init` 里是一个很直白的 switch：

```go
// pkg/action/action.go
var store *storage.Storage
switch helmDriver {
case "secret", "secrets", "":
    d := driver.NewSecrets(newSecretClient(lazyClient))
    store = storage.Init(d)
case "configmap", "configmaps":
    d := driver.NewConfigMaps(newConfigMapClient(lazyClient))
    store = storage.Init(d)
case "memory":
    // ...
    store = storage.Init(d)
case "sql":
    // ...
}
```

注意第一个分支是 `"secret", "secrets", ""`：**空字符串也走 secrets**，所以不设置 `HELM_DRIVER` 时默认就是 Secrets。`storage.Storage` 只是一层包装，真正的读写交给 `driver.Driver` 接口，Secrets、ConfigMaps、Memory、SQL 各是一份实现。

{{< figure src="/images/posts/helm-upgrade-source/img-02.png" caption="Init 里的 driver 选择，红框是 secrets 分支" >}}

存成 Secret 之后，`kubectl get secrets` 能直接看到它们：

{{< figure src="/images/posts/helm-upgrade-source/img-03.png" caption="命名规则 sh.helm.release.v1.&lt;release&gt;.v&lt;revision&gt;，类型是 helm.sh/release.v1" >}}

一个 release 一条 Secret，名字里带着版本号，所以「新老交替」在存储层就是两条 Secret 的状态变化。

### 1.3 release 怎么编码：JSON → gzip → base64

Secret 里不能直接放二进制，`pkg/storage/driver/util.go` 做了三步转换：

```go
func encodeRelease(rls *rspb.Release) (string, error) {
	b, err := json.Marshal(rls)          // 1. JSON 序列化
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)  // 2. gzip 压缩
	if err != nil {
		return "", err
	}
	if _, err = w.Write(b); err != nil {
		return "", err
	}
	w.Close()

	return b64.EncodeToString(buf.Bytes()), nil   // 3. base64 编码
}
```

压缩不是为了省空间（虽然确实能省很多），更主要的是 Secret 的 data 字段要求是 base64 字符串，而整份 release 里包含渲染后的全部 manifest，体积不小。

读取是反过来的三步，但多了一个兼容分支：

```go
func decodeRelease(data string) (*rspb.Release, error) {
	b, err := b64.DecodeString(data)       // 1. base64 解码
	// ...
	// For backwards compatibility with releases that were stored before
	// compression was introduced we skip decompression if the
	// gzip magic header is not found
	if len(b) > 3 && bytes.Equal(b[0:3], magicGzip) {   // 2. 有 gzip 头才解压
		// ...
	}
	var rls rspb.Release
	if err := json.Unmarshal(b, &rls); err != nil {     // 3. JSON 反序列化
		// ...
	}
	return &rls, nil
}
```

`magicGzip` 这个判断是老版本兼容：早期 Helm 存的是未压缩的 JSON，升级到新版本后还要能读出来。这类「读取时兼容、写入时统一」的处理，在做存储格式演进时是标准做法。

{{< figure src="/images/posts/helm-upgrade-source/img-04.png" caption="encodeRelease / decodeRelease 的完整实现" >}}

## 二、upgrade 的入口和两个阶段

### 2.1 RunWithContext 只做两件事

`Upgrade.RunWithContext` 去掉校验和 dry-run 分支后，主体只有两行：

```go
// pkg/action/upgrade.go
func (u *Upgrade) RunWithContext(ctx context.Context, name string, chart *chart.Chart, vals map[string]interface{}) (*release.Release, error) {
	// ...
	currentRelease, upgradedRelease, err := u.prepareUpgrade(name, chart, vals)   // ① 准备
	if err != nil {
		return nil, err
	}
	// ...
	res, err := u.performUpgrade(ctx, currentRelease, upgradedRelease)            // ② 执行
	// ...
}
```

{{< figure src="/images/posts/helm-upgrade-source/img-05.png" caption="RunWithContext：①② 两个红框就是全部核心逻辑" >}}

### 2.2 prepareUpgrade：把资源分成 hooks 和非 hooks

第一件事是拿到「上一次发布的资源」。它会先取最近一个未删除的 release，如果不是 `deployed` 状态（比如上次失败了），再去找最近一个 `deployed` 的。有个细节值得一提：

```go
// Concurrent `helm upgrade`s will either fail here with `errPending` or when creating the release with "already exists". This should act as a pessimistic lock.
if lastRelease.Info.Status.IsPending() {
    return nil, nil, errPending
}
```

这是个**悲观锁**：上一个 upgrade 还停在 `pending`（比如进程被杀），新的 upgrade 会直接报错，而不是并发地改同一份资源。

接着渲染 chart。渲染结果被拆成两部分：

{{< figure src="/images/posts/helm-upgrade-source/img-06.png" caption="manifest_sorter.save 的分流逻辑：没有 hook 注解的进 generic，有注解的进 hooks" >}}

- 没有 `helm.sh/hook` 注解的资源，进 `Manifest` 字段；
- 带注解的，进 `Hooks` 字段，并按事件类型（`pre-install`、`post-upgrade` 等）归类。

这就是第一节那个问题的答案之一：**hook 资源根本不参与后续的差集计算**，因为它们压根不在 `Manifest` 里。

另外，`prepareUpgrade` 里的渲染和 `helm template` 走的是同一套逻辑（`renderResources`）。所以在 CI 里先用 `helm template` 渲染一遍看结果，和 `upgrade` 实际渲染出来的内容是一致的。

### 2.3 performUpgrade：两个集合，一次差集

`performUpgrade` 开头把两边的 Manifest 都解析成 `ResourceList`：

```go
current, err := u.cfg.KubeClient.Build(bytes.NewBufferString(originalRelease.Manifest), false)
// ...
target, err := u.cfg.KubeClient.Build(bytes.NewBufferString(upgradedRelease.Manifest), !u.DisableOpenAPIValidation)
```

然后计算「这次要新建哪些资源」——也就是 target 里存在、current 里不存在的那些：

```go
// Do a basic diff using gvk + name to figure out what new resources are being created so we can validate they don't already exist
existingResources := make(map[string]bool)
for _, r := range current {
    existingResources[objectKey(r)] = true
}

var toBeCreated kube.ResourceList
for _, r := range target {
    if !existingResources[objectKey(r)] {
        toBeCreated = append(toBeCreated, r)
    }
}
```

{{< figure src="/images/posts/helm-upgrade-source/img-07.png" caption="差集的几何含义：B − A 就是「target 有、current 没有」" >}}

这里的 `toBeCreated` 并不直接拿去创建，而是先做一次冲突检查：

```go
var toBeUpdated kube.ResourceList
if u.TakeOwnership {
    toBeUpdated, err = requireAdoption(toBeCreated)
} else {
    toBeUpdated, err = existingResourceConflict(toBeCreated, upgradedRelease.Name, upgradedRelease.Namespace)
}
// ...
toBeUpdated.Visit(func(r *resource.Info, err error) error {
    // ...
    current.Append(r)
    return nil
})
```

这一步容易被忽略，但很重要。`toBeCreated` 只说明「它不在上一个 release 里」，不说明「集群里也没有」。如果集群里已经有一个同名同类型的对象（比如手工 `kubectl apply` 创建的），Helm 默认会拒绝继续，除非你开了 `--take-ownership`。

检查通过后，这些已经存在的资源会被**追加回 current 集合**——因为接下来要对它们做更新，而不是创建。

{{< figure src="/images/posts/helm-upgrade-source/img-08.png" caption="performUpgrade：差集得到 toBeCreated，冲突检查得到 toBeUpdated，最后把两者一起交给 releasingUpgrade" >}}

## 三、releasingUpgrade：pre-hook、普通资源、post-hook

真正动手的函数是 `releasingUpgrade`，结构非常清楚：

```go
// pkg/action/upgrade.go（节选）
func (u *Upgrade) releasingUpgrade(c chan<- resultMessage, upgradedRelease *release.Release, current kube.ResourceList, target kube.ResourceList, originalRelease *release.Release) {
	// ① pre-upgrade hooks
	if !u.DisableHooks {
		if err := u.cfg.execHook(upgradedRelease, release.HookPreUpgrade, u.WaitStrategy, u.Timeout); err != nil {
			// ...
			return
		}
	}

	// ② 普通资源：Update + Wait
	results, err := u.cfg.KubeClient.Update(current, target, u.Force)
	// ...
	if err := waiter.Wait(target, u.Timeout); err != nil {
		// ...
	}

	// ③ post-upgrade hooks
	if !u.DisableHooks {
		if err := u.cfg.execHook(upgradedRelease, release.HookPostUpgrade, u.WaitStrategy, u.Timeout); err != nil {
			// ...
			return
		}
	}

	// ④ 更新 release 状态
	originalRelease.Info.Status = release.StatusSuperseded
	u.cfg.recordRelease(originalRelease)
	upgradedRelease.Info.Status = release.StatusDeployed
	// ...
}
```

| 阶段 | 做什么 | 失败时 |
| --- | --- | --- |
| ① pre-upgrade hook | 按权重执行 `pre-upgrade` 事件的全部 hook | 直接返回，不动普通资源 |
| ② `KubeClient.Update` | 对普通资源做增删改 | 记录原 release，回滚状态 |
| ② `waiter.Wait` | 等资源就绪（`--wait` 才有意义） | 同上 |
| ③ post-upgrade hook | 执行 `post-upgrade` 事件的 hook | 同上 |
| ④ 落状态 | 旧版 `superseded`，新版 `deployed` | — |

{{< figure src="/images/posts/helm-upgrade-source/img-10.png" caption="releasingUpgrade 的三个红框，对应上表的三段" >}}

注意 ② 和 ③ 之间没有「先等 hook 完成再等资源就绪」这种交错：所有普通资源是一次性交给 `Update` 的，`Wait` 也是一次性等全部。这正是「Helm 无法控制两个 Deployment 谁先启动」的根源。

## 四、Update：差集怎么变成 API 调用

`KubeClient.Update` 的注释把语义写得很清楚：

```go
// Update takes the current list of objects and target list of objects and
// creates resources that don't already exist, updates resources that have been
// modified in the target configuration, and deletes resources from the current
// configuration that are not present in the target configuration.
```

{{< figure src="/images/posts/helm-upgrade-source/img-14.png" caption="Update 的注释与签名" >}}

翻译成三种情况：

| 条件 | 动作 |
| --- | --- |
| current 没有、target 有 | 创建 |
| current 有、target 有 | 更新 |
| current 有、target 没有 | 删除 |

实现上分两趟。第一趟遍历 target：

```go
// pkg/kube/client.go，update()
err := target.Visit(func(info *resource.Info, err error) error {
	if err != nil {
		return err
	}
	helper := resource.NewHelper(info.Client, info.Mapping).WithFieldManager(getManagedFieldsManager())
	if _, err := helper.Get(info.Namespace, info.Name); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("could not get information about the resource: %w", err)
		}
		// 集群里不存在 → 创建
		res.Created = append(res.Created, info)
		if err := createResource(info); err != nil {
			return fmt.Errorf("failed to create resource: %w", err)
		}
		return nil
	}
	// 集群里已存在 → 更新
	originalInfo := original.Get(info)
	if originalInfo == nil {
		return fmt.Errorf("no %s with the name %q found", info.Mapping.GroupVersionKind.Kind, info.Name)
	}
	if err := updateResource(c, info, originalInfo.Object, force, threeWayMerge); err != nil {
		updateErrors = append(updateErrors, err)
	}
	res.Updated = append(res.Updated, info)
	return nil
})
```

第二趟处理删除，用的是集合差集 `original.Difference(target)`：

```go
for _, info := range original.Difference(target) {
	// ...
	annotations, err := metadataAccessor.Annotations(info.Object)
	// ...
	if annotations != nil && annotations[ResourcePolicyAnno] == KeepPolicy {
		slog.Debug("skipping delete due to annotation", /* ... */)
		continue
	}
	if err := deleteResource(info, metav1.DeletePropagationBackground); err != nil {
		// ...
		continue
	}
	res.Deleted = append(res.Deleted, info)
}
```

`original.Difference(target)` 就是「旧 release 有、新 release 没有」的资源，也就是用户从 chart 里删掉的资源。这里有一个开关值得记住：**打了 `helm.sh/resource-policy: keep` 注解的资源会被跳过删除**。做数据卷、CRD 这类「删了会出大事」的资源时，这个注解比 `--keep-history` 更有用。

删除失败不会中断整个 upgrade：`continue` 只是跳过这一条，最后的结果里 `Deleted` 列表会少一项。

关于这段实现，有一个可以讨论的点：它用迭代器「找一个处理一个」，集合运算和 CRUD 调用是交织的。如果先算出「该创建的」「该更新的」「该删除的」三个列表，再分别执行，逻辑会更容易读，也更容易为每种操作加上各自的错误处理策略。当前写法在功能上没有问题，只是阅读时要在回调里来回跳。

## 五、hook：为什么它只能控制 Job 和 Pod

### 5.1 hook 资源不归 release 管

官方文档写得很直接：

{{< figure src="/images/posts/helm-upgrade-source/img-09.png" caption="Helm 文档：hook 创建的资源不被当作 release 的一部分来管理" >}}

> The resources that a hook creates are currently not tracked or managed as part of the release. Once Helm verifies that the hook has reached its ready state, it will leave the hook resource alone.

这解释了两个现象：

1. `helm uninstall` 不会删除 hook 创建的资源。要删，得自己加 `helm.sh/hook-delete-policy`，或者给 Job 设 TTL；
2. `helm upgrade` 的差集计算不包含 hook——它们不在 `Manifest` 里，第三节那个 `Update` 根本看不到它们。

需要保留的 hook 资源，用 `helm.sh/resource-policy: keep` 标记。

### 5.2 执行顺序：先按 kind，再按权重

`execHook` 的手上有两个排序：

```go
// pkg/action/hooks.go
// hooke are pre-ordered by kind, so keep order stable
sort.Stable(hookByWeight(executingHooks))
```

第一步的「按 kind 排序」发生在更早的 manifest 解析阶段（`sortHooksByKind`），用的是 `sort.SliceStable`；第二步按权重排序，也是稳定排序，所以**权重相同的 hook 会保留 kind 排序的结果**。

权重本身来自注解，缺省是 0：

{{< figure src="/images/posts/helm-upgrade-source/img-19.png" caption="calculateHookWeight：从 helm.sh/hook-weight 注解解析整数，解析失败按 0 处理" >}}

```go
func (x hookByWeight) Less(i, j int) bool {
	if x[i].Weight == x[j].Weight {
		return x[i].Name < x[j].Name
	}
	return x[i].Weight < x[j].Weight
}
```

比较器还有第二级：权重相同就比较名字。这和官方文档说的一致——Helm 3.2 之前，同权重是按名字的字母序执行的，那很容易踩坑：一个叫 `auto-run-job` 的 Job 会排在 `migration-config` 这个 ConfigMap 前面，然后因为依赖不存在而失败。3.2 之后改成按 kind 的内置顺序，凭经验是「基础设施在前、工作负载在后」。

{{< figure src="/images/posts/helm-upgrade-source/img-20.png" caption="execHook：先按 kind 预排，再按权重稳定排序，然后逐个执行" >}}

结论是：**需要明确顺序时，永远显式写 `hook-weight`**，不要依赖字母序或 kind 序。

### 5.3 为什么只有 Job 和 Pod 会被「等待」

执行每个 hook 后，Helm 会调用 `WatchUntilReady` 等它就绪。关键在于这个 waiter 对不同的 kind 用不同的 status reader：

```go
// pkg/kube/statuswait.go
jobSR := helmStatusReaders.NewCustomJobStatusReader(w.restMapper)
podSR := helmStatusReaders.NewCustomPodStatusReader(w.restMapper)
// We don't want to wait on any other resources as watchUntilReady is only for Helm hooks
genericSR := statusreaders.NewGenericStatusReader(w.restMapper, alwaysReady)
```

`genericSR` 的回调是 `alwaysReady`——**除了 Job 和 Pod，其他 kind 一律立刻返回就绪**。接口注释也是这么写的：

{{< figure src="/images/posts/helm-upgrade-source/img-12.png" caption="kube/interface.go：Job 就绪指成功完成，Pod 就绪指 Succeeded，其他 kind 创建即视为就绪" >}}

Job 和 Pod 的判定逻辑在 pod 的 ready 回调里能看到：

{{< figure src="/images/posts/helm-upgrade-source/img-13.png" caption="waitForPodSuccess：Succeeded 视为成功，Failed 视为失败，其余继续等" >}}

这就是「hook 只能用来控制 Job 和 Pod 顺序」的原因。如果往 `pre-upgrade` 里放一个 Deployment，Helm 创建完它立刻认为「这个 hook 就绪了」，然后去执行下一个——Deployment 的 Pod 有没有起来，Helm 根本不看。

顺带回答一个常见疑问：既然 hook 能等 Job，为什么很多团队还要自己做一套发布流程？因为 hook 的能力边界就在这里——它不是一个通用的「工作负载编排」机制，只有 Job/Pod 这种「跑完就结束」的资源才有明确的「就绪」定义，长期运行的 Deployment、StatefulSet 没有这个语义。

## 六、最后一步：新老交替

全部成功之后，`releasingUpgrade` 结尾只改了两个 release 的状态：

{{< figure src="/images/posts/helm-upgrade-source/img-15.png" caption="旧版标记 superseded，新版标记 deployed，并写入描述" >}}

```go
originalRelease.Info.Status = release.StatusSuperseded
u.cfg.recordRelease(originalRelease)
upgradedRelease.Info.Status = release.StatusDeployed
```

`recordRelease` 最终调用 `Storage.Update`，它就是一次带日志的 driver 写入：

{{< figure src="/images/posts/helm-upgrade-source/img-16.png" caption="Storage.Update：key 由 release 名和版本号拼成" >}}

于是在 `helm history` 里看到的就是：

{{< figure src="/images/posts/helm-upgrade-source/img-17.png" caption="三个版本：前两个 superseded，最新一个 deployed" >}}

## 七、把流程串起来

```
helm upgrade myapp ./chart
  │
  ├─ prepareUpgrade
  │    ├─ 取最近 release（pending → 报错，充当悲观锁）
  │    ├─ 渲染 chart → Manifest（普通资源）+ Hooks（hook 资源）
  │    └─ 得到 currentRelease / upgradedRelease
  │
  ├─ performUpgrade
  │    ├─ Build 出 current / target 两个 ResourceList
  │    ├─ 差集 → toBeCreated
  │    ├─ 冲突检查 → toBeUpdated（已存在的追加回 current）
  │    └─ go releasingUpgrade(...)
  │
  └─ releasingUpgrade
       ├─ ① pre-upgrade hooks（按 kind，再按 weight）
       ├─ ② KubeClient.Update(current, target)
       │      ├─ 遍历 target：不存在则 Create，存在则 Update
       │      └─ original.Difference(target)：Delete（跳过 resource-policy: keep）
       ├─ ② waiter.Wait(target)（--wait 时才有事可做）
       ├─ ③ post-upgrade hooks
       └─ ④ 旧版 superseded / 新版 deployed
```

回到文章开头那三个问题：

**① 为什么删掉的资源真的会消失，hook 资源却不会？**
因为 `Update` 的删除逻辑基于 `original.Difference(target)`，而这件事只作用于 `Manifest`——hook 资源在渲染阶段就被分流到 `Hooks` 字段，不参与差集，Helm 也不跟踪它们。

**② 为什么 `--wait` 会卡住？**
`waiter.Wait(target)` 会逐个等 target 里的资源就绪，超时默认 5 分钟（`--timeout` 可改）。卡住通常是因为某个 Deployment 的 Pod 一直起不来，或者镜像拉取失败。

**③ 为什么 Deployment 之间没有顺序？**
所有普通资源一次性交给 `Update`，没有分阶段、没有依赖图，`Update` 完成后又是一次性 `Wait`。要顺序，只能借助 hook（Job/Pod）或者 `--wait-for-jobs` 加更细的分组。

## 八、四点补充

**1. hook 只是「有明确就绪语义的资源」的顺序控制器。** 需要 Deployment 之间的依赖顺序时，Helm 本身给不了，得靠 Argo CD、Flux 之类的 sync wave，或者把依赖关系拆成多个 release。

**2. 差集看的是「gvk + namespace + name」。** `objectKey` 只由这三样拼成，不看内容。所以把一个 Deployment 改成 StatefulSet，或者改了 namespace，在 Helm 眼里就是「删一个、建一个」，而不是「更新」。

**3. `--force` 会走替换而不是 patch。** 它对目标资源做 delete + recreate，能绕过某些不可变字段的错误，代价是 Pod 会真的重建。

**4. 失败时的状态要看清。** `Update` 失败会把原 release 记录一次（状态回到可用的旧版本），新版停在 `failed`。此时 `helm history` 里会多一条 failed 记录，`helm rollback` 回滚的是「上一条 deployed」，不是这条 failed。

## 九、总结

一次 `helm upgrade` 可以压缩成三句话：

1. **release 是一份带版本号的快照。** 每次升级都新写一条记录，默认存成 Secret，内容是 JSON → gzip → base64 编码后的 release 对象。`helm history` 读的就是这些记录。
2. **普通资源靠集合运算决定增删改。** 旧 manifest 是 current，新 manifest 是 target：只在 target 里的创建，两边都有的更新，只在 current 里的删除。比较的键是 gvk + namespace + name，不看内容。
3. **hook 是游离在集合之外的顺序控制。** hook 资源不进 manifest，不参与差集，Helm 也不负责清理。它们按 kind、再按 weight 排序执行，但只有 Job 和 Pod 会被真正等待，其他 kind 创建完就算完成。

由此也能推出日常使用里的几条经验：

| 现象 | 原因 | 做法 |
| --- | --- | --- |
| 删掉的 Deployment 在集群里消失了 | 它只在 current 里，被差集删除 | 需要保留时加 `helm.sh/resource-policy: keep` |
| hook 资源一直留在集群里 | hook 不归 release 管 | 用 `helm.sh/hook-delete-policy` 指定清理时机 |
| 改了 kind 或 namespace 后资源被重建 | 比较键变了，等于删一个、建一个 | 把这类改动当作迁移来规划 |
| `--wait` 卡很久 | 在等 target 里的资源全部就绪 | 先查 Pod 为什么起不来，再调 `--timeout` |
| Deployment 之间没有先后顺序 | 普通资源一次性提交、一次性等待 | 用 Job hook，或交给 Argo CD / Flux 的 sync wave |
| 上一次升级中断后再升级直接报错 | 最新 release 停在 pending 状态 | 先 `helm rollback` 或处理掉 pending 记录 |

读源码时抓住 `pkg/action/upgrade.go` 里的 `performUpgrade` 和 `releasingUpgrade`，以及 `pkg/kube/client.go` 里的 `Update`，就能把上面这些现象都对上号。

---

*本文依据 `helm.sh/helm/v4` 源码（`pkg/action/upgrade.go`、`pkg/action/hooks.go`、`pkg/action/action.go`、`pkg/kube/client.go`、`pkg/kube/statuswait.go`、`pkg/storage/driver/util.go`、`pkg/release/util/manifest_sorter.go`）与 Helm 官方文档中的 Chart Hooks 一节。终端截图来自本机 `helm_demo` 环境。*
