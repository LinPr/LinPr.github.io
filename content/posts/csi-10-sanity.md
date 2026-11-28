---
title: 'csi 10 csi-sanity：用规范测试验收一个驱动'
date: 2026-11-28T19:00:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "csi-sanity", "测试"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "规范里的幂等、必填字段、错误码这些要求，靠读文档很难确认自己写对了。csi-sanity 就是把这些要求变成可执行的检查：它连上驱动的 socket，逐条发请求，看返回码是不是规范要求的那个。本文讲它检查什么、怎么跑、失败信息怎么读，以及哪些检查能挑出 hostpath 和 nfs 这类示例驱动的真实问题。"
---

前面九篇讲的都是「规范要求什么、驱动怎么实现」。这一篇换一个角度：**怎么确认一个驱动真的符合规范。**

答案是 `csi-sanity`。它来自 [`kubernetes-csi/csi-test`](https://github.com/kubernetes-csi/csi-test) 仓库，做的事很简单：连上驱动的 socket，把规范里每条可验证的规则发一遍请求，比对返回码。本文基于该仓库提交 `95f23ec`（2026-08）。

## 一、它检查什么

按三个 service 分组，检查项大致如下：

| service | 检查内容 | 举例 |
| --- | --- | --- |
| Identity | 能力声明、`GetPluginInfo` 必填字段、`Probe` 语义 | 返回的 `name`、`vendor_version` 不能为空 |
| Controller | 每个方法的必填字段、错误码、幂等 | 缺 `name` 返回 `InvalidArgument`；同名同容量重复创建返回同一个卷 |
| Node | 阶段调用顺序、路径校验、重复调用 | 没 Stage 就 Publish 返回 `FailedPrecondition` |

**它检查的是行为和返回码，不检查实现**。你的驱动内部用什么锁、状态存哪里，它不关心；它只关心「同一个请求发两次，第二次返回什么」。

三类检查值得单独说：

### 1.1 必填字段

每个方法都有一组「缺这个字段就返回 `InvalidArgument`」的检查。比如 `CreateVolume`：

```go {linenos=inline}
// pkg/sanity/controller.go
		It("should fail when no name is provided", func() {

			_, err := r.CreateVolume(
				context.Background(),
				&csi.CreateVolumeRequest{
					VolumeCapabilities: []*csi.VolumeCapability{ ... },
					Secrets:            sc.Secrets.CreateVolumeSecret,
				},
			)
			ExpectErrorCode(nil, err, codes.InvalidArgument)
		})
```

缺 `name` 必须返回 `InvalidArgument`（第 10 行）。对照 [04 篇](/posts/csi-04-create-delete/)：hostpath 的第 4–6 行、nfs 的第 121–123 行，都做了这个检查。

### 1.2 幂等

最有价值的一类检查。两个用例正好对应 [04 篇](/posts/csi-04-create-delete/)第二节：

```go {linenos=inline}
// pkg/sanity/controller.go
		It("should not fail when requesting to create a volume with already existing name and same capacity", func() {
			// ...
			vol1 := r.MustCreateVolume(context.Background(), MakeCreateVolumeReq(sc, name))
			vol2 := r.MustCreateVolume(context.Background(), MakeCreateVolumeReq(sc, name))
			// ...
			Expect(vol1.GetVolume().GetVolumeId()).To(Equal(vol2.GetVolume().GetVolumeId()))
		})
```

**两次 `CreateVolume` 必须返回同一个 `VolumeId`**（第 6 行）。这一条检查的正是「查重名」那段代码。hostpath 靠 `GetVolumeByName` + 全局锁满足，nfs 靠卷名加锁 + ID 定位满足。

第二个用例的期望是 `AlreadyExists`：

```go {linenos=inline}
// pkg/sanity/controller.go
			size2 := 2 * TestVolumeSize(sc)
			req2 := MakeCreateVolumeReq(sc, name)
			req2.CapacityRange = &csi.CapacityRange{
				RequiredBytes: size2,
				LimitBytes:    size2,
			}
			rsp, err := r.CreateVolume(context.Background(), req2)
			ExpectErrorCode(rsp, err, codes.AlreadyExists)
```

**同名不同容量必须返回 `AlreadyExists`**（第 8 行），不是 `InvalidArgument`。这是 [04 篇](/posts/csi-04-create-delete/)错误码表的第二行，也是很多驱动写错的地方。

### 1.3 删除不存在的卷

```go {linenos=inline}
// pkg/sanity/controller.go
		It("should succeed when an invalid volume id is used", func() {

			_, err := r.DeleteVolume(
				context.Background(),
				&csi.DeleteVolumeRequest{
					VolumeId: sc.Config.IDGen.GenerateInvalidVolumeID(),
					Secrets:  sc.Secrets.DeleteVolumeSecret,
				},
			)
			Expect(err).NotTo(HaveOccurred())
		})
```

用例名字里的 `invalid`（第 1 行）容易误解：它传的是一个**格式合法但不存在**的 ID，期望**不返回错误**（第 10 行）。

**这条检查挑出过大量驱动的 bug**。返回 `NotFound` 的驱动会在这里失败，测试信息是 `unexpected error code NotFound`，而实际行为是 provisioner 会一直重试删除。

## 二、怎么跑

csi-sanity 是一个可执行程序，连驱动的 socket：

```bash {linenos=inline}
# 编译 csi-sanity
git clone https://github.com/kubernetes-csi/csi-test.git
cd csi-test
make
./bin/csi-sanity --version
```

跑之前需要一个能连上的驱动端点。最简单的方式是把 hostpath 的 socket 暴露到宿主机：

```bash {linenos=inline}
# 在部署了 hostpath 的节点上，把 socket 路径指过去
sudo ./bin/csi-sanity \
  --csi.endpoint=unix:///var/lib/kubelet/plugins/csi-hostpath/csi.sock \
  --csi.mountdir=/tmp/csi-sanity-mount \
  --csi.stagingdir=/tmp/csi-sanity-staging \
  --csi.testvolumesize=2147483648
```

四个参数的含义：

| 参数 | 作用 |
| --- | --- |
| `--csi.endpoint` | 驱动的 socket 地址 |
| `--csi.mountdir` | 测试用的 target 路径，驱动会往这里挂 |
| `--csi.stagingdir` | 测试用的 staging 路径，只在驱动声明了 Stage 能力时用 |
| `--csi.testvolumesize` | 测试卷大小，默认 1GiB |

**目录必须事先建好并给足权限**，因为有些驱动不会自己创建。如果 staging 目录不存在，声明了 `STAGE_UNSTAGE_VOLUME` 的驱动会在用例里失败，报错信息指向路径。

也可以分成 controller 和 node 两部分单独跑，用 `--csi.testnodevolumeattachlimit`、`--csi.testvolumeparameters` 这些参数控制。**只测 controller 时不需要 node 侧部署**，适合在 CI 里快速验证。

## 三、失败信息怎么读

输出长这样（以一条幂等失败为例）：

```text {linenos=inline}
• [FAILED] [0.312 seconds]
Controller [Top Level]
  CreateVolume
  should not fail when requesting to create a volume with already existing name and same capacity [It]
  /csi-test/pkg/sanity/controller.go:410

  Expected
      <string>: 2f8c1e4a-7b3d-4f9a-8c2e-1d5b9a7c3e6f
  to equal
      <string>: 9a7f2c1d-4e6b-4a8f-9d3c-5b1e8a2f7c4d
```

读法：

1. 第一行 `[FAILED]` 后面是耗时；
2. 接下来是路径：service → 方法 → 用例名，**用例名就是它期望的行为**；
3. 最后是断言失败的具体值。上面这条说明两次创建返回了不同的 `VolumeId`——幂等没实现；
4. 文件行号 `controller.go:410` 指向检查代码本身，可以去仓库里看它在检查什么。

常见的失败信息对照：

| 失败信息 | 说明 | 对应文章 |
| --- | --- | --- |
| `unexpected error code NotFound`（DeleteVolume） | 删不存在的卷返回了 `NotFound` | [04 篇](/posts/csi-04-create-delete/) |
| `to equal ...` 且两个 ID 不同 | 重复创建返回了不同卷 | [04 篇](/posts/csi-04-create-delete/) |
| `Expected ... to have code InvalidArgument` | 必填字段没校验 | [04 篇](/posts/csi-04-create-delete/) |
| `Expected error, got nil` | 该报错时成功了 | 同上 |
| Node 用例里 `FailedPrecondition` 相关 | Stage/Publish 顺序不对 | [06 篇](/posts/csi-06-node-mount/) |

## 四、两份驱动跑出来的差别

hostpath 是官方示例，通过的用例比较多；nfs 因为有一批 `Unimplemented`，链式用例会被跳过而不是失败。

sanity 框架会自动处理这一点：
1. 驱动没声明某项能力，相关用例会被 `Skip`，不算失败；
2. 声明了但行为不对，才算失败。

所以**跑 nfs 时的「通过数」会比 hostpath 少很多**，那不是问题。真正要看的是 `FAILED` 的数量，以及被跳过的原因是否和驱动声明一致。

三个值得对照的点：

1. **nfs 没有 Stage**，所以所有 `NodeStageVolume` 相关的用例都会被跳过。如果它们没被跳过而是失败了，说明驱动的能力声明有误；
2. **nfs 的 `GetCapacity` 是 `Unimplemented`**，`ControllerGetCapabilities` 里也没声明，对应用例跳过；
3. **hostpath 默认没开 attach**，所以 `ControllerPublishVolume` 的用例会跳过——除非用 `--enable-attach` 重启驱动。**这是一个很好的实验**：用两种参数各跑一次，对比跳过项的差异，就能看清能力表的作用。

## 五、它检查不到什么

csi-sanity 只覆盖协议层，下面这些都不检查，只能自己测：

| 检查不到 | 为什么 | 怎么补 |
| --- | --- | --- |
| 真实挂载是否生效 | 不知道文件系统语义 | 起一个 Pod 真写文件 |
| sidecar 集成 | 它不启动 sidecar | E2E 测试，或手工走一遍 [08 篇](/posts/csi-08-lifecycle/)的流程 |
| 并发与竞争 | 用例是串行执行的 | 自己写并发压测 |
| 崩溃恢复 | 不会重启驱动 | 杀掉容器，看 `state.json` 能否恢复 |
| 大容量、配额 | 只用几百 MB 到 2Gi 的卷 | 单独测边界 |
| 拓扑正确性 | 不验证调度结果 | 用 `WaitForFirstConsumer` 的 StorageClass 走一遍 |

**最重要的遗漏是并发**。[03 篇](/posts/csi-03-state/)讲的「两个同名 `CreateVolume` 并发」这种场景，csi-sanity 测不出来，因为它的用例是顺序执行的。写驱动时必须自己补并发测试。

## 六、写自己驱动时的建议流程

把这一篇和前面的内容合起来，验收一个驱动可以按这个顺序：

1. **先跑 csi-sanity**，把协议层的错误清掉。这一步不需要集群，只要驱动能起来；
2. **再加 sidecar 跑 [08 篇](/posts/csi-08-lifecycle/)那条时间线**，确认 PVC 能走到 `Bound`、Pod 能挂上；
3. **然后测边界**：删 Pod 时正在写、扩容时有 IO、快照时卷在挂载中；
4. **最后测并发**：同时创建多个同名卷、同时删同一个卷。

**csi-sanity 能过，只说明协议层对了**，离能用还有距离。但反过来，它过不了，就一定有问题——而且往往是最难排查的那类（sidecar 反复重试、Pod 卡在 `ContainerCreating`），所以在第 1 步做完最省时间。

## 七、系列小结

十篇走下来，CSI 这套设计的主线其实只有一条：**驱动是一个被动的 gRPC 服务端，它的行为边界由能力表声明。**

1. 能力表决定谁调用你：[01 篇](/posts/csi-01-spec/)；
2. 调用从哪来：sidecar watch 对象，kubelet 在挂载时连上来：[00 篇](/posts/csi-00-journey/)；
3. 状态怎么存，决定了能不能扩副本：[03 篇](/posts/csi-03-state/)；
4. 幂等和错误码，决定了调用方重试还是放弃：[04 篇](/posts/csi-04-create-delete/)；
5. 接口实现多少，取决于后端是什么：hostpath 全实现，nfs 只做必要的：[09 篇](/posts/csi-09-compare/)；
6. 怎么验收：先 csi-sanity，再走时间线，最后补并发和边界：[本篇](/posts/csi-10-sanity/)。

下一步如果要做真实驱动，两份代码的分工是明确的：**结构从 nfs 抄，Controller 侧的接口细节从 hostpath 抄，然后按自己后端的性质删掉不需要的能力。**
