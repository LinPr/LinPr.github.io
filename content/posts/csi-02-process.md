---
title: 'csi 02 驱动进程：从命令行参数到 unix socket，再到自我介绍'
date: 2026-11-28T17:40:00+08:00
draft: false
tags: ["kubernetes", "csi", "存储", "gRPC", "csi-driver-host-path"]
categories: ["Kubernetes"]
series: ["csi"]
author: "LinPr"
description: "hostpath 驱动的启动只有四步：读参数、校验必填项、打开状态文件、起 gRPC server。本文跟着 main、NewHostPathDriver、Run 和 serve 走一遍，说明哪些参数会改变驱动行为、一个 socket 怎么同时挂五个 service、退出时 graceful stop 做了什么，以及 logGRPC 这个拦截器为什么是排查问题的第一现场。"
---

[上一篇](/posts/csi-01-spec/)说清了规范要求驱动提供什么。这一篇看最基础的一件事：**这个进程是怎么起来的**。hostpath 的启动路径只有四个函数，读完就能知道一个 CSI 驱动最少需要什么，也就能判断自己写驱动时哪些代码是必须的、哪些是示例特有的。

## 一、main 只做四件事

```go {linenos=inline}
// cmd/hostpathplugin/main.go
func main() {
	cfg := hostpath.Config{
		VendorVersion: version,
	}

	flag.StringVar(&cfg.Endpoint, "endpoint", "unix:///tmp/csi.sock", "CSI endpoint")
	flag.StringVar(&cfg.DriverName, "drivername", "hostpath.csi.k8s.io", "name of the driver")
	flag.StringVar(&cfg.StateDir, "statedir", "/csi-data-dir", "directory for storing state information across driver restarts, volumes and snapshots")
	flag.StringVar(&cfg.NodeID, "nodeid", "", "node id")
	// ...
	flag.BoolVar(&cfg.EnableAttach, "enable-attach", false, "Enables RPC_PUBLISH_UNPUBLISH_VOLUME capability.")
	flag.BoolVar(&cfg.EnableTopology, "enable-topology", true, "Enables PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS capability.")
	// ...
	flag.Parse()

	// ...（版本号输出、proxy 模式、参数合法性检查）

	driver, err := hostpath.NewHostPathDriver(cfg)
	if err != nil {
		fmt.Printf("Failed to initialize driver: %s", err.Error())
		os.Exit(1)
	}

	// ...
	signal.Notify(stopCh, sigs...)

	if err := driver.Run(stopCh); err != nil {
		fmt.Printf("Failed to run driver: %s", err.Error())
		os.Exit(1)
	}
}
```

四件事按顺序是：

1. 把二十来个 flag 收进一个 `Config` 结构体；
2. 处理两个特殊分支：`--version` 打印版本就退出，`--proxy-endpoint` 进入代理模式（只给 E2E 测试用，不跑驱动本身）；
3. 调 `NewHostPathDriver`（第 19 行）建出驱动对象，失败直接退出；
4. 等信号，交给 `Run`（第 28 行）阻塞运行。

**这份 main 里没有业务逻辑**，真实驱动也是如此。如果你的驱动在 `main` 里写了大段初始化，通常说明它该放进 controller 或 node 的构造函数里。

## 二、哪些参数会改变行为

二十来个 flag 里，大部分只影响示例功能，真正决定行为的是下面六个：

| 参数 | 默认值 | 影响 |
| --- | --- | --- |
| `--endpoint` | `unix:///tmp/csi.sock` | 监听地址。部署时通过 `CSI_ENDPOINT` 环境变量传成 `/csi/csi.sock`（第 7 行） |
| `--drivername` | `hostpath.csi.k8s.io` | 必须和 `CSIDriver`、StorageClass 里的名字一致，否则 provisioner 不认领 PVC（第 8 行） |
| `--nodeid` | 空 | `NodeGetInfo` 返回的节点名，**必填**，空值会让启动失败；部署时取自 `spec.nodeName`（第 10 行） |
| `--statedir` | `/csi-data-dir` | 状态文件与卷数据的根目录（第 9 行） |
| `--enable-attach` | `false` | 声明 `PUBLISH_UNPUBLISH_VOLUME`，决定 attacher 是否工作（第 12 行） |
| `--enable-topology` | `true` | 声明 `VOLUME_ACCESSIBILITY_CONSTRAINTS`，决定拓扑信息是否参与调度（第 13 行） |

前四个决定驱动能不能在集群里被认出来，后两个决定它的能力表怎么填。后两个的连锁反应在[上一篇](/posts/csi-01-spec/)第四节讲过：声明什么，就决定了哪个 sidecar 开始工作。

## 三、初始化：三个必填项、一个目录、一个状态文件

```go {linenos=inline}
// pkg/hostpath/hostpath.go
func NewHostPathDriver(cfg Config) (*hostPath, error) {
	if cfg.DriverName == "" {
		return nil, errors.New("no driver name provided")
	}

	if cfg.NodeID == "" {
		return nil, errors.New("no node id provided")
	}

	if cfg.Endpoint == "" {
		return nil, errors.New("no driver endpoint provided")
	}

	if err := os.MkdirAll(cfg.StateDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create dataRoot: %v", err)
	}

	klog.Infof("Driver: %v ", cfg.DriverName)
	klog.Infof("Version: %s", cfg.VendorVersion)

	s, err := state.New(path.Join(cfg.StateDir, "state.json"))
	if err != nil {
		return nil, err
	}
	hp := &hostPath{
		config: cfg,
		state:  s,
	}
	return hp, nil
}
```

这个函数只做三件事：

1. 校验三个必填项（第 3–13 行）。`DriverName`、`NodeID`、`Endpoint` 缺任何一个都没有意义，所以直接报错退出；
2. 建好状态目录（第 15 行）。`0750` 是硬编码的，因为卷数据会落在这个目录下，不能让其他用户读；
3. 打开 `state.json`（第 22 行）。这一步会读盘、解析 JSON，文件不存在就创建一个空的。

注意第 26–29 行：驱动对象里只有 `config` 和 `state` 两个字段。**没有缓存、没有连接池、没有 informer 客户端**——这是 CSI 驱动和 Kubernetes 控制器最大的区别：它不需要连 apiserver。

## 四、一个 socket 挂五个 service

`Run` 本身只有十几行：

```go {linenos=inline}
// pkg/hostpath/hostpath.go
func (hp *hostPath) Run(stopCh <-chan os.Signal) error {
	s := NewNonBlockingGRPCServer()
	var sms csi.SnapshotMetadataServer
	if hp.config.EnableSnapshotMetadata {
		sms = hp
	}
	s.Start(hp.config.Endpoint, hp, hp, hp, hp, sms)

	<-stopCh
	s.Stop()

	return nil
}
```

第 8 行的五个 `hp` 值得停一下：同一个对象同时作为 Identity、Controller、Node、GroupController 四个 service 的实现传进去。**驱动只是把接口实现挂在同一个结构体上**，不需要为每个 service 单独起进程或单开端口。

真正的注册发生在 `serve` 里：

```go {linenos=inline}
// pkg/hostpath/server.go
func (s *nonBlockingGRPCServer) serve(ep string, ids csi.IdentityServer, cs csi.ControllerServer, ns csi.NodeServer, gcs csi.GroupControllerServer, sms csi.SnapshotMetadataServer) {
	listener, cleanup, err := endpoint.Listen(ep)
	if err != nil {
		klog.Fatalf("Failed to listen: %v", err)
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(logGRPC),
	}
	server := grpc.NewServer(opts...)
	s.server = server
	s.cleanup = cleanup

	if ids != nil {
		csi.RegisterIdentityServer(server, ids)
	}
	if cs != nil {
		csi.RegisterControllerServer(server, cs)
	}
	if ns != nil {
		csi.RegisterNodeServer(server, ns)
	}
	if gcs != nil {
		csi.RegisterGroupControllerServer(server, gcs)
	}
	if sms != nil {
		csi.RegisterSnapshotMetadataServer(server, sms)
	}

	klog.Infof("Listening for connections on address: %#v", listener.Addr())

	server.Serve(listener)
}
```

三个细节：

1. **nil 就不注册**（第 15–29 行）。`Run` 里的 `sms` 在没开 `--enable-snapshot-metadata` 时是 nil，这个 service 就不会出现在 socket 上；客户端调它会收到 `Unimplemented`；
2. `endpoint.Listen` 会先删掉旧的 socket 文件再监听。容器重启时上一个进程的 socket 文件还在，不删会报 `address already in use`；
3. `Start` 起了一个 goroutine 就返回，`Run` 阻塞在 `<-stopCh`（第 10 行）。**驱动进程本身没有主循环**，所有工作都在 gRPC handler 里完成。

还有一个和规范相关的点：`endpoint.Parse` 同时支持 `unix://` 和 `tcp://`。CSI 规范推荐 unix domain socket，主机上的插件也基本都这么用；tcp 只用于调试。

## 五、logGRPC：排查问题的第一现场

`serve` 给 gRPC server 装了一个拦截器，所有调用都会先经过它：

1. 每次调用打一行 `GRPC call: /csi.v1.Controller/CreateVolume`，默认是 `-v=3`；
2. `Probe` 例外，压到 `-v=5`。它每十秒被调一次，按 3 打会把日志刷满；
3. 请求体和响应体只在 `-v=5` 时打印，而且用 `protosanitizer.StripSecrets` 把敏感字段替换掉；
4. 返回错误时**无条件**打 `GRPC error`，不受日志级别限制。

所以排查驱动问题时，第一步永远是 `kubectl logs` 看这个 Pod 的 `-v=5` 输出，确认「哪个请求到了、参数对不对、返回了什么」。没有这条日志，就只能靠猜。

## 六、退出做了两件事

`Stop` 走的是 `GracefulStop`：先停止接受新连接，等正在处理的请求结束，再关掉监听。`serve` 里保存的 `cleanup` 会在最后删掉 socket 文件。

还有一个 `ForceStop`，直接 `server.Stop()`，不等在途请求。它只在测试里用，正常运行用不到。

到这里，驱动已经能起来、能被连上、能回答「我是谁」了。但 `GetPluginInfo` 之外的接口一旦被调用，都会碰到同一个东西：驱动记不记得自己建过哪些卷。下一篇看它在磁盘上存了什么。
