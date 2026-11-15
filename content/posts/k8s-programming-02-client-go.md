---
title: 'Kubernetes 编程（二）：用 client-go 操作资源'
date: 2026-11-15T21:10:00+08:00
draft: false
tags: ["kubernetes", "client-go", "源码阅读", "go"]
categories: ["Kubernetes"]
series: ["kubernetes-programming"]
author: "LinPr"
description: "kubectl 的每条命令最终都是一个发往 API Server 的 HTTP 请求。本文从 kubectl -v8 的日志出发，讲清楚 Kubernetes 资源的 URL 是怎么组织的（GVR、核心组的特殊路径），再结合 client-go 官方示例，看 Clientset 如何封装这些请求，以及更新时为什么要用 RetryOnConflict。"
---

[上一篇](/posts/k8s-programming-01-overview/)说过，Kubernetes 的大部分扩展都建立在「一切皆资源，资源通过 RESTful API 暴露」之上。这一篇就从最基础的地方开始：资源到底是什么，程序怎么操作它。

## 一、kubectl 到底做了什么

先别急着写代码，看看 `kubectl` 自己是怎么工作的。给命令加上 `-v8`，它会打印出底层的 HTTP 交互：

```bash
kubectl get svc kubernetes -v8
```

输出整理后是这样（省略了部分请求头和响应体）：

```text
loader.go:373]          Config loaded from file:  /home/lin/.kube/config
round_trippers.go:463]  GET https://127.0.0.1:40029/api/v1/namespaces/default/services/kubernetes
round_trippers.go:469]  Request Headers:
round_trippers.go:473]      Accept: application/json;as=Table;v=v1;g=meta.k8s.io,application/json;as=Table;v=v1beta1;g=meta.k8s.io,application/json
round_trippers.go:473]      User-Agent: kubectl/v1.27.2 (linux/amd64) kubernetes/7f6f68f
round_trippers.go:574]  Response Status: 200 OK in 10 milliseconds
round_trippers.go:580]      Content-Type: application/json
round_trippers.go:580]      Audit-Id: aaa37036-69be-438f-890e-c25368b1c16c
request.go:1188]        Response Body: {"kind":"Table","apiVersion":"meta.k8s.io/v1", ... }
NAME         TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)   AGE
kubernetes   ClusterIP   10.96.0.1    <none>        443/TCP   17m
```

从这几行可以读出 `kubectl` 做的全部事情：

1. **加载 kubeconfig。** 默认是 `~/.kube/config`，里面有 API Server 的地址、CA 证书，以及客户端证书或 token；
2. **发一个 HTTP GET 请求**，路径是 `/api/v1/namespaces/default/services/kubernetes`；
3. **在请求头里声明想要表格格式。** `Accept: application/json;as=Table;...` 让服务端直接返回一个 `Table` 对象（`kind: Table`），列定义和行数据都由 API Server 算好；
4. **把表格打印到终端**，也就是最后两行。

第 3 点很有意思：`kubectl get` 的列不是客户端写死的，而是服务端返回的。这也是为什么 CRD 可以通过 `additionalPrinterColumns` 自定义 `kubectl get` 显示哪些列。

用 `curl` 带上同样的证书和路径，也能拿到一样的数据。所以 **`kubectl` 操作资源，本质上就是向 API Server 发 HTTP 请求。** Kubernetes 的 API 遵循 RESTful 风格（见官方文档 [Kubernetes API Concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/)）：URL 路径标识一个资源，HTTP 方法表示对它的操作。

| HTTP 方法 | 操作 | 对应的 kubectl |
| --- | --- | --- |
| GET（单个对象） | 读取 | `kubectl get pod foo` |
| GET（集合） | 列出 | `kubectl get pods` |
| GET `?watch=true` | 监听变化 | `kubectl get pods -w` |
| POST | 创建 | `kubectl create` |
| PUT | 整体替换 | `kubectl replace` |
| PATCH | 部分修改 | `kubectl patch`、`kubectl apply` |
| DELETE | 删除 | `kubectl delete` |

## 二、资源的 URL 是怎么组织的

Kubernetes 内置了几十种资源，还能通过 CRD 无限扩展，必须有一套规则来组织它们。这就像电脑上用目录组织文件：文件放在文件夹里，文件夹还能嵌套，最后用一个完整路径唯一定位到一个文件。

Kubernetes 要解决三个问题：

1. **作用域。** 有些资源属于某个命名空间（Pod、Service），叫 Namespace Scoped；有些属于整个集群（Node、PersistentVolume、Namespace 本身），叫 Cluster Scoped；
2. **分组。** 功能相关的资源放在同一个组（Group）里，比如 `apps` 组有 Deployment、StatefulSet；
3. **版本。** API 在演进，同一个资源可能同时有 `v1beta1` 和 `v1` 两个版本。

于是 URL 被设计成：

```text
# 集群级资源
/apis/<GROUP>/<VERSION>/<RESOURCE>/<NAME>

# 命名空间级资源
/apis/<GROUP>/<VERSION>/namespaces/<NAMESPACE>/<RESOURCE>/<NAME>
```

**核心组（core group）是个例外。** Pod、Service、ConfigMap 这些最早期的资源属于核心组，这个组的名字是空字符串。由于历史原因，它的前缀是 `/api` 而不是 `/apis`，路径里也没有组名：

```text
/api/<VERSION>/<RESOURCE>/<NAME>                            # 集群级，如 /api/v1/nodes/node-1
/api/<VERSION>/namespaces/<NAMESPACE>/<RESOURCE>/<NAME>     # 命名空间级，如上面的 /api/v1/namespaces/default/services/kubernetes
```

所以一个资源类型由 **Group、Version、Resource** 三者唯一确定，简称 **GVR**，比如 `apps/v1/deployments`、`/v1/services`（核心组的 Group 为空）。

还有一个相近的概念叫 **GVK**（Group、Version、Kind）。两者的区别是：

- **Resource** 是 URL 里的那一段，小写复数，如 `deployments`；
- **Kind** 是对象 JSON 里 `kind` 字段的值，首字母大写单数，如 `Deployment`，对应 Go 代码里的结构体类型。

一个 Kind 通常对应一个 Resource，但 Resource 还可以有子资源，比如 `pods/log`、`pods/exec`、`deployments/scale`，它们没有独立的 Kind。客户端拿到一个 YAML（只知道 GVK）时，要先通过 API Server 的发现接口（`/api`、`/apis`）查出它对应的 GVR，才能拼出 URL。`kubectl apply -f` 背后就有这一步，负责做这件事的是 RESTMapper。

## 三、为什么需要客户端库

理论上，用任何语言的 HTTP 库都能直接操作 Kubernetes：拼 URL、设置证书、构造请求体、解析响应。但这样太繁琐了：认证方式有好几种，响应要反序列化成结构体，watch 是一个长连接上的流式响应，还要处理重试和限流。

所以各语言社区都基于 HTTP 封装了客户端库，官方列表在 [Client Libraries](https://kubernetes.io/docs/reference/using-api/client-libraries/)。其中 Go 的 [client-go](https://github.com/kubernetes/client-go) 是功能最完整、抽象层次最高的。Kubernetes 自己的所有组件（kube-controller-manager、kube-scheduler、kubelet、kubectl）都用它和 API Server 交互，它是和 Kubernetes 一起开发、一起发布的。

## 四、用 client-go 操作 Deployment

下面以 client-go 仓库里的官方示例 [create-update-delete-deployment](https://github.com/kubernetes/client-go/blob/master/examples/create-update-delete-deployment/main.go) 为例。准备工作：一个 Go 开发环境，以及一个可用的集群（本地可以用 [kind](https://kind.sigs.k8s.io/docs/user/quick-start/)）。

### 4.1 导入的包

```go
import (
    appsv1 "k8s.io/api/apps/v1"                     // apps 组 v1 版本的类型，如 Deployment
    apiv1 "k8s.io/api/core/v1"                      // 核心组 v1 版本的类型，如 Pod、Container
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"   // 所有对象通用的元数据：ObjectMeta、ListOptions 等
    "k8s.io/client-go/kubernetes"                   // Clientset
    "k8s.io/client-go/tools/clientcmd"              // 加载 kubeconfig
    "k8s.io/client-go/util/retry"                   // 冲突重试
)
```

注意 `k8s.io/api` 的目录结构就是 `<group>/<version>`，和上一节的 GVR 一一对应。核心组在代码里用 `core` 作为目录名，但它在 API 里的组名是空字符串。

### 4.2 创建 Clientset

```go
// 1. 找到 kubeconfig：命令行参数，或默认的 $HOME/.kube/config
kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "...")
flag.Parse()

// 2. 解析成 rest.Config：API Server 地址、TLS 证书、token 等
config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)

// 3. 用它创建 Clientset
clientset, err := kubernetes.NewForConfig(config)
```

这一步对应 `kubectl` 日志的第一行 `Config loaded from file`。

如果程序运行在集群里的 Pod 中，就不需要 kubeconfig，改用 `rest.InClusterConfig()`。它会读取 ServiceAccount 自动挂载的 token 和 CA 证书（`/var/run/secrets/kubernetes.io/serviceaccount/`），以及环境变量里的 API Server 地址。

### 4.3 Clientset 是什么

`Clientset` 是一组客户端的集合，每个 Group/Version 一个：

```go
// Clientset contains the clients for groups.
type Clientset struct {
    *discovery.DiscoveryClient
    appsV1        *appsv1.AppsV1Client
    batchV1       *batchv1.BatchV1Client
    coreV1        *corev1.CoreV1Client
    networkingV1  *networkingv1.NetworkingV1Client
    rbacV1        *rbacv1.RbacV1Client
    storageV1     *storagev1.StorageV1Client
    // ... 几十个
}
```

为什么要按组拆开，而不是用一个通用的 HTTP 客户端？看 `AppsV1Client` 初始化时做了什么：

```go
// k8s.io/client-go/kubernetes/typed/apps/v1/apps_client.go
func setConfigDefaults(config *rest.Config) {
    gv := appsv1.SchemeGroupVersion      // group=apps, version=v1
    config.GroupVersion = &gv
    config.APIPath = "/apis"
    // ...
}
```

再对比 `CoreV1Client`：

```go
// k8s.io/client-go/kubernetes/typed/core/v1/core_client.go
func setConfigDefaults(config *rest.Config) {
    gv := corev1.SchemeGroupVersion      // group="", version=v1
    config.GroupVersion = &gv
    config.APIPath = "/api"              // 核心组走 /api
    // ...
}
```

每个组的客户端在创建时就把 URL 的前缀（`/apis/apps/v1` 或 `/api/v1`）固定下来了，上一节说的核心组特例也直接体现在这里。使用时只需要再指定资源和命名空间：

```go
deploymentsClient := clientset.AppsV1().Deployments(apiv1.NamespaceDefault)
// 之后的请求都发往 /apis/apps/v1/namespaces/default/deployments
```

这样做的好处是类型安全：`Deployments()` 返回的客户端只接受 `*appsv1.Deployment`，传错类型在编译期就会报错。代价是只能操作编译时已知的类型。要操作 CRD 这种编译时不知道结构的资源，就要用 **dynamic client**，它以 `unstructured.Unstructured`（本质是 `map[string]interface{}`）表示对象，按 GVR 发请求。

### 4.4 创建

```go
deployment := &appsv1.Deployment{
    ObjectMeta: metav1.ObjectMeta{
        Name: "demo-deployment",
    },
    Spec: appsv1.DeploymentSpec{
        Replicas: int32Ptr(2),
        Selector: &metav1.LabelSelector{
            MatchLabels: map[string]string{"app": "demo"},
        },
        Template: apiv1.PodTemplateSpec{
            ObjectMeta: metav1.ObjectMeta{
                Labels: map[string]string{"app": "demo"},
            },
            Spec: apiv1.PodSpec{
                Containers: []apiv1.Container{{
                    Name:  "web",
                    Image: "nginx:1.12",
                    Ports: []apiv1.ContainerPort{{
                        Name:          "http",
                        Protocol:      apiv1.ProtocolTCP,
                        ContainerPort: 80,
                    }},
                }},
            },
        },
    },
}

result, err := deploymentsClient.Create(context.TODO(), deployment, metav1.CreateOptions{})
```

这里的结构体和平时写的 YAML 字段一一对应。`kubectl apply -f deployment.yaml` 是把 YAML 序列化后放进 HTTP 请求体，这里是把 Go 结构体序列化后放进去，两者最终发出的请求是一样的。

### 4.5 更新：为什么要 RetryOnConflict

```go
retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
    // 每次重试都重新 Get，拿到最新版本
    result, getErr := deploymentsClient.Get(context.TODO(), "demo-deployment", metav1.GetOptions{})
    if getErr != nil {
        return getErr
    }
    result.Spec.Replicas = int32Ptr(1)
    result.Spec.Template.Spec.Containers[0].Image = "nginx:1.13"
    _, updateErr := deploymentsClient.Update(context.TODO(), result, metav1.UpdateOptions{})
    return updateErr
})
```

这里涉及 Kubernetes 的并发控制。每个对象的 `metadata.resourceVersion` 是一个版本号，对象每被修改一次，它都会变（底层来自 etcd 的修订号）。

`Update` 是「读-改-写」：先 `Get` 到对象，修改几个字段，再整体提交。提交时请求体里带着 `Get` 时拿到的 `resourceVersion`。API Server 会检查它和存储中的当前版本是否一致：

- 一致，说明这期间没人改过，更新成功，版本号前进；
- 不一致，说明别人已经改过了，拒绝这次更新，返回 **409 Conflict**。

这是**乐观并发控制**：不加锁，假设冲突很少，提交时才检查，冲突了由客户端重试。它避免了「后提交的人覆盖前一个人修改」的问题。比如两个控制器同时改一个 Deployment，一个改副本数，一个改镜像，如果不检查版本，后提交的一方会把前一方的修改悄悄覆盖掉。

所以更新要放在 `RetryOnConflict` 里，**并且每次重试都要重新 `Get`**，基于最新版本再改一次。只重试 `Update` 而不重新 `Get` 是没用的，版本号永远对不上。

一个细节：示例代码的注释说 `RetryOnConflict uses exponential backoff`，但 `retry.DefaultRetry` 的定义其实是：

```go
// k8s.io/client-go/util/retry/util.go
var DefaultRetry = wait.Backoff{
    Steps:    5,
    Duration: 10 * time.Millisecond,
    Factor:   1.0,
    Jitter:   0.1,
}
```

`Factor` 是 1.0，也就是每次固定等约 10 毫秒，最多 5 次，并不是指数退避。需要指数退避可以换成 `retry.DefaultBackoff`（`Factor: 5.0`）。

如果只想改几个字段、不关心其他字段，用 `Patch` 或 Server-Side Apply（`Apply`）可以完全避开这个问题：请求里只带要改的字段，由服务端合并，不需要先 `Get`，也就不存在版本冲突。

### 4.6 列出与删除

```go
list, err := deploymentsClient.List(context.TODO(), metav1.ListOptions{})
for _, d := range list.Items {
    fmt.Printf(" * %s (%d replicas)\n", d.Name, *d.Spec.Replicas)
}

deletePolicy := metav1.DeletePropagationForeground
err = deploymentsClient.Delete(context.TODO(), "demo-deployment", metav1.DeleteOptions{
    PropagationPolicy: &deletePolicy,
})
```

只删了 Deployment，它创建的 ReplicaSet 和 Pod 是怎么被删掉的？靠的是 **ownerReferences 和垃圾回收器**。ReplicaSet 的 `metadata.ownerReferences` 指向 Deployment，Pod 的指向 ReplicaSet。kube-controller-manager 里的垃圾回收控制器发现属主不存在了，就删除这些附属对象。`PropagationPolicy` 决定了删除顺序：

| 策略 | 行为 |
| --- | --- |
| `Foreground` | 先删附属对象，全部删完后才删属主。删除期间属主还在，带着 `deletionTimestamp` 和 `foregroundDeletion` finalizer |
| `Background`（默认） | 立即删属主，附属对象由垃圾回收器在后台清理 |
| `Orphan` | 只删属主，附属对象保留，并移除它们的 ownerReferences |

和它相关的还有 **finalizer**：对象上只要还有 finalizer，删除请求就只会给它打上 `deletionTimestamp`，不会真正删除，直到负责的控制器完成清理并移除 finalizer。这是控制器在资源删除前做清理工作的标准方式，后面讲 Operator 时会用到。

## 五、总结

- `kubectl` 的每条命令本质上都是一个发往 API Server 的 HTTP 请求，用 `-v8` 可以看到完整过程。
- 资源 URL 由 Group、Version、Resource（GVR）和命名空间组成。核心组是例外，前缀是 `/api` 且没有组名。GVK 描述对象的类型，GVR 描述 URL，两者之间靠 RESTMapper 转换。
- client-go 的 Clientset 按 Group/Version 拆分客户端，在初始化时固定 URL 前缀，提供类型安全的接口。编译时未知的类型（如 CRD）用 dynamic client。
- 更新依赖 `resourceVersion` 做乐观并发控制，冲突时返回 409。用 `RetryOnConflict` 重试，每次都重新 `Get`；只改部分字段时优先用 Patch 或 Server-Side Apply。
- 删除时附属对象由垃圾回收器根据 ownerReferences 清理，`PropagationPolicy` 控制顺序；finalizer 让控制器有机会在删除前做清理。

`kubectl` 本身就是 client-go 的一个封装：它把这些增删改查包装成了更友好的命令行交互。下一篇看看怎么通过插件扩展 `kubectl`。不过 client-go 能做的远不止增删改查，它最精华的部分是 informer，[第四篇](/posts/k8s-programming-04-controller-informer/)会专门讲。
