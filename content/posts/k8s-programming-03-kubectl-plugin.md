---
title: 'Kubernetes 编程（三）：编写 kubectl 插件'
date: 2026-11-15T21:20:00+08:00
draft: false
tags: ["kubernetes", "kubectl", "插件", "源码阅读"]
categories: ["Kubernetes"]
series: ["kubernetes-programming"]
author: "LinPr"
description: "kubectl 插件就是 PATH 里一个以 kubectl- 开头的可执行文件。本文从切换默认命名空间这个小需求出发，讲清楚插件的命名规则、安装方式，以及 kubectl 源码里查找和执行插件的过程：为什么插件不能覆盖内置命令，多级子命令怎么匹配，最后为什么用的是 execve 而不是 fork。"
---

[上一篇](/posts/k8s-programming-02-client-go/)说过，`kubectl` 本质上是 client-go 的一层封装。这一篇看看怎么在不改 `kubectl` 源码的前提下，给它加新命令。

## 一、从一个小需求说起：切换默认命名空间

业务通常部署在某个固定的命名空间里，于是每条命令都要带上 `-n`：

```bash
kubectl get pod -n my-namespace
kubectl logs deploy/api -n my-namespace
```

操作一多就很烦。Lens、k9s 这类图形化工具能解决，但我们这里只关心 `kubectl` 本身。

最直接的办法是改 kubeconfig，给当前 context 设一个默认命名空间：

```yaml
apiVersion: v1
kind: Config
clusters:
  - name: your-cluster
    cluster:
      server: https://your-k8s-api-server
contexts:
  - name: your-context
    context:
      cluster: your-cluster
      user: your-user
      namespace: your-namespace   # 默认命名空间
current-context: your-context
users:
  - name: your-user
    user:
      token: your-token
```

`kubectl` 其实也内置了修改它的命令：

```bash
kubectl config set-context --current --namespace=your-namespace
```

但这条命令太长，查看当前命名空间、列出所有命名空间也都不方便。这正是插件适合解决的问题。

## 二、插件长什么样

Kubernetes 官方维护了一个示例插件 [sample-cli-plugin](https://github.com/kubernetes/sample-cli-plugin)，实现的就是切换命名空间：

```bash
# 编译
go build cmd/kubectl-ns.go
# 放到 PATH 里的任意目录
cp ./kubectl-ns /usr/local/bin

# 切换默认命名空间
kubectl ns new-namespace
# 之后的命令都作用在 new-namespace
kubectl get pod
# 列出 kubeconfig 中各 context 使用的命名空间
kubectl ns --list
# 查看当前命名空间
kubectl ns
```

翻它的源码会发现，它做的事和上面手工改 kubeconfig 完全一样，只是封装成了一条顺手的命令。

社区里成熟的插件可以用 [krew](https://krew.sigs.k8s.io/) 安装，它是 `kubectl` 插件的包管理器，作用类似 apt、yum：

```bash
kubectl krew install ns ctx neat tree
```

上面几个都很常用：`ns`/`ctx` 切换命名空间和集群，`neat` 去掉 `kubectl get -o yaml` 输出里的 `managedFields`、`status` 等噪音，`tree` 按 ownerReferences 显示资源的层级关系。

## 三、编写和使用的规则

官方文档 [用插件扩展 kubectl](https://kubernetes.io/zh-cn/docs/tasks/extend-kubectl/kubectl-plugins/) 写得很清楚，归纳下来就几条：

1. **文件名以 `kubectl-` 开头**，后面的部分就是命令名。`kubectl-ns` 对应 `kubectl ns`；
2. **放在 `$PATH` 里的某个目录，并且有可执行权限**；
3. **多级命令用 `-` 连接**：`kubectl-foo-bar` 对应 `kubectl foo bar`；
4. **命令名本身带 `-` 的，文件名里写成 `_`**：`kubectl-foo_bar` 对应 `kubectl foo-bar`；
5. **不能覆盖内置命令**：文件名叫 `kubectl-get`，`kubectl get` 依然执行内置命令。

`kubectl plugin list` 可以列出当前能找到的所有插件，并会对同名冲突、被内置命令遮蔽、没有执行权限等问题给出警告。

插件可以用任何语言写：Shell 脚本、Go 程序、Python 脚本都行，只要能执行。kubectl 会把当前的环境变量传给插件，并额外设置 `KUBECTL_PATH` 指向 `kubectl` 自身的路径。所以插件可以读取 `KUBECONFIG` 等变量，和 `kubectl` 使用同一套配置；Shell 写的插件也可以通过 `$KUBECTL_PATH` 回调 `kubectl`，而不必假设它在 `PATH` 的哪个位置。用 Go 写的话，推荐用 `k8s.io/cli-runtime` 里的 `genericclioptions`，它能直接提供和 `kubectl` 一致的 `--kubeconfig`、`--context`、`-n` 等参数。

上面这些规则，都能在 `kubectl` 源码里找到出处。

## 四、源码：kubectl 怎么找到并执行插件

以下代码位于 `k8s.io/kubectl/pkg/cmd/`。

### 4.1 只有内置命令找不到时才找插件

```go
// pkg/cmd/cmd.go，NewDefaultKubectlCommandWithArgs（节选）
cmdPathPieces := o.Arguments[1:]
// only look for suitable extension executables if
// the specified command does not already exist
foundCmd, foundArgs, err := cmd.Find(cmdPathPieces)
if err != nil {
    // ...
    switch cmdName {
    case "help", cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd:
        // Don't search for a plugin
    default:
        if err := HandlePluginCommand(o.PluginHandler, cmdPathPieces, 1); err != nil {
            fmt.Fprintf(o.IOStreams.ErrOut, "Error: %v\n", err)
            os.Exit(1)
        }
    }
}
```

`kubectl` 先用 cobra 的 `cmd.Find` 在内置命令树里查。**只有找不到时，才去找插件。** 这就是插件无法覆盖内置命令的原因：`kubectl get` 总能在内置命令里找到，根本不会走到插件查找这一步。

有一个例外：

```go
// pkg/cmd/plugin.go
func IsSubcommandPluginAllowed(foundCmd string) bool {
    allowedCmds := map[string]struct{}{"create": {}}
    _, ok := allowedCmds[foundCmd]
    return ok
}
```

`create` 命令允许用插件扩展子命令。`kubectl create foo` 里，`create` 是内置命令，但 `foo` 不是它的内置子命令，这时会去找 `kubectl-create-foo`。这样就能给自己的 CRD 加一个 `kubectl create` 风格的创建命令。

### 4.2 从最长的名字开始匹配

```go
// pkg/cmd/plugin.go
func HandlePluginCommand(pluginHandler PluginHandler, cmdArgs []string, minArgs int) error {
    var remainingArgs []string // all "non-flag" arguments
    for _, arg := range cmdArgs {
        if strings.HasPrefix(arg, "-") {
            break
        }
        remainingArgs = append(remainingArgs, strings.Replace(arg, "-", "_", -1))
    }
    // ...
    // attempt to find binary, starting at longest possible name with given cmdArgs
    for len(remainingArgs) > 0 {
        path, found := pluginHandler.Lookup(strings.Join(remainingArgs, "-"))
        if !found {
            remainingArgs = remainingArgs[:len(remainingArgs)-1]
            // ...
            continue
        }
        foundBinaryPath = path
        break
    }
    // ...
    pluginEnv, err := pluginEnvironment()
    // ...
    // invoke cmd binary relaying the current environment and args given
    if err := pluginHandler.Execute(foundBinaryPath, cmdArgs[len(remainingArgs):], pluginEnv); err != nil {
        return err
    }
    return nil
}
```

这段代码解释了第三节的几条规则：

- **遇到第一个以 `-` 开头的参数就停止**，后面的都当作参数传给插件。所以插件名必须写在所有 flag 前面，`kubectl -n foo ns` 是找不到插件的；
- **参数里的 `-` 被换成 `_`**，所以命令名 `foo-bar` 对应的文件名是 `kubectl-foo_bar`；
- **多个参数用 `-` 拼接，从最长的开始找**。执行 `kubectl foo bar baz` 时，依次尝试 `kubectl-foo-bar-baz`、`kubectl-foo-bar`、`kubectl-foo`，找到哪个就执行哪个，剩下没用掉的部分作为参数传入。

`Lookup` 很简单，就是在 `PATH` 里找可执行文件：

```go
// pkg/cmd/plugin.go
func (h *DefaultPluginHandler) Lookup(filename string) (string, bool) {
    for _, prefix := range h.ValidPrefixes {
        path, err := exec.LookPath(fmt.Sprintf("%s-%s", prefix, filename))
        if shouldSkipOnLookPathErr(err) || len(path) == 0 {
            continue
        }
        return path, true
    }
    return "", false
}
```

`ValidPrefixes` 来自 `ValidPluginFilenamePrefixes = []string{"kubectl"}`。它被定义成切片而不是一个字符串常量，说明设计上预留了支持多个前缀的余地。

### 4.3 执行：直接替换当前进程

```go
// pkg/cmd/plugin.go
func (h *DefaultPluginHandler) Execute(executablePath string, cmdArgs, environment []string) error {
    // Windows does not support exec syscall.
    if runtime.GOOS == "windows" {
        cmd := Command(executablePath, cmdArgs...)
        cmd.Stdout = os.Stdout
        cmd.Stderr = os.Stderr
        cmd.Stdin = os.Stdin
        cmd.Env = environment
        err := cmd.Run()
        if err == nil {
            os.Exit(0)
        }
        return err
    }

    // invoke cmd binary relaying the environment and args given
    // append executablePath to cmdArgs, as execve will make first argument the "binary name".
    return syscall.Exec(executablePath, append([]string{executablePath}, cmdArgs...), environment)
}
```

在类 Unix 系统上，`kubectl` 调用的是 `syscall.Exec`，也就是 `execve` 系统调用。它**不会 fork 出子进程**，而是用插件程序直接替换当前进程的内存映像：进程号不变，标准输入输出、环境变量原样继承，`kubectl` 自己的代码从此不再执行。

这和 C 里的 `system()` 或 `fork()` + `exec()` 不同。那种方式下父进程还在，要等子进程结束、再转发退出码。`execve` 之后，插件就是这个进程本身，它的退出码就是用户在 shell 里看到的退出码，信号（比如 Ctrl+C）也直接送到插件，没有中间层。

Windows 没有 `exec` 系统调用，只能退而起一个子进程，把标准输入输出接过去，等它结束后自己 `os.Exit(0)`。

## 五、总结

- kubectl 插件就是 `PATH` 里一个以 `kubectl-` 开头的可执行文件，可以用任何语言编写。参数原样传入，环境变量也会继承，并额外带上指向 `kubectl` 的 `KUBECTL_PATH`。
- `kubectl` 先在内置命令里查找，找不到才查插件，所以插件不能覆盖内置命令；`create` 是唯一允许扩展子命令的内置命令。
- 多级命令用 `-` 拼接，从最长的名字开始匹配；命令名里的 `-` 在文件名中写成 `_`；插件名要写在所有 flag 之前。
- 在类 Unix 系统上，插件通过 `execve` 直接替换 `kubectl` 进程执行，退出码和信号都直达插件。
- 如果自己要写一个命令行工具，也可以借鉴这种设计：用「`PATH` 里的约定前缀 + 找不到内置命令时回退」的方式，把扩展能力开放给使用者，而无需在主程序里加载任何插件代码。`git` 的 `git-xxx` 子命令就是同样的思路。
