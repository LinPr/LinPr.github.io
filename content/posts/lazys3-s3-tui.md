---
title: '在终端里翻 S3：lazys3'
date: 2026-11-06T22:20:00+08:00
draft: false
tags: ["go", "tui", "s3", "对象存储"]
categories: ["开源项目"]
author: "LinPr"
description: "用惯了文件管理器的人，第一次面对 aws s3 ls 是会骂人的。"
---

> 用惯了 `ls` 和 `cd` 的人，第一次面对 `aws s3 ls s3://bucket/prefix/` 大概都会愣一下。

不是说命令不好用，是它不给人"空间感"。你在一个有几万个对象的桶里，想知道 `reports/2024/` 下面到底有什么，得先 `ls` 一层，再 `ls` 下一层，中间还得手动拼路径。想看一个文件内容，得先 `aws s3 cp` 下载下来再看，看完删掉。想清理一批上传失败留下的 multipart，得去查文档。

[lazys3](https://github.com/LinPr/lazys3) 就是把这些操作变成一个能上下移动的界面。

{{< figure src="/images/posts/lazys3/start.webp" caption="打开就直接在桶里，不需要先 ListBuckets" >}}

## 第一件事：它不要求你能列桶

这是我在设计时最先想清楚的一个点。

很多 S3 工具启动时会先调 `ListBuckets`，然后让你从桶列表里选。这个流程在有权限的情况下很自然，但现实里经常不是这样：**很多生产环境的凭证被严格限制了，只允许访问某一个桶的某一个前缀，`ListBuckets` 直接 403。**

所以 lazys3 支持给每个 profile 配一个"起始位置"。启动时直接进那个 `s3://bucket/prefix/`，完全不碰 `ListBuckets`。

配置写法很朴素，就是在自己的配置文件里给 profile 指定一个起始路径：

```toml
[profiles.prod]
start = "s3://logs/2026/11/"
```

配合 `lazys3 --profile prod` 使用。对只能看一个前缀的凭证来说，这是唯一能用的方式。

## 浏览：像文件管理器一样

`enter` 进入、`backspace` 返回，前缀当目录处理。`/` 过滤、`o` / `O` 按名字/大小/时间排序、`space` 多选、`a` 全选。

{{< figure src="/images/posts/lazys3/search.webp" caption="递归搜索整个桶，直接跳到第一个匹配的 key" >}}

`F` 是递归搜索——在几万个 key 里找一个东西，不用一层层点进去。找到之后直接跳到那个前缀。

## 不下载就能看内容

这个功能用得最多。

按 `p`，把文件的前 256 KiB 流式读进一个浮层里，可以滚动：

{{< figure src="/images/posts/lazys3/preview.webp" caption="预览浮层：只读前 256 KiB，二进制和空文件会提前识别" >}}

以前查看一个 S3 上的日志或者配置文件，流程是 `cp` 下来、`cat`/`vim` 看、`rm` 删掉——为了看一眼内容，传输了一个完整的文件，还得清理。

现在是按一个键，等半秒，看完按 `esc`。

二进制文件会提前识别出来，不会给你糊一屏乱码。空文件也会提示。

## 元数据：不只有大小

按 `m` 看全部字段：

{{< figure src="/images/posts/lazys3/metadata.webp" caption="对象的 HeadObject 全量字段，包括 SSE、checksum 和自定义 metadata" >}}

Content-Type、ETag、存储类、服务端加密（SSE）、校验和、user metadata 都在这里。排查"为什么这个文件被浏览器当成了下载而不是预览"这类问题，看 Content-Type 就够了；排查"这个对象的加密方式对不对"，看 SSE。

对桶来说显示的是 region 和版本控制状态；对本地文件，显示路径、权限、所有者、时间戳和符号链接目标。

## 双栏：本地和远端并排

按 `l` 打开双栏，左边本地、右边 S3，`Tab` 切换焦点，所有文件操作都作用在当前焦点那一栏：

{{< figure src="/images/posts/lazys3/dual.webp" caption="双栏模式，`tab` 切换焦点，上传下载跟着焦点走" >}}

这是我觉得最"文件管理器"的部分。上传一个目录到某个前缀下，就是选中、按 `u`；下载回来就是 `d`。目录是递归处理的，不用加 `--recursive`。

按 `g` 可以直接跳路径。`s3://bucket/prefix/` 或者本地绝对路径都行。

## 传输：有进度，能取消

`t` 打开传输面板，所有上传、下载、同步任务按时间倒序排列，带进度条，`x` 取消正在跑的那个。

{{< figure src="/images/posts/lazys3/transfers.webp" caption="传输面板：进度、状态，可以单独取消" >}}

以前在终端里做大批量传输，最难受的是"不知道还要多久"和"想取消只能 Ctrl+C 全部掐掉"。

## 管理操作

建桶、建目录（`B`）、重命名（`r`）、复制（`c`）、递归删除（`D`）都有。

删除一律走确认弹窗，`tab` 换按钮，`y` / `n` 直接答，`esc` 取消。这个设计是刻意的——在 TUI 里误触一个 `D` 就递归删掉一整个前缀，代价太大了。

还有一个不太起眼但很实用的功能：`P` 清理未完成的 multipart 上传。

分片上传中断之后，那些分片不会出现在对象列表里，但**一直按存储计费**。时间长了会变成一笔莫名其妙的账单，而且用普通的 `ls` 完全看不出来。这个面板能列出所有未完成的 multipart 并中止掉。

## 兼容性

走的是标准 S3 协议，所以除了 AWS S3，阿里云 OSS、华为云 OBS、腾讯云 COS、MinIO、Ceph 这些都能用。

寻址方式会自动判断：AWS 和 OSS 用 virtual-host style，其他自定义 endpoint 用 path-style。OSS 有特殊处理——它拒绝 path-style，所以单独判了一下。

多 profile 直接读 `~/.aws/config` 和 `~/.aws/credentials`，包括每个 profile 自己的 `endpoint_url`。不需要额外维护一份配置。

## 装起来

```bash
go install github.com/LinPr/lazys3@latest
```

或者去 [Releases](https://github.com/LinPr/lazys3/releases/latest) 下静态二进制，Linux / macOS / Windows，amd64 / arm64 都有。

```bash
lazys3                    # 用默认 profile
lazys3 --profile prod     # 指定 profile
lazys3 s3://bucket/prefix # 直接进某个前缀
```

---

我对这类工具的判断标准很简单：**能不能让我少打几次命令，但又不隐藏我到底在做什么。**

lazys3 没有把 S3 抽象成一个本地文件系统（那样会丢语义、也会误导人），它只是把"列、看、传、删"这四件事的交互做顺了。你看到的还是 bucket 和 key，只是不用再拼路径了。

仓库在 [LinPr/lazys3](https://github.com/LinPr/lazys3)。
