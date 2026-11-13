---
title: '让 Grafana 原生查 OpenObserve'
date: 2026-11-06T23:10:00+08:00
draft: false
tags: ["grafana", "openobserve", "可观测性", "go"]
categories: ["可观测性"]
author: "LinPr"
description: "给 OpenObserve 写了个 Grafana 数据源插件，日志、指标、链路都能在 Grafana 里直接查。"
---

我日常的可观测性栈是分开的：指标和日志主要看 Grafana，链路有时候在 Jaeger 里看，有些环境用的是 OpenObserve。

分开用久了会有个很现实的问题：**排查一个问题要在两三个界面之间来回切，时间范围还得手动对齐。**

OpenObserve 官方有个 Grafana 插件，能用，但它的界面更像是在 Grafana 里嵌了一套 OpenObserve 的 API 表单——你得自己知道该往哪个字段填什么。我想要的是另一种：打开 Grafana、选数据源、写 SQL，跟用别的数据源没有区别。

所以就自己写了一个：[grafana-openobserve-datasource](https://github.com/LinPr/grafana-openobserve-datasource)。

{{< figure src="/images/posts/openobserve/explore_logs.webp" caption="在 Explore 里查日志：等级颜色、展开看原始字段" >}}

## 先说清楚定位

它是**非官方**的。官方插件在 [openobserve/openobserve-grafana-plugin](https://github.com/openobserve/openobserve-grafana-plugin)。

两者的取向不一样：官方插件倾向于把 OpenObserve 的 API 原样暴露；这个插件倾向于**让 OpenObserve 看起来像 Grafana 的原生数据源**——该出什么图、该怎么分 series、时间范围怎么处理，都按 Grafana 的规矩来。

插件 ID 是 `linpr-openobserve-datasource`。前缀 `linpr` 是 Grafana Cloud 的 organization slug，插件目录要求这么命名；但后端可执行文件保留了 `grafana-` 前缀，所以在 `ps` 里或者翻进程目录时还能一眼认出这是个 Grafana 插件。

## 三种查询，一个编辑器

顶部第一行是三个关键选择：**stream 类型**（Logs / Metrics / Traces）、**查询模式**（SQL / PromQL）、**编辑器模式**（Builder / Code）。

{{< figure src="/images/posts/openobserve/query_editor.webp" caption="查询编辑器：类型、模式、Builder/Code 三组开关都在最上排" >}}

Builder 和 Code 是互相独立的：**你在 Builder 里点选选项，不会覆盖你已经写好的 SQL。** 这一点我在设计时坚持了很久——很多插件的 Builder 一旦切换过来就把手写语句冲掉了，用起来很难受。

选择顺序也调过：先选类型和模式（决定查询语义），再选 stream，然后是 Raw/Aggregate，最后才是 limit。limit 放在最后，因为它是一个"结果裁剪"，不该和决定查询形状的东西混在一起。

## 日志：等级颜色，原始字段都在

日志按 level 上色，展开一行能看到 OpenObserve 里原始的每一列：

{{< figure src="/images/posts/openobserve/explore_logs.webp" caption="展开日志行，所有原始字段作为 attribute 展示" >}}

## 指标：SQL 里 GROUP BY 自动变成多个 series

这个是让我最满意的部分。

```sql
SELECT histogram(_timestamp) as ts, avg(usage) as usage
FROM metrics
GROUP BY service_name, ts
```

`GROUP BY service_name` 在 Grafana 里直接变成多条带标签的曲线，不需要配 transform，也不需要在 SQL 里做别名 hack：

{{< figure src="/images/posts/openobserve/explore_metrics_sql.webp" caption="SQL + GROUP BY，自动拆成多条 series" >}}

如果更习惯 PromQL，也能用——走的是 OpenObserve 的 Prometheus 兼容接口，`rate()`、`histogram_quantile()`、标签匹配器都正常工作：

{{< figure src="/images/posts/openobserve/explore_promql.webp" caption="PromQL 模式" >}}

## 链路：三种查询方式

链路这块我照抄了 Jaeger 的三种模式，因为那三种模式基本覆盖了实际排查的所有场景：

| 模式 | 用途 |
|---|---|
| **Search** | 按条件筛出 trace 列表，点 trace id 跳进去 |
| **TraceID** | 加载单条 trace 的所有 span，Explore 直接画瀑布图 |
| **Graph** | 从 span 的父子关系推导服务间调用次数，画成节点图 |

{{< figure src="/images/posts/openobserve/trace_search.webp" caption="Trace 搜索" >}}

{{< figure src="/images/posts/openobserve/trace_waterfall.webp" caption="单条 trace 的瀑布图" >}}

Graph 模式有个细节：服务名取的是 OpenObserve 推断出来的**被调用方**，而不是调用方。这样 client span 会归属到它实际调用的服务上，服务图才符合直觉——否则整张图会变成"所有调用都指向自己"。

{{< figure src="/images/posts/openobserve/trace_graph.webp" caption="服务调用图" >}}

## 一个面板里混着日志、指标、链路

因为是同一个数据源，所以可以在一个 dashboard 里放日志、时间序列、饼图、柱状图和 trace 表：

{{< figure src="/images/posts/openobserve/dashboard_logs_metrics.webp" caption="日志 + 指标 + 链路混排的 dashboard" >}}

这正是我最初想要的东西——**排查时不用切界面，也不用对齐时间范围。**

## 模板变量就是一条 SQL

Grafana 的变量在这个插件里不需要记特殊的语法，写 SQL 就行，第一列会作为值：

```sql
SELECT DISTINCT service_name FROM logs ORDER BY service_name
```

多选时会被正确地引号包裹成 `IN (...)`，不需要自己拼。

## 安装

四种方式，按你的 Grafana 环境选：

**1. Grafana CLI 直接从 GitHub 装**（最快）

```bash
grafana cli \
  --pluginsDir /var/lib/grafana/plugins \
  --pluginUrl https://github.com/LinPr/grafana-openobserve-datasource/releases/download/v<version>/grafana-openobserve-datasource-<version>.zip \
  plugins install linpr-openobserve-datasource
```

注意这里的 URL 是 **release 的资源地址**（`releases/download/<tag>/<file>`），不是 release 页面。指到 HTML 页面会报一个很误导人的 `Could not process plugin archive: Error reading file`。

`--pluginUrl` 只是绕过了签名检查，Grafana 本身仍然拒绝加载未签名插件，所以还要放开：

```bash
export GF_PLUGINS_ALLOWING_UNSIGNED_PLUGINS=linpr-openobserve-datasource
```

**2. 解压到插件目录**

```bash
sudo unzip grafana-openobserve-datasource-<version>.linux-amd64.zip -d /var/lib/grafana/plugins/
export GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=linpr-openobserve-datasource
```

**3. 容器镜像**

```bash
ghcr.io/linpr/grafana-openobserve-datasource
```

**4. 从源码构建**（前后端都要）

发布包按平台拆分：linux-amd64 / linux-arm64 / linux-arm / darwin-amd64 / darwin-arm64 / windows-amd64，另外有一个包含全部平台的通用包，给插件目录用。

最低 Grafana 版本 10.4.0。

## 配置数据源

填上 OpenObserve 的地址和组织名，选认证方式（basic auth 或 token）：

{{< figure src="/images/posts/openobserve/config_editor.webp" caption="数据源配置页" >}}

保存后点 "Save & test"，它会去探一下 `/api/{org}/streams` 确认连通和权限。

## 一点实现上的说明

后端是 Go（用 Grafana 的 `grafana-plugin-sdk-go`），前端是 TypeScript + React。

关键设计是**后端负责把三种查询翻译成 OpenObserve 能吃的 SQL**，前端只负责收集参数。这样：
- 前端不需要知道 OpenObserve 的查询语法细节
- 加新的查询类型只要改后端
- 所有请求都经过 Grafana 的后端代理，凭证不会暴露在浏览器里

链路那三种模式其实生成的是三段不同的 SQL，前端只是切换表单字段。

---

写这个插件的起点是"不想再在三个界面之间切了"。做到最后，最花时间的其实是那些不显眼的地方：时间范围怎么传给 OpenObserve、`GROUP BY` 怎么变成 series、服务图里的服务名该取谁的、未签名插件怎么在容器里加载。

如果你也在用 OpenObserve，又习惯 Grafana 的界面，可以试试。

仓库在 [LinPr/grafana-openobserve-datasource](https://github.com/LinPr/grafana-openobserve-datasource)，截图都是从真实环境（Grafana 12.2 + OpenObserve）里截的，不是 mockup。
