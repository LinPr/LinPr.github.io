# AGENTS.md

本文件给参与维护这个博客的 AI 编程助手（Claude、GPT、Gemini 等）参考。人类读者也可以看。

## 项目概况

- Hugo 静态博客，主题 `themes/hugo-PaperMod`，站点定制在 `layouts/`、`assets/css/extended/`、`assets/js/`。
- 文章在 `content/posts/*.md`，系列说明在 `content/series/<slug>/_index.md`，文章图片放 `static/images/posts/<文章或系列名>/`。
- 推送到 `main` 后由 GitHub Actions（Build and deploy）构建并发布到 GitHub Pages。
- 本地检查构建：`hugo --quiet --destination /tmp/hugo-check`（不要覆盖 `public/`）。
- 文章日期不能晚于当前时间，否则 Hugo 不会生成页面。
- 站点没有数学公式渲染（没有 KaTeX/MathJax），公式请写在 ```text 代码块里，不要用 `$$`。

## 写文章的约定

- 简体中文，客观平实；文章是独立的博客，不能出现「你提到」「用户要求」之类对话痕迹，也不要编造个人经历。
- 讲源码时引用真实代码（可删减，用 `// ...` 表示省略），代码块第一行用注释写相对路径。
- 章节标题用 `## 一、……`、`### 1.1 ……`。
- 插图用 Hugo shortcode：

  ```
  {{< figure src="/images/posts/xxx/yyy.svg" link="/images/posts/xxx/yyy.svg" caption="一句话说明图在表达什么（点击查看大图）" >}}
  ```

## 画示意图

站点里的流程图、数据结构图、打分曲线、算例表都是**用 Python 脚本直接生成的 SVG**，不用绘图库，也不用 Mermaid/Graphviz 这类自动布局工具。这样做的好处是风格统一、体积小、可无损缩放、数值和正文算例完全一致，改图只需改脚本重跑。

工具在 `tools/diagrams/`（Hugo 不会发布这个目录）：

- `svglib.py`：无第三方依赖的小库，提供 `Canvas`（`rect`、`text`、`lines`、`arrow`、`line`、`box`、`chip`、`title`、`save`）、文字宽度估算 `tw()`、自动换行 `wrap()` 和全站配色常量。
- `example.py`：完整示例（带成功/失败分支的判定流程 + 图例），新图可以从它复制开始。

```bash
python3 tools/diagrams/example.py /tmp/diagram-example.svg /tmp/diagram-example.png
```

### 工作流程

1. **先想清楚图要解释什么机制**：决策分支、数据结构、时间线、公式曲线、带数字的算例。不画装饰图；一句话能说清的就不画。
2. **写生成脚本**：脚本放 `/tmp` 或 `tools/diagrams/` 下，输出到 `static/images/posts/<名称>/<编号>-<短名>.svg`。坐标全部由代码计算：用 `tw()` 估算文字宽度决定方框大小，用 `wrap()` 换行，用循环排布重复元素。
3. **校验并截图**：`Canvas.save(svg_path, png_path)` 会校验 XML，并用 headless Chrome 截图：

   ```bash
   google-chrome --headless=new --no-sandbox --screenshot=/tmp/shot.png --window-size=W,H file:///abs/path/to/diagram.svg
   ```

4. **必须打开截图检查**（多模态模型直接读取 PNG）：文字是否溢出方框、元素是否重叠、箭头是否指对位置、有没有多余或错误的连线。发现问题改脚本重跑，直到干净为止。SVG 是「盲写坐标」，不看截图几乎一定会有问题。
5. 在文章里用 `figure` shortcode 引用，caption 写清图在表达什么。

### 视觉规范

- 画布宽 760~1000px；`viewBox` 与 `width`/`height` 一致；第一个元素铺满白色矩形（站点有深色模式，白底保证可读）。
- 字体用系统字体栈（`svglib.FONT`），不加载外部字体。
- 文字层级：顶部居中标题 19px 加粗 `#1e293b`，下面一行副说明 12.5px `#64748b`；正文 13~14px；注释 11.5~12.5px。
- 配色（描边/文字色，浅底色），全站保持语义一致：

  | 常量 | 颜色 | 用途 |
  | --- | --- | --- |
  | `BLUE` | `#2563eb` / `#eff6ff` | 主流程、in-tree |
  | `VIOLET` | `#7c3aed` / `#f5f3ff` | 队列、out-of-tree |
  | `GREEN` | `#059669` / `#ecfdf5` | 成功路径、绑定 |
  | `TEAL` | `#0d9488` / `#f0fdfa` | 辅助分组 |
  | `ORANGE` | `#ea580c` / `#fff7ed` | 可恢复的失败（如 `Unschedulable`） |
  | `RED` | `#dc2626` / `#fef2f2` | 不可恢复的失败（如 `UnschedulableAndUnresolvable`） |

- 圆角 6~10；连线和箭头 `#94a3b8`，1.6px；可选/需开关的元素、外部插件用虚线边框（`dash=True`）。
- 中文按每字约 1em、英文数字约 0.6em 估宽，方框四周至少留 12~14px 内边距。
- 曲线类图要画坐标轴、刻度和关键点标注；算例图里的数字由脚本按公式计算，不要手填。

### 常见问题

- **文字溢出**：`tw()` 是估算，粗体和等宽英文会偏宽，给宽度留 5%~10% 余量；长句用 `wrap()`。
- **箭头压住文字**：折线绕开文字区域（`arrow` 支持多点折线），或把标签放在连线旁的空白处、必要时旋转（`text(..., rotate=90)`）。
- **XML 报错**：文字里的 `&`、`<`、`>` 要转义，`Canvas.text` 已自动处理；手写 `raw()` 时要自己注意。
- **深色模式下看不清**：别省略白底矩形，也不要用接近白色的浅灰文字。

## 摄影板块

- 页面 `/photography/`：模板 `layouts/photography/list.html`，样式 `assets/css/extended/photography.css`，灯箱脚本 `assets/js/photo-gallery.js`。
- 图片不进仓库，存放在阿里云 OSS（bucket `linpr-github-io` 的 `photography/` 目录，杭州）。`hugo.yaml` 的 `params.photography` 配置访问域名和缩略图/大图的 OSS 图片处理参数。
- 照片清单在 `data/photos.yaml`，每张一项，只有 `src`（OSS 对象路径）必填；上传用 `tools/photos/upload.sh <目录> -p 2026/ -a 相册`（aws CLI + `~/.aws/credentials` 的 `[oss]` profile，对象以 public-read 上传，bucket 私有不可列目录，已设防盗链白名单），它会读取 EXIF 打印 YAML 条目。
