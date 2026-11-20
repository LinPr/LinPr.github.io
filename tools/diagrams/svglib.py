"""手写 SVG 示意图的小工具库（无第三方依赖）。

用法见仓库根目录 AGENTS.md 的「画示意图」一节，完整示例见 example.py。

设计要点：
- 坐标全部由脚本计算，SVG 不会自动换行，所以用 tw() 估算文字宽度来决定方框大小与换行；
- 白底 + 固定配色，博客切换深色模式时图依然清晰；
- save() 会校验 XML，并可调用 headless Chrome 截图，截图必须人工（或用多模态模型）看一遍。
"""
from __future__ import annotations

import os
import subprocess
import unicodedata
import xml.dom.minidom

FONT = "ui-sans-serif,-apple-system,system-ui,'PingFang SC','Microsoft YaHei',sans-serif"

# 文字颜色
INK = "#1e293b"      # 标题、正文
MUTED = "#64748b"    # 副说明、注释
LINE = "#94a3b8"     # 连线与箭头
BORDER = "#cbd5e1"   # 浅灰边框
PANEL = "#f8fafc"    # 面板底色

# 强调色：(描边/文字色, 浅底色)
BLUE = ("#2563eb", "#eff6ff")     # 主流程 / in-tree
VIOLET = ("#7c3aed", "#f5f3ff")   # 队列 / out-of-tree
GREEN = ("#059669", "#ecfdf5")    # 成功路径 / 绑定
TEAL = ("#0d9488", "#f0fdfa")     # 辅助分组
ORANGE = ("#ea580c", "#fff7ed")   # 可恢复的失败（如 Unschedulable）
RED = ("#dc2626", "#fef2f2")      # 不可恢复的失败（如 UnschedulableAndUnresolvable）
GRAY = ("#475569", "#f1f5f9")


def tw(s: str, fs: float) -> float:
    """估算文字宽度：全角/中文按 1em，其余按 0.6em。"""
    return sum(fs if unicodedata.east_asian_width(ch) in "WF" else fs * 0.6 for ch in s)


def wrap(s: str, fs: float, max_w: float) -> list[str]:
    """按估算宽度把一段文字切成多行（逐字切，适合中文；英文单词可能被切开，必要时手工换行）。"""
    lines, cur = [], ""
    for ch in s:
        if cur and tw(cur + ch, fs) > max_w:
            lines.append(cur)
            cur = ""
        cur += ch
    if cur:
        lines.append(cur)
    return lines


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class Canvas:
    def __init__(self, w: int, h: int):
        self.w, self.h = w, h
        self.items: list[str] = []

    # ---------- 基本图元 ----------
    def rect(self, x, y, w, h, color=BLUE, rx=8, dash=False, sw=1.2, fill=None):
        stroke, bg = color
        d = ' stroke-dasharray="5 4"' if dash else ""
        self.items.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="{rx}" '
            f'fill="{fill or bg}" stroke="{stroke}" stroke-width="{sw}"{d}/>')

    def text(self, x, y, s, fs=13, fill=INK, anchor="start", bold=False, rotate=None):
        b = ' font-weight="700"' if bold else ""
        r = f' transform="rotate({rotate} {x:.1f} {y:.1f})"' if rotate is not None else ""
        self.items.append(
            f'<text x="{x:.1f}" y="{y:.1f}" font-size="{fs}" fill="{fill}" '
            f'text-anchor="{anchor}"{b}{r}>{_esc(s)}</text>')

    def lines(self, x, y, rows, fs=13, fill=INK, anchor="start", gap=1.45):
        """多行文字，y 为第一行基线。返回最后一行之后的 y。"""
        for i, s in enumerate(rows):
            self.text(x, y + i * fs * gap, s, fs, fill, anchor)
        return y + len(rows) * fs * gap

    def arrow(self, pts, color=LINE, dash=False, sw=1.6):
        """折线箭头，pts=[(x,y),...]，终点带箭头。"""
        d = "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        da = ' stroke-dasharray="5 4"' if dash else ""
        self.items.append(f'<path d="{d}" stroke="{color}" stroke-width="{sw}" fill="none" marker-end="url(#a)"{da}/>')

    def line(self, x1, y1, x2, y2, color=BORDER, sw=1, dash=False):
        da = ' stroke-dasharray="5 4"' if dash else ""
        self.items.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{color}" stroke-width="{sw}"{da}/>')

    def raw(self, svg: str):
        self.items.append(svg)

    # ---------- 组合元素 ----------
    def title(self, s, sub=None):
        self.text(self.w / 2, 36, s, 19, INK, "middle", True)
        if sub:
            self.text(self.w / 2, 62, sub, 12.5, MUTED, "middle")

    def box(self, x, y, w, head, body=None, color=BLUE, fs=13, pad=14, dash=False, min_h=0):
        """带标题和自动换行正文的方框，高度自适应。返回方框高度。"""
        rows = wrap(body, fs - 0.5, w - 2 * pad) if body else []
        h = max(min_h, pad + fs + 6 + len(rows) * (fs - 0.5) * 1.45 + pad - 4)
        self.rect(x, y, w, h, color, dash=dash)
        self.text(x + pad, y + pad + fs - 2, head, fs + 1, color[0], bold=True)
        self.lines(x + pad, y + pad + fs + 18, rows, fs - 0.5, MUTED)
        return h

    def chip(self, x, y, s, color=BLUE, fs=12.5, dash=False):
        """胶囊标签，返回宽度。"""
        w = tw(s, fs) + 20
        self.rect(x, y, w, 26, color, rx=13, dash=dash, sw=1)
        self.text(x + w / 2, y + 17.5, s, fs, color[0], "middle")
        return w

    # ---------- 输出 ----------
    def svg(self) -> str:
        head = [
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" '
            f'width="{self.w}" height="{self.h}" font-family="{FONT}">',
            '<defs><marker id="a" markerWidth="8" markerHeight="8" refX="6.5" refY="3" orient="auto">'
            f'<path d="M0,0 L6.5,3 L0,6 z" fill="{LINE}"/></marker></defs>',
            f'<rect width="{self.w}" height="{self.h}" fill="#ffffff"/>',
        ]
        return "\n".join(head + self.items + ["</svg>"])

    def save(self, path: str, shot: str | None = None):
        """写文件 + 校验 XML；给了 shot 路径就用 headless Chrome 截图，之后必须打开截图检查。"""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.svg())
        xml.dom.minidom.parse(path)
        if shot:
            os.makedirs(os.path.dirname(os.path.abspath(shot)), exist_ok=True)
            subprocess.run(
                ["google-chrome", "--headless=new", "--no-sandbox", f"--screenshot={shot}",
                 f"--window-size={self.w},{self.h}", "file://" + os.path.abspath(path)],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return path
