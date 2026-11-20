"""svglib 的完整示例：一条带成功/失败分支的决策流程，外加一个标签面板和图例。

运行：
    python3 tools/diagrams/example.py /tmp/diagram-example.svg /tmp/diagram-example.png
然后打开 PNG 检查有没有溢出、重叠、箭头错位。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from svglib import BLUE, GREEN, MUTED, ORANGE, RED, VIOLET, Canvas  # noqa: E402

out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/diagram-example.svg"
shot = sys.argv[2] if len(sys.argv) > 2 else None

W = 900
c = Canvas(W, 560)
c.title("Filter 判定流程示例", "一个节点依次经过三个检查，任一失败即出局；失败码决定能否靠抢占解决")

# 左列：决策步骤，自上而下
x, y, w = 60, 96, 330
steps = [
    ("1. 节点是否 cordon", "spec.unschedulable 为 true 且 Pod 不容忍对应污点时失败", BLUE),
    ("2. 资源是否放得下", "请求量超过节点剩余可分配量时失败，赶走其他 Pod 可能腾出空间", BLUE),
    ("3. 节点亲和是否匹配", "节点 label 不满足 required 规则时失败，抢占改变不了 label", BLUE),
]
ys = []
for head, body, col in steps:
    h = c.box(x, y, w, head, body, col)
    ys.append((y, h))
    y += h + 34
for (y0, h0), (y1, _) in zip(ys, ys[1:]):
    c.arrow([(x + w / 2, y0 + h0), (x + w / 2, y1 - 3)], GREEN[0])
c.text(x + w / 2 + 8, ys[0][0] + ys[0][1] + 21, "通过", 11.5, GREEN[0])

# 右侧：失败去向
fx = 470
for (y0, h0), (label, col) in zip(ys, [("UnschedulableAndUnresolvable", RED),
                                        ("Unschedulable（可抢占）", ORANGE),
                                        ("UnschedulableAndUnresolvable", RED)]):
    mid = y0 + h0 / 2
    c.arrow([(x + w, mid), (fx - 4, mid)], col[0], dash=True)
    c.chip(fx, mid - 13, label, col)

# 底部：通过后的结果与图例
end_y = ys[-1][0] + ys[-1][1]
c.arrow([(x + w / 2, end_y), (x + w / 2, end_y + 30)], GREEN[0])
c.chip(x + w / 2 - 60, end_y + 33, "节点可行，进入打分", GREEN)

ly = 530
c.chip(60, ly - 18, "in-tree", BLUE)
c.chip(150, ly - 18, "out-of-tree", VIOLET, dash=True)
c.text(270, ly, "虚线箭头 = 失败去向；红色不可抢占，橙色可抢占", 12.5, MUTED)

c.save(out, shot)
print("written", out, "shot", shot)
