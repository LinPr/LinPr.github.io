"""CSI 系列通用组件图：任何 CSI 驱动的部署形态，不特指某一份驱动。

输出 static/images/posts/csi/00-architecture.svg
用法：python3 tools/diagrams/csi/architecture.py [png]
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from svglib import (Canvas, BLUE, VIOLET, GREEN, TEAL, ORANGE, RED, GRAY,
                    INK, MUTED, LINE)

W, H = 1000, 706


def chip(c, x, y, s, color, fs=11):
    w = sum(fs if ord(ch) > 0x2000 else fs * 0.6 for ch in s) + 20
    c.rect(x, y, w, 23, color, rx=11, sw=1)
    c.text(x + w / 2, y + 16, s, fs, color[0], "middle")
    return w


def rows(c, x, y, items, fs=12, gap=40):
    ty = y
    for head, sub in items:
        c.text(x, ty, head, fs + 0.5, INK, bold=True)
        if sub:
            c.text(x, ty + 16, sub, fs - 0.5, MUTED)
        ty += gap
    return ty


def main(out, shot=None):
    c = Canvas(W, H)
    c.title("CSI 驱动在集群里的通用形态：两个工作负载，一组 unix socket",
            "任意 CSI 驱动都遵循这套分工：controller 侧只部署一份，node 侧每个节点各一份")
    c.text(W / 2, 78, "驱动本身不 watch 任何对象，它只是被 sidecar 和 kubelet 调用的 gRPC 服务端", 12, MUTED, "middle")

    # ---------------- apiserver ----------------
    c.rect(250, 90, 500, 100, GRAY)
    c.text(264, 114, "kube-apiserver", 13.5, GRAY[0], bold=True)
    x = 264
    for s in ["PersistentVolumeClaim", "PersistentVolume", "VolumeAttachment"]:
        x += chip(c, x, 124, s, GRAY) + 8
    x = 264
    for s in ["VolumeSnapshot", "CSIDriver / CSINode"]:
        x += chip(c, x, 154, s, GRAY) + 8

    # ---------------- controller 侧 ----------------
    cx, cy, cw, ch = 36, 240, 450, 240
    c.rect(cx, cy, cw, ch, BLUE, rx=10)
    c.text(cx + 16, cy + 26, "controller 侧（Deployment，通常 1 副本）", 13.5, BLUE[0], bold=True)
    rows(c, cx + 16, cy + 58, [
        ("external-provisioner", "watch PVC → CreateVolume / DeleteVolume"),
        ("external-attacher", "watch VolumeAttachment → ControllerPublish"),
        ("external-snapshotter", "watch VolumeSnapshot → CreateSnapshot"),
        ("external-resizer", "PVC 容量变化 → ControllerExpandVolume"),
    ], fs=11.5, gap=33)

    dx, dy, dw, dh = cx + 16, cy + 196, cw - 32, 34
    c.rect(dx, dy, dw, dh, VIOLET, rx=8, fill="#f5f3ff")
    c.text(dx + 12, dy + 22, "驱动：Controller 服务（同一个 unix socket）", 11.5, VIOLET[0], bold=True)

    # ---------------- node 侧 ----------------
    nx, ny, nw, nh = 530, 240, 434, 240
    c.rect(nx, ny, nw, nh, GREEN, rx=10)
    c.text(nx + 16, ny + 26, "node 侧（每个节点一个 DaemonSet）", 13.5, GREEN[0], bold=True)
    rows(c, nx + 16, ny + 58, [
        ("kubelet", "NodeStage / NodePublish / NodeGetVolumeStats"),
        ("node-driver-registrar", "把驱动 socket 注册到 kubelet 的插件目录"),
        ("liveness-probe", "定期调 Probe，失败就重启驱动容器"),
    ], fs=11.5, gap=33)

    dx2, dy2 = nx + 16, ny + 186
    c.rect(dx2, dy2, nw - 32, 38, VIOLET, rx=8, fill="#f5f3ff")
    c.text(dx2 + 12, dy2 + 24, "驱动：Node 服务 + Identity 服务（同一个 unix socket）", 11.5, VIOLET[0], bold=True)

    # ---------------- 后端存储 ----------------
    bx, by, bw, bh = 300, 560, 400, 92
    c.rect(bx, by, bw, bh, ORANGE)
    c.text(bx + 16, by + 24, "后端存储（由驱动决定）", 13, ORANGE[0], bold=True)
    c.text(bx + 16, by + 46, "块设备、分布式文件系统、对象存储、本地磁盘……", 11.5, INK)
    c.text(bx + 16, by + 66, "驱动把 CSI 的语义翻译成对后端的实际操作", 11.5, MUTED)
    c.text(bx + 16, by + 84, "hostpath 的后端只是宿主机上的一个目录", 11.5, MUTED)

    # ---------------- 箭头 ----------------
    c.arrow([(250, 150), (90, 150), (90, cy - 1)])
    c.text(98, 142, "watch", 11.5, MUTED)
    c.arrow([(750, 176), (960, 176), (960, ny - 1)], color=GREEN[0])
    c.text(768, 168, "Pod spec、卷信息", 11.5, GREEN[0], "start")

    c.arrow([(cx + cw / 2, cy + 190), (cx + cw / 2, dy - 1)], color=BLUE[0])
    c.text(cx + cw / 2 + 8, cy + 202, "gRPC", 11.5, BLUE[0], "start")

    c.arrow([(nx + nw / 2, ny + 178), (nx + nw / 2, dy2 - 1)], color=GREEN[0])
    c.text(nx + nw / 2 + 8, ny + 190, "gRPC", 11.5, GREEN[0], "start")

    c.arrow([(cx + cw / 2 - 70, cy + ch + 1), (cx + cw / 2 - 70, by - 1)], color=VIOLET[0])
    c.text(cx + cw / 2 - 76, cy + ch + 20, "建卷、快照", 11, VIOLET[0], "end")
    c.arrow([(nx + nw / 2 - 30, ny + nh + 1), (nx + nw / 2 - 30, by - 1)], color=VIOLET[0])
    c.text(nx + nw / 2 - 36, ny + nh + 20, "挂载、读写", 11, VIOLET[0], "end")

    # ---------------- 图例 ----------------
    lg = [("调用方 / 集群侧", BLUE), ("驱动进程", VIOLET),
          ("kubelet 侧", GREEN), ("后端存储", ORANGE), ("集群对象", GRAY)]
    x = 36
    for s, col in lg:
        x += chip(c, x, 672, s, col) + 10

    c.save(out, shot)


if __name__ == "__main__":
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    out = os.path.join(root, "static/images/posts/csi/00-architecture.svg")
    main(out, sys.argv[1] if len(sys.argv) > 1 else None)
    print(out)
