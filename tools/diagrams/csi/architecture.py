"""CSI 00 篇的组件图：谁在哪里、通过哪个 socket 说话。

输出 static/images/posts/csi/00-architecture.svg
用法：python3 tools/diagrams/csi/architecture.py [png]
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from svglib import (Canvas, BLUE, VIOLET, GREEN, TEAL, ORANGE, RED, GRAY,
                    INK, MUTED, LINE)

W, H = 1000, 690


def box(c, x, y, w, h, head, rows, color, fs=12.5, dash=False, pad=14):
    c.rect(x, y, w, h, color, dash=dash)
    c.text(x + pad, y + pad + 12, head, fs + 1.5, color[0], bold=True)
    ty = y + pad + 34
    for r in rows:
        c.text(x + pad, ty, "·", fs, color[0], bold=True)
        c.text(x + pad + 10, ty, r, fs, INK)
        ty += fs * 1.55
    return ty


def chip(c, x, y, s, color, fs=12):
    w = len(s) * fs * 0.62 + 22
    c.rect(x, y, w, 24, color, rx=12, sw=1)
    c.text(x + w / 2, y + 16, s, fs, color[0], "middle")
    return w


def main(out, shot=None):
    c = Canvas(W, H)
    c.title("CSI 驱动在集群里的位置：谁在哪里，通过哪个 socket 说话",
            "左列 watch apiserver 的对象，右列由 kubelet 驱动，两者都只是连到驱动进程的 gRPC 客户端")

    # ---- apiserver ----
    c.rect(260, 86, 500, 104, GRAY)
    c.text(274, 110, "kube-apiserver", 13.5, GRAY[0], bold=True)
    x = 274
    for s in ["PersistentVolumeClaim", "PersistentVolume", "VolumeAttachment"]:
        x += chip(c, x, 120, s, GRAY, 10.5) + 8
    x = 274
    for s in ["VolumeSnapshot", "CSIDriver / CSINode"]:
        x += chip(c, x, 150, s, GRAY, 10.5) + 8

    # ---- 左侧 sidecar ----
    lx, ly, lw, lh = 36, 300, 250, 210
    c.rect(lx, ly, lw, lh, BLUE, rx=10)
    c.text(lx + 14, ly + 24, "controller 侧 sidecar", 13.5, BLUE[0], bold=True)
    names = [
        ("external-provisioner", "PVC → CreateVolume"),
        ("external-attacher", "VolumeAttachment → Publish"),
        ("external-snapshotter", "VolumeSnapshot → Snapshot"),
        ("external-resizer", "PVC 扩容 → Expand"),
    ]
    ty = ly + 44
    for n, d in names:
        c.text(lx + 14, ty, n, 12.5, INK, bold=True)
        c.text(lx + 14, ty + 16, d, 11.5, MUTED)
        ty += 40

    # ---- 右侧 kubelet ----
    rx, ry, rw, rh = 714, 300, 250, 210
    c.rect(rx, ry, rw, rh, GREEN, rx=10)
    c.text(rx + 14, ry + 24, "节点上", 13.5, GREEN[0], bold=True)
    items = [
        ("kubelet", "调 NodeStage / NodePublish"),
        ("node-driver-registrar", "注册到 kubelet 的插件目录"),
        ("liveness-probe", "定期调 Probe，失败重启容器"),
        ("挂载点", "在 /var/lib/kubelet/pods 下"),
    ]
    ty = ry + 44
    for n, d in items:
        c.text(rx + 14, ty, n, 12.5, INK, bold=True)
        c.text(rx + 14, ty + 16, d, 11.5, MUTED)
        ty += 40

    # ---- 中间驱动进程 ----
    dx, dy, dw, dh = 330, 300, 340, 210
    c.rect(dx, dy, dw, dh, VIOLET)
    c.text(dx + 14, dy + 24, "hostpath 驱动进程（一个 Pod）", 13.5, VIOLET[0], bold=True)
    c.text(dx + 14, dy + 46, "unix socket：/csi/csi.sock", 12.5, MUTED)
    c.text(dx + 14, dy + 64, "同一个 gRPC 服务里注册三个 service：", 12.5, INK)
    ty = dy + 88
    for s, d in [("Identity", "GetPluginInfo、Probe、能力声明"),
                 ("Controller", "建卷、快照、扩容、附着"),
                 ("Node", "Stage、Publish、统计、健康")]:
        c.text(dx + 22, ty, s, 12.5, VIOLET[0], bold=True)
        c.text(dx + 96, ty, d, 12, INK)
        ty += 26
    c.text(dx + 14, dy + 186, "状态记在 /csi-data-dir 下的 JSON 文件里", 11.5, MUTED)

    # ---- 底部存储 ----
    bx, by, bw, bh = 300, 546, 400, 62
    c.rect(bx, by, bw, bh, ORANGE)
    c.text(bx + 14, by + 24, "宿主机目录", 13, ORANGE[0], bold=True)
    c.text(bx + 14, by + 44, "/var/lib/csi-hostpath-data/<volume-id>，bind mount 给容器", 12, INK)
    c.text(bx + 14, by + 60, "节点与节点之间没有任何共享，所以只能单节点使用", 11.5, MUTED)

    # ---- 箭头 ----
    # apiserver → sidecar（watch）
    c.arrow([(260, 138), (161, 138), (161, ly - 1)])
    c.text(170, 130, "watch", 11.5, MUTED)

    # sidecar → driver
    c.arrow([(lx + lw + 1, 380), (dx - 1, 380)])
    c.text((lx + lw + dx) / 2, 372, "gRPC", 11.5, BLUE[0], "middle")

    # kubelet → driver
    c.arrow([(rx - 1, 380), (dx + dw + 1, 380)])
    c.text((dx + dw + rx) / 2, 372, "gRPC", 11.5, GREEN[0], "middle")

    # registrar → kubelet registration（右侧内部）
    c.arrow([(rx + 100, ry - 1), (rx + 100, ly - 1)], color=GREEN[0], dash=True) if False else None

    # driver → 存储
    c.arrow([(dx + dw / 2, dy + dh + 1), (dx + dw / 2, by - 1)], color=VIOLET[0])

    # apiserver → kubelet（Pod spec）
    c.arrow([(760, 176), (838, 176), (838, ry - 1)], color=GREEN[0])
    c.text(770, 168, "Pod spec", 11.5, GREEN[0], "start")

    # 图例
    lg = [("sidecar / 集群侧", BLUE, False), ("驱动进程", VIOLET, False),
          ("kubelet 侧", GREEN, False), ("存储", ORANGE, False), ("集群对象", GRAY, False)]
    x = 36
    for s, col, d in lg:
        w = chip(c, x, 636, s, col)
        x += w + 10

    c.save(out, shot)


if __name__ == "__main__":
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    out = os.path.join(root, "static/images/posts/csi/00-architecture.svg")
    main(out, sys.argv[1] if len(sys.argv) > 1 else None)
    print(out)
