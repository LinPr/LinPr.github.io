"""CSI 系列开篇的通用架构图：任意 CSI 驱动的部署形态。

输出 static/images/posts/csi/00-architecture.svg
用法：python3 tools/diagrams/csi/architecture.py [png]
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from svglib import (Canvas, BLUE, VIOLET, GREEN, ORANGE, GRAY,
                    INK, MUTED, LINE)

W, H = 1000, 772


def tw_cn(s, fs):
    return sum(fs if ord(ch) > 0x2000 else fs * 0.6 for ch in s)


def chip(c, x, y, s, color, fs=10.5, h=23):
    w = tw_cn(s, fs) + 20
    c.rect(x, y, w, h, color, rx=h / 2, sw=1)
    c.text(x + w / 2, y + h / 2 + 4, s, fs, color[0], "middle")
    return w


def badge(c, x, y, n):
    c.raw(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="10" fill="#ffffff" stroke="{MUTED}" stroke-width="1.3"/>')
    c.text(x, y + 4.2, str(n), 11.5, MUTED, "middle", bold=True)


def row(c, x, y, head, sub, hfs=12.5, sfs=11):
    c.text(x, y, head, hfs, INK, bold=True)
    c.text(x, y + 17, sub, sfs, MUTED)


def main(out, shot=None):
    c = Canvas(W, H)
    c.title("CSI 驱动在集群里的通用形态：两个工作负载，一组 unix socket",
            "任意 CSI 驱动都遵循这套分工；controller 侧只部署一份，node 侧每个节点各一份")
    c.text(W / 2, 78, "驱动本身不 watch 任何对象，它只是被 sidecar 和 kubelet 调用的 gRPC 服务端", 12, MUTED, "middle")

    # ---------------- apiserver ----------------
    ax, ay, aw, ah = 170, 92, 660, 104
    c.rect(ax, ay, aw, ah, GRAY)
    c.text(ax + 16, ay + 24, "kube-apiserver", 13.5, GRAY[0], bold=True)
    c.text(ax + 140, ay + 24, "驱动不直接读写这些对象", 11, MUTED)
    x = ax + 16
    for s in ["PersistentVolumeClaim", "PersistentVolume", "VolumeAttachment"]:
        x += chip(c, x, ay + 34, s, GRAY) + 8
    x = ax + 16
    for s in ["VolumeSnapshot", "CSIDriver / CSINode", "StorageClass"]:
        x += chip(c, x, ay + 66, s, GRAY) + 8

    # ---------------- 两个面板 ----------------
    py, ph = 232, 330
    lx, lw = 36, 450
    rx, rw = 514, 450

    # controller 侧
    c.rect(lx, py, lw, ph, BLUE)
    c.text(lx + 20, py + 28, "controller 侧（Deployment，通常 1 副本）", 13.5, BLUE[0], bold=True)
    cy = py + 66
    for head, sub in [
        ("external-provisioner", "watch PVC → CreateVolume / DeleteVolume"),
        ("external-attacher", "watch VolumeAttachment → ControllerPublish"),
        ("external-snapshotter", "watch VolumeSnapshot → CreateSnapshot"),
        ("external-resizer", "PVC 容量变化 → ControllerExpandVolume"),
    ]:
        row(c, lx + 22, cy, head, sub)
        cy += 42

    # node 侧
    c.rect(rx, py, rw, ph, GREEN)
    c.text(rx + 20, py + 28, "node 侧（每个节点一个 DaemonSet）", 13.5, GREEN[0], bold=True)
    ny = py + 66
    for head, sub in [
        ("kubelet", "NodeStage / NodePublish / NodeGetVolumeStats"),
        ("node-driver-registrar", "把 socket 注册到 kubelet 的插件目录"),
        ("liveness-probe", "定期 Probe，失败就重启驱动容器"),
        ("挂载点", "/var/lib/kubelet/pods/<uid>/volumes"),
    ]:
        row(c, rx + 22, ny, head, sub)
        ny += 42

    # 驱动服务（两侧同一高度、同一内边距）
    dy, dh = py + 244, 62
    c.rect(lx + 22, dy, lw - 44, dh, VIOLET, rx=8, fill="#f5f3ff")
    c.text(lx + 38, dy + 26, "驱动：Controller 服务", 12.5, VIOLET[0], bold=True)
    c.text(lx + 38, dy + 46, "unix socket，路径由驱动自己定", 11, MUTED)

    c.rect(rx + 22, dy, rw - 44, dh, VIOLET, rx=8, fill="#f5f3ff")
    c.text(rx + 38, dy + 26, "驱动：Node 服务 + Identity 服务", 12.5, VIOLET[0], bold=True)
    c.text(rx + 38, dy + 46, "unix socket，同一个进程里注册", 11, MUTED)

    # ---------------- 后端存储（整条横带） ----------------
    bx, by, bw, bh = 36, 606, 928, 88
    c.rect(bx, by, bw, bh, ORANGE)
    c.text(bx + 20, by + 26, "后端存储（由驱动决定）", 13, ORANGE[0], bold=True)
    x = bx + 190
    for s in ["块设备", "分布式文件系统", "对象存储", "本地目录"]:
        x += chip(c, x, by + 10, s, ORANGE) + 8
    c.text(bx + 20, by + 52, "驱动把 CreateVolume、NodePublish 这些语义翻译成对后端的实际操作", 11.5, INK)
    c.text(bx + 20, by + 72, "hostpath 的后端是宿主机上的一个目录；nfs 的后端是 NFS 服务端上的一个子目录", 11.5, MUTED)

    # ---------------- 箭头 ----------------
    # ① apiserver → sidecar（watch）
    c.arrow([(210, ay + ah), (110, ay + ah), (110, py - 1)])
    badge(c, 110, ay + ah + 18, 1)
    c.text(126, ay + ah + 22, "watch 对象变化", 11, MUTED, "start")

    # apiserver → node（Pod spec）
    c.arrow([(ax + aw, 176), (938, 176), (938, py - 1)], color=GREEN[0])
    c.text(838, 168, "Pod spec", 11, GREEN[0], "start")

    # ② sidecar → 驱动 controller
    c.arrow([(lx + lw / 2, py + 214), (lx + lw / 2, dy - 1)], color=BLUE[0])
    badge(c, lx + lw / 2 - 24, py + 226, 2)
    c.text(lx + lw / 2 + 8, py + 230, "gRPC", 11, BLUE[0], "start")

    # ③ kubelet → 驱动 node
    c.arrow([(rx + rw / 2, py + 214), (rx + rw / 2, dy - 1)], color=GREEN[0])
    badge(c, rx + rw / 2 - 24, py + 226, 3)
    c.text(rx + rw / 2 + 8, py + 230, "gRPC", 11, GREEN[0], "start")

    # ④ 驱动 → 后端（两条都落在横带上）
    c.arrow([(lx + 120, py + ph + 1), (lx + 120, by - 1)], color=VIOLET[0])
    badge(c, lx + 104, py + ph + 18, 4)
    c.text(lx + 136, py + ph + 22, "建卷、扩容、快照", 11, VIOLET[0], "start")

    c.arrow([(rx + 120, py + ph + 1), (rx + 120, by - 1)], color=VIOLET[0])
    badge(c, rx + 104, py + ph + 18, 4)
    c.text(rx + 136, py + ph + 22, "挂载、读写卷数据", 11, VIOLET[0], "start")

    # ---------------- 图例 ----------------
    lg = [("调用方 / 集群侧", BLUE), ("驱动进程", VIOLET), ("kubelet 侧", GREEN),
          ("后端存储", ORANGE), ("集群对象", GRAY)]
    x = 36
    for s, col in lg:
        x += chip(c, x, 722, s, col, fs=11) + 10
    c.text(W - 36, 738, "① watch ② ③ gRPC ④ 操作后端", 11, MUTED, "end")

    c.save(out, shot)


if __name__ == "__main__":
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    out = os.path.join(root, "static/images/posts/csi/00-architecture.svg")
    main(out, sys.argv[1] if len(sys.argv) > 1 else None)
    print(out)
