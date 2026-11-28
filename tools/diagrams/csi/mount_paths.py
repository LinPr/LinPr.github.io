"""06 篇的挂载路径图：两步挂载与一步挂载。

输出 static/images/posts/csi/06-mount-paths.svg
用法：python3 tools/diagrams/csi/mount_paths.py [png]
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from svglib import Canvas, BLUE, VIOLET, GREEN, ORANGE, GRAY, INK, MUTED

W, H = 1000, 560


def chip(c, x, y, s, color, fs=10.5):
    w = sum(fs if ord(ch) > 0x2000 else fs * 0.6 for ch in s) + 18
    c.rect(x, y, w, 22, color, rx=11, sw=1)
    c.text(x + w / 2, y + 15, s, fs, color[0], "middle")
    return w


def main(out, shot=None):
    c = Canvas(W, H)
    c.title("一个卷怎么变成容器里的目录：hostpath 两步，nfs 一步",
            "kubelet 给同一个卷准备两个路径：staging 路径每节点一份，target 路径每个 Pod 一份")

    # ---------- 上半：hostpath ----------
    c.text(40, 108, "hostpath（声明了 STAGE_UNSTAGE_VOLUME）", 13.5, BLUE[0], bold=True)

    # 数据源
    c.rect(40, 128, 200, 78, ORANGE)
    c.text(56, 152, "宿主机目录", 12.5, ORANGE[0], bold=True)
    c.text(56, 174, "/var/lib/csi-hostpath-data/", 11, INK)
    c.text(56, 192, "<volume-id>", 11, MUTED)

    # Stage（只记账）
    c.rect(290, 128, 240, 90, VIOLET)
    c.text(306, 150, "NodeStageVolume", 12.5, VIOLET[0], bold=True)
    c.text(306, 170, "把 kubelet 给的 staging 路径写进状态", 10.5, INK)
    c.text(306, 186, "/var/lib/kubelet/plugins/kubernetes.io/", 10, MUTED)
    c.text(306, 200, "csi/<driver>/<hash>/globalmount", 10, MUTED)

    # Publish
    c.rect(580, 128, 180, 90, GREEN)
    c.text(596, 150, "NodePublishVolume", 12.5, GREEN[0], bold=True)
    c.text(596, 170, "bind mount 数据目录", 10.5, INK)
    c.text(596, 186, "到 target 路径", 10.5, INK)
    c.text(596, 202, "readOnly 时加 ro", 10, MUTED)

    c.arrow([(240, 173), (289, 173)], color=BLUE[0])
    c.text(250, 166, "读", 10.5, MUTED, "start")
    c.arrow([(530, 173), (579, 173)], color=GREEN[0])

    # target path
    c.rect(800, 128, 160, 90, GRAY)
    c.text(816, 150, "target 路径", 12, GRAY[0], bold=True)
    c.text(816, 170, "/var/lib/kubelet/pods/", 10, INK)
    c.text(816, 186, "<uid>/volumes/", 10, INK)
    c.text(816, 202, "kubernetes.io~csi/<pv>/mount", 10, MUTED)
    c.arrow([(760, 173), (799, 173)], color=GREEN[0])

    # 容器
    c.rect(800, 250, 160, 60, GRAY, dash=True)
    c.text(816, 272, "容器 /data", 12, GRAY[0], bold=True)
    c.text(816, 292, "kubelet 用 target 路径挂进去", 10, MUTED)
    c.arrow([(880, 218), (880, 249)], color=GREEN[0])

    c.arrow([(290 + 120, 218), (290 + 120, 236), (799 - 10, 236)], color=VIOLET[0], dash=True)
    c.text(300, 252, "Stage 不走数据：staging 路径在这份驱动里没有实际挂载", 10.5, MUTED, "start")

    # ---------- 下半：nfs ----------
    c.text(40, 410, "nfs（没有声明 STAGE_UNSTAGE_VOLUME）", 13.5, BLUE[0], bold=True)

    c.rect(40, 430, 200, 70, ORANGE)
    c.text(56, 452, "NFS 服务端", 12.5, ORANGE[0], bold=True)
    c.text(56, 472, "server:/share/subdir", 10.5, INK)
    c.text(56, 490, "网络文件系统，可多节点同时挂", 10.5, MUTED)

    c.rect(290, 430, 200, 70, GREEN)
    c.text(306, 452, "NodePublishVolume", 12.5, GREEN[0], bold=True)
    c.text(306, 472, "直接 mount 到 target 路径", 11, INK)
    c.text(306, 490, "没有 Stage 这一步", 11, MUTED)

    c.rect(540, 430, 200, 70, GRAY, dash=True)
    c.text(556, 452, "target 路径", 12, GRAY[0], bold=True)
    c.text(556, 472, "/var/lib/kubelet/pods/", 10.5, INK)
    c.text(556, 488, "<uid>/volumes/.../mount", 10.5, MUTED)

    c.arrow([(240, 465), (289, 465)], color=ORANGE[0])
    c.arrow([(490, 465), (539, 465)], color=GREEN[0])

    # 图例
    x = 40
    for s, col in [("后端 / 文件系统", ORANGE), ("驱动处理", VIOLET), ("挂载动作", GREEN), ("kubelet 给的路径", GRAY)]:
        x += chip(c, x, 524, s, col) + 8

    c.save(out, shot)


if __name__ == "__main__":
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    out = os.path.join(root, "static/images/posts/csi/06-mount-paths.svg")
    main(out, sys.argv[1] if len(sys.argv) > 1 else None)
    print(out)
