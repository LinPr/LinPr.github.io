"""Pod 从提交到运行的全景架构图：kube-apiserver、kube-scheduler、kubelet 与周边控制器。

输出 static/images/posts/sched-plugins/00-architecture.svg
用法：python3 tools/diagrams/sched/architecture.py [png_path]
"""
import os
import re
import sys
import unicodedata

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from svglib import (Canvas, BLUE, VIOLET, GREEN, TEAL, ORANGE, RED, GRAY,
                    INK, MUTED, LINE, PANEL, _esc)

W = 1000
L, R = 36, 964          # 内容区左右边界
PAD = 12
FS = 12.5               # 正文字号
HF = 13.5               # 卡片标题字号


def tw(s, fs, bold=False):
    w = 0.0
    for ch in s:
        ea = unicodedata.east_asian_width(ch)
        w += fs if ea in "WFA" and ord(ch) > 0x2000 else fs * 0.6
    return w * (1.07 if bold else 1.0)


TOKEN = re.compile(r"[A-Za-z0-9_./<>{}=\-+()'\"%:,]+|\s+|.")


def wrap(s, fs, max_w, bold=False):
    """按词换行：英文标识符整体不拆，中文逐字。"""
    rows, cur = [], ""
    toks = []
    for tok in TOKEN.findall(s):
        # 超长标识符按字符切开，避免溢出
        while tw(tok, fs, bold) > max_w:
            k = len(tok)
            while k > 1 and tw(tok[:k], fs, bold) > max_w:
                k -= 1
            toks.append(tok[:k])
            tok = tok[k:]
        toks.append(tok)
    for tok in toks:
        if cur and tok in "，、；：。）)" :
            cur += tok
            continue
        if cur and tw(cur + tok, fs, bold) > max_w:
            rows.append(cur.rstrip())
            cur = tok.lstrip()
        else:
            cur += tok
    if cur.strip():
        rows.append(cur.rstrip())
    return rows


def layout(w, head, items, fs=FS):
    inner = w - 2 * PAD
    hrows = [head] if re.fullmatch(r"[A-Za-z]+", head) else wrap(head, HF, inner - 8, bold=True)
    body = []
    for it in items:
        rs = wrap(it, fs, inner - 12)
        body.append(rs)
    lh = fs * 1.42
    h = PAD + len(hrows) * HF * 1.3 + 4
    h += sum(len(rs) for rs in body) * lh + max(0, len(body) - 1) * 3 + PAD - 2
    return hrows, body, h


def card(c, x, y, w, head, items, color, dash=False, h=None, fs=FS, tags=()):
    hrows, body, h0 = layout(w, head, items, fs)
    h = max(h or 0, h0)
    c.rect(x, y, w, h, color, dash=dash)
    ty = y + PAD + HF - 1
    for r in hrows:
        c.text(x + PAD, ty, r, HF, color[0], bold=True)
        ty += HF * 1.3
    ty += 4 + fs * 0.2
    lh = fs * 1.42
    for rs in body:
        c.text(x + PAD + 1, ty, "·", fs, color[0], bold=True)
        for r in rs:
            c.text(x + PAD + 11, ty, r, fs, INK)
            ty += lh
        ty += 3
    tx = x + w - 8
    for t in reversed(tags):
        tx -= 22
        tag(c, tx, y - 9, t)
    return h


def tag(c, x, y, s):
    c.raw(f'<circle cx="{x + 10:.1f}" cy="{y + 10:.1f}" r="10" fill="#ffffff" '
          f'stroke="{TEAL[0]}" stroke-width="1.4"/>')
    c.text(x + 10, y + 14.3, s, 12, TEAL[0], "middle", bold=True)


def panel(c, idx, x, y, w, h, title, color, sub=None):
    stroke, bg = color
    c.items.insert(idx,
                   f'<rect x="{x}" y="{y}" width="{w}" height="{h:.1f}" rx="12" fill="{PANEL}" '
                   f'stroke="{stroke}" stroke-width="1.4" stroke-opacity="0.55"/>'
                   f'<rect x="{x}" y="{y}" width="6" height="{h:.1f}" rx="3" fill="{stroke}"/>'
                   f'<text x="{x + 18}" y="{y + 24}" font-size="15" font-weight="700" fill="{stroke}">{_esc(title)}</text>'
                   + (f'<text x="{x + 24 + tw(title, 15, True)}" y="{y + 24}" font-size="12" fill="{MUTED}">{_esc(sub)}</text>' if sub else ""))


def label(c, x, y, s, color=MUTED, anchor="middle", fs=11.5, bg=True):
    if bg:
        w = tw(s, fs) + 8
        bx = x - w / 2 if anchor == "middle" else (x - 4 if anchor == "start" else x - w + 4)
        c.raw(f'<rect x="{bx:.1f}" y="{y - fs:.1f}" width="{w:.1f}" height="{fs + 5:.1f}" fill="#ffffff" rx="3"/>')
    c.text(x, y, s, fs, color, anchor)


def arrow(c, pts, color=LINE, dash=False):
    c.arrow(pts, color=color, dash=dash)


def strip(c, x, y, w, s, color):
    rows = wrap(s, FS, w - 2 * PAD)
    h = PAD + len(rows) * FS * 1.42 + 4
    c.rect(x, y, w, h, color, rx=6)
    c.lines(x + PAD, y + PAD + FS - 1, rows, FS, color[0], gap=1.42)
    return h


def build():
    c = Canvas(W, 10)
    c.title("从 kubectl apply 到容器运行：调度器与周边组件全景",
            "①~㉔ 是主路径；橙色是会重试的失败，红色是这个 Pod 到此为止；Ⓐ~Ⓖ 是外部组件，在图中对应位置标出它从哪里介入")
    y = 84

    # ---------------- 一、kube-apiserver：创建 ----------------
    idx = len(c.items)
    top = y
    y += 40
    n, gap = 5, 12
    bw = (R - L - gap * (n - 1)) / n
    boxes = [
        ("① 提交 Pod", ["kubectl apply，或 ReplicaSet、Job 等控制器 POST pods",
                       "此时 spec.nodeName 为空"], GRAY, ("Ⓐ",)),
        ("② 认证与鉴权", ["认证：确认请求者身份", "鉴权：RBAC 判断能否 create pods"], GRAY, ()),
        ("③ 变更准入", ["LimitRanger：补默认 requests/limits",
                      "Priority：按 priorityClassName 写入 spec.priority 与 preemptionPolicy",
                      "DefaultTolerationSeconds：加 not-ready、unreachable 容忍 300s",
                      "RuntimeClass：写入 overhead",
                      "Mutating webhook：注入 sidecar、加 schedulingGates 等"], BLUE, ("Ⓑ",)),
        ("④ 对象校验", ["字段格式与取值是否合法",
                      "带 schedulingGates 时不能直接设置 spec.nodeName",
                      "创建后 spec.priority 不可改，schedulingGates 只能删不能加"], BLUE, ()),
        ("⑤ 验证准入", ["PodSecurity：检查安全级别",
                      "ValidatingAdmissionPolicy 与 Validating webhook",
                      "ResourceQuota：requests/limits 计入命名空间配额，超额拒绝"], BLUE, ()),
    ]
    ws = [140, 120, 216, 180, 224]
    xs = [L + sum(ws[:i]) + gap * i for i in range(n)]
    hmax = max(layout(ws[i], b[0], b[1])[2] for i, b in enumerate(boxes))
    boxes_y = y
    for i, (hd, it, col, tg) in enumerate(boxes):
        x = xs[i]
        card(c, x, y, ws[i], hd, it, col, h=hmax, tags=tg)
        if i:
            arrow(c, [(x - gap + 1, y + 30), (x - 1, y + 30)])
    y += hmax + 14
    sh = strip(c, L, y, R - L, "②~⑤ 任一步失败：请求直接返回 401/403/422 等错误，Pod 不会写入 etcd，调度器永远看不到它", RED)
    # 从 ②~⑤ 底部引到红条
    for i in range(1, 5):
        x = xs[i] + ws[i] / 2
        arrow(c, [(x, y - 14), (x, y - 1)], color=RED[0])
    y += sh + 14
    eh = card(c, L + 170, y, R - L - 340, "⑥ 写入 etcd",
              ["Pod 处于 Pending，spec.nodeName 为空；调度器、kubelet、各控制器通过 watch 收到 ADDED 事件"], GRAY)
    # 最后一个准入框到 etcd
    arrow(c, [(R + 1, boxes_y + 30), (R + 8, boxes_y + 30), (R + 8, y + eh / 2), (R - 170 + 1, y + eh / 2)])
    etcd_bottom = y + eh
    y += eh + 16
    panel(c, idx, 20, top, 960, y - top, "kube-apiserver", BLUE, "创建 Pod 的请求链路")
    y += 16

    # ---------------- 二、kube-scheduler ----------------
    idx = len(c.items)
    top = y
    arrow(c, [(W / 2, etcd_bottom), (W / 2, y + 40 - 1)])
    y += 40
    h7 = card(c, L, y, R - L, "⑦ informer 与事件处理器",
              ["Pod ADDED，spec.nodeName 为空且 schedulerName 属于本调度器的某个 Profile → 进入调度队列",
               "已绑定的 Pod（spec.nodeName 非空）→ 只写入 cache，用来计算节点上已占用的资源",
               "Node、PVC、PV、StorageClass、ResourceClaim 等对象变化 → 按插件的 EventsToRegister 和 QueueingHint，把相关 entity 移出 unschedulableEntities"],
              VIOLET, tags=("Ⓑ", "Ⓒ", "Ⓔ"))
    y += h7 + 30
    # 队列四格
    gq = 28
    qw = (R - L - gq * 3) / 4
    qboxes = [
        ("⑧ PreEnqueue", ["SchedulingGates：spec.schedulingGates 非空就拒绝",
                          "DynamicResources 等插件也实现了 PreEnqueue",
                          "被拒绝的 Pod 以 gated 状态留在 unschedulableEntities"], VIOLET, ("Ⓑ",)),
        ("activeQ", ["堆，比较函数来自 QueueSort 插件",
                     "PrioritySort：spec.priority 高的先出队，相同时先入队的先出",
                     "出队单位是 entity：单个 Pod 或整个 PodGroup"], VIOLET, ()),
        ("backoffQ", ["堆，按退避结束时间排序",
                      "退避时间从 1s 起随失败次数翻倍，最长 10s",
                      "每秒 flush 一次，到期的移回 activeQ"], VIOLET, ()),
        ("unschedulableEntities", ["map，没有顺序，不会被 Pop",
                                   "存放 gated 和 unschedulable 两类 entity",
                                   "相关事件到达且 QueueingHint 返回 Queue 才移出",
                                   "停留超过 5 分钟强制移出"], VIOLET, ("Ⓔ",)),
    ]
    qh = max(layout(qw, b[0], b[1])[2] for b in qboxes)
    qx = [L + i * (qw + gq) for i in range(4)]
    arrow(c, [(qx[0] + qw / 2, y - 30), (qx[0] + qw / 2, y - 1)])
    label(c, qx[0] + qw / 2 + 6, y - 12, "新 Pod", anchor="start")
    for i, (hd, it, col, tg) in enumerate(qboxes):
        card(c, qx[i], y, qw, hd, it, col, h=qh, tags=tg)
    ay = y + 34
    arrow(c, [(qx[0] + qw + 1, ay), (qx[1] - 1, ay)])
    label(c, qx[0] + qw + gq / 2, ay - 6, "通过", fs=11)
    arrow(c, [(qx[2] - 1, ay), (qx[1] + qw + 1, ay)])
    label(c, qx[1] + qw + gq / 2, ay - 6, "到期", fs=11)
    arrow(c, [(qx[3] - 1, ay), (qx[2] + qw + 1, ay)])
    label(c, qx[2] + qw + gq / 2, ay - 6, "唤醒", fs=11)
    # 唤醒但无需退避 → 直接回 activeQ
    by = y + qh
    arrow(c, [(qx[3] + 40, by), (qx[3] + 40, by + 18), (qx[1] + qw - 20, by + 18), (qx[1] + qw - 20, by + 1)], color=VIOLET[0])
    label(c, (qx[2] + qw / 2), by + 22, "唤醒且已过退避期 → 直接进 activeQ", VIOLET[0], fs=11)
    # PreEnqueue 拒绝 → gated
    arrow(c, [(qx[0] + 30, by), (qx[0] + 30, by + 40), (qx[3] + qw - 30, by + 40), (qx[3] + qw - 30, by + 1)], color=ORANGE[0])
    label(c, qx[0] + 140, by + 44, "PreEnqueue 拒绝 → gated", ORANGE[0], fs=11)
    activeq_x = qx[1] + qw / 2
    unsched_right = (qx[3] + qw, y + qh / 2)
    y = by + 64

    # ---- 调度周期 ----
    idx2 = len(c.items)
    stop = y
    y += 40
    lw = 452
    rx = L + lw + 84
    rw = R - rx - 12
    steps = [
        ("⑨ Pop", ["从 activeQ 队头取出一个 entity，标记为 in-flight，记录这期间发生的集群事件"]),
        ("⑩ 拍快照", ["UpdateSnapshot：把 cache 拷成本周期只读的节点快照，包含已 assume 但还没绑定完成的 Pod"]),
        ("⑪ PreFilter", ["预计算并写入 CycleState，可返回 PreFilterResult 缩小候选节点",
                         "返回 UnschedulableAndUnresolvable → 跳过 Filter 和抢占，直接进入失败处理"]),
        ("⑫ Filter", ["有 nominatedNodeName 时先单独试这个节点，通过就直接选它",
                      "否则并行遍历候选节点，凑够 numFeasibleNodesToFind 个可行节点就停",
                      "节点上优先级不低于本 Pod 的提名 Pod，按已经在这个节点上计算"]),
        ("⑬ PreScore / Score / NormalizeScore", ["各插件打分归一化到 0~100，乘权重后加总",
                                                 "取最高分节点，同分随机；只有一个可行节点时跳过打分"]),
        ("⑭ assume", ["把 Pod 当作已绑定写进 cache，下一个 Pod 立刻看得到它占用的资源",
                      "调度周期因此可以不等绑定完成"]),
        ("⑮ Reserve", ["插件预留卷、DRA 设备等，失败时按逆序调用 Unreserve"]),
        ("⑯ Permit", ["Allow → 进入绑定周期", "Wait → 到绑定周期里等（如 Gang 等同组 Pod 到齐）", "Reject → 失败"]),
    ]
    sy = {}
    for i, (hd, it) in enumerate(steps):
        h = card(c, L + 12, y, lw - 12, hd, it, BLUE)
        sy[hd[:1]] = (y, h)
        if i:
            pass
        y_next = y + h + 22
        if i < len(steps) - 1:
            arrow(c, [(L + 12 + 60, y + h), (L + 12 + 60, y_next - 1)])
        y = y_next
    sched_steps_bottom = y - 22
    # PostFilter
    fy, fh = sy["⑫"]
    py = fy
    ph = card(c, rx, py, rw, "PostFilter（DefaultPreemption 抢占）",
              ["前提：preemptionPolicy 不是 Never，且至少有一个节点的失败码是 Unschedulable",
               "在这些节点上找优先级更低的 victims，尽量少驱逐、尽量不违反 PDB",
               "给 victims 加 DisruptionTarget 条件，再通过 API 删除它们",
               "清掉目标节点上更低优先级 Pod 的 nominatedNodeName",
               "返回目标节点名；本轮仍算调度失败，等 victims 退出后重新调度"], ORANGE)
    arrow(c, [(L + lw + 1, fy + 30), (rx - 1, fy + 30)], color=ORANGE[0])
    label(c, (L + lw + rx) / 2, fy + 22, "0 个可行节点", ORANGE[0], fs=11)
    # 失败处理
    fy2 = py + ph + 40
    fh2 = card(c, rx, fy2, rw, "失败处理 handleSchedulingFailure",
               ["Unschedulable、UnschedulableAndUnresolvable → unschedulableEntities；in-flight 期间已有相关事件则进 backoffQ",
                "Error 以及绑定周期中的失败 → backoffQ",
                "通过 API 写回 PodScheduled=False（reason: Unschedulable），抢占成功时同时写入 status.nominatedNodeName"],
               ORANGE, tags=("Ⓔ",))
    arrow(c, [(rx + rw / 2, py + ph), (rx + rw / 2, fy2 - 1)], color=ORANGE[0])
    # PreFilter 失败
    # Reserve / Permit 失败
    r15y, r15h = sy["⑮"]
    p16y, p16h = sy["⑯"]
    fx_in = r15y + 26
    arrow(c, [(L + lw + 1, p16y + 26), (L + lw + 40, p16y + 26), (L + lw + 40, fx_in), (rx - 1, fx_in)], color=ORANGE[0])
    c.line(L + lw, r15y + 26, L + lw + 40, r15y + 26, color=ORANGE[0], sw=1.6)
    label(c, L + lw + 20, r15y + 20, "失败", ORANGE[0], fs=11)
    # 失败处理 → 队列（沿右边缘向上）
    ex = R + 6
    arrow(c, [(rx + rw + 1, fy2 + fh2 / 2), (ex, fy2 + fh2 / 2), (ex, unsched_right[1]), (unsched_right[0] + 1, unsched_right[1])], color=ORANGE[0])
    # activeQ → Pop
    p9y, _ = sy["⑨"]
    arrow(c, [(activeq_x, by), (activeq_x, by + 6)], color=VIOLET[0]) if False else None
    arrow(c, [(qx[1] + qw - 50, by), (qx[1] + qw - 50, p9y - 1)], color=BLUE[0])
    label(c, qx[1] + qw - 44, p9y - 10, "Pop", BLUE[0], anchor="start", fs=11)
    y = max(sched_steps_bottom, fy2 + fh2) + 18
    panel(c, idx2, L - 6, stop, R - L + 12 - 4, y - stop, "调度周期", BLUE, "单个 goroutine 串行执行，同一时刻只处理一个 entity")
    y += 18

    # ---- 绑定周期 ----
    idx3 = len(c.items)
    btop = y
    arrow(c, [(L + 12 + 60, sched_steps_bottom), (L + 12 + 60, y + 40 - 1)], color=GREEN[0])
    y += 40
    bgap = 30
    bw2 = (R - L - 24 - bgap * 3) / 4
    bsteps = [
        ("⑰ WaitOnPermit", ["等 Permit 返回的 Wait 被 Allow；超时或被 Reject 即失败"], ()),
        ("⑱ PreBind", ["开启 NominatedNodeNameForExpectation 时先写 nominatedNodeName，告诉外部组件 Pod 将落在哪",
                       "VolumeBinding：绑定 PV，或写 selected-node 注解并等待供给",
                       "DynamicResources：写 ResourceClaim 的 allocation 与 reservedFor"], ("Ⓒ", "Ⓓ", "Ⓔ")),
        ("⑲ Bind", ["DefaultBinder：POST pods/<name>/binding",
                    "按顺序调用，第一个不返回 Skip 的 Bind 插件生效"], ()),
        ("⑳ PostBind", ["绑定成功后的通知与清理，不影响结果"], ()),
    ]
    bh = max(layout(bw2, b[0], b[1])[2] for b in bsteps)
    bx = [L + 12 + i * (bw2 + bgap) for i in range(4)]
    for i, (hd, it, tg) in enumerate(bsteps):
        card(c, bx[i], y, bw2, hd, it, GREEN, h=bh, tags=tg)
        if i:
            arrow(c, [(bx[i] - bgap + 1, y + 30), (bx[i] - 1, y + 30)], color=GREEN[0])
    # 绑定失败 → 失败处理
    yy = y + bh + 16
    arrow(c, [(bx[0] + bw2 / 3, y + bh), (bx[0] + bw2 / 3, yy), (bx[3] + bw2 + 6, yy),
              (bx[3] + bw2 + 6, fy2 + fh2 + 30), (rx + rw - 60, fy2 + fh2 + 30), (rx + rw - 60, fy2 + fh2 + 1)],
          color=ORANGE[0])
    for i in (1, 2):
        c.line(bx[i] + bw2 / 3, y + bh, bx[i] + bw2 / 3, yy, color=ORANGE[0], sw=1.6)
    label(c, (bx[1] + bx[2] + bw2) / 2, yy + 4, "⑰~⑲ 任一步失败：Unreserve，再把 Pod 从 cache 中撤销（forget），进 backoffQ",
          ORANGE[0], fs=11)
    bind_x = bx[2] + bw2 * 0.75
    bind_y = y + bh
    y = yy + 20
    panel(c, idx3, L - 6, btop, R - L + 12 - 4, y - btop, "绑定周期", GREEN, "每个 Pod 单独一个 goroutine，调度周期不等它")
    y += 16
    panel(c, idx, 20, top, 960, y - top, "kube-scheduler", VIOLET)
    y += 26

    # ---------------- 三、kube-apiserver：binding ----------------
    arrow(c, [(bind_x, yy + 4 + 10), (bind_x, y - 1)], color=GREEN[0]) if False else None
    arrow(c, [(bind_x, bind_y), (bind_x, y - 1)], color=GREEN[0])
    h21 = card(c, L, y, R - L, "㉑ kube-apiserver 处理 binding 子资源",
               ["spec.schedulingGates 非空 → 拒绝绑定",
                "PodTopologyLabels 准入（需开启 feature gate）：把节点的 zone、region 等拓扑标签复制到 Pod",
                "写入 spec.nodeName，清空 status.nominatedNodeName，置 PodScheduled=True，写回 etcd"], BLUE)
    y += h21 + 26

    # ---------------- 四、kubelet ----------------
    idx = len(c.items)
    top = y
    y += 40
    k1 = L + (R - L - 60) / 6
    arrow(c, [(k1, top - 26), (k1, y - 1)])
    label(c, k1 + 6, top - 6, "kubelet watch 到 spec.nodeName=本节点", anchor="start", fs=11)
    kg = 30
    kw = (R - L - kg * 2) / 3
    kboxes = [
        ("㉒ kubelet 准入", ["重新检查资源是否放得下、required nodeAffinity、nodeName、hostPort、NoExecute 污点",
                           "调度器决策和节点实际状态之间可能有时间差，这里是最后一道检查"], ()),
        ("㉓ 准备与启动", ["DRA driver 执行 NodePrepareResources，CSI 挂载卷",
                        "CRI 创建 sandbox 和容器",
                        "phase：Pending → Running"], ("Ⓒ", "Ⓓ")),
        ("㉔ 运行期驱逐", ["节点内存、磁盘、PID 压力时由 kubelet 驱逐 Pod",
                        "排序依据：用量是否超出 requests → spec.priority → 实际用量"], ("Ⓕ",)),
    ]
    kh = max(layout(kw, b[0], b[1])[2] for b in kboxes)
    kx = [L + i * (kw + kg) for i in range(3)]
    for i, (hd, it, tg) in enumerate(kboxes):
        card(c, kx[i], y, kw, hd, it, GREEN, h=kh, tags=tg)
        if i:
            arrow(c, [(kx[i] - kg + 1, y + 30), (kx[i] - 1, y + 30)], color=GREEN[0])
    y += kh + 22
    r1 = strip(c, kx[0], y, kw, "准入失败：phase=Failed，reason 如 OutOfcpu、NodeAffinity；不会重新调度，由控制器 Ⓐ 新建 Pod", RED)
    r2 = strip(c, kx[2], y, kw, "被驱逐：phase=Failed，reason=Evicted，同样由控制器 Ⓐ 新建 Pod 重新走一遍", RED)
    arrow(c, [(kx[0] + kw / 2, y - 22), (kx[0] + kw / 2, y - 1)], color=RED[0])
    arrow(c, [(kx[2] + kw / 2, y - 22), (kx[2] + kw / 2, y - 1)], color=RED[0])
    y += max(r1, r2) + 16
    panel(c, idx, 20, top, 960, y - top, "kubelet", GREEN, "运行在被选中的节点上")
    y += 30

    # ---------------- 五、外部组件 ----------------
    idx = len(c.items)
    top = y
    y += 40
    eg = 16
    ew = (R - L - eg * 2) / 3
    ext = [
        ("Ⓐ 工作负载控制器", ["ReplicaSet、Job、StatefulSet 等创建 Pod（①）",
                          "Pod 失败或被删除后创建新 Pod，新 Pod 从 ① 重新开始"]),
        ("Ⓑ gate 控制器", ["Kueue 等通过 webhook 加上 schedulingGates（③）",
                         "配额、依赖就绪后删除 gate → Pod 更新事件 → ⑧ 重新判断"]),
        ("Ⓒ 存储控制器", ["PV controller 与 external-provisioner",
                       "看到 selected-node 注解后在该拓扑上供给并绑定 PV，⑱ 的等待随之结束"]),
        ("Ⓓ DRA", ["ResourceClaim controller 按模板为 Pod 生成 ResourceClaim",
                   "DRA driver 发布 ResourceSlice，并在 ㉓ 准备设备"]),
        ("Ⓔ Cluster Autoscaler", ["发现 PodScheduled=False、reason=Unschedulable 的 Pod 就扩容节点",
                                  "参考 nominatedNodeName 避免重复扩容；新节点的 Node Add 事件经 ⑦ 唤醒 Pod"]),
        ("Ⓕ 节点生命周期", ["node-lifecycle-controller：节点失联时加 not-ready/unreachable 的 NoExecute 污点",
                         "taint-eviction-controller：超过 tolerationSeconds（③ 默认 300s）后删除 Pod"]),
        ("Ⓖ descheduler（可选）", ["按策略驱逐已运行的 Pod（如负载不均、违反亲和）",
                                  "调度器自己从不迁移已绑定的 Pod，重平衡只能靠驱逐后重建"]),
    ]
    rows = [ext[0:3], ext[3:6], ext[6:7]]
    for row in rows:
        rh = max(layout(ew, e[0], e[1])[2] for e in row)
        for i, (hd, it) in enumerate(row):
            card(c, L + i * (ew + eg), y, ew, hd, it, TEAL, dash=True, h=rh)
        y += rh + 14
    # 图例放在最后一行的空位
    lx = L + ew + eg
    ly = y - 14 - rh
    legend = [("主流程 / API Server", BLUE, False), ("调度队列", VIOLET, False),
              ("绑定 / 节点", GREEN, False), ("会重试的失败", ORANGE, False),
              ("Pod 到此为止", RED, False), ("外部组件", TEAL, True), ("通用步骤", GRAY, False)]
    cx, cy = lx + 4, ly + 6
    c.text(cx, cy + 12, "图例", 13, INK, bold=True)
    cy += 24
    for i, (s, col, d) in enumerate(legend):
        ww = tw(s, 12.5) + 20
        if cx + ww > R:
            cx, cy = lx + 4, cy + 34
        c.chip(cx, cy, s, col, dash=d)
        cx += ww + 10
    y += 4
    panel(c, idx, 20, top, 960, y - top, "周边组件", TEAL, "不属于调度器，但决定了 Pod 能否进入、何时被唤醒、最终落在哪")
    y += 20

    c.h = int(y)
    return c


if __name__ == "__main__":
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    out = os.path.join(root, "static/images/posts/sched-plugins/00-architecture.svg")
    shot = sys.argv[1] if len(sys.argv) > 1 else None
    build().save(out, shot)
    print(out)
