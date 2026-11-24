"""kube-scheduler 调用栈图生成器。

- 不带参数：生成开篇用的完整调用栈 static/images/posts/sched-plugins/00-call-stack.svg；
- 带 --all：再为系列 01~40 每篇生成一张，把本篇插件实现的扩展点用红框标出来，
  输出到 static/images/posts/sched-plugins/callstack/NN.svg。

函数名与调用关系基于 Kubernetes master aabdf5a2（pkg/scheduler），逐条核对过源码。
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from svglib import Canvas, BLUE, GREEN, ORANGE, VIOLET, TEAL, MUTED, INK, tw  # noqa: E402

ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
OUT = os.path.join(ROOT, "static/images/posts/sched-plugins")

MONO = "ui-monospace,SFMono-Regular,Menlo,Consolas,monospace"
HOOK_BG, HOOK_BD, HOOK_INK = "#fef3c7", "#f59e0b", "#b45309"
KEY_INK = "#1e3a8a"
BOLDATTR = ' font-weight="700" fill="#1e3a8a"'
MARK = "#e11d48"

# (depth, 函数, 关键结论(加粗), 补充说明, 阶段色, 插件钩子)
R = [
 (0,"PriorityQueue.moveToActiveQ","入队前先过 PreEnqueue","被拦下的 Pod 以 gated 状态留在 unschedulableEntities，不消耗调度周期",VIOLET,None),
 (1,"runPreEnqueuePlugins","","",VIOLET,None),
 (2,"runPreEnqueuePluginsForPod","插件存在 map 里，顺序不固定","上次拦住它的 GatingPlugin 先跑，任一拒绝即 gated",VIOLET,None),
 (3,"runPreEnqueuePlugin","","SchedulingGates、DynamicResources、DefaultPreemption、GangScheduling",VIOLET,"pl.PreEnqueue()"),
 (0,"ScheduleOne","串行循环，一次只处理一个 entity","entity 可能是 Pod，也可能是整个 PodGroup（走 scheduleOnePodGroup）",VIOLET,None),
 (1,"NextEntity = PriorityQueue.Pop","阻塞直到 activeQ 有 entity","取堆顶，即 QueueSort 认为最该先调度的",VIOLET,None),
 (2,"lessFn（包装 QueueSortFunc）","整个进程只有一个 QueueSort","所有 profile 必须一致；默认先比优先级，再比入队时间",VIOLET,"QueueSort.Less()"),
 (1,"scheduleOnePod","新建 CycleState","插件在各扩展点之间靠它传数据，抢占模拟时会 Clone",BLUE,None),
 (2,"frameworkForPod","","按 spec.schedulerName 选出 profile（插件链与权重）",BLUE,None),
 (2,"schedulingCycle","成功 → go 绑定；失败 → handleSchedulingFailure","",BLUE,None),
 (3,"Cache.UpdateSnapshot","整个周期只看这一份快照","informer 的并发更新不会影响本轮判断",BLUE,None),
 (3,"schedulingAlgorithm","只有返回 FitError 才会抢占","PodGroup 周期里单个 Pod 不跑 PostFilter",BLUE,None),
 (4,"SchedulePod","只有 1 个可行节点时直接选它","跳过整个打分阶段",BLUE,None),
 (5,"findNodesThatFitPod","三步依次尝试","PreFilter → 提名节点 → 遍历候选节点",BLUE,None),
 (6,"prefilterNodes","","",BLUE,None),
 (7,"RunPreFilterPlugins","PreFilterResult 求交集缩小候选","Skip 跳过同插件的 Filter；UnschedulableAndUnresolvable 立即结束本轮",BLUE,None),
 (8,"runPreFilterPlugin","","预计算写入 CycleState，供 Filter/Score 读取",BLUE,"pl.PreFilter()"),
 (6,"evaluateNominatedNode","提名节点优先单独试","通过就直接返回，不再遍历其他节点",BLUE,None),
 (6,"findNodesThatPassFilters","节点间并行，找够就停","≥100 个节点时只找一部分：比例 50−N/125 %，至少 5%、至少 100 个",BLUE,None),
 (7,"RunFilterPluginsWithNominatedPods","把已提名的更高优先级 Pod 也算进去","先带上它们过一遍，再不带过一遍，两次都通过才算可行",BLUE,None),
 (8,"RunFilterPlugins","同一节点上插件串行","一个插件失败，这个节点就出局",BLUE,None),
 (9,"runFilterPlugin","失败码决定能否抢占","Unschedulable 可以；UnschedulableAndUnresolvable 不行",BLUE,"pl.Filter()"),
 (5,"prioritizeNodes","总分 = Σ 归一化分 × 权重","默认权重：TaintToleration 3，亲和/打散/DRA 2，其余 1",BLUE,None),
 (6,"RunPreScorePlugins","","返回 Skip 的插件跳过 Score",BLUE,None),
 (7,"runPreScorePlugin","","",BLUE,"pl.PreScore()"),
 (6,"RunScorePlugins","","",BLUE,None),
 (7,"runRawScorePlugins","按节点并行","",BLUE,None),
 (8,"runScorePlugin","","原始分，量纲由插件自定",BLUE,"pl.Score()"),
 (7,"normalizeScores","按插件并行","把原始分映射到 0–100",BLUE,None),
 (8,"runScoreExtension","","",BLUE,"NormalizeScore()"),
 (4,"RunPostFilterPlugins","抢占成功本轮也算失败","只删受害者并写 nominatedNodeName，下一轮才真正放上去",ORANGE,None),
 (5,"runPostFilterPlugin","","DynamicResources 先释放闲置 claim，再由 DefaultPreemption 驱逐",ORANGE,"pl.PostFilter()"),
 (3,"prepareForBindingCycle","","",BLUE,None),
 (4,"AssumeAndReserveInCache","assume：缓存里先当它已在节点上","下一个 Pod 的调度周期立刻看到资源被占，绑定才能异步",BLUE,None),
 (5,"reserve","","",BLUE,None),
 (6,"RunReservePluginsReserve","有状态插件在这里记录状态","任一失败，按逆序调用 Unreserve 撤销",BLUE,None),
 (7,"runReservePluginReserve","","",BLUE,"pl.Reserve()"),
 (4,"RunPermitPlugins","批准 / 拒绝 / 等待","Wait 的 Pod 占着资源进等待表；Coscheduling 靠它凑齐一组",BLUE,None),
 (5,"runPermitPlugin","","",BLUE,"pl.Permit()"),
 (2,"go runBindingCycle","调度周期成功后 go 出去，调度循环立刻处理下一个 Pod","绑定周期之间并发执行",GREEN,None),
 (3,"bindingCycle","","",GREEN,None),
 (4,"WaitOnPermit","阻塞到批准、拒绝或超时","此后再失败不算「不可调度」，直接进 backoffQ 重试",GREEN,None),
 (4,"RunPreBindPlugins","绑定前的 API 操作","VolumeBinding 绑定 PVC、等动态供给；DRA 写回 claim",GREEN,None),
 (5,"runPreBindPlugin","","",GREEN,"pl.PreBind()"),
 (4,"bind","","",GREEN,None),
 (5,"RunBindPlugins","第一个不返回 Skip 的插件生效","默认 DefaultBinder：POST pods/binding 子资源",GREEN,None),
 (6,"runBindPlugin","","",GREEN,"bp.Bind()"),
 (4,"RunPostBindPlugins","","纯通知，失败也不影响结果",GREEN,None),
 (5,"runPostBindPlugin","","",GREEN,"pl.PostBind()"),
 (2,"FailureHandler = handleSchedulingFailure","调度周期失败时记下是哪些插件拒绝了它","之后只有这些插件关心的事件才可能唤醒它",ORANGE,None),
 (3,"AddUnschedulablePodIfNotPresent","","",ORANGE,None),
 (4,"determineSchedulingHintForInFlightPod","周期内来过相关事件 → backoffQ","否则进 unschedulableEntities 等 QueueingHint，最长 5 分钟兜底刷新",ORANGE,None),

 (1,"scheduleOnePodGroup","PodGroup 作为一个 entity 出队","整组在一个周期里逐个试放，只在快照里 assume，失败统一撤销",TEAL,None),
 (2,"podGroupCycle","","",TEAL,None),
 (3,"runRootSchedulingAlgorithm","","",TEAL,None),
 (4,"podGroupSchedulingAlgorithm","开了 TopologyAwareWorkloadScheduling 才走 placement","否则直接进 podGroupSchedulingDefaultAlgorithm",TEAL,None),
 (5,"podGroupSchedulingPlacementAlgorithm","","",TEAL,None),
 (6,"RunPlacementGeneratePlugins","把节点切成若干候选 placement","",TEAL,None),
 (7,"runPlacementGeneratePlugin","","",TEAL,"pl.GeneratePlacements()"),
 (6,"evaluatePlacement","在每个 placement 里试放整组","",TEAL,None),
 (7,"podGroupSchedulingDefaultAlgorithm","逐个 Pod 试放，每放一个问一次可行性","",TEAL,None),
 (8,"podGroupPodSchedulingAlgorithm","","调 schedulingAlgorithm：与单 Pod 同一条 PreFilter/Filter/Score 路径",TEAL,None),
 (8,"podGroupPotentiallyFeasible","","",TEAL,None),
 (9,"RunPlacementFeasiblePlugins","","",TEAL,None),
 (10,"runPlacementFeasiblePlugin","达到 minCount 才算成功","Wait 表示继续放下一个；Unschedulable 提前结束",TEAL,"pl.PlacementFeasible()"),
 (6,"findBestPlacement","","",TEAL,None),
 (7,"RunPlacementScorePlugins","在可行的 placement 之间打分","",TEAL,None),
 (8,"runPlacementScorePlugin","","",TEAL,"pl.ScorePlacement()"),
 (3,"RunPodGroupPostFilterPlugins","整组失败时才跑","代替周期内单个 Pod 的 PostFilter",ORANGE,None),
 (4,"runPodGroupPostFilterPlugin","","",ORANGE,"pl.PodGroupPostFilter()"),
 (3,"submitPodGroupAlgorithmResult","绑定仍按 Pod 逐个进行","成功的 Pod 各自 prepareForBindingCycle + go runBindingCycle",TEAL,None),
]

LEFT, INDENT, NAME_FS = 40, 20, 12.4
NOTE_X, W = 540, 1260
NOTE_W = W - NOTE_X - 30
FS, LH = 12.2, 16.5


def hook_key(hook):
    return hook.split(".")[-1].rstrip("()") if hook else None


def note_lines(key, note, hook):
    first_off = (tw(hook, 12) * 1.02 + 28) if hook else 0
    segs, cur, curw, avail = [], [], 0.0, NOTE_W - first_off
    def push(text, bold):
        nonlocal cur, curw, avail
        import re
        # 英文/数字/符号连在一起当一个词，避免把标识符从中间折断
        for tok in re.findall(r"[A-Za-z0-9_.\-/()=+>%≥]+|.", text):
            w = tw(tok, FS) * (1.06 if bold else 1)
            if curw + w > avail and cur:
                segs.append(cur); cur, curw, avail = [], 0.0, NOTE_W
                if tok == " ":
                    continue
            if cur and cur[-1][1] == bold:
                cur[-1] = (cur[-1][0] + tok, bold)
            else:
                cur.append((tok, bold))
            curw += w
    if key: push(key, True)
    if key and note: push("：", True)
    if note: push(note, False)
    if cur: segs.append(cur)
    return segs or [[]], first_off


def render(out, shot=None, marks=(), banner=None, sub=None):
    marks = set(marks)
    top = 116 + ((84 if sub else 64) if banner else 0)
    rows, y = [], top
    for d, name, key, note, col, hook in R:
        lines, off = note_lines(key, note, hook)
        h = max(26, 10 + len(lines) * LH)
        rows.append((y, h, d, name, lines, off, col, hook))
        y += h
    H = int(y + 70)
    c = Canvas(W, H)
    c.title("从队列到绑定：调度器的调用栈与关键决策",
            "缩进表示调用关系；黄色底的行直接执行插件钩子；右侧加粗的是这一步的关键结论")
    c.text(W / 2, 86, "pkg/scheduler：schedule_one.go · schedule_one_podgroup.go · algorithm.go · framework/runtime/framework.go · backend/queue/scheduling_queue.go",
           11.6, MUTED, "middle")
    if banner:
        name, pts = banner
        l1 = f"本篇插件：{name}"
        l2 = f"实现的扩展点：{pts}"
        lines = [(l1, 15, True, MARK), (l2, 12.8, True, MARK)] + ([(sub, 12, False, "#9f1239")] if sub else [])
        bw = max(tw(t, fs) * (1.05 if b else 1) for t, fs, b, _ in lines) + 44
        bh = 14 + sum(fs * 1.5 for _, fs, _, _ in lines)
        bx = (W - bw) / 2
        c.raw(f'<rect x="{bx:.1f}" y="102" width="{bw:.1f}" height="{bh:.1f}" rx="8" fill="#fff1f2" stroke="{MARK}" stroke-width="1.6"/>')
        ty = 102 + 8
        for t, fs, b, colr in lines:
            ty += fs * 1.5
            w8 = ' font-weight="700"' if b else ""
            c.raw(f'<text x="{W/2:.1f}" y="{ty - 5:.1f}" font-size="{fs}"{w8} fill="{colr}" text-anchor="middle">{t}</text>')
    last = {}
    for i, (y, h, d, name, lines, off, col, hook) in enumerate(rows):
        x = LEFT + d * INDENT
        cy = y + 13
        if d == 0 and i > 0:
            c.line(28, y, W - 20, y, "#e2e8f0", 1, dash=True)
        elif i % 2 == 0:
            c.raw(f'<rect x="26" y="{y:.1f}" width="{W - 46}" height="{h}" fill="#f8fafc"/>')
        marked = hook and hook_key(hook) in marks
        if hook:
            stroke = f'stroke="{MARK}" stroke-width="2.6"' if marked else f'stroke="{HOOK_BD}" stroke-width="1"'
            fill = "#ffe4e6" if marked else HOOK_BG
            c.raw(f'<rect x="{x - 8:.1f}" y="{y + 2:.1f}" width="{W - 20 - (x - 8):.1f}" height="{h - 4}" rx="6" fill="{fill}" {stroke}/>')
        if d > 0 and (d - 1) in last:
            px = LEFT + (d - 1) * INDENT + 8
            c.line(px, last[d - 1], px, cy, "#94a3b8", 1.1)
            c.line(px, cy, x - 6, cy, "#94a3b8", 1.1)
        c.raw(f'<rect x="12" y="{y + 3:.1f}" width="5" height="{h - 6}" rx="2.5" fill="{col[0]}" opacity="0.85"/>')
        wgt = ' font-weight="700"' if hook else ""
        ink = MARK if marked else (HOOK_INK if hook else INK)
        c.raw(f'<text x="{x:.1f}" y="{cy + 4.3:.1f}" font-size="{NAME_FS}" fill="{ink}" font-family="{MONO}"{wgt}>{name}</text>')
        if hook:
            hw = tw(hook, 12) * 1.02 + 18
            bd = MARK if marked else HOOK_BD
            c.raw(f'<rect x="{NOTE_X:.1f}" y="{cy - 8:.1f}" width="{hw:.1f}" height="16.5" rx="8" fill="#ffffff" stroke="{bd}"/>')
            c.raw(f'<text x="{NOTE_X + 9:.1f}" y="{cy + 4:.1f}" font-size="12" fill="{ink}" font-family="{MONO}" font-weight="700">{hook}</text>')
        for li, segs in enumerate(lines):
            if not segs:
                continue
            tx = NOTE_X + (off if li == 0 else 0)
            ty = cy + 4.2 + li * LH
            parts = "".join("<tspan" + (BOLDATTR if b else "") + ">" + t.replace("&", "&amp;").replace("<", "&lt;") + "</tspan>" for t, b in segs)
            c.raw(f'<text x="{tx:.1f}" y="{ty:.1f}" font-size="{FS}" fill="#475569">{parts}</text>')
        if marked:
            c.raw(f'<text x="{W - 30:.1f}" y="{cy + 4.5:.1f}" font-size="12.5" font-weight="700" fill="{MARK}" text-anchor="end">◀ 本篇</text>')
        last[d] = cy
        for k in [k for k in last if k > d]:
            del last[k]
    ly = H - 32
    lx = LEFT
    for label, col in [("队列", VIOLET), ("调度周期（串行）", BLUE), ("绑定周期（goroutine）", GREEN), ("PodGroup 周期", TEAL), ("失败 / 抢占路径", ORANGE)]:
        c.raw(f'<rect x="{lx}" y="{ly - 10}" width="5" height="14" rx="2.5" fill="{col[0]}" opacity="0.85"/>')
        c.text(lx + 12, ly + 1, label, 12, MUTED)
        lx += 12 + tw(label, 12) + 26
    c.raw(f'<rect x="{lx}" y="{ly - 12}" width="30" height="17" rx="5" fill="{HOOK_BG}" stroke="{HOOK_BD}"/>')
    c.text(lx + 38, ly + 1, "执行插件钩子", 12, MUTED)
    lx += 38 + tw("执行插件钩子", 12) + 26
    if marks:
        c.raw(f'<rect x="{lx}" y="{ly - 12}" width="30" height="17" rx="5" fill="#ffe4e6" stroke="{MARK}" stroke-width="2"/>')
        c.text(lx + 38, ly + 1, "本篇插件实现的扩展点", 12, MUTED)
    else:
        c.raw(f'<text x="{lx}" y="{ly + 1}" font-size="12" fill="{KEY_INK}" font-weight="700">加粗</text>')
        c.text(lx + 30, ly + 1, "= 关键决策", 12, MUTED)
    c.save(out, shot)
    return H


# 每篇插件实现的扩展点（按源码中的接口断言与方法集核对）
PLUGINS = {
    "01": ("SchedulingGates", ["PreEnqueue"]),
    "02": ("PrioritySort", ["Less"]),
    "03": ("NodeName", ["PreFilter", "Filter"]),
    "04": ("NodeUnschedulable", ["PreFilter", "Filter"]),
    "05": ("TaintToleration", ["PreFilter", "Filter", "PreScore", "Score", "NormalizeScore"]),
    "06": ("NodeAffinity", ["PreFilter", "Filter", "PreScore", "Score", "NormalizeScore"]),
    "07": ("NodePorts", ["PreFilter", "Filter"]),
    "08": ("NodeResourcesFit", ["PreFilter", "Filter", "PreScore", "Score", "ScorePlacement"]),
    "09": ("VolumeRestrictions", ["PreFilter", "Filter"]),
    "10": ("NodeVolumeLimits", ["PreFilter", "Filter"]),
    "11": ("VolumeBinding", ["PreFilter", "Filter", "PreScore", "Score", "Reserve", "PreBind"]),
    "12": ("VolumeZone", ["PreFilter", "Filter"]),
    "13": ("PodTopologySpread", ["PreFilter", "Filter", "PreScore", "Score", "NormalizeScore"]),
    "14": ("InterPodAffinity", ["PreFilter", "Filter", "PreScore", "Score", "NormalizeScore"]),
    "15": ("DynamicResources", ["PreEnqueue", "PreFilter", "Filter", "PostFilter", "Score", "NormalizeScore", "Reserve", "PreBind", "PodGroupPostFilter"]),
    "16": ("NodeDeclaredFeatures", ["PreFilter", "Filter"]),
    "17": ("DeferredPodScheduling", ["PreFilter", "Filter", "Permit"]),
    "18": ("DefaultPreemption", ["PreEnqueue", "PostFilter", "PodGroupPostFilter"]),
    "19": ("NodeResourcesBalancedAllocation", ["PreScore", "Score"]),
    "20": ("ImageLocality", ["Score"]),
    "21": ("GangScheduling", ["PreEnqueue", "PlacementFeasible"]),
    "22": ("TopologyPlacementGenerator", ["GeneratePlacements"]),
    "23": ("PodGroupPodsCount", ["ScorePlacement"]),
    "24": ("DefaultBinder", ["Bind"]),
    "25": ("QOSSort", ["Less"]),
    "26": ("TopologicalSort", ["Less"]),
    "27": ("Coscheduling", ["Less", "PreFilter", "PostFilter", "Reserve", "Permit"]),
    "28": ("CapacityScheduling", ["PreFilter", "PostFilter", "Reserve"]),
    "29": ("NetworkOverhead", ["PreFilter", "Filter", "Score", "NormalizeScore"]),
    "30": ("NodeResourceTopologyMatch", ["PreFilter", "Filter", "Score", "Reserve", "PostBind"]),
    "31": ("PreemptionToleration", ["PostFilter"]),
    "32": ("CrossNodePreemption", ["PostFilter"]),
    "33": ("NodeResourcesAllocatable", ["Score", "NormalizeScore"]),
    "34": ("NodeMetadata", ["Score", "NormalizeScore"]),
    "35": ("PodState", ["Score", "NormalizeScore"]),
    "36": ("SySched", ["Score", "NormalizeScore"]),
    "37": ("TargetLoadPacking", ["Score", "NormalizeScore"]),
    "38": ("LoadVariationRiskBalancing", ["Score", "NormalizeScore"]),
    "39": ("LowRiskOverCommitment", ["PreScore", "Score", "NormalizeScore"]),
    "40": ("Peaks", ["Score", "NormalizeScore"]),
}

NOTES = {
    "11": "Reserve 一行同时代表 Unreserve（失败时退回 PV/PVC 的 assume）",
    "27": "Reserve 一行同时代表 Unreserve；scheduler-plugins 基于 v1.36.4，框架调用链与图中一致",
    "28": "Reserve 一行同时代表 Unreserve；scheduler-plugins 基于 v1.36.4，框架调用链与图中一致",
    "30": "Reserve 一行同时代表 Unreserve；scheduler-plugins 基于 v1.36.4，框架调用链与图中一致",
    "32": "示例代码已整体注释、未注册到调度器，这里标的是它设计上挂的位置",
}
OOT_NOTE = "scheduler-plugins 基于 v1.36.4，运行在独立的调度器二进制里，框架调用链与图中一致"
HOOK_NAMES = {"Less": "QueueSort", "GeneratePlacements": "PlacementGenerate", "ScorePlacement": "PlacementScore"}


if __name__ == "__main__":
    render(os.path.join(OUT, "00-call-stack.svg"), "/tmp/sched-series/shots/00-call-stack.png")
    if "--all" in sys.argv:
        os.makedirs(os.path.join(OUT, "callstack"), exist_ok=True)
        for nn, (name, hooks) in PLUGINS.items():
            pts = " · ".join(HOOK_NAMES.get(h, h) for h in hooks)
            sub = NOTES.get(nn) or (OOT_NOTE if int(nn) >= 25 else None)
            render(os.path.join(OUT, "callstack", f"{nn}.svg"), f"/tmp/sched-series/shots/cs-{nn}.png",
                   marks=hooks, banner=(name, pts), sub=sub)
    print("done")
