/* 全站搜索浮层
   - 点击顶栏「搜索」、按 Ctrl/⌘+K 或 / 打开；Esc、点遮罩关闭；
   - ↑/↓ 选中结果，Enter 打开；
   - 第一次打开时才加载 index.json；Fuse 只在精确检索无结果时才加载，用来兜底拼写错误；
   - 没有 JS 时顶栏链接仍然跳到 /search/ 独立页，作为兜底。 */
(() => {
    "use strict";
    const doc = document;
    const modal = doc.getElementById("search-modal");
    if (!modal) return;

    const input = modal.querySelector(".sm-input");
    const list = modal.querySelector(".sm-results");
    const hint = modal.querySelector(".sm-hint");
    const opts = JSON.parse(modal.dataset.fuse || "{}");
    const limit = Math.max(opts.limit || 0, 20);

    let docs = null;      // 预处理过的索引
    let fuse = null;      // 只在精确检索没有结果时才加载，用来兜底拼写错误
    let loading = null;
    let active = -1;
    let lastFocus = null;

    const loadScript = (src) => new Promise((resolve, reject) => {
        const s = doc.createElement("script");
        s.src = src;
        s.onload = resolve;
        s.onerror = reject;
        doc.head.appendChild(s);
    });

    /* ---------- 检索 ----------
       Fuse 的 bitap 模糊匹配按字符编辑距离打分，对中文很不友好：
       搜「调度器」会因为「调」「度」散落在长文里而命中一堆无关文章，
       真正标题里就有这个词的文章反而排不到前面。这里换成：
       1. 查询按空格切词，中英文交界处再切一次（「k8s服务发现」→ k8s / 服务发现）；
       2. 每个词做精确子串匹配，按字段加权：标题 > 标签/分类 > 摘要 > 正文，
          正文按出现次数取对数，避免长文靠堆词取胜；
       3. 默认要求所有词都命中（AND），整句出现在标题里再加分；
       4. 中文词没有精确命中时，退化为二元组（bigram）覆盖率，容忍「服务的发现」这类写法；
       5. 全部词都命中的文章为空时，降级为部分命中；仍为空再交给 Fuse 做模糊匹配。 */
    const norm = (s) => (s || "").normalize("NFKC").toLowerCase();
    const CJK = /[㐀-鿿豈-﫿]/;
    const isCJK = (s) => CJK.test(s);
    const escRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

    const tokenize = (q) => {
        const out = [];
        norm(q).split(/[\s,，、;；|]+/).forEach((w) => {
            (w.match(/[㐀-鿿豈-﫿]+|[^㐀-鿿豈-﫿]+/g) || []).forEach((t) => {
                t = t.replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}+#]+$/gu, "");
                if (t && !out.includes(t)) out.push(t);
            });
        });
        return out;
    };

    const prepare = (data) => data.map((item) => {
        const meta = [].concat(item.tags || [], item.categories || [], item.series || []);
        return {
            item,
            t: norm(item.title),
            meta: meta.map(norm),
            s: norm(item.summary),
            c: norm(item.content),
        };
    });

    /* 英文短词（go、k8s、io）按单词边界匹配，否则 go 会命中 google、algorithm */
    const matcher = (term) => {
        if (!isCJK(term) && term.length <= 3) {
            const re = new RegExp("(?<![\\p{L}\\p{N}])" + escRe(term) + "(?![\\p{L}\\p{N}])", "gu");
            return {
                has: (h) => { re.lastIndex = 0; return re.test(h); },
                count: (h, cap) => { re.lastIndex = 0; let n = 0; while (n < cap && re.exec(h)) n++; return n; },
            };
        }
        return {
            has: (h) => h.includes(term),
            count: (h, cap) => { let n = 0, i = h.indexOf(term); while (i >= 0 && n < cap) { n++; i = h.indexOf(term, i + term.length); } return n; },
        };
    };

    const bigrams = (s) => { const g = []; for (let i = 0; i < s.length - 1; i++) g.push(s.slice(i, i + 2)); return g; };

    const scoreTerm = (d, term, m) => {
        let sc = 0;
        if (m.has(d.t)) {
            sc += 10;
            if (d.t.startsWith(term)) sc += 3;
        }
        if (d.meta.some((x) => x === term)) sc += 8;
        else if (d.meta.some((x) => m.has(x))) sc += 5;
        if (m.has(d.s)) sc += 3;
        const n = m.count(d.c, 64);
        if (n) sc += Math.min(5, 1.2 * Math.log2(1 + n));
        if (sc > 0) return { sc, hl: [term] };

        /* 「服务的发现」「控制器和informer」：去掉虚词后再精确匹配一次 */
        if (isCJK(term)) {
            const core = term.replace(/[的了之和与及]/g, "");
            if (core.length >= 2 && core !== term) {
                const r = scoreTerm(d, core, matcher(core));
                if (r.sc > 0) return { sc: r.sc * 0.8, hl: r.hl };
            }
        }

        /* 中文近似：词里的二元组有 2/3 以上出现过，就算弱命中 */
        if (isCJK(term) && term.length >= 3) {
            const g = bigrams(term);
            const inT = g.filter((x) => d.t.includes(x));
            const inC = g.filter((x) => d.c.includes(x));
            const ft = inT.length / g.length;
            const fc = inC.length / g.length;
            if (ft >= 0.66) return { sc: 4 * ft, hl: inT };
            if (fc >= 0.66) return { sc: 1.5 * fc, hl: inC };
        }
        return { sc: 0, hl: [] };
    };

    const search = (q) => {
        const terms = tokenize(q);
        if (!terms.length) return { results: [], mode: "none" };
        const ms = terms.map(matcher);
        const phrase = norm(q).replace(/\s+/g, " ").trim();
        const all = [];
        const some = [];
        docs.forEach((d) => {
            let total = 0, hit = 0;
            const hl = [];
            terms.forEach((t, i) => {
                const r = scoreTerm(d, t, ms[i]);
                if (r.sc > 0) { hit++; total += r.sc; hl.push(...r.hl); }
            });
            if (!hit) return;
            if (terms.length > 1) {
                if (d.t.includes(phrase)) total += 8;
                else if (d.c.includes(phrase)) total += 3;
            }
            const res = { d, score: total, hl };
            if (hit === terms.length) all.push(res);
            else { res.score *= (hit / terms.length) ** 2; some.push(res); }
        });
        const byScore = (a, b) => b.score - a.score || (b.d.item.date || "").localeCompare(a.d.item.date || "");
        if (all.length) return { results: all.sort(byScore), mode: "all" };
        if (some.length) return { results: some.sort(byScore), mode: "some" };
        return { results: [], mode: "none", terms };
    };

    const ensureIndex = () => {
        if (docs) return Promise.resolve();
        if (loading) return loading;
        hint.textContent = "正在加载索引…";
        loading = fetch(modal.dataset.index)
            .then((r) => r.json())
            .then((data) => {
                docs = prepare(data);
                hint.textContent = "";
                render();
            })
            .catch(() => {
                loading = null;
                hint.textContent = "索引加载失败，请稍后重试";
            });
        return loading;
    };

    const ensureFuse = () => {
        if (fuse) return Promise.resolve(fuse);
        return (window.Fuse ? Promise.resolve() : loadScript(modal.dataset.fuseSrc)).then(() => {
            fuse = new window.Fuse(docs.map((d) => d.item), {
                keys: [
                    { name: "title", weight: 4 },
                    { name: "tags", weight: 2 },
                    { name: "categories", weight: 2 },
                    { name: "summary", weight: 1.5 },
                    { name: "content", weight: 1 },
                ],
                threshold: opts.threshold ?? 0.4,
                ignoreLocation: true,
                minMatchCharLength: 2,
            });
            return fuse;
        });
    };

    const escapeHTML = (s) => s.replace(/[&<>"']/g, (c) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    }[c]));

    const hlRegex = (terms) => {
        const list = [...new Set(terms)].filter(Boolean).sort((a, b) => b.length - a.length);
        return list.length ? new RegExp(list.map((t) => {
            const e = escRe(escapeHTML(t));
            return !isCJK(t) && t.length <= 3 ? "(?<![\\p{L}\\p{N}])" + e + "(?![\\p{L}\\p{N}])" : e;
        }).join("|"), "giu") : null;
    };

    const highlight = (text, re) => {
        const safe = escapeHTML(text);
        return re ? safe.replace(re, (m) => "<mark>" + m + "</mark>") : safe;
    };

    /* 摘要片段：找一个能同时包含最多关键词的位置，前后各截一段 */
    const snippet = (item, terms, re) => {
        const text = (item.content || item.summary || "").replace(/\s+/g, " ");
        const low = norm(text);
        let best = -1, bestHits = 0;
        terms.forEach((t) => {
            let i = low.indexOf(t), guard = 0;
            while (i >= 0 && guard++ < 20) {
                const win = low.slice(Math.max(0, i - 40), i + 80);
                const hits = terms.filter((x) => win.includes(x)).length;
                if (hits > bestHits) { bestHits = hits; best = i; }
                i = low.indexOf(t, i + t.length);
            }
        });
        if (best < 0) return highlight((item.summary || text).slice(0, 96), re);
        const start = Math.max(0, best - 30);
        return highlight((start > 0 ? "…" : "") + text.slice(start, best + 80), re);
    };

    const metaLine = (item) => {
        const parts = [];
        if (item.date) parts.push(escapeHTML(item.date));
        (item.categories || []).forEach((c) => parts.push(escapeHTML(c)));
        return parts.join('<span class="sm-dot">·</span>');
    };

    const setActive = (i) => {
        const items = list.querySelectorAll("a");
        if (!items.length) { active = -1; return; }
        active = (i + items.length) % items.length;
        items.forEach((a, k) => a.classList.toggle("is-active", k === active));
        items[active].scrollIntoView({ block: "nearest" });
    };

    const render = () => {
        if (!docs) return;
        const q = input.value.trim();
        list.innerHTML = "";
        active = -1;
        if (!q) { hint.textContent = "输入关键词开始搜索，多个词用空格分开"; return; }

        const { results, mode, terms } = search(q);
        if (mode === "none") {
            hint.textContent = "没有精确匹配，正在尝试模糊匹配…";
            const asked = q;
            ensureFuse().then((f) => {
                if (input.value.trim() !== asked) return;
                const fr = f.search(asked, { limit });
                if (!fr.length) { hint.textContent = "没有找到相关文章"; return; }
                hint.textContent = "没有精确匹配，以下是模糊匹配的结果";
                draw(fr.map(({ item }) => ({ item, hl: terms || [] })));
            }).catch(() => { hint.textContent = "没有找到相关文章"; });
            return;
        }
        hint.textContent = mode === "some"
            ? "没有同时包含所有关键词的文章，以下是部分匹配"
            : "找到 " + results.length + " 篇" + (results.length > limit ? "，显示前 " + limit + " 篇" : "");
        draw(results.slice(0, limit).map((r) => ({ item: r.d.item, hl: r.hl })));
    };

    const draw = (rows) => {
        const terms = tokenize(input.value);
        const frag = doc.createDocumentFragment();
        rows.forEach(({ item, hl }) => {
            const re = hlRegex(hl.length ? hl : terms);
            const li = doc.createElement("li");
            const a = doc.createElement("a");
            a.href = item.permalink;
            a.innerHTML =
                '<span class="sm-title">' + highlight(item.title || "", re) + "</span>" +
                '<span class="sm-meta">' + metaLine(item) + "</span>" +
                '<span class="sm-snippet">' + snippet(item, hl.length ? hl : terms, re) + "</span>";
            li.appendChild(a);
            frag.appendChild(li);
        });
        list.appendChild(frag);
        setActive(0);
    };

    const open = () => {
        if (!modal.hidden) return;
        lastFocus = doc.activeElement;
        modal.hidden = false;
        doc.documentElement.classList.add("sm-open");
        requestAnimationFrame(() => modal.classList.add("is-open"));
        input.focus();
        input.select();
        ensureIndex();
    };

    const close = () => {
        if (modal.hidden) return;
        modal.classList.remove("is-open");
        doc.documentElement.classList.remove("sm-open");
        modal.hidden = true;
        if (lastFocus && lastFocus.focus) lastFocus.focus();
    };

    /* 顶栏里指向 /search/ 的链接改为打开浮层 */
    doc.querySelectorAll('#menu a[href$="/search/"]').forEach((a) => {
        a.addEventListener("click", (e) => {
            if (e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;
            e.preventDefault();
            open();
        });
    });

    doc.addEventListener("keydown", (e) => {
        const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName) || e.target.isContentEditable;
        if ((e.key === "k" || e.key === "K") && (e.metaKey || e.ctrlKey)) {
            e.preventDefault();
            modal.hidden ? open() : close();
        } else if (e.key === "/" && !typing && modal.hidden) {
            e.preventDefault();
            open();
        } else if (!modal.hidden) {
            if (e.key === "Escape") { e.preventDefault(); close(); }
            else if (e.key === "ArrowDown") { e.preventDefault(); setActive(active + 1); }
            else if (e.key === "ArrowUp") { e.preventDefault(); setActive(active - 1); }
            else if (e.key === "Enter" && e.target === input) {
                const cur = list.querySelectorAll("a")[active];
                if (cur) { e.preventDefault(); window.location.href = cur.href; }
            }
        }
    });

    modal.addEventListener("click", (e) => {
        if (e.target === modal || e.target.closest(".sm-close")) close();
    });

    let timer = 0;
    input.addEventListener("input", () => {
        clearTimeout(timer);
        timer = setTimeout(render, 120);
    });
})();
