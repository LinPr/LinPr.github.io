/* 全站搜索浮层
   - 点击顶栏「搜索」、按 Ctrl/⌘+K 或 / 打开；Esc、点遮罩关闭；
   - ↑/↓ 选中结果，Enter 打开；
   - 第一次打开时才加载 Fuse 和 index.json，平时不占带宽；
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
    const limit = opts.limit || 10;

    let fuse = null;
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

    const ensureIndex = () => {
        if (fuse) return Promise.resolve();
        if (loading) return loading;
        hint.textContent = "正在加载索引…";
        loading = Promise.all([
            window.Fuse ? Promise.resolve() : loadScript(modal.dataset.fuseSrc),
            fetch(modal.dataset.index).then((r) => r.json()),
        ]).then(([, data]) => {
            fuse = new window.Fuse(data, {
                keys: opts.keys || ["title", "summary", "content"],
                threshold: opts.threshold ?? 0.4,
                distance: opts.distance ?? 1000,
                ignoreLocation: true,
                includeMatches: true,
                minMatchCharLength: Math.max(opts.minmatchcharlength || 1, 1),
            });
            hint.textContent = "";
            render();
        }).catch(() => {
            loading = null;
            hint.textContent = "索引加载失败，请稍后重试";
        });
        return loading;
    };

    const escapeHTML = (s) => s.replace(/[&<>"']/g, (c) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    }[c]));

    /* 在正文里找第一次命中，截取前后一段作为摘要并高亮关键词 */
    const snippet = (item, q) => {
        const text = (item.content || item.summary || "").replace(/\s+/g, " ");
        const idx = text.toLowerCase().indexOf(q.toLowerCase());
        let part;
        if (idx < 0) part = text.slice(0, 90);
        else {
            const start = Math.max(0, idx - 30);
            part = (start > 0 ? "…" : "") + text.slice(start, idx + q.length + 60);
        }
        return highlight(part, q);
    };

    const highlight = (text, q) => {
        const safe = escapeHTML(text);
        if (!q) return safe;
        const re = new RegExp(escapeHTML(q).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
        return safe.replace(re, (m) => "<mark>" + m + "</mark>");
    };

    const setActive = (i) => {
        const items = list.querySelectorAll("a");
        if (!items.length) { active = -1; return; }
        active = (i + items.length) % items.length;
        items.forEach((a, k) => a.classList.toggle("is-active", k === active));
        items[active].scrollIntoView({ block: "nearest" });
    };

    const render = () => {
        if (!fuse) return;
        const q = input.value.trim();
        list.innerHTML = "";
        active = -1;
        if (!q) { hint.textContent = "输入关键词开始搜索"; return; }
        const results = fuse.search(q, { limit });
        if (!results.length) { hint.textContent = "没有找到相关文章"; return; }
        hint.textContent = "";
        const frag = doc.createDocumentFragment();
        results.forEach(({ item }) => {
            const li = doc.createElement("li");
            const a = doc.createElement("a");
            a.href = item.permalink;
            a.innerHTML =
                '<span class="sm-title">' + highlight(item.title || "", q) + "</span>" +
                '<span class="sm-snippet">' + snippet(item, q) + "</span>";
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
