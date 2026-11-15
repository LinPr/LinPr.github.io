/* 列表页前端分页
   list.html 把全部文章一次性渲染进 .post-list[data-pager]，这里负责：
   - 按「每页条数」切片显示，页码、上一页/下一页、跳页输入框；
   - 状态写进 URL（?page=2&size=20），可以分享、可以前进后退；
   - 每页条数记在 localStorage，换个列表页也沿用；
   - 液态玻璃风格的指示块在页码之间滑动，翻页时文章依次浮入。
   没有 JS 时整个列表原样显示，导航保持 hidden。 */
(() => {
    "use strict";
    const doc = document;
    const list = doc.querySelector(".post-list[data-pager]");
    const nav = doc.querySelector("nav.pager");
    if (!list || !nav) return;

    const items = Array.from(list.children).filter((el) => el.tagName === "ARTICLE");
    const total = items.length;
    const SIZES = [5, 10, 20, 50];
    const DEFAULT_SIZE = 10;
    const KEY = "pager-size";
    const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    const store = {
        get() { try { return parseInt(localStorage.getItem(KEY), 10); } catch (_) { return NaN; } },
        set(v) { try { localStorage.setItem(KEY, String(v)); } catch (_) { /* 隐私模式 */ } },
    };

    const clampSize = (n) => (SIZES.includes(n) ? n : DEFAULT_SIZE);
    const readState = () => {
        const q = new URLSearchParams(location.search);
        const size = clampSize(parseInt(q.get("size"), 10) || store.get() || DEFAULT_SIZE);
        const pages = Math.max(1, Math.ceil(total / size));
        const page = Math.min(Math.max(parseInt(q.get("page"), 10) || 1, 1), pages);
        return { page, size, pages };
    };

    let state = readState();

    /* 当前页附近的页码，其余用省略号折叠：1 … 4 5 [6] 7 8 … 20 */
    const pageWindow = (cur, pages) => {
        const span = window.innerWidth < 520 ? 1 : 2;
        const out = [];
        let last = 0;
        for (let p = 1; p <= pages; p++) {
            if (p === 1 || p === pages || Math.abs(p - cur) <= span) {
                if (last && p - last > 1) out.push(p - last === 2 ? last + 1 : 0);
                out.push(p);
                last = p;
            }
        }
        return out;
    };

    const icon = (d) =>
        '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2.2" ' +
        'stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="' + d + '"/></svg>';

    /* 骨架只建一次，之后只更新内容，这样指示块才能从旧位置滑到新位置 */
    nav.innerHTML =
        '<div class="pg-track">' +
        '  <button class="pg-btn pg-step" type="button" data-go="prev" aria-label="上一页">' + icon("M15 18l-6-6 6-6") + '<span>上一页</span></button>' +
        '  <div class="pg-nums" role="list"><span class="pg-blob" aria-hidden="true"></span></div>' +
        '  <button class="pg-btn pg-step" type="button" data-go="next" aria-label="下一页"><span>下一页</span>' + icon("M9 18l6-6-6-6") + '</button>' +
        '</div>' +
        '<div class="pg-tools">' +
        '  <p class="pg-info" aria-live="polite"></p>' +
        '  <div class="pg-size" role="radiogroup" aria-label="每页条数"><span class="pg-blob" aria-hidden="true"></span></div>' +
        '  <form class="pg-jump">' +
        '    <label>跳至<input type="number" inputmode="numeric" min="1" aria-label="跳转到第几页">页</label>' +
        '    <button class="pg-btn pg-go" type="submit" aria-label="跳转">' + icon("M5 12h14M13 6l6 6-6 6") + '</button>' +
        '  </form>' +
        '</div>';

    const nums = nav.querySelector(".pg-nums");
    const numBlob = nums.querySelector(".pg-blob");
    const sizeBox = nav.querySelector(".pg-size");
    const sizeBlob = sizeBox.querySelector(".pg-blob");
    const info = nav.querySelector(".pg-info");
    const jumpForm = nav.querySelector(".pg-jump");
    const jumpInput = jumpForm.querySelector("input");
    const prevBtn = nav.querySelector('[data-go="prev"]');
    const nextBtn = nav.querySelector('[data-go="next"]');

    SIZES.forEach((n) => {
        const b = doc.createElement("button");
        b.type = "button";
        b.className = "pg-chip";
        b.dataset.size = n;
        b.setAttribute("role", "radio");
        b.textContent = n + " 篇";
        sizeBox.appendChild(b);
    });

    /* 指示块：移动到目标按钮下面，同时做一次「被拉长再回弹」的液体形变 */
    const moveBlob = (blob, target, animate) => {
        if (!target) { blob.style.opacity = "0"; return; }
        const x = target.offsetLeft;
        const w = target.offsetWidth;
        const prev = parseFloat(blob.dataset.x || "NaN");
        blob.style.opacity = "1";
        if (!animate || reduceMotion || Number.isNaN(prev)) blob.classList.add("no-anim");
        blob.style.width = w + "px";
        blob.style.transform = "translateX(" + x + "px)";
        blob.dataset.x = x;
        if (animate && !reduceMotion && !Number.isNaN(prev) && prev !== x) {
            blob.classList.remove("is-stretch");
            void blob.offsetWidth;
            blob.classList.add("is-stretch");
        }
        requestAnimationFrame(() => blob.classList.remove("no-anim"));
    };

    const render = (animate) => {
        const { page, size, pages } = state;
        const start = (page - 1) * size;
        const end = Math.min(start + size, total);

        items.forEach((el, i) => {
            const show = i >= start && i < end;
            el.hidden = !show;
            if (show && animate && !reduceMotion) {
                el.classList.add("is-visible");
                el.classList.remove("pg-enter");
                el.style.transitionDelay = "";
                el.style.animationDelay = (i - start) * 45 + "ms";
                void el.offsetWidth;
                el.classList.add("pg-enter");
            }
        });

        /* 只有一页时不需要页码和跳转，但保留每页条数，方便切回去 */
        nav.hidden = total <= SIZES[0];
        nav.classList.toggle("is-single", pages <= 1);

        nums.querySelectorAll(".pg-num, .pg-gap").forEach((el) => el.remove());
        let activeBtn = null;
        pageWindow(page, pages).forEach((p) => {
            if (p === 0) {
                const gap = doc.createElement("span");
                gap.className = "pg-gap";
                gap.textContent = "…";
                gap.setAttribute("aria-hidden", "true");
                nums.appendChild(gap);
                return;
            }
            const b = doc.createElement("button");
            b.type = "button";
            b.className = "pg-btn pg-num";
            b.dataset.page = p;
            b.textContent = p;
            b.setAttribute("role", "listitem");
            b.setAttribute("aria-label", "第 " + p + " 页");
            if (p === page) {
                b.classList.add("is-active");
                b.setAttribute("aria-current", "page");
                activeBtn = b;
            }
            nums.appendChild(b);
        });

        prevBtn.disabled = page <= 1;
        nextBtn.disabled = page >= pages;
        info.innerHTML =
            '第 <b class="pg-cur">' + page + "</b> / " + pages + " 页" +
            '<span class="pg-sep">·</span>第 ' + (total ? start + 1 : 0) + "–" + end + " 篇" +
            '<span class="pg-sep">·</span>共 ' + total + " 篇";
        jumpInput.max = pages;
        jumpInput.placeholder = String(page);

        let activeChip = null;
        sizeBox.querySelectorAll(".pg-chip").forEach((c) => {
            const on = +c.dataset.size === size;
            c.classList.toggle("is-active", on);
            c.setAttribute("aria-checked", on ? "true" : "false");
            if (on) activeChip = c;
        });

        moveBlob(numBlob, activeBtn, animate);
        moveBlob(sizeBlob, activeChip, animate);
    };

    const syncURL = (push) => {
        const url = new URL(location.href);
        const { page, size } = state;
        if (page > 1) url.searchParams.set("page", page); else url.searchParams.delete("page");
        if (size !== DEFAULT_SIZE) url.searchParams.set("size", size); else url.searchParams.delete("size");
        if (url.href === location.href) return;
        history[push ? "pushState" : "replaceState"](null, "", url);
    };

    const scrollToList = () => {
        const header = doc.querySelector(".header");
        const offset = (header ? header.offsetHeight : 0) + 16;
        const top = list.getBoundingClientRect().top + window.scrollY - offset;
        if (window.scrollY > top) {
            window.scrollTo({ top: Math.max(top, 0), behavior: reduceMotion ? "auto" : "smooth" });
        }
    };

    const go = (page, size) => {
        const nextSize = clampSize(size || state.size);
        const pages = Math.max(1, Math.ceil(total / nextSize));
        const nextPage = Math.min(Math.max(page, 1), pages);
        if (nextPage === state.page && nextSize === state.size) return;
        state = { page: nextPage, size: nextSize, pages };
        syncURL(true);
        render(true);
        scrollToList();
    };

    /* 点击涟漪 + 指针跟随高光：给玻璃按钮一点触感 */
    const ripple = (btn, e) => {
        if (reduceMotion) return;
        const r = btn.getBoundingClientRect();
        const s = doc.createElement("span");
        s.className = "pg-ripple";
        const d = Math.max(r.width, r.height) * 2.2;
        s.style.width = s.style.height = d + "px";
        s.style.left = (e.clientX ? e.clientX - r.left : r.width / 2) - d / 2 + "px";
        s.style.top = (e.clientY ? e.clientY - r.top : r.height / 2) - d / 2 + "px";
        btn.appendChild(s);
        s.addEventListener("animationend", () => s.remove());
    };

    nav.addEventListener("pointermove", (e) => {
        const btn = e.target.closest(".pg-btn, .pg-chip");
        if (!btn) return;
        const r = btn.getBoundingClientRect();
        btn.style.setProperty("--mx", ((e.clientX - r.left) / r.width) * 100 + "%");
        btn.style.setProperty("--my", ((e.clientY - r.top) / r.height) * 100 + "%");
    });

    nav.addEventListener("click", (e) => {
        const btn = e.target.closest("button");
        if (!btn || btn.disabled) return;
        if (btn.matches(".pg-btn, .pg-chip")) ripple(btn, e);
        if (btn.dataset.go === "prev") go(state.page - 1);
        else if (btn.dataset.go === "next") go(state.page + 1);
        else if (btn.dataset.page) go(+btn.dataset.page);
        else if (btn.dataset.size) {
            const size = +btn.dataset.size;
            store.set(size);
            /* 换每页条数时尽量停留在原来看到的第一篇所在的页 */
            const first = (state.page - 1) * state.size;
            go(Math.floor(first / size) + 1, size);
        }
    });

    jumpForm.addEventListener("submit", (e) => {
        e.preventDefault();
        const p = parseInt(jumpInput.value, 10);
        if (!p) { jumpForm.classList.remove("is-shake"); void jumpForm.offsetWidth; jumpForm.classList.add("is-shake"); return; }
        jumpInput.value = "";
        jumpInput.blur();
        go(p);
    });

    /* 不在输入框里时，← / → 翻页 */
    doc.addEventListener("keydown", (e) => {
        if (e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return;
        if (/^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName) || e.target.isContentEditable) return;
        if (doc.documentElement.classList.contains("sm-open")) return;
        if (e.key === "ArrowLeft" && state.page > 1) go(state.page - 1);
        else if (e.key === "ArrowRight" && state.page < state.pages) go(state.page + 1);
    });

    window.addEventListener("popstate", () => {
        state = readState();
        render(true);
    });

    let resizeTimer = 0;
    window.addEventListener("resize", () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(() => render(false), 150);
    });

    syncURL(false);
    render(false);
    list.classList.add("is-paged");
})();
