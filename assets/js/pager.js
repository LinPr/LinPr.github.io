/* 列表页前端分页
   list.html 把全部文章一次性渲染进 .post-list[data-pager]，这里按每页 10 篇切片显示：
   - 一条液态玻璃胶囊：上一页 / 页码 / 下一页 │ 跳至第 N 页；
   - 当前页写进 URL（?page=2），可以分享、可以前进后退，← / → 也能翻页；
   - 当前页下面垫着一块会滑动、被拉长再回弹的「液滴」，翻页时文章依次浮入。
   没有 JS 时整个列表原样显示，导航保持 hidden。 */
(() => {
    "use strict";
    const doc = document;
    const list = doc.querySelector(".post-list[data-pager]");
    const nav = doc.querySelector("nav.pager");
    if (!list || !nav) return;

    const SIZE = 10;
    const items = Array.from(list.children).filter((el) => el.tagName === "ARTICLE");
    const total = items.length;
    const pages = Math.max(1, Math.ceil(total / SIZE));
    const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    const readPage = () => {
        const p = parseInt(new URLSearchParams(location.search).get("page"), 10) || 1;
        return Math.min(Math.max(p, 1), pages);
    };
    let page = readPage();

    /* 当前页附近的页码，其余用省略号折叠：1 … 4 5 [6] 7 8 … 20；窄屏只留左右各 1 个 */
    const pageWindow = (cur) => {
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

    /* 骨架只建一次，之后只换页码，这样液滴才能从旧位置滑到新位置 */
    nav.innerHTML =
        '<div class="pg-track">' +
        '<button class="pg-btn pg-step" type="button" data-go="prev" aria-label="上一页">' + icon("M15 18l-6-6 6-6") + "<span>上一页</span></button>" +
        '<div class="pg-nums"><span class="pg-blob" aria-hidden="true"></span></div>' +
        '<button class="pg-btn pg-step" type="button" data-go="next" aria-label="下一页"><span>下一页</span>' + icon("M9 18l6-6-6-6") + "</button>" +
        '<span class="pg-divider" aria-hidden="true"></span>' +
        '<form class="pg-jump">' +
        '<label><span class="pg-jump-text">跳至</span><input type="number" inputmode="numeric" min="1" max="' + pages + '" aria-label="跳转到第几页"><span class="pg-jump-text">页</span></label>' +
        '<button class="pg-btn pg-go" type="submit" aria-label="跳转">' + icon("M5 12h14M13 6l6 6-6 6") + "</button>" +
        "</form>" +
        "</div>";

    const nums = nav.querySelector(".pg-nums");
    const blob = nums.querySelector(".pg-blob");
    const jumpForm = nav.querySelector(".pg-jump");
    const jumpInput = jumpForm.querySelector("input");
    const prevBtn = nav.querySelector('[data-go="prev"]');
    const nextBtn = nav.querySelector('[data-go="next"]');

    /* 液滴移动到目标按钮下面，同时做一次「被拉长再回弹」的形变 */
    const moveBlob = (target, animate) => {
        if (!target) { blob.style.opacity = "0"; return; }
        const x = target.offsetLeft;
        const prev = parseFloat(blob.dataset.x);
        const smooth = animate && !reduceMotion && !Number.isNaN(prev);
        blob.classList.toggle("no-anim", !smooth);
        blob.style.opacity = "1";
        blob.style.width = target.offsetWidth + "px";
        blob.style.transform = "translateX(" + x + "px)";
        blob.dataset.x = x;
        if (smooth && prev !== x) {
            blob.classList.remove("is-stretch");
            void blob.offsetWidth;
            blob.classList.add("is-stretch");
        }
    };

    const render = (animate) => {
        const start = (page - 1) * SIZE;
        const end = Math.min(start + SIZE, total);

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

        nums.querySelectorAll(".pg-num, .pg-gap").forEach((el) => el.remove());
        let activeBtn = null;
        pageWindow(page).forEach((p) => {
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
            b.setAttribute("aria-label", "第 " + p + " 页，共 " + pages + " 页");
            if (p === page) {
                b.classList.add("is-active");
                b.setAttribute("aria-current", "page");
                activeBtn = b;
            }
            nums.appendChild(b);
        });

        prevBtn.disabled = page <= 1;
        nextBtn.disabled = page >= pages;
        jumpInput.placeholder = page + "/" + pages;
        moveBlob(activeBtn, animate);
    };

    const syncURL = (push) => {
        const url = new URL(location.href);
        if (page > 1) url.searchParams.set("page", page); else url.searchParams.delete("page");
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

    const go = (p) => {
        p = Math.min(Math.max(p, 1), pages);
        if (p === page) return;
        page = p;
        syncURL(true);
        render(true);
        scrollToList();
    };

    /* 点击涟漪 + 指针跟随高光 */
    const ripple = (btn, e) => {
        if (reduceMotion) return;
        const r = btn.getBoundingClientRect();
        const s = doc.createElement("span");
        const d = Math.max(r.width, r.height) * 2.2;
        s.className = "pg-ripple";
        s.style.width = s.style.height = d + "px";
        s.style.left = (e.clientX ? e.clientX - r.left : r.width / 2) - d / 2 + "px";
        s.style.top = (e.clientY ? e.clientY - r.top : r.height / 2) - d / 2 + "px";
        btn.appendChild(s);
        s.addEventListener("animationend", () => s.remove());
    };

    nav.addEventListener("pointermove", (e) => {
        const btn = e.target.closest(".pg-btn");
        if (!btn) return;
        const r = btn.getBoundingClientRect();
        btn.style.setProperty("--mx", ((e.clientX - r.left) / r.width) * 100 + "%");
        btn.style.setProperty("--my", ((e.clientY - r.top) / r.height) * 100 + "%");
    });

    nav.addEventListener("click", (e) => {
        const btn = e.target.closest("button");
        if (!btn || btn.disabled) return;
        ripple(btn, e);
        if (btn.dataset.go === "prev") go(page - 1);
        else if (btn.dataset.go === "next") go(page + 1);
        else if (btn.dataset.page) go(+btn.dataset.page);
    });

    jumpForm.addEventListener("submit", (e) => {
        e.preventDefault();
        const p = parseInt(jumpInput.value, 10);
        if (!p || p < 1 || p > pages) {
            jumpForm.classList.remove("is-shake");
            void jumpForm.offsetWidth;
            jumpForm.classList.add("is-shake");
            return;
        }
        jumpInput.value = "";
        jumpInput.blur();
        go(p);
    });

    doc.addEventListener("keydown", (e) => {
        if (e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return;
        if (/^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName) || e.target.isContentEditable) return;
        if (doc.documentElement.classList.contains("sm-open")) return;
        if (e.key === "ArrowLeft" && page > 1) go(page - 1);
        else if (e.key === "ArrowRight" && page < pages) go(page + 1);
    });

    window.addEventListener("popstate", () => {
        page = readPage();
        render(true);
    });

    let resizeTimer = 0;
    window.addEventListener("resize", () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(() => render(false), 150);
    });

    nav.hidden = pages <= 1;
    syncURL(false);
    render(false);
})();
