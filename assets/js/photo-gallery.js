/* 摄影板块的灯箱
   - 点击缩略图打开，←/→ 或左右滑动切换，Esc / 点空白处关闭；
   - 只在当前相册筛选结果里切换；
   - 预加载前后各一张大图；
   - 没有 JS 时缩略图链接直接打开大图。 */
(() => {
    "use strict";
    const doc = document;
    const grid = doc.querySelector("[data-photo-gallery]");
    const box = doc.querySelector(".photo-lightbox");
    if (!grid || !box) return;

    const img = box.querySelector(".pl-img");
    const stage = box.querySelector(".pl-stage");
    const el = (sel) => box.querySelector(sel);
    const items = Array.from(grid.querySelectorAll(".photo-item"));
    let visible = items;
    let cur = -1;
    let lastFocus = null;

    /* ---------- 相册筛选 ---------- */
    const filter = doc.querySelector(".photo-filter");
    if (filter) {
        filter.addEventListener("click", (e) => {
            const btn = e.target.closest("button");
            if (!btn) return;
            const album = btn.dataset.album;
            filter.querySelectorAll("button").forEach((b) => b.classList.toggle("is-active", b === btn));
            items.forEach((it) => { it.hidden = !!album && it.dataset.album !== album; });
            visible = items.filter((it) => !it.hidden);
        });
    }

    const linkOf = (i) => visible[i].querySelector("a");
    const preload = (i) => {
        if (i < 0 || i >= visible.length) return;
        const im = new Image();
        im.src = linkOf(i).href;
    };

    const show = (i) => {
        if (!visible.length) return;
        cur = (i + visible.length) % visible.length;
        const a = linkOf(cur);
        const d = a.dataset;
        stage.classList.add("is-loading");
        img.classList.remove("is-ready");
        img.onload = () => { stage.classList.remove("is-loading"); img.classList.add("is-ready"); };
        img.onerror = () => stage.classList.remove("is-loading");
        img.src = a.href;
        img.alt = d.title || "摄影作品";
        el(".pl-title").textContent = d.title || "";
        el(".pl-meta").textContent = [d.meta, d.camera, d.exif].filter(Boolean).join("  ·  ");
        el(".pl-desc").textContent = d.desc || "";
        el(".pl-count").textContent = `${cur + 1} / ${visible.length}`;
        el(".pl-original").href = d.original || a.href;
        box.classList.toggle("is-single", visible.length < 2);
        preload(cur + 1);
        preload(cur - 1);
    };

    const open = (i) => {
        lastFocus = doc.activeElement;
        box.hidden = false;
        doc.documentElement.classList.add("pl-open");
        requestAnimationFrame(() => box.classList.add("is-open"));
        show(i);
        el(".pl-close").focus();
    };

    const close = () => {
        box.classList.remove("is-open");
        doc.documentElement.classList.remove("pl-open");
        box.hidden = true;
        img.removeAttribute("src");
        if (lastFocus && lastFocus.focus) lastFocus.focus();
    };

    grid.addEventListener("click", (e) => {
        const a = e.target.closest("a");
        if (!a || e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;
        e.preventDefault();
        const idx = visible.indexOf(a.closest(".photo-item"));
        if (idx >= 0) open(idx);
    });

    box.addEventListener("click", (e) => {
        if (e.target.closest(".pl-close")) close();
        else if (e.target.closest(".pl-prev")) show(cur - 1);
        else if (e.target.closest(".pl-next")) show(cur + 1);
        else if (e.target === box || e.target === stage) close();
    });

    doc.addEventListener("keydown", (e) => {
        if (box.hidden) return;
        if (e.key === "Escape") close();
        else if (e.key === "ArrowLeft") show(cur - 1);
        else if (e.key === "ArrowRight") show(cur + 1);
    });

    /* 触屏左右滑动切换 */
    let sx = 0, sy = 0;
    box.addEventListener("touchstart", (e) => { sx = e.touches[0].clientX; sy = e.touches[0].clientY; }, { passive: true });
    box.addEventListener("touchend", (e) => {
        const dx = e.changedTouches[0].clientX - sx;
        const dy = e.changedTouches[0].clientY - sy;
        if (Math.abs(dx) > 50 && Math.abs(dx) > Math.abs(dy) * 1.5) show(cur + (dx < 0 ? 1 : -1));
    }, { passive: true });
})();
