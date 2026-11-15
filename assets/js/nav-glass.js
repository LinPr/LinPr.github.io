/* 顶栏菜单液滴：一块玻璃垫在当前栏目下面，鼠标/键盘焦点移到别的菜单项时滑过去，
   离开后回到当前栏目（不在任何栏目里就淡出）。样式见 assets/css/extended/nav-glass.css。 */
(() => {
    "use strict";
    const menu = document.getElementById("menu");
    if (!menu) return;
    const links = Array.from(menu.querySelectorAll("li > a"));
    if (!links.length) return;
    const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    const blob = document.createElement("li");
    blob.className = "nav-blob no-anim";
    blob.setAttribute("aria-hidden", "true");
    blob.setAttribute("role", "presentation");
    menu.appendChild(blob);
    menu.classList.add("has-blob");

    /* 当前栏目：路径以菜单链接开头的那一项（/posts/xxx/ 不算任何栏目，/categories/k8s/ 算「分类」） */
    const here = location.pathname;
    const current = links.find((a) => {
        const p = new URL(a.href, location.href).pathname;
        return p !== "/" && here.startsWith(p) && !p.endsWith("/search/");
    }) || null;
    if (current) current.classList.add("is-current");

    let at = null;
    const place = (link, animate) => {
        if (!link) {
            blob.classList.remove("is-on");
            at = null;
            return;
        }
        const smooth = animate && !reduceMotion && at && at !== link;
        blob.classList.toggle("no-anim", !animate || reduceMotion || !at);
        blob.style.width = link.offsetWidth + "px";
        blob.style.height = link.offsetHeight + "px";
        blob.style.transform = "translate(" + link.offsetLeft + "px," + link.offsetTop + "px)";
        if (!at) void blob.offsetWidth; // 先就位再淡入，避免从左上角飞过来
        blob.classList.remove("no-anim");
        blob.classList.add("is-on");
        if (smooth) {
            blob.classList.remove("is-stretch");
            void blob.offsetWidth;
            blob.classList.add("is-stretch");
        }
        at = link;
    };

    links.forEach((a) => {
        a.addEventListener("pointerenter", () => place(a, true));
        a.addEventListener("focus", () => place(a, true));
    });
    menu.addEventListener("pointerleave", () => place(current, true));
    menu.addEventListener("focusout", (e) => {
        if (!menu.contains(e.relatedTarget)) place(current, true);
    });

    let t = 0;
    window.addEventListener("resize", () => {
        clearTimeout(t);
        t = setTimeout(() => { const keep = at; at = null; place(keep, false); }, 120);
    });

    /* 字体加载完成后菜单宽度可能变，重新量一次 */
    const init = () => { at = null; place(current, false); };
    init();
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(init);
})();
