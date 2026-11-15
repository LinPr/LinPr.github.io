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
    let leaveTimer = 0;
    const place = (link, animate, hover) => {
        clearTimeout(leaveTimer);
        blob.classList.toggle("is-hover", !!(hover && link));
        if (link === at) return;
        if (!link) {
            blob.classList.remove("is-on");
            at = null;
            return;
        }
        /* 从隐藏状态出现时先瞬移就位再淡入，避免从别处飞过来 */
        const jump = !animate || reduceMotion || !at;
        blob.classList.toggle("no-anim", jump);
        /* 菜单链接和顶栏一样高，液滴上下各留一点空，看起来是浮在玻璃里的一颗 */
        const h = Math.min(link.offsetHeight, 32);
        const y = link.offsetTop + (link.offsetHeight - h) / 2;
        blob.style.width = link.offsetWidth + "px";
        blob.style.height = h + "px";
        blob.style.transform = "translate3d(" + link.offsetLeft + "px," + y + "px,0)";
        if (jump) void blob.offsetWidth;
        blob.classList.remove("no-anim");
        blob.classList.add("is-on");
        at = link;
    };

    /* 离开菜单时稍等一下再回到当前栏目：鼠标擦过菜单项之间的缝隙、
       或者短暂移出又移回来时，液滴不会来回跳 */
    const back = () => {
        clearTimeout(leaveTimer);
        blob.classList.remove("is-hover");
        leaveTimer = setTimeout(() => place(current, true), 160);
    };

    links.forEach((a) => {
        a.addEventListener("pointerenter", () => place(a, true, true));
        a.addEventListener("focus", () => place(a, true, true));
    });
    menu.addEventListener("pointerleave", back);
    menu.addEventListener("focusout", (e) => {
        if (!menu.contains(e.relatedTarget)) back();
    });

    let t = 0;
    window.addEventListener("resize", () => {
        clearTimeout(t);
        t = setTimeout(() => { const keep = at || current; at = null; place(keep, false); }, 120);
    });

    /* 字体加载完成后菜单宽度可能变，重新量一次 */
    const init = () => { at = null; place(current, false); };
    init();
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(init);
})();
