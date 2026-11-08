/* 白天背景：彩色落花雨 */
(() => {
    "use strict";

    const layer = document.getElementById("daylight");
    if (!layer) return;

    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (reduce) return;

    // 七种颜色，按权重出现（粉/玫红最常见，青最少）
    const PALETTE = [
        { src: "/images/petal-sakura.svg", weight: 24 },
        { src: "/images/petal-rose.svg",   weight: 18 },
        { src: "/images/petal-white.svg",  weight: 14 },
        { src: "/images/petal-yellow.svg", weight: 15 },
        { src: "/images/petal-orange.svg", weight: 12 },
        { src: "/images/petal-purple.svg", weight: 12 },
        { src: "/images/petal-mint.svg",   weight: 5 },
    ];

    const TOTAL_WEIGHT = PALETTE.reduce((sum, p) => sum + p.weight, 0);

    function pickPetal() {
        let r = Math.random() * TOTAL_WEIGHT;
        for (const p of PALETTE) {
            r -= p.weight;
            if (r <= 0) return p.src;
        }
        return PALETTE[0].src;
    }

    const narrow = window.innerWidth < 720;
    // 密度：桌面 72 片，窄屏 32 片
    let COUNT = narrow ? 32 : 72;
    const petals = [];

    function makePetal() {
        const el = document.createElement("div");
        el.className = "petal";
        el.style.backgroundImage = `url(${pickPetal()})`;

        // 尺寸分三档，形成远近层次
        const tier = Math.random();
        const size = tier < 0.5
            ? 10 + Math.random() * 8      // 远处小花瓣
            : tier < 0.85
                ? 17 + Math.random() * 9   // 中景
                : 26 + Math.random() * 9;  // 近景大花瓣

        // 小花瓣落得慢，大花瓣落得快
        const dur = (size < 18 ? 15 : size < 26 ? 12 : 9) + Math.random() * 8;
        const drift = Math.random() * 12 - 6;
        const blur = size < 14 ? Math.random() * 1.4 : Math.random() < 0.25 ? 0.5 : 0;
        const spin = (Math.random() < 0.5 ? 1 : -1) * (320 + Math.random() * 240);

        el.style.width = size + "px";
        el.style.height = size + "px";
        el.style.left = Math.random() * 100 + "vw";
        el.style.animationDuration = dur + "s";
        el.style.animationDelay = -(Math.random() * dur) + "s";
        if (blur) el.style.filter = `blur(${blur.toFixed(1)}px)`;
        el.style.setProperty("--drift", drift.toFixed(1) + "vw");
        el.style.setProperty("--spin", spin.toFixed(0) + "deg");
        layer.appendChild(el);
        return el;
    }

    for (let i = 0; i < COUNT; i++) petals.push(makePetal());

    function sync() {
        const dark = (document.documentElement.dataset.theme || "").toLowerCase() === "dark";
        petals.forEach((p) => { p.style.display = dark ? "none" : ""; });
    }

    new MutationObserver(sync).observe(document.documentElement, {
        attributes: true,
        attributeFilter: ["data-theme"],
    });
    sync();

    /* ---------- 自适应：帧率偏低时减少花瓣，保证不拖慢页面 ---------- */
    let frames = 0;
    let start = 0;
    let checked = false;

    function sampleFps(now) {
        if (checked) return;
        if (!start) start = now;
        frames += 1;
        const elapsed = now - start;
        if (elapsed < 2000) {
            requestAnimationFrame(sampleFps);
            return;
        }
        checked = true;
        const fps = (frames / elapsed) * 1000;
        // 低于 45fps 时逐步削减到 40%，最低保留 24 片
        if (fps < 45) {
            const keep = Math.max(24, Math.round(petals.length * 0.4));
            while (petals.length > keep) {
                const el = petals.pop();
                if (el && el.parentNode) el.parentNode.removeChild(el);
            }
        }
    }

    // 等首屏稳定后再采样，避免加载期的抖动误判
    setTimeout(() => requestAnimationFrame(sampleFps), 2500);
})();
