/* 点击烟花：在全屏 canvas 上放一小簇火星
   - 只响应按下（pointerdown），不干扰任何点击行为（canvas 是 pointer-events: none）
   - 尊重 prefers-reduced-motion；触屏不启用
   - 粒子放完就停掉 rAF，不用时零开销
*/
(() => {
    "use strict";

    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const coarse = window.matchMedia("(pointer: coarse)").matches;
    if (reduce || coarse) return;

    const canvas = document.createElement("canvas");
    canvas.id = "click-firework";
    canvas.setAttribute("aria-hidden", "true");
    document.body.appendChild(canvas);

    const ctx = canvas.getContext("2d", { alpha: true });
    if (!ctx) return;

    let dpr = 1;
    let width = 0;
    let height = 0;

    function resize() {
        dpr = Math.min(window.devicePixelRatio || 1, 2);
        width = window.innerWidth;
        height = window.innerHeight;
        canvas.width = Math.floor(width * dpr);
        canvas.height = Math.floor(height * dpr);
        canvas.style.width = width + "px";
        canvas.style.height = height + "px";
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
    resize();
    window.addEventListener("resize", resize, { passive: true });

    /* 配色跟着站点走：浅色偏靛紫粉金，深色偏亮青紫，保证在两种背景上都跳得出来 */
    const PALETTES = {
        light: ["#6366f1", "#a855f7", "#ec4899", "#f59e0b", "#22d3ee", "#f472b6"],
        dark: ["#c7d2fe", "#e9d5ff", "#fbcfe8", "#fde68a", "#a5f3fc", "#fce7f3"],
    };

    function palette() {
        const theme = (document.documentElement.dataset.theme || "").toLowerCase();
        return theme === "dark" ? PALETTES.dark : PALETTES.light;
    }

    const particles = [];
    const rings = [];
    const MAX_PARTICLES = 420;

    let raf = 0;
    let last = 0;

    function spawn(x, y) {
        const colors = palette();

        // 中心的一圈"爆开"火星
        const count = 40 + Math.floor(Math.random() * 12);
        for (let i = 0; i < count; i++) {
            if (particles.length >= MAX_PARTICLES) break;
            const angle = (Math.PI * 2 * i) / count + Math.random() * 0.22;
            const speed = 150 + Math.random() * 330;
            particles.push({
                x,
                y,
                vx: Math.cos(angle) * speed,
                vy: Math.sin(angle) * speed,
                life: 0.9 + Math.random() * 0.75,
                age: 0,
                size: 1.7 + Math.random() * 2.3,
                color: colors[(Math.random() * colors.length) | 0],
            });
        }

        // 几根飞得更远的"拉丝"，带一点重力感
        for (let i = 0; i < 10; i++) {
            if (particles.length >= MAX_PARTICLES) break;
            const angle = Math.random() * Math.PI * 2;
            const speed = 430 + Math.random() * 330;
            particles.push({
                x,
                y,
                vx: Math.cos(angle) * speed,
                vy: Math.sin(angle) * speed,
                life: 1.05 + Math.random() * 0.7,
                age: 0,
                size: 1.4 + Math.random() * 1.2,
                color: colors[(Math.random() * colors.length) | 0],
                streak: true,
            });
        }

        // 中心的一下闪光
        rings.push({
            x,
            y,
            r: 2,
            max: 26,
            age: 0,
            life: 0.13,
            flash: true,
        });

        start();
    }

    function start() {
        if (raf) return;
        last = performance.now();
        raf = requestAnimationFrame(tick);
    }

    function tick(now) {
        const dt = Math.min((now - last) / 1000, 0.05);
        last = now;

        ctx.clearRect(0, 0, width, height);

        // 火星：加色混合，叠在一起更亮
        ctx.globalCompositeOperation = "lighter";

        for (let i = particles.length - 1; i >= 0; i--) {
            const p = particles[i];
            p.age += dt;
            if (p.age >= p.life) {
                particles.splice(i, 1);
                continue;
            }

            const px = p.x;
            const py = p.y;

            // 重力 + 空气阻力（指数衰减，和帧率无关）
            const damp = Math.exp(-2.4 * dt);
            p.vx *= damp;
            p.vy = p.vy * damp + 520 * dt;
            p.x += p.vx * dt;
            p.y += p.vy * dt;

            const t = 1 - p.age / p.life;
            const alpha = t * t;

            ctx.globalAlpha = alpha;
            ctx.strokeStyle = p.color;
            ctx.fillStyle = p.color;
            ctx.lineCap = "round";

            // 拖尾：从上一帧位置拉一条线过去，像一颗飞出去的火星
            const tail = p.streak ? 1 : 0.55;
            ctx.globalAlpha = Math.min(1, alpha * 1.2);
            ctx.lineWidth = p.size * tail;
            ctx.beginPath();
            ctx.moveTo(px, py);
            ctx.lineTo(p.x, p.y);
            ctx.stroke();

            // 亮头
            ctx.globalAlpha = Math.min(1, alpha * 1.45);
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size * 0.55, 0, Math.PI * 2);
            ctx.fill();
        }

        // 光环
        for (let i = rings.length - 1; i >= 0; i--) {
            const r = rings[i];
            r.age += dt;
            if (r.age >= r.life) {
                rings.splice(i, 1);
                continue;
            }
            const t = r.age / r.life;
            const radius = r.r + (r.max - r.r) * (1 - Math.pow(1 - t, 3));

            if (r.flash) {
                const g = ctx.createRadialGradient(r.x, r.y, 0, r.x, r.y, radius);
                const c = palette()[0];
                g.addColorStop(0, c);
                g.addColorStop(1, "transparent");
                ctx.globalAlpha = (1 - t) * 0.55;
                ctx.fillStyle = g;
                ctx.beginPath();
                ctx.arc(r.x, r.y, radius, 0, Math.PI * 2);
                ctx.fill();
                ctx.strokeStyle = c;
                ctx.lineWidth = 2 * (1 - t);
                ctx.globalAlpha = (1 - t) * 0.7;
                ctx.beginPath();
                ctx.arc(r.x, r.y, radius, 0, Math.PI * 2);
                ctx.stroke();
                continue;
            }

            ctx.globalAlpha = (1 - t) * 0.5;
            ctx.strokeStyle = palette()[0];
            ctx.lineWidth = 2.2 * (1 - t) + 0.5;
            ctx.beginPath();
            ctx.arc(r.x, r.y, radius, 0, Math.PI * 2);
            ctx.stroke();
        }

        ctx.globalAlpha = 1;
        ctx.globalCompositeOperation = "source-over";

        if (particles.length || rings.length) {
            raf = requestAnimationFrame(tick);
        } else {
            ctx.clearRect(0, 0, width, height);
            raf = 0;
        }
    }

    window.addEventListener(
        "pointerdown",
        (e) => {
            if (e.button !== 0) return;
            spawn(e.clientX, e.clientY);
        },
        { passive: true }
    );

    // 键盘用户按回车/空格激活时，从焦点元素的中心放一发
    window.addEventListener("keydown", (e) => {
        if (e.key !== "Enter" && e.key !== " ") return;
        const el = document.activeElement;
        if (!el || el === document.body) return;
        const r = el.getBoundingClientRect();
        if (!r.width && !r.height) return;
        spawn(r.left + r.width / 2, r.top + r.height / 2);
    });
})();
