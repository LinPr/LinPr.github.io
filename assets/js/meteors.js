/* 两侧留白处的流星雨：长距离、四边随机发射 */
(() => {
    "use strict";

    const canvas = document.getElementById("sky");
    if (!canvas) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const rand = (a, b) => a + Math.random() * (b - a);
    const pick = (list) => list[Math.floor(Math.random() * list.length)];

    const palette = [
        [255, 255, 255],
        [255, 255, 255],
        [186, 200, 255],
        [165, 180, 252],
        [147, 197, 253],
        [103, 232, 249],
        [192, 132, 252],
        [244, 114, 182],
        [251, 191, 36],
        [45, 212, 191],
    ];

    let W = 0;
    let H = 0;
    let dpr = 1;
    let gutter = null;
    let raf = 0;
    let last = 0;
    let nextSpawn = 0;

    const meteors = [];
    const MAX_METEORS = 26;
    const PAD = 60; // 允许越界一点，出画后再回收

    function measure() {
        dpr = Math.min(window.devicePixelRatio || 1, 2);
        W = window.innerWidth;
        H = window.innerHeight;
        canvas.width = Math.floor(W * dpr);
        canvas.height = Math.floor(H * dpr);
        canvas.style.width = W + "px";
        canvas.style.height = H + "px";
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

        const main = document.querySelector(".main");
        const rect = main ? main.getBoundingClientRect() : null;
        const pad = 24;

        if (rect && rect.width > 0 && rect.left > 140 && W - rect.right > 140) {
            gutter = {
                l: [pad, rect.left - pad],
                r: [rect.right + pad, W - pad],
            };
        } else {
            gutter = null;
        }
    }

    // 光线与矩形求交，返回出口参数 t（起点在盒内）
    function rayExit(x, y, vx, vy, box) {
        let t = Infinity;
        if (vx > 0) t = Math.min(t, (box.x1 - x) / vx);
        else if (vx < 0) t = Math.min(t, (box.x0 - x) / vx);
        if (vy > 0) t = Math.min(t, (box.y1 - y) / vy);
        else if (vy < 0) t = Math.min(t, (box.y0 - y) / vy);
        return t === Infinity ? 0 : Math.max(t, 0);
    }

    function spawn(side) {
        if (!gutter || meteors.length >= MAX_METEORS) return;

        const [x0, x1] = gutter[side];
        const box = { x0, x1, y0: -PAD, y1: H + PAD };
        const inner = side === "l" ? x1 : x0;
        const outer = side === "l" ? x0 : x1;

        // 四条边都可能发射
        const roll = Math.random();
        let x;
        let y;
        let baseAngle; // 与竖直方向的夹角，正为朝页内

        if (roll < 0.5) {
            // 上边沿
            x = rand(x0, x1);
            y = box.y0 - rand(0, 90);
            baseAngle = rand(-0.62, 0.62);
        } else if (roll < 0.68) {
            // 下边沿：向上飞
            x = rand(x0, x1);
            y = box.y1 + rand(0, 90);
            baseAngle = Math.PI + rand(-0.62, 0.62);
        } else if (roll < 0.86) {
            // 外侧边：朝页面中间飞
            x = outer + (side === "l" ? -rand(0, 40) : rand(0, 40));
            y = rand(0, H);
            baseAngle = side === "l" ? rand(0.15, 1.15) : -rand(0.15, 1.15);
        } else {
            // 内侧边：朝页面外侧飞
            x = inner + (side === "l" ? rand(0, 30) : -rand(0, 30));
            y = rand(0, H);
            baseAngle = side === "l" ? -rand(0.15, 1.15) : rand(0.15, 1.15);
        }

        // 方向向量（angle = 0 指向正下方）
        const vx = Math.sin(baseAngle);
        const vy = Math.cos(baseAngle);

        // 走到出界的总距离 —— 这就是这一颗的“划过距离”
        const travel = rayExit(x, y, vx, vy, box);
        if (travel < 180) return; // 太短的丢掉，保证每颗都划得远

        const fireball = Math.random() < 0.08;
        const duration = fireball ? rand(2.4, 4.2) : rand(1.5, 3.4);
        const speed = travel / duration;

        meteors.push({
            x,
            y,
            vx: vx * speed,
            vy: vy * speed,
            travel,
            len: Math.min(travel * rand(0.42, 0.78), 820),
            width: fireball ? rand(5, 9) : rand(1.6, 4),
            color: fireball ? pick([[255, 236, 200], [255, 214, 170], [255, 255, 255]]) : pick(palette),
            life: 0,
            maxLife: duration,
            fireball,
        });
    }

    function drawMeteor(m) {
        const speed = Math.hypot(m.vx, m.vy) || 1;
        const ux = m.vx / speed;
        const uy = m.vy / speed;
        const tx = m.x - ux * m.len;
        const ty = m.y - uy * m.len;
        const nx = -uy;
        const ny = ux;
        const w = m.width;
        // 头部出现、尾部渐隐，避免凭空弹出
        const fade = Math.min(1, m.life / 0.25) * Math.min(1, (m.maxLife - m.life) / 0.45);
        const [r, g, b] = m.color;

        const grad = ctx.createLinearGradient(m.x, m.y, tx, ty);
        grad.addColorStop(0, `rgba(${r},${g},${b},${(0.98 * fade).toFixed(3)})`);
        grad.addColorStop(0.18, `rgba(${r},${g},${b},${(0.62 * fade).toFixed(3)})`);
        grad.addColorStop(0.55, `rgba(${r},${g},${b},${(0.22 * fade).toFixed(3)})`);
        grad.addColorStop(1, `rgba(${r},${g},${b},0)`);

        ctx.save();
        ctx.shadowColor = `rgba(${r},${g},${b},${(0.8 * fade).toFixed(3)})`;
        ctx.shadowBlur = m.fireball ? 26 : 14;
        ctx.beginPath();
        ctx.moveTo(m.x + nx * (w / 2), m.y + ny * (w / 2));
        ctx.lineTo(tx, ty);
        ctx.lineTo(m.x - nx * (w / 2), m.y - ny * (w / 2));
        ctx.closePath();
        ctx.fillStyle = grad;
        ctx.fill();

        ctx.beginPath();
        ctx.arc(m.x, m.y, w * 0.8, 0, Math.PI * 2);
        ctx.fillStyle = `rgba(255,255,255,${(0.95 * fade).toFixed(3)})`;
        ctx.fill();
        ctx.restore();
    }

    function update(dt) {
        for (let i = meteors.length - 1; i >= 0; i--) {
            const m = meteors[i];
            m.x += m.vx * dt;
            m.y += m.vy * dt;
            m.life += dt;
            if (m.life > m.maxLife || m.y < -400 || m.y > H + 400 || m.x < -300 || m.x > W + 300) {
                meteors.splice(i, 1);
            }
        }
    }

    function loop(t) {
        raf = requestAnimationFrame(loop);

        const dt = Math.min(0.05, (t - last) / 1000 || 0.016);
        last = t;

        ctx.clearRect(0, 0, W, H);
        if (!gutter) {
            cancelAnimationFrame(raf);
            raf = 0;
            return;
        }
        if (document.hidden) return;

        // 白天只放花瓣，不画流星
        const isDay = (document.documentElement.dataset.theme || "").toLowerCase() === "light";
        if (isDay) {
            // 清掉残留
            if (meteors.length) meteors.length = 0;
            return;
        }

        if (!reduceMotion && t > nextSpawn) {
            spawn(Math.random() < 0.5 ? "l" : "r");
            if (Math.random() < 0.5) spawn(Math.random() < 0.5 ? "l" : "r");
            if (Math.random() < 0.15) {
                spawn(Math.random() < 0.5 ? "l" : "r");
                spawn(Math.random() < 0.5 ? "l" : "r");
            }
            nextSpawn = t + rand(220, 700);
        }

        update(dt);
        for (const m of meteors) drawMeteor(m);
    }

    function start() {
        measure();
        last = performance.now();
        nextSpawn = last + 400;
        if (reduceMotion) {
            ctx.clearRect(0, 0, W, H);
            return;
        }
        if (!raf) raf = requestAnimationFrame(loop);
    }

    let resizeTimer = 0;
    window.addEventListener("resize", () => {
        clearTimeout(resizeTimer);
        resizeTimer = window.setTimeout(start, 160);
    });

    document.addEventListener("visibilitychange", () => {
        if (!document.hidden && !raf && !reduceMotion && gutter) {
            last = performance.now();
            raf = requestAnimationFrame(loop);
        }
    });

    start();
})();
