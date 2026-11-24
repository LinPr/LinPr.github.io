/* 桌宠：Peekling（https://peekling.com/，Apache-2.0）
   自托管运行时与角色包，角色是 Pip（暖金色柯基），固定蹲在页面左下角。

   参考官方文档 docs/configuration.md：
   - interaction 默认开启（可拖动/抛掷，按下切换内容气泡），
     只有传入 indicator 才会渲染红色未读角标，所以这里不传；
   - 内容气泡只从我们提供的 content 里取，配了 contentInitiallyHidden 时
     点一下才会出现。

   和运行时打交道时踩到的几件事，记在这里免得以后又踩：
   1. 它用 crypto.subtle 校验资源哈希，而 crypto.subtle 只在安全上下文
      （HTTPS 或 localhost）可用 —— 用局域网 IP + http 预览时它起不来，属正常；
   2. 运行时的样式注入在 shadow root 里，页面级 CSS 覆盖不掉
      （shadow 里的 !important 反向压过外部样式表），
      所以气泡外观改的是运行时加载的那份样式表：peekling-lite.css；
   3. 全局只允许一份 styles 配置，重复 hatch 时 url 必须一致，否则报
      visibility.styles-conflict。
*/
(() => {
    "use strict";

    // 尊重系统的「减少动态效果」：直接把控制权交还给访客，不启动
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    // Peekling 用 crypto.subtle 校验资源哈希，它只在安全上下文可用。
    // 用局域网 IP + http 预览时不满足条件，直接跳过，免得控制台刷警告。
    const canVerify =
        window.isSecureContext && typeof crypto !== "undefined" && crypto.subtle;
    if (!canVerify) return;

    /* 触屏设备没有指针可跟，宠物只会缩在角落里挡内容，直接不启动。
       只认 pointer: coarse —— 一些桌面环境（含无头浏览器）会把 hover 报成 none，
       用 hover 判断会误杀桌面端。 */
    const noPointer = window.matchMedia("(pointer: coarse)").matches;
    if (noPointer) return;

    /* 宠物固定待在页面左下角，不再跟随鼠标：
       baseline 只设置 idle 状态、不给 motion，运行时就不会移动它；
       也不订阅 pointer.move / 页面级 pointer.click，鼠标在页面上怎么动都与它无关。
       只有点到宠物本身时，interaction.press 会切换下面的问候气泡。 */
    const PLAN = {
        baseline: {
            channels: ["state"],
            state: { state: "idle" },
        },
        rules: [],
    };

    // 点开气泡时说的话。引擎按 content id 取，这里只放一条常驻的。
    const CONTENT = {
        hello: {
            bottom: {
                title: "Pip",
                text: "我在这里陪你看文章。",
            },
        },
    };

    function boot() {
        if (typeof window.Peekling === "undefined") return;

        let companion;
        try {
            companion = window.Peekling.hatch({
                packUrl: "/vendor/peekling/packs/pip/character.json",
                plan: PLAN,
                position: "bottom-left",
                maxDensity: 2,
                name: "pip",
                motionPreference: reduce ? "reduce" : "system",
                // 气泡内容：只在按下时出现
                content: CONTENT,
                interaction: {
                    press: "toggle-content",
                    contentInitiallyHidden: true,
                    label: "和 Pip 打个招呼",
                    drag: false,
                    throw: false,
                },
                // 用我们自己的样式表：给气泡套上站点的液态玻璃外观
                styles: { url: "/vendor/peekling/peekling-lite.css" },
                diagnostics: { console: false },
            });
        } catch (err) {
            // Peekling 是可选装饰，任何异常都不应该影响正文
            return;
        }

        companion.ready.catch(() => {
            // 加载失败时静默退出
        });

        window.addEventListener(
            "pagehide",
            () => {
                if (companion && typeof companion.destroy === "function") {
                    companion.destroy();
                }
            },
            { once: true }
        );
    }

    /* 启动时机：DOM 解析完就启动，不再等 window.load。
       以前等 load + 600ms 是为了「不和正文抢带宽」，但 load 要等全部图片、
       花瓣 SVG、文章插图都下载完，长文章上这一等就是好几秒。
       现在资源已经在 <head> 里预加载（见 extend_head.html），带宽竞争在
       解析阶段就发生了，再等 load 只会白白推迟首帧。
       用 requestIdleCallback 让出主线程，先完成首屏渲染；timeout 保证繁忙时
       最多推迟 300ms。脚本放在 body 末尾，执行时宠物要挂载的 body 已经存在。 */
    const schedule = window.requestIdleCallback
        ? (fn) => window.requestIdleCallback(fn, { timeout: 300 })
        : (fn) => setTimeout(fn, 0);

    /* 本脚本和 runtime（peekling.min.js）都是 async，谁先下载完谁先执行，
       顺序不保证。runtime 还没就绪时，等它的 load 事件再启动。 */
    let started = false;
    const start = () => {
        if (started || !window.Peekling) return;
        started = true;
        schedule(boot);
    };
    if (window.Peekling) {
        start();
    } else {
        // runtime 可能在注册监听之前就已执行完（命中预加载缓存时很常见），
        // 所以 load 事件之外再兜底轮询一小段时间，最多 10 秒。
        const runtime = document.getElementById("peekling-runtime");
        if (runtime) runtime.addEventListener("load", start, { once: true });
        let tries = 0;
        const poll = setInterval(() => {
            start();
            if (started || ++tries > 100) clearInterval(poll);
        }, 100);
    }
})();
