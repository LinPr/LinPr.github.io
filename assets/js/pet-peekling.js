/* 桌宠：Peekling（https://peekling.com/，Apache-2.0）
   自托管运行时与角色包，角色是 Pip（暖金色柯基），行为跟随鼠标。

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

    /* 关于「大小一致」：
       运行时的 sprite 尺寸是固定的（永远是一个 64px 格子），
       但角色包里不同动作的美术占格比例不一样 —— scroll / scroll:fall /
       scroll:fly 这几个动作画得更小（高度只有 39-43，idle 是 51），
       所以一旦进入滚动动作，视觉上狗会"缩一下"。
       为了始终保持同一体型，这里不挂滚动反应，滚动时它继续走路或待机。
       另外 sleep 也是蜷缩的小体型，同样不使用。

       跟随鼠标的关键：运行时的 follow-pointer 只在「有 pointer.move 订阅」时
       才会拿到指针坐标（world.pointer），光在 baseline 里写 follow-pointer
       是不够的 —— 那样宠物只会原地待机。所以下面有一条 pointer.move 规则。
       arrivalRadius 是"停在离指针多远的地方"，太小会贴着光标抖，
       56px 差不多是一只柯基的半个身子距离，看起来自然。 */
    const FOLLOW = {
        motion: { type: "follow-pointer", speed: 132, arrivalRadius: 64 },
        state: { capability: "locomotion" },
    };

    const PLAN = {
        baseline: {
            channels: ["motion", "state"],
            ...FOLLOW,
        },
        rules: [
            {
                // 这条规则既负责订阅 pointer.move，也负责持续把宠物朝指针挪。
                // 注意：pointer.move 是 continuous effect，不能带 surfaces / until。
                id: "follow-pointer",
                when: { source: "browser", event: "pointer.move" },
                effect: {
                    channels: ["motion", "state"],
                    ...FOLLOW,
                },
            },
            {
                id: "click-hello",
                when: { source: "browser", event: "pointer.click" },
                effect: {
                    channels: ["state"],
                    state: { state: "click" },
                    until: { type: "duration", ms: 700 },
                },
            },
            {
                // 点击宠物时冒个泡：内容由 content.hello 提供。
                // 注意 channel 名是 "surface:<id>"，不是 "surfaces"。
                id: "say-hello",
                when: { source: "browser", event: "pointer.click" },
                effect: {
                    channels: ["state", "surface:hello"],
                    state: { state: "happy" },
                    surfaces: [{ id: "hello", contentId: "hello" }],
                    // surface 自带开关语义，这里不能再配 until
                    // （配了会 invalid-plan），气泡由再次点击收起。
                },
            },
        ],
    };

    // 点开气泡时说的话。引擎按 content id 取，这里只放一条常驻的。
    const CONTENT = {
        hello: {
            bottom: {
                title: "Pip",
                text: "我会跟着你的鼠标跑。也可以拎起我、丢出去。",
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
                    drag: true,
                    throw: true,
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

    // 等首屏渲染完成后再启动，避免和正文抢带宽
    if (document.readyState === "complete") {
        setTimeout(boot, 600);
    } else {
        window.addEventListener("load", () => setTimeout(boot, 600), { once: true });
    }
})();
