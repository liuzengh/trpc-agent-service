# 企业登录浏览器验收（Playwright）

登录 E2E 同时保留开发 Mock 登录和企业微信 CorpApp 扫码链路。Mock 只有显式配置时才出现；假企业微信服务用于验证扫码授权、回调、`gettoken` 与 `auth/getuserinfo` 的完整 Session 链路。

覆盖三条场景：

| 场景 | 命令 | 覆盖 |
|---|---|---|
| mock | `node e2e-login.mjs mock` | `LOGIN_PROVIDER=mock` → 测试登录 → 默认系统管理员 |
| wecom | `node e2e-login.mjs wecom` | 企业微信扫码入口 → 回调 → 系统管理员登录 → 控制台可用 → 登出 |
| expiry | `node e2e-login.mjs expiry` | 4 秒 Session 过期 → 受保护请求 401 → 登录页提示「会话已过期」 |

推荐直接执行：

```bash
bash tests/browser/run.sh
```

脚本会构建服务、启动假企业微信和本地模型。Mock 场景使用 `LOGIN_PROVIDER=mock`；企微场景临时注入 `LOGIN_PROVIDERS_JSON`，Provider ID 为 `wecom-e2e`，首个系统管理员通过 `SYSTEM_ADMIN_PROVIDER_ID=wecom-e2e` 与 `SYSTEM_ADMIN_SUBJECT_ID=zhangsan` 引导。不会修改仓库中的生产登录配置。

手动运行时先启动 `fake-wecom.mjs`，再给服务配置一个指向 `http://127.0.0.1:10086` 的 WeCom Provider；授权入口模拟 `/wwlogin/sso/login`，API 模拟 `/cgi-bin/gettoken` 与 `/cgi-bin/auth/getuserinfo`。

其他独立 UI fixture：

```bash
E2E_BASE=http://127.0.0.1:5173 node tests/browser/e2e-ui-shell.mjs
E2E_BASE=http://127.0.0.1:5173 node tests/browser/e2e-applications.mjs
```

可通过 `CHROMIUM_PATH` 指定已有 Chromium。截图和失败日志输出到 `tests/browser/artifacts/`。
