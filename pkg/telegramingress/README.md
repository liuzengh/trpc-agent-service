# Telegram 轮询接入

`cmd/telegram-poller` 用于没有公网回调入口的部署。它从官方 Bot API 长轮询读取更新，经过现有 Gateway 的回调认证、用户白名单、限流与 Inbox 持久化进入执行链路；回复由现有 Consumer、Worker 与 Outbox Delivery 完成。

Windows 本地部署先将 `deploy/telegram.local.example.json` 复制到仓库外，填写凭据与允许用户，再运行 `scripts/telegram_local_bootstrap.ps1 -ConfigPath <配置文件路径>`。`ConfigPath` 必须显式传入；`-ValidateOnly` 只检查配置与官方账号状态，`-SkipBuild` 复用已有镜像。

| 环境变量 | 配置 |
| --- | --- |
| `TRPC_SECRET_TELEGRAM_BOT_TOKEN` | Bot token |
| `TRPC_SECRET_TELEGRAM_WEBHOOK` | 与 Gateway channel binding 一致的回调 secret |
| `TELEGRAM_GATEWAY_BASE_URL` | Gateway origin，例如 `http://gateway:8080`，不含路径、query 或用户凭据 |
| `TELEGRAM_WEBHOOK_ROUTE_KEY` | channel binding 的 `webhookKey`，与 Bot token 分开 |
| `TELEGRAM_STATE_PATH` | 持久化状态文件；容器默认 `/state/telegram.json` |
| `TELEGRAM_EXPECTED_BOT_ID` | 可选，预期的 Bot 数字 ID；启动时对照 `getMe` |
| `TELEGRAM_POLL_TIMEOUT` | 默认 `30s`，范围为 1–50 整秒 |

进程继承 `HTTPS_PROXY`、`HTTP_PROXY` 与 `NO_PROXY`。容器内访问 Telegram 的网络需单独验证；内部 Gateway 地址应在 `NO_PROXY` 中。Bot API 主机固定为 `api.telegram.org`，所有 HTTP 重定向均拒绝。

启动先执行 `getMe`、`getWebhookInfo`；发现已有 webhook 时退出，交由运维明确切换，不会自动删除 webhook 或丢弃积压。一个 Bot 使用一个轮询实例与持久卷，不能同时配置其他轮询消费者。文件锁只约束共享同一状态路径的进程，不提供跨独立卷的集群选主。

状态绑定 Bot ID、Gateway origin 与路由标识。每条更新在转发前先将原始字节持久化，Gateway 返回 `200` 后才原子写入下一 offset 并清除待确认正文。若 Gateway 已入队而本地 offset 保存失败，进程停止；重启使用相同字节重放，交由 Inbox 幂等处理。原始正文以 base64 保留字节，不是加密：持久目录权限为 `0700`，文件为 `0600`，应使用受控、加密的本地卷。成功确认后不再保留正文。

Gateway 的 `200` 也可能表示已审计拒绝的用户或不支持的事件，因此不能仅凭轮询确认日志宣称产生了 Agent 执行。正常文本应联查 Inbox、execution 与 Outbox。Gateway 的 400/401/403/404/409 等永久拒绝会停止轮询并保留当前更新；网络故障、429、5xx 退避重试。取消 context 会终止在途请求并释放文件锁。

回归运行：`go test -race ./pkg/telegramingress ./cmd/telegram-poller`。测试使用本地 HTTP 协议服务，不调用真实 Telegram、模型或 IM 账号。
