# Phase 5 Telegram、企业微信智能机器人与 Web UI

> 实施分支：`feature/phase5-telegram-wecom-webui`
> 任务协议：`TaskSchemaVersion=2`，不兼容 v1

## 交付范围

Phase 5 在 Phase 3/4 主链路上增加 Telegram 长轮询、企业微信智能机器人 WebSocket 长连接和 localhost Web UI。三种入口都经过 Gateway 可信 Binding、Inbox、任务 Stream、Worker/Runner、Reply Stream；IM 发送成功或达到持久重试终态后才确认 Reply Stream。

```text
Telegram / WeCom / Web UI
  -> trusted ChannelBinding
  -> Inbox + agent.tasks
  -> Worker + strong Session fencing
  -> agent.replies
  -> per-binding serial delivery
  -> IM send -> outbound Redis state -> XACK/XDEL
```

首版只支持一次性文本。Telegram 不启用 Webhook；企业微信不发送图片、附件、卡片或流式回复；群聊不引用原消息。

## v2 身份与可信投递

任务 v2 固化 channel、binding、外部账号、平台消息 ID、actor、runner user、conversation、conversation type 和回复关联字段，摘要覆盖这些身份及投递字段。v1 和缺少新身份字段的伪 v2 任务都会被拒绝。

- 单聊：个人 `runner_user_id` + 个人 Session。
- 群聊：群主体 `runner_user_id` + 群 Session；真实发言人只保存在 `actor_user_id`。
- Executor 只贡献回复文本。Store 从已验证的 ExecutionTask 重建 `DeliveryTarget`，忽略 Executor 提交的渠道、绑定或会话字段。
- 可验证的 Agent 失败携带可信目标，并向 IM 发送固定文本“抱歉，处理失败，请稍后重试。”；内部错误码仅在 Web 查询和持久终态中可见。
- v1、损坏载荷和无法验证的隔离任务没有投递目标，不调用外部 Adapter。

## ChannelBinding 与凭据

支持的通道为 `demo`、`telegram`、`wecom_aibot`、`feishu`：

- Telegram 只允许 `credential_ref`，并通过 `getMe` 核对绑定的 `external_account_id`。
- 企业微信必须提供 `bot_id_ref` 与 `bot_secret_ref`。
- 飞书必须提供 `bot_id_ref` 与 `bot_secret_ref`，分别引用 App ID 和 App Secret。
- demo 不允许任何 IM 凭据。

生产 JSON 只接受 `env:<ENV_NAME>`。引用缺失或格式错误是配置错误；引用值暂时缺失时使用不可用 Adapter，进程保持存活且整体 `/readyz=503`。认证或网络中断同样 not ready，并由 Adapter 重试。启动时用凭据摘要比较已解析 Bot 身份，禁止两个启用绑定复用同一 Bot，摘要和原值都不记录日志。

## Telegram

生产依赖为 `github.com/go-telegram/bot v1.25.0`（MIT，module Go 1.18）。项目使用 SDK 类型、`getMe` 和 `sendMessage`，但自行调用 `getUpdates` 控制 offset：可靠入队或明确终态忽略后才推进；暂时入队失败不推进。群聊只接收 Telegram entity 明确标注的 `@bot`，并按 UTF-16 offset/length 删除 mention。429 尊重 `retry_after`，5xx、超时和认证失败有界退避；构造函数不联网。

## 企业微信

协议与依赖选择见 `docs/stage5-wecom-spike.md`。每个启用 binding 建立一条连接，认证、心跳、断开、踢下线、单聊/群聊和 `aibot_respond_msg(msgtype=stream, finish=true)` 均由本地 Fake WebSocket 固定向量覆盖。Adapter 等待企业微信使用相同 `req_id` 返回的顶层 `errcode=0` 后才确认出站；无需另行发送人为 ACK。

## 飞书

飞书使用官方 `github.com/larksuite/oapi-sdk-go/v3` SDK 建立长连接，不需要公网 Webhook。首版只处理 `im.message.receive_v1` 文本事件，忽略非用户发送者；出站调用回复原消息接口 `/open-apis/im/v1/messages/:message_id/reply`。只有 SDK 请求无错误且飞书业务响应 `code=0` 时才确认发送成功，非零业务码会进入现有可靠出站重试。

开放平台必须启用机器人能力，事件接收方式选择“使用长连接接收事件”，订阅 `im.message.receive_v1`，并开通 `im:message:send_as_bot` 权限。单聊接收需要 `im:message.p2p_msg:readonly`，群聊 @ 机器人接收需要 `im:message.group_at_msg:readonly`。应用版本还需要发布生效，测试用户必须在应用可用范围内。建议先验收单聊；最终成功标准是飞书客户端实际显示模型回复。

## 可靠出站

状态键：

```text
<prefix>:reliable-v1:outbound:<task_id>
```

字段为 `status/attempts/last_error/next_attempt_at/terminal/acked_at/updated_at`。每次实际 Send 才增加 attempts。失败按 1s 起步、30s 封顶重试，默认最多 5 次；成功或超限终态通过 Lua 原子更新状态并 `XACK/XDEL`。Gateway 重启后 `XAUTOCLAIM` Pending，并沿用 Redis 中 attempts。外部发送成功而 Redis 确认前崩溃仍可能重复发送，因此对外语义是至少一次，不是 exactly-once。

## Web API

`serve` 和 `gateway` 默认监听 `127.0.0.1:8080`：

```text
POST /api/v1/web/messages
GET  /api/v1/web/messages/{message_id}?binding_id=<demo-binding>
```

POST 强制使用 `demo` channel，Router 从预置 binding 派生租户和 Agent。请求不接受 tenant、Agent 或配置版本。新任务返回 202，已有终态的相同消息返回 200。GET 由 tenant+binding+message ID 计算 Inbox，不建立 request ID 索引；返回 `submitted/processing/succeeded/failed`，失败包含完整 `error_code`。嵌入式页面每 1500ms 轮询，不保存凭据。

## 验收

自动化覆盖 v1 拒绝、v2 摘要、可信目标、群主体 Session、凭据契约、重复 Bot、Telegram Fake Bot API、企业微信 Fake WebSocket、Agent 失败统一文本、出站退避/超限/重启恢复和 Web 四态查询。真实 Redis 7 复验 Phase 3 Streams/Gateway 恢复、Phase 4 Strong/双 Worker，以及 Phase 5 outbound attempts 跨 Gateway 恢复。

真实企微 smoke 只从环境变量读取 Bot ID/Secret，不打印值。真实群聊 smoke 是可选项。完整审计、流式/媒体消息、多 Gateway 同 Bot 选主和 IM exactly-once 不属于 Phase 5。

### 真实企微单聊 Smoke 验收记录（2026-09-03）

- 使用真实企业微信 Bot 完成单聊端到端验证；gateway `/readyz=200`，worker `/healthz=200`。
- 真实订阅响应采用顶层 `errcode=0`、`errmsg=ok`、`headers.req_id` 格式；Adapter 已兼容该官方响应，同时保留 Fake Server 契约格式，渠道测试通过。
- 消息链路 `aibot_msg_callback` -> Inbox -> Worker/Agent -> `aibot_respond_msg(finish=true)` 已打通。对应任务 `succeeded`，出站仅 1 次尝试即成功，Redis 出站状态已写入 `acked_at`，任务/回复 Stream 消费组无 Pending。
- 本次 smoke 的临时 gateway/worker 已停止，临时配置已删除；未记录任何 Bot ID、Secret、模型密钥或消息正文。
