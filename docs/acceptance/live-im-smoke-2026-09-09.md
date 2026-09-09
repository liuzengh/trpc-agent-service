# 真实 IM 消息 Smoke 记录（2026-09-09）

本记录补充自动化协议 fixture，证明 Telegram Bot 与企业微信 API 模式智能机器人均使用真实 Provider 凭据、真实客户端消息和真实模型完成过文本消息回环。它不是 CI 项；复验仍需要本地 `.env.local`、外部客户端和可用模型额度。

- 验收提交：`50b9903b6de2a574a7268c11ed559f265757aa24`
- 验收时间：2026-09-09 12:03–12:11 CST
- Tenant / Agent App：`tenant-dev` / `im-live-acceptance`
- 隐私边界：不记录 Bot Secret、Bot Token、模型 Key、Provider Account ID、外部用户 ID、会话 ID或完整 Provider 消息 ID。

## Telegram

- `getMe` 对真实 Bot API 凭据返回成功，Provider long polling 状态进入 `connected`。
- 真人客户端发送 `im-live-telegram-roundtrip-20260909`，平台按真实 `chat_id` 路由到 `tenant-dev / im-live-acceptance`。
- 真实模型返回 `已收到：im-live-telegram-roundtrip-20260909 [im-live-ok]`。
- Telegram `sendMessage` 成功，Provider delivery 为 `delivered`，`attempts=1`。
- Tenant Session 持久化 `message.input`、`run.started`、`message.completed`、`channel.reply(status=delivered)`、`run.completed`；审计同时存在 `policy.allowed` 和 `run.completed`。

## 企业微信

- 真实 WebSocket 完成 `aibot_subscribe`，Provider 状态进入 `connected`。
- 真人客户端先发送发现消息取得单聊外部 subject；建立 Tenant 路由后发送 `im-live-wecom-roundtrip-20260909`。
- 真实模型返回 `已收到：im-live-wecom-roundtrip-20260909 [im-live-ok]`。
- `aibot_respond_msg` 获得成功关联响应，Provider delivery 为 `delivered`，`attempts=1`。
- Tenant Session 持久化 `message.input`、`run.started`、`message.completed`、`channel.reply(status=delivered)`、`run.completed`；审计同时存在 `policy.allowed` 和 `run.completed`。

## 结论

两种 README 要求的 Real IM Provider 均完成真实客户端入站、Tenant/App 路由、真实模型执行、Provider 回复成功响应和 Session/Audit 记录核验。`delivered` 表示 Provider API 已确认回复成功，不代表客户端已读；本次未单独采集客户端展示截图或已读回执。

本次实测覆盖两种 Provider 的单聊文本成功路径；群聊、跨租户隔离、重复、乱序、媒体拒绝、失败重试、重连和关闭未进行真实平台故障注入，继续由自动化 fixture 验证。`credential_smoke_status` 当前仍固定显示 `not_run`，不会自动保存人工验收结果，本次结果以此记录为准。
