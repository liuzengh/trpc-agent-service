# IM Channel Adapter 设计与实现边界

本文对应 README 的“IM 软件接入”验收项，描述当前企业微信 API 模式智能机器人和 Telegram Bot 两条真实链路。公共 HTTP Chat、浏览器 IM Simulator 和 Mock Channel 不计入两种真实 Provider。

## 1. 统一适配边界

Provider Runtime 负责连接、协议解析和 Provider API 调用；`ChannelAdapter` 负责统一的 `Receive`/`Send` 端口；Gateway 负责 Tenant/App 路由、治理、Session 事件和 Runner 调度。外部文本最终转换为：

```text
Provider payload
  -> ChannelMessage{message_id, user_id, conversation_id, text}
  -> RunnerRequest{tenant_id, app_id, session_id, user_id, input, request_id}
  -> model.NewUserMessage(input)
  -> runner.Run(ctx, user_id, request-scoped-session, model.Message, ...)
```

上游 Agent Event 的 partial response 转为平台 `message.delta`，completed response 转为 `message.completed`，错误和取消分别转为 `run.failed`/`run.cancelled`。Gateway 在 Artifact、Memory 和 Session 投影成功后聚合最终文本，再构造 `ChannelReply`。当前真实 IM Provider 只投递最终回复：Telegram 使用 `sendMessage`，企业微信使用 `aibot_respond_msg` 的 Markdown 消息；Chat SSE 才逐条暴露 `message.delta`。

危险 Tool 进入 `pending_confirmation` 时只持久化确认状态，操作员在 Management Console 处理确认。当前实现不会占用企业微信的一次性回复引用去发送中间状态，也不会向 Telegram 主动发送处理中消息。确认、失败和取消的 IM 状态通知是后续增强，文档和代码均不得宣称已经发送。

## 2. Provider 差异与认证

| 项目 | 企业微信 API 模式智能机器人 | Telegram Bot |
| --- | --- | --- |
| 入站连接 | `wss://openws.work.weixin.qq.com` WebSocket | `getUpdates` long polling |
| 凭据 | BotID + long-connection Secret | Bot username + Bot token |
| 入站认证 | `aibot_subscribe` 成功响应、帧内 BotID 校验、连接会话绑定 | Bot token 只用于服务端 Bot API 请求；Update 来自已认证 polling 响应 |
| 回复 | `aibot_respond_msg`，复用 callback `req_id` 并校验响应 | `sendMessage(chat_id, text)` |
| 群聊主体 | `chatid` | `message.chat.id` |
| 单聊主体 | `chatid`；缺失时使用 `userid` | `message.chat.id`；路由可回退到 sender user ID |
| 顺序/幂等标识 | `msgid` 去重；协议无可用序号时不做乱序判断 | Update ID 判断乱序，message ID 去重 |

这两种选型都不使用公开 webhook，因此没有 webhook URL 或 HTTP callback 签名需要配置。企业微信传统自建应用的 CorpID、AgentID、EncodingAESKey、HTTP token/secret 不适用于本实现；Telegram webhook 的 `secret_token` 也不适用于 long polling。凭据来自进程环境或生产 Secret Manager，不写入 Channel Binding、Control Plane API 响应、Session Event、日志或 Trace。若未来增加 HTTP webhook Provider，必须在读取 Tenant 路由前完成原始请求体验签、防重放时间窗校验和 body 大小限制。

## 3. Provider Account、Tenant 与用户映射

真实 Provider 路由的唯一键是：

```text
(provider, provider_account, external_subject)
  -> (tenant_id, agent_app_id, conversation_type, enabled)
```

同一 Provider Account 的同一外部主体不能绑定到两个 Tenant/App；不同 Provider Account 可以拥有相同的外部主体 ID。空 `provider_account` 是单账号部署的兼容路由，运行时选中后立即替换为已认证连接的真实账号，用于 Session、去重、治理和投递标识。管理接口不接受消息载荷中的 `tenant_id` 建立路由。

当前参考进程每种 Provider 启动一套账号连接；要运行多个同类 Bot，可为每个账号部署独立 Provider connector，并共享 Control Plane/Gateway。路由数据模型和管理 API 已按账号区分。

外部用户 ID 以 Provider 原始 ID 进入 `RunnerRequest.UserID` 和 Audit Event，同时始终携带 channel/provider account。群聊路由先授权外部会话，再由 Governance Policy 的 `allowed_im_users` 选择是否限制群成员。需要把多个外部身份合并成企业统一身份时，应增加显式 `(provider, account, external_user_id) -> platform_user_id` 目录；当前版本不自动合并跨 Provider 身份。

## 4. Session ID 与隔离规则

真实 IM Session 使用以下确定性规则：

```text
session_id = "im-" + hex(SHA-256(provider + NUL + provider_account + NUL + external_subject)[0:12])
```

- 群聊的 `external_subject` 是群/频道 ID。同一用户进入不同群会得到不同 Session；同一群内不同用户共享群 Session，但事件保留各自 `user_id`。
- 单聊使用会话 ID；Provider 没有稳定单聊 ID 时才回退到外部用户 ID。同一账号内同一用户保持稳定 Session。
- 不同 Provider 或不同 Provider Account 一定进入不同 Session ID 空间。
- Tenant 不进入可显示的 Session 哈希，因为路由已保证一个账号主体只归属一个 Tenant；所有实际查询、事件、Memory、Artifact、Lease 和 Run Coordinator 键仍使用 `(tenant_id, session_id)`。即使两个 Tenant 出现相同 Session 字符串，也不能互相读取或覆盖。
- 路由变更到另一个 Tenant 不迁移历史。新 Tenant 只能在自己的 Tenant 组合键下创建状态；旧 Tenant 历史仍受原 Tenant 授权保护。

## 5. 平台限制与降级矩阵

| 限制 | 当前行为 | 后续扩展边界 |
| --- | --- | --- |
| 消息长度 | Telegram 最终文本上限 4096 字符，企业微信适配器上限 2048 字符；超限记为 `channel_message_too_long`，不写虚假投递成功 | 按 rune 安全切分、多条消息关联同一 request_id，或先生成摘要 |
| 频率限制 | Telegram 429 尊重 `retry_after` 并有界重试；企业微信及网络错误最多重试三次；Tenant Governance 在执行前限流 | Provider Account 级 token bucket、发送队列和全局并发上限 |
| 异步回复 | Agent 在后台执行，入口先记录 accepted；只有最终成功结果投递 IM | 超过回复窗口时改用主动消息 API；发送“处理中”状态并关联后续更新 |
| 图片/文件 | 非文本真实消息记录 `unsupported_media`，不调用 Runner；Mock 可验证附件大小拒绝 | 下载到 Tenant-scoped Object Store，病毒扫描/OCR 后转换为多模态 `model.Message` 或 Artifact 引用 |
| 卡片/流式消息 | 企业微信最终结果使用 Markdown；Telegram 使用文本；真实 IM 不消费 `message.delta` | Provider capability negotiation，编辑原消息或更新卡片；严格输出治理时继续完整缓冲 |
| 撤回 | 当前不处理撤回事件，也不删除已提交 Session 事实 | 追加 `message.recalled` 事件；按 Tenant Policy 隐藏投影并保留审计，不能篡改追加事实 |
| 失败重试 | 同一 request/message ID 有界重试；Telegram 429/5xx、企业微信超时/断连记录 attempts 和终态；重复入站复用既有执行结果 | 将 delivery outbox 持久化，以便进程重启和跨 Gateway 接管 |
| 去重/乱序 | 运行时缓存 message ID/sequence，Session Event 和 request_id 提供执行幂等 | 将 Provider offset、短期去重窗口和 delivery outbox 放入共享 Redis/PostgreSQL |

长度、富媒体、主动状态通知和撤回的“后续扩展”不属于当前文本消息比赛主链；验收时必须按当前行为测试和展示，不能标记为已实现。

## 6. 测试与验收证据

自动化 fixture 覆盖 Telegram polling/回复/429 重试、企业微信 WebSocket 认证/心跳/回复确认/重连、账号路由、重复与乱序、非文本拒绝、Session 确定性和服务关闭。真实凭据 smoke 属于显式本地操作，常规 CI 不消费真实消息或额度。2026-09-09 已分别完成 Telegram 与企业微信真人客户端的真实消息、真实模型和 Provider 回复回环，非敏感结果见 [`acceptance/live-im-smoke-2026-09-09.md`](acceptance/live-im-smoke-2026-09-09.md)。
