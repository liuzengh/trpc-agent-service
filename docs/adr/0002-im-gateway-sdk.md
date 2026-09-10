# ADR-0002：IM 通道接入策略（单一 Connector 体系）

- 状态：**已采纳（2026-09-09）**
- 关联：`CONTEXT.md`、`docs/complete-development-plan.md` 第 7 节

## 背景

平台需要同时接入 Telegram、企业微信和飞书，并保持 Worker 完全不知道供应商协议。早期并存 Webhook、轮询和两套企业微信实现，导致凭据、入站、回复与运行状态出现重复路径。

## 决策

1. 每个平台只保留一种正式接入方式：Telegram `getUpdates` 长轮询；企业微信智能机器人 WebSocket；飞书官方 SDK WebSocket。
2. `channels.Receiver` / `channels.Sender` 是防腐层。供应商 SDK/协议类型只能存在于具体 channel 包，进入平台后统一变成 `InboundMessage` / `OutboundMessage`。
3. Gateway 的 `channelConnectorManager` 通过 Redis lease 选举唯一 Connector Leader，负责三种外部 IM 的常驻连接；Kafka Consumer Group 只负责 Worker 调度，不引入第二套任务分配器。
4. Connector 入站先解析租户配置与 canonical Session，再写 Kafka。发布失败不确认供应商消息：Telegram 不推进 offset；长连接消息持续退避重试，确保短暂 PostgreSQL/Kafka 故障不把消息静默丢弃。
5. 出站只有一套持久化 Outbox Dispatcher。Dispatcher 在 Connector Leader 上领取外部渠道回复，按平台长度限制分片，并用 Redis 在 `(channel,binding,conversation)` 维度协调 pacing；等待期间持续续租 Outbox lease。
6. 图片/文件统一转为临时 Artifact。Connector 只负责供应商媒体下载/解密；公共 staging 模块保存文件后，Kafka 只携带 Artifact 引用。执行成功后回收输入 Artifact，失败/重试时保留。
7. 企业微信对外只表示“智能机器人”；企业微信 OAuth 登录仍是独立身份域，使用自建应用参数，但不得与机器人通道实现混为一体。
8. Binding 只保存公开 `binding_id` 与 allowlist `credential_ref`。每个 Binding 固定属于一个 Agent；一个 Agent 可以绑定多个同类型外部机器人账号。若未来需要“同一个外部机器人按规则路由多个 Agent”，单独引入 Router，不把 Binding 本身做成多目标路由器。
9. Web Chat 是平台原生入口，不建立虚假的 `web` Binding；只有外部 IM 接入才进入 Binding/Connector 生命周期。
10. 新建 Binding 默认使用 `member_only`。`allowlist` 和 `public` 必须由 Tenant Admin 显式选择，默认不向未知外部用户开放。
11. Channel 凭据由 Tenant Admin 配置并写入平台允许的 Secret Provider；Console 不回显明文，只允许替换。System Admin 负责 Secret Provider 基础设施，但不因为全局身份自动获得各 Tenant 的业务凭据读取权。

## 后果

- Gateway 是唯一了解 IM 供应商协议和连接生命周期的节点；Worker 只处理 Kafka 标准消息。
- 不再需要 `/webhooks/{channel}/{binding_id}`、`channels.Adapter`、URL verification 或企业微信自建应用消息通道。
- 多 Gateway 下只有一个外部 Connector owner，但入站与执行仍通过 Kafka/共享存储恢复，不依赖进程粘性。
- 真实供应商凭据的最终 E2E 仍属于部署验收，离线契约测试不能替代真实账号授权。

## 备选方案（已否决）

| 方案 | 否决原因 |
|---|---|
| 同一平台同时保留 Webhook 与长连接/轮询 | 两套入站、凭据和错误恢复语义，运维与测试成本成倍增加 |
| 企业微信自建应用作为机器人通道 | 与 AI 对话机器人产品模型不一致；群聊/单聊接口分裂，且需要公网回调 |
| 自研飞书事件协议层 | 官方 Channel SDK 已提供 WebSocket、事件标准化、mention 与资源下载，重复实现价值低 |
| 个人微信接入（wechaty 等） | 自动化违反微信条款，封号风险，不具企业可用性 |
