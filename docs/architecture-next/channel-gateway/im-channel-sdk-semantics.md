# 首批外部 IM：Telegram 与企业微信智能机器人的行为、SDK 与接入契约

> 设计状态：草案（渠道接入契约建议）；调研与实验结论按证据标签分别记录。所属分支：`codex/channel-gateway`。
> 上游约束：[架构级约束](../constraints.md)、[Deployment V1](../control-api/deployment.md)。本文属于重构的 Channel Gateway 文档，不作为旧版实现规范。
> 核验日期：2026-09-05。首次支持对象：Telegram 标准 Bot API 机器人、企业微信智能机器人 API 长连接模式。
> 本文包含官方文档、固定版本 SDK 源码、本地实验及真实 Telegram 私聊验证。新 Bot 的 11 项 API/交互检查通过；企业微信实测与生产 Gateway 端到端验收仍待执行。
> 历史报告副本的本机路径为 `/Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-sdk-research-20260904/history/pre-relocation/BASELINE.md`。`artifacts/` 被 Git 忽略，这些路径只构成本地审计索引，不是随仓库提交的附件；历史执行记录保留当时路径，不改写为新位置执行。
> 当前实现方向已改为公开纯 Go 包 `platform/im/wecom`，编译进 Gateway；Node SDK 下述观察仅作为源码/协议对照，非部署依赖。详见 [公开 Go Connector](public-go-connector.md)。
> 标签：**[文档]** 官方服务端协议；**[源码]** 指定版本的 SDK 实现；**[实验]** 本轮实际运行；**[建议]** 平台设计推论；**[待实测]** 需要真实账号、客户端或服务端验证。

## 1. 结论与本次更新

**先统一消息接纳和回复意图，再保留渠道自身的机器人行为；不要把两个 SDK 包成同一套长连接、ACK 或流式模型。**

| 维度 | Telegram | 企业微信智能机器人长连接 |
| --- | --- | --- |
| 首选接入方式 [建议] | 平台自有持久 Webhook；SDK 用作类型与出站 Client | 公开 Go 协议库承接 WebSocket，Gateway Adapter 在进程内交接 |
| 群内可见性 [文档] | Privacy Mode、管理员权限等共同决定 | 明确支持群内 @ 触发；不是群全量监听 |
| 横向扩展约束 [文档/建议] | Webhook 可并发分发；按消息持久去重 | 一个 Bot 仅一个有效连接；按 Bot 分片、主备切换 |
| 进度与最终回复 [文档] | 私聊临时 draft、编辑消息、独立 final 各有语义 | 同 req_id + stream.id 主动刷新，finish 结束；受时间与频率约束 |
| SDK 不替平台完成的事 [源码] | 入站持久 ACK、跨实例去重、Session 串行 | 连接 owner 协调、持久 Inbox/Outbox、会话级限流 |
| 最容易误判的地方 | 新版允许有条件 bot-to-bot；旧 FAQ 仍有旧说法 | SDK 暴露的方法不一定是当前长连接服务端支持的能力 |

来源：[Telegram 接收更新](https://core.telegram.org/bots/api#getting-updates)、[Telegram Privacy Mode](https://core.telegram.org/bots/features#privacy-mode)、[Telegram bot-to-bot](https://core.telegram.org/bots/features#bot-to-bot-communication)、[企业微信长连接协议](https://developer.work.weixin.qq.com/document/path/101463)。

相较 2026-08-22 报告，主要变化是：Telegram 文档已到 Bot API 10.3；企业微信官方长连接正文此次已成功取得，补齐群触发、单连接、回复时窗、限流及媒体约束；Go SDK 的默认 Webhook 行为得到单独核验。旧报告中“企业微信当前官方页面证据缺口”不再作为上述字段的结论。

## 2. 版本、证据与冲突处理

| 对象 | 本轮基线 | 使用方式 |
| --- | --- | --- |
| Telegram Bot API | 10.3，2026-08-24 | 服务端能力以当前方法文档、Features 与更新日志交叉核验 |
| `go-telegram/bot` | v1.25.0；`3d38d39d8ee18d926b64cb808c78f0927ea6d045` | Go SDK 初选；固定提交检查入站、派发和类型覆盖 |
| `go-telegram-bot-api/telegram-bot-api` | 最新 release v5.5.1，2021-12-13 | 不作为此次完整新能力接入的默认版本；release 年龄不等于整个仓库无人维护 |
| 企业微信长连接官方页 | 文档 101463；页面标注最后更新 2026/05/25 | 群触发、连接、时窗、限流、媒体以此为主要依据 |
| `@wecom/aibot-node-sdk` | npm latest 1.0.7；registry gitHead `ea48edf7c99be0609fe9740050f4942f897a5d95` | 发布包与仓库 main 分开记录，实施锁 package-lock/integrity |
| WeCom 官方仓库 main | `80615b987ef69c6028ad764924609247c0725955`；package.json 仍为 1.0.6 | 此提交只用于指明源码事实，不称其与 npm 1.0.7 字节一致 |

来源：[Bot API](https://core.telegram.org/bots/api)、[Go SDK 固定提交](https://github.com/go-telegram/bot/tree/3d38d39d8ee18d926b64cb808c78f0927ea6d045)、[另一 Go SDK releases](https://github.com/go-telegram-bot-api/telegram-bot-api/releases)、[企微 npm 元数据](https://registry.npmjs.org/@wecom/aibot-node-sdk/1.0.7)、[企微仓库版本字段](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/package.json)。

本轮还核实官方组织有 [Python SDK](https://github.com/WecomTeam/wecom-aibot-python-sdk)。本次选择 Node 作为详细源码核验对象，不声称官方仅有 Node；本次官方组织仓库清单未发现智能机器人 Go SDK。

已下载并校验 npm 1.0.7 的 SHA-512 与 registry integrity 一致；sourcemap 的 13 个源文件中 12 个与上述 main 一致，ws.ts 有解析前控制字符清理差异。registry gitHead 的官方 GitHub 查询返回 422，因此保留发布包与公开提交两条证据链。本机版本核验记录：`/Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-sdk-research-20260904/wecom-evidence.json`（local-only）。

处理证据冲突的规则：

1. 区分 Webhook 与 WebSocket，先确认条款属于哪种模式。
2. 服务端明确限制优先于 SDK convenience method 的存在。
3. API 更新日志与新的专题文档优先于仍保留旧描述的 FAQ，但将冲突记入验收。
4. SDK 默认值只说明该实现的配置，不提升为服务端 SLA。
5. 源码和模拟实验不替代真实机器人、真实客户端与账号权限验收。

## 3. Telegram：机器人实际会看到什么

### 3.1 创建、私聊、群组与权限

[文档] 通过 BotFather 创建 Bot 并获得 API 凭据。标准机器人与普通用户的私聊通常由用户先启动；用户也可以把机器人加入群。应把“已知 user_id”与“当前具备发送权限”分开判断。[创建与基本限制](https://core.telegram.org/bots)

[文档] 群 Privacy Mode 默认开启。定向命令、相关回复等可被投递；管理员或关闭 Privacy Mode 会扩大消息可见范围。普通群文本、普通 @、回复、定向命令不可笼统视作完全相同的触发条件。上述可见性设置影响哪些更新会被交付；是否启动 Agent 由 Gateway 决定，发送和管理权限另行检查。[Privacy Mode](https://core.telegram.org/bots/features#privacy-mode)、[消息可见性 FAQ](https://core.telegram.org/bots/faq#what-messages-will-my-bot-get)

[建议] 首次支持私聊与群/超级群文本。群默认触发定义为“发给本 Bot 的显式命令或回复本 Bot”；普通 @ 另做实测。若产品要求读取全部群消息，应显式检查 Privacy Mode/权限，再在 Gateway 执行触发规则，而不是假设 Provider 已做过滤。

[文档] Forum topic、私聊 topic、频道与频道私信有不同的目标字段和权限条件。[Topic 能力](https://core.telegram.org/bots/features#topics-in-private-chats)、[Message 字段](https://core.telegram.org/bots/api#message)

[建议] V1 不主动支持频道运营、Business、Guest、Inline/Mini App 或 bot-to-bot 自动对话。收到非本次支持的 Update 类型时做明确忽略/审计，不把服务消息、编辑、按钮点击一律转成新的用户文本 Run。

### 3.2 不能继续假设“机器人之间永远看不到消息”

[文档] 当前 API 10.0 更新记录及 Features 已说明有条件 bot-to-bot：群中的定向命令/直接回复与普通消息有不同设置条件；双方开启相关模式后可按用户名进行机器人私聊。旧 FAQ 仍保留相反描述，不能拿它作为防循环保证。[更新日志](https://core.telegram.org/bots/api#may-8-2026)、[当前互聊条件](https://core.telegram.org/bots/features#bot-to-bot-communication)、[旧 FAQ 描述](https://core.telegram.org/bots/faq#why-doesn-39t-my-bot-see-messages-from-other-bots)

[建议] 先解析 `sender_chat`，将 sender 建模为 user / chat / unattributed；匿名管理员或代表频道发言的兼容 `from` 不冒充真实个人身份。随后默认排除真实 bot sender、自身回声与未批准的自动消息；不要仅按兼容 `from.is_bot` 误丢正常的 chat sender。以后若开启互聊，单独增加允许列表、循环检测和预算。该过滤由代码执行，不依赖模型判断“这看起来像另一只机器人”。

### 3.3 接收、去重、顺序与 ACK

[文档] `getUpdates` 与 Webhook 互斥；更新保留最多 24 小时。Polling 的 offset 会确认之前的更新。Webhook 非 2xx 会重试，但精确次数/退避未公开；并发连接可能导致到达乱序。`update_id` 用于更新去重，长时间无更新后不能假设永远连续增长。[Getting updates](https://core.telegram.org/bots/api#getting-updates)、[getUpdates](https://core.telegram.org/bots/api#getupdates)、[setWebhook](https://core.telegram.org/bots/api#setwebhook)、[Update](https://core.telegram.org/bots/api#update)

[建议] 生产入口自己持有 HTTP 生命周期：验证 secret header → 持久化 Admission/Outbox → 返回 2xx。外部 Inbox identity 为 provider + stable account_id + update_id。V1 一次外部事件只固定一个首次 Decision/Receipt 与至多一个 RouteSnapshot，不隐式扇出到多个 Binding；重投返回原记录。不要把 message_id 当整个 Bot 的全局唯一键，也不要把 DeploymentRevision 放进去。故障恢复避免 `drop_pending_updates=true` 或用负 offset 丢弃待处理输入。

[文档] Webhook HTTP 响应可携带 Bot API 方法，但该方式缺少可用于可靠投递账本的返回结果。[Webhook 中调用 API](https://core.telegram.org/bots/api#making-requests-when-getting-updates)

[建议] final 单独调用发送 API 并记录 provider message ID；接纳 ACK、按钮反馈和最终消息分开。

### 3.4 回复、按钮、流式与限流

[文档] 文本发送、编辑现有消息和私聊 draft 是不同操作；draft 是临时预览，最终需要独立持久消息。API 10.3 可启用 Stop 交互，产生 `stopped_message_generation` 更新。[Streaming replies](https://core.telegram.org/bots/features#streaming-replies)、[sendMessageDraft](https://core.telegram.org/bots/api#sendmessagedraft)、[MessageGenerationStopped](https://core.telegram.org/bots/api#messagegenerationstopped)

[建议] 第一版 Final 必选、Progress 可选；私聊 draft 和群 edit 用不同 capability。Stop 只发起经过授权的取消命令，只有执行所有者确认后才报告已取消，不能把 UI 停止预览当工具副作用已撤销。

[文档] Inline callback 按钮产生 `callback_query`，需独立 `answerCallbackQuery` 结束客户端等待；它不是 Webhook transport ACK，也不是最终业务结果。[CallbackQuery](https://core.telegram.org/bots/api#callbackquery)、[answerCallbackQuery](https://core.telegram.org/bots/api#answercallbackquery)

[建议] `/start`、`/help` 和按钮即时反馈走轻量确定性路径。按钮操作需要校验 actor、会话、绑定、目标、过期时间和重复操作；菜单展示范围不等于服务端授权。

[文档] 常规发送额度大致为单 chat 1/s、群 20/min、普通广播约 30/s；429 可带 `retry_after`。这些是对应文档的常规额度指引，不是所有方法共享的唯一公式。[发送 FAQ](https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this)、[ResponseParameters](https://core.telegram.org/bots/api#responseparameters)

[建议] 以 Bot + chat + method 分层限速，并按 retry_after 调整；Progress 可以丢弃/合并，Final 预留预算。网络超时但服务端可能已接收时，记录 UNKNOWN，不宣称 sendMessage 存在通用业务幂等键。

### 3.5 媒体与会话映射

[文档] 常用文本上限为 4096 characters；Cloud API 普通文件发送与下载上限不同，典型分别为 50 MB 和 20 MB。Local Bot API Server 的限额不同，不作为托管端能力承诺。[sendMessage](https://core.telegram.org/bots/api#sendmessage)、[sendDocument](https://core.telegram.org/bots/api#senddocument)、[getFile](https://core.telegram.org/bots/api#getfile)、[Local server](https://core.telegram.org/bots/api#using-a-local-bot-api-server)

[建议] 所有 ID 在公开 JSON 使用明确的无损表示，解析按类型处理。文本实体位置按协议的 UTF-16 code unit 处理；不能用 Go byte offset 截取 @ 或格式化片段。媒体保留 file_id/file_unique_id/media_group_id 等源信息并转换为受控附件引用；入站先鉴权，再做受限下载。[MessageEntity](https://core.telegram.org/bots/api#messageentity)

Gateway 应形成稳定的外部会话身份：

```text
ExternalConversationKey = provider + stable account_id + chat identity + topic/thread identity
```

Gateway 传递 `ExternalConversationKey` 与 `ReplyContext`，Worker 根据固定 Admission/Manifest
拥有 Session generation、执行顺序与终态；Binding 不进入外部事件去重键。私聊也不要永远
忽略 topic。群共享会话仍保留每条消息的独立作者；群升级/迁移与新 Agent 绑定时，由 Worker
按明确协议迁移或新建 Session，不靠字符串碰巧相同复用历史。

## 4. 企业微信智能机器人：明确选择长连接模式

### 4.1 接入模式、群触发与身份

[文档] 智能机器人 API 模式在“长连接”和“接收消息回调地址”间选择；切换使旧方式失效。长连接使用 BotID + 专用 Secret，区别于 URL 模式的 Token/EncodingAESKey。这里的“群机器人 Webhook”是另一种产品入口，不拿它代替智能机器人。[模式与连接](https://developer.work.weixin.qq.com/document/path/101463)

[文档] 群内 @ 支持文本、图文混排和引用场景；单聊还支持图片、语音、文件、视频。语音回调提供转写文本等协议字段，SDK 有 voice event 不等于平台已经有音频模型支持。[消息交互场景](https://developer.work.weixin.qq.com/document/path/100719)

[建议] 首版群策略定为 mention-driven；“只回复机器人消息、不带 @”是否收到、客户端差异与管理员配置列入实测。不要提供“企业微信群全量监听”开关而实际收不到消息。

[文档] 入站 `msgid` 用于事件身份；`req_id` 关联请求/响应；`aibotid` 标识机器人。群聊有 chatid；单聊按 from.userid 建目的地。userid 是否明文与创建者权限有关。[长连接消息字段](https://developer.work.weixin.qq.com/document/path/101463)

[建议] Principal 对 userid 作不透明标识处理，不假设一定能调用通讯录解析。回复目标显式携带 single/group；主动发送时设置 `chat_type`，避免默认推断。msgid、req_id、stream.id 分别保留，不能合并成一个 message_id。

### 4.2 一个 Bot 只有一个有效连接

[文档] 同一 Bot 新订阅成功会踢掉旧连接，官方建议主备而非同时多连接。[连接数量限制](https://developer.work.weixin.qq.com/document/path/101463)

[建议] 多副本部署时：

```text
按 Bot 申请 owner lease → 取得 epoch → SDK connect / subscribe
续租成功 → 保持连接
失租/停用/轮换 → 停止接收和发送 → disconnect
新 owner 取得更高 epoch → 建立新连接
```

Lease 应绑定物理 Bot/Account，而不是可重建的逻辑 ChannelBinding。SDK 自动重连必须受 owner 状态控制，避免两个副本互相踢出后持续抢连。connected、authenticated、ready 是不同状态：套接字连接成功不等于机器人认证成功，也不等于持久入口已经可用。

[待实测] 跨 owner 旧 req_id/stream 是否仍可用，长连接断线窗口是否补发、补发时长及顺序。此次官方长连接文档没有给出可据以承诺零丢失的离线保留/重投 SLA；msgid 去重能力不推出“服务端一定补发”。

### 4.3 回复时限、频率和流式

| 行为 [文档] | 当前长连接条款 | 平台影响 [建议] |
| --- | --- | --- |
| 普通回复 | 入站回调后 24 小时窗口 | ReplyTarget 记录 deadline，过期选择明确可用的投递方式 |
| 流式 | 首次发送起 10 分钟；finish=true 后结束 | 记录 stream_started_at / deadline / finished；禁止无限刷新 |
| 欢迎语 | 进入会话事件后 5 秒 | 静态快速回复，不等模型 |
| 更新卡片 | 对应点击事件后 5 秒 | 校验并及时响应，复杂任务异步处理 |
| 会话发送额度 | 回复与主动发送合计 30/min、1000/h | 会话级预算；Final 与交互响应优先 |
| 主动推送 | 目标会话此前须有用户向机器人发消息 | 知道 userid/chatid 不等于已可推送 |

来源：[长连接回复、主动发送及频率](https://developer.work.weixin.qq.com/document/path/101463)。数字用于这个模式；URL 回调模式从用户消息开始的 6 分钟刷新循环不移植到 WebSocket。[URL 模式交互](https://developer.work.weixin.qq.com/document/path/100719)

[建议] 删除“所有渠道固定每秒一次 Progress”的统一策略。企微首版可保守采用不快于约 5 秒的累计快照，并与每会话预算、并发和 Final 预留共同计算；这是平台初值，不是官方推荐刷新周期。流式刷新是否每次占用额度仍需真实测试，初版保守计入。

### 4.4 SDK 有方法，不等于长连接服务端支持

**[文档/源码冲突]** 官方长连接页明确限制“流式+卡片组合”以及 `msg_item`，但 SDK README/API 暴露 `replyStreamWithCard` 和 `msgItem` 参数。首版 capability 应以所选接入模式的服务端约束为准；两项先不作为已支持能力。[服务端协议](https://developer.work.weixin.qq.com/document/path/101463)、[SDK README 固定提交](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/README.md)

[建议] 单独卡片、单独流式、媒体发送分别测试。不要把一个 `supports_rich_reply=true` 扩张成每种组合都支持。

### 4.5 媒体

[文档] 入站资源下载 URL 有效期为 5 分钟；下载任务必须有截止时间。长连接上传采用分片：每片最多 512 KB、最多 100 片、上传会话 30 分钟；素材有效期 3 天。实际类型上限为图片 10 MB、语音 2 MB、视频 10 MB、普通文件 20 MB；类型还有格式要求。已上传分片的幂等/续传语义不等于发送消息幂等。[上传协议](https://developer.work.weixin.qq.com/document/path/101463)

[建议] 不能用 SDK 的分片总容量推导“任意文件支持 50 MB”。下载与解密在鉴权之后执行，限制目的域、大小、时限与类型；aeskey 和临时下载 URL 不进入 Prompt 或常规日志。附件保存成平台引用，不把过期的临时素材当长期文件存储。

## 5. SDK 选型与默认行为

### 5.1 Telegram：Go SDK 初选，但自有 durable ingress

[源码] `go-telegram/bot` v1.25.0 的 WebhookHandler 解码后将 Update 放入进程内 channel，再结束 HTTP handler；ProcessUpdate 默认启动 goroutine；Polling 路径在写入内部队列之前已更新 lastUpdateID。同步 handler 配置影响业务派发，不自动让 Webhook 等待持久事务。[Webhook 实现](https://github.com/go-telegram/bot/blob/3d38d39d8ee18d926b64cb808c78f0927ea6d045/webhook_handler.go)、[业务派发](https://github.com/go-telegram/bot/blob/3d38d39d8ee18d926b64cb808c78f0927ea6d045/process_update.go)、[Polling](https://github.com/go-telegram/bot/blob/3d38d39d8ee18d926b64cb808c78f0927ea6d045/get_updates.go)

[实验] 第 8.1 节的无凭据 probe 观察到：默认配置与 `WithNotAsyncHandlers()` 都在模拟持久化 gate 完成前返回 HTTP 200；直接调用同步 `ProcessUpdate` 才等待业务 handler 返回。缺少配置要求的认证头、损坏 JSON 时，实验观察到一次错误回调与 HTTP 200。[源码] 对应错误分支在进入业务队列之前返回；这不是认证绕过，而是 validation failure 与 HTTP 接收回执未作区分。`WebhookHandler` 忽略 ResponseWriter，错误分支仅报告错误后返回；生产入口应由平台显式写状态码、限制读取大小、等待 Admission 持久提交。[固定错误分支](https://github.com/go-telegram/bot/blob/3d38d39d8ee18d926b64cb808c78f0927ea6d045/webhook_handler.go#L11-L29)

[建议] 初选 `go-telegram/bot` 的类型、方法和出站 Client；生产自行实现持久 HTTP 接收。SDK 最新版本支持新方法不代表需要一次接入所有新产品特性。tRPC-Agent-Go/OpenClaw 可参考文本、附件、格式转换与发送逻辑，不继承其内存 offset、同步运行入口或进程内锁作为生产一致性保证。

### 5.2 企业微信：官方 SDK 源码对照，公开 Go 库作为实现方向

[源码] 固定 main 的默认配置包括 30 秒心跳、有限自动重连、10 秒 HTTP 请求超时；回复 ACK 超时另为 5 秒。同一个 req_id 的回复队列串行并等待匹配 ACK，但不同 req_id 可并发；这不是 per-chat 限流。[Client](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/client.ts)、[WebSocket 与队列](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/ws.ts)

[源码] 收到消息先 emit 通用 message，再 emit 对应 message.text 等类型事件；event 同理。同时把通用与类型事件都接到 Admission 会重复提交。事件监听函数的 Promise 不构成平台的持久接纳 ACK；进程内 reply queue 在崩溃后不保留。[消息分派](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/message-handler.ts)

[建议] Go 库只提供单一原生事件入口与外部协议处理；Gateway Adapter 做事件归一化，Gateway 自己持有租户归属、Admission、Delivery 和跨实例连接 lease。库 handler 完成、服务端命令 ACK、客户端显示分别记录。采用进程内 Go 调用，不增加 Node/Go 桥接协议或独立 Connector 服务。

[建议] npm 1.0.7 的确切包与 integrity 保留为研究基线，非生产安装依赖。自研 `platform/im/wecom` 按当前服务端协议实现，官方 SDK 仅作对照；先做文本/Final 与连接异常测试，再扩展 stream 和媒体。上述 Node 源码默认值不自动成为 Go 库的行为或保证。

## 6. 对 Channel Gateway 契约的直接修订建议

### 6.1 保留 Provider-specific reply context

下面是平台候选协议，不是对任一 Provider 字段的直接复制：

```text
AuthenticatedInbound
  provider_kind, stable_account_id
  external_event_id, event_kind, sender_kind
  external_user_id, chat_kind, external_chat_id, thread_id?
  normalized_text, attachments[], trigger_evidence
  reply_context, received_at, source_payload_digest

RouteSnapshot (仅首次新 admit-run 由 Routing 解析、Admission 固定)
  tenant_id, binding_id, generation  # 账户级 RouteGeneration，不是 Binding 编辑版本
  deployment_revision_id, manifest_ref, manifest_digest

ReplyContext (typed union)
  Telegram: chat_id, thread/topic_id?, source_message_id?
  WeCom: chat_type, chatid_or_userid, callback_req_id?, received_at

DeliveryOperation
  delivery_id, operation_kind, content, reply_context
  provider_message_id?, stream_id?, started_at?, expires_at?
  attempt, certainty: NOT_SENT|ACCEPTED|REJECTED|UNKNOWN
  retry_decision, next_retry_at, last_error_class
```

事件身份（去重）、请求身份（关联）、展示身份（消息/流）分开。Owner Epoch 只 fence 本地
claim/commit 与后续新调用资格，并触发旧 Client 取消；它不能撤销或阻止已经写到 Provider
的请求，也不代替上述三类身份或提供外部 exactly-once。
Delivery 先保存 Provider 调用确定性，再由独立 RetryPolicy 结合 operation、deadline、
`retry_after`、错误类别和 attempt 次数计算重试；`UNKNOWN Final` 不进入普通自动重试。

### 6.2 分开三类快速/慢速工作

1. **协议接纳**：验证并持久化输入后 ACK；WeCom 本地持久交接不冒充服务端离线补发保证。
2. **交互反馈**：Telegram callback answer、企微欢迎语/卡片短窗口；走短路径、独立授权与幂等。
3. **Agent 执行与 Final**：异步 Run 与持久 ReplyIntent/Delivery；发送失败仅重试 Delivery。

大附件下载不阻塞到超过外部交互窗口；若先接纳附件元数据，必须有持久附件摄取任务和过期处理，而不是未落盘 goroutine。必要时首版只接纳文本。

### 6.3 Capability 不是一组永久布尔值

能力至少由 Provider、模式、SDK/API 版本、chat kind、账号权限与当前上下文共同决定。例如：

```text
Telegram private → draft candidate
Telegram group   → message edit candidate
WeCom websocket  → stream + deadline
WeCom callback   → poll-based stream（本次不实现）
```

[建议] 输出 `supported / unsupported / requires_probe` 加约束，避免粗粒度 rich_reply 标记。限流按账号/会话/方法分层，Progress 为可丢弃更新，Final 为持久投递；不在队列无限堆积过期流式更新。

### 6.4 首次支持范围

**下表是平台目标阶段，不是当前支持矩阵。** 2026-09-05 本次源码复核：Telegram 入站
仅把符合校验条件的真人私聊文字转成 Run；群/超级群定向命令与回复触发尚未实现，当前
保存 ignore Receipt，不产生 RunRequested。callback 目前只持久分类，不执行即时 answer。
企微会话额度也是目标能力，当前 Final 切片尚未实现。公开协议库 P0、平台目标 P0 和当前
入站切片是三个不同范围，精确现状见[实施状态](implementation-status.md)。本次不重跑历史实验。

| 阶段 | Telegram | 企业微信智能机器人 |
| --- | --- | --- |
| P0 可靠文字闭环 | 私聊、群定向命令/回复、topic保留与隔离、Webhook、Final、发送错误分类 | API 长连接、单聊/群@、Final、连接 owner、会话限流 |
| P1 交互与进度 | callback 按钮、draft/edit、Stop 映射、topic创建管理 | 短窗口欢迎语/卡片、stream deadline、主动回包条件 |
| P2 媒体 | 图/文件与下载限制、相册语义 | 单聊媒体、混排输入、分片上传/过期 |
| 本轮不作为默认能力 | bot-to-bot、Business、Guest、频道运营、Mini App | 普通群全量监听、URL 回调模式、stream+card/msg_item 组合 |

两者都必须保留 Local IM Provider 的同一持久接纳/回复路径。当前新架构的 NATS、固定 Manifest、ChannelBinding 精确 Revision 约束继续适用；本调研不恢复旧 main 的 Redis Streams 拓扑。

## 7. 真实机器人验收矩阵

所有标记待实测的行都需要真实账号与客户端；测试报告必须记 Bot/Binding 的脱敏身份、API/SDK版本、客户端版本、输入、返回码/ACK、持久状态和重试结果，不能只有聊天截图。

| 编号 | 实验 | 通过条件 | 本轮状态 |
| --- | --- | --- | --- |
| TG-01 | 用户未启动/已启动私聊，移除或阻止 Bot | 区分可达性与权限失败，错误不无限重试 | 已启动私聊收发通过；未启动主动发送、阻止/移除待实测 |
| TG-02 | 群 Privacy on/off（记录修改后重新加群）、Bot admin、定向命令、普通@、reply | 建立实际消息可见矩阵；符合平台触发策略 | 待实测 |
| TG-03 | 同 update_id 并发重投、接纳提交前后断开连接 | 一份 Admission、同一 RunID；持久提交前不成功 ACK | 待实测 |
| TG-04 | 私聊/群 topics、多作者、群迁移 | Session 不串线，Reply 返回原 thread | 待实测 |
| TG-05 | draft 刷新、过期、final、Stop | Progress 与 final 身份分开；取消有权威回执 | draft API 接受、独立 Final 与客户端显示通过；刷新/过期/Stop 待实测 |
| TG-06 | callback 重复点击、过期、另一用户点击 | 即时 UI 反馈与业务幂等/授权分开 | 单次 callback + answer 通过；重复/过期/跨用户待实测 |
| TG-07 | 429、网络不确定、超长文本/emoji/格式实体 | retry_after 生效，分段正确，不盲重发已完成分段 | 4097 ASCII 字符返回 400、中文与 emoji 显示通过；其余待实测 |
| TG-08 | bot-to-bot 开关与自身消息 | 默认过滤自动消息，未形成回复循环 | 待实测 |
| WC-01 | 私聊/群@/普通群文本/不带@引用/媒体 | 记录真实投递矩阵，不把缺失消息称正常支持 | 待实测 |
| WC-02 | 相同 Bot 两实例、主备切换、失租、网络抖动 | 只有 owner 保持连接；旧实例不持续抢回连接 | 待实测 |
| WC-03 | 收帧后进程崩溃、断线期间发送多条消息 | 区分已持久输入恢复与服务端是否补发，量化断线窗口 | 待实测 |
| WC-04 | 同 req_id 持续 stream、10分钟边界、finish后再发 | 按实际错误码结束，不重跑 Agent | 待实测 |
| WC-05 | 欢迎语与卡片5秒边界、慢模型 | 快速路径不被模型或媒体阻塞 | 待实测 |
| WC-06 | 同会话多种发送、30/min与1000/h、流刷新是否计数 | 形成方法/会话额度模型并为Final留预算 | 待实测 |
| WC-07 | 未建立会话与已交互会话主动推送、明确chat_type | 正确区分权限、目的地和可重试错误 | 待实测 |
| WC-08 | 各媒体类型边界、过期素材、断线分片续传 | 使用服务端类型上限，不照搬SDK分片总上限 | 待实测 |
| WC-09 | SDK combo/msg_item 与服务端明确限制的冲突 | 默认关闭；记录真实码与官方版本，后续单独决策 | 待实测 |
| X-01 | Binding切换后旧消息重投、旧Run回复 | 旧Admission保持旧Manifest；新Run使用已应用Revision | 待实测 |
| X-02 | 发送成功但本地记录前崩溃 | UNKNOWN可见；不把外部exactly-once作为保证 | 待实测 |
| SDK-01 | 无凭据本地SDK Webhook实验 | HTTP返回点与业务完成点可独立观测 | 见下一节 |

## 8. 本轮验证与实施边界

### 8.1 SDK-01 本地实验

**[实验] 已执行，退出状态 0。** 运行环境为 `go version go1.25.4 darwin/arm64`；独立 module 固定 `github.com/go-telegram/bot v1.25.0`，Go module proxy 返回 Origin SHA `3d38d39d8ee18d926b64cb808c78f0927ea6d045`。本机在 `artifacts/channel-sdk-research-20260904/sdk-probe/` 留存 `main.go`、`go.mod`、`go.sum`、`probe-output.txt` 与 `exit-status.txt`；该目录是 local-only evidence index，不作为仓库附件。依赖与编译缓存只放临时目录，没有修改项目根 go.mod。

实验使用 `httptest.NewRequest` / `httptest.NewRecorder` 直接调用 HTTP handler，业务 handler 先通知“已开始”，等待 gate 后才设置完成标记。`durable_committed` 只是模拟持久化完成时点的布尔量，不是实际 PostgreSQL 集成测试。`WithSkipGetMe()` 禁止初始化请求，注入的 HTTP client 对任何出站调用立即 panic；只有固定测试字符串，没有真实 token，也没有调用 Telegram 服务器。

正常请求输入：

```json
{"update_id":1,"message":{"message_id":1,"date":0,"chat":{"id":1,"type":"private"},"text":"fixture"}}
```

其他输入：直接同步 `ProcessUpdate` 使用 Update ID 2；缺认证头案例使用 `{"update_id":3}`，SDK 配置了本地固定测试用 webhook secret 但请求不带对应 header；损坏 JSON 输入为单个 `{`。错误案例验证一条 error 回调及 HTTP 状态，不把“错误被记录”误称为 Admission 成功。

先在独立目录准备固定依赖与 go.sum（本轮已执行，下载命令退出状态 0）：

```sh
cd /Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-sdk-research-20260904/sdk-probe
GOWORK=off GOPROXY=https://proxy.golang.org GOSUMDB=sum.golang.org GOMODCACHE=/private/tmp/channel-sdk-research-20260904/module-cache GOCACHE=/private/tmp/channel-sdk-research-20260904/go-cache go mod download -json github.com/go-telegram/bot@v1.25.0
```

随后执行完全离线的精确命令：

```sh
cd /Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-sdk-research-20260904/sdk-probe
GOWORK=off GOPROXY=off GOSUMDB=off GOMODCACHE=/private/tmp/channel-sdk-research-20260904/module-cache GOCACHE=/private/tmp/channel-sdk-research-20260904/go-cache go run . > probe-output.txt 2>&1; rc=$?; printf '%s\n' "$rc" > exit-status.txt; cat probe-output.txt; printf 'EXIT_STATUS=%s\n' "$rc"; exit "$rc"
```

字面输出：

```text
WEBHOOK not_async=false http=200 handler_started=true durable_committed=false
WEBHOOK not_async=true http=200 handler_started=true durable_committed=false
DIRECT_PROCESS not_async=true returned_only_after_handler=true
WEBHOOK rejected=missing_secret http=200 errors=1
WEBHOOK rejected=malformed_json http=200 errors=1
RESULT=PASS network_calls=0
EXIT_STATUS=0
```

**实验结论：** SDK 的同步业务派发选项没有改变内存队列型 webhook ACK 边界；Gateway 应自有入站 HTTP adapter，在持久 Admission 事务提交后返回 2xx。此实验只证明固定 SDK 的本地控制流；不证明真实渠道重投、网络故障恢复、数据库提交或用户客户端显示结果。

### 8.2 新建真实 Telegram Bot 与私聊实验

**[实验] 通过 BotFather 新建 `JFS Channel SDK Lab`，实际用户名为 [@jfschannellab260904bot](https://t.me/jfschannellab260904bot)，Bot ID 为 `8853168383`。** 原有机器人未修改。`getMe` 验证了新 Bot 身份，默认 Privacy Mode 开启、private topics 未开启；未设置 Webhook 或命令菜单。

测试凭据通过本机 loopback 验证页进入临时进程内存，没有写入代码、报告或测试输出。真实实验使用 Python HTTP harness 直接调用 Bot API；它与第 8.1 节的 Go SDK 离线实验是两条独立证据链，**不称为 Go SDK 或生产 Gateway 的真实端到端验证**。测试代码与结果的本机目录为 `/Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-sdk-research-20260904/live-telegram`（local-only）。

用户在 Telegram 客户端点击 Start 后产生 `/start`，随后也发送了普通文本。harness 选择 `/start` 作为本次输入；只对本次新机器人私聊发送明确标识的实验消息。用户点击“验证按钮 ACK”后，收到对应 callback 并调用 answer。测试没有运行 Agent、数据库 Admission 或 Delivery Outbox。

| 检查 | 实际输入/动作 | 实际结果 |
| --- | --- | --- |
| LIVE-01 | `getMe` | 身份匹配，`is_bot=true`，Privacy on，topics off |
| LIVE-02 | `getWebhookInfo` | URL 为空，开始本轮时 pending updates 为 2 |
| LIVE-03 | `getUpdates` 接收用户 `/start` | private、非 bot sender、有 update ID |
| LIVE-04 | 不推进 offset，重复 `getUpdates` | 同一个未确认 update 再次返回 |
| LIVE-05 | `offset=该 update_id+1` | 先前 `/start` update 不再返回 |
| LIVE-06 | 发送 `LAB-01` 文本 | `ok=true`，返回 message ID |
| LIVE-07 | 编辑该 message ID，含中文与 emoji | `ok=true`，message ID 不变；客户端可见 |
| LIVE-08 | `sendMessageDraft`，draft ID `260904` | `ok=true`；本轮只确认服务端接受，未测预览显示/消失时刻 |
| LIVE-09 | 独立 `sendMessage` 发送 `LAB-FINAL` + inline button | `ok=true`，返回 message ID；客户端可见 |
| LIVE-10 | 单次发送 4097 个 ASCII `A` | 预期拒绝：`400`，`Bad Request: message is too long` |
| LIVE-11 | 用户点击按钮，data 为 `lab_ack_260904`；`answerCallbackQuery` | 收到 query ID，answer `ok=true`；随后 Final 编辑为验证成功 |

**最终运行：11/11 通过，进程退出状态 0。** 另一次独立 `getMyCommands` 读取确认命令列表为空，不计入上述 11 项。收尾再次读取 Webhook/命令菜单，确认 Webhook 仍为空、pending updates 为 0、命令菜单仍为空，结果留存于 `post-state.json`。客户端最终状态已通过原生 Telegram 截图观察：编辑后的 LAB-01 与验证完成的 LAB-FINAL 留存；未保存包含其他会话的截图。

迁移后的复现命令（依赖临时验证页已连接对应 Bot，凭据不作为命令参数）：本次真实 API 测试在迁移前执行，原始命令与结果的本机路径为 `/Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-sdk-research-20260904/history/pre-relocation/VERIFICATION.txt`（local-only）。本次迁移没有重新调用 Telegram API。

```sh
cd /Users/jfs/Projects/trpc-agent-service-channel-gateway
set -o pipefail
python3 artifacts/channel-sdk-research-20260904/live-telegram/run.py 60 | tee artifacts/channel-sdk-research-20260904/live-telegram/run-output.txt
rc=$?
printf '%s\n' "$rc" > artifacts/channel-sdk-research-20260904/live-telegram/exit-status.txt
printf 'LIVE_EXIT_STATUS=%s\n' "$rc"
exit "$rc"
```

最终字面输出的摘要：

```text
{"test": "LIVE-10 oversized text rejected", "pass": true, "details": {"input_characters": 4097, "ok": false, "error_code": 400, "description": "Bad Request: message is too long"}}
{"test": "LIVE-11 callback and answer", "pass": true, "details": {"callback_data": "lab_ack_260904", "has_query_id": true, "ok": true}}
LIVE_COMPLETE passes=11 total=11
LIVE_EXIT_STATUS=0
```

完整字面输出、结构化逐项结果与实际退出状态的本机文件依次为
`artifacts/channel-sdk-research-20260904/live-telegram/run-output.txt`、
`artifacts/channel-sdk-research-20260904/live-telegram/results.json`、
`artifacts/channel-sdk-research-20260904/live-telegram/exit-status.txt`（均为 local-only）。

此前两次等待因用户尚未 Start 而未接到测试输入，结果保留为 `attempt-01-*` / `attempt-02-*`，不记作渠道能力失败。第一次触发代码中定义为退出 2 的等待失败分支，但 shell 的 tee pipeline 没有 pipefail 而显示 0（未单独捕获 Python 退出状态）；第二次启用 pipefail，实际 shell 退出 2。最终第三次在用户完成 Start 后运行，所有 11 项通过，真实退出 0。此过程保留等待失败而不覆盖其证据。

**推导边界：** Polling 的重复返回证明了本次未确认 update 可重取，不证明 Webhook 重投、崩溃恢复或持久 exactly-once。draft API 成功不证明客户端实际渲染或过期行为。单次按钮成功不证明跨用户授权/重复点击幂等。4097 字符拒绝不证明分段器正确，也没有施加 429 压力。群可见性、topics、媒体、Stop、故障恢复和全部企微真实测试仍按第 7 节执行。

收尾已停止 PID `33993` 的临时凭据进程；loopback `/status` 请求得到 curl 退出码 `7`（连接失败），两个临时浏览器页已关闭。新 Bot 与实验消息保留，没有更改命令菜单、Webhook、Privacy 或 topics 配置；当前没有持续运行的测试接收器。关闭本地进程不等于注销 Bot 凭据。

### 8.3 文档校验

本机审计包包含原始 SHA-256、差异文件、结构/引用/版本检查和独立副本回滚验证；这些
`artifacts/` 文件不会随仓库提交。文档校验只证明报告与本机留存副本的完整性；Telegram
私聊实测证据单列于第 8.2 节，企业微信真实接入仍待验证。原报告保留，本文件保留更新后的内容。

### 8.4 下一步应冻结的决定

1. 接纳首批 Provider：Telegram Bot API + 企业微信智能机器人 WebSocket。
2. Telegram SDK 直接导入，生产入口自有 Webhook；企微采用公开 Go 包 `platform/im/wecom`，与 Gateway 同进程，不独立部署。
3. 群触发从 Provider 实际可见性出发；Bot自动消息默认过滤。
4. 消息接纳、交互反馈、持久 Final 分开；保留 Provider-specific ReplyContext。
5. 连接独占性、短窗口、限流、媒体和SDK能力差异进入可执行验收，而不是只列功能清单。
6. 实现前在当前新架构工作树落地契约，不直接把旧 main 的网络/内存入口迁作生产正确性边界。
