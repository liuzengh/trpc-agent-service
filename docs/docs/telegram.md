# Telegram 长轮询 Channel Adapter

> Issue #31 的基础实现；Issue #77 在此之上补充 webhook、媒体/rich update 与统一回复渲染，Issue #98 补齐受控附件入站和图片/文档原生出站。

## 1. 交付边界

Telegram 适配器是一个绑定级别的协议入口，不创建第二套租户或 Runner 路由。一个适配器实例
只代表一个 active Telegram Binding，运行链路固定为：

```text
Telegram getUpdates
  -> Update.Message 校验和规范化为文本或媒体附件
  -> 已验证的 channels.RoutingTarget
  -> gateway.Channel Principal
  -> gateway.DispatchService
  -> 完整消费脱敏 DispatchEvent
  -> 聚合并分段
  -> Telegram sendMessage / SendPhoto / SendDocument
```

long polling 与 webhook 共用同一个 Adapter、幂等和 Gateway 边界；命令、回调仍 fail closed。
Webhook 由调用方拥有 HTTP listener，`telegram.Webhook` 只负责精确 path、secret header、请求
解码和优雅关闭。媒体 caption 与 rich update 会被规范化为受限的 Gateway content type。

## 2. 适配器边界与构造

实现包位于 `trpcservice/channels/telegram`。公开构造配置的语义如下：

| 配置 | 约束 |
| --- | --- |
| `BotToken` | 仅为运行时输入，不能写入 Binding、Plan、缓存键、日志、trace 或错误；构造成功后只由 SDK client 持有 |
| `Target` | 必须由现有 trusted boundary 创建并通过 `Validate()`；必须是 active Telegram Binding 的 RoutingTarget |
| `Dispatcher` | 使用现有 `gateway.DispatchService`；适配器不能直接调用 Runner |
| `Idempotency` | 可注入现有 `gateway.IdempotencyStore`；未注入时由适配器拥有一个进程内实例，不宣称跨进程保证 |
| `APIBaseURL` | 可选 HTTPS origin，仅用于 SDK Bot API；不从 Telegram update 或 webhook 字段读取 |
| `HTTPClient` | 可选的 SDK HTTP client，测试使用 fake/`httptest`，不要求真实凭据 |
| `PollTimeout` | 可选 long-poll timeout；采用 SDK 默认值时不自行覆盖 |
| `Workers` | 零值为 1；大于 1 必须由调用方显式配置，并由 SDK 同步处理 handler 生命周期 |
| `ErrorHook` | 只接收稳定的适配器错误类别，不接收 SDK/provider 原始错误、token 或 endpoint 凭据 |
| `Factory` | 注入式 Bot factory；生产实现才依赖 `github.com/go-telegram/bot`，测试不创建网络 client |
| `Attachments` | 可选 runtime attachment store；配置后媒体 bytes 在进入 Runner 前先按 tenant/event/reference 持久化 |
| `MediaDownloader` | 可选受控下载器；未配置时生产 SDK client 会在具备 `GetFile` 能力时创建默认下载器 |
| `MaxAttachmentBytes` | 单附件大小上限，零值使用协议中立默认限制 |

构造函数接收 Context，先创建带默认 update handler 的 client，再调用 `getMe`，把返回的 Bot user
ID 规范化为十进制字符串并与 `Target.ProviderAccountID` 精确比较。创建失败、`getMe` 失败或身份
不一致都 fail closed；在身份通过前不得处理任何 update。

适配器内部只保存由 `gateway.NewChannelPrincipal(Target)` 产生的 principal。Telegram update
不包含并且不能覆盖 tenant、binding、app、model、profile 或 routing hint；显示名、username 和
标题仅作为展示元数据，不能参与认证、session 或 Runner identity。

Bot factory 对 SDK 使用以下固定策略：

- `WithSkipGetMe`，由适配器在自己的 Context 中执行并校验 `getMe`；
- `WithDefaultHandler` 指向适配器的单 update handler；
- `WithNotAsyncHandlers`，使 `Run(ctx)` 结束时不会遗留 SDK 自己创建的异步 handler goroutine；
- `WithWorkers` 采用已验证的 worker 数量；
- 可选地设置 server URL、HTTP client 和 polling timeout；
- SDK polling error 只转换成稳定的 `polling` hook 事件，不能直接透传或记录原始错误。

## 3. 入站规范化与幂等

基础文本字段仍按以下规则规范化：

| Telegram 字段 | Gateway 字段 | 规则 |
| --- | --- | --- |
| `Message.Text` | `InboundMessage.Content` | 必须非空；继续交给 `InboundMessage.Normalize()` 做 trim/长度校验 |
| `Message.From.ID` | `ExternalUserID` | 必须存在且非零；不能使用 username/姓名 |
| private `Message.Chat.ID` | `ConversationDirect` + `ExternalPeerID` | chat ID 是稳定会话身份 |
| group/supergroup `Message.Chat.ID` | `ConversationGroup` + `ExternalChatID` | chat ID 进入群会话身份 |
| `Message.MessageThreadID` | `ExternalThreadID` | 大于零时保留；发送回复时原样作为 forum thread |
| `Update.ID` + trusted `BindingID` | `ExternalMessageID` / `RequestID` | 使用长度前缀编码生成稳定、无碰撞的 binding-aware ID |

Issue #98 之后，图片、文档、音频和视频会保留 provider file id，经受控 downloader 下载、限流、
校验并写入 attachment store；没有 attachment store 时仍保留兼容的 caption 或 `[telegram media]`
文本标记，不把 provider 下载 URL 或 token 传入 Runner。

编辑消息、channel post、callback/inline、service update、无 sender/chat、未知 chat 类型和
不支持的 rich/service update 都以稳定的非敏感原因忽略或拒绝，且不得进入 Dispatch。所有合法消息先用固定
principal 调用 `IdempotencyStore.Begin`：

- pending duplicate 不再次调用 Dispatch，也不启动隐藏 retry；
- completed duplicate 重用已缓存的脱敏 DispatchEvent，并只重新发送一个聚合后的逻辑回复；
- dispatch 或发送前的处理失败释放 claim，允许调用方按既有进程内策略重新处理；
- 该 store 只保证当前进程，不能暗示跨节点、重启恢复或持久化语义。

## 4. Dispatch 与回复

适配器把规范化消息和可信 principal 交给 `DispatchService.Dispatch`，`RequestID` 使用上节生成
的稳定值，Context 原样向下传递。它必须读完事件 channel 直到关闭，不能在第一个 `message` 或
`done` 后提前退出。只拼接 `DispatchEventMessage.Text`；收到脱敏 `error`、stream 异常或空的
dispatch stream 时，发送固定的适配器级失败文本，不暴露 provider error、stack trace、Secret
或 repository 细节。

正常文本回复按 Unicode code point 切分，每段最多 4096 个 code point，并逐段调用：

```text
sendMessage(chat_id=Message.Chat.ID,
            message_thread_id=Message.MessageThreadID when > 0,
            text=chunk)
```

不为每个 partial event 发送 Telegram 消息，不在本 Issue 引入编辑消息、队列、退避或后台重试。
`sendMessage` 失败只通过稳定的 `send` hook 暴露；若已发送部分分段，不回滚也不启动隐式重试。

## 5. 生命周期与错误脱敏

```mermaid
sequenceDiagram
    participant C as Service Context
    participant A as Telegram Adapter
    participant B as Bot SDK
    participant G as Gateway Dispatch
    participant T as Telegram API

    C->>A: New(ctx, runtime token, trusted Target)
    A->>B: New + getMe
    B-->>A: bot user ID
    A->>A: compare provider_account_id; mismatch fails closed
    C->>A: Run(ctx)
    A->>B: Start(ctx)
    B->>T: getUpdates (long polling)
    T-->>B: Update.Message
    B->>A: HandleUpdate(ctx, update)
    A->>G: trusted Principal + normalized InboundMessage
    G-->>A: complete redacted DispatchEvent stream
    A->>T: one or more sendMessage chunks
    C-->>B: cancel ctx
    B-->>A: stop polling and synchronous handlers
    A-->>C: Run returns
```

`Run(ctx)` 是阻塞入口；Context 取消必须同时结束 SDK polling、在途 Dispatch 和 sendMessage。适配器
不创建自己的 retry goroutine，不保存 request Context，不持有 Runner lease；lease 和 event drain
由现有 Gateway contract 管理。`Close` 只关闭适配器拥有的进程内幂等 store，不能关闭调用方注入
的 store 或 HTTP client。

错误 hook 只使用 `initialization`、`polling`、`update`、`dispatch`、`send` 等稳定 operation 和
适配器 sentinel error。原始 SDK error 只能用于本地判断，不能出现在返回值、hook payload、日志、
trace 或 Telegram 回复中。

## 6. 文档与代码验收清单

README 和 MkDocs 状态同步以下已交付能力：

- [x] SDK 版本固定，Bot factory/client 可注入，`Run(ctx)` 和单 update handler 可测试；
- [x] `getMe` 身份校验、tenant/Binding/Runner 隔离、普通文本映射和 binding-aware 幂等通过测试；
- [x] Dispatch 完整消费、单逻辑回复、4096 code point 分段、forum thread 路由和失败脱敏通过测试；
- [x] cancellation、polling error、send failure、duplicate delivery 和资源生命周期通过测试；
- [x] Telegram long polling、webhook、持久化 outbox、媒体附件入站、图片/文档原生出站和 fallback
      已实现，并由单测、provider integration 和 live E2E 覆盖。

参考：[Telegram Bot API](https://core.telegram.org/bots/api)、
[getUpdates](https://core.telegram.org/bots/api#getting-updates)、
[github.com/go-telegram/bot](https://github.com/go-telegram/bot)。

## 7. 真实 Telegram E2E

Issue #33 提供根目录 `examples/telegram-e2e/` 示例和手动触发的 CI 工作流，
用于验证真实的 `getMe -> getUpdates -> sendMessage` 边界。示例内部使用确定性
`DispatchService`，因此不会把模型供应商凭据和 Telegram 传输冒烟测试混在一起。

本地运行只需要在进程环境中提供 `TELEGRAM_BOT_TOKEN`；Token 不得进入仓库、日志、
trace 或错误。CI 使用受保护的 `telegram-e2e` Environment，至少配置接收 Bot 的
`TELEGRAM_BOT_TOKEN`，并在需要完全自动化入站消息时配置第二个受控测试 Bot 的
`TELEGRAM_SENDER_BOT_TOKEN`。一个 Bot Token 不能模拟普通用户向自己发送入站消息，
所以当前 workflow 必须显式配置第二个受控测试 Bot；本地人工运行可以不配置发送者。

示例和 CI 以普通文本 marker 完成真实 Bot API 传输验收；媒体、命令和 rich update 的规范化、
fallback 与原生回复行为由 deterministic/provider integration 测试覆盖。详见
[Telegram live E2E example](https://github.com/XnLemon/trpc-agent-service/tree/main/examples/telegram-e2e)
和 Issue #33。
