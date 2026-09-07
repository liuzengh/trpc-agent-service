# IM Channel Adapter 设计

实现更新：rc.4 的 Telegram 限类型附件已可经队列保存 Artifact、按会话权限读取文本，默认关闭；企业微信 MCP 仍隔离媒体。当前实现和配置以[后续记录](reliability-followup.md)为准，下面保留目标方案及历史阶段的说明，不能把媒体占位信息当成模型已经读取文件。

当前用户的企业微信接入入口是托管消息 MCP，已实际连接并查询工具列表，见[企业微信 MCP](wecom-mcp.md)。下文第 3 节描述的是原自建应用回调 Adapter，不适用于该 MCP 配置页；两种协议不能混用。

## 1. 通道抽象

当前代码已实现统一 `Adapter`、`CallbackAdapter`、Channel Registry、企业微信 Adapter、Telegram Adapter 和 HTTP Test Adapter。这里的“实现”指代码与模拟协议测试，不表示已经使用真实企业微信账号或 Telegram Bot 完成联调。当前出站只发送文本；图片和文件只会规范化 provider media ID，受控下载、病毒扫描、Artifact 转存和媒体回复尚未实现。

| 通道 | 当前代码 | 验证层级 | 待完成 |
| --- | --- | --- | --- |
| 企业微信 | URL 验证、加密回调、文本解析、Token 缓存、应用文本发送 | `httptest` 模拟企业微信 API | 真实账号、公网回调、真实收发、媒体/卡片发送 |
| Telegram | Webhook Secret、Update/Topic 解析、文本 `sendMessage` | 固定域名下完成私聊、群聊、Topic、去重和 Webhook 恢复 | 真实 429、群白名单/require_mention、编辑/媒体发送 |
| 微信公众号/微信客服 | 接入设计 | 文档评审 | Adapter 实现与真实联调 |

OpenClaw 的 `Channel` 只有 `ID()` 和 `Run(ctx)`，适合示例和进程内组合。平台需要更明确的入站、回复和能力模型：

```go
type Adapter interface {
    Type() string
    Verify(ctx context.Context, req *http.Request, binding Binding) error
    Decode(ctx context.Context, req *http.Request, binding Binding) ([]InboundEnvelope, error)
    Ack(ctx context.Context, w http.ResponseWriter, result AckResult) error
    Send(ctx context.Context, binding Binding, msg OutboundEnvelope) (DeliveryReceipt, error)
    Capabilities() Capabilities
}
```

统一入站消息结构：

```go
type InboundEnvelope struct {
    ChannelType       string
    ChannelBindingID  string
    ExternalMessageID string
    ExternalUserID    string
    ExternalChatID    string
    ExternalThreadID  string
    ChatType          string
    MessageType       string
    Text              string
    Parts             []ContentPart
    ReplyToID         string
    OccurredAt        time.Time
    RawPayloadRef     string
}
```

`RawPayloadRef` 指向加密原文存储，正常日志不打印原始 callback。Adapter 输出后，Gateway 才能根据 binding 推导租户和 Agent App。

## 2. 与 Runner 的转换

文本消息转换为 `model.Message{Role: model.RoleUser}`。当前图片、音频和文件只转换为包含 provider media ID 的占位文本。目标方案是在独立 Downloader/Artifact Job 中下载和转存媒体，再构造模型支持的多模态内容；远程 URL 必须经过域名白名单、DNS 重绑定检查、大小限制和 MIME 校验。

运行时补充以下 `RuntimeState`：

```text
tenant_id
app_id
channel_type
channel_binding_id
actor_user_id
external_chat_id
external_thread_id
external_message_id
reply_target
```

真实用户身份不拼入 prompt；Tool 和 PermissionPolicy 从可信 context 或 RuntimeState 读取。

Runner Event 转换规则：

| Event | 通道行为 |
| --- | --- |
| partial assistant text | 支持编辑消息时更新预览，否则只聚合 |
| final assistant text | 发送最终文本，按通道长度切分 |
| tool call | 可显示“正在调用某工具”，但隐藏敏感参数 |
| tool result | 默认不直接发给用户，由模型总结 |
| `approval_required` | 当前发送文本确认指令；卡片或带签名按钮是目标能力 |
| artifact/file | 上传文件或发送短期下载链接 |
| error | 映射为可读错误，不返回内部堆栈和密钥 |

## 3. 企业微信

### 账号绑定

一个 Channel Binding 保存：

```text
CorpID
AgentID
callback Token secret_ref
EncodingAESKey secret_ref
应用 Secret secret_ref
可信域名和回调能力
```

回调地址：

```text
/callbacks/wecom/{callback_key}
```

`callback_key` 是随机值，不使用 `tenant_id`。收到请求后先查询 binding，再从 Secret Manager 读取 Token 和 EncodingAESKey。

### 回调处理

Adapter 支持 URL 验证和业务 POST。处理顺序为：

1. 解析 `msg_signature`、`timestamp`、`nonce`；
2. 校验时间偏移和签名；
3. AES 解密消息；
4. 校验接收方 CorpID 或应用标识；
5. 解析 XML/JSON；
6. 生成稳定 external message ID；
7. 持久化成功后返回协议要求的响应。

Agent 通常无法在 callback 的短处理窗口内完成，因此不把长时间 Runner 执行放在被动回复中。Adapter 先 ACK，再由 Reply Sender 调企业微信发送 API。发送 token 按 binding 缓存，缓存键包含 secret version，失效后只允许单个 goroutine 刷新。

### 用户和群聊

- 单聊用户：使用企业成员 UserID 或外部联系人 ID，二者加类型前缀；
- 群聊：使用群 ID，必要时增加机器人或应用账号 ID；
- 同一个自然人在不同企业、不同 binding 下不会自动合并；
- 身份合并必须经过租户自己的映射规则或管理员确认。

### 消息类型

文本直接进入 Runner。当前图片、语音和文件只记录受控的 media ID 占位信息，不会自动下载。隔离临时目录、病毒扫描、Artifact 转存，以及位置、链接和引用消息的结构化处理属于后续实现。无法解析的消息会被忽略或记录为不支持的消息类型。

默认安全模式不自动下载：企业微信 image/file/voice/video 与 Telegram photo/document 会转换为包含 media/file ID、文件名、MIME/caption 的占位文本并可靠入库，不直接访问 callback 中的 URL。启用多模态处理时，必须由独立 downloader 使用 provider API 获取文件，完成大小/MIME/病毒扫描后再保存到 Artifact Router。

## 4. 微信公众号和微信客服

微信公众号与企业微信都使用回调验签和消息加解密思路，但账号体系、用户标识、客服消息 API、模板/订阅消息能力和发送时效不同，因此不能共用一份配置结构。

公众号 Binding 至少包含 AppID、AppSecret 引用、Token、EncodingAESKey 和消息加密模式。用户一般以 OpenID 标识；如果租户有 UnionID 条件，可以通过 `external_identity` 映射到同一 canonical user，但映射过程仍限定在该租户内。

公众号回复策略：

- 简短固定回复可以走被动回复；
- Agent 长任务先返回轻量提示，再走客服消息或其他允许的异步触达方式；
- 超过平台发送窗口时把结果保存到会话，等待用户下一条消息或使用租户允许的模板能力；
- 不绕过平台规则主动发送营销内容。

微信客服存在客服账号、客户身份和会话状态等额外概念，建议单独实现 `wechat_kf` Adapter，不在 `wechat_official` 中堆条件分支。

## 5. Telegram

当前 Telegram Adapter 实现 webhook；`getUpdates` 长轮询仅作为可选设计，尚未实现。生产环境联调需要使用真实 Bot 配置公网 webhook。

当前 Binding 保存 bot token Secret 引用、webhook secret token、可选 API Base URL，以及群聊安全字段：

```json
{
  "bot_token_ref": "env://TELEGRAM_BOT_TOKEN",
  "webhook_secret_ref": "env://TELEGRAM_WEBHOOK_SECRET",
  "bot_user_id": 123456789,
  "bot_username": "example_bot",
  "allowed_chat_ids": [-1001234567890],
  "require_mention": true,
  "ignore_bot_messages": true
}
```

Webhook 校验 `X-Telegram-Bot-Api-Secret-Token`。过滤只作用于群聊：私聊继续正常进入；群聊先检查白名单和发送者，再识别 Telegram `mention`、`text_mention`、`bot_command` entity 或对 Bot 消息的回复。不符合条件时 Adapter 返回正常 ACK，但不写 Inbox。Telegram entity 的 offset/length 使用 UTF-16 code unit，不能按 Go byte 或 rune 下标直接截取。

Telegram 的 `update_id` 可作为外部去重键。消息 ID 在 chat 内唯一，组合键为：

```text
bot_id | chat_id | message_thread_id | message_id
```

普通消息文本上限为 4096 字符。Reply Sender 建议按 4000 rune 切分，避免 HTML 转义后超过限制。格式化消息失败时降级为纯文本。流式模式可分为：

- `off`：只发最终回复；
- `block`：目标方案是先发“处理中”，完成后编辑一次；
- `progress`：目标方案是按节流间隔编辑预览，最后替换为最终内容。

当前 Adapter 只实现 `off`，即聚合完成后调用 `sendMessage` 发送最终文本，不声明 `SupportsEdit` 或 `SupportsFile`。

群组话题使用 `message_thread_id` 生成独立 session。Bot 隐私模式由 Telegram 平台配置决定；平台侧已经支持 `allowed_chat_ids`、`require_mention` 和 `ignore_bot_messages`。要使用自然的 `@Bot` 文本，需要关闭 Privacy Mode 让 Telegram 先投递群消息，再由平台过滤；保持 Privacy Mode 时仍可使用 `/ask@bot` 或回复 Bot。

## 6. Session ID 规则

平台对外部标识做规范化并使用 HMAC 或 SHA-256，避免在存储 key 和日志中暴露原始群 ID。

| 场景 | runtime_user_id | session_id 输入 |
| --- | --- | --- |
| 单聊 | canonical user | tenant、binding、channel account、user、active session salt |
| 群聊按人隔离 | canonical user | tenant、binding、chat、thread/topic |
| 群聊共享 | synthetic group principal | tenant、binding、chat、thread/topic |
| 用户主动 `/new` | 不变 | 原输入加新的随机 session salt |

示例：

```text
runtime_user_id = usr_5f3...
session_id = ses_base64url(hmac(tenantKey, binding|chat|thread|salt))
```

跨群默认不共享 Session。跨租户永不共享 Session、Memory 或身份映射。跨通道是否合并用户由租户配置决定，即使合并 canonical user，`session_id` 仍保留 binding/channel 维度。

## 7. 消息去重和乱序

Adapter 只负责提取外部 ID，最终去重由 Gateway 的数据库唯一索引完成。对于可能乱序的通道，记录平台时间和上游时间，但不直接按上游时间覆盖已有消息。

同一 conversation 的消息进入同一队列分区。若后到消息的上游 sequence 小于已处理水位：

- 已处理过的 ID：忽略；
- 未处理但仍在允许乱序窗口：插入等待队列；
- 超出窗口：按租户策略丢弃、补处理或提示用户重新发送。

## 8. 限流和重试

限流分四层：

- Channel callback QPS：保护入口；
- tenant/user/session：防止单租户或单用户挤占资源；
- model/tool budget：控制外部调用；
- reply sender：遵守 IM 发送频率。

发送失败按错误类型分类。网络错误、限流和服务端错误可退避重试；参数错误、用户屏蔽、账号权限不足通常是终止错误。平台支持通道返回的 retry-after，超过最大重试后进入死信和告警。

## 9. 撤回、编辑和失败处理

如果通道提供撤回或编辑事件，Adapter 将其转换成新的 inbound event，不直接修改历史 Session Event。平台可以追加一条系统 Event 标记原消息已撤回，是否影响模型上下文由租户策略决定。

回复发送失败不回滚 Session。outbound message 保留最终文本和 Artifact 引用，Sender 可以独立重试。用户下一次发消息时，如果上一条回复仍未送达，Gateway 可以先触发补发或在新回复中说明上一条投递失败。
