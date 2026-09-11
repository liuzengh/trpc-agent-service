# IM 平台限制与降级策略

> 状态：实现 + 文档（阶段 33，需求验收 B5；阶段 45 补齐卡片 / 附件 / HTTP 回调入口）。
> 代码：`trpcservice/infra/channels/limit.go`（截断/限流契约）、`redis_ratelimit.go`
> （Redis 固定窗口）、`gateway.go`（gateway.Run 截断 + pumpInbound 限流 + 多模态消息构造）、
> `media.go`（附件 → ContentParts + 清单）、`webhook.go`（HTTP 回调入口）、
> `feishu/webhook.go`（飞书回调验签/解密/归一化）、`wecom/conn.go`+`feishu/conn.go`（附件下载）；
> 配置：`config.yaml rate_limit` 段、`im.webhook` 段（`infra/config/config.go`）。

## 1. 平台差异速查（代码与策略取值）

| 限制项 | 企业微信 wecom | 飞书 feishu | 平台策略 |
| --- | --- | --- | --- |
| 文本消息长度 | 2000 runes | 4000 runes | 出站按通道截断（limit.go `maxTextLen`） |
| 接入方式 | 仅 AI-bot WSS 长连接 | 长连接 **或** HTTP 事件订阅 | 见 §3；`im.webhook.channels` 按通道二选一 |
| 交互卡片按钮回调 | 无（降级为文本提示） | 有（`behaviors.callback`） | 见 §4 |
| 入站附件 | image/file/voice（URL + AesKey，需解密） | image/file/audio（file_key，需 message-resource API） | 见 §5 |
| 撤回事件 | MsgTypeEvent recall | 同 | 忽略（不入库），后续会话继续 |
| 频率 | 无平台硬限，防滥用 | 同 | 入站限流（默认关，见 §2） |


## 2. 出站截断与入站限流（已实现）

### 2.1 出站长度截断

`gateway.Run` 在向 adapter 发送前调用
`truncateText(text, maxTextLen(m.Channel))`（gateway.go，limit.go `truncateText`）。
- 按 rune 截断，多字节 UTF-8 不会被切断；截断末尾补 `…`；
- 超限回复**绝不**因平台错误打爆出站流——先截后发；
- 未知通道走 4000 兜底。

### 2.2 入站频率限流（Redis，多节点共享）

- 契约 `channels.RateLimiter`（`Allow(ctx, key)`）；默认不安装（`nil` = 不限）。
- 实现 `RedisRateLimiter`：`INCR` + 首次 `EXPIRE` 的**固定窗口**计数器，
  key 前缀 `ratelimit:`；窗口内超过 `limit` 拒绝。
- 维度：`inboundLimitKey(tenant, channel, user)`——一个租户的滥用不会波及其他
  租户，不同平台同 id 不共享桶。
- 安装位置：`gateway.pumpInbound` 在消息进入总线**之前**拦截（gateway.go）；
  `main.go` 在 IM gateway 装配处按配置注入（`cfg.RateLimit.Enable &&
  cfg.Redis.URL` 非空时）。
- **错误语义**：limiter 后端不可用 → fail-open（记录 + 放行），可用性优先。

```yaml
rate_limit:
  enable: false      # 默认关；生产建议 true
  per_minute: 60     # 每 租户+通道+用户 每分钟上限
```

### 2.3 固定窗口取舍

固定窗口在窗口边界允许 2 倍突发（最后 1s + 下窗口首 1s 各满额）。对“防脚本刷
接口、防单用户拖垮模型预算”足够；要严格平滑可换令牌桶（Redis Lua），当前未引入
（成本/收益不划算，见 §5 容量评估）。

## 3. 两种接入方式：长连接 vs HTTP 回调（阶段 45 新增）

同一套适配器（归一化 / 去重 / 附件下载 / 回复发送）有两条入站通路，**同一通道
二选一**，不会同时启用（同时启用会把同一条消息投递两次）：

| 方式 | 入站 | 出站 | 适用 |
| --- | --- | --- | --- |
| 长连接（默认） | 我们主动连平台：WeCom AI-bot WSS、飞书 `ws.Client` | 平台 OpenAPI / aibot `SendMessage` | 平台只提供长连接（企业微信） |
| HTTP 回调（`im.webhook`） | 平台 POST 到 `POST /webhooks/im/{binding_id}` | 同左（发送不经过回调） | 飞书「事件订阅 → 请求网址」；容器化/多副本、不想持有长连接 |

- **默认关闭**：`im.webhook.enable: false`。端点是公网可达的，唯一凭据是平台签名。
- **fail-closed**：绑定未配置验证密钥（`verification_token_ref`，飞书填 **Encrypt Key**）、
  密钥解析失败、签名不匹配 —— 一律 401，绝不"验不过就放行"。
- **启动期校验**：`im.webhook.channels` 里出现平台不支持回调的通道（企业微信 AI-bot
  只讲自己的 WSS 协议）→ `exit 2`，而不是挂一个永远收不到请求的端点。
- **响应语义**：握手 → `{"challenge": "..."}`；已接收 → `{"code":0}`；适配器缓冲区满
  → `503`（平台会重试，不丢消息）；未知绑定 → `404`。
- **飞书回调支持的事件**：`im.message.receive_v1`（原样透传进适配器）、
  `card.action.trigger`（重新包装成卡片回调信封，复用与长连接完全相同的审批链路）、
  `url_verification`（握手）。加密模式（`{"encrypt": "..."}`）用 Encrypt Key 解密。

```yaml
im:
  webhook:
    enable: true          # 默认 false
    channels: ["feishu"]  # 仅 feishu 支持；列出其他通道会拒绝启动
```

> 说明：企业微信**没有**回调入口。企业微信「回调 URL」属于自建应用/客服产品，
> 其被动回复与 `message/send` 通道本平台未实现，因此不支持 —— 这是产品边界，
> 不是遗漏。未来接入微信客服/公众号时，只需新增一个 adapter + 一个 verifier
> 并注册进 `webhookChannels()`，入口本身无需改动。

## 4. 出站形态：卡片与流式（阶段 45 新增卡片生产者）

| 形态 | 企业微信 | 飞书 |
| --- | --- | --- |
| 文本 | ✅ | ✅ |
| 交互卡片（按钮回调） | ❌ 降级为文本（正文已写明"回复批准/拒绝"） | ✅ `interactive` 卡片 + `behaviors.callback` |
| 流式原位更新 | ✅ 占位串 + 同 `stream.id` 原位替换 | ❌ 无原生流式，`SendStream` 收齐后一次发出 |

审批通知是卡片形态的唯一生产者：`worker.approvalNotice` 在
`channels.CardActionCapable(channel)` 为真时发 `KindCard`（标题 + markdown 正文 +
批准/拒绝按钮），否则发纯文本。按钮点击经
`CardActionToInbound` 变成"用户回复了 批准/拒绝"，与手工输入走同一条链路
（adapter 去重 → 总线幂等 → worker 审批决议 → 审计）。

> 注意：卡片需要 `Kind`/`Segments` 穿过 Redis Stream。这两个字段原先在
> `bus.encode/decode` 中被丢弃，导致卡片在 outbox 之后被静默降级为纯文本；
> 阶段 45 已修复并加回归测试（`TestMessageRoundTripPreservesCardAndMedia`）。

## 5. 入站附件：现在会真正交给模型（阶段 45 新增）

| 环节 | 实现 |
| --- | --- |
| 下载 | wecom：HTTP 下载 + `DecryptMedia`（AES-256-CBC）；feishu：`im/v1/messages/{id}/resources/{key}` |
| 边界 | 单个附件 8 MiB、15s 超时；超限/失败**不丢消息**，只在附件上记录 `FetchError` |
| 交给模型 | 图片 → `ContentParts` `AddImageData`（格式由字节嗅探，嗅不出则退化为文件 part）；文件/语音 → `AddFileData`；消息文本追加"附件清单"（含失败原因） |
| 归档 | 框架 `session/externalization` 在会话事件落库前把内联字节存进 MinIO 制品，事件里只留 `artifact://name@ver`（含 sha256/大小校验），读取时回填 |
| 回执 | 有附件读取失败时，worker 在开跑前向用户发一条幂等回执（outbox key `{msgID}:media`）说明哪个附件读不到、可以怎么办 |
| 审计 | 读取失败写 `audit_logs`（`decision=failed`, `error_type=attachment_unreadable`）；成功归档计入 `usage_records` 的 `artifact` 维度，制品引用本身（name/version/sha256/size）在会话事件里可溯源 |

为什么不把平台 URL 直接交给模型：企业微信的下载 URL 带 `AesKey` 且短期有效，飞书只给
`file_key`，外部模型服务都拿不到；因此统一"我们取回字节 + 内联给模型 + 归档到对象存储"，
模型看到的是内容而不是一个它永远打不开的链接。

## 6. 异步回复与失败重试（已有，防误读）

- 回复**异步**：worker 消费组 ↔ outbox ↔ outbound 流 ↔ gateway.Run 分发，与会话无锁耦合
  （`im.reply` 有独立 span）。
- 幂等与重试：入站按 `channel:platformMsgID` 去重（adapter 内存 `Seen` + worker
  Redis 两阶段幂等租约 + MySQL `idempotency_keys` 唯一键 + 事件级幂等复用）；
  **出站失败由 outbox 自己重试**（`pending` 按 2s→60s 指数退避，10 次后置 `dead` 并记
  `last_error`），不依赖 `XAUTOCLAIM`——`XAUTOCLAIM` 只负责认领**入站**流里被死消费者
  留下的 pending 消息。IM **发送**失败不会自动补发到平台（平台侧会话继续，用户可再问）。

## 7. 与容量评估的关系

限流阈值应≥业务峰值：IM 回调峰值 `msg/s` × 60 < `rate_limit.per_minute`。
估算方法见《容量评估指南》。
