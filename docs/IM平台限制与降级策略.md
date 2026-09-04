# IM 平台限制与降级策略

> 状态：实现 + 文档（阶段 33，需求验收 B5）。
> 代码：`trpcservice/infra/channels/limit.go`（截断/限流契约）、`redis_ratelimit.go`
> （Redis 固定窗口）、`gateway.go`（gateway.Run 截断 + pumpInbound 限流）；
> 配置：`config.yaml rate_limit` 段（`infra/config/config.go`）。

## 1. 平台差异速查（代码与策略取值）

| 限制项 | 企业微信 wecom | 飞书 feishu | 平台策略 |
| --- | --- | --- | --- |
| 文本消息长度 | 2000 runes | 4000 runes | 出站按通道截断（limit.go `maxTextLen`） |
| 媒体类型 | image/file 识别（wecom.go:131） | image/file 识别（feishu.go:160） | 入站识别存在；回复仅文本，媒体消息**策略**见 §3 |
| 撤回事件 | MsgTypeEvent recall | 同 | 见 §3 |
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

## 3. 媒体消息与撤回（策略：识别即存在，默认不处理）

平台对入站 `MsgType` 的**识别**已实现（image/file/event，channels.go:18-23），
wecom/feishu 均把非文本入站归一化为带 `MsgType` 的 `InboundMessage`。

当前**处理策略**（有意最小化，代码不再扩展）：

| 场景 | 行为 | 理由 |
| --- | --- | --- |
| 入站图片/文件 | 不下载、不转写；若消息同时含文本则走文本，否则忽略 | 平台无多模态理解链路 |
| 撤回（recall） | 忽略（不入库），后续会话继续 | 无消息回收需求 |
| 出站媒体/卡片 | 未接线（KindCard/KindStream 已定义未启用） | 见《IM 联调手册》§4 待联调清单 |

如需“图片/文件自动忽略并回复提示”，在 gateway.pumpInbound 判 `in.MsgType`
后回一条提示消息即可（代码已具备全部接缝）。

## 4. 异步回复与失败重试（已有，防误读）

- 回复**异步**：worker 消费组 ↔ outbound 流 ↔ gateway.Run 分发，与会话无锁耦合
  （`im.reply` 有独立 span）。
- 幂等与重试：入站按 `channel:platformMsgID` 去重（gateway.go + adapter 内存去重 +
  worker Redis SetNX 幂等 + MySQL outbox 唯一键）；出站失败由 XAUTOCLAIM 重投
  （bus.go），但 IM 发送失败不自动补发（平台侧会话继续，人工可再问）。

## 5. 与容量评估的关系

限流阈值应≥业务峰值：IM 回调峰值 `msg/s` × 60 < `rate_limit.per_minute`。
估算方法见《容量评估指南》。
