# 企微智能机器人通道（wecomws）

企业微信「智能机器人」通道：平台**主动向企微 WS 网关
（`wss://openws.work.weixin.qq.com`）建立长连接**，消息经连接双向收发——
不需要公网回调地址，适合内网 / 无域名部署。实现位于
`trpcservice/channels/wecomws/`（`wecomws.go` / `conn.go` / `protocol.go` / `manager.go`）。

> **实测状态（2026-09-07）**：三个通道中唯一在真实机器人上端到端实测通过的
> ——订阅、心跳、收发、审批全链路验证。实测踩出的协议细节都写进了 §4/§5。

## 1. 适用场景与取舍

| 适合 | 不适合 |
|---|---|
| 无公网入口（内网、无域名）：平台主动出站，企微不回调 | 需要「随时主动触达」——回复只能应答回调（须透传 `req_id`），一期不做 `send_msg` 主动推送 |
| 快速接入：后台创建一个机器人即可，无需应用回调配置 | 需要富媒体出站 / 模板卡片 / 欢迎语 —— 一期明确不做 |
| 媒体消息只要求「能应答」：入站媒体降级为占位文本 | 入站媒体内容要进 LLM —— 一期不做下载解密 |

与 webhook 通道（[wecom](./wecom.md) / [wxkf](./wxkf.md)）的根本差异：

| 维度 | webhook 通道 | wecomws |
|---|---|---|
| 连接方向 | 企微回调平台（入站 HTTP，需公网 HTTPS） | 平台主动出站长连接 |
| 凭据 | corpid + corpsecret 换 access_token | 每 bot 一对 BotID/Secret（subscribe 帧鉴权） |
| 回复方式 | 主动发送 API（message/send、kf/send_msg） | `aibot_respond_msg` 帧，必须带回调的 `headers.req_id` |
| 平台重推 | 有（5xx 会重推） | **没有**——入站失败只能本地重试 |
| 连接数 | 无限制 | **每 bot 同时只允许一条连接，新连接踢旧连接** |

## 2. 企微后台配置

企业微信管理后台 →「安全与管理」→「管理工具」→「智能机器人」（或群聊里直接添加智能机器人），
创建后拿到 **BotID** 和 **Secret**。后台**不需要**填回调地址——连接由平台侧发起。

## 3. 密钥与绑定配置

BotID/Secret **不放 env**，放 `channel_binding.config`（secret 只存引用名）：

```bash
# ① 密钥文件（文件名 = 下面 config 里的 secret_ref）
echo -n '你的BotSecret' > data/secrets/wecomws-bot-secret
chmod 600 data/secrets/wecomws-bot-secret

# ② 启用通道
TRPC_WECOMWS_ADDR=wss://openws.work.weixin.qq.com ./start.sh

# ③ 建绑定（channel=wecomws 的 webhook_path 不自动填充，必须手工给，且必须等于 /wecomws/{bot_id}）
curl -s -X POST $A/admin/apps/$APP/bindings -H "$H" -H 'Content-Type: application/json' \
  -d '{"channel":"wecomws",
       "webhook_path":"/wecomws/你的botid",
       "config":{"bot_id":"你的botid","secret_ref":"wecomws-bot-secret"}}'
```

Admin 侧强校验（`web/admin.go` `validateWecomwsBinding`），不合规直接 400：

- `config` 必须有非空 `bot_id` + `secret_ref`，未知字段拒绝（`DisallowUnknownFields`）；
- `webhook_path` 必须匹配 `^/wecomws/[A-Za-z0-9_-]+$`，且路径后缀**必须等于** `config.bot_id`
  ——WS 入站没有平台重推，路径写错就是静默路由黑洞，所以校验前置到写入口；
- `token_ref` / `aeskey_ref` 必须留空（WS 通道按 bot 鉴权，没有回调验签密钥）。

| env | 默认 | 说明 |
|---|---|---|
| `TRPC_WECOMWS_ADDR` | `""`（禁用） | 设为 `wss://openws.work.weixin.qq.com` 即启用；必须 wss，否则拒绝挂载 |
| `TRPC_WECOMWS_PING_INTERVAL` | `30s` | 应用层心跳间隔 |
| `TRPC_WECOMWS_LEADER_TTL` | `15s` | leader 租约 TTL（每 TTL/3 续期） |
| `TRPC_WECOMWS_RESYNC_INTERVAL` | `15s` | 绑定对账周期：新增起连、删除/禁用/改配置停连重连 |
| `TRPC_WECOMWS_SEGMENT_BYTES` | `2048` | 单帧回复分条上限 |

## 4. 平台硬性约束与协议细节（来源：企微官方文档 + 实测）

- **每 bot 单连接，新连接踢旧连接**（旧端收 `disconnected_event`）。所以多副本部署时由
  Redis 上的 `lock:leader:wecomws` 租约（`storage.LeaderLock`：SetNX 竞选 + TTL/3 续期 +
  Lua owner 校验释放）选出全局单 leader，leader 持有全部 bot 连接并运行专用出站消费组
  `senders-ws`；其余副本空转待命，失锁立即关全部连接。**多副本里看到某些副本「没动静」
  是正常的。**
- **心跳必须 30s 左右一发，且 `req_id` 必须带 `ping_` 前缀**（`ping_{毫秒}_{随机}`）。
  实测：不带前缀的心跳平台**完全不应答**（连错误都不回），连接会被自己的「漏 pong」
  看门狗杀掉重连。
- **回复必须是 `msgtype=stream`**：`aibot_respond_msg` 发纯 `text` 会被平台以
  **errcode 40008**（invalid message type）拒收。所以一条回复按 2048 字节切成若干
  stream 帧串行下发，共享同一 `stream.id`，末帧 `finish=true`（平台侧 markdown 单条上限
  20480 字节，分条后体验与 webhook 通道一致）。
- **回复必须透传回调帧的 `headers.req_id`**，且只能在收到该回调的那条连接上回。
  实现上把连接纪元编进 `ReplyToken`（`<epoch>:<req_id>`）：重连后旧 req_id 必然不可达，
  平台会静默丢弃——所以检测到跨连接回复直接报错留 pending 重试，而不是发出一个
  「写成功但用户永远看不到」的帧。
- **ack 帧没有 `cmd` 字段**：订阅应答只有 `headers.req_id` + 顶层 `errcode`/`errmsg`，
  按 req_id 对账；WS 握手头（`Sec-WebSocket-Key` 等）平台**大小写敏感**，Go 的
  `net/http` 规范化写法会被回 404——通道内置了 `RoundTripper` 改回标准大小写
  （`wecomws.go` `standardCaseTransport`）。
- **入站无平台重推**：`Handle` 失败只能本地退避重试（1s 倍增、封顶 30s），**最多 5 次**
  （`inboundRetryMax`）——派发协程是串行的，无界重试会让一条毒消息劫持该连接上所有后续
  消息。超限后**大声丢弃**（error 日志带 bot/msgid/次数），用户可重发。重试耗尽或重试
  期间进程崩溃会丢这一条消息，是一期接受的语义边界（二期方向：失败消息落 Redis list 重放）。
- **每会话限频 30 条/分钟**：单连接串行发送 + 现有 `{channel, tenant}` 令牌桶兜底；
  高频租户用 `rate_policy.send_qps` 收紧。
- 媒体消息（image/voice/video/file/mixed）降级为 `[图片]（暂不支持媒体消息）` 占位文本；
  `enter_chat` / `template_card_event` 事件记日志跳过。

## 5. 常见错误码与处理

| errcode / 现象 | 含义 | 处理 |
|---|---|---|
| subscribe 应答 `errcode≠0` | BotID/Secret 错 | 退避封顶 5 分钟防自旋；核对 `data/secrets/` 里 secret_ref 对应的文件 |
| 40008 | 回复用了 text 类型 | 代码已固定用 stream 类型；见到说明平台协议变了，需要改 `respondFrame` |
| `stale reply token` | 回调所属连接已被替换（重连/换 leader），req_id 不可达 | 自动留 pending 由 `senders-ws` 重试；超窗（req_id 约 24h）进死信 |
| `no live connection for binding` | 该绑定无在线连接（绑定被删/禁用，或本副本不是 leader） | 自动重试至死信；查绑定状态与 leader 日志 |
| `disconnected by platform` | 被新连接踢线 | 预期内的 leader 交接：关连接、5s 起退避重连；**反复出现**说明有两个地方在持同一 bot 的连接（查是否有第二个部署用了同一 BotID） |

## 6. 故障排查入口

```bash
grep -a 'wecomws' data/trpc-service.log | tail -50
```

| 日志关键字 | 含义 |
|---|---|
| `wecomws channel enabled` / `wecomws channel disabled` | 通道挂载结果（缺路由或地址不是 wss 会禁用并打 WARN） |
| `wecomws leadership acquired (owner ...)` / `wecomws leadership lost` | leader 竞选结果，多副本时确认只有一个 owner |
| `wecomws: connection started (bot ..., binding ...)` | 绑定对账后新起连接 |
| `wecomws: subscribe rejected (errcode N)` | 凭据错误，见 §5 |
| `wecomws bot ... disconnected, reconnecting in ~Xs` | 断线重连（带原因） |
| `wecomws bot ...: heartbeat unanswered for 30s` | 漏 pong 杀连接重连；频繁出现查网络/平台状态 |
| `wecomws bot ...: dropping msg ... after 5 failed attempts` | 入站重试耗尽主动丢弃（语义边界，见 §4） |
| `wecomws: skipping binding ...` | 绑定 config 解析失败，单个坏绑定不影响其他连接 |

指标：`stream_pending{group="senders-ws"}`（该组积压 = leader 失联或 WS 发送卡死，
对应告警 `SendersWsBacklog`）、`im_inbound_total{channel="wecomws"}`、
`im_outbound_total{channel="wecomws",result}`。
排查动作详见 [运维手册 §5](../operations.md#5-故障排查-runbook)。
