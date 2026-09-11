# 企微公开 Go 协议库 P0：官方协议核验与实施边界

- **核验日期**：2026-09-05。
- **固定参照**：官方 Node SDK 提交 `80615b987ef69c6028ad764924609247c0725955`；不以浮动 main 作为验收基线。
- **本次工作**：只读官方长连接文档、固定 SDK 源码和 WebSocket 库发布元数据；未连接真实 Bot、未读取凭据、未执行真实账号实验。
- **标记约定**：`[官方协议]` 是服务端文档事实；`[SDK事实]` 是固定版本的实现行为；`[Go策略]` 是我们选择的本地约束；`[待实测]` 是尚待验证的外部行为。
- 本文是 P0 实施依据，不替代[公开 Connector 设计](public-go-connector.md)和[Module 边界](module-boundaries.md)，也不声明完整 Gateway/Connection/Delivery 已交付。

## 1. 版本选择和核验方式

**P0 固定 `github.com/coder/websocket v1.8.15`。** 本次官方 GitHub `releases/latest` 重定向到该稳定 Release；其 `go.mod` 声明模块路径和 `go 1.23`。这是本次查得的版本，不承诺未来仍为最新；依赖修改、构建和 race 验收由实现任务完成。[官方 Release](https://github.com/coder/websocket/releases/tag/v1.8.15)、[版本 go.mod](https://github.com/coder/websocket/blob/v1.8.15/go.mod)

`[Go策略]` WebSocket 库只负责传输；企微 JSON `ping`、订阅、ACK 相关、回调 DTO 和替换断线仍由 `platform/im/wecom` 实现。库直接编译进 Gateway，不增加 Node 镜像、子进程或 Connector RPC。

官方长连接页面本次以公开 HTTP GET 取得 `200`，已读取认证、回调、回复、心跳与连接数量章节。网页提取工具最初报错后，使用只读 HTTP 获取 HTML；没有把搜索摘要或社区转载当作官方正文。[官方智能机器人长连接](https://developer.work.weixin.qq.com/document/path/101463)

## 2. P0 原生协议形状

以下示例全部使用人工构造的占位值；它们是协议 fixture，不是真实账号记录。

### 2.1 WebSocket 与 subscribe

`[官方协议]` 公有服务地址为 `wss://openws.work.weixin.qq.com`。WebSocket 握手后还需订阅；订阅使用 BotID 与长连接专用 Secret，响应通过原 `req_id` 相关。订阅成功后反复订阅可能触发频率保护。[连接与订阅](https://developer.work.weixin.qq.com/document/path/101463)

```json
{
  "cmd": "aibot_subscribe",
  "headers": {"req_id": "subscribe-fixture-1"},
  "body": {"bot_id": "BOT_ID", "secret": "SECRET_PLACEHOLDER"}
}
```

```json
{"headers":{"req_id":"subscribe-fixture-1"},"errcode":0,"errmsg":"ok"}
```

`[SDK事实]` `connected` 与 `authenticated` 分开；收到成功订阅 ACK 后才启动应用层心跳。`[Go策略]` 握手成功不开放 Reply；认证必须匹配本连接当前完整 req_id，且 `errcode` 字段存在、为整数 0。缺失 errcode 不是 Go 零值成功。[SDK 连接状态](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/client.ts#L89-L118)、[SDK 认证处理](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/ws.ts#L249-L280)

### 2.2 应用层 ping 与 ACK

```json
{"cmd":"ping","headers":{"req_id":"heartbeat-fixture-1"}}
```

```json
{"headers":{"req_id":"heartbeat-fixture-1"},"errcode":0,"errmsg":"ok"}
```

`[官方协议]` 建议每 30 秒发送应用层心跳；长期未收到心跳会断连，需要实现断线检测与重连。这里的 ACK 不是 RFC 6455 控制帧 pong。[心跳协议](https://developer.work.weixin.qq.com/document/path/101463)

`[SDK事实]` 固定实现连续丢失两次应用层 ACK 后，在下一次心跳检查关闭连接；它还独立处理 WebSocket ping/pong。`[Go策略]` 单个未知或旧 heartbeat req_id 不刷新存活计时；本地超时值不是服务端 SLA。[SDK 心跳](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/ws.ts#L292-L340)

### 2.3 text callback

```json
{
  "cmd":"aibot_msg_callback",
  "headers":{"req_id":"callback-fixture-1"},
  "body":{
    "msgid":"message-fixture-1", "aibotid":"BOT_ID",
    "chatid":"group-fixture-1", "chattype":"group",
    "from":{"userid":"user-fixture-1"},
    "msgtype":"text", "text":{"content":"你好"}
  }
}
```

`[SDK事实]` 原生消息体包含 msgid、aibotid、chattype、from.userid；群聊含 chatid，文本位于 text.content。SDK 的 create_time、response_url、quote 是可选字段。消息身份 msgid 与回复相关 req_id 不能混为一个字段。[消息类型定义](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/types/message.ts#L91-L119)

`[Go策略]` 公共库保留原生 DTO 与必要关联上下文；可信账户与 aibotid 一致性由调用方配置和 Adapter 联合验证。去重身份、Tenant、Admission 与 Run 不进入公共协议库。未知类型不能伪装成 text；原始 msgtype 交给有类型的 unsupported 分支，不偷偷执行 Agent。

### 2.4 event callback 与 replaced 特例

```json
{
  "cmd":"aibot_event_callback",
  "headers":{"req_id":"event-fixture-1"},
  "body":{
    "msgid":"event-message-fixture-1", "create_time":1700000000,
    "aibotid":"BOT_ID", "from":{"userid":"user-fixture-1"},
    "msgtype":"event", "event":{"eventtype":"enter_chat"}
  }
}
```

`[SDK事实]` eventtype 在 `body.event.eventtype`，不是 body.event_type；事件集合包括 enter_chat、template_card_event、feedback_event、disconnected_event。`types/api.ts` 的概述注释出现 event_type，具体 EventMessage 和 dispatcher 使用嵌套 event.eventtype；实现按具体结构与官方示例，而不是概述注释猜字段。[事件 DTO](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/types/event.ts#L14-L80)、[分派实现](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/message-handler.ts#L84-L100)

```json
{
  "cmd":"aibot_event_callback",
  "headers":{"req_id":"replaced-fixture-1"},
  "body":{
    "msgid":"replaced-message-fixture-1", "create_time":1700000000,
    "aibotid":"BOT_ID", "msgtype":"event",
    "event":{"eventtype":"disconnected_event"}
  }
}
```

`[官方协议]` 同一 Bot 新连接订阅完成后会替换旧连接；上述断开事件示例不包含 from/chattype/chatid。`[SDK事实]` TypeScript EventMessage 却把 from 设为必填。`[Go策略]` disconnected_event 应按专用控制事件校验，避免因没有 from 而漏掉替换通知。[官方连接断开事件](https://developer.work.weixin.qq.com/document/path/101463)、[SDK EventMessage](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/types/event.ts#L54-L80)

`[SDK事实]` replaced 路径停止心跳、清理 pending 并阻止自动重连。`[Go策略]` 先关闭本地新调用/重连门，再有界交付通知，不等待慢 handler 才执行控制状态迁移；后续重新取得运行资格是 Connection 的责任。[SDK replaced 分支](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/ws.ts#L222-L246)

### 2.5 P0 文字 Final

```json
{
  "cmd":"aibot_respond_msg",
  "headers":{"req_id":"callback-fixture-1"},
  "body":{
    "msgtype":"stream",
    "stream":{"id":"stream-fixture-1","content":"处理完成","finish":true}
  }
}
```

`[SDK事实]` replyStream 将 id/content/finish 放入 stream，reply 透传回调 headers.req_id。P0 只暴露单次终结文字回复，使用上述 stream envelope，不因方法叫 ReplyText/Final 就改成普通 msgtype:text。类型注释给出 content 最长 20480 个 UTF-8 字节。[Client replyStream](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/client.ts#L158-L198)、[StreamReplyBody](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/types/api.ts#L81-L97)

`[官方协议]` 同一次回调的流式回复复用原 req_id；stream.id 标识流式消息；finish=true 后结束更新。文档另规定首帧起 10 分钟的流生命周期与回调后 24 小时的回复窗口；这不是 ACK 等待期限。[流式回复规则](https://developer.work.weixin.qq.com/document/path/101463)

`[Go策略]` P0 不暴露 Progress 刷新、卡片、主动发送或媒体；Final 后不开放同回调第二次发送。协议发送成功回执只表示 Provider 明确接受，不代表用户已读或 Delivery 已持久提交。边界长度按 UTF-8 字节验证，保留合法 JSON 转义与文本内容。

## 3. req_id 与结果确定性：不可照搬 SDK 的宽松匹配

`[SDK事实]` 认证/心跳按 req_id 前缀分类；回复按完整 req_id 查 pending。回复队列同 req_id 串行，ACK 超时为 5 秒；超时后队列推进。内部 seq 只保护旧超时回调，没有被发送给服务端。[SDK ACK 与队列](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/ws.ts#L408-L539)

`[静态推导]` 若同 req_id 的 A 超时后开始 B，迟到的 A ACK 只有 req_id，可能被当作 B 的回执；这不是本轮复现的线上事故。增加本地 attempt_id 并不会让服务端回传该字段。

`[Go策略]` P0 固定以下约束，而不是声称官方提供 exactly-once：

1. 先按 cmd 区分 callback 与 ACK；callback 即使带相同 req_id，也不得完成 pending。
2. 认证、心跳、回复分别关联本连接 generation 和完整 req_id；没有对应 pending 的帧不改变成功状态。
3. pending 在真正写出前可被 reader 观察，防止极快 ACK 先于登记；同 req_id 同时只有一个外部调用。
4. 发送前参数错误、未认证或本地发送门拒绝属于 NOT_SENT；进入底层写入后发生超时/断线，保守归为 UNKNOWN。
5. 匹配且格式有效的非零 errcode 是 REJECTED，零是 ACCEPTED；格式错误或缺失 errcode 不推断接受。
6. ACK 超时后暂停该关联，不推进同 req_id 的下一项；P0 Final 终结后也不复用关联，重复 ACK 无新 pending 可匹配。
7. 旧连接 pending 在关闭时完成；旧 generation 的回调/ACK 不进入新连接。库不自动重发 UNKNOWN Final。
8. handler、状态通知和消息队列有界且与 reader 分离；慢 handler 不阻塞 ACK 与心跳，饱和策略显式测试。

`[SDK事实]` MessageHandler 先 emit 通用消息，再 emit 类型消息；没有调用持久接纳 ACK。`[Go策略]` 一个输入只交接一次；handler 成功仅为本地处理结果，不创造服务端离线补发或持久消费确认保证。[官方 SDK 消息派发](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/message-handler.ts#L30-L100)

## 4. 重连不是重新执行业务

`[SDK事实]` 默认网络重连上限 10 次、认证失败上限 5 次，基础延迟 1 秒，指数退避上限 30 秒；认证成功才重置计数，manual close 与 replaced 不自动重连。这些是固定 SDK 策略，不是必须照抄的服务端参数。[SDK 配置](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/types/config.ts#L16-L31)、[SDK 重连](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/ws.ts#L342-L392)

`[Go策略]` P0 可采用有限退避并测试取消；每次重连新建连接 generation、重新认证，再开放发送。Root context 取消、显式 Close、replaced 结束该次 Run 的重连资格。公共库不读 PG lease；Gateway Connection 在 owner 有效期间运行 Client，失租时取消它。正常重连不自动重放回调，也不重新发送已进入 UNKNOWN 的 Final。

`[待实测]` 新 socket 是否能继续使用旧 req_id/stream.id、断线期间是否丢失或重投 callback、同 Bot 抢占到旧连接停止的时窗、服务端限流与认证失败的真实错误码均未在真实账号验证。不能从 SDK 自动重连推导这些保证。

## 5. P0 验收矩阵

以下是待执行或由实现任务另行记录结果的验证设计；本协议核验本身没有运行这些实验。

| 类型 | 本地可控服务端输入 / 故障 | 必须断言 |
| --- | --- | --- |
| subscribe | 正确 ACK、非零错误、缺 errcode、错误完整 ID、仅前缀相同 ID | 只有当前完整关联成功才 authenticated；错误与超时有界 |
| heartbeat | 匹配 ACK、未知 ID、迟到 ACK、只回 RFC pong、持续无 ACK | 应用层存活依据正确；超时后有限重连 |
| callbacks | text、enter_chat、缺 from 的 disconnected_event、未知 msgtype/eventtype | 保留原生字段；控制事件不被普通用户消息校验丢弃 |
| Final wire | 单次文字回复、多字节边界、特殊字符 | 原回调 req_id；msgtype:stream；finish:true；不含 P1 字段 |
| ACK identity | A/B 不同 req_id 乱序、callback 伪装 ACK、重复/未知/旧 generation ACK | 不错配、不重复完成、不跨 generation |
| UNKNOWN | write 后无 ACK、断线、ACK 超时后旧 ACK 到达 | 不盲重发、不推进同关联下一 Final |
| concurrency | handler 内 Reply、慢 handler、队列满、Close 与 ACK 竞争 | reader 不死锁；内存与 goroutine 有界；pending 完成一次 |
| reconnect | 握手后订阅失败、连续网络失败、取消退避、replaced | 认证/网络策略分离；取消后不重连；replaced 不互踢循环 |
| 真实账号 | 私聊/群 @、Final、双连接替换、网络中断后恢复 | 单独记录真实输入与回执、消息可见状态和未确定结果 |

本地契约与 race 通过只证明 Go 库的受控行为；真实账号结果另录，不把合成服务端当作企微线上。媒体与 P1 完整实现不在本次核验交付内。
