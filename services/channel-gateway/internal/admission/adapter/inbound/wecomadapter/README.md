# WeCom Admission inbound Adapter

该 Adapter 把公开 `platform/im/wecom.Event` 转成 Admission 原生输入；它不是连接管理器、
执行器或回复器。Connection 在获得账户 owner lease 后创建 Handler；Admission 在首次
Commit 的同一 PostgreSQL 事务内重新校验 owner，而不是把建立连接视为永久授权。

```go
handler, err := wecomadapter.NewHandler(accountID, botID, admissiondomain.ConnectionFence{
    InstanceID: grant.InstanceID,
    Epoch: grant.Epoch,
    Revision: grant.Revision,
}, admissionService)
// 由 Connection lifecycle 使用 handler.Handle 作为协议回调。
```

- `single` 非空文本的 ConversationID 是 SenderID；`group` 是 ChatID。群 callback 作为已触发
  的协议输入处理，不附加“仅私聊”过滤。正文保留原始空白；纯空白文本记为 ignore。
- SDK notice 记为 interaction；unsupported 记为 ignore。它们的正文不进入 prompt；
  `disconnected_event` 只交由 Connection 处理，不写 Admission。非文本 notice/unsupported 可省略
  chattype；此时仅保留审计身份，ConversationID/ReplyContext 留空，不推测 single 地址。
- EventKey 固定为 `wecom + account_id + MessageID`，不包含 req_id、socket generation 或
  owner epoch。SourceDigest 的版本化结构包括可见语义字段与 SDK BodyDigest；后者覆盖
  严格解码的完整 callback body，包括未知字段，不包含 headers.req_id。数字原词法可能产生
  更保守的冲突判定；本实现不宣称所有 JSON 数字等价形式均合并。
- ReplyContext 只保存 `chat_type/chatid_or_userid/callback_req_id/received_at`。同一稳定事件的
  req_id 改变不改变 SourceDigest；已有 Receipt 仍重放，不覆盖首次持久 ReplyContext。
- 外部身份与 callback_req_id 采用最多 256 UTF-8 bytes 的本地边界，超限拒绝、不截断；
  文本上限 65,536 bytes。ReplyContext 还通过 schema-owned codec 验证。
- ConnectionFence 仅用于本次接纳授权，`json:"-"`；它不进入 SourceDigest、持久 input 或 NATS。
  Telegram 禁止提供该字段并绕过 Connection。企微新事件缺少 guard/失租时返回 ErrUnavailable，
  事务内预算、Inbox、Admission 与 Outbox 一起回滚；有效旧 Receipt 不重检当前 owner。
- Handle 返回 nil 表示 Admission 已提交或重放旧 Receipt，不是企微提供了持久消费 ACK。
  Adapter 不发送 Final，不跨连接重新解释旧回复上下文。

验证入口：`handler_test.go`；实际 PG 与 Connection row-lock 联合验证位于
`../../outbound/postgres/owner_guard_integration_test.go`。所有样本账户/消息均为合成 fixture。
