# 企业微信智能机器人文本适配器

本包包含企业微信智能机器人长连接协议与 `channels.TextAdapter` 的企微实现。持久消费者位于独立的 `channels/text` 包，将规范消息接入共享 Session Run，并回复一条最终文本。配置与社区扩展见 [IM 指南](../../../docs/im-channels.md)，真实正常单聊与当前集成测试范围见[实现与验证](../../../docs/acceptance.md#验证结果)。

## 范围

包含：

- WebSocket 帧编解码、`aibot_subscribe` 鉴权、心跳、指数退避重连、被接管即终止。
- 入站单聊文本的接受与拒绝规则，以及到平台标识（Principal、Session、Stream）的确定性派生。
- 一条 `aibot_respond_msg` 最终流式回复及其回执判定。
- 规范输入和版本化回复目标、20480 UTF-8 字节回复上限，以及发送结果的协议分类。
- 通过公共 `channels/text` 消费者复用 PostgreSQL Inbox 去重、按会话顺序执行真实 Runner、最终文本与 Outbox 原子落库，以及按 Tenant/Binding 限定的恢复扫描；本包不依赖执行服务或数据库实现。
- `cmd/trpc-service` 的可选启动接线，默认禁用。

不包含（本包不实现，也不声称具备）：

- 平台精确一次投递、跨连接重放、完整答案重建、多消费者调度。
- 群聊、图片、混合消息、欢迎语、模板卡片、主动发送、素材上传。这些消息类型会被拒绝而不是部分解析。

## 使用

以下为 Client 协议 API；服务进程使用后文的持久消费者接线。

```go
client, err := wecom.New(wecom.Config{
    Binding: wecom.Binding{
        TenantID:   "tenant-a",
        AgentAppID: "app-a",
        BindingID:  "binding-a",
        BotID:      "<bot id>",
        SecretRef:  "env:WECOM_BOT_SECRET",
    },
    Authorizer: entitlements, // security.SecretRefAuthorizer
})
if err != nil {
    return err
}
go func() {
    // Messages() 有界且永不关闭，退出条件是 ctx，不是 channel 关闭。
    for {
        select {
        case <-ctx.Done():
            return
        case msg := <-client.Messages():
            // 生成回复后在同一连接上发送；target 只在收到该消息的连接上有效。
            _ = client.SendFinalText(ctx, msg.Reply, answer)
        }
    }
}()
err = client.Run(ctx) // 阻塞直到 ctx 取消或到达终止状态
```

`Run` 总是返回非 nil error：正常关闭返回 `context.Canceled`；`ErrAuthRejected`、`ErrTakenOver`、`ErrInboundOverflow`、`ErrReconnectExhausted` 为终止原因，不再重连。拨号失败、读错误、心跳丢失、回复结果未知属于普通故障，按退避重连。

`SendFinalText` 的成功仅指收到 `errcode` 显式为 0 的回执。`ErrReplyRejected` 是平台明确拒绝，`ErrReplyOutcomeUnknown` 是已写出但无回执，两者不同，且本包都不重试。

## 持久消费者

企微 `Adapter.Serve` 按到达顺序把 `Messages()` 转换为规范消息，通过公共消费者的持久受理回调写入 `channels.Store`。公共消费者的任务循环逐条认领、执行、发送。持久任务以 Store 为准，Client 仍有有界接收缓冲。

```go
adapter, err := wecom.NewAdapter(client)
if err != nil {
    return err
}
consumer, err := channeltext.New(channeltext.Config{
    Identity: channels.BindingIdentity{
        TenantID: binding.TenantID, AgentAppID: binding.AgentAppID,
        BindingID: binding.BindingID, Channel: channels.ChannelWeCom,
    }, // 服务端预期身份，必须与适配器的身份匹配
    Adapter:   adapter,
    Store:     store,   // channels.Store
    Runs:      runs,    // *sessionrun.Service，与网页聊天共用
    Revisions: check,   // 每次执行前复核 revision
})
```

`channeltext` 对应 `trpcservice/channels/text`。`cmd` 同时运行 `client.Run` 和 `consumer.Run`，任一终止时取消另一个并等待退出，再关闭共享资源。详细扩展契约见[社区接入指南](../../../docs/im-channels.md#社区接入契约)。

进程接线由 `cmd/trpc-service` 负责。按项目 README 配好 PostgreSQL profile、DSN、schema 和服务凭据后，设置 `TRPC_SERVICE_WECOM_ENABLED=true`，并提供 `TRPC_SERVICE_WECOM_TENANT_ID`、`TRPC_SERVICE_WECOM_APP_ID`、`TRPC_SERVICE_WECOM_BINDING_ID`、`TRPC_SERVICE_WECOM_BOT_ID`。Tenant/App 必须已存在且有已发布 Revision；该 Revision 使用默认 PostgreSQL Session，不能指定独立 BackendProfile。

Bot Secret 只从固定的 `TRPC_SERVICE_WECOM_BOT_SECRET` 读取，由启动代码按精确租户和引用放行，不进入租户 Entitlement。密钥由进程环境提供，不写入聊天、日志或版本库。

最多发送一次最终回复；拒绝、连接目标过期和回执未知都不会重跑 Agent。未知结果记为 `duplicate_risk`，不自动重发。恢复时未启动任务可继续；已启动的未知任务明确失败。连接重建使旧目标失效，不承诺跨重启最终送达。读取消息到落库前仍有丢失窗口，详见[恢复边界](../../../docs/im-channels.md#恢复与平台限制)。

## 凭据与日志

`Binding` 只携带 `SecretRef`，不携带 Secret。`New` 先对精确引用做租户授权，再解析环境变量；Secret 只出现在 `aibot_subscribe` 请求体中。

本包没有任何日志语句，错误文本不包含 Secret、Bot ID、外部用户或消息 ID、消息体，也不透传平台 `errmsg`（该字段根本不解码）。连接地址是常量，不提供 endpoint 配置项；测试通过包内不可导出的 dial 缝隙指向本地 mock 服务器。

## 身份派生

按架构文档 §5.4 的规则派生，租户、App 和 Binding 全部来自静态 `Binding`，帧内容不参与路由：

- `PrincipalID = "p-" + H(["im-principal-v1", tenant, binding, "user", 外部 userid])`
- `SessionID = "d-" + H(["im-session-v1", tenant, app, binding, "direct", principal, "", "0"])`
- `StreamID = "s-" + H(["im-stream-v1", tenant, binding, msgid])`

`H` 是 JSON 字符串数组的 SHA-256 十六进制摘要，字段边界不可移动。结果为 66 个 ASCII 字符，满足 `tenant.ValidateResourceID` 和 SessionID 67 字符上限。

## 测试

默认测试使用本地 mock WebSocket，不连接真实机器人或外部模型：

```
go test -race -count=1 -timeout 120s ./trpcservice/channels/wecom
```

端到端用例另需本地 PostgreSQL，默认跳过。它跑真实协议帧、真实 Runner（确定性 echo 模型），以及持久 Channel Store、Session、Pin 和配置仓库：

```
TRPC_SERVICE_SESSION_INTEGRATION=1 \
TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
go test -race -count=1 -timeout 300s ./trpcservice/channels/wecom -run TestIntegration
```
