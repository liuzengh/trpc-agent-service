# Phase 5 企业微信智能机器人长连接 Spike

> 核对日期：2026-09-03
> 官方来源：https://developer.work.weixin.qq.com/document/path/101463
> 官方页面最后更新：2026-05-25

## 选型结论

企业微信首版使用“工作台 -> 智能机器人 -> API 模式 -> 长连接”，不使用自建应用 callback、验签或消息解密。官方当前只提供 Node.js 与 Python 智能机器人 SDK，没有 Go SDK。`silenceper/wechat/v2` 面向传统微信/企业微信应用能力，未覆盖本阶段必需的智能机器人 WebSocket 命令、`req_id` 回复关联和踢下线事件，因此不进入生产依赖。

Go 实现选择官方协议薄客户端，复用 `golang.org/x/net/websocket`。该路线依赖更少，Endpoint 可替换，本地 Fake Server 可完整控制认证、心跳、回调、断开和回复，且 Go 1.21 编译、普通测试和 race 测试均可验证。

## 固定协议向量

生产 Endpoint：

```text
wss://openws.work.weixin.qq.com
```

订阅请求使用随机、不含凭据的 `headers.req_id` 关联响应：

```json
{
  "cmd": "aibot_subscribe",
  "headers": {"req_id": "subscribe-<opaque>"},
  "body": {"bot_id": "<resolved Bot ID>", "secret": "<resolved Bot Secret>"}
}
```

订阅响应无 `cmd` 字段：`headers.req_id` 透传请求值，顶层 `errcode=0` / `errmsg=ok` 表示成功（官方文档 path/101463「订阅请求」）。认证失败、超时和协议不匹配使 Adapter not ready，进程保持存活并有界退避重连。

> 修正备注（2026-09-04，DeepSeek 复核修正）：初版按 `cmd` + `body.errcode` 解析，属误读官方文档响应格式；真实企微 smoke 实测响应为顶层 `errcode`，与官方文档一致。代码已改为双格式兼容并补契约测试。

文本回调：

```json
{
  "cmd": "aibot_msg_callback",
  "headers": {"req_id": "<callback req_id>"},
  "body": {
    "msgid": "<stable message id>",
    "aibotid": "<bot id>",
    "chattype": "single | group",
    "chatid": "<group only>",
    "from": {"userid": "<actor>"},
    "text": {"content": "<text>"}
  }
}
```

`single` 以 `from.userid` 作为会话主体，`group` 以 `chatid` 作为会话主体，并保留真实 `actor_user_id`。群聊 Adapter 使用官方回调提供的 `text.content`；本阶段不猜测或用字符串规则删除未在字段向量中声明的 mention。

最终文本回复必须透传回调的 `headers.req_id`。当前长连接协议使用
`msgtype=stream` 表达文本；一次性回复生成稳定的 `stream.id` 并直接设置
`finish=true`：

```json
{
  "cmd": "aibot_respond_msg",
  "headers": {"req_id": "<callback req_id>"},
  "body": {
    "msgtype": "stream",
    "stream": {"id": "<stable stream id>", "content": "<final text>", "finish": true}
  }
}
```

企业微信会返回同一 `headers.req_id` 的顶层 `errcode`。Adapter 只有收到
`errcode=0` 才把出站标记为成功；写入 WebSocket 但被服务端拒绝或等待响应
超时都进入现有 Outbound 重试。

协议没有额外的业务 ACK 帧要求，也不发送人为设计的“收到啦”文本。可靠入队后由最终回复关联原 `req_id`。官方建议每 30 秒发送 `ping`。`aibot_event_callback` 仅在 `body.event.eventtype=disconnected_event` 时作为踢下线信号；客户端关闭旧连接并有界指数退避重连。

## 安全与生命周期

- Bot ID/Secret 只从 ChannelBinding 的 `env:` 引用解析，不进入 JSON、任务、日志或测试向量。
- Worker 不解析 IM 凭据；只有 Gateway 构造 Adapter。
- 握手和读取都可由 context 取消；连接失败、踢下线和服务端关闭都会清理旧 socket。
- Fake Server 覆盖延迟读、心跳、单聊、群聊、`req_id`、一次性文本回复、踢下线和重连。
- 同一 Bot 首版只允许一个 Gateway；多个 Gateway 的选主/租约属于后续演进。
