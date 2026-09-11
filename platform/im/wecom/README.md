# WeCom intelligent-bot protocol client（P0）

`github.com/liuzengh/trpc-agent-service/platform/im/wecom` 是顶层可导入的纯 Go
协议库，依赖 `github.com/coder/websocket v1.8.15`。它不是独立 Workload、镜像、
Node 服务或新的 Go module；本轮 Gateway 已直接 import 并在自己的进程中运行，实际工作树与镜像已验收。

## 当前实现与边界

本轮实现了本地可编译的 P0 客户端，以及实际 TCP/WebSocket `httptest` 契约、故障和
race 测试：subscribe/auth、应用 `ping`/ACK、原生文本与事件、文字 Final、有限重连、
状态通知、取消和有界关闭。测试只使用合成 BotID/Secret 与本机服务端。

Gateway 已在库外装配 Connection Supervisor、owner lease/epoch 与 Admission；
实际工作树应用和联合镜像验收已完成，证据见[实施状态 §9.5](../../../docs/architecture-next/channel-gateway/implementation-status.md#95-实际工作树联合验收)，当前代码已有 Delivery adapter；真实企微投递仍需单独验收。库自身不包含 PG/NATS、Tenant/Binding、
平台 SecretRef 读取、持久 Inbox、业务重试或跨进程限流。真实企微账号的私聊、群 @、Final、抢占与断线窗口验收仍是后续工作；
本地协议测试不是完整 Agent E2E，也不是第三方服务端行为的线上认证。

P1 的多次 stream/progress、欢迎语、卡片、主动推送与 P2 的媒体 API 均未实现。
P0 的一次文字 Final 使用协议的 `msgtype: "stream"`、`finish: true`，不代表已经支持
多次流式更新。协议证据与 SDK 差异见
[协议核验说明](../../../docs/architecture-next/channel-gateway/wecom-protocol-implementation-notes.md)。

## 接入认证探测（2026-09-07）

新增 `ProbeAuthentication(ctx, config, opts...)`：在调用者显式确认后，建立一次真实
订阅、等待精确匹配的认证 ACK 并释放连接。最多 10 秒、无自动重连、不发送业务回复。
它不是 Telegram getMe 式只读检查，可能替换同 Bot 的其他连接。

`AuthenticationError` 保留确定性和稳定代码，并继续支持 `errors.Is(err, ErrAuth)`；
明确拒绝与 ACK 超时分别报告，后者不当作 Secret 无效。返回结果不携带 Secret、远端
错误原文或用户数据。认证成功不证明收到消息或 Agent 回复。

Gateway 和本机 `services/channel-gateway/cmd/wecom-smoke` 共用此探测；产品三侧契约见
[WeCom 预检 V1](../../../docs/architecture-next/channel-gateway/wecom-preflight-v1.md)。

## 公开 API

```go
func NewClient(config Config, opts ...Option) (*Client, error)
func (c *Client) Run(ctx context.Context, handler Handler) error
func (c *Client) Reply(ctx context.Context, request ReplyRequest) (CommandAck, error)
func (c *Client) Close(ctx context.Context) error
func (c *Client) State() StateSnapshot
func (c *Client) States() <-chan StateSnapshot

type Handler func(context.Context, Event) error

// WithHTTPClient 注入调用方拥有的 HTTP transport（例如代理或测试服务端）。
func WithHTTPClient(client *http.Client) Option
```

- `NewClient` 只检查本地配置，不拨号、不读环境、不启动 goroutine。
- 一个 Client **只允许一次 Run 生命周期**，重连发生在该 Run 内部。正在运行或已完成
  后再次调用 Run 返回 `ErrAlreadyRun`；显式 Close 后调用 Run 返回 `ErrClosed`。
- Run 的 context 控制整个生命周期；handler 获得当前 socket generation 的 context。
  当前连接取消后，旧回调的 context 也取消，旧队列项不再交给新连接执行。
- reader 与一个顺序 dispatcher 独立运行。handler 可以同步调用 Reply 并等待 ACK，
  reader/心跳继续运行；不为每个事件创建无限 goroutine。
- handler 应响应 context 取消。活动连接上的 handler 错误会终止 Run，外部仅收到固定
  `ErrHandler`，不泄露 handler 原始错误。旧连接已取消的 handler 返回错误不会终止
  新 generation 的 dispatcher。
- handler 返回 nil 只表示**本地处理函数完成**；协议没有因此新增“平台已持久接纳”的
  远端消费确认。库不保证服务端重放掉线时的 callback。
- 关闭操作由拥有方在 handler 之外执行；handler 内同步 Close 会等待自身退出，受到
  CloseTimeout 限制。需要从 handler 发起停止时，取消拥有方的 Run context。

### Event 与 Reply

`Event` 只携带规范化的协议字段：Kind、RequestID、Generation、MessageID、BotID、
SenderID、ChatID、ChatType、Text、EventType、BodyDigest；不透传任意 SDK/raw JSON。
BodyDigest 为完整已解码 callback body 的稳定 SHA-256 摘要，包含未知 body 字段；
解码拒绝重复 JSON key，并保留数字精度。外层 callback req_id 与本地 Generation 不进入 body
摘要。Gateway 另加语义版本、可信 Bot 和规范化字段形成 SourceDigest，库不决定平台 EventKey。

- 普通文本为 `EventText`；支持 `single` 与有 `chatid` 的 `group`。
- `enter_chat`、`template_card_event`、`feedback_event` 为 `EventNotice`。
- 未支持的消息/事件为 `EventUnsupported`，Text 为空；EventType 保留原类型，不把图片、
  卡片或未知事件转成用户 prompt。库不负责判断群触发条件或生成 Run。
- `disconnected_event` 允许没有 sender/chat 字段。客户端先安装 `REPLACED` 终态原因、
  拒绝新调用，再把控制通知交给 dispatcher，并关闭连接，**不自动重连抢回**。
- 非法 JSON、重复字段、缺少必要身份字段、错误 BotID、超 ReadLimit、二进制业务帧等
  终止当前 Run 为 `ErrProtocol`；没有原始 body/error 回显。

```go
type ReplyRequest struct {
    RequestID  string // 保留 callback 原 req_id；不生成新的业务关联 ID
    Generation uint64 // 保留 Event.Generation；不是平台 owner epoch
    StreamID   string // 协议 stream.id
    Content    string // 一次完整 Final 文本
}
```

Reply 只发送一个 `aibot_respond_msg` Final。RequestID/StreamID 为非空、无 ASCII 控制/空白
字符且最多 1,024 UTF-8 bytes；Content 为非空有效 UTF-8，当前库上限 20,480 bytes，与固定官方 Node SDK 的 `StreamReplyBody.content`
类型注释对齐，真实账号边界仍待验收。
这些是本地输入边界；第三方平台仍可在 ACK 中拒绝具体内容。RequestID 不负责平台
授权，调用方仍须从已接纳的来源保存并传回正确上下文。

一个只展示 API 调用关系的文本 echo 函数如下；Gateway 应把接纳/投递语义放回自己的
应用用例，而不是把 echo 逻辑当作平台实现：

```go
package example

import (
    "context"

    "github.com/liuzengh/trpc-agent-service/platform/im/wecom"
)

func Echo(ctx context.Context, botID, secret string) error {
    client, err := wecom.NewClient(wecom.Config{BotID: botID, Secret: secret})
    if err != nil {
        return err
    }
    defer client.Close(context.Background()) // 内部仍受 CloseTimeout 约束
    return client.Run(ctx, func(ctx context.Context, event wecom.Event) error {
        if event.Kind != wecom.EventText {
            return nil
        }
        _, err := client.Reply(ctx, wecom.ReplyRequest{
            RequestID: event.RequestID, Generation: event.Generation,
            StreamID: event.MessageID, Content: "已收到文本",
        })
        return err // 本示例在发送失败时终止 Run，不做自动 Final 重试
    })
}
```

## ACK、并发与失败确定性

`CommandAck` 包含 RequestID、Generation 和数字 ErrCode。仅收到当前连接、完整 req_id
匹配的服务端 ACK 且显式整数 `errcode == 0` 时，Reply 才返回 nil error（`ACCEPTED`）。
`CommandAck` 不是用户已读，也不是 Gateway 已持久记账。

| 结果 | `CommandError.Certainty` | 典型 Code |
| --- | --- | --- |
| 参数无效、尚未认证、已关闭、旧 generation、未进入 write 就取消 | `NOT_SENT` | `invalid_request` / `not_ready` / `closed` / `stale_generation` / `canceled` |
| 同 req_id 在途、内存预算已满、已有 Final 尝试 | `NOT_SENT` | `request_in_flight` / `capacity_exceeded` / `final_already_attempted` |
| 服务端明确非零 errcode | `REJECTED` | `provider_rejected`；数字保留在 CommandAck.ErrCode |
| 进入 WebSocket Write 后错误、ACK 超时、断线/关闭中断 | `UNKNOWN` | `write_failed` / `ack_timeout` / `disconnected` / `canceled` |
| 已不确定的 req_id 再次调用 | `NOT_SENT` | `request_poisoned` |

pending 在 write 前注册，ACK reader 永远不等待 handler。一次 write 进入调用边界后，
即使 Write 返回错误，也保守视为可能已有字节发送，不误报 NOT_SENT。确定性与业务重试
政策分开；库不替业务层决定 Final 可重试。

**P0 本地保守策略，不是 Provider 限制：**同 generation 的每个 req_id 最多一次已尝试
Final；明确 `REJECTED` 也消耗该身份，成功后也不复用。UNKNOWN 会 poison 该 ID，迟到
或重复 ACK 仅被忽略，不把“下一次同 ID 发送”误当成上一次确认。更换 generation 后，
旧 Event.Generation 仍被拒绝；本库不把旧回调自动迁移到新连接，也不自动重发 Final。

每个连接最多保留 MaxRequestIDs 个已尝试 Final 的短期 ID 标记。达到上限后新的 Reply
返回 `NOT_SENT/capacity_exceeded`，连接本身仍可读消息/心跳；调用方据此决策，库不静默
丢 callback、不自动清除 tombstone、不通过重连绕过该保守限制。MaxPending 控制在途
业务 Final 命令数；auth/ping 另保留 1 个控制命令位（两者不同时运行），pending 总表
始终有界于 MaxPending + 1。健康连接不会仅因业务 pending 暂时占满而误判心跳失联。

## 生命周期状态与容量

`StateSnapshot` 包含 State、Generation、Sequence、Reason。`States()` 是**单消费者**的
有界 latest-wins 通知流，不是广播：慢消费者会跳过中间 Sequence，永远不阻塞 reader；
需要当前状态时读 `State()`。终态快照先发布，再关闭 States；可继续读取最后缓存项。

主要状态：IDLE → CONNECTING → AUTHENTICATING → READY；可恢复故障经过
DISCONNECTED → BACKOFF → 新 generation。Run 最终为 STOPPED、FAILED 或 REPLACED；
显式 Close 在活动生命周期内将其终止为 CLOSED。已经发布的终态不会被后来的 Close
重写。Generation 仅隔离 socket/ACK，不提供跨进程租约或撤销远端写入的能力。

| Config 字段 | 零值默认 | 边界/含义 |
| --- | --- | --- |
| BotID / Secret | 无 | 调用方注入；不读平台配置 |
| URL | `wss://openws.work.weixin.qq.com` | 显式 `ws`/`wss`；不接受 URL userinfo/query/fragment |
| DialTimeout | 10s | 每次拨号 |
| AckTimeout | 5s | 单命令含等待 writer、write 和 ACK 的总预算 |
| WriteTimeout | 5s | 不超过命令总预算 |
| HeartbeatInterval | 30s | 应用 `ping`；RFC WebSocket pong 不替代该 ACK |
| CloseTimeout | 5s | Run handler drain 与 Close 等待的各自上限 |
| ReconnectBackoff | 1s | 可取消固定退避 |
| MaxReconnects | 0 | 默认不重连；显式 1–100；不因认证拒绝/协议错误/队列满/被替换而重试 |
| EventBuffer | 64 | 1–65,536；满时终止 ErrEventOverflow，不阻塞 reader/静默丢弃 |
| StateBuffer | 16 | 1–1,024；latest-wins |
| MaxPending | 64 | 1–65,536；业务 Final 在途上限，另有 1 个控制保留位 |
| MaxRequestIDs | 4,096 | 1–1,000,000；每 generation 的已尝试 Final 标记上限 |
| ReadLimit | 1 MiB | 256 bytes–16 MiB；单个 WebSocket 消息 |

所有时长支持 1ms–1h。MaxReconnects 计数属于整个 Run，不因短暂 READY 而重置，因此
持续断线也不会无限重连。认证 ACK 超时终止为 ErrAuth，不在本库中盲重试凭据。

Close 可并发、重复调用：新 Reply 立即被关闭 gate 拒绝，pending 分别获得自己的
NOT_SENT/UNKNOWN/已知 ACK 结果。Close 只报告 aggregate drain，不代替逐次 Reply 结果。

Go 不强制杀死不合作的用户 handler。如果 handler 超过 CloseTimeout 仍不返回，Run
返回 ErrDrainTimeout，并保存 **sticky drain failure**；之后 Close 也返回 ErrDrainTimeout，
即使 handler 后来退出，也不把已经失败的关闭承诺改写成成功。所有库自有 reader、心跳、
连接与 pending 都先取消；这个边界由回归测试验证，并在测试结束时释放阻塞 handler。

## 代码结构

```text
platform/im/wecom/
├── types.go          # 公开 Config/Event/Reply/State/typed errors
├── config.go         # 构造器本地校验与有限默认值
├── client.go         # 单 Run、generation、dispatcher、重连、关闭
├── command.go        # pending/write/ACK/确定性/Final ID 标记
├── protocol.go       # 严格有界 JSON 解码、回调与 ACK 分派
├── client_test.go    # 第一条真实 WS subscribe→handler Reply→ACK 纵切
├── fault_test.go     # 认证、迟到 ACK、抢占、队列、生命周期/race 回归
└── protocol_test.go  # 心跳、容量、类型解码、读上限、取消
```

## 验证

在仓库根目录运行（只绑定 loopback，不需要真实企微凭据/公网隧道/PG/NATS）：

```bash
GOCACHE=/private/tmp/gateway-wecom-cache go test -race -count=10 -cover ./platform/im/wecom
GOCACHE=/private/tmp/gateway-wecom-cache go vet ./platform/im/wecom
```

本轮上述测试已通过（24 个顶层测试，另含表驱动子测试）；命令、输入、RED/GREEN 与
退出状态记录在 [VERIFICATION.txt](VERIFICATION.txt)。10 次重复的最后输出：

```text
ok  github.com/liuzengh/trpc-agent-service/platform/im/wecom  6.169s  coverage: 88.9% of statements
```

覆盖完整认证 ACK/拒绝/缺失或错误字段、handler 内 Reply 与持续心跳、心跳丢 ACK、
同 ID 并发/预算、迟到 ACK poison、断线后旧 generation、被替换不抢回、慢状态观察者、
事件队列满、旧 handler 取消后新 dispatcher 继续工作、并发 Close、非合作 handler 的
sticky drain timeout、unsupported 事件、错误 BotID/JSON/读上限、取消认证与退避。

真实账号行为仍以后续明确验收结果为准；Gateway 集成与部署状态见
[Gateway 实现状态](../../../docs/architecture-next/channel-gateway/implementation-status.md)。

### 官方端点握手兼容

2026-09-07 真实验证发现官方端点对 `Sec-WebSocket-*` 的 HTTP/1 拼写敏感。
client 内部克隆 HTTP client/request 后保留官方 SDK 拼写，修复默认 Go 拼写下的
HTTP 404；不改变调用方 transport 所配置的代理、TLS、超时或重定向策略。
`TestHandshakePreservesProviderHeaderSpelling` 直接检查线上的 HTTP/1 请求字节。
认证及 marker 收发的本轮证据见 Gateway 的 `wecom-preflight-v1.md`。
