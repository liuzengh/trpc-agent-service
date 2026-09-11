# channels 包 — IM 通道适配器

> 本目录将企业微信、飞书等 IM 平台归一化为统一契约（`Adapter` / `InboundMessage` / `OutboundMessage`），
> 供上游 Worker 消费。遵循 tRPC-Agent-Go 的 OpenClaw Channel 模型，但独立实现（复用边界见 AGENTS.md §5）。

## 架构分层

```
channels/                 统一契约 + 平台适配器
├── channels.go           契约：Adapter/Conn 接口 + InboundMessage/OutboundMessage + session_id 规则
├── channels_test.go
├── wecom/                企业微信适配器（单文件）
│   ├── wecom.go          验签(SHA1排序) + AES 解密 + 归一化 + Adapter 组装去重
│   └── wecom_test.go
└── feishu/               飞书适配器（单文件）
    ├── feishu.go         验签(SHA256拼接) + 归一化 + @提及过滤 + Adapter 组装去重
    └── feishu_test.go
```

**分层原则**：契约收在 `channels.go`（跨通道共享）；每个平台一个包、一个实现文件，
文件内以 `// ---- protocol ----` / `// ---- adapter ----` 分隔协议函数与 Adapter 组装。
`Conn` 接口隔离真实 WSS 长连接（沙箱无法实测，用 mock 跑通测试）。

## 真实 SDK 接线（本地执行）

真实 `Conn` 实现只需满足 `channels.Conn` 三个方法：`Recv`（返回明文事件字节）、`Send`、`Close`。

### 企业微信（WSS 客户端模式）

企业微信智能机器人 WSS 长连接，官方无 Go SDK，需按 aibot 协议自实现（或社区库）。
接线要点：连接 `wss://openws.work.weixin.qq.com/...` → 鉴权 → 收加密消息 → 用本包
`VerifySignature` + `DecryptMsg` 解出明文 XML → 交给 `Adapter`。凭据：corpid/secret/token/EncodingAESKey。

### 飞书（官方 SDK）

```bash
cd D:/Develop/Git/trpc-agent-service
# 本地跑，沙箱网络慢故未纳入 go.mod
go get github.com/larksuite/oapi-sdk-go/v3@v3.7.2
```

接线要点：`ws.NewClient(appID, appSecret, ...)` + `EventDispatcher` 订阅 `im.message.receive_v1`，
回调里把事件 JSON 字节交给 `Adapter`（`Recv` 返回），回复经 `Conn.Send` 发回。
凭据：app_id / app_secret；群聊需 `@机器人`（`botOpenID` 在 `New` 时传入）。

## 测试

```bash
go test ./trpcservice/channels/...
```

覆盖：验签正确/错误、AES 解密往返、单聊/群聊归一化、@提及过滤、去重、Send 组装。全用 mock `Conn`。
