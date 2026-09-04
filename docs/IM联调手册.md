# IM 联调手册（真实收发 + mock 冒烟）

> 状态：代码就绪，真实收发待本地账号手测（需求验收 B6）。
> 代码：`trpcservice/infra/channels/{wecom,feishu}`、`cmd/trpc-service` gateway 装配、
> binding 驱动 `channels.Manager`。
> 本文给出：①不依赖真实账号的 **mock 冒烟**；②真实企微/飞书配置步骤；③验证清单。

## 1. 链路速览

```
IM 平台 WSS/长连接 ─► wecom/feishu Conn ─► Adapter 归一化
  ─► bus.PublishInbound(stream:inbound) ─► worker(消费组) ─► runner.Run
  ─► 回复写 outbox ─► stream:outbound ─► gateway.Run ─► adapter.Send ─► IM
```

代码均真实接线：企微走 `wecom-aibot-go-sdk` 长连接（wecom/conn.go:78-81）、
飞书走 lark-go `ws.Client`（feishu/conn.go）。**只差一份真实企业/应用的凭据**
即可完成端到端手测。

## 2. Mock 冒烟（可自动跑，不依赖真实 IM）

mock 冒烟用内存 fakeBus/fakeAdapter 驱动完整桥接逻辑，验证
adapter→bus→outbound→Send 的每一环逻辑，覆盖与本手册 §4 相同的判定点
（会话路由、去重、截断、限流）：

```bash
# IM 桥接闭环（入站发布+路由、出站分发、绑定解析、限流、截断）
go test -count=1 ./trpcservice/infra/channels/ -v

# worker 消费→runner→回复→审计 的单测（不连 IM）
go test -count=1 ./trpcservice/app/worker/ -v
```

`infra/channels/gateway_test.go` / `limit_test.go` 覆盖：
- 入站消息归一化为 bus.Message 且携带 `id=channel:platformMsgID`（幂等键）；
- 出站经 session 路由回原 adapter + chatID；
- IM 用户白名单在 worker 侧生效；频率限流拦截超限入站；
- 超长回复按平台上限截断。

**冒烟通过 ≠ 真实收发完成**——mock 不含平台侧真实握手/加密/重连，必须做 §3。

## 3. 真实收发联调步骤

### 3.1 启动平台（Docker Compose 单机）

```bash
cp deployments/.env.example deployments/.env   # 按需改密码/DSN
./begin.sh up                                   # 或 docker compose -f deployments/docker-compose.yml up -d
curl localhost:8080/healthz                     # 就绪
```

### 3.2 后端/管理面准备

1. 创建租户（PUT /tenants）→ 拿到 `tenant_id`；
2. 创建模型 Endpoint（POST /endpoints，Provider 选 openai/anthropic/gemini，
   模型名 + API Key）→ `endpoint_id`；
3. 创建 Agent（POST /agents，RuntimeProfile 挂 endpoint_id，Publish 发布）→ `agent_id`。

### 3.3 企业微信

1. 企业微信管理后台注册自建应用，获取 Corp ID / AgentId / Secret；服务器需配置
   可公网回调地址（如用 WSS 长连接回调模式则按 SDK 要求配置回调 URL + Token +
   EncodingAESKey）；
2. 管理面 POST /channels 建绑定：
   `{channel:"wecom", tenant_id, agent_id, account_id:"<CorpID>", credential_ref 指向
   存好的 Secret}`（web/channel_handler.go 会把凭据写入 secret store，引用存库）；
3. `imMgr.Reload` 由 ChannelAPI 自动触发（保存绑定即连接，见 channels/manager.go）；
4. 在企微会话里 @机器人发文本 → 观察回复。

预期：服务日志出现 `channels: started adapter …`；Jaeger 出现一条
`im.callback → agent.run → im.reply` 的完整 trace（trace_id 一致）。

### 3.4 飞书

1. 飞书开放平台创建应用，开通机器人能力 + `im:message` 事件订阅，获取 App ID /
   App Secret / 事件订阅密钥（Verification Token + Encrypt Key）；
2. 同上建绑定 `{channel:"feishu", …}`（secret 存 App Secret）；
3. 在飞书群 @机器人（群聊门禁按 botOpenID 判断，feishu/feishu.go:102）或单聊发消息。

### 3.5 验证清单

- [ ] 单聊文本→回复（非流式文本即可；企微流式占位与替换已实现 wecom/conn.go:92-98/174）
- [ ] 群聊 @ 门禁：飞书未 @ 机器人的消息被丢弃；企微群消息全处理
- [ ] 重复投递：平台侧补发同 `platformMsgID` → 服务端不重复执行（幂等）
- [ ] 会话连续性：同租户同人第二次提问可读到上轮会话/记忆
- [ ] 超长回复被截断（>2000 企微 / >4000 飞书字符实验）
- [ ] Jaeger：`im.callback / agent.run / im.reply / tool / session` 同 trace
- [ ] 审计：GET /audit 看到 channel/user/session/agent/tokens/latency/trace_id
- [ ] 失败路径：停掉 MySQL/Redis 一端，观察降级日志与重投行为

## 4. 待真实账号联调后方可确认的项（代码就绪）

| 项 | 现状 | 判定 |
| --- | --- | --- |
| 逐字流式回复（KindStream） | 企微占位+原位替换已实现；飞书回复仍整段文本 | 需真实长连接观察 |
| 卡片消息（KindCard） | 定义存在未接线 | 需求未强制，可选 |
| 媒体文件收发 | 识别已实现，收发未处理 | 见《IM平台限制与降级策略》§3 |
| 长连接断线重连 | SDK 自带重连；平台层 Stop/Reload 已实现 | 真实网络下验证 |

## 5. 排障提示

- 没回复且无 `started adapter`：binding 未建/凭据未入库/Secret 未 resolve
  （看 `channels: binding has no credential, skipping`）；
- 有 `started adapter` 但无 outbound：worker 未消费（Redis/MySQL 未配）、
  agent 未发布（approval 拦）、或回复被限流/白名单拦截（看日志 Warn）；
- trace 断链：确认 telemetry.otlp_endpoint 已配（compose 默认指向 otel-collector）。
