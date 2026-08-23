# tRPC-Agent-Go 多租户 IM Agent 平台

这是一个可运行的多租户 Agent 平台实现：企业微信智能机器人和飞书机器人都通过客户端主动发起的 WebSocket 长连接接入，不需要公网 webhook、`cloudflared`、ngrok 或 frp。Gateway 负责 IM 连接和收发，Worker 通过共享 Session 后端无状态执行 [tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) Runner，PostgreSQL Inbox/Outbox 与 Redis Stream 提供可靠投递。

## 已实现能力

- 企业微信智能机器人：`BotID + Secret`、认证/心跳/重连、文本消息、流式 Markdown 回复、单 Bot Redis lease。
- 飞书机器人：官方 `oapi-sdk-go/v3` 的 WSS 事件客户端、`im.message.receive_v1`、单聊、群聊 @ 过滤、消息 API 幂等回复。
- DeepSeek：OpenAI-compatible 模型接入、流式 Runner、45 秒 deadline、`get_server_time` Function Tool；普通测试不会调用真实模型。
- 多租户：不可变 AgentVersion、无重启发布/回滚、工具白名单、Channel Binding、Session/Memory/Knowledge/Artifact 后端画像。
- 无状态 Worker：企业微信演示租户使用 tRPC-Agent-Go Redis Session，飞书演示租户使用 PostgreSQL Session；同一 session 通过 lease + fencing token 串行执行。
- 可靠性：PostgreSQL Inbox/Outbox、四字段幂等键、Redis Stream consumer group、pending reclaim、回复重试与 DLQ。
- 治理：租户级并发、输入/输出 token、每日费用上限和 Tool allowlist；预算预占、Usage 结算、拒绝审计及指标完整闭环。
- 真实后端隔离：Qdrant 独立 collection/tenant filter，MinIO 统一 bucket 下独立租户前缀，提供可重复的写入、查询、校验和删除 smoke。
- 运维：`all/gateway/worker/admin` 四种角色、健康检查、Prometheus 指标、审计日志、Docker Compose 和 Kubernetes 示例。

详细设计见 [docs/design.md](docs/design.md)，实测证据模板见 [docs/demo-checklist.md](docs/demo-checklist.md)。

## 快速开始

要求 Go 1.22+。只验证离线链路时不需要 Docker，也不会读取任何真实密钥：

```bash
go test ./...
go run ./cmd/trpc-service serve --config configs/demo.yaml --role all --fake-model
```

服务默认只监听 `127.0.0.1:8080`：

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl http://127.0.0.1:8080/metrics
```

启动共享后端：

```bash
docker compose up -d postgres redis qdrant minio jaeger otel-collector
go run ./cmd/trpc-service migrate --config configs/local.local.yaml
go run ./cmd/trpc-service backend-smoke --tenant all --config configs/local.local.yaml
```

## 密钥配置

不要复制密钥内容到仓库或 `.env.example`。程序只接受引用，并在启动组件时读取文件一次：

```powershell
$env:WECOM_CREDENTIAL_FILE = 'C:\path\outside\repo\wecom.txt'
$env:FEISHU_CREDENTIAL_FILE = 'C:\path\outside\repo\feishu.txt'
$env:DEEPSEEK_KEY_FILE = 'C:\path\outside\repo\deepseek.txt'
$env:TRPC_POSTGRES_DSN = 'postgres://trpc:trpc@127.0.0.1:5432/trpc_agent?sslmode=disable'
$env:TRPC_REDIS_ADDR = '127.0.0.1:6379'
```

企业微信文件按两行保存 `BotID`、`Secret`；飞书文件按两行保存 `AppID`、`App Secret`，也接受 `名称: 值` 或 `名称=值`。通道日志只记录 binding、状态和错误分类，不输出凭据值、文件内容、App/Bot ID 或指纹。

若历史企微文件没有字段名且顺序是 `Secret`、`BotID`，设置
`WECOM_CREDENTIAL_ORDER=secret_botid`；默认值为 `botid_secret`。推荐给文件补上字段名，避免人工误判，但绝不能把文件移入仓库。

## 双长连接验证顺序

先验证企业微信，smoke 模式使用确定性 Echo Agent：

```powershell
go run ./cmd/trpc-service channel-smoke --channel wecom --config configs/demo.yaml
```

看到认证成功后，向企业微信 Bot 发文本并确认收到 `echo:` 回复，然后 `Ctrl+C` 退出。

再保持飞书长连接运行：

```powershell
go run ./cmd/trpc-service channel-smoke --channel feishu --config configs/demo.yaml
```

连接 Ready 后在飞书企业应用后台确认长连接、事件权限、机器人可用范围和已发布版本，再在企业客户端完成单聊及群聊 @ 验证。旧个人版应用不作为提交版入口或验收证据。整个流程不开放本机 HTTP 到公网。

正式运行前将 `configs/demo.yaml` 中两个 binding 的 `enabled` 改为 `true`（建议复制为被 gitignore 的 `configs/local.local.yaml`），再执行：

```powershell
go run ./cmd/trpc-service serve --config configs/local.local.yaml --role all
```

生产环境分别启动：

```bash
trpc-service serve --config /etc/trpc/platform.yaml --role gateway
trpc-service serve --config /etc/trpc/platform.yaml --role worker
trpc-service serve --config /etc/trpc/platform.yaml --role admin
```

`gateway` 和 `worker` 不启动管理端口，避免多角色在同一台机器上争用地址；`admin`（以及本地 `all`）负责 Admin API、健康和指标端点。正式双 Worker 验收可同时启动一个 gateway、两个 worker 和一个 admin。

## 管理 API

- `POST /api/v1/tenants`
- `POST /api/v1/agents`
- `POST /api/v1/agents/{id}/versions`
- `POST /api/v1/agents/{id}:publish`
- `GET /api/v1/agents/{id}?tenant_id={tenant}`
- `GET /api/v1/agents/{id}/versions?tenant_id={tenant}`
- `GET /api/v1/runtime/{tenant}`
- `GET /api/v1/audit-logs?tenant_id={tenant}&limit=100`
- `POST /api/v1/channel-bindings`
- `POST /api/v1/backend-profiles`
- `GET /healthz`、`GET /readyz`、`GET /metrics`

本地 Admin API 没有内置公网认证，必须保持 loopback 监听；Kubernetes 中应由内部网关补充 mTLS/OIDC 和 RBAC。

AgentVersion 的完整 RuntimeProfile 存入 PostgreSQL且不可变。发布操作在事务中增加 revision、写审计并发出 `LISTEN/NOTIFY`；Worker 失效缓存后让新请求使用新 Runner，运行中的旧请求完成后才关闭旧 Runner。重新发布旧版本即完成无重启回滚，通知丢失时最多 30 秒轮询过期。

## 质量检查

```bash
test -z "$(gofmt -l $(find . -name '*.go' -not -path './vendor/*'))"
go vet ./...
staticcheck ./...
go test -race ./...
bash ./coverage.sh
docker build -t trpc-agent-service:test .
```

真实 DeepSeek 和真实 IM 测试不进入普通 CI。GitHub Actions 会启动 PostgreSQL、Redis、Qdrant 和 MinIO，执行 Race、核心包 80% 覆盖率、镜像构建及 Gitleaks 全历史扫描；提交前仍应确认本地凭据文件从未进入 Git。

需要显式执行一次真实且会产生少量 API 费用的 Runner/Tool 验证时：

```powershell
$env:LIVE_E2E = '1'
$env:DEEPSEEK_KEY_FILE = 'C:\path\outside\repo\deepseek.txt'
go test ./trpcservice/agent -run TestLiveDeepSeekToolCall -v
```

## 版本

- starter baseline：`aa000c8407dcd6ea7788fcdccde9574b44bbe2d2`
- tRPC-Agent-Go：`0e352fdd1428d30a8d978d39877f5a7b2591ccc1`
- 飞书 Go SDK：`v3.7.2`
- 企业微信 Go SDK：`0cb6bde0f054ba54b0b718521a5b388cb2a1c09c`

飞书 `v3.7.2` 尚未包含主分支文档中的高级 `Channel` 包，因此本实现使用同一官方 SDK 已发布且稳定的 `ws.Client + event/dispatcher + im/v1 Reply` API，避免依赖未发布代码。
