# tRPC-Agent-Go 多租户节点化 Agent 平台

## 项目文档入口

- 项目规格/工程约束：`docs/complete-development-plan.md`（范围、架构、不变量、设计取舍、完成定义）
- 当前实现状态/验收证据：`docs/acceptance.md`
- 术语表与决策：`CONTEXT.md`、`docs/adr/`
- 生产风险清单：`docs/production-risk-register.md`

## 开发工具链基线

- Go 1.24.1 或同一 1.24.x 补丁线；CI 与 Docker builder 均固定为 1.24.x。
- Node.js 20–22，推荐 22.x；前端安装必须使用 `npm ci`。
- Go 1.24 的选择不是因为核心 tRPC-Agent-Go 根模块（其最低版本仍为 1.21），而是当前依赖图中的 Qdrant、OpenClaw、OIDC、OpenTelemetry 与 `golang.org/x/*` 模块已经声明 Go 1.24。详细决策见 [ADR-0010](docs/adr/0010-go-1.24-toolchain.md)。

## 项目概览

本项目基于 [tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) 构建面向企业的多租户 Agent 平台。平台将 Agent 编排、模型调用、Tool/MCP、Session、Memory、Knowledge、Artifact 和 Telemetry 统一接入租户控制面，并通过 Gateway、Worker 与 Channel Connector 的职责分离支持水平扩展。

平台服务于多个部门、业务线和消息入口：租户可以发布不可变应用配置，选择受管模型与数据后端，授权工具和渠道，设置预算与审计策略；运行面以 Kafka 承载异步任务，以 PostgreSQL、Redis 和 S3-compatible 存储保存可靠性状态与业务数据。

## 核心能力

- **多租户控制面**：应用、模型、工具、数据后端、渠道、预算和审计策略统一配置；配置按版本发布并支持查询与回滚。
- **异步执行面**：Gateway 完成认证、租户路由、会话解析和 Kafka 入队；Worker 执行 Agent，业务副作用持久化后提交 offset。
- **可靠消息投递**：执行去重、claim/heartbeat/fencing、持久化重试、事务 Outbox、发送租约与 delivery receipt 共同约束重复执行和投递恢复。
- **企业身份与权限**：支持本地账号、企业微信、飞书和 OIDC 登录；区分系统管理员、租户管理员和成员，渠道身份与会话归属按可信边界处理。
- **会话与平台数据**：统一管理 Session、摘要、Memory、Knowledge 和 Artifact；支持 PostgreSQL、Redis、pgvector、Qdrant、S3-compatible 等受管后端。
- **渠道接入**：支持企业微信智能机器人、飞书、Telegram 和 Web 对话；统一处理消息长度、节流、附件、进度更新和最终回复。
- **治理与可观测性**：工具白名单、参数校验、危险操作确认、租户限流、Token/成本计量、脱敏审计与 OpenTelemetry 链路追踪。
- **部署与验收**：提供 Docker Compose、Kubernetes/Kind 清单，以及 PostgreSQL、Redis、Kafka、S3、浏览器 E2E 和 Race 测试入口。

## 架构与数据原则

1. Worker 保持无状态，业务状态写入共享后端；同一 Session 通过分区键和执行租约维持顺序。
2. 配置快照随消息版本固化，Worker 不使用后来发布的配置覆盖在途执行。
3. 租户数据同时依靠显式作用域、PostgreSQL RLS、受管 Secret 引用和审计脱敏隔离。
4. Kafka 是首期消息主链路；Redis 用于缓存、短租约、限流和实时事件分发。
5. 外部发送采用至少一次语义；支持幂等键的渠道复用稳定键，不支持的渠道通过租约、receipt 和审计降低重复概率。
6. Knowledge VectorStore 是可重建派生索引，Session/Memory/Artifact 和平台控制数据具有独立的迁移与恢复规则。

## 项目交付组成

- Go 服务与命令行入口；
- React 管理控制台；
- 数据库 baseline 与 RLS 策略；
- Docker Compose、Kubernetes 和 Kind 部署清单；
- 单元、Race、真实基础设施和浏览器 E2E 测试；
- 架构、ADR、运维、风险和验收文档。

## 框架复用与平台实现

| 平台需求 | 可复用的框架能力 | 本项目平台实现 |
| --- | --- | --- |
| Agent 编排 | `agent/llmagent`、`agent/graph`、Chain / Parallel / Cycle | 租户级 Agent 注册、发布与路由 |
| 执行入口 | `runner.Runner`（流式 Event、context 取消） | 多租户 Worker 调度、无状态水平扩展 |
| Session / Memory / Artifact / Knowledge | `session`、`memory`、`artifact`、`knowledge` 及多后端实现 | 租户级后端选择、数据隔离与迁移 |
| Tool / MCP / Skill | `tool`、MCP Tool、`skill` | 租户工具白名单与密钥注入 |
| 治理 | Plugin / Guardrail / Callbacks | 租户策略下发、预算与审批 |
| 服务化 | `server/openai`、`server/agui`、`server/a2a`、`server/trpcagent` | 统一 Gateway、Admin API |
| IM 接入 | OpenClaw Gateway + Channel | 微信 / 企业微信等通道与租户绑定 |
| 可观测性 | OpenTelemetry tracing / metrics | 租户维度审计、成本与合规 |

## 代码目录

仓库按运行入口、平台能力、交付资源和验证证据分层：

```txt
|-- cmd/trpc-service       # serve / migrate 命令与应用组合根
|-- trpcservice            # Agent、租户、消息、身份、存储、渠道和 Web 平台实现
|-- internal               # 仅仓库内部使用的基础模块
|-- migrations             # PostgreSQL 当前基线、RLS 与迁移执行器
|-- webui                  # React + TypeScript 管理控制台
|-- configs                # 平台配置示例
|-- deploy                 # Compose、Kubernetes、OTel 与告警配置
|-- scripts                # 基础设施、Kind、运维和质量验证脚本
|-- tests                  # 浏览器、真实基础设施与验收产物
|-- docs                   # 架构、ADR、运维、风险与验收文档
|-- examples               # 租户应用与接入示例
|-- data                   # 本地运行日志、PID 和私有环境文件
|-- build.sh               # 构建 Web 控制台和 Go 服务
|-- start.sh / stop.sh     # 本地服务启停
|-- coverage.sh            # 生成 Go 覆盖率报告
|-- format.sh / lint.sh    # 格式化与静态检查
`-- Dockerfile             # 多阶段、non-root 生产镜像
```

## 快速开始

需要 Go 1.24.x、Node.js 20–22、npm 和 Docker Compose。以下命令从 Fork 仓库启动本地依赖与应用：

```bash
git clone https://github.com/zezhen0222/trpc-agent-service.git
cd trpc-agent-service

cp deploy/.env.example deploy/.env
# 编辑 deploy/.env：替换所有 replace-with-* 值，并设置可用的 MODEL_API_KEY。
mkdir -p data
cp deploy/.env data/platform.env

docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --wait postgres redis redpanda minio
docker compose --env-file deploy/.env -f deploy/compose.yaml run --rm minio-init

./build.sh
./start.sh
curl --fail http://127.0.0.1:8080/readyz
```

控制台地址为 `http://127.0.0.1:8080/console/`。没有真实模型密钥时，可按下文“OpenAI 兼容 mock 模型”说明启动仓库内置 Mock。更完整的 Compose、镜像和 Kubernetes 操作见 [部署说明](deploy/README.md)。

停止应用和本地依赖：

```bash
./stop.sh
docker compose --env-file deploy/.env -f deploy/compose.yaml down
```

默认不会删除持久化 Volume；除非明确需要清空本地数据，否则不要添加 `--volumes`。

## 开发控制台（Web）

服务内置一个产品化的 Web 控制台（React + TypeScript + Vite，构建产物经 `go:embed` 嵌入二进制）：

- 入口：`http://<host>:8080/console`，登录后访问。登录页固定展示企业微信、飞书、OIDC 和 Mock 四种能力；未配置项明确标为“未配置”且不可点击。部署时通常只启用一种正式登录，开发环境可额外启用 Mock。所有受保护 API（`/api/v1/*`）走服务端会话（HttpOnly Cookie），写操作要求 CSRF 双提交；`/healthz`、`/readyz` 无需认证。
- **机器人管理**：创建 / 编辑机器人（租户应用）、配置通道绑定、停用 / 启用。配置以不可变版本发布，持久化在 PostgreSQL 控制面（`tenants` / `applications` / `application_configs` / `channel_bindings`），重启不丢失；每次变更版本号 +1，旧版本保留可回滚。
- **对话**：`POST /api/v1/chat/async` 要求客户端提供本次发送的 UUID `request_id`，网络重试必须复用该值；服务只发布一次 Kafka 消息并为重复请求返回相同 `event_id` 和 `202`。页面随后通过 SSE 等待 Worker → Agent → Outbox 的持久结果；`Last-Event-ID` 支持断线补偿，delta/done 事件支持流式渲染。Web/IM 图片与文件先暂存为输入 Artifact，Kafka 只携带引用。
- **平台数据**：按租户应用管理 Knowledge 与用户偏好。Knowledge 文件采用异步导入：文本/DOCX 复用 tRPC-Agent-Go Reader，PDF/PPTX/XLSX/HTML/图片在启用 Docling 时由框架 Extractor 抽取后统一切片索引；单个上传文件上限 16 MiB。运行产出不作为平台长期数据页签，而是在对应会话中查看和下载。
- **会话管理**：管理员可统一查看 Web、企业微信、Telegram、飞书会话、真实聊天记录和会话产出。外部账号严格按渠道隔离，不提供与 Web 平台用户的手工身份关联；普通成员只可读取自己的 Web 会话。
- **可观测性**：平台与 tRPC-Agent-Go 共用 OpenTelemetry Provider；框架原生导出模型耗时、Token、TTFT、TPOT 与输出速率，平台补充租户成本、Store、Outbox 和渠道投递指标。可选接入 OTLP Collector、Langfuse，或启用 `/metrics` 供 Prometheus 直接抓取；执行页继续使用脱敏后的 Framework ExecutionTrace 投影。
- 管理 API：`GET/POST /api/v1/apps`、`PUT /api/v1/apps/{tenant_id}/{app_code}`（发布新版本），`GET/PUT /api/v1/memory`、`GET/POST /api/v1/knowledge`、`GET/PUT /api/v1/artifacts`（会话产出），以及会话、审计、Outbox、执行和系统状态接口；详见 `trpcservice/web/console_api.go`。

前端改动：

```bash
cd webui
npm ci           # 严格按 package-lock 安装
npm run dev      # 开发模式：HMR，API 代理到 http://127.0.0.1:8080
npm run build    # 产物输出到 webui/dist；./build.sh 会自动执行
```

`./build.sh` 会在 Go 编译前自动构建前端（`webui/src` 有更新时），因此日常只需 `./build.sh && ./start.sh`。

> **关于 `webui/dist` 入库**：`webui/embed.go` 通过 `//go:embed dist` 在编译期把前端产物嵌入二进制，因此 `webui/dist/` 作为**确定性构建产物**随仓库一并维护（`.gitignore` 用 `!webui/dist/` 显式放行），保证新克隆者无需 Node 工具链即可直接 `go build`。改动前端后请同步提交 `webui/dist/` 的更新，二者保持一致。

可观测性按需启用，不配置外部出口时不会阻塞本地开发：

```bash
# 推荐生产链路：应用 -> OTLP Collector -> Prometheus/Trace backend
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318

# 可选：直接在服务的 /metrics 暴露同一组 OTel Metrics。
# 与 Collector Prometheus exporter 二选一采集，避免重复统计。
PROMETHEUS_ENABLED=true

# 可选：框架原生 Langfuse exporter。三项必须同时配置。
LANGFUSE_PUBLIC_KEY=pk-...
LANGFUSE_SECRET_KEY=sk-...
LANGFUSE_HOST=cloud.langfuse.com:443
# LANGFUSE_INSECURE=true                         # 仅本地 HTTP Langfuse
# LANGFUSE_OBSERVATION_LEAF_VALUE_MAX_BYTES=8192 # 可选 observation 叶值限制
```

启用 Langfuse 后，框架会把 LLM Prompt/Output 作为 observation 内容发送给所配置的 Langfuse 实例；密钥、正文或 Prompt 不会被平台额外复制进日志、Prometheus 标签或 ExecutionTrace 管理投影。

没有真实模型 API key 时，可用仓库自带的 OpenAI 兼容 mock 模型做端到端联调：

```bash
node scripts/mock-openai.mjs   # 监听 http://127.0.0.1:9999/v1
# 在平台级 model_providers 中把受管 Provider 的 base_url 改为
# http://127.0.0.1:9999/v1（仅 loopback HTTP 被允许），租户只改 provider_id/model.name。
# 再在 data/platform.env 中设置 MODEL_API_KEY=mock-key 后重启。
```

当前 `go.mod` 锁定的 tRPC-Agent-Go 根模块直接复用 OpenAI-compatible、腾讯混元和 HuggingFace Model Adapter；租户只选择平台受管的 `provider_id + model name`。模型凭据由 `SecretResolver` 统一解析，应用组合层不直接读取模型密钥。HuggingFace 等供应商的模型 ID 按上游原样使用，可包含 `org/model`；多模型故障切换使用框架 Failover，Hedge 暂不启用。

## IM 通道接入（企业微信 / 飞书 / Telegram）

外部 IM 通道统一收敛为各平台最常规、最稳定的**单一规范长连接/长轮询 Connector 模式**，由 Gateway 节点通过 Redis 分布式租约（`channelConnectorManager`）选举 Leader 运行，无需公网 IP 和复杂回调域名配置：
长连接/长轮询接入 → 归一化消息 → Kafka envelope → Worker → Agent → Outbox 回复。

- **企业微信**（`platform.json` 配置 `type: wecom` Binding + binding 级 `credential_ref`）：
  - 唯一采用**企业微信智能机器人 WebSocket 长连接**（`wecombot`，`wss://openws.work.weixin.qq.com`）。
  - 凭据包含 `bot_id` 与 `secret`，以 `aibot_subscribe` 完成握手并以 30 秒心跳保活；入站自动区分单聊与群聊，支持文本、图片、文件、视频、语音与 mixed 消息；媒体下载/解密后统一暂存 Artifact。出站使用 `aibot_send_msg` 发送 Markdown 消息。
- **飞书**（`type: feishu` + binding 级 `credential_ref`）：
  - 唯一采用**飞书官方 SDK WebSocket 长连接**（`feishu.Connector`，`larkws.NewClient`）。
  - 凭据包含 `app_id` 与 `app_secret`，以长连接分发事件，群聊自动校验 `@bot` 触发；图片/文件通过官方 Channel SDK 资源接口下载后统一暂存 Artifact，出站使用官方 SDK 投递回复。
- **Telegram**（`type: telegram` + binding 级 `credential_ref`）：
  - 唯一采用官方 Bot API **长轮询**（`telegram.Poller`，`getUpdates`）。
  - 凭据包含 `bot_token`；只有消息已经持久发布或明确忽略后才推进 `offset`，遇到 429 自动退避自愈。支持 document/photo/video/audio/voice 下载；出站使用 `sendMessage`，超长文本由统一 Outbox 分片层处理。
- 通道凭证均**可选**：无对应绑定时不要求填，服务可只开网页对话运行。
- **正确性保障（无需真实网关）**：三大通道均具备完整的本地单元测试与 Mock 网关契约测试，验证握手保活、断线重连、消息标准化与出站回复。

## 企业登录

控制台登录替换了早期的 HTTP Basic：登录会话存储在 Redis（多节点共享），
Cookie 为 `HttpOnly + SameSite=Lax`，写操作做 CSRF 双提交校验（Cookie `csrf_token` 与头 `X-CSRF-Token`）。

一个部署实例就是一个企业边界，不再额外建“组织”。代码支持企业微信、飞书、OIDC 三种正式 Provider。部署时通常只配置**一种**正式登录方式；如果显式配置了多种，登录页会全部展示。开发环境可额外显示 Mock 登录。

单正式入口推荐直接用 `LOGIN_PROVIDER=feishu|wecom|oidc`，不需要写 Provider JSON。开发环境需要同时保留 Mock 时设置 `LOGIN_MOCK_ENABLED=true`；纯 Mock 则使用 `LOGIN_PROVIDER=mock`。生产环境不要启用 Mock。

飞书开发环境还可直接复用现有飞书通道的 `FEISHU_TRAILFORGE_CONFIG_JSON`（App ID / App Secret），无需重复填写登录凭据；用户点击后跳转到飞书官方授权页完成登录。企业微信智能机器人凭据与企业登录自建应用不是同一种凭据，因此企业微信登录仍需单独提供 CorpID、AgentID 和 Secret。

推荐用文件配置 Provider，密钥仍通过 `SecretResolver` 引用环境变量：

```json
{
  "providers": [
    {
      "id": "wecom-main",
      "type": "wecom",
      "display_name": "企业微信",
      "corp_id": "wwxxxxxxxxxxxxxxxx",
      "agent_id": 1000002,
      "secret_ref": "env:WECOM_LOGIN_SECRET"
    }
  ]
}
```

然后设置：

```bash
LOGIN_PROVIDERS_FILE=/path/login-providers.json
LOGIN_CALLBACK_URL=https://console.example.com/api/v1/auth/callback
```

也可直接使用 `LOGIN_PROVIDERS_JSON`。飞书 Provider 只要求 `app_id + secret_ref`，`tenant_key` 为可选的额外企业边界校验；OIDC 使用 `issuer + client_id + client_secret_ref`。三种正式登录统一使用 `LOGIN_CALLBACK_URL`，避免不同 Provider 各写一份回调地址导致不一致。

登录页只展示当前已启用的登录方式。第一个正式 Provider 作为主入口，其余正式 Provider、本地账号和显式启用的 Mock 收在紧凑的备用入口中；未配置的能力不会占用登录页。开发环境需要额外保留 Mock 时设置 `LOGIN_MOCK_ENABLED=true` 即可。

### 飞书重定向 URL

飞书后台登记的重定向 URL 必须与 `LOGIN_CALLBACK_URL` **完全一致**，包括协议、域名/IP、端口和路径。使用本仓库 Vite 开发服务器时推荐统一走前端 `/api` 代理：

```text
http://localhost:5173/api/v1/auth/callback
```

飞书应用后台也必须登记这一条；`localhost`、`127.0.0.1`、不同端口、HTTPS/HTTP 不同都视为不同地址。浏览器访问控制台时也应使用同一个主机名，否则登录事务 Cookie 不会随 OAuth 回调发送。服务启动时会校验 `LOGIN_CALLBACK_URL` 必须是绝对 HTTP(S) URL，并且路径必须精确为 `/api/v1/auth/callback`。

### 飞书扫码登录（页面内二维码）

默认关闭。开启后登录页会为飞书渲染页面内二维码，用户用飞书 App「扫一扫」并在手机上确认即可完成登录，不再整页跳转到飞书登录页：

```bash
LOGIN_FEISHU_QR_MODE=sdk_redirect   # 默认 off
```

可选配置：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `LOGIN_FEISHU_QR_MODE` | `off` | `off` 关闭；`sdk_redirect` 启用页面内二维码 |
| `LOGIN_FEISHU_QR_SDK_URL` | 飞书官方 CDN | 自托管 SDK 时覆盖，非 loopback 地址必须是 https |
| `LOGIN_FEISHU_QR_AUTHORIZE_URL` | `passport.feishu.cn` 授权页 | 私有化/国际版可覆盖 |

二维码 SDK 使用飞书旧版授权与换取接口；普通跳转登录继续使用 v2 接口，两条流程由签名登录交易区分，不会互相回退。二维码有效期 5 分钟，过期后页面会提示刷新。

### 企业微信后台配置（自建应用）

1. 企业微信管理后台 →「应用管理」→ 创建**自建应用**，得到 `AgentId` 与 `Secret`（corp_secret）。
2. 「网页授权及 JS-SDK」→ 设置**可信域名**（即控制台所在域名，需完成域名校验/备案）。
3. 「可见范围」选择允许登录本平台的员工。可信员工即使暂未分配部门也可登录，系统管理员随后在“用户与部门”中分配。
4. 把 CorpID、AgentID、Secret 引用写入登录 Provider 配置，并把同一个回调地址写入 `LOGIN_CALLBACK_URL` 后重启。

### 内网穿透 / 测试域名提醒

- 企业微信授权回调要求**公网可访问**且与可信域名一致；本地开发需用内网穿透
  （`ngrok` / `cloudflared` 等）把控制台端口暴露为 HTTPS 域名，`LOGIN_CALLBACK_URL`
  填对应公网地址，`LOGIN_SECURE_COOKIE=true`。
- 生产环境务必 HTTPS：`LOGIN_SECURE_COOKIE=true` 使会话与 CSRF Cookie 带 `Secure` 标记，
  未启用时 Cookie 不限制安全传输（开发用 HTTP 可接受）。

### 系统管理员

首次部署可通过 `SYSTEM_ADMIN_PROVIDER_ID` + `SYSTEM_ADMIN_SUBJECT_ID` 指定首个系统管理员。该身份首次登录后权限持久化到 `system_admins`；之后可在“用户与部门”页面授权其他系统管理员。

### 相关配置项

| 配置项 | 说明 | 默认 |
|---|---|---|
| `LOGIN_PROVIDER` | 单正式入口快捷配置：`feishu` / `wecom` / `oidc`；`mock` 表示纯开发登录 | — |
| `LOGIN_PROVIDERS_FILE` / `LOGIN_PROVIDERS_JSON` | 登录 Provider 配置，二选一 | — |
| `LOGIN_CALLBACK_URL` | 企业微信 / 飞书 / OIDC 共用的回调 URL，必须精确指向 `/api/v1/auth/callback` | — |
| `LOGIN_PROVIDER=mock` | 本地开发 Mock 登录快捷方式 | — |
| `LOGIN_MOCK_ENABLED` | 在正式登录入口之外额外显示 Mock 测试登录 | `false` |
| `MOCK_LOGIN_SUBJECT_ID` / `MOCK_LOGIN_DISPLAY_NAME` / `MOCK_LOGIN_EMAIL` | Mock 测试用户 | `mock-admin` / `Mock Admin` / 空 |
| `SYSTEM_ADMIN_PROVIDER_ID` / `SYSTEM_ADMIN_SUBJECT_ID` | 首个系统管理员身份键 | — |
| `LOGIN_SESSION_TTL` | 会话有效期（Go duration，默认 8h） | `8h` |
| `LOGIN_SECURE_COOKIE` | Cookie 加 Secure（HTTPS） | `false` |
| `LOGIN_STATE_SECRET` | OAuth state / 登录事务 HMAC 密钥 | — |

---

## 部署边界与注意事项

本节说明运行环境、第三方平台和开发数据库的明确使用边界。

### 后端 Profile 需显式授权（部署必读）

数据库迁移只写入 `backend_profiles` 的默认条目（`platform-postgres`、`platform-pgvector`），
**不会**自动写入 `tenant_backend_profiles`。新建租户后，若应用配置引用了这些 Profile
（例如 `examples/trailforge/application.json` 中的 `storage.session.profile_id`），
必须先为该租户授权，否则创建或更新应用会失败并提示：

```
backend profile is not authorized for tenant: <profile_id>
```

授权方式：以系统管理员登录控制台，在「数据后端」页面为租户勾选对应 Profile；
或在 `tenant_backend_profiles` 表中插入 `(tenant_id, profile_id)` 记录。

### 登录交互差异

飞书支持页面内二维码登录（`trpcservice/identity/feishu_provider.go` 的 `BeginQR`）；企业微信采用标准 OAuth **跳转登录**：点击按钮跳转到企业微信授权页，授权后回调到同一个 `/api/v1/auth/callback`。
登录方式接口 `GET /api/v1/auth/providers` 的 `qr_supported` 字段标明各 Provider 是否支持二维码，
前端据此决定是否渲染二维码面板。

### 多标签页并发登录

登录事务 Cookie `dsh_login_tx` 为同名 Cookie。同一浏览器多标签页并发发起登录时，
后发起的会覆盖前一个（后者生效）。属既有机制限制，低频场景，未引入第二套状态。

### 开发数据库基线可变

`migrations/000001_init.sql` 是当前**唯一且可变**的开发基线（历史开发迁移已 squash）。
一旦该文件内容变化，已标记 version 1 的数据库会因 checksum 不匹配而拒绝迁移，并提示
`rebuild the development database`。此时需在开发环境重建 schema：

```sql
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
CREATE EXTENSION IF NOT EXISTS vector;      -- DROP CASCADE 会连带删除扩展
GRANT trpc_tenant TO trpc_platform;         -- 恢复租户角色授权
GRANT USAGE, CREATE ON SCHEMA public TO trpc_platform;
GRANT USAGE ON SCHEMA public TO trpc_tenant;
```

然后重新执行 `./bin/trpc-service migrate`。
形成首次兼容性发布后，数据库结构变更必须使用递增迁移文件，并保持已发布迁移不可变。

### 依赖版本

`trpc.group/trpc-go/trpc-agent-go/openclaw` 当前采用上游发布的 `v0.0.1`，由 `trpcservice/channels/channels.go` 引用；升级时必须先通过渠道契约与重连测试。其余 `trpc-agent-go` 子模块统一对齐到 `v1.11.x`。
