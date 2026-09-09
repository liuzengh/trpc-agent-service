# 基于 tRPC-Agent-Go 设计多租户节点化 Agent 部署平台

## 背景和价值

企业在落地 Agent 应用时，通常不会只部署一个单体机器人，而是希望面向多个部门、多个业务线、多个 IM 入口和多个数据后端，构建一套可统一管理的 Agent 平台。例如：客服团队希望把 Agent 接入企业微信，研发团队希望接入内部群机器人，运营团队希望接入微信公众号或微信客服，不同租户又需要隔离会话、记忆、知识库、工具权限和审计日志。

[tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) 已经具备 Agent 编排（LLMAgent / GraphAgent / Chain / Parallel / Cycle）、Tool / MCP、Session、Memory、Knowledge、Artifact、Plugin / Guardrail、Telemetry、HTTP 服务化（OpenAI-compatible / AG-UI / A2A）、OpenClaw / IM 通道等能力。该题要求基于这些能力设计一个“多租户、可节点化部署、支持多后端数据同步、可接入微信 / 企业微信等 IM 软件”的生产级方案。

这个题目解决的业务痛点是：企业希望把 Agent 能力从单点 demo 扩展成平台化服务，同时满足租户隔离、弹性部署、数据一致性、IM 触达、审计合规和后端可替换等要求。它的价值在于把框架能力真正映射到企业级 Agent 平台架构，而不是只停留在单个 Agent 进程。

本题以 **tRPC-Agent-Go** 为实现框架，对称于基于 tRPC-Agent-Python 的同名题目。

### 任务描述

请设计一个基于 tRPC-Agent-Go 的多租户节点化 Agent 部署平台。平台需要支持多个租户创建和部署自己的 Agent，每个租户可以绑定不同 IM 通道、选择不同数据后端、配置不同工具权限和知识库，并允许多个 Agent 节点水平扩展。系统需要考虑跨节点会话路由、数据同步、后端适配、IM 消息接入、监控审计和故障恢复。

本题以架构设计为主，可以包含少量关键 Go 伪代码、接口定义或数据模型示例。不要求实现完整系统，但方案必须足够具体，能指导后续工程落地。

## 具体要求

### 多租户与节点部署

- 设计租户模型，至少包含 `tenant_id`、应用配置、模型配置、工具权限、IM 通道配置、数据后端配置、审计策略。
- 设计节点部署拓扑，说明 Agent Gateway、Agent Worker、Channel Adapter、Storage Adapter、Admin API、Telemetry Collector 等组件如何协作。可对照 tRPC-Agent-Go 中的 `runner.Runner`、`server/*`、`openclaw` Gateway 与 Channel 的职责划分。
- 支持多节点水平扩展，说明用户消息如何路由到正确租户和正确 session。
- 说明是否需要 sticky session；如果不需要，说明如何依赖共享 Session / Memory 后端（例如 `session/redis`、`session/mysql`、`session/postgres`）实现无状态 Worker。
- 设计租户隔离机制，包括配置隔离、数据隔离、工具权限隔离、日志脱敏和密钥管理。

### 数据同步与多后端支持

- 支持不同租户选择不同数据后端，例如 InMemory、Redis、SQL、向量库、对象存储或外部 Memory 服务。tRPC-Agent-Go 已提供 Session（inmemory / redis / mysql / postgres / sqlite / mongodb 等）、Memory、Knowledge、Artifact 以及 `storage`（redis / mysql / postgres / s3 / qdrant / milvus 等）适配，方案需说明如何在平台层做租户级选择与路由。
- 设计统一的数据访问抽象，说明 Session、Memory、Summary、Artifact、Knowledge、Audit Log 分别如何存储。
- 设计数据同步策略，至少覆盖：
  - 多节点并发写入同一 session 的一致性。
  - Session event、state、summary 的更新顺序。
  - Memory 写入后的跨节点可见性。
  - 后端从 Redis 迁移到 SQL 或从本地向量库迁移到远端向量库时的数据迁移方案。
  - IM 消息重复投递时的幂等处理。
- 说明不同后端的一致性取舍，例如强一致、最终一致、读写延迟、成本和运维复杂度。
- 给出一个最小数据模型或表结构示例，至少包含 tenant、agent app、session、message/event、memory、summary、channel binding、audit log。

### IM 软件接入

- 设计 IM Channel Adapter，支持企业微信、微信客服、微信公众号、Telegram 或其他 IM 通道中的至少两类。可复用并扩展 tRPC-Agent-Go 的 OpenClaw Channel 模型。
- 说明外部 IM 消息如何转换为 tRPC-Agent-Go 的用户输入（`model.Message` / `runner.Runner.Run`），Agent Event 如何转换为 IM 回复、流式消息或卡片消息。
- 设计 IM 账号和租户绑定方式，包括 webhook URL、token、secret、回调验签、消息去重、用户身份映射。
- 说明群聊和单聊的 `session_id` 生成规则，以及用户跨群、跨租户时的隔离策略。
- 考虑 IM 平台限制，例如消息长度、频率限制、异步回复、图片 / 文件消息、撤回或失败重试。

### 治理、监控和安全

- 使用 Plugin / Guardrail / Callbacks 设计租户级治理策略，例如工具白名单、敏感信息脱敏、预算限制、危险工具二次确认、IM 用户权限校验。
- 设计监控指标，例如请求量、模型调用耗时、工具调用耗时、IM 投递成功率、错误率、token 消耗、每租户成本、Session 后端延迟。
- 说明如何接入 OpenTelemetry 或等价 tracing，要求 trace 能串起 IM callback、Runner 执行、Tool 调用、Session / Memory 读写和 IM 回复。
- 设计审计日志字段，至少包含 `tenant_id`、`channel`、`user_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency`、`error_type`、`cost`、`trace_id`。
- 说明密钥管理和脱敏策略，IM token、模型 API key、数据库密码不能明文出现在日志、trace 或错误报告中。

### 故障恢复与运维

- 设计节点故障、IM 重试、数据库短暂不可用、模型超时、工具执行失败时的降级策略。Go 侧需同时说明 `context.Context` 取消、goroutine 生命周期和 Runner 事件通道排空，避免泄漏。
- 说明如何做灰度发布和租户级配置回滚。
- 说明如何做容量评估，例如每节点并发 session 数、平均 token 消耗、Redis / SQL QPS、IM 回调峰值。
- 设计最小可运行部署方案和生产推荐部署方案，可以使用 Docker Compose、Kubernetes 或等价部署方式描述。

### 交付物

- 一份架构设计文档，建议 2000 – 4000 字。
- 一张系统架构图，展示 Gateway、Worker、Channel Adapter、Storage Adapter、Plugin / Guardrail、Telemetry、数据库和 IM 平台之间的关系。
- 一张核心时序图，展示“企业微信用户发消息 → Agent 执行 → Tool 调用 → Session / Memory 写入 → IM 回复”的完整链路。
- 一份数据模型设计，包含核心表结构或 JSON schema。
- 一份数据同步和幂等策略说明。
- 一份多后端适配方案，说明 Redis / SQL / 向量库 / 对象存储分别适合存什么。
- 一份风险清单，列出至少 8 个生产风险及对应缓解措施。
- 一份基于该设计的 GitHub 实现代码。

## 题目难点

- 多租户隔离不是只加一个 `tenant_id` 字段，还涉及配置、权限、密钥、数据、日志、工具和成本隔离。
- 节点化部署要求 Agent Worker 尽量无状态，但 Agent 又天然依赖 Session、Memory、Summary 和工具上下文，需要设计可靠的共享状态层。
- IM 通道存在消息乱序、重复投递、响应超时、长度限制和身份映射问题，不能简单等同于 HTTP chat API。
- 不同后端的数据一致性能力不同，Redis、SQL、向量库、对象存储无法用同一种同步策略处理。
- Agent 执行链路包含模型、工具、MCP、知识库、沙箱和外部系统，监控和审计必须跨组件串联。
- 企业级平台必须考虑灰度、回滚、租户级限流、成本控制和合规审计。

## 验收标准

1. 架构方案必须覆盖多租户、节点化部署、数据同步、多后端支持、IM 接入、治理监控和故障恢复。
2. 数据模型必须能表达 tenant、agent、channel binding、session、event、memory、summary、audit log 的关系。
3. 必须说明至少两种 IM 通道的接入差异，其中至少包含微信或企业微信。
4. 必须说明至少三类后端的数据存储和同步策略，例如 Redis、SQL、向量库或对象存储。
5. 必须给出一条完整消息链路的时序说明，包含 `trace_id` 或 `request_id` 如何贯穿链路。
6. 必须列出至少 8 个生产风险和缓解措施。
7. 方案需要明确哪些能力可直接复用 tRPC-Agent-Go，哪些需要新增平台层模块。

## 可直接复用的 tRPC-Agent-Go 能力对照

| 平台需求 | 可复用的框架能力 | 需要新增的平台层 |
| --- | --- | --- |
| Agent 编排 | `agent/llmagent`、`agent/graph`、Chain / Parallel / Cycle | 租户级 Agent 注册、发布与路由 |
| 执行入口 | `runner.Runner`（流式 Event、context 取消） | 多租户 Worker 调度、无状态水平扩展 |
| Session / Memory / Artifact / Knowledge | 根模块接口，以及独立发布的 `session/redis v1.11.0`、`memory/redis v1.11.0`、`storage/redis v1.11.0` 等后端子模块；Redis 方案已由 Phase 1.5 B 路径验证 | 租户级后端选择、`RedisBackend` 延迟初始化、命名空间、生命周期与迁移 |
| Tool / MCP / Skill | `tool`、MCP Tool、`skill` | 租户工具白名单与密钥注入 |
| 治理 | Plugin / Guardrail / Callbacks | 租户策略下发、预算与审批 |
| 服务化 | `server/openai`、`server/agui`、`server/a2a`、`server/trpcagent` | 统一 Gateway、Admin API |
| IM 接入 | OpenClaw Gateway + Channel | 微信 / 企业微信等通道与租户绑定 |
| 可观测性 | OpenTelemetry tracing / metrics | 租户维度审计、成本与合规 |

当前实现已完成 Phase 2 多租户内核：只读 JSON Catalog 和 `PresetRepository` 管理 Tenant、AgentApp、ChannelBinding、ConfigVersion 与 StorageProfile；服务端通过可信 Binding 派生租户和活动配置，`BackendProvider` 按租户选择官方 InMemory/Redis Session/Memory，`RunnerRegistry` 按 `tenant_id + agent_app_id + config_version` 缓存 Runner。原 Demo HTTP 与环境变量入口保持兼容，Redis 不可用时不回退其他后端。详见 [`docs/stage2-multi-tenant.md`](docs/stage2-multi-tenant.md)。Phase 1.5 的官方 Redis 选择与 SQL Spike 证据仍见 [`docs/stage1.5-storage-spike.md`](docs/stage1.5-storage-spike.md)。

## 代码目录

下面只是一个示范目录，用来说明平台需要覆盖的职责分层。实现时不必严格按这个结构组织代码，只要模块边界清晰、能对应到设计方案即可。

```txt
|-- README.md              # 说明文档，包含设计、安装、使用
|-- go.mod                 # Go module 定义
|-- build.sh               # 构建项目
|-- clean.sh               # 清理中间产物
|-- coverage.sh            # 运行单测覆盖率
|-- format.sh              # 格式化 Go 代码
|-- lint.sh                # 静态检查
|-- start.sh               # 启动服务
|-- stop.sh                # 停止服务
|-- data                   # 服务运行时数据
|-- docs                   # 各模块说明与架构设计文档
|-- cmd
|   `-- trpc-service       # 命令行入口，可直接启动服务
`-- trpcservice            # 源码
    |-- agent              # 基于 tRPC-Agent-Go 的 Agent 定义
    |-- channels           # 对接 IM 的 Channel Adapter
    |-- config             # 租户与节点配置
    |-- log                # 日志级别与脱敏
    |-- metrics            # 监控指标
    |-- skill              # 可运行的 Skill
    |-- tenant             # 多租户模型与隔离
    |-- tool               # 平台 Tool
    |-- version.go         # 版本信息
    |-- web                # 管理 / 对话页面
    `-- workspace          # 工作目录，包含本地、容器等沙箱环境
```

## 快速开始

```bash
git clone https://github.com/liuzengh/trpc-agent-service.git
cd trpc-agent-service

./build.sh
./start.sh
```

服务默认只监听 `127.0.0.1:8080`。需要显式对外开放或修改端口时使用 `./start.sh -addr :8081`；脚本会将参数传给 `trpc-service serve`。

Phase 2 也可以通过 `PLATFORM_CONFIG_FILE` 加载只读多租户目录。示例文件为 `configs/phase2.example.json`；先按实际环境修改模型 endpoint，并通过环境变量提供引用的凭据：

```bash
export IDENTITY_SECRET='replace-with-at-least-32-random-bytes'
export PLATFORM_CONFIG_FILE='configs/phase2.example.json'
export PHASE2_MODEL_KEY='replace-with-model-key'
export PHASE2_REDIS_URL='redis://localhost:6379/0'
./start.sh
```

目录文件只允许 `env:<ENV_NAME>` 凭据引用，不得写入模型 Key 或 Redis URL 明文。未设置 `PLATFORM_CONFIG_FILE` 时继续使用原有单租户环境变量契约。

### Phase 3 可靠消息

Phase 3 将 Demo 请求接入 Redis Streams、Inbox 去重、任务租约和有限重试。`serve` 仍是一条命令启动，但内部会运行一个 Gateway 和一个单并发 Worker，并且不会绕过可靠消息链路：

```bash
export IDENTITY_SECRET='replace-with-at-least-32-random-bytes'
export PLATFORM_CONFIG_FILE='configs/phase3.example.json'
export PHASE3_MODEL_KEY='replace-with-model-key'
export PHASE3_MESSAGING_REDIS_URL='redis://localhost:6379/0'
export PHASE3_TENANT_REDIS_URL='redis://localhost:6379/0'
./start.sh
```

也可以分别启动两个角色：

```bash
./bin/trpc-service gateway -addr :8080
./bin/trpc-service worker -health-addr :8081
```

Gateway 只需要目录、`IDENTITY_SECRET` 和 Messaging Redis 凭据；Worker 还需要模型及租户存储凭据。两者必须使用相同的 `IDENTITY_SECRET`。Messaging Redis 可以和租户 Redis 使用同一实例，但使用独立客户端与 `<key_prefix>:reliable-v1` 命名空间。

`POST /api/v1/demo/messages` 的成功响应保持不变。Gateway 默认同步等待 75 秒；如果任务仍在重试，会返回 `504 task_pending`，任务不会取消。客户端应使用完全相同的 `message_id` 和消息内容重试，以等待或读取 Inbox 中的缓存结果。幂等窗口由 `inbox_retention` 控制，默认 24 小时；窗口过期后相同消息 ID 会被视为新消息。

Phase 3 的默认 `session_fencing=legacy` 保持单 Worker 契约。Worker 崩溃或退出后，新 Worker 会在租约过期后恢复 Pending 任务。Phase 3 的实现与验收见 [`docs/stage3-reliable-messaging.md`](docs/stage3-reliable-messaging.md)。

### Phase 4 双 Worker 与 Strong Session Fencing

Phase 4 增加显式 `session_fencing=strong`。两个独立 Worker 可以共享 Consumer Group：同一 Session 按 Redis `session_seq` 严格串行，不同 Session 可以并行；task lease 和 Session lock 都失效后，其他 Worker 才能接管原 Pending。旧 Worker 的迟到 Session commit、Retry、Fail、Recover 和 release 都会被 fencing 拒绝。

使用 Strong 示例：

```bash
export IDENTITY_SECRET='replace-with-at-least-32-random-bytes'
export PLATFORM_CONFIG_FILE='configs/phase4.example.json'
export PHASE4_MODEL_KEY='replace-with-model-key'
export PHASE4_REDIS_URL='redis://localhost:6379/0'

./bin/trpc-service gateway -addr :8080 -consumer gateway-a
./bin/trpc-service worker -health-addr :8081 -consumer worker-a
./bin/trpc-service worker -health-addr :8082 -consumer worker-b
```

Strong 模式有以下硬约束：

- Messaging 与所有活动 Session StorageProfile 必须使用同一 Redis URL、logical DB 和 primary `run_id`；
- Redis 必须是可验证的 standalone primary；InMemory、Redis Cluster、跨 Redis、不可验证代理和 fallback 都会被拒绝；
- Session 数据使用 `<key_prefix>:reliable-v1:fenced-v1`，不读取或迁移旧 namespace；
- `max_turn_events` 和 `max_turn_bytes` 默认分别为 512 与 2 MiB，超限只产生一次 `session_turn_too_large` 终态；
- task heartbeat 只续 task lease/Pending，Session heartbeat 只续 Session lock。

完整 key schema、状态转换、恢复规则、测试矩阵和真实 Redis 7.4.11 验收见 [`docs/stage4-two-workers-session-lock.md`](docs/stage4-two-workers-session-lock.md)。Strong fencing 保护 Session/Inbox/Reply，不承诺模型、Tool、Memory 或外部系统副作用 exactly-once。

### Phase 5 Telegram、企业微信智能机器人与 Web UI

Phase 5 把 Telegram 长轮询、企业微信智能机器人 API 长连接和本地 Web UI 接入同一可靠链路。Telegram 使用 `github.com/go-telegram/bot v1.25.0`，由项目自行控制 `getUpdates` offset；企业微信使用官方 WebSocket 协议薄客户端，不再使用自建应用 callback、验签或消息解密。企微字段级 Spike 见 [`docs/stage5-wecom-spike.md`](docs/stage5-wecom-spike.md)。

示例配置为 `configs/phase5.example.json`。所有真实值仍只通过环境变量注入：

```bash
export IDENTITY_SECRET='replace-with-at-least-32-random-bytes'
export PLATFORM_CONFIG_FILE='configs/phase5.example.json'
export PHASE5_REDIS_URL='redis://localhost:6379/0'
export PHASE5_MODEL_KEY='replace-with-model-key'
export PHASE5_TELEGRAM_TOKEN='replace-with-telegram-token'
export PHASE5_WECOM_BOT_ID='replace-with-wecom-bot-id'
export PHASE5_WECOM_BOT_SECRET='replace-with-wecom-bot-secret'

./bin/trpc-service gateway -consumer gateway-a
./bin/trpc-service worker -health-addr :8081 -consumer worker-a
./bin/trpc-service worker -health-addr :8082 -consumer worker-b
```

Worker 不读取任何 IM 凭据。引用格式错误会拒绝启动；引用值缺失、认证失败或连接断开时进程保持存活，但 Gateway/serve 的 `/readyz` 返回 503。一个 Telegram/企微 Bot 首版只允许一个 Gateway 连接。

Web UI 位于 `http://127.0.0.1:8080/`，使用预置 demo binding，不接受浏览器提交的 tenant/Agent/配置版本。异步接口为：

```text
POST /api/v1/web/messages
GET  /api/v1/web/messages/{message_id}?binding_id=<demo-binding>
```

页面每 1500ms 轮询 `submitted/processing/succeeded/failed`。原同步 `POST /api/v1/demo/messages` 保持兼容。IM 首版只发一次性文本；Agent 失败向 IM 返回统一文案，Web 查询保留完整错误码。

出站状态保存在 `<prefix>:reliable-v1:outbound:<task_id>`。发送成功或达到默认 5 次上限后，Lua 才原子更新终态并确认 Reply Stream；Gateway 重启会恢复 Pending 和 attempts。外部发送成功、Redis 确认前崩溃仍可能重复，因此只承诺至少一次。完整实现和验收矩阵见 [`docs/stage5-telegram-wecom-webui.md`](docs/stage5-telegram-wecom-webui.md)。

### Phase 5.5 Redis / PostgreSQL / MySQL 多后端持久化

Phase 5.5 允许每个 Tenant/Agent App 选择 Redis、PostgreSQL 或 MySQL Session/Memory；Redis 始终承担 Streams、Inbox、lease、Session lock、重试和接管。SQL Turn 成功提交后才会写成功结果和 Reply，SQL 故障不会回退 Redis，也不会重新调用模型。

三租户示例为 `configs/phase5.5.example.json`：

```bash
export IDENTITY_SECRET='replace-with-at-least-32-random-bytes'
export PLATFORM_CONFIG_FILE='configs/phase5.5.example.json'
export PHASE55_MODEL_KEY='replace-with-model-key'
export PHASE55_REDIS_URL='redis://localhost:6379/0'
export PHASE55_POSTGRES_DSN='postgres://user:password@localhost:5432/app?sslmode=require'
export PHASE55_MYSQL_DSN='user:password@tcp(localhost:3306)/app?parseTime=true&charset=utf8mb4&loc=UTC'

./bin/trpc-service gateway -addr :8080 -consumer gateway-a
./bin/trpc-service worker -health-addr :8081 -consumer worker-a
./bin/trpc-service worker -health-addr :8082 -consumer worker-b
```

配置文件只保存 `env:` 引用。PostgreSQL schema 必须预先存在；`skip_db_init=false` 创建并验证表，`true` 只验证现有表。每个 Agent App 首次执行时在 Messaging Redis 锁定脱敏后端指纹，后续禁止改变 backend/database/schema/prefix，但允许密码和 TLS 配置轮换。

PostgreSQL `session/postgres v1.11.0` 的跨时区 Summary 缺陷仍存在，因此 PostgreSQL Summary 在本阶段强制禁用。SQL Memory 固定无限容量、无 Extractor、无 Memory Tool。完整状态机、SQL 事务、错误码、schema/版本契约和升级门槛见 [`docs/stage5.5-sql-persistence.md`](docs/stage5.5-sql-persistence.md)。

停止服务：

```bash
./stop.sh
```

### Phase 7 deployment delivery

Phase 7 adds reproducible `full`, `light`, `obs`, and `ha` Compose modes,
offline Model/Telegram mocks, SQL init/readiness commands, OTLP telemetry, and
an automated fault matrix. Run `scripts/phase7/up.ps1 -Mode full`; the first
run creates `compose/.env` and stops until required values are reviewed. See
`docs/stage7-compose.md`, `docs/stage7-observability.md`, and
`docs/stage7-final-design.md` for the runbook and design boundaries.
