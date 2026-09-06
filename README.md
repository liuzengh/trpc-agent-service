# 基于 tRPC-Agent-Go 设计多租户节点化 Agent 部署平台

## 背景和价值

企业在落地 Agent 应用时，通常不会只部署一个单体机器人，而是希望面向多个部门、多个业务线、多个 IM 入口和多个数据后端，构建一套可统一管理的 Agent 平台。例如：客服团队希望把 Agent 接入企业微信，研发团队希望接入内部群机器人，运营团队希望接入微信公众号或微信客服，不同租户又需要隔离会话、记忆、知识库、工具权限和审计日志。

[tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) 已经具备 Agent 编排（LLMAgent / GraphAgent / Chain / Parallel / Cycle）、Tool / MCP、Session、Memory、Knowledge、Artifact、Plugin / Guardrail、Telemetry、HTTP 服务化（OpenAI-compatible / AG-UI / A2A）、OpenClaw / IM 通道等能力。该题要求基于这些能力设计一个“多租户、可节点化部署、支持多后端数据同步、可接入微信 / 企业微信等 IM 软件”的生产级方案。

这个题目解决的业务痛点是：企业希望把 Agent 能力从单点 demo 扩展成平台化服务，同时满足租户隔离、弹性部署、数据一致性、IM 触达、审计合规和后端可替换等要求。它的价值在于把框架能力真正映射到企业级 Agent 平台架构，而不是只停留在单个 Agent 进程。

本题以 **tRPC-Agent-Go** 为实现框架，对称于基于 tRPC-Agent-Python 的同名题目。

### 任务描述

请设计一个基于 tRPC-Agent-Go 的多租户节点化 Agent 部署平台。平台需要支持多个租户创建和部署自己的 Agent，每个租户可以绑定不同 IM 通道、选择不同数据后端、配置不同工具权限和知识库，并允许多个 Agent 节点水平扩展。系统需要考虑跨节点会话路由、数据同步、后端适配、IM 消息接入、监控审计和故障恢复。



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
| Session / Memory / Artifact / Knowledge | `session`、`memory`、`artifact`、`knowledge` 及多后端实现 | 租户级后端选择、数据隔离与迁移 |
| Tool / MCP / Skill | `tool`、MCP Tool、`skill` | 租户工具白名单与密钥注入 |
| 治理 | Plugin / Guardrail / Callbacks | 租户策略下发、预算与审批 |
| 服务化 | `server/openai`、`server/agui`、`server/a2a`、`server/trpcagent` | 统一 Gateway、Admin API |
| IM 接入 | OpenClaw Gateway + Channel | 微信 / 企业微信等通道与租户绑定 |
| 可观测性 | OpenTelemetry tracing / metrics | 租户维度审计、成本与合规 |

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

## 设计文档

完整设计已拆分到 [`docs`](docs/README.md)，包括：

- [从一个可运行 Agent 开始](docs/getting-started.md)
- [总体架构和系统架构图](docs/architecture.md)
- [企业微信完整执行时序](docs/sequence.md)
- [核心数据模型和表结构](docs/data-model.md)
- [数据同步、一致性和幂等策略](docs/data-consistency.md)
- [多后端适配方案](docs/backend-adapters.md)
- [企业微信、微信公众号和 Telegram 接入](docs/im-channels.md)
- [Telegram 手动测试运行手册](docs/telegram-manual-runbook.md)
- [从聊天走到真实工具调用](docs/current-time-tool-walkthrough.md)
- [Telegram 工具审批上手说明](docs/telegram-approval-walkthrough.md)
- [治理、安全、监控、故障恢复和部署](docs/governance-operations.md)
- [生产风险清单](docs/risks.md)
- [代码实施路线和验收映射](docs/implementation-roadmap.md)
- [功能实现与验证状态](docs/feature-status.md)

仓库中的“已编码”“自动测试”“本地集成”和“真实联调”是不同状态。Telegram 已完成开发环境真实 Bot 基础联调，企业微信仍处于 Adapter 代码与模拟协议测试阶段。各项能力的具体边界以[功能实现与验证状态](docs/feature-status.md)为准。

## 快速开始

```bash
git clone https://github.com/liuzengh/trpc-agent-service.git
cd trpc-agent-service

./build.sh
./start.sh
```

当前仓库包含一个不需要 API Key 的教学 Agent。发送第一轮消息：

```bash
./start-mock.sh
```

```bash
curl -sS -X POST http://127.0.0.1:8080/chat \
  -H 'Content-Type: application/json' \
  -d '{"binding_key":"tutorial-http","message_id":"readme-message-1","user_id":"alice","session_id":"demo","message":"我叫小明。"}'
```

保持相同的 `user_id` 和 `session_id` 再问：

```bash
curl -sS -X POST http://127.0.0.1:8080/chat \
  -H 'Content-Type: application/json' \
  -d '{"binding_key":"tutorial-http","message_id":"readme-message-2","user_id":"alice","session_id":"demo","message":"我叫什么？"}'
```

切换到真实 OpenAI-compatible 模型：

```bash
cp .env.example .env
```

然后编辑 `.env`：

```dotenv
TRPC_AGENT_MODEL_PROVIDER=openai
TRPC_AGENT_MODEL_NAME="你的模型 ID"
OPENAI_API_KEY="你的 API Key"

# 兼容服务可额外设置：
OPENAI_BASE_URL="https://your-provider.example/v1"
```

如果模型由本机的 `workbuddy2api` 提供，先在一个终端启动转换服务：

```bash
./start-workbuddy2api.sh
```

脚本默认进入 `~/workbuddy2api`，执行 `uv run converter.py --desensitize --log converter.log --api-key 0`，以前台方式运行，按 `Ctrl+C` 停止。日志保存在 `~/workbuddy2api/converter.log`。它不会自动启动 Agent，也不会修改 `.env`。安装目录不同时，可以通过 `WORKBUDDY2API_DIR=/实际路径 ./start-workbuddy2api.sh` 指定（这是脚本环境变量，不从项目 `.env` 读取）。

沿用上述 `--api-key 0` 参数时，Agent 的 `.env` 使用 `OPENAI_API_KEY="0"` 和 `OPENAI_BASE_URL=http://127.0.0.1:8787/v1`，模型 ID 保留已验证可用的配置。该简单 Key 仅用于本地测试，不要将转换服务暴露到公网。

保持转换服务终端运行，再在另一个终端检查模型凭据和连通性、启动完整服务：

```bash
./check-model.sh
./start-real.sh
tail -f data/trpc-service.log
```

这两个脚本会清除当前终端中可能覆盖 `.env` 的旧模型环境变量，但不会打印 API Key。`check-model.sh` 只调用一次模型，不启动 PostgreSQL、Redis 或 Agent HTTP 服务。

使用 Redis 保存 Session：

```bash
docker compose up -d redis
```

编辑 `.env`：

```dotenv
TRPC_AGENT_SESSION_BACKEND=redis
REDIS_URL=redis://127.0.0.1:6379/0
REDIS_KEY_PREFIX=trpc-agent-service
TRPC_AGENT_SESSION_TTL=0s

# 单进程使用 local；多 Agent Worker 使用 redis。
TRPC_AGENT_COORDINATOR_BACKEND=redis
TRPC_AGENT_COORDINATOR_LEASE_TTL=30s
TRPC_AGENT_COORDINATOR_RENEW_INTERVAL=10s
TRPC_AGENT_COORDINATOR_RETRY_INTERVAL=50ms

# 多 Worker 使用 redis；重试同一消息时必须复用 message_id。
TRPC_AGENT_IDEMPOTENCY_BACKEND=redis
TRPC_AGENT_IDEMPOTENCY_PROCESSING_TTL=2m
TRPC_AGENT_IDEMPOTENCY_COMPLETED_TTL=24h
TRPC_AGENT_IDEMPOTENCY_RENEW_INTERVAL=30s
TRPC_AGENT_IDEMPOTENCY_POLL_INTERVAL=50ms
```

依赖就绪检查：

```bash
curl -sS http://127.0.0.1:8080/readyz
```

Redis 接入后的启动装配、首轮 Session 创建、历史恢复、模型消息构造和 Event 回写链路，见 [Redis Session 接入后的运行链路](docs/getting-started.md#10-redis-session-接入后的运行链路)。

也可以把 `TRPC_AGENT_SESSION_BACKEND` 设为 `postgres`，复用 `TRPC_AGENT_POSTGRES_URL`，并通过 `TRPC_AGENT_SESSION_POSTGRES_PREFIX` 隔离 tRPC-Agent-Go 的 Session/State/Event/Summary 表。同步持久化保持开启，Worker 返回成功前数据已经对其他节点可见。

同 Session 串行、不同 Session 并行、Redis 租约、续租、安全释放和 fencing token 的链路，见 [Session Coordinator 接入后的运行链路](docs/getting-started.md#11-session-coordinator-接入后的运行链路)。

`message_id` 去重、processing 等待、completed 结果复用和失败重试链路，见 [消息幂等接入后的运行链路](docs/getting-started.md#12-消息幂等接入后的运行链路)。

使用 PostgreSQL 保存租户、Agent App、Revision 和 Channel Binding：

```bash
docker compose up -d postgres
```

```dotenv
TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres
TRPC_AGENT_POSTGRES_URL=postgres://trpc_agent:trpc_agent_dev@127.0.0.1:5432/trpc_agent?sslmode=disable
TRPC_AGENT_POSTGRES_AUTO_MIGRATE=true
TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=true
```

启动、migration、bootstrap 和 readiness 链路见 [PostgreSQL 控制面接入后的启动链路](docs/getting-started.md#13-postgresql-控制面接入后的启动链路)。

Channel Binding、tenant-scoped Runtime 和动态 Agent Revision 编译见 [Channel Binding 路由](docs/getting-started.md#14-channel-binding-到租户-runtime-的路由链路) 与 [Agent Revision Compiler](docs/getting-started.md#15-agent-revision-compiler-运行链路)。

持久化异步入口：

```bash
curl -sS -X POST http://127.0.0.1:8080/inbound \
  -H 'Content-Type: application/json' \
  -d '{"binding_key":"tutorial-http","message_id":"external-001","user_id":"alice","session_id":"durable-session","chat_type":"direct","message":"hello"}'
```

该接口只在 conversation、inbound、agent run 和 queue outbox 同一事务提交后返回 `202`。详见 [持久化 Inbox 和 Transactional Outbox](docs/getting-started.md#16-持久化-inbox-和-transactional-outbox)。

多进程异步队列配置：

```dotenv
TRPC_AGENT_QUEUE_BACKEND=redis
TRPC_AGENT_QUEUE_STREAM=agent-runs
TRPC_AGENT_QUEUE_GROUP=agent-workers
TRPC_AGENT_QUEUE_CLAIM_MIN_IDLE=30s
```

Outbox Relay、Redis Streams pending reclaim 和 Worker 完成链路见 [异步执行链路](docs/getting-started.md#17-outbox-relayredis-streams-和-agent-worker)。

Reply Sender、通道能力、长度切分、发送重试和回执见 [Reply Sender 和 Channel Adapter](docs/getting-started.md#18-reply-sender-和-channel-adapter)。

生产组件可以使用同一镜像分别启动：

```bash
./bin/trpc-service -role gateway -addr :8080
./bin/trpc-service -role relay
./bin/trpc-service -role worker
./bin/trpc-service -role sender
./bin/trpc-service -role jobs
```

角色职责和关闭链路见 [进程角色拆分](docs/getting-started.md#19-进程角色拆分)。

IM callback 地址：

```text
/callbacks/wecom/{callback_key}
/callbacks/telegram/{callback_key}
```

企业微信验签/AES/Token 和 Telegram webhook/sendMessage 的实现链路见 [企业微信和 Telegram Channel Adapter](docs/getting-started.md#20-企业微信和-telegram-channel-adapter)。

启用控制面管理接口：

```dotenv
TRPC_AGENT_ADMIN_ENABLED=true
TRPC_AGENT_ADMIN_TOKEN="replace-with-a-random-token"
```

Tenant、App、Revision、Channel/Backend Binding 和乐观锁发布说明见 [Admin API 和 Revision 发布](docs/getting-started.md#21-admin-api-和-revision-发布)。

Tool Catalog、租户白名单、用户权限、调用预算和危险工具审批说明见 [租户级 Tool 治理](docs/getting-started.md#22-租户级-tool-治理)。

启用 OpenTelemetry OTLP 导出：

```dotenv
TRPC_AGENT_OTEL_ENABLED=true
TRPC_AGENT_OTEL_ENDPOINT=127.0.0.1:4317
TRPC_AGENT_OTEL_SAMPLE_RATIO=1
```

HTTP、Gateway、持久化任务、Worker 的 trace 传播，运行/Admin/Tool 审计，以及 token 与租户成本指标见 [OpenTelemetry、审计和成本链路](docs/getting-started.md#23-opentelemetry审计和成本链路)。模型价格通过 Revision 的 `model_config.prompt_cost_per_million` 和 `completion_cost_per_million` 配置。

危险工具会创建可恢复的 `tool_approval`。企业微信或 Telegram 用户在原会话回复严格的 `批准 apr_xxx` / `拒绝 apr_xxx` 命令，平台会校验租户、Channel Binding、用户、有效期以及工具参数哈希，再生成幂等 continuation。详见 [可恢复的危险工具审批链路](docs/getting-started.md#24-可恢复的危险工具审批链路)。

Memory 通过 tenant-scoped `AppName` 路由到每个租户选择的 InMemory、Redis 或 PostgreSQL 后端；Revision 可开放 tRPC-Agent-Go 原生 Memory Tool，并配置自动 preload。详见 [租户级 Memory Router](docs/getting-started.md#25-租户级-memory-router)。

Artifact 通过同一 Storage Scope 路由到 InMemory 或 S3-compatible 后端。Compose 提供 MinIO，PostgreSQL advisory lock 保护多节点对同一文件的版本分配。详见 [Artifact Router 与 S3 / MinIO](docs/getting-started.md#26-artifact-router-与-s3--minio)。

Knowledge 根据 Revision 构建 InMemory 或 Qdrant Vector Store，支持 Hash/OpenAI Embedder、切块、Admin 文档导入，并在写入和搜索两端强制 tenant/app metadata。详见 [Knowledge Router 与 Qdrant](docs/getting-started.md#27-knowledge-router-与-qdrant)。

Summary、Memory Extraction、Knowledge Upsert/Delete 通过 PostgreSQL `background_job` 异步执行，支持 lease reclaim、幂等、指数退避、dead job 查询/重试和独立 `jobs` 角色。详见 [Durable Background Job](docs/getting-started.md#28-durable-background-job)。

Backend Migration 通过 `planned → dual_write → backfill → verify → cutover → completed` 状态机执行，支持 Memory/Knowledge 在线双写、分批回填、强校验、repair backlog、乐观锁切换和回滚。详见 [Backend Migration 状态机](docs/getting-started.md#29-backend-migration-状态机)。

Session Router 实现完整 tRPC-Agent-Go `session.Service`，让不同 tenant/app 选择 startup、InMemory、Redis 或 PostgreSQL，并支持 Event/State/Summary 双写迁移、批量回填和切读回滚。详见 [Tenant Session Router](docs/getting-started.md#30-tenant-session-router)。

Admin 支持多 Principal RBAC；Gateway/Worker 支持 Local/Redis 租户限流、并发和每日 token/cost 预算；标准日志和 Audit 分别执行 Secret 脱敏。详见 [RBAC、限流、预算与日志脱敏](docs/getting-started.md#31-rbac限流预算与日志脱敏)。

生产交付包含非 root Docker 镜像、独立 migration/loadgen、完整本地可观测栈，以及 Kubernetes 六角色 Deployment/HPA/PDB/NetworkPolicy。详见 [生产部署与可观测栈](docs/getting-started.md#32-生产部署与可观测栈)、[部署文档](docs/deployment.md) 和 [容量评估](docs/capacity.md)。

灰度 Revision、Model Guardrail Callbacks、Tool Execution Journal、Storage/Reply trace 和媒体安全边界见 [最终链路加固](docs/getting-started.md#33-最终链路加固)。完整本地多进程验收执行 `./scripts/e2e-multiprocess.sh`。

逐条验收映射见 [最终验收文档](docs/acceptance.md)。

停止服务：

```bash
./stop.sh
```
