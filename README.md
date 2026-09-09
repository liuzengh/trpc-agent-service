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

## 真实模型与 IM 凭据配置

需要联调真实模型、Telegram Bot 或企业微信智能机器人时，先从仓库模板创建本地配置：

```bash
cp .env.example .env.local
```

编辑 `.env.local`，填写以下七个环境变量：

```bash
# Telegram Bot
TRPC_TELEGRAM_BOT_USERNAME=
TRPC_TELEGRAM_BOT_TOKEN=

# 企业微信 API 模式智能机器人
TRPC_WECOM_BOT_ID=
TRPC_WECOM_BOT_SECRET=

# OpenAI-compatible model
OPENAI_BASE_URL=
OPENAI_API_KEY=
OPENAI_MODEL=gpt-5.6-luna
```

Telegram 的 username 和 token 从 [BotFather](https://t.me/BotFather) 创建的 Bot 获取，username 不包含开头的 `@`。企业微信使用 API 模式智能机器人的 BotID 和长连接 Secret，不使用自建应用的 CorpID、AgentID、应用 Secret、Access Token、EncodingAESKey 或 HTTP 回调配置。模型配置支持 OpenAI-compatible 服务；`OPENAI_MODEL` 应填写该服务实际提供的模型名，运行 `stage7-live-model-smoke.sh` 时必须为 `gpt-5.6-luna`。

`./start.sh` 会自动加载根目录 `.env.local`。该文件包含真实密钥，已被 `.gitignore` 忽略，任何情况下都不得提交；可提交的 `.env.example` 只能保留空值和说明。

最短真实消息验收流程：

1. 准备外部 subject：Telegram 私聊使用数字 `chat_id`（也可用 sender user ID 回退），企业微信单聊使用成员 `userid`。
2. 执行 `./build.sh && ./start.sh`，打开 `http://127.0.0.1:8080/`；先创建并激活一个真实模型 Deployment，再在“IM 通道”中把外部 subject 绑定到目标 Tenant 和 Agent App，确认对应 Provider 为 `connected`。
3. 从 Telegram 或企业微信真人客户端向 Bot 发送一条唯一测试文本，确认客户端收到 Agent 回复，并在“IM 通道”中看到 `delivered`，在 Session/Audit 中看到同一 `request_id` 对应的 `channel.reply` 和 `run.completed`。
4. 验收结束后执行 `./stop.sh`。本项目 2026-09-09 的非敏感实测证据见 [真实 IM 消息 Smoke 记录](docs/acceptance/live-im-smoke-2026-09-09.md)，更详细的 Provider 配置与路由说明见 [Stage 4 IM Providers](docs/stages/stage-4-im.md)。

## 快速开始

```bash
git clone https://github.com/liuzengh/trpc-agent-service.git
cd trpc-agent-service

./build.sh
./start.sh
```

服务启动后打开 `http://127.0.0.1:8080/` 使用 Management Console。Stage 1 默认启用仅供本地开发与自动化验收使用的 Development Identity；它不是生产认证方案。

前端开发模式：

```bash
# 终端 1
go run ./cmd/control-migrate
go run ./cmd/trpc-service

# 终端 2，/api 会代理到 127.0.0.1:8080
cd frontend
npm ci
npm run dev
```

## 当前实现能力

当前代码已覆盖 Stage 7 最终可运行验收范围：

- **多租户管理**：Development Identity、租户切换、Agent 应用、部署版本与状态流转、Gateway/Worker 状态。
- **存储与数据管理**：租户级 Backend Profile 可分别路由 Session/Summary、Memory、Knowledge 和 Artifact，支持 InMemory/Redis/SQLite/PostgreSQL、Qdrant Knowledge 索引、S3 Artifact 内容、外部 Memory、事件回放、迁移与数据检查页面。
- **Chat Workspace**：在 Management Console 中创建/打开租户隔离 Session、读取后端历史、发送消息、取消运行、失败重试和刷新恢复；浏览器仅保存最近打开的 Session ID，不保存会话历史。
- **Mock IM 通道**：提供租户/Session 绑定、HMAC 回调验签、用户与会话映射、消息去重、provider sequence 乱序拒绝、回复投递和可配置故障注入。
- **真实 IM Provider**：Telegram Bot 使用 long polling/`sendMessage`，企微 API 模式智能机器人使用 WebSocket `aibot_subscribe`/`aibot_msg_callback`/`aibot_respond_msg`；两者通过持久化 Bot Tenant Allowlist 进行租户与 Agent App 路由。
- **SSE 契约**：`event_id`、`request_id`、`session_id`、单调 `sequence`、`type`、`data` 稳定 envelope，覆盖 `run.started`、`message.delta`、`message.completed`、`run.failed`、`run.cancelled`、`run.completed`。
- **生产身份与授权**：显式 production 模式验证 HS256 JWT 的签名、issuer、audience、expiry 和 subject，并只接受服务端 Identity Directory 中的 Tenant/Role 分配；Management Console 使用短期 HttpOnly Session。
- **治理与安全**：Tenant/Agent App 策略覆盖 Tool/MCP allowlist、输入输出 Guardrail、危险 Tool 二次确认、外部 IM 用户/会话授权、脱敏、预算和 Tenant 限流，并在 Runner 执行前生效；拒绝直接终结请求，Worker 在 Tool 执行中断连会记录 `outcome_unknown` 且禁止自动重放。
- **审计与可观测性**：提供持久化 Audit Event 查询、Tenant 指标与成本、以及按 `request_id`/`trace_id` 检索的完整平台链路视图；管理界面提供策略、确认、审计和指标/Trace 工作流。
- **Gateway/Worker 部署**：`TRPC_SERVICE_ROLE` 支持独立 Gateway/Worker 进程；Worker 内部接口同时校验 Bearer Token、短期 HS256 Execution Manifest、不可变 Version、fencing token 和 W3C `traceparent`。
- **持久控制面与多 Gateway**：开发使用迁移后的 SQLite，Compose/生产使用 PostgreSQL；两 Gateway 共享 Tenant/App/Deployment/Version/Channel Binding、Backend Selection、Governance Policy 和配置幂等状态，并通过 PostgreSQL Session Lease 与 fencing token 串行化同一 Session。
- **真实模型与数据上下文**：Deployment Version 引用服务端 `default-openai` Profile；兼容 Chat Completions，并为 `gpt-5.6-*` 使用 Responses 流式 API；Memory/Knowledge 进入后续 Agent 输入，执行结果产生带 request/trace 关联的 Artifact 元数据。
- **故障恢复与运维**：优雅排水、组件与依赖健康、服务端运行超时、存储超时/不可用/关闭分类、Worker/依赖重启恢复、事件排水和终端事件唯一性。
- **灰度与容量**：Deployment 灰度状态、确定性请求路由、回滚预览/确认，以及覆盖每节点 Session、平均 Token、Redis/SQL QPS、IM 回调峰值与安全余量的有界容量评估；高风险操作均要求角色、确认和 Audit Event。
- **Compose 恢复证据**：一键从零启动双 Gateway/Worker/Redis/PostgreSQL，并复现强制 lease loss/fencing、危险 Tool 批准与拒绝、Governance outage、Worker 执行中断连与 `outcome_unknown`、PostgreSQL 中断恢复、模型超时、Tool 故障、IM 重试与重复回调。

企微不使用自建应用，不接受 CorpID、AgentID、应用 Secret、Access Token、EncodingAESKey 或传统 HTTP 回调配置。真实凭据仅从被忽略的 `.env.local` 读取；自动化验收使用本地协议 fixture，不消费真实消息。

最终中文交付物已按验收项拆分，统一入口见 [`docs/README.md`](docs/README.md)：

1. [架构设计文档](docs/architecture.md)
2. [系统架构图](docs/system-architecture-diagram.md)
3. [企业微信核心时序图](docs/core-sequence-diagram.md)
   - [IM Channel Adapter 设计与实现边界](docs/im-channel-adapter.md)
4. [数据模型设计](docs/data-model.md)
5. [数据同步与幂等策略](docs/data-sync-idempotency.md)
6. [多后端适配方案](docs/backend-adapters.md)
7. [生产风险清单](docs/production-risks.md)
8. [GitHub 实现代码详解](docs/implementation-details.md)

阶段记录、验收材料和调研资料已归入 `docs/` 子目录，仅用于追溯，不作为最终交付物的替代。

最终验收命令：

```bash
./scripts/stage7-acceptance.sh
```

单独运行双 Gateway Compose 或真实模型 smoke：

```bash
./scripts/stage7-compose-acceptance.sh
./scripts/stage7-live-model-smoke.sh
```

`build.sh` 要求已安装 Node.js、npm 和前端依赖，依次构建 React 前端和 Go 二进制。生产前端资源会嵌入 `bin/trpc-service`，不需要单独部署静态站点。

停止服务：

```bash
./stop.sh
```
