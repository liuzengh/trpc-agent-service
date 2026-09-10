# 项目交付基线：多租户节点化 Agent 部署平台

> 本页把项目目标、交付物和已完成验收收敛为一份可追溯基线。具体运行命令和专项证据见根目录 README 与各开发文档。

## 背景和价值

企业在落地 Agent 应用时，通常不会只部署一个单体机器人，而是希望面向多个部门、多个业务线、多个 IM 入口和多个数据后端，构建一套可统一管理的 Agent 平台。例如：客服团队希望把 Agent 接入企业微信，研发团队希望接入内部群机器人，运营团队希望接入微信公众号或微信客服，不同租户又需要隔离会话、记忆、知识库、工具权限和审计日志。

[tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) 已经具备 Agent 编排（LLMAgent / GraphAgent / Chain / Parallel / Cycle）、Tool / MCP、Session、Memory、Knowledge、Artifact、Plugin / Guardrail、Telemetry、HTTP 服务化（OpenAI-compatible / AG-UI / A2A）、OpenClaw / IM 通道等能力。本任务要求基于这些能力设计一个“多租户、可节点化部署、支持多后端数据同步、可接入微信 / 企业微信等 IM 软件”的生产级方案。

这个项目要解决的业务痛点是：把单点 Agent 能力扩展成平台化服务，同时满足租户隔离、弹性部署、数据一致性、IM 触达、审计合规和后端可替换要求。

## 任务描述

设计一个基于 tRPC-Agent-Go 的多租户节点化 Agent 部署平台。平台需要支持多个租户创建和部署自己的 Agent，每个租户可以绑定不同 IM 通道、选择不同数据后端、配置不同工具权限和知识库，并允许多个 Agent 节点水平扩展。系统需要考虑跨节点会话路由、数据同步、后端适配、IM 消息接入、监控审计和故障恢复。

任务以架构设计、可运行代码和部署验证为主，关键 Go 接口、数据模型、运行时边界和 E2E 入口均已落地。

## 具体要求

### 多租户与节点部署

- 设计包含 `tenant_id`、应用配置、模型配置、工具权限、IM 通道配置、数据后端配置和审计策略的租户模型。
- 说明 Agent Gateway、Agent Worker、Channel Adapter、Storage Adapter、Admin API、Telemetry Collector 的职责与协作关系。
- 支持多节点水平扩展，说明消息如何路由到正确租户和正确 session。
- 说明是否需要 sticky session；如果不需要，说明如何依赖共享 Session / Memory 后端实现无状态 Worker。
- 设计配置、数据、权限、日志、密钥和工具调用的租户隔离机制。

### 数据同步与多后端支持

- 支持租户选择 InMemory、Redis、SQL、向量库、对象存储或外部 Memory 服务等后端。
- 设计 Session、Memory、Summary、Artifact、Knowledge、Audit Log 的统一访问抽象与存储布局。
- 覆盖同一 session 并发写入、事件顺序、Memory 跨节点可见性、后端迁移和 IM 重复投递幂等。
- 说明 Redis、SQL、向量库和对象存储之间的强一致、最终一致、延迟、成本与运维取舍。
- 给出 tenant、agent、session、message/event、memory、summary、channel binding、audit log 的最小数据模型。

### IM 软件接入

- 至少设计两类 IM Channel Adapter，其中包含微信或企业微信。
- 说明外部消息到 `model.Message` / `runner.Runner.Run` 的转换，以及 Agent Event 到文本、流式或卡片回复的转换。
- 设计 webhook、账号绑定、验签、去重、用户身份映射、群聊/单聊 session ID 和跨租户隔离。
- 处理消息长度、频率限制、异步回复、媒体消息、撤回和失败重试等平台差异。

### 治理、监控和安全

- 使用 Plugin / Guardrail / Callbacks 实现租户级工具白名单、敏感信息脱敏、预算限制、危险操作确认和用户权限校验。
- 设计请求量、模型/工具耗时、IM 投递、错误率、token、成本和后端延迟等指标。
- 使用 OpenTelemetry 或等价方案串联 IM callback、Runner、Tool、Session / Memory 和 IM 回复。
- 审计日志至少包含 `tenant_id`、`channel`、`user_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency`、`error_type`、`cost`、`trace_id`。
- 明确 IM token、模型 key、数据库密码不能出现在日志、trace 或错误报告中。

### 故障恢复与运维

- 设计节点故障、IM 重试、数据库不可用、模型超时和工具失败时的降级策略。
- 说明 `context.Context` 取消、goroutine 生命周期和 Runner 事件通道排空规则。
- 说明灰度发布、租户级配置回滚和容量评估方法。
- 提供 Docker Compose、Kubernetes 或等价的最小可运行部署方案与生产推荐方案。

## 交付物

- 架构设计文档（建议 2000–4000 字）。
- 展示 Gateway、Worker、Channel、Storage、Guardrail、Telemetry、数据库和 IM 平台关系的架构图。
- 展示“企业微信用户发消息 → Agent 执行 → Tool 调用 → Session / Memory 写入 → IM 回复”的核心时序图。
- 数据模型、数据同步与幂等策略、多后端适配方案。
- 至少 8 项生产风险及缓解措施。
- 基于该设计的 GitHub 实现代码。

## 题目难点

- 多租户隔离不仅是增加 `tenant_id`，还涉及配置、权限、密钥、数据、日志、工具与成本。
- Worker 应尽量无状态，但 Agent 依赖 Session、Memory、Summary 和工具上下文，需要可靠共享状态层。
- IM 通道存在乱序、重复、超时、长度限制和身份映射问题，不能简单等同于 HTTP chat API。
- Redis、SQL、向量库和对象存储的一致性能力不同，不能使用同一同步策略。
- Agent 执行跨越模型、工具、MCP、知识库和外部系统，监控与审计必须跨组件串联。
- 企业平台还必须考虑灰度、回滚、限流、成本控制和合规审计。

## 已完成验收

- [x] 架构方案覆盖多租户、节点化部署、数据同步、多后端、IM 接入、治理监控和故障恢复。
- [x] 数据模型表达 tenant、agent、channel binding、session、event、memory、summary、audit log 的关系。
- [x] 企业微信自建应用、企业微信 AI Bot 和 Telegram 通道的接入差异已固化，并提供 deterministic/live E2E 入口。
- [x] PostgreSQL、Redis、InMemory、S3-compatible runtime capability 的存储与同步策略已落地并有契约测试。
- [x] 企业微信用户发消息到 Agent 执行、Tool/Storage、Reply Outbox 和 IM 回复的完整时序与 `trace_id`/`request_id` 传播已验证。
- [x] 生产风险清单包含跨租户、Secret、重复投递、CAS/fencing、迁移、模型、Tool、指标、回滚、重试和生命周期风险及恢复措施。
- [x] tRPC-Agent-Go 复用边界与新增平台层模块已在架构、包边界和运行时文档中明确。

统一验收命令：

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
python -m mkdocs build --strict -f docs/mkdocs.yml
./scripts/validate-deployment.sh
```

## tRPC-Agent-Go 能力对照

| 平台需求 | 可复用的框架能力 | 需要新增的平台层 |
| --- | --- | --- |
| Agent 编排 | `agent/llmagent`、`agent/graph`、Chain / Parallel / Cycle | 租户级 Agent 注册、发布与路由 |
| 执行入口 | `runner.Runner`、流式 Event、context 取消 | 多租户 Worker 调度与水平扩展 |
| Session / Memory / Artifact / Knowledge | 对应服务接口与多后端实现 | 租户级后端选择、隔离与迁移 |
| Tool / MCP / Skill | Tool、MCP、Skill | 租户工具白名单与密钥注入 |
| 治理 | Plugin / Guardrail / Callbacks | 租户策略、预算与审批 |
| 服务化 | `server/*`、AG-UI、A2A、OpenClaw | 统一 Gateway 与 Admin API |
| IM 接入 | OpenClaw Gateway / Channel 模型 | 微信、企业微信等通道绑定 |
| 可观测性 | OpenTelemetry tracing / metrics | 租户维度审计、成本与合规 |
