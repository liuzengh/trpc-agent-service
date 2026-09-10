# 需求追踪矩阵

唯一需求来源是根目录 [`README.md`](../README.md)。实现依据当前工作树中的 Go 源码、migrations、测试、部署文件、Workflow、Admin UI 和脚本。本表区分实现、仓库证据和外部环境证据。

状态：`IMPLEMENTED` = 实现存在但证据不完整；`REPO_VERIFIED` = 可由仓库源码、静态检查、单测、集成/E2E 测试或 CI Workflow 核对；`EXTERNALLY_VERIFIED` = 仓库包含可复核的真实外部运行证据；`EXTERNAL_VERIFICATION_NOT_INCLUDED` = 外部验证不在当前仓库；`NOT_APPLICABLE` = 当前路径不适用。状态后的括号用于限定范围，例如 `EXTERNALLY_VERIFIED (Compose)` 不代表生产环境通过。

2026-09-09 已补充本机外部验收证据：真实企业微信/飞书文本收发、客户端截图、Compose 服务健康、`/readyz`、Jaeger Trace、Prometheus 指标和 Grafana 面板。证据见 [`acceptance-screenshots`](acceptance-screenshots/acceptance-status.png)。

## R1 多租户与节点部署

| ID | Requirement | Design / implementation / validation | Status |
| --- | --- | --- | --- |
| R1.1 | tenant_id、状态、审计策略 | `tenant/tenant.go`、`platform.tenant`、tenant/Admin tests | REPO_VERIFIED |
| R1.2 | Agent App、model、tool、IM、Backend、audit config | immutable `AppConfig`/`ConfigVersion`、Admin/config tests | REPO_VERIFIED |
| R1.3 | Gateway/Channel/Worker/Admin/Telemetry 协作 | `cmd/trpc-service/main.go` 的 `gateway`/`channel`/`worker`/`all` roles；Channel role owns adapters, Reply Sender and outbound resolver；Compose/Kustomize；本机 Compose 实测 | EXTERNALLY_VERIFIED (Compose) |
| R1.4 | 多节点扩展与跨节点 Session 路由 | Redis Stream claim、PostgreSQL execution lease/fence、Session lease/lock；multi-consumer/runtime/fault tests | REPO_VERIFIED |
| R1.5 | 不依赖 sticky session | Worker 无本地业务 Session；共享 Session backend + lease/lock/fence | REPO_VERIFIED |
| R1.6 | 配置、数据、工具、日志、密钥隔离 | trusted Scope、SQL predicates、ToolPolicy、SecretProvider、redaction | REPO_VERIFIED |
| R1.7 | 真实生产多节点/外部依赖隔离 | 当前只有受控清单和 deterministic tests | EXTERNAL_VERIFICATION_NOT_INCLUDED |

Gateway 可多副本；Gateway 不启动 IM 长连接。Channel Adapter 为单副本 `channel` role，`Recreate` rollout，复用同一 `gateway.Gateway` Admission，不发现具体 Worker。`all` 仅用于单进程本地部署。

## R2 数据同步与多后端支持

| ID | Requirement | Design / implementation / validation | Status |
| --- | --- | --- | --- |
| R2.1 | 租户选择并路由数据 Backend | `BackendConfig`、Session Router、Memory/Knowledge/Artifact resolvers | REPO_VERIFIED |
| R2.2 | InMemory、Redis、SQL、vector、object、external Memory 类别 | Session postgres/redis/inmemory；TencentDB、Qdrant、COS；PostgreSQL control plane | REPO_VERIFIED |
| R2.3 | Session/Memory/Summary/Artifact/Knowledge/Audit 边界 | framework Session、TencentDB、Qdrant catalog、COS metadata、`audit_event` | REPO_VERIFIED |
| R2.4 | 同一 Session 并发写一致性 | `session_lane`、turn unique constraint、Redis Session Lease/Lock、execution fence | REPO_VERIFIED |
| R2.5 | event/state/summary 顺序 | framework Session 是 transcript/state/summary authority；SQL journal 是 resume/reply projection | REPO_VERIFIED |
| R2.6 | Memory 写入后跨 Worker 可见 | TencentDB resolver 按 tenant/app/user/session 派生 key；群聊跳过无归因写入 | IMPLEMENTED |
| R2.7 | Redis Session → SQL migration | drain/copy/verify/checkpoint/cutover；migration tests/workflow | REPO_VERIFIED |
| R2.8 | vector/backend migration | 当前实现 Qdrant → Qdrant，SQL catalog inventory、point verify、checkpoint、cutover | REPO_VERIFIED |
| R2.9 | IM duplicate/replay/hash conflict | `channel_inbox` binding + external message ID + payload hash | REPO_VERIFIED |
| R2.10 | Backend 一致性、延迟、成本、运维取舍 | [backend-adaptation](backend-adaptation.md) 按真实 resolver 说明 | REPO_VERIFIED |
| R2.11 | 数据模型表达 tenant/app/session/event/memory/summary/channel/audit | SQL 实体 + framework/external logical entities；无无意义 memory/summary 表 | REPO_VERIFIED |

`Summary` 属于 framework Session state；`Memory` 属于当前 TencentDB external authority。`data-model.md` 用逻辑实体和虚线关系表达它们，不虚构平台 SQL 表。

## R3 IM 软件接入

| ID | Requirement | Design / implementation / validation | Status |
| --- | --- | --- | --- |
| R3.1 | 至少两类 IM，含微信/企业微信 | WeCom Bot WebSocket + Feishu/Lark WebSocket；真实账号收发；客户端截图/Trace/人工确认 | EXTERNALLY_VERIFIED (real WeCom + Feishu) |
| R3.2 | inbound normalize → ChannelInput/Runner | provider protocol → `ChannelInput` → Gateway Admission → framework Runner；真实入站 execution 记录 | EXTERNALLY_VERIFIED (real messages) |
| R3.3 | Agent event/接入失败 → reply | terminal event durable projection；媒体/Admission/命令失败写 `channel_failure` Outbox；Channel role 的 WeCom/Feishu Reply Sender 消费 Reply Outbox；客户端截图；真实 `SENT`/Provider receipt | EXTERNALLY_VERIFIED (real replies) |
| R3.4 | binding、secret、auth、dedupe、identity | binding scope/revision、scoped SecretProvider、官方 WebSocket/SDK protocol auth、HMAC/AEAD、inbox；真实连接状态 | EXTERNALLY_VERIFIED (real connections) |
| R3.5 | direct/group/topic Session 隔离与 `/new` | direct=user principal；group/topic=conversation principal；`conversation_session` scoped active pointer；`/new` 原子切换且不进入 Runner | REPO_VERIFIED |
| R3.6 | 长度、频率、异步、媒体、撤回、重试边界 | 32 MiB media/COS staging、Redis reply limiter、Reply Outbox、Feishu recall、adapter tests | REPO_VERIFIED |
| R3.7 | 真实第三方 IM 账号和网络可用性 | 本机真实企业微信/飞书账号、真实网络、文本收发、Provider receipt；证据见 `acceptance-screenshots` | EXTERNALLY_VERIFIED (local accounts/network) |

当前选用官方 WebSocket/long connection。README 的 webhook URL 和 HTTP callback signature verification 对当前模式为 `NOT_APPLICABLE`；不能将 WebSocket authentication 写成 webhook signature verification。

## R4 治理、监控和安全

| ID | Requirement | Design / implementation / validation | Status |
| --- | --- | --- | --- |
| R4.1 | Plugin/Guardrail/Callbacks 治理设计 | 当前可执行链路为 Callbacks、ToolPolicy、Budget、Approval、Secret scope、audit；Runtime 保留扩展边界 | REPO_VERIFIED |
| R4.2 | tool whitelist/unknown fail closed | `ToolCatalog`、executable/visible/policy checks；当前注册 `todo_write` | REPO_VERIFIED |
| R4.3 | PII/credential redaction | audit/log redaction、raw payload trace 禁止、错误截断 | REPO_VERIFIED |
| R4.4 | budget/token/cost | tenant-app scoped per-execution token limit；可选 pricing 仅估算 cost；无 daily/monthly/aggregate quota | REPO_VERIFIED |
| R4.5 | review-required approval | `tool_approval`、`WAITING_APPROVAL`、exact request/tool/argument digest | REPO_VERIFIED |
| R4.6 | IM user access policy | mapped user/conversation allowlist；拒绝也写 durable inbox result | REPO_VERIFIED |
| R4.7 | request/model/tool/IM/Session/Memory metrics | OTel metrics、operations gauges、Grafana dashboard；Prometheus target/指标和 Grafana 实测 | EXTERNALLY_VERIFIED (local observability) |
| R4.8 | callback → Runner → backend → reply trace | W3C traceparent/tracestate 随 Admission、Redis dispatch、Worker、Channel Reply Sender/Reply Outbox 传播；真实 Jaeger Trace | EXTERNALLY_VERIFIED (real traces) |
| R4.9 | README 最低 audit 字段 | `platform.audit_event` tenant/channel/user/session/agent/tool/decision/latency/error/cost/trace/request/config | REPO_VERIFIED |
| R4.10 | token/key/password 不进日志/trace/report | SecretRef/scoped env、digest、AEAD target、security scans | REPO_VERIFIED |

## R5 故障恢复与运维

| ID | Requirement | Design / implementation / validation | Status |
| --- | --- | --- | --- |
| R5.1 | node failure/duplicate delivery | XAUTOCLAIM、execution lease/run token、ACK after durable transition | REPO_VERIFIED |
| R5.2 | IM retry/DB/Redis temporary failure | bounded backoff、readiness gating、outbox/lease recovery、uncertain classification | REPO_VERIFIED |
| R5.3 | model/runtime/tool/budget failure | context cancellation、typed retryable/permanent/uncertain errors；Runner 构建最终失败持久化 terminal event；budget fail closed | REPO_VERIFIED |
| R5.4 | context/goroutine/event drain | shutdown readiness gate、stop claim、Runner event drain before Session lease release | REPO_VERIFIED |
| R5.5 | canary/config rollback | immutable ConfigVersion、stable/canary、pause/rollback/promote、migration guard | REPO_VERIFIED |
| R5.6 | capacity assessment | evaluator/observer/error gate、Worker concurrency、Session serial lane、reply limiter formulas | REPO_VERIFIED |
| R5.7 | local/recommended deployment | Compose + Kustomize Gateway/Channel/Worker + probe/HPA/PDB; deployment validation/workflow definitions；本机 Compose 实测 | EXTERNALLY_VERIFIED (Compose) |
| R5.8 | production fault/Kubernetes/provider recovery | no checked-in production drill or external report | EXTERNAL_VERIFICATION_NOT_INCLUDED |

## 交付物

| ID | Deliverable | Evidence | Status |
| --- | --- | --- | --- |
| D1 | 架构设计文档 | `docs/architecture-design.md` | REPO_VERIFIED |
| D2 | 系统架构图 | `docs/system-architecture.md`、`docs/diagrams/system-architecture.mmd/.svg` | REPO_VERIFIED |
| D3 | 核心时序图 | `docs/core-sequence.md`、`docs/diagrams/core-sequence.mmd/.svg` | REPO_VERIFIED |
| D4 | 数据模型 | `docs/data-model.md`、migrations、logical/external Memory/Summary flow | REPO_VERIFIED |
| D5 | 数据同步和幂等 | `docs/data-sync-idempotency.md`、Admission/lease/outbox/migration | REPO_VERIFIED |
| D6 | 多后端适配 | `docs/backend-adaptation.md`、resolver implementations/tests | REPO_VERIFIED |
| D7 | 至少 8 个生产风险 | `docs/risk-register.md`，16 项 | REPO_VERIFIED |
| D8 | GitHub implementation code | `cmd/`、`trpcservice/`、deployment、Workflows | REPO_VERIFIED |

## README 验收标准

| ID | Requirement | Status |
| --- | --- | --- |
| A1 | 覆盖多租户、节点化、同步、多后端、IM、治理监控、恢复 | REPO_VERIFIED |
| A2 | 数据模型表达 tenant/agent/channel/session/event/memory/summary/audit | REPO_VERIFIED |
| A3 | 至少两种 IM，含微信/企业微信 | REPO_VERIFIED |
| A4 | 至少三类后端及同步策略 | REPO_VERIFIED |
| A5 | 完整消息时序并贯穿 trace/request | REPO_VERIFIED |
| A6 | 至少 8 个风险及措施 | REPO_VERIFIED |
| A7 | 明确 tRPC-Agent-Go 复用与平台新增能力 | REPO_VERIFIED |

仓库仍没有外部生产容量、真实 Kubernetes HA、生产故障演练、生产 SecretProvider 或生产级 Provider/IM 稳定性证据；本机真实 IM/Compose/可观测性验收已记录在 `acceptance.md` 和 `acceptance-screenshots` 中。
