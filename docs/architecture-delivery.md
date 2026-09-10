# trpc-agent-service 交付架构总览

> 本文面向验收、汇报和代码评审，只陈述已实现能力与已验证边界。字段、状态机和接口以 [`docs/design/`](./design/README.md) 为准；可执行交付边界为 Docker Desktop。

## 1. 目标与总体架构

`trpc-agent-service` 建立在 tRPC-Agent-Go 公共 API 之上。Gateway、Worker 与 IM Adapter 可独立扩缩；权威状态位于 PostgreSQL，Redis 只负责队列、租约、fence 和协调，因此 Worker 无需 sticky session。

```mermaid
flowchart LR
  IM["企业微信 / 飞书 / WebUI"] --> CA["Channel Adapter"]
  CA --> IN[("Inbox / Preprocess")]
  IN --> GW["Gateway + Dispatch Outbox"]
  GW --> R["Relay / Redis Broker"]
  R --> W["无状态 Worker × N"]
  W --> RUN["Guardrail + tRPC-Agent-Go Runner"]
  RUN --> M["模型 / Tool（含固定端点 MCP）"]
  W --> S["Storage Router"]
  S --> D[("PostgreSQL / Redis\n向量库 / 对象存储")]
  W --> RO[("Reply / Audit Outbox")]
  RO --> CA
  W -. trace / audit .-> T["OTel / Audit Ledger"]
```

控制面管理 Tenant、Agent App Revision、模型/后端 Profile、工具权限、Channel Binding 和审计策略。执行固定 `tenant_id`、app、policy 与 backend 的版本快照，发布或回滚只影响后续请求。

规范来源：[组件拓扑（总体架构 §2）](./design/0.architecture.md#component-topology)、[无 sticky session 的 Worker（总体架构 §9）](./design/0.architecture.md#stateless-worker)、[Tenant/App/Config 版本模型（Tenant 根设计：第二部分 §2）](./design/1.tenant-root-design.md#version-model)、[ExecutionEnvelope（Gateway/Worker §2.1）](./design/2.gateway-worker-design.md#execution-envelope)。

## 2. 多租户、会话和数据模型

可信 `TenantContext` 只能来自认证 HTTP 身份或验签成功的 Channel Binding；Gateway、Worker、Storage Router 和审计入口均复核 tenant scope。模型、IM、数据库密钥仅以 `SecretRef` 引用，日志、trace 和审计均脱敏。

核心关系是 `tenant → agent_app → revision`，以及 `tenant + channel_binding → external_user/conversation → session → event、summary、memory`。每条入站消息对应唯一 `inbox/execution_record`，回复与审计进入 `delivery_ledger/reply_outbox` 和 `audit_log`；群聊 session 加入群标识，因此跨群、跨租户均不共享。

完整 DDL、ER 图和状态约束见详细设计。

规范来源：[可信 TenantContext（Tenant 根设计：第一部分 §7.1）](./design/1.tenant-root-design.md#trusted-tenant-context)、[SessionID 命名空间（Tenant 根设计：第一部分 §7.3）](./design/1.tenant-root-design.md#session-namespace)、[最小 SQL 数据模型与 ER 图（Storage §7）](./design/3.storage-data-consistency-design.md#sql-data-model)、[event/state/summary/Memory 更新顺序（Storage §9）](./design/3.storage-data-consistency-design.md#event-state-summary)。

## 3. 多后端与一致性策略

PostgreSQL 存强一致控制面、Session、事件、Inbox/Outbox 与审计；Redis 存 broker、lease/fence；向量库或外部 Memory 服务存知识/记忆索引；对象存储存 Artifact、图片和文件。`Storage Router` 按租户 Binding 路由。

并发 session 由 lease/fence 串行化，提交由 PostgreSQL 事务/CAS 保证。`CommitTurn` 原子写结果和 outbox；`ClaimInbox` 与 Delivery Ledger 保证入口和 IM 回复效果幂等。派生数据按版本/水位对账；Redis→SQL、Local Vector→Remote Vector 按 snapshot、dual-write、backfill、verify、cutover 迁移。

规范来源：[Storage Router（Storage §3）](./design/3.storage-data-consistency-design.md#storage-router)、[CommitTurn 原子顺序（Storage §8）](./design/3.storage-data-consistency-design.md#commit-turn)、[多后端迁移状态机（Storage §11）](./design/3.storage-data-consistency-design.md#backend-migration)、[lease/fence（Gateway/Worker §5）](./design/2.gateway-worker-design.md#lease-fence)。

## 4. IM 接入与端到端时序

飞书和企业微信 Adapter 负责验签/解密、去重、身份映射、群聊规则和平台回复，再归一为内部消息。文本、图片和文件经大小/类型校验、ClamAV 和租户隔离暂存后形成 `model.Message`。本地 WebUI 可通过签名 SSE 显示非持久的文本增量，终态仍从 Result/Outbox 读取；飞书单聊/群聊/卡片/图片与企业微信文本/图片已验证。飞书/企业微信的原生流式编辑尚未启用，安全降级为最终回复。

```mermaid
sequenceDiagram
  participant U as 企业微信用户
  participant C as WeCom Adapter
  participant P as PostgreSQL / Gateway
  participant W as Worker / Runner / Tool
  participant D as Delivery Adapter
  U->>C: callback(event_id, message)
  C->>P: 验签、Binding、request_id/trace_id、ClaimInbox
  P->>W: preprocess、Outbox、Broker 投递 trace context
  W->>W: lease/fence、Guardrail、Runner.Run、Tool
  W->>P: CommitTurn：state、summary、reply/audit outbox
  P->>D: relay claim reply
  D->>U: 平台回复；Delivery Ledger 标记 sent
```

`request_id`、W3C `traceparent` 与 tenant/app/session 属性贯穿 callback、broker、Runner、Tool、存储、审计和回复，可在 Jaeger 与审计账本还原链路。Channel 契约和限制见 [`4.channel-governance-security-design.md`](./design/4.channel-governance-security-design.md)。

规范来源：[Binding、去重与 Session 规则（Channel §4）](./design/4.channel-governance-security-design.md#channel-binding-session)、[WeCom 协议适配（Channel §5）](./design/4.channel-governance-security-design.md#wecom-adapter)、[Feishu 协议适配（Channel §6）](./design/4.channel-governance-security-design.md#feishu-adapter)、[媒体预处理（Channel §15）](./design/4.channel-governance-security-design.md#preprocess-job)、[IM 接入与租户解析时序（总体架构 §13.1）](./design/0.architecture.md#im-ingress-sequence)。

## 5. 治理、运维与风险控制

Worker 内嵌不可绕过的 Governance/Filter Chain：校验 tenant、用户、模型/工具白名单、DLP、预算和危险确认。OpenTelemetry 覆盖 callback、Runner、Tool、存储与 delivery；审计记录 tenant、channel、user、session、agent、tool、decision、latency、error、cost 和 trace ID。

| 风险 | 缓解措施 |
| --- | --- |
| Worker 崩溃 | lease 过期接管与 fence 拒绝旧 owner 提交。 |
| PostgreSQL 短断 | 未 durable 的 callback 不 ACK；事务整体回滚并由 reconciler 恢复。 |
| Redis/Broker 故障 | Outbox 积压、有界退避，恢复后幂等重放。 |
| 模型超时 | 有界重试；仅在租户策略允许时切备用模型，否则显式失败。 |
| 工具副作用不确定 | 区分幂等与非幂等工具；ambiguous 结果不盲目重放。 |
| IM 重复、乱序、429 | Inbox/Delivery Ledger 幂等键、顺序约束和 retry-after。 |
| 租户越权或密钥泄漏 | 多层 tenant 校验、SecretRef、日志/trace 脱敏和 fail-closed。 |
| 审计或遥测故障 | 审计 Outbox 可重放；非关键 telemetry 有界丢弃且独立告警。 |

Gateway、Worker、Relay 和 Adapter 可独立部署，Worker 无需 sticky session。租户灰度使用不可变快照与 active pointer；回滚以新版本 copy-forward。容量输入包括活跃 session、token、Redis/SQL QPS、积压、IM 峰值和模型并发；仓库只给出 Docker 本地验证。

规范来源：[Trace 属性与链路（Observability §2）](./design/5.observability-audit-devops-design.md#trace-design)、[AuditEvent 字段与保留（Observability §4）](./design/5.observability-audit-devops-design.md#audit-design)、[故障降级（Observability §6）](./design/5.observability-audit-devops-design.md#fault-degradation)、[灰度/回滚（Observability §8）](./design/5.observability-audit-devops-design.md#progressive-release-rollback)、[容量模型（Observability §9）](./design/5.observability-audit-devops-design.md#capacity-model)。

## 6. tRPC-Agent-Go 复用边界与验收

服务实际复用 tRPC-Agent-Go 的 Runner、Agent、Session contract、Graph checkpoint、Model、Tool、Skill、Knowledge、Callback、`telemetry/metric` 与 OpenAI/A2A/tRPC-Agent 协议公共 API。组合根将 service 的 OTel Provider 绑定到 SDK 的公开 metric 初始化入口，因此框架指标与服务指标共用同一导出管线。当前 SDK `telemetry/trace` 的公开 API 无法在复用该 Provider 时配置全局 payload drop policy，故框架内部 Agent、LLM 与 Tool 自动 span 保持禁用；服务的脱敏边界 span 是唯一 trace 语义，且不会通过 SDK `trace.Start` 创建第二套 exporter/provider。上游 `tool/mcp` client 已通过 service 的 tenant-scoped 适配层接入：仅允许经发布 ToolRef 固定的 HTTPS SSE/streamable 端点、固定远端工具名和已审阅 declaration/binding digest；stdio、动态 URL、`mcpbroker` 与通用 `mcp_call` 一律不在能力面内。Memory 与 Artifact 目前由 service-owned Store 承担多租户原子性和生命周期，`plugin.Manager` 也仅保留升级兼容锚点。平台新增可信租户控制面、Profile/Secret 路由、Inbox/Outbox/relay、lease/fence、IM Adapter、治理装配与审计；不依赖上游 `internal` 包。OpenClaw Channel 的 `ID/Run` 生命周期和 sender 语义由服务 Adapter 兼容扩展。

本地 Docker 的命令、资源与成功证据见 [`runbook/verification-matrix.md`](./runbook/verification-matrix.md)；详细规范见 [`design/`](./design/README.md)。

规范来源：[可直接复用的 tRPC-Agent-Go 能力（模块边界 §6）](./design/6.module-boundaries.md#upstream-reuse)、[service 新增平台能力（模块边界 §7）](./design/6.module-boundaries.md#platform-owned-capabilities)、[端到端 Runtime Slice（验证规范 §5）](./design/7.verification-release-design.md#runtime-slice)、[真实 IM Provider 验证（验证规范 §7）](./design/7.verification-release-design.md#real-im-verification)、[Channel 支持矩阵（能力边界 §2）](./design/8.capability-boundaries.md#channel-support-matrix)。
