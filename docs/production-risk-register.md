# 生产环境风险清单与应急缓解手册

## 1. 概述与风险分级标准

本风险清单与缓解手册针对 `trpc-agent-service` 在多节点生产环境及企业即时通讯（IM）集成部署中的高可用、数据安全、模型治理与灾难恢复场景编制。清单覆盖入站重复、底层依赖宕机、大模型失效、跨租户越权以及 goroutine / Event 流生命周期等 12 项生产风险。

### 1.1 风险影响级别定义
- **P0（致命风险）**：核心集群不可用、大规模跨租户数据泄露、核心数据库物理损坏导致无法提供服务；
- **P1（严重风险）**：部分渠道消息积压或断连、大模型推理大规模超时、会话执行锁大面积死锁导致用户端无应答；
- **P2（中度风险）**：个别租户配额打满触发限流、向量检索质量漂移、非核心日志审计积压；
- **P3（轻度风险）**：控制台偶发加载慢、非关键外部工具偶尔调用失败。

---

## 2. 生产核心风险矩阵 (Production Risk Matrix)

| 编号 | 生产风险场景 | 影响级别 | 核心触发场景 | 预防与常态化检测 | 运行时缓解与故障自愈措施 |
| :--- | :--- | :---: | :--- | :--- | :--- |
| **R-01** | **IM 重连、重投与重复应答** | P1 | WebSocket 重连后事件再次到达；Telegram offset 恢复；网络超时或用户重复点击 | 渠道 `message_id` 归一化；Redis Lua 原子租约；PostgreSQL `messages` 持久认领；Telegram offset 持久化 | 重复执行被幂等层拦截；Outbox 基于全局请求键、投递租约与外部 Receipt 闭环确认，将跨系统重复窗口降至最低，确保 At-least-once 交付可靠性。 |
| **R-02** | **Worker 异常崩溃或 GC 停顿引发脑裂 (Split-Brain)** | P1 | Worker 发生 Full GC 停顿或 OOM 崩溃；Kubernetes 强制逐出 Pod | 引入带单调递增 `fencing_token` 的分布式排他租约；后台协程按 `TTL/3` 心跳保活；事务最终提交强制比对令牌 | 老 Worker 恢复后尝试提交事务时被 DB 强制拒绝 (`ErrSessionExecutionLeaseLost`)；新 Worker 超时安全接管，消除数据脏覆写。 |
| **R-03** | **事务发件箱 (Outbox) 重试风暴与外部 IM 频控** | P1 | 外部 IM 网关抖动、429/频控、连接短暂失效 | `SKIP LOCKED` + Delivery Lease 控制并发；ChannelDeliveryPolicy 负责分段和节流；记录尝试次数与最后错误 | 失败后按指数退避重新设置 `available_at`（最大 300 秒）；物理 Receipt 确认成功。发件箱异常独立治理，不污染上游消息队列死信。 |
| **R-04** | **跨租户数据越权与凭据非法穿透** | P0 | 开发者漏写 SQL 租户过滤条件；多租户上下文混淆 | PostgreSQL 表强制启用 RLS；控制表绑定 `app.tenant_id`，框架 Session/Memory 表绑定 `app.app_name`；租户事务由 `dbscope` 统一创建 | RLS 拒绝越权 DML；立即停止受影响应用/入口并按 `trace_id`、审计事件和数据库日志回溯，不通过关闭 RLS 临时绕过故障。 |
| **R-05** | **核心存储依赖 (PostgreSQL / Redis / Kafka) 宕机** | P0 | 物理机故障、磁盘写满、主从网络分区 | `/readyz`、连接池指标、Backend Health 熔断、Store OTel 指标、Kafka Lag 告警 | 依赖不可用时明确失败或停止认领新工作，不伪造“只读成功”；Kafka 在未提交 Offset 的情况下可重新消费，Outbox 在数据库恢复后继续认领积压事件。 |
| **R-06** | **大模型接口抖动、超时或 Token 预算失控** | P1 | Provider 限流、网络失败、Token 消耗超过租户预算 | Context 超时/取消；`requests_per_minute`、`max_concurrent_runs`、`token_budget_per_hour`、`token_reservation`、`max_tool_calls`；模型 Failover 与 Backend Health | 超限在模型调用前拒绝；瞬态 Provider 故障按候选模型 Failover；持续异常触发熔断，避免每个请求都打到已知故障端。 |
| **R-07** | **工具越权、恶意外网出网与参数注入** | P0 | Prompt 注入诱导危险 Tool/MCP；HTTP Tool 访问内网或带出凭据 | 发布时 `PlatformPolicyValidator` 校验工具目录/Secret；Runner 装配仅暴露允许工具；HTTP/MCP 出站执行 URL/Host 安全策略 | Tool Callbacks / Guardrail / Approval 运行时动态校验调用权限与用户二次确认；依据配置标记与修改型操作特征，对外部调用建立原子审计 Ledger，对于网络异常导致的结果未知状态（outcome_unknown）严格限制自动重试。 |
| **R-08** | **敏感个人数据 (PII) 与密钥泄漏至日志/链路追踪** | P1 | Prompt、Tool 参数或 Provider 错误中包含敏感内容 | 密钥只通过受管 `env:` 引用在构造依赖时解析；Audit 的 detail 使用摘要/脱敏值；Execution Trace 持久化投影主动丢弃模型/工具 input、output 与 raw error；Metric label 不包含正文 | 触发凭据紧急轮换，清理受污染的外部可观测副本；依据 `tenant_id + trace_id` 回溯审计。业务对话正文仅由租户会话服务加密落库，Trace / Metric / Audit 仅记录结构化元数据与脱敏摘要。 |
| **R-09** | **Session / Knowledge 跨后端迁移导致数据不一致** | P1 | Session 双写某一端失败；向量重建不完整 | Session 使用 8 状态迁移和 repair 队列；Knowledge 以 PostgreSQL 权威源重新索引，不复制引擎内部索引 | Repair 未清零不得切读/完成；Knowledge 目标索引验证失败则不切 Profile。Summary 为派生数据，切换后重建。 |
| **R-10** | **滚动发布或 Schema 变更导致服务不可用** | P1 | Migration 锁表；新 Pod 未 Ready；应用配置候选存在缺陷 | 数据库 Migration Job 与 Deployment 分离；PDB、探针、滚动参数；Agent 应用使用不可变版本 + candidate rollout | 二进制滚动失败由 Kubernetes 自动暂停并维持旧版本可用；Schema 演进遵循向前兼容原则，遇到异常通过向前修复补丁处理。Agent 配置可立即终止灰度、丢弃候选或回退至指定历史快照。 |
| **R-11** | **知识库向量索引损坏或 Embedding 模型变更导致 RAG 失效** | P2 | 向量数据库索引分片损坏；业务方更换 Embedding 模型 | 确立“向量数据为派生索引”的原则；PostgreSQL `knowledge_document_sources` 保存权威源文档 | 无需依赖向量库内部索引做恢复；从权威源重新切片/Embedding 到目标 VectorStore，完成数量与可检索性验证后再切换 Profile，并保留源索引作为回滚窗口。 |
| **R-12** | **Context / goroutine / Runner Event 泄漏** | P1 | 客户端取消 SSE、模型流中断、Worker Drain、后台续租 panic | 所有 I/O 接收 `context.Context`；后台任务使用 `safego`；Lease heartbeat 有显式 `Stop/Done`；Worker 有 Drain 状态 | 调用方提前结束时排空 Runner Event；取消会向下传播到模型/存储/发送；panic 被转为组件错误而不是静默杀死后台协程。 |

---

## 3. 核心生产风险详细自愈指南

### 3.1 风险 R-01 自愈：处理重复入站与重复外发
1. **现象**：同一渠道 MessageID 被多次接收，或者用户看到两条相同最终回复。
2. **定位**：按 `tenant_id + channel + binding_id + message_id` 查询 `messages`、Audit 与 Outbox；同时查看 Kafka Lag、Redis 幂等状态和 Outbox `delivery_attempts / delivery_receipt`。
3. **判断边界**：若 Agent 只执行一次而 Outbox 有多次投递尝试，重点排查“Provider 已成功但 Receipt 落库失败”的外部副作用窗口；若 Agent 本身重复执行，则检查 Redis Lease、PostgreSQL execution dedup 和 Kafka retry/offset。

### 3.2 风险 R-04 自愈：跨租户数据隔离安全告警
1. **现象**：PostgreSQL 返回 RLS/权限错误，或 `platform.store.failures` 在某个租户路径异常上升。
2. **处置步骤**：
   - 第一时间停止受影响应用或 Channel Connector 的新流量；
   - 查看 PostgreSQL 日志中的会话变量设置，排查平台表是否缺少 `app.tenant_id`、框架 Session/Memory 表是否缺少 `app.app_name`；
   - 检查 `tenant_members` 表对应的角色授权缓存；
   - 审计对应 `trace_id` 产生的出站消息与日志，确认是否有实际跨租户信息流出，如有立即通知安全与合规团队。

### 3.3 风险 R-09 自愈：存储迁移异常回滚
1. **现象**：`session_backend_migrations` 表的 `phase` 停留在 `verify` 阶段，且 `last_error` 报校验和不一致。
2. **回退操作**：
   - 通过管理 API / Migration Store 的带 generation 更新执行 `rolled_back` 转换，不直接手写 SQL 绕过并发控制；
   - 状态路由恢复为源后端读写；
   - 处理 `session_migration_repairs` 中未完成项，并确认 repair 清零后再结束本次迁移或重新发起。
