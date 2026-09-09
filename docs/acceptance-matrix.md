# 需求验收矩阵

> 双 IM 证据分为两层：企业微信、飞书协议自动化验收已纳入 CI；真实平台 E2E 需要在目标环境
> 由部署方完成人工验收；CI 中的
> `scripts/dual_im_contract_acceptance.sh` 使用纯合成凭据重放两种加密回调，并贯穿
> Inbox、Worker、离线 Runner、Session/Memory/Summary、Outbox 和模拟平台 API。
> 真实平台验收用于证明外部平台可达，协议自动化让评审者无需真实账号或消息正文即可复现协议与平台链路。

本文把题目要求映射到当前代码、设计文档和可复现证据。状态中的“已实现”表示存在可运行代码和自动化测试，不等同于真实平台、真实生产容量或生产 Kubernetes 集群认证。

## 多租户与节点部署

| 验收要求 | 状态 | 实现与验证证据 |
| --- | --- | --- |
| 租户、应用、模型、工具、IM、后端和审计策略 | 已实现 | `trpcservice/tenant/model.go`；`TestLoadValidConfiguration`、`TestRuntimeSnapshotReturnsDefensiveCopies` |
| LLM、Chain、Parallel、Cycle、Graph 编排 | 已实现 | 声明式、版本固定 Runtime；`TestWorkflowValidation`、`TestBuildAgentSupportsPublishedWorkflowTypes` |
| Gateway、Worker、Channel、Storage、Admin、Telemetry 协作 | 已实现 | `docs/architecture.md`、`cmd/trpc-service/production.go`；`TestTwoTenantTwoWorkerEndToEndToolMemoryOutboxAndTrace` |
| Gateway/Worker 水平扩展 | 已实现 | Redis Streams、PostgreSQL Inbox、Worker Registry；`TestRedisStreamDistributesWorkAcrossNodes`、`scripts/multinode_acceptance.sh` |
| 正确租户和 session 路由 | 已实现 | 服务端 Channel Binding 与 canonical user/session；`TestClientCannotChooseSessionOrTenant` |
| 不依赖 sticky session | 已实现 | 共享 Session/Memory、session lease/fence；`TestRedisCoordinatorMonotonicFenceAndCompareRelease` |
| 配置、数据、工具和身份隔离 | 已实现 | 租户前缀主键、版本化 Bundle、Tool Filter、IM ACL；`TestBundlesIsolateSameUserAndSessionAcrossTenants` |
| Secret 与日志脱敏 | 已实现 | 生产配置和运行时强制 Vault/KMS `tenant_id/app_id/...` namespace；旧发布版本读取时再次校验；Runner GovernancePlugin 与泄漏测试 |

## 数据同步与多后端

| 验收要求 | 状态 | 实现与验证证据 |
| --- | --- | --- |
| Session、Memory、Summary、Artifact、Knowledge、Audit 分域抽象 | 已实现 | `trpcservice/storage`、`trpcservice/audit`、`docs/data-model.md` |
| 多节点并发写同一 session | 已实现 | lease、单调 fencing token、事务提交；`TestSQLFenceGuardTurnOrderAndStaleCommit` |
| Event → state → summary/memory 顺序与崩溃恢复 | 已实现 | Inbox `runner/derived/outbox` durable stage、原子写入和派生幂等键；`TestProcessorRecoversRunnerResultWithoutSecondModelCall`、`TestAtomicIdempotencySummaryAndMemory` |
| Memory 跨节点可见 | 已实现 | PostgreSQL 或外部 Memory；双租户双 Worker E2E 和 External Memory 测试 |
| 后端迁移 | 代码路径已实现 | checkpoint、lease、checksum、双写、verify、cutover；Migration Worker 单元测试 |
| Redis ↔ PostgreSQL Session、PGVector ↔ Qdrant、S3 ↔ PostgreSQL | 分层验证 | Redis/PostgreSQL 有临时容器集成测试；PGVector/Qdrant 和 S3/PostgreSQL 仅有适配、迁移代码与单元测试，真实后端待目标环境验证 |
| IM 重投幂等 | 已实现 | 租户/绑定/外部消息唯一 Inbox；`TestConcurrentDuplicatesHaveOneWinner` |
| 一致性取舍和最小表结构 | 已完成 | `docs/architecture.md`、`docs/storage-migrations.md`、`migrations/*.sql` |

生产 Runtime 支持 PostgreSQL/Redis Runner Session/Summary、PostgreSQL/外部 Memory、PGVector/Qdrant Knowledge、PostgreSQL/S3 Artifact，以及 PostgreSQL Audit + 外置 WORM。Redis Session 使用 tenant/App 派生的物理 key prefix，集成测试验证两个节点可见且不同租户隔离；平台 Event/state/fencing 仍以 PostgreSQL 为强一致事实流。配置层保留的其他枚举用于离线模式或后续 Adapter，生产发布门禁不会让未实现组合进入运行态。

## IM 接入

| 验收要求 | 状态 | 实现与验证证据 |
| --- | --- | --- |
| 至少两类 IM，且包含企业微信 | 已实现 + 协议自动化通过 | 企业微信、飞书 Handler/Sender；`TestDualIMContractE2E` 可自动重放完整合成链路；真实账号验收由部署方执行 |
| IM 消息转 Runner、Event 转回复/卡片 | 已实现 | Callback → Inbox → Runner → Outbox → Sender；Channel/Sender 测试 |
| 验签、解密、绑定、去重、身份映射 | 已实现 | 动态 Binding Provider 与 canonical identity；企业微信/飞书 Handler 测试 |
| 群聊/单聊 session 和跨租户隔离 | 已实现 | `dm/{binding}/{user}`、`group/{binding}/{conversation}`；跨通道身份测试 |
| 长度、限流、异步回复和失败重试 | 已实现 | UTF-8 分片、Redis limiter、Outbox retry/DLQ；Delivery 测试 |
| 图片、文件和飞书卡片 | 已实现基础闭环 | 受控下载、限制、临时清理、文本提取、多模态输入；Media/Runtime 测试 |

微信公众号或微信客服不是基础验收必需项：企业微信加飞书已经满足“两类 IM 且至少包含微信或企业微信”。

## 治理、监控与安全

| 验收要求 | 状态 | 实现与验证证据 |
| --- | --- | --- |
| 工具白名单、ACL、预算、危险工具确认 | 已实现 | Policy Engine、Tool wrappers、共享 Budget/Approval Store；Policy/Tool 测试 |
| 租户级审计 fail-open/fail-closed | 已实现 | 稳定 audit ID、complete 前同步审计、失败重试恢复；`TestProcessorFailClosedAuditRetriesWithoutSecondModelCall`、`TestRoutedStoreFailsClosedAfterPrimaryCommit` |
| Gateway 跨节点入口限流与 HTTP 边界 | 已实现 | Redis `(tenant,binding)` 限流、429、生产 timeout/header 上限；`TestRedisAdmissionLimiter`、`TestServerAppliesProductionTimeoutDefaults` |
| 请求、模型、工具、投递、成本和存储指标 | 已实现 | OpenTelemetry Metrics；Metrics/Storage observer 测试 |
| Callback 到 Outbox 完整 Trace | 已实现 | W3C trace context 经 Inbox/Streams 传播；双租户双 Worker E2E |
| 完整审计字段 | 已实现 | `audit_logs` 包含成本、配置/策略版本和 trace；Audit 测试 |
| Secret/正文不进入日志、Trace、错误 | 已实现 | 字段白名单、标识 hash、Redactor、脱敏验收脚本 |
| 错误率、DLQ、积压、无 Worker、数据库和 usage 缺失告警 | 已实现 | `deploy/prometheus-alerts.yml`、`scripts/observability_acceptance.sh` |

## 故障恢复与运维

| 验收要求 | 状态 | 实现与验证证据 |
| --- | --- | --- |
| Worker 崩溃与 lease 接管 | 已实现 | Inbox claim lease、consumer group、fencing；Inbox Poller 与多节点验收 |
| IM、数据库、模型和工具故障降级 | 已实现基础闭环 | 分类重试、DLQ、Outbox、timeout/cancel；Delivery/Recovery 测试 |
| Context、goroutine 生命周期和事件排空 | 已实现 | Managed Runner、bounded drain、组件 Close；Runtime/Queue drain 测试 |
| 灰度、版本固定和租户回滚 | 已实现 | 不可变配置、旧 Bundle drain、Admin rollback；Runtime switch 测试 |
| 容量评估 | 已实现工具；目标环境待实测 | `cmd/capacity`、`docs/capacity.md` |
| 最小 Compose 部署 | 已实现 | `docker-compose.yml`、生产/多节点验收脚本 |
| Kubernetes 推荐拓扑 | 清单与验收脚本已实现 | Kustomize、多副本、PDB/HPA；真实 kind/生产集群验收由部署方执行 |

## 交付物

| 交付物 | 位置 | 状态 |
| --- | --- | --- |
| 中文架构设计、系统架构图和核心时序图 | `docs/architecture.md` | 已完成 |
| 数据模型 | `docs/data-model.md`、`migrations/` | 已完成 |
| 同步、幂等和多后端方案 | `docs/message-runtime.md`、`docs/storage-migrations.md` | 已完成 |
| 至少 8 项生产风险 | `docs/risks.md` | 已完成（18 项） |
| GitHub 实现代码 | 当前仓库 | 已完成 |

## 后置能力

具体云厂商 SDK（当前提供 Vault KV v2/KMS-compatible HTTPS 协议适配）、队列自定义指标 HPA、复杂 PDF/OCR、更多微信产品接入和真实生产规模压测属于后续增强。Kubernetes 清单可离线校验，真实 kind/生产集群结论必须由部署方复验，不能视为云上生产集群认证。

平台事件、Memory、Outbox 和 Audit 通过稳定业务键幂等；`runner_committed` 之后的接管不会重复模型或 Tool。模型/Tool 成功但该阶段尚未落库的极窄窗口仍是 at-least-once，因此 MCP 发布要求 `idempotent: true`，副作用业务工具必须接受稳定 `X-Idempotency-Key`。这项责任边界不是未完成项，而是跨外部系统无法共享数据库事务时必须显式满足的生产契约。

可复现门禁入口包括 `./check.sh`、`scripts/dual_im_contract_acceptance.sh`、`scripts/coverage_acceptance.sh`、`scripts/multinode_acceptance.sh`、`scripts/kubernetes_acceptance.sh` 和 `scripts/observability_acceptance.sh`。运行结果应使用仓库中的脱敏模板保存到受控验收系统，不把易过期的本机快照作为当前代码结论。
