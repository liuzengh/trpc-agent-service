# 赛题需求验收矩阵

本矩阵逐项对应 `赛题.md`。它同时回答两个不同问题：**当前最小实现能验证什么**，以及**生产目标如何设计**。状态定义：

- **已实现**：当前 `solution/` 有可运行路径，核心行为可由自动测试或接口验证。
- **部分实现**：已有主要接口/单机路径，但持久化、多节点、平台能力或生产闭环不完整。
- **生产设计**：本文档已经给出可实施方案，但当前代码没有对应生产 Adapter/基础设施；不得按已实现宣传。
- **不适用代码**：题目要求的是设计/取舍说明，文档本身即交付物。

文档入口：

- [architecture.md](architecture.md)：租户模型、拓扑、路由、隔离、部署与配置版本。
- [data-consistency.md](data-consistency.md)：统一数据抽象、并发顺序、幂等、迁移和表结构。
- [im-adapters.md](im-adapters.md)：Telegram/Slack 映射、验签、身份、平台限制和回复能力。
- [security-operations.md](security-operations.md)：Filter、指标、trace、审计、密钥、故障、灰度和容量。

## 2026-09-11 修复补充

R1 工具权限审计事件身份、R2 输出候选安全决策、R3 企微连接发布生命周期已修复，新增真实 SQL 审计/安全决策回归及连接轮换、失败、回滚测试。HAProxy liveness 与 readiness 路由已分开，数据库断连不会再使 `/healthz` 随业务上游一起下线。证据与明确限制见 [修复记录](review-2026-09-11.md)。下文历史设计矩阵中的“部分实现”不能简单等同于这三个缺陷仍然存在。

## 生产部署门禁（新增）

| ID | 证据 | 门槛 |
| --- | --- | --- |
| P-01 | `deploy/compose.production.yaml`、`scripts/production-acceptance.sh` | 两副本、HAProxy、PostgreSQL、Redis、MinIO、OTel/Jaeger、OIDC fixture 实际启动；任一命令失败或未执行均失败 |
| P-02 | `/healthz`、`/readyz`、`/admin/v1/health/dependencies` | liveness 不受短暂外部断连影响；必需依赖、控制面、内容安全、审计和 drain 阻止 ready；详情不含地址凭据或底层错误原文 |
| P-03 | queue/worker/contentsafety PostgreSQL 集成测试 | 副本退出可 reclaim，提交确认丢失只 replay canonical result，输入/输出安全阶段可由另一节点接管，Session/Outbox/预算/审计可对账 |
| P-04 | OIDC/JWKS 与 Secret canary 扫描 | viewer/operator/security-admin 及 tenant scope 生效；旧/错 issuer、audience、kid、过期 token 拒绝；日志、metrics、audit spool/SQL、health 和 trace 不含 canary/JWT/DSN |
| P-05 | Jaeger 查询与 storage spans | Gateway、Inbox、Worker、Session/Memory/Summary/Audit、Outbox 保持 W3C 关联；span 只含低基数安全属性 |

执行入口为 `./scripts/production-acceptance.sh`。脚本保留脱敏 summary（状态、计数、hash、trace ID、组件和错误类别），清理临时 Compose 网络、容器和卷；没有成功 summary 不得写入“生产部署验收通过”。

2026-09-10 已执行并通过：`production_acceptance=passed`，两副本在场，存活副本接管成功，PostgreSQL 断连后 `/healthz=200`、`/readyz=503` 并恢复为 ready，提交确认丢失和内容安全租约均恢复；RBAC、10 项依赖状态、Jaeger 查询和 Secret 扫描通过，`skips=0`、`unexecuted=0`。证据见 [production summary](../evidence/production/summary-trpc-prod-acceptance-2392023385.txt)。

2026-09-10 已完成一次**本机真实企微单聊联调**：企业微信智能机器人长连接认证成功，单聊消息进入 `wecom-aibot` binding，经 `glm-5.2` 处理后平台确认出站回复成功；指标分别记录一条成功请求和一条成功投递。证据见 [local WeCom single-chat acceptance](../evidence/production/local-wecom-single-chat-acceptance-20260910.md)。该证据只覆盖真实 IM 平台联通，不替代 P-01–P-05 的生产部署证明。

## 1. 多租户与节点部署

| ID | 原始要求 | 当前状态与代码证据 | 验收方式 | 生产设计证据 |
| --- | --- | --- | --- | --- |
| A-01 | 租户模型至少含 tenant_id、应用、模型、工具权限、IM、数据后端、审计 | **已实现**：[config.go](../trpcservice/config/config.go) 的 `TenantConfig` 全部覆盖，另有 budget/version/enabled；未知 YAML 字段拒绝 | `TestDecodeDefaultsAndSecretReferences`、`TestDecodeRejectsUnknownField`；加载 [example.yaml](../config/example.yaml) | [架构 §2](architecture.md#2-租户模型) |
| A-02 | 组件部署拓扑及 Gateway/Worker/Channel/Storage/Admin/Telemetry 协作 | **部分实现**：这些模块已拆包但装配在单二进制 [main.go](../cmd/trpc-service/main.go) | 启动后检查 webhook、Admin、metrics；代码走查依赖装配 | [架构 §3](architecture.md#3-组件与部署拓扑) 给出 Mermaid 最小/生产拓扑 |
| A-03 | 多节点水平扩展，消息路由到正确 tenant/session | **大部分实现**：binding 反推 tenant；session 派生稳定；PostgreSQL Inbox/Outbox 按与 Worker 锁相同的完整 Session lane 严格 FIFO、跨 lane 并发，Redis Coordinator/Session/Memory 可跨节点共享；007 配置控制面以 PostgreSQL CAS、LISTEN/NOTIFY+轮询、稳定灰度分组和节点 ACK 同步 revision；Outbox 发送前再次校验 tenant 绑定所有权；审批 nonce 在 `coordination=redis` 时跨节点共享，`inmemory` 时仅限单进程 | `TestWebhookDerivesTenantFromVerifiedBinding`、`TestIdentityIsolationAndConversationRules`、`TestDurableRunsSessionsConcurrentlyButOneSessionFIFO`、`TestStableCanaryBucketAcrossRegistries`、`TestPostgresControlPlaneCASAndVerifiedRelease`、`TestPostgresIntegrationConcurrentLeaseAndFencing`、`TestRedisSessionIsVisibleAcrossIndependentWorkerRuntimes`、`TestDurableDoesNotDeliverWithReassignedTenantBinding` | [架构 §5](architecture.md#5-租户与-session-路由)、[架构 §8](architecture.md#8-灰度发布与租户级回滚)，[数据 §4](data-consistency.md#4-同一-session-的并发一致性) |
| A-04 | 说明 sticky session | **部分实现**：单机 InMemory 不可横扩；Redis coordination/Session/Memory 模式不依赖 HTTP sticky，但尚非完整无状态生产栈 | `TestRedisSessionIsVisibleAcrossIndependentWorkerRuntimes` 用两个独立 Runtime 验证共享 Session/Memory | [架构 §5.3](architecture.md#53-是否需要-sticky-session) 明确生产不需要及例外 |
| A-05 | 配置、数据、工具、日志脱敏、密钥的租户隔离 | **部分实现**：binding 唯一、租户 hash namespace、Runtime 隔离、执行点工具策略、审计脱敏、secret env 引用均已有；Artifact 元数据已有强制 RLS，PGVector 已有 SQL 级 tenant/app 过滤；云 IAM 与真实隔离演练仍需部署侧验收 | binding collision、identity、tool policy、audit redaction 测试 | [架构 §7](architecture.md#7-五层租户隔离)，[安全 §1–3](security-operations.md) |

## 2. 数据同步与多后端

| ID | 原始要求 | 当前状态与代码证据 | 验收方式 | 生产设计证据 |
| --- | --- | --- | --- | --- |
| D-01 | 不同租户选 InMemory、Redis、SQL、向量、对象或外部 Memory | **大部分实现**：Session 支持 InMemory/Redis/strict PostgreSQL；Memory 支持 InMemory/Redis/外部 Mem0 reader+ingestor；Artifact 支持 InMemory/S3-compatible object；Knowledge 支持租户强制过滤的 PGVector。Mem0 写工具与未构造 provider fail-closed | `TestBuildRuntimeWiresSQLTurnSession`、`TestExternalMem0BackendWiresReaderIngestorAndAllowedTools`、`TestObjectArtifactBackendConstructsWithoutNetworkDial`、`TestValidateAllowsConstructedProductionDataBackends` | [数据 §1、§7](data-consistency.md#1-当前能力真值表) |
| D-02 | 统一访问抽象，覆盖 Session/Memory/Summary/Artifact/Knowledge/Audit | **大部分实现**：Runner 使用框架 Session/Memory/Artifact SPI，Audit Sink 独立；Summary 复用 strict SQL Session 的 008 持久表和可重启 job，Memory visibility 提供 PG/Redis/InMemory 适配层，SQL Audit 提供追加链 + 本地 spool 恢复；内部 backend bundle 已统一装配与失败清理，事务/database identity 使用现有能力接口；通用 provider capability 协商仍未提供 | `TestValidateSummaryMustUseSessionBackend`、008 Summary/watermark/Audit 单测与 PostgreSQL integration、Runtime 构造测试 | [数据 §2、§5.3](data-consistency.md#2-统一访问抽象) |
| D-03a | 多节点并发写同一 session 的一致性 | **大部分实现（SQL Session 路径）**：持久队列保证 Session FIFO；strict PostgreSQL Session 在 Begin 签发单调 fence，Commit 校验 expected version + fence 并原子推进 version，新 Begin takeover 使旧 Handle 提交失败。InMemory/Redis Session 仍只有协调锁保护 | `TestPostgresIntegrationFencingVersionAndAbort`、`TestPostgresIntegrationConcurrentBeginAndDuplicateCommit`、`TestSessionServiceIntegrationFencingAndLocalAbort`，以及 Durable 分区/租约测试 | [数据 §4](data-consistency.md#4-同一-session-的并发一致性) |
| D-03b | Session event、state、summary 更新顺序 | **大部分实现（限定 SQL 组合）**：当 `queue=postgres`、`session=sql` 且同 `dsn_env` 时，v2 required 还要求 Task/Inbox 元数据一致且 Queue/Session/事务连接 database identity 相同；events/state/version/canonical replay、确定性 Outbox 与 Inbox processed 在同一事务提交，并校验 Session fence 和完整 Inbox lease 身份/deadline。legacy `0/空/空` 仅走可恢复双事务；未来版本、能力暂缺或 identity 不匹配停放且不扣死信预算，当前 v2 格式/元数据漂移才死信。008 Summary 在独立可恢复边界校验 coverage/boundary/source hash，Memory、用量/审计和工具外部副作用不在该边界 | `TestPostgresIntegrationAtomicTurnInboxOutboxCommit`、`TestPostgresIntegrationAtomicFenceRejectsPipelineAndTransactionDatabase`、`TestPostgresIntegrationAtomicOutboxFailureRollsBackTurnAndInbox`、`TestPostgresIntegrationAtomicParticipantUsesCommittedCanonicalReplay`、`TestPostgresSummaryValidatesCoverageAndSelectsNewestCompatibleRange`、pipeline legacy/park/DLQ 单测 | [数据 §4.3、§5、§5.3](data-consistency.md#63-已实现的-rolling-upgrade-协议) |
| D-03c | Memory 写后跨节点可见 | **大部分实现**：Redis/Queue PostgreSQL Memory visibility adapter 按 tenant/app/principal 生成单调水位，独立 Runtime 可用 `min_watermark` 互相验证；backend epoch、更新时间和最后错误可识别停滞；InMemory 明确只声明 node-local，超时返回 degraded，Mem0 不宣称强一致 | `TestRedisWatermarksAreSharedAndAtomic`、`TestPostgresVisibilityWatermarkCrossNodeReadAfterWrite`、`TestWaitUntilVisibleReturnsDegradedTimeout`、`TestRedisSessionIsVisibleAcrossIndependentWorkerRuntimes` | [数据 §5.2、§5.3](data-consistency.md#52-memory-跨节点可见性) |
| D-03d | Redis→SQL、本地向量→远端向量迁移 | **大部分实现**：004 ledger、通用可重入 Engine，以及 Redis Session HashIdx/legacy zset → strict SQL 和版本化 JSONL → PGVector adapter 已落地；含 coordination Redis 分布式冻结、drain、断点扫描、目标冲突保护、对象 SHA-256、epoch/sequence 旧摘要保护、逐对象 source/target reconciliation、shadow/canary/drain/finalize mismatch 门禁、配置路由确认、失败续跑和 selective rollback。rollback 只删除本次 migration 且 hash 未变化的对象，冲突保留并记录。006–009 已加入统一迁移链并按 Queue PostgreSQL 持久化，009 Content Safety 额外覆盖 pending lease 接管和 blocked fail-closed。 | `TestEngineAdvancesOneIdempotentBatchAtATime`、`TestEngineParksMismatchBeforeFinalBatch`、`TestVectorTargetRejectsOlderSummaryAndEpoch`、`TestVectorRollbackPreservesReplacement`、`TestJSONLVectorSourceResumesByLineCursor`、`TestRedisSessionSourceScansHashIdxAndSeparatesScopedState`、`TestRedisTenantFreezeIsOwnedAndPersistent`、`TestRoutedMaintenanceGateRejectsWrongDatabaseIdentity`、`TestSourcePrefixForJobRejectsWrongDatabase`、`TestCheckJobTenantRejectsWrongTenant`、`TestTenantJobRejectsWrongTenantOrAppNamespace`、`TestUnfreezeTerminalReleasesOnlyValidTerminalRoutes`；完整门禁为 `go test ./...`、`go test -race ./...`、`go vet ./...`、`./build.sh`、三个命令 `-h`、`./scripts/smoke.sh` 及隔离临时 PostgreSQL 的 `./scripts/postgres-acceptance.sh`，其中任何 `SKIP`、失败或未执行都不通过 | [数据 §8–9](data-consistency.md#8-redis--sql-迁移) 的 PREPARE→FINALIZE 状态机、双写/对账/tombstone |
| D-03e | IM 重复投递幂等 | **大部分实现**：入站 hash/唯一约束 + canonical result 重放；持久回复先确定性分片，每片一个稳定 operation key 与独立 attempt ledger。`leased` 崩溃可安全重试，`dispatched`/网络/畸形成功响应转 unknown 并阻塞同 Session，只有人工 CAS 决议可确认、接受重复风险后重试或取消；不把无平台幂等键描述成 exactly-once | `TestDurableSubmitDeduplicatesPlatformRedelivery`、`TestDurablePlansOneOutboxPerPartWithStableOperationKeys`、`TestDurableUnknownParksWithoutAutomaticRetryAndManualRetryKeepsOperationKey`、`TestDurableDeliveryLedgerCommitFaultWindowsNeverBlindRetry`、`TestPostgresIntegrationDeliveryOperationLedgerAndManualResolution`、`TestPostgresIntegrationConcurrentResolutionIdempotencyAndCAS` | [数据 §6](data-consistency.md#6-im-重投与端到端幂等) 的 operation ledger |
| D-04 | 说明各后端强/最终一致、延迟、成本和运维取舍 | **不适用代码** | 架构评审逐项核对表格 | [数据 §7](data-consistency.md#7-后端一致性取舍) |
| D-05 | 最小模型含 tenant/app/session/event/memory/summary/binding/audit | **不适用代码**；当前运行时数据沿用框架结构 | DDL 评审、迁移工具 dry-run | [数据 §10](data-consistency.md#10-最小关系数据模型) 含全部必需表及 Inbox/Outbox/Artifact |

## 3. IM 软件接入

| ID | 原始要求 | 当前状态与代码证据 | 验收方式 | 生产设计证据 |
| --- | --- | --- | --- | --- |
| I-01 | 至少支持两类 IM（含微信/企业微信） | **已实现**：[telegram.go](../trpcservice/channels/telegram.go)、[slack.go](../trpcservice/channels/slack.go)、[wecom.go](../trpcservice/channels/wecom.go)、[wecom_aibot.go](../trpcservice/channels/wecom_aibot.go) 注册到统一 Adapter Registry；另由 [aibotbridge](../trpcservice/aibotbridge/bridge.go) 维护企业微信智能机器人 Bot ID/Secret WebSocket 长连接。自建应用覆盖 AES 回调，智能机器人覆盖长连接认证、单聊/群聊规范化和主动 Markdown 回复 | `TestTelegramVerifyAndParse`、`TestSlackVerifyParseAndRejectReplay`、`TestWeComURLVerification`、`TestNormalizeTextDirectMessage`、`TestNormalizeMixedGroupMessageAndRejectsWrongBot`、`TestWeComAIBotDeliveryOutcomes` | [IM §1–2](im-adapters.md#1-adapter-契约与能力模型)（含两类企业微信章节） |
| I-02 | 外部消息→tRPC-Agent 输入；Agent Event→回复/流/卡片 | **部分实现**：规范消息→Runner；Event stream 聚合成最终文本；附件仅安全元数据提示；无增量编辑/卡片/附件出站 | Adapter 测试、`TestAttachmentMetadataIsPassedWithoutProviderCredentialedURL`、mock direct chat | [架构 §4.2](architecture.md#42-企业微信--strict-sql--postgresql-queue-核心时序) 给出企业微信 AES→trace/request→Inbox→Runner/Tool/Session/Memory→Outbox→回复完整时序；[IM §3–4](im-adapters.md#3-入站规范化) 给出 ReplyOp 降级 |
| I-03 | 账号租户绑定、URL/token/secret/验签/去重/身份映射 | **大部分实现**：绑定、env secret、Telegram header、Slack HMAC+时间窗、企业微信 AES 验签解密+URL 验证、身份 hash 已有；PostgreSQL Inbox 唯一键提供 durable 去重，Outbox 按原 tenant 解析绑定；仍缺长期 tombstone/平台发送幂等键 | Gateway 签名先于 dispatch、binding collision、`TestDurableSubmitDeduplicatesPlatformRedelivery`、`TestDurableDoesNotDeliverWithReassignedTenantBinding`、`TestWeComURLVerification` | [IM §2、§5–6](im-adapters.md#2-webhook账号与租户绑定) |
| I-04 | 群聊/单聊 session_id 与跨群/跨租户隔离 | **已实现**：单聊按用户，群聊按 conversation/thread 共享 principal；tenant/app/binding/channel/scope 都入 hash；企业微信 `ChatId` 群聊与 userid 单聊同规则 | `TestIdentityIsolationAndConversationRules`、`TestGroupMembersShareOneRunnerSession`、`TestWeComParseGroupChatAndBindingChecks` | [IM §5](im-adapters.md#5-单聊群聊与身份隔离) |
| I-05 | 长度、频率、异步、图片/文件、撤回、失败重试 | **部分实现**：rune 拆分（Telegram/Slack）、字节拆分（企业微信 2048B）、租户入站 RPM、callback 后异步、持久 Outbox 对明确未发送结果遵循 Retry-After/退避/DLQ、附件安全元数据、企业微信顶层 image/file 映射及 access_token 过期重试；通用 5xx/网络/超时按 unknown 停车。无出站主动限频、内容下载/撤回 | `TestChunksUsesRuneLength`、`TestChunksUTF8BytesSplitsOnRuneBoundary`、`TestWeComParsesTopLevelMediaFields`、`TestDurableOutboxRetriesDeliveryThenSucceeds`、`TestDurableOutboxDeadLettersAfterMaxAttempts`；其余做负向能力检查 | [IM §6–7](im-adapters.md#7-平台限制与降级矩阵) 完整能力/降级矩阵 |

## 4. 治理、监控和安全

| ID | 原始要求 | 当前状态与代码证据 | 验收方式 | 生产设计证据 |
| --- | --- | --- | --- | --- |
| G-01a | Filter：工具白名单 | **已实现**：模型可见性 Filter + 执行点 PermissionPolicy，deny 优先、未知默认拒绝 | governance 单测并增加 allow/deny 表驱动用例 | [安全 §2](security-operations.md#2-filter-治理链) |
| G-01b | Filter：敏感信息脱敏 | **规则型文本治理已实现**：租户 `privacy.input/output=off|redact|block`，当前输入及模型 history/tool/summary 文本在 provider dispatch 前处理，完整出站回复在 canonical/Outbox 前处理；日志/trace 既有脱敏保留，语义 DLP/可逆 tokenization 仍未实现 | `privacy`、`worker/privacy_test.go`、`agent/privacy_test.go` 覆盖隔离、非原地变更、完整模型适配器和 canonical replay；[策略边界](privacy-and-storage-metrics.md) | [安全 §2.2、§3](security-operations.md#22-生产-filter-顺序) |
| G-01c | Filter：预算限制 | **已实现（按部署后端分层）**：Redis Lua + 服务端时间提供跨节点租户固定 60 秒 RPM，Redis 故障 fail-closed；Queue PostgreSQL migration 006 按每次真实 provider model call 持久化 reserve/settle/unknown/release，固定点费用锁定双节点总额度。replay 不扣费，实际 retry/failover 新调用计费；unknown 不自动退款，结算回原 UTC 账期 | `TestRedisRateLimiterIsSharedAtomicAndTenantScoped`、`TestRedisRateLimiterFailsClosedWhenRedisIsUnavailable`、`TestInMemoryRateLimiterIsAtomic`、`TestTrackedModelReservesAndSettlesEachActualCall`、`TestTrackedModelMarksMissingUsageUnknownWithoutRefund`、`TestTrackedModelDoesNotAckWhenUsageLedgerCannotTransition`、`TestPostgresLedgerConcurrentReservationsDoNotDoubleTheLimit`、`TestPostgresLedgerUnknownAndCrossMonthSettlement`；`./scripts/postgres-acceptance.sh` 强制覆盖全部 PostgreSQL 测试包，必须使用隔离 `TEST_POSTGRES_DSN` 且无 `SKIP` | [安全 §2.3](security-operations.md#23-分布式预算) 的 Redis 限流与 PostgreSQL ledger |
| G-01d | Filter：危险工具二次确认 | **已实现（InMemory/Redis）**：nonce 绑定 tenant/revision/user/session/tool/规范参数 hash，5 分钟一次性；JSON 规范化保留大整数精度并拒绝尾随值 | `TestRedisApprovalSharedAcrossIndependentInstances`、`TestRedisApprovalConcurrentConsumeOnlyOneWins`、`TestPermissionPolicyRequiresArgumentBoundOneTimeApproval`、`TestExtractApproval`、`TestApprovalArgumentNormalizationPreservesLargeIntegers`、`TestApprovalArgumentNormalizationRejectsTrailingJSONValue` | [IM §8](im-adapters.md#8-用户权限与危险操作确认) 的持久审批/card 方案 |
| G-01e | Filter：IM 用户权限 | **已实现**：binding 级 `allowed_users` 在 Runner 前校验 | allow/deny 用户集成用例 | [安全 §2](security-operations.md#2-filter-治理链) 的目录/角色强化 |
| G-02 | 请求、模型/工具耗时、投递、错误、token、成本、Session 延迟指标 | **大部分实现**：平台 `/metrics` 有 callback/request/worker latency/delivery/token/cost/tool permission/tool side-effect operation；配置 OTLP 时框架 MeterProvider 另导出模型 TTFT/耗时/token 与 Agent/工具 histogram 到 Collector `:9464`；新增 `storage_operation_duration_seconds` histogram、Runner Session SPI 与 InMemory/Redis Memory 边界度量，附 P95/错误率告警；外部 Mem0 后台 ingestion 细分与告警通知端仍需部署集成 | GET `/metrics` 与 Collector `:9464/metrics`，验证 label 无用户/session/request/operation key | [安全 §4](security-operations.md#4-指标设计) 完整生产指标表 |
| G-03 | OTel trace 串 IM callback、Runner、Tool、Session/Memory、IM 回复 | **大部分实现**：W3C 传播、OTLP、callback/signature/worker/send 显式 spans 和框架 Runner spans；Outbox 持久化并恢复 `traceparent/tracestate`，异步 send 保持原 trace；Inbox/Outbox、Session/Summary、Memory watermark、Artifact、Vector、Audit、Content Safety 和 Redis 限流边界统一产生低基数 storage spans，仍需生产 Compose 查询证据 | `TestDurableOutboxContinuesInboundTrace`、storage 属性脱敏测试；生产脚本按 request/trace 查询 Collector/Jaeger 并扫描敏感值 | [安全 §5](security-operations.md#5-opentelemetry-链路) 完整目标链及异步 link |
| G-04 | 审计至少含指定 11 类字段 | **已实现字段与 SQL 恢复路径**：`audit.Entry` 含所有必需字段及 request/revision/hash、tool operation key/phase/state；SQL sink 使用 008 追加 hash chain，数据库故障转入 `0600`、fsync 后的本地 JSONL spool，重启/后台 drain 仅在提交后删除；stdout/file 仍是非 durable 选项，SQL 与 spool 同时失败时 fail-closed | `TestAuditRedactsSecretsAndPII`、SQL sink 幂等/hash-chain/spool integration test；触发 allow/deny/tool ask/side-effect unknown 检查脱敏 JSONL | [安全 §6](security-operations.md#6-审计日志) 的 SQL WORM、链式 hash、对账 |
| G-05 | IM/model/DB secret 不进日志/trace/error | **大部分实现**：只存 env/`secret://` 引用，SecretResolver 缺失时 fail-closed；生产 OIDC/JWKS/RBAC 拒绝共享 token；HTTP 错误归一、审计 redactor，Chat/Agent/Tool/Workflow payload span 属性源端 Drop 且 Collector 二次删除；真实 Secret Manager 双版本轮换由部署侧实现 | `TestTelegramDeliveryErrorNeverLeaksBotToken`、adminauth/SecretResolver/审计/storage span 脱敏测试；生产脚本执行 secret canary trace/access-log 扫描 | [安全 §3](security-operations.md#3-密钥管理与脱敏) |

## 5. 故障恢复与运维

| ID | 原始要求 | 当前状态与代码证据 | 验收方式 | 生产设计证据 |
| --- | --- | --- | --- | --- |
| O-01a | 节点故障降级 | **部分实现**：PostgreSQL 队列支持持久 ACK、数据库时钟、续租与 reclaim；同 DSN strict SQL 组合再用 version/fencing 和 Inbox lease/身份复核，把 Session Turn、Outbox 与 Inbox 一次提交，已提交 Turn 的补齐使用 canonical replay。默认 InMemory、Redis Session 及其他组合仍有各自边界 | 原子组合提交/回滚/canonical replay 测试 + Durable reclaim/clock/fencing 测试；仍需 SIGKILL、网络分区与 COMMIT ACK 不确定部署演练 | [安全 §7](security-operations.md#7-故障恢复与降级) |
| O-01b | IM 重试降级 | **大部分实现**：Provider `Retry-After`、结构化四态结果、attempt/reconciliation ledger、稳定分片 operation、重试上限/DLQ、unknown 停车和脱敏 Admin 决议 API 已实现；进程队列也会阻止后续分片失败触发整任务重放；仍缺平台自动查询/对账 connector 与原生发送幂等键 | Channel structured-outcome tests、`TestLegacyDeliveryDoesNotReplayConfirmedPartsAfterLaterSafeFailure`、`TestDurableUnknownPartBlocksLaterPartsInSameLane`、`TestDurableDeliveryLedgerCommitFaultWindowsNeverBlindRetry`、`TestDurableLeaseRenewalFailureAfterDispatchParksUnknown`、`TestPostgresIntegrationReclaimDistinguishesLeasedAndDispatched`、Web Admin CAS 测试 | [IM §7](im-adapters.md#7-平台限制与降级矩阵)、[安全 §7](security-operations.md#7-故障恢复与降级) |
| O-01c | DB 短暂不可用 | **部分实现**：callback 持久化失败时不返回 accepted；普通路径完成事务失败会退避并从 result 重建，strict SQL 组合路径任一 participant 失败会让 Session/Outbox/Inbox 整体回滚，不降级到 InMemory；仍缺真实断连、COMMIT ACK 不确定、恢复 drain 与 backlog 阈值 | `TestDurableRetriesFailedInboxCommitFromReplayableResult`、`TestPostgresIntegrationAtomicOutboxFailureRollsBackTurnAndInbox`；中断真实 Redis/DB 验证恢复 drain | [安全 §7](security-operations.md#7-故障恢复与降级) |
| O-01d | 模型超时 | **部分实现**：Worker context timeout，默认 90 秒；OpenAI-compatible 主模型支持首个有效响应前按优先级回退本地 mock，且 fallback 配置强制非流式，避免部分输出后混用模型；仍缺 provider-aware 熔断与失败尝试账单对账 | `TestConfiguredModelFallsBackToMockBeforePrimaryResponse`、fake slow model/取消测试 | [安全 §7](security-operations.md#7-故障恢复与降级) 的可重试分类、备用模型与 reconciliation |
| O-01e | 工具执行失败 | **大部分实现（副作用 fencing 基线）**：`tools.side_effects` 显式分类；最终 allow 后才 reserve/lease/mark executing；稳定 semantic intent 可 replay confirmed 结果，普通错误/崩溃/结果落账失败转 unknown 并停止盲重做，只有 typed definitely-not-applied 可重试；PostgreSQL 003 ledger、attempt fencing、tenant-scoped Admin CAS 决议、指标/trace/审计已接入。仍缺独立熔断、各真实 provider 查询/自动 reconciliation connector 和逐工具原生幂等键接线 | `TestRunGuardDeniedPermissionLeavesNoOperation`、`TestRunGuardConfirmsAndReplaysSemanticIntentAcrossCallIDs`、`TestRunGuardOrdinaryToolErrorBecomesUnknown`、`TestRunGuardOnlyTypedNotAppliedErrorCanRetry`、`TestProcessWiresSideEffectGuardThroughRunnerLifecycle`、tooloperation PostgreSQL integration 与 Web Admin CAS 测试 | [安全 §2、§7](security-operations.md#7-故障恢复与降级) |
| O-02 | 灰度发布与租户级配置回滚 | **大部分实现**：007 持久化 immutable revision、release/event、tenant generation、node/boot heartbeat 和 prepared/applied ACK；PostgreSQL 控制器通过 `LISTEN/NOTIFY` 加轮询刷新，发布使用 generation CAS，canary 以 `tenant + app + session` 稳定分桶，rollback 创建新 release 不删除历史；在途 Task 携带原 revision 快照；审批 nonce 在 `coordination=redis` 时跨节点共享，`inmemory` 时仅限单进程 | `TestMemoryRevisionImmutableAndCAS`、`TestMemoryReleaseRequiresAllPreparedAndApplied`、`TestStableCanaryBucketAcrossRegistries`、`TestControllerRefreshesPersistentState`、`TestPostgresControlPlaneCASAndVerifiedRelease`；完整门禁包含 `go test ./...`、`go test -race ./...`、`go vet ./...`、`./build.sh`、三命令 `-h`、`./scripts/smoke.sh` 和隔离临时 PostgreSQL `./scripts/postgres-acceptance.sh`，任何 `SKIP` 都失败 | [架构 §8](architecture.md#8-灰度发布与租户级回滚)、[安全 §8](security-operations.md#8-灰度发布与租户级回滚) |
| O-03 | 容量评估 | **不适用代码**；当前 queue/worker 数可配置 | 用真实压测参数代入并与饱和点校准 | [安全 §9](security-operations.md#9-容量评估) 含并发、节点、队列、token、Redis/SQL/IM 公式与样例 |
| O-04 | 最小与生产部署方案 | **已实现并通过本地 Compose 生产验收（2026-09-10）**：单二进制、双副本 Compose overlay、HAProxy、PostgreSQL/Redis/MinIO、OTel/Jaeger、OIDC fixture、独立 migrator/runtime 凭据、SecretProviderClass、PDB、RollingUpdate、拓扑约束、drain/readiness、依赖探测、内容安全租约和审计 spool 均已固化；云厂商的实际多 AZ、PITR、KMS/NetworkPolicy 仍由部署平台提供 | 启动最小方案；渲染 Compose/Kustomize，确认 migration 先于业务发布、runtime 无 DDL；`./scripts/production-acceptance.sh` 已完成双副本/断连/ACK 丢失/RBAC/Secret/trace 证据 | [架构 §9](architecture.md#9-最小部署与生产部署)、[安全 §10](security-operations.md#10-部署与运行手册) |

## 本轮 Review 修复与验证范围

- Redis/InMemory Summary：复用框架摘要器、触发器、Session 异步队列与 LLMAgent 摘要读取；`TestRuntimeSummaryGeneratedAndReused` 验证生成、持久读取和下一次模型请求携带摘要（Redis 使用独立 Runtime）。`TestDisabledSummaryDoesNotCallModel` 验证 disabled 无摘要。
- Runtime 缓存：128 个实例/构造预约上限，仅淘汰无引用且状态持久化的实例；`TestManagerEvictsIdleDurableRevisionAndRebuildsSnapshot`、`TestManagerPinsLocalStateAtCapacity`、`TestManagerBuildReservationCancellationAndShutdown`、`TestManagerConcurrentAcquireSharesOneRuntime` 覆盖回收、旧快照重建、内存状态保护、并发、取消和关闭。缓存测试使用受控 factory，未替代真实数据库容量/连接故障演练。
- Skills：新增默认拒绝的 `skills.allow`，复用框架 `WithSkillFilter`；`TestRuntimeSkillAuthorizationAppliesToPromptAndLoading` 用实际 Runner/LLMAgent 和本地模型 HTTP fixture 验证授权与越权加载、提示词隔离。`TestSkillPolicyValidationAndLegacyJSON` 与控制面复制测试保证旧 JSON 兼容、持久快照包含授权且无切片别名。
- 装配：Manager 负责缓存生命周期，runtime factory 装配框架 Agent/Runner，backend factory 持有数据接口和资源清理；未改变 strict SQL/Inbox/Outbox 事务协议。
- Redis 审批与 SQL Audit spool 为既有代码，本轮纠正文档中的“审批只在进程内”“审计错误被忽略”过期描述。

本轮实际执行（2026-09-10）：

| 命令 | 实际结果 | 验证边界 |
| --- | --- | --- |
| `go test ./...` | 退出码 0 | 普通全量测试；数据库集成测试以独立 PostgreSQL 门禁为准 |
| `go test -race ./...` | 退出码 0 | 全 Go module 竞态检查 |
| `go vet ./...` | 退出码 0 | 全 Go module 静态检查 |
| `./build.sh` | 退出码 0 | 前端嵌入、服务与两个迁移命令构建 |
| 三个命令 `-h` | 全部退出码 0 | `trpc-service`、`trpc-migrate`、`trpc-data-migrate` 可运行 |
| `./scripts/smoke.sh` | 退出码 0；HTTP smoke 通过 | 本机未安装 `promtool`，仅其可选 Prometheus 语义检查跳过 |
| `TEST_POSTGRES_DSN=... ./scripts/postgres-acceptance.sh` | 退出码 0；14 个包全部 `PASS`，无 `SKIP` | 任务专用临时 PostgreSQL，迁移/校验及真实 SQL 路径 |
| `./scripts/production-acceptance.sh` | 退出码 0；`production_acceptance=passed`、`skips=0`、`unexecuted=0` | 双副本故障恢复、断连、ACK 丢失、内容安全、RBAC、Secret、依赖和 trace |

默认测试中的 miniredis/HTTP fixture 不等于真实 Redis、模型、IM 或生产部署验收；本轮隔离 PostgreSQL 与 Compose 门禁已分别提供无 skip 的数据库和生产证据。

## 6. 自动化验证基线

在 `solution/` 目录执行：

```bash
go test ./...
go test -race ./...
go vet ./...
./build.sh
bin/trpc-service -h
bin/trpc-migrate -h
bin/trpc-data-migrate -h
./scripts/smoke.sh

# 只使用本次任务创建的隔离临时 PostgreSQL 数据库；脚本先应用/校验当前迁移，再拒绝任何 SKIP
TEST_POSTGRES_DSN='postgres://user:pass@127.0.0.1:5432/isolated_acceptance?sslmode=disable' \
  ./scripts/postgres-acceptance.sh
```

本次验收记录必须由实际门禁输出填充：新增 009 内容安全迁移后，PostgreSQL 包数量和迁移版本均以脚本当前值为准；生产 Compose 门禁还必须证明双副本、节点退出、数据库断连、提交确认丢失、内容安全接管、RBAC、依赖状态、Secret canary 和 trace/storage spans。任何数据库缺失、`SKIP`、失败或未执行仍不得标记 PostgreSQL 或生产部署验收通过。

最低自动化覆盖及其证明目标：

| 测试 | 证明什么 | 不能证明什么 |
| --- | --- | --- |
| config decode/default/unknown/collision | 配置严格、secret 使用引用、binding 不跨租户复用 | 多节点配置发布与 Vault 轮换 |
| Telegram/Slack/企业微信 Adapter | 原始请求验签、重放时间窗、规范消息映射、纯 Plan 分片（rune/byte）、单 operation 单 HTTP、四态结果、Retry-After、平台 ID/响应 hash、企业微信 AES/URL 验证、secret 脱敏 | 真实平台限流/权限变化/所有事件类型与自动 reconciliation |
| Gateway binding/signature | tenant 来自已验证服务端 binding，验签失败不 dispatch；PostgreSQL 模式持久化后 ACK | 真实平台网关与数据库断连时序 |
| identity/group tests | ID 稳定、群上下文共享、跨租户隔离 | HMAC 密钥轮换、账号合并 |
| worker continuity/concurrency/Redis | 单机相同 session 串行、不同租户状态分离、独立 Runtime 共享 Redis Session/Memory | Redis 租约失锁后的陈旧写防护 |
| IM dedupe/result/send operation ledger | 重投不重跑；分片稳定；发送成功但响应丢失转 unknown，禁止自动重发并支持人工 CAS 决议 | 平台原生 exactly-once 与自动查询/对账 |
| durable Inbox/Outbox + strict SQL Turn | 单条容量租约、续租、过期回收、Session FIFO/跨 Session 并发；同库组合提交 Session/Outbox/Inbox、participant 故障整体回滚、已提交 canonical replay 补齐；legacy 双事务 drain；v2 Task/Inbox metadata 与 database identity 校验；未来版本/能力/identity 不兼容停放且不耗尽死信预算，当前 v2 损坏才 DLQ；真实 PostgreSQL 并发 migration/数据库统一时钟/租约 fencing（含应用时钟偏移与行锁等待跨租期）、租户绑定校验与异步 trace 连续 | 网络分区、进程 SIGKILL、COMMIT ACK 不确定、外部平台已发送但本地未提交的完整故障矩阵；未设置 `TEST_POSTGRES_DSN` 的 skip 也不能证明真实 SQL 路径通过 |
| Redis rate limit / usage budget | 两个 Redis limiter 共享租户窗口且并发不超限；PostgreSQL ledger 并发 reserve 不翻倍、实际 settle/超额、unknown 不退款、跨月回原账期；实际模型调用与 replay/retry 计费边界 | Redis/PG 集群故障演练、provider 自动 reconciliation；unknown 人工对账不在公共 API |
| input budget/tool approval | 输入上限、确认参数绑定、一次性、过期/篡改拒绝 | 持久审批和实际副作用对账 |
| tool side-effect guard/operation ledger | deny/ask 不留 operation；稳定 intent、精确单 key lease、attempt fencing、confirmed replay、generic error→unknown、typed safe retry、租户隔离、脱敏 Admin CAS 决议；业务 Runner 生命周期已真实接线 | 工具 provider 与本地数据库原子提交、provider 一定支持幂等键/查询、自动 reconciliation 和部署故障恢复 |
| 008 lifecycle | Summary boundary/version/source hash、PG/Redis watermark、SQL audit hash chain + 0600 spool、JSONL vector epoch/sequence/hash reconciliation、resume/selective rollback | 外部 provider 自动 reconciliation、人工审计修正和完整网络分区演练 |
| audit redaction/recovery | 常见 PII/secret 不出现在审计字段；重复 audit ID 幂等；PostgreSQL 故障写 fsynced spool，重启 drain 后校验 hash chain | 任意第三方 SDK/Collector/代理日志均安全 |

发布前还应增加当前缺失的集成/故障测试：真实 Redis 双节点、Session 锁租约丢失、PostgreSQL 网络分区/进程 SIGKILL 与恢复 drain、COMMIT ACK 不确定、Collector 端完整 span、模型超时与取消、真实工具 provider 幂等键/查询/reconciliation、附件 SSRF/病毒/大小限制和备份 PITR。SQL 事务 rollback、并发租约、migration 002–009、Durable→Worker 组合事务、预算账本、配置控制面、生命周期账本、内容安全租约和 tool operation 状态机已有 `PostgresIntegration` 测试基线，但必须用隔离库设置 `TEST_POSTGRES_DSN` 运行；环境缺失导致的 skip 不计为验收通过。审批参数规范化另由 `TestApprovalArgumentNormalizationPreservesLargeIntegers` 覆盖键顺序/空白等价、嵌套大整数区分和 nonce 防碰撞，并由 `TestApprovalArgumentNormalizationRejectsTrailingJSONValue` 固化尾随值拒绝。

## 7. 手工演示验收

1. 用 mock 模型启动单节点，访问 `/healthz`、`/readyz`、`/metrics`；让队列 Store 查询失败或触发 drain，确认 `/readyz` 返回 503，而 `/healthz` 仍为 200。
2. 通过 Admin 测试 API 对 tenant A 连续发送两条消息，确认 turn 连续；同外部 ID 重发确认 `duplicate=true`。
3. 用相同外部 user/message ID 调 tenant B，确认从 turn 1 开始，session/user hash 与 tenant A 不同。
4. 发送 Telegram/Slack 合法与非法签名夹具，分别确认 accepted 与 401，非法请求没有 Worker 调用。
5. 群内两个成员发送消息，确认共享群 session；另一个群、thread 或租户不共享。
6. 调用 allow 工具、deny 工具和 `require_confirmation`/`side_effects` 工具；修改参数复用 nonce 必须失败。相同 turn/tool/参数换 ToolCallID 重放时不得再次执行；模拟普通执行错误应产生 unknown，检查脱敏 Admin 列表和带 expected version 的决议；审计只含参数 hash 与 opaque operation key。
7. 让 IM Adapter 首次投递失败，重投后回复文本相同且 Agent turn 只增加一次。
8. 超过输入、RPM、月预算和 allowed user 限制，确认在模型前拒绝，审计 decision/error_type 正确。
9. 配置 OTLP，按 trace ID 查看 callback→worker→Runner→Inbox/Outbox→Session/Memory/Summary/Audit→send 的 storage spans，并确认 prompt/response/secret 不出现；生产 Compose 还必须查询 Jaeger 并扫描 canary。
10. reload 一个租户 v2 后 rollback，确认其他租户配置不变；重启控制器/服务后重新读取历史 revision、release 和节点状态，并验证 rollback 仍只创建新 release。

## 8. 上生产前的 P0 缺口

以下条目是从“完整赛题原型”走向“生产系统”的硬门槛，文档设计不能替代实现：

1. ~~持久 SQL Inbox/Outbox、DLQ、Session 分区 FIFO、数据库统一时钟和事务故障窗口基线~~ **代码与 SQL 集成测试已实现**：`queue.backend=postgres` 启用 `runtime_inbox`/`runtime_outbox`（[store 包](../trpcservice/store/store.go)），回调先落库再 ACK；完整 Session lane 的 Inbox/Outbox 前驱检查保证同会话严格顺序；每个可用 Worker 一记录一租约、独立 attempt token、自动续租、指数退避与 dead letter；due/lease/reclaim 使用 PostgreSQL 时钟，状态迁移在取得 attempt 行锁及 Outbox 插入后重新检查数据库 deadline；Agent result 可跨 Inbox 事务失败重放，Outbox 校验 tenant binding 并恢复原 trace。真实 PostgreSQL 已覆盖并发建表、遗留 schema、并发分区租约、快慢应用时钟、行锁/Outbox 插入等待跨租期、过期 fencing 和 Outbox INSERT 异常的整事务回滚；发布门槛仍包括网络分区、SIGKILL 与恢复 drain 演练。
2. ~~Session 版本化提交/CAS、fencing，以及同 DSN Session Commit + Inbox lease 校验/Outbox insert/Inbox processed 的组合事务~~ **限定 SQL 组合已实现**：v2 required 还会验证 Task/Inbox rollout 元数据和 Queue/Session/事务连接 database identity；未来版本/能力/identity 不兼容停放而不消耗死信预算，当前 v2 格式/元数据漂移才进入 DLQ。participant 在同一事务复核 Session fence 与 Inbox owner/attempt/路由身份/deadline，Outbox 失败整体回滚，重复 Turn 使用 committed canonical replay。~~IM send operation ledger、独立 002/003 migrator、业务启动 verify-only、runtime DML 最小权限，以及真实副作用工具 operation ledger/guard~~ 也已有代码、迁移、部署清单和自动测试基线。本次隔离 PostgreSQL 门禁已覆盖 14 个包并全部 `PASS`、无 `SKIP`；生产级节点退出、数据库断连、提交确认丢失、内容安全接管、Secret/RBAC 和 trace/storage 证据已由 P-01–P-05 的 Compose 门禁提供。
3. ~~SQL Session~~、~~外部 Memory~~、~~PGVector Knowledge~~、~~S3-compatible Artifact + SQL recovery metadata~~、~~008 Summary/Memory watermark/SQL Audit/vector migration ledger~~ 已完成 Runtime 装配与 fail-closed 配置；仍需 provider 真实故障/隔离演练、外部 reconciliation 和审计归档策略。
4. ~~分布式限流、预算 reserve/settle、配置 revision 控制面~~ 已有 Redis/006/007 代码和对应隔离门禁；Redis ApprovalBackend 和有界 Runtime 缓存已实现；剩余是 Redis 持久化/故障切换验收，以及 unknown 的 provider 查询/自动 reconciliation。
5. 基础入模遮蔽/拦截与出站规则 DLP 已实现（见 [文本治理](privacy-and-storage-metrics.md)）；仍需语义识别/可逆 tokenization、附件安全流水线、资源级工具授权与沙箱。
6. ~~Adapter/queue/model/tool/storage spans、依赖感知 readiness、SQL Audit spool 恢复与逐对象对账~~ 已有低基数 storage spans、健康注册表、审计 hash chain/spool drain、Secret canary/trace 扫描脚本；仍需平台侧 SLO 告警、WAL/WORM 和真实灾备演练。
7. ~~OIDC/RBAC Admin、SecretResolver 外部引用、生产 SecretProviderClass/Workload Identity 示例、secret 值脱敏~~ 已实现；MFA/KMS/mTLS/NetworkPolicy 和真实 Secret Manager 轮换由部署平台提供，不在本仓库本地 Compose 必需范围。
8. ~~迁移 ledger、通用状态执行器、Redis Session→SQL 计划停机 adapter 与本地 JSONL→PGVector adapter~~；继续实现在线双写、Redis Memory mutation journal、发布系统 routing 自动化，以及 canary/cutover/rollback 的真实故障演练。

只有 P0 中与实际部署范围相关的条目实现并通过故障演练后，才能把相应矩阵状态从“部分实现/生产设计”升级为“生产已实现”。
