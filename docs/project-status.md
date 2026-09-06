# 项目现状核对

> 本文是当前工作区的状态核对记录。当前代码、迁移、测试和阶段报告优先；历史基线和过程计划只用于追溯，不覆盖当前状态。

## 1. 核对结论

当前项目已完成平台核心领域、durable execution、channel-aware Sender、Lark/Telegram boundary 和 P0-09G-C/D 的可验证边界；本轮 production ObjectStore composition、Artifact metadata repository、真实 MinIO object gate 与 Redis restart failover boundary 也已通过。Redis restart ordinary/race 使用独立 client/store、PostgreSQL authority epoch、epoch-aware acquire、new-owner renew、old-owner fencing 和 durable fact check；full ordinary/race 在 owner-scoped synthetic prerequisites 下均 PASS。历史 host-process dynamic-port diagnosis 已修复并单独记录。按本轮 scope decision，P0-09G-R/P0-09 在定义的 P0 closure boundary 内为 `verified`；scheduler/lifecycle cleanup 是 deferred、非 P0 门禁，enabled-object 与 PostgreSQL recovery 的联合故障是并列 boundary 的后续 evidence；P1-06 继续 blocked/not started。

- 已有 Go 服务入口、基础 HTTP 路由和本地内存联调链路。
- 已有租户、Agent、Channel Binding、Session、Event、Memory、Summary、Artifact、Audit 的领域模型和校验测试。
- 已有 PostgreSQL 版本化迁移框架，以及包含 15 张业务表和 2 张协调表的数据库 schema。
- 已有 Redis/PostgreSQL 两套协调后端的幂等 Claim、Session Lease、epoch/fencing、续租和故障切换抽象。
- 已有 Redis 三维固定窗口限流、熔断、恢复探测和跨后端契约测试。
- 已有向量存储领域契约：`tenant.BackendPolicy.Vector`、`platform.VectorStore` 和 `memory.vector_ref`；尚无具体 Milvus client/adapter、索引 worker 或生产检索接线。
- 尚未把 PostgreSQL/Redis 实现接入主进程的通用业务 Store；主进程仍使用 `MemoryStore`。P0-09A 已新增独立的 execution result fenced Repository，并已通过本轮真实 PostgreSQL acceptance；它不是完整业务 Store。
- 已有 P0-07 的 framework-independent Agent Runtime 边界，并以公开 `Event.IsRunnerCompletion()` 作为运行完成信号；framework 内部全部 goroutine 的全局退出仍未由上游公开合同证明。
- 已有 P0-08A/B/C/D 的合同切片：版本化 Job/Queue、Execution、固定并发 Worker 和 transport-neutral Gateway；R2 History/安全错误分类、R3 contract-level fencing、R4 delivery failure semantics、R5 bounded Worker shutdown 均已按本轮源码和测试重新核对为合同级/in-process 证据，不等同于生产异步链路。
- 已有 P0-09B 的真实 PostgreSQL durable Job Queue，覆盖 Job 持久化、数据库 claim、visibility recovery、Ack/Nack/ExtendVisibility、tenant isolation 和连接丢失恢复；P0-09C 已完成真实 PostgreSQL atomic result commit + Queue Ack、rollback、fencing/reconciliation 和 durable Worker acceptance，且本轮关闭了 Queue 短 lease 集成测试 race blocker。P0-09D 已完成真实 PostgreSQL Outbox Repository boundary、dedup、database claim、lock reclaim、retry、DLQ 原子 transition、tenant isolation、rollback 和 Docker restart recovery。P0-09E 已完成 storage-neutral Outbox Dispatcher、有限 RetryPolicy、Sender 四类 outcome、安全 retry/DLQ 编排和 bounded shutdown 的 Fake/真实 PostgreSQL/Docker acceptance。P0-09F 已完成 result + Queue Ack + reply Outbox 的同一 PostgreSQL transaction boundary、durable Worker 接线、rollback、dedup、fence、partial/unknown reconciliation 和独立 pool acceptance。P0-09G-A 已完成 channel-aware Sender boundary：版本化 reply routing payload、tenant/channel/destination 校验、channel registry、provider-neutral builder、四类 SenderOutcome 和 Fake/真实 PostgreSQL Outbox Dispatcher 接线已验证。P0-09G-B1 已新增并验证 Lark real Sender boundary，授权测试租户的真实 token/message evidence 已通过；P0-09G-B2 已新增并验证 Telegram real Sender/Adapter boundary，真实 `getMe`/`sendMessage` evidence 已通过。P0-09G-C 已通过真实测试 PostgreSQL composition root 验证 Lark/Telegram webhook、Claim、Queue、Worker、atomic completion、Outbox、Dispatcher 和 channel routing；本轮 Provider transport 为 deterministic fake。P0-09G-D 已通过 in-process coordinator/Web gate、实际 binary startup/listen failure、真实 Linux SIGTERM clean subprocess、SIGKILL 后 Queue lease reclaim、Outbox lock reclaim，以及本轮同 network Linux PostgreSQL outage/restart/reconnect、healthz、fencing 和 final durable facts。历史 Windows host-mapped restart case 的受控 unexpected EOF 仍独立记录，不覆盖本轮 PASS。provider-side dedup、完整业务 Repository 和 framework global Wait/Done 仍未完成。P0-09A 仅覆盖 execution result fenced commit。

因此，P0-01 至 P0-06 应标记为“已完成的基础层”，不能等同于“首个生产版本已完成”。后续开发从新的总计划 `implementation-plan.md` 的 P0-07 开始。

## 2. 已完成能力

| 阶段 | 实际落地 | 证据位置 | 状态 |
| --- | --- | --- | --- |
| P0-01 | Go module、CI、gofmt、lint、test、vet 基线 | `go.mod`、`.github/workflows/ci.yml`、`lint.sh` | 已完成 |
| P0-02 | `Tenant`、`AgentApp`、`ChannelBinding`、`BackendPolicy`、`TenantContext`、Resolver 契约 | `trpcservice/tenant/`、`trpcservice/config/` | 已完成 |
| P0-03 | Session 状态、事件、Memory、Summary、Artifact、Audit 和脱敏模型 | `trpcservice/session/`、`memory/`、`artifact/`、`audit/` | 已完成 |
| P0-04 | Session ID、DedupKey、Repository/Claim/Lease/Outbox 契约、Fake 实现和契约测试 | `trpcservice/session/key.go`、`trpcservice/storage/` | 已完成 |
| P0-05 | 迁移版本、checksum、advisory lock、readiness、up/down 脚本 | `migrations/`、`trpcservice/storage/postgres/` | 已完成 |
| P0-06 | Redis/PostgreSQL Claim、Lease、epoch、fencing、failover、circuit breaker、rate limit | `trpcservice/storage/{redis,postgres,coordination}/`、`trpcservice/ratelimit/` | 已完成 |
| P0-07 / R1 | Runtime 以公开 runner completion event 驱动成功判定，继续 drain tail、等待 framework channel close 和 adapter pump；取消、错误链和有界清理 | `trpcservice/agent/runtime.go`、`runtime_lifecycle_test.go` | contract-level verified；framework 全局 Wait/Done 仍无公开证明 |
| P0-08A | 版本化 Job/DTO、租户恢复、FakeQueue visibility/Ack/Nack/Close contract | `trpcservice/queue/{job.go,fake_queue.go,queue_test.go}` | contract-level verified；非 durable |
| R2 | 有界 History 快照、schema/version 校验、Worker 顺序映射和 safe Nack reason | `trpcservice/{gateway,queue,worker}` 测试 | contract-level verified；非 Repository 事实源 |
| R3 | Validate -> fenced Commit 边界、Fake takeover rejection 和错误链 | `trpcservice/execution/{execution.go,execution_test.go}` | contract-level verified；真实 Repository 适配见 P0-09A |
| R4 | Ack/Nack/visibility 未确认时的 unresolved、有限 retry、Receive backoff 和安全分类 | `trpcservice/worker/{worker.go,worker_test.go}` | contract-level verified；仅 in-process Queue |
| R5 | Worker Stop owner、取消/drain、action 状态、recovery/Close 顺序 | `trpcservice/worker/{worker.go,worker_test.go}` | contract-level verified；非合作 Queue action 可在 Stop 返回后由 deferred finalizer 继续 |
| P0-08D | Gateway fast ACK transport-neutral contract | `trpcservice/gateway/{gateway.go,gateway_test.go}` | contract-level verified；Web/CMD 异步接入 deferred |
| P0-09A | PostgreSQL execution result fenced atomic commit、幂等/冲突和 rollback contract | `trpcservice/storage/postgres/execution_result.go`、`migrations/000003_execution_result.*`、`docs/P0-09A验收报告.md` | verified；真实 PostgreSQL 6/6 acceptance、race、rollback 和 migration 通过；不含 Queue Ack 原子性 |
| P0-09C | Execution Result + Queue Ack 同一 PostgreSQL transaction、rollback、fencing/reconciliation、durable Worker atomic completion | `trpcservice/storage/postgres/completion.go`、`trpcservice/worker/worker.go`、`docs/P0-09C验收报告.md` | verified；真实 PostgreSQL atomic completion、Worker integration、focused 25/25 race、完整受影响范围 race 和 vet/format 通过；不含 Outbox/Retry/DLQ/生产异步装配 |
| P0-09D | PostgreSQL Outbox Repository：durable Enqueue、dedup、database claim、lock reclaim、complete/retry、DLQ transaction、tenant isolation 和 restart recovery | `trpcservice/storage/postgres/outbox.go`、`migrations/000005_outbox_repository.*`、`docs/P0-09D验收报告.md` | verified；真实 PostgreSQL targeted 9/9、storage/ratelimit regression、race、vet、全仓 test/race/vet 通过；不含 Dispatcher、Sender、RetryPolicy 或生产接线 |
| P0-09E | Storage-neutral Outbox Dispatcher、bounded RetryPolicy、Sender outcome 四分类、安全 durable retry/DLQ 编排和 bounded shutdown | `trpcservice/outbox/`、`docs/P0-09E验收报告.md` | verified；Fake、真实 PostgreSQL、独立 pool competing claim、expired-lock fencing、Docker restart/reclaim、race、vet 和全仓普通测试通过；不含真实 IM、Web/CMD async assembly 或生产 SIGTERM |
| P0-09F | Execution result、Queue Ack、reply Outbox 的同一 PostgreSQL transaction；durable Worker 接线和三事实 reconciliation | `trpcservice/storage/postgres/completion.go`、`trpcservice/execution/repository_sink.go`、`trpcservice/worker/`、`docs/P0-09F验收报告.md` | verified；真实 PostgreSQL rollback、dedup/conflict、tenant/fence/delivery、独立 pool、unknown、Worker pending Outbox、storage/execution/worker/outbox race 和 vet 通过；不含真实 Sender、IM、Web/CMD async assembly 或生产 SIGTERM |
| P0-09G-A | Versioned channel-aware reply Sender boundary：routing payload、tenant/channel/destination validation、registry、provider-neutral builders、四类 outcome 和 Dispatcher adapter | `trpcservice/channels/sender.go`、`trpcservice/outbox/channel_sender.go`、`trpcservice/outbox/dispatcher_integration_test.go`、`docs/P0-09G-A验收报告.md` | verified；Fake、真实 PostgreSQL Outbox、race、retry/idempotency、fencing、Docker recovery 和安全 code 通过；不含真实 Lark/Telegram Provider、Web/CMD async assembly 或生产 SIGTERM |
| P0-09G-B1 | Lark real Sender boundary：schema v3 BindingID、SecretRef/token、固定 API contract、官方 uuid、四类 outcome 和 Dispatcher adapter | `trpcservice/channels/lark/`、`trpcservice/channels/sender.go`、`trpcservice/execution/repository_sink.go`、`trpcservice/outbox/dispatcher_integration_test.go`、`docs/P0-09G-B1验收报告.md` | verified (Lark real sender boundary)；授权测试租户 token/message evidence PASS；provider-side dedup、生产装配和 exactly-once 未证明 |
| P0-09G-B2 | Telegram Sender/Adapter：固定 Bot API、Binding/SecretRef、numeric chat/topic routing、webhook secret/update parser、四类 outcome 和 Dispatcher adapter | `trpcservice/channels/telegram/`、`docs/P0-09G-B2验收报告.md` | `verified (Telegram real sender boundary)`；真实 evidence 独立引用，provider-side dedup、exactly-once 和生产装配未证明 |
| P0-09G-C | Lark/Telegram verified webhook 到 PostgreSQL Claim/Queue/Worker/Outbox/Dispatcher 的 production async assembly boundary | `cmd/trpc-service/production.go`、`cmd/trpc-service/production_test.go`、`trpcservice/gateway/ingress.go`、`docs/P0-09G-C验收报告.md` | `verified (production async assembly boundary)`；TEST_DATABASE_URL PostgreSQL 联合链路、五个失败窗口、scoped race/vet/gofmt/diff PASS；Provider transport 为 deterministic fake，真实外部 send 次数为 0 |
| P0-09G-D | 主进程生命周期、SIGTERM、依赖 readiness 和端到端恢复 | `cmd/trpc-service/lifecycle.go`、`cmd/trpc-service/production.go`、`cmd/trpc-service/lifecycle_test.go`、`cmd/trpc-service/lifecycle_process_test.go`、`cmd/trpc-service/lifecycle_postgres_restart_test.go`、`docs/P0-09G-D验收报告.md` | `verified (process lifecycle and recovery boundary)`；Linux SIGTERM、coordinator/draining、SIGKILL reclaim 和本轮同 network PostgreSQL outage/restart/reconnect、healthz、Queue/Outbox、fencing、durable identity/facts 均通过；历史 host-mapped EOF 仍独立记录 |
| P0-09G-R | P0-09 总审查和关闭 | `verified (defined P0 closure boundary)`；production ObjectStore composition、Artifact metadata persistence/reconciliation、独立 MinIO gate、Redis restart failover 和 current same-network G-D recovery 已通过；scheduler/lifecycle cleanup 明确不是本轮 P0 门禁，ObjectStore/G-D 是并列 boundary，enabled-object 联合故障为后续 evidence；真实 Provider、provider-side dedup、完整业务 Repository、认证和 framework global Wait/Done 仍未证明 |

| 原计划 P0-10 映射 | Job Queue、Gateway、Worker 和 durable execution 的可验证边界已由 P0-08、P0-09B、P0-09C、P0-09F 分阶段完成 | `trpcservice/queue/`、`trpcservice/gateway/`、`trpcservice/worker/`、对应 P0-09 验收报告 | 原计划编号已提前完成其 contract/durable slice；P0-09G-C 已验证主进程 production async assembly boundary |
| 原计划 P0-11 映射 | Outbox Repository、Dispatcher、Retry/DLQ、三事实 completion 和 channel-aware Sender boundary 已由 P0-09D 至 P0-09G-A 分阶段完成 | `trpcservice/storage/postgres/outbox.go`、`trpcservice/outbox/`、对应 P0-09 验收报告 | 原计划编号已提前完成其可验证组件边界；P0-09G-C 已验证测试 PostgreSQL production assembly，真实 Provider 和进程生命周期仍未完成 |

P0-06 的细分验收见 `p0-06-acceptance-matrix.md`。该矩阵记录了单后端、跨后端、真实 Redis/PostgreSQL、竞态、故障注入和 fail-closed migration 检查。

## 3. 当前可运行链路

当前入口在 `cmd/trpc-service/main.go`：
设置 `DATABASE_URL` 并提供完整 `BOOTSTRAP_*`/SecretRef 后，主进程 composition root 已装配 PostgreSQL Queue、Worker、Atomic Completion、Outbox、Dispatcher、Lark/Telegram webhook、真实 Sender constructors，以及按 `BOOTSTRAP_OBJECT_BACKEND=s3` 启用的 production ObjectStore 与 ArtifactMetadataRepository。default `none` 不构造 object client，enabled object 缺失配置 fail-closed；现有 `/api/chat` 仍是同步兼容接口。P0-09G-C/D 的历史与本轮 PostgreSQL assembly/recovery evidence 保留，本轮 production object evidence、Redis restart failover evidence 与 current owner-scoped same-network G-D evidence 通过；Provider transport 本轮为 deterministic fake，真实外部 Provider 请求仍为 0。
1. 创建 `platform.MemoryStore`。
2. 写入一个默认 `demo` 租户。
3. 如果设置 `DATABASE_URL` 并提供完整 bootstrap identity/SecretRef，composition root 初始化 PostgreSQL pool、Queue、Worker、Atomic Completion、Outbox、Dispatcher 和 Lark/Telegram Sender；任一必需配置或初始化失败即 fail-closed。
4. 创建 `platform.Runner`，其 Responder 为 `platform.EchoResponder`。
5. 启动 HTTP 服务。

当前 HTTP 路由由 `trpcservice/web/server.go` 提供：

| 路由 | 当前行为 | 生产缺口 |
| --- | --- | --- |
| `GET /healthz` | startup 未完成、数据库不可用或 draining 时返回安全 `503`；依赖 ready 时返回 `200` | 完整业务依赖/版本检查仍有限；Milvus readiness/retrieval 尚未接入 |
| `POST /api/tenants` | 依赖 Context 中的 `tenant.admin` 权限，写入内存租户 | 没有真实认证、管理服务、PostgreSQL 写入和审计 |
| `POST /api/chat` | 保留的同步开发兼容接口，执行 Runner，读取内存历史并返回文本 | 不属于生产异步通道；没有 API 鉴权、限流、SSE、真实生产存储或生产 Web Chat 承诺 |
| `POST /webhook/{channel}/{external_app_id}` | 完整 bootstrap 配置下走 Lark/Telegram 验签/解密、服务端 Binding/TenantContext、PostgreSQL Claim/Queue、Worker、atomic completion、Outbox 和 Dispatcher；测试 composition root 与 server-owned draining gate 已验证 | Provider transport 为 deterministic fake；真实外部 send、provider-side dedup、Binding/Identity 完整持久化、Milvus vector index 和完整业务 Repository 未证明 |

`trpcservice/channels/channels.go` 中的 `wecom` 和旧 `telegram` 类型仍是协议形状的占位/兼容适配器；P0-09G-A 的 provider-neutral builder 不等于真实 Provider。P0-09G-B1/B2 的真实 Sender evidence 分别独立记录；P0-09G-C 已把 Lark/Telegram webhook 接入 production composition root，并在测试 PostgreSQL 与 deterministic fake Provider transport 下验证异步链路；P0-09G-D 已补齐 Linux SIGTERM clean shutdown、SIGKILL reclaim 和本轮同 network PostgreSQL outage/restart/reconnect chain。历史 host-mapped `unexpected EOF` 不再代表当前 Linux network recovery 缺口。Milvus 尚未接入 webhook/Worker/Runner；它属于后续 P1-06 派生索引能力，不能影响现有 `/api/chat` 同步兼容边界。

## 4. 代码边界

### 已存在的包

- `trpcservice/tenant`：租户实体、Context、状态和 Resolver 接口。
- `trpcservice/config`：租户配置解析/解析器相关模型。
- `trpcservice/session`：Session、状态迁移、事件、key 规则。
- `trpcservice/memory`、`artifact`、`audit`：领域模型和审计脱敏。
- `trpcservice/storage`：Repository、Claim、Lease、Outbox 契约、Fake 实现、契约测试。
- `trpcservice/storage/redis`：Redis 协调实现。
- `trpcservice/storage/postgres`：连接池、迁移、readiness、协调实现，以及 P0-09A execution result、P0-09D Outbox Repository 和 P0-09F atomic completion 的真实 PostgreSQL 边界。P0-09G-A/B1 通过既有 Outbox Repository 消费已提交 reply，不新增 storage migration。Milvus 尚无具体实现；P1-06 将在该边界之外增加 vector adapter/index worker。
- `trpcservice/storage/coordination`：故障切换和断路器模型。
- `trpcservice/ratelimit`：Redis Lua 三维固定窗口限流。
- `trpcservice/queue/postgres_queue.go`：P0-09B 真实 PostgreSQL durable Queue；P0-09G-C 已由 composition root 接入异步入口。
- `trpcservice/platform`：仅用于当前同步内存联调的兼容层。
- `trpcservice/web`、`channels`：HTTP/Adapter 壳；Web server 具有并发安全的 accepting/draining/readiness gate，`channels/sender.go` 提供可构造、可测试的 channel-aware Sender boundary，不是生产 Provider 接入。
- `cmd/trpc-service/lifecycle.go`：process lifecycle coordinator、预绑定 listener、Serve error owner、signal/forced-close、剩余 shutdown deadline 和 safe exit status；只使用 Worker/Dispatcher 的公开生命周期 contract。
- `trpcservice/outbox`：storage-neutral Dispatcher、Sender outcome classification、bounded RetryPolicy 和 shutdown boundary。
- `internal/testinfra`：Docker 依赖测试基础设施。

### 尚不存在的生产实现

仓库当前仍没有以下生产实现：

`auth`、`identity`、`governance`、`telemetry`、具体 `vector/milvus` adapter、`object`、`admin`、完整业务版 `storage/postgres` Repository、provider-side dedup 和完整生产业务配置管理仍未实现或证明。P0-09G-B1/B2 的真实 Sender evidence、P0-09G-C 的 production async assembly 和 P0-09G-D 的 process lifecycle/recovery boundary 分别记录；P0-09G-D 已 verified，但不等于完整业务 Repository、Milvus 检索或 P0-09 整体完成。`platform.VectorStore` 等接口不代表 Milvus backend 已存在。

`trpcservice/outbox` 提供可构造、可测试的 storage-neutral 边界；P0-09G-C 由 CMD composition root 将其与真实 PostgreSQL Outbox、Dispatcher 和 channel Sender 接入测试生产入口，但本轮使用 deterministic fake Provider transport。

## 5. 数据库现状

`migrations/000001_initial.up.sql` 创建以下事实源表：

`tenant`、`agent_app`、`channel_binding`、`user_identity`、`session`、`session_event`、`message_dedup`、`memory`、`summary`、`artifact`、`audit_log`、`outbox_message`、`dead_letter`、`agent_release`、`tenant_config_version`。

`migrations/000002_coordination.up.sql` 增加：

- `message_dedup.epoch`。
- `coordination_epoch`：租户资源的 epoch authority。
- `session_lease`：Session owner、租约、fencing token 和过期时间。

`migrations/000003_execution_result.*` 增加 P0-09A fenced execution result 事实表；`migrations/000004_job_queue.*` 增加 P0-09B durable Job Queue 事实表、claim 状态、visibility 和 delivery token。

数据库 schema 已经包含 Outbox、DLQ、发布版本和配置版本；`000005_outbox_repository` 增加 Outbox 状态/锁一致性、payload/error 大小边界和 claim/reclaim partial index。P0-09E Dispatcher 使用 PostgreSQL Outbox 作为事实源；P0-09F 在同一 PostgreSQL transaction 内生成 pending reply Outbox；P0-09G-A/B1/B2/C/D 消费、路由、验证或恢复已提交的 channel-aware reply payload，不新增 migration，也不改变 PostgreSQL durable 状态事实源。现有 `memory.vector_ref` 只是向量引用字段；Milvus index projection、index work/reconciliation 表或 migration 尚未实现，P1-06 需另行设计且不得把 Milvus 写入塞入 P0-09F 事务。

## 6. 验证基线

在当前工作区实测：

```text
go test ./cmd/trpc-service/... ./trpcservice/web/... ./trpcservice/gateway/... ./trpcservice/worker/... ./trpcservice/outbox/... ./trpcservice/queue/... -count=1  PASS
go test 同一 G-D 受影响范围 -race -count=1                         PASS
go vet ./...                                                       PASS
gofmt -l G-D 受影响目录                                          无输出
git diff --check                                                   FAIL/KNOWN EXISTING（既有 LF/CRLF 与 whitespace churn；未清理用户修改）
Linux real SIGTERM subprocess                                         SKIP（本机 Windows；复用 G-D 历史 Linux evidence）
G-D same-network Linux PostgreSQL outage/restart/reconnect              PASS（owner-scoped Docker runner；ordinary/race；outage 503、raw/wrapper pools、SELECT 1、healthz、Queue/Outbox、fencing、final facts）
affected P0-09 ordinary tests                                      PASS（显式 synthetic bootstrap/runner 前置条件）
affected P0-09 race tests                                         PASS（显式 synthetic bootstrap/runner 前置条件）
```

G-D historical evidence 覆盖真实 Linux Unix `SIGTERM`、bounded lifecycle cleanup、Queue/Outbox reclaim、stale delivery fencing 和历史同 network recovery；本轮 owner-scoped same-network runner 的 current G-D ordinary/race 已完整 PASS，包含 outage 503、fresh pools、healthz、Queue/Outbox、fencing 和 durable facts。固定 loopback host-mapped endpoint regression ordinary/race 也通过，历史 dynamic-port transport failure 未被全局忽略。

阶段报告：`docs/P0-07完成报告.md`、`docs/P0-08完成报告.md`。

`go test ./...` 会运行内部测试基础设施和真实 PostgreSQL/Redis 集成测试；因此本地环境需要可用的 Docker 或相应测试依赖。CI 当前执行格式检查、`lint.sh`、全量测试以及租户边界 race test。

## 7. 主要风险与文档规则

- 不要把 `DATABASE_URL` 已配置解释为业务数据已经使用 PostgreSQL。
- 不要把 `channels` 中的协议占位实现或 P0-09G-A 的 provider-neutral builder 解释为飞书（Lark）或 Telegram 生产接入。
- 不要把 Repository 接口和迁移表解释为 Repository 已经实现。
- 不要在新的任务中重新规划已完成的 P0-01 至 P0-06；若发现基础契约不足，应增加兼容迁移或单独的修订任务。
- 所有后续能力必须先通过 `TenantContext`，并在数据 key、SQL 条件、对象 key、向量 metadata、日志和指标中明确租户边界。
- P1-06 Milvus 必须保持 PostgreSQL 事实源、Outbox/index worker、tenant filter、幂等投影、reconciliation 和明确降级语义；Milvus client 可用不等于生产向量检索完成。

## 8. 当前验证与状态核对

历史 G-D focused evidence 覆盖真实 Linux Unix `SIGTERM`、bounded lifecycle cleanup、Queue lease/Outbox lock reclaim 和 fencing；本轮 owner-scoped same-network Linux runner 的 G-D outage/reconnect ordinary/race 与 production object composition 另有 current PASS。最初 host-process dynamic endpoint 曾导致 `pool_ping=connection_refused`/`deadline=EXHAUSTED`，已通过固定 loopback port 修复；不能把这些诊断与 production regression 混同。

历史同网络 Linux runner 的 G-D test evidence 与本轮 current owner-scoped same-network G-D PASS 分开记录；host-mapped fixed-port regression 的 raw/wrapper/SELECT 1 前后 restart evidence 也通过。

Object boundary 根据当前源码与本轮 real gate 证据归类为 B + C + E：独立 S3-compatible ObjectStore adapter 与 production composition 均已通过 ready、认证 S3 probe、真实 Put/Get/Head/Delete、hash/size/MIME、actual presigned URL、tenant/traversal 安全、context cancellation、restart/reconnect、object MIME/size/SHA mismatch reconciliation 和 owner cleanup。PostgreSQL ArtifactMetadataRepository 已接入 production composition，复用既有 `artifact` schema，验证 tenant/session/message relation、pending -> ready、pending -> failed、ready object mismatch/丢失 -> expired，以及 status-gated presign。current G-D recovery 与 Redis restart failover ordinary/race 也已通过；full ordinary/race 在 owner-scoped synthetic prerequisites 下通过。ObjectStore/G-D enabled 联合故障场景未执行，但按 scope decision 它是并列 boundary 的后续 evidence，不阻塞定义范围内的 P0 closure；P0-09G-R/P0-09 在该范围内为 `verified`。

Milvus 当前仍是未实现的 P1-06 目标：已有 `BackendPolicy.Vector`、`platform.VectorStore` 和 `memory.vector_ref` 契约/字段，但没有具体 client、adapter、index worker、Milvus deployment 或 production retrieval wiring。

## 14. 本轮最终 Worker 与同网络 Runner 复验

Worker focused test 已修正为确定性 Nack/recovery failure seam，并通过普通 `-count=50`、race `-count=10`。测试断言 `Stop` 同时包含 `ErrDrainTimeout` 和 `ErrDeliveryUnresolved`，无 Commit/Ack，Queue close 最多一次，delivery 明确保留在 `inFlight`；没有接受后台成功恢复的互斥结果。

同 network Linux runner 的历史 PostgreSQL recovery evidence、本轮 owner-scoped same-network G-D recovery、Redis restart failover、production object composition、独立 MinIO object transport gate 和受影响 ordinary/race 均保留并分别记录。Redis gate 使用 owner-scoped Redis container/network/volume、独立 new-owner client/store、epoch-aware acquire、new-owner renew、old-owner fencing、durable fact check 和 cleanup；production object gate 使用 owner-labeled MinIO resources。current G-D、Redis restart 和 full ordinary/race 均 PASS；ObjectStore/G-D enabled 联合故障是并列 boundary 的后续 evidence，自动 reconciler/background lifecycle cleanup 是 deferred、非 P0 门禁；P0-09G-R/P0-09 在定义范围内 verified，P1-06 仍 blocked/not started。

## 9. P0-09F 当前核对结果

P0-09F 只覆盖 execution result、Queue Ack、reply Outbox enqueue 的同一 PostgreSQL transaction boundary。`AtomicCompletionRequest.Outbox` 的 nil 语义仅为兼容 P0-09C 两项 contract；durable Worker 使用 `NewWithAtomicCompletion` 时会显式构造稳定 reply payload 和 tenant-scoped execution dedup identity。Outbox 初始为 pending、attempt=1、无 lock，提交后由独立 Dispatcher/Outbox Repository 处理。

真实 Lark/Telegram Provider send、provider-side UUID dedup、Web/CMD async assembly 的真实外部通道证明、完整业务 Repository、Milvus vector index、生产认证和 framework global Wait/Done 仍 deferred。unknown commit outcome 必须 reconciliation；unknown retry/发送可能重复，仍需要 provider-side dedup/idempotency。P0-09G-C 的测试 PostgreSQL assembly evidence 见 `docs/P0-09G-C验收报告.md`；P0-09G-D 的 Linux SIGTERM 和 stable-network PostgreSQL recovery evidence 见 `docs/P0-09G-D验收报告.md`。

## 10. P0-09G-A 当前核对结果

P0-09G-A 已验证 versioned reply routing payload、tenant/channel/destination validation、immutable registry、provider-neutral outbound builders、HTTPDoer transport seam、四类 safe SenderOutcome 和 Outbox Dispatcher adapter。真实 PostgreSQL 已验证 committed reply Outbox 的 claim、channel-aware send、completion、retry、竞争 claim、expired-lock fencing、Docker restart/reclaim 和既有 completion/Outbox/migration regression。Fake transport 未连接任何真实 Provider；历史 WeCom builder 只保留兼容/测试价值，不是当前生产目标。

P0-09G-C 修改了 Web/CMD production async assembly 接线，但没有修改 `/api/chat` 或 queue/worker 核心算法；production Dispatcher registration 在测试 composition root 中已验证。P0-09G-D 已加入 process lifecycle coordinator、预绑定 listener、server-owned draining、bounded shutdown、safe exit status、SIGKILL Queue/Outbox lock reclaim 和真实 Linux SIGTERM clean subprocess；本轮同 network Linux PostgreSQL production recovery chain 已通过，历史 host-mapped transport failure 作为独立边界保留。provider-side dedup guarantee、Milvus vector index、完整业务 Repository 和 framework global Wait/Done 仍 deferred。P0-09G-B1/B2 的实现、真实 evidence 门禁和保留边界分别见对应验收报告。

## 11. P0-09G-B1 当前核对结果

P0-09G-B1 状态为 `verified (Lark real sender boundary)`。`trpcservice/channels/lark/` 已实现服务端 binding/tenant/destination 校验、SecretRef/token resolver、固定官方 HTTPS base URL、官方 tenant access token 和文本消息 request/success contract、官方 `uuid` provider idempotency 输入、bounded response 与四类 outcome；授权测试租户的真实 token/message evidence 已通过。`ReplyOutboxPayload` 为 schema v3，`binding_id` 来自已解析 `TenantContext`，不来自用户可控 payload identity。

Lark Sender 不持有 Outbox Repository，不调用 durable mutation、Queue Ack 或 execution commit；Dispatcher 仍是唯一 `MarkCompleted`、`MarkRetry`、`MoveToDLQ` owner。`Retry-After` 不写入 durable 状态，由现有 bounded RetryPolicy 控制 retry timing。PostgreSQL dedup 不等于 Lark provider dedup；OutcomeUnknown 仍可能导致重复发送。

本轮使用明确授权的测试租户参数，通过显式门禁测试 `trpcservice/channels/lark/lark_real_test.go`（仅 `LARK_B1_REAL=1` 执行）验证真实 tenant access token、消息 HTTP 2xx/`Delivered` 和相同 durable identity replay；凭据仅存在于临时进程环境变量。provider-side uuid 去重未证明，Fake transport、unit test、Docker restart 或 PostgreSQL component evidence 也不得外推为 exactly-once。`/api/chat` 仍是同步兼容路径；P0-09G-C 测试 PostgreSQL assembly、P0-09G-D Linux SIGTERM 和 stable-network PostgreSQL recovery evidence 均已独立记录。

## 12. P0-09G-B2 当前核对结果

P0-09G-B2 状态为 `verified (Telegram real sender boundary)`。`trpcservice/channels/telegram/` 已实现固定 Telegram Bot API `getMe`/`sendMessage` sender、Binding/SecretRef、numeric chat ID、4096 字符 plain text 边界、429/5xx/400/401/403/timeout/unknown safe outcome、webhook secret verification、Update/Message/User/Chat parser 和 `update_id` dedup key projection。schema v4 正式表达可选 `message_thread_id`，旧 schema v3 Web/Lark payload 仍兼容；private/group/supergroup chat-level routing 和 private/supergroup topic 已测试，普通 group thread 明确拒绝。

P0-09G-C 已在 deterministic fake Provider transport 下验证 Telegram webhook 到 production Queue/Worker/Outbox/Dispatcher assembly；P0-09G-D 的真实 binary SIGTERM、crash/reclaim、PostgreSQL outage/restart/reconnect 也已分别验证。Telegram `sendMessage` 没有官方 provider-side dedup contract，exactly-once 不支持，OutcomeUnknown retry 仍可能重复发送。Milvus 不在 P0-09G 运行路径，后续由 P1-06 单独实现。

## 13. P0-09G-C 当前核对结果

P0-09G-C 状态为 `verified (production async assembly boundary)`。`cmd/trpc-service/production_test.go` 通过测试进程注入的 `TEST_DATABASE_URL` 创建唯一 schema，直接覆盖 `assembleProduction` 的同一 composition root。Lark 和 Telegram 分别验证 synthetic webhook、server-owned Binding/TenantContext、PostgreSQL Claim、durable Queue、real Worker/Executor、deterministic fake Runner、atomic result/Queue Ack/reply Outbox、Outbox claim、channel-specific recording Sender 和 Dispatcher completion。

Ingress 失败窗口覆盖 Claim 成功但 enqueue 失败、enqueue 成功但 Claim Complete 失败、HTTP ACK 丢失、pending claim takeover/fencing 和 duplicate concurrent webhook；稳定 Job/Execution identity 关闭 enqueue/complete replay 的重复窗口。Lark challenge/签名/AES-CBC fixture 和 Telegram secret/update/chat/thread fixture 独立构造，不调用生产验签/解密 helper。

P0-09G-C 修改了 Web/CMD production async assembly 接线，但没有修改 `/api/chat` 或 queue/worker 核心算法；production Dispatcher registration 在测试 composition root 中已验证。P0-09G-D 的历史与本轮 owner-scoped same-network process lifecycle/recovery evidence 保持独立；current G-D ordinary/race、Redis restart failover ordinary/race、host-mapped fixed-port regression 和 full ordinary/race 均 PASS。Redis restart failure 的根因是 epoch 未写入/比较 Redis physical lease key，已由 epoch-aware Redis acquire/renew/release 与独立-owner regression 修复。B1/B2 真实 Sender evidence 独立引用；G-D Provider transport 为 deterministic fake，真实外部 Provider send 次数为 0；Milvus vector index、provider-side dedup/exactly-once、完整业务 Repository 和 framework global Wait/Done 未证明。

## P1-04 当前最终状态

- `P0-09G-R: verified (defined P0 closure boundary)`；`P0-09: verified (defined durable, assembly and recovery boundary)`。本轮未修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher 核心算法。
- `P1-04: verified (functional Binding/Identity/SecretRef boundary)`。
- `P1-04 project release/security state: partial`，仅表示 govulncheck findings 尚未独立 remediation，不表示 P1-04 功能门禁失败。
- `P1-05: verified (functional Tool Policy/Guardrail boundary)`；`P1-06: ready / not started`，Memory/Summary/Milvus 未开始。
- `env://` 是当前唯一真实 resolver；`secret://` 只做格式验证并返回 validated-but-unresolved，不是 Secret Manager 支持。enabled channel 缺 ref/value 在 startup fail closed；`BOOTSTRAP_LARK_ENABLED` 和 `BOOTSTRAP_TELEGRAM_ENABLED` 默认 `false`；disabled optional Binding 不创建 adapter/sender 且不阻塞启动。
- PostgreSQL migration/repository ordinary/race、Ingress/composition ordinary/race、full ordinary 和 P1-04-R 修复后的两次独立 full race 均为 `PASS`，最终 race 结果无 `DATA RACE`。历史 Queue concurrent Receive bounded `i/o timeout` 保留在 P1-04-R 报告中。测试使用唯一 owner-labeled PostgreSQL/Redis 资源和唯一 schema，结束后 container/network/volume 均为零；未知资源未清理。
- Fake repository 仅作为 deterministic contract evidence；本轮新增真实 PostgreSQL evidence 覆盖 migration/constraint、Binding tenant isolation/CAS/lifecycle、Identity chat/thread conflict、SecretRef reference-only persistence、Audit metadata、rollback、cancel/deadline 和 durable read。
- Ingress 拒绝 caller tenant/channel/binding/destination override、unknown/disabled/expired/cross-tenant/cross-channel/cross-binding/scope-conflict/secret-failure 均发生在 Claim 前；Audit 写失败不绕过安全检查，也不阻塞已完成的 server-owned request。
- Lark、Telegram、Model Provider 真实请求均为 `0`；Provider transport 使用 deterministic fake；没有创建 Milvus resource。
- `go vet ./...`、affected Go `gofmt` 和 P1-04 targeted `git diff --check` 均 `PASS`。gopls check、gitleaks、deadcode 均 `PASS`（gitleaks clean）；govulncheck 为 `FAIL/findings`（5 个可达依赖漏洞），jscpd 为全仓 52 clones；trivy、opengrep、madge 为 `NOT RUN/UNAVAILABLE/N-A`；全树 diff-check 的非零仅来自既有无关文件 whitespace。

## P1-04-R 历史证据与当前结论

以下保留 P1-04-R 前的历史 failure：`TestPostgresQueueConcurrentReceiveHasOneActiveDelivery`，stage=`claim candidate`，category=`database transport i/o timeout`，deadline=`true`，`DATA RACE=false`；当时 targeted queue race diagnostic 通过，但不能覆盖 full race failure。P1-04-R 后续完成 test-only ready barrier、deadline-coupled timeout 分类和安全诊断，最终两次独立 full race 均 `PASS`，无 `DATA RACE`，因此当前 P1-04 功能状态为 `verified`。

安全扫描：gopls 无 error、仅有 performance suggestion；gitleaks `PASS/clean`；deadcode `PASS/clean`；jscpd 最终全仓 Go 140 文件只报告 `252 exact clones`、`2038 duplicated lines`、`5.29%`，不是 duplication-free；govulncheck `FAIL/findings`，5 个可达依赖漏洞；trivy/opengrep `UNAVAILABLE`；madge 对 Go 项目为 `N/A`。全局 diff-check 的非零仅来自既有无关文件 whitespace。

所有本轮 PostgreSQL/Redis container/network/volume 均 owner-scoped 且已清零；未知资源未清理。P0-09G-R 和 P0-09 保持各自 defined boundary 的 `verified`；P1-05 为 `verified (functional Tool Policy/Guardrail boundary)`；P1-06/Milvus 为 `ready / not started`。真实 Lark、Telegram、Model Provider 请求为 `0`，没有创建 Milvus resource，也没有修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher production core algorithm。

## P1-04-R Final Closure Investigation

Queue failure 已完成源码定性与 test-only 修复。历史 failure 保留：`TestPostgresQueueConcurrentReceiveHasOneActiveDelivery`，stage=`claim candidate`，category=`database transport i/o timeout`，last_stage=`Postgres concurrent Receive write`，deadline=`true`，`DATA RACE=false`。根因是 full-suite PostgreSQL-heavy 并行/ race scheduling 下 350ms receiver context 的 deadline 表现和过弱错误分类，不是 row lock/deadlock、schema collision 或 durable claim correctness defect。修复文件仅为 `trpcservice/queue/postgres_queue_integration_test.go`：ready channel barrier、context-expired 且 `net.Error.Timeout` 的局部分类、安全 pool stats 诊断；未修改 Queue production code、SQL、timeout、Ack/Nack、fencing、Completion 或任何 P0 core contract。

最终矩阵：focused ordinary `20/20 PASS`；focused race `20/20 PASS`；Queue package ordinary/race `10/10 PASS`；affected race packages `trpcservice/queue`、`trpcservice/storage/postgres`、`trpcservice/worker`、`trpcservice/outbox`、`cmd/trpc-service` 全部 `PASS`；full ordinary `PASS`；修改后两次独立 full race 均 `PASS`，失败 0、DATA RACE 0。中间同类 `ExtendVisibility/requireNoDurableDelivery` deadline transport failure和一次 test-only `undefined: err` 编译失败也已记录，未被删除或改写。

`govulncheck ./...` 重新确认 `exit=3`，5 个可达 findings 的准确矩阵已写入 `docs/P1-04部分验收报告.md`：`GO-2026-6061`/GHSA grpc `v1.65.0 -> v1.82.1`；`GO-2026-5970`/CVE-2026-56852/GHSA x/text `v0.21.0 -> v0.39.0`；`GO-2026-5004`/CVE-2026-41889/GHSA pgx `v5.7.2 -> v5.9.2`；`GO-2026-4394`/CVE-2026-24051/GHSA OTel SDK `v1.29.0 -> v1.40.0`；`GO-2025-3540`/CVE-2025-29923/GHSA go-redis `v9.7.0 -> v9.7.3`。相关版本在 P1-04 前已存在，均未自动升级；P1-04 functional closure 可记为 `verified`，project release/security closure 因 findings 和未授权 remediation 继续为 `partial`，没有声称 security clean。

P0-09G-R 和 P0-09 保持各自 defined boundary 的 `verified`；P1-05 为 `verified (functional Tool Policy/Guardrail boundary)`；P1-06/Milvus 为 `ready / not started`。真实 Lark、Telegram、Model Provider 请求为 `0`，Milvus resource 为 `0`；本轮 owner-scoped PostgreSQL/Redis 资源及 test-owned schema 均已 cleanup，最终 Docker owner container/network/volume 计数为 `0`。

## jscpd 最终统计更正

最终全仓 jscpd 覆盖 140 个 Go 文件，`exit=0`，`252 exact clones`、`2038 duplicated lines`、`5.29%`。此前 52 clones/2.71% 为 bounded 子集结果；govulncheck 的 5 个 findings 仍单独保持 `FAIL/findings`，没有 security clean 结论。

## 13. P1-05 Tool Policy、Guardrail 和统一脱敏

P1-05 当前状态为 `verified (functional Tool Policy/Guardrail boundary)`。源码依据是 `agent.ToolInvoker`/`ToolRequest`、Agent immutable `ToolSpec`、Executor deadline/TenantContext/lease boundary 和现有 Audit model；新增 `trpcservice/tool` 只包住现有 Tool invocation，不改变 Worker、Queue、Completion、Outbox 或 Dispatcher 核心算法。

已实现并测试：server-owned immutable registry；tenant、AgentApp、release/config version 和 Tool version/capability 一致性；unknown/disabled/undeclared/cross-tenant/caller override fail closed；missing、expired、invalid、unavailable、stale/version-mismatched policy 安全类别；pre-tool schema、bounded input、敏感字段、secret/token/Authorization、DSN、filesystem traversal 和 shell/code/admin/network capability guardrail；post-tool bounded output、敏感结果 redaction、raw backend error 隐藏、timeout/cancellation 和 unknown outcome；server-owned budget scope 的 count/time/concurrency/input/output bytes reservation/commit/release/reclaim；只写安全 ID、category、version、size、latency 和 fingerprint 的 Audit sink contract。生产默认装配空 registry、fail-closed policy、单一 Guardrail/redactor 和无 implementation 绑定的 fail-closed invoker。

Approval 没有当前 Admin/API 或 durable repository 依据，保持 `DEFERRED`：源码只固定 server-generated ID、tenant/requester、Tool/capability/resource scope、parameter fingerprint、policy version、status、expiry、version/CAS 和 bounded-use contract，`approval_required` 不执行 Tool。当前 budget 仅是 bounded process-local admission contract；PostgreSQL durable policy/approval/budget/audit persistence、Redis cache/coordination、token/cost/provider billing、完整生产 AuditRepository、完整 DLP/content moderation 和真实 Tool/provider evidence 均 `DEFERRED`。因此本节不新增 migration、不添加依赖、不声称 production IAM 或 provider exactly-once。

P1-05 focused ordinary/race、PostgreSQL/P1-04/Queue/Worker/Outbox/CMD affected ordinary/race、full ordinary/full race、`go vet ./...`、affected `gofmt` 和 targeted diff check 均通过，full race 为 `DATA RACE=0`。本轮 owner-scoped PostgreSQL/Redis synthetic resources 已在清理前后核对；Lark、Telegram、Model Provider requests 为 `0`，Milvus resources 为 `0`。当前远程安全 scanner `gitleaks`、`govulncheck`、`deadcode`、`jscpd`、`trivy`、`opengrep`、`madge` 和 gopls 均 `UNAVAILABLE/NOT RUN`；既有权威报告记录的 5 个 govulncheck reachable findings 仍独立保持 project release/security `partial`，没有 security clean 结论。

## P1-06A 当前验收状态

本节覆盖 P1-06A 关闭后的当前状态；文件前文中 `P1-06 ready / not started` 是 A 开始前的历史快照。当前为：`P1-06A: verified`；`P1-06B: blocked/unavailable by SDK and local Milvus prerequisite`；`P1-06C: not started`；`P1-06D: blocked by P1-06C`；`P1-06E: blocked by P1-06B/P1-06C/P1-06D`；总 `P1-06: partial`，不能由 A 推导完成。

当前源码证据是 `trpcservice/memory/model.go` 的 durable `memory.Memory` 和 `migrations/000001_initial.up.sql` 的 PostgreSQL `memory`/`summary` 表。仓库没有 `trpcservice/knowledge` durable model/repository，也没有 PostgreSQL Memory repository；因此 A 只实现已有 Memory fact 的 `MemorySource` 映射，不创建假 Knowledge fact。`Memory.VectorRef` 与 `memory.vector_ref` 未被修改或写入，继续作为 opaque compatibility marker，不作为 Milvus primary key。

新增 `trpcservice/vector` provider-neutral contract：server-owned `vd-v1-` identity、tenant context、source type/id/projection scope、source version/sequence、content hash、upsert/delete/tombstone、model/version/dimension/schema、bounded allowlisted metadata、bounded search request/result 和 category-only safe errors。identity 不含 content hash/version/sequence，重试与 source update 复用同一 ID；model/version/dimension/schema 变化不会错误复用不兼容 projection。metadata 不保存正文、原始 source ID、prompt/history、credentials、provider body 或 vector，source ID 只保留不可逆 fingerprint。

`MemorySource` 仅允许 validated `TenantContext` 且要求 Memory fact tenant 相同；`BuildTenantFilter` 只能从 validated context 生成，公共 search request 没有 tenant、collection、partition、raw expression 或 arbitrary filter 字段。`BackendPolicy.Vector=none` 的 `ResolveBackend` 返回无 client 的 `DisabledStore`，不调用 factory、不读取 credential、不 readiness probe、不启动 worker；enabled `milvus` 缺配置 fail closed，绝不回退 fake/memory/silently disabled。

`DeterministicEmbedder` 是 bounded、server-owned、fixed-dimension、model/version 明确的 local contract fake；ordinary/race 已通过。它不读取 credential、不发 Model Provider 请求，也不构成真实 embedding 语义质量证明。focused vector ordinary、race、vet、gofmt 均 `PASS`；受影响 `memory`、`tenant`、`audit`、`storage` ordinary 也 `PASS`。全套 ordinary 仍受既有 `TEST_DATABASE_URL` 缺失的 G-D/production tests 和 PostgreSQL command startup prerequisite 影响，见本轮验收报告。

Milvus SDK 选型尚未进入本轮依赖：当前 `go.mod` 无 SDK。已核对官方 `github.com/milvus-io/milvus/client/v2@v2.5.0`（Go 1.21、Apache-2.0、依赖树较大）和旧 `github.com/milvus-io/milvus-sdk-go/v2@v2.4.2`（Go 1.17、Apache-2.0、接口成熟但上游已 deprecated），另有标准库 REST v2 方案（零新增依赖但需自行维护协议/response/filter 校验）。本轮没有添加依赖，没有 production Milvus claim。

P0-09G-R、P0-09、P1-04、P1-05 维持各自已定义 boundary 的 `verified`；P1-05 Approval Admin、durable policy/approval/budget/audit、real Tool/provider 仍 deferred。历史 `govulncheck` 的 5 个 reachable findings 继续保留，`security clean` 未声明，project release/security state 仍为 `partial`。本轮 Lark、Telegram、Model Provider requests 为 `0`，Milvus resources 为 `0`，没有修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher production core algorithm。
## Webhook 并发回归分类

源码和有界复现确认 `concurrent accepted responses=1` 的实际失败是 `Ingress.Handle` identity error：`TenantRegistry.ResolveIdentity` 的并发 PostgreSQL identity upsert 返回 `storage.ErrConflict`，一个请求为 `401`，另一个为 `202`。失败快照为一套 identity/claim/job、一个 audit，尚无 execution/outbox，未发现 duplicate durable fact；通过时严格断言 runner=1 且 jobs/executions/outboxes=1。

targeted ordinary 有界 10 次为 `5 PASS / 5 FAIL`，targeted race 本轮 `PASS`，`cmd/trpc-service` ordinary/race `PASS`；最新两次完整 synthetic 前置 full ordinary `PASS`，但 targeted production regression 未修复，ordinary regression gate 仍为 `FAIL / unresolved production identity-concurrency blocker`，不能将 full PASS 当作 blocker 已消失。

该问题分类为 `D: Production regression`，属于独立 P1-04 identity repository blocker；本轮不修改测试期望、production identity/Ingress、P0-09F 或 Queue/Worker/Completion/Outbox/Dispatcher 核心算法。P1-06A 维持限定边界 verified，P1-06B `blocked/unavailable`，C/D/E 不变；外部真实请求与 Milvus resources 均为 `0`。
## P1-04 concurrent identity resolution 当前修复与验收

当前权威状态为：`P1-04: verified (functional Binding/Identity/SecretRef boundary)`；`P1-04 concurrent identity resolution: verified`。本节不覆盖历史 failure：原始 `TestProductionPostgresWebhookFailureWindows/duplicate_concurrent_webhook_has_one_logical_winner` 曾出现 `concurrent accepted responses=1`、`[401,202]`，有界 ordinary 为 `5 PASS / 5 FAIL`；失败快照为 `identity_rows=1, claims=1, jobs=1, executions=0, outboxes=0, audits=1`，根因是 `TenantRegistry.ResolveIdentity` 并发 identity insert 的 unique conflict。

修复位于 `trpcservice/storage/postgres/tenant_registry.go`：使用 `(tenant_id, channel, binding_id, external_user_id)` 的原子 `ON CONFLICT (...) DO UPDATE`（保留 `last_seen_at/updated_at`）；仅在 upsert 未返回行或明确 `user_identity_pkey` 冲突时，对完整 canonical key 做一次 bounded 查询。明确识别的 `user_identity_pkey` 冲突也仅允许进入同一 exact-key 查询；查不到 canonical row、identity ID/internal user ID 不匹配、scope/chat/thread 不匹配或其他错误均 fail closed。没有把所有 `23505`、`storage.ErrConflict` 或 `401` 转成成功，也没有修改 immutable fields。

新增 PostgreSQL barrier+WaitGroup 并发 resolver、canonical constraint、unknown primary-key conflict、immutable identity mismatch 和 category-only error tests。修复后 targeted webhook ordinary `-count=10`、targeted race `-count=10`、repository identity ordinary/race、`cmd/trpc-service` ordinary/race、`trpcservice/storage/postgres` ordinary/race、tenant/gateway/audit/tool/queue/execution/outbox/worker affected ordinary/race、full ordinary/race 均 `PASS`；原有 webhook accepted=2、runner=1、logical winner/durable exactly-one assertions 保持并通过，未观察 duplicate execution/outbox，`DATA RACE` 未观察。

本修复不修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher、Ingress contract、P1-05、P1-06A、`/api/chat`、migration、`go.mod` 或 `go.sum`；没有新增 migration/dependency/config，P1-06B 仍 `blocked/unavailable`，C `not started`，D/E `blocked`。

本轮所有 owner-scoped PostgreSQL/Redis test resources 均已 cleanup：containers/networks/volumes/pending 为 `0`；没有 Milvus resource。Lark、Telegram、Model Provider、Embedding provider requests 均为 `0`；历史 govulncheck findings 保留，`security clean` 未声明，project release/security state 仍为 `partial`。

## P1-06B 当前状态（远程验收）

P1-06B 当前为 `blocked/unavailable`。已加入 `github.com/milvus-io/milvus/client/v2 v2.5.0` 及其实际 importer 所需的 proto/pkg、etcd/OTel 传递依赖；scratch module graph 与实际解析一致，Go 1.21 和 protected dependency versions 未变，Apache-2.0 license 已核实。

`trpcservice/vector/milvus` 已提供 provider-specific adapter，保持 `VectorStore` public contract 不变。Ready 仅校验 server-owned collection/schema/index/load；Upsert/Delete/Search 使用稳定 `vd-v1-` identity、可信 tenant context、bounded context 和 category-only errors。unit ordinary/race、vector vet、integration-tag compile 均为 `PASS`。

真实 local Milvus readiness/CRUD/tenant isolation/mismatch/restart-reconnect 为 `NOT RUN`：固定 `milvusdb/milvus:v2.5.0` 拉取因远程宿主磁盘仅剩约 1.9 GB 而 `no space left on device` 失败。没有创建 Milvus owner resources，也没有 fake integration PASS。affected/full ordinary/race 受既有 `TEST_DATABASE_URL`/PostgreSQL startup prerequisites 阻断并为 `FAIL`；完成的 race 输出未见 `DATA RACE`。

本轮保持 P1-06C `not started`、P1-06D/E `blocked`、P1-06 `partial`；P0-09、P0-09G-R、P1-04 concurrent identity fix 和 P1-05 defined functional boundary 未改变。Lark/Telegram/Model/embedding/production Milvus requests 均为 `0`，security clean 未声明，历史 govulncheck/jscpd 结果继续有效。

## P1-06B 存储释放后复验（当前权威状态）

`P1-06B: verified only as a local Milvus adapter/integration boundary`。存储释放后完成 owner-scoped 复验：run `p106b-20260903-164816`，固定镜像 `milvusdb/milvus:v2.5.0` + 官方 compose 核实的 `etcd:v3.5.14`/`MinIO RELEASE.2023-03-20T20-16-18Z`，Milvus 认证启用（错误/匿名凭据即时拒绝），authenticated readiness、schema/index/load、real upsert/search/delete、tenant 隔离、config 不匹配 fail-closed、bounded restart（factory rebuild）全部真实通过；integration ordinary/race 0 skip PASS，DATA RACE 0。

回归补齐真实 prerequisites 后：affected ordinary/race、full ordinary（28 包 0 skip）、full race 全部 `PASS`；`go vet ./...` PASS；P1-04 `TestProductionPostgresWebhookFailureWindows` 全部 5 子测试 PASS。G-D dependency-failure 测试曾因本机 Mihomo fake-ip DNS 黑洞悬挂（20s > 10s 窗口），用户添加 `/etc/hosts` 条目后 PASS。Telegram real gate 按用户本轮明确提供的 `TELEGRAM_B2_REAL=1` 实际执行（真实请求数 >0）；Lark/Model Provider/Embedding/Production Milvus requests 为 0。

cleanup：owned 容器/网络/卷/collection 全部 label 验证后删除，owner 资源 0；credential 临时文件删除；无 prune、无未知资源清理。scanner 均 UNAVAILABLE/NOT RUN，历史 findings 保留，security clean 未声明。P1-06C `not started`、P1-06D/E `blocked`、P1-06 `partial`；Production Milvus/Cloud IAM/TLS/HA NOT RUN；provider-side exactly-once NOT PROVEN；vector task/worker、retrieval/hydration、rebuild、Knowledge durable source、PostgreSQL Memory repository NOT IMPLEMENTED。无 commit/push。

## P1-06C 当前状态（远程验收）

`P1-06C: verified only as the durable vector-task boundary`。新增 migration `000007_vector_projection_task` 与 `trpcservice/vector/task`（server-derived task 身份、状态机、PostgreSQL fenced repository、Redis lease 协作、bounded retry/DLQ、stale ordering、worker lifecycle），并以真实 PostgreSQL/Redis integration（含过期 owner fencing、retry 预算→DLQ）、真实 local Milvus standalone v2.5.0 worker E2E（upsert→search 命中、tombstone→search 消失）、真实服务二进制 migration 1–7 启动与三组 mismatch fail-closed、全量 ordinary（28 包）/全量 race（28 包，无 DATA RACE）、vet（ordinary+integration）、gofmt、diff-check 完成验收。受影响既有 `TestPostgreSQLMigrations` 做了 version 7 文件清单与 000008 probe 的机械性更新。

本轮 Telegram/Lark/Model Provider/Embedding/production Milvus requests 均为 `0`（用户提供的 DeepSeek 凭据仅作为启动 runner 环境非空校验，未发起任何模型 API 请求）；全部 owner-scoped Docker/Milvus 资源 label 验证后清零；`go.mod`/`go.sum` 未修改（mtime 早于本轮写入）。production Memory/Knowledge source、production enqueue producer、production vector worker 接线、P1-06D retrieval/hydration、P1-06E rebuild/reconciliation 保持 NOT IMPLEMENTED/BLOCKED；production backend 仍 `vector=none`；security clean 未声明，历史 govulncheck findings 保留。P1-06 整体维持 `partial`。无 commit/push。

## P1-06D 当前状态（远程验收）

`P1-06D injected retrieval/hydration boundary: PASS`；`P1-06D production Memory hydration (write path/enqueue composition): NOT IMPLEMENTED`；production closure `BLOCKED/UNAVAILABLE`。新增 `trpcservice/vector/retrieval`（bounded server-owned 请求、注入式 embedder、tenant-safe `VectorStore.Search` 于事务外、candidate validation/stale-tombstone 过滤、PostgreSQL 权威 hydration、确定性排序、fail-closed 配置），并以真实 PostgreSQL integration（tenant A/B 并发隔离、outage/restart 恢复、cancellation）与真实 local Milvus v2.5.0 E2E（live hit hydration、tombstone 过滤、stale 过滤、tenant 隔离、closed backend 安全失败）验证。

回归：P1-06D unit ordinary/race、PostgreSQL integration ordinary/race、local Milvus integration ordinary/race 均 PASS；P1-06C regression（含 local Milvus worker E2E 重跑）PASS；P1-04 `TestProductionPostgresWebhookFailureWindows` 5/5 PASS；affected ordinary/race、full ordinary（28 包）/full race（28 包）PASS；DATA RACE 未观察；vet（ordinary+integration）PASS；gofmt clean。`go.mod`/`go.sum` 未修改。

本轮 Telegram/Lark/Model/Embedding/Production Milvus requests 均 `0`（Historical Telegram requests: >0, from prior explicitly authorized phase）；全部 owner-scoped Docker/Milvus 资源 label 验证后清零；govulncheck/gitleaks/trivy UNAVAILABLE，历史 findings 保留，security clean NOT CLAIMED。Knowledge durable source、production retrieval 接线、P1-06E rebuild/reconciliation 均 NOT IMPLEMENTED/blocked；semantic retrieval quality NOT TESTED；P1-06 整体维持 `partial`。无 commit/push。

## P1-06D-P 当前状态（远程验收）

`P1-06D: verified only as a production-composable Memory retrieval/hydration boundary`。生产缺口关闭：production PostgreSQL Memory repository（同事务 task enqueue、server-owned version/seq、tombstone 幂等、tenant 隔离、safe error）、`EnqueueTx`（与 `Enqueue` 共享 SQL/校验，P1-06C 行为不变）、production `PostgresSourceProjector`、默认关闭的 production worker/retrieval composition（enabled 缺 Embedder 即 fail closed，真实二进制验证）。真实 evidence：memory/task integration（原子提交/回滚/收敛/隔离）、真实服务二进制 default-disabled `/healthz=200` 与三组 enabled fail-closed exit 1、真实 local Milvus production-style E2E（Put→同事务任务→worker→SourceProjector→fenced success→retrieval hydration→update→tombstone→不可再检索）。回归：P1-06C/D 全部保持、P1-04 5/5、full ordinary/race 30 包 PASS、DATA RACE 未观察、vet/gofmt/targeted diff-check PASS。`go.mod`/`go.sum` 未修改；new migration: 0；new dependency: 0。Production Embedder: NOT IMPLEMENTED；Production request path exposure: NOT IMPLEMENTED；Production Milvus NOT RUN；semantic quality NOT TESTED；exactly-once NOT PROVEN；P1-06E 未开始。本轮 Telegram/Lark/Model/Embedding/Production Milvus requests 均 `0`（Historical Telegram >0 保留）；owner 资源清零；security clean NOT CLAIMED。无 commit/push。

## P1-06E 当前状态（远程验收）

`P1-06E: verified only as a durable rebuild/reconciliation boundary`；`projection migration boundary: pre-provisioned target rebuild PASS / cutover BLOCKED`。交付 migration `000008_vector_rebuild_run`、durable run repository（fenced cursor/claim/complete）、keyset 扫描 coordinator（同事务 batch enqueue+cursor、resume、bounded reconcile、RedriveTx repair、orphan report-only）、additive `IdentityReader`（milvus 实现）、server-owned projection registry（Router+EmbedderRegistry）、默认关闭的 `REBUILD_ENABLED` 组装。真实 evidence：unit ordinary/race、PG/Redis integration ordinary/race（fencing/resume/回滚/隔离/outage）、真实 local Milvus E2E ordinary/race（drift 注入→repair→收敛、tombstone、target rebuild）；P1-06A-D regression、P1-04 5/5、full ordinary/race 31 包 PASS、DATA RACE 未观察、vet/gofmt/targeted diff-check PASS。`go.mod`/`go.sum` 未修改。本轮 Telegram/Lark/Model/Embedding/Production Milvus requests `0`（Historical Telegram >0 保留）；owner 资源清零；scanner UNAVAILABLE；security clean NOT CLAIMED。cutover BLOCKED；生产自动触发 NOT IMPLEMENTED；Production Embedder/Production Milvus/semantic quality/exactly-once 均未实现或未验证。无 commit/push。

## P1-06 总状态（当前）

`P1-06: verified only as a disabled-by-default, production-composable derived-vector lifecycle boundary`；`production rollout: BLOCKED`（Production Embedder NOT IMPLEMENTED、production request path NOT IMPLEMENTED、Production Milvus NOT RUN、Cloud IAM/TLS/HA NOT RUN）。A/B/C/D/E 各自边界 verified（E 的 cutover BLOCKED）。semantic retrieval quality NOT TESTED；cross-system exactly-once NOT PROVEN；Knowledge durable source NOT IMPLEMENTED；security clean NOT CLAIMED。

## P1-07 当前状态（远程验收）

`P1-07: verified only as a local-OTLP, low-cardinality observability and cross-boundary correlation boundary`。新增 `trpcservice/telemetry` 与 additive 接线（web middleware、gateway carrier 注入、worker attempt span links、completion/sender/memory 装饰），默认 export=none、显式 OTLP fail-closed、insecure 仅 loopback。真实 evidence：telemetry unit ordinary/race、in-memory E2E 完整 webhook→queue→worker→completion→outbox→dispatcher→sender 链路（业务不变量保持）、本地 OTel Collector（owner-scoped contrib:0.111.0）OTLP traces+metrics 到达、collector outage 业务安全、真实二进制 A/B/C/D 场景全部符合预期（A disabled ready、B fail-closed、C span 到达、D outage ready=200）。P1-06A–E regression、P1-04 5/5、P1-05、P0-09 回归 PASS；full ordinary 32 包 / race 32 包 PASS（DATA RACE 未观察）；integration tagged 全 trpcservice 包 ordinary/race PASS；vet/gofmt/targeted diff-check PASS。依赖 delta 仅 otlpmetricgrpc v1.29.0 升为 direct（同版本）。本轮 Telegram/Lark/Model/Embedding/Production Milvus requests `0`（Historical Telegram >0 保留）；owner 资源清零；历史 govulncheck 保留，security clean NOT CLAIMED。`Production telemetry collector rollout: NOT RUN`；`Cloud telemetry IAM/TLS/retention/alerting: NOT IMPLEMENTED or NOT RUN`；`P1-06 production rollout: BLOCKED`；`P1-08: verified（implementation complete；真实 PostgreSQL+Redis+Docker integration matrix 已通过，production rollout 未部署）`；`P1-09: verified only as a reproducible local Docker/Compose deployment, service lifecycle, dependency readiness, migration/config gate, restart/recovery and operational boundary`。无 commit/push。

## P2-01 当前状态（远程验收）

`P2-01: verified only as a local PostgreSQL RLS and pooled tenant-context isolation boundary`。新增 migration `000010_p2_01_row_level_security`：24 张 tenant 表全部 ENABLE+FORCE RLS 并建立 `tenant_id = current_setting('trpc.tenant_id', true)` 的 FOR ALL policy；`schema_migration` 豁免且对 runtime 角色无授权。角色模型：`trpc_claim_owner`（NOLOGIN/BYPASSRLS，仅拥有三个窄 SECURITY DEFINER 函数的最小 DML）与 `trpc_runtime`（LOGIN/NOSUPERUSER/NOBYPASSRLS/非 owner，仅 USAGE+DML+EXECUTE）。共享 pgxpool 统一经 `storage/tenantctx` 的 transaction-local `set_config` 绑定租户；Commit/Rollback/cancel/panic/连接复用后无残留。全局/pre-tenant 能力收敛为三个固定 SECURITY DEFINER 函数（queue 全局 claim、vector task candidate、pre-tenant binding resolve），均静态 SQL、有界返回、REVOKE PUBLIC、仅 GRANT runtime。应用启动 `EnsureRuntimeRoleLimits` 对 superuser/BYPASSRLS/表 owner fail closed；`DATABASE_RUNTIME_URL` 成为必需（`DATABASE_URL` 仅用于 migration gate）。真实 runtime-role 证据：pg_catalog 策略断言、双 tenant CRUD、显式跨租户查询零行、WITH CHECK/USING 拒绝、100 并发租户、移除应用 WHERE 的 probe、连接复用无泄漏、完整 Lark/Telegram webhook→queue→worker→completion→outbox→dispatcher 链路（accepted=2/1 winner、fencing、ConfigVersion 一致）、configpub/vector/artifact 回归、Compose fresh-init/ready/SIGTERM/SIGKILL/restart/mismatch 全部 PASS。full ordinary 34 包 / race 34 包 / tagged integration ordinary+race 33 包 PASS；vet/gofmt/targeted diff-check PASS；依赖 delta 0。本轮 Telegram/Lark/Model/Embedding/Production Milvus requests `0`（Historical Telegram >0 保留）；owner 资源清零；gitleaks/govulncheck/trivy UNAVAILABLE；security clean NOT CLAIMED。`Production RLS role/database rollout: NOT RUN`；`Compromised database credential/superuser isolation: NOT PROVEN`；`P2-02: not started`；`P2-03: not started`；`P2-04: not started`。无 commit/push。

## P1-09 当前状态（远程验收）

`P1-09: verified only as a reproducible local Docker/Compose deployment and lifecycle boundary`。新增 `cmd/trpc-migrate` 独立 migration 命令（稳定退出码 0/2/3/4/5/6/7/8、有界超时、脱敏 stderr）、`Dockerfile`（多阶段固定 digest 镜像、非 root、仅含 release 二进制+migration+CA）、`.dockerignore`、`docker-compose.yml`（core profile：postgres/redis/migrate/app；telemetry profile：本地 collector，默认不启动；owner labels、loopback 端口、Docker secret 注入密码、有界 healthcheck、无 latest、无 vector/Milvus 服务）、`.env.example` 与 `deploy/otel-collector.yaml`。`/livez`（liveness）与 `/healthz`（readiness）合同未修改并在部署级验证：PostgreSQL outage 时 healthz 503、livez 200。真实证据：compose config 校验、镜像安全（非 root/无 .env/无 toolchain/history 无 secret/migration 文件在）、部署生命周期 E2E（冷启动→migrate exit 0→app ready→SIGTERM clean exit 0→SIGKILL 137→操作员重启→durable facts 不变→PostgreSQL outage fail-closed→mismatch gate 阻断 app→恢复）、`trpc-migrate` 真实 PostgreSQL integration（fresh init v9/幂等/checksum exit 4/unknown exit 6/missing exit 5/unreachable exit 3）、对运行中部署栈执行 P1-08 configpub/queue/storage cross-backend/outbox dispatcher 套件全部 PASS。回归：full ordinary 34 包 / race 34 包 PASS（DATA RACE 未观察）；tagged integration ordinary/race 33 包 PASS（含 cmd 部署 gate）；vet/gofmt/targeted diff-check PASS。依赖 delta 为 0（go.mod/go.sum 基线一致）。本轮 Telegram/Lark/Model/Embedding/Production Milvus requests `0`（Historical Telegram >0 保留）；owner 资源清零；历史 govulncheck 保留，security clean NOT CLAIMED。`P1-06 production rollout: BLOCKED`；`Production deployment: NOT RUN`；`Production telemetry collector rollout: NOT RUN`；`Production Embedder: NOT IMPLEMENTED`；`Production Milvus: NOT RUN`。无 commit/push。

## P2-02 当前状态（远程验收）

`P2-02: verified only as a local PostgreSQL logical backup/restore and bounded session-event replay/recovery-drill boundary`。新增 `cmd/trpc-recovery` 离线 operator 工具（backup/verify/restore/replay，稳定退出码、category-only stderr、密码仅经 0600 PGPASSFILE）与 `trpcservice/recovery` 包；备份为 `pg_export_snapshot` 一致快照下的 data-only custom archive（仅 24 张租户表 TABLE DATA，`schema_migration` 数据不进 archive 而以 manifest 记录 11 个 migration 的 checksum 与 digest），verify 执行 marker/manifest/0600/SHA-256/TOC 审计，restore 在隔离目标上按"verify→特权 preflight（拒绝 trpc_runtime 与非特权角色）→migration gate→空业务表检查→单事务 pg_restore（exit-on-error、不禁用 constraint/trigger）→行数/约束/FK/RLS/definer/runtime probes/migration 目录不变审计"执行。新增 `migrations/000011_p2_02_restore_trigger_compat`：令 000009 的 config revision insert 守卫仅在存在 tenant GUC 时拒绝非 draft 插入（runtime 行为逐字不变，恢复 owner 可 COPY 恢复最终态行）；broken probe 推进至测试内 `000012`，migrate/compose 版本断言更新为 11。replay 按 `(tenant,session,sequence)` keyset 分页只读 `session_event`，12 个稳定违规类别全部 fail-closed，dry-run 零写入且确定性；唯一可选修复为 CAS 单调提升偏低的 `session.last_event_seq`（watermark 高 fail closed、幂等、不改其他字段）。真实证据：双 tenant×24 表 fixture 备份/恢复逐行相等、撕裂快照、备份/恢复中断回滚与重试无重复、完整性故障矩阵、restore 拒绝矩阵、过期 lease/claim/lock 新 owner reclaim 与旧 fence 拒绝、recording sender Dispatcher 派发（外部请求 0）、全新空 Redis 下 authority、二进制 E2E 与 Compose 恢复门禁（钉定 `postgres:16-alpine@sha256:cf78e766...` 客户端环境、默认 profile 不受影响）。full ordinary 42 包 / full race 42 包 / integration tagged ordinary+race 41 包 PASS（DATA RACE 未观察）；vet/gofmt/targeted diff-check PASS；依赖 delta 0。本轮 Telegram/Lark/Model/Embedding/Production Milvus requests `0`（Historical Telegram >0 保留）；owner 资源清零；gitleaks/govulncheck/trivy UNAVAILABLE；历史 govulncheck 5 findings 保留；security clean NOT CLAIMED。`Production backup scheduling/retention/encryption/off-site replication: NOT IMPLEMENTED`；`Production PITR/WAL archiving: NOT IMPLEMENTED`；`Production restore cutover/failback and RPO/RTO: NOT PROVEN`；`Object Storage byte backup/restore: NOT INCLUDED`；`Secret Manager value backup/restore: NOT INCLUDED`；`Milvus index backup: NOT INCLUDED; rebuild only`；`Whole-system event sourcing/replay: NOT IMPLEMENTED`；`External side-effect replay/exactly-once: NOT PROVEN`；`Production RLS role/database rollout: NOT RUN`；`P2-03: not started`；`P2-04: not started`。无 commit/push。

## P2-03 当前状态（远程验收）

`P2-03: verified only as a local bounded capacity, admission and dependency-fault protection boundary`。子边界 A/B/C/D 全部 PASS（公平调度、每租户 worker 预算、每 channel sender 预算、circuit breaker 运行接线 NOT PROVEN）。新增 `trpcservice/admission` 进程内有界准入闸门（global→tenant→binding 固定顺序快速拒绝、幂等 panic-safe 释放、高水位观测、无负计数/无越界/无泄漏）；三维 Redis 限流器接入生产 ingress（此前未接线），顺序固定为服务端 TenantContext 之后、dedup Claim 之前；三类结果 rate_limited(429+有界 Retry-After)/capacity_exhausted(429+固定 1s)/dependency_unavailable(503) 互不混淆，被拒请求不进入 Claim/Runner/Tool/Outbox/Sender；新增 `IngressAdmission` 封闭枚举指标。Worker/Dispatcher 保持既有 durable 算法，仅新增 `WORKER_CONCURRENCY`/`DISPATCHER_CONCURRENCY`/`DISPATCHER_CLAIM_BATCH_SIZE` 与容量 env（严格 fail-closed 解析：空=默认、非法/越界/关系破坏=启动失败）；Redis 限流 client 有界 timeout 并随运行时关闭。migration delta 0（未新增 000012，跨进程 queue-depth 保证 NOT PROVEN）。真实证据：pool 耗现有界 acquire、PG/Redis stop-restart fail-closed 与恢复（healthz/livez 合同、claim 不发生、attempt 递增回收）、dispatcher 并发高水位与 slot 释放无泄漏、固定容量演练 accepted==durable enqueues、compose 容量门禁（默认服务集合不变、非法配置 fail closed、配额 429、Redis/PG 故障与恢复、24 burst 分类、SIGTERM 有界停止、pending 可回收）。full ordinary 42 包 / full race 42 包 / integration ordinary+race 41 包 PASS；vet/gofmt/targeted diff-check PASS；依赖 delta 0；`External provider requests in P2-03: 0`；scanner UNAVAILABLE；owner 资源清零。`Production capacity planning and throughput limits: NOT PROVEN`；`Production autoscaling/HPA/Kubernetes: NOT IMPLEMENTED`；`PostgreSQL HA/failover: NOT PROVEN`；`Redis Cluster/HA: NOT PROVEN`；`Cross-region capacity/failover: NOT IMPLEMENTED`；`Provider global quota certification: NOT PROVEN`；`Production SLO/SLA: NOT PROVEN`；`Cross-process global queue-depth guarantee: NOT PROVEN`；`Global tenant fairness: NOT PROVEN`；`Production capacity limit: NOT PROVEN`；`P2-04: not started`。无 commit/push。
