# P0-09 当前背景与后续路线

本文只记录当前源码、migration、实际测试和验收报告可以核对的事实。它不是生产能力声明，也不替代 `docs/ARCHITECTURE.md`、`docs/implementation-plan.md` 或 `docs/project-status.md`。

## 1. 阶段状态

- P0-07 / R1：`contract-level verified`。Runtime 使用公开 runner completion event、tail drain、framework-owned channel close 和 adapter pump done；tRPC-Agent-Go framework 内部是否存在公开 global Wait/Done 仍未证明。
- P0-08A / R2：`contract-level verified`。Job/DTO、租户恢复、History 边界、Fake Queue visibility/Ack/Nack/Close 和安全错误分类已验证。
- R3：`contract-level verified`。Execution 使用带 tenant/session/owner/epoch/fence/job/execution identity 的 fenced commit contract。
- R4 / R5：`contract-level verified`。Worker delivery failure、visibility recovery、Receive backoff 和 Worker 自有生命周期的 bounded shutdown 已验证；非合作外部 Queue action 的 finalizer 边界仍保留。
- P0-09A：`verified`。真实 PostgreSQL execution result fenced commit、幂等/冲突、rollback 和 takeover rejection 已验证。
- P0-09B：`verified`。真实 PostgreSQL durable Job Queue、database claim、visibility、delivery token fencing、tenant isolation 和 rollback 已验证。
- P0-09C：`verified`。同一 PostgreSQL transaction 内的 execution result + Queue Ack、rollback、fencing/reconciliation 和真实 Worker atomic path 已验证。
- P0-09D：`verified (PostgreSQL Outbox Repository boundary)`。真实 PostgreSQL Outbox Repository、dedup、claim、lock reclaim、retry、DLQ 原子 transition、tenant isolation 和 restart recovery 已验证。
- P0-09E：`verified (Outbox Dispatcher boundary)`。storage-neutral Dispatcher、bounded RetryPolicy、Sender 四类 outcome、安全 durable retry/DLQ 编排和 bounded shutdown 已验证；没有真实 Provider、生产异步装配或生产 SIGTERM 证据。
- P0-09F：`verified (Atomic Completion + Outbox enqueue boundary)`。同一 PostgreSQL transaction 完成 execution result、Queue Ack 和 reply Outbox enqueue；durable Worker 已接线，rollback、dedup、fence、partial/unknown reconciliation 和独立 pool 竞争已验证。
- P0-09G-A：`verified (channel-aware Sender boundary)`。版本化 routing payload、tenant/channel/destination validation、immutable registry、provider-neutral builder、四类 SenderOutcome、Dispatcher adapter、真实 PostgreSQL claim/completion、fencing 和 recovery 已验证。没有真实 Provider 调用。
- P0-09G-B1：`verified (Lark real sender boundary)`。独立 Lark Sender 已固定 schema v3 BindingID、binding/tenant/destination、SecretRef/token、官方文本发送 request/success、官方 uuid、bounded body 和四类 outcome；授权测试租户的真实 token/message evidence 已通过，但 provider-side UUID dedup、exactly-once 和生产装配未证明。
- P0-09G-B2：`verified (Telegram real sender boundary)`。独立 Telegram Sender/Adapter 已固定 schema v4 thread routing、Binding/SecretRef、numeric chat ID、webhook secret/update parser、官方 Bot API request/success、429/5xx/4xx/timeout/unknown safe outcome 和 bounded Dispatcher path；真实 `getMe`/`sendMessage` evidence 已通过，provider-side dedup、exactly-once 和生产装配未证明。
- P0-09G-C：`verified (production async assembly boundary)`。测试进程通过 `TEST_DATABASE_URL` 建立唯一 PostgreSQL schema，直接覆盖 production composition root 的 Lark/Telegram webhook、Claim、durable Queue、Worker、atomic completion、Outbox、Dispatcher 和 channel routing；五个 ingress 失败窗口及 scoped race/vet/gofmt/diff 已通过。本轮 Provider transport 为 deterministic fake，真实外部 send 次数为 0。
- P0-09G-D：`verified (process lifecycle and recovery boundary)`。真实 Linux Unix SIGTERM clean subprocess、draining/readiness/HTTP/Worker/Dispatcher stop contract、startup/listen/Serve cleanup 和 SIGKILL Queue/Outbox reclaim 保持独立证据；历史 same-network PostgreSQL recovery evidence 与本轮 owner-scoped same-network G-D ordinary/race PASS 分开记录。历史 Windows host-mapped `unexpected EOF` 单独保留。

本轮 PostgreSQL recovery sub-gate 的历史 Linux evidence 继续为 `verified`；本轮 production ObjectStore composition、Artifact metadata persistence/reconciliation、real MinIO gate、Redis restart failover 和 owner-scoped same-network G-D ordinary/race 也通过；full ordinary/race 在 synthetic prerequisites 下均 PASS。Redis restart failure 已定位为 Redis physical lease key 未携带/比较 PostgreSQL authority epoch，并由 epoch-aware acquire/renew/release 与独立 new-owner regression 修复。按 scope decision，自动 reconciliation scheduler、后台 lifecycle cleanup 是 deferred、非本轮 P0 强制门禁；ObjectStore 与 G-D 是并列 boundary，enabled-object 联合故障是后续 evidence。因此 P0-09G-R 与 P0-09 在定义的 durable、assembly、object 和 recovery boundary 内为 `verified`。

Object boundary 本轮已完成 production composition：default `none` 不构造 object client，enabled `s3` 配置缺失 fail-closed；runtime/readiness 持有真实 ObjectStore，ArtifactMetadataRepository 复用既有 schema 并通过 pending -> ready、pending -> failed、object mismatch -> failed/expired、ready -> expired reconciliation。real MinIO bytes、presigned actual GET、隔离、取消、restart/reconnect 和 cleanup 均通过；owner-scoped Redis restart failover ordinary/race 也通过 PostgreSQL authority epoch、epoch-aware acquire、new-owner renew、old-owner fencing、durable fact check 和 cleanup；full ordinary/race 均 PASS。ObjectStore/G-D enabled 联合故障仍未执行，但它是并列 boundary 的后续 evidence，不阻塞定义范围内的 P0 closure；不声称跨 PostgreSQL/MinIO exactly-once。

## 2. 当前 P0-09 事实边界

P0-09A 至 P0-09F 已建立 PostgreSQL result、durable Queue、Outbox、Dispatcher、Retry/DLQ 和三事实 atomic completion 的可验证边界。P0-09G-A 已建立 channel-aware Sender boundary；P0-09G-B1 已建立并通过真实测试租户 evidence 的 Lark Sender boundary；P0-09G-B2 已建立并通过真实 `getMe`/`sendMessage` evidence 的 Telegram Sender/Adapter boundary。一次真实 2xx/Delivered 也不代表 provider-side UUID dedup、exactly-once 或生产装配已完成。

定义范围外或后续仍缺少：

- provider-side dedup、exactly-once、完整业务 Repository/Unit of Work 和 Binding/Identity 持久化；
- 真实 Provider 接收后的不可重复发送保证；
- framework global Wait/Done 公开证据。

P0-09G-D 的历史 Linux Unix SIGTERM clean shutdown、Queue/Outbox reclaim 和 PostgreSQL recovery evidence 保持 verified；本轮 owner-scoped same-network G-D ordinary/race 已 PASS，未修改 recovery flow。最初 host-process dynamic endpoint diagnosis 与 fixed loopback host-mapped regression 单独记录。production ObjectStore/Artifact metadata composition 与 real MinIO evidence 另有 current PASS；这些证据不外推为 production cluster failover 或 framework global Wait/Done。自动 scheduler/lifecycle cleanup 不属于本轮 P0 gate；显式 `ArtifactMetadataRepository.Reconcile` 是当前实现边界。

## 3. Migration 和事务边界

当前 migration catalog 包含 `000001` 至 `000005`；`000005_outbox_repository` 是 Outbox Repository 阶段新增的 forward migration，`000001` 至 `000004` 未修改。P0-09G-A、P0-09G-B1 和 P0-09G-B2 没有新增 migration。

已验证的 durable boundary：

- P0-09A：execution result 写入和 lease/fence 重新校验在同一 transaction；
- P0-09B：Queue Enqueue、claim、Ack/Nack/visibility 各自使用 PostgreSQL transaction；claim 使用 `FOR UPDATE SKIP LOCKED`；
- P0-09C：execution result 和 Queue Ack 使用同一个 `pgx.Tx`；
- P0-09D：Outbox Enqueue、Claim、Complete/Retry、DLQ transition 使用 PostgreSQL transaction；
- P0-09F：同一个 `pgx.Tx` 锁定 Queue 和 session lease，检查/插入 execution result、检查/插入 pending reply Outbox，条件 Ack 后 commit；任一 SQL 失败全部 rollback；
- P0-09G-A：只消费已提交 Outbox，不改变 durable mutation owner；Sender 不直接写数据库。

Redis 只承担既有协调、epoch/fencing、failover 和限流职责，不承载 Outbox 事实。

## 4. Outbox 和 Sender 当前事实

P0-09D 后，`storage.OutboxMessage` 具有 tenant-scoped `DedupKey`、bounded payload/error、attempt、next attempt、lock 和状态约束；真实 PostgreSQL repository 已验证 claim、reclaim、complete、retry、DLQ 和 tenant/stale-owner fencing。

P0-09E Dispatcher 的路径是：

```text
ClaimBatch -> Sender -> safe outcome code -> RetryPolicy -> MarkCompleted/MarkRetry/MoveToDLQ
```

P0-09F durable Worker 的路径是：

```text
Receive -> ExecuteWithCompletion -> BuildReplyOutboxMessage -> atomic result + Queue Ack + reply Outbox
```

P0-09G-A 将 reply payload 升级到 schema v2，增加 `channel`、`destination_type`、`destination_id` 和 `sender_routing_version`。P0-09G-B1 再升级为 schema v3，增加必需 `binding_id`；P0-09G-B2 升级为兼容 schema v4，增加正式可选 `message_thread_id`，旧 schema v3 Web/Lark payload 仍可解码。当前 provider-neutral registry/builder 支持 Web、Telegram、Lark placeholder 和历史 WeCom 占位适配器；真实 Telegram Sender 位于独立 `channels/telegram`，这不代表真实 webhook 或生产装配已完成。

P0-09G-A 的路径是：

```text
PostgreSQL Outbox Claim
  -> Decode/Validate routing
  -> Registry policy
  -> provider-neutral outbound builder
  -> Fake HTTP transport seam
  -> safe SenderOutcome
  -> existing RetryPolicy
  -> durable mutation
```

## 5. 后续 P0-09G 阶段

### P0-09G-B1：飞书（Lark）真实通道

已完成 contract-level Sender boundary：服务端 Binding/SecretRef/config contract、官方 tenant access token 和文本消息 request/success contract、schema v3 BindingID routing、user/chat destination、固定 HTTPS base URL、官方 `uuid` provider idempotency 输入、bounded response 和 429/5xx/4xx/timeout/unknown 分类均已实现并测试。`Retry-After` 安全忽略，由现有 bounded RetryPolicy 负责重试时间。

真实证据收口轮已通过显式门禁测试 `lark_real_test.go`（仅 `LARK_B1_REAL=1` 才执行）：授权测试租户的 token flow、真实消息 2xx/Delivered 和稳定 UUID replay 已验证；凭据仅通过临时进程环境变量提供。没有把一次真实测试外推为 provider-side uuid 去重、exactly-once、webhook challenge/签名/解密或生产装配证据。不得输出 token、Authorization、完整请求/响应 body 或原始 Provider error。

### P0-09G-B2：Telegram 真实通道

已完成并取得真实证据的 Telegram Bot API Sender/Adapter：固定 `https://api.telegram.org`，Binding/SecretRef、numeric chat ID、`update_id` dedup key、private/group/supergroup scope、private/supergroup `message_thread_id`、`retry_after`、4096 字符上限、400/403/5xx/401、timeout、response unknown 和有限 safe code 均已实现并测试。real `getMe`/`sendMessage` 仅在 `TELEGRAM_B2_REAL=1` 且本机安全变量存在时运行；既有真实 gate evidence 独立保留，本轮未开启 gate，真实外部 `sendMessage` 次数为 0。Telegram `sendMessage` 没有官方 outbound idempotency/dedup contract，exactly-once 不支持，OutcomeUnknown retry 有重复发送风险。

Telegram 的真实证据必须独立记录，不能由 Lark 证据覆盖。两阶段可以在共享 contract 稳定后并行，但 provider config、credentials、success/error response 和测试报告分开。

### P0-09G-C：生产异步装配

状态为 `verified (production async assembly boundary)`。本轮没有调用真实 Lark/Telegram；`TEST_DATABASE_URL` 是真实联合证据门禁，测试内部临时绑定为 `DATABASE_URL` 并使用唯一 schema。测试只注入 deterministic AgentFactory、recording Sender 和事件同步 hook，Queue、Worker、completion、Outbox、Dispatcher 和 PostgreSQL Repository 均为真实实现。将 Lark/Telegram webhook 接入：

```text
verified Binding/TenantContext
  -> inbound Dedup Claim
  -> PostgreSQL durable Job Queue
  -> Worker/Runner
  -> atomic result + Queue Ack + reply Outbox
  -> Dispatcher
  -> selected real Sender
```

必须保留同步 `/api/chat`，但不把它转成生产 Web Chat 异步接口。已验证 Claim-to-enqueue reconciliation 使用稳定 Job/Execution identity，且 Claim、Queue Submit、Claim Complete、HTTP ACK 失败窗口遵守 fail-closed 和 fencing。已验证 `/healthz` readiness 与 `/livez` 分离，以及显式 Start/Stop 的 draining 状态。

### P0-09G-D：生命周期与恢复

P0-09G-D 状态为 `verified (process lifecycle and recovery boundary)`。本阶段使用真实 Linux Unix 语义执行实际 `trpc-service` binary 的 SIGTERM subprocess test，确认真实 `syscall.SIGTERM`、clean exit code 0、配置 shutdown deadline 内退出和 safe lifecycle status；Windows `Process.Kill` 只保留为 crash/reclaim evidence，不能替代 SIGTERM。

隔离 PostgreSQL recovery 使用 owner-labeled Docker network、container 和 volume；本轮 owner-scoped runner attachment、`postgres:5432` alias 和 no-host-port topology 已验证，恢复阶段使用 `pg_isready`、bounded raw/wrapper pool probes、`SELECT 1` 和最终 Queue/Execution/Outbox durable facts 断言，ordinary/race 均通过。未修改 Queue/Worker/Completion/Outbox/Dispatcher 核心算法，不使用固定 sleep 或 retry-until-success。

G-D Provider transport 为 deterministic fake，真实外部 Lark/Telegram/Model Provider 请求为 0。Lark B1、Telegram B2 的真实 Sender evidence 分别独立保留；provider-side dedup/exactly-once、完整业务 Repository/Unit of Work 和 framework global Wait/Done 仍未证明。

### P0-09G-R：P0-09 总审查

复核 P0-09A 至 P0-09G-D 的历史源码、报告、真实依赖、race、vet、格式、安全扫描、生产装配和 `/api/chat` 兼容性，并补充 production ObjectStore/Artifact metadata boundary、Redis restart failover evidence。production object gate、Redis restart gate 与 owner-scoped same-network G-D recovery 均通过 config/epoch fail-closed、readiness、real bytes、new-owner acquire/renew、old-owner fencing、metadata transitions、object mismatch reconciliation、presigned access、restart/reconnect、fencing、final facts 和 cleanup；full ordinary/race 在 synthetic prerequisites 下均 PASS，真实 Provider、provider-side dedup、完整业务 Repository 和 framework global Wait/Done 仍独立未证明。

## 6. P1-06 Milvus 计划边界

P1-06 将补充 README 要求的向量库后端，但当前尚未开始实现。PostgreSQL 继续保存 Memory/Knowledge 原文和版本事实，Redis 继续承担协调、Lease、Claim、fencing 和限流；Milvus 只保存可重建的 embedding/chunk 派生投影。计划必须覆盖 `BackendPolicy.Vector` 路由、tenant-scoped filter、稳定 document identity、embedding/model version、source sequence、content hash、delete/tombstone、幂等 upsert/delete、Outbox/index worker、lag/reconciliation、全量重建、Milvus outage/reconnect、跨节点竞争和安全脱敏。

Milvus 写入不能加入 P0-09F 的 result + Queue Ack + reply Outbox SQL transaction，也不能成为业务事实源。Milvus 不可用时 Memory 事实仍须提交；检索请求必须按明确策略降级或失败关闭，不能返回跨租户结果或把 stale index 当作强一致 Memory。具体代码、依赖、migration、Docker、测试和验收证据由后续 P1-06A 至 P1-06E 提示词单独定义。

## 7. 证据规则

- 当前源码、migration、数据库状态和本轮测试优先于历史报告。
- Fake/in-process、接口存在、SIGKILL reclaim、Docker outage gate 或 PostgreSQL 局部测试不能外推真实 Provider、provider-side dedup、生产集群 failover、Unix SIGTERM clean shutdown、PostgreSQL restart recovery 或 framework global Wait/Done。
- 每个真实通道必须分别证明认证、响应 contract、失败分类、idempotency 和安全脱敏。
- PostgreSQL Outbox 是唯一业务事实源；Redis 不是 Outbox 事实源。
- OutcomeUnknown 不得直接 MarkCompleted；有限 retry 可能造成 provider 重复发送，exactly-once 必须保持未证明。
- 不得输出 DSN、Token、Authorization、Prompt、History、Provider body 或原始外部错误。

## 8. 本轮 WSL Recovery 复验

本轮确认授权的 Linux Docker 环境可运行 owner-scoped same-network G-D recovery、production ObjectStore composition、owned MinIO real gate 和 Redis restart failover gate；production object runtime/readiness、Artifact metadata transitions、real bytes、object mismatch reconciliation、presigned actual access、restart/reconnect 和 cleanup 均通过，G-D 与 Redis restart ordinary/race 也通过。full ordinary 和最终两次 full race 均 PASS；历史 host-process dynamic endpoint diagnosis、Queue concurrent Receive bounded i/o timeout 和其他中间 failure 另有记录；历史 PostgreSQL recovery sub-gate 及 P0-09G-R/P0-09 defined boundary 保持 `verified`；自动 reconciliation scheduler/background lifecycle cleanup 和 ObjectStore/G-D 联合故障 evidence 未证明；P1-06 为 `ready / not started`，本轮未修改 P0-09F 核心算法或发起 Lark、Telegram、Model Provider 请求。

## 9. P1-04 当前最终证据

P1-04 functional boundary 为 `verified`；P1-04 project release/security state 为 `partial`，因为 production Admin API/认证、云 Secret Manager/KMS/Vault/IAM、dynamic reload/rotation/reconcile 和依赖 remediation 仍 deferred。`secret://` 仅 validated-but-unresolved；只有 `env://` 有真实 environment resolver。`BOOTSTRAP_LARK_ENABLED`、`BOOTSTRAP_TELEGRAM_ENABLED` 默认 `false`；enabled secret 缺失 fail closed，disabled optional Binding 不注册 adapter/sender、不阻塞启动。

本轮使用 owner-labeled、唯一 schema 的 PostgreSQL Docker test fixture。P1-04 PostgreSQL migration/repository ordinary/race、Ingress/composition ordinary/race、full ordinary/race、vet、format 和 targeted diff-check 均 PASS；没有 `DATA RACE`，测试后本阶段 owner container/network/volume 为零。Fake repository evidence 与真实 PostgreSQL evidence 已分开记录；真实证据覆盖 metadata constraints、tenant isolation、provider identity、CAS/lifecycle、Identity scope conflict、SecretRef reference-only、Audit metadata、rollback 和 context cancellation/deadline。

Ingress 成功顺序保持 Verify/Challenge/Parse -> server-owned Binding/TenantContext -> SecretRef -> external Identity -> Claim -> durable enqueue -> Claim completion -> fast ACK。caller tenant/channel/binding/destination、unknown/disabled/expired/cross-tenant/cross-channel/cross-binding/scope-conflict/secret-failure 均在 Claim 前拒绝；Ingress 不直接调用 Runner、Model Provider 或 Sender。P0-09G-C、Sender、Dispatcher 和同步 `/api/chat` 原行为保持；本轮不发起真实 Lark、Telegram、Model Provider 请求，均为 `0`。

当前阶段关系为：`P0-09G-R verified (defined P0 closure boundary)`、`P0-09 verified (defined durable, assembly and recovery boundary)`、`P1-04 verified (functional Binding/Identity/SecretRef boundary)`、`P1-05 verified (functional Tool Policy/Guardrail boundary)`、`P1-06 ready / not started`。P1-04 project release/security state 仍为 `partial`，没有创建 Milvus resource，没有修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher 核心算法。

## 10. P1-04 最终状态更正

本节保留 P1-04-R 前的历史 `partial` 快照和 Queue full-race failure：PostgreSQL migration/repository ordinary/race、Ingress/composition ordinary/race 和 full ordinary `PASS`；当时最后一次 full race `FAIL`，唯一失败是既有 `trpcservice/queue` PostgreSQL concurrent Receive 在 `claim candidate` 的 `i/o timeout`，无 `DATA RACE`。独立 targeted queue race diagnostic 后续 `PASS`，但当时不覆盖 full race failure；P1-04-R 后续完成修复，当前 functional boundary 为 `verified`，没有通过 retry-until-success 制造 PASS。

`env://` resolver 已有真实 local environment 行为；`secret://` 仅 validated-but-unresolved。enabled Binding startup 缺 ref/value fail closed，optional channel 默认 false，disabled Binding 不注册 adapter/sender；dynamic reload/reconcile、云 Secret Manager/KMS/Vault/IAM、Admin API/认证 deferred。P1-05 为 `verified (functional Tool Policy/Guardrail boundary)`，P1-06/Milvus 为 `ready / not started`。

gopls、gitleaks、deadcode 结果分别为 PASS/PASS/PASS；jscpd 最终全仓 Go 覆盖 140 文件并报告 `252 exact clones`、`2038 duplicated lines`、`5.29%`，不是 duplication-free；govulncheck 为 `FAIL/findings`，有 5 个可达依赖漏洞；trivy/opengrep `UNAVAILABLE`，madge 对 Go 项目为 `N/A`。P0-09G-R/P0-09 按 defined boundary `verified`，P0-09F/Queue/Worker/Completion/Outbox/Dispatcher 核心算法未修改；真实 Lark、Telegram、Model Provider 请求均为 0，没有创建 Milvus resource。

## P1-04-R Final Closure Investigation

Queue full-race failure 已从源码、fixture 和 controlled matrix 完成定性。原始 `ConcurrentReceive` failure 的 stage/category/last_stage/deadline/DATA RACE 为 `claim candidate`、`database transport i/o timeout`、`Postgres concurrent Receive write`、`true`、`false`；原因是 full-suite PostgreSQL-heavy 并行与 350ms test context 下 pgx deadline transport error shape，非 Queue durable correctness。test-only ready barrier、严格 deadline-coupled timeout 分类和安全 pool diagnostics 已加入 integration test，未修改 Queue production/P0 contract。

最终 focused ordinary/race、Queue package ordinary/race、affected race packages、full ordinary 和两次独立 full race 均 `PASS`，无 DATA RACE。`govulncheck` 仍为 `exit=3`，5 个 finding 的准确 ID、alias、current/fixed、directness、trace、利用前提和 upgrade blast radius 已同步到验收报告；未自动升级依赖，未声称 security clean。

P1-04 functional closure 为 `verified`；project release/security state 仍为 `partial`，P1-05 为 `verified (functional Tool Policy/Guardrail boundary)`，P1-06/Milvus 为 `ready / not started`。P0-09G-R/P0-09 保持 defined boundary `verified`，真实 Lark、Telegram、Model Provider 请求为 `0`，Milvus resource 为 `0`。所有 owner-scoped test resources 已 cleanup。

## jscpd 最终统计更正

最终全仓 jscpd 覆盖 140 个 Go 文件，exit 0，报告 `252 exact clones`、`2038 duplicated lines`、`5.29%`；此前 52 clones/2.71% 仅为 bounded 子集。该结果不改变 govulncheck 5 findings 和 project security blocker。

## 当前阶段顺序

```text
P0-09G-R verified (defined P0 closure boundary)
  -> P1-04 verified (functional Binding/Identity/SecretRef boundary)
  -> P1-05 verified (functional Tool Policy/Guardrail boundary)
  -> P1-06A..P1-06E ready / not started
```

Project release/security state remains `partial` because govulncheck remediation is independent and not yet authorized; this does not change the P1-04 functional closure. P1-06/Milvus remains ready / not started.

## 6. P1-05 接入边界

P1-05 在现有 `agent.ToolInvoker` 入口外增加 server-owned registry、fail-closed policy decision、pre/post Guardrail、统一 Redactor、安全 Audit sink 和 bounded process-local budget admission。异步顺序保持：Job restore 的 trusted TenantContext -> Agent/release/config 和 server-owned Tool identity -> policy -> pre-tool guardrail -> approval/budget gate -> Tool -> post-tool guardrail/redaction -> safe result/audit。Tool、model、network provider 不进入 P0-09F SQL transaction；Worker 仍通过既有 Executor/Completion contract。

当前没有 policy/approval/budget durable migration、Admin/API、完整 production AuditRepository 或 Redis fact/cache implementation 的源码依据，因此不新增 `000007` migration。Approval 为 deferred contract，危险能力只返回 `approval_required`/`approval_deferred`；budget 仅证明 server-owned scope 的 count/time/concurrency/input/output bytes bounded admission，token/cost/provider billing 和 durable accounting deferred。生产默认使用空 registry 和 fail-closed invoker，不注入 fake allow。

统一 Redactor 只向 Tool Audit 暴露安全 category、ID、version、size、latency 和 fingerprint；输入/输出、prompt/history、webhook/provider body、SecretRef/token/Authorization/DSN、raw backend error 和 reservation token 不进入 Audit、日志、trace 或响应。此实现不是完整 DLP/content moderation，也不是 provider-side dedup 或 exactly-once 证明。

P1-05 focused ordinary/race、P1-04/P0-09 affected ordinary/race、full ordinary/full race、vet、format 和 targeted diff check 已通过；当前远程 scanner/gopls 不可用，历史 govulncheck findings 继续独立保持 project security `partial`。本轮 Lark、Telegram、Model Provider requests 为 `0`，Milvus resources 为 `0`；P1-06/Milvus 尚未开始。

## P1-06A 当前验收补充

P1-06A 已在当前源码中开始并完成定义边界：`trpcservice/vector` 提供 provider-neutral VectorStore contract、server-owned `vd-v1-` document identity、Memory durable fact 到 SourceDocument 的稳定映射、content hash/version/sequence、delete/tombstone、model/version/dimension/schema 校验、bounded metadata、server-owned tenant filter、disabled fail-closed backend route 和 deterministic fake embedding。`memory.vector_ref` 没有生产读写证据，本轮不把它当 Milvus primary key，也没有改变旧字段。

A focused ordinary/race、受影响 `memory`/`tenant`/`audit`/`storage` ordinary、vet、gofmt 和新增文件 targeted diff check 均为 `PASS`。全套 ordinary 的失败保持为环境/既有边界：G-D/production tests 缺 `TEST_DATABASE_URL`，PostgreSQL command migration test 的真实服务启动未 ready；这不是 A package failure。gopls 当前远程 `UNAVAILABLE/NOT RUN`，历史 govulncheck reachable findings 继续为 `FAIL/findings`，security clean 未声明。

B 当前 `BLOCKED/UNAVAILABLE`：go.mod 没有 Milvus SDK，也没有 authenticated local Milvus prerequisite；官方新 `github.com/milvus-io/milvus/client/v2@v2.5.0`、旧 deprecated `github.com/milvus-io/milvus-sdk-go/v2@v2.4.2` 和标准库 REST v2 方案已核对，但本轮没有添加 SDK。C 为 `NOT STARTED`，D 为 `BLOCKED`，E 为 `BLOCKED`，P1-06 总状态为 `partial`。A 没有接入 `/api/chat`，没有影响 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher、P1-04 或 P1-05 production core。

本轮 Lark requests、Telegram requests、Model Provider requests 均为 `0`；没有创建 Milvus/ Docker resources。A 的 fake embedding 不是真实 provider 质量证明；local Milvus、生产 HA/IAM/TLS、真实 embedding、durable vector task/retry/reclaim/DLQ、PostgreSQL hydration、rebuild/reconciliation、provider-side dedup 和 cross-system exactly-once 均未声明完成。
## Webhook 并发回归分类

有界 targeted ordinary 10 次得到 `5 PASS / 5 FAIL`；失败 assertion 为两个并发 webhook 只出现一个 `202`，overlay 确认另一个 `401` 来自 `TenantRegistry.ResolveIdentity` 并发 PostgreSQL identity upsert 的 `storage.ErrConflict`。失败快照为一套 identity/claim/job/audit，未发现 duplicate durable fact；通过路径的 runner、jobs、executions、outboxes 均为单数。

因此分类为 `D: Production regression`，不是 test expectation、readiness 或 fixture 修复项；本轮不修改 identity/Ingress production、P0-09F、Queue、Worker、Completion、Outbox、Dispatcher 或 P1-06A。最新两次完整 synthetic 前置 full ordinary 实际通过，但 targeted regression 尚未修复，ordinary regression gate 继续保持 blocker。

P1-06A 为限定 boundary `verified`；P1-06B 为 `blocked/unavailable`；P1-06C 未开始；P1-06D/E blocked。Lark、Telegram、Model Provider、Embedding provider requests 为 `0`，Milvus resources 为 `0`，owner-scoped test resources cleanup 为 `0`。
## P1-04 concurrent identity resolution 当前修复与验收

当前权威状态为：`P1-04: verified (functional Binding/Identity/SecretRef boundary)`；`P1-04 concurrent identity resolution: verified`。本节不覆盖历史 failure：原始 `TestProductionPostgresWebhookFailureWindows/duplicate_concurrent_webhook_has_one_logical_winner` 曾出现 `concurrent accepted responses=1`、`[401,202]`，有界 ordinary 为 `5 PASS / 5 FAIL`；失败快照为 `identity_rows=1, claims=1, jobs=1, executions=0, outboxes=0, audits=1`，根因是 `TenantRegistry.ResolveIdentity` 并发 identity insert 的 unique conflict。

修复位于 `trpcservice/storage/postgres/tenant_registry.go`：使用 `(tenant_id, channel, binding_id, external_user_id)` 的原子 `ON CONFLICT (...) DO UPDATE`（保留 `last_seen_at/updated_at`）；仅在 upsert 未返回行或明确 `user_identity_pkey` 冲突时，对完整 canonical key 做一次 bounded 查询。明确识别的 `user_identity_pkey` 冲突也仅允许进入同一 exact-key 查询；查不到 canonical row、identity ID/internal user ID 不匹配、scope/chat/thread 不匹配或其他错误均 fail closed。没有把所有 `23505`、`storage.ErrConflict` 或 `401` 转成成功，也没有修改 immutable fields。

新增 PostgreSQL barrier+WaitGroup 并发 resolver、canonical constraint、unknown primary-key conflict、immutable identity mismatch 和 category-only error tests。修复后 targeted webhook ordinary `-count=10`、targeted race `-count=10`、repository identity ordinary/race、`cmd/trpc-service` ordinary/race、`trpcservice/storage/postgres` ordinary/race、tenant/gateway/audit/tool/queue/execution/outbox/worker affected ordinary/race、full ordinary/race 均 `PASS`；原有 webhook accepted=2、runner=1、logical winner/durable exactly-one assertions 保持并通过，未观察 duplicate execution/outbox，`DATA RACE` 未观察。

本修复不修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher、Ingress contract、P1-05、P1-06A、`/api/chat`、migration、`go.mod` 或 `go.sum`；没有新增 migration/dependency/config，P1-06B 仍 `blocked/unavailable`，C `not started`，D/E `blocked`。

本轮所有 owner-scoped PostgreSQL/Redis test resources 均已 cleanup：containers/networks/volumes/pending 为 `0`；没有 Milvus resource。Lark、Telegram、Model Provider、Embedding provider requests 均为 `0`；历史 govulncheck findings 保留，`security clean` 未声明，project release/security state 仍为 `partial`。

## P1-06B 本轮远程结果

P1-06B 为 `blocked/unavailable`。已实际加入并编译 `github.com/milvus-io/milvus/client/v2@v2.5.0`，完成 provider-specific adapter、server-owned schema/index validation、tenant-safe expression、bounded context/error mapping、unit ordinary/race 和 integration-tag compile。

带真实 importer 的 scratch dependency simulation 与 actual module graph 一致；Go 1.21 和 protected dependency versions 未变，Apache-2.0 已核实。实际 local Milvus 不可执行：固定官方 v2.5.0 image 在远程宿主因 `no space left on device` 拉取失败，未创建任何本轮 Milvus resource，因此 readiness/CRUD/tenant isolation/restart evidence 为 `NOT RUN`，不是 fake PASS。

affected/full ordinary/race 因既有 PostgreSQL/lifecycle prerequisite 缺少 `TEST_DATABASE_URL` 和既有 storage migration startup failure 而 `FAIL`；无 `DATA RACE` 输出。保持 P1-06C `not started`、D/E `blocked`、P1-06 `partial`，不修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher、P1-04/P1-05/P1-06A contract 或 migration。

## P1-06B 存储释放后复验（当前权威记录）

磁盘释放后完成真实本地 Milvus 复验：run `p106b-20260903-164816`（owner labels 清点与清理），固定 Milvus v2.5.0 + 官方 etcd/MinIO tag，认证启用，authenticated readiness、schema/index/load、real upsert/search/delete、tenant A/B 隔离、cross-tenant 拒绝、mismatch fail-closed、cancellation、bounded restart/factory rebuild 全部真实 PASS；integration ordinary/race 0 skip；DATA RACE 0。adapter 最小修复：请求 config 匹配 fail-closed（dimension/model/modelVersion/schemaVersion）+ 配套单测；fixture 补 collection properties/Strong consistency/bounded 轮询/credential 拒绝测试/cleanup 重试。

回归 prerequisites 补齐后 affected ordinary/race、full ordinary（28 包 0 skip）、full race 全部 PASS；P1-04 webhook 并发窗口 5 子测试 PASS（accepted=2/winner=1 成立，历史 [401,202] 保留）。G-D dependency-failure 测试受本机 Mihomo fake-ip DNS 黑洞影响（20s>10s 窗口），用户添加 /etc/hosts 后 PASS。Telegram real gate 按用户明确授权执行（请求 >0）；Lark/Model/Embedding/Production Milvus requests 0。owner-scoped cleanup 全部归零，credential 临时文件删除。最终：`P1-06B verified only as a local Milvus adapter/integration boundary`；P1-06C not started；P1-06D/E blocked；P1-06 partial；Production Milvus/HA/Cloud NOT RUN；provider-side exactly-once NOT PROVEN；vector task/worker、retrieval/hydration、rebuild、Knowledge durable source、PostgreSQL Memory repository NOT IMPLEMENTED；security clean NOT CLAIMED；未修改 P0-09F/Queue/Worker/Completion/Outbox/Dispatcher 核心算法；无 commit/push。


## P1-06D retrieval/hydration 本轮远程结果

P1-06D 窄边界完成并验收：server-owned retrieval contract（`trpcservice/vector/retrieval`）、注入式 deterministic embedder、tenant-safe `VectorStore.Search`（事务外、bounded rounds/candidates）、candidate validation（malformed/wrong tenant/model/schema/dimension/non-finite score/duplicate 全部过滤）、PostgreSQL 权威 hydration（tenant-scoped batch 读取真实 `memory` 表；missing/deleted/tombstoned/stale/content-hash 漂移过滤）、确定性排序、cancellation/deadline 保持、unavailable/unknown 安全分类、production 默认 disabled（production.go 未改动、无 client/goroutine 构造）。真实 evidence：unit ordinary/race、真实 PostgreSQL integration（tenant A/B 并发隔离、outage/restart 恢复）、真实 local standalone Milvus v2.5.0 E2E（live hydration、tombstone/stale 过滤、tenant 隔离、closed backend 安全失败）、P1-06C regression（含 worker E2E 重跑）、P1-04 5/5、full ordinary/race 28 包 PASS、DATA RACE 0、vet/gofmt/targeted diff-check PASS。Telegram/Lark/Model/Embedding/Production Milvus requests 均 0（Historical Telegram >0 保留）；owner-scoped 资源清零；`go.mod`/`go.sum` 未修改；scanner UNAVAILABLE；security clean NOT CLAIMED。production Memory write path/enqueue composition 与 production retrieval 接线 NOT IMPLEMENTED → production closure BLOCKED；semantic quality NOT TESTED；P1-06E blocked/not started；无 commit/push。


## P1-06D-P 本轮远程结果

生产缺口关闭并验收：PostgreSQL Memory repository（同事务 Memory+vector task 原子提交，server-owned version/seq 单调推进，tombstone/delete 幂等，tenant A/B 隔离，safe error 分类），task repository `EnqueueTx`（与 `Enqueue` 共享 executor/SQL/校验，P1-06C 回归不变），production `PostgresSourceProjector`（stale/hash/deleted 安全分类，delete 边界无 content），默认关闭的 production worker/retrieval composition（enabled 缺 production Embedder 真实二进制 exit 1 fail closed），runtime Start/Stop/ForceClose/readiness 接线。真实 evidence：memory/task integration（真实 PG）、真实服务二进制 A（disabled `/healthz=200`）+ B1/B2/B3（fail-closed exit 1）、真实 local Milvus v2.5.0 production-style E2E（Put→同事务任务→worker→SourceProjector→deterministic test embedder→Milvus upsert→fenced success→retrieval hydration→update→tombstone→delete→不可再检索）。full ordinary/race 30 包 PASS，DATA RACE 0，vet/gofmt/targeted diff-check PASS。`go.mod`/`go.sum` 未修改；new migration 0；new dependency 0。Telegram/Lark/Model/Embedding/Production Milvus requests 0（Historical Telegram >0 保留；DeepSeek 凭据本轮未使用，二进制场景用 synthetic placeholder）；owner 资源清零；security clean NOT CLAIMED。`P1-06D: verified only as a production-composable Memory retrieval/hydration boundary`；Production Embedder NOT IMPLEMENTED；Production request path NOT IMPLEMENTED；P1-06E blocked/not started；P1-06 partial；无 commit/push。


## P1-06E 本轮远程结果

durable rebuild/reconciliation 边界完成并验收：migration 000008（run/cursor/fencing 约束）、keyset scan + 同事务 batch enqueue+cursor、crash resume、Redis epoch/fencing（旧 owner 全部拒绝）、attempt 预算 fail-closed、bounded reconciliation（dry-run 默认；missing/stale/tombstoned-present/wrong-projection/orphan/invalid 分类；repair 经 RedriveTx 既有 task 边界；orphan 默认 report-only）、projection registry（Router + per-projection embedder registry）、pre-provisioned target rebuild 真实收敛、cutover BLOCKED。真实 evidence：unit ordinary/race、PG/Redis integration ordinary/race（outage 注入与恢复）、真实 local Milvus E2E ordinary/race、P1-06A-D regression、P1-04 5/5、full ordinary/race 31 包 PASS、DATA RACE 0、vet/gofmt/targeted diff-check PASS。Telegram/Lark/Model/Embedding/Production Milvus requests 0（Historical Telegram >0 保留）；owner 资源清零；scanner UNAVAILABLE；security clean NOT CLAIMED。`P1-06E: verified only as a durable rebuild/reconciliation boundary`；cutover BLOCKED；生产自动触发 NOT IMPLEMENTED；`P1-06: verified only as a disabled-by-default, production-composable derived-vector lifecycle boundary`，`production rollout: BLOCKED`；无 commit/push。


## P1-07 本轮远程结果

P1-07 完成：`trpcservice/telemetry`（trace/metric providers、allowlist 低基数 metrics、JSON 结构化日志 + SafeError/Fingerprint/Truncate、carrier inject/extract/link、幂等有界 shutdown）；HTTP middleware（route 模板/status class/固定 bucket）、gateway producer span + carrier、worker attempt span links（missing/invalid carrier → context_state，不失败）、completion/sender/memory 装饰。默认 export=none；OTLP 缺 endpoint/insecure-remote fail closed；真实二进制 A（disabled ready 零连接）/B（fail closed）/C（local collector 收到 http /healthz span）/D（collector outage healthz=200）全部 PASS。in-memory E2E 全链路 span 链与 allowlist metrics 断言 PASS；业务不变量（runner=1/lark=1/telegram=0）保持。期间修复：worker attempt ctx 共享 record.ctx DATA RACE（局部 ctx + renew 启动顺序）、telemetry middleware 在 Handler() 固化后才附加的时序缺陷（惰性 per-request 解析）。回归：P1-06A–E、P1-04 5/5、P1-05、P0-09、full ordinary/race 32 包 PASS、DATA RACE 0、integration tagged 全 trpcservice 包 PASS、vet/gofmt/targeted diff-check PASS。依赖 delta 仅 otlpmetricgrpc v1.29.0 direct（同版本，scratch gate 验证）。Telegram/Lark/Model/Embedding/Production Milvus requests 0（Historical Telegram >0 保留）；owner 资源清零；scanner UNAVAILABLE；security clean NOT CLAIMED。`P1-07: verified only as a local-OTLP, low-cardinality observability and cross-boundary correlation boundary`；`Production telemetry collector rollout: NOT RUN`；`P1-06 production rollout: BLOCKED`；`P1-08/P1-09: not started`；无 commit/push。
