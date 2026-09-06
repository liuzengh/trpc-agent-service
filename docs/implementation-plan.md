# tRPC-Agent-Go 多租户平台当前实施计划

> 本文是当前有效的实施顺序和状态映射，依据当前源码、迁移、阶段验收报告和实际工作区，不依据历史计划中的未实现假设。历史编号和过程材料保留在 `docs/` 中用于追溯，但不覆盖本文件和 `project-status.md`。

## 1. 目标与当前起点

### 1.1 当前生产目标

当前首个生产版本的通道目标为：

- 飞书（Lark）机器人；
- Telegram Bot。

Web Chat 不再作为首个生产通道或异步实现目标。现有 `POST /api/chat` 必须继续保留为同步、内存、开发兼容接口，但不纳入生产异步链路、端到端生产验收或首个生产版本的通道承诺。

平台的其他固定目标保持不变：

- PostgreSQL 作为业务事实源；
- Redis 负责协调、Claim、Lease、epoch/fencing、限流和热数据；
- Milvus 作为计划接入的派生向量索引后端，不承载业务事实、Queue Ack、execution result 或 Outbox 原子 completion；
- tRPC-Agent-Go Runner 负责 Agent 执行；
- Gateway、Queue、Worker 和 Outbox 解耦；
- 同一 Session 串行、至少一次投递下业务幂等、Outbox retry/DLQ；
- 租户、Binding、Agent release、Tool Policy、审计和配置版本化；
- OTel、Docker Compose、恢复演练和安全门禁在后续阶段完成。

### 1.2 当前实际起点

当前工作区已经完成或验证了以下基础和边界：

| 范围 | 当前实际状态 | 证据 |
| --- | --- | --- |
| P0-01 至 P0-06 | 基础层已完成 | `trpcservice/` 基础包、迁移、Redis/PostgreSQL 协调和 `docs/p0-06-acceptance-matrix.md` |
| P0-07 / R1 | `contract-level verified` | 公开 Runner completion event、tail drain、取消和 bounded cleanup；framework global Wait/Done 仍未证明 |
| P0-08 / A-D、R2-R5 | `contract-level verified` | Job/Queue/Execution/Worker/Gateway 合同、fencing、delivery recovery 和 Worker 自有 bounded shutdown |
| P0-09A | `verified` | PostgreSQL execution result fenced commit |
| P0-09B | `verified` | PostgreSQL durable Job Queue、claim、visibility、Ack/Nack 和 recovery |
| P0-09C | `verified` | execution result + Queue Ack 的同一 PostgreSQL transaction |
| P0-09D | `verified` | PostgreSQL Outbox Repository、dedup、claim、retry、DLQ 和 recovery |
| P0-09E | `verified` | Outbox Dispatcher、RetryPolicy、Sender outcome、durable mutation 和 bounded shutdown |
| P0-09F | `verified` | execution result + Queue Ack + reply Outbox 的同一 PostgreSQL transaction，Worker durable path 已接线 |
| P0-09G-A | `verified` | versioned channel-aware Sender boundary、routing、registry、Fake transport 和 Dispatcher adapter |
| P0-09G-B1 | `verified (Lark real sender boundary)` | Lark config/SecretRef、schema v3 routing、official message request、provider UUID、四类 outcome、Dispatcher boundary，以及授权测试租户的真实 token/message evidence；provider-side dedup 和生产装配仍未证明 |
| P0-09G-B2 | `verified (Telegram real sender boundary)` | Telegram Sender/Adapter、fake/Dispatcher/Telegram+PostgreSQL component evidence 和历史 gated `getMe`/`sendMessage` evidence；provider-side dedup、生产装配和 exactly-once 未证明 |
| P0-09G-C | `verified (production async assembly boundary)` | 测试 PostgreSQL production composition root、五个 ingress 失败窗口和 deterministic fake transport；真实外部 send 为 0 |
| P0-09G-D | `verified (process lifecycle and recovery boundary)` | Linux SIGTERM、draining、startup/Serve failure、SIGKILL reclaim、历史与本轮 owner-scoped same-network PostgreSQL recovery 和 fencing；host-mapped dynamic endpoint bug 已修复并由 fixed-port regression 覆盖 |
| P0-09G-R | `verified (defined P0 closure boundary)` | production ObjectStore composition、Artifact metadata persistence/reconciliation、独立 MinIO gate、Redis restart failover 与 current same-network G-D recovery 已通过；scheduler/lifecycle cleanup 不属于本轮 P0 门禁，ObjectStore/G-D 为并列 boundary，enabled-object 联合故障是后续 evidence；真实 Provider、provider-side dedup、完整业务 Repository、认证和 framework global Wait/Done 仍未证明 |

因此，原总计划中的 P0-10（Job Queue/Gateway/Worker）和 P0-11（Outbox Dispatcher/Retry/DLQ）的可验证基础设施范围已经通过上述实际切片完成。它们的“完成”只表示已验证的 durable contract 和组件边界，不表示主进程生产装配、真实外部通道或完整业务 Repository 已完成。

### 1.3 当前运行事实

主进程仍然是：

```text
MemoryStore -> platform.Runner -> EchoResponder 或 runner responder
```

设置 `DATABASE_URL` 并提供完整 `BOOTSTRAP_*`/SecretRef 后，主进程 composition root 已装配 PostgreSQL Queue、Worker、Atomic Completion、Outbox、Dispatcher、Lark/Telegram webhook、真实 Sender constructors，以及按 `BOOTSTRAP_OBJECT_BACKEND=s3` 启用的 production ObjectStore 与 ArtifactMetadataRepository。default `none` 不构造 object client，enabled object 缺失配置 fail-closed；现有 `/api/chat` 仍是同步兼容接口。P0-09G-C/D 的历史与本轮 PostgreSQL assembly/recovery evidence 保留，本轮 production object evidence、Redis restart failover evidence 与 current owner-scoped same-network G-D evidence 通过；full `go test ./...` ordinary/race 在 owner-scoped synthetic prerequisites 下均通过；Provider transport 本轮为 deterministic fake，真实外部 Provider 请求仍为 0。

当前生产 bootstrap 将 `BackendPolicy.Vector` 设为 `none`；仓库尚无 Milvus client、具体 VectorStore adapter、向量索引 worker、Milvus migration/deployment 或 production retrieval wiring。现有 `platform.VectorStore`、`BackendPolicy.Vector` 和 `memory.vector_ref` 只表示领域/适配契约，不能写成 Milvus 已实现。

P0-09G-A 已提供 provider-neutral 的 Web、Telegram、历史 WeCom builder 和 HTTP transport seam。P0-09G-B1 已新增独立 `trpcservice/channels/lark/`，主进程 production composition root 现已装配真实 Lark/Telegram Sender constructors；测试 composition root 仍可显式注入 deterministic Sender 用于不触发外部请求的联合验收。源码中的 `wecom` 仍是已有占位/测试适配器，不代表当前生产目标；新的真实通道目标是 Lark 和 Telegram。

## 2. 固定决策

1. **生产通道是 Lark 和 Telegram。** 原企业微信目标取消，不再在当前计划中安排真实 WeCom adapter。
2. **Web Chat 不是生产异步通道。** 保留现有同步 `/api/chat` 兼容路径，不新增 Web Chat/SSE 生产实现任务。
3. **P0-09G-B 拆分为两个真实通道风险包。** `G-B1` 实现飞书（Lark），`G-B2` 实现 Telegram；两者不能互相覆盖验收。
4. **PostgreSQL 是事实源。** Redis 不能承载 Outbox 事实或替代 PostgreSQL completion。
5. **外部发送至少一次。** Provider-side idempotency 需要真实通道分别证明，PostgreSQL dedup 不等于 provider dedup。
6. **租户 ID 只由服务端 Binding/认证解析。** 不信任 webhook body、header 或 Agent 输出中的 tenant ID。
7. **三事实 completion 不回退。** Worker 不得拆回独立 `Commit -> Ack -> Enqueue`。
8. **Fake 不替代真实依赖。** Fake HTTP、Fake Queue、Docker restart 和接口存在不能外推真实 Provider、生产进程 crash 或 global Wait/Done。
9. **版本化而非破坏性修改。** reply routing payload schema v2、Outbox identity 和现有 Session/DedupKey 规则保持兼容。
10. **每阶段独立验收。** 一个阶段只有在源码、失败路径、真实依赖和边界测试均通过后才能标记完成。
11. **Milvus 是派生索引。** PostgreSQL Memory/Knowledge 事实先提交，Milvus 通过受租户约束的异步索引任务构建；Milvus 故障不能回写或替代 PostgreSQL 事实，向量检索与索引重建必须有明确的一致性和降级语义。

## 3. 当前路线图

| 阶段 | 目标 | 状态 |
| --- | --- | --- |
| Foundation P0-01 至 P0-06 | 领域、迁移、协调和限流基础 | 已完成 |
| Runtime P0-07 至 P0-08 | Runner、Execution、Job、Queue、Worker、Gateway 合同 | contract-level verified；生产装配未完成 |
| Durable P0-09A 至 P0-09F | PostgreSQL result、Queue、Outbox、Dispatcher、Retry、DLQ、三事实 completion | verified |
| P0-09G-A | channel-aware Sender boundary | verified |
| P0-09G-B1 | 真实飞书（Lark）Sender/Adapter | verified (Lark real sender boundary)；测试租户 token/message evidence 通过，provider-side dedup 和生产装配未证明 |
| P0-09G-B2 | 真实 Telegram Sender/Adapter | `verified (Telegram real sender boundary)`；fake/Dispatcher/Telegram+PostgreSQL component evidence 和真实 `getMe`/`sendMessage` evidence 通过；provider-side dedup、生产装配和 exactly-once 未证明 |
| P0-09G-C | Lark/Telegram Webhook 到 Queue/Worker/Outbox 的生产异步装配 | `verified (production async assembly boundary)`；真实 TEST_DATABASE_URL PostgreSQL 联合链路、五个 ingress 失败窗口、scoped race/vet/gofmt/diff 通过；本轮 Provider transport 为 deterministic fake，B1/B2 真实 Sender evidence 独立引用 |
| P0-09G-D | 主进程生命周期、SIGTERM、依赖 readiness 和端到端恢复 | `verified (process lifecycle and recovery boundary)`；Linux real SIGTERM clean subprocess、coordinator/draining、Queue/Outbox reclaim 和 fencing PASS；本机 host-mapped production PostgreSQL restart 在 `SELECT 1`/pgxpool reconnect 处为 SKIP |
| P0-09G-R | P0-09 总审查和关闭 | `verified (defined P0 closure boundary)`；production ObjectStore composition、Artifact metadata persistence/reconciliation、独立 MinIO gate、Redis restart failover 与 current same-network G-D recovery 通过；scheduler/lifecycle cleanup 不属于本轮 P0 门禁，ObjectStore/G-D 为并列 boundary，enabled-object 联合故障是后续 evidence；真实 Provider、provider-side dedup、完整业务 Repository、认证和 framework global Wait/Done 仍未证明 |

README/ARCHITECTURE 明确 Object Storage 是平台目标能力；本轮已完成 production ObjectStore composition、fail-closed config、runtime/readiness ownership、Artifact metadata repository 和 owned MinIO real gate。独立与 production evidence 均不加入 P0-09F transaction，也不声称跨 PostgreSQL/S3 exactly-once；current owner-scoped same-network G-D ordinary/race 也已 PASS。按本轮 scope decision，P0-09G-R/P0-09 在定义的 durable、assembly、object 和 recovery boundary 内为 `verified`；scheduler/lifecycle cleanup 是 deferred、非 P0 门禁，enabled-object 与 PostgreSQL recovery 的联合故障是并列 boundary 的后续 evidence。
| P1-04 | Channel Binding、Identity 和 Secret 引用管理 | partial；PostgreSQL migration/repository ordinary/race、Ingress/composition ordinary/race 和 full ordinary PASS；最后一次 full race 因既有 Queue concurrent Receive bounded i/o timeout FAIL；Secret Manager/dynamic reload/admin API 仍 deferred |
| P1-05 | Tool Policy、Guardrail、Approval/Budget 边界和统一脱敏 | `verified (functional Tool Policy/Guardrail boundary)`；Approval、durable policy/approval/budget/audit persistence、token/cost/billing deferred |
| P1-06 | Memory、Summary 和 Milvus Vector Index | 计划中，未开始 |
| P1-07 | OTel、Metrics 和结构化日志 | 未开始 |
| P1-08 | 配置发布、灰度和回滚 | verified（真实 PostgreSQL/Redis/Docker integration matrix 已通过；production rollout 未部署） |
| P1-09 | Docker Compose 和运行文档 | verified（可重复本地 Compose 部署、生命周期、依赖健康、migration/config gate 与恢复边界已验证；production rollout 未部署） |
| P2-01 | PostgreSQL RLS 和数据库强制租户隔离 | verified（本地 RLS + pooled tenant-context isolation boundary；production rollout 未执行） |
| P2-02 | 备份恢复和事件回放 | verified（本地 PostgreSQL logical backup/restore + 有界 session-event replay/recovery drill 边界；production DR/PITR/RPO-RTO 未实现或未证明） |
| P2-03 | 容量/故障保护 | 未开始 |
| P2-04 | 集成/压力/安全测试和生产运维 | 未开始 |

原计划的 `P1-01 企业微信 Adapter` 已改为当前路线的 `P0-09G-B1 飞书（Lark）Adapter`；原计划的 `P1-02 Telegram Adapter` 改为 `P0-09G-B2 Telegram Adapter`。原 `P1-03 Web Chat、SSE 和 API 鉴权` 已从当前生产计划删除，不再安排 Web Chat 异步或 SSE 实现。认证和 TenantContext 保护仍需在 Lark/Telegram 生产入口及后续管理接口中单独完成。

## 4. 已完成范围的实施记录

### P0-01 至 P0-06：基础层

已完成内容包括 Go 质量基线、租户和配置模型、Session/Event/Memory/Summary/Artifact/Audit 模型、Repository/Claim/Lease/Outbox contract、PostgreSQL migration、Redis/PostgreSQL Claim/Lease/fencing/failover 和三维限流。详细证据见 `project-status.md` 和 `p0-06-acceptance-matrix.md`。

### P0-07 / R1：Runner 边界

已验证平台类型、Runner completion event、事件尾部排空、framework-owned channel close、adapter pump done、取消和 bounded cleanup。没有公开证据证明 tRPC-Agent-Go framework 全局 Wait/Done，因此该项保持 contract-level verified。

### P0-08 / A-D、R2-R5：执行编排边界

已验证版本化 Job/DTO、租户和 Trace 恢复、History 边界、Fake Queue contract、Gateway fast ACK、Execution identity/fencing、delivery failure、visibility recovery、Worker bounded shutdown 和同步兼容路径保护。该范围不因测试替身而宣称生产异步装配完成。

### P0-09A 至 P0-09F：durable execution

已验证：

- PostgreSQL execution result fenced commit；
- durable Job Queue 和 delivery token；
- result + Queue Ack atomic completion；
- PostgreSQL Outbox Repository、dedup、claim、retry 和 DLQ；
- bounded Outbox Dispatcher 和 RetryPolicy；
- result + Queue Ack + reply Outbox 同一 transaction；
- durable Worker reply Outbox 接线、rollback 和 unknown reconciliation。

### P0-09G-A：channel-aware Sender boundary

已验证 schema v2 reply routing payload、tenant/channel/destination validation、immutable registry、provider-neutral builder、HTTPDoer seam、四类 SenderOutcome 和 Dispatcher adapter。该阶段只使用 Fake transport 和真实 PostgreSQL Outbox，不代表任何真实 Provider 调用。

### P0-09G-B1：飞书（Lark）真实 Sender boundary

已实现 schema v3 `BindingID` routing、服务端 binding/tenant 校验、SecretRef/tenant access token resolver、固定官方 HTTPS base URL、文本消息 request/success contract、官方 `uuid` provider idempotency 输入、bounded body 和四类 outcome。Lark Sender 不持有 Repository，Dispatcher 继续是唯一 durable mutation owner；`Retry-After` 安全忽略，由 bounded RetryPolicy 负责 retry timing。

已通过 `LARK_B1_REAL=1` 显式门禁的真实证据测试 `trpcservice/channels/lark/lark_real_test.go`：授权测试租户的 tenant access token、真实消息 2xx/`Delivered` 和相同 durable identity replay 均已执行；凭据仅存在于临时进程环境变量。provider-side UUID dedup 和真实 Provider + production send 仍 deferred；Webhook challenge/签名/解密与 G-C 的 deterministic fake assembly evidence 分开记录。

## 5. 后续阶段：P0-09G-B1 已完成边界

B1 的实现、Fake/contract 测试、真实 token/message evidence 和安全边界见 `docs/P0-09G-B1验收报告.md`。真实测试已补齐 Provider 可达性和 success response 证据；provider-side `uuid` 去重、真实 webhook 和生产装配仍需后续单独验收。

## 6. 后续阶段

### P0-09G-B2：Telegram 真实通道

已完成 contract-level Telegram Bot API Sender/Adapter：固定 `https://api.telegram.org`，Binding/SecretRef、numeric chat ID、`update_id` dedup key、private/group/supergroup scope、private/supergroup `message_thread_id`、`retry_after`、4096 字符上限、400/403/5xx/401、timeout、response unknown 和有限 safe code 均已实现并测试。reply payload 从 schema v3 升级为兼容 schema v4；旧 Web/Lark payload 仍可解码。Telegram `sendMessage` 没有官方 outbound idempotency/dedup contract，exactly-once 不支持，OutcomeUnknown retry 有重复发送风险。

状态为 `verified (Telegram real sender boundary)`。真实 gate 由 `TELEGRAM_B2_REAL=1` 开启，并要求本机安全来源与 canonical numeric chat identity；既有真实 `getMe`/`sendMessage` evidence 独立记录，G-C 本轮没有重跑真实 gate。Fake HTTP、真实 PostgreSQL Outbox component evidence、真实 Telegram evidence 和 G-C deterministic fake assembly 分开。Telegram production webhook/Dispatcher assembly 已由 G-C 测试覆盖；真实 Provider send、SIGTERM 和 `/api/chat` 改造保持 deferred。Telegram 证据不能由 Lark 证据覆盖。

### P0-09G-C：生产异步装配

状态为 `verified (production async assembly boundary)`。本轮在测试进程内把 `DATABASE_URL` 临时绑定到 `TEST_DATABASE_URL`，使用唯一 PostgreSQL schema，直接覆盖 `assembleProduction` 使用的同一 composition root。测试只注入 deterministic AgentFactory、recording Lark/Telegram Sender 和事件同步 hook；Queue、Worker、completion、Outbox、Dispatcher 和 PostgreSQL Repository 均为真实实现。Provider transport 为本地 deterministic fake，真实外部 send 次数为 0。目标链路为:

```text
Lark/Telegram webhook
  -> verified Binding/TenantContext
  -> Dedup Claim
  -> PostgreSQL durable Job Queue
  -> Worker/Runner
  -> atomic result + Queue Ack + reply Outbox
  -> Dispatcher
  -> selected real Sender
```

必须保留现有同步 `/api/chat` 兼容接口，但不得把它解释为生产异步 Web Chat。已验证生产装配具有唯一 owner、配置失败即启动失败、migration/pool/Queue/Worker/Dispatcher readiness、tenant/channel routing、atomic completion 和 bounded ingress ACK。

### P0-09G-D：生命周期与恢复

状态为 `verified (process lifecycle and recovery boundary)`。`cmd/trpc-service/lifecycle.go` 将 listener 显式预绑定后交给 `Serve`，由 coordinator 统一拥有 Serve error、首信号 graceful path、第二信号 forced path、HTTP drain、Worker/Dispatcher stop、pool close 和 safe exit code；clean path 输出固定 safe status。server-owned gate 在 webhook body read/Verify/Claim/Enqueue 前拒绝新入口；`/livez` 在进程存活期间保持 200，`/healthz` 在 startup、数据库不可用或 draining 时返回 503。

已通过 in-process coordinator/Web gate、实际 binary startup/listen failure、真实 Linux Unix SIGTERM clean subprocess、SIGKILL 后 Queue lease reclaim 和 Outbox lock reclaim；历史与本轮 owner-scoped same-network PostgreSQL recovery evidence 均保持独立，current G-D ordinary/race 已完整 PASS。最初 host-process dynamic endpoint 在 restart 后导致 `pool_ping=connection_refused`/`deadline=EXHAUSTED`，已修复为固定 loopback port，并由 ordinary/race raw/wrapper/SELECT 1 regression 覆盖；production ObjectStore composition 与 Artifact metadata boundary 另有独立 real MinIO evidence，未加入 P0-09F transaction。G-D Provider transport 为 deterministic fake，真实外部 Provider send 次数为 0。

### P0-09G-R：P0-09 最终审查

复核 P0-09A 至 P0-09G-D 的源码、报告、真实依赖、race、vet、格式、安全扫描、生产装配和同步 `/api/chat` 兼容性，并完成 production ObjectStore/Artifact metadata boundary、Redis restart failover evidence。production object gate、Redis restart gate 与 current owner-scoped same-network G-D recovery 均通过 config/epoch fail-closed、runtime/readiness ownership、real bytes、metadata transitions、object mismatch reconciliation、presigned access、restart/reconnect、new-owner acquire/renew、old-owner fencing、Queue/Outbox fencing、final facts 和 cleanup；full ordinary PASS；本轮最新 full race 的既有 Queue concurrent Receive bounded i/o timeout 另有记录。按 scope decision，自动 reconciliation scheduler、后台 lifecycle cleanup 是 deferred、非本轮 P0 强制门禁；ObjectStore 与 G-D 是并列 boundary，enabled-object 联合故障为后续 evidence；真实 Provider、provider-side dedup、完整业务 Repository、认证和 framework global Wait/Done 仍是独立未证明边界。P0-09G-R/P0-09 在定义范围内为 `verified`。

## 7. 后续 P1/P2

P1-04 及以后保留原有治理方向，但前置条件改为当前真实阶段：

- P1-04：Lark/Telegram Binding、Identity、SecretRef、启停和审计；不再实现企业微信 Binding。
- P1-05：Tool Policy、Guardrail、审批、预算和统一脱敏。
- P1-06：Memory/Summary/Milvus Vector Index，拆分为：
  - P1-06A：VectorStore contract、tenant-scoped identity、embedding/model version、source sequence、content hash、delete/tombstone 和 BackendPolicy 路由；
  - P1-06B：Milvus adapter、连接/collection/schema/index 参数、bounded timeout、server-owned tenant filter 和 local Docker integration；
  - P1-06C：PostgreSQL fact -> index work -> Milvus 的 Outbox/index worker，幂等 upsert/delete、重试、DLQ、lag metrics 和 reconciliation；
  - P1-06D：Knowledge/Memory retrieval 接入 Runner 前的 Context/tenant 校验、结果投影和明确的 stale/unavailable fallback；
  - P1-06E：重建、租户隔离、版本升级、Milvus outage/reconnect、跨节点竞争、数据迁移和完整验收报告。当前全部未开始，不能影响既有 P0-09F 三事实 completion。
- P1-07：OTel、低基数 Metrics、结构化日志和跨入口 correlation。
- P1-08：tenant config、Agent release、灰度和回滚。
- P1-09：Docker Compose、readiness/liveness、依赖健康检查和运行文档。
- P2-01：PostgreSQL RLS 和数据库强制租户隔离（最小映射：聚合描述中的 RLS 部分）。
- P2-02：备份恢复和事件回放（最小映射）。
- P2-03：容量/故障保护。
- P2-04：集成/压力/安全测试和生产运维手册。

这些阶段不得反向修改已经通过的 P0-09 durable contract；如果发现契约不足，新增兼容切片并单独验收。

## 8. 依赖顺序

```text
P0-01..P0-06
  -> P0-07/R1 -> P0-08/R2-R5
  -> P0-09A..P0-09F -> P0-09G-A
  -> P0-09G-B1 Lark
  -> P0-09G-B2 Telegram
  -> P0-09G-C production async assembly
  -> P0-09G-D lifecycle/recovery
  -> P0-09G-R final P0-09 review
  -> P1-04 -> P1-05 -> P1-06A..P1-06E -> P1-07 -> P1-08 -> P1-09
  -> P2-01/P2-02 -> P2-03 -> P2-04
```

同 network Linux runner 的历史与本轮 PostgreSQL recovery evidence 分开记录：本轮 owner-scoped runner attachment、`postgres:5432` alias、no-host-port topology、outage `503`、fresh pool/`SELECT 1`、healthz、Queue/Outbox reclaim、stale fencing 和 final facts 均 ordinary/race `PASS`。历史 host-mapped `unexpected EOF` 未被全局忽略，仍单独作为 transport boundary。

## 9. 文档和完成规则

- 当前源码和本轮测试结果优先于历史报告。
- 文档中的“已完成”必须有源码、测试、迁移或实际依赖证据。
- `implementation-plan-p01.md`、`plan008*` 和旧过程文件只用于追溯，不维护当前状态。
- 每个阶段必须记录允许修改范围、失败/取消/恢复测试、真实依赖门禁、未运行命令和证据边界。
- 不输出 DSN、Token、Authorization、Prompt、History、Provider body 或原始 provider error。
- 未通过或不稳定的全仓 Docker/command fixture、framework global Wait/Done、真实 Provider 联合 evidence 和 provider-side dedup 不能写成已验证；P0-09G-D 的 Windows host-mapped production restart `unexpected EOF` 作为 historical transport boundary 保留，不覆盖本轮 Linux network PASS；Worker ordinary 50/50、race 10/10 和 P006C synthetic prerequisites 结果必须保留。
- P1-06 Milvus 在没有具体 adapter、真实隔离 Milvus、租户过滤、幂等 upsert/delete、Outbox/index worker、重建和故障恢复证据前，只能写成计划或 contract-level 状态；Milvus client 可用不等于生产向量检索完成。
- P0-09G-B1 已更新本文件和 `project-status.md`；状态为 `verified (Lark real sender boundary)`，但不得把一次测试租户 evidence 外推为 provider-side dedup、生产装配或 exactly-once verified。
- P0-09G-B2 已更新本文件和 `project-status.md`；状态为 `verified (Telegram real sender boundary)`，真实 `getMe`/`sendMessage` gate evidence 独立引用，Lark 证据不能覆盖 Telegram。
- P0-09G-C 已更新本文件、`project-status.md`、`ARCHITECTURE.md` 和 `plan009background.md`；状态为 `verified (production async assembly boundary)`。本轮没有重跑真实 Lark/Telegram；B1 PostgreSQL + real Lark combined evidence 仍为独立 `SKIP`。
- P0-09G-R 已新增并同步 production ObjectStore composition、Artifact metadata repository、real MinIO evidence、Redis restart failover fix 和 current same-network G-D classification；G-D owner-scoped runner ordinary/race 已完整 PASS，Redis restart failover focused ordinary/race 与 full ordinary PASS；本轮最新 full race 的既有 Queue concurrent Receive bounded i/o timeout 另有记录。按 scope decision，P0-09G-R/P0-09 在定义的 durable、assembly、object 和 recovery boundary 内为 `verified`；自动 reconciliation scheduler、后台 lifecycle cleanup 是 deferred、非 P0 门禁，ObjectStore/G-D 是并列 boundary，enabled-object 联合故障 evidence 为后续；真实 Provider、provider-side dedup、完整业务 Repository、认证和 framework global Wait/Done 仍未证明。历史 dynamic host-port diagnosis 与 fixed-port regression 单独记录。本轮未修改 P0-09F、Queue、Worker、Completion、Outbox 或 Dispatcher 核心算法。

## 10. 本轮验收补充

Worker 的 `TestWorkerStopForcesCancellationAndReportsUnresolved` 通过 ordinary 50/50 和 race 10/10；fake Queue 使用事件同步的显式 Nack/recovery failure，断言 `ErrDrainTimeout`、`ErrDeliveryUnresolved`、无 Commit/Ack、Queue close 最多一次和 `inFlight` 保留。

同 network Linux runner 的 current PostgreSQL recovery evidence、production object composition、独立 MinIO object transport gate、Redis restart failover ordinary/race 和受影响 ordinary/race 分开记录。Redis restart gate 使用 owner-scoped Redis container/network/volume、固定 loopback host port、Redis persistence volume、独立 new-owner client/store、epoch-aware acquire、new-owner renew、old-owner fencing、durable fact check 和 cleanup；production object gate 使用唯一 owner-labeled MinIO container/volume/network、内部 `minio` alias 和仅绑定 127.0.0.1 的固定 loopback port。上述 current evidence 均通过；ObjectStore 与 G-D 未执行 enabled-object 联合故障场景，但按 scope decision 该场景是并列 boundary 的后续 evidence，不阻塞定义范围内的 P0 closure；scheduler/lifecycle cleanup 是 deferred、非 P0 门禁；P0-09G-R/P0-09 在定义范围内 verified，P1-06 仍 blocked/not started，真实 Lark/Telegram/Model Provider 请求保持 `0`。

## 11. P1-04 当前阶段记录

P1-04 只处理 Lark/Telegram Channel Binding、external Identity、SecretRef reference/resolver、Binding enabled/disabled/expired 生命周期、PostgreSQL metadata persistence、有限 Binding/Identity Audit 和 production composition 最小接线。企业微信 Binding、Tool Policy、Guardrail、审批、预算和统一脱敏属于 P1-05；Memory、Summary、Milvus、vector adapter、index worker 和 retrieval 属于 P1-06A..P1-06E，本轮全部未开始。

当前实现已增加 server-owned Binding/Identity/SecretRef contract。Ingress 只信任 adapter/channel 和 provider external app identity，body/query/header/message 不得覆盖 tenant、binding、channel 或 destination；Identity 在 Claim 前解析，Telegram topic thread 进入 thread-aware session scope。`/api/chat` 仍保持同步 `MemoryStore -> Runner -> EchoResponder` 兼容语义。

`000006_p1_04_binding_identity` 增加 Binding 的 verify SecretRef、状态、version、过期和 bounded target metadata，增加 Identity internal ID/status/version/scope 字段，并建立 tenant-scoped Binding Audit 表。PostgreSQL 是 metadata 事实源，Redis 不作为唯一来源；Update 使用 version/CAS。生产使用一个 startup SecretRef resolver，当前只实现 `env://` reference boundary，不声称云 Secret Manager/KMS/Vault/IAM 集成。disabled optional binding 不注册新入口且不阻塞启动；动态 reload/reconcile deferred。

`000006_p1_04_binding_identity` 增加 Binding 的 verify SecretRef、状态、version、过期和 bounded target metadata，增加 Identity internal ID/status/version/scope 字段，并建立 tenant-scoped Binding Audit 表。PostgreSQL 是 metadata 事实源，Redis 不作为唯一来源；Update 使用 version/CAS。生产使用一个 startup SecretRef resolver，当前只实现 `env://` reference boundary，不声称云 Secret Manager/KMS/Vault/IAM 集成。disabled optional binding 不注册新入口且不阻塞启动；动态 reload/reconcile deferred。

## 11. P1-04 当前证据（本轮）

本节当前证据覆盖真实 PostgreSQL migration/repository、Ingress/composition、CAS/lifecycle、scope conflict、Audit/redaction、rollback/cancellation、无 Claim 的 pre-enqueue rejection、full ordinary 和最终两次独立 full race；最终 race 无 `DATA RACE`。P0-09G-R/P0-09 保持 defined boundary `verified`；P1-04 功能状态为 `verified`，project release/security state 为 `partial`；P1-05 为 `verified (functional Tool Policy/Guardrail boundary)`，P1-06/Milvus 为 `ready / not started`；Lark、Telegram、Model Provider 请求均为 `0`，没有创建 Milvus resource，也没有修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher 核心算法。

真实 PostgreSQL migration/repository ordinary/race 均 PASS：`000006_p1_04_binding_identity` 创建 Binding 生命周期字段、tenant-scoped Audit、外键、状态/启用一致性、target/version 约束和 Lark/Telegram provider identity partial unique index；既有 `web` P0 schema fixture 保持兼容。测试覆盖 Binding create/read/update、tenant isolation、provider identity duplicate、CAS、disabled/active/expired/retired、Identity private/group/topic scope、scope conflict、SecretRef reference-only persistence、Audit fingerprint、foreign-key rollback、context cancellation/deadline 和 PostgreSQL durable read。

Ingress/composition ordinary/race 均 PASS。Ingress 在 Claim 前执行 server-owned Binding/TenantContext、SecretRef resolution 和 external Identity；unknown/disabled/expired/cross-tenant/cross-channel/cross-binding/scope conflict 和 secret resolution failure 不进入 Claim/enqueue。production 的 Lark/Telegram synthetic webhook、五个 failure windows、Queue/Worker/Completion/Outbox/Dispatcher assembly、readiness/lifecycle 和同步 `/api/chat` 兼容测试均通过。P1-04 不修改 P0-09F、Queue、Worker、Completion、Outbox 或 Dispatcher 核心算法。

`env://` 是当前唯一真实 local resolver；`secret://` 只做 reference validation，`EnvironmentSecretResolver` 返回 validated-but-unresolved，不代表云 Secret Manager/KMS/Vault/IAM 支持。enabled channel 默认显式为 `false`；enabled 配置会在 startup 解析必要 SecretRef，缺失 ref/value fail closed；disabled optional channel 不创建 adapter/sender，不阻塞 startup。配置 reload、Secret rotation 生效和 reconcile deferred。

本节保留 P1-04-R 前的历史 Queue full-race failure：当时 full ordinary 通过，但最后一次 full race 在 Queue concurrent Receive 的 claim candidate 阶段出现 bounded `i/o timeout`，无 `DATA RACE`；随后 targeted diagnostic 通过但不能覆盖该 full-race failure。P1-04-R 已完成 test-only 修复，最终 focused/package/affected/full ordinary、两次独立 full race 均通过。

## 12. P1-04 最终状态更正

P1-04-R 前的历史状态为 `partial`，原因是当时 Queue full-race failure 尚未完成定性；该历史 failure 不代表当前功能门禁失败。当前 P1-04 functional closure 为 `verified`，project release/security state 为 `partial`；P1-05 为 `verified (functional Tool Policy/Guardrail boundary)`，P1-06/Milvus 为 `ready / not started`。

gopls 无 error、仅有 performance suggestion；gitleaks `PASS/clean`；deadcode `PASS/clean`；jscpd 最终全仓 Go 140 文件只报告 `252 exact clones`、`2038 duplicated lines`、`5.29%`，不是 duplication-free；govulncheck `FAIL/findings`，5 个可达依赖漏洞；trivy/opengrep `UNAVAILABLE`；madge 对 Go 项目为 `N/A`。`env://` 是唯一真实 local SecretResolver，`secret://` 为 validated-but-unresolved；dynamic reload/reconcile、rotation、Admin API、认证和云 Secret Manager/KMS/Vault/IAM deferred；P1-05 为 `verified (functional Tool Policy/Guardrail boundary)`，P1-06/Milvus 为 `ready / not started`；真实外部请求为 `0`，没有创建 Milvus resource。

## P1-04-R Final Closure Investigation

Queue race 的源码路径、历史 failure、修复和 bounded 复现矩阵以 `docs/P1-04部分验收报告.md` 为准。最终源码中 focused ordinary/race、Queue package ordinary/race、affected race packages、full ordinary 和两次独立 full race 均 `PASS`，无 `DATA RACE`。修复仅触及 `trpcservice/queue/postgres_queue_integration_test.go` 的 test-only barrier、deadline-coupled timeout 分类和安全 pool diagnostics；没有修改 Queue production semantics、SQL、timeout、Ack/Nack、fencing、Completion 或其他 P0 contract。原始 `ConcurrentReceive` timeout、后续 `ExtendVisibility/requireNoDurableDelivery` 同类 timeout 和一次 test-only compile error 均保留在报告中。

`govulncheck ./...` 为 `exit=3`，5 个 reachable findings 已准确分类：grpc `GO-2026-6061`/GHSA、x/text `GO-2026-5970`/CVE-2026-56852、pgx `GO-2026-5004`/CVE-2026-41889、OTel SDK `GO-2026-4394`/CVE-2026-24051、go-redis `GO-2025-3540`/CVE-2025-29923。当前/fixed versions 分别为 grpc `1.65.0/1.82.1`、x/text `0.21.0/0.39.0`、pgx `5.7.2/5.9.2`、OTel `1.29.0/1.40.0`、go-redis `9.7.0/9.7.3`；相关版本在 P1-04 前已存在，未自动升级。升级可能影响 Go baseline、framework gRPC/OTel、PostgreSQL、Redis 以及全部历史 gates，需单独授权。

P1-04 functional closure 现为 `verified`；project release/security closure 仍为 `partial`，因为没有零 reachable vulnerability policy decision 或依赖升级授权，不能声称 security clean。P0-09G-R/P0-09 按原 defined boundary 保持 `verified`；P1-05 为 `verified (functional Tool Policy/Guardrail boundary)`；P1-06/Milvus 为 `ready / not started`；真实外部请求为 `0`，Milvus resource 为 `0`。

## jscpd 最终统计更正

最终全仓 jscpd 扫描覆盖 140 个 Go 文件，`252 exact clones`、`2038 duplicated lines`、`5.29%`，exit 0。此前较低的 52 clones/2.71% 仅为 bounded 子集统计；不能将 duplication 结果写成 security clean，govulncheck 5 个 findings 仍需独立 remediation。

## 当前阶段顺序

```text
P0-09G-R verified (defined P0 closure boundary)
  -> P1-04 verified (functional Binding/Identity/SecretRef boundary)
  -> P1-05 verified (functional Tool Policy/Guardrail boundary)
  -> P1-06A..P1-06E ready / not started
```

P1-04 的 cloud Secret Manager/KMS/Vault/IAM、dynamic reload、Secret rotation、automatic reconciliation、Admin API、完整认证、provider-side dedup、cross-system exactly-once 和依赖 security remediation 仍 deferred；P1-06/Milvus 未开始。

## 13. P1-05 当前证据

P1-05 已在当前 `agent.ToolInvoker` 入口外完成最小 provider-neutral 安全边界：`trpcservice/tool` 提供 immutable server-owned Tool registry、tenant/agent/release/config 绑定、版本化静态 policy contract、fail-closed policy resolver、pre/post Guardrail pipeline、bounded process-local count/time/concurrency/input/output budget admission、稳定 fingerprint 和安全 Audit sink contract。`agent.ToolSpec` 增加 server-owned `Version`/`Capability` 声明 metadata；模型/user 参数不能覆盖 Tool identity、implementation、tenant、policy、approval 或 budget owner。

实际执行顺序为：trusted TenantContext 和 immutable AgentSpec -> registry lookup -> policy decision -> pre-tool schema/size/sensitive/capability guardrail -> bounded budget reservation -> safe pre-decision Audit -> server-owned implementation -> post-tool size/redaction/error guardrail -> bounded budget commit -> safe post-decision Audit。unknown、disabled、undeclared、cross-tenant、wrong Agent/release/config、missing/expired/version-mismatched/unavailable policy、approval-required、budget unavailable/exceeded、invalid/oversized/sensitive input、unsafe capability、timeout/cancellation 和 Audit failure 均 fail closed；policy decision 发生在真实 implementation 调用之前。

当前源码没有 P1-05 durable policy/approval/budget schema、Approval Admin/API、完整生产 AuditRepository 或 provider billing/token cost contract，因此本轮没有新增 migration 或 PostgreSQL/Redis fact implementation。Approval 只保留 server-generated ID、tenant/tool/scope/fingerprint/policy-version、status/expiry/version/CAS/one-time contract 类型，危险能力返回 `approval_required`/`approval_deferred`，不伪造 approval。Budget 仅证明服务端生成 scope 的 bounded count/time/concurrency/input/output admission；token、cost、provider billing、durable reservation/accounting 和 Redis coordination/cache integration 均 `DEFERRED`。生产默认构造空 registry、fail-closed policy、单一 Guardrail/redactor 和无实现绑定的 fail-closed invoker，不注入 fake allow/approval/budget。

统一 `audit.Redactor` 覆盖常见 SecretRef/plain secret values、Bearer/Authorization、DSN、prompt/history/webhook/provider/tool input/output、raw backend error、lease/approval/budget token、object/vector metadata key，并提供 bounded output 和稳定 SHA-256 fingerprint；这不是完整 DLP/content moderation。Tool Audit 只保存 tenant-scoped safe IDs、category、versions、sizes、fingerprints 和 latency，不保存原始参数/结果、prompt、history、provider body 或 token。

P1-05 focused ordinary/race、P1-04 regression、PostgreSQL/Queue/Worker/Outbox/CMD production affected ordinary/race、full ordinary、full race、`go vet ./...`、affected `gofmt` 和 targeted `git diff --check` 均 `PASS`；full race 无 `DATA RACE`。本轮使用 owner-scoped PostgreSQL/Redis synthetic prerequisites，Lark、Telegram、Model Provider 请求为 `0`，Milvus resource 为 `0`。gopls、gitleaks、govulncheck、deadcode、jscpd、trivy、opengrep、madge 当前远程不可用；既有权威报告中的 govulncheck 5 reachable findings 和 project release/security `partial` 状态继续独立保留，不能写成 security clean。

P0-09G-R、P0-09 和 P1-04 仍保持各自已定义功能边界的 `verified`；P1-05 为 `verified (functional Tool Policy/Guardrail boundary)`；P1-06/Milvus 为 `ready / not started`。P1-05 没有修改 P0-09F、Queue、Worker、Completion、Outbox 或 Dispatcher production core algorithm，也没有开始 Milvus。

## P1-06A 当前实施记录

P1-06A 已完成并有当前源码证据：新增 `trpcservice/vector` provider-neutral contract、server-owned `vd-v1-` document identity、Memory durable fact 映射、content hash/version/sequence/delete/tombstone、model/version/dimension/schema 校验、bounded allowlisted metadata、server-owned tenant filter、disabled backend fail-closed 路由和 deterministic fake embedding。A 的 ordinary/race、受影响 unit tests、vet、gofmt 通过。

当前仍没有 Knowledge durable model/repository、vector task migration/repository、Milvus SDK adapter、retrieval hydration、worker/reclaim/DLQ/reconciliation 或 production vector composition；不得把既有 Outbox、Queue 或 P0-09F completion 当作 vector task。`P1-06B` 因尚未选定并接入 SDK 且没有 local authenticated Milvus prerequisite，标记 `blocked/unavailable`；C、D、E 分别为 `not started`、`blocked by C`、`blocked by B/C/D`。P1-06 总状态为 `partial`。

本轮不添加 SDK 或 migration，不接入 `/api/chat`，不修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher 核心算法。官方新 SDK `github.com/milvus-io/milvus/client/v2@v2.5.0` 与旧 `github.com/milvus-io/milvus-sdk-go/v2@v2.4.2` 已完成可行性核对；旧模块已 deprecated，新模块依赖面较大；标准库 REST v2 是零新增依赖备选。选型和 B adapter 保留到有完整 dependency/security/local integration gate 的阶段。
## Webhook 并发回归分类

`duplicate concurrent webhook has one logical winner` 的原始 assertion 期望两个 `202 Accepted`，随后验证单 runner 和单 jobs/executions/outbox。overlay 将失败定位为 `TenantRegistry.ResolveIdentity` 并发 identity upsert 的 PostgreSQL unique conflict，Ingress 安全返回 `[401,202]`；tenant-scoped 失败快照为一套 identity/claim/job/audit，未发现 duplicate durable fact。

有界 targeted ordinary 10 次为 `5 PASS / 5 FAIL`；targeted race、`cmd/trpc-service` ordinary/race 通过。最新两次完整 synthetic 前置 full ordinary 通过，但 identity regression 仍可重复，故 ordinary regression gate 保持 `FAIL / unresolved production regression`。分类为 `D: Production regression`，本轮没有 test-only 或 production 修复，也没有削弱 exactly-one assertion。

本轮保持 P1-06A 限定 boundary verified、P1-06B blocked/unavailable、P1-06C not started、P1-06D/E blocked；没有添加 dependency/migration，外部真实请求为 `0`，Milvus resources 为 `0`。
## P1-04 concurrent identity resolution 当前修复与验收

当前权威状态为：`P1-04: verified (functional Binding/Identity/SecretRef boundary)`；`P1-04 concurrent identity resolution: verified`。本节不覆盖历史 failure：原始 `TestProductionPostgresWebhookFailureWindows/duplicate_concurrent_webhook_has_one_logical_winner` 曾出现 `concurrent accepted responses=1`、`[401,202]`，有界 ordinary 为 `5 PASS / 5 FAIL`；失败快照为 `identity_rows=1, claims=1, jobs=1, executions=0, outboxes=0, audits=1`，根因是 `TenantRegistry.ResolveIdentity` 并发 identity insert 的 unique conflict。

修复位于 `trpcservice/storage/postgres/tenant_registry.go`：使用 `(tenant_id, channel, binding_id, external_user_id)` 的原子 `ON CONFLICT (...) DO UPDATE`（保留 `last_seen_at/updated_at`）；仅在 upsert 未返回行或明确 `user_identity_pkey` 冲突时，对完整 canonical key 做一次 bounded 查询。明确识别的 `user_identity_pkey` 冲突也仅允许进入同一 exact-key 查询；查不到 canonical row、identity ID/internal user ID 不匹配、scope/chat/thread 不匹配或其他错误均 fail closed。没有把所有 `23505`、`storage.ErrConflict` 或 `401` 转成成功，也没有修改 immutable fields。

新增 PostgreSQL barrier+WaitGroup 并发 resolver、canonical constraint、unknown primary-key conflict、immutable identity mismatch 和 category-only error tests。修复后 targeted webhook ordinary `-count=10`、targeted race `-count=10`、repository identity ordinary/race、`cmd/trpc-service` ordinary/race、`trpcservice/storage/postgres` ordinary/race、tenant/gateway/audit/tool/queue/execution/outbox/worker affected ordinary/race、full ordinary/race 均 `PASS`；原有 webhook accepted=2、runner=1、logical winner/durable exactly-one assertions 保持并通过，未观察 duplicate execution/outbox，`DATA RACE` 未观察。

本修复不修改 P0-09F、Queue、Worker、Completion、Outbox、Dispatcher、Ingress contract、P1-05、P1-06A、`/api/chat`、migration、`go.mod` 或 `go.sum`；没有新增 migration/dependency/config，P1-06B 仍 `blocked/unavailable`，C `not started`，D/E `blocked`。

本轮所有 owner-scoped PostgreSQL/Redis test resources 均已 cleanup：containers/networks/volumes/pending 为 `0`；没有 Milvus resource。Lark、Telegram、Model Provider、Embedding provider requests 均为 `0`；历史 govulncheck findings 保留，`security clean` 未声明，project release/security state 仍为 `partial`。

## P1-06B 远程执行结果（当前）

状态保持 `blocked/unavailable`。`trpcservice/vector/milvus` adapter 和窄 unit 已完成，SDK 为 `github.com/milvus-io/milvus/client/v2@v2.5.0`；public VectorStore contract、P1-06A identity/projection contract、生产默认 `vector=none` 均未改变。

dependency gate 为 `PASS`：真实 importer scratch 与实际 module graph 一致，Go 1.21、pgx、go-redis、x/text、gRPC、protobuf、OTel、trpc-agent-go 未发生版本变化。实际 go.mod/go.sum delta 只包含 SDK 及可解释传递依赖，未运行 `go mod tidy`。

unit ordinary/race、vector vet、integration tag compile 为 `PASS`。真实 local Milvus readiness、schema/index/load、CRUD/search、tenant isolation、mismatch 和 bounded restart/reconnect 为 `NOT RUN`，因为固定 v2.5.0 image 拉取在远程宿主以 `no space left on device` 失败。

affected/full ordinary 和 race 为 `FAIL`，既有 PostgreSQL/lifecycle prerequisite 缺少 `TEST_DATABASE_URL`，且 storage migration command 有既有 startup failure；不将这些失败归因于 P1-04 concurrent identity 修复。P1-06C `not started`，P1-06D/E `blocked`，P1-06 `partial`；security clean、production Milvus、HA/failover、retrieval、worker/reconciliation 和 rebuild 不声明。

## P1-06B 存储释放后复验结果（当前）

P1-06B 从 `blocked/unavailable` 更新为 `verified only as a local Milvus adapter/integration boundary`。依赖图复核通过（go.mod/go.sum 哈希与添加 SDK 后状态一致：`dd52acf5...9bdad6` / `1ff22edc...031c5d`，受保护依赖未变，`go mod verify` PASS）。adapter 增加请求与 server-owned config 匹配的 fail-closed 校验（dimension/model/modelVersion/schemaVersion），integration fixture 补 properties/Strong consistency/bounded 轮询/credential 拒绝/cleanup 重试。

真实 evidence：authenticated local Milvus readiness（错误/匿名拒绝）、real CRUD/search、tenant isolation、repeated delete=unknown、mismatch fail-closed、bounded restart + factory rebuild、integration ordinary/race 0 skip PASS。affected ordinary/race、full ordinary（28 包 0 skip）、full race 全部 PASS，DATA RACE 0，vet/gofmt/targeted diff-check PASS。G-D dependency-failure 依赖用户添加的 /etc/hosts 条目（Mihomo fake-ip DNS 黑洞根因）。Telegram real gate 按用户明确授权执行（请求 >0）；Lark/Model/Embedding/Production Milvus 0。owner-scoped cleanup 完成归零；P1-06C not started、D/E blocked、P1-06 partial；security clean 未声明；无 commit/push。


## P1-06D retrieval/hydration boundary 结果（当前）

P1-06D 以窄边界交付并通过定义范围验收：`P1-06D injected retrieval/hydration boundary: PASS`；`production Memory hydration (write path/enqueue composition): NOT IMPLEMENTED`；production closure `BLOCKED/UNAVAILABLE`。新增 `trpcservice/vector/retrieval`：server-owned bounded 请求、注入式 embedder、tenant-safe `VectorStore.Search`（PG 事务外）、candidate validation 与 stale/tombstone 过滤、PostgreSQL 权威 hydration（tenant-scoped batch 读取 `memory` 表、content freshness 以 server-owned hash 重算）、确定性排序、fail-closed 配置；无 caller tenant/collection/filter 字段，未接线 production，未修改受保护核心与 `go.mod`/`go.sum`。真实证据与回归矩阵见 `docs/P1-06D验收报告.md`。P1-06E 保持 blocked/not started。


## P1-06D-P production composition 结果（当前）

production-composable 边界关闭：`storage/postgres/memory.go`（同事务 Memory+task enqueue、server-owned version/seq、tombstone 幂等、tenant 隔离）、`vector/task` `EnqueueTx`（与 `Enqueue` 共享 SQL）、`vector/task/source_postgres.go` production SourceProjector、`cmd/trpc-service/vector_composition.go` 默认关闭组装（enabled 缺 production Embedder fail closed，真实二进制验证）与 runtime 生命周期接线。真实证据与状态详见 `docs/P1-06D-P验收报告.md`。`P1-06D: verified only as a production-composable Memory retrieval/hydration boundary`；Production Embedder NOT IMPLEMENTED；P1-06E 未开始。


## P1-06E rebuild/reconciliation 结果（当前）

durable rebuild/reconciliation 边界交付并验收：migration 000008、fenced run repository、keyset scan（同事务 batch enqueue+cursor、resume）、bounded reconciliation（dry-run 默认、repair 经 RedriveTx/task 边界、orphan report-only）、projection registry（Router+per-projection embedder registry）、pre-provisioned target rebuild PASS、cutover BLOCKED。真实证据见 `docs/P1-06E验收报告.md`。生产默认 disabled、无触发路径；Production Embedder/Production Milvus/semantic quality 未实现或未验证；P1-06 总状态见 project-status。


## P1-07 observability 结果（当前）

P1-07 窄边界交付并验收：`trpcservice/telemetry`（OTel trace/metric providers、low-cardinality registry、结构化 slog JSON logger、safe error mapper、幂等有界 shutdown）、W3C traceparent 可选 carrier 穿过 durable Queue（additive 字段，旧 schema 兼容）、webhook→queue→worker→completion→outbox→dispatcher correlation、默认 none/显式 OTLP fail-closed、本地 Collector integration 与 outage 语义。真实证据见 `docs/P1-07验收报告.md`。依赖 delta：仅 otlpmetricgrpc v1.29.0 direct 化（同版本）；Production telemetry rollout NOT RUN。
