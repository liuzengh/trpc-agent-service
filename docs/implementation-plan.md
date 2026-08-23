# tRPC-Agent-Go 多租户平台实施总计划

> 版本：2026-08-23，基于 Git `62d6963`。P0-01 至 P0-06 已完成；本文只规划从当前 HEAD 继续的工作，并保留已完成阶段作为基线。旧的 `implementation-plan-p0.md`、`implementation-plan-p01.md`、`plan006*` 是历史过程材料，不再作为任务状态来源。

## 1. 目标、交付口径和当前起点

### 1.1 目标

交付一个可本地部署、可测试、可审计的首个生产版本：

- Web Chat、企业微信、Telegram 三类入口。
- PostgreSQL 作为业务事实源，Redis 负责协调、幂等、Lease、fencing、限流和热缓存。
- tRPC-Agent-Go Runner 执行 Agent，支持 Tool、MCP、Guardrail 和 Context 取消。
- Gateway 与 Worker 分离，Worker 无状态，可水平扩展。
- 同一 Session 串行、消息至少一次投递下业务幂等、Outbox 重试和 DLQ。
- 租户级 Agent、Binding、BackendPolicy、Tool Policy、配额、审计和配置发布。
- OTel Trace/Metrics/Logs、Docker Compose、迁移/备份/恢复和集成测试。

### 1.2 当前起点

已经完成的基础层：

| 阶段 | 已交付 |
| --- | --- |
| P0-01 | Go 依赖和 CI 质量基线 |
| P0-02 | Tenant、AgentApp、ChannelBinding、BackendPolicy、TenantContext、Resolver 契约 |
| P0-03 | Session/Event/Memory/Summary/Artifact/Audit 领域模型和测试 |
| P0-04 | Session key、DedupKey、Repository/Claim/Lease/Outbox 契约和 Fake/contract tests |
| P0-05 | PostgreSQL 迁移器、checksum、advisory lock、readiness，初始 15 表 schema |
| P0-06 | Redis/PostgreSQL Claim、Lease、epoch/fencing、failover、circuit breaker、三维限流和真实集成测试 |

当前仍是开发联调状态：主进程注入 `MemoryStore` 和 `EchoResponder`；设置 `DATABASE_URL` 只会初始化迁移 readiness，不会切换业务 Repository。企业微信、Telegram、HTTP chat、Resolver 也仍是最小壳。详细核对见 [`project-status.md`](project-status.md)。

## 2. 固定决策

1. **Worker 无状态。** 不依赖 sticky session；共享 Repository + Session Lease 保证跨节点一致性。
2. **PostgreSQL 是事实源。** Redis、向量库、对象存储都不能成为唯一业务数据源。
3. **Gateway 快速 ACK。** IM 回调不执行模型和长时间 Tool；入站消息转换为 AgentJob，Job 丢失时通过同事务 Outbox 恢复。
4. **fencing 是提交条件。** Lease owner、epoch 和 fencing token 在事件、Claim 完成和 Outbox 创建时再次验证。
5. **所有外部发送至少一次。** 通过 Outbox、provider dedup key、有限重试和 DLQ 处理不确定发送。
6. **租户 ID 只由服务端解析。** 不信任请求 body、header 或 Agent 输出中的 tenant ID。
7. **危险工具默认关闭。** 首个生产版本不提供任意 Shell/代码执行；后续必须配合审批和沙箱。
8. **版本化而非破坏性修改。** Session ID 和协调 key 使用 v1 规则；配置和 Agent release immutable，回滚移动指针。
9. **先完成可验证垂直链路。** 每一阶段必须有单测、竞态测试或真实依赖测试和可运行验收，不以“接口已定义”算完成。

## 3. 阶段总览

| 阶段 | 目标 | 任务 | 退出条件 |
| --- | --- | --- | --- |
| Foundation | P0-01 至 P0-06 基础层 | 已完成 | 质量、领域、迁移、协调验收矩阵通过 |
| Runtime Core | 真正运行 Agent | P0-07、P0-08 | Runner 可取消、事件可排空、租户版本隔离 |
| Durable Execution | 事实源 Repository、Job、Outbox | P0-09、P0-10、P0-11 | 重启、重复、接管、DLQ 验收通过 |
| Channels & API | 真实入口和鉴权 | P1-01 至 P1-04 | Web/企业微信/Telegram 端到端通过 |
| Governance & Ops | 治理、观测、发布和部署 | P1-05 至 P1-09 | 安全、成本、灰度、Compose 和 OTel 可验收 |
| Resilience Release | 迁移、恢复、压测和安全 | P2-01 至 P2-04 | 发布清单和生产前验收全部通过 |

每个任务独立提交，提交说明包含任务 ID；不要把真实外部服务接入、迁移和大范围重构混入同一任务。

## 4. 任务清单

### Foundation：已完成，不重复实施

#### P0-01 依赖与质量基线：已完成

证据：`go.mod`、`go.sum`、`.github/workflows/ci.yml`、`lint.sh`。现有基线包含 `go test ./...`、`go vet ./...`、gofmt、golangci-lint 和租户边界 race test。

#### P0-02 租户领域与 Context：已完成

证据：`trpcservice/tenant/`、`trpcservice/config/`。已覆盖状态、版本、BackendPolicy、Binding 映射、Context 校验和权限判断。

#### P0-03 领域模型：已完成

证据：`trpcservice/session/`、`memory/`、`artifact/`、`audit/`。已覆盖状态迁移、事件、版本、生命周期字段和审计脱敏。

#### P0-04 存储契约：已完成

证据：`trpcservice/storage/`、`session/key.go`。已固定 Repository、Claim、Lease、epoch/fencing、Summary、Artifact、Audit 和 Outbox 的接口形状。

#### P0-05 PostgreSQL Schema：已完成

证据：`migrations/000001_initial.*`、`trpcservice/storage/postgres/`。已完成迁移目录校验、checksum、advisory lock、事务、readiness 和测试数据库脚本。

#### P0-06 Redis/PostgreSQL 协调：已完成

证据：`migrations/000002_coordination.*`、`trpcservice/storage/{redis,postgres,coordination}/`、`trpcservice/ratelimit/`、`docs/p0-06-acceptance-matrix.md`。已完成 Claim、takeover、Complete/Fail fencing、Session Lease、renewal runner、epoch authority、failover/circuit breaker、三维限流和跨后端/真实依赖验收。

### Runtime Core：从当前 HEAD 开始

#### P0-07 tRPC-Agent-Go Runner Adapter

**目标：** 替换 `EchoResponder`，建立可测试的 `AgentRuntime`，把平台事件和 tRPC-Agent-Go Runner 事件连接起来。

**主要修改：** `trpcservice/agent/`、`trpcservice/tool/`、`trpcservice/platform/`、必要的 tRPC-Agent-Go 依赖升级。

**交付内容：**

- `AgentFactory` 按租户、AgentApp、AgentRelease 创建 Runner。
- `AgentInput`/`AgentResult`/`RunnerEvent` 平台类型，不向上层泄露不稳定的框架内部类型。
- Model、Tool、MCP、Guardrail 的 Context、TenantContext 和 trace 传播。
- 事件消费者在成功、错误、取消三种路径排空 channel；明确 goroutine owner 和关闭顺序。
- `MODEL_PROVIDER=echo` 只保留为测试/开发 provider。

**测试与验收：**真实或 fake model 成功、模型错误、Tool 错误、Context 取消、事件 channel 排空、不同租户 Agent 配置隔离、预算拒绝。执行 `go test ./trpcservice/agent ./trpcservice/tool ./trpcservice/platform -race`。

**不做：** 不接 IM、不持有长数据库事务、不允许 Agent 修改租户配置。

#### P0-08 Session Execution Orchestrator

**目标：** 将现有同步 Runner 逻辑改造成可被 Job 消费的执行编排器。

**主要修改：** `trpcservice/execution/`、`trpcservice/session/`、`trpcservice/platform/`、`trpcservice/storage/`。

**交付内容：**

- `ExecutionRequest`、`ExecutionState`、`ExecutionResult` 和 failure 分类。
- Get/Create Session、Acquire/Renew/Release Lease、读取历史和 Memory、执行 Agent、提交事件的明确步骤。
- 执行期间不持有 SQL 长事务；提交时校验 Session version、epoch 和 fencing token。
- `user.received`、`agent.started`、`tool.*`、`assistant.completed/failed` 事件序列。
- Assistant 事件、Audit 和 Outbox 按 Repository 事务边界提交。

**测试与验收：**同 Session 串行、不同 Session 并行、Lease 续租失败取消、旧 fencing token 拒绝、Agent 超时、进程重启接管。执行 `go test ./trpcservice/execution ./trpcservice/session -race`。

### Durable Execution：事实源和异步基础设施

#### P0-09 PostgreSQL 业务 Repository 和 Unit of Work

**前置：** P0-03、P0-04、P0-05、P0-06、P0-08。

**主要修改：** `trpcservice/storage/postgres/repository/` 或与现有包一致的子包、`trpcservice/storage/`。

**交付内容：**

- Tenant/Agent/Binding/Identity/Session/Event/Memory/Summary/Artifact/Audit/Outbox Repository。
- `TenantContext` 二次校验、所有 SQL 的 tenant 条件和跨租户拒绝测试。
- Session CAS/行锁、事件序号、同事务 assistant + audit + outbox。
- 生产连接池、超时、错误分类和事务 rollback。
- 根据 BackendPolicy 组装 Repository；主进程不再隐式使用 MemoryStore。

**测试与验收：**重复写、唯一约束、事件顺序、CAS 冲突、事务回滚、租户隔离、数据库重连。执行真实 PostgreSQL 测试和 `-race`；迁移后运行 `go test ./...`。

**不做：** RLS 的完整启用、备份自动化和向量 Repository，分别在 P2-02/P2-03 实施。

#### P0-10 Agent Job Queue、Gateway 和 Worker

**前置：** P0-06、P0-08、P0-09。

**主要修改：** `trpcservice/gateway/`、`trpcservice/worker/`、`trpcservice/queue/`、`cmd/trpc-service/main.go`。

**交付内容：**

- `AgentJob` 包含 tenant/agent/binding/session/message/request/execution/trace、attempt 和 deadline。
- Gateway 只做入口解析、认证/验签、Claim、Job 入队和快速 ACK。
- Worker consumer 并发、优雅退出、可见性超时、Job 重试和执行编排。
- Session 分区或 Lease 竞争策略；不同 Session 可并行。
- 队列不可用时写 PostgreSQL Outbox，恢复后补投。

**测试与验收：**快速 ACK、ACK 后重启恢复、同 Session 串行、不同 Session 并行、重复 Job、Lease takeover、SIGTERM drain、Context deadline。执行 `go test ./trpcservice/gateway ./trpcservice/worker ./trpcservice/queue -race`。

#### P0-11 Outbox Dispatcher、Retry 和 DLQ

**前置：** P0-09、P0-10。

**主要修改：** `trpcservice/outbox/`、`trpcservice/retry/`、`trpcservice/storage/`。

**交付内容：**

- `ClaimBatch` 使用 `SKIP LOCKED` 或等价租约，租约超时可恢复。
- Reply、Memory Index、Audit Export、Job Requeue 等 kind 的 Dispatcher。
- 可配置的有限指数退避、可重试/不可重试错误分类、最大尝试次数。
- Dead Letter 记录原始 outbox、原因、最后错误和人工重放入口。
- provider dedup key 和发送不确定状态处理。

**测试与验收：**多 Dispatcher 竞争、重启恢复、429/5xx、永久错误 DLQ、同一 outbox 不重复执行、重放幂等。执行 `go test ./trpcservice/outbox ./trpcservice/retry -race`。

### Channels & API

#### P1-01 企业微信 Adapter

**前置：** P0-10、P0-11。

实现回调 challenge、签名计算、AES 加解密、消息类型/群聊解析、MsgID 去重、Binding/Identity 映射、异步发送、长度切分和 429/5xx 重试。新增 `trpcservice/channels/wecom/` 和 `trpcservice/identity/`，禁止信任 payload tenant ID。验收覆盖重放、错误签名、密文、图片/文件、群聊和快速 ACK。

#### P1-02 Telegram Adapter

**前置：** P0-10、P0-11。

实现 secret token、`update_id` 去重、private/group/supergroup/topic Session scope、Bot API sender、`retry_after`、400/403、消息长度和文件下载取消。新增 `trpcservice/channels/telegram/`，使用 numeric user/chat ID，不使用 username 作为主身份。

#### P1-03 Web Chat、SSE 和 API 鉴权

**前置：** P0-09、P0-10、P1-05。

实现 API key/JWT 或企业统一认证的 `Authenticator`、Principal 与 TenantContext，替换当前未经认证的最小路由；支持 body/timeout/并发配额、SSE Runner event、断开取消、分页审计和管理 API 基础鉴权。不得信任 `X-Tenant-ID`，不得开放未认证的租户创建。

#### P1-04 Channel Binding 与 Identity 管理

**前置：** P0-09、P1-01 或 P1-02。

实现 Binding CRUD、Secret Manager 引用、用户/群身份映射、启停和 webhook external app 路由；所有变更写 Audit。覆盖同一外部 App 跨租户拒绝、身份首次创建、禁用 Binding、跨群 Session 隔离。

### Governance & Ops

#### P1-05 Tool Policy、Guardrail 和审计脱敏

**前置：** P0-07、P0-09、P1-03。

新增 `trpcservice/governance/`，实现 Tool 白名单、参数 schema、用户权限、审批 token、预算检查、输入/输出 Guardrail 和统一 Redactor。危险工具默认 deny；测试 Prompt、Tool 参数、错误和 Trace 属性均不能泄露凭据。

#### P1-06 Memory、Summary 和 Vector Index

**前置：** P0-09、P0-11。

实现 Memory/ Summary Repository、Embedding Provider、Vector Repository 和异步 Indexer。SQL 原文先提交，向量写入通过 Outbox；搜索 filter 由服务端附加 tenant/scope；向量不可用时回退 SQL 最近记忆。覆盖版本覆盖、跨租户过滤、向量失败重试和写后最终可见。

#### P1-07 OTel、Metrics 和结构化日志

**前置：** P0-08、P0-10、P0-11。

新增 `trpcservice/telemetry/`，统一传播 trace/request/message/execution/outbox ID，接入模型、Tool、存储和发送 Span；增加低基数指标、成本估算、Redactor 和采样。测试取消后 Span 结束、属性过滤和跨进程 carrier。

#### P1-08 配置发布、灰度和回滚

**前置：** P0-09、P1-03、P1-05。

使用 `tenant_config_version`、`agent_release` 实现校验、immutable 发布、租户/Binding 灰度、健康门禁和回滚指针。Session 创建时固定 Agent version；历史版本不可删除，回滚不改变已发生事件。

#### P1-09 Docker Compose 和运行文档

**前置：** P0-09、P0-10、P1-06、P1-07。

新增 `deploy/compose/`、`.env.example`、readiness/liveness、依赖健康检查、资源限制、日志和 OTEL 配置。验收要求干净环境启动、自动/显式迁移、重启恢复、`/healthz`、Web/IM fake 端到端和无默认生产密钥。

### Resilience Release

#### P2-01 RLS、备份、恢复和事件回放

**前置：** P0-09、P1-06、P1-08。

在应用 tenant 过滤之外逐步启用 PostgreSQL RLS；提供 schema/data 备份校验、恢复演练、按租户/Session 的事件回放、Summary/Vector 重建和 checksum 报告。测试断点续传、幂等、校验失败中止，不删除源数据。

#### P2-02 故障恢复、断路器和容量保护

**前置：** P0-10、P0-11、P1-01、P1-02、P1-07。

统一模型/Tool/IM/DB/Redis failure class、断路器、租户级并发/token/cost/发送配额、DrainController 和容量告警。测试重试风暴、依赖分区、节点失效、优雅退出和 DLQ 告警。

#### P2-03 集成、压力和安全测试

**前置：** P1 全部完成。

新增 `tests/integration/`、`tests/e2e/`、`tests/load/`、`tests/security/`，提供 Fake IM、Fake Model、FaultInjector、TenantFixture。必须覆盖跨租户拒绝、重复消息单执行、同 Session 串行、乱序、Memory 可见性、Trace/Audit 完整性、签名重放、Secret 脱敏和依赖故障。压力测试固定峰值、P95/P99、错误率、租户公平性和成本预算。

#### P2-04 发布检查和生产运维手册

**前置：** P2-01、P2-02、P2-03。

输出部署拓扑、配置清单、迁移前检查、回滚、密钥轮换、备份恢复、DLQ 重放、告警阈值、容量公式、值班操作和已知限制。建立发布门禁：全量 test/vet/race、镜像扫描、迁移 checksum、灾备恢复报告、压测报告和安全测试报告齐全后才能标记首个生产版本。

## 5. 依赖和建议顺序

```text
P0-07 -> P0-08 -> P0-10
P0-09 -> P0-08, P0-10, P0-11
P0-10 -> P1-01, P1-02, P1-03
P0-11 -> P1-01, P1-02, P1-06
P1-03 -> P1-04, P1-05, P1-08
P0-09 + P1-06 -> P2-01
P0-10 + P0-11 + P1-07 -> P2-02
P1 全部 -> P2-03 -> P2-04
```

推荐执行顺序：`P0-07 -> P0-08/P0-09 -> P0-10 -> P0-11 -> P1-01/P1-02/P1-03 -> P1-04/P1-05/P1-06/P1-07 -> P1-08 -> P1-09 -> P2-01/P2-02 -> P2-03 -> P2-04`。

可并行的任务必须共享稳定契约后再启动；例如 P1-01 和 P1-02 可以并行，但都依赖 Gateway/Worker/Outbox 的统一消息合同。每项任务完成时同步更新本文件的状态、验收命令和风险，不再通过单独的临时 `plan006*` 文件维护隐含状态。

## 6. 首个生产版本验收

1. `go test ./...`、`go vet ./...`、格式和 lint 通过；关键包 race test 通过。
2. Web、企业微信、Telegram 都能从入口完成鉴权/验签、Binding、TenantContext、Claim、Job、Runner、事件、Audit、Outbox 和回复。
3. 重复入站消息只产生一次 Agent 执行；同 Session 无重复 sequence、状态覆盖或旧 fencing token 提交。
4. Worker 节点可横向扩展；节点中止后 Job 能接管，Context 和 goroutine 没有泄漏。
5. PostgreSQL、Redis、向量库、对象存储职责清晰；向量和缓存故障不会造成跨租户读或事实源丢失。
6. Tool 权限、审批、预算、Secret 脱敏和审计字段可被测试证明。
7. `trace_id`、`request_id`、`message_id`、`execution_id`、`outbox_id` 可从入口追踪到外部回复或 DLQ。
8. Compose 可在干净环境启动并完成 readiness；备份恢复、事件回放、灰度回滚和容量/安全报告齐全。
9. 未实现或关闭的能力在 API、配置和运维文档中明确返回/呈现，不以静默降级冒充生产可用。

## 7. 每个任务的完成定义

- 代码、迁移和配置变更只落在任务声明的边界内。
- 领域契约和错误语义有单测；并发/协调代码有 race 或真实依赖测试。
- 至少有一个失败、取消、重试或恢复路径测试，而不是只测 happy path。
- 所有入口和 Repository 都验证租户边界，日志和 Trace 不含 Secret。
- 文档同步更新运行前置、验收命令、回滚方式和已知限制。
- `go test ./...`、`go vet ./...`、格式/lint 以及受影响集成测试通过后，才将任务标记为完成。
