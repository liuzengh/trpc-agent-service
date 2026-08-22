# tRPC-Agent-Go 多租户节点化平台实施计划

## 目标与范围

将当前 Go 骨架实现为可部署的多租户 Agent 平台：以 tRPC-Agent-Go 作为 Worker 内部 Agent 执行内核，平台层负责租户、AgentApp、ChannelBinding、Gateway、Worker、Session、Memory、Tool 治理、审计、Outbox 和部署。首个可用版本支持 Web 与企业微信，Telegram 紧随其后；数据后端基线为 PostgreSQL、Redis 和一个向量库；对象存储用于文件与 Artifact。

平台必须支持：无状态 Worker 水平扩展、共享 Session/Memory、企业微信和 Telegram Adapter、租户级 Agent/Tool/Backend 配置、原子幂等、同 Session 串行化、事件顺序、Context 取消、Outbox 重试/DLQ、审计与 OpenTelemetry。

当前 `EchoResponder` 只保留为本地联调模式；生产执行必须通过 tRPC-Agent-Go Runner Adapter。当前内存存储只作为测试和开发 fallback，不能用于生产。

## 固定架构决策

```text
Web / 企业微信 / Telegram
          |
     Agent Gateway
验签、身份、租户映射、幂等、快速 ACK
          |
       AgentJob Queue
          |
       Agent Worker
          |
 tRPC-Agent-Go Runner
Agent / Tool / MCP / Guardrail
          |
 Session / Memory / Summary / Artifact
          |
 PostgreSQL + Redis + Vector DB + Object Storage
          |
 Outbox Dispatcher + OTel Collector
```

- Gateway 不执行模型和 Tool，只负责验证、解析、幂等 Claim、投递 Job 和快速 ACK。
- Worker 无状态；不依赖 sticky session；同一 Session 通过队列分区或 Redis lease 串行化。
- 所有领域表、缓存 key、对象存储 key、向量 metadata 和 Repository 查询都强制包含 `tenant_id`。
- `TenantContext` 在验签和 Binding 解析后创建，并贯穿 Gateway、Job、Runner、Tool、Store、Audit、Outbox 和 IM 发送。
- `session_id = base64url(SHA-256("v1|" + tenant_id + "|" + channel + "|" + binding_id + "|" + scope))`；私聊 scope 为用户，群聊 scope 为群，Telegram Forum Topic 额外加入 `message_thread_id`。
- 企业微信使用 token/timestamp/nonce/signature 与 AES 回调处理；Telegram 使用 webhook secret token、`update_id` 幂等和 Bot API。
- PostgreSQL 是租户、配置、Session、Event、Memory 原文、Summary、Artifact 元数据、Audit 和 Outbox 的事实源；Redis 用于幂等、lease、fencing、限流、热缓存和队列；向量库保存 embedding 与受租户过滤的索引；对象存储保存文件。
- Agent 执行不持有数据库长事务；执行期间持有可续租的 Session lease。lease 续租失败必须取消 Context，旧 Worker 使用 fencing token 不能提交结果。
- Outbox 统一承载 AgentJob、IM 回复、Memory 索引和审计投递；有限指数退避后进入 DLQ。

## 领域模型与接口

新增或稳定以下模型：

- `Tenant`：`tenant_id`、名称、状态、配置版本、默认 Agent、BackendPolicy、预算。
- `AgentApp`：`tenant_id`、Agent ID、版本、状态、模型配置引用、SystemPrompt、ToolPolicy、Guardrail 引用。
- `ChannelBinding`：租户、Binding ID、channel、external_app_id/Bot ID、Secret Manager 引用、启用状态。
- `Session`：租户、Session ID、Agent 版本、Channel、外部会话、state、state_version、summary_version、last_event_seq。
- `SessionEvent`：租户、Session、Seq、event_id、event_type、message_id、execution_id、parent_event_id、attempt、trace_id、脱敏 Payload。
- `Memory`：租户、Memory ID、Session/User scope、kind、content、vector_ref、version、source_seq、删除状态。
- `Summary`：租户、Session、version、covered_seq、内容和 token 估算。
- `Artifact`：租户、Artifact ID、Session/message、object_key、媒体类型、大小、SHA256、状态和过期时间。
- `AuditLog`：租户、audit ID、trace/request/execution ID、Channel、用户、Session、Agent、Tool、decision、latency、cost、error type、脱敏 metadata。
- `TenantContext`：`TenantID`、`AgentAppID`、`BindingID`、Channel、外部/内部用户、SessionID、RequestID、MessageID、TraceID、权限和 BackendPolicy。

固定接口：

```go
type TenantResolver interface {
    Resolve(ctx context.Context, channel, externalAppID string) (TenantContext, error)
}

type SessionRepository interface {
    Get(ctx context.Context, tc TenantContext, id string) (Session, error)
    AppendEvent(ctx context.Context, tc TenantContext, expectedVersion int64, e SessionEvent) (Session, error)
    AcquireLease(ctx context.Context, tc TenantContext, id string, ttl time.Duration) (Lease, error)
}

type IdempotencyRepository interface {
    Claim(ctx context.Context, tc TenantContext, externalMessageID string, ttl time.Duration) (Claim, error)
    Complete(ctx context.Context, tc TenantContext, key, responseRef string) error
}

type MemoryRepository interface {
    Put(ctx context.Context, tc TenantContext, m Memory) error
    Search(ctx context.Context, tc TenantContext, query string, limit int) ([]Memory, error)
}

type SummaryRepository interface {
    UpsertIfNewer(ctx context.Context, tc TenantContext, s Summary) error
}

type ArtifactRepository interface {
    Create(ctx context.Context, tc TenantContext, a Artifact) error
    PresignedURL(ctx context.Context, tc TenantContext, id string, ttl time.Duration) (string, error)
}

type AuditRepository interface {
    Append(ctx context.Context, tc TenantContext, a AuditLog) error
}

type AgentRuntime interface {
    Run(ctx context.Context, tc TenantContext, s Session, input string) (AgentResult, error)
}
```

Repository 实现必须在内部再次校验 `tc.TenantID`，不能只依赖调用方；Vector Search 接口不可允许调用方覆盖 `tenant_id` filter；Artifact 对象 key 固定为 `tenants/{tenant_id}/...`。

## PostgreSQL 迁移

实现以下表和索引：

- `tenant`
- `agent_app`
- `channel_binding`
- `user_identity`
- `session`
- `session_event`
- `message_dedup`
- `memory`
- `summary`
- `artifact`
- `audit_log`
- `outbox_message`
- `dead_letter`
- `tenant_config_version` / `agent_release`

关键唯一约束：

```text
channel_binding(channel, external_app_id)
session(tenant_id, session_id)
session_event(tenant_id, session_id, seq)
session_event(tenant_id, event_id)
session_event(tenant_id, session_id, message_id, event_type)
message_dedup(tenant_id, channel, binding_id, external_message_id)
summary(tenant_id, session_id)
artifact(tenant_id, artifact_id)
audit_log(tenant_id, audit_id)
outbox_message(tenant_id, id)
```

Session 事件、状态版本和 assistant message 在有限 PostgreSQL 事务中提交。所有迁移可重复执行，并提供迁移版本、回滚策略和生产前备份验证。生产逐步启用 PostgreSQL Row-Level Security，应用层租户过滤作为第一层，数据库策略作为第二层。

## 实施任务与提交边界

### P0-01：依赖、CI 和质量基线，0.5 天

- 目标：固定 Go/tRPC-Agent-Go/Redis/PostgreSQL/OTel 版本，接入 test、vet、format、lint。
- 前置依赖：无。
- 修改目录：`go.mod`、`go.sum`、`.github/workflows/`、`lint.sh`、`coverage.sh`。
- 接口/结构：无业务接口。
- 测试：CI 执行 `go test ./...`、`go vet ./...`、`gofmt -l .`。
- 验收：`go test ./... && go vet ./... && test -z "$(gofmt -l .)"`。
- 不修改：业务逻辑、数据库结构、HTTP 路由。
- 风险：框架 API/Go 版本不兼容；回滚为恢复 `go.mod/go.sum` 和 CI。

### P0-02：租户领域模型和 TenantContext，1 天

- 前置：P0-01。
- 修改：`trpcservice/tenant/`、`trpcservice/config/`、`trpcservice/platform/`。
- 接口：`Tenant`、`AgentApp`、`ChannelBinding`、`TenantContext`、`TenantResolver`、`BackendPolicy`。
- 测试：状态、配置版本、Binding 映射、Context 错配和跨租户拒绝。
- 验收：`go test ./trpcservice/tenant ./trpcservice/config ./trpcservice/platform -race`。
- 不修改：真实数据库、IM 协议和 Runner。
- 风险/回滚：防止信任请求租户 ID；删除新 Resolver 即可回退内存租户。

### P0-03：Session/Event/Memory/Summary/Artifact/Audit 模型，1 天

- 前置：P0-02。
- 修改：`trpcservice/session/`、`memory/`、`artifact/`、`audit/`。
- 接口：领域实体、事件类型、状态枚举、版本字段。
- 测试：字段校验、状态迁移、事件合法性、版本递增、审计脱敏。
- 验收：`go test ./trpcservice/session ./trpcservice/memory ./trpcservice/artifact ./trpcservice/audit`。
- 不修改：具体数据库、Redis、IM Adapter。
- 风险/回滚：避免泄露框架内部类型；删除新增领域包并保留兼容 Message。

### P0-04：ID、幂等键和存储契约，1 天

- 前置：P0-02、P0-03。
- 修改：`trpcservice/session/`、`storage/`、`platform/`。
- 接口：`SessionKey`、`DedupKey`、`IdempotencyRepository`、`SessionRepository`、`MemoryRepository`、`OutboxRepository`。
- 测试：ID 稳定性、租户/Binding/Topic 隔离、空值和长度校验。
- 验收：`go test ./trpcservice/session ./trpcservice/storage -race`。
- 不修改：Web 字段名、具体 Redis/SQL 实现。
- 风险/回滚：ID 规则变化导致历史数据不可读；采用 `v1` 版本前缀，保留旧算法。

### P0-05：PostgreSQL Schema 和迁移，1 天

- 前置：P0-02、P0-03、P0-04。
- 修改：`migrations/`、`trpcservice/storage/postgres/`、`scripts/`。
- 接口：`Migrator`、`PostgresConfig`、`MigrationVersion`。
- 测试：重复迁移、唯一约束、租户条件、事务回滚。
- 验收：`docker compose -f deploy/compose.test.yml up -d postgres && go test ./trpcservice/storage/postgres -count=1`。
- 不修改：生产数据迁移和 Agent 逻辑。
- 风险/回滚：锁表和错误外键；每个迁移提供可控 down/forward 方案，生产执行前备份。

### P0-06：Redis 幂等、Lease、Fencing 和限流，1.5 天

- 前置：P0-04、P0-05。
- 修改：`storage/redis/`、`queue/`、`ratelimit/`。
- 接口：`RedisIdempotencyRepository`、`Lease`、`FenceToken`、`RateLimiter`、`ClaimStatus`。
- 测试：100 goroutine 竞争同一消息、lease 接管、旧 token 拒绝、限流窗口。
- 验收：`docker compose -f deploy/compose.test.yml up -d redis && go test ./trpcservice/storage/redis ./trpcservice/ratelimit -race`。
- 不修改：模型和 IM 逻辑。
- 风险/回滚：Redis 分区导致重复执行；切回 PostgreSQL Claim，保留幂等记录。

### P0-07：tRPC-Agent-Go Runner Adapter，2 天

- 前置：P0-01、P0-02、P0-03、P0-04。
- 修改：`trpcservice/agent/`、`tool/`、`platform/`。
- 接口：`AgentFactory`、`AgentRuntime`、`AgentInput`、`AgentResult`、`RunnerEvent`。
- 测试：成功、模型错误、Context 取消、Event channel 排空、租户 Agent 隔离。
- 验收：`go test ./trpcservice/agent ./trpcservice/tool ./trpcservice/platform -race`。
- 不修改：IM webhook、生产密钥存储。
- 风险/回滚：Runner goroutine 泄漏或依赖 API 变化；保留 `MODEL_PROVIDER=echo` 本地模式。

### P0-08：Gateway、AgentJob 和 Worker，2 天

- 前置：P0-04、P0-06、P0-07。
- 修改：`gateway/`、`worker/`、`queue/`、`web/`。
- 接口：`AgentJob`、`JobQueue`、`GatewayHandler`、`Worker`、`ExecutionState`。
- 测试：快速 ACK、Context/Trace 恢复、同 Session 串行、不同 Session 并行、崩溃接管。
- 验收：`go test ./trpcservice/gateway ./trpcservice/worker ./trpcservice/queue -race`。
- 不修改：复杂管理后台和具体 IM 协议。
- 风险/回滚：ACK 后 Job 丢失；队列不可用时写 Job Outbox，临时可回退同步模式。

### P0-09：Outbox、发送 Dispatcher、Retry 和 DLQ，1.5 天

- 前置：P0-05、P0-06、P0-08。
- 修改：`outbox/`、`queue/`、`audit/`。
- 接口：`OutboxMessage`、`OutboxDispatcher`、`RetryPolicy`、`DeadLetterRecord`。
- 测试：SKIP LOCKED 竞争、退避、永久错误 DLQ、重启恢复、同一 Outbox 不重复发送。
- 验收：`go test ./trpcservice/outbox ./trpcservice/queue -race`。
- 不修改：具体企业微信/Telegram API。
- 风险/回滚：外部超时但实际已发送；暂停 Dispatcher，保留 pending/retry。

### P1-01：企业微信 Adapter，2 天

- 前置：P0-02、P0-04、P0-08、P0-09。
- 修改：`channels/wecom/`、`identity/`。
- 接口：`WeComAdapter`、`WeComBinding`、`WeComSender`、`IdentityResolver`。
- 测试：challenge、签名、时间戳重放、AES、XML/JSON、文本/图片/群聊、MsgID 幂等、长消息和 429/5xx。
- 验收：`go test ./trpcservice/channels/wecom ./trpcservice/identity -race`。
- 不修改：租户核心表、TenantContext、日志密钥。
- 风险/回滚：协议差异和回调超时；按 Binding 禁用并保留 Outbox。

### P1-02：Telegram Adapter，2 天

- 前置：P0-02、P0-04、P0-08、P0-09。
- 修改：`channels/telegram/`、`identity/`、`artifact/`。
- 接口：`TelegramAdapter`、`TelegramUpdate`、`TelegramBinding`、`TelegramSender`、`MessagePart`。
- 测试：secret token、update_id 幂等、private/group/supergroup/topic、429 retry_after、400/403、长消息、文件下载取消。
- 验收：`go test ./trpcservice/channels/telegram ./trpcservice/identity ./trpcservice/artifact -race`。
- 不修改：企业微信 XML/AES 解析，不使用 username 作为主身份。
- 风险/回滚：Bot API 限频、Markdown 转义、发送不确定性；停用 Binding 并保留 Outbox。

### P1-03：Web Chat、SSE 和 API 鉴权，1.5 天

- 前置：P0-02、P0-08、P0-09。
- 修改：`web/`、`auth/`、`tenant/`。
- 接口：`Authenticator`、`Principal`、`ChatRequest`、`SSEEvent`、`AdminService`。
- 测试：认证、租户越权、SSE 断开取消、重复 request_id、body 限制、分页。
- 验收：`go test ./trpcservice/web ./trpcservice/auth -race`。
- 不修改：不信任 `X-Tenant-ID`，不开放未认证租户创建。
- 风险/回滚：连接泄漏和 Session 泄露；关闭管理/SSE 路由。

### P1-04：PostgreSQL Repository 和事务一致性，2 天

- 前置：P0-03、P0-05、P0-08、P0-09。
- 修改：`storage/postgres/`、`session/`、`audit/`。
- 接口：实现 Session/Memory/Audit/Outbox Repository，新增 `UnitOfWork`。
- 测试：CAS、行锁、事件 Seq、租户过滤、事务回滚、同事务 Outbox。
- 验收：`go test ./trpcservice/storage/postgres ./trpcservice/session ./trpcservice/audit -count=1`。
- 不修改：领域 ID、Channel 逻辑、破坏性迁移。
- 风险/回滚：长事务和遗漏租户条件；切回 Memory/Redis，保留数据。

### P1-05：Memory、Summary 和向量索引，2 天

- 前置：P0-05、P0-09、P1-04、embedding Provider。
- 修改：`memory/`、`knowledge/`、`storage/vector/`。
- 接口：`EmbeddingProvider`、`VectorRepository`、`MemoryIndexer`、`SearchFilter`。
- 测试：数据库成功/向量失败、重试、版本覆盖、跨租户过滤、写入后可见。
- 验收：`go test ./trpcservice/memory ./trpcservice/knowledge ./trpcservice/storage/vector -race`。
- 不修改：向量库不能成为原文事实源，不能覆盖租户 filter。
- 风险/回滚：最终一致和 embedding 成本；关闭向量检索，使用 PostgreSQL 最近记忆。

### P1-06：Tool Policy、Guardrail 和审计脱敏，1.5 天

- 前置：P0-02、P0-07、P1-04。
- 修改：`tool/`、`governance/`、`log/`、`audit/`。
- 接口：`ToolPolicy`、`ToolAuthorizer`、`ApprovalService`、`Guardrail`、`Redactor`。
- 测试：工具越权、审批、凭据隔离、Prompt/日志/Trace 脱敏、超时取消。
- 验收：`go test ./trpcservice/tool ./trpcservice/governance ./trpcservice/log ./trpcservice/audit -race`。
- 不修改：不执行任意 Shell，不把密钥写入 Prompt。
- 风险/回滚：工具越权和提示注入；默认关闭危险 Tool，只保留只读白名单。

### P1-07：OpenTelemetry、Metrics 和结构化日志，1.5 天

- 前置：P0-08、P0-09、P1-04。
- 修改：`metrics/`、`telemetry/`、`log/`。
- 接口：`TracerProvider`、`MetricsRecorder`、`TraceCarrier`、`AuditEnricher`。
- 测试：trace/request/message ID 跨进程、敏感属性过滤、取消后 span 结束、低基数指标。
- 验收：`go test ./trpcservice/metrics ./trpcservice/telemetry ./trpcservice/log -race`。
- 不修改：不记录完整 Prompt、Key、Authorization 和文件内容。
- 风险/回滚：高基数标签和 Collector 压力；关闭 exporter，保留审计和错误日志。

### P2-01：对象存储和 Artifact 生命周期，1.5 天

- 前置：P0-03、P1-01 或 P1-02、P1-04。
- 修改：`artifact/`、`storage/object/`、清理 Job。
- 接口：`ObjectStore`、`ArtifactService`、`PresignRequest`、`RetentionPolicy`。
- 测试：跨租户 URL、hash、大小/MIME、过期清理、下载取消。
- 验收：`go test ./trpcservice/artifact ./trpcservice/storage/object -race`。
- 不修改：不把二进制放 PostgreSQL，不生成永久公开 URL。
- 风险/回滚：恶意文件和误删；禁用文件消息，保留对象和元数据。

### P2-02：配置发布、灰度和回滚，1.5 天

- 前置：P0-02、P0-05、P1-03、P1-06。
- 修改：`tenant/`、`config/`、`web/`。
- 接口：`ConfigVersion`、`Release`、`RolloutRule`、`RollbackService`。
- 测试：非法配置、稳定灰度、回滚、新旧 Session Agent 版本隔离。
- 验收：`go test ./trpcservice/tenant ./trpcservice/config ./trpcservice/web -race`。
- 不修改：不修改运行中 Session 的 Agent 版本，不删除历史版本。
- 风险/回滚：灰度不稳定；切回 Release 指针，保留事件和审计。

### P2-03：故障恢复、断路器和容量保护，2 天

- 前置：P0-06、P0-08、P0-09、P1-01、P1-02。
- 修改：`retry/`、`worker/`、`queue/`、`config/`。
- 接口：`RetryPolicy`、`CircuitBreaker`、`QuotaManager`、`DrainController`、`FailureClass`。
- 测试：429、模型超时、DB/Redis 断开、SIGTERM、队列重投、DLQ、租户配额。
- 验收：`go test ./trpcservice/retry ./trpcservice/worker ./trpcservice/queue -race`。
- 不修改：不改变事件语义，不无限重试，不删除 DLQ。
- 风险/回滚：重试风暴和重复外部发送；关闭自动重试并保留 DLQ。

### P2-04：Docker Compose 和部署清单，1 天

- 前置：P0-05、P0-06、P1-04、P1-07。
- 修改：`deploy/compose/`、`deploy/nginx/`、`.env.example`、部署文档。
- 接口：健康检查和配置 Schema。
- 测试：干净启动、迁移、重启、readiness、依赖不可用处理。
- 验收：`docker compose -f deploy/compose/docker-compose.yml up -d && curl -fsS http://localhost:8080/healthz && go test ./...`。
- 不修改：真实密钥、生产数据和外部端口默认暴露。
- 风险/回滚：默认密码、资源限制、依赖启动顺序；停止新 Compose，使用旧脚本。

### P2-05：迁移、备份、恢复和事件回放，2 天

- 前置：P0-05、P1-04、P1-05。
- 修改：`cmd/migrate/`、`cmd/replay/`、`trpcservice/migration/`、运维文档。
- 接口：`MigrationJob`、`ReplayCursor`、`ChecksumReport`、`RestorePlan`。
- 测试：断点续传、幂等、租户分片、事件回放一致、向量重建、校验失败中止。
- 验收：`go test ./trpcservice/migration ./cmd/migrate ./cmd/replay -race`。
- 不修改：不删除源数据，不执行无确认生产迁移。
- 风险/回滚：双写不一致和误迁移；保留源后端与 checkpoint，切回旧读路径。

### P2-06：完整集成、压力、安全和容灾测试，2 天

- 前置：P1 全部任务和 P2 部署任务。
- 修改：`tests/integration/`、`tests/e2e/`、`tests/load/`、`tests/security/`、`testdata/`。
- 接口：Test Fixture、Fake IM、Fake Model、FaultInjector、TenantFixture。
- 测试：跨租户拒绝、重复消息单执行、同 Session 串行、乱序拒绝、Memory 可见、审计不丢、故障恢复。
- 验收：`go test ./tests/... -race -count=1`，压力测试使用约定的 benchmark 或 k6 命令。
- 不修改：不连接生产，不写真实租户数据，不跳过签名和权限。
- 风险/回滚：测试环境差异和外部服务费用；销毁测试资源，保留报告和失败样本。

## 端到端验收标准

1. `go test ./...`, `go vet ./...` 和竞态测试通过。
2. 企业微信 webhook 能完成验签、租户/Agent/User/Channel 映射、原子去重和快速 ACK。
3. Telegram Adapter 能完成 secret token、`update_id` 去重、群聊/Topic Session、长消息、文件和 Bot API 重试。
4. 同一 Session 的并发消息无状态覆盖、无重复 Seq、无非法事件迁移。
5. tRPC-Agent-Go Runner 能响应 `context.Context` 取消，Tool、模型和 IM 失败都有明确状态、重试或 DLQ。
6. Redis、PostgreSQL、向量库和对象存储分别承担明确职责，租户隔离通过应用层和数据库层双重验证。
7. trace_id、request_id、message_id、execution_id 和 outbox_id 可从入口追踪到最终回复。
8. Docker Compose 可启动最小环境，生产部署具备健康检查、资源限制、备份、回滚和告警方案。

## 明确不在首个生产版本的范围

- 微信公众号、微信客服和其他未明确要求的 IM 通道。
- 多 Region 强一致部署。
- 任意代码执行型 Tool；危险 Tool 必须后置到审批和沙箱阶段。
- 不受控的 Agent 自主修改租户配置。
- 将 Redis 或向量库作为唯一业务事实源。
- 在未完成安全测试前开放公网未认证管理接口。

## 推荐实施顺序

按 `P0-01 -> P0-02 -> P0-03 -> P0-04 -> P0-05 -> P0-06 -> P0-07 -> P0-08 -> P0-09` 顺序提交；P1-01、P1-02、P1-03 在 P0-08 后并行；P1-04 完成后实现 P1-05；P1-06 和 P1-07 可并行；最后执行 P2。每个任务独立提交、独立测试、独立回滚，禁止把所有模块合并成一个大提交。
