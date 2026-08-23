# P0-06 Redis Claim、Lease、Fencing 与限流实施计划（基于 docs/plan006v1）

## 1. 当前代码状态

- P0-01 至 P0-05 已完成，当前仓库已有：
  - `tenant.TenantContext`、Binding/Session 等领域模型和租户校验。
  - `trpcservice/storage` 中的 `Claim`、`Lease`、`FenceToken`、`ClaimStatus`、`SessionRepository` 等基础契约。
  - `FakeRepository` 已覆盖部分 Claim、Session lease、fencing 和 100 并发 Claim 行为。
  - P0-05 PostgreSQL migration、`message_dedup` 表、租户限定约束、migration runner、checksum 和显式测试 DSN 测试。
- 当前没有 Redis 实现、Redis 客户端依赖、Redis key 规范、Lua 脚本、Redis 故障切换层或限流包。
- 当前没有 PostgreSQL 真实 Repository；PostgreSQL 目前主要提供 Schema 和 migration runner。
- `message_dedup` 已具备 Claim fallback 所需的状态、owner、attempt、`fence_token`、过期时间和响应引用字段。
- 当前没有 PostgreSQL Session lease 表，因此 PostgreSQL fallback 的 Session lease/fencing 需要新增 migration。
- Agent、IM、模型执行链路尚未实现，P0-06 不应修改这些逻辑。

## 2. 需求与现有代码的差距

1. 没有 Redis 原子 Claim，无法保证跨进程 100 个并发请求只有一个 owner。
2. 没有 Claim lease，无法在 owner 崩溃或 lease 过期后安全接管。
3. 没有 Redis Session lease 的 Acquire/Renew/Release 实现。
4. 没有单调递增 fencing token，也没有用 token 拒绝旧 Worker 提交结果的原子脚本。
5. 没有 lease 续租 goroutine、续租失败后的派生 Context cancel 机制。
6. 没有 tenant、Binding、Chat 三个维度的限流接口和原子窗口算法。
7. Redis 故障时没有 fallback 路由。
8. PostgreSQL fallback 目前只能依赖新增 Repository，不能直接复用内存 Fake 作为生产 fallback。
9. P0-05 测试主要验证 Schema，尚未全面验证 PostgreSQL fallback 的 Claim、lease、fencing、过期接管、租户隔离、并发、事务回滚和故障切换。

明确决策：

- Redis 正常时作为高吞吐协调层。
- Redis 故障时，Claim 和 Session lease/fencing 切换到 PostgreSQL。
- 限流在 Redis 故障时默认 fail closed；可以配置受控本地保护阈值，但本地计数不作为全局限流结果。
- PostgreSQL fallback 不修改 Agent、IM 或模型逻辑，只提供与 Redis 实现等价的基础设施接口。
- Redis 与 PostgreSQL 不做双写强一致；每次操作选择唯一 backend，记录 backend 状态，避免“Redis 成功、PostgreSQL 失败”造成双重 owner。

## 3. 详细实施步骤

### 阶段 A：稳定公共契约与 key 规范

1. 在 `trpcservice/storage` 中明确 Claim、Lease、FenceToken 的跨 backend 语义。
2. 统一版本化、租户限定 key：
   - `tenant:{tenant_id}:dedup:{channel}:{binding_id}:{external_message_id}`
   - `tenant:{tenant_id}:session-lease:{session_id}`
   - `tenant:{tenant_id}:session-fence:{session_id}`
   - `tenant:{tenant_id}:limit:{scope}:{id}:{window}`
3. 对所有外部字段做空值、长度、控制字符、分隔符和编码校验，禁止直接拼接未校验字段。
4. owner ID 必须由调用方传入，稳定且非空；不同 backend 不隐式生成不可关联的 owner。
5. 明确 `Claim`/`Lease` 的 backend、owner、attempt、expiry、fencing token 和 response reference 语义，保证 Redis 与 PostgreSQL 返回结果可比较。

### 阶段 B：Redis Client 与原子脚本

1. 引入并锁定 `github.com/redis/go-redis/v9`，只加入 P0-06 直接需要的依赖。
2. 实现 Redis 配置、连接池、Ping、Close、Dial/Read/Write timeout、Redis 错误分类和 DSN 脱敏。
3. 使用受信任的 Lua 脚本或 `go:embed` 资源，不在运行时拼接 Lua；每个脚本固定声明 KEYS/ARGV。
4. Claim 脚本在一次 EVAL 中完成新建、重复返回、完成返回、未过期冲突和过期接管；接管时原子递增 attempt/fencing token 并刷新 lease TTL。
5. Complete、Fail、Renew、Release 脚本同时比较 owner、fencing token 和 lease 状态；旧 owner/token 只能得到 `ErrFenceRejected` 或 `ErrLeaseLost`，不能修改状态。
6. Session lease 使用 hash + TTL；fencing token 由 Redis `INCR` 生成，不能由 Worker 生成或重置。
7. 过期判断使用 Redis server time/TTL，不依赖 worker 本地时钟。
8. 脚本返回固定结果码，由 Go 层转换为稳定 sentinel errors，不直接把原始 Redis 错误暴露给业务层。

### 阶段 C：Claim、Session lease、续租与 Context cancel

1. 实现 `RedisIdempotencyRepository`：`Claim`、`Complete`、`Fail`。
2. `Claim` 明确返回 `ClaimAcquired`、`ClaimInFlight`、`ClaimCompleted` 和过期 takeover 结果；已完成消息必须返回原 response reference，禁止重复执行。
3. 实现 `RedisLeaseManager`：
   - `Acquire`：同一 tenant/session 只能有一个有效 owner。
   - `Renew`：只允许当前 owner/token 续租。
   - `Release`：只允许当前 owner/token 释放。
   - `Validate`/`CommitGuard`：提交前验证 owner/token/expiry。
4. 实现 `WithLease` 或 `LeaseRunner`：
   - 创建派生 Context 和 cancel function。
   - 按小于 TTL 的 interval 续租。
   - 任意不可恢复续租失败、Redis 连接失败、owner/token 不匹配或被新 owner 接管时 cancel execution Context。
   - 主执行结束后停止 renewal goroutine/timer，再执行带 token 校验的 release。
   - 返回原始执行错误与 `ErrLeaseLost` 的可区分结果。
5. 续租失败后禁止继续调用持久化 commit API；不能把 lease lost 转换为普通成功。
6. Release 和过期接管必须幂等；旧 owner 的 Release 不能删除新 owner 的 lease。

### 阶段 D：PostgreSQL fallback 与新 Schema

1. 新增 `migrations/000002_coordination.up.sql` 和配对 `.down.sql`，新增 `session_lease` 表：
   - `tenant_id`、`session_id`、`owner_id`、`fencing_token`、`leased_until`、`created_at`、`updated_at`。
   - 复合主键 `(tenant_id, session_id)`。
   - 复合外键 `(tenant_id, session_id)` 引用 `session`。
   - `fencing_token >= 0`、`leased_until` 非空等 CHECK。
   - tenant/session/expiry 查询索引。
2. 复用 P0-05 的 `message_dedup`，不重复创建 Claim 表；migration 对已有必要列/约束做 fail-closed 校验，不能使用不兼容的 `IF NOT EXISTS` 掩盖 Schema 漂移。
3. 新增 PostgreSQL Claim fallback：
   - 使用事务和 `INSERT ... ON CONFLICT`/`SELECT ... FOR UPDATE`。
   - 新 Claim、未过期冲突、已完成返回、过期接管必须是单事务原子状态转换。
   - 接管时由 PostgreSQL 原子递增 `fence_token`，不得使用应用内计数器。
4. 新增 PostgreSQL Session lease fallback：
   - 事务内锁定 `(tenant_id, session_id)` 行。
   - 未过期时拒绝其他 owner，过期后允许接管。
   - Acquire、Renew、Release 都比较 owner 和 fencing token。
5. Complete/Fail/提交保护必须使用带租户、资源、owner、fencing token 和 expiry 条件的原子 `UPDATE`，并检查 affected rows。
6. PostgreSQL backend 与 Redis backend 实现相同的 `ClaimStore`/`LeaseStore` 接口，业务层不感知 backend。
7. fallback 规则：
   - Redis 网络错误、连接池耗尽、timeout、明确 unavailable 错误可切 PostgreSQL。
   - 参数错误、fence rejection、业务冲突、脚本协议错误不得 fallback。
   - Redis 恢复后不自动复制短期 lease；新操作按唯一 backend 选择策略运行。
   - backend 切换记录 backend 和失败原因，但禁止记录密码、Token、完整 DSN 和用户消息。
8. 增加 backend 选择器/熔断器，保证 Redis 和 PostgreSQL 不会同时接受同一业务 key；切换期间宁可拒绝，也不能产生双 owner。
9. PostgreSQL fallback 的连接池、事务、锁等待和查询 Context 必须可取消，并设置明确超时。

### 阶段 E：三维度 RateLimiter

1. 实现 tenant、Binding/channel、Chat/session 外部会话三个维度的 `RateLimiter`。
2. 使用 Redis Lua 固定窗口算法；计数增加、窗口 TTL 和结果返回必须一次原子完成。
3. 一次请求同时检查三个维度，任一超限则整体拒绝。
4. 所有维度 key 必须包含 `tenant_id`；相同 Binding/Chat ID 在不同租户间必须隔离。
5. 窗口边界使用 Redis server time，不依赖 worker 本地时钟。
6. Redis unavailable 时返回 `ErrRateLimitBackendUnavailable`，默认 fail closed，不使用未经协调的本地计数冒充全局结果。

### 阶段 F：可观测性和边界

1. 定义 Redis unavailable、lease lost、fence rejected、already claimed、rate limited、fallback active 等错误分类。
2. 增加 backend、fallback 原因、lease loss 和 rate limit 拒绝的结构化日志/指标接口，但不输出密码、Token、完整 DSN 或原始外部消息。
3. 不新增 Agent Runner、IM sender、Gateway、Worker 或模型逻辑。
4. 不实现 P0-07/P0-08/P0-09 的业务编排，只提供后续 Worker/Gateway 可调用的基础设施接口。

## 4. Go 接口和数据结构

```go
type ClaimStore interface {
    Claim(context.Context, tenant.TenantContext, DedupKey, time.Duration, string) (Claim, error)
    Complete(context.Context, tenant.TenantContext, DedupKey, string, uint64) error
    Fail(context.Context, tenant.TenantContext, DedupKey, uint64, bool, string) error
}

type LeaseStore interface {
    Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (Lease, error)
    Renew(context.Context, tenant.TenantContext, Lease, time.Duration) (Lease, error)
    Release(context.Context, tenant.TenantContext, Lease) error
    Validate(context.Context, tenant.TenantContext, Lease) error
}

type Lease struct {
    TenantID    string
    SessionID   string
    OwnerID     string
    FenceToken  uint64
    LeasedUntil time.Time
}

type Claim struct {
    Key         DedupKey
    Status      ClaimStatus
    OwnerID     string
    Attempt     int
    FenceToken  uint64
    ClaimedAt   time.Time
    ExpiresAt   time.Time
    ResponseRef string
}

type RateLimiter interface {
    Allow(context.Context, LimitRequest) (LimitDecision, error)
}

type LimitRequest struct {
    TenantID       string
    BindingID      string
    Channel        string
    ExternalChatID string
    Cost           int64
}

type LimitDecision struct {
    Allowed    bool
    RetryAfter time.Duration
    Scope      string
    Remaining  int64
}

type CoordinationBackend interface {
    ClaimStore
    LeaseStore
    Close() error
}
```

建议实现：

- `trpcservice/storage/redis.RedisBackend`
- `trpcservice/storage/postgres.CoordinationStore`
- `trpcservice/storage/coordination.FailoverStore`
- `trpcservice/ratelimit.RedisLimiter`
- `trpcservice/ratelimit.LimitPolicy`
- `trpcservice/storage/lease.Runner`

现有 `SessionRepository.AcquireLease` 保持兼容；P0-06 新增 `LeaseStore` 必须显式携带 owner 和 fencing token，不能把旧接口误当作完整 fencing 协议。

## 5. 测试用例

### Redis 单元和集成测试

- 100 个并发 Claim 同一 tenant/Binding/message 只有一个成功。
- 跨 tenant 相同业务字段相互隔离。
- 已完成 Claim 返回同一 response reference，不重复执行。
- 未过期 Claim 不能接管；过期后只允许一个接管，attempt/token 递增。
- 旧 token 的 Complete/Fail/Renew/Release 全部拒绝且不修改新 owner 状态。
- Session lease 100 并发只有一个 owner；未过期拒绝，过期后接管。
- 续租正常时 Context 不取消；Redis timeout、连接断开、token 被接管时在有界时间内 cancel，并返回 `ErrLeaseLost`。
- 主执行结束后 renewal goroutine/timer 退出，`go test -race` 无竞争和泄漏。
- key 编码、租户前缀、特殊字符、过长字段和空值校验。
- Redis unavailable、脚本未知返回码和 Context cancel 的错误映射。

### PostgreSQL migration 和 Schema 测试

1. 全新隔离数据库/随机 schema 执行 `000001` + `000002` Up，断言 `session_lease` 存在，列类型、默认值、CHECK、复合 PK、复合 FK 和索引完整。
2. `000002` 重复 Up 必须幂等，checksum 不变；修改 migration source 后必须返回 checksum mismatch。
3. `000002` Down 只在隔离测试库执行，断言新增对象删除；Down 后再次 Up 能完整恢复。
4. 验证 `session_lease` 不能插入不存在的 tenant/session。
5. 验证不同 tenant 使用相同 session/resource ID 时数据互不影响。
6. 验证所有 fallback 相关唯一约束和外键都包含 `tenant_id`。
7. 验证 migration 失败时新增表、索引、版本记录全部回滚，不留下半成品。
8. 验证未知 migration version 和 Schema 缺失时 fail closed。

### PostgreSQL Claim fallback 全面测试

1. 100 个 goroutine/多个独立连接对同一 Claim 并发，断言恰好一个 winner。
2. 不同 tenant 的相同 channel、Binding、external message ID 各自只能有一个 winner且互不冲突。
3. 同一 tenant 下相同 external message ID 但不同 Binding 必须隔离。
4. 新 Claim 的 status、owner、attempt、expiry、fencing token 和 created/updated 时间正确持久化。
5. 未过期 Claim 被第二 owner 请求时返回已有状态，不增加 attempt/token。
6. 已完成 Claim 返回 response reference，不能重新取得执行权。
7. Claim 过期后多个并发 owner 竞争接管，只有一个接管成功；新 token 严格大于旧 token。
8. 过期接管时旧 owner 的 Complete/Fail 必须失败，新 owner 可以提交。
9. Complete 成功后重复 Complete、Fail 和旧 token 操作不能覆盖 response/status。
10. retryable/non-retryable Fail 的状态和 attempt 语义明确且持久化正确。
11. owner 不匹配、tenant 不匹配、fencing token 不匹配、空 key、非法 TTL、取消 Context 全部拒绝。
12. 人为制造事务错误/约束错误/连接中断，验证事务回滚，不留下半更新 Claim。
13. 锁等待超过 Context deadline 时返回 Context error，并验证后续 owner 可正常继续。
14. 多次接管循环验证 fencing token 单调递增且不会因数据库重启/重新创建 Repository 而重置。
15. 读取路径不允许跨租户拿到 Claim、response reference 或 owner 信息。

### PostgreSQL Session lease/fencing 全面测试

1. 100 个并发 Acquire 同一 `(tenant_id, session_id)`，只有一个 owner 成功。
2. 多个独立数据库连接和多个 Repository 实例并发 Acquire，结果仍只有一个成功。
3. 未过期 lease 被其他 owner Acquire 时返回冲突且不改 token。
4. lease 过期后多个 owner 并发接管，只有一个成功，token 严格递增。
5. Renew 只允许原 owner + 原 fencing token；错误 owner/token/tenant 全部拒绝。
6. Renew 成功后 `leased_until` 延长，owner/token 不改变。
7. Release 只允许原 owner/token；旧 owner Release 不能删除新 owner lease。
8. 旧 token 在新 owner 接管后执行 Validate/Complete/Release/Renew 全部失败。
9. Session 不存在时 Acquire/lease fallback 返回明确 NotFound 或业务错误，不自动创建伪 Session。
10. PostgreSQL `statement_timeout`、`lock_timeout`、Context cancel 在锁竞争时能终止等待，并能恢复后续操作。
11. 事务提交前连接断开、事务回滚和重复执行不会产生双 owner或半更新 token。
12. 长时间运行续租测试验证不会因时间精度/时区/数据库时钟偏差提前丢 lease。
13. 删除 Session 时受外键约束保护，lease 不会形成跨租户引用。

### PostgreSQL backend/Redis fallback 测试

1. Redis 正常时操作使用 Redis backend，不访问 PostgreSQL fallback。
2. 注入 Redis connection timeout/unavailable，Claim fallback 成功，100 并发仍只有一个 winner。
3. 注入 Redis unavailable，Session lease fallback 成功，100 并发仍只有一个 owner。
4. Redis 参数错误、业务冲突、fence rejection、脚本协议错误不能触发 fallback。
5. PostgreSQL 不可用时不发放新的 lease，不允许继续执行不受保护工作。
6. Redis 恢复后 backend 状态可恢复，不无条件重置 token，不重复复制短期 lease。
7. Redis/ PostgreSQL 切换期间同一业务 key 不能出现两个成功 owner；受控故障测试宁可返回 unavailable。
8. fallback 选择、熔断、恢复和超时状态可观测，错误不包含敏感信息。

### 限流测试

- tenant、Binding、Chat 各维度达到上限时拒绝。
- 多维度任一超限时整体拒绝。
- 同窗口并发请求不会超过额度，窗口过期后恢复。
- 相同 Binding/Chat ID 在不同 tenant 下隔离。
- Redis server time 决定窗口。
- Redis 故障时返回 `ErrRateLimitBackendUnavailable`，不能静默放行。

### 契约与兼容性测试

- Redis 和 PostgreSQL backend 对同一输入返回等价 Claim/Lease 状态。
- `FakeRepository` 适配器继续通过现有 storage contract tests。
- 不修改 Agent、IM、模型和现有平台行为测试。
- 所有协调测试运行 `go test -race`。

## 6. 验收命令

```bash
# 单元、契约和静态检查
go test ./... -count=1
go test ./trpcservice/storage/... ./trpcservice/ratelimit/... -race -count=1
go vet ./...

# 显式本机/CI PostgreSQL 测试库
TEST_DATABASE_URL='postgres://...@127.0.0.1:5432/trpc_agent_test?sslmode=disable' \
  ./scripts/test-postgres-migrations.sh

# Redis 和 PostgreSQL fallback 集成测试，两个 DSN 都必须显式提供
TEST_REDIS_URL='redis://127.0.0.1:6379/15' \
TEST_DATABASE_URL='postgres://...@127.0.0.1:5432/trpc_agent_test?sslmode=disable' \
  ./scripts/test-redis-coordination.sh

# 直接运行 Redis 相关 race tests
TEST_REDIS_URL='redis://127.0.0.1:6379/15' \
  go test ./trpcservice/storage/redis ./trpcservice/ratelimit -race -count=1
```

验收还必须确认：

- 没有 `TEST_REDIS_URL` 时不连接默认 Redis；脚本跳过或以明确非零状态拒绝。
- 没有 `TEST_DATABASE_URL` 时不连接默认或生产 PostgreSQL。
- PostgreSQL fallback integration 每次使用独立随机 schema/数据库，结束后清理；不得使用共享 public schema 写固定业务数据。
- P0-06 package 不 import Agent、IM adapter、模型 provider 或执行流程包。
- `000001`/`000002` migration 的 Up、重复 Up、checksum mismatch、失败回滚、测试 Down、Down 后重新 Up 全部通过。
- PostgreSQL Claim/lease 全部并发、过期接管、旧 token、事务回滚、锁等待、Context cancel、租户隔离和 fallback 切换测试通过。

## 7. 风险和回滚方案

- **Redis 与 PostgreSQL 双 backend 产生双 owner**：由 failover selector 在同一业务 key 上选择唯一 active backend；切换期间宁可拒绝，不接受双 owner。
- **Redis TTL 与 PostgreSQL 时间语义不一致**：Redis 使用 server time/TTL，PostgreSQL 使用 `now()`；测试覆盖边界时间，renew interval 必须明显小于 TTL。
- **旧 fencing token 写入**：所有 Complete、Fail、Session 提交保护必须是带 token 条件的原子写，不能只在应用层校验。
- **PostgreSQL 锁等待/连接池耗尽**：设置明确 Context、lock timeout、statement timeout、fallback 并发上限和熔断；超过容量直接拒绝。
- **Redis 故障期间 PostgreSQL 过载**：记录 fallback 次数和延迟，限制 fallback 并发；Redis 恢复后切回。
- **限流 fail closed 造成短时拒绝**：记录 Retry-After 和指标，不默认为 fail open。
- **PostgreSQL migration 风险**：`000002` 与 down 文件成对、独立事务、checksum 校验；只在隔离测试库执行 Down，生产使用 forward migration 或备份恢复。
- **错误 key/tenant 漏洞**：统一 key builder 和输入校验，增加所有跨租户读写测试。
- **回滚**：先停用 Redis backend，切换到 PostgreSQL fallback 或 fail closed；保留 `message_dedup` 和 `session_lease` 数据；Redis key 使用版本化前缀自然过期；代码回滚不回退 P0-05 migration。已应用的 P0-06 migration 只在隔离测试库使用 down。

## 8. 计划修改的具体文件

### 新增

- `trpcservice/storage/redis/config.go`
- `trpcservice/storage/redis/errors.go`
- `trpcservice/storage/redis/keys.go`
- `trpcservice/storage/redis/scripts.go`
- `trpcservice/storage/redis/claim.go`
- `trpcservice/storage/redis/lease.go`
- `trpcservice/storage/redis/renewal.go`
- `trpcservice/storage/redis/ratelimit.go`
- `trpcservice/storage/redis/fallback.go`
- `trpcservice/storage/redis/config_test.go`
- `trpcservice/storage/redis/keys_test.go`
- `trpcservice/storage/redis/claim_test.go`
- `trpcservice/storage/redis/lease_test.go`
- `trpcservice/storage/redis/renewal_test.go`
- `trpcservice/storage/redis/ratelimit_test.go`
- `trpcservice/storage/redis/fallback_test.go`
- `trpcservice/storage/redis/redis_integration_test.go`
- `trpcservice/ratelimit/ratelimit.go`
- `trpcservice/ratelimit/ratelimit_test.go`
- `trpcservice/storage/coordination/failover.go`
- `trpcservice/storage/coordination/failover_test.go`
- `trpcservice/storage/postgres/dedup.go`
- `trpcservice/storage/postgres/lease.go`
- `trpcservice/storage/postgres/dedup_test.go`
- `trpcservice/storage/postgres/lease_test.go`
- `trpcservice/storage/postgres/coordination_integration_test.go`
- `migrations/000002_coordination.up.sql`
- `migrations/000002_coordination.down.sql`
- `scripts/test-redis-coordination.sh`
- Redis Lua scripts或等价的 embedded script resources。

### 允许修改

- `go.mod`、`go.sum`：仅加入 Redis client 和 P0-06 直接依赖。
- `trpcservice/storage/repository.go`、`trpcservice/storage/errors.go`：仅在现有契约无法表达 owner/fencing/renew 语义时做最小兼容扩展。
- `trpcservice/storage/postgres/`：仅新增 P0-06 fallback Repository、连接/事务 helper 和测试。
- `migrations/`、`scripts/`：仅新增 P0-06 migration、测试入口和文档。
- `trpcservice/storage/redis/`、`trpcservice/storage/coordination/`、`trpcservice/ratelimit/`：P0-06 新增实现。

### 明确不修改

- `trpcservice/agent/`、模型 provider、Runner、Tool 和执行逻辑。
- `trpcservice/channels/`、Web、企业微信、Telegram 或其他 IM adapter 行为。
- `trpcservice/gateway/`、`trpcservice/worker/`、Outbox dispatcher 和 P0-07/P0-08/P0-09 编排。
- 生产配置、生产服务器、生产数据库、生产日志、密钥、Token、密码和 `data/`。
- 不执行 git commit、git push；不引入 Redis 集群部署、监控平台或后续任务基础设施。

## 最终验收标准

- Redis 正常路径和 PostgreSQL fallback 路径都能证明 100 并发只有一个 Claim/lease owner。
- Claim/Session lease 过期后可安全接管，attempt/fencing token 严格递增。
- 旧 fencing token 无法 Complete、Fail、Renew、Release 或提交结果。
- 续租失败在有界时间内取消执行 Context，且没有 goroutine/timer 泄漏。
- tenant、Binding、Chat 限流具备原子性和租户隔离；Redis 故障时不静默放行。
- PostgreSQL migration、Schema、Claim、lease、事务、锁、Context、租户隔离和 fallback 故障测试全面通过。
- 不修改 Agent、IM、模型和后续 P0 任务逻辑。
- 所有测试在 `go test -race` 下通过，且不访问生产环境。
