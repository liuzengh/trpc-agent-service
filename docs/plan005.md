# P0-05 PostgreSQL Schema 与 Migration 完整实施计划

## 1. 当前代码状态

- 仓库模块为 `github.com/liuzengh/trpc-agent-service`，Go 版本为 `1.21`。
- `go.mod` 已锁定 `github.com/jackc/pgx/v5 v5.7.2`，同时已有 Redis、OTel 和 tRPC-Agent-Go 依赖；当前 pgx 仍标记为 indirect，但本任务不需要新增版本或引入迁移第三方库。
- P0-02 至 P0-04 已建立租户、Session、Memory、Artifact、Audit 领域模型，以及 `trpcservice/storage` 中的 Repository 契约、Fake Repository、租户校验、CAS、幂等 Claim 和 Outbox 类型。
- `trpcservice/storage/repository.go` 已定义：
  - `SessionRepository` 与 `AppendEvent(expectedVersion, event)`。
  - `IdempotencyRepository`、`Claim`、`DedupKey`。
  - `MemoryRepository`、`SummaryRepository`、`ArtifactRepository`、`AuditRepository`、`OutboxRepository`。
  - `OutboxMessage` 的状态、Attempt、NextAttempt、锁和错误字段。
- 当前不存在：
  - `migrations/` 目录。
  - `trpcservice/storage/postgres/` 目录。
  - `deploy/` 目录。
  - `scripts/` 目录。
  - PostgreSQL 连接配置、Migrator 实现、迁移版本表和 SQL 文件。
- 当前服务仍以现有内存/本地运行方式为主；`data/` 仅是运行时日志、PID 和本地数据目录，README 明确禁止提交密钥或生产数据。
- 当前脚本只有构建、启动、停止、清理、格式化、Lint 和覆盖率脚本；没有数据库操作脚本。
- 只读检查未连接 PostgreSQL、未访问生产服务器、未读取任何密钥/Token/密码/生产日志，也没有产生写入性构建或测试产物。

## 2. 需求与现有代码的差距

### 目标差距

需要新增一个仅面向测试/开发数据库的 PostgreSQL Schema 和迁移层，覆盖：

`tenant`、`agent_app`、`channel_binding`、`user_identity`、`session`、`session_event`、`message_dedup`、`memory`、`summary`、`artifact`、`audit_log`、`outbox_message`、`dead_letter`、`agent_release`、`tenant_config_version`。

当前以上表、索引、外键、检查约束、迁移版本管理均不存在。

### 关键约束差距

- 当前数据库层没有统一强制 `tenant_id` 的机制。
- 需要所有业务表的主键和唯一约束都显式包含 `tenant_id`；即使逻辑上某个字段全局唯一，也不采用不含租户的唯一约束。
- 需要所有跨表外键使用带 `tenant_id` 的复合外键，防止跨租户引用。
- 需要 `session_event` 同时保证：
  - `(tenant_id, event_id)` 幂等唯一。
  - `(tenant_id, session_id, sequence)` 顺序唯一。
  - 必要时对 `(tenant_id, session_id, message_id, event_type)` 做部分唯一索引，避免同一消息同一事件类型重复写入。
- 需要迁移版本、名称和 checksum 管理，重复执行已应用版本必须成功且无副作用，已应用文件内容改变必须失败而不是静默覆盖。
- 需要明确每次 migration 的事务边界，并验证失败时整个 migration 回滚。
- 当前没有生产迁移入口；本任务必须保持为“测试数据库可执行，生产数据库不触碰”。

## 3. 详细实施步骤

### 3.1 固定迁移策略

采用自定义轻量 Go Migrator，不引入 goose、golang-migrate 等新基础设施：

1. 以 `migrations/` 下的版本化 SQL 文件作为唯一 Schema 来源。
2. 首个版本使用：
   - `migrations/000001_initial.up.sql`
   - `migrations/000001_initial.down.sql`
3. 后续 Schema 变更必须新增版本，不修改已经发布的 migration 文件。
4. Migrator 首次运行时创建 `schema_migration` 元数据表。该表为基础设施元数据，不属于租户业务表。
5. `schema_migration` 至少包含 `version bigint primary key`、`name`、`checksum`、`applied_at`；版本主键是迁移版本，不是租户业务主键。
6. 使用 PostgreSQL advisory lock 保证同一数据库不会同时运行两个迁移器。
7. 每个 up/down migration 独立开启一个事务；执行成功后才写入或删除对应版本记录。
8. 迁移已应用且 checksum 相同则跳过；版本相同但名称或 checksum 不同则立即失败。
9. 不使用 `CREATE INDEX CONCURRENTLY`，确保 DDL 可纳入事务；大规模生产索引建设留给后续运维任务，不能在 P0-05 中执行。
10. SQL 使用 `CREATE TABLE IF NOT EXISTS`、`CREATE INDEX IF NOT EXISTS`、`DROP TABLE IF EXISTS` 等可重复形式；约束补充使用受保护的 `DO $$ ... pg_constraint ... $$`，避免 PostgreSQL 不支持 `ADD CONSTRAINT IF NOT EXISTS` 的问题。

### 3.2 设计核心表和关系

以下是实施时固定的表结构方向。字段类型以 `varchar`、`bigint`、`boolean`、`jsonb`、`bytea`、`timestamptz` 为主，时间统一使用 UTC 的 `timestamptz`。

#### `tenant`

- 主键：`(tenant_id)`。
- 字段：`name`、`status`、`config_version`、`default_agent_app_id`、`backend_config jsonb`、`policy_config jsonb`、预算字段、`created_at`、`updated_at`。
- 不保存明文 Secret、Token、密码或模型 API Key；只保存 Secret Manager 引用。
- `default_agent_app_id` 不在初始建表阶段建立会造成循环依赖的强制外键；如需要引用，在 agent_app 创建后用带租户条件的复合约束补充，或由后续配置发布任务处理。

#### `agent_app`

- 主键：`(tenant_id, agent_app_id)`。
- 字段：名称、状态、模型配置引用、`system_prompt` 或其引用、工具策略 JSON、guardrail 引用、时间字段。
- 外键：`tenant_id -> tenant(tenant_id)`。

#### `channel_binding`

- 主键：`(tenant_id, channel, binding_id)`。
- 唯一：`(tenant_id, channel, external_app_id)`，显式包含 tenant_id；不采用文档中未带租户的全局唯一形式。
- 字段：`external_app_id`、`secret_ref`、enabled、配置 JSON、时间字段。
- 外键：`tenant_id -> tenant(tenant_id)`、必要时 `(tenant_id, agent_app_id) -> agent_app`。
- `secret_ref` 只允许引用，不保存密钥正文。

#### `user_identity`

- 主键：`(tenant_id, identity_id)`。
- 唯一：`(tenant_id, channel, binding_id, external_user_id)`。
- 字段：外部用户 ID、显示名、原始身份摘要/属性 JSON、首次/最近出现时间。
- 外键：`(tenant_id, channel, binding_id) -> channel_binding`。

#### `session`

- 主键：`(tenant_id, session_id)`。
- 字段：`agent_app_id`、`agent_version`、channel、binding、外部 chat/user/thread 标识、状态、`state_version`、`summary_version`、`last_event_seq`、时间字段。
- 外键：`(tenant_id, agent_app_id) -> agent_app`、`(tenant_id, channel, binding_id) -> channel_binding`。
- 初始默认值与领域模型保持一致：`state_version = 1`、`summary_version = 0`、`last_event_seq = 0`。
- 需要增加非负检查约束和合法状态检查约束；状态枚举值与现有 `session.SessionState` 一致。

#### `session_event`

- 主键：`(tenant_id, event_id)`。
- 外键：`(tenant_id, session_id) -> session`。
- 唯一：`(tenant_id, session_id, sequence)`。
- 部分唯一索引：`(tenant_id, session_id, message_id, event_type)`，仅对 `message_id IS NOT NULL` 生效。
- 字段：`sequence`、event_type、role、message_id、execution_id、parent_event_id、attempt、trace_id、payload JSONB、created_at。
- `sequence >= 1`、`attempt >= 1`；`event_id` 是应用层重试时使用的幂等键。
- 数据库只负责唯一性和租户关系；重复 `event_id` 的 payload 一致性比较及“返回已有结果/不重新执行”由后续 Repository/业务层实现。

#### `message_dedup`

- 主键：`(tenant_id, channel, binding_id, external_message_id)`。
- 字段：状态、owner、attempt、fence_token、claim/expire 时间、response_ref、错误信息、created/updated 时间。
- 外键：`(tenant_id, channel, binding_id) -> channel_binding`。
- 状态与现有 Claim 类型一致：acquired/in_flight/completed，并为后续过期接管保留字段。

#### `memory`

- 主键：`(tenant_id, memory_id)`。
- 字段：scope、scope_id、可空 session_id、kind、content、vector_ref、version、source_seq、deleted、时间字段。
- 外键：`(tenant_id, session_id) -> session`，允许非 Session scope 的 `session_id` 为 NULL。
- scope、version、source_seq、size/内容相关检查与现有 `memory.Memory` 语义一致。

#### `summary`

- 主键：`(tenant_id, session_id)`。
- 外键：`(tenant_id, session_id) -> session`。
- 字段：version、covered_seq、content、token_estimate、created_at、updated_at。
- 检查：version >= 1、covered_seq >= 0、token_estimate >= 0。
- 不在迁移层实现单调更新逻辑；后续 Repository 使用带版本/covered_seq 条件的 upsert。

#### `artifact`

- 主键：`(tenant_id, artifact_id)`。
- 外键：`(tenant_id, session_id) -> session`。
- 字段：message_id、object_key、mime_type、size_bytes、sha256、status、expires_at、created_at。
- object_key 继续由领域层强制校验 `tenants/{tenant_id}/...`；数据库增加非空和大小/状态基础约束，但不把二进制内容写入 PostgreSQL。

#### `audit_log`

- 主键：`(tenant_id, audit_id)`。
- 字段覆盖现有 AuditLog：trace_id、request_id、execution_id、channel、external_user、session_id、agent_app_id、tool_name、decision、latency、cost_cents、error_type、metadata JSONB、created_at。
- 可选外键字段使用带租户的复合外键；审计写入不能因关联业务对象删除而丢失，因此采用 `ON DELETE SET NULL` 或不设置强制 session 外键，具体以保留审计完整性为准。
- 数据库不提供敏感字段自动脱敏；写入前必须经过现有 `audit.RedactMetadata`，P0-05 不改审计业务流程。

#### `outbox_message`

- 主键：`(tenant_id, outbox_id)`。
- 字段对应现有 `storage.OutboxMessage`：kind、aggregate_id、payload JSONB/bytea、status、attempt、next_attempt_at、locked_by、locked_until、last_error、created_at、updated_at。
- 增加租户内幂等字段，例如 nullable `dedup_key`，并建立 `(tenant_id, dedup_key)` 的部分唯一索引，避免使用不含租户的全局唯一约束。
- 为后续 `SKIP LOCKED` 查询建立 tenant-qualified 状态/时间索引：`(tenant_id, status, next_attempt_at)`。

#### `dead_letter`

- 主键：`(tenant_id, dead_letter_id)`。
- 唯一：`(tenant_id, outbox_id)`，防止同一租户的 Outbox 消息重复进入 DLQ。
- 字段：outbox_id、kind、payload、attempt、reason、last_error、failed_at、created_at。
- 外键：`(tenant_id, outbox_id) -> outbox_message`。

#### `tenant_config_version`

- 主键：`(tenant_id, config_version)`。
- 字段：状态、配置 JSONB、checksum、created_by、published_at、created_at。
- 外键：`tenant_id -> tenant(tenant_id)`。
- 唯一：`(tenant_id, checksum)`，允许不同租户拥有相同配置 checksum。

#### `agent_release`

- 主键：`(tenant_id, agent_app_id, agent_version)`。
- 唯一：`(tenant_id, release_id)`，如保留 release_id；所有唯一约束均带 tenant_id。
- 字段：发布状态、model_config_ref、tool_policy JSONB、artifact/config 引用、发布时间。
- 外键：`(tenant_id, agent_app_id) -> agent_app`。

### 3.3 索引设计

除主键/唯一索引外，增加以下租户限定索引：

- `session`: `(tenant_id, channel, external_chat_id)`、`(tenant_id, updated_at)`。
- `session_event`: `(tenant_id, session_id, sequence)`、`(tenant_id, session_id, created_at)`、`(tenant_id, event_id)`。
- `message_dedup`: `(tenant_id, status, expires_at)`。
- `memory`: `(tenant_id, scope, scope_id, updated_at)`、`(tenant_id, session_id, source_seq)`。
- `artifact`: `(tenant_id, session_id, created_at)`、`(tenant_id, status, expires_at)`。
- `audit_log`: `(tenant_id, created_at)`、`(tenant_id, session_id, created_at)`。
- `outbox_message`: `(tenant_id, status, next_attempt_at)`。
- `dead_letter`: `(tenant_id, failed_at)`。

所有索引命名加入表名和关键列，避免不同版本或不同表之间名称冲突。

### 3.4 Go Migrator 和配置接口

在 `trpcservice/storage/postgres/` 中新增以下公开类型，避免暴露生产凭据：

```go
type PostgresConfig struct {
    URL                string
    MaxConns           int32
    MinConns           int32
    ConnMaxLifetime    time.Duration
    ConnectTimeout     time.Duration
    MigrationLockKey   int64
}

type MigrationVersion struct {
    Version   int64
    Name      string
    Checksum  string
}

type Migrator interface {
    Up(ctx context.Context) error
    Down(ctx context.Context, steps int) error
    Current(ctx context.Context) (MigrationVersion, error)
}
```

实现要求：

- `NewMigrator(config PostgresConfig, source fs.FS)` 返回使用 `pgxpool.Pool` 的 Migrator。
- 提供 `NewMigratorFromDir` 或嵌入 `embed.FS` 的适配层；SQL 文件按版本名排序并校验 up/down 成对存在。
- `PostgresConfig.URL` 只从调用方传入，错误信息只报告连接阶段/迁移版本，不打印 URL、用户名、密码、Token 或 DSN 查询参数。
- `Up`：获取 advisory lock，确保 metadata 表存在，校验历史 checksum，逐版本事务执行。
- `Down`：只允许显式传入 steps，按逆序执行 down；默认不在服务启动时调用；删除业务表前必须由调用方明确执行并完成备份。
- 每个 SQL 文件执行失败时回滚当前事务，不写 migration 记录；释放 advisory lock 后返回原始错误类别。
- 不在本任务中实现业务 Repository、UnitOfWork、Session AppendEvent、Redis Dedup、Outbox Dispatcher、RLS、生产部署或备份作业。

### 3.5 事务边界

固定为以下边界：

- Migrator 启动/结束：连接池生命周期由调用方管理；迁移期间使用 advisory lock。
- 每个 migration 文件：一个独立数据库事务。
- `schema_migration` 创建和单个版本的 DDL/约束/索引记录在同一事务内。
- 首次 `000001_initial` 的全部 15 张业务表、约束和索引在同一个事务内创建，任何一张表失败都回滚整个初始 Schema。
- 后续业务写事务不在 P0-05 实现，但文档和测试固定后续边界：模型/Tool 执行在 SQL 事务外；Session event、Session CAS、assistant message 和同事务 Outbox 由 P1-04/P0-09 实现。
- 不使用跨 migration 的长事务，不把模型调用、网络调用或发送动作放入 PostgreSQL 事务。

### 3.6 回滚与备份边界

- 本任务只允许在本地/CI 的测试 PostgreSQL 执行；不得设置生产 DSN，不得连接生产服务器，不得读取生产日志或生产备份。
- 生产前置要求写入迁移 README：
  1. 先确认目标数据库、版本和租户范围。
  2. 完成并验证 PostgreSQL 逻辑备份或等价快照。
  3. 在隔离恢复库执行 `pg_restore`/恢复演练并校验行数、约束和应用健康检查。
  4. 记录 migration checksum 和当前版本。
  5. 采用低峰期、可观察、可停止的 forward migration；P0-05 不提供自动生产执行入口。
- 回滚优先使用“恢复备份”或后续兼容的 forward migration，不建议生产执行 destructive down migration。
- `down.sql` 仅用于测试库回滚验证；执行前必须显式指定 steps，按依赖反向删除表，且使用 `DROP ... IF EXISTS`。
- 若 migration 中途失败，事务自动回滚，数据库保持上一个已记录版本；若失败发生在锁等待/连接断开，先确认事务状态，再重新运行 Up。

## 4. Go 接口和数据结构

新增 `trpcservice/storage/postgres/`：

- `config.go`：`PostgresConfig`、默认值校验、敏感信息安全错误策略。
- `migration.go`：`Migrator`、`MigrationVersion`、文件发现、版本排序、checksum、advisory lock、Up/Down/Current。
- `pool.go`：pgxpool 创建、Ping/Close 生命周期辅助函数。
- `errors.go`：迁移配置错误、版本冲突、checksum 不一致、迁移执行错误的可判定错误包装。
- `migration_test.go`：不依赖真实数据库的文件排序、版本解析、checksum 和配置测试。
- `postgres_integration_test.go`：在显式提供测试 DSN 时运行真实 PostgreSQL 契约；未提供时跳过，不尝试默认连接任何环境。

建议接口保持最小化，不在 P0-05 提前增加业务 CRUD 方法：

```go
type Migrator interface {
    Up(context.Context) error
    Down(context.Context, int) error
    Current(context.Context) (MigrationVersion, error)
}
```

后续 P1-04 通过已有 `storage.SessionRepository` 等接口接入 PostgreSQL，不在本任务修改 `platform`、Web、IM Adapter 或 Agent 执行入口。

## 5. 测试用例

### 5.1 静态/单元测试

- 版本文件按数字版本排序，不按字符串顺序错误排序。
- up/down 文件缺失时构造 Migrator 失败。
- migration 文件名非法、版本重复、名称不匹配时失败。
- checksum 对同一文件内容稳定；修改已应用 migration 时返回 checksum mismatch。
- 空 DSN、非法连接超时、非法连接池参数被拒绝。
- 错误文本不包含 URL、密码、Token、Authorization、DSN 查询参数。

### 5.2 PostgreSQL 集成测试

仅连接专用测试 PostgreSQL：

1. 初始数据库执行 `Up` 成功，15 张业务表和 `schema_migration` 存在。
2. 连续执行两次 `Up`，第二次无新增版本、无失败、Schema 不变。
3. 查询 pg_catalog，验证每张业务表的主键都包含 `tenant_id`；所有预期唯一约束/唯一索引都包含 `tenant_id`。
4. 插入 tenant-a 和 tenant-b 的同名 `agent_app_id`、`session_id`、`event_id`、`artifact_id`、`outbox_id`，验证跨租户可独立存在。
5. 插入同一租户重复 `channel_binding(channel, external_app_id)`，失败；不同租户相同值成功。
6. 插入 user_identity 重复外部身份，失败；不同租户成功。
7. 插入 session_event：
   - 同租户同 event_id 第二次插入触发 `(tenant_id, event_id)` 冲突/可被业务层识别为幂等。
   - 同租户同 session 同 sequence 冲突。
   - 不同 session 或不同 tenant 可以使用相同 sequence/event_id 字符串。
   - 相同 message_id + event_type 的重复事件被部分唯一索引拦截；message_id NULL 不产生错误的 NULL 全局互斥。
8. 插入所有子表的跨租户复合外键引用，必须失败。
9. 验证 Session、Summary、Memory、Artifact、Outbox、DLQ 的非负字段和状态 CHECK 约束。
10. 验证 migration 失败时整个事务回滚：在测试专用临时 migration 中插入可观察 DDL/表后故意失败，确认临时表和版本记录都不存在。
11. 验证下滚：测试库执行 `Down(ctx, 1)` 删除初始版本，确认业务表和版本记录消失；然后再次 `Up` 可完整恢复。
12. 并发执行两个 Migrator，只有一个持有 advisory lock 并执行，另一个等待后安全跳过。
13. 验证 `schema_migration` 中版本、名称、checksum 正确且 applied_at 存在。
14. 验证没有触碰仓库 `data/` 目录中的运行时文件，也没有默认连接非测试 DSN。

## 6. 验收命令

由于当前仓库没有 `deploy/` 和 `scripts/` 目录，P0-05 计划新增测试专用迁移脚本，但脚本只接受显式测试数据库连接字符串，不提供生产默认值。

建议验收顺序：

```bash
go test ./trpcservice/storage/postgres -count=1

go test ./trpcservice/storage ./trpcservice/session ./trpcservice/memory ./trpcservice/artifact ./trpcservice/audit -count=1

go test ./... -count=1
```

在本地专用 PostgreSQL 已启动且通过环境变量提供测试 DSN 后：

```bash
TEST_DATABASE_URL='仅指向本地/CI测试库的连接串' \
  ./scripts/test-postgres-migrations.sh
```

脚本行为固定为：检查 `TEST_DATABASE_URL` 非空、执行 Up、重复执行 Up、运行 PostgreSQL integration tests；不打印完整 DSN，不读取其他生产配置，不执行生产迁移。

SQL 级别的人工/CI 检查：

```bash
psql "$TEST_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/000001_initial.up.sql
```

上面的命令仅允许在测试库使用；实际脚本应隐藏或裁剪连接信息输出。最终验收以：

- 全部 Go 测试通过。
- PostgreSQL 重复迁移通过。
- 复合 PK/UK/FK 和 Event sequence/event_id 测试通过。
- 迁移失败事务回滚通过。
- `go vet ./...` 和仓库既有 lint/format 检查通过。
- `git diff --name-only` 仅包含本任务允许文件。

为避免写出覆盖率产物，P0-05 计划不把 `coverage.sh` 作为必需验收命令；若执行覆盖率脚本，需单独清理其产生的 `coverage.out`，且不把该文件纳入提交。

## 7. 风险和回滚方案

### 风险

- **复合外键遗漏 tenant_id**：可能形成跨租户引用。通过 SQL 评审、pg_catalog 断言和跨租户插入测试拦截。
- **把 `channel/external_app_id` 做成全局唯一**：会阻止不同租户复用外部应用标识。固定使用 `(tenant_id, channel, external_app_id)`。
- **event_id 只做应用层去重**：并发重试仍可能写入两次。数据库设置 `(tenant_id, event_id)` 主键，并保留 sequence 唯一约束。
- **event_id 重复但 payload 不同**：数据库只能报告冲突，不能判断业务语义。P0-05 只建立约束，后续 Repository 必须读取已有事件并判定冲突/幂等。
- **migration 文件被修改**：可能造成同版本不同 Schema。通过 checksum 记录并拒绝 mismatch。
- **DDL 锁等待或表级锁**：初始版本只针对空测试库；生产大表索引、在线迁移和分批部署不纳入本任务。
- **down migration 误删数据**：down 仅测试用途，生产优先备份恢复或 forward migration，禁止服务自动调用 Down。
- **敏感信息泄露**：配置错误、连接错误和脚本输出均不得打印完整 DSN、密码、Token、生产日志或备份内容。
- **过度实现后续任务**：不实现 PostgreSQL Repository、Redis、RLS、Outbox Dispatcher、生产部署、备份作业或 Agent 逻辑。

### 回滚执行方案

- 开发/CI：执行 `Down(ctx, 1)` 或删除并重建专用测试数据库，再重新 `Up`。
- migration 执行中失败：依赖事务自动回滚，确认 `schema_migration` 未写入该版本后重新 `Up`。
- checksum 不一致：停止迁移，恢复正确 migration 文件或新增 forward migration；不使用强制覆盖历史版本的开关。
- 生产预演失败：不在生产数据库继续尝试；使用已验证备份恢复到隔离库分析，生产保持原版本。
- 本任务代码回滚：删除新增的 `migrations/`、`trpcservice/storage/postgres/` 和测试脚本，不修改现有领域包、生产数据库或运行时 `data/` 内容。

## 8. 计划修改的具体文件

### 新增

- `migrations/000001_initial.up.sql`
- `migrations/000001_initial.down.sql`
- `migrations/README.md`
- `trpcservice/storage/postgres/config.go`
- `trpcservice/storage/postgres/errors.go`
- `trpcservice/storage/postgres/migration.go`
- `trpcservice/storage/postgres/pool.go`
- `trpcservice/storage/postgres/migration_test.go`
- `trpcservice/storage/postgres/postgres_integration_test.go`
- `scripts/test-postgres-migrations.sh`

### 允许的必要修改

- 如 Go 编译器/模块校验要求将已存在的 pgx 依赖从 indirect 调整为 direct，可修改 `go.mod`/`go.sum`，但不升级版本、不引入额外依赖；优先保持 P0-01 已锁定版本不变。
- 如仓库需要在 `scripts/` 中增加统一入口，只新增 P0-05 测试迁移脚本，不修改现有启动、停止、清理脚本行为。

### 明确不修改

- 不修改生产数据库、生产服务器、生产日志、生产备份或任何真实 DSN 配置。
- 不修改 `data/` 中的运行时文件；只读保留 `data/README.md` 约束。
- 不修改 Web、IM Adapter、Agent Runner、Redis、Vector、Object Storage 或后续部署模块。
- 不提前实现 P0-06、P0-07、P0-08、P0-09、P1-04、P2-04、P2-05。
- 不执行 `git commit`、`git push`、数据库迁移命令或任何生产访问操作。
- 不读取或输出任何密钥、Token、密码和生产日志。

## 默认假设

- P0-05 的“迁移可重复执行”定义为：已应用且 checksum 未变化的 migration 重复运行成功并跳过；未应用版本可安全重试；同版本文件变化必须报错。
- `tenant_id` 使用非空文本标识，所有业务 PK/UK/FK 都采用显式复合键。
- 本任务只提供生产前可审计的迁移代码和测试策略，不提供生产执行入口；生产 RLS、备份恢复自动化和在线大表迁移属于后续任务。
- PostgreSQL 测试需要用户或 CI 显式提供隔离测试 DSN；没有 DSN 时 integration test 跳过，绝不猜测或连接生产环境。
- Event sequence 从 1 开始，Session 的 `last_event_seq` 从 0 开始；sequence 分配和 Session CAS 的原子业务事务由后续 PostgreSQL Repository 实现。
