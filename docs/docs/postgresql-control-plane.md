# PostgreSQL 控制面持久化与启动装配

> 本页是 Issue #37 的实现契约。它复用已经合入的 Tenant、Agent App/Revision、Backend
> Profile 和 Channel Binding 设计，并记录 Model Profile 的持久化形状、统一 migration 顺序、
> Repository 事务边界和进程启动装配。控制面 DDL 与受控 Repository 写入口分别从
> `0001`、`0002` 开始，`migrations.Apply` 执行完整的嵌入式序列至 `0017_agent_chain.up.sql`；
> Go Repository 和 bootstrap 实现在
> `trpcservice/{tenant,app,model,backend,channels}/postgres`；每个领域包拥有自己的
> SQL Repository、行解码和领域 codec。`trpcservice/storage/postgres` 只提供不依赖任何
> 控制面领域的连接池、事务、错误映射与 JSON 基础设施；`trpcservice/bootstrap` 负责装配。

## 目标与边界

Issue #37 的目标是把当前的 InMemory 控制面推进为一条可持久化、可恢复、可测试的纵向链路：

```text
PostgreSQL migration / roles
             │
             ▼
Tenant + App/Revision + Model + Backend + Binding SQL Repositories
             │
             ▼
explicit BootstrapConfig
             │
             ├── Provider Catalogs
             ├── SecretResolver / ModelFactory
             ├── Session capability
             ├── PlanResolver
             ├── RunnerRegistry
             └── HTTP Gateway + readiness
```

本 Issue 持久化当前控制面纵向链路的六类对象：`tenant`、`agent_app`、
`agent_app_revision`、`model_profile`、`backend_profile` 和 `channel_binding`。当前
`gateway.PlanResolver` 的直接 Repository 输入仍只有 Tenant、Agent App、Model Profile 和
Backend Profile 四类，外加两个 Provider Catalog；它不读取 Channel Binding。Channel Binding
由 Channel Adapter/Candidate Index 完成候选发现和可信路由，bootstrap 必须把它作为独立的
Repository/候选索引依赖装配，只有形成可信 Principal 后才进入 Dispatcher → PlanResolver。
Session、Event、Memory、Summary、Knowledge、Artifact、Redis、S3-compatible object、Vault、
Admin API 和 Outbox 消费均通过对应 runtime 与 bootstrap 模块接入；本页记录它们与 PostgreSQL
控制面的事务和启动边界。

## 既有表设计的复用关系

前置设计已经定义了领域字段和安全不变量；Issue #37 的 migration 不重新发明这些模型，
而是把它们整理成一套有明确依赖顺序的 PostgreSQL 变更：

| 对象 | 既有规范 | 本 Issue 的落地责任 |
| --- | --- | --- |
| `tenant` | [数据模型](data-model.md) 的根表、生命周期函数、Outbox 和角色边界 | 建表、状态/配置受控函数和默认引用外键 |
| `agent_app` / `agent_app_revision` | [Agent App 模型](agent-app-model.md) 的发布、回滚和不可变 Revision | 建表、复合外键、发布/回滚事务和 Outbox |
| `model_profile` | 本页补齐的目标形状；领域契约见 [Model Profile](model-profile.md) | 建表、无密钥配置列、生命周期和 Outbox |
| `backend_profile` | [Backend Profile 模型](backend-profile.md) 的 binding、延迟约束和状态函数 | 建表、复合外键、binding 完整性和 Outbox |
| `channel_binding` | [Channel Binding](channel-binding.md) 的候选索引和 active account 约束 | 建表、同租户 App 外键和候选查询索引 |

所有关联对象都显式携带 `tenant_id`。单列 `tenant_id → tenant(tenant_id)` 只表达根租户
存在；对象之间的引用必须使用 `(tenant_id, object_id)` 复合外键，不能让一个租户的 App、
Profile 或 Binding 被另一个租户引用。key 的唯一性也都限定在租户范围内，只有公开候选路由
和 active provider account 按领域规范使用全局索引。

## Migration 组织与执行前提

迁移由 `migrations` 包按文件名和版本顺序执行，并通过历史摘要、连续版本和数据库锁保证可恢复；
控制面与运行时 migration 使用同一套启动装配入口。当前嵌入式有序 migration 序列包括：

```text
0001_control_plane.up.sql
0002_control_plane_repository_functions.up.sql
0003_runtime_storage.up.sql
0004_runtime_session_delete_cascade.up.sql
0005_runtime_event_history.up.sql
0006_audit_usage.up.sql
0007_execution_audit_handoff.up.sql
0008_runtime_reply_target.up.sql
0009_runtime_reply_correlation.up.sql
0010_agent_app_canary.up.sql
0011_reply_trace_parent.up.sql
0012_runtime_capabilities.up.sql
0013_execution_queue.up.sql
0014_wecom_aibot_channel.up.sql
0015_runtime_attachments.up.sql
0016_runtime_reply_media.up.sql
0017_agent_chain.up.sql
```

执行约定如下：

1. 在干净 PostgreSQL 实例上使用一个事务执行完整文件；失败时整个 schema 和权限变更回滚。
2. 迁移开始固定 `search_path` 为 `pg_catalog, public, pg_temp`，所有函数体对业务表使用 `public.`
   限定名；不依赖连接池或客户端会话的隐式 search path。
3. 在干净实例中 migration 会创建缺失的 `NOLOGIN` 受控角色；生产部署也可以在执行前预置
   数据库 owner、migration owner、`tenant_admin_writer` 和 `tenant_app_writer`。这些角色不
   属于普通请求连接池，migration 不把 owner 权限继承给运行时角色。
4. `0002` 为 Tenant、Model、Backend、Agent App/Revision、Channel Binding 以及 Outbox
   写入提供完整的 `SECURITY DEFINER` entry point；函数先撤销 `PUBLIC` 的默认 `EXECUTE`，
   再只授予 `tenant_admin_writer`。Worker 只能消费控制平面下发的固定快照，不能枚举根表
   或草稿。
5. SQL 文件不包含 token、API key、DSN、密码、运行时客户端或测试 Secret。`secret_ref` 是
   唯一允许进入控制面配置的凭据引用，且只按租户作用域解释。

Migration 的对象顺序为：

```text
tenant
  ├── agent_app
  │     ├── agent_app_revision
  │     │     └── agent_app_revision_tool
  │     ├── model_profile
  │     ├── backend_profile
  │     │     └── backend_profile_binding
  │     └── channel_binding
  └── tenant_status_change_outbox / profile change outboxes
```

创建对象时可以先建立不带默认指针的根表，再按上述顺序追加复合外键；默认 App、默认
Backend Profile 必须在最后通过同租户复合外键绑定。Backend Profile 的“至少一个 binding”
和 active 必须有 `session` binding 是延迟约束，发布/配置替换必须在一个事务内完成，不能
分别提交根行和 binding 集合。

## Model Profile 持久化形状

Model Profile 的领域代码已经限制 provider、model、endpoint、option 和 generation 参数；
PostgreSQL 只需要保存规范化后的无密钥结果。目标根表如下，`options` 和 `generation` 不是
开放 JSON 扩展口，Repository 写入前必须用受信 Provider Catalog 重新校验：

```sql
CREATE TABLE model_profile (
    tenant_id       TEXT NOT NULL REFERENCES tenant(tenant_id),
    profile_id      TEXT NOT NULL
                    CHECK (profile_id ~ '^mp_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    profile_key     TEXT NOT NULL
                    CHECK (profile_key ~ '^[a-z][a-z0-9-]{1,63}$'),
    display_name    TEXT NOT NULL
                    CHECK (length(btrim(display_name)) BETWEEN 1 AND 200),
    description     TEXT NOT NULL DEFAULT ''
                    CHECK (length(description) <= 2000),
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'suspended', 'disabled')),
    schema_version  INT NOT NULL DEFAULT 1 CHECK (schema_version = 1),

    provider        TEXT NOT NULL,
    model           TEXT NOT NULL,
    endpoint        TEXT NOT NULL DEFAULT '' CHECK (length(endpoint) <= 2048),
    options         JSONB NOT NULL DEFAULT '{}'::jsonb
                    CHECK (jsonb_typeof(options) = 'object'),
    secret_ref      TEXT NOT NULL DEFAULT '' CHECK (length(secret_ref) <= 256),
    generation      JSONB NOT NULL DEFAULT '{}'::jsonb
                    CHECK (jsonb_typeof(generation) = 'object'),

    content_digest  TEXT NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
    version         BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, profile_id),
    UNIQUE (tenant_id, profile_key)
);
```

`options` 的 value 必须全部是字符串，`generation` 只能包含已支持的字段；SQL 约束无法
替代 Provider Catalog，所以两个 JSONB 列只能由受控 Repository/函数写入。Profile identity
不可变，完整配置替换递增一次 `version`、更新 `updated_at`、重新计算摘要，并和
`model_profile_change_outbox` 在同一事务提交。disabled 是终态；active/suspended 的状态
迁移使用固定 `search_path` 的受控函数。

## Repository 事务契约

SQL Repository 是现有 InMemory Repository 的持久化实现，不向 Gateway 暴露 SQL 错误或底层
provider 细节。每个方法都必须把 `context.Context` 传给 `QueryContext`、`ExecContext`、
`BeginTx` 或等价的 pgx 方法；取消后不再等待连接、锁或查询结果。

| 领域操作 | 事务和锁边界 | 成功结果 |
| --- | --- | --- |
| Tenant 配置/状态 | 先锁 Tenant；校验 expected version、状态迁移和默认对象同租户归属 | 新 Tenant 快照与状态 Outbox 原子提交 |
| App metadata/draft | 锁 `(tenant_id, app_id)`；草稿另外校验 `draft_version` | 防御性 App/Revision 副本 |
| App publish/rollback | App → Revision → 同租户引用按固定顺序加锁；发布冻结 Revision 并移动 current pointer | 新 App、Revision 和 ChangeEvent 原子可见 |
| Model/Backend/Binding 配置 | 锁租户作用域的根行；完整替换只提交一次 version 增量 | 规范化对象和 ChangeEvent |
| 生命周期迁移 | 锁根行后先分类 disabled/conflict，再检查允许的状态迁移 | 状态变更与 Outbox 同一事务 |
| 候选查询 | 只用 `(channel, public_route_key_digest)` active 索引 | 不含 Tenant/App/Secret 的候选上下文 |

实现共同遵守：

- 每个查询显式带租户条件；任何按 key 的查询都不能省略 `tenant_id`。
- 跨对象引用使用复合外键和事务内状态检查，不能靠 key prefix 或调用方传入的 boolean。
- `sql.ErrNoRows` 和唯一冲突映射到领域的 `ErrNotFound`、`ErrDuplicateKey`、`ErrConflict` 等
  可识别错误；其他数据库错误只返回稳定的存储错误类别。
- 扫描出的 map、slice、pointer、JSON 和时间值在返回前复制；调用方修改返回值不能改变缓存
  或 Repository 内部状态。
- 发布、回滚、状态迁移和相关审计/Outbox 不能“先提交主表、再尽力写事件”。
- 错误、日志、trace、ExecutionPlan、Factory cache key 和运行时客户端都不得包含 Secret 值。

## Bootstrap、readiness 与 shutdown

启动装配使用显式 `BootstrapConfig`，配置来自进程配置文件、环境变量解析结果或测试构造
器；请求 body、header、Message 或未验证的 route 不能决定 Tenant/App/Profile。配置对象至少
包含：

- PostgreSQL DB/Pool 和连接关闭责任；
- Tenant、App、Model、Backend、Channel Binding 的 SQL Repositories；
- Model/Backend Provider Catalog；
- SecretResolver、ModelFactory 和 Session capability；
- API Authenticator、PlanResolver、RunnerRegistry、Dispatcher 和 HTTP 参数；
- readiness 所需的数据库 ping/迁移版本检查和可选的资源关闭超时。

当前 `cmd/trpc-service` 的生产入口由 `bootstrap.NewFromEnvironment` 装配真实图，而不是
启动一个永久不可用的空图。它要求 `TRPC_POSTGRES_DSN`、`TRPC_API_TOKEN`、
`TRPC_TENANT_ID`、`TRPC_APP_ID` 和 `TRPC_MODEL_API_KEY`；可选的
`TRPC_MODEL_PROVIDER`、`TRPC_MODEL_NAMES`、`TRPC_MODEL_ENDPOINT_HOSTS`、
`TRPC_MODEL_SECRET_REF` 用于建立受信 Model Catalog 和 SecretRef 映射。缺少必需配置时
进程在绑定 HTTP 端口前失败；`NewUnavailable` 只用于无外部依赖的测试装配。Session capability
根据显式配置使用 PostgreSQL、Redis 或 InMemory，并在 readiness 中报告实际后端。

装配顺序固定为：

```text
validate explicit config
  → verify database + required catalog/capability
  → construct SQL repositories
  → construct PlanResolver
  → construct agent/runnerfactory.NewRuntimeRunnerRegistry
  → construct Dispatcher
  → construct HTTPHandler with real Dispatcher/Authenticator
  → expose readiness
```

`/healthz` 只表示进程存活；`/readyz` 必须在 DB、五类 Repository、Catalog、Secret/Model/Session
capability、Resolver、Registry、Dispatcher 和 Handler 都可用前返回 503。依赖失效或开始关闭
时 readiness 立即降为 false，不能让空的 `HTTPConfig` 误报 200。Bootstrap 测试使用 fake
SecretResolver、fake ModelFactory 和 InMemory Session，不需要真实模型供应商 Secret。

关闭顺序保持当前 Gateway 语义：先把 Handler 置为 draining，再调用 `http.Server.Shutdown`
等待 HTTP 摘流，然后 bounded close RunnerRegistry，最后关闭 Bootstrap 自己拥有的 DB/Pool
和其他资源。借用的 Session、Resolver、Factory 由其声明的 owner 关闭；超时返回稳定的关闭
错误并交给上层处理，不能无限等待 Runner lease 或数据库连接。

## 验证矩阵与交付证据

本页契约对应的验证入口如下；它们是 Issue #37 的纵向实现证据：

- `migrations/migration_test.go`：干净 PostgreSQL migration、权限、跨租户 FK、published
  current pointer、延迟 binding 和嵌套凭据键检查；
- `trpcservice/internal/testsupport/postgres/integration_test.go`：五类 SQL Repository 的租户作用域、
  生命周期、发布、候选消费、Outbox、Context 取消和深拷贝路径；CI 使用独立 PostgreSQL
  服务执行；
- `scripts/coverage.sh`：使用单次原生 Go `-coverpkg` profile 执行各包单测及上述跨领域集成测试，
  让 Codecov 与本地 `go tool cover` 一致地统计五个 Repository 的跨包执行路径，且不会重复初始化
  CI PostgreSQL schema；
- `trpcservice/bootstrap/bootstrap_test.go`：真实 Resolver/Registry/HTTPHandler 组装、
  readiness 503→200 和 shutdown gate；
- 生产代码中的 `codec.go`、受控 SQL 函数和稳定错误映射保证 Secret 不进入运行时对象或底层
  数据库错误。

控制面迁移、Repository、bootstrap、readiness、Admin API、runtime storage、Outbox、Secret
Resolver、Channel candidate routing 和重启恢复共同形成可运行的 PostgreSQL 纵向链路；其
验证由本页列出的单测、live smoke、fault-injection E2E 和部署 golden path 覆盖。
