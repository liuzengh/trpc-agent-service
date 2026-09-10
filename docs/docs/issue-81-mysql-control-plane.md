# Issue #81：MySQL 控制面 Repository 与迁移契约

本页是 Issue #81 的实现与验收边界。它把 MySQL 适配器与现有
PostgreSQL 控制面之间的可观察行为固定下来：租户、Agent App/Revision、Model
Profile、Backend Profile 和 Channel Binding 的领域接口不变，租户隔离、乐观锁、
生命周期、Outbox 事件和错误分类也不因数据库驱动切换而改变。

## 已交付能力

目标是让同一套控制面 API 可以选择 PostgreSQL 或 MySQL，并满足：

- 五类控制面 Repository 都实现各自领域接口；
- 所有租户作用域的控制面读写都带显式 `tenant_id`，跨租户引用由复合主键/外键和
  Repository 双重校验；验签前的 `CandidateIndex.LookupCandidates(channel, digest)` 是有意保留
  的全局候选发现例外，此时租户身份尚未可信，查询只能返回不含 Tenant/Secret 的短期上下文，
  验签后必须重新执行租户作用域读取；
- 发布、回滚、状态迁移和配置替换在一个事务内提交，事件 Outbox 与主表原子可见；
- 行锁加乐观版本检查，重复写入、版本冲突、唯一冲突和非法状态迁移映射到稳定领域错误；
- Bootstrap 根据受信配置选择驱动，重启后能从同一数据库重新发现控制面对象；
- SQL、DSN、密码和 Secret 值不进入领域错误、日志、trace、执行快照或缓存键。

运行时 Session/Memory/Knowledge/Artifact、Redis 迁移、无状态 Worker、Dashboard、Secret
Resolver 和 Admin API 均通过对应模块消费本页控制面契约。

## 驱动与配置边界

Repository 继续使用 `database/sql` 的 `*sql.DB`，因此调用方可以复用连接池、Context
取消和现有的所有权约定。MySQL 适配器由独立的 `tenant/mysql`、`app/mysql`、
`model/mysql`、`backend/mysql` 和 `channels/mysql` 包提供；实现细节不泄漏到领域接口。

生产 Bootstrap 使用显式驱动选择：

```text
TRPC_CONTROL_PLANE_DRIVER=postgres | mysql
TRPC_POSTGRES_DSN=postgres://...       # driver=postgres
TRPC_MYSQL_DSN=user:password@tcp(host:3306)/db?parseTime=true&charset=utf8mb4              # app account
TRPC_MYSQL_MIGRATION_DSN=migrator:password@tcp(host:3306)/db?parseTime=true&charset=utf8mb4  # migration account
```

默认仍为 PostgreSQL，已有 `TRPC_POSTGRES_DSN` 配置和 API 保持兼容。`mysql` 模式必须
同时提供应用账号的 `TRPC_MYSQL_DSN` 和仅用于迁移的
`TRPC_MYSQL_MIGRATION_DSN`；Bootstrap 先在独立迁移连接上 Apply + Verify，再以应用连接
读取当前账号、数据库和权限信息并装配 Repository；迁移账号负责 schema/trigger 校验，应用账号不
需要 `TRIGGER` 元数据权限。应用账号必须在当前数据库控制面 14 张表上逐表具备完整的
`SELECT/INSERT/UPDATE/DELETE`（缺少任意一项也 fail-closed），除此之外不得有任何表级权限；
全局、schema 级、列级、routine/`EXECUTE`、`PROXY`、启用角色的越权权限和 grant option 都会使 Bootstrap
fail-closed；两个 DSN 的 `DATABASE()` 也必须相同且非空。缺失驱动、任一 DSN、迁移版本、必需权限或
连接 Ping 失败时，在绑定 HTTP 端口前 fail-closed。DSN 只存在于 Bootstrap 的短生命周期
配置中，不能写入运行时快照、Repository 错误或 telemetry。测试可以传入已经打开的
`*sql.DB`，不要求 Repository 自己关闭借用的连接池；Bootstrap 只有在 `OwnDB=true` 时
负责关闭它。

## SQL 语义映射

| PostgreSQL 契约 | MySQL 8.0.19+ 实现 | 兼容约束 |
| --- | --- | --- |
| `$1` 参数 | `?` 参数 | 只由 MySQL 包生成，禁止拼接用户输入 |
| `public.table` | 当前数据库中的 `table` | DSN 选定的数据库是唯一控制面 schema |
| `JSONB` | `JSON` | 写入前用领域校验，读取后做完整 JSON 解码 |
| `TIMESTAMPTZ` | `DATETIME(6)` | 应用层统一使用 UTC；DSN 启用 `parseTime=true` |
| `BIGINT GENERATED ...` | `BIGINT AUTO_INCREMENT` | 事件 ID 在事务内由 `LastInsertId` 读取 |
| `RETURNING` | `INSERT/UPDATE` 后按同一事务重新 `SELECT` | 不暴露中间状态 |
| `FOR UPDATE` | `SELECT ... FOR UPDATE` | 仅在事务内使用；Context 取消释放锁 |
| PostgreSQL advisory lock | `GET_LOCK`/`RELEASE_LOCK` | 迁移历史锁和首租户锁是两个独立命名空间；各自固定在同一 `sql.Conn`，并在提交/回滚后显式释放 |
| `SECURITY DEFINER` 函数 | 最小数据库账号 + 事务内受控 DML | 领域校验和显式租户谓词不可省略 |

MySQL 连接必须使用 InnoDB、明确的 `READ COMMITTED` 隔离级别和 UTC session time
zone（DSN 强制 driver system variable `time_zone='+00:00'`，`loc=UTC` 只负责 Go 解码），
以匹配现有 PostgreSQL `database/sql` helper 的事务契约；不得用未界定的“或更严格”
替代这个可测试的级别。迁移脚本不依赖 `public` schema、PostgreSQL 函数、正则运算符或
`RETURNING`。
JSON 字段仍只接受受信 Repository 产生的无密钥对象；`secret_ref` 是唯一可以持久化的
凭据引用。

## Repository 事务与隔离契约

五个适配器的公开方法与 PostgreSQL 版本完全相同。每个方法都先检查 `ctx.Err()`，再
把 Context 传给 `BeginTx`、`QueryContext`、`QueryRowContext` 和 `ExecContext`。

### Tenant

- `Create` 在同一事务插入根行并重新读取规范化快照；MySQL `CreateFirst` 使用同一
  `sql.Conn` 上的 `GET_LOCK('trpc-agent-service:first-tenant', timeout)`，提交或回滚后
  必须释放锁。
- `UpdateConfiguration`、`TransitionStatus` 先按 `tenant_id ... FOR UPDATE` 读取，比较
  `version`，再以 `version = expected` 更新；状态 Outbox 与根行同一事务。
- `Count` 只用于首次 Admin 授权边界，错误映射为稳定 `ErrStorage`。

### Agent App/Revision

- App 根行先锁定；创建 Revision 的 `MAX(revision)+1` 在该锁内计算，避免同一 App 并发
  分配相同 revision。
- Draft 更新只允许 `state=draft`，发布前同时锁 App、Draft 和 Tenant；发布事务冻结
  Revision、移动 current pointer 并写入 Change Outbox。
- 回滚只能指向同租户已有的 published Revision；已发布内容不可更新或删除。

### Model 与 Backend Profile

- 完整配置替换先执行领域 `Prepare*Change`，再锁定 Profile 行并比较版本。
- Backend binding 使用单独表，替换时在同一事务删除旧集合、插入新集合并写 Outbox；读取
  后重新通过受信 Provider Catalog 校验，不能把未知 provider 带入执行快照。
- MySQL 没有 PostgreSQL 的 deferred constraint trigger；因此 Backend `Create` 在同一
  事务内先以 `disabled` 写入根行，再写入完整 binding 集合，最后切到请求状态。这个
  是存储引擎内部的 provisioning 顺序，Repository 返回的状态、版本、Outbox 事件和
  非法迁移结果仍与 PostgreSQL 契约一致；绕过 Repository 的直接 DML 不属于受支持的
  控制面写入口，应用账号也不拥有 schema DDL 权限。

### Channel Binding

- Binding 的稳定身份、`secret_ref` 和非密协议配置按租户保存；更新和状态迁移使用
  `tenant_id + binding_id + version`。
- active provider account 的唯一 owner 约束是 `(channel, provider_account_id)`，只对
  `status='active'` 生效；候选发现另建 `(channel, public_route_key_digest)` 索引，不能把 route
  digest 加入 active-owner 唯一键。MySQL 8.0.19+ 用一个 inactive 为 `NULL` 的 stored generated
  column（例如 `active_provider_account_id`）加唯一键表达 PostgreSQL 的 partial unique index。
  候选索引只返回不含 Tenant/Secret 的短期上下文。
- Candidate capability 继续是进程内一次性消费；重启后由持久化 version/digest 校验
  fail-closed。候选缓存不是跨节点状态，也不能替代数据库租户隔离。

所有数据库错误先在 MySQL storage helper 中按 `1062 duplicate`、`1213/1205
conflict/deadlock`、`1451/1452 foreign-key`、`1264/1406 invalid` 等类别归一化；错误
文本、连接地址和驱动诊断不会向 Gateway 暴露。`context.Canceled` 与
`context.DeadlineExceeded` 原样保留。

## MySQL Migration 与权限

MySQL 维护独立的有序 migration 集合和 `schema_migrations` 历史表，版本号与 PostgreSQL
保持一致但摘要分别计算。由于 MySQL 8.0.19+ 的表/索引 DDL 会隐式提交，迁移协议是
forward-only、幂等且可恢复的，不能承诺通过用户 `ROLLBACK` 撤销 DDL。每次迁移：

1. 在固定连接上执行独立的 migration-history `GET_LOCK`，检查历史摘要和连续版本；
2. 将当前版本以 `applying` 状态记录，再逐条执行幂等 DDL；每条语句完成后更新可恢复
   的 checkpoint，成功后写入 `applied`、文件名、SHA-256 和 UTC 时间；
3. 失败时保留 `applying/failed` 状态和稳定 `ErrMigration`，释放锁；下次启动必须先
   对照对象/摘要恢复或重试该版本，禁止跳过失败版本；
4. `Verify` 只读校验，不创建或修改任何对象；必须能识别 DDL 已落地但历史未完成的重启场景。

首租户创建使用另一个独立的 `GET_LOCK('trpc-agent-service:first-tenant', timeout)`，在
固定连接上串行化“空控制面检查 + 创建”，提交或回滚后显式释放；它不能与 migration-history
锁复用生命周期或命名空间。

迁移至少创建 `tenant`、`agent_app`、`agent_app_revision`、`agent_app_revision_tool`、
`model_profile`、`backend_profile`、`backend_profile_binding`、`channel_binding` 及各自
Change Outbox 表，并保留 `runtime_*`/audit 表所需的同租户复合键形状。表和索引使用
`utf8mb4`、`utf8mb4_bin`、InnoDB；`tenant_id`、各类 `*_id`、`*_key`、
`provider_account_id`、route digest 和所有参与唯一键/外键/精确查找的列在列级固定该 binary
collation（不得依赖服务器默认的 `utf8mb4_0900_ai_ci`）。外键显式包含 `tenant_id`。迁移账号与
应用账号分离；应用连接必须完整拥有控制面 14 张表的表级 DML，且不拥有任意额外表、全局、
schema、列级、routine/`EXECUTE` 或 `PROXY` 权限，也不拥有 schema DDL 权限。运行时
Session/Memory/Knowledge/Artifact 使用各自 runtime provider 和账号边界，控制面应用账号仅用于
控制面表 DML，不承担运行时存储迁移权限。

本方案不依赖 MySQL 存储例程的 `SQL SECURITY DEFINER` 边界，而是由三层共同保证：

1. 应用 Repository 的领域校验、显式租户谓词和事务锁；
2. 数据库账号只授予需要的表/列权限；
3. migration/Bootstrap 在启动时验证版本和必需索引，不满足即 readiness=false。

## 验证矩阵

| 层级 | 证据 |
| --- | --- |
| 契约 | 五类 MySQL Repository 编译实现领域接口，零值、nil DB、取消和错误分类测试 |
| SQL 单元 | `sqlmock` 覆盖 `?` 参数、事务提交/回滚、版本冲突、重复键、跨租户谓词和防御性副本 |
| Migration | `MYSQL_CONTROL_PLANE_MIGRATION_TEST_DSN` 使用 migration 账号指向干净 MySQL 8.0.19+ 服务，执行全量 migration、Verify、重启 Verify；摘要/大小写/DDL 后失败恢复由 sqlmock 契约覆盖 |
| Repository 集成 | `MYSQL_CONTROL_PLANE_TEST_DSN` 使用仅 DML 的应用账号执行五类 Repository 的创建、更新、发布、Backend 生命周期、候选消费和双租户隔离；migration 账号通过独立 DSN 完成该数据库初始化 |
| 并发/race | `go test -race ./...` 在 CI 的 MySQL 8.0.19+ 服务上运行 live smoke 与 SQL 契约测试；optimistic-lock、同 App revision、候选消费和 Context 取消由 Repository 单测覆盖 |
| Bootstrap | `TRPC_CONTROL_PLANE_DRIVER=mysql` 选择 MySQL；双 DSN 账号/数据库分离校验、14 张表逐表完整 DML 白名单（缺失或额外权限、routine/`EXECUTE`、`PROXY`、启用角色和 grant option 均拒绝）、未知驱动、缺 DSN、迁移失败和重启 rediscovery 由 Bootstrap/sqlmock 契约 fail-closed，live job 验证受限应用账号可运行 Repository |

CI 提供独立 MySQL 8.0.19+ 服务运行 migration、Repository 和 race smoke；PostgreSQL 现有 job
共同复用控制面契约和验收矩阵。

## Issue ledger

| 条目 | 状态 |
| --- | --- |
| Tenant、Agent、Model、Backend、Channel MySQL Repository | 已完成：`trpcservice/*/mysql` |
| 事务、乐观锁、生命周期、Outbox 和租户隔离语义 | 已完成：事务/复合键/候选消费集成验证 |
| MySQL migration、摘要校验、权限和重启恢复 | 已完成：`migrations/mysql.go` 与 MySQL 8.0.19+ migration |
| Bootstrap 驱动选择与错误脱敏 | 已完成：`TRPC_CONTROL_PLANE_DRIVER` 与 fail-closed 测试 |
| MySQL unit/integration/race 测试及 CI 服务 | 已完成：sqlmock 失败/恢复契约、MySQL 8.0.19+ live migration/repository smoke、双账号权限初始化与 race job |

## Issue #81、README 与验收对照

| GitHub Issue #81 验收项 | README 对应项 | 当前证据 |
| --- | --- | --- |
| Tenant、Agent、Model、Backend、Channel Repository 支持 MySQL | 多租户控制面中的 MySQL Repository 条目 | 五个 `trpcservice/*/mysql` 包、接口编译断言、sqlmock 与 CI live smoke |
| 事务、乐观锁、生命周期、租户隔离与 PostgreSQL 契约一致 | 控制面隔离/并发/生命周期条目 | 共享领域校验、复合键/显式租户谓词、Repository 单测；MySQL Backend 的 disabled provisioning 仅是引擎内部顺序 |
| Migration、restart、race 运行在 MySQL 服务 | CI 与测试清单 | CI 两个 MySQL 8.0.19+ 服务、migration/repository 双账号、重启 Verify 与 `go test -race ./...` |
| Bootstrap 选择适配器且不泄露 Secret/API | Bootstrap、readiness 和错误脱敏条目 | 双 DSN、应用权限/身份校验、fail-closed 路径与错误脱敏测试 |
