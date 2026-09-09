# Session 后端

`trpcservice/sessionbackend` 提供 InMemory、PostgreSQL 和 Redis 的上游 Session Service 工厂；`storagebundle` 提供租户 BackendProfile 路由。默认进程 profile 为 InMemory；`postgres` profile 将控制面、Session Pin 和默认 Session 放在同一 PostgreSQL schema，支持持久化和租户动态后端。见 [§8 进程存储 Profile](#8-进程存储-profile)。

Redis 可以作为租户 Session 后端，但不作为整套控制面的进程 profile；配置与 Pin 仍由 PostgreSQL 提供。能力与一致性取舍见[多后端设计](storage-and-consistency.md)。

## 1. 模块组成

| 组件 | 路径 | 说明 |
| --- | --- | --- |
| 后端工厂 | `trpcservice/sessionbackend/sessionbackend.go` | `New(Config) (session.Service, error)`，三种后端 |
| 单元测试 | `trpcservice/sessionbackend/sessionbackend_test.go` | 不触网，随 `go test ./...` 默认执行 |
| 集成测试 | `trpcservice/sessionbackend/integration_test.go` | 默认跳过，需显式开关 |
| 本地依赖 | `deploy/docker-compose.session.yml` | PostgreSQL + Redis，仅监听 `127.0.0.1` |

工厂刻意做得很小：只有后端名、该后端的一个连接串，以及共享服务器所需的命名空间开关。其余上游选项保持默认。租户 Profile、ProfileRepository、存储捆绑、SecretRef 授权解析和生命周期实现在上层 `trpcservice/storagebundle` 与 `tenant/postgres`，没有塞回这个低层工厂；完整后端能力矩阵与注册扩展仍是后续工作。这样 `sessionbackend` 继续只回答“怎样构造一个上游 Session Service”，不会变成一份会漂移的上游选项副本。

## 2. 依赖版本与 Go 版本要求

```
trpc.group/trpc-go/trpc-agent-go                 v1.11.2   （核心）
trpc.group/trpc-go/trpc-agent-go/session/postgres v1.11.0   （独立子模块）
trpc.group/trpc-go/trpc-agent-go/session/redis    v1.11.0   （独立子模块）
```

两个 Session 子模块是独立 Go module，各自带入间接依赖 `storage/postgres v0.8.0`、`storage/redis v0.0.3`、`jackc/pgx/v5 v5.7.2`、`redis/go-redis/v9 v9.11.0`。

### 2.1 Go 工具链下限

Go >= 1.24.1 是依赖约束：`storage/redis@v0.0.3` 声明该 go directive，主模块不能低于依赖的下限。依赖图通过 MVS 选定版本，CI 与部署构建环境使用相同工具链要求。

## 3. 兼容性验证

两个子模块声明的核心依赖是 `trpc.group/trpc-go/trpc-agent-go v0.2.0`，并带有指向 `../../` 的 `replace`。**这两条信息在本仓库里都不生效**：依赖模块的 `replace` 只对它自己作为主模块时有效，而 MVS 会选中本仓库直接依赖的 v1.11.2。也就是说，模块图不提供任何"子模块 v1.11.0 与核心 v1.11.2 兼容"的保证——声明的 v0.2.0 下限没有意义。

风险更进一步：两个子模块跨模块引用了核心的内部包。

```
trpc.group/trpc-go/trpc-agent-go/internal/session/sqldb   （postgres、redis 都用）
trpc.group/trpc-go/trpc-agent-go/internal/session/hook    （redis 用）
```

Go 的 internal 规则按导入路径前缀判定而非按模块判定，所以这种引用合法。但 `internal` 包不承担任何兼容承诺，核心版本变动可以在不违反 semver 的前提下破坏子模块编译。

**结论：唯一的兼容性证据是编译加测试通过，没有版本号层面的保证。** 因此：

- `trpcservice/sessionbackend/sessionbackend_test.go` 顶部有三条接口断言，把 `*inmemory.SessionService`、`*postgres.Service`、`*redis.Service` 钉死在 `session.Service` 上。上游改动方法集时，构建在这里失败，而不是在调用点或运行时失败。
- 每次升级核心版本，必须重新执行 `go build ./...` 与本包集成测试，不能只看 semver。

已验证：核心 v1.11.2 + 子模块 v1.11.0 组合下，`go build ./...`、`go vet ./...`、`go test -race ./...` 全部通过，集成测试对真实 PostgreSQL 16 与 Redis 7 通过。

## 4. 三种后端的语义差异

`session.Service` 是同一个接口，但三种实现的可观察行为并不一致。以下差异全部由 `integration_test.go` 针对真实服务验证，不是读代码推断的。

### 4.1 差异总表

| 行为 | InMemory | PostgreSQL | Redis |
| --- | --- | --- | --- |
| `GetSession` 会话不存在 | `(nil, nil)` | `(nil, nil)` | `(nil, nil)` |
| 第二次 `CreateSession` | 返回已有会话 | **返回错误** | 返回已有会话（含历史） |
| `DeleteSession` | 真删 | **默认软删，行保留** | 删除 key |
| 进程重启后数据 | 丢失 | 保留 | 保留 |
| `Close` 重复调用 | 安全 | 安全（`sync.Once`） | 安全（`sync.Once`） |

**`GetSession` 返回 `(nil, nil)`** 是三者一致的：会话不存在不是错误。调用方只检查 `err != nil` 就会对 nil 解引用。这是最容易写错的一条。

**第二次 `CreateSession` 的分歧**是最危险的一条，因为它在接口层完全不可见。PostgreSQL 返回 `session already exists and has not expired`；Redis 返回已存在的会话，连同全部历史事件。任何依赖其中一种形状的代码，在换后端时都会坏掉。平台层如果需要"创建或获取"语义，必须自己在上层实现，不能依赖后端行为。

### 4.2 PostgreSQL

**自动建表。** `NewService` 在构造时连接数据库并创建 6 张表（`WithSkipDBInit(true)` 可关闭，本工厂不使用该选项，保持自动建表）：

```
session_states  session_events  session_track_events
session_summaries  app_states  user_states
```

`WithTablePrefix` 会给每张表加前缀（不以 `_` 结尾时自动补 `_`），`WithSchema` 指定 schema。**schema 必须事先存在**——上游只建表，不建 schema。集成测试使用前缀 `spike`，实测建出 `spike_session_states` 等 6 张表。

这意味着服务账号在首次启动时需要 DDL 权限。生产环境若由迁移工具管理表结构，应改用 `WithSkipDBInit(true)`，当前工厂保持自动建表行为。

**默认软删。** `softDelete` 默认为 `true`，`DeleteSession` 只写 `deleted_at`。会话从读路径消失（`GetSession` 返回 `(nil, nil)`），但行和事件行都留在库里。实测：集成测试删除 2 个会话后，`spike_session_states` 仍有 2 行（`deleted_at` 均非空），`spike_session_events` 仍有 2 行。**软删的存储不会自己回收**，这是一条容量风险，不是可以忽略的实现细节。

**只持久化有效且非 partial 的 Response 事件。** 见 4.4。

### 4.3 Redis

**默认 `CompatModeLegacy`。** `defaultOptions.compatMode = CompatModeLegacy`，即"读时回退到 zset，但不双写"。这是上游为 zset→hashidx 存储迁移准备的兼容档位，新部署拿到的就是这个默认值。本工厂不改它——改动这个开关等于替上游做存储格式决策，应该在真正需要迁移时单独评估。

**`WithKeyPrefix` 不被上游校验。** 前缀原样拼进每个 key。因此本工厂自己校验：只允许 `[A-Za-z0-9_.:-]`，最长 32 字符。拒绝空格（会让 key 空间变形）和 `{}`（Redis Cluster 的 hash tag，会静默改变分片归属）。集成测试用 `spike:<8位随机>` 做每次运行的隔离，并验证了两个不同前缀的服务看不到彼此的会话。

**Redis 不需要预置结构**，客户端也是懒连接，所以一个指向空地址的 URL 通常不会让 `New` 失败，而是在第一次 Session 调用时才报错。

### 4.4 事件必须是有效的 Response，且必须包含 user 消息

有两条上游规则决定"写进去的事件下次还在不在"，任何忽略其中一条的测试都会在空会话上通过：

1. **事件只有在 `Response` 非 nil、非 partial 且带 payload 时才被记录。** `session.Session.UpdateUserSession` 的条件是 `event.Response != nil && !event.IsPartial && event.IsValidContent()`，对所有后端生效，PostgreSQL 的持久化路径再校验一次。
2. **`session.Session.ApplyEventFiltering` 保证结果里至少有一条 user 消息，否则清空整个列表。** 它先从首条 user 消息开始截断（此前的事件被丢弃），找不到时回头从原列表补一条最后的 user 消息，仍然找不到就把 `Events` 置空。所以只追加 assistant 事件的会话，`AppendEvent` 返回 nil，但读回来是空的。

第 2 条在 `TestAssistantOnlySessionReadsBackEmpty` 中被固定为事实（用 InMemory 后端即可复现，无需外部服务）。所有集成测试因此都使用"user 消息 + assistant 回复"的完整一轮作为夹具——这本来也是真实对话的形状。

## 5. 工厂本身的约定

### 5.1 默认后端是 InMemory

`DefaultConfig()` 返回 `BackendInMemory`。这是唯一不需要外部服务的后端，`cmd/trpc-service` 的默认启动路径不变，空机器上 `./build.sh && ./start.sh` 仍然能跑。

`Config.Backend` 为空是配置错误，不是"要默认值"——想要默认值就调 `DefaultConfig()`。这条选择是为了让配置拼写错误在启动时就暴露，而不是静默退化成内存后端、跑到重启丢数据时才被发现。

### 5.2 Validate 必须先于上游选项执行

上游的 `WithTablePrefix` 和 `WithSchema` 走的是 `sqldb.MustValidateTablePrefix` / `MustValidateTableName`，**校验失败时 panic 而不是返回 error**。所以 `Config.Validate()` 必须在 `New` 把值交给上游之前拒绝非法输入，否则一个配置笔误会变成进程崩溃。

本包的校验比上游更严：正则相同（`^[A-Za-z_][A-Za-z0-9_]*$`），但长度上限取 32 而非上游的 64。原因是 PostgreSQL 标识符在 63 字节处截断，而最长的上游表名 `session_track_events` 已占 20 字节；一个上游接受的 64 字符前缀，截断后仍可能撞名。

`Validate` 不联网。通过校验的配置仍然可能连不上。

### 5.3 PostgreSQL 空 DSN 必须被拒绝

上游 `NewService` 的连接优先级是 DSN → 直连参数 → 实例名 → **回退到默认连接串**，而默认串是 `host=localhost port=5432`。也就是说，一个空 DSN 不会报错，而会静默连上本机的 5432。生产环境里这可能是另一个真实数据库。

因此 `PostgresConfig.validate()` 明确拒绝空白 DSN。Redis 的空 URL 同理被拒绝。

### 5.4 错误脱敏

`Scrub` 处理完整 DSN 及其中可识别的密码拼写，包括 URL 编码和 libpq 形式。pgx 解析失败可能把未编码 `/` 之前的密码片段误当端口回显，因此不能仅依赖 `url.Parse` 成功或驱动自带脱敏。已知短密码片段按错误中的端口引用位置处理，避免破坏正常诊断文本。

保证与限制：

- 只处理传入的 error 文本，不能拦截驱动自行输出的日志、metrics、trace 或 stderr。
- 返回全新错误，不保留可通过 `Unwrap` 找回的原始凭据；相应地不保留驱动错误类型。
- 低层 Scrub 保护密码，用户名、主机和库名仍可作为诊断信息；上层动态 Factory 还会整体替换连接值。
- 不认识的驱动改写形式不在既有保证内；已知解析错误由回归测试固定。
- `Config.Describe()` 只报告连接串存在与否，不输出连接内容。

### 5.5 Close 所有权

`New` 返回的 service 归调用方所有，**调用方必须且只需 Close 一次**，且必须在所有共享它的 Runner 停止之后。本包不 Close 它返回过的任何 service，也不持有引用。

两个持久化后端的 `Close` 都用 `sync.Once` 保护并返回 nil，重复调用安全——`TestIntegrationCloseIsIdempotent` 对真实服务验证了这一点。Resolver 的关闭路径依赖这条性质（正常关闭一次、defer 清理再关一次）。

## 6. 一致性边界

Session 后端不提供存储写入 fencing。当前 [Run 租约](session-lease.md)只限制 Run 入口；`Lease.Fence()` 是观测句柄，不参与写入准入。

上游 `WithAppendEventHook` 与实际后端写入不是原子的，当前也未接入该 hook。严格单写者需要 Redis Lua 比较 token 后写入，或 SQL 条件更新；非原子 hook 不能实现过期 writer 的原子拒绝。

### 6.1 持久 Session 与配置 Pin 的共同生命周期

| 进程 profile | 控制面 | Revision Pin | 默认 Session |
| --- | --- | --- | --- |
| `inmemory` | 内存 | 内存 | 内存 |
| `postgres` | PostgreSQL | 同库持久化 | 同库持久化 |

进程不允许只持久化默认 Session 而丢失配置或 Pin。这样重启后不会将旧会话静默绑定到新默认 Revision。租户动态 PostgreSQL/Redis Profile 的真相源和 Pin 仍由进程 PostgreSQL 提供；动态 InMemory 仅适用于明确接受历史易失的单 Worker 组合，多 Worker 组合由 Factory 拒绝。

共享同一 schema 的 Worker 使用 Redis 入口租约协调，另一 Worker 收到 `409 session_busy`。TTL 接管不等于存储 fencing；健康续约可无限持有租约，故障转移和旧写入风险见[已知限制](acceptance.md#已知限制)。

## 7. 集成测试的运行方式

### 7.1 默认不触网

`go test ./...` 在没有 PostgreSQL、没有 Redis、没有网络的机器上必须保持通过。为此：

- `sessionbackend_test.go` 里的所有用例都只做配置校验或使用 InMemory 后端，不建立任何连接。
- `integration_test.go` 里的所有用例在构造任何配置之前先检查开关：`TRPC_SERVICE_SESSION_INTEGRATION` 不等于 `1` 就 `t.Skip`。
- 连接串各自独立门控：只配了 `TRPC_SERVICE_POSTGRES_DSN` 的机器只跑 PostgreSQL 用例，Redis 用例跳过，不会失败。

### 7.2 重复运行不互相碰撞

- **Redis** 用每次运行随机生成的 key 前缀 `spike:<8位>` 隔离，测试结束删除自己写入的会话。
- **PostgreSQL** 用固定表前缀 `spike`，隔离改在行级——app name 带上每次运行的随机后缀。表前缀故意不随机化：上游只建表、从不删表，随机前缀会在每次执行后留下一组新的 6 张表。
- 每个测试有独立的 30 秒超时上下文；**cleanup 使用自己新建的超时上下文**，不复用测试主体的上下文——后者在 cleanup 运行时通常已经取消，继承它会让每次失败的运行都留下垃圾数据。
- Close 通过 `t.Cleanup` 在建立 service 后立刻注册。`t.Cleanup` 是 LIFO，所以后注册的删除清理先跑、Close 最后跑，删除时连接池仍然可用；断言中途失败也不会泄漏连接池。

### 7.3 命令

```bash
# 起依赖（仅监听 127.0.0.1）
docker compose -f deploy/docker-compose.session.yml up -d --wait

# 跑集成测试
TRPC_SERVICE_SESSION_INTEGRATION=1 \
TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
TRPC_SERVICE_REDIS_URL='redis://:trpc-local-dev@127.0.0.1:56379/0' \
go test -race -timeout 120s ./trpcservice/sessionbackend/...

# 无其他使用者时停止依赖，保留数据卷
docker compose -f deploy/docker-compose.session.yml stop
```

Compose 默认宿主端口为 **55432**（PostgreSQL）和 **56379**（Redis），不是 5432/6379：开发机上经常已经跑着真实数据库，避免误连已有业务数据库。通过 `TRPC_SERVICE_POSTGRES_PORT`、`TRPC_SERVICE_REDIS_PORT` 覆盖，用户名/密码/库名分别由 `TRPC_SERVICE_POSTGRES_USER`、`TRPC_SERVICE_POSTGRES_PASSWORD`、`TRPC_SERVICE_POSTGRES_DB`、`TRPC_SERVICE_REDIS_PASSWORD` 覆盖。

> Compose 文件里的 `trpc-local-dev` 是**本地开发占位口令**，用于可复现的本地测试。它不是生产 secret，也不得被当作生产 secret：两个服务都只绑定 `127.0.0.1`，主机之外无法访问。真实部署必须自行通过环境变量提供凭据，绝不能继承这里的默认值。

健康检查两处细节值得留意，都会影响 `up --wait` 的正确性：`pg_isready` 必须带 `-h 127.0.0.1`，因为首次 initdb 期间入口脚本会先起一个只监听 unix socket 的临时服务，走 socket 的探测会过早报告就绪；`redis-cli ping` 必须匹配 `PONG` 而不是只看退出码，因为数据集加载中的 `LOADING` 回复退出码同样是 0。

## 8. 进程存储 Profile

`cmd/trpc-service` 用一个进程级 profile 决定自己把状态放在哪里。实现在 `cmd/trpc-service/storage.go`。

### 8.1 一个 profile，不是三个开关

进程要存的三样东西——控制面（租户 / 应用 / Revision）、Session→Revision 的 Pin、会话历史——是一组，不是三个独立选择。§6.1 的配置组合保证三者生命周期一致：Pin 活过重启但它指向的 Revision 随内存控制面一起没了，等于没有 Pin；反过来 Pin 丢了而会话还在，会话会被静默重新 Pin 到当前默认 Revision。因此 profile 只有一个，三者一起动。

**Redis 不是一个 profile。** 工厂能构造 Redis Session 服务，但仓库里没有 Redis 的控制面 Repository，也没有 Redis 的 Session Directory；把它开出来，开出来的恰好就是上面那个已知会坏的组合。

### 8.2 环境变量

| 变量 | 取值 | 说明 |
| --- | --- | --- |
| `TRPC_SERVICE_STORAGE_PROFILE` | 空 / 未设 / `inmemory` / `postgres` | 大小写敏感，不做 trim。`Postgres`、`pg`、`redis`、带空格的值一律拒绝启动，并在错误里列出合法值 |
| `TRPC_SERVICE_POSTGRES_DSN` | 连接串 | 当且仅当 profile 为 `postgres` 时必需且不得为空白。**从不写日志**，所有相关错误经 `Scrub` 脱敏 |
| `TRPC_SERVICE_POSTGRES_SCHEMA` | 标识符 | 可选，规则复用 `sessionbackend` 的校验（`^[A-Za-z_][A-Za-z0-9_]*$`，最长 32）。schema **必须事先存在**，进程只建表不建 schema |

两条刻意的选择：

- **`inmemory` 完全不读 PostgreSQL 变量。** 环境里遗留的 DSN 不会改变 inmemory 进程的行为，更不会把进程"升级"成持久化——DSN 的存在**永远不能**选择 profile。要写共享数据库，必须显式点名。
- **拼错就拒绝启动，不静默回退。** 回退到内存的进程看起来是健康的，直到重启丢掉全部会话才被发现。

启动日志只打印 profile、DSN 存在与否、schema 名，从不打印连接串内容。

### 8.3 启动与关闭顺序

顺序本身就是实现的主要内容，逐条都有原因：

1. 先校验监听地址。本进程只服务明文 HTTP，可路由的监听地址会把 Admin Bearer token 明文放到网络上，因此必须在连接数据库前拒绝非回环监听配置。
2. 加载并整体校验安全配置（Security Manifest 或 demo profile）。凭据、角色和租户 entitlement 全部在这一步定型，排在存储之前：一份配错的清单不该先建出连接池、跑完迁移再失败。详见[身份、权限与密钥治理](security-and-governance.md#8-启动顺序)。
3. 读取并整体校验存储配置，此时还没有创建任何资源；schema 拼错在这一步就失败，而不是迁移跑到一半才失败。
4. 解析 pgx 连接池配置，把校验过的 schema 写进 `search_path`（写在 pool config 上而不是 checkout 后 `SET`，这样连接池后来新开的连接也带同一个 `search_path`）。
5. 建池，**并立即登记关闭动作**——之后任何一步失败都不会漏掉这个池。
6. 在带超时的上下文里 `Ping`。`pgxpool.NewWithConfig` 不拨号，而上游 Session 构造函数建表时用的是它自己的、**不可取消**的 background 上下文；不可达的库必须在进入上游之前、在调用方的 deadline 还有效时暴露出来。
7. 依次跑 `tenantpostgres.Migrate` 和 `sessiondirpostgres.Migrate`（都持咨询锁、都是 `IF NOT EXISTS`，每个 worker 每次启动都跑是安全的）。
8. 构造 Repository 与 Directory（共用同一个池，两者都只借用、都不关闭）；再构造上游 Session 服务（它自己持有并拥有另一个池）。
9. 最后才 `SeedDemo`——它要写控制面，必须在迁移之后。

关闭顺序是它的严格逆序：HTTP 优雅关闭（在 `waitForStop` 里）→ Runtime Resolver（等待在途 runtime 交还租约并关闭缓存）→ Storage Router（等待全部 Bundle lease）→ Session 服务 → 共享连接池。启动中途失败时只关闭已经建成的资源，同样逆序。关闭错误用 `errors.Join` 合并进进程退出错误，使 Session 刷写失败等问题能够被调用方识别。

### 8.4 快速开始

```bash
docker compose -f deploy/docker-compose.session.yml up -d --wait

# schema 必须先存在；进程只建表。
psql 'postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session' \
  -c 'CREATE SCHEMA IF NOT EXISTS trpc_service'

TRPC_SERVICE_STORAGE_PROFILE=postgres \
TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
TRPC_SERVICE_POSTGRES_SCHEMA=trpc_service \
./bin/trpc-service -addr 127.0.0.1:8080
```

不设 `TRPC_SERVICE_STORAGE_PROFILE` 时行为与以前完全一致：空机器、无网络也能启动。

### 8.5 这一层的测试

- `cmd/trpc-service/storage_test.go`：不触网。覆盖默认 profile、`inmemory` 忽略遗留 DSN、缺失/空白 DSN、非法 profile、非法与超长 schema、连接串解析错误不泄漏密码（含 percent-encoded 拼写）、以及启动中途失败时按逆序关闭且关闭错误不被吞掉——失败注入使用 `storageDeps` 的八个具体构造函数，不引入 mock 框架。
- `cmd/trpc-service/integration_test.go`：默认跳过，门控与 §7 相同（`TRPC_SERVICE_SESSION_INTEGRATION=1` 加 `TRPC_SERVICE_POSTGRES_DSN`）。每个用例建一个一次性 schema 并在结束时 `DROP ... CASCADE`（上游只建表不删表，这是唯一会回收那 6 张表的地方）。断言走真实的 profile 构造路径：两族迁移和上游 6 张表都在、Pin 与真实会话历史跨"重启"存活、重启后的 `SeedDemo` 不会把已发布的 `echo-v2` 改回 `echo-v1`。

```bash
TRPC_SERVICE_SESSION_INTEGRATION=1 \
TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
go test -race -count=1 -timeout 300s ./cmd/trpc-service/...
```
