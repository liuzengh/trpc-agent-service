# Issue #41：可重启控制面与 Admin API

> 本页是 Issue [#41](https://github.com/XnLemon/trpc-agent-service/issues/41) 的交付契约。它承接 PostgreSQL 控制面实现（Issue #37 / PR #38），记录真实启动、readiness、管理 API 和重启恢复证据。

当前页面同步记录已完成的 bootstrap、readiness、Admin API、持久化 Session 和重启恢复验收。

## 目标与边界

Issue #37 已提供六类控制面对象的 PostgreSQL migration、受控写函数、领域 Repository 和基础 bootstrap：

~~~text
PostgreSQL
  -> SQL Repository
  -> PlanResolver / RunnerRegistry / Dispatcher
  -> HTTP Gateway
~~~

本 Issue 要把这条链路变成可操作的服务：

- 进程从显式配置读取数据库和运行时依赖；
- 新进程可以从同一个 PostgreSQL 数据库恢复 Tenant、App、Profile 和 Binding；
- /readyz 只在真实依赖完整可用时返回成功；
- Admin API 通过受认证、受租户约束的 HTTP 操作管理控制面；
- 所有写入继续使用现有 Repository 的乐观锁、状态迁移、发布和 secret-free 约束。

Session/Event/Memory/Summary 持久化、Redis/S3 runtime、Secret Resolver、Outbox 消费队列和
Channel runtime 由对应模块接入 bootstrap；本页固定服务级装配、认证和恢复边界。

## 已完成能力与交付证据

| 能力 | 当前基线 | Issue #41 交付 |
| --- | --- | --- |
| PostgreSQL migration | 已实现并合入 | 复用，不重做 schema |
| SQL Tenant/App/Model/Backend/Binding Repository | 已实现并合入 | 复用既有契约 |
| 显式 bootstrap graph | bootstrap.New / NewFromEnvironment 已实现 | 服务级启动、readiness 和双进程重启恢复验收 |
| HTTP Gateway / readiness | 已有真实 Dispatcher、Registry 和数据库 ping gate | 保持 503/200 和摘流语义 |
| Admin API | 已实现 | 受认证、受租户约束的控制面 HTTP API |
| Session | 已实现 | 按显式配置装配 InMemory/PostgreSQL/Redis capability |
| 重启恢复 E2E | 已实现 | 独立 Bootstrap 读取同一 PostgreSQL 数据并恢复运行时对象 |

## Bootstrap 契约

### 配置来源

启动配置必须来自显式的进程配置、环境变量解析结果或测试构造器。请求 body、header、IM 消息和未验证的 route 不能决定 tenant_id、app_id、Profile 或 Binding。

生产入口当前使用 bootstrap.NewFromEnvironment。最小配置如下：

| 环境变量 | 用途 | 是否必需 |
| --- | --- | --- |
| TRPC_POSTGRES_DSN | PostgreSQL 控制面连接 | 是 |
| TRPC_API_TOKEN | 最小 API principal 凭证 | 是 |
| TRPC_TENANT_ID | API token 固定的租户 | 是 |
| TRPC_APP_ID | API token 固定的 Agent App | 是 |
| TRPC_ADMIN_TOKEN | 独立 Admin principal 凭证；不能复用 TRPC_API_TOKEN | 是 |
| TRPC_ADMIN_TENANTS | Admin principal 的租户范围，逗号分隔；显式 `*` 表示平台管理员全部租户权限 | 是 |
| TRPC_MODEL_API_KEY | 仅在运行时交给 ModelFactory | 是 |
| TRPC_MODEL_PROVIDER | 当前支持 openai | 否 |
| TRPC_MODEL_NAMES | 受信 Model Catalog | 否 |
| TRPC_MODEL_ENDPOINT_HOSTS | HTTPS endpoint host 白名单 | 否 |
| TRPC_MODEL_SECRET_REF | 控制面中的 secret reference | 否 |

TRPC_API_TOKEN 只用于普通对话 API，固定映射到一个已存在的 Tenant/App；它不能访问 /admin/v1/*。Admin API 必须使用独立的 TRPC_ADMIN_TOKEN，并由 Admin principal 携带 subject、role=admin 和 tenant scope。TRPC_ADMIN_TENANTS=* 表示显式配置的平台管理员，可列出、读取和管理所有 Tenant 及其资源；首个 Tenant 创建仍通过受控的初始化门槛，普通 API token 永远不能提权。生产环境应按最小权限配置具体租户 ID。所有 token 和模型 key 只能在进程启动配置或 SecretResolver 输入边界中出现，不能写入数据库、ExecutionPlan、Factory cache key、日志、trace 或错误响应。Issue #41 的 Admin API 只接收 secret_ref，不接收明文凭据。

### 装配顺序

Bootstrap 必须保持以下依赖顺序。配置解析、Admin token 或 migration 执行失败时，生产入口必须在绑定业务 HTTP 端口前以稳定类别失败；已经构造出的 Runtime 对数据库、Repository、Resolver、Registry、Dispatcher 或 Handler 失效统一通过 /readyz=503 摘流：

~~~text
validate config
  -> open/ping PostgreSQL
  -> migrations.Apply (ordered files, advisory lock, schema history)
  -> construct five control-plane Repository + Channel Candidate source
  -> construct Provider Catalogs
  -> construct SecretResolver / ModelFactory / Session capability
  -> construct PlanResolver
  -> construct RunnerRegistry
  -> construct Dispatcher
  -> construct HTTP Gateway + Admin API
  -> expose readiness
~~~

Channel Binding 不是 PlanResolver 的直接输入。它必须由 Channel Candidate Resolver 独立查询和验证，形成 RoutingTarget / trusted principal 后，才能进入 Gateway Dispatch。

### 资源所有权

Bootstrap graph 必须记录 DB、Session、RunnerRegistry、Dispatcher、HTTP Handler 的所有权；进程 supervisor 另外拥有 net/http.Server，并负责把 Runtime 的 draining gate 与 Server.Shutdown 串起来：

1. 先将 Handler 和 Runtime 标记为 draining；
2. 由 cmd/trpc-service 调用 http.Server.Shutdown，停止新请求并等待在途请求；
3. bounded close RunnerRegistry，等待或取消剩余 Runner lease；
4. 关闭 Bootstrap 自己拥有的 DB/pool 和依赖；
5. 不关闭由调用方借用的 Repository、Session、Factory 或 HTTP client。

重复 Close 必须安全；取消和关闭不能遗留 Runner、event consumer、goroutine 或数据库连接。

## Readiness 契约

/healthz 只表示进程存活，不检查业务依赖。/readyz 是唯一流量接入闸门：

| 条件 | /readyz |
| --- | --- |
| 已运行 Runtime 的依赖失效或 readiness gate 为 false | 503 |
| DB ping 失败或 migration 未准备好 | 503 |
| Repository/Catalog/Secret/Model/Session capability 缺失 | 503 |
| Resolver、Registry、Dispatcher 或 Handler 未就绪 | 503 |
| 所有依赖可用 | 200 |
| BeginShutdown 已调用 | 503 |

数据库 readiness 检查必须设置短超时，不能因数据库阻塞 HTTP handler。错误响应只能返回稳定类别，例如 not ready 或 configuration unavailable，不能泄露 DSN、provider endpoint、secret reference 的值或 SQL 错误细节。

## Admin API 资源与权限

第一版 Admin API 只提供控制面所需的最小操作，所有路径都必须从认证 principal 得到可信租户范围。请求体中的 tenant_id 只能作为一致性校验，不能覆盖 principal。

| 资源 | 最小操作 | 写入约束 |
| --- | --- | --- |
| Tenant | create/get/update/transition | tenant identity 不可变；配置更新带 expected_version；状态迁移必须有 actor/reason/correlation |
| Agent App | create/get/update metadata | tenant_id + app_id 同租户；乐观锁 |
| Agent Revision | create/update draft/publish/rollback | 已发布 Revision 不可变；发布和 current pointer 原子提交 |
| Model Profile | create/get/update/transition | Catalog 校验；只保存 secret_ref；版本冲突拒绝覆盖 |
| Backend Profile | create/get/update/transition | binding 能力和租户引用校验；状态迁移受控 |
| Channel Binding | create/get/update/transition | route/account 唯一性、同租户 App 引用、候选路由约束 |

Admin API 的第一版线协议固定如下；所有 JSON 响应都包含 request_id，错误响应为 {"error":"<stable-category>","request_id":"..."}，不得返回底层 SQL/secret。expected_version 字段位于写请求 JSON；reason、correlation_id 位于同一 JSON；服务端从 Admin principal 填充 actor_type=admin、actor_id=subject。

| 方法 | 路径 | 请求体/成功响应 |
| --- | --- | --- |
| POST | /admin/v1/tenants | tenant.CreateInput；201 + Tenant |
| GET | /admin/v1/tenants/{tenant_id} | 无；200 + Tenant |
| PATCH | /admin/v1/tenants/{tenant_id} | UpdateConfigurationInput；200 + Tenant |
| POST | /admin/v1/tenants/{tenant_id}/status | expected_version,next_status,reason,correlation_id；200 + Tenant + event |
| POST | /admin/v1/tenants/{tenant_id}/apps | agent.CreateInput；201 + App |
| GET | /admin/v1/tenants/{tenant_id}/apps/{app_id} | 无；200 + App |
| PATCH | /admin/v1/tenants/{tenant_id}/apps/{app_id} | UpdateMetadataInput；200 + App |
| POST | /admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions | CreateDraftInput；201 + Revision |
| PATCH | /admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions/{revision} | UpdateDraftInput；200 + Revision |
| POST | /admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions/{revision}/publish | expected_app_version,expected_draft_version,reason,correlation_id；200 + App,Revision,event |
| POST | /admin/v1/tenants/{tenant_id}/apps/{app_id}/rollback | target_revision,expected_app_version,reason,correlation_id；200 + App,event |
| POST | /admin/v1/tenants/{tenant_id}/apps/{app_id}/status | expected_version,next_status,reason,correlation_id；200 + App,event |
| POST/PATCH/GET | /admin/v1/tenants/{tenant_id}/models/{profile_id} | model.CreateInput / UpdateConfigurationInput；201/200 + Profile,event |
| POST/PATCH/GET | /admin/v1/tenants/{tenant_id}/backends/{profile_id} | backend.CreateInput / UpdateConfigurationInput；201/200 + Profile,event |
| POST/PATCH/GET | /admin/v1/tenants/{tenant_id}/bindings/{binding_id} | channels.CreateInput / UpdateConfigurationInput；201/200 + Binding,event |

所有资源的状态动作统一使用 /status，next_status 只能使用领域允许的迁移。读取不存在或跨租户对象统一为 404；认证失败为 401；租户范围不足为 403；字段/状态校验失败为 400；expected version 冲突为 409；存储/依赖故障为 503。

实现必须遵循上述路径和字段；不得以“实现 PR 可调整路由”替换契约。资源路径与动作分离的示例：

~~~text
POST   /admin/v1/tenants
GET    /admin/v1/tenants/{tenant_id}
PATCH  /admin/v1/tenants/{tenant_id}
POST   /admin/v1/tenants/{tenant_id}/status
POST   /admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions/{revision}/publish
POST   /admin/v1/tenants/{tenant_id}/apps/{app_id}/rollback
~~~

示例路径与上表一致；实现不能绕过领域 Repository 直接拼 SQL 或更新发布表。

### 认证与租户隔离

- Admin API 使用独立的管理员认证结果（subject、role、tenant scope），不能把普通对话 API token 自动提升为跨租户管理员；
- principal 的 tenant scope、resource path 的 tenant ID、Repository 的 tenant 条件三者必须一致；
- 无权限、跨租户不存在和越权访问统一映射为稳定的 unauthorized / not found 类别；
- 所有写操作写入审计或 Outbox 事件，记录 actor、reason、correlation ID、旧/新版本和 trace ID；
- expected version 不匹配返回可识别的 conflict，绝不能静默覆盖更新。

## 重启恢复时序

完整验收验证“同一 PostgreSQL 数据 + 两次独立 Bootstrap”，而非仅测试 Repository。生产
Bootstrap 是 migration 唯一 owner：先取得 advisory lock，再由 `migrations.Apply` 按文件名
顺序执行从 `0001_control_plane.up.sql` 至 `0017_agent_chain.up.sql` 的嵌入式序列，并在
`schema_migrations` 写入版本；重复启动验证已应用版本，版本缺失、超前或内容 digest 不一致均失败。
迁移事务失败时不构造可接流量的 Runtime。

~~~text
Process A
  -> migrate / bootstrap
  -> Admin API creates Tenant, App, Revision, Model, Backend, Binding
  -> publish active Revision
  -> Resolver builds ExecutionPlan
  -> bounded shutdown (HTTP -> Registry -> DB)

Process B
  -> bootstrap from same DSN and explicit config
  -> readiness becomes 200
  -> PlanResolver reads persisted Tenant/App/Model/Backend
  -> Channel Candidate Resolver reads persisted Binding independently
  -> trusted principal routes to the same tenant/app
  -> stale expected_version is rejected
~~~

测试不得 seed fake roots、绕过 Admin/Repository boundary，或从请求体推导租户来使流程通过。

## 测试矩阵

### Bootstrap / readiness

- 缺少每个必需环境变量时启动失败且不监听端口；
- PostgreSQL ping/migration 失败时启动失败或 readiness 为 503；
- fake SecretResolver、ModelFactory 和 InMemory Session 可以构造测试 graph；
- readiness 从 503 变为 200，再在 BeginShutdown 后回到 503；
- 数据库和依赖关闭只发生一次，取消不泄漏 goroutine。

### Admin API

- 管理员认证成功、普通 API principal 被拒绝；
- 同租户 CRUD 成功，跨租户读取/写入/引用失败；
- expected version 冲突不会覆盖已有更新；
- 发布、回滚、状态迁移产生原子变更和事件；
- 明文 token、API key、DSN 不会出现在持久化记录、响应、日志或错误中。

### Restart E2E

- PostgreSQL migration 只执行一次且可重复验证；
- Process A 写入的数据由 Process B 的新 Resolver/Channel Candidate Resolver 读取；
- Process A 关闭后连接池、Runner 和 Session owner 的生命周期符合声明；
- Process B 不能读取其他租户对象，也不能使用旧版本或未发布 Revision。

## 干净 PostgreSQL 启动示例

~~~bash
createdb trpc_control_plane
export TRPC_POSTGRES_DSN='postgres://postgres:postgres@127.0.0.1:5432/trpc_control_plane?sslmode=disable'
export TRPC_API_TOKEN='chat-token'
export TRPC_TENANT_ID='t_01ARZ3NDEKTSV4RRFFQ69G5FAV'
export TRPC_APP_ID='app_01ARZ3NDEKTSV4RRFFQ69G5FAV'
export TRPC_ADMIN_TOKEN='admin-token'
export TRPC_ADMIN_TENANTS='*'
export TRPC_MODEL_API_KEY='provided-outside-control-plane'
go run ./cmd/trpc-service
~~~

启动会由 bootstrap 执行有序 migration；migration 或配置失败时进程不绑定业务端口；已运行进程的依赖失效由 /readyz 返回 503。

## 验收命令

实现阶段至少运行：

~~~bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
python -m mkdocs build --strict -f docs/mkdocs.yml
~~~

PostgreSQL 集成测试使用 CI 提供的 disposable PostgreSQL；本地快速开始应提供等价 DSN 和 migration 步骤，但不把真实模型供应商或真实 IM 作为本 Issue 的必要依赖。
