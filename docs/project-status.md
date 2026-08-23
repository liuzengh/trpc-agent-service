# 项目现状核对

> 本文是当前仓库状态的核对记录，基准为 Git `62d6963`（`feat:complete stage P0-06`）。它回答“代码现在有什么”，不替代目标架构和实施计划。

## 1. 核对结论

当前项目已经完成了平台核心领域和协调层的试用版基础，但还没有形成可生产运行的 Agent 平台。现状可以准确描述为：

- 已有 Go 服务入口、基础 HTTP 路由和本地内存联调链路。
- 已有租户、Agent、Channel Binding、Session、Event、Memory、Summary、Artifact、Audit 的领域模型和校验测试。
- 已有 PostgreSQL 版本化迁移框架，以及包含 15 张业务表和 2 张协调表的数据库 schema。
- 已有 Redis/PostgreSQL 两套协调后端的幂等 Claim、Session Lease、epoch/fencing、续租和故障切换抽象。
- 已有 Redis 三维固定窗口限流、熔断、恢复探测和跨后端契约测试。
- 尚未把 PostgreSQL/Redis 实现接入主进程的业务 Store；主进程仍使用 `MemoryStore`。
- 尚未实现真正的 tRPC-Agent-Go Runner Adapter；当前 Runner 使用 `EchoResponder`。
- 尚未实现异步 Job Queue、Gateway/Worker 分离、Outbox Dispatcher、Retry/DLQ、真实 IM 协议适配、鉴权、OTel、向量库和对象存储。

因此，P0-01 至 P0-06 应标记为“已完成的基础层”，不能等同于“首个生产版本已完成”。后续开发从新的总计划 `implementation-plan.md` 的 P0-07 开始。

## 2. 已完成能力

| 阶段 | 实际落地 | 证据位置 | 状态 |
| --- | --- | --- | --- |
| P0-01 | Go module、CI、gofmt、lint、test、vet 基线 | `go.mod`、`.github/workflows/ci.yml`、`lint.sh` | 已完成 |
| P0-02 | `Tenant`、`AgentApp`、`ChannelBinding`、`BackendPolicy`、`TenantContext`、Resolver 契约 | `trpcservice/tenant/`、`trpcservice/config/` | 已完成 |
| P0-03 | Session 状态、事件、Memory、Summary、Artifact、Audit 和脱敏模型 | `trpcservice/session/`、`memory/`、`artifact/`、`audit/` | 已完成 |
| P0-04 | Session ID、DedupKey、Repository/Claim/Lease/Outbox 契约、Fake 实现和契约测试 | `trpcservice/session/key.go`、`trpcservice/storage/` | 已完成 |
| P0-05 | 迁移版本、checksum、advisory lock、readiness、up/down 脚本 | `migrations/`、`trpcservice/storage/postgres/` | 已完成 |
| P0-06 | Redis/PostgreSQL Claim、Lease、epoch、fencing、failover、circuit breaker、rate limit | `trpcservice/storage/{redis,postgres,coordination}/`、`trpcservice/ratelimit/` | 已完成 |

P0-06 的细分验收见 `p0-06-acceptance-matrix.md`。该矩阵记录了单后端、跨后端、真实 Redis/PostgreSQL、竞态、故障注入和 fail-closed migration 检查。

## 3. 当前可运行链路

当前入口在 `cmd/trpc-service/main.go`：

1. 创建 `platform.MemoryStore`。
2. 写入一个默认 `demo` 租户。
3. 如果设置 `DATABASE_URL`，只初始化迁移和 readiness gate；不会把 PostgreSQL Repository 注入业务运行链路。
4. 创建 `platform.Runner`，其 Responder 为 `platform.EchoResponder`。
5. 启动 HTTP 服务。

当前 HTTP 路由由 `trpcservice/web/server.go` 提供：

| 路由 | 当前行为 | 生产缺口 |
| --- | --- | --- |
| `GET /healthz` | 返回服务存活；配置 readiness 时检查迁移 | 需要区分 liveness/readiness，并检查依赖、版本和排空状态 |
| `POST /api/tenants` | 依赖 Context 中的 `tenant.admin` 权限，写入内存租户 | 没有真实认证、管理服务、PostgreSQL 写入和审计 |
| `POST /api/chat` | 同步执行 Echo Runner，读取内存历史并返回文本 | 没有 API 鉴权、限流、SSE、异步执行、真实 Agent 和生产存储 |
| `POST /webhook/{channel}/{external_app_id}` | 验证存在性、解析简化 JSON、Resolver 映射、同步 Runner、返回简化 JSON | 没有平台级快速 ACK、真实验签/解密、Job 投递、Outbox 回复和 IM API 发送 |

`trpcservice/channels/channels.go` 中的 `wecom` 和 `telegram` 目前只是协议形状的占位适配器：企业微信只检查 `X-WeCom-Signature` 是否存在，Telegram 只检查 secret header 是否存在；二者都没有真实签名计算、重放保护、完整事件解析或 Bot/API 发送。

## 4. 代码边界

### 已存在的包

- `trpcservice/tenant`：租户实体、Context、状态和 Resolver 接口。
- `trpcservice/config`：租户配置解析/解析器相关模型。
- `trpcservice/session`：Session、状态迁移、事件、key 规则。
- `trpcservice/memory`、`artifact`、`audit`：领域模型和审计脱敏。
- `trpcservice/storage`：Repository、Claim、Lease、Outbox 契约、Fake 实现、契约测试。
- `trpcservice/storage/redis`：Redis 协调实现。
- `trpcservice/storage/postgres`：连接池、迁移、readiness 和协调实现。
- `trpcservice/storage/coordination`：故障切换和断路器模型。
- `trpcservice/ratelimit`：Redis Lua 三维固定窗口限流。
- `trpcservice/platform`：仅用于当前同步内存联调的兼容层。
- `trpcservice/web`、`channels`：当前最小 HTTP/Adapter 壳。
- `internal/testinfra`：Docker 依赖测试基础设施。

### 尚不存在的包或生产实现

仓库当前没有以下目录或可交付实现：

`gateway`、`worker`、`queue`、`outbox`、`auth`、`identity`、`governance`、`telemetry`、`vector`、`object`、`admin`、`channels/wecom`、`channels/telegram`、生产版 `storage/postgres` Repository、真实 `agent` Runtime。

`trpcservice/agent`、`tool`、`skill`、`metrics`、`log`、`workspace` 当前只有最小占位或基础类型，不能据此宣称对应产品能力已经完成。

## 5. 数据库现状

`migrations/000001_initial.up.sql` 创建以下事实源表：

`tenant`、`agent_app`、`channel_binding`、`user_identity`、`session`、`session_event`、`message_dedup`、`memory`、`summary`、`artifact`、`audit_log`、`outbox_message`、`dead_letter`、`agent_release`、`tenant_config_version`。

`migrations/000002_coordination.up.sql` 增加：

- `message_dedup.epoch`。
- `coordination_epoch`：租户资源的 epoch authority。
- `session_lease`：Session owner、租约、fencing token 和过期时间。

数据库 schema 已经为后续阶段预留了 Outbox、DLQ、发布版本和配置版本，但业务 Repository、Unit of Work、RLS、备份恢复自动化尚未实现。迁移器当前是可独立测试的基础设施，不是完整数据库运行时。

## 6. 验证基线

在当前 HEAD 实测：

```text
go test ./...  PASS
go vet ./...   PASS
gofmt -l .     无输出
```

`go test ./...` 会运行内部测试基础设施和真实 PostgreSQL/Redis 集成测试；因此本地环境需要可用的 Docker 或相应测试依赖。CI 当前执行格式检查、`lint.sh`、全量测试以及租户边界 race test。

## 7. 主要风险与文档规则

- 不要把 `DATABASE_URL` 已配置解释为业务数据已经使用 PostgreSQL。
- 不要把 `channels` 中的协议占位实现解释为企业微信/Telegram 生产接入。
- 不要把 Repository 接口和迁移表解释为 Repository 已经实现。
- 不要在新的任务中重新规划已完成的 P0-01 至 P0-06；若发现基础契约不足，应增加兼容迁移或单独的修订任务。
- 所有后续能力必须先通过 `TenantContext`，并在数据 key、SQL 条件、对象 key、向量 metadata、日志和指标中明确租户边界。
