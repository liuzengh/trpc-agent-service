# trpc-agent-service — 基于 tRPC-Agent-Go 的多租户节点化 Agent 部署平台

生产级多租户 Agent 服务：IM（飞书 / Telegram）webhook 准入 → 持久队列 → 无状态 Worker 执行（tRPC-Agent-Go `runner.Runner`）→ 事务性 Outbox 可靠回复。以 **PostgreSQL 为唯一事实源**，全部租户数据由行级安全（RLS）在存储层强制隔离；任何节点崩溃都不产生"已回复但无事实"或"有事实但无法追溯"的状态。

初始项目需求与示范文档存档于 [docs/project-requirements.md](docs/project-requirements.md)。

## 核心特性

- **多租户强隔离**：26 张租户表全部 `ENABLE + FORCE ROW LEVEL SECURITY`，租户身份经事务级 GUC（`trpc.tenant_id`）传递，越权访问由数据库直接拒绝
- **节点化水平扩展**：Worker 无状态，不依赖 sticky session；共享 PostgreSQL 状态层 + 会话租约（单调递增 fencing token）杜绝同会话并发与僵尸执行
- **可靠回复投递**：事务性 Outbox 与业务事实同事务提交；按租户并发投递、指数退避、超限进入死信可重放
- **消息级幂等**：IM 重复投递由 `message_dedup` 认领（owner + fence）吸收；执行结果以 `fence_token` 条件提交，只生效一次
- **治理前移**：三维限流（租户 / 绑定 / 会话）→ 进程内准入槽位 → 跨实例持久容量预算（ingress / worker / sender）→ 工具白名单
- **全链路可观测**：OTel W3C traceparent 贯穿 webhook → 队列 → 执行 → 投递 → 审计；`trace_id` / `request_id` / `execution_id` 三键冗余，任意一点可回放
- **多后端适配**：PostgreSQL（权威层）/ Redis（限流与协调加速）/ S3 兼容对象存储（产物）/ Milvus·pgVector（记忆向量投影，可重建）
- **IM 双通道**：飞书（Lark）与 Telegram 统一 Channel Adapter（验签解密、事件解析、回执 ACK、发送分片与退避）
- **故障恢复**：作业租约到期自动回收重投；`trpc-recovery` 提供 backup → verify → restore drill 周期演练

## 架构总览

| 组件 | 位置 | 职责 |
|------|------|------|
| `trpc-service` | `cmd/trpc-service` | 单一入口，同进程承载 Gateway（webhook 准入）、Worker（Agent 执行）、Dispatcher（回复投递） |
| `trpc-migrate` | `cmd/trpc-migrate` | 版本化 schema 迁移与运行时角色供给 |
| `trpc-recovery` | `cmd/trpc-recovery` | 备份、校验与恢复演练 |
| Storage Adapter | `trpcservice/storage/*` | PostgreSQL 权威层（RLS）、Redis 协调/限流、S3 对象存储、租户上下文 |
| Channel Adapter | `trpcservice/channels/*` | Lark / Telegram 接入（webhook）与回程发送（sender） |
| Agent 装配层 | `trpcservice/agent/*` | 基于 tRPC-Agent-Go 的 `runner.Runner` 装配、工具白名单桥接 |

完整的架构图、核心链路时序图、数据模型、数据同步与幂等策略、多后端适配及风险清单见 **[docs/architecture-design.md](docs/architecture-design.md)**。

## 快速开始

### 环境要求

- Go 1.25+（见 `go.mod`）
- PostgreSQL 16（权威数据层，必需）
- Docker（本地 Compose 部署与集成测试）
- 可选：Redis 7（限流与协调加速）、S3 兼容对象存储（产物）、Milvus 2.5（语义检索）

### 构建

```bash
./build.sh                    # 构建 bin/trpc-service
go build -o bin/trpc-migrate  ./cmd/trpc-migrate
go build -o bin/trpc-recovery ./cmd/trpc-recovery
```

### 数据库迁移（trpc-migrate）

```bash
export DATABASE_URL=postgres://trpc_admin:<密码>@127.0.0.1:5432/trpc?sslmode=disable
export DATABASE_SCHEMA=public
export MIGRATIONS_DIR=./migrations
# 可选：同时供给受限运行时角色（服务以该角色承载业务流量）
export RUNTIME_ROLE=trpc_runtime
export RUNTIME_PASSWORD_FILE=/run/secrets/pg_runtime_password

bin/trpc-migrate
```

### 启动服务（最小配置）

服务在装配阶段 fail closed：以下变量缺一不可（IM 通道默认禁用，密钥引用仅需格式合法）。

```bash
export DATABASE_URL=postgres://trpc_admin:<密码>@127.0.0.1:5432/trpc?sslmode=disable
export DATABASE_RUNTIME_URL=postgres://trpc_runtime:<密码>@127.0.0.1:5432/trpc?sslmode=disable
export DATABASE_SCHEMA=public
export MIGRATIONS_DIR=./migrations
export HTTP_ADDR=127.0.0.1:8080

# 模型与租户自举
export MODEL_PROVIDER=runner MODEL=openai
export MODEL_BASE_URL=https://llm.example.com/v1 MODEL_NAME=demo-model MODEL_API_KEY=<密钥>
export DEFAULT_TENANT=demo DEFAULT_AGENT_APP_ID=demo-agent
export BOOTSTRAP_AGENT_VERSION=1
export BOOTSTRAP_TENANT_ID=demo BOOTSTRAP_TENANT_NAME=demo
export BOOTSTRAP_AGENT_APP_ID=demo-agent BOOTSTRAP_AGENT_NAME=demo-agent
export ASYNC_OWNER_ID=local-owner

bin/trpc-service
# 就绪探针：GET http://127.0.0.1:8080/healthz
```

### Docker Compose 本地环境

```bash
cp .env.example .env    # 填写 P109_*（运行 ID、端口、0600 密钥文件目录、应用镜像）
docker compose up -d    # 应用 + PostgreSQL 16 + Redis 7；--profile telemetry / --profile recovery 见文件内注释
```

### 生产部署

`deploy/production/vm1.compose.yml`（主库 + 应用双活节点）与 `vm2.compose.yml`（流复制备库 + 看门狗），配合 `scripts/production/` 的桥接看门狗与 systemd 定时器；主备切换 = 调整数据库指向 + 重建容器（RTO 分钟级）。

## 配置参考

| 分组 | 变量（默认值） | 说明 |
|------|----------------|------|
| 核心数据面 | `DATABASE_URL`、`DATABASE_RUNTIME_URL`（必填）、`DATABASE_SCHEMA`、`MIGRATIONS_DIR`（`migrations`）、`HTTP_ADDR`（`:8080`） | 权威库与管理/运行时双 DSN、schema 搜索路径、迁移目录、监听地址 |
| 模型 | `MODEL_PROVIDER`（`runner`）、`MODEL`（`openai`）、`MODEL_BASE_URL`、`MODEL_NAME`、`MODEL_API_KEY`、`MODEL_CONFIG_REF`（`env`）、`MODEL_CONFIG_VERSION`（`1`） | runner 模式三者缺一即 fail closed |
| 租户自举 | `DEFAULT_TENANT`（`demo`）、`DEFAULT_AGENT_APP_ID`（`demo-agent`）、`BOOTSTRAP_AGENT_VERSION`、`BOOTSTRAP_TENANT_ID/NAME`、`BOOTSTRAP_AGENT_APP_ID/NAME`、`ASYNC_OWNER_ID`、`TOOL_POLICY_REF`（`default`）、`GUARDRAIL_REF`、`BOOTSTRAP_AGENT_SYSTEM_PROMPT` | 启动时自举默认租户与 Agent 应用 |
| IM 通道（可选） | `BOOTSTRAP_LARK_ENABLED` / `BOOTSTRAP_TELEGRAM_ENABLED`（默认 `false`）及各自 `*_BINDING_ID`、`*_EXTERNAL_APP_ID`、`BOOTSTRAP_LARK_APP_ID`、`BOOTSTRAP_LARK_RECEIVER_ID_TYPE`、`*_SECRET_REF` 系列 | 禁用时密钥引用仅做格式校验，不发起解析 |
| 对象存储（可选） | `BOOTSTRAP_OBJECT_BACKEND`（`none`）、`OBJECT_ENDPOINT`、`OBJECT_USE_PATH_STYLE`、`OBJECT_MAX_BYTES`、`OBJECT_PRESIGN_MAX_TTL` | `s3` 后端时 `OBJECT_ENDPOINT` 必填 |
| 治理（可选） | `RATE_LIMIT_REDIS_URL`（空=禁用）、`RATE_LIMIT_WINDOW`（`1m`）、`RATE_LIMIT_TENANT/BINDING/CHAT_LIMIT`（`600/300/120`）、`INGRESS_MAX_INFLIGHT`（`64`）及 `_PER_TENANT`（`32`）、`_PER_BINDING`（`16`）、`CAPACITY_BUDGET_TTL`（`10m`） | Redis 不可达时限流退化为放行，不影响正确性 |
| Worker / Outbox | `WORKER_CONCURRENCY`（`4`）、`WORKER_VISIBILITY_TIMEOUT`（`2m`）、`WORKER_LEASE_TTL`（`2m`）、`WORKER_RETRY_DELAY`（`5s`）、`WORKER_SHUTDOWN_TIMEOUT`（`10s`）、`WORKER_CLEANUP_TIMEOUT`（`5s`）、`OUTBOX_LOCK_DURATION`、`DISPATCHER_CLAIM_INTERVAL`（`1s`）、`DISPATCHER_CLAIM_BATCH_SIZE`、`DISPATCHER_CONCURRENCY`（`1`）、`DISPATCHER_SHUTDOWN_TIMEOUT`（`5s`） | 队列认领与回复投递的并发/租约/退避 |
| 生命周期 | `STARTUP_TIMEOUT`（`30s`）、`SHUTDOWN_TIMEOUT`（`10s`）、`FORCE_CLOSE_TIMEOUT`（`500ms`） | 装配超时与优雅停机分级 |

迁移工具另支持 `RUNTIME_ROLE`、`RUNTIME_PASSWORD` / `RUNTIME_PASSWORD_FILE`、`MIGRATE_TIMEOUT`。`G_D_*` 系列变量为子进程测试专用钩子，生产环境勿用。

## 测试

```bash
go test ./...                    # 全量：单元测试 + 编译验证；外部依赖类集成测试自动跳过
go test ./trpcservice/storage/postgres/ -count=1 -v   # 含真实服务的集成测试（需 Docker）
```

- **集成测试门禁**：设置 `TEST_DATABASE_URL` / `TEST_REDIS_URL` 后启用数据库级集成测试；依赖 Docker 的测试（真实服务子进程、Compose、恢复演练）在无 Docker 环境自动跳过
- **CI**：`.github/workflows/ci.yml` — gofmt → `./lint.sh`（`go vet` + golangci-lint v2.12.2）→ `go test ./...` → 租户边界 race 测试；lint 配置见 `.golangci.yml`（当前启用 typecheck + govet，其余 linter 随债务清理分批恢复）

## 目录结构

```
cmd/trpc-service          服务入口（Gateway + Worker + Dispatcher 同进程）
cmd/trpc-migrate          迁移与运行时角色供给
cmd/trpc-recovery         备份/校验/恢复演练
trpcservice/              平台业务包
  channels/{lark,telegram}  IM Channel Adapter（webhook 验签 + sender 回程）
  gateway/ queue/ worker/ execution/ outbox/    准入 → 持久队列 → 执行 → 回复投递
  storage/{postgres,redis,coordination,s3object,tenantctx}  存储适配层
  admission/ ratelimit/ capacity/               治理三件套
  tenant/ agent/ tool/ memory/ session/ vector/ telemetry/ …
migrations/               14 个版本化迁移（26 张租户表全部 RLS）
deploy/production/        vm1 / vm2 生产 compose 与看门狗
docs/                     架构设计与初始需求文档
```

## 文档索引

- [docs/architecture-design.md](docs/architecture-design.md) — 平台架构设计说明书（架构图、核心时序图、数据模型、数据同步与幂等策略、多后端适配、风险清单）
- [docs/project-requirements.md](docs/project-requirements.md) — 初始项目需求与示范（原始 README 存档）
