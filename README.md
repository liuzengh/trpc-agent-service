# 多租户 IM Agent Platform

这是基于 `trpc-agent-go` 的赛题完整交付：一个可直接运行的多租户 Agent Gateway/Worker 示例，以及面向生产环境的架构、数据、一致性、安全、可观测性和部署设计。

`acme` 默认使用 OpenAI 兼容接口中的 `glm-5.2`。远端在首个有效响应前失败时自动回退确定性 mock；未注入模型 Key 时也直接使用 mock，因此仍可零密钥走通：

```text
HTTP / Telegram / Slack / 企业微信自建应用 / 企业微信智能机器人
  → webhook 原文验签
  → 可信 channel binding 解析 tenant
  → 消息规范化、用户权限和 Redis 共享租户限流 Filter
  → Inbox/Outbox（默认进程内，可切换 Postgres 持久化）
  → 同 Session 串行、租户 Runtime/Runner 池
  → SQL Session turn 缓冲 + version/fencing 原子提交（可选）
  → trpc-agent-go Runner + Session/Memory/Artifact
  → ToolFilter + ToolPermissionPolicy + 参数绑定审批 + 副作用 dispatch fence
  → IM 分片投递、审计、Metrics、OpenTelemetry
```

## 已实现能力

- 租户模型覆盖应用、模型、工具、Telegram/Slack/企业微信自建应用/企业微信智能机器人、Session/Memory/Summary/Artifact/Knowledge/Audit 后端、审计和预算策略。
- `(channel_type, binding_id)` 是租户身份唯一可信来源；外部消息不能指定 `tenant_id`。
- Telegram secret token 验证和 Slack HMAC-SHA256 + 5 分钟重放窗口验证；Slack 再绑定 `team_id/api_app_id`，阻断共享 App secret 下的跨工作区路由。
- 企业微信回调 AES-256-CBC 解密 + SHA1 验签 + URL echostr 验证；receiveID/corpid 与 AgentID/agentid 双重绑定，阻断跨企业、跨应用路由；单聊/群聊回复分流，access_token 缓存与 40014/42001 刷新重试；2048 字节按 rune 边界拆分。
- 企业微信智能机器人 API 模式使用 Bot ID + Secret 主动建立 WebSocket 长连接，无需公网回调 URL；支持文本、语音转写、图文混排及图片/文件/视频元数据入站，单聊/群聊统一进入现有 Agent 队列，回复使用主动 Markdown 消息并按 20480 UTF-8 字节安全拆分。
- `acme` 模型策略按 `glm-5.2 → mock` 排序：主模型通过 DashScope OpenAI-compatible API 调用；连接、鉴权、限流或 API 错误发生在首个有效响应前时自动切换本地 mock。为避免远端已经输出部分内容后混入另一模型，启用 fallback 时强制关闭 streaming。
- 私聊、群聊、群话题使用确定性租户隔离 ID；群内不同成员共享同一个 Runner group principal，真实发送者单独审计。
- 按租户、配置版本和配置摘要缓存不可变 Runner；不同 revision 可交错复用，后端构建用 singleflight 隔离慢连接且不持有全局缓存锁。Session 支持 InMemory/Redis/严格 PostgreSQL turn，Memory 支持 InMemory/Redis。
- 以 `agent.WithAppName` 强制 Session/Memory namespace；应用名为无 `/` 的 opaque hash，避免 EventFilterKey 层级污染。
- `agent.WithToolFilter` 缩小模型可见工具面，组合后的 `agent.WithToolPermissionPolicy` 在最终参数产生后再次强制鉴权。
- 危险工具审批绑定 tenant、配置版本、用户、session、tool 和规范化参数 hash，五分钟过期且一次性消费。
- `tools.side_effects` 显式分类真实副作用工具；Runner-scoped Plugin 在权限最终 allow 后才以 tenant/app/session/稳定 turn/config revision/tool/规范参数生成 intent key，并写入持久 operation fence。成功结果确认、普通执行错误或确认落账不确定转为 `unknown`，不会自动重做；明确“未应用”的 typed error 才可重试。工具可从 Context 取得 opaque operation key 并传给支持幂等键的 provider。
- 消息 Claim + pending result Outbox：IM 投递失败时重放已有结果，不重复运行 Agent。
- 持久 Inbox/Outbox（`queue.backend=postgres`）：回调先落库再 ACK；完整 Session lane 作为 `partition_key`，Inbox 与 Outbox 都只租用各 lane 最早的非终态记录，因此同 Session 严格 FIFO、不同 Session 可多副本并发；Agent 执行与 IM 投递分离，回复的每个确定性分片在处理完成事务中各自成为一个 Outbox operation。每次发送拥有独立 attempt ledger，区分“确认成功、明确未发送可重试、永久拒绝、结果未知”；网络响应丢失或 dispatched 租约过期会停在 `unknown`，不会盲目重发，并通过受 Admin Bearer 保护的 CAS 决议接口人工选择确认、接受重复风险后重试或取消。`FOR UPDATE SKIP LOCKED`、fencing、自动续租、Provider `Retry-After`、退避和 DLQ 支持崩溃恢复；数据库时间统一使用 PostgreSQL `clock_timestamp()`。`backend=memory` 提供同状态机但不跨重启，`backend=inmemory` 保留进程内队列。
- 严格 SQL Session（`data.session.type=sql`）：用稳定 message dedup key 派生 Turn ID；Runner 的 event/state 先留在 turn-local 缓冲，Worker 再以 expected version + 单调 fencing token 提交。与 `queue.backend=postgres` 组合时，配置强制 Session 与 Queue 使用相同 `dsn_env`，canonical replay 中的确定性投递分片、events、state、session version、Inbox lease/身份复核、Inbox processed 和 Outbox insert 共用一个 PostgreSQL 事务；任一参与步骤失败都会整体回滚。重复 Turn 使用数据库中已提交的 canonical replay 补齐队列侧状态，不采用输家的本地输出，也不重新运行模型。
- 滚动升级协议：新提交的 Task 在 `pipeline` 中持久化 `schema_version`、`atomic_commit_mode`、`database_identity`，并镜像到 Inbox 的 `pipeline_schema_version`、`atomic_commit_mode`、`database_identity` 列。升级前 `pipeline_schema_version=0` 且模式/身份为空的 legacy 记录不会被追认为原子事务，而是沿 canonical replay 支持的可恢复双事务路径排空并计入 `queue_inbox_legacy_pipeline_total`。v2 `postgres_same_database_v1` 要求 Task/Inbox 元数据完全一致且 Queue/Session/事务连接具有相同 database identity；未来版本、暂时缺少原子能力或 identity 不匹配会在运行模型前停放重试且不消耗死信尝试预算，格式错误或当前 v2 的 Task/Inbox 元数据漂移才进入 dead letter。所有拒绝路径都禁止静默降级为双事务。
- Redis coordination 使用 Lua + Redis 服务端 `TIME` 的固定 60 秒窗口，按租户共享 RPM；Redis 故障时 fail-closed，不退回本地计数器。InMemory 限流只用于测试和显式 demo。
- `queue.backend=postgres` 复用同一 PostgreSQL 的 006 用量账本：每个真实 provider model 调用独立 `run_id + call_no` 预留，可靠 usage 结算，超时/断流/缺失 usage 标记 `unknown`，只有确认请求未发出才能 `released`。金额使用 1e-8 USD 固定点单位，unknown 继续占用额度且不自动退款，结算归预留时的 UTC 月份。
- 已完成消息 replay 直接返回持久化结果，不新增模型调用或扣费；真实 retry、failover 和第二次 `GenerateContent` 都使用新调用记录并计费。`model_tokens_total`、费用和 unknown 指标按实际调用更新。
- Prometheus 文本指标、框架 OTLP MeterProvider（模型 TTFT/token/耗时与 Agent/工具 histogram）和跨异步队列的 W3C trace context；LLM/Agent/Tool/Workflow 的正文、参数和结果等敏感 trace 属性由服务端 SpanAttributePolicy 在源端 Drop，OTel Collector 再做一次统一删除兜底。
- Admin API 支持持久配置 revision/release、canary/promote/rollback、节点 ACK 查询，以及 unknown Outbox/工具副作用 operation 的脱敏查看与 CAS 人工决议。

生产方案仍需真实 Secret Manager 与 provider reconciliation connector；审批已接入共享 Redis；本仓库已把 Summary、Memory 可见性、SQL Audit/spool、本地向量迁移和 009 内容安全租约的恢复边界固化。持久预算只在 `queue.backend=postgres` 时启用；`inmemory`/`memory` 明确是本地或测试模式，不能提供跨节点总额度保证。持久配置控制面在 `control_plane.backend=postgres` 时复用 Queue PostgreSQL 的 007 迁移，提供 immutable revision、generation CAS、LISTEN/NOTIFY+轮询、稳定灰度分组和节点 prepared/applied/verified ACK；PostgreSQL 不可用或迁移缺失会阻止启动。SQL Summary、Memory 水位和 SQL Audit 复用同一 Queue PostgreSQL 的 008 事实源；内容安全使用 009 的 fail-closed decision/lease。`unknown` 审计/用量状态都不会自动退款或覆盖，人工对账/修正 API 不在本次公共接口范围内。已实现的组合事务只覆盖“同 DSN 的 strict SQL Session + PostgreSQL Inbox/Outbox”；工具副作用由独立 003 ledger 跨事务 fencing，并不被纳入 Session COMMIT。IM 平台和任意不接受 idempotency key 的工具 provider 都不能宣称 exactly-once。

## 2026-09-11 Review 修复

已修复跨轮工具权限审计冲突、Session 提交失败后输出安全决策阻断重试，以及企微智能机器人连接未随配置发布更新的问题。权限审计按实际检查事件落账；输出审核绑定具体候选回复和 revision；连接在 Registry 发布前完成认证与替换，失败不发布新本地配置。启用的机器人启动时也必须完成认证，同一 binding 更换 Bot/凭据引用需全量发布，不能通过 canary 同时选择两个传输身份。离线 smoke 显式禁用外部机器人连接。

完整修复说明、测试和生产验收边界见 [Review 修复记录](docs/review-2026-09-11.md)。后续已补充租户级规则型入模脱敏/出站 DLP 与存储延迟 histogram/SLO 告警，使用方式及剩余边界见 [文本治理与后端指标](docs/privacy-and-storage-metrics.md)。多媒体、语义 DLP、在线迁移等仍有明确边界。

## 赛题需求完成情况

对照 `赛题.md` 的 7 条验收标准逐项审查（详细矩阵见 [docs/acceptance.md](docs/acceptance.md)）：

| # | 验收标准 | 状态 | 证据 |
| --- | --- | --- | --- |
| 1 | 架构方案覆盖多租户、节点化部署、数据同步、多后端、IM 接入、治理监控、故障恢复 | 已覆盖 | 四篇设计文档 + acceptance.md 逐项矩阵 |
| 2 | 数据模型表达 tenant、agent、channel binding、session、event、memory、summary、artifact、audit log | 已交付 | [migrations/001_schema.sql](migrations/001_schema.sql)、[migrations/002_runtime_pipeline.sql](migrations/002_runtime_pipeline.sql)、[migrations/003_tool_operations.sql](migrations/003_tool_operations.sql)、[migrations/004_data_migrations.sql](migrations/004_data_migrations.sql)、[migrations/005_artifact_objects.sql](migrations/005_artifact_objects.sql)、[数据一致性 §10](docs/data-consistency.md) |
| 3 | 至少两种 IM 通道接入差异，其中至少包含微信或企业微信 | 已满足 | Telegram/Slack/企业微信三通道已实现，差异（明文签名头 vs HMAC vs AES 加密信封、rune vs 字节拆分、单聊/群聊 API 分流）见 [IM Adapter §2](docs/im-adapters.md) |
| 4 | 至少三类后端的存储与同步策略 | 已覆盖（设计层） | [数据一致性 §7](docs/data-consistency.md)：Redis、SQL、向量库、对象存储逐一取舍 |
| 5 | 完整消息链路时序，`trace_id` 贯穿 | 已覆盖 | [架构 §4](docs/architecture.md) Mermaid 时序图；[安全 §5](docs/security-operations.md) trace 链路 |
| 6 | 至少 8 个生产风险及缓解措施 | 已覆盖 | [安全 §7](docs/security-operations.md) 11 项故障/风险缓解表；acceptance.md §8 P0 缺口 |
| 7 | 明确哪些能力复用 tRPC-Agent-Go、哪些是平台层新增 | 已覆盖 | [架构 §6](docs/architecture.md) 集成边界 |

交付物对照：架构设计文档、系统架构图（最小/生产两张拓扑）、核心时序图（Telegram/Slack/企业微信完整链路）、数据模型 SQL、同步与幂等策略、多后端适配方案、风险清单、可运行代码（`go vet`、`go test`、smoke 全部通过）均已交付。

已知差距（按影响排序，与 acceptance.md 状态一致）：

1. 生产数据面主要故障窗口已有代码基线：持久 SQL Inbox/分片 Outbox、send attempt/resolution ledger、Session 分区 FIFO、数据库统一时钟/fencing、006 模型用量预算账本、007 配置控制面、008 生命周期表、009 内容安全阶段和独立工具副作用 operation ledger 均已实现。`trpc-migrate` 顺序应用不可变 002–009，并可按租户配置迁移 Artifact 元数据账本、预建 PGVector schema；业务进程启动只校验 checksum/schema/权限。Runtime 已装配外部 Mem0、带 PostgreSQL 恢复账本的 S3-compatible Artifact 和租户强制过滤的 PGVector Knowledge。`trpc-data-migrate` 已提供 Redis Session（HashIdx + legacy zset）及版本化本地 JSONL Summary/向量到 PGVector 的维护窗口迁移：分布式冻结、断点扫描、逐对象 SHA-256、shadow/canary/drain/finalize 门禁、显式 rollback、epoch/sequence 旧摘要保护和失败续跑；配置切换仍由发布系统完成，命令只在当前配置证明已切到目标后确认 cutover。发布前仍须设置 `TEST_POSTGRES_DSN` 跑全矩阵，并完成网络分区、SIGKILL、COMMIT ACK 不确定与恢复 drain 演练。
2. SQL Session 的 `summary: sql` 现在复用同一 strict PostgreSQL Session，摘要写入校验 boundary/version、事件范围、末事件 ID 和 source hash；旧摘要不覆盖新会话范围，异步 job 通过 PostgreSQL lease + `SKIP LOCKED` 可在重启后续跑。InMemory/Redis Summary 仍复用 Session backend（配置必须匹配 type/DSN/namespace）。外部 Mem0 仍只支持读取工具与 transcript ingestion，不开放框架的逐条增删改工具。
3. 水平扩展部分受限：消息管线、Redis 限流、PostgreSQL 用量账本和 007 配置控制面可由多副本共享（Postgres 租约/CAS + fencing）；审批 nonce 在 `coordination=redis` 时跨节点共享，`inmemory` 时仅限单进程。随附 K8s 清单可通过稳定 node_id 承载多副本，但发布仍需所有在线节点 ACK。
4. PostgreSQL 账本未知费用需要人工对账；本次只保证未知费用不自动退款、不跨月漂移，未提供公共修正接口。
5. 通道出站能力为纯文本：无出站附件、卡片、编辑/撤回；企业微信 MediaId 仅入站引用。

## 目录

```text
cmd/trpc-service/      单二进制入口（Gateway + Worker + Admin）
trpcservice/           服务库根（版本号在此）
trpcservice/channels/  Telegram、Slack、企业微信 Adapter
trpcservice/web/       Web 管理页、Webhook、Admin、直接验收 API
trpcservice/worker/    Runner 调用、幂等、Outbox、投递
trpcservice/agent/     按租户/revision 的 Runner 与后端池
trpcservice/tenant/    租户 Registry 与治理 Filter
trpcservice/tenant/governance/ 用户、预算、工具、审批 Filter
trpcservice/coordination/     InMemory/Redis 幂等与 Session lane
trpcservice/log/       租户路由、JSONL、脱敏审计
trpcservice/metrics/   低基数 Prometheus 指标
trpcservice/skill/     Agent Skills 仓库（SKILL.md 加载）
trpcservice/domain/    InboundMessage 等领域模型
trpcservice/tool/      平台内置工具面
trpcservice/queue/     进程内队列与持久 Inbox/Outbox Durable 队列
trpcservice/store/     Inbox/Outbox 持久化（Memory/Postgres、租约、退避、DLQ）
trpcservice/sessionturn/ PostgreSQL Session turn 缓冲、version/fencing 与 replay
trpcservice/tooloperation/ 工具副作用 intent、attempt、fencing 与人工决议账本
trpcservice/config/    配置加载、校验、Secret 引用
trpcservice/workspace/ Agent 运行时工作区（本地开发）
config/                两租户示例
data/                  本地运行的 PID 与日志
api/                   OpenAPI 契约
migrations/            生产控制面 SQL 与版本化 runtime/session 数据面 migration
deploy/                Dockerfile、Compose 与 Kubernetes
scripts/               冒烟测试
observability/         OTel Collector 和 Prometheus
docs/                  赛题逐项设计及验收矩阵
build.sh 等            build/start/stop/clean/coverage/format/lint 脚本
```

依赖上游发布的 `trpc.group/trpc-go/trpc-agent-go`（见 `go.mod`），不携带任何本地框架补丁。敏感 Span 属性（正文、参数、结果、system instructions）在 OTel Collector 端统一二次删除（见 `observability/otel-collector.yaml`），与源端规则互为纵深防御。

## 快速运行

要求 Go 1.24+（见 `go.mod`）。使用 `build.sh`、`start.sh` 或完整冒烟测试时还需要 Node.js 22+ 与 npm；`./build.sh` 会先生成最新 React 控制台并同步到 Go 的嵌入资源，再产出 `bin/trpc-service`、`bin/trpc-migrate` 和 `bin/trpc-data-migrate`，避免新前端连接旧后端二进制。

```bash
cd solution
export ADMIN_TOKEN="$(openssl rand -hex 24)"
go run ./cmd/trpc-service -config config/example.yaml
```

另一个终端执行两轮对话：

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/chat/acme \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"message_id":"demo-1","user_id":"user-1","scope":"direct","text":"第一轮"}'

curl -sS -X POST http://127.0.0.1:8080/v1/chat/acme \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"message_id":"demo-2","user_id":"user-1","scope":"direct","text":"第二轮"}'
```

配置了 `DASHSCOPE_API_KEY` 时响应来自 `glm-5.2`；远端不可用或未设置 Key 时响应包含 `[mock fallback]`。两轮响应的 `session_id` 应相同，再次发送 `message_id=demo-2` 会返回 `duplicate=true`，不会再向 Session 追加事件。

健康和监控：

```bash
curl -sS http://127.0.0.1:8080/healthz
curl -sS http://127.0.0.1:8080/readyz
curl -sS http://127.0.0.1:8080/metrics
```

`/v1/chat` 是受 Admin Bearer Token 保护的本地验收入口，不是公网 IM 接口。生产消息只应进入 `/webhooks/{channel}/{opaque-binding}`。

### Compose 启动

`deploy/compose.yaml` 不再内置数据库密码、管理员令牌或 IM 凭据。启动前在当前 shell 或被 Git 忽略的 `.env` 中注入本地值；下面的命令只在本机生成临时值，不会把它们写入仓库：

```bash
export ADMIN_TOKEN="$(openssl rand -hex 24)"
export POSTGRES_PASSWORD="$(openssl rand -hex 24)"
export POSTGRES_RUNTIME_PASSWORD="$(openssl rand -hex 24)"
docker compose -f deploy/compose.yaml up --build
```

本地 Compose 配置默认关闭 Telegram、Slack 和企业微信连接，避免未配置凭据时误连外部平台。启用某个渠道前，只通过环境变量或 Secret Manager 注入对应凭据，并在配置中显式启用该 binding。停止并清理本地服务：

```bash
docker compose -f deploy/compose.yaml down
```

## 生产部署验收与恢复证据

生产配置使用 Queue PostgreSQL 作为控制面、预算、Session/Summary/Audit 和内容安全的事实源，Redis 提供跨副本协调与限流；管理端必须使用 OIDC/JWKS，Secret 只以 `secret://provider/path#version` 引用解析。`/healthz` 只表示进程存活，`/readyz` 会在必需依赖断连、控制面未同步、内容安全不可用或节点 drain 时返回 503。受保护的 `/admin/v1/health/dependencies` 只返回组件、后端、状态、epoch、延迟和稳定错误类别。

Compose 生产验收环境包含 HAProxy、两个 service 副本、PostgreSQL、Redis、MinIO、OTel Collector、Jaeger 和一次性 OIDC fixture。迁移凭据与运行时凭据分离，服务副本使用容器 hostname 作为独立 node ID，Secret 文件仅挂载到进程并以 0600 生成：

```bash
./scripts/production-acceptance.sh
```

脚本必须实际执行双副本退出/reclaim、PostgreSQL 断连恢复、提交确认丢失 replay、内容安全租约接管、Admin RBAC、依赖详情、trace 查询和 Secret canary 扫描；任何失败、超时、未执行或 `SKIP` 都返回非零。只有脚本成功后，才可把 README、[docs/acceptance.md](docs/acceptance.md) 和 [docs/security-operations.md](docs/security-operations.md) 的生产部署状态标记为通过。

2026-09-10 已实际通过生产 Compose 门禁：双副本退出/reclaim、数据库断连与恢复、提交确认丢失 replay、内容安全接管、OIDC/RBAC、依赖详情、Jaeger 查询和 Secret canary 扫描均通过，结果为 `skips=0`、`unexecuted=0`。脱敏证据保存在 [production acceptance summary](evidence/production/summary-trpc-prod-acceptance-2392023385.txt)。

Kubernetes 推荐清单使用两个副本、RollingUpdate、PDB、反亲和/拓扑分布、先 drain 再停止的 readiness 生命周期，以及 SecretProviderClass/Workload Identity 引用；Audit spool 使用 Compose named volume/Kubernetes RWX PVC，启动和后台 drain 只在 PostgreSQL 提交后删除记录。生产 OTel Collector 会删除 Authorization、Cookie、DSN、错误原文、LLM/工具正文和参数；应用 storage spans 只保留操作、后端、租户 hash、revision cohort、结果和耗时等低基数属性。

## 本机企业微信单聊联调

2026-09-10 已完成一次真实平台联调：企业微信智能机器人成功认证并接收单聊消息，`acme` 租户以 `glm-5.2` 处理后，企业微信出站投递返回成功。运行时指标为 `agent_requests_total{channel="wecom-aibot",result="success"}=1`、`im_delivery_total{channel="wecom-aibot",result="success"}=1`。脱敏记录见 [本机企微联调证据](evidence/production/local-wecom-single-chat-acceptance-20260910.md)。

这证明真实企微平台与本项目的入站、模型、会话路由和回复投递能够连通；默认 `config/example.yaml` 使用进程内队列、会话和本地鉴权，不能据此宣称生产部署验收通过。

### 1. 配置本机密钥

从模板创建只属于本机的配置文件，`.env` 已被 Git 忽略。不要将任何密钥填入 YAML、README 或提交记录：

```bash
cd solution
cp .env.example .env
chmod 600 .env
```

编辑 `.env`，填写模型与机器人凭据；值只从本地 Secret Manager 或手工环境注入，不要写入仓库：

```text
# DASHSCOPE_API_KEY=<由本地环境注入>
# ACME_WECOM_AIBOT_ID=<由本地环境注入>
# ACME_WECOM_AIBOT_SECRET=<由本地环境注入>
```

`DASHSCOPE_API_KEY` 只通过环境变量传入；`config/example.yaml` 只保存 `api_key_env: DASHSCOPE_API_KEY` 这个引用。启动脚本会自动读取 `.env`，显式导出的同名环境变量优先。

### 2. 选择或新增模型

默认 `acme` 租户使用的模型定义如下。当前密钥已实测可调用 `glm-5.2`；如需换模型，只修改 `name`、`base_url` 与 `api_key_env`，不要写入 API Key 本身：

```yaml
model:
  provider: openai
  name: glm-5.2
  variant: qwen
  base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
  api_key_env: DASHSCOPE_API_KEY
  fallback_provider: mock
  max_tokens: 2048
  temperature: 0.2
  streaming: false
```

也可在内置控制台使用“添加自定义模型”创建一个 OpenAI Chat Completions 兼容的运行时租户：填写服务基础地址、模型 ID 和已存在的环境变量名。控制面不会持久化 API Key。

### 3. 添加企业微信智能机器人

在企业微信管理端创建智能机器人并选择“使用长连接”。把 Bot ID 和 Secret 写入 `.env`，再在目标租户的 `channels` 中添加绑定：

```yaml
- type: wecom-aibot
  binding_id: acme-wecom-aibot
  enabled: true
  bot_id_env: ACME_WECOM_AIBOT_ID
  bot_secret_env: ACME_WECOM_AIBOT_SECRET
  max_message_length: 20480
```

一个 Bot ID 同时只能有一个活动连接；启动前先停止可能正在使用同一机器人的其他服务实例。智能机器人使用长连接，不需要配置公网 Webhook URL。

### 4. 启动、验证与停止

```bash
cd solution
export ADMIN_TOKEN="$(openssl rand -hex 24)"
./start.sh
curl -sS http://127.0.0.1:8080/readyz
```

日志出现 `wecom intelligent bot binding acme-wecom-aibot authenticated` 表示机器人已连通。用企业微信测试账号向机器人单聊发送唯一文本，例如 `IM-ACCEPT-20260910-001`，然后检查服务指标：

```bash
curl -sS http://127.0.0.1:8080/metrics | rg 'agent_requests_total|im_delivery_total|model_calls_total'
```

预期看到 `wecom-aibot` 的 `agent_requests_total` 与 `im_delivery_total` 都为 `result="success"`，并在企微客户端确认收到回复。完成后运行：

```bash
./stop.sh
```

机器人单聊联调只验证真实平台连通性。完整生产验收还需在生产依赖和多副本部署上验证持久化、故障恢复、OIDC、Secret Manager 与可观测性，详见上一节和 [验收矩阵](docs/acceptance.md)。

## IM 接入模拟控制台

浏览器打开 `http://127.0.0.1:8080/` 即进入内置控制台（静态资源嵌入二进制，无外部依赖）：

- **多租户模拟登录**：输入 ADMIN_TOKEN 加载租户列表，选择租户 + 模拟用户 ID，登录态与偏好保存在 localStorage，可随时"切换租户"。
- **会话管理**：侧边栏支持搜索（名称/消息内容）、未读筛选、最近/未读/名称排序；每个会话可切换单聊/群聊 scope（对应不同 session 路由规则）、重命名、清空。
- **消息详情与 Trace**：点击任意回复的"详情/Trace"，查看真实请求参数、响应 JSON（可折叠树）、Worker 各阶段处理时间线（会话锁 → 去重 → 治理 → Agent 执行 → 持久化）、工具治理决策。
- **性能监控**：每条消息显示真实 token/成本/延迟；监控面板聚合 token 消耗柱状图、延迟趋势、dedup 缓存命中率环形图，并解析 `/metrics` 展示服务端指标。

推荐用下面的方式启动内置控制台；它会同时重建前端和后端。若服务已经在运行，先执行 `./stop.sh`，再启动以加载新的二进制：

```bash
export ADMIN_TOKEN="$(openssl rand -hex 24)"
./start.sh
```

登录页的“添加自定义模型”会创建一个运行时租户：填写 OpenAI Chat Completions 兼容的基础地址（例如 `https://host/v1`）、模型 ID 与 API Key 的环境变量/secret 引用；如果粘贴的是完整 `/chat/completions` 地址，需要开启“完整URL”。持久控制面拒绝原始 API key，只把 `api_key_env` 写入 immutable revision，创建配置本身不会消耗模型 token；选择新租户进入控制台并发送第一条消息时，后端才会实际向该地址发起 `POST /chat/completions`。

## 接入真实模型

`acme` 当前使用的主模型与回退配置如下：

```yaml
model:
  provider: openai
  name: glm-5.2
  variant: qwen
  base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
  api_key_env: DASHSCOPE_API_KEY
  fallback_provider: mock
  max_tokens: 2048
  temperature: 0.2
  streaming: false
```

仅通过本地 `.env`、环境变量或 Secret Manager 注入 `DASHSCOPE_API_KEY`。模型 Key、IM Token、数据库 DSN 都只能在 YAML 中出现环境变量名，不能出现明文值。`fallback_provider: mock` 按配置顺序优先调用 `glm-5.2`，只在首个有效响应前发生连接、鉴权、限流或 API 错误时回退；未注入 Key 时启动后直接使用 mock。

`config/example.yaml` 内置了第三个租户 `demo`，演示 OpenAI 兼容协议直连阿里云百炼（DashScope）：

```yaml
model:
  provider: openai
  name: qwen3.7-max-preview
  variant: qwen          # 框架默认 base_url 即 dashscope compatible-mode/v1
  api_key_env: DASHSCOPE_API_KEY
```

```bash
# 由 Secret Manager 或当前 shell 注入；不要把真实值写入脚本、YAML 或提交记录。
test -n "${DASHSCOPE_API_KEY:?请先注入 DASHSCOPE_API_KEY}"
curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  --data '{"message_id":"llm-1","user_id":"u1","conversation_id":"u1","scope":"direct","text":"你是谁？"}' \
  http://127.0.0.1:8080/v1/chat/demo
```

## Agent Skills

平台把 `trpcservice/skill/skills` 下的技能目录接入 tRPC-Agent-Go 的技能仓库，`start.sh` 默认设置 `SKILLS_ROOT=trpcservice/skill/skills`（置空可关闭）。技能只注入知识（`skill_load` / `skill_list_docs` / `skill_select_docs`，knowledge-only profile），不注册代码执行器；每个租户必须同时配置 `skills.allow`（资源名授权）和 `tools.allow`（工具授权），运行时继续经过 allow/deny 与二次确认策略。`skills.allow` 缺省或为空时不暴露任何技能；使用框架 `WithSkillFilter` 统一过滤技能说明、加载和辅助文档读取，knowledge-only 不提供脚本执行。

```yaml
skills: {allow: [greeting]}
tools:
  allow: [skill_load, skill_list_docs, skill_select_docs]
```

目录由平台管理员维护，`skills.allow` 使用精确名称，不支持通配符。升级已有租户时应递增 `version` 并显式授权所需技能；示例中的 `acme`、`demo` 已显式授权 `greeting`。旧持久快照缺少该字段时按拒绝所有技能处理，旧 JSON 摘要保持兼容。

## IM Webhook

### Telegram

在 Bot webhook 注册时设置 `secret_token`，并配置：

```bash
export ACME_TELEGRAM_WEBHOOK_SECRET='random-webhook-secret'
export ACME_TELEGRAM_BOT_TOKEN='bot-token-from-secret-manager'
```

Webhook URL 为：

```text
https://<host>/webhooks/telegram/acme-telegram
```

Adapter 先比较 `X-Telegram-Bot-Api-Secret-Token`，验签成功后才解析 JSON。`update_id` 是幂等键；文本、图片和文件都会转换为统一 `InboundMessage`。

### Slack

```bash
export ACME_SLACK_SIGNING_SECRET='signing-secret'
export ACME_SLACK_BOT_TOKEN='xoxb-...'
```

Events API Request URL：

```text
https://<host>/webhooks/slack/acme-slack
```

Adapter 校验 `v0:{timestamp}:{raw_body}` HMAC、拒绝超过五分钟的请求、处理 URL verification，并过滤 bot/subtype 消息防止回复环路。

### 企业微信

```bash
export ACME_WECOM_CORP_SECRET='self-built-app-secret'
export ACME_WECOM_CALLBACK_TOKEN='callback-token-configured-in-wecom-admin'
export ACME_WECOM_ENCODING_AES_KEY='43-char-encoding-aes-key'
```

配置中 `workspace_id` 填 corpid、`application_id` 填自建应用 agentid。接收消息服务器 URL：

```text
https://<host>/webhooks/wecom/acme-wecom
```

注册 URL 时企业微信发送 GET `echostr` 挑战，服务端验签、AES-256-CBC 解密并校验 receiveID 后回显明文。普通回调是仅含 `Encrypt` 字段的 XML 信封：先验 `SHA1(sort(token, timestamp, nonce, encrypt))` 与 ±5 分钟时间窗，再解密，并强制比较 receiveID/corpid 与 AgentID/agentid，防止跨企业、跨应用路由。带 `ChatId` 的回调按群聊处理。回复走主动 API：单聊 `message/send`（touser+agentid）、群聊 `appchat/send`（chatid），access_token 按 (corpid, secret) 缓存并在 40014/42001 时刷新重试一次。文本上限 2048 UTF-8 字节，按字节拆分且不切断多字节字符。

### 企业微信智能机器人（API 长连接模式）

按照[企业微信智能机器人 API 模式文档](https://developer.work.weixin.qq.com/document/path/101463)选择“使用长连接”，然后配置：

```yaml
- type: wecom-aibot
  binding_id: acme-wecom-aibot
  enabled: true
  bot_id_env: ACME_WECOM_AIBOT_ID
  bot_secret_env: ACME_WECOM_AIBOT_SECRET
  max_message_length: 20480
```

凭据只放在被 Git 忽略的 `.env` 或 Secret Manager 中：

```text
# ACME_WECOM_AIBOT_ID=<由本地环境注入>
# ACME_WECOM_AIBOT_SECRET=<由本地环境注入>
```

执行 `./start.sh` 后会自动加载本地 `.env` 并建立长连接；日志出现 `wecom intelligent bot binding acme-wecom-aibot authenticated` 即认证成功。该模式无需 `/webhooks/...` 地址。一个 Bot ID 同时只配置一个活动 binding/实例，避免企微用新连接断开旧连接。

平台长度、限流、文件、异步回复和失败策略见 [IM Adapter 设计](docs/im-adapters.md)。

## Redis 与多节点

`globex` 示例租户选择 Redis Session：

```bash
export REDIS_URL='redis://127.0.0.1:6379/0'
```

若要验证跨 Runtime 状态共享，还应把顶层 `coordination.backend` 改为 `redis` 并配置 `redis_url_env: REDIS_URL`，同时让每个租户的 Session 选择 Redis 或下述 SQL、Memory 选择 Redis；对话状态路由因此不需要 HTTP sticky session。

## 对象 Artifact、外部 Memory 与向量 Knowledge

三个生产后端均按 tenant revision 懒加载，YAML 只保存 secret 的环境变量名：

```yaml
data:
  artifact:
    type: object
    provider: s3
    bucket: tenant-artifacts
    # 元数据账本使用低权限运行时连接；独立 migrator 使用 DDL 连接。
    dsn_env: ARTIFACT_RUNTIME_DSN
    migration_dsn_env: ARTIFACT_MIGRATION_DSN
    endpoint: https://cos-or-s3.example.com
    region: ap-guangzhou
    path_style: false
    # 留空时使用 workload identity；静态凭据必须成对提供。
    access_key_env: ARTIFACT_ACCESS_KEY
    secret_key_env: ARTIFACT_SECRET_KEY
  memory:
    type: external
    provider: mem0
    mode: cloud                 # 或 self_hosted
    endpoint: https://api.mem0.ai
    api_key_env: MEM0_API_KEY
    async: true
  knowledge:
    type: vector
    provider: pgvector
    dsn_env: KNOWLEDGE_RUNTIME_DSN
    migration_dsn_env: KNOWLEDGE_MIGRATION_DSN
    namespace: knowledge_1536
    embedding_model: text-embedding-v3
    embedding_dimension: 1536
    embedding_base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    api_key_env: DASHSCOPE_API_KEY
```

S3 对象键中的 AppName 使用平台派生的 tenant/app namespace。对象版本由 PostgreSQL advisory lock 串行分配，只有 `ready` 元数据可读；`upload_pending/delete_pending` 由后台 reconciliation 重放，并在读取时核对 size + SHA-256，避免 LIST 并发覆盖和静默坏对象。对象后端必须分别配置 `dsn_env` 与 `migration_dsn_env`，前者只持有 `schema_migrations` SELECT 和 005 表 DML，后者仅挂载给迁移 Job。Mem0 当前按框架 `Reader + SessionIngestor` 接入，只开放 `memory_search`/`memory_load`；把 add/update/delete/clear 放进外部 Memory 租户的 allowlist 会在启动时 fail-closed。PGVector 查询在 SQL 层固定附加 tenant/app 条件，不能被 Agent filter 覆盖；`trpc-migrate -config config/example.yaml` 使用单独的 migration DSN 创建 extension/维度表，长驻 Runtime 仅验证表和维度。

## 严格 PostgreSQL Session turn 与持久队列组合事务

`sql` Session 可单独使用；当它与 `postgres` 队列组合时，二者必须引用同一个 `dsn_env`，否则配置校验直接拒绝启动：

```yaml
data:
  session: {type: sql, dsn_env: TRPC_AGENT_DB_DSN}
  memory: {type: redis, dsn_env: REDIS_URL, namespace: acme-memory}
  summary: {type: sql, dsn_env: TRPC_AGENT_DB_DSN}
queue:
  backend: postgres
  dsn_env: TRPC_AGENT_DB_DSN
```

SQL 路径在 Runner 前 `BeginTurn`，从一致 snapshot 恢复 event/state；Runner 调用 `AppendEvent`/`UpdateSessionState` 时只更新本地 staging scope。输出通道完整排空且 context 未取消后，Worker 先生成确定性投递分片并把它们写入 canonical replay，再开启一次事务：校验 Session expected version/fencing 与 Inbox owner/attempt/tenant/channel/binding/dedup/partition/deadline，提交 events/state/version/replay，插入全部 Outbox，最后把 Inbox 置为 processed。相同消息的新 Begin 会返回数据库中的 canonical replay，并在新 Inbox lease 下复用同一参与事务；活跃 turn 被接管时会签发更大 fencing token，旧 Handle 无法提交。

严格边界只在 `queue=postgres + session=sql + 相同 dsn_env` 下成立。若队列不是 PostgreSQL，则 Session 仍只保证自身 events/state/version/replay 的原子提交。SQL Summary 同样复用该 Session PostgreSQL；Summary、Memory 水位、用量/审计和工具外部副作用不伪装成 Session COMMIT 的参与者。app/user scoped state 有独立 CRUD，Coordinator result 是提交后的缓存/清理状态。

同名 `dsn_env` 是配置期门禁，持久记录还保存不含凭据和数据库角色的 `database_identity`（由 host、port、database、search_path 摘要得到）。Relay 先比较 Task 与 Inbox 两份 pipeline 元数据和当前 Queue identity；Worker 获取租户 Runtime 后再比较 strict Session identity。v2 `atomic_commit_mode=postgres_same_database_v1` 只有在两份元数据一致且 Queue、Session 和事务连接 identity 相同时才能执行。未来 pipeline 版本、部署期间 transaction participant/strict Session 等原子能力暂缺或 identity 不匹配会停放重试，不运行模型、不消耗死信尝试预算，也不退回双事务；格式非法、部分空值或当前 v2 的 payload/列漂移才确定性进入 dead letter。只有 `pipeline_schema_version=0` 且模式/identity 均为空的升级前 backlog 走兼容 drain。

## 持久 Inbox/Outbox 与多副本

把 `queue.backend` 改为 `postgres` 并提供 DSN（只接受环境变量名）：

```yaml
queue:
  backend: postgres
  dsn_env: TRPC_AGENT_DB_DSN
```

```bash
export TRPC_AGENT_DB_DSN='postgres://user:pass@host:5432/db?sslmode=require'
```

之后消息管线具备耐久性与多副本安全性：

- 回调先写入 `runtime_inbox` 再 ACK，进程崩溃不丢已确认消息。
- Agent 执行（relay）与 IM 投递（sender）分离；Adapter 的纯 `Plan` 先按平台限制确定分片，每个分片在标记处理完成的同一事务里各写一条 `runtime_outbox`，因此一次外部请求对应一个稳定 operation，而不是在 Adapter 内隐藏多次发送。
- 每个 Worker 以 `SELECT ... FOR UPDATE SKIP LOCKED` 只租用一条可立即处理的消息；`batch_size` 作为每进程并发 Inbox 租约上限（不再预取本地批次）。每次尝试使用独立 `lease_owner`，长任务自动续租，所有状态迁移均做 fencing，因此后续记录不会在等待时耗尽租期。PostgreSQL 模式忽略节点传入的绝对墙钟：due、lease 和 reclaim 使用数据库 `clock_timestamp()`，retry 仅接收相对 delay；完成/续租/重试先锁定 attempt 行，再用新的数据库时间复核到期，避免请求等待行锁期间越过租期后仍提交。
- `partition_key` 与 Worker 锁共用完整的 tenant/app/principal/session lane。租约查询只允许每个 lane 最早的 active Inbox（received/retry/processing）或 Outbox（pending/retry/sending）通过；即使队首正在处理或退避到未来也会阻塞同 Session 后继，不同 Session 仍可并发。滚动升级遇到旧空 key 时，会在 tenant/channel/binding 范围保守串行直至遗留 backlog 排空。
- 新 Inbox 的 Task payload 与独立列双写 pipeline protocol v2：非 SQL 任务记录 `disabled`；满足 PostgreSQL transaction participant 与数据库身份能力的 SQL 任务记录 `postgres_same_database_v1`。两份元数据必须完全一致。未来版本、required capability 暂缺或 Queue/Session/事务连接 identity 变化在 Processor/Runner 前停放并延迟重试，不计入 Inbox 的死信尝试预算；部分空值、非法模式/identity 组合或当前 v2 的列/payload 漂移进入 DLQ。两类路径都不会运行模型或降级为 legacy 双事务，并计入相应 pipeline 拒绝观测。
- 升级前 Task 缺少 `pipeline` 且 Inbox 三列为 `0/空/空` 时才判定 legacy。新二进制不为它附加 transaction participant；strict SQL Turn 先提交，随后凭 canonical replay 在第二个事务完成 Inbox/Outbox，失败可恢复但存在短暂中间状态。每次兼容处理计入 `queue_inbox_legacy_pipeline_total`，应在该指标归零且 backlog 排空后再移除旧协议支持。
- 独立 `trpc-migrate` 按序应用不可变 [002 runtime pipeline](migrations/002_runtime_pipeline.sql)、[003 tool operations](migrations/003_tool_operations.sql)、[004 data migrations](migrations/004_data_migrations.sql)、[005 artifact objects](migrations/005_artifact_objects.sql)、[006 usage budget](migrations/006_usage_budget.sql)、[007 config control plane](migrations/007_config_control_plane.sql)、[008 data lifecycle](migrations/008_data_lifecycle.sql) 与 [009 production safety](migrations/009_production_safety.sql)，并记录 SHA-256 checksum；传入 `-config` 时还会迁移各 Artifact 元数据库，并用 Knowledge backend 的 `migration_dsn_env` 创建维度匹配的 PGVector 表。Queue、Session、用量账本、配置控制面、生命周期、内容安全 Adapter 和 Artifact 业务进程只执行校验，Knowledge Runtime 只读校验表/维度，缺版本、checksum、RLS/权限或 schema 漂移均 fail-closed。

Redis Session → SQL 使用单独的维护窗口命令。本地运行 `./build.sh` 后使用 `./bin/trpc-data-migrate`；Docker 镜像内命令位于 `/usr/local/bin/trpc-data-migrate`。先确保全量 Worker 都已升级到支持分布式冻结的版本，再创建迁移；`create` 会登记 004 ledger 并在 coordination Redis 建立持久冻结。冻结至少三分钟后反复执行 `advance`，每次只处理一个可恢复批次：

```bash
export TRPC_AGENT_DATA_MIGRATION_DSN='从一次性迁移 Secret 注入的 004 ledger 读写连接'
./bin/trpc-data-migrate -action create -config /etc/trpc-agent-service/config.yaml \
  -tenant acme -migration-id acme-session-20260904 \
  -source-redis-url-env ACME_SESSION_REDIS_URL -target-dsn-env ACME_SESSION_SQL_DSN

./bin/trpc-data-migrate -action advance -config /etc/trpc-agent-service/config.yaml \
  -tenant acme -migration-id acme-session-20260904 \
  -source-redis-url-env ACME_SESSION_REDIS_URL -target-dsn-env ACME_SESSION_SQL_DSN
```

当状态进入 `cutover` 时，在发布系统中把该租户 `data.session.type` 改为 `sql`，保持 Worker 冻结，再继续 `advance`。`finalize` 完成后命令才自动解冻。自定义 Redis `namespace` 必须在每次调用传相同的 `-source-key-prefix`；未配置时命令与 Runtime 一样使用派生 app namespace。需要回退时先请求 `-action rollback`，部署 Redis 路由后再 `advance`；非 terminal job 的 `unfreeze` 会被拒绝。该流程不支持 Redis Cluster，也不是零停机双写迁移。
- Agent result 与 completed claim 保持相同 TTL；Inbox/Outbox 事务失败时重建相同 operation key、分片与 payload hash，不重复运行 Agent。Outbox 发送同时校验原 tenant 的绑定所有权，并恢复持久化的 W3C trace context。
- Outbox lease 在同一事务创建 `leased` attempt；真正调用平台前先持久化 `dispatched`。明确 429/瞬态拒绝才按 `max(Retry-After, 指数退避)` 安全重试，永久拒绝或耗尽进入 dead letter；网络错误、畸形成功响应、发送后本地落账失败、dispatched lease 过期都进入 `unknown`，阻塞同 Session 后继且不自动重发。`GET /admin/v1/outbox/uncertain` 查看脱敏元数据，`POST /admin/v1/outbox/{id}/resolve` 用 version+attempt CAS 决议。
- Docker Compose 已默认启用该模式（`deploy/service-config.yaml` + Postgres 服务），因此 Queue PostgreSQL 同时承载 006 用量账本、007 配置控制面和 008 Summary/Memory 水位/Audit/迁移对账；K8s 节点以 Pod 名称作为稳定 node_id，审批 nonce 在 `coordination=redis` 时跨节点共享，`inmemory` 时仅限单进程。

## Runtime 资源与后端装配

Runtime 缓存最多容纳 128 个配置实例，包含正在构造的实例。达到上限时按最近使用顺序回收无在途引用、Session/Memory/Artifact 状态均可从持久后端恢复的实例；延迟消息仍按其原始配置快照重建。含 InMemory Session、Memory 或 Artifact（包括默认 Artifact）的实例不能淘汰，否则会丢失用户数据。全部实例忙碌或含内存状态时返回 `ErrRuntimeCapacity`，应切换持久后端或增加节点，不会无限增长或静默丢数据。该容量是实例数上限，数据库连接容量还须结合每实例各 pool 的上限评估。

`agent/manager.go` 只负责实例缓存和生命周期；`runtime_factory.go` 装配模型、LLMAgent 与 Runner；`backend_factory.go` 返回框架数据接口及 strict turn 能力，并统一清理构造失败的资源。平台保留 strict Session、Inbox/Outbox 与 Artifact 恢复语义，不以普通框架存储替代组合事务。

Redis/InMemory Summary 使用框架 `WithSummarizer` 和异步作业，在可摘要事件达到 20 条时触发；LLMAgent 读取当前分支摘要，Redis 可由其他 Runtime 读取已持久化摘要。`summary.type=disabled` 不构造摘要器；SQL 继续使用既有 008 作业与提交边界。Redis/InMemory 摘要队列仍为进程内异步队列，不能宣称与 SQL Summary 相同的持久作业恢复能力。

## 配置热更新与回滚

修改 YAML 时必须递增租户 `version`，再调用：

```bash
curl -X POST http://127.0.0.1:8080/admin/v1/reload \
  -H "Authorization: Bearer $ADMIN_TOKEN"

curl -X POST http://127.0.0.1:8080/admin/v1/tenants/acme/rollback \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

在途任务持久化完整租户快照、`config_revision` 和 generation，refresh/rollback 不会修改其 Runtime、Session、预算或审批路由；新入口按 `tenant + app + session` 的 SHA-256 稳定选择 active/canary。PostgreSQL 控制面以 007 的 immutable revision、generation CAS、release 节点 prepared/applied ACK 和 verified 状态为事实源；YAML 只用于首次 bootstrap 和显式 reload import。`LISTEN/NOTIFY` 负责及时刷新，定时轮询负责重连兜底；PostgreSQL 模式启动会验证迁移并要求 `TRPC_CONFIG_NODE_ID`，失败关闭而不切内存。reload/rollback 创建异步 release，只有所有目标节点 applied 后才显示 `verified`。

## 测试与构建

```bash
cd im-console && npm ci && npm run build && cd ..
go mod verify
go test ./...
go test -race ./...
go vet ./...
./build.sh
bin/trpc-service -h
bin/trpc-migrate -h
bin/trpc-data-migrate -h
./scripts/smoke.sh   # 前端构建 + 单测 + vet + 三个命令构建/-h + HTTP 端到端冒烟

# 发布门禁：通过环境变量注入隔离测试数据库，缺失时脚本直接失败
TEST_POSTGRES_DSN='postgres://user:pass@127.0.0.1:5432/test?sslmode=disable' \
  ./scripts/postgres-acceptance.sh

# 生产 Compose 门禁：任务专用容器/网络/卷由脚本创建并在退出时清理
./scripts/production-acceptance.sh
```

PostgreSQL 验收入口先在任务专用隔离临时数据库应用并校验当前迁移版本，再覆盖 migrations、007/008 生命周期、009 内容安全及所有真实数据库测试所在包；强制 `-race -count=1`，拒绝跳过测试。输出只展示包状态和失败/跳过的测试名称，原始诊断在退出时删除，避免连接错误泄露 DSN。普通 `go test ./...` 在缺少数据库时的 skip 不算发布验收通过；只有该门禁所有包实际通过且无 `SKIP` 才能宣称真实 PostgreSQL 验收通过。

本次验收记录以实际命令输出为准：普通测试、`-race`、`go vet`、`./build.sh`、三个命令 `-h` 和 smoke 均退出 0；smoke 的本机 `promtool` 语义检查因未安装而跳过，但 HTTP smoke 通过。2026-09-10 的隔离临时 PostgreSQL 门禁 14 个包全部 `PASS`、无 `SKIP`；生产 Compose 门禁结果为 `passed`、`skips=0`、`unexecuted=0`。普通测试中的数据库 skip 不替代这两个无 skip 的发布门禁。

测试覆盖严格 YAML/env 引用、三类 IM 验签/解密与结构化发送结果、Unicode/UTF-8 分片、私聊/群聊 ID、附件预算边界、跨租户隔离、同 Session FIFO/跨 Session 并发、重复投递、pending result 重放、Outbox 批量原子提交、稳定 operation key、attempt fencing、`leased/dispatched` 崩溃恢复、`unknown` 停重试/阻塞/人工 CAS 决议、Retry-After 与 dead letter、SQL Session turn commit/replay/version/fencing、同库组合事务、pipeline metadata/identity fail-closed、legacy drain、002–009 migration checksum，以及 side-effect guard 的 deny 不落账、semantic replay、generic error→unknown、typed safe retry、Runner 生命周期接线。共享 Redis 限流、实际 provider 调用逐次计费、消息 replay 不扣费、retry/failover 新调用、预算超额、unknown 不退款和跨月结算也有单元/集成覆盖。008 还覆盖 SQL Summary 边界/version/source hash、跨节点 Memory watermark、SQL Audit 幂等/hash chain/spool 恢复、向量迁移逐对象对账、epoch/sequence 旧摘要保护、失败续跑和 selective rollback；009 覆盖内容安全 hash/租约/接管/blocked fail-closed。只有隔离临时数据库门禁所有包通过且无 skip 才能写入通过结论。

本轮补强还覆盖审批参数键顺序/空白等价、嵌套大整数保持区分、审批 nonce 不因精度碰撞，以及迁移任务对错误目标数据库、错误租户/app namespace 的拒绝和 `complete`/`rolled_back` 仅按正确路由解冻；对应单元测试使用 fake store/unfreezer，不替代真实 PostgreSQL 门禁。

## 设计文档

- [总体架构与节点拓扑](docs/architecture.md)
- [后端抽象、同步、一致性与迁移](docs/data-consistency.md)
- [Telegram / Slack / 企业微信接入](docs/im-adapters.md)
- [治理、安全、监控、故障、发布和容量](docs/security-operations.md)
- [赛题逐项验收矩阵](docs/acceptance.md)
- [生产控制面数据模型](migrations/001_schema.sql)
- [Runtime Queue + strict Session migration](migrations/002_runtime_pipeline.sql)
- [Tool side-effect operation migration](migrations/003_tool_operations.sql)
- [Backend data migration ledger](migrations/004_data_migrations.sql)
- [Artifact object recovery ledger](migrations/005_artifact_objects.sql)
- [Model usage and budget ledger](migrations/006_usage_budget.sql)
- [Persistent configuration control plane](migrations/007_config_control_plane.sql)
- [Data lifecycle consistency and recovery](migrations/008_data_lifecycle.sql)
- [Production content-safety lease and recovery](migrations/009_production_safety.sql)

## 最小实现与生产推荐的边界

本仓库的单二进制、JSONL audit 和内存审批是可执行的最小部署，用于评审和本地验证。消息管线提供三档：`inmemory` 为进程内演示队列，`memory` 为同 Session FIFO/投递状态机但不跨重启，`postgres` 为持久 Inbox/Outbox、工具副作用 operation ledger 和模型用量预算账本。只有 `coordination.backend=redis` 提供跨节点共享限流，只有 `queue.backend=postgres` 提供持久预算和双节点总额度保证；`control_plane.backend=postgres` 才提供持久 revision、release 历史、跨节点刷新和总节点 ACK；SQL Summary、Memory watermark、SQL Audit 和迁移对账必须使用 Queue PostgreSQL 的 008，Content Safety 租约必须使用 009。`inmemory`/`memory` 只用于本地/demo/测试，不能作为生产配置事实源；InMemory watermark 只声明 node-local，Mem0 只声明 eventual/accepted。Session 另可选 `sql`，提供版本化 event/state 与 fencing/replay 提交；当它与 PostgreSQL 队列使用相同数据库 identity 时，Session turn、Inbox processed 和 Outbox insert 已收敛到一个事务。Artifact 可选带 SQL 恢复账本的 S3-compatible 后端。升级协议只允许全零/空 legacy backlog 临时走可恢复双事务；新 required 记录 fail-closed。未知用量/审计失败状态不自动退款或静默丢弃；消息 replay 不扣费，实际 retry/failover 生成新调用并计费。向量 rollback 只删除本次 migration 写入且 hash 未变化的对象，冲突对象保留并可对账。审批已按 coordination 配置接入 Redis；Redis 持久化/故障切换和 provider reconciliation 仍需部署验收。
