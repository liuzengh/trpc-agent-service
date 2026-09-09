# GitHub 实现代码详解

## 1. 代码仓库与交付身份

设计对应的实现代码位于 GitHub：[GodBlf/trpc-agent-service](https://github.com/GodBlf/trpc-agent-service)。上游参考仓库配置为 `upstream`，平台交付以 `origin` 为 canonical repository。验收时使用 `git rev-parse HEAD` 记录实际 commit SHA，避免文档绑定后续会漂移的行号或分支状态。

项目入口是 `cmd/trpc-service/main.go`，核心平台代码位于 `trpcservice/platform/`，Management Console 位于 `frontend/`。`compose.stage7.yml` 给出可复现的双 Gateway、独立 Worker、PostgreSQL、Redis 和 Nginx 拓扑。

## 2. 架构组件到代码的映射

| 组件/能力 | 主要实现 | 关键职责与测试证据 |
| --- | --- | --- |
| 进程入口与角色 | [`cmd/trpc-service/main.go`](../cmd/trpc-service/main.go) | 按 `TRPC_SERVICE_ROLE` 启动 Gateway 或 Worker，装配 Store、Provider、生命周期 |
| 平台契约 | [`trpcservice/platform/contracts.go`](../trpcservice/platform/contracts.go) | Tenant、Gateway/Worker、Runner、Channel、Storage、Audit 的稳定内部端口 |
| Gateway 执行 | [`trpcservice/platform/runtime.go`](../trpcservice/platform/runtime.go)、[`chat.go`](../trpcservice/platform/chat.go) | 版本解析、治理准入、Session 事件、Runner 调度、终态与回复 |
| 独立 Worker | [`trpcservice/platform/remote_worker.go`](../trpcservice/platform/remote_worker.go) | Bearer 校验、HS256 Execution Manifest、内部 SSE 与 Worker Server |
| 上游 Runner | [`trpcservice/platform/framework_runtime.go`](../trpcservice/platform/framework_runtime.go) | AgentFactory、`trpc-agent-go` Runner、流式事件和 Tool Plugin callback |
| 模型适配 | [`trpcservice/platform/framework_runtime.go`](../trpcservice/platform/framework_runtime.go)、[`responses_model.go`](../trpcservice/platform/responses_model.go) | OpenAI-compatible Chat Completions 与 Responses 流式协议 |
| 控制面 | [`trpcservice/platform/control_plane.go`](../trpcservice/platform/control_plane.go) | SQLite/PostgreSQL schema、revision、共享配置和 Lease 表 |
| Session/Memory | [`trpcservice/platform/storage.go`](../trpcservice/platform/storage.go)、[`sql_store.go`](../trpcservice/platform/sql_store.go)、[`redis_store.go`](../trpcservice/platform/redis_store.go) | InMemory/SQLite/PostgreSQL/Redis、幂等 append、投影和 fencing |
| 后端路由 | [`trpcservice/platform/backend_registry.go`](../trpcservice/platform/backend_registry.go) | Tenant Backend Selection、Store lease、替换时有界退休和关闭 |
| 数据迁移 | [`trpcservice/platform/migration.go`](../trpcservice/platform/migration.go)、[`cmd/storage-migrate/main.go`](../cmd/storage-migrate/main.go) | Redis/SQLite 到 SQL 的 checkpoint、checksum、dry-run 与恢复 |
| Artifact/Knowledge | [`trpcservice/platform/rich_storage.go`](../trpcservice/platform/rich_storage.go) | 元数据端口、Tenant 过滤、request/trace 引用和状态 |
| IM Channel | [`trpcservice/platform/provider_channels.go`](../trpcservice/platform/provider_channels.go)、[`provider_runtime.go`](../trpcservice/platform/provider_runtime.go) | 企业微信 WebSocket、Telegram long polling、发送与连接状态 |
| Channel 编排 | [`trpcservice/platform/channel.go`](../trpcservice/platform/channel.go) | Binding、HMAC Mock callback、消息去重、乱序检查和 Delivery |
| 治理与审计 | [`trpcservice/platform/governance.go`](../trpcservice/platform/governance.go)、[`internal_governance.go`](../trpcservice/platform/internal_governance.go) | allowlist、Guardrail、预算、确认状态、Audit、Metrics、Trace |
| 身份与隔离 | [`trpcservice/platform/identity.go`](../trpcservice/platform/identity.go) | Development Identity、Production JWT、服务端 Tenant Assignment |
| 服务生命周期 | [`trpcservice/lifecycle/server.go`](../trpcservice/lifecycle/server.go) | readiness、在途请求等待、取消和有界关闭 |
| 管理前端 | [`frontend/src`](../frontend/src) | Tenant/App/Deployment、Chat、数据、策略、确认、审计和 Trace 工作流 |
| 部署与门禁 | [`compose.stage7.yml`](../compose.stage7.yml)、[`scripts/stage7-acceptance.sh`](../scripts/stage7-acceptance.sh) | 可复现拓扑、构建、测试、race、前端和 Compose 故障验收 |

## 3. 关键接口与执行流

`TenantContext` 只能由受信服务端中间件注入；公开请求中的 tenant 参数不能直接建立权限。`GatewayRequest` 固定 App、Session、请求/trace 标识、Deployment/Version、Policy Revision 和 fencing token。`RunnerAdapter` 是平台对上游 `runner.Runner` 的窄边界，避免在平台层复制 Agent runtime。

Gateway 先从 Control Plane 解析不可变 Deployment Version，再获取 Session Lease，持久化 input/started，并构造 `RunnerRequest`。远程适配器把请求签成短期 Execution Manifest；Worker 同时验证 Bearer Token、kid、签名、expiry 和 `traceparent`，拒绝篡改或过期执行。Worker 通过 AgentFactory 创建上游 Agent/Runner，Runner Event 经内部 SSE 回到 Gateway，再被转换为公开稳定事件。

Tool 的治理不是只在入口做一次静态检查。`governanceRuntimePlugin` 注册到上游 Plugin Registry，在真实 Tool 调用前访问 `ToolGovernance`。危险 Tool 从 pending confirmation 到 approved、executing 和最终结果；Worker 失联后留下的 executing 转为 `outcome_unknown`，不得自动重放。

`StorageAdapter` 提供 Session Event 最小端口，`SessionStore`、`MemoryStore`、`ArtifactStore` 和 `KnowledgeStore` 按语义扩展。SQL 使用 Tenant 组合键、事件幂等唯一约束和 fencing token；Redis 保持同样的外部行为。Backend Registry 在一次请求期间持有 Store lease，配置切换后等待活跃使用者释放再关闭旧实例。

## 4. IM、治理与 Telemetry

企业微信实现是 API 模式智能机器人：`provider_runtime.go` 维护 WebSocket 连接并处理 `aibot_subscribe`、`aibot_msg_callback` 与 `aibot_respond_msg`。Telegram 以 long polling 获取 Update 并调用 `sendMessage`。平台不接受企业微信自建应用的 CorpID、AgentID、EncodingAESKey 或传统 callback 配置。

`BotTenantAllowlist` 以 `(provider, provider account, external subject)` 将外部主体绑定到 Tenant/App。进入 ChannelCoordinator 后按 message ID 去重、按 provider sequence 拒绝乱序，再调用与 Chat 相同的 Agent 执行路径。自动化测试使用本地协议 fixture，不消费真实 IM 消息或真实凭据。单群聊 Session 规则、认证差异和平台限制见 [IM Channel Adapter 设计](im-channel-adapter.md)。

Governance Policy 覆盖 Tool/MCP allowlist、输入输出 Guardrail、外部主体和 provider account 授权、危险 Tool 确认、Tenant 限流、预算与脱敏。Audit Event 和 Platform Trace 使用 `request_id`、`trace_id` 关联 Gateway、Worker、模型、Tool、Storage、Artifact 和 Channel reply。当前指标与 Trace 可由管理 API 检索；生产拓扑可在相同边界接入独立 OpenTelemetry Collector。

## 5. 数据库与迁移实现

Control Plane 在 SQLite/PostgreSQL 中保存带 revision 的序列化配置状态；PostgreSQL 额外保存 `session_execution_leases`。业务 SQL Store 创建 `session_events`、`session_memory`、`artifacts` 和 `knowledge_records`，主键都包含 Tenant。`session_events` 额外对幂等键建唯一约束。

`control-migrate` 是生产 schema 前置命令，服务启动只验证已迁移版本。`storage-migrate` 按 Tenant 迁移 Session Event、Memory、Artifact metadata 和 Knowledge，支持 dry-run、checkpoint、checksum、恢复、源冻结和原子路由切换。Qdrant generation 可从权威 Knowledge 重建；S3 兼容对象存储在内容 checksum 校验成功后发布 Artifact metadata。增量向量 outbox、Milvus 客户端、对象孤儿自动回收仍是后续生产扩展。

## 6. Management Console 与部署

React Management Console 覆盖 Tenant 切换、Agent App、Deployment/Version、Channel Binding、Backend Selection、Chat Session、Memory/Knowledge/Artifact、Governance Policy、Tool Confirmation、Audit、Metrics 和 Trace。生产构建后的资源嵌入 Go 服务，不需要单独部署静态站点。

Stage 7 Compose 先运行 PostgreSQL Control Plane migration，再启动 Worker、Gateway A/B 和 Nginx。两个 Gateway 共享 Control Plane 和 PostgreSQL Session Lease，Nginx 暴露 `8080`。验收专用 `X-Gateway` header 可指定 A/B 以复现 fencing，不属于生产公共协议。

## 7. 能力完成度

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| 多租户控制面与数据隔离 | 已实现 | Development/Production Identity、Tenant 组合键、共享配置 revision |
| 独立 Gateway/Worker | 已实现 | 进程角色、签名 Manifest、内部 SSE、无状态 Worker |
| Session/Memory 多后端 | 已实现 | InMemory、Redis、SQLite、PostgreSQL |
| 企业微信与 Telegram | 已实现 | WebSocket 智能机器人、long polling；fixture 自动验收 |
| Tool/Guardrail/预算/确认 | 已实现 | Runner 调用点治理，`outcome_unknown` 禁止自动重放 |
| Artifact/Knowledge | 部分实现 | SQL/InMemory 元数据和权威内容已实现 |
| Telemetry | 已实现参考能力 | Audit、Tenant metrics、Platform Trace；外部 Collector 为部署扩展 |
| Qdrant/Milvus | Qdrant 已接入，Milvus 仅设计 | Tenant/generation collection、metadata 双重过滤、确定性 ID、重建命令和真实 Qdrant 集成测试 |
| S3 对象内容 | 已接入 | 版本、checksum、Tenant 引用校验和真实 MinIO 集成测试；孤儿自动回收待扩展 |
| Kubernetes | 仅指导 | 当前可执行交付为 Docker Compose |
| 多 Gateway 共享治理运行态 | 生产待补 | Compose 将 Worker Governance 请求固定路由 Gateway A |

## 8. 构建、运行与验收

本地快速启动：

```bash
./build.sh
./start.sh
```

开发模式分开启动后端和前端：

```bash
go run ./cmd/control-migrate
go run ./cmd/trpc-service

cd frontend
npm ci
npm run dev
```

完整 Stage 7 验收：

```bash
./scripts/stage7-acceptance.sh
```

单独执行 Compose 故障矩阵或真实模型 smoke：

```bash
./scripts/stage7-compose-acceptance.sh
./scripts/stage7-live-model-smoke.sh
```

最终验收依次覆盖 Go 格式、全量测试、race、lint、构建、前端 typecheck/unit/build/e2e、关键纵向场景、文档检查和双 Gateway Compose。无 Docker 环境可设置 `STAGE7_SKIP_COMPOSE=1` 完成本地代码门禁，但不能据此宣称最终 Compose 验收通过。
