# 功能配置与命令参考

本文保留原 README 的详细功能说明，命令均在仓库根目录执行。它是按功能查阅的参考，不是需要从头执行的部署脚本。已有 `.env` 和数据库的实例从[运行手册](operations-runbook.md)开始；先了解交付边界请看[基本交付说明](delivery.md)。

## 快速开始

```bash
git clone https://github.com/liuzengh/trpc-agent-service.git
cd trpc-agent-service

./build.sh
./start.sh
```

当前仓库包含一个不需要模型 API Key 的教学 Agent。HTTP 调试接口现在默认关闭；先按[安全配置说明](security-boundaries.md#1-http-调试接口)在 `.env` 中启用并设置独立的 `TRPC_AGENT_HTTP_API_TOKEN`，将该变量加载到当前 shell。只测试 Telegram 则保持关闭。发送第一轮消息：

```bash
./start-mock.sh
```

```bash
curl -sS -X POST http://127.0.0.1:8080/chat \
  -H "Authorization: Bearer $TRPC_AGENT_HTTP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"binding_key":"tutorial-http","message_id":"readme-message-1","user_id":"alice","session_id":"demo","message":"我叫小明。"}'
```

保持相同的 `user_id` 和 `session_id` 再问：

```bash
curl -sS -X POST http://127.0.0.1:8080/chat \
  -H "Authorization: Bearer $TRPC_AGENT_HTTP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"binding_key":"tutorial-http","message_id":"readme-message-2","user_id":"alice","session_id":"demo","message":"我叫什么？"}'
```

切换到真实 OpenAI-compatible 模型：

```bash
test -f .env || cp .env.example .env
```

然后编辑 `.env`：

```dotenv
TRPC_AGENT_MODEL_PROVIDER=openai
TRPC_AGENT_MODEL_NAME="你的模型 ID"
OPENAI_API_KEY="你的 API Key"

# 兼容服务可额外设置：
OPENAI_BASE_URL="https://your-provider.example/v1"
```

如果模型由本机的 `workbuddy2api` 提供，先在一个终端启动转换服务：

```bash
./start-workbuddy2api.sh
```

脚本默认进入 `~/workbuddy2api`，执行 `uv run converter.py --desensitize --log converter.log --api-key 0`，以前台方式运行，按 `Ctrl+C` 停止。日志保存在 `~/workbuddy2api/converter.log`。它不会自动启动 Agent，也不会修改 `.env`。安装目录不同时，可以通过 `WORKBUDDY2API_DIR=/实际路径 ./start-workbuddy2api.sh` 指定（这是脚本环境变量，不从项目 `.env` 读取）。

沿用上述 `--api-key 0` 参数时，Agent 的 `.env` 使用 `OPENAI_API_KEY="0"` 和 `OPENAI_BASE_URL=http://127.0.0.1:8787/v1`，模型 ID 保留已验证可用的配置。该简单 Key 仅用于本地测试，不要将转换服务暴露到公网。

保持转换服务终端运行，再在另一个终端检查模型凭据和连通性、启动完整服务：

```bash
./check-model.sh
./start-real.sh
tail -f data/trpc-service.log
```

这两个脚本会清除当前终端中可能覆盖 `.env` 的旧模型环境变量，但不会打印 API Key。`check-model.sh` 只调用一次模型，不启动 PostgreSQL、Redis 或 Agent HTTP 服务。

使用 Redis 保存 Session：

```bash
docker compose up -d redis
```

编辑 `.env`：

```dotenv
TRPC_AGENT_SESSION_BACKEND=redis
REDIS_URL=redis://127.0.0.1:6379/0
REDIS_KEY_PREFIX=trpc-agent-service
TRPC_AGENT_SESSION_TTL=0s

# 单进程使用 local；多 Agent Worker 使用 redis。
TRPC_AGENT_COORDINATOR_BACKEND=redis
TRPC_AGENT_COORDINATOR_LEASE_TTL=30s
TRPC_AGENT_COORDINATOR_RENEW_INTERVAL=10s
TRPC_AGENT_COORDINATOR_RETRY_INTERVAL=50ms

# 多 Worker 使用 redis；重试同一消息时必须复用 message_id。
TRPC_AGENT_IDEMPOTENCY_BACKEND=redis
TRPC_AGENT_IDEMPOTENCY_PROCESSING_TTL=2m
TRPC_AGENT_IDEMPOTENCY_COMPLETED_TTL=24h
TRPC_AGENT_IDEMPOTENCY_RENEW_INTERVAL=30s
TRPC_AGENT_IDEMPOTENCY_POLL_INTERVAL=50ms
```

依赖就绪检查：

```bash
curl -sS http://127.0.0.1:8080/readyz
```

Redis 接入后的启动装配、首轮 Session 创建、历史恢复、模型消息构造和 Event 回写链路，见 [Redis Session 接入后的运行链路](getting-started.md#10-redis-session-接入后的运行链路)。

也可以把 `TRPC_AGENT_SESSION_BACKEND` 设为 `postgres`，复用 `TRPC_AGENT_POSTGRES_URL`，并通过 `TRPC_AGENT_SESSION_POSTGRES_PREFIX` 隔离 tRPC-Agent-Go 的 Session/State/Event/Summary 表。同步持久化保持开启，Worker 返回成功前数据已经对其他节点可见。

同 Session 串行、不同 Session 并行、Redis 租约、续租、安全释放和 fencing token 的链路，见 [Session Coordinator 接入后的运行链路](getting-started.md#11-session-coordinator-接入后的运行链路)。

`message_id` 去重、processing 等待、completed 结果复用和失败重试链路，见 [消息幂等接入后的运行链路](getting-started.md#12-消息幂等接入后的运行链路)。

使用 PostgreSQL 保存租户、Agent App、Revision 和 Channel Binding：

```bash
docker compose up -d postgres
```

```dotenv
TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres
TRPC_AGENT_POSTGRES_URL=postgres://trpc_agent:trpc_agent_dev@127.0.0.1:5432/trpc_agent?sslmode=disable
TRPC_AGENT_POSTGRES_AUTO_MIGRATE=true
TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=true
```

启动、migration、bootstrap 和 readiness 链路见 [PostgreSQL 控制面接入后的启动链路](getting-started.md#13-postgresql-控制面接入后的启动链路)。

Channel Binding、tenant-scoped Runtime 和动态 Agent Revision 编译见 [Channel Binding 路由](getting-started.md#14-channel-binding-到租户-runtime-的路由链路) 与 [Agent Revision Compiler](getting-started.md#15-agent-revision-compiler-运行链路)。

持久化异步入口：

```bash
curl -sS -X POST http://127.0.0.1:8080/inbound \
  -H "Authorization: Bearer $TRPC_AGENT_HTTP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"binding_key":"tutorial-http","message_id":"external-001","user_id":"alice","session_id":"durable-session","chat_type":"direct","message":"hello"}'
```

该接口只在 conversation、inbound、agent run 和 queue outbox 同一事务提交后返回 `202`。详见 [持久化 Inbox 和 Transactional Outbox](getting-started.md#16-持久化-inbox-和-transactional-outbox)。

多进程异步队列配置：

```dotenv
TRPC_AGENT_QUEUE_BACKEND=redis
TRPC_AGENT_QUEUE_STREAM=agent-runs
TRPC_AGENT_QUEUE_GROUP=agent-workers
TRPC_AGENT_QUEUE_CLAIM_MIN_IDLE=30s
```

Outbox Relay、Redis Streams pending reclaim 和 Worker 完成链路见 [异步执行链路](getting-started.md#17-outbox-relayredis-streams-和-agent-worker)。

Reply Sender、通道能力、长度切分、发送重试和回执见 [Reply Sender 和 Channel Adapter](getting-started.md#18-reply-sender-和-channel-adapter)。

生产组件可以使用同一镜像分别启动：

```bash
./bin/trpc-service -role gateway -addr :8080
./bin/trpc-service -role relay
./bin/trpc-service -role worker
./bin/trpc-service -role sender
./bin/trpc-service -role jobs
```

角色职责和关闭链路见 [进程角色拆分](getting-started.md#19-进程角色拆分)。

IM callback 地址：

```text
/callbacks/wecom/{callback_key}
/callbacks/telegram/{callback_key}
```

企业微信验签/AES/Token 和 Telegram webhook/sendMessage 的实现链路见 [企业微信和 Telegram Channel Adapter](getting-started.md#20-企业微信和-telegram-channel-adapter)。

启用控制面管理接口：

```dotenv
TRPC_AGENT_ADMIN_ENABLED=true
TRPC_AGENT_ADMIN_TOKEN="replace-with-a-random-token"
```

Tenant、App、Revision、Channel/Backend Binding 和乐观锁发布说明见 [Admin API 和 Revision 发布](getting-started.md#21-admin-api-和-revision-发布)。

Tool Catalog、租户白名单、用户权限、调用预算和危险工具审批说明见 [租户级 Tool 治理](getting-started.md#22-租户级-tool-治理)。

启用 OpenTelemetry OTLP 导出：

```dotenv
TRPC_AGENT_OTEL_ENABLED=true
TRPC_AGENT_OTEL_ENDPOINT=127.0.0.1:4317
TRPC_AGENT_OTEL_SAMPLE_RATIO=1
```

HTTP、Gateway、持久化任务、Worker 的 trace 传播，运行/Admin/Tool 审计，以及 token 与租户成本指标见 [OpenTelemetry、审计和成本链路](getting-started.md#23-opentelemetry审计和成本链路)。模型价格通过 Revision 的 `model_config.prompt_cost_per_million` 和 `completion_cost_per_million` 配置。

危险工具会创建可恢复的 `tool_approval`。企业微信或 Telegram 用户在原会话回复严格的 `批准 apr_xxx` / `拒绝 apr_xxx` 命令，平台会校验租户、Channel Binding、用户、有效期以及工具参数哈希，再生成幂等 continuation。详见 [可恢复的危险工具审批链路](getting-started.md#24-可恢复的危险工具审批链路)。

Memory 通过 tenant-scoped `AppName` 路由到每个租户选择的 InMemory、Redis 或 PostgreSQL 后端；Revision 可开放 tRPC-Agent-Go 原生 Memory Tool，并配置自动 preload。详见 [租户级 Memory Router](getting-started.md#25-租户级-memory-router)。

Artifact 通过同一 Storage Scope 路由到 InMemory 或 S3-compatible 后端。Compose 提供 MinIO，PostgreSQL advisory lock 保护多节点对同一文件的版本分配。详见 [Artifact Router 与 S3 / MinIO](getting-started.md#26-artifact-router-与-s3--minio)。

Knowledge 根据 Revision 构建 InMemory 或 Qdrant Vector Store，支持 Hash/OpenAI Embedder、切块、Admin 文档导入，并在写入和搜索两端强制 tenant/app metadata。详见 [Knowledge Router 与 Qdrant](getting-started.md#27-knowledge-router-与-qdrant)。

Summary、Memory Extraction、Knowledge Upsert/Delete 通过 PostgreSQL `background_job` 异步执行，支持 lease reclaim、幂等、指数退避、dead job 查询/重试和独立 `jobs` 角色。详见 [Durable Background Job](getting-started.md#28-durable-background-job)。

Backend Migration 通过 `planned → dual_write → backfill → verify → cutover → completed` 状态机执行，支持 Memory/Knowledge 在线双写、分批回填、强校验、repair backlog、乐观锁切换和回滚。详见 [Backend Migration 状态机](getting-started.md#29-backend-migration-状态机)。

Session Router 实现完整 tRPC-Agent-Go `session.Service`，让不同 tenant/app 选择 startup、InMemory、Redis 或 PostgreSQL，并支持 Event/State/Summary 双写迁移、批量回填和切读回滚。详见 [Tenant Session Router](getting-started.md#30-tenant-session-router)。

Admin 支持多 Principal RBAC；Gateway/Worker 支持 Local/Redis 租户限流、并发和每日 token/cost 预算；标准日志和 Audit 分别执行 Secret 脱敏。详见 [RBAC、限流、预算与日志脱敏](getting-started.md#31-rbac限流预算与日志脱敏)。

生产交付包含非 root Docker 镜像、独立 migration/loadgen、完整本地可观测栈，以及 Kubernetes 六角色 Deployment/HPA/PDB/NetworkPolicy。详见 [生产部署与可观测栈](getting-started.md#32-生产部署与可观测栈)、[部署文档](deployment.md) 和 [容量评估](capacity.md)。

灰度 Revision、Model Guardrail Callbacks、Tool Execution Journal、Storage/Reply trace 和媒体安全边界见 [最终链路加固](getting-started.md#33-最终链路加固)。完整本地多进程验收执行 `./scripts/e2e-multiprocess.sh`。

逐条验收映射见 [最终验收文档](acceptance.md)。

只读查看 Agent、依赖和已配置公网入口的状态：

```bash
./status.sh
```

脚本不会启动服务、调用模型生成或发送 IM。完整手动启停说明见[运行手册](operations-runbook.md)。

停止服务：

```bash
./stop.sh
```
