# 最终验收映射

本文映射的是题目要求、代码和可重复测试，不等同于所有外部系统已经完成生产联调。企业微信、Telegram、Kubernetes、云 Secret Manager 等能力的实际验证层级见[功能实现与验证状态](feature-status.md)。

## 1. 多租户与节点化

- Tenant/App/Revision/Channel/Backend/Audit/Quota 模型：`controlplane`、migration 001；
- Gateway/Relay/Worker/Sender/Jobs/Admin 六角色：`config/role.go`、Kubernetes manifests；
- Binding 反查 tenant/app，Storage Scope 强校验：`routing`、`runtimecontext`；
- Session Router + Redis Coordinator + Redis Idempotency 支持无 sticky、多 Worker；
- Revision stable/canary、稳定哈希与 conversation pin；
- Admin Principal RBAC、SecretRef、日志/Audit 脱敏。

## 2. 数据同步与多后端

- Session：startup/InMemory/Redis/PostgreSQL；
- Memory：InMemory/Redis/PostgreSQL；
- Knowledge：InMemory/Qdrant，Hash/OpenAI Embedder；
- Artifact：InMemory/S3-compatible/MinIO；
- PostgreSQL 保存控制面、Inbox/Run/Outbox、Approval、Tool Journal、Background Job、Migration、Audit；
- Session Coordinator + fencing token、外部 message ID 幂等、Transactional Outbox、Redis Streams pending reclaim；
- Summary/Memory/Knowledge durable job、水位、重试/dead；
- Backend Migration 双写、回填、verify、cutover、rollback、repair backlog。

## 3. IM 接入

- 企业微信 Adapter：SHA1 验签、时间窗、AES-CBC/PKCS7、CorpID、Token cache、应用文本消息、429/Token 刷新；当前由模拟协议测试覆盖，真实企业账号联调待完成；
- Telegram Adapter：Webhook Secret、private/group/topic Session、sendMessage、Retry-After、群白名单、mention/command/reply 识别和其他 Bot 过滤；真实 Bot 已完成基础联调，群策略用户反馈复验通过；429 调度与重试终态由模拟 API + MemoryJournal 测试覆盖，真实限流待验证；
- 用户/群/线程经 binding-scoped hash 生成隔离身份；
- 文本、图片和文件 ID 可规范化；当前出站仅支持文本，默认不自动下载媒体；
- 重复 callback 由 `(channel_binding_id, external_message_id)` 唯一约束处理；
- Reply Sender 长度切分、重试、provider receipt；
- 危险 Tool 使用原 IM 会话文本批准/拒绝。

## 4. 治理、监控和安全

- tRPC-Agent-Go ToolFilter、PermissionPolicy、MaxRunDuration；
- Model Callbacks Guardrail：输入阻断、输出正则脱敏；
- Tool Approval 精确绑定 tool + arguments hash；
- Tool Execution Journal 阻断盲目重放；
- Local/Redis rate、concurrency、daily token/cost quota；
- OTLP traces/metrics，HTTP → Queue → Worker → Storage → Reply traceparent；
- tenant-scoped Audit Query 和必需审计字段；
- Secret 不写日志、trace、配置仓库。

## 5. 故障恢复与运维

- SIGINT/SIGTERM root Context + errgroup；
- Runner Event channel 始终消费到关闭；
- Worker/Job/Outbox lease 到期后可 reclaim；
- Redis/PostgreSQL 不可用时 readiness 退出，不创建空 Session；
- 模型/Tool/IM 错误分类、退避、dead 状态和人工 retry；
- Revision 乐观锁发布、canary、回滚；
- Docker Compose、非 root Docker 镜像、migration command；
- Kubernetes HPA/PDB/NetworkPolicy/resources；
- load generator、容量公式、fault drill。

## 6. 自动与真实验收命令

```bash
go test ./trpcservice/reply -run TestSenderTelegramRetryLifecycle -count=1 -v
go test -race ./...
go vet ./...
./lint.sh
./build.sh
docker compose --profile observability config -q
docker build -t trpc-agent-service:local .
./scripts/e2e-multiprocess.sh
./scripts/e2e-observability.sh
./scripts/benchmark-local.sh
./scripts/e2e-backup-restore.sh
```

依赖型集成测试通过环境变量显式启用：

```bash
TEST_POSTGRES_URL='postgres://...' go test ./trpcservice/storage ./trpcservice/controlplane ./trpcservice/background ./trpcservice/toolexec
TEST_POSTGRES_URL='postgres://...' go test ./trpcservice/approval -run TestPostgresDecisionContractIntegration
TEST_S3_ENDPOINT=http://127.0.0.1:9000 go test ./trpcservice/storage -run S3Integration
TEST_QDRANT_HOST=127.0.0.1 TEST_QDRANT_PORT=6334 go test ./trpcservice/storage -run QdrantIntegration
```

只读工具的真实模型预检和已通过的 Telegram 工具验收步骤见[工具调用上手说明](current-time-tool-walkthrough.md)，实际证据见[2026-09-06 验证记录](validation/current-time-2026-09-06.md)。

审批测试、真实模型预检、合法命令基础收发及新版格式拦截/拒绝回执复验见[审批验证记录](validation/approval-2026-09-06.md)。步骤见[审批上手说明](telegram-approval-walkthrough.md)，新版批准结果正文仍待真实 Telegram 复验。

## 7. tRPC-Agent-Go 复用边界

直接复用：LLMAgent、Runner/Event、Session Redis/PostgreSQL、Memory Redis/PostgreSQL、Memory Tools/Extractor、Knowledge/VectorStore/Qdrant、Artifact/S3、Model/Tool Callbacks、PermissionPolicy、OpenTelemetry。

平台新增：Tenant/App/Revision/Binding、Storage Router、Channel Adapter、Inbox/Outbox、Redis Streams Worker、Session Coordinator、Approval、Tool Journal、Background Job、Backend Migration、RBAC/Quota/Audit、部署与运维工具。
