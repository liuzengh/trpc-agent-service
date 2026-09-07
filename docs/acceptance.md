# 最终验收映射

本文映射的是题目要求、代码和可重复测试，不等同于所有外部系统已经完成生产联调。企业微信、Telegram、Kubernetes、云 Secret Manager 等能力的实际验证层级见[功能实现与验证状态](feature-status.md)。

`0.2.0-rc.3` 已补齐此前核对的六类代码缺口，配置、代码入口和自动/隔离验证见[补齐记录](code-gap-closure.md)。本次不把新代码标记为已部署到日常实例。

`0.2.0-rc.4` 的后续修复与 MCP/附件实现见[后续记录](reliability-followup.md)。其中 Session/Memory 的切换现在同样要求服务器验证证明，分段回复与并发名额有独立所有权记录。只有这里明确列出的限类型附件已实现，不能扩大到完整多媒体。

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

另有 **`wecom_mcp`**：复用 tRPC MCP Client，主动读取已授权群文本并以机器人身份发送；基础真实模型自动回复、数据库状态与完整 trace 已核对。指纹去重、单条异常隔离、页级失败保留进度、版本化恢复和 unknown 不重发均有自动测试。它不是上面的企业微信自建应用回调，不能混写验证状态；细节见[运行链路](wecom-mcp-runtime.md)。

## 4. 治理、监控和安全

- tRPC-Agent-Go ToolFilter、PermissionPolicy、MaxRunDuration；
- Model Callbacks Guardrail：输入阻断、输出正则脱敏；
- Tool Approval 精确绑定 tool + arguments hash；
- Tool Execution Journal 阻断盲目重放；
- Local/Redis rate、concurrency、daily token/cost quota；
- OTLP traces/metrics，HTTP → Queue → Worker → Storage → Reply traceparent；
- tenant-scoped Audit Query 和必需审计字段；
- Secret 不写日志、trace、配置仓库。

新增分角色 SQL/Redis 权限生成器与隔离正反向测试、按角色初始化依赖、收窄网络模板、SQL 聚合积压指标和离线 Prometheus 规则测试。真实账号/集群应用和实际通知接收方仍待配置。

## 5. 故障恢复与运维

- SIGINT/SIGTERM root Context + errgroup；
- Runner Event channel 始终消费到关闭；
- Worker 队列/Job/Outbox lease 到期后可 reclaim；**MCP 外部发送尝试没有到期自动重发机制**，unknown/attempting 必须核对；
- Redis/PostgreSQL 不可用时 readiness 退出，不创建空 Session；
- 模型/Tool/IM 错误分类、退避、dead 状态和人工 retry；
- Revision 乐观锁发布、canary、回滚；
- Docker Compose、非 root Docker 镜像、migration command；
- Kubernetes HPA/PDB/NetworkPolicy/resources；
- load generator、容量公式、fault drill。

本轮还通过了 Worker 取消接管、完成确认丢失、不可用队列退避和独立 PostgreSQL 全部平台 schema/合成业务数据恢复测试。恢复点之后的外部副作用不能凭旧备份恢复安全性推断，详见[恢复证据](validation/recovery-2026-09-06.md)。

## 6. 自动与真实验收命令

```bash
# 默认不加载 .env、不启用继承来的集成环境、不重启服务。
./scripts/regression.sh
# 可选：只增加独立测试容器和离线告警规则，要求镜像已缓存。
TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh
./build.sh
docker compose --profile observability config -q
```

以下命令属于**另外的实验环境/真实联调操作**，不要整段粘贴到日常聊天环境运行。部分历史脚本会加载 `.env`、启动 Compose 或迁移目标库；测试 Redis prefix 不等于隔离了 SQL/IM 通道。需要独立工作目录、测试配置和数据后端，外部读发必须有明确授权：

```bash
docker build -t trpc-agent-service:local .
./scripts/e2e-multiprocess.sh
./scripts/e2e-observability.sh
./scripts/e2e-telegram-tracing.sh
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

这些 DSN/端点只指向受控测试实例；并非所有旧集成测试都会自己创建隔离 schema。近期收尾测试结果见[候选版本记录](validation/release-candidate-2026-09-06.md)，不要把被跳过的外部集成计为通过。

只读工具的真实模型预检和已通过的 Telegram 工具验收步骤见[工具调用上手说明](current-time-tool-walkthrough.md)，实际证据见[2026-09-06 验证记录](validation/current-time-2026-09-06.md)。

审批测试、真实模型预检、合法命令基础收发及新版格式拦截/拒绝回执复验见[审批验证记录](validation/approval-2026-09-06.md)。步骤见[审批上手说明](telegram-approval-walkthrough.md)，新版批准结果正文仍待真实 Telegram 复验。

## 7. tRPC-Agent-Go 复用边界

完整追踪的本地组件预检、真实模型 HTTP 和已通过的真实 Telegram trace 证据见[追踪验证记录](validation/tracing-2026-09-06.md)，查看新请求的方法见[追踪上手说明](telegram-tracing-walkthrough.md)。

直接复用：LLMAgent、Runner/Event、Session Redis/PostgreSQL、Memory Redis/PostgreSQL、Memory Tools/Extractor、Knowledge/VectorStore/Qdrant、Artifact/S3、Model/Tool Callbacks、PermissionPolicy、OpenTelemetry。

平台新增：Tenant/App/Revision/Binding、Storage Router、Channel Adapter、Inbox/Outbox、Redis Streams Worker、Session Coordinator、Approval、Tool Journal、Background Job、Backend Migration、RBAC/Quota/Audit、部署与运维工具。
