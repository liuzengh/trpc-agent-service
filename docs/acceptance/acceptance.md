# Stage 7 最终验收说明

Stage 7 是本项目最后一个交付阶段，不存在后续验收阶段。验收以 README 的七项标准、公开 HTTP/SSE 行为、Management Console 工作流、Gateway 到 Worker 内部 HTTP 边界和进程/Compose 生命周期为准。

## 一键门禁

```bash
./scripts/stage7-acceptance.sh
```

该命令依次执行格式化、Go 全量测试、race、lint、构建、前端 typecheck/unit/build/e2e、关键纵向测试、文档检查和双 Gateway Compose 验收。通过后生成 `.scratch/evidence/stage7-<commit_sha>-schema-<version>.json`；目录被 Git 忽略，证据必须由当前提交重新生成，不能使用旧的 `.scratch/stage6-compose-recovery.json` 作为结论。没有 Docker 的本地代码检查可显式设置 `STAGE7_SKIP_COMPOSE=1`，但这不构成最终验收通过。

真实模型 smoke 与自动化 fixture 分开：

```bash
./scripts/stage7-live-model-smoke.sh
```

脚本读取被 Git 忽略的 `.env.local`，要求 `OPENAI_BASE_URL`、`OPENAI_API_KEY`、`OPENAI_MODEL=gpt-5.6-luna`，不会打印或写入这些值。常规 CI 使用本地 OpenAI-compatible fixture，不消费真实额度。

真实 IM smoke 同样与自动化 fixture 分开。2026-09-09 已用真人 Telegram 和企业微信客户端分别完成一次真实消息、真实模型和 Provider 回复回环；非敏感证据见 [`live-im-smoke-2026-09-09.md`](live-im-smoke-2026-09-09.md)。该结果不进入常规 CI，复验需要显式使用真实凭据和外部客户端。

## README 对照

| README 验收要求 | 实现/设计证据 | 自动断言 |
| --- | --- | --- |
| 多租户、节点化、同步、多后端、IM、治理监控、恢复 | [`docs/architecture.md`](../architecture.md) | Stage 7 文档检查、Compose |
| tenant/app/channel/session/event/memory/summary/audit 模型 | [`docs/data-model.md`](../data-model.md) | storage、governance、HTTP tests |
| 至少两种 IM，含微信/企微 | 企业微信 WebSocket + Telegram long polling；[`2026-09-09 真实消息 smoke`](live-im-smoke-2026-09-09.md) | provider runtime fixture tests；真实客户端回环 |
| 至少三类后端策略 | [`backend-adapters.md`](../backend-adapters.md) 的 SQL、Redis、向量、对象存储 | SQL/Redis tests；Qdrant/MinIO 真实后端集成测试 |
| 完整消息链与 trace/request | [`core-sequence-diagram.md`](../core-sequence-diagram.md)、Execution Manifest | remote worker、rich storage HTTP tests |
| 至少八项生产风险 | [`production-risks.md`](../production-risks.md) | `verify-docs.sh` |
| 上游复用与平台新增边界 | [`architecture.md`](../architecture.md) 第 1、2、7 节 | build、framework runtime tests |

## 纵向场景

| 场景 | 验收入口 | 精确结果 |
| --- | --- | --- |
| SQLite Gateway 重启 | `TestSQLiteControlPlaneSurvivesGatewayRestart` | Tenant/App/Version/Binding 重启后可查可路由 |
| 真实运行时模型 | `TestOpenAICompatibleModelStreamsThroughPublicChatSSE`、`TestResponsesModelStreamsThroughPublicChatSSE` | Chat Completions/Responses 本地 fixture 经公开 Chat/SSE 返回完成消息；`gpt-5.6-*` 自动使用 Responses API |
| 双 Gateway 共享控制面 | `stage7-compose-acceptance.sh` | A 创建，B 直接读取 App、Governance Policy、Backend Selection 并执行；A 重启后仍可见 |
| Session fencing | `TestPostgresSessionLease*`、Stage 7 Compose | 暂停 A 使 lease 过期，B 接管后从 `session.lease.acquired` 观察更高 token，并以当前 token 终结 A；旧 token 不能写完成终态 |
| 签名远程 Worker | `TestRemoteWorker*`、`TestExecutionManifest*` | 身份/version/trace 贯穿；篡改、过期、未知 key 拒绝 |
| 危险 Tool | governance、remote Tool tests、Stage 7 Compose | approve/重复 approve、reject/重复 reject、Governance outage fail-closed、Worker 断连转 outcome_unknown 且不自动 replay |
| Memory/Knowledge | `TestMemoryKnowledgeArtifactAndTraceCompletePublicWorkflow` | 权威记录影响后续 Runner 输入，跨 Tenant 空集合 |
| IM 执行写 Memory | `TestChatChannelPersistsCompletedReplyAsMemory` | 最终回复在 IM 回复前写入稳定键并携带 fencing token |
| Artifact/Audit/Trace | 同上及 governance HTTP tests | request/trace 关联，跨 Tenant 不可见 |
| 有界关闭 | lifecycle/runtime/web tests | readiness 先撤、Wait 无泄漏、超时 Worker 退役 |
| 故障恢复矩阵 | Stage 7 Compose | Worker loss/recovery、PostgreSQL outage/recovery、模型 timeout、Tool failure、Governance outage、IM retry/duplicate 都按指定 request_id 检查精确终态 |

## 交付状态与限制

比赛要求的参考实现和八份正式文档在 Stage 7 一并交付，入口见 [`docs/README.md`](../README.md)。S3 Artifact 内容和 Qdrant Knowledge 索引已有可运行适配器及真实后端集成测试；Milvus、增量向量 outbox、对象孤儿自动回收和 Kubernetes 仍是设计或部署指导。

PostgreSQL Control Plane 已共享 Tenant、Agent App、Deployment/Version、Channel Binding、Backend Selection、Governance Policy 和配置幂等状态；Audit Event 也由两个 Gateway 共享 PostgreSQL。Compose 为可复现实验拓扑，Worker 的内部 Governance 请求固定使用 Gateway A；确认、执行中 Tool、预算计数和 Platform Trace 等治理运行态仍由该实例持有并持久化到本地卷。真实生产多 Gateway 需要把这些剩余运行态迁入共享事务存储，并为内部 Governance Service 提供高可用路由。该限制不影响 README 比赛环境中的共享配置、跨 Gateway Session fencing 和完整危险 Tool 恢复验收，但属于上线前必须完成的生产化工作。
