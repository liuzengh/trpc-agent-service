# 租户治理、审计与可观测性

运行时固定执行以下链路：binding 凭证认证、租户路由、canonical identity、Inbox
幂等 claim、binding 级用户/群聊 ACL、身份/预算校验、工具可见性、工具执行与审批、输出脱敏、租户审计。
`Processor.Policy` 是必需依赖；缺失时请求 fail closed。工具同时受 tRPC-Agent-Go 的
`WithToolFilter`、`WithToolExecutionFilter`、`WithToolPermissionPolicy` 和最终
`Guarded.Call` 保护，直接调用不能绕开 tenant/request scope。每个 Runtime Bundle 还安装
租户 `GovernancePlugin`，在 Runner 事件进入持久化或观察者前递归脱敏；Worker 最终输出层
再次脱敏，避免组合 Agent 或第三方 Tool 的事件旁路治理。

MCP 与 HTTPS 业务工具也进入同一条链路。配置发布只把显式命名且列入
`tools.allow` 的工具放进版本固定 Catalog；远端 metadata 在安全包装后继续保留，所有
结果、callback payload 和 metadata 在进入 Agent Event 或审计前递归脱敏。MCP 与业务
接口的原始错误正文不会返回给模型或写入审计。

`require_approval` 工具不会自动执行。Worker 发布 `run.approval_required`，审批方使用
认证后的 `POST /v1/gateway/approve` 提交 `request_id` 与 `tool_name`；共享审批存储唤醒
持有请求的 Worker，Worker 调用 guarded tool，并用 `model.NewToolMessage` 在原 session
续跑模型。本地 `MemoryApprovals` 仅供测试；生产组合使用 PostgreSQL 共享 ApprovalStore，
以 tenant/request/tool 为复合键，支持多节点原子审批、过期和审计。

预算先按输入估算和 `max_tokens` 原子预留，再按模型 usage reconciliation；request token 超限会取消 Runner。月度成本预算启用时，模型配置必须包含价格版本以及每百万输入/输出 token 的微成本。Worker 收到 provider usage 后分项核销，并把实际 `cost_micros` 写入 Metrics 和 Audit；provider 未返回 usage 时保留保守预留值并标记 `reserved_estimate`。

审计遵循 tenant `AuditPolicy`：`enabled=false` 不写；`store_content=false` 不保存错误
正文；`redact_fields` 在结构化字段写入前生效；`RetentionDays` 由定时任务调用
`audit.RetentionWorker` 自动执行 `audit.PruneTenant`，多节点使用 PostgreSQL advisory lock
串行化每轮清理。审计写使用两秒 deadline，稳定 audit ID 使 PostgreSQL和外置 WORM
重复 append 幂等。`audit.fail_closed=false` 时失败只记录低基数
`operation=audit,status=failed` 指标；`true` 时成功请求必须在 Inbox complete 前完成审计，
失败会进入 Inbox retry，并利用 durable execution stage 恢复而不重复已保存的 Runner 结果。

发布配置只保存 `SecretRef`。env/file 仅供离线开发和测试；持久化生产入口只接受 Vault/KMS，
且 key 必须位于 `tenant_id/app_id/...` namespace。`vault` 使用 Vault KV v2-compatible
HTTPS GET，`kms` 使用内部 KMS-compatible HTTPS POST。Endpoint 与 bootstrap token 分别由
`TRPC_AGENT_VAULT_*`、`TRPC_AGENT_KMS_*` 注入，客户端固定五秒 timeout、限制 1 MiB 响应，且所有
错误均为不含 endpoint、key、token 和响应正文的通用错误。未配置 Provider 或非 HTTPS endpoint
一律 fail-closed。

生产入口由 `otelhttp` 从 HTTP `traceparent` 提取并传播到队列，覆盖 callback、Inbox、
lease、Runner、model stream、Tool、Session、Summary/Memory 和 Outbox。metrics 标签只允许
tenant、app、channel、operation、status；request/user/session/message ID 只允许进入
trace/audit。SDK 经 OTLP/gRPC 输出到 Collector，Collector 再向 Prometheus 暴露指标；默认与 tenant redactor 会处理日志、SSE error、Inbox last_error、回复和
审计 details，tRPC-Agent-Go 的上下文 logger 也在 Bundle 构建时安装脱敏包装。

`allowed_users` 和 `allowed_chats` 使用经过验签后得到的平台稳定 ID，并随不可变配置版本进入
Worker。两个列表都为空时兼容现有绑定并允许全部；`allowed_users` 限制单聊和群聊发送者，
`allowed_chats` 限制群聊。仅设置 chat 白名单时直聊默认拒绝。拒绝在 Runtime Bundle/Runner
创建前完成，Inbox 标记为 `rejected`，审计只保存脱敏身份和 `decision=deny`，错误不回显名单或
外部用户 ID。相同用户或群 ID 在不同租户独立判断，不能复用另一租户的 ACL。

生产模型、Storage、迁移、IM 入站/出站、Audit 和 MCP/HTTPS Tool 的凭据都经过租户作用域
Resolver。授权由服务端 namespace 决定，解析不会创建 ownership，也不存在首次使用者抢占；
发布失败不会留下授权副作用。旧配置版本从 PostgreSQL 读取时再次执行同一生产门禁。
Secret 值只在客户端构建和请求头注入时短暂存在，错误、日志和 trace 不包含 key 或 value。

## 监控指标

| 指标 | 建议维度 | 用途 |
| --- | --- | --- |
| `agent.requests`, `agent.operation.duration` | tenant, app, channel, operation, status | 请求量、错误率和模型/Tool/治理操作耗时 |
| `agent.model.first_token.duration` | tenant, app, channel, operation, status | 模型首事件耗时 |
| `agent.im.delivery` | tenant, app, channel, status | IM 投递成功率和平台限流结果 |
| `agent.tokens` | tenant, app, operation, status | 已接入的 token 消耗与预算核对 |
| `agent.cost.micros` | tenant, app, operation, status | 按 provider usage 和请求固定的价格版本记录实际微成本 |
| `agent.model.usage_missing` | tenant, app, channel, operation, status | Provider 未返回 usage；继续保留成本预留并触发告警 |
| `agent.storage.operation.duration`, `agent.storage.operation.errors` | tenant, app, domain, backend, operation, status | 真实 Session/Memory Adapter 调用延迟和错误率 |
| `agent.queue.depth`, `agent.outbox.backlog` | tenant, queue, status | Inbox/Outbox/DLQ 排队与故障恢复状态 |
| `agent.worker.live`, `agent.storage.health`, `agent.storage.healthcheck.duration` | domain, backend, operation, status | Worker 存活与平台 PostgreSQL 健康检查 |

`binding_id` 只有在绑定数量受控时才能作为 metrics 标签，否则只进入 trace。延迟指标使用 histogram，并按部署基线设置 p95/p99 告警；错误率、队列等待和成本可以按租户及全局聚合。

## 审计字段

每条审计记录包含 `tenant_id`、`channel`、`user_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency_ms`、`error_type`、`cost_micros`、`config_version`、`policy_version` 和 `trace_id`，并保留 `request_id`、`event_id` 与脱敏后的 `details_json`。Worker 的策略版本与入站固定的配置版本一致；价格版本和成本依据保存在脱敏 details 中。密钥、Authorization header、Cookie、模型原始请求及未获授权的消息正文不得写入日志、trace 或错误报告。
