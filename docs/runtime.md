# Runtime Bundle 运行时

每个 `(tenant_id, app_id, config_version)` 对应一个不可变的 Runtime Bundle。Bundle 只使用 tRPC-Agent-Go v1.11.2 的公开 API 构建 Agent 和 `Runner`，包括 `WithSessionService`、`WithMemoryService`、`WithArtifactService`、Knowledge 检索工具和 `WithPlugins`。每次执行都会注入可信的规范化应用名和 Inbox 请求 ID；调用方只能提供用户输入，不能覆盖角色或 `RunOptions`。

`workflow.type` 支持 `llm`、`chain`、`parallel`、`cycle` 和 `graph`。每个组合节点都是版本固定的 LLMAgent：Chain 串行执行；Parallel 先并行运行分支，再强制经过 Aggregator；Cycle 必须声明 1–32 次 `max_iterations`；Graph 必须声明存在的 entry/finish、有效 edge，并可限制 `max_concurrency`。配置校验将节点数、循环次数和并发上限限制为 32，避免租户配置制造无界执行。

Runtime Manager 对同一个键只构建一次，同时允许不同租户并行构建。它会拒绝旧配置快照覆盖已激活的新版本，并通过引用计数管理 Bundle 租约。发布新版本时，只有新 Bundle 构建成功后才会淘汰旧 Bundle；如果新版本构建失败，本次请求直接失败，后续请求会继续尝试构建同一个不可变版本，绝不会回退到旧配置执行。旧 Bundle 只继续服务已经钉住旧版本的请求，并在其全部租约释放后关闭。调用方必须在整个执行期间持有租约。

Bundle 负责管理 Runner 事件通道。请求取消时，如果 Runner 支持 `ManagedRunner.Cancel`，Bundle 会调用它，并在有界时间内继续排空事件，避免客户端断开后阻塞 Runner。租户 `GovernancePlugin` 在 Runner 事件管线内递归脱敏，Worker 的 Tool Filter、Execution Filter、Permission Policy 和最终输出脱敏仍构成独立的纵深治理。Bundle 的关闭操作具有幂等性：先关闭 Runner，再关闭其持有的 Session、Memory 等存储服务。

服务入口会向 Bundle 注入按租户选择的 PostgreSQL 或 Redis Runner Session、PostgreSQL/外部 Memory、按租户路由的 PostgreSQL 或 S3 Artifact、可选的 PGVector/Qdrant Knowledge、Inbox、Outbox 和 Audit 服务。Redis 同时用于 lease、fencing token 和跨节点执行事件总线；无论 Runner Session 选择哪种后端，平台 Event/state/fencing 都由 PostgreSQL 保存。

生产模型配置 `deepseek`、`openai` 和 `openai-compatible` 使用 tRPC-Agent-Go 公开的 OpenAI-compatible Model。API 凭据在每个不可变 Bundle 构建时通过 `SecretRef` 解析一次，不会复制到配置快照、日志、Trace 或错误信息中。确定性 quickstart 使用单独的 Mock Model；生产启动会拒绝 Mock Model 和不受支持的存储配置。
