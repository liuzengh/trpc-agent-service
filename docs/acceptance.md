# 验收证据包

本文件定义提交前在干净 WSL 环境中必须重复执行的验收步骤。示例配置、测试数据和容器均不含真实密钥。

## 一、基础质量门禁

```bash
go mod verify
go vet ./...
go test ./...
go test -race ./...
git diff --check -- . ':(exclude)webui/dist/**'
```

通过标准：以上命令全部返回零退出码；不得有未格式化源码、数据竞争或依赖校验失败。`webui/dist` 是 Vite 生成物，第三方模板字符串可能包含有意义或无意义的行尾空白，因此不使用 `diff --check` 检查生成 bundle；CI 会在重新构建后用 `git diff --exit-code -- webui/dist` 单独保证产物与源码一致。

## 二、核心业务验收

| 场景 | 证据 | 通过标准 |
| --- | --- | --- |
| 多租户路由 | `go test ./trpcservice/agent -run TestRuntimeRoutesBindingsToIsolatedTenantRunnersAndRejectsDuplicate` | 同一外部消息 ID 在两个租户分别执行；同一租户重放不重复回复。 |
| Telegram / 企业微信 / 飞书 Connector 体系 | `go test ./trpcservice/channels/... ./cmd/trpc-service` | 三大通道收敛为单一规范用法：企微智能机器人 WebSocket 长连接（`aibot_subscribe`、30 秒心跳、1→30 秒退避、流式回复/模板卡片/媒体）；Telegram Bot API `getUpdates` 长轮询（`offset` 推进与 429 退避）；飞书同一 Binding 复用官方 `channel-sdk-go` 完成 WebSocket 入站与 `Send` 出站（自动 `@bot`、Markdown→Post、分片/重试、媒体类型与上传），仅稳定消息 ID 更新/撤回通过 `RawClient()` 使用底层 OpenAPI；均经统一 `channelConnectorManager` 调度并解耦推入 Kafka。 |
| IM 图片/文件输入 | `go test ./trpcservice/channels/... ./trpcservice/messaging ./trpcservice/agent -run 'Test.*(File|Media|Document|Photo|Artifact)'` | 三通道把供应商媒体下载/解密为统一临时输入；公共 staging 写入 Artifact，Kafka 只带引用；Runtime 通过框架 ContentPart 加载；成功执行后回收输入 Artifact，失败重试不提前删除。 |
| 外部回复分片与节流 | `go test ./trpcservice/messaging -run 'Test.*(DeliveryPolicy|ChannelOutbox)'` | 唯一 Outbox Dispatcher 按平台限制分片；Redis 在 channel/binding/conversation 维度跨 Gateway pacing；等待期间 Outbox lease 持续续租。 |
| 租户模型装配与故障切换 | `go test ./trpcservice/assembly ./trpcservice/config -run 'Test(FactoryUsesTenantModelProvider|FactoryInjectsFrameworkSessionSummaryIntoModelContext|ManagedModelProvider(BuildsFrameworkHuggingFace|SyncUsesSecretResolverAndSharedCatalog|FailoverRecoversFromProviderFailures)|PlatformPolicyValidatorAcceptsFrameworkHuggingFaceProvider|PlatformExample)'` | 单一 `ModelCatalog` 同时供 Validator 与 Runtime 使用；模型凭据只由 `SecretResolver` 解析；框架原生 OpenAI-compatible / 混元 / HuggingFace 可装配；HuggingFace `org/model` ID 不被平台改写；429、5xx、连接失败会由框架 Failover 切到备用模型；Session Summary 会真实进入后续模型上下文。 |
| 工具与 MCP | `go test ./trpcservice/assembly ./trpcservice/tool ./trpcservice/agent ./trpcservice/governance ./trpcservice/netpolicy -run 'TestToolRegistry|TestAgentLevelGovernance|TestDefaultInvocationFactory|TestStaticToolPolicy|TestDurableToolAudit|TestValidatePublic'` | `duckduckgo_search` 直接使用框架工具；固定 HTTPS JSON 工具使用框架 Function Tool 并在调用时经 `SecretResolver` 注入凭据；MCP 使用框架原生 ToolSet，真实测试服务器验证 ListTools、精确过滤和 CallTool，stdio 被配置层拒绝；静态/HTTP/MCP 最终工具名均经同一 exact-name 权限、审批、预算、审计与 Trace；未知/未授权工具无副作用。 |
| 用户偏好 / Knowledge | `go test ./trpcservice/assembly ./trpcservice/storage ./trpcservice/web -run 'Test.*(Memory|Knowledge)'` | Memory 按不可变租户配置选择框架 PostgreSQL / InMemory / Mem0 契约，其中 Mem0 使用框架 ingest-first `SessionIngestor + Tools`；Knowledge 直接装配框架 `BuiltinKnowledge + Embedder + VectorStore + Reranker`，生产后端支持 pgvector / Qdrant，平台不维护第二套 Retriever。控制台按同一租户配置解析 Memory/Knowledge 后端。 |
| Session / Knowledge 后端迁移 | `go test ./trpcservice/assembly ./trpcservice/knowledgeingest ./trpcservice/web -run 'Test(SessionMigrator|KnowledgeMigrator|ConsoleStorageMigration|ConsolePausesKnowledge)'` | Session 迁移使用 `prepared → dual_write → backfill → verify → cut_read → stop_old_write → done`，Backfill/Verify 与 Runtime 共用 Session execution lease；完成前发布新的不可变应用配置。Knowledge 作为派生索引暂停写入后，从首次入库保存的 canonical document snapshot 重建 Qdrant，逐文档校验通过后再发布配置；generation 过期请求拒绝。 |
| 复杂文档 Knowledge 导入 | `go test ./trpcservice/knowledgeingest ./trpcservice/storage ./trpcservice/web` | multipart/text/URL/Repo 原始输入进入持久 `knowledge_ingest_jobs`，任务固定接受时选定的 Knowledge backend；框架 Source/Reader/Docling/Chunking 负责解析和切片，并持久化 canonical document snapshot 作为后续重建事实源；Embedder + 当前 VectorStore 负责索引。任务支持 lease/retry/fencing，删除文档取消任务，旧 Worker 不能继续写入。真实 Repo 测试覆盖 Go/Python/Proto/Markdown Reader。 |
| Framework Artifact | `go test ./trpcservice/storage ./trpcservice/assembly ./trpcservice/tool ./trpcservice/web -run 'Test.*Artifact'` | 后端从注册表打开官方 S3/COS 或平台 PostgreSQL Adapter；`platform.save_artifact` 写入运行产出；控制台仅查看/下载真实输出。 |
| Framework Execution Trace | `go test ./trpcservice/agent ./trpcservice/storage ./trpcservice/web -run 'Test.*(ExecutionTrace|FrameworkTrace|CollectRun|ConsoleExecution)'` | Runner 显式开启 ExecutionTrace；成功/失败 Trace 均来自 completion event；持久化投影不含 Input/Output/Error 原文；`GET /api/v1/execution` 返回一份运行文档，其中框架 Agent Trace 与真实 `tool_executions` 分开投影，工具时间线仅暴露名称、状态、耗时和错误类型，不暴露参数/结果；claim/retry/outbox 仍作为独立可靠性事实，不伪造成 Agent step。 |
| 可靠消息 | `go test ./trpcservice/messaging` | 成功后再提交；失败重试；非法 JSON 携带原始 payload 进入 DLQ 后提交，且同分区下一条合法记录继续处理；动态 Sender 严格使用信封绑定的不可变配置版本和 binding 凭据。 |
| claim、Session fencing 与工具副作用账本 | `go test ./trpcservice/agent ./trpcservice/storage ./trpcservice/tool -run 'Test(ClaimHeartbeat|RuntimePersistsNonZeroSessionExecutionFence|MemoryStateStoreSessionExecutionFence|MemoryExecutionLedger)'` | heartbeat 续租失败会取消执行 Context；每次 Session Run 使用单调 execution lease/fencing token，旧 owner 不能提交；工具 intent 以稳定幂等键持久化，completed 结果重放而不重复执行，执行结果不确定时进入 `outcome_unknown` 并禁止自动重试。 |
| 审计隐私 | `go test ./trpcservice/storage ./trpcservice/agent -run 'Test.*Audit|TestRuntimeEmitsRedactedExecutionTrace'` | 对话相关审计 detail 仅保存 `hmac-sha256:` 摘要，不包含正文/令牌；执行 claim 不重复保存消息摘要；结构化治理审计保持最小可用字段。 |
| 平台用户与登录身份 | `go test ./trpcservice/identity ./trpcservice/web ./cmd/trpc-service -run 'Test.*(PlatformUser|Login|Auth|Provider)'` | 平台用户只由本地账号、企业微信、飞书或 OIDC 登录建立；Channel/Binding 不参与账号归属且不提供 `/link`。公开登录页只展示当前已启用方式；系统管理员登录设置展示完整配置状态，并且企业 Provider 只有完成真实 OAuth 回调后才标记“已验证”。 |
| 登录会话与 CSRF | `go test ./trpcservice/identity ./trpcservice/web ./cmd/trpc-service -run 'TestAuth|TestSession|TestCSRF|TestConsole|TestKafkaHTTP'` | 未登录 401；登录后可访问控制台；写操作 CSRF 拒绝；登出后会话失效；healthz/readyz 免认证。 |
| 租户级授权（INV-01/02） | `go test ./trpcservice/web -run 'TestConsole.*(Member|Admin|NonMember)'` | 非超管仅见成员租户、非成员读 403、member 只读（写 403）、operator/超管可写。 |
| 会话失效审计 | `go test ./trpcservice/identity -run TestSessionMiddlewareRecordsExpireAudit` | 有 Cookie 但会话失效 → `user_session_expire` 审计落库；无 Cookie 不误报。 |
| 观测、计费与退出 | `go test ./cmd/trpc-service ./trpcservice/agent ./trpcservice/metrics ./trpcservice/storage ./trpcservice/tool -run 'Test(ComposeTelemetry|Prometheus|MountPrometheus|ProviderCost|CollectReplyPreservesCached|ObservedFrameworkPostgres|RuntimeEmitsRedactedExecutionTrace|OTel|GovernanceCallbacks)'` | 平台与框架共用同一 OTel Provider；真实流式 LLMAgent 会导出 TTFT、TPOT、输出 token 速率与 GenAI token/耗时指标；Session/Memory/Knowledge PostgreSQL 操作进入平台 Store 指标且不记录 SQL/参数；IM/HTTP 原生 Request ID 仅进入 Span；Prompt cache token 按 Catalog 独立费率计费并进入账本；框架 Langfuse exporter 使用同一 TraceProvider；可选 `/metrics` 直出 Prometheus；平台 Trace 不重复保存对话正文；取消根 Context 后 HTTP 服务按期退出。 |
| 用量预留与未知用量 | `go test ./migrations ./trpcservice/agent -run 'Test(PostgresMigrationsAndStateTransaction|CollectReplyPreservesCachedPromptUsage)'` | 有预算时模型调用先预留 token；并发上限跨 Worker 共享；Provider 未返回 usage 时使用 `settled_unknown` 且保留 reserved token，不把未知伪装成 0；已结算 reservation 不能续租或二次结算。真实 PostgreSQL 语义由 `TEST_POSTGRES_DSN` 集成路径验证。 |
| Fencing Token 乐观锁防护 | `go test ./trpcservice/storage ./trpcservice/agent -run 'Test(MemoryStateStoreSessionExecutionFence|MemoryStateStoreExpiredSessionExecutionFence|RuntimePersistsNonZeroSessionExecutionFence)'` | Runtime 使用真实非零 Session execution fencing token；旧 owner 或过期 lease 的提交被原子拦截，防止接管后的旧 Worker 覆盖新状态。 |
| Execution Manifest 签名契约 | `go test ./trpcservice/messaging ./trpcservice/agent -run 'Test(ExecutionManifestBindsImmutableKafkaEnvelope|KafkaProcessor)'` | Gateway 创建的不可变 Kafka envelope 携带短期签名 Manifest，签名绑定 payload digest、Tenant/App/ConfigVersion/Session/Fencing/Trace；Worker 在读取业务 payload 前验签，payload/manifest 篡改或过期均拒绝。 |
| 企微智能机器人 WebSocket 长连接 | `go test ./trpcservice/channels/wecombot/...` | 模拟企微网关验证 `aibot_subscribe` 握手、心跳保活、`aibot_msg_callback` 消息入站与 Markdown 回复投递。 |
| Telegram Bot API Long Polling | `go test ./trpcservice/channels/telegram -run TestPoller` | 模拟 Telegram API 验证 `getUpdates` 长轮询、群聊触发、媒体下载、429 退避；handler/持久发布失败时不推进 `offset`，恢复后可重试同一 Update。 |

## 三、可选真实基础设施验证

PostgreSQL、Kafka 与 S3 集成测试仅在 Docker 已启动且显式提供环境变量时运行；默认单测绝不访问网络。`quality-gate` 的 `infrastructure` 作业会自动拉起隔离的 PostgreSQL、Redpanda 与 MinIO，并把这些测试作为必过门禁。

本地已有 PostgreSQL 持久卷时，必须继续使用创建该卷时的环境文件；切换到 `deploy/.env.example` 不会更新卷内角色密码。不得为通过测试而修改正在运行的开发管理员密码，应改用匹配原凭据的本地环境文件，或使用独立端口和独立卷的一次性测试数据库。

`deploy/.env.example` 中的密码只用于全新、可丢弃的本地/CI 环境，首次启动前应复制到被 Git 忽略的本地环境文件并替换为随机值；同一个持久卷的整个生命周期内保持该文件不变。

```bash
# 默认使用 Compose project trpc-agent-service-acceptance，
# 端口 15432/16379/29092/19000/19001 和 project-scoped 测试 Volume。
scripts/infrastructure-test.sh all

# 调试时也可显式拆分生命周期；仅 down 该测试 project，不影响开发服务。
scripts/infrastructure-test.sh up
scripts/infrastructure-test.sh test
scripts/infrastructure-test.sh down
```

通过标准：平台控制面仍只使用一个 PostgreSQL `DATABASE_URL` 和一套数据库凭据；平台租户事务在共享数据库连接中切换到内部 `NOLOGIN` 的 `trpc_tenant` 角色，再设置 `app.tenant_id`，所有含 `tenant_id` 的平台表都必须 `ENABLE + FORCE RLS`。框架 Session/Memory 默认可复用该 PostgreSQL，也允许租户显式选择 Redis/Mem0 等框架原生后端；这类外部后端连接只通过受管 `connection_ref` 解析。Schema 始终只由 `migrations/000001_init.sql` 这一份当前 baseline 定义，可在空 Schema 中完整重装；开发阶段直接更新这一文件，不新增历史升级链或 down 文件。平台 Knowledge/Artifact、ExecutionTrace 安全投影、渠道主体映射、active session、归档、用量预留/结算与工具执行账本可用，内容审计为 keyed HMAC；Kafka Consumer 在业务处理或 DLQ 持久化完成后才提交 offset，非法记录不阻塞后续合法记录；S3-compatible Artifact Adapter 完成真实签名读写与版本语义。迁移测试只允许连接数据库名含 `test` 的数据库。

2026-09-11 实际验证：`scripts/infrastructure-test.sh all` 使用独立 Compose project、15432/16379/29092/19000 测试端口和一次性 project-scoped Volume，PostgreSQL 16/pgvector、Redis、Redpanda、MinIO 及 Bucket 初始化成功；migrations、tenant、identity、messaging、storage 全部通过，结束后仅删除测试 project 的容器、网络与 Volume。迁移测试在第二次重装当前 baseline 后会恢复 `trpc_tenant` 的 Schema、表与序列权限，后续 Repository、RLS、Artifact 和 Knowledge 测试不再依赖残留授权。

## 四、Kind 真实集群验收

本地与 CI 共用 `scripts/kind-e2e.sh`。脚本创建或复用 `trpc-agent` Kind 集群，构建并导入应用与 mock OpenAI 镜像，部署 PostgreSQL、Redis、Redpanda、MinIO，初始化 `trpc-artifacts` Bucket，执行向前迁移，然后部署应用、HPA、PDB 与 NetworkPolicy。

```bash
scripts/kind-e2e.sh
```

通过标准：五个依赖 Deployment 均 Ready；MinIO Bucket 与数据库迁移 Job 均 Complete；两个应用副本均为 `2/2 Running` 且重启数为 0；`/readyz` 返回 HTTP 204；`scripts/kubernetes-smoke.sh` 返回零退出码。证据保存在 `deploy/kubernetes/kind/evidence/{pods,jobs,readyz,smoke}.txt`，CI 的 `kind-e2e` 工作流始终执行完整镜像构建并上传同一目录。

## 五、提交前人工复核

- 确认工作树不含访问令牌、聊天正文、私钥、`.env` 或个人绝对路径。
- 确认所有外部入口均经签名校验；所有工具调用均经框架 Tool Callbacks（`NewGovernanceCallbacks`），且仅在 `assembly` 装配点挂载。
- 确认运行时依赖由组合根注入；Session/Memory/Knowledge 只通过租户不可变 `StoragePolicy` 选择已注册的框架原生后端，Memory 不自造 Mem0 Service 契约，Knowledge 不维护第二套 Retriever；Artifact 同样只从注册表解析 PostgreSQL/S3/COS Adapter。
- 保存上述命令输出，连同 PostgreSQL/Kafka/S3、浏览器 E2E、镜像与 manifest 门禁日志作为评审附件。

## 六、当前代码状态与边界

本验收包以当前仓库代码为准，而不是以目标架构图替代实现证据。

- 已具备：单一平台级 Model Catalog，框架原生 OpenAI-compatible / 腾讯混元 / HuggingFace Model Adapter、Failover 与 Token Tailoring，模型凭据统一经 `SecretResolver`；Runner 原生 Knowledge、按租户路由的 Memory、Session Summary → Context Compaction → Token Tailoring、Artifact Service 与 ExecutionTrace；Session 支持 PostgreSQL/Redis/InMemory，并具备带 execution lease 的在线迁移；Memory 支持 PostgreSQL/InMemory/Mem0 框架契约；Knowledge 导入/检索已收敛到框架 Source/Reader/Docling/Chunking/Embedder/VectorStore/Reranker 单链，VectorStore 支持 pgvector/Qdrant，并可从持久 canonical snapshot 重建后切换；工具层使用单一 ToolSurface，并由持久 Tool execution ledger 防止远端副作用在崩溃重试时重复执行；独立 Platform User + WeCom Login Identity + Tenant Member；binding 级凭据与消息/Trace 隔离；Gateway/Worker 角色拆分；Kafka/DLQ/claim/Session fencing；统一 Outbox 分片与跨 Gateway pacing；Web/IM 临时输入附件 → Artifact → framework ContentPart；预算预留/known/unknown 结算、治理、审计、会话隔离/归档；单一当前 Schema baseline；Dockerfile 与 Kubernetes 部署清单。
- 已具备：HTTP、Store、Sender、Runtime 与 Tool 经 OTel 建立 Span，并以 W3C `traceparent` 跨 HTTP→Kafka→Worker 传播；tRPC-Agent-Go 原生 GenAI Tracer/Meter 已接入同一 Provider，TTFT、TPOT、输出 token 速率、token usage 与模型耗时可经 OTLP 或 Prometheus 导出；Langfuse 直接使用框架 exporter 并挂到同一 SDK TraceProvider。是否已成功连到某个生产 Collector/Langfuse 实例属于部署环境证据，不以代码配置存在替代真实连通性证明。
- 已具备：日志脱敏覆盖 JSON 字符串字段、assignment 形式与带 userinfo 的通用 URI；模型账本区分普通/缓存 Prompt token 并按平台 Catalog 独立费率计费；框架 Session/Memory/Knowledge PostgreSQL 操作进入平台 StoreObserver；企微、飞书、Telegram 原生请求标识可沿 Kafka 进入执行 Span，且不会进入 metric label。
- 部署环境验收边界：公网 Embedding/Reranker Provider、企微/飞书真实凭据、生产 KMS/Vault、容量压测与故障注入必须在持有对应凭据和资源的目标环境执行。Knowledge 的 PostgreSQL 验收覆盖 pgvector 写入/向量查询、ready 前不可见、跨租户隔离和 lease 过期 Worker 写拒绝；外部服务连通性以目标环境和对应 Actions 运行记录为最终交付证据。

因此，只有表中已有自动化证据的项目可以在本阶段标记为通过。

## 七、前端 UI 设计与实现验证（2026-09-09）

Console 采用统一的冷灰白 / Apple Blue 视觉体系，Radix 负责 Dialog、Tabs 和 Switch 语义，Lucide 提供图标；业务页面不再重复显示页内主标题。顶部租户选择器保持单行且使用原生 select 外观，主要选择器限制宽度；聊天 Composer 收为单一输入表面并校验输入框与发送按钮垂直对齐。

机器人页面保留搜索、筛选、启停、对话、版本与编辑能力，但创建时不再要求设置运行状态；用户只看到“机器人标识”“文件存储”“外部渠道”等产品语义。当前日期由框架注入，精确时刻使用框架 `environment_context_current_time`；运行产出后端显示为“数据库 / 对象存储”。外部渠道改为按需展开，不再为纯网页对话展示空绑定说明。列表操作列只保留对话与启停，版本和编辑位于展开详情，避免右侧预留大块空白。

执行记录与对话共用同一份运行文档。对话回复下方用产品语言展示“这次回复已完成 / 处理中”、步骤名称和耗时，不出现 `Agent Trace`、node id、claim 或 Outbox。执行页在同一文档上展示状态、发送、框架执行总览和真实工具调用时间线；普通 LLMAgent 只有一个聚合 Agent step 时不把它冒充成细粒度过程，实际工具调用从副作用账本按 request_id 读取名称、状态与起止时间。投递仍作为独立事实。Console 不再提供独立“我的会话”或“消息身份”功能；Web 对话仍使用底层 Session 恢复自己的历史。平台账号只由本地账号、企业微信、飞书或 OIDC 登录身份建立，账号页仅允许编辑平台昵称并查看登录身份，机器人与平台用户之间不存在 `/link` 关联。公开登录页只呈现已启用的正式登录方式，Mock 作为弱化的开发入口；未配置 Provider、所需凭据、统一回调地址、配置步骤和真实 OAuth 验证状态集中在系统管理员“登录设置”。外部 IM 的 Channel/Binding 只负责消息路由。模型状态改为无底色的“已配置 / 缺少凭据”事实；系统页使用“基础服务”，状态置于表格最右侧并取消普通状态胶囊，同时删除重复的 Provider 事实面板。

网页对话恢复的后端证据：`go test ./trpcservice/web -run 'TestProjectChatMessages|TestMatchListedSession|TestConsoleSessionMessages'`。前端证据见下方命令。

2026-09-11 前端执行 `npm --prefix webui run test:coverage`：61 tests 全部通过；Vitest 报告纯业务/状态模块 statements 82.08%、functions 86.44%、lines 82.08%、branches 68.83%。React 页面和跨页交互由下方 7 场景 Playwright 脚本验证。覆盖率在本项目中作为测试盲区分析和趋势证据，不作为提交失败阈值。

2026-09-11 Go 覆盖率报告：`scripts/check-go-coverage.sh` 实测全仓 statement coverage 为 51.70%；`cmd/trpc-service` 23.98%、`agent` 72.38%、`assembly` 66.48%、`identity` 50.14%、`messaging` 66.62%、`storage` 40.80%、`tenant` 36.00%、`web` 59.75%。同期 `go vet ./...`、`go test -count=1 ./...`、`go test -race -count=1 ./...` 均通过。覆盖率报告用于定位测试投入方向，不作为提交验收的数值门槛。

浏览器验证方法：

```bash
# 单一入口会依次执行 Mock、企微、过期登录和四个控制台 UI 场景；
# 应用使用 18080，Vite preview 使用 5178，依赖使用独立 Compose 测试端口。
scripts/infrastructure-test.sh up
tests/browser/run.sh
scripts/infrastructure-test.sh down
```

浏览器脚本支持通过 `CHROMIUM_PATH` 指定已有 Chromium。验收记录中，`e2e-ui-destinations` 使用生产构建的 `vite preview` 执行并通过，browser error 为 0；`e2e-chat-state` 同步通过。验收覆盖：租户控件单行 / 原生选择器几何、Composer 输入与发送按钮中心线、机器人操作列宽度、创建 Dialog 焦点与 Escape、聊天流式重连与多本地会话、回复下方用户可见运行过程、执行详情去实现术语并展示模型/工具步骤、系统状态右对齐、模型状态文案，统一会话页三栏工作台、渠道/私聊群聊筛选和官方品牌图加载，以及全部当前一级目的地在 1600×1000、390×844 和约 900px 下的结构与整体横向溢出检查。多页面截图输出到 `tests/browser/artifacts/`。


浏览器检查使用隔离 fixture 验证页面状态与交互；真实 OAuth、Kafka/SSE、IM 和基础设施链路分别使用对应的集成测试与部署验收脚本，证据位置以本文件各节为准。
