# 架构设计

## 1. 设计目标和范围

`trpc-agent-service` 把一次 Agent 请求拆成“可信接入、持久 Admission、异步执行、持久事件、异步回复”五个边界，使 Gateway 可以接收流量、Worker 可以跨节点消费，而业务状态留在共享后端。当前运行组合为：OpenAI-compatible 模型；Session 的 PostgreSQL、Redis 和 InMemory 路由；TencentDB Memory；Qdrant Knowledge；COS Artifact；WeCom 与 Feishu Channel。配置类型通过 `BackendKind` 表达这些领域的选择。

本文回答整体设计为什么这样工作；实体字段见[数据模型](data-model.md)，并发和恢复语义见[数据同步和幂等](data-sync-idempotency.md)，后端取舍见[后端适配](backend-adaptation.md)，落地参数见[部署](deployment.md)和[容量](capacity.md)。

设计优先级是：租户/应用/Session 隔离，权威状态可恢复，单 Session 有序，重复投递可安全处理，租约丢失时旧 Worker 不能继续写入，以及 secret/payload 不进入日志和 trace。系统不声称 exactly-once；对无法确认外部副作用的结果，明确落为 `UNCERTAIN`。

## 2. 多租户模型与隔离

顶层作用域是 `(tenant_id, app_id)`。`Tenant` 持有租户状态和总审计策略；`AgentApp` 持有 stable `ActiveConfigVersion`、可选 canary 版本/比例/状态；`AppConfig` 是不可变版本，包含模型、工具策略、IM 访问策略、预算、Backend 引用、审计策略、secret 引用、channel binding 和知识库 ID。

入口不接受客户端声明的 tenant/app。HTTP 入口用唯一 `Authorization: Bearer` API key 查 `api_credential`，只持久化 SHA-256 digest，由凭据记录反查 tenant/app，并生成服务 principal。Channel 入口先从当前 binding 快照得到可信 `(tenant_id, app_id, binding_id, binding_revision)`，再由 PostgreSQL 事务校验 binding、身份映射和 access policy。客户端不能通过 body 或 `X-User-ID`、`X-Session-Principal-ID` 伪造主体；OpenAI 入口只允许客户端选择该服务主体下的 `session_id`。

隔离由多个边界共同完成，而不是只加一列：

- 所有平台 SQL 查询按 tenant/app 作用域，平台 key 通过 `tenant.Scope.Key` 编码作用域；Session 的 framework `AppName` 也由该 scope 派生。
- 工具可见性和可执行性都由不可变 `ToolPolicy` 决定，Worker 在真正的工具权限边界再次校验；未知工具 fail closed。当前部署的 `ToolCatalog` 只有 `todo_write`。
- 外部 IM user/chat/thread 只以 binding-scoped HMAC digest 查询，provider target 以 tenant/app/binding/entity 的 AES-256-GCM AAD 加密保存；原始 target 只在内存和一次 Provider 调用中出现。
- API key、模型 key、数据库 DSN、IM secret 均通过 scoped `SecretProvider` 解析。平台数据库保存引用、digest 或加密 envelope，不保存原始凭据。日志、audit 和 trace 不包含 prompt、tool arguments、provider body；错误和 audit metadata 还会做 PII/credential redaction。
- 审计查询由 Admin principal 的 role 和 tenant allowlist 决定；Operator/Auditor 不能靠请求参数扩大作用域。Control Plane 与 data-plane bearer token 分离。

真实 IM 还需要两项独立的平台内部 key：`im-provider-target-key@v1` 用 AES-256-GCM 保护持久化 Provider target，`im-external-id-hmac-key@v1` 对外部 user/chat/thread ID 做稳定 HMAC。两者按 `(tenant_id, app_id)` 通过 scoped `SecretProvider` 注入；每个值都是 32 字节随机数据的无 padding Base64URL 编码，并且必须跨节点、重启和滚动发布保持稳定。它们不是 WeCom/Feishu 的 Bot/App Secret。

这套机制定义了代码级租户隔离契约；真实生产集群、SecretProvider 和跨组织权限审计属于部署环境边界，不由仓库内测试单独证明。

## 3. Gateway / Worker 节点化架构

一个 `trpc-service` 进程可运行 `gateway`、`channel`、`worker` 或 `all` 角色。推荐将三类运行职责分开：

- Gateway 暴露 `/v1/chat/completions`、`/admin/v1/*`，执行身份解析、配置 pin、附件预处理、原子 Admission，并运行 dispatch Relay。它不运行真正的 Agent Runner，也不启动任何 IM 长连接，因此可以多副本和 HPA 扩展。
- Channel Adapter 运行在独立的 `channel` 角色进程中。当前是 WeCom Bot WebSocket 和 Feishu/Lark WebSocket，每个 active binding 在这个单 owner 进程内拥有连接句柄、重连和 connection status。Adapter 在本进程构造同一个 `gateway.Gateway` Admission 组件，把 provider event 规范化后提交到同一 PostgreSQL Admission/outbox 链路；不会绕过队列直接调用 Runner，也不发现具体 Worker。当前没有 binding lease、leader election 或 channel sharding，所以该角色在 Compose/Kubernetes 中固定单副本。
- Worker 消费 Redis Stream，向 PostgreSQL claim execution lease，并用 Redis Session Lease/Session Lock 串行化同一 partition。随后按 execution 的精确 ConfigVersion 构造 tRPC-Agent-Go `LLMAgent`/`Runner`，执行模型、工具、Session、Memory、Knowledge 和 Artifact；它还负责迁移 worker、artifact cleanup 和 heartbeat。Worker 本身没有需要复制的业务状态，可独立水平扩展。Reply Sender 与 WeCom/Feishu outbound resolver 只在单 owner 的 Channel 角色中运行，避免 Worker HPA 为同一 binding 建立重复长连接。
- PostgreSQL 是控制面、execution lease/fence、event、outbox、approval、migration、audit 和 channel metadata 的权威库；Redis 是 dispatch transport、consumer pending、Session Lease/Session Lock、Channel 角色使用的分布式 Reply Rate Limiter，以及可选的 Session 数据后端。Qdrant、COS、TencentDB 是各自领域的外部数据面，不能替代平台 SQL catalog 和执行状态。

HTTP Gateway 和 Worker 都不依赖本地业务 Session 才能正确运行，所以可以横向扩展。真正的分发不是“把请求粘到某台机器”，而是 PostgreSQL 的 durable execution + Redis Stream Consumer Group；Redis message 被重复投递时，数据库 claim 和 run token 决定谁有权转移状态。Worker 本身没有需要复制的业务状态。

HTTP Admission 是无状态路径；Channel Adapter 进程按 active binding 建立并维护 IM 长连接，消息进入 `channel_inbox` 后由数据库幂等约束吸收重复投递。Gateway 副本数不会再改变 IM connection ownership；Channel Deployment 的单副本和 `Recreate` rollout 是当前明确的 ownership 约束。`all` 仅用于本地或单进程部署，不能作为多副本 Channel owner。

## 4. 消息路由与 Sticky Session

HTTP 请求的 session 由认证 service principal + `X-Session-ID` 组成。Channel direct 消息由稳定映射的 user ID 作为 principal，group/topic 消息由 binding-scoped conversation 生成稳定 `conversation_id`/`session_principal_id`；两者再通过 `conversation_session` 读取当前 active Session ID，首次使用默认为 `default`。跨 tenant、app 或 binding 的同名外部 ID 不会复用内部主体。

WeCom/Feishu 中内容经 trim 后精确等于 `/new` 的文本消息是平台命令，不进入 Gateway execution admission，也不交给模型。PostgreSQL 命令事务在 `(tenant_id, app_id, binding_id, session_principal_id)` 作用域内锁定或创建 `conversation_session` 指针，生成新的 Session ID，写入幂等 `channel_inbox`，并以 `source_kind=channel_command` 写成功 Reply Outbox；切换和回复要么共同提交，要么共同回滚。旧 Session 内容不删除，后续普通消息只使用新指针。

Gateway 在一个 PostgreSQL 事务中锁定必要的 app/binding 行，校验 active/canary config 和迁移 gate，给 Session lane 分配 `turn_seq`，插入 execution、入站 artifact 关联和 dispatch outbox。`session_lane` 的 unique `(tenant, app, session_principal, session_id, turn_seq)` 是顺序的持久证据。Worker 使用 `Scope.Key("session", principal, session)` 取得 Redis Session Lease 并持有 Session Lock；同一 partition 在任意节点同一时刻只有一个 Runner 能执行。

因此当前选择“不依赖网络层 sticky session”。sticky 会把路由正确性绑定到负载均衡器和连接生命周期，而当前实现已经把顺序、状态和 ownership 放到共享后端。Sticky 仍可能作为性能优化，但不是正确性条件；任何节点都可以接收 Admission，任何 Worker 都可以接管过期 delivery/lease。

## 5. ConfigVersion 与配置发布

配置采用发布后不可变、激活指针可变的模型。Admin 先写 `PUBLISHED` 的 `app_config_version`，再通过 activate/rollback 改 `agent_app.active_config_version`。配置表有数据库不可变触发器；执行记录永久保存它实际 Admission 时的 `config_version`。因此激活只影响新 Admission，已经入队或运行的 execution 不会中途切换模型、工具或数据后端。

Canary 选择一个已发布版本和 1–100 的比例，按稳定 tenant/app/session principal/session 维度计算 deterministic bucket。`/new` 在切换前使用当前 active Session ID 做相同选择，并用选中版本的 IM access policy 判权，避免普通消息进入 canary 而命令仍按 stable 判权。pause/disable 使新请求回到 stable；rollback 移除 canary；promote 把 canary 变成 stable。应用层 canary 与 K8s rolling update 是两个层次：前者控制租户请求使用的 immutable config，后者替换进程镜像。若只改变 Session/Knowledge Backend，不能直接激活；必须先完成支持范围内的数据迁移，成功事务同时切换 active config。失败保持 source active。

## 6. Session、Memory、Knowledge、Artifact

Session 由 tRPC-Agent-Go framework service 负责保存事件、state、tracks 和 summary。平台没有另造一张“session 内容表”；`session_lane` 只保存排序和 admission 元数据。这样可以复用 framework 的 Redis/PostgreSQL Session schema，同时由平台在 runner 外侧补租户作用域、迁移和 lease。

Memory 当前以 TencentDB Agent Memory ingestor 接入。私聊使用 tenant/app/user/session 派生 key，服务跨 Worker 共享；群聊的 framework transcript 可能包含多个 sender，当前实现选择不把整段共享 transcript 归因给当前用户，因而跳过该 Memory ingestor。这是防止跨成员记忆泄露的边界，不是群聊 Memory 已完成的通用归因方案。

Knowledge 使用 Qdrant 做向量检索，但 SQL 中的 knowledge base/document/chunk catalog 才是租户和 config 的授权来源；向量 payload 不能单独授予访问权。Artifact 使用 COS 保存对象、PostgreSQL `artifact`/`inbound_artifact` 保存元数据和生命周期；framework externalization 让大对象不进入 Session event。入站媒体先以 scoped object key 写 COS，再在 Admission 事务里关联；失败会补偿未共享的 staging 对象。后台 cleanup 以数据库 candidate、lease、attempt、completed 字段驱动，删除失败可重试。

## 7. 数据一致性与迁移

Admission 的线性化点是 PostgreSQL 事务提交：幂等记录、Session turn、execution、artifact attach 和 dispatch outbox 要么共同提交，要么不对外可见。Relay 之后才发布 Redis Stream，Worker 只有在 durable execution transition 完成后 ACK。详见[数据同步和幂等](data-sync-idempotency.md)。

当前数据迁移包含两条已实现路径：Session Redis → PostgreSQL、Knowledge Qdrant → Qdrant。迁移记录由 SQL 保存，生命周期为 `PENDING → DRAINING → COPYING → VERIFYING → SUCCEEDED/FAILED`，带 owner/run token/lease/checkpoint。DRAINING 通过 admission gate 阻止新 execution，等待 source config 的 `PENDING/RUNNING/WAITING_APPROVAL` 清空；copy/verify 可按 session 或 chunk 恢复；只有 verify 成功才在同一事务激活 target config。迁移采用单一 active authority，切换发生在验证成功后的事务中。

## 8. IM 接入

当前代码装配的 Channel 是两类官方长连接：WeCom AI Bot WebSocket 使用 Bot ID（`external_account`）和 Bot Secret；Feishu 使用 Lark WebSocket SDK、App ID 和 App Secret。连接建立前，Adapter 通过 scoped `SecretProvider` 解析 binding secret，并校验当前 binding 的 account/status/revision；SDK/WebSocket protocol 完成 provider 认证。当前模式不提供 HTTP webhook URL，因此 README 中的 webhook URL 与 HTTP callback signature verification 对这两条接入路径均为 `NOT_APPLICABLE`，不能写成“Webhook 验签已实现”。

Adapter 规范化 text/image/file/mixed/card/event/unsupported，映射 direct/group/topic，建立 `channel_identity`/`channel_conversation`，然后携带 provider-neutral `ChannelInput` 进入 Gateway Admission。binding 的 tenant/app scope 和 revision 在连接前、入站消息和 Admission 事务中重复校验；external message ID 做去重；external user/chat/thread 通过 binding-scoped HMAC 映射到内部 identity/conversation，direct 使用 user principal，group/topic 使用 conversation principal，从而隔离 tenant/app/binding 及不同会话。

外部 message ID 是 binding-scoped channel idempotency key；同一 ID 同一 payload 重放已有结果，不同 payload 冲突。媒体下载经过 HTTPS/public-IP/大小校验并写 COS，绑定在 pinned ConfigVersion 下。下载、解密、COS 上传、配置 pin 或 Admission 失败时，Gateway/Adapter 以同一 Inbox 幂等键写 `REJECTED` 和 `source_kind=channel_failure` 的失败 Reply Outbox；若 Admission 实际已提交，则已提交结果优先，不追加错误回复。`/new` 的映射、判权、切换、Outbox 或提交失败也沿该失败记录边界返回用户可见回复。

Agent durable event 在 Gateway 的 `QueuedRunner` 中投影为 OpenAI-compatible stream/non-stream；IM reply 则由 Channel 角色中的 `reply_outbox` 和 Reply Sender 异步发送，持久化 text、stream、card 三类回复，并按 sequence 和上一次 Provider message ID 恢复流式更新。只有 `IsTerminalError()` 事件投影“执行失败”；携带 `Error` 的非终态事件不会提前失败或造成后续成功时重复回复。WeCom 使用 stream update/template card 协议，Feishu 使用同一消息更新和原生 interactive card；`TRPC_AGENT_SERVICE_IM_REPLY_MODE` 选择 `text`（默认）、`stream` 或 `card`。Feishu 支持 recall inbox；WeCom/Feishu 都有 provider-specific 长度、重连、限流和错误分类。

当前通道组合选择 WeCom Bot WebSocket 与 Feishu/Lark WebSocket。绑定、Secret、协议校验、消息去重、身份映射、媒体 staging 和异步文本回复均沿这两条官方长连接路径实现；`public_route_id` 仅作为历史绑定字段，不参与当前通道入口。

## 9. Tool、Approval 和 Governance

平台把 tRPC-Agent-Go 的 tool declaration/execution/callback 作为运行时能力，把 tenant `ToolPolicy`、secret scope、execution fence、审批和审计作为平台责任。Worker 在模型提出工具调用时检查 execution lease、工具是否 executable；review-required 工具建立唯一 pending `tool_approval`，execution 进入 `WAITING_APPROVAL`，批准后继续原 execution，拒绝/过期不产生成功工具完成。

预算 callback 当前只执行 tenant-app scoped 的 per-execution token limit，缺少可计量 usage 时 fail closed；可选的 operator model pricing 只用于该 execution 的成本估算。当前没有 daily tenant budget、monthly tenant budget 或 aggregate cost quota/账单系统，不能把这条能力描述成完整租户计费预算。当前治理链路由 Callbacks、ToolPolicy、Budget、Approval、Secret scope 和 audit 组成；Plugin、Guardrail、MCP、Skill 在平台设计中对应 Runtime 扩展边界，实际运行以当前 ConfigVersion 固化的治理策略为准。

## 10. Telemetry、Audit 和 Secret

入口抽取 W3C `traceparent`/`tracestate`，缺省 trace identity 与 request context 关联；dispatch payload 保存 trace parent/state，Worker 和 Channel 角色中的 Reply Sender 继续建立 span。span 覆盖 channel event、gateway admit、worker execute、runner/model/tool、Session、Memory、Knowledge、reply send；raw LLM request/response、消息正文、工具参数不进入 payload tracing。Telemetry 初始化或 exporter 故障不阻断核心路径，退化为 noop；这保证可用性，但意味着 trace 完整性是可选依赖。

Audit 是 metadata-only 的 PostgreSQL sink，包含 README 要求的 tenant/channel/user/session/agent/tool/decision/latency/error/cost/trace/request/config 字段，并支持控制面 actor、scope 和 query digest。记录是 best-effort，持久化失败由 metric/operation signal 暴露，不把敏感 payload 作为补偿写入。SecretProvider 的真实值只在需要的 resolver/provider 调用时出现；外部 ID 用 HMAC digest 做查找，回复 target 用 AEAD envelope 保护。

## 11. 灰度与回滚

配置回滚是选择旧 immutable version，不是就地修改当前 JSON；旧 execution 仍按原 version 完成。对仅改变模型参数/工具策略且无需搬数据的版本，可发布后 canary，再 pause/rollback/promote。对后端改变，先创建并 begin 数据迁移；迁移 gate 阻挡新 Admission，成功后才切 active。这样把“代码镜像回滚”和“数据 authority 回滚”分开，避免指针回滚到已经没有数据的 Backend。

## 12. 故障恢复

Worker crash 后，Redis pending delivery 可被 XAUTOCLAIM；PostgreSQL execution lease 过期后可被另一 Worker claim。run token 和 execution fence 防止旧 Worker 在 lease 丢失后写终态、事件、Session 或工具权限。Redis session lease 丢失会取消 context；若 Runner 尚未启动可重试，已启动则按可能副作用处理为 uncertain。模型超时和明确的基础设施失败是 bounded retry；Side-effect unknown 不自动重试。若 COS、模型、Session 或 Knowledge 等依赖导致 Runner 在 `RunnerStarted` 前构建失败，非最终 attempt 保持可重试；最终 attempt（或永久错误）先持久化 terminal error event 和 `FAILED`，再结束消费，使 IM Reply Outbox 能产生一次失败回复。

Reply Sender 独立于 Runner，且归 Channel 角色所有：reply outbox 可以在 Agent 已成功后继续投递。已知 retryable Provider error 按 bounded delay 重试；transport/取消/receipt 缺失/lease 丢失会将 reply 标为 `UNCERTAIN`，避免不知道 Provider 是否已经发送时重复发消息。IM recall 通过 inbox 去重并取消尚未完成的 execution。

Go 生命周期也属于正确性：shutdown 先 readiness false，再停止 claim；in-flight consumer 在 grace deadline 内排空；Worker 在释放 Session lease 前持续排空 Runner event channel；模型、Runner、reply、migration、heartbeat 和 adapter goroutine 都等待或受 context/timeout 约束。若 grace deadline 内无法停止，进程将错误暴露，而不是伪装成功。

## 13. 部署和容量设计

最小本地拓扑是 PostgreSQL、Redis、Qdrant、一个 Gateway、一个单 owner Channel Adapter 和两个 Worker；Compose 另提供 COS/Memory endpoint 配置和 OTel/Jaeger/Prometheus/Grafana。推荐 K8s 使用独立 Gateway/Channel/Worker Deployment：Gateway 有 ClusterIP Service、HPA、PDB，可多副本；Channel 只有单副本 `Recreate` Deployment，不挂 HPA，避免 rollout 重叠连接；Worker 有独立 Service、HPA、PDB，可独立扩缩。外部 PostgreSQL/Redis/Qdrant/COS/TencentDB 由运维提供。当前 Admin UI 随 Compose 的 Nginx 容器部署；Kubernetes 发布入口是 Kustomize overlay。

并发上限的第一近似是 `Worker 副本数 × TRPC_AGENT_SERVICE_WORKER_CONCURRENCY`，但同一 Session 始终串行，实际吞吐还受模型延迟、PostgreSQL event/outbox 写入、Redis Stream、Qdrant/Memory/COS、每 binding reply limit 影响。`capacity-evaluate` 和 `capacity-observe.py` 能在 disposable Compose 中测请求、延迟、Redis/PostgreSQL delta、容器 CPU/内存；它们不能给真实 Provider、生产网络或生产数据库推出安全上限。容量推导和限制见[容量文档](capacity.md)。

## 14. tRPC-Agent-Go 复用能力与平台新增能力

当前实际复用：`llmagent`、`runner.Runner`、Session service、Memory ingestor、Knowledge、Artifact 和 externalization、model/openai、tool declaration/callback，以及 OpenAI server projection。平台新增：tenant/app/config/version/canary、API credential 认证、Channel binding/identity/conversation、原子 Admission、execution/turn/outbox/journal、Redis dispatch、Session Lease/Lock、Reply Rate Limiter、Worker claim/fence/retry、Approval、数据迁移、Artifact cleanup、审计/低基数 metrics、Secret scope/protection、WeCom/Feishu adapter 和 Admin API。

框架提供 Graph/Chain/Parallel/Cycle、Plugin/Guardrail、MCP/Skill、AG-UI/A2A/OpenClaw 等可复用能力；当前平台 Runtime 选用 `assistant` LLMAgent、`runner.Runner`、Session/Memory/Knowledge/Artifact、tool callback 和 OpenAI projection，租户策略由平台层补充。

## 15. 当前实现边界

当前实现组合形成一条可持久、可恢复、可验证的 OpenAI-compatible/WeCom/Feishu 主链路，并包含 Redis→PostgreSQL Session 与 Qdrant→Qdrant Knowledge 迁移。本文其余章节按这些真实组件、状态和仓库测试证据说明设计；真实第三方账号、生产基础设施和生产容量结论见[验收矩阵](acceptance.md)的证据边界。
