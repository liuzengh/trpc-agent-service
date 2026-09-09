# 多租户节点化 Agent 平台架构设计

## 1. 目标与设计边界

本项目基于 `trpc-agent-go` 建设一个多租户、可节点化部署的 Agent 平台。目标不是重新实现 Agent runtime，而是把上游已有的 Agent、Runner、Model、Tool、Plugin、Session、Memory、Knowledge、Artifact 和 Telemetry 能力组织成可管理、可隔离、可扩展的企业服务。平台新增 Tenant 身份、Agent App 注册、Deployment Version、发布路由、IM 绑定、租户级后端选择、治理策略、审计、故障恢复和运维控制。

平台的稳定外部边界是 Management Console 使用的 HTTP API、Chat SSE envelope 和 IM Channel 行为。Gateway 与 Worker 的内部协议、Go 接口以及数据库结构不作为外部兼容契约，但必须通过迁移和版本校验演进。通用运行时能力优先复用上游，租户和平台运营逻辑留在本仓库，避免形成 `trpc-agent-go` 对业务平台的反向依赖。

比赛交付提供两种运行拓扑。单节点开发使用 SQLite Control Plane 和可选择的 InMemory、SQLite 数据后端；Stage 7 Compose 使用 Nginx、两个 Gateway、独立 Worker、PostgreSQL 和 Redis。生产拓扑沿用相同组件边界，但还需把当前固定在 Gateway A 的部分治理运行态迁入共享事务存储。

完整拓扑见[系统架构图](system-architecture-diagram.md)，企业微信端到端链路见[核心时序图](core-sequence-diagram.md)，Provider 认证、账号路由、Session 规则和限制矩阵见 [IM Channel Adapter 设计](im-channel-adapter.md)。

## 2. 组件职责

**Gateway** 是统一准入与编排节点。它完成身份认证、Tenant Context 注入、角色检查、Agent App 与版本解析、治理预检、共享 Run Coordinator Claim、Session 事件持久化、Worker 调度、SSE 转换以及回复投递。Gateway 不信任客户端提交的 `tenant_id`，所有资源访问都从服务端建立的 Tenant Context 派生租户范围。

**Worker** 是尽量无状态的执行节点。Gateway 将已经解析且不可变的 Tenant、App、Deployment、Version、Policy Revision、`request_id`、`trace_id`、W3C `traceparent` 和 fencing token 写入短期 Execution Manifest，再以专用 HS256 key 签名。Worker 同时验证内部 Bearer Token 和 Manifest，随后由 AgentFactory 构造上游 Runner。Worker 不保存 Session 或 Deployment 的权威状态，因此可以独立扩缩容和重启。

**Channel Adapter** 把不同 IM 协议转换为统一的 `ChannelMessage` 和 `ChannelReply`。企业微信采用 API 模式智能机器人的 WebSocket 长连接，通过 BotID 和 Secret 完成 `aibot_subscribe`、接收 `aibot_msg_callback` 并发送 `aibot_respond_msg`；不使用 CorpID、AgentID 或传统 HTTP 回调。Telegram 使用 long polling 接收 Update，并通过 `sendMessage` 回复。两种通道都经过平台级 Bot Tenant Allowlist，把 provider account 与外部主体映射到唯一 Tenant 和 Agent App。

**Storage Router / Adapter** 按服务端 Backend Selection 为 Tenant 选择 DataStore。当前实现覆盖 InMemory、Redis、SQLite 和 PostgreSQL 的 Session/Memory 端口，以及 SQL 中的 Knowledge/Artifact 元数据。Qdrant/Milvus 向量索引与 S3 对象内容是明确设计的适配边界，不宣称已经完成生产接入。具体职责见[多后端适配方案](backend-adapters.md)。

**Plugin / Guardrail** 在真实 Tool 调用点执行治理。策略顺序为外部主体授权、租户限流与预算预留、输入 Guardrail、Tool/MCP allowlist、危险 Tool 二次确认、输出 Guardrail/脱敏和用量结算。危险 Tool 的决定与执行状态持久关联 `request_id`，治理不可用时 fail closed，避免在无法审计时产生副作用。

**Telemetry 与 Audit** 承担不同责任。Channel、Gateway、Worker、Runner、Tool 和 Storage 产生 spans 与 metrics，使用 `trace_id`/`traceparent` 串起请求；不可替代的审计事实由 Gateway 写入 Audit Store。指标按 Tenant、App、Provider 聚合请求数、失败率、限流、token、成本以及模型、Tool、存储和 IM 投递延迟。

## 3. 控制面、数据面与部署

控制面保存 Tenant、Agent App、Deployment、不可变 Deployment Version、Channel Binding、Backend Selection、Governance Policy、Tenant Audit Policy 和配置幂等状态。Audit Policy 默认保留 90 天、metadata_only、fail_closed；租户管理员只能更新本租户，平台管理员可更新已授权租户，策略校验拒绝 fail_open。管理员发布 Version 后，只通过 Deployment 指针完成激活、灰度和回滚，不修改历史版本。Version 可引用服务端 Model Provider Profile，但不能保存 API Key；模型和 IM 凭据来自环境变量或生产密钥管理器。

数据面处理 Chat 或 IM 消息。入口可把请求分配给任一 Gateway，不依赖 sticky session。共享 Run Coordinator 以 `(tenant_id, session_id, request_id)` 记录输入哈希、排队顺序、owner、取消和终态；同一 Session 允许多个 queued，但同一时刻只有一个 running owner。Coordinator 再以 `(tenant_id, session_id)` 的 fencing token 保护 Session Event、Memory 和 Artifact 写入。不同 Session 可并发，相同 Session 串行化；跨 Gateway retry/cancel 读取同一共享状态，节点放置不改变结果。

Stage 7 Compose 中 PostgreSQL 是共享控制面和 Session 数据的权威来源，Redis 用于租户可选的热 Session/Memory，Nginx 提供单一入口。`X-Gateway: a|b` 只用于验收时确定性选路，不是生产路由协议。Gateway 与 Worker 使用独立进程角色；Worker 只开放内部执行接口。Control Plane schema 由 `control-migrate` 前置迁移，Gateway 启动只检查 schema 版本，不隐式修改生产数据库。

## 4. 请求执行链路

公开请求进入 Gateway 后，身份组件先建立 Tenant Context。Gateway 校验 App、Active Deployment、Version 与 Policy，使用 `request_id` 派生幂等键写入 `message.input` 和 `run.started`，再获取或确认 Session Lease。随后它签发 Execution Manifest 并调用 Worker。缺失、过期、篡改、未知 key 或 `traceparent` 不一致的 Manifest 都由 Worker 拒绝。

Worker 根据不可变 Version 调用 AgentFactory，构造 `trpc-agent-go` Runner。模型可能直接生成回复，也可能返回 Tool call。Tool callback 在执行副作用前调用内部 Governance API；普通 Tool 在授权后继续执行，危险 Tool 则进入 `pending_confirmation`，只有 approved 状态能够原子转换为 executing。若 Worker 在 executing 后失联，平台记录 `outcome_unknown`，禁止自动重放，由操作员调查外部副作用。

Runner Event 经 Worker 内部 SSE 返回 Gateway。Gateway 将其转换为稳定的 `message.delta`、`message.completed`、`run.failed`、`run.cancelled` 和 `run.completed` 事件，并按 sequence 追加到 Session Event。成功回复发送到 IM 前，必须先发布 Artifact 元数据、写入 `latest_agent_reply` Memory 并推进连续的 Session State/Summary 投影；任一关键写入失败都不能产生虚假的成功终态或 IM 成功回复。完整字段与约束见[数据模型设计](data-model.md)，重试和迁移规则见[数据同步与幂等策略](data-sync-idempotency.md)。

## 5. 多租户隔离与一致性

Tenant 是所有业务资源的隔离根。控制面和数据面查询都使用包含 `tenant_id` 的组合键；外部 ID、Session ID、App ID 和自然键不能单独作为授权依据。对另一个租户资源的查询返回统一 not found 或空集合，不暴露资源是否存在。Provider Account 是平台级资源，但其外部主体必须通过服务端 allowlist 映射到 Tenant/App 后才能执行。

Session Event 是运行数据事实源，Session State 与 Summary 是可重建投影。物化器只允许 checkpoint 从 N 推进到 N+1，发现 sequence 空洞立即失败。相同幂等键和相同内容的重试返回已有结果；相同键携带不同内容返回冲突。Memory 与 Knowledge 先写权威记录，再异步构建向量索引，向量召回不能覆盖权威值。Artifact 采用内容和元数据分离发布，只有元数据可验证地指向已持久化内容时才报告 published。

跨节点正确性不依赖进程内缓存。共享 Control Plane 每次读取按 revision 刷新；数据库不可用时返回稳定的 `control_plane_unavailable` 或 `storage_unavailable`，不回退到旧本地快照。配置写入使用 revision 和幂等键避免不同 Gateway 相互覆盖。

## 6. 治理、观测与故障恢复

每次执行同时拥有业务 `request_id`、平台 `trace_id` 和 W3C `traceparent`。Trace 覆盖 IM callback、Gateway admission、Run Coordinator Claim/Cancel、Lease、Worker、AgentFactory、Runner、Model/Tool、Session/Memory/Knowledge 读写和 IM reply；Audit Event 记录 tenant、channel、user、session、agent、tool、decision、latency、error、cost、policy revision 与按 Tenant Audit Policy 处理的 content。metadata_only 不保存正文，redacted_summary 只保存通用凭据脱敏后的摘要。日志 Redactor 在输出前处理已配置 secret 和所有数据库 DSN，Deployment、浏览器状态、审计、错误响应和验收证据均不得保存凭据。模型回调耗时与端到端执行耗时分别统计；管理 API 用于租户内查询，带内部 Bearer 认证的 `/internal/metrics` 提供 Prometheus 文本抓取。

服务关闭采用分阶段排水。收到 SIGTERM 后先撤销 readiness 并停止 IM 拉取，再停止 HTTP admission；在独立期限内等待在途请求，超时后广播 context cancellation，最后给事件终结和资源关闭单独期限。Runner adapter 不创建无限 drain goroutine；忽略取消的模型会导致对应 Worker 退役，避免继续接收请求。

恢复策略按数据语义区分：Session 依靠事件回放和 fencing；Memory/Knowledge 的向量索引可从权威内容重建；Artifact 通过发布状态和 checksum 对账；危险 Tool 的不确定副作用不自动重试。详细生产风险和缓解措施见[生产风险清单](production-risks.md)。

## 7. 实现范围与演进边界

当前参考实现已经覆盖 SQLite/PostgreSQL Control Plane，InMemory/Redis/SQLite/PostgreSQL Session/Memory，SQL Artifact/Knowledge 元数据，InMemory/PostgreSQL Run Coordinator，OpenAI-compatible Chat Completions 与 Responses，签名远程 Worker，内部 Tool 治理，Telegram 与企业微信 Provider，SSE、审计、指标、Trace、灰度回滚和有界关闭。源码和测试入口见[GitHub 实现代码详解](implementation-details.md)。

S3 Artifact 对象内容和 Qdrant Knowledge 向量索引已有运行适配器；Milvus、增量向量 outbox、对象孤儿自动回收和 Kubernetes 清单仍是设计边界。Audit Event 可接入共享 PostgreSQL；治理确认、预算和 Trace 的现有实现仍可配置本地持久化，部署为多副本时需将这些剩余运行态统一接入共享事务存储。Run Coordinator 的 PostgreSQL 适配器、共享 Provider Route 和 Session fencing 已不依赖 sticky placement；危险 Tool owner loss 仍保持 `outcome_unknown`，禁止自动 replay。
