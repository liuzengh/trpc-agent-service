# 多节点消息运行时：数据同步与幂等

## 组件边界

`gateway/openclaw` 只负责认证、协议解析和服务端路由；`idempotency`
负责持久化 Inbox claim；`InboxPoller` 在所有节点竞争恢复到期任务；生产调度使用 Redis
Streams consumer group，`dispatcher` 只保留给离线开发和单进程测试；
`sessioncoord` 负责跨节点 lease、fencing 和写入顺序；`worker` 才能取得固定版本的
Runtime Bundle 并调用 tRPC-Agent-Go `Runner`。因此 Gateway 和 Worker 都可以水平扩展，
不依赖负载均衡器 sticky session。

```text
OpenClaw/IM callback
  -> credential + binding -> tenant/app/config_version
  -> canonical user/session
  -> PostgreSQL Inbox unique claim + inbox_seq
  -> fast HTTP 202
  -> Redis Streams consumer group（即时竞争调度）
  -> Inbox poller reclaims retry/expired claims with SKIP LOCKED
  -> Redis lease (INCR fencing token)
  -> Runtime Manager -> LLM/Chain/Parallel/Cycle/Graph -> Tool -> Runner events
  -> Inbox runner_committed（保存最终回复）
  -> fenced event/state
  -> fenced Summary/Memory projection -> Inbox derived_committed
  -> fenced Outbox -> Inbox outbox_committed
  -> tenant audit policy
  -> Inbox completed
```

进程内 `MemoryStore`、`Coordinator` 和 `MemoryWriteStore` 只是确定性测试替身，不是部署选项。
生产部署必须使用 `SQLStore` 的 PostgreSQL claim、`RedisCoordinator`，以及
`SQLWriteStore`（或等价的 `WriteStore + FenceValidator` 强一致后端）。平台 Event/state、
Summary/Memory 投影和 Outbox 始终在当前 fence 下写 PostgreSQL。Runner 自身的对话 Session
可按租户选择 PostgreSQL 或 Redis；`FencedSessionService` 先对 `session_heads` 执行
`SELECT ... FOR UPDATE`，并在持有该行锁期间调用具体 Adapter。接管方的 `AdvanceFence`
更新同一行，只能发生在旧 mutation 之前或之后，不能在 mutation 中间完成。两次写不共享
数据库连接或事务，但共享 session head 行锁这一所有权屏障。Redis Session 使用同步写和 tenant/App 物理前缀，
不把一次无锁远端预检查当作事务隔离。

## 顺序和故障语义

1. Inbox 唯一键是 `(tenant_id, binding_id, external_message_id)`。首次 claim 在
   PostgreSQL serializable 事务和 session advisory lock 下分配 `inbox_seq`。
2. 不同 session 可并行；同 session 的 `CommitTurn` 只接受
   `inbox_seq = last_event_seq + 1`。不同节点乱序竞争时，后序消息得到
   `ErrOutOfOrder` 并进入 Inbox retry，而不是越过前序消息。
3. Redis 使用独立持久化计数器 `INCR` 生成 fencing token。lease 续期和释放都比较
   `owner|token`；旧 Worker 即使在 GC pause 或网络恢复后继续执行，也不能写 event、
   state、summary、memory 或 outbox。
4. Worker 的 durable 阶段固定为 Runner → `runner_committed` → event/state →
   summary/memory → `derived_committed` → outbox → `outbox_committed` → audit →
   Inbox completed。阶段和最终回复保存在 `inbox_messages`；新 owner 优先恢复该回复，若阶段
   落库失败但平台 turn 已提交，还可按 Inbox ID 从 `message_events` 恢复。Outbox 用
   `(tenant_id, dedupe_key)` 幂等，Memory 用 source event 幂等，Summary 用 version/cutoff
   CAS，审计用稳定 audit ID 幂等。任何提交阶段失败都会保留可重试 Inbox；已落库步骤重复
   执行不会产生第二条平台 event、Memory、审计或 IM Outbox。
5. Runtime 请求固定携带 ingress 时的 `config_version`。旧版本 Bundle 在已有请求释放
   lease 前不会关闭；新请求只进入新版本。固定版本已经被清除时请求失败并重试/进入
   DLQ，禁止悄悄使用当前版本。
6. 每个生产节点运行一个带唯一 `TRPC_AGENT_NODE_ID` 的 Inbox poller。poller 使用
   `FOR UPDATE SKIP LOCKED` 批量取得到期 retry 或 lease 已过期的 processing 消息，
   按 `inbox_seq` 阻止同 session 后序消息越过前序消息；超过最大尝试次数后进入 DLQ。
   Processor 在调用模型或工具前再次续租并校验 claim token，因此已被其他节点接管的
   本地排队副本会直接退出，不会产生重复模型费用或工具副作用。
7. Gateway 首次 claim 与 InboxPoller 恢复出的请求都进入同一个 Redis Streams consumer
   group。Streams 提供低延迟单消费者投递；PostgreSQL claim token、lease 和 InboxPoller
   提供可靠性。节点在处理途中退出时，即使 Stream entry 保持 pending，Inbox lease 到期后
   也会生成新 claim 并重新投递，旧 claim 恢复后无法续租或写入。
8. `worker_nodes` 记录节点心跳和 drain 状态；活跃节点 ID 冲突会 fail fast。节点失联判定
   不直接授权写入，实际接管仍必须同时取得新 Inbox claim、Redis session lease 和更大的
   fencing token。
9. Delivery Worker 只 claim 本节点注册了 Sender 的 tenant binding。Outbox 使用
   `pending/claimed/sending/sent/retry/dlq/uncertain` 状态机和独立 claim token；
   `claimed` 超时可安全恢复，`sending` 超时转为 `uncertain`，避免企业微信 API 已成功
   但本地尚未落库时产生重复回复。发送前和每个文本分片前都经过 Redis 共享限流。
10. PostgreSQL Memory 写入在事务提交后对所有节点可见；当前请求以内存中的 Runner event
   继续执行，不依赖异步投影做 read-your-writes。外部 Memory 服务或向量索引允许最终一致，
   派生任务记录 source event 和 checkpoint；新节点只有在索引水位达到所需 event 后才把该
   版本视为已同步，超时则降级为读取 PostgreSQL 事实或暂不召回。
11. Gateway 在 Inbox claim 前通过 Redis 对 `(tenant_id, binding_id)` 做共享固定窗口限流；
    默认 100 req/s，超限返回 429，Redis 不可用时 fail-closed。它保护回调入口和数据库，
    Worker 的租户并发配额则独立限制昂贵 Runner 执行，二者不能互相替代。

## Exactly-once 边界

平台对 Inbox、平台 turn、Summary/Memory、Outbox 和 Audit 使用稳定业务键，目标是“至少一次调度、
幂等提交、至多一个待投递回复”。`runner_committed` 之后的崩溃可以直接恢复最终回复，不重跑模型
或工具。但无法把任意外部模型/Tool 调用与 PostgreSQL Inbox 放进同一事务：若调用已经产生结果或
副作用，进程却在保存 `runner_committed` 前退出，重试仍可能再次调用；每次 MCP/HTTPS Tool 调用
同时写入 `tool_executions` 账本，未知结果进入 `outcome_unknown`，由运维确认后再决定补偿或重试。

tRPC Event 的 `RequestID` 已固定为 Inbox ID，但一轮内用户、工具和模型事件合法共享同一个
`RequestID`，而重跑生成新的 Invocation/Event ID，因此不能按 RequestID 全量过滤而不破坏事件
历史。生产发布门禁要求 MCP server 显式 `idempotent: true`；这代表服务端承诺同一稳定业务请求
可安全重试。平台 HTTPS 业务工具传递稳定 `X-Idempotency-Key`。不具备幂等能力的副作用工具不得
作为自动重试 MCP 发布，应改造成支持幂等键的业务适配器，或进入人工补偿流程。

## OpenClaw 兼容 HTTP

这里实现的是 OpenClaw 文本协议的 durable callback profile，不是上游 `gwclient`
的逐字节替代：消息持久化后返回 `202 Accepted`，而不是等待完整回复后返回 `200`。
多模态 DTO 目前也是 text-only 子集。

端点：

- `POST /v1/gateway/messages`：持久化成功即返回 `202`，不等待模型和工具。
- `POST /v1/gateway/messages:stream`：Runner 产生事件时立即输出 `run.started`、
  `run.progress`、`message.delta`、`message.completed`、`run.completed` 或终态事件。
- `GET /healthz`、`GET /v1/gateway/status`。
- `POST /v1/gateway/cancel`：先把取消意图写入 PostgreSQL，再通过 Redis command bus
  广播到所有 Worker；持有请求的节点立即取消 Runner 并以 claim CAS 将 Inbox 标记为
  `canceled`。Pub/Sub 丢失时 Worker 的续租循环仍会发现持久化意图；未知或已结束请求返回 `404`。

本地 `Hub`、`Registry`、`MemoryBudget` 和 `MemoryApprovals` 仅供单进程开发。生产组合使用
`RedisEventBus`、PostgreSQL `run_statuses`、`policy_budget_*` 和 `tool_approvals`，因此状态查询、
SSE、取消和人工审批都不依赖请求落到原 Gateway/Worker 节点。

进程内 Hub 对 delta 使用有界、非阻塞缓冲；慢客户端可能丢失中间 delta，但终态
`message.completed`/`run.completed` 会优先投递。需要完整回放时，应从共享 event log
按 request ID 重放，而不是把 Pub/Sub 当作事实存储。

客户端通过 `Authorization: Bearer ...` 和 `X-Channel-Binding` 认证。token 只参与
常量时间比较，不写入消息、日志或 trace。服务端根据凭证解析 tenant/app；外部
`from` 映射为 canonical user，单聊、群聊、thread 分别使用 `DirectSessionID`、
`GroupSessionID`、`ThreadSessionID`。客户端提交 `session_id` 会被拒绝，避免跨租户
或跨会话注入。`X-Trace-ID` 从 callback 一直写入 Runner 请求、event 和 Outbox；
若缺失则由 Gateway 生成。

## 当前协议范围

DTO 保留 `content_parts`、图片/文件 URL、model 和 extensions，当前 HTTP 可执行输入仅接受
文本。Channel Adapter 先完成各平台的验签和身份解析，再转换成相同的 `InboundMessage`；
企业微信与飞书 Adapter 均已实现，两个通道共用同一条 Outbox 投递链路。出站层从 Outbox 做平台长度切片、
限流、失败退避和 DLQ。HTTP 层不会把图片/文件 URL 直接交给模型或工具，以免形成 SSRF 通道。
