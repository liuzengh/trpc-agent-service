# 核心时序

主链以当前 WeCom/Feishu 官方 WebSocket/long connection 为例；HTTP OpenAI 请求在 `Gateway Admission` 处汇合。Channel Adapter 是独立的单 owner 运行角色，但收到消息后复用同一个 `gateway.Gateway` Admission、PostgreSQL outbox、Redis Stream 和 Worker 链路，不发现具体 Worker。每条消息都必须携带或生成以下上下文：

`tenant_id/app_id`：由 API credential 或 verified channel binding 得到；`request_id`：入口生成并在 execution/outbox/event 中保持；`session_id`：HTTP header 或 direct/group/topic 映射得到；`config_version`：Admission pin 后持久化；`trace_id`：从 W3C trace context 提取或由入口生成，并以 `traceparent/tracestate` 随 dispatch 传播。

当前 IM 模式不提供 HTTP webhook URL；WeCom/Feishu 认证由官方 WebSocket/SDK 使用 binding 的 external account 与 scoped secret 完成。因此 README 中 webhook URL 和 HTTP callback signature verification 对当前模式为 `NOT_APPLICABLE`，不能将 WebSocket protocol authentication 描述成 webhook 验签。

## 主时序图

```mermaid
%% source: diagrams/core-sequence.mmd
sequenceDiagram
  autonumber
  actor User as IM 用户
  participant IM as WeCom/Feishu Adapter
  participant Map as Binding + Identity Mapping
  participant GW as Gateway Admission
  participant DB as PostgreSQL Store
  participant Relay as Dispatch Relay
  participant Redis as Redis Stream
  participant C as Worker Consumer
  participant Exec as PostgreSQL Execution Lease
  participant SLease as Redis Session Lease
  participant SLock as Session Lock
  participant Run as tRPC-Agent-Go Runner
  participant Model as OpenAI-compatible Model
  participant Tool as ToolCatalog / todo_write
  participant Ses as Session Backend
  participant Mem as TencentDB Memory
  participant Kno as Qdrant Knowledge
  participant Art as COS Artifact
  participant Reply as Channel Reply Sender
  participant Rate as Reply Rate Limiter
  participant IMOut as IM Provider

  User->>IM: message(external_message_id, sender/chat/thread, content)
  IM->>Map: verify WebSocket binding account/status/revision
  Map->>Map: derive tenant/app, user/conversation, session_id
  Map->>GW: ChannelInput + binding_revision + traceparent
  Note over Map,GW: tenant_id/app_id, request_id, session_id, trace_id
  GW->>DB: BEGIN, lock binding/app, resolve and pin ConfigVersion
  DB->>DB: channel_inbox dedup + payload_hash
  DB->>DB: session_lane allocate turn_seq
  DB->>DB: insert execution + inbound artifact refs
  DB->>DB: insert dispatch_outbox
  DB-->>GW: COMMIT admitted(request_id, config_version, turn_seq)
  GW-->>IM: return admission result, no IM reply yet
  Relay->>DB: claim dispatch_outbox
  Relay->>Redis: XADD Dispatch(request_id, scope, traceparent)
  Redis-->>C: Consumer Group delivery
  C->>Exec: claim execution(owner, run_token, lease_until)
  Exec->>DB: transactionally fence execution row
  DB-->>Exec: execution lease granted
  C->>SLease: SET NX session partition lease
  SLease->>SLock: create renewable lock handle
  SLock-->>C: session lock granted
  C->>Run: Build Runner(pinned config_version, trusted context)
  Run->>Ses: load session state/events/summary
  Run->>Kno: scoped knowledge query (SQL-authorized)
  Run->>Model: GenerateContent(request_id, trace context)
  Model-->>Run: model event/tool call
  Run->>Tool: authorize execution lease + policy
  Tool-->>Run: todo_write result or approval request
  Run->>Mem: capture private memory if configured
  Run->>Art: load/save referenced artifact if configured
  Run-->>C: Runner events + completion/error
  C->>DB: transaction: verify fence + append event_seq
  C->>DB: update execution + create reply_outbox
  DB-->>C: durable SUCCEEDED/FAILED/UNCERTAIN
  C->>Redis: XACK after durable transition
  Reply->>DB: claim reply_outbox with lease
  Reply->>Rate: acquire binding rate-limit permit
  Rate-->>Reply: permit / bounded wait
  Reply->>IMOut: one provider SendOnce attempt(reply_id, target)
  IMOut-->>Reply: receipt, known failure, or uncertain transport
  Reply->>DB: SENT / retry PENDING / PERMANENTLY_FAILED / UNCERTAIN
  IMOut-->>User: text reply (current IM projection)

  Note over GW,Reply: W3C trace context propagates through DB/Redis boundaries
  Note over GW,Reply: raw prompts, tool arguments and provider bodies are not traced.
  Note over Exec,SLock: Execution Lease owns PostgreSQL execution fencing, Session Lease/Lock serializes one Session, Reply Rate Limiter only paces IM sends.
```

源文件：[core-sequence.mmd](diagrams/core-sequence.mmd)，查看版：[core-sequence.svg](diagrams/core-sequence.svg)。

## Admission 事务的实际含义

同一个 IM message 的 identity mapping、inbox、session lane、execution、artifact reservation 和 dispatch outbox 不是分散的 best-effort 写入。PostgreSQL 提交是线性化点：事务失败后 Relay 没有可发布的 outbox；事务成功后即使 Gateway 进程退出，Relay/Worker 仍可继续。HTTP 入口用 `(tenant, app, source, source_id, idempotency_key)` 唯一键；Channel 用 `(tenant, app, binding, external_message_id)` 和 payload hash。

## 辅助时序：Worker crash 接管

```mermaid
sequenceDiagram
  participant R as Redis Stream
  participant W1 as Worker-1
  participant DB as PostgreSQL execution
  participant W2 as Worker-2
  participant E as PostgreSQL Execution Lease
  participant L as Redis Session Lease
  participant S as Session Lock
  R->>W1: pending delivery
  W1->>E: claim(owner=W1, run_token=t1)
  E->>DB: fence execution row
  W1->>L: acquire partition lease
  L->>S: create renewable lock handle
  W1--xW1: process crash / lease stops renewing
  R->>W2: XAUTOCLAIM after min-idle
  W2->>E: claim only if execution lease expired
  E-->>W2: new owner/run_token=t2
  W2->>L: acquire same partition after t1 expires
  L->>S: create new renewable lock handle
  W2->>DB: append/complete only with t2 fence
  W2->>R: XACK after durable transition
```

旧 Worker 即使恢复，也不能用 t1 写入新的 execution 状态；已启动且外部副作用状态不明时，系统选择 `UNCERTAIN`，而不是自动重放。

## 辅助时序：Human Approval

```mermaid
sequenceDiagram
  participant W as Worker
  participant DB as PostgreSQL
  participant A as Admin API
  participant W2 as Worker continuation
  W->>DB: insert tool_approval(PENDING)
  W->>DB: execution -> WAITING_APPROVAL
  A->>DB: approve/deny exact approval_id and scope
  DB-->>A: decision persisted
  W2->>DB: wait/read approval, claim execution again
  alt APPROVED
    W2->>W2: continue exact request under new execution lease
  else DENIED or EXPIRED
    W2->>DB: durable non-success result, no tool completion
  end
```
