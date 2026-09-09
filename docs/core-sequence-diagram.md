# 企业微信消息核心时序图

本文展示“企业微信用户发消息 → Agent 执行 → Tool 调用 → Session / Memory 写入 → IM 回复”的完整链路。组件职责见[架构设计文档](architecture.md)，事件和字段定义见[数据模型设计](data-model.md)。

## 完整链路

```mermaid
sequenceDiagram
  autonumber
  participant User as 企业微信用户
  participant WeCom as 企业微信智能机器人
  participant Channel as WeCom Channel Adapter
  participant Gateway as Gateway
  participant CP as Control Plane / Lease
  participant Store as Session / Memory Store
  participant Worker as Stateless Worker
  participant Gov as Governance API
  participant Runner as trpc-agent-go Runner
  participant Model as Model
  participant Tool as Tool / MCP

  User->>WeCom: 发送文本消息
  WeCom->>Channel: WebSocket aibot_msg_callback(req_id, msgid)
  Channel->>Channel: 校验 BotID / 账号并生成 request_id
  Channel->>Gateway: ChannelMessage + request_id + traceparent
  Gateway->>CP: 查询 Bot Tenant Allowlist、App、Active Version、Policy
  Gateway->>Store: 幂等写 message.input(request_id:input)
  Gateway->>CP: 获取 Session Lease
  CP-->>Gateway: fencing_token
  Gateway->>Store: 写 session.lease.acquired / run.started
  Gateway->>Worker: Bearer + 签名 Execution Manifest + traceparent
  Worker->>Worker: 校验签名、过期时间、version、traceparent
  Worker->>Runner: Runner.Run(model.Message)
  Runner->>Model: 模型流式调用
  Model-->>Runner: Tool call(name, args)
  Runner->>Gov: allowlist / Guardrail / 预算 / 二次确认

  alt 需要人工确认
    Gov-->>Gateway: pending_confirmation(request_id)
    Gateway->>Store: 持久化确认状态并暂停执行
    Note over Gateway,WeCom: 当前不占用一次性回复引用发送中间状态；操作员在 Management Console 处理确认
  else 已授权执行
    Gov-->>Runner: approved -> executing
    Runner->>Tool: 执行 Tool / MCP
    Tool-->>Runner: Tool result
    Runner->>Gov: completed / failed / outcome_unknown
    Runner->>Model: 携带 Tool result 继续生成
    Model-->>Runner: 回复 token / completed
    Runner-->>Worker: Agent Events
    Worker-->>Gateway: 内部 SSE Events(request_id, trace_id)
    Gateway->>Store: 按 fencing_token 追加 Session Event
    Gateway->>Store: 发布 Artifact 元数据
    Gateway->>Store: 写 latest_agent_reply Memory
    Gateway->>Store: 连续推进 State / Summary checkpoint
    Gateway->>Gov: 写 Audit、Metrics、Trace Span
    Gateway->>Store: 幂等写唯一 run.completed
    Gateway->>Channel: ChannelReply(message_id, text)
    Channel->>WeCom: aibot_respond_msg
    WeCom-->>User: 展示 Agent 回复
  end
```

## 关联标识

| 标识 | 生成与传播 | 用途 |
| --- | --- | --- |
| `request_id` | 由 provider `msgid`/`req_id` 稳定派生，经 Gateway、Worker、事件、治理和回复传播 | 业务幂等、重试查询、Tool 状态关联 |
| `trace_id` | Gateway 为请求建立平台 trace，并写入 Manifest、Audit、Artifact | 跨组件检索一次执行 |
| `traceparent` | 入口接收或生成 W3C 上下文，Manifest 固定后传给 Worker | OpenTelemetry 父子 span 传播 |
| `idempotency_key` | 每一步由 `request_id` 加阶段名派生 | 同内容重试返回原结果，不同内容冲突 |
| `fencing_token` | Control Plane 每次授予 Session Lease 时单调递增 | 拒绝失去 Lease 的旧 Gateway 写入 |

## 成功与异常语义

只有 Session Event、Artifact、`latest_agent_reply` Memory 和连续投影全部成功后，Gateway 才写唯一 `run.completed` 并调用 Channel Adapter 回复。Storage 写入失败、Lease 丢失或治理拒绝都不能发送成功回复。重复的企业微信消息复用同一 `request_id`：若事实源已有相同 input 或终态，则返回已有结果；相同 ID 但内容不同返回幂等冲突。

危险 Tool 在产生副作用前必须从 approved 原子进入 executing。若 Worker 在 executing 后失联，平台记录 `outcome_unknown`，不会自动重放。Gateway 失去 Lease 时取消 Runner；即使旧 goroutine 未及时退出，存储层也通过更高 fencing token 拒绝其后续事件和 Memory 写入。

真实 IM 当前只发送最终成功回复，不转发 `message.delta`，也不发送确认、失败或取消状态；完整 Provider 认证、Session 规则和限制矩阵见 [IM Channel Adapter 设计](im-channel-adapter.md)。
