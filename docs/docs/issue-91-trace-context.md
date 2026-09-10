# Issue #91：跨 Outbox 的 Trace Context

异步回复投递会跨越进程内 goroutine 和 Worker 重启边界。ReplyCorrelation 现在除
`request_id`/`trace_id` 外持久化经过 W3C Trace Context 校验的 `traceparent`；不持久化
baggage、消息内容、secret 或 Prometheus labels。Materializer 从 inbound context 注入
traceparent，Worker 在创建 `channel.send` span 前恢复 parent；同一回复的 retry 继续使用
同一个 correlation。旧记录或格式无效的 parent 保持可投递，并从新的 root span 开始。

## Tempo 检查

使用 Grafana Explore 的 Tempo datasource，按 inbound `trace_id` 或 request correlation
查看 `channel.receive -> gateway.dispatch -> runner.execution -> channel.send`。Trace
correlation 只用于链路诊断；tenant usage、审计详情和授权边界仍由 AuditEvent/query
adapter 负责，不能从 trace 反推出跨租户数据。

## Issue #91 台账

- [x] ReplyCorrelation 持久化 traceparent，并为旧 schema 提供空值兼容
- [x] WeCom/Telegram 共享 Materializer 注入 inbound context
- [x] Worker 在 delivery/retry 前恢复 parent，缺失时使用 root span
- [x] malformed context、旧记录、重启/重试和 shutdown best-effort 测试
- [x] Grafana Tempo 检查文档与 audit authorization 边界
