# Telegram 发送失败如何定位

Agent 执行成功不等于 IM 回复已经送达。rc.7 在不放宽重复发送保护的前提下，补齐 Telegram 出站的安全诊断信息。

## 运行链路

`Runner 完成 → 持久 Outbox → Sender 领取 → BeginPart 记录尝试 → Telegram HTTP 请求 → FinishPart → 回复终态与审计`。

只有取得有效 provider message ID 并保存发送结果，才标记 sent。如果请求报错、响应读不完整或缺少明确回执，part 保持 unknown，父回复进入 dead，等待有事实依据的人工核对。阶段观测和错误类别都不能证明对端没有处理请求，不能据此自动重发。

## 新增诊断信息

Telegram Adapter 使用每请求的 `httptrace.ClientTrace` 观察 DNS、连接、TLS、写请求、等待响应和响应读取阶段。回调只更新原子状态，不创建后台协程，保留已有 HTTP trace hook。涉及代理或连接重用时，它表示本次观察到的 HTTP 进度，不等同于 Telegram 服务器内部状态。

- `kind`：timeout、canceled、dns、connect、tls、connection_reset、unexpected_eof、transport、invalid_response、provider_rejected。
- `phase`：request、dns、connect、tls、write_request、wait_response、response_headers、response_body、provider_response。
- `http_status`：已收到的有效 HTTP 状态码；未收到时为 0。

类别依据 Go 错误类型/错误链判断，不解析不可信错误文本。原始 `url.Error` 不再向外传递；Telegram 的错误 description 不记录，只保留数值错误码。白名单之外的诊断值会收敛到固定默认值。

Outbox 的 `last_error_message` 示例：

```text
Telegram delivery outcome unknown [kind=timeout phase=wait_response http_status=0]
```

审计保留原有 `reply_delivery_unknown / channel_delivery_unknown`，在 details 中增加 `delivery_error_kind`、`delivery_phase`、`delivery_http_status`。`reply.send` span 标记为 Error，并附上 `error.type`、`delivery.error.kind`、`delivery.phase` 及已知 HTTP 状态码。元数据出口仍不导出异常正文、URL、主机、Token、请求内容或回复正文。

原来的明确 429/retry_after 和拒绝策略不变。未知投递仍停止自动重试；本补丁不会修改历史记录，也不会给已失败的 trace 补写从未采集的底层原因。

## 遇到“没回复”时

先按 request_id 分别查看 Run、Tool Journal、Outbound/part 和 trace。服务存活、模型能用、Webhook 正常，只能说明相应部分在检查时正常。若 Run completed 但 Outbound dead/part unknown，重点是发送链路，不应通过重启模型或重放有副作用的请求尝试修复。

本次改动由合成测试覆盖真实 HTTP 写出后的超时、错误/不完整响应、429、底层错误脱敏、并发 hook、跨 Sender 不重发、审计/trace 与 PostgreSQL 诊断持久化。真实问题和部署记录见[本地记录](validation/delivery-diagnostics-2026-09-08.md)。没有主动制造真实 Telegram 故障。
