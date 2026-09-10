# 运行时可观测性契约

本页定义 Issue #45 的框架内部 telemetry 边界和已完成实现：业务包只依赖
`trpcservice/observability` 暴露的接口，不直接依赖 OpenTelemetry SDK、OTLP exporter 或日志实现。
默认 provider 为 no-op，因此没有配置 exporter 时请求、Runner 和关闭流程的行为保持不变。

## 关联关系与稳定命名

HTTP 入口创建或提取 W3C trace context，并生成 `request_id`；子操作沿用同一个 context：

```text
http.request
  └── gateway.dispatch
       └── runner.execution
            ├── model.call
            ├── tool.call
            ├── storage.operation
            └── channel.receive / channel.send
```

这些 operation 名称是兼容性契约。span 只允许 `component`、`operation`、`status`、
`error_class`、`tenant_hash`、`app_hash`、`model_family` 和 `provider` 等受控字段；
不得写入 Secret、token、完整 URL、session/user/message 原文或 request body。取消和
deadline 必须映射为 `canceled` / `timeout` 错误类别，provider 原始错误只在进程内分类。

## 指标目录与标签

指标使用单调计数器、直方图和异步 gauge，名称保持稳定：

| 指标 | 类型 | 允许标签 |
| --- | --- | --- |
| `trpcservice_requests_total` | counter | component, operation, status, error_class |
| `trpcservice_operation_duration_ms` | histogram | component, operation, provider, status, error_class |
| `trpcservice_active_executions` | up/down counter | component |
| `trpcservice_runner_leases` | up/down counter | component, status |
| `trpcservice_operation_retries_total` | counter | component, operation, provider, error_class |
| `trpcservice_readiness` / `trpcservice_shutdown` | gauge | component, status |

标签通过白名单校验；`tenant_id`、`user_id`、`session_id`、`message_id`、完整 URL 和
原文永远不是标签。高基数关联使用 trace/log 字段或受控 digest。

## 结构化日志和脱敏

统一字段为 `request_id`、`trace_id`、`component`、`operation`、`status`、
`error_class`、`duration_ms`。集中 Redactor 对 token、API key、DSN、`secret_ref`、
`Authorization`、Webhook secret 和用户原文执行负向保护；错误只输出稳定类别，不透传
provider 文本或带敏感值的 stack。

## 生命周期和降级

`Provider` 必须支持创建、`Tracer`/`Meter`/`Logger` 获取和幂等 `Shutdown`。exporter
不可用或关闭超时时只能丢 telemetry，不能阻塞业务请求、Runner lease 或进程退出；
所有后台 goroutine 都由 provider context 管理并在关闭时可测试地回收。

## Issue #45 实施台账

- [x] 契约：provider、context、redaction、sampling 和 operation/label 白名单。
- [x] 代码：默认 no-op provider 与可注入接口。
- [x] 代码：HTTP → Gateway → Dispatcher → Runner 的 context/span 关联（Gateway/Dispatcher 已接入，Runner 通过同一 context 继承）。
- [x] 代码：稳定 operation 名称与错误类别契约；Model、Tool、Storage、Channel 使用通用 hook API 接入。
- [x] 代码：低基数指标、结构化日志、OTLP/HTTP 配置与 exporter 故障降级。
- [x] 测试：取消、deadline、shutdown、无 exporter、脱敏负向和标签白名单。
- [x] 验证：`go test ./...`、`go test -race ./...`、`go vet ./...`、`go build ./...`、
  `mkdocs build --strict`。

AuditEvent/usage/cost 持久化、Dashboard、告警和租户授权查询已分别在
[审计与用量](audit-usage.md) 与 [Issue #79](issue-79-observability.md) 中完成；
Session/Memory/Storage 后端、Admin 路由、migration 和 bootstrap 通过对应文档的集成验收。
