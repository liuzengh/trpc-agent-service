# Issue #79：生产可观测性、Dashboard 与告警

> 本页是 Issue [#79](https://github.com/XnLemon/trpc-agent-service/issues/79) 的交付契约。它承接 Issue #45 的 provider-neutral telemetry 和 Issue #54 的 tenant-scoped usage/audit，记录代码、资源和验收证据。

## 已交付能力

目标是让一次可信请求在 http.request → gateway.dispatch → runner.execution 下继续关联到 model.call、tool.call、storage.operation 和 channel.receive/channel.send，并提供可安全聚合的运行指标、租户授权的查询模型以及可部署的 dashboard/alert 资源。

审计事实源继续由 AuditEvent 提供，Prometheus/Grafana 展示进程级低基数聚合，租户 usage/cost
通过授权查询读取；Session/Memory/Knowledge/Artifact 和 IM 适配器复用各自已验收模块。指标导出
失败只丢弃 telemetry，不阻塞业务提交、租约或关闭。

## Trace 合同

所有入口接受 W3C-compatible correlation（当前 HTTP 兼容 X-Request-ID/X-Trace-ID），缺失 request ID 时生成受界限 ID。子操作必须继承同一 context.Context；取消和 deadline 分别归类为 canceled/timeout。span 名称稳定且只允许下列受控属性：component、operation、status、error_class、tenant_hash、app_hash、model_family、provider、channel。

每个 operation 只能有一个终态：开始时可记录 `status=started`，但必须在成功、业务错误、取消或超时时记录恰好一个终态并结束 span。HTTP/SSE 请求的终态由整个协议生命周期决定：客户端断开、写入失败、request context 取消、空 stream 或没有 `done` 的不完整 stream 不能被记为成功；已经写出 HTTP 200 后只能在 telemetry/audit 中记录失败或取消，不能再写第二个 HTTP status。Model callback 以一次模型调用为边界：流式 partial response 不能提前结束 span 或重复累计 token；`GenerateContent` 在创建响应流前返回的错误也必须结束同一 span。企业微信 receive 创建的 context 必须作为 Gateway/Runner dispatch 的 parent，不能从独立的 handler 根 context 重新开始链路。

Storage telemetry 覆盖实际运行时存储能力/Session service 的读写方法（Get/Create/Update/Delete Session、Append/List event、message/reply lifecycle），而不只覆盖 capability factory construction 或 inbound claim；新增 adapter 必须复用同一 hook。

不得写入 token、API key、DSN 密码、Authorization、完整 URL、session/user/message/request 原文或不受界限的外部 ID。tenant/app 关联只使用配置的短 hash；hash 不作为 Prometheus label，避免租户数量直接变成指标基数。

## 指标目录

| 名称 | 类型 | 语义 | 允许标签 |
| --- | --- | --- | --- |
| trpcservice_requests_total | counter | 各组件请求/调用量 | component, operation, provider, channel, status, error_class, model_family |
| trpcservice_operation_duration_ms | histogram | HTTP、Runner、Model、Tool、Storage、Channel 延迟 | 同上 |
| trpcservice_active_executions | up/down counter | 当前执行数 | component |
| trpcservice_runner_leases | up/down counter | Runner lease 数 | component, status |
| trpcservice_operation_retries_total | counter | 重试次数 | component, operation, provider, error_class |
| trpcservice_tokens_total | counter | 输入/输出 token 聚合 | component, provider, model_family |
| trpcservice_cost_minor_total | counter | 按币种隔离的授权聚合成本（最小货币单位） | component, provider, model_family, currency |
| trpcservice_backend_operation_duration_ms | histogram | Session/Storage 后端延迟 | component, provider, status, error_class |
| trpcservice_channel_deliveries_total | counter | IM 发送成功/失败/重试/DLQ | channel, provider, status, error_class |

标签白名单在代码中集中校验；禁止 tenant_id、user_id、session_id、message_id、request_id、trace_id、完整 URL 和原文。provider 的 canonical 枚举是 `openai|postgres|inmemory|other`，channel 是 `api|telegram|wecom|outbox|other`，model_family 是 `gpt|claude|gemini|other`，currency 是小写 ISO-4217 alpha-3 code（例如 `usd`、`cad`、`aud`），每个币种保持一一映射；不支持或格式错误的 currency 拒绝成本 telemetry，不能把多个真实币种合并到 `other`。未知 provider/channel/model 值仍统一映射到 `other`，不能直接把模型名作为 label。Token/cost 的生产来源是单个 AuditEvent 的 `Event.Cost` 增量：只有 `Append` 返回 `err == nil && Duplicate == false` 时上报一次，不能把周期性的 `UsageTotal` 快照当作 counter 增量；重复事件不能重复计数，写入失败不能伪造成本；成本查询和 dashboard 必须按 currency 分组，不能跨币种直接相加。匿名或未获授权的调用者拒绝访问；平台运维管理员才可读取进程级聚合，租户调用者只能读取其授权的 AuditEvent usage aggregate。

## Dashboard 与访问控制

发布包提供 deploy/observability/ 下的 Prometheus recording/alert rules 和 Grafana dashboard JSON。Prometheus telemetry 本身是进程级低基数聚合，不含 tenant label，因此 query adapter 固定区分两种视图：`platform` 视图只对平台运维管理员开放，显示请求/延迟/Runner/交付等进程聚合；`tenant` 视图只返回经授权的 AuditEvent usage aggregate（token/cost）和该租户的 scope 元数据，不返回跨租户的 process telemetry。普通租户只能访问自身 usage 聚合，跨租户管理员只能访问其授权租户集合；匿名或未获授权请求拒绝，经授权但超出基数预算的维度归入 aggregate 桶。Adapter 先通过平台的 tenant authorization，再构造固定查询模板；客户端不能提交任意 PromQL 或把原始 tenant label 拼进查询。随仓库提供的 Grafana JSON 是 platform-only 的进程级示例资源，使用固定 PromQL，不能直接作为 tenant authorization boundary；生产 tenant dashboard 必须经 adapter 返回的授权 usage view 渲染。

生产进程通过标准 OpenTelemetry 环境变量启用 OTLP/HTTP：`OTEL_EXPORTER_OTLP_ENDPOINT`（host:port 或完整 URL）为空时使用 no-op provider；可选 `OTEL_EXPORTER_OTLP_HEADERS`（逗号分隔的 `key=value`）、`OTEL_EXPORTER_OTLP_INSECURE` 和 `OTEL_SERVICE_NAME`。Endpoint 配置错误使 bootstrap fail closed；exporter shutdown/发送失败只丢 telemetry，不阻塞业务、lease 或进程关闭。Header 值只传给 exporter，不能写入 span、metric 或日志。

Dashboard 展示四组面板：请求与错误率、端到端/分阶段延迟、Runner lease/active execution/队列重试/IM 交付、token/cost 与后端延迟。审计详情、原始消息和 provider 错误必须跳转到受权限保护的审计查询，不从 telemetry 反推。

## 告警初始规则

初始阈值是可调的起始值，不是容量承诺：5 分钟顶层 HTTP 终态错误率 > 5%；P95 gateway/runner 延迟 > 2s；IM delivery retryable/dead-letter failure > 1%；backend P95 > 500ms。HTTP request rate/error rate 只统计 `component="http", operation="http.request"`，避免把同一请求的 Gateway、Runner、Model、Tool、Storage 终态叠加成一个 request。规则使用极小 epsilon 仅避免零流量除零，不改变实际终态分母。终态集合固定为 `complete|success|error|failure|canceled|timeout`，错误分子为 `error|failure|canceled|timeout`，分母只取上述终态（`started` 不进入分母）。Delivery 比率按投递尝试计数：分母为 `success|retry|failure|dead_letter`，分子为 `retry|failure|dead_letter`；dead-letter 是最终结果，retry 是当前尝试结果。token/cost 预算由同一个授权 query adapter 从 AuditEvent aggregate 计算 80%/100% 阈值，不把 tenant_id 放进 Prometheus label；本仓库不把租户预算伪装成 Prometheus 指标，部署方如需 Prometheus 告警，必须由 adapter 发布按 currency 分组的受控 aggregate gauge 后自行接入。告警 payload 只带 component、operation、channel、provider、error_class 和受控聚合标识。

## 验收台账

- [x] HTTP/IM callback 到 Gateway、Runner、Model、Tool、Storage、IM reply 的 trace context 连续且取消安全；每个模型调用和 HTTP/SSE 请求只有一个终态（含流式、断连、空 stream 和创建流失败）。
- [x] 目录指标覆盖 volume、latency、终态 errors、IM success/retry/dead-letter、tokens、cost 和 backend latency。
- [x] 标签白名单、固定低基数映射、脱敏和 tenant-authorized aggregate query adapter 有负向测试。
- [x] Prometheus/Grafana dashboard 与 alert rules 可加载，并不包含 secret、原始租户标识或无界 ID。
- [x] no-op provider 保持默认行为；生产 bootstrap 按 OTLP 环境变量构造 exporter，trace/metric exporter 的 shutdown 故障不阻塞业务路径。

验收证据包括 `go test ./...`、`go test -race ./...`、Prometheus/Grafana 资源加载、真实 WeCom
trace、低基数与脱敏负向测试，以及 observability compose 的健康检查。
