# 生产可观测性

OpenTelemetry SDK 已接入生产链路，并配置了
持久化 Tempo 和生产告警：进程通过
OTLP/gRPC 向 Collector 输出 trace 和 metrics，Collector 执行 memory limiter 与 batch，
trace 写入 Tempo 命名卷，Prometheus 抓取 Collector 的 metrics exporter，Grafana 自动加载
平台 dashboard 与 Tempo 数据源。

## Compose 验收

```bash
docker compose up --build -d
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:9090/-/healthy
curl http://127.0.0.1:3000/api/health
curl http://127.0.0.1:3200/ready
curl 'http://127.0.0.1:9090/api/v1/query?query=up'
TRPC_AGENT_OBSERVABILITY_ACCEPTANCE_LIVE=1 ./scripts/observability_acceptance.sh
```

Grafana 默认 dashboard 位于 `Agent Platform / tRPC Agent Service`。Compose 开放匿名
Viewer 只是本机验收方式，不应直接暴露公网。Prometheus 使用命名卷保存 15 天数据，Tempo
使用独立命名卷保存 7 天 trace；任何
清理卷的操作都必须由运维人员明确执行。

## Trace

HTTP Server middleware 为 Admin、Gateway、企业微信和飞书入口统一提取 W3C
`traceparent`。上下文继续穿过 Inbox、Redis Streams、Worker、Runner、model、Tool、
Session/Memory 和 Outbox。`TRPC_AGENT_TRACE_SAMPLE_RATIO` 使用 0..1 的 parent-based
采样，默认 0.1；上游已经采样的 parent 会保持采样决定。

Compose 的 Collector 把 trace 发给 Tempo，Grafana Explore 可通过 `tempo` 数据源执行
TraceQL 查询。`traceparent` 会持久化穿过 Inbox/Redis Streams 和 Outbox；后台投递从该
parent 继续创建 `outbox.deliver`。不可信 `baggage` 和 `tracestate` 不跨 HTTP 或持久队列，
调用者提供的 request/correlation ID 只以不可逆短 hash 出现在 span attribute 中。OTLP
header 只能使用标准 OpenTelemetry Secret 环境变量或 Secret 挂载，不能写进租户 YAML。

## Metrics 与基数

指标只使用 `tenant.id`、`app.id`、`channel`、`operation`、`status` 以及有限的
`queue/domain/backend` 标签。user、session、request、message、trace ID 不进入标签。
当前包括：

- 请求量、操作延迟、模型首事件耗时、输入/输出 token、按版本价格计算的 `agent.cost.micros` 和 `agent.model.usage_missing`；
- IM 投递成功/失败；
- 按租户的 Inbox、Outbox、DLQ 深度；
- 活跃 Worker；
- 平台 PostgreSQL 健康和 ping 延迟；
- 实际 Session/Memory Adapter 每个操作的耗时和错误计数；
- 审计保留任务成功/失败。

Compose 已加载错误率、DLQ 非零、队列持续增长、无活跃 Worker 和 PostgreSQL 异常五类
规则。告警注释只描述操作动作，不包含 tenant payload、DSN 或凭据；生产环境还应由部署方
接入 Alertmanager/值班平台，并根据容量基线补充模型 p95/p99、Collector export failure 和
审计保留任务失败规则。

验收脚本默认只做结构与 Prometheus 规则校验；live 模式仅输出状态和 span 名称。指定一条
独立测试 trace 时可验证完整持久链路，可选 private canary 只执行“不存在”判断，不输出
canary 或 trace 原文。结果使用[脱敏报告模板](observability-acceptance-template.md)。

## 审计保留

每个节点都可以启动 Retention Worker，但 PostgreSQL advisory lock 保证一轮只有一个节点
执行。Worker 只读取 `tenants.current_config_version` 指向的不可变配置；disabled tenant 或
`retention_days <= 0` 不删除。删除条件始终包含 `tenant_id` 和截止时间，租户之间不会互相
清理。

当前审计表以 PostgreSQL 为在线事实存储；租户可以把 Audit `migration_target` 配置成受认证的
外部 HTTPS WORM 端点，同步归档脱敏 envelope。归档失败进入低基数错误指标且不会把 endpoint、
凭据或正文写入日志。法律保全流程和特定 SIEM 产品连接器仍由部署方补充，不能把 Prometheus
或 Collector debug exporter 当作审计归档。
