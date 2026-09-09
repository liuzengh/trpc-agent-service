# 持久化 Trace 与生产告警验收报告模板

> 只记录计数、状态和 span 名称。不得粘贴 trace JSON、指标原文、用户消息正文、HTTP
> Header、SecretRef 解析值、数据库 DSN 或任何平台凭据。

## 基线

| 项目 | 脱敏记录 |
| --- | --- |
| 日期 / 环境 | `<UTC time / non-production>` |
| Git commit | `<commit hash>` |
| Compose project | `<non-secret project name>` |
| Tempo / Collector / Prometheus / Grafana | `<image digest or approved version>` |
| 既有数据卷 | `保留；未执行 down -v 或 volume rm` |

## 自动化结果

| 检查 | 结果 | 脱敏证据 |
| --- | --- | --- |
| Compose 渲染与 Prometheus rule 校验 | PASS / FAIL | `<rule count>` |
| Tempo OTLP exporter 与持久卷 | PASS / FAIL | `<health only>` |
| Grafana Tempo 数据源 | PASS / FAIL | `<datasource uid only>` |
| 错误率 / DLQ / 积压 / 无 Worker / PostgreSQL / usage 缺失告警 | PASS / FAIL | `<alert names only>` |
| `traceparent` 跨 Inbox 和 Outbox 持久化 | PASS / FAIL | `<span names only>` |
| Baggage / tracestate / 正文 / Secret canary 不进入遥测 | PASS / FAIL | `<absent=true>` |

## 完整链路

使用独立测试 tenant/binding/message，采样率临时设为 1。只勾选 span 是否存在，不记录
attribute 值或 Trace ID。

| Stage | 结果 |
| --- | --- |
| `channel.callback` / `gateway.callback` | PASS / FAIL |
| `inbox.claim` | PASS / FAIL |
| `worker.run` / `runner.execute` | PASS / FAIL |
| `model.stream` | PASS / FAIL |
| `tool.call`（含 Tool 的用例） | PASS / FAIL / N/A |
| `session.write` / `memory.summary.write` | PASS / FAIL |
| `outbox.write` / `outbox.deliver` | PASS / FAIL |

## 结论与例外

- 结论：`PASS / FAIL`
- 告警通知链路（Alertmanager / 值班平台）：`部署方配置 / 已验证`
- 未解决问题：`<仅写问题分类和负责人，不写 payload>`
