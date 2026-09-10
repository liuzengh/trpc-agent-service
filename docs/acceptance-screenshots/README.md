# 本机外部验收证据

验收日期：2026-09-09（Asia/Shanghai）

本目录保存本机交付验收截图，不包含密钥：

- `acceptance-status.png`：Compose、`/readyz`、真实 IM、Trace、指标和未验证边界汇总。
- `企业微信IM.png`、`飞书IM.png`：客户端真实消息收发截图。
- `jaeger-wecom-trace.png`、`jaeger-feishu-trace.png`：真实消息对应的完整 Trace。
- `prometheus-targets.png`、`prometheus-metrics.png`：scrape target 和 IM reply success 指标。
- `grafana-operations.png`：Grafana 运行指标面板。

本次企业微信、飞书客户端真实收发由人工操作确认；后台对应 execution 为 `SUCCEEDED`，reply outbox 为 `SENT` 且有 Provider receipt。Kubernetes Pods/HPA、生产 HA 和生产容量不在本目录的验收结论内。
