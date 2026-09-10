# Issue #77：Telegram 与 WeCom 扩展通道能力

本页是 Issue #77 的实现合约和验收 ledger。它在 Issue #31/#60 的可信租户路由、
幂等和 Outbox 边界上扩展能力，不改变 Gateway 的认证边界，也不把不同微信产品混成一个
协议。

## 交付范围

- Telegram 同时支持 long polling 与带 secret header 的 webhook；webhook 由调用方拥有 HTTP
  listener，Adapter 负责请求去重、更新校验和优雅关闭。
- Telegram 文本、caption 媒体和 rich update 都规范化为受限的 `InboundMessage`；未知更新
  fail closed。回复事件先聚合为文本，无法安全表达的 rich/card 事件使用固定 fallback。
- WeCom Provider 支持多个 tenant/binding account 的注册、按账号隔离的 provider cache，
  以及有界 worker group。文本回复同时支持 direct receiver 和 group chat；本地 receipt
  可重放，无法查询供应商状态时 `Reconcile` 返回 unknown。
- Public WeChat 与 WeChat customer-service 仅提供显式、互不兼容的 provider boundary；
  它们不会被标记为 WeCom，也不会复用 WeCom credential 或路由。
- Gateway Runner event stream 必须完整消费；adapter 使用统一 renderer 聚合 text，stream
  partial 不提前发送，error/不可表达 rich event 一律使用稳定 fallback。

## 可信与生命周期约束

外部 Telegram update、WeCom XML、微信 payload 不能选择 tenant、binding、app、secret 或
runner。所有 adapter 继续使用现有 `RoutingTarget`/`Principal`，消息幂等键必须包含 binding
作用域。Webhook secret 只用于恒时比较，不进入日志、错误或审计正文。

Webhook 和 worker group 都由创建者关闭；关闭先阻断新 admission，再取消并等待已经接受的
处理。Provider 的 HTTP client、credential resolver 和 durable Outbox 仍由调用方拥有。

## 验收矩阵

| 项目 | 证据 | 状态 |
| --- | --- | --- |
| Telegram webhook ownership、secret、replay-safe delivery | `telegram.Webhook` 与测试 | ✅ |
| Telegram media/rich update 与 deterministic fallback | `normalizeUpdate`、renderer 与测试 | ✅ |
| WeCom multi-account registry、worker group | `wecom.Registry`/`WorkerGroup` 与测试 | ✅ |
| WeCom group delivery、receipt reconciliation | `Provider.Deliver/Reconcile` 与测试 | ✅ |
| Public WeChat/customer-service provider boundaries | `channels/wechat` 显式类型与测试 | ✅ |
| Runner event text/stream/card fallback | `gateway/replies` renderer 与 adapter 测试 | ✅ |
| Deterministic external integration E2E | fake Telegram/WeCom/WeChat tests | ✅ |

生产凭据和真实公网 webhook 由受保护部署环境注入；live E2E workflow 复用同一 adapter 契约，
仓库测试不记录真实 token、用户正文或 provider 原始错误。
