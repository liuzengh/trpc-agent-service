# 本机企业微信智能机器人单聊联调

执行时间：2026-09-10（Asia/Shanghai）

结论：**通过真实平台联调；不构成生产部署验收。**

## 已验证

| 检查项 | 结果 |
| --- | --- |
| 本机服务 | `/healthz=200`、`/readyz=200` |
| 企业微信智能机器人 | `acme-wecom-aibot` 长连接认证成功 |
| 入站与路由 | 企业微信单聊消息由 `wecom-aibot` 绑定路由至 `acme` 租户，产生独立用户和会话哈希 |
| 模型 | `glm-5.2` 的 OpenAI 兼容调用已成功完成并结算 |
| Agent 与审计 | 处理请求记录为 allow；记忆读取工具策略允许且留下脱敏审计 |
| 出站 | `im_delivery_total{tenant="acme",channel="wecom-aibot",result="success"}=1` |
| 处理计数 | `agent_requests_total{tenant="acme",channel="wecom-aibot",result="success"}=1` |

服务随后已正常停止，释放机器人长连接。

## 验收边界

本次使用 `config/example.yaml` 的本机 local 配置，其中队列、会话、控制面和协调均为进程内实现。它验证真实企业微信 API 长连接、消息入站、模型调用、会话路由和回复投递；不验证生产 PostgreSQL/Redis、多副本接管、OIDC、外部 Secret Manager、持久审计或真实生产故障恢复。
