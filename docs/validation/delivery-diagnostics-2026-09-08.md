# rc.7 发送诊断补齐记录

2026-09-08，在用户确认第二次文档查询收到回复后继续开发。本轮不增加 Agent 工具、不修改租户 revision 或 `.env`，也不重放任何历史请求。

## 触发问题与证据

首次文档请求 `req_5a1ef3994f1ddb4b59f31adeeaf43e0b` 的真实 MCP 调用成功，发送失败留下 `out_195d339ebad1d0651054d66377978dfa`：dead、attempt 1、part 0 unknown，无 provider 回执。旧错误仅为 `Telegram delivery outcome unknown`，trace 的 reply.send 也没有 Error 状态。无法从旧记录还原具体网络错误。

第二次请求 `req_f047d7d9c7f016eb5ed15a6981f24c26` completed，回复 `out_e8950b498818e6de599f913f4723e626` sent、attempt 1、有 part 回执，但没有新 MCP 调用。详见[文档 MCP 记录](project-docs-mcp-2026-09-08.md)。

## 修复范围

- 增加固定枚举的 HTTP 错误类别和阶段观测，跨回调使用原子状态，无新增 goroutine。
- Telegram 返回安全诊断，不包装原始 HTTP 错误，不记录 provider description；不输出 URL、Token 或聊天正文。
- Outbox 保留安全错误摘要；审计加入诊断字段，失败 reply.send 设置 Error 状态并通过已有隐私出口导出受限元数据。
- 不修改超时值、幂等规则、分段协议或未知发送终止重试行为；只读检查成功不被当成此前发送未生效的证据。

## 验证

`TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh` 通过全仓 race/lint/build、独立 PostgreSQL/Redis/Qdrant/MinIO、备份恢复与 promtool 告警检查。新增测试覆盖真实本地 HTTP 写出后超时、429/错误响应、底层 canary 脱敏、并发 HTTP hook、Sender 更换后仍不重发、审计/trace、PostgreSQL 安全诊断持久化及原 unknown 状态保留。

测试容器和数据均为隔离合成数据，测试未加载日常 `.env`、未调用真实 IM，也未故意断开本地代理制造真实故障。本次仅清理测试脚本自己创建的临时容器和数据。

## 本地部署

本机已启用 `0.2.0-rc.7`，schema 仍为 23，新 Agent PID 为 582645。部署前后比对 App version 6、revision `tutorial-docs-mcp-v6`、后端配置及唯一已知未知回复的快照，内容一致；该旧回复仍是 dead/unknown、attempt 1。没有发布新 Agent revision，未修改 `.env`。

私有备份目录为 `data/delivery-diagnostics-upgrade.VATqJNvV/`，含旧配置、二进制、日志、PostgreSQL dump、Redis RDB 和控制面快照。目录 0700，敏感文件 0600；SHA-256 与 dump 目录检查通过。只备份未恢复真实业务数据，不提交凭据或完整聊天数据。

本地 `/readyz`、MCP `/healthz`、公网 `/healthz` 正常。使用与 Agent 相同的代理配置，Telegram getMe/getWebhookInfo 返回成功，Webhook 地址一致、pending_updates=0；没有调用 sendMessage 补发旧回复。独立 Memory/Artifact Router 仍可读回原长期记忆和 MinIO 附件；原跨 Session 附件拒绝也通过。真实 MCP 协议的 `fencing` 只读查询已验证，可返回匹配文档；它没有运行模型或生成业务 Run。

后续 `fencing` 请求 `req_79f39cf23876ed5363ad99b7f1b26fb6` 已在 rc.7 完成同请求的工具调用和单次投递复测，见[完整记录](project-docs-mcp-2026-09-08.md)。没有故意让真实 Telegram 发送超时来验证新诊断字段，这部分证据来自自动与隔离测试。
