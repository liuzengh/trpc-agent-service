# 双通道演示检查表

> 只记录版本、Trace ID、时间和脱敏截图。不得粘贴 Bot Secret、App Secret、API Key、数据库口令或完整凭据文件路径。

## 构建信息

- Git commit：`以提交分支 HEAD 为准；发布证据记录最终完整 SHA`
- Agent version：`0.1.0`
- tRPC-Agent-Go commit：`0e352fdd1428d30a8d978d39877f5a7b2591ccc1`
- 企业微信 SDK commit：`0cb6bde0f054ba54b0b718521a5b388cb2a1c09c`
- 飞书 SDK：`v3.7.2`
- `go test -race ./...`：`通过（2026-08-24 11:47 +08:00，包含 PostgreSQL、Redis、Qdrant、MinIO 与 OTLP/Jaeger 集成测试）`
- Staticcheck / go vet：`通过（2026-08-24 11:47 +08:00）`
- Core coverage：`channels 100%、config 91.3%、secrets 95.9%、tenant 100%、metrics 100%`
- Docker image：`trpc-agent-service:acceptance 构建通过（2026-08-24 11:49 +08:00，Go 1.22 builder）`
- Secret scan：`Gitleaks v8.24.3 全历史扫描 8 commits，no leaks found（2026-08-24 11:49 +08:00）`

## 无公网入口证明

- [x] 本地 Admin 监听 `127.0.0.1`
- [x] 未启动 `cloudflared`、ngrok、frp
- [x] 企业微信与飞书均为客户端主动 WSS
- [x] 2026-08-23 23:11 +08:00 进程检查未发现 `cloudflared`、ngrok、frp
- [x] 最终进程复核未发现 `cloudflared`、ngrok、frp、frpc、frps；可复核状态见 [`docs/demo-evidence/README.md`](demo-evidence/README.md)

## 企业微信

- [x] `channel-smoke --channel wecom` 认证成功（2026-08-23 22:12:14 +08:00；凭据文件改用英文半角分隔符后通过）
- [x] 历史 Echo Smoke 收发成功（只证明通道，不作为正式 Agent 证据）
- [x] 正式私聊经 Inbox → Redis Stream → 双 Worker → DeepSeek → Reply Outbox 回复成功（2026-08-24 10:12:55 +08:00）
- [x] 群 `trpc-test` 已完成不 @ 平台过滤与明确 @ 后回复；真实 ChatID：`wrkSFfCgAAQeb7frRm-RqgENEXdw7vaA`
- [x] 实际私聊“记住我的代号是 roboutezhao”后，第二轮正确召回 `roboutezhao`（2026-08-24 10:19:01 +08:00）
- [x] `get_server_time` Tool 审计、Reply Outbox `done/1` 与 8 Span Trace 一致（2026-08-24 11:24 +08:00）
- [x] 询问飞书租户代号明确返回不知道；Trace `81d7dcc7-cd88-4ece-a9e1-85ae8f82290a`
- [x] 单 Worker 真实私聊返回 `WORKER-FAILOVER-OK`；Trace `47a470ea-f375-4445-9d4d-334f90c848b2`
- Trace ID：群 Tool `cfa0993e-ad8b-4b33-b1be-c9a442a4a83d`；首次记忆 `aac22b88-6aab-47cc-b5e1-d47395000d46`；第二轮召回 `63310fe0-017e-4852-aaab-307a3436c43c`
- 脱敏截图：[群 Tool](demo-evidence/wecom-group-tool.png) · [私聊隔离与故障恢复](demo-evidence/wecom-private-isolation-failover.png) · [Jaeger](demo-evidence/jaeger-wecom-trace.png)

## 飞书企业版

- [x] 事件订阅选择长连接
- [x] 已添加 `im.message.receive_v1`
- [x] 单聊、群聊 @、机器人发消息权限已开
- [x] `feishu-primary` 保留原 Tenant/Binding，改为读取企业版更新凭据；未复制凭据、未迁移个人版数据
- [x] Bot Info API 解析自身 OpenID且 WSS Ready（2026-08-23 23:18:58 +08:00，持续连接）
- [x] 旧个人版 `0.1.0` 与 Echo 仅标为历史证据，不作为提交版入口
- [x] 企业版应用版本、机器人可用范围与事件权限已在企业版上线及真实消息阶段复核
- [x] 企业版正式私聊两轮成功，第二轮正确召回 `FEISHU-B`（2026-08-24 10:17:49 +08:00）
- [x] 私聊真实 ChatID：`oc_85d8ae0b50a3f1c8bc5345a5f343bf0e`
- [x] 企业群 `测试企业 - trpc-test`：不 @、@其他成员、@所有人均忽略；明确 @机器人后回复
- [x] 真实 ChatID `oc_4f8cabf6a1460878e726b138d01c7a90` 已关联 Session/Audit/Trace
- [x] 无法读取企微租户代号；Trace `e3b3bcf9-45ab-4a82-a009-84179209b251`
- [x] `0.1.2-acceptance` 无重启发布 revision 4，精确返回 `RUNTIME-V012`；随后回滚 `0.1.0` revision 5 且 Worker PID 未变化
- Trace ID：群 Tool `9b7f941e-93ae-405f-946a-9b2fe1b6d76f`；发布探针 `71ff354a-929c-49b1-b285-fdbf54e98f10`；回滚探针 `61bd8077-675c-4456-9c3d-bb205594eace`
- 脱敏截图：[群 Tool](demo-evidence/feishu-group-tool.png) · [私聊隔离与发布/回滚](demo-evidence/feishu-private-runtime-isolation.png) · [Jaeger](demo-evidence/jaeger-feishu-trace.png)

## 故障与幂等

- [x] 自动化测试：相同 message/event ID 只生成一个 Inbox/Dispatch/Reply
- [x] PostgreSQL 实测：发布 `0.1.1` 后 revision 2，重新发布 `0.1.0` 回滚后 revision 3；LISTEN/NOTIFY 缓存失效测试连续通过
- [x] Redis 集成测试：Worker 2 使用 XAUTOCLAIM 接管 Worker 1 未 ACK 的 pending message
- [x] 平台测试：回复连续失败只重试 Reply Outbox，Agent 调用次数保持一次
- [x] 实机恢复：休眠唤醒后的 Redis `i/o timeout` 不再终止 Worker，新增瞬时读错误退避回归测试
- [x] 实机恢复：tRPC-Agent-Go PostgreSQL Session 改用 `runner_` 表前缀，避开控制面 `session_events`；两条原 DLQ 消息按序重放成功
- [x] 同 session 锁竞争改为等待 lease 后二次查重，避免 Redis Stream 热重投
- [x] 所有非模型处理错误统一最多尝试 8 次，超过阈值进入 DLQ，禁止无限热重投
- [x] 修复后 Redis 消费组 `pending=0`、`lag=0`，Admin `dispatch_ready=0`、`reply_ready=0`（2026-08-24 10:27 +08:00）
- [x] 飞书与企微新 Trace 均包含 `channel.accept_inbound`、`outbox.dispatch`、`worker.process_message`、`runner.run`、`agent.governance`、`tool.get_server_time`、`session.commit_result`、`channel.deliver_reply`
- [x] 停止 PID `56464` 后仅 PID `49484` 完成真实企微消息；随后用同一二进制恢复双 Worker
- [x] 两条历史 DLQ 记录原样保留，并关联此前成功重放结果

## Qdrant / MinIO

- [x] `backend-smoke --tenant all` 通过（2026-08-23 23:15 +08:00）
- [x] `tenant_wecom_demo`：Qdrant 与 MinIO 隔离为 true
- [x] `tenant_feishu_demo`：Qdrant 与 MinIO 隔离为 true
- [x] 同逻辑 point/object key 的双向串读检查均失败，smoke 后完成删除清理

## DeepSeek / tRPC-Agent-Go

- [x] 使用 `deepseek-v4-flash` 完成一次显式 `LIVE_E2E=1` 测试（2026-08-23 23:25 +08:00）
- [x] 修改后的 tRPC-Agent-Go Runner、治理 Callback 与 `get_server_time` Function Tool 全链路通过
- Trace ID：`123e4567-e89b-12d3-a456-426614174000`
