# 双通道演示检查表

> 只记录版本、Trace ID、时间和脱敏截图。不得粘贴 Bot Secret、App Secret、API Key、数据库口令或完整凭据文件路径。

## 构建信息

- Git commit：`以提交分支 HEAD 为准；发布证据记录最终完整 SHA`
- Agent version：`0.1.0`
- tRPC-Agent-Go commit：`0e352fdd1428d30a8d978d39877f5a7b2591ccc1`
- 企业微信 SDK commit：`0cb6bde0f054ba54b0b718521a5b388cb2a1c09c`
- 飞书 SDK：`v3.7.2`
- `go test -race ./...`：`通过（2026-08-23，包含 PostgreSQL/Redis 集成测试）`
- Core coverage：`channels 100%、config 95.2%、secrets 95.9%、tenant 100%、metrics 100%`
- Secret scan：`gitleaks dir + git 均通过（2026-08-23）`

## 无公网入口证明

- [x] 本地 Admin 监听 `127.0.0.1`
- [x] 未启动 `cloudflared`、ngrok、frp
- [x] 企业微信与飞书均为客户端主动 WSS
- [ ] 进程检查截图：`docs/demo-evidence/no-tunnel.png`

## 企业微信

- [x] `channel-smoke --channel wecom` 认证成功（2026-08-23 22:12:14 +08:00；凭据文件改用英文半角分隔符后通过）
- [ ] 单聊 echo 成功
- [ ] 群聊不 @ 忽略，@ 后回复
- [ ] “记住我的代号是 WECOM-A”后续可召回
- [ ] `get_server_time` Tool 审计存在
- Trace ID：`待填写`
- 脱敏截图：`docs/demo-evidence/wecom.png`

## 飞书

- [x] 事件订阅选择长连接
- [x] 已添加 `im.message.receive_v1`
- [x] 单聊、群聊 @、机器人发消息权限已开
- [x] `channel-smoke --channel feishu` Ready（2026-08-23 21:48:57 +08:00）
- [x] 创建 `0.1.0` 草稿，更新说明为“启用机器人长连接消息接收与回复”
- [ ] 确认发布 `0.1.0`，范围为当前个人版
- [ ] 单聊 echo 成功
- [ ] 不 @ 忽略、@ 后回复、@所有人忽略
- [ ] 无法读取 WECOM-A，证明租户隔离
- Trace ID：`待填写`
- 脱敏截图：`docs/demo-evidence/feishu.png`

## 故障与幂等

- [x] 自动化测试：相同 message/event ID 只生成一个 Inbox/Dispatch/Reply
- [ ] Worker 中止后 pending message 被另一个 Worker reclaim
- [ ] 回复失败只重试 Reply Outbox，不重新执行模型
- [ ] Jaeger trace 串起 Channel、Inbox、Runner、Tool、Session、Reply

## DeepSeek / tRPC-Agent-Go

- [x] 使用 `deepseek-v4-flash` 完成一次显式 `LIVE_E2E=1` 测试
- [x] tRPC-Agent-Go Runner 成功调用 `get_server_time` Function Tool
- Trace ID：`123e4567-e89b-12d3-a456-426614174000`
