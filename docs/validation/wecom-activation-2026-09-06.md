# 企业微信 MCP 开发环境启用记录

日期：2026-09-06。用户在得知迁移、重启、仅处理原测试群及本人 @ 消息的范围后，要求继续执行直到需要其操作。

## 实际操作

1. 确认原服务健康、AgentRun 和 Outbound 没有待处理项，背景作业均已完成。日常数据库处于迁移 11。
2. 真实模型预检通过：`glm-5.3-flash`，响应 `OK`，耗时约 2.95 秒。
3. 获取新会话快照，用既有单次发送记录里的群指纹匹配原测试群，匹配数为 1；不把其他群或私聊当作替代目标。
4. 仅查询已验证的 `2026-09-06 15:57:38` 至 `16:01:38` 窗口，再次取得 `TRPC-WECOM-TEST-001`。用其发送者和 @ 前缀生成配置，没有将旧测试消息交给 Runner 重放。
5. 真实格式显示名称中含空格、正文可紧接 @ 名称。新增显式 `mention_style=prefix` 并补测试，默认 `whitespace` 模式保留。匹配依靠文本前缀而非实体元数据，这一限制已写入运行说明。
6. 将原 `.env`、正在运行的旧程序、原日志及 PostgreSQL 自定义格式备份保存在本机私有目录 `data/wecom-activation-EerZuk/`。目录权限 `0700`，环境/Binding/数据库备份权限 `0600`；数据库备份本机与容器副本校验和一致。不要上传该目录或把备份内容贴进聊天。
7. 保留原 Telegram grant，追加 `wecom_mcp_read`、`wecom_mcp_send`；设置单一接收目标 `tutorial-tenant / wecom-mcp-tutorial`。未修改模型和 Telegram 原有配置。
8. 正常停止旧 Agent 进程，应用增量迁移 12–14，启动新版本。数据库、Redis、模型服务和隧道未停止。
9. 经本地 Admin API 创建 `wecom-mcp-tutorial`，指向原 `tutorial-app`。配置仅 1 个群、1 个人类成员，`fingerprint-v1`，首次起点 `2026-09-06T08:54:05Z`（北京时间 16:54:05），没有补拉此前历史。

临时会话/消息原始响应在配置成功后清理，不留原文副本；Binding 配置及回退备份按用途保留在私有目录，单次发送尝试指纹也保留。未删除企业微信消息。

## 首次启用时检查

- `/readyz` 返回 ready；原 Telegram 公网 `/healthz` 返回 ok。
- PostgreSQL schema 版本为 14。
- MCP Binding 为 active/version 1，Telegram 原 Binding 仍为 active/version 2。
- MCP 检查点从版本 4 前进到 10，已处理时间推进至 `2026-09-06T08:57:14Z`，证明真实轮询已运行。
- Binding 创建后没有 `channel_poll_failed` 审计。启动到创建 Binding 之间可能出现目标尚不存在的预检查失败，不是越权读取。
- 当时尚无该 MCP Binding 的新出站尝试，未主动多发测试回执；等待用户从测试群发送新消息。

## 真实自动回复：已通过

用户发送新消息并确认“收到了回复”后，只核对该测试绑定的运行元数据，没有读取聊天正文。实际记录为：

```text
request_id: req_c663066b425ff13dd2c66bcf2b813de7
trace_id: 3a18c087443d5e71d295fc3d111c0da1
revision_id: tutorial-approval-v1
agent_name: approval-demo-agent
model (verified in Tempo): glm-5.3-flash
inbound_status / run_status / outbound_status: processed / completed / sent
delivery_attempts: 1
channel_delivery_attempt.status: sent
prompt_tokens / completion_tokens (AgentRun): 485 / 72
received_at: 2026-09-06 17:02:25.219720 +08:00
sent_at: 2026-09-06 17:02:33.543281 +08:00
```

从平台入站到发送约 8.3 秒，不含此前的轮询等待。入站接受、执行完成、发送成功三条审计使用同一个 trace ID；该消息指纹只有一条 seen 记录。没有伪造 ProviderMessageID，上游仍未提供这个字段。

从本地 Tempo 按该 trace ID 实际读取到 17 个 span：只有一个根、无缺失 parent、无环、无错误 span，包含 `wecom_mcp.poll`、`wecom_mcp.read`、`gateway.accept`、`queue.publish`、`worker.agent.run`、框架 `invoke_agent`/`chat`、Session 读取/事件写入、`reply.send` 和 `wecom_mcp.send`。没有超出元数据白名单的 span 属性，也没有事件正文。

因此已完成“真实企业微信群消息 → 平台队列 → 真实模型/Runner → Session → 机器人自动回复”的开发环境联调，区别于之前的固定文本发送探针。这次没有工具调用，不代替真实工具审批、媒体、所有重试场景或多群/多租户压力验证；trace 中包含后台工作，不能把模型 span 数直接当作前台费用。

本项无需用户重复发送。服务继续按既定单群/单成员范围运行，仍不支持媒体格式和无损历史补拉。

## 回退边界

可先通过 Admin 禁用 MCP Binding，停止后续读取和发送；单纯清空目标列表不取消已入队任务。当前是手动启动，没有设置开机自启。`./stop.sh` 停止 Agent，`./start-real.sh` 使用保存好的 `.env` 再启动。

旧程序与配置可用于进程回退，新增数据库表保留即可；不要直接恢复整个数据库覆盖现有数据。数据库备份是灾难恢复保障，若真需恢复应先核对后续写入。当前无自动补拉、未知发送重发或检查点重置 API。
