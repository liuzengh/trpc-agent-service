# current_time 真实模型预检与 Sender 重试测试

日期：2026-09-06。本文区分开发环境真实模型预检、模拟 Telegram 自动测试和真实 Telegram 工具验收。

## 环境与发布

- 正在运行的 Agent 日志标识：`provider=openai name=glm-5.3-flash stream=false`。
- 模型通过本机 workbuddy2api 接入；控制面及 Tool Journal 使用 PostgreSQL，Session 使用 Redis。
- 通过已开启的本地 Admin API 创建 `tutorial-current-time-v1`，并将 `tutorial-app` 从 version 1 发布到 version 2。
- 模型来源为 `startup_env`，仅允许 `current_time`，每轮最多 2 次工具调用、最长 60 秒。
- 原 `tutorial-revision-1` 保留；Telegram Binding 仍为 version 2 / active，原有 conversation pin 未改写。没有修改 `.env` 或重启现有服务。

## 真实模型 HTTP 预检：通过

经本地 `/chat` 发送“调用 current_time，返回 UTC 和北京时间”。本次使用独立的 `tool-preflight` 用户和测试 Session，没有读取用户原有会话。

实际返回：

```text
HTTP status: 200
request_id: 7d9861b1-d48b-4966-8f87-8d7143468929
revision_id: tutorial-current-time-v1
agent_name: current-time-agent
event_count: 14
replayed: false
UTC: 2026-09-06 03:32:28
北京时间: 2026-09-06 11:32:28
```

只读查询 PostgreSQL 确认，同一 `request_id` 下有且仅有 1 条 `tool_execution`，工具为 `current_time`、状态为 `succeeded`，并有 `tool_succeeded` 审计。由此确认不是仅凭模型回答文字判断工具成功。

随后重发完全相同的 `message_id` 和正文：HTTP 返回相同 request/reply，`replayed=true`，Tool Journal 数量仍为 1。该检查验证本次同步 `/chat` 幂等回放没有重复执行工具，不代表所有工具重试场景都具备 exactly-once 保证。

本次工具审计的 `trace_id` 为空，使用 `request_id` 核对；未将其记为完整 OTel trace 验收。此入口不经过 Telegram Callback、Inbox 队列和 Reply Sender。

## Sender 重试自动测试：通过

```bash
go test ./trpcservice/reply -run TestSenderTelegramRetryLifecycle -count=1 -v
go test ./...
go test -race ./...
go vet ./...
./lint.sh
```

新增测试使用本地 HTTP 模拟服务、真实 Telegram Adapter/Sender 和 MemoryJournal，未访问真实 Telegram：

- 首次 `sendMessage` 返回 429、`retry_after=1`，落为 pending；即使默认 RetryDelay 为 1 小时，也按服务端给出的 1 秒调度。
- 到期前，另一 Sender 实例不能认领并发送这条回复。
- 到期后由另一实例再次发送；成功时写入回执 `88`，状态变为 sent。
- request/outbound ID 保持一致，attempt_count 从 1 增加到 2；审计顺序为 reply_failed → reply_sent。
- 连续两次 429 达到本测试的 MaxAttempts=2 后进入 dead；403 首次失败即进入 dead。

这是内存状态层的组件链路测试，不是 PostgreSQL 持久化恢复、真实 Telegram 429 或跨进程 Sender 验收。全量 Go 测试中需要显式配置外部依赖的集成测试，本次没有另外启用。

## Telegram 群策略与真实工具验收：通过

用户在本次工具准备之前反馈群策略测试成功。其 Binding v2 的白名单、require_mention 和忽略 Bot 配置由前一轮本地 Admin 更新确认；这是用户反馈的开发环境复验，不扩展为所有 Telegram 边界情况均已通过。

用户在新 Topic 发消息并收到回复后，通过 PostgreSQL 只读查询核对以下记录：

```text
request_id: req_4ce897779fe860d03a459aa85402bd43
接收时间（北京时间）: 2026-09-06 11:37:01.843098
发送时间（北京时间）: 2026-09-06 11:37:16.716434
revision_id / pinned_revision_id: tutorial-current-time-v1
agent_name: current-time-agent
turn_seq: 1
inbound_status: processed
tool_name / status / count: current_time / succeeded / 1
run_status: completed
outbound_status / attempt_count: sent / 1
provider_message_id: 27
```

同一 request 下的审计顺序为 `inbound_accepted → tool_allow → tool_succeeded → run_completed → reply_sent`，无错误记录。这次验证覆盖了真实 Telegram 入站、模型请求工具、权限检查、Go 函数执行、持久化审计和 Telegram 出站，耗时约 15 秒。

该请求的 `trace_id` 仍为空，以 `request_id` 核对，不代表完整分布式 trace 已验收。危险工具审批、真实 Telegram 429 和长期稳定性测试也不在本次通过范围内。复验步骤见[工具调用上手说明](../current-time-tool-walkthrough.md)。
