# 从聊天走到真实工具调用

这一阶段验证的是：真实模型能否通过 tRPC-Agent-Go 调用平台允许的 Go 函数，并留下可核对的执行记录。先使用只读的 `current_time`，不开放文件操作、命令执行或有外部副作用的工具。

## 1. 已经准备了什么

2026-09-06，本地控制面通过 Admin API 创建并发布了如下配置。模板在 [`examples/current-time-revision.json`](../examples/current-time-revision.json)，不包含凭据。

| 配置 | 值 |
| --- | --- |
| Tenant / App | `tutorial-tenant` / `tutorial-app` |
| 新 Revision | `tutorial-current-time-v1`，revision_no 为 2 |
| Agent 名称 | `current-time-agent` |
| 模型 | `source=startup_env`，复用服务启动时从 `.env` 读取的模型 |
| 工具白名单 | 只有 `current_time` |
| 单轮工具调用上限 | 2 次 |
| 单轮运行时限 | 60 秒 |

App 的 stable 指针从 `tutorial-revision-1` 切到新版本，App version 从 1 增加到 2。原 Revision 没有修改或删除；Telegram Binding 仍为 version 2，群白名单、mention 策略、Webhook 和密钥引用均未改变。

这次只更新了数据库中的版本配置，不需要修改 `.env`、重新编译或重启正在运行的 Agent。以后更换 `.env` 中的模型配置仍需要重启。

**影响范围是这个 App 的新建异步会话，以及后续使用 stable 路由的 `/chat` 请求。** Telegram 已有会话在首次接收消息时锁定了旧 Revision，不会随 stable 指针自动升级。HTTP `/chat` 是同步调试入口，每次解析当前路由，不使用 Inbox 的 conversation pin；不要用它的行为推断 Telegram 老会话会切换版本。

## 2. 你现在怎么测

保持模型服务、PostgreSQL、Redis、Agent 和 Cloudflare Tunnel 运行。在已经加入白名单的测试群中，**新建一个从未和 Bot 对话过的 Topic**，例如“工具测试-0906”，然后发送：

```text
@trpc_agent_test_bot 请调用 current_time 工具查询现在的时间，同时告诉我 UTC 时间和北京时间。
```

没有收到回复时，也可以用已有命令入口发送：

```text
/ask@trpc_agent_test_bot 请调用 current_time 工具查询现在的时间，同时告诉我 UTC 时间和北京时间。
```

回复应包含两个时区的时间，北京时间比 UTC 快 8 小时。具体措辞由真实模型决定。`current_time` 读取的是 **Agent Worker 所在机器的时钟**，不是查询网络授时服务；机器时间错误也会反映在结果中。

再发送一次新的消息询问当前时间，应产生新的 request 和工具执行。不要复用之前的私聊或旧 Topic；清空 Telegram 聊天界面、再发 `/start`，都不等于删除服务端会话或解除版本锁定。

可以补充问一句“你能调用哪些工具？”。但模型的自我描述或拒绝文本不是权限校验的证据；权限由 Revision 白名单和运行时策略约束，执行记录才是核对依据。

## 3. 怎样确认真的执行了工具

在仓库目录执行以下只读查询。它只查看最近的 Telegram 请求，不输出聊天正文、模型凭据或工具参数。

```bash
docker compose exec -T postgres psql -U trpc_agent -d trpc_agent -X -c "
  WITH recent AS (
    SELECT r.request_id, r.tenant_id, r.revision_id, r.status, r.trace_id,
           i.received_at
    FROM inbound_message i
    JOIN agent_run r ON r.request_id = i.request_id
    WHERE i.tenant_id = 'tutorial-tenant'
      AND i.channel_binding_id = 'telegram-tutorial-binding'
    ORDER BY i.received_at DESC
    LIMIT 10
  )
  SELECT r.received_at, r.request_id, r.revision_id,
         r.status AS run_status, o.status AS outbound_status,
         t.tool_name, t.status AS tool_status,
         a.decision AS tool_audit,
         COALESCE(NULLIF(a.trace_id, ''), r.trace_id) AS trace_id
  FROM recent r
  LEFT JOIN outbound_message o ON o.request_id = r.request_id
  LEFT JOIN tool_execution t ON t.request_id = r.request_id
    AND t.tenant_id = r.tenant_id
  LEFT JOIN audit_log a ON a.request_id = r.request_id
    AND a.tenant_id = r.tenant_id
    AND a.decision = 'tool_succeeded'
    AND a.details->>'tool_call_id' = t.tool_call_id
  ORDER BY r.received_at DESC;
"
```

一次完整成功的 Telegram 时间查询应同时出现：

```text
revision_id     = tutorial-current-time-v1
run_status      = completed
outbound_status = sent
tool_name       = current_time
tool_status     = succeeded
tool_audit      = tool_succeeded
```

同一个 `request_id` 把 Agent Run、Tool Journal、工具审计和回复串起来。没有启用或采样到 tracing 时，`trace_id` 可以为空，但不能因此声称已经验证完整 trace。

常见结果的含义：

- `revision_id` 还是旧值：当前是已有会话；换一个新 Topic 测试。
- 新版本有回复，但 `tool_name` 为空：该轮没有可核实的工具执行，不能仅凭时间文本判定成功。
- 工具成功，但 Run 失败：检查工具之后的模型调用、运行时限等；工具成功不代表整轮成功。
- Run 完成，但 Outbound 为 `pending`、`sending` 或 `dead`：检查 Sender 和 Telegram 出站链路。
- SQL 中根本没有新请求：先检查回调、群过滤策略和本地服务状态，参见[手动运行手册](telegram-manual-runbook.md)。

## 4. 一条消息在代码里怎样运行

这里的工具不是“让模型直接访问服务器”，而是由框架解析模型返回的调用请求，再执行预先注册的 Go 函数。

1. **收消息并确定版本。** Telegram Adapter 验证回调并检查群策略，Gateway 持久化 Inbox/Outbox。首次出现的会话锁定当时解析出的 Revision，Worker 后续按这个版本执行。
2. **组装 Agent 和可用工具。** [`agent/compiler.go`](../trpcservice/agent/compiler.go) 加载不可变 Revision，通过 Tool Catalog 找到 `current_time`，交给 tRPC-Agent-Go 的 LLMAgent；模型继续使用原来的 OpenAI-compatible 接入。
3. **模型提出调用请求。** 框架把工具名称、说明和参数 schema 放入模型请求。模型应返回结构化 `tool_calls`，而不是仅写“我调用了工具”。
4. **运行前检查权限。** [`governance/policy.go`](../trpcservice/governance/policy.go) 使用框架的 ToolFilter、PermissionPolicy 和运行时限配置。白名单里没有的工具不会因为提示词要求就被授权。
5. **执行 Go 函数并记账。** [`toolexec/callbacks.go`](../trpcservice/toolexec/callbacks.go) 在工具执行前后写 Journal 和审计。实际函数在 [`tool/tool.go`](../trpcservice/tool/tool.go)，通过 `time.Now().UTC()` 生成结果。Journal 保存参数/结果哈希和状态，不保存原始参数与结果正文。
6. **将结果交回模型并回复。** tRPC-Agent-Go 将工具结果加入本轮上下文，模型生成最终文字。Runtime 消费 Event channel，Worker 完成 Run 并生成 Outbound，Sender 调用 Telegram `sendMessage`。

平台负责“哪个租户、哪个版本、能用什么、如何持久化和投递”；tRPC-Agent-Go 负责 LLMAgent、模型/工具调用循环、Runner、Session 和 Event。`current_time` 这个函数及工具注册由本项目提供。

## 5. 这次验证到哪里了

真实模型 HTTP 预检和新 Topic 中的 Telegram 工具链路均已通过，详见[2026-09-06 验证记录](validation/current-time-2026-09-06.md)。后者由用户真实发消息后查询数据库确认；不是用 `/chat` 预检替代 Telegram 验收。第 2、3 节可用于重复验证。

另外新增了 [`reply/sender_retry_test.go`](../trpcservice/reply/sender_retry_test.go)：

```bash
go test ./trpcservice/reply -run TestSenderTelegramRetryLifecycle -count=1 -v
```

测试使用真实 Sender 和 Telegram Adapter、本地模拟 HTTP API、MemoryJournal，覆盖 429 的 `retry_after` 调度、到期前不发送、另一 Sender 实例接手、成功回执、失败审计、重试耗尽和 403 永久失败。不会读取 Bot Token，也不会向真实 Telegram 制造限流。这不等于 PostgreSQL 多进程故障测试，也不证明网络超时、部分分片已发送等情况下绝不会重复回复。

## 6. 如果需要回滚

旧版本还在数据库里。通过 Admin 的 `POST /admin/apps/get` 查询当前 App version，再向 `POST /admin/revisions/publish` 提交：

```json
{
  "tenant_id": "tutorial-tenant",
  "app_id": "tutorial-app",
  "revision_id": "tutorial-revision-1",
  "expected_version": 2
}
```

`expected_version` 必须以查询结果为准，不能永远填写 2。本机 `data/current-time-rollback.json` 保存了本次发布后的回滚请求，未纳入 Git；Admin 请求仍需认证。

回滚只改变后续路由，不删除新 Revision、历史 Session 或审计。已经锁定 `tutorial-current-time-v1` 的 Telegram 会话也不会自动降级，需要新会话或另行设计会话迁移；不要直接改历史数据来模拟回滚。

模板也不是自动安装脚本。在新环境重做准备时，应先查询 App 和已有版本，选择不冲突的 `revision_id` / `revision_no`，通过 `POST /admin/revisions` 创建，再带当前 App version 发布，不能直接覆盖已有 Revision。
