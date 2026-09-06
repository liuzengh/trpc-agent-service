# Telegram 工具审批：先确认，再执行

这一阶段验证原始需求中的“危险工具二次确认”。使用已有的 `dangerous_demo`：它只返回演示结果，不会删文件、执行命令或操作外部系统。工具名称中的 dangerous 表示需要测试审批策略，不代表它真的有破坏能力。

## 1. 本次准备的版本

配置模板在 [`examples/approval-revision.json`](../examples/approval-revision.json)：

| 配置 | 值 |
| --- | --- |
| Tenant / App | `tutorial-tenant` / `tutorial-app` |
| Revision | `tutorial-approval-v1`，revision_no 为 3 |
| Agent | `approval-demo-agent` |
| 模型 | 复用服务启动时 `.env` 的真实模型 |
| 唯一允许的工具 | `dangerous_demo` |
| 必须审批的工具 | `dangerous_demo` |
| 本轮调用上限 / 运行时限 | 1 次 / 60 秒 |
| 审批有效期 | 默认 15 分钟 |

审批版本与之前的时间查询版本是两个不可变 Revision。发布只改变 App 的 stable 指针，旧 Topic 仍锁定原版本；不要清空历史数据来切换版本。Telegram Binding、白名单和密钥引用无需更改。

2026-09-06 本地已经发布此版本，App version 为 3，Agent 已重启并通过就绪检查。你现在可以直接按下一节测试，不需要再修改 `.env` 或更新 Binding。

审批需要 Inbox/Agent Run 等持久化记录，本节使用 Telegram 异步入口。不要用同步 `/chat` 替代这项验收。

## 2. 先测试“批准”

保持模型、Agent、PostgreSQL、Redis 和 Tunnel 运行，在白名单测试群里新建一个 Topic，例如“审批测试-0906”。发送：

```text
@trpc_agent_test_bot 请调用 dangerous_demo，参数 action=approval-demo。我要测试审批流程。
```

正常情况下 Bot 会说明需要确认，并附上平台生成的命令，形如：

```text
批准 apr_实际的32位编号
拒绝 apr_实际的32位编号
```

此时工具尚未执行。**在 Telegram 中使用“回复”操作，回复 Bot 这条审批提示**，正文只粘贴它提供的完整批准命令：

```text
批准 apr_实际的32位编号
```

不要照抄示例编号，也不要在命令前加 `@bot`、`/ask` 或其他文字。当前解析器要求整条消息严格为“批准/拒绝 + 审批编号”。群策略要求消息指向 Bot，使用 Telegram 的“回复”操作可以满足这个条件，不需要在正文额外 @。

正确的结果是：审批单变为 `approved`，平台回复“已批准，执行任务已提交”，随后根据 Tool Journal 返回独立的执行结果。这两条都是平台消息，不是让模型猜测状态。批准不等于执行成功；正常演示应有 1 次成功执行记录。要在 15 分钟有效期内操作。

## 3. 再测试“拒绝”和重复确认

批准和拒绝不能用同一张已处理的审批单互相覆盖。

1. 在相同新 Topic 再发一次“请调用 dangerous_demo，参数 action=approval-demo”，获得一张新审批单。
2. 使用 Telegram 的“回复”操作，向新的提示发送 `拒绝 新审批编号`。
3. 应收到以“平台确认”开头的拒绝回执；拒绝不会创建恢复任务，也不调用模型或工具。
4. 再次回复第一张审批单的批准命令。可以不产生新回复，但不应多出一个恢复任务或工具执行。

另一用户、另一群、另一 Topic 的审批，以及已过期、不存在或反向修改的审批，会记入 `approval_rejected` 审计并返回平台错误提示，不进入正常聊天或工具执行。数据库/审计暂时不可用仍返回错误，让 IM 平台重试。

可以故意发送旧格式 `拒绝请回复：拒绝 实际审批编号`：现在应收到“本条消息没有更改审批状态”的格式提示，审批单仍为 pending，模型不会被调用；随后发送正确命令才能改变状态。包含审批编号或被识别为审批/取消意图的非规范消息，只会得到格式帮助，不会按模型的理解自动批准或拒绝。

跨身份、跨会话、过期和重放的自动测试已经覆盖，你不必为了首次上手再注册一个 Telegram 账号或特意等待 15 分钟。

## 4. 如何核对，不依赖模型的说法

在仓库目录执行只读查询：

```bash
docker compose exec -T postgres psql -U trpc_agent -d trpc_agent -X -c "
  SELECT a.created_at, a.approval_id, a.status AS approval_status,
         a.tool_name, a.request_id AS original_request_id,
         a.expires_at, a.resumed_at,
         (SELECT count(*) FROM tool_execution t
          WHERE t.tenant_id = a.tenant_id AND t.request_id = a.request_id)
           AS original_tool_records,
         i.request_id AS continuation_request_id,
         r.status AS continuation_status,
         (SELECT count(*) FROM tool_execution t
          WHERE t.tenant_id = a.tenant_id AND t.request_id = i.request_id
            AND t.status = 'succeeded') AS succeeded_tools,
         o.status AS execution_reply_status,
         receipt.status AS platform_receipt_status
  FROM tool_approval a
  LEFT JOIN inbound_message i
    ON i.tenant_id = a.tenant_id
    AND i.channel_binding_id = a.channel_binding_id
    AND i.external_message_id = a.decision_message_id
  LEFT JOIN agent_run r ON r.request_id = i.request_id
  LEFT JOIN outbound_message o ON o.request_id = i.request_id
  LEFT JOIN LATERAL (
    SELECT l.details->>'receipt_request_id' AS request_id
    FROM audit_log l
    WHERE l.tenant_id = a.tenant_id
      AND l.details->>'approval_id' = a.approval_id
      AND l.decision IN ('approval_approved', 'approval_denied')
      AND l.details->>'receipt_request_id' IS NOT NULL
    ORDER BY l.occurred_at DESC LIMIT 1
  ) feedback ON true
  LEFT JOIN outbound_message receipt ON receipt.request_id = feedback.request_id
  WHERE a.tenant_id = 'tutorial-tenant'
    AND a.channel_binding_id = 'telegram-tutorial-binding'
    AND a.revision_id = 'tutorial-approval-v1'
  ORDER BY a.created_at DESC LIMIT 10;
"
```

对本次测试版本，预期如下：

| 阶段 | approval_status | 原请求工具记录 | 恢复请求工具成功次数 | 恢复请求 / 执行回复 | 平台回执 |
| --- | --- | --- | --- | --- | --- |
| 等待确认 | pending | 0 | 0 | 尚不存在 | 尚无决定回执 |
| 批准后完成 | approved | 0 | 1 | completed / sent | sent |
| 拒绝后完成 | denied | 0 | 0 | 不创建恢复任务 | sent |

审批状态一旦变为 approved，并不代表执行已经成功，必须继续核对恢复请求和 Tool Journal。审计里的 `receipt_request_id` 关联平台回执，批准时另有 `continuation_request_id` 关联后续工具执行。平台回执复用持久化 Inbox/Outbound，`agent_run.agent_name=platform-control`、token/event 数为 0，没有 queue_outbox 任务；这里的 completed 表示回复已持久化，不代表运行了 LLM。

修正前的拒绝历史可能仍有关联的恢复任务，这是旧版数据，不能以新表格反推或删除它们。

查看近期审批拒绝原因时，只查询状态字段，不输出原始聊天或工具参数：

```bash
docker compose exec -T postgres psql -U trpc_agent -d trpc_agent -X -c "
  SELECT occurred_at, decision, error_type,
         details->>'approval_id' AS approval_id,
         details->>'continuation_request_id' AS continuation_request_id,
         details->>'receipt_request_id' AS receipt_request_id,
         details->>'duplicate' AS duplicate
  FROM audit_log
  WHERE tenant_id = 'tutorial-tenant'
    AND channel_binding_id = 'telegram-tutorial-binding'
    AND decision LIKE 'approval_%'
  ORDER BY occurred_at DESC LIMIT 20;
"
```

## 5. 这条链路在代码中怎样运行

1. **模型提出工具调用。** LLMAgent 收到结构化 Tool Call 后，平台的 PermissionPolicy 检查白名单、调用预算和批准凭据。`dangerous_demo` 尚未批准时返回 `ask`，并写入 `tool_approval`。
2. **本轮结束，等待用户。** Worker 查询同一租户、Binding、用户和 Session 的待批记录，用平台提示替换模型正文。即使模型声称“已取消”，只要还有待批项，也会显示真实的等待状态（最多列出 10 条）。Runner 正常结束，不会为等待批准长期占用 goroutine。
3. **回调优先识别审批命令。** Telegram Adapter 先验签并过滤群消息；Gateway 计算可信路由、用户和 Session。严格命令才可改变状态；疑似审批但格式错误的消息只创建平台格式回执，不交给模型。
4. **校验身份并保存决策。** 必须匹配同一租户、Binding、用户和 Session。PostgreSQL 使用行锁串行更新审批，决定批准还是拒绝；重复的同方向决策复用第一次的 decision_message_id，反向修改会冲突。
5. **回执与执行分开。** 拒绝后直接持久化平台回执，不再生成 Agent 任务。批准后保留原恢复任务标识以兼容旧数据，另用稳定 ID 写入批准回执。批准权限绑定工具名称和参数哈希；普通消息中的“我已批准”不构成权限，参数变化必须重新审批。
6. **根据执行记录报告结果。** 权限通过后 `toolexec.StartAuthorized` 才预留执行，AfterTool 写入结果。审批恢复任务的最终回复由 Worker 读取 Tool Journal 生成：成功、失败、没有执行记录或结果未确认，不再直接发送模型原话。业务结果正文的结构化展示仍需后续业务工具适配。

关键代码是 [`approval/service.go`](../trpcservice/approval/service.go)、[`approval/postgres.go`](../trpcservice/approval/postgres.go)、[`agent/compiler.go`](../trpcservice/agent/compiler.go) 和 [`toolexec/callbacks.go`](../trpcservice/toolexec/callbacks.go)。

特别注意：当前固定的 tRPC-Agent-Go v1.11.2 中，BeforeTool 发生在权限检查之前，且 `ask` / `deny` 不进入 AfterTool。本次修正了原先在 BeforeTool 创建 Journal 的做法，避免等待审批时留下一条虚假的 running 记录。历史数据没有被自动改写。

## 6. 已验证与仍待验证的边界

- 自动测试：真实框架执行循环 + 模拟模型/Telegram API，覆盖请求、审批、恢复、Go 工具、Journal、审计和 Sender；另有跨身份/会话、过期、重复和决策冲突测试。
- PostgreSQL：同一组审批约束测试使用临时隔离 schema，包含行锁更新及 decision_message_id 唯一约束。
- 真实模型预检：使用 `.env` 模型、隔离 PostgreSQL、InMemory Session/Queue 和本地 TestAdapter；批准、拒绝和重复决策均通过，没有向 Telegram 发消息。
- 真实 Telegram 的合法批准和拒绝命令已完成基础验收，期间发现非规范拒绝被模型误报取消的问题。修正后的格式拦截和新审批单的确定性拒绝回执已于 2026-09-06 复验通过；新版批准后的独立执行结果正文仍待复验。完整 OTel trace、真实业务副作用、节点崩溃后的业务对账不在本次通过范围内。

重复操作仍可能不产生新回复：当前复用第一次的回执。此项用户体验保留为待优化项，不等于服务停止。

相关自动测试可以重复执行：

```bash
go test ./trpcservice/approval ./trpcservice/toolexec -count=1
TEST_POSTGRES_URL='postgres://trpc_agent:trpc_agent_dev@127.0.0.1:5432/trpc_agent?sslmode=disable' \
  go test ./trpcservice/approval -run TestPostgresDecisionContractIntegration -count=1 -v
```

真实模型预检证据见[2026-09-06 审批验证记录](validation/approval-2026-09-06.md)。幂等测试通过不等于任意外部工具具备 exactly-once：真实业务工具仍需要自己的幂等键、可查询结果和异常对账机制。

## 7. 回滚与已有会话

发布后本机 `data/approval-rollback.json` 保存了指回 `tutorial-current-time-v1` 的 Admin publish 请求。使用前通过 `/admin/apps/get` 核实当前 App version，再更新 `expected_version`；需要认证，不能直接写数据库绕过审计。

回滚只改变新会话的路由，已经锁定审批版本的 Topic 仍用审批版本。尚未过期的审批单仍按原 Revision 恢复；stable 指针回滚不等于撤销已授予的权限。
