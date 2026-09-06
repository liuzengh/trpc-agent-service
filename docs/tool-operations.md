# 工具业务幂等、结果查询与对账

## 已实现的链路

`tool_execution` 记录一次 Runner 内的工具调用；`tool_operation` 记录一次业务操作。两者不是同一个幂等键：模型重试可能换 `tool_call_id`，用户也可能在新会话再次提交同一业务。

```text
工具白名单与参数绑定审批
→ tool_execution 预留
→ tenant + app + user + tool + business_key 派生 operation_id
→ 检查业务输入哈希 → 关联执行日志
→ 幂等业务后端 → 保存业务事实 → 更新关联执行日志
```

本轮提供可运行的 `create_work_item`：在平台本地创建工作项。PostgreSQL 模式实际写入 `work_item`，InMemory 模式用于开发与测试；它不是外部工单系统、订单或支付接口。工作项的 `operation_id` 唯一约束保证并发重复调用只有一条业务记录。

工具参数是 `business_key` 和 `title`。前者必须是用户提供的稳定业务编号，不能为绕过冲突不断生成新编号。相同租户/App/用户/工具/业务编号、相同内容复用原结果；同一个编号更换内容则拒绝。跨用户、App 或租户有不同的业务空间。新业务编号代表新业务，平台不能自动判断两段自然语言是否是同一请求。

只有 Revision 白名单声明该工具才会提供给模型，且平台强制要求审批，即使租户忘记写 `dangerous_tools`。当前已发布的 Telegram App/Revision 没有自动改变，不会突然获得这个工具。

## 不确定结果如何恢复

| 场景 | 行为 |
| --- | --- |
| 后端成功，但网络超时 | 记录 `unknown`，不能宣称已失败或已回滚 |
| 后端成功，但平台状态写入失败 | 保留可查询的 operation ID；重新查询后端事实来修复 |
| 多个 Runner/调用 ID 重复同一业务键 | 复用同一 operation ID；已成功则返回原回执 |
| 后端明确拒绝操作 | 记录 `failed`，后续不自动重新执行 |
| 查询返回不存在 | 仍为 `unknown`，不存在不证明没有延迟中的请求 |
| 后端有可靠的幂等契约 | 新的授权工具调用可携同一 operation ID 安全重试 |
| 后端没有幂等契约 | 已发起过的请求只允许查询，不盲目重发 |

`Reconcile` 永远不调用后端 `Execute`，也不接受人工填入的“成功/失败”状态。它读取后端已提交事实并修复平台记录。后台调用晚到的超时不能覆盖已经确认的成功。退出/超时后的结果写入使用最多 3 秒的清理 context，没有额外常驻 goroutine。

普通、未托管工具仍保留重复调用阻断。工具异常现在标为 `unknown`，避免把可能产生副作用的错误说成“确定失败”。支持业务幂等重放的标记来自代码注册，不由租户 JSON 开启。

## 管理接口

以下都是带 Admin Bearer 的 POST 请求，按 `tenant_id` 授权：

| 路径 | 请求字段 | 权限 |
| --- | --- | --- |
| `/admin/tool-executions/list` | `tenant_id`、`request_id` | read |
| `/admin/tool-operations/get` | `tenant_id`、`operation_id` | read |
| `/admin/tool-operations/list` | `tenant_id`，可选 `status`、`after_id`、`limit` | read |
| `/admin/tool-operations/reconcile` | 仅 `tenant_id`、`operation_id` | operate |

列表按 operation ID 分页，默认 50 条、最多 100 条。`running` 或 `unknown` 列表用于查找待核对操作。跨租户访问拒绝，auditor 只能查询。对账后仍可能返回 `status=unknown`，HTTP 200 表示查询完成，不代表业务成功。

关联记录包括输入/结果哈希和 operation ID，审计不保存原始业务编号、标题或工具参数。工作项标题属于业务数据，保存在工作项后端；生产仍需配置加密、保留与删除策略。

## 部署与验证边界

新增 migration `012_business_operations.sql`；旧迁移未修改。启动使用自动迁移时会应用新表，否则先执行独立迁移命令。开发过程中未对日常数据库应用迁移、修改 Bot Binding 或重启日常服务。

自动测试覆盖真实 Runner 审批前不执行、审批后工作项创建、不同请求 ID 重放、16 个并发调用、输入冲突、租户/用户隔离、响应丢失、平台写失败后重建服务与对账、非幂等后端禁止盲重试、Admin RBAC 和日志不包含业务正文。Memory 与 PostgreSQL 运行同一业务契约。

PostgreSQL 测试只创建随机的测试专用 schema，应用迁移并在结束后删除该 schema；不迁移或清空日常表。命令：

```bash
go test -race ./...
TEST_POSTGRES_URL='postgres://trpc_agent:trpc_agent_dev@127.0.0.1:5432/trpc_agent?sslmode=disable' \
  go test ./trpcservice/toolexec -run 'TestPostgres(BusinessOperations|ToolJournal)Integration' -count=1
```

外部业务系统需要实现 `OperationProvider`。必须核实其幂等键作用域、保留时间和查询一致性，不能仅把 `Idempotent()` 改成 true 就算接入完成。没有后端业务事实的历史 `tool_execution` 不会被自动补成成功；这类记录仍需人工查业务系统。
