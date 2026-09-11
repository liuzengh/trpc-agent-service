# 运行管理与审计 V1

- **实现分支**：`codex/run-audit-v1`
- **目标**：让租户成员在 Web 中检索 Run 过程与稳定业务审计事实，而不是查日志、数据库或 trace。
- **本轮页面**：`/tenants/{tenant}/runs`、`/tenants/{tenant}/runs/{run}`、`/tenants/{tenant}/audit`。

## 1. 所有权与查询路径

```text
Web ──Cookie──> Control public API ──租户成员校验──┬─> Control 配置事实投影
                                                   └─mTLS─> Worker 管理查询
                                                              └─> Execution ledger
```

Control 是管理查询入口，但不成为运行事实拥有方。Worker 只返回 Run、Attempt、Completion、
Session Head、Memory 状态、模型用量和 ReplyIntent 交接状态的脱敏投影；不返回输入文本、模型输出、
Session 正文、凭据、工具参数或工具结果。Web 不连接 Worker 数据库。

## 2. 运行状态解释

| 页面阶段 | 确定事实 |
| --- | --- |
| `QUEUED` | Worker 已持久接管 Run，尚未形成运行 Attempt。 |
| `EXECUTION` | 存在运行中或等待重试的 Attempt。 |
| `MEMORY` | Completion 已提交，但正式 Memory 应用仍为 `PENDING`。 |
| `REPLY_PUBLISH` | ReplyIntent 已创建，Worker Outbox 尚未确认发布。 |
| `REPLY_HANDOFF` | Worker 已将 ReplyIntent 交给消息总线；这不等于 Gateway 接收或 Provider 送达。 |
| `COMPLETED` | Worker 侧运行与回复交接已完成。 |
| `FAILED` | Run 或 Memory 已进入持久失败状态，页面显示稳定 reason。 |

详情按时间组合 `RUN_ACCEPTED`、Attempt 创建/开始/结束、Completion 与 Reply 交接事实。Memory
只展示当前状态；现有账本没有 Memory Finalizer 的完成时间，因此 V1 不伪造时间线事件。
Attempt 创建和 Agent 开始事件分别使用 `CREATED`、`RUNNING`，最终状态和失败原因只出现在
`ATTEMPT_ENDED` 及 Attempt 概览中，避免把后来发生的失败倒填到较早时间点。
分页使用 `accepted_at DESC, run_id DESC`；所有运行查询先限定 `tenant_id`。

模型用量带有 `usage_status`：没有持久化行时为 `UNAVAILABLE`，页面显示“未采集”而不是数字
`0`；存在部分记录时为 `PARTIAL`；成功 Run 的每个 Attempt 都有记录时才为 `COMPLETE`。
该状态只描述当前 `execution_model_usage` 账本的覆盖度，不代表 Provider 账单或完整成本。

## 3. 审计事实

Control 侧从自身持有的不可变或带执行者字段的事实生成：Agent、Profile、Deployment、Channel
的创建，以及 AgentVersion、ProfileRevision、DeploymentRevision 的发布。Worker 侧生成 Run、
Attempt、Completion 与 ReplyIntent 交接事件。Control 在完成租户授权后合并两侧结果并统一排序。
V1 的跨服务 offset 聚合限定最近 100 条，避免为了深分页而无界查询两侧数据；后续改为
游标式的多来源归并。

该列表是业务查询契约，不读取结构化日志或 trace。`traceparent` 仍用于外部观测关联，但不作为
审计事件主键，也不补造缺失的业务事实。

## 4. 当前明确缺口

1. Gateway 尚未提供 Provider Delivery Attempt/Observation 的受认证管理查询。因此本轮只能确定
   ReplyIntent 已交接，不能把它显示为 Telegram/企微已送达；后续应增加 Gateway 自有 mTLS 查询
   或由 Gateway 发布不可变审计事件，再由 Control 聚合。
2. 普通工具调用和节点级模型调用目前没有独立业务账本。页面明确显示覆盖范围，不从 trace 猜测
   “哪个工具失败”。后续应由 Worker 在 Attempt 生命周期内持久化有界、脱敏的调用事件。
3. V1 不新增审批中心和成本中心。Token 用量仅展示已持久化模型用量，不换算价格，不宣称完整成本。
4. Control 当前投影覆盖创建和发布；Draft 每次保存、凭据轮换和全部授权决策需要独立不可变审计事件
   后再进入列表，不能通过更新时间反推执行者。

## 5. 验收

- 非租户成员不能触发 Worker 查询；受限登录态不能读取运行或审计页面。
- Worker 内部路由只接受已配置 Control SPIFFE URI，Gateway 身份不能读取。
- 同一 `run_id` 在不同租户下不能越权读取。
- 列表不包含请求正文、输出正文、凭据或工具参数。
- Control 与 Worker 审计事件按时间合并后再做全局分页，不分别分页后简单拼接。
- Worker 不可用时返回稳定 `RUN_MANAGEMENT_UNAVAILABLE`，页面不展示伪造的空成功结果。
- Control 的 Worker HTTP Client 必须分别编码 URL Path 和 Query；HTTPS 回归测试检查两个列表
  路由收到原始 `/runs`、`/audit-events` 路径及独立的 `offset`、`limit` 参数。
- 翻页请求成功后才提交页面 offset；请求失败时保留上一成功页及其页码，首次失败不显示
  “暂无记录”。
