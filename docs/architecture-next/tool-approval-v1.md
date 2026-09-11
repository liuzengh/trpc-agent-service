# 危险工具二次确认 V1

- 状态：已实现（代码与自动化夹具）；真实外部测试工单服务联合验收待执行。
- 日期：2026-09-10。
- 范围：管理 Web 审批；只治理 `test.ticket.status.update` 一种副作用能力。

## 1. 目标与边界

节点拥有某个工具，只说明该工具进入节点的可调用集合；它不代表模型提出的每一次具体
操作已经得到用户批准。V1 在既有 `AgentSpec -> ProfileRevision -> RuntimeManifest ->
Worker` 工具选择之后增加逐次操作确认：

```text
模型提出 test.ticket.status.update 调用
  -> Worker 固定租户、Run、Attempt、SDK Invocation、ToolCall、节点和参数
  -> PostgreSQL 持久化 PENDING 操作
  -> 租户成员在 Web 查看目标和参数摘要
  -> 当前租户 OWNER 批准、拒绝，或请求到期
  -> 同一个仍在等待的 SDK 工具调用在批准后执行
  -> Worker 持久化成功结果或 UNKNOWN
```

本版不新增通用策略中心、审批微服务、IM 卡片、邮件发送、Shell 审批、任意参数 Schema、
委托审批人或多级审批。普通工具仍按 Manifest 的节点级集合执行；只有工具资源明确声明
Capability `test.ticket.status.update` 时进入本流程。

## 2. 唯一受支持操作

V1 复用既有 MCP Resource，不新增工具 Kind。Agent requirement 与 Profile 中同名 MCP
资源在 Deployment 发布时照常完成匹配；Worker 使用 RuntimeManifest 中固定的能力决定
是否加审批回调。

请求参数必须是关闭对象，且只允许：

```json
{"ticket_id":"TEST-42","status":"resolved"}
```

- `ticket_id`：1 到 64 个字母、数字、`_` 或 `-`，首字符为字母或数字；
- `status`：`open`、`in_progress`、`resolved`、`closed` 之一；
- 未知字段、尾随 JSON、其他状态和其他 Capability 全部拒绝；
- Worker 规范化 JSON 后计算 `sha256:` 参数摘要，Web 不接收或回传明文参数，只展示目标、
  人类可读摘要和完整摘要值。

## 3. 身份与不可变绑定

一次审批操作不可变绑定：

- `TenantID`、`RunID`、`AttemptID`；
- SDK `InvocationID`、`ToolCallID`；
- `NodeID`、实际 SDK ToolName、Profile Resource 名称与 Capability；
- 目标测试工单、规范化参数及 `ArgumentsDigest`；
- 请求时间和到期时间。

`OperationID` 由上述执行身份、工具和参数摘要稳定派生；数据库另有
`(TenantID, AttemptID, InvocationID, ToolCallID)` 唯一约束。相同调用重放返回同一事实；
同一个 ToolCall 改参数会冲突，不能消费旧批准。数据库触发器禁止更新任何身份、目标、
参数、摘要或期限字段。

Web 做决定时必须提交页面所见 `expected_arguments_digest`。Control API 重新验证当前登录
用户属于同一 Tenant 且仍是活跃 OWNER，再把认证主体写入内部 mTLS 请求。Worker 以
Tenant、OperationID、状态与摘要执行 CAS。普通 MEMBER 可以查看本租户记录，不能决定；
跨租户查询和决定均不成立。

## 4. 状态机与幂等

```text
PENDING --approve--> APPROVED --等待中的调用占用--> EXECUTING --持久成功--> SUCCEEDED
   |                    |                                  `--结果不确定--> UNKNOWN
   +--reject---------> REJECTED
   +--deadline-------> EXPIRED
   +--等待上下文结束-> EXPIRED

PENDING/APPROVED/EXECUTING --所属 Worker 重启--> UNKNOWN
```

- 相同主体或其他 OWNER 重复提交**相同决定**，返回 `REPLAY`，不再次执行；
- 相反决定、摘要不一致、过期、已经执行或 UNKNOWN 均返回稳定冲突；
- `APPROVED -> EXECUTING` 是数据库 CAS，只有取得该转换的等待调用可以进入 MCP；
- 工具返回的有效 JSON 不超过 32 KiB 时保存结果摘要和结果 JSON；
- 工具返回错误、回调异常、结果无法可靠持久化时记录 `UNKNOWN`，该 Attempt 以非依赖类
  错误结束，不进入普通依赖重试；UNKNOWN 不自动解释为失败，也不自动再次调用工具。

批准只在原 tRPC-Agent-Go Runner 的 `BeforeTool` 回调中等待，批准后通过同一回调上下文
进入 `AfterTool`。因此恢复审批不重新发起模型请求，也不会重放本轮在它之前完成的工具。
等待取消会先把尚未决定的请求置为 EXPIRED，晚到的点击无法放行未来调用。

进程非正常中断时，Worker 启动按稳定 `worker_id` 只处理该身份拥有的未完成操作，将其置为
UNKNOWN，并把 Run 标记为 `TOOL_APPROVAL_RECONCILIATION_REQUIRED`。调度查询明确排除含
UNKNOWN 审批的 Run，即使旧 Attempt 租约到期也不创建新 Attempt。V1 只保留记录供人工
诊断，不提供“继续执行”操作；后续恢复能力必须从确定的工具调用续接，不能重跑整轮 Agent。
不同 Worker 身份的活跃操作不会被本实例启动流程修改。

## 5. 组件职责

| 组件 | V1 职责 | 不负责 |
| --- | --- | --- |
| Deployment | 将同名 MCP Resource、Capability 和节点级选择固定进 Manifest | 为一次具体调用作决定 |
| Worker SDK Adapter | 在 `BeforeTool` 识别能力、提出并等待操作；在 `AfterTool` 固定结果 | 信任 Prompt 中的“已批准”文本 |
| Worker PostgreSQL | 保存操作状态机、CAS、结果与重启阻断事实 | 把 UNKNOWN 猜成失败并重试 |
| Worker internal HTTP | 只接受 Control mTLS 身份的租户查询和决定 | 接受浏览器 Cookie 或 Gateway 身份 |
| Control API | 成员查询授权、OWNER 决定授权、转发认证 Actor | 读取或执行 MCP 工具 |
| Control Web | 轮询记录、展示目标/摘要、提交批准或拒绝 | 直接调用 Worker 或修改参数 |

工具外部系统仍需自己的幂等语义。V1 保证平台不会因重复点击重复占用同一批准，但网络在副
作用已经发生后断开时，平台只能记录 UNKNOWN；审批去重不能替代外部测试工单接口的幂等键。

## 6. HTTP 契约

公开 Control API：

- `GET /v1/tenants/{tenant_id}/tool-approvals?offset=0&limit=25`；
- `POST /v1/tenants/{tenant_id}/tool-approvals/{operation_id}/decision`。

决定请求：

```json
{
  "action": "approve",
  "reason": "已核对目标测试工单",
  "expected_arguments_digest": "sha256:..."
}
```

内部 Worker API 使用独立路径，并由 Control mTLS 身份调用；内部决定体额外携带经过认证的
`actor_id`。两个写端点均只接受 `application/json`、关闭对象、单一 JSON 值和有限正文。
列表响应使用 `Cache-Control: no-store`，Web 每两秒轮询并允许手动刷新。

## 7. 验收矩阵

| 案例 | 期望 |
| --- | --- |
| 同一调用相同参数重复提出 | 返回同一 Operation，不产生第二条操作 |
| 同一 ToolCall 改参数 | `APPROVAL_CONFLICT`，旧批准不可用 |
| MEMBER 查看 | 只看到本租户摘要，无决定按钮 |
| MEMBER 决定 | `TENANT_FORBIDDEN` |
| OWNER 批准后重复点击批准 | 第二次为 `REPLAY`；工具只执行一次 |
| OWNER 批准后点击拒绝 | 冲突，不改变已批准事实 |
| 未批准到期 | EXPIRED，工具不执行 |
| 工具返回成功 JSON | SUCCEEDED，并固定结果摘要 |
| 工具结果/网络状态不确定 | UNKNOWN，不自动重试 |
| Worker 在等待或执行阶段重启 | 仅本 worker_id 的操作变 UNKNOWN，Run 不重新调度 |
| Profile 有额外普通工具 | 不因 Worker 支持而获得审批能力；仍受节点选择约束 |

自动化覆盖纯参数/身份测试、SDK Before/After 回调、Control 授权与转发、Worker mTLS 路由、
真实 PostgreSQL 状态机/租户隔离/重启阻断，以及 Web OWNER/MEMBER 交互。真实外部测试工单
服务和真实模型主动选择该工具仍是独立联合验收，不由 fixture 测试替代。

## 8. 后续扩展点

出现真实需求后再分别设计：外部工具幂等键和查询式结果对账、确定调用点续接、审批策略与
审批人模型、IM 卡片签名/防转发、更多关闭参数 Schema、审计 Outbox，以及 Shell 等高风险
工具的命令级约束。扩展不得把“工具在 Manifest 中”退化为“所有操作默认批准”。
