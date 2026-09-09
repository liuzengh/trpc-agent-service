# Tool 与 Policy Runtime

## 1. 当前能力

已实现的最小纵向链路为：

```text
Revision.ToolRefs / PolicyRefs
-> 静态 Registry 解析与授权
-> LLMAgent.WithTools
-> 模型发出 tool_calls
-> Function Tool 执行
-> tool result 返回模型
-> 最终回答经 OpenAI-compatible SSE 返回
```

当前内置两个无副作用、确定性的 Function Tool：

| Tool Ref / Function Name | 输入 | 输出 |
| --- | --- | --- |
| `builtin_echo` | `{"text":"..."}` | 原样返回 `text` |
| `builtin_add` | `{"a":2,"b":3}` | 返回 `{"sum":5}`，整数溢出时失败 |

两者都由 `builtin.safe-tools` 策略授权。Tool 名同时是发给模型的 function name，因此在 Registry 构造时强制满足 OpenAI-compatible 的常见命名约束：只允许字母、数字、下划线和连字符，长度不超过 64。

## 2. Revision 解析语义

Revision 仍然是唯一的 Runtime 配置快照。`ToolRefs` 决定请求装配哪些工具，`PolicyRefs` 只允许进一步收紧工具集合：

- 没有 `ToolRefs` 的历史 Revision 不装配 Tool callback，行为保持不变。
- 有 `ToolRefs` 时至少需要一个 `PolicyRef`。
- 未知 Tool、未知 Policy、重复引用或未被任一指定策略允许的 Tool 都使 Runtime 构建失败。
- 多个 Policy 取交集；工具必须被每一个 Policy 允许。增加 Policy 只能收紧权限，不能扩大权限。
- 解析发生在模型、Runner 和协议 Adapter 构造之前。无效 Revision 不解析模型凭据，也不产生半成品 Runtime。
- 每个 Runtime 通过 Factory 获得独立 Tool 实例，不跨 `(tenant, app, revision)` 共享可变实例。

创建 Revision 时可加入：

```json
{
  "config": {
    "agent_name": "calculator",
    "instruction": "Use builtin_add when arithmetic is needed.",
    "model": {
      "provider": "openai-compatible",
      "name": "your-model",
      "base_url": "https://provider.example/v1",
      "secret_ref": "env:TEAM_MODEL_API_KEY"
    },
    "tool_refs": ["builtin_add", "builtin_echo"],
    "policy_refs": ["builtin.safe-tools"]
  }
}
```

`ToolRefs` 和 `PolicyRefs` 已包含在 Revision `config_digest` 中；发布后不能修改，回滚和 Session Revision Pin 也会恢复同一套 Tool/Policy 配置。

## 3. 执行与审计

Runtime 使用 tRPC-Agent-Go 的 `llmagent.WithTools`、`WithToolCallbacks` 和 `WithMaxToolIterations`。单个 Run 最多执行 4 轮包含 tool calls 的模型响应；第 5 轮在执行工具前结束，避免错误模型无限循环并长期持有 Session Run Lease。

每次 Tool 调用生成 before/after 两条结构化审计事件。允许字段只有：

```text
tenant_id, app_id, principal_id, session_id, revision_id,
tool, tool_call_id, phase, success, duration_ms, scope_valid
```

租户、应用、主体、Session 和 Revision 只从可信 `identity.RunContext` 获取。审计结构不包含 arguments、result 或 error 文本；sink panic 被隔离，不会把日志故障变成 Tool 故障。开始时间通过每个 Tool 调用自己的 `context.Context` 传到 after callback，同名并发调用不会互相覆盖。

### 3.1 生产失败策略

以下分类由 Tool 包装层和策略执行器决定，尚未实现完整分类器。模型不能自行把拒绝或未知结果改成可重试，所有重试和参数修正共享当前 Run 的 deadline、工具轮数及调用预算。

| 失败类别 | 下一动作与用户结果 |
| --- | --- |
| 参数校验失败，尚未执行 | 返回脱敏的字段错误；策略允许时最多让模型修正一次，仍失败则结束该操作 |
| 身份、Policy 或审批拒绝 | 不执行，拒绝该操作并结束本轮；不允许模型换措辞或重复调用绕过拒绝 |
| 已确认的临时依赖失败 | 仅在可证明未发生副作用，或业务系统支持稳定操作键去重时，在剩余 deadline 内最多重试一次；复用同一业务操作 ID |
| 必需 Tool 已知失败或重试耗尽 | 默认结束 Run 并返回明确失败；不能生成成功结果或虚构业务状态 |
| 显式标记为可选的 Tool 已知失败 | 策略允许时使用已有可信结果生成降级回复，明确缺失信息；不把可选策略套到必需操作 |
| 超时或副作用结果未知 | 停止执行并记录未知，进入结果查询或人工对账；不自动重试 Tool 或重跑 Agent |

审批不跨人工等待持有 Session 租约，具体消费规则见[生产治理设计](security-and-governance.md#112-危险操作审批)。当前内置 Tool、循环上限和审计回调的实现边界仍以下文为准。

## 4. 验证

默认测试完全离线：本地 `httptest` OpenAI-compatible 上游在第一轮返回两个 `tool_calls`，第二轮检查框架回传的 assistant tool calls 和 `role=tool` 结果，再返回最终文本。测试从真实 OpenAI HTTP Adapter 的 `stream:true` SSE 路径断言最终响应，因此不是直接调用函数的伪链路。

```bash
go test -race -count=1 ./trpcservice/tool ./trpcservice/agent
go test -race -count=1 ./...
```

关键证据：

- 第二次模型请求包含 `builtin_add` 算出的 `sum=5` 和 `builtin_echo` 返回的 `text=pong`，并按首轮 call ID 关联。
- 未授权配置全部 fail closed，且 Tool/Policy 校验先于模型与 SecretRef 构造。
- 无 ToolRefs 时，上游请求不出现 `tools` 字段。
- 同一轮并发调用两个 Tool 时，4 条 before/after 审计事件完整配对且不含参数、结果或错误明文。
- 永远返回 `tool_calls` 的上游只能执行 4 轮工具，第 5 次模型响应在工具执行前终止。

## 5. 明确限制

- `PolicyRefs` 由 Revision 选择，但**必须先被该租户的 entitlement 授权**：授权表来自 Security Manifest 的 `tenant_entitlements.allowed_policy_refs`，在 Admin 创建、Admin 发布和 Runtime 构建三处由同一个 authorizer 判定，未授权一律返回同一个 `not_entitled`，不区分策略是否真的注册过。规则见[身份、权限与密钥治理](security-and-governance.md#6-租户-entitlement)。默认 demo profile 只授权 `demo` 租户使用 `builtin.safe-tools`。仍然没有的是**按 Tool 粒度**的租户授权：entitlement 的单位是 Policy，一个租户被授权某个 Policy 就等于被授权该 Policy 里的全部 Tool。
- 4 轮上限不是 Run 总超时，也不限制一轮返回的并行 Tool 数量。慢模型、慢 Tool 和单轮大量 Tool 仍可能长期占用 Session Lease；后续需要 Run deadline、单轮 Tool 数和并发预算。
- 审计当前写结构化 `slog`，不是持久化、不可篡改的 Audit Store，也没有查询、保留期和访问控制。
- 尚未实现 MCP、动态插件市场、危险操作审批、Tool Secret 注入、业务幂等键和沙箱。
- tRPC-Agent-Go v1.11.2 在所有 after callback 都返回 `nil` 时会把原 Tool 输出提升为 `CustomResult`，`args == nil` 时该路径还会解引用空指针。当前 observer callback 返回空的 `AfterToolResult`，避免进入这条上游路径；这不是对上游语义的修复。
