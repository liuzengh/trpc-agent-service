# P0-07 Review 修复实施方案

## 1. 方案目标

P0-07 当前保持 `in_progress`，不能标记 `closed`。本方案只针对 review 已确认的缺口补齐实现和证据，不修改 `docs/plan007v1` 原计划文件，也不重新实现 P0-01 至 P0-06。

本次修复必须最终证明：

- Runtime 具备按 `TenantContext + AgentSpec` 构造的 ProviderFactory，不再由一个 Factory 共享固定 Provider。
- Agent 配置能够传递 `ModelConfigRef`、Tool Policy 和 Guardrail 引用，Provider 选择与租户/Agent release 绑定。
- Runtime 具备平台无关的 ToolInvoker 边界和真实的框架 Tool 注册/桥接；Tool started/completed/failed 事件能够转换为平台 `RunnerEvent`。
- Provider 原始错误保留在 error chain 中，调用方能区分 provider、tool、framework、cancel、deadline 和 drain failure。
- Runner 取消会触发下游停止，事件会继续排空到 channel close，Producer 完成后才结束；超时不会留下未验证的 goroutine。
- Platform 在取消发生后不再写入 assistant Session message/event；取消审计使用独立的有界审计上下文，不能使用已取消的业务提交上下文强行写 assistant 数据。
- `MODEL_PROVIDER=runner` 不再注入 `unavailableProvider`，而是拥有真实 ProviderFactory；在本地测试中通过 OpenAI-compatible `httptest` 服务验证实际 Runner 成功路径，不依赖外部 API Key。
- 所有 review 缺失测试补齐后，才重新执行一次 P0-07D 全量回归并形成关闭报告。

## 2. P0-06 经验转化为执行规则

上一阶段困境文件的核心结论应用如下：

- 先冻结当前工作树事实。当前已有 `cmd/trpc-service/main.go`、`go.mod`、`go.sum`、`trpcservice/agent/agent.go`、`trpcservice/agent/runtime.go`、`trpcservice/agent/agent_test.go`、`trpcservice/platform/platform.go`、`trpcservice/platform/platform_test.go` 和 `cmd/trpc-service/main_test.go` 的修改，全部视为当前待审查基线；不得 `git reset`、`git checkout` 或覆盖用户改动。
- 将修复拆成可独立关闭的 R1 至 R5 子阶段。每轮只推进一个子阶段；已经通过的子阶段不因后续缺口重新打开。
- 代码存在、编译通过、普通测试通过和 `go test ./...` 通过都不能单独关闭一个行为项。`[no test files]`、skip、缺少配置和未执行的真实 Provider 测试都只能记录为 `blocked/not verified`。
- 每个子阶段最多维护一个当前阻塞项。发现新的独立问题时先归并到当前阻塞项或 backlog，不横向扩展当前实现。
- 测试按修改边界执行：先 targeted 单测，再 targeted race；P0-07D 只在 R1 至 R4 关闭后执行一次全量 test/race/vet/gofmt。
- 失败时收集完整诊断，例如请求租户、AgentApp、ConfigRef、Provider 名称、Tool 名称、事件序列、错误类别、producer completion 和 assistant 写入计数；不能只调整断言把失败变成通过。
- 每个子阶段关闭时输出变更文件、实际命令、未运行/blocked 项、已知限制和下一阶段入口。没有关闭报告就不能停止当前阶段。

## 3. 初始事实和验收矩阵

修复开始时先记录一次，不把旧会话或 review 文字替代当前工作区事实：

```bash
git status --short
git diff --stat
git diff --check
go test ./trpcservice/agent ./trpcservice/platform ./cmd/trpc-service -count=1
go test ./trpcservice/agent ./trpcservice/platform ./cmd/trpc-service -race -count=1
go vet ./trpcservice/agent ./trpcservice/platform ./cmd/trpc-service
```

不在修复第一轮重复执行全量测试。根据 review 和当前代码，初始矩阵应记录为：

| 验收项 | 当前状态 | 证据/说明 |
| --- | --- | --- |
| TenantContext、TenantID/AgentAppID/ConfigVersion 校验和权限复制 | `passed` | review 已确认；保留现有测试并做回归 |
| 框架类型不泄漏到 tenant/channels/storage/platform 公共接口 | `passed` | 边界扫描已通过；修复后重扫 |
| Echo 保留、runner/echo 显式分支、未知模式拒绝、runner 不自动回退 | `passed` | 现有 main/platform 测试；不等于 runner 可运行 |
| ProviderFactory 按租户/Agent/ConfigRef 构造 | `failed` | 当前 RuntimeDependencies 只有共享 Provider |
| ToolInvoker 和框架 Tool bridge | `failed` | 当前未注册 Tool，ToolName 不填充 |
| Provider 原始错误链和错误分类 | `failed` | 当前被转换为 APIError，原始 error 丢失 |
| 取消触发 Stop、drain 到 close、producer completion | `failed` | 当前 drain 使用 Background，未等待 producer |
| 永不关闭 channel 的 bounded timeout 和无泄漏证据 | `not verified` | 只有 drain timeout 测试，没有 producer 生命周期测试 |
| Platform 取消后的 assistant 提交保护 | `failed` | Respond 返回后无 ctx 检查/原子提交保护 |
| runner 模式真实 Provider 成功路径 | `failed` | 当前注入 `unavailableProvider` |
| 两租户并发使用不同 Provider/Agent 配置 | `not verified` | 当前 fake/provider 无状态，未暴露共享配置风险 |
| P0-07D 全量关闭 | `not verified` | 全量测试通过不能覆盖上述行为缺口 |

## 4. 子阶段划分

### R1：Provider/Tool 依赖边界和错误语义

**目标**：先稳定 Runtime 的平台公共契约和每次构造的依赖边界，不进入取消生命周期或平台提交。

**允许修改**：

- `trpcservice/agent/agent.go`
- `trpcservice/agent/runtime.go`
- 新增 `trpcservice/agent/provider.go`、`trpcservice/agent/tool.go` 或同包等价文件
- `trpcservice/agent/agent_test.go` 及同包测试
- 必要时修改 `trpcservice/platform/platform.go` 中 `AgentConfig` 的纯配置字段映射，但只有为传递现有 Agent 配置所必需时才修改

**接口与数据流固定如下**：

```go
type ProviderFactory interface {
    Build(context.Context, tenant.TenantContext, AgentSpec) (Provider, error)
}

type ProviderFactoryFunc func(context.Context, tenant.TenantContext, AgentSpec) (Provider, error)

type ToolCall struct {
    Name      string
    Arguments []byte
}

type ToolResult struct {
    Content string
    IsError bool
}

type ToolInvoker interface {
    Invoke(context.Context, tenant.TenantContext, ToolCall) (ToolResult, error)
}

type RuntimeDependencies struct {
    ProviderFactory ProviderFactory
    ToolInvoker     ToolInvoker
    DrainTimeout    time.Duration
    StopTimeout     time.Duration
}
```

`Provider` 可以继续作为单次 Runtime 内部的纯数据接口，但不能再作为 `RuntimeDependencies` 的生产主入口。若为保持已有 Fake 测试兼容需要暂时保留 `Provider` 字段，只允许通过明确命名的 `StaticProviderFactory` 测试适配器使用，生产 `NewFactory` 必须要求 `ProviderFactory`；不能让固定 Provider 继续默默共享。

`AgentSpec` 增加 `ModelConfigRef`，保留 `ModelProvider`，并明确 `ToolPolicyRef`/`GuardrailRef` 的来源。`platform.AgentConfig` 增加与现有领域模型对应的 `ModelConfigRef`、`ToolPolicyID`、`GuardrailRef` 字段；`defaultAgentSpec` 映射这些字段。已有 `tenant.AgentApp.ModelConfigRef`、`ToolPolicyID` 和 `GuardrailRef` 是配置事实源，不能用 `ModelProvider` 字符串代替 ConfigRef。

Factory 的 `Build` 和 Runtime 的 `Run` 都必须校验：TenantContext、AgentSpec 的 `TenantID`、`AgentAppID`、版本、`ModelConfigRef`、Tool Policy/Guardrail 引用和权限上下文一致。Factory 调用 ProviderFactory 时传入防御性复制后的 Context 和 Spec；ProviderFactory 每次 Build 返回与该 ConfigRef 绑定的 Provider 实例或不可变 client，不能把一个租户的可变模型配置复用于另一个租户。

Tool bridge 只做 P0-07 需要的执行边界，不实现审批、预算、危险工具治理：

- 根据 tRPC-Agent-Go v1.11.2 实际 Tool API，在 `agent` 内部将 `ToolInvoker` 转换成框架 Tool 注册对象。
- Tool 名称和参数从框架请求转换为 `ToolCall`；调用时传递已经验证的 TenantContext。
- Tool 返回值转换为框架结果；错误同时保留 `ErrToolFailure` 和原始 cause。
- `convertEvent` 从框架事件中的 Tool call/result 字段填充 `RunnerEvent.ToolName`、类型、角色、内容和 ErrorType；若框架版本无法从同一事件读取字段，使用同一次执行的 request-scoped tool event metadata，不使用全局变量。
- 不允许模型输出直接决定租户、Binding 或凭据；ToolInvoker 只接收 Runtime 已绑定的 Context。

错误语义固定为 `errors.Is` 可判断、`errors.As` 可取原始 cause：

- Provider 返回错误：`fmt.Errorf("%w: %w", ErrProviderFailure, cause)`，不得只生成 `ResponseError{APIError}`。
- Tool 返回错误：`fmt.Errorf("%w: %w", ErrToolFailure, cause)`。
- Runner 框架错误：`ErrFrameworkFailure` 包裹原始 cause。
- 主动取消和 deadline 保留 `context.Canceled`/`context.DeadlineExceeded` 链。
- Drain 超时包裹 `ErrDrainTimeout`，并在需要时同时保留取消 cause；错误文本只包含类别和有限诊断，不包含密钥、Authorization、完整 Prompt 或原始文件内容。

**R1 测试**：

- ProviderFactory 收到正确的 TenantID、AgentAppID、ConfigVersion、ModelConfigRef、ToolPolicyRef 和 Trace/Request 上下文。
- 两个租户并发 Build，ProviderFactory 返回两个不同的可识别 Provider；请求、System Prompt、ConfigRef 和输出不能串租户。
- ProviderFactory 拒绝错误租户、错误 ConfigRef、缺少配置和取消 Context。
- ToolInvoker 成功、返回错误、Context 取消；Tool started/completed/failed 事件包含稳定 ToolName。
- Provider 返回 sentinel error 后，`errors.Is(runErr, ErrProviderFailure)` 和 `errors.Is(runErr, sentinel)` 均为真。
- 错误事件和已有 assistant/tool 事件仍被转换，不因错误映射丢失。

**R1 关闭条件**：

- `RuntimeDependencies` 不再以共享 Provider 作为生产依赖。
- Tool 请求/结果/事件链路 targeted tests 全部通过。
- Provider/Tool 原始错误链测试通过。
- `go test ./trpcservice/agent -run 'Test(Factory|Provider|Tool|Event|Error)' -count=1` 和对应 `-race` 实际运行通过。
- R1 关闭报告完成后才进入 R2。

### R2：Runner 停止、Event drain 和 goroutine 生命周期

**目标**：修复一次 Runtime 执行的取消和资源所有权，保证取消不是“Run 返回即可”，而是下游停止、事件通道关闭、producer 完成的完整协议。

**允许修改**：

- `trpcservice/agent/runtime.go`
- `trpcservice/agent/agent.go` 中 `RuntimeDependencies` 的 timeout 配置
- `trpcservice/agent/agent_test.go` 及同包新增测试

**内部生命周期契约固定如下**：

- 扩展内部 `frameworkRunner`/execution handle，至少能表达 `Run` 结果、`Stop/Close`、`Done`/producer completion；框架拥有的 Event channel 仍由框架关闭，Adapter 不关闭。
- `Run` 对每次执行创建 request-scoped execution state，包含事件收集、原始 Provider/Tool error、stop once、producer completion 和 drain completion；禁止全局状态。
- Runner 启动后，Adapter 同时管理事件 drain 和 producer completion。Provider/Runner 返回错误时仍先排空已产生事件，再返回分类错误。
- 父 Context 取消时，先调用框架公开的 Stop/Close；调用只允许一次，并使用独立的 `StopTimeout` 防止 Close 永久阻塞。
- 取消被观察后，drain 进入有限 grace window：继续消费到 channel close；到达 `DrainTimeout` 时返回 `ErrDrainTimeout`，并保留 `context.Canceled` 或 `context.DeadlineExceeded` cause。正常路径使用父 Context 的取消状态，不能无条件使用 `context.Background()` 掩盖取消；取消后的 drain grace 只能是明确的、有界的内部清理上下文。
- 无论 channel 是否先关闭，都必须等待 producer completion；超时要报告 producer 未完成，而不是只返回文本结果。
- `fw.Close()` 的返回错误不能丢弃：若主执行没有更早错误，返回 `ErrFrameworkFailure` 包裹 Close cause；若已有取消/drain 错误，作为 joined/secondary cause 保留并可诊断。
- Adapter 不调用框架 Event channel 的 `close`；只有框架 producer/内部 bridge 的唯一 owner 负责关闭自有 channel。

**推荐执行顺序**：

1. 校验输入和 Context。
2. 构造 request-scoped provider/tool/runtime state 和 framework runner。
3. 启动 Run，并记录 producer completion。
4. 消费 Event channel，同时累积结果和事件。
5. 观察父 Context 取消时调用 Stop，并开始有界 drain grace。
6. Event channel close 后等待 producer completion 和 Close completion。
7. 根据取消、producer error、drain error、provider/tool cause、结果校验的优先级组合返回结果。

**R2 测试**：

- 正常 Run：事件按序收集，channel close，producer completion 已到达，Close 调用一次。
- Provider/Runner 错误后仍继续 drain，返回原始错误链而不是提前退出。
- 主动取消：Fake Provider 收到取消；Fake producer 收到 Stop；事件 channel 被排空/关闭；producer completion 在 Run 返回前到达。
- deadline exceeded：与主动 canceled 可区分，且 stop/drain 的超时诊断明确。
- Fake producer 永不关闭 channel：在 DrainTimeout 内返回 `ErrDrainTimeout`，Stop/Close 只调用一次，并验证 producer completion 未被伪造为完成。
- Fake producer 在取消后仍发送多个事件：发送端不阻塞，Run 返回前发送端完成。
- Close 返回错误、Run 返回错误、drain 超时同时发生时，错误分类和 cause 保留规则稳定。
- 并发运行多个 Runtime/多个租户，`go test -race` 无共享状态竞争。

**R2 关闭条件**：

- 不再使用 `context.Background()` 作为无界取消掩盖路径。
- 所有成功、错误、取消、deadline、永不关闭 channel 路径都有 producer completion 证据。
- `go test ./trpcservice/agent -run 'Test(Run|Cancel|Deadline|Drain|Producer|Close|Error)' -count=1 -v` 和 `-race` 通过。
- R2 关闭报告完成后才进入 R3。

### R3：Platform 取消后的 assistant 提交保护

**目标**：修复 Runtime 返回成功后到 assistant message/event 写入之间的取消窗口，确保 canceled execution 不产生 assistant Session 事实数据。

**允许修改**：

- `trpcservice/platform/platform.go`
- `trpcservice/platform/platform_test.go`
- 如需复用纯提交类型，只修改 `trpcservice/agent` 公共结果类型；不修改 storage/migration

**提交策略固定如下**：

- 在 `Runner.Run` 中，Responder 返回后立即检查 `ctx.Err()`；取消或 deadline 时不创建/提交 assistant message/event。
- assistant message、`assistant.completed` event 和成功审计必须通过一个明确的 `CommitAssistant`/等价原子提交边界，避免在三个独立 Store 调用之间留下半提交。当前 `MemoryStore` 实现该原子方法，在同一锁保护下再次检查 Context 后一次性追加 assistant message/event；失败或取消不追加任何一项。
- 为保持已有 Store 兼容，优先增加平台内部可选的 `AssistantCommitter` 接口；若 Store 不实现该接口，则 Runner 必须在每个写入前检查 Context，并将该实现标记为非原子兼容路径，不能把它当作强一致提交已经验收。P0-07 当前 MemoryStore 主链路必须使用 `AssistantCommitter`，不能依赖可选路径通过验收。
- 取消/失败审计不能使用已取消的业务 Context。创建有限时长的 `auditCtx`（不携带取消状态但有超时），只写入脱敏的 error category、trace、latency 和 execution metadata；不得用它写 assistant message/event。
- 在 assistant commit 前后分别记录 cancellation/commit 状态，保证诊断可以区分“Runtime 成功但提交被取消”和“提交失败”。
- 不改变 Claim、Session Lease、Outbox 或 PostgreSQL Repository；P0-07 只保护当前同步平台 MemoryStore 提交边界，P0-08/P0-09 再把同一语义迁移到执行编排和事实源事务。

**R3 测试**：

- Responder 返回成功后主动取消 Context，Runner 返回 `context.Canceled`，Session 中没有 assistant message，也没有 `assistant.completed` event。
- cancel 发生在 assistant commit 前，MemoryStore 的 assistant message/event 数量保持不变。
- assistant commit 失败不会留下 message/event 半提交；审计错误分类仍可诊断。
- 正常成功仍产生一条 assistant message、一条 assistant event 和成功审计。
- 已有幂等、租户隔离、取消入口测试继续通过。

**R3 关闭条件**：

- 取消后的 assistant 写入保护测试实际通过，不能只依赖 MemoryStore 方法入口的 ctx 检查。
- `go test ./trpcservice/platform -run 'Test(Runner|Assistant|Cancel|Runtime|Tenant)' -count=1 -v` 和 `-race` 通过。
- R3 关闭报告完成后才进入 R4。

### R4：真实 ProviderFactory、runner 成功路径和跨租户配置隔离

**目标**：消除 `unavailableProvider` 占位，使 runner 模式拥有具体的可运行 ProviderFactory，并用本地 OpenAI-compatible HTTP 服务证明实际 Runner 链路成功。

**允许修改**：

- `cmd/trpc-service/main.go`、`cmd/trpc-service/main_test.go`
- `trpcservice/agent/` 内 ProviderFactory 和具体 Provider adapter 文件/测试
- `trpcservice/platform/platform.go` 中 AgentConfig/AgentSpec 配置映射
- `go.mod`、`go.sum`：只加入 v1.11.2 `model/openai` 所需的直接依赖和 checksum，不升级 tRPC-Agent-Go
- 必要的本地测试 helper；不修改 migrations、storage、channels、web

**Provider 方案固定如下**：

- 使用 tRPC-Agent-Go v1.11.2 的 `model/openai` 作为具体 OpenAI-compatible Provider 适配器，封装在 `trpcservice/agent` 内部；具体构造函数和 option 名称以锁定版本 API 探针为准。
- 增加 `ModelConfig`/`ModelConfigResolver` 和 `SecretResolver` 的内部依赖边界。`AgentSpec.ModelConfigRef` 只传引用；Secret 原文只在 Provider 构造和 HTTP 客户端内部短暂使用，不进入 AgentSpec、日志、trace、错误文本或测试输出。
- ProviderFactory 的每次 Build 使用 `(TenantContext, AgentSpec.ModelConfigRef, AgentSpec.ModelProvider)` 解析独立 Provider 配置，并校验返回配置的 tenant、AgentApp 和 provider 与请求一致；禁止使用单个全局可变 Provider 实例承载多租户配置。
- `main.newResponder("runner")` 注入真实 ProviderFactory，而不是 `unavailableProvider`。本地启动所需的最小配置为 OpenAI-compatible endpoint、model 和 secret reference/环境注入；缺配置时启动明确失败，不能自动回退 Echo。
- `MODEL_PROVIDER=echo` 仍只选择 EchoResponder；`MODEL_PROVIDER=runner` 表示 Runner + 具体 ProviderFactory。不要把 `runner` 当作 Provider 名称与 OpenAI/其他模型后端混淆。
- 为避免真实外部网络验收，新增本地 `httptest` OpenAI-compatible server，返回固定 completion/usage；测试通过真实 `newResponder("runner")`、真实 ProviderFactory、真实 tRPC-Agent-Go Runner 和平台 Runner，验证最终 assistant message/event。该测试不是 `fakeProvider` 注入测试，必须单独标记为 actual provider adapter path。

**R4 测试**：

- 缺少 ModelConfigRef、Provider、model 或 secret 时，runner 初始化失败且错误分类明确，不启动后自动 Echo。
- ModelConfigRef 解析到 tenant-a 的配置时，tenant-b 的 Context 不能使用；同一进程并发两个租户，Provider 请求的 endpoint/model/config ref 和输出彼此隔离。
- 本地 OpenAI-compatible server 返回成功时，`newResponder("runner")` 能通过 RuntimeResponder 返回结果；平台 Store 产生正确 assistant message/event/audit。
- OpenAI-compatible server 返回 HTTP/JSON provider error 时，调用方可通过 `errors.Is/As` 识别 ProviderFailure 和原始分类；不只得到 APIError 字符串。
- `go test ./cmd/trpc-service ./trpcservice/agent ./trpcservice/platform -run 'Test(Runner|Provider|Config|Tenant|Responder)' -count=1 -v` 通过；R4 相关包 race 通过。

**R4 关闭条件**：

- runner 模式不再依赖 `unavailableProvider`。
- 真实 Provider adapter path 的本地端到端成功测试实际执行，而不是 skip。
- 两租户配置隔离、缺配置 fail-closed、Provider 原始错误链和 Echo 回归全部通过。
- R4 关闭报告完成后才进入 R5。

### R5：一次性 P0-07D 重新验收和关闭

**前置**：R1、R2、R3、R4 均已关闭，并且每个关闭报告已记录。

**只执行一次**：

```bash
go test ./...
go test ./... -race
go vet ./...
test -z "$(gofmt -l .)"
rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/tenant trpcservice/channels trpcservice/storage
rg -n 'MODEL_PROVIDER|unavailableProvider|ProviderFactory|ToolInvoker|CommitAssistant|DrainTimeout|StopTimeout' trpcservice/agent trpcservice/platform cmd/trpc-service
```

边界扫描第一条必须无输出。第二条用于确认依赖方向和关键实现存在，不能代替行为测试。R5 期间不得顺手实现 Gateway、Worker、Job Queue、Outbox、真实 IM、Repository、Guardrail、审批或 Secret Manager。

**最终关闭条件**：

- review 的所有 P1/P2 项都有对应通过测试和代码证据。
- P0-07 原矩阵中 Tool 事件、错误映射、取消、Event drain、Platform bridge、runner/echo 模式和全量回归均能提供实际证据；不存在只写 `passed` 但无 targeted 测试的格子。
- `unavailableProvider` 不再作为 runner 生产路径存在；如保留测试用，名称和作用必须限定为测试 fixture，不能被 main 引用。
- 所有未实现项仍明确列在 P0-08/P0-09/P1 backlog，不以 P0-07 的 Runtime 修复扩大范围。
- 形成最终关闭报告：变更文件、R1-R4 报告链接/摘要、实际命令结果、未运行/blocked 项、已知限制、回滚方式和 P0-08 入口。

## 5. 回滚与安全边界

- 不回滚 P0-01 至 P0-06 的迁移、Claim、Lease、fencing、限流和租户契约。
- ProviderFactory、ToolInvoker、Runner lifecycle 和 CommitAssistant 都是 P0-07 内部/平台边界；框架类型继续限制在 `trpcservice/agent`。
- 任何 Provider/Tool 错误、配置引用、endpoint、secret ref、Authorization、完整 Prompt 和原始响应都不得写入日志或测试输出。
- 如果 R4 的 OpenAI-compatible 依赖解析或锁定版本 API 无法在当前依赖状态下完成，R4 状态为 `blocked/not verified`；不使用 `unavailableProvider` 伪造成功，也不通过删除真实 Provider 验收来关闭 P0-07。
- 如果发现需要修改 TenantContext、Storage 或 Migration 才能完成本方案，先停止当前子阶段并将问题记录为唯一阻塞项；默认不扩大 P0-07 范围。

## 6. 预期变更文件

| 文件 | 预期作用 |
| --- | --- |
| `trpcservice/agent/agent.go` | ProviderFactory、ToolInvoker、ModelConfigRef、稳定错误和依赖复制 |
| `trpcservice/agent/runtime.go` | Tool bridge、Provider error cause、Runner stop/drain/completion 生命周期 |
| `trpcservice/agent/provider.go` | OpenAI-compatible ProviderFactory、配置解析和 SecretResolver 边界 |
| `trpcservice/agent/tool.go` | 框架 Tool 注册/调用桥接 |
| `trpcservice/agent/agent_test.go` | Provider/Tool/错误/取消/drain/producer/租户隔离测试 |
| `trpcservice/agent/provider_test.go` | 本地 OpenAI-compatible server、ProviderFactory 和错误映射测试 |
| `trpcservice/platform/platform.go` | Agent 配置映射、AssistantCommitter 和取消后的提交保护 |
| `trpcservice/platform/platform_test.go` | 取消后无 assistant 写入、原子提交和真实 runner bridge 回归 |
| `cmd/trpc-service/main.go` | 真实 ProviderFactory 注入，移除 runner 对 unavailableProvider 的依赖 |
| `cmd/trpc-service/main_test.go` | runner/echo/缺配置/真实工厂构造测试 |
| `go.mod`、`go.sum` | 仅补齐 OpenAI model adapter 直接依赖和 checksum |

不修改：`trpcservice/tenant/`、`trpcservice/storage/`、`migrations/`、`trpcservice/channels/`、`trpcservice/web/`、Gateway/Worker/Queue/Outbox 及后续治理目录；不修改 `docs/plan007v1`。
