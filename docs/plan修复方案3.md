# P0-07 修复方案3

## 1. 全新上下文项目背景

### 1.1 项目目标

当前仓库 `C:\Documents\trpc-agent-service` 是一个面向多租户的节点化 Agent 平台。平台主链路负责接收带租户边界的请求，解析 `TenantContext`，根据 Agent 配置构造 Runtime，执行模型/Tool，并把成功结果写入 Session、Event 和 Audit。

项目采用分阶段建设方式：

- P0-01 至 P0-05 建立租户、配置、Session、Memory、基础平台和运行时骨架。
- P0-06 建立 Redis/PostgreSQL Claim、Lease、epoch/fencing、failover、熔断和多维限流等协调基础设施。
- P0-07 的职责是把真实的 tRPC-Agent-Go `v1.11.2` Runtime Adapter 接入当前平台，同时保持平台公共接口不暴露框架类型。
- Gateway、Worker、Job Queue、Outbox、Repository、真实 IM 协议、鉴权、OTel、向量库、对象存储、危险 Tool 治理和后续编排不属于本轮。

当前主流程已经从早期的 `MemoryStore -> platform.Runner -> EchoResponder` 扩展为包含真实 `ProviderFactory`、tRPC-Agent-Go Runner、Tool bridge 和 AssistantCommit 的实现，但 P0-07 尚未关闭。

### 1.2 权威资料和阅读顺序

执行本方案的全新 Pi 必须以实际代码和实际测试结果为事实，阅读顺序固定如下：

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/implementation-plan.md`
4. `docs/project-status.md`
5. `docs/plan006v2.md`
6. `docs/plan006v2困境分析.txt`
7. `docs/plan006v2方案修正与困境解决.txt`
8. `docs/plan006v2困境解决的实际操作范式.txt`
9. `docs/plan007review.md`
10. 当前 P0-07 实现和测试文件

`docs/plan007v1` 是原始计划文件，本方案明确要求不修改它。`docs/plan007review.md` 是已有 review 方案资料，也不作为本轮代码修改目标；本方案是独立的 P0-07 修复方案3。

### 1.3 P0-06 困境对本轮的直接约束

P0-06 的困境材料已经证明，复杂阶段不能把“代码存在”“编译通过”“普通测试通过”“全量测试通过”直接等同于行为完成。本轮必须继续遵循：

- 先冻结工作树，再修改；不使用 `git reset --hard`、`git checkout --` 或覆盖已有用户改动。
- 每次只推进一个明确子阶段，阶段有独立关闭条件。
- 最多维护一个当前阻塞项；其他发现归并到该阻塞项、已知限制或 backlog。
- API 不确定时先查锁定版本源码，不凭印象猜测。
- `[no test files]`、skip、只覆盖 fake seam、只验证函数返回但未验证 goroutine/producer 生命周期，都不能记为 `passed`。
- 先 targeted test，再 targeted race；所有行为阶段通过后才执行一次全量 test/race/vet/gofmt。
- 失败报告必须记录真实事件序列、租户、Agent、ConfigRef、ToolName、错误链、pump/producer 状态、Stop/Close 次数和 assistant 写入计数。
- 已通过的租户边界、平台协调契约和 AssistantCommit 不得被无关重构重新打开。

### 1.4 P0-07 当前关键文件和边界

本轮重点文件：

- `trpcservice/agent/agent.go`：`AgentSpec`、`AgentInput`、`AgentResult`、`RunnerEvent`、ProviderFactory、ToolInvoker、RuntimeDependencies 和稳定错误。
- `trpcservice/agent/runtime.go`：tRPC-Agent-Go Runner 适配、Tool 注册选择、Event pump、drain、取消、Stop、Close 和错误组合。
- `trpcservice/agent/tool.go`、`trpcservice/agent/tool_bridge.go`：平台 ToolInvoker 与框架 Tool API 的桥接。
- `trpcservice/agent/provider.go`：OpenAI-compatible ProviderFactory、ModelConfigResolver、SecretResolver、ProviderResponseError。
- `trpcservice/agent/*_test.go`：Runtime、Tool、Provider、配置隔离和生命周期测试。
- `trpcservice/agent/runner_integration_test.go`：真实 httptest OpenAI-compatible server、真实 Runner 和真实 Tool 两轮流程测试。
- `trpcservice/platform/platform.go`：已有 AssistantCommit、取消后的 assistant 提交保护和 audit context。
- `trpcservice/platform/platform_test.go`：平台提交保护回归测试。
- `cmd/trpc-service/main.go`：`MODEL_PROVIDER=runner/echo` 选择、开发环境 Provider 配置和 fail-closed 绑定。
- `cmd/trpc-service/main_test.go`：启动模式和配置错误测试。
- `go.mod`、`go.sum`：锁定的 tRPC-Agent-Go/OpenAI adapter 依赖。

框架类型只能存在于 `trpcservice/agent` 内部，不能进入 `tenant`、`channels`、`storage` 或平台公共接口。

## 2. 当前第三次审查结论和基线

### 2.1 当前状态

P0-07 当前状态必须保持为：`blocked`。

本轮没有修改代码、没有修改 `docs/plan007v1`、没有提交或推送。

已经通过的基础验证：

- `go test ./... -count=1`
- `go test ./... -race -count=1`
- `go vet ./...`
- `gofmt` 检查
- framework 类型边界扫描
- 真实 OpenAI-compatible Runner 基础成功路径
- 真实 `WithTools` 注册和 Tool callback 基础调用
- Provider 基本配置隔离
- AssistantCommit 取消保护

不能据此关闭 P0-07。

### 2.2 当前唯一阻塞项

当前唯一阻塞项为：

> `runtime.go` 的 adapter pump 终止协议不完整。pump 向容量为 16 的自有 channel 发送时没有取消分支；drain timeout 后 Runtime 停止消费，达到第 17 个事件时 pump 可能永久阻塞，`producerDone` 不会置位，且 framework `Close` 不等待该 adapter pump。相同 timeout 终止路径还没有把已保存的 framework Run error 加入最终错误链。

该阻塞项必须优先修复并单独验收。

### 2.3 已知 residual risk

tRPC-Agent-Go v1.11.2 的公开接口只有：

- `runner.Runner.Run`
- `runner.Runner.Close`
- `runner.ManagedRunner.Cancel`
- `runner.ManagedRunner.RunStatus`

没有公开的 `Done`、`Wait` 或 framework producer completion API。框架内部 `runEventLoop` 和相关 goroutine 的完成状态不可由 Adapter 直接观察。

因此本方案最多能够证明：

- framework-owned Event channel 的关闭状态；
- adapter-owned pump channel 的关闭状态；
- adapter pump 的 `done`/`pumpDone`；
- framework Run error、Cancel、Stop、Close 的返回结果。

本方案不能声称已经证明 v1.11.2 framework 内部所有后台 goroutine 已退出。即使本轮 Critical 阻塞项修复，若框架版本仍没有公开 completion API，P0-07 仍不能标记为 `closed`，只能形成带 residual risk 的 `in_progress`/`blocked` 报告，并把框架能力缺口列入后续升级或框架适配 backlog。

## 3. 方案3目标和非目标

### 3.1 目标

本轮只做以下修复和证据补强：

1. 修复 adapter pump 在 backpressure、drain timeout 和取消后的无界阻塞。
2. 明确区分 adapter pump completion、framework Event channel close 和不可观察的 framework 内部 completion。
3. 保证 timeout、producer incomplete、cancel、deadline 和 close error 路径都保留已保存的 framework Run error 原始 cause。
4. 补齐真实 Tool 两轮请求内容和 Tool failed/canceled 证据。
5. 补齐 transport、Tool、deadline、Stop、Close 和 `errors.Join` 的最小错误链测试。
6. 补齐两个 Runtime 并发执行时 SystemPrompt、ToolPolicyRef、GuardrailRef、Tool 列表、Event metadata 和输出的隔离测试。
7. 保留当前已通过的 Platform AssistantCommit 和 `MODEL_PROVIDER=runner/echo` 行为，不扩展到其他 P0 阶段。
8. 重新生成一份事实完整的 P0-07 修复方案3阶段报告；不把 framework 内部 completion residual risk 伪装成通过。

### 3.2 非目标

本轮明确不做：

- 不修改 `docs/plan007v1`。
- 不修改 `docs/plan007review.md`，除非用户另行要求记录报告；默认只在执行结果中输出报告。
- 不修改 `trpcservice/tenant/`、`trpcservice/storage/`、`trpcservice/channels/`、`trpcservice/web/`、migrations。
- 不实现 framework 内部不存在的 Done/Wait API。
- 不关闭 framework-owned Event channel。
- 不声称用 goleak 可以证明 framework 内部 goroutine 绝对完成。
- 不实现 Gateway、Worker、Queue、Outbox、Repository、真实 IM、鉴权、OTel、向量库、对象存储、危险 Tool 治理或 Secret Manager。
- 不为了让测试通过而把 pump cancel 视为 framework producer completion。

## 4. 执行阶段

### P3-0：基线冻结和源码复核

**目标**：确认第三次审查描述与当前工作树一致，避免基于旧代码修改。

执行：

```bash
git status --short
git diff --stat
git diff --check
go test ./trpcservice/agent ./trpcservice/platform ./cmd/trpc-service -count=1
go test ./trpcservice/agent ./trpcservice/platform ./cmd/trpc-service -race -count=1
go vet ./trpcservice/agent ./trpcservice/platform ./cmd/trpc-service
```

读取并核对：

- `trpcservice/agent/runtime.go` 的 `newExecution`、pump goroutine、`drainExecution`、`frameworkError`、`producerDone`、`done`、defer cleanup。
- `trpcservice/agent/runner_integration_test.go` 的第二次 HTTP 请求断言和 Tool 事件断言。
- `trpcservice/agent/provider_test.go`、`runtime_lifecycle_test.go`、`provider_isolation_test.go`。
- tRPC-Agent-Go v1.11.2 `runner.Runner`、`ManagedRunner`、`Runner.Close`、内部 `runEventLoop` 和 Tool API 源码。

P3-0 不修改代码。

**关闭条件**：

- Critical backpressure 缺陷、framework error 丢失路径和 residual risk 的源码证据与当前代码行号一致。
- 如果基线与审查描述不一致，先以当前源码为准，重新更新唯一阻塞项，不开始猜测式修复。

### P3-1：修复 adapter pump 的取消和 backpressure 协议

**目标**：保证 adapter pump 不会因为自有 channel 满载而永久阻塞，同时不把 pump 退出伪装成 framework producer completion。

**允许修改**：

- `trpcservice/agent/runtime.go`
- 必要时 `trpcservice/agent/agent.go` 中的内部错误/生命周期类型
- `trpcservice/agent/runtime_lifecycle_test.go`
- 必要时新增同包测试 helper

**固定设计**：

为每次执行建立明确的 request-scoped 生命周期状态，至少区分：

```go
type execution struct {
    frameworkEvents   <-chan *frameworkevent.Event // framework-owned
    adapterEvents     chan *frameworkevent.Event  // adapter-owned
    pumpDone          chan struct{}               // adapter pump completion
    frameworkClosed   atomic.Bool                 // observed framework channel close
    pumpCompleted     atomic.Bool                 // pump goroutine exited
    producerDone      atomic.Bool                 // only use with explicit documented meaning
    pumpCancel        context.CancelFunc
    stopOnce          sync.Once
    closeOnce         sync.Once
    runErr            error
}
```

具体规则：

1. pump 从 framework-owned channel 读取时必须同时监听 request-scoped pump context：

   ```go
   select {
   case eventValue, ok := <-frameworkEvents:
       ...
   case <-pumpCtx.Done():
       ...
   }
   ```

2. pump 向 adapter-owned channel 发送时也必须同时监听 pump context：

   ```go
   select {
   case adapterEvents <- eventValue:
       ...
   case <-pumpCtx.Done():
       ...
   }
   ```

3. pump 退出时只关闭 adapter-owned channel，并关闭 `pumpDone`；绝不关闭 framework-owned channel。

4. framework channel 正常关闭时，记录 `frameworkClosed=true`，再关闭 adapter channel 和 `pumpDone`。

5. pump 因 timeout/cancel 退出时，记录 `pumpCompleted=true`，但不得把 `frameworkClosed` 设为 true，也不得把“pump 已退出”自动称为 framework producer 已完成。

6. `producerDone` 必须重新命名或补充明确语义。推荐使用两个状态：

   - `pumpDone/pumpCompleted`：Adapter 自己的 pump 已退出。
   - `frameworkChannelClosed`：Adapter 观察到 framework-owned channel 已关闭。

   如果保留旧的 `producerDone` 字段，只能将其文档化为 adapter pump completion，不能用它表示 framework 内部 completion。

7. drain timeout 到达后，先执行一次 request-scoped `pumpCancel()`，再在独立的 bounded cleanup window 内等待 `pumpDone`。等待不到时返回 `ErrProducerIncomplete`，但不得无限等待。

8. drain timeout 后，即使 pump 能够退出，也必须根据 `frameworkClosed` 区分：

   - `pumpDone=true, frameworkClosed=true`：framework Event channel 已正常关闭，adapter 清理完成。
   - `pumpDone=true, frameworkClosed=false`：adapter 已停止消费；framework channel 未关闭，不能称为 framework producer 完成。
   - `pumpDone=false`：adapter pump 自身仍未完成，返回 `ErrProducerIncomplete`。

9. pump 取消时可能丢弃尚未转发的尾部事件，这是 bounded termination 的明确行为；该行为必须只发生在取消/timeout/故障路径，正常成功路径不得丢事件。关闭报告必须记录该语义。

10. 如果 framework Cancel 可以使 framework channel 最终关闭，优先等待有限时间观察 `frameworkClosed`；如果 v1.11.2 无法提供更强保证，超时后只能记录 residual risk，不能无限等待。

11. `fw.Close()` 不负责也不能被假定为等待 adapter pump。Runtime 必须先通过自己的 pump cancellation 协议让 adapter pump 可退出，再执行一次 `fw.Close()`；Close 的返回错误仍需保留。

12. stop、pump cancel 和 close 必须是幂等的：

   - Stop/Cancel 最多调用一次。
   - pumpCancel 最多执行一次或使用可重复安全的 context cancel。
   - framework Close 最多调用一次。

**P3-1 测试**：

- Pump channel 容量为 1 或小容量时，发送 2 个以上事件并触发 drain timeout；验证 Runtime 在 bounded 时间内返回，pump 不永久阻塞，`pumpDone` 最终或在 cleanup deadline 内有明确状态。
- 发送至少 17 个事件，复现原审查中的满载路径；验证不存在无条件 `adapterEvents <- event` 的生产代码。
- timeout 后继续向 framework-owned channel 发送事件；验证 pump 不因 adapter channel 无消费者而永远阻塞。
- cancel 后发送尾部事件；验证正常 bounded cleanup，不关闭 framework-owned channel。
- framework channel 正常关闭：`frameworkClosed=true`、`pumpDone=true`、事件完整且顺序稳定。
- pump 因取消退出但 framework channel 未关闭：返回 `ErrProducerIncomplete` 或等价稳定错误，不返回成功，不把 `frameworkClosed` 标记为 true。
- Stop timeout、pump cleanup timeout 和 Close error 同时发生时，状态和错误链可诊断。
- `go test ./trpcservice/agent -run 'Test(Execution|Pump|Backpressure|Producer|Drain|Cancel|Stop|Close)' -count=1 -v`
- 对应 `-race` 必须通过。

**P3-1 关闭条件**：

- 生产 pump 的 receive/send 都有取消分支。
- 满载 backpressure 路径不再可能无界阻塞在 adapter-owned channel send。
- adapter pump completion 与 framework channel close 明确区分。
- 测试证明 timeout 后不把 pump 退出伪装成 framework producer 完成。
- P3-1 关闭报告明确写出仍无法观测的 framework 内部 completion residual risk。

### P3-2：统一 drain 终止路径的错误组合

**目标**：修复 drain timeout/producer incomplete 路径丢失 framework Run error 的问题，并让所有终止路径使用一致的 `errors.Is`/`errors.As` 语义。

**允许修改**：

- `trpcservice/agent/runtime.go`
- `trpcservice/agent/agent.go` 中稳定错误定义（仅必要时）
- `trpcservice/agent/runtime_lifecycle_test.go`
- `trpcservice/agent/provider_test.go`、必要时新增错误测试文件

**固定规则**：

1. `newExecution` 保存的 framework Run error 必须在所有最终返回路径中参与错误组合：

   - 正常 channel close 后；
   - framework Run error + 已有事件；
   - context canceled/deadline；
   - Stop error；
   - drain timeout；
   - producer incomplete；
   - pump cleanup timeout；
   - Close error。

2. 建议实现统一的内部错误组合函数，例如：

   ```go
   func combineExecutionErrors(state executionState, primary error) error
   ```

   但只有该抽象能减少重复并保持当前代码风格时才新增，不为此做无关重构。

3. 所有原始 cause 必须用 `%w` 或 `errors.Join` 保留。禁止使用 `%v` 作为唯一包装方式。

4. 取消和 deadline 的 `context.Canceled`/`context.DeadlineExceeded` 必须仍可通过 `errors.Is` 判断；如果同时有 framework error、Stop error 或 drain error，使用 `errors.Join` 保留全部 cause。

5. `ErrDrainTimeout`、`ErrProducerIncomplete`、`ErrFrameworkFailure`、`ErrProviderFailure`、`ErrToolFailure` 等稳定 sentinel 必须可判断。

6. framework Run error + drain timeout 的组合必须满足：

   ```go
   errors.Is(err, frameworkSentinel) == true
   errors.Is(err, ErrDrainTimeout) == true
   ```

7. Close error、Stop error、transport error 和 Tool 原始 sentinel 必须分别满足 `errors.Is`/`errors.As`。

8. 错误返回不得泄漏 API key、Authorization、完整 prompt 或完整 response body。

**P3-2 测试**：

- framework Run 返回 sentinel，同时 pump 不在 drain window 内完成：最终错误同时包含 framework sentinel、`ErrDrainTimeout` 或 `ErrProducerIncomplete`。
- provider transport 返回 sentinel：最终错误同时包含 `ErrProviderFailure` 和 transport sentinel。
- Tool callback 返回 sentinel：最终错误同时包含 `ErrToolFailure` 和 Tool sentinel。
- deadline：`errors.Is(err, context.DeadlineExceeded)` 为 true。
- Stop 返回 sentinel：最终错误保留 framework lifecycle sentinel 和 Stop sentinel。
- Close 返回 sentinel：最终错误保留 `ErrFrameworkFailure` 和 Close sentinel。
- 多错误 `errors.Join` 组合不丢失 primary 和 secondary cause。
- `go test ./trpcservice/agent -run 'Test(Error|Cause|Join|Deadline|Framework|Provider|Tool|Close|Stop)' -count=1 -v`
- 对应 `-race` 必须通过。

**P3-2 关闭条件**：

- 原审查指出的 framework error 丢失路径有回归测试且通过。
- transport、Tool、deadline、Stop、Close 和 Join 至少各有一条 `errors.Is`/`errors.As` 证据。
- 没有错误分类通过字符串比较作为唯一行为依据。

### P3-3：补强真实 Tool 两轮集成证据

**目标**：只补充现有 integration test 的缺失断言，不重新设计 Tool 业务范围。

**允许修改**：

- `trpcservice/agent/runner_integration_test.go`
- 必要时 `trpcservice/agent/tool_bridge.go` 或 `runtime.go` 中错误事件映射
- 不修改 `tenant`、`storage`、`channels`、`platform` 公共接口

**测试必须补齐**：

1. 第一次 HTTP request body：

   - 包含 tool declaration；
   - 包含预期 ToolName；
   - 不包含其他租户或 Agent 的 Tool；
   - 必要时包含正确的 schema。

2. Tool callback：

   - 实际收到正确的 TenantID、AgentAppID、ConfigVersion、ModelConfigRef、ToolPolicyRef、GuardrailRef；
   - 收到正确 ToolName 和参数；
   - 参数是防御性复制，callback 修改参数不会影响 Runtime 输入。

3. 第二次 HTTP request body：

   - 包含第一轮 assistant Tool Call；
   - 包含对应 Tool Call ID；
   - 包含 Tool result/message；
   - Tool result 内容和 `IsError` 语义正确；
   - 第二次请求不串入其他租户或 Agent 的信息。

4. 最终结果：

   - assistant 文本正确；
   - Tool event 具有稳定 Sequence；
   - 对应 ToolName 在 started/completed/failed 事件中稳定；
   - started 先于终态；
   - 同一 Tool invocation 只有一个终态事件。

5. 增加 framework-level failed Tool 测试：ToolInvoker 返回 sentinel，验证请求错误、Tool failed event、`errors.Is(ErrToolFailure)` 和原始 sentinel。

6. 增加 framework-level canceled/deadline Tool 测试：下游收到取消，不能生成 completed event；最终错误可区分 canceled 和 deadline。

7. 这些测试必须实际执行，不得 skip，不得只直接调用 `invokeTool` 辅助函数。

**P3-3 关闭条件**：

- 第二次 HTTP body 实际包含 Tool Call/Tool Result/Call ID 的证据存在。
- Tool success、failed、canceled 至少各有一条 framework-level 测试。
- 事件序列和 ToolName 不再只用布尔值或事件数量间接判断。

### P3-4：补强错误链和完整 Agent 配置隔离证据

**目标**：关闭第三次审查中的 Medium 项，但不扩大生产配置中心范围。

**允许修改**：

- `trpcservice/agent/provider_test.go`
- `trpcservice/agent/provider_isolation_test.go`
- `trpcservice/agent/runner_integration_test.go`
- 必要时 `trpcservice/agent/provider.go` 的校验逻辑
- 必要时 `cmd/trpc-service/main_test.go`

**错误测试**：

- Provider transport 原始 sentinel。
- Provider structured response error 的 `ProviderResponseError`、Type、Code、Message。
- Tool 原始 sentinel。
- `context.Canceled` 和 `context.DeadlineExceeded`。
- Stop error、Close error。
- framework error + drain timeout/producer incomplete 的 Join。
- 错误文本脱敏。

**配置隔离测试**：

构造两个完整的 Runtime：

- tenant-a / agent-a / version-a / ref-a；
- tenant-b / agent-b / version-b / ref-b。

两套配置必须在测试中使用不同的：

- endpoint；
- model；
- secret reference；
- SystemPrompt；
- ToolPolicyRef；
- GuardrailRef；
- Tool 列表/Tool declaration；
- Event metadata；
- 最终 assistant 输出。

并发执行后验证：

- ProviderFactory 看到的所有 key 均正确；
- HTTP 请求 endpoint/model/config ref 不串；
- ToolInvoker 收到的 TenantContext/AgentSpec 不串；
- Tool declaration 和事件 metadata 不串；
- assistant 输出不串；
- Runtime 输入的 History、Metadata 和 AgentSpec 防御性复制有效。

不把 `cmd/trpc-service/main.go` 的环境变量 resolver 当作生产多租户配置中心。只验证它对明确绑定的 Tenant/Agent/Version/ConfigRef fail closed；真实 Registry/Secret Manager 仍属于后续范围。

**P3-4 关闭条件**：

- Medium 错误链覆盖项全部有实际测试。
- 两个完整 Runtime 的 Agent 配置字段并发隔离测试通过。
- 不修改生产租户模型、不引入真实配置中心。

### P3-5：回归验收和 residual risk 报告

**前置条件**：P3-1 至 P3-4 均有关闭报告。任何阶段为 `failed` 或 `blocked` 都不能进入最终回归关闭。

执行：

```bash
go test ./trpcservice/agent -count=1 -v
go test ./trpcservice/agent -race -count=1 -v
go test ./trpcservice/platform ./cmd/trpc-service -count=1 -v
go test ./trpcservice/platform ./cmd/trpc-service -race -count=1 -v
go test ./... -count=1
go test ./... -race -count=1
go vet ./...
test -z "$(gofmt -l .)"
rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/tenant trpcservice/channels trpcservice/storage trpcservice/platform
rg -n 'adapterEvents <-|frameworkEvents|pumpDone|frameworkClosed|producerDone|ErrProducerIncomplete|errors\.Join|ProviderResponseError|ToolInvoker\.Invoke|WithTools' trpcservice/agent cmd/trpc-service
```

如果环境是 Windows，`test -z` 可使用等价 PowerShell 检查，但必须记录实际命令和输出，不得只写“gofmt clean”。

最终矩阵必须重新填写：

| 类别 | 必须达到的状态 |
| --- | --- |
| Pump backpressure | `passed`，满载发送不再无界阻塞 |
| Adapter pump completion | `passed`，timeout 后有 bounded 状态和证据 |
| Framework Event channel ownership | `passed`，adapter 不关闭 framework channel |
| Framework Run error + drain error | `passed`，原始 cause 可 `errors.Is` |
| Tool 两轮请求内容 | `passed`，第二次 body 含 Tool Call ID 和 Tool Result |
| Tool success/failed/canceled | `passed`，均有 framework-level 测试 |
| Provider/Tool/Stop/Close/Deadline/Join error chain | `passed` |
| 全部 Agent 配置字段并发隔离 | `passed` |
| AssistantCommit 取消保护 | 回归 `passed` |
| 全量 test/race/vet/gofmt | `passed` |
| framework 内部后台 completion | `blocked` 或 `known residual risk`，不得伪造为 passed |

即使所有 Adapter 可验证项通过，只要 v1.11.2 仍没有 framework 内部 completion API，最终 P0-07 状态仍不得写成 `closed`。最终报告应写成：

- `P0-07 = in_progress` 或 `blocked`；
- adapter pump backpressure 缺陷已修复；
- adapter-owned completion 已有 bounded 证据；
- framework 内部 completion 仍受 v1.11.2 公开 API 限制；
- 只有在后续升级框架或获得可观察 completion API 后，才重新评估关闭。

## 5. 预期文件边界

允许修改：

- `trpcservice/agent/runtime.go`
- `trpcservice/agent/agent.go`，仅必要的生命周期/错误类型
- `trpcservice/agent/tool_bridge.go`，仅必要的错误/事件映射
- `trpcservice/agent/runtime_lifecycle_test.go`
- `trpcservice/agent/runner_integration_test.go`
- `trpcservice/agent/provider_test.go`
- `trpcservice/agent/provider_isolation_test.go`
- `trpcservice/agent/tool_test.go` 或同包测试文件
- `cmd/trpc-service/main_test.go`，仅必要回归
- `go.mod`、`go.sum` 仅在测试依赖确实需要时变更；默认不新增 goleak

默认不修改：

- `docs/plan007v1`
- `docs/plan007review.md`
- `trpcservice/tenant/`
- `trpcservice/storage/`
- `trpcservice/channels/`
- `trpcservice/web/`
- `trpcservice/platform/platform.go`，除非回归证明已有 AssistantCommit 被本轮破坏；优先只运行现有平台测试
- migrations、Gateway、Worker、Queue、Outbox、Repository 和后续治理目录

## 6. 阶段报告和最终状态要求

每个 P3 阶段关闭报告必须包含：

- 阶段目标。
- 实际变更文件。
- 实际执行命令和完整结果。
- 关键测试名称及验证内容。
- pumpDone、frameworkClosed、producerDone 的实际含义和观测值。
- Stop/Close 调用次数。
- Tool 事件序列和第二次 HTTP body 关键字段。
- `errors.Is`/`errors.As` 证据。
- 当前唯一阻塞项、已知 residual risk 和下一阶段入口。

最终报告必须直接回答：

1. adapter pump 在自有 channel 满载时是否还能无界阻塞？
2. drain timeout/producer incomplete 返回时，framework Run error 是否仍在错误链中？
3. Runtime 返回前是否有可验证的 adapter pump completion？
4. framework-owned Event channel 是否始终由 framework 自己关闭？
5. 第二次模型请求是否实际包含 Tool Call ID 和 Tool Result？
6. Tool failed/canceled 是否有 framework-level 行为证据？
7. transport、Tool、deadline、Stop、Close 和 Join 原始 cause 是否都能通过 `errors.Is`/`errors.As` 取回？
8. 两个完整 Agent 配置组合是否在并发 Runtime 中完全隔离？
9. v1.11.2 framework 内部后台 completion 是否仍不可观察？

若第 1 至第 8 项全部有通过证据，但第 9 项仍受 v1.11.2 API 限制，必须保持 `P0-07 = blocked/in_progress`，不得标记 `closed`。
