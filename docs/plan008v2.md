# P0-08 执行编排实施计划 v2

> 本文件是 `docs/plan008.md` 的修订副本。原文件保留不变。本版本吸收 P0-06 实施期间形成的分阶段执行、验收矩阵和对话收敛规则，用于指导 P0-08 的实际编码。
>
> 本计划描述的是一个可独立验收的 P0-08 执行编排切片：Gateway、AgentJob、Queue、Worker 和 Execution。它不提前实现 P0-09 的真实业务 Repository，也不把 P0-10/P0-11 的持久化 Queue、Outbox、Retry/DLQ 业务能力伪装成已完成。

## 1. 修订目标和执行规则

### 1.1 为什么需要 v2

P0-06 的实际问题不是技术约束过严，而是执行计划把多个互相依赖的子系统绑定为一个全局 AND 终止条件。Agent 每轮完成一小段、运行已有测试后，仍会发现大量未完成项，于是停止当前轮，用户再次发送“继续”，形成重复性对话和过度审查。

P0-08 v2 保留安全和一致性要求，但把实施过程改为可以逐段关闭的任务队列：

- 每次只有一个 P0-08 子阶段处于 `in_progress`。
- 每个子阶段拥有明确的 `in-scope`、`out-of-scope`、允许文件、定向命令和关闭条件。
- “代码存在”不等于“已验证”；“普通测试通过”不等于“阶段关闭”。
- 测试未运行、环境缺失或真实依赖不可用时，状态必须是 `not verified` 或 `blocked`，不能标记为 `passed`。
- 当前子阶段发现的新需求进入 backlog，不自动扩大当前范围。
- 每个子阶段最多维护一个当前阻塞项；其余问题记录为后续项或已知限制。
- 修改后的定向测试通过后才进入下一项；不因后续阶段未完成而重复运行前面已经关闭的全部测试。
- 阶段关闭时才运行一次该阶段要求的全量回归、race、vet 和边界扫描；未关闭阶段不运行“大而全”的验证来制造无效循环。
- 任何失败都必须保留原始错误和错误分类，不通过放宽断言、增加 sleep、忽略 goroutine 或吞掉错误来“修复”测试。
- Agent 只有生成当前子阶段关闭报告或明确的阻塞交接报告后才能停止。

### 1.2 状态定义

| 状态 | 含义 |
| --- | --- |
| `not started` | 尚未进入该子阶段 |
| `in_progress` | 当前唯一正在处理的子阶段 |
| `verified` | 该项所需代码、测试和环境证据全部具备 |
| `not verified` | 有代码或部分测试，但必需证据缺失，不能推导为通过 |
| `blocked` | 因外部依赖、前置阶段或明确技术阻塞无法验证 |
| `deferred` | 明确属于后续任务，不影响当前子阶段关闭 |

## 2. 权威材料和当前基线

### 2.1 阅读优先级

实施前按以下优先级重新核对，不依赖历史计划中的静态描述：

1. 当前源码、当前测试结果和当前交接材料。
2. `docs/project-status.md`。
3. `docs/ARCHITECTURE.md`。
4. `docs/implementation-plan.md`。
5. `docs/P0-07阻塞交接结论.md`、P0-06 验收矩阵和本文件。
6. `docs/plan008.md` 仅作为原始范围和设计追溯材料，不作为当前状态来源。

实施入口必须先执行只读检查：

```bash
git status --short
git diff --stat
git diff --check
```

不得撤销、覆盖、重置或清理已有工作区修改。不得执行 `git commit`、`git push`，不得访问生产环境，不得读取或输出密钥、Token、密码和生产日志。

### 2.2 P0-07 交接约束

`docs/P0-07阻塞交接结论.md` 将 P0-07 冻结为 `blocked`：tRPC-Agent-Go v1.11.2 没有公开、稳定、合法的 framework run completion/wait API。P0-08 可以消费 P0-07 已交接的平台公共接口，但不负责解除该阻塞。

P0-08 严禁：

- 把 adapter pump completion 描述成 framework completion。
- 访问 tRPC-Agent-Go 私有字段或内部 channel。
- 通过 goleak、sleep、放宽 drain timeout 或忽略残留 goroutine 绕过 blocker。
- 修改 P0-07 Runtime 生命周期来伪造 completion。
- 在 Queue、Gateway、Execution、Worker、Web 或 Storage 中导入 tRPC-Agent-Go 类型。

Worker 必须把 `agent.ErrProducerIncomplete`、`agent.ErrDrainTimeout`、framework failure、Context cancellation 和 Lease loss 映射为明确的失败或不完整状态，不能把它们 Ack 成成功。

### 2.3 P0-06 可消费契约

P0-08 只消费已有的 `storage.LeaseStore` 和 `storage.Lease` 契约，不重新实现 Redis/PostgreSQL Lease、epoch 或 fencing：

```go
type LeaseStore interface {
    Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (Lease, error)
    Renew(context.Context, tenant.TenantContext, Lease, time.Duration) (Lease, error)
    Release(context.Context, tenant.TenantContext, Lease) error
    Validate(context.Context, tenant.TenantContext, Lease) error
}
```

Lease 的 owner、epoch、fencing token 和过期语义由 P0-06 负责。Execution 只负责在执行期间持有、续租、验证和释放，并在失效时取消 Runtime。

### 2.4 P0-08 基线边界

当前主进程仍是 `MemoryStore -> platform.Runner -> EchoResponder` 同步演示链路；当前没有完整的 `gateway`、`queue`、`worker`、`execution` 异步执行链路。P0-08 应新增可测试的编排边界，但不得把内存 Fake 宣称为生产持久化能力。

当前总计划把 P0-08 描述为 Session Execution Orchestrator、把 Gateway/Queue/Worker 放在 P0-10；历史计划和本轮目标将它们合并为一个执行编排切片。本版本沿用用户明确的 P0-08 目标，但保留以下边界：不实现真实业务 Repository、Outbox Dispatcher、真实 IM 协议、认证、Telemetry、模型厂商 SDK 或外部基础设施。

## 3. P0-08 总目标和不变量

### 3.1 目标链路

```text
已解析请求
  -> Gateway 校验
  -> Queue Enqueue
  -> Queue receipt Accepted
  -> 快速 ACK
  -> Worker Receive Delivery
  -> 恢复 TenantContext/Trace
  -> Acquire Session Lease
  -> Lease renewal
  -> AgentFactory.Build
  -> AgentRuntime.Run
  -> 提交前 Validate fencing
  -> ExecutionSink Commit
  -> Ack 或 Nack
  -> Release Lease
```

### 3.2 必须始终成立的不变量

1. Queue receipt 未明确 `Accepted=true` 前，Gateway 不返回成功 ACK。
2. `AgentJob` 不包含 `context.Context`、框架 Runner、数据库连接、Secret 明文、API Key、Authorization、可变 Repository 或完整外部请求对象。
3. Job 跨边界只携带版本化 DTO；Worker 重新创建执行 Context，不直接转运 Queue Context。
4. Worker 恢复后的租户、Agent、Binding、Channel、Session、Request、Message、Trace 和 Execution 字段必须通过一致性校验。
5. 同一 Session 的正确性依赖 `LeaseStore` 和 fencing，不依赖进程内 mutex 或 sticky session。
6. 不同 Session 不得被错误的全局锁串行化。
7. Lease Renew/Validate 失败、epoch 变化或 fencing 失效时，Runtime Context 必须被取消，旧 Worker 不得提交结果。
8. ExecutionSink 成功提交且提交前 fencing 验证通过后，Worker 才能 Ack Delivery。
9. Runtime 失败、取消、Lease 丢失或 commit 失败不得伪造成功 Ack。
10. Stop 必须停止接收、停止续租、处理 in-flight、关闭 Queue，并在有界时间内返回；重复 Stop 不得 panic。
11. P0-08 只能宣称 Queue Contract 层的 ACK/Nack/visibility 语义已验证；没有真实持久化 Queue 时，不宣称进程崩溃后的 durable handoff 已完成。

## 4. 分阶段交付包

四个子阶段必须按顺序执行。前一阶段关闭前，不进入后一阶段；任何阶段都不得跨阶段顺手修复无关问题。

### P0-08A：AgentJob、Tenant DTO 和 Queue Contract

**目标：** 固定跨边界 Job/Queue 公共契约，使用 FakeQueue 证明序列化、租户恢复和基本 Delivery 语义。

**In-scope：**

- `AgentJob`、`JobEnvelope`、`QueueReceipt`、`Delivery`、`NackOptions`。
- `SchemaVersion`、Job 必填字段、deadline、attempt、trace/request/message/execution ID。
- `TenantContextDTO` 和 `TraceContextDTO`；DTO Restore 时调用现有 TenantContext 校验。
- `JobQueue` 接口：Enqueue、Receive、Ack、Nack、ExtendVisibility、Close。
- JSON 或等价稳定编码；顶层字段和 DTO 的一致性检查。
- 仅用于测试的 FakeQueue：阻塞 Receive、visibility timeout 重投、Nack 延迟、Ack 幂等、Close 唤醒。

**Out-of-scope：**

- Redis/Kafka/RabbitMQ/PostgreSQL 持久化 Queue。
- Outbox、Retry Policy、DLQ 业务落库。
- Gateway、Worker、Lease、Runtime、Web 和真实 IM。
- 修改 `tenant`、`storage`、`agent` 的生产契约来迁就 Queue DTO。

**允许文件：** `trpcservice/queue/**` 新增文件和测试；不得修改其他生产包。若现有类型无法表达 DTO，只允许先记录为 blocker，不在 A 阶段扩展前置包。

**定向验收：**

1. Job JSON round-trip 后字段等价，SchemaVersion 正确。
2. TenantContextDTO 能恢复 TenantID、AgentAppID、BindingID、Channel、SessionID、RequestID、MessageID、TraceID、ConfigVersion、权限和 BackendPolicy。
3. 空租户、非法版本、ID 不一致、过期或超长 deadline、Agent/Tenant 不匹配均被拒绝。
4. 序列化结果中不存在 Context、Secret、Token、Password、Runner 或 Provider。
5. Enqueue 成功后可 Receive；未 Ack 的 Delivery 在 visibility timeout 后可再次 Receive。
6. Nack with requeue 按 RetryAfter 重新出现；Ack 后不重复出现；Close 唤醒阻塞 Receive。
7. FakeQueue 不使用外部网络、真实 Redis/PostgreSQL 或真实凭据。

**关闭条件：** P0-08A 的代码、单元测试、FakeQueue 行为测试和 race 测试全部通过；所有必需项在矩阵中为 `verified`。A 阶段不要求真实 Queue 集成测试，真实持久化能力标记为 `deferred`。

### P0-08B：Execution Core、Lease 和 Runtime Boundary

**前置：** P0-08A verified；P0-07 继续保持 blocked，但公共 Runtime 合同可被 Fake/现有实现调用。

**目标：** 在不实现 Worker 消费循环和 Web 接入的前提下，关闭一次 Session 执行的 Lease、取消、Runtime 调用和提交保护语义。

**In-scope：**

- `ExecutionRequest`、`ExecutionState`、failure 分类和 `ExecutionSink` 平台抽象。
- Session Lease Acquire、Renew、Validate、Release 生命周期。
- 从 Job DTO 创建新的 execution Context，注入 TenantContext、deadline、Trace/Request/Message/Execution ID。
- 通过 `agent.AgentFactory.Build` 和 `agent.AgentRuntime.Run` 调用 Runtime。
- Lease 失效取消 Runtime；提交前再次 Validate；旧 fencing token 禁止 Commit。
- Fake LeaseStore、Fake Runtime 和 Fake ExecutionSink 测试替身，若现有测试基础设施没有可复用替身才新增。

**Out-of-scope：**

- Queue Receive/Ack/Nack 循环、Worker 并发控制和 SIGTERM。
- 真实 PostgreSQL Session/Event/Audit/Outbox 事务。
- 修改 `trpcservice/agent` 的 P0-07 生命周期或 framework completion 语义。
- 事件事实源、Memory、Summary、回复发送和 Outbox。

**允许文件：** `trpcservice/execution/**`；必要的测试替身只放在 execution 测试装配中。不得修改 `trpcservice/agent/**`、`trpcservice/storage/**`、`trpcservice/session/**` 的生产实现。

**定向验收：**

1. 成功路径按 Acquire -> Renew/运行 -> Validate -> Commit -> Release 顺序执行。
2. Runtime 输入包含正确 AgentSpec、历史和当前消息，且使用新的 execution Context。
3. Renew 失败、Validate 返回 LeaseLost/FenceRejected、epoch 变化或根 Context 取消时，Runtime 收到取消。
4. Runtime 返回 `ErrProducerIncomplete`、`ErrDrainTimeout`、framework failure、deadline 或 cancellation 时，错误链可用 `errors.Is/As` 检查，且不 Commit 成功结果。
5. Runtime 成功但提交前 Lease 失效时，Sink 不被成功调用。
6. 两个不同 Session 能并行进入 Fake Runtime；同 Session 的第二个执行不能绕过 Lease。

**关闭条件：** execution 单元测试和 `go test -race ./trpcservice/execution/...` 通过；Lease/Runtime/Commit 的必需项全部 verified。P0-07 framework completion blocker 保留为已知限制，不因 Fake Runtime 通过而关闭。

### P0-08C：Worker Delivery、并发和有界退出

**前置：** P0-08A、P0-08B verified。

**目标：** 将 Queue Delivery 和 Execution Core 连接为可停止的 Worker，证明 Ack/Nack、同 Session 串行、不同 Session 并行和 shutdown 语义。

**In-scope：**

- 固定并发数的 Worker consumer。
- Delivery decode/validate、TenantContext restore、Execution 调用、Ack/Nack 分类。
- 同 Session Lease 冲突时不调用 Runtime，按 RetryAfter Nack/requeue。
- 不同 Session 并行；本地优化锁若存在必须有 bounded cleanup，不能替代 Lease。
- Stop/DrainController：停止 Receive、取消或等待 in-flight、停止 renewer、Nack 未完成 Delivery、Close Queue。
- 多次 Stop/SIGTERM 等价调用幂等；阻塞 Receive 能在有界时间内被唤醒。

**Out-of-scope：**

- Gateway 入口、Web 路由、真实信号装配、生产 Queue。
- PostgreSQL Repository、Outbox、DLQ、持久化重启恢复。
- 修改 P0-07 Runtime；不得关闭 framework-owned channel。

**允许文件：** `trpcservice/worker/**` 和 worker 测试；可消费 `queue`、`execution`、`storage.LeaseStore`、`agent.AgentFactory` 的现有接口。不得为 Worker 修改前置包的生产实现。

**定向验收：**

1. Queue 未接受的 Job 不会产生 Gateway 成功 ACK；Worker 失败不成功 Ack。
2. 成功执行只有在 Sink Commit 和 fencing 验证成功后才 Ack。
3. Runtime 错误、Lease 丢失、取消、deadline 和 commit 失败进入明确 Nack/失败状态。
4. 同 Session 两个 Delivery 最多一个 Runtime 进入；另一个 Nack/requeue 或等待，不能双执行。
5. 不同 Session 的两个 Runtime 可同时进入 barrier。
6. Lease renewal 失败会在有界时间内取消 in-flight Runtime。
7. Stop 后不再调用 Receive；in-flight 在 shutdown deadline 内结束或收到取消；renewer 退出；Queue Close 唤醒 Receive。
8. 重复 Stop 不 panic、无重复 close、无死锁；不使用固定 sleep 伪造完成。

**关闭条件：** worker targeted tests、race、vet 和 goroutine/取消路径证据通过。FakeQueue/FakeLeaseStore/FakeRuntime 的所有 shutdown 必需项 verified。生产 Queue 持久化和节点崩溃恢复标记 deferred。

### P0-08D：Gateway 快速 ACK 和最小入口接入

**前置：** P0-08A、P0-08B、P0-08C verified。

**目标：** 将已解析请求转换为 AgentJob，在 Queue 接受后返回快速 ACK，并以最小方式接入现有 Web/cmd；保留同步 Echo 回滚路径。

**In-scope：**

- Transport-neutral `GatewayRequest` 校验和 Job 创建。
- TenantContext 与 AgentSpec 的 TenantID、AgentAppID、版本、Binding、Session、Message、Trace 一致性校验。
- Queue receipt 驱动的 Accepted 响应；Enqueue 失败、取消或 `Accepted=false` 不返回成功。
- `trpcservice/web/server.go` 的最小 Gateway 注入和测试路径，如确有必要。
- `cmd/trpc-service/main.go` 的 Queue/Worker/Execution 装配和有界 SIGTERM 顺序，如确有必要。
- 异步模式与现有同步 Echo 模式的显式区分。

**Out-of-scope：**

- 企业微信、Telegram、微信公众号的真实验签、解密、发送 API。
- API 鉴权、限流、Admin API、Outbox 回复、Dedup Claim 入站事实源。
- 将同步 `/api/chat` 和异步 ACK 混为同一隐式行为。
- 删除或破坏 `MemoryStore -> platform.Runner -> EchoResponder` 回滚路径。

**允许文件：** `trpcservice/gateway/**`；必要时 `trpcservice/web/server.go`、对应测试、`cmd/trpc-service/main.go`、对应测试。若 transport-neutral 契约可独立交付，则不为“看起来接入”而修改 Web。

**定向验收：**

1. Gateway 在 Enqueue 完成且 `Accepted=true` 后才返回 Accepted。
2. Gateway 不调用 Runner，不等待模型或 Tool。
3. Queue 失败、拒绝和请求 Context 取消均返回失败且无后台提交 goroutine。
4. 每次提交生成独立 JobID/ExecutionID；入站 Dedup Claim 留给后续任务。
5. Web 成功响应明确表示 Job accepted，不冒充最终 Agent 回复。
6. SIGTERM 装配顺序为停止入口 -> 停止 Worker Receive -> drain/cancel -> 停止 renewer -> Close Queue -> Shutdown HTTP。

**关闭条件：** gateway targeted tests、如有修改则 web/cmd tests、race、vet、gofmt 和 diff check 通过；同步回滚路径仍通过。真实 IM 和生产持久化标记 deferred。

## 5. 验收矩阵

每个子阶段开始时建立或更新同一格式矩阵。矩阵必须记录命令是否实际执行，不能只写“测试通过”。

| 子阶段/能力 | 代码 | 单元测试 | Fake/契约行为 | 真实依赖 | race | vet/静态边界 | 状态 | 备注/唯一阻塞 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| P0-08A Job/DTO |  |  |  | deferred |  |  | not started |  |
| P0-08A FakeQueue |  |  |  | deferred |  |  | not started |  |
| P0-08B Execution |  |  |  | deferred |  |  | not started |  |
| P0-08B Lease/fencing |  |  |  | P0-06 contract |  |  | not started |  |
| P0-08C Worker/Ack |  |  |  | deferred |  |  | not started |  |
| P0-08C shutdown |  |  |  | deferred |  |  | not started |  |
| P0-08D Gateway |  |  |  | deferred |  |  | not started |  |
| P0-08D Web/cmd |  |  |  | deferred |  |  | not started |  |

矩阵填写规则：

- 任何必需列为空，能力项不能是 `verified`。
- `[no test files]` 只能说明编译通过，不能填入测试通过。
- 真实依赖未配置或测试被 skip 时，真实依赖列写 `not run`，状态保持 `not verified` 或 `blocked`。
- 后续任务明确承接的能力写 `deferred`，不作为当前阶段失败，但不能写成 verified。
- 发现失败后先写入矩阵和唯一阻塞，再决定是否修当前阶段；不重复运行同一命令而不改变环境、代码或问题假设。
- 某阶段关闭后，其矩阵只允许补充证据，不重新打开处理无关问题；发现新需求进入 backlog。

## 6. 分阶段执行和测试命令

### 6.1 通用入口检查

每次开始新子阶段执行一次：

```bash
git status --short
git diff --stat
git diff --check
```

同时读取本阶段允许修改的目录和已有契约测试。禁止以旧计划中的“当前没有某目录”替代源码核对。

### 6.2 P0-08A 命令

编码过程中只运行受影响包：

```bash
gofmt -w trpcservice/queue
go test ./trpcservice/queue/... -count=1 -v
go test ./trpcservice/queue/... -race -count=1
```

关闭 A 前运行一次：

```bash
go test ./trpcservice/queue/... -count=1
go test ./trpcservice/queue/... -race -count=1
go vet ./trpcservice/queue/...
test -z "$(gofmt -l trpcservice/queue)"
git diff --check
```

### 6.3 P0-08B 命令

编码过程中只运行：

```bash
gofmt -w trpcservice/execution
go test ./trpcservice/execution/... -count=1 -v
go test ./trpcservice/execution/... -race -count=1
```

关闭 B 前额外确认 P0-06/agent 边界，不修改其代码：

```bash
go vet ./trpcservice/execution/...
rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/execution
```

`rg` 必须无输出。真实 P0-07 framework completion 仍保持 blocked，不通过全量测试覆盖该限制。

### 6.4 P0-08C 命令

编码过程中只运行：

```bash
gofmt -w trpcservice/worker
go test ./trpcservice/queue/... ./trpcservice/execution/... ./trpcservice/worker/... -count=1 -v
go test ./trpcservice/queue/... ./trpcservice/execution/... ./trpcservice/worker/... -race -count=1
```

关闭 C 前运行：

```bash
go vet ./trpcservice/queue/... ./trpcservice/execution/... ./trpcservice/worker/...
rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/queue trpcservice/execution trpcservice/worker
```

### 6.5 P0-08D 命令

编码过程中只运行受影响路径：

```bash
gofmt -w trpcservice/gateway
if test -d trpcservice/web; then go test ./trpcservice/gateway/... ./trpcservice/web/... -count=1 -v; else go test ./trpcservice/gateway/... -count=1 -v; fi
```

如果修改了 cmd，再增加：

```bash
go test ./cmd/trpc-service/... -count=1 -v
go test ./trpcservice/gateway/... ./trpcservice/web/... ./cmd/trpc-service/... -race -count=1
go vet ./trpcservice/gateway/... ./trpcservice/web/... ./cmd/trpc-service/...
```

关闭 D 时再运行一次全量回归：

```bash
go test ./... -count=1
go test ./... -race -count=1
go vet ./...
test -z "$(gofmt -l .)"
git diff --check
```

全量回归只在 D 关闭时执行一次。若失败，创建一个新的明确阻塞项并定位到受影响包，不回到已关闭子阶段重复做无关审查。

### 6.6 环境和真实依赖规则

P0-08 本阶段使用 Fake Queue、Fake LeaseStore 和 Fake Runtime，不需要真实 Redis/PostgreSQL、API Key、IM 或外部网络。若测试意外连接真实服务，应停止并修正测试装配。

P0-06 Lease contract 已有真实依赖证据时，P0-08 只记录“消费既有契约”；不复制 P0-06 的 Redis/PostgreSQL 真实集成测试。没有 P0-06 验证证据时，不擅自把 P0-08 的 Fake 测试升级为真实基础设施测试。

## 7. 错误、重试和状态映射

| 错误或条件 | Execution 状态 | Delivery 行为 | 是否成功 Ack |
| --- | --- | --- | --- |
| Runtime 成功且 fencing/Commit 成功 | `succeeded` | Ack | 是 |
| Agent 配置、租户或 Job 校验错误 | `permanent_failure` | 不重试或进入明确失败路径 | 否 |
| `context.Canceled` / shutdown | `canceled` | Nack/requeue 或等待 visibility timeout | 否 |
| `context.DeadlineExceeded` | `retryable_failure` 或明确终止 | 按策略 Nack | 否 |
| `storage.ErrLeaseLost` / `ErrFenceRejected` | `lease_lost` | 立即取消 Runtime，Nack/requeue | 否 |
| `agent.ErrProducerIncomplete` / `ErrDrainTimeout` | `retryable_failure` 或 `permanent_failure` | 保留错误链，按有限策略 Nack | 否 |
| framework failure | `retryable_failure` 或 `permanent_failure` | 按错误分类 Nack | 否 |
| ExecutionSink commit 失败 | `retryable_failure` | 不成功 Ack，保留可重投语义 | 否 |
| Ack/Nack/Release 本身失败 | `retryable_failure` 或 `unknown` | 记录 Delivery 未确定完成 | 不得假定是 |

错误分类必须保留原始错误链；不得用普通字符串替换 P0-07/P0-06 sentinel error。P0-08 不定义后续 Retry Policy、DLQ 落库或 Outbox 事实源，只提供当前 Delivery 的明确 Ack/Nack 行为。

## 8. 允许修改和明确禁止

### 8.1 允许修改

- P0-08A：`trpcservice/queue/**`。
- P0-08B：`trpcservice/execution/**`。
- P0-08C：`trpcservice/worker/**`。
- P0-08D：`trpcservice/gateway/**`，必要时 `trpcservice/web/server.go`、对应测试和 `cmd/trpc-service/main.go`、对应测试。
- 各阶段对应的测试文件。

### 8.2 明确禁止

- `trpcservice/agent/**`、`trpcservice/tenant/**`、`trpcservice/storage/**`、`trpcservice/session/**` 的生产逻辑修改。
- `migrations/**`、真实 PostgreSQL Repository、Redis Queue、Outbox Dispatcher、Retry/DLQ 业务落库。
- 真实企业微信、Telegram、微信公众号协议、验签、解密和发送 API。
- API 鉴权、Admin API、Guardrail、Secret Manager、Telemetry、向量存储、对象存储、Docker 部署。
- 把 `context.Context` 直接放入 AgentJob。
- 用进程内 Session mutex 替代 Lease/fencing。
- 关闭不属于 Queue/Worker 自己所有的 channel，特别是 P0-07 framework channel。
- `git commit`、`git push`、`git reset --hard`、`git checkout --`、生产服务访问或敏感信息输出。

## 9. 实际操作范式

### 9.1 每个子阶段的固定循环

1. 读取当前源码、契约和测试；执行入口检查。
2. 建立或恢复本阶段矩阵，只把一个子阶段设为 `in_progress`。
3. 先运行最小单调用/最小 happy path，确认测试 fixture 合法；不要先从并发或全量测试推断根因。
4. 实现一个最小行为闭环。
5. 运行当前包的定向测试；失败时保留完整错误、错误分类和受控诊断。
6. 只有在假设改变、代码改变或环境改变后才重跑同一命令。
7. 每完成一个验收项立即更新矩阵。
8. 所有关闭条件通过后运行该阶段关闭命令，生成阶段关闭报告。
9. 阶段关闭后才启动下一阶段；未关闭则生成 blocker 交接，不继续横向扩展。

### 9.2 诊断优先级

出现“winner 数为 0”、所有并发调用失败或 ACK 数异常时，按以下顺序排查：

1. 测试 Fixture 是否满足现有租户/Agent/Session 校验。
2. 单调用是否成功；错误是否被测试吞掉。
3. 输入 ID、TenantID、SessionID、SchemaVersion 是否一致。
4. 测试是否把“返回当前状态”误判为“本次新建/本次 winner”。
5. 只有确认输入合法且 contract 语义正确后，才判断生产实现有缺陷。

任何并发测试都必须收集每个调用的请求 owner、返回 owner、状态、attempt、fencing token、epoch 和 error；不能只统计 winner 数量。

### 9.3 P0-08 的下一步执行提示词

进入 P0-08A，范围严格限定为 AgentJob、TenantContext DTO、Trace DTO、Job 序列化和 FakeQueue contract。

先读取：

- `docs/plan008v2.md` 的第 2、3、4、5、6、9 节；
- 当前 `trpcservice/tenant` 的 TenantContext 构造、Validate、WithContext；
- 当前 `trpcservice/agent` 的 AgentSpec、AgentInput、Message 公共类型；
- 当前 `trpcservice/storage` 的 LeaseStore/Lease 类型和已有合法测试 fixture；
- 当前 `trpcservice/web`、`cmd` 只做状态核对，不修改。

本轮只允许修改 `trpcservice/queue/**`。先建立 P0-08A 验收矩阵并将 P0-08A 设为唯一 `in_progress`；P0-08B/C/D 保持 `not started`。

先用现有合法 TenantContext fixture 完成单调用 round-trip 和 Enqueue/Receive 诊断，再补并发、visibility timeout、Nack/requeue、Ack 幂等和 Close 唤醒测试。所有测试必须保留完整错误，不得用放宽断言、sleep 或吞错推进。

本轮不实现 Gateway、Worker、Execution、Lease renewal、SIGTERM、Web 接入、真实 Queue、Redis/PostgreSQL、Outbox、Retry/DLQ 或 P0-07 Runtime 修改。测试只运行 `trpcservice/queue/...` 的定向命令；P0-08A 全部关闭条件通过后，生成关闭报告并停止，等待下一阶段入口。

### 9.4 阻塞交接格式

如果无法继续，输出以下固定信息，不重复运行同一命令：

```text
当前子阶段：P0-08X
唯一阻塞：<一个可验证的问题>
已完成矩阵项：<代码/单测/Fake/真实依赖/race/vet>
未运行：<命令和原因>
已确认事实：<最小诊断结果>
未修改范围：<明确列出>
下一步最小动作：<一个动作>
```

## 10. 阶段关闭报告格式

每个子阶段关闭时必须输出：

```text
阶段：P0-08X
状态：verified
变更文件：<按文件列出>
已通过命令：<实际执行的命令和结果>
未运行命令：<明确写出 deferred 或环境原因>
矩阵结论：<每个必需项的状态>
已知限制：<例如 P0-07 framework blocker、Fake 非持久化>
下一阶段入口：<P0-08Y 的唯一启动条件>
```

关闭报告生成后，允许停止当前轮。未生成关闭报告时，不能说“阶段完成”，只能说 `in_progress`、`not verified` 或 `blocked`。

## 11. P0-08 完成定义

只有 P0-08A、B、C、D 依次生成 `verified` 关闭报告，且 D 阶段的全量回归通过，P0-08 才能在项目状态中标记为完成。最终必须具备：

1. Job/Queue DTO 可序列化、可校验、可恢复，且不携带 Context 或敏感数据。
2. Gateway 只在 Queue receipt accepted 后快速 ACK，不等待 Agent 执行。
3. Worker 能恢复租户和 trace 关联，调用平台 AgentFactory/AgentRuntime。
4. 同 Session 由 Lease/fencing 保证单 owner，不同 Session 可以并行。
5. Lease 丢失、Runtime 不完整、取消、deadline 和 commit 失败都不会成功 Ack。
6. Runtime 成功后提交前再次校验 fencing；旧 Worker 不能提交结果。
7. Worker Stop/SIGTERM 在有界时间内停止接收、停止续租、处理 in-flight 并关闭自有 Queue。
8. Queue Contract 层 ACK/Nack/visibility 证据完整；真实持久化 Job handoff、Outbox、真实 IM 和 P0-07 framework completion 仍明确列为后续/已知限制。
9. `go test ./...`、`go test ./... -race`、`go vet ./...`、gofmt、diff check 和边界扫描在 D 阶段实际运行并通过。
10. `docs/project-status.md`、`docs/implementation-plan.md` 等权威状态文件是否更新，必须作为后续文档同步任务处理；本次 P0-08 计划修订不自动修改它们。

## 12. 风险、回滚和后续任务

### 12.1 已知风险

- P0-07 framework completion blocker 不会因 Worker drain 或 Fake Runtime 测试消失。
- 内存 FakeQueue 在进程崩溃时会丢失数据；P0-08 只能证明 Queue Contract，不证明 durable handoff。
- ExecutionSink 为 Fake/内存实现时，commit 与 Ack 的跨进程原子性留给后续事实源和 Outbox 任务。
- Lease 续租和提交之间仍存在故障窗口；提交前 Validate 和后续事实源 fencing 是必要但不是持久化事务本身。
- 如果现有 P0-06 LeaseStore 契约不足以表达某项 P0-08 语义，必须先记录 blocker，不能直接修改 P0-06 生产代码扩大范围。

### 12.2 回滚

P0-08 不新增数据库迁移，因此回滚不涉及数据回滚：

- 保留同步 `platform.Runner`/Echo fallback，通过显式运行模式禁用异步路径。
- 停止新 Worker；未完成 Delivery 依赖 visibility timeout 或 FakeQueue 重投。
- 只按文件级审查回滚 P0-08 允许范围内的新增或修改，不触碰 P0-06/P0-07 前置实现。
- 不使用 `git reset --hard` 或 `git checkout --` 覆盖用户工作区修改。

### 12.3 后续任务边界

- P0-09：真实 PostgreSQL Repository、Session/Event/Audit/Outbox 事务和 TenantContext 二次校验。
- P0-10/P0-11：持久化 Job Queue、Outbox Dispatcher、Retry Policy、DLQ、重启恢复和外部发送。
- P1：真实 Web 鉴权、企业微信/Telegram 协议、Telemetry、治理和生产部署。
- P0-07：只有 tRPC-Agent-Go 提供公开稳定的 framework completion/wait 能力后，才重新评估 blocker。
