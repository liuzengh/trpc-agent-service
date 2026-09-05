# P0-07 修补至 P0-08 全阶段完成实施计划

## 0. 给全新上下文 pi 的完整背景

### 0.1 仓库、工作区和执行规则

仓库路径：`C:/Documents/trpc-agent-service`。

下一次 pi 启动时不能假设当前工作区干净，也不能只依据本计划推断代码状态。必须先执行只读入口检查，并保留用户和前序阶段已有修改：

```text
git status --short
git diff --stat
git diff --check
```

不得执行以下操作：

- `git reset --hard`；
- `git checkout --`；
- 清理或覆盖未跟踪文件；
- `git commit`；
- `git push`；
- 访问生产服务；
- 读取、输出或记录密钥、Token、密码、Authorization 或生产日志。

编辑时使用 `apply_patch`。Go 文件修改后执行 `gofmt`。只修改本阶段允许范围内的文件；如果发现跨阶段问题，记录为后续项或唯一 blocker，不顺手扩大范围。

### 0.2 权威材料优先级

全新上下文 pi 必须按以下顺序重新读取材料：

1. 当前源码、当前测试和当前工作区状态；
2. 当前阶段关闭报告或阻塞交接报告；
3. `docs/project-status.md`；
4. `docs/ARCHITECTURE.md`；
5. `docs/implementation-plan.md`；
6. `docs/P0-07阻塞交接结论.md`；
7. `docs/plan008v2.md`；
8. `docs/plan008.md`；
9. 其他历史计划和阶段文档。

如果文档与源码或阶段关闭报告冲突，以当前源码、实际测试结果和最新交接报告为准。不要为了消除冲突而改写历史计划。

本计划不修改：

- `docs/plan007v1`；
- `docs/plan008.md`；
- `docs/plan008v2.md`；
- `docs/P0-07阻塞交接结论.md`。

实施期间状态只通过阶段报告和最终交付说明表达，除非用户另外明确要求同步文档。

### 0.3 P0-08 当前已完成基线

用户已经报告 P0-08A 和 P0-08B 完成，其中 P0-08B 状态为 `verified`。

P0-08A 已有 `trpcservice/queue/**` 工作区文件，包含 AgentJob、JobEnvelope、TenantContextDTO、TraceContextDTO、FakeQueue、序列化/反序列化、Ack/Nack、visibility timeout 和 Close 语义。P0-08A 的具体文件必须由新 pi 重新读取确认，不能仅依据“目录存在”判定已验证。

P0-08B 已有：

- `trpcservice/execution/execution.go`；
- `trpcservice/execution/execution_test.go`。

P0-08B 公开了 `ExecutionRequest`、`ExecutionState`、`FailureClass`、`ExecutionError`、`ExecutionResult`、`ExecutionSink`、`ExecutionCommit` 和 `Executor`。

当前已报告的 P0-08B 编排顺序为：

```text
Validate Job/Agent
  -> Restore TenantContext
  -> create execution Context
  -> Lease Acquire
  -> Lease Renew / Runtime.Run
  -> stop renewal
  -> Lease/epoch/fencing Validate
  -> ExecutionSink.Commit
  -> bounded Release
```

P0-08B 已报告覆盖：

- TenantContext、Trace、deadline 恢复；
- AgentSpec、AgentInput、历史消息和 Runtime Context；
- Lease Acquire/Renew/Validate/Release；
- Lease lost、fencing reject、epoch reject；
- `ErrProducerIncomplete`、`ErrDrainTimeout`、framework failure、context cancellation、deadline 的错误链；
- 提交前 fencing 校验；
- Runtime 失败不 Commit；
- Commit 失败不伪造成功；
- Release 失败保留错误链；
- 同 Session 串行、不同 Session 并行；
- `trpcservice/execution` 不导入 tRPC-Agent-Go framework 类型。

用户报告的 P0-08B 命令为：

```text
go test ./trpcservice/execution/... -count=1 -v
go test ./trpcservice/execution/... -race -count=1
go vet ./trpcservice/execution/...
rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/execution
test -z "$(gofmt -l trpcservice/execution)"
git diff --check
```

但这些是用户提供的历史执行证据。新 pi 在进入下一阶段时必须核对工作区是否仍与报告一致；不需要无理由重做 P0-08A/B，也不能把历史报告当成当前源码证据而跳过入口审计。如果入口审计发现代码发生回归，只建立一个明确 blocker，先处理一致性，不直接开始 C/D。

P0-08B 仍使用 FakeLeaseStore、FakeAgentFactory/FakeRuntime、FakeExecutionSink。它没有证明：

- 真实 Redis/PostgreSQL Lease 集成；
- 真实持久化 Queue；
- 真实 Session/Event/Audit/Outbox；
- 崩溃恢复；
- 真实 Worker 接入 Runner；
- framework 内部 goroutine 的全局退出。

这些不能在最终 P0-08 报告中伪装成已完成。

### 0.4 P0-07 原始状态和新的 framework 事实

`docs/P0-07阻塞交接结论.md` 原先把 P0-07 冻结为 `blocked`，理由是认为 tRPC-Agent-Go v1.11.2 没有公开、稳定、合法的 framework completion/wait API。

该判断需要重新定级。当前研究确认，v1.11.2 没有名为 `framework.Completion()` 或 `framework.Wait()` 的统一顶层 API，但存在以下公开能力：

- `event.Event.IsRunnerCompletion()`：执行流的公开终止事件判断；要求 `Done == true` 且 `Object == model.ObjectTypeRunnerCompletion`；
- `agent.Agent.Run(...)` / `runner.Run(...)`：返回 `<-chan *event.Event`，调用方可以消费事件；
- `Invocation.AddNoticeChannelAndWait(...)` 与 `Invocation.NotifyCompletion(...)`：自定义 notice 通知，但需要直接持有 Invocation；
- `taskrun.Controller.Wait(ctx, runID)`：taskrun 控制面等待，但只适用于 taskrun 运行模型；
- `taskrun/inprocess.Service.Wait(...)`：in-process taskrun 的 waiter 实现。

锁定 v1.11.2 源码的关键证据：

- `event/event.go:395` 附近的 `Event.IsRunnerCompletion`；
- `runner/runner.go:1496-1503` 附近的 event loop defer、completion 发送和输出 channel close；
- `runner/runner.go:3045` 附近的 completion event 构造；
- `runner/runner.go:3150-3168` 附近的 completion event 输出；
- `agent/invocation.go:2275` 附近的 `AddNoticeChannelAndWait`；
- `agent/invocation.go:2333` 附近的 `NotifyCompletion`；
- `agent/taskrun/types.go:152` 附近的 `Controller.Wait`；
- `agent/taskrun/inprocess/service.go:317` 附近的 `Service.Wait`。

上游版本为 tRPC-Agent-Go `v1.11.2`，tag 对应提交 `5a0030b628a5bd93c8c5a30b1451f6fdd9d6740e`。下一次 pi 必须再检查当前 `go.mod` 和 module source，确认依赖没有升级。

### 0.5 P0-07 新的准确边界

当前 Runtime 通过 `runner.RunWithMessages` 获得 framework event channel。新的 P0-07 目标不是寻找不存在的统一顶层 Wait，而是正确使用公开 event-stream completion 协议：

```text
framework event stream
  -> evt.IsRunnerCompletion()
  -> drain remaining observable events / channel close
  -> adapter pump completion
  -> bounded cleanup
  -> Runner.Close
```

必须严格区分：

- `frameworkCompletionObserved`：收到了合法的 `runner.completion` event；
- `frameworkChannelClosed`：framework-owned event channel 已正常关闭；
- `pumpCompleted`：adapter 自己的 pump goroutine 已退出；
- adapter-owned channel close：adapter 只关闭自己创建的 channel。

新的事实解除的是“没有可观察 Runner completion signal”这一 blocker。仍然存在的 residual risk 是：v1.11.2 没有统一公开 API 来证明所有 framework 后台 goroutine、外部异步任务和资源都已全局退出。completion event 加上 framework channel close 可以证明当前 Runner event stream 的终止协议，但不能声称 framework 全部 goroutine 无泄漏。

本计划将 P0-07 重新打开为 `in_progress`。P0-07 的 scoped Runtime completion/drain/pump 通过后，可标记为 `verified` 或 `passed with residual risk`；不得把 `pumpCompleted` 写成 framework completion，也不得把 `Runner.Close` 写成全局 Wait。

### 0.6 P0-08 原计划的范围和编号说明

`docs/plan008.md` 和 `docs/plan008v2.md` 将本轮 P0-08 定义为一个执行编排切片：

```text
已解析请求
  -> Gateway 校验
  -> Queue Enqueue
  -> Queue Accepted
  -> 快速 ACK
  -> Worker Receive
  -> TenantContext/Trace 恢复
  -> Session Lease
  -> Runtime
  -> fencing Validate
  -> Commit
  -> Ack/Nack
  -> Release
```

仓库总计划可能把 P0-08 描述为 Session Execution Orchestrator，并把 Gateway/Queue/Worker 放入其他编号；本轮以用户明确要求和 `plan008v2.md` 的 A/B/C/D 阶段为执行范围，不修改编号文档，也不提前实现 P0-09/P0-10/P0-11。

P0-08 的范围不包括：

- 真实 PostgreSQL Repository；
- Redis/Kafka/RabbitMQ/PostgreSQL 持久化 Queue；
- Outbox Dispatcher；
- Retry Policy/DLQ 业务落库；
- 真实企业微信、Telegram 或其他 IM 协议、验签、解密、发送；
- API 鉴权、Admin API、Telemetry、Guardrail、Secret Manager；
- 向量存储、对象存储、Docker/生产部署；
- 修改 P0-06 LeaseStore 生产契约；
- 修改 P0-07 之外的基础设施来伪造 completion。

## 1. 工作流程决定

需要调整工作流程，但不需要回退或重做已完成的 P0-08A/P0-08B。

新的顺序固定为：

```text
P0-08A verified
  -> P0-08B verified
  -> P0-07 completion 协议修补和独立验收
  -> P0-08C Worker
  -> P0-08D Gateway/最小入口
  -> P0-08 全量回归和最终关闭报告
```

阶段状态规则：

- 每次只有一个阶段是 `in_progress`；
- 已关闭阶段不因后续需求重新打开；
- P0-08A、P0-08B 维持 `verified`，除非入口审计发现真实回归；
- P0-07 是当前唯一 `in_progress`；
- P0-08C、P0-08D 为 `not started`；
- 测试未运行、真实依赖缺失或证据不足时，不能写 `verified`；
- 每个阶段最多保留一个当前 blocker，其余问题进入 backlog、deferred 或 residual risk。

不要直接进入 P0-08C：虽然原 `plan008v2.md` 允许 C 在 P0-07 blocked 状态下消费已交接的公共 Runtime 合同，但本次用户要求的是“从 P0-07 修补到 P0-08 完成”的完整路线，且 C 将首次把 Runtime 接入 Worker 编排，因此先完成 P0-07 的公开 completion 协议验收，避免把 P0-08B Fake Runtime 证据扩大解释为真实生命周期证据。

## 2. 阶段 0：入口审计

### 目标

确认当前源码、P0-08A/B 报告和 workspace 一致，并建立唯一阶段状态。

### 必须读取

- `docs/P0-07阻塞交接结论.md`；
- `docs/plan008.md` 背景、边界、P0-08C/P0-08D；
- `docs/plan008v2.md` 第 1、2、3、4、5、6、7、9、10、11、12 节；
- 当前 `trpcservice/agent/**`；
- 当前 `trpcservice/queue/**`；
- 当前 `trpcservice/execution/**`；
- 当前 `trpcservice/storage/**`、`trpcservice/tenant/**`、`trpcservice/session/**`；
- 当前 `trpcservice/web/**` 和 `cmd/trpc-service/**`，只读核对；
- 当前 `go.mod` 和 `go.sum`，确认 framework 仍为 `v1.11.2`。

### 必须执行

```text
git status --short
git diff --stat
git diff --check
```

根据当前实际目录检查 P0-08A/B 文件是否仍然存在。入口审计不修改源码和文档，不运行全量测试制造无关循环。

### 入口输出

记录如下状态：

```text
P0-08A: verified，来源为既有关闭报告 + 当前源码一致性检查
P0-08B: verified，来源为既有关闭报告 + 当前源码一致性检查
P0-07: in_progress
P0-08C: not started
P0-08D: not started
```

若 P0-08A/B 与报告不一致，只记录一个 blocker：具体文件、具体行为、具体缺失证据和下一步最小动作。

## 3. 阶段 1：P0-07 Runtime completion 协议修补

### 3.1 允许修改范围

优先只修改：

- `trpcservice/agent/runtime.go`；
- `trpcservice/agent/runtime_lifecycle_test.go`；
- 必要时修改 `trpcservice/agent/runner_integration_test.go`；
- 必要时修改现有 Provider/Tool 相关测试。

禁止修改 Queue、Execution、Worker、Gateway、Web、CMD、Storage、Tenant、Session 生产逻辑。禁止修改 module cache、升级 framework、导入私有 API、访问 framework 私有字段或内部 channel。

### 3.2 Runtime 状态协议

在现有生命周期实现中新增或明确一个 completion event 状态，例如 `frameworkCompletionObserved`。状态语义必须独立：

- `frameworkCompletionObserved`：drain 读到且确认 `evt.IsRunnerCompletion()`；
- `frameworkChannelClosed`：pump 观察到 framework event channel 正常关闭；
- `pumpCompleted`：adapter pump goroutine 已退出；
- `done`/pumped channel：adapter 自己创建并管理的完成/事件 channel。

不使用 `pumpCompleted` 替代 completion event。adapter 不关闭 framework-owned channel。

### 3.3 正常成功路径

framework v1.11.2 的 event loop 在退出 defer 中先发送 completion event，然后执行清理并关闭输出 channel。因此收到 completion event 不是立即结束 drain 的理由。

正常成功必须同时具备：

1. 收到 `evt.IsRunnerCompletion()` 为真的事件；
2. 事件流中的业务、assistant、Tool、error 事件保持既有顺序和语义；
3. framework-owned event channel 正常关闭；
4. adapter pump 在 bounded window 内退出；
5. 没有 context、framework、provider、tool、Stop、Close 或 cleanup 错误。

channel 先关闭但没有 completion event 时，不能报告成功，应返回 `ErrProducerIncomplete` 或等价明确不完整错误。completion event 已收到但 channel 没有在 bounded window 内关闭时，也不能无限等待，应取消 adapter pump、有限等待并返回 `ErrProducerIncomplete`，同时保留已存在的错误。

### 3.4 取消、timeout 和 Close

- 请求 context 取消或 deadline 在 completion event 前发生时，保留 `context.Canceled` 或 `context.DeadlineExceeded`；取消路径不强制要求 completion event。
- drain timeout 时取消 request-scoped pump，使用已有 bounded cleanup；不能通过 sleep 或无限等待解决。
- `Runner.Close` 仍然是关闭动作，不描述为等待所有 framework 内部 goroutine 的公共句柄。
- Runtime defer 中先完成 adapter pump 的取消和 bounded wait，再执行 `Close`；仍不关闭 framework-owned event channel。
- completion event 的 `Response.Error` 若包含 framework 终止错误，必须安全分类，不输出敏感信息。

### 3.5 错误链

保留 `%w` 和 `errors.Join`，验证以下错误可以同时被定位：

- `ErrDrainTimeout`；
- `ErrProducerIncomplete`；
- `ErrFrameworkFailure`；
- `context.Canceled`；
- `context.DeadlineExceeded`；
- Tool/Provider 原始 sentinel；
- Stop error；
- Close error；
- completion event 中的结构化 error。

如果 completion event 只能携带 framework/model 层 type/code/message，必须区分“结构化错误已保留”和“原始 Go sentinel 已保留”，不能伪造 cause。

### 3.6 P0-07测试矩阵

必须补充或核对：

1. 合法 completion event -> channel close -> pump exit -> Runtime 成功。
2. `Done=false` 或 `Object != runner.completion` 不被误判为 completion。
3. channel close 但无 completion event -> incomplete/failure，不能成功。
4. completion event 已观察但 channel 不关闭 -> bounded 返回，不能永久等待。
5. completion event 后事件顺序和尾部 drain 符合设计。
6. 32 事件背压下 completion event 可被消费；取消时 pump bounded 退出。
7. framework-level Tool failure/cancellation 真实路径或稳定 framework fixture；若成本/fixture 不足，明确登记 `not verified`，不能将 callback-only 测试升格。
8. completion/framework/Stop/Close/drain timeout 的 `errors.Is` / `errors.As` 组合证据。
9. 真实 OpenAI-compatible 两轮 Tool 测试包含合法 `IsRunnerCompletion()` 终止事件，并保持 assistant tool call、Tool result、Tool started/completed 顺序。
10. Provider/Agent 并发配置隔离没有因为 completion state 引入共享可变状态。

不得使用固定 sleep、goleak、放宽 timeout、忽略 goroutine 或忽略尾部错误来关闭测试。

### 3.7 P0-07阶段命令

```text
gofmt -w trpcservice/agent
go test ./trpcservice/agent -count=1 -v
go test ./trpcservice/agent -race -count=1 -v
go vet ./trpcservice/agent
test -z "$(gofmt -l trpcservice/agent)"
git diff --check
```

执行边界扫描，确认 tRPC-Agent-Go 类型仍只在 `trpcservice/agent` 内部，Queue、Execution、Worker、Gateway、Web、Storage、Tenant、Session 不出现 framework import。

### 3.8 P0-07关闭标准

满足以下条件后，P0-07 scoped Runtime 能力可标记 `verified` 或 `passed with residual risk`：

- completion event 识别真实有效；
- channel close 和 pump completion 语义未混淆；
- 正常/取消/timeout/backpressure/Stop/Close 路径 bounded；
- 错误链和真实 Tool/Provider/Runner 证据通过；
- targeted test、race、vet、gofmt、diff check 和边界扫描通过。

最终报告必须保留 residual risk：completion event/channel close 证明 Runner event stream 的终止协议，但不证明 v1.11.2 所有 framework 后台 goroutine 已全局退出。如果 completion event 在当前路径无法稳定产生，则 P0-07 保持 `blocked`，只交接一个具体 blocker，不开始 P0-08C。

## 4. 阶段 2：P0-08C Worker Delivery、并发和有界退出

### 4.1 允许修改范围

只新增或修改：

- `trpcservice/worker/**`；
- Worker 测试和必要的测试装配。

不得修改 `agent`、`queue`、`execution`、`storage` 的生产代码来迁就 Worker。Worker 不导入 tRPC-Agent-Go。

### 4.2 Worker接口和所有权

采用固定并发 Worker pool，避免每个 Delivery 派生无界 goroutine。Worker 持有：

- `queue.JobQueue`；
- `execution.Executor`；
- 固定并发数；
- visibility timeout；
- Lease/Execution 参数；
- root context/cancel；
- consumer/worker WaitGroup；
-幂等 Stop 状态。

采用：

```go
Start(ctx context.Context) error
Stop(ctx context.Context) error
```

`Start` 重复调用不得产生第二组 consumer。`Stop` 第一次调用停止 Receive、取消 in-flight、等待 worker/renewer、bounded Nack 未完成 Delivery、关闭 Worker 自有 Queue；重复 Stop 不重复 close、不 panic、不死锁。Stop 使用调用方 deadline，不能无限等待。

Worker 只关闭自己拥有的 Queue/Worker 资源，不关闭 framework-owned channel。

### 4.3 Delivery执行协议

固定流程：

```text
Receive
  -> Decode/Validate Envelope
  -> Restore TenantContext
  -> 校验 tenant/agent/binding/session/request/message/trace
  -> 新建 execution Context
  -> execution.Executor.Execute
  -> Commit 成功后 Ack，否则 Nack
```

成功 Ack 的必要条件：

1. Job 合法；
2. Lease Acquire 成功；
3. Lease renewal 没有失效；
4. Runtime 成功；
5. 提交前 fencing Validate 成功；
6. ExecutionSink.Commit 成功；
7. Ack 自身成功。

错误映射固定为：

- decode/schema/tenant/agent 永久校验错误：`Nack(Requeue=false)`，不成功 Ack；
- Lease conflict/lost/fencing/epoch rejection：取消 Runtime，`Nack(Requeue=true)`，使用 bounded RetryAfter；
- `ErrProducerIncomplete`、`ErrDrainTimeout`、framework/provider/tool retryable failure：保留错误链并 `Nack(Requeue=true)`；
- cancellation/shutdown：不成功 Ack，优先使用独立 bounded cleanup context requeue；
- deadline：按 retryable failure 处理，不能成功 Ack；
- Commit 失败：不 Ack，requeue 并保留 Commit cause；
- Release 失败：不覆盖主错误，组合错误链，不成功 Ack；
- Ack/Nack 失败：状态不确定，不能假定 Delivery 已完成。

本阶段不实现 DLQ、持久化失败事实或生产 Retry Policy。

### 4.4 并发、Lease和visibility

- 同一 Session 的正确性由 `LeaseStore` 和 fencing 保证，不使用进程内 mutex 作为唯一保障。
- 同一 Session 第二个 Delivery 获取不到 Lease 时不进入 Runtime，按 bounded 延迟 requeue。
- 不同 Session 必须可同时进入 Fake Runtime barrier。
- Renew 失败必须取消 execution Context，旧 Runtime 返回后不能 Commit。
- Worker 重新创建 Execution Context，不直接携带 Queue context；恢复 deadline、TenantContext 和 correlation metadata。
- Visibility extension 只由当前 Delivery owner 执行，并在 Stop/Execute 返回后退出。

### 4.5 Shutdown协议

固定顺序：

```text
停止 Receive 新 Delivery
  -> 取消 Worker root/in-flight execution
  -> 停止 Lease renewal/visibility extension
  -> 等待 in-flight 至 shutdown deadline
  -> 对未完成 Delivery bounded Nack/requeue
  -> Close Queue
  -> 返回组合 shutdown error
```

用 context、channel 和 WaitGroup 协调，不使用固定 sleep。阻塞 Receive 必须能被 context cancel 或 Queue.Close 唤醒。

### 4.6 P0-08C测试

至少覆盖：

1. Queue 未接受的 Job 不产生成功 ACK。
2. 成功顺序为 Execute -> Commit -> Ack -> Release。
3. Commit 失败不 Ack。
4. P0-07 sentinel、framework failure、cancel、deadline 进入明确 Nack/失败状态。
5. 同 Session 至多一个 Runtime 进入，第二个 requeue。
6. 不同 Session 可以同时进入 barrier。
7. Renew/Validate/fencing 失败取消 Runtime，阻断 Commit/Ack。
8. visibility timeout、Ack/Nack failure、重复 Delivery 可判定。
9. Stop 后不再 Receive；in-flight bounded 完成或取消；renewer 退出；Queue Close 唤醒 Receive。
10. 重复 Stop 不 panic、无重复 close、无死锁。
11. `worker`、`queue`、`execution` 不导入 framework。
12. 错误通过 `errors.Is/As` 保留，不使用字符串匹配替代。

### 4.7 P0-08C命令

```text
gofmt -w trpcservice/worker
go test ./trpcservice/queue/... ./trpcservice/execution/... ./trpcservice/worker/... -count=1 -v
go test ./trpcservice/queue/... ./trpcservice/execution/... ./trpcservice/worker/... -race -count=1
go vet ./trpcservice/queue/... ./trpcservice/execution/... ./trpcservice/worker/...
test -z "$(gofmt -l trpcservice/queue trpcservice/execution trpcservice/worker)"
rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/queue trpcservice/execution trpcservice/worker
git diff --check
```

rg 必须无输出。P0-08B 只作为依赖回归，不因 C 阶段无关问题重写其设计。

### 4.8 P0-08C关闭标准

Worker targeted test、race、vet、gofmt、diff check、边界扫描和所有 shutdown/Lease/Ack/Nack 矩阵项为 `verified`。真实 Queue、崩溃恢复、DLQ、Outbox、Retry Policy 继续 `deferred`。

## 5. 阶段 3：P0-08D Gateway快速ACK和最小入口

### 5.1 允许修改范围

主要新增：

- `trpcservice/gateway/**`；
- Gateway 测试。

只有确有必要才修改：

- `trpcservice/web/server.go`；
- Web 测试；
- `cmd/trpc-service/main.go`；
- cmd 测试。

保留当前 `MemoryStore -> platform.Runner -> EchoResponder` 同步回滚路径，不把 `/api/chat` 的同步最终回复悄悄改为异步 accepted 语义。

### 5.2 Gateway请求和职责

使用 transport-neutral 请求，不实现真实 IM 验签、解密、发送：

```go
type GatewayRequest struct {
    TenantContext tenant.TenantContext
    Agent         agent.AgentSpec
    History       []agent.Message
    Input         agent.Message
    Deadline      time.Time
}
```

Gateway 负责：

1. 校验 TenantContext、AgentSpec、SessionID、MessageID、RequestID、TraceID 和 deadline。
2. 校验 TenantID、AgentAppID、Agent Version、Binding、Channel、Session 和 correlation IDs 一致。
3. 生成独立 JobID/ExecutionID；优先复用仓库现有 ID helper 或已有 `github.com/google/uuid`，不引入不必要依赖。
4. 转换 TenantContextDTO、AgentRefDTO、MessageDTO、TraceContextDTO 和 AgentJob。
5. 调用 `JobQueue.Enqueue`。
6. 仅当 `QueueReceipt.Accepted == true` 时返回成功。
7. 返回 Accepted、JobID、RequestID、TraceID、ReceiptID 等 metadata，不等待 Worker、Runner、Model 或 Tool。

Gateway 不负责 Lease、Runtime、Session Event、Assistant Commit、IM 发送、Dedup Claim、鉴权、限流、Admin API 或 Outbox。

### 5.3 Web/CMD接入规则

如果不破坏现有同步路径，可以：

- 增加显式 async 模式或测试入口；
- 把已解析请求交给 Gateway；
- 成功响应明确表示 Job accepted，不表示最终 Agent 回复；
- cmd 装配 Queue、Execution、Worker、Gateway；
- SIGTERM 顺序为：停止入口 -> 停止 Worker Receive -> drain/cancel -> 停止 renewal -> Close Queue -> Shutdown HTTP。

如果修改 Web 会引入协议歧义，则只交付 transport-neutral Gateway contract 和测试，Web 接入标记为 deferred，不为“看起来接入”改变现有 `/api/chat`。

### 5.4 P0-08D测试

至少覆盖：

1. 合法请求生成可验证 AgentJob。
2. Tenant/Agent/Binding/Session/Message/Trace/version 不一致在 Enqueue 前拒绝。
3. Enqueue 未接受不返回成功 ACK。
4. Accepted=true 后只返回 accepted metadata，不调用 Runtime。
5. Queue failure、拒绝、cancel 不留下脱离请求的后台提交 goroutine。
6. 每次请求产生独立 JobID/ExecutionID。
7. Job 中无 `context.Context`、Runner、Secret、API key、Authorization 或 mutable Provider config。
8. Web 若接入，成功响应表示 accepted；同步 Echo 路径仍通过。
9. cmd 若接入，SIGTERM、重复关闭和 bounded shutdown 通过。
10. Gateway/Worker/Queue/Execution/Storage 无 framework import。

### 5.5 P0-08D命令

根据实际目录执行：

```text
gofmt -w trpcservice/gateway
go test ./trpcservice/gateway/... -count=1 -v
go test ./trpcservice/gateway/... ./trpcservice/web/... -count=1 -v
go test ./trpcservice/gateway/... ./trpcservice/web/... ./cmd/trpc-service/... -race -count=1
go vet ./trpcservice/gateway/... ./trpcservice/web/... ./cmd/trpc-service/...
test -z "$(gofmt -l trpcservice/gateway trpcservice/web cmd/trpc-service)"
rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/gateway trpcservice/queue trpcservice/execution trpcservice/worker
git diff --check
```

如果 `web` 或 cmd 没有本阶段修改，命令按实际影响范围收窄，并在报告中写明未运行原因。`[no test files]` 只能说明编译，不得写成测试通过。

### 5.6 P0-08D关闭标准

Gateway targeted test、必要的 Web/cmd test、race、vet、gofmt、diff check、边界扫描通过；同步回滚路径通过；真实 IM、鉴权和持久化能力继续 deferred。

## 6. 阶段 4：P0-08最终回归和完成报告

P0-08D 关闭条件全部通过后，才执行一次全量回归：

```text
go test ./... -count=1
go test ./... -race -count=1
go vet ./...
test -z "$(gofmt -l .)"
git diff --check
```

执行边界扫描，确认：

- `queue`、`execution`、`worker`、`gateway`、`web`、`tenant`、`storage`、`session` 不导入 tRPC-Agent-Go；
- framework 类型只留在 `trpcservice/agent`；
- 没有关闭 framework-owned channel 的代码；
- `pumpCompleted` 没有被作为 framework completion 的 API 或文字证据；
- 没有新增固定 sleep、goleak 规避、私有字段访问或忽略残留 goroutine 的代码。

只有 P0-08A、B、C、D 都有独立 `verified` 关闭报告，且 D 阶段全量回归通过，P0-08 才能标记完成。

P0-08 最终报告必须分别列出：

- A/B/C/D 状态；
- 变更文件；
- 实际执行命令和结果；
- 未运行命令及原因；
- Fake 和真实依赖边界；
- 错误分类和 `errors.Is/As` 证据；
- Lease/fencing 证据；
- Ack/Nack/visibility 证据；
- Worker shutdown 证据；
- Gateway fast ACK 证据；
- 唯一 blocker、deferred 和 residual risk；
- P0-09/P0-10/P0-11 后续边界。

P0-07 报告必须独立于 P0-08 报告，明确：

- scoped Runtime completion/drain/pump 协议是否通过；
- framework event stream 的公开 completion 证据；
- `Runner.Close` 不是全局 Wait；
- v1.11.2 内部全部 goroutine 退出仍未被证明；
- 是否需要未来 framework 升级后单独补充 run-level Wait/Done 验证。

## 7. 明确禁止的规避方案和范围扩张

全流程禁止：

- 把 `pumpCompleted` 改写或描述成 framework completion；
- 把 P0-08B Fake Runtime 通过描述为真实 framework completion 证据；
- 访问 framework 私有字段或内部 channel；
- 通过固定 sleep、放宽 drain timeout、吞掉错误、忽略残留 goroutine 或 goleak 关闭阶段；
- 把 `taskrun.Controller.Wait` 强行引入当前直接 Runner 架构；
- 关闭 framework-owned event channel；
- 修改 `docs/plan007v1`、覆盖工作区用户改动、回滚、commit 或 push；
- 提前实现真实 PostgreSQL Repository、持久化 Queue、Outbox、DLQ、Retry Policy、真实 IM、鉴权、Telemetry、向量/对象存储或治理；
- 修改 P0-06、Tenant、Storage、Session 生产契约来迁就 P0-08。

## 8. 最终执行决策

当前正确路线是：

1. 先审计工作区和 P0-08A/B 现状，不重做已关闭阶段。
2. 重新打开 P0-07，使用公开 `IsRunnerCompletion()` 补齐 Runtime completion event、channel close、pump cleanup 和错误链协议。
3. P0-07 scoped 验收通过后，进入 P0-08C Worker。
4. P0-08C 通过后进入 P0-08D Gateway/最小入口。
5. P0-08D 通过后执行一次全量 test、race、vet、gofmt、diff check 和边界扫描。
6. 分别交付 P0-07 报告和 P0-08 A/B/C/D 完成报告。

当前不需要回退 P0-08B，不需要修改原计划文档，也不应直接开始 P0-08C。P0-07 的 framework 全局 goroutine residual risk 必须独立登记，不能由 P0-08 Worker/Queue 测试关闭，也不能用措辞伪造成不存在。
