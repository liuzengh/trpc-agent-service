# P0-08 Gateway、AgentJob、Queue 与 Worker 执行编排实施计划

## 0. 背景与当前基线

本计划面向一个全新启动的 pi 实例。实施前必须重新读取当前仓库和权威材料；实际代码、测试结果和当前交接材料优先于历史计划描述。

- 仓库：`C:/Documents/trpc-agent-service`。
- 当前分支：`project/production-platform`。
- 当前 HEAD：`9f8dbd7569cc1c4baa4329d6aeb8e75c4e191bf9`。
- 当前提交：`feat:complete stage P0-07`。
- 直接父提交：`f517131a89f5097f3be7a8ad44cfed314b399b57 feat:complete stage P0-06 rewrite docs`。
- 更早的 P0-06 实现提交：`62d69631f6d34330b6c8c38152033ecd8aad9af1 feat:complete stage P0-06`。
- 当前实施基线是 P0-07 提交，不再是 P0-06 提交。
- 当前工作区可能包含用户已有修改。实施前执行只读 `git status --short`，不得撤销、覆盖或重置既有修改。
- 当前同步链路仍是 `MemoryStore -> platform.Runner -> Responder`，本地 `EchoResponder` 必须保留为显式回滚路径。
- 当前 `trpcservice/agent` 已有 P0-07 平台边界：`AgentFactory`、`AgentRuntime`、`AgentInput`、`AgentResult`、`RunnerEvent` 以及 `ErrProducerIncomplete`、`ErrDrainTimeout`、`ErrFrameworkFailure` 等错误。
- 当前 P0-06 已有 `storage.LeaseStore`、`storage.Lease`、epoch 和 fencing token 契约。
- 当前没有完整的 `gateway`、`queue`、`worker`、`execution` 执行链路。

### P0-07 状态约束

`docs/P0-07阻塞交接结论.md` 明确 P0-07 仍为 `blocked`。tRPC-Agent-Go v1.11.2 缺少公开、合法、稳定的 framework run completion/wait API，因此不能证明 framework producer、Runner.Close 后 active run 或 framework 内部 goroutine 已完整退出。

P0-08 可以消费 P0-07 已交接的公开 Runtime 合同，但不得：

- 把 adapter pump completion 描述为 framework completion。
- 访问框架私有字段或内部 channel。
- 通过 goleak、sleep、放宽 drain timeout 或忽略残留 goroutine 绕过 blocker。
- 修改 P0-07 Runtime 生命周期以伪造 completion。

Worker 必须将 `ErrProducerIncomplete`、`ErrDrainTimeout`、framework failure、Context cancellation 和 Lease loss 作为明确的失败或不完整状态处理。P0-08 不负责解除 P0-07 blocker。

### 编号和范围说明

当前 `docs/implementation-plan.md` 将 P0-08 定义为 Session Execution Orchestrator，将 Gateway/AgentJob/Queue/Worker 放在 P0-10；历史 `docs/implementation-plan-p01.md` 和 `docs/plan007v1` 将本轮范围称为 P0-08 Gateway、AgentJob、Queue、Worker。本计划按照用户明确提出的目标组成一个执行编排切片，不修改任务编号文档，也不提前实现真实持久化 Repository、Outbox Dispatcher 或真实 IM 协议。

# P0-08 Gateway、AgentJob、Queue 与 Worker 执行编排计划

## 背景信息

本计划面向一个全新启动的 pi 实例。执行前必须先重新读取仓库和权威材料，不能仅根据本计划假定代码状态；实际代码、测试结果和当前交接材料优先于历史计划中的描述。

- 仓库路径：`C:/Documents/trpc-agent-service`。
- 当前基线提交：`62d6963 feat:complete stage P0-06`。
- 当前工作区可能包含用户或此前阶段对 `docs/README.md`、`docs/ARCHITECTURE.md`、`docs/implementation-plan.md`、`docs/project-status.md` 等文件的修改。不得撤销、覆盖或重置这些既有修改；实施前先执行只读的 `git status --short` 并逐项识别。
- 当前只读分析已经确认：P0-01 至 P0-06 的基础层、TenantContext、Session 模型、Claim、Lease、fencing token、Redis/PostgreSQL 协调抽象和 Fake Repository 已存在；Gateway、Queue、Worker、Execution 编排目录目前不存在或尚未形成 P0-08 链路。
- 当前同步执行链路是 `MemoryStore -> platform.Runner -> Responder`，本地仍保留 `EchoResponder`；P0-08 不应直接删除或破坏这条本地回滚路径。
- `trpcservice/agent` 已有 P0-07 的平台边界：`AgentFactory`、`AgentRuntime`、`AgentInput`、`AgentResult`、`RunnerEvent`，以及 `ErrProducerIncomplete`、`ErrDrainTimeout`、`ErrFrameworkFailure` 等错误。Worker 必须依赖这些平台类型，不得把 tRPC-Agent-Go 类型扩散到 `queue`、`gateway`、`execution`、`worker`、`web`、`tenant`、`channels` 或 `storage`。
- P0-07 当前不能被简单视为 closed。`docs/P0-07阻塞交接结论.md` 明确记录：tRPC-Agent-Go v1.11.2 缺少公开、稳定的 framework run completion/wait API，因此 framework completion blocker 仍然存在。P0-08 可以消费已经交接的 Runtime 公共接口，但不得在 P0-08 中解除、伪造或绕过该 blocker。
- 当前 P0-06 的 `storage.LeaseStore` 已提供 `Acquire`、`Renew`、`Release`、`Validate`，`storage.Lease` 含有 `OwnerID`、`FenceToken`、`ExpiresAt`、`Backend` 和 `Epoch`。P0-08 应消费该契约，不应重新实现 Redis/PostgreSQL Lease 或修改迁移。
- 当前仓库没有真实企业微信或 Telegram 协议实现。本任务只接受已经解析、验证的 transport-neutral 请求，不实现 IM 验签、解密、回调协议和发送 API。
- 当前总计划与历史计划存在编号差异：`docs/implementation-plan.md` 将 P0-08 描述为 Session Execution Orchestrator，将 Gateway/Queue/Worker 放在后续阶段；`docs/implementation-plan-p01.md` 和 `docs/plan007v1` 将本轮目标称为 P0-08 Gateway、AgentJob、Queue、Worker。本计划按照本轮用户明确列出的目标组成一个执行编排切片，不修改计划编号文档，也不把后续持久化 Queue、Outbox 或真实 IM 工作提前并入。
- 计划实施时只能修改本文允许的目录和文件；不执行 `git commit`、`git push`，不访问生产服务，不读取或输出密钥、Token、密码或生产日志。
- Fake Queue、Fake LeaseStore、Fake Runtime/AgentFactory 只用于测试，不读取真实 API Key，不访问外部网络，不依赖生产服务。
- 新 pi 应在实施前重新核对以下权威材料：`docs/README.md`、`docs/ARCHITECTURE.md`、`docs/implementation-plan.md`、`docs/project-status.md`、`docs/implementation-plan-p01.md`、`docs/plan007v1`、`docs/P0-07阻塞交接结论.md`、相关 P0-06 acceptance/lease/claim 材料，以及当前 `agent`、`storage`、`session`、`tenant`、`web`、`cmd` 源码和测试。
- 新 pi 应保持以下判断优先级：实际代码和测试结果 > `docs/project-status.md` > `docs/ARCHITECTURE.md` > `docs/implementation-plan.md` > P0-07/P0-08 历史计划和交接材料。
- 实施前应确认 tRPC-Agent-Go 版本仍固定为 `v1.11.2`；不得升级版本，不得引入模型厂商 SDK，也不得为 P0-08 解决 P0-07 framework completion blocker 而引入私有 API、sleep、goleak 或私有字段。
- P0-08 的完成不是“把同步调用包进 goroutine”。成功标准是 ACK、Job 边界、Queue Delivery、TenantContext/Trace 恢复、Session Lease、fencing、Runtime 取消、同 Session 串行、不同 Session 并行和 bounded shutdown 都有明确 owner、错误语义和测试证据。

> 本计划为只读分析结果，未修改仓库文件、未执行 commit/push、未访问生产环境。
>
> **编号说明：** 当前权威总计划 `docs/implementation-plan.md` 将 P0-08 定义为 `Session Execution Orchestrator`，而历史/交接材料 `docs/implementation-plan-p01.md` 与 `docs/plan007v1` 将 Gateway、AgentJob、Queue、Worker 列为 P0-08。本轮用户目标明确要求实现后者的完整执行链路，因此以下按“P0-08 执行编排切片”制定计划，同时保留当前总计划的边界：不实现真实持久化 Repository、Outbox Dispatcher 和真实 IM 协议。

## 当前任务

**P0-08 Gateway、AgentJob、Queue 与 Worker 执行编排**

## 目标

在 P0-07 的平台 Runtime 接口之上，建立一次消息执行从入口到 Worker 的最小异步链路：

```text
已解析请求
  -> Gateway 校验并快速 ACK
  -> AgentJob 序列化
  -> JobQueue 接受
  -> Worker 消费
  -> 恢复 TenantContext / TraceContext
  -> 获取 Session Lease
  -> 续租并验证 fencing token
  -> 调用 AgentRuntime
  -> 执行完成或取消
  -> Ack / Nack / 重试
```

本阶段必须证明：

- Gateway 在 Job 被 Queue 接受前不返回成功 ACK。
- Queue 传递的 AgentJob 不依赖进程内 `context.Context`，可序列化、可恢复。
- Worker 恢复 TenantContext 后再次校验租户、Agent、Binding、Session 和请求 ID 的一致性。
- 同一个 Session 在跨 Worker/跨 goroutine 场景下最多只有一个有效执行者。
- 不同 Session 可以并行执行。
- Lease 续租失败或 fencing token 失效时，Runner Context 会被取消，旧 Worker 不得继续提交执行结果。
- Worker 调用 P0-07 的 `AgentFactory`/`AgentRuntime`，但不依赖 tRPC-Agent-Go 类型。
- SIGTERM 会停止接收新 Job、取消或等待正在执行的 Job、停止 Lease 续租并在有界时间内退出。
- Queue 已接受但 Worker 失败时，Delivery 可见性超时后重新投递；Queue 未接受时 Gateway 不返回成功 ACK。

## 前置依赖

### 已完成的基础依赖

- **P0-04**：稳定 ID、DedupKey、Repository/Lease/Outbox 契约和 Fake 测试基础。
- **P0-06**：Redis/PostgreSQL Claim、Session Lease、epoch、fencing token、续租和故障切换语义。
- **P0-07 Runtime 合同**：`AgentFactory`、`AgentRuntime`、`AgentInput`、`AgentResult`、`RunnerEvent` 已存在于 `trpcservice/agent`，并且 P0-07 的 Runtime 行为可以被 Fake Runtime 替身调用。

### P0-07 当前状态约束

`docs/P0-07阻塞交接结论.md` 明确 P0-07 仍为 `blocked`，原因是 tRPC-Agent-Go v1.11.2 没有公开、稳定的 framework run completion/wait API。P0-08 不解除该阻塞，也不得把 P0-07 的 pump completion 描述成 framework completion。

P0-08 可以依赖 P0-07 已交接的**平台公共 Runtime 合同和已验证的取消/错误语义**，Worker 将 `ErrProducerIncomplete`、drain timeout、framework failure 等视为执行失败或不完整状态，执行 Nack/重试或释放 Lease；P0-08 不访问 tRPC-Agent-Go 私有字段、不修改 P0-07 的 Runtime 生命周期协议。

如果项目流程要求“P0-07 必须 closed 后才能进入下一阶段”，则本任务入口状态应为 `blocked`；如果允许按交接合同并行推进，则只允许使用现有公开接口，不修复 P0-07 framework blocker。

## 允许修改的范围

### 新增目录

- `trpcservice/queue/`
  - AgentJob、JobEnvelope、Delivery、QueueReceipt、JobQueue 接口。
  - 序列化/反序列化、版本和字段校验。
  - 仅用于单元测试的 FakeQueue 和可见性超时模拟。
- `trpcservice/gateway/`
  - GatewayHandler、入站请求转换、Job 创建、Queue 提交和快速 ACK。
  - 不实现 IM 验签和真实外部协议。
- `trpcservice/execution/`
  - Session 执行编排、Lease 生命周期、Runner 调用、取消和结果状态。
  - 面向 `agent.AgentFactory`/`agent.AgentRuntime` 的平台层接口。
- `trpcservice/worker/`
  - Worker 消费循环、并发控制、Nack/Ack、优雅停止和 DrainController。
  - Fake Queue、Fake LeaseStore、Fake Runtime 的测试装配。

### 允许修改的现有文件

- `trpcservice/web/server.go`
  - 仅将已经解析的 Web 请求接入 Gateway，或增加测试用 Gateway 入口。
  - 不实现真实企业微信/Telegram 协议，不改变 Channel Adapter 验签逻辑。
- `cmd/trpc-service/main.go`
  - 仅增加 Gateway、Queue、Worker、Execution 的装配和 SIGTERM 生命周期管理。
  - 保留现有本地同步/Echo 运行模式作为回滚路径。
- `trpcservice/queue/*_test.go`、`trpcservice/gateway/*_test.go`、`trpcservice/execution/*_test.go`、`trpcservice/worker/*_test.go`、必要的 `trpcservice/web/*_test.go`。

### 允许的测试替身

- Fake `JobQueue`：内存队列、确认/拒绝、visibility timeout、重投和关闭状态。
- Fake `LeaseStore`：Acquire、Renew、Validate、Release、冲突、过期和 fencing rejection。
- Fake `AgentFactory`/`AgentRuntime`：记录输入、阻塞等待取消、返回成功/失败/不完整错误。
- Fake Session/Event sink：只验证执行编排调用顺序和 fencing 检查，不实现 PostgreSQL Repository。

## 明确禁止

1. 不修改允许范围之外的文件。
2. 不执行 `git commit` 或 `git push`。
3. 不访问生产服务器。
4. 不读取或输出密钥、Token、密码和生产日志。
5. 不提前实现后续任务。
6. 不引入本任务之外的基础设施。
7. 不修改 `trpcservice/tenant` 的 TenantContext 领域定义来迁就 Queue；序列化使用 Queue 自己的版本化 DTO。
8. 不修改 `trpcservice/agent` 的 P0-07 Runtime 生命周期实现，不绕过其公开接口。
9. 不修改 `trpcservice/storage`、`migrations` 或实现 PostgreSQL/Redis 业务 Repository；P0-08 只消费已存在的 `LeaseStore`/Fake 契约。
10. 不实现真实企业微信、Telegram、微信公众号或其他 IM 验签、解密、回调协议和发送 API。
11. 不实现 Outbox Dispatcher、Retry Policy、Dead Letter 业务落库；本阶段只实现 Queue 的抽象和测试替身，以及 Delivery 的 Ack/Nack 语义。
12. 不实现 API 鉴权、Admin API、Guardrail、审批、Secret Manager、Telemetry、向量存储、对象存储或 Docker 部署。
13. 不把 `context.Context` 直接放入 AgentJob；取消和 deadline 必须在 Worker 侧重新创建。
14. 不使用进程内 Session mutex 作为跨节点一致性的唯一保障；正确性必须依赖 Lease 和 fencing。
15. 不在 Queue 未确认接受 Job 时发送成功 ACK，也不以“内存队列写入成功”冒充生产级持久化保证。

## 1. 当前代码状态

### 1.1 P0-07 Runtime 已有可消费的公共边界

当前 `trpcservice/agent` 已包含：

- `AgentSpec`、`AgentInput`、`AgentResult`、`RunnerEvent`。
- `AgentFactory.Build(context.Context, tenant.TenantContext, AgentSpec)`。
- `AgentRuntime.Run(context.Context, AgentInput)`。
- `RuntimeDependencies`、Provider/Tool 的平台级接口。
- `ErrProducerIncomplete`、`ErrDrainTimeout`、`ErrFrameworkFailure` 等执行错误。

Worker 应只依赖这些本项目类型，不导入 `trpc.group/trpc-go/trpc-agent-go`。P0-07 的框架类型隔离边界继续保持在 `trpcservice/agent` 内。

### 1.2 当前没有 P0-08 组件

当前仓库没有以下实现目录或符号：

- `trpcservice/gateway/`
- `trpcservice/queue/`
- `trpcservice/worker/`
- `trpcservice/execution/`
- `AgentJob`、`JobQueue`、`GatewayHandler`、`ExecutionState`、`DrainController`

### 1.3 当前同步入口

`cmd/trpc-service/main.go` 当前创建 `MemoryStore`，再构造 `platform.Runner` 并注入 Echo 或 P0-07 运行模式。`trpcservice/web/server.go` 当前 `/api/chat` 直接调用同步 Runner。

P0-08 的目标是增加可测试的 Gateway/Worker 编排层，但不应把当前 Web/IM 协议实现误认为生产 Gateway。`/webhook/{channel}/{external_app_id}` 仍保持当前简化协议壳；本阶段不实现真实企业微信和 Telegram。

### 1.4 P0-06 Lease/Fencing 契约

现有 `trpcservice/storage` 已提供：

```go
type LeaseStore interface {
    Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (Lease, error)
    Renew(context.Context, tenant.TenantContext, Lease, time.Duration) (Lease, error)
    Release(context.Context, tenant.TenantContext, Lease) error
    Validate(context.Context, tenant.TenantContext, Lease) error
}
```

`Lease` 包含 `TenantID`、`SessionID`、`OwnerID`、`FenceToken`、`ExpiresAt`、`Backend` 和 `Epoch`。P0-08 不重新设计该语义，只在执行期间持有和验证它。

### 1.5 当前确认的验证基线

本轮只读分析没有重新执行测试。此前项目基线中 `go test ./...`、`go vet ./...` 和格式检查通过；P0-07 交接材料记录了 agent/platform/cmd 的 targeted 测试、race、全量测试、vet、gofmt 和边界扫描结果，但 P0-07 阶段仍然是 blocked，而不是 closed。

## 2. 需求与现有代码的差距

| 目标 | 当前状态 | P0-08 需要补齐 |
| --- | --- | --- |
| Gateway 快速 ACK | 当前 Web 直接同步调用 Runner | 先构造并提交 Job，Queue 接受后才 ACK |
| AgentJob | 没有跨边界 Job 类型 | 版本化、可序列化、可校验的 Job Envelope |
| Queue 接口 | 没有 Queue 抽象 | Enqueue、Receive、Ack、Nack、Visibility Extend、Close |
| Worker 消费 | 没有 Worker 循环 | bounded concurrency、消费、执行、Ack/Nack 和退出 |
| TenantContext 恢复 | 只存在于当前进程 Context | Job 内保存无密钥 DTO，消费时校验并重建 Context |
| Session Lease | P0-06 只有底层 LeaseStore | Execution 持有 Lease、续租、释放和失效取消 |
| fencing token | P0-06 已有 token | Worker 在执行关键边界重新 Validate，过期 Worker 不得继续 |
| Runner 调用 | P0-07 可独立调用 Runtime | Worker 由 AgentFactory 构造 Runtime 并传入 AgentInput |
| Context/Trace 恢复 | Context 不能跨 Queue 直接传递 | 用 deadline、RequestID、MessageID、ExecutionID、TraceID 恢复新 Context |
| SIGTERM | main 只关闭 HTTP Server | Worker Stop/Drain、取消 in-flight、关闭 Queue 并有界退出 |
| ACK 后 Job 丢失保护 | 没有 ACK/Delivery 语义 | enqueue receipt、visibility timeout、Nack/requeue、未接受不 ACK |
| 同 Session 串行 | 没有执行层协调 | 分布式 Lease 保证单 owner；冲突 Job 延迟重投 |
| 不同 Session 并行 | 没有 Worker 并发模型 | worker concurrency 与 Session Lease 结合，避免全局串行 |

## 3. 详细实施步骤

### P0-08A：固定 Job 和 Queue 公共契约

1. 在 `trpcservice/queue` 定义 `AgentJob`、`JobEnvelope`、`QueueReceipt` 和 `Delivery`。
2. 为 Job 加入 `SchemaVersion`，固定必需字段：
   - `JobID`、`Attempt`、`CreatedAt`、`Deadline`。
   - `TenantID`、`AgentAppID`、`AgentVersion`、`BindingID`、`Channel`。
   - `SessionID`、`ExternalUser`、`ExternalChat`、`MessageID`、`RequestID`、`ExecutionID`、`TraceID`。
   - `TenantContext` 序列化 DTO。
   - `AgentSpec` 快照或最小 Agent 引用。
   - 当前输入消息和可选的只读历史消息快照。
3. 明确不进入 Job 的字段：Secret 明文、API Key、Authorization、数据库连接、框架 Runner、`context.Context`、可变 Repository、完整外部请求对象。
4. 采用 JSON 或等价稳定编码，字段顺序不作为语义依据；反序列化后校验 SchemaVersion、租户字段一致性、ID 和 deadline。
5. `TenantContext` 不能直接序列化 Go `context.Context`。建立 `TenantContextDTO`，消费时通过 `tenant.WithContext` 恢复新的 Context。
6. 定义 `TraceContextDTO`，只保存当前平台的 `TraceID` 和跨边界关联 ID；不引入 OTel 类型，P1-07 再接入正式 trace carrier。
7. 定义 Queue 语义：

```go
type JobQueue interface {
    Enqueue(context.Context, AgentJob) (QueueReceipt, error)
    Receive(context.Context, string, time.Duration) (Delivery, error)
    Ack(context.Context, Delivery) error
    Nack(context.Context, Delivery, NackOptions) error
    ExtendVisibility(context.Context, Delivery, time.Duration) error
    Close() error
}
```

8. `QueueReceipt.Accepted` 是 Gateway 返回成功 ACK 的必要条件；`Delivery` 必须包含 delivery ID、Job、attempt、visibility deadline 和 queue-owned token。
9. FakeQueue 模拟：enqueue 失败、receive 阻塞、visibility timeout 重投、Ack 幂等、Nack 延迟、Close 唤醒阻塞 Receive。
10. 这一阶段不实现 Redis Stream、PostgreSQL SKIP LOCKED、Kafka、RabbitMQ 或 Outbox 持久化。

### P0-08B：实现 Gateway 快速 ACK

1. 定义只接收“已解析/已验证输入”的 Gateway 请求，不在本阶段实现 IM 验签：

```go
type GatewayRequest struct {
    TenantContext tenant.TenantContext
    Agent agent.AgentSpec
    History []agent.Message
    Input agent.Message
    Deadline time.Time
}
```

2. Gateway 对 `TenantContext`、AgentSpec、SessionID、MessageID、RequestID、TraceID 和 deadline 做校验，并检查 Agent 与 TenantContext 的 TenantID/AgentAppID/版本一致性。
3. 生成稳定的 `JobID`/`ExecutionID`，避免使用未校验的外部 tenant ID 作为全局 key；Job 中保留原始平台 ID 用于追踪。
4. 在调用 Queue `Enqueue` 成功前不写成功响应。
5. Queue 返回失败、上下文取消或 `Accepted=false` 时返回明确失败，不模拟成功 ACK。
6. Enqueue 成功后只返回 `Accepted`、`JobID`、`RequestID`、`TraceID`，不等待 Runner 或模型。
7. Gateway 不负责 Session Lease，不直接调用 Runner，不写 Session Event，不发送 IM 回复。
8. 对 Web 入口只做最小接入；Webhook 路由继续保持当前占位行为或仅接入同一 transport-neutral Gateway 测试路径。

### P0-08C：实现 Worker 消费和执行编排

1. `Worker.Start` 启动固定数量的消费 goroutine；每个 goroutine 只拥有自己收到的 Delivery。
2. `Worker.Stop(ctx)` 先停止 Receive 新 Job，再等待已有 Job 在 deadline 内结束；超时则取消执行 Context，并对未完成 Delivery 执行可审计的 Nack/释放流程。
3. 每个 Delivery 的处理步骤固定为：

```text
Decode/Validate AgentJob
  -> Restore TenantContext
  -> Create execution context
  -> Load or accept Session input
  -> Acquire Session Lease
  -> Start lease renewal
  -> Build AgentRuntime
  -> Run AgentRuntime
  -> Validate lease/fence
  -> Commit through execution sink
  -> Ack or Nack Delivery
  -> Release Lease
```

4. `ExecutionContext` 从 `context.Background()` 或 Worker 根 Context 创建，注入：
   - `TenantContext`。
   - `RequestID`、`MessageID`、`ExecutionID`、`TraceID`。
   - Job deadline；deadline 不得超过 Worker shutdown deadline。
5. Worker 不把 Queue Delivery 的原始 Context 直接传给 Runner；Delivery Context 只控制消费操作，执行 Context 有明确 owner 和取消路径。
6. 租约参数必须可配置且有上限：Lease TTL、Renew interval、Renew timeout、Execution timeout。Renew interval 必须小于 TTL，并在关闭时停止 ticker。
7. Lease renew 失败、Validate 返回 `ErrLeaseLost`/`ErrFenceRejected`、epoch 变化或 execution Context 取消时：
   - 取消 AgentRuntime Context。
   - 停止继续处理新的 Runner 事件/结果。
   - 不提交 assistant/session 事实结果。
   - Delivery 按可重试或不可重试错误执行 Nack。
8. Runner 调用只通过：

```go
factory.Build(ctx, job.TenantContext, job.Agent)
runtime.Run(ctx, agent.AgentInput{...})
```

9. Worker 不导入 tRPC-Agent-Go 包；框架 Event 已经在 P0-07 转为 `agent.RunnerEvent`。
10. P0-07 的 `ErrProducerIncomplete`、`ErrDrainTimeout`、framework failure 和 `context.Canceled` 必须保留错误链，并映射为 ExecutionState，不得把失败结果 Ack 成功。
11. Runner 返回成功后、ExecutionSink 提交前必须再次 `LeaseStore.Validate`，防止旧 Worker 以过期 fencing token 写入结果。
12. P0-08 的 `ExecutionSink` 只作为平台编排抽象或 Fake；不实现 PostgreSQL Unit of Work。P0-09 再实现 Session Event、Audit、Outbox 的事实源事务。

### P0-08D：同 Session 串行和不同 Session 并行

1. 正确性依赖 `LeaseStore.Acquire`，不依赖单节点 map/mutex。
2. 同 Session 的第二个 Job 获取不到 Lease 时，不调用 Runner，使用 Nack 延迟重投。
3. 不同 Session 的 Job 允许被不同 Worker goroutine 同时处理。
4. 允许使用本地 `sessionKey` map 作为减少竞争的优化，但不能替代分布式 Lease；如果新增本地 map，必须有 bounded cleanup，不能无限增长。
5. 测试通过 barrier 控制两个 Fake Runtime：
   - 相同 Session：只能有一个 Runtime 进入，另一 Job 被重投或等待；无双执行。
   - 不同 Session：两个 Runtime 可同时进入 barrier，证明没有全局锁。
6. Lease 被主动过期后，第二个 Worker 可以接管；旧 Worker 的 Validate/提交必须被 fencing 拒绝。

### P0-08E：ACK 后丢失保护和 Delivery 语义

1. Queue 的 `Enqueue` 必须返回明确 receipt；Gateway 只有 `Accepted=true` 才 ACK。
2. FakeQueue 建模三种情况：
   - Enqueue 失败：Gateway 返回失败，不产生成功 ACK。
   - Enqueue 成功、Worker 尚未消费：Job 仍可 Receive。
   - Worker 收到 Job 后崩溃/未 Ack：visibility timeout 后 Job 再次 Receive。
3. Worker 只有在执行结果已完成其当前 `ExecutionSink` 提交、且 Lease/fencing 验证通过后才 Ack。
4. Runner 失败、Lease 丢失、Context 取消或 commit 失败时不能伪造成功 Ack；按明确 Nack 分类处理。
5. P0-08 不宣称“跨进程持久化已经完成”。没有真实持久化 Queue/Outbox 时，验收结论只能是“Queue Contract 层面的 ACK 不丢保护已通过”；Durable Job handoff 留给后续 Outbox/持久化任务完成。

### P0-08F：SIGTERM 和优雅退出

1. `cmd/trpc-service/main.go` 由根 Context 派生 Worker Context，并统一监听 SIGINT/SIGTERM。
2. 收到信号后按顺序执行：

```text
停止 Gateway 接收新 Job
  -> Worker 不再 Receive
  -> 停止 lease renew ticker
  -> 等待 in-flight execution
  -> 到 shutdown deadline 时 cancel execution Context
  -> Nack/release 未完成 Delivery/Lease
  -> 关闭 Queue
  -> Shutdown HTTP
```

3. `Worker.Stop(ctx)` 必须幂等；重复 SIGTERM 不得重复 close channel、重复关闭 Queue 或产生 panic。
4. `Queue.Close` 只关闭 Queue 自己拥有的 channel；不能关闭调用方或 P0-07 framework Event channel。
5. 所有 goroutine 都必须有 owner 和退出信号：consumer、lease renewer、FakeQueue blocked receiver、execution watcher。
6. Worker 的 shutdown timeout 必须显式配置并有最大上限；禁止无限等待，也禁止用固定 sleep 伪造退出完成。
7. 现有 HTTP Server 的 `Shutdown` 仍然保留，但 Worker 停止必须在依赖关闭顺序中明确执行，避免新请求继续提交 Job。

### P0-08G：最小 Web 接入

1. `web.Server` 增加 Gateway 依赖或独立的 `GatewayHandler` 字段。
2. `/api/chat` 只负责解码和构造已验证的 GatewayRequest；本阶段不新增认证，因此不能把当前接口标记为生产 API。
3. 成功响应改为已接受的 Job 信息，不等待 Agent 结果；若必须保留本地同步开发体验，应明确通过 Echo/synchronous mode 选择，不让异步 ACK 和同步 reply 混用。
4. `/webhook/` 不实现真实企业微信/Telegram 协议；可以继续返回当前占位行为或仅接入同一 transport-neutral Gateway 测试路径。
5. Web 测试验证：Queue 接受后快速返回、Queue 失败不返回成功、请求 Context 取消不会留下后台提交 goroutine。

## 4. Go 接口和数据结构

以下接口是计划中的平台边界示例，实际实现时以现有 P0-07 和 P0-06 类型为准。它们不包含 tRPC-Agent-Go 类型。

### 4.1 Queue 类型

```go
package queue

type AgentJob struct {
    SchemaVersion int `json:"schema_version"`
    JobID         string `json:"job_id"`
    Attempt       int `json:"attempt"`
    CreatedAt     time.Time `json:"created_at"`
    Deadline      time.Time `json:"deadline"`

    Tenant TenantContextDTO `json:"tenant"`
    Agent  agent.AgentSpec `json:"agent"`
    Input  agent.Message `json:"input"`
    History []agent.Message `json:"history,omitempty"`

    ExecutionID string `json:"execution_id"`
    Trace       TraceContextDTO `json:"trace"`
}

type TenantContextDTO struct {
    TenantID string `json:"tenant_id"`
    AgentAppID string `json:"agent_app_id"`
    BindingID string `json:"binding_id"`
    Channel string `json:"channel"`
    ExternalUser string `json:"external_user"`
    ExternalChat string `json:"external_chat"`
    InternalUser string `json:"internal_user"`
    SessionID string `json:"session_id"`
    RequestID string `json:"request_id"`
    MessageID string `json:"message_id"`
    TraceID string `json:"trace_id"`
    ConfigVersion int64 `json:"config_version"`
    Permissions []string `json:"permissions,omitempty"`
    BackendPolicy tenant.BackendPolicy `json:"backend_policy"`
}

type TraceContextDTO struct {
    TraceID string `json:"trace_id"`
    RequestID string `json:"request_id"`
    MessageID string `json:"message_id"`
    ExecutionID string `json:"execution_id"`
}

type QueueReceipt struct {
    ID string
    Accepted bool
}

type Delivery struct {
    ID string
    Job AgentJob
    Attempt int
    VisibilityUntil time.Time
    Token string
}

type NackOptions struct {
    RetryAfter time.Duration
    Reason string
    Requeue bool
}

type JobQueue interface {
    Enqueue(context.Context, AgentJob) (QueueReceipt, error)
    Receive(context.Context, string, time.Duration) (Delivery, error)
    Ack(context.Context, Delivery) error
    Nack(context.Context, Delivery, NackOptions) error
    ExtendVisibility(context.Context, Delivery, time.Duration) error
    Close() error
}
```

`TenantContextDTO` 是跨边界数据，不是对 `context.Context` 的序列化。实现中必须提供：

```go
func NewTenantContextDTO(tc tenant.TenantContext) (TenantContextDTO, error)
func (dto TenantContextDTO) Restore() (tenant.TenantContext, error)
```

`Restore` 必须调用 `TenantContext.Validate`，并检查 DTO 中的 `TenantID`、`AgentAppID`、`ConfigVersion`、`SessionID`、`RequestID`、`MessageID` 和 `TraceID` 不为空且与 Job 顶层字段一致。

### 4.2 Gateway 类型

```go
package gateway

type GatewayRequest struct {
    TenantContext tenant.TenantContext
    Agent agent.AgentSpec
    History []agent.Message
    Input agent.Message
    Deadline time.Time
}

type Accepted struct {
    Accepted bool
    JobID string
    RequestID string
    TraceID string
}

type GatewayHandler interface {
    Submit(context.Context, GatewayRequest) (Accepted, error)
}
```

`Submit` 的成功定义是 Queue 返回 `Accepted=true`；它不代表 Runner 已执行，也不代表最终回复已发送。

### 4.3 Execution 类型

```go
package execution

type State string

const (
    StateReceived State = "received"
    StateRunning State = "running"
    StateSucceeded State = "succeeded"
    StateCanceled State = "canceled"
    StateRetryableFailure State = "retryable_failure"
    StatePermanentFailure State = "permanent_failure"
    StateLeaseLost State = "lease_lost"
)

type ExecutionState struct {
    JobID string
    ExecutionID string
    TenantID string
    SessionID string
    WorkerID string
    State State
    Attempt int
    FenceToken uint64
    Error error
}

type SessionLeaseManager interface {
    Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (storage.Lease, error)
    StartRenewal(context.Context, tenant.TenantContext, storage.Lease, time.Duration) (<-chan error, error)
    Validate(context.Context, tenant.TenantContext, storage.Lease) error
    Release(context.Context, tenant.TenantContext, storage.Lease) error
}

type ExecutionSink interface {
    Commit(context.Context, ExecutionState, agent.AgentInput, agent.AgentResult) error
}
```

`ExecutionSink` 在 P0-08 中只允许 Fake 或内存测试实现；真实 Session Event、Audit、Outbox 同事务提交属于后续 Repository/Outbox 任务。

### 4.4 Worker 类型

```go
package worker

type Config struct {
    WorkerID string
    Concurrency int
    ReceiveTimeout time.Duration
    LeaseTTL time.Duration
    LeaseRenewInterval time.Duration
    LeaseRenewTimeout time.Duration
    ShutdownTimeout time.Duration
    RetryDelay time.Duration
}

type Worker struct {
    // queue, factory, lease store, execution sink and lifecycle state
}

type DrainController interface {
    Stop(context.Context) error
    Wait(context.Context) error
}

func (w *Worker) Start(context.Context) error
func (w *Worker) Stop(context.Context) error
```

不建议在公共 `Worker` 结构中暴露框架 Runner、channel 或可变 Tenant 状态。每个 Job 调用 `AgentFactory.Build` 获得请求/Agent 版本对应的 Runtime；Worker 只保存生命周期状态和依赖。

### 4.5 状态和错误映射

| 错误 | Worker 行为 |
| --- | --- |
| `context.Canceled` / shutdown | 不提交成功结果；按可重投策略 Nack；释放 Lease |
| `context.DeadlineExceeded` | 根据 Job 类型 Nack 或终止；不成功 Ack |
| `storage.ErrLeaseLost` / `ErrFenceRejected` | 立即取消 Runtime，不提交结果，Nack/记录 lease lost |
| `agent.ErrProducerIncomplete` / `ErrDrainTimeout` | 视为 Runtime 不完整，保留错误链，按有限重试处理 |
| Agent 配置/租户校验错误 | 永久失败，不重复执行无效 Job |
| Queue 接收/ACK/NACK 失败 | Worker 记录 Queue failure，不能假设 Delivery 已完成 |
| ExecutionSink commit 失败 | 不成功 Ack；交给后续重试/持久化任务定义策略 |

## 5. 测试用例

### 5.1 Job 和 TenantContext 序列化

1. `AgentJob` JSON round-trip 后字段等价，`SchemaVersion` 正确。
2. TenantContext DTO 能恢复原始 TenantID、AgentAppID、BindingID、SessionID、RequestID、MessageID、TraceID、权限和 BackendPolicy。
3. 恢复时拒绝空 TenantID、无效 ConfigVersion、无效 SessionID、无效 TraceID 和未通过 `TenantContext.Validate` 的数据。
4. Job 顶层 TenantID 与 DTO TenantID 不一致时拒绝。
5. AgentSpec TenantID/AgentAppID/Version 与 TenantContext 不一致时拒绝。
6. Job 中不存在 `context.Context`、tRPC-Agent-Go Runner、Provider、Secret、Token 或密码字段。
7. 历史消息和输入消息的切片/Map 不会被调用方后续修改影响已提交 Job。
8. deadline 已过期、过长或缺失时按约定拒绝。

### 5.2 Gateway 快速 ACK

1. Queue `Enqueue` 成功后立即返回 `Accepted=true`，不调用 Runner。
2. 使用阻塞 Fake Runner 验证 Gateway 不等待模型结果。
3. Queue `Enqueue` 失败时 Gateway 返回错误，不返回成功 ACK。
4. Queue 返回 `Accepted=false` 时 Gateway 不返回成功。
5. 请求 Context 在 Enqueue 期间取消时不遗留后台提交 goroutine。
6. 多次提交生成独立 JobID/ExecutionID；P0-08 不重复实现入站 Dedup Claim。
7. TenantContext 不合法或与 Agent 不匹配时不会调用 Queue。

### 5.3 Queue 和 ACK 后丢失保护

1. Enqueue 成功的 Job 可以被 Receive。
2. Worker 收到 Delivery 后不 Ack，visibility timeout 后 Job 可以再次 Receive。
3. Nack with requeue 会按 RetryAfter 重新出现。
4. Ack 后 Job 不再重复 Delivery。
5. 伪造 Delivery token、错误 worker owner 或重复 Ack 被拒绝/幂等处理。
6. Queue Close 会唤醒阻塞 Receive，并且只关闭 Queue 自己拥有的 channel。
7. Enqueue 失败不会产生可消费的半截 Job。

### 5.4 Worker 和 Runner 调用

1. Worker 能从 Job 恢复 TenantContext，并将 `tenant.WithContext` 放入新的 execution Context。
2. Worker 通过 `AgentFactory.Build` 后调用 `AgentRuntime.Run`，输入包含正确的 AgentSpec、历史和当前消息。
3. Fake Runtime 返回成功时，ExecutionSink 提交一次，随后 Ack 一次，Lease 最终 Release。
4. Fake Runtime 返回错误时不提交成功结果，按策略 Nack，不成功 Ack。
5. `ErrProducerIncomplete`、`ErrDrainTimeout` 和 framework error 的 `errors.Is`/`errors.As` 语义保持可检查。
6. Job Context 的 deadline 被传递到 Runtime；Runtime 阻塞时取消根 Context 能使其退出。
7. TenantContext 恢复失败、Agent 配置不匹配或 SessionID 不一致时不调用 Runtime。
8. Worker 不导入 `trpc.group/trpc-go/trpc-agent-go`；框架类型边界扫描无输出。

### 5.5 Lease、fencing 和同 Session 串行

1. 两个 Worker 同时消费相同 Session 的 Job，只有一个 Acquire 成功。
2. Acquire 失败的 Job 不调用 Runner，并被 Nack/requeue。
3. Lease 续租失败时 Fake Runtime 收到 Context 取消。
4. Lease 被接管后，旧 Worker 的 Validate 或 ExecutionSink commit 返回 fencing rejection。
5. 旧 Worker 在 Runner 返回成功后、提交前失去 Lease，不产生成功提交。
6. 正常成功路径执行 Acquire -> Renew -> Validate -> Commit -> Ack -> Release 的顺序。
7. Release 失败不会伪造成功 Lease 状态，且错误可观测。
8. 不同 Session 的两个 Job 能够并行进入 Fake Runtime barrier。
9. 不使用全局 mutex 证明不同 Session 不被错误串行化。

### 5.6 SIGTERM 和优雅退出

1. 收到 SIGTERM 后 Gateway 不再接受新 Job。
2. Worker 不再调用 Queue.Receive。
3. 正在运行的 Fake Runtime 收到取消或在 shutdown deadline 内正常完成。
4. Lease renewal goroutine 在 Worker 停止后退出。
5. 未完成 Delivery 被 Nack/requeue 或明确保留为可见性超时重投。
6. Queue.Close、Worker.Stop、DrainController.Stop 可重复调用而不 panic。
7. SIGTERM 后无新的 Job 被 Ack 为成功。
8. 多次 SIGTERM 不导致重复 close、死锁或 goroutine 泄漏。
9. `Queue.Receive` 阻塞时，Close/Stop 能在 bounded timeout 内唤醒它。

### 5.7 Web 最小接入

1. `/api/chat` 在 Queue 接受后返回 Job accepted 信息，不等待 Runner。
2. Queue 不可用时返回错误，不返回成功 ACK。
3. 当前简化 webhook 不新增真实企业微信/Telegram 协议行为。
4. Web 层只依赖 `gateway.GatewayHandler`，不依赖 tRPC-Agent-Go 类型。

### 5.8 静态和竞态边界

1. `rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/queue trpcservice/gateway trpcservice/execution trpcservice/worker trpcservice/web` 无输出。
2. `go test` 和 `go test -race` 覆盖 Queue、Gateway、Execution、Worker。
3. 测试不读取 API Key，不访问外部网络，不启动真实 Redis/PostgreSQL，不依赖生产日志。
4. FakeQueue、FakeLeaseStore、FakeRuntime 的关闭和取消路径有明确断言。

## 6. 验收命令

### 6.1 入口前检查

```bash
git status --short
rg -n 'type (AgentFactory|AgentRuntime|AgentInput|AgentResult|RunnerEvent)' trpcservice/agent
rg -n 'type (LeaseStore|Lease|OperationGuard)' trpcservice/storage
rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/queue trpcservice/gateway trpcservice/execution trpcservice/worker trpcservice/web
```

第一、二条用于确认前置事实；第三条必须无输出。

### 6.2 定向测试

```bash
go test ./trpcservice/queue ./trpcservice/gateway ./trpcservice/execution ./trpcservice/worker ./trpcservice/web -count=1 -v
go test ./trpcservice/queue ./trpcservice/gateway ./trpcservice/execution ./trpcservice/worker ./trpcservice/web -race -count=1 -v
go vet ./trpcservice/queue ./trpcservice/gateway ./trpcservice/execution ./trpcservice/worker ./trpcservice/web
```

如果 `trpcservice/web` 保留同步兼容测试，应同时验证异步 Gateway 路径，不得只通过旧 Echo 路径。

### 6.3 全量回归

```bash
go test ./... -count=1
go test ./... -race -count=1
go vet ./...
test -z "$(gofmt -l .)"
git diff --check
```

P0-08 不应因为目标不涉及 Redis/PostgreSQL 而跳过已有 P0-06 回归；但不需要为本阶段新增真实外部服务依赖。

### 6.4 进程退出验收

在仅使用 Fake Queue/Fake Runtime 的测试配置下：

```bash
MODEL_PROVIDER=echo HTTP_ADDR=:18080 go run ./cmd/trpc-service
```

然后通过项目现有的本地测试入口触发一个 Job，发送 SIGTERM，验证：Worker 停止接收、in-flight Context 取消、Queue 关闭、HTTP Server 退出。不得使用真实 API Key、生产 URL 或真实 IM 服务。

命令必须根据最终实现的本地入口调整；如果没有安全的本地进程测试入口，则以 `worker`/`cmd` 的可控单元测试替代，不能把人工观察当作通过证据。

## 7. 风险和回滚方案

### 7.1 P0-07 framework completion blocker 影响

P0-07 交接材料明确无法证明 tRPC-Agent-Go framework producer 完整退出。P0-08 不应通过 Worker 的 `Stop`、Queue drain 或 goroutine 计数掩盖这个问题。Worker 只消费 P0-07 暴露的 `AgentResult`/error，并将 `ErrProducerIncomplete` 等作为执行失败处理。

**缓解：** P0-08 测试使用 Fake `AgentRuntime` 证明 Worker 生命周期；真实 Runner 的 framework completion 仍标记为 P0-07 的已知限制。

### 7.2 ACK 后 Queue 丢失

如果 Queue 只有内存实现，进程崩溃会丢失已接受但未持久化的 Job。

**缓解：** Gateway 只在 Queue receipt accepted 后 ACK；Delivery 使用 visibility timeout；P0-08 验收明确只覆盖 Queue contract。真正的持久化 Job handoff、Outbox 和重启恢复由后续 durable execution 任务完成。

### 7.3 Lease 过期和旧 Worker 提交

Runner 长于 Lease TTL，或续租线程异常退出，可能产生 stale owner。

**缓解：** Renew interval 小于 TTL；Renew/Validate 失败立即取消 Runtime；提交前再次 Validate；ExecutionSink 不允许绕过 fencing token。测试主动接管和旧 token 拒绝。

### 7.4 同 Session 误用本地锁

单进程 mutex 不能解决多节点竞争，且可能造成不同 Session 全局串行。

**缓解：** LeaseStore 是正确性来源；本地锁最多作为有界优化。测试必须覆盖同 Session 竞争和不同 Session 并行。

### 7.5 Context 恢复错误

不能跨 Queue 传输原始 Go Context；如果漏传 Session、Request、Trace 或 deadline，会导致审计、取消和后续幂等关联错误。

**缓解：** Job SchemaVersion、DTO Restore、顶层字段一致性校验、deadline 上限、端到端字段断言。

### 7.6 优雅退出死锁

Receive、lease renewer、Runner 或 Queue close 顺序错误可能导致 Stop 永不返回，或关闭不属于 Worker 的 channel。

**缓解：** 每个 goroutine 明确 owner；Stop 幂等；Queue Close 唤醒 Receive；shutdown 使用有界 Context；P0-07 framework channel 由 adapter/framework 所有者处理，P0-08 不关闭它。

### 7.7 错误状态被错误 Ack

如果 Runtime 返回错误后仍调用 Ack，会造成消息丢失；如果 commit 成功后只 Nack，则可能重复提交。

**缓解：** 固定处理顺序：成功结果和 fencing 验证、ExecutionSink commit、再 Ack；commit/ack 组合不确定时保留 Delivery 可重投，并等待后续 Outbox/事实源幂等语义解决。

### 7.8 回滚

本阶段不涉及数据库迁移，因此回滚不需要数据回滚：

- 保留现有同步 `platform.Runner`/Echo fallback 作为本地回滚路径。
- 通过显式运行模式禁用 Gateway/Worker 异步路径，恢复直接同步处理。
- 停止新 Worker，不再接收 Queue Delivery；未完成 Delivery 通过 visibility timeout 或 FakeQueue 重投。
- 仅回滚本阶段允许修改的 `queue`、`gateway`、`execution`、`worker`、`web`、`cmd` 文件，不触碰 P0-06 协调实现和 P0-07 Runtime 代码。
- 不执行 `git reset --hard`、`git checkout --`、commit 或 push；回滚由代码审查后按文件级变更处理。

## 8. 计划修改的具体文件

### 新增

```text
trpcservice/queue/job.go
trpcservice/queue/context.go
trpcservice/queue/queue.go
trpcservice/queue/fake_queue.go
trpcservice/queue/job_test.go
trpcservice/queue/context_test.go
trpcservice/queue/fake_queue_test.go

trpcservice/gateway/gateway.go
trpcservice/gateway/gateway_test.go

trpcservice/execution/execution.go
trpcservice/execution/lease.go
trpcservice/execution/execution_test.go
trpcservice/execution/lease_test.go

trpcservice/worker/worker.go
trpcservice/worker/lifecycle.go
trpcservice/worker/worker_test.go
trpcservice/worker/shutdown_test.go
trpcservice/worker/concurrency_test.go
```

### 允许修改

```text
trpcservice/web/server.go
trpcservice/web/server_test.go
cmd/trpc-service/main.go
```

具体是否修改 `web/server.go` 取决于最终是否把 `/api/chat` 接到 Gateway；如果本阶段只交付 transport-neutral Gateway/Worker 契约，则不应为了“看起来接入”修改 Web 路由。

### 明确不修改

```text
trpcservice/agent/**
trpcservice/tenant/**
trpcservice/channels/**
trpcservice/storage/**
migrations/**
trpcservice/platform/platform.go
trpcservice/audit/**
trpcservice/outbox/**
```

其中 `trpcservice/agent`、`platform` 和 `storage` 是 P0-07/P0-06 的前置边界；P0-08 通过接口消费它们，不扩展或重写它们。

### 完成定义

只有同时满足以下条件，P0-08 才能标记完成：

1. Gateway、AgentJob、Queue、Worker、Execution 和 shutdown 的 targeted 测试通过。
2. Queue 接受前不 ACK；Queue 未接受、Worker 失败、Lease 丢失和 commit 失败均不会伪造成功完成。
3. TenantContext、Trace/Request/Message/Execution ID 和 deadline 能跨 Queue 恢复并通过一致性校验。
4. 同 Session 只有一个有效 Lease owner，不同 Session 能并行。
5. Lease 续租失败或 fencing 失效会取消 Runner Context，并阻止旧 Worker 提交。
6. Worker 调用 P0-07 `AgentRuntime`，不把 tRPC-Agent-Go 类型扩散到 Queue、Gateway、Execution、Worker、Web 或 Storage。
7. SIGTERM 停止接收、取消/等待 in-flight、停止续租、关闭 Queue，并在有界时间内退出。
8. Fake Queue/Fake LeaseStore/Fake Runtime 测试不依赖真实 API Key、真实 IM、Redis、PostgreSQL 或外部网络。
9. `go test`、`go test -race`、`go vet`、gofmt、diff check 和框架边界扫描通过。
10. 文档中明确本阶段只证明 Queue Contract 层 ACK 不丢保护，不冒充已经完成持久化 Job、Outbox、真实 IM 或生产 Repository。

