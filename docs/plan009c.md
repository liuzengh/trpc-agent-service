# 可直接发送给全新上下文 pi 的提示词

```text
你正在接手本地项目：

C:/Documents/trpc-agent-service

当前任务不是开始 P0-09D，也不是继续扩展功能。当前唯一任务是关闭 P0-09C 的最后一个 race gate blocker：定位并消除既有 PostgreSQL Queue 测试 `TestPostgresQueueExtendVisibilityUsesConditionalLease` 的非确定性，然后重新执行 P0-09C 的完整受影响范围验收。只有所有证据实际通过，才能把 P0-09C 从 `in_progress / blocked` 更新为 `verified`。

这是全新上下文。不能假定此前会话、报告、源码或测试结论已经被你读取。必须先核对当前工作区、当前源码和当前报告；实际源码及本轮实际命令结果高于历史报告。

## 一、完整项目背景

这是一个 Go 多租户 Agent 服务，基于 tRPC-Agent-Go。项目同时包含同步兼容路径和正在建设的异步执行路径。

当前已知架构包括：

- 同步 Web `/api/chat` 兼容路径；
- transport-neutral Gateway Submit contract；
- 版本化 AgentJob/DTO 和有界 History 快照；
- FakeQueue contract；
- 真实 PostgreSQL durable JobQueue；
- Worker delivery processing 与 bounded shutdown；
- Execution fenced commit contract；
- 真实 PostgreSQL Execution Result Repository；
- 真实 PostgreSQL atomic result-and-ack coordinator；
- 尚未完成的 Outbox、Retry/DLQ、Web/CMD 异步生产装配、SIGTERM 入口级关闭、真实 IM、鉴权和 Telemetry。

项目整体状态仍是 `partial`，不能描述成完整生产异步平台。

## 二、计划实施背景与已完成阶段

### P0-07 / R1：Runtime completion/drain

状态：`contract-level verified`。

已验证公开 Runtime event stream 的：

`completion observed -> tail event drain -> framework-owned channel close -> adapter pump done -> bounded return`

仍未证明 tRPC-Agent-Go framework 内部所有后台 goroutine 存在公开 global Wait/Done。不得访问 framework 私有字段或 channel，不得把 adapter pump done 外推为 framework 全局退出。

### P0-08A：Queue/Job contract

状态：`contract-level verified`。

已完成 AgentJob、DTO、TenantContext、History、FakeQueue、Delivery visibility、Ack/Nack/Close contract。FakeQueue 只证明 in-process contract。

### R2：History 与安全错误分类

状态：`contract-level verified`。

已完成 Job schema v2、有界 History、Gateway -> Queue -> Worker -> Execution 传递、深拷贝、大小限制和 safe Nack reason。不得把原始 Prompt、Authorization、API key、Provider body 或任意 `cause.Error()` 写入 Nack reason。

### R3：Execution fencing contract

状态：`contract-level verified`。

已移除无 fencing 的生产 Commit fallback，ExecutionCommit 携带 tenant、session、owner、epoch、FenceToken、JobID、ExecutionID 和 Lease。

### R4：Worker delivery processing

状态：`contract-level verified`。

已完成 Ack/Nack unknown、有限 retry、visibility recovery、unresolved delivery、visibility failure cancellation 和 Receive backoff。

### R5：Worker bounded shutdown

状态：`contract-level verified`。

已完成停止 Receive、graceful drain、forced cancellation、renewer/action 生命周期协调、Queue Close 幂等和并发 Stop。非合作 Queue action 仍可能在 Stop 返回后由 finalizer 继续处理，该边界必须保留。

### R6：最终审查

状态：`complete`，项目整体为 `partial`。

R6 曾运行全仓 test/race/vet，但这些是历史证据，不能替代本轮 gate。

### P0-09A：真实 PostgreSQL fenced Execution Repository

状态：`verified`。

已完成真实 PostgreSQL `execution_result` fenced atomic commit，包括 tenant/session/owner/epoch/FenceToken/lease expiry 校验、幂等、冲突分类、rollback 和真实 takeover acceptance。

### P0-09B：真实 PostgreSQL durable Queue

状态：`verified`。

已实现并验证：

- `trpcservice/queue/postgres_queue.go`；
- migration `000004_job_queue`；
- durable Enqueue；
- transaction claim；
- `FOR UPDATE SKIP LOCKED`；
- delivery token；
- visibility expiry recovery；
- Ack/Nack/ExtendVisibility；
- tenant isolation；
- rollback；
- independent pgx pool recovery；
- stale token rejection；
- History/Trace/schema payload persistence。

P0-09B 不证明 Commit/Ack 原子性，也不证明生产进程/SIGTERM 恢复。

### P0-09C：Atomic Result Commit + Queue Ack

当前状态：`in_progress / blocked`。

P0-09C 已实现并通过真实 PostgreSQL核心验收：

- storage-neutral `AtomicCompletionRequest`、`DeliveryAckRecord`、`AtomicCompletionCoordinator`；
- PostgreSQL coordinator 使用同一个 `pgx.Tx` 写 `execution_result` 并更新 `job_queue` 为 Ack；
- transaction 内重新校验 tenant、session、owner、epoch、FenceToken、Lease expiry、JobID、ExecutionID、DeliveryID/token、Queue payload 和 delivery 状态；
- result 写入失败或 Ack 条件失败时两张表一起 rollback；
- duplicate completion 幂等；
- result-only 状态可安全补 Ack；
- ack-only 状态明确 conflict；
- commit connection interruption 返回可识别的 `ErrCompletionOutcomeUnknown` 并执行只读 reconciliation；
- Worker 提供显式 `NewWithAtomicCompletion` 路径，真实 PostgreSQL成功路径为 `Execute -> AtomicCompletion -> Release`，不调用独立 Queue Ack；
- unknown completion outcome 进入 unresolved，不立即 Ack/Nack；
- legacy `New` / `Commit -> Ack` 只保留 Fake/in-process contract，不是 durable PostgreSQL Worker fallback。

P0-09C 已有专用真实 PostgreSQL测试、错误注入、并发 winner、Worker integration 和专用 race 证据。当前唯一未关闭项是完整受影响范围 race 命令被一个既有 P0-09B 测试间歇性阻断。

## 三、当前唯一 blocker

阻塞测试：

`TestPostgresQueueExtendVisibilityUsesConditionalLease`

所在文件预计为：

`trpcservice/queue/postgres_queue_integration_test.go`

历史复现：该测试使用约 60ms visibility。在 race instrumentation 下，Delivery 有时在 `ExtendVisibility` SQL 执行前已经依据 PostgreSQL `clock_timestamp()` 过期，返回：

`queue: delivery expired`

历史单独复验曾出现约 `3 PASS / 2 FAIL`。完整命令因此不能记录为通过：

```bash
go test ./trpcservice/queue/... ./trpcservice/storage/... ./trpcservice/execution/... ./trpcservice/worker/... -race -count=1
```

这目前只是待核实的 blocker 诊断。不能直接假定它一定是测试缺陷，也不能直接假定是生产 Queue Bug。

## 四、当前阶段名称和边界

将本轮命名为：

`P0-09C-B1: ExtendVisibility race-gate blocker closure`

状态规则：

- P0-09A：保持 `verified`；
- P0-09B：保持 `verified`，除非本轮确认生产 Queue 行为确有 Bug；
- P0-09C-B1：当前唯一 `in_progress`；
- P0-09C：保持 `in_progress / blocked`，直到完整 gate 通过；
- P0-09D 以及 Outbox/Retry/DLQ/生产装配：`not started`。

本轮不得开始 P0-09D。

## 五、必须先读取的参考文件

按当前实际存在情况读取：

- `README.md`
- `docs/ARCHITECTURE.md`
- `docs/implementation-plan.md`
- `docs/project-status.md`
- `docs/R6最终审查报告.md`
- `docs/P0-09A验收报告.md`
- `docs/P0-09B验收报告.md`
- `docs/P0-09C验收报告.md`
- `docs/P0-08完成报告.md`
- `docs/plan008v3.md`
- `docs/p0-06-acceptance-matrix.md`
- `trpcservice/queue/postgres_queue.go`
- `trpcservice/queue/postgres_queue_test.go`
- `trpcservice/queue/postgres_queue_integration_test.go`
- `trpcservice/queue/queue.go`
- `trpcservice/queue/fake_queue.go`
- `trpcservice/worker/**` 中 AtomicCompletion 和 visibility 相关代码/测试
- `trpcservice/storage/**` 和 `trpcservice/storage/postgres/**` 中 AtomicCompletion 代码/测试
- `trpcservice/execution/**` 中 AtomicCompletion/RepositorySink 适配
- `migrations/000003_execution_result.*`
- `migrations/000004_job_queue.*`
- 当前 PostgreSQL test harness、migration runner 和测试 schema helper
- `go.mod`

如果当前项目文档不足以让后续全新上下文理解 P0-09A/B/C 的接口、数据库表、事务边界和状态，可以新增：

`docs/plan009background.md`

该文档只允许记录从当前源码、migration、测试和验收报告中核实的事实，包括：

- P0-09A/B/C 的目标与状态；
- `execution_result` 与 `job_queue` schema；
- Fenced Commit、durable Delivery 和 AtomicCompletion 的接口关系；
- Worker durable path 与 legacy Fake path 的边界；
- 当前 deferred 能力；
- 本轮 flaky gate 的根因与关闭证据。

不得把未验证的设计设想写成当前能力。若现有文档已经足够，不要重复创建。

## 六、先执行只读工作区检查

```bash
git status --short
git diff --stat
git diff --check
git diff --name-only
git ls-files --others --exclude-standard
```

保留所有用户和前序阶段 tracked/untracked 文件。禁止执行 reset、checkout、clean、删除、commit 或 push。工作区中可能存在名为 `NUL` 的既有文件，不得清理。

再执行定向搜索：

```bash
rg -n -C 30 'TestPostgresQueueExtendVisibilityUsesConditionalLease' trpcservice/queue
rg -n -C 25 'func \(.*\) ExtendVisibility|ErrDeliveryExpired|delivery expired|leased_until|clock_timestamp' trpcservice/queue
rg -n 'VisibilityTimeout|MaxVisibility|ExtendVisibility|60\s*\*\s*time.Millisecond|60ms' trpcservice/queue trpcservice/worker
rg -n 'AtomicCompletion|CommitResultAndAck|ErrCompletionOutcomeUnknown|NewWithAtomicCompletion' trpcservice/storage trpcservice/execution trpcservice/worker
```

先报告当前测试的准确执行顺序：Receive 后是否还有额外 SQL 查询、断言、barrier 或日志操作，再调用 ExtendVisibility；同时报告生产 `ExtendVisibility` SQL 在什么数据库时刻判定 delivery 过期。

## 七、根因判定要求

必须在修改前把根因归入以下一类，并给出证据：

### A. 测试 fixture 时间预算错误

若 Delivery 在调用 `ExtendVisibility` 前已经被 PostgreSQL 判定过期，且生产合同本来就要求“SQL 条件更新执行时仍未过期”，则返回 `ErrDeliveryExpired` 是正确生产行为。此时应修复测试，使“有效 holder 延长成功”和“过期 holder 被拒绝”都由确定性数据库状态证明。

### B. 测试存在无必要的前置工作

若 Receive 后测试先执行额外数据库 round trip、序列化、等待或其他操作，消耗了 60ms lease，应移除这些非必要步骤，或把它们放到不依赖短 lease 的断言之后。

### C. Go 时钟与 PostgreSQL 时钟混用

若测试使用 `time.Now()` 推断数据库 lease 是否有效，而生产 SQL 使用 `clock_timestamp()`，应改为使用 PostgreSQL 时钟建立和验证前置状态，避免时钟偏差及调度开销。

### D. 生产 ExtendVisibility 算法缺陷

只有在能够证明 SQL 执行时 delivery 仍有效，但生产实现仍错误返回 expired，或更新条件/事务/错误分类确有问题时，才允许修改生产 `postgres_queue.go`。必须新增先失败后通过的真实 PostgreSQL回归测试。

禁止在未完成上述判定前直接把 60ms 改成任意更大的值并宣称修复。

## 八、推荐的确定性测试方案

如果根因属于 A/B/C，采用以下测试结构；数值应根据当前 Queue config 和最大 extension 约束选择，不要机械复制：

### 1. 有效 lease 的成功分支

- 使用测试专用但有充分 race instrumentation 余量的 visibility fixture；
- 该数值只属于测试配置，不修改生产默认值；
- 不通过等待其自然流逝验证成功；
- Receive 后立即调用 ExtendVisibility；
- 在调用前可通过同一 PostgreSQL test schema 查询 `clock_timestamp()` 和 `leased_until`，断言 lease 仍有效且剩余 guard margin 明确大于一次测试 SQL 往返预算；
- ExtendVisibility 后查询数据库，断言：状态仍为 `in_flight`、delivery token 未变化、`leased_until` 按合同延长、attempt/delivery count 未被错误改变；
- 使用数据库时间作比较，不以 Go 本地时钟猜测结果。

这里允许为“有效 holder 成功”分支使用更长且明确的测试 lease，因为它是在建立前置条件，而不是放宽生产 timeout 或等待测试通过。报告必须解释这一点。

### 2. 过期 lease 的拒绝分支

- 不使用 `time.Sleep` 等待几十毫秒；
- 在隔离测试 schema 中，通过受控 SQL fixture 把目标 `leased_until` 设置为 `clock_timestamp() - interval ...`，同时保持测试所需的 in-flight/token 状态；
- 调用 ExtendVisibility；
- 断言稳定返回 `ErrDeliveryExpired` 或当前等价 typed error；
- 断言数据库状态、token 和 lease 没有被错误更新。

### 3. stale token 分支

- 使用已有 visibility reclaim/重新领取机制，或受控数据库 fixture 生成新 delivery token；
- 旧 token ExtendVisibility 必须稳定失败；
- 新 token 必须能按合同延长；
- 不依赖 60ms wall-clock 窗口。

### 4. 最大 extension 和条件更新

- 保留当前最大 extension 上限的测试；
- 明确断言超限被拒绝或被当前合同处理；
- 确认 SQL 的 tenant/job/delivery token/state/lease 条件未被测试修复削弱。

不要通过以下方式制造稳定：

- 添加固定 sleep；
- 循环重试直到成功；
- 捕获 `ErrDeliveryExpired` 后把测试当作通过；
- 修改生产算法允许已过期 token 续期；
- 删除 lease expiry 条件；
- 放宽完整 race gate；
- 只降低 `-count`；
- 吞掉 PostgreSQL错误。

## 九、允许修改范围

若确认是测试非确定性，优先只允许修改：

- `trpcservice/queue/postgres_queue_integration_test.go`
- 该文件或 queue test package 内最小的 PostgreSQL fixture helper

若确认生产实现确有直接属于 ExtendVisibility 的 Bug，才允许最小修改：

- `trpcservice/queue/postgres_queue.go`
- 对应 queue unit/integration tests

完成 gate 后允许事实同步：

- `docs/P0-09C验收报告.md`
- `docs/project-status.md`
- `docs/R6最终审查报告.md`，仅当能力边界确实变化
- `docs/plan009background.md`，仅在前述条件满足时
- `docs/P0-09B验收报告.md`，仅当确认 P0-09B 原报告中的测试或生产语义描述需要校准

默认禁止修改：

- `trpcservice/agent/**`
- `trpcservice/gateway/**`
- `trpcservice/web/**`
- `cmd/**`
- `trpcservice/tenant/**`
- `trpcservice/session/**`
- `trpcservice/execution/**`
- `trpcservice/storage/**`
- `trpcservice/worker/**`
- migrations
- R4/R5 delivery/shutdown 算法
- Outbox、Retry/DLQ
- `docs/plan008.md`
- `go.mod`、`go.sum`

如果根因要求修改禁止范围，停止并输出唯一 blocker，不要扩大任务。

## 十、测试和验证顺序

数据库只使用本地测试 PostgreSQL。DSN 必须来自测试进程的 `TEST_DATABASE_URL`，不得写入源码、文档或日志，不得连接生产数据库。

### 1. 建立修改前复现证据

实际命令以当前 package path 为准：

```bash
go test ./trpcservice/queue -run '^TestPostgresQueueExtendVisibilityUsesConditionalLease$' -race -count=20 -v
```

记录：

- PASS/FAIL/SKIP；
- 每次失败发生在哪一步；
- PostgreSQL 侧 lease 是否在 ExtendVisibility SQL 前已过期；
- 测试是否真实连接 PostgreSQL；
- 不输出 DSN 凭据、payload、History 或敏感内容。

若 20 次没有复现，不得直接宣称问题消失。结合当前测试代码和历史复现判断是否仍存在短 lease 竞态，并继续修复确定性前置条件。

### 2. 修改后的 focused stability gate

```bash
go test ./trpcservice/queue -run '^TestPostgresQueueExtendVisibilityUsesConditionalLease$' -race -count=25 -v
```

要求：25/25 PASS，0 SKIP，0 FAIL，且不是通过 retry-until-success 或忽略 expired 获得。

再运行 ExtendVisibility/visibility 相关真实 PostgreSQL测试：

```bash
go test ./trpcservice/queue -run 'TestPostgresQueue.*(ExtendVisibility|Visibility|Reclaim|Delivery)' -race -count=10 -v
```

实际 regex 应先根据当前测试名核对，不要让“0 tests matched”被误报为通过。

### 3. P0-09B Queue 回归

```bash
go test ./trpcservice/queue/... -count=1 -v
go test ./trpcservice/queue/... -race -count=1
```

确认真实 PostgreSQL Queue acceptance 有实际运行，不是全部 SKIP。

### 4. P0-09C 专用 atomic completion 回归

先从当前源码定位真实测试名，然后运行对应 focused tests。必须覆盖：

- normal atomic completion；
- duplicate/reconciliation；
- fencing/takeover；
- result insert failure；
- queue ack update failure；
- commit outcome unknown；
- Worker `NewWithAtomicCompletion` integration；
- no legacy independent Ack on durable success path。

不得只依赖历史报告的 `27 PASS`。

### 5. 当前唯一完整 race gate

```bash
go test ./trpcservice/queue/... ./trpcservice/storage/... ./trpcservice/execution/... ./trpcservice/worker/... -race -count=1
```

该命令必须本轮实际 exit 0，不能有 FAIL。环境变量 gated 的无关 Redis/Docker 测试可以按既有合同 SKIP，但必须记录具体 SKIP 数和原因；真实 PostgreSQL P0-09B/P0-09C 核心测试不能 SKIP。

### 6. 普通受影响包、vet 和格式

```bash
go test ./trpcservice/queue/... ./trpcservice/storage/... ./trpcservice/execution/... ./trpcservice/worker/... -count=1 -v
go vet ./trpcservice/queue/... ./trpcservice/storage/... ./trpcservice/execution/... ./trpcservice/worker/...
test -z "$(gofmt -l trpcservice/queue trpcservice/storage trpcservice/execution trpcservice/worker)"
git diff --check
```

边界扫描：

```bash
rg -n 'trpc\.group/trpc-go/trpc-agent-go' trpcservice/queue trpcservice/storage trpcservice/execution trpcservice/worker
rg -n 'cause\.Error\(\)|Error\(\).*Reason|Reason.*Error\(' trpcservice/queue trpcservice/worker
rg -n 'ExtendVisibility|leased_until|clock_timestamp|delivery_id|tenant_id|job_id' trpcservice/queue migrations/000004_job_queue.up.sql
rg -n 'NewWithAtomicCompletion|AtomicCompletion|CommitResultAndAck|ErrCompletionOutcomeUnknown' trpcservice/storage trpcservice/execution trpcservice/worker
```

第一条必须无 framework import。第二条不能出现原始错误透传到 durable reason。后两条需要人工核对，不是关键词出现即通过。

除非当前公共接口变化影响全仓，否则本轮不要求重复 `go test ./...`。若你认为必须运行全仓，先说明依赖原因。

## 十一、诊断和证据要求

测试失败时只输出安全诊断：

- test case；
- tenant 的测试别名或 hash；
- JobID/DeliveryID 的安全 hash；
- Queue state；
- PostgreSQL `clock_timestamp()`；
- `leased_until` 与数据库时间的差值；
- action 类型；
- typed error 分类；
- affected rows。

不得输出：

- `TEST_DATABASE_URL`；
- 密码；
- Authorization；
- Prompt/History 完整内容；
- Provider body；
- 任意生产数据。

必须明确说明测试稳定性来自确定性前置状态，而不是更宽松的生产语义。

## 十二、P0-09C-B1 关闭条件

只有以下条件全部满足，P0-09C-B1 才能 `verified`：

- flaky test 的根因已由当前源码和真实 PostgreSQL状态证明；
- 有效 lease、过期 lease 和 stale token 分支不再依赖 60ms 调度窗口；
- 未允许过期 holder 续期；
- focused race stability gate 25/25 PASS；
- visibility 相关 race repetition 通过；
- P0-09B Queue acceptance 通过且真实 PostgreSQL测试未 SKIP；
- P0-09C atomic completion focused acceptance 通过；
- 完整受影响范围 race 命令 exit 0；
- 普通受影响包、vet、gofmt 和 diff check 通过；
- 没有修改无关生产逻辑；
- 没有启动 P0-09D、Outbox、Retry/DLQ 或生产装配。

只有 P0-09C-B1 关闭后，才允许把 P0-09C 更新为：

`verified`

P0-09C 的 verified 结论仍只能说明：

- 真实 PostgreSQL 同一 transaction 内的 fenced result commit + Queue Ack；
- 真实 PostgreSQL rollback、reconciliation、token/fence/tenant 校验；
- 当前测试覆盖的 commit outcome unknown；
- durable Worker 明确使用 atomic constructor。

不得外推为：

- Outbox；
- Retry/DLQ；
- 多数据库/异构 Queue 的原子性；
- 完整 OS 进程 crash；
- 生产 SIGTERM；
- Web/CMD 异步装配；
- 真实 IM、鉴权或 Telemetry；
- tRPC-Agent-Go framework global Wait/Done。

## 十三、文档更新规则

代码和测试 gate 关闭前不要把 `project-status.md` 或 R6 报告写成 P0-09C complete。

成功关闭后：

1. 更新 `docs/P0-09C验收报告.md`：
   - 将状态更新为 `verified`；
   - 增加 blocker 根因；
   - 说明测试修复为何是确定性前置条件，而不是放宽生产 timeout；
   - 记录本轮 focused 25 次、visibility repetition、完整 race gate、普通测试和 vet 的实际结果；
   - 保留 commit unknown 和证据边界。
2. 最小更新 `docs/project-status.md`；
3. 仅在能力边界发生变化时更新 `docs/R6最终审查报告.md`；
4. 若创建 `docs/plan009background.md`，确保内容全部来自已验证事实；
5. 不修改 `docs/plan008.md`。

如果任一 gate 仍失败：

- P0-09C 保持 `in_progress / blocked`；
- 不修改状态为完成；
- 输出唯一 blocker；
- 给出失败测试、数据库前置状态、已确认根因、允许范围内的最小下一步；
- 不开始 P0-09D。

## 十四、最终回复格式

最终输出 `P0-09C-B1 Blocker Closure Report`：

1. `Final Status`：P0-09C-B1 和 P0-09C 状态；
2. `Root Cause`：测试预算、测试顺序、时钟边界或生产 Bug，附实际证据；
3. `Changes`：实际修改文件；
4. `Deterministic Test Design`：有效、过期和 stale token 如何建立；
5. `Focused Stability Evidence`：次数、PASS/FAIL/SKIP；
6. `PostgreSQL Queue Regression`；
7. `Atomic Completion Regression`；
8. `Full Race Gate`；
9. `Vet / Formatting / Diff / Boundary Scans`；
10. `Commands Not Run` 及原因；
11. `Documentation Updates`；
12. `Residual Risks and Deferred Work`；
13. `Workspace Preservation`：确认未 reset、checkout、clean、删除、commit 或 push；
14. 明确写：`P0-09D not started`。

不要为了得到 `verified` 而隐藏 flaky、减少测试次数、允许已过期 token 续期或修改无关系统。
```

## 计划摘要

这一步只关闭 P0-09C 的唯一 race gate，不开始新功能。默认判断路径是先用 PostgreSQL 时钟证明 60ms fixture 是否在 SQL 执行前已过期；若是，则把成功、过期和 stale token 三类状态改成数据库驱动的确定性测试，而不是修改生产 lease 语义。完成 focused 重复 race、P0-09B Queue 回归、P0-09C atomic completion 回归和完整受影响范围 race 后，才允许把 P0-09C 更新为 `verified`。
