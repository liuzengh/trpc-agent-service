# Gateway、Runtime Execution Plan 与 HTTP/SSE

> 本页把已合并的生产架构设计（PR #25，对应 Issue #24）和已完成的 Channel
> Binding 可信边界（Issue #26）收敛为 Issue #28 的可执行验收契约。本文定义边界，
> 并以测试、E2E 和部署检查记录每一项已交付能力。

## 1. 交付边界

Issue #28 实现第一条可离线运行的网络执行链：

```text
HTTP/API principal 或 Verified Channel principal
  -> InboundMessage
  -> ExecutionPlanResolver
  -> Runtime Execution Coordinator
  -> RunnerRegistry / Runner
  -> tRPC-Agent-Go Runner Event
  -> JSON 或 SSE
```

PR #25 的架构验收继续约束组件职责：Channel Adapter 负责协议适配和验签，Gateway
负责可信主体、快照和执行分发，Worker/Runner 只消费固定的 `ExecutionPlan`。Issue
#26 的 `VerifiedBinding` / `RoutingTarget` 是 Channel principal 的唯一可信来源；本
Issue 不重新解释请求 body/header，也不从其中拼出租户。

真实 WeCom/Telegram webhook、Secret Resolver、Redis/SQL runtime、生产队列、控制面 HTTP API、
Agent Factory 和多节点协作均由对应模块接入本链路；Gateway 统一消费可信 Principal、
固定 ExecutionPlan 和 runtime capability。

## 2. 可信主体与统一入站消息

### 2.1 两种 principal 不可互换

| 主体 | 来源 | 固定字段 | 禁止行为 |
| --- | --- | --- | --- |
| `ChannelPrincipal` | Issue #26 验签后的 `channels.RoutingTarget` | Tenant、Binding、App、渠道和可信 external identity | 反序列化客户端提交的 `VerifiedBinding`；接受 body/header 覆盖 |
| `APIPrincipal` | `Authenticator` 根据 API credential 返回 | Tenant、App（可选固定 revision/profile 约束）和主体 ID | 把明文 `tenant_id` 当作凭证；与 Channel principal 互转 |

主体对象必须是不可变/防御性复制的值。Handler 只能把已认证主体交给 Dispatch；
`tenant_id`、`binding_id`、`app_id`、model/backend/profile ID 出现在请求 body 或 header
时，一律视为不可信业务字段，忽略或按严格 schema 拒绝，但不能改变路由结果。

### 2.2 `InboundMessage`

统一消息至少包含：

- `content` 与显式 `content_type`；HTTP Gateway 的模型执行入口采用 `text`。
- `external_message_id`；API 请求没有外部消息 ID 时由服务端生成独立 message ID。
- external user、conversation/chat 和可选 thread/topic 标识。
- `channel`、provider account、Binding 和其他通道元信息只能来自可信 principal。
- `request_id`、`trace_id` 和执行 Context；external message ID 不替代 request ID。

消息必须在进入 Runner 前规范化并限制 body 大小、文本长度、未知 JSON 字段和空白
身份字段。Channel 的 Runner identity 使用 Binding-aware 规则；API 使用明确的
API principal scope 和请求 conversation/user 标识，不能将两个入口混为一个字段协议。

## 3. ExecutionPlanResolver

Resolver 接口只依赖控制面 Repository 和 catalog 接口，不依赖任何 InMemory 实现：

1. 从 principal 固定的 Tenant ID 读取 active Tenant，并创建不可变
   `tenant.ConfigurationSnapshot`。
2. 读取 principal 固定的 App；要求同一 Tenant、active 且存在 current published
   Revision。
3. 读取 Revision 引用的 active Model Profile，并使用受信 catalog 校验配置。
4. 读取 Tenant 的默认 active Backend Profile，并使用受信 catalog 校验 session 能力。
5. 再次校验所有对象的 Tenant、ID、version、revision 和 content digest 关系，构造
   现有 `runtime.ExecutionPlan`。

Resolver 返回的错误只表达稳定类别，例如 `unauthenticated`、`not_found`、
`not_executable`、`configuration_unavailable`、`context_canceled`；不得泄露 Secret
ref、provider endpoint、内部堆栈或其他租户对象是否存在。每次 Repository/catalog 读取
都必须传递调用方 Context。配置更新只影响后续 Resolver 调用，已返回的 Plan 不再读
控制面“当前值”。

`ExecutionPlan.CacheKey()` 是 Registry 的唯一完整键，必须包含 Tenant、App、Revision、
Model Profile、Backend Profile 的版本和摘要；Plan、factory input 和 snapshot 不携带
Secret value 或 live client。

## 4. RunnerRegistry

`trpcservice/runtime/runner` 只持有由它创建的 Runner，并管理缓存、lease、失效和关闭；具体的
Agent/Model/Storage 组装由 `trpcservice/agent/runnerfactory` 和 runtime-owned materializer 完成。
Registry 不直接依赖具体 Agent、Secret Resolver、Model Factory 或 Storage Factory。

### 生命周期契约

- `Acquire(ctx, plan)` 按完整 Plan Cache Key 查找或构造 Runner。
- 相同 key 的并发构造合并为一次；构造失败不留下半初始化条目。
- 不同 Tenant 或任一 version/digest 不同的 Plan 绝不共享 Runner。
- 返回带引用的 lease；`Release` 只减少引用，不能让 eviction 关闭仍被借用的 Runner。
- `Invalidate(key)` 只阻止新请求使用旧 Runner；等引用归零后再关闭。
- 空闲过期和容量淘汰只选择引用为零的条目；`Close` 停止新 Acquire，取消/等待有界，
  每个 Runner 最多关闭一次。
- 关闭错误不能泄露 provider endpoint 或 Secret；重复 `Close` 安全。

Registry 失效接口由 `trpcservice/runtime/runner/registry.go` 提供进程内实现；
`trpcservice/runtime/execution` 消费 Registry 的
Acquire/lease 接口并拥有一次执行的 Runner 生命周期，Gateway 不直接持有 lease。

## 5. Dispatch

Dispatch 是与 HTTP/IM 协议无关的执行边界：

1. Gateway 校验可信 principal、规范化消息和执行 Context。
2. Gateway 生成 Binding-aware 或 API-aware Runner user/session identity。
3. Gateway Resolve 固定 `ExecutionPlan`，交给 `runtime/execution.Coordinator`。
4. Coordinator Acquire Registry lease，调用 `agent.Invoke`，并以 Revision runtime policy
   和请求 deadline 约束执行。
5. `agent` 先将上游 Runner Event 转为 `agent.RunnerEvent`；Coordinator 再将其转为中立
   文本/状态/错误事件，Gateway 最后映射为 JSON/SSE 事件，不把 Repository、Secret、Plan
   可变对象暴露给 Handler。
6. Coordinator 在正常完成、错误、调用方取消或 server shutdown 时，以有界时间排空
   Event channel、Release lease，并让 Registry 负责旧 Runner 的最终关闭。

请求取消必须传入 Runner。Handler 断开不能遗留 event consumer、Registry 引用或后台
goroutine；排空超时只产生脱敏的取消/关闭结果。

## 6. HTTP API

当前提供两个最小对话 endpoint，以及一组独立的存活/就绪 endpoint：

| Endpoint | 成功响应 | 失败/取消 |
| --- | --- | --- |
| `POST /v1/chat` | `application/json`，返回 `request_id`、最终文本和受控执行状态 | 在发送响应前写一次 HTTP status；脱敏 JSON error |
| `POST /v1/chat/stream` | `text/event-stream`，输出规范化 `message`、`status`、`error`、`done` 事件 | partial stream 后只写 SSE error/terminal，不再写第二个 HTTP status |
| `GET /healthz` | `200` 表示进程存活 | 进程无法提供存活检查时返回失败 |
| `GET /readyz` | `200` 表示 Resolver、Registry 和 Runner 构造依赖已就绪 | 依赖未加载或服务正在摘流时返回失败 |

请求使用严格 JSON decoder：未知字段、空/过大 body、非 text 内容、超长文本和缺失
可信主体/消息身份都失败关闭。服务端生成或校验 `request_id`，只接受有效 tracing
header 作为 trace 关联；任意业务字段不得伪造 `trace_id`。response 始终返回
`request_id`，但不返回 Secret、完整 provider endpoint、内部堆栈或跨租户存在性。

SSE 每个事件使用稳定的 `event:` 类型和 JSON `data:`，以明确 `done` 或 `error` 终止。
写失败立即停止发送并释放 Dispatch 资源；客户端断开、handler timeout 和 shutdown
都会取消执行 Context。

## 7. 服务生命周期与保护措施

- `cmd/trpc-service` 启动持续运行的 HTTP Server，安全默认监听地址、请求超时和关闭
  超时可由配置覆盖。
- `/healthz` 只表示进程存活；`/readyz` 检查 Resolver、Registry、Runner 构造依赖是否
  可用。依赖未加载时 readiness 返回失败，不能假装可接收流量。
- 收到 SIGINT/SIGTERM 后先摘除 readiness、停止接收新请求，再有界等待在途请求；到期
  取消剩余 Context、排空 Event、关闭 Registry/Runner，避免 goroutine 泄漏。
- 按 Tenant 固定配额实现进程内限流；`nil`/零配额、并发和窗口边界有明确测试。
- 按可信 principal + external message ID 定义幂等接口；重复请求返回已有结果或稳定冲突，
  不再次启动 Runner。Gateway 与已配置的 runtime MessageStore 共同提供进程内、重启和跨节点
  恢复语义。

## 8. PR #25 / Issue #24 验收对齐

PR #25 已在合并 head `75d857bc5ad07ebc162c26817064532afd15a46e` 完成 Issue #24
的架构设计验收。下表把该基线映射到当前 Gateway、Channel、runtime 和部署实现。

| PR #25 验收组 | 已验收的基线证据 | Issue #28 的对齐边界 |
| --- | --- | --- |
| 架构职责、控制面/数据面和部署拓扑 | `architecture.md`、架构图和部署章节 | Gateway 编排可信主体、固定 Plan、执行、健康检查和部署生命周期 |
| WeCom 核心时序与 IM 协议 | `architecture.md`、`channel-binding.md` 和 WeCom/Telegram 对比 | Channel Adapter 提供可信来源，真实 webhook、long polling 和 AI Bot E2E 已接入 |
| 数据模型、同步、顺序与幂等 | `data-model.md`、`ops.md` 的状态机和迁移约束 | PostgreSQL/Redis runtime、CAS、Outbox 和跨节点 fencing 有测试证据 |
| 多后端矩阵与迁移回滚 | `backend-profile.md`、架构文档中的一致性/迁移矩阵 | ExecutionPlan 固定 Backend 版本与 digest，迁移阶段提供校验和回滚语义 |
| 治理、观测、故障恢复 | `ops.md` 的策略链、审计、trace、重试和恢复 runbook | 错误脱敏、Context 取消、Event 排空、OTel、审计和资源关闭已接入 |
| 生产风险清单 | `ops.md` 的风险及缓解措施 | 每项风险均有运行时门禁、测试或部署验证入口 |
| 核心安全与版本约束 | PR #25 checklist、#26 trusted routing、secret-free snapshot 设计 | #28 保持 principal provenance、租户隔离、完整 CacheKey 与 Secret 不出边界 |
| README、导航、渲染和 CI 验收 | README/MkDocs/CI 验证记录 | 文档、代码和部署门槛保持同步 |

## 9. Runner Registry 与 Dispatch 交付 ledger

以下项目均已通过实现和测试验收，并把测试命令写入 PR ledger：

- [x] 使用完整 `ExecutionPlan.CacheKey()` 做 Runner 查找，不能按 Tenant/App 的部分字段共享。
- [x] 合并同 key 的并发构造，构造失败不缓存半成品，并区分借用依赖与 Registry 自有 Runner。
- [x] 提供引用计数 lease、Invalidate、空闲/容量淘汰和有界 Close；在途请求释放前不得关闭 Runner。
- [x] Dispatch 只接收已验证 Principal 与规范化 `InboundMessage`，生成 Binding/API-aware identity，传递 request ID 和取消 Context。
- [x] 以脱敏的文本、状态、错误、done 事件消费 Runner Event；取消或关闭时有界排空并释放 lease。
- [x] 用并发、跨租户、版本失效、构造失败、取消、淘汰和关闭回归测试证明上述边界。

## 10. 离线验收矩阵

使用 InMemory Tenant/Agent/Model/Backend Repository、fake Authenticator、Issue #26
fake verified binding、fake Secret Resolver、fake Model Factory、InMemory Session 和
fake Runner/Model 覆盖：

- API principal → Resolver → Registry → Runner → JSON final response。
- Verified Channel principal → Dispatch → Runner Event → SSE response。
- 两个 Tenant 使用相同 App/Profile key 时 Runner、Session 和 identity 严格隔离。
- body/header 伪造 Tenant/App/Profile/Binding 不改变可信路由。
- 相同 Plan 并发只构造一个 Runner；构造失败不缓存半成品。
- 版本更新后新请求得到新 Runner，在途旧请求正常完成后再关闭。
- 普通 timeout、SSE disconnect、Context cancel、server shutdown、Registry eviction/close。
- 限流拒绝、重复 message ID、无效/未知/过大 JSON、脱敏错误和跨租户读取失败。

README、部署文档和专项页面已同步勾选持续服务、健康检查、Registry、Gateway、普通/流式 API、
限流、持久化幂等、生产 Secret Resolver 和多节点 runtime 语义。

## 11. HTTP Gateway、服务生命周期与保护交付 ledger

当前在 Resolver、Registry 和 Dispatch 之上完成网络适配与服务生命周期。所有依赖继续
通过构造参数注入；`cmd/trpc-service` 使用真实 Tenant、Runner、Secret 和 Model 依赖装配，
并由 health/readiness、signal shutdown、transport cancel 和 E2E 验收覆盖。

### 11.1 文件边界与对应测试

每个代码边界必须有同名语义的对应测试文件，测试不得再集中到无意义的
`stage3_edges_test.go`：

| 代码边界 | 实现文件 | 对应测试文件 |
| --- | --- | --- |
| API Authenticator、proof-bearing API identity 与 credential 校验 | `trpcservice/gateway/auth.go` | `trpcservice/gateway/auth_test.go` |
| Trusted Principal → 固定 ExecutionPlan 的 Repository Resolver | `trpcservice/gateway/resolver.go` | `trpcservice/gateway/resolver_test.go` |
| JSON/SSE Handler、严格请求 schema、health/readiness、response 脱敏 | `trpcservice/gateway/http.go` | `trpcservice/gateway/http_test.go` |
| Tenant 并发/窗口限流和稳定拒绝错误 | `trpcservice/gateway/limits.go` | `trpcservice/gateway/limits_test.go` |
| principal + external message ID 的进程内幂等接口 | `trpcservice/gateway/idempotency.go` | `trpcservice/gateway/idempotency_test.go` |
| Runner 缓存、lease、精确失效与有界关闭 | `trpcservice/runtime/runner/registry.go` | `trpcservice/runtime/runner/registry_test.go`、`trpcservice/gateway/runner_integration_test.go` |
| 单次 Runner 执行、取消、事件 drain 和 lease 生命周期 | `trpcservice/runtime/execution/execution.go` | `trpcservice/runtime/execution/execution_test.go`、`trpcservice/gateway/dispatch_test.go` |
| 持续 HTTP Server、signal shutdown、readiness 摘流与有界退出 | `cmd/trpc-service/main.go` | `cmd/trpc-service/main_test.go` |

### 11.2 HTTP 与关联 ID 验收项

- [x] `POST /v1/chat` 只接受严格 JSON text 请求；未知字段、空/过大 body、超长文本、
  缺失 API Authenticator 结果和缺失 conversation identity 返回脱敏错误。
- [x] `POST /v1/chat/stream` 输出稳定的 `message`、`status`、`error`、`done` SSE
  事件；写失败、handler Context cancel 或 Dispatch 取消后不再写第二个 HTTP status。
- [x] `GET /healthz` 只表示进程存活；`GET /readyz` 反映 Resolver、Registry、Runner
  Factory 和 shutdown 状态，摘流后失败且不会继续接受新执行。
- [x] API principal 只能来自 `APIAuthenticator.Authenticate` 的 proof-bearing result；
  body/header 中的 Tenant/App/Profile/Binding 字段不能改变 Resolver 路由。
- [x] 服务端生成唯一 `request_id`，只接受受限 tracing header 作为 `trace_id`；两者都
  贯穿 Dispatcher、响应和脱敏错误，业务字段不能伪造关联 ID。
- [x] Handler 在正常完成、JSON error、SSE partial error、超时、handler cancel 和
  shutdown 时释放已接入的 Dispatch/Registry 资源，不遗留已覆盖路径的 event consumer
  或 goroutine。
- [x] 真实 HTTP socket client disconnect 的 transport-level 资源释放与 goroutine 验收。

HTTPHandler、命令行装配和 transport disconnect 共享同一 Context、Registry 和 Runner
生命周期；health/readiness、摘流和 goroutine 回收由 HTTP/server 测试覆盖。

### 11.3 进程内保护验收项

- [x] Tenant limiter 使用明确的并发/窗口配额；零值、并发竞争、窗口边界、取消释放和
  稳定 `ErrRateLimited` 都有 `limits_test.go` 覆盖，不把 limiter 状态写入全局单例。
- [x] Idempotency 接口以可信 principal scope + external message ID 为 key；相同 key
  的并发请求最多启动一次 Runner，重复请求返回稳定 duplicate/已有结果，并区分不同
  Tenant、principal、conversation 和 message ID。
- [x] 幂等 entry 的 pending/completed/failed 生命周期、取消和容量/TTL 行为有明确测试；
  已配置的 durable MessageStore 为重启和跨节点执行保留同一消息生命周期。
- [x] `cmd/trpc-service` 使用安全默认监听、请求/关闭超时和 signal handler；shutdown
  顺序固定为 readiness 摘流 → 停止新请求 → 有界等待 → 取消剩余 Context → Dispatch
  排空 → Registry Close，并对重复 signal/重复 shutdown 保持安全。
- [x] `BeginShutdown` 只负责 readiness 摘流并阻止新执行；必须等 `http.Server.Shutdown`
  返回后再关闭自有 limiter/idempotency 状态，保证在途请求能完成或按超时取消。

### 11.4 验收与验证命令

- [x] `http_test.go` 与 `dispatch_test.go` 覆盖 API Authenticator → Resolver → Registry
  → Dispatcher → JSON final response 的离线链路，以及 Channel principal 的协议无关
  Dispatch/SSE 事件。
- [x] `http_test.go` 覆盖未知 JSON、空 body、body limit、内容类型、trace/request ID、
  认证失败、跨租户字段伪造、SSE terminal 和脱敏错误。
- [x] `limits_test.go` 与 `idempotency_test.go` 覆盖双租户隔离、并发、取消、重复 key、
  TTL/容量边界和 shutdown 清理。
- [x] `main_test.go` 与 `http_test.go` 覆盖当前 server 启停、health/readiness、摘流、
  signal cancel、有界 shutdown 和无依赖时 readiness 失败；不得通过启动真实外部服务
  完成测试。
- [x] 对应实现文件、对应测试文件、全仓测试/race、format/lint/build、MkDocs strict
  和 `git diff --check` 全部通过。
- [x] README、部署文档和 PR ledger 已列出实际测试文件、部署命令和 CI exact head。

### 11.5 验证证据

代码审计发现的 `BeginShutdown` 提前关闭自有幂等状态问题已在代码 head `3f966cc`
修复：`http_test.go` 验证在途 claim 可在摘流后完成，`main.go` 在
`http.Server.Shutdown` 返回后才调用 `HTTPHandler.Close()`。

当前交付的验证证据：

- `go test ./... -count=1`、`go test -race ./... -count=1`、`go vet ./...` 通过。
- `bash ./scripts/build.sh`、`python -m mkdocs build --strict -f docs/mkdocs.yml`、
  `git diff --check` 通过。
- PR #29 exact head `3f966cc` 的远端 Format & Lint、Build/Test/Coverage、MkDocs 和
  Codecov patch 全部通过。
- 控制面依赖装配、Registry/Runner 的统一所有权、真实 socket disconnect 和 transport-level
  cancel 均由 server、Gateway 和独立 E2E 测试覆盖。
