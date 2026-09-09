# Phase 3 Gateway/Worker 可靠消息验收结论

> 验证日期：2026-08-31
> 代码基线：`b1ae13d`
> 实施分支：`feature/phase3-reliable-messaging`

## 结论

Phase 3 将 Phase 2 的多租户 Runtime 接入统一的 Redis 可靠消息链路。`gateway`、`worker` 和兼容入口 `serve` 均不再直连执行 Agent：

```text
Demo HTTP -> Gateway 可信 Binding 解析 -> Inbox 原子去重
  -> Redis Stream agent.tasks -> 单并发 Worker
  -> 固定 ConfigVersion Runtime.Execute
  -> Inbox 唯一终态 + agent.replies -> Gateway HTTP 响应
```

本阶段提供至少一次投递与幂等终态，不承诺 Agent 副作用绝对只执行一次。任一时刻只验收一个 Worker；双 Worker 并发、Session 分布式锁和跨 Worker 顺序属于 Phase 4。

## 启动与配置

新增三个启动角色：

- `trpc-service gateway [-addr :8080] [-consumer name]`
- `trpc-service worker [-health-addr :8081] [-consumer name]`
- `trpc-service serve [-addr :8080]`

`serve` 在单进程中启动 Gateway 和一个 Worker，但仍完整经过 Redis Streams。`start.sh` 继续调用 `serve`。未指定 consumer 时，名称由角色、hostname、PID 和随机后缀组成。

配置保持 `schema_version: 1`，新增顶层 `messaging`。示例见 `configs/phase3.example.json`。JSON Gateway 只解析目录、`IDENTITY_SECRET` 和 Messaging Redis 凭据；Worker/serve 还校验模型与所有启用 StorageProfile 的凭据。Legacy Gateway 保留旧目录环境变量要求，但不会读取 `MODEL_API_KEY_ENV` 指向的模型密钥值。

Gateway 和 Worker 必须共享同一个 `IDENTITY_SECRET`。Messaging Redis 使用独立客户端和以下命名空间：

```text
<prefix>:reliable-v1:agent.tasks
<prefix>:reliable-v1:agent.replies
<prefix>:reliable-v1:agent.retry
<prefix>:reliable-v1:inbox:<sha256>
```

Consumer Group 固定为 `agent-workers-v1` 和 `agent-gateways-v1`，通过 `XGROUP CREATE ... 0 MKSTREAM` 幂等初始化。

## 路由与固定版本执行

- 独立 `routing.Router` 只持有 Repository 和身份密钥；Gateway 不构造 BackendProvider、RunnerRegistry 或完整 Runtime。
- Router 从可信 Binding 派生租户、Agent、活动 ConfigVersion、Runner user 和 Session，并生成版本化 `ExecutionTask`。
- Worker 调用 `Runtime.Execute(ExecutionTask)`，执行任务创建时固定的 ConfigVersion，不重新选择最新活动版本。
- Runtime 在执行前重新验证 Binding、Tenant、AgentApp、ConfigVersion 关联关系。
- 原 `Runtime.Handle` 保留为 `Resolve -> Execute` 兼容包装。

## Inbox、租约与恢复

Inbox 键由 tenant、binding 和平台 message ID 的 length-prefixed SHA-256 生成。payload digest 覆盖路由、身份、版本、Session 和正文，不包含请求 trace、接收时间或 attempt。

- 同键同 digest 复用现有任务或终态结果。
- 同键不同 digest 返回 `409 message_conflict`。
- Worker 执行前重新计算 digest，并与 Inbox 中保存值比较。
- 状态机为 `queued -> processing -> succeeded/failed_terminal`，或 `processing -> retry_wait -> queued`。
- 非终态 Inbox 不过期；终态按 `inbox_retention` 设置 TTL，默认 24 小时。
- 每次 Begin 递增 `lease_epoch`；Complete/Fail/Retry 必须同时匹配 owner、epoch 和 stream ID。
- 每个活跃任务使用独立心跳 goroutine 续租，并通过 `XCLAIM JUSTID` 重置 Pending idle。
- 到期重试由单个 Lua 脚本原子选择、入队并从 ZSET 删除。
- `XAUTOCLAIM` 同时恢复 processing Pending 和 Worker 在 Read 后、Begin 前崩溃留下的 queued Pending。
- Worker 在 `Begin` 因租约竞争失败时，会安全重排自身 Pending entry；若 Inbox 已被其他状态接管，则不会误删或重复入队。
- 崩溃与优雅关闭都会增加 attempt，并受相同 `max_attempts` 限制。

所有 Lua 脚本在修改前检查状态、task、digest、attempt、owner/epoch 及 Redis key 类型。成功、失败和重试均先保存目标状态，再执行 `XDEL`、`XACK`。

## HTTP 与健康语义

成功响应保持原 Demo JSON 契约。Gateway 默认等待 75 秒；如果任务仍在执行或重试，返回：

```text
HTTP 504
code: task_pending
```

任务不会被取消。客户端必须用相同 `message_id` 和完全相同的业务 payload 重试。每次 HTTP 请求使用自己的 request ID；成功、终态失败和 `task_pending` 均返回原始 Agent 任务 trace ID。

稳定错误映射：

| 场景 | HTTP / code |
| --- | --- |
| 非法请求 | `400 invalid_request` |
| 未知 Binding | `404 binding_not_found` |
| payload 冲突 | `409 message_conflict` |
| Messaging、配置或存储不可用 | `503 not_ready` |
| 任务仍运行 | `504 task_pending` |
| 模型超时终态 | `504 model_timeout` |
| 其他 Agent 终态失败 | `502 agent_failed` |
| Worker 丢失终态 | `502 worker_lost` |

Gateway `/readyz` 检查 Messaging Redis、Group 和回复消费循环；Worker `/readyz` 同时检查 Messaging 与 Runtime。`/healthz` 只表示进程和主循环存活。Redis 故障恢复后无需重启即可重新 ready。

## 自动化验收

测试覆盖：

- JSON/legacy 角色化加载、默认值、非法组合、未知字段、密钥读取边界和完整脱敏。
- 50 路并发重复提交只创建一个 Inbox/任务，跨租户隔离和 payload 冲突。
- Redis key 类型错误在 Lua 修改前失败，不产生部分任务写入。
- digest 篡改进入 `invalid_task`，不调用 Agent。
- Runtime 固定执行任务指定 ConfigVersion。
- 独立心跳阻止活跃任务被 stale claim，迟到租约不能提交。
- 临时失败重试、原子到期调度、Begin 前崩溃和 processing Pending 恢复。
- 反复优雅关闭不会绕过 `max_attempts`。
- Gateway 终态缓存、回复通知加 Inbox 轮询、409/504 和原始 trace 语义。
- Gateway/Worker/serve 参数、readiness 和幂等关闭回归。

真实 Redis 7 契约测试由以下命令显式启用，日常测试在未设置 URL 时跳过：

```text
PHASE3_REDIS_SMOKE_URL=redis://localhost:6379/0 go test ./trpcservice/messaging -run TestRedis7ReliableMessagingSmoke -v
```

关闭门槛包括 `go test ./...`、`go test -race ./...`、`go build ./...`、`go vet ./...`、`go mod verify` 和真实 Redis 7 Smoke。

## 已知边界

- 幂等窗口等于 `inbox_retention`；终态过期后相同 message ID 会作为新消息执行。
- Agent 已产生外部副作用、但结果尚未写入 Inbox 时 Worker 崩溃，恢复后可能再次调用 Agent。
- 普通模型错误缺少结构化 HTTP 状态，Phase 3 不解析错误字符串猜测 4xx/5xx，而是统一进行有限重试。
- Gateway 不检查活跃 Worker 数量；没有 Worker 时任务仍会入队，HTTP 最终返回 `task_pending`。
- Phase 3 不提供双 Worker 并发接管、Session 分布式锁、dead-letter Stream、管理重放 API、SQL Repository 或完整 OTel。
