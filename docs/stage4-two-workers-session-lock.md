# Phase 4 双 Worker、共享 Session 与强 Fencing 验收结论

> 验证日期：2026-09-02
> 代码基线：`b758731`
> 实施分支：`feature/phase4-two-workers-session-lock`

## 结论

Phase 4 在 Phase 3 Reliable Messaging 上增加显式 `strong` Session fencing。两个独立 Worker 可以共享 Consumer Group：同一 Session 按 Redis 分配的 `session_seq` 严格串行，不同 Session 可以并行；Worker 丢失后，只有 task lease 和 Session lock 都失效，其他 Worker才可以接管原 Pending。

```text
Gateway -> Inbox/agent.tasks -> Worker A / Worker B
                               -> task heartbeat
                               -> Session heartbeat
                               -> local turn staging
                               -> commit_turn_with_session
                                  (Session + Inbox + Reply + cursor + unlock)
```

`session_fencing` 默认仍为 `legacy`，保持 Phase 3 行为。Strong 必须显式开启，并要求 Messaging 与所有活动 Session StorageProfile 使用同一 Redis URL、logical DB 和 primary `run_id`。Strong 拒绝 InMemory、Redis Cluster、跨 Redis、不可验证代理和 fallback。

## 统一坐标与 Key

平台内部统一使用 `trpcservice/keyspace`。外部字段先按 `uint64_be(length) || bytes(value)` 编码，再计算小写 SHA-256。

```text
session_coord = hash(tenant_id, channel_binding_id, runner_user_id, session_id)
user_coord    = hash(app_name, user_id)

coordination = <prefix>:reliable-v1
fenced       = <prefix>:reliable-v1:fenced-v1
```

主要键：

```text
<coordination>:session:seq:<session_coord>
<coordination>:session:state:<session_coord>
<coordination>:session:lock:<session_coord>
<coordination>:session:wait

<fenced>:session:<session_coord>:meta
<fenced>:session:<session_coord>:events
<fenced>:user:<user_coord>
```

用户索引为 Redis HASH，`field=session_id`、`value=session_coord`。首次成功提交在同一 Lua 中原子写入 meta、`created_at` 和索引；后续提交只更新 `updated_at`。`GetSession` 不回退旧 namespace。`ListSessions` 使用 HSCAN，支持 `WithListSessionPage`；无分页最多 1000 条，索引/meta 不一致时 fail closed。

## 状态转换

Strong 使用 Session-aware Lua 完成 Begin、Retry、Complete、Fail、Reject、Recover 和 release。所有脚本先检查全部 key 类型和 task/digest/attempt/owner/epoch/stream/sequence/lease，再执行第一次业务写入。

Fenced Session Service 只在进程内暂存 Turn，并通过 `Prepare` 生成不可再修改的提交数据；生产环境唯一的持久化入口是 `messaging.CompleteTurn`。该入口在同一 Lua 中提交 Session、Inbox、Reply、Session 游标并释放锁，不保留只写 Session 的旁路。

| 转换 | Session 游标 | attempt | 锁 |
| --- | --- | --- | --- |
| Begin | 不变 | 不变 | 创建 token/task/owner/epoch 锁 |
| SessionWait / Promote | 不变 | 不变 | 不抢占有效锁 |
| Retry | 不变 | `+1` | 原子释放匹配锁 |
| Complete | `+1` | 不变 | 终态脚本内释放 |
| Fail | `+1` | 不变 | 终态脚本内释放 |
| 有序 Reject | `+1` | 不变 | processing 只清理已过期匹配锁 |
| stale Reject | 不重复推进 | 不变 | 不删除其他任务锁 |
| queued Recover | 不变 | 不变 | 无有效锁才转移 Pending |
| processing Recover | 不变 | `+1` | 双租约失效后转移 |
| `worker_lost` | 仅恰好下一序号时 `+1` | 不再增加 | 终态脚本内释放 |

Recover 先用 XPENDING/XRANGE 只读扫描 stale Pending。有效 Session lock 返回 `session_lock_active`，不执行 XCLAIM、XACK 或 XDEL，原 Pending 保留在旧 Worker 名下。双租约失效后 `recover_with_session` 才执行一次 XCLAIM；并发恢复只有一个 Worker 能重新入队。

独立 `release_session_lock` 只存在两个生产调用点：

1. Agent context 取消后 Retry/Fail 未能完成的清理；
2. Worker shutdown 等待超时后的主动让锁。

它必须同时匹配 `lock_token + task_id + lease_epoch`。正常 Retry/Fail/Recover/Complete 不调用独立 release。

## Turn 与 Heartbeat

Runner 的 Session 写入只进入本地 Turn staging。`AppendEvent` 在追加前按最终 JSON 数组大小检查，默认最多 512 个事件和 2 MiB；超限事件不追加，只设置 `OverLimit`。Runner 事件通道排空后，`ExecuteFenced` 返回不可重试的 `session_turn_too_large`，Worker 只调用一次 Strong Fail。

Turn 的 status/events/state/bytes/over-limit 均受同一同步边界保护。`Prepare` 冻结 Turn；`Discard` 或 `Prepare` 后（包括后续 `CompleteTurn` 期间）的迟到写返回 `ErrSessionFenceLost`。模型请求使用 ConfigVersion 的 `request_timeout`。

两个 heartbeat 完全分离：

- task heartbeat 只续 Inbox task lease，并用 `XCLAIM JUSTID` 刷新 task Pending；
- Session heartbeat 只续匹配 Session lock，不修改 task lease 或 Pending idle。

任一 heartbeat 丢失 fencing 都取消 Agent。Worker 不再使用未知 fence 写 Retry/Fail，交由 Recover 接管。

`wait_count` 保存在 Inbox HASH。每次等待使用：

```text
delay = min(session_wait_backoff * 2^wait_count,
            session_wait_max_backoff)
```

首次使用 `wait_count=0`，随后持久化递增。Begin、Retry 和终态清零；SessionWait、Promote 和 queued Recover 不增加业务 attempt。

## 配置与启动

示例见 `configs/phase4.example.json`。关键配置：

```json
{
  "session_fencing": "strong",
  "session_lock_duration": "30s",
  "session_wait_backoff": "100ms",
  "session_wait_max_backoff": "2s",
  "max_turn_events": 512,
  "max_turn_bytes": 2097152,
  "shutdown_timeout": "10s"
}
```

Strong 示例只包含 Redis StorageProfile。两个 Worker 使用不同 consumer 名称：

```text
trpc-service gateway -addr :8080 -consumer gateway-a
trpc-service worker -health-addr :8081 -consumer worker-a
trpc-service worker -health-addr :8082 -consumer worker-b
```

## 自动化与真实 Redis 验收

自动化覆盖：

- 跨组件多轮执行 `Prepare -> messaging.CompleteTurn -> sessionfence.GetSession/ListSessions`，验证状态和事件累积、时间戳、游标、索引及 Reply 数量；
- HASH 索引/meta 原子创建、幂等更新、`created_at` 保持、损坏索引拒绝；
- HSCAN 分页、显式上限和 1000 条无分页保护；
- Fail/Reject/`worker_lost` 推游标，Retry/SessionWait/queued Recover 不推进；
- 本地自动化验证 task lease 过期但 Session lock 有效时拒绝接管并保留 Pending owner；锁也过期后允许接管，并发 Recover 只有一个成功；
- 旧 Worker 迟到 commit/release 拒绝；
- turn 超限单终态、Discard race 和 shutdown/cancel cleanup；
- task/Session heartbeat 的 lease 与 Pending 隔离；
- wrong-type 无部分写入；
- InMemory、Cluster、跨 Redis、run_id 不匹配和不可验证拓扑拒绝；
- Legacy Fail/Reject/Recover 与 submit namespace 回归。

质量门：

```text
go test ./...                         PASS
go test -race ./...                   PASS
go test -race -p 1 ./...              PASS
go build ./...                        PASS
go vet ./...                          PASS
go mod verify                         PASS
git diff --check                      PASS
```

默认并行 `go test -race ./...` 在冷缓存并行编译时曾因 Windows `VirtualAlloc errno=1455` 触发系统提交内存不足；串行 `-p 1` 先覆盖全部包并通过，缓存就绪后原始并行命令也完整通过。

真实 Redis 7.4.11 验证：

- `ROLE=master`、standalone、`run_id` 可读取、Cluster 禁用；
- `TestRedis7Phase4StrongSmoke` 真实执行 Complete、Fail、Reject、Recover，覆盖 Streams、Lua、Session 可见性、游标推进、有效锁拒绝 Recover、Pending owner 保持、锁过期接管，以及 Redis 暂停后 readiness 失败并自动恢复；
- Phase 3 Redis 7 smoke 在最终源码下复验 Legacy `XAUTOCLAIM` 恢复，确保 Strong 改造没有破坏旧路径；
- `TestRedis7Phase4TwoWorkersSmoke` 证明同 Session 无并发、不同 Session 实际并行；
- Smoke 前缀和专用容器在验收后清理。

## 保留边界

- fencing 只保护平台自有 Session、Inbox、Reply 和 Session 顺序，不保证模型、Tool、Memory 或外部 HTTP 副作用 exactly-once；
- Session meta/events 和协调游标本阶段不设置 TTL，长期 GC/归档需后续设计；
- 完全缺失协调字段的损坏任务不能安全猜测 Session 游标，只能隔离并告警；
- Strong 不提供 Redis Cluster、代理拓扑推断、跨实例原子提交、数据迁移或 fallback。
