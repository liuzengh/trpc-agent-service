# 数据同步、一致性和幂等策略

## 1. 一致性目标

平台不要求所有存储都采用同一种一致性模型。Session 和工具副作用需要强约束；自动 Memory、Knowledge 索引和审计分析可以接受短暂延迟。

运行面维护以下不变量：

1. 同一 conversation 同一时刻最多有一个有效 fencing token。
2. 一个外部消息最多创建一个 `request_id` 和一个逻辑 Agent run。
3. Session Event 的顺序由 `event_seq` 或后端原子写入顺序确定，不能依赖客户端时间。
4. Session state 只能由已提交 Event 的 `StateDelta` 推进。
5. Summary 和 Memory 都不能越过尚未提交的 Event，也不能用旧水位覆盖新结果。
6. 产生外部副作用的 Tool 必须具备业务幂等键或补偿流程。

## 2. 同一 Session 并发写入

Redis、MySQL 和 PostgreSQL Session 适配器可以保证单次 `AppendEvent` 内部原子，但两个 Runner 仍可能交错执行。例如 A、B 两条用户消息几乎同时到达，两个 Worker 都先读取旧 Session，再分别调用模型。即使数据库最终按顺序写入，B 的模型上下文也没有包含 A 的回答。

平台采用两层串行化：

- 消息队列以 `conversation_key` 分区，正常情况下同一会话由一个消费者顺序处理；
- Session Coordinator 提供跨节点租约和 fencing token，处理 rebalance、重投和网络分区。

Redis 租约示例：

```text
lease key: lease:{tenant_id}:{app_id}:{runtime_user_id}:{session_id}
value: worker_id | request_id | fencing_token
ttl: max_run_duration + grace_period
```

获取租约时用 Lua 完成 token 递增和 `SET NX PX`。Worker 每隔 TTL 的三分之一续租。所有关键提交使用类似条件：

```sql
UPDATE agent_run
SET status = 'completed', completed_at = now()
WHERE request_id = $1
  AND fencing_token = $2
  AND status = 'running';
```

更新行数为零说明租约已失效，当前结果只能丢弃或进入审计，不能发送回复。

## 3. Event、State 和 Summary 顺序

Runner 会先把用户输入作为 Event 写入 Session，再开始模型调用。后续只持久化完整响应或包含 StateDelta 的 Event。平台保留这个顺序，并为每个 turn 记录：

```text
turn_seq
first_event_seq
last_event_seq
terminal_event_id
```

自定义 SQL Session Service 在事务中锁定 Session 行，分配 `event_seq`，插入 Event，再合并 StateDelta。Redis Service 使用 Lua 将 Event 和 StateDelta 一次提交。禁止先更新 state、后写 Event，否则发生中断时无法解释状态来源。

Summary Job 的 payload 不携带完整 Session 副本，只保存 Session Key、`filter_key` 和目标 `high_watermark`。Job Worker 收到任务后重新读取已提交 Session，最多总结到该水位。写入时使用条件更新：

```sql
INSERT INTO session_summary (..., high_watermark)
VALUES (..., $new_watermark)
ON CONFLICT (...) DO UPDATE
SET summary = EXCLUDED.summary,
    high_watermark = EXCLUDED.high_watermark,
    updated_at = now()
WHERE session_summary.high_watermark <= EXCLUDED.high_watermark;
```

这层检查不能省。不同后端的内置 summary 行为并不完全相同，任务乱序时仅靠 last-write-wins 可能让摘要倒退。

## 4. Memory 写入和跨节点可见性

用户主动调用 Memory Tool 时，`AddMemory`、`UpdateMemory` 或 `DeleteMemory` 成功返回后，写入已经提交到共享后端。其他节点下一次读取主库即可看到结果。若启用本地缓存，缓存键必须包含 tenant、app 和 subject，并通过版本号或 pub/sub 失效。

自动 Memory 提取是最终一致的。Runner 完成后只提交一个持久化任务，Memory Job Worker 执行以下步骤：

1. 读取 Session 和 `memory_high_watermark`；
2. 取出上次水位之后、目标水位之前的用户和 assistant 消息；
3. 调用 `memory/extractor.MemoryExtractor` 生成 add、update、delete 操作；
4. 读取现有 Memory 做去重和冲突判断；
5. 执行幂等 Memory 操作；
6. 在所有操作成功后推进 `memory_high_watermark`。

框架内置 auto-memory worker 使用进程内 channel，适合单进程应用，不承担生产任务持久化。平台自己的任务队列需要支持至少一次投递，因此 Memory 操作和水位更新都必须可重试。

如果租户要求下一轮对话读到上一轮自动 Memory，可以在获得 session 租约后等待 `memory_high_watermark >= previous_turn.last_event_seq`，并设置很短的最大等待时间。超时后继续执行并记录 memory lag，避免 Memory 服务拖垮对话入口。

## 5. IM 消息幂等

Channel Adapter 尽量提取平台原生消息 ID。Gateway 在同一事务中插入 `inbound_message` 和 outbox，唯一约束为：

```text
(channel_binding_id, external_message_id)
```

`request_id` 使用稳定输入生成：

```text
base64url(sha256(tenant_id | channel_binding_id | external_message_id))
```

重投消息命中唯一索引后，Gateway 根据原记录状态处理：

- `received/running`：返回成功，不重复入队；
- `completed`：返回成功，必要时重新投递尚未成功的 outbound message；
- `failed_retryable`：只重新发布原 request，不创建新 request；
- `failed_terminal`：返回成功并保留人工处理记录，防止无限重试。

没有稳定消息 ID 的事件使用规范化哈希。哈希字段至少包含账号、用户、群、事件类型、平台时间戳和主要 payload；去重窗口根据通道重试特征设置。

## 6. Agent run 恢复

Worker 在执行前插入或 CAS 更新 `agent_run`。崩溃重投时按以下顺序判断：

1. `agent_run=completed`：复用结果；
2. Session 已有同 `request_id` 的 terminal assistant Event：补写 run 和 outbound 状态；
3. Session 已有未完成 tool call：查询 `tool_execution`，恢复或等待外部结果；
4. 只有用户 Event：对无副作用 Agent 可重跑，对 Graph/长任务使用 checkpoint 或 `WithResume(true)`；
5. Session 中没有该 request：正常执行。

Runner 内部的 Event 去重只覆盖一次运行中的重复持久化，不应视为跨进程业务幂等。生产级 SQL Event 表建议对 `event_id` 建唯一索引；使用原生 SQL Session 适配器时，由 `agent_run` 和 Session 扫描承担恢复判断。

## 7. 工具调用幂等

只读工具可以按退避策略重试。写操作工具分为三类：

- 原生支持幂等键：传入 `request_id/tool_call_id`；
- 支持查询状态但不支持幂等键：调用前写 journal，超时后先查结果再决定是否重试；
- 无法查询且不可逆：不自动重试，转人工确认或补偿。

`tool_execution` 的唯一键为 `(request_id, tool_call_id)`。参数变化时 `arguments_hash` 不同，应拒绝复用旧结果并写安全审计。

## 8. Redis 到 SQL 的迁移

Session 后端迁移采用以下状态机：

```text
PREPARE
  → DUAL_WRITE
  → BACKFILL
  → VERIFY
  → READ_NEW_WRITE_BOTH
  → CUTOVER
  → DRAIN
  → CLEANUP
```

具体步骤：

1. 创建目标表、索引和容量配额；
2. Storage Router 开始双写，读仍走 Redis；
3. 按 Session Key 扫描历史数据，保留 Event ID、顺序、水位和时间；
4. 比较 Session 数、Event 数、state checksum、summary watermark；
5. 对一小部分请求影子读取 SQL，比较模型可见上下文；
6. 切换为读 SQL、写双端；
7. 观察错误率和数据差异后停止旧端写入；
8. 过保留期后清理 Redis 数据。

双写不能放在两个互不相关的 goroutine 中。主写成功、次写失败时记录 repair outbox，由后台修复；切读前 repair backlog 必须为零或低于明确阈值。

## 9. 本地向量库到远端向量库

向量迁移以原始文档和 chunk 元数据为真相，不把旧库向量结果当成唯一来源。新旧库必须使用相同 embedding 模型和维度；如果 embedding 模型也变化，需要重新计算向量，不能直接复制。

迁移步骤包括：

- 冻结知识库 revision，记录 source checksum；
- 新写入双写 document、metadata 和 embedding；
- 旧数据按 document ID 回填；
- 校验数量、metadata、随机向量和 top-k 结果；
- 小流量影子查询，记录结果重合率和排序差异；
- 切换 Retriever；
- 保留旧 collection 直到观察期结束。

文档 ID 使用 `tenant/app/kb/document/chunk` 命名空间，所有 Search、Count、GetMetadata、DeleteByFilter 都由 VectorStore 包装器强制增加 tenant、app 和 knowledge base 条件。

## 10. 一致性取舍

| 数据 | 目标一致性 | 说明 |
| --- | --- | --- |
| inbound message / agent run | 强一致 | 决定是否可以 ACK 和是否重复执行 |
| Session Event + StateDelta | 强一致、按 session 串行 | 直接影响下一轮模型上下文 |
| Tool 副作用 journal | 强一致 | 防止支付、工单、数据库写入重复 |
| Summary | 最终一致，水位单调 | 延迟可接受，禁止回退 |
| 用户主动 Memory | 提交后读可见 | 下一次请求应从主库读取 |
| 自动 Memory | 最终一致 | 由持久化任务队列处理 |
| Knowledge 索引 | 最终一致 | 用 revision 和索引状态控制可见性 |
| Artifact 二进制 | 写后可读 | 元数据和对象需要对账 |
| Audit 分析索引 | 最终一致 | 原始审计记录先可靠落库 |
