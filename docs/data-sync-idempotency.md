# 数据同步与幂等策略

## 1. 目标与不变量

本策略覆盖同一后端内的事件写入、多 Gateway 并发、IM 重复投递、请求重试，以及 Redis/SQLite/向量索引向目标后端迁移。无论后端如何选择，都必须保持五项不变量：所有操作带 Tenant 范围；Session Event 只追加；同一 Session 的 sequence 连续；同一幂等键只对应一种内容；失去 Lease 的执行不能继续提交结果。

Session Event 是运行数据事实源，Session State、Summary 和向量索引都是派生数据。迁移和恢复优先保护事实源，派生数据允许从权威记录重建。危险 Tool 的外部副作用不属于可自动重建数据，必须采用独立状态机处理。

## 2. 在线写入顺序

同一请求按以下顺序提交：

1. Gateway 校验 Tenant、App、Deployment Version 和 Governance Policy。
2. 使用 provider message ID 或公开 `request_id` 派生 `<request_id>:input`，幂等写入 `message.input`。
3. 按 `(tenant_id, session_id)` 获取 Session Execution Lease，获得单调递增的 fencing token。
4. 写入 token 唯一的 `session.lease.acquired` 和 `<request_id>:started`。
5. Worker 执行 Runner；Gateway 按事件序号追加 delta、completed 或失败事件。
6. 写入 `latest_agent_reply` Memory；发布 Artifact 内容并校验 checksum 后写入元数据；最后追加带 `source_sequence` 的 Summary Projection Checkpoint。
7. 发送 IM 回复并记录投递结果，写入唯一运行终态 `run.completed`、`run.failed` 或 `run.cancelled`，随后释放 Lease。

每个步骤都使用由 `request_id` 和阶段名确定性派生的幂等键。成功回复要求 Artifact、Memory 和投影已经提交；任何关键写失败都不能写成功终态。调用方取消和存储超时使用稳定错误分类，不能被误报为业务成功。

## 3. 幂等与重复投递

`session_events` 对 `(tenant_id, session_id, idempotency_key)` 建唯一约束。相同 key、type 和 payload 的重试返回已有结果；相同 key 携带不同 type 或 payload 返回 idempotency conflict。事件 sequence 由存储层在事务或原子操作中分配，调用方不能指定覆盖现有位置。

企业微信使用 `msgid`/`req_id`，Telegram 使用 Update ID，Mock Channel 使用显式 message ID。Channel Adapter 将 provider 标识稳定转换为 `request_id`，并在绑定范围内检查 provider sequence。重复消息若已有 input 或终态，直接返回已有状态，不启动第二个 Runner；小于等于已处理 sequence 的非重复消息被视为乱序并拒绝。IM 出站记录 delivery status 和 attempts，发送重试沿用同一消息标识。

公开 Chat 重试也沿用原 `request_id`。恢复逻辑先读取 input、started 和唯一终态：已有终态则返回历史结果；只有 input 而没有 started 时可继续准入；已经开始但结局不明的危险 Tool 不得自动执行。Tool Confirmation 的重复 approve/reject 返回相同决定，只有 approved 能原子消费为 executing。

## 4. Lease、fencing 与投影

PostgreSQL 以数据库时间判断 Lease 的 `expires_at`。Gateway 定期续租；续租失败或 owner/token 不匹配时关闭 Lost 信号并取消 Runner。新 owner 接管时产生更高 token，并用当前 token 为遗留未终结请求写 `run.cancelled`。存储在追加 Session Event、写 Memory、发布 Artifact 和终态时同时比较 Session 当前最高 token，旧执行即使没有及时响应取消也会收到 `stale_fencing_token`。

租约过期但还未被新 owner 接管时，原 token 可以在有界期限内收尾；一旦更高 token 出现，旧 token 立即失效。不同 Session 使用不同 lease row，可以水平并行。sticky session 只能降低路由和缓存成本，不能作为正确性机制。

物化器从 sequence 1 连续消费事件，`projection_sequence` 只能从 N 推进到 N+1。发现空洞立即失败并保留 checkpoint，恢复后从下一条重新处理。Summary、State 或索引损坏时删除派生数据即可从事件重建，不能通过跳过缺失事件制造“最新”状态。

## 5. 权威数据与异步索引

Memory 的权威值先提交到选定的 SQL/Redis 后端，再发布索引任务。Agent 读取上下文时先读权威 Memory，向量召回只扩展候选。Knowledge 同样先保存来源、内容或内容引用，再异步分块、生成 embedding、upsert 向量并推进 index checkpoint。索引故障把状态置为 `retry_pending`，使用指数退避和有界并发重试，不删除权威记录。

Artifact metadata 只引用已持久化的 `message.completed` 事件。配置 S3 时，适配器把事件内容写入确定性名称的版本对象，回读校验 checksum 后再发布 metadata；对象成功而 metadata 失败会留下不可达版本，交给生命周期或后续扫描清理。未配置对象存储时保留可由事件事实源校验的 `session-event://` 引用。metadata 已发布但对象不可读或 checksum 不符时，API 返回恢复错误而不返回虚假内容。

## 6. Redis/SQLite 到 SQL 迁移

迁移采用 forward-only Migration Job。正式 Redis 迁移先在共享 Control Plane 设置 Tenant Migration Lock，使新请求返回 `tenant_storage_migrating`，再用 Redis 持久锁阻止已经持有旧 adapter 的节点写入：

1. 锁定目标 Tenant 的 Backend Selection，阻止迁移期间切换配置。
2. 按 Session 列表和 sequence 分批复制 Session Event，再复制 Memory。
3. 每批保存 source cursor、最后 sequence、记录数和 checksum 的 Migration Checkpoint。
4. dry-run 比较源/目标数量、事件身份和摘要，不切换在线流量。
5. 正式迁移完成后执行双读校验，确认无缺失、无多余、checksum 一致。
6. 在 Control Plane 中原子更新 Backend Selection，源 Redis 保留持久只读锁进入观察窗口；明确回滚时才用 `storage-migrate -resume-source-writes` 解锁。
7. 观察期通过后再按运维流程回收源数据；失败任务从最后完整 checkpoint 恢复。

目标端仍使用原有幂等键，重复批次不会产生第二份事件。迁移工具不得跨 Tenant 接受源或目标范围。Control Plane schema 迁移由 `control-migrate` 执行 expand-contract：先添加兼容结构，升级全部 Gateway，最后在后续维护窗口删除旧结构；Gateway 只验证版本，不在启动时改表。

进程崩溃会保留 Control Plane Migration ID 和 Redis 只读锁。重启后 `GET /api/v1/admin/migrations/{migration_id}` 返回 `recovery_required`；管理员核对路由、checkpoint 和目标数据后，可调用 `DELETE` 同一路径解除对应 ID 的两层锁。独立迁移工具也提供 `-resume-source-writes`，用于已确认 Control Plane 状态的离线回滚。

## 7. 本地向量库到远端向量库

向量是 Knowledge/Memory 权威内容的可重建派生物。`storage-migrate -profiles <file> -reindex-profile <id> -tenant <tenant>` 按 Tenant 和 index generation 重建 Qdrant collection；profile 固定 embedding model、向量维度和 generation，确定性向量 ID 使重试不会产生重复记录。

向量 ID 使用 `(tenant_id, app_id, source_type, source_id, chunk_id, embedding_version)` 确定性生成，批量 upsert 可安全重试。checkpoint 记录源 cursor、watermark、成功/失败数与批次 checksum。Tenant ID 同时进入 collection/partition 路由和 metadata filter。只有模型、维度与切片版本完全相同时才允许复制已有向量，否则从权威内容重新切片和 embedding。

watermark 之后的新增、更新和删除进入 SQL outbox；回填期间继续写源索引，并异步同步目标 generation，删除使用 tombstone。回填结束后先追平 outbox，再核对有效 ID、缺失/多余项、metadata checksum、维度和 embedding 版本，并对固定查询集执行 shadow read。只有 Top-K 覆盖率、过滤条件和延迟达到阈值时，才原子切换 active index generation。

切换通过选择指向新 generation 的 Backend Profile 完成，回滚时恢复旧 profile。当前实现会在查询时补齐遗漏的权威 Knowledge，并在向量服务不可用时回退到权威记录；持续 outbox、删除 tombstone、shadow-read 阈值和未发布 generation 自动清理仍是生产扩展项。

## 8. 故障恢复与检测

Redis 或 PostgreSQL 不可用时返回 `storage_unavailable`，不能把本地缓存当成提交成功。Control Plane 不可用时返回 `control_plane_unavailable`，不读取旧配置继续高风险操作。迁移、投影和索引任务都必须暴露 checkpoint lag、retry count、checksum mismatch 和最后错误。

监控至少覆盖 IM duplicate/out-of-order、幂等冲突、Lease 获取/续租失败、stale fencing 拒绝、投影落后量、索引 retry 队列、Artifact `recovery_required` 和迁移校验失败。恢复演练必须注入确定的 `request_id` 并验证其精确终态，不能用“系统中出现过成功请求”替代目标断言。各后端职责与实现状态见[多后端适配方案](backend-adapters.md)。
