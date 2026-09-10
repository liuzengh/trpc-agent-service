# 数据同步与幂等策略

本系统把“同一请求只创建一个权威 execution”“同一 Session 有序”“外部副作用不被错误重放”分别处理。各层是 at-least-once 或 idempotent 的边界，不把全链路包装成 exactly-once。

Channel ownership 是部署边界，不是队列调度边界：Gateway HTTP 可多副本，Channel Adapter 在独立单副本进程中接收官方 WebSocket/long connection，再调用现有 Gateway Admission。Channel 不发现 Worker、不直接执行 Runner；单 owner 之外没有 binding lease、leader election 或 sharding。

## 1. Admission 和 Session 顺序

Gateway 不做内存队列后返回成功。PostgreSQL `Store.Admit` 是 Admission 线性化点，在一个事务中完成：

1. 重新验证 credential/tenant/app/binding、active 状态、channel revision、IM access 和迁移 gate。
2. 对 HTTP 使用 `(tenant_id, app_id, tenant_source, source_id, idempotency_key)` 唯一键；对 Channel 使用 `channel_inbox` 的 binding-scoped external message ID 和 payload hash。
3. 锁定/创建 `session_lane`，读取 `next_turn_seq`，为该请求分配 `turn_seq`；对同一个 lane 的并发插入由数据库锁和 unique turn constraint 串行化。
4. 写入 execution、入站 artifact 关联和 dispatch outbox，再提交。

事务提交前任何步骤失败，不能留下可消费的半个请求；提交后即使 Gateway 崩溃，Relay 仍能从 outbox 继续。新请求一旦进入迁移的 `DRAINING/COPYING/VERIFYING`，Admission 返回 `ErrAdmissionDraining`；已经 Admission 的 source execution 仍按 source ConfigVersion 排空。

Channel 的 `/new` 是独立的平台命令事务，不创建 execution。它先用映射后的 principal 和当前 active Session ID 执行与普通消息相同的 stable/canary 选择及 IM access 判定，再锁定 `(tenant, app, binding, session_principal)` 的 `conversation_session` 指针，生成新 Session ID，并原子写入 Inbox、指针更新和 `channel_command` Reply Outbox。相同 external message ID + payload 重放已有结果；不同 payload 冲突。权限拒绝不创建/切换指针，只提交 `REJECTED` Inbox 和失败 ACK。

## 2. Event、state、summary 关系

Runner 运行时产生 framework event。Worker 不在释放 Session lease 后再异步写这些 event，而是先持续排空 Runner channel，再在 execution fence 有效时写 `execution_event(event_seq, payload)`；terminal 状态可以和最后一批 event 在同一数据库事务中提交。Gateway/HTTP `QueuedRunner` 从 `execution_event` durable source resume，而不是读取 Worker 内存。

framework Session service 仍是 transcript/state/tracks/summary 的 authority。平台 journal 是外部事件投影，两者不应被理解为两份可任意独立修改的 transcript。一个 Runner 持有同一 Session Lease 对应的 Session Lock，框架 Session 写入和 event journal 在该执行期间按事件顺序发生；如果在外部 backend 写入与进程崩溃之间失去精确原子性，下一次接管依靠 execution fence 和 framework 服务的幂等/已有事件检查，不能凭空声称跨 provider exactly-once。

## 3. Memory 可见性

Memory 只在 immutable ConfigVersion 选择 TencentDB Memory 时装配。Resolver 由 `tenant.Scope`、user 和 session 生成 key，任何 Worker 都能解析同一外部服务；因此成功提交到 TencentDB 后，后续节点有机会读到同一 scope 的记忆。写入是外部服务调用，Admission/Runner 事务不能把它和 PostgreSQL 一起原子提交，故属于外部 at-least-once/Provider 语义；TencentDB 网络和跨节点可见性需要由对应外部环境验证，事务边界仍保持不变。

群聊 transcript 包含多个发送者，而 framework ingestor 得不到逐消息 sender attribution；当前实现对 `SessionPrincipalID != UserID` 的共享 Session 不写 Memory。这个保守选择避免把他人内容写入当前用户的 Memory scope，但也意味着群聊 Memory 不是“所有群成员共享记忆”的实现。

## 4. HTTP / IM 幂等

HTTP OpenAI 入口要求唯一 `request_id`、`Idempotency-Key`、`X-Session-ID` 和 Bearer API key。请求 payload 只允许一个 user text message，tenant/app/user/principal/config 不能由 body 覆盖。相同 scope/source/idempotency key 且 payload hash 相同，Admission 返回原 execution 结果；相同 key 配不同 payload 返回 conflict。`request_id` 自身也是 scope 内主键，不能用不同请求复用。

WeCom/Feishu adapter 使用官方 WebSocket/long connection，external message ID 作为 idempotency key。当前模式没有 HTTP webhook URL 或 callback signature verification，因此这些 README 字段对当前接入为 `NOT_APPLICABLE`；WebSocket/SDK 使用 binding external account + scoped secret 完成认证。`channel_inbox` 记录 message type、payload hash、请求 ID、ADMITTED/REJECTED 和加密 reply target。重复消息同 hash 重放原结果；hash 不同是冲突。unsupported、附件拒绝、IM access denied 和 channel processing failure 也保留 durable rejection，避免 Provider 不断重试造成无界副作用。下载/解密/COS 上传、配置 pin、Admission 或 `/new` 命令处理失败时，以同一 payload hash 写 `REJECTED` Inbox 和唯一 `channel_failure` Reply Outbox；重复记录不会新增回复。若同一 Admission 已经提交为 `ADMITTED`，提交结果优先，失败补偿不覆盖它也不发送错误回复。

外部 user/chat/thread 映射使用每个 `(tenant_id, app_id)` 稳定注入的 `im-external-id-hmac-key@v1`；Reply target 使用 `im-provider-target-key@v1` 加密后才写入 identity、conversation 或 inbox。进程重启后，Resolver 必须用同一 scope、同一版本 key 重新计算 digest 或打开 envelope；缺 key、scope 错配或格式错误都不能回退到原始外部 ID。

用户映射同样幂等：direct 以 binding-scoped external user hash 查稳定 user；group/topic 以 chat hash + thread hash 查稳定 conversation。数据库 `ON CONFLICT DO NOTHING` 后再次读取，避免两个 Adapter 并发创建两个内部主体。

## 5. Dispatch Outbox 与 Worker ownership

Admission 只写 PostgreSQL outbox；Relay 以 owner/lease claim 后向 Redis Stream `XADD`。发布和 SQL 状态更新不是跨系统原子事务，所以可能出现“已发布但 outbox 仍可重试”或“outbox SENT 但 Worker 尚未消费”。这是预期的 at-least-once 传输；execution idempotency 和数据库 claim 吸收重复 delivery。

Redis Stream 使用 Consumer Group。Worker 先 XAUTOCLAIM 过期 pending，再读取新消息；本进程用 local ownership map 避免把仍在运行的长任务误当成 crashed consumer。ACK 只在 PostgreSQL execution durable transition 后进行；数据库暂时不可用或状态未完成时 Release local marker，保留 Redis pending 供后续接管。无法解码的消息进入 `<stream>:dlq` 后 ACK，避免永久 poison message 阻塞队列。

Worker 通过 PostgreSQL execution lease 和 `run_token` claim。续租失败会取消 run；每次 completion、event sink、tool authorization 和 Session 写都要验证当前 owner/token。旧 owner 即使网络恢复，也不能用旧 token 覆盖新 owner。execution 终态包括：

| 结果 | 含义 | 后续动作 |
| --- | --- | --- |
| `SUCCEEDED` | Runner 完成且平台认为没有未分类失败 | ACK；reply outbox 由独立 Sender 处理 |
| `FAILED` | 明确的永久错误或已知不可重试错误 | 不自动重放；保留 error metadata |
| `CANCELED` | Recall/取消在可安全边界生效 | 不继续执行 |
| `WAITING_APPROVAL` | 等待 exact approval，不是成功 | 不把消息当完成；审批后重新 claim/继续 |
| `UNCERTAIN` | Runner 已启动且外部副作用/状态可能已发生但结果不明 | 不自动重试；交由运维/Provider 对账 |

普通基础设施 retry 通过 bounded exponential/jitter 将 execution 放回 `PENDING` 并创建下一次 dispatch；attempt 受实现边界限制。queue claim 明确携带当前是否为最终 attempt。COS、模型、Session、Knowledge 等 Runner 构建错误发生在 `RunnerStarted` 前时，非最终 attempt 不写终态并继续 retry；最终 attempt 或永久错误先写 terminal error event 和 `FAILED`，从而驱动一次 IM 失败回复。它只适用于 retryable 错误，不能覆盖 side-effect uncertain。

## 6. Session 迁移

迁移记录和 gate 在 PostgreSQL。当前 Session 只支持 Redis → PostgreSQL，且 source 必须是 active ConfigVersion、target 必须已发布；模型、工具、Knowledge、secret、channel binding、Memory、Artifact 等行为必须相同，只能替换 Session Backend。

迁移流程是 `PENDING → DRAINING → COPYING → VERIFYING → SUCCEEDED/FAILED`：

- Begin 进入 DRAINING 后阻止新 Admission；只等待 source config 的 PENDING/RUNNING/WAITING_APPROVAL execution 清空。
- Copier 按 platform SQL 的 Session inventory 读取 Redis 旧/新 key 格式，复制完整 events/state/tracks/summaries；不凭 Redis 扫描结果发现跨租户 ownership。
- Checkpoint 记录 total/copy/verify/success/failure stage；Worker lease 丢失或 crash 后可被新 Worker 接管。
- Verify 比较 Session identity、event、state、tracks、summary；目标不通过时 FAILED，source 继续 active。
- 只有 `AdvanceDataMigration(SUCCEEDED)` 在锁住 app 的事务中把 active config 切到 target；没有 dual-write、live dual-read 或中间双权威。

## 7. Knowledge 迁移

当前只支持 Qdrant → Qdrant，source/target 必须同 embedding dimension 和 `index_generation`，其它 config 行为不变。迁移 inventory 来自 PostgreSQL 的 knowledge base/document/chunk catalog，并要求 chunk 属于 source config；Qdrant 不被扫描来猜 tenant 归属。每个 point upsert 使用 wait/确定性 point identity，并在目标读取后校验。chunk checkpoint 支持恢复，成功后才切换 config。

因此当前知识库迁移的实际路径是 Qdrant → Qdrant；它与 Session Redis → PostgreSQL 共同构成当前已实现的数据迁移能力。

## 8. Artifact 一致性和 cleanup

入站媒体的 raw provider ref 只留在 adapter closure。Pinned attachment prepare 先解析 exact ConfigVersion、下载/解密/大小和公网地址校验，再把对象写入 scoped COS，并创建 `inbound_artifact` staging metadata；Admission 事务 attach `artifact://...@version`。Admission 失败调用 compensator 删除未共享的对象；已经被其它引用共享的对象不误删。

Artifact object 和 SQL metadata 不具备跨系统两阶段提交：可能出现 object 已上传但 attach 没提交，或 metadata 已标记删除但删除调用失败。`artifact`/`inbound_artifact` 保存 cleanup attempts、next time、owner/lease/error/completed，cleanup worker claim exact candidate，删除成功后才完成 metadata；删除失败保留 retry。不存在“清理永远只做一次”的假设，`DELETED` metadata 也不等于 provider 立即完成。

## 9. Reply Outbox 和外部副作用

Runner journal 只为客户端可见 event 创建 Reply Outbox projection；失败投影只接受框架的 terminal error event，不能用 `evt.Error != nil` 代替终态判断，否则非终态错误后恢复成功会产生错误或重复回复。Channel 角色中的 Reply Sender 与 Runner 分离，可在 execution 成功后继续投递。Sender claim row、重新验证 binding/revision、使用 Redis 分布式 binding rate limit，再调用 Provider 的一次 `SendOnce`。WeCom/Feishu 对已知 HTTP/provider retryable 错误有限重试；Feishu 的 `uuid=ReplyID` 是 provider 请求幂等提示，但当前代码没有通用的安全结果查询协议。

`reply_outbox.source_kind` 区分三类权威来源：`execution` 必须关联 execution；`channel_command` 和 `channel_failure` 必须关联同 scope/binding/request 的 `channel_inbox`。数据库 deferred constraint trigger 校验该来源，避免失败回复绕过 Inbox 幂等边界。

Reply Outbox 保存的是带 scope/AAD 的加密 target envelope，不是原始 Provider target。Sender 在每次投递前通过 scoped `SecretProvider` 解析 `im-provider-target-key@v1` 并解密，因此该 key 必须跨进程重启和多节点保持稳定；key 丢失时不能伪造成功，也不能改用明文 target。

若 transport timeout、context cancellation、receipt 无效、lease 丢失或 completion 更新失败，Provider 是否已经接受消息无法知道，状态写为 `UNCERTAIN`，不自动再次发送。已知 permanent 错误写 `PERMANENTLY_FAILED`；已知 retryable 写回 `PENDING` 并应用 Retry-After/上限；reply attempts 有默认最大值。该分类是“安全防重复”与“可能漏发”的明确取舍。

## 10. Crash / recovery 语义表

| 边界 | 失败后保证 | 不保证 |
| --- | --- | --- |
| Admission → SQL | 同一幂等 key 不会创建两个 execution；事务失败不产生可消费 admission | 外部 provider 发送不在该事务内 |
| SQL outbox → Redis | Relay 可重试；Redis message 至少一次到 Consumer | 发布和 SQL 状态不是跨系统 exactly-once |
| Redis → execution claim | 过期 lease 可接管；run token fencing | 已经发生的外部副作用可被安全推断 |
| Runner → event journal | lease 有效时 event seq/终态持久化，stream 可 resume；最终构建失败也写 terminal event | Session provider 与 SQL journal 跨存储原子性 |
| Runner → tool | 当前 `ToolCatalog` 以 `todo_write` 和安全策略作为执行边界；未知/无权限工具拒绝 | 外部副作用由工具自身语义和 `UNCERTAIN` 分类共同约束 |
| execution/command/channel failure → reply | 回复可从对应 source kind 的 outbox 异步恢复；失败回复按 Inbox/event 幂等 | Provider uncertain 时不自动重发，可能需要人工对账 |
| object → cleanup | exact candidate、lease、retry 可恢复 | COS 和 SQL 不构成单个原子事务 |
