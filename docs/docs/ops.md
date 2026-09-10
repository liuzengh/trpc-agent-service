# 运维、可观测性与生产风险

> 本页把 [生产架构设计](architecture.md) 转成可执行的发布、监控、恢复和风险检查表。
> 本页按代码、部署清单和自动化测试证据标注已交付能力，并提供发布、监控、恢复和风险检查入口。
> 控制面、Runner spine、SQL/InMemory/Redis 运行时存储、真实 IM 验签、Dashboard、告警资源和
> 外部存储适配均由对应模块和验收流程覆盖。

## 运行边界与值班目标

生产请求分为四个可观测阶段：`callback_accept`（验签和幂等接收）、`execution`（Runner、
Model 和 Tool）、`state_commit`（Session/Event/Memory/Audit）、`reply_delivery`（分段、
发送和重试）。每个阶段都必须携带 `request_id`、`trace_id`、`tenant_id`（仅在可信上下文
建立后）、`binding_id`、`idempotency_key` 和外部 `message_id` 的脱敏引用。

建议把以下目标作为容量和告警的起始线，最终阈值在压测后按租户合同确定：

| 阶段 | 目标 | 触发动作 |
| --- | --- | --- |
| callback_accept | 在供应商回调时限内确认；企业微信按 5 秒约束设计 | 超时、签名失败突增或重复率突增时检查 Adapter、Secret Resolver 和公开入口 |
| execution | 受 Tenant 并发/预算、Model timeout 和 Tool deadline 共同限制 | 排队时延、模型超时、工具错误或取消率超阈值时限流/降级 |
| state_commit | event_seq 无空洞，CAS 冲突可重试且不覆盖新版本 | 冲突重试、outbox 积压或 summary 落后时暂停切换并修复 |
| reply_delivery | 可重试错误进入有界退避，最终状态可查询 | 429/5xx、DLQ、缓存回复未发送时按通道 runbook 处理 |

## 发布、灰度与回滚

1. **预检**：校验控制面 schema/Provider Catalog、Secret capability、Queue 连接、Session
   adapter conformance test 和 OTel 导出；任何必需能力未加载都不得标记 readiness。
2. **兼容发布**：先发布可读旧格式的新版本，再发布写入者；配置字段增加时使用显式 schema
   version，不能让 Worker 猜未知字段。
3. **小租户灰度**：以 `tenant_id` 或 Binding 为环，先选无生产副作用的测试租户，观察
   callback、execution、state、reply 四阶段指标和审计事件完整性。
4. **固定快照**：每个新执行选择一套 Tenant/App Revision/Model/Backend version 和 digest；
   灰度切换只影响之后的执行，不能在一个 Runner 中途热换 Model 或 Storage。
5. **扩大范围**：按租户并发、消息类型和工具权限分层扩大；当错误预算、成本或 DLQ 超过阈值
   时停止扩大，而不是仅重启 Worker。
6. **回滚**：把路由/Binding 或默认 Profile 指针切回历史 published 版本，保留新版本的审计
   和已提交事件；进行中的 plan 继续使用原版本，新的 plan 使用回滚版本。配置回滚不撤销已
   发送消息，也不删除审计事实。

控制面发布采用乐观锁和 Outbox；缓存失效必须带版本而非只依赖 TTL。Secret rotation 要
先写新 `secret_ref`、验证候选回调和出站调用，再撤销旧 handle，最后切换新 Binding 版本。
旧 Secret 值不出现在回滚记录、trace 或错误消息中。

## 监控、日志和审计

### Metrics

指标按阶段和受控低基数维度聚合：

| 类别 | 指标示例 | 推荐标签 |
| --- | --- | --- |
| 流量 | callback 数、有效/无效签名数、重复投递数 | channel、provider、status、error_class |
| 延迟 | callback、队列等待、Runner、Model、Tool、Storage、reply 延迟 | operation、provider、result |
| 执行 | active executions、取消、超时、Tool deny/approval | model_family、tool_family、decision |
| 数据 | CAS 冲突、event_seq gap、outbox backlog、summary lag、vector index lag | backend、capability、state |
| 回复 | 成功率、分段数、429/5xx、重试、DLQ | channel、reply_type、error_class |
| 成本 | tokens、模型成本、Tool 成本、租户预算使用率 | tenant 的授权/聚合维度、model_family |

不能把 `message_id`、完整 URL、原始 user/chat/session 或 request body 作为指标 label；高基数
问题用日志关联、trace exemplar 和审计查询解决。租户成本指标必须有访问控制，匿名租户或
超过基数预算的维度归入受控聚合桶。

### Trace 与结构化日志

入口创建或提取 `trace_id`，子 span 至少覆盖：

```text
im.callback
  └── binding.candidate_lookup
       └── binding.verify_and_decrypt
            └── idempotency.claim
                 └── execution.plan_load
                      └── runner.run
                           ├── model.call
                           ├── tool.call
                           ├── session.event/state
                           ├── memory.write/vector.enqueue
                           └── im.reply.enqueue/send
```

日志字段使用 `request_id`、`trace_id`、`tenant_id`、`binding_id`、`session_id` 的脱敏或哈希
形式以及 `error_class`、`event_seq`、`plan_digest`、`retry_count`。不得记录 Secret、模型
API key、完整 webhook XML/JSON、用户原文或可重放 token。必要的原文只在受控 Artifact/审计
存储中按租户权限保存，并以 digest 关联。

审计不是 trace 的别名。`audit_log` 采用 append-only 约束或 WORM/不可篡改归档，写入失败
必须影响提交策略或进入可观测 repair 状态，不能因为 telemetry exporter 故障而静默丢失。事件
至少有 `tenant_id`、channel、user/session 引用、app/revision、tool、decision、latency、
error_type、cost、request_id、trace_id、actor、reason、occurred_at 和前后版本；保留期限
遵循 Tenant policy，合规安全事件不能被普通 trace sampling 丢弃。

## 故障恢复 Runbook

### IM 回调重复、乱序或验签失败

- 先按 `channel/provider/error_class` 看签名失败率、重复率和 `message_event` 状态分布；
  不用消息正文搜索日志。
- 验签失败只检查候选 route、时间窗、Token/AES key 的 secret version 和 clock skew；不把
  失败 payload 直接当可信 Tenant，也不为了“止重试”而跳过验签。
- `running`/`completed`/`reply_pending`/`replied` 的重复请求只返回确认或重放缓存回复；
  不重新执行模型和副作用 Tool。
- 回调重复不负责回收执行；对 `running`/`execution-reconciling` 的队列任务检查 execution owner、heartbeat、lease
  deadline 和 fencing token。过期任务先对账已提交 event、Tool 幂等键和 provider receipt，
  安全可恢复才用新 fence 重排；副作用结果不明则进入 failed/DLQ/人工处理，不能盲目重跑。
- `execution-reconciling` 的任务必须先检查 `tool_invocation` 的 request digest、外部幂等键、
  claim/fence 和 provider receipt；已接受不重跑，结果不明或无供应商幂等能力的副作用转
  `manual`/DLQ，不能只看聚合 `message_event` 或 `audit_log` 决定重试。
- 多段回复按 `reply_id + segment_index` 查看 outbox；发送前检查 owner、lease deadline 和
  fencing token，过期 `sending`/`unknown` 先用原幂等键向供应商对账，确认未接受后再换新 fence
  进入 retryable；全部 segment 确认成功后才把聚合状态置为 `replied`，仍不明的分段保留
  unknown 并告警/DLQ，不能盲目重发。
- 乱序事件进入 pending/repair，按外部序号或收到时间保留，不修改已经提交的旧 event_seq。

### Session CAS 冲突或 outbox 堵塞

先确认是否同一 Session 的并发消息峰值或租约/fencing 错误，再检查冲突是否最终收敛。短时
冲突可按固定次数重新读取最新 state 并重放未提交 event；持续冲突时对该 Session 限流或串行
化，而不是无限扩大重试。Outbox 堵塞时暂停新回复发送的扩容，保留幂等记录，修复 provider
或消费者后按 cursor 重放，并监控重复发送保护。

提交屏障固定为 `event/state → durable Memory → reply outbox → async Summary`：Memory 写入
失败时不放行可发送的 outbox，进入 repair；Memory 已成功而 Summary 失败时保留事件和 Memory，
只重排 Summary，不回滚或重新执行 Runner。

`completed` 只在执行结果和 reply cache 持久化后成立；必须先幂等物化完整 reply outbox，再 CAS
为 `reply_pending`。发现 `completed` 缺段或 `reply_pending` 缺段时，按 cache ref 和 repair
cursor 补齐；修复必须按 `tenant_id + event_id + reply_id` 关联 event，校验 `segment_count`，
不重新运行 Runner，也不能把另一 event 的 outbox 归入本次回复或跳过分段直接标记 `replied`。

### Model、Tool 或数据库故障

- Model：使用 request deadline、有限重试和 token 预算；超时写失败/取消 event，返回固定
  兜底。若模型调用可能产生外部副作用，重试前必须有 Tool capability 证明。
- Tool/MCP：按错误类型区分参数拒绝、权限拒绝、超时和 provider 故障；危险工具默认转人工
  审批或补偿，不因网络错误自动重放。
- Redis/SQL：读取/写入策略由 Backend Profile 声明；不能用过期授权缓存绕过 Tenant/App
  状态。恢复后从 outbox/event cursor 补写，校验 event_seq、state digest 和审计完整性。

### Worker 进程终止与优雅关闭

关闭顺序为：停止接收新回调 → 停止领取新队列任务 → 取消超出 deadline 的执行 → 等待有限
时间消费 Runner Event channel → 写入完成/取消/失败和 reply outbox → 关闭 worker 自己拥有
的 client、consumer 和 exporter。tRPC-Agent-Go Runner 负责其 Event 语义，平台负责传递
`context.Context`、排空、超时和资源所有权。超时后宁可让幂等队列重新投递，也不能让旧 Worker
继续写新版本 Session。

## 容量模型

在没有压测数据前只定义变量和保护关系，不给出虚假的单节点 QPS：

```text
concurrent_executions
  ≈ callback_rate × p95(execution_duration + reply_queue_delay)

session_write_qps
  ≈ callback_rate × (inbound_events + runner_events + state_commits)

reply_api_qps
  ≈ callback_rate × average(reply_segments) × retry_multiplier

monthly_model_cost
  ≈ Σ(tenant_input_tokens + tenant_output_tokens) × model_price
```

压测至少覆盖：单租户突发、多个租户同时突发、同 Session 并发、长模型调用、Tool 超时、IM
429、数据库 failover、向量索引落后和 Worker 滚动重启。容量上限由最小者决定：租户并发/预算、
Worker CPU/memory、Model provider 限额、Session 写 QPS、队列吞吐、IM 发送配额或 OTel exporter
backpressure。高峰保护使用租户级 token bucket、全局队列上限、每 Session 串行化和 DLQ，而
不是无限增加 Worker。

## 迁移操作摘要

### Redis Session → SQL Session

迁移必须是 Backend Profile 版本迁移，不修改旧 Profile 的语义：

1. 记录租户、源版本、目标版本和迁移 correlation ID；冻结新执行的切换点。
2. 全量复制 Session/Event/State/Summary/Idempotency/Outbox，保留 `event_seq`、版本和 digest。
3. 用 outbox 双写和源 cursor 做增量追平；任何丢失、冲突或审计写失败都阻止切读。
4. 进行行数、最大序号、摘要哈希、随机内容、权限过滤和未完成回复校验。
5. Shadow read 稳定后仅让新 plan 切 SQL；旧 plan 继续访问旧 Profile。
6. 保留回滚窗口。目标 provider 出现延迟/冲突时切回旧 Profile，并从 cursor 修复，不能撤回
   已经对外发送的副作用。

### 本地向量库 → 远端向量库

以稳定文档/Memory ID 和原文 digest 导出，按 embedding model/version 分批导入；维度不符就
重建，不做未经验证的转换。完成数量、digest、租户过滤、抽样查询和删除 tombstone 校验后，
通过新的 Backend/Knowledge index version 进行 shadow query 和灰度。向量索引只负责检索，
源事件和权限仍在 SQL/Session 真相中。

## 生产风险清单

每项风险都必须有触发条件、影响范围、检测信号和可执行缓解/恢复措施。以下清单是上线门禁，
不是泛化的“注意安全”。

| 风险 | 触发条件 | 影响范围 | 检测信号 | 缓解或恢复措施 |
| --- | --- | --- | --- | --- |
| 跨租户访问 | 未验签 payload/header 直接提供 `tenant_id`；Adapter/Storage 漏掉租户谓词 | 读取或写入其他租户的配置、Session、Memory、审计 | 同一 request 的候选租户与可信租户不一致；双租户 conformance test 失败；越权告警 | 验签成功后才建立可信 Tenant；所有表使用显式 `tenant_id` 和复合约束；失败关闭并审计，暂停有问题 Binding |
| Secret 泄露 | secret 进入 plan、缓存、trace、错误、备份或日志 | 伪造 IM 回调、模型/数据库被接管 | secret pattern 扫描、日志 DLP、Secret Manager 访问异常、rotation 后旧 handle 使用 | 只保存 `secret_ref`；短路径解析 handle；脱敏和最小权限；rotation 主动失效并重放验证 |
| IM 重复/乱序 | 企业微信超时重试、Telegram 重复 update、网络重排 | 重复模型调用、重复扣费、summary 错乱、用户收到多条回复 | duplicate rate、`message_event` unique conflict、event_seq gap、DLQ | `tenant+channel+message_id` 幂等；状态机/CAS；pending/repair；completed 后仅重放缓存回复 |
| Session 并发冲突 | 同一群/会话多节点同时执行；lease 过期旧 Worker 回写 | event/state 覆盖、上下文丢失、不可重放 | CAS conflict、fencing reject、state version 回退、summary lag | SQL lock/CAS 或 Redis 原子脚本；fencing token；每 Session 串行化/限流；冲突重读重放 |
| 后端迁移分叉 | 全量复制后 cursor 未追平、双写失败或错误切读 | 新旧 Session/Event/Memory 不一致，回滚无法恢复 | row/count/digest mismatch、outbox backlog、shadow read diff | Backend Profile 版本固定；全量+双写+增量+校验+shadow read；保留旧源和明确回滚窗口 |
| 模型超时/成本失控 | provider 延迟、上下文过长、重试无预算 | 回调积压、租户预算耗尽、Worker 资源耗尽 | model p95/p99、timeout、retry、token/cost burn rate | context deadline、有限退避、token/并发预算、降级文案；不重试有副作用执行 |
| Tool 副作用重放 | reply 失败或 Worker crash 后把完整执行重新跑一遍 | 重复发货、工单、写外部系统或扣费 | tool idempotency mismatch、外部系统重复号、审计 decision | 默认标记不可重试；外部 idempotency key、确认/审批、outbox 和人工补偿；缓存最终回复 |
| 指标高基数 | 把 tenant/user/session/message/URL 全放 label | Prometheus/Collector 内存和费用上涨，查询不可用 | active series、remote write lag、collector backpressure | 低基数 label 白名单；高基数放 trace/log；租户成本用受控聚合和 exemplars |
| 配置回滚不完整 | 回滚只改指针但缓存、Binding 或 Worker 仍用新版本 | 一次执行混用版本，故障持续或无法复现 | plan key 与路由版本不一致；cache hit old/new mismatch | 版本化快照和主动失效；回滚只影响新执行；审计每次 plan；旧 plan 允许完成/取消 |
| 回复重试风暴 | IM 429/5xx、固定间隔重试、无 per-chat 限速 | 供应商封禁、用户刷屏、队列雪崩 | retry multiplier、429、DLQ、outbox age | 指数退避+jitter、解析 Retry-After、按通道/chat 分桶、最大预算和 DLQ |
| goroutine/事件泄漏 | context 未传递、Runner Event channel 未排空、consumer 无关闭边界 | Worker 内存上涨、滚动发布卡住、重复消费 | goroutine、FD、channel backlog、shutdown duration 持续上升 | owner 明确；context deadline；有界 drain；supervisor/health check；超时交给幂等重投递 |

## 当前交付与运行门禁

状态只代表当前仓库已有的实现和测试证据：

| 能力 | 状态 | 证据或边界 |
| --- | --- | --- |
| Tenant、Agent App/Revision、Model Profile、Backend Profile、Execution Plan 和 Runner policy | 已实现 | 控制面模型、快照和策略测试 |
| InMemory 运行时存储能力 | 已实现 | Tenant-scoped Session/Event/Memory 契约测试 |
| PostgreSQL 运行时存储能力 | 已实现 | 迁移、CAS、幂等、Outbox、重启恢复和 live conformance |
| Redis runtime storage | 已实现（Issue #108） | `TRPC_SESSION_BACKEND=redis`，Session/Event/Reply Outbox/Memory、WATCH/MULTI CAS、租户隔离、readiness PING 和 reconnect 测试 |
| S3/OSS-compatible Artifact/Object provider | 已实现（Issue #113） | `runtime/storage/s3`、tenant-scoped `artifact` binding、bounded transfer、metadata 校验、Probe/Close 和 MinIO integration entry |
| PostgreSQL vector capability | 已实现 | `runtime_vector_index`、tenant-scoped Knowledge/Memory projection、版本化 upsert 和检索 conformance |
| IM 与可观测性运行资源 | 已实现 | WeCom/Telegram/WeCom AI Bot adapter、媒体附件、Prometheus/Grafana 资源和告警规则 |

Adapter 和运维组件的验收覆盖双租户隔离、重复/乱序/验签失败、provider conformance、
故障注入、审计字段检查和 `mkdocs build --strict`；CI service 和部署环境执行带外部依赖的流程。
