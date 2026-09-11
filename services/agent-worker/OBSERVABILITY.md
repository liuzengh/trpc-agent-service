# Worker V1 运行信号

Worker 默认向 stdout 输出结构化 JSON；可通过显式配置将低基数 Metrics 发送给 OTLP HTTP
Collector。M1 增加独立可选的 OTLP Traces；缺省仍关闭。这些信号不是 Receipt、Completion、授权或费用账本，观测失败不改变执行结果。

## 信号与含义

| 信号 | 内容 |
| --- | --- |
| `worker.operation` JSON | 固定 operation/result，内部 Tenant/Run/Attempt ID，阶段时长；有有效 ctx 时含 trace_id/span_id；不携带原始错误 |
| `worker.sdk` JSON | tracing 启用时将 SDK 自由文本替换为固定 event/sdk_level；进程级事件，不伪造 Run 关联 |
| `worker.backlog` JSON | 队列/运行/重试数量，未发布 Reply，最老未完成 Run 和 Reply age，本机活跃数 |
| `worker.operation.count` | 按固定 operation/result 统计操作尝试，不是唯一业务完成数 |
| `worker.operation.duration` | 操作时长直方图，单位秒，包含 prepare/execute/Session/commit/renew/Drain |
| `worker.provider.tokens` | SDK 实际返回的 input/output/total usage；不预占、不结算、不施加额度 |
| `worker.backlog` Metrics | `queued/running/retry_wait/reply_pending/active_local` 五种固定 kind |
| `worker.oldest.age` | 最老 unsettled Run/unpublished Reply 的秒数；不是每租户 label |
| `worker.storage.sample.success` | 最近一次积压采样是否成功；0 时不要将旧 gauge 当作最新事实 |
| `worker.observation.dropped` | stdout 写入阻塞时丢弃的观测事件数量 |

operation 包括 intake、manifest、claim、prepare、credential_resolve、session_open、session_load、execute、session_stage、complete、
renew、fence、terminalize、manifest_apply、wire_reject、reply_publish、startup、drain 等有限枚举。
Session 初始化额外记录封闭的 stage（parse_config/connect/target_query/namespace_query/table_query/candidate_acl），
用来区分连接构造、目标校验和表权限；stage 不携带底层错误，不加入 Metrics label。
result 区分冲突、容量、Manifest 等待、Session 等待、凭据拒绝、Session 准备/完整性、依赖错误、
失租、取消、deadline。常规成功续租/Fence/等待轮询只记指标，避免逐次轮询刷日志。

成功 complete 计数表示数据库调用成功或重放，并非不重复的 Run 数；唯一 Completion 仍查
Worker 账本。execute 表示 SDK 执行阶段，不将它包装成所有实际 HTTP 请求的网络计数。
Provider 没返回 usage 时不合成 0 Token 成本。各指标均无 Tenant/Run/Session、URL、正文、
自由文本错误等高基数 label；关联 ID 仅出现在结构化日志中。

积压来自 Worker 自有表的只读查询，使用未完成 Run/未发布 Reply 的 partial index，不扫描
已完成历史、不加业务行锁。数据库级 gauge 在共享同一 Execution DB 的副本间重复，查询
使用 `max` 而不是 `sum`；`active_local` 才是进程局部数。采样超时只使 sample.success=0，
不把观测读取故障升级为新的 Run 失败或 readiness 判定。

## 显式启用 OTLP Metrics

可在 `WORKER_CONFIG_FILE` 的根对象增加：

```json
{
  "telemetry": {
    "metrics_endpoint": "https://collector.example.com/v1/metrics",
    "export_interval": "10s",
    "export_timeout": "2s"
  }
}
```

这是需要合并进完整 Worker 配置的片段，不是独立启动配置。三个字段缺一、未知字段、null、
带 userinfo/query/fragment 的 endpoint 都会拒绝。HTTP 仅用于字面回环地址的本地 Collector；
远程使用 HTTPS 和系统信任根，默认不降低证书或主机名校验。

省略 telemetry 对象时不会创建 exporter，也不会从环境变量偷偷继承 OTLP 网络目标；JSON
阶段/积压日志仍工作。显式 exporter 使用后台 PeriodicReader，有界 export timeout，无透明
无限重试；不会持有 Loki/Tempo/Grafana 凭据。Metrics-only 的 Resource 保持 service.name=agent-worker、
service.version=worker-v1、namespace=agent-platform、worker instance 与 environment=unspecified。
启用下节 Tracing 后，Metrics 和 Traces 共享同一 Resource（构建版本与显式部署环境）。

stdout 采用 256 条有界队列。队列满时丢弃观测并递增 dropped 指标，业务流程不等待日志 I/O。
关闭时先停业务任务，再在既有 HTTP shutdown timeout 内 flush/shutdown；阻塞的 stdout 或
失联 Collector 不让进程无限等待。Metrics 与日志不是不可丢弃的业务 Audit。

## 运行检查

- intake 增长但 queued/oldest_run 持续增长：检查 Manifest 等待、Session 队首、prepare 和依赖错误。
- complete 成功但 reply_pending/oldest_reply 增长：检查 NATS PubAck、权限与 Reply Relay。
- Reply 已发布而用户未收到：继续查 Gateway 的 Completion proof、Delivery 状态和 Provider 回执，
  不把 Worker 的 publish 成功当作外部发送成功。
- fenced/renew 错误或 observation.dropped 增长：分别检查 lease/DB 延迟和日志采集阻塞。

## 验证与边界

`internal/infra/telemetry` 测试验证实际 OTLP protobuf HTTP 导出、低基数标签、敏感负例、
Collector 失联、阻塞 stdout、有界关闭、禁用 exporter 不继承环境目标。Processor 测试验证
观察成功/失败阶段不改变执行次数、Completion 或失败分类。真实 PG gate 验证积压随
Accept → Claim → Complete → Reply 发布变化，已发布历史不计入待处理积压。

IM Tracing V1 已完成 M0–M5 本工作树开发与首版验收，包含 Collector/Tempo/Grafana
最小部署和真实 Telegram/DeepSeek 联合 Trace；完整告警、日志检索平台和 Dashboard
产品化不在本切片内。逐项证据见[完成审计](../../docs/architecture-next/operations/im-runtime-tracing-v1-acceptance.md)。
严格 RunRequested/ReplyIntent JSON、摘要、原提交/去重/重试契约不变；Memory、预算系统
和隐式输出限额没有引入。下方 M1–M4 增量说明中的部署时间点属于历史记录，当前部署
状态以本节和 M5 最终状态为准。

## 显式启用 OTLP Traces（M1）

在 `WORKER_CONFIG_FILE` 的完整根对象加入以下片段；它和 `telemetry` 独立可选：

```json
{
  "tracing": {
    "traces_endpoint": "https://collector.example.com/v1/traces",
    "sampling_ratio": 1.0,
    "export_timeout": "2s",
    "batch_timeout": "1s",
    "max_queue_size": 2048,
    "max_export_batch_size": 512
  }
}
```

六字段必须齐全，不接受 null/重复/未知字段。端点必须显式为 `/v1/traces`，不含
userinfo/query/fragment；远端 HTTPS 使用系统根和 TLS 1.3，本地 HTTP 仅允许字面回环 IP。
容量为正、batch 不大于 queue，ratio 位于 [0,1]，超时至少 1ms；这些只控制观测资源。
没有 exporter 自动重试，不跟随重定向，Collector 响应读取最多 64 KiB；固定错误分类不含
响应正文/地址。配置缺省不创建 Trace exporter、不读取 `OTEL_*` 来继承网络目标。

- 每进程一个 Provider；bootstrap 显式注入平台 Tracer，并绑定 SDK 独立 Tracer。没有替换
  OTel 全局 Provider/propagator，也没有第二套 SDK exporter。
- Resource 使用实际 vcs.revision（缺省 development）、Worker ID，以及
  `DEPLOYMENT_ENVIRONMENT`（缺省 unspecified）；启用 Metrics 时共用这一 Resource。
- 当前局部链为 `worker.run.attempt → worker.runner.run → invoke_agent → chat`。
  Runner Span 覆盖事件流与已有取消 drain；Tool 仅在实际 SDK 调用时产生。
- SDK 源端 Drop 加导出 allowlist；不导出正文、原始错误、Span Event、Status description
  或任意 tracestate，Link 只保留关系与白名单属性。Memory 仍未接入。
- 异步 `worker.operation` 日志在入队时捕获有效 trace_id/span_id；后台 writer 不创建关联。
  SDK 的 context 日志帮助函数未保留 ctx，因此 `worker.sdk` 只有固定级别，不冒充 Run 日志。
- `Runtime.Stats()` 的 Finished 是已结束采样 Span 数，Exported 是整批确认成功 Span 数，
  ExportFailures 是失败导出调用数。差值包含在途/丢弃/失败，不是精确 drop 指标或业务账本。
  这些当前为进程内统计接口，不是新 HTTP 指标端点。

本次代码没有改变现有服务配置或运行二进制。新源码支持配置不等于当前 Telegram 实例已
导出 Trace；M2–M5 完成后才验证跨重启持久关联、正式 Session/Reply 和真实 IM 查询链。
回退旧二进制时先移除新增 tracing 配置；代码回滚演练只在独立副本进行。

## M2 持久入站增量

Run Consumer 的 process Span context 随首次接纳保存，后台调度从 `ScheduledRun.Carrier`
恢复至当前 activeCtx；旧 HTTP/消费 ctx 的取消不会传入新尝试。未采样 context 仍保存与
传播，重复接纳不替换首次 Carrier。Migration 为 `0005_run_trace.sql`，只追加可空列。

本地专项验证包括实际 PostgreSQL、实际 JetStream、Consumer ACK 次序、调度取消与
新的恢复测试进程。运行中的 Telegram 部署未切换，M3/M4/M5 与完整外部查询验收仍待完成。

## M3 Session 分层增量

局部运行链增加以下实际 Span：Manifest resolve、Claim、Runtime prepare、Credential resolve、
Session open/load/stage/verify、Run complete/find_completion、Session commit、实际 terminalize。

`worker.session.stage` 成功的 outcome 是 candidate，不代表正式提交；原有不确定写入后的
确定性读取核验作为 verify 子 Span。正式 commit 在数据库事务返回后才标 accepted，服务端
明确拒绝标 failed，已尝试提交但响应不确定标 UNKNOWN；重放已有 Completion 没有第二次
正式 commit Span。运行状态使用 `app.run.status`，与事务操作是否完成分开。

`session.overlay.get/create/append` 仅在真实 SDK 调用时出现，partial append 不逐 token 生成
Span。Runner 的 `app.session.overlay.appends/bytes` 为临时操作计数/快照字节数，不是正式
Session 提交数或费用字段。所有新字段仍经过进程导出白名单，不携带原文或底层错误。

该代码已做实际 PG、SDK/OTLP 专项验证，未切换运行部署；Reply/Delivery 与完整 IM 联合
验收继续由 M4/M5 完成。Memory 仍为 DEFERRED。


## M4 Reply 增量

Reply creation context 与 Completion 同事务保存；Relay 以不变 creation Header 发布、每次发布有独立 Span。Gateway Delivery 持久接续至真实 SendFinal，proof mTLS 显式传播。新增迁移 0006（Worker）/0013（Gateway），旧行 NULL 兼容。M4 源码与专项测试已完成，当前手测部署未切换，完整部署/真实 IM 验收继续由 M5 完成。


## M5 观测栈与真实联合验收（已完成）

独立 Collector/Tempo/Grafana、TLS overlay 和完整故障矩阵已通过。原手测实例已切换
新版 Worker/Gateway，真实 Telegram 输入经过 DeepSeek、正式 Session 提交和 Final 回复
后，在同一 Trace 中查询到 29 个 Span。Trace ID 为 `6edda2f2a31bd94ba186a9a5fdcdc6dc`，
Run ID 为 `4a34369a10c5f4a2bbaa073e210a47b7`；DB 摘要、accepted head 与真实发送回执
已交叉核对。该实例的子进程归属记录在原状态目录的 `TRACING_UPGRADE.json`。

入口为 `scripts/test-tracing-v1-stack.py`、`scripts/test-im-tracing-v1.py` 和只读
`scripts/worker-v1-joint/live_trace_verify.py`。配置与查询见 `deploy/compose/TRACING_V1.md`。
Memory 仍为 DEFERRED；Tool 仅验收隔离 SDK 契约，没有开放生产执行能力。
