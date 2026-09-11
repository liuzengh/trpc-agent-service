# IM 运行链路 Tracing V1 设计与实施计划

- 设计状态：接入方向已确认；本文冻结首版实施口径。
- 实现状态：M0–M5 已完成本工作树代码开发和首版验收；T01–T17 证据见[完成审计](im-runtime-tracing-v1-acceptance.md)。T15 已在原手测部署使用真实 Telegram 与 DeepSeek 验证，Memory 仍为 DEFERRED。未提交/推送不等于已合并发布。
- 日期：2026-09-08（最终验收更新）。
- 开发基线：`worker` / `fd1f790bf4f779216ed44373bd2356aa3ce7e186`。
- SDK 基线：`trpc-agent-go v1.11.2`、现有 OpenTelemetry Go `v1.29.0` 依赖。
- 执行责任：当前 task 在当前 Worker 工作树直接完成 Gateway、Worker、协议、部署及测试修改；其他工作树只可询问，不派发修改、不要求同步实施。
- 上位设计：[可观测性与 Telemetry](observability.md)；现状：[Worker 运行信号](../../../services/agent-worker/OBSERVABILITY.md)。

## 1. 目标与首版完成定义

首版用同一条 Trace 关联一条真实 Telegram 文本输入的 IM callback、持久 Admission、
Worker 接纳和调度、Runner/LLM、正式 Session 读写、Completion、Reply Relay 和 IM 回复。
同一业务处理的正常重投、重试和进程重启不丢失持久关联。

“端到端”表示可查询的因果关系，不表示平台能观测 Telegram 内部链路、用户终端展示或已读。
完成事实仍来自 Run/Completion/Session commit/Delivery 账本，Trace 仅承担诊断。

首版范围：

- Gateway 与 Worker 进程级 TracerProvider、OTLP HTTP 导出、日志 Trace 关联。
- W3C Carrier 在两次 NATS 传递及四处持久化交接中的保存和恢复。
- Runner 完整流式生命周期；复用 SDK Agent、LLM 与已具备的 Tool 埋点。
- 正式 Session load、临时 overlay、candidate stage、accepted head commit 的区分。
- Collector → Tempo → Grafana 最小查询链；本地显式全采样验收。
- 格式错误、重放、重启、取消、失租、导出故障及正文负例验收。

后置范围：

- Memory 整体后置；首版只定义真实接入时的接口位置和测试契约。
- Tools/Knowledge、Parallel/组合执行能力不因 Tracing 扩展；SDK 支持不等于 Worker 已开放。
- 累计 Token 预占、费用结算、Memory projector、新预算和隐式输出上限均不引入。
- Control 发布链全量埋点、Loki/Alloy/Prometheus/Alertmanager 全栈、tail sampling、
  完整 Dashboard/告警及多 Collector 路由后置。
- 不生成没有发生的 Tool/Memory Span，不将能力尚未开放记为全链路验收成功。

## 2. 已核对的实现与接入差异

| 接入前实现 | 接入动作 |
| --- | --- |
| Worker 只有 JSON 日志和可选 OTLP Metrics | 增加独立 Tracing 配置，保留旧 Metrics 配置的有效性 |
| SDK 的独立 Tracer 初始为 No-op | 在 bootstrap 启动业务前绑定进程统一 Provider |
| SDK trace.Start 自建 Provider、默认 AlwaysSample | 平台自行拥有采样、Resource、批量导出与有界关闭，不重复启动 SDK exporter |
| Observer 在阶段结束后汇报结果 | 在真实调用前 Start Span，传递返回的 ctx；保留 Observer 做指标/日志 |
| Run Consumer 先落库 ACK，后台另行调度 | Carrier 与 Run 接纳同事务保存，调度从数据库恢复 |
| Gateway Reply 先接受 Delivery Intent，再单独记 transport receipt | Carrier 随 Intent 接受事务保存，保留现有两事务和 ACK 时点 |
| RunRequested/ReplyIntent 是封闭 Schema，摘要参与重放 | Carrier 使用 NATS Header 和独立数据库列，不进入 JSON 与业务摘要 |
| SDK Session 使用 attempt overlay，Worker 另行持久候选并提交 head | 分层埋点，不把临时追加或候选写入当成正式提交 |
| SDK 可采集消息/Tool 参数结果/原始错误 | 源端 Drop 加进程内导出字段过滤，再由 Collector 兜底 |
| 当前首版不组装 Tool/Memory 运行能力 | 基础接线与将来的能力验收分开记录 |

源代码核对入口见 §15。不得以依赖存在、测试 fixture 返回或文档更新代替实际接线证据。

## 3. Trace 模型与 Span 生命周期

### 3.1 Trace 的单位

一条首次接纳的 IM 输入对应一条逻辑执行 Trace；同一 Session 的不同轮次分别创建 Trace，
通过内部 Session ID 查询关联，不将整个对话或长连接塞进单个 Trace。

外部 webhook 入口建立本地根上下文；外部请求自带的采样位、tracestate 不作为平台信任来源。
内部受控 HTTP 使用 W3C 传播；第三方模型/IM 请求默认只记录本地 client Span，不自动向任意
外部目的地转发 Carrier 或 Baggage。受控远端 Tool 的传播另外显式配置。

Webhook 的 HTTP Span 在回调响应结束时结束。Long polling/WebSocket 对每条 Update 建立
消息接收 Span；循环取消息、连接保活和批量抓取不作为所有消息的共同执行根。

### 3.2 逻辑阶段图

以下是阶段顺序；异步创建、发布、消费的精确父子/Link 关系以 §4 为准。

```text
gateway.im.callback / gateway.im.receive
  → gateway.run.admit
  → create execution.run-requested.v1
  → publish execution.run-requested.v1
  → process execution.run-requested.v1
  → worker.run.attempt
      ├─ worker.run.claim / manifest.resolve / runtime.prepare
      ├─ worker.session.open / worker.session.load
      ├─ worker.runner.run
      │   └─ invoke_agent <agent>                 SDK
      │       ├─ chat <model>                     SDK
      │       ├─ execute_tool <tool>              有实际调用时
      │       └─ session.overlay / memory.*       有实际操作时
      ├─ worker.session.stage
      └─ worker.run.complete
          ├─ worker.session.commit
          └─ create execution.reply-intent.v1
  → publish execution.reply-intent.v1
  → process execution.reply-intent.v1
  → gateway.reply.deliver
      ├─ gateway.reply.verify
      └─ gateway.im.send                         按分片和发送尝试
```

### 3.3 生命周期和成功语义

- `worker.runner.run` 在 Runner.Run 前开始，覆盖事件消费至关闭；取消时覆盖已有有界 drain。
  不新增 detach、不延长 execution deadline、不改变 renewal/fence 规则。
- 每次实际调度处理有独立 Span；成功 Claim 后记录 Attempt ID。空轮询、正常续租不逐次刷 Span。
  阶段失败记录固定 error.type；预期 duplicate/wait/fence 结果采用明确 outcome。
- SDK 的 Agent/LLM/Tool Span 保留既有名称；平台不再制造一层含义相同的 model/tool 调用 Span。
- Session commit Span 只有 Completion 事务确认提交后才表示接受成功；不确定提交标明
  unknown，并单独记录 FindCompletion 查询与最终核验结果。
- IM send 以实际 Provider 调用为界，结果区分 ACCEPTED、NOT_SENT、UNKNOWN。
  ACCEPTED 不是已读；NATS PubAck 不是 IM 已发送。
- Queue wait/outbox age 来自持久化时间戳或现有指标，不伪造覆盖进程离线时间的常驻 Span。
  跨进程时钟使用 UTC/NTP；不得用 callback Span 的时长充当整条执行耗时。
- 同一 Run 的自动重试继续原 Trace，创建新的 Span/Attempt ID。人工发起的新 Run 建新 Trace，
  有需要时 Link 原 Run，而不是沿用旧 Span ID。
- 进程硬退出可能丢失尚未导出的 Span；持久 Carrier 保证后续关联，并非保证 Trace 永不缺段。

## 4. Carrier 传输协议

### 4.1 值类型与归一化

M1 已新增纯技术 Module `platform/tracecontext`，只承担 Carrier 值、W3C 解析、注入/提取及
归一化。它不拥有 Provider、数据库、租户授权、运行状态或费用语义；业务 Domain 不依赖 OTel SDK。

```go
type Carrier struct {
    Traceparent string
    Tracestate  string
}
```

载体规则：

1. 仅承载 `traceparent`、`tracestate`，不默认接入 Baggage。
2. Header 名大小写不敏感；traceparent 多值歧义、非法 ID、控制字符按无有效 parent 处理。
   tracestate 遵循 W3C 格式；合法 parent 配非法 state 时保留 parent、清空 state。
3. 采用 OTel W3C propagator 做语义解析，不手写随机 trace ID、Span ID 或 sampled 位。
4. 明确观测元数据容量：输入 traceparent 最多 512 字节，tracestate 最多 512 字节且最多
   32 个成员；超过容量仅丢弃对应观测元数据。前者是平台接收容量，后者按 W3C 列表约定。
   这些是显式观测协议容量，不是 Manifest 字段、Token 限额或 Run 拒绝条件。
5. 解析和归一化发生在业务事务之前；缺失 Carrier 不导致入站拒绝、NAK 或 Run 失败。
6. tracestate 不写业务 ID/自由文本，入口不透传外部提供的任意 state，导出时仅保留已批准
   的供应商键。数据库保存值和对外传播值均使用同一归一化规则。
7. 未采样的有效 SpanContext 仍传播和保存。关闭 exporter 不应切断已有合法 Carrier；
   无上游且未启用 tracing 时允许没有 Carrier，不为关闭状态伪造观测成功。

W3C 字段语义参考 [Trace Context](https://www.w3.org/TR/trace-context/)。

### 4.2 NATS Header 与不可变创建上下文

- Gateway 在 Admission 事务前创建 RunRequested 的 message creation Span，注入并持久化
  Carrier；事务失败则该 Span 标为失败，不产生待发布业务记录。
- Relay 从 Outbox 恢复 creation context，每次实际 publish 创建独立发送 Span，并 Link
  creation context。Header 注入持久化的 creation context，不随发布重试改成另一份消息身份。
- 对应发送 Span 使用 CLIENT；独立 message creation Span 使用 PRODUCER。
  若未来某条路径没有持久 creation Span、由 send 作为 creation，则再使用 PRODUCER send。
- Consumer 为每条处理创建 CONSUMER Span；单消息处理以 creation context 为 parent，
  并保留 creation Link。已有其他 ambient Span 时用 Link 保留其关联，不得覆盖消息因果链。
- Worker 处理 ReplyIntent 的创建/发布采用同样规则。
- 调整 Publisher Interface/Adapter 以支持 Msg/Header，保留原 payload 和 WithMsgID。
  消费端从 Header 提取，payload 严格解码独立执行。
- 不把 Header 加入 EventDigest、RunDigest、ReplyIntentDigest、SourceDigest、Final proof、
  transport raw digest 或 broker 去重 ID。
- NATS receive/fetch 的耗时与逐消息 process 分开。批处理未来用 Links，不让一个消息成为
  其他消息的父级。

本项目明确选择“单消息 creation parent”模式；默认 Link 和单消息父子模式的区别参考
[OTel Messaging Spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)。

## 5. 四处持久化与事务规则

继续使用同一 PostgreSQL/database 及既有 schema/role 隔离。完整 Span 不写业务数据库，
仅在既有业务行增加两列可空 `traceparent`、`tracestate`。不新建 Trace Repository/事实表。

| 记录 | 保存的 Carrier | 保存事务 | 恢复消费者 |
| --- | --- | --- | --- |
| Gateway `gateway_outbox` | RunRequested creation context | 原 Admission/Inbox/Outbox 事务 | Gateway Relay |
| Worker `execution_runs` | 首次成功持久接纳时 process Span context | 原 Receipt/Run 事务 | Ready/Advance、retry、terminal recovery |
| Worker `execution_reply_outbox` | ReplyIntent creation context | 原 Completion/Session commit/Reply 事务 | Worker ReplyRelay |
| Gateway `gateway_delivery_intents` | 首次成功接受 Reply 时 process Span context | 原 Delivery Intent/Parts 接受事务 | Delivery Runner、分片及重试 |

补充规则：

- 首次保存的 Carrier 不因 duplicate/replay 被更新，即使首次值为空；后来的重投只能添加新的
  观测尝试/Link，不改已存在的逻辑执行根。老数据没有 Carrier 时使用新诊断根和稳定业务 ID，
  不宣称历史段已经补齐。
- Gateway Admission 对既有 Receipt 的重复输入可按既有 Admission/Event ID 查对应 Outbox
  Carrier 做 Link；记录已清理则只保留业务 ID，不为了 Trace 延长 Outbox 保留期。
- Gateway Delivery Intent 已提交、transport receipt 尚未提交时重投：Intent 中的 Carrier
  已持久，重放仍复用原根。保留当前两事务与 DoubleAck 顺序。
- Worker 的 terminalize/超时恢复及任何能产生 Reply Outbox 的路径都要携带持久 Run context；
  只在成功 Runner 返回路径设置 Carrier 不足以完成接入。
- 调度/Outbox 返回值在 Application Interface 显式携带 Carrier；如 ScheduledRun 包装
  domain.Run，不把 tracing 列混入 domain.Requested 的 JSON 或 Digest。
- 同步调用传 ctx；重新调度时以当前调度生命周期 ctx 为底恢复 SpanContext，再施加原有
  execution deadline。禁止在这里复活旧 HTTP context 的取消/超时状态。
- Gateway/Worker 各自追加 migration，编号在实施时按当前目录分配；不改历史 migration、
  不跨 schema 写数据、不增加 runtime DDL 权限。默认 NULL，不全表回填历史 Trace。
- 新字段随原有行生命周期清理，无独立 GC、无 trace 索引首版需求。

## 6. TracerProvider、SDK 与日志接线

### 6.1 所有权

M1 将可复用的纯技术实现收敛到 `platform/telemetrytrace`，由 Gateway/Worker 各自
bootstrap 创建、注入和关闭独立进程实例，不复制两套 exporter/filter。共享模块管理 Provider、
Resource、OTLP HTTP、非阻塞批量队列、字段过滤和有界关闭，不包含业务或数据库依赖。
Worker 既有 `internal/infra/telemetry` 继续拥有 Metrics/JSON 日志；业务埋点由相应
Application/Adapter 定义，不为每个 Run、租户、模型创建 Provider。

bootstrap 在 HTTP/NATS/后台调度之前完成接线，业务停机后用新的有界 shutdown context
flush。业务 readiness 不增加 Collector 可用性条件；本地非法显式配置在启动校验时报错，
已启动后的 Collector 失联不升级为执行失败。

保留 SDK v1.11.2；现有 OTel v1.29.0 优先同版本接入。新增依赖必须兼容当前 API，
本切片不搭车升级整个 SDK/语义约定。HTTP 自动埋点包若引入，单独锁定兼容版本和测试。

### 6.2 SDK 桥接示例

以下代码只表示 Provider 桥接，不是完整启动配置。属性策略须先安装，所有赋值在业务启动前
完成；测试保存/恢复 SDK 全局变量，不在并行测试或运行中动态替换。

```go
// Worker bootstrap，所有业务 goroutine 启动之前：
trpcagent.BindLogging(observation.SDKLog)
trpcagent.BindTracing(traces.Provider())
// 平台自身显式注入：
tracer := traces.Tracer("agent-worker/execution-v1")
```

不额外调用 SDK trace.Start 创建第二个 Provider/exporter，也不替换 OTel 全局 Provider/
propagator。`BindTracing` 安装源端 Drop 策略并绑定 SDK 独立 Tracer；取消上下文的 Span
在 End 时记录固定 cancelled/deadline，覆盖当前 SDK Tool 取消分支缺少状态标注的情况。
平台自身显式注入 Tracer，SDK 的进程级桥接仅在 bootstrap 组装成功后执行一次。

### 6.3 Application Span 与 JSON 日志

Observer 继续表达低基数操作结果和 Metrics；增加调用前 Span lifecycle，而不是从结束事件
反推一棵 Trace。使用 Start 返回的 ctx 调用 Prepare/Load/Execute/Stage/Complete 和 SDK。
不引入新的 Run 决策事件总线或遥测专用业务调度器。

日志 record 入有界队列时捕获 trace_id/span_id；后台 writer 只写入记录，不读取自己的
context。缺少有效关联时省略这些字段。Collector/日志阻塞不改变已有 Completion 次数、
失败分类、lease 或重试策略。

SDK 自身的错误日志也可能含 Tool 参数/整个 event，独立于 Span 属性策略。M1 增加
`BindLogging`：只将固定 info/warn/error/fatal 级别传入原有有界 JSON 队列，丢弃原始格式和
参数。当前 SDK context 日志辅助方法未保留 ctx，因此 `worker.sdk` 只记进程级事件，不伪造
trace_id/run_id。平台 `worker.operation` 才按调用 ctx 关联 Trace。

## 7. Session、Tool 与 Memory 的精确接入点

| 操作 | 埋点位置 | 语义 |
| --- | --- | --- |
| Session Open/Load | runtimeadapter、sessionstore Adapter | 构造/校验目标和加载已接受快照 |
| Session overlay Get/Create/Append | trpcagent overlay 的实际方法 | attempt 临时状态；不逐 token 制造 Span |
| Candidate Put | runtimeadapter.Stage → sessionstore.Put | 候选持久化；不等于正式接受 |
| Session commit | postgresadapter.Complete 的事务 | 提交成功后才标为 accepted head 已推进 |
| LLM | SDK chat Span | 复用 SDK；平台记录实际模型调用的必要元数据 |
| Tool | SDK execute_tool；实际远端 Adapter | 只在真实执行时出现；错误/取消/并发关系须验收 |
| Memory | 未来实际 memory.Service 的方法 Adapter | search/read/write/delete；不提前创建存储或生成器 |

Session overlay 聚合高频追加的计数/字节数，必要的完整事件追加才形成操作 Span；避免按流式
token 产生海量 Span。Trace 不携带快照内容或摘要的原文输入。

Memory 同步读写传递调用者 ctx；异步记忆生成需要在其任务持久化交接中保存 Carrier，
长生命周期独立任务用新 Trace + Link。届时明确实际 SDK Interface 方法，不现在创造虚构
CRUD Interface、Memory 消费者或 projector。

可用隔离的 SDK Tool fixture 验证 Provider 接线、父子关系和过滤；该证据只证明 SDK 接线，
不代表生产 Manifest 已支持 Tool。当前 Memory=nil，验收应标为 DEFERRED，而不是 PASS。

## 8. 字段、过滤与数据界限

### 8.1 采集白名单

- Resource：service.name/version/instance.id/namespace、deployment.environment.name。
- Trace/日志关联：内部 run/attempt/session/intent/binding ID；自定义属性统一 app.* 命名，
  现有日志字段保留兼容，ID 不默认成为 Metrics label。
- 操作：稳定 Span 名、stage、outcome、error.type、时长、重试/分片编号。
- Messaging：固定 system/destination/operation、message ID、redelivery 次数。
- 模型：允许的模型标识、实际 usage、TTFT、finish reason；不合成未知 usage。
- 存储：backend/operation、记录数、字节数、版本、candidate/accepted 状态。

### 8.2 默认不采集

原始 IM callback/聊天正文、Prompt、模型完整输入输出与思考正文、Tool 参数结果、
Session/Memory 文本、认证头、API key、DSN、SQL 参数、URL 路径中的 Bot token、
原始错误正文/堆栈、自由文本 Tool description 和任意 RunOptions.SpanAttributes。

过滤必须覆盖 Attributes、Events、Links 属性和 Status description，需覆盖全部属性层级。
HTTP URL/route 和 SQL instrumentation 先审查字段；尤其 Telegram URL 中包含凭据，
不得把完整 URL 交给默认观测导出。

### 8.3 三层处理

1. SDK SpanAttributePolicy 在源端 Drop message/request/response/tool arguments/result、
   workflow payload 等已知正文，避免无谓序列化。
2. 进程内 exporter Adapter 产生只含白名单的只读 Span 视图/副本，交给真实 exporter；
   保留关系、时长与固定错误分类；M1 不导出任何原始 Span Event，过滤 Link 属性、清空
   Status description 和导出的 tracestate，清理 scope/version/schemaURL。只读 SDK SpanData
   不原地修改；未批准供应商 state 的首版导出为空。
3. Collector 再次处理字段。业务错误不通过观测组件回写；过滤遗漏以 canary 测试阻断发布。

SDK 属性策略对未知 key 的默认行为仍可能采集，因此第二层不是可选项。Truncate 不等于
去正文；不得用截断替代 Drop。Exporter 队列容量属于观测资源配置，不是业务预算。

## 9. 配置与最小部署

### 9.1 配置兼容

新增独立根对象 `tracing`，保留当前 `telemetry.metrics_endpoint/export_interval/export_timeout`
的全部行为，不把 traces endpoint 塞进 Metrics 三字段校验。Gateway 增加相同语义的配置入口，
不改 Account、AgentSpec、Manifest 或 Runtime Profile。

以下配置由 M1 源码支持。当前手测部署已按 §28 切换；未显式启用的实例仍缺省关闭：

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

Gateway 沿用现有环境变量配置形式：`GATEWAY_TRACING_CONFIG_FILE` 指向绝对路径的
JSON 文件，文件直接保存上述 `tracing` 内的六字段对象，不额外套根对象；文件最多 64 KiB、
必须是普通文件。启用时要求 `GATEWAY_INSTANCE_ID`。Worker 的入口仍为根 `tracing`。

- 对象缺省时不建 exporter、不从 OTEL 环境变量隐式继承目标；现有日志/Metrics 继续工作。
- 显式对象按严格字段校验；ratio 在 [0,1]，queue/batch 为正且 batch ≤ queue；
  容量/超时在配置中可见，不伪装为生产容量结论。
- Resource 版本读取构建的 vcs.revision，缺失时为 development；环境读取显式
  `DEPLOYMENT_ENVIRONMENT`，缺省 unspecified。启用 Tracing 时 Worker Metrics 使用
  同一 Resource；仅启用旧 Metrics 时保留 worker-v1/unspecified 默认值。
- 远程 HTTPS 保留证书校验；本地测试 HTTP 仅使用显式 loopback endpoint。
  本机业务进程使用显式 loopback HTTP；应用容器通过 TLS overlay 接收，不为容器名隐式放宽 HTTP 校验。
- M1 使用官方 OTLP protobuf encoder 和受控 HTTP transport：响应读取最多 64 KiB、
  拒绝重定向、不读取 OTEL endpoint/header 环境配置、无 exporter 自动重试。batch/请求/
  关闭均有界，队列满丢弃观测而不阻塞执行；错误只有固定分类，不打印 Collector 错误正文。
- `Runtime.Stats()` 提供 Finished（已结束的采样 Span）、Exported（整批确认成功的 Span）
  与 ExportFailures（失败导出调用）计数。Finished−Exported 包含在途、丢弃与失败，不是
  精确队列丢弃数，也不是业务失败数。进程外健康指标和精确 drop 统计留待后续观测补充。
- 旧配置对新二进制有效；回退旧二进制前移除新增 tracing 对象，因旧严格配置会拒绝未知字段。

### 9.2 部署最小集与采样

应用 → Collector → Tempo → Grafana。Trace Backend 使用自己的存储卷，不借业务 schema
存完整 Span。应用不持有 Grafana/Tempo 后端凭据，观测查询限制为平台运维角色；
共享后端的 tenant_id 标签不提供租户隔离，首版不开放租户直接查询。

部署资产放在既有 deploy 目录的独立 tracing overlay/profile，不强制启动完整日志/告警栈，
镜像版本/digest、OTLP 路由和 Trace 保留期在实施时显式记录。

本地验收显式采样 100%；生产 ParentBased(TraceIDRatioBased(ratio))，根端决定采样，
下游尊重决定，未采样 context 仍持久传播。首版不宣称所有错误 Trace 都被保留。
未来 tail sampling 须另做决策窗口、跨 Collector 的 trace-ID 路由及晚到 Span 验收。

## 10. 代码落点与单工作树责任

以下路径均相对当前仓库。全部由当前 task 在 worker 工作树实施，不是派工列表。

| 落点 | 计划改动 |
| --- | --- |
| platform/tracecontext/（新增） | Carrier、归一化、W3C Inject/Extract；纯技术共享 |
| platform/telemetrytrace/（M1 新增） | 两服务复用的 Provider/exporter、过滤、Carrier 无关的技术生命周期 |
| services/agent-worker/internal/infra/telemetry/ | 保留 Metrics、有界日志；新增 Trace 关联、SDK 固定日志和共享 Resource |
| 两个服务 internal/bootstrap/ | 独立 tracing 配置、Resource、生命周期与 HTTP 接线 |
| Gateway admission Application/PG/Telegram Adapter | 入站根、creation context、Outbox 同事务元数据 |
| Gateway infra/nats 与 admission relay | Header 发布、独立发送尝试和 creation Link |
| Worker infra/natsadapter、execution Application/PG | 接纳保存、调度恢复、attempt/阶段与失败恢复 |
| Worker runtimeadapter/trpcagent/sessionstore | Runner 完整生命周期、SDK 桥接、Session 分层 |
| Worker ReplyRelay/Completion | 所有 Final 产生路径的 Carrier 与重放 |
| Gateway delivery Consumer/Application/PG/Sender | 接受保存、两事务兼容、分片和重试恢复 |
| 两个服务 migrations/ | 仅各自表追加可空列，保留角色隔离 |
| api/events/execution/v1/ 文档和测试 | Header 协议；证明原 JSON/DTO/digest 不变 |
| deploy/compose/ 及实际已有部署资产目录 | Collector/Tempo/Grafana 独立 overlay |
| services/agent-worker/integration/ 与 Gateway 测试 | 单进程、真实 PG/NATS、重启和真实 IM 验收 |

当前 branch 未包含其他工作树所有后续 receive-mode/preflight 修改。仅在本工作树核对和处理
需要的接缝；其他工作树可只读咨询，不触发修改或同步。V1 实机验收先覆盖此基线真实可用的
Telegram webhook；未集成模式明确列为待验证，不顺带改变 receive_mode 行为。

## 11. 分阶段实施计划

| 阶段 | 交付内容 | 必须通过的退出条件 | 当前状态 |
| --- | --- | --- | --- |
| M0 设计冻结 | 本计划、总体设计修订、Worker 状态/入口更新 | 本文与现有契约对齐；文档检查通过 | 文档完成 |
| M1 进程接线 | Carrier Module、Provider、SDK 桥接、过滤、配置、日志关联 | 实际 OTLP HTTP protobuf 导出；SDK child span；正文负例；有界故障 | 已实现、验证并部署到本机手测实例 |
| M2 入站到持久 Run | Gateway 入站/Outbox Header、Worker intake/Ready 恢复及迁移 | 原 wire/digest 不变；真实 PG/NATS；接纳后重启同 Trace | 已实现；PG/NATS 与精确恢复窗口通过 |
| M3 Runner 与正式 Session | attempt、完整 Runner、SDK LLM、Session 分层和 terminal recovery | 事件流结束才 End；取消/失租语义不变；candidate 与 commit 正确区分 | 已实现；正式 Session、取消、失租、deadline 通过 |
| M4 Reply 到外部发送 | Reply creation/relay、Delivery 持久关联、分片/重试、证明 HTTP | Worker/Gateway 分别重启后继续关联；不多执行/多回复；状态真实 | 已实现；分片/不确定发送/恢复及真实 IM 通过 |
| M5 最小部署与联合验收 | Collector/Tempo/Grafana、真实 Telegram、退化/回滚证据 | §12 的首版必验项通过，按 run_id/trace_id 可查询 | 已完成；T01–T17 及真实 Telegram Trace 联合验收通过 |

执行顺序 M1 → M2 → M3 → M4 → M5；每个阶段保持功能开关缺省关闭和旧配置可运行。
M1 至 M4 可用本地测试 Collector 验证，无需先部署完整运维栈。
M5 完成后才更新“IM Tracing V1 已交付”；Memory/Tool 生产能力仍单独排期。

## 12. 验收矩阵与证据

| ID | 输入/动作 | 期望结果 |
| --- | --- | --- |
| T01 配置兼容 | 原 Worker/Gateway 配置；tracing 缺省 | 无新增遥测外连；原 JSON/Metrics 行为保持 |
| T02 Carrier | 有效/未采样/缺失/非法/超长/多值/Header 大小写 | W3C 规范化；无业务拒绝或 NAK；非法正文不写日志 |
| T03 SDK 桥接 | 测试 Runner + 确定性模型 + OTLP receiver | Agent/LLM child 与 Runner 同 trace_id；按真实事件关闭 |
| T04 正文过滤 | 在 IM/Prompt/响应/Tool/error/URL/SpanAttributes 各放 canary | 实际导出 protobuf、Events/Links/Status/日志均无 canary |
| T05 业务摘要 | 同一业务 payload 不同 trace Headers | event/run/reply/raw digest、去重 ID 和授权结果不变 |
| T06 Gateway Outbox 恢复 | 接纳提交后、发布前重启 Gateway | 原 creation context 被恢复，消息正确发布 |
| T07 Worker 接纳恢复 | Run 接纳 ACK 后、Claim 前重启 Worker | 新 attempt Span 接回原 Trace，业务执行次数正确 |
| T08 重投与重试 | NATS redelivery、同 Run 重放、可重试模型失败 | Span ID 各异；原持久 Carrier 不改；原幂等/重试策略保持 |
| T09 Session | Load→overlay→Stage；分别模拟 Stage/Complete 失败 | 临时/候选不冒充正式 commit；accepted head 与原事务一致 |
| T10 Reply 恢复 | Completion 后、Reply 发布前重启 Worker | 无额外 Runner 执行；Final Carrier 与 payload 保持 |
| T11 Delivery 两事务 | Intent 接受后、transport receipt 前重启 Gateway | 重放不覆盖 Carrier；后台发送接回原 Trace |
| T12 分片与发送重试 | 长 Final、429、明确 NOT_SENT、UNKNOWN | 每分片/尝试独立 Span；不改原安全重试及确认语义 |
| T13 取消/失租 | 流式执行中取消、deadline、lease 丢失 | 原取消/drain/fence/Completion 规则保持；Trace 如实结束 |
| T14 导出故障 | Collector 失联、慢响应、队列满、stdout 阻塞 | 业务继续；有丢弃/失败观测；有界 shutdown；无无限 retry |
| T15 真实联合 | 真实 Telegram 文本→真实模型→正式 Session→回复 | 同 trace_id 串起所有实际阶段，PG/Delivery 事实交叉核对 |
| T16 回滚兼容 | 移除 tracing 配置，运行旧构建；保留新增列 | 原业务可运行；旧 payload 解码/摘要通过；无破坏性 down migration |
| T17 Tool 契约 | 隔离 SDK Tool fixture 的成功/错误/取消/并发 | 接线/过滤/父子关系通过；不等同生产 Tool 能力 |
| T18 Memory 契约 | 未来实际 memory.Service 同步与异步读写 | 能力落地后才执行；当前记 DEFERRED，不生成占位 Span |

T01–T17 是首版测试计划（T17 只验 SDK 接线）；T18 随 Memory 能力排期。
真实 PG/NATS 测试显式配置隔离数据库与 broker，报告 skip 数；全量 go test 的绿色
并不替代这些 gate。外部实测与 fixture 分开记，不拿历史 Telegram 成功证明新 Tracing 成功。

每阶段保存：commit/构建标识、脱敏配置、测试命令/输入/字面输出/退出码、运行实例信息、
Trace ID/Span ID/parent/links 列表、查询导出、Run/Completion/Session/Delivery 对照结果。
验收断言父子/Link 关系，不只比较一组手工写入的 trace_id 属性。

## 13. 发布、兼容与回滚

1. 保存基线 commit、配置和待修改文件哈希；分阶段开发不动手测服务，部署验收使用受控切换窗口并保留入口和数据。
2. 迁移只追加可空观测列；先用隔离数据库验证，再在计划部署窗口应用。
3. 新构建先使用 tracing 缺省配置验证旧行为，再显式启用 Collector endpoint。
4. 先测试环境 100% 采样；真实测试前确认不启动第二个竞争 Telegram receiver。
5. Collector 不可达时维持业务，排查观测链；需要退回则移除 tracing 对象并滚回已验证构建。
6. 数据库保留可空列，旧二进制忽略；不删 Run/Session/Reply，不清 NATS Stream。
   完全移除列只在所有新构建退出且确认无使用后另做维护。
7. 观测关闭与代码回滚分开验收；新旧配置兼容和旧 binary 严格字段拒绝必须测试。
8. 回滚演练在独立副本/隔离部署完成，不在用户正在测试的实例执行破坏性恢复。

本次 M1 实现不包含 migrations、运行二进制切换、运行配置变更或 Collector 部署；
M1 当时未接入持久 Carrier；后续 M2 接线见 §17。当前 Telegram 手测部署已按 §28 接入新版。

## 14. 后续计划与完成状态

| 后续事项 | 触发条件 |
| --- | --- |
| Memory read/write 与异步生成 Trace | 实际 Memory 能力被安排并实现 |
| 生产 Tool/Knowledge/Parallel Trace | Manifest 与执行器正式开放对应能力 |
| Telegram polling/WeCom 的新模式实测 | 对应代码集成到本工作树且具备真实运行条件 |
| Control 发布链/HTTP 全量观测 | IM Tracing V1 完成后独立切片 |
| Loki/Alloy 与 Trace↔日志跳转、完整告警 | 运行查询链稳定后 |
| Tail sampling、多 Collector、长任务 trace 查询 | 有实际容量/保留需求及独立设计验收 |
| 业务费用与预算系统 | 单独产品契约；不由 telemetry 自动推出 |

当前 M0–M5 的实现及证据见 §16–28 和完成审计。真实 Telegram、精确提交窗口重启、
旧构建兼容均有独立证据；没有复用 Worker V1 的历史成功记录充当 Tracing 验收。
总体设计中的完整部署目标仍保留，但不作为本切片前置依赖。

## 15. 依据与核对入口

当前源码（均为 worker 基线）：

- [Worker Metrics 与有界日志](../../../services/agent-worker/internal/infra/telemetry/telemetry.go)。
- [现有 telemetry 配置](../../../services/agent-worker/internal/bootstrap/observation.go)。
- [Run Consumer](../../../services/agent-worker/internal/infra/natsadapter/consumer.go)。
- [Worker Ledger Interface](../../../services/agent-worker/internal/execution/application/ports.go)。
- [Worker 阶段与取消](../../../services/agent-worker/internal/execution/application/execute.go)。
- [Runner/Memory=nil](../../../services/agent-worker/internal/execution/adapter/outbound/trpcagent/executor.go)。
- [Session overlay](../../../services/agent-worker/internal/execution/adapter/outbound/trpcagent/overlay.go)。
- [Candidate Stage](../../../services/agent-worker/internal/execution/adapter/outbound/runtimeadapter/attempt.go)。
- [Completion 原子事务](../../../services/agent-worker/internal/execution/adapter/outbound/postgresadapter/completion.go)。
- [Worker Reply Relay](../../../services/agent-worker/internal/infra/natsadapter/relay.go)。
- [Gateway Admission Relay](../../../services/channel-gateway/internal/admission/application/relay.go)。
- [Gateway Delivery Consumer 两事务](../../../services/channel-gateway/internal/delivery/adapter/inbound/nats/consumer.go)。
- [封闭事件协议](../../../api/events/execution/v1/README.md)。

SDK 核对通过 go list -m 定位锁定的 v1.11.2；查看该模块的 telemetry/trace/trace.go、
telemetry/trace/span_attribute_policy.go、internal/telemetry/trace.go。SDK 全局 Tracer 默认
No-op、trace.Start 的采样与关闭行为、正文属性及原始错误写入均以此版本为准。

标准依据：[W3C Trace Context](https://www.w3.org/TR/trace-context/)、
[OTel Messaging Spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)、
[OTel Go Instrumentation](https://opentelemetry.io/docs/languages/go/instrumentation/)。
标准建议与本文平台选择分别标明；后续依赖升级重新核对，不静默改变消息创建上下文策略。


## 16. M1 实施记录（本地技术验证）

- 实施目录：`platform/tracecontext`、`platform/telemetrytrace`、两个 bootstrap、Worker
  runtimeadapter/trpcagent/telemetry；未修改业务 Domain、事件 JSON/DTO/digest 或 migration。
- 真实 OTLP HTTP protobuf 接收测试断言 Trace ID、parent ID、Resource、Links、采样与过滤，
  并覆盖禁用、环境变量不继承、重定向、超大/partial-success 响应、超时、队列饱和及关闭。
- 固定 SDK v1.11.2 使用本地确定性 HTTP 模型：真实 Runner/Agent/LLM Span 父子关系与
  SSE 结束时点通过；真实 SDK Tool 的成功/错误/取消通过，Tool 测试不开放生产 Tool 能力。
- Tool 取消测试等待 SDK 实际 deferred Span.End，不能只看 Runner channel 已关闭；
  源端固定日志桥接的测试证明 SDK 原始参数/错误未进入 stdout。
- targeted race 覆盖 Carrier、Trace Runtime、SDK、Worker Metrics/bootstrap、Gateway
  bootstrap 和事件协议；全量 Go 测试另行统计外部依赖 skip，不以此替代真实 PG/NATS gate。
- M1 验收时尚无 durable Carrier、Session 分阶段 Span、Reply/IM send Trace 或 Collector
  查询栈；后续 M2 入站持久化记录见 §17，Memory 仍为 DEFERRED。

可重复技术验证（仓库根目录）：

```bash
go test -race -count=1 ./platform/tracecontext ./platform/telemetrytrace \
  ./services/agent-worker/internal/execution/adapter/outbound/trpcagent \
  ./services/agent-worker/internal/infra/telemetry \
  ./services/agent-worker/internal/bootstrap \
  ./services/channel-gateway/internal/bootstrap ./api/events/execution/v1
go test ./...
```

## 17. M2 实施记录（2026-09-08）

本阶段接通持久入站链路，继续由当前 task 在 Worker 工作树直接修改。M3–M5 仍为活动目标，
不以局部 Trace 或专项测试代替整个 IM Tracing V1 的交付。

- Gateway：公开 callback 本地根 → `gateway.run.admit` → 实际 Outbox
  `create execution.run-requested.v1`；新增 `0012_admission_trace.sql`，在原 Admission
  事务保存 creation Carrier。事务提交失败时 creation Span 标为失败，不留下 Outbox。
- Gateway Relay：Claim 同时返回显式 `OutboxMessage.Carrier`；每次发布恢复 creation
  parent，并记录 creation Link；`PublishMessage` 用真实 `nats.Msg.Header`，仍传原 payload
  和 `WithMsgID`。业务 JSON、摘要、去重 ID 和 Published/Retry 顺序不变。
- Worker：Run Consumer 从实际 Header 恢复 parent 并开始 CONSUMER process Span；
  在原接纳事务中保存该 context。新增 `0005_run_trace.sql`，列缺省 NULL。
- Worker 调度：新增 Application `ScheduledRun`（Run + Carrier），Postgres 在原 Ready 查询
  一并读取 Carrier；bootstrap 在当前 activeCtx 上恢复上下文后调用原 Processor。保留原
  Ready 接口给既有调用，不把观测字段加入 Domain、Requested JSON 或摘要。
- 首次 Carrier（包括 NULL）不因重放更新。Gateway Receipt 查询与 Worker Receipt 查询
  在原 SQL 中取回 context 并添加 Link，无新的观测查询事务、无全表回填。
- 专项测试覆盖实际 PG 的首次保存/重复接纳、NULL 与未采样 context、延迟提交失败、
  Outbox 重新 Claim、实际 JetStream 字节/Header/MsgID 去重、Consumer ACK 次序和调度
  取消上下文。Worker 另启动新的测试 OS 进程，从数据库恢复 Carrier 并生成关联 attempt；
  这是跨进程 Adapter gate，不冒称整个部署 Worker/Telegram 已重启验收。
- 旧 Telegram 服务没有迁移或切换二进制；新构建部署前须应用各自 schema 的新增迁移。
  旧二进制回退时保留新增可空列，不执行破坏性 down migration。

剩余 M3 正式 Session/terminal recovery，M4 Reply/Delivery 两处持久交接和外部发送，
M5 部署与真实 IM 查询验收。Memory 仍为 DEFERRED，生产 Tool/Parallel 能力不因埋点开放。

## 18. M3 实施记录（2026-09-08）

本阶段补充 Manifest resolve、Claim、Runtime prepare、Credential resolve、Session open/load、
candidate stage/verify、Completion/FindCompletion、正式 accepted commit 及实际 terminalize
的调用边界；复用原 Provider，不新增业务事件总线或更改调度/授权接口。

- `worker.session.load` 读取已接受 head；空 head 的逻辑 Load 正常记录零字节。
- `worker.session.stage` 只表示候选持久写入。原有不确定 Put 后的确定性 Load 核验增加
  `worker.session.verify` 子 Span；不增加 Put 重试、不改变原身份或候选摘要。
- `worker.session.commit` 位于 Completion 的真实事务路径，覆盖 Session commit 行和
  accepted head 修改直到事务返回。成功才标 accepted；提交前/服务端明确拒绝标 failed；
  已尝试提交但丢失响应的错误标 UNKNOWN。后续 FindCompletion 独立记录实际核验。
- 重放已有 Completion 不生成第二次 Session commit Span。失败 Run 不伪造 accepted head
  提交；`app.run.status` 单独记录 SUCCEEDED/FAILED，不混淆业务状态与事务操作结果。
- Terminalize 只有实际进入终态写入才创建 Span，普通空检查不生成。无当前有效 Run context
  时从已有 Run 行同一次加锁查询恢复；不同 ambient Trace 通过 Link 保留，不替换持久因果链。
- Claim 成功后将真实 Attempt ID 写到当前 attempt Span。正常 renew/fence 不逐次新增 Span。
- SDK overlay 的真实 Get/Create 和非 partial Append 有独立 Span。partial Append 不逐 token
  生成 Span；Runner 汇总 `app.session.overlay.appends` 与 `app.session.overlay.bytes`。
  数值只代表临时操作/快照大小，不是正式持久提交或新增容量政策。
- Memory 仍未初始化；不生成 Memory 占位 Span。Trace 导出仍丢弃正文、Span Event 和原始错误。

新增专项测试覆盖实际 PG 成功提交/延迟拒绝/回滚、accepted head 对照、重放无重复 commit、
终态恢复父级与无空轮询 Span、Runtime candidate 核验调用次数、SDK overlay 高频汇总。
实际 SDK + 本地 HTTP 模型的 OTLP protobuf 同时验证 overlay Span、聚合属性和正文过滤。
UNKNOWN 分类单测不冒称真实网络丢包，完整部署的不确定性及重启链继续在 M5 联合验证。

当前部署仍运行原二进制。剩余阶段为 M4 的 Reply/Delivery 关联与 M5 部署/真实 IM 查询；
整个目标保持活动，不能将本阶段专项通过当作 IM Tracing V1 全部交付。


## 19. M4 Reply / Delivery 实施记录（2026-09-08）

本节记录当前 Worker 工作树的源码和隔离测试，不表示当前 Telegram 二进制已经升级。

### 19.1 持久回复链

- Worker `0006_reply_trace.sql` 给 `execution_reply_outbox` 追加可空 Carrier。创建
  `create execution.reply-intent.v1` 的 PRODUCER Span，与 Session commit 同属
  `worker.run.complete` 的子节点；Carrier 和 Final/Completion 在原事务一起提交。
  正常 Final 与 FailAttempt 的失败 Final 都覆盖；SYSTEM_TERMINATION 仍为 NONE，
  没有虚构回复。重放已有 Completion 不重新创建 Span 或覆盖元数据。
- `TracedReply` 是 Application 技术包装，不把 Carrier 放入 Domain/事件 JSON。
  Relay 使用真实 PublishMsg，每次发布为 CLIENT Span 并 Link creation，Header 始终
  注入已保存的 creation context。原 payload/digest/Nats-Msg-Id、PubAck 后标记顺序不变。
- Gateway `0013_delivery_trace.sql` 给 `gateway_delivery_intents` 追加可空 Carrier。
  Reply Consumer 在原 broker 校验后创建 CONSUMER Span，保存当前 process context。
  Delivery 和 TransportReceipt 的原有两事务、ACK 时机、RawDigest 均不变；重复查询在
  原 SELECT 内读取 Carrier 并 Link 首次接纳，不新增查询依赖，也不产生 self-link。
- `ClaimTraced` 在原 Claim 事务读取同一 Intent Carrier，`TracedClaim` 不改变 Claim
  发送授权。Dispatcher 每个 part 恢复持久因果上下文，保留当前调度取消/deadline；
  缺失/非法 Carrier 形成诊断根，已有其他 ambient Span 只作 Link。
- `gateway.im.send` 只包住真实 SendFinal；Reserve/A2 失败没有 send Span。
  ACCEPTED、NOT_SENT、UNKNOWN 分别标注，不把成功写 observation 当成 Provider ACK。
  原授权、分段顺序、只重试 NOT_SENT、UNKNOWN 不重发与有界 evidence 持久化规则不变。
- `gateway.reply.verify → worker.reply.verify` 在已有专用 mTLS proof 客户端/服务端之间
  显式传播 W3C；Worker 先校验 Gateway 身份再继承 Header。未增加通用 HTTP 自动注入，
  没有将 Header 扩散到 Telegram/模型请求。

### 19.2 验证与容量

专项测试覆盖真实 PostgreSQL 的创建/延迟约束回滚/重放、失败 Final、独立连接池读取与
两段有序发送、NULL 历史行；Consumer 采样/未采样/坏 Header、ACK 与 replay；
真实 mTLS proof 请求传播及服务端身份边界；实际 SendFinal 的三种 certainty 和零调用分支。

真实 JetStream 测试断开连接后恢复 Relay，证明相同 MsgID 去重、相同 payload 和 creation
Carrier 保持不变。65536 字节的合法最大文本采用六倍 JSON 转义 fixture，payload 为
393483 字节，携带 512 字节 tracestate 后仍通过现有 1048576 帧容量；没有缩减 Manifest
max_output_tokens 或引入 Worker Token 限制。这是现有文本契约的容量测试，不是放宽其他
事件、附件或新协议的容量声明。

剩余 M5：最小 Collector/Tempo/Grafana 部署资产、真实进程/IM 纵向查询与故障注入、
重启和完整部署回滚验收。独立连接池/JetStream 重连不代替完整服务重启证据。
Memory 仍标 DEFERRED，生产 Manifest 能力不因 SDK Tool 测试扩大。

本阶段可复核入口：`/private/tmp/im-tracing-m4-3e20o_lc/VERIFICATION.txt`；
记录原始 hash、差异、测试命令/退出码和一次性副本回滚。活动工作树保留修改，旧手测部署
持续运行，尚未提交本轮 Tracing 改动。


## 20. M5 观测部署与真实进程 Gate（2026-09-08，阶段记录）

### 20.1 已落地并实际验证

- 独立 `compose.tracing.yaml` profile：Collector → Tempo → Grafana；镜像 tag/digest
  固定，loopback 端口、独立数据卷、24h Trace retention、只读 rootfs、显式内存和队列。
  本机 HTTP 和应用容器 TLS 模式分开；没有放宽应用的端点校验。
- TLS overlay 已经实际启动和请求验证：可信 TLS 1.3 成功，不可信 CA/TLS 1.2 拒绝。
  Grafana 匿名 datasource 请求 401，认证代理的实际 Trace 查询 200。
- `scripts/test-tracing-v1-stack.py` 创建全新观测项目，原生校验配置，运行真实 Collector /
  Tempo 查询和 TLS/Grafana gate，最后仅清理自己创建的资源。该 smoke 的应用 Span 为
  显式技术 fixture，不冒充实际 Runner 或真实 IM。
- `scripts/test-im-tracing-v1.py` 编译真实 Control/Gateway/Worker，以真实管理 HTTP
  发布 Manifest、独立 PG 角色/schema、TLS NATS、mTLS proof 和 SDK 完成两轮执行。
  每轮在 Tempo 查询到 29 个 Span，包含 callback、Run creation/publish/process、
  attempt/Runner/Agent/LLM、正式 Session、Reply creation/publish/process、proof/deliver/send。
  两轮共享正式 Session，但分别具有独立 Trace；第二轮 SDK 请求包含第一轮已接受内容。
- 所有实际 span traceId/parentSpanId 均核对；Worker Run、Reply Outbox、Delivery Intent
  三处持久 Carrier 对应实际 Span，而不是把属性写成同一 trace_id。Trace 中未发现正文
  canary 或该 fixture 的凭据。
- 已验证完成后的 Gateway SIGKILL/重启与 Reply 重投：Worker proof 停止，外部 fixture
  发送次数不增，首次 Carrier 不变；无 Header 重投的诊断 Trace Link 到原 process。
  为使这种诊断根可按 run_id 查询，Consumer 在原持久 receipt 成功后追加 app.run.id /
  app.intent.id / outcome；失败提交不标 ACCEPTED，ACK 顺序未改。

本节两个脚本不读取现有业务 `.env`，不操作正在手测的 Telegram receiver。
此处真实的是业务进程、协议与存储，模型/Telegram Bot API/HTTPS ingress 仍是外部 fixture。

### 20.2 M5 证据索引（最终状态）

- T06/T07/T10/T11 精确事务窗口见 §21；T16 旧构建回退见 §22。
- T12 发送矩阵见 §23；T09 Session 矩阵见 §24；T08 重投与重试见 §25。
- T13 生命周期见 §26；T14 完整进程导出故障见 §27。
- T15 真实 Telegram → DeepSeek → 正式 Session → Final 的可查询 Trace 见 §28。

IM Tracing V1 首版已经验收；Memory 仍为 DEFERRED。
本轮审计入口 `/private/tmp/im-tracing-m5-82jc3zwm/VERIFICATION.txt`，阶段源码副本回滚
仍与完整业务部署回滚区分。部署使用说明见 `deploy/compose/TRACING_V1.md`。


## 21. M5 精确恢复窗口（2026-09-08）

`scripts/test-im-tracing-v1.py --windows` 在原两轮纵向执行后增加四轮真实进程恢复。
故障模块 `scripts/worker-v1-joint/tracing_windows.py` 仅操作本次创建的 PostgreSQL
运行角色：临时撤销白名单内的表权限，独立 SQL/JetStream 查询确认事务已经提交，
实际 SIGKILL 对应服务，再恢复权限并启动新进程。所有权限在 finally 中恢复。
未增加生产暂停开关，未直接伪造或更新业务记录。

| Gate | 杀进程前的实际证据 | 重启后断言 |
| --- | --- | --- |
| T06 | Admission/Outbox 已提交、未发布、attempts=0、无 Worker Run | Gateway 新进程发布并完成原 Run |
| T07 | Intake Receipt 已提交、broker ACK floor 已推进、无 Attempt/模型调用 | Worker 新进程 Claim 并执行一次 |
| T10 | Completion/Reply 已提交、未发布、无 Delivery | Worker 新进程发布同一 Reply，Runner 不重跑 |
| T11 | Delivery/parts 已提交、第二事务 Receipt 为空、part=PENDING | Gateway 新进程恢复，Receipt=ACCEPTED 并回复 |

四轮均核对新进程启动时间之后的实际 Span、持久 Carrier 不变、Reply id/digest/bytes
不变、每个 Run 只有一次模型调用和一个 Attempt/Completion/Reply。
查询先确认已结束的持久 handoff Span 导出后才杀进程；强杀可能丢失其他尚未结束或
尚未导出的父 Span，测试显式记录 missing_parent_spans，不把这些缺口当成完整父树。
普通两轮仍要求所有父 Span 存在；故障轮仍要求同一 Trace、核心阶段及持久 Span ID 对齐。

审计入口：`/private/tmp/im-tracing-recovery-4s_pnkc6/VERIFICATION.txt`。
本组外部模型和 Telegram 仍为 fixture。T08/T09/T12/T13/T14、T15 真实外部链路及
T16 旧二进制兼容不由本组证明，另见 §22；本组 PASS 不扩展 Memory/Tool 能力。


## 22. M5 旧二进制实际回退兼容（2026-09-08）

`scripts/test-im-tracing-v1.py` 新增成对参数 `--rollback-source`、`--rollback-ref`，
通过只读 `git archive` 获取显式旧提交，在私有目录构建带 race 检测的 Worker/Gateway。
记录提交、源码归档 hash、构建命令、二进制 hash 和实际启动 PID；不 checkout/reset
任何工作树，也不调用 down migration。实现位于 `scripts/worker-v1-joint/tracing_rollback.py`。

本轮旧版本固定为 `fd1f790bf4f779216ed44373bd2356aa3ce7e186`：

1. 旧 Worker `--check-config` 对含 tracing 配置返回 exit 1，原始错误为
   `worker configuration fields are invalid`；仅移除 tracing 字段后返回 exit 0、
   `WORKER_CONFIG=PASS`。Gate 区分预期字段拒绝与其他启动错误。
2. 新版先完成真实 Runner/Session/Completion，故障 fixture 暂停 Reply 发布。
   随后停止新版进程，运行旧版：旧 Worker 从保留新列的同一 Outbox 读取并发布
   新版真实 payload；旧 Gateway 正常解码、摘要验证、接纳和发送。Reply ID/digest/bytes
   与首次 Carrier 不变，模型只调用一次，正式 Session commit 保留。
3. 旧版本完整处理下一条文本，创建新的正式 Session commit 和 Final；新行 Carrier
   为 NULL，仍读到新版已接受的历史。相同新版 Reply bytes 再次投递给旧 Gateway，
   durable receipt 接纳且发送次数不增。
4. 最后切回新版并恢复 tracing 配置，处理新请求；正式历史包含旧版接受内容，
   实际 Tempo Trace 通过完整阶段与持久 Carrier 校验。
5. 四个持久化点共 8 个新增可空列在旧版运行前后和再次升级后保持不变；不删除 Run、
   Session、Reply 或 broker 数据。全部动作只作用于本次创建的隔离实例。

实际进程结果：`/private/tmp/im-tracing-binary-rollback-_os1f7d7/process-v3/tracing-binary-rollback.json`。
审计与源码副本回滚：`/private/tmp/im-tracing-binary-rollback-_os1f7d7/VERIFICATION.txt`。
`--windows` 与旧版回退参数可组合执行。外部模型/Telegram 仍为 fixture；本项为 T16
业务进程回退证据，不替代 T15 外部真实链路或剩余 T08/T09/T12/T13/T14 故障矩阵。


## 23. M5 分片、重试与不确定发送的实际 Trace（2026-09-08）

`scripts/test-im-tracing-v1.py --delivery-matrix` 调用新增 `tracing_delivery.py`，
通过真实 SDK HTTP/SSE、Gateway 持久 ledger 和 Tempo 导出交叉验证 T12：

- **UNKNOWN**：复用 `delivery_scenarios.run_delivery`。外部 Telegram fixture
  已接收，故意丢失 HTTP 响应；Gateway 落 UNKNOWN。实际 `gateway.im.send` 标记
  UNKNOWN；SIGKILL/重启、原 Reply 重投、Worker proof 暂离线后仍只有一次发送。
  后续请求继续使用 Worker 已提交 Session，Transport ACCEPTED 不冒充 Provider ACK。
- **多分片**：外部模型通过实际 SSE 产生多字节长 Final。SDK/Worker/正式 Session
  和 Gateway 分片均走原实现；每个 part 有独立 deliver/send Span，send 的真实 parent
  指向对应 deliver，part.index 与实际发送顺序一致，拼接文本等于原 Final。
- **明确未发送**：仅在 fixture 的 getMe 返回一次 typed 429，故障发生在 Reserve
  准备阶段。数据库记录 NOT_SENT/preparation_attempts=1，Trace 有 NOT_SENT deliver
  而没有虚构的 send child；原有有界准备重试后只有一次实际 SendFinal/模型执行。
- **发送被拒绝**：sendMessage 返回 typed 429，按当前契约记录
  REJECTED/rate_limited，而非 NOT_SENT。Trace outcome 同步，外部接收数为零；
  多个发送扫描周期后 ledger 不变，不扩展为新的自动重试策略。

`gateway_fixture.py` 增加一次性 typed 429 注入，限定 getMe 或精确 chat/text 的
sendMessage；拒绝发生在外部消息接纳前。不修改生产重试/分片/确认策略。

实际 Tempo HTTP 200 可能仅包含先到的部分批次。Gate 保持完整分片数量和 parent
断言，轮询直到最后一批到达；不因首个 200 提前判 PASS，也不删减预期 Span 数。
新增单元测试覆盖该部分响应，以及错误 certainty、错误父关系、重复 Span ID 和
429 fixture 的一次性/零接收语义。

本轮实际结果入口：`/private/tmp/im-tracing-delivery-kpi67x8i/process-v4/tracing-delivery-matrix.json`；
审计：`/private/tmp/im-tracing-delivery-kpi67x8i/VERIFICATION.txt`。
T08/T09/T13/T14 和 T15 真实外部联合链路继续单独验收；Memory 仍为 DEFERRED。


本组还发现并修复了生产过滤器遗漏：`app.outcome=REJECTED` 原先在源端白名单中
被丢弃，虽然数据库已有正确结果，Tempo 中仍缺少该 outcome。现补齐这个固定枚举，
不放行任意 Provider 文字/错误。新增 Go 回归在修改前明确失败、修改后通过；实际
429 进程 Gate 再次查询导出的 REJECTED，而不是只验证本地 Span 属性。


## 24. M5 Session Stage/Complete 与响应丢失（2026-09-08）

`--session-matrix` 在真实 Control 发布前安装私有 PostgreSQL 协议 relay，固定发布
Session destination，然后运行三项业务/Trace 交叉验证。无生产接口或业务行改写：

1. **Stage INSERT 拒绝**：先确认真实 SDK 已进入模型调用，再临时撤销私有
   session_runtime 对候选表的 INSERT。实际 Stage/精确读回失败后，候选、Completion、
   Reply、正式 Session commit 均不存在，accepted_ref/digest 保持种子版本。Trace
   没有 accepted commit；恢复权限后原重试策略完成，唯一 accepted commit 对应真实 head。
2. **Complete 事务回滚**：同样在真实模型调用期间撤销私有 worker_runtime 对
   execution_session_commits 的 INSERT。Stage 已产生候选，但 Complete 的事务失败，
   Completion/Reply/正式 commit 都回滚，accepted head 不前移。Trace 中
   worker.session.commit=failed，而 Stage=candidate；恢复后只有一次 accepted commit。
   未被接受的旧候选不冒充正式历史，后续请求读取的是最终被接受的候选。
3. **候选提交响应丢失**：复用真实 PG 协议故障 relay。它观察到 INSERT 0 1 和
   ReadyForQuery I 后暂不回传；独立连接确认候选已提交、accepted head 未动，才断开
   原连接。Worker 在新连接按固定 candidate identity 读回，然后正式提交。实际
   worker.session.verify 是该 Stage 的 child；只有一次 SDK/Attempt/Candidate/Completion/Final，
   Stage 最终标 candidate、正式 commit 标 accepted。

三项均在 Tempo 查询原 Trace，核对唯一 accepted commit、失败阶段与持久 Carrier，
并通过 follower 模型请求验证已接受历史。权限恢复放在 finally；只扩展故障 fixture 的
精确权限白名单，不改变 Session 存储协议、候选可见性或生产重试策略。

源码入口 `scripts/worker-v1-joint/tracing_session.py`；使用入口
`scripts/test-im-tracing-v1.py --session-matrix`。可与 delivery-matrix、windows、旧版
回退参数合跑。实际结果：
`/private/tmp/im-tracing-session-4jdcgmn3/process-v2/tracing-session-matrix.json`；
审计：`/private/tmp/im-tracing-session-4jdcgmn3/VERIFICATION.txt`。

剩余 T08/T13/T14 和 T15 真实外部联合链路单独验收。Memory 仍为 DEFERRED。


## 25. M5 原消息重投与模型临时失败重试（2026-09-08）

新增 `--retry-matrix`，实现于 `scripts/worker-v1-joint/tracing_retry.py`：

- **真实 broker redelivery**：私有 NATS 只撤销 Worker 的 Run ACK 发布权限；独立
  server log 确认对确切 stream sequence 的 ACK 拒绝。Run/Receipt/Completion 已提交，
  原消息仍保留，Gateway 没有重新发布。等待首次持久 process Span 实际导出后，
  SIGKILL 原 Worker，恢复 ACK 权限并启动另一 Worker。原 sequence 消费后正常 ACK；
  Tempo 至少两个不同 process Span，parent 都是原 creation Span。首次持久 Carrier
  不变，Receipt/Attempt/Candidate/Completion/Final 均不重复。
- **同一 webhook 重放**：复用原始 webhook bytes；认证拒绝负例与合法重复接纳
  同时验证，Admission 数量仍为 1，不触发额外执行。
- **模型暂时不可用**：仅对精确测试输入注入一次真实 HTTP 503，返回正文 canary；
  下一次调用正常 SSE。原配置禁用 SDK 隐式 HTTP 重试，Execution 按既有预算重试：
  第一 Attempt=FAILED/DEPENDENCY_UNAVAILABLE，第二 Attempt=SUCCEEDED。
  两次模型调用对应不同 Attempt/Runner/LLM Span，错误模型 Span 存在；Span 上
  app.attempt.id 与两条实际 Attempt 对齐。首次 Carrier 不变，仅一个 Candidate、
  Completion、Reply，原始错误正文没有进入实际导出的 Trace。

模型 fixture 的一次性故障只接受 500/503，不改生产模型配置或 Retry Policy。
原 intake helper 新增可选的导出观察 hook，默认调用方式不变；hook 不写业务状态。

本轮还修复测试中的异步观察竞争：客户端可能已收到 TLS proxy 的最后字节，而 proxy
尚未写入 post-flush 记录。测试现在等待 downstream_status 出现后核对精确字节数，
未放宽长度或成功断言。fixture 单元测试同时覆盖错误凭据不消耗一次性模型故障。

实际结果：`/private/tmp/im-tracing-retry-hsnkv2de/process-v3/tracing-retry-matrix.json`；
审计：`/private/tmp/im-tracing-retry-hsnkv2de/VERIFICATION.txt`。
本组可与 Session/Delivery/windows/旧版回退矩阵组合执行；外部模型与 Telegram 仍为
fixture。剩余 T13、T14、T15 单独验收，Memory 保持 DEFERRED。


## 26. M5 流式取消、存活进程失租与 deadline（2026-09-08）

新增 `--lifecycle-matrix`，实现在 `scripts/worker-v1-joint/tracing_lifecycle.py`。
所有输入来自真实 Gateway，故障前均确认 SDK 已收到并 flush 非终止 SSE，且没有候选
或 Completion。测试没有改写业务时间戳、租约、状态或 Manifest：

- **正常停机取消**：真实 Worker 收到 SIGTERM，按原 drain 流程退出，exit=0，
  总停机耗时有界。旧 Attempt 的真实 Span 以 cancelled 结束；旧流的未接受文本
  没有进入候选/Completion。新 Worker 恢复原 Run 后，仅新 Attempt 正式提交并回复。
  这是已有 Worker 停机带来的执行取消，不是新增逐 Run 取消 API。
- **存活旧进程被隔离**：对已进入流式调用的 Worker SIGSTOP，让真实数据库时钟
  推进到 lease 过期。另一 Worker 以新的 generation/lease_epoch 接管，并进入实际 SDK。
  原 Worker 仍存活，SIGCONT 后 renewal/fence 检查使旧 Span 以 fenced 结束，旧 Attempt
  不产生候选。释放新流后只有新 Attempt 提交，follower 不包含旧流的未接受文本。
- **流式 deadline**：两个 Worker 使用显式测试 MaxRunAge=12s；不修改 Manifest。
  流一直停在非终止 SSE，实际 deadline 到达后由原系统终止流程生成唯一
  SYSTEM_TERMINATION / DEADLINE_EXPIRED / Reply NONE，accepted head 不变。
  旧 Attempt Span 以 deadline 结束，Trace 有唯一 terminalize，没有 Stage、正式
  Session commit 或 Reply/IM send 占位 Span。follower 继续读取种子正式历史。

旧进程恢复前，fixture 将已暂停的 proof 后端移出路由并关闭代理拥有的旧 TCP stream，
避免 Control 的 keep-alive 继续落到暂停端，将失租测试混成额外的凭据解析超时。
这是私有 L4 relay 的 EOF/连接恢复，不伪造授权响应，也不修改生产发现/路由；新增
`ProofSwitch.disconnect()` 的测试验证旧流断开后监听器和后端仍可用于新连接。
SIGCONT 和私有进程恢复均位于 finally 路径，已有 Telegram 手测部署不受影响。

实际结果：`/private/tmp/im-tracing-lifecycle-bao25pyx/process-v3/tracing-lifecycle-matrix.json`；
审计：`/private/tmp/im-tracing-lifecycle-bao25pyx/VERIFICATION.txt`。
此组与 Retry/Session/Delivery/windows/旧版回退矩阵组合执行。
剩余 T14 导出故障与 T15 真实外部联合链路，Memory 仍为 DEFERRED。


## 27. M5 导出故障、队列饱和与 stdout 背压（2026-09-08）

新增 `--export-matrix`，由 `scripts/worker-v1-joint/tracing_export.py` 驱动
隔离的真实 Worker/Gateway/PostgreSQL/NATS 进程；不修改现有手测部署。

- 私有 TCP 端口已 bind 但没有 listen，验证实际 OTLP 连接拒绝时业务完成。
- 私有 OTLP 接收器读取实际 protobuf 后返回 HTTP 503；只记录批次 SHA256、
  字节数、响应状态与 canary 检查结果，不存储原始导出正文。重复批次摘要断言
  验证本次运行没有自动重发同一失败批次。
- 慢接收器保持请求不响应，显式测试 queue=1/batch=1/export_timeout=10s；
  同时给两个服务连接预先填满的阻塞 stdout pipe，期间从不读取 pipe。
  三轮业务均只有一次模型调用、一个 Candidate、Completion 和 Reply。
  结束后再次验证 pipe 未被排空，且在 pipe 保持打开、满载时 SIGTERM 正常退出。
- sampled 已完成 Span 下界与已接收批次、有限缓冲容量的比较提供丢弃证据，
  不把 Finished−Exported 解释为精确 drop 计数。保守缓冲上界包含两个进程各自
  queue、batch 和可能正在交接的 Span；外部健康指标及精确 drop 统计仍后置。
- 恢复原私有 tracing 配置后，下一轮重新在 Tempo 校验完整父子链和正式业务结果。
  fixture 的异常清理也先停子进程、再关闭 stdout pipe，避免 SIGPIPE 改变测试结果。

配套 Go 测试 `TestExportOutageAndQueueRemainBounded` 验证 1001 个 Span 的
非阻塞结束、失败计数与有界 Shutdown；Worker 测试验证阻塞日志写入时丢弃而非
阻塞执行。Python 测试覆盖满管道、503 摘要、敏感正文检测及异常清理顺序。

实际组合结果：`/private/tmp/im-tracing-export-642x0j8z/process-v2/tracing-export-matrix.json`；
审计：`/private/tmp/im-tracing-export-642x0j8z/VERIFICATION.txt`。
T14 完成后仅 T15 真实外部模型与 Telegram Trace 联合验收待执行；Memory 仍为 DEFERRED。


## 28. M5 真实 Telegram 联合验收与最终审计（2026-09-08）

在原手测数据库、Control、Ingress、HTTPS webhook 和账号绑定上，替换 Worker/Gateway
为本工作树当前代码的 race 构建，显式开启 OTLP Traces。原实例没有正在执行的 Run；
两进程先正常 drain，再由独立守护进程管理替换后的子进程。原资源控制器继续持有
Control/Ingress/数据库；守护进程检测原控制器退出或撤回 READY 后会停止自己的子进程。
`TRACING_UPGRADE.json` 标明替代子进程归属，原 HEARTBEAT 内的旧句柄不作为新版状态。
升级没有清库、改 Manifest、修改 webhook 地址或启动第二个 Telegram receiver。

真实 Telegram 客户端发送 `tracelivealpha please reply tracingok only`，收到 `tracingok`。
实际 DeepSeek `deepseek-v4-flash` 返回 HTTP 200；原 SDK 使用正式历史执行，唯一 Candidate
内容 SHA256 与 Completion 和 accepted Session head 一致。Delivery ACCEPTED 的真实
Telegram ProviderMessageID 为 `28`，输入 message ID 为 `27`；最终文本与模型结果一致。

- Run：`4a34369a10c5f4a2bbaa073e210a47b7`。
- Trace：`6edda2f2a31bd94ba186a9a5fdcdc6dc`，29 个 Span，正常父链无缺失。
- 三处运行中持久 Carrier 指向实际对应 Span；Trace 包含 callback、Runner/LLM、正式
  Session load/stage/commit、Completion、Reply relay、proof、Delivery/send。
- Tempo 按 run_id 搜索命中该 Trace；Grafana 使用已配置账号 `trace-admin` 的认证
  datasource proxy 返回 200。首次误用账号 admin 的请求返回 401，未改变认证配置。
- `scripts/worker-v1-joint/live_trace_verify.py` 只读实际 ingress/provider evidence 与 PG。
  它不合成输入、调用模型或修补业务行；输出仅保存正文摘要和必要关联事实。
  `--secrets-env` 显式读取私有 dotenv，用于检查实际导出 Trace 不包含凭据，值不写结果。
- Telegram UI 中实际显示的回复已核对。该 UI 核对不把 Delivery ACCEPTED 解释为通用已读状态。

最终审计还补强 T17：真实 SDK Tool 的成功/错误/取消路径，以及显式并发调用必须沿实际 parentSpanId
回到 Runner，不能仅凭相同 trace_id 判定；新增孤儿、循环和跨 Trace 父节点负例。
并发测试用实际 SDK 的 WithEnableParallelTools(true)，通过屏障证明两个调用同时在执行，
然后核对两个不同 sibling Span 的时间区间重叠与共同 Runner 祖先。
生产 Tool 和 Memory 能力没有因此开放。

真实结果：`/private/tmp/im-tracing-live-l1ndby4p/live-v2/tracing-live-result.json`。
最终证据及逐项判定：[IM Tracing V1 完成审计](im-runtime-tracing-v1-acceptance.md)。
源码尚未提交/推送；本机手测服务保持运行，不宣称其他工作树已同步或远端已合并。
