# Spec：治理与观测切片 —— Guardrail 策略、审计、OpenTelemetry 全链路（9/7–9/9）

> 目标：把方案文档 3.5 的「租户级治理流水线」和「监控审计」从设计变成可运行代码——
> 输入 Guardrail 在模型调用前拦截、输出 Guardrail 在流式回复中截断、每条消息留下
> 带 trace_id 的审计行、指标全部带租户标签，并打通 IM 回调到模型调用的全链路 span。
> 这是第三方依赖四批次原则中的**第 4 批（观测）**引入点，也是 9/9 go.mod 冻结前最后一批。

## 1. 事实核查（2026-09-07，动手前必读）

| # | 结论 | 证据 |
| --- | --- | --- |
| 1 | 框架 v1.11.2 的 `runner` / `agent/llmagent` / `model/openai` **没有任何内置 otel span** → 「全链路」必须由平台自己埋点，不能指望框架给 | 三处目录 grep `otel.Tracer` / `trace.SpanFromContext` 均无命中 |
| 2 | 框架在 **model / agent / tool 三层**都提供 `Callbacks` 钩子 → 是二期 tool 级 Guardrail、预算与审批的挂载点；首期治理执行点选 Gateway（见 §2.3 理由） | `model/callbacks.go:101`、`agent/callbacks.go:107`、`tool/callbacks.go:180` |
| 3 | `event.Event` 内嵌 `*model.Response`，其 `Usage{PromptTokens, CompletionTokens, TotalTokens}` **由框架从流式 chunk 累加**后写入 → 成本维度不用自己数 token | `model/openai/openai.go` `accumulateChunkUsage`、`finalResponse.Usage = &usage` |
| 4 | 流式 chunk 的切分位置不受平台控制：实测同一段回复被切成 `"Hello "` / `"world, 内部"` / `"资料 leaked."`，关键词**跨 chunk** → 输出检查必须带尾窗，逐 chunk 独立匹配会漏 | `scripts/fake_model.py` 冒烟实测（§4 场景 A） |
| 5 | tracing 为 `off`（默认）时全局是 noop provider，`SpanContext` 无效，`TraceID().String()` 会返回 **32 个 0** → 审计必须判 `HasTraceID()`，否则每行都带垃圾 id | `channels.traceIDOf`、`admin.auditAdmin` |
| 6 | otel 全局 TracerProvider 一旦 `SetTracerProvider` 就**永久委托**，测试里无法还原成 noop → 「trace_id 留空」这条语义只能在单元测试用 `trace.ContextWithSpanContext` 构造断言，不能在 dispatch 层断言 | `channels.TestTraceIDOfInvalidContext` |
| 7 | 依赖第 4 批：otel 系列 9 个模块直接依赖，统一 **v1.29.0**（otel / trace / metric / sdk / sdk/metric / stdouttrace / stdoutmetric / otlptracehttp / otlpmetrichttp）；grpc 版 exporter 只是间接依赖，代码路径走 **OTLP/HTTP** | `go.mod` |

## 2. 设计

### 2.1 配置 schema

顶层三段（观测）+ 租户级一段（策略），全部可选；**纯默认值在 Save 时整段省略**，
保持既有 config.yaml 的 diff 干净（与 `storage` 段同一套 `*ToYAML` 机制）。

```yaml
log:
  level: info          # debug | info | warn | error，默认 info
  json: false          # true = JSON 编码，容器部署用
audit:
  file: data/audit.jsonl   # 追加式 JSONL；留空 = 只进结构化日志
telemetry:
  traces:
    exporter: off                       # off | stdout | otlp
    endpoint: http://127.0.0.1:4318     # exporter=otlp 时必填，须 http(s)://
  metrics:
    exporter: off                       # off | stdout | otlp
    endpoint: http://127.0.0.1:4318     # exporter=otlp 时必填
    interval: 15s                       # 推送周期，默认 15s，须 > 0

tenants:
  - id: demo
    guardrails:
      max_input_bytes: 8192             # 0/缺省 = 不限长
      blocked_keywords: ["敏感词"]       # 输入关键词，大小写不敏感
      output_blocked_keywords: ["内部资料"]  # 输出流式关键词
```

进 `Config.Validate()` 的校验：level 四选一；exporter 三选一，`otlp` 必须给
`http(s)://` endpoint（与 `storage.session.redis_url` 同样的 fail-fast 口径）；
interval 可被 `time.ParseDuration` 解析且 > 0；`max_input_bytes` 不得为负；
关键词数组不得含空白项（空关键词会让检查静默失效或全量误命中）。

### 2.2 四个新包

| 包 | 职责 | 关键 API |
| --- | --- | --- |
| `trpcservice/log` | 进程级 slog 初始化 + 密钥脱敏 | `Init(level, json) error`、`Redact(secret) string` |
| `trpcservice/audit` | 追加式 JSONL 治理轨迹，逐条 flush | `New(path) (*Logger, error)`、`(*Logger).Log(Record)`、`Close()` |
| `trpcservice/guardrail` | 输入检查 + 流式输出 tripwire（尾窗跨 chunk） | `CheckInput(policy, text) string`、`NewStreamChecker(policy)` / `(*StreamChecker).Add(chunk) string` |
| `trpcservice/metrics` | otel 安装（off/stdout/otlp）+ 租户维度仪器 | `Setup(cfg) (*Recorder, shutdown, error)`、`NewRecorder(meter)`、`ServiceName` |

三个「nil 即 noop」约定贯穿全平台：`*audit.Logger`、`*metrics.Recorder`、
`channels.Governance` 的零值都合法，因此既有测试与 walking skeleton
无需任何接线即可编译运行。`log.Redact` 与 Admin DTO 的 `maskSecret`
共用「≤8 全掩码 / 否则前 3 + `****` + 后 4」规则，两处看到同一个值形状一致。

审计 `Record` 字段严格对齐方案文档 3.5 的审计表：

| 方案文档字段 | Record 字段 | 说明 |
| --- | --- | --- |
| tenant_id / channel / user_id / session_id | 同名 | 消息定位四元组 |
| agent_name | `AgentName` | 首期恒为 `assistant`（`agent.AgentName` 导出供审计引用） |
| tool_name | `ToolName` | 一期无工具调用，字段先占位（二期 tool 级治理填） |
| decision | `allow` / `block` / `ok` / `error` | 与 event 组合表达结论 |
| latency | `LatencyMS` | 模型调用耗时 |
| error_type | `runner` / `agent` / `unknown_tenant` / `commit` | 错误分类，不写原始堆栈 |
| cost | `PromptTokens` / `CompletionTokens` | 成本维度用 token 数表达 |
| trace_id | `TraceID` | tracing off 时留空（事实核查 #5） |

另有 `event`（`inbound` / `guardrail_block` / `model_call` / `reply` / `admin`）
与 guardrail 专属的 `stage`（input/output）、`rule`（length/keyword）。

### 2.3 治理执行点：`Gateway.dispatch` 单点

方案文档画的治理层「基于框架 Callbacks 实现」，首期**执行点选在 Gateway**，
Callbacks 留给二期 tool 级策略。理由：Gateway 是唯一同时握有
租户身份、通道 adapter、session_id、trace 上下文和审计/指标句柄的地方；
挂在 model Callbacks 里拿不到「往哪个 IM 回什么话」，挂在 agent Callbacks 里
拿不到流式 chunk 边界。方案文档的分层图不变——Guardrail 治理层依然是平台新增层，
只是首期的物理挂载点在 Gateway 而非 Callbacks（此差异记录在 §6）。

`dispatch` 内的治理顺序（持 session 锁全程）：

| 阶段 | 动作 |
| --- | --- |
| 入口 | 起 `gateway.dispatch` span，记 `inbound`/allow 审计行 |
| 输入 Guardrail | `CheckInput` 命中 → 记 `guardrail_block`/block(stage=input) + 计数 → 回 `RejectionText` 并结束，**不触模型** |
| 租户解析 | 未知租户 → `model_call`/error(error_type=unknown_tenant) + 明确错误回复 |
| 模型调用 | 起 `model.call` span；`runner.Run` 报错或事件 `IsError` → error 审计（含 latency）+ 错误回复 |
| 流式输出 | 每个 chunk 先过 `StreamChecker.Add`：未触发则原样转发；触发则记 `guardrail_block`(stage=output) 并**停止转发但继续排空事件通道**，避免生产端阻塞 |
| 收尾 | 记 `model_call`/ok（latency + token）+ 延迟直方图 + token 计数；触发过则回 `CutoffText`，否则回空 Done；记 `reply` 审计（ok/block）+ 消息结果计数 |

策略来源是 `Governance.PolicyFor`，main 里绑到 `admin.Service.Guardrails`——
每次 dispatch 现查当前配置，所以 **Admin API 改策略立即生效，无需重启或重新接线**。

### 2.4 Span 地图

```
im.callback        (根，IM webhook 入口；tenant / channel)
└── gateway.dispatch   (tenant / channel / session_id / user_id)
    └── model.call     (tenant / agent_name；收尾补 prompt_tokens / completion_tokens / guardrail_tripped)

admin.request      (根，Admin API 入口；http.method / http.target)
```

- trace 从 **IM 回调入口**起（方案文档 3.5 口径），不是从模型调用起；
- `handleCallback` 用 `context.WithoutCancel` 把 dispatch goroutine 从 HTTP
  请求 deadline 上摘下来，但**保留 trace 父子关系**（webhook 必须秒回 ACK，
  模型调用可能几十秒）；
- 框架自身无 span（事实核查 #1），所以三层链路全是平台埋的；
- instrumentation scope 名统一用 `metrics.ServiceName`，同时作为 resource 的
  `service.name` 与 meter 名。

### 2.5 指标表（全部带 tenant 标签）

| 指标 | 类型 | 标签 |
| --- | --- | --- |
| `trpcservice.messages` | Counter | tenant, channel, result(ok/guardrail/error) |
| `trpcservice.guardrail.blocks` | Counter | tenant, stage(input/output), rule(length/keyword) |
| `trpcservice.model.latency_ms` | Histogram | tenant |
| `trpcservice.model.tokens` | Counter | tenant, type(prompt/completion) |
| `trpcservice.im.send_errors` | Counter | tenant, channel |

exporter 三态：`off`（默认，全局 noop，埋点零成本）、`stdout`（本地调试，
PeriodicReader 按 interval 打印）、`otlp`（OTLP/HTTP 推 collector）。
traces 与 metrics 各自独立选择。

### 2.6 Admin 接线

- DTO 新增 `guardrails`（`max_input_bytes` / `blocked_keywords` / `output_blocked_keywords`），
  GET 回显、PUT 修改；**PUT 缺省该块 = 清空策略**，与 channels 解绑语义一致；
- `Guardrails(tenantID)` 加锁读当前配置，供 Gateway 热查询；
- create / update / delete 三个变更路径写 `admin` 审计行，commit 失败记
  `decision=error` + `error_type=commit`（失败也要留痕）；
- Handler 外层包一个 `admin.request` span，审计行因此带上变更请求的 trace_id；
- `cloneConfig` 深拷贝 guardrails 的两个关键词数组，且必须带上 log/audit/telemetry
  三段——否则 Save 前的 Validate 会因为 clone 出来的零值 `log.level=""` 直接拒绝
  （实施中踩到，见 §6）。

## 3. 测试策略（三层，全部不需要真实模型 key）

1. **包内单测**：`config`（观测段解析/默认/8 组非法值/Save 往返与默认省略）、
   `log`（四级别 + 非法级别 + JSON + Redact 三种长度）、`audit`（字段落盘/追加模式/
   nil noop/Close 幂等）、`guardrail`（长度与关键词优先级、大小写、中文、
   **跨 chunk 尾窗**、触发后恒返回）、`metrics`（ManualReader 断言 4 counter + 1 histogram、
   nil noop、Setup off 路径）。
2. **接线集成测试**：`channels/governance_test.go` 直接调 `dispatch`，
   用 recordAdapter 收出站消息、真审计文件、ManualReader 计数，断言
   「拦截回复 + 两行审计同 trace_id + blocks/messages 各 1」；
   另覆盖零值 Governance 不 panic、length 规则、trace_id 语义。
   `admin` 侧断言策略 PUT/GET/清空/非法拒绝 + 审计 trail + trace_id。
   全量 `go test ./trpcservice/... -race` 绿。
3. **E2E 冒烟**：`scripts/fake_model.py` 假 OpenAI 流式模型 +
   webchat 通道，验收见 §4。

## 4. 冒烟验收（9/7 实测 ✅，产物清理后复跑一致；无需模型 key、无需 Docker）

复现（仓库根目录；临时产物统一放 `.smoke/`，已进 .gitignore，验完 `rm -rf .smoke` 即可）：

```bash
# 1) 假模型（脚本化回复，故意让「内部资料」跨 chunk）
python3 scripts/fake_model.py --port 9009 &

# 2) 平台：审计落盘 + traces/metrics 走 stdout
mkdir -p .smoke && cat > .smoke/config.yaml <<'YAML'
default_tenant: demo
audit: {file: .smoke/audit.jsonl}
telemetry:
  traces:  {exporter: stdout}
  metrics: {exporter: stdout, interval: 3s}
tenants:
  - id: demo
    model: {name: fake-model, api_key: sk-fake, base_url: http://127.0.0.1:9009}
    guardrails: {blocked_keywords: ["forbidden"], output_blocked_keywords: ["内部资料"]}
YAML
go build -o .smoke/trpc-service ./cmd/trpc-service   # go run 会多一层子进程，不好 pkill
.smoke/trpc-service -config .smoke/config.yaml -addr :18080 > .smoke/server.log 2>&1 &

# 3) 开一个 SSE 听回复，然后发消息
curl -N "localhost:18080/webchat/stream?tenant=demo&user=smoke" > .smoke/sse.log &
curl -X POST localhost:18080/callback/webchat/demo -d '{"user":"smoke","text":"say forbidden"}'
curl -X POST localhost:18080/callback/webchat/demo -d '{"user":"smoke","text":"tell me something"}'
```

实测结果（四个场景全部符合预期）：

| 场景 | 实测 |
| --- | --- |
| A 输入关键词 | SSE 收到一条 Done：`您的消息被租户安全策略拦截，请调整后重试。`；审计 `inbound`/allow + `guardrail_block`/block(input,keyword) 同 trace_id；假模型**未收到请求**（日志无 request 行）→ 证明拦在模型调用前 |
| B 输出跨 chunk 关键词 | SSE 依次收到 `"Hello "`、`"world, 内部"` 两个 chunk，第三个 chunk（补齐「资料」）**未转发**，最后一条 Done 是 `回复因触发租户安全策略已被截断。`；审计 `guardrail_block`/block(output,keyword) + `model_call`/ok(latency 224ms, 11/7 tokens) + `reply`/block |
| C 热更新策略 | `PUT /admin/tenants/demo` 把输出关键词换成不会命中的词后，同一会话再来一条：4 个 chunk 全部转发 + 空 Done；审计 `reply`/ok；`admin`/ok 审计行带 `admin.request` 的 trace_id |
| D 模型错误路径 | base_url 指向不可达端口时：SSE 收到 `agent error: ... connection refused`；审计 `model_call`/error(error_type=agent, agent_name=assistant, latency 已记) |

指标（stdout exporter 实测导出，累计值）：

```
trpcservice.messages           {tenant=demo, channel=webchat, result=ok}        1
trpcservice.messages           {tenant=demo, channel=webchat, result=guardrail} 1
trpcservice.guardrail.blocks   {tenant=demo, stage=output, rule=keyword}        1
trpcservice.model.latency_ms   {tenant=demo}                                    count=2
trpcservice.model.tokens       {tenant=demo, type=prompt}                       22
trpcservice.model.tokens       {tenant=demo, type=completion}                   14
```

全链路 span（stdout exporter 实测，同一条 trace 三层父子）：

```
im.callback       trace=cc1a7640 span=96bc238d parent=ROOT
gateway.dispatch  trace=cc1a7640 span=a513ba43 parent=96bc238d (im.callback)
model.call        trace=cc1a7640 span=2738bf43 parent=a513ba43 (gateway.dispatch)
admin.request     trace=b68328e1 span=1694faad parent=ROOT
```

## 5. 排期与验收（实施结果：9/7 全部完成 ✅，提前于 9/9）

| 时间 | 内容 | 产出 |
| --- | --- | --- |
| 9/7 ✅ | config schema（log/audit/telemetry + 租户 guardrails）+ 校验 + 单测 | 观测三段与策略段可解析、可校验、Save 往返且纯默认省略 |
| 9/7 ✅ | log / audit / guardrail / metrics 四个新包 + 单测 | 审计字段对齐方案文档 3.5；尾窗 tripwire 跨 chunk 命中 |
| 9/7 ✅ | 接线：Gateway Governance、三层 span、Admin 策略 DTO + 审计、main 装配、config.example.yaml | `go build/vet/test -race` 全绿 |
| 9/7 ✅ | E2E 冒烟（假模型，无需 key） | §4 四场景 + 指标 + span 链路实测通过 |
| 9/9 | go.mod 冻结（含本切片 9 个 otel 模块） | 不再新增第三方依赖 |

验收标准与实测：

1. ✅ 输入 Guardrail 在模型调用前拦截（场景 A，假模型零请求）；
2. ✅ 输出 Guardrail 在流式回复中截断，且能抓跨 chunk 关键词（场景 B）；
3. ✅ 策略热更新即时生效，无需重启（场景 C）；
4. ✅ 审计 JSONL 每行带 tenant/channel/user/session/decision/latency/tokens/error_type，
   tracing 开启时带 trace_id（场景 A–D）；
5. ✅ 全链路 span `im.callback → gateway.dispatch → model.call`，Admin 变更独立成链；
6. ✅ 指标 5 个仪器全部带 tenant 标签并可导出；
7. ✅ 默认配置（exporters off、无 audit file、无 guardrails）行为与切片前一致，
   既有测试零修改通过。

## 6. 实施记录（与设计的差异 / 已知限制）

- **latency 指标在错误路径也记录**（设计外补的）：慢失败恰恰是最需要延迟数据的时刻，
  原来只在成功路径打点会漏掉；
- **`cloneConfig` 必须带观测三段**：Admin 每次变更都 clone→改→Save，Save 前先 Validate；
  漏拷 `log/audit/telemetry` 会让 clone 出零值 `log.level=""`，所有租户变更被 400 拒绝
  （首轮全量测试一次性暴露 5 个 admin 测试红，已修）；
- **`fromEnv` 也要填观测默认值**：无 config.yaml 的环境变量兜底路径同样要过 Validate；
- **tracing off 时 trace_id 留空**而非 32 个 0（事实核查 #5）；要看关联就配
  `telemetry.traces.exporter: stdout`，一行配置的事；
- **输出触发后继续排空事件通道**：只停止向 IM 转发，不中断模型生成，避免生产端阻塞
  在无人读取的 channel 上；
- **假模型脚本踩坑**：HTTP/1.1 下必须用 `Transfer-Encoding: chunked` 收尾，
  否则 Go 客户端等不到 EOF，事件通道永不关闭，dispatch 一直挂到 2 分钟超时
  （首轮冒烟时输出 tripwire 已正确截断，但收尾审计与 Done 回复迟迟不来，根因在脚本）；
- **两个 stdout exporter 的 JSON 排版不一致**：`stdoutmetric` 是单行紧凑 JSON，
  `stdouttrace` 是**多行 pretty JSON** —— 按行 `json.loads` 会在 span 上报
  `Extra data`，得按 `{`…`}` 块切分再解析；
- **输入拦截场景不产生 `model.call` span**：复跑实测该 trace 只有
  `im.callback → gateway.dispatch` 两层，可与「假模型零请求」互为第二个断言，
  证明拦截确实发生在模型调用之前；
- 一期限制（二期项）：tool 级 Guardrail / 工具白名单 / 预算限流 / 审批走框架 Callbacks；
  审计文件无轮转（追加式，运维侧 logrotate）；msg_id 去重仍是进程内表，
  Redis SETNX 随 Storage Adapter 落地；Guardrail 目前是关键词 + 长度规则，
  语义级脱敏与合规检查待策略中心。
