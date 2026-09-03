# 治理、安全和运维设计

## 1. 租户级治理

治理策略由 `agent_revision` 固定，并在每次 Runner 调用时通过 Plugin、Guardrail 和 RunOption 注入。不能在 Agent 运行中途读取可变的“当前策略”，否则一次工具调用的前后判断可能使用不同版本。

推荐的治理链路：

```text
Channel 身份校验
→ 租户与用户限流
→ BeforeAgent 输入检查
→ BeforeModel 预算和敏感内容处理
→ Tool 可见性过滤
→ BeforeTool 权限、参数和审批
→ Tool 自身 PermissionChecker
→ AfterTool 结果脱敏
→ OnEvent 输出过滤
→ AfterRun 成本、审计和账单
```

### 工具权限

`agent.WithToolFilter` 只控制模型能看到的工具，不是完整授权。平台同时使用：

- `WithToolFilter`：根据租户工具白名单缩小工具集合；
- `WithToolPermissionPolicy`：对每次实际执行做租户、用户、角色、参数和预算判断；
- Tool `PermissionChecker`：工具自身的不可绕过规则；
- MCP Client wrapper：强制租户凭据、目标域名和超时。

Tool policy 返回 `allow`、`deny` 或 `ask`。`ask` 表示需要用户确认，平台保存 approval record 并生成 IM 卡片。确认回调绑定：

```text
tenant_id
actor_user_id
request_id
tool_call_id
tool_name
arguments_hash
expires_at
nonce
```

批准后重新进入 PermissionPolicy，只有上述字段全部匹配才返回 allow。高风险工具还可以要求管理员或双人审批。

### 模型预算

预算同时限制单次请求和周期用量：

- 单次最大模型调用次数；
- 单次最大 prompt、completion、total token；
- 单次最大运行时间和工具时间；
- 每用户、每 App、每租户分钟/小时/日额度；
- 模型和工具的金额预算。

BeforeModel 在发送请求前预留预算，AfterModel 根据实际 usage 结算，多退少补。预留失败直接返回受控错误。模型调用链中存在 Tool 循环时，每次调用都重新检查剩余额度。

### 敏感信息处理

输入和输出按租户策略执行 DLP：身份证、手机号、邮箱、银行卡、密钥和内部账号等字段可以脱敏、拒绝或只在受控工具中使用。脱敏发生在日志和 trace 写出之前；若业务需要模型看到原值，DLP Plugin 可以保留内存中的原始输入，但输出记录只保留掩码和摘要。

当前 Revision `guardrail_config` 已编译为 tRPC-Agent-Go `ModelCallbacks`：`max_input_chars` 和 `blocked_input_patterns` 在 BeforeModel 阻断，`redact_output_patterns` 在 AfterModel 克隆 Response 后替换文本。Go regexp 使用 RE2，不存在灾难性回溯；更复杂的企业 DLP 可继续实现为 Plugin/外部服务。

## 2. 密钥管理

以下数据不进入配置明文、Git、Session、Memory、日志或 trace：

- IM token、EncodingAESKey、AppSecret；
- 模型 API key；
- 数据库、Redis、对象存储凭据；
- MCP Authorization header；
- Tool 调用下游系统的访问令牌。

Control DB 只保存 `secret_ref` 和 secret version。Worker 通过 workload identity 访问 Secret Manager，权限范围限定到所处理租户的命名空间。密钥在内存中短暂存在，缓存使用较短 TTL，并在版本变化后失效。

日志库提供统一 `Redact` 方法，至少识别：

```text
Authorization
X-API-Key
Cookie / Set-Cookie
access_token
refresh_token
secret
password
api_key
encoding_aes_key
数据库 DSN
```

错误包装不能直接输出整个配置对象、HTTP header 或下游响应体。panic recovery 记录 stack，但先对附带字段脱敏。

## 3. 审计日志

每次 run 和工具决策都写结构化审计记录。必需字段：

```text
tenant_id
channel
user_id
actor_user_id
session_id
request_id
agent_name
revision_id
tool_name
decision
policy_version
latency_ms
error_type
cost
trace_id
```

工具参数默认只保存 `arguments_hash` 和经过白名单筛选的摘要。需要保留完整参数的行业租户，应把原文写入独立加密仓库，审计表只保存引用和访问级别。

审计写入失败的处理取决于策略：普通租户可以写本地 WAL 后异步补传；强监管租户采用 fail-closed，审计无法可靠落地时不执行危险工具。

## 4. OpenTelemetry 链路

Channel callback 创建根 span，后续链路包含：

```text
im.callback
  gateway.persist_inbound
  outbox.publish
  worker.acquire_lease
  runner.run
    session.get
    session.append.user
    model.chat
    tool.execute
    session.append.tool_result
    session.append.assistant
    summary.enqueue
    memory.enqueue
  reply.persist
  im.send
```

Worker 使用 `agent.WithSpanAttributes` 注入低基数字段：

```text
tenant.plan
channel.type
agent.type
revision.version
backend.session.type
backend.memory.type
model.provider
model.name
```

`tenant_id` 可以用于 trace 和日志查询，但不建议直接作为 Prometheus 高基数 label。规模较小时可保留；租户数较大时使用套餐、区域等聚合 label，并通过 exemplar 或日志关联具体 tenant。

tRPC-Agent-Go 会记录模型、工具和 workflow spans。平台还要为 Storage Router、租约、队列和 IM 发送补 span。跨队列传播 `traceparent`；如果生产者 span 已结束，也可以由消费者创建新 trace 并添加 link。

当前实现已覆盖 HTTP callback、durable AgentTask、Worker、Session/Memory/Knowledge/Artifact Router、Background Job、outbound payload 和 Reply Sender；响应头 `X-Trace-ID` 可直接关联 `audit_log.trace_id`。

### Payload 采集

生产环境默认不导出完整 prompt、模型响应、工具参数和工具结果。配置 span attribute policy：

- 不需要的 payload 直接 Drop，避免序列化带来的内存峰值；
- 必须保留的内容只保留大小、hash、类型和截断摘要；
- 图片、音频、文件只记录 Artifact ID、MIME 和大小；
- 错误响应移除 Authorization、URL query secret 和内部地址。

## 5. 指标和告警

### 接入与运行指标

```text
im_callback_total{channel,status}
im_callback_duration_seconds{channel}
inbound_deduplicated_total{channel}
queue_lag_seconds{queue,worker_pool}
agent_run_total{agent_type,status}
agent_run_duration_seconds{agent_type,model}
active_agent_runs{worker_pool}
runner_event_drain_timeout_total
```

当前代码导出的稳定指标名为：

```text
agent.inbound.messages
agent.idempotency.replays
agent.runs
agent.run.duration
agent.reply.deliveries
agent.reply.duration
agent.model.prompt_tokens
agent.model.completion_tokens
agent.model.cost
```

队列 lag、background pending/dead、repair backlog 当前从 PostgreSQL/Redis 状态采集，生产可用 Collector SQL/Redis receiver 或独立 exporter 转成 Prometheus gauge。

### 模型和工具指标

```text
model_call_total{provider,model,status}
model_call_duration_seconds{provider,model}
model_tokens_total{provider,model,type}
tool_call_total{tool,status,decision}
tool_call_duration_seconds{tool}
approval_pending_total{tool}
tenant_cost_total{billing_group}
```

### 数据和投递指标

```text
session_backend_duration_seconds{backend,operation}
session_append_error_total{backend}
session_lease_conflict_total
summary_job_lag_seconds
memory_job_lag_seconds
migration_repair_backlog
im_delivery_total{channel,status}
im_delivery_duration_seconds{channel}
outbox_pending_total{event_type}
```

主要告警：

- callback 5xx 或验签失败率异常；
- queue lag 超过对话 SLO；
- session lease 冲突突增；
- 模型超时和限流持续升高；
- reply delivery 成功率下降；
- summary/memory lag 长时间不恢复；
- 数据迁移 checksum 差异；
- 单租户成本或 token 消耗异常；
- Secret 读取失败或过期凭据使用。

## 6. 故障和降级

### 节点故障

Gateway 和 Worker 不保存唯一状态。Gateway 重启后由 IM 重试和 inbound 唯一索引恢复；Worker 崩溃后由队列重投和 session 租约接管。fencing token 阻止旧 Worker 提交过期结果。

### Session 数据库不可用

读取失败时不调用模型，因为模型缺少可靠上下文。任务保留在队列中退避重试。达到租户最大等待时间后，发送“服务暂时不可用”提示，并保留原 request 供恢复，不能创建新 session 规避错误。

### Memory 或 Knowledge 不可用

它们不是所有 Agent 的强依赖。租户策略允许时：

- Memory 读取失败：本轮不加载长期记忆，写 warning；
- 自动 Memory 写入失败：任务重试，不影响已完成回复；
- Knowledge 检索失败：告诉模型知识库暂不可用，禁止编造检索结果；
- 强依赖知识库的 Agent 可以 fail-closed。

### 模型超时

模型调用设置独立超时和总 run deadline。可选降级顺序：同供应商重试、备用 endpoint、备用模型、返回固定提示。已经产生 Tool 副作用后切换模型时，必须把工具结果写入 Session，再由备用模型基于相同结果生成回答，不能重新执行 Tool。

### 工具失败

只读工具按指数退避重试。副作用工具按 journal 和幂等键处理。工具超时后状态未知时标记 `uncertain`，先查询下游结果或进入人工对账。

### IM 发送失败

Session 和 Agent run 已经完成，发送失败只影响 delivery。Reply Sender 独立重试并尊重 retry-after；长期失败进入死信。用户再次发消息时可以补发上一条结果。

## 7. Go 生命周期

服务根 context 由 SIGTERM、SIGINT 控制。关闭顺序：

1. readiness 置为 false，停止接收新任务；
2. HTTP Server `Shutdown`，停止 Channel polling；
3. 等待已取得租约的 run 到 grace deadline；
4. 取消剩余 Runner request；
5. 持续排空 Event channel；
6. 关闭 Runner；
7. 停止 Job Worker、Reply Sender 和 outbox relay；
8. 关闭平台持有的 Session、Memory、Artifact 和消息队列连接；
9. flush trace、metric 和 log。

每个后台 goroutine 都由 `errgroup` 或明确的 WaitGroup 管理。禁止启动没有退出条件的 goroutine。框架中用户传入的 Session、Memory、Artifact Service 通常由调用者持有，不能只调用 `Runner.Close()` 后假定连接已经释放。

## 8. 灰度发布和回滚

发布新 revision 后，通过稳定哈希选择灰度对象。当前实现使用 `tenant_id | app_id | salt | user_id | session_id` 作为 routing key：

```text
hash(tenant_id | app_id | conversation_id) % 100 < rollout_percent
```

conversation 首次命中 revision 后写入 `pinned_revision_id`，后续 turn 继续使用该版本。这样同一会话不会在两套提示词和工具策略之间跳变。

回滚时把稳定 revision 指向旧版本，并停止新会话进入问题 revision。已固定的会话可以继续完成，也可以在管理员明确选择后迁移。危险工具策略发生安全回滚时允许立即覆盖，但要记录 emergency policy version，不能静默修改 revision 内容。

配置回滚不自动回滚 Session、Memory 和工具副作用。涉及数据格式变化时，revision 必须声明兼容版本和迁移器。

## 9. 容量评估

Worker 并发首先由外部等待时间决定，不只由 CPU 决定。使用 Little 定律估算活跃 run：

```text
active_runs ≈ peak_requests_per_second × average_run_seconds
```

Pod 数量：

```text
worker_pods = ceil(active_runs / safe_concurrency_per_pod / target_utilization)
```

`safe_concurrency_per_pod` 通过压测得到，受以下因素限制：

- 模型连接和供应商并发配额；
- 每个 run 的 Session/Event 内存；
- Tool 和 MCP 连接数；
- Event channel 和流式聚合缓冲；
- goroutine、文件描述符和网络连接；
- 代码执行或浏览器等高资源工具。

后端 QPS 估算：

```text
session_qps ≈ request_qps ×
  (1 get_session + 1 user_event + average_persisted_events + state/summary operations)

model_token_rate ≈ request_qps × average_model_calls_per_run × average_tokens_per_call

reply_qps ≈ request_qps × average_reply_parts × retry_multiplier
```

Redis 重点估算热 key、内存、AOF、Lua p99；SQL 重点估算连接数、Session 行锁等待、Event 写入和索引膨胀；向量库估算文档数、维度、top-k、过滤条件和索引构建资源。

## 10. 最小部署

Docker Compose 组件：

```text
trpc-agent-service    Admin + Gateway + Channel + Worker + Jobs
postgres             Control DB、journal、audit
redis                Session、Memory、lease、queue（MVP）
minio                Artifact 和知识原文
qdrant                Knowledge vector store
otel-collector        trace/metric/log 汇聚
prometheus + tempo    本地观测
```

MVP 可以使用 Redis Streams 代替 Kafka，降低组件数量。所有后台任务仍写持久化 stream，不能回退为进程内 channel。

## 11. 生产部署

Kubernetes 推荐拆分：

```text
admin-api Deployment
agent-gateway Deployment
channel-wecom Deployment
channel-telegram Deployment
agent-worker Deployment / 多工作池
job-worker Deployment
reply-sender Deployment
outbox-relay Deployment
otel-collector DaemonSet / Gateway
```

生产要求：

- 每类服务至少两个副本和 PodDisruptionBudget；
- Worker 使用 preStop 和足够 termination grace period；
- HPA 基于 queue lag、active runs 和 delivery lag；
- NetworkPolicy 限制 Worker 只能访问允许的模型、MCP 和数据服务；
- 代码执行工具放独立 sandbox pool，不与 Gateway 共进程；
- PostgreSQL、Redis、对象存储和向量库使用高可用形态；
- 备份恢复、跨可用区故障和 Secret 轮换按季度演练。

## 12. 建议 SLO

| 指标 | 初始目标 |
| --- | --- |
| IM callback 接收可用性 | 99.95% |
| callback 持久化 p95 | 小于 500 ms |
| 消息开始处理 p95 | 小于 2 s，不含供应商故障 |
| Agent 首个可见响应 p95 | 按模型分级设定 |
| Session 写入成功率 | 99.99% |
| IM 最终投递成功率 | 99.9%，排除用户屏蔽等终止错误 |
| summary/memory 任务 99% 延迟 | 小于 5 min |
| 跨租户数据事故 | 0 |
