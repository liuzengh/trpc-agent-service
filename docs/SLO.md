# 服务等级、告警与故障处置

SLO 按目标环境连续 30 天流量评估；验收须检查 PromQL、告警路由和通知可达性。

## 服务目标

| SLI | 30 天目标 | PromQL/数据源 |
|---|---:|---|
| Gateway 持久化接收可用性 | 99.95% | `sum(rate(agent_gateway_webhooks_total{result="accepted"}[5m])) / sum(rate(agent_gateway_webhooks_total{result=~"accepted|persistence_error"}[5m]))`，仅在分母大于 0 时求值 |
| Consumer 单次处理成功率 | 99.0% | `sum(rate(agent_pipeline_messages_total{stage="consumer",result="success"}[10m])) / sum(rate(agent_pipeline_messages_total{stage="consumer"}[10m]))` |
| Delivery 单次投递成功率 | 99.0% | 同上，`stage="delivery",result=~"success|chunk_sent"`；分段成功不是失败 |
| Inbox claim→处理完成 p95 | < 30s（包含 Worker/模型/工具执行） | `histogram_quantile(.95,sum by(le)(rate(agent_pipeline_duration_seconds_bucket{stage="consumer"}[10m])))` |
| Outbox claim→发送 p95 | < 10s | `histogram_quantile(.95,sum by(le)(rate(agent_pipeline_duration_seconds_bucket{stage="delivery"}[10m])))` |
| Inbox 排队延迟 p99 | < 120s | `histogram_quantile(.99,sum by(le)(rate(agent_pipeline_queue_lag_seconds_bucket{stage="consumer"}[10m])))` |
| Outbox 排队延迟 p99 | < 60s | 同上，`stage="delivery"` |
| 自动队列深度与最老消息年龄 | 按租户容量基线设阈值 | `agent_pipeline_queue_depth{queue=~"inbox|outbox"}` 与 `agent_pipeline_queue_oldest_age_seconds{queue=~"inbox|outbox"}`；Consumer/Delivery 的 QueueInspector 更新快照 |
| 过期 fence 写入拒绝 | = 0 正常态；接管演练按预期出现 | `increase(agent_pipeline_fence_rejections_total[10m])`，统计被拒绝的旧写入 |
| Worker 缓存饱和 | = 0 正常态 | `increase(agent_worker_cache_saturation_total[5m])` |
| 执行状态核对错误 | = 0 正常态 | `increase(agent_execution_reconcile_errors_total[10m])` |
| Summary 单次生成成功率 | > 80%，且连续失败 < 5/10m | `1 - sum(increase(agent_summary_runs_total{result="failed"}[10m])) / clamp_min(sum(increase(agent_summary_runs_total[10m])),1)` |
| Summary 生成耗时 p95 | < 60s | `histogram_quantile(.95,sum by(le)(rate(agent_summary_run_duration_seconds_bucket[10m])))` |

租户指标统一使用有界标签：`METRICS_TENANT_ALLOWLIST` 最多 100 个租户，在每个 HTTP 进程启动时一致注入；非空未允许值映射为 `__other__`，缺失值为 `__unknown__`。`agent_name`、`model` 保留精确值还须分别匹配 `METRICS_AGENT_ALLOWLIST`、`METRICS_MODEL_ALLOWLIST` 中的 `tenant/name`，每类最多 200 对，否则聚合为 `__other__`。完整明细进入日志/成本仓库，发布版本和名称轮换也须遵守这些上限。

Worker 结果路径报告的结算量记录为 `agent_tokens_total{type="accounted"}`：Provider 返回有效 usage 时使用实际汇总量，usage 缺失时使用保守预留量。失败或取消由预算 finalizer 保留的额度以 Redis 账本为准，不能仅靠该指标还原全部账本。该指标不拆分 prompt/completion，也不表示货币金额；金额分析须结合模型价格表和账单核对。

<a id="error-budget"></a>

## 错误预算

99.95% 目标允许每 30 天约 21.6 分钟不可用。每个消耗速率窗口均要求有效流量分母大于 0，禁止用 `clamp_min` 将空闲窗口计算为 100% 错误率：

- 1h 消耗速率 > 14.4 且 5m > 14.4：紧急通知，冻结发布。
- 6h > 6 且 30m > 6：紧急通知，排查供应商/数据库。
- 3d > 1：创建工单，本周期仅开展可靠性工作。
- 消耗 50%：停止非必要灰度；消耗 75%：运维核对变更关联性后执行兼容版本回滚；100%：变更冻结并事故复盘。

## 告警规则

完整规则位于 `deploy/prometheus-rules.yml`，由 `deploy/prometheus.yml` 的 `rule_files` 加载，覆盖排队延迟、最老消息年龄、队列检查失败、容量拒绝、重试风暴、fence 拒绝、Summary 失败/耗时及 Gateway 5m/1h、30m/6h 预算消耗。pipeline 与 Summary 耗时 bucket 覆盖 30/60/120/300 秒。`validate.sh` 使用 `promtool` 校验语法；部署须配置 Alertmanager receiver 与通知路由，并触发测试告警验证接收端。

核心片段：

```yaml
groups:
- name: agent-platform
  rules:
  - alert: AgentInboxLagHigh
    expr: histogram_quantile(0.99, sum by(le)(rate(agent_pipeline_queue_lag_seconds_bucket{stage="consumer"}[10m]))) > 120
    for: 10m
    labels: {severity: page}
    annotations: {runbook: "docs/SLO.md#inbox-lag"}
  - alert: AgentDeliveryRetryStorm
    expr: sum(rate(agent_pipeline_retries_total{stage="delivery"}[5m])) > 0.2 * sum(rate(agent_pipeline_messages_total{stage="delivery"}[5m]))
    for: 10m
    labels: {severity: page}
  - alert: AgentFenceRejected
    expr: increase(agent_pipeline_fence_rejections_total[10m]) > 0
    labels: {severity: ticket}
```

DLQ 数量需使用 PostgreSQL exporter 的只读查询：

```sql
SELECT 'inbox' queue, count(*) FROM inbox_messages WHERE status IN ('DEAD_LETTERED','WAITING_RECONCILIATION')
UNION ALL
SELECT 'outbox', count(*) FROM outbox_messages WHERE status IN ('DEAD_LETTERED','WAITING_RECONCILIATION');
```

## 故障处置

<a id="inbox-lag"></a>

### Inbox 积压

1. 检查 Worker 的 Provider 延迟、Consumer 副本数、数据库连接等待和领取查询耗时。
2. 模型变慢时按容量公式限制并发并联系 Provider；扩 Consumer 可能加重模型/数据库压力。
3. 数据库 checkpoint/I/O 饱和时降低轮询频率和并发，保留 Gateway 写入能力，必要时暂停非关键租户。
4. 恢复后观察重试放大与 DLQ，禁止批量无审计重放。

<a id="delivery-retry-storm"></a>

### Delivery 重试风暴

1. 按 channel/account 聚合 429/5xx，核对 Provider `Retry-After`。
2. 降低 Delivery 并发并增加退避上限，避免扩容加重限流。
3. Provider 恢复后逐租户重放 DLQ，记录 incident ID。

<a id="queue-inspection"></a>

### 队列检查失败

1. `AgentQueueInspectionFailing` 表示 Consumer/Delivery 无法读取队列快照；检查 PostgreSQL 连通性、只读权限、连接池和 `InspectQueue` 耗时。
2. 失败期间 `agent_pipeline_queue_depth`、`agent_pipeline_queue_oldest_age_seconds` 保留最后成功快照，不能代表当前状态。恢复检查并观察至少一个抓取周期后，再判断积压是否解除。
3. 告警仅表明观测链路失败；结合数据库只读查询、队列处理和重试指标核对积压。未配置 Prometheus/Alertmanager 接收器时，须完成通知可达性验收。

<a id="queue-admission"></a>

### 队列容量拒绝

1. `AgentTenantQueueAdmissionRejecting` 表示租户触发运维配置的 `max_queued`；Gateway 返回 429，保留 Provider 重试机会，不计为持久化故障。
2. 按租户容量基线检查自动 Inbox 行数、Consumer 吞吐和 `MaxInflight`；扩 Worker/Consumer 或经审计提高配额，禁止删队列行绕过准入。
3. 相同 Provider 消息重投不重复占用配额。429 持续超过容量窗口时冻结该租户发布，检查回调峰值、数据库锁等待和 Session 热点。

<a id="fence-rejection"></a>

### 过期写入拒绝

1. 查询相同 message ID 的 lease_owner/version/updated_at。
2. 检查进程暂停、超时配置、数据库时钟和模型调用是否超过 lease。
3. 禁止绕过 fence 手工改为 COMPLETED；确认当前 owner 后再决定重放。

<a id="auditresult-persistence-failure"></a>

### 审计与结果持久化失败

1. 检查 PostgreSQL 和 execution 状态，区分 Runner 启动前读取失败与执行后结果提交失败。
2. 启动前明确标记 retry-safe 的失败按策略重试；执行后结果未知进入 reconciliation，按幂等键查询目标系统。
3. 核对工具副作用与持久化结果后审计恢复；危险工具缺少业务幂等证据时转人工处置。

<a id="audit-sink-failure"></a>

### 审计写入故障

1. `AgentDurableAuditWriteFailing` 对应 `agent_audit_write_failures_total{sink="durable"}`，检查 PostgreSQL、审计表写权限及连接等待。数据库为审计权威；stderr 镜像独立尝试，镜像失败不阻断数据库写入。
2. `auditLevel=detailed` 在 Runner 启动前持久化 `execution_admitted`；失败按 preflight 释放预算预留，允许安全重试。最终结果审计失败时执行已发生，进入结果核对，禁止自动重跑模型或工具。
3. 核对执行记录、结果缓存和工具业务幂等证据，记录事件单并补齐经确认的审计事实；不得把缺失记录直接认定为“未执行”。
4. `AgentAuditLogMirrorFailing` 表示日志镜像故障，检查日志采集、磁盘与 stderr 输出；数据库审计成功时保持业务成功语义。恢复后分别检查持久审计和日志采集，避免用一条链路的成功替代另一条。

<a id="worker-cache-saturation"></a>

### Worker 缓存饱和

1. 检查活跃请求、Agent 版本/租户配置变更频率和模型客户端内存，禁止无界扩缓存。
2. 长调用占满时扩 Worker 并限制单租户并发；版本频繁变更时停止灰度，等待旧 key 空闲回收。
3. RSS/FD/连接池基准有余量时才提高 `WORKER_CACHE_SIZE`。

<a id="execution-reconciliation"></a>

### 执行状态核对

1. 检查 PostgreSQL 可用性、`idx_execution_stale_running` 和 Worker 日志中的稳定错误类。
2. 查询 ABANDONED 对应的 invocation result；该状态表示终态未可靠记录，不能据此认定模型失败。
3. 禁止将 ABANDONED 批量改为 SUCCEEDED；依据结果及工具业务幂等证据逐条处置。

<a id="summary-generation"></a>

### Summary 生成异常

1. 用 `agent_summary_runs_total{result}`、`agent_summary_run_duration_seconds` 区分失败突增、失败率和延迟，关联同窗口的模型、Session 后端、PostgreSQL、Redis 延迟。
2. 只读查询 `summary_jobs` 的 PENDING/PROCESSING/FAILED 分布、`lease_until`、`attempts`、`last_error` 稳定错误类，核对 `summary_checkpoints.max_event_sequence` 单调推进；未确认 lease owner 时禁止手改状态。
3. 模型或 Session 后端变慢时降低 `SUMMARY_CONCURRENCY`；扩副本前确认限流、连接池和 token 预算有余量，保护数据库与供应商。
4. 单次超时以独立有界上下文持久化失败；父进程取消后停止领取并排空已领取任务。先等待 lease/retry 收敛，禁止批量重置或重复生成。
5. 恢复后至少观察两个 10 分钟窗口，抽验 checkpoint 序号、内容哈希和 Runner 可见摘要；告警恢复不代表历史摘要已补齐。
