# 可靠性、发布与容量验收 Runbook

本文把现有的 durable runtime 能力整理为可执行的本地 Docker Compose 验收口径。全部验证在 Docker Desktop 的 PostgreSQL、Redis、Worker、OTel 与 IM profile 上完成；不要求 Kubernetes、云数据库或生产运维资源。仓库提供的 Kubernetes 文件和告警规则仅辅助验收者审阅运行契约；不构成远端发布、告警送达或验收证据。

## 1. 验收范围与不可变规则

本次验收证明：节点重启、重复 IM 回调、短暂依赖中断、模型/工具错误时，系统不会把未提交的工作误报为成功，也不会绕过租户、版本、lease/fence、Inbox、CommitTurn 或 Delivery Ledger。

- PostgreSQL 是 Tenant、Task、Session、Inbox/Outbox 和 Delivery Ledger 的权威源；Redis 仅是可重放 transport 与协调层。
- 任何外部 IM callback 都必须先 durable claim 才 ACK；未能写入 PostgreSQL 时返回非成功，让 Provider 重试。
- `CommitTurn` 原子写入终态、事件、`next_input_seq` 和 Outbox。进程退出或数据库事务失败时不会出现半提交。
- 传输为 at-least-once；Inbox、确定性 request/commit ID、lease/fence 与 Delivery Ledger 保证业务效果至多一次。
- 操作员不得删除 pending stream、手工推进 `next_input_seq`、手改 delivery 状态或跳过 tenant predicate 来清积压。

## 2. 故障与降级策略

| 故障 | 立即行为 | 恢复/重试所有者 | 验收断言 |
|---|---|---|---|
| Worker 节点被终止 | 停止 lease 续租；未完成 delivery 留在 Redis pending | 租约到期后其他 Worker reclaim；旧 fence 的 CommitTurn 被拒绝 | 终态唯一、旧 owner 不能覆盖新 owner |
| IM 重投、429 或 5xx | callback 由 Inbox 去重；Reply relay 不直接调用 Provider；Delivery Ledger 先 claim segment | Provider retry 进入 `retry_wait`，按 Retry-After 或有界退避重试；sent segment 不再发送 | 相同 IM message 只形成一个 request；已发送分片不重复 |
| PostgreSQL 短暂不可用 | callback 不 ACK；依赖 probe 失败时 `/readyz` 为 503；已有事务整体回滚 | Provider 重投 callback；Outbox/lease 在数据库恢复后重放或接管 | 无只写 event、只推进 input seq 或只发 reply 的半提交 |
| Redis 短暂不可用 | 角色 unready；不把 broker 状态当成功；已提交 Outbox 保留 | WebUI 本地组合的 broker/relay/delivery consumer 以有界退避重连；Redis 恢复后 relay 重发 | durable state 不丢，恢复后只产生一次业务效果 |
| 模型超时/网络错误 | 请求 context 取消模型 HTTP 调用，未提交 terminal reply | execution 保持可重放；当前本地模型 Profile 默认 60 秒，可设 `timeout_ms`（100ms–10min） | 不发送半截回复；重放结果仍受 session fence/input gate 约束 |
| 工具失败 | 无副作用 Tool 可随 execution reclaim 重试，但完整结果前不写 terminal reply；有副作用 Tool 的 timeout/未知结果标为 ambiguous，禁止盲重试 | 人工对账或工具自身 idempotency key 后才继续 | 不会因 timeout 重复执行外部副作用 |
| Telemetry/Jaeger 不可用 | 有界 telemetry 降级，业务路径继续 | Collector 恢复后继续导出；Audit 走独立 Outbox | Trace 可丢，Audit intent 不丢 |

当前本地 Tool 仅用于 durable local-note 验证，不是有副作用业务 Tool。任何新增外部写 Tool 必须在发布前声明 `idempotency_key`、重试类别、ambiguous 对账方式和人工 owner；否则禁止进入自动重试路径。

## 3. Go 取消、goroutine 与 Runner 事件通道约束

以下是代码级约束，修改运行角色时必须保持：

1. 进程根 context 来自 `SIGTERM/SIGINT`；每个 HTTP、broker、数据库、模型和 Tool 调用只派生子 context，不得另起脱离父 context 的无限生命周期工作。
2. `trpcservice/worker.Consumer` 在进入 drain 时先取消消费 context，拒绝新 delivery；已领取 work 只在 `DrainTimeout` 内继续，超时后取消 work context，lease 由下一 owner 接管。
3. `cmd/trpc-service/worker_role.go` 由 `Lifecycle` 驱动 readiness：先进入 draining，再等待 consumer、HTTP server、Bundle 与后台 `WaitGroup` 在有界 shutdown context 中退出。
4. `trpcservice/worker/runner.go` 消费 Runner event channel 至 completion；发生错误或取消时调用 `closeRunner`/`drainRunnerEvents`。第三方 `Runner.Close()` 没有 context，因此只给 `EventDrainTimeout` 的有界等待，绝不无限阻塞 goroutine。
5. 后台 goroutine 必须由 `WaitGroup` 归属，错误只能写入有界错误通道或遵从已取消的 context；清理阶段如必须使用 `context.Background()`，只能用于带 timeout 的关闭、解锁或 telemetry flush，不能重新开始业务 work。
6. cancellation hint 是加速信号而非权威事实；Worker 始终从 TaskStore 复核 cancel intent，避免 Redis hint 丢失导致错误执行或错误取消。

审查问题：该 goroutine 的 owner 是谁？它在父 context 取消后何时退出？它是否会阻塞写错误通道？事件 channel 的 producer 在 consumer 退出时是否有有界 drain？四项任一没有明确答案，就不能进入 role 组合根。

## 4. 灰度发布与租户级回滚

### 二进制与 schema

1. 生成不可变镜像 digest；先部署能读 N/N-1 schema 的 consumer，再部署 producer。
2. 以 `schema-migrate` 角色执行显式 `expected-current` 与 `target`。它持有 PostgreSQL advisory lock，拒绝 history gap、checksum drift、未知 migration 和 downgrade。
3. 数据变更遵从 `expand → dual-read/write → backfill → contract`。contract 删除必须晚于回滚观察窗；二进制回滚保留 expand schema。
4. 先只放一个无状态 role 副本，观察 15–30 分钟；门禁为 readiness、错误率、oldest backlog、stale fence、delivery retry/ambiguous、audit lag 与跨租户拒绝，不以 HTTP 2xx 单独判定。

`ExecutionEnvelope` 的 JSON decoder 对未知字段 fail-closed。执行预算的 `execution_budget` 是 schema-v1 的可选扩展：全零预算会省略该字段，允许新 Gateway 与 N-1 Worker 共存；**发布非零预算前必须先完成全部 Worker 升级**，再升级 Gateway 并发布对应 revision。不得在旧 Worker 尚在消费时启用该字段，也不得将 Broker 的解码错误重试为无预算执行。

### 租户级灰度与回滚

配置发布走租户 CAS 和不可变 `ConfigSnapshot`：先用 `/configs/validate` 验证，再用带 `expected_version` 的 `/configs/publish` 发布 allowlist 租户。执行 Envelope 固定 App revision、Config、Policy 和 secret generation；因此在途请求继续使用旧版本，新请求才使用新版本。

回滚**不是**把 active version 指针回拨。调用 `/configs/rollback?expected_version=N&target_version=M` 时，将目标 snapshot payload copy-forward 成一个更大的 ConfigVersion，再原子切换 active pointer、写 Audit 与 TenantControl Outbox。回滚前后都记录：operator、tenant allowlist、旧/新版本、原因、开始/结束时间、SLO 与审计查询证据。

触发回滚：任一 correctness 门禁失败、跨租户拒绝异常、audit dead letter、重复 delivery、超过预算的模型错误率，或 operator 显式终止。禁止通过修改已发布 snapshot、覆盖 Secret 历史版本或删除 outbox 达成回滚。

## 5. 容量评估方法

容量基线必须用本地 Docker 的真实负载报告，不凭容器数量猜测。每个候选版本至少收集：

- 峰值/持续 IM callback QPS，重复率和单租户热点比例；
- p50/p95/p99 turn 时长、TTFT、输入/输出 token、模型超时率；
- 每个 Worker 的 active session、模型并发、lease reclaim/stale fence；
- PostgreSQL connections、TPS、慢事务、Inbox/Outbox/CommitTurn 写入量；
- Redis command QPS、stream pending、oldest lag、lease renew 与 reply queue QPS；
- IM Reply API 429/5xx、retry/ambiguous、审计 lag，以及 drain 时长。

初始估算使用：

```text
worker_replicas = ceil(peak_turn_rps × p95_turn_seconds / safe_model_concurrency_per_worker)
sql_tps ≈ turn_rps × (writes_per_turn + inbox/outbox/relay_writes)
redis_qps ≈ broker_ops + active_sessions / lease_renew_seconds + reply/control/wakeup_ops
```

`safe_model_concurrency_per_worker` 不是 CPU 核数：取模型限额、内存、p99 latency 和预留 30% 余量中的最小值。先从每节点 4–8 个模型并发、数据库连接池上限 32、lease TTL 至少为 p99 单次关键区间的 3 倍开始本地压测，再用观测结果调整；这些是本机基线，不能外推成生产承诺。

仓库提供确定性判定器，报告模板见 [capacity-report.example.json](examples/capacity-report.example.json)：

```bash
go run ./cmd/capacity-evaluate \
  docs/runbook/examples/capacity-report.example.json
```

模板会被判定器以 `evidence_incomplete` 拒绝，不是容量通过证据。真实报告必须替换时间窗口、镜像 digest、观测值和至少两个不可变 evidence reference；`accepted=true` 还要求 backlog 清零、audit 无 dead letter、指标覆盖完整且满足选定阈值。

在已获得模型额度授权时，可先用下列受控客户端建立真实链路的小并发基线。它不会输出模型回复或密钥，每个请求使用独立 session，并等待 durable reply：

```bash
go run ./cmd/local-webui-load \
  -base-url http://127.0.0.1:58086 \
  -requests 8 -concurrency 4 -timeout 2m
```

该命令只产生端到端完成数、失败数、p95 与完成吞吐，适合验证本地 DeepSeek、PostgreSQL、Redis、Worker 和 reply relay 的闭环；它不是长期压测工具，也不会单独构成 `capacity-evaluate` 的容量验收证据。

## 6. 部署方案

### 最小可运行部署：Docker Desktop

使用仓库的 `deploy/compose/docker-compose.local.yml`：PostgreSQL 16、Redis 7、OTel Collector、Jaeger 与一个 standalone `webui-local`/`feishu-local`/`wecom-local` runtime。完整命令、密钥文件约束与真实 IM tunnel 步骤见 [getting-started.md](getting-started.md)。三个 standalone profile 共享本地权威后端，因此有 PostgreSQL advisory lock，必须串行运行；多节点验证使用隔离 Compose project 的 `webui-multinode`。

最小本地验收顺序：

```bash
bash scripts/ci/admission.sh
bash scripts/e2e/backend-adapter.sh
bash scripts/e2e/dependency-recovery.sh
```

随后按 getting-started 的步骤串行完成 WebUI、Feishu 与 WeCom 的 real-account smoke。每次只启动一个 standalone IM profile。

### 非验收参考：生产推荐拓扑

生产应将当前已有入口的 Channel callback、Gateway、Preprocess、Worker、Channel Delivery、Audit Relay、Audit Query/Purge 分开部署；Business Relay 只有在拥有实际进程入口后才能单列 Deployment。PostgreSQL 使用 HA/备份恢复策略，Redis 使用具备 failover 的托管或自管集群，审计库独立于业务库。每个角色独立 Deployment、ServiceAccount、Secret projection、NetworkPolicy、readiness/liveness、PDB 与资源限额。

- callback/gateway 按请求延迟和 callback QPS 扩展；worker/delivery/relay 按 queue/outbox oldest age 与 active concurrency 扩展，缩容必须有稳定窗口；
- `terminationGracePeriodSeconds` 大于应用 shutdown budget，`preStop` 先触发 lifecycle drain，readiness 变为 false 后才终止；
- Secret 用 workload identity/CSI 只读挂载，禁止放入 image、ConfigMap、日志或普通环境模板；
- 网络仅放行角色所需方向：Channel→官方 IM、Worker→Model/Tool/Storage、所有角色→PostgreSQL/Redis/OTel；默认拒绝其余流量；
- 发布按第 4 节 canary 扩大。PDB 保证 gateway/worker 至少一个 ready 副本，Worker 不能因 HPA 缩容直接中断未 drain work。

[`deploy/kubernetes/base`](../../deploy/kubernetes/base/README.md) 以 gateway/worker
展示 Kubernetes 的 role、drain 与 deny-ingress 设计；
[`deploy/prometheus-alerts.yml`](../../deploy/prometheus-alerts.yml) 展示六条 durable-runtime
告警的指标语义。两者没有云数据库、Alertmanager 路由、生产凭据或环境 overlay，不能
替代本 runbook 中的 Docker Compose 验收命令，也不得作为发布资产宣称。

## 7. 最终验收清单

- [ ] `bash scripts/ci/admission.sh` 和 `bash scripts/e2e/backend-adapter.sh` 通过。
- [ ] 下列定向恢复契约通过：

  ```bash
  go test -count=1 ./trpcservice/worker ./trpcservice/relay ./trpcservice/channels/delivery
  docker compose -f deploy/compose/docker-compose.local.yml up -d postgres redis
  docker compose -f deploy/compose/docker-compose.local.yml --profile runtime-test run --rm runtime-test
  ```

  其中覆盖 Worker drain、bounded Runner event drain、ACK 前崩溃 reclaim、relay publish/mark 间退出、callback/reply 重复投递、lease reclaim 与真实 PostgreSQL/Redis Runtime Slice。
- [ ] `bash scripts/e2e/dependency-recovery.sh` 证明 PostgreSQL/Redis 短断会使两个节点 unready，恢复后两个节点无需重启即可重新 ready；durable work 不发生半提交。
- [ ] WebUI 真实 DeepSeek、Feishu 私聊/群 @、WeCom 单聊各自留下脱敏证据；standalone profile 串行切换。
- [ ] Feishu 私聊与 WeCom 单聊各发送一张不超过 10 MiB 的新 JPEG、PNG、GIF 或 WebP 图片，确认该 standalone profile 的 `/readyz`、ClamAV 容器健康、provider media download、tenant-scoped artifact、视觉模型输入与最终文本回复均有脱敏证据。非图片文件只验证安全接收、扫描和审计；当前本地 DeepSeek Profile 不把 PDF/Office 文件伪装为可理解的模型输入。飞书 media-only 群消息按缺少可验证 @mention 的安全规则忽略，不作为失败。
- [ ] 本地 fixture 的 ConfigSnapshot copy-forward rollback 与 schema migration 观察窗契约通过，并记录发布门禁。
- [ ] 本地 Docker 负载报告经 `capacity-evaluate` 判定通过，或明确标为未进行容量验收。

以上全部是本地 Docker 技术验收。生产拓扑段落不产生额外资源需求，也不阻塞本次验收。
