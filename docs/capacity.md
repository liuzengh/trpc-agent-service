# 容量测试与估算

租户的模型执行并发由发布配置中的 `runtime.max_concurrent_runs` 控制，并通过 Redis 在所有
Worker 节点共享。容量测试应同时验证节点总并发和各租户上限，配置及故障边界见
[租户级 Runner 并发配额](tenant-concurrency.md)。

容量数字必须来自目标模型、目标数据库和接近生产的网络；仓库不提供伪造的“每节点 QPS”。`cmd/capacity` 是有上限的 HTTP 负载探针，只输出聚合延迟、状态码和错误率，不输出凭据、请求正文或响应正文。

## 分层测试

先用 `/healthz` 测量入口和 HTTP 基线，再用 `/readyz` 测量共享 PostgreSQL/Redis 探测能力。最后才运行 `gateway` 场景：它会创建真实 Inbox 并可能调用模型，因此必须在隔离租户中使用受限 token，并预先设置预算。

```bash
go run ./cmd/capacity -scenario health -requests 10000 -concurrency 50 \
  -base-url https://agent.example.com -max-error-rate 0.001 -max-p95 200ms

export TRPC_AGENT_LOAD_TOKEN='<from secret manager>'
export TRPC_AGENT_LOAD_BINDING='capacity-binding'
go run ./cmd/capacity -scenario gateway -requests 500 -concurrency 20 \
  -base-url https://agent.example.com -max-error-rate 0.01 -max-p95 2s
unset TRPC_AGENT_LOAD_TOKEN TRPC_AGENT_LOAD_BINDING
```

`gateway` 的 p95 只表示异步接收耗时，不代表 Runner 完整执行耗时。完整耗时使用 `run.completed - run.started` 指标和 trace 统计，并同时记录模型首事件、Outbox 投递、PostgreSQL、Redis 与 Provider 限额。测试中不要使用真实用户 ID、内容或生产 IM binding。

## 估算与准入

- 活跃 Runner 约为 `峰值入站 RPS × 完整执行 p95 秒数`；再按 60%–70% 目标利用率留出节点失效和长尾余量。
- 模型容量分别计算输入、输出 token/s，并受 Provider RPM/TPM 取最小值。
- PostgreSQL 观察连接池等待、事务 p95、热点 session 锁等待、Inbox/Outbox 写放大；Redis 观察 Streams backlog、consumer lag、lease/限流 QPS。
- IM 容量取平台回调峰值、账号发送限额与 Outbox drain 速度中的最小值。
- 单 Pod 增加并发的上限由 `TRPC_AGENT_WORKER_CONCURRENCY` 控制，合法范围 1–256；超范围或非法值启动失败。

记录每轮 commit/image digest、配置版本、Pod 资源、并发、Provider 配额、样本数、p50/p95/p99、错误分类、队列峰值和数据库指标。达到阈值后至少再做一次单 Pod 故障演练；只有剩余副本仍满足 SLO，结果才可用于生产容量承诺。
