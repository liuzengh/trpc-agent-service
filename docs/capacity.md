# 容量与容量验证边界

## 结论先行

当前仓库提供可重复的 disposable Compose 容量工具。本文保留工具口径与规划公式；仓库内没有足以推出生产容量上限的真实 Provider、真实 IM、生产 Kubernetes 或生产数据库测量报告，因此生产容量为 `EXTERNAL_VERIFICATION_NOT_INCLUDED`。

## 工具实际测什么

`.github/workflows/capacity.yml` 是手工触发的 Capacity Evaluation，默认输入为 concurrency=4、requests=100、可选 duration、最大错误率 0.05。流程：

1. 启动 `compose.yaml + compose.deployment-e2e.yaml` 的 PostgreSQL、Redis、Qdrant、Gateway、worker-1、worker-2。
2. `cmd/capacity-prepare` 创建随机 tenant、app、PostgreSQL Session config 和一次性 API key；key 写到权限为 0600 的临时文件。
3. `cmd/capacity-evaluate` 并发 POST `/v1/chat/completions`，为每个 worker 使用独立 session header，记录成功/失败、吞吐和 latency min/p50/p95/p99/max。
4. `scripts/capacity-observe.py` 每 2s 采样 Docker CPU/内存、Redis Stream pending/lag/consumer、Redis command delta、PostgreSQL transaction/write delta，并把报告写入 `data/capacity`。
5. Workflow 要求 total_requests>0、error_rate≤max_error_rate、`success_criteria_passed=true`，再上传报告；敏感值扫描失败会删除相关证据。

`cmd/capacity-evaluate` 的 HTTP 成功只按 2xx 统计；它不会把模型 token、IM Provider、Qdrant 查询、COS 对象吞吐变成生产容量结论。当前 capacity workflow 使用 `Dockerfile.e2e-worker` 的 deterministic model，不是真实 OpenAI-compatible endpoint。

## 代码中的容量控制点

| 资源/控制点 | 当前实现 | 对容量的含义 |
| --- | --- | --- |
| Worker execution concurrency | `TRPC_AGENT_SERVICE_WORKER_CONCURRENCY`，默认 4；production overlay 为 8 | 粗略上限 `worker replicas × concurrency`；受 execution lease、Session serial lane 和资源影响 |
| Session | 同一 `(tenant, app, principal, session)` 通过 Redis Session Lease/Session Lock 串行 | 同一 Session 吞吐近似受单次模型/工具耗时限制，增加 Worker 不会并行同一 lane |
| Execution retry | PostgreSQL execution 最多 3 次，retry delay 1–30s bounded | 故障时实际后台工作量可能高于入口请求量 |
| Reply | 默认每 binding 5 次/s、1s window；reply 最多 8 attempts，retry max 1m | IM 回复峰值由 binding 数 × 5/s 近似受限，排队会在 reply outbox 增长 |
| Dispatch | Redis Stream + SQL dispatch outbox | 每次 Admission、Relay、claim、event/终态都会增加 SQL/Redis 操作；不能用单一 request/s 指标代表负载 |
| PostgreSQL | execution/session lane/event/outbox/audit/catalog 都会写 | SQL 连接池、IO、索引和 event 数量通常是长对话瓶颈之一 |
| Memory/Knowledge/Artifact | 外部 TencentDB/Qdrant/COS | 端到端 latency 和 quota 由 Provider 决定，本地 HTTP evaluator 不覆盖真实组合 |

## 规划公式（不是实测结论）

令：

- `W` = ready Worker 数，`C` = 每 Worker concurrency；
- `L_model` = P95 模型/工具执行时延，`L_queue` = 可接受排队时延；
- `λ` = 入口请求率，`p_retry` = retry 比例；
- `T_in/T_out` = 平均输入/输出 token，`P_in/P_out` = operator 定价；
- `B` = active IM binding 数。

可用于容量规划的第一近似是：

```text
in-flight execution capacity <= W × C
required worker slots ≈ peak λ × (L_model + L_queue) × (1 + p_retry)
reply provider rate <= B × 5 replies/second   # 当前默认值，按 binding 分布实际验证
estimated cost/request = T_in × P_in + T_out × P_out
```

若同一 Session 的请求率是 `λ_session`，还需满足 `λ_session × L_session < 1`，因为 lane 是串行的。SQL QPS 不能从 `λ` 直接相乘得出固定常数：每个运行会产生多少 Runner event、tool call、Session write、audit 和 reply projection 取决于工作负载。应以 capacity observer 的 PostgreSQL transaction/write delta 和 Redis command delta 进行工作负载实测。

## 分层证据

| 层 | 当前能说什么 | 状态 |
| --- | --- | --- |
| 测试工具能力 | evaluator 可发并发 HTTP 请求；observer 可写资源/队列/SQL delta；参数和报告有单测 | REPO_VERIFIED |
| 本地 smoke | Workflow 定义了 disposable 2-worker deterministic topology、错误率门槛和报告 artifact；当前工作树没有该次 run artifact | IMPLEMENTED |
| 受控 E2E | deployment/runtime/fault/IM workflows 定义 deterministic model 的路径、重启、恢复和投影 | IMPLEMENTED |
| 生产容量 | 真实模型、真实 IM、真实外部 Memory/Vector/Object、生产数据库、K8s HPA 下的 p95/SLO/安全上限 | EXTERNAL_VERIFICATION_NOT_INCLUDED |

## 实际测量记录口径

有意义的容量测量应保留 git SHA、worker/gateway 数、concurrency、请求数/时间、Session 分布、模型/Provider 类型、错误分类、p50/p95/p99、Redis pending/lag/commands、PostgreSQL transactions/writes、CPU/memory 和 reply backlog。当前工作区只记录测量口径和 disposable 工具；缺少这些环境、工作负载、数值及可定位的运行证据时，结果只能作为 smoke 或容量验证，不能作为生产上限、SLO 或扩容承诺。
