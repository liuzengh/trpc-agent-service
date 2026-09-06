# 容量评估与压测

## Worker 并发

先区分“模型并发能力”和“当前消费者实现”：`worker.Worker.Run` 每个进程只有一个消费循环，一次执行一个异步任务。当前没有每进程 30 个消费者的配置项。不同 Worker 进程可并行；同一 Session 仍由 Coordinator 串行协调。同步 `/chat` 是另一条可并发的调试入口，不能拿它的并发量代替异步 Worker 容量。

```text
所需并发 = 峰值完成吞吐（turn/s）× 平均端到端执行时间（s）
Worker 数 = ceil(所需并发 / 单 Pod 安全并发) × 1.3 余量
```

示例：峰值 40 turn/s，平均 Runner 6s，需要约 240 活跃 run。按当前每进程 1 个消费者计算，理论上至少 240 个 Worker，再留余量；这显然不是本地部署的目标。实际应先限制入站流量、测量供应商配额，再决定是否开发有界消费者池。不能按一个尚未实现的 30 并发参数估算只需 11 Pod。

单进程 `all` 模式同时还有 Sender、Jobs 和轮询循环，但它们并不增加 Agent Worker 消费并发。单个进程也不是生产租户资源隔离边界。

## 当前边界与故障余量

| 项目 | 当前值/行为 | 调整时需要考虑 |
| --- | --- | --- |
| Worker 自动执行重试 | 最多 3 次；任务重试等待 250ms | 不是副作用工具重放许可，未知结果仍须人工核对 |
| 队列持续错误或空读 | 等待至少 100ms，可随 Context 取消 | 默认 Worker 设置下为 250ms，防止后端故障时空转 |
| Redis pending reclaim | 默认 30s idle | 应大于预期整次处理时间及余量；当前没有队列 PEL 心跳，慢请求可能提前被其他节点认领，不能把租约/幂等保护当作零额外开销 |
| Redis Stream 长度 | 默认约 100,000 条 | 保留 pending 和未读消息，只清理已确认前缀；达到容量拒绝 Publish，任务仍留在 SQL Outbox。按峰值×故障窗口配置并告警，不用裁剪丢任务 |
| SQL 连接池 | 每进程默认 20 open / 5 idle | 总连接数约为进程数×上限，另计 Session/Memory 后端的独立池 |
| MCP 单窗口 | 20 页 / 1000 条 / 120s；HTTP 响应 2MiB | 超限停止推进，需缩小窗口/目标分片，不静默截断 |
| MCP 接收白名单 | 每 Binding 最多 20 群、100 个已确认人类账号 | 当前单进程按目标串行轮询，慢群会影响其他群 |
| 去重与发送事实 | PostgreSQL seen/attempt 没有自动清理 | 增长、索引、备份与保留期必须规划，不能随意清表 |

Agent Revision 应显式设置 `tool_policy.max_run_duration` 和工具次数；不配置就不能假设框架替平台保证统一的执行时限。供应商并发、token/min 与每日预算是额外约束。性能报告还应包含取消时延和后台摘要/记忆调用的资源消耗。

## Token 与成本

```text
每日 prompt token = 日 turn × 平均 prompt token
每日 completion token = 日 turn × 平均 completion token
每日成本 = prompt/1e6×输入单价 + completion/1e6×输出单价
```

Session Summary 会降低长会话 prompt，但产生额外 summary model call；容量模型要单独计入 Summary 和 Memory Extraction Job。

## Redis 与 PostgreSQL

每个 Agent turn 的近似写放大：

- PostgreSQL：inbound/run/outbox 事务 1 次，完成事务 1 次，审计 2–6 行，background job 2 行；
- Redis：queue publish/ack、Session Event/State、lease renew、idempotency、quota；
- Qdrant：每次文档更新约为 `ceil(runes/chunk_step)` 个 point upsert；
- S3：保存 Artifact 前至少一次 list，加一次 put。

从 2 倍预计峰值开始压测，观察连接池 wait、Redis command p95、PostgreSQL lock wait、队列 lag 和 GC。

## 工具

仓库提供无外部依赖的入站压测器：

```bash
go run ./cmd/trpc-loadgen \
  -url http://127.0.0.1:8080/inbound \
  -requests 10000 \
  -concurrency 200 \
  -sessions 1000
```

输出成功/失败、吞吐、p50/p95/p99/max。它只测 Gateway 持久化 ACK；Runner 容量需要同时观察 Redis Stream lag 到 0 的时间。压测消息使用唯一 message ID，避免把幂等命中误当作真实吞吐。

仓库还保留本地完整流水线基线脚本。它会启动 Compose、迁移目标库，并使用 Mock Model 压测持久化 `/inbound`。**只在独立实验环境运行**：不要让它读取日常 IM 服务的 `.env`、复用日常数据库或控制面 Binding，否则可能触发迁移、消费既有任务或启用已有通道。当前收尾回归没有运行这个脚本。历史基线不是新版本生产容量证明。

```bash
BENCHMARK_REQUESTS=1000 \
BENCHMARK_CONCURRENCY=50 \
BENCHMARK_SESSIONS=200 \
  ./scripts/benchmark-local.sh
```

输出分为两部分：`trpc-loadgen` 给出 Gateway ACK 吞吐和延迟，`pipeline` 给出 Worker/Sender 排空时间与最终成功数。该结果只代表运行机器和 Mock Model，不可当作真实模型容量；正式报告还要加入模型供应商配额和真实响应耗时。

## 上线基线

建议至少记录：硬件/Pod 配额、模型供应商限额、平均/分位 token、平均 Tool 数、Session 历史长度、Gateway 峰值、Worker active run、SQL/Redis QPS、队列最大 lag、GC pause 和成本。容量报告必须注明模型与后端版本，否则结果不可复现。
