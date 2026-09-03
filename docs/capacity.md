# 容量评估与压测

## Worker 并发

单 Worker 可承载的活跃 Session 主要由模型延迟和供应商并发决定：

```text
所需并发 = 峰值完成吞吐（turn/s）× 平均端到端执行时间（s）
Worker 数 = ceil(所需并发 / 单 Pod 安全并发) × 1.3 余量
```

示例：峰值 40 turn/s，平均 Runner 6s，需要约 240 活跃 run。若单 Pod 安全并发 30，则基础 8 Pod，乘 1.3 后配置 11 Pod。

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

仓库还提供本地完整流水线基线脚本。它启动 PostgreSQL 和 Redis，使用 Mock Model 压测持久化 `/inbound`，并等待 Agent Run 与 Outbound 全部完成：

```bash
BENCHMARK_REQUESTS=1000 \
BENCHMARK_CONCURRENCY=50 \
BENCHMARK_SESSIONS=200 \
  ./scripts/benchmark-local.sh
```

输出分为两部分：`trpc-loadgen` 给出 Gateway ACK 吞吐和延迟，`pipeline` 给出 Worker/Sender 排空时间与最终成功数。该结果只代表运行机器和 Mock Model，不可当作真实模型容量；正式报告还要加入模型供应商配额和真实响应耗时。

## 上线基线

建议至少记录：硬件/Pod 配额、模型供应商限额、平均/分位 token、平均 Tool 数、Session 历史长度、Gateway 峰值、Worker active run、SQL/Redis QPS、队列最大 lag、GC pause 和成本。容量报告必须注明模型与后端版本，否则结果不可复现。
