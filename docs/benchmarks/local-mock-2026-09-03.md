# 本地 Mock 流水线容量基线（2026-09-03）

这份记录验证压测工具和完整持久化流水线是否能稳定处理并排空任务，不代表真实模型或生产集群容量。

## 环境

```text
OS: Linux 6.6.87.2-microsoft-standard-WSL2 x86_64
CPU: AMD Ryzen 7 H 255，8 核 16 线程
内存: 15 GiB
Go: go1.25.7
Docker: 29.1.3
模型: Tutorial Mock Model
部署: 单进程 role=all
Session/Coordinator/Idempotency/Queue/Quota: Redis 7
Control Plane/Inbox/Run/Outbox: PostgreSQL 16
```

## 正式基线

执行命令：

```bash
BENCHMARK_REQUESTS=1000 \
BENCHMARK_CONCURRENCY=50 \
BENCHMARK_SESSIONS=200 \
  ./scripts/benchmark-local.sh
```

结果：

```text
requests=1000
success=1000
failed=0
Gateway ACK elapsed=1.080833392s
Gateway ACK throughput=925.21 req/s
latency p50=50.489917ms
latency p95=77.139595ms
latency p99=97.760206ms
latency max=111.453262ms
Agent Run completed=1000
Outbound sent=1000
pipeline failed=0
drain after final ACK=38s
```

同一脚本还以 500 请求、50 并发、100 Session 做过一次预检，结果为 500/500 成功，Gateway ACK 874.20 req/s，p95 94.60 ms，最终排空 19 秒。

## 结论

- Gateway 的本地 PostgreSQL 持久化入口在这组参数下没有失败，p95 低于 100 ms。
- 单进程单 Worker 消费链路可以完整排空 1000 个 Agent Run 和 Outbound，没有产生失败任务。
- 从压测开始到完全排空约 39 秒，对应本地 Mock 全流水线约 25～26 turn/s。这个数字主要反映当前单进程 Worker/Sender 消费方式，不包含真实模型等待时间。
- Gateway ACK 吞吐不能直接当作 Agent 完成吞吐。生产容量必须分别记录入口 ACK、Queue Lag、Worker 完成和 Reply Sent。

## 仍需补充

- 拆分多个 Worker/Sender 后的扩展曲线；
- PostgreSQL、Redis 的 CPU、连接池、命令和锁等待；
- 真实模型的首 Token、完整响应耗时和供应商并发限制；
- 长 Session、Tool 调用、Memory/Knowledge 和大消息对容量的影响；
- 30 分钟以上稳定性测试和错误注入。
