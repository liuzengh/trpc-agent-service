# 故障演练手册

演练必须在已确认的命名空间、维护窗口和回滚负责人就绪后执行。先观察告警与真实流量，再改变状态；禁止删除 PostgreSQL/Redis 数据卷。`scripts/kubernetes_fault_drill.sh` 默认只预览，实际只允许删除一个已就绪的应用 Pod，并要求至少三个就绪副本与显式确认：

```bash
TRPC_AGENT_DRILL_NAMESPACE=trpc-agent ./scripts/kubernetes_fault_drill.sh --preview
TRPC_AGENT_DRILL_NAMESPACE=trpc-agent \
TRPC_AGENT_DRILL_CONFIRM=pod-restart ./scripts/kubernetes_fault_drill.sh --execute
```

验收要求是入口持续可用、PDB/滚动策略生效、旧 Pod 的在途请求在应用配置的 100 秒排空上限内完成或在 lease 到期后由其他节点有界接管、Inbox 不丢失且 Outbox 不重复投递。Pod 的 120 秒终止窗口还为 preStop 和进程退出保留余量。演练前后对比 `agent_queue_depth`、运行中请求、DLQ、Outbox backlog、错误率和 p95，并保留 trace_id 作为证据。

## 必做场景

| 场景 | 注入方式 | 预期结果 | 停止条件 |
| --- | --- | --- | --- |
| 单 Pod 退出 | 使用仓库脚本删除一个 Pod | Service 只路由 ready Pod；副本恢复；持久化 claim 被接管 | 可用副本低于 2、错误率越过 SLO |
| PostgreSQL 短暂不可用 | 在测试环境由 DBA/网络策略阻断单个测试客户端 | `/readyz` 503、Pod 不被路由；已有操作显式失败/重试，不回退 InMemory | 数据不一致、恢复后仍不 ready |
| Redis 短暂不可用 | 阻断测试命名空间到 Redis | `/readyz` 503；持久化 Inbox 保留，恢复后重新调度 | 消息丢失、无限重试或 goroutine 增长 |
| 模型超时/限流 | 测试 Provider 返回 429/超时 | context 有界结束，状态/审计记录分类，未产生空成功回复 | 请求超过总超时、密钥出现在错误中 |
| Outbox Sender 失败 | 测试 Sender 返回 retryable/permanent/uncertain | 指数退避或进入对应 DLQ/隔离态，不重复无界发送 | 平台限流被绕过、重复投递失控 |
| Collector 不可用 | 停止测试 Collector | 核心消息链路继续；SDK 有界缓存且不阻塞请求 | 内存持续增长或请求被遥测阻塞 |

每个场景都要记录时间线、镜像 digest、配置版本、故障范围、恢复时间、用户影响、审计/trace 证据和改进项。数据库与 Redis 故障必须在非生产或由对应系统负责人实施；仓库不提供会修改共享数据库状态的自动脚本。
