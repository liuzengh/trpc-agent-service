# 租户级 Runner 并发配额

系统在所有 Worker 节点之间使用 Redis 过期信号量限制每个租户同时执行的 Runner 数量，
防止一个突发租户占满模型、Tool 和 Worker 资源。配额属于不可变租户配置：

```yaml
tenants:
  - tenant_id: demo
    runtime:
      max_concurrent_runs: 8
```

`max_concurrent_runs` 可配置为 1–256。旧配置没有该字段时使用保守默认值 8，不要求重写
历史版本。Admin 发布新版本后，新请求使用新配额；正在执行的请求继续完成：

- 增大配额后，等待请求可以立即竞争新增 permit；
- 缩小配额不会取消旧请求，但在活跃数降到新上限以下前不会接纳新请求；
- permit 的成员由 tenant、Inbox ID 和 claim token 哈希生成，不把这些原始标识写入 Redis key；
- Worker 正常完成会精确释放自己的成员，旧 Worker 的延迟释放不能删除新 claim 的 permit；
- Worker 崩溃后 permit 按 TTL 过期，其他节点可以有界恢复；执行期间按 Inbox/session lease
  节奏续租；
- Redis 不可用、返回异常或 permit 丢失时 fail closed，Runner 不会绕过共享配额启动。

等待 permit 时 Worker 会保持并在需要时续租 PostgreSQL Inbox claim，并轮询共享取消意图。
等待采用 500ms 有界窗口；仍无容量时通过专用 Inbox `Defer` CAS 无损让出 Worker，稍后由
PostgreSQL poller 恢复。`Defer` 不增加 attempt，因此健康的容量背压不会把消息推入 DLQ，
也不会让一个超额租户用等待 goroutine 饿死其他租户。进程退出或上下文取消时仍按原 Inbox
恢复机制接管。不同租户使用不同 Redis sorted set，因此同名 request/session 不会互相占用配额。

指标中 `operation=tenant_run_quota` 记录准入等待耗时和失败。它与
`agent_queue_depth`、活跃 Runner、模型延迟和 Provider 限额一起用于容量判断。当前 Kubernetes
HPA 仍使用 CPU/内存基线；将这些指标接入集群 custom metrics adapter 属于后续独立部署增量。

本地/CI 的真实 Redis 验证：

```bash
GOCACHE=/private/tmp/trpc-agent-service-cache ./scripts/redis_integration_test.sh
```

脚本创建无数据卷的临时 Redis 容器，覆盖两节点竞争、动态缩放、租户隔离、精确释放和崩溃
过期恢复，结束后只清理该临时容器。
