# Worker V1 NATS 声明

`streams.yaml` 是四个 Stream 的容量与持久化声明，`permissions.yaml` 是运行身份 ACL
的单一来源。`server.conf` 由以下命令生成，密码只保留环境变量引用：

```bash
go run ./services/channel-gateway/cmd/channel-gateway nats-config deploy/nats/permissions.yaml > deploy/nats/server.conf
```

| Stream | Subject | Retention | Publisher / durable |
| --- | --- | --- | --- |
| CHANNEL_ROUTES_V1 | control.channel-route.v1 | Limits | Control / channel-gateway-routes-v1 |
| RUN_REQUESTS_V1 | execution.run-requested.v1 | WorkQueue | Gateway / agent-worker-runs-v1 |
| RUNTIME_MANIFESTS_V1 | control.runtime-manifest.published.v1 | Limits | Control / worker-manifests-v1 |
| REPLY_INTENTS_V1 | execution.reply-intent.v1 | WorkQueue | Worker / channel-gateway-replies-v1 |

Reconciler 创建四个 durable。运行进程只绑定和验证，不创建 Stream 或 consumer。
每个 durable 使用 explicit ACK、DeliverAll、30 秒 AckWait、64 MaxAckPending 和无限次数
恢复投递；没有消息本地无限排队。共享 Worker database 的副本共享两条 Worker durable。

容量为 Route 64 MiB，其余各 256 MiB；message size 为 Route 16 KiB，其余各 1 MiB。
所有 Stream 采用 FileStorage、DiscardNew、禁 Delete/Purge、无 MaxAge/自动 GC；满容量时
PG Outbox 保留原始事实并等待重试，不淘汰未处理输入。2 分钟 broker dedup window 只是
传输优化，PG Receipt/Run/Completion/Final 的唯一性才是业务保证。离线恢复窗口由容量
和实际积压增长决定，不以“无 MaxAge”承诺无限存储；容量报警和源回填仍由 Workload 运维
接线执行，改保留策略前需核对持久源与回填验收。

Worker 仅能发布 Reply、查询自己的三条 Stream 配置、拉取并 ACK Run/Manifest durable；
Gateway 仅发布 Run、消费 Route/Reply；Control 仅发布 Route/Manifest；reconciler 是唯一
拓扑管理身份。各角色 reply inbox 精确隔离。运行服务没有 `$JS.API.>` 管理权限。

实测 ACL 门禁：

```bash
# 环境提供专用 broker、四个 NATS_*_PASSWORD 及 GATEWAY_TEST_TOPOLOGY_FILE。
go test -count=1 -v ./services/channel-gateway/internal/infra/nats -run 'Test(Broker|WorkerBroker)PermissionsIntegration'
```

测试覆盖允许的真实 PubAck、Run/Manifest pull+ACK、Gateway Reply pull+ACK，及跨 owner
发布、跨 durable 消费、拓扑写入、错误凭据的拒绝。
