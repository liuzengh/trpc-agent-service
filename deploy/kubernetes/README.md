# Kubernetes 生产部署

这些 Manifest 将无状态 Gateway 和 Worker 分成两个独立角色。Gateway Pod 承载 HTTP、Admin API 和 Channel 回调入口，只把已经完成 Inbox claim 的请求写入 Redis Streams。Worker Pod 负责 Redis 消费、Inbox 恢复、Runner 执行、Outbox 投递、存储迁移任务和审计保留。所有持久状态均保存在 PostgreSQL/Redis，因此两类 Pod 都不需要 sticky session。

可执行文件支持 `--role gateway`、`--role worker` 和向后兼容的 `--role all`。Gateway 模式不会构建 Runtime Bundle，也不会启动队列消费者；Worker 模式不会监听 HTTP 端口或挂载 Channel/Admin 路由。Outbox 和后台维护循环目前仍随 Worker 运行，拆分为独立角色属于后续增强。

## 必需输入

先基于确定的 commit 构建并推送镜像，再替换两个 Kustomization 中的 `trpc-agent-service:replace-me`。通过外部密钥管理系统或经审批的命令，在 `trpc-agent` namespace 中创建 `trpc-agent-secrets`，其中必须包含：

- `postgres-dsn`
- `redis-url`
- `deepseek-api-key`
- `admin-tokens`

发布配置使用 `provider: vault` 或 `provider: kms` 时，再成对提供可选的
`vault-endpoint` / `vault-token` 或 `kms-endpoint` / `kms-token`。未使用外部 Provider 时
这些 key 可以不存在；只提供 endpoint 或 token 之一会使进程 fail-closed，避免静默回退。

禁止提交渲染后的 Secret。示例 HTTP binding 默认关闭；首次接入生产流量前，应使用经过审核的租户配置和明确的 Channel SecretRef 替换 bootstrap ConfigMap。租户一旦存在已发布配置版本，PostgreSQL 就是唯一事实源，配置文件只作为初始种子。

## 有序发布

```bash
kubectl apply -f deploy/kubernetes/base/namespace.yaml
# 在此创建或同步 trpc-agent-secrets
kubectl apply -k deploy/kubernetes/migration
kubectl -n trpc-agent wait --for=condition=complete job/trpc-agent-migrate --timeout=10m
kubectl apply -k deploy/kubernetes/base
kubectl -n trpc-agent rollout status deployment/trpc-agent-gateway --timeout=10m
kubectl -n trpc-agent rollout status deployment/trpc-agent-worker --timeout=10m
```

数据库 migration 与 Deployment 有意分离。下一次发布时只删除已经完成的 migration Job 对象并重新应用，不得删除 PostgreSQL、Redis 或应用数据卷。迁移事务使用 PostgreSQL advisory lock，避免多个任务并发修改 schema。

Ingress Controller 所在 namespace 必须带有 `trpc-agent.io/ingress=allowed` 标签。默认 Service 类型为 ClusterIP；TLS、WAF/IP 白名单和公网 IM 回调路由由集群入口层负责。应用使用 `/healthz` 检查进程存活，使用 `/readyz` 对 PostgreSQL/Redis 做有界就绪检查。

Gateway 和 Worker 分别配置 HPA，回调流量与 Runner 负载可以独立扩容。CPU/内存指标只是初始基线，无法反映模型供应商饱和或队列等待时间。完成容量测试并确定阈值后，生产环境应通过自定义指标适配器接入 `agent_queue_depth`、模型延迟和 Outbox backlog。

发布镜像前先渲染并校验两个 Kustomization：

```bash
./scripts/kubernetes_validate.sh
```

容量评估、受控故障注入和发布门禁分别见[容量测试](../../docs/capacity.md)、[故障演练](../../docs/fault-drills.md)和[生产验收](../../docs/production-acceptance.md)。真实集群的 admission dry-run 与 CRD、准入控制和安全策略相关，必须连接目标集群 API Server 后才能验证。

## 可复现的本地集群 Demo

`demo` overlay 在专用 kind 集群中运行生产形态的 Gateway/Worker 拆分拓扑：Gateway 和 Worker 各三个副本，PostgreSQL/Redis 使用持久化 StatefulSet，同时部署协议兼容的 Mock Model 和 OpenTelemetry Collector。Mock Model 只返回固定的合成结果，各组件均不记录请求正文。

创建或选择专用集群后执行：

```bash
kind create cluster --name trpc-agent-demo --wait 180s
./scripts/kubernetes_acceptance.sh --run
```

设置 `TRPC_AGENT_K8S_CREATE_KIND=1` 后，脚本可以在目标 kind 集群不存在时自动创建。对于非 kind context，只有 `TRPC_AGENT_K8S_CONFIRM_CONTEXT` 与当前 context 完全一致时脚本才允许运行。

脚本会验证 Gateway → Redis → Worker → Model → PostgreSQL 完整链路、有界回调负载、单 Pod 恢复、依赖故障、Sender 重试/DLQ、PDB/HPA、滚动升级、回滚、配置持久性和 PVC 保留。它只缩放名称明确的 Demo 资源，不会删除 namespace、StatefulSet、PVC、volume 或数据库。凭据在权限为 0600 的临时文件中生成，只保存到 Kubernetes Secret；验收报告仅记录脱敏摘要。

在专用 kind context 中，脚本会安装固定版本的 Metrics Server v0.8.0，并要求两个 HPA 的 `ScalingActive=True`。仅用于 kind 的 kubelet TLS 例外不会应用到经确认的非 kind context；非 kind 集群必须预先提供可用的资源指标 API。

没有 Kubernetes 集群时，可以只验证 Manifest 结构以及“无内联 Secret”边界：

```bash
./scripts/kubernetes_acceptance.sh --validate
```
