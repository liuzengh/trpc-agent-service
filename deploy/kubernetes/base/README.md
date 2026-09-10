# Kubernetes 验收参考（非发布资产）

此目录以 `gateway` 与 `worker` 两个现有二进制 role 为例，展示生产环境应如何
映射既有的 readiness、drain、网络与最小权限契约。它仅供验收者审阅设计，不是
Kubernetes 发布资产，也不属于仓库的验收门禁。

- 如需在其他项目落地，环境 overlay 应将
  `ghcr.io/liuzengh/trpc-agent-service:replace-me` 替换为已审核的不可变镜像
  digest，并提供 `trpc-service-runtime` ConfigMap 与
  `trpc-service-runtime-secrets` Secret/CSI projection。
- `worker` 没有 Service，且通过 `trpc-worker-deny-ingress` 拒绝 Pod ingress；
  它只从 Redis broker 消费工作。依赖出口策略应由环境 overlay 按 PostgreSQL、Redis、
  ObjectStore、模型、MCP 与 DNS 的实际地址显式放行。
- 两个 Deployment 都以无 shell 的 `prestop` role 对 PID 1 发送 `SIGTERM`，等待
  readiness 进入 draining；120 秒 termination grace 覆盖现有有界 shutdown/drain。
- 不在这里定义 `schema-migrate` Job。若其他项目以 Helm 落地，schema migration
  应由唯一的 `pre-install`/`pre-upgrade` hook 承载，固定镜像 digest 与
  expected-current/target，不能同时维护裸 Job 和 chart hook 两条发布路径。

可选的静态阅读/渲染命令（不等于验收）：

```bash
kubectl kustomize deploy/kubernetes/base
```
