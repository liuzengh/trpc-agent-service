# Kind 本地 Kubernetes 验收环境

本目录包含基于 [Kind](https://kind.sigs.k8s.io/) 运行本地自动化 Kubernetes 端到端测试所需的 Overlays 与 Mock 依赖。

## 运行 E2E 测试套件

从仓库根目录执行：

```bash
./scripts/kind-e2e.sh
```

- 自动创建并配置 `trpc-agent` 本地 Kind 集群；
- 构建本地测试镜像，拉起测试中间件依赖（Postgres、Redis、Redpanda、MinIO 及 Mock OpenAI），并验证服务 Pod 健康度；
- 测试证据与状态快照记录于 `deploy/kubernetes/kind/evidence/`；
- 若本地镜像已完成构建与加载，可使用 `KIND_SKIP_BUILD=1 ./scripts/kind-e2e.sh` 加速重复执行。
