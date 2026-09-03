# 部署、可观测性与发布

## 本地最小部署

最小闭环使用一个 `-role all` 进程，加 PostgreSQL 和 Redis；需要 Artifact/Knowledge 时再启动 MinIO/Qdrant：

```bash
docker compose up -d postgres redis minio minio-init qdrant
./build.sh
./bin/trpc-migrate
./bin/trpc-service -role all -addr :8080
```

启用可观测栈：

```bash
docker compose --profile observability up -d
```

本地端到端验收可以执行：

```bash
./scripts/e2e-observability.sh
```

脚本使用固定 W3C Trace 上下文发送 `/chat`，从 Tempo 查询 HTTP 与 Session spans，检查 Collector 导出的租户级 inbound/run/reply metrics，并确认 Grafana Dashboard 与 Prometheus Alert Rule 已加载。验收结束后会停止本次应用进程和可观测容器，但不会删除数据卷。

端口：OTLP gRPC 4317、OTLP HTTP 4318、Prometheus 9090、Tempo 3200、Grafana 3000、MinIO Console 9001、Qdrant REST 6333。Grafana 本地默认账号为 `admin/admin`，只用于开发环境。

## 镜像

```bash
docker build -t trpc-agent-service:local .
docker run --rm trpc-agent-service:local -role worker
```

镜像采用 Go build stage 和非 root Alpine runtime，包含 `trpc-service` 与只执行 migration 的 `trpc-migrate`。`.env`、本地数据和构建产物不会进入镜像。

## Kubernetes

文件：

```text
deploy/kubernetes/platform.yaml
deploy/kubernetes/secret.example.yaml
deploy/kubernetes/migration-job.yaml
```

部署顺序：

1. 替换镜像地址；
2. 用 Secret Manager/External Secrets 生成 `trpc-agent-secrets`，不要直接应用示例值；
3. 应用 namespace、ConfigMap、ServiceAccount 和 NetworkPolicy；
4. 运行 migration Job，确认完成；
5. 应用六类 Deployment/Service/HPA/PDB；
6. 配置 Ingress，只公开 Gateway；Admin 通过内网和额外身份代理访问。

```bash
kubectl apply -f deploy/kubernetes/platform.yaml
kubectl apply -f /secure/generated-secret.yaml
kubectl apply -f deploy/kubernetes/migration-job.yaml
kubectl wait --for=condition=complete job/trpc-agent-migrate -n trpc-agent --timeout=180s
```

生产清单默认 Gateway/Worker 3 副本，Admin/Relay/Sender/Jobs 2 副本；HPA 上限分别为 20/50/20。实际生产应把 HPA 扩展到 Redis Stream lag、background pending、outbound pending 和 active run 等自定义指标。

## 灰度与回滚

镜像发布先更新 5% Worker，再更新 Gateway/Sender/Jobs。Agent 行为灰度不依赖镜像：创建 immutable revision，按 conversation pin 后发布。回滚只需把 App stable revision 指回旧 revision，已有 conversation 保持原 pin，新 conversation 使用回滚版本。

数据库 migration 只允许向前兼容：先扩 schema，再部署读写新字段，最后清理旧字段。禁止在同一发布中删除旧列。Backend 数据迁移使用平台状态机，切读后保留反向双写窗口。

## 备份与恢复

本地可以先运行不破坏现有应用数据的工具链演练：

```bash
./scripts/e2e-backup-restore.sh
```

脚本为 PostgreSQL 创建临时数据库，执行 `pg_dump`、删除、重建和 `pg_restore`；Redis RDB 会恢复到独立临时容器。它不会清空 `trpc_agent` 主数据库或现有 Redis 数据卷，结束后只清理本次演练资源。

- PostgreSQL：每日全量 + WAL/PITR，季度恢复到独立集群；
- Redis：AOF everysec + 副本，Session 的最终耐久事实可选 PostgreSQL；
- S3：版本化、生命周期、跨区域复制和对象锁按合规要求开启；
- Qdrant：snapshot 到对象存储，恢复后运行 Knowledge verification；
- Secret：轮换采用双凭据窗口，日志和 trace 做 DLP 扫描。

## 告警

最低告警集合：5xx、Runner p95、模型错误/429、Session backend p95、Redis Stream pending、background dead、outbound delivery failure、repair backlog、每日成本使用率、审计写失败、Pod 重启和 readiness 失败。
