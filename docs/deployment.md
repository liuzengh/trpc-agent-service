# 部署、可观测性与发布

本机日常测试使用[运行与升级手册](operations-runbook.md)。本篇侧重部署拓扑和生产差异，示例不是已经在真实集群执行过的操作。

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

脚本为自己的临时进程生成 HTTP 测试凭据，使用固定 W3C Trace 上下文发送 `/chat`，从 Tempo 查询 HTTP 与 Session spans，并检查 metrics、Dashboard 与 Alert Rule。结束时只停止本次应用进程，共享可观测容器保持运行，不删除数据卷。日常服务的 HTTP 入口默认关闭，见[安全配置](security-boundaries.md)。

端口：OTLP gRPC 4317、OTLP HTTP 4318、Prometheus 9090、Tempo 3200、Grafana 3000、MinIO Console 9001、Qdrant REST 6333。Grafana 本地默认账号为 `admin/admin`，只用于开发环境。

## 镜像

```bash
docker build -t trpc-agent-service:local .
docker run --rm trpc-agent-service:local -role worker
```

镜像采用 Go build stage 和非 root Alpine runtime，包含 `trpc-service` 与只执行 migration 的 `trpc-migrate`。`.env`、本地数据和构建产物不会进入镜像。

`.dockerignore` 同时排除 `.env.*` 和子目录 `*.env`，不要把真实配置写入 Dockerfile 或构建参数。如果本机联网必须通过 loopback HTTP 代理，普通构建容器的 `127.0.0.1` 并不是宿主机；在支持 host 网络的 Linux 构建环境，可以仅对构建过程使用宿主网络并传入已有代理变量：

```bash
docker build --network host --force-rm --pull=false \
  --build-arg HTTP_PROXY --build-arg HTTPS_PROXY --build-arg NO_PROXY \
  -t trpc-agent-service:0.2.0-rc.1 .
```

这不是生产容器使用 host 网络的建议，也不改变运行时网络策略；代理值不写入 Dockerfile，应用密钥不作为构建参数。禁网构建还要求依赖缓存已准备好，`go mod download` 失败不等同于业务代码编译失败。

## Kubernetes

文件：

```text
deploy/kubernetes/platform.yaml
deploy/kubernetes/secret.example.yaml
deploy/kubernetes/migration-job.yaml
```

部署顺序：

1. 替换镜像地址；
2. 用 Secret Manager/External Secrets 分别生成 `trpc-agent-{gateway,admin,relay,worker,sender,jobs,migration}-secrets`，不要直接应用示例值或复制同一份全集凭据；
3. 在 ConfigMap 中配置精确 secret grants；应用 namespace、ConfigMap、ServiceAccount 和 NetworkPolicy，并为入口代理与管理命名空间分别设置 `trpc-agent-access=gateway` / `admin` 标签；
4. 运行 migration Job，确认完成；
5. 应用六类 Deployment/Service/HPA/PDB；
6. 配置 Ingress，只公开 Gateway；Admin 通过内网和额外身份代理访问。

Gateway 默认只接受 IM 回调，不挂载 Admin 或同步 `/chat`，且不需要模型 API Key。需要 HTTP 异步调用时为它显式启用 `/inbound` 并注入受限调用方凭据。Admin 只有自身角色启用管理接口。仓库现已提供[SQL/Redis 权限生成器和角色网络模板](deployment-permissions.md)，但登录账号、密码、真实依赖标签和外连地址仍由部署者配置；隔离测试通过不等于已经应用到真实集群。

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

脚本仅创建三个带本次运行标签的独立容器：PostgreSQL、Redis 源和 Redis 恢复实例，均不连接网络或暴露端口。PostgreSQL 将合成数据 dump 后恢复到另一个新数据库；Redis 只导出合成测试键的 RDB，再读入恢复实例，不读取业务 Redis 全量数据。结束时核对容器 ID 和运行标签，只移除本次资源；不执行共享 Compose 的 start/stop/down。

它需要本机已缓存本仓库的 `postgres:16-alpine` 和 `redis:7-alpine`，不会自动拉取或升级镜像。默认验证的是备份恢复工具链。新增 `TEST_RECOVERY_DOCKER=1 go test -race -count=1 ./trpcservice/recovery` 使用所有平台迁移及合成业务数据，在独立 PostgreSQL 中验证 Inbox/Run/Outbox、检查点和发送状态恢复，见[业务恢复记录](validation/recovery-2026-09-06.md)。这仍不是实际业务库全量恢复、PITR 或灾难恢复时间目标达标证明。

- PostgreSQL：每日全量 + WAL/PITR，季度恢复到独立集群；
- Redis：AOF everysec + 副本，Session 的最终耐久事实可选 PostgreSQL；
- S3：版本化、生命周期、跨区域复制和对象锁按合规要求开启；
- Qdrant：snapshot 到对象存储，恢复后运行 Knowledge verification；
- Secret：轮换采用双凭据窗口，日志和 trace 做 DLP 扫描。

## 告警

已实现的积压、轮询、未知发送及采集失败规则、离线测试和多节点聚合方式见[监控说明](monitoring.md)。实际通知接收方尚未配置，不阻塞本地开发；下列完整生产告警集合仍需按基础设施和 SLO 补齐。

最低告警集合：5xx、Runner p95、模型错误/429、Session backend p95、Redis Stream pending、background dead、outbound delivery failure、repair backlog、每日成本使用率、审计写失败、Pod 重启和 readiness 失败。
