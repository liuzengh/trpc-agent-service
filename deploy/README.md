# 本地基础设施与部署基线

`compose.yaml` 启动 PostgreSQL、Redis、Redpanda、MinIO 和 OpenTelemetry Collector；它刻意不启动应用进程，因为应用的模型与 IM 凭证必须通过受控 Secret 注入，不能写进 Compose 或仓库。

## 本地启动

```bash
# 从仓库根目录执行
cp deploy/.env.example deploy/.env
# 将 POSTGRES_PASSWORD 改成仅限本机开发的随机值
docker compose --env-file deploy/.env -f deploy/compose.yaml up -d
docker compose --env-file deploy/.env -f deploy/compose.yaml ps
```

容器内访问 Kafka 使用 `redpanda:29092`；本地/主机访问使用 `127.0.0.1:19092`（对应 `data/platform.env` 中的 `KAFKA_BROKERS=localhost:19092`）。若本地镜像源限制无法拉取 Redpanda 镜像，可直接启动原生 Apache Kafka（KRaft 模式）：

```bash
docker run -d --name trpc-kafka -p 127.0.0.1:19092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  apache/kafka:3.7.0
```

MinIO API 为 `127.0.0.1:9000`，`minio-init` 会创建 `trpc-artifacts` Bucket。停止开发环境：

```bash
docker compose --env-file deploy/.env -f deploy/compose.yaml down
```

除非确认不再需要数据库和 Redis 中的开发数据，否则不要附加 `--volumes`。

## 应用上线前的必要输入

组合根已从不可变租户配置装配模型、工具与平台存储。部署平台必须注入 PostgreSQL/Redis/Kafka 连接、平台 Provider Catalog 引用的模型 API key、可选 `S3_CONFIG_JSON`、binding 级 IM 凭证以及 OTel 导出端点。模型 endpoint 与 `api_key_ref` 属于平台受管 Catalog；租户快照只保存 `provider_id`、model name、`tools.allowed` 与存储 driver。API key、S3 access/secret key 和 IM token 只通过平台 allowlist 中的 `env:` 引用解析。

可观测性只维护一套 OTel Provider。Kubernetes Deployment 默认把 Trace/Metric 经 `OTEL_EXPORTER_OTLP_ENDPOINT` 发往 Collector，仓库 Collector 已在 `:9464` 导出 Prometheus，因此生产环境通常不再启用服务自身的 `PROMETHEUS_ENABLED=true`。需要 Langfuse 时同时注入 `LANGFUSE_PUBLIC_KEY`、`LANGFUSE_SECRET_KEY`、`LANGFUSE_HOST`；框架原生 Langfuse exporter 会在同一 TraceProvider 上增加出口，不需要第二套埋点。Langfuse observation 可以包含模型 Prompt/Output，应按部署的数据治理要求显式启用。
生产组合根还要求 `AUDIT_HMAC_KEY`（至少 32 个随机字节），用于对话正文和消息摘要的不可逆 keyed digest；密钥只能由 Secret 平台注入。轮换会改变后续摘要值，因此应记录轮换时间并保留审计关联元数据，禁止把该密钥写入配置快照。


默认 `configs/platform.example.json` 使用 PostgreSQL Artifact；切换 S3 时参考 `configs/platform.s3.example.json`，并令 `S3_CONFIG_JSON` 包含 endpoint、region、bucket、access_key、secret_key。S3-compatible 服务必须支持 path-style object URL 与 AWS SigV4。

## 容器镜像

仓库根目录 `Dockerfile` 会在独立 Node 阶段构建控制台，再用 Go 1.24 构建静态二进制，最终镜像基于 distroless 并以 nonroot 运行：

```bash
docker build -t trpc-agent-service:local .
```

运行时只挂载配置文件并从 Secret 注入环境变量；镜像内不包含配置密钥。

## 数据库迁移

服务副本默认不执行 schema migration，避免多副本使用平台角色并发变更数据库。开发环境可在 deploy/.env 设 AUTO_MIGRATE=true；生产发布先用同一镜像执行 deploy/kubernetes/migration-job.yaml 的 migrate 子命令，Job 成功后再更新 Deployment。迁移仅允许向前执行，失败时停止发布，不要让服务副本重试或回滚历史迁移。

## Kubernetes

`deploy/kubernetes/` 提供 Namespace、ConfigMap、Deployment、Service、PDB、HPA 与 NetworkPolicy。Deployment 已启用 non-root、只读根文件系统、全部 capability drop、资源限制和 `/healthz`/`/readyz` 探针；HPA 以 CPU 70% 为目标，保持 2–10 个副本。

```bash
# 先用 ExternalSecret/密钥平台创建 trpc-agent-service-secrets。
# secret.example.yaml 只能作为字段清单，禁止原样部署。
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/configmap.yaml
kubectl apply -f deploy/kubernetes/migration-job.yaml
kubectl wait --for=condition=complete job/trpc-agent-service-migrate -n trpc-agent --timeout=10m
kubectl apply -f deploy/kubernetes/gateway-deployment.yaml
kubectl apply -f deploy/kubernetes/worker-deployment.yaml
kubectl apply -f deploy/kubernetes/service.yaml
# Prometheus Operator 已部署时再应用告警规则。
kubectl apply -f deploy/kubernetes/prometheus-rule.yaml
kubectl apply -f deploy/kubernetes/pdb.yaml
kubectl apply -f deploy/kubernetes/hpa.yaml
kubectl apply -f deploy/kubernetes/networkpolicy.yaml
```

上线前必须替换 Deployment 镜像地址、ConfigMap 的租户样例、Secret 后端地址和登录回调域名。`.github/workflows/quality-gate.yml` 会构建镜像、解析全部正式 manifest，并在隔离容器中执行 PostgreSQL/Kafka/S3 与浏览器 E2E 门禁。

## 受控运维演练

真实 Kubernetes、备份恢复与密钥轮换必须在拥有相应凭据的受控 Runner 上执行；仓库不会伪造这些环境的通过证据。

```bash
# 集群部署完成后的只读 smoke。
KUBE_CONTEXT=production-cluster scripts/kubernetes-smoke.sh

# 三个变量必须是已审核、绝对路径且可执行的包装器；参数、凭据通过 Runner 环境注入。
BACKUP_COMMAND=/opt/trpc-ops/backup.sh \
RESTORE_COMMAND=/opt/trpc-ops/restore.sh \
VERIFY_COMMAND=/opt/trpc-ops/verify-restored-data.sh \
scripts/backup-restore-smoke.sh

SECRET_ROTATION_COMMAND=/opt/trpc-ops/rotate-secret.sh \
KUBE_CONTEXT=production-cluster \
scripts/secret-rotation-smoke.sh
```

包装器不得打印密钥；backup 包装器必须仅在标准输出输出恢复点标识。脚本拒绝 shell 片段、相对路径和不可执行文件，避免 CI 环境变量被当作代码解释。

## Observability and alerting

The service propagates W3C `traceparent` from HTTP ingress to Kafka and into the Worker/Runtime path. `http.server.request`, `agent.runtime.handle`, `platform.store.operation`, `agent.tool.call`, and `channel.sender.send` therefore share one trace where a remote parent is supplied. Metrics are emitted through OTLP/HTTP; the bundled local Collector exposes Prometheus metrics at `127.0.0.1:9464/metrics`.

Telemetry deliberately contains only stable route, status, tenant, channel, binding, backend, and operation attributes. It excludes request bodies, query strings, cookies, user IDs, session keys, credentials, SQL, provider receipts, and model/tool arguments. `deploy/kubernetes/prometheus-rule.yaml` requires a Prometheus Operator and alerts on HTTP 5xx rate, channel delivery failures, durable Store failures, and Runtime execution failures.
