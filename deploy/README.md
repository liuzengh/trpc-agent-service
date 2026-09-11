# 部署与运维指南

本项目支持两种部署形态：
1. **本地 / 边缘轻量部署**：使用 Docker Compose 启动中间件基础设施，应用进程本地直接运行；
2. **Kubernetes 云原生生产部署**：采用职责分离架构（Gateway、Channel Ingress、Worker 独立弹性伸缩），配合持久化存储与安全隔离策略。

---

## 一、系统基础设施要求

部署前请确保具备以下中间件或等效云服务：

| 组件 | 推荐版本 | 作用 | 说明 |
| :--- | :--- | :--- | :--- |
| **PostgreSQL** | 16+ | 元数据与业务存储 | 存储租户、Agent、Session、会话状态与审计日志（需启用 `vector` 扩展用于知识库） |
| **Redis** | 7.0+ | 低延迟共享状态 | 消息幂等快路径、限流/用量协调、实时协作；租户也可选择 Redis Session / Memory Backend。Session 执行排他 Lease 本身由 PostgreSQL 仲裁 |
| **Kafka / Redpanda** | 3.x+ / 23.x+ | 异步任务消息总线 | 解耦 IM 消息接收、Agent 规划与工具执行 |
| **S3 / MinIO** | S3 API 兼容 | Artifact 对象存储 | 存储 Agent/Tool 生成的 Artifact 与出站文件版本（可选；Artifact 也可使用 PostgreSQL） |
| **OpenTelemetry Collector** | 0.90+ | 可观测性导出 | 统一收集并导出 Traces、Metrics 与 Logs |

---

## 二、本地开发基础设施（Docker Compose）

用于在本地一键拉起开发所需的全部中间件：

```bash
# 1. 复制环境变量配置文件
cp deploy/.env.example deploy/.env

# 2. 根据需要修改 deploy/.env 中的密码（如 POSTGRES_PASSWORD）

# 3. 启动中间件基础设施
docker compose --env-file deploy/.env -f deploy/compose.yaml up -d

# 4. 检查容器运行状态
docker compose --env-file deploy/.env -f deploy/compose.yaml ps
```

- **访问端口映射**：
  - PostgreSQL: `127.0.0.1:5432`
  - Redis: `127.0.0.1:6379`
  - Kafka (Redpanda): `127.0.0.1:19092`（容器内部访问为 `redpanda:29092`）
  - MinIO: `127.0.0.1:9000`（控制台: `127.0.0.1:9001`）
  - OTel Prometheus 导出端点: `127.0.0.1:9464/metrics`

- **停止本地中间件**：
```bash
docker compose --env-file deploy/.env -f deploy/compose.yaml down
```

### 最小可运行方案

最小演示部署使用单个 `trpc-service` 进程，保持默认 `SERVICE_ROLE=all`，由同一进程承担 Gateway、Channel 与 Worker；PostgreSQL、Redis、Redpanda 和 MinIO 可由 Compose 提供。该模式用于本地验证完整链路，不代表生产高可用拓扑。

---

## 三、生产环境配置与 Secret 注入

应用通过**环境变量**和**平台配置文件**（`configs/platform.json`）进行组合装配，敏感凭证禁止明文写入代码或配置快照。

### 1. 核心环境变量清单

| 环境变量 | 必填 | 默认值 / 格式 | 说明 |
| :--- | :---: | :--- | :--- |
| `DATABASE_URL` | 是 | `postgres://user:pwd@host:5432/trpc_agent?sslmode=disable` | PostgreSQL 连接串 |
| `REDIS_ADDR` | 是 | `host:6379` | Redis 连接地址 |
| `KAFKA_BROKERS` | 是 | `host:9092` | Kafka Broker 地址列表（逗号分隔） |
| `AUDIT_HMAC_KEY` | 是 | 32+ 字节随机字符串 | 对话正文与隐私消息摘要加密 HMAC Key |
| `AUTO_MIGRATE` | 否 | `false` | 仅本地开发可设为 `true`；生产建议通过迁移 Job 独立执行 |
| `S3_CONFIG_JSON` | 否 | JSON 字符串 | S3 对象存储配置（包含 endpoint、bucket、access_key、secret_key 等） |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 否 | `http://host:4318` | OpenTelemetry OTLP 导出地址 |
| `LANGFUSE_PUBLIC_KEY` | 否 | `pk-lf-...` | 可选的大模型链路追踪 Langfuse 公钥 |
| `LANGFUSE_SECRET_KEY` | 否 | `sk-lf-...` | 可选的大模型链路追踪 Langfuse 私钥 |
| `LANGFUSE_HOST` | 否 | `https://cloud.langfuse.com` | Langfuse 服务地址 |

### 2. 模型与通道凭证

大模型 API 密钥与 IM 机器人 Token 统一采用外部安全注入：
- **模型密钥**：在平台配置目录中定义 Provider Catalog，通过 `api_key_ref`（如 `env:OPENAI_API_KEY`、`env:DEEPSEEK_API_KEY`）引用系统环境变量；
- **IM 通道凭据**：在企业微信/飞书绑定配置中，通过受控凭据映射注入各 AppSecret / Token。

---

## 四、容器镜像构建

项目提供多阶段构建的官方 `Dockerfile`，集成 Node 前端静态打包与 Go 1.24+ 静态二进制链接，运行时基于极简的安全 distroless 基础镜像以 non-root 用户运行：

```bash
docker build -t trpc-agent-service:latest .
```

---

## 五、Kubernetes 生产部署

`deploy/kubernetes/` 目录提供了完整的云原生分角色部署编排清单：

### 1. 组件角色划分
- **Gateway (`gateway-deployment.yaml`)**：负责外部 HTTP API、SSE 长连接、控制台页面与前端交互，支持根据 CPU 使用率自动横向伸缩（HPA）；
- **Channel (`channel-deployment.yaml`)**：负责企业微信智能机器人 WebSocket、飞书长连接以及 Telegram `getUpdates` 长轮询等外部 IM Connector 生命周期；
- **Worker (`worker-deployment.yaml`)**：消费 Kafka 队列，执行多租户 Agent 推理、Tool / MCP 调用、Knowledge 检索与会话持久化；
- **Migrate Job (`migration-job.yaml`)**：在发布新版本时独立前置执行数据库 Schema 迁移。

生产推荐至少将 `gateway / channel / worker` 分角色部署：Gateway 与 Worker 可水平扩容；Channel 是否能多副本取决于 Provider 的连接所有权语义，例如同一企业微信智能机器人 WebSocket 连接不能由多个实例互相抢占。所有角色共享 PostgreSQL / Redis / Kafka 和租户选择的数据 Backend，因此不需要 Sticky Session。

### 2. 发布步骤

```bash
# 1. 创建命名空间
kubectl apply -f deploy/kubernetes/namespace.yaml

# 2. 注入外部 Secret（请参考 deploy/kubernetes/secret.example.yaml 准备真实凭据）
kubectl apply -f deploy/kubernetes/secret.yaml

# 3. 应用基础配置
kubectl apply -f deploy/kubernetes/configmap.yaml

# 4. 执行数据库前置迁移 Job 并等待完成
kubectl apply -f deploy/kubernetes/migration-job.yaml
kubectl wait --for=condition=complete job/trpc-agent-service-migrate -n trpc-agent --timeout=10m

# 5. 分布式部署各业务角色服务
kubectl apply -f deploy/kubernetes/gateway-deployment.yaml
kubectl apply -f deploy/kubernetes/channel-deployment.yaml
kubectl apply -f deploy/kubernetes/worker-deployment.yaml

# 6. 配置 Service 暴露与网络/可用性策略
kubectl apply -f deploy/kubernetes/service.yaml
kubectl apply -f deploy/kubernetes/pdb.yaml
kubectl apply -f deploy/kubernetes/hpa.yaml
kubectl apply -f deploy/kubernetes/networkpolicy.yaml

# 7. 应用 Prometheus 告警规则（依赖 Prometheus Operator）
kubectl apply -f deploy/kubernetes/prometheus-rule.yaml
```

---

## 六、生产运维与健康巡检

针对生产环境，仓库在 `scripts/` 下提供了非侵入式冒烟巡检与灾备演练脚本：

```bash
# 1. 部署健康状态冒烟检查（只读检查 Pod 状态与探针）
KUBE_CONTEXT=production-cluster ./scripts/kubernetes-smoke.sh

# 2. 备份恢复演练（需在具备相应运维凭据的受控执行节点运行）
BACKUP_COMMAND=/opt/trpc-ops/backup.sh \
RESTORE_COMMAND=/opt/trpc-ops/restore.sh \
VERIFY_COMMAND=/opt/trpc-ops/verify-restored-data.sh \
./scripts/backup-restore-smoke.sh

# 3. 密钥动态轮转演练
SECRET_ROTATION_COMMAND=/opt/trpc-ops/rotate-secret.sh \
KUBE_CONTEXT=production-cluster \
./scripts/secret-rotation-smoke.sh
```

---

## 七、可观测性与告警指标

服务全面支持 OpenTelemetry 标准，并在链路传递中透传 W3C `traceparent`：
- **分布式追踪**：HTTP Ingress → Kafka 投递 → Worker 消费 → Agent 规划 → Tool 调用 → 渠道下发共享同一 Trace ID；
- **指标与监控**：通过 OTLP 导出，或者由配套 Collector 暴露 Prometheus 端点；
- **告警策略**：`deploy/kubernetes/prometheus-rule.yaml` 覆盖了 HTTP 5xx 错误率、Channel 下发失败、存储写入失败、Runtime 运行异常等核心生产告警。

---

## 八、容量评估与压测

容量规划不使用“每节点固定能跑多少 Session”这种拍脑袋常量，而是用真实模型延迟、Token 量和依赖 QPS 做测量。仓库提供 `tests/capacity/capacity-load.mjs`，会走真实登录、Chat 入队、SSE 完成链路，并可同时抓取每个节点的 Prometheus 指标。

### 1. Worker 并发上限

单进程 `WORKER_CONCURRENCY` 默认是 **4**，配置范围为 **1–64**。理论执行槽位为：

```text
worker_slots = worker_replicas × WORKER_CONCURRENCY
effective_parallelism <= min(worker_slots, Kafka partitions)
```

若峰值到达率为 `λ` 请求/秒，实测 Agent 执行 P95 为 `T95` 秒，则按 Little's Law 先估算 `λ × T95` 个并发执行，再预留 20%–30% 空间。租户自身的 `max_concurrent_runs` 会进一步限制单租户占用，不能把节点槽位全部算给一个租户。

### 2. Token、SQL、Redis 与 IM 峰值

- **模型容量/成本**：`峰值请求率 × 平均 total_tokens` 得到 Token/s；按实际 Provider 分解后的 prompt / cached prompt / completion 计算成本，不使用配置模型名代替真实 Failover Provider。
- **PostgreSQL**：压测前后比较 `platform.store.operations`、连接池 `in_use/max/waits/wait_duration`，用“每成功请求新增多少 Store 操作”推导峰值 SQL QPS。
- **Redis**：关注连接池 wait/timeout，以及幂等、限流、实时流量的峰值；Redis 故障时 PostgreSQL execution dedup 仍是重复执行的持久兜底。
- **Kafka**：持续观察 `messaging.kafka.consumer.lag` 与 partition 数；扩 Worker 但不扩 partition 时，并行度不会无限提升。
- **IM Outbox**：观察 `outbox.pending.events`、`outbox.oldest.age.seconds` 和 `channel.delivery.*`；如果模型吞吐正常但 Outbox backlog 增长，瓶颈在渠道频控/发送而不是 Worker。

### 3. 基准命令

```bash
node tests/capacity/capacity-load.mjs \
  --base http://127.0.0.1:8080 \
  --tenant example \
  --app support \
  --username capacity-user \
  --password 'replace-me' \
  --concurrency 8 \
  --requests 200 \
  --warmup 20 \
  --metrics-url http://127.0.0.1:8080/metrics \
  --output /tmp/capacity-report.json
```

正式容量结论至少记录：成功率、enqueue / 首 Token / 端到端 P50/P95/P99、平均与 P95 Token、Worker active/slots 峰值、Kafka Lag、SQL/Redis pool wait、Outbox backlog。只有这些指标同时稳定，才能把该并发档位作为节点容量基线。
