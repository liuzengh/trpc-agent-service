# 部署与运维

部署事实来自 `Dockerfile`、`compose.yaml`、`compose.*-e2e.yaml`、`deploy/kubernetes`、`deploy/otel`、`admin-ui/Dockerfile` 和 `.github/workflows`。应用角色启动时执行 PostgreSQL schema migration；Kubernetes 发布通过 Kustomize overlay 完成。

验证边界：本文件描述已实现的 Compose/Kustomize 拓扑。本机 Compose、真实企业微信/飞书文本收发和本地 Jaeger/Prometheus/Grafana 已在 2026-09-09 外部验收；真实生产集群、HA backend、SecretProvider、Ingress 或发布恢复仍未验证。证据见 [`docs/acceptance.md`](acceptance.md) 和 [`acceptance-screenshots`](acceptance-screenshots/acceptance-status.png)。

## 本地最小运行

使用仓库提供的开发变量启动 PostgreSQL、Redis、Qdrant、Gateway、单一 Channel Adapter owner 和 Worker：

```bash
docker compose --env-file .env.example up -d --build --wait postgres redis qdrant gateway channel worker-1 worker-2
curl -fsS http://127.0.0.1:8080/readyz
```

`.env.example` 中的密码、token 和 endpoint 只适合 disposable/local 环境；生产不得直接复用。Admin UI 和可观测性是可选的本地组件：

```bash
docker compose --env-file .env.example up -d --build --wait admin-ui otel-collector jaeger prometheus grafana
```

本地 Compose 实际包含：PostgreSQL 16、Redis 7（AOF）、Qdrant 1.16、OTel Collector 0.111、Jaeger 1.57、Gateway、单一 Channel owner、两个显式 Worker 和 Admin UI。Gateway 端口默认 `127.0.0.1:8080`，Admin UI 默认 `127.0.0.1:4173`；数据库、Redis、Qdrant 和观测端口也绑定 loopback。

## Docker Quick Start Golden Path

验收人 clone 仓库后只需安装 Docker 和 Docker Compose；脚本会把 `cmd/deployment-e2e` 编译到 `Dockerfile.quickstart` 镜像内，不要求本机安装 Go，也不需要真实模型或 IM 凭据：

```bash
# Linux / macOS / Git Bash
./scripts/quickstart.sh

# Windows PowerShell
.\scripts\quickstart.ps1
```

两个脚本使用同一组 Compose 文件：`compose.yaml`、`compose.deployment-e2e.yaml` 和 `compose.quickstart.yaml`。后者仅增加一次性 `quickstart` runner；依赖启动后，runner 复用现有 `cmd/deployment-e2e` 完成 Tenant/App/Config 创建、临时 Credential 签发、`/v1/chat/completions` 请求和 durable execution `SUCCEEDED` 校验。默认 Worker 使用现有 deterministic E2E model。成功输出为 `Golden Path PASSED`；脚本只清理名为 `trpc-agent-service-quickstart` 的 Compose 项目及其 disposable 卷。

默认 Quick Start 只验证 HTTP deterministic Golden Path，不创建或启用真实企业微信/飞书 binding，因此不需要 IM 内部加密密钥。

## IM 流式与卡片回复

IM 回复模式由 `TRPC_AGENT_SERVICE_IM_REPLY_MODE` 控制，取值为 `text`（默认）、`stream` 或 `card`。`stream` 将增量事件按序写入 `reply_outbox`，重试或重启时从已持久化的完整快照继续，并复用 Provider 已发送消息进行更新；`card` 发送原生卡片，正文和状态由统一 Reply 模型生成。WeCom 依赖回调请求上下文，Feishu 依赖同一消息的更新接口；两者均受平台响应窗口、消息长度和频率限制约束。Provider 不支持对应能力时，Reply Sender 将其记录为不可重试的投递失败，不降级为另一种消息类型。

## 模型附件能力

`AppConfig.Model.AttachmentCapabilities` 是模型附件能力的显式 allowlist：

```json
{"image":true,"audio":false,"file":false}
```

默认值全部为 `false`，因为 OpenAI-compatible 只代表协议形状，不代表目标模型支持 image、audio 或 generic file。Worker 在 `artifactHydratingModel` 调用实际 Model Provider 前校验能力；ArtifactRef 的 MIME 元数据只用于区分 image/audio/file，不读取内容做 OCR、解析或文本提取。未声明支持的附件返回不可重试的 `unsupported model attachment/file capability`，不会转成 Provider 5xx 或自动 fallback。

## 本机外部验收结果

本次使用真实企业微信/飞书账号和真实模型完成文本消息收发：两端客户端均收到回复，并保存了客户端截图；对应 execution 为 `SUCCEEDED`，`reply_outbox` 为 `SENT` 且有 Provider receipt。Jaeger Trace、Prometheus target/指标和 Grafana 运行面板也已实测。截图证据集中在 [`acceptance-screenshots`](acceptance-screenshots/acceptance-status.png)。

该结果只覆盖本机 Compose 和本次账号/网络；Kubernetes Pods/HPA、生产 HA、生产容量、重投/撤回和生产 SecretProvider 不在本次验收范围。

## Compose 拓扑和启动依赖

- Gateway 依赖 PostgreSQL/Redis healthy，暴露 `/readyz`，运行 HTTP ingress、Admin API 和 dispatch relay；不运行 IM 长连接。
- Channel 依赖 PostgreSQL/Redis healthy，暴露仅供健康检查的 `/readyz`，运行 WeCom/Feishu adapters；它把 `ChannelInput` 提交到现有 Gateway Admission/outbox 链路。当前没有 binding lease/sharding，必须保持一个 Channel owner。
- 每个 Worker 依赖 PostgreSQL/Redis/Qdrant healthy，暴露 `/readyz`，运行 Consumer、Runner、migration、cleanup 和 heartbeat。Worker ID 明确为 `worker-1`、`worker-2`；不能简单复制一个带相同 ID 的容器。
- Channel 角色同时运行 Reply Sender 和 outbound resolver，消费同一个 durable `reply_outbox`；WeCom binding-scoped 长连接因此只由单一 Channel owner 创建。Channel 仍不执行 Runner。
- Admin UI 是 Node 24 构建后由 Nginx 1.27 提供静态文件，`/admin/v1/` 反向代理到 Gateway；Kubernetes 清单没有该 UI。
- `compose.admin-e2e.yaml`、`compose.deployment-e2e.yaml` 把 Worker 镜像替换为 `Dockerfile.e2e-worker`，使用 deterministic model/approval 模式；这用于受控验收，不是生产模型。

Compose 中 Gateway/Worker 的默认 `stop_grace_period` 是 45s，模型超时默认 1m，Worker concurrency 默认 4，artifact retention 默认 720h。实际变量可在 `.env` 中覆盖，敏感值不能提交。

## Kubernetes 推荐拓扑

入口是 `kubectl kustomize deploy/kubernetes/overlays/<env>`；base 包含：

- `trpc-agent-service-gateway` Deployment 和 ClusterIP Service。
- `trpc-agent-service-channel` 单副本 Deployment；没有 Service/HPA，更新策略为 `Recreate`，避免新旧 Pod 同时枚举 active binding。
- `trpc-agent-service-worker` Deployment 和 ClusterIP Service。
- Gateway/Worker 默认 replicas=2，RollingUpdate `maxUnavailable=0/maxSurge=1`，termination grace=45s；Channel 默认 replicas=1、`Recreate`。
- Gateway/Worker 两个 HPA 默认按 CPU 70% 扩缩，Worker 额外按 `trpc_agent_service_queue_backlog` 外部指标（目标 backlog=10）扩缩；base 为 2–10 副本，缩容稳定窗口 300s。该业务指标需要 Prometheus Adapter 或其它 External Metrics Provider 暴露给 Kubernetes；未部署 provider 时 CPU 指标仍是可用的保底信号。两个 PDB `minAvailable: 1`。Channel 不挂 HPA/PDB，单 owner 是当前明确边界。
- Gateway/Worker 请求资源均为 500m CPU/512Mi，限制为 2 CPU/2Gi。

环境 overlay 是当前仓库实际提供的三个选择：

| Overlay | replicas / HPA | 其它配置 |
| --- | --- | --- |
| `dev` | Gateway/Worker 1；HPA 1–2；Channel 1 | image tag `dev` |
| `staging` | Gateway/Worker base replicas 2、HPA max 6；Channel 1 | OTLP/Qdrant example endpoint，需要替换 |
| `production` | Gateway/Worker 3；HPA 3–20；Channel 1；缩容窗口 600s | Channel 仍 `Recreate`；termination grace 75s；请求 1 CPU/1Gi；限制 4 CPU/4Gi；Worker concurrency 8；shutdown timeout 60s；发布前必须把 fail-closed 占位镜像替换为已发布镜像的 SHA-256 digest |

Worker 的 queue backlog 业务指标来自固定低基数 OTel gauge；Prometheus Adapter 规则应把 `trpc_agent_service_queue_backlog` 映射为 External Metric，并对多个 scrape instance 使用 `max` 聚合（每个实例可能发布同一 PostgreSQL 聚合快照，不能直接 `sum`）。Worker 扩容能增加 execution claim capacity，但不会并行执行同一 Session；Gateway 扩容能增加 HTTP capacity，且不会启动 IM 长连接。Channel Adapter 保持单 owner；跨 Gateway 不存在重复 binding connection，代价是 Channel rollout/owner 故障期间可能有短暂 IM 接入窗口，见[风险登记](risk-register.md)。

## Probe、readiness 和 graceful shutdown

服务实现的 `/healthz` 在 HTTP server 可接收请求时返回 200；`/readyz` 只有进程角色组件启动并且 PostgreSQL、Redis ping 成功才返回 200。Gateway ingress `/v1/*` 被 readiness gate 保护，未 ready 返回 503；Admin `/admin/v1/*` 由独立 bearer auth 保护。

Kubernetes base：readiness 每 5s、timeout 3s、failure 6 次；liveness 每 10s、timeout 3s、failure 3 次。健康检查覆盖服务启动和 PostgreSQL/Redis 关键依赖；模型、Qdrant、COS、TencentDB 的调用状态由 runtime/operations/metrics 暴露。

收到 SIGINT/SIGTERM 后：

1. server 先 `MarkNotReady`，停止新 ingress。
2. Worker 停止 claim，取消辅助 heartbeat/migration loops；Channel 停止 reply sender 和 adapter；已经 claim 的 Consumer 在 shutdown deadline 内排空。
3. HTTP server、Runner、reply、migration、adapter goroutine 在各自 context/timeout 下结束；Worker 释放 lease 前排空 Runner event channel。
4. deadline 到期仍未退出会返回可观测错误，不伪装成成功关闭。

`TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT` 默认 30s；production overlay 是 60s，Pod termination grace 是 75s。Compose grace 45s，应确保它不短于希望的应用 shutdown timeout。

## Secret、ConfigMap 和网络边界

`deploy/kubernetes/secret.example.yaml` 只描述固定 Secret 名 `trpc-agent-service-secrets` 的形状，明确要求 out-of-band 创建。Gateway/Worker 需要 PostgreSQL DSN、Redis URL；Gateway 还需要独立 System Admin token。可选的 Operator/Auditor token 必须分别配套 `TRPC_AGENT_SERVICE_OPERATOR_TENANT_IDS` / `TRPC_AGENT_SERVICE_AUDITOR_TENANT_IDS` 逗号分隔租户 allowlist；角色 token 必须互不相同，配置不完整时 Gateway fail closed。

### IM 内部加密密钥

真实企业微信或飞书 binding 除厂商凭据外，还必须为每个 `(tenant_id, app_id)` 注入两项平台内部 scoped secret。它们不是企业微信或飞书提供的凭据，也不能用厂商 Bot Secret、App Secret 或同一份内部 key 互相替代：

| SecretRef | 用途 |
| --- | --- |
| `im-provider-target-key@v1` | 使用 AES-256-GCM 加密持久化 Provider reply target，使 Reply Outbox 在进程重启后能恢复真实回发目标 |
| `im-external-id-hmac-key@v1` | 对外部 user/chat/thread ID 做稳定 HMAC 映射，避免原始外部标识直接落库 |

`@v1` 是文档中的 `name@version` 写法；实际 `SecretRef` 拆分为 `Name`=`im-provider-target-key` / `im-external-id-hmac-key`，`Version`=`v1`。两个 secret 每个都必须是 32 字节随机数据的无 padding Base64URL 编码。每个 scope 分别生成并保存；多节点、滚动发布和服务重启必须继续使用相同值。不要把值写入 Git、镜像、ConfigMap、日志、trace 或验收 artifact。

可使用 Python 生成单个值：

```bash
python -c "import secrets,base64; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip('='))"
```

环境变量名必须按以下格式生成：

```text
TRPC_AGENT_SERVICE_SECRET_<tenant-id-hex>_<app-id-hex>_<secret-name-hex>_<version-hex>
```

四个 `*-hex` 都是对应 UTF-8 字节的小写十六进制；不是把原始 ID 直接拼接，也不是把 `@v1` 放进 `secret-name`。例如 `tenant_id=tenant-a`、`app_id=support` 时，分别把 `tenant-a`、`support`、两个 secret name 和 `v1` 编码后组成变量名。

Kubernetes 将这两项作为 `trpc-agent-service-secrets` 的额外 key 由外部 Secret 管理系统注入；base 的 `envFrom` 会让 Channel owner（以及需要构造对应 resolver 的角色）读取 scoped key。不要把真实值填入 `secret.example.yaml`。Docker Quick Start 不需要这些 key；若本地确实启用真实 IM，应把 key 放在被 `.gitignore` 忽略的独立 env 文件，并通过 Compose override 的 service-level `env_file` 注入 `channel`，不要修改 `.env.example` 提交值。

这些值由环境 `SecretProvider` 按 `(tenant_id, app_id, SecretRef)` 解析。缺失、scope 错配、Base64URL 带 padding 或解码后不是 32 字节时，真实 IM 的 target 恢复/ID 映射应失败，不得回退到明文 target 或原始外部 ID。实际外部 SecretProvider、KMS、rotation 流程由部署环境管理；仓库只记录接口与 scope 约束，具体注入、轮换和日志边界仍需在目标环境验证。

生产模型 base URL 要使用 HTTPS，并通过代码的 egress/IP policy；COS/Qdrant/TencentDB endpoint map 由 operator 维护，tenant config 只能选择逻辑 name，不可任意注入 URL。

## Schema migration 和发布

每个 `trpc-service` 角色在创建 PostgreSQL Store 后执行 `store.Migrate`。迁移在 `platform.schema_migration` 保存版本/name/SHA-256 checksum，并使用 PostgreSQL advisory lock；已应用 migration checksum 不匹配或数据库含未知版本时 fail closed。多个副本启动时依靠 advisory lock 串行。

因此发布必须遵循：migration 只追加、不可修改已应用 SQL；DDL 在滚动期间对旧/新二进制都兼容；先完成 schema，再使用新代码；删除字段要分多阶段。production overlay 故意使用全零 digest，不能直接发布；发布流水线必须替换为已推送镜像的真实 digest。Backend data cutover 不是 SQL DDL，而是[数据迁移状态机](data-sync-idempotency.md#6-session-迁移)，成功验证后才切 immutable ConfigVersion。

## 灰度、回滚和实际边界

应用层灰度由 Admin API 的 ConfigVersion canary 完成：按稳定 Session principal/session hash 分流，支持 pause/disable/rollback/promote。现在也提供最小指标驱动入口 `POST /admin/v1/configs/canary/evaluate`：调用方提交候选版本、聚合样本数、错误率、P95 execution latency、budget rejection 数和阈值规则；样本不足只返回 `NONE`，超阈值才按规则触发 `PAUSE` 或 `ROLLBACK`。PostgreSQL 以候选 `ConfigVersion` 做 CAS/行锁校验，旧告警迟到时返回冲突而不会影响新候选；动作写入控制面审计。Prometheus 告警规则见 `deploy/otel/alerts.yml`，告警编排仍是外部运维责任，不引入复杂发布平台。

示例（5 分钟窗口已由 Prometheus/Alertmanager 聚合）：

```json
{
  "tenant_id": "tenant-a",
  "app_id": "support",
  "version": "v2",
  "samples": 120,
  "error_rate": 0.12,
  "p95_latency_ms": 4200,
  "budget_rejections": 0,
  "minimum_samples": 20,
  "max_error_rate": 0.10,
  "max_p95_latency_ms": 5000,
  "max_budget_rejections": 0,
  "action": "ROLLBACK"
}
```

Kubernetes overlay 使用普通 RollingUpdate 和不可变 image digest；代码镜像回滚与 Backend 数据回滚分开处理，target Backend 切换后按数据迁移策略完成 authority 对账。Admin UI 的 Jaeger 链接由构建参数 `VITE_JAEGER_URL` 注入；未配置时只显示 Trace ID，不猜测 `localhost` 地址。

## 生产边界验收清单

以下项目是目标生产环境的验收证据，不由仓库内 Compose 或 Kustomize 渲染结果替代：

- HA PostgreSQL：主库故障切换、连接重建、事务/锁恢复、备份恢复和 RTO/RPO 报告；验证旧 execution 的 pinned ConfigVersion 不变。
- HA Redis：主从/故障切换或托管服务重启；验证 Stream Outbox、Consumer Group PEL reclaim、Session lease/lock 恢复和重复投递幂等。
- Ingress：TLS、真实负载均衡、readiness 摘除、超时/请求体限制、Admin 鉴权和 trace/request ID 透传证据。
- SecretProvider/KMS：SecretRef scope、解密、滚动重启可见性、轮换和旧版本撤销；证据不能包含 secret 原文。
- HPA business metric：Prometheus Adapter 以 `max` 聚合查询到 `trpc_agent_service_queue_backlog`，backlog 增长触发 Worker 扩容，恢复后按 stabilization window 缩容；保存 HPA Events、metrics API 和 Pod 数变化。

Channel 继续保持单 owner；本次补强不扩展到 Channel 多副本、binding lease 或 sharding。

部署 Workflow 覆盖可渲染清单、Compose golden path、一次 Worker restart 后继续服务和敏感证据扫描；本机 Compose 与真实 IM/观测链路已有外部验收证据。生产 Kubernetes admission、Ingress、secret 注入、Provider 网络、HA PostgreSQL/Redis/Qdrant 和发布恢复仍不由这些证据直接证明。Gateway 可多副本；Channel Adapter 是单副本 `Recreate` owner，不能随 Gateway HPA 一起扩展。当前没有 distributed binding lease/leader election/channel sharding；若需要多 Channel owner，需另行设计并验证。
