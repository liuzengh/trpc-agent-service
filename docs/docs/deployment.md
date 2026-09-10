# 部署、配置与快速开始

本页是 Issue #74 的可执行部署说明，覆盖一个可验证的 Docker Compose 本地环境和
Kubernetes 生产清单。服务启动时会先连接控制面并执行/校验 migration；`/healthz` 表示
进程和 HTTP 服务存活，`/readyz` 还要求启动图的数据库、Provider 和运行时依赖已经就绪。

这两种部署入口都让服务在容器接口 `0.0.0.0:8080` 上监听。服务默认的本地 CLI 仍监听
`127.0.0.1:8080`，所以直接运行二进制和容器部署不会互相改变安全边界。

## Docker Compose 本地快速开始

需要 Docker Engine 和支持 `up --wait` 的 Docker Compose v2。示例文件只包含本地占位值，
不会调用真实模型；在共享环境中必须替换所有凭据。

```bash
git clone https://github.com/XnLemon/trpc-agent-service.git
cd trpc-agent-service

# 可选：使用自己的本地值；不复制时脚本直接使用 example 文件。
cp deploy/example.env deploy/service.env
./scripts/quickstart.sh
```

`quickstart.sh` 会构建镜像、等待 PostgreSQL 健康、等待服务健康，并在容器内依次验证
`/healthz` 和 `/readyz`。成功后服务保持运行，宿主机可访问
`http://127.0.0.1:${TRPC_HTTP_PORT:-8080}`。停止并保留本地数据库卷：

```bash
docker compose --env-file deploy/service.env -f deploy/docker-compose.yml down
```

如果没有复制 `deploy/service.env`，把上面的参数替换为
`deploy/example.env`。删除本地数据库卷会丢失演示数据，只有明确需要重置时才执行
`down -v`。

`deploy/example.env` 是仓库内提交的默认参数模板；复制后得到的
`deploy/service.env` 仅用于本机覆盖值，真实凭据不会随镜像构建上下文提交。

本页的最小可用配置流程使用显式 `init` 和 Admin API 管理控制面对象。

该命令的默认端到端边界是迁移、bootstrap、HTTP 存活/readiness 和容器入口；它不会自动
创建控制面资源。需要一条真正可运行的本地链路时，使用开发专用入口：

```bash
./scripts/quickstart.sh --demo
```

`--demo` 会先执行 `trpc-service demo --confirm`，在 PostgreSQL 中幂等创建或复用 Tenant、
Agent App、deterministic fake Model、InMemory Backend 及已发布 Revision，再启动服务并实际
调用 `/v1/chat`。脚本断言固定响应 `Hello from the tRPC Agent Service demo.`；它不访问公共
模型、IM 或 Secret Manager endpoint，也不会把 DSN、token、模型 key 或 SecretRef 写入输出。
demo 仅支持 PostgreSQL、本地开发使用，遇到部分或多租户歧义状态会 fail closed。普通生产启动
仍不会自动建资源；生产请按 [Issue #67 首次运行初始化](issue-67-first-run-init.md) 初始化
控制面，再通过 Admin API 创建并发布配置。

### Compose 验收契约

- `postgres` 使用 PostgreSQL 16，并以 `pg_isready` 作为依赖健康条件。
- `service` 只有在数据库健康后才启动，启动时自动应用并验证控制面 migration。
- 容器健康检查使用静态 `/app/trpc-healthcheck`，不会依赖 distroless 镜像中的 shell。
- `/healthz` 用于存活和 startup probe；`/readyz` 用于流量接入前的 readiness。
- `TRPC_SERVICE_IMAGE` 可在 CI 或本地覆盖服务镜像名；默认值是 `trpc-agent-service:local`。
- Compose 同时启动 Redis 7（AOF、`redis:6379`）作为可选运行时后端；只有显式设置
  `TRPC_SESSION_BACKEND=redis` 时服务才会读写它。生产环境应替换为受管 Redis，并通过 Secret
  Manager 注入 `TRPC_REDIS_PASSWORD`。

填充后的 `deploy/service.env` 已被 `.dockerignore` 排除，不会进入 Docker build context；该文件
仍可能被 Compose 读取，因此不要提交到 Git，也不要把它作为生产 Secret 管理方案。

Compose 的默认 DSN 会把 `POSTGRES_USER`、`POSTGRES_PASSWORD` 和 `POSTGRES_DB` 组合起来；如果
用户名或密码包含 `@`、`:`、`/` 等 URL 保留字符，请先做 URL 编码并显式设置
`TRPC_POSTGRES_DSN`，不要依赖字符串拼接。

## 最小部署 E2E 流程

### 1. 先验证可部署服务

不需要外部凭据时，使用 deterministic demo 验证镜像、PostgreSQL、migration、bootstrap、
health/readiness 和 `/v1/chat`：

```bash
cp deploy/example.env deploy/service.env
./scripts/quickstart.sh --demo deploy/service.env
```

这条命令是 Docker Compose 的最小部署门槛。成功后服务监听
`http://127.0.0.1:8080`，停止命令为：

```bash
docker compose --env-file deploy/service.env -f deploy/docker-compose.yml down
```

### 2. 使用真实 Telegram Bot API 做传输 E2E

Telegram E2E 使用真实 `getMe`、`getUpdates` 和 `sendMessage`，执行链路使用确定性
Dispatcher，因此不需要模型 key。准备一个接收 Bot；需要 CI
自动发送消息时，再准备第二个受控 Bot，并开启 Bot-to-Bot Communication Mode：

```bash
export TELEGRAM_BOT_TOKEN='接收 Bot token'
export TELEGRAM_SENDER_BOT_TOKEN='自动发送 Bot token' # 本地人工发送时可省略
export TELEGRAM_DELETE_WEBHOOK=true                    # Bot 已配置 webhook 时设置
go run ./examples/telegram-e2e
```

程序会打印本次运行的唯一文本 marker。人工运行时，从 Telegram 向接收 Bot 发送该 marker；
自动运行时，第二个 Bot 会发送 marker。通过标准是收到对应的
`telegram-e2e-ok:<correlation>` 回复，并且进程能在 `Ctrl+C`/SIGTERM 后干净退出。

该流程覆盖真实 Telegram 网络和适配器生命周期；普通服务的 PostgreSQL/HTTP 部署验证由上面的
Compose golden path 完成。CI 使用同一示例和受保护的 `telegram-e2e` Environment。

### 3. WeCom deterministic callback E2E

WeCom callback 的常规 E2E 不需要企业微信凭据，但需要一个可写的 PostgreSQL：

```bash
export POSTGRES_MIGRATION_TEST_DSN='postgres://postgres:postgres@127.0.0.1:5432/trpc_agent_wecom_e2e?sslmode=disable'
go test ./examples/wecom-e2e -run TestWeComCallbackOutboxE2E -count=1 -v
```

该测试验证签名/AES callback、Gateway dispatch、PostgreSQL Event/Reply Outbox、provider
delivery 和重复 callback 不重复执行。CI 在 PostgreSQL service 中执行相同测试。

## Kubernetes 部署

`deploy/kubernetes` 是一个可用的 Kustomize base，包含 ConfigMap、Secret 引用、
Deployment 和 ClusterIP Service。Deployment 默认两副本，滚动更新策略为
`maxUnavailable: 0`、`maxSurge: 1`，并设置 startup/readiness/liveness probes、资源上下限、
非 root、只读根文件系统和 `RuntimeDefault` seccomp profile。

### 1. 创建 Secret

不要把真实 Secret 写入仓库。`deploy/kubernetes/secret.example.yaml` 只用于说明字段形状；
生产环境应由 Secret Manager、External Secrets 或集群密钥服务生成同名 Secret
`trpc-agent-service-secrets`。最少需要提供：

```text
TRPC_POSTGRES_DSN
TRPC_API_TOKEN + TRPC_TENANT_ID + TRPC_APP_ID
TRPC_ADMIN_TOKEN + TRPC_ADMIN_TENANTS
TRPC_MODEL_API_KEY
```

多租户时，用 `TRPC_API_IDENTITIES` 替代旧的三个 API identity 字段，并用
`TRPC_MODEL_API_KEYS` 为每个 tenant 提供模型密钥；详细格式见下方配置参考。若启用企业微信，
四个 `WECOM_*` 字段必须一起提供。

### 2. 固定镜像版本并应用

base 使用当前服务版本 `0.1.1`，而不是可变的 `latest`。每次发布都应在部署清单的
`images` 块中更新 release tag，或改用已经由发布系统解析出的 digest；这样 PodTemplate 会
变化，Deployment 才会创建新的 ReplicaSet，并保留可回滚的版本目标。例如：

```yaml
images:
  - name: ghcr.io/xnlemon/trpc-agent-service
    newName: registry.example.com/trpc-agent-service
    digest: sha256:<published-image-digest>
```

仓库的 `Publish Container Image` workflow 会在 `v*.*.*` tag 推送时构建并发布
`ghcr.io/xnlemon/trpc-agent-service` 的 amd64/arm64 镜像，并同时生成版本 tag 和 commit
SHA tag。发布动作使用 GitHub Actions 内置的 `GITHUB_TOKEN`，不需要把个人凭据写入仓库。
对应 tag 发布完成且集群具备 registry 拉取权限后，可以直接应用下面的 base；也可以在部署
系统的 overlay 中覆盖为已发布的 tag 或 digest。集群镜像应使用已发布版本或 digest。

准备好 namespace、Secret 和镜像版本后：

```bash
kubectl -n trpc-agent create namespace trpc-agent --dry-run=client -o yaml | kubectl apply -f -
kubectl -n trpc-agent apply -f deploy/kubernetes/secret.example.yaml # 仅适用于已替换且受控的副本
kubectl -n trpc-agent apply -k deploy/kubernetes
kubectl -n trpc-agent rollout status deployment/trpc-agent-service --timeout=5m
kubectl -n trpc-agent get pods -l app.kubernetes.io/name=trpc-agent-service
```

生产环境不要直接执行包含真实值的 `secret.example.yaml`，也不要把 DSN 或密钥放进 shell
历史；上面的 Secret 命令只说明资源名和字段契约。需要临时验证 HTTP 时，可以使用：

```bash
kubectl -n trpc-agent port-forward service/trpc-agent-service 8080:8080
curl --fail http://127.0.0.1:8080/healthz
curl --fail http://127.0.0.1:8080/readyz
```

`TRPC_CONTROL_PLANE_DRIVER=postgres`、`TRPC_SESSION_BACKEND=postgres` 和
`OTEL_SERVICE_NAME` 已由 ConfigMap 提供；其余非敏感配置可在部署系统的 overlay 中覆盖。任何缺失的
数据库、identity、Admin 或模型配置都会在绑定 HTTP 端口前 fail closed。

升级已有环境时不要改写已执行 migration 的版本号。当前发布顺序中 trace-parent 使用
`0011_reply_trace_parent.up.sql`，运行时能力使用 `0012_runtime_capabilities.up.sql`；这两个
文件的版本与 digest 必须保持不变，数据库应通过服务 bootstrap 继续增量升级。首次部署前请确认
`schema_migrations` 没有未经审计的版本或 digest 修改。

## 配置参考

环境变量由 `trpcservice/bootstrap` 解析。除 Compose 专用变量外，服务不会为必需凭据猜测
默认值；示例文件中的 `local-*` 值只适合单机演示。

### Compose 专用变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `POSTGRES_DB` | `trpc_agent` | PostgreSQL 容器初始化数据库名 |
| `POSTGRES_USER` | `trpc` | PostgreSQL 容器初始化用户 |
| `POSTGRES_PASSWORD` | `trpc-local-password` | 本地演示密码；生产使用 Secret Manager；默认 DSN 会同步使用它 |
| `TRPC_POSTGRES_DSN` | 由上面三个变量派生 | PostgreSQL 服务连接；显式设置时覆盖派生值，不要放进参数或日志 |
| `TRPC_HTTP_PORT` | `8080` | 宿主机映射端口，容器内固定为 8080 |
| `TRPC_SERVICE_IMAGE` | `trpc-agent-service:local` | 服务镜像名/标签，供 CI 或本地覆盖 |

### 控制面与 API identity

| 变量 | 必需/默认 | 说明 |
| --- | --- | --- |
| `TRPC_CONTROL_PLANE_DRIVER` | 否，`postgres` | `postgres` 或 `mysql` |
| `TRPC_POSTGRES_DSN` | PostgreSQL 时必需 | 控制面和 PostgreSQL runtime 的连接；Compose 未显式设置时由 `POSTGRES_USER/PASSWORD/DB` 派生；不要放进参数或日志 |
| `TRPC_MYSQL_DSN` | MySQL 时必需 | 应用账号连接 |
| `TRPC_MYSQL_MIGRATION_DSN` | MySQL 时必需 | 独立 migration 账号连接；不能与应用账号复用 |
| `TRPC_API_IDENTITIES` | 多租户可选 | 逗号分隔 `token\|tenant\|app\|subject`；设置后替代旧 identity 字段 |
| `TRPC_API_TOKEN` | 单租户时必需 | 旧兼容路径的 API token |
| `TRPC_TENANT_ID` | 单租户时必需 | 旧兼容路径的租户 ID |
| `TRPC_APP_ID` | 单租户时必需 | 旧兼容路径的 Agent App ID |
| `TRPC_SUBJECT_ID` | 否，`service` | 旧兼容路径的主体 ID |
| `TRPC_ADMIN_TOKEN` | 必需 | Admin API bearer token |
| `TRPC_ADMIN_TENANTS` | 必需 | 逗号分隔的可管理 tenant；显式 `*` 表示平台管理员可管理全部租户，生产环境按最小权限配置具体 tenant ID |

`TRPC_API_IDENTITIES` 与 `TRPC_API_TOKEN`/`TRPC_TENANT_ID`/`TRPC_APP_ID` 互斥。Token
只作为认证 map key 使用，不会写入错误信息或运行时快照。

`TRPC_ADMIN_TENANTS=*` 是显式的平台管理员配置，允许 Admin API 管理所有租户，同时保留首租户创建的启动门槛。共享或生产环境应改为逗号分隔的最小租户范围，并通过 Secret Manager 注入管理员凭据。

### 模型、Secret 和运行时后端

| 变量 | 必需/默认 | 说明 |
| --- | --- | --- |
| `TRPC_MODEL_API_KEY` | 单租户必需 | 单一 API identity 的模型密钥 |
| `TRPC_MODEL_API_KEYS` | 多租户必需 | 逗号分隔 `tenant_id=api_key`，每个 identity 都必须有 key |
| `TRPC_MODEL_PROVIDER` | 否，`openai` | 当前 bootstrap 支持的 Provider |
| `TRPC_MODEL_NAMES` | 否，`gpt-4o-mini` | 逗号分隔模型白名单 |
| `TRPC_MODEL_ENDPOINT_HOSTS` | 否，`api.openai.com` | 逗号分隔 HTTPS endpoint host 白名单 |
| `TRPC_MODEL_SECRET_REF` | 否，`env/trpc-model-api-key` | 运行时 Secret 引用，不是 Secret 值 |
| `TRPC_SESSION_BACKEND` | 必需，显式 `postgres`/`redis`/`inmemory` | Compose/Kubernetes 示例使用 `postgres`；Redis 模式只提供 Session/Memory，MySQL 控制面当前应使用 `inmemory` |
| `TRPC_REDIS_ADDR` | Redis 模式必需 | Redis `host:port`；Compose 默认使用 `redis:6379` |
| `TRPC_REDIS_PASSWORD` | 否 | Redis 认证密码，使用 Secret Manager 注入，不进入日志或快照 |
| `TRPC_REDIS_SECRET_REF` | 否，`env/trpc-redis-password` | Redis Backend Profile 的可选 SecretRef |
| `TRPC_REDIS_DB` | 否，`0` | Redis logical database |
| `TRPC_REDIS_KEY_PREFIX` | 否，`trpc:runtime:v1` | tenant-scoped key 前缀 |
| `TRPC_REDIS_DIAL_TIMEOUT` / `TRPC_REDIS_READ_TIMEOUT` / `TRPC_REDIS_WRITE_TIMEOUT` | 否 | Go duration，限制 Redis 客户端 I/O |
| `TRPC_REDIS_POOL_SIZE` | 否 | 大于 `0` 时覆盖连接池大小 |
| `TRPC_S3_ACCESS_KEY_ID` | 否 | S3-compatible Artifact provider 的 access key；仅在 materialize S3 binding 时使用 |
| `TRPC_S3_SECRET_KEY` | 否 | S3-compatible Artifact provider 的 secret key；通过 Secret 注入，不写入日志或快照 |
| `TRPC_S3_SECRET_REF` | 否，`env/trpc-s3-credentials` | S3 Backend Profile 必须匹配的 tenant SecretRef |
| `TRPC_DEMO_MODE` | 否，`false` | 仅由 `quickstart.sh --demo` 显式启用；要求 `TRPC_MODEL_PROVIDER=fake`，不读取模型凭据 |

模型 API key 只在受信任的 Secret Resolver/Factory 路径中使用，不进入 Execution Plan、缓存、
日志或数据库。

S3 Artifact provider 是按 Profile 选择的可选能力，不会因为设置上述凭据就改变默认后端。Binding
需要 `Provider: "s3"`、`artifact` capability、`bucket` option 和匹配的 `SecretRef`；本地 MinIO
可通过 `docker compose --profile s3` 启动，HTTP endpoint 必须同时设置 `allow_insecure=true`。
真实部署应使用 HTTPS S3/OSS-compatible endpoint，并将凭据交给 Secret Manager。[runtime-storage.md](runtime-storage.md)
中的 live conformance 测试只在显式提供测试环境变量时运行。

### 企业微信与 OpenTelemetry

| 变量 | 必需/默认 | 说明 |
| --- | --- | --- |
| `WECOM_CALLBACK_TOKEN` | 启用 WeCom 时四项全需 | 回调签名 token |
| `WECOM_ENCODING_AES_KEY` | 启用 WeCom 时四项全需 | 回调加解密 key |
| `WECOM_APP_SECRET` | 启用 WeCom 时四项全需 | 企业微信应用 Secret |
| `WECOM_SECRET_REF` | 启用 WeCom 时四项全需 | 与 tenant 的 Binding SecretRef 匹配 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 否，空为 no-op | OTLP/HTTP endpoint；配置错误会阻止启动 |
| `OTEL_EXPORTER_OTLP_HEADERS` | 否，空 | 逗号分隔 `key=value`，只传给 exporter |
| `OTEL_EXPORTER_OTLP_INSECURE` | 否，`false` | `true` 仅用于 HTTP/本地 Collector |
| `OTEL_SERVICE_NAME` | 否，`trpc-agent-service` | OTLP resource service name |

四个 WeCom 变量必须全为空或全部设置；单套 WeCom 凭据只允许绑定一个 API identity。
OTLP exporter 故障不会把 header 或 Secret 写入 span、metric、日志或错误文本。

## 验证与 CI

不依赖集群的部署清单预检：

```bash
./scripts/validate-deployment.sh
```

该脚本检查填充 env 文件的 Docker 忽略规则、Compose 渲染结果、容器监听地址、Kustomize
输出和固定镜像 tag。仓库 CI 的 Deployment job 还会构建 Docker 镜像，并用 PostgreSQL 服务
运行 Compose smoke test，验证容器内 `/healthz`、`/readyz` 及 `--demo` golden path 的第一条
`/v1/chat`；MkDocs workflow 以
`mkdocs build --strict` 构建本页。由于本地环境可能没有运行 Docker daemon，无法连接 daemon
时仍可运行上述 Compose config、Kustomize、Go 单测和静态检查，实际镜像/Compose smoke 会由
CI 完成。

### 部署与 CI 门槛

部署清单的本地静态门槛：

```bash
./scripts/validate-deployment.sh
docker compose --env-file deploy/example.env -f deploy/docker-compose.yml config --quiet
kubectl kustomize deploy/kubernetes >/tmp/trpc-agent-service-k8s.yaml
```

常规 CI 门槛由以下 job 组成：

- `Format & Lint`：gofmt、go vet、golangci-lint；
- `Commit Secret Scan`：固定版本 Gitleaks、allowlist fixture 和 SARIF；
- `Deployment Validation`：Compose/Kustomize 校验、Docker build、Compose golden path；
- `Build, Test & Coverage`：全仓测试、PostgreSQL/MySQL live smoke、构建和覆盖率；
- `Race Tests`：全仓 `go test -race ./...`；
- `Docs`：`mkdocs build --strict`；
- 独立 E2E：fault-injection、WeCom deterministic callback 和 Telegram live E2E。

## 部署验收覆盖

quick start 验证 PostgreSQL migration、bootstrap、HTTP 存活/readiness、镜像入口和第一条
deterministic 对话；Secret rotation、备份恢复、容量压测、Provider/IM E2E、灰度和回滚均有
对应的 CI workflow、运行手册和验收命令。
