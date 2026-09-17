# 本地 Docker Desktop 验收指南

本仓库只提供本地 Docker Desktop 验收入口。PostgreSQL、Redis、Vault、Qdrant、Jaeger 和 OpenTelemetry Collector 都运行在本机容器中；不需要 Kubernetes、云主机、Prometheus Alertmanager 或外部运维资源。

DeepSeek 是唯一默认启用的外部调用。API Key 只用于本机容器中的真实模型调用，绝不能提交到仓库。可选的 Feishu、WeCom 与 MCP smoke 会使用开发者自行创建的凭据和受控端点；它们同样只服务于本机 Docker 验收。

验收者首选先运行 `./start.sh --demo`，验证无凭据启动与 deterministic fake model。它不需要任何 Secret 文件；覆盖范围和豁免项见 [Fake Demo 覆盖面](demo-fake-coverage.md)。真实 DeepSeek、Feishu 与 WeCom 验证均是后续可选项。

## Acceptance from zero

**验收者从零复现（推荐顺序）**

本节面向第一次拿到仓库、没有任何历史数据库或 Docker 数据卷的验收者。无需申请云主机、Kubernetes、运维账号或任何模型密钥；Docker Desktop 即可先完成无凭据核心验收。真实 DeepSeek、Feishu 与 WeCom 是其后的可选外部验证，不能替代无凭据门禁。

1. 克隆仓库，进入根目录，并确认 Compose 文件可解析：

   ```bash
   git clone <repository-url>
   cd trpc-agent-service
   docker compose -f deploy/compose/docker-compose.local.yml config --quiet
   ```

2. 先运行无凭据最终验收。它会创建独立的临时 PostgreSQL/Redis、断言空库只应用 `000001`，并验证 deterministic fake chat 与 SSE：

   ```bash
   ./start.sh --demo
   ```

命令成功时会打印可访问的 HTTP 地址及其专属资源的清理命令。若还需执行格式、依赖、build、vet 与全量单测门禁，使用 `bash scripts/ci/admission.sh --demo`（需要本机 Go 1.25）。

3. 只有需要真实模型闭环时，才创建仅本机可读的 DeepSeek 模型密钥文件。`/absolute/path/to/deepseek-api-key` 是保存**单行 API Key 内容**的现有文件路径，不是字面量；密钥不要加引号，也不要提交：

   ```bash
   mkdir -p deploy/compose/secrets
   install -m 600 /absolute/path/to/deepseek-api-key \
     deploy/compose/secrets/deepseek-api-key
   ```

4. 启动真实 DeepSeek WebUI 闭环并检查 readiness。首次启动会从空的 PostgreSQL 16 数据库执行业务 schema 基线 `000001`：

   ```bash
   ./start.sh
   curl --fail http://localhost:58081/readyz
   docker compose -f deploy/compose/docker-compose.local.yml exec -T postgres \
     psql -U postgres -d trpc_agent_service_test -Atc \
     'SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1'
   ```

   预期最后一条命令输出 `000001`。然后打开 <http://localhost:58081/webui/>，按第 3 节完成一次文本和 durable confirmation 验收。

5. 按需执行本地自动化验证。它们使用独立的临时容器/卷或当前本地 Compose，不要求任何 IM 凭据；依赖恢复演练需要第 3 步的 DeepSeek Key：

   ```bash
   # 无凭据的最终验收：空库单一基线、fake chat 与 SSE
   bash scripts/ci/admission.sh --demo
   bash scripts/ci/admission.sh
   bash scripts/e2e/backend-adapter.sh
   bash scripts/e2e/dependency-recovery.sh
   ```

6. 只有需要真实 IM 验收时，才停止 WebUI standalone runtime，改按第 4 节或第 5 节配置飞书/企业微信。每种 IM 都必须使用开发者自己的应用凭据和新的临时 HTTPS tunnel；Quick Tunnel 重启会更换域名，因此需把新的完整回调 URL 重新保存到平台后台。

验收结束后执行 `./stop.sh` 可保留本地数据便于排查。确需重新从空库开始时，才执行下列破坏性命令；它只会删除名为 `trpc-agent-local` 的 Compose project 所创建的卷：

```bash
docker compose -f deploy/compose/docker-compose.local.yml --profile webui down -v
```

## 1. 前置条件

- Docker Desktop（包含 `docker compose` v2）；
- 无凭据 Demo 还需要 `curl`；
- 真实 WebUI、IM 与依赖恢复验证才需要一个可用的 DeepSeek API Key；
- 本机端口 `55432`、`56379`、`58081`、`56686`、`59464` 未被占用。

### 1.1 可选：声明多个 Session PostgreSQL 数据平面

默认情况下，Worker 把 `TRPC_POSTGRES_DSN` 同时注册为无密钥的 `connection_id=default`，因此现有 schema v1 Profile 与单库部署不需要新增参数。需要为 schema v2 的已发布 `session` Backend Profile 选择其他 PostgreSQL 数据平面时，在 Worker 的部署环境中设置 JSON 注册表；Profile 只保存 `connection_id`，不得保存 DSN、密码或 `SecretRef`：

```bash
export TRPC_SESSION_POSTGRES_CONNECTIONS='{
  "session-primary": "postgres://worker:password@session-primary:5432/session?sslmode=require",
  "session-secondary": "postgres://worker:password@session-secondary:5432/session?sslmode=require"
}'
```

每个被引用的数据平面都必须先应用服务 schema 基线。未注册、格式非法或未发布的 connection ID 会让该执行 fail closed；不会回退到默认库。此变量是 Worker 的部署密钥配置，应通过 Secret projection/环境注入提供，不能提交到 `.env.local` 或 Backend Profile。

### 1.2 Session 数据平面迁移作业

`session-migrate` 是有界的一次性 operator job，不是常驻 Worker。它只处理已在 `backend_migration` authority 中创建的 `session` migration：解析 source/target 的精确 Backend Profile、回填官方 Session snapshot、修复 durable mutation，并在安全阶段推进 `planned → snapshot → dual_write → backfill → verify`。每次执行只做一小步或一个 batch，可安全重跑。

它不会自行切换 tenant 的 active ConfigSnapshot，也不会自行 rollback；verify 成功后仍需由已授权的控制面操作携带 verification evidence 执行 cutover。cutover/observe 阶段该作业仅处理正反向 repair 并报告 drain 状态。

```bash
export TRPC_SESSION_MIGRATION_TENANT_ID='t_example'
export TRPC_SESSION_MIGRATION_ID='session-move-2026-01'
export TRPC_SESSION_MIGRATION_WORKER_ID='operator-01'
export TRPC_SESSION_MIGRATION_CONFIRM=true

# TRPC_POSTGRES_DSN 与可选 TRPC_SESSION_POSTGRES_CONNECTIONS 按 1.1 注入。
go run ./cmd/trpc-service session-migrate
```

该 job 拒绝缺少确认、未应用 schema、未部署或指向同一数据平面的 source/target、非 Session domain 与非法 phase；不记录或打印任何 DSN。迁移期间保持使用与 Worker 相同的 `TRPC_SESSION_POSTGRES_CONNECTIONS` Secret projection，避免 job 与实际数据面路由产生偏差。

创建、切换和收尾均经同源 Admin API，而不是直接写 `backend_migration`。创建请求只指定 stable migration ID 与**已 staged** 的 target ConfigVersion；服务从当前 active ConfigSnapshot 推导 source Session binding，固定 target binding，并拒绝未变更的 Session Profile。迁移作业在 `verify` 阶段把 shadow evidence 以 CAS 写入 authority；浏览器不能提交 count/digest/watermark。

```text
POST /v1/tenants/{tenant_id}/session-migrations
  {"migration_id":"session-move-2026-01","target_config_version":42}

GET  /v1/tenants/{tenant_id}/session-migrations
GET  /v1/tenants/{tenant_id}/session-migrations/{migration_id}/status
POST /v1/tenants/{tenant_id}/session-migrations/{migration_id}/cutover
POST /v1/tenants/{tenant_id}/session-migrations/{migration_id}/observe
POST /v1/tenants/{tenant_id}/session-migrations/{migration_id}/rollback
POST /v1/tenants/{tenant_id}/session-migrations/{migration_id}/cleanup
```

切换、观察、回滚与清理请求均携带 authority 的 migration version 与 tenant version；切换和回滚还需要稳定 `switch_id`，并沿用 `X-Reason-Code`、`X-Correlation-ID`、`X-Trace-ID`。切换/回滚由数据库函数在同一事务内更新 active ConfigSnapshot、写 switch journal、追加 audit/config-invalidation outbox；创建也与其 audit outbox 原子提交。rollback/cleanup 的同步 watermark 同样从 authority 中读取，不能由浏览器覆盖。

同源 `/admin/` 控制台提供同一套迁移操作：输入 staged target ConfigVersion 创建、按 migration ID 查看 authority evidence/drain，再以浏览器确认执行 cutover、observe、rollback 或 cleanup。它仍使用短期 Admin Token 换取 HttpOnly Strict Cookie，所有变更由同源 CSRF 检查保护；页面不会读取或显示 Session PostgreSQL DSN。

### 1.3 Memory Redis → PostgreSQL 迁移（Compose 验收）

`memory-migrate` 是与 Worker 分离、须显式确认的一次性 Compose operator。当前可执行的数据面仅为官方 `redis-memory` schema v1 到 `postgres` schema v1/v2，且两端都必须声明 `strong_ryw`、使用部署注册表中无额外 credential 的 `connection_id`。Mem0、InMemory 及其他向量/Memory provider 可以被租户选作运行时后端，但**不在本仓库的在线迁移能力面内**。

Admin API 只允许从当前 active ConfigSnapshot 推导 source，并指向已 staged 的 target ConfigVersion 创建 Memory migration；可查询、暂停、恢复或在 dual-write 前 abort。当前没有 Memory cutover/observe/rollback HTTP 入口，`memory-migrate` 也会在 `verify` 写入 evidence 后停止。因此这是一条可重复的 Compose 数据面验收流程，不是生产切流工具。

```bash
export TRPC_MEMORY_MIGRATION_TENANT_ID='t_example'
export TRPC_MEMORY_MIGRATION_ID='memory-redis-to-postgres-2026-01'
export TRPC_MEMORY_MIGRATION_WORKER_ID='operator-01'
export TRPC_MEMORY_MIGRATION_CONFIRM=true

# 与 Worker 相同的部署注册表；Profile 中只保存 connection_id。
export TRPC_MEMORY_REDIS_CONNECTIONS='{"memory-source":"redis://redis:6379/0"}'
export TRPC_MEMORY_POSTGRES_CONNECTIONS='{"memory-target":"postgres://postgres:postgres@postgres:5432/trpc_agent_service_test?sslmode=disable"}'
go run ./cmd/trpc-service memory-migrate
```

每次调用至多推进一个 authority phase 或一个 backfill/repair batch：`planned → snapshot → dual_write → backfill → verify`。进入 `dual_write` 后，Worker 对该 tenant 的每次可追踪 Memory 写都会按精确 ConfigVersion 读取整用户镜像并复制到目标；目标短暂失败会留下 durable mutation ledger，由 operator 的 claim/repair 重试。`verify` 要求 repair backlog 清零、count/digest 一致，因而不会把单边失败报告为成功。共享 Redis export 会跳过其他 tenant 的 entry；反向 Redis 镜像 writer 已具备，以便未来受控 cutover/observe 实施 rollback sync，但当前 Compose 交付不激活该阶段。

## 2. 可选：配置一个审阅后的 MCP 工具

MCP 不属于 demo，也不会由模型提供 URL 或工具名。只允许一个 tenant 下的一个已审阅 HTTPS SSE/streamable 端点映射为一个固定 ToolRef；stdio、私网/回环地址、重定向、动态 ToolSet、`mcpbroker` 和通用 `mcp_call` 均被拒绝。

先在隔离的审阅环境取得目标 MCP `tools/list` 中**单个工具对象**，保存为不含凭据的 JSON 文件（字段为 `name`、`description`、`inputSchema`，可选 `outputSchema`）。本仓库的离线命令将其规范化为运行时使用的 declaration digest：

```bash
go run ./cmd/trpc-service mcp-declaration-digest \
  --declaration-file /absolute/path/to/reviewed-weather-tool.json
```

把输出写进 `.env.local` 的 `TRPC_MCP_ENDPOINTS` 对应项的 `declaration_digest`；再运行下列同样离线的命令。其第三列就是创建/更新 Draft Revision 时 `tool_refs[].content_digest` 必须使用的 binding digest：

```bash
set -a; source deploy/compose/.env.local; set +a
go run ./cmd/trpc-service mcp-binding-digests
# weather_lookup  1  <64-char-binding-digest>
```

发布 Revision 后，Worker 会在构建 Bundle 时用可信 ExecutionContext 执行 discovery，验证远端 declaration digest；每次调用也会通过同一受限端点、SecretRef 和结果大小门禁。改动 URL、超时、认证引用、远端名称或 declaration 都会使旧 Revision fail closed，必须发布带新 binding digest 的 Revision。

## 2.1 可选：启用受限代码执行

代码执行默认关闭，且不是本机 shell 的快捷入口。Worker 只接受固定 tenant/id/version 的注册，并使用上游 OS sandbox 的 managed、禁网、无环境继承 profile；没有可用 sandbox 时执行会失败，不会退回宿主执行。准备一个仅由 Worker 使用、不是仓库、不是 Skill staging 且不含业务文件的绝对目录，例如 `/var/lib/trpc-agent-service/codeexecutor`：

```bash
export TRPC_CODE_EXECUTOR_WORKSPACE_ROOT=/var/lib/trpc-agent-service/codeexecutor
go run ./cmd/trpc-service code-executor-binding-digest \
  --tool-id sandbox_execute_code --version 1
# <64-char-binding-digest>
```

将输出写入 `TRPC_CODE_EXECUTORS[].content_digest`，例如 `[{"tenant_id":"replace-after-seeding","tool_id":"sandbox_execute_code","version":1,"content_digest":"<output>"}]`；再以同一个值创建或更新 Draft Revision 的 `tool_refs[].content_digest` 后发布。工具仅支持 Python/Bash，每次调用使用可信 tenant/request 创建并清理 workspace；模型提供的 `execution_id`、本地执行器、共享 workspace 和未扫描输出文件都不会被接受。部署前可运行 `code-executor-binding-digests` 复核已配置条目的 digest。

## 3. 启动本地 WebUI

在仓库根目录执行：

如需覆盖默认端口、Route Key 或本地 runtime 参数，先从示例创建仅供本机使用的配置文件：

```bash
cp deploy/compose/.env.local.example deploy/compose/.env.local
chmod 600 deploy/compose/.env.local
```

早期本地环境若存在旧阶段配置文件，不要直接提交或继续引用它；以新的 `.env.local.example` 为基准，只迁入仍需要的非敏感覆盖项。Compose project 已更名为 `trpc-agent-local`；旧容器和卷会保留，开发者应先用 `docker compose ls` 确认历史 project，再决定何时停止或清理。

```bash
mkdir -p deploy/compose/secrets
install -m 600 /absolute/path/to/deepseek-api-key \
  deploy/compose/secrets/deepseek-api-key
./start.sh
```

或直接使用 Compose：

```bash
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile webui up --build
```

访问：

- WebUI：<http://localhost:58081/webui/>；
- Jaeger Trace：<http://localhost:56686/>；
- Collector Prometheus 格式指标：<http://localhost:59464/metrics>。

页面默认 Route Key 和 Account ID 都是 `local-webui`，Channel Token 是 `local-webui-token-change-me`。点击连接后，可发送：

```text
创建一条标题为验收、内容为 WebUI durable confirmation 的笔记
```

页面应展示持久化的“批准一次 / 拒绝”卡片。批准后，Graph 从 Redis checkpoint 恢复并只执行一次本地 note Tool；刷新页面不会重复执行。消息链路为：

```text
WebUI → HMAC callback → Inbox / Preprocess → Redis dispatch
→ Worker + DeepSeek → Result / Reply Outbox → Delivery Ledger → WebUI mailbox
```

WebUI 验证 provider-neutral 的本地链路。Feishu 与 WeCom 另有下面各自的可选真实账号 smoke。

`webui-local`、`feishu-local` 和 `wecom-local` 都是同一套本地 PostgreSQL/Redis composition 的完整 Worker runtime，不能并行启动。它们会通过 PostgreSQL advisory lock 拒绝并发启动，避免跨渠道争抢 durable work 或读取不到对方的 scoped secret。切换 profile 前先停止当前的 standalone local service：

```bash
docker compose -f deploy/compose/docker-compose.local.yml stop \
  webui-local feishu-local wecom-local
```

`webui-multinode` 使用独立 Compose project 与显式节点 ID，不受此限制。

## 4. 可选：真实 Feishu 本地验收

此 smoke 运行完整的 Feishu Webhook、验签、durable ingress、Redis Worker、DeepSeek 调用和 Feishu Reply API。数据库、Worker 和可观测性组件仍全部运行在 Docker Desktop；唯一的外部依赖是开发者自己的 Feishu 应用、DeepSeek Key 和临时 HTTPS tunnel。

先创建仅供本机使用的凭据文件：

```bash
mkdir -p deploy/compose/secrets
cp deploy/compose/feishu.env.example deploy/compose/secrets/feishu.env
chmod 600 deploy/compose/secrets/feishu.env
```

编辑 `deploy/compose/secrets/feishu.env`，填入同一个 Feishu 应用的 `FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`FEISHU_VERIFICATION_TOKEN` 和 `FEISHU_ENCRYPT_KEY`。`FEISHU_BOT_OPEN_ID` 对私聊可留空；要验收群聊 @mention 时必须填入该机器人的 Open ID。不要给值添加引号或反引号。这个文件被 Git 忽略，严禁强制加入版本库。

启动本地服务：

```bash
TRPC_LOCAL_FEISHU_PORT=58086 \
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile feishu-local up -d --build

curl --fail http://localhost:58086/readyz
```

然后用开发者选择的临时 HTTPS tunnel 将宿主机 `58086` 暴露出去。例如，已安装 Docker Desktop 时可运行一次性 Cloudflare Quick Tunnel：

```bash
docker run --rm --name trpc-feishu-tunnel \
  cloudflare/cloudflared:latest tunnel --no-autoupdate \
  --url http://host.docker.internal:58086
```

从 tunnel 输出中复制 `https://<temporary-host>.trycloudflare.com`。在 Feishu 开放平台的“事件与回调”中选择 **Webhook 接收事件**，而不是“长连接接收事件”，并配置：

```text
https://<temporary-host>.trycloudflare.com/callbacks/feishu?route_key=local-feishu
```

使用同一份 `Verification Token` 和 `Encrypt Key` 完成 URL 验证，订阅 `im.message.receive_v1`，开启机器人能力及 `im:message` 权限，并在每次变更后发布应用版本。先私聊机器人发送一条新消息；成功时机器人会调用 DeepSeek 并回复。群聊验收通过“群设置 → 群机器人 → 添加机器人”将机器人加入群，再从 @ 菜单选择机器人发送消息；普通未 @ 的群消息会被安全地忽略。

图片/文件验收必须在**私聊**中发送一张不超过 10 MiB 的新媒体。服务会用该应用的官方 Media API 下载原始内容、由本机 ClamAV 扫描、写入 PostgreSQL 的 tenant-scoped 临时制品，再交给 Runner；本地 Profile 的不可变 v2 版本固定为 `deepseek-v4-flash-vision-exp`，因此 JPEG、PNG、GIF 或 WebP 图片会作为视觉输入交给模型。DeepSeek 的该接口不提供通用 PDF/Office 文档理解：非图片文件仍会被安全下载、扫描和审计，但应明确提示当前本地模型不解析它们，而非谎称已经读取。`/readyz` 只有在 ClamAV 可用时才返回成功。当前本地 fixture 的输入 DLP 策略明确为 disabled（记录在制品元数据中），不应把它表述为已接入远端 DLP。飞书媒体-only 群消息没有可验证的 @mention 证据，因此会被安全忽略；请先用私聊验证媒体链路。

排查顺序：

1. `docker compose -f deploy/compose/docker-compose.local.yml --profile feishu-local logs feishu-local`：`app secret invalid` 表示 `FEISHU_APP_SECRET` 与 App ID 不匹配；更换 Secret 后必须重建服务。
2. `docker logs trpc-feishu-tunnel`：Quick Tunnel 重启会生成新域名，必须把新 URL 重新保存到 Feishu。
3. 若私聊成功但群聊无响应，确认使用 @ 菜单选择机器人，且 `FEISHU_BOT_OPEN_ID` 为当前机器人 Open ID。

App Secret 更新后的本地重启命令：

```bash
TRPC_LOCAL_FEISHU_PORT=58086 \
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile feishu-local up -d --force-recreate feishu-local
```

## 5. 可选：真实 WeCom 本地验收

此 smoke 运行完整的企业微信回调验签（GET URL 验证与加密消息）、durable ingress、Redis Worker、DeepSeek 调用和企业微信 `message/send` Reply API。数据库、Worker 和可观测性组件仍全部运行在 Docker Desktop；唯一的外部依赖是开发者自己的企业微信自建应用、DeepSeek Key 和临时 HTTPS tunnel。

先创建仅供本机使用的凭据文件：

```bash
mkdir -p deploy/compose/secrets
cp deploy/compose/wecom.env.example deploy/compose/secrets/wecom.env
chmod 600 deploy/compose/secrets/wecom.env
```

编辑 `deploy/compose/secrets/wecom.env`，填入同一个企业微信自建应用的 `WECOM_CORP_ID`、`WECOM_AGENT_ID`、`WECOM_CALLBACK_TOKEN`、`WECOM_ENCODING_AES_KEY` 和 `WECOM_APP_SECRET`。`WECOM_AGENT_ID` 必须为正整数，且四项凭据必须属于同一应用；`WECOM_AGENT_ID` 启动时会强校验。不要给值添加引号或反引号。这个文件被 Git 忽略，严禁强制加入版本库。

启动本地服务：

```bash
TRPC_LOCAL_WECOM_PORT=58087 \
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile wecom-local up -d --build

curl --fail http://localhost:58087/readyz
```

企业微信回调 URL 只接受 80/443 端口，因此同样需要临时 HTTPS tunnel 将宿主机 `58087` 暴露出去：

```bash
docker run --rm --name trpc-wecom-tunnel \
  cloudflare/cloudflared:latest tunnel --no-autoupdate \
  --url http://host.docker.internal:58087
```

从 tunnel 输出中复制 `https://<temporary-host>.trycloudflare.com`。在企业微信管理后台该应用的“接收消息 → 设置 API 接收”中配置：

```text
https://<temporary-host>.trycloudflare.com/callbacks/wecom?route_key=local-wecom
```

`Token` 与 `EncodingAESKey` 必须与 `wecom.env` 一致；保存时企业微信会发起 GET URL 验证，本地服务完成回调验签并返回 echostr 后握手成功。先在手机端给该应用发一条消息；成功时应用会调用 DeepSeek 并通过官方 Reply API 回复。媒体验收同样发送一张不超过 10 MiB 的新 JPEG、PNG、GIF 或 WebP 图片；它会经过企业微信官方 Media API、本机 ClamAV 与 tenant-scoped 临时制品后，以视觉模型输入进入 Runner。非图片文件只做安全接收和审计，不宣称其内容已被模型读取。

排查顺序：

1. `docker compose -f deploy/compose/docker-compose.local.yml --profile wecom-local logs wecom-local`：`WeCom local configuration is incomplete` 表示 `wecom.env` 缺字段或 `WECOM_AGENT_ID` 不是正整数；`existing WeCom local control plane is incompatible; recreate the Compose volume` 表示已持久化的绑定与当前 Corp ID 冲突，需按第 6 节清理卷后重建。
2. `docker logs trpc-wecom-tunnel`：Quick Tunnel 重启会生成新域名，必须把新 URL 重新保存到企业微信。
3. 回复失败且日志出现 `60020` 等 errcode：在企业微信管理后台“企业可信 IP”中放行本机出口 IP，否则 `gettoken` 会被拒绝。

App Secret 更新后的本地重启命令：

```bash
TRPC_LOCAL_WECOM_PORT=58087 \
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile wecom-local up -d --force-recreate wecom-local
```

## 6. 多节点 Compose 参考

`webui-multinode` profile 可用于本地观察两个独立 WebUI composition node；它们共享 PostgreSQL 和 Redis，并使用唯一 Worker、relay、delivery 和 wakeup consumer ID。该 profile 支撑依赖短断恢复演练，但不再作为 SIGKILL 接管的自动验收。

若要手动查看两个节点页面：

```bash
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile webui-multinode up --build
```

默认访问地址为 `http://localhost:58083/webui/` 和 `http://localhost:58084/webui/`。

停止本地环境但保留数据：

```bash
./stop.sh
```

需要丢弃所有本地测试数据、修改 Token 或更换 Key 时：

```bash
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile webui down -v
```

## 7. 本地验证命令

```bash
# 纯 Go 静态、单元与 race 检查（Go 1.25）
bash scripts/ci/admission.sh
bash scripts/ci/admission.sh --race

# Disposable PostgreSQL 16、Redis 7、Qdrant、Vault 真适配器 smoke
bash scripts/e2e/backend-adapter.sh

# PostgreSQL / Redis runtime slice
docker compose -f deploy/compose/docker-compose.local.yml up -d postgres redis
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile runtime-test run --rm runtime-test
```

`bash scripts/e2e/backend-adapter.sh` 成功后会删除它创建的容器和卷；失败时会保留临时 Compose 日志路径。除第 4、5 节中开发者亲自完成的 Feishu 与 WeCom real-account smoke 外，所有验收都只针对本机 Docker 环境，不得把 WebUI、fixture 或 fake adapter 的成功表述为云对象存储、DLP 或生产集群已通过。

每个验证项的命令、所需资源和成功证据汇总见 [verification-matrix.md](verification-matrix.md)。

## 8. 本地边界

- 结构化 JSON 日志默认脱敏；`TRPC_LOG_LEVEL` 可设 `debug|info|warn|error`，`TRPC_LOG_MASKING_LEVEL` 可设 `none|basic|strict`。
- 节点故障、IM 重试、PostgreSQL/Redis 短断、模型/Tool 超时的降级策略，以及灰度、回滚、容量和生产推荐拓扑，统一见 [`reliability-release-capacity.md`](reliability-release-capacity.md)。
- Collector 或 Jaeger 停止不会阻断本地业务链路；Trace 可见性是本地诊断辅助，而非远端 SLO 告警。
- MinIO、远端 DLP 和 Kubernetes 不属于本地闭环必需项；相关 adapter 与代码级测试保留，外部 smoke 和运维资产不再维护。ClamAV 是 Feishu/WeCom 图片或文件 smoke 的本地必需依赖，由 Compose 自动启动；它的官方多架构镜像首次拉取与病毒库初始化可能需要数分钟。Feishu 与 WeCom 只分别支持第 4、5 节所述的开发者自有账号、本地 Docker 和临时 tunnel smoke，不承诺生产可用性或 tunnel 的稳定域名。
- `deploy/compose/secrets/` 已被 Git 忽略；不得用 `git add -f` 加入 API Key 或其他凭据。
