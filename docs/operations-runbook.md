# 安装、部署与运行手册

所有命令在仓库根目录执行。已有配置和数据库的环境不要重新 bootstrap，也不要复制模板覆盖 `.env`。

## 1. 环境与配置

Go 版本以 [go.mod](../go.mod) 为准。手动启停需要 Linux、flock 和支持 pidfd 的内核；其他系统可直接以前台二进制或容器运行。持久化/多进程部署还需要 Docker Compose 或自行准备 PostgreSQL、Redis，以及按需使用的 MinIO/Qdrant。

```bash
test -f .env || cp .env.example .env
chmod 600 .env
./build.sh
```

模板使用 Mock Model 和 InMemory，不需要模型密钥或数据库；HTTP 聊天、Admin、外部 IM 默认关闭。进程环境变量优先于 dotenv 文件，配置在启动时读取。指定其他文件可用 `TRPC_AGENT_ENV_FILE=/path/dev.env ./start.sh`；脚本不把 dotenv 当 shell 代码执行。

真实模型在 `.env` 配置：

```dotenv
TRPC_AGENT_MODEL_PROVIDER=openai
TRPC_AGENT_MODEL_NAME="部署者选择的模型 ID"
OPENAI_API_KEY="服务端密钥"
OPENAI_BASE_URL="https://provider.example/v1"
TRPC_AGENT_MODEL_STREAM=false
```

兼容服务须支持 OpenAI Chat Completions；本机模型转换服务由部署者单独启动。不要把简单本地测试 Key 的模型接口暴露到公网。聊天模型与 Embedding 分别配置、分别授权，不能混用。

启用持久化平台时设置：

```dotenv
TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres
TRPC_AGENT_SESSION_BACKEND=redis
TRPC_AGENT_COORDINATOR_BACKEND=redis
TRPC_AGENT_IDEMPOTENCY_BACKEND=redis
TRPC_AGENT_QUEUE_BACKEND=redis
TRPC_AGENT_QUOTA_BACKEND=redis
TRPC_AGENT_POSTGRES_AUTO_MIGRATE=false
TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=false
```

同时提供 `TRPC_AGENT_POSTGRES_URL` 和 `REDIS_URL`。首次空环境由迁移身份执行 `./bin/trpc-migrate`；若需要示例租户，可只在首次初始化时显式启用 tutorial bootstrap，随后关闭。应用账号不应获得 DDL 权限。Backend Binding 与 Secret grant 见[后端方案](backend-adapters.md)和[治理说明](governance-operations.md)。

## 2. 手动启动、状态和停止

Mock 和真实模型统一使用同一个入口，模型由配置决定：

```bash
# 仅对选择了这些后端的环境执行；不需要的依赖可省略。
docker compose up -d postgres redis minio qdrant
./start.sh
curl -fsS http://127.0.0.1:8080/readyz
./bin/trpc-local status
```

启动前检查已有 Compose 依赖状态；成功启动须通过 PID 身份核对和 HTTP 就绪检查。不会自动创建依赖、启动模型、注册 IM 或配置开机自启。主程序已存在时不会自动重编译；改代码先正常停止，再构建启动。

`trpc-local status` 只读检查 PID、readyz、配置的数据库/Redis、模型列表和本地对象/向量服务。模型列表成功不等于生成成功；不支持列表的 404/405 为 unknown，连接/鉴权失败为 down。可在 `.env` 配置不含凭据的 `TRPC_AGENT_PUBLIC_BASE_URL` 检查公网 healthz，但这不能证明 IM 出站可达。

需要单独诊断时直接使用已构建的命令，无额外 shell 包装：

| 命令 | 行为 |
| --- | --- |
| `./bin/trpc-modelcheck -env-file .env` | 一次真实模型生成，会消耗模型额度 |
| `./bin/trpc-embeddingcheck -env-file .env` | 一次合成文本 Embedding 请求，不导入知识库 |
| `./bin/trpc-wecomcheck -env-file .env` | MCP initialize/tools-list，不读取业务消息或发送 |
| `./bin/trpc-local status -env-file .env` | 只读依赖探针，不调用生成 |

检查器同样遵循“进程环境优先”；若终端遗留了旧配置，先清除对应环境变量。不要在命令行或日志中传真实密钥。

停止：

```bash
./stop.sh
```

脚本共享生命周期锁，核对工作区 exe/cwd、PID 启动时间与 boot ID，通过 pidfd 对同一进程发送 SIGTERM，最多等待 20 秒；身份不符或超时不强杀、不删除证据。仅停止 Agent，模型、Tunnel 和数据库由部署者单独管理。

## 3. HTTP、Admin 和 IM

HTTP 调试入口默认关闭；启用时必须配置强随机 Token 和精确的租户/Binding/用户授权，见[访问控制](governance-operations.md#1-访问控制)。`/chat` 返回完整 JSON；`/inbound` 在 Inbox/Run/Outbox 原子提交后返回 202，异步结果由 Sender 处理。

使用已初始化的 tutorial HTTP Binding，可在受控终端单独注入对应 HTTP Token 后调用：

```bash
curl -fsS http://127.0.0.1:8080/chat \
  -H "Authorization: Bearer $TRPC_AGENT_HTTP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"binding_key":"tutorial-http","message_id":"client-001","user_id":"alice","session_id":"demo","message":"你好"}'
```

新消息使用新的 message_id，重试原消息复用原 ID；保持 user_id/session_id 延续会话。不要把模型或 IM Token 当成 HTTP Token，也不要为了 curl 把整份私有配置公开到日志。

Admin 仅在 `TRPC_AGENT_ADMIN_ENABLED=true` 时注册，使用独立 Bearer Principal。所有管理调用为 POST，常用资源如下：

- `/admin/tenants`、`/admin/apps`、`/admin/revisions`：创建租户、应用和不可变版本。
- `/admin/revisions/publish`、`/admin/apps/rollout`：带 expected_version 的发布和灰度。
- `/admin/channel-bindings`、`/admin/backend-bindings`：绑定渠道与数据后端。
- `/admin/tenants/policies`：带版本更新审计和配额策略。
- `/admin/tool-operations/get`、`/admin/tool-operations/reconcile`：查询或仅依据后端事实对账。

请求结构以 [Admin Handler](../trpcservice/admin/handler.go)及对应类型为准，鉴权后仍按 tenant_id 检查权限。生产只从内网或身份代理访问 Admin。IM 接入步骤见[通道文档](im-channels.md)，公网入口只转发 Gateway，不公开管理、数据库或模型服务。

## 4. 容器与多节点

```bash
docker build -t trpc-agent-service:local .
```

镜像为非 root 运行，排除私有配置、数据、`bin`、`dist` 和日志。最小部署是一个 all 进程加共享 PostgreSQL/Redis；按需使用对象、向量和观测后端。

生产将同一程序按 `gateway / relay / worker / sender / jobs / admin` 分别启动。多个 Worker 共享 Session、协调器、队列与控制面，不需要负载均衡 sticky session；单个异步 Worker 当前一次处理一个任务。

Kubernetes 模板位于 [deploy/kubernetes](../deploy/kubernetes/platform.yaml)。顺序是：准备分角色 Secret 与依赖 → 应用命名空间/网络策略 → 独立 migration Job → 六角色 Deployment/Service → Ingress。镜像地址、账号、namespace/Pod 标签与真实外连范围必须由部署者核对；模板验证不等于已在集群生效。

启用观测依赖：

```bash
docker compose --profile observability up -d
```

配置 `TRPC_AGENT_OTEL_ENABLED=true`、OTLP endpoint、service name 和采样率。Collector、Tempo、Prometheus、Grafana 的配置位于 `deploy/compose`；实际告警通知渠道另行设置。

## 5. 升级与回滚

1. 备份配置、当前二进制和数据库，先核对未完成工具及 unknown/attempting 发送事实。
2. 停止旧 Worker/Jobs/Sender，不能混跑不兼容的队列、权限或分段发送协议。
3. 构建，使用迁移身份应用缺失 migrations；当前控制面 schema 为 23，不能修改已应用 SQL 文件。
4. 核对新增表/函数/Redis 命令权限，再启动候选实例，检查就绪和受控请求。
5. Agent 行为通过不可变 Revision、stable/canary 和 conversation pin 灰度；切回稳定 revision 不会自动迁移已 pin 的会话。
6. 数据迁移按[迁移协议](data-consistency.md)执行。回滚配置不会撤销已提交的工作项或已发送消息，不得恢复旧备份后盲目重放。

## 6. 容量与恢复

所需活跃并发约为“峰值 turn/s × 平均执行秒数”。当前每 Worker 的异步执行并发为 1，节点数量还需考虑模型供应商配额、SQL 连接池和故障余量；不能用同步 `/chat` 的并发推断异步队列容量。

`trpc-loadgen` 测量 `/inbound` ACK 吞吐和分位延迟；完整容量还要测队列排空时间、最终完成/送达数、token、成本、SQL/Redis QPS、GC、取消时延和失败率。真实模型压测必须先设预算与供应商限额，不使用生产 IM 群压测。

- PostgreSQL：全量备份加 WAL/PITR，恢复到独立实例后核对业务与发送事实。
- Redis：持久化、复制和恢复演练，禁止清空预算/幂等/租约键以“解决”错误。
- S3/MinIO：版本化、权限、对象 checksum 与独立恢复；重启可读不等同于灾备成功。
- Qdrant：snapshot 备份与恢复校验，更换 Embedding 模型/维度需重建向量。

`./scripts/e2e-backup-restore.sh` 只演练独立合成 SQL/Redis 数据的恢复工具链，不恢复日常业务库。完整验证入口见[验收说明](acceptance.md)。

## 7. 排障、清理与打包

日志位于 `data/trpc-service.log`。只分享错误类别、时间和 request_id/trace_id；不要粘贴完整日志、原始会话、MCP URL 或 dotenv。

- 无回复：依次检查模型、Agent readiness、公网入口、Inbox/Run/Outbound 状态，不能把“模型完成”当作“发送成功”。
- 队列持续错误：检查依赖、所有权和退避，不清队列强行恢复。
- unknown/attempting：先核对供应商或工具业务事实，不自动重发，也不直接手改成成功。
- MCP 接收卡住：用 Admin 的 channel-rejections/checkpoints 查询和带版本 recover 接口；不能清空 seen 记录跳过历史缺口。

`./clean.sh` 默认预览；`--apply` 仅归档已知构建产物，运行 PID 存在时拒绝。私有快照和临时个人工具不属于交付仓库，数据卷也不能仅因停止或显示 reclaimable 就删除。

源码在本地提交干净后执行 `./build.sh --package`，只导出已提交文件至 `dist` 并生成 SHA-256；不会 push。不要直接压缩整个工作目录，私有 `.env`、`data` 和数据库卷不能交付。

## 8. 管理页面与可执行 Skill

管理页面随 Agent 二进制内嵌，无需 Node/npm，也不需要额外启动脚本。配置 `TRPC_AGENT_ADMIN_ENABLED=true` 和已有的 Admin Token/Principals，在 admin/all 角色启动后访问 `http://127.0.0.1:8080/admin/ui/`，输入 **Admin Token**，不是模型或 IM Key。

该能力从 rc.10 提供，数据库 schema 仍为 23，无新增迁移。先升级相关 Admin/Worker 再发布带 skills 字段的新版本，不要混跑无法识别新字段的旧 Worker；本轮代码开发不自动修改现有 `.env` 或重启实例。

页面支持创建/查询租户和应用、修改租户策略、创建不可变版本、发布/回滚、灰度策略、通道注册/更新、后端注册、Skill 选择和审计查询。基础字段或后端不提供随意覆盖；已有后端变更仍走迁移流程。只读角色只能查询，刷新页面会清除登录凭据。

启用内置示例 Skill（替换为实际授权租户，不覆盖其他 grants）：

```dotenv
TRPC_AGENT_SKILLS_ROOT=./skills
TRPC_AGENT_SKILL_GRANTS_JSON='[{"tenant_id":"tutorial-tenant","name":"json-digest","version":"1"}]'
TRPC_AGENT_SANDBOX_ENABLED=true
TRPC_AGENT_SANDBOX_IMAGE=alpine:3.22
TRPC_AGENT_SANDBOX_SOCKET=/var/run/docker.sock
```

Worker 所在主机需已安装 Docker CLI，并且所指定 daemon 已有该镜像；服务不会自动 pull 或修改 daemon。镜像必须提供 /bin/sh 和 /bin/busybox，启动时解析并固定 image ID。默认容器部署模板不授予 Docker socket 权限；容器化 Worker 启用沙箱前须单独准备可信 Docker CLI、只读 Skill 挂载和专用 daemon 访问，不应把生产主机 root socket 直接共享给所有应用。

在页面选择租户 → 版本与发布 → 创建或复制版本 → 勾选已授权 Skill。页面会写入完整 name/version/checksum，并加入 skill_load、skill_run 工具白名单；原有权限和配置应保留。保存后显式发布。已 pin 的会话不会自动换版本，测试应使用新会话或受控迁移。

json-digest 示例计算输入 JSON 文件字节数和 SHA-256，是真正的脚本执行，不调用模型生成假结果。Agent 先用 skill_load 读取说明，再请求 skill_run；首次返回审批指令，用户批准后执行固定 run.sh，脚本从 /workspace/input.json 读取输入，stdout/stderr 作为有界工具结果返回。不能通过模型的一句“已执行”判断成功，应同时核对执行 Journal。

自定义 Skill 在 root 的 catalog.json 增加注册，提供新的目录和版本；不要原地覆写已发布版本。目录只加载 SKILL.md/run.sh，其他文件不自动挂载。授权、镜像和 root 均为部署者配置，租户只能选择获授权的固定版本，页面不提供未审核脚本上传。

验证：`TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh` 包含真实沙箱和 Skill Runner；浏览器 E2E 的可选依赖与命令见[验收说明](acceptance.md)。所有测试使用合成输入和独立环境，不读取现有会话或发送真实 IM。
