# 安装、部署与运行手册

所有命令在仓库根目录执行。已有配置和数据库的环境不要重新 bootstrap，也不要复制模板覆盖 `.env`。

## 0. 第一次拿到源码：从空环境到可用平台

本节是一条完整的本地交付路径，不依赖作者电脑上的数据库、域名、workbuddy2api 或私有脚本。已有环境跳过本节，按升级章节处理；不要通过删数据卷获得“空环境”。需要 Go（版本见 go.mod）、Node.js 22.12+/24、npm、Docker Compose v2，以及可用的 8080/5432/6379 端口。

### 0.1 构建并准备私有配置

在新 clone 或源码包解压目录执行：

```bash
test -f .env || cp .env.example .env
chmod 600 .env
./build.sh
openssl rand -hex 24
openssl rand -hex 24
```

两次生成的不同值分别填入 `.env` 的 Admin Token 和 HTTP Token；不要提交或截图这些值。将模板中以下同名字段改为下列内容，其余字段保留默认值：

```dotenv
TRPC_AGENT_ADDR=127.0.0.1:8080
TRPC_AGENT_ROLE=all
TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres
TRPC_AGENT_POSTGRES_URL=postgres://trpc_agent:trpc_agent_dev@127.0.0.1:5432/trpc_agent?sslmode=disable
TRPC_AGENT_POSTGRES_AUTO_MIGRATE=false
TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=false
TRPC_AGENT_SESSION_BACKEND=redis
TRPC_AGENT_COORDINATOR_BACKEND=redis
TRPC_AGENT_IDEMPOTENCY_BACKEND=redis
TRPC_AGENT_QUEUE_BACKEND=redis
TRPC_AGENT_QUOTA_BACKEND=redis
REDIS_URL=redis://127.0.0.1:6379/0
TRPC_AGENT_ADMIN_ENABLED=true
TRPC_AGENT_ADMIN_TOKEN="第一份随机值"
TRPC_AGENT_HTTP_API_ENABLED=true
TRPC_AGENT_HTTP_API_TOKEN="第二份随机值"
```

这里的 PostgreSQL 开发账号与仓库 Compose 匹配，仅用于本地验证。不要把该账号用于公网或生产。模板默认 Mock 用于离线验证装配；要直接使用真实模型，在启动前同时填写第 1 节的五个模型字段。模型检查用 `./bin/trpc-modelcheck`，无需绑定作者的本地转换服务。

### 0.2 先启动依赖，再迁移和初始化

```bash
docker compose up -d --wait --wait-timeout 60 postgres redis
TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=true ./bin/trpc-migrate
./start.sh
curl -fsS http://127.0.0.1:8080/readyz
```

迁移命令读取 `.env`，仅本次命令显式启用示例初始化；文件中的 bootstrap 仍保持 false。成功后应看到 `database migration completed` 和 `{"status":"ready"}`。数据库中有 `tutorial-tenant`、`tutorial-app`、发布版本和 `tutorial-http` 绑定，Session 使用共享 Redis。不要先在未启动的 PostgreSQL 上执行迁移，也不要仅凭 healthz 判断模型或 IM 已可用。

### 0.3 登录、使用与创建第二个租户

1. 打开 `http://127.0.0.1:8080/admin/ui/`，使用刚设置的 **Admin Token** 登录。
2. 选择 `tutorial-tenant` → Agent 应用 → `tutorial-app`，在右侧创建独立调试会话。可连续发送“我叫小明”和“我叫什么”查看会话；切到真实模型后回复措辞不要求固定。
3. 创建第二个租户：例如 ID `tenant-b`、名称“第二租户”、region `local`、secret namespace `tenant-b`，配额/审计配置可先填 `{}`。secret namespace 只是元数据，不会自动授予密钥权限。
4. 切换至第二租户，在 Agent 应用中新建 ID `app-b`、名称“第二 Agent”。通过页面创建的应用会自动注册/继承 Session 后端；模型先选执行节点默认模型，设置提示词、工具白名单、预算后保存草稿并调试。
5. 发布后再建立该应用的 IM 或 HTTP 绑定。两个租户使用不同绑定；没有共享授权时，不能相互读取会话、记忆、知识库或使用对方工具。要选择不同模型凭据，先按治理文档增加精确 `model` grant，再重启执行节点；页面只能选择已授权引用。

HTTP 调用用第 3 节示例，使用 **HTTP Token** 而不是 Admin/模型 Token。curl 不会自动读取 `.env`，应在受控终端提供对应 Token，不要 `source .env`。真实 IM 按 [IM 接入](im-channels.md)为自己的账号注册，不要使用作者的域名或测试群。Telegram + 企业微信即可覆盖本项目选择的两类 IM。

### 0.4 启用对象存储、向量库、Skill 等功能

核心平台不要求这些资源全部启用。需要附件/知识库时，再执行：

```bash
docker compose up -d --wait --wait-timeout 60 minio qdrant
docker compose run --rm minio-init
```

第二条命令创建 `trpc-agent-artifacts` bucket，重复执行不会清空已有对象；超时/失败应先处理，不要让应用带着缺失 bucket 继续使用附件。仅启动 MinIO 容器不会创建 bucket。生产使用预先配置的 bucket 和专属身份；本地创建受限身份、填写凭据和 Backend Binding 的完整步骤见 [对象存储初始化](backend-adapters.md#61-本地-minio-初始化)。

- Session 默认 Redis；需要持久 Memory 时，注册 `memory` 后端及对应用途授权，不能把默认 InMemory Memory 当跨节点持久化。
- Knowledge 需要独立 Embedding 服务/Key、维度一致的 Qdrant 后端、租户授权与已发布知识配置；聊天 API Key 不会自动成为 Embedding Key，具体字段见后端方案第 5 节。
- Skill/沙箱按本手册第 8 节配置注册目录、租户授权和已安装镜像。未启用时页面应明确不可用，不能暗中回退为宿主机执行。

需要验证多节点时，在上述 all 进程保持运行的同时，在另一个终端执行 `./bin/trpc-service -env-file .env -role worker`。额外 Worker 不监听 8080，自动产生独立消费者身份，共享相同 PostgreSQL/Redis；不需要复制真实数据或启动第二套数据库。完整角色拆分及生产权限见第 4 节与治理文档。

完成上述路径后，交付方应能独立登录、创建应用、调试/发布、配置所选后端及通道。IM 账号开通、模型额度和生产 Secret 是接收方提供的外部条件，不随源码包提供。

## 1. 环境与配置

Go 版本以 [go.mod](../go.mod) 为准。从源码构建控制台还需要 Node.js 22.12+（推荐 24 LTS）与 npm；运行构建好的 Go 二进制不需要 Node。`build.sh` 按锁文件安装前端依赖、检查类型并构建页面，再编译 Go。手动启停需要 Linux、flock 和支持 pidfd 的内核；其他系统可直接以前台二进制或容器运行。持久化/多进程部署还需要 Docker Compose 或自行准备 PostgreSQL、Redis，以及按需使用的 MinIO/Qdrant。

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

生产将同一程序按 `gateway / relay / worker / sender / jobs / admin` 分别启动。多个 Worker 共享 Session、协调器、队列与控制面，不需要负载均衡 sticky session。`TRPC_AGENT_WORKER_CONCURRENCY` 默认 4（1～64），大于 1 时预留一个执行槽只接近期流量，其余槽公平处理两类队列。供应商并发上限和租户额度仍是额外约束，运行中的操作不会被强行抢占。

Kubernetes 模板位于 [deploy/kubernetes](../deploy/kubernetes/platform.yaml)。顺序是：准备分角色 Secret 与依赖 → 应用命名空间/网络策略 → 独立 migration Job → 六角色 Deployment/Service → Ingress。镜像地址、账号、namespace/Pod 标签与真实外连范围必须由部署者核对；模板验证不等于已在集群生效。

启用观测依赖：

```bash
docker compose --profile observability up -d
```

配置 `TRPC_AGENT_OTEL_ENABLED=true`、OTLP endpoint、service name 和采样率。Collector、Tempo、Prometheus、Grafana 的配置位于 `deploy/compose`；实际告警通知渠道另行设置。

## 5. 升级与回滚

1. 备份配置、当前二进制和数据库，先核对未完成工具及 unknown/attempting 发送事实。
2. 停止旧 Worker/Jobs/Sender，不能混跑不兼容的队列、权限或分段发送协议。
3. 构建，使用迁移身份应用缺失 migrations；当前控制面 schema 为 27，不能修改已应用 SQL 文件。026 增加持久化等待、调度代数和独立补读，027 增加 Run 收尾回执，不重建业务会话。Worker 需要 INSERT queue_outbox，Gateway 需要 UPDATE channel_poll_gap；同步更新权限后再启动新版本，不给 Worker 开放修改投递状态的权限。
4. 核对新增表/函数/Redis 命令权限，再启动候选实例，检查就绪和受控请求。
5. Agent 行为通过不可变 Revision、stable/canary 和 conversation pin 灰度；切回稳定 revision 不会自动迁移已 pin 的会话。
6. 数据迁移按[迁移协议](data-consistency.md)执行。回滚配置不会撤销已提交的工作项或已发送消息，不得恢复旧备份后盲目重放。

`0.3.0-rc.1` 的升级检查已在独立 PostgreSQL 上验证：基线 `0ce285f`（`0.2.0-rc.10`）可在保留 schema 24 新增表时启动、查询 Admin 并完成 HTTP Agent 执行。回退仍须先停止新版本、处理或保留在途调试任务，不混跑新旧实例；旧版不提供新工作台，也不会消费独立调试队列。此结论不代表任意历史版本、所有供应商或生产容灾均已验证。

## 6. 容量与恢复

所需活跃并发约为“峰值 turn/s × 平均执行秒数”。每节点业务并发默认 4，另有独立的网页调试执行并发 1，两者共享租户并发与预算限制。等待恢复不占执行槽，但占持久存储，需要监控 `run/waiting` 和 `backfill/pending/blocked` 积压。节点数量还需考虑模型配额、SQL 连接池和故障余量，不能用同步 `/chat` 并发推断异步队列容量。

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
- 企业微信消息 MCP 默认近期优先：近期循环先查最近窗口，另一个循环限速补读新记录的历史缺口。当前轮询间隔 5 秒、落盘等待 2 秒，响应仍受上游和模型影响；同时查看近期 `through_at` 与补读 `cursor_at/status`，不能仅以近期进度健康推断没有漏读。人工跳过历史仍需暂停绑定、明确时段并经带版本 recover 接口记录审计。群消息须满足 `mention_prefix`。

`./clean.sh` 默认预览；`--apply` 仅归档已知构建产物，运行 PID 存在时拒绝。私有快照和临时个人工具不属于交付仓库，数据卷也不能仅因停止或显示 reclaimable 就删除。

源码在本地提交干净后执行 `./build.sh --package`，只导出已提交文件至 `dist` 并生成 SHA-256；不会 push。不要直接压缩整个工作目录，私有 `.env`、`data` 和数据库卷不能交付。

## 8. 管理工作台、网页调试与可执行 Skill

管理工作台随 Agent 二进制内嵌，运行时不需要 Node/npm 或额外前端服务。配置 `TRPC_AGENT_ADMIN_ENABLED=true` 和已有的 Admin Token/Principals，在 admin/all 角色启动后访问 `http://127.0.0.1:8080/admin/ui/`，输入 **Admin Token**，不是模型或 IM Key。登录换取最长 8 小时的 HttpOnly 会话，刷新后恢复；长期 Token 不进入浏览器本地存储。远程浏览器登录要求 HTTPS，本机回环 HTTP 仅用于开发。

新版工作台从 `0.3.0-rc.1` 提供，需要 schema 24。先升级 Admin/Worker，再开放工作台。网页调试使用独立 SQL 调试队列，不会被旧版 IM Worker 误领；旧版本的管理页不支持新的登录会话。升级不会自动发布 Agent 版本或迁移已有 IM 会话。

当前 `0.3.0-rc.4` 需要 schema 27。“近期优先”的 30～120 秒是接收窗口，不是请求有效期；配置键 `max_age_seconds` 为兼容保留。已接收消息持久保存，模型尚未产生输出且无工具执行时，暂时连接故障进入 waiting，5/10/20/30 秒退避，不消耗普通执行错误的三次尝试。模型恢复后自动继续，管理页展示等待原因与下一次调度时间。每条请求最多一条等待提示；未发出的提示会在最终完成时撤回，已经发送或结果未知的提示不能撤销。

已经 completed 的请求从 Run/最终 Outbound 读取结果，不再进入 Agent。finalized_at 为空时只补审计、用量及幂等后台任务提交；有回执时只 ACK 重投。因此去重缓存过期不会重做已持久完成的模型/工具执行，收尾错误也不能被普通模型重试次数上限吞掉。该保证针对 IM 和 `/inbound` 持久入口；同步 `/chat` 是诊断接口，其去重缓存有明确 TTL。

绑定 JSON 示例仍为 `"message_policy":{"mode":"realtime","max_age_seconds":120}`。近期与后台补读分别持有租约、游标，共享 Inbox 去重；只读已授权群、成员及起点后的区间，超过源保留期或恢复下界冲突会 blocked，需管理员核对。补读消息按平台接收顺序进入会话，不倒插历史。启动不会重放旧版 dead/expired/disposition/skipped 记录。执行前重新核对路由和新任务记录的 Binding 版本，接入授权变化时终止并反馈，不自动沿用旧授权。

禁止混跑旧 Worker/Relay/Sender：旧代码不理解调度代数、收尾回执和新的回复唯一约束。Redis ACL 应覆盖原 stream 和同前缀 `-backlog` stream；现有按 queue 前缀授权的模板可复用。schema 24 的回退结论不覆盖本次升级，回退必须先停接收与消费、保留等待任务和出站事实，使用兼容 schema 27 完成态恢复语义的构建，不能直接恢复仍会重做 completed 任务的旧 Worker。

Docker 多阶段构建在 Node 阶段完成页面编译，运行镜像只包含 Go 程序。构建网络无法访问默认 Go 模块代理时，可传入 `--build-arg GOPROXY=https://goproxy.cn,direct`，按部署环境选择可信代理；不需要关闭 TLS 或校验和验证。

页面以 Agent 为中心组织操作：

- 在“Agent 应用”创建应用并进入工作台；展示名称、说明和接入状态在“应用设置”修改。新应用优先继承租户会话后端，没有默认绑定时注册部署者的 startup_config 会话后端。
- 在配置页编辑模型、提示词、工具、Skill、知识/Embedding、记忆和输入输出规则。模型与 Embedding 凭据从当前租户、当前用途的授权引用清单选择，不返回密钥值，也不检查环境变量是否存在。保存草稿不会修改线上版本；并发保存冲突会展示两份配置，按配置组选择合并，合并后仍需再次保存。MCP 等复杂字段保留高级 JSON 入口。
- 在右侧发送消息进行调试。首次发送会保存有写权限用户的草稿并创建不可变快照；新修改需显式开始新调试，不影响正在运行的请求。
- 调试使用独立身份，不发送 IM；外部写工具关闭，MCP 仅开放部署者授权的只读工具。Skill 仍需批准。审批、运行状态和工具记录来自后端，不以模型文字判断成功。
- 发布弹窗展示配置检查、具体差异和同配置的隔离调试记录。版本 ID/序号由后端生成；不可变版本列表支持继续翻页，发布历史另从审计事实展示操作人、操作时间、版本变化、回滚和灰度调整。旧记录缺少的字段不补造；审计保留期之外的操作不会显示。旧业务会话保持原版本。
- 在“运行记录”按应用、来源、时间和状态查看请求、耗时、Token/成本、工具与投递。成本使用部署者配置的计费单位，未配置价格时为 0，不表示供应商免费。通道详情只查询已有的接收/投递记录；企业微信 MCP 另展示消费进度和拒绝原因，不主动读取新消息。
- 请求详情关联带源 request_id 的后台任务，应用的“后台任务”页可分页查看摘要、记忆、知识同步与迁移状态。只展示任务元数据，不返回文档正文或原始供应商错误；旧任务没有关联编号时不推测归属，不自动重试。
- 在“系统状态”查看 Worker 心跳、默认 Session/队列/配额和 Docker 固定镜像的实际检查结果。Worker 每 15 秒做有界只读检查，每 5 秒上报；专用 Session 每轮最多轮询 32 个已初始化实例，不因检查创建后端表。Admin 只读取共享观测，超过 30 秒、尚未初始化或本轮未观测时显示未知。发布预检按应用后端和配置指纹匹配，保守汇总活跃 Worker；模型仍需显式调试，不自动生成。公网探测只访问部署者配置的 `TRPC_AGENT_PUBLIC_BASE_URL/healthz`，必须点击触发，不会自动启动 Tunnel。

网页调试每个会话最多 50 轮，有工具时要求明确的 1～32 次工具调用上限；执行最长 2 分钟，显示输出最多 64 KiB。数据保留 7 天，Worker 清理对应独立用户的原生 Session/Memory 后再清理控制台记录；后端不可用时保留清理目标重试。InMemory 仅适合单进程开发，多节点需 PostgreSQL 和共享 Session 后端。节点中断产生未知结果时不会盲目重跑。

启用内置示例 Skill（替换为实际授权租户，不覆盖其他 grants）：

```dotenv
TRPC_AGENT_SKILLS_ROOT=./skills
TRPC_AGENT_SKILL_GRANTS_JSON='[{"tenant_id":"tutorial-tenant","name":"json-digest","version":"1"}]'
TRPC_AGENT_SANDBOX_ENABLED=true
TRPC_AGENT_SANDBOX_IMAGE=alpine:3.22
TRPC_AGENT_SANDBOX_SOCKET=/var/run/docker.sock
```

Worker 所在主机需已安装 Docker CLI，并且所指定 daemon 已有该镜像；服务不会自动 pull 或修改 daemon。镜像必须提供 /bin/sh 和 /bin/busybox，启动时解析并固定 image ID。默认容器部署模板不授予 Docker socket 权限；容器化 Worker 启用沙箱前须单独准备可信 Docker CLI、只读 Skill 挂载和专用 daemon 访问，不应把生产主机 root socket 直接共享给所有应用。

在页面选择租户 → Agent 应用 → 工作台 → 勾选已授权 Skill。页面写入完整 name/version/checksum，并加入 skill_load、skill_run 工具白名单；原有权限和配置保留。保存草稿后可以直接在网页调试，不需要 Tunnel 或新建 Telegram Topic。验证配置后再显式发布到业务使用；已 pin 的 IM 会话不会自动换版本。

保存前点击“检查配置”。可执行 Skill 的 `tool_policy.max_tool_calls` 至少为 2，初次测试可明确设为 4；1 只够加载说明，不能完成接下来的执行。平台会阻止这种配置，不自动提高限额。0 表示不限次数而不是禁用工具。检查结果会区分错误、警告与运行依赖未知；静态配置检查不会消耗模型额度，也不证明真实调用已经通过。详细接口见[治理说明](governance-operations.md)。

json-digest 示例计算输入 JSON 文件字节数和 SHA-256，是真正的脚本执行，不调用模型生成假结果。Agent 先用 skill_load 读取说明，再请求 skill_run；首次返回审批指令，用户批准后执行固定 run.sh，脚本从 /workspace/input.json 读取输入，stdout/stderr 作为有界工具结果返回。不能通过模型的一句“已执行”判断成功，应同时核对执行 Journal。

自定义 Skill 在 root 的 catalog.json 增加注册，提供新的目录和版本；不要原地覆写已发布版本。目录只加载 SKILL.md/run.sh，其他文件不自动挂载。授权、镜像和 root 均为部署者配置，租户只能选择获授权的固定版本，页面不提供未审核脚本上传。

现有回归入口为 `./scripts/regression.sh`，会先构建控制台；隔离后端检查仍可使用 `TRPC_AGENT_VERIFY_ISOLATED=1`。不把代码编译、页面调试或隔离环境检查扩大为真实 IM/生产上线验收，边界见[验收说明](acceptance.md)。
