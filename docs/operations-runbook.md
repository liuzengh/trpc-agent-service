# 本地运行与升级手册

这份文档是日常操作入口。按手动测试方式运行，不设置开机自启；模型服务、Agent、数据依赖和公网 Tunnel 是不同的进程。学习内部链路看[上手指南](getting-started.md)，核对完成范围看[功能状态](feature-status.md)。

## 1. 第一次安装和配置

需要 Go（以 `go.mod` 为准，当前声明 1.24；Docker 构建使用 1.25）、Docker Compose 和 curl。手动 Agent 启停脚本需要 Linux、flock 和支持 pidfd 的内核；其他平台可直接管理 `go run`/二进制进程。使用本机 workbuddy2api 还需要已配置好的 `uv` 和 converter；Telegram Webhook 需要公网 HTTPS 入口，企业微信消息 MCP 主动拉取不需要公网回调。

在仓库根目录运行，已有 `.env` 不要覆盖：

```bash
test -f .env || cp .env.example .env
chmod 600 .env
./build.sh
```

`.env.example` 默认是内存后端 + Mock，HTTP/Admin 和 MCP 轮询默认关闭。真正的日常 IM 配置包含以下几组，不是只填模型三项就能完成接入：

| 配置组 | 保存位置与作用 |
| --- | --- |
| 模型 | `.env` 的 provider/name/key/base URL；本地转换服务使用已验证的模型 ID |
| PostgreSQL | 控制面、Inbox/Run/Outbox、审批、审计、接收与发送事实；连接信息只在 `.env`/Secret |
| Redis | Session、Coordinator、Idempotency、Queue、Quota 的后端开关和 URL/prefix |
| IM 凭据与授权 | `.env` 保存 Token/完整 MCP URL；`TRPC_AGENT_SECRET_GRANTS_JSON` 精确授权租户和用途 |
| Channel Binding | 保存在 PostgreSQL，含租户/App、账号、群/成员白名单、SecretRef；不把 Token 填进 Binding |
| 接收开关 | Telegram 用已登记的 Webhook；企业微信另需 `TRPC_AGENT_WECOM_MCP_TARGETS_JSON` |
| 本地运维 | HTTP/Admin 默认关闭；按需启用、鉴权；OTel 指向本地 Collector |

仅在全新的开发数据库上，设置 PostgreSQL 控制面、`TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=true` 后执行一次 `./bin/trpc-migrate` 创建教学租户；随后改回 `false`，不要覆盖已经发布的教学配置。新通道按 [Telegram 手册](telegram-manual-runbook.md)或[企业微信 MCP 配置](wecom-mcp-runtime.md)绑定。生产不使用教学账号或自动 bootstrap。

一般配置优先级是命令行参数 > 已导出的环境变量 > `.env` > 默认值。`start-real.sh` / `check-model.sh` 会清掉旧的模型环境变量并强制 OpenAI-compatible 模式，让模型配置取自选定的 env 文件；**其他变量仍遵循环境优先**。不需要为正常启动 `source .env`。

## 2. 每次启动

终端 A，模型来自本机 workbuddy2api 时：

```bash
./start-workbuddy2api.sh
```

保持这个前台终端运行。默认目录是 `~/workbuddy2api`；参数是原来的 `--desensitize --log converter.log --api-key 0`。`WORKBUDDY2API_DIR` 是启动脚本自己的环境变量，不从项目 `.env` 加载。Key `0` 仅用于本机测试，不应对公网开放转换服务。

终端 B，本机日常实例已配置 PostgreSQL、Redis、MinIO 和 Qdrant：

```bash
docker compose up -d postgres redis minio qdrant
./check-model.sh
./start-real.sh
curl -fsS http://127.0.0.1:8080/readyz
```

模型检查会真实调用一次模型，但不启用 Bot。`start-real.sh` 检查已有 Compose PostgreSQL/Redis/MinIO/Qdrant 的运行或健康状态，**不会替你创建依赖**；未配置对应后端的开发环境可省略它们。随后核对 Agent PID 身份、记录启动时间/boot ID，并等待 HTTP 角色的 `/readyz`，避免仅凭进程存在就报告启动成功。非 HTTP 角色只确认进程，依赖可用性另查。主程序存在时不重新编译它，运维辅助程序会重新构建。

重复启动不会覆盖正在运行的 Agent。日志采用追加方式，历史日志保留；PID 与元数据文件在 `data`，不要复制到另一台机器当作运行状态。MinIO/Qdrant 数据和密钥保持原配置，不必因正常重启重新初始化。

需要 trace/metrics，再按需启动监控组件：

```bash
docker compose --profile observability up -d otel-collector prometheus tempo grafana
```

修改过 Compose 端口/网络配置后，`up` 可能重建相关容器。不要在聊天或业务操作进行中更新共享依赖；数据卷需保留。`/healthz` 仅表明进程存活，`/readyz` 才检查本角色所需依赖；两者都不能证明模型或 IM 平台此刻可用。

终端 C，仅 Telegram 需要，使用你已经保存的 Named Tunnel 配置：

```bash
cloudflared tunnel --config /home/shiyu/.cloudflared/config.yml run trpc-agent-telegram
```

这个路径和名称属于本机示例，其他机器换成自己的配置。企业微信 MCP 无需启动这个 Tunnel，也不用把 MCP URL 填成回调地址。

正常使用只需要从已授权的 Telegram/企业微信群发送消息，不必再重复配置 Binding、重新设置 Webhook或开启 HTTP 调试入口。

### 一次查看当前状态

```bash
./status.sh
```

它读取配置并检查 Agent PID/readyz、PostgreSQL ping、Redis PING、模型 `/models`、本地 MinIO/Qdrant，以及 Compose 运行状态，不启动服务、不调用模型生成或发送 IM。模型列表能访问不等于聊天一定成功；若供应商以 404/405 表示不支持 `/models`，显示 unknown，另用 `check-model.sh`。连接失败、鉴权失败或其他非成功响应显示 down，不因缺少生成测试而忽略故障。MinIO/Qdrant 探针针对本仓库 Compose 的 loopback 端口，不代替远程后端审计。

要同时检查公网入口，在私有 `.env` 设置已有的域名，不带回调路径或凭据：

```dotenv
TRPC_AGENT_PUBLIC_BASE_URL=https://your-existing-domain.example
```

`ok` 表示相应探针成功，`down` 表示已配置项失败，`unknown` 表示该检查无法确认。存在 down 时返回非零退出码；unknown 不会被误写成健康。公网 healthz 正常只说明入口可达，不能证明 Telegram API 出站正常。

## 3. 修改代码或 `.env` 后怎样升级

改文件不会热更新已有进程。先确认没有正在等待的模型/危险工具操作；不要在真实群测试中运行其他实验脚本抢消费同一条队列。

1. 保存私有 `.env`、当前二进制及数据库备份，备份权限设为仅本人可读。旧备份保留，不在公共日志输出内容。
2. 用 `./stop.sh` 优雅退出 Agent。脚本核对工作区可执行文件、cwd、已记录的启动时间/boot ID，再通过 pidfd 对同一个进程发 SIGTERM，最多等待 20 秒；超时不强杀、不删除 PID 记录。旧格式 PID 文件也须匹配 exe/cwd，不能指向任意进程。模型和 Tunnel 不由这个脚本停止。
3. 执行 `./build.sh` 编译新二进制。
4. PostgreSQL 升级使用 `./bin/trpc-migrate`；它从仓库根目录 `.env` 和进程环境加载配置。确认 `TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=false`。分角色部署必须用独立迁移凭据，不能给 runtime 提升 DDL 权限。
5. 执行 `./start-real.sh`，再检查 `/readyz`；确认没有反复启动失败，再发一条新的测试消息。

当前代码 `0.2.0-rc.9` 对应 schema **23**，rc.5–rc.9 没有新增平台 migration；从此前 schema 16 升级需应用 **017–023**，迁移器会依据已应用记录跳过旧版本。新增内容包括审计函数、迁移协调元数据和回复分段记录。不要修改已应用 migration 的内容，也不要为回滚二进制反向删除新表。分角色部署须同步核对 SQL/Redis 权限；升级前排空或人工核对旧版未完成的发送记录，并停止旧 Worker/Jobs/Sender，不能混跑新旧并发配额和发送协议。

本机已在 2026-09-07 经用户授权完成 rc.4 升级，具体备份、配置变化及尚待真实消息验证的项目见[本地升级记录](validation/local-rc4-upgrade-2026-09-07.md)。这不等于生产上线；生产 SQL/Redis 账号、网络策略和告警通知接收方仍需部署者核对，见[部署权限](deployment-permissions.md)和[监控](monitoring.md)。

后续已部署 rc.5，并配置 PostgreSQL 长期记忆，见[Memory 启用记录](validation/memory-postgres-2026-09-07.md)。Memory 表由运维账号预建，运行时使用受限凭据和 `skip_db_init=true`。rc.5 增加可信会话类型传递及私聊记忆工具限制；不要用不识别该策略的旧 Worker 执行新 revision。

rc.6 增加[可选的只读项目文档 MCP](project-docs-mcp.md)，schema 仍为 23。本机启用和真实 IM 验证状态见[记录](validation/project-docs-mcp-2026-09-08.md)。启用后它随 Agent 在 loopback 独立端口启动和关闭，无需新增终端；MCP 密钥和服务开关都保存在 `.env`。文档索引为启动快照，修改白名单文档后需要重启刷新。

rc.7 补齐 [Telegram 出站诊断](telegram-delivery-diagnostics.md)。新增的是错误分类和 HTTP 阶段，不是自动恢复承诺；收到消息、模型执行完成、回复送达必须分别核对。`dead + unknown` 不会因为重启而重发，也不能靠扩大重试次数解决。

rc.8 将 Embedding 预检与正式 Knowledge 接到同一个安全包装层，新增本地 Qdrant API Key 配置。启用真实 Knowledge 后必须保留 Qdrant 数据卷、密钥、1024 维绑定和对应 revision；不要只启动聊天模型就假定知识检索可用，见[运行链路](knowledge-runtime.md)。

rc.9 增加每工具的用户白名单及仅私聊限制，用于[本地工作项审批](validation/workitem-approval-2026-09-08.md)。先升级 Worker 再发布新权限字段；配置回滚不得自动撤销已提交的业务写入，待批及不确定操作仍须按原请求核对。

## 4. 测试结束如何停止

```bash
./stop.sh
```

确认 Agent 已退出后，在 Tunnel 与 workbuddy2api 的前台终端分别按 Ctrl+C。如果没有其他程序使用这些依赖，再执行：

```bash
docker compose stop postgres redis minio
docker compose --profile observability stop otel-collector prometheus tempo grafana
```

不删除 `.env`、Tunnel 凭据或数据卷，不执行 `down -v`。停止模型或数据库不会自动停止 Agent；错误恢复测试之外，应先停 Agent 再停依赖。

## 5. 常见问题

| 现象 | 先检查什么 |
| --- | --- |
| 6379 connection refused / Redis LOADING | Redis 是否启动并 healthy；不要只重新填写模型 Key |
| 模型检查通过，Agent 启动失败 | PostgreSQL/Redis、迁移权限、端口占用和角色配置；模型检查不覆盖这些 |
| cloudflared 8080 connection refused | 本地 Agent 是否启动；这不是域名 DNS 配置成功就能解决的 |
| 已改 `.env` 但行为没变 | 旧进程未重启、已有环境变量覆盖、选错 env 文件或运行的是旧二进制 |
| HTTP 401/403/404 | 调试入口默认关闭；核对调用方 Token、Binding/租户/用户授权和进程角色 |
| Telegram 群不回复 | Privacy Mode 的投递规则、Bot 是否在群、Binding 白名单、@/command/reply 条件 |
| 企业微信 MCP 不回复 | 目标列表、群/人类白名单、精确 @ 前缀、Binding 状态、检查点和隔离记录 |
| unknown/attempting 发送状态 | 先核对发送证据；超时不代表没发出，禁止直接删记录或盲目重发 |
| 没看到告警通知 | 当前只有指标/规则，尚未配置真实通知接收方 |

日志在 `data/trpc-service.log`；分享排障信息时只给错误类型、request_id/trace_id 和相关时间，不粘贴完整 `.env`、MCP URL、Bot Token、数据库 URL 或原始聊天内容。更多恢复入口见[通道恢复](channel-recovery.md)。
