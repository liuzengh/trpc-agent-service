# 本地部署

本指南提供单进程网页、PostgreSQL、企业微信/飞书文本和可选 Collector 的运行方式。生产拓扑、配置前提与运维策略见[生产部署设计](production-deployment.md)，当前能力与验证结果见[实现与验证](acceptance.md)。

## 环境与版本

需要 Git、Bash、curl 和 Go >= 1.24.1。配置文件启动和自动演示检查另需 Node.js >= 22；默认 `./start.sh` 不需要 Node。可选 PostgreSQL/Collector 需要 Docker Compose v2 和可用 Docker daemon。首次克隆、Go 模块下载及容器镜像下载需要网络；默认 echo 演示在构建完成后不访问外部模型、数据库或 CDN。

源码与启动示例见 [README](../README.md#快速开始)。请从干净检出构建，切换版本后重新运行 `./build.sh`。

## 默认网页

在干净检出的项目根目录执行：

```bash
./build.sh
TRPC_SERVICE_STORAGE_PROFILE=inmemory \
TRPC_SERVICE_SESSION_COORDINATION=inmemory \
TRPC_SERVICE_WECOM_ENABLED=false \
TRPC_SERVICE_FEISHU_ENABLED=false \
TRPC_SERVICE_TELEMETRY_ENABLED=false \
./start.sh
curl --fail http://127.0.0.1:8080/healthz
node scripts/verify-reference.mjs http://127.0.0.1:8080
```

打开 [网页聊天](http://127.0.0.1:8080/)。预置 `demo/echo` 使用实际 LLMAgent、Runner 和 InMemory Session，模型为确定性回显，默认无需 API Key。真实模型的 SecretRef 授权和 Revision 发布步骤见 [README](../README.md#运行真实模型)。切换代码版本后重新执行 `./build.sh`，现有启动脚本只在二进制不存在时自动构建。

自动检查只用于可丢弃的 `demo/echo` 环境，会创建测试 Revision 并临时发布，退出时恢复原发布；不要对正在使用的 Bot 或业务库运行。端口占用时在启动命令前设置 `TRPC_SERVICE_ADDR=127.0.0.1:18082`，对应替换所有检查 URL；先确认该端口可用。一个检出只有一套 PID/日志文件，不在同一检出内并行启动两份。

```bash
./stop.sh
```

`stop.sh` 发送 SIGTERM 后返回，可能尚在完成 HTTP/Runner 排空；重新启动前确认旧进程和监听端口已退出。默认 InMemory 会话随退出消失，日志、Admin Key 与 PID 的位置见 [data 目录说明](../data/README.md)。

## 私密环境文件

```bash
install -m 600 deploy/local.env.example data/local.env
node scripts/start-local.mjs data/local.env
```

首次准备时才运行 `install`，再次运行会覆盖已填配置。私下编辑 `data/local.env` 后再启动；示例字段为空，不含真实 Bot 或模型密钥。`start-local.mjs` 使用 Node 的 `loadEnvFile` 按数据解析，不执行 shell 替换，不输出环境值；已导出的同名环境变量优先。多个文件按参数顺序读取，较早文件的值优先（包括空值），因此凭据文件放在带空占位字段的模板配置之前。文件变更需要停止旧进程后再启动，运行中的服务不会热加载。仅示例模板可提交，私密文件保持 0600 并留在已忽略的 `data/` 目录。

## 可选 PostgreSQL 与企业微信

先启动已有本地数据库配置。下列用户名、口令和端口是公开开发占位值，不能作为生产配置：

```bash
docker compose -f deploy/docker-compose.session.yml up -d --wait postgres
```

在 `data/local.env` 中把 `TRPC_SERVICE_STORAGE_PROFILE` 改为 `postgres`，沿用模板 DSN、`POSTGRES_SCHEMA=public` 和 `SESSION_COORDINATION=inmemory`。这是一进程部署，不需要 Redis。Docker daemon 若在远端，必须先提供本机可达的 PostgreSQL 端口转发；本页 localhost 指运行 Go 进程的机器。

使用独立 schema 时先创建它，再配置同名 schema。建议使用短 ASCII 名称；当前上游索引名限制使 schema 与表前缀合计最多 27 个字符，过长会在启动时拒绝。例如仅在本地开发库执行：

```bash
docker compose -f deploy/docker-compose.session.yml exec -T postgres \
  psql -U trpc -d trpc_session -v ON_ERROR_STOP=1 \
  -c 'CREATE SCHEMA IF NOT EXISTS agent_demo;'
```

服务会在所选 schema 自动建表并预置 `demo/echo`；控制面、Pin、Session 和 Inbox/Run/Outbox 共享该库。先保持 Bot 关闭验证 PostgreSQL 网页启动，再停止进程，在私密文件填写 Bot ID/Secret 并设置 `TRPC_SERVICE_WECOM_ENABLED=true`，重新运行 `node scripts/start-local.mjs`。默认 Tenant/App/Binding 已在模板列出。自定义 App 须先发布 Revision，且不能指定独立 BackendProfile。

同一 Bot 只启动一个实例；第二个连接可能接管旧连接。健康 HTTP 通过不等于机器人订阅成功，需观察用户单聊发消息后是否收到最终回复。当前只支持单聊文本，最终发送成功指平台 ACK，不是用户已读；重连后旧回复目标不补投。协议与恢复边界见 [IM 接入指南](im-channels.md)。

无需 Bot 凭据的本地协议集成测试使用临时 schema，可运行：

```bash
TRPC_SERVICE_SESSION_INTEGRATION=1 \
TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
go test -race -count=1 -timeout 120s ./trpcservice/channels/wecom ./trpcservice/telemetry
```

结束服务后可用 `docker compose -f deploy/docker-compose.session.yml stop postgres` 停库，保留卷；其他服务也在使用该库时保持运行。`down -v` 会删除已有数据，需要保留数据时请使用 `stop`。

## 可选飞书

沿用 PostgreSQL 单进程配置，并先发布绑定 App 的 Revision；当前 IM 只支持进程默认的持久 Session/Pin，不接受独立 BackendProfile。飞书与企微同时启用时，同租户须使用不同 Binding ID。

在 `data/local.env` 中设置：

```dotenv
TRPC_SERVICE_FEISHU_ENABLED=true
TRPC_SERVICE_FEISHU_TENANT_ID=demo
TRPC_SERVICE_FEISHU_AGENT_APP_ID=echo
TRPC_SERVICE_FEISHU_BINDING_ID=feishu-local
```

另建私密 `data/feishu.env`（权限 `0600`），只填写：

```dotenv
TRPC_SERVICE_FEISHU_APP_ID=
TRPC_SERVICE_FEISHU_APP_SECRET=
```

`AGENT_APP_ID` 是服务内部应用，`APP_ID` 是飞书应用，两者不能混用。执行只读身份预检，再从已构建的同一检出启动：

```bash
node scripts/check-feishu.mjs data/feishu.env
node scripts/start-local.mjs data/feishu.env data/local.env
```

预检读取 dotenv 数据、固定访问飞书官方地址并拒绝重定向，15 秒超时，仅输出 HTTP 状态、业务码和布尔检查，不输出凭据或外部身份；任一校验失败退出 1。预检不会建立长连接或发送消息。

| 飞书后台项目 | 配置 |
| --- | --- |
| 应用能力 | 企业自建应用，开启机器人 |
| 单聊接收 | `im:message.p2p_msg:readonly` |
| 机器人发送 | `im:message:send_as_bot` |
| 企业身份查询 | `tenant:tenant:readonly`，用于本实现额外的企业绑定校验 |
| 应用身份事件 | 只订阅 `im.message.receive_v1` |
| 接收方式 | 使用长连接；保存时必须有 SDK 客户端在线 |
| 可用范围 | 包含实际测试用户 |

日志中的固定 `channel feishu connected` 表示 SDK 已建立过连接。客户端在线后，在后台保存长连接方式、添加事件、创建版本并发布，使权限和订阅生效；普通企业自建应用可能需要管理员审核。官方测试版应用有自动生效例外。之后由测试用户私聊机器人发送文本，检查正常回复与持久 Run/Outbox 状态。

同一飞书 App 只运行一个实例，多连接可能分摊事件。HTTP 健康、凭据有效、长连接建立和真实收发是不同检查，不能互相替代。无需公网 Webhook URL，不需要用户 OAuth、群消息或卡片权限。

官方依据：[接收消息](https://open.feishu.cn/document/server-docs/im-v1/message/events/receive)、[回复消息](https://open.feishu.cn/document/server-docs/im-v1/message/reply)、[长连接配置](https://open.feishu.cn/document/server-docs/event-subscription-guide/event-subscription-configure-/request-url-configuration-case)、[应用可用范围](https://open.feishu.cn/document/home/introduction-to-scope-and-authorization/availability)。

## 可选本地 Collector

```bash
docker run --rm --name trpc-otel-local \
  -p 127.0.0.1:4318:4318 \
  -v "$PWD/deploy/otel-collector.yaml:/etc/otelcol/config.yaml:ro" \
  otel/opentelemetry-collector:0.120.0 \
  --config=/etc/otelcol/config.yaml
```

在另一个终端把私密配置的 `TRPC_SERVICE_TELEMETRY_ENABLED` 改为 `true`，`TRPC_SERVICE_OTLP_ENDPOINT` 保持 `http://127.0.0.1:4318`，停止旧服务后重新启动。Collector 端口占用可改主机映射和端点；远端 Docker 同样需要本机端口可达且绑定配置文件在 daemon 所在机器可读。

仅启用网页不会产生这三个阶段的记录，当前观测接入公共文本消费者，可随企微或飞书启用；已有观测集成证据使用模拟平台，未宣称真实 Bot 到 Collector 已实测。收到单聊并完成回复后，在 Collector 终端查看 `channel.accept`、`channel.execute`、`channel.deliver`，用 `trpc.request_id` 关联；阶段可能属于不同 Trace。计数名为 `trpc.channel.stage.count`，耗时名为 `trpc.channel.stage.duration`，单位 `ms`。Span 通常批量导出，指标默认约每 60 秒导出，也会在正常退出时刷新。此 debug exporter 仅为本地观察，不提供查询 UI 或持久历史。

Collector 失败不改变业务执行/发送结果，队列满或进程崩溃可丢遥测；指标不能作为持久账本。遥测只采集白名单，启用时 OTel 错误诊断统一为固定文字，完整细分 Trace 与生产 Collector 权限、保留期、容量策略见[目标设计](production-deployment.md#7-生产观测设计)。停止 Collector 可按 Ctrl-C 或在另一终端运行 `docker stop trpc-otel-local`。

## 观测边界

三个阶段使用独立 SDK TracerProvider/MeterProvider，不注册全局 provider，也不启用上游自动捕获模型/工具内容的 tracing。Span 只记录内部 Tenant/App/Binding、request/run/outbox、固定阶段/结果/错误类别、attempt 和耗时；不记录 Principal、Session、外部账号/消息、正文、目标载荷、工具参数或原始错误。指标标签仅为静态租户/App及有限阶段分类，不使用请求 ID。

启用观测时会安装进程级 `otel.SetErrorHandler`，将导出器诊断统一为固定文本；这会同时减少其他 OTel 诊断的详情。导出使用有界队列和超时，失败不改变业务结果或尝试次数，关闭在业务排空后 flush。阶段关联依赖持久 request_id，不承诺同一连续父子 Trace，指标也不是持久账本。

## 生产观测设计

完整 Trace、指标统计口径、Collector 认证与当前三阶段观测边界见[生产观测设计](production-deployment.md#7-生产观测设计)。本页的 Collector 命令仅用于本地观察。

## 容量估算

生产并发、Worker 数、存储写入与 token 预算的算例，以及扩缩容约束见[容量与扩缩容](production-deployment.md#4-容量与扩缩容)。这些是规划假设，不能作为当前版本的实测吞吐。
