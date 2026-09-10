# 多租户多 Agent Telegram / WeCom 验收链路

本文是本地 Compose 环境中验证“两个租户、两个 Agent、Telegram 与 WeCom AI Bot、同一模型和 endpoint”完整闭环的操作单。目标是确认每个 Channel Binding 都能把消息路由到所属租户的 Agent，并且会话、审计、回复不会串租户。

## 验收范围

验收通过需要同时满足以下条件：

- PostgreSQL、Redis 和服务容器健康；
- 两个租户均为 `active`，各自拥有一个 `active` Agent App；
- 每个 Agent 都有已发布 Revision、Model Profile 和 Backend Profile；
- 两个 Model Profile 可以使用同一个 provider、model 和 endpoint；
- Telegram 或 WeCom AI Bot Binding 分别属于对应租户，并绑定正确 Bot；
- 从两个渠道发起新会话都能得到回复；
- 同一个用户在两个 Bot 上的会话不会复用另一租户的 Runner session；
- 服务重启后重新连接 Bot，两个租户仍能完成对话。

## 1. 准备环境

复制并编辑本地环境文件。真实密钥只放在本机 `deploy/service.env`，不要提交到 Git：

```bash
cp deploy/example.env deploy/service.env
```

至少确认以下配置：

```ini
TRPC_ADMIN_USERNAME=admin
TRPC_ADMIN_PASSWORD=<本地管理员密码>
TRPC_ADMIN_TOKEN=<独立管理 token>
TRPC_ADMIN_TENANTS=*

TRPC_MODEL_PROVIDER=openai
TRPC_MODEL_API_KEY=<模型 API key>
TRPC_MODEL_NAMES=gpt-5.6-terra
TRPC_MODEL_ENDPOINT_HOSTS=api.adlzw.sbs
```

`TRPC_MODEL_ENDPOINT_HOSTS` 只填写域名，不填写协议、路径或端口。WebUI 中的 endpoint 可以填写完整 URL，例如 `https://api.adlzw.sbs/v1`，但它的 host 必须出现在该环境变量中。

使用环境文件启动服务：

```bash
docker compose --env-file deploy/service.env \
  -p trpcfresh -f deploy/docker-compose.yml \
  up -d --build service
curl -fsS http://localhost:8080/readyz
```

期望输出为 `ready`。确认容器实际加载的配置，避免 Compose 回退到默认值：

```bash
docker inspect trpcfresh-service-1 \
  --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | rg '^TRPC_MODEL_(PROVIDER|NAMES|ENDPOINT_HOSTS)='
```

## 2. 在 WebUI 配置租户和 Agent

1. 打开 `http://localhost:8080`，使用管理员账号登录。
2. 创建或选择租户 A，确保状态为 `active`。
3. 注册 Agent A，填写 Agent 名称、指令、模型 `gpt-5.6-terra` 和 endpoint `https://api.adlzw.sbs/v1`。
4. 完成发布并将 Agent A 设为租户 A 的默认 Agent。
5. 创建或选择租户 B，重复上述步骤注册 Agent B，并将 Agent B 设为租户 B 的默认 Agent。
6. 在 Agent 详情中核对 Model、Endpoint、Instructions 和发布状态。

两个租户可以使用同一个 Model Profile 内容。租户隔离由租户 ID、Agent App、Revision、Provider Registry 和运行时计划共同保证，不要求每个租户使用不同模型。

## 3. 连接两个 Telegram Bot

从 BotFather 为两个 Bot 分别取得 Bot Token。Token 只在 WebUI 的连接表单中输入，不写入文档、日志或数据库控制面记录。

对租户 A：

1. 进入租户 A 的 Agent 页面。
2. 选择 Telegram channel。
3. 输入 Bot A Token 并连接。
4. 确认连接状态为 active，并核对 Bot username/账号展示信息。

对租户 B 重复上述步骤，使用 Bot B Token。一个 Binding 只代表一个 Bot 和一个可信 Routing Target；不要把 Bot B 连接到租户 A 的 Binding。

> 服务重启会清除进程内保存的 Telegram Bot secret。每次重启后都必须在 WebUI 重新连接两个 Bot。

## 4. 前端验收企业微信 AI Bot

WebUI 中的 `WeCom` 入口对应企业微信 **AI Bot 长连接**（`wecom_aibot`），服务端会主动连接 `wss://openws.work.weixin.qq.com`。它不是企业微信自建应用回调入口，因此表单中必须使用 AI Bot 管理页面生成的两项凭据：

- **AI Bot ID**：AI Bot 的 `bot_id`/`aibotid`；
- **AI Bot secret**：与该 AI Bot ID 配对的长连接密钥。

不要在这里填写 Corp ID、Agent ID、应用 Secret、回调 Token 或 EncodingAESKey。协议说明见[企业微信 AI Bot 长连接文档](wecom-aibot.md)和[企业微信帮助中心](https://open.work.weixin.qq.com/help2/pc/21663)。

在前端完成一次真实验收：

1. 选择已经配置并发布 Agent 的租户，确认 Agent 状态为 active。
2. 选择 **WeCom** channel。
3. 在 `AI Bot ID` 中填入企业微信 AI Bot 设置页的 ID，在 `AI Bot secret` 中填入同一个 Bot 的 Secret。
4. 点击 **Connect channel**，等待页面显示 `Your agent is connected`。
5. 打开企业微信客户端，找到该 AI Bot，发送一条唯一 marker，例如 `wecom-tenant-b-session-001`。
6. 确认 Bot 在同一会话返回回复，并且回复内容符合当前租户所选 Agent 的指令。

前端连接成功只代表 WebSocket 已完成认证；必须再从企业微信客户端发一条消息，才能验收入站路由、Agent 执行和回复发送。两个租户分别验收时，必须为每个租户连接其对应的 AI Bot Binding。

服务端证据：

```bash
docker logs --since 5m trpcfresh-service-1 2>&1 \
  | rg 'AI Bot|wecom-aibot|tenant_id|app_id|authentication|websocket|error_detail'
```

期望看到连接认证成功后的运行日志，以及消息对应的 tenant/app 路由；不应出现 `connection_failed`、`authentication_rejected`、`authentication_timeout` 或 `websocket_dial`。认证失败日志只会记录错误码和非敏感错误消息，不会记录 Secret。

如果页面仍显示 `Connection failed`，先确认容器是用 `deploy/service.env` 启动的，然后按日志中的阶段定位：

| 日志阶段 | 含义 | 首要检查 |
| --- | --- | --- |
| `binding_create` | 控制面 Binding 创建失败 | 租户/Agent 是否 active；运行是否为最新镜像 |
| `target_resolve` | 可信租户路由解析失败 | Binding 的 tenant、app 和 revision 是否匹配 |
| `websocket_dial` | 未建立企业微信 WebSocket | 出站网络、代理和 `wss://openws.work.weixin.qq.com` |
| `authentication_rejected` | 企业微信拒绝凭据 | AI Bot ID 与 Secret 是否来自同一个 Bot |
| `authentication_timeout` | 未收到订阅确认 | AI Bot 长连接能力、网络和服务版本 |

服务重启会清除进程内保存的 AI Bot Secret。重启后需要在 WebUI 为每个租户重新连接，然后再执行一次真实消息验收。

## 5. 跨租户、跨 Agent、跨会话验收

准备两个 Telegram 对话窗口，分别向 Bot A 和 Bot B 发送不同的 marker，例如：

```text
tenant-a-session-001
tenant-b-session-001
```

验收记录至少包含：

| 检查项 | 期望结果 |
| --- | --- |
| Bot A 回复 | 由 Agent A 的指令和行为生成 |
| Bot B 回复 | 由 Agent B 的指令和行为生成 |
| A 的消息到 B | 不会出现在 B 的会话上下文 |
| B 的消息到 A | 不会出现在 A 的会话上下文 |
| 同一 Bot 的连续消息 | 能恢复该 Bot 对应的 session |
| 服务重启后重新连接 | 两个 Bot 均可重新对话 |
| 相同 model/endpoint | 不影响租户和 Agent 路由 |

如果两个 Agent 使用相同指令，给它们设置不同的短标识或首句，便于人工确认实际命中了哪个 Agent。

## 6. 服务端证据

发送消息后立即查看日志，确认 tenant 和 app 成对出现：

```bash
docker logs --since 5m trpcfresh-service-1 2>&1 \
  | rg 'telegram|dispatch|tenant_id|app_id|error_detail'
```

重点核对：

- Bot A 的 `tenant_id` 与 Agent A 的 `app_id` 匹配；
- Bot B 的 `tenant_id` 与 Agent B 的 `app_id` 匹配；
- 没有 `dispatch failed`；
- 没有 `audit tenant scope violation`；
- 正常回复会进入 Reply Outbox 并由对应 Binding 发送。

PostgreSQL 中可检查控制面对象：

```bash
docker exec trpcfresh-postgres-1 psql -U trpc -d trpc_agent -c \
  "select tenant_id, display_name, status, default_agent_app_id, default_backend_profile_id from tenant order by created_at;"
```

审计检查应按两个租户分别出现执行事件。不要只看全局最新一条记录，要用 `tenant_id`、`agent_app_id` 和 request ID 关联一次完整请求。

## 7. 重启恢复验收

```bash
docker compose --env-file deploy/service.env \
  -p trpcfresh -f deploy/docker-compose.yml \
  up -d --build service
curl -fsS http://localhost:8080/readyz
```

服务恢复后：

1. 在 WebUI 重新连接 Bot A 和 Bot B；
2. 分别发送新的 marker；
3. 检查两个 Bot 都返回回复；
4. 检查新请求的 tenant/app 路由没有变化。

服务重启不会删除 PostgreSQL 中的租户、Agent、Revision、Model Profile 或 Binding；只会使进程内 Telegram 连接失效。

## 8. 常见故障定位

### 提示“model and endpoint match the server environment”

WebUI 会把多个 `invalid_request` 统一显示成这句话。先确认：

```bash
docker inspect trpcfresh-service-1 \
  --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | rg '^TRPC_MODEL_(PROVIDER|NAMES|ENDPOINT_HOSTS)='
```

然后确认模型名称完全匹配 `TRPC_MODEL_NAMES`，endpoint 的 host 完全匹配 `TRPC_MODEL_ENDPOINT_HOSTS`。如果实际容器仍显示 `gpt-4o-mini` 或 `api.openai.com`，说明启动时没有使用 `--env-file deploy/service.env`。

### 租户 2 进入正确 tenant/app 但对话失败

查看 dispatch 的 `error_detail`：

```bash
docker logs --since 5m trpcfresh-service-1 2>&1 \
  | rg 'dispatch failed|error_detail|tenant_id|app_id'
```

历史上该链路曾因固定租户的审计 writer 报 `audit tenant scope violation`，导致第二租户在执行前失败。当前实现使用按事件 `tenant_id` 动态路由的多租户审计 writer；若再次出现该错误，应先确认运行的是最新构建镜像。

### 第二个 Bot 没有任何反应

- 检查服务是否刚重启；若是，重新连接 Bot；
- 检查 Bot token 对应的 Telegram account 是否与 Binding 的 `provider_account_id` 一致；
- 检查同一 Bot 是否仍被其他进程或 webhook 消费；
- 检查日志中是否有 polling、identity mismatch 或 binding inactive；
- 确认租户、Agent、Revision 和 Binding 都是 active/published 状态。

### 两个 Bot 回复内容串了

核对每条请求的 `tenant_id`、`binding_id`、`app_id` 和 session identity。Telegram 消息不能自行提供 tenant ID；可信租户必须来自 active Channel Binding。若只通过普通 `/v1/chat` API 测试，还要确认 API token 映射的是哪个 tenant，因为普通 API identity 与 Telegram Binding 路由是两条入口。

## 实现边界

- Agent、Model Profile、Backend Profile 和 Revision 在控制面动态创建；不需要为新增租户重启服务。
- Tenant Runtime 按租户懒加载，并以 tenant/app/revision/profile 版本和内容摘要区分计划缓存。
- Telegram Bot secret 当前保存在服务进程内，重启后需要重新连接；生产环境应接入持久化 Secret 管理方案。
- 同一 endpoint 可以服务多个租户，但模型密钥、权限、审计和会话仍按租户边界处理。
- 本文验证的是人工真实 Telegram 对话闭环；协议适配器的 deterministic 单测和独立 live E2E 仍按 [Telegram 文档](telegram.md) 执行。
