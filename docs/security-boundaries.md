# 接口与租户密钥的安全边界

本轮补齐的是可执行的访问控制，不是宣称已经接入企业 SSO 或云 Secret Manager。HTTP 调试、IM 回调和管理接口是三种不同的入口，不能共用一套“只知道 URL 就能调用”的权限判断。

## 1. HTTP 调试接口

`/chat` 和 `/inbound` 默认不注册，返回 404。仅把服务绑定到 `127.0.0.1` 不够：Cloudflare Tunnel 也会从本机连接服务，所以不能把 loopback 或 `X-Forwarded-For` 当作身份凭证。

本机教学场景在 `.env` 中配置：

```dotenv
TRPC_AGENT_HTTP_API_ENABLED=true
TRPC_AGENT_HTTP_API_TOKEN="填入自行生成的随机 Token"
TRPC_AGENT_HTTP_API_PRINCIPALS_JSON=
```

先执行 `openssl rand -hex 24` 生成 Token，然后将结果写入本地 `.env`，不要提交。这个快捷配置只授权 `tutorial-tenant`、`tutorial-http`、`alice`，没有跨租户或任意用户权限。它和模型的 `OPENAI_API_KEY`、Telegram 的 Bot Token、Admin Token 是不同的凭据，不能混用。

重新启动后，自己维护的 `.env` 可以这样加载到当前 shell，用于文档中的 curl 示例（只 source 自己信任的本地文件）：

```bash
set -a
source .env
set +a
curl -sS -X POST http://127.0.0.1:8080/chat \
  -H "Authorization: Bearer $TRPC_AGENT_HTTP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"binding_key":"tutorial-http","message_id":"secure-001","user_id":"alice","session_id":"secure-demo","message":"你好"}'
```

多个调用方需要改用 `TRPC_AGENT_HTTP_API_PRINCIPALS_JSON`，并清空快捷 Token。格式如下；`token` 必须替换为至少 32 字符的随机值：

```json
[
  {
    "name": "tenant-a-client",
    "token": "<由 Secret 注入的随机值>",
    "tenant_id": "tenant-a",
    "binding_keys": ["tenant-a-http"],
    "user_ids": ["alice"]
  }
]
```

每个凭据都有精确的租户、绑定键和用户列表，不支持 `*`。流程是：

```text
Bearer 校验 → 校验 binding_key / user_id 授权
→ 从控制面解析 Scope → 确认 tenant_id 且 ChannelType=http
→ 配额 → Runner 执行或 Inbox 持久化
```

未携带或错误 Token 返回 401；越权返回 403，不创建任务、不调用模型。`/inbound` 的 Scope 授权在 Intake 自己解析路由后、写入前完成，不依赖 HTTP 层提前查询另一份可能变化的路由。HTTP 接口不能使用 Telegram/企业微信 Binding 来绕过回调验签。Token 不接受 query 参数，错误信息不回显凭据。

## 2. 租户密钥授权

`secret.Store.Resolve` 现在要求 `tenantID`、`purpose`、`reference` 三项。生产进程装配使用默认拒绝的 EnvStore；只有部署者配置的精确 grant 可以读取环境变量。控制面的 `secret_namespace` 是组织元数据，不是访问任意环境变量的授权凭证。

现有教学 Telegram Binding 对应的 `.env` 配置为：

```dotenv
TRPC_AGENT_SECRET_GRANTS_JSON='[{"tenant_id":"tutorial-tenant","purpose":"telegram_bot","reference":"env://TELEGRAM_BOT_TOKEN"},{"tenant_id":"tutorial-tenant","purpose":"telegram_webhook","reference":"env://TELEGRAM_WEBHOOK_SECRET"}]'
```

这里保存的是授权关系，真实值仍在原来的两个环境变量中。该字段只能由部署者修改，没有对租户开放修改 grant 的 Admin API。共享机器上的环境变量存在，不代表任何租户都能引用它。

| 用途 | 调用位置 |
| --- | --- |
| `model` | 租户 Revision 的模型 API Key |
| `session`、`memory` | 对应后端连接凭据 |
| `artifact` | S3/MinIO 凭据 JSON |
| `knowledge`、`embedding` | 向量库 Key、Embedding Key，分别授权 |
| `telegram_webhook`、`telegram_bot` | 入站验签、出站 Bot Token，分别授权 |
| `wecom_callback`、`wecom_aes`、`wecom_app` | 回调 Token、AES Key、应用 Secret，分别授权 |
| `wecom_mcp_read`、`wecom_mcp_send` | 托管消息 MCP 的接收与发送用途，分别授权 |

托管 MCP 的 URL 本身可能同时具有上游读写能力；这里的两个用途是本平台的软件限制，不是两种由企业微信签发的权限 Token。只有启用该通道的 Gateway/Sender 注入 URL，Worker/Jobs/Admin 不需要其真实值，见[运行说明](wecom-mcp-runtime.md)。

例如，租户模型使用 `api_key_ref: "env://TENANT_A_MODEL_KEY"`，需要该租户的 `model` grant。旧的 `api_key_env` 仍可使用，但也转换成同样的引用并经过授权，不再直接调用 `os.LookupEnv`。把已有的数据库 grant 填到模型配置中会被拒绝。

Admin 创建 Revision、Channel Binding、Backend Binding，以及更新启用中的 Channel Binding 时，先检查授权再保存；执行时仍再次通过 Store 读取，直接写入数据库不能绕过读取边界。授权检查不读取密钥值，因此 Admin 节点不需要取得模型和 IM 的真实凭据。

`source=startup_env` 和 Session `startup_config` 是部署者显式提供的共享教学/平台资源，不允许租户改写它们的凭据或地址。独立租户模型和后端应使用自己的 binding/reference。OpenAI Embedding 与 S3 适配不再偷偷使用进程默认凭据：必须显式提供引用；Workload Identity 尚未实现。

## 3. 进程角色

| 角色 | 对外入口与可读取的租户密钥用途 |
| --- | --- |
| `all` | 本地完整链路；按开关提供 HTTP 调试、回调和 Admin |
| `gateway` | 回调、可选 `/inbound`、显式启用的 MCP 拉取；读取入站验证密钥/MCP read 用途，不提供 `/chat` 或 Admin |
| `worker` | 无 HTTP 监听；模型、Session/Memory/Knowledge/Artifact/Embedding |
| `sender` | 无 HTTP 监听；只读取 IM 出站凭据 |
| `jobs` | 无 HTTP 监听；后台模型和数据后端 |
| `relay` | 无 HTTP 监听；不解析租户模型或 IM 密钥 |
| `admin` | 只有健康检查和启用后的 Admin；可读取管理数据操作所需的后端密钥 |

Gateway/Admin/Relay/Sender 不加载模型 API Key，误调用模型会明确失败，不会退回 Mock 回复。单体 `all` 仍拥有组合权限，不用于模拟进程级密钥隔离。

Kubernetes 模板已经拆成六个角色 Secret 和单独的 migration Secret；入口 NetworkPolicy 分别限制 Gateway 与 Admin 的来源命名空间。部署者必须提供真实的分角色 Secret、namespace 标签和数据库/Redis 权限，不能把同一份全集凭据复制进七个 Secret 就算完成隔离。

## 4. 更新、生效与剩余范围

配置在启动时读取；本轮不做热更新。修改 Token 或 grants 后，需要滚动重启相关进程，旧进程和缓存可能继续持有旧凭据。紧急撤销还应在供应商端撤销密钥；仅修改 `.env` 不会立即取消正在运行的调用。

本次为现有 `.env` 增加了两项 Telegram grant，并明确关闭 HTTP 调试；没有更换模型、Bot Token、Webhook 或停止正在运行的服务。下次正常构建、重启后，新代码才生效。只使用 Telegram 不需要开启 HTTP 调试。

本地 `.env` 权限收紧为 `0600`（仅文件所有者读写）；它仍被 Git 忽略。不要把 `.env` 内容贴进日志或把 shell 调试模式 `set -x` 用于含凭据的启动脚本。

尚未覆盖：企业 SSO/OIDC、在线轮换/撤销、Vault/KMS、完整的数据库 GRANT/Redis ACL、按目标地址限制外连、真实 Kubernetes 部署。租户可配置的后端地址还需要生产网络出口策略约束，Secret grant 不能替代 SSRF 防护。

自动测试覆盖默认拒绝、错误 Token、跨租户/用户/绑定、HTTP 伪造 IM、密钥用途错用、未授权配置不落库、隐式凭据拒绝，以及三个独立进程角色的 HTTP 行为；不依赖真实模型或 IM 账号，也不停止日常服务。

本轮命令与结果见[安全验证记录](validation/security-2026-09-06.md)。
