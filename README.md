# 多租户 Agent 部署平台

基于 [tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) v1.11.2 的平台架构与 Go 参考实现。通过统一的租户配置、版本发布、会话管理和 IM 适配，让网页、企业微信与飞书共享真实的 Agent 执行链路。

[快速开始](#快速开始) · [本地部署](docs/local-deployment.md) · [生产部署设计](docs/production-deployment.md) · [设计方案](docs/solution.md) · [文档索引](docs/delivery.md) · [实现与验证](docs/acceptance.md)

## 当前能力

| 领域 | 已实现范围 |
| --- | --- |
| 应用与版本 | Tenant/App/Revision 管理、发布与回滚，Session 首轮固定 Revision |
| Agent 执行 | 复用 LLMAgent、Runner 和 OpenAI-compatible HTTP/SSE；支持确定性模型与真实模型 |
| 网页与 IM | 内嵌网页聊天；企微、飞书长连接单聊文本，共用持久去重、执行和最终回复流程 |
| 会话存储 | InMemory、PostgreSQL、Redis；租户 BackendProfile 路由与合作型 Run 租约 |
| 身份与工具 | 独立的对话/Admin 凭据、租户 SecretRef/PolicyRef 授权、内置工具及结构化调用审计 |
| 可观测性 | 可选受理、执行、发送三阶段 Span 与指标，按 request_id 关联 |

Memory/Summary、知识与附件路由、在线迁移、群聊和媒体、完整 Trace、预算审批及多角色生产部署已有设计，尚未实现。当前入口共用进程，租约不提供存储 fencing；IM 已启动未知任务不重跑，未知发送不重发。完整边界见[风险清单](docs/risks.md)与[验证说明](docs/acceptance.md)。

## 快速开始

需要 **Go 1.24.1 或以上**、Git、Bash 和 curl。首次克隆和构建需要网络获取代码与 Go 模块；构建完成后，默认网页演示不依赖外部模型、数据库或前端资源服务。

从项目仓库的 `feature/Wang-Pengfei` 分支获取源码：

```bash
git clone --branch feature/Wang-Pengfei https://github.com/d2bz/trpc-agent-service.git
cd trpc-agent-service

./build.sh
./start.sh
```

当前实现会启动内存控制面，预置 `demo` Tenant、`echo` Agent App 和已发布的 `echo-v1` Revision，再通过 Runtime Resolver 懒加载真实的 tRPC-Agent-Go `LLMAgent + Runner + InMemory Session`。服务监听 `127.0.0.1:8080`，确定性回显模型不需要外部 API Key。

独立端口、私密环境文件、PostgreSQL、企业微信、飞书和 OTel 的配置见[本地部署](docs/local-deployment.md)。更换代码版本后先重新运行 `./build.sh`；`start.sh` 仅在二进制不存在时自动构建。

打开 [网页聊天](http://127.0.0.1:8080/) 即可发送消息、查看流式回复、停止生成、新建对话和继续已有对话。页面由同一 Go 二进制内嵌提供，无需前端构建或外部 CDN；默认使用 `echo` 和公开的开发 chat key。要访问其他已配置应用或凭据，在页面设置中填写 App ID 和**对话凭据**，不要填 Admin Key、模型 API Key 或企业微信 Secret。

网页的会话列表和凭据仅保留在当前页面内存中，刷新后不恢复；更换 App 或凭据会清空本地对话。续聊复用服务端返回的 Session ID，每次只发送最新一条用户消息。停止会中断当前 HTTP 请求，但不承诺撤销已发生的服务端写入或 Tool 操作；失败不会自动重发。企业微信与飞书复用公共文本消费者，支持长连接单聊和一条最终回复；协议、配置边界与社区扩展见[IM 指南](docs/im-channels.md)。真实正常收发及三阶段 OTel 的验证范围见[实现与验证](docs/acceptance.md#验证结果)。IM 与 OTel 均默认关闭。

对话面和 Admin 面都要求 Bearer 凭据，且使用两套互不相通的凭据体系。Admin Key 没有公开默认值：环境里没有 `TRPC_SERVICE_ADMIN_API_KEY` 时，`start.sh` 会生成一个并存到 `data/admin-api-key`（`0600`，已被 `.gitignore` 排除），重启复用同一个文件。脚本只打印路径，不打印 key：

```text
generated a new admin API key: /path/to/project/data/admin-api-key
admin API key: /path/to/project/data/admin-api-key
```

后面的 Admin 示例统一用这个值：

```bash
ADMIN_KEY="$(cat data/admin-api-key)"
```

环境里已有 `TRPC_SERVICE_ADMIN_API_KEY`，或设置了 `TRPC_SERVICE_SECURITY_CONFIG_FILE` 时，脚本不生成也不写这个文件。

健康检查（不需要凭据）：

```bash
curl http://127.0.0.1:8080/healthz
```

调用 OpenAI-compatible 非流式接口。对话面需要 Bearer 凭据，平台由凭据决定租户和会话归属：

```bash
curl -i http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer local-development-key-not-a-secret' \
  -H 'X-Agent-App-ID: echo' \
  -H 'X-Session-ID: demo-session' \
  -d '{"model":"deterministic-echo","messages":[{"role":"user","content":"hello platform"}]}'
```

本地开发 chat key 通过环境变量覆盖，未设置时使用上面这个公开的、明确命名为非密钥的占位值。chat key 至少 16 个字符，Admin key 至少 32 个字符，首尾带空白或含有无法放进 HTTP 头的字节的 key 会在启动时直接拒绝（否则进程会正常启动，然后对着刚刚配置好的 key 返回 `401`）：

```bash
TRPC_SERVICE_API_KEY=replace-with-your-own-local-key ./start.sh
```

关于这条链路需要知道的几件事：

- **请求体中的 `user` 字段被忽略。** 会话归属只来自认证结果，写入的 Session 用户是 `u/{principal_id}`，客户端无法指定别人的身份。
- **`X-Session-ID` 可以不传**，平台会生成一个并通过响应头 `X-Session-ID` 回传，续接对话时原样带回即可；`-i` 就是为了看到这个响应头。
- **响应头 `X-Agent-Revision-ID`** 是本次实际执行的版本。每个 Session 在首轮就被钉在当时的 Revision 上，之后发布新版本或回滚都不会改变它，新建 Session 才会用上新版本。

将请求体加入 `"stream":true` 即可验证 SSE 流式响应，响应头同样带回上述两个字段。

创建其他 Tenant、Agent App 和 Revision 的接口、完整路由顺序和错误码见 [Admin API 与动态路由](docs/admin-api.md)；凭据体系、角色模型、Security Manifest、租户 Entitlement 和已知边界见[身份、权限与密钥治理](docs/security-and-governance.md)。

用 `platform_admin` 凭据创建一个新租户：

```bash
curl -X POST http://127.0.0.1:8080/admin/v1/tenants \
  -H "Authorization: Bearer ${ADMIN_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"id":"team-a","slug":"team-a","name":"Team A"}'
```

### 运行真实模型

Revision 也可以把模型 Provider 设为 `openai-compatible`，用 `secret_ref` 从 Worker 环境解析 API Key。

引用 `secret_ref` 需要该**租户**被授权引用那个变量。默认的 demo profile 只授权 `demo` 租户使用 `builtin.safe-tools` 策略，**不授权任何 SecretRef**，所以直接用默认配置发布下面这个 Revision 会得到 `403 not_entitled`。要跑真实模型，需要提供一份自定义 security manifest。

`security.json`（key 的值不写在文件里，只写它来自哪个环境变量）：

```json
{
  "version": 1,
  "credentials": [
    {
      "purpose": "chat",
      "principal_id": "demo-user",
      "key_ref": "env:DEMO_CHAT_KEY",
      "tenant_id": "demo",
      "allowed_app_ids": ["echo"]
    },
    {
      "purpose": "platform_admin",
      "principal_id": "local-admin",
      "key_ref": "env:DEMO_ADMIN_KEY"
    }
  ],
  "tenant_entitlements": [
    {
      "tenant_id": "demo",
      "allowed_secret_refs": ["env:TEAM_MODEL_API_KEY"],
      "allowed_policy_refs": ["builtin.safe-tools"]
    }
  ]
}
```

设置了 `TRPC_SERVICE_SECURITY_CONFIG_FILE` 后，manifest 就是**全部**配置：`TRPC_SERVICE_API_KEY` 和上面那个公开的开发 key 都不再参与，`start.sh` 也不再生成 `data/admin-api-key`，两个 key 都由你自己提供。

```bash
export TEAM_MODEL_API_KEY='replace-with-provider-key'
export DEMO_CHAT_KEY='replace-with-your-own-chat-key'
export DEMO_ADMIN_KEY='replace-with-your-own-admin-key-at-least-32-chars'
export TRPC_SERVICE_SECURITY_CONFIG_FILE="$PWD/security.json"
./start.sh

ADMIN_KEY="$DEMO_ADMIN_KEY"

curl -X POST http://127.0.0.1:8080/admin/v1/tenants/demo/apps/echo/revisions \
  -H "Authorization: Bearer ${ADMIN_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"echo-openai-v1",
    "revision_no":2,
    "config":{
      "agent_name":"echo-assistant",
      "instruction":"Answer the user request.",
      "model":{
        "provider":"openai-compatible",
        "name":"gpt-4o-mini",
        "base_url":"https://api.openai.com/v1",
        "secret_ref":"env:TEAM_MODEL_API_KEY",
        "temperature":0.2,
        "max_tokens":256
      },
      "tool_refs":["builtin_add","builtin_echo"],
      "policy_refs":["builtin.safe-tools"]
    }
  }'

curl -X POST \
  -H "Authorization: Bearer ${ADMIN_KEY}" \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:8080/admin/v1/tenants/demo/apps/echo/revisions/echo-openai-v1/publish
```

请求体里**没有** `created_by`：作者身份取自认证凭据的 Principal，不是请求可以声明的字段，仍然携带它的请求体会被 `400 invalid_json` 拒绝。`publish` 虽然不带 body，同样需要 `Content-Type: application/json`——所有 Admin 写操作都要求它，这是把 Admin 写请求挡在浏览器"简单请求"集合之外的那一条。

之后用一个新的 `X-Session-ID` 调用前面的对话接口，Bearer 换成 manifest 里那个 chat key（`$DEMO_CHAT_KEY`）；已开始的 Session 仍保持原 Revision。默认测试不会访问外网，真实端点冒烟测试必须显式开启：

```bash
TRPC_SERVICE_MODEL_INTEGRATION=1 \
TRPC_SERVICE_MODEL_BASE_URL='https://api.openai.com/v1' \
TRPC_SERVICE_MODEL_NAME='gpt-4o-mini' \
TRPC_SERVICE_MODEL_SECRET_REF='env:TEAM_MODEL_API_KEY' \
go test -race -timeout 120s \
  -run TestOpenAICompatibleLiveEndpoint ./trpcservice/agent/...
```

上面这个 Revision 会把两个内置安全工具交给真实模型：`builtin_add` 做整数加法，`builtin_echo` 原样返回文本；`builtin.safe-tools` 是当前唯一白名单策略。有 `tool_refs` 却没有已知 `policy_refs`、引用未知或重复、或者策略不允许指定工具时，Runtime 会拒绝构建，不会静默减少工具集合。工具调用通过 tRPC-Agent-Go 的 callback 记录结构化 before/after 审计，但不记录参数、结果或错误正文。完整语义、离线两轮 Tool/SSE 测试和当前限制见 [Tool 与 Policy Runtime](docs/tool-policy.md)。

`base_url` 必填，避免上游客户端从进程环境静默选择请求目标。空 `secret_ref` 明确表示无凭据调用，不会继承进程的 OpenAI API Key。`secret_ref` 仍然只支持 `env:VAR_NAME`，但现在**必须**先被该租户的 entitlement 授权：租户之间不能互相引用变量，任何租户都不能被授权引用持有平台自身凭据的变量，也不能引用 `TRPC_SERVICE_` 命名空间。未授权的引用一律返回同一个 `403 not_entitled`，不区分变量存在与否，也不区分策略是否注册。完整规则见[身份、权限与密钥治理](docs/security-and-governance.md)。

**Admin API 要求 Bearer 凭据**，角色为 `platform_admin`（可管理任意租户，且是唯一能创建租户的角色）或 `tenant_admin`（只能管理绑定的那一个租户，访问别的租户得到与"资源不存在"逐字节相同的 `404`，且不会产生任何 Repository 调用）。认证发生在路由、方法和 `Content-Type` 判断之前，因此无凭据的调用方在真实路由、不存在的路由和错误方法上得到同一个 `401`。Admin 的任何响应都不带 CORS 头。

服务只允许绑定回环地址：`127.0.0.1`、`localhost`、`[::1]` 之外的监听地址（包括 `:8080`、`0.0.0.0:8080` 这类通配形式）会在启动时直接拒绝，且没有绕过开关。当前服务使用明文 HTTP，可路由监听会使 Admin Bearer token 暴露在网络传输中；demo profile 可使用公开的开发 chat key 启动。TLS 由外部反向代理终止，本二进制不提供 TLS 配置。

自定义监听地址时使用：

```bash
TRPC_SERVICE_ADDR=127.0.0.1:18080 ./start.sh
```

停止服务：

```bash
./stop.sh
```

## 设计文档

| 内容 | 文档 |
| --- | --- |
| 部署方案 | [本地运行](docs/local-deployment.md)、[生产部署设计](docs/production-deployment.md)（目标架构，尚非可执行部署包） |
| 主设计文档 | [平台方案](docs/solution.md) |
| 系统架构图与组件职责 | [总体架构](docs/architecture.md) |
| 完整消息时序 | [核心时序](docs/sequence.md) |
| 核心实体与关系 | [数据模型](docs/data-model.md) |
| 后端选择、同步、幂等与迁移 | [存储方案](docs/storage-and-consistency.md) |
| IM 协议与社区扩展 | [IM 接入指南](docs/im-channels.md) |
| 治理、安全与生产风险 | [安全设计](docs/security-and-governance.md)、[风险清单](docs/risks.md) |

框架负责 Agent 编排、Runner 和数据服务能力；平台负责租户作用域、版本路由、执行协调及消息投递。当前依赖没有可直接导入的 OpenClaw Go Channel 包，IM 使用平台自有适配契约。逐项复用关系见[框架能力与平台职责](docs/project-foundation.md)。

## 源码入口

| 路径 | 职责 |
| --- | --- |
| `cmd/trpc-service` | 启动配置、依赖组装、平台连接与关闭顺序 |
| `trpcservice/tenant`、`sessiondir` | Tenant/App/Revision、持久发布配置与 Session Pin |
| `trpcservice/agent`、`sessionrun` | Runtime 缓存、Runner 调用与统一会话执行生命周期 |
| `trpcservice/web` | Admin API、对话 HTTP/SSE 与内嵌网页 |
| `trpcservice/channels` | 公共契约、文本消费者、PostgreSQL Store、企微和飞书适配器 |
| `trpcservice/sessionbackend`、`storagebundle`、`sessionlease` | Session 后端、租户路由与运行租约 |
| `trpcservice/identity`、`security`、`secretref`、`tool` | 身份、授权、密钥引用与工具策略 |
| `trpcservice/telemetry` | IM 阶段观测 |
| `deploy`、`scripts` | 配置示例、本地依赖与运行验证脚本 |

## 验证

默认检查不调用外部模型或真实 IM，构建和首次测试需要下载 Go 依赖：

```bash
TRPC_SERVICE_MODEL_INTEGRATION=0 TRPC_SERVICE_SESSION_INTEGRATION=0 \
  go test -race -count=1 -timeout 900s ./...
go vet ./...
go build ./...
```

持久化集成命令、真实平台记录及各自适用范围见[实现与验证](docs/acceptance.md#验证结果)。正常收发不代替故障测试，平台接受回复不表示用户已读。
