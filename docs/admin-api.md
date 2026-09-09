# Admin API 与动态路由

## 1. 当前边界

Admin API 提供租户、应用、Revision 和 BackendProfile 的配置管理。控制面默认使用 InMemory Repository，也可通过 `TRPC_SERVICE_STORAGE_PROFILE=postgres` 使用 PostgreSQL。

Admin API **要求 Bearer 凭据**，凭据体系与对话面完全独立：admin key 和 chat key 是不同的 Go 类型、不同的认证方法和 `PlatformServer` 上的两个独立字段，因此一个 chat key 送到 Admin 面只会得到 `401`。角色为 `platform_admin` 或 `tenant_admin`（见第 2.1 节）。完整规则见[身份、权限与密钥治理](security-and-governance.md)。

进程只允许绑定回环地址。当前服务使用明文 HTTP，可路由监听会使 Admin Bearer token 暴露在网络传输中；demo profile 可使用公开的开发 chat key 启动。TLS 由外部反向代理终止。

本地默认 profile 下，admin key 由 `start.sh` 生成到 `data/admin-api-key`（`0600`），重启复用，脚本只打印路径。下文示例统一使用：

```bash
ADMIN_KEY="$(cat data/admin-api-key)"
```

平台启动时预置：

| 资源 | ID |
| --- | --- |
| Tenant | `demo` |
| Agent App | `echo` |
| Agent Revision | `echo-v1` |
| Model | `deterministic-echo` |

## 2. Admin 端点

| 方法 | 路径 | 行为 | 所需角色 |
| --- | --- | --- | --- |
| `POST` | `/admin/v1/tenants` | 创建 Tenant | `platform_admin` |
| `GET` | `/admin/v1/tenants/{tenant_id}` | 读取 Tenant | 该租户 |
| `POST` | `/admin/v1/tenants/{tenant_id}/apps` | 创建 Agent App | 该租户 |
| `GET` | `/admin/v1/tenants/{tenant_id}/apps/{app_id}` | 读取 Agent App 和当前路由 | 该租户 |
| `POST` | `/admin/v1/tenants/{tenant_id}/backend-profiles` | 创建不可变 Backend Profile | 该租户 |
| `GET` | `/admin/v1/tenants/{tenant_id}/backend-profiles` | 按 ID 排序列出 Backend Profile | 该租户 |
| `GET` | `/admin/v1/tenants/{tenant_id}/backend-profiles/{profile_id}` | 读取 Backend Profile | 该租户 |
| `POST` | `/admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions` | 创建不可变 draft Revision | 该租户 |
| `GET` | `/admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions/{revision_id}` | 读取 Revision | 该租户 |
| `POST` | `/admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions/{revision_id}/publish` | 发布新版本或把已发布旧版本切回默认 | 该租户 |

"该租户"表示：`platform_admin` 通过，`tenant_admin` 仅当路径里的 `tenant_id` 与它绑定的租户**精确相等**时通过。

所有 `POST` 必须携带 `Content-Type: application/json`，`publish` 虽然不带 body 也一样；否则返回 `415`。请求体最大 1 MiB，未知 JSON 字段、格式错误的 JSON、多于一个 JSON 值和非法资源 ID 会返回 `400`。重复 ID 或 Revision 编号返回 `409`。跨租户和不存在的资源统一返回 `404 resource not found`，避免暴露其他租户的资源是否存在。

请求体里的**重复 JSON 成员**不被拒绝：`encoding/json` 取最后一个，Admin 的解码器没有在这上面加检查。重复成员的严格拒绝只适用于 Security Manifest（见[身份、权限与密钥治理](security-and-governance.md#53-严格解析)），因为那里被静默丢掉的是一条授权。

Admin 的任何响应都**不带** `Access-Control-Allow-*` 头，也没有预检分支；`OPTIONS` 不被特殊处理，它先认证，再作为这些路由不接受的方法得到 `405`。这与"所有 POST 必须声明 JSON"配合，使浏览器页面既读不到 Admin 响应，也发不出 Admin 写请求。

### 2.1 角色

| 角色 | 租户绑定 | 可做 |
| --- | --- | --- |
| `platform_admin` | 不属于任何租户 | 创建租户；管理任意租户的 App / Revision / Backend Profile |
| `tenant_admin` | 绑定且仅绑定一个租户 | 管理自己租户的 App / Revision / Backend Profile |

`tenant_admin` 调用 `POST /admin/v1/tenants` 返回 `403 forbidden`——"我不能建租户"对这个调用方不是秘密。而 `tenant_admin` 访问**别的租户**返回 `404`，与"资源真的不存在"逐字节相同，并且在**任何 Repository 调用之前**返回：状态码或措辞只要差一个词，这个接口就成了枚举租户是否存在的 oracle；先查库再拒绝则会让一个租户的管理员驱使本进程代表另一个租户查库。

### 2.2 请求处理顺序

```text
1. 认证 Bearer 凭据                             → 401 unauthenticated（带 WWW-Authenticate: Bearer）
2. POST 的 Content-Type 必须是 application/json → 415 unsupported_media_type
3. 路径前缀匹配（原始 URL.Path，精确比较）        → 404 not_found
4. 租户作用域检查（路径含 tenant 时）             → 404 not_found
5. 方法匹配                                      → 405 method_not_allowed（带 Allow）
6. 角色检查（仅 POST /admin/v1/tenants）          → 403 forbidden
7. Entitlement / Tool Registry / Repository
```

**认证在最前面。** 没有有效凭据的调用方，在真实路由、不存在的路由和错误方法上都得到同一个 `401`，因此这个 API 不回答任何关于自己形状的问题。

整个 `/admin` 子树在 `http.ServeMux` **之前**被接管，并按**原始** `URL.Path` 精确比较，不解析任何 `.`、`..` 或重复斜杠。否则 mux 会先清理路径，把 `/admin//v1/tenants`、`/admin/./v1/tenants`、`/admin/v1/tenants/../secrets` 变成写给一个**没有凭据**的调用方的 `301`。现在这些路径先认证，再作为不存在的名字得到 `404`。

### 2.3 错误语义

| 情况 | 状态码 | `code` |
| --- | --- | --- |
| 缺少、格式错误或无效的 Bearer 凭据（含把 chat key 送到 Admin） | `401` | `unauthenticated` |
| `POST` 未声明 `application/json` | `415` | `unsupported_media_type` |
| 未知 Admin 路由；`tenant_admin` 访问其他租户；资源不存在 | `404` | `not_found` |
| 方法不被该路由接受（含 `OPTIONS`） | `405` | `method_not_allowed` |
| `tenant_admin` 创建租户 | `403` | `forbidden` |
| Revision 引用了本租户未被授权的 SecretRef 或 PolicyRef | `403` | `not_entitled` |
| Backend Profile 引用了本租户未被授权的 SecretRef | `403` | `not_entitled` |
| 请求体非法、未知字段、多于一个 JSON 值、非法资源 ID | `400` | `invalid_json` / `invalid_argument` |
| `publish` 时 Tool/Policy 组合不可服务 | `400` | `invalid_revision` |
| 重复 ID 或 Revision 编号 | `409` | `already_exists` |
| Backend Profile 达到每租户 32 个上限 | `409` | `profile_limit` |
| 租户被停用 | `403` | `tenant_inactive` |
| Revision 未发布、未被授权或 digest 不匹配 | `409` | `revision_unavailable` |

`not_entitled` 的措辞是固定的，不说明是哪个引用被拒绝：对"环境变量存在"和"不存在"、"策略已注册"和"从没听说过"必须给出同一个答案，否则这个端点就是对进程环境和 Tool Registry 的探针。`revision_unavailable` 同理，它把"未发布"、"未被授权"和"digest 不匹配"塌缩成同一句话。

## 3. 创建和发布 Agent

创建 Tenant（仅 `platform_admin`）：

```bash
curl -X POST http://127.0.0.1:8080/admin/v1/tenants \
  -H "Authorization: Bearer ${ADMIN_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"id":"team-a","slug":"team-a","name":"Team A"}'
```

创建 Agent App：

```bash
curl -X POST http://127.0.0.1:8080/admin/v1/tenants/team-a/apps \
  -H "Authorization: Bearer ${ADMIN_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"id":"assistant","name":"Team Assistant"}'
```

创建 Revision：

```bash
curl -X POST http://127.0.0.1:8080/admin/v1/tenants/team-a/apps/assistant/revisions \
  -H "Authorization: Bearer ${ADMIN_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"revision-1",
    "revision_no":1,
    "config":{
      "agent_name":"team-assistant",
      "instruction":"Answer the user request.",
      "model":{"provider":"deterministic","name":"team-echo-v1"}
    }
  }'
```

请求体里**没有** `created_by`。作者身份不是请求可以声明的东西，它就是发出这次请求的凭据的 Principal（`admin.PrincipalID`）。字段是"不存在"而不是"接受后覆盖"，因此仍然携带 `created_by` 的请求体会被 `DisallowUnknownFields` 拒绝为 `400 invalid_json`，而不是安静地表达一个和字面意思不同的语义。

发布 Revision（不带 body，但同样要求 `Content-Type`）：

```bash
curl -X POST \
  -H "Authorization: Bearer ${ADMIN_KEY}" \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:8080/admin/v1/tenants/team-a/apps/assistant/revisions/revision-1/publish
```

Revision 配置在创建时计算 SHA-256 `config_digest`，之后没有修改接口。更新配置必须创建新 Revision。再次发布一个历史 `published` Revision 会把它设为默认版本并递增 App 的 `routing_version`；当前没有记录本次回滚操作人与历史流水的独立审计存储。Runtime 在构建时会**重新计算并精确核验** `config_digest`，空 digest 算不匹配、不算豁免；对不上的 Revision 不会被服务（见第 3.2 节）。

### 3.1 Tool 与 Policy

Revision 可通过 `tool_refs` 选择进程内注册的 Tool，并通过 `policy_refs` 进一步收紧白名单。当前支持 `builtin_add`、`builtin_echo` 和策略 `builtin.safe-tools`：

```json
{
  "tool_refs": ["builtin_add", "builtin_echo"],
  "policy_refs": ["builtin.safe-tools"]
}
```

有 Tool 却没有 Policy、引用未知或重复、或者任一 Policy 不允许指定 Tool，都会在 Runtime 构建时 fail closed。多 Policy 取交集。详细执行、审计和测试契约见 [Tool 与 Policy Runtime](tool-policy.md)。

### 3.2 租户 Entitlement

Revision 引用的 `model.secret_ref` 和 `policy_refs` 必须先被**该租户**授权。授权表按租户组织，来自 Security Manifest 的 `tenant_entitlements`：能不能引用一个能力，是"跑这个 Revision 的租户"的属性，不是"碰巧创建它的人"的属性。

同一个 authorizer 实例在三处被询问：

| 位置 | 时机 | 失败 |
| --- | --- | --- |
| `POST .../revisions` | 写库**之前** | `403 not_entitled` |
| `POST .../revisions/{id}/publish` | 读出存储的 Revision 之后 | `403 not_entitled` |
| Runtime 构建 | digest 核验之后，Tool Registry 与 Secret 解析**之前** | `409 revision_unavailable` |

创建时校验可在保存 draft 前拒绝未授权引用，避免错误配置延迟到发布或运行时才暴露。Runtime 中 entitlement 检查先于 Secret 解析，未授权引用会在读取环境变量前被拒绝。

默认 demo profile 只授权 `demo` 租户使用 `builtin.safe-tools`，**不授权任何 SecretRef**。要让某个租户引用模型 key，需要提供自定义 manifest，示例见 [README 运行真实模型](../README.md#运行真实模型)，规则见[身份、权限与密钥治理](security-and-governance.md#6-租户-entitlement)。

### 3.3 Backend Profile

Backend Profile 是租户级的存储版本。ID 即版本，创建后不能 Update 或 Delete；切换配置需要创建新 ID，再让新的 Revision 引用它。请求体不允许声明 `tenant_id`、`fingerprint`、`created_by` 或 `created_at`：租户来自路径，作者来自认证凭据，其余字段由服务端产生。每租户最多 32 个 Profile。

创建一个无需 SecretRef 的 InMemory Profile：

```bash
curl -X POST http://127.0.0.1:8080/admin/v1/tenants/team-a/backend-profiles \
  -H "Authorization: Bearer ${ADMIN_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"id":"session-local-v1","session":{"backend":"inmemory"}}'
```

PostgreSQL 和 Redis Profile 只保存引用，不保存连接值：

```json
{
  "id": "session-postgres-v1",
  "session": {
    "backend": "postgres",
    "postgres": {
      "dsn_ref": "env:TEAM_A_SESSION_DSN",
      "schema": "team_a",
      "table_prefix": "sessions"
    }
  }
}
```

`TEAM_A_SESSION_DSN` 必须先出现在该租户的 `allowed_secret_refs` 中。创建 Profile、创建引用它的 Revision和发布时都会重新检查 entitlement，但不会读取环境或连接数据库；只有 Runtime 第一次解析这个 Profile 时才读取连接值。动态 Factory 会在 15 秒预算内 probe 后端，PostgreSQL 首次建表还会按目标获取 advisory lock。解析后的 DSN/URL 不进入响应；底层错误若回显连接值，会在 Factory 边界被整体替换并切断原错误链。

Revision 通过 `config.backend_profile_id` 引用 Profile。空值继续使用进程默认 Bundle：

```json
{
  "backend_profile_id": "session-postgres-v1"
}
```

## 4. 动态对话路由

对话路径仍为上游兼容的 `/v1/chat/completions`。租户、主体和 Session 归属全部由服务端从凭据推导，请求体中的 `user` 字段被忽略。

| Header | 必填 | 说明 |
| --- | --- | --- |
| `Authorization` | 是 | `Bearer {api_key}`；平台由此确定 Tenant、Principal 和可用 App |
| `X-Agent-App-ID` | 是 | Tenant 内的稳定 Agent App ID，必须在凭据授权范围内 |
| `X-Tenant-ID` | 否 | 只是客户端对自己凭据的断言；与认证结果不一致返回 `403` |
| `X-Agent-Revision-ID` | 否 | 仅对**新 Session 的首轮**生效的开发用指定版本；对已 Pin 的 Session 只能重复相同值 |
| `X-Session-ID` | 否 | 客户端会话 ID；缺失时由平台生成 UUID 并在响应头回传 |

响应头（成功和 Adapter 返回的 `400`/`500` 都会带上）：

| Header | 说明 |
| --- | --- |
| `X-Session-ID` | 本次实际使用的 Session ID，客户端用它续接同一段对话 |
| `X-Agent-Revision-ID` | 本次实际执行的 Revision，即该 Session 的 Pin |

```bash
curl -i http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer local-development-key-not-a-secret' \
  -H 'X-Agent-App-ID: echo' \
  -H 'X-Session-ID: conversation-1' \
  -d '{
    "model":"deterministic-echo",
    "messages":[{"role":"user","content":"hello"}]
  }'
```

路由顺序为：

```text
OPTIONS 预检直接返回（不认证）
→ 解析 Bearer 并认证，得到 {tenant_id, principal_id, allowed_app_ids}
→ 校验 X-Tenant-ID 断言（不一致 403）
→ 校验 X-Agent-App-ID 并检查授权（越权 403）
→ 校验或生成 X-Session-ID
→ Session Run Service 获取合作型租约（冲突 409，不可用 503）
→ Session Directory 查 Pin；没有 Pin 则解析候选 Revision 并原子 EnsurePin
→ Runtime Resolver 按 (tenant, app, pinned revision) 加载或复用 Runtime
→ 写入响应头，注入可信 RunContext
→ OpenAI-compatible Adapter
→ contextRunner（丢弃协议层 userID/sessionID）
→ Runner.Run(userID = u/{principal_id}, sessionID = 已 Pin 的 Session)
→ App 级共享 Session Service
```

错误语义：

| 情况 | 状态码 | `code` |
| --- | --- | --- |
| 缺少或错误的 Bearer 凭据 | `401`（带 `WWW-Authenticate: Bearer`） | `unauthenticated` |
| `X-Tenant-ID` 断言不符，或 App 不在凭据授权内 | `403` | `forbidden` |
| 缺少或非法 `X-Agent-App-ID` | `400` | `missing_route` |
| 非法 `X-Session-ID` | `400` | `invalid_session_id` |
| 同一 Session 已在另一个 Run 中 | `409`（带 `Retry-After: 2`） | `session_busy` |
| Run 租约后端不可用 | `503` | `coordination_unavailable` |
| 已 Pin 的 Session 收到不同的 `X-Agent-Revision-ID` | `409` | `pin_conflict` |
| Resolver 已关闭 | `503` | `runtime_unavailable` |

Session 的 Revision Pin 在首轮请求中原子写入，键为 `{tenant_id, app_id, principal_id, session_id}`。因此：发布新版本不会改变已经开始的 Session，回滚同样不会；新 Session 才会采用当前默认版本。并发首轮只有一个候选能成为 Pin，落败的请求会释放自己的 Runtime 租约并改用胜出的 Revision。

Runtime 的框架 `AppName` 为 `t/{tenant_id}/a/{app_id}`，不包含 Revision。因此发布或回滚后，同一 Tenant、App、Principal 和 Session 仍读取同一段会话历史。不同 Tenant 即使使用完全相同的 App、Revision、Principal 和 Session ID，也会进入不同的 Session 命名空间和 Runtime 缓存键。

CORS 预检允许 `Authorization`、`X-Tenant-ID`、`X-Agent-App-ID`、`X-Agent-Revision-ID`、`X-Session-ID`，并向浏览器暴露 `X-Session-ID` 和 `X-Agent-Revision-ID`。预检本身不认证，因为浏览器不会在预检请求上附带凭据。**这一节只适用于对话面**：Admin 面完全不发布 CORS 头，也没有预检分支（见第 2 节）。

## 5. 未实现能力

- 管理操作审计：谁在什么时候创建了租户、发布了哪个 Revision，目前只有 Revision 上的 `created_by` 一个字段，没有独立、不可篡改、可查询的 Audit Store。
- 静态 API Key 之外的凭据体系：JWT/OIDC、动态 RBAC、轮转、过期、撤销、按 Principal 的配额与限流。Security Manifest 只在启动时读一次，改文件必须重启进程。
- BackendProfile 列表以及 Tenant/App/Revision 查询都没有分页；跨节点配置通知尚未实现，Worker 目前在解析时读取 PostgreSQL 真相源，并依靠本进程缓存生命周期。
- 跨进程 Session 目录：默认 profile 下 Pin 只在单进程内存中，多节点部署会各自 Pin；`postgres` profile 下 Pin 落库，这一条才不成立。
- 主体间共享 Session、显式 Retire/Unpin（`Key.Epoch` 已预留但恒为 0）。Redis Run 租约已实现（见 [Session Run Lease](session-lease.md)），但它只做 Run 入口的合作型互斥，不做写入准入。
- 权重灰度、白名单路由、Runtime TTL/LRU 淘汰和配置失效通知。
- `openai-compatible` 模型 Provider 已支持运行时构造。Revision 的模型配置需要 `base_url`，可用 `secret_ref: "env:VAR_NAME"` 从 Worker 环境解析 API Key；未设置 `secret_ref` 表示无凭据调用。因为 `base_url` 由租户的 Revision 指定，上游 openai-go 从 Worker 进程环境派生的请求头都会被显式删除，具体是且仅是这三个：

  | 请求头 | 环境变量 | 删除时机 |
  | --- | --- | --- |
  | `OpenAI-Organization` | `OPENAI_ORG_ID` | 总是 |
  | `OpenAI-Project` | `OPENAI_PROJECT_ID` | 总是 |
  | `Authorization` | `OPENAI_API_KEY` | 仅 `secret_ref` 为空时；显式 `secret_ref` 保留自己解析出的那一个 |

  前两个是运营方 OpenAI 账号的标识，Revision schema 不建模这两个值，所以无论是否配置 `secret_ref` 都删除。`OPENAI_WEBHOOK_SECRET` 是 openai-go 的第四个环境默认值，但它不设置请求头，因此不在此列。这只是这一个客户端上的请求头抑制，**不等于**进程级 Secret 隔离：进程环境仍然可读，显式 `env:VAR_NAME` 只要落在该租户的 entitlement 之内就仍会解析。静态 Tool/Policy 组装和租户级 SecretRef/PolicyRef entitlement 都已实现（见 [安全与治理](security-and-governance.md)）；Secret Manager、Knowledge/Skill/MCP 仍未完成。

## 6. 已知限制

- **`base_url` 不是一项受 entitlement 约束的能力。** `secret_ref` 和 `policy_refs` 现在都要经过租户 entitlement（见 [安全与治理](security-and-governance.md) 第 6 节），但 Revision 指向哪个上游地址不受任何白名单约束。租户只能引用自己被授权的那些变量，却可以把解析出来的凭据发往它自己指定的 `base_url`。也就是说，entitlement 决定的是"哪个 Secret 会泄露给谁"，不是"会不会泄露"。
- **`config_digest` 是不带密钥的 SHA-256。** 发布态 Revision 在构建 Runtime 时会重算并逐字节比对指纹，因此绕过 Admin 面直接改库会在下一次构建时被拒；但指纹的计算方式是公开的、无密钥的，一个既能改数据行、又能顺手重算指纹的写入方仍然可以把流量改道。这一条由 `agent.TestRuntimeDigestDoesNotDefendAgainstAWriterWhoCanRecomputeIt` 显式记录，不是遗漏。要真正封死需要签名（HMAC 或非对称签名），密钥不能与数据行同源。
- **静态 Admin Key 没有轮转、过期和撤销。** 认证只回答"这个 Bearer 是不是清单里的某一条"，撤销一把 key 的唯一手段是改清单并重启进程。也没有管理操作审计：`created_by` 记录了是谁创建了 Revision 或 BackendProfile，但没有独立的、不可篡改的操作流水。
- **Admin 面走明文 HTTP，只监听回环地址。** 传输层加密预期由反向代理提供；进程本身不做 TLS，也不校验来源网段之外的任何东西。把它暴露到回环之外时，前置代理是必须的，不是可选的。
- **合法凭据可以制造无界 Session。** Session 没有总量配额或清理 TTL，一个持有有效 key 的调用方可以使用无限多的 `X-Session-ID`；InMemory profile 消耗进程内存，PostgreSQL/Redis profile 则持续占用外部存储。
- **首轮 OpenAI 历史可以伪造。** 平台只决定 Session 归属，不校验请求体里的 `messages`。新 Session 的第一轮里，调用方可以自行编造一段"历史对话"送进模型上下文。后续轮次会与服务端存储的 Session 事件合并，但首轮注入无法阻止。
- **Adapter 拒绝的请求也会建立 Pin。** Session ID 和 Revision 在调用上游 Adapter 之前就已确定并写入响应头，因此一个 JSON 格式错误的首轮请求同样会把该 Session 钉在当时的 Revision 上。
- **Tool 循环有轮数上限，没有总时长上限。** 每个 Run 最多执行 4 轮 tool calls，但慢模型、慢 Tool 或单轮大量并行 Tool 仍可能长期持有 Session Lease。结构化审计目前只写日志，不是持久化 Audit Store。
- 两条链路（对话面 `/v1/chat/completions` 与控制面 `/admin`）都已认证，但都只是静态 API Key。**不能据此认为平台整体达到生产安全标准。**
