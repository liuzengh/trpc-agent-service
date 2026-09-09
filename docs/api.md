# Admin API 参考

本文档是 Admin API 的**完整参考**，路由逐一提取自
[`trpcservice/web/admin.go`](../trpcservice/web/admin.go) 的 `RegisterRoutes`。
[guide.md §7](./guide.md#7-admin-api-速查) 是速查版，本文档覆盖每个端点的请求体、
响应结构与错误码。

## 总览

- **监听地址**：`TRPC_ADMIN_ADDR`，默认 `127.0.0.1:8081`。**仅内网**——默认绑 loopback，
  要暴露到主机之外必须显式改绑。每个角色（含 all-in-one）都有自己的 Admin 监听，
  不与 Gateway 的公网回调口共享地址。
- **鉴权**：每个请求都要 `Authorization: Bearer <TRPC_ADMIN_TOKEN>`，常数时间比较。
  fail-closed 双向兜底：token 未设时进程拒绝启动；即使进程起来了（不该发生），
  运行时每条路由也回 503。本地开发哨兵值 `dev-insecure`（仅允许绑 loopback 时使用）。
- **可选 mTLS**（仅独立 admin 角色）：`TRPC_ADMIN_TLS_CERT` / `_KEY` / `_CLIENT_CA`
  三者同设即启用，客户端证书须由该 CA 签发。
- **操作人归因**：写操作记入审计时，mTLS 下取客户端证书 CN；否则取请求头
  `X-Admin-User`（该头是"token 持有者自称"，不是身份证明）；都没有则为 `admin`。
- **请求体上限**：1 MiB（`adminBodyLimit`）。

### 错误响应的分层原则

错误体统一为 `{"error": "<message>"}`，状态码按「错的是哪一方」分：

- **本服务自己判断出的失败**（参数缺失、找不到、冲突、配置被拒）原样返回可读消息，
  状态码 400 / 404 / 409。
- **数据库 / 存储层的失败**只回 `"<操作名> failed"`（500），驱动原文（表名、列名、
  约束名、SQLSTATE、连接失败时的 DSN 明细）只进日志——响应体是最容易被原样贴进工单
  和群聊的东西。
- 鉴权失败：**401** `missing or invalid admin token`；token 未配置：**503**
  `admin token not configured`。

| 状态码 | 含义 | 典型场景 |
|---|---|---|
| 400 | 请求不合法 | 必填字段缺失、JSON 解析失败、配置校验被拒（模型端点不在白名单、webhook_path 未挂载、config 未知字段等） |
| 401 | 未鉴权 | 缺/错 Bearer token |
| 404 | 不存在 | 租户/应用/版本/绑定/迁移不存在 |
| 409 | 状态冲突 | 重复发布、无可回滚版本、改 published 应用、`webhook_path` 已被占用、同租户同资源已有活跃迁移、直改 `storage_config` |
| 500 | 服务端失败 | `"<操作名> failed"`，明细看日志 |
| 503 | 功能关闭 | token 未配置；未配置 embedder 时调知识库摄入 |

所有写操作都会：① 同步记审计（`channel=admin`，`detail` 带变更前后内容）；
② 经 Redis pub/sub 广播配置失效，Worker 秒级丢弃本地缓存（TTL 30s 兜底）。

### 端点一览

| 方法 | 路径 | 用途 |
|---|---|---|
| POST | `/admin/tenants` | 创建租户 |
| GET | `/admin/tenants` | 列出租户 |
| GET | `/admin/tenants/{id}` | 租户详情 |
| PATCH | `/admin/tenants/{id}` | 更新租户配置 / 启停 |
| POST | `/admin/tenants/{id}/apps` | 创建应用（新版本，draft） |
| GET | `/admin/tenants/{id}/apps` | 列出租户的应用 |
| PATCH | `/admin/tenants/{id}/apps/{app}` | 改 draft 应用的 config |
| POST | `/admin/apps/{id}/publish` | 发布指定版本（原子切换） |
| POST | `/admin/apps/{id}/rollback` | 回滚到历史版本 |
| POST | `/admin/apps/{id}/bindings` | 创建渠道绑定 |
| GET | `/admin/apps/{id}/bindings` | 列出应用的绑定 |
| DELETE | `/admin/apps/{id}/bindings/{binding}` | 删除绑定 |
| POST | `/admin/apps/{id}/knowledge/documents` | 摄入知识库文档 |
| POST | `/admin/tenants/{id}/storage-migrations` | 发起后端迁移 |
| GET | `/admin/storage-migrations/{id}` | 查迁移进度 |
| GET | `/admin/audit` | 审计查询 |

---

## 租户

### `POST /admin/tenants` — 创建租户

请求体：

| 字段 | 必填 | 说明 |
|---|---|---|
| `name` | 是 | 租户名 |
| `model_config` | 否 | 模型配置 JSONB；`model.base_url` 必须 https 且 host 命中平台白名单（`TRPC_MODEL_BASE_URL_ALLOW`），否则 400 |
| `tool_policy` | 否 | 工具策略（白名单等） |
| `audit_policy` | 否 | 审计与数据保留策略 |
| `guardrail_policy` | 否 | 治理策略：`input_allow_users` / `input_deny_words` / `output_deny_words` / `max_tokens_per_day` |
| `rate_policy` | 否 | 限流：`{"qps":50,"burst":100}`，可加 `send_qps` / `send_burst` 覆盖发送侧 |
| `storage_config` | 否 | 后端覆盖（受控菜单），如 `{"session":{"type":"redis","dsn_ref":"t42-redis"}}` |

响应 `201`：`{"id": "<uuid>"}`。缺 `name` → 400。

### `GET /admin/tenants` — 列出租户

响应 `200`：按创建时间倒序的数组，每项
`{"id","name","status","created_at","updated_at"}`。

### `GET /admin/tenants/{id}` — 租户详情

响应 `200`：`{"id","name","status","model_config","tool_policy","audit_policy",
"guardrail_policy","rate_policy","storage_config","created_at","updated_at"}`
（策略字段无值时为 `null`）。不存在 → 404 `tenant not found`。

### `PATCH /admin/tenants/{id}` — 更新租户

请求体（全部可选，至少给一个）：`name`、`status`（仅 `active` / `disabled`）、
`model_config`、`tool_policy`、`audit_policy`、`guardrail_policy`、`rate_policy`。
`model_config` 同样过端点白名单校验。

- `storage_config` **不允许在这里改** → 409，必须走迁移流程（见下文）。
- 全部字段缺席 → 400 `nothing to update`；不存在 → 404。
- 响应 `200`：`{"updated": true}`。

## 应用

### `POST /admin/tenants/{id}/apps` — 创建应用

请求体：`name`（必填）、`agent_type`（必填，如 `llm`）、`config`（必填 JSONB，
含 `prompt`、`model`、`tools` 等；`config.model.base_url` 受白名单校验）。

同名应用自动递增版本号，新建即 `draft`（未发布不接客）。

响应 `201`：`{"id","version","status":"draft"}`。
并发创建撞版本唯一约束 → 409（提示重试）。

### `GET /admin/tenants/{id}/apps` — 列出应用

响应 `200`：按 `name`、版本倒序的数组，每项
`{"id","name","agent_type","version","status","updated_at"}`。
`status` 取值：`draft` / `published` / `disabled`。

### `PATCH /admin/tenants/{id}/apps/{app}` — 改 draft 配置

请求体：`config`（必填，过白名单校验）。**只能改 `draft`**——published 版本是
不可变快照，回滚才是纯状态切换。目标不存在或非 draft → 409。
响应 `200`：`{"updated": true}`。

### `POST /admin/apps/{id}/publish` — 发布

请求体：**无需请求体**——路径里的 `{id}` 就是目标版本行的 ID（`agent_app` 每行即一个版本；
示例中常见的 `{"version":1}` 会被忽略）。把同 `(tenant, name)` 下当前 published 版本置为
`disabled`、目标版本置为 `published`，**同一事务内**顺带把该应用族所有
`channel_binding.app_id` 重指到新版本——绑定跟随已发布版本。发布时再过一次端点
白名单（堵「白名单收紧前存的 draft」）。

响应 `200`：`{"published": "<app_id>"}`。已是 published → 409；不存在 → 404。

### `POST /admin/apps/{id}/rollback` — 回滚

请求体：`{"version": <n>}`，**可空**——空则回滚到当前版本的前一个版本。
本质是"把历史版本重新 publish"，走与发布相同的事务与校验。

响应 `200`：`{"published": "<app_id>", "version": <n>}`。
无更早版本或目标即当前版本 → 409；目标版本不存在 → 404。

## 渠道绑定

### `POST /admin/apps/{id}/bindings` — 创建绑定

只能绑到 **published** 的应用（否则 404 `app not found or not published`）。

请求体：

| 字段 | 必填 | 说明 |
|---|---|---|
| `channel` | 是 | 通道名：`mock` / `wecom` / `wxkf` / `wecomws` |
| `webhook_path` | 否 | **留空最安全**：自动填充为 `/callback/{channel}/{binding_id}`。wecomws 例外，见下 |
| `token_ref` | 否 | 密钥**引用名**，绝不收明文 |
| `aeskey_ref` | 否 | 同上 |
| `config` | 否 | 通道专有配置 JSONB（按通道校验 schema，未知字段一律 400） |

会被 400 拒掉的写法（有意为之，理由见 guide §7）：

- `webhook_path` 填了平台没挂载的路径——只接受空（自动填充）或该通道适配器自己挂载的
  legacy 路径（`/mock/callback` / `/wecom/callback` / `/wxkf/callback`）。
- 用 legacy 路径却自带 `token_ref` / `aeskey_ref`——legacy 路径验签只用 env 全局凭据，
  绑定行上的引用"看起来权威、实际从不被读"。
- `config` 含未知字段——`config` 原样进审计明细，未被识别的键会既进审计又不生效。
- `channel=wecomws`：`webhook_path` 必须匹配 `^/wecomws/[A-Za-z0-9_-]+$` 且后缀必须等于
  `config.bot_id`；`config` 必须有非空 `bot_id` + `secret_ref`；`token_ref`/`aeskey_ref`
  必须留空（WS 通道按 bot 的 secret 鉴权，不走回调加解密密钥）。

响应 `201`：`{"id": "<uuid>", "webhook_path": "<实际生效路径>"}`。
`webhook_path` 撞唯一约束 → 409。

### `GET /admin/apps/{id}/bindings` — 列出绑定

响应 `200`：按创建时间排序的数组，每项
`{"id","channel","webhook_path","token_ref","aeskey_ref","status","created_at"}`。

### `DELETE /admin/apps/{id}/bindings/{binding}` — 删除绑定

响应 `200`：`{"deleted": true}`。不存在 → 404。

## 知识库

### `POST /admin/apps/{id}/knowledge/documents` — 摄入文档

请求体：`name`（必填）、`content`（必填，内联文本）。`tenant_id` / `app_id` 被强制
写入文档 metadata，保证 Agent 侧检索按租户隔离。

响应 `201`：`{"ingested": "<name>"}`。
**未配置 embedder（`TRPC_EMBEDDER_MODEL` 未设）→ 503** `knowledge is disabled`；
应用不存在 → 404。

## 存储迁移

### `POST /admin/tenants/{id}/storage-migrations` — 发起迁移

请求体：`resource`（目前只接受 `"session"`）、`to_backend`（`"redis"` 或 `"postgres"`）。

`from_backend` 取租户的 `storage_config.session.type` 覆盖值，没有则用平台默认
（`TRPC_SESSION_BACKEND`）。创建后即进入 `dual_write` 阶段，装配层开始双写，
由 Migrator 推进后续阶段（回填 → 读切换 → 观察窗，流程见
[design.md §5.2.6](./design.md)）。

响应 `201`：`{"id": "<uuid>", "phase": "dual_write"}`。
租户不存在或非 active → 404；已在目标后端 → 409；同租户同资源已有活跃迁移 → 409。

### `GET /admin/storage-migrations/{id}` — 迁移进度

响应 `200`：`{"id","tenant_id","resource","from_backend","to_backend","phase",
"progress","error","created_at","updated_at"}`。不存在 → 404。

## 审计

### `GET /admin/audit` — 审计查询

查询参数（全部可选，可组合）：`tenant_id`、`session_id`、`trace_id`、`decision`
（`allow` / `deny` / `review` / `review_timeout`）、`limit`（默认 100，上限 1000）。

响应 `200`：按时间倒序的数组，每项：

```json
{
  "id": "...", "tenant_id": "...", "channel": "...", "user_id": "...",
  "session_id": "...", "tool_name": "...", "decision": "allow",
  "error_type": "", "latency_ms": 0, "trace_id": "...",
  "detail": {"before": ..., "after": ...}, "created_at": "..."
}
```

管理端写操作的审计行 `channel` 为 `admin`、`tool_name` 为操作名（如
`create_tenant`），`detail` 带变更前后内容。

## 调用示例

```bash
H='Authorization: Bearer dev-insecure'
A=127.0.0.1:8081

curl -s -X POST $A/admin/tenants -H "$H" -H 'Content-Type: application/json' \
  -d '{"name":"acme-demo"}'
curl -s -H "$H" "$A/admin/audit?tenant_id=$T&decision=deny&limit=50"
```

更多实战示例见 [quickstart.md](./quickstart.md) 第 3 章与
[guide.md §7](./guide.md#7-admin-api-速查)。
