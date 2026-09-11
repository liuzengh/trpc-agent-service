# ChannelBinding V1 公开 API 契约审计（供 Web 流程设计）

> 2026-09-07 增量：双接收方式已在 Web 实施。下文的首版 config 全只读、Telegram 两项凭据必需等是历史基线；当前仅开放停用态的结构化 config.receive_mode 修改，生成字段仍只读。新建默认 LP、模式化凭据/预检、旧 pending 兼容与独立启用确认见 [双模式实现记录](./telegram-receive-modes-v1-plan.md#15-实施记录2026-09-07)。运行验收另记。
- 日期：2026-09-06。
- 审计基线：`codex/tenant-rbac-workspace`，HEAD `661e4a8`。
- 本文是当前源码、JSON Schema、OpenAPI 与一方设计文档的对照记录，不是前端已实现声明。
- 未操作运行服务、数据库或真实机器人；本文的 HTTP 行为结论来自 handler/application/domain，而非本次在线调用。
- 核心结论：**11 个公开管理操作 = ChannelAccount 6 个 + ChannelBinding 5 个；7 个写操作全部 OWNER-only，4 个读操作允许 ACTIVE MEMBER/OWNER。** 账户连接与绑定接纳是两个开关；固定 DeploymentRevision 是唯一绑定目标。

> 后续增量：Telegram预检V1已冻结、实现中/待联调，另增2条公开与3条私有操作，见第11节。以下11项清单与历史核验输出保留原审计时点，不将新增接口写成当时已经部署。

## 1. 公开路由清单

所有路径均为公开 Session 管理 API，下表列出完整路径。

| # | 方法与后缀 | 请求 / 用途 | 成功响应 |
| --- | --- | --- | --- |
| 1 | `POST /v1/tenants/{tenant_id}/channel-accounts` | 创建账户，按 Provider 提交完整必需凭据 | `201 ChannelCommandResult` |
| 2 | `GET /v1/tenants/{tenant_id}/channel-accounts` | 账户列表，`cursor?`、`page_size?` | `200 {accounts,next_cursor?}` |
| 3 | `GET /v1/tenants/{tenant_id}/channel-accounts/{account_id}` | 账户详情、当前 Binding、观测 | `200 ChannelAccountDetails` |
| 4 | `PATCH /v1/tenants/{tenant_id}/channel-accounts/{account_id}` | `expected_account_revision` + `name` / `description` 至少一项 | `200 ChannelCommandResult` |
| 5 | `POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/credentials/{purpose}/update` | 两级 Expected + `keep/replace/clear` | `200 ChannelCommandResult` |
| 6 | `POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/enabled` | `expected_account_revision` + `enabled` | `200 ChannelCommandResult` |
| 7 | `POST /v1/tenants/{tenant_id}/channel-bindings` | `account_id` + 精确 `target` | `201 ChannelCommandResult` |
| 8 | `GET /v1/tenants/{tenant_id}/channel-bindings` | 绑定列表，`cursor?`、`page_size?` | `200 {bindings,next_cursor?}` |
| 9 | `GET /v1/tenants/{tenant_id}/channel-bindings/{binding_id}` | 绑定详情与分发事实 | `200 ChannelBindingDetails` |
| 10 | `POST /v1/tenants/{tenant_id}/channel-bindings/{binding_id}/target` | `expected_binding_revision` + 精确 `target` | `200 ChannelCommandResult` |
| 11 | `POST /v1/tenants/{tenant_id}/channel-bindings/{binding_id}/enabled` | `expected_binding_revision` + `enabled` | `200 ChannelCommandResult` |

依据：[实际路由注册](../../../services/control-api/internal/channelbinding/adapter/inbound/http/handler.go#L40-L51)、[账户设计表](../control-api/channel-account.md#L111-L131)、[绑定设计表](../control-api/channelbinding.md#L76-L92)、[公开 OpenAPI](../../../api/openapi/control/v1/channel-public.yaml#L8-L1567)。

**没有公开的 delete、validate、probe、test-message、Binding 历史、按 account_id 查询 Binding 的过滤参数、回滚命令或 Gateway 路由 ACK 查询。** 删除、在线测试、即时生效、历史时间线不得从现有路由推导为已有产品功能。业务回退用“改回旧 DeploymentRevision”的新写命令完成。

## 2. 输入字段与关闭约束

### 2.1 创建账户

必需字段：`provider`、`provider_account_id`、`name`、`credentials`；`description` 可省略，响应中为字符串。创建固定为 `enabled=false`，账户与连接版本均为 1。客户端不提交 `tenant_id`、`config`、`enabled`、`account_id` 或凭据引用。

| Provider | 物理 ID | credentials 精确键集合 | 只读生成配置 |
| --- | --- | --- | --- |
| `telegram` | 非零十进制数字字符串，服务端去前导零；不是 @username | `telegram.bot_token`、`telegram.webhook_secret` | `webhook_path=/v1/telegram/{account_id}` |
| `wecom` | Bot ID 字符串，拒绝空白/控制字符 | `wecom.bot_secret` | `bot_id=provider_account_id` |

每项 credentials 均为 `{action:"replace",value:string}`。初次创建必须全部齐全，不接受 keep/clear。Provider 与物理 ID 创建后不可编辑。同一执行范围内 Provider + 规范化物理 ID 唯一，重复登记跨租户也返回统一冲突，不泄露另一归属。

- name 去首尾空白后 1..128 Unicode code points；description 最大 4096。
- provider_account_id Domain 最大 **1024 字节**；Schema 的 maxLength 是 1024 字符，前端应同时做 UTF-8 字节校验以免中文 ID 通过本地后被拒绝。
- 通用凭据 Domain 最大 **16 KiB UTF-8 字节**，不 trim、不自动转换；Schema maxLength 为 16,384 字符。
- webhook_secret 必须全串匹配 `^[A-Za-z0-9_-]{1,256}$`。
- 创建不在线探测 Provider，保存成功只是保存配置，不等于 Bot Token 被远端接受。

依据：[Create Schema](../../../api/schemas/channel/v1/account-create.schema.json#L4-L142)、[Create DTO 与完整用途验证](../../../services/control-api/internal/channelbinding/application/commands.go#L175-L245)、[身份/长度/默认状态](../../../services/control-api/internal/channelbinding/domain/account.go#L80-L130)、[凭据值校验](../../../services/control-api/internal/channelbinding/domain/credential.go#L20-L39)、[唯一性与非在线验证](../control-api/channel-account.md#L57-L68)。

### 2.2 编辑、启停与凭据更新

- 元数据：`{expected_account_revision,name?,description?}`；至少一个修改字段，字段不能为 null。**真实接口仅支持名称和描述，不支持 config PATCH。**
- 账户启停：`{expected_account_revision,enabled:boolean}`。
- 凭据：路径 purpose 必须适用于该 Provider；Body 必需 `expected_account_revision`、`expected_credential_version`、`action`。
  - replace 必需非空 value；每次成功都推进该凭据版本，即使输入值相同。
  - keep / clear 禁止 value（包括 null）；keep 为 NOOP。
  - clear 只允许 account 已停用；已清空再 clear 是 NOOP。
  - 公开 GET 只给 `purpose,credential_version,configured`，不回显明文或 credential_id。

依据：[元数据 Schema](../../../api/schemas/channel/v1/account-update.schema.json#L4-L37)、[启停 Schema](../../../api/schemas/channel/v1/account-enabled.schema.json#L4-L19)、[凭据更新 Schema](../../../api/schemas/channel/v1/credential-update.schema.json#L4-L62)、[两级 CAS/clear/replace 语义](../../../services/control-api/internal/channelbinding/domain/credential.go#L109-L146)、[公开凭据 DTO](../../../services/control-api/internal/channelbinding/domain/credential.go#L42-L68)。

### 2.3 Binding 目标

```json
{
  "account_id": "cha_SAMPLE",
  "target": {"deployment_id": "dpl_SAMPLE", "revision_number": 1}
}
```

上例仅表示关闭字段结构。SetTarget 去掉 account_id，增加 `expected_binding_revision`；SetEnabled 使用 Expected + enabled。版本整数范围 `1..9007199254740991`。不得使用 latest、Draft、AgentVersion、ProfileRevision 或客户端指定 manifest_ref 来替代目标；Deployment 拥有方解析并验证准确 Revision、Manifest 和摘要。

一账户至多一个稳定 Binding；初次 Binding revision=1、enabled=false。没有改 account_id 或删除后重建接口。

依据：[Binding Create Schema](../../../api/schemas/channel/v1/binding-create.schema.json#L4-L23)、[精确 target Schema](../../../api/schemas/channel/v1/common.schema.json#L373-L395)、[SetTarget Schema](../../../api/schemas/channel/v1/binding-target.schema.json#L4-L19)、[目标解析](../../../services/control-api/internal/channelbinding/application/commands.go#L158-L173)、[一账户一绑定](../../../services/control-api/internal/channelbinding/application/commands.go#L394-L425)、[Binding 初始值](../../../services/control-api/internal/channelbinding/domain/binding.go#L40-L47)。

### 2.4 HTTP 通用规则

JSON 是关闭对象，未知/重复/大小写变体 key、非法 UTF-8、缺失必需字段、禁止的 null 均被拒绝。Body 必须 `application/json`；Content-Encoding 被拒绝。账户写 Body 上限 64 KiB，Binding 写上限 16 KiB。所有写请求拒绝 Query；详情 GET 也拒绝 Query。列表仅 cursor/page_size，默认 50、上限 100，重复参数、空值、未知参数拒绝。

依据：[handler 解码/请求限制](../../../services/control-api/internal/channelbinding/adapter/inbound/http/handler.go#L73-L117)、[详情与列表 Query](../../../services/control-api/internal/channelbinding/adapter/inbound/http/handler.go#L237-L291)、[分页默认](../../../services/control-api/internal/channelbinding/application/queries.go#L14-L24)、[关闭 Schema 验证](../../../api/schemas/channel/v1/schema.go#L65-L101)。

## 3. 响应形状：不要混用命令回执和最新读模型

### 3.1 基础对象

- `ChannelAccount`：tenant_id/account_id/provider/provider_account_id/name/description，account_revision/connection_revision/min_route_generation，enabled/config，created_by/created_at/updated_at，credentials 状态数组。
- `ChannelBinding`：tenant_id/binding_id/account_id/binding_revision/enabled，target，created_by/created_at/updated_at。
- `target` 返回：tenant_id/deployment_id/revision_number/deployment_revision_id/**manifest_ref**/manifest_digest。公开 wire 字段不是文档个别段落中的 manifest_id。

依据：[Account wire struct](../../../services/control-api/internal/channelbinding/domain/account.go#L62-L78)、[Binding/target wire struct](../../../services/control-api/internal/channelbinding/domain/binding.go#L5-L38)。

### 3.2 查询与命令

```text
CommandResult = {account?,binding?,route_generation,event_id?,distribution}
AccountDetails = {account,binding?,route_generation,route_event_id?,distribution,gateway_application,observations}
BindingDetails = {binding,route_generation,route_event_id?,distribution,gateway_application}
AccountPage = {accounts: Account[],next_cursor?}
BindingPage = {bindings: Binding[],next_cursor?}
```

- **命令字段 event_id；详情字段 route_event_id**。不要误用同一个 DTO。
- 命令正常实现含 account；绑定存在时还含 binding。成功 Receipt 保存当时快照，重放不变；随后 GET 才是最新状态。
- `CommandResult.distribution` 实现仅新事件 `PENDING` / 本次未发事件 `NOT_EMITTED`。`NOT_EMITTED` 可能表示本次只是元数据/凭据/NOOP，不代表历史没有路由。
- 详情 distribution 读取当前路由事件 Outbox，枚举 `NOT_EMITTED | PENDING | IN_FLIGHT | PUBLISHED | FAILED`；PUBLISHED 只确认持久 PubAck。
- 详情 `gateway_application` 当前实现固定 `UNKNOWN`。
- AccountPage 不含 Binding、分发状态或观测；BindingPage 不含账户名称/Provider。不得在未加载的页上推断“未绑定”，也不能为列表全量筛选发送未支持的 query。
- 分页以 ID 升序稳定游标进行，不是更新时间倒序；没有 total_count。

依据：[命令返回构造](../../../services/control-api/internal/channelbinding/application/ports.go#L120-L136)、[新事件标记](../../../services/control-api/internal/channelbinding/application/commands.go#L135-L156)、[查询 DTO](../../../services/control-api/internal/channelbinding/application/queries.go#L27-L49)、[实际分发/观测查询](../../../services/control-api/internal/channelbinding/adapter/outbound/postgres/queries.go#L18-L108)、[列表排序与独立页](../../../services/control-api/internal/channelbinding/adapter/outbound/postgres/queries.go#L114-L202)。

### 3.3 连接观测

账户详情的 observations 为最近 24h、最多 32 条记录，按 received_at 倒序。每个实例/epoch 有自己的状态；不能只取任意一条 READY 宣称整个入口就绪。

state：`CONFIG_APPLIED | CONNECTING | READY | DISABLED | ERROR`。

reason_code：`NONE | AUTHENTICATION_FAILED | PROVIDER_UNAVAILABLE | REMOTE_IDENTITY_MISMATCH | CONFIG_INVALID | SOURCE_UNAVAILABLE | CREDENTIAL_UNAVAILABLE | OWNERSHIP_LOST | REGISTRATION_PENDING | REGISTRATION_UNKNOWN | SHUTDOWN`。

`effective_state` 由服务端计算：接收时间超过 90 秒，或观测 connection_revision 不等于当前账户连接版本，则 `STALE`；否则等于 state。界面优先显示 effective_state、版本、received_at，并可展开原始 state/reason/instance。`READY` 是连接观测，不是路由已应用或 Worker 执行完成的证明。空 observations 显示“尚无连接观测”，不是 ERROR。

依据：[observations Schema 枚举](../../../api/schemas/channel/v1/observations.schema.json#L80-L102)、[观测公开 DTO](../../../services/control-api/internal/channelbinding/application/runtime.go#L66-L76)、[查询期限/数量/STALE](../../../services/control-api/internal/channelbinding/adapter/outbound/postgres/queries.go#L52-L77)。

## 4. 权限、CAS 与幂等重试

所有公开请求要求有效非 Restricted Session + Active Tenant membership。7 个写操作还要求 OWNER；Handler 在读 Body 前授权，Application 再授权，事务提交前通过 Tenant 拥有方复核。普通 MEMBER 应能查看页面，但写控件显示 OWNER 限定。平台运维身份不自动获得租户 OWNER 权限。

每个写请求必须恰好一个 `Idempotency-Key`，长度 1..128，字符 ASCII 33..126；推荐客户端为一次明确提交生成随机 UUID。作用域 tenant/operation/scope/key，请求规范化输入还绑定 actor。

| 情况 | 后端语义 | 前端动作 |
| --- | --- | --- |
| 响应丢失/网络超时，结果未知 | 原 key + 原规范化请求体返回原脱敏结果；创建重放仍 201 | 保留一次提交意图，禁止换 key 自动重发；优先同 key 精确重试，成功后 GET |
| 原 key + 改过目标、Expected 或正文 | 409 Idempotency conflict | 新编辑意图必须生成新 key，不覆盖原请求 |
| 同操作另一 actor 使用旧 key | 规范化输入/MAC 不匹配；仍先授权 | 不跨用户共享写意图 |
| 旧 Expected，即使内容和当前相同 | CAS conflict | 保留非秘密用户输入，拉取当前版本，对照确认后用新 Expected/key |
| 当前 Expected + 相同状态/目标 | NOOP，无版本或事件推进 | 显示“状态已是目标值”，不要杜撰新版本 |
| 凭据 replace 同一个值 | 显式 replace 仍推进版本 | 防双击；不要用值相等判断 NOOP |
| 写事务失败 | 没有部分账户/凭据/路由或成功 Receipt | 明确错误修正后新 key；超时未知仍按上一条处理 |

凭据写意图只在内存中短期保存，避免把 value 放入 URL、本地持久缓存、诊断文本或截图。刷新后若丢失原秘密，不伪造“原请求重试”；重新 GET 核对版本/configured，并让用户明确重新输入。当前没有公开 Receipt 查询 API；版本/configured 变化本身不足以在并发场景证明原命令成功，界面应保留“原提交结果待核对”，不擅自认定成功或失败。非秘密 Binding 意图可以按 user/tenant/object 分区保存原 key/body，身份变化时清理。

依据：[前置身份与禁止 Restricted](../../../services/control-api/internal/channelbinding/adapter/inbound/http/handler.go#L53-L89)、[Owner 授权](../../../services/control-api/internal/channelbinding/application/commands.go#L19-L39)、[Receipt 规范化、授权、原结果重放](../../../services/control-api/internal/channelbinding/application/commands.go#L52-L132)、[创建重放 201 的 OpenAPI 明文](../../../api/openapi/control/v1/channel-public.yaml#L23-L41)、[Account CAS 先于 NOOP](../../../services/control-api/internal/channelbinding/domain/account.go#L151-L192)、[Binding CAS 先于 NOOP](../../../services/control-api/internal/channelbinding/domain/binding.go#L49-L96)。

## 5. 两个开关与版本语义

```text
effective_route_enabled = account.enabled AND binding.enabled
```

| Account | Binding | UI 应说明 |
| --- | --- | --- |
| disabled | 不存在/disabled | 配置已保存，账户未接入；初始态 |
| enabled | disabled | 账户可由 Gateway 接入，但新消息尚不交给部署 |
| enabled | enabled | 两项期望配置已开放；仍需等待实际 Gateway 资格，不能直接显示“已上线” |
| disabled | enabled | 账户停用，Binding 启用意图保留；重新启用账户会恢复该目标路由 |

推荐明确动作：保存账户 → 启用账户 → 查看连接观测 → 选择固定 DeploymentRevision 创建绑定 → 显式启用绑定。也可先创建 disabled Binding 后启用账户；**API 不以 READY 观测作为启用 Binding 的前置条件**，前端不应制造 READY 与首次接入的循环依赖。

- 启用 Binding 要求 account enabled、必需凭据 configured、精确 target 完整性通过。
- 停用账户不是停用 Binding；重新启用确认对话框必须显示可能恢复的具体 Deployment rN。
- 改名只推进 account_revision；凭据/账户启停推进 account_revision + connection_revision。
- Binding 变化推进 binding_revision；只在有效路由内容变化时推进 route_generation 与 min_route_generation，不重建机器人连接。
- 创建 Binding 首次发 disabled generation=1；disabled 期间改 target 保存目标但没有新有效路由事件。
- 回退目标是以新 CAS/key 改回旧 Deployment rN，不降低 generation，不重写 Manifest，也不重定向已接纳 Run。

依据：[Binding 前置条件](../../../services/control-api/internal/channelbinding/domain/binding.go#L68-L96)、[账户重新启用核对原 target](../../../services/control-api/internal/channelbinding/application/commands.go#L349-L390)、[四状态与恢复说明](../control-api/channelbinding.md#L55-L74)、[版本矩阵](../control-api/channel-account.md#L94-L109)、[初次 disabled 投影](../control-api/channelbinding.md#L148-L153)、[业务回退](../control-api/channelbinding.md#L300-L301)。

## 6. 稳定错误 → 前端动作

统一 `{error:{code,field,message}}`，message 是固定泛化文本，前端应基于 code 本地化，field 是有限 owner 路径。Schema 解码失败可能 field 为空，不承诺逐字段详细错误。

| HTTP / code | 前端处理 |
| --- | --- |
| 401 `UNAUTHENTICATED` | 登录恢复；保留允许保存的非秘密表单信息 |
| 403 `CHANNEL_PERMISSION_DENIED` | 刷新身份/租户角色，切到只读或返回租户入口；不重试写入 |
| 400 `CHANNEL_INPUT_INVALID` / `CHANNEL_BINDING_INPUT_INVALID` | field 对应错误或表单总错误；检查关闭字段、purpose、Expected、版本/ID；不要仅显示 generic message |
| 413 或 422 `CHANNEL_LIMIT_EXCEEDED` | 413 缩减请求；422 账户数/快照/平台额度已达上限，提交给运维诊断，不原样循环重试 |
| 404 `CHANNEL_ACCOUNT_NOT_FOUND` / `CHANNEL_BINDING_NOT_FOUND` | 对象已不可见，刷新列表；不泄露跨租户存在性 |
| 404 `CHANNEL_TARGET_NOT_FOUND` | 清理失效选项并重新选择本租户已发布 DeploymentRevision |
| 409 `CHANNEL_ACCOUNT_IDENTITY_CONFLICT` | 物理账户已登记，提示核对 Bot ID/已有账户；不显示另一租户信息 |
| 409 `CHANNEL_ACCOUNT_ALREADY_BOUND` | GET Account 获取真实现有 Binding，转“更改目标”，不再次 create |
| 409 `CHANNEL_REVISION_CONFLICT` / `CHANNEL_BINDING_REVISION_CONFLICT` / `CHANNEL_CREDENTIAL_VERSION_CONFLICT` | GET 当前值，显示冲突；用户确认新基线后新命令，不能悄悄覆盖 Expected |
| 409 `CHANNEL_IDEMPOTENCY_CONFLICT` | 保留并核对原写意图；新意图新 key，未知结果不能随意换 key |
| 409 `CHANNEL_ACCOUNT_DISABLED` | 引导账户启用；展示当前 Binding 目标，不偷偷代用户打开账户 |
| 422 `CHANNEL_CREDENTIAL_REQUIRED` | 跳到对应账户凭据区补齐 configured 状态 |
| 409 `CHANNEL_ACCOUNT_MUST_BE_DISABLED` | 清除凭据前明确执行账户停用；停用会影响现有绑定接纳 |
| 409 `CHANNEL_VERSION_EXHAUSTED` / `CHANNEL_ROUTE_GENERATION_EXHAUSTED` | 系统计数上限，显示诊断并交由后端处理；不重置版本 |
| 500 `CHANNEL_SOURCE_INTEGRITY` / `CHANNEL_TARGET_INTEGRITY` | 保持当前事实，不跳到 latest，不把完整性错误包装成输入错；复制脱敏诊断 |
| 503 `CHANNEL_DEPENDENCY_UNAVAILABLE` | 有界退避/手动刷新，提交结果未知时原 key/body 重试 |

依据：[公开 handler 精确映射](../../../services/control-api/internal/channelbinding/adapter/inbound/http/handler.go#L317-L354)、[Domain 稳定错误名](../../../services/control-api/internal/channelbinding/domain/errors.go#L13-L24)、[请求长度与 Schema 错误](../../../services/control-api/internal/channelbinding/adapter/inbound/http/handler.go#L91-L115)。

## 7. 公开管理面与内部运行面边界

下面 **3 个内部操作不属于上述 11 个公开 API**，由独立 mTLS listener 面向 Gateway workload：

1. `GET /internal/v1/channel-accounts/snapshot`
2. `POST /internal/v1/tenants/{tenant_id}/channel-accounts/{account_id}/credentials:resolve`
3. `POST /internal/v1/channel-account-observations`

Web 不调 snapshot/resolve/report；只消费公开 AccountDetails 的脱敏观测。Gateway 运维探针也不是租户账户管理入口。

另外，Channel 模块只在 `CONTROL_CHANNEL_CONFIG_FILE` 有效配置时注册。代码已拉取不等于当前运行后端已启用接口。前端加载失败需区分标准 ChannelError 与普通路由不存在；当前没有公开 capability endpoint，不能把所有 404 都解释为“租户没有账户”。

依据：[内部独立监听与 3 路由](../../../services/control-api/internal/channelbinding/adapter/inbound/internalhttp/handler.go#L1-L2)、[内部路由注册/mTLS](../../../services/control-api/internal/channelbinding/adapter/inbound/internalhttp/handler.go#L56-L85)、[配置为空不初始化](../../../services/control-api/internal/bootstrap/channel_config.go#L80-L82)、[条件装配](../../../services/control-api/internal/bootstrap/app.go#L151-L165)、[OpenAPI 条件声明](../../../api/openapi/control/v1/channel-public.yaml#L2-L6)。

## 8. 已确认的文档漂移与产品读模型缺口

| 优先级 | 现象 / 依据 | 前端当前处理 / 建议 |
| --- | --- | --- |
| P1 文档 | account 文档 PATCH 写“name/description 与非秘密 config”，版本表也列 config 编辑；真实 DTO/Schema 没有 config | 只做 name/description 编辑；config 只读。同步修正文档，不为接口添加未知字段 |
| P1 读模型 | 列表只返回基础 Account，缺 Binding/观测摘要；Binding 列表独立分页、无 account_id 过滤 | 第一版以账户详情做完整工作台，列表不宣称全量就绪；如需运营列表，后端增加有界 summary/read model，避免每行轮询和跨页假“未绑定” |
| P1 状态 | gateway_application 固定 UNKNOWN；PUBLISHED 仅 PubAck，READY 是连接观测 | 分开显示“期望配置 / 路由分发 / 连接观测”；后续若需“新消息可接纳”，增加精确版本+generation 的 Gateway 应用回执，不用组合 badge 冒充证明 |
| P1 不确定写入结果 | 含秘密的 create/update 在刷新后丢失原 Body；公开 API 无按幂等键查脱敏 Receipt | 第一版仅内存精确重试，刷新后明确待核对并重新读取；建议补 actor/tenant/operation 绑定的脱敏结果查询，不要求再次提交秘密 |
| P1 部署能力 | Channel API 有条件注册，但没有 capability endpoint | 第一版清楚展示后端未启用/未识别响应；建议补能力声明以区别模块关闭与资源 404 |
| P2 Schema | docs 4.1 声称 Create 和 CredentialUpdate 的关闭 Schema 同样覆盖 webhook_secret 256 ASCII；通用 credential-update Schema 仅 16,384 字符，path purpose 的规则实际在 Domain | 前端按 purpose 用 Domain 等价约束；文档注明 Schema+Domain 分工，或拆分 purpose 变体并补覆盖测试 |
| P2 Schema | public OpenAPI 的 config 允许空对象/两字段同现，state/reason/effective_state 为 string；Domain 与内部 Schema 更严格 | API client 显式 Provider 类型和状态映射，未知状态兜底；补强公开 response Schema 的条件/枚举 |
| P2 文档 | Binding 6.2 描述无密钥 SHA-256 Receipt 可用；真实统一 execute 使用 SignRequest/VerifyRequest（MAC） | 按真实实现统一文档；不影响前端 key/body 的精确重试规则 |
| P2 文档 | Binding 实体表写 manifest_id、updated_by；公开 wire 是 manifest_ref，Binding struct 只有 created_by 与 updated_at | 生成 DTO 以实际 OpenAPI/struct 为准；同步术语，UI 不显示不存在的“最后修改人” |
| P2 运维 | 无公开历史/分发重试/测试连接/测试消息 API | 第一版使用明确详情、手动刷新和诊断复制；相关按钮等后端契约再设计 |

对应证据：[PATCH 文档](../control-api/channel-account.md#L98-L100)、[PATCH 文档路由](../control-api/channel-account.md#L117-L122)、[真实 PATCH DTO](../../../services/control-api/internal/channelbinding/application/commands.go#L249-L253)、[webhook 文档](../control-api/channel-account.md#L133-L145)、[更新 Schema](../../../api/schemas/channel/v1/credential-update.schema.json#L4-L33)、[公开 config Schema](../../../api/openapi/control/v1/channel-public.yaml#L1648-L1659)、[公开观测 Schema](../../../api/openapi/control/v1/channel-public.yaml#L1867-L1888)、[Receipt 文档](../control-api/channelbinding.md#L172-L175)、[Binding 文档实体表](../control-api/channelbinding.md#L30-L35)。

## 9. 前端设计必须覆盖的契约验收

- 11 路由、7 种写命令 DTO 与关闭字段；公开列表正确使用 accounts/bindings 而非 items。
- OWNER/MEMBER/Restricted/身份变化，写按钮与直接请求错误一致。
- Telegram / WeCom 创建、只读物理身份、字节/字符上限、用途切换清空不适用秘密。
- 凭据 keep/replace/clear、两级 CAS、已启用账户 clear 前置停用。
- 双开关四状态，账户重新启用恢复原 Binding 的确认。
- 精确 DeploymentRevision，发布新版本不自动切换，回退旧版本仍新 CAS/generation。
- 超时原 key/body 重试、create 重放 201、旧回执后的 GET、冲突不自动覆盖。
- 空观测/多实例/STALE/连接版本不匹配，PUBLISHED 与 UNKNOWN 不被翻译成上线。
- 分页不完整时不误判未绑定；无 total/服务端过滤不装成全量搜索。
- 模块未配置与资源 404 区分；不调用内部 Gateway 凭据接口。

## 10. 本次核验方式

静态对照实际 handler、application、domain、PostgreSQL read model、公开 OpenAPI、关闭 Schema、设计文档。公开路由由 JSON 解析计数并与 handler 的 11 行注册逐一核对；没有借用旧运行报告当成本次在线验证。

已执行以下本地契约/领域/应用测试（不是 PostgreSQL 或 Provider 在线集成）：

```bash
GOCACHE=/tmp/trpc-channel-api-audit-gocache go test ./api/schemas/channel/v1 ./services/control-api/internal/channelbinding/domain ./services/control-api/internal/channelbinding/application ./api/openapi/control/v1 -count=1
```

执行目录：[`仓库根目录`](../../..)。退出码 `0`，字面输出：

```text
ok  github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1 0.660s
ok  github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain 0.337s
ok  github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application 0.965s
ok  github.com/liuzengh/trpc-agent-service/api/openapi/control/v1 1.853s
```

## 11. Telegram 预检 V1 增量：实现中 / 待联调（2026-09-06）

本节是原审计之后的开发增量，不修改前文11个公开管理操作、3个内部mTLS操作的历史依据、日期或实际测试输出。预检新增 **2条公开 + 3条私有操作**；13/6是完成集成后的协议总数，不是当前运行服务已暴露这些接口的证明。原7个账户/Binding写命令的语义保持不变。

权威入口：[跨端冻结设计](../channel-preflight-v1.md)与[冻结记录](../channel-preflight-v1.md#10-冻结记录)。Control唯一wire位于 `docs/architecture-next/control-api/telegram-preflight-v1.md` §3–9；Gateway解释位于 `docs/architecture-next/channel-gateway/telegram-preflight-v1.md`。两份后端设计当前为**跨工作树待合并**；合入本树后再将路径改成可解析的仓库相对链接。不要复制候选JSON生成第二套DTO。

| 新增操作 | 权限 / 语义 | 当前标记 |
|---|---|---|
| POST `/v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights` | unrestricted Session + ACTIVE OWNER；Idempotency-Key；已保存的停用Telegram账户；202不可变创建回执 | 已冻结，实现中/待联调 |
| GET `/v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights/{preflight_id}` | ACTIVE MEMBER读取；200任务状态；no-store；不得跨租户/账户枚举 | 已冻结，实现中/待联调 |
| POST `/internal/v1/channel-preflights:claim` | 授权mTLS工作负载领取；独立任务lease/fence | 已冻结，后端集成待验证 |
| POST `/internal/v1/channel-preflights/{preflight_id}/credentials:resolve` | 任务绑定的精确版本Bot Token用途；不借用delivery/registration许可 | 已冻结，后端集成待验证 |
| POST `/internal/v1/channel-preflights/{preflight_id}:complete` | fence/版本/期限/闭合结果Schema校验，迟到结果不能覆盖 | 已冻结，后端集成待验证 |

公开请求只引用expected_account_revision、expected_connection_revision、expected_bot_token_version；不接收临时Token或任意URL。无Binding/Deployment目标也能检查，不能先启用账户才能使用预检。Gateway出站只调用getMe/getWebhookInfo，既有启用/切流动作仍另行提交。

COMPLETED结果固定8项：`credential_configuration`、`bot_identity`、`public_origin`、`webhook_registration`、`pending_updates`、`delivery_errors`、`recovery_materials`、`delivery_verification`。前6项聚合overall，后2项固定UNKNOWN且始终展示“旧Secret不可回读”“真实消息投递未验证”；PASS只表示“配置检查通过”。public origin仅静态检查，不证明真实外网投递。旧Webhook URL仅在Adapter内部比较，不透传、不用作HTTP下一跳。

`state/outcome/freshness`不能混用。CURRENT只覆盖账户连接身份与结果TTL，claim后的`gateway_config_freshness`仍为UNCONFIRMED；metadata-only变化可保留连接检查有效并标记metadata_changed，凭据/启停ABA与TTL则按wire显示STALE/EXPIRED。

前端开发采用用户+tenant+account隔离的sessionStorage，仅保存非秘密任务/请求标识与期望版本；切换身份/退出清理。分享链接`?preflight=<id>`供授权MEMBER读取，不能授予权限或自动发起任务；GET由验证后的ID自行构造，禁止跟随任意status_url。独立[预检API源文件](../../../web/lib/channel-preflight-api.ts)、[协议测试](../../../web/lib/channel-preflight-api.test.ts)、[测试fixtures](../../../web/test/channel-preflight-fixtures.ts)及[面板测试](../../../web/components/channels/preflight-panel.test.tsx)均属于**正在开发**的源码地图，待根任务最终复核与真实联调后再改成完成。

相邻UX已落实到[账户工作台](../../../web/components/channels/account-workspace.tsx)：metadata一次保存、CAS固定编辑起点、固定目标“部署名称 · rN”；[顶栏](../../../web/components/app-shell.tsx)显示“已登录”。这些源码事实不等于新预检API已经部署。完整交互、8项含义与验收边界见[前端增量计划](channelbinding-v1-flow-plan.md#13-telegram-预检-v1实现中--待联调2026-09-06-增量)。
