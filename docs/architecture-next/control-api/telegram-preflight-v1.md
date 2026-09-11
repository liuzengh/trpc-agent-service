# Telegram 接入预检 V1：Control 任务与 wire 契约

- 日期：2026-09-07；状态：**契约已冻结；Control代码已实现并通过本地回归，协调联合验收待完成**。
- 2026-09-07 双模式补充：本分支已实现模式化预检；第 12 节覆盖原 webhook-only 的适用性与配置摘要规则，不引入产品 V2。
- 基线：`661e4a826ce8f539d8f610b3f1ef99cdbe4a4fdd`。
- Control 分支：`codex/control-telegram-preflight-v1`；本文由 Control ChannelBinding 任务维护。
- 上位产品契约由协调任务维护：`docs/architecture-next/channel-preflight-v1.md`；Gateway 实现设计由其任务维护：`docs/architecture-next/channel-gateway/telegram-preflight-v1.md`。它们在各自工作树设计，不复制正文；合并时检查仓库链接。
- 新增2公开/3私有入口、8份shared Schema、Application、独立PostgreSQL Store与0002迁移及Bootstrap维护均已落代码；真实PG/Session/mTLS测试通过。现有11个公开操作、3个运行私有操作语义保持不变；联合Telegram/Web验收另记。

## 1. 定义与完成边界

接入预检（ChannelPreflight）是对一个已保存、仍停用的 Telegram ChannelAccount 的短时、只读诊断。任务完成表示诊断执行结束，不表示账户已启用、Webhook 已接管或消息已投递。

```text
OWNER 创建预检 → Control 持久排队 → Gateway mTLS 领取
→ 独立诊断授权解析 BotToken → getMe / getWebhookInfo
→ Control 验证租约与版本并保存去秘密结果 → ACTIVE MEMBER 查看
```

预检不要求 Binding、Deployment、RuntimeManifest、运行目录 READY 或 Worker。它不修改 Account、Credential、Binding、RouteGeneration、catalog revision、Observation、注册记录、Inbox/Admission、Control 路由 Outbox 或 RunRequested。现有启用操作不新增强制预检票据。

固定禁止调用 `setWebhook`、`deleteWebhook`、`getUpdates`、`sendMessage`、远端 `close`，不临时启用、不安装运行 webhook handler、不探测外部旧 Webhook URL。回收 HTTP 连接使用本地 `CloseIdleConnections`，不是 Bot API close。

## 2. 已核对的现状与复用位置

| 现状 | 代码依据与设计影响 |
|---|---|
| 普通凭据使用拒绝 disabled | `services/control-api/internal/channelbinding/domain/credential.go` 的 `MatchCredentialUses`；原守卫保持 |
| Runtime 解析精确版本与用途 | `application/runtime.go`；预检新 Interface 不向旧 `ResolveCredentials` 加 bypass |
| Gateway 运行 Lookup 拒绝 disabled | `services/channel-gateway/internal/connection/application/catalogrefresh/refresh.go`；预检不走普通 AccountUsePermit |
| Token 只有当前密文 | `adapter/outbound/postgres/write.go` 原地 UPSERT；ID 稳定，replace/clear 增加 credential/connection/account revision；不为预检增加历史秘密 |
| 改名/描述只增 Account revision | `domain/account.go`；不使 Token/连接诊断无意义失效 |
| 凭据读取锁顺序已确定 | `adapter/outbound/postgres/credentials.go`：catalog→Tenant→Account，事务内解密、事务外使用 |
| 普通写入会投影路由与目录 | `adapter/outbound/postgres/store.go` / `application/commands.go`；新任务不调用 Aggregate.Save/project |
| public origin 在 Gateway 平台配置 | `services/channel-gateway/internal/bootstrap/control_config.go` 的 `ControlConfig.PublicOrigin`；不接受浏览器提供探测地址 |

本文依据 Telegram 的 [getMe](https://core.telegram.org/bots/api#getme) 与 [getWebhookInfo/WebhookInfo](https://core.telegram.org/bots/api#getwebhookinfo)：前者验证认证并返回机器人身份，后者返回现有 Webhook 状态。WebhookInfo 不含旧 `secret_token`；[setWebhook](https://core.telegram.org/bots/api#setwebhook) 的写入参数不等于可读取的恢复资料。预检只保留受限状态，不宣称能恢复旧 secret。

## 3. 数据与状态模型

### 3.1 固定身份

任务固定 `scope_id + source_epoch + tenant_id + account_id + provider + provider_account_id + connection_revision + bot_token credential_id/version`。创建时同时保存 `account_revision` 作为 CAS 和审计快照。

- 创建必须精确匹配三个 expected version；保存账号允许未配置 Token，随后配置检查给明确失败，未执行的网络项标记 SKIPPED。
- 执行有效性依据 connection revision、Token ID/version/configured 状态、provider identity、Tenant ACTIVE 和账号 disabled；**不要求最新 Account revision 等于创建 revision**。
- 纯改名/描述：结果仍可 CURRENT，`metadata_changed=true`；keep/重复 disable 等 NOOP 不失效。
- 启用、启用再停用、Token 或 WebhookSecret replace/clear：连接版本变化，未完成任务 STALE，防止 ABA。没有“自动读取最新 Token”。
- Binding 创建/改目标/启停不参与预检新鲜度。

请求者持续授权：claim、resolve、首次complete通过Identity/Tenant拥有方的事务查询复核requested_by仍是ACTIVE用户且为该ACTIVE租户OWNER；在claim、resolve、首次complete或维护检查发现请求者已移除、降为MEMBER或禁用时→STALE，reason=CHANNEL_PREFLIGHT_REQUESTER_REVOKED，零新解析/零首次接受结果。正常Session过期或退出不撤销已授权任务，已落STALE的任务不会因重新恢复OWNER复活；本版不新增membership generation，未承诺捕获两次检查之间撤权又恢复的所有中间状态。后台不存/不重放cookie；Session仅在公开创建和公开幂等重放时重新校验。已经完成的同claim同摘要回执重放只确认历史提交，不发新授权；当前ACTIVE MEMBER仍可读取去秘密历史结果。

### 3.2 三个独立维度

| 维度 | 值与规则 |
|---|---|
| `state` | `QUEUED`, `RUNNING`, `COMPLETED`, `TIMED_OUT`, `STALE` |
| `outcome` | `PASS`, `WARN`, `FAIL`, `UNKNOWN`；Control 从检查项计算，Gateway 不自行决定 overall |
| `freshness` | `NOT_CHECKED`, `CURRENT`, `STALE`, `EXPIRED`；GET 根据当前连接身份及服务器时间派生 |

QUEUED→RUNNING→COMPLETED；租约到期且 attempt<2、总期限未到可由下一 claim 直接 RUNNING→RUNNING，递增 lease_epoch。总期限到或第二次租约耗尽→TIMED_OUT；固定身份失效→STALE。终态不可被新 claim 复活。

网络错误也可形成 COMPLETED（outcome FAIL/UNKNOWN），不是“执行未结束”。TIMED_OUT/STALE 的 outcome=UNKNOWN，checks 保持空集合，不伪造远端事实。

已完成结果不因后来配置变更改写历史状态或检查时间；GET 派生 freshness=STALE。TTL 到期为 EXPIRED；连接已变更优先 STALE。`metadata_changed` 不影响 freshness。没有 Gateway 时 Control 自己推进超时，不依赖 Gateway 报告。

### 3.3 时间与限额（候选冻结默认）

- 任务总期限 `requested_at + 120s`；每次租约 30s；最多 2 次领取；不续租。
- Gateway 单次工作预算 20s（含诊断 resolve），每个 Telegram 请求最多 5s，留至少 5s 完成上报。
- 本地 `report_deadline = claim请求发起单调时间 + min(lease_expires_at-server_time, job_deadline_at-server_time)`；`work_deadline = min(请求起点+20s, report_deadline-5s)`。响应过慢、剩余预算不足就零解析/零Telegram调用。server_time是每次响应的当前Control数据库时间。
- 每账户最多 1 个 QUEUED/RUNNING；每账户每分钟最多 3 个新任务；每租户每分钟最多 30 个新任务、最多 20 个活跃任务；数据库锁内计数，不只依赖进程内存。
- Gateway 每实例固定 4 个执行槽，先拿槽再 claim；每次 limit 固定 1；空队列 2s±20% 抖动轮询，实例级共享轮询/速率门控，不让每个槽独立热轮询，传输失败 2–30s 退避。
- Control 每实例 claim 每秒最多 2 次；任务/results 与创建幂等记录保留 24h；claim 请求回执保留 5min；结果有效期为接受完成时 `checked_at + 5min`。
- Control 维护每 1s、有界批次推进超时并清理过期记录；GET 即使维护滞后也按服务器 deadline 派生有效 TIMED_OUT，不无限显示排队。
- 删除记录后 GET 返回 404。24h 幂等窗口到期后同 key 不保证原任务重放；客户端每次用户新检查生成新 key，不自动复用过期 key。

## 4. 公开 Interface（2 个新增操作）

全部使用现有 unrestricted Session + Tenant 权限，`Cache-Control: no-store`。POST 为 ACTIVE OWNER；GET 为 ACTIVE MEMBER。跨租户、不可见 account/preflight 返回 404；有账户读取权但非 OWNER 发起返回 403。

### 4.1 创建

`POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights`

必需 `Content-Type: application/json`、`Idempotency-Key`（沿用现有 Channel 格式和最大长度）。Body 只允许：

```json
{
  "expected_account_revision": 7,
  "expected_connection_revision": 4,
  "expected_bot_token_version": 2
}
```

三版本是 1..9007199254740991 的整数；不允许额外字段。服务端补齐实际 Tenant/scope/credential ID，用户不提交 URL、Token、用途、Gateway、Binding、Deployment。

成功 202；`Location` 为 status_url；`Retry-After: 2`。创建回执 JSON 固定，重放仍为相同 202 和相同回执，当前状态通过 GET 读取：

```json
{
  "preflight_id": "cpf_example",
  "tenant_id": "tnt_example",
  "account_id": "cha_example",
  "requested_at": "2026-09-06T04:00:00Z",
  "job_deadline_at": "2026-09-06T04:02:00Z",
  "status_url": "/v1/tenants/tnt_example/channel-accounts/cha_example/preflights/cpf_example"
}
```

事务内先重新验证当前 OWNER/Session，再命中合法同 key 同规范请求重放；合法重放先于当前 CAS、账号启停/并发任务和限流，不重复计数。相同 key 的 actor 或 body 不同返回 409 `CHANNEL_IDEMPOTENCY_CONFLICT`。新请求版本不符 409；账号 enabled 409；非 Telegram 422；同账号已有活跃任务 409；限流 429（有 Retry-After）。

### 4.2 读取

`GET /v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights/{preflight_id}`

响应 200。以下是任务完成但当前没有注册 Webhook 的示例；checks 的结构和顺序见 §7：

```json
{
  "preflight_id": "cpf_example",
  "tenant_id": "tnt_example",
  "account_id": "cha_example",
  "provider": "telegram",
  "provider_account_id": "123456789",
  "requested_by": "usr_example",
  "account_revision": 7,
  "connection_revision": 4,
  "bot_token_version": 2,
  "state": "COMPLETED",
  "outcome": "WARN",
  "reason_code": "CHANNEL_PREFLIGHT_COMPLETED",
  "freshness": "CURRENT",
  "metadata_changed": false,
  "requested_at": "2026-09-06T04:00:00Z",
  "started_at": "2026-09-06T04:00:02Z",
  "checked_at": "2026-09-06T04:00:06Z",
  "job_deadline_at": "2026-09-06T04:02:00Z",
  "expires_at": "2026-09-06T04:05:06Z",
  "gateway_config_digest": "sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d",
  "gateway_config_freshness": "UNCONFIRMED",
  "expected_public_origin": "https://gateway.example.com",
  "checks": [
    {"id":"credential_configuration","status":"PASS","code":"CREDENTIALS_CONFIGURED","details":{"bot_token_configured":true,"webhook_secret_configured":true}},
    {"id":"bot_identity","status":"PASS","code":"BOT_IDENTITY_MATCH","details":{"identity_match":true}},
    {"id":"public_origin","status":"PASS","code":"PUBLIC_ORIGIN_STATIC_VALID","details":{"validation":"STATIC_ONLY"}},
    {"id":"webhook_registration","status":"WARN","code":"WEBHOOK_NONE","details":{"presence":false,"relation":"NONE"}},
    {"id":"pending_updates","status":"PASS","code":"PENDING_UPDATES_ZERO","details":{"pending_update_count":0}},
    {"id":"delivery_errors","status":"PASS","code":"DELIVERY_ERROR_NOT_REPORTED","details":{"has_last_error":false,"last_error_at":null}},
    {"id":"recovery_materials","status":"UNKNOWN","code":"RECOVERY_MATERIALS_UNAVAILABLE","details":{"secret_token_readable":false,"restore_available":false}},
    {"id":"delivery_verification","status":"UNKNOWN","code":"DELIVERY_NOT_TESTED","details":{"verification":"NOT_TESTED"}}
  ]
}
```

字段空值：QUEUED 的 started_at/checked_at/expires_at/config digest/origin 为 null，checks=[]；RUNNING 的 started_at 非 null，checked_at/expires_at 为 null，checks=[]。TIMED_OUT/STALE 不生成 checked_at/expires_at；COMPLETED 三检查时间非 null。gateway_config_freshness在首次claim前为null，首次claim后固定UNCONFIRMED，不出现CURRENT；因此从未领取的TIMED_OUT也为null。首次claim后摘要固定，STALE仍保留历史摘要，不替换为新配置。requested_at/job_deadline_at 永不变化；started_at 是第一次领取时间，不随重领改变。

`checked_at` 是 Control 接受结果的服务器时间，Gateway 时间仅内部证据。public 不返回 credential_id、claim token、mTLS identity、原始异常、原始远端 URL、原始 last_error_message、秘密值。provider_account_id 来自用户保存的账号，不回显错误 Token 实际指向的其他机器人信息。

## 5. 私有 mTLS Interface（3 个新增操作）

独立监听器，沿用 URI SAN→WorkloadPrincipal 的可信映射和 audience=`control-channel-v1`；新增允许的 diagnostic consumer kind=`telegram_preflight`。没有该 kind 的 principal 不可领取、解析、完成。消费者名称不是新持久凭据 purpose：数据库仍只存 `telegram.bot_token`。

公共 Schema 与运行 consumer Schema 不加入 disabled bypass。预检有自己的闭合 Schema，JSON 重复键、额外字段、无界数值、尾随 JSON 拒绝。ID/epoch/version沿现有约束；令牌仅内部、请求日志必须清除。

### 5.1 Claim

`POST /internal/v1/channel-preflights:claim`，Body≤4KiB：

```json
{
  "schema_version": 1,
  "scope_id": "pool",
  "source_epoch": "11111111-1111-4111-8111-111111111111",
  "instance_epoch": "22222222-2222-4222-8222-222222222222",
  "claim_request_id": "33333333-3333-4333-8333-333333333333",
  "claim_token": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "gateway_config_digest": "sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d",
  "expected_public_origin": "https://gateway.example.com",
  "origin_status": "PUBLIC_ORIGIN_STATIC_VALID",
  "limit": 1
}
```

示例 token 是协议占位；实现必须 CSPRNG 生成 32 字节、无 padding base64url 43 字符。Control 仅存 SHA-256 摘要，Gateway 持有原 token；响应不回传 token。`instance_id/principal_id` 只来自认证上下文；instance_epoch 是每次启动的随机 UUID，不能单独扩大授权。

无任务 204，Retry-After:2；有任务 200：

```json
{
  "schema_version": 1,
  "server_time": "2026-09-06T04:00:02Z",
  "preflight_id": "cpf_example",
  "scope_id": "pool",
  "source_epoch": "11111111-1111-4111-8111-111111111111",
  "tenant_id": "tnt_example",
  "account_id": "cha_example",
  "provider": "telegram",
  "provider_account_id": "123456789",
  "account_revision": 7,
  "connection_revision": 4,
  "webhook_path": "/v1/telegram/cha_example",
  "credentials": {"purpose":"telegram.bot_token","credential_id":"ccr_example","credential_version":2,"configured":true},
  "webhook_secret_configured": true,
  "lease_epoch": 1,
  "lease_expires_at": "2026-09-06T04:00:32Z",
  "job_deadline_at": "2026-09-06T04:02:00Z",
  "gateway_config_digest": "sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d"
}
```

响应中的配置摘要必须和请求经校验的规范对象相同。Gateway请求失联只有限重传同request_id/token，收到204后按轮询间隔生成新id，不等待5min。

幂等键 `(scope, principal, instance_id, instance_epoch, claim_request_id)`；同 key 同请求重放同原 grant 或同 204 决定，不新领任务、不延长租约。相同 key 异请求/令牌摘要 409。5min 内保存非秘密回执；回执只含 task/grant 元数据。重放 grant 的 server_time 为本次服务器时间，lease 不改变；已过期未完成 grant 返回 409 `CHANNEL_PREFLIGHT_LEASE_EXPIRED`。Gateway 每次新的轮询/领取使用新 request_id/token。

Control 从符合 scope、Telegram、disabled、Tenant ACTIVE 的候选选择；不能要求 Binding 或目录 READY。同任务至多一个有效租约，每次重领 lease_epoch+1，替换 token hash/owner/instance epoch，deadline不变。

### 5.2 独立诊断 Token 解析

`POST /internal/v1/channel-preflights/{preflight_id}/credentials:resolve`，Body≤4KiB：

```json
{
  "schema_version":1,
  "scope_id":"pool",
  "source_epoch":"11111111-1111-4111-8111-111111111111",
  "instance_epoch":"22222222-2222-4222-8222-222222222222",
  "lease_epoch":1,
  "claim_token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
}
```

成功 200 是私有响应：`{schema_version, preflight_id, connection_revision, purpose:"telegram.bot_token", credential_id, credential_version, value, lease_expires_at}`。每个字段必需，value 只存在于 mTLS 响应和短时内存，不进入结果/receipt。Response no-store，最大20KiB。没有可选择的 `uses` 或 `purpose` 请求字段。

校验当前认证主体+实例、instance epoch、scope/source epoch、task/lease epoch/token hash、服务器期限、租户状态、disabled、固定连接与Token精确身份；版本不符返回409并使任务STALE。Token未配置返回409 `CHANNEL_CREDENTIAL_REQUIRED`；Gateway应在 grant metadata 阶段产生配置失败，跳过解析。

锁内验证并解密当前密文，锁外发送响应/执行网络请求。运行 `MatchCredentialUses` 与 Gateway AccountUsePermit 不改。解密错误仅返回稳定内部错误，日志不含 value/原始请求 URL。租约不能撤回已发出的只读 HTTP；完成时复核防止旧结果冒充当前配置。

### 5.3 Complete

`POST /internal/v1/channel-preflights/{preflight_id}:complete`，Body≤16KiB：

```json
{
  "schema_version":1,
  "scope_id":"pool",
  "source_epoch":"11111111-1111-4111-8111-111111111111",
  "instance_epoch":"22222222-2222-4222-8222-222222222222",
  "lease_epoch":1,
  "claim_token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
  "gateway_config_digest":"sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d",
  "expected_public_origin":"https://gateway.example.com",
  "observed_at":"2026-09-06T04:00:05Z",
  "checks":[
    {"id":"credential_configuration","status":"PASS","code":"CREDENTIALS_CONFIGURED","details":{"bot_token_configured":true,"webhook_secret_configured":true}},
    {"id":"bot_identity","status":"PASS","code":"BOT_IDENTITY_MATCH","details":{"identity_match":true}},
    {"id":"public_origin","status":"PASS","code":"PUBLIC_ORIGIN_STATIC_VALID","details":{"validation":"STATIC_ONLY"}},
    {"id":"webhook_registration","status":"WARN","code":"WEBHOOK_NONE","details":{"presence":false,"relation":"NONE"}},
    {"id":"pending_updates","status":"PASS","code":"PENDING_UPDATES_ZERO","details":{"pending_update_count":0}},
    {"id":"delivery_errors","status":"PASS","code":"DELIVERY_ERROR_NOT_REPORTED","details":{"has_last_error":false,"last_error_at":null}},
    {"id":"recovery_materials","status":"UNKNOWN","code":"RECOVERY_MATERIALS_UNAVAILABLE","details":{"secret_token_readable":false,"restore_available":false}},
    {"id":"delivery_verification","status":"UNKNOWN","code":"DELIVERY_NOT_TESTED","details":{"verification":"NOT_TESTED"}}
  ]
}
```

成功204。Control Schema+语义复核固定8项、code/status/details对应及元数据一致，然后计算 outcome 并落终态。同一有效 claim 已完成的相同 JCS payload 摘要重放204，即使重传时租约已到期；先认证/匹配原owner+token，再命中完成回执，之后才对首次完成检查期限。相同claim异payload409 `CHANNEL_PREFLIGHT_RESULT_CONFLICT`。旧lease、新owner、首次迟到、已TIMED_OUT/STALE结果409，不能覆盖当前结果或更新时间。

observed_at 为内部信息，不用于租约/TTL；不回显客户端 future clock。complete摘要包括业务结果/观测时间/config digest，排除claim token明文，控制端使用现有JCS实现；保存同规范化结果可重复发送，不为每次重试生成新的 observed_at。

## 6. 配置指纹与诊断授权

Gateway 平台配置来自运行 Bootstrap，不来自用户请求。首次 claim 的 `gateway_config_digest` 固定该任务检查上下文；重领或complete遇不同digest使任务STALE，reason=`CHANNEL_PREFLIGHT_GATEWAY_CONFIG_CHANGED`，不混合不同origin的结论。

指纹采用下列闭合对象的 JCS 字节执行 SHA-256；这是预检配置摘要，不是 Deployment PlatformExecutionContract digest。scope/source epoch 来自平台Control接入配置。public_origin标准化合法HTTPS origin，否则null；origin_status取 PUBLIC_ORIGIN_STATIC_VALID / PUBLIC_ORIGIN_INVALID / PUBLIC_ORIGIN_NOT_PUBLIC。禁止摘要字段含Token、证书/私钥路径或值、代理凭据、机器时间。

```json
{
  "schema_version": 1,
  "policy_version": "telegram-preflight-v1",
  "scope_id": "pool",
  "source_epoch": "11111111-1111-4111-8111-111111111111",
  "telegram_api_origin": "https://api.telegram.org",
  "public_origin": "https://gateway.example.com",
  "origin_status": "PUBLIC_ORIGIN_STATIC_VALID"
}
```

该示例的规范化测试向量：`sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d`。对该纯字符串/整数对象按JCS键序无空白序列化，UTF-8输入；实现用已有JCS库，不自写通用canonicalizer。合法origin规则：scheme/ASCII host小写、默认443端口省略、末尾根斜杠删除；带userinfo/query/fragment/path或非ASCII未规范主机先判无效，不进行DNS查询。IP字面量的公开性静态检查先于接受origin。两个Gateway相同有效配置必须给同digest。

claim请求同时携带expected_public_origin与origin_status；Control重新做闭合值/静态规则和摘要一致性校验后首次固定，不能只相信任意digest。complete.expected_public_origin必须等于该快照，public_origin检查code必须等于origin_status。GET仅返回合法平台origin，否则null。配置错误不作为任意URL探测输入。

Control没有最新Gateway配置注册表；已完成的public `gateway_config_freshness` 在首次成功claim前为null、首次claim后（包括RUNNING/COMPLETED/TIMED_OUT/STALE）固定为 `UNCONFIRMED`，显示检查时指纹与合法expected_public_origin。`freshness=CURRENT`只表达保存的账户连接身份与5min时间窗口，不承诺Gateway后来未改配置。页面提示“针对检查时Gateway配置”，重新预检才产生新事实；不建立配置注册中心/新心跳系统。

origin只静态解析：HTTPS、合法host、Telegram支持端口、无userinfo/query/fragment、无业务path；拒绝localhost、单标签本地名、.localhost/.local与私网/环回/链路本地/保留地址的IP字面量。只校验，不解析DNS、不发送探测请求。合法origin标准化后才进入public；不合格时expected_public_origin=null并提供稳定码，不透传原始异常值。

Bot API生产出网固定 `https://api.telegram.org`，只允许getMe/getWebhookInfo；禁重定向、每响应64KiB、固定超时。测试替身仅测试构造入口注入，不开放浏览器/平台用户选择任意API endpoint。

## 7. 检查项、稳定原因与整体结论

checks必须按以下8项顺序完整出现；每项对象只有id/status/code/details，details按id闭合；两个凭据都未配置时优先BOT_TOKEN_MISSING。status=`PASS|WARN|FAIL|UNKNOWN|SKIPPED`。

| id | 可用code / status | details白名单 |
|---|---|---|
| credential_configuration | CREDENTIALS_CONFIGURED/PASS；BOT_TOKEN_MISSING 或 WEBHOOK_SECRET_MISSING/FAIL | bot_token_configured, webhook_secret_configured（bool） |
| bot_identity | BOT_IDENTITY_MATCH/PASS；BOT_IDENTITY_MISMATCH、TOKEN_REJECTED/FAIL；PROVIDER_NETWORK、PROVIDER_TIMEOUT、PROVIDER_RATE_LIMITED、PROVIDER_UNAVAILABLE、PROVIDER_RESPONSE_INVALID/UNKNOWN；NOT_EXECUTED/SKIPPED | identity_match（bool或null） |
| public_origin | PUBLIC_ORIGIN_STATIC_VALID/PASS；PUBLIC_ORIGIN_INVALID、PUBLIC_ORIGIN_NOT_PUBLIC/FAIL | validation固定STATIC_ONLY |
| webhook_registration | WEBHOOK_MATCH/PASS；WEBHOOK_DIFFERENT、WEBHOOK_NONE/WARN；WEBHOOK_COMPARISON_UNAVAILABLE/UNKNOWN；TOKEN_REJECTED/FAIL；上列PROVIDER_* /UNKNOWN；NOT_EXECUTED/SKIPPED | presence（bool或null），relation=MATCH/DIFFERENT/NONE/UNKNOWN |
| pending_updates | PENDING_UPDATES_ZERO/PASS；PENDING_UPDATES_PRESENT/WARN；NOT_EXECUTED/SKIPPED | pending_update_count（非负安全整数或null） |
| delivery_errors | DELIVERY_ERROR_NOT_REPORTED/PASS；DELIVERY_ERROR_REPORTED/WARN；NOT_EXECUTED/SKIPPED | has_last_error（bool或null），last_error_at（UTC或null） |
| recovery_materials | RECOVERY_MATERIALS_UNAVAILABLE/UNKNOWN | secret_token_readable=false, restore_available=false |
| delivery_verification | DELIVERY_NOT_TESTED/UNKNOWN | verification固定NOT_TESTED |

Gateway先检查metadata/origin，Token已配置才resolve/getMe；只有getMe确认is_bot=true且规范十进制ID精确相同才getWebhookInfo。名称/username不用于匹配；身份不符不公开错误Token对应机器人信息。Token缺失或身份未验证时下游网络项目SKIPPED；网络失败不是Token无效。getMe成功后Token仍可能被Telegram外部撤销；getWebhookInfo此时401使用TOKEN_REJECTED/FAIL（presence=null、relation=UNKNOWN），pending_updates与delivery_errors为NOT_EXECUTED/SKIPPED。无效响应/超限/JSON损坏归PROVIDER_RESPONSE_INVALID，不保存原文。

public origin不合法不阻止已确认身份后的getWebhookInfo：旧URL为空仍WEBHOOK_NONE；旧URL存在但没有合法expected origin时presence=true、relation=UNKNOWN、code=WEBHOOK_COMPARISON_UNAVAILABLE、status=UNKNOWN。积压/历史错误仍从已读取响应独立展示，不误报DIFFERENT或Provider失败。

Webhook URL仅在Adapter内存与期望origin+账户webhook_path比较；不入DB/log/DTO、不作下一跳URL。积压>0和历史last_error仅WARN，不推断当前服务一定故障；0积压/无错误也不代表真实收信成功。getWebhookInfo无URL可说明未注册，但不自动恢复/接管。

整体outcome仅聚合前6项：任意FAIL→FAIL；否则UNKNOWN/SKIPPED→UNKNOWN；否则WARN→WARN；否则PASS。后2项固定UNKNOWN是能力边界，不强迫所有任务整体UNKNOWN，也绝不变成“上线成功”。前端必须同时显示DELIVERY_NOT_TESTED和旧secret不可回读，不只显示一个绿色PASS。

## 8. 事务、持久化与期限收敛

新增独立 PreflightStore/PreflightQuery/PreflightRuntime Interface，由Channel Application拥有；PostgreSQL Adapter落库，Bootstrap组装维护循环。复用Tenant/Identity拥有方授权，不复制其SQL到Channel模块。新增`PreflightRequesterAuthorizer`事务Interface按用户ID检查ACTIVE用户+ACTIVE租户OWNER；现有公开Session事务授权组合不直接用于后台mTLS上下文，原Session检查也不放宽。Bootstrap组合新的拥有方Adapter，所需Identity/Tenant方法由各拥有方文件实现。

统一锁序：`catalog SHARE → Preflight命名事务advisory fence → Identity/Tenant授权锁（后台含requested_by持续OWNER） → Account SHARE/UPDATE → Preflight UPDATE → request/complete receipt`。所有路径一致，先无锁挑候选，再按顺序锁定并复核；不先锁task再回头等Account。网络不在数据库事务内。

- 创建：OWNER授权锁、Account锁、幂等检查、CAS/provider/disabled检查、先将已到deadline或已失效的旧活跃任务落终态、再做活跃任务与限流检查、插task+创建回执同事务。
- Claim：无锁选候选→按锁序检查当前身份→任务条件更新+lease_epoch+token hash+owner+回执原子提交；竞争者SKIP/重试有上限。
- Resolve：同锁序验证任务/账号并解密；不更新Account或运行permit。
- Complete：先认证mTLS+原claim主体/token；保持全局锁序取得Identity/Tenant/Account/Task行锁，但拥有方授权查询返回事实而不是在这里提前拒绝requested_by撤权。先匹配已完成同claim同摘要回执并返回204；仅未命中回执的首次完成才判定requested_by持续OWNER、Tenant ACTIVE、lease/当前版本，之后严格结果验证、写结果与摘要同事务。这样撤权后确认历史提交不变成新授权，也不被通用Authorizer提前误拒。
- 失效不要求改动原账户命令的公开行为；claim/resolve/complete/GET检查当前连接版本。维护循环按同锁序将未完成过期/失效任务落终态，GET防御性派生即时有效状态。
- 截止时间与比较采用数据库服务器时间；源epoch变更时旧任务STALE并拒绝旧grant。终态事实不能因清理/重试写成新成功。

新增迁移建议 `services/control-api/migrations/0002_channel_preflights.sql`（落实协调本次“新迁移”，不修改已运行0001，不创建V2产品协议）：

1. `channel_preflights`：Tenant/Account复合FK；固定identity、requested_by、版本元数据、任务/租约字段、config digest、受限result_jsonb、complete_digest、各时间；无秘密密文/值，无Binding/Run FK。
2. `channel_preflight_requests`：独立创建幂等与claim请求回执，带kind和明确作用域/请求摘要；不扩展已有channel_command_receipts封闭operation CHECK。
3. partial unique约束每account一个QUEUED/RUNNING；state/lease/result/时间关系CHECK；候选(scope,state,deadline)、限流(tenant,account,requested_at)、cleanup索引。

令牌仅摘要；claim回执只存非秘密grant和token摘要；public结果不得混入credential_id。创建使用Tenant维度的Preflight命名事务advisory fence串行检查配额；claim使用principal/instance维度fence串行请求回执与每秒限流。两种fence都位于catalog SHARE之后、Identity/Tenant授权锁之前，所有预检路径一致，不用于网络期。创建先无锁发现同账号旧活跃任务requested_by，事务中按稳定排序锁定当前和旧请求者全部Identity后再锁Tenant/Account。当前Session/OWNER和合法幂等回执优先；只有新建才用旧请求者持续授权事实收敛旧任务并写新任务，同一事务提交。发现旧任务请求者发生竞争变化时有界重试，不在持有Account后反向锁其他Identity；提交期撤销Session的请求对预检表也零写入。

## 9. HTTP/协议错误分类

| HTTP | code与含义 |
|---|---|
| 400 | CHANNEL_INPUT_INVALID：格式/未知字段/用途/数字/重复键 |
| 401 | 沿用Session或CHANNEL_WORKLOAD_UNAUTHENTICATED |
| 403 | CHANNEL_PERMISSION_DENIED / CHANNEL_WORKLOAD_DENIED |
| 404 | CHANNEL_ACCOUNT_NOT_FOUND / CHANNEL_PREFLIGHT_NOT_FOUND |
| 409 | CHANNEL_REVISION_CONFLICT / CHANNEL_CREDENTIAL_VERSION_CONFLICT / CHANNEL_ACCOUNT_MUST_BE_DISABLED / CHANNEL_PREFLIGHT_ALREADY_RUNNING / CHANNEL_IDEMPOTENCY_CONFLICT |
| 409 | CHANNEL_PREFLIGHT_CLAIM_CONFLICT / CHANNEL_PREFLIGHT_LEASE_EXPIRED / CHANNEL_PREFLIGHT_STALE / CHANNEL_PREFLIGHT_RESULT_CONFLICT / CHANNEL_SOURCE_EPOCH_MISMATCH |
| 422 | CHANNEL_PREFLIGHT_PROVIDER_UNSUPPORTED |
| 429 | CHANNEL_PREFLIGHT_RATE_LIMITED，Retry-After |
| 503 | CHANNEL_DEPENDENCY_UNAVAILABLE；不透传PG/SDK/TLS原始错误 |

TIMED_OUT原因区分CHANNEL_PREFLIGHT_NO_EXECUTOR（从未领取）和CHANNEL_PREFLIGHT_EXECUTION_TIMEOUT（曾领取/租约耗尽）；STALE区分CHANNEL_PREFLIGHT_ACCOUNT_CHANGED、CHANNEL_PREFLIGHT_TENANT_INACTIVE、CHANNEL_PREFLIGHT_REQUESTER_REVOKED、CHANNEL_PREFLIGHT_GATEWAY_CONFIG_CHANGED、CHANNEL_SOURCE_EPOCH_MISMATCH。业务检查code见§7，不与HTTP错误混为一层。

## 10. 测试矩阵与实施顺序

| 类别 | 必须验收 |
|---|---|
| 公共授权 | OWNER创建、MEMBER只读、restricted Session拒绝、跨租户404、提交前撤销成员或Session零写入；创建后移除/降级/禁用requested_by使未完成任务STALE；正常Session退出不撤销任务 |
| 创建幂等 | 同key并发单任务；异body/actor409；响应丢失重放202原回执；合法重放不重复限流 |
| CAS/限制 | 三expected版本冲突；enabled/非Telegram拒绝；同账户活跃冲突；多Control实例配额一致 |
| 精确凭据 | disabled普通resolve仍拒绝；预检只BotToken；伪造purpose/id/其他账号/租约/instance/scope拒绝；未配置Token失败不触网 |
| 版本变化 | claim前/resolve后/complete前rotate或clear、启用ABA均STALE；WebhookSecret变化STALE；metadata-only和NOOP不失效 |
| 租约竞争 | 两Gateway仅一有效claim；同request重放；空决定重放；旧epoch/token/boot迟到409；重领最多2且总deadline不延长 |
| 完成幂等 | 响应丢失同payload204且checked_at不变；不同payload409；首次超期409；旧claim不覆盖新claim |
| 时间 | 无Gateway120s超时、Gateway崩溃、时钟漂移、未来observed_at、结果5min过期、维护滞后GET收敛 |
| Gateway配置 | origin静态校验；metadata-only不误判；claim后配置digest变更STALE；终态config current明确UNCONFIRMED |
| Provider | 正确ID/错ID、401、网络/DNS/超时、429、5xx、超大/损坏响应、不同旧Webhook、积压、历史错误 |
| 无副作用 | Account/Binding/route/catalog/credentials/Observation/Outbox/registrations/Receipt/Admission/RunRequested前后hash/行数不变（仅预检自己的表变化） |
| 脱敏 | token/原Webhook URL/last_error_message/URL userinfo和pathsecret均不进入public JSON、日志、trace、receipt、Git |
| SQL | 独立真实PG锁屏障验证启停/rotate/complete序列，无死锁；迁移从现有0001升级；回滚仅在副本 |
| 契约 | 2公开+3私有Schema和OpenAPI双向一致；固定8项、状态/细节组合、重复键和超限拒绝 |

冻结后再实施：先Schema/Domain纯状态与测试→真实PG迁移/事务→公开Session+私有mTLS→Control维护/Bootstrap→Gateway只读Adapter和Runner→协调Web真实后端→真实Telegram只读验收。旧运行接口与接纳回归必须继续通过。

用户已直接授权代码实现、测试和功能分支推送；协调任务已冻结契约。Control本地实现与回归已落地；main合并仍等待协调验收通过后的串行指令。Web和已有运行服务、数据库、Webhook不由本次实现修改。

## 11. 已冻结协议与交付门禁

1. Gateway配置指纹、固定八项结果、配额/期限、2公开/3私有路径已由协调冻结；Go DTO与8份Schema是共享唯一线协议，Gateway消费同一包，不维护私有副本。
2. Control真实PG+Session+mTLS覆盖创建→claim→resolve→complete→GET；完整结果只持久化去秘密事实。普通运行disabled门禁、路由Outbox与mTLS身份回归保持通过。
3. 重点回归包括合法配置A→首次complete合法B持久STALE、原A迟到拒绝；旧OWNER降级后新OWNER同事务收敛旧任务；当前Session在middleware后提交前撤销时预检两表与八张运行表零变化。
4. 功能分支可以提交/推送供跨端集成；必须通过协调任务的最终审查及真实环境验收之后，再按串行指令合入远端main。Control测试不宣称修改Webhook、运行就绪或真实投递。

## 12. 双接收模式的实现补充

共享 wire 的唯一当前依据为 `api/schemas/channel/v1/` 的闭合 Schema、Go 验证器及 fixtures。
本文前述原始八项规则继续用于历史 webhook-only 结果；新任务采用下列固定解释，不从
读取时的 Account 最新 mode 重算旧事实。

| 字段/行为 | 冻结规则与实现 |
|---|---|
| 任务身份 | Create 固定 receive_mode 与 diagnostic_policy=telegram-receive-modes-v1；mode 变化与 connection revision 变化都会使未完成任务失效 |
| Claim | 请求声明 diagnostic_policy；Store 候选查询按 policy 隔离，事务锁内复核；旧请求只领旧任务 |
| global 摘要 | 现有 PreflightConfigDigest 保持原算法，证明一次领取的全局 Gateway 配置 |
| effective 摘要 | PreflightEffectiveConfigDigest 固定 policy/scope/epoch/mode/connection revision/API origin；LP 将无关入站 origin 置 NOT_APPLICABLE |
| Grant / View | 已领取后具有 effective_config_digest；QUEUED 没有；LP 公开 expected_public_origin=null |
| Complete | 显式回传 mode/policy/connection_revision/effective_config_digest/origin_status，先校验 wire 再匹配固定任务与租约；不信任客户端 overall |
| 新租约 | LP 到期后重领允许 global origin 改变，但 effective 必须一致；原 claim receipt 原样保留；旧 lease 不再获得任何授权 |
| 同租约 | global 不同而 effective 相同返回 CHANNEL_PREFLIGHT_RESULT_CONFLICT，事务回滚且保留原配置；effective 不同持久 STALE |
| 持久解码 | 新私有 gateway_public_origin 仅用于重算 global 摘要；字段 omitempty 保留旧 record 的严格 round-trip；终态数据不重写 |

LP 检查矩阵：

1. credential_configuration 仅要求 BotToken；WebhookSecret configured 记录实际状态但不构成必需项。
2. bot_identity 沿用 getMe 身份比对及有限远端错误；缺 Token 则 NOT_EXECUTED。
3. public_origin 固定 NOT_APPLICABLE / PUBLIC_ORIGIN_NOT_APPLICABLE。
4. webhook_registration 无 Webhook 为 WEBHOOK_NONE / PASS；有 Webhook 为
   WEBHOOK_BLOCKS_LONG_POLLING / FAIL，relation=DIFFERENT，含义是与 LP 要求无 Webhook
   的状态相异，不是根据 origin 推测该 Webhook 属于谁。远端错误/前置未执行继续有限分类。
5. pending_updates 沿用 0 PASS、非0 WARN、未读取 SKIPPED。
6. delivery_errors 固定 NOT_APPLICABLE / DELIVERY_ERRORS_NOT_APPLICABLE。
7. recovery_materials 固定 NOT_APPLICABLE / RECOVERY_MATERIALS_NOT_APPLICABLE。
8. delivery_verification 仍 UNKNOWN / DELIVERY_NOT_TESTED。

N/A 三项的 details 精确为 `{ "applicability": "NOT_APPLICABLE" }`。仅聚合前六项中
适用项：FAIL 优先，其次 UNKNOWN/SKIPPED、WARN、PASS。任何预检代码路径都不调用
getUpdates、deleteWebhook、setWebhook，不清除外部 Webhook，也不启动接收；运行时模式
协调由 Gateway 单独拥有，预检 PASS 不成为强制启用票据。

代码验证保留 Application 的零运行写入断言、真实 PG 的旧记录/迁移与租约存储检查，
以及 Session/mTLS HTTP 的权限、CAS、幂等、凭据精确使用和历史重放。联合 Gateway/Web
运行证据由协调验收另记，不用 Control 的合成检查结果代替真实 Telegram 状态。
