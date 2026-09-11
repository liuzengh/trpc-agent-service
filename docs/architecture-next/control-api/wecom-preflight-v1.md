# WeCom 接入预检：Control 开发契约

状态：Control / Schema / OpenAPI 代码切片；Gateway 与 Web 分别接入。
真实企微认证、消息入站与 Agent 回复需要各自的独立验收记录。本设计延续现有 V1，
不引入产品 V2，也不增加业务对象或新端点。

## 1. 与 Telegram 的关键区别

Telegram 预检继续调用只读 Provider API。WeCom 没有等价的只读凭据检查；
`subscribe` 是真实连接认证，可能替换同一个 Bot 的其他客户端。用户须明确确认该影响。
认证 ACK 仅说明本次 BotID / Bot Secret 组合通过订阅认证，不构成 `getMe` 身份比对、
持续在线、消息投递或 Agent 回复证明。

Gateway 的独立 WeCom 诊断 runner 只做有界连接探测，结束时断开；诊断期间收到的
业务帧不进入 Admission、Run 或 Outbox。预检不启用账户、不配置 Binding、不发业务消息，
不自动重连。平台仍需保证 runner 的调用和关闭预算严格小于领取租约。

## 2. 公开 DTO 与权限

仍使用两个现有 Session 端点：

- `POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights`
- `GET /v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights/{preflight_id}`

WeCom 创建输入：

```json
{
  "expected_account_revision": 7,
  "expected_connection_revision": 4,
  "expected_bot_secret_version": 2,
  "allow_connection_probe": true
}
```

POST 要求 ACTIVE OWNER、有效的非受限 Session、账户停用、必需的 `Idempotency-Key`。
在同一事务内核对 Account / Connection / `wecom.bot_secret` 三个固定版本。
WeCom 缺省或 false 的确认位返回 `422 CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED`；
此拒绝不创建任务或回执。权限、停用和版本冲突按现有顺序先检查，不泄露不可见账户。

Telegram 仍使用 `expected_bot_token_version`，两种凭据版本字段互斥且都大于零。
Telegram 传 `allow_connection_probe:true` 返回 `400 CHANNEL_INPUT_INVALID`。
确认位属于创建命令幂等摘要：相同 key / 相同命令返回原创建回执；变更确认位或版本返回
原有幂等冲突。`false` 与缺省的确认位都表示未同意。

WeCom 结果视图包含：

```json
{
  "provider": "wecom",
  "receive_mode": "long_connection",
  "diagnostic_policy": "wecom_long_connection_v1",
  "bot_secret_version": 2,
  "allow_connection_probe": true,
  "expected_public_origin": null
}
```

完整视图见闭合 Schema。WeCom 不输出 `bot_token_version`；Telegram 不增加 Secret 版本或
确认字段。`long_connection` 是预检的 Provider 派生协议值，不修改既有 WeCom
`ChannelAccount.config`，也不提供用户可编辑的连接 endpoint。

GET 继续要求 ACTIVE MEMBER，返回去秘密的固定诊断事实与派生 freshness。
Web 仅在 `QUEUED` / `RUNNING` 继续轮询；`COMPLETED` / `STALE` / `TIMED_OUT` 终止。
`COMPLETED` 仅代表诊断执行结束，必须结合 outcome 与 checks 展示，不等于成功。

## 3. 内部领取、凭据与 Provider 隔离

现有三个 mTLS 端点保持路径不变：claim、credentials:resolve、:complete。

| Claim diagnostic_policy | 允许领取的任务 | 必需 workload consumer |
| --- | --- | --- |
| 缺省 | 历史 Telegram webhook-only | `telegram_preflight` |
| `telegram-receive-modes-v1` | Telegram 双模式 | `telegram_preflight` |
| `wecom_long_connection_v1` | WeCom 短连接诊断 | `wecom_preflight` |

Policy 精确匹配就是 Provider 能力协商。旧 runner 不隐式领取新任务，不增加候选 Provider
列表或任意名称猜测。Control 在 Claim 与实际任务持久状态上都检查匹配，Resolve / Complete
再次按已保存 Provider 检查当前 workload consumer，随后检查原 principal / instance /
instance_epoch / lease_epoch / claim_token。持有 Telegram 预检权限不获得 WeCom 凭据。

WeCom Grant 的 `allow_connection_probe` 固定为 true；`credentials` 只包含
`purpose=wecom.bot_secret` 与固定 CredentialID / Version / configured。
为保持 Grant DTO 兼容，`webhook_path=""`、`webhook_secret_configured=false`。
Resolve 仅在有效授权下返回这一个版本的 Bot Secret；不返回 Telegram Token、WebhookSecret
或其他资源。真实值只进入 no-store 的 mTLS Resolve 响应，不写入任务、结果、日志或事件。

部署侧在现有 Control workload 配置中显式追加诊断权限，例如：

```json
{
  "consumers": [
    "wecom_connection",
    "telegram_receiver",
    "telegram_webhook",
    "telegram_delivery",
    "telegram_preflight",
    "wecom_preflight"
  ]
}
```

这是配置片段；仅保留该 workload 实际承担的运行权限。两个预检 consumer 都不用于普通
运行时 Resolve，不绕过普通运行账户的 enabled 检查。源码支持并不自动修改现有运行配置。

## 4. 固定检查项与分类

WeCom `checks` 严格按以下三项排序；每项 details 都闭合，禁止 raw Provider error、URL、
凭据、ACK payload 或任意说明字符串。

| Check | Status / Code | Details |
| --- | --- | --- |
| `credential_configuration` | `PASS / CREDENTIALS_CONFIGURED` | `{"bot_secret_configured":true}` |
| 同上 | `FAIL / BOT_SECRET_MISSING` | `{"bot_secret_configured":false}` |
| `connection_authentication` | `PASS / WECOM_AUTHENTICATED` | `{"authenticated":true}` |
| 同上 | `FAIL / WECOM_AUTH_REJECTED` | `{"authenticated":false}` |
| 同上 | `SKIPPED / NOT_EXECUTED` | `{"authenticated":null}`，只允许缺 Secret 时 |
| 同上 | `UNKNOWN / PROVIDER_NETWORK` | `{"authenticated":null}` |
| 同上 | `UNKNOWN / PROVIDER_TIMEOUT` | `{"authenticated":null}` |
| 同上 | `UNKNOWN / PROVIDER_RESPONSE_INVALID` | `{"authenticated":null}` |
| 同上 | `UNKNOWN / PROVIDER_UNAVAILABLE` | `{"authenticated":null}` |
| 同上 | `UNKNOWN / WECOM_CONNECTION_REPLACED` | `{"authenticated":null}` |
| `delivery_verification` | 固定 `UNKNOWN / DELIVERY_NOT_TESTED` | `{"verification":"NOT_TESTED"}` |

总 outcome 只聚合前两项：任何 FAIL 得到 FAIL，认证 PASS 得到 PASS，其余为 UNKNOWN。
第三项始终明确表示未验证投递。ACK 超时不是凭据明确无效；明确拒绝才归为 AUTH_REJECTED。
固定常量、有效/无效 fixture 与依赖矩阵由 `api/schemas/channel/v1/preflight_wecom*` 维护。

## 5. 摘要与授权期限

WeCom Claim / Complete 固定 `expected_public_origin=null`、
`origin_status=PUBLIC_ORIGIN_NOT_APPLICABLE`。
`PreflightConfigDigestForPolicy` 将 scope、source_epoch、policy 与固定官方 WebSocket
endpoint `wss://openws.work.weixin.qq.com` 编入 JCS / SHA-256；effective digest 额外固定
mode 与 ConnectionRevision。Telegram 公网 origin 的变化不影响 WeCom 摘要。
现有 Telegram `PreflightConfigDigest` 字节与测试向量保持不变。

保留 120 秒任务 deadline、30 秒租约、最多两次领取、创建回执 24 小时、claim 回执与完成
freshness 5 分钟。数据库时间决定期限；客户端 observed_at 只是证据，不延长授权。
相同 claim 的重放不分配新租约；租约仍有效时其他领取得不到任务；旧 lease 的 Resolve /
首次 Complete 在重新分配后被拒绝。新的执行必须再次解析当前固定凭据，不能复用旧值。

配额仍为每账户 1 活跃 / 每分钟 3 次、每租户 20 活跃 / 每分钟 30 次；所有 Provider
共享 principal / instance 每秒 2 次 Claim，不为双 runner 放宽。Gateway 应共用领取限速器。
请求者撤权、租户停用、账户启用或连接/凭据版本变化会收敛任务为 STALE 并阻止新的 Resolve。
单纯名称/说明变更继续使用原 `metadata_changed` 语义，不伪造新的连接版本。

## 6. 存储、兼容与验收边界

复用 `channel_preflights` / `channel_preflight_requests` 及现有事务锁顺序。新增
`0004_wecom_preflights.sql`，只将原完成结果数量约束改为按 Provider 检查 Telegram 8 项 /
显式同意的 WeCom 3 项；旧 SQL 文件保持原字节，不新增表、队列、调度服务或 Secret 产品。记录在 JSON 中固定 Provider、Secret 版本与确认位；
Store 对任务、Grant 回执和不可变转换执行一致性校验。历史 Telegram 记录仍按原 policy 读取。

源码回退应先停止新诊断写入并排空两种 runner 的在途租约；旧版本不认识新 WeCom 记录。
代码回退不等于恢复外部被替换的连接，不自动删除任务或篡改历史记录。测试回滚只作用于
一次性源码副本，不作用于运行中的数据库、工作树或机器人连接。

Control 验收覆盖 consent / 三版本 CAS、OWNER/disabled、跨 Provider 权限、并发单领取、
撤销/租约/重试、凭据最小解析、闭合诊断矩阵、幂等回执以及运行表零写入。
真实 Provider 认证、真实 WeCom 入站、Deployment/Worker/回复链路分别记录，不能相互替代。
