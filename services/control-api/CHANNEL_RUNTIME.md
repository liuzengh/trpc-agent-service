# Control Channel V1 runtime

当前已实现账户/绑定的公开管理、完整快照、按用途凭据供应、运行观测、原子Route Outbox和
JetStream Relay。这里的运行配置由平台维护，不是用户必须管理的Environment/Secret对象。

## 1. 启动入口

常规Control配置（数据库、Profile加密key、发布contract digest、Session和bootstrap）继续适用。
额外设置`CONTROL_CHANNEL_CONFIG_FILE`为绝对JSON文件路径。不设置时不注册Channel路由；
提供不完整配置、错误key/cert、错误source epoch或启动时NATS连接失败时，进程退出。

```json
{
  "scope_id": "gateway_pool",
  "source_epoch": "00000000-0000-4000-8000-000000000001",
  "internal_address": "127.0.0.1:18081",
  "tls_cert_file": "/run/channel/control.crt",
  "tls_key_file": "/run/channel/control.key",
  "client_ca_file": "/run/channel/client-ca.crt",
  "credential_keys_file": "/run/channel/credential-keys.json",
  "route_nats_file": "/run/channel/route-nats.json",
  "max_tenant_accounts": 50,
  "workloads": [{
    "principal_id": "spiffe://trpc-agent-service/gateway/gw-1",
    "instance_id": "gw-1",
    "scope_id": "gateway_pool",
    "audience": "control-channel-v1",
    "consumers": ["telegram_receiver", "telegram_registration", "telegram_webhook", "telegram_delivery"]
  }]
}
```

这些是路径示例，不是可用凭据。证书私钥、下面两份key/Producer文件要求owner-only(0600)，
所有JSON拒绝重复key/未知字段/多文档，单文件上限64KiB。内部TLS至少1.3，要求验证客户端链、
ClientAuth EKU和唯一精确URI SAN，再映射平台允许的instance/scope/audience/consumer集合。
Header、Cookie、请求体声明不能扩大该身份。Workload映射上限32个。

credential-keys.json的结构：

```json
{
  "active_key_id": "platform-k1",
  "keys": {
    "platform-k1": {
      "encryption_key": "<independent random 32-byte key in base64>",
      "mac_key": "<independent random 32-byte key in base64>"
    }
  }
}
```

保持旧key可读以解密已有账户并校验原Idempotency Receipt；keyring最多8项，
活动key用于新写入。凭据替换更新模块私有当前值及credential/connection/account版本，
不是建立用户Secret产品。Profile凭据使用自己的模块和key，不复用Channel凭据。

route-nats.json的结构：

```json
{
  "url": "tls://broker.internal:4222",
  "user": "control",
  "password": "<platform producer password>",
  "ca_file": "/run/channel/nats-ca.crt"
}
```

本地隔离联调允许`nats://127.0.0.1:PORT`；非loopback地址要求TLS。URL不接受内嵌认证/query。
Producer仅publish `control.channel-route.v1`、subscribe `_INBOX.control.>`；客户端固定
`CustomInboxPrefix("_INBOX.control")`。不为Control授予JetStream管理/读消息权限。
流由部署工具预建，不由业务启动自动创建：`CHANNEL_ROUTES_V1`、FileStorage、LimitsPolicy、
DiscardNew、MaxBytes64MiB、MaxMsgSize16384、replicas按部署配置、无自动时间/条数淘汰，
DenyDelete/DenyPurge。建议dedup窗口2分钟，业务永久幂等仍由Gateway EventID/generation保证。

## 2. API和执行边界

公开12个操作见[OpenAPI](../../api/openapi/control/v1/channel-public.yaml)：OWNER写、成员读，
写入要求真实非restricted Session并在提交事务下重查Identity/租户授权。
创建默认disabled；保存凭据不触发Provider探测。Binding stable与可选canary均选择精确
deployment_id/revision_number，读取拥有方校验的Published Revision/Manifest，不读取
Draft/latest。灰度命令发布0–10000基点与最多100个显式sender ID；0基点停止之后的canary接纳。

内部监听保留以下三个运行工作负载API（均no-store）；独立预检API见§5：

| Method | Path | Wire schema / limit |
| --- | --- | --- |
| GET | `/internal/v1/channel-accounts/snapshot` | account-snapshot.schema.json；完整目录≤1000账户、≤2MiB |
| POST | `/internal/v1/tenants/{tenant_id}/channel-accounts/{account_id}/credentials:resolve` | request≤16KiB、response≤64KiB；闭合schema、同连接版本、限定用途 |
| POST | `/internal/v1/channel-account-observations` | observations.schema.json；≤100条/128KiB；成功204 |

所有请求拒绝query；snapshot拒绝body。内部HTTP执行预算5秒。凭据只在已授权的resolve成功
响应中返回，不进入快照、普通详情、Receipt、Outbox或日志。registration必须同时解析同连接
版本的bot_token和webhook_secret；Gateway在取值前后自行核验其Grant/Fence，Control不查
Gateway内部lease表。observations属于诊断而非运行授权，收到同seq重试不会刷新freshness。

账户目录/route可异步到达；Gateway核验账户许可、route generation≥min_route_generation，
在Admission事务内固定真实Control发布的目标三元组。这里不新增Manifest正文下载API，也不
宣称Worker已经可以执行。新消息才触发RunRequested，启用Binding本身不创建Run。

## 3. Relay及运维恢复

Route Relay只领取本scope、本模块event_type，15秒租约+随机claim token+attempt fencing；
5秒发布预算，收到正确stream/非零sequence的PubAck后才标记PUBLISHED。临时失败指数重试
1～60秒并保留正文；正文完整性失败进入FAILED。不要改写不可变payload/digest/event_id。
调查并修复后，运维可以在受控数据库维护中将合法FAILED事件的投递元数据重新置PENDING，
清除claimed_by/claimed_until/last_error并设置available_at，仍使用原ID/正文；损坏正文先
恢复可信数据再重发。V1没有自动删除历史Outbox、管理重试API或全局调度服务。

public/internal任一监听失败将关闭另一监听；关闭过程取消Relay/清理任务，再关闭NATS/DB。
未获得Gateway应用ACK协议之前，普通查询`gateway_application`始终UNKNOWN，不能根据
PUBLISHED推断机器人连接就绪。observations提供带版本和服务器时间的独立诊断。

## 4. 验证与发布

```sh
go test -count=1 -race ./services/control-api/... ./api/...
go vet ./services/control-api/... ./api/...
go build ./services/control-api/cmd/control-api
```

真实PG测试要求`CONTROL_TEST_DATABASE_URL`，每个测试创建/清理独立schema；缺失时相关测试skip。
真实JetStream测试另外要求`CONTROL_TEST_NATS_CONFIG_FILE`：一份0600 JSON，包含url、
admin_user/admin_password、control_user/control_password。它必须指向专用可清理测试broker，
测试会创建/删除`CHANNEL_ROUTES_V1`；不能使用联合联调或生产broker。验证报告必须记录实际skip数。

已部署Channel基线之后，Telegram预检使用新增`0002_channel_preflights.sql`，不改写
`0001_baseline.sql`。正常启动迁移只增加两张诊断表及索引，保留已有账户/凭据/路由数据。
SQL文件编号不表示产品V2。旧镜像不认识`telegram_preflight` consumer，回滚必须同步恢复
旧镜像与旧Channel配置；保留新诊断表，不通过删表回滚历史业务数据。源代码回滚在副本执行。
真实Telegram验收另需真正机器人凭据、可达HTTPS origin、远端注册与用户消息，以及Gateway
Receipt/RunRequested的持久证据；合成远端或普通go test成功不代替这一步。


## 5. Telegram 只读预检

Control 已实现独立 `PreflightService`、PostgreSQL Store、2公开/3私有入口与维护循环。
Gateway Provider 调用和跨端真实验收由各自任务交付；本节不以Control测试代替线上诊断。
冻结协议见[Telegram预检V1](../../docs/architecture-next/control-api/telegram-preflight-v1.md)，
公开和私有文档分别为[Session OpenAPI](../../api/openapi/control/v1/preflight-public.yaml)与
[mTLS OpenAPI](../../api/openapi/control/v1/preflight-internal.yaml)。

| Method | Path | 语义 |
| --- | --- | --- |
| POST | `/v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights` | ACTIVE OWNER + 非restricted Session + Idempotency-Key；202固定创建回执 |
| GET | `/v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights/{preflight_id}` | ACTIVE MEMBER；去秘密结果，无列表/latest |
| POST | `/internal/v1/channel-preflights:claim` | mTLS + 显式telegram_preflight；200一项grant或204 |
| POST | `/internal/v1/channel-preflights/{preflight_id}/credentials:resolve` | 只读取该任务固定BotToken；不读取WebhookSecret |
| POST | `/internal/v1/channel-preflights/{preflight_id}:complete` | 8项闭合事实；相同claim/payload历史重放204 |

要让Gateway领取预检，平台在其已有workload `consumers`数组中显式增加
`telegram_preflight`。不新增用户业务对象或新的环境变量；未增加该consumer的已有运行权限
保持原样，预检私有操作返回403。普通resolve仍拒绝disabled账户，不复用预检授权绕过它。

创建只要求已保存、停用的Telegram账户，不依赖Binding、Deployment或目录READY。
任务120秒、每租约30秒、最多2次领取；数据库时间控制所有期限。每账户1活跃/3次每分钟，
每租户20活跃/30次每分钟，Control按principal/instance每秒2次claim，在数据库事务中执行。
Webhook任务的有效配置摘要第一次领取后固定；重新领取或首次完成遇另一有效配置持久STALE。
双模式任务另存global/effective摘要：long_polling忽略无关入站origin；仅新租约可刷新
global证据，同一租约的不同global证据返回RESULT_CONFLICT，不悄悄覆盖原领取配置。
当前Gateway配置新鲜度保持UNCONFIRMED，不将静态origin合法当作公网可达或真实投递。

创建事务先稳定排序锁当前及旧任务请求者Identity，再锁Tenant/Account/Task；当前Session
与OWNER检查、幂等回执先于旧任务收敛。撤销Session后拒绝的请求对预检表也零写入。
后台claim/resolve/首次complete检查持久ACTIVE用户与OWNER，不依赖原Session仍登录。
历史完成回执只确认已提交事实；撤权后原claim/同payload可以204，但不重新读取凭据。

Bootstrap在已有关闭可等待的维护goroutine中每秒处理最多64个任务，每轮5秒context；
按数据库时钟收敛超时/失效并有界清理。GET也收敛未领取120秒任务，不依赖维护及时运行。
完成事实不可变，结果有效期5分钟；task/创建回执保留24小时，claim回执保留5分钟。
每分钟原Observation清理继续执行；不增加独立调度服务或消息队列。

真实PG+Session+mTLS回归覆盖完整链路、配置变更STALE、请求者撤权、创建幂等与Session
提交前撤销零写入；八张运行表完整内容哈希保持不变。Provider真实响应、Gateway账本和
用户看到的Web结果属于协调联合验收，不由Control写入或模拟。

## 6. Telegram 双接收模式（Control 代码切片）

本切片沿用现有账户与预检端点，不新增切换/接管 API；Gateway 的 Receiver、Cursor、
Telegram 协议调用与 Web 交互分别由对应任务实现。

- 新 Telegram create 的 `config.receive_mode` 默认 `long_polling`；显式 `webhook`
  要求 BotToken 和 WebhookSecret。两种模式都保留生成的 webhook_path 和两条稳定凭据
  元数据；LP 未配置 Secret 时 version=1、configured=false、无 key/ciphertext。
- PATCH 使用 expected_account_revision；只有 disabled 账户能实际换 mode。一次换模
  account/connection/catalog revision 各推进一次，同 mode 为 NOOP；元数据与 mode 同改
  不重复推进。保存不调用 Telegram，不改 Binding 目标；enable 仍按所存 mode 检查凭据。
- `0003_telegram_receive_modes.sql` 在升级事务内把历史 Telegram 明确回填 webhook，
  同步 account/connection/catalog 水位。Telegram 物理 Bot ID 跨 scope/tenant 唯一，
  disabled 也不例外。发现重复时整个迁移失败，不选择胜者。凭据密文、命令回执、路由、
  Binding、Outbox 保持原样；旧 Observation 的 mode 回填但原摘要不改。
- 升级后旧二进制的 config decoder 不认识新字段，不能只换回旧镜像继续使用双模式数据库；
  发布/回退由协调任务联合安排。本地源代码回滚仅在副本验证，不在活跃数据库做降级。
- 新 `telegram_receiver` workload consumer 对两种 mode 只解析 BotToken，要求
  owner_epoch，禁止 registration_epoch。Control 检查身份、用途和版本；Gateway 再检查
  owner 的实际有效性。registration/webhook consumer 仅用于 webhook；delivery 两种皆可。
- 新 Telegram Observation 显式携带 mode；LP READY 要求 owner_epoch。旧格式缺 mode
  只按 legacy webhook 解释，不根据当前默认值推断；错误 mode/version 报告不覆盖现有状态。
- 旧页面 pending create 明确使用 `X-Channel-Create-Contract: webhook-v1` 和原 body/key，
  body 不含 config；已落库则重放原回执，未落库则仍创建 webhook。无此 header 的新请求
  缺省 mode 为 LP。成功 mutation 的 `X-Channel-Result-Contract` 指示历史 webhook-v1
  或当前 receive-modes-v1 响应形状，历史结果不按当前账户 mode 改写。

新预检固定 mode 与 diagnostic_policy=telegram-receive-modes-v1；只有声明同 policy 的
claim 能领取。grants/claimed views/completions 携带 effective_config_digest；QUEUED
不携带。LP 第3/6/7项 NOT_APPLICABLE，第4项无 Webhook 为 PASS，有 Webhook 为 FAIL；
第8项仍 DELIVERY_NOT_TESTED。公开 LP view 的 expected_public_origin=null，私有记录
仅为验证 global 摘要保留去秘密 origin 证据。旧任务/旧结果保持原 webhook-only 解码。

测试入口：application 的 receive_modes/preflight_receive_modes 测试、PostgreSQL 的
receive_modes_integration_test.go，以及真实 Session/mTLS 的 integration 测试。
测试覆盖迁移回填、重复 Bot 原子失败、旧回执恢复、缺可选 Secret、mode CAS、consumer
精确取值、owner Observation、policy 隔离、LP 重领与同租约冲突、终态幂等及运行表零写入。


## 7. WeCom 显式连接预检

同一组 2 公开 / 3 私有端点支持已保存且停用的 WeCom 账户。部署先应用增量
`0004_wecom_preflights.sql`，将完成检查数约束区分为 Telegram 8 项 / WeCom 3 项，
0001–0003 原字节保持不变，不新建表。OWNER 显式提交
`expected_account_revision`、`expected_connection_revision`、`expected_bot_secret_version`
和 `allow_connection_probe:true`。缺省/false 确认位返回
`422 CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED`，不创建任务或回执。

WeCom `subscribe` 可能替换同 Bot 的其他客户端，不称为只读检查；Gateway 完成短时认证后
断开、丢弃收到的业务帧，不启用账户、不发消息或创建 Run。结果只有配置、连接认证、
未测试投递三项；总 PASS 不等于消息或 Agent 回复成功。

在现有 Control workload 的 `consumers` 配置中显式追加 `wecom_preflight` 才允许领取
`diagnostic_policy=wecom_long_connection_v1`；Telegram 权限不因此扩大。Resolve/Complete
还会根据持久任务 Provider 检查授权，并固定 `wecom.bot_secret` 的 ID/版本。
配置示例、完整 DTO/检查矩阵和回退边界见
[WeCom 接入预检](../../docs/architecture-next/control-api/wecom-preflight-v1.md)。

所有 Provider 共用原有每 principal/instance 每秒 2 次 claim 限流；不按 Provider 新增
独立配额。普通 `wecom_connection` 与诊断 consumer 保持分离。此源码变更不修改现有
运行配置；两个诊断 runner 的开关与有界连接实现由 Gateway 部署文档说明。

### 7.1 已部署环境补齐预检授权

在 `CONTROL_CHANNEL_CONFIG_FILE` 指向的 JSON 中，找到与 Gateway mTLS 身份匹配的
`workloads` 项，在现有 `consumers` 数组中追加 `wecom_preflight`，不要替换或扩展其他
workload 的权限。`wecom_connection` 只授权普通连接，不能替代诊断 consumer。

该文件在启动时读取；修改挂载文件后重启 Control 才会生效，单独这个配置变更无需重建
镜像。先保存原文件，核对 JSON 与差异，并避开管理面正在保存或发布的操作窗口。
Gateway 在 `GATEWAY_ACCOUNT_SOURCE=control` 下默认开启企业微信诊断 runner，可显式
配置 `GATEWAY_WECOM_PREFLIGHT_ENABLED=true`；环境变量变更需通过容器重新创建生效。

验证应分别记录：服务启动、真实预检 COMPLETED 与检查结果、实际消息及回复。
合成凭据返回 `WECOM_AUTH_REJECTED` 可证明执行和结果回传，不证明真实凭据认证成功。
回退授权配置前先停止新诊断并等待在途租约结束；不要删除任务历史。配置回退不会恢复
企业微信被替换的外部连接。相关实际验收见
[2026-09-09 企业微信验收记录](../../docs/architecture-next/channel-gateway/wecom-live-acceptance-20260909.md)。
