# ChannelAccount V1：租户机器人接入与 Gateway 配置供应

- **设计状态**：2026-09-06 联合评审修订已落地；本文记录当前协议与实现边界。
- **实现状态**：账户管理、真实 PostgreSQL 目录/凭据、公开 HTTP、mTLS 内部 HTTP、Bootstrap 已实现并通过真实 PostgreSQL/mTLS 回归；真实 Telegram 收信到持久RunRequested已通过联合验收。
- **代码基线**：历史协调基线为 Control `79e5795` / Gateway `bf10776`；本次集成实现分别为 `239e8ad` / `9190241`。
- **拥有方**：Control API 的 `channelbinding` Module；两类聚合分文件，不新建 Workload。
- **关联**：[ChannelBinding](channelbinding.md)、[架构约束](../constraints.md)、
  [模块术语](../../../services/control-api/internal/channelbinding/CONTEXT.md)。

## 1. 目标、现状与完成边界

用户在平台添加自己申请的 Telegram / 企业微信机器人，直接填写凭据，随后选择已发布部署。
新增机器人是新增租户业务记录，不是增加环境变量、修改运维文件或重启 Gateway。

当前 Control 已接通关闭 Schema、账户/绑定 Domain、管理命令与查询 Application、
模块私有 AES-GCM/HMAC 凭据、真实 PostgreSQL 事务、公开 Session API 和独立 mTLS 内部监听。
平台显式配置 `CONTROL_CHANNEL_CONFIG_FILE` 后注册本切片；缺少配置时不注册 Channel 路由，
部分配置或目录 epoch 不一致使启动失败。凭据、证书与受限 NATS Producer 配置不写入业务对象。

Gateway 独立任务已接入 ControlSource、可信 Credential Bridge、动态 Telegram 注册与观测；
其代码及运行证据由 Gateway 工作树维护。Control/Gateway 的合成远端纵切已经通过，真实
Telegram 收信已通过本次联合消息验证，详见[验收记录](channel-acceptance.md)。Gateway 运维探针不是账户管理入口。

本设计闭环是“保存账户 → Gateway 应用配置 → 取得授权凭据 → 接入机器人”。
“绑定目标 → 路由投影 → 新消息接纳”由配套 Binding 文档定义。实际 Worker 执行不包含在本切片。
一个页面可以组合这些操作，后端不把账户、凭据与路由混为一个消息协议。

## 2. 所有权与实体

V1 在 `channelbinding` Module 中实现 ChannelAccount、私有 AccountCredential、
ChannelBinding 和 AccountRouteState；它们有独立类型与用例，共用模块事务边界。
账户启停需要原子改变有效路由，因此本阶段不拆出跨进程账户服务或账户微服务。

| 对象 | 身份与主要字段 | 规则 |
| --- | --- | --- |
| ChannelAccount | tenant_id、account_id、provider、provider_account_id、name、description、enabled | account_id 服务端生成，全平台唯一且终身不复用；租户和渠道身份创建后不可改 |
| 版本 | account_revision、connection_revision、min_route_generation | 前两者分别为管理CAS和连接版本；后者是新Admission必须达到的路由下限 |
| AccountCredential | tenant_id、account_id、purpose、credential_id、credential_version、configured、加密值 | 模块私有；内部 ID 非授权凭证，不向普通账户详情返回 |
| AccountCatalog | scope_id、source_epoch、snapshot_revision | 平台执行池目录的同步水位，不是用户管理的 Environment |
| AccountObservation | instance_id、instance_epoch、report_sequence、连接版本、状态、observed_at | 非权威运行观测；不决定账户、租户或凭据授权 |

账户凭据不放进 Runtime Profile。Profile 继续拥有模型、工具、Storage 执行凭据；
账户模块拥有 Gateway 的机器人凭据。可以复用不带领域规则的加密技术能力和平台密钥配置，
禁止导入 Profile Repository、复用其 CredentialID 或绕过 Profile Owner 修改其表。

### 2.1 Provider 的关闭配置

| provider | provider_account_id | config | 必需凭据 purpose |
| --- | --- | --- | --- |
| telegram | 机器人稳定的数字 ID 字符串；不是用户名 | 只读 webhook_path，由服务端生成 `/v1/telegram/{account_id}` | telegram.bot_token、telegram.webhook_secret |
| wecom | SDK 使用的 Bot ID | bot_id，必须等于 provider_account_id | wecom.bot_secret |

渠道 endpoint、Telegram webhook 的公共 origin、SDK 版本与网络目的范围由平台配置固定，
用户不提交任意 URL、文件路径、环境变量名或 SDK Option。Telegram Bot Token 与 webhook
校验 Secret 是两个 purpose，不能互相替代。V1 不接受第三种 provider 或开放 config Blob。

V1 的 config 为只读派生配置：Telegram 的 webhook_path 由 account_id 生成，WeCom 的
bot_id 等于创建时的 provider_account_id。公开 Create 和 PATCH 都不接受 config；首版前端
显示真实只读值，不展示可保存的连接 URL、Webhook origin 或 SDK 选项编辑框。
PATCH 只接受 expected_account_revision、name、description，后两者至少提交一项；未知字段
由关闭 Schema 拒绝。连接版本的变化来自凭据替换/清除和账户启停，不存在独立 config 编辑用例。

服务端 ID 使用 `cha_` / `chb_` / `ccr_` 加随机不透明后缀，符合
`[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`。name 去首尾空白后为 1..128 个 Unicode code point，
description 最多 4096；provider_account_id 最多 1024 字节且拒绝空白/控制字符，Telegram
另外要求非零十进制数字且规范化去掉多余前导零。bot_id 不进行模糊名称匹配。

平台配置的 provider_scope 与 provider、规范化物理 ID 形成登记唯一键，跨租户也不得存在
两个可用逻辑映射。V1 不支持身份转移、改 Bot ID、删除后复用或跨租户迁移；停用保留身份。
重复登记对调用方只返回 CHANNEL_ACCOUNT_IDENTITY_CONFLICT，不暴露另一租户信息。
这是重复接入保护，不是已完成的机器人所有权证明。保存不在线试连；Gateway 必须在实际
SDK 认证成功并核对可得到的远端身份后才标 READY，认证失败不能接纳消息。
不存在可验证身份回执的协议分支必须写明限制，不得把用户自报 ID 当作第三方所有权证明。
物理账户争议、登记恢复及身份转移是人工运维处理事项，不在本切片扩成注册认证产品。

## 3. 最小用例、状态与权限

有效 Session 且 ACTIVE Tenant 的 MEMBER / OWNER 可读脱敏账户与诊断。创建、改名称/描述、
替换/清除凭据、启停、改绑定全部 OWNER-only；Platform Operator 不隐式获得租户权限。
身份与 Membership 在 Application 验证，提交前通过 Tenant Owner 提供的事务授权接口复核，
不跨模块直接查询 Tenant 表。Restricted Session 不得进行管理操作。

| 用例 | 输入关键点 | 成功状态 |
| --- | --- | --- |
| CreateChannelAccount | provider、物理 ID、name、description、write-only 凭据、Idempotency-Key | account_revision=1、connection_revision=1、enabled=false；登记身份和完整配置 |
| UpdateChannelAccount | expected_account_revision、name/description 至少一项 | 非空的语义变化令 account_revision +1；provider/物理身份与只读 config 不可改 |
| Replace/ClearAccountCredential | expected_account_revision、purpose、expected_credential_version、action/value | 单 purpose 版本 +1，同时 connection_revision、account_revision +1 |
| SetChannelAccountEnabled | expected_account_revision、enabled | 期望状态改变；缺少必需凭据时启用失败；不是“连接成功”承诺 |
| Get/ListChannelAccounts | 路径 Tenant、稳定 ID/游标 | 仅普通配置、configured/version、期望状态与标注时间的观测 |

账户不存在 Draft 或 Published Revision。保存即更新期望配置；credentials 替换令已启用账户
在下一次成功同步时重新接入，不要求重新发布 Profile 或 Deployment。更换连接凭据可能造成
短暂重连，Control 成功不保证远端凭据有效。V1 不在业务 SQL 事务里探测 Provider，也不让
Control 主动建立企微连接抢占已有 Owner。在线故障由 Gateway 的连接诊断返回。

清除必需凭据只允许账户已停用；已启用时返回冲突，要求先停用。Create 必需凭据应齐全，
但仍以 disabled 初始状态保存，避免“录入”自动等于“对外接入”。同一账户各 purpose
可以分次替换；各次都是完整可辨别的新连接版本，不在一个运行 Client 中拼接不同版本。

### 3.1 版本推进矩阵

| 操作 | account_revision | connection_revision | credential_version | snapshot_revision | RouteGeneration |
| --- | --- | --- | --- | --- | --- |
| name/description 修改 | +1 | 不变 | 不变 | +1（快照含 account_revision） | 不变 |
| 某 purpose replace/clear | +1 | +1 | 该 purpose +1 | +1 | 不变 |
| 账户启停 | +1 | +1 | 不变 | +1 | 有效路由改变时 +1 |
| Binding 创建/改绑/启停 | 不变 | 不变 | 不变 | 有效路由改变时 +1（min_route_generation变化） | 有效路由改变时 +1 |
| 重复幂等命令或当前版本语义 NOOP | 不变 | 不变 | 不变 | 不变 | 不变 |

min_route_generation初值为0，每次有效路由发布时设为该账户的新RouteGeneration；不改变
account_revision或connection_revision。一次事务同时改变账户和路由时，snapshot_revision只推进一次。
其余版本均使用正 int64，协议数值上限为 2^53-1，耗尽明确拒绝，不回绕。过期 CAS 即使提交值等于
当前值也冲突；只有原 Idempotency-Key 的合法重试可以读取历史 Receipt。凭据显式 replace
每次都推进版本，即使值相同，不提供猜测明文相等的公开接口。

### 3.2 账户启停与 Binding 意图的前端契约

账户启停命令本身不修改 binding.enabled，也不冻结 Binding 的目标。停用账户保留服务端
当时的 Binding 启用意图和固定目标；并发 OWNER 仍可通过独立 Binding 命令修改目标或意图。
重新启用账户时，若服务端提交时的 Binding.enabled=true，则恢复该时刻已保存的目标路由；
若 Binding 不存在或 enabled=false，则仅恢复账户接入配置，不开启新消息路由。

AccountEnabledInput 只有 expected_account_revision 和 enabled。Binding-only 变更不推进
account_revision，所以账户 CAS 不能锁定确认框曾展示的 binding_revision 或目标。服务端先
读取/核验当时的精确目标，再在账户锁事务内核对；目标变化会重做准备或返回依赖错误，不会
把客户端旧快照当作唯一允许恢复的目标。确认界面必须显示最近读取的目标、两类版本和读取时间，
明确重新启用按服务端提交时的状态执行。要求精确恢复确认目标时，应增加 Binding 版本/目标
条件的服务端契约，首版文案不宣称已有这项保证。

“暂停消息路由”显式令 binding.enabled=false，保留账户接入配置、凭据和固定目标；“停用本平台
接入”显式令 account.enabled=false，保留 Binding 意图。需要保留路由关闭意图时，应先暂停
Binding、确认结果，再停用 Account；这是两次独立命令，不是原子总开关，并发管理仍需重新读取。
这些变化异步传播，不承诺所有 Gateway 同时停止，不取消已接纳 Run；账户停用还影响后续
回复的账户发送资格。Telegram 本平台接入停用不等于删除远端 Webhook。

后端依据：[命令 DTO 与事务](../../../services/control-api/internal/channelbinding/application/commands.go#L249-L508) 的 UpdateAccountInput、AccountEnabledInput、
SetAccountEnabled、SetBindingEnabled，以及 [Binding Domain](../../../services/control-api/internal/channelbinding/domain/binding.go#L68-L96) 的 SetEnabled。
对应确认文案和验收矩阵见[渠道前端计划](../web/channelbinding-v1-flow-plan.md#42-双开关及恢复语义)。

## 4. 已实现的公开 HTTP

所有路径沿用现有 `/v1` 管理入口；显式配置Channel后注册，已列入当前OpenAPI。

| 方法与路径 | 用例 |
| --- | --- |
| POST /v1/tenants/{tenant_id}/channel-accounts | 创建，成功 201 |
| GET /v1/tenants/{tenant_id}/channel-accounts | 游标分页摘要，成功 200 |
| GET /v1/tenants/{tenant_id}/channel-accounts/{account_id} | 详情与观测，成功 200 |
| PATCH /v1/tenants/{tenant_id}/channel-accounts/{account_id} | 仅 name/description，成功 200；config 只读 |
| POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/credentials/{purpose}/update | replace/clear，成功 200 |
| POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/enabled | 显式 enabled 与 CAS，成功 200 |

所有变更带 Idempotency-Key，PATCH/启停带 expected_account_revision；凭据命令另带
expected_credential_version。JSON 关闭对象、拒绝重复 key/未知字段；普通管理 Body 上限
64 KiB，单个凭据最多 16 KiB，page_size 默认 50、最大 100。值不从 Query/Header 接收。

Create 的 credentials 按 purpose 提交 `{action:"replace",value:...}`；编辑动作只接受
keep/replace/clear，其中 keep 是显式不变且不能携带 value，replace 必须是非空字符串，
clear 不携带 value。公开 Read 只给 purpose、configured、credential_version，不回显
value、ciphertext、CredentialID、内部 key ID、Scope 凭证或请求摘要。

### 4.1 Provider用途静态约束（P2-4）

通用16 KiB是传输/内存上限，不替代Provider约束。telegram.webhook_secret的replace值必须
按完整字符串匹配 `^[A-Za-z0-9_-]{1,256}$`：1..256个ASCII字符，拒绝空值、中文、空白、
换行、斜杠或第257个字符，不trim、不自动转换。该约束来自
[Telegram setWebhook 官方协议](https://core.telegram.org/bots/api#setwebhook)。
JSON Schema同时声明minLength=1、maxLength=256，并以not + pattern `[^A-Za-z0-9_-]`
排除任何非许可字符，避免某些正则引擎的`$`接受尾部换行。
Create与CredentialUpdate的Domain、关闭Schema和fixture在加密/业务提交前使用同一规则；
keep不重新接收值，clear仍服从“先停用”的规则。静态错误返回CHANNEL_INPUT_INVALID并指向
相应purpose/value，不保存密文或成功Receipt。Gateway初始化和注册前再次校验，不发确定非法请求。
最小fixture覆盖长度0/1/256/257、合法下划线与连字符、中文、尾部换行、空格和斜杠。
这是本地静态校验，不向SQL事务加入在线Provider探测。

## 5. 持久化、幂等与加密

已实现模块自有表：channel_accounts、channel_account_credentials、channel_account_catalog、
channel_command_receipts、channel_account_observations；Binding 文档补充其两张业务表。
租户表通过 `(tenant_id, id)` 外键关系约束，account_id 另有全局唯一索引；凭据唯一键为
`(tenant_id, account_id, purpose)`，CredentialID 与该归属不可重新绑定。

值采用带随机 nonce 的 AEAD（AES-256-GCM）；AAD 绑定域 `channel-account-v1`、Tenant、
Account、Provider、Purpose、CredentialID、credential_version。密文列带 key_id，密钥仅由
平台密钥配置供应，不与密文同库存储。换加密主密钥的 rewrap 不推进业务凭据版本；
更换 Bot Token 推进凭据版本。账户加密 Adapter 已实现于 `adapter/outbound/credentialcrypto/keyring.go`，
真实 PostgreSQL 事务保存密文，公开查询和 Receipt 保持脱敏。

包含凭据的请求摘要使用独立 MAC key 的 HMAC-SHA-256，绑定 operation/tenant/actor scope、
Expected 值与规范化请求；不得保存明文请求、无密钥密码摘要或可离线枚举的凭据 hash。
Receipt 只保留脱敏成功结果、稳定 ID、版本与请求 MAC；密钥轮换必须能在 Receipt 有效期内
验证旧 MAC key，不通过丢弃 Receipt 重新执行命令。

同一事务依次取得目录行锁（账户命令或可能改变有效路由的Binding命令）、账户锁、账户 RouteState 锁、Binding 锁，完成
CAS/唯一性/授权复核，保存密文、账户、快照计数、必要路由事件和 Receipt。涉及子集时
保留该顺序。目录计数使用受锁行 UPDATE，禁止用提交顺序不确定的独立 sequence。
NOOP 可保存成功 Receipt，但不推进业务版本。所有变更 Receipt 的唯一作用域是
`(tenant_id, operation, scope_id, key_hash)`；先重新认证授权，再匹配请求 MAC，才重放成功结果。
并发同 key 同正文得到同一结果，异正文返回 409；事务失败不留部分账户/凭据/路由记录。
成功响应丢失后重试不得重复旋转凭据。失败不写成功 Receipt，客户端可修正后换 key 重试。

账户停用与有效路由 tombstone 必须原子，详见 [Binding 事务](channelbinding.md#binding-transaction)。
账户配置通过 HTTP 快照供应，V1 不新增账户配置 NATS 流；审计只存动作/身份/版本元数据。
当前开发期已直接更新 `0001_baseline.sql` 与 Channel Schema，无旧开发数据兼容层；
基线与真实 PostgreSQL 集成验证见[当前实现入口](#当前实现入口与验证)。

<a id="ca-sync-v1"></a>
## 6. Control → Gateway 完整账户快照协议

### 6.1 传输、身份与范围

已实现 `GET /internal/v1/channel-accounts/snapshot`，不是公开 Session API。
V1 使用双向 TLS 工作负载认证；由平台配置的可信 issuer、SAN principal、audience 和
用途 allowlist 映射 Gateway pool 的 scope_id。Body、Query 和客户端自报 tenant_id
不扩大授权范围；反向代理转发认证身份时必须阻断外部同名头并认证代理本身。
内部路径不注册到匿名 8091，也不复用用户 Cookie 授权。

V1 一个部署执行池使用一个稳定 scope，scope 包含该池被授权服务的租户集合；当前先使用
一个固定全平台 Gateway pool，不引入调度或用户分片配置。每次请求校验 scope 授权仍有效。
更改 scope 身份/租户集合必须显式停机重建同步信任，不能以权限缩小后的成功空列表
冒充原集合的删除。scope_id 是机器授权范围，不是 Environment 业务对象。

#### 6.1.1 mTLS 与账户快照的含义

mTLS（mutual TLS，双向 TLS）是机器之间的认证加密连接：Gateway 验证 Control 的服务端
证书，Control 同时验证 Gateway 的客户端证书。当前 Control 内部监听要求 TLS 1.3 与受信
客户端证书，并用证书 URI SAN 匹配预配置 workload principal，再校验 scope、账户和用途。
它不是用户登录方式，也不要求租户用户填写证书；证书与信任配置由平台运维提供。

账户快照是有版本、完整且可校验的一份非秘密账户配置清单，不是 SQL 数据库备份。
Gateway 在启动/周期刷新时读取它；快照仅含普通配置与凭据引用/状态，真实值由独立
`credentials:resolve` 接口按授权和精确版本供应。每条消息不重新读取账户快照或查询 Control 路由。

### 6.2 关闭响应字段

```text
schema_version: 1
scope_id: 平台配置的池身份
source_epoch: 目录源的持久 UUID
snapshot_revision: 1..2^53-1
snapshot_digest: sha256: + 64 位小写 hex
complete: true
accounts: [
  {tenant_id, account_id, provider, provider_account_id,
   account_revision, connection_revision, enabled, min_route_generation,
   config: provider 关闭结构,
   credentials: [{purpose, credential_id, credential_version, configured}]}
]
```

账户数组按 `(provider, account_id)` 排序，credentials 按 purpose 排序，禁止重复身份/purpose。
Digest 对 `{scope_id,source_epoch,snapshot_revision,accounts}` 的项目统一 RFC 8785 Canonical JSON 求
SHA-256，拒绝重复 JSON key，不包括传输字段和凭据值。相同 snapshot_revision 必须有同一
Digest；metadata 变化推进目录水位，但相同 connection_revision 不重建 Client。

服务器在同一 PostgreSQL REPEATABLE READ 只读事务中取得目录行与全部账户/凭据元数据，
关闭事务前完成完整性与大小检查；目录水位与账户变更在写侧同事务推进。Gateway 不读取
Control 数据库，也不把多个 HTTP 页面拼成貌似原子快照。

### 6.3 限额、时序与错误

- 每 scope 最多 **1000** 个累计账户，含停用记录；租户可配更小配额。写入时在目录锁下预检
  数量和规范化完整快照不超过 **2 MiB**；读取再复核。额度耗尽返回稳定错误，不返回前 N 项。
- V1 无分页、无 304 简写、无压缩输入；响应 decoded Body 上限 2 MiB，complete 必须为 true。
- Gateway 每 **10 秒**轮询，加入 ±20% 抖动；HTTP 单次总期限 **5 秒**，一次只允许一个在途轮询。
- 快照新鲜度上限 **30 秒**，从成功请求的本地单调时钟起点计，不使用服务器时钟推断租约。
  V1 任一真正轮询失败立即取消新工作的账户资格，不提供 stale grace；超时 watchdog 同样关闭资格。
  已被并发更新取代的合法响应是SUPERSEDED（见6.4），不是源失败，不续新鲜度并立即重拉。
- 响应过大、格式错、超时、401/403/5xx、scope 不符、complete 不真都作为错误，不变成空数组。
  失败不覆盖最后的持久水位；诊断可保留旧元数据，但不能用它恢复工作资格。
- snapshot_revision必须按6.4区分请求起点水位与共享提交水位；真正回退或已知同版异Digest隔离。
  新source_epoch需显式重建信任，不自动接受新epoch中的较旧账户/凭据版本。
- 只有准备应用的候选快照，才对照当前持久账户校验connection_revision、credential_version和
  min_route_generation不回退；已SUPERSEDED的旧响应不与更新账户逐项比较并误报回退。
  相同connection_revision必须有相同连接配置/凭据映射（metadata与路由下限除外）。
  account_id不复用，普通重启/备份恢复不生成新epoch掩盖回退。

Gateway 的可信快照 Adapter 必须把 scope/epoch/水位与非秘密完整投影一起持久化后再交付
Supervisor；持久化失败不应用。重启后磁盘投影仅用于校验水位，先成功刷新再开放资格。
成功完整快照内某账户缺席，才可在同 scope 下解释为移除资格；日常停用显式保留 enabled=false。
新客户端不可信任网络第一条任意 epoch：期望 source_epoch 由部署引导读取并固定；更换时停止
接纳、核验 Control 权威目录并受控清理/重建账户投影，路由日志使用自己的独立恢复协议。

租户当前仅 ACTIVE 状态，V1 无租户停用接口。未来 Tenant 增加停用/迁移时，必须通过其拥有方
用例协调账户资格和目录水位，并测试失效传播；不得只在快照读路径动态过滤而不推进版本。

<a id="ca-snapshot-order"></a>
### 6.4 多副本请求与共享水位的排序（P2-1）

Gateway每个实例最多一个在途请求。发请求前从共享PG读取同一source_epoch的
`H_start`及其Digest，并记录`request_started_at_db`和本地单调起点；请求总期限仍为5秒。
响应自带的R不是请求序号。验证关闭DTO、范围、Digest与源身份后，在持久目录事务内读
`H_commit`（其他实例可能已推进）并使用下表，不以“R低于当前值”一概隔离：

| 条件 | 分类 | 行为 |
| --- | --- | --- |
| R < H_start | SOURCE_ROLLBACK | 对端在本请求开始前的已知可信事实之后倒退；隔离、不应用 |
| H_start <= R < H_commit | SUPERSEDED | 合法在途旧响应；不覆盖目录、不续本实例新鲜度，立即重拉 |
| R == H_commit 且Digest一致 | SAME | 幂等；使用当前持久完整投影，可按本请求起点刷新本实例资格 |
| R > H_commit | ADVANCE | 检查账户版本/身份不回退，原子应用目录与共享水位，再刷新本实例资格 |
| 与已知同epoch同revision的Digest不一致 | CONTENT_CONFLICT | 无论上面哪一行，优先隔离 |

H_start必须是发请求前实际读取的可信共享水位，不能由客户端常量0替代；没有水位时为0。
目录源必须提供写主库的一致性快照，不把落后的只读副本作为正常供给。SOURCE_ROLLBACK的
判定相对于H_start，不相对于响应到达时其他实例刚写入的H_commit。

Gateway保留有界的非秘密snapshot receipt `(source_epoch,revision,digest,received_at)`，
用于比对并发窗口内的同版异内容；当前水位与每个在途请求的起点Digest也保留。
普通receipt至少90秒后清理（覆盖5秒在途期限与30秒freshness）；收到已过请求deadline的响应
直接丢弃，不允许其在清理后重新申请资格。receipt保存的是协议一致性证据，不是历史账户正文。
SUPERSEDED可记录一致性receipt，但不改变当前投影/共享高水位/本实例资格到期点。
持续SUPERSEDED也不豁免30秒过期；不按别的副本写库时间替本实例续期。

例：A记录H_start=10并读R=10，B先应用R=11，A到达时H_commit=11，应为SUPERSEDED；
A下一次实际读H_start=11而对端仍回10，才是SOURCE_ROLLBACK。两者必须分别有双副本屏障测试。

<a id="ca-route-floor"></a>
### 6.5 账户与路由的恢复屏障（P2-2）

`min_route_generation`是账户快照的必需非秘密字段，范围0..2^53-1，来自本模块AccountRouteState。
没有已发布路由时为0，不代表存在可用路由。每次有效路由改变，都在同一个Control事务内保存
新的generation、min_route_generation、route Outbox和snapshot_revision；Binding管理CAS、
连接版本与路由下限不是同一计数器。快照读取在同一事务关联本模块RouteState，不跨Owner查表。

Gateway新Admission必须同时满足账户enabled、当前精确连接/本实例资格、可信路由enabled、
相同Tenant/Provider/Account，以及 `route.generation >= account.min_route_generation`。
低于下限返回可重试ROUTE_BEHIND_ACCOUNT，不使用旧目标、不改查latest。路由超过下限仍须
满足原有Manifest/归属/Digest校验，不把数字大当成授权。此字段不加入现有RouteProjection wire。

因此disable→改目标→enable即使被一次HTTP快照合并，也会携带重新启用时的更高下限，
旧enabled路由不再通过门禁。快照只因路由下限变化时不重建Client；Gateway事务guard使用
持久目录当前下限，而非Handler手里的旧副本。Delivery对已接纳Run仍沿用原ReplyOrigin，
不拿当前路由下限重定向/删除旧回复；它检查当前账户/凭据与自身发送资格。

这是“已应用新账户快照后不使用更旧路由”的恢复屏障，不是Control与所有Gateway瞬时原子切换。
尚未观察到变化的副本仍受原有有限freshness窗口约束；连接与路由均未更新时不宣称立即撤销。

<a id="ca-credentials-v1"></a>
## 7. Gateway 读取账户凭据

已实现 `POST /internal/v1/tenants/{tenant_id}/channel-accounts/{account_id}/credentials:resolve`。
请求 schema_version=1，包含 scope_id、source_epoch、connection_revision、uses 与 consumer。
内部HTTP总期限5秒，响应decoded Body最多64 KiB且不接受压缩；禁止缓存或自动重定向到另一来源。
uses 每项为 `{purpose,credential_id,credential_version}`；consumer 为
`{kind,instance_id,owner_epoch?,registration_epoch?}`。owner_epoch仅用于wecom_connection，
registration_epoch仅用于telegram_registration；两者都是本地fence绑定/审计声明，不是Control的
独立租约证明。拒绝不适用字段、未知/重复purpose，最多2项、Body最多16 KiB。

| consumer.kind | 可读取 purpose | 本地前提 |
| --- | --- | --- |
| wecom_connection | wecom.bot_secret | 已取得当前真实 OwnerGrant |
| telegram_webhook | telegram.webhook_secret | 新鲜、enabled的账户配置与本地Handler安装资格；不以已收到首条Webhook为前提 |
| telegram_delivery | telegram.bot_token | 当前账户版本与Delivery准备资格；实际发送另受A1/A2事务guard约束 |
| telegram_registration | telegram.bot_token + telegram.webhook_secret（恰好两项） | 新鲜、enabled的账户配置及当前注册fence；不依赖Binding、已有Webhook或Delivery Claim |

Control 按以下顺序授权，并在账户行锁事务中与 replace/clear/disable 序列化：

1. 验证 mTLS principal、Gateway 角色、audience、scope 与 principal 对应的 instance_id。
2. 通过 Tenant Owner 查询 Tenant 有效状态；核对账户实际 Tenant、Provider、scope 与 enabled。
3. 核对 source_epoch、精确 connection_revision；取模块自己的 purpose→CredentialID 映射，
   验证每个 ID/版本/configured，禁止只凭请求中的引用找值或自动替换成最新版本。
4. 按 principal 的工作用途和 consumer.kind 验证允许用途集合；所有项成功后才批量解密。
5. 返回 scope_id、source_epoch、tenant_id、account_id、connection_revision 与
   values:[{purpose,credential_id,credential_version,value}]，且只返回获准用途；Cache-Control:no-store。

内部服务器可认证一个 Gateway workload 获准服务该 scope，但裸 OwnerGrant/owner_epoch 是
调用方声明，不是 Control 可独立验证的租约证明。V1 不承诺 Control 读取 Gateway DB 或远程
核验每个 lease。实际所有者限制由 Gateway 的可信 Resolver Adapter 执行：取值前、响应后、
构造/attach 前检查当前 grant、取消上下文和精确配置版本。失去 lease 即丢弃结果，不启动 Client。
若以后要求 Control 本身证明实例所有权，必须另行设计 Owner Authority 认证协议，不能把
增加几个自报字段当作完成。Telegram 不伪造企微 grant；其接收与发送沿用各自本地资格校验。

### 7.1 Telegram注册用途与首次启用（P1-2）

telegram_registration是独立的受认证consumer授权，不借用telegram_delivery或telegram_webhook。
Control检查Gateway principal的注册用途allowlist、账户provider=telegram、desired.enabled=true，
并在同一个账户锁事务内返回同一connection_revision下恰好两个purpose及其精确版本。
缺一项、多一项、旧版本或不适用epoch字段都拒绝；不允许分两次取值后拼成“同版本集合”。

首次启用先保存账户enabled和完整凭据，Gateway成功应用该配置后即可竞争注册fence并取值，
不要求连接READY、收到消息、Binding存在或已有Delivery Claim，避免循环前置条件。
Gateway注册前核对当前配置、fence和取消令牌；返回后和写注册完成状态前再次核对。
过期fence/版本的迟到成功仅作为原操作的观察结果，不能把当前版本标为注册完成或重装旧Handler。
Control不信任自报registration_epoch来扩权；实际fence由Gateway自己的持久注册Owner执行。

### 7.2 操作时间预算（P2-3）

凭据Resolve使用独立CredentialResolveTimeout=5秒，由当前instance/账户/Owner上下文派生，
不被现有Supervisor通用OperationTimeout=2秒提前截断。LeaseOperationTimeout仍默认2秒、
满足严格小于LeaseTTL/3（默认TTL15秒）；不把通用timeout直接改成5秒。
快照轮询的HTTP5秒有独立父ctx。失租、账户版本变化/停用、source资格失效或关闭都取消
凭据ctx，5秒只是上限而非Owner有效期；实现位置与测试见 Gateway Control 接入文档的时间预算表。

解析不读取 Binding，不改选 Agent，不使用 Runtime Profile 凭据 API。网络失败或旧版本请求
返回可识别失败；Gateway 重新取得完整配置后再发新逻辑解析，禁止透明重试混用旧/新版本。
连接恢复可以对相同精确版本重新解析；已完成版本替换后，旧版本请求返回 409，不读取旧值。
Control 仅保存当前值，不为开发历史保存旧密文；已得到旧值的 SDK 可能仍有短暂在途调用，
停用/轮换不是撤回已发送网络字节的保证，Gateway 必须停止新调用并尽快 drain/close。

凭据仅在受限输入、Owner 加解密及经认证响应中出现；不进入普通 GET、快照、路由流、
RuntimeManifest、RunRequested、Receipt、日志或 Trace。Gateway 不落盘缓存凭据，不跨账户、
用途或 connection_revision 缓存；内存材料随 Client 结束释放，错误只含稳定分类。

<a id="ca-observations-v1"></a>
## 8. 运行观测，不把“保存成功”当“接入成功”

公开详情分开呈现 desired.enabled/connection_revision 与 observed 状态；无上报时为 UNKNOWN，
过时观测为 STALE，不推导 CONNECTED。Control 不靠轮询 Gateway 8091 来判断单个账户。

已实现 `POST /internal/v1/channel-account-observations`，使用同类工作负载认证，最多 100 条、
128 KiB，成功 204。每项包含 scope_id、source_epoch、tenant_id、account_id、provider、
connection_revision、instance_id、instance_epoch、report_sequence、state、reason_code、observed_at。
企微另带 owner_epoch 作为诊断，不作为授权；state 为 CONFIG_APPLIED/CONNECTING/READY/
DISABLED/ERROR。文本 message、provider 原始响应和秘密字段不接收。

Control 校验实际账户归属、principal/instance、版本和单调报告序列；以服务器 received_at
判断时效，observed_at 仅展示，不允许客户端未来时钟延长资格。每个实例启动生成新的
instance_epoch；新旧实例并发报告分别保留，不能让旧启动的迟到报告覆盖新启动视图。
Gateway 状态变更上报，稳定时每 30 秒心跳；90 秒未见心跳标 STALE。
V1每账户最多返回32个已授权实例摘要；旧启动观测在24小时后清理，清理不删除账户/路由水位。上报失败不改变已授权
运行资格，仅令观察状态变旧。多副本状态返回有界实例摘要；不以最高自报 owner_epoch 推导
可信所有者，也不把某个非 Owner 的 STANDBY/非 READY 误报成所有连接都失败。

连接状态、路由投影应用和 Worker 运行是不同观测，后两者不能用这里的 READY 代替。
V1 不新增监控/调度服务；上报、查询和现有观测基础设施属于原有两项 Workload。

## 9. 稳定诊断

| code | HTTP / 语义 |
| --- | --- |
| CHANNEL_INPUT_INVALID / CHANNEL_LIMIT_EXCEEDED | 400 / 413 或配额 422；明确字段路径，不回显 value |
| CHANNEL_ACCOUNT_NOT_FOUND | 404；跨租户对象也隐藏为不存在 |
| CHANNEL_PERMISSION_DENIED | 403；无 Session 为 401 |
| CHANNEL_REVISION_CONFLICT / CHANNEL_IDEMPOTENCY_CONFLICT | 409；仅返回可见版本 |
| CHANNEL_ACCOUNT_IDENTITY_CONFLICT | 409；不泄露其他登记主体 |
| CHANNEL_CREDENTIAL_REQUIRED / CHANNEL_ACCOUNT_DISABLED | 422 / 409；不返回内部 ID |
| CHANNEL_CREDENTIAL_VERSION_CONFLICT | 内部 409；应刷新配置，不自动取新值 |
| CHANNEL_WORKLOAD_DENIED | 内部 403；不靠 consumer 自述授权 |
| CHANNEL_SOURCE_INTEGRITY / CHANNEL_SOURCE_EPOCH_MISMATCH | 500 / 内部 409；Gateway 隔离并取消资格 |
| CHANNEL_DEPENDENCY_UNAVAILABLE | 503；Owner/密钥依赖故障，不伪装凭据不存在 |

Provider 在线错误属于 Gateway reason_code，例如 AUTHENTICATION_FAILED、PROVIDER_UNAVAILABLE、
REMOTE_IDENTITY_MISMATCH；它们不导致 Control 保存事务回滚，也不冒充静态字段校验成功。

## 10. 已实现切片与文件归属

| 阶段 | 当前实现位置 | 验收出口 |
| --- | --- | --- |
| A0 协议 | api 下独立账户管理/内部协议 Schema、DTO 与 fixture | 本文与 Gateway control-integration-v1.md 一致，不复制路由 Schema |
| A1 纯领域 | channelbinding/domain/account.go、credential.go、snapshot.go | 版本矩阵、purpose/配置、限额与身份去重规则 |
| A2 管理纵切 | application/commands.go；adapter/outbound/postgres/write.go、receipt.go；inbound/http/handler.go | 真实 PG、租户角色、CAS、MAC Receipt、加密与失败原子性 |
| A3 快照/凭据 | application/runtime.go；adapter/outbound/postgres/snapshot.go、credentials.go；inbound/internalhttp/handler.go | 真实 mTLS、scope、原子快照、行锁与版本竞争测试 |
| A4 Gateway 接线 | Gateway 自有 snapshot Adapter、CredentialResolver、Supervisor/Telegram Adapter 与 bootstrap | 动态新增/轮换/停用，无生产文件/env退路 |
| A5 观测与组合 | application/runtime.go、adapter/outbound/postgres/observations.go；Binding 与 Gateway 验收 | 状态可区分，凭据不泄漏；不假装 Worker 已运行 |

V1 直接完善当前设计，不新建 Secret 产品、动态注册中心、策略服务或独立同步服务。
上述 A0～A5 已落地，公开 11 个操作已纳入当前 OpenAPI，3 类内部 mTLS 接口单独记录。
Runtime Profile 专属文档与 Channel Web 不由本切片修改；Gateway 正文和代码由 Gateway 模块拥有方维护，在集成仓库中使用相对链接引用。

## 11. 设计验收矩阵

| 场景 | 必须验证的结果 |
| --- | --- |
| 用户自助新增机器人 | 无人工环境变量；默认 disabled；启用并同步后由 Gateway 实际接入 |
| 两租户使用相同显示名 | account_id 独立；不能跨租户读取配置/凭据；物理 ID 冲突不泄露对方 |
| 改名 | 管理与快照版本前进，Client/连接版本不变 |
| 已启用账户轮换凭据 | 新连接版本、旧 Resolve 冲突、SDK 重连；绑定/Manifest 不变 |
| 凭据请求期间丢失 owner | 本地三次检查拒绝 attach，响应值丢弃，不声称 Control 自报epoch证明授权 |
| Telegram 多副本 | 不造企微grant；Webhook Secret与发送Token分权；轮询不每副本调用setWebhook |
| 账户停用与启用 | 禁用连接资格与路由 tombstone 同事务发起；恢复需新连接版本，旧Run不改目标 |
| 快照截断、失败、401或越界 | 不作为成功空列表；取消资格，不覆盖水位 |
| 同版本异内容、版本回退、新epoch | 进入隔离；显式恢复，不清水位自动接受 |
| 重启/Control短暂不可用 | 重启先刷新；源失败立即取消新工作资格；无离线明文缓存 |
| 并发凭据替换和重试 | 仅一方CAS成功；旧key重放原结果，无秘密Receipt，无重复旋转 |
| 状态展示 | 保存成功、配置已应用、连接READY、路由已生效分别表达；无观测为UNKNOWN |

## 12. 本轮审查修订与回归要求

本轮针对 2 项 P1 与 4 项 P2 的契约修订已落地并有回归证据：事务 AccountUseGuard 见 Gateway 正文，
telegram_registration见7.1，快照SUPERSEDED见6.4，min_route_generation见6.5，独立超时见7.2，
WebhookSecret静态约束见4.1。新增验收：双副本10/11先后屏障、跳过disabled快照的旧路由拒绝、
注册首次启用与同版双purpose、3秒凭据成功/失租取消、Provider字符边界、停用与A2先后及结果落账。

<a id="当前实现入口与验证"></a>
## 13. 当前实现入口与验证

- 公开契约：`api/openapi/control/v1/channel-public.yaml`，由主 OpenAPI 引用12个已实现操作。
- 启动配置、三条内部 mTLS 路径、Producer权限与本地回归命令：
  [Control Channel 运行说明](../../../services/control-api/CHANNEL_RUNTIME.md)。
- 真实 PostgreSQL 集成验证账户/凭据/Catalog/Receipt/Route的原子性、提交期OWNER检查、
  同key并发、RR快照、凭据替换/禁用互斥、观测seq/服务端freshness和SQL非空约束。
- `integration/channel_test.go`通过真实Session和mTLS握手验证公开管理、真实编译目标、
  同版本registration凭据集合、跨租户拒绝、轮换拒绝旧引用和观测回传。
- observations的同seq重放不刷新received_at；相同seq异内容冲突。清理由独立短事务每分钟
  分批删除24小时以前的记录，不持有账户目录写锁，不参与运行授权。

实际Telegram及跨Workload持久证据见[联合验收记录](channel-acceptance.md)。
