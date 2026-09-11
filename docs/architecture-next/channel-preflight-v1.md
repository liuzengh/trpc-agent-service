# Telegram 接入预检 V1：跨端设计与交付契约

状态：**契约已冻结，开始实现；尚未完成代码验收**。日期：2026-09-06。

总设计、契约审查、Web 与最终集成：`优化 Web 页面框体对齐` 任务。
Control 设计与实现：`control-api` 任务。Gateway 设计与实现：`设计 Channel Gateway` 任务。
本文归协调侧维护；Control 拥有 wire Schema/OpenAPI，Gateway 与 Web 消费同一冻结契约。

## 1. 用户问题与完成定义

用户应能在 Telegram ChannelAccount **仍停用**时检查配置、凭据与远端已有 Webhook，
而不是先启用账户才能发现 Token 无效、机器人身份不匹配或 public origin 为 localhost。

完整交付包括：两侧文档对齐、协议冻结、Control 与 Gateway 实现和测试、Web 接入、
正常合入远端 main、协调侧拉取后独立真实 HTTP/浏览器验收。
“设计完成”“代码合入”“真实预检通过”“真实消息接纳”“Worker 回复”分别记录，不互相替代。

本次预检交付不授权改变现有 Telegram Webhook、启用真实账户、清空 pending updates 或发送消息。

## 2. 职责

| Module | 所有权 |
|---|---|
| Control `channelbinding` | OWNER 发起、任务生命周期、并发/幂等/限流、受限凭据授予、结果存储、成员读取与结果新鲜度 |
| Gateway `connection` | 领取诊断工作、精确版本诊断上下文、只读 Telegram 调用、运行配置检查、结构化结果上报 |
| Web Channel | 发起/轮询/重试、逐项结果、过期与未知提示、可读修改建议；不接触托管秘密值 |
| Gateway routing/admission | 后续真实路由应用和消息接纳；不是预检任务执行器 |
| Worker/Deployment/RuntimeProfile | 本次不改；预检不要求已有 Binding 或发布目标，不产生 RunRequested |

现有运行目录 Lookup 和凭据 MatchCredentialUses 均拒绝 disabled。新增预检必须走独立、
短时、任务绑定的诊断授权，保留普通注册、接收、发送通路的现有守卫。

## 3. 交互和副作用不变量

```text
创建账户（停用）→ 检查接入条件 → 查看逐项诊断 → 修正后重新检查
                                         ↓ 用户另行确认
                               启用接入 → 开启路由 → 真实消息验收
```

- 检查不调用 setWebhook/deleteWebhook/getUpdates/sendMessage。
- 检查不临时 enabled=true，不安装用于接纳消息的 webhook handler。
- 检查不更改 Account/Binding revision、connection revision、route generation、credentials。
- getMe 成功仅说明 Token 可用于识别机器人；还须与保存的 provider_account_id 精确匹配。
- getWebhookInfo 读取旧 URL、积压和错误，不验证尚未注册的新 URL，也不能回读旧 secret_token。
- origin 的语法/DNS检查、本机自连、setWebhook ACK 都不能标记 Telegram 实际投递成功。
- 首版是诊断能力，不将预检结果当成永久授权，不偷偷改变现有启用 API 为强制预检票据协议。

## 4. 冻结公开接口（完整 JSON 以 Control 契约为准）

```http
POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights
Idempotency-Key: <client-generated-key>

GET /v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights/{preflight_id}
```

POST 返回 202 与任务资源；发起者是正常 unrestricted Session + ACTIVE OWNER。
GET 允许 ACTIVE tenant member，禁止跨租户枚举。响应 no-store，只包含白名单去秘密字段。
请求只引用已保存账户及期望版本，不接收临时 Token、任意 URL、任意 scope 或 instance。

Control 设计必须明确：

1. POST body、202 原始/重放响应、GET response 的完整必填/可选字段。
2. 同 key 同 body 重放、同 key 异 body 冲突、过期 CAS 和重复进行中任务的语义。
3. job state 与检查结果分开：任务完成不表示每项检查通过。
4. checks 的固定集合/顺序、状态枚举、稳定原因码、可呈现的受限 details。
5. requested_by、requested_at、started_at、checked_at、expires_at 的所有权和空值规则。
6. 账户身份、connection_revision、凭据版本、Gateway 配置指纹与检查结果的关联。
7. metadata-only 修改不应无意义地判定 Token/网络检查过期；版本取舍需明确。
8. 无 Gateway、执行超时、进程重启、重复领取、迟到结果和最终无结果的终态。

## 5. 检查项与事实粒度

首版至少涵盖下列语义，最终 machine codes 由两侧文档对齐后冻结。

| 检查 | 数据来源 | 不得推断 |
|---|---|---|
| 凭据配置齐全 | Control 保存元数据 | 已配置不等于真实可用 |
| 机器人身份匹配 | Gateway → Telegram getMe | 非网络错误不能统一冒充无效Token |
| public origin 配置适用于公开 Telegram | Gateway 实际配置，HTTPS/host/端口/本地地址检查 | 格式通过不等于外网可达 |
| 当前 Webhook 与期望入口关系 | getWebhookInfo + 本账户期望路径 | 其他 URL 不代表可随意接管 |
| 远端积压/最近投递错误 | getWebhookInfo | 无近期错误不代表已收到测试消息 |
| Telegram 外网投递 | 本次没有真实消息时为未验证 | 不以 READY/PUBLISHED 代替 |

必须能分别呈现通过、明确失败、警告、未知/未验证；某项失败不应伪造未执行项目的成功。
网络失败、限流、超时、远端身份不匹配采用稳定分类，禁止透传含 Token 的 SDK 错误字符串。
旧 Webhook URL 只用于比较与受限展示，不能作为任意目标发起 HTTP 请求。
首版只对 origin 做静态有界检查，不做 DNS/TLS/任意 URL 探测。

## 6. 私有执行协议与权限

推荐沿现有 Gateway 主动访问 Control 的 mTLS 方式扩展“领取工作→解析诊断Token→上报结果”。
不建立 Web 到 Gateway 管理口的连接；不借用运行 RunRequested 事件。

- Control 创建具有 scope/tenant/account/connection-version 约束的诊断工作。
- 领取绑定被允许的 mTLS principal、instance/epoch、claim/fencing token 和 lease 截止时间。
- 同一任务同一时间最多一个有效执行者；有界重试不得无限扫描或无等待热循环。
- 凭据解析仅授予该工作所需的 Bot Token，并验证任务、claim、版本和有效期。
- 不借用 telegram_delivery 作为旁路，不把 telegram_registration 注册许可送给预检。
- Gateway 预检上下文独立于普通 enabled 账户资格；专用 Adapter 不暴露注册/发送方法。
- 停用可以预检；删除授权、租户停用、版本变化、claim 过期后不能继续解析或提交为当前结果。
- 完成请求须可幂等；旧 lease 的迟到结果不能覆盖新执行者或新配置的结果。
- 回报结果按同一版本化 Schema 校验，未识别字段/检查码不悄悄作为成功接受。

建议预算待双方确认：每账户最多一个进行中任务；每实例有限并发；单次 provider 调用5秒；
完整任务有硬截止；lease 大于一次完整检查预算；结果 TTL 明确，错误结果也有检查时间。
原始密钥不进入任务表、日志、审计报告、浏览器存储或响应；公开结果只保留受限事实。

## 7. 前端计划

- 账户创建完成后给出“检查接入条件”，与“启用本平台接入”分开。
- 诊断页新增预检面板：未检查、排队/进行中、逐项结果、过期、重新检查。
- POST 立即返回任务，GET 有界轮询；页面卸载停止轮询，断网保留任务ID，避免重复创建。
- “运行目标未配置”只影响后续路由，不阻止机器人接入预检。
- 只改名称/说明采用一次保存；保留 CAS、原 key 恢复和错误反馈。
- 当前目标主要展示部署名称 · rN，长ID留技术信息；账户列表不引入 N+1。
- “Control API可达”“预检结果”“Gateway连接”“路由应用”“消息/Run”独立显示；顶栏只写“已登录”。
- MEMBER可通过本账户分享链接 `?preflight=<id>` 查看；前端自行构造同源GET，不跟随任意status_url。
- PASS只写“配置检查通过”，固定展示“真实消息投递未验证”“旧Secret不可回读”。
- Gateway配置的当前性为UNCONFIRMED；CURRENT只指账户连接身份和结果TTL，不冒充实时配置就绪。

## 8. 测试与合并门禁

Control：真实Session OWNER/MEMBER/outsider；禁用账户预检；CAS/幂等/限流；mTLS身份、
claim抢占/过期/重复完成/迟到结果；凭据精确用途和版本；无Gateway终态；Schema/OpenAPI一致。

Gateway：只读Adapter调用白名单；Token无效/网络失败/429/超时/身份不符；旧Webhook差异；
localhost/public origin；结果受限字段与脱敏；执行取消/有界并发；零注册/发送/接纳副作用。

Web：真实DTO夹具；OWNER触发/MEMBER只读；202轮询/中断/超时/重放/403；失败与未知不呈现绿灯；
过期提示；账户无Binding可检查；移动端和真实后端浏览器验收。

集成：固定基线与修改后测试；原文件哈希保存；回滚仅在副本验证；保留修改后的交付文件。
真实Telegram只调用读取API，比较检查前后Webhook、Account/Binding开关、revision、
route generation、Gateway registrations/receipts/admissions，确认无启用/注册/消息副作用。

远端合并串行：Control测试并合入origin/main → Gateway拉取该main并集成测试、合入 →
协调侧拉取两端提交，完成Web与真实联调，再合入Web并复核远端包含性。只使用正常push。
主项目脏main不作为集成目录；当前Web未提交功能先受控保存，不执行覆盖性reset/clean。

## 9. 对齐后的公开状态摘要

公开创建只接收 `expected_account_revision`、`expected_connection_revision`、`expected_bot_token_version`；
202为不可变创建回执，GET为任务状态。新任务仅允许停用的Telegram账户；缺Token仍可得到配置失败诊断。

- state：QUEUED / RUNNING / COMPLETED / TIMED_OUT / STALE。
- outcome：PASS / WARN / FAIL / UNKNOWN。
- freshness：NOT_CHECKED / CURRENT / STALE / EXPIRED。
- 固定8项：凭据配置、机器人身份、public origin、已有Webhook关系、积压、历史投递错误、恢复资料、真实投递。
- overall只聚合前6项，后2项的UNKNOWN永远展示，不隐藏。
- public origin非法但身份核对成功仍读WebhookInfo；有旧URL则WEBHOOK_COMPARISON_UNAVAILABLE/UNKNOWN，
  无旧URL仍WEBHOOK_NONE，积压/历史错误照实展示。
- 只改名称/描述保持连接检查有效，metadata_changed=true；轮换、清除、启停ABA使旧连接事实过期。

所有逐字段空值、稳定码、配额与私有JSON以 [Control冻结契约](control-api/telegram-preflight-v1.md) 为唯一wire来源；
[Gateway执行设计](channel-gateway/telegram-preflight-v1.md) 解释具体适配器/调度实现，不复制分叉Schema。

## 10. 冻结记录

- 冻结时间：2026-09-06T13:27:02.661750+00:00。
- 契约版本：Telegram preflight V1，schema_version=1；仅上述2公开+3私有操作。
- Wire唯一来源：Control `docs/architecture-next/control-api/telegram-preflight-v1.md` 的最终§3–9。
- 双方已通过任务消息确认版本、requester撤权、origin非法分支、配置新鲜度与JCS向量。
- JCS已由Gateway实际Go依赖、Control与协调侧独立计算一致：
  `sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d`。
- Gateway旧临时稿含历史候选JSON；安装正式文档前删去候选wire并引用Control唯一契约，
  同步并发4、空队列2秒±20%以及固定8项；不得照旧样例实现。
- 冻结补充：getMe之后Token外部撤销使getWebhookInfo返回401时，
  webhook_registration=TOKEN_REJECTED/FAIL、presence=null/relation=UNKNOWN。
- 根任务已下发“契约冻结，开始实现”；后端只完成各自实现/测试/功能分支提交，
  远端main合并继续等待根任务串行门禁。
- 冻结时尚无实现完成声明；后续源码实现和验收边界见第11节。


## 11. 实现与验收边界（2026-09-07）

### 11.1 模块交付

- Control 实现提交 `c7cf4e184e0355f2da635f0b43d91b18ab580952`：
  2个公开操作、3个私有操作、共享 DTO/Schema、PostgreSQL 任务和幂等回执、
  真实 Session/OWNER 检查、claim/lease/版本围栏、有界维护循环及 additive 0002 migration。
  首次完成发现 Gateway 配置变化会先提交 STALE，再返回冲突；撤权 Session 的创建请求保持零写入。
  真实联调后的可见性修复 `48d589e793950d8e09988710c9162fb516b527ce` 使非成员的预检
  POST/GET 在参数解析前统一404，保留可读 MEMBER 发起403及提交时 Session 撤销403。
- Gateway 实现提交 `7e22af0d2cba3f68cee4e208f194a3f917462bb2`，
  共享契约集成提交 `6114ea2a43bb5d532c09faf0c09ddcb5132e052a`：
  只读 Telegram Adapter、4并发 Runner、独立 mTLS 连接池和显式启动开关。
  HTTP 适配器消费 Control 所有的共享 DTO 和校验器，保留本地任务、配置、时间和凭据围栏，
  不另行维护私有 wire 结构或第二套8项检查语义。
- Web 实现提交 `b36104094dcb1a508ee719c9e796272544ba2509`：
  独立预检客户端、202回执、120秒有界轮询、原 key 恢复、MEMBER分享只读、
  8项检查及配置/投递边界说明；同时优化名称保存、固定目标显示和双开关影响说明。

### 11.2 已执行的源码验证

- Control/API：39个有测试包、1358项测试、0测试跳过、0失败，真实 PostgreSQL、
  Session 和 mTLS；副本回滚恢复基线37包1075项。无测试文件的包单独统计。
- Gateway 与 Control 首次契约集成：69个有测试包、2554项测试、0测试跳过、0失败，
  包括实际启动 Control 二进制的跨进程测试；该测试覆盖真实 Session、PostgreSQL、mTLS、
  Gateway HTTP Adapter/Service、完成重放与公开查询，**TelegramProbe 为测试替身**。
  此结果不等于真实 Telegram 调用，也不代替部署后 Runner 生命周期验收。
- Web：38文件525项测试、类型检查和生产构建通过。
  第一阶段仅前端增量的副本回滚恢复36文件457项；整合后阶段以已提交525项为基线，
  两个不同范围的回滚统计不得混写。

### 11.3 部署和真实验收单独记录

运行版本取决于实际构建提交、镜像 ID、容器启动状态和配对配置，而非源码已合并。
部署报告须记录远端 main 包含关系、Web工作树拉取后的提交、3个镜像的源码标签，
并比较部署前后 PostgreSQL、NATS、Ingress 的实例身份，避免只凭健康检查宣称版本一致。

预检启用要求 Control workload consumers 含 `telegram_preflight`，Gateway 显式设置
`GATEWAY_TELEGRAM_PREFLIGHT_ENABLED=true`。旧 Control 不接受新增 consumer；
回滚必须同时还原旧镜像和旧配置引用，保留 additive 数据库表及原有数据。

部署后使用真实账号密码经 Web BFF 和 Control 公共入口分别验证202、权限、CAS、幂等和终态；
再通过浏览器核对实际结果、分享只读和移动布局。真实机器人只执行 `getMe`、`getWebhookInfo`，
检查前后核对账号/凭据/路由/运行账本及远端 Webhook，不调用注册、删除或发送方法。
公网 origin 格式检查失败、缺 Token 等诊断可以正确终止为 COMPLETED / FAIL；
这表示发现接入问题，不是测试驱动失败，也不是实际消息投递成功。

运行验收报告必须保留具体时间、版本、请求与结果，不用本节的源码测试结论替代现场证据。
