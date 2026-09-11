# Telegram 接入预检 V1：Gateway 执行设计

- 日期：2026-09-07；状态：**Gateway 独立实现已发布，共享契约已整合；真实 Control 进程互操作已通过，完整回归与协调验收单独记录**。
- 工作树：`/Users/jfs/Projects/trpc-agent-service-channel-gateway`；分支：`codex/channel-gateway`。
- 代码审查基线：`661e4a826ce8f539d8f610b3f1ef99cdbe4a4fdd`。
- 本文只定义 Gateway 的 Module、Interface、Adapter、执行规则及验收项；不创建新工作负载，不改变已合并的正常接入链路。

## 1. 契约拥有方与本次交付

[产品设计](../channel-preflight-v1.md)由协调任务维护；[Control wire 设计](../control-api/telegram-preflight-v1.md)是公开及私有 wire、固定检查项、错误码、状态与配额的唯一来源。两份文档在各自工作树维护，本文使用最终仓库相对路径；Gateway独立分支暂不复制其文件，串行合并Control和产品文档后再验证这些链接可解析。

本文引用 Control 文档 §3–§9，不复制整份 JSON，不单独扩字段或维护另一版 shared DTO。Control 拥有任务授权、排队、租约、版本校验、结果存储、整体 outcome、过期与配额；Gateway 只领取并执行一次有界、只读诊断。

**执行前提是已保存且 disabled 的 Telegram ChannelAccount；不要求 Binding、Deployment、RuntimeManifest、运行目录 READY 或真实 Worker。** 预检完成不等于账户启用、Webhook 接管或真实消息投递。现有启用操作不强制要求预检 PASS。

Gateway 的本地应用、Adapter、Runner、Bootstrap 与 Compose 开关已按本次冻结契约实现；Control 迁移/Schema/Handler 已由其拥有方正式交付，真实跨端与Bot验收由协调任务推进，本文不把局部实现计为整体完成。

## 2. Module 与代码落点

预检归属现有 Connection Module；不增加第五个业务子领域。Go Telegram SDK 仍进程内导入，SDK 类型不穿过应用 Interface；现有顶层 `platform` Connector 包保持公开复用职责，预检任务编排和 Control 授权适配留在 Gateway 内部，不为一项诊断新增公共框架。

以下路径已在Gateway工作树落盘，测试文件随实现维护：

```text
services/channel-gateway/internal/
├── connection/application/preflight/
│   ├── service.go              # 一个已领取任务：元数据检查、只读检查、结果组装
│   ├── runner.go               # 共享领取调度、槽位、deadline、完成重报与停止
│   ├── types.go                # 应用拥有的小 Interface、任务/结果/稳定错误
│   ├── config.go               # origin 静态规范化与诊断配置 JCS 指纹
│   ├── service_test.go
│   ├── runner_test.go
│   ├── config_test.go
│   └── shared_contract_test.go # 应用输出与真实共享契约的跨层一致性回归
├── connection/adapter/outbound/
│   ├── controlhttp/
│   │   ├── preflight.go        # 适配唯一 claim/resolve/complete wire
│   │   ├── preflight_test.go
│   │   └── preflight_joint_integration_test.go # 真实 Control 进程/PG/Session/mTLS 互操作
│   └── telegrampreflight/
│       ├── client.go           # 私有 SDK；只暴露一次 Inspect
│       ├── transport.go        # 方法白名单、固定出网、大小/时间/日志限制
│       └── client_test.go
└── bootstrap/
    ├── preflight.go            # 装配并接入进程生命周期
    └── preflight_test.go
```

Bootstrap 创建 Adapter 并注入应用；Adapter 依赖应用拥有的 Interface，应用不导入 SDK、Control 内部包或数据库实现。建议本地 seam：

- `Control`：`Claim`、`ResolveBotToken`、`Complete`；本地强类型与 wire 映射集中在 Adapter。
- `TelegramProbe`：`Inspect(context.Context, ProbeRequest)`；Adapter 内先确认身份再读 Webhook，返回已去秘密的受限观测。
- Runner 调用 Service 执行单任务；测试通过以上相同 seam 注入假的 Control、Telegram 与时间源，验证行为而不是复制实现。

不复用 registration 的 `Remote.Identity/Register` 作为预检 Interface；它暴露写入能力。正常 `accountuse`、`AccountUsePermit` 和 disabled 守卫保持原样，诊断不经过运行注册循环，也不安装 Handler。

## 3. 接口与授权接入

三个私有操作完全适配 Control §5：

| 操作 | 私有路径 | Gateway 行为 |
|---|---|---|
| Claim | `POST /internal/v1/channel-preflights:claim` | 先取得执行槽；limit=1；发送摘要及 expected_public_origin/origin_status；接受 200 grant 或 204 空决定 |
| Resolve | `POST /internal/v1/channel-preflights/{preflight_id}/credentials:resolve` | 只凭当前诊断 claim 解析固定 Token；请求没有可选择的 uses/purpose |
| Complete | `POST /internal/v1/channel-preflights/{preflight_id}:complete` | 发送固定八项检查、同一 observed_at 和配置快照；接受 204 回执 |

mTLS principal 必须允许 diagnostic consumer kind=`telegram_preflight`；数据库凭据 purpose 仍是 `telegram.bot_token`。Gateway 校验 grant 的 scope/source epoch、provider、任务身份、配置摘要、期限与精确凭据元数据；不允许诊断 claim 转成运行 Permit，不申请 WebhookSecret 明文。

`claim_token` 每次新领取尝试用 CSPRNG 生成 32 字节，base64url 无 padding，共 43 字符；`claim_request_id` 为 UUID，`instance_epoch` 每次进程启动重建。Token 只存在于短时内存和必要的 mTLS 请求，不进入日志、结果或任务持久缓存。

Control claim/resolve/首次 complete 持续复核 requested_by 为 ACTIVE 用户、ACTIVE 租户 OWNER。发现撤权、降级、禁用时任务 STALE，reason=`CHANNEL_PREFLIGHT_REQUESTER_REVOKED`；Gateway 收到拒绝即停止该授权下的后续解析与探测，不拿新版本 Token 补救。正常 Session 过期/退出不撤销已授权任务；已 STALE 不因恢复 OWNER 复活。本版不新增 membership generation，不承诺观测两次检查间撤权又恢复的全部中间状态。

已接受的同 claim 同摘要完成重放是历史回执确认：Control 先认证原 mTLS+claim，再查已完成回执，命中返回 204；只有首次完成才执行持续 OWNER、Tenant ACTIVE、lease 和版本检查。Gateway 不把重报当作重新授权或重新探测。

Control HTTP Adapter 保留现有 mTLS、固定 HTTPS origin、禁重定向、有界响应和脱敏策略，使用独立连接池以免诊断占满正常目录/注册连接。新方法保留预检闭合错误分类，不能把所有 409 压成普通版本错误。Claim/Resolve 请求≤4KiB，Resolve 响应≤20KiB，Complete 请求≤16KiB；其余响应也必须有显式上限。

共享契约接入直接导入 `api/schemas/channel/v1` 的 `PreflightClaimRequest`、`PreflightGrant`、`PreflightResolveRequest`、`PreflightResolveResponse`、`PreflightCompleteRequest` 和 `PreflightCheck`。请求调用 `Validate`，响应调用 `Decode`，固定八项检查调用 `ValidatePreflightChecks`；Adapter 不再维护镜像 DTO、反射式 JSON 防火墙或另一套八项语义校验。应用的 Grant/Result 仍是应用类型，不依赖 HTTP wire；转换集中在 Adapter。共享 Schema 负责闭合字段、重复键、大小写别名、值域及协议内部语义，Gateway 继续负责与本地领取请求、配置快照、凭据版本和租约的精确比对。

单次 HTTP 的 5s 子期限到期但父任务预算仍有效时，Adapter 返回可重试的 `ErrUnavailable`，由 Runner 原样重传 claim/complete；父 context 取消或过期才映射 `ErrExpired`。真实 lease-expired 409 仍直接结束旧领取，不混淆 HTTP 尝试期限与任务期限。

## 4. 执行顺序与固定检查结果

1. Service 从 grant 产生 `credential_configuration`，从启动配置快照产生 `public_origin`；两个凭据均缺失时优先 BOT_TOKEN_MISSING。
2. Token 未配置：跳过 resolve 与两个 Telegram 请求；仍返回静态 origin 检查和固定能力说明。只缺 WebhookSecret：配置项 FAIL，但已有 Token 仍可用于只读检查。
3. 诊断 resolve 出现503或传输超时则停止该claim，不伪造PROVIDER检查，交由Control有界重领/超时；401/403/409立即停止。诊断 resolve 成功后，再核对 task、connection revision、credential ID/version、purpose、lease 与本地预算。连接、Token 或租约变化时不自动取最新值继续。
4. Adapter 构造 SDK 时零网络请求；显式调用一次 getMe，验证 `is_bot=true` 且规范十进制 ID 与保存的 provider_account_id 完全一致。名称和 username 不参与匹配，不回显错误 Token 指向的其他机器人身份。
5. 身份成立后显式调用一次 getWebhookInfo。**origin 无效也继续读取**；身份未成立才跳过该请求。
6. Adapter 只在内存比较旧 URL 与规范 origin+grant.webhook_path；按受限字段返回结果。Service 完成固定八项后上报，Control 计算 overall。

检查项的 code/status/details 必须逐字遵守 Control §7；以下只说明行为：

| 顺序 / id | 执行或展示规则 |
|---|---|
| 1 credential_configuration | Token/secret 配置布尔值；缺失给 FAIL，不传秘密 |
| 2 bot_identity | MATCH/PASS；ID不匹配、本地Token语法拒绝或401为FAIL；网络/超时/429/不可用/无效响应为UNKNOWN；未执行为SKIPPED |
| 3 public_origin | 仅静态检查：STATIC_VALID/PASS，INVALID或NOT_PUBLIC/FAIL；validation=STATIC_ONLY |
| 4 webhook_registration | 相同URL为MATCH/PASS；不同或未注册为WARN；无法比较为UNKNOWN；401为TOKEN_REJECTED/FAIL |
| 5 pending_updates | 非负安全整数；0为PASS，大于0为WARN；未取得有效响应为SKIPPED |
| 6 delivery_errors | 仅布尔值与UTC时间；无记录为PASS，有历史记录为WARN；未取得有效响应为SKIPPED |
| 7 recovery_materials | 固定RECOVERY_MATERIALS_UNAVAILABLE/UNKNOWN，secret_token_readable=false，restore_available=false |
| 8 delivery_verification | 固定DELIVERY_NOT_TESTED/UNKNOWN，verification=NOT_TESTED |

必须覆盖的分支：

- origin 无效、旧 URL 存在：`WEBHOOK_COMPARISON_UNAVAILABLE/UNKNOWN`，presence=true、relation=UNKNOWN；不误报 DIFFERENT，积压和历史错误独立展示。
- origin 无效、旧 URL 为空：仍 `WEBHOOK_NONE/WARN`，presence=false、relation=NONE。
- getMe 成功后 Token 被外部撤销，getWebhookInfo 返回401：第四项 `TOKEN_REJECTED/FAIL`、presence=null、relation=UNKNOWN；第五、六项 `NOT_EXECUTED/SKIPPED`。
- 本地Token语法拒绝产生TOKEN_REJECTED、零网络；它不是实测HTTP401。
- 429 不在任务内睡眠重试；网络错误不是 Token 无效；超限、截断、损坏 JSON、非法字段值归 `PROVIDER_RESPONSE_INVALID`。403/5xx 等不可用响应不冒充401凭据结论。
- Control 仅聚合前六项：FAIL > UNKNOWN/SKIPPED > WARN > PASS。最后两项固定UNKNOWN必须保留；overall PASS也不表示真实投递成功或旧secret可恢复。

## 5. 只读 Adapter 与 SDK 细节

已按本仓库固定的 `github.com/go-telegram/bot v1.25.0` 源码核对：`bot.New` 默认 getMe，`WithHTTPClient` 的第一个参数仅是 polling timeout，`rawRequest` 默认读取 body 无大小上限，部分错误/debug handler 含原始响应，`Bot.Close` 是远端方法而非本地回收。

因此新 Adapter 必须：

- 构造显式 `WithSkipGetMe`；关闭 debug 并安装不输出外部原文的 error/debug handlers；给 `http.Client.Timeout` 显式设 5s，每次请求同时继承任务 context。
- 对固定 `https://api.telegram.org:443` 只允许 POST 当前 Token 的 getMe/getWebhookInfo 路径；不接受任意 API base URL、query、跳转。测试用内部 transport seam，不把测试 endpoint 变成产品配置。
- Header≤16KiB，Body≤64KiB；在 SDK `io.ReadAll` 前通过 transport 包装检测超限；关闭自动压缩并拒绝压缩响应；不通过截断后解析伪造成功。
- 沿用受控部署环境出网代理策略，代理配置不来自页面、不进入诊断结果或指纹；代理凭据与原始网络错误不记录。
- 不调用 setWebhook、deleteWebhook、getUpdates、sendMessage、Start、StartWebhook、logOut 或远端 close；回收用 `transport.CloseIdleConnections()`。
- 原始 Webhook URL、last_error_message、响应 body、Token-bearing URL 与 SDK err.Error() 留在 Adapter 内，不进入 public DTO、DB、日志、trace 或稳定错误对象。原始 URL 即使只保留 path 也可能泄密，因此不做“删除 query 后展示”。
- 不向旧 URL 或 PublicOrigin 发 DNS/TCP/TLS/HTTP 探测；不发测试消息，不建立 loopback 自验。旧 secret 不可由此次检查恢复，当前保存的 secret 也不是旧注册 secret 一致性的证据。

## 6. 配置快照与可复现指纹

唯一规范见 Control §6。闭合 JCS 对象包含 schema_version、policy_version、scope_id、source_epoch、telegram_api_origin、public_origin、origin_status；使用仓库已有 JCS 库后 SHA-256，不自行编写通用 canonicalizer。

固定 schema_version=1、policy_version=telegram-preflight-v1、telegram_api_origin=https://api.telegram.org。下表向量固定 scope_id=pool、source_epoch=11111111-1111-4111-8111-111111111111：

| public_origin | origin_status | 预期 digest |
|---|---|---|
| https://gateway.example.com | PUBLIC_ORIGIN_STATIC_VALID | sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d |
| null | PUBLIC_ORIGIN_NOT_PUBLIC | sha256:63c09ea92b63a02e1750e76d92f3d6ea76be142d7b6d3dd1b6c590936b9ff58a |
| null | PUBLIC_ORIGIN_INVALID | sha256:03f2b058b89dcf96f3b74459cf4851902e480cb112593a2e253c2d7527b03ed4 |

静态规范化：HTTPS/ASCII host 小写、去默认443端口与根斜杠；因此 `https://GATEWAY.EXAMPLE.COM:443/` 与首行同摘要。禁止 userinfo/query/fragment/业务path、未规范非ASCII主机及编码歧义；端口限443/80/88/8443；拒绝 localhost、单标签名、.local/.localhost和私网/环回/链路本地/保留IP字面量。仅做静态解析，不DNS解析；规范域名仍可能解析到私网，静态PASS不证明公网可达。

非法/非公网输入用 null+稳定分类，不散播原始配置字符串；同类不同非法值可有同摘要。指纹不包含 Token、证书或私钥路径/值、代理凭据、时间、instance epoch 或 lease；它不是完整网络环境指纹，也不是 RuntimeManifest digest。

Claim 同时带摘要、expected_public_origin、origin_status；Control 首次领取即复算并固定。Complete 重用同一快照；重领或首次完成出现不同摘要使任务 STALE，reason=CHANNEL_PREFLIGHT_GATEWAY_CONFIG_CHANGED，不混合多个 Gateway 配置的结果。

Control 没有当前 Gateway 配置注册表：gateway_config_freshness 首次 claim 前为null，之后固定UNCONFIRMED，包括终态；账户 freshness=CURRENT仅表达连接版本和5min窗口。纯改名/描述仅 metadata_changed=true，不让连接诊断失效。启停、Token/WebhookSecret版本、provider/scope等依赖变化按Control规则失效；Binding变化不影响预检。

## 7. Runner、期限、重试和停机

- 任务总期限120s、租约30s、最多2次claim、不续租。并发固定4（以协调任务最终冻结令为准）；取得空闲槽后领取，一次limit=1，无额外无界本地队列。
- 每个 Gateway 实例内部共享调度与速率门控，包含重传在内≤2次claim/s；空队列2s±20%抖动轮询，传输失败2–30s退避，不能让每个槽独立热轮询。
- `t0` 取本次 Claim HTTP 发出前的单调时间；`remaining=min(lease_expires_at-server_time, job_deadline_at-server_time)`；`report_deadline=t0+remaining`；`work_deadline=min(t0+20s, report_deadline-5s)`。Control每次响应包含新的数据库server_time，即使同claim重放也不沿用旧时间；Gateway不依赖墙钟对齐。
- 20s工作预算包含诊断resolve；每个Telegram请求≤5s；每一步开始前核对context和预算。预算不足则零新解析/零新Telegram调用，不延长租约兜底。
- Claim 响应不确定时最多一次同request_id/token原请求重传；不立即用新ID重复领任务。收到204后按轮询间隔使用新ID，不等待5min回执保留期。
- Complete 响应不确定时在剩余报告期限内最多一次原payload重报；保持observed_at、八项结果与配置摘要不变，不重跑探测。明确409即结束旧执行，不覆盖新claim或终态。
- 停机取消执行context、停止新claim并关闭空闲连接；有界清理context可用于最后一次完成重报，但不能延长报告期限或使过期claim有效。
- 已发出的只读远端请求有有界在途窗口；撤权/换版本靠Control在resolve和首次complete再核对，不宣称即时撤回网络请求。
- 无Gateway、进程崩溃或两次租约耗尽由Control维护/GET收敛为TIMED_OUT；Gateway不需要新增持久队列或NATS subject。

## 8. 启动与部署影响

仍只有现有 `channel-gateway` 部署单元；预检Runner与其同进程启停。启用时既有Control mTLS principal增加明确的telegram_preflight权限，装配独立诊断client和默认并发/轮询参数；具体配置字段在代码阶段与Control契约测试一起落地，不自行扩私有wire。

预检不受正常账户 enabled/READY 门控；但整个Gateway现有配置、PostgreSQL、NATS等启动依赖仍存在。本版不创建“无基础设施预检进程”，也不放宽正常启动校验。语法错误origin若使既有Bootstrap拒绝启动，Control如实显示NO_EXECUTOR超时；可启动但非公网的origin由预检静态项报FAIL。未来若需要在配置无效时也运行诊断，另行设计启动策略。

Control模式通过LoadConfig默认启用，GATEWAY_TELEGRAM_PREFLIGHT_ENABLED=false可在Control接口/诊断权限升级前关闭；fixture不启动预检，直接Go构造Config需明确设置TelegramPreflightEnabled。Compose已增加此开关；不新增Node镜像、wecom-connector进程、CronJob或Helm发布任务。Helm继续等所有workload完成后的最终集成阶段。

升级和回滚须将 Control 镜像与 mTLS principal 的 diagnostic consumer kinds 配置成对切换：旧 Control 枚举不接受 `telegram_preflight`，单独回退镜像而保留新权限配置会导致启动失败。测试环境原有 `https://localhost:18443` 按端口优先规则得到 `PUBLIC_ORIGIN_INVALID`；负面验收保留此配置，不通过新增 Tunnel 或改 Webhook 把静态 FAIL 变成 PASS。

## 9. 冻结后验收矩阵

下列是必须实现的测试，不是本次已跑通过的产品测试：

| 编号 | 输入/场景 | 必须观察的结果 |
|---|---|---|
| G01 | disabled且无Binding/Deployment | 预检可执行；普通AccountUsePermit/credential resolve仍拒绝 |
| G02 | 错scope/epoch/claim/token/用途/精确版本 | 零Telegram调用，旧授权不换新Token继续 |
| G03 | Token缺失；仅WebhookSecret缺失 | 前者零resolve/零网络；后者可只读探测但配置FAIL |
| G04 | 正常SDK构造和执行 | 构造零调用；getMe/getWebhookInfo各恰好一次 |
| G05 | getMe错误ID、is_bot=false、401 | 身份FAIL；下游SKIPPED；不回显其他机器人信息 |
| G06 | getMe成功后getWebhookInfo401 | webhook TOKEN_REJECTED/FAIL；积压/错误SKIPPED |
| G07 | invalid origin+有旧URL/无旧URL | COMPARISON_UNAVAILABLE或NONE；有响应时积压/错误仍展示 |
| G08 | 超时/429/网络/403/5xx | 稳定UNKNOWN；无单任务provider重试或误报Token失效 |
| G09 | 重定向/压缩/超大/截断/畸形body | 有界失败；无原文泄漏、无超限成功 |
| G10 | 旧URL为本地/metadata/含秘密地址 | 捕获所有网络调用，证明只请求固定Bot API两方法 |
| G11 | 调用禁用方法、关闭Adapter | transport拒绝；仅本地CloseIdleConnections，无远端close |
| G12 | 固定4槽、空队列、Claim响应丢失 | 槽位和实例限速成立；同ID重放不重复领；无无界队列 |
| G13 | server_time漂移、慢Claim、超期grant | 单调deadline不增长；无新过期探测 |
| G14 | 完成响应丢失、异payload、旧lease | 同结果重报204且时间不变；其余409，不重探测 |
| G15 | request者撤权、Session退出、已完成后撤权 | 未完成STALE停止；Session退出不撤任务；历史同claim同摘要仍204 |
| G16 | metadata-only、启停ABA、Token/secret换版 | 元数据改名不失效；依赖变化STALE，旧结果不冒充当前 |
| G17 | 三指纹向量、大小写/443等价、配置变更 | JCS与Control一致；配置变更STALE；freshness null→UNCONFIRMED |
| G18 | Gateway停机/崩溃/无执行者 | 无泄漏goroutine；有界结束；Control在总期限内收敛 |
| G19 | eight checks各种组合 | 顺序、code/status/details及Control整体计算一致，最后两项始终UNKNOWN |
| G20 | 预检前后运行状态及泄密canary | Account/Binding/Route/catalog/Observation/registration/Inbox/Admission/Outbox/RunRequested不变；日志/结果/trace/receipt无秘密 |

应用及Adapter单测使用本地替身；跨Control验收使用独立真实PG与mTLS，旧接入回归继续跑。真实Telegram验收只在协调授权的已保存disabled测试账户执行两项只读检查，保持Webhook、数据库和运行容器的现有状态约定，不用本次设计核验冒充真实E2E。

## 10. 本次核验与实施起点

设计阶段已核对现有disabled守卫、registration Adapter、SDK v1.25.0、Control最新三私有操作和固定八项检查；已用仓库JCS Go依赖重算三条合成向量，输出 `JCS_VECTORS=PASS count=3`，退出0。该结果只证明示例规范摘要一致；实现证据另外记录，真实Control/Telegram验收不由此推出。

文档核验还包括：Control八段JSON语法解析、三项私有路径与八项检查ID对齐、引用文档存在、正式文件复制后一致性及git diff检查。后续实现已落盘Gateway代码、测试及部署开关；未改已有运行服务或Control拥有的文件。

协调任务已完成契约对齐。Gateway 应用/配置、两个 Adapter 与 Runner/Bootstrap 已有本地验证，独立实现已推送 `codex/channel-gateway` 的 `7e22af0d2cba3f68cee4e208f194a3f917462bb2`。共享契约接入先在独立副本验证实际 Schema/DTO/fixture 快照。Control 正式提交 `c7cf4e184e0355f2da635f0b43d91b18ab580952` 进入远端 main 后，Gateway 正常合入该提交并直接消费正式共享包；60 个契约文件与此前冻结快照逐字一致，Gateway 不另行修改 Control 文件。

共享 Schema 加本地 mTLS 测试替身证明的是 wire 互操作与本地约束，不等于真实 Control Handler/PostgreSQL、Control/Web 联调或 Telegram 验收。最终 main 合并仍由协调任务根据串行验收结论推进。


### 真实 Control 进程互操作回归

`TestPreflightGatewayClientAgainstControlBinary` 从 Gateway 测试启动当前源码构建的 Control 二进制，在专用 PostgreSQL 中创建随机独立 Schema，并生成临时 CA、服务端证书和 Gateway mTLS 身份。测试经真实登录、首次改密和租户 OWNER API 保存 disabled 且没有 Binding/Deployment 的账户，再用实际 Gateway `NewPreflight` 完成 Claim/Resolve、实际应用 Service 组装结果、Complete 和公开 View 读取。

测试覆盖 `https://localhost:18443` 的 INVALID、无端口 loopback 的 NOT_PUBLIC 和合法 origin 三条分支；验证八项结果、同 Complete 历史回执重放、公开结果与进程日志的秘密 canary，以及八张运行态表前后摘要不变。Claim 间隔遵守实例限速；Control 进程通过 SIGTERM 停止并校验退出状态，随后删除该测试 Schema。

此项只以 TelegramProbe 替身提供只读观测，不连接 Telegram，不替代 SDK transport 测试、Runner 调度测试或真实平台验收。执行需要设置 `GATEWAY_TEST_DATABASE_URL`、`GATEWAY_CONTROL_PREFLIGHT_BINARY` 和 `GATEWAY_CONTROL_PREFLIGHT_NATS_FILE`；缺失时明确跳过，不算真实集成通过。NATS 配置文件为 owner-only 的 `{url,user,password}` JSON，指向专用测试 broker 的 Control producer；不要使用现有部署的 broker。完整测试入口应先构建当前源码的 Control 二进制，再注入这三个变量：

```bash
go build -o /tmp/control-api-preflight-test ./services/control-api/cmd/control-api
GATEWAY_CONTROL_PREFLIGHT_BINARY=/tmp/control-api-preflight-test \
  go test -race -count=1 \
  ./services/channel-gateway/internal/connection/adapter/outbound/controlhttp \
  -run '^TestPreflightGatewayClientAgainstControlBinary$'
```

2026-09-07 本次独立进程互操作的三个分支与父测试均通过，退出 0。Control/API 与 Gateway/platform 全量测试、修改前后清单及副本回滚结果保存在本次交付验证记录；部署与 main 合并仍以协调任务结论为准。
