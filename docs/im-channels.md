# IM 接入与社区扩展

企业微信智能机器人和飞书企业自建应用均通过 `channels.TextAdapter` 接入公共文本消费者。当前支持单进程、每类通道一个静态绑定、单聊纯文本和一条最终回复。配置见[本地部署](local-deployment.md)，验证范围见[实现与验证](acceptance.md)。群聊、Webhook、媒体、卡片、撤回及生产重试方案见[IM 差异设计](im-channels.md#协议细节与扩展设计)。

## 职责与依赖

| 组件 | 职责 | 依赖边界 |
| --- | --- | --- |
| 平台适配器 | 平台认证、事件解析、身份映射、回复目标、字节上限和发送结果分类 | 不直接调用 Runner 或访问 Inbox/Outbox，不依赖公共消费者 |
| 公共文本消费者 | 持久受理、去重、恢复、Session Run、最终文本收集、截断和 Outbox 发送 | 不导入企微/飞书，不解析平台目标载荷 |
| 启动入口 | 静态绑定、凭据授权、依赖注入、连接及消费者生命周期 | 从服务端配置确定 Tenant/App/Binding/Channel |

平台包与公共消费者共同依赖 [`channels`](../trpcservice/channels/adapter.go) 契约，由 [`cmd/trpc-service`](../cmd/trpc-service) 组装。新增通道只需平台包、配置和组装代码。现有凭据授权包间接引用上游 model/event 类型，因此该边界不等同于完全独立于 Agent 框架的 SDK，也不提供不可信插件沙箱。

## 消息转换与持久化

1. 适配器认证平台连接，筛选支持的消息，生成 `InboundEnvelope`。Tenant/App/Binding 来自服务端配置；外部事件只能在该绑定内映射用户和会话。
2. 公共消费者复核 Tenant/App/Binding/Channel，在事务内按 `(tenant_id, channel_binding_id, external_event_id)` 去重并创建 Inbox/Run。重复投递返回原 `request_id`。持久回调成功只表示已落库或已存在。
3. 串行任务循环在 SQL 的 LIMIT、认领和恢复前限定 Tenant/Binding；使用 claim token CAS 保护状态更新。同 Session 忙时在执行预算内等待，已标记开始执行的任务不再 Yield 或重新调用 Runner。
4. [`sessionrun`](../trpcservice/sessionrun/sessionrun.go) 把规范文本转成 `model.NewUserMessage`，调用真实 `runner.Runner.Run`。完整排空 Event，只选择完成、无错误且无 Tool 调用的 assistant 文本；工具结果、推理字段和 runner completion 不作为 IM 回复。
5. 最终文本在写 Outbox 前清洗并按 UTF-8 字节截断，溢出带 `[truncated]`。Run 终态与最多一个 Outbox 原子提交，适配器发送已存正文并分类平台结果。

平台目标是带版本的不透明数据，只由对应适配器解析。SQL/SDK 错误经固定错误边界脱敏；正文、凭据、外部账号和目标 JSON 不进入日志或遥测。

## 两类通道

| 能力 | 企业微信智能机器人 | 飞书企业自建应用 |
| --- | --- | --- |
| 连接 | 固定 `wss://openws.work.weixin.qq.com`，Bot ID/Secret 订阅、心跳与重连 | 官方 Go SDK `v3.11.0` 长连接，App ID/Secret 认证 |
| 身份验证 | 核对消息 Bot ID 与受信任连接一致 | 启动查询 Bot 和企业身份；核对事件 header App/企业与 sender 企业 |
| 持久受理确认 | 无等价入站 ACK，不假定断线后一定补投 | 官方 SDK 在处理器返回后确认；持久受理成功才返回 nil |
| 去重键 | 平台 `msgid` | 平台 `message_id`，不只依赖事件 `event_id` |
| 回复 | 同连接 `aibot_respond_msg`，固定 `stream.id`，一次 `finish=true` | 按原始 `message_id` 调用回复 API，一次最终文本 |
| 成功判据 | 回执显式 `errcode=0` | HTTP 2xx、显式 `code=0` 且平台消息 ID 有效 |
| 文本上限 | 20480 UTF-8 字节 | 保守限制 4096 UTF-8 字节 |
| 目标有效性 | 包含连接 generation，重连后旧目标失败 | 包含内部 Tenant/App/Binding 与外部 App/企业，不随 WebSocket 换代失效 |

企微协议只参考[官方 SDK](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/README.md)，未引入 Node SDK；Go API 示例见[企微包说明](../trpcservice/channels/wecom/README.md)。飞书依赖固定为 `github.com/larksuite/oapi-sdk-go/v3 v3.11.0`，接收用官方 SDK，回复用固定官方地址的私有 HTTP Client，禁止重定向和发送重试。SDK bootstrap 的凭据请求使用同一禁止重定向策略。

## 账号、用户与会话

绑定由服务端静态配置授权；同进程启用的企微与飞书不能复用同一 `(TenantID, BindingID)`，启动时拒绝冲突。凭据分别通过固定保留引用 `env:TRPC_SERVICE_WECOM_BOT_SECRET` 与 `env:TRPC_SERVICE_FEISHU_APP_SECRET` 解析，不能授权给租户模型。跨进程的账号注册唯一性仍由部署约束保证。

令 `H` 为 JSON 字符串数组的 SHA-256 十六进制摘要。企微单聊使用：

```text
Principal = "p-" + H(["im-principal-v1", tenant, binding, "user", external_user])
Session   = "d-" + H(["im-session-v1", tenant, app, binding, "direct", principal, "", "0"])
```

飞书单聊使用：

```text
Principal = "p-" + H(["im-principal-v1", tenant, app, binding, feishu_app, tenant_key, "user", open_id])
Session   = "d-" + H(["im-session-v1", tenant, app, binding, feishu_app, tenant_key, "p2p", principal, chat_id, "0"])
```

框架 AppName 另以 `t/{tenant}/a/{app}` 限定作用域，不包含 Revision；Session Pin 在首轮确定版本。相同外部用户跨租户、应用或绑定不会进入同一框架会话。群聊的共享群/按成员隔离及跨群策略见[Session 命名](architecture.md#54-session-命名)，当前适配器不接收群聊。

## 恢复与平台限制

| 触发条件与影响 | 当前处理和监测 | 生产缓解与残余风险 |
| --- | --- | --- |
| 受理前断线或进程退出导致消息未落库 | 观察连接和持久化失败；未受理消息由用户重试 | 评估平台补投/回放能力；不能保证无限重投或最终送达 |
| 飞书受理超过 2 秒预算 | 最多 16 个回调等待、串行受理；实际受理错误结束服务，停止时排空 SDK 回调 | 按容量配置预算、监控数据库并由进程管理器恢复；平台有限补投可能耗尽 |
| 平台乱序或并发回调 | 仅承诺串行持久受理后的顺序 | 有可信序号时可做有限重排；无法恢复没有序号的严格原始顺序 |
| Agent 启动后中断、状态未知 | `first_execution_started_at` 非空的恢复任务明确失败，不重跑模型或 Tool | 生产需结果对账和业务幂等；不提供已生成但未存答案的重建 |
| 发送失败或结果未知 | 最多一次发送尝试；Unknown 终结并记录 `duplicate_risk`，不重发、不重跑 Agent | 平台查询与幂等能力决定对账方案；不承诺 exactly-once |
| 凭据获取失败、限流、Token 失效 | 飞书每次发送获取 Token；获取失败为 Unknown，明确业务拒绝为 Rejected | 可按平台规则做缓存与限频；当前串行执行不是限频器，Unknown 不区分未发送与可能已发送 |
| 同一飞书 App 多进程或额外订阅事件 | 部署仅运行一个实例，只订阅 `im.message.receive_v1` | 多连接可能分摊消息，未注册事件可能被 SDK 拒绝；生产需账号注册和订阅治理 |

飞书 Token 与回复共用公共消费者的 15 秒发送 Context。其回复 UUID 由完整绑定与原消息派生，平台去重窗口有限；当前只有一个回复分片，未来若拆分必须把分片身份纳入去重键。停止时先取消并等待连接和消费者，再关闭 Runtime、Session、数据库与遥测。

图片/文件、混合消息、卡片和撤回不进入当前执行链路。生产媒体由 Adapter 下载/解密、验证大小和类型、保存受租户保护的附件引用；撤回需关联原消息并保留 Audit，不自动撤销已执行 Tool。限频、回复有效期和跨连接恢复按平台能力核实，详细方案见[IM 差异](im-channels.md#协议细节与扩展设计)。

## 社区接入契约

```go
type TextAdapter interface {
    Identity() BindingIdentity
    ReplyTextLimit() int
    Serve(context.Context, AcceptFunc) error
    Send(context.Context, DeliveryTarget, OutboundMessage) DeliveryReport
}
```

- `Identity` 返回已校验的内部绑定，公共消费者独立比较预期身份；其中不携带平台凭据。
- `ReplyTextLimit` 返回 UTF-8 字节上限，公共层在持久化前处理长度；平台发送边界再次校验。
- `Serve` 先认证、筛选和规范化，再调用持久受理回调。回调错误应终止服务并返回原错误，由公共消费者脱敏；支持 ACK 的平台不能提前确认。`Serve` 必须响应取消，在所有已启动回调退出后返回。
- `Send` 校验目标版本和完整绑定，原样发送持久正文或拒绝，不能调用 Agent、改写正文或自行重试。

| 平台结果 | 公共发送结果 | Outbox 终态 |
| --- | --- | --- |
| `Delivered` | `succeeded` | `sent` |
| `DeliveryTargetStale` / `DeliveryRejected` | `permanent` | `failed` |
| `DeliveryUnknown` 或非法结果 | `outcome_unknown` | `failed`，保留重复风险 |

新增 Telegram 等通道时，在独立包使用官方协议库实现四个方法，使用合法小写 `ChannelType` 标识。通过启动代码注入现有 Store、Session Run 和 Revision 检查，无需复制公共消费者、修改数据库结构或增加平台分支。平台特有群聊/媒体能力应有显式契约，不应丢弃附件后把剩余文本送给 Runner。

验证至少覆盖正常收发、重复消息、绑定错配、代表性发送失败和取消；复用 [`text` 集成测试](../trpcservice/channels/text/e2e_integration_test.go) 的公共链路结构。模拟平台测试与真实平台证据分别记录。

## 协议细节与扩展设计

| 能力 | 企业微信智能机器人长连接 | 飞书事件订阅 |
| --- | --- | --- |
| 入站方式 | 主动连接 `wss://openws.work.weixin.qq.com`，发送 `aibot_subscribe`；接收 `aibot_msg_callback` | 当前采用自建应用官方 Go SDK 长连接；HTTPS Webhook 保留为扩展设计 |
| 安全 | Bot ID/Secret 认证；校验事件 `aibotid` 与受信任连接绑定一致，不用自建应用 access token | App ID/Secret 认证查询企业身份；核对事件 App/企业和 sender 企业，静态绑定内部 Tenant/App；Webhook 签名/Token 方案见下文 |
| 入站确认 | 推送帧没有 HTTP 响应；只有 Inbox 事务提交后才在平台内部标记受理，不假定断连后必然补投 | SDK 在处理器返回后 ACK；持久受理成功才返回 nil，回调串行受理且有限等待，停止时排空；验证范围见实现与验证 |
| 幂等与回复关联 | `body.msgid` 作入站事件键；`headers.req_id` 用于回复关联，不等同于平台 `request_id` | `im.message.receive_v1` 按 `message_id` 在 Binding 内去重，不能只依赖 `event_id`；message/chat/thread 标识用于回复定位 |
| 身份与会话 | `from.userid`、`chattype`、群聊 `chatid`，机器人账号先绑定 Tenant/App | App、Chat、User、Thread 共同决定绑定和会话作用域 |
| 文本回复 | `aibot_respond_msg` 透传 `req_id`，以固定 `stream.id` 发送最终文本；收到成功回执再确认 Outbox | Bot 消息 API 按 message/chat ID 回复；平台流式/卡片更新能力与普通文本分开 |
| 限制与失败 | 官方 SDK 的流式文本上限为 20,480 字节；按具体消息类型限制输出，同一 `req_id` 串行发送，超时记录结果未知 | 当前采用保守 4096 UTF-8 字节最终文本；单次 HTTP 回复，未知不重发；生产按消息类型配置限频/退避 |
| 扩展与撤回 | 图片/文件需下载解密和媒体上传，卡片、欢迎语、主动发送有独立协议；当前均未实现 | 图片/文件、富文本、交互卡片和撤回分别映射事件与出站动作，未支持时明确拒绝或记录 |
| 当前状态 | 单静态 Binding、单聊纯文本已实现，真实正常收发已验证；增量流、群聊、媒体、卡片和撤回仍为设计 | [飞书接入](im-channels.md)已实现，身份预检、本地协议与 PostgreSQL 集成通过；真实连接及两轮正常单聊收发已验证 |

协议依据为[企微官方 SDK README](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/README.md) 和 [WebSocket 实现](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/ws.ts)，2026-09-07 核对；这里只参考协议，不将 Node SDK 引入 Go 服务。自建应用 Webhook 的 `msg_signature`/AES、HTTP 200 和 access token 发送流程是另一种接入模式，不与智能机器人长连接混用。未核实的平台回复有效期、跨连接重试能力和限频数值保持待联调，不能据 SDK 的本地请求超时推定平台保证。

当前企微超长文本按 UTF-8 字节截断并带 `[truncated]` 标记，只发送一次 `finish=true`，不是逐字流式。生产限频设计在外部账号作用域分配配额、保持同会话顺序，按消息/API 类型配置额度，对已确认可重试的限流采用有上限的指数退避并遵守平台有效等待值；当前没有限频调度器，也不因发送失败自动重发。媒体下载/解密、受租户保护的附件引用和出站上传由 Adapter 负责；当前不支持的消息不进入 Runner。各项降级和待核实边界见[平台限制](im-channels.md#恢复与平台限制)。

**飞书 HTTPS 回调扩展设计，未实现。** 当前实际接入采用[官方长连接](im-channels.md)，以下 Webhook 方案继续用于通道差异和扩展设计。入口拟为 `POST /channels/feishu/{account_id}/events`，每个飞书应用只登记一个回调 URL。路径中的账号 ID 仅定位服务端已登记的候选 Binding 和凭据引用（Verification Token、可选 Encrypt Key、预期 App ID 及允许的飞书 tenant_key），不是租户认证结果；不依赖请求体声明的 App/Tenant 去寻找任意密钥。

处理顺序如下，所有鉴权前解析和解密都不触发 Inbox、Runner 或业务路由：

1. 按入口定位候选凭据，限制请求体大小并保留原始 body 字节。根据服务端配置解析明文或解密 `encrypt` 包装，识别事件类型；配置了 Encrypt Key 却收到普通明文事件时拒绝降级。解析出的身份此时仍不可信。
2. `url_verification` 是独立分支：必要解密后校验顶层 `token` 与该账号的 Verification Token 一致，再在官方要求的 1 秒内返回 `{"challenge":"原值"}`；不创建 Run。官方 challenge 示例没有 App ID/tenant_key，不要求这些字段，也不把普通事件的签名要求套到 challenge 上。
3. 普通事件配置了 Encrypt Key 时，必须用 `X-Lark-Request-Timestamp`、`X-Lark-Request-Nonce`、Key 和**原始 body** 按顺序计算 SHA-256，并核对 `X-Lark-Signature`；缺失或不匹配即拒绝。不能对重新序列化或解密后的 JSON 验签。签名计算本身不依赖解密，可在签名头齐备时提前执行；仅解密成功不能跳过验签。
4. 普通事件未配置 Encrypt Key 时，仍须校验 Verification Token，不允许因 SDK 跳过签名就放行。对本设计采用的 v2.0 消息事件，无论是否加密都显式核对 `header.token`、`header.app_id` 和已登记的 `header.tenant_key`；App/租户不符或无法唯一定位有效 Binding 即拒绝。通过后才按受信任的 Binding 及 Chat/User/Thread 映射平台 Tenant、App、Session，并校验允许的事件类型。
5. 消息内容通过验证后，以 Binding 内的 `message_id` 持久去重，Inbox 提交成功或命中已提交记录后再确认受理；不等待 Agent 执行完成。签名校验不替代持久去重，Token 和原始消息体也不写入日志。

以上依据为 2026-09-07 核对的飞书官方[Webhook 配置与 challenge](https://open.feishu.cn/document/ukTMukTMukTM/uYDNxYjL2QTM24iN0EjN/event-subscription-configure-/choose-a-subscription-mode/send-notifications-to-developers-server)、[事件安全校验与解密](https://open.feishu.cn/document/ukTMukTMukTM/uYDNxYjL2QTM24iN0EjN/event-subscription-configure-/encrypt-key-encryption-configuration-case)和[接收消息事件](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/events/receive)。官方 Go SDK 固定版本 [`b059ee1` 的 Dispatcher](https://github.com/larksuite/oapi-sdk-go/blob/b059ee1824d45444306559b5c33c3f268c0de10d/event/dispatcher/dispatcher.go)会先解析/解密、对 challenge 跳过签名、无 Encrypt Key 时直接跳过签名，并仅在 challenge 分支校验 Token；普通事件的 Token/App/租户绑定校验仍由平台负责，不能只构造 SDK Dispatcher 就宣称完成认证。

**撤回策略尚未实现。** 设计上，收到已验证的撤回事件时，记录对原消息和 Run 的关联，不删除已经提交的 Audit 或 Session Event；撤回也不自动抵消已执行的 Tool。通道支持撤回机器人回复时，经 Outbox 提交撤回动作，否则按租户策略忽略或发送更正说明。当前支持的消息类型见[IM 接入指南](im-channels.md)。

设计上两个 Adapter 共享统一 InboundEnvelope 和 Outbox，重复投递由 PostgreSQL Inbox 唯一约束裁决，Redis 不参与权威去重。企微出站目标保存版本、Binding、收到的 `req_id`、会话引用和稳定 `stream.id`；飞书按原始 `message_id` 回复，目标携带完整绑定作用域，不随 WebSocket 换代失效。敏感引用不写日志。ACK 超时或连接断开不算成功，也不重跑 Agent；生产扩展只有确认平台允许且目标仍有效时才重试，否则记录结果未知或投递失败。当前企微每条最终回复最多一次发送尝试，未知结果保留 `duplicate_risk` 且不重发，连接换代后旧目标失败。平台不提供幂等保证时，稳定 ID 仅用于关联，不能宣称发送 exactly-once。

单聊按 Binding 与可信用户映射 Session，群聊按 Binding、群和显式线程划分，规则见[Session 命名](architecture.md#54-session-命名)。当前部署限制同一 Bot 一个活动实例。企微连接可能互相替换，断线重连停止旧连接的待回执等待；飞书多个连接可能分摊事件，SDK 回调并发不保证原始时间顺序。Inbox 提交前的进程故障可能丢失尚未持久化的帧；飞书虽有超时重推，也不能作为无限恢复保证。监测连接与持久化失败、明确提示用户重试，生产可评估平台回放能力或本地持久接收层。该残余风险不因采用长连接而自动消失。
