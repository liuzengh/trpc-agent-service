# Channel Gateway 优化设计：从文本接通到多租户可运营

- **日期**：2026-09-08（源码与官方资料核查：2026-09-07；文档复核：2026-09-08）
- **文档状态**：优化提案；描述拟新增行为和实施路径，不表示功能已经实现或验收。
- **核对基线**：`7fc79234853884780b1b99abb12bbd9af56213c0`。
- **工作位置**：Channel Gateway 工作树；本文不修改旧 main 目录、实现代码或运行部署。
- **优化性质**：保留已有四个业务 Module、公开 Go Connector、Control 发布事实、共享执行账本及 Session；补足授权、交互、恢复和运维，不重新搭建一套 Gateway。
- **阅读原则**：每项优化都回答“现状是什么、为什么新增、不做有什么后果、如何实现、由谁负责、如何验收”。
- **协议阶段**：遵循 [架构约束 ARC-000](../constraints.md)：当前开发期直接收敛现有 `/v1` 与 Schema，不为本提案新增 v2 双栈或兼容旧 Reader。资源 revision、策略 epoch 和连接 generation 是运行事实，不等于对外协议版本。

## 目录

1. 优化目标与当前基线
2. 决策原则、术语和职责分工
3. 优化总表：为什么要新增
4. F01 外部身份与租户使用授权
5. F02 群聊触发和输入规范化
6. F03 Session 策略与会话操作
7. F04 Callback、命令和审批交互
8. F05 Progress、流式和卡片
9. F06 图片、文件和 Artifact 引用
10. F07 限流、公平调度和投递恢复
11. F08 路由应用确认和运营诊断
12. F09 Trace、Metrics、审计和脱敏
13. F10 多节点、故障恢复与真实 IM 验收
14. F11 租户灰度、配置回滚与发布流程
15. F12 容量、归档和部署
16. 数据访问、同步与多后端的协作契约
17. 最小数据模型与事务示例
18. Interface、协议、HTTP 和代码落点
19. 分阶段实施与验收矩阵
20. 待确认决策、取舍和引用

## 1. 优化目标与当前基线

### 1.1 为什么这是优化，而不是重新开发 Channel

当前已完成的核心价值是：外部输入经过可信渠道身份识别后，固定租户和已发布运行目标，持久接纳；Worker 执行后由 Gateway 记录并投递 Final。新增功能不能破坏这些已建立的事实边界。

但“收到消息并回复文本”只证明协议和基础链路可用，还不能回答：

- 能找到 Bot 的人是否都有权使用租户的 Agent？
- 同一个群的多名用户是否共享历史？谁有权重置它？
- 按钮被其他人点击、重复点击或过期后，是否还会执行工具？
- 限流、断线、结果未知时，系统是等待、拒绝，还是需要人工确认？
- 管理员看到 READY，是已连接、路由已应用，还是消息已经送达？
- 多租户同时使用时，热点租户会不会占满连接、数据库或投递容量？

**本优化把这些隐含假设变成有所有者、可配置、可审计、可验收的产品规则。**

### 1.2 已实现的基础不重复列为新功能

| 基线能力 | 当前事实 | 本次怎样复用 |
| --- | --- | --- |
| 渠道类型 | Telegram、企业微信智能机器人 | 优先完善这两类，不以新增第三个 Provider 代替现有闭环 |
| 部署 | 一个 Go Channel Gateway；四 Module；公开 Go 协议包直接 import | 不拆四个服务，不新增独立 Node Connector |
| Telegram 接收 | webhook + long polling；receiver owner、持久 cursor、切换协调 | 所有新增输入能力复用同一 Admission |
| 企微接收 | 智能机器人长连接、owner/lease/epoch/socket generation | 流式、附件和交互继续遵守连接相关性 |
| 账户和绑定 | ChannelAccount、ChannelBinding、固定 DeploymentRevision | 策略与身份不从消息正文推导租户或运行目标 |
| 凭据及预检 | 托管凭据、用途/版本校验、两类接入验证 | 不把 Secret 下发到 Web 查询结果、日志或业务事件 |
| 入站事实 | Receipt、SourceDigest、Inbox、Admission、RunRequested Outbox | 重复消息仍永久复用首次决定，不因策略更新重跑 |
| 回复事实 | ReplyIntent、committed-Final proof、Delivery ledger、文本 Final | 进度与交互不伪装成正式 Final |
| Worker/Session | 共享 PG 执行账本、租约、正式候选/head 提交 | 不引入按用户固定 Worker 的 sticky session |
| Web | 账户、绑定、凭据、预检和实例连接观测 | 在现有工作台增加可解释的业务状态，而非另起后台 |

代码定位见 [Gateway 服务说明](../../../services/channel-gateway/README.md)、[SessionScope](../../../services/agent-worker/internal/execution/domain/run.go)、[Control 账户模型](../../../services/control-api/internal/channelbinding/domain/account.go)。

### 1.3 已验证与尚需验证必须分开

- Telegram webhook 已有真实 Control 发布、真实模型、正式 Session、Final 和重启/重投的联合证据，见 [Worker W12](../agent-worker/implementation-status.md)。该记录包含修复前一次投递失败，不能改述为四轮所有投递都成功。
- Telegram long polling 已有独立真实接收/恢复记录，见 [双模式记录](telegram-receive-modes-implementation.md)；最终合并版本还应补齐本优化所需的完整 Worker/Session/Final 矩阵。
- 企微已完成真实认证、marker 收发/Final ACK，以及产品预检，见 [WeCom 预检验收](wecom-preflight-v1.md)。这些不等价于企微正式 Binding→Worker→Session→Delivery 全链验收。
- [合入审查](wecom-preflight-main-review.md)记录源码、PG/NATS、Web 和回滚测试；其结果不表示本优化或生产部署已经完成。
- 历史 implementation-status 中的旧切片不作为当前待办的唯一来源；冲突时，以当前源码和明确日期的最新验收记录为准。

## 2. 决策原则、术语和职责分工

### 2.1 本文拟采用的术语

本节是本提案的局部术语表；提案确认后再由主线维护者合入全局 CONTEXT，避免当前文档写入即改变已接受契约。

| 术语 | 含义 | 不应混淆为 |
| --- | --- | --- |
| ExternalPrincipal | 某租户、Provider 和受控身份空间内的外部 IM 使用者 | 租户 OWNER、Bot 身份、用户名 |
| ChannelAccessPolicy | 某渠道允许谁、在哪种会话、执行哪些渠道操作的发布规则 | 模型配置或工具执行器 |
| AdmissionAuthorization | 一次输入接纳时确定的主体、授权版本和范围事实 | 永久授权或允许任意工具的 bearer token |
| TriggerPolicy | 哪些已授权消息应触发 Agent 或交互命令 | 身份认证 |
| SessionPartition | 外部会话内部的共享/按用户隔离规则 | Worker 实例、连接 owner |
| ConversationCommand | 新会话、取消等具有独立幂等身份的操作意图 | 普通 Prompt 或自动产生的 Run |
| InteractionReceipt | 按钮/命令已持久受理的事实 | 审批已通过或工具已执行 |
| DeliveryRecoveryRequest | 对失败投递提出的显式、有审计的恢复请求 | 修改旧 ledger 或重新执行模型 |
| RouteApplicationObservation | Gateway 已验证并持久应用某路由版本的观测 | Control 发布成功、连接 READY |

### 2.2 不变原则

1. 租户来自可信 ChannelAccount 与已发布 Route；输入中的 tenant、角色名称和 URL 不决定租户。
2. 管理权限与机器人使用权限分别判断；外部用户不会自动变成 Control 用户。
3. ExternalEventKey 不增加策略 revision、Session generation 或 DeploymentRevision，重复输入不变成新事件。
4. 入站接纳、模型执行和外部投递是三种事实；各自有 owner、幂等键和失败恢复。
5. Worker 不直接调用 IM Sender；Gateway 不执行模型和工具，不直接读写 Worker/Control 业务表。
6. Provider 结果未知时保留 UNKNOWN；多节点锁也不能提供外部发送 exactly-once。
7. 当前开发期同步更新 v1 Schema/生产者/消费者/fixtures；不假装旧二进制会接受新增必填字段。
8. 配置、Secret 和用户内容分级；治理与审计不成为内容泄漏的新入口。

### 2.3 谁负责什么

| 所有者 | 新增职责 | 明确保留在其他所有者的职责 |
| --- | --- | --- |
| Control | 外部身份映射、策略发布、使用授权撤销、部署/灰度意图、查询授权 | 不在每条消息热路径同步选择绑定；不发送 IM |
| Gateway Routing | 发布策略/目标的运行投影与应用确认 | 不编译 Manifest，不访问 Draft |
| Gateway Admission | 重复事件处理、渠道使用授权、触发判定、可信输入与交互接纳 | 不拥有正式 Session head、工具审批事实 |
| Gateway Connection | Bot 连接/receiver owner、协议能力和原连接 reservation | 不拥有业务重试、租户预算账本 |
| Gateway Delivery | Final/Progress/协议应答的调度、限流、投递证据与恢复 | 不重新执行 Agent、不改变原回复目标 |
| Worker | Session partition/generation、Run/Attempt、工具 Filter、审批 owner、模型 usage | 不持有 IM Token，不接受任意回复地址 |
| Storage/Memory | 正式 Session、Memory、Summary、Artifact、Knowledge 的后端实现与迁移 | 不解析外部 IM 身份 |
| Web | 通过 Control API 编辑和查询；展示真实状态层次 | 不跨过 Control 直连 PG/Gateway 管理端口 |
| Telemetry Collector | 接收和转发观测数据 | 不成为业务授权或持久接纳的前置依赖 |

## 3. 优化总表：为什么要新增

| ID | 优化 | 现状缺口 | 新增价值 / 不做的后果 | 主要所有者 | 阶段 |
| --- | --- | --- | --- | --- | --- |
| F01 | 外部身份与使用授权 | 账户租户归属不等于发消息者有使用权限 | 避免公开 Bot 暴露租户 Agent、数据和成本额度 | Control + Admission + Worker | A |
| F02 | 群聊触发策略 | Telegram 群聊仍 ignore；企微已有群文本解析但产品规则不足 | 避免“放开群聊”变成全群消息进模型 | Admission + Connector + Control | B |
| F03 | Session 分区/新会话 | 当前共享规则隐含、缺用户操作 | 避免群内串历史、并发 reset 不可解释 | Worker + Admission | B，设计前置 |
| F04 | Callback/命令/审批 | interaction 有记录，无完整业务闭环 | 按钮有反馈、审批不可伪造、操作可恢复 | Admission + Delivery + Worker | B，工具审批依赖后置 |
| F05 | Progress/流式/卡片 | 正式链以文本 Final 为主 | 降低长等待的不确定感；不让临时状态覆盖 Final | Worker + Delivery | B |
| F06 | 附件通路 | 非文本缺统一受控引用 | 避免任意下载、跨租户文件访问和内存失控 | Admission + Artifact + Worker + Delivery | B，依赖 Artifact |
| F07 | 限流/公平调度/恢复 | 有分类和保守重试，缺完整租户调度 | 避免一租户拖垮全部 Bot，失败可解释且不重复发 | Delivery + Control | A |
| F08 | 应用确认/业务诊断 | READY、PUBLISHED 不能证明业务链可用 | 减少反复试发；定位消息究竟卡在哪里 | Routing + Control + Web | A |
| F09 | 全链观测/审计/脱敏 | Gateway 尚缺完整 OTel 接线 | 量化延迟和成功率，定位且追责，不泄漏内容 | 全 Workload | A |
| F10 | 多节点和真实 IM 门禁 | 有机制，但跨 Provider 组合证据不完整 | 将水平扩展和故障恢复变为可验证承诺 | Gateway + Worker + 运维 | A 起持续 |
| F11 | 租户灰度/配置回滚 | 双目标用户分桶已接通；结果指标窗口和自动治理未完成 | 发布有边界、回滚不打断既有执行或串会话 | Control + Routing + Worker | C |
| F12 | 容量/归档/生产部署 | 有上限和基本 Compose，缺长期运营模型 | 防止无界累积、错误扩容和不可恢复满额 | Gateway + 数据所有者 + 运维 | A 量测 / C 生产化 |

A = 两种渠道可交付闭环；B = 聊天能力增强；C = 后续生产优化。Helm 只进入全部 Workload 完成后的 FINAL-INTEGRATION，不放入 A/B。

## 4. F01 外部身份与租户使用授权

### 4.1 为什么新增

已有 Bot Token/Webhook Secret/WSS 认证证明消息来自配置的渠道；已有 Account/Binding 证明消息归属哪个租户。它们都没有证明外部发言者能使用该租户的 Agent。尤其公开可发现的 Bot 不能仅凭“能发私信”取得租户数据权限。

本功能补的是使用者授权，不重复实现租户管理员登录体系，也不把 Bot 身份当作用户身份。

### 4.2 设计

- ExternalPrincipal 采用稳定、不透明的平台 ID；绑定唯一键为 `(tenant_id, provider, account_id, external_user_id)`。
- 在当前范围内不自动合并同一 Provider 不同 Bot 的用户；若将来支持跨 Bot/企业身份统一，必须增加已验证的身份空间和显式关联操作。
- `external_user_id` 由经过来源认证的 Provider DTO 提取；昵称、群名、消息正文中的角色不参与授权。
- Principal 与 Control 登录用户可以建立经验证的可选关联，但创建 Principal 不会增加 TenantMember/OWNER。
- Control 维护 `ChannelAccessPolicyRevision`，每次发布产生新 revision 与 digest。内容包括 `access_mode`、允许的主体/群、允许渠道操作、session policy 引用和使用额度引用。
- 初期仅支持 `DENY_ALL`、`ALLOWLIST`、`PUBLIC_LIMITED`。新建策略默认 DENY_ALL；PUBLIC_LIMITED 必须明确选择，使用隔离 Session、低配额且不获得敏感工具能力。
- 开发测试 Bot 通过显式 allowlist 加入测试用户，不使用隐藏的全局 bypass。

示意策略；仅说明拟议字段，不是当前已提供的 HTTP 请求：

```json
{
  "access_mode": "ALLOWLIST",
  "allowed_principal_ids": ["principal_demo"],
  "allowed_conversation_ids": ["conversation_demo"],
  "allowed_operations": ["message.send", "session.new", "run.cancel_own"],
  "session_policy": "per_user_in_conversation",
  "authorization_max_age_ms": 30000,
  "tenant_quota_ref": "quota_demo"
}
```

其中 30 秒是本提案建议的初始新鲜度上限，不是已实现 SLA；要用 F10 测试校准后冻结。主体列表较大时存关系表，运行快照分页带完整性证明，不把无限数组放进单个事件。

### 4.3 热路径和一致性

1. Provider Adapter 完成来源认证和外部事件标识解析。
2. Admission 按 ExternalEventKey 查询既有 Receipt。重复事件按首次解释规则验证摘要并返回原决定，不重新授权或创建 Run。
3. 新事件读取 Gateway 内的已发布路由、主体和策略投影；不逐消息同步查询 Control Draft/绑定。
4. 授权同时校验租户、账户、Principal 状态、群范围、操作范围及投影新鲜度。
5. Admission 在自己的 PG 事务中锁定/比较同库策略投影 fence，固定 policy revision/digest、主体 ID 与路由快照，并提交 Receipt、Admission、Outbox、审计 outbox。
6. Control 更新与 Gateway 接纳不跨库事务。必须公开承认从 Control 撤销到各运行投影更新存在传播窗口，不能宣称“立即全局撤销”。
7. 实例仅能用有界新鲜度内的授权投影接纳新 Run；投影未知/过期时拒绝新接纳，仍可返回既有 Receipt。
8. Worker 接收可信 AdmissionAuthorization，仅作为接纳时事实；执行新 Attempt、危险工具或审批兑现时，还要核对自身已发布授权投影和当前 policy epoch，过期则等待/拒绝。普通工具每次调用不同步访问 Control。

资源管理快照可固定版本，使用者撤销具有更高优先级；两者不混为“旧 Run 永久持有旧授权”。已有外部请求已经开始的效果不由撤销逆转。

### 4.4 拒绝和审计

- `DENIED` 是确定的业务拒绝：写独立原因码和 Receipt，不创建 Run。
- 授权依赖未知是暂时不可判定：保持未接纳，不把临时故障写成永久拒绝；Webhook 返回可重试状态，polling 不越过未持久处理的输入。
- 对已授权渠道可以发送受限的拒绝提示；提示自身有幂等键、限流和内容白名单，不能成为匿名攻击者放大器。
- 审计记录 actor、policy revision、decision、reason；不记录访问策略中的全部用户列表。

### 4.5 验收

同 Bot 的允许/拒绝用户、同外部 ID 的两个租户、群授权撤销、主体撤销、策略过期、权限撤销后的旧事件重投、双 Gateway 并发接纳都必须覆盖。未授权新输入产生零 Run；历史重复输入产生零新增 Run。

### 4.6 与 Worker Filter 治理的协作

**为什么单有渠道授权仍不够**：允许发消息不等于允许调用所有工具；允许一次工具调用也不等于租户还有模型或外部服务预算。渠道层解决“谁能进入”，Worker Filter 解决“本次执行能做什么”。两层的结果都写独立审计，不以 Prompt 中的角色声明替代权限。

建议的治理顺序如下，Filter 名称为设计角色，不表示当前 SDK 已提供同名接口：

| 边界 / Filter | 校验与动作 | 事实所有者 |
| --- | --- | --- |
| 入站准入 | 主体、账户、群、操作范围及已发布策略新鲜度；首次接纳另占租户入站配额 | Admission；额度后端共享 |
| Run admission Filter | 校验受信 AdmissionAuthorization 和运行目标；预留本次最大允许消耗，不把旧接纳当永久授权 | Worker + 预算 owner |
| Before-model Filter | 校验模型权限、剩余额度和超时；按租户策略处理敏感内容；限定单次最大输出 | Worker |
| Before-tool Filter | 求已发布工具能力、租户 allowlist、用户权限的交集；校验参数和目标资源；危险操作进入 F04 审批 | Worker Tools |
| 审批兑现 Filter | 复查最新有效授权、工具参数摘要、一次性审批状态和预算，再放行固定调用 | Worker Tools / Approval owner |
| After-model / After-tool Filter | 按稳定调用 ID 记录实际 usage，结算预算；对待回传内容按策略脱敏 | Worker + 预算 owner |
| Before-IM-render | 校验原目标、租户和已批准的可展示内容，再执行平台转义/拆分 | Delivery；不替代 Worker 的工具权限判断 |

多节点预算不能只保存在 Filter 的进程内计数器中。预算 owner 以稳定 operation ID 在共享后端原子预留，重试命中同一预留；新一次真实模型调用使用新的调用 ID 单独计费。对于 usage 尚未知的调用，先保留保守占额并等待对账，不因超时立即退款。硬预算依赖可约束的最大调用消耗；外部计费只能事后确认的部分，应明确为有界风险或软预算，不宣称实时账单绝对不超限。

这一部分冻结协作契约，具体模型、工具和计费 Filter 随所属 Worker 能力实现；Channel 第一批不因此增加工具执行器或计费微服务。

## 5. F02 群聊触发和输入规范化

### 5.1 为什么新增

当前 Telegram [Normalize](../../../services/channel-gateway/internal/admission/adapter/inbound/telegramadapter/normalize.go)仅接纳真人私聊纯文本；企微 [Handler](../../../services/channel-gateway/internal/admission/adapter/inbound/wecomadapter/handler.go)已有 single/group 的文本解释。把两者描述成“群聊已完整支持”会误导产品；简单移除 Telegram private 判断则会把普通群聊天送给模型。

### 5.2 设计与实现

- 保留两条接收方式共享 Normalize/Admission 的结构，增加显式 `ConversationKind`、可验证 mention/command/reply-to-bot 元数据和 TriggerDecision。
- 分开 Source Authentication、Access Authorization、Trigger Evaluation；“@了 Bot”只是触发条件，不是授权。
- 建议默认：私聊允许普通文本；群聊要求允许群且满足 `mention_bot`、`reply_to_bot` 或 `explicit_command` 至少一个；默认不允许 `all_messages`。
- Telegram 识别目标 Bot 时使用配置并验证过的物理 Bot 身份及消息 entities，不用正文字符串子串匹配昵称。
- 企微仅对协议实际提供、已契约验证的字段执行规则；若没有可验证的 mention 元数据，则该触发能力显示不支持，不猜测文本中的 @。
- Topic ID 纳入会话身份。群迁移事件先记录，再由 Control 的受控操作更新会话关联；不直接把 Provider MigrateError 改写为新的回复地址。
- 服务消息、机器人消息、匿名发送者、编辑/撤回默认只作事件记录；编辑不自动重新执行原 Run。将来需要“按编辑重新运行”时另建显式动作。
- 统一 CapabilitySet 必须区分协议能力、已实现能力、账户已授权能力、最近验证状态；Web 只开启前三项的交集，并受最近验证状态门禁约束，不使用“SDK 有方法”作为可用证明。

### 5.3 幂等和发布注意事项

规范化改变时不改 ExternalEventKey。已有 ignore/interaction Receipt 不因新能力开启重跑。当前开发期如同步调整 SourceDigest 定义，按 ARC-000 明确停止旧消费者并重建专用开发数据，不引入仅为开发数据保留的双 normalizer。稳定发布后的摘要演进须另立迁移契约。

### 5.4 验收

包含 Telegram 私聊/群/超级群/topic，企微单聊/群聊，错误 @目标、未授权群、reply-to-other-user、匿名发言、Bot 消息、同消息在两种接收模式重投。负例不能变成 Prompt。

## 6. F03 Session 策略与会话操作

### 6.1 为什么新增

当前 SessionScope 包含 Tenant、Provider、Account、Conversation、Thread、Binding、DeploymentRevision，不包含 sender。它已隔离跨租户、跨群和不同发布版本，但群内不同用户默认共享一条历史。需要由租户明确选择，而不是藏在 hash 函数中。

### 6.2 拟议规则

```text
base_scope = tenant + provider + account + conversation + thread
             + binding + deployment_revision
partition  = shared | principal_id
session_scope = base_scope + session_policy_id + partition
session_id = stable_hash(session_scope + session_generation)
```

- 新策略默认 `per_user_in_conversation`；租户明确选择 `shared_conversation` 才共享历史。
- 默认不提供“按用户跨群共享”，因为那会改变群信息的传播边界。
- Session policy 改变产生新作用域，不暗中合并旧历史。
- 保留按 DeploymentRevision 隔离；切回旧 Revision 可以复用该作用域最新 generation 的已接受历史。UI 在切换前说明这一效果。
- 身份纠正/合并同样不自动合并 Session；需独立、可审计的数据处理流程。
- 共享群 Session 的重置需 `session.reset_shared` 权限；普通使用者默认只能新建自己的分区。

### 6.3 新会话与并发

Gateway 将 `/new` 或按钮解释为 ConversationCommand，不拼成特殊 system prompt。Worker 拥有以下线性化事务：

1. 用稳定 command_id 查命令 Receipt；重复则返回既有 generation。
2. 锁定 conversation registry，校验当前授权和 `expected_generation`。
3. 原子推进 generation、写命令结果和审计 outbox。
4. 新 Run 在 Worker intake 同一 registry 锁下选择当时有效 generation。

**顺序以 Worker 持久 intake/命令事务的提交顺序定义，不承诺 IM 发出时间顺序。**在 reset 前已被 Worker 接纳的 Run 保持旧 generation；还未 intake 的消息可能进入新 generation。UI 只有拿到 Worker 命令结果才显示“新会话已生效”；“命令已收到”不是已生效。

正在执行的旧 Run 不因 reset 被隐式取消；如用户同时要求取消，生成单独 CancelRun 命令。旧 Run 的结果不得推进新 generation head。当前开发期可同步调整 SessionID 契约和测试数据，不维护旧 hash 双栈。

### 6.4 为什么不需要 sticky Worker

Worker 节点的本地执行对象只属于活跃 Attempt，正式 Session candidate/head、执行租约和命令结果都在共享后端。下一次执行可由另一节点领取。Session 的并发顺序依赖账本事务和 fence，不依赖负载均衡 cookie，也不依赖 IM socket 所在机器。

### 6.5 验收

同一群两人共享/隔离切换、跨租户同 ID、topic 隔离、切换/切回 Revision、并发 reset、重复 reset、reset 响应丢失、旧 Run 完成晚于 reset、Worker 强杀后恢复都要断言 session_id、generation 与 accepted head。

## 7. F04 Callback、命令和审批交互

### 7.1 为什么新增

目前 interaction 表达“这是交互事件”，不等于有命令处理或审批流程。用户点击按钮后既需要及时的协议反馈，也需要业务动作的持久、去重和权限校验。两种确认必须拆开。

Telegram 官方要求响应 callback 以结束客户端等待；协议应答不能代替业务提交。[Telegram CallbackQuery](https://core.telegram.org/bots/api#callbackquery)

### 7.2 交互状态与流程

```text
RECEIVED → VALIDATED → PERSISTED → DISPATCHED → APPLIED
                    ↘ DENIED / EXPIRED
```

- Admission 根据规范化的 interaction kind 分派有限命令，不把原始 callback_data 当 Prompt 或可执行代码。
- 以 ExternalEventKey/interaction_id 写 InteractionReceipt 与 CommandOutbox，同事务提交。
- 独立的 ProtocolAck 操作有短期限、去重与预算。持久受理后可回复“已收到”；若持久失败，只能回复“暂未受理，请重试”，不能显示成功。
- Worker 消费命令后在自身事务去重、更新状态并返回 CommandResult；Gateway 再更新卡片/反馈。Broker ACK 只说明命令已耐久接管。
- `/help`、`/status`、`/new`、`/cancel` 按白名单解释。`/status` 仅查看调用者授权的会话/运行，不能借 ID 枚举其他租户。

### 7.3 危险工具审批

审批依赖 Worker 工具治理落地，本批只冻结接缝，不把按钮接入等同于工具系统完成。

- Worker 创建 ApprovalRequest，固定 tenant、run、attempt、tool、参数摘要、允许审批主体、有效期和状态版本。
- Gateway 只投递由该请求授权的卡片，按钮携带短随机 handle；服务端仅保存 handle 哈希和绑定事实，不把权限/参数正文放入 callback_data。
- handle 绑定 account、conversation、principal、approval_id、action、expiry，审批 owner 再校验权限和请求状态。
- Approval 是对固定参数摘要的一次决定，参数改变必须新建请求；审批通过不免除当前 tool allowlist、预算及用户撤销检查。
- 多次点击通过 Worker CAS 只产生一次决定。取消、超时、Run 已结束或参数变更时返回稳定结果，不重新执行工具。
- 工具外部副作用另有幂等键/补偿规则；审批去重不等于工具 exactly-once。

### 7.4 验收

成功/拒绝/过期/重复按钮、复制卡片到别的群、其他用户点击、伪造 handle、响应丢失、Worker 重启、审批完成后权限撤销、取消和完成竞争均需覆盖。

## 8. F05 Progress、流式和卡片

### 8.1 为什么新增

长耗时任务只在结束时发 Final，用户容易误认为机器人无响应并反复发消息；但逐 token 调用 IM 会引发限流和大量无价值写入。优化目标是有界、可解释的过程反馈，不是把 SDK 的全部流式事件原样透传。

### 8.2 协议分层

| 类型 | 含义 | 持久化 / 恢复 |
| --- | --- | --- |
| Progress | 当前 Attempt 的临时展示快照 | 可合并和过期；不作为正式历史或完成证据 |
| Final | Worker 已提交的唯一最终回复 | 沿用 committed-Final proof 和 Delivery ledger |
| ProtocolAck | callback 等平台协议反馈 | 独立短期限，不表示 Run 或审批成功 |
| InteractionPresentation | 命令结果/审批卡片 | 绑定持久命令/审批事实，不伪造 Completion |

新字段在现有开发期 Schema 中同步收敛；Progress、Interaction 使用独立意图类型/subject，与 Final 分开权限和容量，不能绕过 Final proof。

### 8.3 调度和展示

- Worker 输出展示快照，带 run_id、attempt_id、execution_generation、display_sequence、expires_at；没有外部回复地址。
- Delivery 验证当前执行资格和原 Admission 目标，在有界 store 中只保留每个展示流最新有效快照。
- 初始建议每个展示流最多 1 秒尝试刷新一次，同时受 F07 总预算限制；数值属于配置试验参数，不是 Provider 的固定允许速率。
- Telegram 初期采用能力验证过的文本更新；draft/rich 等路径必须确认当前固定 Go SDK 与账户能力，再增加对应 adapter。无已验证能力则退化为 Final-only。
- 企微复用同一原始回调关联与 stream_id，区分中间 `finish=false` 和最终 `finish=true`；实现细节以已核验协议库为准，保留 ReplyOrigin 不迁移规则。[官方企微 SDK 流式接口](https://github.com/WecomTeam/aibot-node-sdk#replystream-详细说明)
- 已有公开库以一次 Final request ID 处理为主，不能把现有 req_id 一次性去重保护简单删除来“支持流式”。应新增明确的 stream 生命周期，内部串行协调 sequence、pending ACK、结束标记，并保留旧 Final 行为测试。
- 卡片模板采用内部类型和参数白名单；内容渲染后的长度、Markdown/实体转义、按钮数量都由 Provider CapabilitySet 验证。

### 8.4 Final barrier 与外部未知结果

持久接受 Final 时关闭对应展示流的新 Progress 调度；Final 与已开始的 Progress 共用每目标展示锁。Final 等待已开始调用有界结束，但本地顺序不等于远端已按序呈现。

因此首期 Telegram 使用“独立 Final 消息”，不让可能迟到的 progress edit 覆盖 Final；旧进度清理是 best-effort。企微若某次中间 ACK 未知且协议不能证明同 stream 的后续终结语义，就将该展示流标为不确定，按 Provider 专项规则结束，而不声称本地锁已消除远端重排。真实流式验收是开启该能力的门禁。

### 8.5 验收

乱序、重复、旧 Attempt 的进度、Final 先到、进度调用已开始时 Final 到达、429、断线、ACK 未知、重启和队列满额。进度失败不创建新 Run；Final 后不能启动新的旧进度调用。

## 9. F06 图片、文件和 Artifact 引用

### 9.1 为什么新增

直接把图片 URL/文件名传给模型会丢失租户所有权、访问期限和资源预算；把所有附件读入 Gateway 内存又会放大单条消息的内存及并发成本。附件必须变成受控对象，而不是文本旁边的一段任意 URL。

### 9.2 统一内容模型

```json
{
  "kind": "artifact_ref",
  "artifact_id": "artifact_demo",
  "media_type": "image/png",
  "size_bytes": 12345,
  "content_digest": "sha256:...",
  "display_name": "example.png"
}
```

业务事件仅携带授权范围内的 artifact_ref 和非秘密元数据；临时下载签名、Bot Token、文件密钥不进入事件、日志或长期表。

### 9.3 入站流程

1. 来源认证、事件去重、用户/群授权先于附件下载。
2. 对已授权输入持久登记 AttachmentIntakeJob 和短期受保护的 Provider 取件引用；重复消息复用 job。持久接收附件任务不等于 Run 已创建。
3. 按租户/账户/大小预算有界下载，检查 DNS/IP、目的地主机、重定向、MIME/实际类型、解压后大小与内容摘要；Provider 返回 URL 也必须验证，不直接信任。
4. Artifact owner 写对象，再原子发布“可用对象引用”；扫描或必要解密失败时不给 Worker 可用引用。解密能力须由官方协议验证，缺失能力时拒绝该输入类型。
5. 所需附件全部成功才固定规范化输入并产生一次 Run；明确失败写结果和提示，不反复下载已完成对象。
6. 过期 Provider 引用不能假定重新拉取永久可用；给出重新上传的用户操作。

下载 job 属于 Admission 的入站准备，不自动增加独立 Connector workload。Artifact 存储通过专门 Port 接入；单机也不依赖 Gateway 临时目录作为正式存储。

### 9.4 出站与生命周期

Worker 生成 Artifact 后发布受限引用；Delivery 校验 tenant/Run/原目标，再上传和发送。分别记录上传和发送结果，不因最终发送失败盲目重复上传。对象存储孤儿有 TTL 回收；已被正式消息引用的对象按租户保留策略处理。

需要内容审查、图片模型或文件工具的功能依赖 Worker 能力；Gateway 只做接入不代表 Agent 已能理解附件。

### 9.5 验收

跨租户引用、过期 URL、恶意重定向、超大文件、内容类型伪装、压缩炸弹、重复消息、同事件不同摘要、下载途中重启、上传成功但发送 UNKNOWN、对象回收与消息保留冲突。

## 10. F07 限流、公平调度和投递恢复

### 10.1 为什么新增

当前 [Provider classify](../../../services/channel-gateway/internal/delivery/adapter/outbound/telegram/provider.go)识别 Telegram 限流为 REJECTED/rate_limited；[RetryDelay](../../../services/channel-gateway/internal/delivery/domain/policy.go)只允许部分 NOT_SENT 重试。说明“知道是限流”和“等待后恢复”之间仍缺显式契约。

当前进程并发槽位也不等于租户公平调度。增加副本后若每个副本各自限速，物理 Bot 的总速率反而会倍增。

### 10.2 结果、重试和额度分别建模

建议扩充内部 ProviderObservation：

```text
certainty: ACCEPTED | REJECTED | NOT_SENT | UNKNOWN
error_class: rate_limited | temporary | permanent | ...
retry_not_before: optional timestamp
retry_basis: provider_explicit_no_effect | local_not_sent | none
provider_message_id: optional opaque ID
```

- Bot API 返回的 `retry_after` 表达洪水控制等待时间；只将可信数字转为有上限的调度信息，不传播原始错误文本。[Telegram ResponseParameters](https://core.telegram.org/bots/api#responseparameters)
- REJECTED/rate_limited 只有在具体操作确认“未产生该发送效果”、未过期且预算允许时才自动重试。
- 401/403、目标失效、身份错配等明确永久拒绝不重试；UNKNOWN 不自动重试 Final。
- NOT_SENT 的临时故障继续有界重试，Provider 调用期限必须预留结果落账窗口。
- 修改的是显式 RetryPolicy，不把所有 REJECTED 改成可重试。

### 10.3 多层预算与公平性

初期复用 Gateway PG 保存 `not_before`、领取权和租户公平轮转状态，不为限流立即增加 Redis 依赖。

- 限额层次：Provider 出口 → 物理 Bot → conversation → tenant；同一物理 Bot 的预算跨 Gateway 副本共享。
- 每层键采用稳定内部标识；租户不能通过创建多个逻辑账户绕过物理 Bot 上限。
- 首期使用按租户轮转、每轮有界 batch 和最小服务保障；Final 预留容量，Progress 合并丢弃，ProtocolAck 使用小额独立预算。
- 领取事务只做预算 reservation、资格/fence 检查和 CALLING 状态推进，外部网络在事务外。
- 明确 NOT_SENT 可以按规则返还预算；未知调用已可能占用 Provider 额度，保守处理；租约过期不能推导额度未消费。
- 初始参数来自压测和官方约束，不写死“每租户 N 次”的业务常量；租户级成本硬预算仍由 Worker/计费 owner 负责。

### 10.4 人工恢复

- 管理员提交 DeliveryRecoveryRequest，携带原 receipt/attempt ID、expected state、reason、幂等键。
- 新恢复动作保留原目标、原 Final 和原审计，先核验权限、账号资格、截止时间及 Provider 能力；不覆盖原失败事实。
- ACCEPTED 不提供“重试”按钮；确需再发属于新的显式重复发送请求。
- UNKNOWN 显示“结果未确认”，默认只允许补充确认/人工标注；没有 Provider 可验证证据时不自动转 NOT_SENT。
- 企微原 ReplyOrigin 失效时拒绝沿新连接伪装原回调发送；若将来有独立主动消息能力，必须作为新功能/新投递授权，不作为本恢复分支。

### 10.5 验收

两个租户高低流量并存、双副本同 Bot、429 等待、时钟边界、额度耗尽、Final 与 Progress 争抢、调用后强杀、UNKNOWN 后人工操作、重复恢复请求。断言没有重复模型调用、没有改写旧事实、低流量租户有服务机会。

### 10.6 超长回复的分片恢复

**为什么需要单列**：一条逻辑 Final 被拆成多次平台调用后，前几片成功、后续片限流是正常的部分成功状态。如果只在 Final 总体上记录 REJECTED 再整体重发，用户会收到重复前缀；这不因入站去重而消失。

- 首次渲染形成不可变发送计划，固定 `intent_id + plan_digest + part_index`、正文摘要及顺序。长度计算按各 Provider 的字符/字节/实体规则实施，Markdown/实体边界不拆坏。
- 逐片持久记录 `ACCEPTED / REJECTED / NOT_SENT / UNKNOWN` 和可用的 provider_message_id；恢复只处理未成功的片，不重新渲染或重发已 ACCEPTED 的片。
- 某片明确限流时，按该片的 retry_basis/not_before 等待；某片 UNKNOWN 时暂停其后续片，展示“部分发送、结果未确认”，不把整条 Final 标为 Delivered。
- 逻辑 Final 只有全部必要片 ACCEPTED 才展示“平台已接受”；平台没有已读回执时不显示“用户已读”。取消或到期保留已发送部分的原事实。
- 测试至少包含第一片成功后第二片 429、第二片 UNKNOWN、进程重启、渲染器发布变化和恢复请求重投；既有账本若已支持部分证据则复用，不另建一套发送事实源。

## 11. F08 路由应用确认和运营诊断

### 11.1 为什么新增

当前 Web 已区分分发与连接，但 [账户诊断页](../../../web/components/channels/account-workspace.tsx)仍显示“尚无路由应用确认”。管理员无法仅凭 PUBLISHED 或 READY 判断消息是否会进入正确的 Agent，更无法解释“认证通过但没有回复”。

### 11.2 明确分层状态

| 层 | 来源 | 可以证明 | 不推断 |
| --- | --- | --- | --- |
| Desired | Control 配置提交 | 管理意图已保存 | 运行实例已应用 |
| Published | Control outbox / broker ACK | 发布事件已传输 | Gateway 已应用 |
| Applied | Gateway 持久投影观测 | 某目标版本已验证并应用 | Provider 已连接 |
| Connected | Connection 观测 | 当前 owner/连接报告就绪 | 有使用授权或绑定可接纳 |
| AdmissionEligible | 发布目标、授权投影、账户门禁的新鲜快照 | 该时点可以尝试接纳 | 后续一定成功 |
| Received / Admitted | 真实入站 Receipt / Admission | 消息到达 / 已持久接纳 | Worker 完成 |
| Completed | Worker Completion | Agent 执行已终结 | IM 已送达 |
| Delivered | Delivery 确定结果 | Provider 接受了该发送 | 用户已阅读 |

### 11.3 应用确认设计

- Gateway 持久投影提交后，在本库写 ObservationOutbox；后台经 mTLS 上报 Control，Control 只保存投影观测，不直接查询 Gateway PG。
- 最小字段：scope_id、source_epoch、account_id、route_generation、route_event_id、route_digest、instance_id、instance_epoch、report_sequence、applied_at、observed_at、expires_at。
- Control 按受信工作负载映射校验 scope/instance，按代次和序号去重；内容冲突单独告警。
- 聚合为 `APPLIED / PARTIAL / PENDING / STALE / UNKNOWN`；多个活动接收实例要按部署期望集和最近资格聚合，不能用“有一个实例报告过”表示全局完成。
- 共享 PG 投影只证明共享事实；实例本地 Handler/owner 准备情况另列，不能混为一个 applied=true。
- `eligible_at` 是有新鲜度的诊断计算，不是下一条消息的发送/执行凭证。

### 11.4 查询与隐私

Control 管理 API 聚合来自 Gateway 和 Worker 的受限观测；提供 receipt/admission/run/delivery 关联键、最后入站时间、失败原因码与恢复建议。正文查询单独授权并审计，默认页面只显示脱敏摘要；普通成员不能因可读 preflight 就获得所有聊天记录。

查询返回分页、时间范围上限和数据新鲜度；租户未知资源与无权资源使用统一不可枚举响应。前端不依据本地最大 owner_epoch 自行选主，不在一次查询失败后再次创建诊断任务。

### 11.5 验收

报告乱序、重投、实例 epoch 变化、scope 错配、Control 短暂不可用、报告过期、一个实例掉线、绑定快速切换、历史 READY 对新配置不可用。UI 文案逐层对应事实。

## 12. F09 Trace、Metrics、审计和脱敏

### 12.1 为什么新增

只有日志文本和健康探针时，很难区分“IM 没送到、Admission 拒绝、Worker 未领到、模型超时、Final 未投递”。为了多租户运营，需要同一业务请求跨异步边界可关联，又不能把所有消息内容上传观测后端。

### 12.2 Trace 设计

```text
im.receive → admission.decide → outbox.publish
                                 ↓
                            worker.attempt
                            ├─ model.call
                            ├─ tool.invoke
                            ├─ session.load / session.stage / completion.commit
                            └─ memory.read / memory.write / summary.derive
                                 ↓
                         reply.publish → delivery.attempt → im.send
```

- Provider 输入不提供可信内部 trace 身份；入口创建本系统 context，外部 header 只按白名单作非授权关联。
- Outbox 保存有界 trace carrier，NATS 使用头传播；业务 digest、幂等键不包含 trace 字段。
- 重试创建新的 attempt span，并保留原 run/admission/intent 关联；跨消息、批量或重投用 span links，避免假装所有工作都在同一同步调用栈。语义采用发布时固定的 OTel messaging conventions。[OTel messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
- Collector 故障只影响观测导出，不阻断业务接纳；SDK 队列有界，丢弃计数可见。
- Tool/Memory/Summary spans 随所属 Worker 功能一起实现；Channel 文档不把尚未存在的调用路径算作已串通。

### 12.3 指标

| 指标族 | 例子 | 主要用途 |
| --- | --- | --- |
| 入站 | received、authenticated、duplicate、denied、admitted | 接收量、拒绝和去重 |
| 时延 | ingress→receipt、queue wait、delivery queue age、send latency | 定位等待位置 |
| 连接 | owned accounts、reconnect、lease lost、observation stale | 扩容与连接故障 |
| 路由 | source age、apply lag、quarantine、outbox pending | 发布与投影健康 |
| 投递 | accepted/rejected/not_sent/unknown、rate_limited、expired | 真实成功率和恢复 |
| 容量 | admission slots、tenant queue、retained bytes、artifact jobs | 背压和配额 |
| Worker 关联 | model/tool latency、usage、session latency | 完整执行成本和依赖 |

高基数的 user_id、session_id、message_id 不作为常规指标 label。tenant 级精确用量进入有幂等的 usage/审计明细；指标使用受限租户标签或聚合分组。Provider usage、预估成本和最终计费分别标记，Gateway 不重复累计 Worker 上报成本。

### 12.4 审计模型与写入

```text
schema_version, audit_event_id, occurred_at, ingested_at,
tenant_id, provider/channel, account_id, binding_id,
external_principal_id, user_id_hash, conversation_id_hash,
session_id, session_generation, run_id, attempt_id,
agent_name, deployment_revision_id, tool_name,
action, decision, reason_code, policy_revision,
latency_ms, error_type, input_tokens, output_tokens,
cost_amount, cost_currency, cost_basis, trace_id,
actor_kind, correlation_id, retention_policy_ref
```

- 不适用的 tool/cost 字段为 null，不用零冒充“已经计量”。
- 每个事实 owner 在本地事务写 AuditOutbox；审计服务/存储按 `tenant_id + audit_event_id` 去重接收。
- 授权、审批、恢复等关键审计若本地 outbox 写失败，则业务操作同事务失败；Collector 或远端审计暂时离线不要求同一网络事务。
- 审计 outbox 满额触发可解释背压和告警，不允许无限增长或静默丢弃关键决定。
- 保留期、删除、法律留存、对象归档由租户审计策略管理；租户管理员修改策略也被审计。

### 12.5 密钥与内容处理

- IM Token、Bot Secret、模型 API key、数据库密码只通过既有受信凭据 Port 使用；常驻配置保存引用/元数据，事件不携带凭据。
- TLS/mTLS 私钥使用受限挂载；落盘密文与密钥材料分离，轮换按凭据版本进行，调用开始前检查用途及版本。
- HTTP 自动 instrumentation 必须删除含 Bot Token 的 URL path、Authorization、Cookie、签名查询串；错误仅映射稳定 error_type。
- 用户/会话伪名采用带租户域分隔的 HMAC，不能用可枚举短 ID 的裸 hash；HMAC key 有版本且不导出，轮换后的关联策略由审计 owner 定义。
- 原始消息、附件、Prompt/工具参数默认不写日志/trace；显式诊断采样需要额外权限、短期限、字段白名单和自动过期。
- 原始 SDK error、panic 附带对象、请求转储同样经过脱敏测试。脱敏在业务日志入口执行，Collector 再做纵深清理。

### 12.6 验收

构造明显的合成 Secret/PII canary，扫描日志、trace、API 错误、审计查询和前端状态；要求零 canary 泄漏。NATS 重投不重复计费；Collector 关闭不改变 Run/Delivery 成败；审计 outbox 故障有明确规则。

## 13. F10 多节点、故障恢复与真实 IM 验收

### 13.1 为什么新增

公开 Connector 的认证和一次回复不能证明分布式接管、正式 Session 或长时间运行恢复。现有 PG/NATS/fence 机制应复用，新增的是对实际组合场景的可复验证据，而不是另造分布式框架。

### 13.2 部署拓扑

```text
管理员 → Control Web → Control API / Admin API
                               │ 发布/快照/受信凭据初始化
外部 Telegram / WeCom          ↓
           │            NATS + Gateway 路由/策略投影
           ├─ webhook LB → Gateway A / B / ...
           └─ polling/WSS ← 当前 account receiver/connection owner
                              │
                    Admission → RunRequested
                                      ↓
                              Worker A / B / ...
                              ├─ 固定 Manifest + 模型/工具
                              ├─ 共享执行账本
                              └─ Session/Memory Storage Adapter
                                      ↓
                                ReplyIntent
                                      ↓
                         Gateway Delivery → 原 IM 目标

各 Workload → OTel Collector → 日志/Trace/Metrics 后端
```

这里的 Agent Gateway 对应现有 Channel Gateway；不额外引入同职责的第二层 Gateway。Channel Adapter 是进程内协议 Adapter；Storage Adapter 是数据能力的实现角色，不因名称相似就成为独立服务。

### 13.3 sticky 的精确结论

- Worker 不按用户做 sticky；正式事实在共享后端，执行资格由账本保证。
- webhook HTTP 可分配到多个有效 Gateway；同消息并发由持久 Receipt 去重。
- long polling/WSS 是账号级有状态接收者，仍需 owner；负载均衡 sticky 不能代替 lease/epoch。
- 企微 ReplyOrigin 绑定原实例、owner epoch、配置 revision 和 socket generation。接管后的新连接不自动继承旧回调回复资格。
- 某台机器所有权变更不改变 tenant/session 身份；某个 Worker 完成也不代表 Gateway 可以绕过原目标资格。

### 13.4 故障处理矩阵

| 故障 | 新输入 | 已接纳/执行/回复 | 恢复依据 |
| --- | --- | --- | --- |
| Gateway 强杀 | 新 owner/副本按资格接管 | Receipt/Outbox 保留；CALLING 超时保留 UNKNOWN；企微旧 Origin 不迁移 | PG lease、持久事实、真实 Provider 证据 |
| Worker 强杀 | 其他 Worker 可接管合格 Run | 旧 Attempt 失租，不提交历史；后续 Attempt 读取 accepted head | 执行 fence 与唯一 Completion |
| Gateway PG 短暂离线 | 不 ACK 未持久新输入；polling 不推进 cursor | 不以内存成功覆盖持久失败 | 后端恢复后重投/重领 |
| WeCom 回调时 PG 离线 | 有界重试后明确失败/断开 | 不承诺 Provider 永久重放或零丢失 | 必须真实验证恢复/丢失窗口并告警 |
| NATS 暂时离线 | 依本地 outbox 容量接纳，满额背压 | 已持久消息等待 relay；无无界内存队列 | 原 EventID + PubAck/claim CAS |
| 模型超时 | Channel 不重跑模型 | Worker 决定 Attempt 重试/失败，Delivery 展示正式结果 | 执行策略、确定性和期限 |
| 工具失败 | 不把错误当新的聊天输入 | Worker 处理副作用幂等/补偿与审批 | 工具 owner 的执行账本 |
| IM 明确限流 | 独立队列与账号额度 | 依据 retry_basis 和期限恢复 | ProviderObservation |
| IM 发送超时 | 不影响已提交模型事实 | UNKNOWN，不直接重发 | 可验证证据或显式人工操作 |
| 策略投影过期 | 拒绝新授权操作 | 重复 Receipt 可读；危险工具等待/拒绝 | 新鲜授权投影 |
| Collector 离线 | 正常业务继续 | 有界观测丢弃计数；关键审计另有 outbox | 观测导出恢复 |

Telegram 提供两种互斥接收方式且待收更新存在保留期限，系统不能把 Provider 队列当作永久持久存储。[Telegram 接收更新](https://core.telegram.org/bots/api#getting-updates)

### 13.5 必须补齐的真实门禁

- 最终合并版本 Telegram long polling：真实用户输入→Control 固定目标→Worker→正式 Session→Final；包含重启、重复 Update、切换模式后唯一接收者。
- 企微正式业务链：真实用户输入→非 smoke Admission→正式 Binding→Worker→Session→原连接 Final；包含两轮历史与重投。
- 两 Gateway 的连接接管与旧 epoch/旧 socket 拒绝；两个 Worker 的顺序和强杀恢复。
- 所有测试记录 build/commit、account/binding revision、run/attempt/session、provider 确定结果，凭据不进入证据。
- 真实测试由显式测试账户/绑定和独立环境驱动；文档生成、普通单测、预检不自动启动真实订阅。

## 14. F11 租户灰度、配置回滚与发布流程

### 14.1 为什么新增

单 Binding 只指向固定版本时适合确定性执行，但不是百分比灰度系统。当前实现增加受限的
stable+canary 策略；Gateway 不在每条消息上随机挑版本，而是按可信 sender 做确定性选择，
避免同一用户来回变更模型/工具/Session 历史。

当前状态须按能力拆开，不得将二者合并为“灰度/回滚已支持”：

| 能力 | 状态 | 当前或目标行为 |
| --- | --- | --- |
| 不可变版本与路由回退 | 已实现 | Binding 可用新的 CAS 与更高 RouteGeneration 精确指回仍有效旧 DeploymentRevision；旧 Admission/Run/Reply 不变。 |
| 按比例灰度 | 已实现核心链路 | stable+canary 双目标与0–10000基点进入路由事件；Gateway 对非显式用户确定性分桶。 |
| 按用户分组 | 已实现核心链路 | 最多100个显式 sender ID；其余使用可信 sender ID、rollout ID、Provider、Account 计算稳定 cohort，不按请求随机。 |
| 选择观测维度 | 已实现 | Admission span 记录 rollout ID、stable/canary variant 与最终 DeploymentRevision。 |
| 结果指标窗口 | 未实现 | 尚无按策略代次和所选 DeploymentRevision 汇总的最小样本、错误率、延迟窗口。 |
| 自动停止/回退 | 未实现 | 尚无基于上述正式指标、带迟滞和最小样本的单调新策略发布。 |

“发布了多个不可变 Revision”只表示存在多个候选版本；只有 Owner 显式提交灰度命令并且
新 RouteGeneration 被 Gateway 应用后才会拆分新接纳流量。Binding 指回旧 Revision 仍只是
人工整体切换，不能作为自动回退验收证据。

### 14.2 分两步实现

1. **先做租户/账户级切换**：复用现有固定 Binding target。一次切换只影响首次新接纳；旧 Admission/Run/Reply 保留原目标。先完善预览、应用确认和回滚 UI。
2. **双目标用户 cohort 灰度**：Control 在同一 Binding 发布 stable target、一个 canary PublishedTarget、基点比例、rollout ID 和显式 sender ID；Routing 使用可信 sender ID 做无状态确定性选择。不是新增两个同时生效的冲突 Binding。

cohort key 不包含瞬时 instance、conversation/thread 或选择结果 DeploymentRevision，避免版本选择的循环依赖，并让同一用户跨 Gateway 副本、重试和会话稳定。候选不变时比例更新保留 rollout ID，因此阈值扩大/缩小时哈希桶单调变化；更换候选会生成新 rollout ID。V1 不保存逐用户 assignment 表。

### 14.3 回滚语义

- 策略/绑定回滚通过发布“内容等于旧配置的新 revision”完成，epoch 单调递增，不直接降低历史版本号。
- 配置内容回滚不回退最新 Secret、被撤销用户或被关闭的安全能力。
- 已接纳 Run 继续旧 Manifest；若需要立即停止，走明确的暂停接纳/取消运行命令，不改写历史目标。
- Session 仍按所选 DeploymentRevision 分区；没有模型/历史 schema 兼容性证据时不自动迁移会话。
- Reply 保持原 Admission 目标，不在回滚后重新解析当前 Binding。

### 14.4 开发期与生产灰度的区别

ARC-000 下，新增必填 actor/policy 字段可以同步修改现有 v1。这样的不兼容契约不能用“滚动升级”掩盖：开发环境应显式停止旧消费者，部署同一 contract digest 的完整组合，再重建专用测试数据并验收。

真正的租户灰度仅在运行二进制共同支持同一已确认契约时启用。老进程不理解授权字段时，不能让它继续消费或接纳受保护账户；部署身份/启动契约检查和实例清单必须先证明旧进程退出。后续稳定对外版本再单独设计 expand/contract 升级，不在当前优化中增加开发期兼容双栈。

### 14.5 验收

同一 sender 跨会话/重试不抖动、相同候选的权重更新保持同一哈希顺序、显式 sender 优先、0基点停止新 canary 接纳、旧 Run 在回滚后完成仍发原目标、回滚不恢复已撤销权限、两个不同契约二进制被部署门禁阻止。

当前代码已覆盖 Control→Route event→Gateway 选择→Admission 固定目标的核心链路；完整
R01 仍需真实 Worker 结果、分版本指标窗口和自动停止/回退证据。单目标 Binding 的切换与
回退测试只能验收版本回退子项，不能填补这些剩余门禁。

## 15. F12 容量、归档和部署

### 15.1 为什么新增

水平扩容只能缓解一部分 CPU/并发压力，不能自动解除单 Bot 限流、同 Session 串行化或单 PG 热点。Receipt、投递证据和附件也会长期增长。容量规划必须同时覆盖入口、执行、出口和保留数据。

### 15.2 容量模型

定义以下实测量，不在设计中声称单节点固定承载数：

```text
lambda_in         = 峰值外部事件/秒（包含重投）
p_new             = 首次有效事件比例
p_admit           = 首次事件中产生 Run 的比例
lambda_run        = lambda_in * p_new * p_admit
C_worker          ≈ lambda_run * T_run
C_webhook         ≈ lambda_in * T_receipt
R_delivery        = lambda_run * E[Final 分片数 + 实际 Progress 调用数 + 重试次数]
QPS_gateway       ≈ lambda_in * q_receipt + lambda_run * q_admission
                    + R_delivery * q_delivery + QPS_projection + QPS_leases
retained_bytes    ≈ daily_events * E[receipt+audit+delivery bytes] * retention_days
session_store_QPS ≈ lambda_run * E[load+stage+commit+memory/summary operations]
```

这是 Little's Law 和工作量估算，不是独立事件假设下的延迟保证。热点 Session 的吞吐上界还受单会话顺序限制；每 Bot 受 Provider 上限限制；附件需单独测 bytes/s 与峰值驻留内存。

用 p95/p99 耗时、突发倍数、依赖恢复积压和连接池 headroom 评估副本数。入站 lookup 与新接纳槽位分开，保留重投查询与 Final 恢复的最低容量。消息类型变化后重新标定，不把文本测试容量套到附件。

### 15.3 保留和归档

- Receipt/ExternalEventKey 的首次决定不能因普通 TTL 删除而让重投重新执行；正文可按策略到期删除，但保留足以维持身份防重的 tombstone/摘要，或使用仍可查询且可证明覆盖范围的归档索引。
- Routing retained history 归档先生成带 scope/epoch/watermark/digest 的一致快照，并验证快照+增量可重建后，才执行明确的历史裁剪。
- Delivery UNKNOWN、待恢复和审计留存数据不能被一般完成任务清理器删除。
- Session/Memory/Artifact 各 owner 执行自己的 GC；Gateway 不跨 schema 删除。
- 容量耗尽先拒绝新的资源占用，同时保留读取旧 Receipt、完成在途发送和导出审计的能力。

### 15.4 最小可运行方案

复用现有 [Compose](../../../deploy/compose/compose.yaml)和 [Worker Compose](../../../deploy/compose/compose.worker-v1.yaml)：

- 1 个 Control API、1 个 Control Web、1 个 Channel Gateway、1 个 Agent Worker。
- 1 个 PostgreSQL；`control/gateway/worker/runtime_session` 各自 schema、migration/runtime role。
- 1 个 NATS JetStream；reconciler 作为初始化任务，运行账户不持有管理权限。
- 初始正式 Session 使用已支持的 PostgreSQL；附件未开启时不增加对象存储依赖。
- Collector 可选用于开发观测；开启相应功能后再加独立对象存储/Memory 后端。
- Telegram polling 和企微长连接可走出口；webhook 开启时才需要适合的受控公网入口和 TLS。

该方案用于开发/演示，不承诺单机基础设施高可用。Secret 通过现有部署机制注入，不复制进 Compose YAML 或镜像。

### 15.5 生产推荐拓扑

- 多个 Gateway 和 Worker 副本，数量分别由连接/入口/出口与执行耗时评估；不是二者永远等比例扩容。
- Control API 多副本、共享可信配置；PostgreSQL 主备/备份恢复与连接池，NATS 多节点持久副本均需按故障域和仲裁验证。
- 公网只暴露允许的 ingress；管理与 mTLS 内部端点在受限网络。显式出口规则、DNS/证书检查和最小角色权限。
- Collector、观测存储和审计归档独立设置保留与故障策略。
- 滚动停止先 quiesce 新接纳/领取，再有界 drain；连接 owner 和活跃 Attempt 续租到退出阶段，未完成部分交给已定义恢复语义。
- Kubernetes Chart、HPA/PDB/NetworkPolicy/Secret references 在全部 Workload 完成后的 **FINAL-INTEGRATION** 统一落地。本文不创建 Helm，也不把它改回早期 P1。

## 16. 数据访问、同步与多后端的协作契约

### 16.1 为什么 Channel 不直接支持六套数据库

租户选择 Redis、SQL、向量库或外部 Memory，不应该改变 Telegram/企微解析、入站幂等或回复目标。将后端差异放进 Channel 会造成每增加一个 IM 都要重复实现数据一致性。

因此本优化只为这些能力定义 Channel 侧输入/身份/引用契约；具体后端实现属于数据能力 owner。

### 16.2 不使用一个万能 DataStore

| 数据 | 独立 Port 的核心语义 | 正式 owner | 当前/后续后端 |
| --- | --- | --- | --- |
| Session | LoadAcceptedHead、StageCandidate；正式接受由 Worker 账本原子决定 | Worker + Session Store | 当前正式 PG；其他后端必须满足候选不变性和 head 验证 |
| Message/Event | Append/FindByExternalKey、稳定 digest、去重 | Gateway 入站；Worker 执行事件各自拥有 | 当前 PG；不通过缓存替代幂等事实 |
| Memory | UpsertByMutationID、Search、ReadVersion | Memory owner | Redis/SQL/向量/外部 Memory 作为不同 Adapter |
| Summary | PutForAcceptedHead、GetForHead | Worker 派生数据 owner | SQL/对象存储；绑定 source head 和摘要版本 |
| Artifact | Stage、Finalize、GetAuthorizedReference、Retire | Artifact owner | 对象存储为主；元数据存数据库 |
| Knowledge | QueryPinnedIndex、ResolveDocument | Knowledge owner | 向量库/搜索/对象存储组合 |
| Audit Log | AppendIdempotent、QueryAuthorized、Archive | 各 owner outbox → 审计存储 | SQL/追加日志/对象归档 |

接口的不同来自实际语义，不为所有对象统一做 CRUD。每租户通过发布的 Runtime Profile/backend descriptor 选 Adapter，不在消息中提交任意 DSN。

### 16.3 一致性和更新顺序

1. Session event 在当前 Attempt 内记录为候选，不立即改变正式共享历史。
2. Candidate 内容先在选定 Session 后端持久化，引用和摘要不可变。
3. Worker 在同一账本事务核验当前 fence，接受 Completion、candidate ref、accepted head 和 ReplyOutbox。
4. Summary 从已接受 head 派生，带 `source_head_digest`；迟到结果不能覆盖新 head 的 Summary。读取可回退到正式 events，而非把过期 Summary 当完整历史。
5. Memory write 通过 committed-mutation outbox 触发；mutation_id 去重，附 source Completion/head。外部 Memory 可见后返回 version/watermark。
6. 需要 read-your-writes 的后续 Run 读取 `min_memory_version`；未达到时有界等待或明确降级，不以节点本地缓存假装全局可见。
7. 强一致的 head 接受与最终一致的 Summary/Memory 分开，不要求跨 SQL、向量库和对象存储分布式事务。

### 16.4 后端取舍

| 后端 | 适合 | 一致性/运维代价 | 多节点限制 |
| --- | --- | --- | --- |
| InMemory | 本地测试、短期缓存 | 快但进程丢失即丢状态 | 不作为本方案多节点正式 Session/幂等账本 |
| Redis | 缓存、在线索引、满足持久与原子条件的特定数据 | 低延迟；需明确持久化、复制和故障转移丢失窗口 | 不能把单实例原子性直接当跨故障强持久保证 |
| SQL | 账本、身份授权、事务性 head/索引 | 事务约束明确；热点写和维护成本需测 | 通过租户键、角色、索引和受控连接隔离 |
| 向量库 | 相似检索、Knowledge/Memory 索引 | 索引传播与读延迟因产品配置而异 | 不作为 Run/Receipt 的唯一事实源 |
| 对象存储 | 附件、不可变候选、大正文、归档 | 大对象成本较低；没有跨对象业务事务 | 元数据发布、摘要和租户授权仍由 owner 管理 |
| 外部 Memory 服务 | 专项记忆能力 | 增加 API 配额、可用性和数据边界依赖 | 必须适配幂等、版本可见性、超时和退出策略 |

以上是设计取舍，不表示当前 Worker 已实现全部 Adapter。真实后端参数和一致性承诺在选型后按官方文档及故障测试确认。

### 16.5 Redis→SQL、向量库本地→远端的迁移

- 先清点每租户 namespace、格式、编码、embedding 模型/维度、版本、数据量和权限，定义迁移 manifest。
- 选一个事实源，获取有序变化水位；全量复制保留稳定业务 ID，增量通过 outbox/CDC 或明确停写窗口补齐。
- 不采用两端独立 best-effort 双写；无法获得可靠变化日志时，选择可解释的停写/排空窗口。
- 比较数量、内容摘要、引用可解性和检索质量；向量维度/模型变化须重建索引，不宣称复制原向量即可。
- 追平水位后发布新的 backend descriptor/revision；新 Run 固定新目的地，旧 Attempt/已发布引用继续能访问原后端，直至排空和引用迁移完成。
- “切回旧 DSN”不是有新写入后的回滚。需保留反向增量或暂停新写入、回灌后再切回；到达不可逆点必须明确标记。
- 整个过程不改变 ExternalEventKey、租户身份或原 Delivery target。

该节是平台协作要求和未来后端迁移方案，不把多后端实现纳入 Channel 第一批交付。

## 17. 最小数据模型与事务示例

### 17.1 复用与新增对象

以下为逻辑模型，不宣称都是当前表名；实际 DDL 由各 owner 在既有 schema 内实现，跨 owner 用 ID/接口关联，不建立跨 schema 业务写事务。

| 对象 | 关键字段 | 约束 / 所有者 |
| --- | --- | --- |
| tenant（复用） | tenant_id、status、audit_policy_ref、quota_ref | Control；应用查询必须有受信租户上下文 |
| agent_app（映射现有 Agent/Deployment） | tenant_id、agent_id、deployment_id、published_revision、model_profile_ref、tool_policy_ref、backend_profile_ref | Control 发布，Worker 固定消费 |
| channel_account（复用） | tenant_id、account_id、provider、physical_identity、config、connection_revision、credential_metadata | Control；Secret 独立密文，不在配置 JSON |
| channel_binding（复用/扩展） | tenant_id、binding_id、account_id、target、binding_revision、access_policy_ref、session_policy_ref | Control；目标与策略属于同一租户 |
| external_principal_binding（新增） | tenant_id、principal_id、provider、account_id、external_user_id、state、revision | Control；账户范围唯一，状态撤销可发布 |
| channel_policy_revision（新增） | tenant_id、policy_id、revision、digest、body、published_at | Control；发布内容不可变 |
| channel_policy_projection（新增） | scope、source_epoch、tenant_id、account_id、policy_revision、digest、fresh_until | Gateway 自有只读运行投影 |
| session / registry（扩展） | tenant_id、scope_digest、partition、generation、session_id、accepted_head、sequence | Worker；registry 与 intake/reset 线性化 |
| message/event（复用/扩展） | provider、account_id、external_event_id、source_digest、kind、receipt_decision、admission_id、run_id、actor_ref | Gateway；ExternalEventKey 唯一 |
| interaction_receipt（新增） | tenant_id、interaction_id、event_key、actor_ref、command_id、state、reason | Gateway；持久命令接纳不等于执行 |
| conversation_command（新增） | tenant_id、command_id、session_scope、expected_generation、result_generation、result | Worker；一次命令一次决定 |
| memory（后置） | tenant_id、memory_id、namespace、mutation_id、version、source_completion、value_ref | Memory；幂等和版本可见性 |
| summary（后置） | tenant_id、session_id、source_head_digest、summary_version、body_ref | Worker/派生数据；不覆盖不同源 head |
| artifact（新增能力依赖） | tenant_id、artifact_id、digest、size、type、object_ref、state、retention_ref | Artifact；可用前不暴露给 Worker |
| delivery_observation / recovery（扩展/新增） | tenant_id、intent_id、attempt_id、certainty、retry_basis、not_before、recovery_id、reason | Delivery；原事实不被恢复操作覆盖 |
| route_application_observation（新增） | scope、epoch、account_id、route_generation、digest、instance、sequence、expires_at | Gateway 产生、Control 聚合 |
| audit_log（复用平台能力/扩展字段） | 见 F09；audit_event_id、tenant_id、action、decision、trace_id、cost | 每 owner 本地 outbox，审计端幂等保存 |

### 17.2 约束示意

```sql
-- 设计示意；不是可直接应用的迁移。
CREATE TABLE control.channel_principal_bindings (
    tenant_id text NOT NULL,
    principal_id text NOT NULL,
    account_id text NOT NULL,
    provider text NOT NULL,
    external_user_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('ACTIVE', 'REVOKED')),
    revision bigint NOT NULL CHECK (revision > 0),
    PRIMARY KEY (tenant_id, principal_id),
    UNIQUE (tenant_id, provider, account_id, external_user_id)
);

CREATE TABLE control.channel_policy_revisions (
    tenant_id text NOT NULL,
    policy_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    digest text NOT NULL,
    body jsonb NOT NULL,
    published_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, policy_id, revision)
);

CREATE TABLE gateway.channel_interaction_receipts (
    tenant_id text NOT NULL,
    interaction_id text NOT NULL,
    provider text NOT NULL,
    account_id text NOT NULL,
    external_event_id text NOT NULL,
    actor_ref text NOT NULL,
    command_id text NOT NULL,
    source_digest text NOT NULL,
    decision text NOT NULL,
    PRIMARY KEY (tenant_id, interaction_id),
    UNIQUE (provider, account_id, external_event_id)
);
```

实现时补齐同 schema 的账户/策略外键、字段长度/闭合 Schema、主体映射规则、索引和 runtime DML 权限；不能用这三个示例表替代整个完整模型。`principal_id` 在本首期是账户范围内主体，跨账户共享身份需显式扩展，不隐式复用该主键。

### 17.3 三个关键事务

```text
Gateway 新输入事务：
  复查 ExternalEventKey 唯一性
  → 固定路由 + 本库策略 projection fence
  → Receipt + Admission/Interaction + 对应 Outbox + AuditOutbox

Worker 新会话事务：
  command 去重 → registry 锁 → 授权/expected_generation
  → 推进 generation + command result + AuditOutbox

Delivery 开始调用事务：
  claim/fence + 原 target + deadline + 预算 reservation
  → CALLING + attempt evidence
  commit 后才进入 Provider 网络调用
```

跨模块调用不能偷偷扩展成 Control、Worker、Gateway 共用一个 SQL transaction；事实一致性用各 owner 的事务、稳定身份与幂等消费者组合。

### 17.4 租户隔离的落点

**为什么不能只增加 tenant_id 列**：如果引用关系、后台任务或缓存键没有租户约束，表里即使有 tenant_id，也仍可能通过错误的 Join、对象引用或复用缓存读到另一租户的数据。

| 隔离面 | 拟实施规则 | 验证方式 |
| --- | --- | --- |
| 配置及模型/工具/后端引用 | 由受信请求上下文或已发布 Route 确定 tenant；同一配置图内所有资源引用属于该租户；共享系统模板必须显式标注只读共享 | 交叉引用另一租户的模型、工具策略、凭据和后端时发布失败 |
| SQL 行及关系 | 查询和写入包含 tenant 条件；同 schema 关系采用含 tenant 的复合唯一键/外键；共享 runtime role 采用封装 Repository，必要时增加 RLS 纵深防御 | 两租户同局部 ID、遗漏过滤的负例、错误外键、跨租户分页测试 |
| RLS 与连接池 | 如启用 RLS，事务内设置受信 tenant 上下文，事务结束清除；runtime 无 BYPASSRLS/表所有者权限，系统作业使用单独受限角色 | 池连接从租户 A 复用到 B 后不保留旧上下文；后台任务仍受控 |
| 缓存/索引/向量检索 | key、namespace、检索过滤条件包含 tenant 及必要的 app/principal 范围；共享群与个人 Memory 不隐式混用 | 缓存命中、相似检索、批量读取和索引重建都运行越权负例 |
| Artifact 与对象存储 | 对象 namespace 不是访问授权；所有读取先经 owner 校验租户/主体/用途，再签发短期引用 | 猜测路径、借用另一租户 artifact_id、复制过期链接都不获得新访问权 |
| 凭据与诊断 | credential purpose/tenant/revision 三者校验；查询脱敏、正文额外授权；导出和人工恢复同样留审计 | Secret canary、跨租户日志查询、错误用途凭据和恢复越权测试 |

高隔离租户可选择独立数据库、对象空间、密钥和 Worker 池，但仍沿用同一业务接口与 tenant 约束；物理独占是隔离等级选项，不作为每个租户的默认部署开销。

## 18. Interface、协议、HTTP 和代码落点

### 18.1 小而深的 Interface

下列为拟议 Go 签名，参数类型和包路径需与现有 Application 类型收敛；不是当前可编译 API。接口把投影新鲜度、授权事实、重试依据等复杂规则封装在明确接缝内，不暴露通用 Repository 给调用者。

```go
// Gateway Admission 消费自己的 Port；实现读取 Gateway 内的可信投影。
type ChannelAuthorizer interface {
    Evaluate(ctx context.Context, input AuthorizationInput) (AuthorizationDecision, error)
}

// 外部输入只提供可信身份/内容；正式 generation 和命令结果由 Worker 决定。
type ConversationCommandSubmitter interface {
    Submit(ctx context.Context, command ConversationCommand) (CommandReceipt, error)
}

// Provider 报告事实；Delivery 决定重试、预算和状态迁移。
type ProviderSender interface {
    Send(ctx context.Context, request AuthorizedSend) (ProviderObservation, error)
}

// 对象引用不等于可访问；tenant/run 约束必须由 owner 核验。
type ArtifactReader interface {
    Resolve(ctx context.Context, query AuthorizedArtifactQuery) (ArtifactReference, error)
}
```

不新增一个拥有 Auth、Session、Memory、SQL 和全部 Provider 的 `ChannelService` 大接口。Telegram/WeCom 已形成真实变化点；每个数据能力的 Port 只在已有或即将实现的两个适配需求之间引入，不为想象中的十种后端先建空层。

### 18.2 拟议 HTTP / 事件增量

以下路径是接口设计草案，正式名称在 Schema 审查时冻结，不能据此宣称路由已注册。

| 用途 | 拟议 HTTP / 事件 | 核心约束 |
| --- | --- | --- |
| 外部主体管理 | `/v1/tenants/{tenant_id}/channel-principals` | OWNER 或明确运维角色；CAS、幂等、不可枚举 |
| 访问策略 | `/v1/tenants/{tenant_id}/channel-accounts/{account_id}/access-policy` | 编辑/发布区分，引用精确 revision |
| 会话/触发策略 | 账户下 session-policy / trigger-policy | 不将操作权限放进 Provider connection config |
| 消息/投递诊断 | 账户下 receipts / deliveries / timeline | 分页、时间窗、字段脱敏和细粒度查询授权 |
| 投递恢复 | `/v1/tenants/{tenant_id}/delivery-recovery-requests` | 原 target/Final、expected state、reason、幂等键 |
| 路由应用观测 | 内部 mTLS observations 扩展 | workload scope、instance epoch、sequence、新鲜度 |
| 新 Run | 现有 RunRequested v1 同步扩充 actor/policy/content | 完整 Schema/fixture/codegen/producer/consumer 同步 |
| 会话命令 | 独立 ConversationCommand/CommandResult | 不制造伪 Run；同主体范围授权 |
| 进度 | 独立 Progress intent | TTL、generation、sequence；不能冒充 committed Final |
| 策略发布/撤销 | 不可变策略事件 + 可验证当前状态投影 | 原子发布 outbox、完整快照和 gap detection |

普通管理 API 的 no-store、幂等原请求恢复、Secret 写后不回显、OWNER 校验应复用现有实现，不为新页面另写较弱逻辑。

### 18.3 代码结构增量

以下目录中 `[现有]` 表示保留；`[拟新增/扩展]` 不是当前文件存在的声明。

```text
api/
├── schemas/channel/v1/                 # [扩展] actor/policy/content/interaction/observation
├── openapi/control/v1/                 # [扩展] 管理与内部契约
└── events/{control,execution}/v1/       # [扩展] 策略、命令、Progress；明确各自 ACL

platform/im/
├── telegram/                           # [现有+扩展] 有界协议调用，不拥有业务队列
└── wecom/                              # [现有+扩展] Stream/媒体/交互原生协议能力

services/control-api/internal/channelbinding/
├── domain/                             # [扩展] 主体、访问/触发/会话策略
├── application/                        # [扩展] 管理、发布、查询、恢复授权
└── adapter/                            # [扩展] HTTP、PG、运行投影发布

services/channel-gateway/internal/
├── routing/                            # [扩展] policy projection / applied observation
├── admission/
│   ├── domain/                         # [扩展] actor / trigger / interaction / content
│   ├── application/                    # [扩展] authorize / interaction / attachment intake
│   └── adapter/                        # [扩展] Provider normalize / PG / command outbox
├── connection/                         # [扩展] 能力报告、stream/媒体原连接 reservation
├── delivery/
│   ├── domain/                         # [扩展] retry basis / Progress / recovery
│   ├── application/                    # [扩展] 公平调度 / protocol ACK / 可控恢复
│   └── adapter/                        # [扩展] Provider Sender / PG / Worker proof
├── infra/telemetry/                     # [拟新增] 有界日志/trace/metrics 接线
└── bootstrap/                          # [扩展] 生命周期与依赖装配，仍一个 Composition Root

services/agent-worker/internal/
├── execution/                          # [扩展] session registry/命令/actor/治理
└── ...                                 # Memory/Artifact/Tools 按 owner 独立设计

web/components/channels/                # [扩展] 策略、主体、业务时间线、恢复
web/lib/                                # [扩展] 与真实 Control 契约一致的客户端

deploy/compose/                         # [扩展] feature config / Collector / 显式依赖
helm/agent-platform/                    # [FINAL-INTEGRATION] 全 Workload 完成后再建
```

### 18.4 数据变更与发布纪律

表落在所属 schema，runtime 不获取 DDL/跨 schema 权限。具体迁移号在实施时按主线实际目录分配；本文不预占号码，也不把新策略放进现有秘密配置 JSON。

当前开发期可按 ARC-000 同步收敛 v1 和基线 fixture，不增加仅为开发数据的兼容逻辑。默认对本优化的新实体提供清晰 schema/迁移增量；历史环境若有需要保留的数据，先单独选择保留/重建方案，文档本身不执行清库、降级或推送。

## 19. 分阶段实施与验收矩阵

### 19.1 阶段 A：多租户文本链可交付

顺序建议：A0 先冻结 F01/F03 的身份与 Session 决策；A1 同步 Contract 和 Control 发布；A2 接 Gateway 授权/观测/调度；A3 接 Worker 主体核验；A4 完善 Web；A5 真依赖与真实 IM 验收。

| 交付 | 依赖 | 完成标准 |
| --- | --- | --- |
| F01 主体/策略 | Control 发布、两端投影、Worker 授权上下文 | 负例不建 Run；撤销窗口有界且可观测 |
| F07 限流/恢复基础 | ProviderObservation、PG 预算、Recovery 审计 | 双副本不倍增 Bot 额度；UNKNOWN 不自动重发 |
| F08 分层状态 | 应用观测、消息/Run/Delivery 关联查询 | Web 不把 READY/PUBLISHED 当全链成功 |
| F09 基础观测 | OTLP 配置、trace carrier、审计 outbox | 一条消息跨关键步骤可关联；Secret canary 零泄漏 |
| F10 两 Provider 文本完整链 | 同一最终 build、正式 Binding/Session | 实际用户消息、真实 Worker/Final、重启/重投证据 |
| F12 容量量测 | 上述指标、隔离测试环境 | 有基线/热点/故障恢复压测报告，而非拍脑袋副本数 |

### 19.2 阶段 B：聊天交互增强

先 F02 群触发与 F03 session registry，再做 F04 基础命令；工具审批等 Worker Tools 治理完成后启用。F05 Progress 与 F06 附件各有独立 Provider 能力门禁，不要求一批全部上线。默认关闭未经真实验证的交互能力。

### 19.3 阶段 C：生产优化

完成 F11 的正式结果指标窗口和自动停止/回退，并继续长期归档、正式多后端迁移、生产 HA/容灾。Helm 归 FINAL-INTEGRATION，不作为本优化设计阶段的实际交付。

### 19.4 可执行验收场景

| ID | 输入 / 故障 | 必须观察到的事实 |
| --- | --- | --- |
| A01 | 未授权外部用户发私聊 | 明确拒绝；零 Run；有稳定审计 |
| A02 | 两租户同 external_user_id/chat_id | 主体、Session、文件、诊断查询完全隔离 |
| A03 | 同事件并发从两个 Gateway 进入 | 一个首次 Receipt/Admission/Run，其他返回原事实 |
| A04 | 授权撤销、投影中断、旧输入重投 | 新输入在定义窗口后拒绝；旧 Receipt 不重执行 |
| G01 | 群内普通消息/错误 @/正确 @ | 仅满足授权与触发的消息形成输入 |
| S01 | 共享/个人群 Session、并发 reset | 作用域明确，generation 单次推进，旧结果不污染新 head |
| I01 | 按钮重复、转发、他人点击、过期 | 仅合法主体一次决定；协议反馈与业务结果分离 |
| P01 | Progress 乱序/在途未知/Final 先到 | Final 不被新启动的旧 Progress 覆盖；未知语义明确 |
| M01 | 恶意 URL/超大/跨租户/对象不可用 | 有界拒绝；无越权读取、无限下载或假可用引用 |
| D01 | 429 + RetryAfter、双副本限流 | 时间和次数有界，低流量租户仍可获服务 |
| D02 | send 已开始后断网/强杀 | UNKNOWN，不重复模型或盲目重发 Final |
| D03 | 超长 Final 前片成功、后片限流或 UNKNOWN | 只恢复明确可重试的片；已成功前缀不重复发送 |
| Q01 | 双 Worker 同时占额、模型 usage 未知、工具授权撤销 | 原子预算不被副本绕过；未知不提前退款；工具按有效权限执行 |
| O01 | Applied/Connected 报告乱序和过期 | Web 精确展示当前 revision 和新鲜度 |
| O02 | Secret/PII canary + Collector 离线 | 零泄漏；业务继续；观测丢弃有计数 |
| N01 | 两 Gateway/两 Worker owner 接管 | 旧 fence 失效，只有合法 Attempt 可提交 |
| N02 | 企微原连接失效后新连接建立 | 旧 ReplyOrigin 不借新 socket 发送 |
| R01 | 灰度、配置回滚、旧 Run 晚完成 | 同 cohort 稳定；旧执行/回复仍按原目标 |
| C01 | PG/NATS 短断与所有保留预算满额 | 拒绝新占用，保留旧 Receipt/在途恢复通道 |
| B01 | 后端复制、增量追平、切换和回灌 | ID/摘要/水位一致；切换后新写入不会在回滚中丢失 |
| L01 | 真实 Telegram polling 完整两轮 | 原生输入、正式 Session 延续、Final 与重复事件证据 |
| L02 | 真实企微正式 Binding 完整两轮 | 非 smoke 链、正式 Run/Session、原连接 Final 证据 |

每条场景均记录 build、配置 revision、输入、命令、退出码与业务断言。Go/前端单测、真实 PG/NATS、真实 Provider、生产部署四种证据分栏；Skip 保持为 Skip。

### 19.5 实施时可复用的任务描述

```text
围绕 Channel 优化项 Fxx 实现一个有验收结果的纵向切片。
先核对主线源码和已接受约束，说明现状缺口与新增理由。
确定 Control、Gateway 四 Module、Worker、Web 的事实所有者；
同步现有开发期 v1 契约及 fixtures，不增加空兼容层或独立 Connector 部署。
先写复现/负例，再修改实现；检查重复事件、跨租户、失租、UNKNOWN 和恢复。
只在专用测试环境运行有副作用的验收，输出源码测试与真实依赖证据的区别。
不要在没有明确请求时切换 Bot、重启当前部署、清理共享数据库或推送 main。
```

## 20. 待确认决策、取舍和引用

### 20.1 建议默认值及待确认点

| 决策 | 本文建议 | 为什么 / 后续确认 |
| --- | --- | --- |
| 新策略默认权限 | DENY_ALL，显式 allowlist / PUBLIC_LIMITED | 避免公开 Bot 自动取得租户资源；先确认产品用户邀请流程 |
| 群 Session | 默认按用户隔离，显式选择共享 | 降低跨用户历史暴露；共享助手场景可主动选择 |
| 访问撤销窗口 | 初始评估 30 秒投影新鲜度上限 | 与可用性取舍；通过故障测试确认，不写成已达成 SLA |
| Reset 顺序 | Worker 事务提交顺序 | 不引入全 Provider 消息时钟/全局排序假设 |
| 限流后端 | 首期共享 PG，有指标后再评估 Redis | 已有依赖，先获得正确多副本预算语义 |
| Telegram Progress | 先选已验证路径，Final 独立展示 | 降低在途未知进度覆盖 Final 的风险 |
| WeCom 回调丢失 | 不承诺永久补发；测量并暴露窗口 | 以真实协议为准，不把本地 lease 当 Provider 队列 |
| 灰度 | 先租户/账户整体切换，后 cohort | 先复用单目标 Binding；避免提前引入复杂路由 |
| 工具审批/附件/Memory | owner 能力具备后分别启用 | 接入层不能替代完整工具和数据能力 |
| Helm | 全 Workload 完成后 FINAL-INTEGRATION | 与已确定开发顺序一致 |

这些是设计建议，不是文档生成后立即启用的配置。需要产品确认的策略必须在对应实现任务开始前冻结。

### 20.2 被放弃的做法

- **独立 Node WeCom Connector**：增加部署、身份和跨进程故障成本，现有公开 Go 包已经覆盖真实基础协议。
- **按用户 sticky 到 Worker**：把会话一致性绑在机器上，影响恢复且不能替代共享账本。
- **Gateway 内万能 DataStore**：混合 Session、Memory、Artifact 的不同一致性需求，增加耦合。
- **每消息同步 Control 最新配置**：违反已发布目标和运行投影设计，也放大 Control 故障影响。
- **所有错误自动重试**：UNKNOWN 外部副作用可能重复；需要按确定性和操作建模。
- **先开放群聊再补权限**：一旦消息接纳并调用模型，后补配置无法撤销已产生的数据和费用影响。
- **把一次预检 PASS 作为全链上线门禁**：认证事实不证明 Binding、Session、模型或 Delivery。
- **当前优化先做 Helm/第三个 IM**：不能解决现有两种渠道的主要使用和运营缺口。

### 20.3 参考资料与证据优先级

源码/当前测试用来判断“已经实现什么”；官方协议用来判断“外部平台提供什么”；本文用来说明“准备新增什么”。三者不能互相替代。

**仓库依据**

- [架构约束](../constraints.md)
- [Gateway 四 Module](module-boundaries.md)与[术语](CONTEXT.md)
- [Gateway 服务说明](../../../services/channel-gateway/README.md)
- [Telegram 输入规范化](../../../services/channel-gateway/internal/admission/adapter/inbound/telegramadapter/normalize.go)
- [企微入站 Handler](../../../services/channel-gateway/internal/admission/adapter/inbound/wecomadapter/handler.go)
- [当前 SessionScope](../../../services/agent-worker/internal/execution/domain/run.go)
- [投递重试策略](../../../services/channel-gateway/internal/delivery/domain/policy.go)
- [Worker 能力与正式 Session](../../../services/agent-worker/README.md)
- [WeCom 预检](wecom-preflight-v1.md)与[main 合入审查](wecom-preflight-main-review.md)

**官方资料，查阅日期 2026-09-07**

- [Telegram Bot API](https://core.telegram.org/bots/api)：接收方式、callback 应答、错误等待信息等协议事实。
- [Telegram Bots FAQ](https://core.telegram.org/bots/faq)：群消息可见性及流量约束的补充入口；上线按具体账户能力验证。
- [企业微信官方 Node SDK](https://github.com/WecomTeam/aibot-node-sdk)：用于核对协议和能力，不代表本平台需要 Node 部署。
- [OpenTelemetry messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)：异步消息关联与 span links；发布时固定实际采用的规范版本。

本文不承诺 SDK 所有最新能力都已经进入当前固定 Go 依赖。新增方法、媒体限制、群消息权限和频率参数均须在实现时再次核对，并进入 Provider 契约和真实账号验收。
