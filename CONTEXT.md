# CONTEXT（术语表 · 登录与身份域 + IM 接入域）

> 单上下文仓库。本文件定义登录/身份域与 IM 接入域的**标准词汇**；方案/决策详见
> `docs/adr/0001-enterprise-login-identity.md`、`docs/adr/0002-im-gateway-sdk.md`、
> `docs/adr/0003-platform-user-channel-identity.md`、`docs/adr/0004-channel-neutral-direct-session.md`、
> `docs/adr/0005-channel-identity-linking.md`。
> 如有冲突，以 ADR 为准；术语在对话、代码、文档中必须使用同一词汇。

## 开发期约束（用户拍板）

- **开发阶段，不兼容优先**：当前处于开发中，所有实现**不保留前面的残余设计**、不做向后兼容。旧表结构、旧接口、旧前端状态可以随实现**破坏性重写**（开发库允许重建）；发现"为将来预留但现在无用"的字段/表/接口，一律砍掉，需要时再按当时需求设计。
- 推论：已经进入产品契约的登录 Provider、租户显示名称、Platform User、渠道身份关联直接按当前最终模型落库，不保留旧结构双轨。
- 标注：若出现"迁移兼容旧数据"的需求，除非用户明确要求，一律不做。

## 身份与账号

- **平台实例（Platform Instance）**：一套部署中的平台边界。它可以服务企业、学校、实验室、项目组、托管客户等任意场景，产品模型不假设其对应某种组织实体。
  - 避免同义词：企业、Organization、部门（当指平台最上层边界时）。
- **平台用户（Platform User）**：平台内表示“同一个人”的稳定 canonical identity，以全局唯一 `platform_user_id` 标识；登录身份与消息渠道身份都只能映射到它，不能替代它。
  - 避免同义词：账号、员工、成员（当指“人”时必须叫平台用户）。
- **登录身份（Login Identity）**：用于进入 Console 的身份凭据映射。正式支持 Local、企业微信、飞书和 OIDC；不同登录方式只能通过显式关联汇聚到同一个 `platform_user_id`，不得按邮箱或姓名自动合并。
- **本地登录身份（Local Login Identity）**：平台内建的用户名/密码登录方式。Local 用户由系统管理员创建，不开放匿名自助注册；密码材料与 Platform User 基本资料分开保存。
- **自举管理员（Bootstrap Admin）**：平台实例首次初始化时建立的首个系统管理员。它只解决“系统尚无可登录管理员”的冷启动问题，初始化完成后不持续作为权限来源。
- **临时密码（Temporary Password）**：系统为新建或重置的 Local 登录身份生成的一次性初始凭据；用户首次成功登录后必须更换，管理员不能再次读取原值。
- **外部身份（External Identity）**：外部 IM 中的发送者身份，例如企业微信 `userid`、飞书 `open_id/user_id`、Telegram user id。唯一边界是 `(tenant, channel, binding, external_user_id)`；首次出现时即可建立轻量身份记录，并允许 `platform_user_id` 为空，不要求先成为 Platform User。
  - 避免同义词：渠道账号、IM 用户、Channel Identity（当指领域实体时统一叫外部身份）。
- **可信身份解析（Trusted Identity Resolution）**：只有当消息通道能够在同一可信 Provider 边界内提供与 Login Identity 相同的稳定主体标识时，才允许自动把 External Identity 关联到 Platform User；姓名、邮箱、昵称等弱标识不得用于自动合并。
- **登录身份键（Login Identity Key）**：`provider_id + provider subject`；企业微信、飞书、OIDC 还分别校验各自 Provider 边界。它不是平台用户主键。
- **租户成员（Tenant Member）**：平台用户与某个租户之间的关系，存储于 `tenant_members`，权限挂在关系上。同一个平台用户可以属于零个、一个或多个租户，并在 Console 中切换自己有权限的租户。
  - 避免同义词：部门成员、企业用户、租户用户。
- **租户显示名称（Tenant Display Name）**：面向用户展示的租户名称；租户 ID 仍是稳定机器标识，只作为二级信息展示。
- **租户角色（Tenant Role）**：`admin` / `member` 两档，挂在 `tenant_members`。运行能力差异若未来需要细分，优先表达为 permission/policy，而不是新增用户角色。
- **系统管理员（System Admin）**：当前平台实例的全局管理授权，存储于 `system_admins`。系统管理员可以管理租户生命周期、平台用户、模型资产与身份源，但不会因为系统管理员身份自动获得每个租户的业务数据读取权限。
- **平台管理权（Platform Administration）**：System Admin 管平台级资源与 Tenant 生命周期，不等同于 Tenant Admin。若要读取或修改某个 Tenant 的 Agent、知识、会话、执行记录等业务数据，仍必须具备该 Tenant 的 `admin` Membership。
- **管理员不变量（Admin Invariant）**：一个 Active Tenant 始终至少保留一个 Active Tenant Admin；平台实例始终至少保留一个可用 System Admin。最后一个管理员不能通过普通产品操作被移除、降级或停用。

## 登录与认证

- **身份提供方（Identity Provider）**：登录方式的抽象接口（`IdentityProvider`）。正式产品身份源包括 Local、企业微信、飞书和 OIDC；部署时可以启用一种或多种，开发环境可额外启用 Mock 测试登录。
- **登录入口与消息入口分离**：Telegram、飞书、企业微信智能机器人等 Channel 只负责消息接入，不因为某个 Channel 可聊天就自动成为 Console 登录方式；Console 登录只通过明确配置的 Identity Provider 完成。
- **企业微信登录（WeCom Login）**：使用企业微信网页扫码登录 `wwlogin/sso/login`（`login_type=CorpApp`）→ 回调 `code` → `auth/getuserinfo` → 得到稳定 `userid`。
  - ⚠️ 不是「微信开放平台登录」（openid/unionid，无企业组织架构）——那个不接。
- **飞书登录（Feishu Login）**：跳转飞书 Web OAuth 官方授权页，回调后通过 `authen/v1/user_info` 获取 `tenant_key` 与稳定用户标识。
- **OIDC 登录**：标准 Authorization Code + PKCE + nonce；身份键使用 `(issuer, sub)`，不按邮箱自动合并用户。
- **第三方首次登录建号**：已配置并受信任的企业微信、飞书、OIDC 身份首次成功登录时可创建 Platform User；新用户可以暂时没有任何 Tenant Membership。
- **登录身份关联（Login Identity Linking）**：已登录用户可以通过再次完成目标 Provider 的认证，把新的 Login Identity 显式关联到自己的 Platform User。禁止根据邮箱、姓名等弱标识自动合并。
- **登录回调 URL（Login Callback URL）**：企业微信、飞书、OIDC 共用 `LOGIN_CALLBACK_URL`，固定路径 `/api/v1/auth/callback`。外部身份平台后台登记值必须与它完全一致；不得在各 Provider 内重复配置不同的回调地址。
- **Mock 登录**：仅开发/测试使用。`LOGIN_PROVIDER=mock` 可启动纯 Mock；`LOGIN_MOCK_ENABLED=true` 可在当前正式登录入口之外额外显示 Mock。
- **登录会话（Session）**：登录态载体，Redis 存储，HttpOnly Cookie（`SameSite=Lax`）承载 Session ID；TTL 默认 8 小时可配置。
  - 避免同义词：token（JWT 不用）、登录态。
- **当前租户（Current Tenant）**：由前端路由/页面上下文决定，不写入登录 Session。每个租户请求都携带 `tenant_id`，后端基于当前 `platform_user_id` 实时校验 Membership 与角色，允许不同浏览器标签页同时处于不同租户。
- **CSRF Token**：写操作防跨站校验。Double-Submit Cookie + 自定义头 `X-CSRF-Token`，绑定 Session。

## 相关已有概念

- 审计（Audit）：`audit_events` 表；新增动作 `user_login_success` / `user_login_failure` / `user_logout` / `user_session_expire`。
- **租户（Tenant）**：平台中的第一等业务、配置与数据隔离单元。UI 与后端统一使用“租户”，不再把它表述为部门或工作区。
- **应用（Application / Agent App）**：属于某个 Tenant 的 Agent/机器人配置主体。
- **租户状态（Tenant Status）**：租户当前是否可以继续接受新业务流量。当前产品语义只区分 `active` 与 `suspended`；停用不会等价于删除历史数据。
- **Agent 生命周期（Agent Lifecycle）**：Agent 从未发布配置到可服务再到停止接收新请求的状态边界，统一使用 `draft` / `active` / `disabled`；历史 Session、Execution、Audit 不因禁用而删除。
- **知识库（Knowledge Base）**：Tenant/Agent 范围内的共享知识，由 Tenant Admin 管理；它不是个人长期 Memory。
- **我的偏好（My Preferences）**：当前 Platform User 在当前 Tenant 范围内的长期个人 Memory。Tenant Admin 不因管理 Knowledge 而获得读取所有成员私人偏好的权限。
- **模型资产（Model Asset）**：由平台实例集中管理的模型 Provider、模型能力与凭据资源。Tenant 只获得被授权的模型集合，Agent 再从该集合中选择使用的模型。
- **后端档案（Backend Profile）**：由平台实例集中管理的一组数据后端连接与凭据。Tenant 通过 Storage Policy 选择被授权的 Profile，而不是直接持有数据库、Redis、向量库或对象存储密钥。

## IM 接入域

- **通道（Channel）**：IM 协议种类，`telegram` / `wecom`（企业微信）/ `feishu`（飞书）。
  同一通道可有多个 Binding；组合根按 `(channel, binding_id)` 建立常驻 Receiver/Sender。
  外部 IM 不暴露平台 Webhook 路由，统一由 Gateway 的 Connector Manager 维护长连接/长轮询。
- **通道绑定（Binding）**：一个具体外部 IM 账号/机器人接入到一个确定租户 Agent 的关系；每个 Binding 只属于一个 Agent，一个 Agent 可以拥有多个 Binding。`binding_id` 是外部标识非密钥；Binding 只保存平台允许的 `credential_ref`，运行时解析整组 binding 级凭据。
- **通道访问策略（Channel Access Policy）**：决定某个外部通道谁可以与 Agent 对话。当前语义为 `public` / `allowlist` / `member_only`；它约束 External Identity 的准入，不要求所有外部用户都先注册 Platform User。
- **默认通道访问策略（Default Channel Access Policy）**：新建 Channel Binding 默认使用 `member_only`。只有 Tenant Admin 显式选择后，Binding 才可变为 `allowlist` 或 `public`。
- **Agent 可见性策略（Agent Access Policy）**：租户成员默认可使用当前租户已启用的 Agent；如需收紧到特定成员或群组，使用 Agent ACL，而不是引入更多 Tenant Role。
- **主体停用（Principal Suspension）**：External Identity 若已关联到 suspended Platform User，则该外部身份拒绝继续交互；未关联 Platform User 的 public 外部用户不受此规则影响。Identity Link 永远不能替代 Tenant Authorization。
- **通道密钥所有权（Channel Credential Ownership）**：Tenant Admin 管自己 Tenant 下 Channel Binding 的供应商凭据；System Admin 只管理平台级 Secret Provider / Vault / KMS 等密钥基础设施。凭据写入后只允许替换，不通过 Console 回显明文。
- **连接器管理器（Channel Connector Manager）**：通过 Redis 分布式租约选举 Leader 节点统一调度常驻连接器进程，实现免公网穿透、免域名回调的统一外部通道接入。
- **发送器（Sender）**：`channels.Sender` 接口，承载归一化回复 → 外部通道消息；由 Outbox 调度器异步调用（不阻塞 agent 链路）。
- **入站消息（InboundMessage）**：通道无关的归一化消息（message_id / channel / conversation_id / sender_id / text / files），进 Kafka envelope 的唯一形态。
- **消息参与者（Actor）**：一条消息的真实发送者。私聊中通常也是会话用户；群聊中仅表示“谁发送/触发了这条消息”，不改变群会话本身的归属。
- **群会话（Group Session）**：群聊 Session 归属于外部群或线程上下文，而不是当前 Actor。平台可以记录每条消息对应的 Actor，但默认不读取或写入任何 Actor 的私人长期 Memory。
- **触发方式（Trigger Type）**：机器人处理该消息的入口原因，当前统一使用 `direct` / `mention` / `command`。群聊应保留 Actor 与 Trigger Type，便于解释“谁通过什么方式触发了机器人”。
- **通道会话关联（Channel Conversation）**：外部 `(channel, binding_id, conversation_id)` 到 Session 的有生命周期映射。不同 Channel/Binding 的 Conversation 默认保持独立，不因 External Identity 后续关联到同一个 Platform User 而自动合并；归档会结束当前关联但保留历史。
- **我的会话（My Sessions）**：当前平台用户在当前租户中拥有的 direct Session 视图。Web、企业微信、Telegram、飞书等入口各自保留独立 Session；同一 Platform User 可以跨入口共享长期 Memory/用户偏好，但聊天历史不自动合并。群聊、其他用户以及未关联外部身份不可见。
- **Web 原生入口（Web Native Entry）**：Console/Web Chat 是平台自身入口，不创建伪造的 Channel Binding；运行时仍可统一标记为 `channel=web`，但不存在外部凭据、Webhook 或外部账号语义。
- **租户会话（Tenant Sessions）**：Tenant Admin 对当前租户 Session 的管理视图。默认只展示用户、Agent、Channel、会话类型、消息数量、最近活动与状态等元数据，不默认授予其他用户私聊正文读取权。
- **会话内容审计（Conversation Content Audit）**：允许被显式授权的管理员读取其他用户会话正文的高敏感能力；默认关闭，且每次读取本身也必须形成审计记录。
- **外部身份目录边界（External Identity Boundary）**：当前不提供独立“外部用户/联系人”目录。External Identity 只在会话、Channel allowlist、账号关联等真实场景中出现，不能混入 Tenant Member 列表。
- **临时输入附件（Input Artifact）**：IM/Web 上传的图片或文件先暂存到框架 `artifact.Service`，Kafka 只携带引用；执行成功后回收，失败/重试时保留。它不是“运行产出”，控制台产出列表不展示 `input/` 前缀。
- **智能机器人长连接（WeComBot WebSocket）**：免公网穿透/免域名的企业微信 API 模式智能机器人通道（`wss://openws.work.weixin.qq.com`），主动发起连接，通过 `aibot_subscribe` 握手、`ping` 心跳、`aibot_msg_callback` 接收并归一化消息，以及 `aibot_send_msg` 异步 Markdown 回复。天然支持单聊与群聊。
- **电报长轮询（Telegram Poller）**：免公网 Webhook 的 Telegram Bot API 长轮询器，通过 `getUpdates` 拉取更新并单调递增推进 `offset`，支持网络重连与 429 `retry_after` 动态退避。
- **飞书长连接（Feishu Connector）**：基于飞书开放平台官方 `channel-sdk-go`；同一 Binding 复用同一个 Channel 完成长连接事件消费与出站发送，自动处理群聊 `@bot`、Markdown 富文本、消息分片、重试和媒体上传。持久 Outbox 仅在按稳定消息 ID 更新/撤回既有消息时通过 `RawClient()` 使用底层 OpenAPI。

## 平台数据与 Agent 执行生命周期域（Framework-Native）

- **防护令牌（Fencing Token）**：会话级别的单调递增防护令牌；持久化层在更新会话状态（`sessions`）时断言 `fencing_token > current_revision`，从根本上杜绝 Kafka Rebalance 或 Worker 长耗时导致的脑裂旧执行脏写。
- **执行清单（Execution Manifest）**：Gateway 与 Worker 之间的签名执行契约；携带租户标识、应用编码、配置版本、Fencing Token、Trace ID 与 HMAC 签名，确保 Worker 无状态且仅执行经过 Gateway 准入签发的可信任务。
- **平台会话投影（Platform Session Projection）**：平台 `sessions` 表中仅维护租户、应用、会话标识、framework subject、可选 `owner_platform_user_id`、最新消息 ID、修订号与归档状态的路由与管理投影；对话权威历史与事件生命周期由框架 `session.Service` 全权负责。
  - 避免同义词：会话历史记录、Session KV、平台上下文。
- **会话摘要（Session Summary）**：由框架内置 Summarizer 结合租户模型自主生成的权威语义摘要，归属于框架 Session 生命周期；平台不持久化第二份 summary。
  - 避免同义词：平台摘要、伪摘要。
- **用户偏好（Framework Long-Term Memory）**：底层仍使用框架官方 PostgreSQL `memory.Service`，按 `AppName (tenant_id/app_code) + subject_id` 严格隔离。已关联 direct 用户的 subject 是 `platform_user_id`，因此可跨 Web/企业微信/Telegram/飞书复用；未关联 direct 使用 binding-scoped fallback subject；群聊使用 group subject，绝不以 Actor 的 Platform User 读取或沉淀私人偏好。平台采用保守自动提取：只沉淀明确且稳定、未来仍有复用价值的偏好/长期约束，普通事件、临时任务和一次性要求默认不保存；Agent 通过 `memory_search` 按需检索，并只预载少量高相关偏好。控制台产品名称统一为“用户偏好”。
  - 避免同义词：用户档案（Profile）、生活日志、KV 记忆、Last Reply。
- **运行产出（Runtime Artifact）**：Agent 或 Tool 在真实执行中写出的带版本文件，由框架 `artifact.Service` 保存。控制台只查看和下载，不当成附件、知识库或聊天记录。
  - 避免同义词：制品、运行制品、附件、用户文件、聊天 JSON。
- **执行追踪安全投影（Execution Trace Projection）**：平台从 Runner completion event 中提取的框架真实执行轨迹投影（保存于 `execution_traces`），仅包含调用拓扑、状态、耗时与 token usage，不持久化原始输入输出与工具敏感载荷；与可靠性事实（Claim、Outbox、重试）正交并分区呈现。
  - 避免同义词：假步骤（Fake steps）、执行阶段推导、审计明细代替 Trace。
- **租户执行记录（Tenant Execution Records）**：Tenant Admin 可查看当前租户的安全执行投影，用于排障和成本观察；它不默认包含原始 Prompt、完整模型输出或敏感工具参数。
- **框架知识检索（Framework Knowledge Retrieval）**：租户知识文档通过框架 `knowledge.Knowledge` 契约挂载，模型通过工具调用自主决定检索时机；Runtime 严禁在调用前手工检索或拼接 Prompt。
  - 避免同义词：知识 Prompt 注入、手工 top-N 检索。
- **知识导入任务（Knowledge Ingest Job）**：复杂文档上传后的持久化抽取任务。TXT/Markdown/JSON/CSV/DOCX 优先使用 `trpc-agent-go` Reader；PDF/PPTX/XLSX/HTML/图片等在配置 Docling 时使用框架 Docling Extractor。任务以租约抢占，最终写入现有 Knowledge Store，不创建第二套知识库。
