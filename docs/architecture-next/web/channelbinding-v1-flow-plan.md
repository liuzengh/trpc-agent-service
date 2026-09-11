# ChannelBinding V1 前端设计与实现记录

> 2026-09-07 增量：双接收方式已在 Web 实施。下文的首版 config 全只读、Telegram 两项凭据必需等是历史基线；当前仅开放停用态的结构化 config.receive_mode 修改，生成字段仍只读。新建默认 LP、模式化凭据/预检、旧 pending 兼容与独立启用确认见 [双模式实现记录](./telegram-receive-modes-v1-plan.md#15-实施记录2026-09-07)。运行验收另记。
- 日期：2026-09-06。
- 状态：2026-09-06 已实现渠道列表、新建账户和账户工作台三个页面及协议/组件测试；首版 config 只读与双开关影响确认已落实到源码。容器、HTTP 与真实 Provider 验收分别记录，不由“页面已实现”推定。
- 实现基线：`codex/tenant-rbac-workspace` / `661e4a826ce8f539d8f610b3f1ef99cdbe4a4fdd`；最终提交与运行构建状态以对应交付记录为准。
- 工作树：[`仓库根目录`](../../..)。
- 权威顺序：注册路由、关闭请求 Schema、Application/Domain、实际读模型 → 当前设计文档 → 历史记录。
- 配套：[公开 API 契约审计](channelbinding-v1-api-audit.md)。

## 1. 结论：以渠道账户为入口，Binding 是账户内的运行目标

已新增主导航“渠道接入”，不做两套并列的 ChannelAccount / ChannelBinding CRUD。

| 对象 | 用户理解 | 前端责任 |
| --- | --- | --- |
| ChannelAccount | 哪个机器人、如何接入本平台 | Provider、稳定身份、凭据、接入启停、连接观测 |
| ChannelBinding | 此机器人的新消息交给谁 | 固定 Deployment rN、消息路由启停、路由分发信息 |
| DeploymentRevision | 已固定的 Agent 和资源组合 | 复用现有部署版本选择与详情，不在渠道页重配 Models / Tools / Knowledge |

每账户至多一个稳定 Binding；多个账户可以选择同一 DeploymentRevision。账户/绑定属于租户共享管理记录，OWNER 写、有效 MEMBER 读。Provider 和物理机器人身份创建后固定；停用保留身份占用，当前没有删除、跨租户迁移、绑定换账户或一账户多条件分流接口。[S1][S2]

现有产品链补齐为：

```text
Agent 已发布版本 + Runtime Profile 已发布版本
                    ↓
             Deployment 已发布 rN
                    ↓
渠道账户 → 唯一 Binding → 固定 Deployment rN
                    ↓
        控制面启用意图与路由事件
                    ↓
       Gateway 同步、连接与消息接纳
                    ↓
          RunRequested → 后续 Worker
```

Deployment 发布仍不自动切流。Binding 保存/启用也不主动创建 Run；真正接纳由后续消息触发。现有联合验收文档记录了真实 Telegram 入站到持久 RunRequested，不代表 Worker、模型调用和机器人回复完成；真实 WeCom 账户验收仍需单独进行。本次未重做真实渠道收发验收。[S3]

## 2. 原管理面基线：11 个公开管理操作

本节保留原11项管理契约；新增Telegram预检的2条公开、3条私有操作见第13节，仍为实现中/待联调，不将它们倒写为本节历史运行基线。

以下公共前缀均为 `/v1/tenants/{tenant_id}`；浏览器继续通过现有 `/api/control` BFF。

| # | 方法与路径 | 前端入口 | 请求重点 |
| --- | --- | --- | --- |
| 1 | POST `/channel-accounts` | 新增渠道账户 | provider、provider_account_id、name、全部必需 credentials；description 可选；创建后 disabled |
| 2 | GET `/channel-accounts` | 账户列表/选择器 | cursor、page_size；返回 accounts、可选 next_cursor |
| 3 | GET `/channel-accounts/{account_id}` | 账户工作台、写后刷新 | account、可选 binding、distribution、observations 等 |
| 4 | PATCH `/channel-accounts/{account_id}` | 编辑基本信息 | expected_account_revision，name/description 至少一个；当前不接受 config |
| 5 | POST `/channel-accounts/{account_id}/credentials/{purpose}/update` | 单项凭据替换/清除；API 另支持 keep | expected_account_revision + expected_credential_version + action；仅 replace 携带 value |
| 6 | POST `/channel-accounts/{account_id}/enabled` | 启用/停用本平台接入 | expected_account_revision + enabled |
| 7 | POST `/channel-bindings` | 保存首次运行目标 | account_id + target:{deployment_id,revision_number}；创建后 disabled |
| 8 | GET `/channel-bindings` | API client 辅助查询；首版无独立列表页 | cursor、page_size；返回 bindings、可选 next_cursor |
| 9 | GET `/channel-bindings/{binding_id}` | API client 辅助诊断；工作台使用 AccountDetails | binding、路由 generation、分发状态；连接观测去账户详情读取 |
| 10 | POST `/channel-bindings/{binding_id}/target` | 切换/回退固定目标 | expected_binding_revision + target |
| 11 | POST `/channel-bindings/{binding_id}/enabled` | 开启/暂停新消息运行 | expected_binding_revision + enabled |

全部 **7 个写操作**需要 Idempotency-Key，包括 PATCH；所有写操作 OWNER-only，4 个查询供有效 MEMBER/OWNER 读取。创建命令首次及相同请求重放都返回 201，其余成功返回 200；不要套用 Deployment“重放必为 200”的判断。成功以响应结果为准，不通过状态码猜测是否执行了新副作用。[S1][S4]

列表使用游标，不是 Deployment 的 offset/limit：默认 page_size=50、最大100，按稳定 ID 升序而非最近更新时间排序，没有 total、provider/enabled/search 筛选参数。前端使用“加载更多”或游标栈，不伪造总页数或全库筛选；如仅过滤已加载项，要标明“当前已加载范围”。[S4]

Channel DTO 独立定义：Binding 公开 target 使用 `manifest_ref`，不是 Deployment 的 `manifest_id`；公开 Binding 没有 `updated_by`。这些字段不从旧实体设计表或其他模块类型推导，界面不显示虚构的修改人。[S1][S4]

另有 **3 个 mTLS 内部 API**：账户快照、凭据 resolve、连接 observations 上报，供 Gateway 工作负载使用。它们不属于浏览器调用面；渠道管理也不直接调用 Gateway 运维端口。[S5]

## 3. 已实现的页面和导航

| 页面路由（已实现源码） | 内容与操作 | 首版决策 |
| --- | --- | --- |
| `/tenants/{tenantId}/channels` | 账户名称、Provider、物理ID、接入配置启停、凭据配置摘要、更新时间、管理入口 | 简洁列表；不逐行发详情请求制造状态聚合 |
| `/tenants/{tenantId}/channels/new` | Provider/身份/名称/凭据录入，最终确认后创建 | 进入页面不创建对象；取消未提交表单不产生记录 |
| `/tenants/{tenantId}/channels/{accountId}` | 概览、运行目标、凭据与设置、连接诊断 | 账户是稳定工作台，唯一 Binding 嵌入其中 |

账户详情的四块内容：

1. **概览**：身份只读；账户接入意图、消息路由意图分别显示；缺目标/缺凭据等下一步操作明确。
2. **运行目标**：当前 Deployment rN 与 Agent/Profile 来源；选择新目标、前后对比、开启/暂停消息路由；历史版本链接复用现有 Deployment 页面。
3. **凭据与设置**：基本信息独立编辑；每个 purpose 一行“已配置/未配置 + 版本”，逐项替换/清除；账户停用独立操作。
4. **连接诊断**：路由事件分发、Gateway 路由应用未知、逐实例连接观测、最近观测接收时间（received_at）、原因码和技术字段；该时间不是最后入站消息时间。

账户列表当前仅返回 AccountView，没有绑定目标、分发或连接摘要。P0 列表不显示“运行异常数量”“最近消息”“会话成功率”或一个混合在线灯。详情一次 GET AccountDetails 已含可选 Binding 和观测，避免再为同一绑定重复查询。[S4][S6]

Deployment rN 详情已增加“接入渠道”入口：选择现有账户或新建账户；仅携带固定 deployment_id/revision_number。选择现有账户后先读其详情：已有 Binding 时进入“切换目标”确认，不再调用 CreateBinding。目标预选不是自动保存，不发送 latest。[S2][S7]

### 3.1 首版连接 config 只读（已实现）

- Telegram 只读显示服务端返回的相对 webhook_path；WeCom 只读显示 bot_id。Provider、物理 ID
  和 config 都不是现有账户的可编辑基本信息，不渲染可保存输入框。
- 基本信息编辑区只编辑 name/description，PATCH Body 恰好为 expected_account_revision 与至少
  一个 name/description；不提交 config、provider、provider_account_id 或 enabled。
- “只读”不是先隐藏控件再把旧 config 合并回请求；API client 和组件测试都检查发送字段集合。
  控制面文档中的 config 可编辑表述已在本轮纠正；页面实现与组件测试已覆盖关闭请求字段集合。源码实现不等于运行实例已经更新。[S8][S12][S13]

## 4. 核心流程：先配置齐全，再分别启用

### 4.1 首次接入四步

**第 1 步：创建渠道账户，保持停用。**

- Telegram：数字 Bot ID（字符串保存，不用 JS Number，不填 @username）、Bot Token、Webhook Secret、名称/说明。
- WeCom：SDK Bot ID、Bot Secret、名称/说明；不替换成另一套 CorpID/AgentID/回调 AES 表单。
- Telegram Webhook Secret 已提供显式“生成随机值”按钮，生成符合1～256位 ASCII字母/数字/下划线/连字符的值；只在当前表单内存使用。服务端同时检查字节和 Provider 约束。
- 当前 Create 必须一次给全所需 purpose 的 replace 动作；不存在先存空凭据 Draft 的路径。
- provider、provider_account_id 创建后锁定；首次确认必须展示这一点。用户取消创建后不留对象；创建成功后的中断保留已经生成的账户，不假装回滚或删除。[S1][S8]

**第 2 步：选择已发布 Deployment rN，保存 Binding，保持停用。**

- 使用现有 Deployment list/listRevisions/getRevision；只选择本租户精确发布版本。
- 展示“部署名称 · rN / Agent vN / Profile rM”，Digest 折叠，不让用户手填 Manifest ID。
- 没有可用发布版本时，跳转现有部署页面；返回只恢复非秘密目标选择和 account ID。
- Account disabled 时允许创建 Binding；保存成功只表示目标固定，不接入消息。
- 没有单独的 Channel validate 或 publish API；前端做字段预检查，实际目标校验由 CreateBinding/SetTarget 执行。[S2]

**第 3 步：显式启用渠道接入。**

- GET 当前账户版本后提交 Account enabled=true；成功文案为“接入配置已启用，等待 Gateway 同步/连接”。
- Telegram 启用会驱动身份核对和 setWebhook 注册，可能改变该机器人的已有 Webhook 指向；确认内容明确这不是无副作用试连。
- 公网 origin 来自平台配置，API 只返回相对 webhook_path；首版只展示真实路径，不用浏览器 localhost 拼完整回调URL。
- 显示异步连接观测，允许稍后继续；不把 READY 观测额外设置为服务端没有要求的硬前置。[S8][S9]

**第 4 步：显式开启消息路由。**

- 账户已经 enabled、必需凭据已配置、Binding 已存在；再次确认固定 Deployment rN。
- 提交 Binding enabled=true。成功显示“路由配置已保存”，分发/连接分别更新。
- 不显示“机器人已成功回复”，不将等待页设计成没有退出入口的永久加载。

四步是多个独立命令，不是原子事务。每步显示已完成结果；失败从当前步骤继续，不自动从头重建，不因第4步失败而撤销前3步。

### 4.2 双开关及恢复语义

```text
控制面有效路由意图 = account.enabled AND binding.enabled
```

| Account | Binding | 界面解释 |
| --- | --- | --- |
| false | 无 / false | 接入配置已停用；无开启的路由意图 |
| true | 无 / false | 接入配置已启用；新消息路由未开启 |
| true | true | 控制面已开启路由；传播与连接观测另看 |
| false | true | 接入已停用，路由启用意图仍保留；重新启用按服务端当时保存的 Binding 状态恢复 |

**操作名固定为“启用本平台接入 / 停用本平台接入”和“开启消息路由 / 暂停消息路由”。**
“重启”在账户业务页仅指停用后的重新启用，不是容器重启、SDK 重连或原子重启 API。
账户命令本身保留 binding.enabled 的值，不代表把停用时的目标/意图保存成以后必然恢复的快照；
其他 OWNER 仍可显式修改 Binding。需要路由意图关闭时，用户先暂停 Binding，再停用 Account；
这是两次独立写，不构成原子总开关。每步显示完成与未完成的状态，中断不自动撤销已完成步骤。[S2][S4]

#### 4.2.1 确认框共享事实区

工作台打开写操作确认框时重新 GET AccountDetails；显示账户名、Provider/物理 ID、account_revision、
可选 binding_revision、Binding enabled、固定 Deployment ID+rN，以及前端本次 GET 成功的读取时间。
读取时间只是前端事实快照时间，不冒充服务端版本；Agent/Profile 来源在运行目标区展示。
新建账户尚无 AccountDetails，使用本次已校验输入确认固定身份和创建后停用，不伪造服务器快照。
加载失败时不使用旧缓存直接确认；确认期间读到新版本/目标时显式展示变化并要求重新确认，
不得静默替换原提交 Expected。最近读取快照不是目标锁；下述并发边界始终适用。

#### 4.2.2 四类动作确认内容（已实现的语义与验收要求）

当前共享确认框的提交按钮为“确认此操作”，取消按钮为“取消”。下表列出各动作标题与影响要点；源码使用简洁文案呈现这些边界，不要求逐字复制整段说明。

| 动作 ID / 标题 | 确认正文与条件分支 | 提交和成功结果 |
| --- | --- | --- |
| `ACCOUNT-DISABLE` / **停用本平台接入？** | “将保存此账户的停用配置。Gateway 应用后将停止此账户接入和新消息接纳。账户、凭据和固定目标保留，不取消已接纳 Run；后续回复仍受账户发送资格约束。” 若读取到 Binding enabled=true，追加“消息路由的开启意图将保留；以后重新启用接入时，若该意图仍开启，会按服务端当时保存的目标恢复。” 若 false，显示“路由当前已暂停，停用接入不会替你开启路由。” 若无 Binding，显示“当前未配置消息路由。” Telegram 追加“不承诺删除远端 Webhook；此操作仅停用本平台接入。” | Account enabled=false + expected_account_revision；“接入停用配置已保存，等待 Gateway 同步”，随后 GET 展示最新事实，不宣称各副本已瞬时停用 |
| `ACCOUNT-ENABLE` / **启用本平台接入？** | “将启用账户接入配置，等待 Gateway 同步与认证。最近读取的路由意图：{开启/暂停/未配置}；最近读取目标：{Deployment rN/无}。提交时以服务端保存的 Binding 状态为准。” 若当前意图 true，突出“重新启用可能同时恢复新消息路由，目标按服务端当时状态决定，不锁定本框展示值。” 若 false/无，显示“按最近读取状态，本次仅恢复接入；并发管理员仍可能改变 Binding。” Telegram 追加“启用会触发身份核对和 setWebhook 注册，可能改变此机器人的已有 Webhook 指向，这不是无副作用试连。” | Account enabled=true + expected_account_revision；“接入启用配置已保存，等待 Gateway 同步/连接”，随后 GET 查看实际 Binding 意图与目标 |
| `BINDING-DISABLE` / **暂停消息路由？** | “将把此 Binding 的路由意图设为暂停。账户接入配置、凭据和固定部署目标保持不变；传播生效后不再按此路由接纳新消息，不取消已接纳 Run，已有回复仍按原 ReplyOrigin 和账户发送资格处理。” 追加“保留目标不等于保留开启意图；恢复需再次显式开启消息路由。” | Binding enabled=false + expected_binding_revision；“路由暂停配置已保存”，随后 GET，不写“机器人连接已关闭” |
| `BINDING-ENABLE` / **开启消息路由？** | “将把新消息路由到 {Deployment rN}。本次命令以 Binding revision {revision} 校验目标，若提交前被修改则返回冲突；传播后仍需满足 Gateway 连接和路由资格。不会自动运行一条消息，不取消或重定向旧 Run。” 账户未启用时先展示“请先启用本平台接入”，不隐式提交 Account enabled=true。 | Binding enabled=true + expected_binding_revision；“路由开启配置已保存”，随后分别展示事件分发与连接观测，不写“已上线/已回复” |

每个确认框有“取消”按钮；提交中禁用重复确认，保留原 key/body。写成功但 GET 失败时显示
“操作已接受，最新状态读取失败”，仅重试 GET。401/403/409 不乐观更新开关；冲突保留非秘密
输入和原意图，由用户核对最新事实。历史 Receipt 重放后同样 GET，不用旧回执覆盖当前状态。[S4]

#### 4.2.3 服务端当时目标与并发边界

账户 enabled 命令仅接收 expected_account_revision；不会接收 expected_binding_revision 或 target。
Binding-only 改目标/启停不推进 account_revision。**账户确认框显示的是最近读取事实，恢复依据是
服务端提交时的有效 Binding 状态，不保证恢复的就是用户此前看到的目标。**

示例：T0 界面读取 Account revision=7、Binding revision=2、enabled=true、目标 A；T1 另一
OWNER 将 Binding 改为目标 B、revision=3，Account 仍为7；T2 原界面提交 Account enabled=true
和 expected_account_revision=7；T3 服务端可能成功启用账户并恢复 B，而不是 A。这不构成账户 CAS
失效，因为当前 DTO 没有对 Binding revision=2/A 声明条件。[S4][S12]

服务端在准备阶段读取/校验其当时目标，并在账户锁事务内比对；若准备与写入间目标变化，会重做
准备或返回依赖错误，不提交与当次有效目标不一致的已准备结果。该服务端内部一致性检查不等于
锁定客户端确认值。另一 OWNER 在提交前暂停 Binding 时，账户启用可只恢复接入；提交后又修改
Binding 时，写后 GET 也可能已经显示较新的状态。

Binding enabled 命令则以 expected_binding_revision 约束目标和启停意图；同一 Binding 的目标或
enabled 若先被修改，旧 Expected 会返回 409。它不锁定账户状态；并发账户停用会由同一账户锁及
启用前提校验阻止错误开放。暂停 Binding 后再停用 Account 的两步流程也不能保证无人在两步之间
修改 Binding，界面只承诺各自已确认的命令结果。

如果产品要求“精确恢复刚确认的目标”或“一次原子停用接入并清除路由意图”，需要新增服务端条件
命令，同时校验 Binding 版本/目标或在同事务修改双开关；首版不增加未知字段，不用前端串联冒充
该保证。当前首版以显式影响文案、读取版本与时间、写后事实展示呈现这一边界。[S4][S12]

### 4.3 切换目标和凭据维护

- 启用中的 Binding 换目标：展示旧→新固定版本与 Agent/Profile 来源，明确将变更后续新消息接纳目标；传播异步，已有 Run 快照不变。
- 回退到旧部署：仍提交一次新 CAS 命令；不倒退 binding_revision/route_generation。
- 禁用中的 Binding 换目标：保存目标但维持禁用；没有新路由事件可以是预期结果。
- 在线 replace 凭据：允许，但会推进连接版本，可能触发重连；无需重新发布 Deployment。
- clear 凭据：先停用账户；不能将空白输入或星号占位串当 clear/replace。
- 双 Telegram 凭据轮换：当前是逐 purpose 命令，不是“保存全部”的原子批量。每次成功 GET 最新 account revision，再改下一项。需要可控停机窗口时提供先停用、逐项更新、再明确启用的操作说明。[S8]

## 5. 状态设计：四个维度，不合成一个“在线”灯

| 维度 | 真实来源 | 展示文案 |
| --- | --- | --- |
| 控制面意图 | account.enabled、binding.enabled、固定 target | 接入配置已启用 / 消息路由已开启或暂停 |
| 路由事件分发 | distribution | NOT_EMITTED=尚未产生路由事件；PENDING=等待发布；IN_FLIGHT=发布中；PUBLISHED=事件已发布到消息流；FAILED=发布失败 |
| Gateway 路由应用 | gateway_application | 当前代码固定 UNKNOWN，显示“尚无路由应用确认”，不从其他状态推断 |
| 连接观测 | observations[].effective_state/state/reason_code | 按实例显示已应用配置、连接中、报告就绪、停用、错误、观测过期 |

`PUBLISHED` 表示 Outbox 对应事件获得消息流发布确认，不表示 Gateway 已应用；连接 READY 不表示该 route generation 已应用，也不表示 Worker 可完成执行。[S6]

观测读取规则：

- 服务端按 received_at 超过90秒或 connection_revision不匹配标记 STALE；采用 effective_state，不使用客户端 observed_at 延长有效期。
- 空 observations 显示“尚未收到连接观测”；不显示失败、不显示成功。
- 显示逐实例摘要；不通过最大 owner_epoch 自行选主，不把某个等待实例归纳为账户整体故障。
- 已实现详情可见时约15秒重新 GET，失败退避至最多60秒，隐藏页面不发轮询、组件卸载停止、聚焦触发刷新。编辑表单不被轮询覆盖；轮询刷新只读快照/CAS基线提示。
- 已显示“最近读取时间”，读取错误保留错误提示；当前报告类型/原因码之外的值采用未知兜底。额外的本地快照过期视觉标记仍属可改进项，不与服务端 effective_state 混用。
- 仅展示服务端提供的有界实例集合（最多32个、近期24小时保留），不称完整实例历史。[S6][S10]

## 6. 写请求、CAS、幂等与恢复

所有写操作串行锁定当前对象，按钮 loading；不采用 optimistic toggle 宣告已启用。成功时先确认命令结果，再 GET AccountDetails 读取当前事实。若 GET 失败，显示“操作已接受，最新状态读取失败”，只重试 GET，不重发写命令。

| 字段 | 用途 | 前端规则 |
| --- | --- | --- |
| account_revision | 基本信息、接入启停、凭据命令 CAS | 每次读当前账户；保留用户输入并显式处理409 |
| connection_revision | 连接配置/凭据/启停的运行版本 | 只读诊断，不代替 account CAS |
| credential_version | 单 purpose 凭据 CAS | 凭据更新同时发送账户版本和该凭据版本 |
| binding_revision | Binding目标/启停 CAS | 不用 account revision 或 route_generation 代替 |
| route_generation | 有效路由传播序列 | 只读；旧目标回退也不递减 |

写后重读有两项特殊理由：

1. 幂等重放返回原脱敏 CommandResult，不一定是当前读模型；create 重放仍可201。
2. CommandResult 的 event_id 与详情 route_event_id 名称不同；本次无新事件时 result.distribution 可以为 NOT_EMITTED，而原路由事件仍已发布。不要直接覆盖完整详情。[S4][S6]

**无凭据命令**：按 user/tenant/object 保存非秘密 pending intent、原 key、原 body，刷新后允许原请求确认。过期 CAS 或不同内容不得悄悄换 key 重试；重新读取并明确确认新操作。

**含凭据的 Create/Replace**：秘密与完整原 body 仅保留当前页面内存；不进入 URL、sessionStorage/localStorage、诊断复制、埋点、缓存或恢复文件。当前页面超时可用原 key+原 body 重试；结果确定后清除内存。

刷新后不自动重构或重发含秘密请求：保留的只有非秘密待确认标记。已有 account ID 时 GET 当前状态；创建 ID 未知时引导在本租户账户列表核对固定 Provider/物理ID。状态变化本身不证明某个秘密值已被接受；需要精确重放时重新输入原值并使用保留的原非秘密字段和 key，经用户确认后发送。结果仍不确定时保留待确认状态，不将新 key 的再次创建/轮换当恢复步骤。

已实现的恢复边界还包括：

- 每个账户的待确认命令按 user/tenant/account 隔离；退出登录/身份切换清理渠道恢复状态，权限失效清理表单、确认和秘密内存。
- 含秘密原请求重放遇到 `CHANNEL_IDEMPOTENCY_CONFLICT` 时，不自动结束原 key；清空输入后允许重新输入真正的原值，仍按原 key/原版本确认。新意图必须与原请求核对明确区分。
- 恢复记录读取失败时暂停新写，并提供显式重读；marker 写入失败时不发送写请求，避免产生没有恢复标记的未知结果。
- 创建成功已取得 account_id 后，即使后续 GET 失败也打开该账户工作台；不再调用 CreateAccount。

当前缺少公开幂等结果查询 API，是刷新恢复体验的明确缺口；建议后端补充经当前租户/角色授权的脱敏操作回执查询，再承诺跨刷新无损恢复。此项不通过持久化秘密请求体弥补。[S4]

### 错误后的操作

| 错误 | 页面动作 |
| --- | --- |
| 非JSON列表404 | 显示当前环境尚未提供渠道管理入口；与空 accounts 数组区分 |
| 401 / CHANNEL_PERMISSION_DENIED | 重新登录 / 切换为只读权限；停止后续写操作 |
| CHANNEL_ACCOUNT_IDENTITY_CONFLICT | 提示此Provider/身份已被登记；仅在本租户查找已有账户，不承诺删除释放 |
| CHANNEL_ACCOUNT_ALREADY_BOUND | GET账户详情并打开其已有目标面板，不循环CreateBinding |
| CHANNEL_REVISION_CONFLICT / CHANNEL_BINDING_REVISION_CONFLICT / CHANNEL_CREDENTIAL_VERSION_CONFLICT | 保留非秘密输入，对比最新版本，由用户确认后开启新命令 |
| CHANNEL_ACCOUNT_DISABLED / CHANNEL_CREDENTIAL_REQUIRED / CHANNEL_ACCOUNT_MUST_BE_DISABLED | 定位接入开关/缺失purpose，并解释先后顺序 |
| CHANNEL_TARGET_NOT_FOUND | 保留账户，重新选择本租户的确定发布版本 |
| CHANNEL_TARGET_INTEGRITY / CHANNEL_SOURCE_INTEGRITY | 保留原目标，不自动换latest；显示诊断并联系平台处理 |
| CHANNEL_DEPENDENCY_UNAVAILABLE / 超时 / 非JSON5xx | 显示结果待确认；同请求重试或重新读取，不重复创建 |

前端按 error.code + field 映射中文提示。当前后端 message 为通用文本，原样 toast 不足以指导操作；不要把 error.field 内容当秘密回显。[S1]

## 7. 视觉布局与已实现组件

```text
渠道接入 / 研究机器人                 Telegram · 数字ID
接入配置：已启用          [停用本平台接入]
消息路由：已开启 → 研究部署 r3        [暂停消息路由]

[概览] [运行目标] [凭据与设置] [连接诊断]

运行目标卡片（完整宽度）
部署名称选择                 固定发布 rN 选择
Agent v2 / Profile r4         [查看部署快照]
旧目标 → 待保存目标           [保存目标]

路由分发                     连接观测
事件已发布 / generation 5     实例A：报告就绪 / 最新received_at
路由应用：尚无确认            实例B：连接中 / 原因码
```

复用现有 AppShell / PageHeader / Button / StatusBadge，渠道使用共享 ChannelDialog 与独立样式提示区。来源选择卡片保持完整行，输入统一最小44px高度，标签与帮助文本对齐；宽屏两列、窄屏单列。危险动作放在独立确认区，不使用一排小图标隐藏“停用接入”和“暂停路由”的区别。技术ID、版本、水位与Digest折叠展示，保持可复制与可聚焦。[S7]

## 8. 检查中发现的问题与建议

| 优先级 | 当前事实 / 问题 | 设计或契约建议 |
| --- | --- | --- |
| P0 联调前提（前次快照） | 前次审计时13001的Channel列表原始404，Deployment列表同条件401；本轮重部署后的状态以运行验收为准 | 对齐新版Control Channel配置、mTLS/NATS等启动依赖及迁移后再做真实联调；前端区分未提供模块与空数据 |
| P0 交互（已落实前端） | 两种 enabled 容易被合成“上线”；账户重新启用恢复服务端当时保存的目标 | 已实现双开关、影响确认、逐步结果与路由应用未知展示；运行传播另行验收 |
| P0 凭据恢复（已落实前端） | Deployment现有pending body恢复方式不适合含秘密命令 | 已分离内存秘密与非秘密 marker，同 key 重放、原值重输、存储失败阻断新写；不复制秘密 body 到 sessionStorage |
| 已修正文档 | 旧文档曾称 PATCH 可编辑 config；真实关闭 Schema/DTO 只有 name/description | 本轮已纠正控制面文档；首版只读 config，按 3.1 与 READONLY-CONFIG 验收 |
| P1 读模型 | 列表无Binding/分发/连接摘要、无筛选或total | 首版简单列表；需要丰富渠道总览时补有界聚合摘要/筛选，避免N+1 |
| P1 可观测性 | gateway_application固定UNKNOWN，没有租户级消息/运行/回执查询 | 新增绑定event/generation/digest且有时效的应用确认，再设计端到端检查面板 |
| P1 恢复 | 缺少脱敏操作回执查询；多purpose更新非原子 | 后端评估operation查询及明确的批量轮换协议，不在前端假装原子 |
| P1 重启并发 | Account enabled只校验账户版本，Binding改目标不推进该版本 | 若要求精确恢复用户确认的目标，补Binding版本/目标条件；首版显示最近读取版本与时间并说明并发窗口 |
| P1 启用能力 | Channel按平台配置条件注册，公网Webhook origin不在公开读模型 | 增补明确的模块能力/可用Provider/受控公开回调信息；不要从浏览器域名或运维探针猜测 |
| P1 生命周期 | 无删除/转移/身份修改；错误登记难自助修复 | 创建前强调固定身份，后端单独设计修正/移交生命周期，不用停用冒充删除 |
| P1 Provider文案 | Telegram停用只撤下本平台handler，未实现deleteWebhook | 使用“停用本平台接入”，不写“注销远端Webhook”；首次启用说明注册副作用 |
| 已修正文档与入口 | Web README 原称 Runtime Profile/Deployment 未实现；Deployment详情原称运行分发尚未接入当前产品 | 2026-09-06 已按真实路由更新 README，并实现固定版本“接入渠道”入口；保留“发布不自动启用、Worker状态另行验证” |
| P2 契约细节 | 凭据更新Schema未完整表达webhook_secret的256位ASCII规则；Binding旧文档字段与公开DTO不同 | 前端按实际Domain做预检查，以公开DTO的manifest_ref及真实字段为准；同步补齐Schema和文档 |

这些后端增强均为建议，不作为已经存在的 API 使用。P0主闭环可在现有11个管理API上完成；路由ACK与完整对话验收面板必须等待相应读契约。

## 9. 实现拆分、当前进度与交付边界

### 阶段 A：契约已实现；运行环境独立验收
- 已实现11个API的类型、错误映射、分页、CAS、幂等和含秘密请求恢复策略。
- PATCH config 文档漂移已纠正，源码遵循 3.1 的只读契约；测试租户下结构化列表、模块启用与运行版本仍按独立联调证据确认。
- 已复用同源 BFF 的 Cookie/Idempotency-Key/no-store，并增加渠道写命令转发测试；浏览器不接入内部 mTLS 面。

### 阶段 B：账户与固定目标页面已实现
- 渠道导航、游标列表、新建两Provider表单、账户详情与OWNER/MEMBER呈现。
- 单purpose凭据维护、账户启停、唯一Binding创建/目标切换/启停。
- Deployment固定版本快捷入口与返回路径；所有写后GET、冲突保留、无重复对象创建。

### 阶段 C：状态与可恢复交互已实现
- 接入意图/路由意图/分发/逐实例观测分层，轮询可见性、过期状态、未知状态兜底。
- 接入重启恢复当前已保存目标的确认及并发边界、Telegram注册副作用提示、在线凭据替换的重连提示。
- 明确跨刷新含秘密命令的待确认状态；禁止隐式二次轮换。

### 阶段 D：独立记录的真实验收与交付要求
- 类型、组件、协议fixture测试；真实Control+PostgreSQL+NATS+Gateway联调。
- 实际浏览器完成“账户→固定目标→接入启用→路由启用→切目标→暂停→恢复”。
- Telegram入站验收到RunRequested与完整Worker/回复验收分别记录；WeCom做对应真实账户专项验收，结果不互相代替。
- 检查390/1134/1440布局、键盘可达性、焦点恢复、慢请求、错误重试、空数据与未启用模块。

实际已存在的实现文件（2026-09-06 源码检查）：

- [`web/lib/channel-api.ts`](../../../web/lib/channel-api.ts)：公开11个方法、独立Channel DTO/error。
- [`web/lib/channel-editor-state.ts`](../../../web/lib/channel-editor-state.ts)：非秘密编辑/命令状态；秘密只留组件或内存请求容器。
- [`web/components/channels/account-list.tsx`](../../../web/components/channels/account-list.tsx)：游标列表、角色展示、固定目标选账户。
- [`web/components/channels/account-create.tsx`](../../../web/components/channels/account-create.tsx)：两 Provider 创建表单、创建确认与含秘密未知结果恢复。
- [`web/components/channels/account-workspace.tsx`](../../../web/components/channels/account-workspace.tsx)：双开关、目标、逐 purpose 凭据、元数据和逐实例观测；凭据与观测当前嵌入此文件，未虚构独立 credential-editor/connection-observations 文件。
- [`web/components/channels/target-selector.tsx`](../../../web/components/channels/target-selector.tsx)：已发布 Deployment rN 选择及真实来源读取。
- [`web/components/channels/use-channel-access.ts`](../../../web/components/channels/use-channel-access.ts) 与 `confirmation-dialog.tsx`：会话/角色访问状态与键盘可达确认框；布局位于同目录 `channel.module.css`、`create.module.css`。
- [`web/app/tenants/[tenantId]/channels/`](../../../web/app/tenants/[tenantId]/channels)：三类页面入口；沿现有Next版本规范读取异步params/searchParams。
- [`web/components/app-shell.tsx`](../../../web/components/app-shell.tsx)：渠道导航、标题、身份切换时清除本渠道编辑状态。
- [`web/components/deployments/revision.tsx`](../../../web/components/deployments/revision.tsx)：固定版本“接入渠道”入口和准确产品边界说明。

## 10. 验收矩阵（自动化与真实运行分开记录）

以下保留完整验收要求，不表示每行均完成真实 Provider 验收；自动化已覆盖的主要边界见 11.2，实际运行结果另附。

| 分类 | 必测场景 |
| --- | --- |
| 协议 | 11路由、7写幂等头、201重放、关闭DTO、cursor/page_size、无total、错误field映射 |
| 权限 | MEMBER脱敏只读；全部写403；OWNER写；过期会话；跨租户404；Operator无隐式权限 |
| 初次创建 | 必需凭据、两Provider字段、重复物理ID、取消不创建、创建后中断可继续、CreateBinding账户禁用时可成功 |
| 生命周期 | 双开关四组合与四类确认框；Account重新启用按服务端当时保存状态；T0-T3 并发目标变化；Binding暂停保留连接与目标但清除开启意图；绑定启用旧CAS冲突；不宣称瞬时传播或取消旧Run |
| 凭据 | 单purpose双CAS、在线replace推进连接版本、clear须先disabled、keep不带value、失败不回显秘密 |
| 目标 | 固定rN、无latest/Draft、同租户、同目标NOOP但旧CAS仍冲突、目标回退不倒退generation |
| 幂等 | 双击/断网/超时/迟到响应、相同key/body重放、不同body冲突、写成功GET失败只重试GET、刷新不自动重发含秘密命令 |
| 观测 | 无观测、90秒过期、旧connection revision、不同instance epoch、未知reason、多副本部分READY、旧轮询不覆盖新账户 |
| 事实边界 | PUBLISHED不等于Gateway已应用；READY不等于路由ACK或Worker完成；无数据接口不显示假指标 |
| 运行 | 真正的动态注册与接入、路由事件传播、目标切换后新消息精确Admission；旧Run快照不变；真实Provider验收单列 |

### 10.1 本轮固定的文案/只读验收条目

- **READONLY-CONFIG-1**：Account 基本信息表单只有 name/description；config 的只读文本可复制但无保存入口；Telegram 不拼接浏览器 origin，WeCom bot_id 不可编辑。
- **READONLY-CONFIG-2**：PATCH 请求只含 expected_account_revision 与至少一项 name/description；旧对象 merge、隐藏字段或表单默认值不得使 config/provider/provider_account_id 混入请求。
- **CONFIRM-INTENT-1**：ACCOUNT-DISABLE 分别覆盖无 Binding、enabled=false、enabled=true，显示当前目标与“保留路由意图”的准确条件。
- **CONFIRM-INTENT-2**：ACCOUNT-ENABLE 覆盖三种意图与 T0-T3 并发；不把快照目标当作后端 CAS 保证，成功后展示 GET 实际目标。
- **CONFIRM-INTENT-3**：BINDING-DISABLE 说明暂停意图、保留接入/凭据/目标、不取消已接纳 Run；BINDING-ENABLE 使用 Binding CAS，账户未启用时不暗中打开账户。
- **CONFIRM-INTENT-4**：读取失败不以旧缓存直接提交；读到变化要求重新确认；已确认原请求原 key/body 重试；写成功读失败仅重试 GET；无原子总开关或 deleteWebhook 承诺。

### 10.2 可重复执行的前端检查命令

在 [`web`](../../../web) 执行：

```bash
npm test -- --maxWorkers=2
npm run lint
npm run build
```

这些命令验证源码、协议/组件行为与生产构建，不替代已认证后端工作流或真实 Provider 收发。

## 11. 按阶段保留的检查快照与本轮实现记录

### 11.1 2026-09-06：页面实现前的计划与契约审计快照

定向检查命令（工作目录为上文工作树）：

```bash
GOCACHE=/tmp/trpc-agent-sync-main-gocache go test \
  ./api/openapi/control/v1 ./api/schemas/channel/v1 \
  ./services/control-api/internal/channelbinding/domain \
  ./services/control-api/internal/channelbinding/application -count=1
```

当次实际四个包均返回 ok，退出码 0；这是当次源码、Schema 和应用逻辑检查，不是本次真实 Provider/数据库/NATS 全链路验收。

以下为前次计划审计时的只读、无 Cookie 代理探测快照；本轮重部署后的状态另以运行验收记录为准：

| GET路径 | HTTP | 解释 |
| --- | --- | --- |
| `/api/control/healthz` | 204 | 代理上游健康响应 |
| `/api/control/v1/tenants/tnt_profileqa_9c3e721c7c/deployments` | 401 UNAUTHENTICATED | 已注册路由进入认证 |
| `/api/control/v1/tenants/tnt_profileqa_9c3e721c7c/channel-accounts` | 404，非结构化，18 bytes | 当时运行链未暴露该管理路径，不作空列表解释 |
| `/api/control/v1/tenants/tnt_profileqa_9c3e721c7c/channel-bindings` | 404，非结构化，18 bytes | 同上 |

结合Bootstrap仅在config.Channel非空时注册模块，前置联调应区分旧服务构建和未配置模块；此次探测本身不区分两者。[S11]

原始记录：
- [CONTRACT_TESTS.json](../../../artifacts/channelbinding-web-plan-20260906/CONTRACT_TESTS.json)
- [LIVE_ROUTE_PROBE.json](../../../artifacts/channelbinding-web-plan-20260906/LIVE_ROUTE_PROBE.json)

前次阶段仅新增前端计划与契约审计文档，之后修正了 channel-account.md 的 config 文案；以上历史探测不代表新构建的当前状态。

### 11.2 2026-09-06：三个页面及可恢复交互源码已实现

第 3、9 节所列页面、API client、非秘密状态模块、导航与 Deployment rN 快捷入口现已存在。
前端产品请求使用真实同源 BFF，不装载测试 fixture 作为产品数据，也不直接访问 Gateway 内部面。
本轮组件/协议测试覆盖列表分页、角色、两 Provider 创建、固定版本、双开关确认、CAS/幂等、
原值重输、存储读写失败阻断、权限失效清除秘密、写成功读失败仅重试 GET 等边界。
最终测试数量、production build、容器镜像、HTTP 和浏览器结果以同轮交付证据为准，本文不固化并发变化的测试数量。

运行重部署与健康验收由独立运行记录说明；真实 Telegram/WeCom Provider、Worker 执行与最终回复仍需分别记录，不以页面实现或自动化 fixture 代替。

## 12. 主要依据

- **S1**：[公开Handler](../../../services/control-api/internal/channelbinding/adapter/inbound/http/handler.go)，40–51行路由，258–290行分页，317–354行错误映射；[Channel公开OpenAPI](../../../api/openapi/control/v1/channel-public.yaml)。
- **S2**：[Binding领域与流程](../control-api/channelbinding.md)，37–74行关系及双开关；[Binding Domain](../../../services/control-api/internal/channelbinding/domain/binding.go)，40–95行创建与启停；[数据库约束](../../../services/control-api/migrations/0001_baseline.sql)，598–656行身份与一账户一绑定。
- **S3**：[联合验收](../control-api/channel-acceptance.md)，3–8、86–88行；[Gateway集成](../channel-gateway/control-integration-v1.md)，328–335行；[Final交付边界](../channel-gateway/delivery-final-v1.md)，13–20行。
- **S4**：[Application查询DTO](../../../services/control-api/internal/channelbinding/application/queries.go)，9–49行；[命令执行](../../../services/control-api/internal/channelbinding/application/commands.go)，52–136行幂等执行、344–389行账户启停、435–457行绑定目标更新；[命令回执DTO](../../../services/control-api/internal/channelbinding/application/ports.go)，124–136行。
- **S5**：[内部mTLS Handler](../../../services/control-api/internal/channelbinding/adapter/inbound/internalhttp/handler.go)，1–2、57–60、62–87行。
- **S6**：[真实查询投影](../../../services/control-api/internal/channelbinding/adapter/outbound/postgres/queries.go)，18–36、52–77、104–108、142–171行。
- **S7**：[已有导航](../../../web/components/app-shell.tsx)，96–123行；[部署版本详情](../../../web/components/deployments/revision.tsx)，8–23行；[Deployment API Client](../../../web/lib/deployment-api.ts)；[BFF](../../../web/app/api/control/[...path]/route.ts)，7–29行。
- **S8**：[账户创建Schema](../../../api/schemas/channel/v1/account-create.schema.json)；[凭据更新Schema](../../../api/schemas/channel/v1/credential-update.schema.json)；[Credential Domain](../../../services/control-api/internal/channelbinding/domain/credential.go)，111–146行；[账户Domain](../../../services/control-api/internal/channelbinding/domain/account.go)，80–101、182–213行。
- **S9**：[Telegram运行适配](../../../services/channel-gateway/internal/connection/application/telegramruntime/runtime.go)，87–99行停用；[账户注册语义](../control-api/channel-account.md)，373–383行。
- **S10**：[连接观测文档](../control-api/channel-account.md)，405–425行；[上报Schema](../../../api/schemas/channel/v1/observations.schema.json)，80–103行状态/原因码。
- **S11**：[条件装配](../../../services/control-api/internal/bootstrap/app.go)，151–175行；[Channel配置](../../../services/control-api/internal/bootstrap/channel_config.go)，20–21、80–82行。
- **S12**：[账户 PATCH 关闭 Schema](../../../api/schemas/channel/v1/account-update.schema.json)，4–37行；[账户启停 Schema](../../../api/schemas/channel/v1/account-enabled.schema.json)，4–19行；[Binding 启停 Schema](../../../api/schemas/channel/v1/binding-enabled.schema.json)，4–19行；[准确 DTO 与启停事务](../../../services/control-api/internal/channelbinding/application/commands.go)，249–253、344–390、462–508行。

- **S13**：[Channel API client](../../../web/lib/channel-api.ts)；[非秘密恢复状态](../../../web/lib/channel-editor-state.ts)；[账户工作台](../../../web/components/channels/account-workspace.tsx)；[账户创建](../../../web/components/channels/account-create.tsx)；[账户列表](../../../web/components/channels/account-list.tsx)。

## 13. Telegram 预检 V1：实现中 / 待联调（2026-09-06 增量）

### 13.1 当前状态与唯一协议来源

[跨端设计契约](../channel-preflight-v1.md)已冻结并下发实现；当前不声称新增接口已在联调容器部署，也不声称已完成真实Telegram预检。原11个公开管理操作和3个内部mTLS操作保持原语义，预检另增 **2个公开操作 + 3个私有操作**；整合目标为13个公开、6个私有操作，并非新增8种账户/Binding业务写命令。

- 新公开面：`POST /v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights`，OWNER发起；`GET .../preflights/{preflight_id}`，ACTIVE MEMBER读取。202是不可变创建回执，GET才是任务进展/结果。
- 新私有面：claim、任务绑定的诊断Token resolve、complete；只由授权Gateway通过mTLS访问。Web既不读取凭据值，也不调用Gateway管理端口。
- 完整JSON/空值/枚举/限额/错误码由Control唯一wire文档 `docs/architecture-next/control-api/telegram-preflight-v1.md` §3–9定义；Gateway实现说明位于 `docs/architecture-next/channel-gateway/telegram-preflight-v1.md`。两份后端文档正从各自工作树集成，当前本工作树标记为**跨工作树待合并**，不创建不可解析的本地源码链接。冻结版本以[中央冻结记录](../channel-preflight-v1.md#10-冻结记录)为准，前端不维护第二份候选wire。

### 13.2 不依赖启用或运行目标的检查流程

```text
已保存、仍停用的Telegram账户 → OWNER检查接入条件 → 排队/运行 → 逐项结果
                                                   ↓ 仅查看或修正后重新检查
                                    启用接入/开启路由仍是另外的明确操作
```

没有Binding、Deployment、RuntimeManifest、Worker或运行目录READY，也可进行预检。预检不临时启用账户、不安装接收handler、不改变Binding，不产生RunRequested；出站方法仅getMe/getWebhookInfo。它不调用setWebhook/deleteWebhook/getUpdates/sendMessage，不探测旧Webhook地址。

当前public origin检查仅静态判断配置是否适用于公开Telegram入口，**不代表公网可达或真实投递通过**。配置错误与Token/身份结果分别呈现；某项未执行显示SKIPPED/UNKNOWN，不制造成功结果。

### 13.3 固定8项与新鲜度

COMPLETED结果按冻结顺序显示8项；排队、运行、超时或STALE没有完整结果时，不能填充8个假成功行。

| 序号 / wire id | 用户看到的事实 |
|---|---|
| 1 `credential_configuration` | Bot Token / webhook secret 是否配置 |
| 2 `bot_identity` | getMe认证与保存的物理ID是否精确匹配；网络错误不冒充Token无效 |
| 3 `public_origin` | 检查时Gateway origin的静态适用性 |
| 4 `webhook_registration` | 已有Webhook与期望入口关系；旧URL仅内部比较，不回传/探测 |
| 5 `pending_updates` | 远端积压；0积压不是已收信证明 |
| 6 `delivery_errors` | 历史投递错误；没有历史错误不是当前投递成功证明 |
| 7 `recovery_materials` | 固定UNKNOWN：旧Secret不可回读，未证明恢复资料齐全 |
| 8 `delivery_verification` | 固定UNKNOWN：真实消息投递未验证 |

overall只聚合前6项；后两项永久可见，不因overall PASS消失。PASS只写“配置检查通过”。`state`、`outcome`、`freshness`分别展示，不把COMPLETED翻译为检查全通过。

`freshness=CURRENT`仅表示账户连接身份与结果TTL仍匹配；Gateway配置新鲜度在首次claim前为null，之后为`UNCONFIRMED`，展示“针对检查时Gateway配置”，不声称实时配置未变。名称/描述变化可显示`metadata_changed=true`但不无意义地废弃连接检查；轮换、清除、启停ABA和TTL分别使结果STALE/EXPIRED。120秒任务deadline、30秒lease、20秒执行预算与至多2次claim是不同边界，页面轮询必须有终态，不永久转圈。

### 13.4 已落实的相邻UX与开发中的预检恢复

已存在源码的相邻改动：

- 基本信息采用“编辑名称与描述→一次保存”，保存直接发送metadata PATCH；不会再增加一次风险动作确认。
- `expected_account_revision`固定为**开始编辑时的CAS版本**。后台刷新不替换该版本；409保留编辑值，重新准备后才允许提交，防止覆盖另一OWNER的更新。
- 当前目标以“部署名称 · rN”为主；固定ID保留在技术信息中。详情额外读取一次该目标的名称，不在账户列表制造N+1；名称读取失败不改变已保存目标。
- 顶栏明确显示“已登录”，不把登录事实扩写成平台、渠道、路由或Agent健康状态。

预检面板和路由恢复仍在开发/待联调：`sessionStorage`以用户+tenant+account隔离，只保存非秘密请求标识、期望版本、任务ID；不存Bot Token或内部claim凭据。退出/切换身份清理恢复标记。刷新后只恢复任务GET与明确的原请求重试，不自动启用或切流。

分享链接为当前账户页面的`?preflight=<id>`；有效MEMBER可读，链接本身不授予权限，也不发起新任务。单个参数和账户归属需校验；非法/重复参数有提示。前端依据已验证ID构造同源GET，不跟随任意`status_url`。

### 13.5 预检源码地图（正在开发，最终根任务复核后改完成）

- [独立预检API与状态保护](../../../web/lib/channel-preflight-api.ts)：不复用原Channel命令结果shape；闭合DTO、错误、分享路径和存储命名空间。
- [预检协议测试](../../../web/lib/channel-preflight-api.test.ts)与[预检fixtures](../../../web/test/channel-preflight-fixtures.ts)：只用于测试，不能装载为产品结果。
- [预检面板测试](../../../web/components/channels/preflight-panel.test.tsx)：任务状态/8项/权限/过期/恢复的开发中测试。
- `web/components/channels/preflight-panel.tsx`：面板实现正在开发；本节暂不把目标文件存在或测试文件存在当作集成完成。
- [账户工作台](../../../web/components/channels/account-workspace.tsx)、[账户路由](../../../web/app/tenants/[tenantId]/channels/[accountId]/page.tsx)与[应用壳](../../../web/components/app-shell.tsx)：预检入口、分享查询与身份边界由根任务最终联调复核。

验收必须分别记录源码/fixture、真实HTTP、真实Telegram只读调用、浏览器截图和运行镜像。预检前后比较账户/Binding开关、连接/凭据版本、RouteGeneration、注册/Receipt/Admission/Outbox；普通observations的时间和序号更新不是预检写副作用。getWebhookInfo前后URL相等也不能证明secret未变，需与专用只读Adapter方法白名单共同核验。历史真实消息接纳记录不替代本次预检验收。
