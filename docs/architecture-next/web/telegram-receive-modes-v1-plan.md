# Telegram 双接收模式：Web 独立修改计划

状态：**设计阶段记录已完成；2026-09-07 收到实施授权，Web 实现及验证进展见第15节。**
日期：2026-09-07。统筹任务：`设计 Channel Gateway`（01a06c74-3ecb-7213-99b9-61a4ec9b4114）。
Web 负责本文件中的交互、消费契约、恢复语义与验收计划；Control 拥有唯一共享 OpenAPI / JSON Schema，Gateway 拥有接收生命周期与跨端验收次序。

## 0. 核对基线与本轮变更

- Worktree：`/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac`。
- 分支：`codex/tenant-rbac-workspace`，沿用既有 Web worktree，没有新建或切换分支。
- `git fetch origin` 后，HEAD 与 origin/main 均为 **f61d49b09b8ae534f39f8065ddec699aa35ee10e**；开始设计时工作区干净。
- `git diff --stat HEAD..origin/main -- web api/openapi api/schemas/channel docs/architecture-next` 输出为空。
- `git grep -n -E 'receive_mode|long_polling' HEAD -- web services/control-api/internal/channelbinding api/schemas/channel` 没有命中；当前接口没有该模式字段。
- 本轮仅新增这份设计文档，不修改实现、Schema、运行配置或历史验收产物；不 commit / push / merge / deploy，不调用真实 Bot 接口，不运行产品测试。后文测试均是实施后的计划，不是本轮已通过结果。
- 已读取 `web/AGENTS.md`。实施时先按其要求读取本地 Next.js 对应版本指南；本轮没有编写 Next.js 代码。

需求附件已读取：`/Users/jfs/.codex/attachments/578d3491-8704-46bd-bb09-d180b3c79151/pasted-text.txt`。其中既有说明用于理解需求和候选架构；当前接口能力以本轮代码核对为准。

## 1. 目标、模块所有权与不变量

用户在 Telegram **ChannelAccount.config.receive_mode** 选择 `long_polling` / `webhook`。它回答“机器人怎样接收消息”，不是 ChannelBinding 的运行目标/路由字段；Agent、Runtime Profile、Deployment 编辑器不增加此字段。

产品目标：新账户默认长轮询；已有账户由后端迁移为显式 webhook，保持既有行为。Web 新建表单显式展示并提交用户选择，不把后端缺字段解释为新默认。

- Control 管理期望模式、保存/启用权限与版本、凭据要求、公共观测和预检事实。
- Gateway Connection 管理唯一活跃接收者、模式切换、owner/lease/在途请求、游标与归一化接收；共用 Admission。
- Web 消费稳定 Interface：读取配置/观测、提交被确认的管理意图、呈现检查结果。不把 Poller、offset、fencing、SDK 调用放进浏览器或 BFF。
- BFF 继续同源访问 Control、使用 Session Cookie；不增加浏览器直连 Gateway 或 Telegram 的通道。
- 修改 Binding 不改变 receive_mode，不为模式切换自动改目标、删凭据、开关路由。
- “配置已保存”“Gateway 正在切换”“连接报告就绪”“收到消息”“执行/回复成功”分别显示，不相互推断。
- 预检始终只读；保存模式也建议只改 Control 的期望配置。真正远端切换放在用户之后的**显式启用**流程，等待共同决议 D2。

协议依据：Telegram 的两种接收方式互斥；存在 Webhook 时 getUpdates 不工作；删除 Webhook 可选择丢弃积压；getWebhookInfo 的空 URL 不证明 Poller 正在运行。参见 [Getting updates / getUpdates](https://core.telegram.org/bots/api#getupdates)、[deleteWebhook](https://core.telegram.org/bots/api#deletewebhook)、[getWebhookInfo](https://core.telegram.org/bots/api#getwebhookinfo)。本计划不将“保留积压”表述为无限期保证：Telegram 自身保留期最长 24 小时。[官方说明](https://core.telegram.org/bots/api#getting-updates)

## 2. 当前源码事实与需要修改的点

| 当前事实 | 对双模式的影响 | 源码证据（本轮已读） |
|---|---|---|
| Account.config 只有 webhook_path / bot_id，Telegram response guard 强制 webhook_path 字符串 | 创建、列表、详情和写入成功响应均需按唯一新契约调整；未知模式不能回落为 LP | [channel-api.ts:9–14,75–80](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/lib/channel-api.ts) |
| CreateAccountInput 没有 config，createAccount 手工白名单组包；Patch 只发送 name/description | 单加 UI radio 会被客户端丢字段，旧后端 Schema 也会拒绝它 | [channel-api.ts:42–48,162–177](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/lib/channel-api.ts)、[account-create.schema.json](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/api/schemas/channel/v1/account-create.schema.json)、[account-update.schema.json](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/api/schemas/channel/v1/account-update.schema.json) |
| 新建 form 总是组装两项 Telegram replace；CreateMarker 恢复时只拷贝身份/名称/描述 | 模式和可选凭据的“原请求形状”必须进入非秘密恢复设计，不能刷新后变成另一模式 | [account-create.tsx:13–16,71–89,109–113](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/components/channels/account-create.tsx) |
| credentialPurposes 同时用于允许列表与必需校验，validateChannelAccount 要求两项；Binding 开启也用该函数 | LP 缺 webhook_secret 会被 Web 提前错误阻止；允许保存和当前模式必需必须拆开 | [channel-api.ts:233–265](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/lib/channel-api.ts)、[account-workspace.tsx:97](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/components/channels/account-workspace.tsx) |
| Pending storage 为闭合白名单，create / updateAccount 不允许 config | 须设计新操作恢复与旧 marker 兼容；新增字段不能被 load 时删除或重放时遗漏 | [channel-editor-state.ts:52–71,86–105](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/lib/channel-editor-state.ts) |
| 启用确认固定说 setWebhook；停用明确不承诺删除远端Webhook；config 当前只读 | 必须按模式改影响说明，单独提供结构化模式编辑，其他 config 继续只读 | [account-workspace.tsx:195–203](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/components/channels/account-workspace.tsx) |
| 列表按配置过的凭据数/所有返回凭据数显示，如1/2 | LP 的1个必需项已齐全时不能误导为“缺一项” | [account-list.tsx:65–67](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/components/channels/account-list.tsx) |
| 预检只有5种 check status，固定8项及每项闭合details；首6项独立校验 aggregate；结果无模式字段 | 新 N/A / 模式快照会被旧 Web 拒绝；不能仅换文案，更不能忽略新字段 | [channel-preflight-api.ts:1–87](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/lib/channel-preflight-api.ts)、[preflight.go:177–240](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/api/schemas/channel/v1/preflight.go) |
| 预检面板是独立恢复/轮询 Module，有 deadline、页面隐藏暂停、401/403停读、TTL、固定链接 | 保留已验证的调度与恢复规则；仅扩展冻结契约的模式事实和展示 | [preflight-panel.tsx:25–119](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/components/channels/preflight-panel.tsx) |
| BFF 是通用方法转发，不是 Channel 专用路由白名单；透传 Cookie/Idempotency-Key、Retry-After及202 Location | 核对新增契约的状态/头即可；不复制后台业务规则或另造URL | [route.ts:7–40](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/app/api/control/[...path]/route.ts) |

## 3. 信息架构：扩展既有 Channel 页面，不新增一级菜单

### 3.1 列表

- Telegram 行显示“长轮询”或“Webhook”模式标签；企业微信不显示 Telegram 模式。
- 标签来自服务端明确字段。缺失/未知模式显示“接收方式未确认”，提供重新读取/检查后端版本提示，不自动填 LP。
- 凭据列改为“必需凭据 1/1 已配置”或“2/2 已配置”；展开/详情展示可选 Webhook Secret 已保存/未保存。
- enabled 标签仍说明“接入配置已启用/停用”，不把它改名为“运行中”。

### 3.2 新建页面

建议顺序：**机器人身份 → 接收方式 → 按模式显示凭据 → 确认并保存（保持停用）**。

```text
接收方式
(●) 长轮询（新账户默认）
    Gateway 主动接收 Telegram 更新，无需公网入站回调；仍需可用的出站网络。
( ) Webhook
    Telegram 向 Gateway 推送，需要平台配置公开 HTTPS 入口。

Bot Token                         [必需]
Webhook Secret                    [Webhook 必需 / 长轮询可选]
[保存渠道账户]                    保存不会启动接收或切换远端模式
```

- 新建表单首次选择 LP 是产品选择，不是从缺失的服务端字段推断。POST 显式包含最终冻结的 config.receive_mode。
- LP 只要求 Bot Token；Webhook Secret 放在“可选高级设置”，默认不生成、不发送空 replace、不发送 clear。
- 如用户主动填写可选 Secret，仍按原规则校验并列在提交确认里；切换 radio 不默默把已输入的有效可选 Secret 清掉或发布。
- LP → Webhook 在同一未提交表单内切换时，缺 Secret 显示关联字段错误；Webhook → LP 时 Secret 转为可选，保留输入于当前表单内存。用户明确清空后才省略未保存的可选字段。
- Provider 切换沿用清空旧 Provider 秘密输入的逻辑；从企业微信切回 Telegram 是新的表单选择，不污染已经锁定的 pending 请求。
- 成功页/跳转首先提供“检查接入条件”，然后独立配置固定目标、启用和路由。来自 Deployment 的固定目标 query 继续保留但不自动创建 Binding。
- 创建结果不确定后，模式/身份/原幂等 key 锁定；秘密不写 sessionStorage。新 marker 需保存 mode 及**凭据用途存在性清单**等非秘密请求形状，确保刷新后要求重新输入当时所有原凭据，而不是按当前 mode 猜原请求。
- 旧 marker 无 mode 时不能套新 LP 默认重放。先核实旧命令；只有 Control 明确承诺旧 body 的幂等原义兼容时才允许按旧 body 精确重放，否则保留待核实提示，不以新 key 隐式重建。

### 3.3 详情与编辑

- 概览“账户接入”卡片显示**已保存接收方式**；单列“Gateway 当前报告”的模式/连接版本/时间（若后端尚不提供则标未确认）。
- “凭据与设置”增加结构化“接收方式”区及“更改接收方式”按钮；不开放任意 config JSON 编辑，不允许编辑 webhook_path、bot_id 等生成字段。
- Webhook 模式显示只读回调路径及平台入口检查入口；LP 显示“公网回调不适用”。若后端仍保留 webhook_path，放技术信息区标“预留字段，当前模式不使用”，不要删除后端字段。
- 名称/描述保存与模式切换分开确认。模式修改不借 metadata 编辑按钮顺便提交；无变化时不发请求。
- OWNER 且账户停用才准备模式编辑（建议 D1）。在线账户显示解释及“先停用接入”独立动作，不自动连续调用停用→切换→启用。
- MEMBER 查看模式、凭据元数据与已知预检结果；不显示写操作。

## 4. 显式切换：分别确认期望配置与远端生效

建议首版采用“停用后保存模式、再次显式启用生效”，不是无感热切换。Gateway 统筹方已表达同方向候选；最终条件以共同决议 D1/D2 为准。

1. 读取最新 Account/Binding/凭据/运行观测，展示当前模式与期望目标模式。
2. 如果账户启用，先展示独立停用确认：本平台接收和新接纳会停止；已有执行/回复资格仍按现行契约；账户、Token、Secret、cursor、固定目标保留；路由开启意图是否保留按服务端规则明确显示。
3. 账户配置已停用后，允许准备模式保存。**enabled=false 不证明旧 Poller/在途请求已停止**；Web 也不凭最大 owner_epoch 或一条旧 DISABLED 观测判定全局切换完成。纯配置保存不等待 Web 自行证明全局静默；旧路径静默是 Gateway 启动新接收者前的强门槛，不是前端额外增加的保存门槛。
4. 保存确认显示“Webhook → 长轮询”或相反、固定版本、保留的凭据与路由意图。推荐此步仅保存期望配置，不请求 Telegram。
5. 成功后仍保持停用；新模式及新 account/connection revision 来自服务端 GET。旧模式预检保留，但不再作为当前模式的诊断。
6. 用户可执行只读预检，然后独立点击启用。启用确认再次以最新保存模式说明远端影响；Gateway 必须完成旧接收 owner/lease/在途栅栏处理及远端状态核对，才启动新模式。
7. 展示“启用意图已保存，等待 Gateway 应用”；只从服务端观测呈现实际切换/运行状态。失败不自动回切旧模式、不重新启用另一种接收者。

### 确认文案要点

**保存为长轮询**：本次保存只改变期望接收方式，当前仍停用；已有 Webhook Secret 保留；不清空积压。下次启用可能需要移除 Telegram 现有 Webhook 才能开始长轮询。

**启用长轮询**：Gateway 将主动请求更新；如需移除已有 Webhook，必须按冻结的接管/确认契约执行；停止其他系统对同一 Bot 的长轮询。`getWebhookInfo.url` 为空不能确认不存在外部 Poller，预检不调用 getUpdates 来“测试互斥”。

**启用 Webhook**：Gateway 将停止本平台旧 Poller并等待在途处理，注册平台入口；可能改变现有 Webhook 指向。预检成功不替代这次副作用确认。

**积压**：产品不提供默认清空选项，切换不自动设置 drop_pending_updates=true，不把负 offset 当成“从现在开始”。“保留积压”仅表示平台不主动丢弃，不承诺跨停机超过 Telegram 保留期仍可恢复。

**路由**：账户停用当前会保留 Binding 开启意图；恢复接入时可能恢复消息接纳。确认框显示该事实和最新目标，提供单独“先暂停消息路由”入口，但不替用户自动改 Binding。其他 OWNER 可独立改目标，Account CAS 不是 Binding 目标锁。

### 切换所需 Interface，等待 Control/Gateway 定稿

- 保存模式究竟复用 Account PATCH 还是专用命令、响应是同步管理回执还是异步操作资源：Web 不命名新 endpoint。
- 版本至少覆盖账户与连接语义；请求究竟只有 account_revision、还是双 CAS，以及 mode 原值校验，由 Control 冻结。
- 如果“保存模式”也会触发远端动作，必须改变上述文案和恢复模型：提供操作ID/终态/失败恢复，不能返回普通 metadata saved 后让用户误以为仅配置改变。
- 外部 Webhook 接管意图怎样绑定一次明确确认、确认后远端又改变时怎样拒绝/再确认；不由 Web 根据过期预检自动推定接管授权。
- 切换回原模式是新管理意图、新幂等 key 和递增版本，不倒退 connection_revision 或游标。

## 5. 凭据：允许保存、模式必需、实际使用分别处理

| 凭据 | LP | Webhook | UI 行为 |
|---|---|---|---|
| Bot Token | 必需 | 必需 | 保存后仅显示配置状态/版本；不回显 |
| Webhook Secret | 可保存、非必需 | 必需 | LP 显示“已保留，当前模式不需要”；切模式不clear、不replace、不旋转版本 |
| 企业微信 Bot Secret | 与 Telegram 模式无关 | 与 Telegram 模式无关 | 企业微信行为不变 |

- `allowed credential purposes` 不等于 `required for receive_mode`；列表、创建校验、启用提示、Binding 开启前置检查应消费同一份冻结规则/共享fixtures，不能各写一套独立判定。
- Secret 缺失时切回 Webhook：推荐停用态允许先保存配置并标待补凭据，启用时阻止；若 Control 决定保存也要求齐全，则用“先补 Secret，再保存模式”两步明确流程，绝不承诺原子批量（D3）。
- 新建模式默认不意味着允许无 Token 的空草稿；空凭据草稿是独立议题，本轮不扩大其契约。
- 从来没保存的 optional Secret，其 status 元数据是缺项、version=0 还是正版本占位，由 Control 明确。现有 Web 只接受正版本且更新代码会在缺项时形成0；新协议必须给出“首次保存可选凭据”的合法 CAS，Web不编造0或1。
- 如果在 LP 下清除可选 Secret，依然单独确认并遵守现有禁用/版本规则；模式切换不隐式放宽清除条件。

## 6. 运行状态：保存意图与观察事实不混淆

使用三个并列视角：**账户接收配置 / Gateway 接收观测 / 新消息路由**，不合成单一“在线”灯。

| 事实或候选显示态 | 展示说明 |
|---|---|
| enabled=false | “接入配置已停用”；实际旧接收停止情况另看当前版本观测 |
| enabled=true、暂无新观测 | “启用意图已保存，等待 Gateway”；不显示正在接收 |
| LP 当前版本报告连接/轮询成功 | “长轮询已就绪”及最近成功时间；无消息也可能成功；不等于消息投递或回复验证 |
| 等待 owner / 排空旧请求 / 处理远端模式 | 若后端提供原因与阶段，按稳定code显示；未返回则“等待应用/未确认”，不创造伪进度条 |
| provider409 / 网络超时 / Token拒绝 | 分别映射竞争接收者、网络未知、凭据失败；不一律引导换Token，不自动切换模式 |
| ERROR / STALE / 未识别状态 | 明确错误/过期/未知；保留最后一次报告时间与连接版本，历史 READY 不冒充当前就绪 |

模式、owner信息与最近轮询成功事实的公开字段需单一共享契约；Web不通过Gateway内部API补数据。兼容未知字符串时仍显示未知，不因含READY字样判成功。

现有 `gateway_application=UNKNOWN` 保持原义；路由PUBLISHED与连接READY不等于Admission/Worker已完成。修改 Binding 不应重启 Poller，但这项由Gateway自动化与现场观测验证，不由Web界面自证。

## 7. 模式感知的固定八项预检

保留八项ID/顺序，扩展模式快照和“不适用”的语义；以下是**产品显示建议，不是第二份 wire Schema / code 枚举**。

| 检查ID / 当前标题 | Webhook 显示 | LP 显示建议 |
|---|---|---|
| credential_configuration / 凭据配置 | Token + Secret必需 | Token必需；Secret是否保存仍可见，但不算缺失失败 |
| bot_identity / 机器人身份 | 只读身份匹配 | 同左；不会启动getUpdates |
| public_origin / 公开入口 | 静态检查，成功也不代表公网可达 | **不适用**，不是PASS、UNKNOWN或前置失败SKIPPED |
| webhook_registration / 现有Webhook | 比较当前与期望Webhook | 表明是否存在阻止LP的Webhook；空URL只说明无Webhook，不证明Poller已运行 |
| pending_updates / 积压消息 | 从getWebhookInfo展示数量 | 若仍取同来源，注明“Telegram报告的待投递更新”，不声称Poller内部队列/cursor落后量 |
| delivery_errors / 历史投递错误 | 明确是Webhook历史错误 | 若显示事实，注明Webhook历史、非当前LP健康；若冻结为N/A也仍按历史协议呈现旧记录 |
| recovery_materials / 原Webhook恢复资料 | 原secret_token不可回读 | 不因LP就声称可完整恢复旧Webhook；是否N/A取决于共享规则，相关历史风险仍保留 |
| delivery_verification / Telegram实际投递 | 未实际验证时NOT_TESTED | 同左，预检不消费更新、不提高offset，不从空Webhook/identityPASS推导已投递 |

聚合要求：

1. 推荐共享契约用明确“不适用”状态/原因组合；最终枚举名称由 Control 冻结，本文不预设其字面值。
2. N/A 只在合法模式与具体检查语义下排除于聚合，不能成为任意失败的逃生值；Web严格校验backend outcome，不根据本地checkbox重算当前结果。
3. 现有首6项 FAIL > UNKNOWN/SKIPPED > WARN > PASS 的规则需由 Control 统一更新，提供完整mode×check×status×code×details fixtures；Web只同步消费/验证，不先行定义另一套真值表。
4. LP且已有Webhook应是“启用前需要处理”的可解释阻碍；FAIL还是WARN、启用时是否可经明确接管处理，列为D5。**不要把预检必须全PASS当成启用的自造前提**，否则用户在停用态永远无法消除需要启用才能处理的旧Webhook。
5. LP无可用公网origin不能继续拖低总体结果；出站Telegram失败仍是UNKNOWN/失败事实，N/A不能遮蔽它。

### 历史结果与契约版本

- 结果按**检查时的接收模式**解释，标题区同时标“检查时模式”“当前保存模式”，二者不同提示“该结果针对旧模式”。
- 当前Account切模式后，保留旧任务固定链接、原检查值、时间、expiry和snapshot，不在浏览器把旧 public_origin FAIL 重写为N/A。
- 新结果应携带被冻结的模式快照/协议判别依据；无该字段的旧结果，只有在Control明确旧版本就是Webhook契约时才能标“旧版Webhook预检”，否则显示“检查模式未提供”。
- 模式变更推进connection_revision；新请求按已保存版本发起。旧任务对版本变化变STALE/取消/返回历史结果的时序由Control固定；前端不沿用旧三CAS创建“新模式”任务。
- 仅Account元数据变化与连接模式变化区分处理，沿用metadata_changed与freshness的各自语义。
- 新旧严格DTO的读兼容必须在发布前测试。不能仅给现有闭合结构追加字段后假设旧Web会忽略，也不能在缺receive_mode时猜LP。

### 批级配置摘要与逐任务模式事实（统筹补充已纳入）

当前 `PreflightClaimRequest` 把 origin / origin_status / gateway_config_digest 放在一次领取批次，`PreflightConfigDigest` 的输入包含scope、source epoch、平台入口与策略；不是每个账户独立配置摘要。[preflight.go:58–72,134–170](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/api/schemas/channel/v1/preflight.go)

同一Gateway可领取混合模式任务，不能因为其中一个LP任务把批级origin改成N/A，从而改变Webhook任务的解释。统筹建议保留批级Gateway配置摘要、逐task另绑定模式/诊断策略/适用事实；Web接受这一分层方向，具体字段与摘要算法等待Control提案（D12）。Web不自己生成模式摘要，也不把既有gateway_config_digest改名后冒充新的逐任务指纹。

## 8. 命令、CAS、权限与不确定结果恢复

| 情况 | Web要求 |
|---|---|
| 模式改变 | 使用Control冻结的公开命令与CAS，确认快照包含模式、账户/连接版本、已保存凭据元数据及路由意图；同一目标无变化不提交 |
| 409版本冲突 | 保留用户选择与原基线，读取最新模式/版本后让用户重新确认；不自动升级expected版本覆盖 |
| 409幂等body冲突 | 保留原key与原内容，核实原结果；新选择是新意图，不偷换旧body |
| 409身份冲突 | 同Bot既有账户、外部竞争接收者分别使用后端稳定code展示，不额外查询其他租户 |
| 超时/断网/5xx/成功但结构错误 | 持久化非秘密原意图、原key及新模式；待确认时锁住其他相关写操作。契约版本已明确的请求才按原key原body恢复；升级前无mode旧pending先走下述兼容分流。GET结果可能相同不等于某次命令已接受 |
| 401 / 403 | 401引导登录、清秘密；403进入只读，不自动连续重试/换身份；恢复后核对原意图 |
| 404 | 沿用具体接口的可见性语义。预检不可见资源404；普通管理接口按Control现行规则，不能把所有403统一吞为404，也不能把缺模块404当空列表 |
| 202异步任务 | 按Location/status资源和Retry-After、终止deadline读结果；只有冻结命令真的返回202时才采用，不能给同步PATCH伪造任务 |
| 429 | 采用服务端Retry-After；禁用立即重复提交；不自动扩大限额 |

Pending storage 增量须有版本/兼容策略：新增mode操作的封闭字段列表、合法枚举、原始CAS；新建保留凭据用途存在性但不存值；旧marker不被直接删除后丢失原意图。刷新/换用户/跨租户/离页的清理与隔离沿用现有实践。

### 升级前旧 pending：回执存在与否必须分开

Control 初稿第9节已明确以下两类，本计划同步采用，精确兼容协议仍是 D9：

1. **已有旧 command receipt**：后端按原 request_contract、原 MAC 规范化与原响应形状重放；Web仅在明确历史契约来源下解释缺mode的旧响应，随后GET当前账户。不能向原body补mode后复用旧key。
2. **原请求可能从未落库、没有 receipt**：旧无mode marker原来代表Webhook意图，但新建默认已改LP。Web保留并锁定原marker/key，显示“升级前创建请求待核对”，不自动重发无mode body、不补LP、不换key重建。必须先冻结可区分原意图的旧契约标记/兼容重放入口，或由服务端可证明的核对流程解决。按Bot身份GET到相似配置不是该命令已执行的证明；查询未找到也不能单独证明原命令从未执行。
3. **新版本明确模式的 pending**：按其已保存mode、原凭据用途集合、原CAS/key恢复；秘密值仍只在内存重新输入。同body同key恢复规则只在该版本明确且后端支持的契约内适用。

无receipt旧pending的恢复协议是开放新默认/新写操作前的发布门槛，不能只迁移receipt表就宣称兼容完成。如选择新增请求契约header，必须同时冻结BFF白名单和幂等规范化测试，而不是Web独自发一个后端不验证的标记。

BFF继续转发 `/api/control/...`，保留Cookie、Idempotency-Key、no-store、Retry-After和相关Location；若新接口需要额外确认header则由Control先明确并增加精确白名单测试。BFF不重写receive_mode、不补默认、不放宽403/404、不对Telegram发出任何请求。

超时术语核对：当前10秒预算在Web客户端 `CHANNEL_TIMEOUT_MS`；BFF上游fetch没有显式AbortController/deadline。总计划“保持原有超时”应理解为保留各层已存在的预算，不把它写成“现有BFF有10秒截止”。若以后给BFF补deadline，另定需求与恢复测试；本功能不通过延长预算掩盖失败。

## 9. 实施拆分与文件落点（待批准后执行）

| 阶段 | 计划改动 | 依赖门禁 |
|---|---|---|
| W0 契约对齐 | 核对Control OpenAPI、Account create/update/view/snapshot、预检view/claim/complete及shared fixtures；形成现有/新/历史输入矩阵 | D1–D12共同决议完成 |
| W1 客户端与恢复 | channel-api.ts、channel-editor-state.ts、各自tests；只白名单发送receive_mode，拆分allowed/required；校验mode、首次optional凭据CAS；pending版本兼容 | Control wire可运行；不以mock代替主验收 |
| W2 页面 | account-create.tsx、account-list.tsx、account-workspace.tsx、confirmation-dialog.tsx及CSS/tests；模式选择、两阶段确认、凭据提示、运行事实 | W1与公开观测语义可用 |
| W3 预检 | channel-preflight-api.ts、preflight-panel.tsx、preflight fixtures/tests；mode快照/N/A/历史兼容；保留只读和预算 | Control/Gateway共同check矩阵冻结 |
| W4 集成回归 | routes/BFF tests及真实HTTP/浏览器验收；统一设计文档、API审计和使用指引 | 两后端同一可验收版本，统筹明确运行环境 |

尽量保留现有深 Module：Account命令恢复隐藏幂等/超时细节，Preflight面板隐藏任务读生命周期。只在有模式变化的表示/校验Seam做增量，不复制两个AccountWorkspace或两个预检轮询器，也不新建接收协议特有的Agent/Binding页面。

本轮不启动上述步骤。实施后按仓库格式提交，是否推送/合并/部署由统筹另行安排。

## 10. 验收矩阵（后续执行，当前未执行）

### 自动化

- 新Telegram首次默认LP且POST显式mode；WeCom不带该字段；修改radio后按新模式校验。
- LP只有Token可保存/启用/开启已配置路由；Webhook缺Secret准确阻止对应命令；可选Secret合法保存、不发空replace。
- 列表LP必需1/1不显示误导缺项；mode切换Secret值未回显、版本未变化；首次补optional Secret用合法后端CAS。
- 仅disabled可准备模式保存；enabled时独立停用流程；保存成功不自动enable、bind、改路由或执行provider副作用。
- 确认窗口打开后另一OWNER改mode/credentials/版本，旧提交409；Binding独立变化不被伪称锁定。
- 原key同body重放；变body409；断网/502/JSON异常后恢复不丢mode；刷新后所有原凭据用途需重新输入；旧无mode marker不被补默认或悄然删除；分别覆盖旧receipt存在、原请求未落库、结果未知三种情况，无明确兼容协议时保持锁定且零重发。
- 401、OWNER变MEMBER403、不可见404、路由未实现404、429/Retry-After均按原义处理。
- 新旧预检8项逐项合法/非法矩阵：LP origin N/A、出网未知仍降级、存在Webhook阻碍、无Webhook不自证Poller、旧Webhook历史FAIL原样保留、mode变更STALE/EXPIRED、无秘密字段。
- BFF不丢mode/CAS/幂等key/状态头，不直接访问Telegram；未知枚举不回落LP。
- 运行state/原因未知或过期仍可读，历史READY不作为新版本事实；普通Webhook账户与WeCom全回归。

### 真实后端与浏览器（另行安排隔离验收Bot/环境）

| 场景 | 实际验收点 |
|---|---|
| 旧账户升级 | 升级后显式webhook，旧凭据、目标、路由意图和接收行为不被静默转换 |
| 新LP、无公网入口 | 经真实账号密码保存仅Token账户；只读预检origin N/A；允许没有Binding先检查；明确启用后才启动接收 |
| LP → Webhook | 停用、保存mode、补Secret（如缺）、确认启用；观察旧Poller停止/新Webhook生效，再以真实Update验收共用Admission |
| Webhook → LP | 保留Secret，预检只读发现现有Webhook；明确切换/启用后才移除Webhook，积压不主动清空 |
| 路由意图保留 | Binding开启意图在账户停用/模式保存时保持；重启确认明确可能恢复原意图；暂停Binding不停止Poller的事实独立验证 |
| 外部竞争接收者 | 在隔离Bot上由统筹安排；界面呈现后端冲突，不无限自动重试或切模式，不从无Webhook假定独占 |
| 多副本、重启、失败 | 由Gateway验owner/游标/持久接纳；Web展示当前事实，不把多行观测都画成独立活跃Poller |
| 公共接口权限与并发 | OWNER/MEMBER/非成员真实Session、两页面409、旧/新marker恢复，不用直接DB写替代Web流程验收 |
| 积压与回复 | 逐条持久Receipt/Admission/Outbox后才推进cursor由Gateway验证；Web验收记录区分接收与Worker回复，未测的不写成功 |

Webhook/长轮询两模式的真实provider切换须使用统筹指定的隔离Bot及显式确认，不复用当前真实Bot做本轮设计验证。保留原配置/模式/凭据状态/已知Webhook恢复资料；旧secret_token不可回读，不承诺仅凭getWebhookInfo即可完整还原。

### 可访问性与响应式

- 接收方式用fieldset/legend及原生radio或等价可访问radio group；键盘方向键、标签点击、清晰焦点及错误关联。
- 模式名称、必需/可选、不适用、失败/未知均含文字，不只靠颜色；状态变化用适度aria-live，轮询不反复抢焦点。
- 复用ChannelDialog的焦点限制、Escape、返回焦点；忙碌/版本冲突时按钮状态与原因可读。
- 390px/768px/常规桌面实际截图+geometry；模式卡片窄屏纵排、长错误code/身份ID可折行、按钮44px目标、无横向溢出。
- 当前全局窄屏账户/退出菜单问题已另有UX记录，不将改全局AppShell偷偷捆绑为双模式功能；若验收切换账号需要该修复，由统筹另定范围。

## 11. 发布兼容与回退计划

1. 先冻结单一wire/fixtures和迁移规则；旧Account缺模式由后端规范化为显式webhook，新账户默认LP的规则由Control明确。
2. 现有Web对预检字段闭合。优先发布可读取**明确版本的旧/新契约**的Web但暂不开放新写操作，再升级后端，最后开放模式选择；能力判定来自明确契约/发布配置而非猜HTTP错误或推断缺字段。
3. 若产品选择同批部署，也必须记录短暂新旧组合的失败表现和可恢复提示；Gateway版本尚不支持LP时不得接受并启用LP配置。具体兼容矩阵由统筹冻结。
4. 配置迁移后回退旧代码可能不认识新枚举或LP账户；不能只换旧镜像就保证业务回退。先按Gateway/Control策略停用/转换受影响LP账户、确认在途与游标，再执行批准的应用回退。Secret/cursor与积压不自动清理，schema不降级覆盖。
5. 实施时保存原hash、差异、测试/真实验收记录及受保护副本回滚工具；真实provider恢复是独立操作，不能用源码回滚冒充恢复Webhook。

## 12. 待三方共同决议

下表是Web建议，不是已冻结要求。由Gateway统筹汇总、Control定义wire；定稿前不实施。

| ID | 需要共同决定 | Web建议 / 影响 |
|---|---|---|
| D1 | 模式修改是否仅disabled；旧执行终止确认在哪个阶段 | 首版仅disabled保存；Gateway在新接收启动前负责真实owner/在途栅栏。UI不把false或旧DISABLED当全局确认 |
| D2 | 保存模式是否纯Control配置；远端切换在哪个命令发生 | 建议保存不触Telegram，另次显式enable才执行；否则必须有异步操作/恢复Interface并重写文案 |
| D3 | Webhook凭据不齐是否允许保存模式；首次optional凭据版本语义 | 建议停用态可保存但启用阻止；若保存也阻止则采用先补Secret两步；首次Secret不能由Web编造版本 |
| D4 | 新模式写命令/路由、单或双CAS、版本推进/幂等响应 | Control唯一OpenAPI决定；Account/connection推进，Binding目标锁与版本独立；Web不定义第二endpoint |
| D5 | LP已有Webhook时预检severity、启用接管确认及竞态处理 | 显示阻碍但不自造“必须全PASS才可启用”；确认需约束真实副作用，远端变更后如何拒绝/再确认需定稿 |
| D6 | N/A确切枚举、原因码、details、8项聚合与LP历史错误语义 | 保留8IDs，模式化N/A排除聚合但不遮蔽真实未知/失败；采用Control共享矩阵 |
| D7 | 历史预检mode标识/版本兼容、新旧Web混布 | 新结果固定mode快照；旧记录按原协议解释，不按当前Account重算；提供可靠discriminator/兼容策略 |
| D8 | 公开观测的mode、停机/切换阶段、owner、最近轮询成功事实 | Control公开只读DTO消费Gateway事实；未返回显示未确认，不绕内网API，不扩散私有字段 |
| D9 | 新建mode默认与旧create body/旧pending幂等兼容 | 新表单显式LP；旧receipt按原契约重放；无receipt旧marker锁定，须有旧契约标记/兼容路径或可证明核对流程后才恢复，禁止无mode自动重发/换key重建 |
| D10 | 功能能力判定、部署顺序与LP数据后的回退 | 先reader兼容再开放write；旧Gateway不得误接LP；应用回退要有模式数据前提 |
| D11 | 同一物理Bot跨scope的唯一性/接收互斥 | 当前DB仅UNIQUE(scope_id,provider,provider_account_id)，不是全平台唯一；需Control/Gateway定义跨scope方案/冲突code，Web不补跨租户探测或自称全局独占 |
| D12 | 批级Gateway配置digest与逐任务mode/策略/适用事实如何绑定 | 保留批级平台事实，另按任务固定模式解释；Control拥有摘要算法/Schema，Web只消费，混合模式批次及历史digest必须回归 |

## 13. 计划交付和交叉回读记录

本文件在原Web worktree新增；仅更新设计。初稿落盘时两侧文档尚未读到，本次已回读实际初稿并补充对齐结论。以下是2026-09-07的文件快照，不代表已提交或wire冻结：

- [Gateway 总计划](/Users/jfs/Projects/trpc-agent-service-channel-gateway/docs/architecture-next/channel-gateway/telegram-receive-modes-plan.md)：SHA256 `10eec8aad4dbf07f6d967b6cbb2a1c0b23ede308b2178b7bbf844dd068550225`；核对第4、7、8、9、12节。
- [Control 独立计划](/Users/jfs/Projects/trpc-agent-service-channelbinding/docs/architecture-next/control-api/telegram-receive-mode-plan.md)：SHA256 `a31dea2f369fe06ba8ad557c279197022efcc20174f06190243cb1ab4e8cbfaa`；核对第3、4、7.3、8、9节及协调反馈。
- Web回读前本文件SHA256：`0ce710a12fdf20247776f06dc27231e318d36434f47d338a25acd034e93535fc`。
- 两侧与本文件的D编号是各自局部编号；跨文档按主题映射，不将同号决议直接等同。

### 13.1 与总计划指定章节的核对

| 总计划章节 | Web对应位置 | 结论与需明确的措辞 |
|---|---|---|
| §4 账户配置/默认/修改 | §1、3、4、5、8；D1–D4、D9 | 方向一致：Account.config而非Binding；新默认与旧请求分开；disabled-only纯配置保存、连接版本推进、保留Secret。旧无receipt pending兼容门槛已补入§8 |
| §7 多副本/模式切换 | §4、6、8；D1、D2、D5、D8、D11 | 无实质冲突。§7.2顺序图应明确：Web可在disabled后保存期望配置，不要求前端等待或证明全局quiescence；Gateway必须在新receiver启动前证明旧路径静默。外部接管确认由服务端绑定当前修订/远端事实 |
| §8 预检/运行观测 | §6、7；D6–D8、D12 | 一致：只读getMe/getWebhookInfo、固定八项、N/A非PASS、旧结果按旧策略；批级共享摘要与逐任务模式事实分离。公共观测字段依Control唯一契约，不公开或自算内部游标 |
| §9 Web交互 | §3–10 | 一致：扩展已有页面、停用/保存/启用分别确认、只显示事实、真实Session验收。补充超时措辞：当前10秒是Web客户端预算，BFF本身未设显式上游deadline；不任意延长现有预算 |
| §12 待共同冻结 | §12 D1–D12 | 七个主题覆盖Web待决议，无新增独立业务模块。optional Secret初始版本归入配置/凭据主题；公开观测归入停旧/启用主题；旧pending、结果policy/摘要、发布门槛仍需逐字冻结 |

### 13.2 Control候选方案的Web接受情况

以下均是**方向接受、尚未冻结**，不是产品现状或实施授权：

- **复用PATCH/account CAS**：符合D4。后端可在一个PATCH原子修改metadata与mode；Web将模式操作单独确认，是交互拆分，不要求后端禁止原子输入。mode变化account/connection各推进一次；同mode NOOP与原请求幂等由Control统一定义。
- **保留WebhookPath及两条稳定凭据元数据**：符合§3/5。optional Secret用服务端预分配ID、`version=1/configured=false`解决首次replace的合法CAS；Web只读取服务端返回版本。disabled可先保存缺Secret的Webhook配置、enable再校验；创建要求与enabled禁止clear规则不顺带放宽。
- **固定八项及NOT_APPLICABLE候选**：LP第3/6/7项N/A、第4项继续getWebhookInfo且现存Webhook为阻碍FAIL、第8项保持未真实投递；Web可以消费，等待完整mode/policy×status/code/details/aggregate fixtures。FAIL是诊断结果，不额外发明“必须全PASS才能提交经明确确认的启用”门槛。
- **旧结果双读与双摘要**：认可新结果公开区分mode/policy，旧结果由明确legacy variant解释。认可LP不因无关origin变化而STALE的方向，但必须连同有效摘要、同lease固定授权、重领/receipt规则一起冻结；Web不自行实现摘要或按最新账户重新聚合。
- **旧receipt与无receipt旧pending分开**：Control第9节确实解决了两类不同问题。已落库记录可按原MAC/原响应重放；未落库旧pending仍是发布阻塞决议，本文件§8和D9已强化，不将其标作已解决。
- **运行观测与跨scope Bot归属**：基础状态加稳定reason比另造Web状态机更合适；owner有效性由后端决定。跨scope唯一索引/registry及迁移重复审计仍待定，Web不补跨租户查询或宣称约束平台外poller。

### 13.3 本轮收口

指定章节没有实质设计冲突；停旧门槛与超时层次有两处需要总计划澄清措辞。§12仍保留12项共同决议，其中若干已收敛到Control候选，但尚无新增wire获准冻结。Web计划及回读结论回传Gateway统筹，并将旧pending结论同步Control。本轮在计划完成处停止；后续由统筹向用户汇总三份文档与待决议表，不开始W0–W4实施。

## 14. 本轮文档验证记录

验证针对设计文件，不是功能测试：

- `git fetch origin` / `git rev-parse HEAD origin/main`：两值均为f61d49b09b8ae534f39f8065ddec699aa35ee10e。
- 文档UTF-8重开、Markdown围栏配对、本地源码/计划链接存在性、D1–D12唯一性及指定章节回读矩阵检查；按实际输出核对，不构造产品测试通过数。
- 已执行 `git diff --exit-code HEAD -- web api services` 和 `git diff --cached --exit-code`，两者输出为空、exit0；实际Git状态仅本新增文档。
- 完成后回传绝对路径、文件SHA256、核对基线、待定决议与检查结果给Gateway统筹。没有执行任何Bot请求、产品构建、测试、提交、推送或部署。

交叉回读完成后的文档检查实际输出（退出状态0）：

```text
PLAN_CHECK=PASS
LOCAL_LINKS=17
OPEN_DECISIONS=12
CROSS_REVIEW_SECTIONS=4,7,8,9,12
TRACKED_PRODUCT_CHANGES=0 STAGED_CHANGES=0
UNTRACKED_DOCS=1
EXIT_STATUS=0
```


## 15. 实施记录（2026-09-07）

本节是后来收到“开始代码开发”后的增量，前面0–14节保留设计阶段的事实与待决议记录，不把历史计划措辞当成本阶段状态。工作分支 `codex/web-telegram-receive-modes`，起点 `f61d49b09b8ae534f39f8065ddec699aa35ee10e`；本任务只修改Web与其文档，Control独占共享契约，Gateway统筹集成。

### 已采用的协调结论

- Account create/PATCH的模式是 `config.receive_mode`；新表单显式LP。PATCH用account CAS且disabled-only，metadata与模式在Web分开确认；生成WebhookPath保留只读。
- LP可省略Secret，服务端保留两条稳定元数据、首次Secret版本1。保存缺Secret的Webhook模式允许，启用校验必需项；Web不伪造credential version。
- 未落库旧Telegram pending已获得Control/Gateway认可的恢复协议：无config原body、两项原replace、原key，加 `X-Channel-Create-Contract: webhook-v1`。仅明确确认后重放；不补LP、不换key。
- 全部Channel命令响应由 `X-Channel-Result-Contract: receive-modes-v1 | webhook-v1` 区分。只有后者允许历史receipt缺mode；普通GET/list仍要求明确mode。BFF精确透传请求/响应头，不改变权限。
- 预检新GET有成对 `receive_mode` / `diagnostic_policy=telegram-receive-modes-v1`；claim后另有 `effective_config_digest`。旧结果三字段均省略，按旧Webhook协议原样读取。
- LP N/A固定第3/6/7项，机器状态NOT_APPLICABLE，code分别PUBLIC_ORIGIN_NOT_APPLICABLE、DELIVERY_ERRORS_NOT_APPLICABLE、RECOVERY_MATERIALS_NOT_APPLICABLE，details为 `{applicability:"NOT_APPLICABLE"}`。第4项无Webhook是PASS、已有Webhook是阻碍FAIL；最后一项仍UNKNOWN/DELIVERY_NOT_TESTED。
- V1只协调平台已管理或空Webhook；未知外部Webhook保留冲突。Web没有盲目接管入口；预检FAIL不被当作客户端自造的全绿启用门槛。
- 观测新增receive_mode，逐实例展示后端reason与当前/历史版本，不在浏览器按owner_epoch选主。

### 验证与运行记录

- 原基线525项Web测试通过；新增三项核心回归在基线准确失败，分别暴露模式被白名单丢弃、LP误要求Secret、pending丢模式/用途集合。
- Account阶段545项Web测试通过，TypeScript与Next production build通过；最终预检/历史兼容回归与真实BFF结果在交付证据中独立记录。
- 已新增可重复运行的真实BFF脚本 `web/test/channel-receive-modes-real-e2e.mjs`，只对显式指定的隔离环境创建disabled合成身份，不发送Telegram请求、不启用账户。
- 本阶段不push、不合并main、不部署、不操作真实Bot。最终本地提交与副本回退证据返回Gateway协调任务，由其执行集成门禁。


### Web源码交付检查

- 已消费Control共享契约提交 `4939e1f527cd2060476e657f515a08659aeca7af`；本分支cherry-pick为 `4a0f998`，api内容逐字一致，Web未修改共享目录。Web最终提交须与该共享依赖共同集成。
- Web全量 **569/569** 通过，含直接读取共享 `preflight-polling-view-valid.json` / `preflight-polling-queued-valid.json` 及八项checks逐字对照。TypeScript、Next production build和差异空白检查通过。
- 一次性副本恢复了136个基线Web/文档文件的原哈希及属性，原有 **525/525** 测试通过；只加入最初三项新回归后，三个原缺陷再次全部复现，退出1符合预期。活动Web工作树仍保留新实现。
- 并行全量回归曾发现一个原测试在DOM提交后过早断言revision读取effect；已改为等待原有精确断言成立，没有放宽调用参数或产品行为。
- 源码提交时真实Control BFF脚本已通过语法检查，彼时仍等待隔离实例；后续真实HTTP和浏览器结果见下方增量记录。真实Bot、消息接纳和回复另由统筹验收。
- 四件交付证据位于 `/private/tmp/web-telegram-receive-modes-b4P1cz/`：`MODIFIED_FILE.tar.gz`、`DIFF_FILE.patch`、`VERIFICATION.txt`、可执行 `ROLLBACK.sh`。回退工具仅接受该证据目录下带专用标记的一次性副本，不操作活动worktree。

### 真实 Session / BFF / 浏览器增量验收

- Web功能提交为 `ef9a075b0500a841ff4dc424ec9e971acc30e8c1`；交给Gateway后保持不变。本次补充只涉及测试断言计数与本文档，不改产品代码或共享契约。
- 使用Control任务提供的独立数据库、真实Session和专用HTTP实例 `127.0.0.1:52775`，本任务启动临时Next production实例 `127.0.0.1:13105`。未修改既有部署。凭据只通过受限本地配置读入进程，不写入仓库或测试报告。
- `web/test/channel-receive-modes-real-e2e.mjs` 实际通过 **70项断言 / 21个HTTP请求**。覆盖匿名401/no-store、LP Token-only创建、可选Secret稳定版本、disabled模式PATCH/CAS/重放、metadata与connection revision分离、首次补齐Secret，以及未落库旧Webhook请求恢复和幂等冲突。成功结果与退出0记录于 `live/real-bff.stdout.log`、`live/real-bff.exit`。
- 真实浏览器登录与操作通过 **31项检查**：默认LP、只填Token创建、身份确认、取消不提交、LP→Webhook→LP、模式变更保留凭据和停用状态、缺Secret仍可保存Webhook并提示启用前补齐。检查未点击启用或触发预检。
- 浏览器只读DOM几何检查覆盖 **1440×1000、768×1024、390×844**，三者document scrollWidth均等于viewport宽度；桌面卡片等高对齐，手机单列，模式选项点击区域高度均大于44px。浏览器warning/error日志为0。
- 操作结束后额外通过真实Session GET回读：**13项断言 / 4个HTTP请求**，三个测试账户全部 `enabled=false`。原BFF账户回到LP、connection revision 6；浏览器创建账户保存Webhook、connection revision 2、Secret仍为version 1且configured=false。
- 证据目录为 `/private/tmp/web-telegram-receive-modes-b4P1cz/live/`，含 `browser-checks.json`、`browser-final-http.json`、`browser-final.dom.txt`、`create-desktop.png`、`create-tablet.png`、`create-mobile.png`、`missing-secret-saved.png`。截图均已实际回看。
- 所有账户均为隔离合成身份；本轮 **enable=0、preflight=0、Telegram请求=0**。这里的真实验收指Session、Control、BFF与浏览器操作，不将其写作真实机器人收取消息或执行回复成功。
- 本任务已恢复浏览器原尺寸、关闭专用标签页，停止临时Next进程（Ctrl-C退出130）；Control实例交还所属任务管理。结果已回报Control和Gateway，远端集成由Gateway统筹。
