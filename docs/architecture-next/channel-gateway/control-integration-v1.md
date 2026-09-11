# Gateway Control 接入：设计、实现与真实入站验收

- **设计状态**：2026-09-06 已完成双方协议复审；由 Channel Gateway 任务在独立工作树维护。
- **实现状态**：GCI1/GCI2 已落地 Control mTLS 目录、托管凭据 bridge、账户事务门禁、动态 Telegram 注册/接收、Delivery Runner、观测与 Compose；真实 Telegram 入站至持久 RunRequested 已验收。
- **范围**：Control 账户/绑定到 Gateway 接入与持久入站；真实 Worker、Manifest 正文执行和模型/Storage 可执行性不由本次入站验收替代。
- **代码核对**：本文描述随代码交付的 GCI1/GCI2 接线与 GCI3 入站验收；历史验证基线为 `bf10776`，提交与 main 集成状态以 Git 历史为准。
- **协议权威**：[账户设计](../control-api/channel-account.md#ca-sync-v1)、
  [凭据接口](../control-api/channel-account.md#ca-credentials-v1)、
  [观测接口](../control-api/channel-account.md#ca-observations-v1)、
  [路由生产端](../control-api/channelbinding.md)。

Control 与 Gateway 集成后使用仓库相对链接定位拥有方协议；此处不复制第二份 Control 协议正文。

## 1. 用户目标与当前差距

用户添加自己的机器人并填写凭据，平台保存渠道账户，Gateway 无需修改运维文件或重启即可
接入；用户再绑定精确部署修订。连接凭据与路由目标是两种数据，不放进同一可回放事件。

当前已按实际代码与运行核验：

| 所属位置 | 当前实现与边界 |
| --- | --- |
| Connection `accountcatalog/catalogrefresh` | 完整快照、scope/epoch/摘要/单调水位、非秘密目录与实例资格；Supervisor 只消费企微账户 |
| Connection `accountuse`、`controlhttp` | 精确 tenant/account/purpose/version 解析；Supervisor 传递原始 OwnerGrant，解析前后和构造前检查 |
| Admission / Delivery PG Adapter | consumer-owned AccountUseGuard 已注入实际事务；A1 Claim 与 A2 Attempt 固化资格/客户端绑定 |
| Telegram runtime / registrationpostgres | 动态 Handler、持久 fence、单次 BEGIN_CALL、GetMe 身份核验与真实 setWebhook、迟到原操作事实 |
| bootstrap / accountobservations | Control 来源成为生产默认；有界运行循环与观测上报已接线；fixture 来源必须显式选择 |
| Compose | 引用 mTLS 文件、scope/source epoch、公网 origin；仍只有 Go Gateway，不增加独立 Connector 单元 |

原始独立 Supervisor 构造器默认仍为100；Control bootstrap 显式设置 MaxAccounts=1000，
与 scope 完整目录容量一致。此配置对齐不等于1000个真实机器人并发吞吐验收。

真实入站证据见[2026-09-06 联合验收](telegram-real-inbound-20260906.md)。

## 2. 模块职责与不变的运行结构

- Control 的 channelbinding Module 拥有账户、私有凭据、绑定与期望配置。
- Gateway 的账户快照 Adapter 验证协议/来源并持久化非秘密投影；Connection 消费已验证配置。
- Connection 继续负责企微 SDK Client、lease/epoch、建立/更新/关闭，不承担租户账户 CRUD。
- Telegram 接收 Adapter 负责 webhook校验与入站资格；Delivery 使用发送用途凭据，不伪造企微租约。
- Routing 继续消费 control.channel-route.v1，Admission 继续固定不可变运行目标。
- 8091 继续是探针，不增加未认证的账户、凭据、启动机器人或通用管理接口。

仍是同一个 Gateway 进程直接使用 Go SDK；不增加 Connector 进程、Secret 服务或账户同步服务。
Control 的内部 HTTP 是启动/刷新与凭据初始化路径，不是逐消息同步路由查询。

## 3. 完整账户快照接入

HTTP 字段、认证、大小与时间值以账户文档 #ca-sync-v1 为唯一权威。本扩展选择有上限的完整
快照轮询，不引入第二条账户配置 NATS 流，不复用路由事件承载连接配置。

### 3.1 已实现的接口与应用顺序

实现将账户运行目录放在现有 Connection Module 内，由专用刷新Use Case与SnapshotStore承接，
不新增第五个业务Module，也不把 wire DTO 直接传入现有 List。它需要表达 scope、epoch、
revision、digest、complete 与所有账户的完整性；Connection目录同时保存两种provider，
企微Supervisor只消费其中的wecom账户。Telegram接收/Delivery通过使用方只读Port获取其
当前账户资格与用途映射，bootstrap注入拥有方实现，不导入Connection的Repository。
业务 Connection 不应导入 Control 的内部 Go类型、HTTP客户端或对端 Repository。

```text
Connection RefreshAccounts用例经SnapshotSource取完整响应
 → 验证mTLS来源/授权scope/预期epoch、关闭DTO、全量边界和摘要
 → 与本地持久目录核对账户身份及版本单调性
 → 同一Gateway PG事务写非秘密完整投影与水位
 → 原子交付一次可用账户集合
 → Supervisor按connection_revision应用
```

- 源失败、部分列表、超限、错误schema、未完成响应不能作为成功空集合。
- 捕获请求前共享水位H_start，再在提交时读H_commit。H_start<=R<H_commit属于SUPERSEDED，
  不覆盖投影、不续本实例新鲜度且立即重拉；R<H_start才是SOURCE_ROLLBACK。
  已知同epoch同revision异Digest始终隔离；详情及短期receipt见Control #ca-snapshot-order。
- 候选快照先完成上述分类；只有SAME/ADVANCE才与当前账户运行/凭据/min_route_generation
  比较。合法旧响应不能因另一个副本先推进账户版本而被误报为回退。
- source_epoch 改变必须走显式重新建立信任流程；不能自动接受任意新源的低版本数据。
- 只有同scope、完整性确认的缺席才意味着移除资格；正常停用保留disabled项。
- 上限1000包含已停用账户；不能只调HTTP响应限制而保留Supervisor累计身份容量100。
- V1真正刷新错误立即取消新工作资格，SUPERSEDED不是源错误也不自动续资格；30秒freshness watchdog必须
  独立于被阻塞的HTTP调用，避免慢请求无限延长资格。
- 重新启动先成功刷新再启用；持久目录只提供单调校验依据，不恢复离线凭据或自动开放接纳。

快照的事务应用不等于所有SDK同时完成切换。目录就绪、账户配置已应用、连接READY分别记录。
多副本独立拉取和核验同一scope，企微租约继续防止同账户双Owner。metadata变化不得重建Client。
共享PG内相同epoch/revision/digest应用幂等；慢副本的合法旧响应只能成为SUPERSEDED，不可
覆盖快副本的投影，也不可清掉整个scope资格。每个副本freshness来自自身有效请求起点；
持续SUPERSEDED超过30秒仍过期，不能借其他副本写库续期。共享PG保留90秒非秘密snapshot
receipt以检测在途窗口同版异内容，超5秒deadline的迟到响应不得再入账为新资格。

### 3.2 账户目录、实例资格和本地失效令牌（P1-1）

Connection目录拥有三类不同数据：非秘密账户投影（scope/source_epoch/Tenant/Provider/Account、
connection_revision、enabled、min_route_generation与用途映射）、每实例的资格行，以及当前
进程的AccountUseToken。账户投影的停用/移除保留同身份disabled行，不删除guard行后重建身份。

每实例资格行绑定instance_id/instance_epoch、scope/source_epoch、qualification_generation、
最近自身成功请求水位、valid_until和enabled。valid_until上限为请求前从Gateway PG取到的
request_started_at_db+30秒，另有从同一次请求单调起点计算的本地30秒截止；两者都必须有效。
更新目录/资格来自可信RefreshAccounts用例，不复用Control观测上报，也不接受业务请求自填证明。
共享目录较新不替其他实例续资格；本实例只有完整验证SAME/ADVANCE后才更新自己的资格行。

AccountUseToken是Connection拥有方颁发的进程内句柄，绑定账户、精确连接版本、用途、
instance_epoch、qualification_generation、client_generation和取消ctx；不是API bearer token。
源失败/freshness到期、配置换代、停用、失租或进程关闭会先使本地句柄失效，并尝试持久化
本实例资格失效；DB故障不能成为继续工作的理由。失效持久化与已持SHARE锁的操作按事务
提交顺序排列；本地取消立即防止尚未进入操作的请求使用旧句柄。

相同connection_revision的metadata/路由floor更新不重建Client；guard总读持久账户的当前floor。
资格句柄失效与SDK实例换代分开：成功刷新可签发新资格句柄，但不能复活旧owner或旧Client。
失效代数只能由可信拥有方推进，不以Handler缓存里的enabled布尔值替代。


### 3.3 失败恢复

源恢复且通过所有完整性检查后可恢复资格，再按精确连接版本取得凭据并重建SDK。
epoch/内容冲突不是普通暂时网络失败，不靠无限重试或清库自动修复。
Control快照错误不清除可信路由历史，路由日志错误也不通过账户快照抹去Stream lineage。
部署重建信任流程必须停止新接纳，核验权威目录身份并清理账户目录的旧授权资格后再初始化；
不能悄悄释放或跳过尚未终止的旧SDK，现有lease/fence仍需满足。

## 4. 托管凭据与所有者时序

内部接口/consumer用途集合以Control #ca-credentials-v1 为准（含新增telegram_registration）。Resolver必须使用真实工作负载身份，
带scope、source_epoch、可信Tenant/Account、精确connection_revision、purpose/ID/version集合。

企微新连接的顺序：

1. 从已验证完整目录获得Account与用途映射。
2. 从现有LeaseStore取得当前OwnerGrant。
3. 请求前检查grant、context和精确连接版本。
4. 通过Control内部认证接口读取该账户允许用途的值。
5. 收到响应后重新检查grant/context/版本与所有响应绑定字段。
6. SDK构造与attach前再次检查；失败即丢弃材料并清理未attach的Client。

裸OwnerGrant/owner_epoch不构成Control服务器的独立租约证明。Control验证workload与账户用途
授权，Gateway Resolver和连接生命周期执行本地Owner真实性约束。不能宣称Control已核验租约，
也不能为此直接读取对方数据库。若后续需要服务器独立的Owner Authority，另行冻结协议。

Resolver不接受任意CredentialRef/文件路径/env名称；ID由账户投影和服务器映射双向核对。
值只进入当前用途Client的内存，不落盘、不进Snapshot/RunRequested/ReplyIntent/Outbox/Trace。
旧连接版本Resolve返回冲突时先刷新目录，不擅自用最新凭据回答旧请求。
网络库不透明重试并拼接版本，返回成功也不延长已失去的lease或账户资格。

## 5. Provider特定接入

### 5.1 企业微信

继续使用现有公开Go SDK和Connection lease/epoch。bot_id与可信账户物理身份核对；
静态录入不等于在线认证成功，SDK认证成功与可获取远端身份核验后才READY。
凭据轮换推进connection_revision，在当前Supervisor的quiesce/drain/close与重建路径应用。
不让Control为了校验凭据建立第二条企微连接造成replacement；实际认证失败作为运行诊断。

### 5.2 Telegram

Telegram webhook可由多副本接收，不为取凭据制造企微OwnerGrant：

- telegram_webhook只取webhook校验Secret；新鲜enabled配置即可安装本地Handler，不以已收到首条Webhook作为取值前提。
- telegram_delivery只取Bot API Token；准备资格不等于发送许可，真正调用仍过A1/A2账户事务guard。
- webhookSecret与发送Token轮换是不同用途；每个Client/Handler按当前完整版本原子换代。
- 本地webhook handler配置更新不等于远端setWebhook已完成。远端注册/变更是显式操作，
  需要同账户单一reconciler和可观察结果，不能每副本每次轮询都调用setWebhook。
- 当前 reconciler 已使用 Gateway PG 建立 Telegram 专属注册 fence，不复用企微 lease。
  注册 lease 25秒，最多8个账户并行；正常 READY 60秒重申，失败5秒退避。

<a id="gci-telegram-registration"></a>
#### 注册consumer与bootstrap（P1-2）

新增telegram_registration，Control只对获准注册的mTLS principal返回同一connection_revision下
恰好两项：telegram.bot_token和telegram.webhook_secret。请求需携带精确ID/版本和本地
registration_epoch；该epoch只供绑定/审计，Control仍独立做scope/Tenant/Account/purpose授权。
它不复用Delivery Claim、不伪造企微OwnerGrant，也不要求路由、首条消息或先前注册READY。

当前流程：本实例取得新鲜完整配置后，通过 telegram_webhook 用途独立安装当前版本 Handler；
注册协调器再竞争账户级 fence，同版批量取两项凭据，GetMe 核实物理 bot ID，经 BEGIN_CALL
取得单次许可后调用 setWebhook，核对 fence/版本/取消令牌后保存该版本注册观测。
Handler安装只代表本地准备，不代表远端已注册；新Admission还要满足账户、路由和目标门禁。
注册fence键为(scope,provider,account_id,operation=set_webhook)，由Gateway PG持久分配，
lease/epoch与current connection_revision绑定。只有当前持有者调用，调用前/响应后/提交观测前
检查资格与fence；所有副本的普通轮询不直接调用setWebhook。

注册fence失效/配置换代时取消操作；迟到响应只能记录原operation/版本的事实，不能标记新版本
READY、重新挂载旧Handler或重复发旧配置。Provider不支持本平台epoch条件写，已发出的HTTP
请求可能有迟到副作用，PGfence不构成远端强制fencing。超时/结果不确定记UNKNOWN，由当前
持有者对最新期望版本重新协调；本地只接受当前版本Secret，不恢复旧Secret来掩盖故障。
注册READY是有时间/版本的观测，不是远端永不变化的授权证明；不得以忽略旧ACK声称撤回旧请求。

telegram.webhook_secret必须匹配 `^[A-Za-z0-9_-]{1,256}$`，注册与Handler初始化均防御复核；
非法值不发网络请求。Control先在静态输入Schema/Domain拒绝，规则来自
[Telegram setWebhook官方协议](https://core.telegram.org/bots/api#setwebhook)。


注册对端使用的平台公共HTTPS地址和账户路径不接受租户任意URL；错误信息不得包含带Token的
Bot API URL。Webhook注册失败时报告未就绪，不能以本地Handler存在声称机器人已接入。

### 5.3 启停与停用后的回复

Delivery.AccountEligibility 保留 local precheck，GCI2 已把新增账户事务 guard 接到实际
Admission/A1/A2。下面分别约束“新工作准入”和“既有结果落账”，不以内存 enabled 布尔值
替代共享数据库中与停用提交互斥的事务资格。

<a id="gci-account-guard"></a>
#### AccountUseGuard、锁序与线性化（P1-1）

消费方定义AccountUseGuard事务seam，由Connection目录拥有方实现并经bootstrap注入；
与已有ConnectionGuard相同，可在消费方PostgreSQL Adapter接口中接收当前pgx.Tx。
Application/Domain不导入PG，Admission/Delivery不直接编写Connection目录SQL。

新工作事务先锁持久账户行FOR SHARE，再锁本实例资格行FOR SHARE，核对Tenant/Provider/
Account/scope/source_epoch、enabled、精确connection_revision、用途和qualification_generation，
同时要求DB clock_timestamp()<valid_until及本地AccountUseToken仍有效。该检查与新工作状态
写入同一事务；新工作SQL还要复核有效期，ctx截止不晚于本地资格期限，commit不确定不授予许可。
目录更新按catalog行→账户行（稳定键排序）→本实例资格行取得UPDATE锁；停用与SHARE guard
互斥。目录写事务不在持锁期间调用SDK、连接lease、Routing或Delivery，失效通知/关闭在提交后处理。
单实例源失败只撤销该实例资格，不能把同scope其他健康实例的账户enabled改为false。

| 操作 | 在新账户guard之后的锁/校验顺序 | 额外语义 |
| --- | --- | --- |
| 新Admission | Inbox幂等键锁→预算行→企微Connection owner（适用时）→Routing generation guard→Inbox/RunRequested写入 | account.floor与所选route在同一事务核对，避免预检之后变更 |
| A1 ClaimDue | Delivery part稳定顺序/SKIP LOCKED→原有Connection owner（企微）→Claim状态 | Claim携带本次AccountUseToken绑定及连接版本，不把准备成功当发送许可 |
| A2 MarkCalling | Delivery part→原有Connection owner→Claim/请求Digest→Attempt写入 | 再次AccountUseGuard，A1后的停用/轮换/失效不能被跳过 |
| 注册BEGIN_CALL | 注册operation/fence行→该版本注册Attempt | 同样在AccountUseGuard之后取得调用许可，不只检查本地reconciler布尔值 |
| 完成/迟到结果/维护 | 保留现有part→attempt/证据与Owner历史语义 | 不新增enabled/freshness/floor准入guard，不要求重新取得当前账户凭据 |

这里是在现有操作外层增加共同的account→instance guard前缀，不把A1/A2内部的part→owner
擅自反转。完成/维护也不得持part后回头获取账户准入锁，避免与新工作形成反向锁链。
新Admission仍Receipt-first：已有同摘要Receipt可返回，不重新准入；初次查询miss且后续guard失败时
再做只读Receipt复查，避免并发已提交的重复消息被错误当新工作。无Receipt时才按资格错误拒绝。

AccountUseToken在开始准备、进入事务、commit前以及实际调用SDK前均检查；A1/A2还核对
捕获的client_generation/ownerctx，任何不匹配都停止新调用。持久guard解决跨副本目录变更，
本地句柄解决当前进程的freshness/Client换代，两者缺一不可；资格表由拥有方写，不接受自报期望值授权。

停用的线性化点是Gateway目录disabled事务提交，发送授权的线性化点是A2成功提交CALLING：
停用先提交，则旧Claim的A2失败且不调用SDK；A2先提交，则该唯一Attempt已进入有限在途状态，
可能在停用之后结束。若实际调用前本地已观察失效且Sender可以证明未调用，保存NOT_SENT；
不能证明则保留UNKNOWN/既有证据处理，禁止盲目重发。PG事务不跨网络持锁，不宣称撤回已授权在途字节。
新Admission及注册BEGIN_CALL同样按guard事务提交顺序与停用排序；注册观测完成沿原operation
证据落账而不重新申请调用资格。Control commit本身不是全副本停用确认。

Account撤权不能阻断已发生副作用的Finish、FinishPreparation、Observe或维护/终态收敛；
这些路径仍验证原Claim/Attempt身份、证据token、请求Digest和原有owner lineage，不赋予新的发送权。
如果现有Owner检查阻止旧Owner直接Finish，使用既有持久证据/Observe收敛路径，不通过解除账户guard
就顺势删除防伪校验。已提交Run不改Manifest/ReplyOrigin，旧回复不改投新账号或新Binding。


## 6. 路由与账户到达顺序

路由沿用已确认字段，不调整现有route-projected Schema：

```text
deployment_revision_id = DeploymentRevision.ID
manifest_ref = RuntimeManifest.ID
manifest_digest = RuntimeManifest.ContentDigest
```

manifest_ref是不透明标识，Gateway不在Routing中解析它。RuntimeManifestPublished使用字符串v1
的独立envelope；route-projected使用整数1，不能互相包裹。

账户先到而路由未到：渠道可以连接，Agent新运行不得接纳。路由先到而账户未初始化：也不接纳。
当前 Admission 检查账户资格、可信路由完整性/代数/租户及 route floor，并固定三元组。
Control 在真实发布事务从同 Tenant 的已发布 DeploymentRevision/RuntimeManifest 读取 ID 与
ContentDigest，再经受限 Relay 发布；Gateway 不存在恒 true TargetReady，也不下载 Manifest
正文。完整路由与发布目标身份的验证不等于正文内容重新哈希或执行可用性验证；后者属于
真实 Worker 的分发/读取/执行边界，不是本次入站新增同步查询前置。

Binding修改只更新Routing，不推进connection_revision，不无故重连。停用任一资格立即阻止
后续新工作；两个供应通道异步到达，Control commit并非瞬时全副本撤销，必须报告观察到的版本。
所有已接纳Run与ReplyOrigin按原快照保持，不以新绑定改写历史执行目标。

<a id="gci-route-floor"></a>
### 6.1 重新启用的路由恢复屏障（P2-2）

账户快照必需字段min_route_generation由Control与有效路由generation/Outbox/目录水位同事务
产生。初始0仅表示没有发布路由，不产生默认目标。新Admission在AccountUseGuard与RoutingGuard
的同一事务内要求 `route.generation >= account.min_route_generation` 且route.enabled、
Tenant/Provider/Account匹配；低于下限拒绝新工作，不静默使用旧目标。当前 Adapter 将该
资格错误映射为可重试 unavailable/HTTP503；ROUTE_BEHIND_ACCOUNT 是语义描述，不是已发布 HTTP code。

这样HTTP跳过disable中间态只看到最终enable时，仍会看到恢复时的更高下限，旧enabled路由
必须等待。Binding-only变化引起floor/目录水位推进但不改变connection_revision、不重建Client。
只改变账户floor时不能靠旧Handler快照绕过持久guard。既有路由Schema不新增该字段。

屏障保证“已应用新账户快照后不使用更旧路由”，不保证所有副本在Control提交瞬间一起切换；
尚未观察变化的实例仍受自己的有限freshness约束。Delivery处理旧Run不检查当前route floor，
只检查当前账户/用途/连接版本和原发送证据，不按新路由改投旧回复。

## 7. 启动配置、状态与部署

production模式要求显式配置Control内部地址、mTLS证书/可信CA、scope与预期source_epoch，
HTTP客户端/目录Store由Gateway bootstrap创建，业务Module只接显式依赖。缺少配置则fail-start，
不自动降级到文件/env；dev/test Adapter必须明确选择且不能与同一生产scope混用。

所有Gateway副本的快照上限、Supervisor累计容量、HTTP大小限制与Control配额对齐。
10秒±20%轮询、5秒快照HTTP总期限、30秒freshness见Control权威表；修改值必须同步文档/配置测试。

<a id="gci-time-budgets"></a>
### 7.1 父ctx、凭据与lease操作时间预算（P2-3）

GCI1 已把 construct 中 Resolver 的 credentialContext 与2秒通用 setupContext 分开，
GCI2 owner bridge 再次保留独立2秒检查。通用 OperationTimeout 仍严格小于默认 LeaseTTL15秒
的三分之一；实际预算如下：

| 预算 | 默认 | 父ctx与用途 |
| --- | --- | --- |
| SnapshotHTTPTimeout | 5秒 | RefreshAccounts生命周期ctx；独立于企微lease ctx，受请求/freshness watchdog约束 |
| LeaseOperationTimeout | 2秒 | Supervisor生命周期/lease维护ctx；仍要求严格小于LeaseTTL/3 |
| CredentialResolveTimeout | 5秒 | 当前账户资格+精确连接版本+owner/local registration fence的可取消ctx |
| ClientConstructionTimeout | 2秒 | 同一Owner/账户ctx，单独构造预算；不包住前一段5秒Resolve |

account.go 中 construct 的 Resolver 调用已使用独立 credentialContext，不再经过2秒的
setupContext；lease Renew/Check/Release保留lease预算。Telegram注册/Handler/Delivery准备使用
同一5秒CredentialResolveTimeout，但取各自的资格ctx，不伪造企微lease。
有效deadline取配置上限与父级真实工作期限的较早者；失租、配置变化、源失效或Shutdown立即
取消。凭据等待期间lease续约独立运行，5秒不延长OwnerGrant；禁止使用Background脱离取消。
3秒正常凭据响应在资格持续有效时应成功，失租时即使HTTP尚未满5秒也必须取消。


连接观测按 #ca-observations-v1 上报：状态变化+30秒心跳，90秒未收到为STALE；观测不是授权。
上报失败不令已授权连接失效，配置/凭据失败则按其单独资格规则处理。不能把state=READY或
自报owner_epoch当作可信所有权，也不能用8091整体readyz代替单个账户接入状态。

Control保存成功、快照已应用、连接正常、路由已应用、实际Worker执行分开记录。
这些接线与 Compose 配置已实现；真实 Control 内部 HTTPS 和 Gateway 进程已联合运行。
不新增独立 Connector/同步进程，Helm 仍等全部 workload 完成后统一处理。

## 8. 分阶段实现与验收状态

| 阶段 | 已有实现位置 | 当前验收与边界 |
| --- | --- | --- |
| GCI-0 契约 | 独立账户HTTP Schema/DTO/fixtures，已有api/events/control路由契约保持 | 双方字段/关闭语义/大小限制一致 |
| GCI-1 账户目录 | Connection内RefreshAccounts用例、SnapshotSource HTTP Adapter/PG SnapshotStore | 完整快照、单调水位、epoch、错误fail-closed与原子交付 |
| GCI-2 凭据 | connection/application解析port与构造路径、所属controlhttp Adapter | 可信tenant/scope与Owner检查，响应绑定/过期/取消/内存处理 |
| GCI-3 动态Provider | Connection Supervisor、Telegram入站/Delivery Adapter | 新增、改名不重连、轮换、停用、恢复和Webhook注册 |
| GCI-4 装配/观测 | bootstrap/app.go、配置解析、账户观测上报 | production不回退，100→1000容量明确，状态不冒充授权 |
| GCI-5 组合验收 | 实际 Control/Gateway 进程、mTLS、PG 与受限 NATS | 真实 Telegram 用户消息至持久 RunRequested 已通过；真实 Worker/企微账号及完整回复链仍单独验收 |

每个阶段用真实接口/数据库测试，不将文件账户fixture、手工路由事件或Fake Worker称为用户自助
接入闭环。没有真实外部机器人凭据时，协议/本地SDK fixture验证与外部验收分别记录。

## 9. 必需测试矩阵

1. scope/epoch不符、body超限、完整快照缺席、重复身份、低版本/同版异Digest。
2. 初始空目录、目录满1000、100→1000配置不匹配、重启后必须刷新、存储失败不能应用。
3. 源401/5xx/超时与freshness watchdog，不能把失败当删除或继续使用旧资格。
4. 跨租户/错误用途/伪造ID/过期版本凭据请求被Control拒绝；普通User Session不能调用内部API。
5. Acquire后丢Owner、取值中丢Owner、收到响应后换配置、attach前取消，均不启动过期Client。
6. metadata更新不重连；凭据轮换/启停驱动新connection_revision；Binding变更不重连。
7. Telegram Token/WebhookSecret分权、多副本不重复setWebhook，企微真实lease真实性本地验证。
8. 路由/账户先后到达、目标未就绪、账户停用后迟到启用路由；新Run门禁不受迟到事件绕过。
9. 保留旧Run/ReplyOrigin，不把旧回复改投新绑定；停用后发送失败遵守Delivery资格。
10. 观测迟到/重复/旧启动并发、未来时间、自报epoch；不能因此推进控制事实或授权。

## 10. 本轮审查修订与回归清单

原复审补充的6项契约已由 GCI1/GCI2 落地；以下保留验收矩阵，实际运行结果分别见实施状态 §12–14 和独立审计，不将矩阵本身当成测试日志：

| 用例 | 最小屏障/输入 | 预期 |
| --- | --- | --- |
| FIX-P1-1A | Admission预检后目录先提交disabled | 无新Inbox/RunRequested，重复已有Receipt保持可读 |
| FIX-P1-1B | A1完成后停用先提交，再A2 | 无CALLING/SDK调用；FinishPreparation/维护仍可落账 |
| FIX-P1-1C | A2先提交，再停用/源失效 | 仅原Attempt在途；结果/迟到证据可记录，不授权下一次发送 |
| FIX-P1-2 | 首次enabled无Binding/无Webhook/无Delivery Claim | 注册consumer同版取得两项并注册；旧fence迟到不能标新版本READY |
| FIX-P2-1 | 双副本H_start=10，B先写11，A后回10 | SUPERSEDED而非隔离；下一请求H_start=11回10才隔离 |
| FIX-P2-2 | disable→改目标→enable，HTTP只读最终态且路由落后 | floor挡住旧目标，追平才新Admission；不重连仅floor变化 |
| FIX-P2-3 | 3秒凭据响应；另一例期间失租 | 第一例成功、第二例提前取消；2秒lease预算不被改成5秒 |
| FIX-P2-4 | Secret长度0/1/256/257、ASCII/中文/换行 | 按purpose静态规则接受/拒绝，非法输入无Provider调用 |

GCI2 已在真实 PG 执行目录 guard 与实际 Admission/A1/A2/结果路径测试；源码 race 回归28包
通过。具体覆盖与命令在 GCI2 审计包；真实外部收信另有逐字段比对报告，不以 Schema fixture
或本矩阵宣称所有故障组合、真实企微账户或 Worker 执行均已验收。

## 11. Gateway既有文档同步清单

本轮已同步 Gateway 自有总览、Module §15、实施状态、运行说明与设计复审最新入口，清理
被 GCI2 替代的“尚无账户目录/内部HTTP/事务guard/注册fence”现状。历史分轮复审保留时间点，
不改写历史验收；当前状态由本节与真实联合验收报告覆盖。

- 运行设计：本文及 `services/channel-gateway/CONTROL_RUNTIME.md`。
- 当前实施状态：`implementation-status.md` §13（GCI2）与 §14（真实 Telegram）。
- 独立脱敏证据：`evidence/telegram-inbound-20260906.json`，保留真实消息/Receipt/Run/事件标识，
  不包含 sender/chat ID、凭据值、DSN 或完整消息正文。

共享constraints、架构README、Control专属文档由ChannelBinding任务负责；route-projected
Schema/DTO由双方先协调再同步。不要各自产生第二份同名协议，也不覆盖Gateway的未提交实现。
