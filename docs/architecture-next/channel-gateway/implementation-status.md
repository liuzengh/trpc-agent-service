# Channel Gateway 实施状态与当前协议选择

- **历史开发工作树**：`trpc-agent-service-channel-gateway`，分支 `codex/channel-gateway`，基线 `bf107766`。
- **最新复核日期**：2026-09-07；历史切片保持原日期。
- **第一切片历史应用/验证状态**：第一切片已应用到本工作树，真实 PostgreSQL/NATS race 联合测试、全仓测试、vet、生成一致性、NATS ACL、实际镜像启动与有界停止均通过；未提交或发布。验收使用合成 Provider 输入与测试订阅者，不代表真实机器人收发或完整 Agent E2E。
- **上一 Connection 切片：已实现，实际工作树与镜像已核验。** 新增 Connection 账户/lease/epoch/Supervisor、0005、企微入站、同事务 owner guard 与 SDK 直接装配；实际工作树应用、完整联合套件与新镜像验收已完成，准确证据见 §9.5。CGR-27/0004 与 SDK 上一切片的已应用证据原样保留于第 8 节，该切片增量见第 9 节。
- **证据时间点**：第 7–10 节保留此前切片验收；当前 Runtime 切片的分项与最终验收在第 11 节。不以历史日志替代新增代码验证。
- **设计基线**：保留实际工作树最新的四 Module 介绍、CGR-23/24/25 与 Helm FINAL-INTEGRATION；不合并已分叉的本地 main。
- **入口**：[服务 README](../../../services/channel-gateway/README.md)、[Compose](../../../deploy/compose/README.md)、[完整设计](README.md)、[复审记录](design-review.md)。

**2026-09-05 Delivery Runtime 历史切片**：新增 Maintenance、可组合 Runner、PG RuntimePorts、LocalOwner、
0007 五索引及 eventadapter 错误分类。默认 App 仅启动维护，空账户也处理已持久旧账本；
不启用 Sender、发送 Runner、ReplyIntent Consumer 或默认许可。本轮最终验收已通过，见第 11 节。

## 0. 最新阅读口径：设计、实现与验证分开

**2026-09-07 Telegram 双接收模式**：Control/API/Web 与 Gateway 源码在
`codex/channel-gateway-receive-modes` 本地集成；增加长轮询、统一物理 owner、持久 cursor、
模式化预检及旧请求兼容。Gateway 0011/Control 0003 不改旧迁移；同一 Gateway workload。
真实 PG/NATS + 合成 Telegram HTTP 纵切与独立 Control/Web 验证已执行；没有本轮真实 Bot
模式切换或远端 main 发布。当前口径见 [双模式开发记录](telegram-receive-modes-implementation.md)，
精确命令/结果由该工作树 artifacts 下最终 VERIFICATION.txt 记录；以下旧节为历史证据。

**2026-09-06 GCI2 运行接线**：生产来源现在默认 Control mTLS；凭据 bridge、Admission/A1/A2
账户门禁、Telegram 持久注册 fence/BEGIN_CALL/动态 Handler、Delivery Runner 和 observations
已接到实际 App。当前迁移 0001–0010；独立 Go Gateway workload 不新增 Connector 容器。
真实 mTLS + PG + NATS 的合成远端纵切已通过；真实 Control 进程 + Telegram 用户消息的
联合入站验收也已通过，单列在第14节，不用本地 fixture 代替。下面 GCI1/GCI2 分项保留历史记录。

**2026-09-06 GCI1 基础切片历史补充**：本树新增 Control 账户目录模型、共享 HTTP Schema 消费、
mTLS Snapshot/Resolve Adapter、RefreshAccounts 生命周期、PG 目录/实例资格/事务 Guard 与
0008；Supervisor 的凭据预算与源变更取消已修改。下方旧 Runtime 表保留其切片日期，
该历史切片范围见第 12 节，当时迁移文件为 0001–0008；最新状态以第 13–15 节为准。


2026-09-05，当前继续实现 Runtime 切片。第 7–10 节原文保留此前验收记录；第 11 节记录
本轮源码范围与最终运行结果，不以旧日志或临时副本测试替代本轮实际应用验收。

| 能力 | 当前源码状态 | 阅读结论 |
| --- | --- | --- |
| 四 Module / 公开企微 P0 / Final Ledger | 保留既有实现 | 一个 Go Gateway Workload，不增 Connector |
| Delivery Maintenance | Maintainer 与 PG RuntimePorts；默认 App 独立运行 | 无账户/owner/Sender 也维护旧事实；不代表启用发送 |
| Delivery Runner | 有限页/worker、每账户一个 dispatch、Quiesce/Drain 与 eligibility 已有 | 可显式组合，默认 App 未启动生产 Runner |
| Connection LocalOwner | 从真实 localLease 返回有效 grant 拷贝 | 只是本地预检查，PG A1/A2 与原 Sender 匹配仍必需 |
| CGR-36 错误分类 | 确定坏 wire 与内部 Decode 不可用已分开 | Consumer/持久拒绝/ACK 尚未交付，修复本轮最终验收已通过 |
| CommittedFinalVerifier | 既有 Gateway Port/精确匹配 | 真实 Execution Owner/认证/完成 Outbox 尚待交付 |
| ReplyIntent Broker 接入 | 只有 eventadapter，仍为 Route/Run 两个 stream | Reply transport receipt/拒绝、ACL 与 Consumer 未交付 |
| 0007 | 五个 Runtime 查询索引，当前共七个迁移 | 不改旧 SQL、不清理事实、不释放容量；升级验收见 §11 |
| CGR-37 / 有效发送截止 | 累计身份容量组合策略、业务/渠道有效截止合成仍未实现 | LocalOwner READY 或业务 deadline 不关闭这些门禁 |
| 外部 IM E2E | 仍需真实 Worker/渠道凭据与账号 | 本地 HTTP/WS fixture 不等于真实机器人验收 |

CGR-35/36 已补代码，**本轮最终验收已通过**；CGR-37 与有效截止策略仍开放。原始发现见
[复审 §16–17](design-review.md#16-介绍归档与设计实现对照复审)，本轮范围见[§18](design-review.md#18-delivery-runtime-v1-实现与剩余生产门禁)
与 [Runtime V1](delivery-runtime-v1.md)。D0 按切片落实，不因接口或维护循环存在整体关闭。

## 1. 第一切片代码边界

```text
Control route event fixture → JetStream retained route stream
→ Routing trusted metadata / receipt / generation / replay checkpoint
→ Telegram authenticated HTTP → Admission
→ PostgreSQL Inbox + Admission + RunRequested Outbox + budget
→ Relay claim / publish acknowledgment / claim CAS
→ JetStream WorkQueue → integration subscriber
```

这里的 fixture 与 integration subscriber 是测试入口，不是已接通的生产 Control publisher
或真实 Worker。第一切片只接纳输入并传输 RunRequested，不执行 Agent，也不发送 IM 回复。
Telegram Adapter 使用第三方 SDK 的 models；HTTP 持久确认点由 Gateway 的标准库 net/http
Handler 控制。Control 既有 Gin 实现保持原样。真实 Bot API 的历史实验不作为本切片验收。

| Module / 资产 | 当前代码 | 尚未完成 |
| --- | --- | --- |
| Routing | Control route Schema/DTO、可信 Stream metadata、receipt/投影/连续 checkpoint、启动 Initialize、generation guard、持久连续积压 60 秒门禁 | 真实 Control publisher、快照/归档/受控重建、完整账户管理投影 |
| Admission | 既有 Telegram 真人私聊文字持久入站，群消息暂存 ignore；本轮实现新增企微 Adapter、同事务 owner guard、有界重试和准备失败后的 Receipt 复查 | callback 即时反馈/跨 Worker 命令、完整账户与会话契约 |
| Connection | 账户/lease/epoch、配置 revision、Supervisor、分级恢复、持久 replacement 隔离；本切片新增原 ReplyOrigin 的 ReserveFinal/一次性 Sender 与 drain | 真实 Control 账户来源/凭据 Owner、全局预算与真实 IM 恢复 |
| Delivery | Final/text、Acceptor、完成验证 Port、PG Ledger/A1/A2/Sender 保留；新增 Maintenance/可组合 Runner | 真实 Execution 验证拥有方、NATS Consumer/ACL/生产调度、凭据配置、Progress/媒体/额度及真实 IM E2E |
| 公开企微 Go 库 | P0 已有历史验证；本轮实现新增完整 callback body 的 BodyDigest，并由 Gateway 入站/Connection 装配 | 默认生产 Final Consumer/调度、真实账号验收；P1/P2 后续 |
| Bootstrap / deploy | 可选 SDK/Connection/入站；当前单镜像、0001–0007 与独立 Maintenance | 生产 Final Consumer/发送 Runner 与完整 Workload 集成；本轮验收见 §11 |

本轮实现源码入口在 `services/channel-gateway/internal/{routing,admission,connection,delivery,bootstrap,infra}`，
事件源文件在 `api/events/{control,execution}/v1`，生成 DTO 在 `gen/events/`。Connection 与 Delivery 均已有
Domain/Application/PostgreSQL 纵切；Delivery 默认生产事件入口尚未装配。Gateway 已直接装配 `platform/im/wecom`。
后续能力按真实用例实现，不为目录对称性创建占位包。

## 2. 第一切片采用的具体选择

下表记录现有代码选择，不等于 D0-01～D0-10 已整体冻结或验收：

| 设计项 | 当前代码选择 | 后续工作 |
| --- | --- | --- |
| D0-04 表与事务 | 独立 `channel_gateway` database / `gateway` role；当前 7 个顺序迁移（0005 Connection、0006 Delivery/ReplyOrigin、0007 Runtime 索引）与 SHA ledger；进程启动 advisory lock；Admission 使用 Routing 与企微 Connection 同事务只读 guard | 完整权限/迁移与跨 Workload 集成 |
| D0-05 路由同步 | 单 subject 完整保留历史；StreamInfo.Created 标识源实例；metadata sequence 驱动 receipt/投影/连续水位原子提交；同步 Initialize 后开放 Handler | Control publisher、历史满后的快照/归档、显式重建入口 |
| D0-06 NATS | Route 使用 Limits 历史；RunRequested 使用 WorkQueue；显式 topology reconcile，runtime 仅校验 | Worker durable consumer、ReplyIntent transport/ACL、完整离线窗口验收 |
| D0-06 授权部署 | permissions.yaml → nats-config → server.conf → Server 加载；runtime / control publisher / reconciler 三角色 | 后续 Worker 权限；第一切片真实 ACL 允许/拒绝已验收，见 §7；reconcile 不等于授权加载 |
| D0-08 准入预算 | Receipt lookup 与新接纳各 128 并发，10 秒用例期限；固定一分钟新 Inbox 10,000；pending Outbox 10,000、最老 10 分钟；事务检查/扣减 | 负载压测、租户公平、可配置预算 |
| D0-08 路由健康 | 来源观察年龄超过 5 分钟拒绝新 Run；连续已知积压满 60 秒亦拒绝，PG 保存 episode，完整追平才清除；Ready/Resolve/事务 guard 一致 | OTel/告警、生产阈值调优与端到端停用传播验收；不把观察/积压门禁当作发布 SLA |
| D0-10 去重 | 稳定 EventKey + SourceDigest；同键异文冲突；当前不自动清理 Inbox/Admission/Outbox/route receipts | 正文/墓碑分层保留、审计、归档与恢复工具 |
| 当前账户来源 | Telegram 静态 webhook 配置；本轮实现用独立企微 accounts.json 的五字段投影及获 lease 后环境引用解析；跨副本 revision 单调 | Control ChannelAccount owner、Provider 物理身份核验与注册管理；环境引用不等于生产凭据 Owner |

当前 RouteSnapshot 的 `generation` 是 `(provider, account_id)` 级单调路由序列，不是
Binding 自己的版本号；改绑/停用不重置。当前 `Resolve(provider, account)` 每账户只有一个
有效路由，按会话或 topic 的 selector 尚未实现。CGR-27 现由独立持久 apply-lag 门禁
处理：已知最高水位持续超过连续应用水位满 60 秒时阻断新 Run，来源观察成功不续期。
部分进展、重复观察和进程重启保留起点；完全追平后恢复，旧 Receipt 重放不受新 Run 门禁影响。

Routing 的锁序为 replay-state → account → projection；本轮实现 Admission 的顺序为
EventKey → 预算 →（仅企微新事件）Connection owner →（仅 admit-run）Routing guard。只有 admit-run 解析并验证运行路由，ignore /
interaction 不强行解析 Binding。当前账户 ID 来自静态配置，不能把这一配置检查说成完整
Control ChannelAccount 运行资格与物理 Bot 注册管理已经落地。

路由源删除/更换、Schema 错误、同代次不同内容或坏投影进入持久隔离并阻止新 Run；未绑定
可信 stream lineage 的投影拒绝使用。隔离清除、受控重放和快照管理命令尚未实现。

## 3. CGR-23～25 的实现状态

- **CGR-23：Final 首版已有实现。** Delivery 的 `PENDING → CLAIMED → CALLING`、
  RecoverExpiredClaims / RecoverStaleCalling 与原 Attempt 结果证据在独立账本中实现。
  Admission Outbox claim 仍是另一个传输状态机；当前默认仅独立维护，发送 Runner 尚未启用，见第 11 节。
- **CGR-24：已有代码，验证记录单独维护。** topology reconciliation 与 Server auth 配置加载
  是不同路径；配置生成不展开秘密。两条路径分别验收，合法 PubAck 不代替越权拒绝验证。
- **CGR-25：未完整实现。** 本轮实现 Connection 表已有 bot_id 唯一/永久身份的纵深约束；Control 的 `provider + scope + physical bot identity` 唯一有效
  账户映射、身份核验与 Gateway 冲突投影尚未落地。accounts.json 的 account_id 不重复
  不证明两个逻辑账户没有指向同一物理 Bot；当前不得以静态配置声称已解决该约束。

## 4. 当前接纳与恢复语义

- 重复 Webhook 先读 Receipt，再考虑 stopping/新事件预算；Binding 切换不生成第二个 Run。
- ignore / interaction 保存 Receipt，不发 RunRequested；callback 尚未执行
  `answerCallbackQuery` 或跨 Worker 取消命令。
- Outbox 发布先持久领取，网络在 PG 事务外；PubAck 后按有效 claim token/lease 标记
  published。重试保持 EventID/Payload，语义为 at-least-once，不依赖短时 NATS 去重永久防重。
- Run WorkQueue 的未来消费者须在自己的 Inbox + Run 事务提交后 ACK；ACK 可释放传输消息，
  但 Gateway 的 Admission / Outbox 不因 PubAck 删除。测试订阅者不是 Worker 的业务实现。
- Route Stream 为完整历史、DiscardNew；容量满后发布失败，不静默淘汰路由。归档、快照迁移
  与重建尚待实现，必须在持续生产运行验收前闭合。

## 5. 部署代码与验证边界

当前已有 base/local Compose 的 Gateway、NATS、`gateway-database` 与 `nats-reconcile`。
Gateway 一个镜像提供运行、probe、reconcile 和 nats-config 命令；前一个初始化作业只建库/
角色，业务 SQL 仍由 Gateway 进程启动迁移。后一个初始化作业只负责 Stream/Consumer 拓扑。
`server.conf` 保留密码环境引用，Server 启动加载授权配置；不会由 JetStream API 更新 ACL。
公开端口 8090 与管理端口 8091 分开，base 无 PG/NATS 宿主端口，local 只映射回环 HTTP。

代码中存在 Schema fixtures、单元测试、真实 PG 集成测试以及双副本 HTTP→PG→NATS 测试。
本轮实际执行状态统一记录在页首及审计记录；代码/测试文件存在不等于测试执行成功。
不把历史 Bot API 实验结果写成这次 Gateway 或完整 Agent E2E 已通过。

```sh
just gateway-build
just gateway-test
# 外部注入专用 GATEWAY_TEST_DATABASE_URL / GATEWAY_TEST_NATS_URL 后：
# 使用独立测试 broker，允许测试重建固定 Stream。
GATEWAY_TEST_ALLOW_NATS_RESET=1 just gateway-integration
```

未设置相应数据库/broker 变量时集成测试会 Skip，普通 go test 成功不证明真实 PG/NATS
通过。实际工作树整合、构建、race/集成、NATS 权限、镜像启动、探针与回滚分别记录证据。

## 6. 尚未完成的完整目标

1. Control ChannelAccount/Binding 管理、物理 Bot 唯一注册、可信凭据解析与真实 route publisher。
2. Worker 固定 Manifest 执行、Run/Attempt/Session、ReplyIntent 生成与执行授权。
3. Delivery 默认生产 Consumer/调度、真实执行授权读取、Progress/媒体与发送额度；Final 账本/分段/恢复已有模块实现。
4. 真实企微故障/接管与 Final 验收；Sender reservation 已有代码，
   Connection/企微入站本轮联合验收已完成，见 §9.5；公开企微 P0 历史 SDK 矩阵见第 8 节，不重复列作空白实现。
5. 真实新 Gateway 机器人收发、群组、callback、429、归档/隔离恢复、负载与可观测性；
   CGR-27 的持久门禁已实现，上一切片 observer 联合测试与镜像证据见第 8 节，本轮结果见 §9.5。
6. 全部生产 Workload 完成后才进入 Helm **FINAL-INTEGRATION**；当前不创建 Chart。

## 7. 2026-09-05 第一切片落地证据（历史验收）

- 实际新工作树新增 109 个文件、修改 19 个既有文件；HEAD / Git 索引保持不变，最新设计
  CGR-23/24/25 与 Helm FINAL-INTEGRATION 条款保留。
- Gateway/API/gen 共 13 个有测试包的 race 测试通过，87 个顶层测试通过；真实 PG 测试覆盖
  Routing 14 项、Admission 10 项及双副本 HTTP→PG→NATS 联合链路。主套件跳过的独立 ACL
  测试另在 Compose broker 上通过，包含合法 PubAck 与越权/错误凭据拒绝。
- 实际 Go 镜像以非 root UID 65532 启动，public/admin listener 隔离；livez/readyz=204，
  public readyz=404。数据库为 channel_gateway，role=gateway 非 superuser，3 个迁移落库。
- Gateway 正常停止退出码 0，无 OOM。测试容器生命周期及完整字面日志由审计包记录，
  不把一次健康检查当作持续在线或生产发布承诺。
- 配置拒绝重复/大小写别名字段；Migration/Routing rollback 有独立 5 秒 cleanup deadline；
  Run Stream 消息上限至少容纳完整 1 MiB wire contract，避免持久接纳后永久发布失败。
- 本机审计包：`artifacts/channel-gateway-ingress-20260905/VERIFICATION.txt`，包含原始哈希、
  源码包、差异及独立副本回滚证据。artifacts 为 local-only，不作为随 Git 交付的附件。

上述证据只证明第一入站切片；第 6 节的完整目标仍待继续交付。


## 8. 2026-09-05 后续实施：CGR-27 与公开企微 P0（已应用并验证）

### 8.1 当前代码与已完成的分项验证

- [Routing replay Store](../../../services/channel-gateway/internal/routing/adapter/outbound/postgres/replay.go)
  持久维护 `apply_lag_since`；[Domain](../../../services/channel-gateway/internal/routing/domain/replay.go)
  将连续已知积压门限固定为 60 秒，与来源观察年龄和启动完整性分开。
- [0004 迁移](../../../services/channel-gateway/migrations/0004_routing_apply_lag.sql)为既有积压回填
  升级时的 PG 时钟；已追平行保持 NULL，不改旧 0001–0003 文件或重置历史水位。
- [真实 PG 门禁回归](../../../services/channel-gateway/internal/routing/adapter/outbound/postgres/apply_lag_integration_test.go)
  已完成基线失败/修订通过的 61 秒红绿验证；新增 partial/restart/迟到观察/乱序/完整追平恢复/
  新 episode/事务 guard/并发推进断言也有实际源码，完整运行日志按最终审计收束。
- [旧库升级测试](../../../services/channel-gateway/migrations/upgrade_integration_test.go)已通过：
  覆盖已有三迁移的积压与已追平数据库、并发 Apply、旧 ledger 与水位保留、重复升级不续期。
- [双副本 HTTP 故障测试](../../../services/channel-gateway/internal/bootstrap/apply_lag_integration_test.go)
  已通过：Fetch 持续失败期间，实际 Consumer.Run 的 30 秒周期观察自动识别积压，
  测试不手动调用 ObserveSource；同测验证门禁、旧 Receipt、禁用追平及重新启用。
  独立运行耗时 32.39 秒，随后联合 race 套件重跑为 32.27 秒。
- 本轮 Gateway/API/gen 联合 race 已通过 13 个测试包、95 个顶层测试，1 个需特定 ACL
  环境的测试跳过；日志为本轮审计中的 `logs/modified-integration.log`。真实 PG 持续计时
  回归在该套件耗时 61.14 秒。此结果不替代 SDK 最终矩阵或实际工作树/镜像验收。

### 8.2 公开库存在不等于 Gateway 已接入企微

[公开 `platform/im/wecom` P0](../../../platform/im/wecom/README.md) 已有 Client、原生 DTO、
状态、关联 ACK、单次文字 Final 与本地真实 WebSocket 服务端测试。依赖固定为
`github.com/coder/websocket v1.8.15`，精确 API/配置只在库 README 与源码维护。
协议事实与 Go 策略分开记录在[核验说明](wecom-protocol-implementation-notes.md)。

SDK 全矩阵及实际工作树/镜像已核验，真实企微账号尚未验收。当前 Gateway 二进制未
import/运行企微库；Connection、Delivery、企微 Adapter、真实 Control publisher 与 Worker
仍未实现，现有 Compose 不增加企微账户启动路径或独立 Connector 单元。

本节分项结果不覆盖完整 Agent E2E，不改写第 7 节的三迁移历史证据。Helm 仍只进入全部
生产 Workload 完成后的 FINAL-INTEGRATION。本轮最终测试、应用与独立源码副本回滚证据见下节。


### 8.3 实际工作树最终验收与复核处置

- 本轮 35 个源文件变更应用于 `codex/channel-gateway` 工作树；main、HEAD 与 Git index
  保持原样，无提交/合并/推送。原有 422 文件作为完整回滚基线保存。
- 实际工作树全仓 `go test -count=1 ./...` 通过 37 个测试包；Gateway/API/gen/WeCom
  联合 `-race -count=1 -v` 通过 14 个测试包、119 个顶层测试，1 个专用 ACL 环境测试跳过。
  本轮没有把该跳过项说成重新通过；历史 ACL 证据仍单列在第 7 节。
- 公开 SDK 单独 `-race -count=10 -cover` 通过；24 个顶层测试及表驱动子测试，
  88.9% statement coverage。实际树日志 `actual-sdk-repeat.log` 耗时 5.756 秒。
- 首次实际联合 race 发现 HTTP 测试错误共享两个副本的 SDK Stream 可变缓存；生产 New
  每副本本就独立创建句柄。测试修正后双次回归通过，再跑最终完整联合套件通过。
  `actual-integration.log` 保留失败，不用先前偶然通过的 staging 日志覆盖。
- SDK 审查修复：旧取消 handler 误停新 dispatcher、非合作 handler 关闭误报成功、错误
  类型 ACK cmd、读取超限错误分类、业务 pending 挤掉心跳控制配额。Final/UNKNOWN 与
  迟到 ACK 规则有独立回归；具体字面 RED/GREEN 保留在公开库验证记录。
- 实际 Dockerfile + Compose 镜像：UID 65532；管理 livez/readyz=204、public readyz=404；
  数据库 channel_gateway / 非 superuser gateway；4 个迁移落库；停止 exit 0、无 OOM。
  镜像成功不等于企微已接线，当前二进制仍未 import 公开企微库。
- NATS 初次启动失败属于生成测试密码的环境变量解析边界；失败日志保留。当前实测镜像
  使用字母前缀的独立随机测试密码，具体配置审查结论见复审记录。未修改用户凭据。
- 本地审计：`artifacts/channel-gateway-routing-wecom-20260905/VERIFICATION.txt`。
  原始哈希、修改包、diff、精确命令与退出状态、独立源码副本回滚及逐字节 patch 重放均在
  此处核验。源码回滚不执行持久数据库降级；测试数据库/容器单独清理。

本切片验收不关闭整个实施目标：Connection、Delivery、企微 Adapter、真实 Control
publisher/Worker 与真实 IM E2E 仍待继续实现。Helm 保持 FINAL-INTEGRATION。


## 9. Connection 与企微持久入站：已应用并验收

### 9.1 交付层级与源码入口

**本轮状态：已应用到实际工作树，完整联合套件及新镜像验收通过。**
本节不改写第 7、8 节历史证据；测试 WS、样本 Control 事件与测试订阅者均不是外部 IM、
真实 Control publisher 或 Worker。当前新增输入链路如下：

```text
部署侧五字段账户投影 → Connection ApplyAndAcquire → 获得 owner grant
→ 解析环境凭据引用 → bootstrap 直接装配公开 WeCom SDK + 入站 Adapter
→ callback 归一化/有界重试 → Admission 旧 Receipt 或同事务 owner guard
→ Inbox + Decision + Admission/RunRequested Outbox → NATS
```

- [Connection Domain](../../../services/channel-gateway/internal/connection/domain/account.go)、
  [PostgreSQL Store](../../../services/channel-gateway/internal/connection/adapter/outbound/postgres/store.go)
  与 [0005](../../../services/channel-gateway/migrations/0005_connection.sql)拥有非秘密账户投影及持久租约。
- [Supervisor](../../../services/channel-gateway/internal/connection/application/supervisor.go)拥有每账户
  Client 生命周期；[恢复策略](../../../services/channel-gateway/internal/connection/application/recovery.go)
  与 SDK 传输重连分开。[企微入站](../../../services/channel-gateway/internal/admission/adapter/inbound/wecomadapter/handler.go)
  负责外部消息到 Admission 的转换；Bootstrap 只显式装配，不编排租约/重试状态机。
- [Admission Store](../../../services/channel-gateway/internal/admission/adapter/outbound/postgres/store.go)
  在自己的事务调用 `VerifyOwner`；[Application](../../../services/channel-gateway/internal/admission/application/service.go)
  补准备阶段失败后的最终 Receipt 查询。SDK 类型不进入 Domain/Application。

### 9.2 账户、revision、租约与隔离

当前来源是部署侧文件，不是 Control ChannelAccount API。进程读取
`GATEWAY_WECOM_ACCOUNTS_FILE`；五个字段都必需：`account_id`、`bot_id`、`revision`、
`enabled`、`secret_env`。拒绝未知/重复字段、非正整数 revision、重复账户/Bot 和无效引用。
账户 ID 与 Bot 绑定后不更换；数据库 bot_id 唯一，永久保留账户行，不以删除重建绕过 epoch。
这只提供本地配置/租约纵深约束，未实现 Provider 身份核验或 Control 注册管理。

- 配置 revision 在所有副本之间单调；同代次同内容幂等、异内容冲突，旧 revision 拒绝。
  更高 revision 即使暂被旧 lease 挡住而返回 `ErrHeld`，仍持久提交配置；旧 grant 立即失去
  Check/Renew/新接纳资格。新 owner 仍等待旧 lease 释放或过期，不重叠授予有效本地租约。
- 每次新 claim 都递增 epoch，包括相同 instance；`Release` 匹配 instance + epoch，允许旧
  revision 的 Client 关闭后释放自己，不得释放新 owner。进程重启不重置数据库 epoch。
- PG `clock_timestamp()` 在获账户行锁后采样；ObservedAt 与 LeaseUntil 同源。Store TTL
  接受 100ms–10min；Supervisor 默认 TTL 15s、账户轮询 1s、操作截止 2s、drain 5s。
  这些是当前代码默认，不是吞吐或外部连接绝不重叠的承诺，也尚未全部暴露环境变量配置。
- `replaced` 按原 owner/epoch/revision CAS 写 `blocked_revision`，清除该 lease；相同
  revision 的其他实例/重启不得重新 claim，只有可信更高配置 revision 解除隔离。
  旧通知不得封禁新 revision 或新 epoch。
- replacement 持久记录不以 Client.Close 成功为前提。Status 用 `IsolationPersisted` /
  `IsolationError` 分别报告隔离成功或失败，未尝试均为 false；Close 失败不覆盖隔离结果。
  隔离失败不 Release 来加速接管；数据库不可写时不声称全副本封禁已经成立。

凭据仅在取得 lease 后按 `secret_env` 解析；真实值不进账户行、事件或日志。环境值不能靠
修改宿主机 shell 热更新现有进程。轮换须提前注入新引用，再发布更高 revision 指向它；
若值没有预先注入，则显式重建/重启进程并注入新环境。操作示例见[Compose 说明](../../../deploy/compose/README.md#企业微信账户配置已实现)。

### 9.3 入站身份与两层有界恢复

`Event.BodyDigest` 覆盖完整已解码 callback body，含未知 body 字段；拒绝重复 key，保留
数字精度，外层 req_id 与 socket generation 不进入摘要。Admission Adapter 在此基础上
固定语义版本/可信 Bot 与归一化字段构成 SourceDigest。EventKey 仍是 provider + stable
account_id + msgid；owner epoch、配置 revision、req_id 与 socket generation 不进入去重键。
首次 ReplyContext 保留第一次 callback 的相关 ID；跨 owner 重放不覆盖它，也不证明新
socket 可以继续旧 req_id 的回复。

新企微事件在同一个接纳事务校验账户、instance、epoch、配置 revision 和 PG 租约时间，
锁保持至提交；ignore/interaction 同样适用。旧 Receipt 在 guard/预算前重放，Telegram
完全绕过 Connection。新 Run 另有 Routing guard；Connection revision 不替代 RouteGeneration。
CGR-33 的准备失败窗口使用剩余期限内一次新快照 Receipt 查询，同摘要重放、异摘要冲突，
无记录保留原错误；不等待尚未提交的并发请求，不以缓存掩盖 PG 失败。

| 层 | 当前本轮实现默认策略 | 不代表什么 |
| --- | --- | --- |
| Admission Adapter | 同一已归一化输入最多 6 次，总期限 2s，退避 100/200/400ms，后续封顶 400ms；确定拒绝不重试 | 不重建 EventKey/摘要/ReceivedAt/ReplyContext，不承诺断线补发 |
| 组合 Adapter → Supervisor | 临时接纳耗尽或入站队列 overflow 映射 `ClientStatus.Retryable`；认证/协议/replaced 不映射 | 不要求公开 SDK 理解平台业务错误 |
| Supervisor | 最多 3 次快速重建，默认 1/2/4s；用尽后每 60s 一次半开尝试；短暂 Ready 不清预算 | 预算限本进程/账户/revision，不是跨副本或跨重启的全局速率限制 |
| SDK | 单 Client Run 内已有有限传输重连；handler 错误结束该 Run | 不无限创建新 Client 绕过认证/协议/重连预算终态 |

配置源暂不可用/暂时移除账户只暂停 acquire，不清除已经分类的重试预算；重新出现的相同
revision 不绕过已建立的退避。新配置 revision 或新进程开启新的预算。单个 Bot 认证失败、replaced、
暂未 ready 或被其他实例持有，不撤掉整个 Gateway 的共享 readiness；Supervisor 主循环/
完整配置源健康、PG、Routing 与 Admission 预算仍参与共享探针。账户细节当前由本地 Status
Port 表达，未新增对外账户状态 API、metrics 或告警服务。

### 9.4 当前分项证据与未交付项

本轮实现的 Connection Store / Admission owner guard 真实 PG race 分项，以及
[0004→0005 升级测试](../../../services/channel-gateway/migrations/connection_upgrade_integration_test.go)
已通过：两个独立 pool / 8 个并发迁移者，保留旧 9 张事实表、SHA ledger、水位与积压起点；
重复迁移不清隔离；注入 CREATE TABLE 后的 ledger 写失败会原子回滚新表/ledger，解除故障后
可重新升级。上述分项结果本身不替代联合验收；本轮完整套件、实际工作树与镜像验证
另已完成，结果集中于 §9.5。

CGR-29、CGR-31、CGR-33 已在本轮实现；CGR-30 采用上面的有界首版策略。
实际应用及联合结果见下节；这是本地入站切片验收，不等于真实 IM/Worker/Final 生产验收。

仍未完成：Delivery/ReplyIntent/CLAIMED/CALLING/UNKNOWN Observation（CGR-32）、Connection
供 Delivery 使用的 SessionRegistry/Sender lookup、真实 Control 发布与账户/凭据 Owner、
Worker 固定 Manifest 执行、Telegram 生产出站、企微 Agent Final 与真实 IM E2E。CGR-28 的
bundled NATS 密码预检仍开放。Helm 继续仅属全部生产 Workload 完成后的 FINAL-INTEGRATION。


### 9.5 实际工作树联合验收

本轮 54 个源文件变更（35 个新增）已应用于 `codex/channel-gateway`；最终源码共 472 个文件。
HEAD/main/Git index 未移动，上一轮说明与 review §1–12 的历史正文保留。

- 实际工作树全量 `go test ./...`：42 个测试包通过，exit 0；Control PG 集成仍取决于其
  专用测试环境，不能把普通全量测试当成 Control 真实数据库验收。`go vet ./...` exit 0。
- Gateway/API/gen/公开 SDK 的真实 PG/NATS/WS `-race -count=1 -json`：19 个测试包、
  208 个顶层测试通过，1 个需专用授权 broker 的 ACL 测试在该套件跳过；无失败，exit 0。
- 上述跳过的 ACL 已在独立授权 NATS 2.11.8 上另行运行：实际 CLI reconcile 与
  `TestBrokerPermissionsIntegration -race -count=1` 均 exit 0，验证合法发布/PubAck、
  topology 只读、管理 reconcile、错凭据拒绝与 Gateway 越权发布/修改拓扑拒绝；无 skip。
- 双 Gateway 真实 WS/PG/NATS 流程另重复 3 次通过：同 Bot 只有一个 owner socket，
  退出后更高 epoch 接管，req_id/owner 变化不重复接纳，chatless notice 不创建 Run；
  短暂路由故障保留同一 callback，预算耗尽后同配置新 Client 恢复接收，过期未接纳
  callback 不伪造 Receipt；验证持久 replaced、凭据轮换、停用/旧配置/重新启用。
- Connection Application 33 个顶层测试 `-race -count=5` 通过，覆盖正常 drain 续租、
  本地租约截止、source 故障、冷却/半开及隔离与 Close 失败；此端口套件不冒充真实 PG。
- 接线复核捕获并修复两种分类竞态：普通 Terminal 只在最终 Run 结果分类后原子发布；
  临时 handler 候选不得污染后来的协议/auth/drain 终态。replaced 仍即时优先，overflow
  进入 Supervisor 的同一有界恢复预算。真实 WS 回归与 SDK/managed/Supervisor 组合通过。

实际 Go 镜像 `trpc-agent-service/channel-gateway:connection-20260905` 构建及 Compose 启动
exit 0，镜像 ID 为 `sha256:966bd350755d36f7821cd9994287103b14662c2312cd7f56c69198b2e973d3d5`。
镜像采用用户 65532；Gateway 以非 superuser 角色访问独立 database，迁移数为 5。
管理 `/livez` 和 `/readyz` 均 204，公开 listener 的 `/readyz` 为 404；临时配置目录中
atomic rename 写入无效账户源后 readyz=503/livez=204，恢复空数组后 readyz=204。
正常停止 exit 0、OOMKilled=false，实测约 1.01s。镜像空账户 fixture 不连接真实企微。

可重跑命令、字面输出、源文件哈希、diff 和独立源码副本回滚结果集中在本机审计目录
`artifacts/channel-gateway-connection-20260905`。该目录是 local-only evidence，不充当随
Git 提交的附件；保留基线与完整日志，源码回滚不自动执行数据库 down migration。

## 10. Delivery Final V1：源码纵切与剩余运行接线

### 10.1 固定的实现范围

本切片按[Final V1 规格](delivery-final-v1.md)新增：

- 闭合的 ReplyIntent Final/text Schema、生成 DTO、严格 Codec、完整业务摘要与 fixtures；
  Worker 不提供可覆盖原目标的 account/chat/tenant。Progress、媒体、编辑不在当前 wire 中。
- Delivery Acceptor 从原 Admission 读取只读快照，逐项匹配 CommittedFinalVerifier 的不可变
  已提交完成证明；无默认许可。相同 Intent 同摘要重放不依赖当前路由、活跃 lease 或验证方在线。
- 单 Run 唯一 Final 屏障及不可变完整分段计划；Telegram 顺序发送，前段 UNKNOWN 阻止后段，
  不重发已 ACCEPTED 前段；企微 P0 单次 Final，超限不截断、不伪分段。
- PG A1 CLAIMED / A2 CALLING 两道持久状态，随机 evidence capability、attempt CAS 与企微
  owner guard；A2 提交不确定时不调用 Provider。准备失败与真实发送分别最多三次。
- 独立 Observation 保存原调用晚到证据；ResolveObserved 显式收束一致明确结果，不触发发送。
- 0006 为 Admission 单独保存原 ReplyOrigin；旧行 NULL 不猜当前连接，跨 owner 重放不覆盖。
  Connection ReserveFinal 精确匹配原 owner/config/socket，句柄一次性且计入有界 drain。
- Telegram 直接 SDK、本地 HTTP 验证；企微经 Connection/公开 SDK、本地真实 WS 验证。
  两种 Adapter 都检查 A2 身份与原请求，不将错误/未知回执提升为成功。

0001–0005 文件保持原字节；0006 由既有迁移入口执行。Go 依赖、部署单元与 Compose 服务
拓扑不变；Gateway 仍单进程/镜像，Helm 仍在全部生产 Workload 完成后的 FINAL-INTEGRATION。

### 10.2 生产装配尚未完成

默认 App.New 仍运行 Routing、Admission、Relay、Connection 与入站 Handler；未启动
ReplyIntent NATS Consumer 或 Delivery 调度循环。当前 bootstrap bridge 是可组合能力，
集成测试显式注入 committed Final fixture；它不是已实现的 Execution Owner/Worker。

后续必须同时交付真实不可变完成事实/授权读取、Worker Final Outbox、ReplyIntent topology /
ACL/consumer/隔离/重投、调度生命周期、Telegram 可信凭据与有界客户端配置，然后才启用
默认生产入口。这里没有新增宽松 verifier、公开测试授权路由或第二个 Connector Workload。

### 10.3 已有分项证据与本轮修正

此前修改副本的真实 PG/NATS race 为 24 个测试包、283 个顶层测试通过；其中 broker ACL
专项因未提供专用授权环境而跳过，不能称其在该命令内通过。Telegram 与企微 Full Final
测试使用本机 HTTP/WS 和显式完成事实 fixture，不是外部机器人或真实 Worker E2E。

本轮另补 eventadapter 的严格事件到 Application seam 回归，并修正两项已复现的异常返回：
非 nil reservation + error 也 Release；nil,nil 消耗有界准备预算。Connection 的 ProviderCode
在 bridge 中保留，矛盾的接受/未发送结果归 UNKNOWN/permanent，不得到成功或普通重试资格。
红绿、当前源码联合回归和实际镜像证据按第 10.4 节记录，历史成功不替代新的回归。

### 10.4 最终联合验收

78 个文件变更（56 个新增）已应用到 `codex/channel-gateway`，源码清单共 528 个文件；
Git HEAD/main/索引未变。旧 0001–0005 和设计复审 §1–14 正文保留，本切片新增 0006。

- 实际工作树全量 `go test ./...`：48 个测试包通过；`go vet ./...` 无诊断。
- 实际工作树 Gateway/API/gen/SDK `-race -count=1 -json`：25 个测试包、293 个顶层测试通过，
  无失败；包含真实 PG/NATS 与 Telegram HTTP、企微 WS 的 Final 纵切。该命令跳过独立
  `TestBrokerPermissionsIntegration`，另在下述带认证 Compose 环境运行通过。
- 事件生成的 3 个 DTO 文件重生成字节不变；gofmt 和 diff whitespace 无诊断。
- 从实际工作树构建 `trpc-agent-service/channel-gateway:delivery-20260905`，镜像 ID 为
  `sha256:7a7a4688e3ac01f2adfe8dad05c2f1ebfca4b6a6802a9b30a9b88398e9bd2c6c`。
- 独立 Compose 空账户验收：管理 livez/readyz 均 204，public readyz 404；UID 65532；
  `channel_gateway` 数据库的 gateway 非超级用户、迁移数 6；原子替换无效账户文件后
  readyz 503/livez 204，恢复空数组后 readyz 204；正常停止 exit 0、OOMKilled false、1.07 秒。
- 带认证 broker 的独立 ACL 测试通过：允许运行方只读拓扑和合法 RunRequested PubAck、
  reconciler 幂等管理；拒绝 Gateway 发布 Control 事件、修改拓扑及错误凭据。
  这是既有事件族权限回归，不宣称尚未部署的 ReplyIntent ACL 已完成。

最终差异包、原始哈希、精确命令/输出、独立源码副本回退结果集中保存在本地
`artifacts/channel-gateway-delivery-20260905/VERIFICATION.txt`；此本地证据目录被 Git 忽略。
数据库不执行向下迁移，回退脚本只操作显式指定的源码目录。当前生产 Final 入口、真实
Execution 验证与外部 IM E2E 仍按 §10.2 继续，不因这组本地验收而标记完整目标完成。

## 11. Delivery Runtime V1：实现与本轮验收

### 11.1 当前范围与默认装配

当前新增 [Runtime V1](delivery-runtime-v1.md)，接续 §10 的 Final/ReplyOrigin 账本，不重新定义
其 wire 或执行授权。源码提供：

- Delivery Maintainer：CLAIMED/CALLING 恢复、ownerless pending expiry 和有界观察候选恢复；
  独立失败不阻塞后续步骤，游标跨轮保留，不删除事实或释放总行数容量。
- Delivery Runner：provider/账户 keyset、有限 worker、每账户一个 dispatch、每次 claim
  一个 part、eligibility 检查、Quiesce/Drain。默认生产发送仍未启用。
- PostgreSQL RuntimePorts 与 0007 五个查询索引；严格页界限、C 排序、5 秒独立查询截止、
  锁后 PG clock、SKIP LOCKED/CAS；旧 0001–0006 SQL 不改写。
- Connection.LocalOwner：核验真实本地 lease/config/Client/context/期限后返回 grant 拷贝；
  不取得租约、不从 Status/ReplyOrigin 拼资格。A1/A2 guard 和原 Sender 匹配继续执行。
- CGR-36：确定坏 ReplyIntent wire 才映射 ErrInvalid，内部 Decode 故障保留 ErrUnavailable。

默认 `App.New/Run` **只新增独立 Maintenance**，无账户、无 owner、无 Sender 的副本也运行。
没有默认 ReplyIntent Consumer、发送 Runner、Telegram Client、Final Acceptor 或 permissive
verifier。测试中的自动 Runner/完成事实/Sender 是显式 `_test.go` 组合，不进入默认生产装配。

### 11.2 问题状态与未交付项

| 项目 | 本轮源码处理 | 仍需区分 |
| --- | --- | --- |
| CGR-35 | 独立 ExpirePending、PG 发现、Maintainer/Runner、LocalOwner 与默认空账户维护 | 实现已有，本轮最终验收已通过；生产发送装配及负载公平性另行交付 |
| CGR-36 | eventadapter 按明确 typed wire 错误分类 | 修复已有，本轮最终验收已通过；没有新建 Reply Consumer/永久拒绝账本/ACK |
| CGR-37 | 保留既有库/Adapter 行为 | 累计身份容量的 typed 区分、账户可用性、准入/告警与有界恢复仍未实现 |
| 有效发送截止 | 仍使用已授权 Intent.deadline | Final V1 §3.1 的业务/渠道窗口/驻留期限合成和时间来源策略未实现 |
| 完整 Final 通路 | 既有 Acceptor/Sender 可组合；默认只维护 | 可信 Execution/Worker/Outbox、Reply topology/ACL/transport、凭据与发送 Runner 装配、真实 IM E2E |

保留/GC、生产配额、OTel、跨重启全局恢复预算和 Control ChannelAccount 管理继续开放。
没有新增部署单元。Helm 仍仅在全部生产 Workload 完成后的 FINAL-INTEGRATION 交付。

### 11.3 本轮验证口径

本轮独立复核还修复默认 4 worker / 4 页的 Provider 饥饿：起始 Provider 改为每 tick 独立
轮换。公开 Interface 的 `TestRunnerDefaultPoolSharesCapacityAcrossContinuouslyDueProviders`
持续保留两个 Provider 的 due 工作，验证两者均获槽位而非仅偶尔被遍历；根代理记录的
分项为旧实现 count=3 RED、修正后 count=5 GREEN。本轮联合复验仍以下节为准。

需要分别记录：Application 与 LocalOwner 的 Port/race 测试，真实 PG runtime/竞争/升级与
EXPLAIN，默认空账户 App + PG/NATS 维护，显式 Runner + PG/Telegram HTTP/企微 WS 组合，
实际工作树全量测试/vet/生成一致性、七迁移镜像/Compose 与源码副本回退。

既有测试源码入口见 Runtime V1 §9。普通 `go test` 在未配置真实 PG/NATS 时可能 Skip，
不能据包通过声称数据库或 Broker 已验收；本地 HTTP/WS 仍不等于外部机器人或真实 Worker。
第 7–10 节的旧包数、迁移数、镜像 ID 和命令结果逐字保留，不作为本次新增源码成功证明。

### 11.4 最终联合验收

2026-09-05，37 个文件变更（19 个新增）已应用到实际 `codex/channel-gateway` 工作树，
源码清单共 547 个文件；HEAD=`bf107766`、main=`ab315e4` 与索引保持原值。旧 0001–0006、
设计复审 §1–17、实施状态 §7–10 正文保持，不将历史成功替代本次验收。

- 修改前完整源码副本 `go test -count=1 ./...`：48 个测试包通过。
- 实际工作树最终 `go test -count=1 ./...`：48 个测试包通过；`go vet ./...` 无诊断。
- 实际工作树 Gateway/API/gen/公开企微库 `-race -count=1 -v`：25 个测试包、339 个顶层
  测试通过。唯一跳过的是专用认证环境 `TestBrokerPermissionsIntegration`，随后在隔离
  Compose 网络中单独通过；既有 Route/Run ACL 的回归不表示 Reply ACL 已交付。
- 实际联合覆盖真实 PG/NATS、默认两个 App 实例的无账户维护、显式 Runner 的 Telegram
  HTTP 分段/UNKNOWN 屏障、企微本地 WS 原 ReplyOrigin 与当前 owner；这是本地协议 fixture，
  不等于真实外部 IM 或实际 Worker。
- 新增持续积压公平性回归在旧 Runner 上三次失败，独立轮转首 Provider 后通过；根代理
  复验该测试 `-race -count=5` 通过，最终联合套件再次通过。每账户单在途/worker 上限保持。
- 真实 PG 用例涵盖 expiry/claim 竞争、锁后时钟、无人 owner/前段 UNKNOWN、分页与超时、
  0006→0007 并发升级和故障 DDL 回滚，以及稀疏/稠密 EXPLAIN；不据 fixture 计划保证生产容量。
- 三份事件生成 DTO 重生成字节不变；21 个变更 Go 文件 gofmt 无输出；差异 whitespace
  检查无诊断（`git diff --no-index --check` exit 1 表示目录存在差异，原日志保留）。

实际工作树构建镜像 `trpc-agent-service/channel-gateway:runtime-r18-20260905`：

```text
sha256:32a00e43befee9390646e1a5242c0900b365e39ec4bd7eec5df12f1313472d49
```

独立 Compose 验收结果：管理 livez/readyz 204、公开 readyz 404、UID 65532；数据库为
channel_gateway / gateway 非超级用户，且无 createdb/createrole；全部 7 个迁移名称及
SHA-256 匹配。默认循环把已持久的合成企微 PENDING 变为 EXPIRED，owner/attempt/observation
均为零，原 Receipt/digest/target/body 保留。账户文件原子换成非法值后 readyz 503/livez 204，
恢复空数组后 readyz 204；正常停止 exit 0、OOMKilled false，耗时 1.24 秒。

NATS 专项允许 Gateway 合法 RunRequested PubAck、只读拓扑和 reconciler 幂等管理，拒绝
Gateway 发布 Control 事件、修改拓扑与错误凭据。专用 Compose 的 5 个容器、3 个独占卷
已清理，新镜像与验收证据保留；用户其他运行服务保持不变。

实际路径第一次全仓检查发现审计目录内的四份原始 Go 副本被递归发现，触发 internal
导入错误；不是业务用例失败。原件已保留在校验过的压缩包，移除这四份松散审计副本后，
重新执行完整 Go/race/vet 并通过。首轮失败日志保留，不以副本绿灯替代实际路径回归。

独立源码副本回退恢复原 528 文件、移除本轮 19 个新增文件，回退后全仓 48 个测试包通过。
回退只恢复源码，不执行数据库 down migration；实际工作树保持本轮实现。最终文档回填后
再核验差异重放、完整源哈希和最终包回退，精确命令/输出、原始哈希与四项工件集中在本地
`artifacts/channel-gateway-delivery-runtime-r18-20260905/VERIFICATION.txt`；该审计目录由 Git 忽略，
不是随仓库分发的运行依赖。

CGR-35/36 的本切片实现与上述验收已完成；生产发送装配、CGR-37、有效发送截止合成、
真实 Execution/Control/IM E2E、保留/GC 与观测等继续按 §11.2 推进，不宣布完整 Gateway 完成。


## 12. 2026-09-06 Control 接入 GCI 基础切片

用户恢复正式 Control 接入开发；Gateway 在自己的 `codex/channel-gateway` 工作树实现。
Control 账户/Binding拥有方仍由 ChannelBinding 任务负责。此切片不是整体验收完成。

### 12.1 已落地的代码

- 原样同步 Control 拥有的 `api/schemas/channel/v1` 30个契约文件；未修改原 RouteProjection。
- `connection/domain/accountcatalog`：非秘密账户、JCS 摘要、身份/用途/版本/路由下限、
  快照排序、SUPERSEDED与回退分类、进程内可撤销Permit。
- `connection/adapter/outbound/controlhttp`：真实双向TLS、关闭Schema、完整快照与精确凭据
  响应核对；拒绝重定向/压缩/超限，错误不带响应正文或URL，完整操作最多5秒。
- `connection/adapter/outbound/catalogpostgres`与0008：非秘密目录、水位、冲突隔离、
  90秒receipt、累计身份保留、实例资格与account→instance同事务Guard/到期复查。
- `connection/application/catalogrefresh`：单在途刷新、原子交付、独立新鲜度截止、
  故障先撤本地上下文、metadata/floor更新不换Client上下文、只向旧Supervisor投影企微。
- 旧Supervisor新增独立5秒CredentialResolveTimeout、取值前owner复核；源错误、配置变化、
  失租、Shutdown会取消凭据请求。超时后返回的成功值也不构造Client；租约操作仍默认2秒。

### 12.2 本切片验证与明确剩余

分项验证包含真实双向TLS fixture、真实PostgreSQL目录事务竞争与失效检查，及3秒凭据响应/
失租取消。Gateway真实PG race回归已执行；具体命令和字面输出见本轮本地审计
`artifacts/channel-gateway-control-gci1-20260906/VERIFICATION.txt`。
测试资源是独立PG容器；未接真实Control进程或外部Telegram，未配置NATS的集成项仍会Skip。
既有PG屏障测试验证的是新目录Guard本身，不能称已覆盖尚未注入的Admission/A2调用路径。

默认App仍使用原文件/env账户配置，没有默认启用本次Source/凭据Adapter。尚待继续：
生产配置装配、带真实资格/owner的凭据bridge、Admission/A1/A2 guard注入、注册fence/
BEGIN_CALL/协调器、Telegram动态Handler/客户端、观测、Compose和双侧真实Telegram入站。
Delivery Consumer/默认发送等既有缺口未由这个基础切片代为完成；真实Worker仍单独验收。
本次未提交、推送或合并main；当前原始基线及独立副本回滚证据保留在审计包。


## 13. GCI2：Control 生产运行接线（2026-09-06）

本轮仅在 `codex/channel-gateway` 独立工作树修改，GCI1 原始哈希已留存。共享 HTTP/route
Schema 不变；0001–0008 不变。生产 `LoadConfig` 默认 Control，fixture 文件/env 来源仅
显式开发启用；Compose 增加 mTLS 文件引用和 scope/source/public-origin 配置。

### 13.1 已接入真实调用路径

- `bootstrap.App`: Control transport + 目录刷新 + 可信 AccountUse bridge，readyz 检查来源
  和当前版本注册；WeCom Supervisor 与 Telegram reconciler 独立，不增加 Node 单元。
- `accountuse`: 本地可撤销精确版本 Permit；企微拿 Supervisor 原始 OwnerGrant，前后复核，
  独立凭据 5 秒与 owner 2 秒预算。旧客户端上下文随源错误/停用/轮换撤销。
- Admission：Receipt-first，新工作 account→instance→Inbox key→budget→owner→route；
  tenant/floor 同事务复核，资格失败仅只读复查并发 Receipt，不重试不确定提交。
- Delivery：A1 将 use/client fingerprint 写 Claim；A2 再次资格/原 Claim/摘要检查后生成
  CALLING。实际 Sender 在调用前检查原 Permit；Finish/Observe/Maintenance 不新增准入 guard。
  Control 模式启动原有有界 Runner，尚未接 ReplyIntent NATS Consumer/真实 Final verifier。
- Telegram：独立持久注册 lease 25 秒，BEGIN_CALL 单次许可，同版批量 token+secret，
  GetMe 核实 bot ID；immutable webhook Handler 换代。正常远端 READY 60 秒重申，失败
  5 秒退避；每进程 8 workers。未知/迟到操作保留原版本事实，不将旧 ACK 升格为新版本 READY。
- `accountobservations` 用例：100 项/128KiB 上报，状态变化和 30 秒心跳，独立序列与
  有界关闭上报。bootstrap 只翻译读取到的领域状态，未把观测当成发送授权。

### 13.2 运行验证

本轮专用资源：`gateway-control-gci1-pg`（127.0.0.1:32772）与新建的
`gateway-control-gci2-nats`（127.0.0.1:32774）；测试在独立 PG schemas 运行，NATS
显式允许重建测试流，不使用其它任务的 broker。

已验证：真实 mTLS/PG/NATS 完整 App 纵切（远端 Telegram 为显式 fixture），注册先于路由、
动态 Secret 轮换拒绝旧值、Webhook 持久入站和 Outbox→RunRequested、来源失败、观测封装；
真实 PG 的 Admission tenant/floor/停用与 Receipt replay、A1 绑定与 A2 换代拒绝、停用后
证据入账、注册 BEGIN_CALL 单次和迟到原操作事实；owner 解析前后失效与取消。

全部精确命令、输入范围、字面输出、退出状态、源哈希与独立副本回滚位于
`artifacts/channel-gateway-control-gci2-20260906/VERIFICATION.txt`。本轮新增列使旧升级测试
必须比较原 0006 字段集合；不是删除历史事实断言，原业务值与迁移 hash/timestamp 仍核验。

### 13.3 联合入站验收已完成；执行边界保持独立

实际 Control mTLS、Gateway 进程、公开 HTTPS、真实 Telegram 账户与 Binding 已接入；
当前版本注册 READY 后发送的真实私聊文本已形成唯一持久 RunRequested，详见第14节。
目标是固定真实 Published DeploymentRevision/Manifest 身份的入站→RunRequested，不假造
TargetReady，不用测试 committed-Final 代替真实 Worker。Gateway 当前不下载 Manifest
正文；真实发布目标真实性由 Control 发布事务与受认证 Relay 提供，Worker 执行时另核验内容。

启动命令与配置清单见 `services/channel-gateway/CONTROL_RUNTIME.md`。
本轮不提交、推送或合并 main；Helm 等全部 workload 完成后处理。


## 14. GCI3：真实 Telegram 收信及当前文档对齐（2026-09-06）

**真实入站 PASS**：update_id=`309271229`、message_id=`6`；Receipt=admit-run，
admission/event_id=`c00f77d6b3a94dcd98b77ccf071b047b`，
run_id=`45ccc1ec2d4b600a656ed0b489497e5c`。Inbox/Admission/Outbox/published Outbox
分别1条；NATS `RUN_REQUESTS_V1` sequence1、同事件1条，Schema与PG规范payload一致，
固定 dpr/rmf/digest 与实际 Control Binding generation2 完全相同。DeliveryIntent为0。

原始 PubAck 帧未另外存储；确认依据是实际 published_at、收到合法 PubAck 后才落该字段的
源码路径及可读取的 NATS FileStorage 消息。读取不消费/ACK/重发消息。脱敏报告见
[真实入站验收](telegram-real-inbound-20260906.md)。

本轮同步接入设计、Module §15、总览、运行说明和复审最新入口中的过期现状，不修改业务
代码/共享Schema/迁移，不改写历史分轮测试。模型/Storage 是 admission-only fixture，
Worker、Manifest正文执行和完整回复仍独立验收；没有伪造机器人回复或 Worker 完成。

## 15. Control / Gateway 提交集成（2026-09-06）

本次集成以 Control `239e8ad` 为基，保留 Gateway 功能提交 `9190241`；两个实现位于同一
仓库，不再通过本机跨工作树链接引用协议。共享契约原样保留；Manifest 发布事件与渠道路由
事件的 Schema 测试分别保留。依赖统一为根 Go module；Control / Web 与 Gateway 的 just
命令同时保留。主工作树既有脏改动与分叉本地 main 不参与本次集成。

生产 Control 模式由单 Runner 独占 Maintenance 生命周期，fixture 才独立维护；当前为
10 个 Gateway 迁移。真实 Telegram 入站证据保留第14节及脱敏 JSON；本次集成测试不是
第二次发送真实消息，也不消费或清理其 RunRequested。ReplyIntent Consumer、真实 Worker
与完整回复仍保持独立交付边界，Helm 留待全部 Workload 完成后的 FINAL-INTEGRATION。

提交前 Gateway 全仓 race（两侧专用 PG、NATS/ACL）54 包、1499 测试、0 skip / 0 fail。
最终集成提交及合并后的回归状态以 Git 历史和本次提交任务的验证记录为准；第7–14节不
回写历史命令、哈希或实验结果。
