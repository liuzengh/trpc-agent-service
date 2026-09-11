# ChannelBinding V1：精确部署目标、有效路由与发布事务

- **设计状态**：2026-09-06 联合评审修订已落地；本文记录当前协议与实现边界。
- **实现状态**：Binding Domain、Application、真实 PostgreSQL/HTTP、路由 Outbox/受限 JetStream Producer 已接通并通过回归；真实 Telegram 收信到持久RunRequested已通过联合验收。
- **基线**：Control `79e5795` 已有 Deployment Publication；Gateway 路由消费在独立任务工作树。
- **拥有方**：Control API `channelbinding` Module；当前切片同时维护代码、协议、事务测试和运行说明。
- **依赖**：[ChannelAccount](channel-account.md)、[Deployment](deployment.md)、
  [Gateway 接入改造](../channel-gateway/control-integration-v1.md)。

## 1. 模型与目标

```text
Tenant
 ├─ ChannelAccount ── 当前连接配置与模块私有凭据 ── Gateway 连接/接收
 ├─ ChannelBinding ── 精确 DeploymentRevision ── RuntimeManifest
 └─ AccountRouteState ── 账户级 RouteGeneration ── RouteProjection Outbox

用户消息 → Gateway 账户资格 + 固定路由投影 → RunRequested → 后续 Worker
```

ChannelBinding 回答“这个账户的新消息交给哪个已发布运行目标”，不回答机器人如何连接。
账户连接配置用 HTTP 快照供应，绑定路由用现有 NATS 流供应；两个通道都不传递凭据值。

Deployment Publish 只是生成不可变 Revision/Manifest 并持久化发布 Outbox，不自动切流。
Binding 生效是路由配置变化，不自动创建 Run；真正接纳由后续消息触发，Worker 仍为后续切片。
不新增 Environment、Agent→资源映射表、Deployment 全局 active 指针或独立调度产品。

## 2. 实体、身份与不变量

| 类型 | 持久字段 | 不变量 |
| --- | --- | --- |
| ChannelBinding | tenant_id、binding_id、account_id、binding_revision、enabled、stable PublishedTarget、可选 TrafficRollout、created/updated_by/at | stable/canary目标均由 Deployment Owner 解析，不信客户端自报 |
| AccountRouteState | tenant_id、provider、account_id、generation、min_route_generation、canonical_projection、projection_digest | 生命周期跟随账户，不能用 Binding CAS 代替 generation |
| CommandReceipt | tenant_id、operation、scope_id、key_hash、request_digest、result、created_at | 只存脱敏确定结果，按稳定 key 重放 |
| RouteProjection Outbox | 现有 control_outbox 行 | 一次有效投影变化一个事件，永久 event_id 与确定正文 |

V1 每账户至多一个稳定 Binding 记录，account_id 创建后不变；不实现 Binding 删除或迁移。
一个 Binding 最多携带一个 canary，不支持任意多目标权重表、群/会话条件表达式或跨租户转移。Gateway 已支持同账户换 Binding
后 generation 延续；V1 即使暂不开放该操作，仍独立持久化账户序列，避免未来重置路由水位。

绑定只能引用本租户的账户和精确发布目标。AgentVersion、ProfileRevision、Manifest 的内容
由 Deployment 冻结，Binding 不再匹配名称、编译资源或读取真实凭据。发布新的 ProfileRevision
或 DeploymentRevision 不改变任何现有 Binding。历史 Run 的 Admission 快照不会被改绑覆盖。

## 3. 最小用例与状态变化

| 用例 | 输入 | 持久变化 |
| --- | --- | --- |
| CreateChannelBinding | account_id、确定 deployment_id/revision_number、Idempotency-Key | binding_revision=1、enabled=false；保存解析目标，建立初始 disabled 投影 |
| SetChannelBindingTarget | binding_id、expected_binding_revision、新精确目标、Idempotency-Key | 目标语义变化时 Binding CAS +1；有效路由改变时账户 generation +1 |
| SetChannelBindingEnabled | binding_id、expected_binding_revision、enabled、Idempotency-Key | Binding 启停意图改变；有效路由变化时发布快照 |
| Get/ListChannelBindings | 同租户路径、ID/游标 | 读取管理事实和已排队事件身份，不宣称 Gateway 已应用 |
| SetChannelAccountEnabled | 账户用例，见账户文档 | 账户行与有效路由在同一事务变化；不冒充 Binding CAS 命令 |

Binding 的 desired.enabled 与账户 enabled 分开：

```text
effective_route_enabled = account.enabled AND binding.enabled
```

| 账户 | Binding | 运行投影 | 说明 |
| --- | --- | --- | --- |
| disabled | disabled | disabled | 默认录入，无新运行 |
| enabled | disabled | disabled | 可以接入渠道，尚不接纳 Agent 运行 |
| enabled | enabled | enabled + 精确目标 | Gateway 仍须满足自身资格/就绪门禁 |
| disabled | enabled | disabled | 账户停用保留 Binding 的启用意图；重新启用账户会恢复该目标的路由 |

创建 Binding 不隐式启用账户。显式启用 Binding 要求账户已启用且凭据元数据齐全。
停用账户保留 Binding 意图，所以账户重新启用可能重新开放流量；账户启停用例/界面必须明确
提示这一结果，事务重新核对已存目标完整性。修改 disabled Binding 的目标可暂存但不发
含目标的 disabled 事件。账户停用和 Binding 停用不是同一个操作。

同一目标/状态且 CAS 当前时为 NOOP，版本不变；CAS 过期仍冲突。Binding变更不改Account connection_revision或凭据版本，不因此重新建立机器人连接。
有效路由变化会推进min_route_generation和账户目录snapshot_revision；这是传播恢复屏障，不是连接变更。

## 4. 管理输入与目标解析

已实现的管理 API（显式Channel配置后注册）：

| 方法与路径 | 用例 |
| --- | --- |
| POST /v1/tenants/{tenant_id}/channel-bindings | Create，201 |
| GET /v1/tenants/{tenant_id}/channel-bindings | List，200 |
| GET /v1/tenants/{tenant_id}/channel-bindings/{binding_id} | Get，200 |
| POST /v1/tenants/{tenant_id}/channel-bindings/{binding_id}/target | SetTarget，200 |
| POST /v1/tenants/{tenant_id}/channel-bindings/{binding_id}/traffic | SetTraffic，200 |
| POST /v1/tenants/{tenant_id}/channel-bindings/{binding_id}/enabled | SetEnabled，200 |

Create 输入：account_id 与 target:{deployment_id,revision_number}；初始 disabled 是固定规则，
不接受用户 supplied Manifest/AgentVersion/ProfileRevision、latest 或 agent_id 替代目标。
SetTarget 输入还含 expected_binding_revision；SetTraffic 输入含 expected_binding_revision、
确定 canary target、percentage_basis_points 与 canary_subjects；SetEnabled 输入含
expected_binding_revision 与布尔 enabled。普通Body上限16 KiB，灰度Body上限64 KiB；关闭DTO、
重复 JSON key/未知字段拒绝。List 默认50、最大100。全部写操作需 Idempotency-Key，Tenant
身份来自有效 Session+Membership，不来自 Body。

用户可在产品中先选 Agent，但提交前必须选择该 Agent 对应的确定部署修订；不引入服务端
“自动挑最新部署”的隐含逻辑。目标解析端口定义于使用方 Application：

```text
PublishedTargetReader.ReadExact(tenant_id, actor_id, deployment_id, revision_number)
  -> {tenant_id, deployment_id, revision_number, deployment_revision_id,
      manifest_id, manifest_digest}
```

Adapter 调用现有 Deployment Application.GetDeploymentRevision，由拥有方进行授权、
Canonical Content、来源关系与 Digest 完整性检查，再返回最小目标 DTO。不导入 Deployment
PostgreSQL Adapter，不自己 JOIN 其表；公开 manifest_view 不用于运行完整性校验。
目前公开查询以 deployment_id + revision_number 为定位方式，因此管理写协议采用相同形状；
解析输出再固定 deployment_revision_id，不要求额外增加按不透明 Revision ID 读取的业务接口。
启用Binding或重新启用账户时，使用其已存deployment_id/revision_number复核，输出三元组必须
与已存值完全一致；不同则报CHANNEL_TARGET_INTEGRITY，不悄悄覆盖为新值。

目标不可变且 V1 不删除，所以无需跨模块共享长事务锁住 Manifest。写事务仍需复核 Tenant/Owner
权限、账户与 Binding 的归属/CAS；将来增加目标删除/撤销必须先补生命周期与引用竞争契约。

## 5. 校验边界与稳定错误

顺序为：认证/角色 → 输入关闭校验 → 账户与 Binding 归属 → Deployment Owner 精确读取 →
目标完整性 → 账户资格/必需凭据元数据 → CAS/幂等/原子保存。

写命令 OWNER-only；有效 MEMBER 可读脱敏状态。普通用户、Platform Operator 或只持有
account_id/manifest_id 的调用方均不获得额外权限。跨租户资源返回404以隐藏存在性。

| code | HTTP | 含义 |
| --- | --- | --- |
| CHANNEL_BINDING_INPUT_INVALID | 400 | Draft/latest、未知字段、非法版本、重复 key |
| CHANNEL_BINDING_NOT_FOUND / CHANNEL_TARGET_NOT_FOUND | 404 | 当前租户内不可见对象 |
| CHANNEL_PERMISSION_DENIED | 403 | 无写权限；未认证为401 |
| CHANNEL_ACCOUNT_ALREADY_BOUND | 409 | V1 一账户一 Binding |
| CHANNEL_BINDING_REVISION_CONFLICT | 409 | Expected Binding CAS 过期 |
| CHANNEL_IDEMPOTENCY_CONFLICT | 409 | 同 key 不同规范化输入 |
| CHANNEL_ACCOUNT_DISABLED / CHANNEL_CREDENTIAL_REQUIRED | 409 / 422 | 启用前提不满足 |
| CHANNEL_TARGET_INTEGRITY | 500 | 精确目标/Manifest 关系或摘要损坏 |
| CHANNEL_ROUTE_GENERATION_EXHAUSTED | 409 | 2^53-1 耗尽，禁止回绕 |
| CHANNEL_DEPENDENCY_UNAVAILABLE | 503 | Deployment/Tenant Owner 依赖故障 |

这些都是 Control 的静态与拥有方事实校验；Provider 在线、Token 真实可用、SDK 连接、NATS
实时可达与 Worker 在线不在 SQL 事务中探测。Gateway 的连接/投影资格仍决定是否接纳，
Control 保存成功不保证运行面已经就绪。

<a id="binding-transaction"></a>
## 6. 发布事务、并发与幂等

### 6.1 表与锁

新增 channel_bindings、channel_account_route_states，复用账户模块的
channel_command_receipts 和现有 control_outbox。表、唯一键和外键包含 Tenant；
账户的全局稳定 ID 支撑 Gateway `(provider, account_id)` 路由身份，不由租户自填碰撞值。

RouteState 在账户创建时生成，generation=0 且没有已发布投影；0 只存在于 Control 内部。
第一次 CreateBinding 即生成 generation=1 的 disabled tombstone。随后比较“忽略 generation
的规范化有效投影内容”；只有内容改变才 generation +1，并在事件中写入新 generation。
例如disabled时改目标只更新Binding/Receipt，不重复投递同一disabled投影；第一次
CreateBinding的disabled generation=1也将min_route_generation设为1并推进目录水位。
这不把内部修改说成已发布路由：没有投影变化就没有新的 route event。

锁顺序：账户目录（账户命令和可能改变有效路由的Binding命令均参与）→ Account → AccountRouteState → Binding。
启停/改绑必须先拿 Account 锁，因此 Account disable 与 Binding enable/target 更新互斥。
同账户不同 Binding/CAS 用例也按同一路由行分配 generation；不用 Binding revision、时间戳
或独立 SQL sequence 伪造账户提交顺序。

### 6.2 写事务步骤

1. 重新认证/授权；查 Receipt 并核对请求摘要。命中时返回原确定结果，不重新读最新目标。
2. 对新命令通过 Deployment Owner 解析精确目标；缓存只是本次调用局部值，不跳过其校验。
3. 事务内取得上述锁，复核当前 Owner 授权、账户身份、CAS 和一账户一绑定约束。
4. 计算新 Binding/账户状态及有效投影，检查平台限额、目标关系和 generation 上限。
5. 保存业务变化；若有效投影改变，更新RouteState的generation/min_route_generation，推进账户
   目录snapshot_revision并写完整事件到control_outbox（PENDING）。同一事务账户也变化时目录只+1。
6. 写脱敏 Receipt，记录固定 Binding/账户版本、RouteGeneration、event_id（如有），原子提交。
7. 返回管理结果。数据库提交成功不等于 PubAck，更不等于 Gateway 的投影已应用。

同一事务任何一步失败均不保留业务、序列、Outbox 或成功 Receipt 的部分状态。
Receipt 唯一键为 `(tenant_id,operation,scope_id,key_hash)`；输入摘要绑定精确 target、Expected
和 operation，不包括服务器生成 event_id。该命令无明文凭据，使用项目统一 Canonical JSON
和 SHA-256 即可；账户凭据命令必须采用账户文档的 MAC 规则，不能照搬无密钥摘要。
并发同 key 同正文只有一次业务提交，异正文冲突；授权撤销后不因持有旧 key 重放敏感结果。

如果客户端没有收到提交响应，使用原 key 重试得到原结果；Outbox 发布失败不会回滚已提交
Binding。后续重新改绑是一条带新 Expected/new key 的新命令，不修改旧事件或旧 Manifest。

## 7. 与已存在 Gateway 路由契约衔接

权威接收 Schema 是 Gateway 任务中的 `api/events/control/v1/route-projected.schema.json`，
本轮只读核对 SHA-256 为 `97c1f701e220fb25aa9e94d5afa2d340828cf4559864d8ffe6407c0060c50cba`。
正式开发已与 Gateway 任务协调，将该 Schema 按相同哈希原样同步到 Control 工作树；
Control 的路由 Domain 测试以它验证生产者输出，不另行维护不同的事件格式。
当前已验证真实 PostgreSQL 原子 Outbox 与受限 JetStream Producer：等待正确 stream 的持久 PubAck 后才标记 PUBLISHED；真实 Telegram 消息接纳已通过联合验收，见[验收记录](channel-acceptance.md)。

- Subject：`control.channel-route.v1`；Stream：`CHANNEL_ROUTES_V1`。
- Consumer：`channel-gateway-routes-v1`，Gateway 用自己的受限 NATS 身份消费。
- Envelope 必须且只含 event_id、整数 schema_version=1、enabled、route；最多 **16384** 字节。
- enabled=true：route 必须含 provider/account_id/generation/tenant_id/binding_id/
  deployment_revision_id/manifest_ref/manifest_digest；存在灰度时还含完整TrafficRollout，
  canary PublishedTarget 不依赖 Gateway 回查Control，其他额外字段仍被拒绝。
- enabled=false：route 仅含 provider/account_id/generation；目标/租户/Binding 字段必须省略，不能传 null。
- generation 范围1..2^53-1，属于账户，换 Binding/停用/重启用也不重置。

双方已确认字段对应：

```text
deployment_revision_id = DeploymentRevision.ID
manifest_ref           = RuntimeManifest.ID
manifest_digest        = RuntimeManifest.ContentDigest
```

Manifest.ID 是不透明 `rmf_...` 标识，不要求 URL/路径；Gateway 不在这里解析它。
同 ID 异 ContentDigest 是不可变产物冲突，不改查 latest。摘要不是公开 manifest_view
的摘要，也不是 Outbox payload_digest。

### 7.1 完整启用与停用示例

以下 ID 和摘要只是结构 fixture，不宣称示例对应可执行的数据库记录。

```json
{
  "event_id": "evt_route_fixture_1",
  "schema_version": 1,
  "enabled": true,
  "route": {
    "provider": "wecom",
    "account_id": "cha_fixture",
    "generation": 1,
    "tenant_id": "tnt_fixture",
    "binding_id": "chb_fixture",
    "deployment_revision_id": "dpr_fixture",
    "manifest_ref": "rmf_fixture",
    "manifest_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
}
```

```json
{
  "event_id": "evt_route_fixture_2",
  "schema_version": 1,
  "enabled": false,
  "route": {
    "provider": "wecom",
    "account_id": "cha_fixture",
    "generation": 2
  }
}
```

disabled wire 不含 tenant_id 不等于 Control 丢弃归属：Outbox 行、Receipt、账户和 RouteState
仍持有 Tenant。Relay 使用已授权的 Producer 身份，按模块生成的合法 payload 发布，不把
Outbox 行 envelope 整份再套在 wire 上，也不将公开请求直接广播。

### 7.2 Outbox 行映射与 Relay

内部 event_type=`ChannelRouteProjected.v1`，aggregate_type=`ChannelAccountRoute`，
aggregate_id=account_id，aggregate_revision=generation；control_outbox.schema_version 用现有
text列值 `"1"`，payload 的 schema_version 是 JSON整数1。Deployment 的
RuntimeManifestPublished.v1 则沿用字符串 `"v1"` 和独立 payload；两种事件独立分派。

payload_jsonb 保存规范化完整路由事件，payload_digest 对 Canonical JSON 求 SHA-256。
Relay 从持久行重建并校验 Canonical payload，不用 PostgreSQL JSONB 文本格式作字节身份。
`event_id`、稳定正文和 NATS `Nats-Msg-Id` 在重试中保持；JetStream 去重窗口不是业务永久幂等保证。

Relay 作为既有 Control bootstrap 后台任务领取可到期 lease，完成 PENDING→IN_FLIGHT→PUBLISHED
或重试。仅在收到正确 subject/stream 的持久 PubAck 后记录 published_at；发布成功后进程崩溃
允许重复发送，由 Event ID/generation 处理。NATS 授权失败、流满/DiscardNew、超时分类重试或
置FAILED并保留可恢复记录，不删除失败 Outbox，不为发送成功伪造 ACK。

### 7.3 顺序、重建与运行就绪

Gateway 对旧 generation 存 Receipt 后忽略；同 generation 同内容幂等、异内容冲突；同 event_id
异内容也冲突。账户配置和路由可先后到达，Gateway必须同时满足可信账户资格、可信目标投影以及
`route.generation >= account.min_route_generation`才能接纳；最低路由下限来自已应用账户目录。停用账户先被某个通道观察到时即可关闭新工作，不等待另一个通道来宣布停用。
重新启用也不绕过任一门禁。账户disable→改目标→enable即使被HTTP合并，最终快照也携带
更高min_route_generation，旧enabled路由必须等待；这不是所有副本瞬时切流。新Admission
在Gateway同一事务的AccountUseGuard和RouteGuard下验证下限；已接纳的Receipt/Run/ReplyOrigin
保持不变，Delivery不因当前路由改变而改投旧回复。详见[账户恢复屏障](channel-account.md#ca-route-floor)。

当前 Gateway 路由重建依赖完整保留的 Limits 日志、Stream 实例身份和连续提交 checkpoint；
启动追到捕获水位后才 initialized。截断、purge或重建触发隔离，不能把账户 HTTP 快照当作
重置路由 Stream lineage 的恢复办法。显式路由“权威快照+增量”恢复协议仍为后续开发项。

Control 必须长期保留当前 AccountRouteState，未来快照重建不得只依赖 Outbox 历史。
当前阶段没有该恢复接口，保留当前状态也不代表 Gateway 已支持恢复。路由流采用 DiscardNew，
满时保留旧日志并让 Producer 保留事件，禁止静默淘汰停用记录以腾空间。
Control在Binding发布前经拥有方校验真实Published Revision/Manifest，Gateway通过可信
Route投影接纳并固定三元组。Manifest正文的分发/读取与执行归后续Worker；本次Gateway
Admission不依赖新增Target Resolver，格式相同本身也不构成可信来源。

## 8. 状态查询与用户流程

Control 读模型返回 binding_revision、desired.enabled、精确 target、当前RouteGeneration，
及本次投影 event_id/Outbox状态（有事件时）。状态名分别为控制已保存、待分发/已PubAck；
没有 Gateway 应用观测时返回 UNKNOWN，不根据 PUBLISHED 推断实际可接纳。

完整无 Environment/无用户映射表流程：

1. 用户保存 Runtime Profile，直接填写执行资源凭据，发布确定 ProfileRevision。
2. 用户发布 AgentVersion；Deployment 用二者编译并保存精确 Revision/Manifest。
3. 用户创建自己的 ChannelAccount，直接输入机器人凭据，启用账户。
4. Gateway 按完整快照、内部授权解析与 SDK 认证接入，报告连接状态。
5. 用户创建 Binding 选择该 DeploymentRevision，并显式启用；Control 原子写路由 Outbox。
6. Relay PubAck 后 Gateway 应用路由；只有实际账户/路由/目标资格齐全才接纳下一条消息。
7. 消息触发 RunRequested；Worker 消费和真实执行是后续独立切片，本次不宣称已经完成。

回滚业务路由是把 Binding 目标改回另一个仍有效的旧 Revision，使用新的 Binding CAS 和更高
账户 generation；不回滚 Manifest 内容、generation或旧 Event ID。停用后恢复也用新的路由事件。

### 8.1 版本回退不等于灰度发布

当前实现必须拆成独立能力描述，不能用“支持灰度/回滚”合并表述：

| 能力 | 当前状态 | 准确语义 |
| --- | --- | --- |
| Revision 路由回退 | **已实现** | DeploymentRevision/RuntimeManifest 不可变；Owner 可用 Binding CAS 将目标重新指向仍有效的旧 Revision，并发布更高 RouteGeneration。只影响之后首次接纳的新事件，旧 Admission/Run/Reply 继续使用其固定目标。 |
| 双目标流量灰度 | **已实现** | Binding 保留一个 stable target，并可发布一个确定的 canary target、0–10000 基点比例和最多100个显式 sender ID；完整策略随更高 RouteGeneration 投影。 |
| Gateway 稳定选择 | **已实现** | 0基点暂停灰度；非零时显式 sender 优先，其余使用 rollout ID、Provider、Account 与可信 sender ID 的确定性哈希。同一用户在同策略和账户下跨重试/会话保持目标一致。Admission 固定最终 DeploymentRevision/Manifest。 |
| 结果指标与自动回退 | **未实现** | Admission trace 已带 rollout ID、variant 和所选 DeploymentRevision，但尚无正式分版本聚合窗口、阈值决策器或自动发布停止/回退命令。 |

`SetChannelBindingTarget` 仍只是**精确目标切换/回退**；它在目标真正改变时清除已有灰度
策略。`POST /v1/tenants/{tenant_id}/channel-bindings/{binding_id}/traffic` 才是灰度命令，
输入 Binding CAS、确定 canary Revision、基点比例和显式 sender 列表，并支持
`Idempotency-Key`。发布新的 DeploymentRevision 本身不会自动产生流量。

V1 使用受限的 stable+canary 双目标，而不是任意多目标权重表。Control 在事务内保存策略，
并把完整 canary PublishedTarget 编译进 `ChannelRouteProjected.v1`；Gateway 不读取 Control
Draft、不查询“最新版本”，也不在接纳时访问 Deployment。比例更新若候选不变会保留
rollout ID，使哈希桶单调扩张/收缩；更换候选会分配新 rollout ID。0基点是人工停止灰度，
把 Binding target 改到旧 Revision 是人工整体回退，两者都不是自动回退。

## 9. 已实现后端切片与跨任务归属

| 阶段 | 当前实现位置 | 验收出口 |
| --- | --- | --- |
| B0 关闭协议 | 管理/事件 fixture、目标DTO；协调已有路由 Schema | 无新增相互冲突的同名协议、整数/字符串版本分别验证 |
| B1 Domain | domain/binding.go、route.go、snapshot.go | 状态表、投影比较、CAS/序列上限、无秘密payload |
| B2 Application | application/commands.go、queries.go、ports.go | 通过 Deployment Query Port；角色/租户/Receipt/NOOP |
| B3 PostgreSQL | 所属 Binding/Account/Route Repository；灰度字段由0009迁移加入，不改已发布基线 | 账户停用/绑定启用竞争、事务原子、唯一键、并发重试、策略重启恢复 |
| B4 HTTP / wiring | 所属 inbound/http、模块wiring、bootstrap | 后端真实接口与OpenAPI一致，不抢先改Web |
| B5 Distribution | 模块事件映射、Control Relay/infra连接及NATS受限权限 | 真PG+JetStream、PubAck丢失/重投/断电、保留与恢复 |
| B6 Gateway组合 | Gateway 的 Control 接入实现与验收文档 | 账户/路由乱序、动态接入、凭据轮换、已接纳快照不变 |

上述 B0～B6 已落地；账户生命周期与 Binding 在同一模块内按聚合和用例组织，
不新建共享 Repository 或全局业务事件工具箱。真实 Telegram 收信到 RunRequested 已验收；
Worker、Manifest 正文执行与完整回复不因本次联合验收而视为完成。
当前未稳定V1可直接调整协议与测试基线，不为旧开发数据新增V2、迁移兼容或历史文档副本。

## 10. 设计验收矩阵

| 场景 | 期望结果 |
| --- | --- |
| 同 AgentVersion + 两个 ProfileRevision | 两份独立 DeploymentRevision，Binding精确选其中之一 |
| 发布新Profile/Deployment Revision | 现有 Binding、旧Manifest和已接纳Run不变 |
| Binding精确切换与回退 | Binding CAS和RouteGeneration各自推进，新Run选新目标，旧Run不变 |
| 15% 灰度 + 显式用户 | Control 固定 stable/canary 与1500基点；显式 sender 优先，其他 sender 确定性分桶；Admission 固定所选目标 |
| 比例改为0 | 新接纳全部选择 stable；已接纳 Run 不变，策略仍保留以便审计和后续调整 |
| 把版本回退误报为自动灰度回退 | 拒绝该结论；当前回退是显式 Binding 命令，尚无指标阈值自动决策器 |
| 一账户并发创建两绑定 | 唯一键只允许一个，失败方不留Outbox/Receipt |
| 停用账户与启用Binding竞争 | Account行锁串行；绝不出现账户disabled但effective route enabled |
| disabled期间改目标 | 管理CAS推进，无含目标的disabled事件；重新启用发完整新目标 |
| 多次启停或未来换Binding | generation终身单调，不复用Binding CAS或旧event_id |
| 重复/乱序事件与同代异内容 | 前两者按Receipt/generation幂等，后者冲突隔离 |
| 两租户重用相同名称/伪造目标 | 稳定ID独立，跨租户隐藏；凭据不在路由事件 |
| 账户先到/路由先到/Manifest未就绪 | 缺任一运行前提均不接纳；不临时查Draft/latest |
| Outbox发布后崩溃、NATS流满 | 原event_id重试，DB事实保留，PUBLISHED只以PubAck确认 |
| 路由日志截断或重建 | 隔离并按专门恢复协议处理；账户快照不重置路由水位 |
| Provider掉线 | 运行诊断，不回滚已发布Manifest或伪造Control失败 |
| 无Environment、无用户映射表 | 完整流程只需确定版本、账户录入和显式绑定 |

### 10.1 审查补充验收

- B-FLOOR-1：账户与路由同事务发布新generation/min_route_generation和目录水位，失败全回滚。
- B-FLOOR-2：disabled→改目标→enabled只抓到最后一份快照时，旧路由低于floor被拒绝，追平后才接纳。
- B-FLOOR-3：Binding-only变更不推进连接/凭据版本，也不重建Client；必要时目录版本推进。
- B-FLOOR-4：已接纳旧Run的Delivery继续绑定原ReplyOrigin，账户停用检查与路由下限检查分别适用。

## 11. 当前 Relay 实现与边界

Bootstrap 为 Channel 装配同进程后台 Relay，不创建调度服务。仅领取本scope的
`ChannelRouteProjected.v1 / ChannelAccountRoute` 记录，不领取 Deployment的Manifest事件。
PostgreSQL用`FOR UPDATE SKIP LOCKED`领取到期PENDING或过期IN_FLIGHT；每次claim生成独立
随机token，lease=15秒，attempt_count递增。完成更新同时核对token、attempt、IN_FLIGHT和
DB时钟下未过期lease；过期发送者即使随后得到PubAck也不能完成新claim。

发送前校验Canonical payload及digest、event_id、账户、generation和租户三元组字段。
静态内容损坏进入FAILED并保留；网络/授权/流满/超时统一存储稳定错误码，指数退避1～60秒
继续尝试，不保存broker原始错误文本。发布预算5秒，`Nats-Msg-Id=event_id`；
仅正确`CHANNEL_ROUTES_V1`且sequence>0的PubAck才完成。进程在PubAck后、DB完成前退出时，
lease到期后重发原ID/正文，Gateway永久EventID/generation幂等不依赖短时NATS去重窗口。

真实PG+独立JetStream回归覆盖过期claim重取/旧claim被fence、PubAck后失去DB完成的重投、
稳定ID正文去重、无stream拒绝、临时失败保留与损坏正文FAILED；DB不可变trigger也单独验证。

目标可信性在Control发布Binding前通过Deployment拥有方Application对真实Published Revision/
Manifest进行读取与完整性校验。Gateway消费受限Producer写入的可信路由三元组，并在同一
Admission事务核验账户floor与route generation，固定到RunRequested；它不宣称已经下载或
校验Manifest正文。正文读取/执行完整性属于后续Worker边界。本切片不新增TargetReady服务、
ManifestResolver端点或恒真的就绪适配器。

实际Telegram及跨Workload持久证据见[联合验收记录](channel-acceptance.md)。
