# 实现与验证

本文列出设计覆盖、代码实现和验证范围。目标架构中尚未实现的能力单独标明；完整风险与缓解方案见[风险清单](risks.md)。

## 设计覆盖

| 设计主题 | 文档 |
| --- | --- |
| 多租户、节点化部署、数据同步、多后端、IM、治理监控和故障恢复 | [方案](solution.md)、[架构](architecture.md) |
| tenant、agent、binding、session、event、memory、summary、audit 关系 | [数据模型](data-model.md) |
| 企业微信与飞书的协议及接入差异 | [企微与飞书差异](im-channels.md#协议细节与扩展设计)、[接入指南](im-channels.md) |
| SQL、Redis、向量库、对象存储的数据放置与同步 | [存储与一致性](storage-and-consistency.md) |
| 完整消息时序与 request_id/trace_id 关联 | [核心消息时序](sequence.md) |
| 并发、故障恢复、隔离与资源治理风险 | [十三项生产风险](risks.md) |
| 最小可运行与生产推荐部署 | [本地部署](local-deployment.md)、[生产部署设计](production-deployment.md)；生产角色与编排尚未实现 |
| 框架复用能力与平台新增职责 | [能力基线](project-foundation.md#6-上游能力基线与平台新增职责) |

## 参考实现范围

| 已实现能力 | 代码入口与边界 |
| --- | --- |
| Tenant/App/Revision、发布/回滚、服务端 Session Pin | [`tenant`](../trpcservice/tenant)、[`sessiondir`](../trpcservice/sessiondir)、[Admin API](admin-api.md)；动态灰度和 Pin 换代未实现 |
| Runtime、真实 LLMAgent/Runner、HTTP/SSE 与网页 | [`agent`](../trpcservice/agent)、[`web`](../trpcservice/web)、[`sessionrun`](../trpcservice/sessionrun)；支持确定性模型与 OpenAI-compatible 模型 |
| Session 多后端与跨节点互斥 | [`sessionbackend`](../trpcservice/sessionbackend)、[`storagebundle`](../trpcservice/storagebundle)、[`sessionlease`](../trpcservice/sessionlease)；InMemory/PostgreSQL/Redis，租约不提供存储写入 fencing |
| 身份、SecretRef/PolicyRef 授权与工具审计 | [安全与治理](security-and-governance.md)、[Tool Policy](tool-policy.md)；静态凭据和结构化工具日志，未实现生产 Secret Manager、预算、审批和持久 Audit |
| 企微和飞书单聊文本 | [`channels/text`](../trpcservice/channels/text) 共用消费者与 PostgreSQL Store；独立适配器、持久去重、最终回复，详见[接入契约](im-channels.md) |
| 可选观测与本地部署 | [部署指南](local-deployment.md)；accept/execute/deliver 三阶段 Span、次数和毫秒耗时，按 request_id 关联 |

Memory/Summary、Knowledge/Artifact 路由、在线迁移、群聊、媒体/卡片、动态 Binding、完整连续 Trace、成本治理及多角色生产部署已有设计，尚未实现。架构图与 ER 图中的这些组件同样遵循此边界。

## 验证结果

| 检查 | 已取得的证据 | 适用范围 |
| --- | --- | --- |
| 最终功能代码回归 | 2026-09-08，`650f504` 对应代码通过全仓 race、vet、build，含 Redis 租约 TTL 精度校验与执行中取消回归 | 默认测试关闭外部模型与存储门控；后续仅文档修改不改变该代码版本 |
| PostgreSQL 与 IM 公共链路 | 2026-09-08，`650f504` 对应代码通过 postgres、wecom、text、feishu 四包集成 | 真实本地 PostgreSQL、模拟平台、真实 Runner/Session/Store，验证去重、顺序、隔离、发送失败与保守恢复 |
| 真实企业微信 | 2026-09-07，构建 `0378175`：1 条 Inbox、Run succeeded/attempt=1、Outbox sent/attempt=1、duplicate_risk=false，明确 errcode=0 | 正常单聊、真实模型与 PostgreSQL；该平台记录早于公共消费者提取，当前代码通过本地集成回归 |
| 真实飞书 | 2026-09-08，构建 `f6bb709`：2 条 Inbox/Run/Outbox，同一 Principal/Session/Revision；每轮一次执行和发送，均 succeeded/sent、有平台消息 ID、duplicate_risk=false | 官方长连接、真实模型与 PostgreSQL，正常两轮单聊 |
| 网页与部署 | 2026-09-07，部署提交 `29a6b90`，Go 功能代码同 `f5ed53c`；默认网页 8 项 HTTP/SSE、临时 PostgreSQL schema 启动和 Collector 配置校验通过 | 健康、认证隔离、续聊、发布/回滚保持 Pin；Collector 只验证配置，未联调真实 Bot 遥测，未验证生产部署 |
| 图表 | 系统架构图、核心时序图和 ER 图通过 Mermaid CLI 11.12.0 渲染检查 | 描述目标架构，图中生产能力不等同于代码实现 |

`sent` 表示平台接受回复，不表示用户已读。真实正常收发不证明平台故障恢复；重复投递、拒绝、结果未知和对象重建恢复以本地测试为证据。验证只记录状态、计数和必要版本，不发布用户正文、外部身份、凭据或原始运行日志。

## 复现命令

在项目根目录执行；构建后默认网页无需外部模型或数据库，启动步骤见[本地部署](local-deployment.md)。

```bash
TRPC_SERVICE_MODEL_INTEGRATION=0 TRPC_SERVICE_SESSION_INTEGRATION=0 \
  go test -race -count=1 -timeout 900s ./...
go vet ./...
go build ./...
```

准备 [Docker 本地数据库](local-deployment.md#可选-postgresql-与企业微信) 后可运行四包集成，使用独立临时 schema，无需真实 Bot 凭据：

```bash
TRPC_SERVICE_MODEL_INTEGRATION=0 TRPC_SERVICE_SESSION_INTEGRATION=1 \
  TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
  go test -race -count=1 -timeout 300s ./trpcservice/channels/postgres \
  ./trpcservice/channels/wecom ./trpcservice/channels/text ./trpcservice/channels/feishu
```

PostgreSQL/Redis 后端与双 Worker 的可选验证命令见 [Session 后端](session-backend.md#7-集成测试的运行方式)和 [Session 租约](session-lease.md#6-测试)。真实 Bot 测试只在配置所有者自行发送消息后确认；不能把凭据预检当成收发验证。

## 已知限制

以下为当前实现的明确边界，生产采用前应结合[风险清单](risks.md)评估：

- **入口共用进程，尚无故障隔离。** IM 消费者终止会关闭 HTTP、Admin API 和其他 IM；单次模型或投递失败不等同于消费者终止。`/healthz` 不表示 Bot 就绪。按角色与 Binding 隔离、局部重启和健康检查属于[生产设计](architecture.md#63-故障隔离与恢复设计)，未在当前代码中实现。
- **合法凭据可以制造无界 Session。** Session 目录与 Session Service 都没有配额、TTL 或 LRU，一个有效 key 可以用无限多的 `X-Session-ID` 无限增长。默认 profile 下耗的是进程内存；`postgres` profile 下耗的是磁盘，且上游默认软删、不回收，只是把失败从 OOM 换成了库涨满。
- **首轮 OpenAI 历史可以伪造。** 平台只决定会话归属，不校验请求体 `messages`；新 Session 的第一轮可以注入编造的"历史对话"。
- **Adapter 拒绝的请求也会建立 Pin。** Session 与 Revision 在调用上游 Adapter 之前确定，格式错误的首轮同样会钉住该 Session。
- **默认 profile 的 Pin 只在单进程内存中**，多节点部署会各自 Pin 到各自看到的默认版本。`postgres` profile 下 Pin 落库，这一条才不成立。
- **`postgres` 进程 profile 仍是持久控制面的唯一实现。** 它不允许“只持久化 Session”：控制面、Pin 和进程默认会话历史绑在同一个 DSN 与 schema 上，因此不会出现“Session 还在、Pin 丢了”。租户 BackendProfile 已可把 Session 动态路由到 PostgreSQL 或 Redis，但 Pin 与 Profile 真相源仍在进程 profile 的 PostgreSQL；并发仍由 Run 租约在入口处理。
- **BackendProfile 有界但不淘汰。** Profile ID 即版本，Admin 只有 Create/Get/List、没有 Update/Delete；每租户最多 32 个。Router 不做 TTL/LRU，一个成功解析的动态 Profile 会持有连接池直到进程关闭。Profile 行保存并在每次读取时复核 fingerprint，Router 还会拒绝同 ID 的进程内漂移；fingerprint 不写入 Revision，因此一个同时越过 Repository 修改 `spec` 和重算 fingerprint 的数据库写入方，在全进程重启后仍可绕过这层事故检测。与 `config_digest` 一样，它不是防恶意数据库写入的认证器。
- **持久 Pin 可以搭配单 Worker 的动态 InMemory Session。** 这个组合不会把旧历史静默切到新 Revision：Pin 仍然存在且继续指向原版本；但进程重启后会话历史会消失，只剩 Revision Pin。它适用于明确接受短暂会话的租户，不等同于持久 Session。多 Worker + InMemory Session 仍由 Factory 明确拒绝。
- **Run 租约提供合作型互斥，不提供写入准入。** 租约在 Run 入口拒绝第二个 Worker（`409 session_busy`），持有者失效后按 TTL 接管，失去租约会取消 Run Context。`Lease.Fence()` 仅用于观测，不阻止旧 Worker 继续写入。上游 `session.Service.AppendEvent` 没有 fence/CAS 参数，`WithAppendEventHook` 与后端写入之间也不具备原子性，因此当前接口无法实现拒绝过期 writer 的存储 fencing。被暂停或分区的持有者在 TTL 内恢复后仍可写入；Runner 在 Context 取消后仍通过 `context.WithoutCancel` 写约一秒终态 Event。取消是尽力而为且最终一致的。租约层没有等待队列，也没有 `MaxRunDuration`，健康续约的 Worker 可以无限持有租约。详见 [Session Run Lease](session-lease.md#4-这把租约不保证什么准确表述)。
- **单实例 Redis 是唯一验证过的协调部署。** failover 下锁 key 可能随未同步的副本一起丢失、fence 计数器可能回退，因此**不宣称** failover 下仍然互斥或 fence 仍然单调。fence key 永不回收，一个历史 Session 一个小整数。
- **`SeedDemo` 的"不覆盖已发布版本"存在一个已记录的窗口。** 它先读应用当前默认 Revision、再决定是否发布，两步之间不是原子的；Repository 没有"仅在未发布时发布"的原语。并发启动是安全的（各自要么不发布，要么发布同一个 `echo-v1`），但操作员恰好在这两步之间发布新版本时，该次发布可能被启动流程覆盖。
- **4 轮工具循环上限不是 Run deadline。** 它既不限制慢模型/慢 Tool，也不限制单轮并行 Tool 数；Tool 审计只落结构化日志，不是持久化 Audit Store。详见 [Tool 与 Policy Runtime](tool-policy.md#5-明确限制)。
- **`base_url` 不是受 entitlement 约束的能力。** SecretRef 和 PolicyRef 都要过租户授权，但 Revision 指向哪个上游地址不受白名单约束。租户只能引用被授权的变量，却可以把解析出来的凭据发往自己指定的地址：entitlement 决定的是"哪个 Secret 会泄露给谁"，不是"会不会泄露"。
- **`config_digest` 是不带密钥的 SHA-256。** 发布态 Revision 在构建 Runtime 时会重算并逐字节比对，绕过 Admin 面直接改库会在下一次构建时被拒；但计算方式公开且无密钥，一个既能改数据行、又能重算指纹的写入方仍可改道。这一条由 `agent.TestRuntimeDigestDoesNotDefendAgainstAWriterWhoCanRecomputeIt` 显式固定，**不能**当作已解决。
- **两条链路都只是静态 API Key。** 没有 JWT/OIDC，没有动态 RBAC，没有轮转、过期或撤销——撤销一把 key 的唯一手段是改清单并重启进程。Security Manifest 只在启动时读一次，**没有热加载**。没有独立的管理操作审计流水（只有 Revision 上的 `created_by`），没有生产 Secret Manager，没有预算、审批或 Guardrail。
- **进程只绑定本机地址，且只服务明文 HTTP。** 可路由监听会使 Admin Bearer token 暴露在网络传输中；TLS 由外部反向代理终止。回环监听本身不足以满足生产安全要求。
