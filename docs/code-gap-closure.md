# 代码缺口补齐进度

代码候选版本：`0.2.0-rc.3`，数据库 schema 20。当前业务实例未随本轮开发升级。

范围：用户要求补齐 README 对照中确认的六类代码缺口；不扩展到管理 UI、所有 IM/Agent 类型或生产账号配置。以下仅记录本轮新工作，不把既有能力重复算作新增。

- [x] 模型/工具/存储操作耗时指标及错误统计：初始化框架原生 metric provider；Session 物理后端包装；前台/后台模型操作指标；去除指标中的用户、Session 和原始错误属性。
- [x] 模型调用预算预留、幂等结算、后台模型用量记账：Redis 原子预扣、按调用 ID 结算；摘要/Memory 使用 revision 模型；Embedding 一并记账；失败保守计费，不与 Run 汇总重复记账。
- [x] 跨节点自动 Memory 水位单调推进：migration 017 的独立进度表，纳秒精度、原子 GREATEST、旧 Session 值首次导入、任务作用域校验；内存并发及独立 PostgreSQL 跨实例测试通过。
- [x] 租户审计策略的解析、过滤、保留期和失败处理：策略热读取、分级、可选私有磁盘缓冲、幂等重放、受限清理函数和带版本更新 API。
- [x] Knowledge 历史回填、自动校验及迁移门禁：持久化写意图、跨节点协调、历史 chunk/vector 回填、续作任务、内容/向量/检索校验、写入后旧证明失效；InMemory 和独立 Qdrant 测试通过。
- [x] Redis 队列长任务保活、所有权确认、未确认消息保留与背压：心跳、丢失所有权取消、原子 ACK/Retry、安全历史清理、容量拒绝；单元及独立 Redis ACL 测试通过。

开发期间不修改日常 `.env`、不重启业务进程、不迁移业务库或操作真实 IM。使用单元测试和独立容器/测试 schema 验证。新增持久化表与权限将一起提供，实际启用单独说明。不会自动推送 GitHub。

## 已实现部分的兼容性

- 队列 Stream 现在只允许一个平台 consumer group；消费实例名自动加随机后缀，避免同配置节点共享所有权。ACK 会原子删除已确认的记录，Publish 只清理已确认前缀，容量满时任务留在 SQL Outbox。不得把同一个 Stream 当作多个独立订阅组的广播源。
- Redis ACL 新增限定 Stream 键空间的 XINFO/XPENDING/XCLAIM/XLEN/XTRIM/XDEL 和 Lua 操作；未应用到日常 Redis。
- Memory 进度以 `background_watermark` 为真相，不再用无条件的 Session state 写回。旧 Session 水位仅在首次读取时导入；自动提取明确拒绝 update/delete/clear。
- 部署时须先停旧消费者/Jobs、应用迁移与权限，再启新版本；不能混跑仍使用无保护 ACK/MAXLEN 或旧 Session 水位的消费者。

## 1. 模型调用与预算

实际接线：`RevisionCompiler.ModelForRevision → modelops.Model → tRPC Model`。不依赖只在 Runner 执行前检查一次余额，也不要求后台任务假装成一次对话。Summary 使用框架 `NewDynamicSummarizer`，按当前任务解析模型；Memory Extractor 每个任务独立创建。两者均使用任务绑定的 revision、租户密钥授权及价格。

模型调用前使用消息和工具 schema 的 UTF-8 字节数加固定余量估算输入上界，并限制输出 `MaxTokens`。在模型 revision 中可配置：

```json
{
  "source": "startup_env",
  "max_prompt_tokens": 131072,
  "max_completion_tokens": 4096,
  "timeout_seconds": 90,
  "prompt_cost_per_million": 1,
  "completion_cost_per_million": 2
}
```

价格仅为配置示例，不是任何厂商报价。三个调用上限为默认值；分别最多允许 1,000,000、131,072 和 300 秒。真实报价由部署者维护；启用 `daily_cost_usd` 后，不能用零价格绕过金额预算。免费本地模型可仅设置 token 限额，不启用金额限额。Embedding 的价格放在 `knowledge_config.embedding.prompt_cost_per_million`，查询和文档入库均经过同一预算。

Redis Lua 原子完成“检查余额 + 预扣”，余额包含尚未完成调用的占用。流式 usage 按累计值结算一次，不能将每个分片的累计值相加。正常取得 usage 时补差额；超时、取消、开流失败或没有可信 usage 时，至少保留预扣金额和 token。结算失败不退款，并产生告警指标。调用按开始时的 UTC 日期归账，Redis 调用明细和日汇总保留七天；本地后端仅用于单进程开发，重启会丢失预算状态。

这是配置价格下的运行时准入与用量账本，不是供应商账单或绝对不超支承诺。厂商隐藏 token、重试计费或忽略输出上限可能导致实际用量超过估计；代码会记全已知超额、阻止后续调用，并记录 `reservation.overrun`。生产仍需供应商侧硬限额、持久化 Redis（预算库禁止淘汰键）和账单对账。缺少日汇总键时结算会拒绝继续，不能把退款写成负余额；人为删除整个账本仍需备份恢复，代码无法从供应商还原账单。预算账本不是无限保留的财务台账。

新增指标包括 `agent.model.calls`、`agent.model.call.duration`、`agent.model.settlements` 和 `agent.storage.call.duration`。结算区分 `reported/estimated`、成功/失败及超额；原有 token/cost 指标现在包括实际前台调用、摘要、Memory 和 Embedding。原生 tRPC 模型/工具指标保留模型/操作名称，删除用户、Session、提示词及原始错误等高风险属性。

## 2. 租户审计策略

```json
{
  "level": "basic",
  "retention_days": 0,
  "failure_mode": "fail_closed"
}
```

- `basic`：记录事件；普通成功事件不保存可选 details，安全事件保留脱敏后的必要细节。
- `full`：记录全部事件及安全脱敏后的 details，不开启正文或密钥记录。
- `security`：只记录工具、审批、管理、通道治理、失败、拒绝、取消等安全事件，不能关闭这些事件。
- `retention_days=0`：不自动删除。显式设置 1–36500 后，Jobs 每次按租户最多清理 1000 条；清理和清理回执在同一 SQL 事务提交。
- `fail_closed`：写入失败返回错误；已发生的外部动作不能因此倒退，所以工具授权审计仍在执行之前，后置审计错误仍需对账。
- `buffered`：数据库写失败时，只在事件已落到节点私有磁盘且 fsync 后返回成功。未配置缓冲、缓冲已满或写盘失败都返回错误，不静默丢弃。

缓冲默认关闭。启用时设置 `TRPC_AGENT_AUDIT_SPOOL_DIR`，目录必须私有、非符号链接；文件为 0600。`TRPC_AGENT_AUDIT_SPOOL_MAX_RECORDS` 默认 10000，单条事件最多 256 KiB。**每个进程独占自己的持久卷目录，不要多进程共享目录**。节点每 30 秒重放最多 100 条；保留稳定 audit ID，防止“SQL 已提交但响应丢失”造成重复。崩溃残留临时文件计入容量，宁可报满也不自动删除不明文件。使用临时容器文件系统不能宣称缓冲具有节点故障持久性。

控制面短暂不可用时，只允许使用一分钟内缓存的最后有效策略；未知租户或过期缓存仍拒绝。新增 `AgentAuditPersistenceFailure` 和 `AgentModelSettlementFailure` 规则；文件已提供，不代表当前运行 Prometheus 已热加载，更不代表已接通外部通知。

修改已有租户策略：`POST /admin/tenants/policies`，需要 tenant_admin/superadmin 的对应租户写权限：

```json
{
  "tenant_id": "tutorial-tenant",
  "expected_version": 1,
  "audit_policy": {"level": "basic", "retention_days": 0, "failure_mode": "fail_closed"},
  "quota_config": {"daily_prompt_tokens": 5000000, "daily_completion_tokens": 1000000}
}
```

未提供的策略保留原值；提供的对象整体替换对应策略。先查询当前 tenant version。PostgreSQL 的更新和必需审计回执原子提交，旧版本返回冲突；Admin 不获得直接 UPDATE tenant 或 DELETE audit_log 权限。内存开发后端记录请求审计，没有跨服务事务持久性。

## 3. Knowledge 回填与校验

同 embedding/同维度的后端搬迁可以直接读取现存 chunk 的正文、metadata 和向量，**不要求历史数据已有平台文档清单，也不通过向量反推文本**。切换 embedding 模型或维度仍要从原始文档重新建索引，不属于这个复制流程。

正常 Upsert/Delete 先在 `knowledge_sync` 保存写意图、提升 app epoch，再写源端/目标端，成功后移除意图。删除保存 tombstone。进程或后端故障留下的意图可由重试或回填修复；同 tenant/app 使用 PostgreSQL advisory lock 协调。任何平台写入都会使旧迁移证明失效。

迁移到 `backfill` 状态后，提交：

```text
POST /admin/backend-migrations/backfill-knowledge
{"tenant_id":"...","migration_id":"...","operation_id":"copy-001"}
```

每个作业最多复制 50 个 chunk，每个成功 chunk 保存游标；未完成则先持久化下一作业，再完成当前作业。回填完成后自动进入 verify 并提交校验作业。重试、重复提交和 enqueue-before-ACK 崩溃使用稳定任务 ID 去重。失败写入会先修复，目标端已删除文档的陈旧 chunk 会清除。

校验逐一比较 ID、正文、名称、metadata、向量维度和数值，并对每个非零向量做检索探针：源/目标 top-1 分数应在容差内，允许同分不同 ID。向量/分数容差分别为 `1e-5` 和 `1e-4`，零向量拒绝通过。它证明迁移一致性，不等同于业务知识召回质量评估。可通过 `verify-knowledge` 路由显式提交校验；同样需要 operation_id。

`POST /admin/backend-migrations/knowledge-status` 接受 tenant_id、migration_id，返回 epoch、待修复写数、回填/校验游标和计数，不返回文档正文。原 `backend-migrations/get` 仍返回状态及最终 verification。切换为 cutover/completed 时，Repository 在同一 app 锁下核查服务器持久化证明及当前 epoch，不能通过请求里写 `passed:true` 绕过。切换期间继续双写；又发生写入后，需要重新回填/校验才能 completed。

当前固定 Qdrant 适配器忽略 metadata offset，因此代码枚举有界元数据后排序，以 ID 保存游标，**每个 tenant/app 最多支持 100000 个 chunk，超过会明确失败，不截断后宣称通过**。这是正确性优先的有界实现；更大规模应升级为原生 scroll 游标实现，避免重复枚举成本。迁移期间需固定知识配置、暂停高频知识编辑；持续写入会反复使扫描失效。检索不中断，但不能保证查询总看见原子替换的完整文档，向量索引仍是最终一致。禁止绕过平台直接修改源/目标 collection；外部写入不参与 epoch 协议。

## 4. 验证与启用

本轮本地提交：`0ab794b`（队列与水位）、`784c262`（调用指标接线）、`2ea57e4`（预算、审计及知识迁移）。未推送远端。

自动测试覆盖调用预算竞争、重复结算、流式累计用量、异常保守占用、后台模型解析/记账、审计缓冲重启/重放、租户保留期与版本冲突、历史向量回填、批次续作、删除/篡改/失效证明、Redis 所有权与 Memory 水位。独立 PostgreSQL、Redis ACL、Qdrant 测试只使用本次创建的临时容器和合成数据。

```bash
./scripts/regression.sh
# 包括独立 SQL/Redis/Qdrant、备份恢复工具链及离线告警规则验证：
TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh
```

2026-09-06 已执行上面的完整隔离回归并通过：全仓 race、lint、build、diff check；独立 PostgreSQL 权限/持久化契约与恢复测试；Redis Lua/ACL；Qdrant 历史迁移；SQL/Redis 备份恢复工具链；Prometheus 离线规则测试。末次预算汇总键保护及迁移状态显示调整后，又通过相关包 race、Redis ACL、lint 和 build。测试没有加载日常 `.env`，临时容器和合成数据已按所有权检查清理。

实际启用前：备份日常数据库和 Redis；停止旧 Worker/Jobs；使用迁移账号应用 **017–020**；按最新生成器补充 SQL 函数/表和 Redis 心跳/预算权限；再构建启动新版本。已有角色不能直接重跑创建角色脚本，应审核并增补授权。仅构建新代码不需要停止旧服务。

新增表：`background_watermark`、`knowledge_sync`；`audit_log` 增加幂等 hash，新增受限 append/prune/policy-update 函数。不要直接把旧二进制滚回新 schema：旧版原始 audit INSERT 权限及旧水位/队列语义不同，应按备份和受控回滚方案处理。

本轮交付是代码及自动/隔离集成验证。真实模型/IM 端到端复验、生产账号、Kubernetes 集群、供应商账单与长期容量验证仍与[功能状态表](feature-status.md)分开记录。
