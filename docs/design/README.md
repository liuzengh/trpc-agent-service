# trpc-agent-service 多租户多节点技术方案

本目录是 `trpc-agent-service` 的规范性技术方案入口。文档描述当前系统的稳定结构和行为约束；代码可以调整内部组织，但不得改变这里定义的可信边界、原子性、版本固定和幂等语义。

## 文档导航

| 文档 | 负责回答的问题 |
|---|---|
| [总体架构](0.architecture.md) | 租户模型、组件拓扑、核心链路、进程职责及异常恢复 |
| [控制面领域模型](1.tenant-root-design.md) | Tenant、Agent App、Config、Model/Backend Profile 的版本、发布、回滚与运行时绑定 |
| [Gateway 与 Worker](2.gateway-worker-design.md) | 请求如何可靠派发，Worker 如何执行、取消、续租和故障接管 |
| [Storage 与数据一致性](3.storage-data-consistency-design.md) | 权威数据、原子事务、对象生命周期、多后端路由与迁移协议 |
| [Channel、治理与安全](4.channel-governance-security-design.md) | Feishu、WeCom、WebUI 如何进入统一 Channel 主链，以及权限、密钥、媒体和投递约束 |
| [Observability、Audit 与 DevOps](5.observability-audit-devops-design.md) | trace、指标、审计、健康、发布、容量与故障处置 |
| [模块边界与上游集成](6.module-boundaries.md) | 服务内部模块依赖、`trpc-agent-go` 公共能力复用和禁止依赖 |
| [验证与发布规范](7.verification-release-design.md) | 单元、契约、集成、故障恢复、Provider 和发布门禁如何组合 |
| [能力与兼容边界](8.capability-boundaries.md) | 生产角色、Channel、协议和明确不支持能力的边界 |
| [任务书可追溯矩阵](9.requirement-traceability.md) | 任务要求、设计、代码与验收证据的一一映射 |

## 推荐阅读顺序

```text
总体架构 / 模块边界
→ Channel：外部事件如何成为可信请求
→ Gateway/Worker：请求如何调度与执行
→ Storage：哪些状态成为权威事实
→ Channel：结果如何可靠投递
→ Observability：如何观测、发布和恢复
→ 能力边界 / 验证规范
```

## 模块划分

- A：多租户、多节点 Runtime，包括 Gateway、Worker、Broker、协调与业务 Relay。
- B：Storage 与数据一致性，包括 Session、Messaging、Artifact、Knowledge、Inbox/Outbox 与迁移。
- C：Channel、治理与安全，包括 Feishu、WeCom、WebUI、Policy、Secret 和 Delivery。
- D：Observability、Audit 与 DevOps，包括遥测、健康、部署、容量和 Runbook。

模块是架构和依赖边界，不表示人员或项目阶段。

## 全局不变量

1. `TenantContext` 只能由服务端根据认证凭据、已验证 Channel Binding 或管理面身份构造，不能信任客户端提交的 `tenant_id`。
2. 所有持久化键、缓存键、消息、日志和审计事件都显式携带 `tenant_id`；存储层必须再次校验租户作用域。
3. Worker 无状态且不依赖 sticky session；Session、Artifact、执行状态和回复状态均由共享后端保存。
4. Broker 分片只控制同 Session 消息的可见顺序；真正的互斥由 lease 和单调 fence 保证。
5. `input_seq` 按 `PrepareDispatch` 成功提交顺序分配，不按 callback 时间、Inbox claim 或 request ID 排序。
6. event、state、终态、`next_input_seq` 和 Reply/Wakeup Outbox 必须由 `CommitTurn` 原子提交。
7. 消息传输采用 at-least-once；业务效果依靠 Inbox、Commit 和 Delivery Ledger 幂等。
8. 配置、消息和审计只携带 `SecretRef`；Secret 明文不得进入日志、trace、数据库业务字段或错误响应。
9. 后端缺少租户隔离、原子提交、lease/fence 或版本条件写能力时，对应分布式角色必须 fail closed。
10. `trpc-agent-service` 只依赖上游公共 API，不导入其他 module 的 `internal` 或产品运行时包。
11. `PrepareDispatch` 成功才表示执行系统接受任务；仅完成 `ClaimInbox` 的消息仍需经过租户状态和预处理门禁。
12. Tenant 状态变化、配置发布、审计事实和缓存失效/取消 Outbox 必须在同一事务提交。
13. `agent_app_revision` 是 Agent 定义的唯一发布版本；运行时 Profile 只是固定 App、Config 和 Policy 的投影。
14. 发布后的 Revision、拓扑以及 Tool/Skill/Knowledge 子记录不可修改；回滚通过发布新的活动引用完成。
15. 固定 App Revision、Config 和 Policy 版本由 `PrepareDispatch` 生成，并由 Worker 从权威 TaskStore 复核。
16. 公开 Channel route、账号提示和 callback payload 在验签前不得暴露 Tenant、App 或 SecretRef；验签成功后才能建立可信租户上下文。
17. Secret 解析必须绑定 tenant、subject、purpose、resource 和 generation；生产入口不得提供无 scope 的通用 Secret 读取。
18. ModelProfile 与 BackendProfile 是租户内不可变、严格 schema、无密钥的版本化对象，不得取代 Agent Revision。
19. 稳定业务 `request_id` 只能由 durable claim 建立；重复 callback 必须收敛到同一请求。
20. 所有协议 façade 都必须进入共享 Inbox、Task、Session 和 Event Store，不得创建本地 Runner 或 InMemory 权威旁路。
21. Knowledge、Skill、Model、Tool 和 Backend 的运行引用必须固定 tenant、version 和 digest。
22. Plugin、callback 和 telemetry hook 只能扩展或观测；mandatory Guard、CommitTurn 和 Audit Outbox 位于不可绕过的外层。
23. 合规审计事实默认不可变；销毁是唯一例外，只能经 `compliance_purger` 角色门禁的受控 purge 函数执行并留下不可变销毁凭证；审计查询逐次落不可变二次审计记录。

## 代码映射

| 能力 | 主要代码区域 |
|---|---|
| Tenant、Config、Agent App | `trpcservice/tenant/`、`trpcservice/config/`、`trpcservice/agentapp/` |
| Gateway、Worker、Broker、协调 | `trpcservice/gateway/`、`trpcservice/worker/`、`trpcservice/broker/`、`trpcservice/coordination/` |
| Session、Messaging、Artifact、ObjectStore | `trpcservice/storage/`、`trpcservice/preprocess/` |
| Channel 与投递 | `trpcservice/channels/`、`trpcservice/relay/` |
| Runtime Profile 与 Provider | `trpcservice/profile/`、`trpcservice/provider/`、`trpcservice/agent/` |
| Secret、治理、审计与健康 | `trpcservice/secrets/`、`trpcservice/governance/`、`trpcservice/audit/`、`trpcservice/health/` |
| 审计查询与保留销毁 | `trpcservice/audit/query/`、`trpcservice/audit/purge/`、`trpcservice/audit/purgebusiness/`、`compliancemigrations/` |
| 后端迁移与 Knowledge 摄取 | `trpcservice/migration/`、`trpcservice/storage/knowledge/` |
| 生产组合入口 | `cmd/trpc-service/`（含 `business-audit-purge`）、`cmd/audit-purge/`、`cmd/compliance-migration-test/` |

## 文档维护规则

- 修改 Tenant、ExecutionEnvelope、CommitTurn、ChannelEvent、ReplyEvent、Secret scope 或 AuditEvent 时，必须同步检查所有消费方。
- 兼容性变更增加 `schema_version`；破坏性变更必须定义双读/双写、切换和回滚窗口。
- 数据库文档描述逻辑表、约束和状态机，不引用迁移文件序号。
- 不支持能力必须以明确边界描述，不使用“以后补”“下一轮实现”等过程性措辞。
- Runbook 只记录部署与处置动作；验证报告、提交记录和项目排期不进入规范性设计。
