# 验收证据

Enterprise Multi-Tenant Agent Platform 的验收分为源码回归、后端集成和目标环境三个层次。本文件将功能要求映射到测试入口与验收断言；执行方法见 [验证指南](VERIFICATION.md)。

真实 Docker 后端、Telegram 与企业微信智能机器人的执行结果见 [IM 接入与部署验收报告](LIVE_IM_ACCEPTANCE.md)。

## 1. 提交包验证记录

交付包的 `verification-evidence/current-validation.log` 记录源码快照、工具链、执行时间、命令输出及退出码，覆盖以下检查：

```powershell
go test -buildvcs=false -count=1 -p 1 ./...
go vet -p 1 ./...
go test -buildvcs=false -race -count=1 -p 1 ./...
go run -buildvcs=false ./cmd/demo
```

全量常规测试覆盖未使用 `integration` 构建标签的测试；本地模型、HTTP/MCP 服务和内存后端由测试创建。[故障演示](JUDGE_QUICKSTART.md#2-两分钟故障演示)使用 `MemoryStore`。真实后端、镜像与目标部署的结果分别记录，注明提交和环境。

项目 CI 对提交分支执行源码、race、四后端集成、镜像及漏洞检查；Windows job 构建 ZIP，Linux job 解压该 ZIP，校验 SHA-256、Shell 执行位和 LF 行尾，再运行包内静态检查与全量常规测试。交付证据记录源码基线、文件哈希与实际运行范围；CI 结果仅对应其 `headSha`，本地未提交修改以源码清单和本地验证记录核对。

## 2. 功能与回归断言

| 功能 | 实现与测试入口 | 验收断言 |
|---|---|---|
| 多租户控制面 | `pkg/tenant`、`pkg/controlplane`、`cmd/admin` | Tenant/App/Version/Deployment 按租户授权；更新使用乐观锁；发布版本不可变 |
| 企业微信适配 | `pkg/channel/wework_adapter.go` 及适配器测试 | URL challenge、签名、AES 解密、CorpID、文本规范化和回复分段符合协议 |
| 企业微信完整链路 | `test/integration/wecom_callback_e2e_test.go` | 两种交叉 Session/Memory 组合各覆盖正常回复、token 失效刷新和模型取消；检查去重、工具、持久化、trace 与排空 |
| Telegram 适配 | `pkg/channel/telegram_adapter.go` 及适配器测试 | webhook secret 校验、会话标识、分段、429 和 Retry-After 处理正确 |
| 可靠消息 | `pkg/reliable`、`pkg/pipeline` | 重复入站幂等；不同 payload 冲突；同 Session FIFO；过期租约接管后拒绝旧 fence |
| 公平调度 | `pkg/reliable`、`test/integration` | 新租户加入、空闲恢复和策略重置保持当前活跃租户公平性；权重与并发配额生效 |
| 原子完成与投递 | `pkg/reliable`、`pkg/pipeline/delivery.go` | Inbox 完成与 Outbox 创建同事务；分段 cursor 可恢复；发送结果未知进入 reconciliation |
| Worker 执行 | `pkg/worker`、`cmd/worker` | 请求固定 AgentVersion、租户和会话身份；持有 Session lease；执行结果可重用 |
| Runtime 组合 | `pkg/worker/runtime_factory.go` 及组合测试 | LLM/Chain/Graph/Parallel/Cycle 拓扑、节点预算、工具范围和有限循环受校验；运行时注册表启动后封存 |
| SecretRef 授权 | `pkg/tenant`、`pkg/worker`、`cmd/admin` | 引用绑定 tenant、purpose、provider 和 model；发布与执行均检查作用域 |
| 租户后端生命周期 | `pkg/storage` | 后端借用和释放受控；节点 readiness 检查公共依赖，租户后端按使用范围检查 |
| 租户级交叉后端组合 | `test/integration/cross_backend_storage_test.go` | 两个生产存储适配器分别访问两租户的相反 Session/Memory 组合；验证双向可见性、作用域隔离、物理后端选择和租约释放 |
| Summary | `pkg/summary`、`pkg/summaryruntime`、`pkg/storage` | 固定事件边界、后续目标解析、Session 代次隔离、fenced CAS、取消排空和预算结算；无法证明绝对序号时返回 `ErrTranscriptIncomplete`，代次不匹配时拒绝使用摘要 |
| Knowledge | `pkg/knowledgeplane`、`pkg/knowledgeingest`、`pkg/platformtool` | 查询和记录绑定 tenant/app；导入入口限制 1 MiB/256 分片、拒绝保留 metadata、按 source 指纹幂等更新并清理旧分片；框架 Knowledge 通过运行时装配进入 Runner |
| Artifact | `pkg/artifactplane` | 元数据与正文分离；提交响应丢失不误删正文；删除失败后重试继续清理；不可变版本、SHA-256 与精确版本投影 |
| 迁移库与投影器 | `pkg/datamigration`、`pkg/dataprojection` | snapshot/catch-up 状态和 hook 调用受 lease/fence 约束；缺失 hook 时拒绝推进；projection marker 绑定 migration identity，目标应用后再次检查 fence |
| 在线迁移生产装配 | `cmd/data-migrate`、`pkg/migrationruntime`、`pkg/datamigration/live*.go`、migration 045 | 创建即捕获；源写前 intent、完整规范记录 journal、目标读回验证；未完成删除先恢复再允许重建；配置/路由/阶段/fence/审计原子切换，旧缓存跟随路由 |
| 工具治理 | `pkg/governance/plugin.go`、`pkg/governance/approval_postgres.go`、`pkg/governance/budget.go` | 工具白名单、危险操作审批、预算 reservation/settlement、输出脱敏和审计贯穿执行 |
| MCP | `pkg/platformtool/mcp.go`、`pkg/worker/mcp_runtime_integration_test.go` | 本地 Streamable HTTP 服务完成 Worker→Runner→治理→Tool→回复；检查 Header SecretRef、工具过滤和资源关闭 |
| 内部通信 | `pkg/auth`、`pkg/worker` | HMAC 绑定请求内容和 trace context；nonce 单次使用；错误请求被拒绝 |
| 可观测性 | `pkg/telemetry`、`pkg/telemetry/audit_postgres.go` | trace 跨消息边界传播；指标鉴权和标签基数受控；审计保留决策与错误类 |
| 审计持久化与级别 | `pkg/telemetry/audit_sink_test.go`、`pkg/worker/audit_outcome_test.go` | 日志镜像故障不阻断数据库；detailed 准入审计失败不执行 Runner，结果审计失败不确认成功；basic/detailed 均保留结果审计 |
| 连接池容量 | `pkg/controlplane/runtime_databases_test.go`、`pkg/datamigration/live_pool_test.go` | 总预算按职责拆分；持锁请求不耗尽依赖池；启动失败、取消和关闭归还连接 |
| PostgreSQL 容量基线 | `cmd/queue-bench`、`pkg/queuebench`、`scripts/queue_bench.ps1` | 隔离 `queuebench_*` 数据库；普通/公平领取覆盖均匀、4:1 权重、热点会话及 1/4/8/16 Consumer；JSON 校验 Inbox/Outbox 无重复、无在途 |
| 发布准入 | `pkg/releaseverify`、`deploy/kubernetes` | 镜像 digest、迁移类别、网络策略和 rollout 断言受校验 |

## 3. 真实后端集成

`test/integration` 提供 PostgreSQL、Redis、Qdrant 和 MinIO 的纵向测试，运行前按 [验证指南](VERIFICATION.md) 准备服务及测试环境变量。

| 场景 | 后端 | 关键断言 |
|---|---|---|
| 队列与执行持久化 | PostgreSQL | 事务原子性、lease/fence、重复投递、执行 reconciliation |
| 双租户交叉后端组合 | Redis + PostgreSQL；`cross_backend_storage_test.go` | A 使用 Redis Session/PostgreSQL Memory，B 使用相反组合；两个独立适配器双向读取，验证同逻辑 ID 与 actor 隔离、未选择后端无数据、关闭等待活跃租约 |
| 企业微信回调到回复 | Redis + PostgreSQL；`wecom_callback_e2e_test.go` | 两种后端组合各运行正常回复、42001 刷新重试、活动模型 context 取消三个场景；真实 Inbox/Outbox、Runner/Memory Tool，重复回调不重复执行 |
| Summary→Runner | PostgreSQL + Redis + 本地捕获模型 | checkpoint 的摘要进入下一次请求，已覆盖历史被裁剪，新消息保留 |
| Session 迁移库 | Redis + PostgreSQL | 测试自建 journal producer 与迁移 hooks；State/Event/Track 复制并追平后在目标读回，由测试 hook 执行配置 CAS 切换 |
| Session 在线迁移 | Redis + PostgreSQL；`online_session_migration_test.go` | 生产装饰器捕获迁移前已有 Session、校验与回滚窗口增量；失败删除先恢复再重建；目标不可用时回滚；完成后旧缓存客户端写目标 |
| Knowledge / Artifact 在线迁移 | PostgreSQL + Qdrant + MinIO；`online_dataplane_migration_test.go` | 生产捕获、真实后端 inventory 与规范记录投影；profile 路由切换、跨租户隔离、版本及 tombstone 保留 |
| Knowledge 检索与导入 | Qdrant + 本地 embedding 服务；`pkg/knowledgeingest` | 写入、搜索、tenant/app 隔离及框架注入；受控导入的边界、幂等替换、旧分片清理、取消和超量拒绝由单元测试覆盖 |
| Artifact | PostgreSQL + MinIO | save/load/list/delete、版本、内容哈希和删除标记 |
| 数据投影 | PostgreSQL + Qdrant + MinIO | Session/vector/object 重放、tombstone、失败 marker 与最终 fence |

模型与 embedding 在这些测试中使用本地协议服务，测试对象是平台数据流和后端一致性。线上模型效果与服务额度在目标账号中验证。

交叉后端存储测试使用生产 `StorageAdapter` 与 PostgreSQL execution fence，执行上下文由测试预置。企业微信完整链路测试使用 loopback 模型/IM 协议服务与 LocalClient，覆盖业务数据流；Worker HTTP admission/fence 和真实账号分别由对应测试与目标部署验收覆盖。迁移库测试使用自建 hooks，在线迁移测试调用生产装饰器与 `LiveCoordinator`；两者的断言分别列于上表。

## 4. 存储支持范围

| 数据域 | 可用后端 | 配置与所有权 |
|---|---|---|
| 控制面、Inbox/Outbox、执行记录、审计 | PostgreSQL | 平台统一管理 |
| Session/Memory | Redis 或 PostgreSQL | 运维注册 profile，租户选择已授权配置 |
| Summary job/checkpoint | PostgreSQL | 平台统一管理，Runner 读取 checkpoint overlay |
| Knowledge | Qdrant | 运维 profile，tenant/app 作用域 |
| Artifact | PostgreSQL 元数据 + S3-compatible 对象存储 | 运维 profile，本地集成使用 MinIO |

数据迁移使用 `cmd/data-migrate`，schema 迁移使用 `cmd/migrate`。在线迁移支持 Redis/PostgreSQL Session、embedding 定义与维度兼容的 Qdrant Knowledge、S3/MinIO Artifact；Session 准入检查 shared state、SDK native summary、TTL 和数据规模，VALIDATE/READ_SHADOW 比较完整 inventory 与规范记录。配置矩阵见[多后端设计](MULTI_BACKEND_DESIGN.md)，准入拒绝项、Memory 迁移和查询质量抽样的扩展要求见[迁移运行指南](ONLINE_MIGRATION.md)。

部署验收还须确认所有写入副本版本一致、profiles 不可变且均使用平台入口。活跃迁移按租户数据域串行化，需测量全量校验的 gate 阻塞与同步镜像的目标延迟、故障影响，包括源已提交但调用返回错误的情况。回滚窗口应保持读目标、写源并镜像目标；complete 最终验证后读写目标、停止镜像、保留源数据。实际负载与恢复证据按第 5 节收集。

## 5. 目标环境验收

部署到目标环境时，按 [验收 Runbook](EXTERNAL_ACCEPTANCE_RUNBOOK.md) 执行下列检查并记录结果：

| 验收项 | 所需条件 | 验收输出 |
|---|---|---|
| 企业微信 / Telegram 实际收发 | 测试账号、应用凭据、公网 HTTPS 回调 | 入站、执行、出站和 trace 对应记录 |
| 目标 Kubernetes / service mesh | 集群、镜像仓库、身份与网络配置 | rollout/rollback、Ready、镜像 digest、身份 allow/deny |
| 身份与密钥系统 | 目标 OIDC/IAP、KMS/Vault、ServiceAccount | 最小权限、错误身份拒绝、密钥轮换和审计 |
| HA 与灾备 | 数据库/Redis 测试实例、备份与恢复窗口 | failover、PITR、RPO/RTO、backlog 恢复和备份还原记录 |
| 业务容量与模型服务 | 目标规格、业务 payload、模型账号 | 吞吐、p50/p95/p99、queue lag、错误率、资源与成本 |
| 在线迁移运行与恢复 | 全部写入副本升级、相同不可变 profiles、空目标租户命名空间、实际容量与故障窗口 | 创建时捕获、故障 intent 恢复、全量比对门禁、目标延迟与 gate 阻塞、cutover/rollback/complete、旧缓存路由和源数据保留 |
| 业务 MCP | 已授权 profile、认证和出网配置 | 工具授权、超时、幂等、配额和实际返回结果 |

记录只收集配置版本、时间、状态、脱敏标识和哈希；凭据、聊天正文、数据库 URL 与证书私钥不进入证据包。CI 证据由 `scripts/ci_evidence.py` 记录同一源码 SHA、运行号、工具链、逐项命令、退出码和日志哈希；Windows 打包先验证三个门禁任务的证据，再把验证清单放入归档。
