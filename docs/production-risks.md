# 生产风险清单

## 1. 评估口径

风险按影响、触发条件、检测信号、缓解措施和残余风险记录。凡是可能导致 README 验收失败、跨租户数据可见、凭据进入交付物或请求无法完成的问题，按阻塞功能缺陷处理；不影响比赛结果但需要生产化补齐的事项作为已知限制保留。详细组件设计见[架构设计文档](architecture.md)。

## 2. 风险与缓解措施

| # | 风险 | 影响 | 触发条件 | 检测信号 | 缓解措施 | 残余风险 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 同一 Session 跨节点并发写 | 历史乱序、终态覆盖、Memory 回退 | 两个 Gateway 同时处理相同 Tenant/Session，或旧节点网络分区后继续执行 | Lease 冲突、`stale_fencing_token`、同一请求多终态 | PostgreSQL Lease、定期续租、单调 fencing token、所有执行期写入原子校验 token | 数据库整体不可用时该 Session 必须暂停，不能以可用性换一致性 |
| 2 | Worker 在 Tool 副作用后崩溃 | 自动重试可能重复付款、发信或修改外部系统 | Tool 已执行但 completed 状态尚未返回 | 长时间 executing、Worker 断连、缺少 Tool 终态 | 副作用前持久化 executing；重启转 `outcome_unknown`；禁止自动 replay；人工调查和处置 | 外部 Tool 若不提供幂等键，平台无法自动证明副作用结果 |
| 3 | IM 重复或乱序投递 | 重复模型计费、重复 Tool、重复回复 | Provider 重试、长连接重连或乱序 Update | duplicate/out-of-order 计数、相同 message ID、多次 delivery attempt | provider message ID 派生 `request_id`；幂等事件唯一键；provider sequence；已有终态直接返回 | Provider 缺少稳定 ID 时只能使用内容/时间窗去重，可能误判 |
| 4 | PostgreSQL 或 Redis 暂时不可用 | 配置、Lease、Session 或 Memory 无法确认，请求中断 | 数据库重启、网络分区、连接池耗尽 | dependency health、连接错误、延迟、`control_plane_unavailable`/`storage_unavailable` | 有界超时；明确错误分类；不读取旧配置或回退 InMemory；PITR/副本和恢复演练 | 强一致路径在故障期间牺牲可用性 |
| 5 | 模型不响应或忽略取消 | 请求和关闭卡死，Worker 容量耗尽 | Provider 超时、流式连接悬挂、SDK 不响应 context | runtime timeout、活跃执行数、Worker readiness、关闭超时 | 服务端执行期限；传播 context cancellation；超时 Worker 退役；分阶段有界关闭 | 外部连接可能在操作系统级超时前占用少量资源 |
| 6 | Execution Manifest key 泄露或轮换错误 | 伪造 Worker 执行，或全部请求被拒绝 | Secret 暴露、active kid 配错、节点未同步 keyring | 未知 kid、签名失败率、过期 Manifest、配置审计 | 独立 keyring、短有效期、kid、双 key 验证窗口、Secret Manager、禁止写日志 | 紧急轮换期间可能出现短暂请求失败 |
| 7 | 跨租户资源猜测或路由错误 | 数据泄露、越权 Tool 或错误 IM 回复 | 信任客户端 tenant_id、自然 ID 查询、Bot 路由歧义 | 越权审计、Tenant mismatch、同外部主体多映射 | 服务端 Tenant Context；组合主键；统一 not found；Bot Tenant Allowlist；Tenant 进入所有存储 key/filter | 配置管理员错误仍需审批和配置审计发现 |
| 8 | 流式输出后才发现敏感内容 | 已发送 token 无法撤回，造成合规泄露 | 输出 Guardrail 需要完整上下文但系统逐 token 发布 | 输出规则命中、redaction 事件、敏感模式告警 | 严格策略启用完整缓冲后检查再发布；普通策略逐块脱敏；日志统一 Redactor | 完整缓冲增加首 token 延迟和内存开销 |
| 9 | Memory 已提交但向量索引失败 | 语义检索暂时不完整，召回旧内容 | 向量服务故障、embedding 版本或维度不匹配 | `retry_pending`、checkpoint lag、upsert 失败、shadow read 差异 | 权威记录先提交；outbox/checkpoint；指数退避；generation 重建与原子切换 | 修复期间召回质量下降，但权威 Memory 仍可读取 |
| 10 | Artifact 内容成功而 metadata 失败 | 孤儿对象、内容不可检索或假成功 | S3 与 SQL 之间的跨系统提交失败 | 临时对象增长、checksum mismatch、`recovery_required` | 临时 key、checksum、metadata 发布状态机、按 request/trace 对账、后台回收 | 对账窗口内会保留临时对象并产生额外存储成本 |
| 11 | 审计存储失败 | 无法证明危险操作的身份、策略和结果 | 磁盘满、写入失败、治理状态损坏 | Audit 写错误、事件 lag、健康检查和容量告警 | 高风险路径 fail closed；普通路径返回稳定错误；追加写、备份和恢复校验 | 审计依赖故障会主动降低高风险功能可用性 |
| 12 | schema 迁移与旧进程并存 | 查询失败、字段误读、服务无法启动 | 滚动升级中 schema 与二进制不兼容 | schema version mismatch、迁移错误、旧实例错误率 | `control-migrate` 前置；Gateway 只检查版本；expand-contract；升级全部节点后再收缩 | 跨多个版本长期混跑不受支持 |

## 3. 风险处置优先级

风险 1、2、3、4、7 和 11 直接影响会话正确性、跨租户隔离或危险副作用，发现后应阻止发布。风险 5、6、8、9、10 和 12 根据影响范围决定回滚或降级，但凭据真实泄露、敏感数据已输出、生产 schema 不兼容仍按阻塞事故处理。

上线前必须为每项风险指定指标 owner、告警阈值和处置 runbook。比赛环境使用 Stage 7 Compose 故障矩阵验证 Lease loss/fencing、Worker loss、PostgreSQL outage、模型 timeout、Tool failure、Governance outage 以及 IM retry/duplicate；生产环境还需增加密钥轮换、PITR、向量 generation 回滚和对象孤儿回收演练。

## 4. 当前已知生产限制

当前 Compose 已把 Audit Event 写入共享 PostgreSQL，两个 Gateway 的查询可立即看到同一审计事实。内部 Governance 请求仍固定使用 Gateway A；确认、执行中 Tool、预算计数和 Platform Trace 等运行态尚未全部迁入多 Gateway 共享事务存储。该限制不影响比赛拓扑的确定性验收，但生产高可用部署仍需完成其余 Governance 状态共享和内部 Service 路由。

Qdrant Knowledge 索引和 S3 Artifact 内容已提供适配及真实后端集成测试；Milvus、Knowledge 原文件、增量向量 outbox、对象孤儿自动回收和 Kubernetes 仍只有设计或扩展边界，不应以“已完成”方式对外承诺。Production Identity 当前参考实现使用 HS256 JWT 和服务端 Identity Directory，托管 OIDC、企业密钥管理、告警值班和异地容灾属于部署方上线工作。
