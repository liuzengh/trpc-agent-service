# PostgreSQL Schema 迁移说明

本目录说明 `migrations/` 中 SQL 的职责和执行边界，便于评审者从数据库结构反查多租户、消息恢复、治理和多后端能力。这里的迁移管理平台自身的 PostgreSQL schema；Redis、Qdrant、S3-compatible 等外部后端的数据搬迁流程见[多后端与迁移](storage-migrations.md)。

## 执行模型

生产部署通过 `trpc-service --migrate-only` 执行 schema 迁移，Compose 和 Kubernetes 都把它作为独立于 Gateway/Worker 的一次性任务。运行时 Adapter 关闭自动建表，业务 Pod 不负责修改 schema。

`migrations/embed.go` 按编号嵌入全部 Up SQL，迁移命令在一个 PostgreSQL 事务中取得 advisory lock 后按顺序执行。当前实现没有单独的 `schema_migrations` 版本表，而是依靠 `CREATE ... IF NOT EXISTS`、`ADD COLUMN IF NOT EXISTS`、条件更新和约束检查保证整组脚本可重复执行：

- 新数据库执行 `000001` 至最新版本，得到完整 schema；
- 已有数据库同样执行整组脚本，已存在对象保持不变，新增列、索引和约束完成升级；
- 任一步失败会回滚整个迁移事务，Gateway/Worker 不应在 migration Job 成功前发布；
- advisory lock 防止两个 migration Job 同时修改 schema。

这种方式适合当前交付规模，但后续多人长期维护时，可替换为 Goose、golang-migrate 等带版本表和 dirty-state 管理的迁移工具。替换前不能改变现有 SQL 的执行顺序和升级语义。

## 文件职责

| 编号 | 文件 | 数据库对象与用途 | 保留原因 |
| --- | --- | --- | --- |
| 000001 | `control_plane` | Tenant、Config Version、Agent App、Channel Binding、Identity、Session Head、Event、Summary、Memory、Inbox、Outbox、Audit、Migration Job | 平台控制面和租户数据模型基线 |
| 000002 | `message_runtime` | Inbox 稳定 ID、配置版本、session 顺序、claim/lease、Outbox 来源约束、Derived Job | IM 去重、顺序提交和跨节点接管 |
| 000003 | `persistent_runtime` | tRPC-Agent-Go PostgreSQL Session/State/Event/Summary/Memory 表，以及 Artifact、Knowledge 元数据 | 无状态 Worker 的共享运行时存储 |
| 000004 | `inbox_recovery` | 待处理、重试和 lease 到期 Inbox 的部分索引 | 避免恢复扫描退化为全表扫描 |
| 000005 | `outbox_delivery` | Outbox claim/lease、发送完成时间和投递索引 | 多 Delivery Worker 竞争消费、重试和 DLQ |
| 000006 | `cluster_control` | Run Status、Worker Node、预算使用/预留、Tool Approval | 跨节点状态、取消、容量和治理 |
| 000007 | `storage_migrations` | 迁移配置版本、checkpoint、checksum、claim token 和目标端幂等 ledger | Redis/PostgreSQL 等后端迁移和断点续传 |
| 000008 | `external_storage_catalog` | S3 Artifact 逻辑目录、Knowledge 文档名称 | 不列举或泄漏对象 key 的跨后端迁移 |
| 000009 | `audit_versions` | Audit 的 config/policy version 及查询索引 | 把审计决定关联到不可变配置版本 |
| 000010 | `execution_recovery` | Inbox 的 `runner_committed`、`derived_committed`、`outbox_committed` 阶段及已保存回复 | Worker 接管后不重复已持久化的 Runner 结果 |
| 000011 | `tool_executions` | Tool 调用状态、参数摘要、幂等键和核对信息 | MCP/HTTP Tool replay 与未知结果人工对账 |
| 000012 | `tool_execution_ciphertext` | 为 Tool ledger 增加加密结果 | 兼容已执行旧 `000011` 的数据库，不能回写历史迁移代替升级 |

## Up、Down 与升级规则

- `*.up.sql` 是部署所需的前向迁移，必须按编号保留。
- `*.down.sql` 主要用于临时数据库的 `up → down → up` 验证和经过审批的灾备操作；Down 可能删除数据，不能作为日常生产回滚方式。
- 已进入共享或生产环境的历史 Up 文件不得原地修改；结构变化必须增加新的编号。
- `000012_tool_execution_ciphertext` 是这一规则的示例：旧 `000011` 已经创建 `tool_executions`，新字段必须通过后续 `ALTER TABLE` 添加。
- 应先运行 migration Job，再发布 Gateway/Worker；失败时保留旧镜像和旧配置版本，不执行自动 Down。

## 验证

`scripts/postgres_migrations_test.sh` 在临时 PostgreSQL 16 中验证：

1. 逐个执行到旧 `000011`，确认加密结果列尚不存在；
2. 执行 `000012`，确认已有数据库可以前向升级；
3. 重复执行完整 Up，确认幂等；
4. 校验关键表、索引和列；
5. 执行 Down 后重新 Up，确认空库可以重建；
6. 运行 Inbox/Outbox、恢复、配置、审计、fencing 和 Redis/PostgreSQL Session 迁移集成测试。

执行入口：

```bash
./scripts/postgres_migrations_test.sh
```

该脚本只操作自己创建的临时容器和数据库，不应指向生产 DSN。
