# 数据模型设计

PostgreSQL 是控制面和会话事实数据的系统记录（system of record）。所有业务主键、唯一键、外键路径和运维索引都以 `tenant_id` 开头；进程内测试替身不属于生产存储选项。

## 实体关系

```text
tenant -> config_version
       -> agent_app -> session_head -> message_event
                    |                -> session_summary(cutoff_event_seq)
                    -> memory_entry(source_event_seq, stable memory_id)
channel_binding -> identity_mapping
                -> inbox_message -> outbox_message
tenant -> audit_log
tenant -> migration_job
tenant -> run_status -> cancel intent
tenant -> policy_budget_usage -> policy_budget_reservation
tenant -> tool_approval
worker_node
```

`tenants.current_config_version` 是配置发布头。发布时锁定租户记录并比较 expected version，只有一个并发请求能够成功；`config_versions` 写入后不可修改。回滚会复制旧 payload 并创建新版本，不覆盖历史记录。配置正文只保存 `SecretRef` 的 provider/key，不保存模型 API key、IM token 或数据库密码。

## 最小逻辑表

下表省略了通用时间戳和部分错误字段。实际 DDL 以 [`000001_control_plane.up.sql`](../migrations/000001_control_plane.up.sql)及后续增量迁移为准；每个迁移文件的职责、执行顺序和升级规则见[数据库 Schema 迁移说明](database-migrations.md)。

| 表 | 最小字段 | 主键、唯一键与用途 |
| --- | --- | --- |
| `tenants` | `tenant_id`, `name`, `enabled`, `current_config_version` | PK `tenant_id`；配置发布头在此做 CAS |
| `config_versions` | `tenant_id`, `version`, `config_yaml`, `config_sha256`, `status`, `rolled_back_from`, `created_by` | PK `(tenant_id, version)`；版本不可原地修改 |
| `agent_apps` | `tenant_id`, `app_id`, `name`, `enabled`, `config_version` | PK `(tenant_id, app_id)`；关联同租户配置版本 |
| `channel_bindings` | `tenant_id`, `app_id`, `binding_id`, `channel_type`, `provider_account_id`, `config_version` | PK `(tenant_id, binding_id)`；账号在租户/通道内唯一 |
| `identity_mappings` | `tenant_id`, `binding_id`, `external_user_id`, `internal_user_id` | PK `(tenant_id, binding_id, external_user_id)` |
| `session_heads` | `tenant_id`, `app_id`, `user_id`, `session_id`, `last_event_seq`, `last_fence`, `state_version`, `state_json` | PK `(tenant_id, app_id, user_id, session_id)`；保存顺序和 CAS 坐标 |
| `message_events` | `tenant_id`, `app_id`, `user_id`, `session_id`, `event_id`, `inbox_id`, `event_seq`, `event_type`, `payload_json`, `state_delta_json`, `trace_id` | PK 以 session 和 `event_seq` 组成；`event_id`、`inbox_id` 在租户内唯一 |
| `session_summaries` | `tenant_id`, `app_id`, `user_id`, `session_id`, `summary_version`, `cutoff_event_seq`, `content`, `metadata_json`, `status` | version 与 cutoff 分别唯一，用于摘要 CAS |
| `memory_entries` | `tenant_id`, `app_id`, `user_id`, `memory_id`, `source_session_id`, `source_event_id`, `source_event_seq`, `version`, `content`, `metadata_json`, `status` | 稳定 memory ID；source event 在用户/App 作用域内唯一 |
| `inbox_messages` | `tenant_id`, `binding_id`, `external_message_id`, `inbox_id`, `inbox_seq`, `session_id`, `config_version`, `status`, `attempts`, `next_attempt_at`, `trace_id`, `execution_stage`, `execution_reply`, `execution_event_id` | 外部消息唯一键吸收 IM 重投；session seq 唯一；保存 Runner/派生/Outbox 可恢复进度 |
| `outbox_messages` | `tenant_id`, `outbox_id`, `dedupe_key`, `binding_id`, `session_id`, `source_inbox_id`, `source_event_id`, `status`, `attempts`, `retry_at`, `payload_json` | PK `(tenant_id, outbox_id)`；UNIQUE `(tenant_id, dedupe_key)`；trace 通过来源事件和 payload 关联 |
| `audit_logs` | `tenant_id`, `audit_id`, `channel`, `user_id`, `session_id`, `agent_name`, `tool_name`, `decision`, `latency_ms`, `error_type`, `cost_micros`, `config_version`, `policy_version`, `trace_id` | PK `(tenant_id, audit_id)`；按 tenant/trace/time 查询；成本和治理决定可追溯到不可变配置/策略版本 |
| `migration_jobs` | `tenant_id`, `job_id`, `app_id`, `config_version`, `domain`, `source_backend`, `target_backend`, `source_route_hash`, `status`, `checkpoint_json`, `source_rows`, `copied_rows`, `attempts`, `lease_owner`, `claim_token`, `lease_until`, `last_error_type` | PK `(tenant_id, job_id)`；同配置/App/domain 唯一；claim 与 checkpoint 支持跨节点断点续传 |
| `storage_migration_items` | `source_route_hash`, `table_name`, `source_key`, `checksum`, `copied_at` | 目标端幂等 ledger；与目标数据行同事务提交，避免 checkpoint 丢失造成重复复制 |
| `run_statuses` | `tenant_id`, `binding_id`, `request_id`, `status`, `worker_id`, `cancel_requested`, `reply`, `error` | PK `(tenant_id, request_id)`；跨节点状态查询与持久化取消意图 |
| `worker_nodes` | `node_id`, `started_at`, `last_heartbeat`, `draining`, `stopped_at` | PK `node_id`；拒绝活跃节点 ID 冲突并记录 drain/liveness |
| `policy_budget_usage` | `tenant_id`, `period`, `used_micros` | PK `(tenant_id, period)`；跨节点月度预算原子累计 |
| `policy_budget_reservations` | `tenant_id`, `request_id`, `period`, `reserved_micros`, `actual_micros` | PK `(tenant_id, request_id)`；请求重试时幂等预留与核销 |
| `tool_approvals` | `tenant_id`, `request_id`, `tool_name`, `approved_at` | PK `(tenant_id, request_id, tool_name)`；跨节点人工批准 |
| `tool_executions` | `tenant_id`, `request_id`, `tool_call_id`, `tool_name`, `arguments_hash`, `idempotency_key`, `status`, `result_hash`, `error_type`, `trace_id` | 记录 MCP/HTTPS Tool 的 running/completed/failed/outcome_unknown；租户/请求/调用唯一，支持副作用核对 |

表之间不使用裸 `app_id`、`session_id` 或外部用户 ID 关联。应用、绑定、会话和事件的外键路径都带 `tenant_id`，Repository 方法也显式接收租户参数，因此两个租户可以安全使用相同的业务 ID。

## 事件、状态和投影

`message_events` 是平台会话事实流，`session_heads` 保存当前序号、fencing token 和 state 版本。一次 turn 在同一 PostgreSQL 事务中校验 `last_fence`、推进 head、追加 event 并更新 state。只有 `inbox_seq = last_event_seq + 1` 才能提交，乱序请求退避后重试。

Summary、Memory、Knowledge 和 Artifact 是从已提交 event 派生的投影。Summary 使用 `(summary_version, cutoff_event_seq)` CAS，只接受更新的截断位置；Memory 使用 source event 唯一键去重。Knowledge 索引、Artifact 对象和外部 Memory 可以异步生成，但必须保存租户、来源 event 和版本，失败后从 checkpoint 重试。派生后端不可用时不会撤销已经提交的事实事件。

Inbox 和 Outbox 分别处理入站与出站幂等。Inbox 唯一键是 `(tenant_id, binding_id, external_message_id)`；Outbox 唯一键是 `(tenant_id, dedupe_key)`。发送结果不确定时记录为 `uncertain`，不能把未知结果当作普通失败立即重发。

`inbox_messages.execution_stage` 只能单调推进 `none -> runner_committed -> derived_committed -> outbox_committed`，同时保存已脱敏的最终 `execution_reply` 和稳定平台 `execution_event_id`。claim owner/token 与未过期 lease 是推进阶段的前置条件；接管后的新 claim 可以读取旧阶段并继续执行。阶段字段由 `000010_execution_recovery` 增加，不替代 status：前者描述处理内部 checkpoint，后者描述 Inbox 生命周期。

## tRPC-Agent-Go 运行时表

`000003_persistent_runtime` 固定 tRPC-Agent-Go PostgreSQL Session/Memory Adapter 的表契约，并增加 `runtime_artifacts` 与 `runtime_knowledge_documents`。`000007_storage_migrations` 扩充迁移状态并创建目标端 ledger，`000008_external_storage_catalog` 增加不含正文的 S3 Artifact 版本目录，`000010_execution_recovery` 增加 durable Runner checkpoint。平台表负责 Inbox、fencing、审计和可恢复执行；`runtime_session_*`、`runtime_memories` 保存 Runner 使用的具体状态。两组表职责不同，但都使用 canonical `app_name` 或显式 tenant/app 字段保持租户作用域。

Artifact revision 的主键是 `(tenant_id, app_id, user_id, session_id, filename, revision)`；S3 使用相同逻辑 key/revision，并由共享 PostgreSQL advisory lock 协调分配。Knowledge 通过 PGVector/Qdrant 保存 chunk、embedding 与 metadata，Runtime 只在显式允许 `knowledge_search` 时暴露检索工具；`runtime_knowledge_documents` 保存 Admin ingest 的原始文档与版本，是跨向量后端重建的租户级事实源。

运行时 Adapter 关闭自动建表，所有 schema 变化由独立 migration 命令管理。CI 会在临时 PostgreSQL 16 中验证首次 up、重复 up、down、空 schema 和再次 up。down migration 会删除数据，只能用于测试或经过审批的灾备流程，不能作为日常生产回滚手段。

## Admin 配置接口

控制面的最小 HTTP 接口是：

- `POST /v1/tenants/{tenant}/configs/validate`
- `POST /v1/tenants/{tenant}/configs/publish?expected_version=N`
- `GET /v1/tenants/{tenant}/configs`
- `POST /v1/tenants/{tenant}/configs/rollback?expected_version=N&target_version=M`

生产入口已在 Handler 外层挂载 Bearer 认证和 tenant-scoping middleware。租户范围只取自
`/v1/tenants/{tenant_id}` 路径并与管理员凭据授权范围比较，不信任请求体、Header 或查询参数
提供的 `tenant_id`；未配置管理员凭据时 fail closed。
