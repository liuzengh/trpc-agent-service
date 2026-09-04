# CRUD 落库审计（阶段 33）

> 方法：对 `trpcservice/app/web` 全部管理路由逐条追到 Manager → Store → SQL，
> 确认 create/read/update/delete 每一环真实落库/生效、删除后查询不可见。
> 审计时点：阶段 33；覆盖 `deployments/mysql/init/001~011` 现存表。

## 1. 逐资源结论

| 资源（API） | Store 落库点 | 删除语义 | 结论 |
| --- | --- | --- | --- |
| Tenant `/tenants` CRUD | tenantstore/tenant_mysql.go | **阶段 33 改为级联软删**（事务）：租户软删 + agents/endpoints(tenant)/tools(tenant)/skills(tenant)/kbs/bindings 软删 + chat_sessions/chat_messages 物理删；usage/audit/config_versions/secrets 保留（审计历史） | ✅ |
| Agent `/agents` CRUD+Publish/Rollback | agentstore/agent_mysql.go | 软删 agents（is_deleted=1，RowsAffected 校验）；agent_versions 保留（发布历史=回滚依据） | ✅ |
| Endpoint `/endpoints` CRUD | llmstore/llm_mysql.go | 软删 model_endpoints（is_deleted=1 + RowsAffected→404）；Upsert 会把同 id 复活（INSERT…is_deleted=0）——记录为已知语义 | ✅（复活语义注明） |
| Tool `/tools` list + `/grants` | toolstore/tool_mysql.go | 工具目录无删除 API（仅授权增删），无孤儿问题；IsAllowed 查 grants 表 | ✅ |
| Skill `/skills` CRUD+版本 | skillstore/skill_mysql.go | 软删 skills **事务内清理 agent_skills**（DELETE WHERE skill_id）；skill_versions 保留 | ✅ |
| Knowledge `/kbs` CRUD+文档 | knowledgestore/knowledge_mysql.go | 软删 knowledge_bases（docs 经 kb 不可见）；Milvus collection 不随删（向量遗留，运维侧处理） | ✅ |
| Channel binding `/channels` | bindingstore/binding_mysql.go | 物理 DELETE；ChannelAPI 同步 Reload 停连接 | ✅ |
| Secret `/secrets` | secret/mysql.go | 物理 DELETE；引用的模型端点/IM 绑定将无法解析（UI 已警示） | ✅ |
| Audit `/audit`、Usage `/usage` | audit/mysql | 只读+追加；不提供删除（合规保留） | ✅（设计） |
| Chat `/chat`、历史 `/history` | ledgerstore/ledger_mysql.go | chat_messages/chat_sessions 只追加（RecordTurn 幂等 by message_id），无删除 API | ✅（设计） |
| Tenant config `/config-versions` `/config-rollback` | tenantstore（tenant_config_versions） | 版本只追加；回滚=应用旧快照并生成新版本 | ✅ |

## 2. 阶段 33 修复/增强项

1. **tenant 删除升级为级联事务**（原仅软删租户主行）：见 tenantstore `Delete`；
   内存模式（无 MySQL）下各管理域内存独立，删除仅作用于租户本身——dev 语义，
   持久语义以 MySQL 级联为准。
2. 审计中发现并核实 **skill 删除已清理 agent_skills**、**各 store Delete/Get 均
   RowsAffected / is_deleted=0 过滤**（此前已具备，无新增 bug）。
3. 保留策略：usage_records / audit_logs / tenant_config_versions 在租户删除时
   不清理（成本、合规、回滚历史需存活），在 tenant Delete 事务注释中写明。

## 3. 遗留低危观察（不阻塞）

- endpoint Upsert 可使被删 endpoint 复活（同 id 重新 PUT）——upsert 语义的一部分，
  若需要严格“删除后不可重建同 id”再加唯一墓碑；当前管理 UI 不暴露同 id 重建。
- tool 目录目前无“删除工具”入口；未来若加，需在删除事务中清 `agent_tool_grants`
  （与 skill 删除同模式）。
- agent 删除不清理 channel_bindings/agent_skills（已由租户级联与 skill 删除覆盖；
  agent 级删除时 binding 保留为 agent_id 悬空，会在运行时 resolve 失败并记日志）。
