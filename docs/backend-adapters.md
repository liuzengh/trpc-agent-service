# 多后端适配方案

## 1. 适配目标

平台按数据语义拆分 Control Plane Store、Session Store、Memory Store、Knowledge Store、Artifact Store 和 Governance/Audit Store，而不是定义一个万能 KV。`DataStore` 是当前 Storage Router 组合 Session 与 Memory 的便利接口，不要求所有数据最终落在同一产品。每个端口方法都必须携带 Tenant 范围；Backend Selection 只引用服务端登记的 adapter/config，不接受客户端传入任意 DSN。

## 2. 后端职责矩阵

| 后端 | 适合存储 | 一致性与访问方式 | 当前交付状态 |
| --- | --- | --- | --- |
| PostgreSQL | 控制面、Session Event、Lease、权威 Memory、Artifact/Knowledge 元数据、共享 Audit Event | 事务强一致；组合主键、唯一幂等键、row lock 和 fencing 校验 | Control Plane、Session/Memory、元数据、Lease、Audit 已实现；Compose 默认 |
| SQLite | 单节点控制面、开发环境 Session/Memory 和本地验证数据 | 单文件事务；适合单进程，不承担多 Gateway 协调 | 已实现开发适配器 |
| Redis | 热 Session/Memory、Knowledge/Artifact metadata、短期去重 | 单 key 原子；跨 key 事务和持久恢复较弱，持久性取决于配置 | Session/Memory/Knowledge/Artifact metadata 已实现 |
| InMemory | 单元测试、fixture、无外部依赖的本地演示 | 仅进程内，重启丢失，不提供跨节点一致性 | 已实现，仅测试/开发 |
| Qdrant/Milvus | Knowledge 与 Memory 的语义向量索引 | 最终一致的派生索引；按 Tenant collection/partition 和 metadata 双重隔离 | Qdrant 已接入；Milvus 保留适配边界 |
| S3 兼容对象存储 | Artifact 大对象、附件、知识源文件 | 对象写入并校验 checksum 后发布 metadata | Artifact 内容已接入；知识源文件和孤儿回收待扩展 |

## 3. Storage Router 与租户选择

Control Plane 保存每个 Tenant 的 Backend Selection，服务端 Backend Registry 根据 `backend` 和受控 `address/config ref` 延迟创建适配器。一次请求获得 Store lease 后使用同一实例，配置切换会把旧实例标记 retiring；只有活跃请求归还后才关闭资源，避免在途操作访问已关闭连接。

已支持的基础运行时选择是 `inmemory`、`redis`、`sqlite` 和 `postgres`。命名 Backend Profile 可分别指定 Session/Summary、Memory、Knowledge metadata、Artifact metadata，并叠加 Qdrant 向量索引、S3 对象内容或 Mem0 外部 Memory。创建失败或后端不可达时注册 unavailable store，使 API 返回明确的 `storage_unavailable`，而不是静默回退到默认后端。多 Gateway 下 Backend Selection 从共享 PostgreSQL Control Plane 刷新 revision，不能依赖各进程的旧本地配置。

Backend Selection 是租户级路由，但数据种类仍可以进一步拆分。例如生产 Tenant 可使用 PostgreSQL 保存 Session Event 和权威 Memory，Qdrant 保存派生向量，S3 保存 Artifact 内容；SQL 中保留向量 generation、对象引用、checksum 和发布状态。

服务端通过 `TRPC_BACKEND_PROFILES` 指向 JSON 文件。管理 API 只返回并选择 profile ID，文件内地址与环境变量名不会返回浏览器：

```json
{
  "production-data": {
    "backend": "postgres",
    "address": "postgres://service-owned-dsn",
    "memory": {"backend": "redis", "address": "redis://service-owned-address"},
    "knowledge": {"backend": "postgres", "address": "postgres://service-owned-dsn"},
    "artifact": {"backend": "postgres", "address": "postgres://service-owned-dsn"},
    "object": {
      "bucket": "tenant-artifacts",
      "region": "ap-guangzhou",
      "access_key_env": "S3_ACCESS_KEY_ID",
      "secret_key_env": "S3_SECRET_ACCESS_KEY"
    },
    "vector": {
      "backend": "qdrant",
      "host": "qdrant.internal",
      "port": 6334,
      "tls": true,
      "api_key_env": "QDRANT_API_KEY",
      "generation": "knowledge-v2",
      "dimensions": 1536,
      "embedding_model": "text-embedding-3-small",
      "embedding_url": "https://api.openai.com/v1",
      "embedding_key_env": "OPENAI_API_KEY"
    },
    "external_memory": {"host": "https://api.mem0.ai", "api_key_env": "MEM0_API_KEY"}
  }
}
```

共享 Audit Store 使用 `TRPC_AUDIT_POSTGRES_DSN`。Audit Event 追加时按 `(tenant_id, audit_id)` 幂等写入，查询始终带 Tenant 条件；治理策略、确认状态、预算和 Trace 仍由 Governance Center 的既有持久化负责。

Framework Runtime 每次执行使用 request-scoped 上游 Session；共享上下文来自平台 Session Event、Summary、Memory 和 Knowledge。上游默认 InMemory Session 因此不会让执行结果依赖请求落到哪个 Worker，也不会在重试时重复累积平台已注入的摘要。

## 4. SQL：权威事实与协调

PostgreSQL 适合需要事务、唯一约束、审计和跨节点协调的数据。Session Event 使用 `(tenant_id, session_id, sequence)` 主键和 `(tenant_id, session_id, idempotency_key)` 唯一约束；Session Lease 使用 `(tenant_id, session_id)` 主键并保存 owner、fencing token 和数据库时间 `expires_at`。控制面配置以 revision 防止并发覆盖。

Artifact/Knowledge 的大内容不应直接无限增长在 SQL 中，但 metadata、状态、checksum、对象引用和 index generation 应保留在 SQL，作为发布与恢复的判断依据。PostgreSQL 的代价是连接数、热点 Session row、存储膨胀和维护成本，需要连接池、按 Session 分散热点、PITR 和表增长监控。

SQLite 复用相同 Session/Memory 语义，适合开发和单节点演示。它不用于多 Gateway Lease，也不作为 Compose 共享控制面；从 SQLite 升级到 PostgreSQL 采用有 checkpoint 和 checksum 的迁移任务。

## 5. Redis：低延迟热数据

Redis 适合低延迟 Session/Memory、短期 message 去重、限流窗口和预算计数。事件 append 必须通过原子命令或脚本同时完成幂等检查和 sequence 分配，Tenant ID 必须进入 key。Redis 不应在默认配置下成为唯一审计事实源，也不能用进程缓存替代其提交结果。

若 Redis 被选作权威 Session/Memory 后端，应启用合适的 AOF/RDB 和副本策略，并明确可接受的数据丢失窗口。Redis 故障时请求失败，不自动写入 InMemory；迁往 SQL 时按 Session sequence 复制并双读校验。

## 6. 向量库：可重建派生索引

Qdrant/Milvus 适合 Knowledge 与长期 Memory 的语义检索，不适合作为权威值来源。权威内容先进入 SQL/Redis 或对象存储，再通过 outbox 异步切片、embedding 和 upsert。检索时向量结果只扩展候选，最终内容仍回查权威存储。

向量记录使用包含 Tenant、App、source、chunk 和 embedding version 的确定性 ID；Tenant 同时进入物理 collection/partition 路由与 metadata filter。索引记录 generation、checkpoint、embedding model、维度和切片版本，可整代重建并原子切换。服务中断只导致检索暂时不完整，不删除权威数据。

## 7. 对象存储：大内容与附件

S3 适合 Artifact 二进制内容、IM 附件和 Knowledge 原始文件。对象 key 至少包含 Tenant 和不可猜测 ID，不直接使用用户文件名作为授权边界。当前 Artifact 适配器写入确定性名称的版本对象，回读校验 checksum 后才发布 metadata；读取只允许访问 published 且 Tenant 匹配的引用。

生产部署需要启用对象版本、生命周期规则和孤儿回收。当前 metadata 写入失败会留下不可达版本，须由生命周期或后续扫描清理；metadata 已发布但对象不可读或 checksum 不符时返回恢复错误并拒绝虚假内容。备份和恢复以对象版本配合 PostgreSQL PITR 的一致时间点为准。

## 8. 成本、运维与选型建议

| 场景 | 推荐组合 | 原因 |
| --- | --- | --- |
| 单元测试 | InMemory | 快速、确定、无外部依赖 |
| 单节点开发 | SQLite，必要时 Redis | 安装简单，可验证 SQL/Redis 适配行为 |
| 比赛 Compose | PostgreSQL + Redis | 展示共享控制面、Lease/fencing 和租户后端选择 |
| 生产会话与审计 | PostgreSQL 为事实源，Redis 做热数据 | 事务与恢复语义明确，同时降低热点读取延迟 |
| 生产知识检索 | PostgreSQL/S3 权威内容 + Qdrant/Milvus 派生索引 | 大内容成本可控，索引可重建和换代 |
| 生产 Artifact | S3 内容 + PostgreSQL metadata | 避免 SQL 大对象膨胀，保留事务发布状态 |

监控按后端区分：SQL 关注连接池、锁等待、事务延迟、PITR；Redis 关注内存、淘汰、复制延迟和持久化；向量库关注 checkpoint lag、失败队列和召回质量；S3 关注上传失败、checksum mismatch、临时对象和 `recovery_required`。在线切换与迁移细节见[数据同步与幂等策略](data-sync-idempotency.md)。
