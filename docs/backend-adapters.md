# 多后端适配方案

## 1. 统一访问层

平台不重新定义 Agent 框架的数据接口，而是在 tRPC-Agent-Go 接口外增加路由、租户校验、指标和迁移能力。

| 数据类型 | 框架接口或能力 | 平台包装器 |
| --- | --- | --- |
| Session / State / Summary | `session.Service` | `TenantSessionRouter` |
| Memory | `memory.Service` | `TenantMemoryRouter` |
| Artifact | `artifact.Service` | `TenantArtifactRouter` |
| Knowledge | `knowledge.Knowledge`、`vectorstore.VectorStore` | `KnowledgeFactory`、`ScopedVectorStore` |
| Audit Log | 无统一框架接口 | `AuditRepository` |
| 异步任务 | `EnqueueSummaryJob`、`EnqueueAutoMemoryJob` | `DurableJobDispatcher` |

路由器不接收外部 `tenant_id` 参数，而是从内部 `storage_scope` 和可信 context 中取值，并要求两者一致。

```go
type RuntimeScope struct {
    TenantID string
    AppID    string
}

type BackendBundle struct {
    Session  session.Service
    Memory   memory.Service
    Artifact artifact.Service
}

type TenantSessionRouter struct {
    resolver BindingResolver
    pool     *BackendPool
}

func (r *TenantSessionRouter) AppendEvent(
    ctx context.Context,
    sess *session.Session,
    evt *event.Event,
    opts ...session.Option,
) error {
    scope, err := ValidateStorageScope(ctx, sess.AppName)
    if err != nil {
        return err
    }
    backend, err := r.pool.Session(ctx, r.resolver.SessionBinding(scope))
    if err != nil {
        return err
    }
    return backend.AppendEvent(ctx, sess, evt, opts...)
}
```

连接池以 `backend_binding_id + version` 为键。Secret 轮换或连接参数变化时创建新版本，旧实例等到活跃请求归零后再关闭。所有 Router 方法记录 backend type、operation、latency 和 error type，但不把 DSN、bucket credential 等写入 span。

## 2. Session 后端

### InMemory

适合单元测试、示例和本地单进程开发。它不支持多节点共享，进程退出后数据丢失，不能用于生产租户。

### Redis

适合高频会话、低延迟状态和分布式租约。当前 Redis Session 实现使用 Lua 原子提交 Event 和 StateDelta，支持 TTL、summary 和兼容迁移模式。

生产配置建议：

- 使用同步持久化，保持 `WithEnableAsyncPersist(false)`；
- 开启 AOF 或使用有持久化保证的托管 Redis；
- 读 Session 使用主节点或保证读己之写的路由；
- key prefix 包含环境和业务域，`AppName` 继续包含 tenant/app；
- 租约 Redis 与 Session Redis 可以分实例，避免大 key 或慢查询影响协调；
- 监控 Lua 延迟、key 数、内存、eviction、复制延迟和 hot key。

Redis 适合作为活跃 Session 主存储，但长期审计和复杂查询仍放 SQL。

### PostgreSQL / MySQL

适合需要事务、查询、备份和长期保存的 Session。框架 SQL 适配器在 `AppendEvent` 时锁定 Session 行，合并 StateDelta 并插入 Event。它能阻止单次写丢失，但不能代替整段 Runner 的 session 租约。

建议：

- 所有运行时读写走主库；只读管理查询可以走副本；
- 连接池按 Worker 数量设置总上限，避免扩容时打爆数据库；
- Event 表按时间或 tenant hash 分区；
- 为过期 Session 建清理索引，批量小事务删除；
- 长会话使用 Event 分页和 Summary，避免每次加载全部历史；
- 生产自定义表增加 `event_id` 唯一索引和 `event_seq`。

PostgreSQL 更适合统一控制面和运行 journal；MySQL 适合已有 MySQL 基础设施的企业环境。

### SQLite / MongoDB

SQLite 适合本地演示和单节点边缘部署。多副本共享一个文件会带来锁和存储卷问题。MongoDB 适合已有文档数据库体系、Session payload 变化较多的场景，但仍需要验证事务、索引、TTL 和分页行为。

## 3. Memory 后端

Memory 与 Session 的生命周期不同。Session 记录完整对话，Memory 保存跨会话事实或事件。`memory.UserKey` 由 `AppName + UserID` 组成，因此 Router 必须保持 tenant/app 命名空间，并明确群共享模式下的 subject。

### Redis Memory

写入和读取延迟低，适合规模较小、检索方式简单的 Memory。数据量变大后，遍历和复杂搜索能力有限。需要持久化、备份和内存配额控制。

### SQL Memory

适合事实型 Memory、软删除、版本管理、合规查询和数据导出。框架 MySQL/PostgreSQL 实现通过稳定 memory ID 和 upsert 提供幂等写入。若需要语义检索，可以将 SQL 作为真相源，异步同步到 pgvector 或独立向量库。

### 外部 Memory 服务

Mem0 或企业自建 Memory API 适合把提取、去重、检索交给专用服务。平台仍要传入 tenant/app/user scope，并保存外部对象 ID、版本和审计记录。外部服务不可用时可以降级为“本轮不加载长期记忆”，但不能错误加载其他租户数据。

### 自动 Memory 任务

框架内置 auto-memory worker 是进程内队列。生产环境由 `TenantMemoryRouter.EnqueueAutoMemoryJob` 写持久化任务，再由 Job Worker 调用 `memory/extractor.MemoryExtractor` 和目标 `memory.Service`。这样可以记录重试次数、提取水位、模型成本和失败原因。

## 4. Summary 存储

Summary 可以跟随 Session 后端存储，也可以统一放 SQL。跟随 Session 的优点是 Runner 读取简单；统一 SQL 的优点是便于检查水位、迁移和管理。

无论采用哪种方式，都要保存：

```text
filter_key
summary text
topics
high_watermark / last_event_id
updated_at
summarizer model and revision
```

异步任务只能以更高水位更新 Summary。任务队列按 Session Key 去重，同一 filter 可以合并到最高目标水位。

## 5. Knowledge 和向量库

Knowledge 包含两部分：原始文档真相和检索索引。原始文件放对象存储，文档元数据、版本和处理状态放 SQL，chunk 与 embedding 放向量库。

### Qdrant

适合独立向量检索服务，部署和过滤模型较直接。共享 collection 时，payload 必须包含 `tenant_id`、`app_id`、`knowledge_base_id`、`document_id` 和 revision。

### Milvus

适合更大规模的向量数据和复杂索引管理，运维组件更多。需要提前规划 collection、partition、索引构建、加载状态和容量。

### pgvector

适合中小规模知识库、希望减少组件数量的部署。文档元数据和向量可以在同一 PostgreSQL 中事务管理，但大规模检索和索引构建会与控制面争用资源，生产上建议使用独立数据库集群。

### ScopedVectorStore

框架 VectorStore 的 `Get(id)`、`Delete(id)` 等接口不都包含 filter，因此平台包装器要同时做 ID 命名空间和查询条件注入：

```text
physical document id:
  tenant_id/app_id/kb_id/logical_document_id/chunk_id

mandatory metadata:
  tenant_id
  app_id
  knowledge_base_id
  document_revision
```

`Search`、`Count`、`GetMetadata`、`DeleteByFilter`、`UpdateByFilter` 都要合并强制条件。调用方不能覆盖这些字段。文档 ID 解码后也要再次校验 scope。

## 6. Artifact 和对象存储

Artifact 适合放图片、语音、文件、代码产物和报表。对象 key 统一为：

```text
env/{tenant_id}/{app_id}/{runtime_user_id}/{session_id}/{artifact_id}/{version}
```

SQL `artifact_metadata` 分配版本并保存 checksum。上传流程为：

1. 在 SQL 事务中取得下一个版本，状态置为 `uploading`；
2. 上传临时 object key；
3. 校验大小和 checksum，执行病毒扫描；
4. 原子更新 metadata 为 `ready`；
5. 失败对象由清理任务回收。

tRPC-Agent-Go 的 S3 Artifact Service 会通过列举已有版本计算新版本，并明确不保证同名并发写安全。生产 Router 应使用 SQL 版本分配器，或要求每次保存使用唯一 artifact ID。

下载 URL 使用短期签名，不能把永久公网 URL 写入模型上下文。上传和下载都限制文件大小、MIME 类型、压缩比和访问域名，防止恶意文件及 SSRF。

## 7. Audit Log

审计日志推荐先写 PostgreSQL 分区表或具备不可变写入能力的日志服务，再异步同步到 Elasticsearch、ClickHouse 或对象存储。

SQL 保留结构化索引字段，原始大 payload 单独加密保存并记录引用。查询索引的最终一致不会影响原始审计记录的可靠性。

## 8. 后端选择矩阵

| 后端 | 适合数据 | 一致性和延迟 | 成本与运维 |
| --- | --- | --- | --- |
| InMemory | 测试、示例 | 单进程强一致，重启丢失 | 最低，不可生产 |
| Redis | 活跃 Session、租约、去重缓存 | 低延迟；主库写后可见 | 内存成本高，关注持久化和热 key |
| PostgreSQL | 控制面、journal、Session、Memory、Audit | 事务强，延迟高于 Redis | 通用性强，需分区和连接池治理 |
| MySQL | Session、Memory、既有业务数据 | 事务强 | 适合已有 MySQL 体系 |
| MongoDB | 文档型 Session | 取决于 write concern | Schema 灵活，需单独治理 |
| Qdrant | 中大型向量检索 | 索引可有短暂延迟 | 专用组件，过滤友好 |
| Milvus | 大规模向量数据 | 依索引和加载状态 | 运维复杂，扩展能力强 |
| pgvector | 中小知识库 | 与 PostgreSQL 事务体系接近 | 组件少，需隔离 OLTP 压力 |
| S3 / MinIO | Artifact、知识原文、归档 | 对象写后读取决于实现 | 低成本，需处理版本和孤儿对象 |
| External Memory | 长期记忆 | 取决于服务 SLA | 能力集中，但有供应商依赖 |

## 9. 依赖版本管理

tRPC-Agent-Go 主模块和 Session、Memory、Storage、VectorStore、AG-UI、OpenClaw 使用独立 Go module 和独立 tag，版本号并不总是完全一致。项目需要维护固定依赖清单和兼容矩阵。

当前实现若同时使用 AG-UI、OpenClaw、Qdrant 和 Milvus，建议将项目 Go 版本提升到至少 1.24.6，并在 CI 中运行：

- 所有包单测和 race test；
- Redis、PostgreSQL、MySQL、Qdrant、MinIO 集成测试；
- Session 接口契约测试；
- 后端迁移和双写故障测试；
- 依赖升级前后的 Session/Event JSON 兼容测试。
