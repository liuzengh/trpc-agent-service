# 多后端适配方案

## 1. 统一访问层

平台不重新定义 Agent 框架的数据接口，而是在 tRPC-Agent-Go 接口外增加路由、租户校验、指标和迁移能力。

网页“资源中心 → 数据后端”提供具名存储连接。平台管理员按类型填写参数和凭据，或选择已授权的环境凭据；租户管理员选择已有连接并绑定到应用，不需要手写连接 JSON 或内部凭据引用。连接信息保存在 `backend_connection`，秘密值复用加密凭据库；绑定后仍由下述框架适配器执行实际读写。

连接的地址、类型和凭据归属在创建时固定。旧绑定 API 不能把托管凭据重新指向其他目的地。保存不执行网络探测、建表或数据迁移，也不覆盖已有绑定。修改已有物理后端应新建连接并按[迁移策略](data-consistency.md)切换。

| 数据类型 | 框架接口或能力 | 平台包装器 |
| --- | --- | --- |
| Session / State / Summary | `session.Service` | `storage.SessionRouter` |
| Memory | `memory.Service` | `storage.MemoryRouter` |
| Artifact | `artifact.Service` | `storage.ArtifactRouter` |
| Knowledge | `knowledge.Knowledge`、`vectorstore.VectorStore` | `storage.KnowledgeRouter`、`scopedKnowledge` |
| Audit Log | 无统一框架接口 | `audit.Writer/Reader` |
| 异步任务 | Summary/Memory hooks | `background.Repository/Processor` |

路由器不接收外部 `tenant_id` 参数，而是从内部 `storage_scope` 和可信 context 中取值，并要求两者一致。

可信调用方使用 `runtimecontext.WithStorageScope(ctx, storageScope)` 注入授权范围。Router 不接受无身份的 context，并交叉检查框架 Invocation；嵌套绑定不能扩大原授权范围。健康探测使用保留的无业务数据命名空间，不构成租户访问入口。

当前代码已接入的物理实现：Session startup/InMemory/Redis/PostgreSQL，Memory InMemory/Redis/PostgreSQL，Artifact InMemory/S3-compatible，Knowledge InMemory/Qdrant，Control/Audit/Job PostgreSQL。MySQL、MongoDB、Milvus 等仍属于框架可扩展选项，不在默认二进制依赖中。

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

Router 按后端 Binding 和配置摘要缓存服务，不同资源的具体缓存键由实现决定。当前没有通用的按引用计数热淘汰，旧服务由 Router 关闭时回收；Secret 或连接配置更新需配合受控重启。后端操作记录类型、耗时和安全错误分类，DSN、bucket credential 等不写入 span。

## 2. Session 后端

### InMemory

适合单机演示与临时会话。它不支持多节点共享，进程退出后数据丢失，不能用于生产租户。

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

当前 PostgreSQL 包装器支持 `schema`、`table_name` 和 `skip_db_init`。先用迁移/运维身份初始化表，再配置 `skip_db_init=true` 和仅有表 DML 权限的 SecretRef，避免运行时要求建表权限。支持后端见[功能范围](capabilities.md)。

Memory 用户键不包含 Session ID。若不希望私聊事实被带入群聊，可设置 revision `memory_config.direct_only=true`，并保持 `preload_memory=0`、`auto_extract=false`；框架工具按可信请求受众过滤，群聊和未知受众无法调用 `memory_*`。该模式仅在用户明确操作时保存/读取，不等于自动长期记忆提取。

### 外部 Memory 服务

Mem0 或企业自建 Memory API 适合把提取、去重、检索交给专用服务。平台仍要传入 tenant/app/user scope，并保存外部对象 ID、版本和审计记录。外部服务不可用时可以降级为“当前请求不加载长期记忆”，但不能错误加载其他租户数据。

### 自动 Memory 任务

框架内置 auto-memory worker 是进程内队列。生产路径由平台 Worker 的 `enqueueSessionJobs` 写持久化任务，再由 Job Worker 调用 `memory/extractor.MemoryExtractor` 和目标 `memory.Service`，不依赖框架进程内队列。这样可以记录重试次数、提取水位、模型成本和失败原因。

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

当前实现生成时不持有存储锁，提交时校验快照与摘要边界，旧结果不能覆盖已更新的摘要。配置 `summary_every_turns > 0` 后，单 LLMAgent 使用框架的整会话摘要投影；后续模型请求包含持久摘要及未覆盖的历史，而不是仅生成摘要后继续发送全部原始消息。

## 5. Knowledge 和向量库

Knowledge 包含原始文档与检索索引。生产建议原始文件放对象存储，元数据/处理状态放 SQL，chunk 与 embedding 放向量库。当前持久化 Job 管理入库/删除，Router 强制注入 tenant/app 过滤；它不等于任意文档格式都能自动解析。

文本入库、删除和检索已实现；当前采用手动配置与 Admin API 导入，网页不提供文档上传或编辑。操作步骤见[安装运行手册](operations-runbook.md#knowledge-setup)。

真实 Embedding 与聊天模型分开配置，在 Agent 的 `knowledge_config` 中明确模型、API 地址和向量维度，并引用已授权的独立密钥。运行时校验向量维度、有限数值和非零向量；这些检查不代表检索质量已经达标，发布前仍需使用业务样本验证。

运行时必须明确 `purpose=embedding` 的 Key 引用和 `purpose=knowledge` 的向量库凭据；不回退到 SDK 默认 Key。维度与目标 collection 必须一致，变更模型/维度需重建索引。远端请求禁止不受控重定向，限制响应大小，不导出原始供应商错误体或向量内容到 trace。

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

Artifact 用于文件和产物。建议的逻辑命名空间如下；当前物理对象布局由所复用的框架后端实现，不承诺与示例字符串完全相同：

```text
env/{tenant_id}/{app_id}/{runtime_user_id}/{session_id}/{artifact_id}/{version}
```

当前复用 tRPC-Agent-Go Artifact Service，以 Storage Scope 隔离文件，通过 PostgreSQL advisory lock 保护跨节点的同名版本分配，操作记录保护重试。部署者必须提供 bucket-scoped 凭据和显式 Backend Binding，不使用 MinIO root 或隐式 SDK 身份。

生产可进一步增加独立元数据分配器、临时对象校验、病毒扫描和过期对象回收，但这些流程不应当作当前已实现的完整附件平台。已实现的 Telegram 限类型导入会检查大小、MIME、图片尺寸和路径，并按原会话授权读取；当前没有完整杀毒、Office/PDF 解析和媒体发送。

### 6.1 本地 MinIO 初始化

先按运行手册执行 `docker compose run --rm minio-init`。以下只适用于仓库默认的本地 Compose；固定 root 凭据仅供初始化，不注入 Agent。已有环境不要重复创建同名用户或覆盖其策略。

在 Bash 终端输入准备给平台使用的**新** Access Key/Secret Key（不是 root 账号），然后只授予示例 bucket 权限：

```bash
read -rp 'New artifact Access Key: ' MINIO_ARTIFACT_ACCESS_KEY
read -rsp 'New artifact Secret Key: ' MINIO_ARTIFACT_SECRET_KEY
export MINIO_ARTIFACT_ACCESS_KEY MINIO_ARTIFACT_SECRET_KEY
docker compose run --rm \
  -e MINIO_ARTIFACT_ACCESS_KEY -e MINIO_ARTIFACT_SECRET_KEY \
  -v "$PWD/deploy/compose/artifact-policy.json:/tmp/artifact-policy.json:ro" \
  --entrypoint /bin/sh minio-init -ec '
    mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null
    mc admin policy create local agent-artifacts /tmp/artifact-policy.json
    mc admin user add local "$MINIO_ARTIFACT_ACCESS_KEY" "$MINIO_ARTIFACT_SECRET_KEY" >/dev/null
    mc admin policy attach local agent-artifacts --user "$MINIO_ARTIFACT_ACCESS_KEY"
  '
unset MINIO_ARTIFACT_ACCESS_KEY MINIO_ARTIFACT_SECRET_KEY
```

将两项值保存在私有 `.env`，不要提交：

```dotenv
MINIO_ARTIFACT_CREDENTIALS='{"access_key_id":"新 Access Key","secret_access_key":"新 Secret Key"}'
```

下面接续运行手册创建的 `tenant-b / app-b`（新应用尚无 Artifact 绑定）。向既有 `TRPC_AGENT_SECRET_GRANTS_JSON` **追加** `{"tenant_id":"tenant-b","purpose":"artifact","reference":"env://MINIO_ARTIFACT_CREDENTIALS"}`，不要覆盖其他 grant。重启相关 Worker/Admin 后，在管理页创建后端绑定，或调用 `/admin/backend-bindings`：

```json
{
  "binding_id": "tenant-b-artifacts",
  "tenant_id": "tenant-b",
  "app_id": "app-b",
  "resource_type": "artifact",
  "backend_type": "s3",
  "secret_ref": "env://MINIO_ARTIFACT_CREDENTIALS",
  "config": {
    "bucket": "trpc-agent-artifacts",
    "endpoint": "http://127.0.0.1:9000",
    "region": "us-east-1",
    "path_style": true
  }
}
```

此 endpoint 供宿主机运行的 Worker 使用；容器内应改为它能访问的 MinIO 服务地址。若已有活动 artifact 绑定，使用迁移接口，不直接覆盖。绑定成功后再在 Telegram 绑定中启用附件、追加 `telegram_media` grant，文本附件才会进入导入流程。该示例将平台身份限制在一个 bucket；生产可按租户拆 bucket 或细化对象前缀，不能把示例策略当作完整的存储层租户隔离。

## 7. Audit Log

审计日志推荐先写 PostgreSQL 分区表或具备不可变写入能力的日志服务，再异步同步到 Elasticsearch、ClickHouse 或对象存储。

SQL 保留结构化索引字段，原始大 payload 单独加密保存并记录引用。查询索引的最终一致不会影响原始审计记录的可靠性。

## 8. 后端选择矩阵

| 后端 | 适合数据 | 一致性和延迟 | 成本与运维 |
| --- | --- | --- | --- |
| InMemory | 单机演示、临时会话 | 单进程强一致，重启丢失 | 最低，不可生产 |
| Redis | 活跃 Session、租约、去重缓存 | 低延迟；主库写后可见 | 内存成本高，关注持久化和热 key |
| PostgreSQL | 控制面、journal、Session、Memory、Audit | 事务强，延迟高于 Redis | 通用性强，需分区和连接池治理 |
| MySQL | Session、Memory、既有业务数据 | 事务强 | 适合已有 MySQL 体系 |
| MongoDB | 文档型 Session | 取决于 write concern | Schema 灵活，需单独治理 |
| Qdrant | 中大型向量检索 | 索引可有短暂延迟 | 专用组件，过滤友好 |
| Milvus | 大规模向量数据 | 依索引和加载状态 | 运维复杂，扩展能力强 |
| pgvector | 中小知识库 | 与 PostgreSQL 事务体系接近 | 组件少，需隔离 OLTP 压力 |
| S3 / MinIO | Artifact、知识原文、归档 | 对象写后读取决于实现 | 低成本，需处理版本和孤儿对象 |
| External Memory | 长期记忆 | 取决于服务 SLA | 能力集中，但有供应商依赖 |
