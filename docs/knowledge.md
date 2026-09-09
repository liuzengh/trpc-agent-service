# Knowledge/RAG 与 S3 Artifact

Knowledge 已实现管理员 ingest、OpenAI-compatible Embedding、PGVector/Qdrant 写入和 `knowledge_search` 运行时检索代码路径。仓库 CI 不启动真实 PGVector/Qdrant，生产可用性、召回质量和迁移结论必须在目标环境验证。它不是完整的文档管理系统，不包含 PDF/OCR、网页抓取、MCP 或业务 Tool。

## Knowledge 配置

Knowledge 默认关闭。启用时必须同时配置 embedding、共享向量后端，并显式允许工具：

```yaml
tools:
  allow: [calculator, knowledge_search]
knowledge:
  enabled: true
  embedding:
    provider: openai-compatible
    model: text-embedding-3-small
    base_url: https://embedding.example/v1
    api_key: {provider: vault, key: demo/assistant/embedding-api-key}
    dimensions: 1536
  max_results: 8
  min_score: 0.2
storage:
  knowledge:
    type: qdrant
    endpoint: grpcs://qdrant.example:6334
    namespace: support_docs
    credential: {provider: vault, key: demo/assistant/qdrant-api-key}
```

PGVector 使用 `type: postgres`；其目标 PostgreSQL 必须预装 `vector` extension，SecretRef 可像其他路由一样解析独立 DSN。发布阶段会建立并检查对应 table/collection。物理名称由配置 namespace 加 tenant/App 摘要派生，检索还会强制添加可信 `tenant_id` 与 `app_id` metadata filter；请求体里的同名字段会被覆盖。

Admin 路径继续使用 `TRPC_AGENT_ADMIN_TOKENS` 的 Bearer 认证和 URL tenant scope：

```http
POST /v1/tenants/demo/apps/assistant/knowledge/documents
Authorization: Bearer <admin-token>
Content-Type: application/json

{"document_id":"runbook-1","name":"Runbook","content":"...","metadata":{"topic":"operations"}}
```

文本上限 1 MiB，按 Unicode rune 分片并带重叠；chunk ID 由 tenant、App、document ID 和序号确定，重复 ingest 是 upsert。检索端点是同一路径下的 `/search`，请求字段为 `query`、`max_results`、`min_score`。API 不接受 tenant_id、后端 endpoint 或凭据。

## S3-compatible Artifact

Artifact 路由可使用 `type: s3`，`endpoint` 是 HTTPS S3 API 地址，`namespace` 是 bucket。Credential SecretRef 解析出的值是 JSON，字段为 `access_key_id`、`secret_access_key`、`region`，MinIO 还应设置 `path_style: true`；可选 `session_token`。真实 JSON 只存在密钥提供方，不写入 YAML、日志、trace、审计或 API 响应。

官方 S3 Artifact Adapter 的 revision 分配本身不是并发安全的，因此平台在共享 PostgreSQL 上取得按 app/user/session/filename 派生的 advisory lock，再执行 list-and-put。多个 Worker 节点仍能得到单调 revision，不需要 sticky session。

PostgreSQL 与 S3 Artifact 双向迁移都使用 `migration_target` 流程。新 Bundle 同步双写；
S3 每次成功写入后会把租户/App、逻辑 key、revision 和 checksum 写入共享 PostgreSQL 目录，
目录不包含文件正文、下载 URL 或对象存储凭据。反向任务按目录读取指定 S3 revision 并写回
PostgreSQL。Worker 丢失 lease 后可从 checkpoint 重放；内容冲突、缺失 revision 或 checksum
不一致都会让任务失败并禁止 cutover。

Knowledge ingest 同时维护租户/App 文档目录。PGVector ↔ Qdrant 迁移从该目录重新生成 embedding
并 upsert 目标索引，在 `migration_target` 阶段对新文档同步双写。完成任务后仍应做召回抽样，
再发布下一配置版本切换主索引。只存在于向量库、没有进入文档目录的早期历史数据，
需先通过 Admin Knowledge API 重新 ingest 一次。S3/PostgreSQL 双向迁移同样需要真实 bucket、
权限、网络失败和版本冲突演练，单元测试不能替代该验收。
