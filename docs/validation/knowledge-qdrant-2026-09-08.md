# 真实文本向量与 Qdrant 启用记录

日期：2026-09-08。用户确认 Embedding 检查通过，并同意使用 1024 维后继续开发。程序为 `0.2.0-rc.8`，平台 schema 仍为 23，没有新增 SQL migration。

## 启用前与备份

检查时 Agent、模型和原 Docker 依赖均已停止。恢复现有 PostgreSQL/Redis/MinIO 与监控容器，保留原数据卷；控制面仍为 App version 6 / `tutorial-docs-mcp-v6`，没有 Knowledge binding，前六个 revision 均未启用 Knowledge。

Qdrant 数据卷已有 `trpc_agent_integration` 集合，因此没有清空该卷、删除旧集合或把它当作新索引使用。新建的目标名称为 `tutorial_knowledge_1024_20260908`，创建前通过鉴权 API 确认不存在。

私有备份目录 `data/knowledge-upgrade.TTvW7ci0/` 保存 `.env`、旧二进制、日志、PostgreSQL dump、Redis RDB、控制面及 Qdrant 集合清单。目录 0700、敏感文件 0600，备份 SHA-256 与 dump 目录验证通过。本次没有恢复旧业务快照，SQL dump 不包含 Qdrant/MinIO 对象或数据库集群角色。

## 新增代码与配置

- 新增 `embedding.Remote`，由检查器与正式 Knowledge 复用。仍调用 tRPC-Agent-Go 的 Embedder，增加显式目的地、凭据边界、无内部重试、禁止重定向、有界输入/响应和向量检查；SDK 日志不接触真实 endpoint 或原始服务端错误。连接池随 Router 关闭。
- Qdrant REST/gRPC 只映射到 `127.0.0.1:6333/6334`。`.env` 新增随机 `TRPC_AGENT_QDRANT_API_KEY`，Compose 启用 API Key；无 Key 访问 `/collections` 返回 401。这里使用 loopback 明文连接，SDK 的无 TLS 警告被保留，不宣称完成生产 TLS 或 tenant 级数据库 ACL。
- Secret grants 只增加 `tutorial-tenant / embedding / env://TRPC_AGENT_EMBEDDING_API_KEY` 和 `tutorial-tenant / knowledge / env://TRPC_AGENT_QDRANT_API_KEY`，现有模型、IM、Memory、Artifact、MCP 参数不覆盖。
- 经鉴权 Admin API 创建 `tutorial-knowledge-qdrant-v1` binding 与未发布的 revision `tutorial-knowledge-v7`。维度 1024、Cosine、max_results 3、min_score 0.1、chunk_size 600、overlap 60；工具规则、Memory 私聊策略和旧 MCP 配置保留。min_score 是本轮初始配置，不是质量保证阈值。
- Embedding 金额价格暂为 0、租户没有金额限额；实际 token 仍走平台调用记账，但金额不代表供应商账单，也不意味着外部 API 免费。

## 已验证

`TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh` 通过全仓 race/lint/build、隔离 PostgreSQL/Redis/Qdrant/MinIO、恢复工具链及离线告警规则检查。增加真实本地 HTTP 协议与日志 canary 测试、Runner → knowledge_search → Embedder → 工具 Journal/审计测试，以及同 Qdrant collection 下三个合成 tenant/app 组合的过滤测试。随后补充的 float32 溢出/下溢检查与相关包 race 测试也通过。

真实环境中的证据：

1. 新版 `./check-embedding.sh` 返回 1024 维、有限非零向量，用时 475ms；模型检查 `glm-5.3-flash` 返回 OK。没有用 Hash Embedder 代替真实 API。
2. 只导入仓库公开合成资料 [`examples/knowledge-test.md`](../../examples/knowledge-test.md)，document_id 为 `firefly-borrowing-kn-20260908`。没有读取或导入历史聊天、私人附件、旧 Qdrant 文档。
3. 持久任务 `job_34b318027db874f2d7aaf593a568d187`，类型 knowledge_upsert，completed、attempt 1。目标 collection 中有一条 1024 维 Cosine 向量。
4. 独立 KnowledgeRouter 使用真实 Embedding 对改写后的问题检索，返回正确文档和“十七个自然日”“重新提交申请”的事实，分数 0.7646；返回正文 SHA-256 为 `001cacd5e4a63b0c4f1cf08d006430f4af49d31b7f02071176cef8b2b39be300`。这是单样本检索结果，不代表完整召回质量评估。
5. 使用该真实向量做只读 Qdrant 过滤检查：本 tenant/app 返回 1 条，其他 tenant 或其他 app 返回 0 条。不存在的租户也无法解析 Embedding Secret grant；没有向真实其他租户授予凭据。
6. 独立 Router 仍可读回原 PostgreSQL 长期记忆和 MinIO 测试附件，跨 Session 附件访问仍被拒绝。

## 发布与真实 IM 验证

入库和独立检索验证时 App stable 仍为 revision 6。随后停止空闲 Agent、重启 Qdrant 容器，并用新进程重新打开 KnowledgeRouter：点数、1024 维/Cosine、过滤结果和检索正文 hash 均与重启前一致，分数仍为 0.7646；没有重新执行导入作业。

重启后的 rc.8 Agent PID 为 67378，原模型和 Tunnel 保持运行。经 Admin API 发布 `tutorial-knowledge-v7`，App version 6 → 7；仅把此前测试私聊固定到新 revision，变更与审计 `audit-knowledge-repin-TTvW7ci0` 同事务提交，原 Session 与历史 Run 保留。没有批量升级其他既有会话。

已知旧回复 `req_5a1ef3994f1ddb4b59f31adeeaf43e0b` 仍为 dead、attempt 1、part unknown，不重放、不改写。

### 真实 Telegram 验证（已通过）

17:44:17（Asia/Shanghai）的新请求 `req_5f481ef89c1e3407a2a84cfe8c1480b3` 使用 `tutorial-knowledge-v7`，17:44:38 completed。`knowledge_search` 执行一次且 succeeded，无 error_type。

同一 trace `c270bd165eb6cc3dc0ddba26123d5c2c` 已从 Tempo 实际读回：Telegram callback → 队列 → Worker/Runner → `execute_tool knowledge_search` → `storage.knowledge.search` → `embeddings qwen3.7-text-embedding`，父子 span 对应，另有模型、Session 读写及 reply.send。Qdrant 查询由 storage.knowledge.search 覆盖，未声称存在额外的独立 Qdrant RPC span。

回复 `out_a935f6b4cf7ac30759c5ea71564fb525` 于 17:44:39 sent，attempt 1、part 0 sent 且有 provider 回执；正文包含正确借阅天数和资料来源，用户确认收到。因此本地 **Telegram → 实际知识检索/Embedding/Qdrant → 模型回答 → Telegram** 链路通过。多租户真实账号、大规模语义质量、远端云 Qdrant 和生产恢复仍不包含在该结论中。

本轮消息内容：

```text
请调用 knowledge_search 查询：流萤资料借阅的期限是多久？到期后能否直接继续使用？请根据知识库回答，并注明资料来源。
```

上述消息已通过，不需要重复发送。后续复测仍须核对同一 request_id 下的工具执行和投递事实；文档 MCP 的通过结论不能替代 Knowledge 检索。
