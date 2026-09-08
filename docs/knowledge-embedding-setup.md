# 从文档 MCP 到真实知识库

目前通过真实 Telegram 验证的文档 MCP 使用固定关键词查找。向量知识库是另一条链路：Embedding 模型先把文本转换为数值向量，Qdrant 保存并检索向量，Agent 根据返回的文档作答。聊天模型负责回答，Embedding 模型负责表示文本，两者不能只靠改一个模型名称互换。

本机 `workbuddy2api/converter.py` 在 2026-09-08 检查时提供 models、chat/completions、responses、messages 等路由，没有 embeddings 路由。因此现有 `glm-5.3-flash` 聊天检查通过，不代表 Embedding 可用。不会自动把聊天配置复制给 Embedding，也不会下载模型或替你注册付费服务。

## 先准备独立配置

向 Embedding 服务的部署者确认 API root、模型 ID、输出维度和调用凭据，然后在私有 `.env` 追加或更新以下四项。不要复制覆盖整份现有 `.env`，也不要把真实 Key 发到聊天或提交 Git。

```dotenv
TRPC_AGENT_EMBEDDING_MODEL="实际的向量模型 ID"
TRPC_AGENT_EMBEDDING_BASE_URL="https://embedding-provider.example/v1"
TRPC_AGENT_EMBEDDING_API_KEY="该服务的 API Key"
TRPC_AGENT_EMBEDDING_DIMENSIONS=1024
```

本机已选择服务支持的 `1024` 维；其他环境应按模型实际支持的维度填写，并与 Qdrant 保持一致。Base URL 填 API 根路径，不填完整 `/embeddings` 路径；检查器会通过框架调用 `POST <API root>/embeddings`。当前框架设置维度后会把 `dimensions` 发送给服务，服务须接受相应参数；不支持时应先确认兼容方式，不能靠伪造返回维度通过。

本机免鉴权服务也应填写其允许的显式占位 Key，避免 SDK 回退到聊天凭据；远程服务优先使用 HTTPS。只填这四项不会启用 Knowledge，不需要重启正在运行的 Agent。

## 像模型检查一样执行

```bash
./check-embedding.sh
```

也可以选择文件或缩短等待：

```bash
TRPC_AGENT_ENV_FILE=./config/embedding-dev.env ./check-embedding.sh -timeout 15s
```

脚本重新编译并只运行 `trpc-embeddingcheck`，清除终端遗留的四个 Embedding 环境变量后读取指定文件；不影响其他进程，也不使用 `OPENAI_API_KEY`/`OPENAI_BASE_URL` 作为缺省值。直接运行二进制时，环境变量仍优先于文件；`-env-file=` 可用于纯环境注入。

通过时类似：

```text
checking embedding provider=openai dimensions=1024 requests=1
embedding check passed: dimensions=1024 latency=800ms finite=true nonzero=true
Connectivity and vector shape verified; no document import or semantic retrieval validation performed.
```

输出不展示 Key、模型返回正文、完整 URL 或向量内容。命令仅发送一条固定、无用户信息的中文测试文本，可能消耗少量供应商额度。没有模型调用重试，不会读取 IM 聊天、调用工具、写数据库或启用 Bot。

## 内部链路与边界

`check-embedding.sh → 独立配置校验 → tRPC-Agent-Go knowledge/embedder/openai → 单次 HTTP 请求 → 向量维度/有限值/非零范数检查`。

rc.8 起，检查器与 KnowledgeRouter 都使用 `embedding.Remote` 包装框架 Embedder，共享内部零重试、禁止重定向、显式凭据、响应大小与向量检查。正式运行时不再仅依靠预检把关，详见[运行链路](knowledge-runtime.md)。检查器的 timeout 还受 Remote 每请求 60 秒上限约束。

- 401/403：凭据或权限不正确。
- 404：API root、模型 ID 或 embeddings 接口可能不存在。
- 429：速率/额度限制，不自动重试。
- 维度不一致、空向量、零向量或非有限值：不能按此配置写入向量库。
- 超时：检查服务或网络，命令有上限并取消请求。

检查通过只证明这一次接口兼容与向量形状有效，不能证明语义质量、知识库入库、召回、租户隔离或生产容量已通过。

## 通过之后继续做什么

1. 配置 Qdrant 的持久化后端与维度，不复用或清空来源不明的 collection。
2. 给目标租户添加 `purpose=embedding`、`reference=env://TRPC_AGENT_EMBEDDING_API_KEY` 的精确授权。
3. 发布包含真实模型与 `secret_ref` 的 Knowledge revision；密钥本身不进入 revision。API 地址不包含凭据或 query。
4. 导入一份明确授权的公开测试文档，先检查真实向量入库、检索和租户隔离，再让用户从 IM 查询。

现有 Hash Embedder 继续只用于开发/自动测试，现有文档 MCP、Memory 和附件功能保持不变。本轮不替用户选择新供应商，不擅自把历史聊天或私人文件作为知识库语料。
