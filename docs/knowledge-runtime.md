# 真实 Embedding 与 Qdrant 的运行链路

rc.8 将独立预检和正式 Knowledge 运行时接到同一个安全 Embedder。原来的项目文档 MCP 仍是关键词检索；这里使用真实文本向量和 Qdrant，不用 Hash Embedder 替代真实服务。

## 文档怎样进入知识库

管理员提交 `POST /admin/knowledge/documents`，包含 tenant/app/revision、稳定 document_id、operation_id、正文与来源。鉴权后写入持久化 `knowledge_upsert` Job。Jobs 进程读取该 revision 的 Knowledge 配置，由 KnowledgeRouter 进行作用域检查、写意图登记、切块和入库。

每块文本通过 `modelops.Embedding` 预留用量，再经 `embedding.Remote` 调用 tRPC-Agent-Go `knowledge/embedder/openai`。服务返回的向量经过形状检查后写入 Qdrant，payload 固定附加 tenant_id、app_id、source_document_id、chunk_index 和来源。输入 metadata 不能覆盖平台作用域。

文档替换仍采用“登记修复意图 → 按准确文档过滤删除旧块 → 写新块 → 清除意图”；中途失败会保留修复依据，不承诺向量库中的整份文档原子替换。这次只导入一个全新的公开合成文件，不替换用户资料。

## IM 查询怎样执行

`Telegram → Gateway/队列 → Worker/Runner → knowledge_search → 问题向量化 → Qdrant 作用域过滤 → 文档片段 → 模型回答 → 持久回复 → Telegram`。

`knowledge_config.enabled=true` 时，Compiler 使用框架 `WithKnowledge` 注册 `knowledge_search`，并在运行时权限策略中允许它。这个工具不是全局 Catalog 中的普通函数，不要再将它手动塞进普通工具列表。它仍经过工具权限、调用次数限制、Tool Journal、审计和 tracing。启用配置本身就是对该 App 的知识检索授权；当前不是逐知识库/逐文档 ACL。

写入向量与查询向量必须使用同一模型和维度。平台强制 Qdrant 绑定维度与 revision 一致，查询过滤中的 tenant/app 不接受模型覆盖。不同租户选择各自的 Secret 和后端，不能从进程里的聊天 Key 隐式继承 Embedding 权限。

## 为什么增加一个 Embedder 包装层

仍然复用框架的 Embedder 和 SDK，请求生成、模型参数、用量解析及框架 embedding span 都由它们完成。平台新增的是网络与数据边界：

- 专用 HTTP Doer 将 SDK 请求的副本发往部署者指定地址；SDK 只看到固定的 `embedding.invalid` 内部标识，不对该标识发起网络请求。真实 endpoint 和原始网络错误不会进入 SDK 错误日志。
- 不转发隐式组织/项目头、baggage 或 SDK 扩展头；只使用显式凭据和必要的 JSON/trace headers。禁止重定向，关闭框架及 SDK 内部重试。
- 每次请求最多 60 秒、输入最多 128 KiB、解码响应最多 4 MiB。正常回复必须恰好有一个 index 0 向量，维度匹配、数值有限，且转换为 Qdrant 的 float32 后仍有有效非零范数。
- SDK 只接收验证后的向量、数值用量及配置中的模型名称，不接收服务端扩展字段、错误描述或原始响应头。失败保留固定类别和 HTTP 状态码；不把失败伪造成成功向量。
- HTTP 连接池由 Remote 持有，KnowledgeRouter 关闭时连同预算包装层一起释放。调用取消沿 context 传播，不为重试启动额外 goroutine。

后台 Job 自身仍可按持久化重试策略重试失败任务，稳定文档 ID 和写意图用于恢复；“无内部重试”不代表整个后台系统永不重试。每次实际 Embedding 调用仍独立记账。价格未配置时，金额统计不代表供应商账单。

## 本地启用边界

本地选择 1024 维、专用 collection `tutorial_knowledge_1024_20260908`，Qdrant REST/gRPC 只绑定 loopback，使用独立随机 API Key。已有 `trpc_agent_integration` 集合保留，不用于新测试，也不被清空。

Qdrant API Key 是这个本地服务的访问凭据，不是 tenant 级数据库 RLS。平台的精确 Secret grant、每 App 专用 collection 和查询过滤共同形成开发环境隔离；生产仍需网络策略、TLS、独立账号/集群及容量验证。配置方式参考 [Qdrant 官方安全说明](https://qdrant.tech/documentation/operations/security/)；同集合向量的维度约束见 [Collections](https://qdrant.tech/documentation/manage-data/collections/)。

测试资料是 [`examples/knowledge-test.md`](../examples/knowledge-test.md)，不读取历史聊天或私人附件。启用、导入、持久性、检索结果与真实 IM 状态单独记录，不能把预检成功当成整条知识库链路通过。
