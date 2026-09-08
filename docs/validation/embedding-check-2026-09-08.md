# Embedding 预检开发记录

日期：2026-09-08。文档 MCP 的同请求真实验证通过后，继续准备真实 Knowledge 接入。没有修改当前 `.env`、Agent revision、后端 Binding 或业务数据，也没有重启运行中的 rc.7。

## 新增交付

- `check-embedding.sh` 和 `cmd/trpc-embeddingcheck`，加入 `build.sh`。
- 独立的 `TRPC_AGENT_EMBEDDING_MODEL/BASE_URL/API_KEY/DIMENSIONS` 配置，仅供显式预检使用；不继承聊天参数，不自动启用 Knowledge。
- 复用 tRPC-Agent-Go v1.11.2 的 `knowledge/embedder/openai`，一次固定文本调用；维度、有限值及非零范数检查。
- 关闭框架/SDK 重试，禁止重定向，限制解码响应大小，屏蔽原始框架错误日志，只输出安全错误分类，避免泄露 URL/Key/服务端正文。

使用方法和后续启用顺序见[配置说明](../knowledge-embedding-setup.md)。

## 验证结果

`go test -race ./cmd/trpc-embeddingcheck ./trpcservice/config ./scripts` 和 `./scripts/regression.sh` 均通过。后者包含全仓 race、lint、build、diff check；本次没有重复运行 Docker 隔离恢复测试，因为新增命令不连接数据库/队列，之前 rc.7 的完整隔离回归记录单独保留。

合成 HTTP 测试验证：实际框架发送 `/v1/embeddings` 和显式维度；只使用 Embedding Key；不携带聊天凭据或隐式组织/项目头；鉴权失败、404、429、500、错误 JSON、零向量、维度不匹配、超大响应、超时取消、拒绝重定向、无隐式重试、框架日志及命令输出无 canary 泄露。数值测试覆盖 NaN、Inf 和范数溢出。

在日常环境执行 `./check-embedding.sh`，因尚未配置 `TRPC_AGENT_EMBEDDING_MODEL` 而在请求前退出，没有访问外部 Embedding 服务。检查本机 converter 路由也未发现 embeddings 接口。当前 Agent PID 582645、模型与 Tunnel 继续运行，readiness 正常。

## 需要部署者提供的内容

需要可用的 Embedding 服务 API root、模型 ID、API Key 和输出维度；Key 只填写到私有 `.env`。预检成功后再配置向量后端、租户授权和 Knowledge revision，导入明确授权的测试文档并验证真实检索。没有将“预检代码测试通过”记成“真实知识库已接通”。
