# 无凭据 Fake Demo 覆盖面

`bash scripts/compose/quickstart.sh --demo`（或 `./start.sh --demo`）是一个可重复的本地验收入口。它只使用 Docker Desktop、仓库中的代码和 `docker-compose.backend-smoke.yml` 提供的 PostgreSQL/Redis；不读取 DeepSeek、IM 或 Secret 文件。

该入口会构建服务镜像、在空 PostgreSQL 应用嵌入式 schema migration，并断言 `schema_migrations` 只记录压缩后的 `000001`；随后发布固定 Demo Tenant/App/Revision/Policy/Config，并以 `fake-deterministic-v1` 启动 `demo-server`。随后它必须通过 `/healthz`、`/readyz`，让普通 `POST /v1/chat` 返回固定的 `demo: deterministic fake response`，并让带 `"stream":true` 的请求以 SSE 依序送出固定的 `demo: ` 与 `deterministic fake response` delta。重复运行会复用相同的控制面 ID 和已发布 revision。

## 已覆盖

- 镜像构建、PostgreSQL 连通性、schema migration 就绪与 Demo bootstrap 的幂等性；
- Provider Catalog 对 fake model 和共享 PostgreSQL backend 的约束；
- fake Model Resolver 的无网络、无凭据普通响应与固定 delta 流式路径；普通 HTTP chat 与 SSE `event: delta` 的端到端编解码和顺序；
- 服务存活/readiness 门禁与可操作的失败日志提示。

## 明确豁免

- 这不是 production Gateway/Worker 的 durable inbox、Redis broker、lease、tool、DLP、预算或 IM delivery 验收；这些链路仍由现有 runtime/adapter smoke 与凭据化本地 Compose profile 覆盖。
- 不覆盖 DeepSeek、Feishu、WeCom、S3、Vault、Qdrant 或 ClamAV 的真实网络和凭据行为。
- 不覆盖 fake provider 的 SecretRef 投放：fake schema 明确禁止 SecretRef，model resolver 在该分支不会调用 secret provider。生产 DeepSeek 路径则必须从 scoped、版本化 SecretRef 获取模型凭据，不能把 demo 的无凭据行为外推到生产。
