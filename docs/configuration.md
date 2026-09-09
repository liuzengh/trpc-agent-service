# 配置参考（环境变量全量）

本文档是环境变量的**全量参考**，逐条提取自 `trpcservice/config/config.go` 的 `Load()`。
[guide.md 附录 A](./guide.md#附录-a配置与密钥机制) 讲的是**机制**（密钥引用、resolver、
常用变量速查），按需读那一篇；这里回答的是「这个变量叫什么、默认是什么、不设会怎样」。

通用规则：

- 配置**只来自环境变量**，仓库里没有配置文件；未设即取表中默认值。
- 「必填 = 是」的变量未设时进程**拒绝启动**（fail-closed）；其余未设=用默认值或该功能关闭。
- 密钥永远只配**引用名**（`*_REF`），运行时由 SecretResolver 解析；明文不进入任何 env、
  配置和数据库。时长类值用 Go duration 语法（`60s`/`24h`）。
- 角色相关的启动校验：`worker` / `admin` 角色要求 PG 可达，`gateway` / `worker` 要求
  Redis 可达，启动时 fail fast。

## 服务监听

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_HTTP_ADDR` | `:8080` | 否 | Gateway 回调口，面向 IM 平台 |
| `TRPC_ADMIN_ADDR` | `127.0.0.1:8081` | 否 | Admin API 监听口。默认绑 loopback：仅内网，要对外须显式改绑 |
| `TRPC_METRICS_ADDR` | `127.0.0.1:8082` | 否 | `/metrics` 内网监听口，每个角色各起一个。默认 loopback（指标带租户维度流量与成本，无自带鉴权）；k8s 探针/集群抓取需显式绑 `0.0.0.0`。端口被占时服务不崩，只是 metrics 关闭并打 WARN |

## 数据库与 Redis

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_PG_DSN` | `postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable` | worker/admin 角色必需 | PostgreSQL DSN，与 docker-compose 一致 |
| `TRPC_REDIS_ADDR` | `localhost:6380` | gateway/worker 角色必需 | compose 把容器 6379 映射到宿主 **6380**（宿主 6379 常被本机其他服务占用） |

## 日志

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_LOG_LEVEL` | `info` | 否 | `debug` / `info` / `warn` / `error` |
| `TRPC_LOG_FORMAT` | `console` | 否 | `json` 为 JSON 输出（生产建议），其余值为 console |

## 密钥 Resolver

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_SECRET_RESOLVER` | `file` | 否 | 密钥后端：`file`（本地开发）或 `kms`（生产，KMS sidecar / Vault agent） |
| `TRPC_SECRETS_DIR` | `data/secrets` | 否 | file resolver 的密钥目录；kms 模式下也用于读 bootstrap token |
| `TRPC_KMS_ENDPOINT` | `""` | resolver=kms 时必需 | KMS 取值端点 |
| `TRPC_KMS_TOKEN_REF` | `kms-bootstrap-token` | 否 | KMS bearer token 的引用名——它自己也只是引用，经 file resolver 从 `TRPC_SECRETS_DIR` 读 |
| `TRPC_SECRET_CACHE_TTL` | `1m` | 否 | 进程内密钥缓存 TTL（所有 resolver 外层都套这个短 TTL 缓存） |

## 模型

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_MODEL_BASE_URL` | `https://api.deepseek.com` | 否 | OpenAI 兼容端点。平台默认，租户/应用配置可覆盖但受白名单约束 |
| `TRPC_MODEL_BASE_URL_ALLOW` | `""` | 否 | 模型端点 host 白名单（逗号分隔，精确匹配）。**空 = 只允许 `TRPC_MODEL_BASE_URL` 自己的 host**。会话原文流向该端点，选择权在平台层 |
| `TRPC_MODEL_NAME` | `deepseek-v4-flash` | 否 | 模型名 |
| `TRPC_MODEL_APIKEY_REF` | `deepseek-apikey` | 否 | 模型 API key 的**引用名**（对应 `data/secrets/deepseek-apikey`），不是 key 本身 |
| `TRPC_MODEL_TIMEOUT` | `60s` | 否 | 单次模型调用超时；超时后取消并重试 1 次，仍失败回「服务繁忙」降级回复 |
| `TRPC_MODEL_PRICES` | `""` | 否 | 模型单价表，JSON：`{"deepseek-v4-flash":[0.1,0.4]}`（USD / 1M tokens，[输入,输出]）。配了才算 `audit_log.cost`；不配只记 token 数 |
| `TRPC_APP_NAME` | `00000000-0000-0000-0000-000000000101` | 否 | 路由失败时的兜底 runner 应用 ID（默认指向 seed 演示应用）。正常路由的消息走自己的 `agent_app`，env 模型配置是最低优先级默认 |

## 会话与存储

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_SESSION_BACKEND` | `redis` | 否 | 会话后端：`redis`（热数据）或 `postgres`（事件流水 + 快照）。想用 SQL 查对话须切 `postgres` |
| `TRPC_SUMMARY_EVENT_THRESHOLD` | `20` | 否 | 触发会话摘要的未压缩事件数（仅 PG 会话后端） |
| `TRPC_ARCHIVE_RETENTION` | `720h`（30 天） | 否 | `session_event` / `audit_log` 在热表中的保留时长，超期由归档任务搬到归档表 |
| `TRPC_ARCHIVE_INTERVAL` | `24h` | 否 | 归档任务执行周期 |
| `TRPC_MIGRATION_OBSERVE` | `24h` | 否 | 存储后端迁移读切换后的双写观察窗口 |

## 限流

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_GATEWAY_RATE_QPS` | `50` | 否 | 平台默认入口令牌桶（按租户）；租户 `rate_policy` 可覆盖 |
| `TRPC_GATEWAY_RATE_BURST` | `100` | 否 | 入口令牌桶突发容量 |
| `TRPC_SEND_RATE_QPS` | `20` | 否 | 发送侧限速（按 `{channel, tenant}`，对应 IM 主动发送限流） |
| `TRPC_SEND_RATE_BURST` | `40` | 否 | 发送侧突发容量 |

## Admin

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_ADMIN_TOKEN` | `""` | **是** | Admin API 的 Bearer token。**未设拒绝启动**；本地开发用哨兵值 `dev-insecure`（该值只允许绑 loopback 时使用）。生产用强随机值，如 `openssl rand -hex 24` |
| `TRPC_ADMIN_TLS_CERT` | `""` | 否 | Admin 监听口 mTLS 服务端证书（仅独立 admin 角色） |
| `TRPC_ADMIN_TLS_KEY` | `""` | 否 | 服务端私钥 |
| `TRPC_ADMIN_TLS_CLIENT_CA` | `""` | 否 | 客户端证书 CA。**三个 TLS 变量同时设置才启用 mTLS** |

## 通道开关与凭据引用

通道全部默认关闭，设了对应开关变量才挂载。密钥均只配引用名。

### mock（本地演示）

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_MOCK_CHANNEL` | `false` | 否 | `true` 启用 mock 通道。**它是无鉴权的消息注入器，生产绝不能开** |

### wecom（企业微信自建应用，webhook）

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_WECOM_CORP_ID` | `""` | 启用通道必需 | 设了才挂载企微通道 |
| `TRPC_WECOM_AGENT_ID` | `""` | 启用通道必需 | 应用 AgentID（整数） |
| `TRPC_WECOM_TOKEN_REF` | `wecom-token` | 否 | 回调 Token 的引用名 |
| `TRPC_WECOM_AESKEY_REF` | `wecom-aeskey` | 否 | EncodingAESKey 的引用名 |
| `TRPC_WECOM_SECRET_REF` | `wecom-secret` | 否 | corpsecret 的引用名 |
| `TRPC_WECOM_API_BASE` | `https://qyapi.weixin.qq.com` | 否 | 企微 API 基地址 |

### wxkf（微信客服）

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_WXKF_CORP_ID` | `""` | 启用通道必需 | 与 `TRPC_WXKF_KF_ACCOUNT` **同时设置**才挂载 |
| `TRPC_WXKF_KF_ACCOUNT` | `""` | 启用通道必需 | open_kfid |
| `TRPC_WXKF_TOKEN_REF` | `wxkf-token` | 否 | 回调 Token 的引用名 |
| `TRPC_WXKF_AESKEY_REF` | `wxkf-aeskey` | 否 | EncodingAESKey 的引用名 |
| `TRPC_WXKF_SECRET_REF` | `wxkf-secret` | 否 | 客服 secret 的引用名（独立于企微 corpsecret） |
| `TRPC_WXKF_API_BASE` | `https://qyapi.weixin.qq.com` | 否 | API 基地址 |

### wecomws（企微智能机器人，WebSocket 长连接）

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_WECOMWS_ADDR` | `""` | 启用通道必需 | 设为 `wss://openws.work.weixin.qq.com` 即启用。BotID/Secret 不放 env，放 `channel_binding.config` |
| `TRPC_WECOMWS_PING_INTERVAL` | `30s` | 否 | 心跳间隔 |
| `TRPC_WECOMWS_LEADER_TTL` | `15s` | 否 | 全局单 leader 锁 TTL（TTL/3 续期；企微每 bot 同时只允许一条连接） |
| `TRPC_WECOMWS_RESYNC_INTERVAL` | `15s` | 否 | bindings 对账周期（新增起连、消失停连） |
| `TRPC_WECOMWS_SEGMENT_BYTES` | `2048` | 否 | 回复分条单帧上限（字节） |

## Artifact 存储（S3 兼容）

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_S3_ENDPOINT` | `localhost:9000` | 否 | MinIO / 云 OSS 端点；启动时不可达则 Artifact 功能关闭 |
| `TRPC_S3_BUCKET` | `artifacts` | 否 | bucket 名 |
| `TRPC_S3_ACCESSKEY_REF` | `s3-accesskey` | 否 | AccessKey 的引用名 |
| `TRPC_S3_SECRETKEY_REF` | `s3-secretkey` | 否 | SecretKey 的引用名 |
| `TRPC_S3_SECURE` | `false` | 否 | `true` 走 TLS（云 OSS） |

## Knowledge / Embedder（向量检索）

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `TRPC_EMBEDDER_MODEL` | `""` | 启用必需 | **不设 = 向量检索整体禁用**（知识库摄入接口回 503）。默认聊天端点（DeepSeek）没有 embeddings API，需指向支持 embeddings 的 OpenAI 兼容端点 |
| `TRPC_EMBEDDER_BASE_URL` | `""` | 启用必需 | embeddings 端点 |
| `TRPC_EMBEDDER_APIKEY_REF` | `embedder-apikey` | 否 | API key 的引用名 |
| `TRPC_EMBEDDER_DIMENSION` | `1536` | 否 | 向量维度 |
| `TRPC_KNOWLEDGE_TABLE` | `knowledge_embeddings` | 否 | pgvector 表名 |

## 可观测（非 `TRPC_*`）

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 未设 | 否 | OTel SDK 标准变量，gRPC exporter 要 `host:port` **不带 scheme**（本地 `localhost:4317`，Jaeger UI 在 16686）。不设也能正常启动，只是不导出 trace |

## 测试专用（`trpcservice/testenv`）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `TRPC_TEST_PG_DSN` | 同 `TRPC_PG_DSN` 默认值（开发库） | 集成测试的 PG。建议指向独立 `trpc_test` 库，避免污染开发数据（见 [development.md](./development.md) §2） |
| `TRPC_TEST_REDIS_ADDR` | `localhost:6380` | 集成测试的 Redis（无 db index 开关） |
| `TRPC_TEST_S3_ENDPOINT` | `localhost:9000` | 集成测试的 MinIO |
