# 本地验证矩阵

本文是当前交付物的可执行验证索引。所有命令以仓库根目录为工作目录；除明确标为“真实外部”的行外，验证仅使用 Docker Desktop 本地资源。真实凭据、图片和回调 payload 均为本机私有数据，严禁提交。

## 判定规则

- **通过**：命令退出码为零，且表中对应的成功证据可见。
- **真实外部**：需要开发者自有账号、可消费 API Key 或临时 HTTPS tunnel；本地 WebUI、fixture 和 mock 不能替代该证据。
- **隔离性**：带“自动清理”的命令只操作其创建的 Compose project；其他命令保留本地卷，重置必须由开发者显式执行。
- **REPO_VERIFIED**：`CI` 表示仓库以无凭据、零-skip 的可重放 job 自动验证；`本地`表示脚本存在但未纳入 CI；`否`表示没有仓库内可执行证据。
- **EXTERNALLY_VERIFIED**：`不需要`表示该项不依赖外部账号；`需人工`表示只有真实账号、回调或部署环境产生的记录才是有效证据，不能由 mock 替代。

| 验证项 | 命令或操作 | 所需资源 | 通过证据 | 范围 | REPO_VERIFIED | EXTERNALLY_VERIFIED |
| --- | --- | --- | --- | --- | --- | --- |
| Compose 契约 | `docker compose -f deploy/compose/docker-compose.local.yml config --quiet` | Docker Compose v2 | 退出码 0 | 本地 | 本地 | 不需要 |
| Admission | `bash scripts/ci/admission.sh`；race 使用 `--race` | Go 1.25.x；或等价 Go 1.25 容器/CI runner | format、依赖边界、Markdown 链接/锚点、build、vet、test 全部通过 | 本地/CI | CI | 不需要 |
| 最终无凭据验收 | `bash scripts/ci/admission.sh --demo` | Go 1.25、Docker Desktop/CI Docker daemon、curl | 空 PostgreSQL 只记录完整的压缩基线 `000001`；demo bootstrap、fake chat、SSE delta、health/ready 全部通过 | 本地/CI | CI | 不需要 |
| Migration e2e | `bash scripts/e2e/backend-adapter.sh migration` | Docker Desktop 或 CI Docker daemon | PostgreSQL 16 Up/Down/回放、schema probes、迁移 control/journal 零-skip contract | 本地/CI | CI | 不需要 |
| Runtime e2e | `bash scripts/e2e/backend-adapter.sh runtime` | Docker Desktop 或 CI Docker daemon | PostgreSQL/Redis worker slice、lease、relay、recovery 零-skip contract | 本地/CI | CI | 不需要 |
| Framework telemetry metrics bridge | `go test ./trpcservice/telemetry/otel` | Go 1.25.x | tRPC-Agent-Go 公开 metric 初始化入口与 service metrics 使用同一 Provider；SDK 自动 trace 未启用，以避免未经公开策略配置的 payload 导出 | 本地/CI | CI | 不需要 |
| Storage e2e | `bash scripts/e2e/backend-adapter.sh storage` | Docker Desktop 或 CI Docker daemon | MinIO、Qdrant、Vault 的真实协议 adapter 均零-skip；临时资源自动清理 | 本地/CI | CI | 不需要 |
| IM e2e | `bash scripts/e2e/im.sh` | Go 1.25.x | Feishu/WeCom 签名 callback、1000 并发重复投递与 durable ingress 契约 | 本地/CI | CI | 真实渠道另需人工 |
| Prometheus 配置 | `docker run --rm --volume "$PWD/deploy/compose/prometheus.yml:/etc/prometheus/prometheus.yml:ro" --entrypoint promtool prom/prometheus:v2.54.1 check config /etc/prometheus/prometheus.yml` | Docker daemon | `promtool` 成功解析 scrape 配置 | 本地/CI | CI | 不需要 |
| Session 迁移控制面 | 包含在 `bash scripts/e2e/backend-adapter.sh migration` | Docker Desktop 或 CI Docker daemon | current source 与 immutable candidate 的绑定推导；verification/watermark 仅从迁移 authority 读取；cutover/observe/rollback CAS 请求通过 | 本地/CI | CI | 不需要 |
| Memory Redis→PostgreSQL 数据面 | `bash scripts/e2e/backend-adapter.sh runtime`；PostgreSQL ledger contract 同时包含在 `migration` | Docker Desktop 或 CI Docker daemon | 两 tenant 路由、跨 Worker 可见性、Redis export 的跨租户过滤、backfill、双写整用户镜像、repair ledger 的 claim/ack 均通过；不包含 Memory cutover | 本地/CI | CI | 不需要 |
| Knowledge 迁移 operator role / SDK Qdrant runtime | `go test ./cmd/trpc-service ./trpcservice/migration/... ./trpcservice/storage/knowledge/qdrant`（亦包含于 migration/storage e2e） | Go 1.25.x、后端集成 job 另需 Docker | role 拒绝非法环境；step、journal recovery、官方 Qdrant gRPC scope filter 与 envelope 复核通过 | 本地/CI | CI | 不需要 |
| Knowledge 迁移切换与收尾所有权 | `POST /v1/tenants/{tenant}/knowledge-migrations/{migration}/{cutover,observe,rollback,cleanup}` | 认证 Admin principal、PostgreSQL control plane | operator repair/推进 pre-cutover；Admin 显式切流、观察、回滚与 cleanup | 生产控制面 | 否 | 需人工 |
| Skill / Knowledge 装配 | 包含在 `bash scripts/e2e/backend-adapter.sh storage` | Docker Desktop 或 CI Docker daemon | published Skill digest/name 固定；Knowledge manifest/backend/embedder/vector generation 不一致或缺失均 fail-closed | 本地/CI | CI | 不需要 |
| Graph 条件边 | `go test ./trpcservice/agent/condition ./trpcservice/agent` | Go 1.25.x | `present/empty` 分支固定；未知条件、缺分支、混合普通/条件边被拒绝 | 本地/CI | CI | 不需要 |
| CodeExec 控制面 | 包含在 `bash scripts/e2e/backend-adapter.sh storage` | Docker Desktop 或 CI Docker daemon | ToolRef digest 固化、workspace root 为 `0700`、symlink root 被拒绝 | 本地/CI | CI | 真实执行器另需人工 |
| MCP 适配器 | `go test ./trpcservice/tool/mcp ./trpcservice/tool ./trpcservice/worker` | Go 1.25.x | scoped secret、声明/binding digest、私网 DNS 拒绝、Worker 上下文固定全部通过 | 本地/CI | CI | 真实 MCP 服务另需人工 |
| PostgreSQL/Redis runtime slice | `docker compose -f deploy/compose/docker-compose.local.yml up -d postgres redis` 后执行 `--profile runtime-test run --rm runtime-test` | Docker Desktop | migration 与真实 PostgreSQL/Redis slice 通过 | 本地/CI | CI（runtime e2e） | 不需要 |
| WebUI + 模型/Knowledge | `./start.sh`，打开 `http://localhost:58081/webui/` | Docker Desktop、DeepSeek Key | `/readyz` 为 200；一次文本对话得到回复；Qdrant fixture、fake embedding、Skill 与 Knowledge manifest 就绪 | 本地 + 真实模型 | 否 | 需人工 |
| 渠道真机联调 | [channel-live-validation.md](channel-live-validation.md) | 对应渠道的真实凭据与回调/接收模式 | request ID、回复消息 ID 与 terminal audit ID 的真实往返 | 人工发布门禁 | 否 | 需人工 |
| CodeExec Worker | `docker compose -f deploy/compose/docker-compose.local.yml --profile codeexec up worker-codeexec` | Docker Desktop、`.env.local` 中经 digest 审核的 `TRPC_CODE_EXECUTORS` | 专用 image、bubblewrap、私有 workspace；ToolRef 与治理许可齐备 | 本地 | 本地 | 需人工 |
| 依赖短断恢复 | `bash scripts/e2e/dependency-recovery.sh` | Docker Desktop、DeepSeek Key | PostgreSQL/Redis 中断期间节点 unready，恢复后无需重启重新 ready | 本地 + 真实模型 | 本地 | 需人工 |
| Feishu 文本/群聊 | 依据 [getting-started.md](getting-started.md) 第 3 节启动 `feishu-local` 并配置 tunnel | DeepSeek Key、`feishu.env`、Feishu 应用、临时 HTTPS URL | callback 验签成功；私聊收到回复；群聊仅 @ 机器人时处理 | 真实外部 | 否 | 需人工 |
| Feishu 图片 | 在同一 Feishu p2p 会话发送一张新的 JPEG/PNG/GIF/WebP（≤10 MiB） | 同上、ClamAV healthy、视觉模型 | 回复基于图片内容；日志显示媒体下载、扫描和 prepared input | 真实外部 | 否 | 需人工 |
| WeCom 文本/图片 | 依据 [getting-started.md](getting-started.md) 第 4 节启动 `wecom-local` 并配置 tunnel | DeepSeek Key、`wecom.env`、WeCom 自建应用、临时 HTTPS URL | callback URL 验证成功，消息收到官方 Reply API 回复；图片经扫描后进入视觉模型 | 真实外部 | 否 | 需人工 |
| 可观测性 | 打开 `http://localhost:56686/` 与 `http://localhost:59464/metrics` | 本地 Compose profile | Jaeger 可查询 trace，metrics endpoint 返回 200 | 本地 | 本地（promtool 为 CI） | 需人工 |

## 参考资产（非验收门禁）

- [`deploy/prometheus-alerts.yml`](../../deploy/prometheus-alerts.yml) 说明 broker/audit/quarantine
  指标在远端 Prometheus 中可采用的六条告警语义；它刻意不被本地 Compose 的 Prometheus
  加载。CI 只用 `promtool` 解析其语法，不能证明 metrics 已被真实抓取或告警已送达。
- [`deploy/kubernetes/base`](../../deploy/kubernetes/base/README.md) 以 gateway/worker 展示 role、
  preStop drain、PDB 与 Worker deny-ingress 的设计。它不属于 CI 或本地 Compose 验收，不提供
  环境 overlay、Helm chart、集群发布或 rollout 证明。

## 已知边界

- Feishu 的群聊文本需要可靠的机器人 mention；**media-only 群消息会被忽略**。图片/文件的真实验收只以 p2p 为准。
- WeCom 支持的是自建应用 Agent callback，不包含智能机器人长连接或 Bot 流式协议。
- 非图片文件会下载、扫描和审计，但当前本地视觉模型不宣称理解 PDF 或 Office 内容。
- 审计查询、保留和销毁的角色、权限与不可变约束见 [能力与兼容边界](../design/8.capability-boundaries.md)；破坏性 purge 不暴露 HTTP 端点。
- 通用 distroless Worker 镜像没有打包 `bubblewrap`、`bash`、`python`。因此 CodeExec 的默认状态是未配置；启用它前必须使用专用 sandbox-capable Worker 镜像并完成禁网、cgroup/进程上限和每 turn 清理的部署演练。不能把本表的控制面契约当作主 Worker 内执行任意代码的证据。

完整的启动、密钥格式、回调地址和故障排查步骤见 [本地 Docker Desktop 验收指南](getting-started.md)。
