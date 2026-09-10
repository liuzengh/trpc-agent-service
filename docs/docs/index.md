# tRPC-Agent-Service

基于 [tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) 的多租户节点化 Agent 部署平台。

项目最初的任务描述、交付物和验收标准已归档至[项目交付基线](project-brief.md)；本页及各专项文档记录当前已完成实现和验证入口。

## 平台能力

- **多租户隔离**：租户级配置、数据、工具权限、审计与密钥隔离
- **节点化部署**：Gateway、Worker、共享 Session/Memory、队列和 Outbox 的无状态协作
- **多后端支持**：InMemory、PostgreSQL、Redis 和 S3-compatible runtime capability
- **IM 接入**：企业微信自建应用、企业微信 AI Bot、Telegram 长轮询/webhook 与媒体回复
- **治理与可观测**：工具策略、OpenTelemetry、Prometheus/Grafana、租户审计与成本统计
- **故障恢复**：重试/DLQ、lease fencing、取消安全、灰度发布和租户级配置回滚

## 当前运行时设计

- [Model Profile、Secret Resolver 与最小 Runner 链路](model-profile.md)：Model Profile、Secret Resolver、Execution Plan 和 deterministic Runner 链路。
- [Issue #71：Tenant-scoped Provider Registries](issue-71-provider-registries.md)：Secret、Model、Backend、Channel 的租户隔离注册表契约。
- [Issue #69：Multi-tenant Bootstrap](issue-69-multi-tenant-bootstrap.md)：多租户 API identity、动态 Session capability 和按租户审计路由。
- [Issue #72：Precise Cache Invalidation](issue-72-cache-invalidation.md)：按 tenant/app/profile 精确失效，并保持 in-flight Runner 快照。
- [Issue #82：Agent App Registry 与租户灰度](issue-82-agent-app-registry.md)：实例内按租户选择不可变 candidate revision、授权变更、审计事实和 lease-safe 回滚。
- [生产架构设计](architecture.md)：控制面/数据面拓扑、可信 Channel Binding、企业微信全链路、
  幂等、迁移和能力边界的实现基线。
- [Gateway、Execution Plan 与 HTTP/SSE](gateway.md)：对齐 PR #25 架构验收与 Issue #26
  可信主体，定义 Issue #28 的 Resolver、Runner Registry、Dispatch、普通/SSE API、
  限流、幂等和服务生命周期契约。
- [Telegram 长轮询 Adapter](telegram.md)：Issue #31 的交付契约，固定单 Binding、Bot
  身份校验、普通文本映射、Dispatch 聚合回复和生命周期边界。
- [多租户多 Agent Telegram / WeCom 验收链路](multitenant-telegram-acceptance.md)：从 Compose 启动、WebUI 配置、双 Bot 连接到跨租户真实会话和故障定位。
- [企业微信自建应用 Channel Adapter](wecom.md)：Issue #60/#98 的契约，固定 callback
  验签/AES 解密、可信 Binding 路由、文本/媒体入站和可靠回复边界。
- [Telegram live E2E 示例](https://github.com/XnLemon/trpc-agent-service/tree/main/examples/telegram-e2e)：
  Issue #33 的真实 Bot API 传输冒烟测试和手动 CI 运行说明。
- [PostgreSQL 控制面与启动装配](postgresql-control-plane.md)：Issue #37 的实现契约，
  复用既有表设计并统一 migration、Repository 事务边界、bootstrap、readiness 和 shutdown。
- [Issue #81：MySQL 控制面 Repository](issue-81-mysql-control-plane.md)：MySQL 与 PostgreSQL
  的 SQL 语义映射、事务/锁、迁移、Bootstrap 选择和验证矩阵。
- [Issue #41：可重启控制面与 Admin API](issue-41-runtime-bootstrap-admin-api.md)：已合并的实现契约，
  固定真实 Bootstrap、readiness、最小 Admin API、重启恢复和验收矩阵。
- [Issue #67：首次运行初始化](issue-67-first-run-init.md)：显式 `trpc-service init`、数据库状态判定、
  并发幂等和 local/staging/production 首次运行流程。
- [Issue #101：本地 golden path](deployment.md)：`quickstart.sh --demo` 从空 PostgreSQL
  到第一条 deterministic `/v1/chat`，并与生产显式初始化路径保持一致的验收入口。
- [Issue #50：Reliable Reply Delivery](issue-50-reliable-delivery.md)：Outbox worker、Provider
  交付、重试/DLQ、lease recovery、telemetry 与验收 ledger。
- [租户审计与用量成本](audit-usage.md)：Issue #54/PR #55 的实现契约，固定 mandatory audit
  与 telemetry 边界、事件 schema、append-only/重复语义、失败策略、聚合和运维 ledger。
- [Issue #79：生产可观测性、Dashboard 与告警](issue-79-observability.md)：trace/metrics 链路、
  低基数标签、租户授权查询以及 Prometheus/Grafana 资源和告警。

## 当前交付与验收

当前分支已完成控制面、运行时、Gateway、Channel、可靠回复、审计、可观测性和部署链路的代码与文档验收。
统一验证入口如下：

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
python -m mkdocs build --strict -f docs/mkdocs.yml
./scripts/validate-deployment.sh
```

真实部署验收使用 [部署文档](deployment.md) 的 Compose golden path、Kubernetes Kustomize
预检、WeCom deterministic callback E2E、fault-injection E2E 和 Telegram live E2E。

## 快速开始

生产入口、Docker Compose 快速开始、开发 golden path、Kubernetes 清单和完整环境变量参考见
[部署、配置与快速开始](deployment.md)。

```bash
git clone https://github.com/XnLemon/trpc-agent-service.git
cd trpc-agent-service

./scripts/build.sh
./scripts/start.sh
```

停止服务:

```bash
./scripts/stop.sh
```

## 文档导航

- [架构设计](architecture.md) — 组件拓扑、可信路由、消息链路、数据同步和多后端迁移
- [数据模型](data-model.md) — 核心表结构、Session/Event/Memory/Summary/Audit 和租户约束
- [Channel Binding](channel-binding.md) — 租户级通道绑定、候选发现与可信入站路由
- [Telegram 长轮询 Adapter](telegram.md) — 单 Binding Telegram long polling、文本映射与安全边界
- [多租户多 Agent Telegram / WeCom 验收链路](multitenant-telegram-acceptance.md) — 两租户、两 Agent、双渠道真实对话验收操作单
- [企业微信自建应用 Channel Adapter](wecom.md) — 自建应用 callback、文本/媒体入站与回复 Outbox
- [企业微信 AI Bot 长连接](wecom-aibot.md) — `wecom_aibot` WebSocket、认证、重连与流式回复
- [Gateway、Execution Plan 与 HTTP/SSE](gateway.md) — 可信主体、固定执行计划、Runner Registry、
  Dispatch、健康检查、优雅停机和普通/流式 API
- [PostgreSQL 控制面与启动装配](postgresql-control-plane.md) — 六类控制面表的 migration 顺序、
  SQL Repository 事务和真实运行时启动装配
- [Issue #81：MySQL 控制面 Repository](issue-81-mysql-control-plane.md) — MySQL 适配器、迁移、
  Bootstrap 驱动选择和租户隔离验证
- [Issue #41：可重启控制面与 Admin API](issue-41-runtime-bootstrap-admin-api.md) — Bootstrap/readiness、
  管理 API、重启恢复和测试矩阵
- [Issue #67：首次运行初始化](issue-67-first-run-init.md) — 显式首租户/App 初始化命令、幂等并发和部署流程
- [Issue #50：Reliable Reply Delivery](issue-50-reliable-delivery.md) — 回复 Outbox 交付、Provider、
  重试/DLQ、恢复与运维证据
- [租户审计与用量成本](audit-usage.md) — 版本化事件、append-only writer、用量成本聚合、
  保留/脱敏/访问控制和 failure/repair 规则
- [Issue #79：生产可观测性、Dashboard 与告警](issue-79-observability.md) — trace、metrics、
  dashboard、告警和租户查询边界
- [运维方案](ops.md) — 发布灰度、监控审计、故障恢复、容量模型和生产风险清单

## CI

仓库在 push 到 `main` 和 PR 时自动运行格式、静态检查、构建、测试覆盖率与文档构建,详见仓库根目录的 `.github/workflows/`。
