# 交付内容索引

项目：Enterprise Multi-Tenant Agent Platform

交付包的 `platform-source/` 保存源码，`verification-evidence/` 保存对应提交的验证日志；包根目录包含文件清单和 SHA-256 校验文件。

## 目录职责

| 路径 | 内容 |
|---|---|
| `cmd/` | 独立进程、管理命令与本地演示入口 |
| `pkg/` | 控制面、数据面、协议、治理和可靠状态机 |
| `migrations/` | PostgreSQL schema、嵌入式迁移器、up/down 和 checksum |
| `deploy/` | Compose、Kubernetes 源模板、监控和容器构建 |
| `scripts/` | 构建、验证、接入配置、发布和打包工具 |
| `test/` | PostgreSQL/Redis/Qdrant/MinIO 集成测试 |
| `docs/` | 项目方案、架构设计、数据模型、数据同步与幂等、安全、运行和验收 |
| `.github/workflows/` | 自动化验证流程 |
| `go.mod` / `go.sum` / `LICENSE` | 依赖版本、校验与许可 |

数据库初始化包含 `migrations/001..048`，由嵌入式迁移器按顺序执行并校验 checksum。

## 命令入口

| 命令 | 职责 |
|---|---|
| `cmd/gateway` | IM 验证、租户/通道路由、限流、Inbox 提交 |
| `cmd/consumer` | FIFO/fence 领取、Worker 调用、Inbox/Outbox 事务衔接 |
| `cmd/worker` | 固定版本 Runner、共享数据面、治理和执行结果持久化 |
| `cmd/summary-worker` | 摘要领取、生成、预算、fenced checkpoint、排空 |
| `cmd/delivery` | Outbox dispatch fence、分段、重试、核对和 DLQ |
| `cmd/admin` | 租户与 Agent 生命周期、审批和审计 |
| `cmd/migrate` | schema 迁移与状态查询 |
| `cmd/data-migrate` | 在线数据迁移创建、推进、状态查询、暂停、终止、切换回滚与完成 |
| `cmd/replay` | 带 actor/reason 的 Inbox/Outbox 恢复 |
| `cmd/releaseverify` | Kubernetes workload、digest、传输和网络策略门禁 |
| `cmd/demo` | MemoryStore 本地故障状态演示 |

Admin/Worker 在进程启动时封存 runtime registry；配置、HTTP、策略和装配保留在各自组合根内，领域规则由 `pkg/` 共享。

## 核心模块

| 模块 | 主要契约 |
|---|---|
| `reliable` / `pipeline` | 幂等、同 Session FIFO、lease/fence、原子完成、dispatch fence 和审计重放 |
| `controlplane` | immutable Version、stable/canary、重试版本绑定、execution guard 与 reconciliation |
| `tenant` / `adminauth` | 加密配置、SecretRef 作用域、Principal/RBAC |
| `storage` / `worker` | 共享 Session/Memory、调用租约、Runner 缓存和模型执行 |
| `governance` / `platformtool` | Plugin、预算、审批、脱敏、MCP 准入与工具白名单 |
| `summary` / `summaryruntime` | 事件边界、固定版本生成、checkpoint CAS、Runner overlay |
| `runtimeplane` / `knowledgeplane` / `artifactplane` | operator profile、Qdrant 作用域、S3/MinIO 对象及版本元数据 |
| `datamigration` / `dataprojection` | 迁移状态机、持久化 intent/journal/route、snapshot/catch-up、Session/Knowledge/Artifact 投影、完整比对、配置 CAS 与 lease/fence |
| `migrationruntime` | Session/Knowledge/Artifact 生产装饰器、实际后端 inventory/record 适配及跨缓存路由 |
| `telemetry` / `health` | trace、指标、审计、readiness、drain |
| `releaseverify` | 受控应用发布物及网络、密钥和迁移前置条件 |

Worker 装配 Session/Knowledge/Artifact 迁移装饰器，Summary Worker 装配相同 Session 装饰器；`cmd/data-migrate` 驱动生产协调器，`cmd/migrate` 负责 schema。集成入口为 `test/integration/online_session_migration_test.go` 和 `online_dataplane_migration_test.go`。支持范围、完整记录比对、扫描延迟、源写目标读的回滚窗口和恢复命令见[迁移运行指南](docs/ONLINE_MIGRATION.md)，对应运行结果保存在交付证据中。

## 验证与配置

[项目方案](docs/COMPETITION_SUBMISSION.md)包含组件与协议细节，[架构设计](docs/ARCHITECTURE.md)说明系统边界和设计取舍，[数据同步与幂等设计](docs/DATA_SYNC_IDEMPOTENCY.md)定义逐域提交、可见性、摘要顺序与恢复契约。

[验证方法](docs/VERIFICATION.md) 说明测试环境与命令，[验收矩阵](docs/ACCEPTANCE_EVIDENCE.md) 说明各能力的检查项，[交付指南](ENTERPRISE_PLATFORM_HANDOFF.md) 说明启动和接入顺序。

本地 Compose 使用进程内测试配置；目标部署通过 Secret Manager 或受保护的环境文件注入凭据。运行数据、实际凭据和本机生成的部署快照不包含在源码包中。
