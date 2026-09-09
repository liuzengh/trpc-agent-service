# Enterprise Multi-Tenant Agent Platform 交付指南

作者：王子龙

## 交付物

- 平台源码、测试、数据库迁移、Compose/Kubernetes 模板和 CI 配置。
- 项目方案、架构与数据模型、安全与风险、运行及验收文档。
- 本地演示、基准、验证日志、包内文件清单和 SHA-256。

完整目录职责见[交付内容索引](PACKAGE_MANIFEST.md)，题目映射与设计见[竞赛方案](docs/COMPETITION_SUBMISSION.md)。

## 启动顺序

1. 在源码根目录运行 `go run -buildvcs=false ./cmd/demo`，按[四步观察表](docs/JUDGE_QUICKSTART.md#2-两分钟故障演示)核验内存状态机。
2. 运行 `go test -buildvcs=false -count=1 -p 1 ./...` 验证源码；完整环境命令见 [验证方法](docs/VERIFICATION.md)。
3. 启动 Docker Linux engine，执行 `scripts/run_c_local_stack.ps1 -ProjectName agent-platform-review -Build`。
4. 确认迁移任务完成、应用健康、Prometheus 抓取正常，随后配置租户、Agent 版本和部署。
5. 使用真实 IM 配置完成接入检查，步骤见 [接入与部署验收](docs/EXTERNAL_ACCEPTANCE_RUNBOOK.md)。

本地隔离栈默认绑定 `127.0.0.1`：Gateway 18080、Admin 18081、Prometheus 19095、Grafana 13000。结束时使用同一 ProjectName 执行 `-Down`，命名卷保留。

## 配置责任

| 配置 | 管理方式 |
|---|---|
| 租户、模型、Agent 与通道绑定 | Admin API，受角色与 tenant allowlist 约束 |
| Version/Deployment | 无密钥不可变快照，stable/canary 和发布审计 |
| 数据库与 Session/Memory | operator-owned profile、独立账号、SecretRef |
| 模型/IM/MCP 凭据 | 租户与用途绑定，按进程最小权限注入 |
| 内部服务身份与观测 | HMAC、nonce、HTTPS/mesh、metrics token、OTLP TLS |
| 集群镜像与网络 | digest 固定、releaseverify、默认拒绝 NetworkPolicy |

生产 Session/Memory fencing 需要 PostgreSQL 直连或 PgBouncer session pooling。Admin 通过受控私网入口暴露；日常角色与 bootstrap 凭据分开管理。秘密不得写入源码、日志或交付包。

## 消息恢复

- 可重试错误：按照 retry policy 和 Provider 的 Retry-After 延迟重试。
- lease 丢失：陈旧 owner/fence 提交被拒绝，由有效租约继续处理。
- 结果未知：进入 `WAITING_RECONCILIATION`，核对外部结果后恢复。
- 死信：记录原因及关联 trace，由操作者携带 actor/reason 重放。
- Outbox resume：保留已确认 cursor；restart：从首段重发，操作前确认业务影响。

同 Session 的阻塞前序暂停后续消息，其他 Session 独立推进。提交与恢复契约见[数据同步与幂等设计](docs/DATA_SYNC_IDEMPOTENCY.md)，故障处理见[风险登记册](docs/RISK_REGISTER.md)。

## 验证记录

源码验证命令、退出码和环境保存在交付包 `verification-evidence/current-validation.log`。能力检查项及目标部署验收状态统一见 [验收矩阵](docs/ACCEPTANCE_EVIDENCE.md)。Kubernetes 发布步骤见 [部署指南](deploy/kubernetes/README.md)。

后端集成使用独立 PostgreSQL、Redis、Qdrant 和 MinIO 环境，按[验收矩阵](docs/ACCEPTANCE_EVIDENCE.md#3-真实后端集成)核对持久化后状态。环境变量和命令见[后端集成与部署检查](docs/VERIFICATION.md#4-后端集成与部署检查)，本地耗时与分配比较见[可靠性基准](docs/VERIFICATION.md#51-本地可靠性基准)。
