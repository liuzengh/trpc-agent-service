# 评委快速入门

本指南提供 Enterprise Multi-Tenant Agent Platform 的演示、源码核验和阅读入口。评审重点是租户独立选择数据后端、Worker 接管恢复，以及存储迁移的增量捕获与切换回滚。

## 1. 先看整体链路

消息路径为 IM→Gateway→Inbox→Consumer→Worker→Outbox→Delivery→IM。Summary Worker 异步发布 checkpoint，下一轮 Worker 读取摘要。组件接线、存储所有权与 at-least-once 恢复边界见[架构设计](ARCHITECTURE.md)。

## 2. 两分钟故障演示

安装 Go 后，在源码根目录运行：

```powershell
go run -buildvcs=false ./cmd/demo
```

程序使用 `pkg/reliable.MemoryStore`，输出 JSON 事件，断言失败时返回非零退出码；无需模型账号、IM 凭据或公网回调。

| 步骤 | 操作 | 观察结果 |
|---|---|---|
| 消息入队 | 创建带租户、会话和消息标识的 Inbox | 分配消息 ID 与 Session sequence |
| 租约接管 | 首个 owner 的短租约到期，新 owner 重新领取 | 旧 owner 使用旧 fence 完成消息时得到 `ErrStaleLease` |
| 完成衔接 | 新 owner 完成 Inbox 并创建回复 | 生成关联 Outbox |
| 未知结果 | 标记发送开始后让租约过期，再执行 reap | Outbox 转入 reconciliation，停止自动重发 |

演示只验证内存状态机，不启动 PostgreSQL 或独立 Worker 进程。持久化、数据库事务和真实后端接管见[后端集成与部署检查](VERIFICATION.md#4-后端集成与部署检查)。

## 3. 核验实现

```powershell
go test -buildvcs=false -count=1 -p 1 ./...
go vet -p 1 ./...
```

本提交的执行记录位于交付包 `verification-evidence/current-validation.log`。完整源码、race、真实后端、镜像和监控规则验证见 [VERIFICATION.md](VERIFICATION.md)。

建议重点检查以下断言：

| 关注点 | 预期行为 | 阅读入口 |
|---|---|---|
| 租户数据组合 | Session 与 Memory 独立选择 Redis/PostgreSQL；Knowledge/Artifact 选择授权的实例与命名空间 | `pkg/storage/backend_factory.go`、`pkg/runtimeplane/resolver.go`、[多后端设计](MULTI_BACKEND_DESIGN.md) |
| 企业微信完整消息链路 | 两种交叉后端组合均完成加密回调、持久入队、Runner/Memory Tool、回复投递；覆盖 token 刷新与活动请求取消 | `test/integration/wecom_callback_e2e_test.go`、[执行命令](VERIFICATION.md#4-后端集成与部署检查) |
| 版本稳定性 | 重试继续使用已绑定 AgentVersion，运行时注册表启动后封存 | `pkg/controlplane`、`pkg/worker` |
| 租户隔离 | 入口身份、SecretRef、存储作用域和工具授权相互一致 | `pkg/tenant`、`pkg/governance` |
| 消息可靠性 | 同 Session FIFO；旧 lease/fence 不能提交；Inbox/Outbox 原子衔接 | `pkg/reliable`、`pkg/pipeline` |
| 外部副作用 | 已开始发送但结果未知时进入 reconciliation | `pkg/pipeline/delivery.go` |
| 长会话 | 冻结摘要边界、fenced checkpoint、下轮请求裁剪已覆盖历史 | `pkg/summary`、`pkg/summaryruntime` |
| 在线迁移与多后端投影 | 生产写入捕获、intent 恢复、snapshot/catch-up、完整记录比对、配置 CAS 和回滚；旧缓存随持久化路由切换 | `cmd/data-migrate`、`pkg/migrationruntime`、`pkg/datamigration`、`test/integration/online_session_migration_test.go`、`online_dataplane_migration_test.go` |

集成测试的运行范围和断言见[验收证据](ACCEPTANCE_EVIDENCE.md)。数据迁移使用 `cmd/data-migrate`，schema 迁移使用 `cmd/migrate`；支持数据域、准入条件、扫描影响和恢复命令见[迁移运行指南](ONLINE_MIGRATION.md)。

## 4. 阅读顺序

1. [竞赛方案](COMPETITION_SUBMISSION.md)：题目映射、总体设计和技术取舍。
2. [架构设计](ARCHITECTURE.md)、[数据模型](DATA_MODEL.md)与[数据同步与幂等设计](DATA_SYNC_IDEMPOTENCY.md)：组件边界、状态所有权与同步恢复。
3. [多后端设计](MULTI_BACKEND_DESIGN.md) 与 [在线迁移](ONLINE_MIGRATION.md)：租户组合、存储取舍和切换恢复。
4. [验收证据](ACCEPTANCE_EVIDENCE.md)：功能对应的测试和验收断言。
5. [安全设计](SECURITY_REVIEW.md) 与 [SLI/SLO](SLO.md)：授权、故障处置与服务目标。

## 5. 接入测试账号

企业微信和 Telegram 账号收发使用应用凭据与公网 HTTPS 回调。配置后执行文本、重复投递、限流和回复检查；Kubernetes、密钥系统、HA/DR 和业务容量按同一部署清单验证。步骤和结果记录见 [验收证据](ACCEPTANCE_EVIDENCE.md#5-目标环境验收) 与 [目标环境验收 Runbook](EXTERNAL_ACCEPTANCE_RUNBOOK.md)。
