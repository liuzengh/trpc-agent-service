# 文档导航

本仓库的可执行验收边界是本地 Docker Desktop：Compose、单元/契约测试和按域 e2e。
不提供 Kubernetes 发布、远端告警或生产 Provider 验收资产；相关文件只帮助验收者审阅运行契约。

## 从这里开始

| 目标 | 阅读或执行入口 |
| --- | --- |
| 无凭据从零验收 | [根目录 README](../README.md#快速开始) 与 [从零复现流程](runbook/getting-started.md#acceptance-from-zero) |
| 查看每项验收的资源、命令和成功证据 | [本地验证矩阵](runbook/verification-matrix.md) |
| 排查本地运行、密钥、回调或 IM 联调 | [本地运行手册](runbook/getting-started.md) |
| 验证故障恢复、容量与回滚 | [可靠性、发布与容量 Runbook](runbook/reliability-release-capacity.md) |
| 审阅交付范围、组件与风险 | [交付架构总览](architecture-delivery.md) |
| 查阅规范性设计 | [设计索引](design/README.md) |
| 理解空数据库 schema 基线与后续追加规则 | [迁移基线](../migrations/README.md) |

## 文档约定

- `design/` 记录当前架构、领域模型、协议、数据一致性、安全边界与验证规范；不记录开发轮次、里程碑或提交历史。
- `runbook/` 只给出可复现的本地操作、资源前提和证据边界。没有真实账号或凭据的结果不得表述为真实 Provider 已通过。
- 所有仓库内 Markdown 链接和标题锚点由 `bash scripts/ci/check-doc-links.sh` 校验；新增或移动文档时必须同步更新引用。
- 服务仅依赖 `trpc-agent-go` 的公共 API；生产代码和技术方案不得依赖其他 Go module 的 `internal` 包。
