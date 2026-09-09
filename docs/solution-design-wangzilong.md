# 王子龙：多租户节点化 Agent 部署平台

作者：王子龙；提交分支：`feature/wangzilong`。

平台基于 tRPC-Agent-Go，提供租户级 Agent 配置与发布、共享 Session/Memory、可靠 IM 消息处理、可恢复数据迁移、工具治理和运维控制台。Session/Memory 可独立选择 Redis 或 PostgreSQL，Knowledge 使用 Qdrant，Artifact 使用 PostgreSQL 元数据与 S3-compatible 对象存储。

| 交付内容 | 阅读入口 |
| --- | --- |
| 项目运行与演示 | [项目首页](../README.md)、[评审指南](JUDGE_QUICKSTART.md) |
| 完整方案与框架复用边界 | [项目方案](COMPETITION_SUBMISSION.md) |
| 系统架构图与设计决策 | [架构设计](ARCHITECTURE.md)、[设计决策](ARCHITECTURE_REVIEW.md) |
| 数据模型、同步与消息幂等 | [数据模型](DATA_MODEL.md)、[同步与幂等](DATA_SYNC_IDEMPOTENCY.md) |
| 多后端选择与在线迁移 | [多后端方案](MULTI_BACKEND_DESIGN.md)、[迁移指南](ONLINE_MIGRATION.md) |
| 治理、监控与故障恢复 | [安全设计](SECURITY_REVIEW.md)、[风险清单](RISK_REGISTER.md)、[SLO](SLO.md) |
| 代码实现与部署 | [进程入口](../cmd/)、[平台模块](../pkg/)、[部署配置](../deploy/)、[运维控制台](OPERATIONS_CONSOLE.md) |
| 测试与实际接入结果 | [验证指南](VERIFICATION.md)、[验收矩阵](ACCEPTANCE_EVIDENCE.md)、[IM 接入验收](LIVE_IM_ACCEPTANCE.md) |

企业微信和 Telegram 的真实消息接收、Runner 模型执行与官方回复投递已完成验收。代码覆盖租户隔离、持久化队列、执行 fencing、Session/Memory/Knowledge/Artifact 路由、工具权限与审批、审计 tracing 及故障接管；测试对象和部署范围在验收文档中逐项说明。
