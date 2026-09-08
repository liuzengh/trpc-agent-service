# 文档导航

第一次接手项目，先读[基本交付说明](delivery.md)，再按[运行手册](operations-runbook.md)操作。不需要按时间顺序翻完所有验证记录。

## 交付与使用

- [基本交付说明](delivery.md)：交付范围、源码基线、文档清单和验证边界。
- [运行手册](operations-runbook.md)：安装配置、手动启停、升级与排障。
- [当前进度](execution-plan.md)：基本交付已完成什么，哪些属于后续增强。
- [功能状态](feature-status.md)：区分代码实现、自动测试、本地集成和真实联调。
- [功能配置参考](runtime-reference.md)：从原 README 移出的详细配置与命令。
- [环境整理说明](workspace-hygiene.md)：本地归档、构建产物、源码打包和数据保护规则。

## 题目要求与设计交付物

| 要求 | 对应文档 |
| --- | --- |
| 原始题目 | [requirements.md](requirements.md) |
| 总体架构、系统图、框架复用边界 | [architecture.md](architecture.md) |
| 企业微信完整消息时序、request/trace 传播 | [sequence.md](sequence.md) |
| 核心实体、关系与表结构 | [data-model.md](data-model.md) |
| 会话并发、同步顺序、幂等与迁移 | [data-consistency.md](data-consistency.md) |
| Redis / SQL / 向量库 / 对象存储适配 | [backend-adapters.md](backend-adapters.md) |
| 至少两种 IM 的接入差异 | [im-channels.md](im-channels.md) |
| 治理、安全、运维方案 | [governance-operations.md](governance-operations.md) |
| 生产风险与缓解措施 | [risks.md](risks.md) |
| 要求到实现和测试的映射 | [acceptance.md](acceptance.md) |

设计中列出的可选组件不等于当前代码已经全部支持，实际能力以功能状态表为准。Mermaid 图保留在 Markdown 中，可用支持 Mermaid 的阅读器渲染。

## 按模块查阅

- **学习代码**：[链路演进教程](getting-started.md)、[早期实施路线](implementation-roadmap.md)。教程的前几节刻意描述早期阶段，不是当前功能清单。
- **Telegram**：[手动运行](telegram-manual-runbook.md)、[工具调用](current-time-tool-walkthrough.md)、[审批](telegram-approval-walkthrough.md)、[追踪](telegram-tracing-walkthrough.md)、[发送诊断](telegram-delivery-diagnostics.md)。
- **企业微信**：[消息 MCP 接入](wecom-mcp.md)、[MCP 运行链路](wecom-mcp-runtime.md)、[异常恢复](channel-recovery.md)。
- **工具和数据**：[工具业务幂等](tool-operations.md)、[IM 回执与媒体边界](im-feedback.md)、[文档 MCP](project-docs-mcp.md)、[Embedding 配置](knowledge-embedding-setup.md)、[Knowledge 运行链路](knowledge-runtime.md)。
- **部署运维**：[部署拓扑](deployment.md)、[权限](deployment-permissions.md)、[安全边界](security-boundaries.md)、[监控](monitoring.md)、[容量](capacity.md)。

## 历史与验证证据

- [验证记录索引](validation/README.md)：优先列出最近的收尾和真实联调证据，失败或部分通过的记录也保留。
- [原执行清单](history/execution-plan-2026-09-08.md)：保留阶段性进度，不再当作待办。
- [rc.3 代码补齐](code-gap-closure.md)、[rc.4 可靠性修复](reliability-followup.md)：解释当时的实现变更，版本状态可能已经被后续记录更新。
- [历史 Mock 性能基线](benchmarks/local-mock-2026-09-03.md)：不是最新版真实模型容量报告。

## 术语约定

`tenant_id` / `app_id` 标识租户和应用；`revision_id` 标识不可变配置。
`request_id` 用于关联一次逻辑请求及其恢复记录，`trace_id` 用于追踪执行链路。
`storage_scope` 是平台传给 tRPC-Agent-Go `AppName` 的内部命名空间。
平台注入并校验这些边界，不能直接相信外部消息中的租户或存储作用域。
