# 文档与运行入口

本项目包含多租户 Agent 平台的架构设计与基于 tRPC-Agent-Go 的参考实现。建议依次阅读平台方案、架构与时序、数据设计、运行指南和验证结果；各文档分别标明目标生产能力与当前实现。

## 架构与源码

| 主题 | 内容入口 |
| --- | --- |
| 架构设计文档 | [平台方案](solution.md)，主文集中说明目标、架构与关键决策；协议、数据、治理和风险细节见专项文档 |
| 系统架构图 | [总体架构](architecture.md#2-系统架构图)，展示 Gateway、Worker、Channel、Storage、治理与 Telemetry |
| 核心消息时序图 | [企业微信完整链路](sequence.md#1-企业微信完整链路)，含 Runner、Tool、Session/Memory、回复与 request/trace 关联 |
| 数据模型 | [实体及 ER 图](data-model.md)，含 tenant、agent、binding、session、event、memory、summary、audit |
| 同步与幂等策略 | [存储与一致性](storage-and-consistency.md)，含并发、派生数据、入站去重、恢复与迁移 |
| 多后端方案 | [数据放置](storage-and-consistency.md#2-数据放置)，覆盖 SQL、Redis、向量库、对象存储 |
| 风险与缓解 | [十三项生产风险](risks.md)，说明触发条件、当前边界、监测/降级和生产缓解 |
| GitHub 实现代码 | [源码仓库](https://github.com/d2bz/trpc-agent-service/tree/feature/Wang-Pengfei)，分支 `feature/Wang-Pengfei`；代码入口为 `cmd/trpc-service` 与 `trpcservice` |

## 运行与验证

| 部署方式 | 内容与状态 |
| --- | --- |
| [本地部署](local-deployment.md) | 当前参考实现的构建、配置与运行步骤 |
| [生产部署设计](production-deployment.md) | 拓扑、部署前提、扩缩容、发布回滚及恢复，尚未提供可执行生产部署包 |

按[本地部署](local-deployment.md)构建并启动，默认网页使用确定性模型，无需外部凭据。通过[IM 接入指南](im-channels.md)可配置企业微信智能机器人和飞书自建应用的单聊文本。

[实现与验证](acceptance.md)列出设计主题、代码能力、可复现检查与真实消息结果，覆盖网页发送/续聊、版本 Pin，以及企微、飞书的正常收发和持久 Run/Outbox 状态。平台成功回执不代表用户已读，真实正常路径不替代故障测试。

## 技术参考

- [框架能力与平台职责](project-foundation.md)：复用边界、依赖与术语。
- [Admin API](admin-api.md)、[安全与治理](security-and-governance.md)、[Tool Policy](tool-policy.md)：配置、权限及运行契约。
- [Session 后端](session-backend.md)、[Session 租约](session-lease.md)：后端差异、持久化与并发保证。
- [社区 IM 扩展](im-channels.md#社区接入契约)：适配器接口、生命周期与验证要求。

## 实现边界

参考实现包含多租户配置、Revision、Runtime/Runner、持久 Session、HTTP/SSE、网页、两类 IM 文本、工具权限及可选三阶段观测。Memory/Summary、媒体/卡片、迁移、完整 Trace/成本治理和生产节点编排已有设计及风险说明，尚未实现。示例配置只包含开发占位值，真实密钥由运行者在本地提供。
