# 验证记录索引

这里保存每次验证当时的事实，不覆盖失败记录，也不把旧版本的成功自动继承到新功能。记录中的“已部署”只指当时的本地环境，不保证服务现在仍然运行。

## 接收方优先阅读

本次环境归档、文档重组与打包规则见[交付前准备](delivery-preparation-2026-09-08.md)。

1. [本地交付收尾](delivery-closeout-2026-09-08.md)：两租户 / 两 Worker、手动启停、隔离回归。
2. [本地工作项审批](workitem-approval-2026-09-08.md)：批准前无写入、批准后单次写入、重复批准不重做。
3. [Knowledge 与 Qdrant](knowledge-qdrant-2026-09-08.md)、[文档 MCP](project-docs-mcp-2026-09-08.md)、[长期记忆](memory-postgres-2026-09-07.md)、[文本附件](artifact-minio-2026-09-07.md)：各模块的真实开发环境证据。
4. [企业微信运行验证](wecom-runtime-2026-09-06.md)、[Telegram 验证](telegram-2026-09-03.md)：两类 IM 的基础链路，需结合后续修复记录阅读。

最新能力汇总见[功能状态](../feature-status.md)，本次交付范围见[基本交付说明](../delivery.md)。本机原始 trace、私有配置、数据库快照不在本目录；整理后的私有归档规则见[环境说明](../workspace-hygiene.md)。

## 全部记录（按文件名索引）

- [工具审批准备与验证记录](approval-2026-09-06.md)
- [本地 MinIO 附件持久化启用记录](artifact-minio-2026-09-07.md)
- [current_time 真实模型预检与 Sender 重试测试](current-time-2026-09-06.md)
- [本地交付收尾记录](delivery-closeout-2026-09-08.md)
- [rc.7 发送诊断补齐记录](delivery-diagnostics-2026-09-08.md)
- [Embedding 预检开发记录](embedding-check-2026-09-08.md)
- [真实文本向量与 Qdrant 启用记录](knowledge-qdrant-2026-09-08.md)
- [本地 rc.4 升级记录](local-rc4-upgrade-2026-09-07.md)
- [本地 PostgreSQL 长期记忆启用记录](memory-postgres-2026-09-07.md)
- [运维收尾记录](operations-2026-09-06.md)
- [业务工具与 IM 回执开发记录](operations-feedback-2026-09-06.md)
- [只读项目文档 MCP 本地启用记录](project-docs-mcp-2026-09-08.md)
- [0.2.0-rc.1 本地启用记录](rc1-rollout-2026-09-06.md)
- [真实模型开发环境验收（2026-09-03）](real-model-2026-09-03.md)
- [故障与业务恢复回归（2026-09-06）](recovery-2026-09-06.md)
- [0.2.0-rc.1 收尾候选记录](release-candidate-2026-09-06.md)
- [接口与密钥安全改动验证](security-2026-09-06.md)
- [Telegram 真实联调验收（2026-09-03）](telegram-2026-09-03.md)
- [完整链路追踪准备与验证](tracing-2026-09-06.md)
- [企业微信 MCP 开发环境启用记录](wecom-activation-2026-09-06.md)
- [企业微信真实图片与文本联调](wecom-media-2026-09-06.md)
- [企业微信 MCP 平台接入开发记录](wecom-runtime-2026-09-06.md)
- [企业微信 MCP 消息取样记录](wecom-sample-2026-09-06.md)
- [本地工作项审批启用记录](workitem-approval-2026-09-08.md)
