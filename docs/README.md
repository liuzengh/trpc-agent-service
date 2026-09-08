# 交付文档

本目录只保留最终设计、安装运维和验收资料。题目原文见根目录 [README](../README.md)；开发日志、逐轮测试记录和早期教程不随当前源码交付。

| 文档 | 用途 |
| --- | --- |
| [architecture.md](architecture.md) | 总体架构、系统图、组件职责、租户隔离与框架复用边界 |
| [sequence.md](sequence.md) | 企业微信完整消息时序、工具执行、Session/Memory 和回复链路 |
| [data-model.md](data-model.md) | 核心实体、关系、表结构和索引 |
| [data-consistency.md](data-consistency.md) | 并发写入、事件顺序、幂等、恢复与数据迁移 |
| [backend-adapters.md](backend-adapters.md) | Redis / SQL / 向量库 / 对象存储选择和租户路由 |
| [im-channels.md](im-channels.md) | Telegram、企业微信两种接入方式、配置及媒体边界 |
| [governance-operations.md](governance-operations.md) | 鉴权、工具审批、密钥、预算、审计和监控 |
| [operations-runbook.md](operations-runbook.md) | 安装、配置、手动启停、部署、升级、容量与恢复 |
| [risks.md](risks.md) | 20 项生产风险及缓解措施 |
| [acceptance.md](acceptance.md) | 题目映射、可重复验证入口和明确的交付限制 |

先运行服务：读运行手册。了解实现：读架构与时序。检查交付：读验收说明。图采用 Mermaid，表结构示例与实际 migrations 的区别在数据模型文档中说明。

默认交付只包含源码、配置模板、部署文件和自动测试。模型/IM 密钥、会话和后端数据、原始 trace、日志、二进制及本机备份不包含在源码包中。
