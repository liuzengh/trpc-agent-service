# 多租户 Agent 部署平台

基于 tRPC-Agent-Go 的 Agent 管理与运行平台。支持多个租户独立配置模型、工具、知识库和存储后端，通过网页工作台发布 Agent，并接入 Telegram 或企业微信。

## 主要能力

- 多租户配置、会话、工具权限、密钥和审计隔离。
- Agent 草稿、在线调试、不可变版本发布、灰度和回滚。
- Gateway、Worker、消息投递与后台任务分角色部署，支持共享后端下的多节点运行。
- Session/Memory 支持 Redis、PostgreSQL 和 InMemory；Knowledge 支持 Qdrant；Artifact 支持 S3-compatible 存储。
- IM 消息去重、持久化任务、失败恢复、工具审批和投递状态查询。
- 租户预算、日志脱敏、OpenTelemetry、监控告警和审计。

支持范围与使用条件见[功能说明](docs/acceptance.md)。

## 安装

准备 Docker 与 Docker Compose v2，在解压目录执行：

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml up -d --build --wait
```

获取安装时生成的管理员凭据：

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml exec platform trpc-init -show-token
```

访问 `http://127.0.0.1:18080/admin/ui/`，登录后依次创建工作空间、配置模型连接、创建 Agent、调试并发布。

此配置仅监听本机回环地址。服务器部署、HTTPS、备份和权限设置见[安装运行手册](docs/operations-runbook.md)。模型和 IM 账号由部署者提供；未配置模型服务时，内置演示模型只提供固定回复。

停止平台时使用：

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml stop
```

不要删除安装数据卷：其中保存管理员凭据、加密主密钥、数据库和会话数据。

## 文档

- [安装与运行](docs/operations-runbook.md)
- [功能范围](docs/acceptance.md)
- [IM 接入](docs/im-channels.md)
- [系统架构](docs/architecture.md)
- [消息时序](docs/sequence.md)
- [数据模型](docs/data-model.md)
- [数据同步与幂等](docs/data-consistency.md)
- [多后端适配](docs/backend-adapters.md)
- [治理、安全与监控](docs/governance-operations.md)
- [生产风险与缓解措施](docs/risks.md)
