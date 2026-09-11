# tRPC Agent Service

基于 tRPC-Agent-Go 的多租户 Agent 管理与运行平台。通过网页工作台配置模型、工具、知识库和存储后端，调试并发布 Agent，接入 Telegram 或企业微信。支持单机运行，也可拆分为多个角色节点部署。

## 主要能力

- 多租户配置、会话、工具权限、密钥和审计隔离。
- Agent 草稿、在线调试、不可变版本发布、灰度和回滚。
- Gateway、Worker、消息投递与后台任务分角色部署，支持共享后端下的多节点运行。
- Session/Memory 支持 Redis、PostgreSQL 和 InMemory；Knowledge 支持 Qdrant；Artifact 支持 S3-compatible 存储。
- IM 消息去重、持久化任务、失败恢复、工具审批和投递状态查询。
- 租户预算、日志脱敏、OpenTelemetry、监控告警和审计。

支持范围与使用条件见[功能说明](docs/acceptance.md)。

## 快速开始

需要 Git、Docker 和 Docker Compose v2。首次构建需能访问容器镜像、Go 和 npm 依赖源；宿主机不需要额外安装 Go、Node.js 或数据库。

### 1. 获取代码

```bash
git clone https://github.com/liuzengh/trpc-agent-service.git
cd trpc-agent-service
```

以下命令均在仓库根目录执行。

### 2. 启动平台

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml up -d --build --wait
```

首次启动会生成管理员凭据和加密主密钥，启动 PostgreSQL、Redis，执行数据库迁移并启动平台。

### 3. 登录并创建 Agent

获取管理员凭据：

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml exec platform trpc-init -show-token
```

在浏览器访问 `http://127.0.0.1:18080/admin/ui/`，使用管理员凭据登录：

1. 创建工作空间，配置模型连接。
2. 创建 Agent，设置提示词、工具和知识库。
3. 在线调试，确认配置后发布。
4. 按需连接 Telegram 或企业微信，接入步骤见 [IM 接入说明](docs/im-channels.md)。

模型服务和 IM 账号由部署者提供。未配置模型服务时，内置演示模型只提供固定回复。

### 停止与再次启动

停止平台：

```bash
docker compose --env-file deploy/compose/demo.env.example -f compose.demo.yaml stop
```

再次启动时执行第 2 步的命令，已有数据与凭据会保留。不要执行 `down -v` 或删除安装数据卷：其中保存管理员凭据、加密主密钥、数据库和会话数据。

## 部署说明

上述 Compose 配置用于单机运行，仅监听本机回环地址。公网服务器部署需配置 HTTPS 并限制管理入口；多节点部署需使用共享控制面、队列、协调器及数据后端。InMemory 后端仅适用于单进程。

服务器部署、宿主机安装、多节点配置、备份与升级步骤见[安装与运行手册](docs/operations-runbook.md)。

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

[项目原始需求](docs/requirements.md)完整保留最初的任务说明、具体要求和验收标准。当前功能范围及安装方式以本 README 和上述文档为准。
