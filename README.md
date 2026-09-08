# tRPC-Agent-Service

基于 tRPC-Agent-Go 的多租户 Agent 平台：将 IM 消息接入、Agent 执行、共享状态、工具权限和审计组织为可独立部署的服务。

当前交付的是**设计文档、可运行实现及开发环境验证记录**，不是已完成生产上线验收的托管服务。

## 从哪里开始

| 你要做什么 | 入口 |
| --- | --- |
| 了解交付内容和明确限制 | [基本交付说明](docs/delivery.md) |
| 安装、配置、手动启停与排障 | [运行手册](docs/operations-runbook.md) |
| 看整体设计和消息链路 | [架构图](docs/architecture.md)、[核心时序](docs/sequence.md) |
| 逐步理解代码怎么运行 | [链路演进教程](docs/getting-started.md) |
| 对照题目验收 | [原始要求](docs/requirements.md)、[验收映射](docs/acceptance.md) |
| 查某项能力是否真的联调过 | [功能状态](docs/feature-status.md)、[验证记录索引](docs/validation/README.md) |
| 查全部文档或某项配置 | [文档导航](docs/README.md)、[功能配置参考](docs/runtime-reference.md) |

## 已有能力

- 多租户 Tenant / App / Revision / Binding 控制面，租户级模型、工具和存储路由。
- Gateway、Relay、Worker、Sender、Jobs、Admin 角色拆分；共享会话、租约、幂等和任务接管。
- Redis / PostgreSQL Session，Memory，Qdrant Knowledge，S3-compatible / MinIO Artifact，以及后端迁移。
- Telegram 和企业微信接入、工具权限与审批、预算、审计、OpenTelemetry、监控规则。
- 手动运维脚本、Docker Compose 依赖、容器镜像和 Kubernetes 部署模板。

Telegram 与企业微信**消息 MCP 群文本**已跑通开发环境真实模型回复；企业微信**自建应用回调**只有代码和模拟协议测试。不能把这两种接入方式的验证混用。

目前每个异步 Worker 进程一次处理一个任务，通过增加 Worker 进程横向扩展。完整多媒体、更多 Agent 编排、云端灾备、实际告警通知及生产容量不包含在基本交付通过结论内，详见[功能状态](docs/feature-status.md)。

## 本地启动

需要 Go（版本以 [go.mod](go.mod) 为准）；手动启停脚本需要 Linux / flock / pidfd。完整平台后端需要 Docker Compose；默认 Mock + InMemory 可以不启动 Docker。

在全新工作目录准备配置；**已有 .env 不要覆盖**：

```bash
test -f .env || cp .env.example .env
chmod 600 .env
./build.sh
./start.sh
curl -fsS http://127.0.0.1:8080/healthz
./status.sh
```

模板默认不调用真实模型，也不启用 HTTP 聊天、Admin 或外部 IM。健康检查成功只表示基础服务启动；体验对话前按[鉴权说明](docs/security-boundaries.md#1-http-调试接口)配置受限调用方。

已有真实模型 / IM 环境请按[运行手册](docs/operations-runbook.md)启动依赖、模型和 Tunnel，再运行 `./start-real.sh`。模型配置从私有 `.env` 读取，密钥不能放进交付包。启动脚本不会热更新已经运行的服务。

停止 Agent：

```bash
./stop.sh
```

不设置开机自启；模型转换服务、Tunnel 和数据库独立管理。

## 测试与交付

默认回归不加载日常 `.env`、不调用真实 IM / 模型、不重启日常实例：

```bash
./scripts/regression.sh
# 可选，要求 Docker 和已缓存测试镜像：
TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh
```

完整隔离回归包含两租户 / 两 Worker 联合测试、故障接管、数据恢复和告警规则测试。真实外部联调与自动测试分开记录，不把被跳过的集成测试算作通过。

工作区提交干净后，可以导出只含已提交文件的源码包：

```bash
./package-source.sh
```

交付包不含 `.git`、私有配置、运行数据或二进制；不会 push。工作区归档与恢复规则见[环境整理说明](docs/workspace-hygiene.md)。`./clean.sh` 默认只预览可整理的构建产物，不清数据库或 Docker 数据卷。
