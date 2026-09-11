# 基于 tRPC-Agent-Go 的多租户 Agent 部署平台

本项目提供一个面向企业 IM 场景的多租户 Agent 平台参考实现。平台以
tRPC-Agent-Go 为执行框架，提供租户隔离、Agent 发布、企业微信与微信客服接入、
共享会话、知识库、受控工具、审计和可观测能力。

## 交付物

项目文档按交付物拆分，设计目标、实现边界和已提供的代码入口在
[交付物说明](docs/交付物说明.md) 中统一索引。

- [架构设计方案](docs/架构设计方案.md)
- [系统架构图](docs/系统架构图.md)
- [核心时序图](docs/核心时序图.md)
- [数据模型设计](docs/数据模型设计.md)
- [数据同步与幂等策略](docs/数据同步与幂等策略.md)
- [多后端适配方案](docs/多后端适配方案.md)
- [风险清单](docs/风险清单.md)
- [代码实现说明](docs/代码实现说明.md)
- [部署说明](deploy/部署说明.md)

## 快速开始

### 本地运行

```bash
cp config.example.yaml config.yaml
# 在 config.yaml 中配置模型信息，或设置 MODEL_API_KEY
./build.sh
./start.sh
./stop.sh
```

未提供 `config.yaml` 且未设置 `MODEL_API_KEY` 时，服务会退出并提示配置方式。

### Docker Compose

默认 Compose 使用项目内置的假模型，不需要真实 API Key。

```bash
docker compose up -d --build
curl localhost:8080/readyz
curl localhost:8080/healthz
docker compose down
```

更多环境变量、可靠模式角色部署和 Kubernetes 配置见 [部署说明](deploy/部署说明.md)。

## 验证

```bash
go test ./...
bash scripts/check_deps.sh
bash scripts/check_deploy.sh
```

需要 Docker 的集成验证：

```bash
bash scripts/e2e.sh --strict
bash scripts/fault_drill.sh --strict
bash scripts/reliable_e2e.sh
bash scripts/reliable_fault_drill.sh
```

## 目录

```txt
cmd/             服务与测试上游命令入口
trpcservice/     平台实现
migrations/      MySQL 数据模型迁移
docs/            交付物文档
deploy/          Compose、Kubernetes 与可观测配置
scripts/         构建、门禁、联调与故障演练脚本
```
