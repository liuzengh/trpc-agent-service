# 基本交付说明

## 交付结论与范围

本次交付覆盖[原始 README 题目](requirements.md)要求的架构方案、数据模型、多后端与同步设计、IM 接入、治理监控、故障处理、风险清单和可运行代码。接收方可从本仓库构建运行，并查阅对应验证证据。

这是一份**有明确验证边界的基本实现交付**，不是生产上线验收报告。当前实现通过多 Worker 进程水平扩展，每进程一次处理一个异步任务；节点内并发池不是本阶段的前置条件。

## 源码与配置

功能和验证基线为本地提交 `eaa892d`，此前实现检查点为 `c5104ca`。本次文档、工作区整理及打包脚本由后续本地提交记录；最终以源码包文件名中的 Git commit 标识交付版本，不把历史 rc.1 镜像当作当前镜像。

- 源码：`cmd`、`trpcservice`、`go.mod` / `go.sum`。
- 部署与测试：`deploy`、`compose.yaml`、`Dockerfile`、`scripts` 和根目录脚本。
- 配置模板：[.env.example](../.env.example)，仅含模板和本地示例值，生产凭据由接收方提供。
- 不交付：私有 `.env`、IM/模型凭据、聊天数据、数据库及对象/向量数据卷、原始 trace、日志、PID、历史二进制和本机备份。

在干净、已提交的工作区执行 `./package-source.sh`，得到 `dist` 下的源码压缩包及 SHA-256 文件。脚本只导出已提交文件，拒绝未提交改动和显然不应跟踪的私有路径，不执行 push。它不是任意内容的秘密扫描器，提交前仍需审阅变更；不得把整个工作目录直接压缩发给接收方。

## 文档交付物

| 交付物 | 文件 |
| --- | --- |
| 架构文档与系统图 | [architecture.md](architecture.md) |
| 核心时序图 | [sequence.md](sequence.md) |
| 核心实体、表结构 | [data-model.md](data-model.md) |
| 同步与幂等策略 | [data-consistency.md](data-consistency.md) |
| 多后端适配 | [backend-adapters.md](backend-adapters.md) |
| IM 差异与绑定 | [im-channels.md](im-channels.md) |
| 治理、安全与运维 | [governance-operations.md](governance-operations.md) |
| 风险清单（20 项） | [risks.md](risks.md) |
| 安装使用、验收映射 | [operations-runbook.md](operations-runbook.md)、[acceptance.md](acceptance.md) |

## 已验证与未验证

- 两租户 / 两 Worker：合成模型环境下的 Session、Memory、Knowledge 隔离、权限、处理中故障接管和幂等联合验证已通过。
- 真实 IM：Telegram 和企业微信消息 MCP 群文本已跑通开发环境真实模型执行与回复。企业微信自建应用回调不是同一条入口，仍只有模拟协议验证。
- 真实数据/工具链路：文本附件与 MinIO、PostgreSQL 长期记忆、只读文档 MCP、外部文本 Embedding + 本地 Qdrant、本地工作项审批均有对应记录。
- 自动回归：全仓 race、静态检查、构建、文档链接，及隔离权限/恢复、备份工具链和告警规则检查已通过。详细记录见[验证索引](validation/README.md)。

生产集群、实际告警通知、远端云后端、真实大规模性能/语义质量，以及完整多媒体、其他 Agent 编排不在上述结论内。具体能力逐项以[功能状态](feature-status.md)为准。

## 接手顺序

1. 阅读本说明和功能状态，明确只测试或启用哪些通道与后端。
2. 按[运行手册](operations-runbook.md)在自己的环境配置密钥。已有实例不重新 bootstrap 或覆盖配置。
3. 先执行不访问外部模型/IM 的 `./scripts/regression.sh`；需要隔离后端验证时按验收文档启用 Docker 测试。
4. 只有接收方准备好账号并明确授权后，才配置真实 IM 和模型。历史回执不能代替新环境验证，也不要求交付时重复发送所有历史测试消息。

本次整理后的本机备份去向和恢复方法见[环境整理说明](workspace-hygiene.md)。它们是部署者私有材料，不随源码包分发。
