# 最终交付物检查入口

本目录按 README 的八项交付要求整理。导师可按下表从上到下逐项检查，每项交付物均为独立可读文档。

## 正式交付物

| # | 交付物 | 检查入口 | 核心内容 |
| --- | --- | --- | --- |
| 1 | 架构设计文档 | [architecture.md](architecture.md) | 多租户、节点化部署、组件职责、隔离治理、故障恢复和实现边界 |
| 2 | 系统架构图 | [system-architecture-diagram.md](system-architecture-diagram.md) | Gateway、Worker、Channel/Storage Adapter、Guardrail、Telemetry、数据库和 IM 平台 |
| 3 | 核心时序图 | [core-sequence-diagram.md](core-sequence-diagram.md) | 企业微信消息、Agent/Tool、Session/Memory 和 IM 回复完整链路 |
| 3A | IM Channel Adapter | [im-channel-adapter.md](im-channel-adapter.md) | 企业微信/Telegram 差异、账号路由、Session 规则、认证与平台限制 |
| 4 | 数据模型设计 | [data-model.md](data-model.md) | 核心实体、主键约束、ER 图、JSON Schema 和状态模型 |
| 5 | 数据同步与幂等策略 | [data-sync-idempotency.md](data-sync-idempotency.md) | 事件顺序、消息去重、Lease/fencing、迁移、checkpoint 和恢复 |
| 6 | 多后端适配方案 | [backend-adapters.md](backend-adapters.md) | PostgreSQL/SQLite、Redis、向量库、对象存储和 InMemory 职责 |
| 7 | 生产风险清单 | [production-risks.md](production-risks.md) | 12 项风险的影响、信号、缓解措施和残余风险 |
| 8 | GitHub 实现代码详解 | [implementation-details.md](implementation-details.md) | 仓库地址、模块映射、关键实现、完成度和复现命令 |

推荐先读架构设计和两张图，再核对数据模型、同步与后端方案，最后用风险清单和实现详解检查代码证据。

## 代码与验收

GitHub 仓库：[GodBlf/trpc-agent-service](https://github.com/GodBlf/trpc-agent-service)

本地最终验收：

```bash
./scripts/stage7-acceptance.sh
```

该命令覆盖 Go、前端、关键纵向场景、文档门禁和双 Gateway Compose。只检查文档结构可运行：

```bash
./scripts/verify-docs.sh
```

验收时使用 `git rev-parse HEAD` 记录本次实现的 commit SHA。真实凭据不进入仓库或验收证据。

## 辅助材料

- [`acceptance/`](acceptance/)：Stage 7 验收说明和已通过项清单。
- [`stages/`](stages/)：Stage 2、4、5、6 的阶段实现记录，仅用于追溯。
- [`research/`](research/)：外部分支调研材料，不作为当前实现声明。
- [`adr/`](adr/)：已经接受的架构决策记录。
- [`agents/`](agents/)：Agent 协作和本地 issue/domain 工作说明。

正式验收结论以根目录八份交付文档和当前 commit 的自动化结果为准；阶段记录与调研资料不能替代最终交付物。
