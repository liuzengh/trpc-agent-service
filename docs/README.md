# 项目文档

## 权威文档

- [`project-status.md`](project-status.md)：基于当前 Git HEAD 和实际源码的能力盘点、运行链路、已实现/未实现边界和验证基线。
- [`ARCHITECTURE.md`](ARCHITECTURE.md)：目标拓扑、组件职责、租户隔离、领域模型、一致性、IM 链路、治理、观测、故障恢复和部署架构。
- [`implementation-plan.md`](implementation-plan.md)：从当前 HEAD 继续执行的总计划、任务依赖、文件边界、测试验收和首个生产版本门禁。
- [`p0-06-acceptance-matrix.md`](p0-06-acceptance-matrix.md)：P0-06 Claim、Lease、epoch/fencing、failover、熔断和限流验收记录。
- [`../migrations/README.md`](../migrations/README.md)：PostgreSQL 迁移器使用、checksum、锁、down 迁移和测试数据库约束。

## 阅读顺序

第一次了解项目：`README.md` -> `project-status.md` -> `ARCHITECTURE.md` -> `implementation-plan.md`。

准备开发任务：先看 `project-status.md` 确认当前代码边界，再看总计划中对应任务的前置条件、修改范围和验收命令，最后阅读目标包和已有契约测试。

## 历史材料

`implementation-plan-p0.md`、`implementation-plan-p01.md`、`plan003_004.md`、`plan005.md`、`plan006Revise`、`plan006v1` 和 `plan006v2.md` 保留用于追溯设计演进。它们不维护当前任务状态；若与权威文档冲突，以当前代码、`project-status.md`、`ARCHITECTURE.md` 和 `implementation-plan.md` 为准。

## 文档维护规则

- 文档中的“已完成”必须能由源码、测试、迁移或提交记录核对。
- 接口、迁移表和占位适配器不等于生产能力；必须区分契约、基础设施和可运行链路。
- 任务完成后同步更新现状、架构边界、验收命令和已知限制。
- 架构变更优先更新 `ARCHITECTURE.md`，实施顺序优先更新 `implementation-plan.md`；不要再新增无索引的临时计划文件。
