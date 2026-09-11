# 技术文档全景 (Technical Documentation Overview)

欢迎查阅 `trpc-agent-service` 平台的技术文档库。本文档库面向开源社区贡献者、企业系统架构师、平台开发者与 SRE 运维团队，系统性阐述平台的架构拓扑、领域数据模型、一致性与幂等保障、多存储适配及生产高可用运维指南。

---

## 核心技术规格与设计文档 (Technical Specifications)

| 核心文档 | 领域范畴 | 关键内容概要 |
| :--- | :--- | :--- |
| **[系统架构设计 (Architecture)](architecture.md)** | 全局架构与时序 | 平台总体定位、分层拓扑架构、**系统架构全景 Mermaid 图**、**企业微信端到端全链路时序 Mermaid 图**、核心接口契约与部署伸缩。 |
| **[数据模型设计 (Data Model)](data-model.md)** | 存储结构与协议规范 | 领域实体关系图 (ER Diagram)、核心数据库表结构 DDL 及不可变配置快照、执行 Trace、发件箱标准 JSON Schema。 |
| **[数据同步与幂等机制 (Data Sync & Idempotency)](data-sync-and-idempotency.md)** | 并发控制与一致性保障 | 深入源码解析多级消息去重（Redis Lua 原子脚本 + 数据库 CAS 认领）、会话排他租约与 Fencing Token、防重复执行、事务发件箱 (Outbox) 与在线存储迁移。 |
| **[多后端存储适配方案 (Multi-Backend Storage)](multi-backend-storage.md)** | 异构存储与数据拓扑 | 详述 SQL、NoSQL、向量库与对象存储的适用场景、选型对比、多存储适配器抽象及租户专属隔离机制。 |
| **[生产运维与风险防范指南 (Production Guide & Risk Register)](production-risk-register.md)** | 高可用运维与容灾处置 | 梳理 12 项涵盖并发脑裂、IM 重连/重投、大模型限流、跨租户越权、存储宕机与 goroutine 生命周期等生产风险。 |
| **[部署与容量评估](../deploy/README.md)** | 部署、伸缩与容量 | 最小 `SERVICE_ROLE=all` 方案、Kubernetes 分角色部署、Secret 注入、Worker/Kafka 并行度与端到端容量压测。 |

---

## 推荐阅读路径 (Reading Guide)

- **架构初探与技术选型**：建议首先通读 **[系统架构设计](architecture.md)**，理解接入、调度、执行、存储与分发的五层解耦模型；
- **核心数据与持久化开发**：查阅 **[数据模型设计](data-model.md)** 与 **[多后端存储适配方案](multi-backend-storage.md)**，掌握多租户数据隔离与异构存储引擎划分；
- **高并发与一致性实现**：深入研读 **[数据同步与幂等机制](data-sync-and-idempotency.md)**，理解分布式租约锁与事务发件箱的协同工作原理；
- **生产环境部署与运维**：先按 **[部署与容量评估](../deploy/README.md)** 建立容量基线，再结合 **[生产运维与风险防范指南](production-risk-register.md)** 做高可用配置与故障演练。
