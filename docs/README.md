# 交付物文档索引

> 多租户 Agent 平台 — 架构设计交付物

---

## 交付物清单

1. [架构设计文档](架构设计文档.md) — 2000-4000 字，覆盖多租户、节点化部署、数据同步、IM 接入、治理监控、故障恢复
2. [系统架构图](系统架构图.mmd) — Gateway、Worker、Channel Adapter、Storage Adapter、Plugin/Guardrail、Telemetry、数据库和 IM 平台之间的关系（Mermaid 格式，可用 draw.io 打开）
3. [核心时序图](核心时序图.mmd) — 企业微信用户发消息 → Agent 执行 → Tool 调用 → Session/Memory 写入 → IM 回复的完整链路（Mermaid 格式）
4. [数据模型设计](数据模型设计.md) — 核心表结构、表关系图、Redis 键设计、JSON Schema 示例
5. [数据同步与幂等策略](数据同步与幂等策略.md) — 并发一致性、更新顺序、跨节点可见性、迁移方案、IM 幂等、后端一致性取舍
6. [多后端适配方案](多后端适配方案.md) — 五域路由架构、各后端适配说明、后端选择矩阵、Router 实现细节、新增后端接入指南
7. [风险清单](风险清单.md) — 10 个生产风险及对应缓解措施

---

## 文档说明

- 所有文档基于项目事实代码，非理论设计
- 架构图和时序图使用 Mermaid 语法，可用 `mmdc` CLI、draw.io 桌面版或在线 Mermaid 编辑器打开
- 旧文档（详细设计、技术选型、存储设计等）已归档至 `tempdocs/` 目录，仅供参考
- 部署资产（Docker Compose、MySQL DDL）位于 `deployments/` 目录
