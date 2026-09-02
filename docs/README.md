# 文档目录

> 阶段 11（E2E 与文档）已对齐：本目录为设计蓝图文档，最终代码组织以根 `README.md`「代码目录」段与 `AGENTS.md` §6 进度表为准。

设计文档：

- [平台架构设计草案](平台架构设计草案.md)：阶段 0 总体架构与 Mermaid 图（已确认）
- [详细设计文档](详细设计文档.md)：阶段 1 数据模型 / 接口 / 部署 / 测试（与 `trpcservice/` 实现对照见文档首部「实现对照」表）
- [多后端适配方案](多后端适配方案.md)：Redis / SQL / 向量库 / 对象存储各自定位与租户级路由（已确认）
- [技术选型与风险清单](技术选型与风险清单.md)：阶段 0 选型与风险（已确认，含 E2E/Milvus/MinIO 等已落定选型）
- [WX.md 可复用性评估报告](WX.md可复用性评估报告.md)：微信/企业微信 SDK 复用评估

部署与建表资产（不在 docs/，但在 `deployments/`）：

- `deployments/docker-compose.yml` + `.env.example`：本地完整栈
- `deployments/k8s/00~09.yaml`：生产推荐 K8s 清单
- `deployments/mysql/init/001~009.sql`：9 个建表迁移
- `deployments/README.md`：部署说明（compose/k8s/观测栈）
