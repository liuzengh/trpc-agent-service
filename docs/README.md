# 文档目录

设计意图在 [`方案文档.md`](方案文档.md)，落地证据在四份切片 spec。其中**三份**
（storage-redis / governance-observability / deployment-fault-drill）用同一个四段结构：
**事实核查 → 设计 → 测试策略 → 实测/验收**，实测段里的数字都是真跑出来的（含变异证伪）；
`spec-im-channels.md` 是更早的接入 spec，结构不同（结论与范围 → 接口契约 → 实现要点 →
验证矩阵 → 验收映射）。**没验过的地方在每份文档里都如实标注**（部署侧那份集中清单在
[`../deploy/README.md`](../deploy/README.md) §「验证：什么验过，什么没验过」），不拿“应该可以”冒充“已经可以”。

## 交付物对照（题目建议的四项各在哪）

| 建议项 | 位置 |
| --- | --- |
| 系统架构图（Gateway / Worker / Channel Adapter / Storage Adapter / Guardrail / Telemetry） | 方案文档 §2 总体架构 |
| 核心时序图（IM 消息 → Runner 执行 → Session/Memory 写入 → IM 回复） | 方案文档 §4（验收要求项） |
| 数据模型与多后端适配 | 方案文档 §5 概念设计 + [`spec-storage-redis.md`](spec-storage-redis.md)（Redis Session 落地）；**多后端 ≥3 类的现状与缺口**记在 [`spec-deployment-fault-drill.md`](spec-deployment-fault-drill.md) §5「与方案文档的差异」 |
| 风险清单（≥8 项） | 方案文档 §8（**10 项**）；演练中**新识别**的 2 项（无上限调用风暴、客户端重试放大 3 倍）记在 spec-deployment-fault-drill §1 事实 #16/#17 |

## 切片 spec（按时间）

| 文档 | 时间 | 范围 |
| --- | --- | --- |
| [`spec-storage-redis.md`](spec-storage-redis.md) | 9/4–9/5 | 数据层：把会话存储从进程内 `session/inmemory` 换成 Redis 共享后端（第三方依赖**第 3 批**引入点） |
| [`spec-im-channels.md`](spec-im-channels.md) | 8/21 导师答复落地 | IM 通道接入与本地验证：企微 / 微信客服两类差异、统一适配抽象、网页版 IM 做本地验证 |
| [`spec-governance-observability.md`](spec-governance-observability.md) | 9/7–9/9 | 治理与观测：输入/输出 Guardrail、带 `trace_id` 的审计行、租户维度指标（**第 4 批**，冻结前最后一批） |
| [`spec-deployment-fault-drill.md`](spec-deployment-fault-drill.md) | 9/9–9/10 | 部署与韧性：go.mod 冻结、Dockerfile/Compose/K8s 清单、端到端联调（44 条）、故障演练 D1–D7（143 条） |
| [`spec-reliable-loop.md`](spec-reliable-loop.md) | 第二批 | 可靠消息闭环：MySQL 事实源、Inbox/租约/fencing/原子提交、分角色进程（worker/delivery/jobs）、KF durable 拉取，`scripts/reliable_e2e.sh` 74 条断言；故障矩阵 `scripts/reliable_fault_drill.sh` 29 条（R1 kill 接管 / R2 MySQL 停机 / R3 Qdrant 停机自愈） |
| [`spec-tool-governance.md`](spec-tool-governance.md) | 第二批 | 受控工具与执行账本（P3）：`tool_calls` 台账、governor 检查链、SSRF/secret/schema 拒绝、unknown 阻断与人工处置（CLI + admin/v2），E2E 扩到 63 条断言 |
| [`spec-platform-completion.md`](spec-platform-completion.md) | 第三批（待实施） | 平台补齐：可靠链路最后一公里（gateway 接收端与多通道投递、worker 工具接线）、Skill/Workspace 两空壳、租户级后端选择、迁移 CLI、灰度发布、审计轮转、验证补强（G1–G10 / S1–S6） |
| [`deps-baseline.txt`](deps-baseline.txt) | 9/9 起冻结，第二批持续更新 | go.mod 冻结基线（第二批后 17 项直接依赖），由 `scripts/check_deps.sh` 当门禁读 |

## 部署与运维

[`../deploy/README.md`](../deploy/README.md)：构建、Compose、指向真实模型、可观测变体、Kubernetes，
末尾带一份「验过什么 / 没验过什么」的清单。想直接上手看仓库根的 [`../README.md`](../README.md) 「快速开始」。

