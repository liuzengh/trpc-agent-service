# 交付范围与验收

交付对象是基于 tRPC-Agent-Go 的多租户平台设计、可运行代码、部署模板和自动测试。根目录 README 保留原始题目；本说明集中给出实现映射与验证边界，不包含逐轮开发日志。

## 1. 要求映射

| 题目要求 | 实现与文档 |
| --- | --- |
| 租户模型、配置、发布与路由 | `controlplane`、`routing`、`agent/compiler.go`；[架构](architecture.md)、[数据模型](data-model.md) |
| 多 Worker、无 sticky、Session 一致性 | `worker`、`coordination`、`workqueue`、`storage`；[同步协议](data-consistency.md) |
| Session/Memory/Summary/Artifact/Knowledge/Audit | 租户路由与持久任务；[后端方案](backend-adapters.md) |
| 至少两类 IM，包含微信/企业微信 | Telegram、企业微信自建应用 Adapter 与消息 MCP；[通道](im-channels.md) |
| 完整消息链路和 request_id/trace_id | Runner、Tool Journal、OTel、Reply Sender；[核心时序](sequence.md) |
| 工具权限、审批、预算、审计与密钥 | `governance`、`approval`、`toolexec`、`modelops`、`audit`、`secret`；[治理](governance-operations.md) |
| 故障、取消、恢复、灰度与部署 | Context/事件通道消费、claim/重试、Revision、Compose/Kubernetes；[运行手册](operations-runbook.md) |
| 至少 8 项风险和缓解措施 | [20 项风险](risks.md) |
| 代码与框架复用界限 | 本仓库源码；[架构中的复用说明](architecture.md) |

## 2. 实现和验证层级

| 能力 | 已实现与已验证 | 明确限制 |
| --- | --- | --- |
| Agent 执行 | LLMAgent、Runner、真实兼容模型、多轮会话 | 未平台化注册 Graph/Chain/Parallel/Cycle |
| Skill、沙箱与 Web UI | 本版不提供；`web` 仅承载 HTTP API，管理功能通过 Admin API 提供 | 不把目录占位或普通 Tool/MCP 当作 Skill 执行、沙箱或可视化页面 |
| 租户与多节点 | 两租户/两真实 Worker 进程，配置/Session/Memory/Knowledge/工具隔离、故障接管与去重的隔离测试 | 联合测试采用合成模型，不代表真实多供应商压测 |
| 数据后端 | Session: InMemory/Redis/PostgreSQL；Memory: InMemory/Redis/PostgreSQL；Knowledge: InMemory/Qdrant；Artifact: InMemory/S3-compatible | 不是框架所有后端均已适配；远端云后端未完整联调 |
| 数据迁移 | 双写、分批回填、服务器验证门禁、切读/回滚与修复任务 | 更换 Embedding 要重建；历史 Session 主体需要完整清单 |
| Telegram | 私聊/群/Topic、真实模型回复；工具、审批、文本附件、记忆、文档 MCP 和知识检索的开发环境联调 | 真实 429、多账号高负载、媒体发送/编辑未验收 |
| 企业微信消息 MCP | 已授权群文本接收 → Runner/模型 → 机器人回复，开发环境真实链路与 trace 已验证 | 指纹去重不是源消息 ID；分页、延迟、多群和媒体能力仍有限 |
| 企业微信自建应用回调 | URL 验证、签名、AES、Token 和应用文本发送的模拟协议测试 | 不能继承消息 MCP 的真实联调结论 |
| 工具与审批 | 单用户/会话绑定、白名单、参数校验；本地工作项审批前无写入、批准后一次写入、重复批准不重做 | 本地工作项不是外部企业工单系统 |
| Agent MCP | 受授权的 Streamable HTTP 工具；只读项目文档工具已真实联调 | 不支持任意 MCP 自助接入、stdio 或外部企业系统的通用接入保证 |
| 附件/记忆/知识库 | 文本附件存 MinIO、PostgreSQL 长期记忆、真实 Embedding+本地 Qdrant，含重启读取 | 文件保存不等于图片理解；无完整杀毒/PDF/Office；语义质量和灾备需另验 |
| 监控与安全 | OTLP、指标、审计、规则、预算预留结算、精确 Secret grant、分角色权限生成器 | 未接真实告警接收方、SSO/OIDC、Vault/KMS；权限模板须实际部署 |
| 故障与运维 | Worker 接管、取消、退避、SQL/Redis 恢复、手动生命周期与隔离测试 | 无生产 PITR/主从切换、完整对象/向量灾备或生产容量承诺 |

部署模板和设计中的可选方案不算已验证实现；自动测试、真实模型联调、云环境及生产上线是不同层级。微信公众号/微信客服等额外通道、UI、完整多媒体和节点内并发池不属于本阶段基本交付门槛。

## 3. 可重复验证入口

```bash
./scripts/regression.sh
```

默认清除继承的集成测试地址，不加载日常 `.env`、不调用真实 IM/模型、不重启服务。执行全仓 race、go vet、可用的 golangci-lint、构建和文档链接检查。

独立后端与恢复验证要求 Docker 及已缓存的测试镜像：

```bash
TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh
```

覆盖隔离 SQL/Redis 权限、恢复套件、两租户/两 Worker 联合测试、合成 SQL/Redis 备份工具链和 Prometheus 规则。测试自行创建并核对所属容器，不复用日常业务数据卷。

重点用例在：

- [联合多租户/多 Worker](../trpcservice/recovery/joint_integration_test.go)：同外部身份的作用域隔离、权限拒绝、处理中杀死实际 claim owner、存活 Worker 接管、处理中及完成后的重复投递。
- [恢复测试目录](../trpcservice/recovery)：故障、迁移、持久化和恢复用例；外部副作用不能仅凭数据库备份推断回滚。
- [脚本测试](../scripts)：真实临时 Agent 启停、隔离恢复脚本安全约束、构建产物归档及源码包边界。
- 各业务模块的 `*_test.go`：审批、取消、预算、权限、媒体限制、trace 脱敏和数据隔离。

交付基线已通过全仓与完整隔离回归；本地 Go 1.27.1 / golangci-lint 2.13.2 检查为 0 issues。独立源码包已验证解压、编译、Mock 两轮会话和优雅退出。真实 IM 的结论限于上表，不因文档整理或自动回归扩大。

## 4. 交付文件与上线边界

正式文档共 11 份（含索引），见 [docs/README.md](README.md)。源码包用 `./build.sh --package` 从 Git HEAD 导出并附 checksum，保留源码、测试、配置模板和部署文件，不包含私有配置、历史记录目录、日志、二进制或运行数据。

基本交付可以用于构建、演示和继续开发；生产上线前还需确认真实分角色账号、网络策略、告警通知、数据驻留/保留、供应商额度、容量和恢复目标。此说明不是生产上线验收报告。
