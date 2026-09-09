# 交付范围与验收

交付对象是基于 tRPC-Agent-Go 的多租户平台设计、可运行代码、部署模板和自动测试。根目录 README 保留原始题目；本说明集中给出实现映射与验证边界，不包含逐轮开发日志。

管理页面、可执行 Skill 和沙箱均为必做项，现已提供实际实现：页面接通 Admin API，Skill 通过框架加载并固定授权版本，执行在受限 Docker 容器内完成。验证包括真实浏览器操作、Runner/审批/Journal 和独立容器测试，不扩大为生产环境验收。

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
| 管理页面（必做） | React/TypeScript 工作台、持久登录、授权凭据选择、结构化配置与草稿、版本分页、真实发布/回滚历史、后台任务与运行关联、资源/通道/请求/诊断页面 | 无企业 SSO；后端切换仍走迁移 API，不提供任意数据库编辑；源码构建新增 Node/npm 依赖 |
| 网页调试 | 独立快照和用户身份、SQL 持久任务、原 Runner/权限/预算/审批、状态流、工具记录、取消、刷新恢复和有界保留 | 不发送 IM；外部写工具关闭、MCP 仅允许部署者声明的只读工具；输出为完整回复和状态流，不是模型逐字流；未知执行不自动重放 |
| 配置预检与平台错误反馈 | 创建/发布/灰度统一检查；字段级错误和建议；Skill 预算冲突拦截；运行预算反馈和审批/沙箱错误分类 | 静态预检不调用模型；远端依赖未观测或过期时返回 unknown；隔离调试通过不代表全部业务路径通过 |
| 可执行 Skill（必做） | 框架 SKILL.md 解析/加载、部署注册、租户 grant、name/version/checksum 固定、Agent 调用、强制审批及执行 Journal | 执行注册的 run.sh，输入为 JSON；不支持租户自助上传或未审核在线安装 |
| 沙箱（必做） | 固定本地镜像 ID、非 root、禁网、只读根、独立 tmpfs、资源/时长/输出限额、取消清理；真实 Docker 测试 | 是共享内核容器，不是 VM；Docker daemon 必须受控，无持久交互终端 |
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

部署模板和设计中的可选方案不算已验证实现；自动测试、真实模型联调、云环境及生产上线是不同层级。管理页面、Skill、沙箱按上述实际实现和测试验收；微信公众号/微信客服等额外通道、完整多媒体和节点内并发池不作为本阶段前置条件。

## 3. 可重复验证入口

```bash
./scripts/regression.sh
```

默认清除继承的集成测试地址，不加载日常 `.env`、不调用真实 IM/模型、不重启服务。执行全仓 race、go vet、可用的 golangci-lint、构建和文档链接检查。

独立后端与恢复验证要求 Docker 及已缓存的测试镜像：

```bash
TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh
```

覆盖隔离 SQL/Redis 权限、Admin SQL 列表、恢复套件、两租户/两 Worker 联合测试、Docker 沙箱与 Skill Runner、合成 SQL/Redis 备份工具链和 Prometheus 规则。测试自行创建并核对所属容器，不复用日常业务数据卷。

重点用例在：

- [联合多租户/多 Worker](../trpcservice/recovery/joint_integration_test.go)：同外部身份的作用域隔离、权限拒绝、处理中杀死实际 claim owner、存活 Worker 接管、处理中及完成后的重复投递。
- [恢复测试目录](../trpcservice/recovery)：故障、迁移、持久化和恢复用例；外部副作用不能仅凭数据库备份推断回滚。
- [脚本测试](../scripts)：真实临时 Agent 启停、隔离恢复脚本安全约束、构建产物归档及源码包边界。
- [Skill Runner 测试](../trpcservice/agent/skill_runtime_test.go)、[Docker 隔离测试](../trpcservice/workspace/docker_test.go)：审批前零执行、框架正文装载、受控脚本运行、幂等、禁网/非 root/只读根/超时/输出限制。
- 控制台构建执行 TypeScript 检查；原 UI 专用的浏览器脚本已随旧界面移除。新版页面做过隔离浏览器运行检查，不能把旧页面的自动化结果直接等同于新版页面覆盖率。
- 各业务模块的 `*_test.go`：审批、取消、预算、权限、媒体限制、trace 脱敏和数据隔离。

原交付基线的结论不自动覆盖新工作台。`0.3.0-rc.1` 已完成前端类型检查、全仓 Go race 和静态检查；隔离 PostgreSQL/浏览器检查覆盖授权引用隔离、重复草稿发布、回滚操作历史与分页、请求关联摘要/记忆任务及 Worker 状态上报。此前另以真实 Docker 验证网页调试审批、单次 Skill 执行和重复决定幂等。

升级核对包括：基线 `0ce285f` 在 schema 24 上启动、管理查询及 HTTP Agent 执行；日常数据库备份、23→24 增量升级，升级前后租户/应用/版本/通道/后端配置指纹一致；现有真实模型通过新版网页调试任务返回预期结果。源码包已核对 SHA-256、私有/生成文件排除，并在解压目录独立完成前端与 Go 构建；Docker 多阶段构建亦已通过（构建环境使用可达的 Go 模块镜像代理）。

本轮代码及部署交付准备已完成，P6 仅剩升级后的真实 IM 收发确认，需要用户在已有授权测试会话发送消息。确认前不将新版本的 Telegram/企业微信真实链路标记为已验收；以上结果也不代表生产容量或容灾验收。

## 4. 交付文件与上线边界

正式文档共 11 份（含索引），见 [docs/README.md](README.md)。源码包用 `./build.sh --package` 从 Git HEAD 导出并附 checksum，保留源码、测试、配置模板和部署文件，不包含私有配置、历史记录目录、日志、二进制或运行数据。

当前源码包含三个必做模块，可按运行手册配置后使用；默认不启用沙箱，也不会自动注册租户授权。生产上线前还需确认真实分角色账号、网络策略、告警通知、数据驻留/保留、供应商额度、容量和恢复目标。此说明不是生产上线验收报告。
