# 交付范围与验收

当前候选版本：**0.4.0-rc.9，数据库 schema 31**。交付物是基于 tRPC-Agent-Go 的多租户 Agent 平台源码、控制台、设计文档和部署文件。根目录 README 保留题目原文。

## 1. 对照题目

| 要求 | 实现 | 说明文档 |
| --- | --- | --- |
| 租户、应用、模型、工具、通道、后端和审计配置 | Control Plane、不可变 Revision、可信路由和租户 Scope | [架构](architecture.md)、[数据模型](data-model.md) |
| 多节点、共享会话、无 sticky session | 独立 Worker、Redis Streams、Session 租约与 fencing、持久化完成记录 | [数据一致性](data-consistency.md) |
| Session、Memory、Summary、Knowledge、Artifact、Audit | 租户后端路由、异步持久任务、更新水位 | [后端适配](backend-adapters.md) |
| 后端迁移和消息幂等 | 双写/回填/修复/验证/切读、Inbox/Outbox、工具 Journal | [数据一致性](data-consistency.md) |
| 至少两类 IM，包含微信或企业微信 | Telegram 与企业微信消息 MCP；另保留自建应用 Adapter | [IM 接入](im-channels.md) |
| 完整消息时序与追踪 | Channel → Gateway → Worker → Runner → Tool/存储 → Sender，贯穿 request_id/trace_id | [时序图](sequence.md) |
| 治理、安全和监控 | 工具白名单、审批、预算、Guardrail/Callbacks、OTel、审计、加密凭据 | [治理](governance-operations.md) |
| 故障、取消、灰度、回滚、容量和部署 | 等待恢复、租约接管、Event 排空、版本发布、Compose/Kubernetes 模板 | [运行手册](operations-runbook.md) |
| 至少 8 项风险 | 20 项风险与缓解措施 | [风险清单](risks.md) |
| 补充必做：管理页面、Skill、沙箱 | React 控制台、框架 Skill 加载、受限 Docker 执行 | [运行手册](operations-runbook.md) |

## 2. 当前能力与限制

| 模块 | 可以使用 | 明确不包含 |
| --- | --- | --- |
| Agent | tRPC-Agent-Go LLMAgent/Runner、真实兼容模型、多轮会话 | 尚未平台化注册 Graph/Chain/Parallel/Cycle |
| 控制台 | 创建工作空间和 Agent、模型连接与 Key 更新、草稿、调试、审批、发布、灰度、回滚、运行记录 | 企业 SSO、账号自助注册 |
| 机器人管理 | 网页添加、暂停、恢复、更新凭据、换绑 Agent、移除；企业微信逐群成员授权及撤权 | 自动接管旧服务器绑定；普通租户管理员自行管理机器人凭据（当前仅平台管理员） |
| 网页调试 | 独立身份和快照、持久任务、状态流、取消、刷新恢复、受控 Skill | IM 发送、任意外部写工具、模型逐字流式显示 |
| Session / Memory | InMemory、Redis、PostgreSQL | 框架所有后端的穷举适配 |
| Knowledge / Artifact | InMemory/Qdrant、InMemory/S3-compatible，独立 Embedding、租户授权 | 所有云供应商联调、完整 PDF/Office 解析、图片理解和杀毒服务 |
| Skill / 沙箱 | 固定 name/version/checksum、授权加载、审批、Journal、禁网/非 root/只读根/资源限额 | 未审核脚本上传、任意宿主机命令、VM 级隔离 |
| Telegram | 私聊、群、Topic、文本回复、受控附件导入 | 卡片、文件发送、回复编辑等完整媒体功能 |
| 企业微信消息 MCP | 群文本接收/回复，近期接收与历史补读分离 | 私聊、卡片、媒体发送；源接口无可靠消息 ID，因此不能承诺端到端 exactly-once |
| 企业微信自建应用 | 验签、AES、URL 验证与应用文本发送的 Adapter | 尚无真实自建应用账号验收，不能沿用 MCP 的结论 |
| 运维 | 角色拆分、指标、审计、告警规则、恢复工具和部署模板 | 生产容量承诺、真实告警接收方、完整云端容灾、Vault/KMS 与主密钥在线轮换 |

后端切换通过迁移 API 完成；知识库、对象存储和 Skill/沙箱需要部署者按手册启用。默认 Compose 提供最小平台，不等于所有可选资源已经配置完毕。

## 3. 验证层级

### 3.1 2026-09-11 修复与本地封版

本次修复存储层可信租户 Scope 与 Invocation 的交叉校验、Session/Memory 的过粗资源锁，以及 Summary 未注入后续模型请求的问题。普通访问按会话/用户协调，迁移保留应用级排他门禁；摘要生成不持有存储锁，提交校验快照和边界，双写失败可修复次端而不重复调用模型。后台、管理和附件操作显式携带授权 Scope。无需新增数据库迁移。

修复后的验证结果：全仓 `go test -race -count=1 ./...` 通过（51 个测试包、732 个通过条目，含子测试）；常规模式跳过 32 个外部依赖条目，另行运行的隔离恢复套件覆盖 PostgreSQL、Redis、Qdrant、S3、多租户/双 Worker 和迁移恢复。新增 PostgreSQL 摘要重开持久化检查通过。`./lint.sh` 为 0 issues，Go 构建和前端类型检查通过。

正式源码包仅从干净的本地 Git 提交导出。随包提供 SHA-256 和以同一提交标识命名的验包报告，记录实际解压包的干净构建、初始化、启动与受控对话结果；不能用工作区测试结果替代包本身的检查。本轮只做本地提交和归档，不 push，不替换正在运行的服务。生产环境与真实 IM 的剩余验收边界仍按下节执行。

源码包不含 `.git`，回归入口仍可直接运行；仅在当前目录确为 Git 工作区根目录时执行 `git diff --check`，保留源码工作区的检查，同时避免误检查解压目录的父仓库。

### 3.2 其他能力与外部验收边界

以下结论分开记录，不能相互替代：

- **既有真实链路**：Telegram 和企业微信消息 MCP 已在开发环境完成真实模型收发；附件、记忆、知识检索、只读文档 MCP、Skill/审批分别有对应联调记录。
- **网页创建的企业微信连接**：schema 30 的开发环境已有接收、Agent completed、Outbound sent 的完整记录。这证明新接入主流程可用，不代表所有多群/凭据变更场景已在真实企业微信验证。
- **本次连接管理实现**：独立 PostgreSQL schema 31、部署生成的 Admin 数据库权限和合成 IM 接口已核对逐群授权、撤权、凭据更新、换绑新会话、移除后重新添加、审计失败回滚，以及移除回执丢失后的只读对账。没有使用日常凭据或修改真实 Webhook。
- **未知副作用门禁**：即使请求 completed 或出站总状态 dead，存在 unknown/attempting 分段或 running/unknown 工具记录时仍拒绝维护连接；独立数据库已核对按证据对账后才允许换绑。
- **候选版本构建与恢复**：前端构建、现有全仓 Go race、静态检查通过；独立恢复套件（含多租户/双 Worker）、真实 Docker 沙箱与 Skill Runner 检查通过。浏览器用拦截的接口数据完成逐群撤权、暂停、凭据更新、换绑、确认移除及未知移除对账的操作检查。
- **首次安装**：不包含作者 `.env`、data、bin、node_modules 或构建页面的源码副本已完成 Docker 多阶段构建、空库初始化和 schema 31 迁移。浏览器完成生成凭据登录、创建工作空间、加密模型连接、Agent 配置、两轮 Runner 对话、发布、刷新恢复；容器重启后凭据与会话保留。模型使用仅隔离网络可达的合成兼容接口，不将其记为真实供应商联调。
- **公网服务器模板**：服务器 Compose 合并配置、Nginx 语法及隔离 HTTPS 反向代理已核对，覆盖回调路径/查询串/验签头保留、管理接口阻断和 ACME webroot。使用临时测试证书，没有替部署者申请真实证书或配置 DNS。独立数据库核对了网页地址与系统检查一致、改址后缓存失效、数据库失败不回退，以及内网目标和只读账号限制。
- **日常升级**：原 8080 环境已完成 30→31，升级前备份并校验 PostgreSQL、角色、Redis、私有配置及旧二进制；升级后 `.env` 字节及租户/模型/后端/通道配置指纹一致。新版控制台、就绪检查及公网健康入口通过，未重新注册真实 Webhook，也未主动发送 IM 测试消息。
- **最新版本待人工确认的外部项**：通过新版网页新建 Telegram 连接后的真实收发；新版多群权限与连接维护的真实 IM 操作。旧绑定能回复不等于这些新入口全部通过。
- **生产上线**：真实分角色部署、网络策略、告警通知、供应商额度、容量、数据保留和恢复目标需在接收方环境另行验收。

源码内保留已有验证入口，不新增独立测试脚本：

```bash
./scripts/regression.sh
# 需要 Docker 和相应镜像；只创建隔离资源，不使用日常数据库或模型。
TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh
```

默认入口执行前端构建、现有 Go race、静态检查和文档链接检查。隔离模式还覆盖双 Worker/双租户、恢复、迁移、数据库权限、Skill/Docker 和合成备份恢复。相关代码见 [recovery](../trpcservice/recovery)、[权限](../deploy/permissions)、[Skill](../trpcservice/agent/skill_runtime_test.go)、[沙箱](../trpcservice/workspace/docker_test.go)。浏览器人工/隔离操作不冒充完整自动 UI 覆盖率。

## 4. 接收方如何体验

部署到公网服务器按[运行手册第 0.5 节](operations-runbook.md#05-公网服务器部署)准备 Docker、域名、Nginx 和 HTTPS，再在网页完成：工作空间 → 模型连接 → Agent 草稿 → 调试 → 发布 → 机器人连接。本机开发体验见第 0.0 节。模型与 IM 账号由接收方提供，不依赖作者的 workbuddy2api、域名、数据库或测试群。

完整功能的依赖和授权见[运行手册第 0.4 节](operations-runbook.md#04-启用对象存储向量库skill-等功能)。管理页面和 Skill/沙箱已实现；默认关闭沙箱是部署权限边界，不是以宿主机执行替代容器隔离。

## 5. 源码与发布边界

正式文档共 11 份（含索引），见 [docs/README.md](README.md)。最终源码包必须由完成本地提交后的同一 Git HEAD 导出：

```bash
./build.sh --package
```

源码包包含源码、锁文件、Skill 示例、现有测试、配置模板及部署文件；不包含 `.env`、密钥、会话、原始 trace、日志、node_modules、二进制、数据库卷或备份。包附 SHA-256。旧版归档不代表当前候选版本；不得把未提交的新文件漏在交付包之外。

schema 31 保留退役绑定和历史数据，不进行破坏性 down migration。已使用逐群授权或退役连接后，不能直接换回不理解这些字段的旧二进制；恢复必须按[升级与回退说明](operations-runbook.md)进行。GitHub 更新由仓库所有者决定，本轮不自动 push。
