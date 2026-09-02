# AGENTS.md — 项目约束与执行进度

> 本文件是项目的「自进化约束文档」，记录约束、架构决策、执行进度、已确认事项。
> **不得超过 300 行**，每次迭代后更新。

## 1. 项目定位

基于 **tRPC-Agent-Go** 的企业级多租户节点化 Agent 部署平台，含 Vue3 可视化前端。
多租户隔离、弹性部署、数据一致性、多 IM 触达（企业微信/飞书）、审计合规、后端可替换。

## 2. 核心约束（无条件遵守）

1. **复用优先**：复用 tRPC-Agent-Go 的 Agent 编排（LLMAgent/GraphAgent/Chain/Parallel/Cycle）、Tool/MCP、Session、Memory、Knowledge、Artifact、Plugin/Guardrail、HTTP 服务化、IM 通道。工具用 `FunctionTool` 封装，禁止重复造轮子；复用方式为 `import` 官方包，严禁照抄源码重构。
2. **懒加载查阅**：tRPC-Agent-Go 源码/文档与参考 PR 只在开发到对应模块时查阅，避免上下文过载。
3. **多租户隔离**：会话、记忆、知识库、工具权限、审计日志必须隔离。
4. **多后端**：必须支持 MySQL、Redis；其他后端（向量库/消息队列/对象存储等）须先与用户讨论同意。
5. **IM**：企业微信 `wss://openws.work.weixin.qq.com`；飞书 v3.7.2 `ws.Client`+`EventDispatcher` 订阅 `im.message.receive_v1`；接入统一消息总线。
6. **生产部署**：Docker Compose 全栈编排（`deployments/docker-compose.yml`，MySQL/Redis/Milvus/MinIO/后端/前端/观测）。K8s 清单已移除（阶段 25 grill 决策：单机/Compose 已覆盖；未来上 K8s 集群时再按需补）。
7. **可观测性**：Jaeger 分布式追踪 + OpenTelemetry + Prometheus 指标。
8. **TDD**：先写测试（单元/集成/端到端）再实现，测试可重复执行。
9. **注释英文，文档中文**。
10. **存疑先沟通**：需求不明、选型冲突、设计权衡、外部依赖问题，必须停下来向用户提问（grill），禁止自行假设。
11. **迭代收束**：分阶段，每阶段开始简述计划，结束提交成果与测试结果，等确认再进入下一阶段。

## 3. 工程实践与技能

- 启动阶段：**superpowers**（从零构建基础能力与核心流程）。
- 后续开发：**matt pocock**（类型安全、工程化、可维护性）。
- 需求不清：**grill-with-docs**（主动提问 + 输出文档澄清）。
- 目录架构：**ponytail**（简洁、分工明确、可扩展）。
- **每个编码阶段完成后用 ponytail 审查目录结构**，及时调整，避免冗余/过度设计。

## 4. 目录约定

- 正式交付物 → `docs/`；中间文档 → `tempdocs/`。
- 后端 Go 根目录；前端 Vue3 → `front/`；部署 → `deployments/`（Docker Compose）。
- **后端代码在原有 `trpcservice/` 包基础上开发**（不新建 `internal/`），设计结构按以下映射迁移进原包：
  - `config`/`log`/`tenant`/`agent`/`channels`/`tool`/`skill`/`metrics`/`web`/`workspace` 沿用原包名承载对应职责。
  - 新增子包（按需）：`health`（健康检查）、`bus`（消息总线）、`storage`（后端抽象）、`llm`（ModelEndpoint）、`audit`（审计）、`secret`（密钥）。
- 每个编码阶段完成后用 ponytail 审查目录简洁性与职责划分。

## 5. 关键架构决策（阶段 0 草案 + 调研结论）

- 单一二进制 + 角色标志（gateway/worker/admin/all）。
- 无状态 Worker + 共享 Session/Memory 后端（Redis/MySQL），无需 sticky session。
- 统一消息总线：Redis Streams + Consumer Group（XAUTOCLAIM 认领 + 消息 ID 幂等）。
- 租户隔离：`tenant_id` 贯穿 context + 表列过滤 + Redis key 前缀 + 对象存储路径。
- 工具权限：租户级白名单 + RBAC，经 `FunctionTool` + Plugin/Guardrail 治理。
- 参考 PR 借鉴（不照搬）：Inbox/Outbox、WSS 客户端 IM、不可变 RuntimeProfile/AgentVersion 发布回滚、租户级存储隔离、端到端 OTel。

### tRPC-Agent-Go 复用边界（阶段 0 调研结论）

| 能力 | 框架是否已有 | 复用/新增 |
| --- | --- | --- |
| Agent 编排 | 有（`agent/llmagent`、`agent/graph` 等） | 复用 |
| 执行入口 | 有（`runner.Runner`） | 复用 |
| Session/Memory/Knowledge | 有（`session/*`、`memory/*`、`knowledge/*`） | 复用 |
| Tool 封装 | 有（`tool/function.FunctionTool`、`tool/mcp`、`tool/codeexec`、`tool/workspaceexec`） | 复用 |
| 沙箱/代码执行 | 有（`codeexecutor`：local/container[Docker]/sandbox[seccomp]/e2b/jupyter） | 复用接口；自实现 `DockerExecutor`（K8s Pod 后端已决策不实现，阶段 17） |
| 审批（Approval） | 有（`plugin/guardrail/approval`） | 复用 |
| Guardrail | 有（`plugin/guardrail`：promptinjection/unsafeintent） | 复用 |
| Artifact | 有（`codeexecutor/artifact`、`artifact/*`） | 复用 |
| 对象存储 | 有（`storage/s3`，S3 兼容 → MinIO） | 复用 |
| 向量库 | 有（`knowledge/vectorstore/milvus` 等；`storage/milvus` 是通用 client 非 VectorStore） | 复用（选 Milvus） |
| 模型 | 有（`model/openai` 等，`WithBaseURL`+`WithAPIKey`，代码期构造） | 复用；**缺 ModelEndpoint 归一化层需补** |
| Skill | 有（`skill`：FS 仓库 + app/user scope + 注入） | 复用 SKILL.md 格式+注入；**缺四级模型/版本/三态/GLOBAL-TENANT scope 需补** |
| HTTP 服务化 | 有（`server/openai`、`server/agui`、`server/a2a`、`server/trpcagent`） | 复用；**缺通用 REST CRUD → Admin API 自建 RESTful** |
| IM 通道 | 有（`openclaw` channel 模型） | 借鉴，独立实现 IMAdapter |

## 6. 执行进度

| 阶段 | 状态 | 说明 |
| --- | --- | --- |
| 0 准备与审阅 | **完成** | 已读 readme/WX.md/tRPC-Agent-Go 源码/参考 PR；产出 3 份草案 + 13 项问题已全部确认 |
| 1 详细设计 | **完成** | 《详细设计文档》《架构设计文档》《多后端适配方案》+ 2 张图，7 项验收标准达标 |
| 2 骨架与 TDD | **完成** | Go 模块+依赖就绪；config/log/tenant/health 测试通过；前端脚手架+Vitest 测试通过 |
| 3 租户管理 | **完成** | 租户上下文传递 + 租户 CRUD API + 租户隔离 Session/Memory 存储（inmemory/mysql/redis 三后端，testcontainers 集成测试通过）+ 前端租户管理页 + 联调 |
| 4 Agent 编排与工具 | **完成** | ModelEndpoint(llm 含 OpenAI/Anthropic/Gemini 三协议) + 工具RBAC(tool) + Agent运行时管理(agent 版本发布/回滚) + Agent/Endpoint/Tool CRUD API + 前端 Agent/Endpoint 配置页；存储架构收敛（领域包内嵌内存态，storage 回归 session/memory） |
| 5 IM 适配器 | **完成** | channels 统一契约(Adapter/InboundMessage/OutboundMessage) + session_id 规则 + 企业微信适配器(验签SHA1/AES解密/归一化) + 飞书适配器(验签SHA256/@提及过滤/归一化) + Conn 接口隔离 + mock 测试全绿；真实 SDK 接线待用户本地手测 |
| 6 会话路由与同步 | **完成** | `bus` 包：Redis Streams 总线(PublishInbound/PublishOutbound/ConsumeInbound + XAUTOCLAIM 故障认领) + `route:{tenant}:{session}` 会话路由 + `idem:{msg_key}` SETNX 幂等 + `lock:session:` 会话锁(Lua 比较删除)；单测 + testcontainers Redis 集成全绿。Worker 接线 runner.Run 与 MySQL Outbox 随阶段 7 |
| 7 知识库与多后端 | **完成** | 7A：四域 MySQL 持久化（store 接口+内存/MySQL 双实现）+ `tenants.data_backend` 多后端路由（storage.Router）+ OpenMySQL。7B：MySQL Outbox（单事务幂等标记+事件，dispatcher 排空→stream:outbound）+ worker 包（幂等→会话锁→runner.Run→Outbox，Redis 快检+MySQL 标记双保险）+ main 角色装配。7C：knowledge 包（元数据双实现 + 框架 RAG 管线摄入[URL/内联文本] + 检索 + SearchTool；embedder 复用 ModelEndpoint；collection 清洗为 Milvus 合法名；Milvus 工厂幂等建 collection，空地址回落 inmemory）+ RuntimeProfile.kb_ids 挂载（worker 注入 knowledge_search 工具）+ /kbs Admin API + 集成测试（元数据重启恢复、worker 全链路挂 KB、Milvus 容器往返）。**坑已记**：BuiltinKnowledge.Close 连关 VectorStore（勿 Close 实例）；vectorstore/milvus 依赖 Milvus **2.5+**（BM25 全文检索），K8s 清单与测试镜像已升 v2.5.6；etcd v3.6.14 修 otelgrpc 冲突；搜索为 Bounded 一致性（写入后短暂不可见属正常） |
| 8 监控审计可观测 | **完成** | metrics 包复用框架 OTel provider（inbound/outbound/agent_run/error/token/IM delivery/tenant_cost/session_latency，noop 兜底 init 防 nil panic）+ worker 埋点（trace span `agent.run` + 指标 + audit 记录 + token 提取自 event.Usage）+ audit 包（Entry 全维度 + MySQLRecorder 异步批量[channel buffer/批量 flush/优雅关停/溢出计数]，DDL 009 audit_logs + usage_records + artifacts）+ config.Telemetry + main.go setupTelemetry 接线（无 endpoint 时 noop，有则 ftrace.Start + InitMeterProvider + 重绑 metrics）；单测 17 包全绿 + audit/worker 集成测试（testcontainers MySQL/Redis）通过 |
| 9 前端完善 | **完成** | 补齐全部 Admin API 前端：知识库管理页（KB CRUD+文档摄入 URL/内联+文档列表+检索演示，api/kb.ts+stores/kb.ts+spec）、Agent 发布对话框挂载知识库（RuntimeProfile.kb_ids 多选+profile 预填）、工具只读目录页、审计日志页（后端新增 GET /audit：audit.Log DTO + MySQLRecorder.List 倒序/租户过滤/限长，web.AuditAPI，main 接线仅在 MySQL 启用时注册）；路由/侧边导航补齐 6 页；vitest 2 文件 7 用例全绿 + vite build EXIT=0；后端单测全绿 + audit/web 集成测试（testcontainers）通过 |
| 10 K8s 部署 | **完成** | 部署资产与实际能力对齐：**configs/otel-collector.yaml 由空目录改为真实配置**（otlp grpc/http receiver + batch + health_check :13133 + debug/jaeger/prometheus exporter）；compose：milvus v2.4.15→**v2.5.6**（BM25 前置）、backend 挂载 `backend-compose.config.yaml`（MySQL/Redis/Milvus/OTLP 全接，替代镜像默认 dev 内存配置）、新增 otel-collector/jaeger/prometheus 服务 + prometheus.yml；K8s：01-configmap 改承载观测非敏感配置（otel-collector.yaml+prometheus.yml，observability-config），02-secret 增 `config.yaml` 键（含 DSN 的全量运行配置，因为平台只读单 YAML 且 DSN 含密码→ConfigMap 不放），07-backend 改从 Secret 挂载 /etc/trpc-service/config.yaml，新增 09-observability.yaml（otel-collector/jaeger/prometheus）；README 部署节全面同步（删除「dev InMemory」过时声明）。**实测**：compose config 语法 OK、K8s 全部 YAML pyyaml 解析 OK、config.Load 冒烟解析 backend-compose.config.yaml 字段全对齐、otel collector `validate` EXIT=0、临时 MySQL 容器执行 9 个 init SQL 建出 **27 张表**（含 audit_logs/usage_records/artifacts）；本机 3306 被本地 MySQL 占用故 compose 起 mysql 需改映射（环境问题非清单问题） |
| 11 E2E 与文档 | **完成** | **E2E**：Playwright v1.47 + chromium（`front/e2e/platform.spec.ts` 6 用例：新建/删除租户 → 新建端点 → 新建 KB 挂端点 → 发布 Agent 挂端点+KB → 列表显示 published v1），webServer 编排内存后端 + vite dev；**文档**：根 README 仅更新「代码目录」段为实际 trpcservice/ + front/ + deployments/ 布局并标注 skill/workspace 未接线；详细设计文档加「实现对照」表（19 章逐条标 ✅/⚠️/❌）；技术选型与风险清单更新版本/已选选型（Milvus v2.5.6 / MinIO / Element Plus / Playwright / Go 1.26）；平台架构设计草案、多后端适配方案、docs/README、WX.md 头部状态从「待确认/草案」改为「已确认」并附回写说明；**验证**：E2E 6/6 EXIT=0（含后端+前端冷启动） |
| 12 Skill 运行时闭环 | **完成** | Skill 资产全链路落地（清除阶段 11 遗留「Skill 运行时」项）。运行时注入侧（本阶段前已具备）：`worker.skills` + `skillInstruction()` 按 `RuntimeProfile.SkillIDs` 经 `LoadByIDs` 取**已发布版**（发布 Agent 引用最新，改 skill 发版即全局生效，用户已确认该语义）拼接 SKILL.md → `BuildFromProfile(extraInstruction)`；`agent` profile.skill_ids 走 `runtime_profile` JSON 全量持久化（内存/MySQL 双实现透传）；`web.SkillAPI`（CRUD+版本+发布）+ main 装配。**本阶段补齐**：① `web/skill_handler_test.go`（生命周期 CRUD/版本发布/错误路径/global 与租户隔离 2 用例，内存范式）；② 前端 `api/skill.ts` + `stores/skill.ts` + `skill.spec.ts`（5 用例）+ `SkillListView.vue`（新建/编辑/版本管理/发布，tenant scope 需属主租户）；③ `/skills` 路由 + 侧边导航；④ `AgentListView.vue` 发布对话框挂 `skill_ids` 多选（仅列已发布 skill，profile 回填）；⑤ `api/agent.ts` `RuntimeProfile.skill_ids`。README skill 接线状态同步。**验证**：go build EXIT=0；go test ./trpcservice/... 18 包 EXIT=0（web 含新 handler 测试）；vitest 3 文件 12 用例 EXIT=0；vite build EXIT=0。**环境坑再记**：构建缓存须用 `.mc8`/`.gc14`（730M 完整）；空 `.gomodcache` 会触发全量下载且 goproxy.cn 间歇挂死（30min 无进展需 Stop 重试命中缓存） |
| 13 审批流接线 | **完成** | 人工审批全链路（清除阶段 11 遗留「审批流」项）。复用框架 `plugin/guardrail/approval`（BeforeTool 同步评审），grill 拍板：①**同步挂起**模型（reviewer 阻塞等人工决策，批准后原工具调用继续；否决异步重放）；②触发**双轨**：`RuntimeProfile.approval_tool_ids` 发布勾选 ∪ 工具元数据 `risk_level=high` 自动（工具目录注释早有该意图）；③**会话级单 pending**，用户直接回复「批准/拒绝」等词。**架构改动**：bus `ConsumeInbound` 改为**有界并发消费**（每消息 goroutine + 信号量，`WithConsumeWorkers` 可调）——否则单 worker 顺序消费下审批回复排在阻塞轮次后形成死锁；`RedisBus` 新增审批状态原语（`Set/Pending/Clear` + `Resolve/Result`，key `approval:req/res:{tenant}:{session}`）+ `RefreshLock`（Lua compare-token + EXPIRE）。**worker**：`approval.go`（humanReviewer：外发审批通知经 outbox → 阻塞轮询 `approval:res` → 每 10s 续会话锁；超时 5min 自动拒绝；`approvalPlugin` per-turn 装配 policy；`tryResolveApproval` **取锁前**无锁识别审批回复 → ResolveApproval + 外发确认 + audit[DecisionApprove/Deny]，不进 agent）；`run()` 装配 `runner.WithPlugins`；`toolsFromProfile` 返回需审工具名集（FunctionTool.Name 匹配 policy）。**验证**：单测 18 包全绿（新增 classifyApprovalReply/toolsFromProfile 双轨/notice 单测）；bus 集成（审批状态/锁续期 ownership/阻塞时并发消费仍可达）；worker 集成（批准放行全链路 + 拒绝 + MySQL outbox notice/confirm 双落库 + testcontainers）全绿。文档：根 `CONTEXT.md`（审批治理术语表）+ `docs/adr/0001-approval-model.md` |
| 14 Artifact MinIO 上传 | **完成** | Artifact 存储链路（清除阶段 11 遗留「Artifact MinIO」项）。复用框架 `artifact.Service` 接口 + `runner.WithArtifactService`（代码执行工具自动保存产物，平台无需手动上传）。`storage/artifact_minio.go`：`MinioArtifactService` 实现完整 Service 语义，对象键布局 `{tenant}/{user}/{session}/{filename}/{rev}`（借用框架 COS 约定；`user:` 前缀文件名走 `{tenant}/{user}/user/...` 跨会话持久），revision 递增=list 前缀计数（首存 0），ListArtifactKeys 双前缀（session + user namespace）与框架 inmemory 语义对齐；`NewMinioArtifactService` 幂等建 bucket。config 加 `MinIOConfig`（endpoint/access_key/secret_key/bucket/use_ssl；空 endpoint 禁用）；worker.New 加第 10 参 `artifact.Service`（nil 不启用）→ `run()` 装配 `runner.WithArtifactService`；main.go cfg.MinIO 非空时构造并接线。部署资产同步：configs/config.yaml 示例、backend-compose.config.yaml（指向独立 `artifact-minio:9000`/artifactadmin）、K8s 02-secret config.yaml（artifact-minio/minioadmin）。**验证**：依赖 minio-go v7.0.73（@latest 曾拖版本漂移+网络 EOF，改固定版本）；go build + 单测 18 包 EXIT=0（新增 key 布局/租户隔离/revision 解析单测）；storage 集成 testcontainers MinIO 全绿（版本化生命周期 rev0→rev1/latest/显式旧版/ListVersions/ListKeys/删除全清/二进制往返/mime/跨会话 user 前缀/租户隔离/入参校验）。CONTEXT.md 增「制品与对象存储」术语分组。剩余：`artifacts` MySQL 表待消费侧（代码沙箱）落地后回填 |
| 15 Docker 代码执行 | **完成** | 打通平台代码执行闭环（AGENTS.md §7 沙箱待确认项的落地基础）。关键事实：框架 v1.11.2 **无 container(Docker) 后端**（阶段0调研已过时），sandbox(seccomp) 仅 Linux/macOS，本机 Windows 不可用 → 平台自实现 `workspace.DockerExecutor`（实现窄接口 `codeexecutor.CodeExecutor`：ExecuteCode+Delimiter），经 **docker CLI** 执行 `docker run --rm -i --network none`（python:3.12-alpine `python3 -` / alpine:3 `sh -s`，代码走 stdin），120s 超时；**运行时错误语义**：非零退出（SyntaxError/异常）归入 Output 回给模型自修复，传输错误（CLI 缺失/拉镜像失败/超时）才是工具 error。工具接线：main 内置工具 `code-exec`（def id `code-exec`/name `execute_code`，**risk_level=high** → 审批 rail2 每次执行自动人工审批，与阶段 13 打通）+ `builtinToolSource` 分支（`tool/codeexec.NewTool(dockerExec)`，工具名与 def.Name 一致保 policy 匹配）+ RBAC Grant 沿用。框架 `tool/codeexec.NewTool` 返回 `CallableTool⊇tool.Tool` 可直接进 worker 工具列表。**验证**：go build + 单测 18 包 EXIT=0（新增语言映射/空输入不触 docker/不支持语言拒绝）；Docker 集成 testcontainers 宿主真实执行全绿：python `6*7=42`、bash echo、ZeroDivisionError traceback 落 Output、`--network none` 网络隔离探测；**遗留**：artifacts 表回填消费侧现已具备生产者（代码产物），但仍需 workspaceexec/codeact 级工具自动 SaveArtifact 场景；授权管理 web API（Grant 目前仅 store 层） |
| 16 工具授权管理 | **完成** | 补齐工具 RBAC 授权管理闭环（阶段 15 遗留；实际阶段 4 就缺管理入口）。问题：Agent 发布可勾选工具，但运行时 `IsAllowed` 需 grant（仅有 store 层，无 API/前端）→ 工具形同虚设。交付：`tool.Registry.GrantedAgents(toolID)`（反向查询：mem 遍历 + MySQL `agent_tool_grants WHERE tool_id=?`）；web ToolAPI 三端点 `GET /tools/{id}/grants` / `PUT`（grant）/`DELETE`（revoke），tool 不存在 404、grant/revoke 幂等；前端 `api/tool.ts`（listGrants/grantTool/revokeTool）+ ToolListView 从只读目录升级为带「授权」弹窗（勾选 Agent 多选→差集 grant/revoke，agents 列表复用 api/agent）。**验证**：tool/web 单测 EXIT=0（新 handler 用例：初始空/授权幂等/RBAC 生效/未知工具 404/revoke 失效）；全量单测 19 包 EXIT=0；vitest 12/12 + vite build EXIT=0。至此工具链路完整：目录 → 授权（Grant）→ 发布挂载 → 运行时 RBAC 校验 → 高风险自动审批 → Docker 执行 |
| 17 代码执行后端决策 | **完成** | grill-with-docs 澄清 + 决策（无代码改动）：①DockerExecutor 作用=code-exec 工具的执行后端（容器隔离层承担沙箱角色）；②「沙箱」术语辨析——平台实际隔离层是 **Docker 容器**（镜像+`--network none`），框架 `codeexecutor/sandbox`(seccomp 进程级 OS 沙箱) 未接入且仅 Linux/macOS；③用户确认生产部署形态 **Docker Compose/单机** → 关闭 §7「K8s Pod 后端」待确认项（**不实现**；K8s Pod 内无 docker socket 时该后端才必需，届时靠窄 CodeExecutor 接口后端替换补充）。CONTEXT.md「代码执行」分组补齐三术语：容器隔离层 vs sandbox 辨析、K8s Pod 执行后端（候选→已决策不实现）。未建 ADR（决策可逆、无重权衡，AGENTS.md §7 决策区记录足矣） |
| 18 storage Router 扩域澄清与统一数据访问抽象 | **完成** | grill-with-docs 收敛（无六域 Router 化）。**澄清**：「六域」出处=详细设计 §5.8 数据域 session/memory/summary/artifact/vector/audit；Router 现状只注册 session/memory（tenant 常量仅 DomainSession/DomainMemory）；其余域各自全局单后端（artifact=MinIO、knowledge=Milvus/内存、audit=MySQL），**summary 平台无独立存储**（随框架 session/mysql 表）。**grill 决策**：①动机=统一数据访问抽象 + 说明每域如何存储（非为对齐文档扩码、非逐租户可选后端）；②范围=轻量聚合 + 文档（**worker/web 签名不动**）；③**扩域准则**：域出现第二个后端实现才注册 Router，Tenant.DataBackend 保留为扩展点。**交付**：`storage/stores.go` `DataStores`（Router+Knowledge+Artifacts+Auditor 装配聚合，无 summary 字段并注释理由）+ `stores_test.go`（装配冒烟 + 域语义显式断言）+ main worker 分支单点组装后喂 worker；`docs/存储与数据访问设计.md`（数据域总览表[域×接口×后端×布局×一致性×状态] + 各域细节 + DataStores + Router 扩域准则/步骤 + §5.8 对照说明）；CONTEXT.md「数据访问与存储域」分组。**验证**：go build EXIT=0；storage/worker 单测 EXIT=0 |
| 19 Admin 对话(SSE) + IM 通道绑定 | **完成** | 补齐 web 消息路由缺口（README 注记的 `/chat` `/channels` 未实现项；grill 收敛：①/chat=SSE 流式·**不写账本**（chat_messages 仍预留，等会话历史/成本页启用）；②/channels=channel_bindings CRUD+前端页；③范围全做）。**后端**：bus `RedisBus.ReadOutbound`（XREAD 无消费组、返回流游标，admin SSE 独立跟随 outbound，不影响 IM group 消费）+ 集成测试（从 0/游标推进/`$` 只新）；`channels` 包 `ChannelBinding` + `BindingStore`（mem/MySQL 双实现，channel+account 唯一、物理删除保可重绑）+ 单测 + MySQL 集成测试；web `ChatAPI`（`POST /chat`：校验 tenant/text、无 route 且无 agent 报 400、agent 首条 SetRoute、Publish inbound channel=admin 返回 202+message_id；`GET /chat/stream` SSE：cursor 前进、只转发 admin+同 session 回复、flusher 心跳）+ `ChannelAPI`（bindings CRUD：wecom/feishu 白名单、dup 409）+ handler 单测（fake ChatBus：路由绑定/发布/404 语义/SSE 只推匹配会话，stream 泄漏防护）；main：ChannelAPI 全局注册（MySQL/mem store）、ChatAPI 在 Redis 可用 worker 分支注册。**前端**：`api/chat.ts`（sendChat + openChatStream EventSource）、`ChatView`（SSE 实时对话窗：租户/Agent/新会话/消息气泡/连接指示）、`api/channel.ts`、`ChannelListView`（绑定 CRUD：channel/租户/Agent/account/credential_ref）、路由 `/chat` `/channels` + 导航。**验证**：go build + 单测（web/channels/bus）EXIT=0；channels MySQL 集成 EXIT=0；vitest 12/12 + vite build EXIT=0 |
| 20 会话账本（chat_messages）启用 | **完成** | 启用 007 业务账本（README 原「预留」项，会话历史/成本页的基础）。grill 收敛：①角色=**USER+ASSISTANT** 每轮两行共享 turn_id（工具调用留 session_events/审计，不重复）；②写策略=worker 收尾**同步 best-effort**（失败仅日志不阻断/重试），`message_id` 唯一键幂等（INSERT IGNORE）；③交付=后端+前端+验证。**交付**：新 `trpcservice/chat` 包（`Ledger` 接口 + `MySQLLedger`：RecordTurn 单事务 upsert chat_sessions + 两行 INSERT IGNORE，Sessions 列表 member/tenant 过滤活动倒序、Messages turn 倒序+`BeforeTurn` 游标分页）+ MySQL 集成测试（幂等重放/turn 分组/分页/隔离/排序）；worker：`New` 加第 11 参 ledger（nil 不写）+ `recordLedger` best-effort + main 传入 ledger + 全链集成测试（TestWorkerLedgerWritesTurn：真实 worker turn 后 chat_sessions 1 行、USER/ASSISTANT 内容断言、同 turn_id）；web `ChatHistoryAPI`（GET /sessions、GET /sessions/{id}/messages，仅 MySQL ledger 时注册）+ handler 单测（fake ledger）；前端 `api/history.ts` + `SessionHistoryView`（会话列表 + turn 历史 + 「加载更早」游标分页 + 通道/角色标注）+ 路由 `/history` + 导航。**验证**：go build + 单测 EXIT=0；chat MySQL 集成 + worker 全链集成（含 ledger 断言）EXIT=0；vitest 12/12 + vite build EXIT=0。剩余：usage_records 成本归属页（成本核算）、secret manager、IM 真实 SDK 手测 |
| 21 secret manager（统一凭据管理） | **完成** | grill-with-docs 收敛（范围=模型 api_key + IM 通道凭据；存储=MySQL AES-256-GCM + 主密钥注入 + 内存 dev）。**现状债**：`llm.Endpoint.APIKey` 明文存 MySQL（TODO 早已标注 Phase 8）、`channel.credential_ref` 悬空引用。**交付**：`trpcservice/secret` 包（`Store` 接口 + `MemStore` dev 明文 + `MySQLStore` AES-256-GCM：主密钥 sha256 派生 32B、密文 `base64(nonce‖ct)`、空主密钥拒建）；`DDL 010_secrets.sql`；`config.SecretConfig{MasterKey}`（env `TRPC_SECRET_MASTER_KEY` 优先）；`web.SecretAPI`（POST/GET/DELETE /secrets，GET 仅 key+updated_at 不返回明文）；`llm.Endpoint.APIKeyRef` + `Registry.SetKeySource`（`KeySource` 接口=secret.Store 满足，Resolve 时 APIKeyRef 取用、无 source 报错、legacy APIKey 兼容）；main 接线（MySQL 无主密钥→secret 禁用，dev 用 mem；reg.SetKeySource）。**验证**：secret 单测（AES 往返/异钥解不开/空钥拒建）+ MySQL 集成（密文不含明文/往返/删除）+ web handler 单测（不泄露明文）+ llm KeySource 单测；全量单测 EXIT=0。文档：CONTEXT「凭据管理」分组 + `docs/adr/0002-secret-manager.md`。剩余：usage_records 成本页、IM 真实 SDK 手测（credential_ref 取用随之落地） |
| 22 用量计量（成本归属） | **完成** | 启用 usage_records（009 已建表但无生产者）。grill 收敛：①**只计量不折价**（token/工具/沙箱时长，不折算金额——无单价价目）；②**worker 写 token 维度**（复用 finalTextWithUsage 的 tokens，run 收尾 best-effort 幂等：record_id=入站消息 id+":token"，INSERT IGNORE 防重放重复计）。**交付**：`audit/usage.go`（`UsageEntry/UsageQuery/UsageRow/UsageSummary` + `Recorder` 接口加 `RecordUsage` + MySQLRecorder 实现同步 INSERT IGNORE 幂等 + `UsageRows` 明细 + `UsageSummary` 按维度聚合，支持 tenant/agent/dimension/时间过滤）；worker `recordUsage`（auditor nil 跳过、tokens≤0 跳过）；`web.UsageAPI`（`GET /usage` 返回 summary+rows，仅 MySQL 注册）+ main 注册；前端 `api/usage.ts` + `UsageView`（租户/Agent/维度筛选 + 维度聚合卡片 + 明细表，token 总量突出，不折价）+ `/usage` 路由 + 导航。**验证**：audit 单测 + MySQL 集成（幂等去重/聚合/维度过滤/明细）+ web UsageAPI 集成（GET 返回 summary+rows）+ worker 单测；全量单测 + build EXIT=0；vitest 12/12 + vite build EXIT=0。剩余：IM 真实 SDK 手测 |
| 23 IM 桥接层（gateway） | **完成** | IM 接线「桥接层优先」（grill：真实 WSS Conn + config + main 启动待真实账号手测）。现状：阶段 5 适配器（wecom/feishu 归一化+去重）已就绪，但 adapter.Inbound() 无人入 bus、stream:outbound 无人发回 IM——桥接层是缺口。**交付**：`channels/gateway.go` `Gateway`（`Bus` 接口 PublishInbound+ReadOutbound；`Attach` 起 pumpInbound goroutine：inbound→按 binding store/默认解析 agent→PublishInbound，`Message.ID=channel:platformMsgID` 幂等，记住 session→(adapter,chatID) 路由；`Run` 无消费组跟随 outbound、按 session+channel 分发到发起 adapter 的 `Send`，admin 通道跳过）；`gateway_test.go`（fakeBus+fakeAdapter 三用例：inbound 发布与字段断言/outbound 分发回 chatID/binding 优先于 attach 默认）。**验证**：channels 单测全绿；全量单测+build EXIT=0。剩余：真实 WSS Conn 实现（企业微信 openws / 飞书 ws.Client）+ config IM 段 + main gateway 角色装配，需真实账号手测 |
| 24 IM 真实 Conn（wecom aibot SDK + feishu lark SDK） | **完成** | 阶段 23 剩余项：真实 WSS Conn + config IM 段 + main gateway 装配。**SDK 选型**：企业微信用 `github.com/go-sphere/wecom-aibot-go-sdk/aibot`（用户指定，v1.0.4，WSS 长连接 `aibot_subscribe` 认证 + `aibot_msg_callback` 回调）；飞书用 `larksuite/oapi-sdk-go/v3`（`ws.Client` + `dispatcher.OnP2MessageReceiveV1` + OpenAPI `im/message` Create）。grill 收敛：**tenant/agent 路由 = config 声明连接 + binding 动态路由**（config IM 段只声明 bot 身份 + `credential_ref` 指向 secret store，tenantID 从 binding store 按 `AccountID=botID` 查，未绑定则跳过连接 warn）。**交付**：`wecom/conn.go`（`Conn` 用 aibot SDK：`OnMessageText` → `translateText` 把 `aibot.TextMessage` 映射成 XML `Message` 复用 `Adapter.Start` 的 xml.Unmarshal 归一化+去重；`Send` 用 `CreateTextReplyBody`+`SendMessage`）+ `conn_test.go`（群聊/单聊映射、无 msgid 跳过、Recv 生命周期）；`feishu/conn.go`（`Conn` 用 lark SDK：OnP2MessageReceiveV1 转发 JSON event，`Send` 用 OpenAPI Create）；`config.IMConfig{WeCom,Feishu}`（凭据值走 secret store、credential_ref 指向）+ config.yaml 示例；`cmd/trpc-service/gateway.go` `wireIMGateway`（config 读 bot → secret store 读凭据 → 构造 Conn → binding 查 tenant → Attach+Run，未绑定 warn 跳过）+ main worker 分支接线。**验证**：go build + 全量单测 EXIT=0；channels 单测（wecom/feishu/gateway）全绿。**剩余**：真实收发需本地账号手测（代码与配置已就绪） |
| 25 移除 K8s 清单 + begin.sh 一键启动 | **完成** | grill：K8s 部署清单删除（生产已定 Docker Compose，双份清单 YAGNI）+ begin.sh 全 compose 一键（检查 docker→.env→compose up -d --build→等健康→打印地址，up/down/status/logs）。**交付**：删 `deployments/k8s/` 10 个 yaml + 文档同步（deployments/README 删「Kubernetes 生产推荐」节、README 目录树、AGENTS §6、CONTEXT、MEMORY）；`begin.sh` + `.gitattributes`（*.sh 强制 LF，CRLF 破坏 bash）；compose mysql 端口可配置默认 3307（避本地 3306）+ backend 注入 `TRPC_SECRET_MASTER_KEY`（secret 主密钥，env 不落 config）。**验证**：begin.sh bash -n + compose yaml pyyaml OK。 |
| 26 联调修复（collection 首字符 / skill 租户 / IM binding 驱动） | **完成** | 验收暴露四问题，grill 收敛：①**Milvus collection 首字符**——`sanitizeName` 未处理数字开头 tenant，Create 拼名后首字符数字加 `kb_` 前缀 + 单测；②**skill 查询/挂载**——`skill_mysql.List` SQL bug（空 tenant 只查 `scope='global'`→tenant 级被过滤「刷新消失/agent 挂载不可见」+ OR 优先级泄漏软删）修 SQL + 集成断言「空 tenant 返回所有」；前端引入**全局租户选择器**（tenant store `currentTenantId`+localStorage、App.vue 下拉、skill/kb/agent store fetch 默认带 currentTenantId、切换 watch 刷新）；③④**IM binding 驱动**——删 config IM 段，新增 `channels.Manager`（`CredentialSource`+`AdapterBuilder`+`Reload` 按 binding 集动态启停 adapter）+ `ChannelBinding.VerificationTokenRef`（DDL 008+mem+mysql）+ ChannelAPI `SetManager`（CRUD 后 Reload）+ 前端通道页（Secret/Verification Token 明文自动入 secret store、binding 存 key 引用，保存即连接）。**验证**：go build + 全量单测 EXIT=0；前端 vitest 12/12 + vite build。**剩余**：真实收发手测（飞书长连接本就不需 verification token，字段为 webhook 模式预留） |
| 27 修 embedding 端点类型 + 当前时间工具 + trace 埋点 | **完成** | 三件事：①**bug-1（embedding 返回 text/html）**——三层错配：embedding 硬编码 `openai.New` 但用户端点是 anthropic 协议 + chat 模型（deepseek-v4-flash），且前端把 chat/embedding 端点混在一起。修：`Endpoint` 加 `Type`（chat/embedding，默认 chat）+ DDL 002 `endpoint_type` 列 + llm 提取 `ResolveAPIKey`（embedding 与 chat 一致走 secret store，消除阶段 21 遗漏）+ `RegistryEmbedderFactory` 校验端点类型 + 前端端点页加「类型」+ 知识库下拉只列 embedding 类型；②**当前时间工具**——`CurrentTimeTool`（照 echo FunctionTool 模板）+ 注册 registerBuiltinTools/builtinToolSource；③**trace 埋点**——gateway pumpInbound 建 `im.callback` span 并生成 TraceID 写入 bus.Message、Run 建 `im.reply` span、worker `withTraceID` 关联 agent.run span，Jaeger 可见「IM callback → Runner → Tool → Session/Memory → IM reply」一条 trace。**bug-2**（Unknown column）为环境问题：`down` 不删卷、init 只首次执行，需 ALTER 加列或重建数据卷。**验证**：go build + 全量单测 EXIT=0；前端 vitest 12/12 + vite build。 |

## 7. 待确认问题

- 无阻塞项（阶段 0 已全部确认）。
- ~~沙箱 K8s 后端落地方式~~ → **已决策（阶段 17 grill-with-docs）**：**不实现 K8s Pod 执行后端**。用户确认生产部署形态为 **Docker Compose / 单机**（`deployments/docker-compose.yml`），本平台自实现 `DockerExecutor`（docker CLI，容器级隔离 + `--network none`）已覆盖。K8s Pod 后端仅在「平台运行于 Kubernetes 集群内」时必要（Pod 内无 docker socket → docker CLI 不可用）；届时再补（框架 `CodeExecutor` 窄接口已天然支持后端替换）。

## 8. 已确认事项

- A.1 完整可运行实现（11 阶段全做）。
- A.2 沙箱：复用框架 `codeexecutor` + `tool/workspaceexec` + `plugin/guardrail/approval`；补 K8s 后端完成「K8s Pod exec」段。
- A.3 审批（Approval）纳入，复用 `plugin/guardrail/approval`，保证多租户安全。
- A.4 Skill 纳入，组织级资产：四级模型（Skill/SkillVersion/SkillScript/SkillReference）+ 三态（草稿/发布/禁用）+ 原子版本切换 + GLOBAL/TENANT 双 scope；复用框架 SKILL.md 格式与注入。
- B.1 数据库 MySQL（确认）。
- B.2 向量库 Milvus（复用 `storage/milvus`）。
- B.3 对象存储 MinIO（经 `storage/s3` S3 兼容复用）。
- B.4 消息总线 Redis Streams。
- C.1 租户层级：**两级 Tenant→Agent**（共享资源 ModelEndpoint/Skill[TENANT]/知识库为租户级资产；分组用 Agent 轻量 `group` 标签兜底，不引入 Workspace 实体）。
- D.1 ModelEndpoint 归一化层：平台层实现（baseUrl+modelName+apiKey，运行时路由+缓存复用），框架无此能力。
- D.2 模块名保留 `github.com/liuzengh/trpc-agent-service`。
- D.3 前端 E2E Playwright + UI 组件库 Element Plus。
- D.4 Admin API 用 RESTful（框架无通用 REST CRUD）。
