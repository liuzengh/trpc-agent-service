# Agent 数据能力：P0 SDK 映射与协作契约

日期：2026-09-09。集成基线：`4b1382cc848e55ebfa0088c423b5769394c398e2`。

## 状态与文件归属

这是实现中的 P0，不是全能力可用声明。Worker 当前新增装配 helper 与逻辑 Memory scope 的契约测试；生产 Manifest gate 保持关闭，executor 尚未从新增声明调用这些能力。

- Control：AgentSpec / Profile / Manifest schema、DTO、编解码、领域校验、编译与薄代理。独立工作树 `trpc-agent-service-control-data-contract`。
- Worker：SDK 装配、数据库 Adapter、运行数据拥有方、内部接口、权限与 tracing、Attempt 语义、`worker_v1.go` 门禁、Go 依赖和总集成。
- Web：`web/` 单写入，消费已提交契约；真实保存、回读、校验与发布。
- Gateway：维持 v1 请求与文本 Final，不传新 subject 字段，只做兼容性回归。

## AgentSpec 字段方向

最终规范以 Control 的 JSON Schema / fixtures 为准；本表是已协调的 SDK 映射。

| 字段 | 语义 / SDK 映射 |
| --- | --- |
| `node.memory.tools` | 必填唯一数组，可空；SDK `memory_add/update/delete/clear/search/load` 的完整名称；从最终 `memory.Service.Tools()` 筛选，仅显式选中工具可见 |
| `node.memory.preload_limit` | 可选；缺省/0 关闭；-1 加载全部；正安全整数为 SDK adaptive 条目预算，映射 `WithPreloadMemory`，不是 Token 预算；小于 -1 拒绝 |
| `node.artifact.enabled` | 显式启用 `runner.WithArtifactService`；不自动生成文件工具 |
| `runtime.summary.enabled` | 会话级摘要生成配置；缺省关闭 |
| `runtime.summary.model_slot` | 启用时必填，声明于模型 requirements，进入 Manifest 实际模型闭包 |
| `runtime.summary.event_threshold` | 启用时显式正整数，对应 `summary.WithEventThreshold`；不隐式增加 Token/输出限额 |
| `node.add_session_summary` | 显式 true 消费摘要，依赖全局 summary 启用；映射 `llmagent.WithAddSessionSummary` |
| `node.knowledge_slots` | 保留既有入口，解析检索资源，最终 `llmagent.WithKnowledge` |

Summary enabled=false 禁止带模型/阈值，避免非活动参数。Memory 空工具且 preload 缺省/0 不激活资源闭包。旧对象新增字段缺省必须保持 canonical/digest 兼容，不只检查可解析。Profile 存在资源不自动启用 Agent 能力。

## SDK v1.11.2 装配

Memory 使用最终 SDK `memory.Service`，同时装配工具与 Runner 服务。SDK 六工具从 invocation 获取 MemoryService；因此 Runner 必须传入执行语义包装后的同一服务。禁止 SDK 默认 Session UserID（当前为固定 `session`）直接成为长期记忆隔离键。

Summary 复用 SDK `summary.NewSummarizer(model, summary.WithEventThreshold(n))` 和 Session 摘要方法。PostgreSQL/Redis Session 后端有 Summarizer 选项。默认 Worker overlay 保持关闭；下述显式 Summary overlay 已复用 SDK 生成与边界逻辑并进入原 Session candidate，生产 factory 与 accepted-head 联合接线仍待完成，不能只开上下文注入选项。

Artifact 组合 S3 字节与 Worker SQL 元数据，提供一个 SDK `artifact.Service`。SQL 元数据不增加用户第二逻辑槽。SDK 初始文件版本为 0。管理上传与模型工具开放分开。

Knowledge 的 SDK `knowledge.Knowledge` 是 Search 接口。导入需要独立解析、分块、Embedding、Qdrant 写入与 Worker 文档/索引可见状态链，不把上传内容塞进 Manifest。

当前本地独立数据库 SDK 模块与 root SDK 版本不同；引入前需编译验证，不能将“有缓存源码”当成可直接兼容。

## Memory 作用域与提交语义

逻辑作用域编码：`StableID("mem", JSON(["worker-memory-scope/v1", TenantID, SocialIdentityID, stableAgentID]))`。

- `SocialIdentityID` 沿用可信请求的 Tenant + Provider + Account + Sender。
- `stableAgentID` 来自已验证固定 Manifest 的 `sources.agent.agent_id`。
- 不以 Control 登录 User、SessionID、AgentVersion 或 DeploymentRevision 替代。
- JSON 数组保留组件边界；不隐式修剪/合并不相同身份。
- Scope 派生不是授权；调用前必须完成请求、Manifest 来源、租户和权限验证。
- 同 Agent 跨会话/发布版本保持逻辑作用域。物理后端位置独立，更换目标不自动搬迁数据。

失败 Attempt 不写正式 Memory。同 Attempt 读己之写；成功接受结果须与待应用变更可靠持久关联，PG/Redis 应用幂等可恢复。后续 Run 的可见水位和 pending apply 恢复尚待实现；不使用接受后的内存回调代替可靠记录。自动提取入口同样不得绕过该规则。不新增手动 Memory 管理产品或通用跨库事务平台。

## 分批门禁

1. P0 AgentSpec schema / canonical 回归 + SDK 装配 / scope 基础测试。
2. Profile / Manifest DTO、编译闭包、诊断，默认和旧快照兼容。
3. Session PG/Redis → Summary → Memory PG/Redis → Artifact → Knowledge，逐项集成。
4. Artifact/Knowledge 管理链和真实 Web 保存回读发布。
5. 同固定 Manifest 的 Worker 运行与 Gateway 回归；真实 IM 与 fixture 分开验收。

每项必须具备真实后端读写/隔离/恢复证据才登记可运行。未接通时失败关闭，不能发布后静默忽略能力。保留旧工作树未提交代码，按能力择取，不整目录覆盖 Tracing。

## 测试生命周期修复

重复 race 回归观察到取消测试在 SDK flow 尚读全局 logger 时恢复 logger 的竞争。取消场景改用同一个带 race 检测的测试二进制子进程，保持原断言、固定该进程的 SDK globals 至退出；仍关闭测试 runtime/HTTP fixture，不改生产 logger，不用 sleep 掩盖竞争。

## P0b1 / Memory Attempt 实现进展（2026-09-09）

Profile managed 后端角色契约已集成，目录先验证 Tenant / role / 固定 revision，再派生角色隔离的 Backend Snapshot；Memory 与 Session 可以共享物理实例，但授权 digest 与隔离角色不混用。目录 revision 与 Web 同为安全正整数。Bootstrap 和按 Agent 声明生成 Manifest 的闭包仍待后续接线，不能把目录资格检查当作运行验收。

Worker 新增 `MemoryAttempt`：复用 root SDK `memory/inmemory` 和六种 SDK 工具，构造参数只有可信 SDK key、固定后端 scope、已加载快照与 base revision，没有持久 Store，也没有自动提取器。所有调用验证输入 scope、映射固定 scope；输入、读取与候选全部深拷贝。同 Attempt 读己之写；`Seal` 返回按 ID 排序的完整候选并禁止后续写入；`Close` 丢弃私有视图。候选仅保留基线 revision，正式接受记录、跨后端 CAS、冲突处理与下一 Run 可见水位仍未实现，不能对未接受 Attempt 的候选直接执行持久覆盖。

SDK兼容事实：
- SDK `memory_search` 工具固定带 `HybridSearch=true`，但 inmemory 后端只执行关键词检索。本适配明确采用关键词语义，接受该 SDK hint，不宣称语义向量检索；显式 RRF 参数仍拒绝。
- SDK `ReadMemories` 返回内部 Entry 指针，因此恢复快照时间戳的依赖封装在本模块中，并用别名/时间戳测试约束；对外不泄漏这些指针。
- SDK 默认 Memory 数量和搜索数量限制不成为新 Worker Policy：私有视图使用机器整数上限和不截断搜索结果；预加载仍遵循 Agent 显式配置。
- 引入 inmemory 增加 SDK 自身关键词分词依赖 `gse` / `cedar`，root SDK 保持 v1.11.2。

`TraceMemoryService` 为最终 Attempt 服务增加 `memory.read/search/write/delete` span。SDK工具从 invocation 使用外层服务，span沿调用上下文串联，不记录Memory正文、query、身份键或原始依赖错误；写span当前仅证明暂存操作，不代表正式数据库提交。

当前生产 executor 仍拒绝工具调用事件，runtime factory 仍未创建上述 MemoryAttempt。下一步必须连同工具事件验证、多轮usage、Manifest映射、candidate持久协议一起接通，而非先放开门禁造成静默丢失Memory候选。真实六工具/SDK Runner/候选测试是适配层验收，不是正式PG/Redis或IM验收。

## P0b2 聚合字段与 Worker 门禁（2026-09-09）

已集成共享 Manifest 的 `content.runtime.summary` 与 LLM 节点
`memory`、`artifact`、`add_session_summary` wire 字段、组件 Schema 和
Control domain clone/presence 校验。省略字段保留旧编码；严格解码拒绝
显式 null、false 和空启用组件，`add_session_summary` 仅允许 true。

这仍是契约接线，不是完整编译或运行支持。Worker `ValidateWorkerV1`
同批按字段存在性拒绝 `Runtime` 及任一节点的三个新增字段；直接 Go 调用
即使传入空组件或 `*bool(false)` 也被拒绝。执行器保持旧单 LLM 行为。
新 Manifest 在实际 Reader 中先完成 schema、digest 与来源校验，再返回
unsupported；非法 wire 返回 invalid，不生成可执行 Plan。

后端资源描述、编译闭包、Summary 模型依赖和最终工具名校验已在下述联合批次补齐。
Worker 对应适配/候选持久化/真实后端回归全部就绪后，再逐能力替换
拒绝门禁。不得以新增 DTO 或目录可选择作为开放生产能力的依据。


## 完整编译与 Manifest → SDK 桥接（2026-09-09）

Control 后端编译与完整应用切片已集成：managed Snapshot / DTO / 公开投影、
Memory / Artifact / Summary 最小资源闭包、Summary 模型依赖、最终 provider
工具名称碰撞校验，以及 Deployment resolver、Profile BackendAccess 和
SHA256 pin 目录 Bootstrap 接线。默认 Worker V1 静态合同仍不登记这些
未验收运行能力；目录存在不自动开启 Adapter。

Worker 新增 `BuildManifestCapabilityOptions`，把已认证且完成 digest/schema
校验的固定 Manifest 与选中 LLM 节点映射到 SDK Agent/Runner options。
它验证组件、storage role/resource、Backend tenant/role、Artifact metadata
contract、Summary 模型/Session 依赖与 Knowledge 维度，不隐式启用可用服务。
目前 Knowledge 只接受一个明确资源引用，多资源需要显式组合服务后再开放。
该桥接借用最终服务，调用者仍负责从同一固定资源构造服务、授权、生命周期、
Summary Summarizer/Session 安装及 Memory Attempt 提交语义。

`test-worker-manifest-sdk.sh` 重新运行真实 Control Compile，逐字节比对冻结
Manifest fixture，然后执行真实 SDK LLMAgent → memory_add → memory_load →
Final。模型为确定性测试 Model；Memory 为 traced Attempt 私有视图；生成的
候选保留固定 scope/base revision。真实 SDK Tool span 下观察到 memory.write
与 memory.read，正文未进入 OTLP。这个实验不连接 fixture 描述中的 Redis，
不运行生产 runtime factory，不写 accepted Attempt / 正式 Memory；它证明
编译产物和 SDK 选择契约已经对齐，而非完整存储上线。

下一运行切片：平台固定后端连接/凭据适配、Session PG/Redis 与 Summary
持久语义、Memory accepted candidate/CAS/恢复、Artifact S3+SQL 元数据、
Knowledge 检索与独立导入；随后联合 Web/真实 Provider/IM 验收。


## Session Summary 与真实 PostgreSQL 候选（2026-09-09）

`BuildManifestSummarizer` 从固定 `runtime.summary.model_resource` 选择已初始化
模型，复用 SDK `summary.NewSummarizer` / `WithEventThreshold`。不另加字数、
Token、跳过最近事件限制。SDK v1.11.2 事件阈值语义为 count > threshold，
不是 >=；模型、Session 资源缺失或不匹配时返回错误。

`newSummaryOverlay` 是显式启用构造。它把 `Session.Summaries` 和 SDK structural
boundary 放在原 snapshot 中，继续复用 `runtime_session.session_candidates`。
默认 `newOverlay` 仍拒绝带摘要的输入。输入来自 owned Session 深拷贝，模型
调用不持有 overlay mutex；模型期间新事件保留，不被旧摘要边界覆盖。结果
分别深拷贝到 stored 和 caller.Summaries，兼容 SDK 直接读取 Invocation.Session。

Enqueue 在当前调用同步处理请求的 filter，不 detach context、不提交后台任务。
暂不复制 SDK 默认 branch → full-session cascade；需要全局摘要时显式请求空
filter。正常 Runner 未指定 EventFilterKey 时使用 effective AppName，而不是
Agent 名称（SDK runner.go sessionRestoreFilterKey）；集成测试遵循这一默认值，
不修改 BranchFilterMode 来绕过匹配。

SDK 临时 inmemory service 会创建一个空闲 worker；直接同步 Create 后通过
Close/Stop/wg.Wait 释放。overlay.Close 只标记关闭，不取消/等待在途模型；调用者
必须等待同步调用返回再释放借用模型。Snapshot 在生成中或关闭后明确拒绝并记录
sticky failure，迟到结果不导入；失败/取消/容量错误也阻止候选输出。

`test-worker-summary-postgres.sh` 创建独立临时 PostgreSQL，使用 session_migrator
和 session_runtime 分离角色，执行现有迁移/Store。测试观察到：真实 SDK 摘要
生成 → 同一 Session candidate Put → 重开池 Load → overlay 恢复 → 下一次 SDK
LLMAgent 请求实际包含摘要。原 Head 对应字节不变、重复 Put 幂等、Tenant/Session
错配读拒绝、失败摘要不输出新候选；容器随后删除。

这些是真实 PostgreSQL 候选/SDK 消费证据，仍不等同于生产 accepted-head 事务、
外部 LLM、Redis 或 IM 验收。生产 factory/gate 暂未开启 Summary。

## Summary Executor / Factory 与正式接受事务（2026-09-09）

在前述候选实验之后，本批补齐运行适配与正式 head 验证，仍按能力分阶段发布：

- `Plan.Summary` 固定摘要 endpoint、model、credential use、event threshold 与
  AddSessionSummary。Factory 在 resolve 前验证用途/audience/endpoint，固定批次
  加入摘要凭据，完全相同 use 去重，同 ID 不同绑定拒绝。Prepare 私有复制 Summary
  配置；Close 清空主模型与摘要模型的 key。没有新环境变量凭据回退。
- `Executor.Request.Summary` 实际创建第二个固定 OpenAI-compatible 模型及 SDK
  Summarizer，安装到原 Session overlay，并按选项装配 AddSessionSummary。
  摘要仍是同一 Session snapshot 的数据；没有第二个摘要数据库或 migration。
- 主模型和摘要模型各自使用 attempt-local HTTP transport；重试为零。摘要请求
  使用发布的 `execution.max_output_tokens` 每次输出上限，不继承主节点更小的
  generation override，也不增加累计 Token 预算。成功 Run 的 usage 包含两者。
- 摘要 HTTP 使用非流式响应；适配器等底层 channel 关闭并完成 usage 后才交付 SDK，
  防止 SDK 收到 Done 提前返回。取消沿原 ctx 传播；未完成摘要不产出候选。
- 摘要 429/5xx/网络错误映射为可重试模型依赖；其他 provider 错误为模型失败，
  不误判为 Session 损坏。错误正文不进入 overlay 错误或 SDK 日志。

验收分成两个真实链路，不能将其拼成已经上线的端到端结果：

1. HTTP fixture → 实际 Executor/SDK 主模型+摘要模型 → Snapshot → 下一 Run 消费；
   检查独立凭据、发布输出上限、合计 usage、provider 失败、取消与 Done 生命周期。
   Factory 测试使用真实 TLS credential resolver 和内存 Store 测试替身。
2. `scripts/test-worker-summary-accepted-head.sh` 创建独立 PostgreSQL，使用部署
   八角色 provisioner、原 Worker/Session migrations 和实际 Ledger/Session Store。
   SDK Summary → Put 不推进 head → Complete 接受一次 → 新 Claim.Parent →
   重开 Store/Load → SDK 消费。模型失败、FAILED 带候选、错误 parent、实际 lease
   过期后的旧 Attempt Complete 均不提升 head。要求一主测五子测零跳过。

**该批次当时的发布状态（已由下述正式闭环批次取代）：**生产 Manifest Reader 尚未把 runtime.summary 投影为 Plan.Summary；
默认 WorkerV1 contract/gate 继续拒绝新能力。上述 Runtime 配置入口已可测试，但
不是用户配置 Summary 后线上自动生效。后续先补 Reader 固定资源映射与最小
Summary capability 合同，再联合 Control 编译/凭据导出/真实 Factory/数据库验收，
最后开放该能力。Memory 的 accepted CAS、Artifact 双存储以及 Knowledge 检索/导入
仍按后续独立切片完成；不因 Summary 接线而隐式开放它们。


## 正式 Session Summary 发布与运行入口（2026-09-09）

本批替换上一批的 Summary 拒绝门禁：Control WorkerV1 默认合同仅声明 summary，
Reader 构造固定 SummaryPlan，Factory/Executor 使用真实 SDK 并沿原 Session 接受事务
持久化。Memory/Artifact/Knowledge 与 managed Session 不随之开放。

配置、SDK 阈值语义、失败 Final、摘要启停连续性、真实运行验收及 release pin 切换流程
见 [正式 Session Summary V1](session-summary-v1.md)。既有共享服务保持原配置，
新增验收在独立环境完成，不以 helper/手工 Plan 成功代替实际生产代码运行链路。
