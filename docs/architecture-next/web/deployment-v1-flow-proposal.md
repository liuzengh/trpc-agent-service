# Deployment V1 前端流程设计提案

- 日期：2026-09-05
- 状态：文档与前端已实现，完成真实 API / 浏览器验收及独立副本回滚验证；代码保留在本地 Web worktree，尚未提交或合并。
- 代码基线：`codex/tenant-rbac-workspace`，`4d8c225c05fa86b6810349a0c95c56fa132a2412`，保留现有 Web 未提交改动。
- 权威顺序：当前注册路由、HTTP DTO、Application 和 Schema → 当前 Deployment 文档 → 历史设计。
- 当前契约已替代 Environment / 显式用户 binding / 服务端 DeploymentDraft 方案。

## 1. 产品定位与边界

主路径：AgentVersion + ProfileRevision → 同名配对校验 → 发布 DeploymentRevision 和 RuntimeManifest。

三个对象各自回答一个问题：

| 对象 | 用户问题 | 编辑位置 |
| --- | --- | --- |
| Agent | 做什么、有哪些节点、节点需要哪些逻辑槽位？ | Agent 编辑器，发布 Agent Version |
| Runtime Profile | 模型、工具、知识和存储实际连到哪里？ | 运行配置编辑器，发布 Profile Revision |
| Deployment | 固定用哪份 Agent 版本和哪份运行配置版本？ | 部署版本选择与校验工作台 |

Deployment 不再复制 Models / Tools / Knowledge / Storage 的连接编辑表单，不接收密钥、Environment 或 Slot → Resource 自定义映射。

发布成功表示控制面事务生成了不可变 Revision、Manifest 和 PENDING Outbox。当前运行分发、ChannelBinding、Gateway、Worker 仍属后续阶段。
前端使用“发布部署版本”“已发布 rN”，不用“上线成功”“运行中”“当前生效”“开始聊天”。
`latest_revision_number` 是最近发布序号，不是 active 指针；公开 DTO 没有 Outbox 投递状态，因此不绘制某条记录的投递进度或“已送达”。

## 2. 实际 API 与页面操作

下表路径省略公共前缀 `/v1/tenants/{tenant_id}`。浏览器请求通过现有 `/api/control` BFF。

| 方法与路径 | 页面操作 | 要点 |
| --- | --- | --- |
| POST `/deployments` | 创建稳定身份 | name、description；必需 Idempotency-Key；201 新建 / 200 原创建回执 |
| GET `/deployments` | 部署列表 | offset、limit，返回 deployments / total；只含元数据 |
| GET `/deployments/{id}` | 详情页头、并发基线刷新 | metadata_revision 与 latest_revision_number 分开 |
| PATCH `/deployments/{id}` | 编辑名称、描述 | expected_metadata_revision + 至少一个变更字段；语义无变化不推进 CAS |
| POST `/deployments/{id}/validate` | 校验固定两版本 | Body 直接是 DeploymentInput；200 时仍需检查 valid |
| POST `/deployments/{id}/revisions` | OWNER 发布版本 | input + expected_latest_revision_number；必需 Idempotency-Key；201 / 200 均成功 |
| GET `/deployments/{id}/revisions` | 发布历史 | Summary 分页，包含源 ID / 版本号 / Digest，不包含完整 Input / Manifest |
| GET `/deployments/{id}/revisions/{n}` | 不可变版本详情 | input、manifest_view、来源、Digest、发布者与时间 |

不存在：DeploymentDraft 保存/加载、独立 Manifest CRUD、Delete、Activate、Deactivate、运行、切流、运行状态接口。

### 请求边界示例

校验：`POST .../deployments/{id}/validate`，Content-Type 为 application/json：

```json
{
  "schema_version": "v1",
  "agent": { "agent_id": "agt_example", "version_number": 3 },
  "profile": { "profile_id": "rpf_example", "revision_number": 2 }
}
```

发布：`POST .../deployments/{id}/revisions`，另携带 Idempotency-Key：

```json
{
  "expected_latest_revision_number": null,
  "input": {
    "schema_version": "v1",
    "agent": { "agent_id": "agt_example", "version_number": 3 },
    "profile": { "profile_id": "rpf_example", "revision_number": 2 }
  }
}
```

第一次发布 expected 必须显式为 null；后续为用户确认时的最新发布正整数。校验请求没有 input 包装、没有 expected_latest_revision_number；不能直接复用发布 Body。

## 3. 信息架构：四个页面角色

以下四类 UI 路由已实现；发布与浏览器验收记录见文末。

1. `/tenants/[tenantId]/deployments`：部署列表。
2. `/tenants/[tenantId]/deployments/new`：首次版本准备。
3. `/tenants/[tenantId]/deployments/[deploymentId]`：部署工作台，概览 / 准备新版本 / 发布历史。
4. `/tenants/[tenantId]/deployments/[deploymentId]/revisions/[revisionNumber]`：固定版本详情。

“首次版本准备”和“准备新版本”复用一个组合编辑组件，不各写一套逻辑。
侧栏按“Agent 工作台 → 运行配置 → 部署 → 成员管理”组织。
Agent 的已发布版本页加“用此版本创建部署”；Profile 已发布版本页加同名入口。深链接只携带租户内对象 ID 和固定版本号，到目标页重新鉴权、读取、校验。

### 3.1 部署列表

列：名称/描述、最近发布版本、创建者、更新时间、操作。未发布显示“尚未发布”，不是“运行失败”。
动作：新建部署、打开工作台、查看最近版本（有发布时）。
列表 API 不含最近版本的 Agent/Profile 名称或运行健康状态；首版不放这几列，避免每行追读历史造成 N+1。
仅做后端已有的分页；名称搜索若首版采用客户端过滤，明确“筛选当前页”，不要把当前页结果伪装成全租户搜索。

### 3.2 版本准备页布局

```text
租户 / 部署 / 准备新版本                  当前角色：OWNER
名称：研究助手部署                       本地配置：尚未发布

① Agent 版本                           ② 运行配置版本
[研究工作流 ▼] [v3 · 固定版本 ▼]          [研究资源 ▼] [r2 · 固定版本 ▼]
查看 Agent v3                           查看 Profile r2

③ 资源配对概览（只读，本地预检查）
类别       Agent 槽位        Profile 同名 key     使用节点     结果
Models     primary           primary              researcher   名称匹配
Tools      search            缺失                 researcher   缺少同名资源
Knowledge  docs              docs                analyst      名称匹配
Storage    会话角色          session             全局运行角色   已声明

服务端校验：尚未执行
错误 / 警告面板（支持定位与修复入口）

[返回]                         [创建部署并校验] / [重新校验] [发布版本]
```

桌面顶部使用完整宽度的双来源卡片行；下方匹配表约占 2/3 宽度，右侧约 1/3 放校验结果。中等宽度优先展示诊断；窄屏来源与诊断上下排列。
版本选择器不能只显示 ID，也不使用数值输入让用户猜版本。显示当前名称、固定 vN/rN、发布时间；必要时折叠 Digest。
列表和详情异步读取有独立 loading / error / retry；读请求有 deadline，旧响应不得覆盖新租户或新版本选择。

## 4. 首次创建路径

1. 用户在新建页填写 name、description，选择 Agent 与其已发布 Version。
2. 用户选择 Profile 与其已发布 Revision；读取固定 Agent spec 和 Profile config。
3. 页面显示只读同名匹配预检查。来源没有已发布版本时给出“先发布 Agent/运行配置”的明确入口。
4. 用户点击“创建部署并校验”：先 POST 创建身份，再用返回 ID POST validate。按钮文本明确这是两阶段操作。
5. 创建成功后立即进入带 deploymentId 的工作台并持久化本地输入；validate 失败也保留已创建身份，下次只重新校验，不能再次创建。
6. 报告 valid=false：停留、定位问题并修复；仅 warning：展示警告但允许 OWNER 继续。
7. OWNER 确认固定 Agent vN / Profile rM、expected latest 和“发布不切流”后发布。
8. 201 或 200 均按返回 revision 身份导航到固定版本详情；不根据客户端猜测 rN+1。

为何不是“进入新建页立即创建空对象”：后端没有删除接口，取消操作会留下多余身份。
这里把身份创建推迟到用户明确点击“创建部署并校验”；若创建后的校验失败，展示“部署已创建，尚未发布”，不声称已回滚创建。
创建回执重放可能是原始元数据快照，拿到 ID 后需 GET Deployment 获取当前 metadata/latest，避免旧回执覆盖当前名称。

## 5. 更新已有部署路径

- 默认打开概览和发布历史，不自动把当前“最近 Profile”拼进一个新输入。
- 点击“基于 rN 准备新版本”，读取该精确 Revision 的 input 作为编辑初始值。
- 用户可以只替换 Agent 版本、只替换 Profile 版本，或都替换；当前两份固定来源始终可见。
- 有更高版本时提示“有新版本，点击查看”，不自动跟随 latest。
- 选择改变立即令旧校验结果过期；返回 Profile 修改后，即使选择号未变，动态凭据可能变了，也要求重新校验。
- 发布固定 expected latest；发生并发冲突后刷新当前元数据/历史、展示差异、保留用户输入，由用户确认新发布基线。
- “编辑部署名称/描述”用独立对话框和 Metadata CAS，不把修改名称混入发布事务。
- “基于旧版本重新发布”产生一个新历史记录，并按当前平台契约重新编译；它不是切流回滚，也不保证 Manifest Digest 与旧版相同。

## 6. 同名匹配与修复闭环

### 6.1 匹配规则的界面表达

- `requirements.models.primary` 只匹配 `config.models.primary`。
- `requirements.tools.search` 只匹配 `config.tools.search`，不匹配 `config.tools.web_search`。
- MCP 的 `tool_name` 是远端方法，不是资源 key；即使它是 search，也不替代 key 匹配。
- 同名之后还要满足 capability；有 Tools 或 Knowledge callable 的节点，其模型要满足 tool_call。
- 全部已声明 requirements 都执行存在性/能力检查；未被节点引用不代表缺资源时可以放行。
- `DEPLOYMENT_UNUSED_REQUIREMENT` 按节点引用独立产生，可与缺资源/能力错误同时出现；以报告 valid 和全部 error 决定发布资格，未使用资源不进入节点执行授权。
- Profile 额外资源不自动启用。发布后只从真实 manifest_view 查看实际闭包和逐节点分配。

页面预检查仅帮助用户理解名称与声明；不复制 Go Compiler 的平台允许范围、凭据授权与完整编译规则，不将本地绿色等同服务端可发布。

### 6.2 Storage 独立卡片

- `storage.session` 为必需固定约定；任意名为 conversation_state / state 的资源不自动顶替。
- `storage.memory` 可选；缺少表示不启用，存在但错误就显示错误。
- 其他 storage key 不自动启用；不新增 Agent Storage Slot 或可编辑角色映射。
- Profile 编辑器增加 session / memory 模板提示；若用户把同一数据库用于二者，仍显式建立两项。

### 6.3 错误与修复目的地

| 稳定 code/情形 | 用户说明 | 操作 |
| --- | --- | --- |
| DEPLOYMENT_RESOURCE_MISSING | Profile rM 缺少同名 Tools / search | 查看来源；去 Profile 草稿补资源并发布新 rM，返回重新选择 |
| DEPLOYMENT_CAPABILITY_MISMATCH | 对应模型/工具能力不满足声明或节点调用需求 | 定位类别、资源、节点；修正真实配置或 Agent 要求后重新发布来源 |
| DEPLOYMENT_STORAGE_ROLE_MISSING | 尚未配置固定的 session 运行角色 | 去 Profile 添加 storage.session |
| DEPLOYMENT_UNUSED_REQUIREMENT | 声明存在但无节点使用，不进入执行配置 | 警告不阻断；可选回 Agent 清理声明 |
| DEPLOYMENT_CREDENTIAL_UNAVAILABLE | 所需凭据目前不可用 | 打开当前 Profile Revision 的凭据管理，由 OWNER 检查；报告本身只有 /credentials 时不虚构具体坏字段 |
| DEPLOYMENT_ADAPTER_UNSUPPORTED / EXECUTION_RANGE_DENIED / LIMIT_EXCEEDED | 平台执行契约不接受资源类型、目标或配置 | 显示平台类问题与复制诊断；由平台配置处理，不在部署表单让用户修改白名单 |
| DEPENDENCY_UNAVAILABLE（503） | 凭据检查服务暂时出错 | 保留选择并重试，不要求重填 Key |

诊断定位由 code + source + path + category/name/node_id 共同驱动。缺资源诊断可能 source=agent，但修复在 Profile；不能单纯按 source 字符串决定跳转。
Profile 版本不可直接编辑；到草稿修复后要发布新版本并重新选择，不偷偷替换当前固定选择。
修复入口接收资源 focus target 与受控 return link；已接通 ProfileResourceEditor 的 focusTarget 和同租户跨路由返回协议。

## 7. 校验与发布的客户端状态模型

```text
editing → validating → invalid / ready / readyWithWarnings
ready → publishing → published
publishing → uncertain（连接中断、超时、部分 5xx）
publishing → conflict（Latest CAS 或幂等冲突）
任意来源选择变化 → editing（旧 report 失效）
```

- 校验 report 绑定本次 input 指纹和读取序号；过时响应不覆盖当前选择。
- 保存校验时间、compiler_version、platform_contract_digest 供说明/排错；valid=true 不代表下次发布永久成功。
- Validate 不预留版本、不发发布 token；Publish 仍会重新取事实、检查动态凭据与平台规则。
- Report 只有诊断，没有 manifest_view 或完整 resolved map；发布前页面标题用“配对概览”，不要标为“最终 Manifest”。
- 身份/来源读取与校验设置超时和可重试错误；写操作超时只说明结果未确认，不宣称后端取消。

### 本地编辑、跨页面返回与 MEMBER 交接

- name/description 只有 Create/PATCH 成功才显示“信息已保存”。
- 版本选择是客户端待发布配置，不显示“已保存到服务器的 Draft revision”。
- 可用按 userId + tenantId + deploymentId（或 new nonce）隔离的 sessionStorage 保存两份来源选择、编辑阶段与待确认的逻辑请求；不存 Profile config、credential_states 或凭据值。
- 页面刷新可恢复编辑与未确认请求；同一源输入恢复后重新做 Validate。
- 本地存储不可用时保留内存并明确提醒；退出登录/切换身份清理该身份数据。
- 离开/返回/新标签修复不丢输入；丢失持久化写意图时不自动用新 Key 重发。
- MEMBER 可以复制仅含 Deployment ID 和固定版本选择的准备链接，OWNER 打开后重新读取、校验并发布。它不是共享草稿或审批记录；服务端仍按当前 OWNER 身份鉴权。

### 幂等和 CAS

Create 与 Publish 各自生成一枚稳定随机 Idempotency-Key。传输层只发送，不为每次重试新造 Key。
- 超时后保留原 Key、完整原 Body（含 expected latest）并提供“重试确认本次发布”。
- 同 Key+同 Body 返回原成功结果（200）；不能先刷新 expected latest 再复用旧 Key，否则会变成幂等冲突。
- 409 DEPLOYMENT_LATEST_REVISION_CONFLICT：读取最新历史，用户确认新基线后开始新的逻辑操作，用新 Key。
- 409 DEPLOYMENT_METADATA_REVISION_CONFLICT：保留本地文字，对比当前值后再次确认 PATCH。
- 409 IDEMPOTENCY_CONFLICT：展示这是不同请求复用了 Key；不静默改 Key 重试。
- 422：渲染 response.validation；不是简单 toast 后清空表单。
- 401/403/404/503 与 200 valid=false 分开；失去 OWNER 权限后不继续发布，保留编辑用于恢复。

## 8. 发布版本详情

页头显示“部署名称 · rN / 已发布”，发布者、时间和精确来源链接。
正文四块：
1. 来源：Agent vN、Profile rM、各自 ID / Digest。
2. 节点资源分配：从 manifest_view.agent_plan.nodes 读取 model_resource、tool_resources、knowledge_resources、callable_entries；按节点显示哪些工具被分配。
3. 执行配置：实际 resources、resolved_requirements、storage_roles、execution 上限；全部只读。
4. 技术详情：manifest_id、manifest_digest、input_digest、compiler/runtime/platform 版本、脱敏 JSON 下载。

`credential_present=true` 表示发布配置中存在凭据关联，不表示当前 Key 在线有效。当前状态要单独去 Profile 管理查看；不要混进不可变快照。
脱敏 manifest_view 缺少内部字段，前端不使用其 JSON 自算 Digest 后与服务端 manifest_digest 比较。
不绘制“启动、停用、回滚流量、对话测试、在线状态”等当前没有 API 支撑的控件。

## 9. 权限矩阵

| 操作 | MEMBER | OWNER |
| --- | --- | --- |
| 列表、详情、版本历史、版本详情 | 是 | 是 |
| 创建身份、改名称/描述 | 是 | 是 |
| 选择两来源、服务端 Validate | 是 | 是 |
| 发布 DeploymentRevision | 否 | 是 |
| 修改 Profile 已用凭据 | 按 Profile 规则否 | 按 Profile 凭据规则是 |

Publish 的 OWNER 校验已在 Application 实现；前端展示清晰角色说明，服务端处理直达请求。
无目标 Tenant Membership 的平台管理员没有隐式操作权限。

## 10. 前端落地拆分与验收

实施模块（已有部分 Client/组件；完成状态以验收证据为准）：
- `web/lib/deployment-api.ts`：八个方法、独立 Deployment DTO/Error/ValidationReport，保留 200/201 与 error.validation。
- `web/lib/deployment-editor-state.ts`：固定来源选择、状态转换、幂等请求意图、本地恢复。
- `web/components/deployments/`：list、source-selector、pairing-overview、validation-report、workspace、revision-detail。
- 四个 app route 页面仅负责路由参数、身份入口和组合。
- 复用 AppShell、ui、通用对话框外壳和表格视觉；不要把 ProfileWorkspace 连同服务端 Draft 语义原样复制。
- 现有 BFF 已转发 Cookie、Idempotency-Key、错误 Body 和状态，GET no-store；仍补 Deployment 路由的转发回归。

### P0：最小可用闭环

八 API client → 四页面角色 → 固定来源选择 → 诊断 → OWNER 幂等发布 → Revision/Manifest 只读详情。
同时具备本地恢复、超时/uncertain、CAS 恢复与 MEMBER 权限提示。

### P1：操作效率

Agent/Profile 版本页快捷入口、资源定位返回、按 Agent Requirements 辅助创建 Profile 资源骨架（不猜真实连接或凭据）、版本 Input 对比。

### P2：确有需求再加 API

- 若必须预览准确 Manifest，新增/扩展只读 validate 预览返回脱敏视图；目前只有报告，首版不阻塞。
- 若列表必须有来源名/全局搜索，扩充轻量 List projection；不前端逐行查询完整 Manifest。
- 若必须 MEMBER→OWNER 跨设备持续协作，再讨论服务端共享准备态；先不虚构 Draft 保存 API。
- 分发/运行/切流页面待相应 Runtime 与 ChannelBinding 契约落地后加入。

### 必测场景

1. 基本同名成功、类别正确但名称不同、同名 capability 不足。
2. 多节点只看到自己的工具；未使用 requirement 与多余 Profile 资源的区别。
3. 缺少 storage.session、可选 memory、仅 Knowledge callable 也要求模型 tool_call。
4. 没有已发布来源；跨 Tenant/失效来源；固定版本不随 latest 漂移。
5. Validate HTTP 200 valid=false、warning-only、旧报告过期、动态凭据变化后 Publish 422。
6. 双击创建/发布、请求响应丢失、刷新恢复同 Key/同 Body重放、两客户端不同 Key 的 Latest CAS 冲突。
7. MEMBER 可以 Validate 但 Publish 被拒；平台管理员无 Membership 时也被拒。
8. 创建成功但校验失败不重复创建；元数据和待发布配置保存状态清晰。
9. Back/Forward/修复 Profile 后返回仍保留输入；正确选择新发布的 Profile rM。
10. 发布后详情展示实际 Manifest View、不暴露凭据内部字段、不声称已经运行或切流。

## 11. 本次核对证据

当前文档、路由、DTO、Application、Schema、Profile 和 Agent 的现有 Web Client 已交叉核对。
运行以下定向契约检查：

```bash
GOCACHE=/tmp/trpc-agent-sync-main-gocache go test ./api/openapi/control/v1 \
  -run 'TestControlOpenAPIContainsDeploymentV1Routes|TestControlOpenAPIDeploymentSchemasExposeFrozenContract|TestDeploymentValidationReportFixtures' \
  -count=1
```

实际输出：`ok github.com/liuzengh/trpc-agent-service/api/openapi/control/v1 1.933s`，退出码 0。
这验证代码中的路由/Schema/Fixture 一致性，不代表当前监听端口已加载新版 Deployment 后端，也不代表 Web 已实现。

## 12. 当前权威来源

- [Deployment 领域与用户流程](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/docs/architecture-next/control-api/deployment.md)
- [八个 HTTP 路由](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/services/control-api/internal/deployment/adapter/inbound/http/routes.go)
- [严格请求 DTO](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/services/control-api/internal/deployment/adapter/inbound/http/request.go)
- [响应、诊断和 HTTP 错误](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/services/control-api/internal/deployment/adapter/inbound/http/response.go)
- [发布幂等、权限与 CAS](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/services/control-api/internal/deployment/application/publish.go)
- [校验与 Profile 凭据检查](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/services/control-api/internal/deployment/application/compile.go)
- [当前 OpenAPI](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/api/openapi/control/v1/openapi.yaml)
- [Deployment Input Schema](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/api/schemas/deployment/v1/deployment-input.schema.json)
- [脱敏 Manifest 示例](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/api/schemas/deployment/v1/examples/valid/runtime-manifest-view.json)
- [现有 Profile UI](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/components/runtime-profiles/profile-resource-editor.tsx)
- [现有 Profile API Client](/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/web/lib/runtime-profile-api.ts)

## 13. 已确认实施计划与完成门槛

先文档后代码；保留当前 Web worktree 的全部已有改动。

- [x] 八个 API 方法和类型、超时/错误/CAS/幂等逻辑。
- [x] 四类页面、部署导航、固定版本选择、名称预检查与诊断。
- [x] OWNER 发布、MEMBER 查看/准备/校验、元数据 CAS 编辑。
- [x] 本地输入恢复、同请求重试、冲突恢复、分享准备链接。
- [x] 不可变版本详情、逐节点工具、Manifest JSON 和来源对比。
- [x] Agent/Profile 版本快捷入口、Profile 定位修复与返回。
- [x] 定向及全量测试、类型检查、独立目录构建。
- [x] 真实 Control API + PostgreSQL 的创建/校验/发布/重放/权限测试。
- [x] 浏览器实际创建发布和截图/布局检查。
- [x] 修改包、差异、VERIFICATION.txt、可执行回滚，独立副本恢复测试。

P1 的“按需求生成 Profile 资源骨架”和 P2 API 扩展保留为后续增强，不作为新造未实现 API 的理由。本轮完成快捷入口、诊断往返与输入对比。

## 14. 实施与实际验收记录（2026-09-05）

- 四类页面、侧栏导航、固定来源选择、分页、配对预检查、诊断复制与修复往返已接通。
- OWNER 发布、MEMBER 准备/校验、Metadata CAS、同请求重试、本地恢复和输入对比已实现。
- 校验结果绑定固定输入；选择变化/返回来源修复后失效；刷新不会把后续编辑覆盖回入口链接的原版本。
- 真实 HTTP 回归经独立构建 Web BFF 13003 → 新版 Control API 18083 → 克隆 PostgreSQL：36 请求、94 断言通过。两个会话同 CAS 竞争结果为 201/409；Revision/Manifest/PENDING Outbox 均为 2 条。
- 浏览器实际创建“多工具研究部署 · Web验收 20260905”：Agent v1（6 节点、3 模型、4 Tools、2 Knowledge）+ Profile r1 缺 tools.search；诊断定位 Profile；返回后明确选择修复的 r2，校验通过并发布 Deployment r1。
- 浏览器发布身份：dpl_jmP2jWriLEWB76owTFkDAzER；Manifest rmf_7sjAA3UsuL1Hu1dW7TazFs11。
- 1134px 初次视觉检查发现来源框被侧边诊断挤窄，已改为完整来源行；390/1134/1440px 均无文档横向溢出，输入高 44.5px；手机发布按钮保持单行。
- 开发端口 13001 已切换到含 Deployment 的 18083 后端。切换前核对原后端 13 张业务表的每个原有行均在克隆中保持一致；原 18082 后端与数据库保持原样，新增测试记录只在副本中。
- 证据目录：`artifacts/deployment-web-20260905/`，含 HTTP_E2E_REPORT.json、UI_JOURNEY.json、SOURCE_COPY_COMPARISON.json、构建/测试日志和页面截图。
- 发布是控制面快照，不是 Provider 连通性测试、Worker 执行或流量切换；前端保留这个明确区分。
- 全量前端测试 27 文件 / 268 项通过；类型检查及独立目录生产构建通过。保留的原始 83 文件经 SHA-256 核验；基线与回滚副本均为 22 文件 / 202 项测试通过，其他已有改动保持原样。
- 副本测试首轮遗漏 api/schemas/agentspec 测试数据；补齐未修改的后端 fixture 后通过。这是验收脚本目录装配问题，未为通过测试修改产品逻辑。原始失败记录保留在证据目录。
