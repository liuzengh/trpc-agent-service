# Web、对话与管理页面、服务管理及配置支持：按 PR 序号

核对日期：2026-09-16。覆盖原评审批次 `2026-09-14-open-pr-astra-xhigh-v2` 榜单中的 **19 个独立 PR**；已关闭的重复 PR #5 不重复计入。表格按 **PR 编号升序**，与综合得分排名无关。

依据原批次冻结清单（`manifest.json`）、19 张评审卡及各 PR 的冻结源码，逐项查看前端页面、实际请求、服务端路由、配置装配和已有验证结论。19/19 checkout 的 HEAD 与清单一致，受跟踪文件无改动。本次是专项静态分析，**没有启动浏览器/服务、重新构建前端或重跑业务测试**；“源码支持”“已有独立运行验证”和“未验证”分别表述。原榜单、评分卡与封存记录保持原样。原始材料存于原评审批次，本次只提交专项分析文档；评审记录入口见 [PR 评审过程记录](pr-review-records.md)。

Web 按浏览器聊天与管理控制台解释。对话管理关注会话创建、切换、历史回读、持久化和删除语义；管理页面关注实际导航及服务端接线；服务管理区分状态查询、业务开关/恢复处置与进程部署；配置区分启动参数、租户资源编辑、不可变版本、灰度和回滚。这里的“有页面”表示有源码与服务接线，不表示已独立完成浏览器验收。

## 整体分布

| 交付形态 | 数量 | PR |
| --- | ---: | --- |
| 聊天/调试＋管理/监控页面 | 7 | #8、#13、#19、#23、#24、#25、#26 |
| 仅聊天页面 | 3 | #14、#15、#27 |
| 仅管理页面 | 4 | #6、#16、#17、#22 |
| 有 HTTP/API，未见浏览器页面 | 4 | #4、#18、#20、#21 |
| 仅设计与包骨架 | 1 | #3 |

合计 **14 个 PR 有浏览器页面，10 个有聊天或调试对话，11 个有管理或监控页面**。管理范围差异很大：#13/#23/#25/#26 的页面覆盖多个业务域；#6/#16/#17 侧重管理与发布；#22 侧重接入向导；#24 侧重 IM 模拟与监控。上述分类描述功能覆盖，不重新评分，也不代表运行稳定性或安全性排名。

## 全量支持表（按 PR 升序）

| PR | Web 交付与管理页面 | 对话与会话管理 | 服务管理 | 配置支持 | 已知边界与验证 |
| --- | --- | --- | --- | --- | --- |
| [#3](https://github.com/liuzengh/trpc-agent-service/pull/3) | 仅设计；没有可运行页面，web/config 包为骨架。 | 没有聊天、会话目录或历史管理实现。 | 设计包含独立 Admin、健康检查、扩缩容、发布回滚；入口尚不运行平台服务。 | 规划租户模型、工具、后端、SecretRef、版本指针与灰度；未交付配置加载/管理实现。 | 不能把设计中的“管理与对话页面”计为已实现。 |
| [#4](https://github.com/liuzengh/trpc-agent-service/pull/4) | **管理 API**：租户、Agent、版本、Binding、后端、审计；未见浏览器前端。 | 没有浏览器聊天及会话管理页面。 | healthz/readyz/metrics、渠道健康；all/gateway/worker/admin 角色与 CLI。 | YAML＋环境变量；API 创建租户、应用版本、通道/后端并发布，读取生效 RuntimeProfile。 | 既有独立评审复现 **Admin 无租户凭据可跨租户读写**；有 HTTP 服务不等于有管理页面。 |
| [#6](https://github.com/liuzengh/trpc-agent-service/pull/6) | **运维控制台** `/console/`：概览、应用发布、后端、请求追踪、审计、迁移状态；Go 内嵌静态资源。 | 无直接聊天页；Inbox/Attempt/Outbox 展示运行元数据，不展示对话正文。 | 页面查执行/投递与迁移；API 有执行对账、Outbox 重放和工具审批；角色化部署、readiness/drain。 | 页面创建租户/应用/版本，发布 stable/canary 与比例，选择 Session/Memory profile；服务器校验权限和 SecretRef。 | 迁移页面以状态查询为主，执行由运维流程推进；控制台存在不代表完整外部 IM 或浏览器流程已独立验收。 |
| [#8](https://github.com/liuzengh/trpc-agent-service/pull/8) | **聊天＋轻量管理页**：根页面聊天，`/admin/` 展示总览、租户、Binding、任务、最近消息元数据、审计。 | 指定 binding/user/conversation 发文本，轮询消息终态；无服务端会话目录或历史编辑页面。 | 页面查看节点/任务；Admin API 可查 reconciler、修改 placement/assignment、租户策略与脱敏规则。 | 启动 JSON 定义模型、工具、后端与 Binding；任务冻结配置版本；部分治理/调度配置可经 API 改动。 | 管理页主要读数据，并非完整配置编辑器；既有评审复现 **公开 demo/web 自报身份后可执行另一租户业务**。 |
| [#13](https://github.com/liuzengh/trpc-agent-service/pull/13) | **React 综合控制台**：租户、应用、部署、Chat、IM 通道、运行节点、数据管理、治理观测；Go 提供前端资产。 | 创建/打开指定 Session，服务端事件回读、SSE、重试与取消；浏览器记住当前 Session ID。 | 页面支持部署激活/暂停、灰度与回滚预览、运行状态、优雅 drain；另有渠道诊断/重放、迁移与开发故障注入。 | 共享控制面维护租户/应用/不可变 DeploymentVersion；页面创建版本、调整灰度、回滚，选择后端和治理策略。 | Chat 集合端点只创建，会话目录能力不能由该路径名称推断；源码概览页仍为空态。既有拓扑的 Worker/默认入口故障未恢复。 |
| [#14](https://github.com/liuzengh/trpc-agent-service/pull/14) | **嵌入式聊天工作台**：Agent/Binding 目录＋聊天；管理能力另走 Basic Auth API。 | SSE 回复，Cookie 标识浏览器归属；页面切换 Agent 时清空当前显示，未见服务端会话目录/历史管理页。 | 管理 API 管租户、App、Binding、版本激活，以及迁移开始/推进/回滚；页面没有这些运维表单。 | YAML 启动；租户/App 配置与版本持久化，API 激活版本、失效 Binding 缓存，SecretRef 配置。 | 既有独立 WebUI 业务运行通过，但 **任意公开 Binding 可被未认证租户用户选择**；全局 Basic Auth 不构成租户自主管理。 |
| [#15](https://github.com/liuzengh/trpc-agent-service/pull/15) | **轻量聊天页**：多对话侧栏、连接设置、运行标识；管理功能是独立 Bearer API。 | SSE，当前页内新建/切换对话、停止请求；服务端保持 Session，浏览器对话列表/标题/消息刷新后丢失。 | healthz、租约与退出处理；当前交付为合并进程，缺生产角色编排与运维管理页。 | API 建 Tenant/App/不可变 Revision/BackendProfile；发布旧版本可回切。聊天 Key 与 Admin Key 分离，Secret/Policy entitlement 校验。 | 没有权重灰度、配置失效通知、完整操作审计；安全清单启动时加载。聊天“设置”不提供完整平台配置编辑。 |
| [#16](https://github.com/liuzengh/trpc-agent-service/pull/16) | **独立 React/TDesign 管理站**：总览、租户、应用、配置版本、后端、通道、执行、审批、迁移、审计、运行状态。 | 没有直接聊天页；执行记录可显示 Session ID，`/admin/v1/session` 是管理员身份会话。 | 页面启停 Binding、处理审批、开始迁移、查询节点/执行/错误；独立 Nginx 代理管理 API。 | 不可变配置发布＋激活；版本差异查看；Canary 启用/暂停/提升/回滚，后端引用、凭据签发/撤销 API。 | 后端页主要展示配置引用；有配置/服务控制不等于页面管理进程生命周期。Channel 单 owner 的部署限制仍在。 |
| [#17](https://github.com/liuzengh/trpc-agent-service/pull/17) | **Next.js 管理站＋独立文档站**：账号、平台管理员、租户成员、Agent 画布、Runtime Profile、Deployment、渠道、Run/审批/审计/用量。 | 没有直接聊天/对话预览输入页；Run 详情用于执行诊断。 | 页面管理渠道接入与路由两个开关、固定部署目标和连接观测；发布 Deployment 与实际接入渠道是分开的动作。 | 草稿带 expected_revision 并发校验；校验后发布不可变 AgentVersion/ProfileRevision/RuntimeManifest，渠道固定引用具体版本。 | 发布部署不会自动启用渠道或证明 Worker 已运行；README 部分状态较旧，Run/审批/审计页以冻结源码为准。既有固定 proof 地址存在故障单点。 |
| [#18](https://github.com/liuzengh/trpc-agent-service/pull/18) | **管理 API**；未见 HTML/React/Vue 聊天或控制台页面。 | IM 业务有 Session，未见浏览器会话列表、正文查看和编辑入口。 | 独立 Gateway/Admin/Worker；API 管 Binding、Knowledge 文档、存储迁移和审计；有健康与部署配置。 | 启动环境变量；数据库保存租户/App 模型、工具、审计、限流、存储策略；发布/回滚原子切换，Redis 通知失效。 | Admin 为平台级单 Bearer token。既有独立评审发现显式 deny 的 Memory 工具仍能写库，配置字段存在不代表策略有效执行。 |
| [#19](https://github.com/liuzengh/trpc-agent-service/pull/19) | **两个嵌入页面**：`/webui/` 聊天；Admin 管理台展示租户、App、Model、Binding 和 Session 迁移。 | 指定路由/user/chat ID，签名请求；持久回复轮询＋临时 SSE 增量，工具确认；未见完整会话目录/改名/删除 UI。 | 页面可操作 Session 迁移切换、观察、回滚等；API 扩展到 Knowledge/Memory 迁移和配置 release。 | 租户配置 validate/stage/publish/rollback，expected_version 并发检查；配置 release 的 rollout/rollback；目录页不等于全功能配置编辑器。 | WebUI 双实例业务和故障恢复已有独立证据；**独立生产 Worker 可被首次模型/后端配置通知终止**，Memory 迁移尚未完成切流。 |
| [#20](https://github.com/liuzengh/trpc-agent-service/pull/20) | **管理与业务 HTTP API**；web 包仍是骨架，未见浏览器页面。 | 业务接口支持请求状态/取消；没有浏览器对话或会话管理页。 | 租户作用域恢复项查询/redrive、未知结果人工决议；迁移计划/查询/取消、知识摄入/检索 API。 | YAML＋严格校验；租户版本 validate/publish/current/list/rollback，expected_version 防并发覆盖；回滚将旧内容发布成新版本；Vault/KMS 引用。 | 管理能力主要面向 API 使用者。独立故障窗口有 PostgreSQL 40001 准入冲突；完整配置隔离/管理流程不能据源码认定通过。 |
| [#21](https://github.com/liuzengh/trpc-agent-service/pull/21) | **基础 HTTP API**：`/api/chat`、租户创建、Webhook、探针；未见浏览器资产。 | 有聊天 API，无对话列表/历史页面。 | readiness、停止接收与优雅退出；另有恢复/迁移工具，缺管理控制台。 | 环境变量启动；configpub 库实现不可变 Revision、校验、CAS 发布、灰度与回滚，`CONFIGPUB_ENABLED` 默认关闭；运行装配可读取版本。 | 未见 configpub 写操作的浏览器或公开管理 HTTP 接线；灰度桶按 tenant ID 生成。既有原 CLI 第二租户和新会话执行均有缺陷。 |
| [#22](https://github.com/liuzengh/trpc-agent-service/pull/22) | **React 接入向导**：登录→选择/创建租户→创建/选择 Agent→连接 Telegram/企微/飞书；可设默认 Agent、暂停 Agent。 | 没有直接浏览器聊天或历史会话管理页；最终引导到外部 IM 交谈。 | 页面创建并轮询通道连接状态；API 管租户/App/模型/后端/Binding、状态及通道连接。 | 页面可配置指令/模型端点并组装后端、创建 Revision 后发布；API 另有版本、回滚、Canary 与缓存失效。 | **PG 多能力 BackendProfile 创建/回读可返回 503**；通道连接凭据/状态有进程内部分，重启需重连；页面覆盖小于完整 API。 |
| [#23](https://github.com/liuzengh/trpc-agent-service/pull/23) | **React 综合工作台**：聊天、偏好/账号、机器人、成员、租户/用户、知识库、执行记录、模型、后端、登录设置、系统状态。 | 服务端个人会话目录、历史分页、SSE、附件、工具审批；新建/切换；“删除会话”调用服务端 archive。 | 页面查运行节点/依赖/渠道/执行；模型同步、资源管理；迁移等能力另有 API。系统状态页主要观测，不是集群启停台。 | 应用配置与版本历史、候选版本、灰度、提升/停止/回滚；租户模型/工具/后端授权和系统登录配置。 | 会话归档不等于物理删除所有数据；独立原生 SSE 业务与故障恢复已有证据，完整浏览器流程、Memory 迁移等未独立验证。 |
| [#24](https://github.com/liuzengh/trpc-agent-service/pull/24) | **React IM 模拟控制台**：登录/租户选择、会话、消息详情、监控、自定义模型；Go 内嵌构建资产。 | 同步 JSON 返回完整回复；本地多会话、搜索/排序/未读、改名/清空；列表与消息存在按租户/用户分区的 localStorage。 | 页面读健康/指标；API 有依赖健康、drain、Outbox/工具未知结果对账和配置节点状态。 | YAML 导入/reload；不可变 Revision/Release、稳定 Session 灰度、节点 prepared/applied/verified ACK、提升与回滚；完整发布主要通过 API。 | **清空只改浏览器状态，不清除服务端 Session**；“保持登录”可保存 token 到 localStorage。既有 OIDC 管理重复读取复现审计冲突 503。 |
| [#25](https://github.com/liuzengh/trpc-agent-service/pull/25) | **React/Ant Design 管理工作台**：引导、模型连接、Agent、资源、机器人、运行记录、系统状态；同源 Cookie＋CSRF。 | 草稿快照上的隔离调试会话；服务端保存 session/run，恢复最近会话、重新开始、取消、审批与 SSE 状态；不等同终端用户会话目录。 | 机器人准备/激活/暂停/重试与诊断、Worker 依赖观测、任务重试、工具/出站对账；资源/迁移 API。 | 持久草稿并发版本校验、readiness/验证、发布/回滚/灰度；模型与后端连接、凭据轮换、Knowledge/Skill/工具配置。 | 前端必须先构建，资产缺失管理页返回 503；既有构建与 Admin RBAC/资源目录检查通过，完整调试/发布浏览器闭环未据此认定通过。 |
| [#26](https://github.com/liuzengh/trpc-agent-service/pull/26) | **Vue/Element Plus 综合前端**：租户、Agent、模型端点、工具、知识库、Skill、聊天、历史、渠道、密钥、审计、用量、用户。 | SSE；服务端会话目录与按 turn 分页的业务对话账本，支持新建/选择会话；未见改名/删除会话接口。 | 资源 CRUD、渠道管理；死信查询/重放 API；Admin/Gateway/Worker/all 角色与部署工具。 | Agent 版本发布/回滚；租户配置版本与回滚；configctl 可导出/diff/apply/dry-run；模型端点、密钥、工具/Skill 授权可配置。 | 既有独立评审已复现 **SSE 跨租户读取、注册 owner 提权**；历史查询的租户检查不能替代流接口授权。 |
| [#27](https://github.com/liuzengh/trpc-agent-service/pull/27) | **最小 HTML 聊天页**：选租户、发文本、SSE；管理另走 API，无管理表单。 | 刷新生成随机 user，当前 DOM 展示对话；无持久会话目录、改名/删除或历史恢复页面。 | 健康/就绪探针、可靠角色分工；Admin v1 管租户与全局设置，v2 管 App/Revision/工具等控制资源。 | 旧路径 YAML 原子保存并热应用；可接共享 RuntimeStore。可靠路径 MySQL 不可变 Revision、发布与回滚，v2 有数据库身份授权。 | **公开 WebChat 可伪造 URL 租户**；独立可靠路径还复现已提交历史未进入模型上下文。不能把新版 Admin 身份机制套用到公共聊天入口。 |

## 聊天类页面的管理语义（按 PR 升序）

以下只展开具有聊天/调试页面的 10 个 PR。其余 9 个 PR 的无页面、仅 API 或仅管理页边界已在总表逐一列明。SSE 表示事件流；使用 SSE 不自动代表模型逐 token 输出、断线可完整重放或后台任务可取消。

| PR | 新建/切换 | 历史与持久化 | 改名/删除/清空 | 回复与运行操作 | 身份边界 |
| --- | --- | --- | --- | --- | --- |
| #8 | 手填 conversation/user/binding；当前页面消息 | 单消息终态查询，未见目录/历史回读 UI | 无改名/删除 UI | 1.5 秒轮询；无流式输出 | 公开 demo 身份可自报 |
| #13 | 创建/输入 Session ID；本地记住当前 ID | 从服务端 events 恢复；不提供 Chat 集合 GET 列表 | 未见改名/删除聊天会话接口 | SSE、重试、服务端取消 API | 登录/租户角色；完整授权矩阵未独立完成 |
| #14 | 按 Agent/Binding 与 Cookie 归属继续对话 | 当前页面显示；未见服务端历史管理 UI | 切换只清空显示 | SSE；未见取消按钮/API | Cookie 保护归属；租户访问缺少身份限制 |
| #15 | 页面内新建/切换多对话 | 服务端保存上下文；前端目录/消息不跨刷新恢复 | 当前页自动标题；未见服务端管理接口 | SSE；AbortController 停止当前请求 | Chat Key 与 Admin Key 分离 |
| #19 | 指定 external_chat_id 连接 | 按 chat/user 拉取持久回复；无完整会话目录 UI | 未见改名/删除 UI | SSE 临时增量＋终态轮询；工具确认 | 路由绑定签名凭据 |
| #23 | 新建/切换；服务端个人会话列表 | 服务端消息分页；可加载更早历史 | 页面“删除”＝服务端 archive；未见改名 | SSE、附件与审批 | Cookie/CSRF、租户与会话归属检查 |
| #24 | 本地新建/搜索/排序/切换/未读 | localStorage；无服务端目录/历史同步调用 | 本地改名/清空，服务端历史仍保留 | 同步 JSON 完整回复；非 SSE 聊天 | Admin 凭据；登录可选本地持久化 |
| #25 | 草稿专用调试会话，恢复最近会话/重新开始 | 服务端 debug_session/run，非业务会话总目录 | 重新开始独立调试；无全量业务会话管理 UI | SSE 状态、服务端取消、审批 | tenant/app/owner 隔离；Cookie/CSRF |
| #26 | 新建/选择；服务端会话目录 | 业务账本按 turn 分页，有独立历史页 | 未见改名/删除会话接口 | SSE；未见任务取消 UI | JWT；已复现 SSE 跨租户读取 |
| #27 | 页面初始化随机 user，选择租户 | DOM 当前记录；无历史恢复页 | 无改名/删除 UI | SSE | 无可信租户身份；可靠路径另有历史缺陷 |

## 对选型有直接影响的区别

1. **完整会话目录要核对服务端。** #23、#26 有服务端会话列表与历史回读；#13 支持指定 Session 恢复，#25 支持专用调试会话恢复。#24 的丰富会话交互主要保存在浏览器，#15 甚至只保存在当前页内存中。跨设备查看、平台清理与数据保留要求不能从相似的侧栏界面推导。
2. **管理 API 往往比页面完整。** #6 的迁移页主要读状态，#8 管理页主要读数据，#19 的页面重心在资源目录与 Session 迁移，#22 是接入向导，#24 是模拟控制台。发布、灰度、恢复、审批等能力需要逐项标明是页面操作还是额外 API/CLI。
3. **配置保存、发布和生效是不同阶段。** #17 将 Agent/Profile/Deployment 固定为版本快照，并另行启用渠道；#16 将发布、激活和 Canary 操作拆开；#24 有节点准备/应用/验证 ACK；#25 先保存草稿，再调试或发布。#21 的版本发布模块默认关闭且缺公开写入管理面，不能按库接口直接算成可用控制台。
4. **服务状态页不等于服务进程控制。** #23/#25 有节点与依赖观测；#13 有 drain；更多实现通过应用/Binding 开关控制业务，通过 CLI、Compose 或 Kubernetes 管进程。页面发布配置不自动意味着创建模型进程、重启 Worker 或扩缩集群。
5. **已有缺陷会改变功能的实际可用范围。** #4 的 Admin、#8/#14/#27 的公共聊天、#26 的 SSE/注册都存在已复现的授权问题；#19 配置通知可终止独立 Worker，#22 多能力后端配置可失败，#24 重复管理读取可 503。这些是本批次已有证据，不能被“有登录页”“有角色字段”或“API 已实现”覆盖。

## 逐 PR 依据与补充说明

以下源码链接固定到各 PR 的冻结 HEAD 和对应行号，可直接在 GitHub 阅读；各 PR 另列原评审卡在批次归档中的相对标识。运行验证及已复现缺陷沿用该卡与关联原始证据，原评审卡和原始日志不随本文上传。

### PR #3 · 冻结 HEAD `75e881df450efbdd019aeae00e8d076ed2dd3560`

设计里提到 Admin CRUD、版本灰度及管理页面，但冻结入口只打印版本。按实现口径，这四类能力均不能记为已交付。

证据：[Web 骨架](https://github.com/liuzengh/trpc-agent-service/blob/75e881df450efbdd019aeae00e8d076ed2dd3560/trpcservice/web/web.go#L2)；[配置骨架](https://github.com/liuzengh/trpc-agent-service/blob/75e881df450efbdd019aeae00e8d076ed2dd3560/trpcservice/config/config.go#L2)；[设计中的 Admin](https://github.com/liuzengh/trpc-agent-service/blob/75e881df450efbdd019aeae00e8d076ed2dd3560/docs/design.md#L161)；[启动入口](https://github.com/liuzengh/trpc-agent-service/blob/75e881df450efbdd019aeae00e8d076ed2dd3560/cmd/trpc-service/main.go#L10)；原评审卡：`reviews/pr-3.md`（存于原评审批次）。

### PR #4 · 冻结 HEAD `26a63c44cba4d475a4ee058318eb4367725fdc57`

源码没有前端资源服务路由；租户/版本/审计 API 确实存在。最重要的管理边界是未认证访问已被实测复现，不能因默认监听或部署说明而忽略。

证据：[HTTP 路由与租户读写](https://github.com/liuzengh/trpc-agent-service/blob/26a63c44cba4d475a4ee058318eb4367725fdc57/trpcservice/web/web.go#L75)；[启动配置](https://github.com/liuzengh/trpc-agent-service/blob/26a63c44cba4d475a4ee058318eb4367725fdc57/trpcservice/config/config.go#L38)；[角色入口](https://github.com/liuzengh/trpc-agent-service/blob/26a63c44cba4d475a4ee058318eb4367725fdc57/cmd/trpc-service/main.go#L95)；原评审卡：`reviews/pr-4.md`（存于原评审批次）。

### PR #6 · 冻结 HEAD `f2e091e315ffd3f1710645c44505043c789e9aae`

控制台是有服务端读写接线的运维产品面。请求视图刻意只提供运行元数据；迁移页的“有状态展示”与“有操作按钮推进迁移”需要分开。

证据：[控制台交付与操作边界](https://github.com/liuzengh/trpc-agent-service/blob/f2e091e315ffd3f1710645c44505043c789e9aae/docs/OPERATIONS_CONSOLE.md#L21)；[实际页面导航](https://github.com/liuzengh/trpc-agent-service/blob/f2e091e315ffd3f1710645c44505043c789e9aae/pkg/console/assets/app.js#L20)；[管理写入/重放/审批 API](https://github.com/liuzengh/trpc-agent-service/blob/f2e091e315ffd3f1710645c44505043c789e9aae/cmd/admin/controlplane.go#L27)；[控制台路由与权限](https://github.com/liuzengh/trpc-agent-service/blob/f2e091e315ffd3f1710645c44505043c789e9aae/cmd/admin/operations_console.go#L18)；原评审卡：`reviews/pr-6.md`（存于原评审批次）。

### PR #8 · 冻结 HEAD `5ea17f0f3ae8b457367c8e8384404d935ee9e86e`

聊天按单条 message_id 查询终态，不是会话历史浏览器。Admin 页面主要 GET catalog/tasks/audit/nodes；修改 placement/policy 等是额外 API 能力。

证据：[聊天交互及轮询](https://github.com/liuzengh/trpc-agent-service/blob/5ea17f0f3ae8b457367c8e8384404d935ee9e86e/trpcservice/httpapi/static/index.html#L67)；[管理页导航与只读请求](https://github.com/liuzengh/trpc-agent-service/blob/5ea17f0f3ae8b457367c8e8384404d935ee9e86e/trpcservice/httpapi/static/admin.html#L77)；[管理 API 路由](https://github.com/liuzengh/trpc-agent-service/blob/5ea17f0f3ae8b457367c8e8384404d935ee9e86e/trpcservice/httpapi/admin.go#L51)；[Web 协议边界](https://github.com/liuzengh/trpc-agent-service/blob/5ea17f0f3ae8b457367c8e8384404d935ee9e86e/docs/stage5-telegram-wecom-webui.md#L67)；原评审卡：`reviews/pr-8.md`（存于原评审批次）。

### PR #13 · 冻结 HEAD `10b667250d4d982523f4ffa96b365e48258f4d0c`

聊天服务的 `/api/v1/chat/sessions` 只接受 POST，已有会话通过指定 ID 和 events 读取恢复。数据管理页另有会话数据查看，但不据此认定聊天侧已经具备完整目录、重命名、删除。运行 drain 按实现作用域理解，不能外推为任意集群节点重启。

证据：[页面导航与概览空态](https://github.com/liuzengh/trpc-agent-service/blob/10b667250d4d982523f4ffa96b365e48258f4d0c/frontend/src/App.tsx#L14)；[会话创建/恢复与事件流](https://github.com/liuzengh/trpc-agent-service/blob/10b667250d4d982523f4ffa96b365e48258f4d0c/frontend/src/ChatPage.tsx#L171)；[会话后端路由](https://github.com/liuzengh/trpc-agent-service/blob/10b667250d4d982523f4ffa96b365e48258f4d0c/trpcservice/platform/chat.go#L741)；[部署/版本/灰度](https://github.com/liuzengh/trpc-agent-service/blob/10b667250d4d982523f4ffa96b365e48258f4d0c/frontend/src/DeploymentsPage.tsx#L6)；[运行状态与 drain](https://github.com/liuzengh/trpc-agent-service/blob/10b667250d4d982523f4ffa96b365e48258f4d0c/frontend/src/RuntimePage.tsx#L6)；原评审卡：`reviews/pr-13.md`（存于原评审批次）。

### PR #14 · 冻结 HEAD `7ffdd18ab2a7c2a475801a7aa552a9d461b2f575`

切换 Binding 的 resetTranscript 只清理当前页面；SSE 和 Cookie 归属不提供租户登录授权。Admin API 的全局 Basic Auth 与公开聊天入口是两种不同访问边界。

证据：[聊天页面交互](https://github.com/liuzengh/trpc-agent-service/blob/7ffdd18ab2a7c2a475801a7aa552a9d461b2f575/trpcservice/web/index.html#L279)；[Cookie 与 SSE 接线](https://github.com/liuzengh/trpc-agent-service/blob/7ffdd18ab2a7c2a475801a7aa552a9d461b2f575/trpcservice/channels/webui/adapter.go#L48)；[Basic Auth 管理与迁移 API](https://github.com/liuzengh/trpc-agent-service/blob/7ffdd18ab2a7c2a475801a7aa552a9d461b2f575/trpcservice/admin/handler.go#L69)；原评审卡：`reviews/pr-14.md`（存于原评审批次）。

### PR #15 · 冻结 HEAD `f144ced3758ae4abb01c36690d9375e683e5452f`

页面顶部注释与实现都明确对话状态驻留当前页；服务端 Session 持久性依赖选择的后端。停止按钮调用 AbortController，不能据此认定存在持久化的后台任务取消记录。

证据：[页面内存对话与停止请求](https://github.com/liuzengh/trpc-agent-service/blob/f144ced3758ae4abb01c36690d9375e683e5452f/trpcservice/web/ui/assets/app.js#L5)；[聊天及管理入口](https://github.com/liuzengh/trpc-agent-service/blob/f144ced3758ae4abb01c36690d9375e683e5452f/trpcservice/web/platform.go#L83)；[Admin 资源/版本实现](https://github.com/liuzengh/trpc-agent-service/blob/f144ced3758ae4abb01c36690d9375e683e5452f/trpcservice/web/admin.go#L118)；[配置与限制说明](https://github.com/liuzengh/trpc-agent-service/blob/f144ced3758ae4abb01c36690d9375e683e5452f/docs/admin-api.md#L280)；原评审卡：`reviews/pr-15.md`（存于原评审批次）。

### PR #16 · 冻结 HEAD `d942529fd65fe056a078265b63084eb66c6a9985`

`session` 接口返回 AdminPrincipal/角色范围，不能计成“聊天 Session 管理”。版本页面确有 JSON 草稿、差异、激活与 Canary 操作；发布配置、激活配置、启用通道是不同动作。

证据：[实际页面与配置操作](https://github.com/liuzengh/trpc-agent-service/blob/d942529fd65fe056a078265b63084eb66c6a9985/admin-ui/src/app.tsx#L45)；[管理 HTTP 路由](https://github.com/liuzengh/trpc-agent-service/blob/d942529fd65fe056a078265b63084eb66c6a9985/trpcservice/admin/http.go#L34)；[部署代理](https://github.com/liuzengh/trpc-agent-service/blob/d942529fd65fe056a078265b63084eb66c6a9985/admin-ui/nginx.conf#L9)；[配置持久化](https://github.com/liuzengh/trpc-agent-service/blob/d942529fd65fe056a078265b63084eb66c6a9985/trpcservice/postgres/app_config.go#L1)；原评审卡：`reviews/pr-16.md`（存于原评审批次）。

### PR #17 · 冻结 HEAD `a2cabfff86a99e058911c5303d11f471c648b03e`

冻结源码已有 runs、approvals、audit、usage-governance 页面，README 中“Run/Preview consoles 未实现”的旧表述不能整体照搬。仍未看到直接发送对话的 Chat/Preview 页面。Deployment 发布固定版本组合，渠道账号接入与 Binding 路由还需分别启用。

证据：[页面与发布边界说明](https://github.com/liuzengh/trpc-agent-service/blob/a2cabfff86a99e058911c5303d11f471c648b03e/web/README.md#L42)；[Agent/Profile/Run 管理客户端](https://github.com/liuzengh/trpc-agent-service/blob/a2cabfff86a99e058911c5303d11f471c648b03e/web/lib/control-api.ts#L1)；[Deployment 发布接口](https://github.com/liuzengh/trpc-agent-service/blob/a2cabfff86a99e058911c5303d11f471c648b03e/web/lib/deployment-api.ts#L1)；[实际 Run 页面](https://github.com/liuzengh/trpc-agent-service/blob/a2cabfff86a99e058911c5303d11f471c648b03e/web/app/tenants/%5BtenantId%5D/runs/page.tsx#L1)；[账号和角色边界](https://github.com/liuzengh/trpc-agent-service/blob/a2cabfff86a99e058911c5303d11f471c648b03e/docs/site/administration.md#L11)；原评审卡：`reviews/pr-17.md`（存于原评审批次）。

### PR #18 · 冻结 HEAD `c0794b3cb234fe9cf36f5bf48085ec4535984d26`

平台启动参数来自环境变量，租户/App 的动态配置来自数据库管理 API，这两层应分别描述。`web` 包名不意味着浏览器 UI；管理单 token 也不是租户级 RBAC。

证据：[完整管理路由与单 token](https://github.com/liuzengh/trpc-agent-service/blob/c0794b3cb234fe9cf36f5bf48085ec4535984d26/trpcservice/web/admin.go#L64)；[启动配置机制](https://github.com/liuzengh/trpc-agent-service/blob/c0794b3cb234fe9cf36f5bf48085ec4535984d26/docs/configuration.md#L9)；[Gateway 业务入口](https://github.com/liuzengh/trpc-agent-service/blob/c0794b3cb234fe9cf36f5bf48085ec4535984d26/trpcservice/web/gateway.go#L46)；原评审卡：`reviews/pr-18.md`（存于原评审批次）。

### PR #19 · 冻结 HEAD `2fd83ab946e1311973d8ae9f03ba952d104771e3`

聊天持久回复和临时增量采用不同数据来源；临时 SSE 不能替代最终 Result/Outbox。管理台实际重点是资源目录与 Session 迁移，配置发布/灰度等更宽能力位于 API。已通过的 WebUI 组合不覆盖独立生产角色配置通知缺陷。

证据：[聊天路由、回复与进度](https://github.com/liuzengh/trpc-agent-service/blob/2fd83ab946e1311973d8ae9f03ba952d104771e3/trpcservice/channels/webui/http.go#L69)；[实际聊天页面](https://github.com/liuzengh/trpc-agent-service/blob/2fd83ab946e1311973d8ae9f03ba952d104771e3/trpcservice/channels/webui/assets.go#L1)；[管理台与迁移表单](https://github.com/liuzengh/trpc-agent-service/blob/2fd83ab946e1311973d8ae9f03ba952d104771e3/trpcservice/admin/ui/index.html#L9)；[版本/发布/迁移 API](https://github.com/liuzengh/trpc-agent-service/blob/2fd83ab946e1311973d8ae9f03ba952d104771e3/trpcservice/admin/http.go#L26)；[管理登录与同源 Cookie](https://github.com/liuzengh/trpc-agent-service/blob/2fd83ab946e1311973d8ae9f03ba952d104771e3/trpcservice/admin/console.go#L25)；原评审卡：`reviews/pr-19.md`（存于原评审批次）。

### PR #20 · 冻结 HEAD `53435cc638a216934f78b4f48002e7e82ad6ac84`

Admin 有实质运维 API：重放恢复项、决议不确定结果、管理迁移和知识数据。版本回滚不是改写历史记录，而是把旧 payload 作为新版本发布；未见对应浏览器表单。

证据：[配置/迁移/恢复 HTTP 路由](https://github.com/liuzengh/trpc-agent-service/blob/53435cc638a216934f78b4f48002e7e82ad6ac84/trpcservice/admin/handler.go#L55)；[恢复与版本服务](https://github.com/liuzengh/trpc-agent-service/blob/53435cc638a216934f78b4f48002e7e82ad6ac84/trpcservice/admin/service.go#L222)；[生产入口鉴权接线](https://github.com/liuzengh/trpc-agent-service/blob/53435cc638a216934f78b4f48002e7e82ad6ac84/cmd/trpc-service/production.go#L712)；[Web 包骨架](https://github.com/liuzengh/trpc-agent-service/blob/53435cc638a216934f78b4f48002e7e82ad6ac84/trpcservice/web/web.go#L2)；原评审卡：`reviews/pr-20.md`（存于原评审批次）。

### PR #21 · 冻结 HEAD `892d3f1c9970203de09bdb2e783d3de73f74d68d`

configpub 不是纯设计，库与读取装配确实存在；但本轮未找到公开管理写端点。默认未开启该装配，且灰度 bucket 以租户为单位，同租户请求共享该分桶，不应表述成会话级随机百分比分流。

证据：[实际 HTTP 路由](https://github.com/liuzengh/trpc-agent-service/blob/892d3f1c9970203de09bdb2e783d3de73f74d68d/trpcservice/web/server.go#L101)；[发布/回滚库](https://github.com/liuzengh/trpc-agent-service/blob/892d3f1c9970203de09bdb2e783d3de73f74d68d/trpcservice/configpub/coordinator.go#L95)；[默认关闭的运行装配](https://github.com/liuzengh/trpc-agent-service/blob/892d3f1c9970203de09bdb2e783d3de73f74d68d/cmd/trpc-service/config_composition.go#L17)；[租户分桶规则](https://github.com/liuzengh/trpc-agent-service/blob/892d3f1c9970203de09bdb2e783d3de73f74d68d/trpcservice/configpub/rollout.go#L20)；原评审卡：`reviews/pr-21.md`（存于原评审批次）。

### PR #22 · 冻结 HEAD `094f58d550c32fe4452589c306a88664a8b82333`

UI 目标是把租户、Agent 和外部 IM 连接起来。“Ready to chat”/连接成功文案不表示页面有聊天框，也不证明完整模型回复。源码公开 API 的版本/灰度范围大于向导提供的按钮。

证据：[接入向导与真实请求](https://github.com/liuzengh/trpc-agent-service/blob/094f58d550c32fe4452589c306a88664a8b82333/trpcservice/web/src/App.tsx#L63)；[管理 API](https://github.com/liuzengh/trpc-agent-service/blob/094f58d550c32fe4452589c306a88664a8b82333/trpcservice/admin/api.go#L114)；[进程拥有的连接](https://github.com/liuzengh/trpc-agent-service/blob/094f58d550c32fe4452589c306a88664a8b82333/trpcservice/admin/connections.go#L30)；[前端静态资源服务](https://github.com/liuzengh/trpc-agent-service/blob/094f58d550c32fe4452589c306a88664a8b82333/trpcservice/web/web.go#L12)；原评审卡：`reviews/pr-22.md`（存于原评审批次）。

### PR #23 · 冻结 HEAD `8f6d0b5e27832495e58ac1137279b6328fd39a79`

页面名“删除会话”调用 `/api/v1/sessions/archive`；这是服务端归档能力，未据此承诺清除 SDK Session、Memory、Artifact、审计等全部关联数据。历史正文按权限回读，个人目录和租户管理视图有不同授权。

证据：[导航与页面](https://github.com/liuzengh/trpc-agent-service/blob/8f6d0b5e27832495e58ac1137279b6328fd39a79/webui/src/App.tsx#L88)；[会话目录/分页/归档 UI](https://github.com/liuzengh/trpc-agent-service/blob/8f6d0b5e27832495e58ac1137279b6328fd39a79/webui/src/pages/ChatPage.tsx#L29)；[删除映射 archive](https://github.com/liuzengh/trpc-agent-service/blob/8f6d0b5e27832495e58ac1137279b6328fd39a79/webui/src/api.ts#L627)；[服务端路由](https://github.com/liuzengh/trpc-agent-service/blob/8f6d0b5e27832495e58ac1137279b6328fd39a79/trpcservice/web/console_api.go#L241)；[版本与灰度客户端](https://github.com/liuzengh/trpc-agent-service/blob/8f6d0b5e27832495e58ac1137279b6328fd39a79/webui/src/api.ts#L1033)；[系统状态页](https://github.com/liuzengh/trpc-agent-service/blob/8f6d0b5e27832495e58ac1137279b6328fd39a79/webui/src/pages/SystemPage.tsx#L17)；原评审卡：`reviews/pr-23.md`（存于原评审批次）。

### PR #24 · 冻结 HEAD `7139c3df9a9d1b5324f59d6420838d6d45026f17`

本地会话 ID 会用于真实请求，但列表、消息展示、改名和清空由 Zustand/localStorage 管理。clearConversation 没有删除服务端记录的调用，因此 UI 清空后模型仍可能保留该 Session 历史。后台灰度与 ACK 功能来自控制面，不等于页面已提供全部发布操作。

证据：[本地对话存储与清空](https://github.com/liuzengh/trpc-agent-service/blob/7139c3df9a9d1b5324f59d6420838d6d45026f17/im-console/src/stores/chat.ts#L27)；[同步聊天调用](https://github.com/liuzengh/trpc-agent-service/blob/7139c3df9a9d1b5324f59d6420838d6d45026f17/im-console/src/api/chat.ts#L5)；[实际聊天与管理路由](https://github.com/liuzengh/trpc-agent-service/blob/7139c3df9a9d1b5324f59d6420838d6d45026f17/trpcservice/web/server.go#L156)；[版本/Release 管理](https://github.com/liuzengh/trpc-agent-service/blob/7139c3df9a9d1b5324f59d6420838d6d45026f17/trpcservice/web/control_api.go#L131)；[登录持久化](https://github.com/liuzengh/trpc-agent-service/blob/7139c3df9a9d1b5324f59d6420838d6d45026f17/im-console/src/stores/auth.ts#L2)；原评审卡：`reviews/pr-24.md`（存于原评审批次）。

### PR #25 · 冻结 HEAD `b26c10b33380bc9773b65f31e8c3ce01d0525f23`

DebugPanel 使用保存的草稿快照创建专用调试身份，服务端按 tenant/app/owner 约束访问；支持最近会话恢复、重新开始、审批和取消。它是管理人员的调试工作台，不能替代全量业务用户会话检索与删除系统。系统状态页自身说明只检查依赖，不自动启动模型或注册 IM 回调。

证据：[页面导航](https://github.com/liuzengh/trpc-agent-service/blob/b26c10b33380bc9773b65f31e8c3ce01d0525f23/trpcservice/web/console/src/App.tsx#L20)；[调试会话恢复/取消/审批](https://github.com/liuzengh/trpc-agent-service/blob/b26c10b33380bc9773b65f31e8c3ce01d0525f23/trpcservice/web/console/src/debug.tsx#L17)；[调试后端隔离与持久化](https://github.com/liuzengh/trpc-agent-service/blob/b26c10b33380bc9773b65f31e8c3ce01d0525f23/trpcservice/admin/debug.go#L33)；[全部管理路由](https://github.com/liuzengh/trpc-agent-service/blob/b26c10b33380bc9773b65f31e8c3ce01d0525f23/trpcservice/admin/handler.go#L46)；[前端构建要求](https://github.com/liuzengh/trpc-agent-service/blob/b26c10b33380bc9773b65f31e8c3ce01d0525f23/trpcservice/admin/ui/README.md#L1)；[系统观测边界](https://github.com/liuzengh/trpc-agent-service/blob/b26c10b33380bc9773b65f31e8c3ce01d0525f23/trpcservice/web/console/src/pages.tsx#L1174)；原评审卡：`reviews/pr-25.md`（存于原评审批次）。

### PR #26 · 冻结 HEAD `5b314a3cc124539f592ac17175d01287d05c23b4`

历史页读取的是 `chat_sessions/chat_messages` 业务账本，按 turn 分页；工具细节另在 SDK 事件和审计里。历史读取路由有归属检查，但 SSE 的独立漏洞已复现，应单独保留，不能用普通 CRUD 的授权测试掩盖。

证据：[前端页面路由](https://github.com/liuzengh/trpc-agent-service/blob/5b314a3cc124539f592ac17175d01287d05c23b4/front/src/router/routes.ts#L11)；[聊天与历史切换](https://github.com/liuzengh/trpc-agent-service/blob/5b314a3cc124539f592ac17175d01287d05c23b4/front/src/views/ChatView.vue#L81)；[服务端历史及授权](https://github.com/liuzengh/trpc-agent-service/blob/5b314a3cc124539f592ac17175d01287d05c23b4/trpcservice/app/web/history_handler.go#L24)；[SSE 读取实现](https://github.com/liuzengh/trpc-agent-service/blob/5b314a3cc124539f592ac17175d01287d05c23b4/trpcservice/app/web/chat_handler.go#L142)；[Agent 版本接口](https://github.com/liuzengh/trpc-agent-service/blob/5b314a3cc124539f592ac17175d01287d05c23b4/trpcservice/app/web/agent_handler.go#L52)；[租户配置 CLI](https://github.com/liuzengh/trpc-agent-service/blob/5b314a3cc124539f592ac17175d01287d05c23b4/cmd/configctl/main.go#L47)；原评审卡：`reviews/pr-26.md`（存于原评审批次）。

### PR #27 · 冻结 HEAD `15f1430849d816e4767bc5b4f367da107d3c6d44`

旧 Admin YAML 热更新、可选共享 RuntimeStore、可靠路径 SQL Revision 和公共 WebChat 是不同入口/配置层。v2 控制面有数据库身份授权，但聊天仍信任 URL 中租户与自报 user；历史丢失发生在实际 Runner/Workspace 身份键接线。

证据：[简单聊天页](https://github.com/liuzengh/trpc-agent-service/blob/15f1430849d816e4767bc5b4f367da107d3c6d44/trpcservice/web/index.html#L31)；[服务及公共路由](https://github.com/liuzengh/trpc-agent-service/blob/15f1430849d816e4767bc5b4f367da107d3c6d44/trpcservice/web/web.go#L38)；[旧 Admin 配置热应用](https://github.com/liuzengh/trpc-agent-service/blob/15f1430849d816e4767bc5b4f367da107d3c6d44/trpcservice/admin/admin.go#L123)；[v2 控制 API](https://github.com/liuzengh/trpc-agent-service/blob/15f1430849d816e4767bc5b4f367da107d3c6d44/trpcservice/admin/control_api.go#L54)；原评审卡：`reviews/pr-27.md`（存于原评审批次）。

## 核验记录

- 已核对 PR 集合：3、4、6、8、13、14、15、16、17、18、19、20、21、22、23、24、25、26、27，共 19 个，无重复、无遗漏。
- 已逐个核对冻结 HEAD，并检查受跟踪源码未修改；页面统计按实际交付形态计数，聊天与管理两类允许重叠。
- 本文源码引用已由本机路径转换为固定完整提交 SHA 的 GitHub 链接；已核对文件与行号、表格列数、PR 顺序和覆盖范围。
- 本次没有新增业务通过结论；浏览器可用性、全部页面操作、跨节点配置传播与完整授权矩阵仍需各自运行证据。
