# 治理、安全、审计与监控

平台在 tRPC-Agent-Go 的 ToolFilter、PermissionPolicy、Model Callbacks 和 Runner 上实施租户策略。策略来自不可变 Revision，可信身份来自通道映射；模型文本、工具参数或客户端 tenant_id 不能授予权限。

## 1. 访问控制

`/chat`、`/inbound` 默认不注册（404）。启用后，无有效 Bearer 返回 401，越权返回 403；失败不创建任务、不调用模型。loopback、代理头和难猜 URL 都不能代替身份校验。

本地快捷配置：

```dotenv
TRPC_AGENT_HTTP_API_ENABLED=true
TRPC_AGENT_HTTP_API_TOKEN="自行生成的至少 32 字符随机 Token"
TRPC_AGENT_HTTP_API_PRINCIPALS_JSON=
```

用 `openssl rand -hex 24` 生成值并写入私有配置；该快捷身份只授权 `tutorial-tenant / tutorial-http / alice`。多租户改用 principals 数组，同时清空快捷 Token：

```json
[
  {
    "name": "tenant-a-client",
    "token": "<由部署者注入>",
    "tenant_id": "tenant-a",
    "binding_keys": ["tenant-a-http"],
    "user_ids": ["alice"]
  }
]
```

凭据、Binding、用户均为精确匹配。Gateway 解析实际 Scope 后再次确认租户和 ChannelType=http，禁止 HTTP 冒充 Telegram/企业微信。Token 不接受 query 参数，不复用模型、IM 或 Admin Key。

Admin 是独立入口和 Principal RBAC，支持 superadmin、租户管理及只读审计权限，所有写入仍校验作用域、Secret 引用和 expected_version。生产置于内网/身份代理后；当前没有企业 SSO/OIDC。

## 2. 密钥与节点权限

控制面只存 `secret_ref`。运行时通过 `tenant_id + purpose + reference` 精确 grant 解析 `env://`，默认拒绝。部署者维护 grants；租户不能通过设置 namespace 或任意环境变量名扩大权限。

```json
[
  {"tenant_id":"tenant-a","purpose":"model","reference":"env://TENANT_A_MODEL_KEY"},
  {"tenant_id":"tenant-a","purpose":"telegram_bot","reference":"env://TENANT_A_BOT_TOKEN"},
  {"tenant_id":"tenant-a","purpose":"telegram_webhook","reference":"env://TENANT_A_WEBHOOK_SECRET"}
]
```

真实值在对应环境变量或部署 Secret 中。用途还包括 session、memory、artifact、knowledge、embedding、wecom_callback/aes/app、wecom_mcp_read/send、mcp_server。知识库 Key 和 Embedding Key 分开授权，S3/Embedding 不隐式回退到进程默认凭据。

`POST /admin/resources/list` 使用 `kind=credentials` 和精确 `purpose` 分页查询当前租户的授权引用。此接口不调用 Resolve、不检查变量存在性，也不返回其他租户或其他用途的引用。网页模型与 Embedding 表单使用该清单；配置保存、发布和实际执行仍重复校验授权。

Gateway 获得入站密钥，Sender 获得 IM 出站密钥，Worker/Jobs 获得执行模型与后端密钥；Relay 不解析模型或 IM 凭据。all 是本地组合权限，不代表进程级隔离。上游企业微信 MCP URL 可能同时具备读写权限，平台用途分离不等于上游签发了独立 Token。

分角色策略生成命令只输出配置，不连接数据库或执行授权：

```bash
./bin/trpc-permissions -format sql -schema agent_platform -role-prefix trpc
./bin/trpc-permissions -format redis-acl -role-prefix trpc -redis-prefix trpc-agent-production
```

SQL 生成 NOLOGIN 最小职责角色，迁移、运行和管理身份分离；审计 append/prune/策略更新通过受限函数。部署者另建登录账号。共享角色不是租户 RLS，业务查询和 Storage Scope 校验仍必需。Redis 同时约束命令和 key prefix，禁止 FLUSH、CONFIG、全局扫描和任意键授权，Lua 也受 ACL 约束。

Kubernetes 的分角色 Secret、NetworkPolicy 和依赖标签要在实际集群配置验证；模板不是域名防火墙。Secret 引用不能代替网络出口/SSRF 控制。当前没有 Vault/KMS、Workload Identity 或在线撤销，修改配置需更新相关进程，紧急撤销还应在供应商侧执行。

## 3. 工具、审批与业务幂等

ToolFilter 控制模型可见性，PermissionPolicy 控制执行；工具自身和 MCP 包装器还校验可信 Scope。白名单、用户限制、调用次数和总执行时间随 Revision 固定。

可在 tool_policy 配置 `tool_allowed_users` 和 `direct_only_tools`，用户键使用运行时映射后的身份，不使用昵称。缺少可信受众时仅私聊工具拒绝执行。Memory 可用 `direct_only=true` 防止个人事实被群聊读取。

危险工具返回 ask，平台持久化 `tool_approval`，回复严格的文本命令：

```text
批准 apr_xxx
拒绝 apr_xxx
```

审批绑定租户、通道、用户、会话、工具、参数哈希和有效期；确认后再次检查权限。只收到模型“已取消/已批准”的文字不算平台决策，平台控制命令才改变状态。重复确认返回平台回执，不重复模型调用或业务写入。

Tool Execution Journal 使用 request_id/tool_call_id 和参数哈希记录授权后执行事实。外部超时不等于未发生副作用，未知结果不能自动重放。

托管写操作另有稳定 operation ID 和业务幂等键，`OperationProvider` 负责执行/查询；内置本地工作项示范批准后单次写入。对账只查询后端事实，不调用 Execute，也不允许用户填写成功状态。不存在记录不证明远端没有在途请求；无可靠幂等契约的后端不允许盲目重试。

管理员通过 tool-executions/list、tool-operations/list/get/reconcile 查询，对账后 HTTP 200 仍可能表示业务 unknown。详见 [Admin 入口](operations-runbook.md#3-httpadmin-和-im)与 [幂等策略](data-consistency.md)。

## 4. Agent MCP 与只读文档

租户通过 agent_config 的 mcp_servers 声明已授权服务引用和工具；实际工具名加入白名单。部署者固定 URL、认证和 schema 边界，不能由模型连接任意地址，也不能依据服务端 annotation 自动授予免审批权限。

可选的内置文档服务配置：

```dotenv
TRPC_AGENT_DOCS_MCP_ENABLED=true
TRPC_AGENT_DOCS_MCP_ADDR=127.0.0.1:18090
TRPC_AGENT_DOCS_MCP_ROOT=.
```

`MCP_DOCS_SERVER` 为私有 JSON，包含 url、bearer_token、allowed_tools/read_only_tools 和 timeout_seconds。租户必须具有 `purpose=mcp_server / reference=env://MCP_DOCS_SERVER` grant，Revision 声明：

```json
{"mcp_servers":[{"name":"docs","credential_ref":"env://MCP_DOCS_SERVER","tools":["search_project_docs"]}]}
```

工具白名单还需包含 `mcp_docs_search_project_docs`。服务运行在 Worker/all 的 loopback 监听，随进程关闭；Docker 启用时须只读挂载受控文档目录。同主机多个 Worker 不应占用同一端口。

这是固定正式文档的关键词快照检索，不是向量知识库或外部业务 MCP。白名单无递归或调用方文件路径，拒绝符号链接/越界，单文件 512 KiB、总计 2 MiB；仅支持受鉴权 JSON POST，结果最多 5 条。没有 SSE、stdio 或任意目录访问，不读取 `.env`、运行数据和会话。文档修改后重启刷新快照，摘录始终作为不可信参考数据而非执行指令。

## 5. 模型预算与 Guardrail

`modelops.Model` 包装前台和后台模型/Embedding 调用。每次请求前以输入字节、工具 schema 和最大输出估算预留；Redis 原子检查余额并预扣，按调用 ID 结算。不能只在 Runner 开始时检查一次预算。

model_config 支持 max_prompt_tokens、max_completion_tokens、timeout_seconds 和输入/输出每百万 token 价格。价格由部署者维护，不代表供应商账单。金额预算启用时必须配置有效价格；本地免费模型可只用 token 限额。

流式累计 usage 只结算一次；缺少可信 usage、超时或取消保守保留预扣。超额记账后阻止后续调用，结算失败不自动退款。按 UTC 日期归账，Redis 明细和日汇总保留七天；这是运行准入账本，不是无限保留的财务系统。供应商隐藏 token、计费和输出上限行为仍需供应商侧硬额度与对账。

Model Callbacks 支持 max_input_chars、blocked_input_patterns 和 redact_output_patterns。BeforeModel 阻断输入，AfterModel 克隆并脱敏响应，不修改共享对象；Go RE2 降低正则回溯风险。更复杂的企业 DLP 需另行接入。

## 6. 审计策略

每条审计至少关联 tenant_id、channel、user_id、session_id、agent_name、tool_name、decision、latency、error_type、cost、trace_id，以及 request_id、资源版本和必要的结果哈希。不记录模型/IM/数据库凭据或原始业务参数。

`/admin/releases/list` 按租户/应用与时间游标返回真实发布、回滚、灰度审计，不用版本创建时间代替操作时间。`/admin/jobs/list` 和运行详情按应用及源请求关联后台任务；元数据查询不会读取任务中的文档内容或触发重试。无 trace 时仍可通过 request_id 查询对应决策。

```json
{"level":"basic","retention_days":0,"failure_mode":"fail_closed"}
```

- basic 保留事件和安全必要字段；full 保留脱敏详情；security 只保留安全、管理、工具、失败等必要事件，不能关闭这些事件。
- retention_days=0 不自动删除；显式保留期按小批事务清理，并同时写清理回执。
- fail_closed 写失败即失败；它不意味着已经发生的外部操作回滚。
- buffered 只在节点私有持久目录落盘并 fsync 后确认成功，容量满或写失败仍拒绝，稳定 audit ID 用于幂等重放。

缓冲由 TRPC_AGENT_AUDIT_SPOOL_DIR 开启，每进程独占目录，权限 0700，文件 0600；默认上限 10000 条，单条 256 KiB。不要共享目录或把临时容器层当持久卷。策略缓存有时限，未知/过期策略拒绝，不静默采用宽松默认值。

标准日志、审计、trace 和 MCP 返回 JSON 分别进行结构化脱敏。不要打印整个配置、原始下游错误体、请求头或 URL query；普通源码路径和哈希也不能代替敏感内容治理。

## 7. 监控与 trace

OTLP 串联入口 callback/poll、Gateway、持久化任务、Worker/Runner、模型、Tool/MCP、Session/Memory/Knowledge 和 Sender。队列/审批跨请求保存 W3C trace context 或 span link，不能把 request_id 当 trace_id。对外 MCP 只传播必要 trace context，不传播 baggage。

指标包括请求量、模型与工具耗时、存储延迟、错误、Token/成本、预算结算、IM 投递、后台任务、接收检查点和队列积压。高基数用户/会话/请求放 trace 或审计，不作通用指标 label。

PostgreSQL 的 platform_backlog 聚合视图由 Gateway/Admin 读取，只含租户、阶段、状态、数量和最老年龄。queue 阶段是 SQL Outbox，不是 Redis PEL。多个观测节点看同一数据，按维度取 max，不求和。

agent_backlog_snapshot_up=0 表示采集失败，不能导出假的零积压；timestamp 用于识别过期快照。无新消息时接收检查点也应推进。unknown/attempting 计入失败/待核对，不能当发送成功。

Prometheus 规则、测试和 Grafana 配置见 [deploy/compose](../deploy/compose/prometheus-rules.yaml)。阈值是部署初始值，实际通知接收方、静默与 SLO 需另行配置；规则评估成功不代表已经有人收到通知。

## 8. 管理页面与 Skill 沙箱

管理工作台位于 `/admin/ui/`，静态资源不带租户配置。浏览器首次使用已有 Admin Token 登录，之后使用最长 8 小时的 HttpOnly/SameSite Cookie；服务端只保存随机会话凭据摘要，长期 Token 不进入 localStorage。修改请求同时检查 CSRF 与 Origin，注销、到期或 Principal/Token 配置变化后旧会话失效。远程登录要求 HTTPS，本机回环 HTTP 仅用于开发。已有 Bearer API 保留，不通过伪造 Cookie 绕过其鉴权。租户、角色和资源归属都在服务器再次检查。

页面使用 React/TypeScript 与同源 CSP，不渲染不可信 HTML。Ant Design 动态样式通过当前页面 nonce 授权，样式属性用于组件布局；没有开放内联脚本或 eval。长期登录凭据、模型 Key、数据库密码、回调密钥均不通过工作台 API 返回。

版本编辑页面提供“检查配置”，对应 `POST /admin/revisions/validate`，请求体为 AgentRevision 配置（预检不要求生成版本 ID/序号）。接口要求当前租户的写权限，只读取控制面元数据和部署者授权，不保存版本、不解析密钥值、不连接模型/MCP/数据后端，也不启动沙箱。

返回 `check_id`、`valid`、`issues`、`runtime_status`、`dependencies`；每个问题包含 `code`、`field`、`severity`、`message`、`suggestion`。创建、发布和灰度使用同一校验服务。写入被阻止时保留 HTTP 400/403 的兼容语义，并返回 `code=configuration_invalid` 和完整 `validation`，不回显含敏感输入的底层解析错误。

启用 skill_run 后，skill_load 必须在白名单中；“加载后执行”至少需要两次工具调用，正数 `max_tool_calls` 小于 2 时禁止新建/发布该执行配置。`0` 保持不限次数的含义，并返回警告；平台不会自动把 1 改为 4。只加载说明、不启用 skill_run 的只读配置不要求沙箱或两次额度。旧版本不被原地修改。

`valid=true` 不等于真实模型联调成功。依赖只接受执行节点提供的有时效观测；过期、未来时间或非 Worker 来源按 `unknown` 处理。目前内置启动装配仅能明确报告本地 Worker 沙箱未启用，远端 Worker、模型和后端连通性未观测时保持未知，不能把 Admin 节点自身可用当作所有 Worker 就绪。主动模型检查仍需显式运行模型检查命令。

权限审计详情新增 `code`、`calls_used` 和 `call_limit`。运行中耗尽工具次数时，Runtime 使用固定平台反馈替换误导性的模型解释，包含 `tool_budget_exceeded` 和 request_id；缓存回复保证重复投递不重做。沙箱未启用的已批准调用记录为 `failed / sandbox_unavailable`（明确未开始执行）；其他沙箱失败仍保留未知结果边界，不因为错误分类就允许重放。审批的结构化类别为 `approval_required`，批准与执行成功继续分开。

网页调试使用独立快照、队列、审批和 Journal 表，不创建虚假的 IM 绑定或可发布版本，也不放宽原 IM 表的外键。只有 Engine 注入的内部调试上下文才能解析快照和选择调试仓储；请求中的 tenant/user/session 不能直接成为可信身份。浏览器只能操作自己发起的调试会话，审计员不能因能看元数据就执行模型或读取调试正文。

调试仍经过同一 Runtime/Runner、配额、工具白名单、审批参数哈希及沙箱。批准只消费一次对应授权；同批请求全部批准后才创建一次继续任务，拒绝不会创建继续任务。MCP 只读资格来自部署者的凭据配置，不相信上游 annotation；其他外部写工具在网页调试中关闭。模型输出不能替代工具 Journal 的执行事实。

调试数据使用服务端生成的独立 UserID/SessionID，保留所属 tenant/app 的存储路由但不读取业务用户的 Session/Memory。自动记忆和摘要任务不在调试中运行。7 天保留期后先删除对应原生 Session/Memory，再清理控制台记录；失败保留目标等待重试。状态流重连只读取持久状态，不重发消息；节点中断后的未知结果不会自动重放。

Skill 由部署者在 skills root 的 catalog.json 注册 name/version/directory；每个目录加载 SKILL.md 与 run.sh。框架负责 Markdown 解析和 skill_load，平台冻结正文与脚本快照，并校验租户 grant 和 Revision 中的 name/version/checksum。部署目录变化不会偷偷改变已编译代码；新内容要发布新引用，旧版本应保留以支持回滚。

skill_run 是固定平台工具，不能由请求指定 shell 命令、宿主路径、镜像或 Docker 参数。即使租户省略 dangerous_tools 也要求审批；执行前再次核对 Tool Journal 的授权状态和参数哈希。未知结果保留原有防重放规则，不直接重跑脚本。

Docker 执行使用本地镜像的不可变 ID，不自动拉取；默认非 root、禁网、只读根、全部 capability 丢弃、no-new-privileges，工作区为独立 16 MiB tmpfs。默认 128 MiB 内存、32 个 PID、0.5 CPU、10 秒脚本时长、64 KiB 合并输出，每节点最多 2 个沙箱执行。超时在容器内执行，Worker 中断后也有退出边界；取消或超限会按随机名称和所有权标签清理本次容器，不做全局 prune。

这不是虚拟机或可证明抵抗所有内核漏洞的隔离。Docker daemon 及镜像由可信部署者管理，生产应使用专用执行节点/受控 rootless daemon，不能将 socket 暴露给租户或挂进沙箱。创建后尚未启动便发生硬崩溃的容器元数据可能需人工核对；不自动删除归属不明资源。[Docker 安全边界](https://docs.docker.com/engine/security/)

当前不支持交互终端、联网安装依赖、宿主机执行或任意文件挂载。脚本结果经 Tool/Session 返回，审计保存调用身份、状态和摘要，不保存脚本正文或完整输出。
