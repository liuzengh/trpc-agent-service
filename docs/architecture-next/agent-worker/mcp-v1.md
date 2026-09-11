# 普通 MCP 工具 V1

## 边界与配置链

本包仍使用单个 LLM root、一个 SDK Runner、一个平台 Run 和正式 Session。
复用 SDK v1.11.2 的 `tool/mcp`，没有新增工具注册 DSL、队列、累计预算、隐式
输出上限、自动重试或补偿任务。Sequence、Parallel + 显式汇总、Loop 属于后续编排包。

配置沿用已有字段：

- AgentSpec `requirements.tools.<slot>.capability` 声明能力，节点 `tool_slots` 显式选择。
- Profile `tools.<slot>` 固定 `mcp_streamable_http`、`server_url`、`toolset_name`、
  `tool_name`、`auth.kind` 和 `capability`。
- `auth.kind` 仅为 `none` 或 `bearer`；Bearer 使用既有只写凭据动作
  `credentials.tools.<slot>.bearer_token`，发布后绑定固定 credential use。
- Capability 与 AgentSpec 复用 `^[a-z][a-z0-9_.-]{0,127}$`，编译精确匹配，保留
  `web.search`；它不是服务端能力证明，也不产生额外授权。
- Toolset / remote tool 名称保持既有 Profile `^[a-z][a-z0-9_]{0,63}$` 约束。
- 不暴露整个 ToolSet；每个资源只选取声明的一个工具，未选资源不进入 Manifest 闭包。

Control 新发布合同启用既有 `mcp-web-search-v1` adapter ID。此名称是历史命名，
实现匹配 MCP transport kind，而非限制只能搜索网页。历史 platform-v1 构造器及其
固定 digest 不变；Worker 新发布合同 digest 改变。部署时应按实际固定 hosts 重算
Control/Worker 相同 release pin，不改写历史 Manifest，不自动切换共享部署。

## Worker 装配

Reader 只从已校验的不可变 Manifest 构造 `Plan.Tools`。Factory 在同一次固定凭据
批次中解析 Bearer（完整 use 相同则去重），none 不读取凭据，也不回退环境变量。
SDK MCP 初始化并进行实际 `tools/list`，只捕获目标 callable；不再调用会隐式刷新
和缓存回退的 `Tools()`。每个 Attempt 持有并关闭自己创建的 MCP service。

Runner 内使用 `llmagent.WithTools` 加入已选普通工具；Memory / Artifact / Knowledge
仍沿用各自显式配置，不因启用普通 MCP 自动获得它们。

内部工具 alias 为 `mcp_<resource>`，避免不同服务器同名工具互相覆盖。
Provider 名称沿用既有合同：`fn_` + `sha256("tools/" + resource)` 的前 60 个十六进制字符。
公开 SDK model wrapper 在声明、历史请求和完整/分片响应边界做双向映射，Session
保存内部稳定名称。Knowledge 复用同一映射，Memory / Artifact 原工具名保持不变。

## Schema 与错误语义

MCP schema 来自本次真实 discovery，**不是发布时冻结的 Manifest schema**。
没有增加 schema snapshot/version 平台。

SDK converter 会遗漏部分约束。薄适配从同一次 `tools/list` 响应观察输入 schema，
以 public `tool.Schema` 保留可无损表示的声明（包括 `additionalProperties: false`），
并在调用前验证参数。不能无损表达的约束明确返回 schema 错误，不悄悄丢弃；外部
`$ref` 解析关闭。实际调用仍委托 SDK callable，不重新实现 MCP 执行协议。
分页 discovery 当前明确拒绝，而不是在不完整列表中静默选错。

- 参数错误、MCP `IsError` 业务结果回到模型，允许现有 SDK 纠正循环。
- 网络、认证、协议故障取消当前 SDK run，沿现有依赖/凭据/执行失败分类处理。
  不把发生基础设施故障后的文本接受为成功 Final。
- 使用已有 Run 截止时间和已发布 `max_tool_calls`；无新增隐式 per-tool policy。
- 固定 HTTP(S) endpoint、none/bearer、无重定向、无环境代理回退；HTTPS 校验证书。
- 一个 Runner 最终返回一个 Result，随后沿既有 Stage → Complete 接受 Session 和
  单个逻辑 Final。工具回合不是新的平台 Run 或独立 Final。

## 验收入口

```sh
GOCACHE=/tmp/trpc-worker-go-cache go test -race \
  ./services/agent-worker/internal/execution/adapter/outbound/mcptoolset \
  ./services/agent-worker/internal/execution/adapter/outbound/trpcagent \
  ./services/agent-worker/internal/execution/adapter/outbound/runtimeadapter \
  ./services/agent-worker/internal/execution/adapter/outbound/manifestadapter

python3 -B scripts/test-worker-mcp-joint.py --race --artifacts /tmp/mcp-joint
python3 -B scripts/test-worker-mcp-joint.py --race --live \
  --env-file /absolute/path/to/.env --artifacts /tmp/mcp-live
```

Adapter tests 使用真实 `trpc-mcp-go` Streamable HTTP server/client；Executor tests
使用实际 SDK Runner 和 HTTP/SSE 模型 fixture，工具依赖可控；两层证据不混称。
Joint 使用真实 Control HTTP 发布、Gateway/Worker/PG/NATS、MCP server 与 Channel Lab。
默认 2 Run 覆盖正常调用及 IsError 纠正，实际读取正式 Session/head/Route、Delivery 和
逐字 Final；live 模式 1 Run 验证真实 DeepSeek 选择与使用真实 MCP 返回，不声称 live
故障纠正、真实 Telegram 或 Embedding 验收。

GUI 沿用 `scripts/test-worker-memory-web.py --scenario mcp` 的人工浏览器交接模式：
真实编辑 Agent/Profile Capability 为 `mcp.search` 并发布，再验收唯一 selected resource、
新版本路由和实际调用。保存后重开 Profile，凭据保持 keep、输入为空、页面不回填原值。

## 本包已观察结果（2026-09-09）

- 四个 Worker 装配包 `-race` 通过，完整 Go 113 个测试 package 通过。
- Profile/Agent Web 定向 4 files / 62 tests 与 TypeScript lint 通过；GUI guards 13 项通过。
- 确定性 HTTP 联合 2 Run / 每轮 1 Attempt，实际 selected MCP 调用 3 次，包含
  `correct-me` → IsError → `orchid` 纠正。下一轮实际模型请求含前轮已接受 Final。
- 真实 DeepSeek 联合 1 Run / 1 Attempt，两次完整 HTTP 200，实际工具调用 1 次，
  Final 使用服务端 canary 与时间。Provider 响应 usage 合计 1726，不作计费推断。
- 实际浏览器完成 Agent v2 / Profile r2 / Deployment r2，双侧 `mcp.search`，
  重开 Profile 的 Bearer 输入为空且不回显原值。该页面发布的同一个 Manifest 实际
  完成 MCP → 正式 Session/Completion → Gateway ACCEPTED → Channel Lab，清理成功。
- 联合直接核对 provider schema 的 object、query string 和 required。完整 discovery
  schema 保真由真实 adapter tests 验证，其中 additionalProperties:false、required、
  object / string 类型与发现值保持一致，额外参数在网络调用前被拒绝；未保存联合原始完整 schema。
- fixture 与 live 之间追加共享 Bearer use 去重及 resource 名称验证，故 Worker 二进制
  不同；live 和 GUI 使用后续源码。单工具 fixture 不覆盖多资源共享 use，后者由
  定向装配测试验证。原二进制相等读回失败记录保留，不声称三个门禁二进制全相同。
- 最初 fixture 的脚手架 record 方法遗漏已修复；原失败在发布/调用之前，记录保留。
  没有为获得绿色结果放宽单 Attempt、正式接受、原文匹配或清理门槛。
