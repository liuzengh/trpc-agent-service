# 只读项目文档 MCP 本地启用记录

日期：2026-09-08。用户要求继续开发，直到需要本人发消息。程序升级到 `0.2.0-rc.6`，控制面 schema 仍为 23。本轮没有发布远端版本或配置开机自启。

## 本轮实现

新增 `docsmcp`，由 Worker/all 进程按配置启动 loopback MCP HTTP 服务，使用 `trpc-mcp-go` 服务端和平台现有 MCP 客户端。唯一工具是 `search_project_docs`，在 Agent 中映射为 `mcp_docs_search_project_docs`。仅检索 10 份固定白名单项目说明的启动快照，返回路径、行号和短摘录；不读取 `.env`、上传附件、业务数据或验证记录，不接受任意路径、URL 或命令。

补齐了随机 Bearer 凭据与鉴权、请求大小/时间/并发限制、JSON 协议门禁、符号链接与文件类型检查、结构化结果脱敏，以及只传播 W3C trace context、不传播 baggage 的 MCP HTTP 链路。服务随 Agent 关闭，不创建额外守护进程。原理和边界见[文档 MCP 说明](../project-docs-mcp.md)。

## 本地配置变化

- `.env` 新增 `TRPC_AGENT_DOCS_MCP_ENABLED=true`、`TRPC_AGENT_DOCS_MCP_ADDR=127.0.0.1:18090`、`TRPC_AGENT_DOCS_MCP_ROOT=.`；`MCP_DOCS_SERVER` 保存随机专用令牌、固定 URL 和只读工具子集，内容不写入本记录。
- 增加 `tutorial-tenant / mcp_server / env://MCP_DOCS_SERVER` 精确 Secret grant，其他凭据、后端和 IM 配置保留。
- 经鉴权 Admin API 创建并发布 `tutorial-docs-mcp-v6`，App version 5 → 6。只增加 MCP 配置、文档工具授权和相应指令，保留模型、Memory、附件工具和危险操作审批规则。
- 仅将此前 Memory 测试所用的 Telegram 私聊迁到 revision 6；事务检查该会话空闲、无未完成回复和不确定工具操作，并记录原子审计 `audit-docs-mcp-repin-trmyRlDV`。没有批量升级其他已有会话、改写 Session 历史或重放旧 Run。

升级前保存了私有 `.env`、旧二进制、旧日志、PostgreSQL dump、Redis RDB 和配置快照，目录为本机 `data/docs-mcp-enable.trmyRlDV/`，0700，敏感文件 0600；备份 SHA-256 和 dump 目录已检查。本轮未对真实业务数据库做恢复，数据库 dump 不包含集群角色与 MinIO 对象。

## 已验证

1. `TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh` 通过：全仓 race/lint/build，独立 PostgreSQL/Redis/Qdrant/MinIO 合约与恢复工具链、离线告警检查。合成测试不读取日常 `.env` 或使用真实聊天。
2. 新增真实 MCP 客户端/服务端互通及并发测试；确定性模型驱动的完整 Runner → MCP HTTP → Tool Journal/审计；父 trace → `mcp.docs.request` → `mcp.docs.search` 和 baggage 排除；独立 Agent 进程的可选服务启动、鉴权和两端口退出。
3. 本机模型服务恢复后，`./check-model.sh` 成功，`glm-5.3-flash` 返回 `OK`。这只验证模型可用，不代表新工具已被真实模型调用。
4. 新 Agent 的 `/readyz` 和文档服务 `/healthz` 正常。MCP 仅监听 `127.0.0.1:18090`；无 Token 请求返回 401；现有公网 Agent `/healthz` 正常，公网 `/mcp` 返回 404。
5. 通过平台真实 MCP 客户端完成 initialize、工具发现和 `Redis Session` 查询，得到文档路径和匹配摘录。该只读探针没有调用模型、发送 IM 或新增业务 Run。
6. 核对数据库 App version、stable revision、测试会话 revision 及保留配置；原 Memory/模型/其他工具与审批策略未被覆盖。独立 Router 仍能使用受限凭据读回原长期记忆及原 MinIO 测试附件，跨 Session 附件读取被拒绝。

这些证据分别属于自动测试、本机协议联通和既有数据持久性检查，不能合并成尚未发生的真实 IM 验收。

## 两次真实请求的结果

15:19:33（Asia/Shanghai）收到首次文档查询，request 为 `req_5a1ef3994f1ddb4b59f31adeeaf43e0b`，trace 为 `4492b36c810ed681efd05ae1b10e4ff4`。本轮使用 revision 6，`mcp_docs_search_project_docs` 实际调用一次且 succeeded；Run 于 15:20:08 completed。Tempo 实际读回了 `execute_tool mcp_docs_search_project_docs` → `mcp.docs.request` → `mcp.docs.search` 的父子关系，以及模型、Session 和回复发送 span。

但本轮回复发送约 10 秒后失败，没有 provider 回执：Outbound 为 dead、attempt 1，part 0 为 unknown，审计为 `reply_delivery_unknown`。旧版只保存 `Telegram delivery outcome unknown`，未保留底层错误类别，不能事后断定是 DNS、TLS 还是响应超时。当时 Agent、模型、MCP、数据库及 Tunnel 均在运行；随后复查 Telegram getMe/getWebhookInfo 正常。这只能说明复查时可用，不能倒推出失败瞬间的具体原因。

15:26:39 的新请求 `req_f047d7d9c7f016eb5ed15a6981f24c26`，trace `367d2b371456b4415c1c9445b2a3e968`，Run completed；回复于 15:27:11 sent，attempt 1，part 0 有 provider 回执，用户确认收到。但是本轮 Tool Journal 为 0 条，trace 也没有 MCP 调用；模型可能使用了同 Session 的既有检索结果。不能把两次请求拼接成“一次请求完整成功”的验收结论。

两条记录都保留，没有重放旧 Run、改写未知投递状态或伪造成功记录。后续 rc.7 补上[发送诊断信息](../telegram-delivery-diagnostics.md)，不改变未知发送的停重试策略。

## 第三次真实请求（同请求完整通过）

15:40:40（Asia/Shanghai）收到 `fencing` 查询，request `req_79f39cf23876ed5363ad99b7f1b26fb6`，trace `d0b046d8e39e74efba17290ca3b9cbb6`，运行程序 rc.7、Agent revision 仍为 `tutorial-docs-mcp-v6`。

- Tool Journal 中有且仅有一次 `mcp_docs_search_project_docs`，15:40:57 执行且 succeeded，无 error_type。
- Run 于 15:41:09 completed；回复 `out_00718aa7838785b81d3dd15d0ffd86e3` 于 15:41:10 sent，attempt 1、part 0 sent 且有 provider 回执。回复包含项目文档路径，用户确认收到。
- Tempo 实际读回同一个 trace，`execute_tool mcp_docs_search_project_docs` → `mcp.docs.request` → `mcp.docs.search` 的 parent span 匹配；同 trace 还包含 Telegram callback、队列、Worker/Runner、模型、Session 读写和 reply.send。

至此，**同一条真实消息的 Telegram → Runner/模型 → 文档 MCP 工具 → Session → Telegram 回复**链路通过，不再依赖前两条请求拼接证据。前两条失败/未调用工具的记录继续保留；这不代表自动检索始终被模型选择，也不代表外部业务 MCP 或向量知识库已经通过。

### 本轮发送内容

在原 Telegram 私聊发送：

```text
这次请实际调用 mcp_docs_search_project_docs，查询新关键词“fencing”，不要复用上一轮检索结果。根据本次返回的文档，用三句话说明失去租约的旧 Worker 为什么不能覆盖新 Worker 的结果，并注明文档路径和行号。
```

本轮已完成上述记录核对，无需为此重复发送。后续复测仍须核对新 request_id 的工具执行和投递事实，不能只凭模型回答正确判断工具被调用。本工具不新增或修改长期记忆。

外部企业业务 MCP、向量语义搜索、stdio、自助任意服务接入和生产多节点容量仍不在此次完成结论中。
