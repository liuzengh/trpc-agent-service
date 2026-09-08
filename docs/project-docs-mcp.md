# 只读项目文档 MCP

rc.6 增加一个可选的本地文档检索 MCP 服务，供 Agent 通过真实 Streamable HTTP/JSON 协议调用。它与企业微信消息 MCP 是两个用途：前者查询公开的项目说明，后者接收和发送企业微信消息。这里不是外部企业知识系统，也不是向量检索。

## 运行链路

```text
Telegram 文本 → Gateway / 队列 → Worker / Runner
  → 租户工具白名单与执行 Journal
  → mcp_docs_search_project_docs
  → MCP initialize / tools/list / tools/call
  → 127.0.0.1:18090/mcp（Bearer 鉴权）
  → 固定白名单文档快照 → 路径、行号和短摘录
  → 模型据此作答 → 持久化回复 → Telegram
```

Agent 侧继续使用平台已有的 MCP 客户端、权限和审计链路，服务端使用 `trpc-mcp-go` 的 Tool/HTTP Handler，不手工伪造成功回包。服务不启动额外进程，而是在启用的 Worker/all 进程中运行独立的 loopback HTTP 监听；随 Agent 优雅关闭。不配置开机自启。

## 配置

默认关闭。部署者在私有 `.env` 设置：

```dotenv
TRPC_AGENT_DOCS_MCP_ENABLED=true
TRPC_AGENT_DOCS_MCP_ADDR=127.0.0.1:18090
TRPC_AGENT_DOCS_MCP_ROOT=.
```

`MCP_DOCS_SERVER` 保存 JSON，含与监听地址一致的 `url`、随机 `bearer_token`、`allowed_tools:["search_project_docs"]`、`read_only_tools:["search_project_docs"]` 及 `timeout_seconds`。密钥不要放进 revision、启动参数或文档。

给租户增加 `purpose=mcp_server / reference=env://MCP_DOCS_SERVER` 精确授权，并发布包含以下内容的新 revision：

```json
{
  "mcp_servers": [
    {"name":"docs","credential_ref":"env://MCP_DOCS_SERVER","tools":["search_project_docs"]}
  ]
}
```

上例是 `agent_config` 的增量字段，不要覆盖原指令和其他配置。工具白名单还需包含 `mcp_docs_search_project_docs`。只读免审批来自部署者显式授权，不是服务端 annotation 自动赋予权限。旧会话固定原 revision，发布不会批量切换它们。

正常使用仍执行 `./start-real.sh`，不需要另开 MCP 终端。Docker 默认镜像不内置整套项目文档；开启此功能时须把受控文档目录只读挂载到配置的 root，否则启动失败。多 Worker 同机部署不能占用同一个监听端口；可只在专用 Worker/pod 启用，或给每个节点配置各自本机服务。

## 文件与协议边界

- 固定文档文件名白名单，无目录递归或模型提供的文件路径；不索引 `.env`、`data`、上传文件、验证记录或聊天内容。`os.Root`、普通文件检查和 inode 比较限制目录穿越、符号链接及打开时替换。
- 启动时构建只读快照；单文件最多 512 KiB、合计最多 2 MiB。修改文档后须重启才能刷新，返回值带快照 hash。
- 查询是 1–8 个短关键词，最多 256 字符；结果最多 5 条，每条摘录最多约 1000 字符。支持 Unicode 文本匹配，但不是语义搜索或自动联网浏览。
- API 仅绑定明确的 loopback IP，拒绝浏览器 Origin、URL query、非 JSON、压缩请求、超大请求和非白名单 RPC；需要 Bearer Token。请求最多 16 KiB，HTTP 限时，有界并发队列。
- 使用 SDK `WithoutSession` 的 JSON POST 模式，不创建会话清理后台协程，不提供 SSE 或服务端推送。协议处理串行化，避免所固定 SDK 在并发 initialize 时修改共享能力映射的竞态。
- 结果按 JSON 字段脱敏，包括嵌入在文本块中的 JSON，不再直接对整段序列化 JSON 替换；保留合法结构并限制深度和总大小。文档摘录是参考数据，不是可执行指令。
- MCP HTTP 跳转只传播 W3C trace context，不携带 baggage。服务端 `mcp.docs.request`、`mcp.docs.search` 可接入现有 Tool trace；日志不记录查询正文、令牌或完整结果。

## 已验证与未验证

代码测试覆盖鉴权、输入限额、禁止文件越界、快照、脱敏、真实 MCP 客户端互通、Runner/工具 Journal、trace 关联以及随 Agent 关闭监听。测试使用合成文件和本地 HTTP 服务，不调用真实模型或 IM。

本地已于 2026-09-08 启用。第三次真实 Telegram 查询 `fencing` 已在同一个 request_id 下确认：工具调用 succeeded、完整跨 MCP HTTP trace、回复 sent 且有 provider 回执，用户确认收到，见[启用记录](validation/project-docs-mcp-2026-09-08.md)。此前发送未知及未重新调用工具的两条记录分别保留，不拼接成成功记录。自建只读文档 MCP 不等于外部企业业务 MCP、任意 MCP 自助接入、stdio、完整资源协议或向量知识库。
