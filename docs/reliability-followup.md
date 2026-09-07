# 后续可靠性与能力补齐

日期：2026-09-07。代码候选 `0.2.0-rc.4`，schema 23；日常实例未升级。

本轮持续开发范围：用户要求先完成明确的可靠性缺口，再继续 Agent MCP 与最小安全附件链路。每项完成后继续下一项，除非用户打断或确需外部操作。

- [x] Session 回填与在线写入协调，非破坏性、可恢复迁移：App/资源 I/O 协调、目标 staging generation、验证后发布别名；失败不删除当前目标，会话 ID 对调用者保持不变。
- [x] 摘要原样复制，事件内容/顺序、摘要正文/水位的完整校验：保留 Event ID/Version/FilterKey，双写只生成一次摘要，采用后端可移植摘要快照。
- [x] Session / Memory 服务器证明与切换保护：migration 021 元数据、完整已登记主体清单校验、写入 epoch 失效、调用方 passed 标记拒绝；旧 fencing token 在 Session 存储入口拒绝。
- [x] IM 分段发送的持久化状态、续发与未知结果处理：migration 022/023、逐段尝试与回执、发送租约保活、按 claim 次数拒绝旧终结写入；未知结果不重发，提供带权限/证据引用的人工对账入口。
- [x] 每个 Run 独立的并发配额租约、续租、过期回收和安全释放：Redis TIME + ZSET、唯一尝试 ID、续租失败取消执行；Worker/HTTP 使用租约 Context；本地、miniredis、独立 Redis ACL 测试通过。
- [x] Agent 侧 MCP：Streamable HTTP、分页发现、工具子集、部署者凭据/只读授权、调用前 schema 检查、框架审批及执行审计、逐调用关闭客户端。
- [x] 最小安全附件：Telegram 文本/PNG/JPEG，默认关闭，队列导入、大小/MIME/图片尺寸约束、不可变 Artifact、同会话 read_attachment；不自动调用模型。

开发不读取或修改日常 `.env`，不重启业务服务、不迁移业务库、不调用真实模型或 IM。测试使用合成输入、HTTP 测试服务器、miniredis 和独立容器。新表、权限、兼容性和已验证范围在各项完成时记录。

并发配额改用 `:quota:concurrent:v2:<tenant>`，不得与旧共享计数器版本混跑。Redis Worker ACL 增加 TIME 和受限 ZSET 操作。已验证活跃租约跨多个 TTL 保持占用、崩溃过期回收、旧释放不能删除新占用、失去租约取消执行、Close 结束续租 goroutine。新版本未部署到日常实例。

## Session / Memory 迁移边界

`resource_sync` 保存已登记主体、epoch、fencing token、staging/当前别名及服务器验证证明，不保存聊天正文。Worker/Jobs 拥有读写权限，Admin 只读。物理 Session 后端仍保存全部 Event、state 和摘要；`_platform:session_summaries_v1` 是保留的可移植摘要键，用户状态更新不允许覆盖它。摘要只调用一次模型，然后复制同一份结果及 boundary。

只有 Redis Coordinator 的持久序列参与跨进程 fencing 比较；Local Coordinator 的进程计数不能在重启后与旧值比较。Local 仍只用于单节点开发。生产变更 Redis prefix 或恢复旧 Redis 快照时，必须连同控制面的 fence 状态制定恢复方案，不能自动清除高水位绕过保护。

所有平台 Session/Memory I/O 与回填通过同一个 tenant/app/resource 协调边界。PostgreSQL 使用 advisory lock；开发内存控制面使用共享、可取消的锁。当前按 app/resource 串行 I/O，保守保障一致性，长回填/摘要可能增加该 app 的等待时间，需纳入容量评估。绕过平台直接操作后端不参与此协议；旧版未登记会话/用户须在迁移时显式列入回填、校验清单，不能用部分样本代表全量。服务器至少要求全部已登记主体通过当前 epoch 的校验，后续写入使证明失效。

Session 回填保留原目标和中断的 staging，恢复时检查已复制前缀再续写，不删除会话重建。别名存放在控制面，因此控制面与数据后端须一起备份。暂不自动清理旧 generation，避免删除仍需回滚的数据；容量要计入这些保留副本。普通读取和列表隐藏未发布 staging。Session/Memory binding 缓存不再因仅切换迁移状态的 version 变化而换成空的 InMemory 后端。

已增加：事件同数不同内容拒绝、摘要只生成一次、原事件 ID 保留、staging 故障不破坏目标、回填与写入互斥、写后旧证明失效、旧 fencing token 拒绝、部分主体清单不能切换，以及独立 PostgreSQL 元数据/权限测试。Memory 校验包含正文、topics、事件时间、参与者和地点，不比较各物理后端自行生成的 LastUpdated。

## 回复重试

`outbound_part` 按 tenant/outbound/part_index 保存 input hash、独立 owner、状态和真实 provider 回执。全部分段由同一固定正文/绑定版本派生；中途配置改变停止继续发送。已确认 sent 的段跨 Sender 重启复用，不再重新发送。Telegram/企业微信发送网络失败或响应不完整归入 unknown，不把连接报错当成远端一定未执行；企业微信不再伪造 provider message ID。

Sender 对整批已 claim 项保活，每条结束立即停止对应续租 goroutine。分段启动和父消息终结都核对当前 claim 次数；进程名称相同也不能让旧尝试覆盖新尝试。原版已经尝试但缺少新协议记录的 SQL 出站行保持 delivery_protocol=0，不能直接自动重发，升级前应排空或人工核对。

`POST /admin/outbound-parts/list` 查询 tenant_id/outbound_id；`POST /admin/outbound-parts/reconcile` 需要 operator 权限、part_index、expected_owner、outcome=sent/not_sent、provider_id 和 evidence_ref。只能核对 unknown/attempting 且父消息当前没有活跃租约的记录。证据引用只保存 hash，不能把模型判断当成“未发送”的证据。PostgreSQL 的分段修正、父消息恢复及审计回执在同一事务执行；没有事实证据时保留 unknown。不会自动读取真实聊天替操作者判断。

已验证：第二段限流后不重发第一段，跨 Sender 续发，成功后回执落库失败不盲目再发，长回复跨租约窗口保活，旧完成不能覆盖人工核对，以及独立 PostgreSQL 分段状态/审计函数合约。以上只使用合成消息。

## Agent MCP 工具

这是 Agent 调用远端业务工具，与企业微信消息 MCP 通道不同。复用与 tRPC-Agent-Go `tool/mcp` 相同的 `trpc-mcp-go` Client，实现框架 `CallableTool`，由 LLMAgent 的 PermissionPolicy、Callbacks、Tool Execution Journal 和审计驱动。未开放本地 stdio 命令和服务器推送订阅。

部署者通过 Secret 提供 JSON，例如 `MCP_DOCS_SERVER`：

```json
{
  "url":"https://mcp.example.invalid/mcp",
  "bearer_token":"<由部署者填写>",
  "allowed_tools":["search"],
  "read_only_tools":["search"],
  "timeout_seconds":20
}
```

给租户增加用途 `mcp_server`、引用 `env://MCP_DOCS_SERVER` 的 Secret grant；只有 Worker 获得该用途。URL 和 Token 不放在租户 revision 中。内网 HTTP 也是部署者明确授权的目标，不接受模型提供的地址；生产应采用 HTTPS 和出口网络策略。禁止跨地址重定向，错误不回显凭据。

租户的 `agent_config` 示例：

```json
{
  "name":"assistant",
  "instruction":"必要时查询资料。",
  "mcp_servers":[
    {"name":"docs","credential_ref":"env://MCP_DOCS_SERVER","tools":["search"]}
  ]
}
```

`tool_policy.allowed_tools` 还必须包含 `mcp_docs_search`。可调用集合是“部署者 allowed_tools ∩ revision tools ∩ tool_policy 白名单”。未由部署者明确列为只读的远端工具默认需要审批，不能靠服务器注解或租户 JSON 绕过。只读授权撤销、目标变更、输入 schema 改变时旧工具拒绝执行，需要发布新 revision。检查不能原子锁定远端升级，远端仍须维护版本契约。

每次调用重新读取授权、建立客户端、检查 schema、执行一次工具并关闭连接；禁止自动重复 `tools/call`，未知结果通过 Journal 阻断盲目重放。发现最多 20 页/1000 个工具，单 Agent 最多 8 个服务器，每项最多 32 个显式工具。参数和返回给模型的结果各最多 64 KiB，HTTP 响应最多 2 MiB，超时最多 60 秒。支持文本/结构化结果，不自动访问结果里的资源 URL。

## 最小安全附件链路

仅 Telegram 已接通下载。`attachments_enabled` 默认为 false；其他 IM 的不支持反馈、企业微信 MCP 媒体隔离保持不变。没有将企业微信 MCP 的媒体 ID 猜测成下载 API。

启用时在 Telegram binding config 设置 `attachments_enabled:true`，并额外为现有 Bot Token 引用增加用途授权：

```json
{"tenant_id":"tutorial-tenant","purpose":"telegram_media","reference":"env://TELEGRAM_BOT_TOKEN"}
```

这只限制平台用途，不会改变上游 Bot Token 自身的权限。该用途只注入 Worker；绑定配置与执行时均检查权限和版本，Gateway 不下载。

链路：验签/群策略 → MediaReference → Inbox/SQL Outbox → 队列 → Worker 导入 → Artifact → 持久化文字回执。Caption 不解析为审批，导入不进入 Runner，也不安排 Session 摘要或 Memory 提取；文件引用参与幂等 hash，不能重用 message_id 更换附件。

下载依照 [Telegram getFile 协议](https://core.telegram.org/bots/api#getfile)，仅使用其返回的相对 file_path 构造官方域名下的请求，拒绝重定向、穿越路径和自定义生产下载地址。平台限制为 **2 MiB**，不相信用户声明的大小/MIME。

支持 UTF-8 文本、PNG/JPEG，校验实际 MIME、编码、图片格式及最多 1600 万像素。拒绝 HTML、压缩包、可执行内容及其他格式，不执行文件或展开压缩包。拒绝 EICAR 测试串不等于完整杀毒引擎，**不能宣称已接入病毒扫描服务**。PDF/Office、图片理解和文件出站发送不属于本次最小链路。

Artifact 名称由 tenant/app/user/session/request 派生，不采用上传文件名作为路径。`SaveArtifactOnce` 在版本锁内检查、比较和写入：重试不增加版本，冲突不覆盖。只返回 `att_...` 编号，没有免鉴权下载链接。

要让模型读取文本，需把 `read_attachment` 加入 revision 工具白名单。该工具从 Invocation 获取 tenant/app/user/session，只读取当前会话附件；文本最多返回 16000 字符并标记截断，图片只返回 MIME/大小。内容标记为不可信数据；读取进入工具权限、模型用量和附件审计链路。未启用该工具时，不会自动把附件内容上传给模型。

## 验证与启用

`827bfe2` 是并发租约修复，`9e00a35` 是迁移和分段回复修复。后续 MCP/附件使用 HTTP fixture 与合成文件验证；完整 `TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh` 已通过全仓 race、lint、build、独立 PostgreSQL/Redis/Qdrant、备份恢复及离线告警检查。

从日常 schema 16 启用需备份、受控应用 **017–023**、更新 SQL/Redis 权限，停止旧 Worker/Jobs/Sender 后再启新版本，不能混跑旧计数器/迁移/发送协议。源数据、staging 和控制面元数据共同保留并备份；不自动清理旧副本。MCP 和附件均未在日常环境配置启用，真实端点、凭据和外部验证另行安排。
