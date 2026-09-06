# 企业微信 MCP：接入平台后的运行链路

## 当前状态

`wecom_mcp` 已接入租户路由、消息队列、tRPC-Agent-Go Runner 和回复 Sender，完整链路通过自动测试，检查点与发送状态通过独立 PostgreSQL 集成测试。

真实测试群的读取和单次发送已验证。随后完成迁移、绑定和进程升级；用户已发送新消息并收到模型自动回复，PostgreSQL 状态及 Tempo 完整 trace 均已核对，**开发环境的真实群文本 Agent 链路通过**。这不等同于媒体、所有异常和生产容量验证。操作、备份和实证见[启用记录](validation/wecom-activation-2026-09-06.md)。

## 1. 消息如何运行

```text
Gateway 进程中的 WeComPoller（默认关闭）
  → 读取明确登记的 tenant_id / binding_id
  → 检查租户、应用、Binding 状态及群/人类成员白名单
  → 获取群级租约、读取持久化检查点
  → MCP chat_messages_list：读取完整窗口的分页
  → 校验消息、过滤 @、计算指纹、按发送时间排序
  → 共用 CallbackGateway 的配额与审批处理
  → Inbox + AgentRun + Queue Outbox
  → Relay → 工作队列 → Worker
  → 租户 Revision Compiler → Runner → LLMAgent → Model
  → Session Event / AgentRun 完成 → Outbound
  → Sender → wecom_mcp Adapter → message_aibot_send
```

这里的 MCP 是通道传输协议，不是开放给模型自由调用的工具集。运行时不列举最近会话、不下载文件、不使用通用身份的 `message_send`、不订阅 GET/SSE。原来的 `wecom` 自建应用回调和 Telegram 保持独立。

只处理白名单成员在指定群内按配置 @ 前缀发送的文本。成员白名单只能填写确认过的人类账号，不能包含机器人。身份取自 `userid`，不使用成员姓名，也不将 `extra_identity_context` 放入 prompt。

`mention_style` 默认 `whitespace`，要求完整前缀后有空格或换行，因此 `@机器人名称-其他` 不匹配。真实样例中，机器人显示名含内部空格，正文又可以直接紧接名称，所以额外支持显式 `prefix` 模式，按完整显示名前缀截取正文；本机测试绑定使用此模式。它只是文本匹配，不是企业微信实体级的 @ 证明，同名前缀可能误匹配，必须和群/人类成员白名单共同使用。默认严格模式没有被全局放宽。

原有 `RuntimeIdentity` 将成员/群映射为内部会话标识。Session 键仍是租户/应用 Storage Scope + 用户 + 会话，因此同群不同成员当前也有各自的历史，不是全员共享上下文。入队前会再次检查 Binding 版本和状态。原来的审批、工具权限、预算及业务幂等继续生效。

## 2. 检查点与去重保证

默认每 10 秒扫描一次登记的目标，单群单次操作超时 45 秒；最多前进 60 秒、重叠读取前 60 秒，单次窗口不超过 120 秒。结束时刻落后当前时间 5 秒。首次从显式 `start_at` 开始，不默认补拉七天历史。

每个窗口最多 20 页、1000 条源记录；单次 HTTP 响应最多 2 MiB。先读完整窗口，再按发送时间排序后接收。声称还有下一页却没有游标、游标重复、结构不符或超限，都会报错并保留检查点，不能编造分页或跳过失败窗口。同秒消息没有可靠的源排序，只能按指纹作确定性排序。

真实接口样例没有唯一消息 ID，因此必须显式填写 `dedupe_mode: fingerprint-v1`。指纹包含租户、绑定、群、发送者、秒级时间、消息类型和原始文本，不包含可变成员名称。

**同一个人在同一秒发送两次完全相同文本，会合并为一次。** 这是需要明确接受的降级语义，不是服务端消息 ID，也不是端到端 exactly-once。不同成员、群、绑定的文本不会因此混在一起。

- Inbox 成功但 seen 未写入：崩溃后重放同一指纹，由 Inbox 唯一键去重。
- seen 成功但检查点未推进：重启后跳过已确认记录，继续窗口。
- 节点失去租约：取消上下文；检查点版本 CAS 也阻止旧节点覆盖较新进度。

多节点需共享 PostgreSQL 状态与 Redis Coordinator。InMemory 只用于单进程演示。窗口重叠只能补偿有限延迟；更晚才可见的消息可能漏读。检查点超过源保留期七天时会停止并审计，不静默跳到当前时间。真实分页及延迟可见性契约仍需验证。

## 3. 发送失败怎样处理

Sender 按最多 4000 个 Unicode 字符拆分回复，每段最坏 16000 UTF-8 字节，低于接口声明的 20480 字节上限。每段稳定的 OutboundID 对应共享尝试记录：

| 状态 | 再处理相同 ID 时 |
| --- | --- |
| 不存在 | 原子创建 `attempting` 后才联网 |
| `sent` | 返回已有结果，不重发 |
| `attempting` / `unknown` | 可能已经送达，禁止自动重发 |
| `rejected` | 终止当前自动尝试 |
| 同 ID 不同正文/目标 | 输入冲突，要求核对 |

只有 MCP 非错误结果且业务字段同时为 `errcode=0`、`success=true` 才记录成功。接口没有返回消息 ID，所以 `ProviderMessageID` 留空。

超时、连接中断、发送后崩溃、回执落库失败都可能留下未知状态。Outbound 停止自动重试，并记录 `reply_delivery_unknown` 审计；发送尝试表用于排查。即使在实际联网前崩溃也可能需要人工核对，这是避免重复通知的保守取舍。多段回复失败时，前几段可能已送达，不能把整条标为成功。

不能通过“消息列表回读为空”判定发送失败：此前实际测试已经出现群里收到、回读仍为空的情况。当前没有自动对账或强制重发 API，不能删除尝试记录来解锁，也不能没有证据就改成成功。

## 4. 配置与启用

以下步骤是后续启用说明，不代表开发时已经执行。先确认专用测试群、允许的成员，以及是否接受指纹去重语义。

URL 继续只保存在本地 `.env` 或 Secret Manager：

```dotenv
WECOM_MCP_URL="企业微信提供的完整 HTTPS MCP 地址"
# 默认 []：仅有 URL 或 Binding 不会启动拉取。仅 Gateway/all 使用。
TRPC_AGENT_WECOM_MCP_TARGETS_JSON=[]
```

在原有 `TRPC_AGENT_SECRET_GRANTS_JSON` 数组中追加两项，**不要覆盖 Telegram 等已有授权**：

```json
[
  {"tenant_id":"tutorial-tenant","purpose":"wecom_mcp_read","reference":"env://WECOM_MCP_URL"},
  {"tenant_id":"tutorial-tenant","purpose":"wecom_mcp_send","reference":"env://WECOM_MCP_URL"}
]
```

Gateway 只获得 read 用途，Sender 只获得 send 用途；Admin 只检查授权关系，不需要密钥值。但上游 URL 本身可能同时有读写权限，这两个用途只是平台的软件访问控制，不代表企业微信签发了两种权限 Token。Worker、模型工具和不受信任进程不应取得这个凭据。

使用既有 `POST /admin/channel-bindings` 管理入口创建 Binding，先保持 `disabled`。下列 ID、名称和起点必须替换为确认过的值；`start_at` 填准备启用的时刻，格式为 RFC3339、精确到秒：

```json
{
  "channel_binding_id":"wecom-mcp-tutorial",
  "tenant_id":"tutorial-tenant",
  "app_id":"tutorial-app",
  "channel_type":"wecom_mcp",
  "account_id":"tutorial-mcp-bot",
  "callback_key":"wecom-mcp-tutorial-route",
  "secret_ref":"env://WECOM_MCP_URL",
  "status":"disabled",
  "config":{
    "allowed_chat_ids":["替换为群chat_id"],
    "allowed_user_ids":["替换为你的userid"],
    "mention_prefix":"@替换为机器人名称",
    "mention_style":"whitespace",
    "timezone":"Asia/Shanghai",
    "start_at":"2026-09-06T18:00:00+08:00",
    "dedupe_mode":"fingerprint-v1"
  }
}
```

`callback_key` 只是内部路由键，不需要注册 webhook 或配置 Cloudflare 公网 URL。Binding 不接受额外 URL/API Key 字段，只保存密钥引用。

需要兼容客户端直接将正文接在 @ 名称后的格式时，经样例确认后把 `mention_style` 设为 `prefix`。显示名可包含内部空格，但不能有控制字符或首尾空白。

PostgreSQL 需按既有迁移流程应用 `014_channel_poll.sql`，增加绑定的租户复合唯一约束及三张表：

- `channel_poll_checkpoint`：租户、绑定、群指纹、配置指纹、窗口进度、版本。
- `channel_poll_seen`：已进入平台的消息指纹，不保存原文。
- `channel_delivery_attempt`：租户、绑定、OutboundID、输入指纹、发送状态。

准备好后，用 `POST /admin/channel-bindings/update` 更新 Binding 为 `active`，提交 `tenant_id`、`binding_id`、完整 `config`、`status` 和当前 `expected_version`，并配置：

```dotenv
TRPC_AGENT_WECOM_MCP_TARGETS_JSON='[{"tenant_id":"tutorial-tenant","binding_id":"wecom-mcp-tutorial"}]'
```

重启启用了 Gateway 的进程后才开始拉取；Worker/Sender 沿用既有队列和模型配置。分角色部署需同时更新代码、grant 和数据库权限。清空目标列表并重启只停止拉取，不取消已入队任务；暂停接收和发送应先禁用 Binding，已发出的在途请求无法撤回。

范围配置改变仍不会自动重置进度。新增受控检查点查询/恢复 API，必须先禁用 Binding、提供版本和明确起点；跳过历史或采纳配置变化必须显式确认。恢复保留全部去重和发送记录，详见[异常与恢复](channel-recovery.md)。

## 5. 监控与当前限制

完整 trace 串起 `wecom_mcp.poll/read`、Gateway、队列、Worker、Runner、Session event、`reply.send` 和 `wecom_mcp.send`。不记录 URL、原始聊天内容或成员姓名。新增 `agent.channel.polls`、`agent.channel.checkpoint_lag`；`channel_poll_failed` 审计区分源格式、配置冲突、保留期等错误，未知发送单独分类。真实告警通知尚未配置。

目前只支持群文本。新异常隔离代码会把单条媒体/格式问题保存为不含原文的隔离记录，继续处理正常文本；整页或分页异常仍保留检查点。隔离记录不是图片理解或媒体回执能力，不下载文件、不进入模型。私聊、卡片、文件发送和编辑语义未开放；真实媒体联调尚未完成。

一个进程内按绑定/群有界串行轮询，慢群会影响其他群，需要配置目标分片并观察延迟。seen 和发送记录暂无自动清理，需评估增长和备份，不能随意删去重状态。正式数据库账号权限、Redis ACL、容量压测与完整恢复流程仍需完善。

## 6. 测试与证据

```bash
go test -race ./...
./lint.sh
go build ./...
```

- `gateway/wecom_flow_test.go`：MCP → 队列 → 真实 Runner/LLMAgent/Session → Sender 及同一 trace。
- `gateway/wecom_poll_test.go`：失败窗口、重启、重叠、取消、禁用和审批入口。
- `wecommcp/adapter_test.go`：分页、白名单、指纹、租户/用途授权、成功重放与未知发送。
- `wecommcp/state_postgres_test.go`：通过 `TEST_POSTGRES_URL` 在随机隔离 schema 中测试迁移、CAS、并发独占和跨实例持久化，只清理测试 schema。

外部 MCP 服务和模型输出使用替身；平台执行组件是真实实现。真实接口证据与本轮开发证据分别见[取样记录](validation/wecom-sample-2026-09-06.md)和[平台开发记录](validation/wecom-runtime-2026-09-06.md)。

## 7. 有界配置辅助命令

`go run ./cmd/trpc-wecomsetup` 是本机操作者命令，不是对租户公开的 HTTP 接口。`-mode prepare -authorized` 根据新会话快照、既有发送尝试指纹和明确的历史测试时间，定位已经确认的群；只取该时间前后两分钟，用测试标记提取唯一成员及 @ 前缀，生成 `0600` 的私有 Binding JSON，不在输出中展示 ID、姓名或凭据。已有带群指纹的消息快照可通过 `-messages-file` 复用，不重复联网。

`-mode apply -authorized -binding-file <私有文件>` 从 `.env` 读取 Admin 凭据，只向 loopback Admin API 提交该教学 Binding。已有相同配置时只确认，不重复创建；已有不同配置时拒绝覆盖。不能向公网发送 Admin Token，禁止重定向。命令不修改 `.env`、运行迁移或重启服务，这些是操作者另外执行的步骤。对照[启用记录](validation/wecom-activation-2026-09-06.md)理解本次实际操作，不必重做已成功的接口探测。
