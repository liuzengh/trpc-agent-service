# IM 通道接入与运行契约

平台支持 Telegram、企业微信自建应用回调和企业微信托管消息 MCP。它们共用租户路由、审批、Inbox/队列与 Reply Sender，但认证和传输协议不同。真实联调边界见[验收说明](acceptance.md)。

## 1. 统一链路与隔离

Adapter 校验上游身份并规范化文本/媒体引用，Gateway 根据 Binding 反查 tenant/app/revision，生成内部用户和 Session Scope；Runner 接收 `model.NewUserMessage`，Worker 持续消费 Event channel 并持久化结果，Sender 负责出站。

外部请求不能直接指定可信 tenant_id。完整 Session Key 为内部 AppName + runtime_user_id + session_id。AppName 固定 tenant/app，不包含 revision，避免灰度时丢失历史。

- 单聊：绑定作用域 + 上游用户/聊天标识。
- 群聊：绑定作用域 + 群 + 成员；Telegram Topic 还包含 message_thread_id。
- 跨群、跨 Topic、跨 Binding 或租户产生不同身份/会话键，不以昵称识别人。
- 当前默认群会话按成员隔离，不表示全员共享上下文；群共享方案须另外确认 Memory 与工具权限边界。

进入模型前执行群/成员/机器人/mention 策略。media ID 和文字内容都不是授权证据；实际用户信息保存在可信 runtime context 和审计中。

IM 默认采用 `message_policy={"mode":"realtime","max_age_seconds":120}`。从 schema 26 起，120 秒表示企业微信的近期接收窗口，不再是消息有效期：合法消息先持久保存，Telegram 延迟投递和已入队请求不会因年龄被丢弃。近期接收与历史补读独立运行，近期请求与恢复请求分队列调度。`mode=reliable` 保留按历史检查点顺序接收的方式；两种模式都不自动重放旧版已经终止/忽略的请求。

模型连接失败、HTTP 408/429/5xx 且本轮尚无模型输出或工具执行时，请求进入 `waiting`，延迟 5/10/20/30 秒调度，之后间隔上限为 30 秒，不消耗普通执行失败的三次尝试。等待不占 Worker 执行槽或租户并发租约；同租户/应用/版本共享短暂退避。平台为每条请求最多创建一条等待提示，恢复后仍使用原请求编号产生最终回复。确定无法执行的错误仍终止并反馈，结果未知的工具/发送不盲目重试。

## 2. 接入差异

| 项目 | Telegram | 企业微信自建应用 | 企业微信消息 MCP |
| --- | --- | --- | --- |
| 入站 | HTTPS Webhook | 加密 callback | Gateway 主动轮询登记的群 |
| 验证 | Webhook Secret | 签名、时间窗、AES、CorpID | 带凭据 MCP URL + 平台用途授权 |
| 消息幂等 | update_id | 上游消息字段 | 规范化 fingerprint-v1 |
| 出站 | sendMessage | 应用 Token + 文本 API | message_aibot_send |
| 公网回调 | 需要 | 需要 | 不需要 |
| 当前出站能力 | 文本 | 文本 | 群文本 |
| 验证层级 | 基础真实联调 + 自动测试 | 模拟协议测试 | 基础群文本真实联调 + 自动/SQL 测试 |

微信公众号/微信客服可按同一 Adapter 扩展，但当前没有对应实现；不能因为列在框架能力或设计中就称为已接入。

## 3. Telegram

### 配置与注册

1. 由部署者创建 Bot，将完整 Token 保存在私有环境变量。
2. 在控制面建立 channel_type=telegram 的 Binding，配置 bot_token_ref 和 webhook_secret_ref；分别授予 telegram_bot、telegram_webhook 用途。
3. 准备 HTTPS 入口，Webhook 指向 `/callbacks/telegram/{callback_key}`，注册时使用与配置一致的 webhook secret。
4. 群使用 allowed_chat_ids、bot_user_id、bot_username、require_mention、ignore_bot_messages 限制接收范围，再启用 Binding。

配置示例（不含真实凭据）：

```json
{
  "bot_token_ref":"env://TENANT_A_BOT_TOKEN",
  "webhook_secret_ref":"env://TENANT_A_WEBHOOK_SECRET",
  "bot_user_id":123456,
  "bot_username":"example_bot",
  "allowed_chat_ids":[-1001234567890],
  "require_mention":true,
  "ignore_bot_messages":true,
  "attachments_enabled":false
}
```

Binding 的 callback_key、tenant/app、状态和版本由控制面管理。变更使用 expected_version 防止覆盖；首次注册与日常启动分开，不要每次启动都重新注册 Webhook。

Privacy Mode 影响上游是否投递群消息；普通文本 @ 不一定被投递。平台能识别命令、mention 实体和回复目标，但不能处理上游未发来的更新。应先检查 Webhook/群策略，再判断模型故障。

### 附件与编辑

文本、图片/文件 ID 和 edited update 可以解析；当前 Sender 不调用 editMessageText/sendDocument/sendPhoto。编辑消息拒绝再次执行业务操作，媒体默认给出受控提示。

显式启用 attachments_enabled 后，Telegram 可经固定下载器保存限定类型附件到 Artifact；还需要 telegram_media grant 和可用的 Artifact Backend Binding。支持 text/plain、PNG/JPEG，最大 2 MiB，并检查名称、MIME 和图片尺寸。未知类型、超限、错误主机或不安全路径拒绝，不允许模型任意下载 URL。

导入本身不自动进入模型或工具审批。读取文本需单独开放 read_attachment，并校验原 tenant/app/user/session。保存图片不等于图片理解；没有完整杀毒、PDF/Office 解析、媒体发送或通用多模态能力。

## 4. 企业微信自建应用回调

Binding 保存 CorpID、AgentID、callback Token/AES Key/应用 Secret 的引用。回调路径为 `/callbacks/wecom/{callback_key}`。

GET 验证 URL；POST 校验签名和时间窗，AES 解密并检查接收方后规范化消息。Gateway 持久化后快速 ACK，不在上游回调超时窗口内等待模型。出站缓存 access_token，处理 Token 失效、限流和发送错误。

这是自建应用协议，不适用于只提供 StreamableHttp URL / JSON Config 的消息 MCP 页面。当前实现由模拟服务测试，不能把 MCP 群文本的成功算作此接口已真实验证。

## 5. 企业微信托管消息 MCP

### 授权与启用

完整 MCP URL 的 query 中可能带 apikey，整个 URL 都按密钥处理。控制面 SecretRef 指向私有 WECOM_MCP_URL，并分别配置 wecom_mcp_read/send grant。Gateway 使用 read，Sender 使用 send，模型和 Worker 不获得该 URL。

Binding 类型为 wecom_mcp，至少配置：

```json
{
  "allowed_chat_ids":["<已授权群 ID>"],
  "allowed_user_ids":["<已确认人类账号 ID>"],
  "mention_prefix":"@机器人名称",
  "mention_style":"whitespace",
  "timezone":"Asia/Shanghai",
  "start_at":"2026-01-01T00:00:00+08:00",
  "dedupe_mode":"fingerprint-v1"
}
```

start_at 必须替换成明确授权的起点，不能照抄示例或默认回溯全部历史。默认 whitespace 要求完整前缀后有空格/换行；显式 prefix 模式用于特定上游格式，仅是文本匹配，必须结合群和人类成员白名单，不是实体级 @ 认证。

只有配置了 `TRPC_AGENT_WECOM_MCP_TARGETS_JSON` 中的精确 tenant_id/binding_id，并启用 Binding 后，Gateway/all 才开始拉取。默认空数组，即使 URL 存在也不读取。配置变化后重启相关角色；停止拉取不等于取消已入队任务。

### 接收与检查点

Poller 在群级租约内读取完整窗口，调用 chat_messages_list 分页后排序，逐条校验并交给统一 Gateway。近期优先每次查询近期窗口，并尊重 start_at 和人工恢复下界；跨过的区间与近期检查点在同一事务中保存为待补读任务。另一个循环使用独立租约和区间游标补读，每次最多查询 2 分钟，不修改近期检查点。两条路径共享 seen 和 Inbox 去重，先接收落库、后推进进度。单窗口最多 20 页、1000 条，HTTP 响应最多 2 MiB；分页错误和超限时保留进度。

跨过源保留期或人工恢复下界发生变化时，补读区间标记 `blocked`，在通道诊断中展示原因，不能假装全部补齐。旧版区间保留 `skipped`，不会因升级突然执行历史指令。同一会话按平台接收时分配的 `turn_seq` 排队；晚到的历史消息不会倒插已经提交的 Session，也不能承诺供应商未提供的原始严格顺序。

源样例没有可靠唯一消息 ID，指纹使用租户、绑定、群、发送者、秒级时间、类型和文本。同人同秒两次完全相同文本会合并，因此不是端到端 exactly-once。同秒无可靠源顺序，只能作确定性排序。超出重叠窗口才可见的消息仍可能漏读，保留期边界需人工确认。

检查点版本 CAS、seen 与 Inbox 唯一键共同支持重启和重叠读取。媒体单条隔离不阻塞正常文字，不下载、分析或回复；媒体分组不能当作精确附件计数。分页/窗口整体失败则不推进。

### 发送与恢复

Sender 调用 message_aibot_send，不使用通用身份的 message_send。平台先创建稳定 OutboundID 的 attempting 记录；成功需要非错误 MCP 结果且业务 success/errcode 同时满足成功条件。

同 ID 已 sent 返回已有结果，attempting/unknown 不自动重发，rejected 终止尝试。接口没有可靠消息 ID 时不编造 ProviderMessageID；回读为空也不能证明未送达。同 ID 不同目标/正文拒绝复用。

恢复前禁用 Binding，通过受鉴权 Admin 的 channel-checkpoints/list/recover 接口核对版本、起点及历史缺口。不能清空 seen、删除发送尝试或把未知记录直接改成成功。当前不提供私聊、卡片、编辑或文件发送。

## 6. 通用出站、限流和错误语义

当前各通道以完整文本聚合回复，不对 IM 实时推送每个模型 token。Sender 按通道长度上限分段，保存分段事实；已发送部分不会因后续失败而回滚。卡片、流式预览或文件发送是可扩展能力，不是当前 Capabilities 的承诺。

明确的限流按 Retry-After/退避处理；可能已经发生发送的网络超时、回执持久化失败进入 unknown，停止盲目自动重试。HTTP 202 只代表入站持久化，模型生成完成也不代表回复送达。

原始错误体、Bot Token、MCP URL 和数据库地址不得进入平台提示或 trace。使用 request_id 关联 Inbox/Run/Outbound、工具记录和审计，用 trace_id 串联执行时延。
