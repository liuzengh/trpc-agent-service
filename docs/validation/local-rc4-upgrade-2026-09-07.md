# 本地 rc.4 升级记录

日期：2026-09-07。范围：现有手动测试环境，不是生产上线记录。

用户已确认模型检查通过，并授权继续备份、升级本地数据库与服务，以及启用 Telegram 附件功能。没有修改模型凭据、重新 bootstrap、重建 Bot 绑定或配置开机自启。

## 已完成

- 原 Agent 已退出、数据容器已停止；启动原有 PostgreSQL / Redis 容器，未重建数据卷。升级前 schema 16，1545 条 Run 均 completed、1545 条回复均 sent，未发现待发送的旧协议消息。
- 私有备份位于本机 `data/rc4-upgrade.rGAls1WV/`：`.env`、旧程序、日志、控制面配置快照、PostgreSQL custom-format dump、Redis RDB 和校验清单。目录权限 0700、含凭据文件 0600，已被 Git 忽略。PostgreSQL dump 目录可解析，Redis RDB 校验通过，备份 SHA-256 校验通过；**本次未对业务备份做实际恢复演练**。
- 重新执行 `./build.sh`，显式应用迁移 017–023。当前程序 `0.2.0-rc.4`、schema 23。原有数据和旧 Agent revision 保留。
- `.env` 保持 bootstrap=false，并将 auto-migrate 改为 false；增加现有 Bot Token 的 `telegram_media` 用途授权，保留此前授权。
- 通过带鉴权和版本检查的 Admin API 发布 `tutorial-attachments-rc4-4`，App version 3 → 4。在原工具白名单中增加 `read_attachment`，原 `dangerous_demo` 及其审批规则不变。
- Telegram binding `telegram-tutorial-binding` version 2 → 3，开启 `attachments_enabled`。账户、回调、凭据引用、群白名单和 mention 过滤保持不变。企业微信 binding 未修改；同 App 的新工具白名单不意味着企业微信已支持附件下载。
- 启动 Agent、现有观测组件和固定 Cloudflare Tunnel，均为本次手动启动，没有设置自动启动。

本地仍使用开发级 PostgreSQL superuser 和默认 Redis 账号，因此没有套用生产分角色权限模板，也不将本次升级描述为生产最小权限部署。正式上线须另行迁移到受限身份。

## 已验证与待验证

已验证：

- 本地 `/readyz` 返回 ready；固定域名 `/healthz` 返回 ok。
- Telegram Webhook 地址与现有 callback 一致，查询时 pending=0、last_error_date=0；没有重新设置 Webhook。
- Redis 消费组 pending=0、lag=0；SQL 旧协议未发送记录为 0。
- 企业微信消息 MCP 检查点持续更新；未额外采样或导出原始聊天内容。
- 升级前后配置语义比较通过：模型、指令、审批规则、后端配置、通道身份与过滤策略均保留，只增加授权的附件功能。
- `go test ./trpcservice/attachments ./trpcservice/channels/telegram ./trpcservice/admin ./trpcservice/storage` 通过。这些测试没有调用真实模型或 IM。

升级时尚待真实消息验证。随后用户已完成 Telegram 文件上传和模型工具读取，结果与后台记录已交叉核对，详见文末“修复后的真实验证”。企业微信升级后的新消息回复仍需单独验证；Webhook 健康、自动测试及检查点推进不能替代业务链路验证。

## 下一次手动操作

1. 在 Telegram 私聊 `@trpc_agent_test_bot`，发送普通问候，确认新版本能回复。
2. 将仓库的 [attachment-test.txt](../../examples/attachment-test.txt) **作为文件上传**，不是粘贴内容。它不含凭据或私人资料，小于平台 2 MiB 上限。
3. 应收到平台保存回执，其中包含 `att_...` 编号。复制该编号，在**同一个账号、同一会话**发送：

   ```text
   请调用 read_attachment 读取附件 att_这里替换为实际编号，告诉我测试项目名称和本轮测试编号。
   ```

   预期从工具结果得到“星河计划”和“RC4-20260907”。不要仅凭模型说“已读取”判定成功，还需核对 `attachment_imported`、`attachment_read` 和对应工具执行记录。

群聊沿用原群白名单和 @/reply 条件；在群里上传时可以回复 Bot 的消息并附上文件，后续读取仍须在同一群、同一 Topic 中进行。首次验证推荐私聊以减少群聊投递策略干扰。

## 当前边界与恢复注意

初次附件验证使用 InMemory，只证明同一进程内保存/读取。后续已恢复该测试附件并切到 MinIO，重启后的后端读取检查与 Telegram 实际工具读取均通过，见[持久化启用记录](artifact-minio-2026-09-07.md)。Session 继续使用 Redis。尚未接通真实业务 MCP 工具服务；企业微信媒体下载、PDF/Office、图片理解和文件出站发送不在本次启用范围。

备份含真实业务数据和凭据，不上传 Git、不粘贴到聊天。Redis 运行时启用了 AOF，不能认为把 RDB 直接覆盖到数据目录就完成了恢复；恢复须明确 RDB/AOF 的加载顺序，并与 PostgreSQL 控制面、fencing 和别名元数据一致。升级后已有写入时，禁止直接恢复旧快照或盲目混跑旧程序，避免丢失新数据或重新执行已经完成的操作。

服务保持运行，没有在核对结果后自动停止。测试结束后按[运行手册](../operations-runbook.md)手动停止。

## 附件实测暴露的问题与修复（同日）

用户上传测试文件后，请求 `req_d6f0ea2cf01ea048e1313f3e0408986c` 返回失败通知。核对该请求的 Run、审计、trace 和对应时间段 PostgreSQL 错误，确认三次均失败在附件存储前的 advisory lock：复合锁键包含 `\x00`，不能作为 PostgreSQL TEXT 参数。未进入模型、没有工具执行记录，也没有成功导入附件的审计；失败通知正常送达。

修复内容：

- 在 Go 中将完整锁键做 SHA-256 并编码为 ASCII，再传给 PostgreSQL；加锁和解锁使用同一键，不移除租户或会话维度。
- 终止重试时保留 `FailRun` 已脱敏并限长的错误正文，避免仅剩 `retry_exhausted` 无法追因。不会把内部错误正文发往 IM。
- 附件失败使用独立 `attachment_import` 审计/trace 错误分类；trace 只记录分类，不记录原始文件、凭据或任意错误正文。
- 原锁测试仅使用简单字符串，附件全链路测试只覆盖内存控制面，未暴露此组合问题。现改用实际复合键，并加入真实 PostgreSQL 控制面、队列日志、advisory lock、附件保存/幂等读取和审计的组合测试，纳入隔离 PostgreSQL 回归入口。修复前可复现 `SQLSTATE 22021`，修复后通过；同时通过错误正文保留/脱敏测试及全仓 race、lint、build 回归。

同时发现此私聊在早期测试时固定到 `tutorial-revision-1`。发布 App 新 revision 不会自动更新既有会话，这是会话版本固定策略，不应通过修改旧 revision 绕过。此次在停止 Agent、备份并检查没有未完成 Run、回复、审批或不确定工具操作后，用一次带行锁、旧版本/turn 检查和同事务审计的本地运维操作，仅把该请求所属测试私聊切到已发布的 `tutorial-attachments-rc4-4`。这不是新增通用 Admin API。会话 ID、历史消息、旧 Run 的 revision 和失败状态保留，其他会话不变，原请求没有重放。

修复前快照位于本机私有目录 `data/rc4-attachment-hotfix.qJT9rTPD/`，含旧程序、数据库、Redis、配置和会话切换前信息。修复版已部署，schema 仍为 23；模型和 Tunnel 未停止。部署后要求用户重新上传测试文件，再使用新保存回执中的编号验证读取，结果如下。重传生成新的请求，不是重新执行旧的 dead 请求。

## 修复后的真实验证

2026-09-07 16:59（Asia/Shanghai），用户在原 Telegram 私聊重新上传测试文件并要求读取，随后提供了包含“星河计划”“RC4-20260907”以及测试目标的模型回复。后台只查询本轮测试相关元数据，未重放请求、读取其他会话正文或替用户发消息。

- 导入请求：`req_e2198df8f33982dcdcdf4e978552946d`，trace `5be352936bbbd2d8a9ccf797c29c75a2`。Run 为 completed，执行者为 `platform-attachment`，prompt/completion tokens 均为 0，存在 `attachment_imported` 审计；保存回执为 sent，发送尝试 1 次。
- 读取请求：`req_6e476c84d9fd02977c7d32f5834b6a22`，trace `d34aacf3c77f26eb17220f0f6e838fa1`。Run 为 completed，使用 revision `tutorial-attachments-rc4-4`；有且仅有一条本请求的工具执行记录，`read_attachment` 状态 succeeded，无 error_type；最终回复为 sent，发送尝试 1 次。
- `attachment_read` 审计与导入审计的 tenant、user、session 和附件编号匹配；读取审计通过 trace_id 关联读取请求。两条出站消息的 part 0 均为 sent。此次不是仅依据模型自述判定“已读取”。

结论：**Telegram 真实文件上传 → 平台导入 → 同会话工具读取 → 模型回复 → Telegram 投递的最小文本附件链路通过。** 该轮验证时 Artifact 是 InMemory，不证明跨重启持久化、跨节点读取、图片理解或其他媒体类型。后续持久化验证单独记录，不追溯扩大这轮测试的结论。
