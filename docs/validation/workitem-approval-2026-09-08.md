# 本地工作项审批启用记录

日期：2026-09-08。真实 Knowledge 链路通过后，继续推进需要确认的写操作。`create_work_item` 使用平台 PostgreSQL 的 `work_item` 表，不是外部工单、订单或支付接口。配置阶段不调用创建工具，也不代用户批准。

## 新增权限边界

rc.9 增加 `tool_policy.tool_allowed_users` 和 `direct_only_tools`：前者为工具指定精确 runtime_user_id 白名单，后者限制可信 chat_type 必须为 direct。省略字段保持原行为；显式空用户列表拒绝所有人，未知/旧消息缺少受众字段时拒绝 direct-only 工具。

Compiler 从已验证的 ChatInput 传递 Caller；工具列表先按用户/受众过滤，权限检查再使用同一有效白名单。批准状态只免除确认步骤，不能覆盖身份或受众限制。过滤返回新切片，不修改缓存中的租户策略。

本地仅给此前测试用户的私聊启用 `create_work_item`，同时显式列入 dangerous_tools；框架还会根据代码中的 ManagedSideEffect 标记强制审批。即使其他人向公开 Bot 发私聊或尝试在群聊调用，也不会获得该写工具权限。

## 代码验证

新增权限可见性、正确用户/错误用户、私聊/群聊/未知受众、已有审批不可绕过、空白符绕过防护、共享策略并发不变、非法策略拒绝和 Compiler 传递测试。完整 Runner 使用合成模型强行请求被隐藏的工具时，未产生授权执行、待审批记录或业务操作。既有真实业务幂等/审批测试继续通过。

`TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh` 通过全仓 race/lint/build、独立 PostgreSQL/Redis/Qdrant/MinIO、备份恢复与离线规则检查；随后补充的 Runner 拒绝路径测试通过。未向日常实例伪造审批或发起创建操作。

## 本地发布

发布前确认无未完成 Run/发送任务/后台任务/审批；tutorial-tenant 下 tool_operation 和 work_item 均为 0。私有备份目录 `data/workitem-enable.ksUXPvnG/` 保存配置、旧程序、数据库 dump、Redis RDB 和发布前控制面快照，敏感文件 0600、目录 0700，备份校验通过。

已先部署 rc.9（PID 214720），再经 Admin API 发布 `tutorial-workitem-v8`，App version 7 → 8，并仅更新原空闲测试私聊的 revision；审计 `audit-workitem-repin-ksUXPvnG` 与会话变更同事务提交。schema 仍为 23，没有改 `.env`。

结束前检查发现原模型和 Tunnel 进程已退出，公网返回 530；本轮没有主动停止它们，现有记录不足以判定退出原因。18:53 按原脚本/Named Tunnel 配置重新启动，无 DNS/Webhook 配置变更或开机自启。随后模型检查返回 OK，公网 healthz 和本地 readyz 正常；模型 PID 224133、Tunnel PID 224143，工作项数量仍为 0。

新配置只增加创建工具、单测试用户/仅私聊限制和指令。SQL 核对模型、Knowledge、Memory、附件/MCP 等其他 Agent 配置和治理字段保留。对真实已发布策略做只读权限预览：测试身份 + 私聊要求确认，其他身份/群聊/未知受众均拒绝，即使提供批准标记也不能绕过。预览没有附加审批仓库、Tool Journal 或 Runner，不创建业务操作。核对后 work_item 仍为 0，本地就绪与公网健康检查正常。

不能用旧 Worker 执行新权限字段；生产多 Worker 必须先全部升级，再发布新配置。公开文档不保存实际测试身份标识。

## 下一条用户消息

在原 Telegram 私聊发送：

```text
请调用 create_work_item 创建一个平台内测试工作项，business_key 为 WORK-20260908-01，title 为“验证审批后创建本地工作项”。请先发起审批，确认前不要声称已经创建。
```

### 首次申请（已确认等待审批，未执行）

18:57:50（Asia/Shanghai）的请求 `req_fc7f2d9473c06e22a757834c90ec7b69` 使用 `tutorial-workitem-v8`，trace `2935a22e6f6e00b34f070063db284d6d`。模型提出 create_work_item 后产生 `tool_ask`，审批 `apr_c4bbf2d78bdd1a70e1660aa3646000c5` 为 pending，有效期至 19:12:57。

审批提示已 sent、尝试 1 次且有 provider 回执，用户确认收到。该 request 的授权 Tool Execution 为 0；tutorial-tenant 下 work_item 和 tool_operation 均为 0，证明本轮尚未执行创建。Run 的 completed 只表示这轮申请/提示处理完成，不是业务创建成功。

### 批准与真实创建（已通过）

用户在 19:00:50（Asia/Shanghai）批准该审批，状态 approved，恢复执行请求为 `req_43c373e13be48758dc3376629b2d7922`。`create_work_item` 实际执行一次且 succeeded，关联业务操作 `op_f2a51c62a1de67f548f27e8f9b86e1a2fdc4b23131e88f25`。

PostgreSQL 中有一条工作项 `wi_e409d5050d6fab44ee7913ed4903522f0d77249d`，19:00:56 创建；标题与用户要求一致，business_key_hash 与 `WORK-20260908-01` 的 SHA-256 一致。tutorial-tenant 下只有一条业务操作和一条工作项。

批准确认请求 `req_ac610faae3f1a337f513131da8f4fcbe` 与恢复执行请求的回复均为 sent、attempt 1 且有 provider 回执，用户确认收到。两条分别是“已提交执行”和“执行结果”，不是重复执行。

审批决策与恢复执行共用新 trace `f0aaff04a9ee4293267169ded14241ce`；Tempo 中 approval.decide 的 span link 指向原申请 trace `2935a22e6f6e00b34f070063db284d6d`，并包含队列、Runner、create_work_item、Session 和回复发送链路。批准前无业务记录、批准后实际写入一次的本地链路已通过，不扩大为外部业务系统验证。

### 重复批准（已通过）

用户于 19:06:04（Asia/Shanghai）再次批准同一审批，新回执请求为 `req_f9cb0d0f552121dc61ef333b5473ed66`，trace `c354f86c3b7daefe2875dc0b494e58c1`。它是 platform-control 请求，prompt/completion token 均为 0，没有新 queue_outbox 执行任务；提示 sent、attempt 1 且有 provider 回执，用户确认收到。

原审批的 decided_at、resumed_at 和 decision_message_id 没有变化。tutorial-tenant 下仍只有一条 work_item、一条 tool_operation，以及原恢复请求下的一条成功 create_work_item 执行，没有追加调用。

至此，这组“申请 → 待批无写入 → 本人批准 → 实际写入一次 → 重复批准不重做”的真实本地链路通过，无需继续重复发送。跨新申请的同 business_key 复用、参数冲突、并发和故障恢复仍以自动/隔离测试证据为准，不能把重复批准测试等同于全部业务幂等场景。

已有失败回复保持原状态，没有为了本次测试重放历史 Run 或改写不确定发送记录。当前测试工作项保留，未执行删除或回滚。
