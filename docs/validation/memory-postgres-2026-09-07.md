# 本地 PostgreSQL 长期记忆启用记录

日期：2026-09-07。用户要求持续推进，直到需要本人发消息。当前本地程序 `0.2.0-rc.5`，控制面 schema 仍为 23；本次没有新增平台 migration，而是由运维账号预建框架 Memory 表。

## 启用前的事实

此前四个 revision 的 `memory_config` 都为空，没有 `memory_*` 工具，也未开启 preload 或自动提取；`resource_sync` 没有登记 Memory I/O。此前 completed 的 `memory_extract` Job 因配置关闭而直接结束，**不表示已经生成长期记忆**。

本次是为尚未启用的 Memory 初始化持久后端，不是对运行进程内存做全量导出，也没有从历史聊天重新提取事实。如果源 Memory 已有数据，不能直接套用这个空后端切换流程。

## 后端与权限

- 复用现有 PostgreSQL 和框架 `memory/postgres`，目标表为 `agent_memory.tutorial_memories`。
- 表和索引由运维账号初始化。新建的随机专用账号只有该 schema 的 USAGE 和这张表的 SELECT/INSERT/UPDATE/DELETE，没有 superuser、建表或控制面 `agent_run` 表读取权限。
- 密码通过 `psql \password` 的标准输入设置，不把明文拼入 SQL 或命令参数；该命令在客户端加密后提交密码变更，见 [PostgreSQL 16 官方说明](https://www.postgresql.org/docs/16/app-psql.html)。
- `.env` 新增私有 `MEMORY_POSTGRES_URL` 及 `tutorial-tenant / memory / env://MEMORY_POSTGRES_URL` 精确授权，保留其他模型、IM、MinIO 配置。
- 新 binding `tutorial-memory-postgres-v1` 为 active，配置 `schema=agent_memory`、`table_name=tutorial_memories`、`skip_db_init=true`、`memory_limit=1000`。平台新增 `skip_db_init` 映射到框架 `WithSkipDBInit`，运行时不用 DDL 权限。
- 旧 `tutorial-memory-backend` 保留为 retired、version 2。旧绑定退役、新绑定创建和审计在一个事务提交，切换前已停旧 Agent、检查未完成 Run。

这只改进了 Memory 数据连接的权限；现有 all-role 开发进程的控制面连接仍是开发管理员账号，不代表整个平台已完成生产最小权限部署。

## 记忆写入和读取规则

通过 Admin API 发布 `tutorial-memory-manual-v5`，App version 4 → 5。保留原附件工具和审批规则，仅增加 `memory_add`、`memory_search`、`memory_load`；没有向模型开放修改、删除或清空记忆工具。指令要求用户明确提出记忆需求才保存，工具成功前不能宣称已保存。该语义由 Agent 指令引导，不等同于独立审批机制。

```json
{"direct_only":true,"auto_extract":false,"every_turns":0}
```

`preload_memory=0`，不自动提取整段聊天或注入记忆。Memory 按租户/App + 当前通道 binding 映射的用户归属，与按 Session 保存的聊天历史不同。

新增代码级受众限制：验签后的 IM `chat_type` 随 Gateway → 持久队列 → Worker → Runner 传递。`direct_only` 会移除群聊和未知受众请求的 `memory_*` 工具，并在权限检查时拒绝执行，已有审批不能绕过。旧队列缺少该字段时按未知处理。此策略不能与 preload 或自动提取同时开启，以免绕开工具限制。

仅将此前测试所用、已空闲且没有待审批/不确定操作的 Telegram 私聊切到新 revision，切换和审计同事务完成。Session ID、历史消息和旧 Run 保留；其他既有会话没有批量升级。

## 已验证

- 专用账号写入合成探针后，另一个进程可读回；换用户不能读取。随后按准确 ID 清理探针，没有向真实用户预填记忆，也没有删除真实用户事实。切换后表中为 0 行。
- 新增完整 Runner/框架工具/真实 PostgreSQL 测试：写入后关闭 Runner，再用新的 Runner 和空白 Session 读取成功；其他用户与群聊均不能读取。
- 新增私聊权限、禁止 preload/自动提取绕过、Gateway/队列/Worker 受众传递测试。PostgreSQL Memory 测试增加重新打开、幂等和用户隔离覆盖。
- `TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh` 通过全仓 race/lint/build、独立 PostgreSQL/Redis/Qdrant/MinIO、备份恢复工具链和离线告警检查，仅清理自建合成数据和容器。
- rc.5 已重启，本地 `/readyz`、公网 `/healthz` 正常；独立进程仍可从活跃 MinIO 读取原测试附件。

私有备份在本机 `data/memory-upgrade.i55Y65pw/`，含配置、程序、数据库 dump、Redis RDB、快照及新凭据信息。目录 0700、敏感文件 0600、Git 忽略，已校验备份 SHA-256 和 dump 目录。本次没有执行真实业务库恢复；数据库 dump 不含集群角色，恢复时还须重建专用角色及授权。

## 真实保存与重启检查（已完成）

用户已发送保存消息并确认收到回复。2026-09-07 17:59（Asia/Shanghai）的请求 `req_06fa63d44d19d2fd96f3789b46a4e5bf` 为 completed，trace 为 `48c43dcb74e93f2136c4ba5ca10d6da7`：

- 有一条 `memory_add` 执行记录，状态 succeeded，无 error_type。
- 按本请求所属 tenant/App/用户查询，数据库有一条 Memory，正文同时包含“长风三号”和“MEM-20260907”。没有查询其他用户的记忆正文。
- 回复 sent、发送尝试 1 次、part 0 有 provider 回执。核对时没有未完成 Run、回复或后台任务。

保存消息是：

在原 Telegram 私聊发送：

```text
请调用 memory_add 保存一条长期记忆：我的本轮测试识别词是“长风三号”，测试编号是“MEM-20260907”。只有工具成功后再告诉我已保存。
```

核对后已直接完成备份与优雅重启，没有重复执行写入或清空 Session。新 Agent 于 18:04:09 启动，仍为 rc.5；模型和 Tunnel 保持运行。重启后独立 `MemoryRouter` 使用受限数据库账号读回该条事实成功，Memory ID 和完整 `memory_data` 的 SHA-256 与重启前一致；本地就绪和公网健康检查通过。

本次备份在私有 `data/memory-restart-check.IDw7wshL/`，含数据库 dump、Redis RDB、配置、重启前日志和前后摘要。未恢复旧快照、改写真实记忆或重放旧请求。

## 重启后的真实工具读取（已通过）

用户确认收到读取回复。2026-09-07 18:35（Asia/Shanghai），新请求 `req_9e7661f4d5cf0c8d3b65b91946c8283f` 为 completed，trace 为 `a350d44e2b8fe94d225bce574c864fcf`。`memory_load` 实际执行一次且 succeeded，无 error_type；最终回复包含正确识别词和编号，sent、尝试 1 次、part 0 有 provider 回执。记忆表仍为 1 行，没有重复写入。

结合此前真实 `memory_add`、数据库事实、Agent 重启及独立读取检查，**Telegram → 工具保存 → PostgreSQL → Agent 重启 → 工具读取 → Telegram 回复**的本地链路通过。群聊限制和空白新 Session 的验证来自自动/隔离测试，不扩大为真实多用户、多节点或生产恢复验收。

### 本轮读取消息

在同一个 Telegram 私聊发送，不必再次提供答案，也不要重复保存：

```text
请调用 memory_load，从长期记忆中读取我保存的本轮测试识别词和测试编号。不要根据聊天历史回答，也不要新增或修改记忆。
```

本轮已核对工具执行和投递记录。后续复测仍不能只凭模型复述 Redis Session 中的历史判定成功。服务保持运行。
