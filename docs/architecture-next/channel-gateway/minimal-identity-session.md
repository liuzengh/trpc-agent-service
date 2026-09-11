# 当前切片：社交身份记录与用户连续对话

状态：替代此前优化实现的授权／预算主路径。基于 `d2a61d7` 的最新主线，保留独立 Web／Tracing 变更。

## 用户目标

记录消息来自哪个社交账号；同一用户在同一 Bot／会话中可以连续对话；另一用户不读取这段历史；Worker 换节点后仍读取已提交历史。
这不是 allowlist 管理、撤销传播、模型账单或硬预算产品。原优化设计保留为历史方案，不再作为本切片的强制前置条件。

## 实际接线

1. Telegram／企微原有 Connector 和 Gateway handler 验证消息来源，提取 Provider `sender_id`。
2. Gateway 沿原正式 Admission／Outbox／RunRequested 路径接纳文本；Control 仍负责 Bot 凭据、Binding、Agent／Profile／Deployment 发布。
3. Worker 原 `Acceptor -> Ledger.Accept` 路径先处理 Receipt 重放、Run 冲突和现有存储容量门禁。
4. 同一数据库事务内写入社交身份、Session 关联、Run、Receipt。无需当前授权投影、动态 Session 策略或 pending promoter。
5. 原 Processor 解析固定 Manifest、Claim、读取已接受 Session、执行 SDK、提交 Session／Final。Delivery 使用原回复目标投递。
6. Trace carrier、durable span、原有消息幂等、租约 fence、单次 MarkExecuting 保留。

最小身份记录由 Worker Execution intake 拥有：它表示「已接纳消息中观察到的身份」，不是全局人员目录或 Control 登录账号。
没有额外身份同步服务、独立 Connector 镜像或新的部署单元。现有 Channels Web 与 Tracing 功能保留；本切片无新增身份管理页面或权限配置表单。

## 数据与键

`execution_social_identities`:

- `tenant_id, identity_id, provider, account_id, external_user_id`
- `created_at, last_seen_at`
- 唯一键 `(tenant_id, provider, account_id, external_user_id)`。
- `identity_id = stable_hash([tenant, provider, account, external_user])`，不记录昵称作为身份、不自动合并不同 Bot／Provider。

`execution_session_identities`:

- `(tenant_id, session_id) -> identity_id`，外键关联正式 Session 与身份记录。
- `session_scope = [tenant, provider, account, conversation, thread, binding, deployment_revision, identity_id]`。
- 消息 ID、文本、Run ID、路由刷新 generation 不改变 Session；更换会话、sender 或发布版本会隔离历史。
- 不实现跨群共享、自动身份合并、动态 Session policy、`/new` 或共享群 reset。

身份只在新 Run 成功接纳的事务里创建／更新。重复事件返回旧 Receipt，不改变 last_seen、不追加历史；容量拒绝和事务失败不产生孤立身份。
当前原有 retained-Run 上限仍保留；身份记录保留用于持续会话，未来归档应按关联事实清理，而不是新增一套 token 配额系统。

## 撤出的内容

撤出 Control channel-principal／access-policy／policy-definition 新增管理发布代码与对应 API、策略专用 NATS 配置、Gateway 授权投影／快照刷新、Worker 当前授权／pending intake／策略型 Session registry、Run quota、consumption 预算／usage 账本／对账及 ModelAdmission 框架。

保留原有 Control 管理账号认证、租户归属、凭据 mTLS、Gateway 来源验证、Worker 在线 grant proof、固定目标校验、幂等、原有容量上限和 Delivery UNKNOWN 处理。这些是原业务链的正确性，不是撤出的用户使用授权产品。

## 数据库与切换

- 已发布 Control／Gateway／Worker SQL 文件保持字节不变。旧表和历史事实保留作迁移历史，不再由退役模块读写；本次不清库、不批量删除历史模型消耗。
- Worker 新迁移 `0018_minimal_identity_sessions.sql` 增加两张小表，移除旧授权 Attempt 和 partitioned-pending 触发器；保留单次发送与 Tracing 迁移。
- 若仍有旧 governed pending 输入，或带旧 authorization 的非终态 Run，迁移明确失败且事务回滚。先核对并处置这些旧输入；源码替换不将它们自动变为无需授权的新消息。
- 同步升级 Gateway 生产者与 Worker 消费者；停止旧治理生产者，并核对 NATS／outbox 中是否还有带 authorization 的旧事件。新闭合协议拒绝该退役字段，不静默剥离授权信息。
- 旧 Session、Receipt、Run 不改写、不删除。旧事件重放仍返回旧 Session。新接纳输入使用新的用户分区作用域，因此不会自动继承可能多人共享的旧 scope 历史；不做隐式历史迁移。
- 这是前向替换提交，非二进制滚动双栈方案。源码回滚副本验证不代表生产数据库降级或旧新 Session 历史自动合并。
- 清除旧 Worker `authorization` 配置；Compose 已移除两个 Gateway policy／authorization refresh 开关，其他部署单元保持不变。

## 验收与证据

- Domain 先红后绿：旧 sender 无关 scope 出现碰撞，新用户分区通过。
- 真实 PostgreSQL：Telegram／企微身份、同会话不同用户、同用户 8 个并发输入、双连接池、Receipt 重放、正式 Claim、重复发送拒绝、容量失败不留身份。
- 升级：原迁移摘要不变；旧历史保留；存在旧 governed 输入时前向切换原子失败；明确处理夹具后重入迁移成功；Trace 列保留。
- `scripts/test-minimal-identity-session.py`：真实 Control HTTP 发布、Gateway 入口、NATS、Worker、SDK HTTP、PostgreSQL Session、Final；两个用户各两轮，换 Worker 后第二轮 SDK 请求确实包含各自第一轮问答且无另一用户历史。模型与 Telegram API 为显式外部协议夹具，不是实时外部 Bot 验收。
- 本次 Telegram 群触发能力不变，仍不因用户分区而放开群消息；企微同会话分区在 Worker PostgreSQL 层验证，真实企微双轮尚非本记录的证据。

本地证据入口：`artifacts/minimal-session-replacement/VERIFICATION.txt`。全仓测试、真依赖测试与外部 IM 验收分开报告。
