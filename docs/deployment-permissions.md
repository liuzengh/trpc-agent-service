# 分角色部署权限

本阶段提供可生成的 SQL/Redis 策略、角色依赖收窄和网络模板，并用隔离 PostgreSQL/Redis 验证允许及拒绝操作。**没有替换当前开发服务账号，也没有应用真实 Kubernetes 策略**。目前运行中的 `all` 模式仍使用本地开发凭据。

## SQL：只给当前角色需要的表权限

```bash
go run ./cmd/trpc-permissions -format sql -schema agent_platform -role-prefix trpc
```

该命令只输出 SQL，不连接数据库、不执行 GRANT、不生成密码。部署者先用迁移账号在专用 schema 应用全部迁移，再审核并执行输出。禁止使用 `public`，避免误收回其他应用的公共权限。输出按事务执行，遇已有同名角色就失败，不覆盖现有授权。

| 角色 | 主要权限 | 明确不授予 |
| --- | --- | --- |
| Gateway | 路由配置只读、Inbox/Run/Outbox 创建、审批决策、接收检查点 | 修改 Agent 发布配置、修改发送尝试结果 |
| Worker | 路由只读、Run 完成、回复创建、工具与审批记录 | 发布新 Revision、标记回复已发送 |
| Relay | Queue Outbox 查询和投递状态更新 | 读取聊天正文或业务工具数据 |
| Sender | 租户/应用/Binding 查询、出站状态、发送尝试 | 读取 Inbox 正文、修改 Agent 配置 |
| Jobs | 后台任务、迁移状态、后端切换 | 修改 Agent 发布配置 |
| Admin | 控制面 CRUD、审计查询、业务操作只读对账所需更新 | 执行业务工作项插入、删改审计 |

角色均为 NOLOGIN、非超级用户，不授予 CREATE/DELETE/GRANT OPTION，不自动向新表开放权限。部署者另建 LOGIN 用户，分别加入对应角色；迁移用户独立管理，应用角色必须关闭 `TRPC_AGENT_POSTGRES_AUTO_MIGRATE`。升级增表时需要更新授权清单，不能用 `GRANT ALL` 绕过。

这是节点职责隔离，不是 PostgreSQL 租户 RLS。共享角色仍服务其负责的租户，逐条查询的租户过滤、BackendBinding/Secret grant 仍必需。Session/Memory 使用独立 SQL 后端时，还要为框架数据表单独准备 schema 和账号，不能直接复用控制面权限。对象权限与成员授权语义见 [PostgreSQL GRANT](https://www.postgresql.org/docs/16/sql-grant.html)。

## Redis：命令和键空间一起限制

```bash
go run ./cmd/trpc-permissions -format redis-acl -role-prefix trpc -redis-prefix trpc-agent-production
```

输出 Redis 7 selector 语法。所有用户默认 `off` 且无密码，必须由部署者通过安全的凭据管理方式设置密码并启用；不要把生成示例当作已可登录账号，也不要对真实实例执行测试中的 `fixture-only` 密码。

- Gateway 只能访问限流、预算读取和 `:channel-poll:coord:session:*` 轮询锁。
- Worker 可操作执行锁、幂等键、配额、队列及固定版本框架的数据键。
- Relay 只建队列 group 和追加 Stream，不消费队列或读取 Session。
- Jobs 可操作固定框架数据键；Sender/Admin 不使用平台 Redis 状态键。
- 禁止 FLUSHDB/FLUSHALL、KEYS、全局 SCAN、CONFIG、ACL 修改和任意 `~*`；Lua 仍受命令与键范围约束。

基线覆盖当前 Redis Session（hashidx/已启用用户索引）与 Memory 的键族；兼容模式或其他租户自定义 prefix 需要追加经审核的单独凭据，不能全局放开。逻辑 Redis DB 编号不是 ACL 隔离边界。selector 及授权累计规则见 [Redis ACL 文档](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/)。

程序同时调整了装配：Sender/Admin 不再因共享配置创建无用的远端 Session/队列；Relay 只初始化队列，Gateway 只按启用情况连接配额/轮询协调，Worker/Jobs 才加载执行数据后端。后台数据迁移仍由 Jobs 执行。轮询锁与 Worker 执行锁使用不同 prefix；混合版本升级需有序滚动，检查点 CAS/Inbox 幂等仍保留。

## 网络

Compose 中数据库、Redis、MinIO、Qdrant 端口改为只监听 loopback；**修改文件没有重建现有容器**，现有开发实例需要下次受控更新后才采用新监听范围。

Kubernetes 模板不再允许任意 namespace 访问数据端口。需要部署者设置依赖 namespace 的 `trpc-agent-dependency=data/telemetry` 标签，并匹配数据库/Redis/Collector Pod 标签；DNS 默认按 kube-system/kube-dns 限定。外部 HTTPS 只允许相关角色，排除私网/loopback/link-local，Relay 不获得外网权限。

该模板不是域名级防火墙。内网模型、托管数据库、NodeLocal DNS、不同 Pod 标签或 IPv6 集群需要显式调整，不能直接应用未核对模板。[NetworkPolicy 的执行依赖支持它的 CNI](https://kubernetes.io/docs/concepts/services-networking/network-policies/)，文件检查通过不等于真实集群策略生效。

## 验证

`deploy/permissions` 的 PostgreSQL 集成使用随机 schema/NOLOGIN 测试角色，验证发布权限、业务写入、Inbox 读取、审计删除和 DDL 拒绝；完成后只清理本次对象。Redis 集成只在新建的 loopback 临时容器执行 ACL，验证跨 prefix/危险命令拒绝，以及实际 Coordinator、Idempotency、Redis Stream 和框架 Session 的允许操作。没有改变日常 Redis ACL。

```bash
go test ./deploy/permissions ./deploy/kubernetes ./trpcservice/config ./cmd/trpc-service
# 以下为显式集成测试；TEST_POSTGRES_URL 指向受控测试实例。
TEST_PERMISSIONS_DOCKER=1 go test -race ./deploy/permissions -run TestRedisACLIntegration -count=1
```
