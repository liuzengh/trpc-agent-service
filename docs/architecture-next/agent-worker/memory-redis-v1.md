# Redis Memory V1

## 运行链与复用边界

Redis 是可选的正式 Memory 后端，不是 PostgreSQL Memory 的缓存。AgentSpec 的
`node.memory.tools` / `preload_limit` 不变，Profile 选择 `managed_memory` 的 Redis
Backend revision，写入 `credentials.storage.memory.dsn_password`；这里仍是密码，
不是 Redis URL。Control 发布不可变 Manifest，Worker 从固定 Snapshot 的 host、port、
database、username、TLS 与 credential audience 构建客户端。

根 SDK 固定 `trpc-agent-go v1.11.2`。本地检查的独立 `memory/redis` 和 `session/redis`
源码缓存版本是 v1.10.0，缓存不代表项目依赖。本实现不导入它们，也不升级根 SDK。
独立模块的 CRUD/Append/Summary 直接写 Redis，不提供本项目 accepted candidate 协议。
直接把持久 SDK Service 接到 Runner 会使未接受或失败 Attempt 的写入提前可见。

因此复用当前 SDK MemoryAttempt、六工具、search/load、Runner、scope、候选编码与
Final 顺序，只新增 `memorystore.Redis` 的 Load / ApplyAccepted 适配，底层依赖
`go-redis/v9 v9.11.0`。没有复制工具执行器、编译器或管理页面。

## 配置与作用域

- Backend kind 为 `redis`，adapter 为 `managed-redis-v1`，isolation 为
  `tenant-subject-agent-v1`；Redis username 固定 `memory_runtime`。
- Profile 仍为 `storage.memory={kind:managed_memory,backend_id,backend_revision}`。
  密码 keep/replace/clear 和发布后轮换沿用既有协议。切换后端或 revision 必须显式
  replace 绑定新的 Snapshot.Digest，不能把原关联默认为新目标的凭据。
- Tenant + 社交主体 + 稳定 Agent ID 确定 Memory scope，Session 和 Deployment revision
  不参与 scope。摘要仍属于 Session，与 Memory 的独立持久数据无关。
- Key 使用固定 `runtime_memory` 前缀、Tenant hash tag、不可歧义编码的 scope/completion
  hash。命名隔离配合可信身份与固定目标，不能代替 Redis 访问控制。
- 同一个 Redis 实例可用独立 ACL 用户/允许的 key namespace 隔离不同存储角色；Redis
  database 编号不是 ACL 安全边界。本阶段仅支持固定 standalone Redis，不宣称 Cluster、
  Sentinel 或自动故障转移。
- 开启 TLS 时校验系统信任链与固定 host，密码不能改变目标或禁用 TLS。

## 原子写与 Final

SDK 工具只改变 Attempt 私有视图。成功结束后复用现有顺序：

`PG Complete 接受 Session/Run + PENDING Final → Redis ApplyAccepted → PG FinalizeMemory → Final`

Redis head、completion receipt、run/attempt receipt 用一次 MSET 原子保存。脚本在唯一
写命令之前完成键类型、过期状态、完整旧 head 和幂等内容的比较；脚本本身没有运行时
错误回滚保证，所以不先写 head 再写 receipt。revision 由 Go 端精确校验并用十进制字符串
传递，避免 Lua 浮点在大整数上的精度丢失。旧接受的精确重放返回其原 revision，不覆盖
已经推进的新 head；不同接受对同一 base revision 的竞争明确报冲突。

同 Session 的 Memory PENDING 延续原有 Claim/Scheduled 等待门禁；写失败释放固定失败
Final，不报写入成功，不把 Redis 错误文本/连接密码写入日志。不同 Session 仍不全局串行。

## 持久性、后端切换与未完成项

- 本适配不设置 TTL，正式 head 和幂等 receipt 均长期存在。没有新增后台清理任务。
- Redis 服务端必须作为持久存储运维：明确 AOF/RDB、备份、恢复和 `maxmemory-policy
  noeviction`。验收使用 AOF always + noeviction，并测试真实重启后读取。生产持久性
  取决于实际 Redis 配置、磁盘和故障模型，不承诺进程返回成功即跨机器零丢失。
- 从 PostgreSQL 切换到 Redis，不自动迁移旧 Memory。新 Run 只读取所选固定后端中与
  当前 tenant/subject/agent scope 匹配的数据；切回原后端可再次读取其原有同 scope 数据。
  当前 head 的显式复制由 [多后端迁移 V1](backend-migration-v1.md) 实现；旧 revision、
  接受收据、在线双写和隐式切换仍不迁移。
- Redis 与 PG ledger 之间不原子。Complete 后 Apply/Finalize 前崩溃仍可能留下 PENDING；
  自动恢复、补偿、调度保留后续计划，不隐式重放未确认的业务运行。
- 旧无密码 Redis Manifest 描述仍可读取；新的 active Memory 发布与 Worker 执行要求完整
  credential。新 Redis 发布物不能投递给旧 PG-only Worker。同批协调二进制/发布物切换，
  不把 catalog digest 误认为二进制能力协商，不改写历史 Manifest。
- 下一阶段是 managed Redis Session + 相随 Summary：PG accepted head 指向同一 Redis
  不可变 snapshot，摘要不另建数据库。Artifact/Knowledge 属于之后独立包。

## 验收入口

```sh
GOCACHE=/tmp/trpc-worker-go-cache services/agent-worker/memorymigrations/test-redis.sh
GOCACHE=/tmp/trpc-worker-go-cache python3 scripts/test-worker-memory-redis-joint.py \
  --artifacts /tmp/redis-memory-joint --race
GOCACHE=/tmp/trpc-worker-go-cache python3 scripts/test-worker-memory-web.py \
  --backend redis --artifacts /tmp/redis-memory-web --coordination /tmp/redis-memory-gui
```

存储测试使用独立真实 Redis，联合测试使用真实 Control/Gateway/Worker/PG/Redis/NATS/
Channel Lab；外部模型是确定性 HTTP fixture。GUI 入口等待实际浏览器完成 Agent/Profile/
Deployment 发布，然后核对 Worker route 的 Manifest ID、digest、Deployment revision 与
该页面发布物一致，再读取正式 Redis 和 Lab Final。真实外部模型的统一最终版本回归另行
进行，不将此 fixture 验收表述为真实模型或真实 Telegram 运行。

### 已观察的第一阶段结果（2026-09-09）

真实 Redis 存储合同在 race 下通过，并在 AOF 容器重启后读取原 accepted 数据；联合
HTTP/Lab 测试通过八轮输入，包含六工具、后继读取、可纠正工具业务错误、模型失败零写、
Redis MSET ACL 拒绝无部分写入及固定失败 Final、权限恢复后成功、不同主体空 Memory、
重启后回原主体继续读取。跨 Tenant 隔离由独立真实 Redis store 合同覆盖，不将单 Tenant
Lab 测试冒称为跨 Tenant 全栈验收。GUI 发布与同 Manifest 运行结果见独立 `memory-web.json`。
