# 正式 Memory PostgreSQL V1

## 运行链与实现边界

AgentSpec `nodes.<llm>.memory` 显式声明 → Profile 选择 `managed_memory`
后端 ID/revision 并写入密码 → Control 固定非秘密 backend snapshot、credential use
与平台契约 → Worker Reader / Factory → SDK MemoryAttempt 与六工具 → 正常执行接受
→ 正式 Memory 落库 → Final 可投递 → 同 Session 下一轮读取。

Memory 与 Session Summary 是独立能力。Summary 仍保存在原 Session 中；启用
Memory 不隐式启用 Summary，也不隐式启用 Artifact、Knowledge 或自动提取器。
Profile 中资源的存在不是 Agent 使用授权。运行时不重新选择 backend revision，
不从环境变量猜测模型密钥、Memory 密码或连接目标。

### SDK 复用与新增适配

- 六工具 `memory_add/update/delete/clear/search/load`、ID、metadata、keyword search、
  Agent/Runner 执行与 preload 使用 root SDK `v1.11.2`。
- 未知或未选工具、调用预算越界属于执行边界错误，整轮候选丢弃；已选 SDK 工具
  的参数错误、不存在的 ID 等业务错误仍作为 tool response 交给模型纠错，不用
  错误文本分类器将它们统一升级成平台失败。SDK 自由文本日志桥接不依赖 tracing
  开关，避免错误日志带出工具参数。
- 模型只能调用发布的工具子集。preload 独立配置，`0` 禁用，`-1` 全量，正整数按
  SDK 条目数语义执行。正式快照总是完整加载，不能按 preload 数量截断再保存。
- 每个 Attempt 使用私有 SDK Memory 视图；工具不直接写正式数据库。
- 新增 `memorystore` 是保存 **SDK 完整候选快照** 的 PostgreSQL 薄 adapter，
  **不是直接使用 SDK PostgreSQL Memory backend**。后者公开逐条 CRUD 接口不具备
  本项目的接受边界，直接注入会令失败 Attempt 的写入提前生效。
- PostgreSQL adapter 只做固定 scope 的读取、单本地事务保存完整 entries、revision
  冲突守卫及已接受 completion 幂等收据。不另写工具 CRUD 或检索链。

## 配置与凭据

AgentSpec 在已有 LLM 节点声明：

```json
{
  "memory": {
    "tools": ["memory_add", "memory_update", "memory_delete", "memory_clear", "memory_search", "memory_load"],
    "preload_limit": 0
  }
}
```

主模型 requirement 及 Profile 模型都应声明 `chat`、`tool_call`。没有工具而仅启用
preload 时不需要额外暴露工具。沿用 Manifest `execution.max_tool_calls`，不新增
工具预算平台或累计 Token 限制。

Profile 的公开配置片段与写入动作：

```json
{
  "config": {
    "storage": {
      "memory": {
        "kind": "managed_memory",
        "backend_id": "memory-pg",
        "backend_revision": 1
      }
    }
  },
  "credentials": {
    "storage": {
      "memory": {
        "dsn_password": {"action": "replace", "value": "PASSWORD"}
      }
    }
  }
}
```

`value` 是密码，不是 DSN。PG host/port/database/username/TLS 来自受信目录的固定
backend revision；Worker 仅把解析到的密码填入该目标。Manifest 包含非秘密
`credential_id`、`purpose=dsn_password` 和等于 backend snapshot digest 的 audience，
不包含密码。公开 config 不暴露 canonical 内部 ID/audience。

换 backend/revision 必须显式重绑凭据；已发布 revision 的凭据状态与轮换沿用其
固定关联及现有权限/CAS 协议，不依赖当前目录仍列出旧 revision。

Backend limits 的 timeout、max_bytes 和 max_concurrency 分别用于后端调用时限、
完整候选容量及连接池并发配置，不新增私有阈值。

## 存储与隔离

- 可与 Session 使用同一个 PostgreSQL 实例/数据库，使用独立 `runtime_memory`
  schema、`memory_migrator` 迁移角色和 `memory_runtime` 运行角色。
- 正式内容位于 `runtime_memory.memory_heads`；幂等接受收据位于
  `runtime_memory.memory_receipts`。它们不是 Session 表或 Control 后端目录表。
- scope 由已验证的 tenant、social identity（provider/account/sender）及稳定 Agent ID
  派生，不包含 Session 或 Deployment/Profile revision。不同 Bot、用户、Agent、
  tenant 分离；同一可信作用域可以跨 Session 读取。
- `clear` 只保存当前 scope 的空集合，不清空其他 scope。
- 不同 Session 同时修改同一 scope，采用本地 revision 冲突失败，不静默覆盖，
  不扩大为全局串行化。

数据库管理员先显式创建两个最小角色、授予目标数据库 CONNECT、创建迁移者拥有的
schema 并授予运行者 USAGE。运行时 Open 校验角色/ACL，不执行 DDL。随后执行：

```sh
MEMORY_MIGRATION_DATABASE_URL='postgres://memory_migrator:PASSWORD@HOST:5432/DATABASE?sslmode=require' \
  go run ./services/agent-worker/cmd/agent-worker prepare-memory
```

成功输出 `MEMORY_PREPARATION=PASS`。Worker 本身另需正常执行新增的 Worker migration
`0019_memory_final_visibility.sql`。迁移、代码版本和平台契约需要同步发布，不改写
旧 migration 或旧 Manifest。

## 接受、Memory 与回复的明确顺序

1. SDK 工具仅修改 Attempt 私有视图；完整 LLM Final、Session 和工具校验成功后 seal。
2. 原 `Ledger.Complete` 检查租约、parent 等接受条件，原子接受执行/Session；若本轮
   使用 Memory，则记录 `memory_status=PENDING`，既有 Final outbox 的 `ready=false`。
3. Processor 停止并等待旧租约续期，使用已确认的 Completion 身份及 caller 下的
   backend timeout 同步调用 `ApplyAccepted`，不依赖已终结 Attempt 的旧租约授权。
4. PG Memory 使用一个本地事务保存候选与收据。相同 completion/digest 重放不重复
   应用；revision 冲突或持久化错误明确返回。
5. 原 Worker 数据库中的 `FinalizeMemory` 将状态定案：成功 `APPLIED` 并释放原
   Final；失败 `FAILED` 并把未发布 Final 改为固定失败文本后释放。
6. 同 Session 的后继 Claim/Scheduled 在存在 Memory PENDING 时等待；成功或失败
   定案后恢复正常调度。Final proof、outbox relay、MarkPublished 都遵守 ready gate。

执行 `SUCCEEDED`、Memory `APPLIED`、Final 可投递是三个有顺序的事实，不是一次跨库
原子提交。Memory 写失败不会再调用 `FailAttempt`、改写已接受 Session，或把模型
生成的“已保存”成功文本发出去。后台没有自动重新运行模型。

### 未覆盖的窗口与后续计划

- 进程在执行接受后、Memory 应用前退出：Memory 可能未保存，PENDING 保留，成功
  Final 隐藏，同 Session 后继等待。
- Memory 已提交但响应丢失：调用方可能只能报告写入未确认；若失败定案，`FAILED`
  表示本次应用未被确认成功，不承诺跨存储回滚。数据库收据提供固定身份的重复应用
  语义，但 V1 不引入自动恢复任务。
- Memory 已保存、Final 定案前退出：Memory 已有新值，PENDING 可能保留，回复及
  同 Session 后继仍等待。自动处理遗留 PENDING、崩溃恢复和收据保留策略后置。
- Redis、直接 SDK PG backend 的事务扩展、Memory 自动提取、独立 Memory CRUD UI、
  Artifact/Knowledge 后续分别推进，不以它们阻断本 PG 正常闭环。

## 验收入口

```sh
services/agent-worker/memorymigrations/test-postgres.sh
./scripts/test-worker-memory-finalization.sh
python3 scripts/test-worker-memory-joint.py --race --artifacts /tmp/memory-joint
python3 scripts/test-worker-memory-live.py \
  --env-file .env --model deepseek-v4-flash \
  --provider-base https://api.deepseek.com/v1 --artifacts /tmp/memory-live
```

- 存储脚本：真实独立 PostgreSQL、scope/CAS/幂等/失败事务回滚。
- finalization 脚本：真实 Worker PostgreSQL，Memory port 为受控 fixture；阻塞写入时
  同 Session 第二轮不读取旧快照，释放后可读；三条 Complete 出口与失败解锁。
- joint：真实 Control/Gateway/Worker、PG/NATS 和 Channel Lab HTTP；外部 LLM 为
  确定性 HTTP fixture，六工具及模型失败/PG 写入失败验收。
- live：实际外部模型调用，要求本轮实际 `memory_add`、正式 PG 保存，以及后继
  本轮实际 `memory_load` 的结果包含 canary，最终 Lab 回复与 provider Final 相同。

脚本的退出码和落盘 JSON 才是实际验收结果；GUI、真实 Telegram 和共享部署需分别
记录，不由单元测试、模拟外部模型或 Channel Lab HTTP 结果代替。

### 浏览器到同一 Worker 产物

`test-worker-memory-web.py` 启动专用 Control/PG/Worker/Web/Lab 测试进程，临时
OWNER 账号与数据库密码仅在 mode 600 私有文件中提供给浏览器操作。页面实际配置
Agent Memory、选择 Profile PostgreSQL backend、写入密码并发布新版本，再经
Deployment 页面发布。浏览器完成 marker 仅携带非秘密的发布标识。

```sh
python3 scripts/test-worker-memory-web.py \
  --artifacts /tmp/memory-web-evidence --coordination /tmp/memory-web-coordination
```

该脚本需要配合真实浏览器操作，不是自行假造 GUI 通过的 unattended 测试。收到
marker 后，它通过正式 Control HTTP 回读发布物，将同一 revision 绑定到 Lab，
核对 Worker Request 的 Manifest ID/digest/revision，再执行 SDK 六工具及正式 PG
保存/Final。截图和页面响应、实际后端执行与清理结果分别保存。
