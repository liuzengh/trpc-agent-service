# Managed Redis Session 与同后端 Summary V1

## 完成的最小装配

AgentSpec 仍使用既有 `runtime.summary` 和节点 `add_session_summary`。Profile 的
`storage.session` 可选择固定 `managed_session` Redis backend ID/revision，并用既有
`credentials.storage.session.dsn_password` 写入密码。没有新增 AgentSpec 字段、摘要
存储项或数据库。Control 发布不可变 Manifest，Reader 映射 `Plan.SessionBackend`，
Factory 解析固定 credential batch 后打开 Redis；旧 `postgres_state` 路径保持。

根 SDK 仍为 v1.11.2，复用原 Session overlay、同步 Summary summarizer 与 Runner。
本地检查的独立 session/redis v1.10.0 是源码缓存，不是项目依赖；它直接 Append/Summary
持久写入，不承接本项目的 accepted 可见性，因此没有直接替代 Attempt 私有 Session。
新 `sessionstore.Redis` 只实现已有 Store.Put/Load/Close，使用既有 go-redis/v9 v9.11.0。

## 正式可见性

`Runner/SDK 私有事件与Summary → 原Candidate.Encode → Redis不可变Put → PG Complete接受 → Final`

- Redis key 使用 `runtime_session:{tenant-hash}:session-hash:sc1_ref`，value 就是原
  Candidate canonical bytes；没有 Redis latest/head、没有另一套 completion receipt。
- `Head.Ref`、`Head.Digest`、Candidate.Identity/Parent/Snapshot 协议均不改变。
- Put 的单 Lua 在全部检查后只执行一次 SET；已存在时完整字节相等才可幂等返回，
  同 identity 不同 bytes 报冲突。Lua 不承诺失败回滚，因此不进行多阶段写入。
- 摘要正文、结构边界和事件处于同一 Snapshot、同一个 Redis key。PG ledger 中的
  accepted_ref/accepted_digest 仍是唯一正式入口，写入候选不等于正式接受。
- 指定的非空 Parent 在所选 Redis 中缺失时返回 `SESSION_INVALID`，模型零调用，
  不建空会话、不查询 latest、不回落旧 PG。网络/TLS故障与缺失/损坏的确定错误分开。
- 后端单次操作 timeout_ms 耗尽且外层 Attempt 仍有效时，Open/Load/Stage 返回可重试
  的 DEPENDENCY_UNAVAILABLE；外层 Attempt 取消仍按原取消语义处理，不把子超时
  错当永久 RUNTIME_FAILED。独立慢操作与 Processor retry 回归覆盖此边界。
- Factory 使用 Snapshot 固定 timeout、pool concurrency、容量上限；密码只作密码，
  不解释为 URL。客户端关闭沿原 Attempt 生命周期；开启 TLS 时验证证书与固定host。

## 配置与隔离

```json
{
  "config": {
    "storage": {
      "session": {
        "kind": "managed_session",
        "backend_id": "session-redis",
        "backend_revision": 1
      }
    }
  },
  "credentials": {
    "storage": {
      "session": {
        "dsn_password": {"action": "replace", "value": "REDIS_PASSWORD"}
      }
    }
  }
}
```

上述仅展示现有 Profile write 的相关字段，不是省略版本号等字段的完整保存请求。
固定 Backend Snapshot kind=redis、adapter=managed-redis-v1、isolation=tenant-session-v1、
username=session_runtime。host/port/database/TLS 来自后端目录固定快照，不由密码决定。
credential audience 为整个 Snapshot.Digest；keep/replace/clear、固定关联及发布后CAS
轮换沿原协议复用。managed PostgreSQL Session 本阶段没有开放；旧 postgres_state 仍可用。

Session 与 Memory 可使用同一 Redis 实例，但固定前缀和独立 ACL 用户分别为
`runtime_session`/`session_runtime` 与 `runtime_memory`/`memory_runtime`。数据库编号
不是 ACL 边界，prefix 也不代替可信 tenant/session 身份校验。此版为 standalone Redis，
不将它称为 Cluster/Sentinel 支持。

## Session 身份、Summary 开关和后端切换

现有 `Requested.Scope()` 包含 DeploymentRevisionID，因此**同一固定 Manifest 的多轮**
共享正式 Session；**新发布 Deployment revision 建立新 Session scope**。本阶段不改变
这条身份协议，也不把新的空 Session 描述为自动迁移或跨发布连续。

- 内部同一 Session overlay/候选恢复时，关闭摘要生成/消费可保留旧摘要 metadata；再次
  启用可使用已接受 metadata。这是原 SDK/候选层行为。
- 通过正式发布修改 Summary 或 Session backend 会产生新 Deployment revision；它使用
  新 Session，不自动继承旧版本摘要和历史。原版本的 accepted Session 不被删除。
- 选定后端已有数据的使用范围仅为精确 tenant/session/ref/digest。当前已有
  [多后端迁移 V1](backend-migration-v1.md) 可显式复制同一正式 Session 的 accepted
  candidate，但没有跨后端扫描，也不会把旧 Deployment snapshot 自动解释为新 Session。

## 持久性与后续计划

不设置 TTL。服务端 AOF/RDB、备份、磁盘、故障恢复与 `maxmemory-policy noeviction`
需要明确配置；验收采用 AOF always 并验证真实容器重启后 Summary snapshot 字节可读。
不承诺 Redis 与 PG ledger 跨库原子事务。未接受候选可能留在 Redis，不提供隐式回收；
旧 Memory PENDING 自动恢复、候选清理、历史迁移均是后续独立计划。

新增 platform managed_session adapter 改变 release digest，Control/Worker 必须协调使用
同一固定合同并发布新的部署版本；不改写历史发布物，不混投新 Redis Session Manifest
给旧 Worker。Artifact、Knowledge 尚属后续包；最终统一版本真实外部模型回归另行完成。

## 可复核入口与结果边界

```sh
services/agent-worker/sessionmigrations/test-redis.sh
python3 scripts/test-worker-session-redis-joint.py --race --artifacts /tmp/redis-session-joint
python3 scripts/test-worker-memory-web.py --backend redis --scenario session \
  --artifacts /tmp/redis-session-gui --coordination /tmp/redis-session-gui-coordination
```

存储合同是真实 Redis ACL/AOF/race，覆盖不可变冲突/重复、跨tenant/session、缺失、
wrongtype/TTL/ACL拒绝、TLS与取消。联合门禁使用真实 Control发布、Worker、Gateway、
PG accepted ledger、Redis、NATS与Channel Lab；仅外部模型使用已有确定性Summary fixture。

2026-09-09 联合已观察9个Lab输入、14个SDK HTTP请求、7份Redis不可变候选、5个精确摘要
版本：同Manifest三轮持续→摘要401不接受→后继恢复；临时隔离指定accepted key后
SESSION_INVALID/零SDK/零新候选/head不变，恢复原bytes后继续；新revision两轮是另一
Session，首轮无旧摘要。PG Session candidate表始终0行，不用SQL构造业务状态。

GUI入口复用同一Web进程生命周期，实际页面选Redis Session并发布，随后连续三轮核对
Worker route与该发布Manifest ID/digest/revision一致，Redis正式候选与下一模型摘要消费
匹配。此证据不等同于真实Telegram或真实外部模型验收。
