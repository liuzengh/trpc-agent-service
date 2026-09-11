# Worker 多后端迁移 V1

## 1. 本次交付边界

V1 只迁移当前 Worker 已经具有可互换实现的数据能力：

| 能力 | 源/目标 | 迁移单位 | 保持不变的正式身份 |
| --- | --- | --- | --- |
| Session（含 Summary） | PostgreSQL、Redis | 已接受的完整 Session candidate | `Head.Ref`、`Head.Digest` |
| Memory | PostgreSQL、Redis | 一个租户作用域的当前完整 Snapshot | `Scope`、`Revision`、内容摘要 |

Summary 已包含在 Session Snapshot 中，因此随 Session 一起复制，不建立独立迁移协议。
Artifact 当前固定为 S3 内容加 Worker PostgreSQL 元数据，Knowledge 当前固定为 Qdrant；
它们还没有第二个可互换 Adapter，本次不伪造“任意后端互迁”。后续只有在同一能力出现
第二个真实 Adapter 时，才复用本设计增加对应迁移器。

本次不保存旧版本历史：

- Session 只复制 Worker 正式 `accepted_head` 指向的完整候选，不复制未接受 orphan，
  也不追溯复制 parent 链；完整候选足以恢复下一次运行所需的 Session 与 Summary。
- Memory 只复制当前 head 的 Revision 和完整 entries，不复制旧 revision 或接受收据。
- 迁移不会修改历史 RuntimeManifest，也不会自动切换 Deployment。

## 2. 产品纵向切片与调用顺序

Control 仍负责把选定后端编译进新的不可变 RuntimeManifest。Worker 负责数据能力和
具体 Adapter。首个产品入口只搬迁 Memory；底层 Session 复制能力保留给同一正式 Session
的物理恢复，不用于跨 DeploymentRevision 续接会话：

```text
Web 读取来源 DeploymentRevision、目标 AgentVersion/ProfileRevision 和 ChannelBinding
  -> Control 编译并校验目标 RuntimeManifest，但尚不发布
  -> Control 按来源和目标 Profile 的固定 credential association 解析本次密码
  -> Control 通过 mTLS 调用 Worker 内部迁移入口
  -> Worker 对租户取得 admission fence，拒绝活跃 Run/待应用 Memory
  -> Worker 从租户正式账本的全部已接受 Run 派生该 Agent 的 Memory scopes
  -> PostgreSQL/Redis Adapter 复制当前 Memory snapshot 并回读校验
  -> 全部成功后 Control 以既有事务、Outbox、幂等键发布新 DeploymentRevision
  -> Web 使用 expected_binding_revision CAS 切换 ChannelBinding
```

迁移模块不扫描 PostgreSQL 表或 Redis key，不从 Draft/Profile 猜测作用域，不自行解析
用户输入的连接地址，也不修改正式 Worker 账本。这样后端的物理枚举方式不会泄漏到
Session/Memory 迁移语义中。

调用方必须在取得根之前停止相关写入，并确认没有待应用的 Memory completion。V1 不做
在线双写、增量追赶或跨后端分布式事务。未满足停止线时不得执行切换。

## 3. 深模块接口

入口位于：

`services/agent-worker/internal/execution/adapter/outbound/backendmigration`

调用方提供 `Plan`：

- `Sessions[]`：TenantID、SessionID、Worker 已接受的非空 Head；
- `Memories[]`：Worker 已派生并使用的 Tenant + Scope ID；
- source/target：已通过现有固定 target、运行身份、TLS、凭据和容量校验的 Store。

`Migrate` 先验证整个 Plan，拒绝空计划、重复 Session、重复 Memory Scope、无效 Head 和
缺失 Adapter。执行时 fail-fast，返回此前已经完成回读校验的计数；失败后可用同一 Plan
重试。

### Session

1. 按显式 Tenant、Session、Head 从 source 加载完整 candidate；
2. 用 target 的普通不可变 `Put` 写入；
3. 要求 target 返回完全相同的 Head；
4. 从 target 按同一 Head 回读，并比较 canonical bytes 和 Head。

Session candidate 是内容寻址且不可变的，相同写入是幂等重放，不同内容占用相同正式键
会冲突。目标已有其他 candidate 不妨碍迁移；迁移只复制正式根指定的 candidate。

### Memory

1. 按显式 Scope 从 source 加载当前 Snapshot；
2. target 的 `ImportSnapshot` 只允许写入空 Scope；
3. 相同 Revision 与相同 canonical 内容的重放成功；
4. 目标已有不同 Revision 或内容时返回冲突，绝不覆盖；
5. 回读 target，比对 Revision 和与正常接受写入共用的 SnapshotDigest。

PostgreSQL 使用作用域 advisory transaction lock、`FOR UPDATE` 与单事务 INSERT。
Redis 使用单个 Lua 脚本检查 key 类型、禁止 TTL、比较旧值并以既有 `MSET` 权限原子写入。
两者都沿用现有租户作用域编码、容量上限和运行角色，不增加后端扫描权限。

## 4. 幂等、失败和切换规则

- **重放**：源不变且目标已有完全相同值时成功。
- **目标冲突**：目标 Scope/正式不可变键存在不同内容时停止；不覆盖、不合并。
- **源变化**：由于调用方必须先冻结写入，源变化表示停止线失效；重新取得计划后重做。
- **部分完成**：结果计数只包含已经回读校验的单元；未完成项可用原 Plan 重试。
- **切换**：全部复制和回读校验成功之前，不发布/切换到新 Manifest。Memory Scope 不含
  DeploymentRevision，因此可在显式迁移后由新 Manifest 继续使用；SessionID 含
  DeploymentRevision，当前精确 Head 复制只支持同一正式 Session 的物理搬迁/恢复准备，
  不能被描述成新 Deployment 自动续接历史。
- **回退**：切换后如需回退，重新冻结写入，以当前正式后端为 source、旧后端为 target；
  旧后端若已有不同 Memory head 会冲突，因此 V1 回退前应使用新的空目标或明确重建，
  不做隐式双向合并。

## 5. Control 与 Web 契约

- `POST /v1/tenants/{tenant}/deployments/{deployment}/backend-migrations` 是 OWNER 操作；
  输入包含来源 revision、当前 latest CAS、新的确定 DeploymentInput，并要求 Idempotency-Key。
- Control 不把密码写入 RuntimeManifest、DeploymentRevision、Outbox 或公开响应；密码只在
  Control→Worker 的 mTLS 请求中短暂存在，Worker 打开 Adapter 后清除请求字段。
- Worker 内部入口为 `/internal/v1/backend-migrations:execute`，只接受 Control 工作负载身份。
- 迁移失败或来源繁忙时不调用发布；迁移成功后复用既有发布事务。若随后发布 CAS 冲突，
  目标中的相同 snapshot 可安全重放，但操作者必须读取最新发布基线后重新决定。
- Web 路由 `/tenants/{tenant}/deployments/{deployment}/migrate?source={revision}` 展示迁移、
  发布、渠道切换三个阶段。渠道切换失败时保留已发布的新版本，只重试 ChannelBinding CAS，
  不重复迁移和发布。

## 6. 已实现验收

快速测试：

```sh
GOCACHE=/tmp/backend-migration-gocache go test \
  ./services/agent-worker/internal/execution/adapter/outbound/backendmigration \
  ./services/agent-worker/internal/execution/adapter/outbound/memorystore \
  ./services/agent-worker/internal/execution/adapter/outbound/sessionstore
```

真实双后端测试：

```sh
GOCACHE=/tmp/backend-migration-gocache \
  services/agent-worker/backendmigration/test-postgres-redis.sh
```

第二个测试建立一次性 PostgreSQL 17 和 Redis 7.4，使用各自最小运行/迁移身份，真实执行：

- PostgreSQL Session + Memory → Redis；
- Redis Session + Memory → PostgreSQL；
- 两个方向均重放一次；
- 校验 Session Head/内容、Memory Revision/内容与租户隔离；
- 结束后删除两个容器和临时 ACL 文件。

这些测试证明 Adapter 间真实复制语义；Control/Application/HTTP 和 Web component 测试另外
验证“迁移成功才发布”及“发布成功后才切换”。V1 仍不是在线零停机迁移：它通过 Worker
admission fence 和来源静默检查形成短暂停写窗口，不实现双写、增量追赶或自动回滚。
