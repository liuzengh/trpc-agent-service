# 在线数据迁移

`cmd/data-migrate` 驱动持久化在线迁移协调器。生产 Worker 装配 Session、Knowledge、Artifact 数据访问装饰器，Summary Worker 使用相同的 Session 装饰器；`cmd/migrate` 负责独立的 PostgreSQL schema 迁移。

迁移单位为 `(tenant_id, domain)`。后端引擎决定数据访问实现，连接配置（profile）决定源目标实例及命名空间；相同引擎的不同实例也需要执行完整迁移协议。

## 支持矩阵

| 数据域 | 源目标范围 | 迁移内容 | 迁移准入条件 |
| --- | --- | --- | --- |
| `session` | Redis / PostgreSQL，可跨引擎或跨实例 | 官方 SDK Session-owned State、按序 Event 与 Track | 拒绝 App/User shared state、SDK native summary、TTL，以及达到配置安全上限的 inventory/history |
| `knowledge` | Qdrant endpoint 或 collection 之间 | 租户/应用作用域的 document、vector、content 与 metadata | embedding 定义及向量维度兼容；目标租户命名空间为空 |
| `artifact` | S3/MinIO endpoint 或 bucket 之间 | 精确版本正文、版本身份及 tombstone | 目标对象命名空间为空；PostgreSQL 版本元数据继续由平台持有 |
| Memory | 在线迁移未实现 | 后端引擎与连接配置可在首次配置时选择 | 数据迁移需采用受控离线导出/导入及专用受审计绑定变更流程 |
| 平台 Summary checkpoint | 保持 PostgreSQL 权威 | 不随 SDK Session store 迁移 | 通过 Session 读取 overlay 提供给 Runner |

所有支持的数据域在创建时校验源目标实际存储身份不同、连接配置授权、目标租户命名空间为空，以及租户当前 backend/profile 与 `config_version`。两个 profile 别名指向同一存储时拒绝创建。规范记录的 key 最大 4096 字节，payload 最大 16 MiB；inventory/history 的具体上限由后端适配器的安全配置确定。

Knowledge 数据访问契约保留已安装 SDK 的操作能力：Qdrant 支持单文档 `Add` / `Update` upsert、读取、检索和过滤删除；`UpdateByFilter` 返回 unsupported-operation 错误。使用 `Get` 后再以完整 document/vector 调用 `Add` 或 `Update` 时，两次调用各自执行，不构成原子读改写。

## 部署前置条件

1. 执行完整 schema 至 `047`，再部署所有 Worker、Summary Worker 与 Consumer 副本。045 提供在线迁移表，046/047 提供摘要目标刷新与 Session 代次绑定；047 改变写入唯一键，已有部署按 breaking migration 流程排空并停止写入后升级。全部数据访问副本必须支持迁移装饰器与协调 gate。
2. 创建迁移前，向迁移进程和所有写入副本发布源目标连接配置。跨节点定义必须一致且在进程内保持不可变；回滚窗口内保留两端配置和凭据。profile 的租户 allowlist 持续生效。
3. 迁移进程需要 `DATABASE_URL`、`STORAGE_BACKEND_PROFILES`、`DATA_PLANE_PROFILES`，以及清单所引用的秘密环境变量。生产数据库连接要求 TLS；控制数据库连接池至少允许 3 个连接，命令配置的最大连接数为 10。
4. PostgreSQL 连接级 advisory lock 要求直连或 session pooling；transaction/statement pooling 不受支持。加锁、数据库校验和解锁必须落在同一物理连接。
5. 所有数据变更必须通过平台装饰器。直接 SDK 客户端、维护脚本及外部写入绕过 intent/journal，必须在迁移窗口内停用或接入同一协议。普通 Tenant 更新拒绝已有 backend/profile 改绑；迁移期间也不得通过其他路径修改连接配置。
6. 预留全量校验期间的请求阻塞窗口、同步镜像的网络和目标容量，以及故障恢复所需的 intent/journal 保留空间。准入校验成功后仍需验证目标环境的延迟、故障转移与恢复容量。

## 创建与推进

Compose 的 `operations` profile 提供迁移命令镜像。操作前确认目标栈的 schema 迁移已完成，PostgreSQL 和 Redis 健康。下面示例使用隔离栈的本地 Redis/PostgreSQL 连接配置；迁移容器获取数据面凭据，IM 与聊天模型凭据无需注入。

在源码根目录准备受保护的 `deploy/.env`，字段参考 `deploy/.env.example`，填写**已有目标栈正在使用的值**。Compose 会先解析完整配置，需要 `POSTGRES_PASSWORD`、`MASTER_KEY`、`SERVICE_AUTH_SECRET`、`ADMIN_API_TOKEN` 和 `GRAFANA_PASSWORD` 等插值变量；这些变量与直接执行 `cmd/data-migrate` 所需的进程变量不同，仅设置 `DATABASE_URL` 不足以运行 Compose 示例。

`COMPOSE_PROJECT_NAME` 必须与启动目标栈时的项目名完全一致。使用 `run_c_local_stack.ps1 -ProjectName agent-platform-review` 时，项目名就是 `agent-platform-review`；脚本结束后会恢复其临时环境变量，因此运维进程仍需提供同一组验证配置。若启动时覆盖了端口，环境文件中的 `PLATFORM_*` 端口也应保持一致。其他 Compose 部署应将下列项目名、环境文件和 `-f` 配置文件集合替换为该部署的实际值。

```bash
COMPOSE_PROJECT_NAME=agent-platform-review
COMPOSE_ENV_FILE=deploy/.env
migration_compose=(docker compose
  --project-name "$COMPOSE_PROJECT_NAME" --env-file "$COMPOSE_ENV_FILE"
  -f deploy/docker-compose.yml -f deploy/docker-compose.isolated.yml
  --profile operations)

"${migration_compose[@]}" config --quiet
"${migration_compose[@]}" build data-migrate
"${migration_compose[@]}" run --rm --no-deps data-migrate \
  create --id tenant-a-session-1 --tenant tenant-a --domain session \
  --source-profile local-redis --target-profile local-postgres \
  --source-backend redis --target-backend postgres --config-version 7 \
  --actor operator-name --reason "replace session backend"
"${migration_compose[@]}" run --rm --no-deps data-migrate \
  run --id tenant-a-session-1 --batch-size 100 --timeout 30m
"${migration_compose[@]}" run --rm --no-deps data-migrate \
  status --id tenant-a-session-1
```

`--no-deps` 复用已启动的数据库和网络，不启动或重建依赖服务；连接失败时先检查目标项目与基础设施。`tenant`、迁移 ID、源目标 backend/profile 及预期 `config_version` 应替换为实际值。创建事务校验当前配置，陈旧版本会被拒绝。批大小为 `1..1000`；全量校验扫描受命令 deadline 约束，其工作量不受单次 copy batch 限制。

| 命令 | 行为 | 停止与恢复条件 |
| --- | --- | --- |
| `create` | 校验配置、源目标与目标空命名空间，建立 route 并启用捕获 | 首次 inventory 复制前即开始记录增量 |
| `run` | 持有可续约迁移 lease，逐步推进状态机 | 到 `ROLLBACK_WINDOW` 停止；暂停、终态或错误同样停止；不会自动 `complete` |
| `step` | 推进一步状态机 | 适合受控维护；以同一 ID 重复调用继续持久化进度 |
| `status` | 查询迁移状态 | 核对 phase、paused、watermark、last_error 与恢复条件 |

以下单步和运维动作以已安装的 `data-migrate` 二进制为例，进程变量按部署前置条件注入；使用 Compose 时，将 `data-migrate` 替换为 `"${migration_compose[@]}" run --rm --no-deps data-migrate`，保留同一组目标参数。单步命令示例：

```bash
data-migrate step --id tenant-a-session-1 --batch-size 100 --timeout 30m
```

Kubernetes 部署使用 `deploy/Dockerfile.data-migrate` 构建镜像并固定 digest，在运维控制的 Job 中运行 `/app/data-migrate`。Job 需要相同的 profile ConfigMap 与最小范围存储 Secrets，并设置明确的 namespace、`restartPolicy: Never`、`backoffLimit: 0`、deadline、非 root 用户、只读根文件系统、禁用 ServiceAccount token 挂载，以及到控制数据库和源目标存储的受控网络访问。数据迁移 Job 独立于 schema Job 和应用发布 bundle allowlist；以相同 ID 重试 `run` 时继续持久化状态。

## 持久化写入协议

创建操作建立租户数据域 route，并在 inventory 复制前启用捕获。每次经装饰器的数据访问先取得共享 PostgreSQL advisory gate 并读取 route；mirroring 活跃时，释放共享 gate、取得排他 gate，再次读取 route。不同租户和不同数据域使用独立 gate。

单次变更遵循以下顺序：

1. 恢复并排空此前未完成的写意图与待投影记录，防止新变更越过未完成的删除/重建边界。
2. 将全部受影响记录身份作为 intent 提交到 PostgreSQL。
3. 调用源后端执行变更，再读取源端规范状态。
4. 追加不可变、有序 journal 记录；删除以独立 tombstone 表达。
5. 将记录应用到目标，实际读回并比对 payload/hash/deleted 后，才写入 `projected_at` 并推进相应进度。

Intent 在请求取消、进程失败和源响应结果未知时仍保留。后续数据访问或协调步骤从源重新捕获并恢复投影；Artifact 源删除部分失败时，先恢复该版本的源清理，再允许后续重建。

**调用返回失败时，源后端可能已经提交。** 源存储与 PostgreSQL intent/journal 分属不同系统，业务重试必须结合状态、持久化意图与后端幂等契约处理，禁止仅依据 HTTP/调用错误判断“未写入”。

## 复制、校验与切换

Snapshot 从源后端原生 inventory 发现迁移创建前的数据，复制 cursor 和 watermark 仅在投影完成后推进。复制、追平、校验和回滚窗口期间的新变更统一进入同一 journal。

| 阶段 | 校验与状态变更 | 数据访问路由 |
| --- | --- | --- |
| Snapshot / Catch-up | 复制 inventory、追平有序 journal | 读源、写源并同步镜像目标 |
| `VALIDATE` / `READ_SHADOW` | 全量 inventory、规范记录及删除身份比对 | 继续捕获与镜像，校验由排他 gate 保护 |
| Cutover | 排空未完成写入并重新验证两端；同一事务提交租户配置 CAS、route、phase、lease/fence 校验与审计 | 进入回滚窗口 |
| `ROLLBACK_WINDOW` | 保留源作为恢复基础，持续同步镜像 | 读目标；写源后镜像目标 |
| `complete` | 最终全量验证，原子完成路由及状态变更 | 读写目标，停止镜像，保留源数据 |
| `rollback` | 验证源可用并恢复源侧意图，原子恢复源路由 | 读写源，停止镜像 |

`READ_SHADOW` 的验证对象是完整规范记录及 inventory。实时相似度检索、排名、应用响应及真实查询流量的采样比对未接入该阶段，检索质量应单独验收。

缓存 Worker 客户端每次调用都读取持久化 route，已创建实例会跟随切换。活跃迁移按 tenant/domain 串行化数据访问；全量校验和最终 cutover 的扫描期间可能阻塞该作用域请求。同步镜像增加目标延迟和故障依赖，维护窗口与客户端 deadline 应覆盖相应开销。

## 运维操作与故障恢复

`create`、`pause`、`resume`、`abort`、`rollback`、`complete` 要求 `--actor` 和 `--reason`。`step` / `run` 的租约推进使用创建时保存的审计身份。

```bash
data-migrate pause --id tenant-a-session-1 --actor operator-name --reason "inspect lag"
data-migrate resume --id tenant-a-session-1 --actor operator-name --reason "backend recovered"
data-migrate abort --id tenant-a-session-1 --actor operator-name --reason "cancel before cutover"
data-migrate rollback --id tenant-a-session-1 --actor operator-name --reason "target degraded"
data-migrate complete --id tenant-a-session-1 --actor operator-name --reason "observation accepted"
```

以上为分别适用的运维动作，应按当前状态选择执行。`abort` 仅适用于 cutover 之前，`rollback` 仅适用于回滚窗口。

| 场景 | 操作 | 必要条件与恢复结果 |
| --- | --- | --- |
| 暂停协调推进 | `pause`；检查后 `resume` | 捕获与同步镜像继续，源变更仍纳入恢复协议 |
| 切换前取消 | `abort` | 当前路由仍读源、租户数据域仍绑定源；停止 mirroring，保留源数据与审计。与数据域无关的配置版本变更不阻止取消 |
| 瞬时后端故障 | 修复依赖，以相同 ID 重新 `run` / `step` | 先核对 `status`、`last_error`、pending intent/journal；协调器恢复后继续，不手工标记 projected 或推进 cursor |
| 回滚窗口内目标不可用 | `rollback` | 仅解析并验证源后端，恢复源侧意图，不依赖目标在线。源身份必须与 route 固定身份一致，租户仍为 active 且该数据域仍指向预期目标；其他配置字段保留 |
| 回滚前已暂停 | `resume` 后 `rollback` | 路由切换要求迁移未暂停；恢复推进与回滚仍受 lease/fence 约束 |
| 源不可用或源意图无法恢复 | 先修复源，保留协议记录 | 回滚不能绕过源读取/恢复验证；目标不可用不降低源正确性要求 |
| 观察窗口结束 | `complete` | 两端最终验证成功后读写目标；旧缓存继续由保留的终态 route 路由 |
| 完成后反向迁移 | 创建新的迁移任务 | 原回滚窗口已结束，新的目标租户命名空间必须为空 |

查看恢复积压的 SQL：

```sql
SELECT migration_id, count(*) AS unresolved_intents
FROM data_migration_live_intents GROUP BY migration_id;
SELECT migration_id, count(*) AS pending_records, min(created_at) AS oldest_pending
FROM data_migration_live_journal WHERE projected_at IS NULL GROUP BY migration_id;
SELECT id, tenant_id, domain, phase, paused, applied_watermark, last_error
FROM data_migrations ORDER BY updated_at DESC;
```

`complete`、`abort`、`rollback` 均保留源存储。终态 route 服务已有缓存客户端；旧数据及 intent/journal 的保留、备份和清理由独立运维流程安排，在仍需故障恢复或回滚时禁止删除协议记录。

## 验证入口

`test/integration/online_session_migration_test.go` 与 `online_dataplane_migration_test.go` 使用生产装饰器和协调器，在 PostgreSQL、Redis、Qdrant、MinIO 上验证捕获、规范记录投影、路由及故障恢复。Qdrant 集成同时验证 unsupported `UpdateByFilter` 保持源目标文档不变，以及复制后受支持的 upsert 能同步到目标。

执行提交、日志和目标环境验收条件见 [验收证据](ACCEPTANCE_EVIDENCE.md)；数据表、外键、记录边界和持久化类型见 [数据模型](DATA_MODEL.md)。
