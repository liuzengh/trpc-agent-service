# Database V1：同库、多 Schema、独立角色

- 日期：2026-09-07。
- 适用：当前 `worker` 分支的 V1 部署实现；不表示既有运行实例已迁移。
- 已实现：Compose provision、Control/Gateway/Worker 双连接启动、权限与迁移测试，Session 显式准备。
- Worker 账本、SessionCandidate Adapter 和真实 PG/NATS/SDK fixture 已实现；真实模型/Telegram 联合验收见 [Worker 状态](../agent-worker/implementation-status.md)。

## 1. 固定布局

复用一个 PostgreSQL 17 实例和一个 database，库名通过 PLATFORM_POSTGRES_DB 显式配置。
服务拥有独立的逻辑数据空间，不强制独立物理实例/database。

| Schema | Owner / migration login | Runtime login | Runtime 对自有业务对象 |
| --- | --- | --- | --- |
| control | control_migrator | control_runtime | SELECT / INSERT / UPDATE / DELETE |
| gateway | gateway_migrator | gateway_runtime | SELECT / INSERT / UPDATE / DELETE |
| worker | worker_migrator | worker_runtime | SELECT / INSERT / UPDATE / DELETE；Run/Attempt/Completion/Manifest |
| runtime_session | session_migrator | session_runtime | SELECT / INSERT；不可变 SessionCandidate |

数据库由独立管理员拥有。八个应用角色均非超级用户，无 CREATEDB/CREATEROLE/REPLICATION/
BYPASSRLS、无角色 membership；runtime 不是 Schema owner，也没有 Schema CREATE。
没有通过授予 migrator membership 来实现迁移/运行共享权限。

PUBLIC 不获得业务 Schema、业务表/序列权限；收回共享 database 的默认 CONNECT/CREATE/TEMP，
仅向应用角色明确授 CONNECT。收回 public Schema 权限；各角色的 search_path 固定为自己的
Schema 和末尾 pg_temp，pg_catalog 隐式优先。search_path 不是 ACL：限定其他 Schema 的查询
也必须被数据库拒绝。Schema 隔离不是租户隔离，业务仍按受信 tenant_id 限定数据访问。

## 2. Provision 与迁移顺序

1. 管理员连接已经创建的共享库，取得 provision advisory lock。
2. 先检查 legacy public 对象/旧 channel_gateway database、角色高权限/membership/database
   ownership、既有 Schema/对象 owner。发现不兼容即事务回滚，不先创建空的新业务空间。
3. 创建缺失的固定角色与 Schema，设定显式权限与 role 默认 search_path。不接管既有 owner，
   不覆盖已存在角色的密码；重复运行不改变业务内容。
4. 默认权限按实际 `*_migrator` 建表角色设置，覆盖未来业务表与序列。函数不向 PUBLIC 默认
   授 EXECUTE。已有对象权限同步收敛，迁移账本在同一 provision 事务内排除 runtime 写权限。
5. Control/Gateway/Worker 各自两个连接首先核对配置 host/port/database 与实际 database/schema，
   必须同目标且不同身份，并绑定所属 Workload 的固定 Schema/角色。两连接均要求直接登录
   身份（session_user=current_user），迁移角色必须是普通 Schema owner；拒绝管理员、角色伪装、
   public/系统 Schema、高权限 runtime 和另一 Workload 的 DSN，之后才开始迁移。
6. 历史 migration SQL 不变；ledger 在所属 Schema 中维护。Control/Gateway/Worker 使用各自 advisory
   lock；ledger 创建、runtime 撤权和迁移在同一受锁事务内完成。
7. 关闭迁移 pool，App 只保留 runtime pool；启动后再以 runtime 检查 ledger 没有写入/DDL相关
   权限（包括 PG17 MAINTAIN）。配置与数据库连接错误不输出 DSN 或底层凭据详情。

迁移依然由各 Workload 启动时执行，没有新增另一个 migration owner；后续若转独立 Job，
必须一次性移除该 Workload 的启动迁移。当前分开的是数据库权限身份，不宣称同一进程对迁移
凭据具有进程级隔离；需要更强隔离时再调整部署生命周期。

## 3. Session Store 保持既有消费契约

运行账本与 Session 内容可以同库不同 Schema，但分别使用 Worker 部署连接与独立 Session
连接。同库不授权跨服务 SQL，不改变 Manifest、Attempt 授权或 Final 证明。Profile Storage
管理 API 接受受限 DSN，仍只允许现有 `sslmode` 参数；Control 解析后仅加密 password，
并固定发布非秘密 destination。runtime `purpose=dsn` 的授权值是密码，Worker 按固定
Manifest 的 host/port/database/username/sslmode 与该密码转义组装 Session DSN，不把返回值
当完整连接串，也不借 Worker 自有 DSN 替换目标。Schema 通过部署固定的 Session role 默认值
或受信 Adapter 选择，不向用户开放任意 SQL namespace。

runtime_session 的默认对象权限只有 SELECT/INSERT。Worker V1 已实现固定候选键/摘要幂等和
完整性校验，使用 `prepare-session` 以 `session_migrator` 显式准备业务表。发布 gate 与运行
Adapter 均要求 target username 为 `session_runtime`。内容先耐久、Completion 接受引用已
通过真实 PostgreSQL spike；物理同库不自动合并两条连接的事务。

## 4. 已有数据与升级

这是新部署/明确完成数据迁移后的 V1 基线，**不是自动迁库脚本**。当前运行卷不会因修改
POSTGRES_DB/POSTGRES_USER 环境变量而自动变成新布局；不要通过删卷来绕过 legacy guard。

有旧数据时，在维护窗口单独制定并验证：备份与还原演练 → 停止相关写入 → 将业务对象、
序列、约束、函数及 migration ledger 一起迁入对应 Schema → 校对数据/owner/ACL/历史
migration Digest → 配置新 DSN → 服务读写与拒访问验收 → 切换。迁移前后保持 Profile Key，
不修改已执行 SQL 文件来绕过 checksum。这里只记录迁移前提，未执行或交付通用自动搬迁器。

既有 database 名可保留；统一 schema 布局不要求重命名。若现网暂保留 Gateway 原独立库，
需使用其显式独立部署配置，不把默认 Compose 的同库验收外推到旧实例。

## 5. 可重复验收

```bash
python3 scripts/test-v1-database-isolation.py
```

脚本自行创建/删除独立 PG17.6 与 NATS Docker 实例，固定回环端口映射，不读取现有业务 DSN。
NATS 仅用于现有 Gateway 真实启动/恢复回归，并只对这个临时 broker 开启测试重置。
测试实例使用隔离的临时认证配置；生产 Compose 仍强制独立密码与连接身份。

必须运行并通过：首次 provision、相同输入重跑、实际 migration 后再 provision、未来对象
默认权限、runtime 正常 DML/序列、runtime DDL/TEMP/TRUNCATE/SET ROLE 拒绝、所有跨 Schema
限定名读写拒绝、ledger 写权限拒绝、Control 并发首迁移、错误目标在 DDL 前拒绝、legacy
对象与异常角色 guard。专用 Go 合同测试无匹配或 Skip 会令脚本失败；脚本也执行完整 Control 集成门禁，以及
Gateway bootstrap/migrations 的真实 PG/NATS 回归，后者同样要求零 Skip。

普通 `go test ./...` 与此真实 PG 门禁分别记录。更完整的 Control 集成门禁仍由
`scripts/test-control-integration.sh` 负责，要求 Control 管理测试 DSN、Control 合同测试的 admin/migrator/runtime 三个 DSN，以及
Gateway migrator/runtime 两个跨 Workload 负例 DSN，以免新合同测试被 Skip。

## 6. 后续边界

本次不新增独立数据库服务器、租户 Schema 平台、跨 Schema 共享 Repository、
Session projector 或费用账本。CPU/IO/WAL/备份恢复仍共享；业务规模、故障或恢复要求需要时
再拆物理实例。Worker 的 Memory/组合/Token 后续范围保持不变。
