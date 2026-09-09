# 增量 schema 迁移

`deploy/db/init.sql` 是**冻结的 000001 基线**：只在空库上执行一次（compose 的
initdb.d、k8s 的 `db-init` Job、CI 三处），末尾把 `schema_migrations` 标到版本 1。
此后所有 schema 变更都写在本目录，由 [`../migrate.sh`](../migrate.sh) 通过
[golang-migrate](https://github.com/golang-migrate/migrate) 应用。

**不要改 init.sql 里的 DDL 来变更已存在的 schema。** 那会让新建库和线上库悄悄分叉：
新环境拿到改过的结构、老环境停在旧结构，而两边都报告"schema 已就绪"。

## 加一个迁移

```bash
./deploy/db/migrate.sh new add_tenant_region   # 生成 000002_add_tenant_region.{up,down}.sql
$EDITOR deploy/db/migrations/000002_*.sql
./deploy/db/migrate.sh up                      # 本地验证
./deploy/db/migrate.sh version                 # 应为 2
```

约定：

- `up` 与 `down` 必须成对存在，缺一则 migrate 拒绝运行整个目录。
- 版本号从 **000002** 起（000001 是 init.sql），`migrate.sh new` 会自动接号。
- `up` 只做增量：加表、加列、加索引。破坏性变更按 design.md 第 8 节的回滚方案拆两步——
  先加新列双写、数据迁移完成后，再用**另一个**迁移删旧列。
- 加索引在大表上用 `CREATE INDEX CONCURRENTLY`。注意它不能在事务里跑，需要该迁移文件
  单独声明（golang-migrate 逐文件执行，`up` 里只放这一条语句即可）。
- 涉及新表的，记得同步 `docs/design.md` 5.1.2 的表总览。

## 各环境怎么应用

| 环境 | 基线（000001） | 增量 |
|---|---|---|
| 本地 compose | `docker compose down -v && docker compose up -d` 时 initdb.d 自动跑 | `./deploy/db/migrate.sh up` |
| CI | workflow 的 `Init DB schema` 步骤 `psql -f deploy/db/init.sql` | 每次 run 都是新库，基线即最新；有增量时在该步骤后追加 `migrate up` |
| k8s | `db-init` Job（幂等，`tenant` 表已存在则跳过） | 发布前对目标库跑 `migrate.sh up`，或加一个同形态的 migrate Job |

生产建议：迁移与应用发布解耦——先在低峰期跑完 `up`，再滚动更新镜像。schema 只做增量
意味着旧代码能跑在新 schema 上，回滚镜像不需要回滚 schema。

## 接管一个"没有 schema_migrations"的老库

本次机制引入之前由 genesis 脚本建好的库，表结构等于版本 1 但没有版本记录。直接
`migrate up` 会让 migrate 新建一张空的 `schema_migrations` 并从 0 开始计数。用
`force` 把它对齐到实际结构：

```bash
./deploy/db/migrate.sh force 1
./deploy/db/migrate.sh version   # 1 (干净)
```

## dirty 状态

迁移执行到一半失败会把 `dirty` 置为 true，此后所有命令都拒绝运行。先查清库的**实际**
结构改到了哪一步，手工补完或回退，再 `force` 到对应版本清掉标记。不要盲目
`force` 到失败前的版本然后重跑——已经执行过的 DDL 会二次报错。

## 为什么 migrate CLI 不在 go.mod 里

服务本身不 import 它。加进去会把整个 migrate 模块树拖进二进制的 module graph，
换来一个只在部署时用的命令行工具。装一次即可：

```bash
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```
