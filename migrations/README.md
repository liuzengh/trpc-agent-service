# 数据库迁移基线

本目录包含业务库的嵌入式 schema 迁移契约。`000001_service_schema` 是唯一的首次交付压缩基线：它同时包含核心表、索引、函数、触发器、权限、受控 purge 角色、迁移 pause/resume/abort 控制、租户 redaction rules 与 phase-intent journal。验收者从空的 PostgreSQL 16 数据库启动 Docker Compose 时，Runner 只应用该基线，并在 `schema_migrations` 中记录唯一的 `000001`。

该基线保留多租户控制面、Session/Event/Summary、Inbox/Outbox/Delivery 幂等、Channel/媒体、治理、审计、Redis→SQL 与 Local Vector→Remote Vector 迁移状态机所需的全部最终数据结构；它没有任何真实租户、会话、消息、密钥或模型配置数据。

`compliancemigrations/000001_compliance_schema` 是独立合规库的对应基线，包含不可变审计事实、保留策略、legal hold、quarantine resolution 与窄授权 purge 路径。两套数据库必须分别运行各自的 Runner。

## 演进规则

这是一份首次交付基线，不兼容本仓库压缩前的历史数据库。此后若继续开发，必须从 `000002_*.up.sql` / `000002_*.down.sql` 开始追加新版本：不得改写或删除已发布基线，Runner 会用 `schema_migrations` checksum 拒绝漂移。`down` 仅用于可丢弃的本地测试数据库；合规库在已完成销毁后会拒绝回滚。

本地从零验证入口是 `bash scripts/ci/admission.sh` 或 Compose 的 `runtime-test` profile。数据库卷如需重置，必须使用 [根目录 README](../README.md#快速开始) 中声明的显式 `docker compose ... down -v` 命令。
