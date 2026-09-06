# 代码缺口补齐进度

范围：用户要求补齐 README 对照中确认的六类代码缺口；不扩展到管理 UI、所有 IM/Agent 类型或生产账号配置。以下仅记录本轮新工作，不把既有能力重复算作新增。

- [ ] 模型/工具/存储操作耗时指标及错误统计。
- [ ] 模型调用预算预留、幂等结算、后台模型用量记账。
- [x] 跨节点自动 Memory 水位单调推进：migration 017 的独立进度表，纳秒精度、原子 GREATEST、旧 Session 值首次导入、任务作用域校验；内存并发及独立 PostgreSQL 跨实例测试通过。
- [ ] 租户审计策略的解析、过滤、保留期和失败处理。
- [ ] Knowledge 历史回填、自动校验及迁移门禁。
- [x] Redis 队列长任务保活、所有权确认、未确认消息保留与背压：心跳、丢失所有权取消、原子 ACK/Retry、安全历史清理、容量拒绝；单元及独立 Redis ACL 测试通过。

开发期间不修改日常 `.env`、不重启业务进程、不迁移业务库或操作真实 IM。使用单元测试和独立容器/测试 schema 验证。新增持久化表与权限将一起提供，实际启用单独说明。不会自动推送 GitHub。

## 已实现部分的兼容性

- 队列 Stream 现在只允许一个平台 consumer group；消费实例名自动加随机后缀，避免同配置节点共享所有权。ACK 会原子删除已确认的记录，Publish 只清理已确认前缀，容量满时任务留在 SQL Outbox。不得把同一个 Stream 当作多个独立订阅组的广播源。
- Redis ACL 新增限定 Stream 键空间的 XINFO/XPENDING/XCLAIM/XLEN/XTRIM/XDEL 和 Lua 操作；未应用到日常 Redis。
- Memory 进度以 `background_watermark` 为真相，不再用无条件的 Session state 写回。旧 Session 水位仅在首次读取时导入；自动提取明确拒绝 update/delete/clear。
- 部署时须先停旧消费者/Jobs、应用迁移与权限，再启新版本；不能混跑仍使用无保护 ACK/MAXLEN 或旧 Session 水位的消费者。
