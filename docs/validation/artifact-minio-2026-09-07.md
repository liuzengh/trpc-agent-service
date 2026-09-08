# 本地 MinIO 附件持久化启用记录

日期：2026-09-07。用户授权继续开发和配置，直到需要本人发消息。程序继续使用包含附件锁修复的 rc.4，schema 23；没有新增数据库迁移。

## 实际变更

- 使用仓库已有的 tRPC-Agent-Go `artifact/s3` 适配与 `ArtifactRouter`，没有另写一套 S3 文件协议。
- 启动本地 MinIO，保留既有 `trpc-agent-service_minio-data` 数据卷与原有桶。API/控制台仅映射 `127.0.0.1:9000/9001`，未增加公网路由或自动启动。
- 创建 `trpc-agent-artifacts-tutorial` 专用桶及随机专用账号，策略仅允许该桶的 ListBucket/GetBucketLocation 和对象 Get/Put/Delete。Agent 不使用 MinIO 管理员凭据；已验证专用账号访问目标桶成功，访问其他桶的元数据被明确拒绝。
- 凭据 JSON 保存在 `.env` 的 `MINIO_ARTIFACT_CREDENTIALS`，增加 `tutorial-tenant`、`purpose=artifact`、`env://MINIO_ARTIFACT_CREDENTIALS` 的精确授权。保留模型、IM 凭据和其他授权，未输出新凭据。
- 活跃 Artifact binding 改为 `tutorial-artifact-minio-v1`：S3-compatible、endpoint `http://127.0.0.1:9000`、region `us-east-1`、path_style=true。旧 `tutorial-artifact-backend` 保留为 retired，version 1 → 2。
- 源绑定退役、目标绑定创建及 `admin_artifact_backend_cutover` 审计在同一个 PostgreSQL 事务提交，执行前停止旧 Agent、检查未完成 Run 和附件清单。其他后端、会话 ID、Agent revision、历史 Run 和 IM 绑定不变。

## 原附件如何保留

这次只涉及已核对的 **1 个测试附件**。旧运行进程没有在线 Artifact 导出接口，因此不是对其内存做通用全量导出：使用成功请求 `req_e2198df8f33982dcdcdf4e978552946d` 保存的 Telegram 文件引用取回原文件，与仓库 `examples/attachment-test.txt` 逐字节比较，215 字节完全一致。

恢复文件先写入 MinIO，在停止旧 Agent 前核对原 `att_...` 编号、版本 0、MIME 和全部字节。重试使用 `SaveArtifactOnce`，不会增加版本。没有重放旧请求、重新执行模型/业务工具或发送 IM 消息。切换前再次检查已知附件清单；如果出现额外附件就中止，而不是假定这一个样本代表任意生产全量。

私有备份目录为本机 `data/minio-upgrade.xLZwPSvy/`（0700），含配置、原程序、数据库 dump、Redis RDB、恢复字节、manifest、凭据和校验清单（敏感文件 0600，Git 忽略）。已校验 dump 目录、文件 SHA-256 和恢复字节；本次没有把业务数据库备份实际恢复到另一套库。

不能简单把 retired 的 InMemory 绑定重新激活当作数据回滚：旧进程已退出，其内存已经释放。恢复应使用留存的文件、对象存储与控制面备份，并核对切换之后的新写入。

## 已验证

1. MinIO 加入健康检查，`start-real.sh` 等待已运行的 MinIO 健康。容器就绪不代替租户凭据和对象可读性检查。
2. 写入原附件后重建 MinIO 容器，保留同一数据卷；新进程仅执行读取，仍得到原字节、原编号和唯一版本 0。
3. 切换活跃 binding 并重启 Agent 后，另一个独立进程通过真实控制面和 `ArtifactRouter` 读取相同附件成功，错误 Session 的查询无法取到附件。本地 `/readyz` 与固定域名 `/healthz` 正常。
4. 新增隔离 MinIO 合约入口，验证 S3 保存、关闭后重建 Router、读取、不可变重试和 Session 边界。测试使用独立随机端口/tmpfs 容器，不访问日常桶或 `.env`。
5. `TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh` 通过：全仓 race/lint/build、独立 PostgreSQL/Redis/Qdrant/MinIO、备份恢复工具链和离线告警检查。仅清理自建的合成测试容器与数据，没有删除业务卷。

## 重启后的 Telegram 实测（已通过）

2026-09-07 17:24–17:25（Asia/Shanghai），用户在原私聊要求重新读取上次附件，并确认收到回复。后台核对结果：

- 当前 Agent 进程启动于 17:19:16；本次请求 `req_6e26acbe57bdb1c947d32f581da170d9` 在重启后完成，trace 为 `27b5e61ad3c0f1c19371c2b9631eb151`。
- `read_attachment` 实际执行一次，状态 succeeded、无 error_type；本次 `attachment_read` 与原导入记录的 tenant、user、session、附件编号匹配。
- 切换后没有新的 `attachment_imported`，本次不是通过重新上传文件掩盖内存数据丢失。活跃后端仍为 S3-compatible MinIO，旧 InMemory binding 为 retired。
- 最终回复状态 sent、发送尝试 1 次，part 0 有实际 provider 回执；返回正文包含“星河计划”和“RC4-20260907”。

结论：**原附件保留 → MinIO 持久化 → Agent 重启 → Telegram 请求 → 工具读取旧附件 → 回复投递**的本地完整链路通过。这不扩展为高可用、异地恢复或通用在线迁移的验收结论。核对期间未重启服务、重放请求或发送 IM 消息，服务保持运行。

### 本轮使用的测试消息

**不要重新上传文件。** 在之前成功测试的 Telegram 私聊，沿用原保存回执里的 `att_...` 编号发送：

```text
请重新调用 read_attachment 读取附件 att_原来的实际编号，不要沿用历史回答。告诉我文件中的测试项目名称和本轮测试编号。
```

预期内容为“星河计划”“RC4-20260907”。本轮已按上述执行记录核验；以后复测仍应检查新的工具调用，不能仅以模型复述历史答案判定成功。

## 日常启动与边界

启动依赖时现在执行 `docker compose up -d postgres redis minio`，其余模型、Agent 和 Tunnel 启动方式不变。不要执行 `down -v` 或删除 MinIO 卷；正常启动不需要重建账号和绑定。

这是单机持久化测试，不是 MinIO 高可用或异地容灾部署。沿用 Compose 的本地开发管理员配置；生产需独立管理员密钥、TLS、磁盘冗余和异地备份。本轮只持久化 Artifact；后续 Memory 已单独配置 PostgreSQL，见[记忆启用记录](memory-postgres-2026-09-07.md)。未实现通用 Artifact 在线双写迁移、企业微信媒体下载、PDF/Office、图片理解或文件出站发送。
