# 运维收尾记录

日期：2026-09-06。企业微信真实 Agent 群文本链路已通过，继续处理运维代码，不停止已运行服务。

## 备份恢复工具链隔离

检查发现原 `scripts/e2e-backup-restore.sh` 虽然使用临时 PostgreSQL 数据库，但会向共享 Redis 写测试键、导出共享 RDB，且 cleanup 会停止共享 PostgreSQL/Redis。这与“只清理演练资源”的文档不符。

已改为三个独立容器，使用合成数据、`--network none`、tmpfs、不发布端口、不挂载业务数据卷。清理依赖 Docker 创建的 cid 文件和本次随机标签，拒绝对无法证明归属的对象执行删除；异常时保留待核对路径。只清理明确列出的临时文件，再删除空目录，不递归清理任意路径。

调试时发现 Docker cp 不能直接取得此环境中源容器 tmpfs 内的 RDB；已先在该隔离容器的可写层复制一份再导出。Redis CLI 增加 `-e`，确保 Redis 命令错误不会被当作成功。

最终实际执行通过：

```text
isolated PostgreSQL dump/restore passed
isolated Redis RDB restore passed
backup restore drill passed: postgres=ok redis=ok no_shared_data_access=true
removed only this drill's synthetic containers/data; shared services unchanged
```

所有本次临时容器和合成数据已清理，没有保留恢复副本，因为它们不含业务数据且可重新生成。日常 Agent PID 未改变，`/readyz` 为 ready，共享 PostgreSQL/Redis/观测栈保持运行。此前真实业务备份 `data/wecom-activation-EerZuk/` 没有改动。

新增脚本安全回归检查，覆盖 Bash 语法、禁止共享 Compose/删除业务键操作、cid/标签归属验证及只读合成快照挂载。该演练不是实际业务全量恢复或 PITR 验证，不能扩大表述。

## 后续需要的外部选择

Prometheus 已有告警规则，但尚未配置真实通知接收方。不默认复用业务机器人会话发送运维告警，需要用户确定通知通道及接收位置；凭据继续写入本地 Secret 配置，不贴入聊天或提交 Git。

实际数据库/Redis 分角色账号权限、业务恢复演练、容量基线和生产部署仍需继续，未将本记录标为全部运维交付完成。
