# 积压与接收进度监控

告警规则已经可以在本地评估；发送到哪个群、邮件或值班系统是独立的部署配置，目前没有配置真实通知接收方。规则存在不代表已经有人收到告警。

## 看哪些指标

Gateway/Admin 在 PostgreSQL 控制面下注册积压观测器，读取 migration 016 的 `platform_backlog` 聚合视图。其他角色不会为监控额外读取业务表。视图只返回租户、阶段、状态、数量和最老记录年龄，不包含聊天正文。

| 阶段 | 数量/年龄的来源 | 用途 |
| --- | --- | --- |
| queue | SQL Outbox 的 pending/publishing | 判断待发布任务是否积压；**不是 Redis Streams PEL 数量** |
| run | queued/running/failed/dead | 区分待执行、执行中与失败任务 |
| outbound | pending/sending/dead | 判断回复是否阻塞 |
| background | pending/running/dead | 判断摘要、记忆等后台任务是否阻塞 |
| delivery | MCP attempting/unknown | 发现没有确定发送结果的尝试，不能据此自动重发 |
| checkpoint | active binding 的最旧 through_at | 判断接收进度是否落后；没有新消息时也应推进 |

Prometheus 指标名为 `agent_backlog_items`、`agent_backlog_oldest_age_seconds`，标签为 `tenant_id`、`backlog_stage`、`backlog_state`。阶段为空时仍输出显式 0，避免任务处理完成后看起来一直有积压。

多个 Gateway/Admin 观测的是同一份数据库。统计全局积压使用 `max by (tenant_id, backlog_stage, backlog_state)`，**不能把各节点相加**。每个进程使用独立、随机的 `service.instance.id`，避免不同节点的累计计数器混在一起；不把主机名、环境变量或进程参数导出为资源标签。

## 不把“采集失败”当作“没有任务”

每次查询有 3 秒超时，结果上限 10,000 行，超过上限整次失败，不导出截断结果。它限制返回结果，不保证数据库扫描量；大量历史死信需另行归档并测量查询开销。

- `agent_backlog_snapshot_up=1`：此次读取成功。
- `agent_backlog_snapshot_up=0`：读取失败，没有导出假的零积压。
- `agent_backlog_snapshot_timestamp_seconds`：最后成功快照的 Unix 时间，用于发现进程退出或 Collector 缓存了旧值。

OTel 当前默认约每 60 秒导出一次指标，告警再经过 Prometheus 的评估和 `for` 窗口，不是即时通知。InMemory 控制面没有这些 SQL 快照；纯内存演示应禁用快照缺失告警。

## 已加入的规则

规则在 [`prometheus-rules.yaml`](../deploy/compose/prometheus-rules.yaml)：任务最老等待、dead/unknown、悬而未决的发送尝试、检查点停滞、轮询失败、单条消息隔离、快照失败和过期。回复失败率也计入 `unknown`，不能把“结果不知道”算成成功。

这些阈值是本地初始值，不是已经验证的生产 SLO。长耗时模型、主动暂停的租户、历史 dead 记录和计划维护都需要值班侧设置路由、静默和处理流程。`failed` run 可以仍在重试，不直接等同于永久死信。检查点告警依赖至少成功创建过检查点；首次轮询失败要同时查看轮询错误指标。

规则的离线测试（使用已经缓存的镜像，不启动或重载现有监控服务）：

```bash
docker run --rm --pull=never --network none \
  -v "$PWD/deploy/compose:/rules:ro" --workdir /rules \
  --entrypoint /bin/promtool prom/prometheus:v3.5.0 \
  test rules prometheus-rules.test.yaml
```

自动测试覆盖重复观测节点不重复报警、空队列恢复为零、未知发送计入失败、停滞检查点、SQL 聚合权限，以及读取失败不报健康。真正部署前需先完成 schema 升级和只读视图授权，再启动新版本 Gateway/Admin 并加载规则；这次代码测试没有重启日常服务或监控容器。

## 收到告警后的入口

先检查 `/readyz`，再按租户和阶段查看受鉴权的 Admin 查询。异常消息与检查点使用[通道恢复接口](channel-recovery.md)，未知发送需要核对实际发送证据；不能删除 delivery attempt 或 seen 记录来“解除告警”。数据库权限配置见[部署权限](deployment-permissions.md)。
