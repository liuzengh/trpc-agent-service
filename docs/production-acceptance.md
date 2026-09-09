# 生产验收

生产验收以可复现证据为准，不以“容器已启动”为准。部署固定 commit/image digest，使用外部 Secret 管理、共享 PostgreSQL/Redis、独立 migration 和 TLS/WAF 入口。通用远端健康检查仍可运行：

```bash
export TRPC_AGENT_ACCEPTANCE_BASE_URL=https://agent.example.com
./scripts/production_acceptance.sh
```

脚本验证 `/healthz` 和会实际 Ping PostgreSQL/Redis 的 `/readyz`。Admin 检查是可选的，token 只从环境读取且响应保存到权限受控的临时目录：

```bash
export TRPC_AGENT_ACCEPTANCE_ADMIN_TOKEN='<from secret manager>'
export TRPC_AGENT_ACCEPTANCE_TENANT='acceptance-tenant'
./scripts/production_acceptance.sh
```

只有在已确认模型成本和测试租户后，才显式启用一条真实 Runner 消息。它不替代企业微信/飞书平台回调验收：

```bash
export TRPC_AGENT_ACCEPTANCE_GATEWAY_TOKEN='<from secret manager>'
export TRPC_AGENT_ACCEPTANCE_BINDING='acceptance-binding'
export TRPC_AGENT_ACCEPTANCE_RUN_MESSAGE=1
./scripts/production_acceptance.sh
```

## Compose 多节点门禁

验收拓扑是一个 producer-only Gateway 和两个 consumer-only Worker；默认 `all` 服务仍兼容。
指定既有 Compose 项目名即可复用原 PostgreSQL/Redis 命名卷：

```bash
export TRPC_AGENT_COMPOSE_PROJECT=trpc-agent-service-acceptance
export TRPC_AGENT_ACCEPTANCE_START=1
./scripts/multinode_acceptance.sh
```

默认检查不会写测试消息或重启容器。真实共享后端回归使用唯一测试 tenant 并在结束时清理：

```bash
TRPC_AGENT_ACCEPTANCE_RUN_INTEGRATION=1 \
  ./scripts/multinode_acceptance.sh
```

只有确认测试 tenant、binding 和模型成本后，才启用合成消息与定点 `worker-a` 重启：

```bash
export TRPC_AGENT_ACCEPTANCE_TENANT='<dedicated acceptance tenant>'
export TRPC_AGENT_ACCEPTANCE_BINDING='<dedicated HTTP binding>'
export TRPC_AGENT_ACCEPTANCE_GATEWAY_TOKEN='<from secret manager>'
export TRPC_AGENT_ACCEPTANCE_RUN_MESSAGES=1
# 同一项目仍运行旧 service --role all 时，临时隔离并在退出时自动恢复：
export TRPC_AGENT_ACCEPTANCE_ISOLATE_TOPOLOGY=1
./scripts/multinode_acceptance.sh
```

脚本不执行 `down`、`down -v`、`volume rm`、数据库清空或不带服务名的 stop/restart；隔离模式
只临时停止明确的旧 `service`，故障检查只停止 `worker-a`，退出时恢复两者。凭据只写入
`umask 077` 的临时文件并在退出时删除。stdout 只输出门禁结论，不输出请求/回复正文、SecretRef、
数据库密码或 IM 凭据。

验收证据分两类：自动化测试证明竞争消费、lease 接管、旧 fencing token、session 顺序、配置
版本和租户/通道隔离；人工真实平台验收证明企业微信、飞书平台回调及回复可达。当前仓库只
声明协议自动化验收通过；真实 IM E2E 必须在目标环境由部署方执行并保存脱敏证据，仓库不保存
截图、回调原文或用户消息正文。

## Kubernetes 最小生产门禁

不具备集群时先执行离线结构门禁。`--run` 不会自动配置生产 Secret Provider；运行前必须把
demo/base 中的 env SecretRef 替换为目标环境可访问的 Vault/KMS tenant/app namespace：

```bash
./scripts/kubernetes_acceptance.sh --validate
TRPC_AGENT_K8S_CREATE_KIND=1 ./scripts/kubernetes_acceptance.sh --run
```

完成上述 Secret 配置后，脚本预期部署 3 个 Gateway 和 3 个 Worker，并运行合成 Runner 链路、100 个 health 请求和
20 个并发 Gateway→Runner→PostgreSQL 完成态请求的容量冒烟、
单 Pod 重建、Redis/PostgreSQL/模型/Collector 故障恢复、Sender retry/DLQ 回归、滚动升级与
`rollout undo`。验收前后比较 PostgreSQL 中的配置版本并确认 PVC 仍存在。专用 kind demo 安装
固定版本 Metrics Server，并要求两个 HPA 的 `ScalingActive=True`；生产集群仍须按容量基线校准
实际扩容阈值。该真实模式当前不在 GitHub CI 中执行，结果必须记录到脱敏报告模板。脚本不执行 namespace/PVC/volume 删除，也不输出凭据、用户正文或 SecretRef 值。

## 发布门禁

- `./check.sh`、PostgreSQL 集成测试、Kubernetes manifest 校验和镜像构建均通过；migration Job 先完成，业务 Deployment 后发布。
- 三副本跨节点/可用区调度，PDB 与滚动更新生效；readiness 失败不会触发 liveness 重启风暴。
- Admin 未认证/越权、并发 publish、rollback 不可变性和 SecretRef 脱敏测试通过；当前配置版本与变更单一致。
- Session/Memory 为共享 PostgreSQL，调度/限流为 Redis；缺失或初始化失败必须 fail fast，不能出现 InMemory 回退。
- 容量测试达到批准的错误率、完整执行 p95、队列和数据库阈值，并留出一个 Pod 失效余量。
- 单 Pod、PostgreSQL、Redis、模型、Sender 和 Collector 故障场景有演练记录；告警能在目标时间内到达值班人员。
- 企业微信与飞书真实回调、幂等、Runner、Outbox 回复必须由部署方完成人工回归并留存脱敏证据；自动化协议和隔离测试同时通过。
- 日志、trace、HTTP 错误、审计和验收产物抽查无模型 Key、IM Secret/Token、EncodingAESKey、数据库密码或用户敏感正文。

任一硬门禁失败都停止发布并保留上一镜像和上一不可变配置版本。配置问题用 Admin rollback 创建新版本；二进制问题使用 Kubernetes rollout undo。两者都不得删除或重建 PostgreSQL/Redis 数据卷。
