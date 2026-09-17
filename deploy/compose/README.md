# 本地 Compose 环境

这是仓库唯一受支持的运行环境：Docker Desktop 本地验收，而非生产部署模板。

完整启动、验证与清理步骤见 [`docs/runbook/getting-started.md`](../../docs/runbook/getting-started.md)。

- `webui` profile：PostgreSQL、Redis、Jaeger、OTel Collector 和 `webui-local`；需要本机 Docker secret 中的 DeepSeek Key。它还创建隔离的 `webui-local-skills` volume，供已发布 Skill 的受保护 staging 使用。
- `webui-multinode` profile：先等待 Qdrant 的 HTTP health，再运行一次性 `webui-local-bootstrap`，随后启动两个独立的 WebUI composition node；它们共享 PostgreSQL/Redis 及同一个只用于 Skill staging 的 volume，使用不同 Worker、relay、delivery 和 wakeup consumer ID。该 profile 供本地依赖短断恢复演练使用，不构成单节点故障接管的自动验收声明。
- `feishu-local` profile：开发者显式提供本地忽略的 `secrets/feishu.env` 与 DeepSeek Key 后，启动真实飞书 callback、验签、durable ingress、Worker 和 Reply API 投递。它只用于本机 Docker 验收；外部事件订阅还需要把宿主机的 `58086` 端口通过临时 HTTPS tunnel 暴露为 `/callbacks/feishu?route_key=local-feishu`。
- `wecom-local` profile：同 feishu-local 的组合方式，改用本地忽略的 `secrets/wecom.env`（Corp ID、Agent ID、回调 Token/EncodingAESKey、应用 Secret），启动真实企业微信回调验签、durable ingress、Worker 和官方 Reply API 投递。外部回调需要把宿主机的 `58087` 端口通过临时 HTTPS tunnel 暴露为 `/callbacks/wecom?route_key=local-wecom`。
- `runtime-test` profile：本地 PostgreSQL/Redis migration 与恢复契约。
- `docker-compose.backend-smoke.yml`：由 `bash scripts/e2e/backend-adapter.sh` 创建并自动销毁的 PostgreSQL、Redis、Qdrant、Vault smoke 环境。

## 本地 Admin 控制面

`gateway-worker` profile 还会启动只供内部使用的 `admin` 服务（默认宿主机端口 `58083`）。它需要独立的 HMAC 密钥，不能复用 Gateway 密钥。先选择一个已经初始化、未禁用的 tenant，再从仓库根目录创建本地文件型 Secret Provider 投放：

```bash
go run ./cmd/admin-bootstrap secret \
  -secret-root deploy/compose/secrets \
  -tenant-id <tenant-id>
```

命令会输出应写入忽略文件 `deploy/compose/.env.local` 的三项 `TRPC_ADMIN_*` 配置，但绝不输出原始密钥，也拒绝覆盖已有投放文件。获取目标 tenant 的当前 version 后，显式签发一个最长 15 分钟的本地测试 token：

```bash
go run ./cmd/admin-bootstrap token \
  -secret-root deploy/compose/secrets \
  -tenant-id <tenant-id> \
  -tenant-version <current-version>
```

token 会输出到标准输出；应只在本机短期使用，并通过 `Authorization: Bearer <token>` 调用 Admin API。生产环境必须由 Vault/CSI 与身份系统分别投放密钥和签发 token，不能使用这个本地 bootstrap 工具。

Admin Console 的会话 Cookie 默认带 `Secure`，即使 TLS 在反向代理终止也不会降级。仅 Docker Desktop 本地 HTTP 验收可显式设 `TRPC_ADMIN_ALLOW_INSECURE_SESSION_COOKIE=true`；该开关不得用于 ingress 或公开端口。

Compose 环境的数据只用于开发和测试。不要把默认 Token、默认密码或容器卷复制到真实环境。
