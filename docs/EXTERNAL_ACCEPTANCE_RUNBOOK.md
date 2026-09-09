# 目标环境验收 Runbook

本手册规定目标账号、基础设施与业务容量的验收步骤。各阶段保存预期、实际结果及配置版本，失败时先定位原因再继续。

## 1. 总体顺序

```text
离线门禁 → 目标基础设施 → 公开回调空探针 → Telegram → 企业微信 → KMS/Vault 轮换 → 故障/灾备 → 正式容量
```

基础设施与回调检查通过后，用测试账号验证完整收发链路。两种 IM 可分别执行；共享依赖失败时暂停后续步骤。

## 2. 准备条件与成功标准

| 阶段 | 准备条件 | 成功标准 |
|---|---|---|
| 源码与后端验证 | Go、Bash、Docker、四类测试后端 | 按验证指南完成源码与 integration 检查 |
| K8s 发布与回滚 | 目标集群、镜像 digest、兼容迁移与网络策略 | workload Ready；session 版本绑定稳定；可恢复原 digest |
| 公网 callback | 域名或测试 Tunnel、HTTPS 证书、Ingress | 证书有效；缺少 route token 的请求返回预期 4xx |
| Telegram | Bot token、webhook secret、白名单 private chat，可选测试群 | 注册成功；单聊/群聊隔离；收发与审计记录一致 |
| 企业微信 | CorpID、AgentID、App Secret、Token、EncodingAESKey、测试成员 UserID | challenge 成功；加密文本入站与应用回复完整 |
| 密钥身份与轮换 | ServiceAccount、允许路径、两个 key version | 正向读取成功；越权拒绝；旧密文可解且新写使用新 key |
| DB/Redis 故障 | 专用测试实例、备份与恢复窗口 | 恢复后 backlog 清空；旧 fence 无法提交 |
| 业务容量 | 目标规格、业务 payload、模型账号、配额及费用预算 | p50/p95/p99、QPS、queue lag、资源、成本和错误率齐全 |

故障注入只在授权测试环境执行。账号调用、资源费用与测试窗口在运行前由部署负责人确认。

## 3. 零调用 Preflight

Windows 企微沙箱使用隔离端口和交互配置流程：

```powershell
# 1. 启动固定 digest 的临时 HTTPS Quick Tunnel，获得 HTTPS 回调基础地址
& .\scripts\wecom_sandbox_tunnel.ps1

# 2. 交互采集 CorpID/AgentID/UserID/Secret/Token/AES；输入隐藏，文件 ACL 收紧
& .\scripts\wecom_sandbox_setup.ps1

# 3. 在独立端口启动 trpc-platform-wecom Compose 项目，创建租户、发布版本并复检公网入口
& .\scripts\wecom_sandbox_bootstrap.ps1
```

UserID 填写企业通讯录中的成员“账号”。若留空，setup 会调用企业微信 `gettoken`/`getuserid` 按手机号查询；该查询需要相应通讯录权限，普通应用 Secret 可能没有权限，此时由管理员提供成员账号。

setup 与 bootstrap 不发送模型请求。Tunnel URL 随容器重建变化，正式部署使用稳定域名。真实配置保存在被 gitignore 排除、ACL 受限的 `deploy/.env.wecom.local`。企微沙箱使用 15432/14317/14318/18080/18081/19095/13000 端口。

目标部署通过进程环境或 Secret Manager 注入凭据；本地交互脚本使用上述受限环境文件。凭据不进入脚本参数、命令历史、CI 日志或截图。需要的变量名：

```text
TRPC_WEBHOOK_ROUTE_KEY
TRPC_SECRET_TELEGRAM_BOT_TOKEN
TRPC_SECRET_TELEGRAM_WEBHOOK
TRPC_SECRET_WECOM_TOKEN
TRPC_SECRET_WECOM_CORP_SECRET
TRPC_SECRET_WECOM_AES
WECOM_CORP_ID
WECOM_AGENT_ID
```

企微沙箱使用已安装的 `openai` Provider 和 `gpt-4o-mini` 模型，模型密钥同样保存在上述受限环境文件中。

只做格式、公开 URL 和可选 TLS 探针，不调用 Telegram/企业微信：

```powershell
$callbackBaseUrl = Read-Host '公网 HTTPS 回调基础地址（不含 token 查询参数）'
& .\scripts\external_acceptance_preflight.ps1 `
  -Channel All `
  -CallbackBaseUrl $callbackBaseUrl `
  -ProbeEndpoint
```

成功输出包含检查数量和 HTTP 状态。Provider callback URL 的 `token` 查询参数使用 `TRPC_WEBHOOK_ROUTE_KEY`；将完整地址直接填入 Provider 控制台。

Windows setup 会把完整 URL 临时写入剪贴板，粘贴后清空剪贴板。Gateway/Admin health 与公网 preflight 通过后，再在企业微信控制台保存回调配置。

Preflight 不调用模型或 IM 消息接口；失败时按报告检查 DNS、证书、Ingress 和配置格式。

## 4. 目标 Kubernetes 一次性准备

1. 锁定本次 release bundle、七个应用镜像 digest、migration schema class 和 NetworkPolicy review hash。
2. 先运行 `releaseverify` 检查镜像 digest、4143 mesh 路径和身份断言；breaking migration 需单独审批并提供排空记录。
3. 使用 `scripts/k8s_apply.sh`，顺序为：NetworkPolicy→profile→migration→PDB→Worker/Summary Worker/Admin→Consumer/Delivery→Gateway。
4. 记录每个 Deployment 的 generation、revision、imageID、Ready、restart count、node 分布和 Linkerd identity。
5. 运行同请求 allow/deny：有 identity 的 client 必须到达应用鉴权，无 identity client 必须被 mesh 拒绝。
6. HPA 必须显示 `ScalingActive=True`；使用 `ContainerResource`，不接受 `<unknown>`。

阻断条件：migration 失败、任一 Pod restart 增长、旧/新 digest 混跑超时、Linkerd identity 缺失、NetworkPolicy 需要临时全放通、HPA 指标未知。

## 5. Telegram

### 5.1 注册

1. 调用 `setWebhook`，同时设置 HTTPS callback 和 `secret_token`。
2. 调用 `getWebhookInfo`，保存 URL host/path、pending count、last error code/time；遮盖 bot token 与 route key。
3. 检查注册结果后再执行消息用例。

### 5.2 端到端验收用例

| 用例 | 操作 | 断言 |
|---|---|---|
| T-01 private | 白名单用户发送唯一短文本 | callback 2xx；1 Inbox、1 execution、1 Outbox；reply 成功 |
| T-02 group | 测试群白名单用户发送唯一短文本 | group session 与 private session 不同；actor/owner 映射正确 |
| T-03 duplicate | 在专用测试 Gateway 的 `/webhook` 重放同一测试消息请求，保留消息 ID 与 payload | Inbox 数量不增；payload hash 相同返回幂等成功 |
| T-04 bad secret | 向专用测试 Gateway 发送带错误 webhook secret 的测试请求 | 签名校验拒绝；无 Inbox、无 tenant 泄漏 |

429、Retry-After 和 outcome-unknown 使用本地 contract/fault 测试注入；账号联调确认正常发送和错误记录。

## 6. 企业微信

### 6.1 URL challenge

1. 检查服务器时钟偏差小于 60 秒；请求时间戳接受窗口为 ±300 秒。
2. 在控制台一次保存 callback URL、callback token 和 EncodingAESKey。
3. 保存动作必须完成 `msg_signature` 校验、echostr 解密、receiver corp ID 校验并返回原文。
4. 若失败，查看稳定错误类和脱敏 trace，在本地复现并修正后重新保存。

### 6.2 端到端验收用例

| 用例 | 操作 | 断言 |
|---|---|---|
| W-01 text | 白名单用户向测试应用发送唯一文本 | 加密 callback 验签/解密；1 Inbox；Agent 完成；应用消息回复 |
| W-02 duplicate | 对专用测试消息保留 MsgId 与密文，在有效签名时间窗口内重放 `/webhook` 请求 | 相同 MsgId 不产生第二次 execution/outbox |
| W-03 unsupported | 发送一个已允许的非文本测试事件 | callback 被确认但不调用 Agent，不制造 Provider 重试风暴 |
| W-04 unauthorized | 非白名单测试身份发消息 | Gateway 拒绝入队并记录授权决策；无工具/模型调用 |

重放请求仅在测试进程内使用，测试结束后释放。提交记录只保留 MsgId、trace_id、状态、延迟和 hash，不收集聊天正文、access token、corp secret、EncodingAESKey 或原始 XML。

## 7. KMS/Vault 生产验收

按目标环境采用的 Secret Manager 完成身份接线，并验证以下用例。

1. workload identity 绑定到专用 ServiceAccount，禁止 node-wide/static access key。
2. 正向 Pod 只读取本服务/测试租户的一条 secret；默认或其他 ServiceAccount 读取同路径必须被拒绝。
3. 创建 key version N+1；读取 key ring 时同时保留 N/N+1 解密能力，新加密只用 N+1。
4. 滚动 Gateway/Admin/Worker/Consumer/Delivery，观察无解密错误、无 Pod restart。
5. 用 N 生成的既有密文仍可解；新写记录标记 N+1；完成观察窗口后再禁用 N 的加密，不立即销毁。
6. 搜索 log/trace/error report，确认没有 key ID 之外的密钥材料、token、DSN 或 secret value。

阻断条件：需要静态云 access key、权限范围为整库/整项目、负向身份可读、轮换必须停机、旧密文不可解、任何 secret 出现在 telemetry。

## 8. 故障、回滚与容量

- PostgreSQL：一次短暂不可用或 primary failover；Gateway 不在未提交 Inbox 时回 2xx，恢复后 backlog 可推进。
- Redis：一次稳定端点切换；Worker 不降级为本地锁，session coordination 缺失时 fail-closed。
- Worker：中断一个正在处理的副本；新副本以更高 fence 接管，旧副本迟到写被拒绝，只生成一个 Outbox。
- Delivery：在 provider 调用边界制造一次 outcome-unknown；记录必须进入 reconciliation，不自动重复发送。
- Rollback：只回滚兼容应用版本；breaking schema 使用独立停机/排空 runbook。
- Capacity：warm-up 不计入报告；使用业务 payload 和模型服务完成至少两次测量，报告各轮分位数、吞吐、错误率、资源与成本。

## 9. 每次测试必须保存的证据

```text
release digest / schema class / config version
UTC start/end / operator / environment
tenant_id（测试租户）/ channel / hashed user / session_id
provider message ID 或其 hash / inbox ID / execution ID / outbox ID
trace_id / decision / error_type / latency / token/cost（如有）
Pod imageID / restart count / node / mesh identity
DB/Redis QPS、pool wait、queue lag、p50/p95/p99
预期、实际、PASS/FAIL、账号调用量与资源用量
```

证据包必须经过 secret scan；聊天正文、Authorization、Cookie、Bot token、WeCom token/secret/AES key、数据库 URL、Vault token、Kubernetes projected JWT 和证书私钥均禁止收集。
