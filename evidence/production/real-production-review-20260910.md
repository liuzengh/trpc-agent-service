# 真实生产验收记录（2026-09-10）

结论：**未通过；真实生产闭环尚未完成**。本次已实际重跑隔离 Compose 门禁，并直接请求配置中的真实模型。Compose 成功不能替代生产环境、真实身份系统及 IM 收发验收。

## 本次证据

| 检查 | 实测结果 | 证据与边界 |
| --- | --- | --- |
| 隔离 Compose 门禁 | 脚本退出 0，summary 为 passed | `summary-trpc-prod-acceptance-414713997.txt`；使用模型/OIDC fixture、临时数据库和存储 |
| 两副本和故障探测 | 副本退出后 ready=200；数据库断连 health=200、ready=503，恢复 ready=200 | 同上；队列 reclaim、提交确认丢失与内容安全接管另由同库集成测试验证，不等于线上在途消息故障演练 |
| RBAC 与依赖接口 | viewer 读 200、写 403、跨租户 403；10 个依赖条目 | fixture 签发 token 的本地结果 |
| Jaeger 与脱敏扫描 | 脚本检查通过 | 当前脚本仅检查查询 data 为数组，未断言非空或指定完整链路；不能证明所有 storage spans 和跨组件关联通过 |
| 早期真实 Qwen 调用 | HTTP 403 | `real-provider-probe-20260910.json`；模型 `qwen3.7-max-2026-06-08`，无 fallback；诊断请求同样返回 `AllocationQuota.FreeTierOnly` |
| 真实 GLM 调用 | HTTP 200，已结算 104 tokens | 默认模型已改为 `glm-5.2`；后续企微单聊联调使用同一模型并成功完成 |
| 默认服务配置 | local 模式、local_token、内存队列/控制面/协调、mock fallback | `config/example.yaml`；不符合其自身生产配置门槛 |
| 真实部署定位 | 未发现可用 Kubernetes current-context；本机 8080 没有监听服务 | 本次命令检查；不能据此断言其他地址不存在部署 |
| 企业微信真实收发 | 本机单聊联调通过 | 智能机器人认证、入站、模型处理和出站回复均成功；详见 `local-wecom-single-chat-acceptance-20260910.md`。本机 local 配置不等于生产部署 |
| 临时资源清理 | 未残留本次验收容器 | 门禁结束后 Docker 容器名称检查 |

## 完成验收所需条件

1. 指定实际生产服务地址、测试租户及认证配置引用；凭据留在本机或 Secret Manager。
2. 为实际生产租户指定有权调用的正式模型并验证真实响应及 usage。`glm-5.2` 已在本机联调使用；不能用 mock 响应代替。
3. 在实际生产持久数据面中，完成平台入站、持久 Inbox、真实模型、Session/预算/审计、Outbox、平台回执和客户端收件的同一消息对账。本机 IM 链路已通过，但仍使用进程内后端。
4. 在明确的验收环境内完成在途消息节点退出、数据库断连恢复及重放对账；对真实环境故障注入需先明确范围，不能把本地 fixture 演练等同线上演练。
5. 收集非空、按请求关联的完整 trace，以及真实 OIDC/Secret、部署所用数据后端的验证证据。

本记录不升级 README 或验收矩阵的生产就绪状态，也不把已有 historical passed summary 当作真实生产完成证据。
