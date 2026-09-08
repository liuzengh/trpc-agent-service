# 本地交付收尾记录

用户要求整理提交并继续收尾，明确禁止 push。本轮只做本地 Git 提交和隔离验证，不修改远端仓库。

## 第一阶段检查点

本地提交 `c5104ca` 收录此前 rc.5–rc.9 的实现、测试与真实联调记录，共 92 个文件。提交前对 493 个仓库文件检查当前私有凭据字面值及高风险 key 形态，未发现匹配；`.env`、PID、日志及 `data` 下私有备份不在提交中。该检查不是对所有历史提交的完整 DLP 保证。

`TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh` 通过，文档中陈旧的“待配置/待验证”和历史版本表述已校准。工作项、知识库等通过范围仍按各自记录，不扩大为生产验收。

## 最新多租户/多 Worker 联合测试

新增 `TestIsolatedTwoTenantTwoWorkerWorkflow`，由 `scripts/e2e-multiprocess.sh` 与完整隔离回归调用。测试自行创建带所有权标签的 PostgreSQL、Redis、Qdrant 容器，使用 tmpfs 和随机 loopback 端口，没有挂载业务数据卷。

测试编译真实服务二进制，启动 Gateway、Relay、Sender 和两个 Worker 子进程；只给子进程传入明确列出的测试环境变量，并以 `-env-file=` 启动。模型和 Embedding 使用本地合成 HTTP 服务，分别检查两个租户的模型名称/密钥，不产生真实供应商费用或 IM 消息。

已覆盖：

- 相同 user_id/session_id 下两个租户的 Redis Session 历史隔离。
- 共享 PostgreSQL Memory 表、共享 Qdrant collection 下的事实隔离，经真实 Worker/Runner 工具读回。
- A 租户凭据不能调用 B 的 HTTP Binding；B 模型强行要求未授权 echo 工具时，没有授权执行记录。
- 消息处理中和完成后的重复提交复用原 request_id。
- 在模型请求处理中，对数据库记录的实际 claim owner 对应的测试子进程发 SIGKILL，存活 Worker 接管并完成同一请求，只保留一条回复；另一个租户的会话继续可读。

首次单独运行约 17 秒通过。清理只针对测试持有的子进程句柄和已核对标签的容器，不用进程名批量停止，不操作日常 Compose。此处证明的是合成模型下的进程/数据链路，不是两家真实模型、多 IM 账号或生产容量验收。

## 手动启停与状态查看

新增 `trpc-local` / `status.sh`。启停脚本共享工作区锁，新启动记录 PID、启动时间、boot ID、exe/cwd；停止使用 Linux pidfd 防止核对与发信号之间的 PID 复用，等待退出但不强杀。身份不符、元数据符号链接或超时均拒绝危险操作。日志改为追加，启动等待 HTTP 角色 readyz。

隔离工作目录测试使用实际编译的 Agent 与管理程序，验证无外部依赖的初次启动、重复启动、PID 不变、就绪和优雅停止。额外覆盖 PID 身份篡改、不同可执行文件、旧格式 PID、符号链接与忽略 TERM 的超时进程。

日常 `status.sh` 只读检查通过：Agent、PostgreSQL、Redis、本地 MinIO/Qdrant、模型列表、公网 healthz 及 Compose 运行状态正常。未重启或停止日常 Agent、未调用真实模型生成或 IM 发送。仅在私有 `.env` 增加已有公网入口的 `TRPC_AGENT_PUBLIC_BASE_URL`，没有更改 DNS、Webhook 或密钥。

以上是首次探针结果，不代表服务持续在线。提交前的末次只读复查中，Agent 和各依赖已不可达，Compose 显示 exited，公网 healthz 返回 530；本轮没有执行日常实例的启停命令，也未自动恢复这些服务，不据此推断外部状态变化原因。

具体操作见[运行手册](../operations-runbook.md)。这些脚本不创建开机自启，也不自动启动模型和 Tunnel；非 Linux 环境仍按直接进程/容器方式运维。

## 最终回归与交付边界

收尾改动后再次运行以下命令，退出码为 0：

```bash
TRPC_AGENT_VERIFY_ISOLATED=1 ./scripts/regression.sh
```

全仓库 race 测试、静态检查与构建、文档链接、隔离部署权限/恢复测试、PostgreSQL dump/restore、Redis RDB 恢复和告警规则检查均通过。恢复测试包含上面的两租户联合测试；隔离容器与合成数据由测试自行清理，日常运行实例和共享数据没有被重置。

最后修正模型列表探针的状态分类：只有 404/405 返回 unknown，连接失败、鉴权失败等仍返回 down。新增相应测试后再次运行 `./scripts/regression.sh`，全仓库回归通过。对 503 个候选仓库文件再次进行当前私有凭据与高风险 key 形态检查，匹配数为 0；私有 `.env`、日志、PID 和数据备份未纳入提交。

本轮仅交付本地提交，不执行 push、不创建远端版本；无需再发送业务测试消息。生产集群部署、真实告警通知接收方、云后端联调和大规模语义质量/容量评估仍需对应环境，不包含在本次通过结论内。
