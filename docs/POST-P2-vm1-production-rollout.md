# POST-P2 执行记录：VM1 生产形 rollout（WS-2 RLS 审计 + WS-3A 部署 + WS-4 备份离机）

> 本文档记录 POST-P2 决策包获批后的第一次生产形 rollout 执行。环境：
> 单台 Ubuntu 20.04.6 VM（192.168.1.6，2C/3.9G/95G，Ubuntu 仓库
> Docker 28.1.1 + Compose v2.35.1）。它是 production-shaped 边界证据，
> 不是 `Production deployment: NOT RUN` 的解除——真实生产身份与流量
> 切换仍需运营确认。

## 1. 拓扑与角色

| 节点 | 角色 | 内容 |
| --- | --- | --- |
| 物理机 remote.caelum.com (192.168.0.244) | 控制节点 + 离机备份目标 + witness（后续） | 仓库、构建、测试套件、备份归档 |
| VM1 192.168.1.6 (ubuntub) | **生产形活动节点** | app + PostgreSQL 16 primary（streaming-ready）+ Redis + OTel collector |
| VM2（待用户克隆 VM1） | 待建 standby/第二活动节点 | 下一轮：复制/standby/Sentinel/故障切换 |

SSH：物理机 → VM1 采用 ed25519 key（`~/.ssh/p204_vm_ed25519`）；VM1
密码登录仍开放（弱密码 123456——**建议轮换**，见第 6 节）。

## 2. VM1 provisioning（幂等脚本）

`scripts/p2-prod-vm1-provision.sh`（repo 持有，sha256
`55b496a955b46a24…`，`bash -n` 通过）：安装 Docker（get.docker.com，
focal 需禁用 cdrom 源并跳过 docker-model-plugin）、创建 /opt/p2prod
（secrets 0700/0600，随机 32 字符密码）、生成 redis.conf（requirepass
来自 secret，chown 999）、生成 pg-init/01-replication.sh（首启创建
`repl` 复制角色 + pg_hba 行，为 VM2 standby 预备）、写 compose 并启动
基础设施。

镜像交付：物理机 `docker build -f Dockerfile -t app:release .` →
`docker save | ssh docker load`（VM1 不需要 Go 工具链/仓库）。

## 3. 数据库 rollout（migration + runtime role）

物理机 `trpc-migrate` → VM1 PG（postgres 经 192.168.1.6:5432）：
`migration ok current_version=11`；`trpc_runtime`（NOBYPASSRLS）+ 授权
由 RUNTIME_ROLE 机制完成。`schema_migration` = 11 行。

排障实录（保留供未来参考）：VM1 PG 容器曾因 bitnami 标签不存在
（catalog 整合）与 redis.conf 权限（chown 999）进入 restart 循环；物理
机侧一次凭据转移因 root-700 目录导致空密码（表现为 migrate
category=unavailable 与 psql 密码提示）——全部按服务端日志定位并以
官方 postgres/redis 镜像 + 权限修正解决。

## 4. WS-2 RLS rollout 审计（生产形 PG，真实证据）

P2-01 套件从物理机对 VM1 PG 重跑：

- TestRLSPolicyCoverage **PASS**
- TestRLSTwoTenantIsolation **PASS**
- TestRLSConnectionReuseNeverLeaksTenant **PASS**
- TestRLSConcurrentTenants **PASS**
- TestRLSNarrowGlobalCapabilities **PASS**

24 表 ENABLE+FORCE RLS、`<table>_tenant_isolation` FOR ALL policy、
definer 函数、runtime NOBYPASSRLS/非 owner、连接复用无租户泄漏、窄
全局能力——在生产形拓扑全部成立。`Production RLS role/database
rollout: NOT RUN` 保持（本机不是生产环境），但 roll-out 路径已在
production-shaped 基础设施上完整演练。

## 5. 应用与就绪

`p2prod-app` healthy；从物理机探测：`/healthz=200`、`/livez=200`
（http://192.168.1.6:8080）。栈：app + PG primary + redis + otel
（telemetry 暂 no-export，待 TLS/IAM）。MODEL_PROVIDER=runner + 回环
端点：无外部 Provider 请求。

## 6. WS-4 备份离机（off-host copy）

`p202-recovery-tool:local`（Dockerfile.recovery 构建，含 pg16 客户端 +
trpc-recovery + migrations）从物理机执行：

- backup → VM1 PG：archive 9,764B + manifest + COMPLETED（sha256
  `4817491f…`），归档落在物理机 `/home/pi/p204-backups/`（**离机副本**）；
- verify：24/24 TOC 表、sha256 匹配 **PASS**；
- restore 演练 → VM1 scratch 库 `trpc_agent_restore`：**PASS**
  （schema_migration=11），演练后库已删除（生产库不留演练产物）。

权限注意：容器产物为 root 属主——控制节点侧访问需 chown（本轮用
alpine 容器 chown 1002:1003 + 保持 0600 模式合同）。

## 7. 当前边界（未变）

`Production deployment: NOT RUN`、`Production RLS role/database rollout:
NOT RUN`、`Automatic PG failover: NOT PROVEN`（2 节点自动切换有
split-brain 风险，v1 采用手册化 promote；VM2 加入后评估 Patroni+etcd
三节点或云托管 HA）、`Production telemetry rollout: NOT RUN`、
`Security clean: NOT CLAIMED`。VM1 上真实 Provider 请求 0。

## 8. VM2 克隆与 HA 接线清单（下一步）

1. 用户克隆 VM1 → VM2（同磁盘内容）；
2. VM2 重识别：hostname、IP（192.168.1.8）、机器 ID、SSH host key；
3. PG standby：VM2 以 `REPMGR_ROLE=standby`（或 pg_basebackup from
   VM1）加入；复制验证（`SELECT * FROM pg_stat_replication`）；
4. Redis：VM2 replica + Sentinel ×3（VM1/VM2/物理机 witness）；
5. app 双实例验证：两节点同时服务、durable facts 单源（PG primary）；
6. 故障演练：kill VM1 PG primary → 手册化 promote VM2 → app 重指向 →
   事实不丢；评估自动 failover（Patroni+etcd 三节点含物理机 witness）；
7. 备份定时：VM1 每日 backup + 离机 rsync 到物理机 + verify。
