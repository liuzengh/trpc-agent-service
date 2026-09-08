# POST-P2 readiness gate: production-readiness inventory and Go/No-Go decision pack

> 本文是 P2-01..P2-04 全部关闭后的**就绪评审门**：冻结 post-P2 基线、
> 盘点生产化阻塞项、形成按优先级排序的决策包与 Go/No-Go 准则。它不是
> 生产部署、不是 P3 实施阶段、不改变任何既有边界的 NOT PROVEN/NOT RUN
> 状态。"决策包完成"不等于"生产就绪"。

## 1. Gate 结论

`POST-P2 readiness gate: PASS — production-readiness inventory and Go/No-Go decision pack completed`

范围：复核（re-verification）+ 冻结基线 + 阻塞盘点 + 决策包。排除：
生产部署、真实凭据、真实 Provider 请求、把本地证据改写为生产证据。

**WS-1 执行更新（用户授权决策后）**：定向依赖 remediation 已执行并
通过全量回归——govulncheck 复扫 **0 reachable vulnerabilities**（此前
8 项 reachable 全部修复）。基线哈希已随升级更新（见第 3.1 节）；WS-2
..WS-8 选项 A 路径已采纳为正式决策，执行待生产基础设施就绪。

## 2. 可重复性复验（本门实际重跑，非引用报告）

| 检查 | 结果 |
| --- | --- |
| `bash scripts/p204_release_gate.sh`（6 步） | PASS（含断电恢复后重跑） |
| `docker compose config -q`（默认 profile + placeholder env） | PASS |
| `go test ./... -count=1`（42 包） | PASS，exit=0，0 FAIL |
| `go test ./... -race -count=1`（42 包） | PASS，exit=0，无 DATA RACE |
| `go test -tags=integration ./trpcservice/... ./cmd/... -count=1`（41 包，含 compose 三门禁与 P2-01/02/03/04 focused gates） | PASS，exit=0 |
| `go test -race -tags=integration ./trpcservice/... ./cmd/... -count=1`（41 包） | PASS，exit=0，无 DATA RACE |
| `go vet ./...` | PASS |
| focused：P2-01 RLS（TestRLS/TestRuntimeRole）、P2-02（TestBackupRestoreDrill/TestRestoreRefusalMatrix）、P2-03（TestProductionCapacityDrill）、P2-04（TestP204SecretScan/ComposeSecurityAudit/AppContainerRuntimeAudit/BoundedSoak） | 全部 ok |
| targeted `git diff --check`（scripts/docs） | 0 findings |
| scanner（gitleaks/govulncheck/trivy） | UNAVAILABLE（主机无二进制；既有 5 个 reachable findings 保留） |

结论：**P2-04 交付可重复**。回归在全新 owner-scoped 套件
（`post-p2-readiness-1788740360`）上重跑，全部 exit=0。

## 3. 冻结的 post-P2 基线

- git：HEAD `0af233f`（branch project/production-platform），worktree
  228 项（未 commit/push，按合同保留）；
- 依赖（WS-1 前基线）：go.mod `50ace433…`、go.sum `23cba731…`（Go 1.26.3）；WS-1 remediation 后更新为 go.mod `8587b80a…`、go.sum `223dfac9…`（见 3.1 节）；
- migrations：22 个 SQL 文件（11 版本对，1..11），聚合 checksum
  `8858ca7176e1d66b`；broken probe 保持测试内 000012；
- 运行时版本：Docker 29.7.2、Compose v5.5.0、PostgreSQL 16（服务器）/
  psql 15.19（宿主客户端，容器内 16.x 客户端用于 dump/restore）、
  Redis 7；
- 交付面：`cmd/trpc-service`（生产装配+生命周期+容量保护）、
  `cmd/trpc-migrate`、`cmd/trpc-recovery`、`trpcservice/*`（42 包）、
  Dockerfile/Dockerfile.recovery、docker-compose（core+telemetry+
  recovery profiles）、`scripts/p204_release_gate.sh`、
  `docs/P0-09…P2-04` 验收/运行文档；
- 测试面：42 包 ordinary/race、41 包 integration ordinary/race、
  compose 三门禁、release gate、内建 secret scan；
- 外部请求：P2-01..P2-04 与 POST-P2 全部为 0（真实 Provider/生产凭据
  从未使用）。

### 3.1 WS-1 remediation 后的基线更新（用户授权执行）

| 模块 | 旧版本 | 新版本 | 修复的 finding |
| --- | --- | --- | --- |
| github.com/jackc/pgx/v5 | v5.7.2 | v5.9.2 | GO-2026-5004 / CVE-2026-41889 |
| github.com/redis/go-redis/v9 | v9.7.0 | v9.7.3 | GO-2025-3540 / CVE-2025-29923 |
| google.golang.org/grpc | v1.65.0 | v1.82.1 | GO-2026-6061 |
| golang.org/x/text | v0.21.0 | v0.39.0 | GO-2026-5970 / CVE-2026-56852 |
| go.opentelemetry.io/otel(+sdk+sdk/metric+otlp exporters) | v1.29.0 | v1.43.0 | GO-2026-4394 / CVE-2026-24051 |
| github.com/gorilla/websocket（新发现） | v1.4.2 | v1.5.3 | GO-2026-6278 |
| go.etcd.io/etcd/server/v3（新发现） | v3.5.5 | v3.5.33 | GO-2026-6114、GO-2026-5736 |
- 传递升级：x/sys、genproto/googleapis/api+rpc、protobuf；
- API/行为影响：编译零改动、全量回归（ordinary/race/integration/race
  41-42 包）exit=0 无 DATA RACE；测试范围覆盖全部直接调用面（pgx
  storage 层、go-redis 限流/协调、OTel telemetry、grpc Milvus 间接）；
- 安全收益：govulncheck 复扫 `No vulnerabilities found`（此前 8 项
  reachable）；
- 修复期间一次测试构建失败为瞬态并行 Docker `go mod download`（冷缓
  存多构建并发）与一处 vet printf 非常量格式串（测试代码），均已修复
  并重跑至全绿。

## 4. 生产化阻塞项盘点（按优先级）

### P0 — 硬阻塞（安全/正确性/部署前提）

| # | 阻塞项 | 现状 | 归属工作流 |
| --- | --- | --- | --- |
| B1 | 依赖漏洞 5 项（grpc GO-2026-6061、x/text GO-2026-5970、pgx GO-2026-5004、OTel GO-2026-4394、go-redis GO-2025-3540） | reachable，未升级（无授权不改 baseline） | WS-1 |
| B2 | Production RLS role/database rollout NOT RUN；被攻陷凭据/superuser 隔离 NOT PROVEN | 角色模型只在本地验证 | WS-2 |
| B3 | Production deployment/integration NOT RUN | 只有本地/预生产 compose 证据 | WS-3 |
| B4 | Security clean NOT CLAIMED（扫描器不可用+findings） | — | WS-1 |

### P1 — 运维就绪（上线前必须）

| # | 阻塞项 | 归属 |
| --- | --- | --- |
| B5 | PostgreSQL HA/failover NOT PROVEN；Redis Cluster/HA NOT PROVEN | WS-4 |
| B6 | backup scheduling/retention/encryption/off-site NOT IMPLEMENTED；PITR/WAL NOT IMPLEMENTED；RPO/RTO NOT PROVEN | WS-4 |
| B7 | Production telemetry rollout NOT RUN；cloud IAM/TLS/retention/alerting NOT IMPLEMENTED | WS-5 |
| B8 | Secret Manager/IAM 接入 NOT IMPLEMENTED（当前 Docker secret/环境解析） | WS-6 |
| B9 | 生产事故演练（本文档矩阵）未在生产拓扑执行 | WS-3/WS-4 |

### P2 — 规模与公平（按负载需求）

| # | 阻塞项 | 归属 |
| --- | --- | --- |
| B10 | Production capacity/throughput NOT PROVEN；capacity limit NOT PROVEN | WS-8 |
| B11 | Cross-process queue-depth guarantee / Global tenant fairness NOT PROVEN | WS-8 |
| B12 | per-tenant worker budget / per-channel sender budget / circuit breaker runtime wiring NOT PROVEN | WS-8 |
| B13 | autoscaling/HPA/Kubernetes NOT IMPLEMENTED | WS-3 |

### P3 — 能力补全（按产品需求）

| # | 阻塞项 | 归属 |
| --- | --- | --- |
| B14 | Vector 生产链路 BLOCKED：Production Embedder NOT IMPLEMENTED、Production Milvus NOT RUN、production request path/Knowledge durable source 未实现 | WS-7 |
| B15 | Object 字节备份、Secret Manager 值备份、Milvus index backup：NOT INCLUDED | WS-4/WS-6 |
| B16 | 全系统 event sourcing/replay、provider exactly-once：NOT IMPLEMENTED/NOT PROVEN | —（设计决策） |
| B17 | 完整 IAM/审批/admin API/DLP/content moderation：未实现 | WS-6（按需求） |

## 5. 决策包（工作流选项与建议）

### WS-1 依赖与安全 remediation（P0）

- **A（推荐）**：授权对 5 个模块的定向升级（grpc、x/text、pgx、OTel、
  go-redis），升级前冻结 API 影响清单，升级后重跑全量回归+安装
  govulncheck 复扫。前置：用户明确授权改变依赖 baseline。规模 M。
- **B**：保持版本 + 补偿控制（输入校验已覆盖 x/text 归一化路径；pgx
  仅本地测试路径触发），书面风险接受。规模 S。
- **C**：全量依赖 sweep（Go 版本+全部传递依赖）。规模 L，风险最高。
- Go 条件：A 完成且 govulncheck 0 reachable，或 B 的风险接受签字。

### WS-2 生产 RLS/凭据 rollout（P0）

- **A（推荐）**：在生产拓扑 provision `trpc_runtime`/`trpc_claim_owner`
  （cloud IAM 或 secret manager 供给密码），执行跨租户探针与
  NOBYPASSRLS/owner 审计；复用 P2-01 套件作为验收。规模 M。
- **B**：维持单租户/单主体部署（RLS 仍强制，但多租户风险接受）。
- Go 条件：A 的审计在真实拓扑全部 PASS + 被攻陷凭据场景演练（仅
  runtime 凭据泄漏不影响跨租户读取）。

### WS-3 部署拓扑（P0/P2）

- **A（推荐第一步）**：单主机 compose（与现有证据同构）+ 操作员
  runbook（P2-04 运行文档直接适用）。规模 S。
- **B**：VM + systemd（二进制直跑，compose 退役）。规模 M。
- **C**：Kubernetes + HPA/Service Mesh。规模 L（B13 解除前提）。
- Go 条件：至少 A 完成 + 生产事故演练一轮。

### WS-4 HA/DR/备份（P1）

- 选项：PG 流复制/Patroni/云托管 HA；Redis Sentinel/Cluster；备份
  cron+对象存储+加密+异地；PITR/WAL 归档；定义 RPO/RTO 目标后选择。
- 建议顺序：先 backup scheduling+恢复演练（复用 P2-02 工具）→ HA →
  跨 Region。规模 M-L。
- Go 条件：RPO/RTO 目标定义 + 生产级 restore 演练 + failover 演练
  通过。

### WS-5 Telemetry rollout（P1）

- OTel collector 生产部署 + TLS/IAM + retention/alerting（P1-07 语义
  保持：outage 不改业务分类）。规模 S-M。建议与 WS-3A 同批。

### WS-6 Secret/IAM（P1/P3）

- Secret Manager 接入（替换 env:// 解析器为 provider-backed resolver，
  合同不变）、轮换、workload identity；admin/审批体系按产品需求。
  规模 M。

### WS-7 Vector 生产 rollout（P3）

- 解除 `P1-06 production rollout: BLOCKED` 的前置：Production
  Embedder、production request path、Production Milvus、Cloud IAM/TLS、
  Knowledge durable source（均 NOT IMPLEMENTED/NOT RUN）。规模 L。
- 建议：保持 disabled-by-default，待 P0/P1 完成后单独立项。

### WS-8 容量/公平（P2）

- 生产级负载测试（生产拓扑+生产数据形状）、per-tenant worker 预算、
  per-channel sender 预算、cross-process queue-depth cap、circuit
  breaker 接线。规模 M-L。建议：仅在确认多租户生产负载特征后启动。

## 6. Go/No-Go 准则

**Go（最小生产）**：WS-1（remediation 或签字）+ WS-2（rollout 审计
PASS）+ WS-3A + WS-4 最小集（backup 调度+生产级 restore 演练+RPO/RTO
定义）+ WS-5 最小集 + P2-04 事故矩阵演练一轮 + 全部门禁 PASS。

**Conditional Go**：单租户/单主体内部试点可接受 B 部分风险由所有方
书面签字（不含 B1 未 remediation 且未签字的情况）。

**No-Go**：B1 未处理且未签字；无生产级备份/恢复演练；无 RLS rollout
审计；无事故演练。

## 7. 保留的全部 NOT PROVEN / NOT RUN / NOT IMPLEMENTED

第一、二节列出的全部语句逐字保留（含 P1-06 BLOCKED、P1-07 本地边界、
P1-08 本地边界、P1-09 本地边界、5 个依赖 findings、`Project
release/security closure: PARTIAL`、`Security clean: NOT CLAIMED`、
`External provider requests（各阶段）: 0`、`Historical Telegram
requests: >0, prior explicitly authorized phase`）。本门未新增任何
"已证明/已实现"声明。
