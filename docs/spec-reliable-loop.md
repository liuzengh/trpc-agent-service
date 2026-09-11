# 可靠消息闭环 Spec（Inbox → Worker → Outbox）

> 范围：第二批（8/28 之后的“可靠闭环”切片）。把消息链路从「回调内联 dispatch」
> 换成「MySQL 事实源 + 分角色进程」，并让每一处可靠性主张都有真跑出来的证据。
> 结构沿用另外三份 spec：事实核查 → 设计 → 测试策略 → 实测/验收；**没验过的地方如实标注**。

---

## 1. 事实核查（写代码前实测出来的，不是查文档抄的）

| # | 事实 | 证据 |
| --- | --- | --- |
| 1 | `SELECT … FOR UPDATE` 锁不住**不存在**的行：两个并发的首条消息都越过「查不到」检查，双双走到唯一键冲突 | `inbox` 并发测试首版直接红（duplicate entry on `uk_sessions_identity_generation`） |
| 2 | 同样两个并发路径还会死锁（1213）：duplicate-key 检查拿的 S 锁与随后 `SELECT FOR UPDATE` 的 X 锁转换互咬 | 同上的第二版红；MySQL 手册的经典 INSERT-IGNORE-then-SELECT-FOR-UPDATE 陷阱 |
| 3 | `INSERT … ON DUPLICATE KEY UPDATE` + `LAST_INSERT_ID(expr)` 能一条语句同时完成「不存在则建、存在则自增」并取回 id | 上述两处修好后的实现，19 个集成测试稳定通过 |
| 4 | **MySQL 没有** `UPDATE … RETURNING`（那是 Postgres 语法），解析期即失败 | `execution.Commit` 首版 red |
| 5 | 把 SQL NULL 扫进 `*json.RawMessage` 会失败（命名切片类型不在 database/sql 的可空指针名单里） | `controlplane` 第三轮 red（`guardrails` 列为 NULL 的 revision） |
| 6 | `tenant.Guardrails/Tools` 仅有 yaml tag 时，经 JSON 列往返会静默使用 Go 字段名（`BlockedKeywords`），与库内约定的 snake_case 不一致 | worker 输入护栏测试 red（策略读出来是空的） |
| 7 | 同一进程里 `bootAt` 过滤会误伤重启后的历史拉取；而**持久化游标**才是正确机制 | 旧链路 `wechatkf.go` 的已知限制；可靠拉取因此改成 MySQL 游标 |
| 8 | `docker compose down` 不带 `--profile` 不会拆 profile 下的服务（mysql 留在原地让下一轮撞名） | `reliable_e2e.sh` 首版跑完仍有 `tas-rel-mysql` |
| 9 | compose 镜像只有一个构建入口（`app` 服务的 `build:` 段）；启动列表不含 app 时 `up --build` **什么也不构建** | 首版 E2E 报 `flag provided but not defined: -migrate`（旧镜像） |
| 10 | 同一条 `-p` 项目里 `container_name` 冲突会让第二个栈起不来，端口 9009 也会撞（默认栈已占） | 默认栈与可靠栈要并存 → `reliable.override.yml` |

## 2. 设计

### 2.1 角色与拓扑

同一二进制分角色（`-role worker|delivery|jobs`，默认 `all` 仍是旧的单进程形态，
所有既有脚本/演练语义不变）：

| 角色 | 职责 | 依赖 |
| --- | --- | --- |
| gateway | 旧 HTTP 面（回调/管理/网页）。**可靠接收端尚未挂载**，见 §4 | — |
| worker | `ClaimNext` → 私有工作区跑 Runner → 条件提交 | MySQL、模型上游 |
| delivery | `reply_outbox` → 渠道发送（目前仅微信客服） | MySQL、KF API |
| jobs | `channel_notifications` → `kf/sync_msg` → Inbox | MySQL、KF API |

Compose（`--profile reliable`，独立项目 `tas-reliable` + override 共存）：
`mysql` + `worker-a`/`worker-b`（双副本从第一天就在）+ `delivery` + `jobs` + 复用
默认栈的 `fake-model`（新增 KF 桩）。**不带 Redis**：可靠链路当前没有角色用它，
不摆一个不存在的依赖。

### 2.2 事务与状态机

- **接收**（`inbox.AcceptInTx`）：预检（按 `(tenant,binding,platform_msg_id)` 读旧行）
  让重投零成本返回；唯一键仍是权威判重——竞态输家整事务回滚，连同它乐观多占的
  `in_seq` 一起消失。`AcceptInTx` 供拉取器把「整页入库」放进调用方的大事务。
- **领取**（`execution.ClaimNext`）：`FOR UPDATE SKIP LOCKED` 只取「有队头活、
  未被阻塞、租约空闲」的会话；`fencing_token` 单调 +1，租约 30s。
- **执行**：Runner 绑在 `sessionstore.Workspace` 上（P0 切片），SQL 长事务为零；
  续租 goroutine 失租即取消。
- **提交**（`execution.Commit`）：重读并校验 owner/fence/租约/generation/队头/
  `base_session_version`，然后在**一个事务**里写：prepared 落 `execution_attempts`、
  事件、State+版本、execution 终态、审计、`reply_outbox`、inbox done、队头推进、
  Redis 投影任务。fence 失效 → `ErrCommitConditionFailed`，**回复不落任何行**
  （有测试专门断言这一点）。
- **投递**（`outbox`）：Sent / Rejected（指数退避 5 次后 dead）/ **Unknown**（超时、
  半途崩溃 → 挂起会话，绝不自动重发）。KF 发送目标来自 `channel_reply_routes`
  （提交事务里写的），不是进程内存。
- **拉取**（`KfPuller`）：notification 持久化；scope 租约（checkpoint 上的
  owner+version CAS）串行化同一 `binding+open_kfid`；每页一个事务：整页
  `AcceptInTx` + reply route + 游标 CAS + （末页）notification done。同 id 不同
  内容 → notification 标 `failed` 停下来给人看，不无限重试。

## 3. 测试策略

- 9 套真 MySQL 集成测试（各用独立库与独立 DSN 变量，`go test ./...` 并行互不干扰；
  这是被「两个包同时 drop/建表」实测打出来的规则）。
- 协议桩复用既有资产：KF 拉取/发送走 `cmd/fake-model` 新增的官方 wire 形状端点
  （可脚本化分页、可注入 errcode、记录 `send_msg`）。
- `scripts/reliable_e2e.sh`：31 条断言，自起自拆，纪律与 `e2e.sh` 同（就绪门禁、
  helper 逐例签名自检、JSON 走文件）。

## 4. 实测/验收（本次真跑，可复跑）

```
bash scripts/reliable_e2e.sh   →  31 PASS / 0 FAIL / 0 SKIP → RELIABLE E2E PASS
```

其中值得单独引用的证据：

- **双 worker 只执行一次**：两个 worker 副本同时轮询，一条消息的结束状态是
  `executions=1`、上游模型调用计数 `requests=1`。
- **原子推进**：`in_seq|head_seq|session_version` 从 `1|2|1` 到 `2|3|2`，
  两次提交各推一格；第二条消息 `session_pk` 不变（同一会话续聊）。
- **投递闭环**：假 KF 收到 `to=ext-seed-1 / open_kfid=wk1 / 全文回复`，
  且 `reply_outbox` 无 pending/unknown/dead 遗留。
- **游标**：`C1 → C2` 随页提交推进；重启不吃游标（游标在 MySQL）。

未验/未接（如实标注）：

- **可靠 gateway 角色未挂载**：微信客服回调的 HTTP 入口还没接（`Callback` 的
  durable 钩子 `WithDurableNotifications` 已有单测）；企微/webchat 的接收端持久化未做。
  E2E 里「回调已记下 notification」一步由 SQL 模拟。
- **投递通道仅微信客服**；webchat/企微 被明确拒绝（不是静默成功）。
- **可靠链路没有故障矩阵脚本**：kill worker 接管、MySQL 闪断、delivery unknown
  这些场景的包级测试都有，但还没按 D1–D7 那样编排成演练；现有 D1–D7 验的是旧链路。
- **YAML 仍是启动必需**：角色本身不读 tenants，但 `config.Load` 的校验要求它存在
  —— 可靠模式下这份 tenancy 是残留，待控制面 bootstrap 流程收敛。
- KF token 缓存仍是进程内（丢一次缓存只是多一次 gettoken，不影响正确性）。
