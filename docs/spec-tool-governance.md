# 受控工具与执行账本 Spec（P3）

> 范围：第二批（可靠闭环）的 P3 切片——「Tool 执行账本与人工核对」。让模型能
> 真正用上工具，同时把「越权、SSRF、schema、secret、不确定副作用」每一类风险
> 变成一条**有真跑证据**的拒绝或阻断。结构沿用另外几份 spec：事实核查 → 设计 →
> 测试策略 → 实测/验收；**没验过的地方如实标注**。

---

## 1. 事实核查（写代码时实测出来的，不是查文档抄的）

| # | 事实 | 证据 |
| --- | --- | --- |
| 1 | 第一次 `client.Do` 会耗掉 `http.Request` 的 body；重试复用同一个 request 会以 body-length mismatch 直接失败——重试**必须重建请求** | `TestHTTPToolReadRetriesOnceOn5xxAndWriteDoesNot` 首版 red（5xx 重试变成了 transport error） |
| 2 | 自定义 `DialTLSContext` 返回裸 TCP 连接会被 transport 当成「已完成 TLS」使用，把 https 静默变明文 | 本文件首版实现即踩（`NewHTTPTool` 初稿）；现在只设 `DialContext` |
| 3 | `errors.As` 只认错误链上真实出现的类型：把 outcome 包在自造的 `callResult` 里，governor 的 `classify` 认不出 `*CallError`，所有 HTTP 结果都会降级成 `Failed/tool_error` | `TestHTTPToolTimeoutsClassifiedByPolicy` 首版 red（打印出 `*tool.callResult`） |
| 4 | block reason 的文本前缀是契约：`TryUnblock` 只处置 `tool ` 开头的 blocked_reason，恢复路径的措辞换了顺序后处置直接 422/错误 | `TestRecoveryBlocksStaleWriteAbandonsStaleRead` red：`not blocked by a tool call: "previous attempt left tool call…"` |
| 5 | `jsonschema/v6` 的 `UnmarshalJSON` 内部用 `UseNumber()`，大整数经校验不失真；Draft 2020-12 的 metaschema 是 **embed** 的，整个校验过程离线可跑 | 依赖落地后 `TestCompileAndValidateDocumentLocalSchema` + 精度断言通过 |
| 6 | 可靠模式的部署里**没有进程挂 admin HTTP**（roles 只有 worker/delivery/jobs）：人工处置必须有 CLI 形态，否则 E2E 与真实运维都够不着 | E2E 里用 `-list-blocked` / `-resolve-session`，与 admin/v2 走同一个 service |
| 7 | E2E 脚本的断言是「活」的：多加一版 migration（0003）就会打红「迁移落了两版 schema」 | `reliable_e2e.sh` 首轮 62/1，唯一红的就是这条遗留断言 |
| 8 | 「模型流式 tool_call」并不是猜的 wire 形状：`llmagent` 在收到 `finish_reason=tool_calls` 后会执行工具、把结果作为 `role=tool` 消息发第二轮 | 集成测试断言上游收到 2 次请求且第二次带 tool 角色，才把它固化成 fake-model 的 `tools` 模式 |

## 2. 设计

### 2.1 账本（migrations/0003_tooling.sql）

- `tool_calls`：一行一次**逻辑调用**。`call_id = execution_id:fence:seq`——同一
  execution 的每次领取（更高 fence）写新行，死掉的那次留在原地，正是恢复检查
  要看的历史。行内含 `side_effect / idempotent`（从绑定冗余，撤销后仍能判断）、
  `arguments_hash`（模型原样的 sha256）+ `arguments_masked`（密钥值全部 `***`）、
  `status / attempts / latency / resolution / resolved_by`。
- `tool_call_attempts`：一次物理请求一行（HTTP 的允许重试会留下第 2 行），
  「安全重试计数受限」的原始证据。
- 写入不参与 execution 的提交事务：意图行在调用**之前**写，结果行在调用**之后**
  写，各自独立短事务。进程死在中间 → 行停在 `running`，恢复规则把它当「可能
  已经发生」。

### 2.2 Governor（`trpcservice/tool/governor.go`）

每个可靠执行装配一个 governor，检查顺序即契约：

1. 已有 Unknown → 一切调用直接拒绝（`ErrExecutionBlocked`）；
2. 每 execution 调用预算（默认 8，拒绝也计数）；
3. `risk_level=high` → `ErrHighRisk`（审批流后置，先拒绝）；
4. 输入 schema（Draft 2020-12，文档内 `$ref` 之外一律拒绝，64KiB/深度/节点上限）；
5. 绑定复核：每次调用重读 `tool_bindings`，撤销或版本漂移即拒绝（不进网络）；
6. journal `Begin`——**fail closed**：写不了账本就不调用；
7. 调用（单次超时 ≤ 30s，默认绑定值）；
8. journal `Finish`；结果为 Unknown → 记录阻断原因、取消整个 run。

恢复检查（`CheckRecovery`，领取后、模型前）：旧 fence 的未决行里，
`write 且非幂等` → 阻断挂起；`read / 幂等` → 标 `abandoned` 后允许重跑
（读没有副作用，重放是文档化的安全动作）。

### 2.3 受控 HTTP 工具（`trpcservice/tool/httptool.go`）

- URL、method、headers 来自管理员编写的绑定 spec，模型只能给参数；
  凭据只以 `secret_refs`（env:/file:）引用，经 `trpcservice/secrets` 的
  allowlist 解析——不在名单内 = 装配期失败，而不是「静默无凭据调用」。
- 地址策略：loopback、link-local（含 169.254.169.254 元数据地址）、private、
  unspecified、multicast、100.64/10 一律拒绝；唯一出口是绑定里**精确域名**
  `allow_hosts` 或 CIDR `allow_cidrs`（通配符被编译期拒绝）。解析与拨号在同一个
  函数里：检查的是即将连接的那个 IP。不跟随重定向、不走环境代理。
- 失败三态：4xx/重定向/建连失败 = `Failed`（什么都没发生）；非幂等 write 的
  超时/EOF/5xx = `Unknown`；read 的 5xx 允许**一次**重试（attempt 行留痕）。

### 2.4 装配与处置

- 可靠模式的工具只有一个来源：`agent_revisions.tools` 的 `{"pinned":[{name,version}]}`。
  legacy 的 `{"allowed":[...]}` 在可靠路径**没有入口**（防止名字级白名单悄悄
  放回不受治理的内置工具）；pin 缺失/撤销/无实现 → 执行失败，不静默降级。
- 阻断（`execution.BlockForReview`）：与提交同样的条件复检；不推进队头、不写
  session 事件、不发任何回复，只落：attempt=blocked、execution=unknown、
  inbox=unknown、`sessions.blocked_reason`、审计行、Redis 投影任务。
- 处置：`confirmed`（副作用已发生）→ 队头retire + 一条固定文案回执（幂等键
  `resolve:<exec>:confirmed`）；`cancelled` → inbox/execution 回 pending，等下一个
  worker 重跑（旧账本行已 resolution，恢复检查放行）。多调用时全部未决清空才解锁；
  任一 `confirmed` 取胜（不能重跑已确认发生的工作）。
- 入口两枚、同一实现：`/admin/v2/tool-calls`（HTTP）与
  `-list-blocked` / `-resolve-session`（CLI，供无 HTTP 进程的角色部署）。

## 3. 测试策略

- `trpcservice/tool` 纯单测：schema 编译/校验/越界、远程 `$ref` 拒绝、
  SSRF 地址分类全表、redirect 不跟随、响应上限、read 重试恰一次 / write 不重试、
  超时分类、掩码、pin 解析、secret 越界。
- `trpcservice/execution/tools_ledger_test.go`（真 MySQL + 真 HTTP）：全链路
  tool 调用（断言上游二轮、账本 succeeded、attempts=1、物理 attempt 200）、
  unknown 阻断→处置→重跑、恢复（stale write 阻断 / stale read abandon）、
  拒绝矩阵（schema/risk/revoked/budget 各一行账本）。
- `trpcservice/admin`：操作员路由的形状与校验（401/400/404/405、空列表）。
- `scripts/reliable_e2e.sh` E/F 段：容器里真栈的 63 条断言。

## 4. 实测/验收（本次真跑，可复跑）

```
TOOLS_MYSQL_TEST_DSN=… go test ./trpcservice/execution/ -run 'TestTool|TestRecovery|TestGovernor'  →  4/4 PASS
bash scripts/reliable_e2e.sh  →  63 PASS / 0 FAIL → RELIABLE E2E PASS
```

E/F 段单独引用：

- **工具全链路**：模型（fake-model `tools` 模式）发起 `echo_upstream` 调用 →
  账本 `succeeded / kind=http / attempts=1`；物理 attempt 行 `succeeded/200`；
  假工具目标 `hits=1` 且 body 含模型给的 `ping-from-tool`；最终回复
  `tool round complete` 经 KF 投递到 `ext-tool-1`。
- **unknown 阻断**：`charge_upstream`（write、非幂等）挂在 1.5s 超时上 →
  会话 `blocked_reason LIKE 'tool %'`、execution/inbox=`unknown`、队头不动、
  零回复、账本 `unknown attempts=1`（没有重试）。
- **人工处置与重跑**：`-list-blocked` 报出未决调用 → 上游恢复后
  `-resolve-session … -resolution cancelled` 输出 `unblocked: true` →
  重跑提交（execution=committed、队头 1→2）、账本同时留下
  `unknown+cancelled` 与 `succeeded` 两段历史、`tool_resolved/cancelled` 审计行。

未验/未接（如实标注）：

- **可靠 gateway 角色仍未挂载**：工具轮的消息入口仍走 KF 拉取，回调 HTTP 面
  同 P2 切片（`WithDurableNotifications` 已有单测，路由未挂）。
- **SSRF 的容器级演练只覆盖显式 allow_hosts 路径**（fake-model 服务名）；
  公网 https 目标的真实拨号、DNS rebinding 对抗只在单测层面覆盖。
- **secret resolver 的 file: 根 allowlist 未在容器里演练**（env: 路径已验）。
- **幂等 write 的 stale 恢复**（idempotent=true 时 abandon 重跑）只有代码路径与
  单测分类覆盖，没有容器 E2E 场景。
- **工具并发**：框架串行执行工具；并行工具调用下的预算/阻断语义未演练。
- **confirmed 处置的回执投递**有集成测试覆盖（reply_outbox 幂等键），容器 E2E
  只演练了 cancelled 分支。
