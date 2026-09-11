# 正式 Session Summary V1

## 实现边界

Summary 是原 Session 的能力，不是第二个数据库或服务。最初正式 Session 使用
PostgreSQL `runtime_session.session_candidates`，后续也支持 managed Redis Session
（见 `session-redis-v1.md`）；两者都将摘要正文、SDK structural boundary 与原事件
保存在同一个不可变 snapshot 中。Worker 原有 `Complete`
接受事务决定哪个 snapshot 成为正式 head；仅写入候选不等于正式保存成功。

本节记录最初开放 `summary` 的独立交付边界。当时未同时开放其他数据能力；后续已完成
PostgreSQL/Redis Memory，以及 managed Redis Session + 同后端 Summary，分别见
`memory-postgres-v1.md`、`memory-redis-v1.md`、`session-redis-v1.md`。后续 Artifact 与
Knowledge 的装配、正式 HTTP/Web 和独立真实存储验证也已完成，见 `artifact-v1.md` 与
`knowledge-v1.md`；Knowledge 的真实外部 Embedding 验收仍待明确可用配置。
没有增加跨库事务、CAS 平台、恢复队列、补偿任务、后台调度或累计 Token 预算。

## 从配置到执行

1. AgentSpec 声明 `runtime.summary`，引用一个明确的模型 slot，显式给出事件阈值。
2. Profile 的 `models` 给该 slot 绑定模型、endpoint 和凭据；可以复用主模型 slot。
3. Control 编译所需模型、Session 和 credential use 的最小闭包，发布不可变 Manifest。
4. Worker Reader 校验发布身份、内容 digest、固定 release contract 和模型/端点闭包，
   把 Summary 模型、凭据用途、阈值和消费开关映射到 `Plan.Summary`。
5. Factory 一次解析固定凭据批次；独立模型分别使用自己的凭据，相同完整 use 去重。
6. Executor 创建 SDK `summary.NewSummarizer`，安装在本 Attempt 的 Session overlay。
   `llmagent.WithAddSessionSummary` 按节点显式声明装配，不隐式消费摘要。
7. 完成的摘要进入同一 snapshot；实际 Stage → Complete 接受后，下次 Claim 的 Parent
   才指向它。下一 Run Load 恢复，并由 SDK 放入模型上下文。

AgentSpec 最小示例（其他业务 instruction 可自行调整）：

```json
{
  "schema_version": "v1",
  "root": "assistant",
  "requirements": {
    "models": {
      "primary": {"capabilities": ["chat"]},
      "summarizer": {"capabilities": ["chat"]}
    },
    "tools": {},
    "knowledge": {}
  },
  "runtime": {
    "summary": {
      "enabled": true,
      "model_slot": "summarizer",
      "event_threshold": 1
    }
  },
  "nodes": {
    "assistant": {
      "kind": "llm",
      "model_slot": "primary",
      "instruction": "Answer using the conversation context.",
      "tool_slots": [],
      "knowledge_slots": [],
      "add_session_summary": true
    }
  }
}
```

Profile 沿用既有 `models.primary` / `models.summarizer` 绑定和独立 credentials
写入协议，Session 继续绑定既有 `postgres_state`，运行用户为 `session_runtime`。
Summary 没有额外 storage role。模型可独立配置，也可以将 `model_slot` 指向 primary
并省去第二个 requirement/Profile model。

## SDK 语义、失败与启停

- SDK v1.11.2 事件阈值是符合当前摘要视图的 event count **大于**阈值，而不是大于等于；
  首轮不保证生成摘要。V1 不改变 SDK 的事件选择、summary boundary 或摘要提示词。
- 摘要生成和本 Attempt 使用同一取消/截止上下文，不提交脱离 Attempt 的后台任务。
- 主模型和摘要模型分别使用发布的每次输出限制；摘要不继承主节点更小的 generation
  override，也没有另加累计输出上限。成功 Run 的 usage observation 是两类调用合计，
  不是新增 usage 持久化表。
- 摘要 401 等确定性模型错误使 Run 正常失败；429/5xx/网络错误沿现有依赖分类处理。
  不误报为 Session 损坏。失败不 Stage 候选、不推进正式 head，后继不读取失败轮输入。
- 失败 Run 仍遵守原有终态回复协议，可以发送固定失败 Final；这不代表接受了失败正文。
- 在同一 Session overlay/候选恢复层，关闭 Summary 时保留既有摘要 metadata，但不生成、
  不消费；再次开启可继续使用已接受 metadata。这不表示跨发布的历史连续：现有
  Session scope 包含 DeploymentRevisionID，正式发布修改 Summary 产生新 revision 时
  建立新 Session，不自动继承旧版本历史或摘要，也不删除旧版本的 accepted Session。

## 验收入口

在仓库根目录执行：

```sh
./scripts/test-worker-summary-vertical.sh
python3 scripts/test-worker-summary-joint.py --race --artifacts /tmp/summary-joint-evidence
```

第一个命令创建独立 PostgreSQL，测试完整 Reader → Factory → Processor → SDK →
正式 Session 接受与下一轮恢复；Profile 和模型使用明确的 HTTP fixture。

第二个命令使用真实 Control、Gateway、Worker 进程及独立 PostgreSQL/NATS，通过正式
HTTP 创建 Agent/Profile/Deployment 并发布，不用 SQL 构造业务状态、不手工注入 Plan。
外部模型是确定性 HTTP fixture，用独立主模型/摘要模型凭据和确定 marker 验证上下文。
IM 使用原有 `tools/channel-lab/app.py` 独立进程和 SQLite，通过真实 `/lab/chat`
自动 webhook 入站、实际 `sendMessage` 出站；不是旧 TelegramFixture，也不手工
注入 webhook。此命令的模型仍为确定性 HTTP fixture，不代表真实外部模型可用性。

真实外部模型验收使用另一个入口（密钥仅从指定文件读取，不写入报告）：

```sh
python3 scripts/test-worker-summary-live.py \
  --env-file /absolute/path/to/.env \
  --key-name deepseekapi \
  --model deepseek-v4-flash \
  --provider-base https://api.deepseek.com/v1 \
  --artifacts /tmp/summary-live-evidence
```

该命令同样通过实际 Control HTTP 发布配置，以透明观测代理原样转发模型请求正文、
模型名、生成参数和流式响应至真实 HTTPS Provider，不使用测试 Model 或模拟模型输出。
本次验收中主模型与 Summary 复用 primary，验证了同一模型资源的正常装配。

2026-09-09 已验证结果：

| 验收 | 已观察结果 |
| --- | --- |
| PostgreSQL 纵向 | 5 Run、4 个成功 Session commit；失败轮 0 candidate；下一轮不含失败输入 |
| 全进程 + Channel Lab + 确定模型 | 3 成功 Run + 1 摘要 401 失败；逐轮精确摘要版本、usage、正式 head、成功/失败 Final 与 Lab 输出全集均匹配 |
| 全进程 + Channel Lab + 真实 DeepSeek | 3 成功 Run，5 次 HTTP 200（3 主模型 SSE、2 摘要 JSON），真实 provider usage 合计 1,627 tokens |
| 摘要/回复一致性 | 正式摘要逐字等于真实摘要响应，下一次主请求包含前 accepted 摘要，Lab Final 逐字等于真实主模型输出 |
| 清理与凭据 | 独立进程/PG/NATS/Lab 均清理成功；真实 API key 扫描无泄漏 |

真实模型报告为 `summary-live.json`；联合故障报告为 `summary-joint.json`；Lab 最终
输出全集与清理见 `channel-lab-outgoing-final.json`、`channel-lab-cleanup.json`。
这里证明 Channel Lab 的真实 HTTP 入口，不宣称浏览器 GUI 或真实 Telegram 验收。

## 部署合同与既有运行栈

本批默认 Worker 合同增加 `runtime_data_capabilities: ["summary"]`，因此 digest
变化；同一版本字符串并不意味着内容 digest 相同。严格 release pin 继续生效，不把
任意旧/新 digest 加入动态信任列表，也不改写历史 Manifest。

`.env.example` 已同步当前冻结契约对应的 digest。真实环境应自行计算，不复制文档中的值：

```sh
go run ./services/control-api/cmd/control-api -print-deployment-contract-digest
```

将输出同时配置到 Control 的 `CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST` 和 Worker
JSON 的 `platform_contract_digest`。新 Agent/Profile 通过正式 API 发布为新 Deployment
revision，绑定切换到新 revision 后才会生成匹配的新 Run；历史发布物保持不变。

对正在运行的栈，应先停收待升级账号/绑定的新输入，确认旧 Run 队列和在途 Attempt 已
结束，再协调 Control/Worker release pin、新发布物及绑定的切换，最后恢复接收。不要
仅升级 Worker 的 pin 后继续向它投递旧发布物。本批验收运行在独立环境，不自动替换
用户正在使用的共享服务、Bot 或数据库。

## 四能力组合补验（2026-09-09）

同一发布 Manifest 下，Memory + Summary + Artifact + Knowledge 连续 3 个正式 Run
通过。每轮单 Attempt；摘要原文与 SDK boundary 随同正式 Session 接受，后两轮首个
真实 DeepSeek 请求逐字包含前轮 accepted Summary。7 次主模型与 6 次摘要 HTTP
调用均成功，3 条 Final 的 Worker outbox、Gateway receipt/Delivery 与 Channel Lab
正文逐字对应；Memory 每轮 APPLIED，文件保持原版本，知识点只读不变。

本组合使用 PostgreSQL Session/Memory、真实 S3/Qdrant，Embedding 单独明确为
HTTP fixture。Redis Session/Memory 的真实后端验收由各自独立门禁记录，不把本次
PostgreSQL 组合宣称为全部后端组合矩阵。未切换共享部署，也未新增真实 Telegram
验收。

## 当前 Worker 分支复验（cfce361，2026-09-09）

在已包含正式 Summary 实现的 `cfce361` 上重新执行运行门禁，未重写已完成的
摘要装配，也未改变 Manifest 或发布 pin。验收使用独立 PostgreSQL/NATS、真实
Control/Gateway/Worker 与 Channel Lab HTTP，模型为确定性 HTTP fixture。

- `test-worker-summary-accepted-head.sh`：race 模式主测试与五个子分支，
  覆盖下一 Attempt 恢复/SDK 消费、摘要失败无候选、失败 completion、
  rejected parent、stale lease；禁止跳过测试。
- `test-worker-summary-joint.py`：正式 HTTP 发布 Agent/Profile/Deployment；
  三个成功 Run 验证摘要生成、正式接受、下一轮精确消费及 Final，
  一个摘要 401 失败 Run 验证正式 head 不变、无候选和既有失败回复。
- 原始报告保存在 `/private/tmp/worker-summary-current-20260909/`。
  `summary-joint.json` 明确区分模型 fixture 与真实服务链路。

本轮没有调用外部付费模型，没有替换共享 Control/Worker/Gateway 或 Telegram Bot；
临时服务由验收脚本清理。源码实现已由 `76027c6` 提交，本节仅补充当前分支复验记录。


### 22a030f 独立源码复验

2026-09-09 再次从已提交的 `22a030f` 导出独立源码，运行 accepted-head race
门禁与全进程 Summary joint 门禁，均 exit 0，且临时资源清理 PASS。报告见
`/private/tmp/worker-summary-final-20260909/joint/summary-joint.json`。
当前工作树同时有尚未完成的 Workspace 附件装配，其缺失共享字段导致直接编译失败；
本次通过结果仅对应上述已提交源码及文档变更，不代表未提交附件改动通过回归。
模型使用确定性 HTTP fixture，IM 使用真实 Channel Lab HTTP，不新增付费模型调用。


### 当前整合源码闭环复验（3d1c59e，2026-09-09）

此前 Workspace 未完成导致的编译状态是历史记录；本轮直接在已整合 Workspace 的
`3d1c59e` Worker 源码上运行，不再使用旧版本导出源码替代当前代码。

- accepted-head race 门禁：1 个主测试、5 个子分支全部通过，零跳过。
- Reader → Factory → SDK → PostgreSQL 纵向门禁：5 Run 全部断言通过，零跳过。
- 全进程联合门禁：正式 Control HTTP 发布、凭据解析、Channel Lab 入站、Gateway、
  Worker SDK、正式 Session 摘要保存、下一轮精确消费、Final 和 usage 均通过。
  3 个成功 Run 与 1 个摘要 401 失败 Run；失败轮无候选且 accepted head 保持不变。
- 修复两个独立 PG 门禁的启动竞态：由 socket `pg_isready` 改为 TCP 连接目标数据库
  执行 `SELECT 1`，避免初始化临时服务器提前报告 ready。修复后两门禁均重跑通过。
- 原始命令、结果和回滚副本见 `/private/tmp/worker-summary-closure-20260910/VERIFICATION.txt`；
  联合事实见该目录 `joint/summary-joint.json`。全部临时资源清理通过。

本轮模型为确定性 HTTP fixture，渠道为 Channel Lab；未调用付费模型、未执行真实
Telegram 验收，未更换常驻服务、Bot 或主目录。运行能力与存储协议没有新增字段。
