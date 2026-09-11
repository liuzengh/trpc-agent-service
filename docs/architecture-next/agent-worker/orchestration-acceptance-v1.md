# 普通工具与 SDK 编排：最终验收矩阵

本记录区分生产运行能力、确定性正式业务验收、真实模型指令遵循、真实数据后端组合。
不把 SDK 编译、helper 单测、数据库锁或旧单 Agent 结果代替组合运行证据。

## 已完成的运行包

| 包 | 实现提交 | 已验证的业务链 |
| --- | --- | --- |
| 显式普通 MCP | `96c2017` | 正式发布→固定选定工具→真实 Streamable HTTP MCP→正式 Session/Final；业务 IsError 修正与真实 DeepSeek 正常调用 |
| Sequence | `7c6a2bd` | 嵌套有序 Chain，节点级模型/工具/数据依赖，终端失败与后继恢复，真实模型正常链、既有 GUI 同 Manifest |
| Parallel | `703dd0d` | 实际并发、两种相反完成顺序、显式汇总、兄弟历史隔离、分支失败取消与迟到事实复查；真实模型正常链和既有 GUI |
| Loop | `9cbce3f` | 既有 body/max_iterations→Cycle，1/2/3/32 精确次数/历史/usage，末轮错误/空文本/取消不提交早轮结果；正式 fixture 恢复和 GUI 差异输出 |

每包都保留一个根 Runner、一个平台 Run、原正式 Session Stage→Complete 协议；
没有额外工作流 DSL、任务调度平台、累计 Token 预算或跨后端补偿事务。

## 真实模型边界

MCP、Sequence、Parallel 的正常真实 DeepSeek 场景已通过，不能延伸为任意指令、任意
模型或故障场景必然成功。Loop 两次额外“逐字引用/第二轮标记”场景均严格 FAIL：一次
改写，一次两响应相同；没有将 FAIL 改绿，也没有继续付费重试。

Loop 最后一次 live 已有恰好两次 HTTP 200、第二实际请求完整携带首轮 assistant 内容、
唯一正式候选/Completion/Final、Gateway receipt/part ACCEPTED 和 Lab 等于实际第二
响应的证据。由于两次输出相同，该 live 不独立证明首末选择，差异输出的确定性 SDK、
完整 fixture 和 GUI 证明只交付末轮。模型指令遵循未满足不是已确认的 Worker 缺陷。

## Parallel 真实数据后端组合

| 后端与组合 | 实际正式事实 | 状态 |
| --- | --- | --- |
| Artifact：真实 MinIO + Worker PostgreSQL | 3 Run：并行不同名保存；同名两份内容分别关联版本 0/1，逐版本真实对象字节/hash/length 与 PG metadata 匹配；确认 A 保存成功后 B 模型 401，0 Session candidate、accepted head 不变，A 对象与 metadata 仍存在 | PASS |
| PostgreSQL Memory | 3 Run：两并行 add 后后继 load 两条完整内容；正式 APPLIED 后新 Run 的三个节点只 load、ID/时间/正文不变；确认 A 私有 add 成功再 B 401，正式记录与 Session head 不变、0 candidate | PASS |
| Redis Memory | 与 PG 相同三场景，真实 Redis 原始 body/digest/revision 与正式 Completion/receipt 对齐，不以 PG 结果替代 | PASS |
| Knowledge：真实 Qdrant + 固定 embedding | 两资源经正式 owner API 分别导入；同 named vector `[1,0,0]` 下两个实际 SDK 并行检索只返回各自 canary；显式汇总→正式 Session/Final/Lab。真实 tenant scope 正对照命中、负对照为空，跨 tenant owner 导入 404，points 前后不变 | PASS |

Artifact 实际并发由两首请求入站/释放的单调时钟 barrier 证明，同名两版本不假设
分支先后。每 Run 实际读取唯一 Attempt/Completion/Final outbox、Gateway receipt/part
及 Lab 正文。Artifact 的即时保存不承诺因同 Run 的另一分支失败自动撤销。
两个 Memory 后端的正式 revision 都为 1→2→2（成功只读 Run 仍走原接受协议），
各 Run 模型 HTTP 次数 6/6/3、SDK Memory 工具次数 3/3/1。失败后等全部已观察
HTTP handler 退出，再读原始正式记录无变化；两个后端的 digest 与 Completion 对齐。

最初 MinIO provision 在 ready=200 后 bucket PUT 返回 503，尚未进入业务；修正新测试
子 Harness 的有界认证就绪探测后必要重验通过，原失败保留，生产代码未变。

## 本轮发现的生产接缝修复

真实 Knowledge 双资源正式导入暴露旧接缝：bootstrap `knowledgeQueries.ImportKnowledge`
只查单 LLM 的 `Plan.Knowledge`，而组合 Reader 使用 `Plan.Knowledges`，导致已合法
发布的图在第一条正式 import 返回 403。原失败保留，尚未进入 Agent Run。

最小修复仅在已验证组合 Plan 的资源 union 中按请求 resource 精确查找；保留原单 LLM
入口，不访问 Profile 全量资源、不因同 backend 放宽授权，不在图资源缺失时回退到旧
指针。原 Tenant/Manifest ref/digest/DeploymentRevision/资源身份检查保持。定向红绿
及受影响 bootstrap/HTTP/Reader 三包 race 通过，真实双资源链必要复验已通过。跨 tenant 负例是实际 Control/Qdrant 固定 scope 对照，
不是第二 tenant 的 SDK Run；固定 embedding 不证明外部语义检索质量。

这是一项实际生产修复，不把最终增量称为纯测试包。Memory/Artifact 成功证据在修复前
取得，其后不重跑不受影响的执行链；Knowledge 新 Worker 二进制单独记录，不能声称
所有 backend gate 都使用同一 binary。最终统一 Go/Web 覆盖最终源代码。

## 统一测试与剩余边界

最终源代码统一回归：

- `GOCACHE=/tmp/trpc-worker-go-cache go test ./... -count=1`：113 个有测试包 PASS，exit 0。
- `npm run test -- --maxWorkers=2`：61 个测试文件、861 tests PASS，exit 0。
- `npm run lint`、`npm run build`：均 exit 0。
- 联合脚手架单测：77 tests PASS，exit 0；仅作为 fixture/断言门禁，不替代上述实际后端。

普通 Go 集成测试可能依环境 skip，真实后端结论只依据本页明确的独立栈运行及原始事实。
所有独立业务栈清理，未推送或改共享部署；最终源代码与提交状态以交付 SHA 为准。

外部 Embedding 仍缺明确可用配置：确定性 HTTP embedding 只证明 Knowledge 资源隔离、
SDK/后端检索链，不证明外部语义质量。Channel Lab 是实际 HTTP 模拟 IM，当前新增编排
不借用旧 Telegram 验收宣称真实 Telegram 已测试；live 故障注入、任意深层嵌套与所有
数据能力交叉组合也不由有限矩阵替代。
