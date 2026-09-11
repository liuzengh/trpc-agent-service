# Delivery Final V1：接纳、发送门与结果恢复

> 2026-09-06 状态对齐：默认 Control 来源已接账户托管凭据、A1/A2 门禁和 Delivery Runner；
> Gateway 当前 10 个迁移（0001–0010）。真实 Telegram 入站已验收；ReplyIntent Consumer、
> 真实 Worker 完成证明与真实 Final 回复闭环仍待接线，见[实施状态](implementation-status.md)。

> 2026-09-05，本轮实现规格。本文冻结当前 Final/text 纵切，不替代完整 Gateway 目标。
> 当前实现与实测结果以 [实施状态](implementation-status.md) 最新章节为准。
> 四 Module 的完整关系见 [边界规范](module-boundaries.md)。

## 1. 本轮实现什么

Delivery 增加独立 Domain、Application、PostgreSQL Ledger、版本化 ReplyIntent 入站 Adapter，
以及直接调用 Telegram Go SDK 和 Connection 受限企微 Sender 的出站 Adapter。
[Runtime V1](delivery-runtime-v1.md) 已补独立维护与有界调度；当前默认 Control App 已接
托管 Telegram 凭据与 Delivery Runner，并由 Runner 拥有 Maintenance。ReplyIntent NATS
Consumer 与真实 Execution/Worker 的已提交 Final 授权读取能力仍未交付。
既有 Final 集成测试使用显式的不可变完成事实 fixture，不注入默认许可到生产装配；
2026-09-05 Runtime 的源码与验收保留于实施状态 §11；当前 Control 接线与真实入站
验收见 §13–14，保留本 Final 切片历史证据而不扩张为完整回复闭环。

同一 Gateway Workload/Go 二进制/镜像保持不变；没有 Node Connector、Connector RPC
或新增 Sender 部署单元。Helm 始终是全部生产 Workload 完成后的 FINAL-INTEGRATION。

## 2. ReplyIntent Final V1

一个 Intent 表示一个完整逻辑 Final，Worker 不根据 Provider 限制预先分段：

```json
{
  "schema_version": 1,
  "intent_id": "final-1",
  "admission_id": "admission-1",
  "run_id": "run-1",
  "execution": {
    "attempt_id": "execution-attempt-1",
    "generation": 1,
    "completion_id": "completion-1"
  },
  "sequence": 1,
  "kind": "final",
  "content": {"type": "text", "text": "完整回复正文"},
  "deadline": "2026-09-05T12:00:00Z"
}
```

- Subject 为 `execution.reply-intent.v1`；intent_id 同时是稳定事件身份。
- Schema 与 Codec 当前只接纳 `final/text`。Progress、媒体、编辑和多展示流仍待后续实现；
  当前不把不支持的输入伪装成已接纳后丢弃。
- Intent 不带 provider/account/tenant/chat/ReplyContext。原目标只来自 Admission 的不可变
  ReadReplySnapshot，经 composition bridge 转成 Delivery 自己的 Target。
- Codec 拒绝未知字段、重复 key、非法 Unicode、超限整数和非规范 UTC deadline。正文
  最多 65536 UTF-8 bytes；执行 generation/sequence 为 1..2^53-1。
- Gateway 重新计算全部稳定业务字段的 RFC 8785 SHA-256，不接受 Worker 自填摘要自证。
  Domain 与 wire codec 有跨边界摘要相等测试。Trace carrier 不进入业务摘要；跨队列 trace
  的持久化和透传仍未接线。
- execution.generation、RouteGeneration、owner epoch、账户 revision 和 socket generation
  是五个不同的身份/版本概念。

## 3. 执行授权与首次接纳

Application 定义 CommittedFinalVerifier，核验的是 Execution 拥有的**已提交且不可变**
Final 完成/Outbox 事实，不是当前 Attempt 是否仍持有活跃 lease。

可信结果必须逐项匹配 IntentID、Digest、AdmissionID、RunID、AttemptID、CompletionID、
ExecutionGeneration、Sequence，以及原 Admission 的 TenantID/ManifestDigest。
Execution 必须在自己的事务里完成有效 Attempt fence、接受执行终态与 Final/Outbox 写入。
NATS 发布权限、Profile 的在线取凭据授权或 Worker 自报成功都不替代此事实。

接纳顺序：

```text
严格输入与摘要 → Receipt-first
→ 原 Admission 回复快照 → 已提交 Final 授权核验
→ 固定分段计划 → 单个 Delivery 本地事务提交 Intent、Final 屏障和全部 parts
```

同 intent_id 同摘要重放原 Receipt，不依赖今日路由/授权服务在线。相同身份不同正文冲突。
准备阶段动态依赖失败时，在原期限内至多一次最终 Receipt 查询，覆盖并发请求先提交的窗口。
新接纳容量与并发均有上限。当前无 GC：默认最多 10000 Intent、100000 part；幂等 Receipt
与每 Run 的 Final 屏障不能随正文删除而丢失。超限返回容量错误而不是无限保留的承诺。

首版每 Run 只有一个展示流，**run_id 是 Final 屏障唯一键**。仅按 `(run_id,generation)`
唯一会允许另一 generation 建立第二个 Final，因此当前相同 Run 的不同 Intent 也冲突。
未来 Progress 进入系统时必须使用同一屏障，不得另造可绕过它的写入路径。

### 3.1 业务 deadline 与有效发送截止：剩余设计门禁

当前 `Intent.deadline` 是必须提供、参与摘要并由完成事实授权的业务字段。现有 Codec
校验规范 UTC 时间，Domain 校验合法年份，PG 首次接纳要求它晚于数据库当前时间；当前
没有根据原回调时间或渠道模式另算一个持久的有效发送截止。它既没有默认延长期，也不
应因为通过了执行授权，就被解释为 Provider 承诺此时仍可回复。

[企微官方长连接文档](https://developer.work.weixin.qq.com/document/path/101463)在普通回复
中规定收到回调后的 24 小时会话回复窗口（2026-09-05 重新核对）。这个表述属于长连接，
不是 URL 回调模式的 response_url；但它不充分定义旧 req_id 的独立 TTL、新回调是否延长
旧关联及本地队列延迟的计时方式。本设计不据此宣称某个 req_id 在 24 小时整必然失效。

D0-08/09 在启用生产发送前还要冻结：

1. **时间来源**：当前 `Target.ReceivedAt` 来自首次入站 handler 的本机时间，不是已验证的
   服务端发送时间或 WebSocket reader 收包时间。队列延迟、时钟偏差、重投不得静默延长窗口。
2. **双字段语义**：保留原 Intent/deadline/摘要不变；由 Delivery 持有另一个本地有效发送截止。
   候选规则是对业务截止、已验证的所选模式窗口和平台最大驻留期限取最早值；具体字段、
   策略版本与时间基准需与 Execution/渠道契约一起接受后实施，不暗改已授权业务正文。
3. **状态与副作用**：仅尚未调用的 part 因有效截止到期转 EXPIRED；已 CALLING/UNKNOWN
   仍按已有确定性与证据恢复规则处理。窗口到期不授权改为主动推送、不换目标或原 ReplyOrigin。
4. **验证**：覆盖排队后首次接纳、临界时刻、晚完成 Final、重投与重连不续期、策略变更，
   以及调用开始前后跨越截止的区别；外部模式窗口还需真实账号验收。

这是既有 deadline/授权议题的精化，不另编 CGR，也不代表此策略已经进入当前运行代码。

## 4. 分段计划

- Telegram 纯文字按最多 4096 个 Unicode code point 切段，保留原顺序与全部内容。
  每段 ID 由 intent_id + part_index 稳定派生，接纳事务一次保存完整计划。
- 企微公开 SDK 的 P0 只支持同 callback 的单次 Final，正文最多 20480 UTF-8 bytes。
  超限明确为不支持；不截断，也不把多次 Final 分段冒充成 SDK 已支持的行为。
- part 独立拥有发送 Attempt 与结果。前面所有段都 ACCEPTED，后段才可领取。
  中间段 UNKNOWN 会阻塞后段；不重发此前已经 ACCEPTED 的整组。
- 单段成功不等于逻辑 Final 完成。完整结果需要读取并核对全部 parts。

## 5. A1、准备、A2 与外部调用

```text
A1：PENDING/到期可重试 NOT_SENT → CLAIMED，生成 claim token/deadline
→ Reserve 本地 Sender，无 Provider 发送副作用
→ A2：重读权威目标/正文，核验 claim/DB 时间/owner，提交 CALLING + 唯一 Attempt
→ A2 确认提交后才调用 Sender
→ 保存受限 Observation → 按当前 Attempt/owner CAS Finish → Release Sender
```

A2 是唯一持久发送门。提交失败或结果不确定时，调用方不发起 Provider 请求；同 claim 再次
调用 A2 不重新获得发送资格。Attempt 返回权威正文与目标，并生成绑定原请求的随机
EvidenceToken；数据库只保存其哈希，该能力从不进入 Worker wire 或日志。

准备失败另用 FinishPreparation 在 CLAIMED 状态收束 NOT_SENT/EXPIRED，或有限重排，
不制造一个已知不会发送的 CALLING 窗口，也不增加真实 Provider Attempt 数。
准备与外部调用分别最多三次；只有明确 NOT_SENT + temporary/rate_limited 可按 1s/2s
退避，deadline 不足则停止。REJECTED、ACCEPTED、UNKNOWN 不走普通自动重发。

每个 ClaimDue 批次限定单 provider/account/instance；企微附带当前 owner fence。
统一锁顺序是 part 行 → Connection 只读 owner guard → 原 Attempt/观察记录。
数据库时间在相应锁之后采样；A1、A2、活动 Finish 都对企微核验 owner，Telegram 完全绕过。
网络调用始终在事务外，本地 fence 不制造外部 exactly-once。

## 6. UNKNOWN 与晚到证据（CGR-32）

- 过期 CLAIMED 可以按 token CAS 回收，因为还没通过发送门。
- 过期 CALLING 保守进入 UNKNOWN。调用前崩溃与已写出后崩溃都可能，没有内存结果不能
  推断 NOT_SENT；恢复不会普通重发该段。
- Observe 必须匹配已存在 Attempt 的 EvidenceToken、ProviderRequestID 和 RequestDigest。
  失租不阻止原调用追加可信观察；重复同 ID 同内容幂等、冲突拒绝。每 Attempt 最多 16 条
  不同观察，达到上限后同 ID 重放仍有效。
- Observation 自身不改变当前投递状态。它允许 UNKNOWN 状态与已知的晚到证据同时存在，
  不把“状态推进被拒”误写成“没有收到明确 ACK”。
- 显式 ResolveObserved 只处理当前 Attempt 对应的 UNKNOWN part：唯一一致的明确
  ACCEPTED/REJECTED 证据才收束原状态。CALLING、新 Attempt、矛盾证据、只有 UNKNOWN/
  NOT_SENT 均不变。该用例无 Provider 调用、无重试，不修改其他 Attempt。
- 恢复记录保留首次 UNKNOWN 的 finished_at，并另记录 resolved_at/原 Observation ID，
  不回写成一开始就确定成功。Resolve 不是 Worker 可调用的“自报渠道成功”入口。

## 7. 企微 ReplyOrigin 与受限 Sender

0006 在 Admission 增加独立 nullable reply_origin：instance_id、epoch、revision、
socket_generation。Handler 从可信 grant 与原 Event.Generation 构造，首次提交保存，
跨 owner/req_id 重投不覆盖。字段不进入 SourceDigest、ReplyContext wire 或 RunRequested。
旧行保持 NULL，明确不可继续当前 P0 Final，绝不根据当前 socket 猜测原来源。

Connection ReserveFinal 精确查找原 lease/client，返回一次性句柄。Reserve 与 Send 都核验
owner/config/本地保守期限；SDK 再核验原 req_id + socket generation。新 Client 的 generation
即使从 1 重新计数，也不能绕过旧 epoch。

Reservation 同时计入 Connection Drain，覆盖 SDK 调用和后续有界结果落账。ACK 返回不
立即释放；正常关闭继续续租直到 Release。Quiesce 拒绝新 Reserve/未调用句柄，失租取消
在途调用；已经匹配的明确 ACK 不因随后失租被改成 UNKNOWN。

## 8. Telegram Adapter

公开 SDK 直接 import。Provider 接收复制后的 account→已配置 SDK Client 映射，不自行
创建凭据服务，也不证明调用方传入的客户端已经完成真实身份核验。配置拥有方必须提供
已认证 Client、有界 HTTP transport 与脱敏 SDK error handler，且不原地修改使用中的 token。

Reserve 零 HTTP；Send 核对捕获 claim token、权威 part/目标/摘要与 A2 身份后调用一次。
回复固定原 chat/topic/source message；迁移错误不自动改收件人。明确 SDK typed 4xx 为
REJECTED；网络/超时/畸形回执为 UNKNOWN；进入 SDK 前已取消为 NOT_SENT。
成功回执还要匹配原 chat/topic 和有效 message_id，原始错误/URL/Provider body 不返回上层。

### 8.1 当前实现限制与后续接线门禁

- 企微累计 Final 身份容量耗尽与瞬时 pending 背压当前同类返回，Gateway 都按 temporary
  处理；账户级区分与有界恢复仍待交付，见[公开库说明](public-go-connector.md#41-累计身份容量不等于瞬时背压gateway-组合门禁)。
- CGR-36 已修正 eventadapter：仅确定 ErrInvalidReplyIntent 映射 ErrInvalid，内部 Schema
  初始化等错误保留为 ErrUnavailable。本轮最终验收已通过；持久拒绝/ACK Consumer 仍待交付。
- CGR-35 已增加独立 ExpirePending、观察候选恢复、PG RuntimePorts、Maintainer/Runner。
  当前默认 Control 来源已启动 Runner 并由它独占 Maintenance，fixture 来源只独立维护；
  expiry 不释放总行数容量，不改变 CALLING/UNKNOWN。
- AdmissionReplyReader 对历史缺少 ReplyOrigin 的记录返回不支持发生在接纳前；已有 Intent
  的原 Sender 失效才属于 FinishPreparation。拒绝前者不代表已经写入 Delivery 拒绝账本。
- Observe 不自动完成状态转换；已持久晚到 ACK 仍需显式 ResolveObserved 校验唯一一致性。

完整目标补充见[Module §7.6–7.7](module-boundaries.md#76-后台调度与无连接时的恢复目标补充尚未实现)，
历史缺口见[复审 §16](design-review.md#16-介绍归档与设计实现对照复审)；当前实现及
验收边界见[Runtime V1](delivery-runtime-v1.md)与[复审 §18](design-review.md#18-delivery-runtime-v1-实现与剩余生产门禁)。
有效发送截止仍是 §3.1 的待冻结策略，本 Runtime 继续使用既有 Intent.deadline。

## 9. 本轮之后仍需实现

1. 真实 Execution Owner/Worker 的不可变 Final 授权、执行终态与 Outbox。
2. ReplyIntent NATS topology、ACL、consumer、重投/隔离及完整生产交接；Control Runner 已接线。
3. 会话级额度与 Provider rate-limit budget；Telegram 账户托管凭据与轮换已由 Control/Gateway 接入。
4. Progress/编辑/媒体、更多企微协议能力与真实 IM Final E2E。
5. 可观测性、保留/GC 策略、bundled NATS 密码预检和全 Workload 集成。

测试中的 committed Final fixture 不是 Worker；本地 HTTP/WS 不是外部 IM 服务。
这些差异必须出现在实施状态与最终验收记录中，不把已完成模块扩张为完整 Gateway。
