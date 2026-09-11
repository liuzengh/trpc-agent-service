# IM Tracing V1 完成审计

- 日期：2026-09-08。
- 分支：`worker`；基线：`fd1f790bf4f779216ed44373bd2356aa3ce7e186`。
- 结论：M0–M5 本工作树开发与首版验收完成；未提交/推送，未声明其他工作树或远端已更新。
- 范围来自冻结计划 §11–12，不把尚未排期的 Memory/生产 Tool/Parallel/预算系统算作已实现。

## 逐项证据

| Gate | 判定 | 实际核对范围 | 证据 |
| --- | --- | --- | --- |
| T01 | PASS | 原配置/显式关闭/独立 Metrics 与 Traces；禁用时不使用环境目标。 | [go-complete.jsonl](/private/tmp/im-tracing-live-l1ndby4p/go-complete.jsonl) |
| T02 | PASS | 有效、未采样、缺失、多值、大小写、非法/过长 Carrier；保留任务取消/截止时间和 MsgID。 | [go-complete.jsonl](/private/tmp/im-tracing-live-l1ndby4p/go-complete.jsonl) |
| T03 | PASS | 真实 SDK Agent/LLM 沿实际父边回到 Runner；流结束前不提前关闭 Runner。 | [go-complete.jsonl](/private/tmp/im-tracing-live-l1ndby4p/go-complete.jsonl) |
| T04 | PASS | 实际 protobuf/Events/Status/Links/日志敏感负例、错误正文与响应边界。 | [go-complete.jsonl](/private/tmp/im-tracing-live-l1ndby4p/go-complete.jsonl)；[tracing-live-result.json](/private/tmp/im-tracing-live-l1ndby4p/live-v2/tracing-live-result.json) |
| T05 | PASS | 当前源码在独立真实 NATS 上验证 Run/Reply payload 字节与去重 ID 不变、重连/重发和最大帧。 | [nats-final-commands.json](/private/tmp/im-tracing-live-l1ndby4p/nats-final-commands.json)；[go-complete.jsonl](/private/tmp/im-tracing-live-l1ndby4p/go-complete.jsonl) |
| T06 | PASS | Admission 提交/发布前的真实窗口恢复；同一 creation Carrier。 | [tracing-recovery-windows.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-recovery-windows.json) |
| T07 | PASS | Worker 接纳 ACK 后/Claim 前恢复；模型执行一次。 | [tracing-recovery-windows.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-recovery-windows.json) |
| T08 | PASS | 原 broker sequence 重投、webhook 重放、一次模型 503 后两 Attempt；不同 process Span，首次 Carrier 和原策略不变。 | [tracing-retry-matrix.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-retry-matrix.json) |
| T09 | PASS | Stage 拒绝、Complete 回滚及服务端候选提交响应丢失；readback、accepted head 与 follower 历史验证。 | [tracing-session-matrix.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-session-matrix.json) |
| T10 | PASS | Completion 后/Reply 发布前恢复；不重复 Runner。 | [tracing-recovery-windows.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-recovery-windows.json) |
| T11 | PASS | Delivery Intent 第一事务后/receipt 第二事务前恢复；Carrier 不覆盖。 | [tracing-recovery-windows.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-recovery-windows.json) |
| T12 | PASS | 实际多分片/NOT_SENT preparation retry/429 REJECTED/UNKNOWN 不重发；确认语义与 Span 对齐。 | [tracing-delivery-matrix.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-delivery-matrix.json)；[go-complete.jsonl](/private/tmp/im-tracing-live-l1ndby4p/go-complete.jsonl) |
| T13 | PASS | 真实非终止 SSE 下 SIGTERM/存活进程失租/真实时钟 deadline；cancelled/fenced/deadline Span 与正式状态对齐。 | [tracing-lifecycle-matrix.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-lifecycle-matrix.json) |
| T14 | PASS | 真实拒绝连接/503/慢 OTLP/满队列和 stdout 管道；正式业务继续、丢弃与失败证据、有界停机及恢复导出。 | [tracing-export-matrix.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-export-matrix.json)；[go-complete.jsonl](/private/tmp/im-tracing-live-l1ndby4p/go-complete.jsonl) |
| T15 | PASS | 真实 Telegram UI 输入/可见回复、真实 DeepSeek HTTP 200、正式 Session/Delivery 账本；29 Span 同 Trace、完整父链、凭据/正文不导出、Grafana/Tempo 按 ID 查询。 | [tracing-live-result.json](/private/tmp/im-tracing-live-l1ndby4p/live-v2/tracing-live-result.json)；[grafana-query.json](/private/tmp/im-tracing-live-l1ndby4p/live-v1/grafana-query.json)；[live-upgrade-verification.json](/private/tmp/im-tracing-live-l1ndby4p/live-upgrade-verification.json) |
| T16 | PASS | 实际旧 fd1f790 二进制拒绝新配置、去掉 tracing 后完整运行；保留八列、旧版消费新 Reply、不重跑 Runner，前向恢复 Trace。 | [tracing-binary-rollback.json](/private/tmp/im-tracing-live-l1ndby4p/completion-evidence/tracing-binary-rollback.json) |
| T17 | PASS | 隔离实际 SDK Tool 成功/错误/取消及实际并发均过滤正文且沿真实父边接回 Runner；孤儿/循环/跨 Trace 父边被拒绝。 | [go-complete.jsonl](/private/tmp/im-tracing-live-l1ndby4p/go-complete.jsonl)；[tool-final.log](/private/tmp/im-tracing-live-l1ndby4p/tool-final.log) |
| T18 | DEFERRED | Memory 按用户确认整体后置；真实 Trace 无 Memory 占位 Span。 | 冻结计划 §7/§14 |

## 阶段与非功能要求

- M0：设计、总体观测文档、Worker 运行说明及部署说明均已更新；本表覆盖全部首版必验 Gate。
- M1：独立显式 OTLP Traces，不改变现有 Metrics；配置关闭/过滤/SDK 桥接/日志/队列测试通过。
- M2–M4：四处 Carrier 追加可空列；原 wire/digest、授权、ACK、两次 Delivery 事务与 Completion/Session 事务不变。精确恢复窗口及真实业务进程测试不是 SQL 修补出来的成功。
- M5：Collector/Tempo/Grafana 实际查询、TLS 1.3/信任验证、匿名拒绝与认证查询已通过；复用栈证据前，对 9 个相关配置/测试源码逐一核对 SHA256 未变。真实 T15 也重新执行了当前栈的认证查询。
- 本轮最终 Go 回归：93 个测试包通过，2815 个测试通过；213 个测试因显式外部环境未配置而跳过，另有 34 个无测试包。跳过不计通过；本切片的 PG/NATS/恢复/外部链路依赖另有具名真实证据。
- 当前源码额外重跑了独立真实 NATS 的 Run/Reply 字节/去重/容量门禁，以及三轮 race Tool 父链与实际并发测试。
- 完整进程组合矩阵来自上一轮 T14 的实际运行；本轮生产 Go 源码和协议未改，只补验收脚本、Tool 测试父边断言和状态文档。
- 回滚包括源码副本恢复及 T16 真实旧二进制兼容；数据库新增列保留，不执行破坏性 down migration。

## 当前本机运行事实

- 原 Control/Ingress/数据库保持运行，原 webhook 地址不变；Worker/Gateway 已由本工作树 race 构建替换，并显式开启 tracing。
- 新子进程由审计目录 LIVE_UPGRADE.py 管理；原状态目录 TRACING_UPGRADE.json 明确新旧进程归属。守护进程随共享资源所有者退出而停止自己的子进程。
- 真实 Run：`4a34369a10c5f4a2bbaa073e210a47b7`；Trace：`6edda2f2a31bd94ba186a9a5fdcdc6dc`。
- 输入/回复 message ID：27/28；真实 DeepSeek 调用一次，Session 正式 accepted head 与 Candidate SHA256 一致。Telegram UI 也观察到 `tracingok` 回复。
- 当前无需新建业务 database；Worker/runtime_session/Gateway 继续原 schema/角色隔离。观测数据使用独立 Tempo/Grafana 卷。

## 后续计划（不是本切片已实现能力）

Memory、生产 Tool/Knowledge/Parallel、polling/WeCom 新模式、Control 全量 HTTP 观测、Loki/Alloy、完整 Dashboard/告警、tail sampling/多 Collector、业务预算系统仍按触发条件独立排期。

机器可读审计：[/private/tmp/im-tracing-live-l1ndby4p/COMPLETION_AUDIT.json](/private/tmp/im-tracing-live-l1ndby4p/COMPLETION_AUDIT.json)。
