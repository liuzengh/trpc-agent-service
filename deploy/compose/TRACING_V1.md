# IM Tracing V1：独立观测栈

当前资产支持 Collector → Tempo → Grafana 的实际导出、trace_id 查询和 run_id 搜索。
这是观测部署与查询 gate；真实业务进程重启、Telegram 联合结果另记在实施计划的 M5。

## 1. 边界与版本

- `compose.tracing.yaml` 是独立、显式 `tracing` profile；没有 PostgreSQL、NATS、
  Control、Worker 或 Telegram receiver，不读取业务 `.env`、模型 key 或 Bot token。
- Collector 0.133.0、Tempo 2.9.0、Grafana 12.1.1 均固定镜像 digest；这是实际验证的
  可复现组合，不使用浮动 `latest`。版本变更重新运行下述 gate。
- Tempo 采用 2.9 单二进制/local storage，独立 named volume，保留期 24h；
  Grafana 有独立 volume。没有把完整 Span 存进业务 schema，也没有引入 Kafka。
- 默认所有宿主端口只绑定 127.0.0.1：OTLP 14318、Tempo 13200、Grafana 13000、
  Collector health 13133、Collector metrics 18888；环境变量可显式改端口。
- Docker bridge 为本项目独立网络；非 `internal` 网络使 Docker 的宿主 loopback
  端口转发可用。Tempo 的 OTLP 4318 不发布到宿主。此网络设置不等同禁止容器外连。
- Grafana 关闭匿名查询、注册、analytics/update/plugin 自动检查，通过密码文件
  初始化 `trace-admin`。首次初始化后修改文件不等于修改已存在 Grafana 用户密码。
  当前查询只面向平台运维；service/run/tenant 标签不是多租户授权。
- 容器只读 rootfs、移除 capabilities、no-new-privileges；写入仅在专属数据卷。
  Collector/Tempo/Grafana 的内存分别为 192/512/256 MiB，这些是观测部署配置，
  不改变 Manifest 或 Worker 执行预算。24h 是保留策略，不是磁盘硬配额。

## 2. 启动本机观测栈

先准备仅用于 Grafana 初始化的密码文件，保持其上级目录私有；容器 uid 需要读取所挂载的文件。
本机应用可使用显式 loopback HTTP，容器应用使用下一节 TLS overlay。

```bash
export TRACING_GRAFANA_ADMIN_PASSWORD_FILE=/absolute/private/grafana-admin-password

docker compose -p trpc-agent-tracing \
  -f deploy/compose/compose.tracing.yaml --profile tracing config --quiet

docker compose -p trpc-agent-tracing \
  -f deploy/compose/compose.tracing.yaml --profile tracing up -d
```

- Grafana：`http://127.0.0.1:13000`，用户 `trace-admin`，密码来自准备的文件。
- 固定 datasource UID：`im-runtime-tempo`，名称 **IM Runtime Traces**。
- Grafana Explore 选择该 datasource，可直接查询 Trace ID，或用 TraceQL：
  `{ span.app.run.id = "实际内部 Run ID" }`。
- Tempo 的 `/api/traces/<trace_id>` 和 `/api/search?q=...` 可用于本机验收。
  查询结果中的真实 traceId/spanId/parentSpanId/links 才是关系证据；不以属性代替 ID。

Worker 原配置新增独立根对象（Metrics 的 `telemetry` 不变）：

```json
{
  "tracing": {
    "traces_endpoint": "http://127.0.0.1:14318/v1/traces",
    "sampling_ratio": 1.0,
    "export_timeout": "2s",
    "batch_timeout": "1s",
    "max_queue_size": 2048,
    "max_export_batch_size": 512
  }
}
```

Gateway 的 `GATEWAY_TRACING_CONFIG_FILE` 指向只包含上述六个内部字段的 JSON 文件，
不套 `tracing` 外层，启用时设置 `GATEWAY_INSTANCE_ID`。两个服务的旧配置默认仍关闭。
使用此新配置前须部署新二进制，并先应用各自追加列迁移；本文件不执行服务切换。

## 3. TLS 接收入口

```bash
export TRACING_TLS_DIR=/absolute/private/collector-tls
# 目录包含 server.pem / server-key.pem；SAN 必须匹配应用使用的 Collector 主机名。

docker compose -p trpc-agent-tracing \
  -f deploy/compose/compose.tracing.yaml \
  -f deploy/compose/compose.tracing.tls.yaml --profile tracing up -d
```

overlay 把同一 OTLP HTTP receiver 改成 HTTPS，最低 TLS 1.3；原明文入口不并存。
应用配置改为 `https://<受信任的 Collector 主机名>:<端口>/v1/traces`。
应用容器需显式加入该独立网络，并将签发 CA 配入系统信任；Linux 可挂载包含系统根与
组织 CA 的 bundle 并显式设置 `SSL_CERT_FILE`。没有 `insecure_skip_verify`，也没有为
Docker 主机名放宽应用的 HTTP 校验。HTTPS 只是服务端证书校验；此 overlay 未开启 mTLS
或租户级 ingestion auth，跨主机入口仍需由平台网络/网关控制。

## 4. 过滤与故障可见性

应用的 `platform/telemetrytrace` 先执行完整白名单，清理属性、Events、Link 属性、Status
描述及导出 tracestate。Collector 重复清理资源/Span 属性、scope 属性、Status/tracestate，
并移除全部 Span Events；保留已经由应用过滤的 Link 关系。Collector 本身不是任意非平台
OTLP 客户端的通用脱敏网关，接收入口只连接这一已接线应用路径。

没有 debug exporter，也没有原始日志接收 pipeline。Collector 使用 memory limiter、
有界 batch、128 批队列、两个消费者及最长 15s 后端重试；应用 exporter 本身仍不重试。
Collector 的 `/metrics` 可查看 accepted/refused/sent/send_failed spans 和 exporter queue；
应用 `Runtime.Stats` 的 Finished−Exported 仍不是精确 drop 数，也不是业务失败数。
业务执行状态仍以 Run/Completion/Session/Delivery 账本为准。

## 5. 可重复验证

```bash
# 创建全新的观测容器、私有随机密码、随机 loopback 端口；结束后只清理本次资源。
python3 scripts/test-tracing-v1-stack.py --artifacts /absolute/evidence-directory

# 可选保留本次成功的观测栈，便于继续联验；结果记录项目名、端口与私有目录位置。
python3 scripts/test-tracing-v1-stack.py \
  --artifacts /absolute/evidence-directory --keep-running

go test ./deploy/compose/tracing ./platform/telemetrytrace
```

脚本使用原生 Collector validate、实际 Compose 启动、应用 Trace Runtime 导出、Tempo
trace/run 查询、真实 ID/parent/Link 断言、正文 canary 负例、Grafana 匿名拒绝及认证
数据源代理的实际 Trace 查询；另启动仅本次拥有的 TLS Collector，验证可信 TLS 1.3 成功、不可信 CA
及 TLS 1.2 拒绝。测试中的四个应用 Span 是观测 smoke，不冒充实际 Runner/IM 执行。

`commands.json`、`stack-result.json`、`stack-trace.json` 保存可复核结果；脚本不从已有
业务部署读取凭据。未提供显式 stack 环境时 Go 的集成测试显示 SKIP，不作 PASS 证据。
脚本默认 finally 清理自建容器/数据卷与私有目录，不影响其他 Compose 项目。

## 6. 关闭与回退

先移除应用 tracing 配置并按业务部署流程重启/回退二进制，之后关闭观测服务：

```bash
docker compose -p trpc-agent-tracing \
  -f deploy/compose/compose.tracing.yaml --profile tracing down
```

该命令保留 Trace/Grafana 数据卷。明确不再保留本次观测数据时才附加 `--volumes`。
业务数据库追加的可空 Carrier 列保留，不执行 down migration，不删除 Run/Session/Reply，
不清业务 NATS stream。源码副本回滚与完整旧二进制兼容验收是两项不同证据。

## 7. 依据

- [Collector 配置](https://opentelemetry.io/docs/collector/configuration/)
- [Collector Transform Processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.133.0/processor/transformprocessor)
- [Tempo 2.9.0 release](https://github.com/grafana/tempo/releases/tag/v2.9.0)
- [Tempo Docker quick start](https://grafana.com/docs/tempo/latest/docker-example/)


## 8. 实际业务进程纵向 Gate

观测栈运行时执行：

```bash
python3 scripts/test-im-tracing-v1.py   --artifacts /absolute/process-evidence   --otlp-endpoint http://127.0.0.1:14318/v1/traces   --tempo-url http://127.0.0.1:13200
```

该脚本编译并启动真实 Control/Gateway/Worker，通过管理 HTTP 发布真实 Manifest，
采用独立 PG schema/角色、TLS NATS、真实 mTLS proof 与 SDK，完成两轮正式 Session
和 Reply。仅外部模型、Telegram Bot API 与 HTTPS 入口是显式 fixture。Trace 查询核对
原始 ID/父子关系、三处持久 Carrier、真实 LLM Span 和正文负例，不手写 trace_id 属性。

随后停止 Worker proof、SIGKILL Gateway，再以相同 Reply bytes/新 broker sequence
重放并重启 Gateway；核对发送次数不增、首次持久上下文不变，以及无 Header 重放形成的
诊断 Trace Link 回原 process。它证明完成后的重放恢复，不替代在 Admission 发布前、
Worker Claim 前、Completion 后发布前或 Delivery 两事务之间断电的专项测试。


### 8.1 精确持久化窗口

在上述命令添加 `--windows`，会额外执行 T06/T07/T10/T11：Admission 发布前、
Worker Claim 前、Completion 提交后发布前、Delivery 两事务之间。每轮先用实际
SQL/broker 状态确认窗口，强杀并重启对应服务，再查询原 Trace 验证因果恢复及一次执行。

输出 `tracing-recovery-windows.json` 保存前后状态、重启时间、权限恢复记录与 Trace ID；
`<run_id>-trace.json` 保存实际查询结果。权限故障只注入脚本创建的私有 PG 实例，
finally 恢复并清理私有进程/容器。强杀丢失的未导出父 Span 单独记录，不伪造补齐。
脚本不操作已有 Telegram receiver，也不读取业务 `.env`。


### 8.2 旧二进制兼容 Gate

```bash
python3 scripts/test-im-tracing-v1.py --windows \
  --rollback-source /Users/jfs/Projects/trpc-agent-service-worker \
  --rollback-ref fd1f790bf4f779216ed44373bd2356aa3ce7e186 \
  --artifacts /absolute/rollback-evidence \
  --otlp-endpoint http://127.0.0.1:14318/v1/traces \
  --tempo-url http://127.0.0.1:13200
```

两个 rollback 参数必须一起提供。脚本只读导出指定 Git 提交，在临时目录构建旧版，
记录提交和二进制 hash。旧版不接受 tracing 配置，所以 Gate 先验证拒绝，再移除该字段。
保留数据库新增列，旧版接续发送新版已提交的 Reply，完成旧版新请求和正式 Session，
验证重复 Reply 不重发，最后恢复新版与 tracing 并查询新 Trace。
`tracing-binary-rollback.json` 记录旧配置校验输出、实际 PID、前后持久状态和再次升级结果。
这不是对已有业务部署的自动回滚命令；所有实例均由本次 Gate 新建和清理。


### 8.3 回复发送矩阵

在实际进程 Gate 命令添加 `--delivery-matrix`，执行 UNKNOWN 接收后响应丢失、
多字节长 Final 分片、getMe 准备阶段 NOT_SENT 重试，以及 sendMessage typed 429
REJECTED。脚本核对数据库 certainty、实际外部接收次数、Tempo Span 数量/父关系及
分片次序，结果存入 `tracing-delivery-matrix.json`。

getMe 与 sendMessage 的 429 具有不同业务位置：前者证明尚未发送并沿原准备预算
重试，后者按现有 REJECTED/rate_limited 终止。脚本不把两者合并成新的重试规则。
此参数可与 `--windows` / 显式旧版回退参数组合；模型和 Telegram API 仍为 fixture。


### 8.4 Session 故障矩阵

添加 `--session-matrix`，在真实发布前固定 Session 协议 relay，验证 Stage INSERT
失败、Complete 事务回滚，以及候选已提交但 PG 响应丢失。测试只在私有实例暂时撤销
两个精确 INSERT 权限，并在 finally 恢复；不直接写业务行。

`tracing-session-matrix.json` 保存失败前后 accepted head、候选数量、正式 Completion、
Trace ID、权限恢复记录和 follower 历史结果。候选响应丢失的 wire-level 证据还存于
`worker-session-commit-loss.json`，不记录原始协议凭据。
Trace 的 candidate/failed/accepted 与数据库事实分别对应；候选存在不等于正式接受。
该参数可与已完成的其他矩阵合跑，外部模型/Telegram 仍是 fixture。


### 8.5 原消息重投和模型重试

添加 `--retry-matrix`，验证 Run 已提交但 broker 拒绝 ACK 后的原 sequence redelivery，
以及一次真实模型 HTTP 503 后的 Execution 重试。脚本记录不同 process/Attempt/LLM
Span 与不变的首次 Carrier，核对只有一个正式候选/Completion/Reply。

`tracing-retry-matrix.json` 保存 broker ACK 拒绝、真实进程切换、模型故障和 Span 身份；
`model-retry-attempts.json` 保存两个实际 Attempt 的状态/时间，不记录 token。
故障只改变本次私有 broker ACL 或外部模型 fixture；ACL 在 finally 恢复，未改变流拓扑
或生产重试预算。可与其他矩阵参数合跑。


### 8.6 流式生命周期矩阵

添加 `--lifecycle-matrix`，验证 Worker SIGTERM 取消、SIGSTOP/租约过期/另一进程接管/
SIGCONT 后隔离，以及真实流式 deadline。`tracing-lifecycle-matrix.json` 记录 partial
SSE、进程 PID/退出状态、generation/lease_epoch、旧 Attempt Span 的实际结束结果和
后续 Session。deadline 场景要求 Reply NONE，没有虚构的 IM send Span。

测试仅向本次创建的 Worker 发信号；恢复暂停进程和标准 fixture 配置均在 finally。
私有 proof relay 断开旧 keep-alive，保留实际 TLS/授权与失败语义，不修改生产路由。
可与其他矩阵合跑；没有新增 Run 取消 API、Manifest 限额或线上服务配置。


### 导出故障门禁（T14）

在现有 `scripts/test-im-tracing-v1.py` 命令中添加 `--export-matrix`，可与
`--lifecycle-matrix --retry-matrix --session-matrix --delivery-matrix --windows`
和旧版二进制回退参数组合使用。接收器拒绝连接/503/慢响应和满 stdout pipe
均只在 harness 私有进程上注入。成功结果写入 `tracing-export-matrix.json`，
包含每轮正式提交、回复、管道未排空、进程 exit=0、停机耗时和恢复后 Trace。
批次仅保留摘要，不保存 protobuf 或模型输入。此门禁不代表真实 Telegram 验收。


### 真实部署只读联合验收（T15）

先通过真实 Telegram 客户端向目标 bot 发送唯一文本并等待真实模型回复。再运行：

```sh
python3 scripts/worker-v1-joint/live_trace_verify.py \
  --evidence-dir "$LIVE_EVIDENCE_DIR" \
  --pg-container "$LIVE_PG_CONTAINER" \
  --input-text "$EXACT_TELEGRAM_INPUT" \
  --tempo-url "$TEMPO_URL" \
  --secrets-env "$EXPLICIT_PRIVATE_ENV" \
  --artifacts "$AUDIT_DIR"
```

此脚本针对本机已有真实 ingress/provider relay 证据格式，不会发送伪造 callback，
不会调用模型，也不写业务表。PG 查询强制只读事务。输入必须唯一，provider 请求/响应
必须配对，模型 HTTP 200，Run SUCCEEDED，当前正式 Session head 必须指向这次唯一
Candidate/Completion，实际 Delivery 的 provider message ID 与 Final 必须一致。
脚本随后查询真实 Tempo 并检查父链、持久 Carrier 和敏感负例；输出
`tracing-live-result.json`，只保存正文摘要，不把 unit fixture 当作真实验收。

2026-09-08 原手测实例已通过该门禁，并保留原数据库和 webhook。当前新版进程由
`/private/tmp/im-tracing-live-l1ndby4p/LIVE_UPGRADE.py` 管理；原状态目录的
`TRACING_UPGRADE.json` 是替换后 Worker/Gateway 的 PID 与归属记录。旧 READY 中的
Control/Ingress/数据库所有者仍有效，旧 HEARTBEAT 的 Worker/Gateway 句柄已退役。
需要回到升级前二进制时，在该审计目录创建 `ROLLBACK.request`；守护进程会先 drain
自己拥有的两个子进程，再使用保留的原配置/二进制启动，不执行数据库 down migration。
该操作是本机临时手测部署控制，不是新增生产编排协议。
