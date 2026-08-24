# 公开验收证据

本目录只保存确定性裁剪、固定矩形遮挡后的公开截图。原始截图留在本机临时目录，未提交；截图不含凭据、完整本地路径、成员头像、成员姓名或无关聊天。

## 双 IM 与完整 Trace

| 通道 | 群聊主键 | 新 Tool Trace | 结果 | 证据 |
| --- | --- | --- | --- | --- |
| 飞书企业版 | `oc_4f8cabf6a1460878e726b138d01c7a90` | `9b7f941e-93ae-405f-946a-9b2fe1b6d76f` | `get_server_time`、Reply Outbox `done/1` | [群聊](feishu-group-tool.png) · [Jaeger](jaeger-feishu-trace.png) |
| 企业微信 | `wrkSFfCgAAQeb7frRm-RqgENEXdw7vaA`（群名 `trpc-test`） | `cfa0993e-ad8b-4b33-b1be-c9a442a4a83d` | `get_server_time`、Reply Outbox `done/1` | [群聊](wecom-group-tool.png) · [Jaeger](jaeger-wecom-trace.png) |

两个新 Trace 均包含 8 个 Span：

```text
channel.accept_inbound
outbox.dispatch
worker.process_message
runner.run
agent.governance
tool.get_server_time
session.commit_result
channel.deliver_reply
```

## 租户隔离、动态发布和恢复

- 企业微信询问飞书租户代号，Trace `81d7dcc7-cd88-4ece-a9e1-85ae8f82290a`，明确返回不知道。
- 飞书恢复验收代号 `FEISHU-B` 后询问企业微信租户代号，Trace `e3b3bcf9-45ab-4a82-a009-84179209b251`，明确返回不知道。
- 飞书发布 `0.1.2-acceptance`：revision `3 → 4`，两个 Worker PID 未变化；Trace `71ff354a-929c-49b1-b285-fdbf54e98f10` 精确返回 `RUNTIME-V012`。
- 重新发布 `0.1.0`：revision `4 → 5`，两个 Worker PID 仍未变化；Trace `61bd8077-675c-4456-9c3d-bb205594eace` 不再执行版本探针指令。
- 停止 Worker PID `56464` 后，只保留 PID `49484` 处理企微真实消息；Trace `47a470ea-f375-4445-9d4d-334f90c848b2` 返回 `WORKER-FAILOVER-OK`。随后以同一二进制恢复第二个 Worker。
- 截图：[飞书隔离与发布/回滚](feishu-private-runtime-isolation.png) · [企微隔离与故障恢复](wecom-private-isolation-failover.png)

## 可复核运行状态

- 最终二进制 SHA-256：`7B87035788375E970C4FA796A23D3FF08634DB32A41876C14CCF26C4E39B4238`。
- Admin：`status=ok`、`dispatch_ready=0`、`reply_ready=0`。
- Redis Stream：`pending=0`、`lag=0`；两条历史 DLQ 记录原样保留，没有清理或伪造零故障状态。
- 双 Channel lease 均持续存在；未发现 `cloudflared`、ngrok、frp、frpc 或 frps 进程。
- `backend-smoke --tenant all`：两个租户的 Qdrant 与 MinIO 读写、删除及双向不可读检查均通过。
- `TestRedisQueueClaimsAbandonedPendingDelivery`：隔离 Stream 的 XAUTOCLAIM 接管通过。
- `TestPostgresInboxOutboxIntegration`：隔离测试租户的重复消息 ID 只产生一条 Inbox/Dispatch/Reply。
- OTLP gRPC 集成探针 Trace：`54def9d3-ba59-4ef6-8370-fc49592872ff`，证明 `127.0.0.1:4317` 可写入 Jaeger。

截图由 [`scripts/redact_demo_evidence.py`](../../scripts/redact_demo_evidence.py) 通过固定裁剪与遮挡生成，不使用生成式重绘。
