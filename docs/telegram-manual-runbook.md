# Telegram 手动测试运行手册

本文记录已经完成首次配置后的日常操作。适用场景是开发者在本机手动启动 PostgreSQL、Redis、Agent 和 Cloudflare Named Tunnel，测试结束后全部手动停止，不配置开机自启。

## 已完成的一次性配置

- Telegram Bot 已创建，Token 保存在仓库根目录 `.env`；
- `telegram-tutorial-binding` 已保存到 PostgreSQL；
- Telegram Webhook 已指向 `https://telegram.4392845.xyz/callbacks/telegram/telegram-tutorial-callback`；
- Cloudflare Named Tunnel 为 `trpc-agent-telegram`；
- Tunnel 配置保存在 `/home/shiyu/.cloudflared/config.yml`；
- Tunnel 凭据保存在 `/home/shiyu/.cloudflared/<Tunnel-UUID>.json`；
- Admin API 和 tutorial 重复 bootstrap 已关闭。

这些配置不会因关闭终端或正常重启电脑而消失。不要删除 `.env`、Cloudflare 凭据文件或 Docker Volume，也不要执行 `docker compose down -v`。

## 每次开始测试

第一个终端启动数据依赖和 Agent：

```bash
cd /home/shiyu/trpc-agent-service
docker compose up -d postgres redis
./start-real.sh
```

`start-real.sh` 会等待当前 Compose 中的 PostgreSQL 和 Redis 进入 healthy，再启动 Agent。电脑重启后 Redis 可能需要先加载 RDB/AOF；如果跳过健康等待，Agent 会因 `LOADING Redis is loading the dataset in memory` 按 fail-fast 退出，随后 Tunnel 日志会出现 `dial tcp 127.0.0.1:8080: connect: connection refused`。

检查本地服务：

```bash
curl -sS http://127.0.0.1:8080/readyz
```

预期：

```json
{"status":"ready"}
```

第二个终端以前台方式启动固定 Tunnel：

```bash
cloudflared tunnel \
  --config /home/shiyu/.cloudflared/config.yml \
  run trpc-agent-telegram
```

该终端必须在测试期间保持运行。检查固定域名：

```bash
curl -sS https://telegram.4392845.xyz/healthz
```

预期：

```json
{"status":"ok"}
```

启动过程不需要执行 `source .env`。`start-real.sh` 会主动读取 `.env`，`cloudflared` 会读取自己的 YAML 和凭据文件。

## 检查 Telegram Webhook

只有手工调用 Telegram API 时才需要把 `.env` 加载到当前终端：

```bash
cd /home/shiyu/trpc-agent-service
set -a
source .env
set +a
```

查询 Webhook：

```bash
curl -sS \
  "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getWebhookInfo"
```

应确认：

```text
url = https://telegram.4392845.xyz/callbacks/telegram/telegram-tutorial-callback
pending_update_count = 0
```

`last_error_message` 可能保留历史错误；判断当前状态时同时观察固定域名、最新消息和 pending 数量。

## 私聊、群聊和 Topic

私聊消息会进入 `chat_type=direct`。群聊和 Topic 会进入 `chat_type=group`，`message_thread_id` 参与 Session ID，因此同一个用户在不同 Topic 中的历史相互隔离。

Privacy Mode 开启时，使用明确命令：

```text
/ask@trpc_agent_test_bot 你的问题
```

普通 `@trpc_agent_test_bot 文本` 不保证被 Telegram 投递。关闭 Privacy Mode 后需要把 Bot 移出群再重新加入；当前平台还没有 `require_mention` 和群白名单，因此关闭后 Bot 会处理 Telegram 推送的所有群文本，只适合测试群。

## 查看完整处理状态

```bash
docker compose exec -T postgres \
  psql -U trpc_agent -d trpc_agent \
  -c "
    SELECT
      i.received_at,
      c.chat_type,
      i.status AS inbound_status,
      r.turn_seq,
      r.status AS run_status,
      o.status AS outbound_status,
      o.attempt_count,
      r.error_type,
      o.last_error_type
    FROM inbound_message i
    JOIN conversation c ON c.conversation_id = i.conversation_id
    JOIN agent_run r ON r.request_id = i.request_id
    LEFT JOIN outbound_message o ON o.request_id = i.request_id
    WHERE i.channel_binding_id = 'telegram-tutorial-binding'
    ORDER BY i.received_at DESC
    LIMIT 20;
  "
```

正常状态为：

```text
inbound_status = processed
run_status = completed
outbound_status = sent
```

## 故障排查顺序

第一步检查本地 Agent：

```bash
curl -i http://127.0.0.1:8080/readyz
tail -n 100 /home/shiyu/trpc-agent-service/data/trpc-service.log
```

第二步检查固定 Tunnel：

```bash
curl -i https://telegram.4392845.xyz/healthz
cloudflared tunnel info trpc-agent-telegram
```

第三步检查 Telegram Webhook：

```bash
curl -sS \
  "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getWebhookInfo"
```

第四步查询 PostgreSQL：

- 没有 Inbox：Telegram 没有投递，或 Webhook/Secret/Tunnel 有问题；
- Tunnel 报 `connect: connection refused`：本地 Agent 没有监听 `8080`，先检查 `./start-real.sh` 和服务日志；
- 服务日志报 Redis `LOADING`：等待 Redis healthy 后重新执行 `./start-real.sh`；
- Inbox 为 `queued`：检查 Redis、Relay 和 Worker；
- Run 为 `failed`：检查模型和 Session 错误；
- Outbound 为 `pending/failed/dead`：检查 Telegram Token、网络、限流和 Sender。

## 每次结束测试

先在运行 `cloudflared tunnel ... run` 的终端按 `Ctrl+C`。

然后停止 Agent：

```bash
cd /home/shiyu/trpc-agent-service
./stop.sh
```

停止数据依赖：

```bash
docker compose stop postgres redis
```

`docker compose stop` 不会删除数据。下次仍然使用同一个固定域名、Webhook、Channel Binding 和历史数据库。

## 电脑重启后的操作

重启后只需要重新执行：

```bash
cd /home/shiyu/trpc-agent-service
docker compose up -d postgres redis
./start-real.sh
```

再在另一个终端执行：

```bash
cloudflared tunnel \
  --config /home/shiyu/.cloudflared/config.yml \
  run trpc-agent-telegram
```

固定域名和 Telegram Webhook 不需要重新创建。

## 当前真实验收结果

截至 2026-09-03：

```text
Telegram inbound: 16
processed: 16
Agent Run completed: 16
Outbound sent: 16
failed/dead: 0
private max turn: 10
group/topic sessions: 3
Webhook pending: 0
```

已覆盖私聊、多轮 Session、进程重启恢复、重复 Update、群聊、Topic、Webhook 暂时不可用后的恢复，以及 Quick Tunnel 到固定 Named Tunnel 的切换。

尚未覆盖 Telegram 真实 429、应用层 `require_mention`/群白名单、媒体发送、消息编辑和长期稳定性压测。
