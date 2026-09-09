# Stage 4 IM Providers

Stage 4 uses two platform-owned Bot accounts:

- Telegram Bot API with long polling.
- WeCom API-mode Smart Bot with a WebSocket long connection.

WeCom self-built applications are not used. Do not configure CorpID, AgentID, an application Secret, an application Access Token, EncodingAESKey, or an application callback URL.

## Local Configuration

Create an ignored `.env.local` file containing the required process environment variables:

```bash
TRPC_TELEGRAM_BOT_USERNAME=...
TRPC_TELEGRAM_BOT_TOKEN=...
TRPC_WECOM_BOT_ID=...
TRPC_WECOM_BOT_SECRET=...
# Optional; defaults to data/bot-routes.json
TRPC_BOT_ROUTES_PATH=...
```

`./start.sh` loads this file before starting the Go service. The values are never returned by the management API. Missing credentials leave the corresponding provider in `unconfigured` state without preventing the HTTP service from starting.

The Management Console exposes Provider Account connection status and Bot Tenant Allowlist routes. Only a platform administrator can create, update, disable, or delete a route from an external conversation subject to a Tenant and Agent App. Routes are persisted atomically in the server-owned route file. Unmapped or disabled messages do not invoke the Agent.

Telegram calls `getUpdates` with long polling and sends Agent output through `sendMessage`. Group conversations route by chat ID; private chats route by chat ID and then use the sender user ID as a documented fallback. Non-text updates are acknowledged by advancing the update offset but do not invoke the Agent.

WeCom connects to `wss://openws.work.weixin.qq.com`, authenticates with an `aibot_subscribe` frame containing BotID and long-connection Secret, receives `aibot_msg_callback`, and sends Markdown through `aibot_respond_msg` using the callback request ID. Authentication, heartbeat, and reply frames require successful correlated acknowledgements. Replies from an expired connection are rejected rather than sent on a replacement connection.

## Acceptance Boundary

CI uses deterministic Telegram HTTP fixtures and a local WeCom WebSocket server without credentials. These fixtures cover callback/update to Runner to provider reply, acknowledgement correlation, routing, unsupported media, and shutdown. Real-provider smoke tests are explicit local operations. The Web UI remains an IM Simulator and does not count as a real provider.

Text is the required end-to-end message format. Complex media download, conversion, OCR, and storage remain outside Stage 4.
