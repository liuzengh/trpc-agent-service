# Stage 4 Handoff

Status: complete

Stage 4 freezes the following contracts for Stage 5:

- Provider names are `enterprise_wechat` and `telegram`; `mock` remains a local validation provider.
- WeCom uses API-mode Smart Bot BotID/long-connection Secret over WebSocket. Traditional WeCom self-built applications are not used.
- Telegram uses one platform Bot username/token with long polling; it does not use a webhook in this phase.
- Platform-owned Provider Accounts are routed through a server-owned Bot Tenant Allowlist; they are not tenant-owned Channel Bindings.
- `ChannelCoordinator` dispatches providers through the existing `ChannelAdapter` interface and keeps binding selection tenant-scoped.
- Platform Bot credentials are process environment only and never appear in allowlist routes, API responses, Session Events, logs, or browser state.
- Bot credentials are `TRPC_TELEGRAM_BOT_USERNAME`, `TRPC_TELEGRAM_BOT_TOKEN`, `TRPC_WECOM_BOT_ID`, and `TRPC_WECOM_BOT_SECRET`; they are process-only and never exposed.
- Provider message IDs drive duplicate suppression and provider sequence values drive out-of-order rejection.
- Stable public outcomes include signature invalid, callback invalid, attachment rejected, message too long, timeout, rate limited, disabled, unavailable, and retry exhaustion.
- Web UI and deterministic replay validate the flow but do not count toward the two real providers required by the README.
- `POST /api/v1/admin/providers/replay` is platform-admin-only and uses the local deterministic reply adapter; it never sends to a real provider.
- `GET /api/v1/admin/providers/deliveries` exposes bounded, tenant-filtered delivery status without payloads or credentials.
- `TRPC_BOT_ROUTES_PATH` selects the atomic server-owned allowlist file and defaults to `data/bot-routes.json`.
