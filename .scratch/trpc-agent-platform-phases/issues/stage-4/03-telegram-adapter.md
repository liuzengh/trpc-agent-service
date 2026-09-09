# 03: Telegram Long Polling Adapter And Local Closed Loop

**What to build:** The platform Telegram Bot uses `TRPC_TELEGRAM_BOT_USERNAME` and `TRPC_TELEGRAM_BOT_TOKEN` with long polling, maps allowlisted updates to a Tenant and Agent App, routes them through the existing Runner, and sends replies. Deterministic update fixtures provide a complete local loop without live credentials.

**Blocked by:** 01: Shared Channel Configuration And Management

**Status:** resolved

- [x] Long polling starts only when credentials are configured, stops cleanly with service context, and reconnects with bounded backoff.
- [x] Private-chat and group-chat updates route through the Bot Tenant Allowlist by chat ID with the documented user fallback; duplicate update IDs are suppressed.
- [x] Agent events are converted to asynchronous Telegram text replies using the platform Bot token without exposing it.
- [x] Message-length limits and minimal supported media messages are handled explicitly.
- [x] Timeout, retry, rate-limit, cancellation, and terminal-failure paths are covered by deterministic protocol replay tests.
- [x] End-to-end tests prove update to Agent execution to Telegram reply while preserving request and tenant identity.

## Answer

Implemented cancellable long polling, chat/user fallback routing, message/update deduplication and ordering, `sendMessage`, bounded retry with rate-limit handling, unsupported-media advancement, and deterministic callback-to-reply tests.
