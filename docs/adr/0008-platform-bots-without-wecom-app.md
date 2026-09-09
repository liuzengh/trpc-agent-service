# Platform Bots Without A WeCom Self-Built App

Status: accepted

## Context

Stage 4 needs two real IM integrations. The available WeCom credentials are `TRPC_WECOM_BOT_ID` and `TRPC_WECOM_BOT_SECRET`, which identify a WeCom API-mode Smart Bot. They do not form the credential set for a traditional WeCom self-built application. Telegram similarly provides one platform Bot account.

## Decision

Use platform-owned Provider Accounts:

- WeCom uses the Smart Bot WebSocket long connection with BotID and long-connection Secret.
- Telegram uses one Bot username/token with long polling.
- Neither integration uses a WeCom self-built application, CorpID, AgentID, application Secret, application Access Token, or traditional application webhook.
- Bot credentials are process-level environment configuration loaded from `.env.local` for local development only. They are never stored in Channel Bindings, browser storage, API responses, logs, traces, or Session Events.
- A server-owned Bot Tenant Allowlist maps external conversation subjects to one Tenant and Agent App. The allowlist, not an untrusted request field, establishes routing.

## Consequences

Provider Account lifecycle, connection health, reconnect, and shutdown are separate from tenant Channel Binding CRUD. CI uses deterministic protocol fixtures and the IM Simulator; real credential smoke tests are explicit and reported separately. Existing self-built-app credential names and HTTP callback assumptions must not be added as compatibility configuration.
