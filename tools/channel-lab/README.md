# Channel Lab (initial implementation)

Local, text-only Telegram protocol laboratory. This is not the official
Telegram Bot API server and does not connect to Telegram. It runs without
product database access. Bots, update queues and messages persist in SQLite.

## Run

From the repository root:

```sh
docker compose -p channel-lab-dev -f deploy/compose/compose.channel-lab.yaml --profile testing up -d --build
```

Open http://127.0.0.1:18090. Create a Bot, select a user, and send messages.
Replies appear only after a Gateway/SDK consumes updates and calls sendMessage.
The GUI does not fabricate Agent replies. Displayed Bot/LLM credentials are
local test credentials; expose the UI only on localhost or a trusted test network.

For integration, add the lab Compose file to the product Compose invocation,
so both services share the same default network. A standalone lab project cannot
resolve a Gateway in a different project's network. Configure
`CHANNEL_LAB_GATEWAY_ORIGIN` to the reachable Gateway origin. Webhook registration
accepts only that origin plus `/v1/telegram/<account_id>`, never arbitrary URLs.

## Implemented

- Persistent Bot creation, token generation, account-config copy, chat UI.
- getMe, getWebhookInfo, setWebhook, deleteWebhook, getUpdates, sendMessage.
- Long polling 0..50s, limit 1..100, nonnegative offset confirmation, one active
  poll per Bot, webhook/polling exclusion, same-ID replay.
- Webhook delivery with secret header and five bounded attempts, no redirects.
- Multiple Bot/user isolation; unknown reply targets fail instead of silently
  succeeding. Text only; no attachments/buttons/draft support yet.
- Optional `/v1/chat/completions`, model `lab-echo`, Bearer key shown in the GUI,
  JSON or SSE responses. Echo is deterministic, not semantic reasoning. Recorded
  model requests are available in `/lab/state` (local test data).
- Real models remain configured directly in the product Runtime Profile.

## Main service integration

This branch implements account `config.endpoint_profile` with `official` (default)
and `test`. The option appears in the existing account creation and connection
configuration screens, including the account flow used when creating a Binding.
Control persists it in existing JSON configuration. Gateway uses it consistently
for preflight, webhook registration, polling and replies. Test API origin is
fixed to `http://channel-lab:8080`; accounts cannot submit arbitrary URLs.

Deploy the matching Web, Control and Gateway versions before using this option.
The previously running main deployment is not changed by this branch.
Recommended first use: create a separate test account with a lab-generated Bot
ID/token and `long_polling`, then bind an existing Deployment using a real model.
Both containers must share a network on which `channel-lab` resolves.

Webhook mode retains the existing configured Gateway public origin; configure
`CHANNEL_LAB_GATEWAY_ORIGIN` to that exact reachable origin. This change does
not add an inbound TLS proxy or a second Gateway HTTP listener.

Changing the endpoint requires the account to be disabled and matching Bot
credentials to be configured. Existing Bot ID remains immutable. On a server
change Gateway clears the old server's receiver offset/registration metadata;
it does not delete external webhook registrations or rewrite historical messages.
Use separate accounts for official and simulated Bots to keep their histories
and provider message identifiers separate. Worker/Session code is unchanged.

## Tests

```sh
python3 -m unittest discover -s tools/channel-lab/tests -v
```

Tests use real local HTTP for webhook retry, chunked SDK-style requests and
model SSE; SQLite restart, polling ACK/replay and Bot isolation are also tested.
These tests do not validate real Telegram availability or a real model.

## Stop

```sh
docker compose -p channel-lab-dev -f deploy/compose/compose.channel-lab.yaml --profile testing down
```

The SQLite volume remains. No product database migration is performed.


## Real LLM proxy mode

The lab UI now has **LLM 工作模式**. Choose **代理真实 LLM**, enter an
OpenAI-compatible API base URL (usually ending in `/v1`, not `/chat/completions`),
the real upstream model ID and upstream API key, then save. This sends future
model requests, including conversation history, to that selected provider and
may incur provider charges. No upstream is enabled by default.

Keep the product Runtime Profile unchanged:

```text
base_url: http://channel-lab:8080/v1
model: lab-echo
api_key: the existing laboratory model key
```

`lab-echo` is the stable downstream alias. Echo mode produces the previous local
reply; proxy mode replaces only `model` with the configured upstream model and
forwards the request body (messages/history, tools, generation parameters and
stream options). The upstream Authorization header is generated from the stored
upstream key, never the downstream laboratory key. JSON responses are passed
through after completion-shape validation. SSE chunks are flushed incrementally,
not collected into a synthetic complete response; tool deltas and `[DONE]` are
preserved. Failure never silently falls back to echo.

Settings are stored in the existing local SQLite data volume and survive container
recreation. **The upstream key is stored locally in plaintext**, protected by the
lab's private data-file permissions; it is not encrypted by a vault. Admin state
and configuration responses return only `key_configured`, never the upstream key.
An empty key preserves the existing value only for the same Base URL. Changing the
Base URL requires entering a key again. `POST /lab/model` with `mode=echo` and
`clear_key=true` removes the saved key. Admin writes retain same-origin JSON checks.
The lab is a localhost development service, not a multi-user production proxy.

Existing in-flight requests keep their configuration snapshot. Saving affects
subsequent requests globally for all lab bots using this model endpoint. To switch
providers without mixing histories, start a separate test user/session as needed.
Redirects are rejected. Requests are limited to 1 MiB; responses to 8 MiB. Upstream
socket timeout is 30 seconds; streams stop after 120 seconds checked between reads
(one blocked read can add up to the socket timeout). Upstream HTTP failures become
sanitized HTTP 502 errors; connection-open timeout becomes 504. After SSE headers
are sent, a broken/oversize/timed-out upstream stream closes without inventing a
successful terminal event. Model request bodies are retained in local lab history;
upstream authentication headers are not recorded.

Proxy tests use an independent local HTTP upstream to verify model/header mapping,
JSON, incremental SSE, redirected/error response handling, key redaction,
configuration persistence and echo switching. They do not claim any real provider
is connected until its credentials are configured and a real chat succeeds.

### Actual document uploads

The simulated Telegram API accepts multipart `sendDocument` with real file
bytes. String file IDs/URLs/ref text are rejected in this mode. Received bytes
are stored in SQLite `documents.body` (BLOB), alongside filename, MIME and
SHA-256; the outgoing message contains a Telegram-style `document` receipt.
Original reply source and thread are recorded. `/lab/state` exposes metadata,
not inline file bytes. `GET /lab/documents/{file_id}` returns the stored bytes
as a download with length and `X-Content-SHA256`; chat bubbles provide a download
link and file size/hash. This remains a local protocol laboratory, not evidence
of external Telegram delivery. The multipart MIME type is recorded as sent by
the client, which can be application/octet-stream even for text files.
