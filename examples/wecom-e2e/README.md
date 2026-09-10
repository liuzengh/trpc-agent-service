# WeCom deterministic E2E

This example exercises the WeCom text callback boundary without external
credentials or network calls to WeCom/LLM providers:

```text
signed + AES callback
    -> WeCom Handler
    -> Gateway Dispatch
    -> PostgreSQL Event + Reply Outbox
    -> httptest WeCom Provider
```

The test creates an isolated in-memory control-plane Tenant/App/Binding and
uses the PostgreSQL runtime store for sessions, message events, and replies.
The provider's token and send endpoints are local `httptest` endpoints. A
deterministic Runner supplies the reply, so no model API key is required.

## Local run

Set `POSTGRES_MIGRATION_TEST_DSN` to an empty PostgreSQL database, then run:

```powershell
go test ./examples/wecom-e2e -run TestWeComCallbackOutboxE2E -count=1 -v
```

The test applies repository migrations, sends a signed encrypted text callback,
checks the durable reply and provider delivery, then sends the same callback
again to prove the Runner is not executed twice.

Native media ingress and image/file egress are covered by deterministic channel
unit tests; this E2E keeps using text so it does not need live WeCom media
credentials or public provider endpoints.

CI runs this test in a dedicated PostgreSQL service through the **WeCom
deterministic E2E** job. It intentionally does not require `WECOM_*`, Telegram,
model, or other production credentials.
