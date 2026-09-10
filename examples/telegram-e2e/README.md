# Telegram live E2E example

This example exercises the real Telegram Bot API boundary around the
tenant-scoped long-polling adapter and durable reply delivery:

```text
getMe -> getUpdates -> trusted Telegram Adapter -> DispatchService -> sendMessage
```

The protected CI job also runs the durable reply path from a deterministic
Runner through reply materialization, the Telegram Provider, and the fenced
Outbox Worker:

```text
Runner reply -> reply_outbox -> Telegram sendMessage -> provider receipt
  -> reply_outbox.sent -> message_event.replied
```

The interactive transport command deliberately uses a deterministic
`DispatchService`, so it tests Telegram transport, trusted target construction,
update normalization, reply delivery, cancellation, and secret handling without
requiring a production LLM provider. The protected outbox test uses a
deterministic Runner behind the real Gateway path.

## Local run

Create a dedicated test Bot with `@BotFather`, revoke any token that has been
shared outside a secret store, and set the replacement token in the process
environment. Do not place it in this repository or print it.

PowerShell:

```powershell
$env:TELEGRAM_BOT_TOKEN = '<receiver bot token>'
go run ./examples/telegram-e2e
```

The command prints a unique ordinary-text marker. Open the receiver Bot in
Telegram, send that marker, and confirm the `telegram-e2e-ok:<run-correlation>` reply. Commands,
media, and rich updates are intentionally outside this live E2E; media behavior is covered by deterministic fake tests. Press
`Ctrl+C` to stop the local polling process cleanly.

If PowerShell can reach `api.telegram.org` but this command reports
`telegram E2E getMe network failure`, Go is using a different HTTPS path. Go
uses the standard `HTTPS_PROXY`/`HTTP_PROXY` environment variables; it does not
automatically import every Windows system-proxy setting. Configure the proxy
for the same PowerShell process, without printing credentials, and rerun the
command.

If the Bot has a webhook, either remove it before starting long polling or set
`TELEGRAM_DELETE_WEBHOOK=true`. Pending updates are preserved by default; set
`TELEGRAM_DROP_PENDING_UPDATES=true` only when discarding them is intentional.

Optional local settings:

| Variable | Meaning | Default |
| --- | --- | --- |
| `TELEGRAM_TEST_MESSAGE` | Exact marker to wait for | generated per run |
| `TELEGRAM_TIMEOUT` | Maximum run duration | `2m` |
| `TELEGRAM_POLL_TIMEOUT` | Telegram long-poll timeout | `5s` |
| `TELEGRAM_DELETE_WEBHOOK` | Delete an existing webhook | `false` |
| `TELEGRAM_DROP_PENDING_UPDATES` | Drop queued updates when deleting webhook | `false` |

## CI run

The live workflow runs automatically for pull requests from this repository
and pushes to `main`, and remains manually dispatchable. It references a
protected GitHub Environment named `telegram-e2e`:

- `TELEGRAM_BOT_TOKEN`: required secret for the receiving test Bot.
- `TELEGRAM_SENDER_BOT_TOKEN`: required secret for a second controlled test Bot
  in CI; it sends the unique marker and receives the expected reply. The same
  controlled Bot is the durable outbox delivery target for the provider E2E. This
  sender secret is optional only for local human-driven runs.

For a fully automatic message round trip, enable Telegram Bot-to-Bot
Communication Mode for both dedicated test Bots. A single Bot API token cannot
act as a normal user sending an inbound message to itself. Without the sender
secret, the example remains suitable for a local human-driven run but the CI
job will eventually time out waiting for the marker.

The workflow uses one concurrency group so two runs cannot poll the same test
Bot at the same time. Pull requests from forks are skipped because GitHub does
not provide these Environment secrets to them; use the manual workflow from a
trusted branch when an external contributor's change needs a live check.
