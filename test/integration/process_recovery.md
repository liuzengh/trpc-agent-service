# Real process recovery acceptance

Run from the repository root with Go, a C compiler for the race detector,
`TEST_DATABASE_URL`, and `TEST_REDIS_URL`:

```sh
go test -race -tags=integration -run '^TestProcessRecovery$' -count=1 -v -timeout=25m ./test/integration
```

The database URL must use the `postgres://` or `postgresql://` form and a test
role with `CREATEDB`. Each scenario creates a random `process_recovery_*`
database, applies the real migrations, and drops only that database after
stopping its own processes and closing connections. Redis cleanup is limited
to the generated tenant's keys and exact service-auth nonces observed by the
test proxy. Other databases, processes, and Redis tenants remain independent.

The suite compiles the current `cmd/worker` and `cmd/consumer` with `-race` and
`-buildvcs=false`, then starts the executables. This includes the production
HTTP execution contract, HMAC and Redis nonce verification, pinned published
deployment, execution admission, PostgreSQL fencing, Runner, shared PostgreSQL
Session and Memory, durable result cache, and atomic Inbox/Outbox completion.
A Go build overlay injects the fixture through `ModelFactory`'s private HTTP
client seam, restricted to its loopback origin and `/v1/chat/completions`.
Tenant endpoint validation remains enforced. The fixture requires a real
`memory_add` result before returning, and rejects any third model call.

| Fault boundary | Injected failure | Required recovery |
| --- | --- | --- |
| Inbox committed and claimed; Worker request held before forwarding | Kill the Consumer | Natural 45-second lease expiry, stale completion rejected, restarted Consumer claims fence 2, one execution and one Outbox |
| Worker execution and result receipt committed; response held before Consumer can complete Inbox | Kill the Consumer and Worker | Restarted Worker returns the committed receipt, model calls remain exactly 2, one persisted memory and tool event, one execution and one Outbox |

Both scenarios verify Inbox deduplication, two queue claims, fences `1 -> 2`,
rejection of the old lease before and after recovery, one successful execution
attempt, one result receipt, and exactly one pending Outbox. They inspect
persisted transcript and memory after processes exit. Child logs are checked
for race detector reports, including logs from intentionally killed children.
Premature child exit and failed owned-resource cleanup fail the test.

These scenarios start at the durable Inbox boundary. They do not test Gateway
HTTP acknowledgement, real provider quality, or IM delivery. The result is a
pending Outbox; no Delivery executable or real IM account is used. They prove
safe reuse of a **committed successful receipt**, not automatic recovery from
an unknown mid-tool outcome, which still requires the platform's reconciliation
policy. The separate encrypted callback E2E covers Gateway protocol behavior.
