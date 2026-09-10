# Issue #50: Reliable Reply Delivery

Issue #48 provides the tenant-scoped reply_outbox facts and fenced storage
operations. Issue #50 defines the delivery loop that consumes those facts
without coupling queue logic to a Telegram SDK.

## Contract and Boundaries

The delivery path is:

    Runner event -> materialize reply segments -> claim with lease/fence
      -> Provider.Deliver(stable key) -> sent/provider receipt
      -> all segments sent -> message_event replied

Provider is an injectable protocol-neutral interface. Its delivery identity is
(tenant_id, reply_id, segment_index) and implementations must pass the stable
reply_id plus segment index to provider-level idempotency when the provider
supports it. Database fencing protects the commit race; it does not promise
external exactly-once delivery.

The worker owns no Runner, Telegram SDK, request body, secret, or provider raw
error. It receives the tenant-scoped ReplyStore, MessageStore, and explicit
delivery provider capabilities it needs, plus a context and bounded
retry/shutdown configuration. Providers include Telegram, WeCom, WeCom AI Bot,
and deterministic test fakes through the same delivery contract.

Reply materialization is an atomic batch operation. Every segment is validated
against the same event/reply identity before any new row is committed. A failed
batch leaves no newly deliverable prefix: PostgreSQL commits the batch in one
transaction and the in-memory store validates then commits under one lock.

## Lifecycle

Pending rows are eligible immediately; retryable rows are eligible when the
exponential delay derived from attempts and updated_at is due. A worker
claims one row, increments the attempt and receives a lease/fencing token. Only
that owner and fence can commit sent, retryable, or dead_letter.

An expired sending lease is first reconciled with the provider using the same
stable key. accepted becomes sent, rejected becomes retryable, and unknown or
reconciliation errors remain unresolved and are never automatically
redelivered. This avoids duplicating a provider side effect whose receipt was
lost.

Retryable errors use bounded exponential backoff with jitter and a configurable
maximum attempt count. Permanent errors go directly to dead_letter. Only a
stable error class is stored in last_error_class; raw provider errors never
enter storage, logs, traces, metrics, or client responses.

When a reply contains multiple segments, successful segments remain sent and
only remaining segments are retried. The event advances to replied only after
all segments are sent. Duplicate materialization uses the existing outbox key
and does not create a second row.

## Worker Lifecycle and Telemetry

Run(ctx) stops claiming after cancellation, cancels in-flight provider calls
through the same context, waits for bounded in-flight work, and returns without
leaking goroutines or leases. Close is idempotent. Shutdown is observable
through low-cardinality metrics: claims, send success/failure, retries,
dead-letters, lease recovery, and delivery latency. Traces and logs carry only
request_id, trace_id, component, operation, provider/channel, status, and
stable error class. Tenant/session/user/message bodies, tokens, DSNs, and raw
provider errors are redacted.

Bootstrap does not choose a channel route from an inbound request. A production
caller that has already constructed a trusted, tenant-scoped Provider constructs
an `outbox.Worker`, passes it through `bootstrap.Config.OutboxWorker`, and may
set `OutboxPollInterval`. Bootstrap starts that worker only after the complete
runtime graph exists, and Runtime.Close cancels and joins it before closing the
store or database. Leaving this dependency unset deliberately disables delivery
instead of guessing a recipient.

## Issue Ledger

- [x] Injectable Provider/Delivery contract and tenant-scoped worker lifecycle.
- [x] Runner reply materialization into idempotent outbox segments.
- [x] Fenced concurrent claims with one valid winner.
- [x] Exponential backoff, bounded retries, permanent-error DLQ, and stable
      error classes.
- [x] Expired-lease reconciliation, restart recovery, and stale-fence rejection.
- [x] Multi-segment completion and partial-failure recovery without duplicate rows.
- [x] Cross-tenant claim/read/transition rejection.
- [x] Context cancellation and graceful shutdown leak tests.
- [x] Low-cardinality metrics, trace correlation, and secret/message redaction.
- [x] Telegram provider integration test and opt-in real-provider E2E workflow.
- [x] InMemory tests, live PostgreSQL/restart tests, race tests, and full CI.
- [x] Operational documentation for delivery semantics, retry/DLQ, recovery,
      provider limitations, and capacity estimates.

## Acceptance Evidence

The deterministic tests run in every CI build. The protected Telegram workflow
runs the real outbox delivery test after validating both bot credentials. The
PostgreSQL restart suite runs with an explicitly provisioned DSN, tenant, and
binding, and both suites use the same provider contract as the local tests.
