# Admission: immutable acceptance and durable execution outbox

Admission owns the permanent first `Receipt` for `(provider, stable account_id,
external_event_id)`. `SourceDigest` conflicts return an error rather than opening
a second run. Replay does not consult a changed/disabled route, stopping state,
or fresh-work budgets after the stored receipt is found.

The PostgreSQL adapter obtains an event-key transaction lock and rechecks replay
before charging its global budget. A fresh `admit-run` then verifies routing
under the same transaction's shared generation guard. Inbox, Admission, fixed
RouteSnapshot, Run ID, normalized input, validated RunRequested outbox event and
budget charge all commit together. A statement failure or COMMIT failure leaves
none of these new facts. `ignore` and `interaction` create only an Inbox receipt
and quota charge: no RouteSnapshot, Admission, Run or execution Outbox.

## Initial operating limits

These are configurable starting values, not measured throughput commitments:

- At most 10,000 unpublished execution outbox rows before new run admissions stop.
- At most 10 minutes oldest pending outbox age before new run admissions stop.
- At most 10,000 fresh Inbox receipts per durable fixed one-minute window across
  all Gateway replicas, including ignore and interaction decisions.
- At most 128 concurrent receipt lookups per process, and separately at most 128
  concurrent new acceptance operations. Lookup capacity remains available while
  fresh-work slots are occupied; individual lookup saturation still returns a
  retryable error instead of adding unbounded database waiters.
- A default 10-second total acceptance deadline covers lookup, resolution and
  commit. The workload still owns HTTP/socket deadlines and shutdown drain.

A singleton PostgreSQL row serializes fresh budget decisions, so readiness is
not the enforcement boundary. `Store.Health` is only an observational snapshot.
The Outbox threshold applies to run-producing decisions; other fresh receipts
still consume the shared Inbox quota. Duplicate checks precede quota checks.
A fixed-window reset may allow bursts around the window boundary; it is not a
rolling-minute rate promise. Inbox retention/GC remains separate work.

`Service.Stop()` immediately closes new-work entry and is checked again after
route resolution, just before `Commit`. Operations already inside `Commit` are
in flight and are drained by the workload shutdown. No process mutex spans a
network/database call, and committed receipts remain replayable after Stop.

## Outbox reliability

Claim is an atomic `FOR UPDATE SKIP LOCKED` selection with a random claim token,
30-second lease and incremented attempt count. Both successful publication and
retry bookkeeping require the same token and an unexpired lease. A failed
publisher request schedules retry after one second; failed retry bookkeeping
leaves the lease to expire. A crash after broker acceptance but before local
publication marking replays the same immutable event ID and payload after lease
expiry. This is at-least-once delivery, not external exactly-once.

`Relay.PublishNext` has a 10-second operation deadline and a five-second publisher
deadline. The versioned execution wire codec validates transport DTOs before an
Outbox write. Domain packages do not import provider or wire DTOs.

## Evidence

Unit tests exercise stopping, replay priority, independently bounded lookup and
new-work concurrency, bounded route-race retry, deadlines and decision ownership.
PostgreSQL tests use the actual workload migrations and actual routing guard in
isolated disposable schemas. They require `GATEWAY_TEST_DATABASE_URL`; an unset
variable skips those tests and is not PostgreSQL verification. Real tests cover
concurrent single-winner acceptance, digest conflict, disabled-route replay,
statement/COMMIT rollback, generation races, globally atomic budgets, window
reset, corrupt receipts, claim CAS, retry delay, expiration and crash recovery.
