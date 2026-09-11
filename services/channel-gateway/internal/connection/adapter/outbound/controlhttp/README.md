# Control account HTTP adapter

This adapter consumes the shared `api/schemas/channel/v1` contracts. Its only
network entry points in this slice are complete account snapshots and exact
credential resolution. It validates real TLS server identity and presents a
client certificate; Control owns URI SAN to scope/instance/consumer authorization.
No user-session cookie, forwarded identity header or untrusted reference grants
access. Redirects, compressed responses and credential-bearing error text are not
forwarded. Snapshot and resolve budgets cover the complete operation, including
validation. Responses and credentials are bounded; no credential cache is stored.

The returned accountcatalog types are not SDK clients or sending authority.
Connection's `catalogrefresh` lifecycle consumes Source and Replica ports; the
PostgreSQL replica persists catalog lineage, digest receipts, account tombstones
and per-incarnation qualification. `catalogpostgres.Guard` holds an account then
instance SHARE lock inside the consumer transaction; callers must retain their
own original locks, rerun expiry checks at the state write, check local Permit
before commit/call, and never hold a transaction across a provider request.

Current slice implements/test-drives these seams, not their default App wiring:
- snapshot sorting, closed shape, exact digest, identity and version checks;
- SAME / ADVANCE / SUPERSEDED, rollback/conflict quarantine and persistent floors;
- instance-local failure, 30-second request-start freshness, full-snapshot atomicity;
- immutable per-account lifecycle contexts and WeCom-only AccountSource projection;
- separately budgeted Supervisor Resolve, owner pre/post checks and cancellation.

Outstanding next slice: production bootstrap configuration and credentials
bridge, Admission/Delivery transaction guard injection, registration operation
ledger/reconciler, Telegram dynamic handlers/client rotation, observations, and
Control-to-Gateway/real Telegram acceptance. No default allow-all verifier or
production credentials are introduced. A working Schema or local TLS server is
not evidence that the actual Control process or external Telegram bot was used.


GCI2 update: the production App now composes these seams with Control-backed
credential resolution, Admission/A1/A2 transaction guards, dynamic Telegram
handlers, fenced registration, Delivery Runner and the observations use case.
The earlier outstanding list above describes GCI1 only. See CONTROL_RUNTIME.md
and implementation-status section 13 for current commands and verification.
Real local mTLS/PG/NATS with a synthetic remote is distinct from real Control
process plus Telegram acceptance; the latter remains a joint runtime check.
