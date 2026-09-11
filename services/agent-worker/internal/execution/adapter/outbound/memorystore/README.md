# Accepted Memory storage

The fixed PostgreSQL adapter uses `runtime_memory`, `memory_migrator` for explicit
schema migration, and `memory_runtime` for runtime access. `Open` verifies the
fixed destination/role/privileges; it never performs DDL. Schema is an adapter
constant, not an additional Manifest field. Credentials are password-only when
using `CredentialDSN`.

This package does not implement memory tools or search. The existing SDK-backed
`MemoryAttempt` owns those operations. Load its detached initial entries, execute
against the private view, seal the candidate, and call `ApplyAccepted` only from
the authoritative completion owner after durable acceptance. Scope uses the
existing trusted `MemoryScopeID`: SDK AppName is TenantID, UserID is Scope.ID.

`ApplyAccepted` validates candidate digest and scope, locks the local scope head,
compares BaseRevision, and updates the full snapshot and immutable completion
receipt in one transaction. Empty entries clear the scope. Replaying the exact
same completion returns its original revision and entries without changing a
newer head. A different candidate/identity for the same completion or a repeated
Run/Attempt under a different completion is a conflict. Distinct accepted
candidates using a stale base revision conflict rather than merge silently.

This local transaction does not prove remote Execution acceptance and is not a
distributed transaction with Execution or Session. The caller must implement the
authoritative accepted-only invocation and deal with cross-database recovery;
this package adds no queue or background recovery platform. Snapshot input must
not be concurrently mutated by the caller during a method call.

Run isolated PostgreSQL acceptance (Docker required):

```sh
GOCACHE=/private/tmp/go-worker-capabilities-cache \
  services/agent-worker/memorymigrations/test-postgres.sh
```

The script creates its own PostgreSQL container and removes it on exit. Direct
Go integration tests require all `WORKER_MEMORY_TEST_{ADMIN,MIGRATION,RUNTIME}_URL`
variables and `WORKER_MEMORY_TEST_ALLOW_RESET=1`; without them they explicitly skip.
