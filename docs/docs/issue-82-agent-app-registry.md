# Issue #82: Agent App Registry, tenant canary, and rollback

> This page records the implemented [Issue #82](https://github.com/XnLemon/trpc-agent-service/issues/82) App Registry: immutable execution plans, reference-counted `RunnerRegistry`, tenant canaries, rollback, and durable audit facts.

## Goal and boundary

Each deployed tRPC instance owns an isolated control-plane namespace and materializes tenant Agent Apps locally. Within an instance, an Agent App selects one stable published revision and can select one candidate published revision for its tenant.

~~~text
tRPC instance
  -> tenant
       -> Agent App
            -> current revision (stable)
            -> canary revision (candidate)
                 -> immutable ExecutionPlan
                      -> reference-counted Runner lease
~~~

The implemented App Registry provides revision selection, exact Runner reuse, lease-safe invalidation, tenant-authorized canary configuration, durable rollback, and auditable decisions. A selected tenant receives the candidate revision for each new execution of that App.

## Implemented baseline

`runtime.ExecutionPlan.CacheKey()` already contains the tenant, App, revision, content digest, and resolved Model/Backend versions. `runtime/runner.RunnerRegistry` owns runners by that complete key, combines concurrent construction, and keeps invalidated runners alive until the last lease releases. `agent.Repository` already persists immutable published revisions and atomically moves `current_revision` for publish and rollback.

The implementation preserves those properties. A canary selects a different immutable plan and never mutates a cached runner or evicts an in-flight lease.

## App selection state

`agent.App` gains an optional `CanaryRevision` field alongside `CurrentRevision`. Both fields refer to a published revision in the same `(tenant_id, app_id)` scope. The following invariants are mandatory:

- `CurrentRevision` remains the stable/default revision and is required for an active App.
- A non-nil `CanaryRevision` is distinct from `CurrentRevision`, positive, and points to a published revision.
- Only an active Tenant and active App can start, change, promote, or clear a canary.
- Tenant scope, App scope, and revision scope are checked by the repository; an Admin request body cannot override its authenticated tenant scope.
- A suspended or disabled App admits no new execution, even if it retains revision pointers for history.
- Existing published revisions are never changed, copied, or renumbered.

The PostgreSQL migration adds nullable `canary_revision` to `agent_app`, a same-App foreign key, and a trigger that rejects references to draft revisions or a candidate equal to `current_revision`. In-memory and PostgreSQL repositories expose the same validation and optimistic-lock behavior. Existing rows receive `NULL`, so they retain the old stable-only behavior.

## Authorized changes and rollback

The repository adds one explicit operation:

~~~go
SetCanary(context.Context, SetCanaryInput) (*App, ChangeEvent, error)
~~~

`SetCanaryInput` contains the explicit tenant/App scope, expected App version, an optional candidate revision, trusted tenant-active gate, and mandatory actor/reason/correlation metadata. A non-nil candidate starts or replaces the canary; `nil` stops it. The Admin endpoint uses the existing administrator authenticator and tenant-scope check:

~~~text
POST /admin/v1/tenants/{tenant_id}/apps/{app_id}/canary
{
  "expected_app_version": 7,
  "candidate_revision": 13,
  "reason": "tenant rollout",
  "correlation_id": "rollout-2026-08-27"
}
~~~

An omitted or JSON `null` `candidate_revision` clears the canary. The route returns the updated App and a control-plane change event. A non-admin caller is rejected before repository access; a scoped administrator cannot configure another tenant.

`Rollback` atomically moves `CurrentRevision` to a historical published revision. It clears `CanaryRevision` in the same update, so rollback restores one selected stable runtime plan for new executions. Rolling back to the candidate revision promotes it; rolling back to the former stable revision terminates the rollout. In either case, post-commit App invalidation removes the affected tenant/App entries before the next resolution.

## Deterministic runtime behavior

`PlanResolver` chooses `CanaryRevision` when it is set; otherwise it chooses `CurrentRevision`. Both choices originate exclusively in the persisted App snapshot, never in an HTTP header, request body, channel payload, user ID, or client-provided revision. Thus selection is deterministic for the tenant/App pair without a percentage hash or an unauditable fallback.

The resolver loads and validates the selected published revision before constructing `ExecutionPlan`. A candidate and stable revision naturally produce different complete cache keys. The existing registry then guarantees:

1. same selected key reuses a local Runner;
2. different tenant, App, revision, or dependency version cannot share a Runner;
3. a canary mutation or rollback invalidates the affected App entries before the next resolution;
4. an already acquired lease keeps its frozen Runner until `Release`; and
5. a restart reloads selection state from PostgreSQL before accepting traffic.

## Audit facts

Every canary configuration mutation produces an `agent.ChangeEvent` with the existing trusted actor, reason, correlation ID, version transition, and selected revision metadata. The durable control-plane outbox adds explicit `canary_started` and `canary_stopped` event types.

For every execution selected through the candidate pointer, Gateway writes an `audit.EventCanarySelected` fact before the normal execution-started fact. It records only tenant, App, chosen revision, request/trace correlation, and the stable audit fields; it never stores request text, identity secrets, provider endpoints, or credentials. The normal execution facts already preserve the actual revision, so a restart or later analysis can join every request to its immutable App definition.

## Compatibility and lifecycle

- Existing App rows, callers, API clients, and stable-only execution retain their behavior because the canary pointer defaults to `nil`.
- Existing `Publish`, status transitions, cache keys, and lease API remain source-compatible.
- New public inputs follow existing `context.Context`-first repository methods and return existing sentinel error classes (`agent.ErrInvalid`, `agent.ErrConflict`, `agent.ErrDisabled`, and `agent.ErrNotFound`).
- No goroutine, channel, or timer is introduced for canary selection. The existing Registry remains the single owner of runner lifecycle and bounded close.

## Issue ledger and verification

- [x] Add a durable App candidate-revision pointer with PostgreSQL migration, repository support, and a tenant-authorized Admin operation.
- [x] Resolve an immutable candidate plan when configured, otherwise preserve stable-plan resolution and exact complete-key Runner reuse.
- [x] Invalidate affected tenant/App entries for canary changes and rollback; in-flight leases complete on their frozen revision.
- [x] Append durable control-plane canary facts and per-execution candidate-selection audit facts without exposing sensitive data.
- [x] Prove tenant isolation, optimistic conflicts, candidate validation, publish/promotion/rollback, in-flight leases, restart recovery, and stable-only compatibility with contract tests.
- [x] Update the README and documentation navigation after the implemented behavior was covered by tests.

Verification covers focused Agent, Gateway, Bootstrap, migration, and audit tests, followed by `go test ./... -count=1`, `go test -race ./... -count=1`, `go vet ./...`, formatting, and strict MkDocs build.
