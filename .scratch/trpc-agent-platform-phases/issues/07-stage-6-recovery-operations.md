# Stage 6: Failure Recovery And Operations Console

Type: task
Status: resolved
Blocked by: 06

## Goal

Prove and safely operate recovery, cancellation, rollout, rollback, and capacity behavior in a reproducible multi-process environment.

## In Scope

Gateway/Worker failure; Redis/SQL outage; model timeout; Tool failure; IM retry; context cancellation; event-channel draining; gray release; tenant rollback; capacity estimation; Docker Compose with Gateway, Worker, Redis, SQL, and Mock IM; fault-injection scripts; Kubernetes production guidance. Extend the frontend with component health, rollout state, rollback preview/confirmation, development/Compose-only fault injection, capacity and recovery results, and drain/shutdown status. High-risk operations require backend authorization, confirmation, and Audit Events.

## Out Of Scope

Requiring a Kubernetes cluster or Chaos Mesh for local/CI acceptance, production-enabled arbitrary fault injection, and unaudited high-risk operations.

## Acceptance

Component stop/restart recovery, dependency outage classification, timeout cancellation, tool degradation, duplicate callback idempotency, clean shutdown, rollout/rollback, capacity smoke, Compose-from-zero smoke, frontend/API contracts, role/confirmation/audit checks, and operations Playwright flows pass.

## Handoff

Publish the final architecture, diagrams, data model, synchronization strategy, multi-backend/provider matrix, frontend delivery model, README traceability matrix, and risk register with at least eight risks and mitigations.
