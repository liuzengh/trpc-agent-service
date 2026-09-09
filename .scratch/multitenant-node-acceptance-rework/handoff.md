# Multi-Tenant And Node Acceptance Rework Handoff

Status: ready-for-human

## Luna Entry Point

Read `spec.md`, then execute `issues/01` through `issues/06` in the Recommended
Execution Order from the spec. Treat the spec as the single source of truth for
interfaces, invariants, migration behavior, acceptance scenarios, and gates.

## Non-Negotiable Contracts

- trusted Tenant identity comes from server context or a verified Execution
  Manifest, never request data;
- public Version IDs and existing HTTP/SSE envelopes remain compatible;
- node placement cannot affect Provider Route visibility, Version selection,
  request idempotency, or cancellation;
- PostgreSQL is the shared correctness adapter; in-memory behavior is a test and
  single-node adapter with the same interface semantics;
- one Session has at most one running owner, multiple requests may queue, and
  different Sessions remain concurrent;
- stale fencing tokens cannot write Session Event, Memory, Artifact, projection,
  or terminal state;
- dangerous Tool owner loss remains `outcome_unknown` and is not replayed;
- secrets and raw credentials do not enter Control Plane snapshots, Run
  Coordinator rows, Audit Events, errors, traces, evidence, or frontend state.

## Final Verification

All required Go, frontend, documentation, and two-Gateway Compose gates pass on
branch `fix/multitenant-node-acceptance-rework`. The only Compose caveat was a
Docker Hub metadata outage; the successful run used the same Dockerfile and
local builder image with BuildKit disabled.

## Completion

For each ticket, add the red regression test first, implement through the module
interface described in the spec, run focused tests and `go test ./...`, then set
the ticket to `ready-for-human` with a concise evidence note. Ticket 06 completes
only after every root gate and the extended two-Gateway Compose assertions pass
from the final code state.
