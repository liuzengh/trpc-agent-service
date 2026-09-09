# 04: Deployment Drafts And Immutable Versions

**What to build:** An authorized tenant user can create a draft Deployment for an Agent App, capture its configuration as monotonically numbered Deployment Versions, and inspect both resources in the Management Console. The backend validates every ownership relationship and preserves each created version as an immutable publishable record.

**Blocked by:** None (can start immediately; 03: Tenant-Isolated Agent App Management is resolved)

**Status:** resolved

- [x] An authorized user can create a draft Deployment for an Agent App belonging to the active Tenant.
- [x] A draft can produce Deployment Versions with unique, monotonically increasing version numbers and validated configuration.
- [x] A created Deployment Version cannot be modified in place; a configuration change creates a new version.
- [x] Tenant, Agent App, Deployment, and Deployment Version ownership is enforced by the backend for every read and mutation.
- [x] Version creation requires a valid `Idempotency-Key` header of 1–128 printable ASCII characters; missing or malformed keys return stable structured errors.
- [x] Idempotency is scoped to `(Tenant ID, Deployment ID, Idempotency-Key)`: the same key and normalized configuration returns the original Version and `201`, while different configuration returns `409 idempotency_key_reused`.
- [x] Sequential and concurrent replays atomically create exactly one immutable Version and do not consume extra version numbers; different keys may create distinct Versions with identical configuration.
- [x] Invalid ownership, missing parents, duplicate resource IDs, malformed configuration, and unauthorized operations return stable structured errors.
- [x] The Management Console provides Deployment list, draft creation, detail, version creation, version history, loading, empty, validation, forbidden, and error states.
- [x] Repository and API tests prove version ordering, immutability, ownership isolation, idempotent replay, key-reuse conflict, and deterministic concurrent behavior under in-memory storage.
- [x] The Management Console generates one UUID key per explicit Version creation, keeps that key while a result is ambiguous, rotates it after success or configuration changes, and surfaces validation or key-reuse conflicts without losing the draft configuration.
- [x] Component and Playwright tests verify the complete idempotent Version workflow on desktop and mobile layouts.

## Answer

Implemented Tenant-owned draft Deployments and immutable Deployment Versions with deep-copied JSON configuration, deterministic monotonically increasing version numbers, ownership validation, structured errors, and non-mutating version endpoints. Repository and HTTP tests verify ordering, immutability, missing-parent behavior, and Tenant scoping.

## Comments

- Reopened after Stage 1 code review. Version creation did not distinguish a retried request from an intentional new version. The confirmed contract uses required `Idempotency-Key`, preserves the original successful response for matching replay, and rejects payload mismatch without consuming a version number.
- This rework ticket can run in parallel with Ticket 07. Ticket 05 consumes the completed Version creation contract.
- Resolved with atomic process-local replay records, normalized JSON comparison, stable validation/conflict errors, concurrent exactly-once coverage, and Management Console UUID reuse/rotation tests.
