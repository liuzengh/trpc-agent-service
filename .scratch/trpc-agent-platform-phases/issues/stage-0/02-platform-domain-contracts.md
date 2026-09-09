# 02: Platform Domain And Port Contracts

**What to build:** A compileable and testable contract surface for the platform's core domain objects and adapters, so later phases can implement tenant, deployment, routing, session, runner, channel, storage, and audit behavior against stable boundaries.

**Blocked by:** None (can start immediately; may run in parallel with 01)

**Status:** resolved

- [x] Contracts cover Tenant, Agent App, Deployment, Deployment Version, Gateway, Worker, Session, and Session Event.
- [x] Ports cover Runner Adapter, Channel Adapter, Storage Adapter, and Audit Event integration points.
- [x] Tenant Context is server-injected and cannot be accepted as an untrusted client value.
- [x] Contract tests are reusable by later concrete implementations.
- [x] Domain glossary and ADR decisions remain consistent with the contracts and their exclusions.

**Verification:** `trpcservice/platform` defines the frozen domain and adapter ports with reusable contract tests. `go test ./...` and `go vet ./...` pass.
