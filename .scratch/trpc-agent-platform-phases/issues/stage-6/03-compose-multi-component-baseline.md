# 03: Compose Multi-Component Baseline

**What to build:** A clean Compose environment runs Gateway, Worker, Redis, SQL, and Mock IM from zero, and the Management Console completes an end-to-end chat run with component health.

**Blocked by:** 02: Gateway/Worker Dispatch Boundary.

**Status:** resolved

- [x] A documented one-command Compose startup builds or starts the required components with health checks and deterministic local configuration.
- [x] An operator can seed or create a Tenant, Agent App, Deployment, immutable Version, Session, and Mock Channel through the API or console in the Compose environment.
- [x] A Mock IM callback produces one governed Agent execution and one provider reply while preserving request and trace identity.
- [x] The console shows Gateway, Worker, Redis, SQL, and Mock IM component health and delivery/recovery evidence without exposing secrets or addresses.
- [x] Repeating the Compose startup is reproducible after teardown, and stopping only the frontend does not affect already-running backend work.
- [x] Automated Compose smoke assertions prove zero-to-working service, health readiness, end-to-end execution, and clean teardown.
