# 10: Kubernetes Guidance And Stage 6 Handoff

**What to build:** The final Stage 6 package provides production Kubernetes guidance, architecture and recovery evidence, README traceability, risk register, and an acceptance/handoff suitable for review.

**Blocked by:** 09: Compose Fault Injection And Recovery Evidence.

**Status:** resolved

- [x] Kubernetes guidance covers Deployment topology, replicas, readiness/liveness/startup probes, resource bounds, graceful shutdown, rolling updates, rollback, secrets, storage, and configuration separation.
- [x] Production guidance explains distributed health and draining, shared-state assumptions, identity requirements, observability collection, and dependency recovery without requiring a local Kubernetes cluster for acceptance.
- [x] Architecture documentation includes Gateway, Worker, Channel Adapter, Storage Adapter, governance, telemetry, Compose, and Kubernetes responsibilities and boundaries.
- [x] The risk register identifies at least eight concrete failure, security, data-integrity, lifecycle, or capacity risks with mitigation and acceptance classification.
- [x] README and stage documentation provide a reproducible validation matrix and map each delivered Stage 6 capability to evidence.
- [x] All backend, race, frontend, build, Playwright, and Compose recovery commands pass, and the handoff freezes Stage 6 contracts and known limitations without changing the parent spec.
