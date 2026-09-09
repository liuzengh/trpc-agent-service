# tRPC Agent Platform Phased Delivery

Status: confirmed

This spec turns the README into independently testable development phases. Stage 0 is complete and remains unchanged. Starting in Stage 1, every phase delivers its backend capability and the corresponding increment in one progressive management frontend.

## Delivery Rules

- Work in phase order; a phase may consume only contracts frozen by earlier phases.
- Stage 0 code, issues, acceptance, and handoff are frozen and are not changed by this revision.
- A phase is complete only when its backend and frontend automated tests, applicable formatting/static checks, documentation, exclusions, and known limitations are complete.
- Each phase gets one parent issue under `issues/`, plus `acceptance.md` and `handoff.md` when implementation begins.
- The default integration environment is local Go/frontend tests and Docker Compose. Kubernetes is a production profile, not a development prerequisite.
- `trpc-agent-go` is consumed through explicit adapters; platform concerns remain in this repository.
- The runtime dependency is the published Go module `trpc.group/trpc-go/trpc-agent-go v1.11.2`. A local checkout may be used for API inspection or temporary debugging only; delivery and acceptance must not depend on a local `replace` directive.
- Web UI is a local management and IM-validation surface. It does not count toward the README requirement to implement two real IM providers.
- The two real IM providers are Enterprise WeChat and Telegram.

## Acceptance Priority And Non-Blocking Quality

- `README.md` is the source of truth for final competition acceptance. A requirement is blocking when it changes an observable README capability, API/data contract, required isolation guarantee, required provider/backend workflow, build/test reproducibility, or listed delivery artifact.
- Findings that do not change an accepted README outcome are non-blocking by default. Examples include defense-in-depth hardening, internal error-message sanitization when no credential or tenant data is exposed, refactoring/code-smell cleanup, and production-scale performance or observability improvements outside the stated acceptance scenario.
- A concern becomes blocking when it causes a required workflow to fail or hang, permits cross-tenant data or credential exposure, breaks data integrity, prevents cancellation/shutdown required by the workflow, or makes the documented build/test path non-reproducible.
- Reviews and handoffs must label findings as `blocking-acceptance` or `non-blocking-quality`. Non-blocking findings may remain in known limitations and must not trigger mandatory rework or prevent phase handoff.
- Phase tests should spend effort on README-traceable behavior first. Do not add production-only gates for capabilities that are explicitly assigned to a later phase or excluded from the current phase.

## Progressive Frontend

- The single frontend source tree is `frontend/`, using React, TypeScript, Vite, and npm.
- Development uses the Vite server with `/api` proxied to Go. Production frontend assets are built and served by the Go service through embedding or an equivalent packaged mechanism.
- `build.sh` builds both frontend and Go artifacts and fails clearly when its declared Node.js/npm prerequisites are absent.
- The backend API is the source of truth for platform resources and conversations. Browser storage may contain non-sensitive UI preferences, filters, and the most recently opened Session ID only.
- The UI must never persist secrets, trusted tenant identity, or the sole copy of business data.
- Stage 1 establishes the management shell. Stages 2–6 extend the same application rather than creating separate sites.

## Identity And API Boundaries

- Tenant Context is established only by trusted server middleware. A request body, arbitrary header, or browser storage value cannot grant tenant access.
- Stage 1 provides a server-validated development identity and tenant-switching mechanism behind a stable identity-provider boundary. Production authentication and complete authorization are delivered in Stage 5.
- The minimum role vocabulary is `platform_admin`, `tenant_admin`, `operator`, and `viewer`. Authorization is enforced by the backend; hiding a UI control is not sufficient.
- `/v1/run` remains the Stage 0 compatibility and diagnostic endpoint.
- `/api/v1/admin/...` contains management resources and operations.
- `/api/v1/chat/sessions/...` contains Session creation, history, message execution, streaming, retry, and cancellation.
- `/api/v1/auth/...` contains current identity, development tenant switching, and later production authentication.
- `/healthz` and `/version` remain unchanged.

## Phase Map

| Phase | README mapping | Runnable outcome | Frontend increment | Primary test boundary |
| --- | --- | --- | --- | --- |
| 0 | Baseline implied by code directory and quick start | Completed health/version service and stable ports | None; unchanged | Build, start/stop, cancellation, contract tests |
| 1 | Multi-tenant and node deployment | Tenant/App/Deployment to Gateway/Worker/fake Runner loop | Frontend foundation and basic management console | Isolation, routing, deployment state, authorization, Session serialization, management E2E |
| 2 | Data synchronization and multi-backend support | Shared Session/Memory adapters and repeatable migration command | Data backend, Session/Event, Memory/Summary, migration, and health views | Ordering, idempotency, cross-node visibility, migration validation, data-management E2E |
| 3 | IM integration contract and local validation | Standard Channel Adapter, Mock IM, chat APIs, and SSE stream | Chat workspace in the same management frontend | Mock faults, mapping, streaming, refresh recovery, cancellation, chat E2E |
| 3.5 | Framework runtime integration | Published `trpc-agent-go` runtime, AgentFactory, streaming RunnerAdapter, and runtime-backed chat execution | Runtime status and deployment validation evidence in the existing console | Version pinning, input/event mapping, streaming, cancellation, close/drain, tenant isolation, framework integration E2E |
| 4 | Two real IM implementations | Enterprise WeChat and Telegram Channel Adapters | Channel Binding, provider configuration, replay, and smoke-status views | Protocol replay, signature, deduplication, retry, provider limits, optional credential smoke |
| 5 | Governance, monitoring, and security | Tenant policy and production identity around the Stage 3.5 runtime, audit, metrics, and trace propagation | Policy, authorization, audit, metric, cost, and trace views | Policy denial, redaction, authorization, budget, audit, end-to-end trace, DOM secret checks |
| 6 | Failure recovery and operations | Compose deployment with failure injection, gray release, and rollback | Operations console for health, rollout, rollback, faults, and recovery evidence | Dependency failure, cancellation, draining, recovery, capacity, operations E2E |

## Stage Details

### Stage 1: Multi-Tenant Routing And Basic Management

Deliver in-memory Tenant, Agent App, Deployment, and Deployment Version management; trusted Tenant Context; Gateway/Worker routing; same-Session serialization; cross-Session concurrency; and a replaceable fake Runner. Establish `frontend/`, the management shell, navigation, common loading/empty/error states, server-validated development identity, role enforcement, and Tenant/Agent App/Deployment management pages. Include basic Gateway/Worker status. Exclude shared external storage, real IM, real model calls, and production authentication.

Within a Tenant and Agent App, at most one Deployment may be active. Activating another Deployment is rejected atomically with HTTP `409` and `agent_app_already_has_active_deployment`; Stage 1 does not automatically pause, replace, weight, or roll back the current Active Deployment. Deployment Version creation requires an `Idempotency-Key` header of 1–128 printable ASCII characters, scoped to `(Tenant ID, Deployment ID, Idempotency-Key)`. Replaying the same key with the same normalized configuration returns the original `201` response without consuming a version number; reusing it with different configuration returns HTTP `409 idempotency_key_reused`. Concurrent replays create exactly one immutable version.

Stage 1 acceptance uses two Tenants and two Agent Apps with independently active Deployment Versions. Integration and Playwright coverage must prove both successful routes select the expected Tenant-scoped Deployment and Version, while a cross-Tenant App guess returns a non-leaking error and never invokes the Runner. The Management Console runtime contract includes `healthy`, `unavailable`, `closing`, and `error` lifecycle states.

### Stage 2: Storage, Synchronization, And Data Management

Deliver immutable monotonically sequenced Session Events, materialized Session state and Summary, Memory, InMemory/Redis/SQLite adapters, PostgreSQL compatibility tests, tenant backend routing, and a repeatable Redis-to-SQL migration command with dry-run, batching, retry, resume, and content/count validation. Extend the frontend with backend selection and health, Session/Event inspection, Memory/Summary status, and migration job creation and progress. Exclude arbitrary SQL, mutation of immutable events, destructive audit editing, and production support for every vector/object vendor.

### Stage 3: Mock IM And Chat Workspace

Deliver the standard Channel Adapter and Channel Binding contract, Mock IM fault semantics, chat Session APIs, ordered history, cancellation, retry, and SSE streaming. Extend the same frontend with a full-height chat workspace reached from Agent App or Deployment pages: create/open Sessions, restore history after refresh, stream replies, cancel, show failures, retry idempotently, and inject Mock IM faults. Browser storage is not conversation history. Web UI validates the IM flow locally but does not count as a real IM provider.

The minimum SSE event envelope contains `event_id`, `request_id`, `session_id`, monotonic `sequence`, `type`, and `data`. Minimum event types are `run.started`, `message.delta`, `message.completed`, `run.failed`, `run.cancelled`, and `run.completed`. Clients deduplicate by `event_id`; reconnect either resumes from the last event position or falls back to persisted Session Events. Server adapters translate runtime events so the frontend does not depend on `trpc-agent-go` internal event types.

### Stage 3.5: Framework Runtime Integration

Consume the published `trpc.group/trpc-go/trpc-agent-go v1.11.2` module and replace the production `EchoRunner` path with an explicit platform adapter around the upstream `runner.Runner`. Keep `EchoRunner` as a deterministic test double and Stage 0 compatibility aid; it is not the production Agent runtime.

Define an `AgentFactory` that builds a minimal deterministic or injectable-model Agent from a server-owned, already-published `Deployment Version`. The request cannot override Agent type, model, prompt, Tool, or runtime configuration. The factory boundary must be extensible for later Tool/MCP, Plugin/Guardrail, and tenant policy configuration without implementing the full governance system in this phase.

Extend the runtime boundary to preserve streaming: translate platform `RunnerRequest` into upstream `model.Message`, `userID`, `sessionID`, and `agent.RunOption` values, then translate upstream `event.Event` values into the stable platform runtime event model and existing SSE envelope. The upstream event types must not cross the platform API boundary. Preserve `tenant_id`, `app_id`, `deployment_id`, `version_id`, `session_id`, and `request_id` through execution and persistence.

Create and cache one upstream Runner per active `Deployment Version`; reuse it for requests, reject requests to inactive versions, and call `Close()` when a version is retired or the service shuts down. The Worker remains stateless with respect to business data; Session/Memory access continues through the Stage 2 tenant-selected adapters. Cancellation must reach the upstream Runner, terminal events must be persisted once, and event channels must be drained or closed during shutdown.

Stage 3.5 excludes real IM provider protocols, production identity, full tenant governance, arbitrary Agent graph authoring, production model credentials, and the complete Tool/MCP policy surface. It must provide framework-backed integration tests with no required external model credential, plus optional credential-dependent smoke tests reported separately.

### Stage 4: Enterprise WeChat And Telegram

Implement Enterprise WeChat and Telegram as the two required real Channel Adapters. Cover webhook verification, signatures, credentials, identity mapping, Channel Binding, duplicates, provider limits, asynchronous replies, retry, and minimum text/media behavior. Extend the frontend with role-enforced provider configuration, enable/disable controls, write-only secret replacement, webhook and latest delivery status, protocol replay, and credential smoke-test status. CI acceptance uses deterministic protocol fixtures and does not require credentials; credential smoke results are recorded separately and cannot be claimed as passed when not executed.

### Stage 5: Governance, Observability, Security, And Production Identity

Build governance on the Stage 3.5 `AgentFactory` and framework `RunnerAdapter`; do not reimplement or replace the upstream runtime. Deliver production identity integration, full backend authorization, tenant Tool/MCP allowlists, Plugin/Guardrail policy wiring, dangerous-tool confirmation, redaction, budgets, tenant rate limits, IM-user authorization, Audit Events, metrics, cost tracking, and trace propagation across callback, Gateway, Worker, AgentFactory, Runner, Tool, Storage, and reply. Extend the frontend with policy and budget management, Audit Event search, metrics/cost views, policy decisions, and trace inspection. Secrets and sensitive values must not appear in logs, traces, errors, API responses, or the DOM.

### Stage 6: Failure Recovery And Operations

Deliver Gateway/Worker and dependency-failure recovery, model/Tool timeout handling, IM retry, context cancellation, event-channel draining, tenant gray release and rollback, capacity estimation, Docker Compose fault injection, and Kubernetes production guidance. Extend the frontend with component health, Deployment Version rollout state, rollback preview/confirmation, development/Compose-only fault injection, capacity results, drain/shutdown status, and recovery evidence. High-risk operations require server authorization, confirmation, and an Audit Event; production fault injection is disabled by default.

## SSE And Trace Evolution

- Stage 3 establishes `request_id` across browser, Gateway, Worker, and Runner plus the stable SSE envelope.
- Stage 3.5 binds that contract to `trpc-agent-go v1.11.2`, translating upstream model and event types through the platform runtime adapter.
- Stage 4 propagates it through provider callback and reply paths.
- Stage 5 associates it with `trace_id` across Tool, Storage, policy, audit, and Channel operations.
- Later stages may add event types without changing the meaning of the Stage 3 minimum set.

## Common Handoff Gate

1. `go test ./...`, applicable integration tests, `go test -race ./...`, `gofmt`, `go vet ./...`, and repository scripts pass.
2. Frontend type checking, unit/component tests, production build, and API contract tests pass.
3. Playwright covers each newly added management workflow and the relevant desktop/mobile layouts are checked for overlap and usability.
4. Scope, exclusions, assumptions, known limitations, pages, public interfaces, data contracts, and reproduction commands are documented and frozen.
5. No undeclared local state, credential, or manual-only step is required for automated acceptance. Credential-dependent smoke tests are reported separately.

The common gate is evaluated using the priority above: all `blocking-acceptance` items must pass; `non-blocking-quality` findings are documented but do not fail the phase gate.

## README Traceability

| README requirement | Delivery phases | Management/frontend evidence | Automated evidence |
| --- | --- | --- | --- |
| Multi-tenant and node deployment | 1, extended by 5–6 | Tenant/App/Deployment, identity, node status, rollout pages | Isolation, routing, role authorization, concurrency, rollout tests |
| Data synchronization and multi-backend support | 2, hardened by 6 | Backend, Session/Event, Memory/Summary, migration and health pages | Adapter contracts, ordering, idempotency, cross-node visibility, migration validation |
| tRPC-Agent-Go runtime integration | 3.5, consumed by 4–6 | Runtime-backed Deployment validation and streaming chat evidence | Version pinning, AgentFactory, input/event mapping, cancellation, lifecycle, and framework integration tests |
| At least two IM providers including WeChat/Enterprise WeChat | 3–4 | Mock chat workspace plus Enterprise WeChat and Telegram configuration/replay pages | Mock failure tests and two real-provider protocol suites; Web UI is excluded from the provider count |
| Governance, monitoring, and security | 3.5 foundation, 5 delivery | Policy, budget, audit, metrics, cost, trace, and identity pages | Authorization, denial, redaction, audit completeness, metric and trace tests |
| Failure recovery and operations | 6 | Health, gray release, rollback, fault, drain, capacity, and recovery pages | Compose failure injection, cancellation, draining, recovery, rollback, and capacity smoke tests |
| Complete message chain with request/trace identity | 3–5 | Streaming chat, delivery status, and trace inspection | SSE ordering plus request/trace propagation from UI/callback through reply |
| Final architecture and risk deliverables | 6 | Operations evidence links to final delivery artifacts | Documentation consistency gate and at least eight tested/documented risk mitigations |
