# 09: Two-Tenant Routed Acceptance Matrix

**What to build:** A reproducible packaged acceptance path creates two Tenants with separate Agent Apps and Active Deployment Versions, successfully routes each App through Gateway, Worker, and the fake Runner, and proves a cross-Tenant App guess is rejected before Runner execution. The workflow uses the existing Management Console and management run API without introducing the Stage 3 Chat Workspace.

**Blocked by:** 04: Deployment Drafts And Immutable Versions; 05: Deployment Lifecycle And Routing Loop

**Status:** resolved

- [x] Integration acceptance creates two Tenants, two Agent Apps, separate Deployments, immutable Versions, and one Active Deployment per Tenant/App using the finalized Version and lifecycle contracts.
- [x] Both successful routes reach the Runner exactly once and carry the expected trusted Tenant, Agent App, Deployment, Deployment Version, and Session identities.
- [x] After switching Tenant Context, guessing the other Tenant's Agent App returns the stable non-leaking not-found response and does not increase Runner invocations.
- [x] The packaged Playwright workflow provisions and activates both Tenant/App paths through the Management Console, invokes the management run API through the same authenticated browser session, and verifies both successes and the cross-Tenant rejection.
- [x] Desktop and mobile Playwright projects complete the matrix without adding a chat UI, relying on credentials, or introducing undeclared local state.
- [x] Tests remain deterministic under normal and race-enabled Go execution and preserve the Stage 0 `/v1/run`, `/healthz`, and `/version` contracts.

## Answer

Added a two-Tenant/two-Agent-App integration matrix that captures trusted Tenant Context and resolved Deployment Version at the Runner boundary, plus desktop/mobile packaged Playwright workflows that create both paths through the Management Console, execute both routes, and prove a cross-Tenant guess is rejected before Runner execution.
