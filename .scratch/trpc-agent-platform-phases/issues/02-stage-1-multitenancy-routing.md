# Stage 1: Multi-Tenant Routing And Basic Management

Type: task
Status: resolved
Blocked by: 01

## Goal

Run the first platform loop and establish the progressive frontend: create and manage Tenant and Agent App, publish a Deployment, route through Gateway to a Worker, execute a fake Runner, and return a result.

## In Scope

In-memory Tenant/App/Deployment repositories; `draft -> published -> active -> paused` lifecycle with Deployment Version; at most one Active Deployment per Tenant and Agent App; idempotent Deployment Version creation through required `Idempotency-Key`; trusted Tenant Context; stateless Worker; same-Session serialization and cross-Session concurrency; cross-tenant rejection; cancellation and event propagation. Establish `frontend/` with React, TypeScript, Vite, and npm; management shell and common states; Tenant, Agent App, Deployment, Deployment Version, and basic Gateway/Worker status pages; server-validated development identity and backend-enforced `platform_admin`, `tenant_admin`, `operator`, and `viewer` roles.

## Out Of Scope

Shared external state, real model calls, IM protocols, production authentication, knowledge/data management, gray release, and rollback execution.

## Acceptance

Two-Tenant/two-App isolation and route correctness, single-Active-Deployment enforcement, sequential and concurrent Version idempotency, lifecycle transitions, same-Session serialization, different-Session concurrency, cancellation, shutdown, role authorization, all four runtime lifecycle states in the frontend contract, frontend type/component/build checks, API contracts, and Tenant/App/Deployment Playwright flows all pass on desktop and mobile layouts.

## Handoff

Freeze Gateway/Worker/Runner contracts, Tenant Context and development identity semantics, role vocabulary, management API conventions, frontend build boundary, Session key, single-Active-Deployment semantics, and Deployment Version idempotency semantics for Stage 2 after the rework gate passes.

## Comments

- Stage 1 code review found incomplete two-Tenant/two-App routed acceptance, nondeterministic routing when one Agent App has multiple active Deployments, undefined Deployment Version replay behavior, and a missing frontend `error` lifecycle state. The confirmed rework contract is recorded in the Stage 1 child tickets, acceptance, and handoff documents.
- Rework completed through Tickets 04, 05, 07, 08, and 09. The complete Go, race, lint, vet, frontend type/component/build, combined build, and desktop/mobile Playwright gate passed before Stage 2 handoff was restored.
