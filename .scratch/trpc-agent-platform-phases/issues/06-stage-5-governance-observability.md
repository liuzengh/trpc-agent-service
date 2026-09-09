# Stage 5: Governance, Observability, Security, And Production Identity

Type: task
Status: needs-triage
Blocked by: 05 and 08 (Stage 3.5 Framework Runtime Integration)

## Goal

Enforce tenant-level governance and production authorization, make every execution auditable and traceable, and expose those controls and results safely in the management frontend.

## In Scope

Production identity integration and backend authorization around the Stage 3.5 `AgentFactory` and framework `RunnerAdapter`; Tool/MCP allowlists; Plugin/Guardrail policy wiring; redaction; budget and rate limits; dangerous-tool confirmation; IM-user authorization; Audit Events; trace propagation across callback, Gateway, Worker, AgentFactory, Runner, Tool, Storage, and reply; OpenTelemetry or equivalent; tenant metrics, cost, and secret handling. Extend the frontend with policy/budget management, Audit Event search, authorization decisions, metrics/cost views, and trace inspection.

## Out Of Scope

A specific hosted alerting product or production secret vendor integration unless separately approved, and exposing secret values in any management response or page.

## Acceptance

The Stage 3.5 framework-backed runtime is used for governance tests. Production authorization, policy denial before Agent/Tool execution, Tool/MCP allowlist enforcement, Plugin/Guardrail decisions, redaction, budget, tenant throttling, audit completeness, end-to-end request/trace propagation, metric/cost collection, frontend/API contracts, management Playwright flows, and secret non-disclosure tests including DOM assertions pass.

## Handoff

Freeze identity/role enforcement, governance policy contracts, audit schema, trace fields, metric/cost names, management APIs, and security invariants for Stage 6.
