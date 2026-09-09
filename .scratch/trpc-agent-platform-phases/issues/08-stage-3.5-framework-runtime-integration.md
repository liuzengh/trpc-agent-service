# Stage 3.5: tRPC-Agent-Go Framework Runtime Integration

Type: task
Status: needs-triage
Blocked by: 04

## Goal

Bind the frozen Stage 3 platform contracts to the published `trpc.group/trpc-go/trpc-agent-go v1.11.2` runtime before implementing real IM providers.

## In Scope

Pin the official Go module version; implement the `AgentFactory`; build a minimal deterministic or injectable-model Agent; implement the streaming `RunnerAdapter`; map platform requests to `model.Message`, `userID`, `sessionID`, and `agent.RunOption`; map upstream `event.Event` values to the platform runtime event model and SSE envelope; preserve request and tenant/deployment identity; cache one Runner per active Deployment Version; support version retirement, `Close()`, context cancellation, event-channel draining, and service shutdown; use Stage 2 Session/Memory adapters; add framework-backed integration tests without required external credentials.

## Out Of Scope

Real IM provider protocols, production identity, full governance and policy enforcement, arbitrary Agent graph authoring, required production model credentials, and the complete Tool/MCP integration surface. EchoRunner remains a deterministic test double and Stage 0 compatibility aid.

## Acceptance

- `go.mod` and `go.sum` resolve `trpc.group/trpc-go/trpc-agent-go v1.11.2`; acceptance does not depend on a local `replace` directive.
- Published Deployment Version configuration is the only AgentFactory input; request fields cannot override Agent, model, prompt, Tool, or runtime configuration.
- Framework-backed execution proves input mapping, identity propagation, ordered event mapping, stable SSE behavior, normal completion, failure, cancellation, and terminal-event idempotency.
- Runner reuse, inactive-version rejection, version retirement, `Close()`, context cancellation, event-channel draining, and service shutdown are covered by tests.
- Tenant-scoped Session/Memory access remains isolated and cross-Tenant requests cannot reach the framework Runner.
- Existing Stage 3 Echo/Mock tests remain deterministic and pass as test-double coverage; framework integration tests run without external model credentials.

## Handoff

Freeze the `AgentFactory`, streaming `RunnerAdapter`, framework version, request/event mappings, Runner lifecycle, cancellation/shutdown semantics, and runtime integration test fixtures for Stage 4 and Stage 5.
