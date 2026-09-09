# Stage 0: Engineering Baseline And Platform Contracts

Type: task
Status: needs-triage
Blocked by: none

## Goal

Create a reproducible build/start/stop/test baseline and define the platform ports without implementing real model or IM integrations.

## In Scope

- Minimal `cmd/trpc-service` entry point, health check, and version endpoint.
- Context cancellation, graceful shutdown, and goroutine lifecycle tests.
- Internal application-service ports plus minimal HTTP JSON adapters.
- Contracts for Tenant, Agent App, Deployment, Gateway, Worker, Session, Session Event, Memory, Runner, Channel Adapter, Storage Adapter, and Audit Event.
- Injectable fake Runner and test layering conventions.
- Context/ADR updates required by resolved terms and decisions.

## Out Of Scope

Real model calls, external storage, real authentication, real IM protocols, production telemetry, and production deployment.

## Acceptance

`go test ./...`, `go vet ./...`, `./build.sh`, `./start.sh`, and `./stop.sh` pass; health/version work; shutdown cancels work without leaks; fake Runner is replaceable; HTTP is an adapter rather than the domain implementation.

## Handoff

Freeze the ports and test commands. Record all intentionally absent integrations for Stage 1.
