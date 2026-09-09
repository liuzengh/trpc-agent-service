# Stage 2: Data Synchronization, Multi-Backend, And Data Management

Type: task
Status: needs-triage
Blocked by: 02

## Goal

Make Workers horizontally scalable through shared state and expose that state safely through the progressive management frontend.

## In Scope

Immutable, monotonically sequenced Session Events with idempotency keys; materialized Session state and Summary; InMemory, Redis, and SQLite adapters; PostgreSQL compatibility profile; tenant-level backend routing; cross-node visibility and concurrency rules; repeatable Redis-to-SQL Session/Memory migration with dry-run, batches, retry, resume, and count/content report; vector/object-store contracts and migration design. Add backend selection and health, Session/Event inspection, Memory/Summary status, and migration job/progress/result pages to the Stage 1 frontend.

## Out Of Scope

Production migration of every vector/object-store vendor, provider-specific online cutover, arbitrary SQL, mutation of immutable Session Events, and destructive audit editing.

## Acceptance

Ordering, duplicate writes, concurrent writes, cross-node reads, Redis integration, SQLite tests, PostgreSQL profile, migration validation, transient backend error classification, frontend checks, API contracts, and data-management Playwright flows pass.

## Handoff

Freeze Storage Adapter, data model, consistency guarantees, migration report/API format, backend selection and health rules, and frontend data-view contracts for Stage 3.
