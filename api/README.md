# Versioned protocols

This directory contains source protocol definitions shared across workloads.
It may contain OpenAPI documents, JSON Schema, and versioned event schemas. It
must not contain domain entities, repositories, or handwritten shared business
logic.

Current protocol namespaces:

- `openapi/control/v1`: the implemented Control API contract. Identity, Admin,
  Tenant, Agent V1, Runtime Profile V1, and the eight Deployment V1 management
  operations are registered through their Handler and Bootstrap wiring.
  The tenant-scoped runtime backend directory is also wired; listing metadata
  does not imply that the selected backend has an executable Worker adapter.
- `schemas/agentspec/v1`: the frozen AgentSpec V1 JSON Schema and examples.
- `schemas/runtimeprofile/v1`: the frozen RuntimeProfileSpec V1 JSON Schema
  and positive/negative examples.
- `schemas/deployment/v1`: closed Deployment Input, internal RuntimeManifest,
  and public credential-identifier-free RuntimeManifest View schemas with
  positive/negative examples.
- `events/control/v1`: the closed `RuntimeManifestPublished.v1` Control Outbox
  payload contract and fixtures. Deployment publication persists this event in
  the same transaction with status `PENDING`; no Relay, JetStream delivery,
  consumer, or runtime projection is implemented yet.
- `events/execution/v1`: reserved for versioned execution-plane events; no
  event contract is implemented yet.
