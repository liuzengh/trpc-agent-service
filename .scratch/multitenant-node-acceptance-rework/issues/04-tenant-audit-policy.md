# 04: Tenant Audit Policy

Type: task
Status: ready-for-human

## Goal

Replace the undocumented `audit_policy` placeholder with a concrete, durable,
tenant-isolated policy that affects audit behavior.

## Work

- Add `AuditPolicy` and `AuditContentMode` from `spec.md` to `Tenant`.
- Normalize existing Tenants to deterministic defaults on load.
- Add validated, role-enforced tenant policy update/read behavior.
- Enforce retention and content mode in audit storage/query paths while keeping
  high-risk operations fail-closed.
- Update the architecture and data-model documents.

## Tests

- Defaults, invalid values, role denial, cross-Tenant denial, update, and durable
  reload.
- Retention filtering/cleanup and metadata-only versus redacted-summary output.
- Canary credentials never appear in policy responses, Audit, Trace, logs, or
  the frontend DOM.

## Done

The documented fields match Go and JSON contracts, survive both SQLite and
PostgreSQL Control Plane reloads, and visibly govern audit behavior.

## Evidence

- `go test ./trpcservice/platform/...` passed defaults, validation, role scope,
  persistence, retention, redaction, and metadata-only behavior.
- Stage 7 Compose passed fail-closed governance and dangerous Tool
  `outcome_unknown` checks without exposing credentials.
