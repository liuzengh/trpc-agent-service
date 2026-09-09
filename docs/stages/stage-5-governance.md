# Stage 5 Governance, Observability, And Production Identity

Stage 5 places a server-owned governance boundary around the existing Stage 3.5
framework runtime. The platform still executes Agents through the published
`trpc-agent-go v1.11.2` Runner; policy is evaluated before Session input is
persisted or the Runner is invoked, and the framework Runner registers the
`platform-governance` Plugin for Agent callbacks.

## Authentication Modes

`TRPC_AUTH_MODE` is explicit:

- `development` (or empty) uses the deterministic Development Identity and is
  intended only for local development and automated acceptance.
- `production` disables Development Identity and requires a signed JWT to be
  exchanged for a bounded, HttpOnly `trpc_auth_session` cookie.

Production mode requires:

| Variable | Purpose |
| --- | --- |
| `TRPC_IDENTITY_DIRECTORY` | JSON file mapping JWT `sub` values to server-owned Tenant assignments and roles |
| `TRPC_AUTH_ISSUER` | Required JWT `iss` value |
| `TRPC_AUTH_AUDIENCE` | Required JWT `aud` value |
| `TRPC_AUTH_HMAC_SECRET` | HS256 verification key used by deterministic local acceptance |

The token exchange validates signature, issuer, audience, expiry, subject, and
the identity directory. Tenant switching only selects an assignment already in
that directory. Tokens are not stored in browser storage and are discarded by
the console after exchange. Hosted OIDC discovery, JWKS rotation, and RS256 are
not implemented; a hosted verifier can replace the `IdentityProvider`.

Example identity directory:

```json
{
  "operator-1": {
    "id": "operator-1",
    "name": "Operator One",
    "assignments": [
      {"tenant_id": "tenant-a", "tenant_name": "Tenant A", "role": "operator"}
    ]
  }
}
```

## Governance Policy

`TenantPolicy` is keyed by `(tenant_id, agent_app_id)` and receives a monotonically
increasing revision. It contains Tool and MCP allowlists, dangerous Tool names,
input/output Guardrail patterns, write-only redaction patterns, allowed external
IM users and subjects, token/cost budgets, estimated reservation size, model
cost, per-Tool costs, and a Tenant request-limit window. When a monetary budget
is active, every declared Tool must have an explicit price (zero is allowed),
so unknown pricing cannot silently bypass the budget. Requests cannot supply or override this
policy. Updating an Agent App policy preserves the Tenant's already-accounted
token/cost usage, so a policy revision cannot reset or bypass a Tenant budget.
Metrics expose the period start, limits, consumption, and remaining allowance.

The request path is:

```text
identity -> tenant/app route -> IM authorization -> Tool/MCP and Guardrail policy
         -> rate/budget reservation -> Gateway -> Worker -> AgentFactory -> Runner
         -> output Guardrail/redaction -> Session storage -> Channel reply
```

Dangerous Tool requests create a durable confirmation in the framework
`BeforeTool` callback, after the real arguments are available and before the
Tool can execute. The record contains only a SHA-256 argument summary. An
operator or administrator in the same Tenant approves or rejects it, then the
client retries with the same request ID. Approval is atomically consumed and
the `AfterTool` callback records completed, failed, or cancelled state and real
Tool latency. Confirmations expire after 15 minutes. No suspended goroutine is
used as the record of truth.

## Management APIs

All endpoints derive Tenant Context from authentication middleware:

- `GET|POST|PUT /api/v1/admin/governance/policy`
- `GET /api/v1/admin/governance/audit`
- `GET /api/v1/admin/governance/metrics?app_id=...&provider=...&from=...&to=...`
- `GET /api/v1/admin/governance/traces?trace_id=...&request_id=...`
- `GET /api/v1/admin/governance/confirmations`
- `POST /api/v1/admin/governance/confirmations/{id}/decision`

Audit search supports `from`, `to`, `user_id`, `channel`, `session_id`,
`agent_name`, `decision`, `error_type`, `request_id`, `trace_id`, `offset`, and
`limit`; the maximum page size is 200. Audit records are append-only through the
public API. Successful mutation responses are buffered until their Audit Event
is durably accepted and otherwise become `503 audit_unavailable`. Policy
redaction patterns are write-only and reads return only `[REDACTED]`
placeholders.

## Audit, Metrics, And Traces

Audit Events include Tenant, channel, user, Session, Agent App, Tool, decision,
latency, error type, cost, request ID, trace ID, and occurrence time when those
values are available. Authentication, authorization, policy, confirmation,
management mutation, and execution outcomes use stable decision/error codes.

Tenant metrics expose request, active/completed/failed/denied/rate-limited
execution counters, token and cost totals, model/Tool/storage latency,
end-to-end execution latency, and IM delivery totals. Model latency is measured
at the framework's actual BeforeModel/AfterModel callbacks rather than inferred
from the complete Runner duration. The deterministic Runner estimates token usage when upstream
usage metadata is absent. Budgets use per-request reservations so concurrent
runs cannot collectively start beyond the configured allowance. Reservations
that remain active for 15 minutes are reconciled exactly once before the next
admission decision. Metric samples use only bounded Tenant, Agent App, and
provider dimensions; queries default to 24 hours and reject ranges over 31 days.
An authenticated Prometheus text endpoint is available at
`GET /internal/metrics`; it uses the same internal Bearer credential as Worker
governance calls and must remain on the cluster-internal network.

The platform trace model is deliberately independent of upstream telemetry
types. `trace_id` follows browser/provider ingress, policy, Gateway, Worker,
AgentFactory, Runner, Model/Tool authorization, successful and failed
Session/Memory/Knowledge storage reads, storage writes, and reply. Lookup is
Tenant-scoped by trace or request ID. This is an equivalent bounded trace model,
not an OTLP exporter. Each span has a stable span ID and the preceding operation
as its parent, forming an inspectable causal chain. Trace snapshots persist with
governance state.

## Persistence And Limitations

`TRPC_GOVERNANCE_PATH` selects the atomic JSON persistence file and defaults to
`data/governance.json`. Policies, audits, confirmations, metrics, and accounted
tokens survive restart. This local file is appropriate for deterministic
acceptance but is not a distributed control-plane store; multi-node policy
distribution and globally shared counters remain Stage 6/production work.

Write-only redaction patterns are AES-GCM encrypted in the governance snapshot.
The adjacent `<TRPC_GOVERNANCE_PATH>.key` file is generated with mode `0600` and
must be backed up and protected together with the snapshot; losing it makes the
encrypted redaction configuration unreadable. Legacy plaintext snapshots are
read for compatibility and migrate to encrypted form on the next write.

Redaction is applied at platform logs, policy responses, Runner input/output,
Audit/Trace attributes, public errors, and Provider diagnostics. Explicitly
clearing an existing write-only redaction pattern is not modeled: a policy
update containing only placeholders preserves the stored values.
The process log Redactor includes Worker/governance/manifest credentials, model
and IM credentials, Redis/backend configuration, and every supported PostgreSQL
DSN; URL passwords are independently registered as redaction values.
