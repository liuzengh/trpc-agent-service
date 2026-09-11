# Control OpenAPI v1

`openapi.yaml` describes the Control API management contract: Identity, Platform
Operator administration, Tenant membership, Agent authoring, Runtime Profile,
Deployment publication, and Channel account/binding management. Runtime Profile directly adjusts the current V1; it
has no reference-only DTO or old Schema/Digest compatibility stack.

The eight Deployment operations are implemented by the tenant-scoped Gin Handler,
registered by the Control API Bootstrap, and covered by route-level contract tests.
Their publication boundary ends at an atomic PostgreSQL Revision, RuntimeManifest,
Receipt, and `PENDING` Outbox event. Relay/JetStream delivery and runtime execution
are not part of these HTTP operations.

## Deployment: 8 management operations

All paths below use the prefix `/v1/tenants/{tenant_id}/deployments`.

| Method | Suffix | Request / response |
| --- | --- | --- |
| POST | empty | metadata + `Idempotency-Key`; new `201`, identical replay `200` |
| GET | empty | paginated Deployment metadata |
| GET | `/{deployment_id}` | Deployment metadata and latest publication number |
| PATCH | `/{deployment_id}` | metadata CAS using `expected_metadata_revision` |
| POST | `/{deployment_id}/validate` | closed two-source Input; `200` Report even when `valid=false` |
| POST | `/{deployment_id}/revisions` | expected latest + Input + `Idempotency-Key`; new `201`, identical replay `200` |
| GET | `/{deployment_id}/revisions` | metadata-only Revision Summary page |
| GET | `/{deployment_id}/revisions/{revision_number}` | full Revision + fixed `manifest_view` |

Deployment Input contains only `schema_version=v1`, exact
`agent.agent_id/version_number`, and exact
`profile.profile_id/revision_number`. It has no Deployment Draft, Environment,
latest selector, binding map, user-supplied digest, runtime-role mapping, worker
option, or automatic activation flag. Validate receives that Input directly and
does not accept Expected Latest or `Idempotency-Key`. Publish requires nullable
`expected_latest_revision_number`: null is valid only for the first publication;
later commands provide the exact current positive number.

Create and Publish alone use `Idempotency-Key`. An identical retry returns the
original stable response; reuse with a different request returns `409`. Metadata
PATCH uses its independent CAS. Compatibility failures return `200 valid=false`
from Validate and `422` plus the same typed report from Publish. Transport DTO
errors use `400`, body limits `413`, hidden/missing sources `404`, CAS or receipt
conflicts `409`, credential-metadata owner outages `503`, and stored integrity
failures `500`.

Revision lists use `DeploymentRevisionSummary` and do not load canonical input or
manifest content JSONB. Full reads and Publish responses verify the stored input,
complete internal Manifest, digest, and identity relationships before projecting
the explicit `RuntimeManifestView`. That public view contains only
`credential_present` booleans. It never exposes Credential IDs, purposes,
audience digests, values, ciphertext, association tokens, or mutable credential
state. `manifest_digest` remains the digest of complete internal canonical
content and is not recomputed from the public view.

The internal Manifest accepts only the Profile-owned purposes `api_key`,
`bearer_token`, `qdrant_api_key`, `embedding_api_key`, and `dsn`. PostgreSQL
storage destinations contain fixed `host`, `port`, `database`, `username`, and
`sslmode`; the credential descriptor is separate and contains no value.

## Runtime Profile: 11 management operations

All paths below use the prefix `/v1/tenants/{tenant_id}/runtime-profiles`.

| Method | Suffix | Request / response |
| --- | --- | --- |
| POST | empty | create metadata; `201` with profile + public draft |
| GET | empty | paginated Profile metadata |
| GET | `/{profile_id}` | Profile metadata |
| PATCH | `/{profile_id}` | name/description update |
| GET | `/{profile_id}/draft` | public ProfileRead with current credential states |
| PUT | `/{profile_id}/draft` | ProfileWrite and Idempotency-Key; DraftWriteResult |
| POST | `/{profile_id}/credentials/update` | explicit live credential update and Idempotency-Key |
| POST | `/{profile_id}/draft/validate` | `{expected_revision}`; Validation Report |
| POST | `/{profile_id}/revisions` | `{expected_revision}`; first publish `201`, replay `200` |
| GET | `/{profile_id}/revisions` | Revision Summary page |
| GET | `/{profile_id}/revisions/{revision_number}` | public ProfileRead with fixed config and current credential states |

Draft PUT accepts `expected_draft_revision`, `credential_protocol_version: v1`,
`config`, and `credentials`. The four config collections must be explicit; PUT
replaces configuration rather than overlaying or deep-merging it. Credential
purposes accept typed keep/replace/clear actions; missing actions keep only the
current association for a retained, unchanged purpose/destination.

Draft PUT and live POST require an Idempotency-Key of 1–128 visible ASCII
characters without spaces. Complete input is limited to 512 KiB and a single
credential value to 64 KiB. Unknown, duplicate, case-variant, null, malformed,
masked, or inappropriate action/value inputs are rejected without echoing secrets.

PUT returns only `profile_id`, `draft_revision`, and `updated_at`. Live POST returns
`credential_revision` and the fixed command-result `status`. Live target fields are
`profile_revision_number`, `category`, `resource_name`, `purpose_field`, and
`association_token`; the action is replace/clear with a positive independent
`expected_credential_revision`. Clients never submit internal CredentialIDs.

## Write, Canonical, and Read ownership

Public writes use independent DTOs. Internal Canonical Spec uses both
`schema_version=v1` and `credential_protocol_version=v1`, non-secret configuration,
and server-generated CredentialIDs. Current credential values are encrypted in
Profile-owned storage, not copied into Spec, Manifest, receipts, or logs.

Public Draft/Revision reads use `config` and optional `credential_states`; they
never include `spec`, internal IDs, values, ciphertext, or masked fragments of the
original value. Detail states may include configured/status/credential_revision
and conditional-write association tokens, but these are mutable projections.
`spec_digest` refers to the internal Canonical document, not this redacted view.

Publish returns `{revision: ProfileRead}` **without dynamic credential_states**.
Its idempotent result remains stable after live rotation or clearing. Revision
lists use `RuntimeProfileRevisionSummary`, omit Spec/config/states, and do not load
`spec_jsonb`. Full detail/publication reads first verify internal Canonical content
and stored Digest, then redact. The Canonical JSON Schema is not the public GET or
PUT DTO schema.

## Authorization, COW, and live updates

ACTIVE Tenant OWNER/MEMBER can read, edit ordinary configuration, validate, and
publish. Credential replace/clear, removal of credential-bearing resources, and
live updates require OWNER. Platform Operator status does not bypass membership.

Ordinary Draft replace is copy-on-write: new CredentialID, new initial version,
no change to published associations. Draft clear only unlinks the Draft. Explicit
live replace updates the active value behind the same ID; live clear terminates
that ID. Neither changes published Revision/Manifest content or Digest. Draft CAS,
Credential CAS, authorization rechecks, and request-MAC receipts are independent
and transactional.

Storage credentials accept a PostgreSQL URI with explicit sslmode
`disable`/`require`/`verify-full` and no other options. Only password is encrypted;
`destination` fixes host, port, database, username, and sslmode. A live password
replacement cannot change that destination.

## Deployment and runtime status

Profile publication stays static and Agent-independent. Deployment matches
AgentVersion requirements by exact category/name in a selected ProfileRevision,
checks compatibility, and fixes only required resources in Manifest. No extra
resource mapping table, capability search, or fuzzy lookup is used. V1 has no
Environment, environment_id, hidden default, overlay, or inherited configuration.

The current Control Publication implementation includes the closed Deployment
schemas and event, pure Compiler, Application commands/queries, Profile-owned
credential metadata checks, PostgreSQL atomic publication, all eight HTTP routes,
Bootstrap wiring, and real PostgreSQL integration coverage. Successful Publish
persists `RuntimeManifestPublished.v1` as `PENDING`; its Manifest distribution path
remains follow-up work. Channel has a separate implemented route-only Relay and
Gateway projection, validated through real Telegram inbound-to-RunRequested. This
does not establish Worker execution, Manifest body retrieval or the full reply chain.

Profile consumer Application methods exist. The optional Profile-owned internal
adapter route `POST /internal/v1/runtime-profiles/credentials/resolve` is separate
from these 11 management operations and is not a public management GET. It requires
paired trusted workload authentication and an ExecutionAuthorizationVerifier.
Default bootstrap supplies no real execution-owner verifier and does not enable
that route. Real Run/Attempt authorization, internal deployment transport, and
Worker batch initialization remain follow-up integration; no default permission
is assumed.

Control-plane DTO, storage, publication, route, and integration tests establish
this implemented management surface; they do not establish runtime execution.
The tool kind remains `mcp_streamable_http`. Capability uses the existing AgentSpec
grammar `^[a-z][a-z0-9_.-]{0,127}$` and exact requirement matching; `web.search`
remains valid. The declaration is not evidence of server behavior or authorization.
Future built-in/workspace tool protocols are not added by this credential change.

See the [credential contract](../../../../docs/architecture-next/control-api/runtime-profile-credentials.md)
for internal consumption, failure, and key-configuration boundaries.
Generated clients and server bindings belong under `/gen`, not this directory.

## Channel: 12 implemented management operations

`openapi.yaml` references `channel-public.yaml` for the configured Channel surface.
The request schemas directly reference the canonical closed Channel V1 JSON Schemas;
OpenAPI 3.1 conditionals (`if`/`then`/`else`) are preserved rather than weakened.
Eight OWNER write commands require `Idempotency-Key`; both original create and
its valid replay return `201`. Four member read operations expose only redacted
account/binding/status projections. Lists use stable-ID cursor/page_size (1–100,
default 50), not offset pagination. Internal mTLS endpoints are deliberately absent
from this public Session API; their exact contracts live in Channel shared schemas
and the Control Channel runtime guide. Profile-owned schemas and APIs are unchanged.

## Telegram preflight: two public and three private operations

`preflight-public.yaml`, referenced by `openapi.yaml`, adds OWNER
`POST .../channel-accounts/{account_id}/preflights` (202, fixed idempotent
creation receipt) and ACTIVE MEMBER `GET .../preflights/{preflight_id}` (200,
redacted task/result). Existing 12 Channel management operations are unchanged.
A saved disabled Telegram account needs no Binding/Deployment or READY state.
All three expected account/connection/BotToken versions are required. State,
diagnostic outcome, account freshness and unconfirmed Gateway config freshness
are separate; a completed PASS is not real Telegram delivery verification.

`preflight-internal.yaml` is a separate OpenAPI 3.1 **mTLS** document for
claim/diagnostic-BotToken-resolve/complete. None of these paths is exposed in the
public Session document. URI SAN principal mapping and explicit diagnostic kind
`telegram_preflight` govern access; normal enabled-only credential consumers
retain their original semantics. All success responses are `Cache-Control:
no-store`; 200 responses are JSON, 204 responses have no body. The only plaintext
value field is the private, lease-bound resolved response.

Both documents reference the eight canonical closed Channel preflight schemas.
The pinned kin-openapi parser handles their OpenAPI structure but not all 3.1
keywords: tests allow `const`, conditionals and `prefixItems` while the canonical
JSON Schema validator independently enforces them. It also lacks `mutualTLS`
validation, so the private-document test explicitly verifies that exact security
shape and validates paths, parameters, responses and schemas without replacing
certificate authentication with a fabricated API-key scheme.
