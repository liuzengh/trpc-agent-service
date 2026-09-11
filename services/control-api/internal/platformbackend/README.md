# Platform backend directory

Implemented: immutable non-secret directory, strict startup configuration and
`GET /v1/tenants/:tenant_id/runtime-backends`, composed by Control bootstrap.
The route uses Identity authentication, rejects restricted sessions, checks current
Tenant membership and filters the platform allowlist. Responses use `no-store`.
There is no registration or credential API in this module.

Set both `CONTROL_PLATFORM_BACKEND_CATALOG_FILE` and
`CONTROL_PLATFORM_BACKEND_CATALOG_SHA256` (64 lowercase hex characters, SHA256 of
exact file bytes). Both absent means an empty directory; partial/invalid config
fails before database initialization. Change the deployment-pinned digest together
with catalog bytes on every replica. No hot reload or latest-revision fallback.

```json
{
  "version": "v1",
  "backends": [
    {"id":"sessions-pg","revision":1,"label":"SQL Session","kind":"postgresql","roles":["session","memory"],"enabled":true,"tenant_ids":["tenant-a"]},
    {"id":"sessions-redis","revision":1,"label":"Redis Session","kind":"redis","roles":["session"],"enabled":true,"tenant_ids":["tenant-a"]},
    {"id":"knowledge","revision":1,"label":"Knowledge","kind":"qdrant","roles":["knowledge"],"enabled":true,"tenant_ids":["tenant-a"]},
    {"id":"artifacts","revision":1,"label":"Artifacts","kind":"s3","roles":["artifact"],"enabled":true,"tenant_ids":["tenant-a"]}
  ]
}
```

`enabled` describes configured directory eligibility, not live backend health.
Profile managed selection is now checked during Draft save/validation and first
publication through a consumer-owned BackendAccess port. A separate private typed
physical target file is loaded with an exact SHA256 and owner-only permissions.
Bootstrap connects the resolver to Deployment when managed publication is explicitly
enabled. Both catalog hashes contribute to the release-pinned platform contract.
See `deploy/compose/managed-data/README.md` for configuration. No credentials are
stored in either file. Worker clients and four-backend execution remain pending.

Tests cover HTTP authentication/membership/allowlist isolation and no secret
projection, strict duplicate/null/unknown/case-variant rejection, exact digest
pinning, and the startup-before-database rejection gate. Route tests use a real
Gin router with injected Identity/Membership; they do not prove a live login or
four-backend execution.
