# 03: Tenant-Isolated Agent App Management

**What to build:** An authorized tenant user can register and inspect Agent Apps in the selected Tenant through the management API and Management Console. All reads and writes derive ownership from trusted Tenant Context, so an identity cannot discover or mutate another Tenant's Agent Apps by changing request data or resource identifiers.

**Blocked by:** 02: Tenant Management

**Status:** resolved

- [x] An authorized `platform_admin` or `tenant_admin` can create an Agent App for the active Tenant and retrieve it through list and detail views.
- [x] `operator` and `viewer` permissions are explicitly defined and enforced by the backend for Agent App reads and mutations.
- [x] Agent App ownership is derived from trusted Tenant Context rather than a client-supplied `tenant_id`.
- [x] Cross-Tenant list, detail, create, and mutation attempts cannot reveal or alter another Tenant's Agent Apps and return stable non-leaking errors.
- [x] Duplicate identifiers, invalid names, missing Tenants, malformed requests, and unsupported methods return structured API errors.
- [x] The Management Console renders Agent App list, create, detail, loading, empty, validation, forbidden, and service-error states from backend data.
- [x] Automated isolation tests use at least two Tenants and two Agent Apps and cover both authorized and adversarial requests.
- [x] Component and Playwright tests verify Agent App management and Tenant switching on desktop and mobile layouts.

## Answer

Implemented Tenant-scoped Agent App list, create, and detail APIs. Ownership is always derived from trusted Tenant Context; client tenant fields and arbitrary headers do not grant access. Platform and tenant administrators may create, all four Stage 1 roles may read their active Tenant, and cross-Tenant lookups return a non-leaking not-found response. Two-Tenant API isolation tests pass.
