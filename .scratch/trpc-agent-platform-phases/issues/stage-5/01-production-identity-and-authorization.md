# 01: Production Identity Provider And Unified Backend Authorization

**What to build:** Operators can run the platform with a production Identity Provider that validates OIDC-compatible JWTs and establishes a trusted Tenant Context. Every management, Chat, data, and Provider operation is authorized by the backend from that identity, while Development Identity remains available only in an explicit development mode.

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] Production mode validates token signature, issuer, audience, expiry, subject, and server-owned Tenant assignments using deterministic local fixtures that require no hosted identity service.
- [x] Missing, malformed, expired, or otherwise invalid credentials return stable non-leaking public errors and never establish Tenant Context.
- [x] The complete API surface enforces the frozen `platform_admin`, `tenant_admin`, `operator`, and `viewer` role matrix on the backend; UI visibility is not treated as authorization.
- [x] Tenant selection is limited to server-approved assignments, and request bodies, arbitrary headers, query parameters, cookies, or browser state cannot grant a Tenant or role.
- [x] Development Identity and tenant switching work only when explicitly enabled and cannot be enabled by a request in production mode.
- [x] The Management Console represents authenticated, unauthenticated, forbidden, and session-expired states and exposes only the subject's approved Tenant assignments.
- [x] Two-Tenant API and management tests prove allowed access, denied mutations, non-leaking cross-Tenant guesses, and production/development mode separation.

## Answer

Added an `IdentityProvider` boundary, deterministic HS256 JWT validation, server-owned Tenant/role assignments, bounded HttpOnly sessions, explicit auth modes, backend authorization auditing, and console login/session-expiry handling with two-Tenant tests.
