# 05: Tenant Backend Routing And Cross-Node Consistency

**What to build:** Let authorized platform operators select a data backend per Tenant and route every Session, Event, Summary, and Memory operation through the trusted Tenant Context to that backend. The management API reports backend health and preserves the documented cross-node consistency and concurrency guarantees.

**Blocked by:** 03: Redis Shared Session And Memory Adapter; 04: SQLite Adapter And PostgreSQL Compatibility Profile

**Status:** resolved

- [x] Tenant backend selection is server-authorized, persisted through the supported control plane, and cannot be supplied by request input or browser storage.
- [x] Requests route to the selected InMemory, Redis, or SQL backend using trusted Tenant Context, with no cross-tenant data leakage.
- [x] Cross-node reads, concurrent writes, idempotency, and event ordering satisfy the frozen Stage 2 consistency contract.
- [x] Backend health exposes healthy, unavailable, and degraded/error states with bounded, non-sensitive details.
- [x] Integration tests cover at least two Tenants using different backends and prove routing and isolation.

## Answer

Implemented server-controlled backend catalogs, persisted tenant selection, trusted routing, non-leaking health, unavailable-backend behavior, runtime event persistence, resource closure, and authorization tests.
