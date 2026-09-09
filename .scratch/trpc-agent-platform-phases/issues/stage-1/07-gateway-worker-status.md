# 07: Gateway And Worker Status

**What to build:** Operators can inspect the basic runtime state of Gateway and Worker from the Management Console using backend-reported status rather than frontend simulation. The view distinguishes healthy, unavailable, closing, and error conditions and reflects the same admission and shutdown behavior used by the routing loop.

**Blocked by:** None (can start immediately; the required backend runtime status behavior is already present)

**Status:** resolved

- [x] The management API reports stable Gateway and Worker identities, role, availability, lifecycle state, and a bounded non-sensitive execution summary.
- [x] Status data comes from the running backend components and does not expose secrets, request input, or cross-Tenant business data.
- [x] Backend authorization defines and enforces which roles may inspect platform-wide and Tenant-scoped runtime status.
- [x] The Management Console type contract and visible status treatment distinguish Gateway and Worker `healthy`, `unavailable`, `closing`, and `error` lifecycle states, as well as empty, forbidden, and service-request-error states.
- [x] Entering service shutdown changes admission and reported lifecycle consistently and prevents new routed work.
- [x] An unavailable Worker is represented as a stable status and routing error rather than a panic or indefinite request.
- [x] API and component tests cover status transitions, authorization, redaction, Worker `error` lifecycle rendering, and service-request-error handling.
- [x] Playwright verifies the status workflow and checks desktop and mobile layouts for readable, non-overlapping content.

## Answer

Implemented backend-reported Gateway and Worker identity, role, availability, lifecycle, and bounded execution counters with operator authorization and no request inputs. Unavailable and closing states share the routing admission source of truth. The Management Console renders all async and runtime states; component and desktop/mobile Playwright checks pass with recorded screenshots and overflow assertions.

## Comments

- Reopened after Stage 1 code review. The backend already emits the Worker `error` lifecycle, but the frontend TypeScript union, status styling, and component coverage omitted it.
- This frontend contract repair can run in parallel with Ticket 04 and does not depend on the routing rework.
- Resolved by adding `error` to the typed lifecycle contract, a distinct error status treatment, and strongly typed component coverage.
