# 01: Channel Adapter And Binding Baseline

**What to build:** Establish the standard Channel Adapter and Channel Binding contracts for trusted inbound/outbound conversion. A Mock IM message can be received, converted to the platform message model, mapped to the correct tenant-scoped chat Session, routed through the existing execution path, and answered through the same binding, including basic user mapping and single/group Session rules.

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] Channel Adapter, Channel Binding, inbound conversion, outbound reply, trusted credential/signature, user mapping, and single/group Session rules are represented by stable contracts.
- [x] Duplicate and out-of-order inbound messages are handled deterministically without duplicate execution or cross-Session corruption.
- [x] The same apparent channel user or external identifier cannot access another Tenant's Agent App or Session.
- [x] The Mock IM happy path proves callback-to-reply behavior with automated tests and without real-provider credentials.
- [x] The contracts preserve the Stage 2 storage, trusted Tenant Context, backend routing, and cancellation boundaries.

## Answer

Implemented the tenant-scoped Channel Adapter and Channel Binding contracts, HMAC callback verification, single/group conversation mapping, deterministic Session IDs, Mock IM callback-to-reply execution, duplicate suppression, and cross-Tenant binding rejection. Chat execution now carries `request_id` through Gateway and Runner while preserving the Stage 2 Session Event and backend lease boundaries.
