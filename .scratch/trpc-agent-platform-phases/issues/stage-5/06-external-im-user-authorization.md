# 06: External IM User Authorization

**What to build:** Tenant administrators can control which external users or conversations may invoke each Agent App through Enterprise WeChat and Telegram. Unauthorized provider messages stop before Session creation or Runner execution and leave a safe, inspectable decision.

**Blocked by:** 03: Tenant Governance Policies And Tool/MCP Allowlists.

**Status:** resolved

- [x] Tenant policy expresses allow or deny rules for Provider Account, External IM Subject, external user, conversation type, and Agent App without placing trust in provider payload Tenant fields.
- [x] Enterprise WeChat, Telegram, and deterministic provider replay evaluate the same authorization boundary after Bot Tenant Allowlist routing and before Session creation or Runner execution.
- [x] Allowed messages preserve existing duplicate, ordering, Session mapping, request correlation, and provider reply behavior.
- [x] Denied, unmapped, ambiguous, disabled, and policy-unavailable outcomes remain distinct stable Delivery Status values and do not invoke the Runner.
- [x] Authorization decisions emit tenant-scoped Audit Events without storing message bodies, credentials, or unrelated routing entries.
- [x] Policy management and decision inspection APIs enforce role and Tenant boundaries, including platform-bot routes shared across multiple Tenants.
- [x] Management Console and protocol tests cover allowed and denied users for both real providers, duplicate denied delivery, and cross-Tenant attempts.

## Answer

Applied the same governance authorization boundary to Telegram, Enterprise WeChat, and provider replay after server-owned routing and before Session/Runner work, with stable rejected delivery codes, auditing, and tenant isolation.
