# 07: Tenant Budgets And Execution Cost Tracking

**What to build:** Tenant administrators can set token and monetary budgets, and operators can see current consumption and remaining allowance. Agent executions are checked before starting and their model and Tool usage is attributed to the correct Tenant after completion.

**Blocked by:** 03: Tenant Governance Policies And Tool/MCP Allowlists.

**Status:** resolved

- [x] Tenant policy supports bounded token and monetary budgets over a documented accounting period, with server-side validation and authorized management operations.
- [x] A pre-execution reservation prevents concurrent requests from overspending the same Tenant budget; rejected requests never invoke Agent or Tool code.
- [x] Completion, failure, cancellation, and reservation expiry reconcile usage exactly once using request identity and do not create negative or duplicate balances.
- [x] Model token usage, configured model pricing, and Tool cost are attributed to Tenant, Agent App, Session, request, and trace when available.
- [x] Missing usage metadata and unknown pricing have an explicit conservative behavior and cannot silently bypass an enforced budget.
- [x] Budget allow, threshold, and deny decisions produce stable API/SSE/IM outcomes and Audit Events with safe cost fields.
- [x] The Management Console provides budget configuration and consumption/remaining views, including period state, validation, role enforcement, and two-Tenant tests.

## Answer

Added validated token/cost policy fields, atomic pre-execution reservations, request-idempotent reconciliation, conservative token estimation, Tenant cost accounting, stable budget denials, and console configuration/consumption views.
