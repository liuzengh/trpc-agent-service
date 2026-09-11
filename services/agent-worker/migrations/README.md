
`0021_tenant_usage_governance.sql` adds the compact V1 tenant reservation and
settlement ledger. All Worker replicas share it. Missing Provider usage creates
an explicit unknown settlement and continues to hold its reservation.

`0022_tool_approvals.sql` adds the persisted dangerous-tool approval state
machine. It binds a decision to the exact tenant, execution identity, tool,
target, and argument digest while preventing duplicate execution claims.
