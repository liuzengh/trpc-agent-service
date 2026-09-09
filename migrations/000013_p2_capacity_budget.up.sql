-- WS-8: durable capacity budget with atomic counter per (tenant_id, scope).
-- Complements the 000012 capacity_reservation table: the budget row is the
-- authoritative active_count; the reservation rows are the audit trail.
-- Acquire/Release use single-row conditional UPDATE for atomic budget management.
CREATE TABLE capacity_budget (
    tenant_id text NOT NULL,
    scope text NOT NULL CHECK (scope IN ('ingress','vector_rebuild','reconciliation','worker','sender')),
    budget_limit bigint NOT NULL CHECK (budget_limit >= 0),
    active_count bigint NOT NULL DEFAULT 0 CHECK (active_count >= 0),
    config_version bigint NOT NULL DEFAULT 1,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, scope),
    CHECK (active_count <= budget_limit)
);
ALTER TABLE capacity_budget ENABLE ROW LEVEL SECURITY;
ALTER TABLE capacity_budget FORCE ROW LEVEL SECURITY;
CREATE POLICY capacity_budget_tenant_isolation ON capacity_budget
    USING (tenant_id = current_setting('trpc.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('trpc.tenant_id', true));
-- Global scope row (tenant_id = '__global__') is accessible to the runtime
-- role through the normal RLS path because it is also tenant-scoped.
ALTER TABLE capacity_reservation ADD COLUMN IF NOT EXISTS budget_scope text NOT NULL DEFAULT 'ingress';
