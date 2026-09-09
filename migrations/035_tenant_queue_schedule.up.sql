-- Operator-owned fair scheduling state for the optional tenant-aware Inbox
-- claimer. Tenant configuration cannot mutate these values through ingress.
CREATE TABLE IF NOT EXISTS tenant_queue_schedule (
    tenant_id VARCHAR(64) PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    weight BIGINT NOT NULL DEFAULT 1 CHECK (weight > 0 AND weight <= 1000000),
    max_queued BIGINT NOT NULL DEFAULT 0 CHECK (max_queued >= 0),
    max_inflight BIGINT NOT NULL DEFAULT 0 CHECK (max_inflight >= 0),
    virtual_runtime BIGINT NOT NULL DEFAULT 0 CHECK (virtual_runtime >= 0),
    last_claimed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Seed every existing tenant once so fair claim does not need a queue-wide
-- DISTINCT scan on every transaction. New tenants are added lazily by the
-- atomic admission path before their first message is counted.
INSERT INTO tenant_queue_schedule (tenant_id)
SELECT id
FROM tenants
ON CONFLICT (tenant_id) DO NOTHING;

-- One index serves the fair claimer's per-tenant head lookup while keeping
-- terminal and reconciliation rows out of the candidate scan.
CREATE INDEX IF NOT EXISTS idx_inbox_fair_tenant_head
    ON inbox_messages (tenant_id, next_attempt_at, created_at, id)
    WHERE status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL');
