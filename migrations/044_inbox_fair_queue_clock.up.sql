-- The dispatch clock advances with active service, independently of idle tenants.
CREATE TABLE IF NOT EXISTS inbox_fair_queue_clock (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    virtual_time BIGINT NOT NULL DEFAULT 0 CHECK (virtual_time >= 0)
);

-- Establish a common admission baseline for existing schedules once at rollout.
INSERT INTO inbox_fair_queue_clock (singleton, virtual_time)
SELECT TRUE, COALESCE(MAX(virtual_runtime), 0)
FROM tenant_queue_schedule
ON CONFLICT (singleton) DO NOTHING;
