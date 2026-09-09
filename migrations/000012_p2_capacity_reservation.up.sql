-- P2-03/WS-8: cross-process durable capacity reservation.
-- Enables the production capacity admission boundary (complementing the
-- P2-03 process-local admission gate). Each active request holds one
-- reservation row; the reservation is created and released in the same
-- transaction as the Queue enqueue/Ack to ensure atomic capacity management.
CREATE TABLE capacity_reservation (
    reservation_id text NOT NULL,
    tenant_id text NOT NULL,
    scope text NOT NULL CHECK (scope IN ('ingress','vector_rebuild','reconciliation')),
    owner_id text NOT NULL,
    config_version bigint NOT NULL DEFAULT 1,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','released','expired')),
    created_at timestamptz NOT NULL DEFAULT now(),
    released_at timestamptz,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, reservation_id)
);
CREATE INDEX ix_capacity_reservation_state ON capacity_reservation (tenant_id, state) WHERE state = 'active';
ALTER TABLE capacity_reservation ENABLE ROW LEVEL SECURITY;
ALTER TABLE capacity_reservation FORCE ROW LEVEL SECURITY;
CREATE POLICY capacity_reservation_tenant_isolation ON capacity_reservation
    USING (tenant_id = current_setting('trpc.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('trpc.tenant_id', true));
