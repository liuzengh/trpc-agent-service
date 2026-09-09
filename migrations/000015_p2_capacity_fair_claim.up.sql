-- WS-8 fair scheduling + durable worker budget for the durable queue claim.
--
-- 1. Fair interleave: claim order prefers the tenant whose in-flight work is
--    oldest (never-claimed tenants first), so a deep backlog from one tenant
--    cannot starve the others. Within a tenant, FIFO order is preserved.
-- 2. Worker budget: tenants with an enabled capacity_budget row at scope
--    'worker' may only claim while active_count < budget_limit. The slot is
--    accounted atomically inside this function (+1 on claim), released by
--    the terminal paths (Ack / permanent Nack) and by the expired-delivery
--    recovery below. Tenants without a budget row are unlimited (fail-open):
--    enforcement is active only where budgets are seeded.
--
-- The function signature, ownership and privilege surface are unchanged;
-- recovery audits (owner, PUBLIC revocation, EXECUTE grants) hold.
CREATE OR REPLACE FUNCTION trpc_queue_claim_next(
    p_visibility_micros bigint,
    p_delivery_id text
) RETURNS TABLE (
    tenant_id text,
    job_id text,
    execution_id text,
    schema_version integer,
    payload jsonb,
    attempt integer,
    leased_until timestamptz,
    received_at timestamptz
) LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $fn$
DECLARE
    candidate record;
    claim_leased_until timestamptz;
    claim_received_at timestamptz;
    budget_limited boolean;
BEGIN
    -- Recover expired deliveries; release one worker-budget slot per
    -- recovered job (the slot was taken at claim time).
    WITH expired AS (
        UPDATE job_queue
        SET status = 'queued', delivery_id = NULL, leased_until = NULL,
            available_at = clock_timestamp(), attempt = job_queue.attempt + 1,
            updated_at = clock_timestamp()
        WHERE job_queue.status = 'in_flight' AND job_queue.leased_until <= clock_timestamp()
        RETURNING job_queue.tenant_id
    )
    UPDATE capacity_budget b
    SET active_count = GREATEST(b.active_count - e.cnt, 0), updated_at = clock_timestamp()
    FROM (SELECT expired.tenant_id, count(*) AS cnt FROM expired GROUP BY expired.tenant_id) e
    WHERE b.tenant_id = e.tenant_id AND b.scope = 'worker' AND b.enabled;

    LOOP
        -- Fair candidate selection: prefer the tenant whose most recent
        -- in-flight claim is oldest (NULLS FIRST admits starved tenants),
        -- then per-tenant FIFO. Exclude tenants whose worker budget is
        -- exhausted; tenants without a budget row are never excluded.
        SELECT j.tenant_id, j.job_id, j.execution_id, j.schema_version, j.payload, j.attempt
        INTO candidate
        FROM job_queue j
        WHERE j.status = 'queued' AND j.available_at <= clock_timestamp()
          AND NOT EXISTS (
                SELECT 1 FROM capacity_budget b
                WHERE b.tenant_id = j.tenant_id AND b.scope = 'worker'
                  AND b.enabled AND b.active_count >= b.budget_limit
          )
        ORDER BY
            (SELECT MAX(k.updated_at) FROM job_queue k
              WHERE k.tenant_id = j.tenant_id AND k.status = 'in_flight') ASC NULLS FIRST,
            j.available_at, j.created_at, j.job_id
        FOR UPDATE SKIP LOCKED
        LIMIT 1;
        IF NOT FOUND THEN
            RETURN;
        END IF;

        -- Worker-budget accounting: only for tenants that have an enabled
        -- budget row. The conditional increment is the same atomic pattern
        -- as capacity.Acquire. A raced-full row (concurrent claim took the
        -- last slot between the exclusion check and this increment) skips
        -- this candidate and retries the loop.
        SELECT EXISTS (
            SELECT 1 FROM capacity_budget cb
            WHERE cb.tenant_id = candidate.tenant_id AND cb.scope = 'worker' AND cb.enabled
        ) INTO budget_limited;
        IF budget_limited THEN
            UPDATE capacity_budget cb
            SET active_count = cb.active_count + 1, updated_at = clock_timestamp()
            WHERE cb.tenant_id = candidate.tenant_id AND cb.scope = 'worker'
              AND cb.enabled AND cb.active_count < cb.budget_limit;
            IF NOT FOUND THEN
                CONTINUE;
            END IF;
        END IF;

        UPDATE job_queue j
        SET status = 'in_flight', delivery_id = p_delivery_id,
            leased_until = clock_timestamp() + (p_visibility_micros::double precision * interval '1 microsecond'),
            delivery_count = j.delivery_count + 1, updated_at = clock_timestamp()
        WHERE j.tenant_id = candidate.tenant_id AND j.job_id = candidate.job_id
          AND j.status = 'queued' AND j.available_at <= clock_timestamp()
        RETURNING j.leased_until, clock_timestamp()
        INTO claim_leased_until, claim_received_at;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'claim candidate disappeared' USING ERRCODE = 'P0002';
        END IF;

        RETURN QUERY
        SELECT candidate.tenant_id, candidate.job_id, candidate.execution_id,
               candidate.schema_version, candidate.payload, candidate.attempt,
               claim_leased_until, claim_received_at;
        -- One claim per call: RETURN QUERY appends to the result set but
        -- does not exit; the loop must terminate after a successful claim
        -- or the next iteration would reuse this call's delivery_id.
        RETURN;
    END LOOP;
END
$fn$;

ALTER FUNCTION trpc_queue_claim_next(bigint, text) OWNER TO trpc_claim_owner;
REVOKE ALL ON FUNCTION trpc_queue_claim_next(bigint, text) FROM PUBLIC;
-- The SECURITY DEFINER body (running as trpc_claim_owner) reads and updates
-- the worker-scope budget; bind the grants to the claim owner explicitly.
GRANT SELECT, UPDATE ON TABLE capacity_budget TO trpc_claim_owner;
DO $mig$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime') THEN
        GRANT EXECUTE ON FUNCTION trpc_queue_claim_next(bigint, text) TO trpc_runtime;
        GRANT SELECT, UPDATE ON TABLE capacity_budget TO trpc_runtime;
    END IF;
END
$mig$;
