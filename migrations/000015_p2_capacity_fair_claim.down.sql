-- Restore the 000010 claim function (FIFO, budget-unaware) and drop the
-- capacity_budget grants bound for the fair-claim accounting.
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
BEGIN
    UPDATE job_queue
    SET status = 'queued', delivery_id = NULL, leased_until = NULL,
        available_at = clock_timestamp(), attempt = job_queue.attempt + 1,
        updated_at = clock_timestamp()
    WHERE job_queue.status = 'in_flight' AND job_queue.leased_until <= clock_timestamp();

    SELECT j.tenant_id, j.job_id, j.execution_id, j.schema_version, j.payload, j.attempt
    INTO candidate
    FROM job_queue j
    WHERE j.status = 'queued' AND j.available_at <= clock_timestamp()
    ORDER BY j.available_at, j.created_at, j.job_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1;
    IF NOT FOUND THEN
        RETURN;
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
END
$fn$;

ALTER FUNCTION trpc_queue_claim_next(bigint, text) OWNER TO trpc_claim_owner;
REVOKE ALL ON FUNCTION trpc_queue_claim_next(bigint, text) FROM PUBLIC;
REVOKE SELECT, UPDATE ON TABLE capacity_budget FROM trpc_claim_owner;
DO $mig$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime') THEN
        GRANT EXECUTE ON FUNCTION trpc_queue_claim_next(bigint, text) TO trpc_runtime;
        REVOKE SELECT, UPDATE ON TABLE capacity_budget FROM trpc_runtime;
    END IF;
END
$mig$;
