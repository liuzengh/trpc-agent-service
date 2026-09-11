-- Additive upgrade from the deployed Channel V1 baseline. Diagnostic tasks do
-- not reference Binding/Deployment/Run or change operational credential grants.
CREATE TABLE channel_preflights (
    id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES channel_account_catalog(scope_id),
    tenant_id text NOT NULL,
    account_id text NOT NULL,
    requested_by text NOT NULL REFERENCES user_accounts(id),
    state text NOT NULL CHECK (state IN ('QUEUED','RUNNING','COMPLETED','TIMED_OUT','STALE')),
    requested_at timestamptz NOT NULL,
    job_deadline_at timestamptz NOT NULL,
    lease_expires_at timestamptz,
    last_checked_at timestamptz NOT NULL,
    record_jsonb jsonb NOT NULL,
    FOREIGN KEY (tenant_id,account_id) REFERENCES channel_accounts(tenant_id,id),
    UNIQUE (scope_id,tenant_id,account_id,id),
    CHECK (job_deadline_at = requested_at + interval '120 seconds'),
    CHECK (last_checked_at >= requested_at),
    CHECK (lease_expires_at IS NULL OR lease_expires_at > requested_at),
    CHECK ((state='QUEUED' AND lease_expires_at IS NULL) OR
           (state='RUNNING' AND lease_expires_at IS NOT NULL) OR
           state IN ('COMPLETED','TIMED_OUT','STALE')),
    CHECK (jsonb_typeof(record_jsonb)='object' AND octet_length(record_jsonb::text)<=65536),
    CHECK (record_jsonb ?& ARRAY['scope_id','source_epoch','view','credential_id','lease_epoch','lease_expires_at','last_checked_at']),
    CHECK (jsonb_typeof(record_jsonb->'view')='object'),
    CHECK ((record_jsonb->'view') ?& ARRAY['preflight_id','tenant_id','account_id','requested_by','state','requested_at','job_deadline_at','checks','checked_at','expires_at']),
    CHECK (record_jsonb->>'scope_id'=scope_id),
    CHECK (record_jsonb->'view'->>'preflight_id'=id),
    CHECK (record_jsonb->'view'->>'tenant_id'=tenant_id),
    CHECK (record_jsonb->'view'->>'account_id'=account_id),
    CHECK (record_jsonb->'view'->>'requested_by'=requested_by),
    CHECK (record_jsonb->'view'->>'state'=state),
    CHECK ((record_jsonb->'view'->>'requested_at')::timestamptz=requested_at),
    CHECK ((record_jsonb->'view'->>'job_deadline_at')::timestamptz=job_deadline_at),
    CHECK ((record_jsonb->>'last_checked_at')::timestamptz=last_checked_at),
    CHECK (((record_jsonb->>'lease_expires_at')::timestamptz) IS NOT DISTINCT FROM lease_expires_at),
    CHECK ((record_jsonb->>'lease_epoch')::bigint BETWEEN 0 AND 2),
    CHECK ((state='COMPLETED' AND jsonb_typeof(record_jsonb->'view'->'checks')='array' AND jsonb_array_length(record_jsonb->'view'->'checks')=8 AND record_jsonb->'view'->>'checked_at' IS NOT NULL AND record_jsonb->'view'->>'expires_at' IS NOT NULL) OR
           (state<>'COMPLETED' AND record_jsonb->'view'->'checks'='[]'::jsonb AND record_jsonb->'view'->'checked_at'='null'::jsonb AND record_jsonb->'view'->'expires_at'='null'::jsonb))
);

CREATE UNIQUE INDEX channel_preflights_one_active_account
    ON channel_preflights(tenant_id,account_id)
    WHERE state IN ('QUEUED','RUNNING');
CREATE INDEX channel_preflights_candidates
    ON channel_preflights(scope_id,requested_at,id)
    WHERE state IN ('QUEUED','RUNNING');
CREATE INDEX channel_preflights_maintenance
    ON channel_preflights(scope_id,last_checked_at,id)
    WHERE state IN ('QUEUED','RUNNING');
CREATE INDEX channel_preflights_rate
    ON channel_preflights(scope_id,tenant_id,requested_at,account_id);
CREATE INDEX channel_preflights_cleanup
    ON channel_preflights(requested_at,id)
    WHERE state NOT IN ('QUEUED','RUNNING');

-- Request keys/digests and claim tokens are one-way hashes. A claim receipt may
-- represent an empty queue; it is then independent of any account or task row.
CREATE TABLE channel_preflight_requests (
    scope_id text NOT NULL REFERENCES channel_account_catalog(scope_id),
    kind text NOT NULL CHECK (kind IN ('create','claim')),
    key_hash text NOT NULL CHECK (key_hash ~ '^sha256:[0-9a-f]{64}$'),
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    tenant_id text,
    account_id text,
    preflight_id text,
    principal_id text NOT NULL DEFAULT '',
    instance_id text NOT NULL DEFAULT '',
    instance_epoch text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    record_jsonb jsonb NOT NULL,
    PRIMARY KEY (scope_id,kind,key_hash),
    FOREIGN KEY (scope_id,tenant_id,account_id,preflight_id)
        REFERENCES channel_preflights(scope_id,tenant_id,account_id,id),
    CHECK (expires_at > created_at),
    CHECK ((tenant_id IS NULL AND account_id IS NULL AND preflight_id IS NULL) OR
           (tenant_id IS NOT NULL AND account_id IS NOT NULL AND preflight_id IS NOT NULL)),
    CHECK (kind<>'create' OR preflight_id IS NOT NULL),
    CHECK (jsonb_typeof(record_jsonb)='object' AND octet_length(record_jsonb::text)<=65536)
);
CREATE INDEX channel_preflight_requests_cleanup
    ON channel_preflight_requests(expires_at,scope_id,kind,key_hash);
CREATE INDEX channel_preflight_requests_claim_rate
    ON channel_preflight_requests(scope_id,principal_id,instance_id,instance_epoch,created_at)
    WHERE kind='claim';
