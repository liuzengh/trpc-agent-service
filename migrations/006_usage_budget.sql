BEGIN;

-- The budget ledger is shared by every service replica that points at the
-- Queue PostgreSQL database. Monetary values are fixed-point 1e-8 USD units;
-- zero means unlimited for compatibility with the tenant configuration.
CREATE TABLE IF NOT EXISTS budget_periods (
    tenant_id text NOT NULL,
    billing_period date NOT NULL CHECK (EXTRACT(DAY FROM billing_period) = 1),
    budget_limit_units bigint NOT NULL CHECK (budget_limit_units >= 0),
    reserved_units bigint NOT NULL DEFAULT 0 CHECK (reserved_units >= 0),
    settled_units bigint NOT NULL DEFAULT 0 CHECK (settled_units >= 0),
    unknown_units bigint NOT NULL DEFAULT 0 CHECK (unknown_units >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, billing_period)
);

CREATE TABLE IF NOT EXISTS model_usage_calls (
    tenant_id text NOT NULL,
    call_id text NOT NULL,
    app_namespace text NOT NULL DEFAULT '',
    session_id text NOT NULL DEFAULT '',
    dedup_key text NOT NULL DEFAULT '',
    request_id text NOT NULL DEFAULT '',
    run_id text NOT NULL,
    call_no integer NOT NULL CHECK (call_no > 0),
    model_name text NOT NULL DEFAULT '',
    billing_period date NOT NULL,
    estimated_units bigint NOT NULL CHECK (estimated_units >= 0),
    input_price_per_million_units bigint NOT NULL DEFAULT 0 CHECK (input_price_per_million_units >= 0),
    output_price_per_million_units bigint NOT NULL DEFAULT 0 CHECK (output_price_per_million_units >= 0),
    actual_units bigint NOT NULL DEFAULT 0 CHECK (actual_units >= 0),
    estimated_prompt_tokens bigint NOT NULL DEFAULT 0 CHECK (estimated_prompt_tokens >= 0),
    estimated_completion_tokens bigint NOT NULL DEFAULT 0 CHECK (estimated_completion_tokens >= 0),
    prompt_tokens bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
    completion_tokens bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
    state text NOT NULL CHECK (state IN ('reserved', 'settled', 'unknown', 'released')),
    unknown_reason text NOT NULL DEFAULT '',
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settled_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, call_id),
    UNIQUE (call_id),
    UNIQUE (tenant_id, run_id, call_no),
    FOREIGN KEY (tenant_id, billing_period)
        REFERENCES budget_periods (tenant_id, billing_period)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS model_usage_calls_replay_idx
    ON model_usage_calls (tenant_id, dedup_key, started_at DESC);
CREATE INDEX IF NOT EXISTS model_usage_calls_unknown_idx
    ON model_usage_calls (tenant_id, billing_period, updated_at DESC)
    WHERE state = 'unknown';

COMMIT;
