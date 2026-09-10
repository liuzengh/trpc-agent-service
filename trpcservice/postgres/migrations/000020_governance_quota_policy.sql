ALTER TABLE platform.tenant
    ADD COLUMN IF NOT EXISTS quota_policy JSONB NOT NULL DEFAULT '{}'::JSONB
        CHECK (jsonb_typeof(quota_policy) = 'object');

ALTER TABLE platform.audit_event
    ADD COLUMN IF NOT EXISTS policy_rule_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS policy_reason TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS platform.quota_usage (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL DEFAULT '',
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('TENANT', 'APP')),
    period_kind TEXT NOT NULL CHECK (period_kind IN ('DAY', 'MONTH')),
    period_start DATE NOT NULL,
    token_used BIGINT NOT NULL DEFAULT 0 CHECK (token_used >= 0),
    cost_used DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (cost_used >= 0),
    token_reserved BIGINT NOT NULL DEFAULT 0 CHECK (token_reserved >= 0),
    cost_reserved DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (cost_reserved >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, scope_kind, period_kind, period_start),
    CHECK ((scope_kind = 'TENANT' AND app_id = '') OR
           (scope_kind = 'APP' AND app_id <> '')),
    FOREIGN KEY (tenant_id) REFERENCES platform.tenant (tenant_id)
);

CREATE TABLE IF NOT EXISTS platform.quota_reservation (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL DEFAULT '',
    request_id TEXT NOT NULL,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('TENANT', 'APP')),
    period_kind TEXT NOT NULL CHECK (period_kind IN ('DAY', 'MONTH')),
    period_start DATE NOT NULL,
    reserved_tokens BIGINT NOT NULL DEFAULT 0 CHECK (reserved_tokens >= 0),
    reserved_cost DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (reserved_cost >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, request_id, scope_kind, period_kind, period_start),
    FOREIGN KEY (tenant_id) REFERENCES platform.tenant (tenant_id)
);

CREATE TABLE IF NOT EXISTS platform.quota_usage_record (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    input_tokens BIGINT NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens BIGINT NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    total_tokens BIGINT NOT NULL DEFAULT 0 CHECK (total_tokens >= 0),
    cost DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (cost >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, request_id, attempt),
    FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);

CREATE INDEX IF NOT EXISTS quota_usage_scope_idx
    ON platform.quota_usage (tenant_id, app_id, scope_kind, period_kind, period_start);
