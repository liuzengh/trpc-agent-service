-- Control-plane progress is independent of the selected Session backend.
-- Nanoseconds avoid rounding an event boundary forward and skipping an event.
CREATE TABLE background_watermark (
    tenant_id VARCHAR(64) NOT NULL,
    app_id VARCHAR(64) NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    job_type VARCHAR(64) NOT NULL,
    watermark_ns BIGINT NOT NULL CHECK (watermark_ns >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,app_id,user_id,session_id,job_type),
    FOREIGN KEY (tenant_id,app_id) REFERENCES agent_app(tenant_id,app_id)
);
