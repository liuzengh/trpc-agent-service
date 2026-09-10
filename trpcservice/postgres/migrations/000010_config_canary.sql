ALTER TABLE platform.agent_app
    ADD COLUMN canary_config_version TEXT,
    ADD COLUMN canary_percentage INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN canary_status TEXT NOT NULL DEFAULT 'DISABLED';

ALTER TABLE platform.agent_app
    ADD CONSTRAINT agent_app_canary_percentage_ck
        CHECK (canary_percentage >= 0 AND canary_percentage <= 100),
    ADD CONSTRAINT agent_app_canary_status_ck
        CHECK (canary_status IN ('DISABLED', 'ENABLED', 'PAUSED')),
    ADD CONSTRAINT agent_app_canary_state_ck
        CHECK (
            (canary_status = 'DISABLED'
                AND canary_config_version IS NULL
                AND canary_percentage = 0)
            OR
            (canary_status IN ('ENABLED', 'PAUSED')
                AND canary_config_version IS NOT NULL
                AND canary_config_version <> ''
                AND canary_percentage > 0)
        ),
    ADD CONSTRAINT agent_app_canary_config_fk
        FOREIGN KEY (tenant_id, app_id, canary_config_version)
        REFERENCES platform.app_config_version (tenant_id, app_id, version)
        DEFERRABLE INITIALLY DEFERRED;
