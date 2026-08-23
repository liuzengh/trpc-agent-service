ALTER TABLE message_dedup
ADD COLUMN epoch bigint NOT NULL DEFAULT 1 CHECK (epoch >= 1);

CREATE TABLE coordination_epoch (
    tenant_id text NOT NULL,
    resource_id text NOT NULL,
    epoch bigint NOT NULL DEFAULT 1 CHECK (epoch >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, resource_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id) ON DELETE CASCADE
);
CREATE INDEX ix_coordination_epoch_tenant ON coordination_epoch (
    tenant_id, resource_id
);

CREATE TABLE session_lease (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    owner_id text NOT NULL,
    epoch bigint NOT NULL DEFAULT 1 CHECK (epoch >= 1),
    fencing_token bigint NOT NULL CHECK (fencing_token >= 0),
    leased_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES session (
        tenant_id, session_id
    ) ON DELETE CASCADE
);
CREATE INDEX ix_session_lease_expiry ON session_lease (tenant_id, leased_until);
