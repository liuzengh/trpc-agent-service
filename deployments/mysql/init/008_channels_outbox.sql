-- =============================================================================
-- 008_channels_outbox.sql — IM channel bindings & reliable delivery
--
-- credential_ref is a secret-store reference (token/secret never plaintext).
-- outbox_events is the MySQL-authoritative reliable-delivery log consumed by the
-- dispatcher (Redis Streams is the transport; the outbox is the source of truth
-- for at-least-once + retry). idempotency_keys dedupes IM message re-delivery.
-- =============================================================================

CREATE TABLE IF NOT EXISTS channel_bindings (
    binding_id     VARCHAR(36)  NOT NULL,
    tenant_id      VARCHAR(36)  NOT NULL,
    agent_id       VARCHAR(36)  NOT NULL,
    channel        VARCHAR(32)  NOT NULL             COMMENT 'wecom | feishu',
    account_id     VARCHAR(128) NOT NULL             COMMENT 'enterprise/app identity',
    credential_ref VARCHAR(128) NOT NULL             COMMENT 'secret-store ref for token/secret',
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_deleted     TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (binding_id),
    UNIQUE KEY uk_channel_account (channel, account_id),
    KEY idx_binding_tenant_agent (tenant_id, agent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='IM account <-> tenant/agent binding (webhook + token + verify secret)';

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id   VARCHAR(36) NOT NULL,
    tenant_id  VARCHAR(36) NOT NULL,
    topic      VARCHAR(64) NOT NULL,
    payload    JSON        NOT NULL,
    status     ENUM('pending','sent') NOT NULL DEFAULT 'pending',
    retries    INT         NOT NULL DEFAULT 0,
    created_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id),
    KEY idx_outbox_status (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='reliable-delivery outbox: MySQL is authoritative, dispatcher drains to Redis Streams';

CREATE TABLE IF NOT EXISTS idempotency_keys (
    msg_key    VARCHAR(128) NOT NULL                COMMENT 'hash(channel + platform message id)',
    tenant_id  VARCHAR(36)  NOT NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (msg_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='IM message dedup: at-least-once delivery idempotency';
