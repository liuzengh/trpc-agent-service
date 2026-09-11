-- =============================================================================
-- 008_channels_outbox.sql — IM channel bindings & reliable delivery
--
-- credential_ref is a secret-store reference (token/secret never plaintext).
-- outbox_events is the MySQL-authoritative reliable-delivery log consumed by the
-- dispatcher (Redis Streams is the transport; the outbox is the source of truth
-- for at-least-once + retry). A publish failure is retried on an exponential
-- schedule derived from (retries, created_at) until the retry budget is spent,
-- then the event is parked as 'dead' with last_error for inspection:
--   UPDATE outbox_events SET status='pending', retries=0 WHERE event_id='...';
-- idempotency_keys dedupes IM message re-delivery.
--
-- Existing volumes (init scripts only run on a fresh volume) need:
--   ALTER TABLE outbox_events
--     MODIFY status ENUM('pending','sent','dead') NOT NULL DEFAULT 'pending',
--     ADD COLUMN last_error VARCHAR(255) NOT NULL DEFAULT '';
-- Without it the dispatcher keeps working (pending -> sent, retries +1); only
-- the park step degrades, logging the failure instead of recording it.
-- =============================================================================

CREATE TABLE IF NOT EXISTS channel_bindings (
    binding_id     VARCHAR(36)  NOT NULL,
    tenant_id      VARCHAR(36)  NOT NULL,
    agent_id       VARCHAR(36)  NOT NULL,
    channel        VARCHAR(32)  NOT NULL             COMMENT 'wecom | feishu',
    account_id     VARCHAR(128) NOT NULL             COMMENT 'enterprise/app identity',
    credential_ref VARCHAR(128) NOT NULL             COMMENT 'secret-store ref for token/secret',
    verification_token_ref VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'feishu verify-token secret-store ref',
    created_by     VARCHAR(64)  NULL                 COMMENT 'authoring member id (weak ref); NULL = pre-authorship row, treated as tenant-shared',
    visibility     ENUM('private','shared') NOT NULL DEFAULT 'private'
                                                     COMMENT 'private = author + tenant managers only; shared = tenant-readable (others read-only)',
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
    status     ENUM('pending','sent','dead') NOT NULL DEFAULT 'pending'
                            COMMENT 'dead = retry budget spent (or unpublishable payload); kept for inspection',
    retries    INT         NOT NULL DEFAULT 0  COMMENT 'publish attempts so far; drives the backoff schedule',
    last_error VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'why the event was parked as dead',
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
