-- 0005_completion.sql — the completion batch (docs/spec-platform-completion.md):
-- the webchat mailbox marker and the rollout table.
--
-- Two changes, one file: both belong to the same batch of work, and neither
-- supersedes anything before it.

-- The webchat mailbox's "the browser got it" marker. Delivery marks a row
-- sent; the gateway's mailbox flips pushed_at only after the SSE write
-- succeeded, and re-pushes rows that are sent but not yet pushed when the
-- browser reconnects. KF/WeCom rows keep it NULL — their transports already
-- have their own acknowledgement.
ALTER TABLE reply_outbox
    ADD COLUMN pushed_at TIMESTAMP(6) NULL AFTER sent_at;
ALTER TABLE reply_outbox
    ADD KEY idx_reply_outbox_webchat (tenant_id, session_pk, status, pushed_at);

-- Session-sticky percentage rollouts of an app's published revision. The
-- assignment unit is the actor — one conversation stays on one revision for
-- its whole life — and basis_points is the share of actors routed to the
-- candidate, so 10000 means "everyone" (a whitelist is simply 10000).
CREATE TABLE IF NOT EXISTS rollouts (
    rollout_id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id    VARCHAR(64)     NOT NULL,
    app_id       BIGINT UNSIGNED NOT NULL,
    revision_id  BIGINT UNSIGNED NOT NULL COMMENT 'candidate revision; must belong to the same app',
    basis_points INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'share of actors on the candidate, 1..10000',
    status       ENUM('active', 'stopped') NOT NULL DEFAULT 'active',
    created_by   VARCHAR(128)    NOT NULL,
    created_at   TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    stopped_at   TIMESTAMP(6)    NULL,
    stopped_by   VARCHAR(128)    NOT NULL DEFAULT '',
    PRIMARY KEY (rollout_id),
    KEY idx_rollouts_active (tenant_id, app_id, status),
    CONSTRAINT fk_rollouts_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
