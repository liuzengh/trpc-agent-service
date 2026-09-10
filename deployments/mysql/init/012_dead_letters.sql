-- =============================================================================
-- 012_dead_letters.sql — Inbound dead-letter queue (persistent record)
--
-- Written by the bus dead-letter policy when an inbound message exceeds the
-- retry threshold (default 5 business failures): the Redis copy lives on
-- stream:inbound:dlq for operational inspection, this table is the durable
-- record and the replay source for the admin API (GET /dlq, POST /dlq/{id}/replay).
-- Rows are kept forever (audit trail); replay only stamps replayed_at.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dead_letters (
    id           BIGINT       NOT NULL AUTO_INCREMENT COMMENT 'physical row id (replay handle)',
    message_id   VARCHAR(128) NOT NULL                COMMENT 'bus envelope id, the idempotency key',
    stream_entry VARCHAR(64)  NOT NULL                COMMENT 'redis stream entry id of the failed delivery',
    tenant_id    VARCHAR(36)  NOT NULL DEFAULT ''     COMMENT 'tenant isolation key',
    agent_id     VARCHAR(64)  NOT NULL DEFAULT '',
    session_id   VARCHAR(128) NOT NULL DEFAULT '',
    channel      VARCHAR(32)  NOT NULL DEFAULT ''     COMMENT 'wecom / feishu / admin',
    user_id      VARCHAR(64)  NOT NULL DEFAULT '',
    trace_id     VARCHAR(64)  NOT NULL DEFAULT ''     COMMENT 'joins the jaeger trace of the failed attempts',
    payload      MEDIUMTEXT   NOT NULL                COMMENT 'JSON-encoded original bus envelope, the replay bytes',
    fail_reason  VARCHAR(512) NOT NULL DEFAULT ''     COMMENT 'last business failure cause',
    attempts     INT          NOT NULL DEFAULT 0      COMMENT 'business failures observed before dead-lettering',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    replayed_at  DATETIME     NULL                    COMMENT 'set when an operator replayed the message',
    PRIMARY KEY (id),
    KEY idx_dlq_created (created_at),
    KEY idx_dlq_tenant (tenant_id, created_at),
    KEY idx_dlq_message (message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='dead-lettered inbound messages; replay source for the admin API';
