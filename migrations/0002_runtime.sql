-- 0002_runtime.sql — second batch, P2: reliable Inbox -> Worker -> Outbox.
--
-- This migration turns the platform's message path from "ACK first, run in a
-- goroutine, hope the process survives" into a durable queue with leases and
-- fencing. Sessions, not just configuration, become MySQL-authoritative
-- (approved plan, "MySQL 权威，Redis 缓存"), which is what lets the final
-- commit write events, execution outcome, audit, and the reply to send in one
-- transaction.
--
-- Two conventions carried over from 0001_control.sql:
--   * tenant_id leads every uniqueness and lookup index on tenant-owned data;
--   * a foreign key into a tenant-owned table carries tenant_id in its
--     referenced key, so "row A points at row B of a different tenant" is
--     unrepresentable rather than merely discouraged.

SET NAMES utf8mb4;

-- A session is the durable conversation a series of messages appends to. The
-- lease and fencing columns are not decoration: they are the mechanism that
-- keeps two workers from writing the same conversation at once, and that
-- rejects a worker whose lease expired while it was still running.
CREATE TABLE IF NOT EXISTS sessions (
    session_pk                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id                 VARCHAR(64)     NOT NULL,
    -- Identity of the conversation, before generation. Two people in one
    -- WeChat-KF scene, or the same person in two groups, are different
    -- sessions; the pieces that say which are stored separately rather than
    -- concatenated so a change in one (a group id format, say) does not
    -- silently become a new session.
    app_id                    BIGINT UNSIGNED NOT NULL,
    channel_type              ENUM('webchat', 'wecom', 'wechat_kf') NOT NULL,
    binding_id                BIGINT UNSIGNED NULL,
    actor_key                 VARCHAR(255)    NOT NULL COMMENT 'single-chat platform user id, or the group id for a group chat',
    is_group                  TINYINT(1)      NOT NULL DEFAULT 0,
    generation                INT UNSIGNED    NOT NULL DEFAULT 1,

    -- The version fixity the approved plan makes a first-class promise about:
    -- a session keeps the revision and profile versions it was created with,
    -- regardless of what the app publishes later.
    revision_id               BIGINT UNSIGNED NOT NULL,
    model_profile_version     INT UNSIGNED    NOT NULL,
    backend_profile_version   INT UNSIGNED    NOT NULL,

    -- Ordered delivery bookkeeping, kept here rather than derived: in_seq is
    -- assigned at acceptance (so ordering is a property of what was received,
    -- not of what a worker happened to claim first), head_seq is the next one
    -- a worker may execute, and a retry or unknown never advances it.
    in_seq                    INT UNSIGNED    NOT NULL DEFAULT 0,
    head_seq                  INT UNSIGNED    NOT NULL DEFAULT 1,

    -- Authoritative conversation state, and the version the fencing token is
    -- checked against. Redis may cache a projection of this; it never owns it.
    state                     JSON            NULL,
    session_version           BIGINT UNSIGNED NOT NULL DEFAULT 0,
    -- Summary is stored next to the session it covers (approved plan, 表分组)
    -- with the sequence it covered, so a stale summary cannot overwrite a
    -- newer one and cannot hide events after its cutoff.
    summary                   MEDIUMTEXT      NULL,
    summary_covered_seq       INT UNSIGNED    NOT NULL DEFAULT 0,
    summary_version           INT UNSIGNED    NOT NULL DEFAULT 0,

    -- Lease and fencing for "who may write this session right now". Token
    -- only ever increases, so a holder that lost the lease can be refused by
    -- number rather than by comparing timestamps it may itself have drifted.
    lease_owner               VARCHAR(128)    NULL,
    lease_until               TIMESTAMP(6)    NULL,
    fencing_token             BIGINT UNSIGNED NOT NULL DEFAULT 0,

    -- 'unknown' or a failed commit parks the reason here; the session stops
    -- accepting new work until a human resolves it, because a queued message
    -- that would run before a resolved side effect would reorder history.
    blocked_reason            VARCHAR(255)    NULL,

    created_at                TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at                TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (session_pk),
    -- Same reason agent_apps grew one: every table below that stores session
    -- history references (tenant_id, session_pk), and a foreign key needs the
    -- referenced columns to be unique together, not just the id alone.
    UNIQUE KEY uk_sessions_tenant_pk (tenant_id, session_pk),
    UNIQUE KEY uk_sessions_identity_generation (tenant_id, app_id, channel_type, actor_key, generation),
    KEY idx_sessions_claimable (head_seq, lease_until),
    CONSTRAINT fk_sessions_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    CONSTRAINT fk_sessions_app FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps (tenant_id, app_id) ON DELETE RESTRICT,
    CONSTRAINT fk_sessions_revision FOREIGN KEY (tenant_id, revision_id) REFERENCES agent_revisions (tenant_id, revision_id) ON DELETE RESTRICT,
    CONSTRAINT fk_sessions_binding FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_bindings (tenant_id, binding_id) ON DELETE RESTRICT
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- The durable event log. session_version is what a prepared commit compares
-- against, so this is the "base version" the approved plan's commit protocol
-- checks; seq is the position within the session, which is what ordering and
-- summary cutoffs are measured in.
CREATE TABLE IF NOT EXISTS session_events (
    tenant_id       VARCHAR(64)     NOT NULL,
    session_pk      BIGINT UNSIGNED NOT NULL,
    seq             INT UNSIGNED    NOT NULL,
    event_id        VARCHAR(64)     NOT NULL COMMENT 'the id the framework gave the event; preserved, never regenerated',
    execution_id    CHAR(36)        NOT NULL,
    author          VARCHAR(255)    NOT NULL DEFAULT '',
    payload         JSON            NOT NULL,
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, session_pk, seq),
    UNIQUE KEY uk_session_events_event (tenant_id, session_pk, event_id),
    KEY idx_session_events_execution (tenant_id, execution_id),
    CONSTRAINT fk_session_events_session FOREIGN KEY (tenant_id, session_pk) REFERENCES sessions (tenant_id, session_pk) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- An inbox row is the durable fact that "this message was accepted". The HTTP
-- ACK means this insert committed; nothing downstream may be inferred from
-- Redis' dedup hint, and nothing here is lost if the process dies afterwards.
CREATE TABLE IF NOT EXISTS inbox_messages (
    tenant_id           VARCHAR(64)     NOT NULL,
    binding_id          BIGINT UNSIGNED NOT NULL,
    platform_message_id VARCHAR(255)    NOT NULL,
    -- Same id, different bytes is a caller bug or an attack, not a duplicate:
    -- the hash is what lets the platform tell those two apart.
    content_hash        CHAR(64)        NOT NULL,
    session_pk          BIGINT UNSIGNED NOT NULL,
    in_seq              INT UNSIGNED    NOT NULL,
    execution_id        CHAR(36)        NOT NULL,
    traceparent         VARCHAR(255)    NOT NULL DEFAULT '' COMMENT 'W3C trace context captured at acceptance, so a retried run keeps the same trace id',
    text                MEDIUMTEXT      NULL,
    status              ENUM('pending', 'running', 'done', 'failed', 'unknown', 'superseded') NOT NULL DEFAULT 'pending',
    received_at         TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, binding_id, platform_message_id),
    UNIQUE KEY uk_inbox_session_seq (tenant_id, session_pk, in_seq),
    KEY idx_inbox_execution (tenant_id, execution_id),
    CONSTRAINT fk_inbox_session FOREIGN KEY (tenant_id, session_pk) REFERENCES sessions (tenant_id, session_pk) ON DELETE CASCADE,
    CONSTRAINT fk_inbox_binding FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_bindings (tenant_id, binding_id) ON DELETE RESTRICT
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- The logical execution: one per inbox message, stable across worker attempts.
CREATE TABLE IF NOT EXISTS executions (
    execution_id        CHAR(36)        NOT NULL,
    tenant_id           VARCHAR(64)     NOT NULL,
    session_pk          BIGINT UNSIGNED NOT NULL,
    in_seq              INT UNSIGNED    NOT NULL,
    revision_id         BIGINT UNSIGNED NOT NULL,
    model_profile_id    BIGINT UNSIGNED NOT NULL,
    backend_profile_id  BIGINT UNSIGNED NOT NULL,
    traceparent         VARCHAR(255)    NOT NULL DEFAULT '',
    status              ENUM('pending', 'running', 'committed', 'failed', 'unknown') NOT NULL DEFAULT 'pending',
    attempts            INT UNSIGNED    NOT NULL DEFAULT 0,
    last_error          VARCHAR(512)    NOT NULL DEFAULT '',
    created_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (execution_id),
    UNIQUE KEY uk_executions_session_seq (tenant_id, session_pk, in_seq),
    CONSTRAINT fk_executions_inbox FOREIGN KEY (tenant_id, session_pk, in_seq)
        REFERENCES inbox_messages (tenant_id, session_pk, in_seq) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Each claim gets its own attempt row. A prepared write set is stored here,
-- never in session_events: nothing in this table is authoritative until the
-- final commit copies it out, which is what makes "the commit failed halfway"
-- a recoverable state instead of a corrupted one.
CREATE TABLE IF NOT EXISTS execution_attempts (
    attempt_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    execution_id        CHAR(36)        NOT NULL,
    worker_id           VARCHAR(128)    NOT NULL,
    fencing_token       BIGINT UNSIGNED NOT NULL,
    status              ENUM('running', 'prepared', 'committed', 'abandoned', 'blocked') NOT NULL DEFAULT 'running',
    prepared_at         TIMESTAMP(6)    NULL,
    prepared            JSON            NULL COMMENT 'serialized sessionstore.Prepared: events, state, summary intents',
    prepared_hash       CHAR(64)        NOT NULL DEFAULT '',
    base_session_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
    error               VARCHAR(512)    NOT NULL DEFAULT '',
    created_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (attempt_id),
    UNIQUE KEY uk_attempt_fencing (execution_id, fencing_token),
    KEY idx_attempts_worker (worker_id, status),
    CONSTRAINT fk_attempts_execution FOREIGN KEY (execution_id) REFERENCES executions (execution_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- The reply the delivery role is responsible for sending. Split into parts so
-- a partially delivered reply retries only the missing part, not the whole
-- message (which would double-send what already arrived).
CREATE TABLE IF NOT EXISTS reply_outbox (
    outbox_id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id           VARCHAR(64)     NOT NULL,
    execution_id        CHAR(36)        NOT NULL,
    session_pk          BIGINT UNSIGNED NOT NULL,
    part_seq            INT UNSIGNED    NOT NULL,
    channel_type        ENUM('webchat', 'wecom', 'wechat_kf') NOT NULL,
    binding_id          BIGINT UNSIGNED NULL,
    target              VARCHAR(255)    NOT NULL COMMENT 'userid / external_userid / open_kfid+external pair, channel-specific',
    text                MEDIUMTEXT      NOT NULL,
    is_done             TINYINT(1)      NOT NULL DEFAULT 0,
    idempotency_key     VARCHAR(128)    NOT NULL DEFAULT '',
    status              ENUM('pending', 'sending', 'sent', 'unknown', 'dead') NOT NULL DEFAULT 'pending',
    attempts            INT UNSIGNED    NOT NULL DEFAULT 0,
    max_attempts        INT UNSIGNED    NOT NULL DEFAULT 5,
    next_attempt_at     TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_error          VARCHAR(512)    NOT NULL DEFAULT '',
    -- Delivery has its own lease and fence, independent of the execution's:
    -- a reply is sent by a different role than the one that computed it.
    lease_owner         VARCHAR(128)    NULL,
    lease_until         TIMESTAMP(6)    NULL,
    delivery_fencing    BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    sent_at             TIMESTAMP(6)    NULL,
    PRIMARY KEY (outbox_id),
    UNIQUE KEY uk_reply_outbox_part (tenant_id, execution_id, part_seq),
    KEY idx_reply_outbox_claimable (status, next_attempt_at, lease_until),
    CONSTRAINT fk_reply_outbox_execution FOREIGN KEY (execution_id) REFERENCES executions (execution_id) ON DELETE CASCADE,
    CONSTRAINT fk_reply_outbox_session FOREIGN KEY (tenant_id, session_pk) REFERENCES sessions (tenant_id, session_pk) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- One row per actual send attempt, including the ones that did not get an
-- answer. That is the evidence a human needs to resolve an 'unknown'.
CREATE TABLE IF NOT EXISTS delivery_attempts (
    attempt_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    outbox_id           BIGINT UNSIGNED NOT NULL,
    attempt_no          INT UNSIGNED    NOT NULL,
    worker_id           VARCHAR(128)    NOT NULL,
    fencing_token       BIGINT UNSIGNED NOT NULL,
    started_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    finished_at         TIMESTAMP(6)    NULL,
    result              ENUM('sending', 'sent', 'rejected', 'unknown') NOT NULL DEFAULT 'sending',
    detail              VARCHAR(512)    NOT NULL DEFAULT '',
    PRIMARY KEY (attempt_id),
    UNIQUE KEY uk_delivery_attempt_no (outbox_id, attempt_no),
    CONSTRAINT fk_delivery_attempts_outbox FOREIGN KEY (outbox_id) REFERENCES reply_outbox (outbox_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- WeChat customer service (and any other pull-based channel) announces
-- "there is news"; the notification is what gets ACKed durably, before the
-- actual messages are fetched. The old code advanced a cursor and then handed
-- messages to an in-process goroutine, which loses work on a crash.
CREATE TABLE IF NOT EXISTS channel_notifications (
    notification_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id           VARCHAR(64)     NOT NULL,
    binding_id          BIGINT UNSIGNED NOT NULL,
    -- The channel's own event token, when it has one (WeChat KF sends a
    -- short-lived token that is what authorizes the following pull).
    event_token         VARCHAR(255)    NOT NULL DEFAULT '',
    scope_key           VARCHAR(255)    NOT NULL COMMENT 'e.g. open_kfid: the pull unit this notification is about',
    payload             JSON            NULL,
    status              ENUM('pending', 'running', 'done', 'failed') NOT NULL DEFAULT 'pending',
    received_at         TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    lease_owner         VARCHAR(128)    NULL,
    lease_until         TIMESTAMP(6)    NULL,
    fencing_token       BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (notification_id),
    KEY idx_notifications_claimable (status, received_at),
    CONSTRAINT fk_notifications_binding FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_bindings (tenant_id, binding_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- A cursor for a pull-based channel, versioned so two pullers cannot both
-- advance from the same checkpoint and one silently overwrite the other.
CREATE TABLE IF NOT EXISTS channel_checkpoints (
    tenant_id           VARCHAR(64)     NOT NULL,
    binding_id          BIGINT UNSIGNED NOT NULL,
    scope_key           VARCHAR(255)    NOT NULL,
    value               VARCHAR(512)    NOT NULL DEFAULT '',
    version             BIGINT UNSIGNED NOT NULL DEFAULT 0,
    lease_owner         VARCHAR(128)    NULL,
    lease_until         TIMESTAMP(6)    NULL,
    updated_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, binding_id, scope_key),
    CONSTRAINT fk_checkpoints_binding FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_bindings (tenant_id, binding_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- A durable reply route for a customer-service session, so an open_kfid
-- captured when the message arrived is still available to the delivery role
-- (possibly in a different process) when the reply is sent. The previous
-- adapter kept this only in process memory.
CREATE TABLE IF NOT EXISTS channel_reply_routes (
    tenant_id           VARCHAR(64)     NOT NULL,
    session_pk          BIGINT UNSIGNED NOT NULL,
    binding_id          BIGINT UNSIGNED NOT NULL,
    scope_key           VARCHAR(255)    NOT NULL,
    updated_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, session_pk),
    CONSTRAINT fk_reply_routes_session FOREIGN KEY (tenant_id, session_pk) REFERENCES sessions (tenant_id, session_pk) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- The general background queue the approved plan's "MySQL outbox" is: index
-- jobs, summary generation, cache projection, orphan cleanup, and any later
-- job kind, all with the same lease/retry/unknown shape.
CREATE TABLE IF NOT EXISTS outbox_events (
    job_id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id           VARCHAR(64)     NOT NULL,
    kind                VARCHAR(64)     NOT NULL,
    -- An idempotency key lets a job be safely re-enqueued (a retried commit
    -- or a crash before the outbox row was visible is not a reason to do a
    -- side effect twice) and lets a consumer deduplicate without a lock.
    idempotency_key     VARCHAR(191)    NOT NULL,
    payload             JSON            NOT NULL,
    status              ENUM('pending', 'running', 'done', 'failed', 'unknown') NOT NULL DEFAULT 'pending',
    attempts            INT UNSIGNED    NOT NULL DEFAULT 0,
    max_attempts        INT UNSIGNED    NOT NULL DEFAULT 5,
    next_attempt_at     TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    lease_owner         VARCHAR(128)    NULL,
    lease_until         TIMESTAMP(6)    NULL,
    fencing_token       BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_error          VARCHAR(512)    NOT NULL DEFAULT '',
    created_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (job_id),
    UNIQUE KEY uk_outbox_events_idem (kind, idempotency_key),
    KEY idx_outbox_events_claimable (status, next_attempt_at),
    KEY idx_outbox_events_tenant (tenant_id, kind, status)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- The queryable audit the approved plan asks for ("按 tenant、session、tool、
-- decision、trace 查询"), written in the same transaction as the commit it
-- describes. The existing JSONL trail is kept: this table cannot replace a
-- file an operator tails, and a file cannot replace a row a query can find.
CREATE TABLE IF NOT EXISTS audit_events (
    audit_id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    ts                  TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    trace_id            CHAR(32)        NOT NULL DEFAULT '',
    event               VARCHAR(64)     NOT NULL,
    tenant_id           VARCHAR(64)     NOT NULL DEFAULT '',
    channel             VARCHAR(64)     NOT NULL DEFAULT '',
    user_id             VARCHAR(255)    NOT NULL DEFAULT '',
    session_id          VARCHAR(255)    NOT NULL DEFAULT '',
    execution_id        CHAR(36)        NOT NULL DEFAULT '',
    agent_name          VARCHAR(255)    NOT NULL DEFAULT '',
    tool_name           VARCHAR(64)     NOT NULL DEFAULT '',
    decision            VARCHAR(32)     NOT NULL DEFAULT '',
    stage               VARCHAR(32)     NOT NULL DEFAULT '',
    rule                VARCHAR(64)     NOT NULL DEFAULT '',
    latency_ms          BIGINT          NOT NULL DEFAULT 0,
    error_type          VARCHAR(64)     NOT NULL DEFAULT '',
    detail              MEDIUMTEXT      NULL,
    prompt_tokens       INT             NOT NULL DEFAULT 0,
    completion_tokens   INT             NOT NULL DEFAULT 0,
    -- Secrets are never stored; a reference is, so an audit row can say which
    -- credential was in play without holding it.
    secret_ref          VARCHAR(255)    NOT NULL DEFAULT '',
    PRIMARY KEY (audit_id),
    KEY idx_audit_trace (trace_id),
    KEY idx_audit_tenant_session (tenant_id, session_id, ts),
    KEY idx_audit_decision (tenant_id, decision, ts),
    KEY idx_audit_execution (execution_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
