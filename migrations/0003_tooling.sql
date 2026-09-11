-- 0003_tooling.sql — the tool execution ledger (approved plan, P3:
-- "Tool 执行账本与人工核对").
--
-- Why a ledger at all: a tool call is the one place this platform can cause an
-- effect outside its own database (an HTTP write), and a crash between "we
-- sent it" and "we recorded the answer" is exactly the window where a retry
-- would run the effect twice. The ledger's job is to make that window
-- visible: the intent row is written *before* the call in its own short
-- transaction, the outcome after it, and a row left in 'running' is the
-- platform saying "this may have happened" instead of guessing. The approved
-- plan's recovery rule reads straight off this table: a stale write call
-- blocks the execution for a human; a stale read call is safe to re-run.

-- One row per *logical* call attempt-window: call_id is derived from
-- (execution_id, fencing_token, call_seq), so a new claim of the same
-- execution writes fresh rows instead of colliding with the dead attempt's
-- history — the old rows are precisely what the recovery check inspects.
CREATE TABLE IF NOT EXISTS tool_calls (
    call_id         VARCHAR(160)    NOT NULL,
    tenant_id       VARCHAR(64)     NOT NULL,
    execution_id    CHAR(36)        NOT NULL,
    session_pk      BIGINT UNSIGNED NOT NULL,
    call_seq        INT UNSIGNED    NOT NULL COMMENT '1-based position of this call within its execution attempt',
    tool_id         BIGINT UNSIGNED NULL COMMENT 'tool_bindings row this call was pinned to; NULL for platform builtins',
    tool_name       VARCHAR(64)     NOT NULL,
    tool_version    INT UNSIGNED    NOT NULL DEFAULT 0,
    tool_kind       ENUM('go', 'http') NOT NULL DEFAULT 'go',
    -- Denormalised from the binding on purpose: the recovery check decides
    -- "block or re-run" from this table alone, and a revoked binding must
    -- not erase the fact of what the dead attempt was allowed to do.
    side_effect     ENUM('none', 'read', 'write') NOT NULL DEFAULT 'none',
    idempotent      TINYINT(1)      NOT NULL DEFAULT 0,
    arguments_hash  CHAR(64)        NOT NULL COMMENT 'sha256 of the canonical JSON arguments',
    arguments_masked JSON           NULL COMMENT 'arguments with secret-looking values blanked; what an operator reviews',
    status          ENUM('running', 'succeeded', 'failed', 'rejected', 'unknown') NOT NULL DEFAULT 'running',
    error_type      VARCHAR(64)     NOT NULL DEFAULT '',
    detail          VARCHAR(512)    NOT NULL DEFAULT '',
    result_bytes    INT UNSIGNED    NOT NULL DEFAULT 0,
    latency_ms      INT UNSIGNED    NOT NULL DEFAULT 0,
    trace_id        CHAR(32)        NOT NULL DEFAULT '',
    worker_id       VARCHAR(128)    NOT NULL DEFAULT '',
    fencing_token   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    attempts        INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'physical attempts, retries included',
    -- Human disposition of an unknown/stale row. resolution IS NULL means
    -- "still unresolved", which is what keeps a session blocked and what the
    -- recovery check refuses to re-run past.
    resolution      ENUM('confirmed', 'cancelled') NULL COMMENT 'confirmed: the side effect happened; cancelled: it did not',
    resolved_by     VARCHAR(128)    NULL,
    resolved_at     TIMESTAMP(6)    NULL,
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (call_id),
    KEY idx_tool_calls_execution (tenant_id, execution_id),
    KEY idx_tool_calls_unresolved (tenant_id, status, resolution),
    KEY idx_tool_calls_session (tenant_id, session_pk, updated_at),
    CONSTRAINT fk_tool_calls_execution FOREIGN KEY (execution_id)
        REFERENCES executions (execution_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Every physical attempt behind a logical call — the HTTP retry the governor
-- allowed, or the single call it did not. Kept separate so the retry budget
-- is auditable ("安全重试计数受限"): the count of rows here is the count of
-- times the network was actually touched.
CREATE TABLE IF NOT EXISTS tool_call_attempts (
    attempt_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    call_id         VARCHAR(160)    NOT NULL,
    tenant_id       VARCHAR(64)     NOT NULL,
    attempt_no      INT UNSIGNED    NOT NULL,
    worker_id       VARCHAR(128)    NOT NULL DEFAULT '',
    status          ENUM('running', 'succeeded', 'failed', 'unknown') NOT NULL DEFAULT 'running',
    http_status     INT             NOT NULL DEFAULT 0,
    error_type      VARCHAR(64)     NOT NULL DEFAULT '',
    detail          VARCHAR(512)    NOT NULL DEFAULT '',
    started_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    finished_at     TIMESTAMP(6)    NULL,
    PRIMARY KEY (attempt_id),
    UNIQUE KEY uk_tool_call_attempt (call_id, attempt_no),
    CONSTRAINT fk_tool_call_attempts_call FOREIGN KEY (call_id)
        REFERENCES tool_calls (call_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
