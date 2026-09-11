-- 0004_knowledge.sql — documents, chunks, memory and artifacts (approved
-- plan, P4: "知识、文件和显式 Memory").
--
-- The shape here follows one rule the plan states for every object in this
-- slice: SQL is the authority, Qdrant and MinIO are rebuildable or replaceable
-- stores. A retrieval decides *what is allowed* in SQL (tenant, app, kb
-- binding, status, generation) and only then uses the vector store as an
-- index; a chunk's text that reaches a citation is read from SQL, never from
-- a vector payload. That is what makes "未 ready 不召回" and "跨租户零泄露"
-- checkable facts instead of configuration.

-- The embedding identity lives on the knowledge base, not in code: a
-- re-embed with a different model or dimension is a property of what was
-- stored, and the reconciliation job needs it to know which rows are stale.
ALTER TABLE knowledge_bases
    ADD COLUMN embedding_model VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pinned at upload time; empty means platform default',
    ADD COLUMN embedding_dim   INT UNSIGNED NOT NULL DEFAULT 0,
    ADD COLUMN chunker_version INT UNSIGNED NOT NULL DEFAULT 1,
    -- A composite foreign key needs a composite unique key on the parent;
    -- documents pins both halves so a row can never point across tenants.
    ADD UNIQUE KEY uk_knowledge_bases_tenant_kb (tenant_id, kb_id);

-- One row per uploaded object. status is the staged lifecycle the plan
-- names: uploaded (bytes in MinIO, nothing indexed) → indexing → ready
-- (every chunk confirmed, generation CAS held) → deleting → deleted, with
-- failed as the terminal "a human should look at this".
CREATE TABLE IF NOT EXISTS documents (
    doc_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id       VARCHAR(64)     NOT NULL,
    app_id          BIGINT UNSIGNED NOT NULL,
    kb_id           BIGINT UNSIGNED NOT NULL,
    public_id       CHAR(36)        NOT NULL COMMENT 'caller-visible id, stable across a re-index',
    -- The generation is the deletion and re-index epoch: a delete bumps it
    -- *before* any cleanup, so an index job that is still in flight writes
    -- under an old generation and must compensate instead of resurrecting.
    generation      INT UNSIGNED    NOT NULL DEFAULT 1,
    title           VARCHAR(255)    NOT NULL DEFAULT '',
    mime            VARCHAR(128)    NOT NULL DEFAULT '',
    size_bytes      BIGINT UNSIGNED NOT NULL DEFAULT 0,
    content_sha256  CHAR(64)        NOT NULL DEFAULT '',
    object_key      VARCHAR(255)    NOT NULL DEFAULT '' COMMENT 'MinIO key; the bytes are never kept in SQL',
    status          ENUM('uploaded', 'indexing', 'ready', 'failed', 'deleting', 'deleted') NOT NULL DEFAULT 'uploaded',
    chunk_count     INT UNSIGNED    NOT NULL DEFAULT 0,
    indexed_count   INT UNSIGNED    NOT NULL DEFAULT 0,
    embedding_model VARCHAR(128)    NOT NULL DEFAULT '',
    embedding_dim   INT UNSIGNED    NOT NULL DEFAULT 0,
    chunker_version INT UNSIGNED    NOT NULL DEFAULT 1,
    error           VARCHAR(512)    NOT NULL DEFAULT '',
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (doc_id),
    UNIQUE KEY uk_documents_public (tenant_id, app_id, kb_id, public_id),
    UNIQUE KEY uk_documents_tenant_doc (tenant_id, doc_id),
    KEY idx_documents_status (tenant_id, status, updated_at),
    CONSTRAINT fk_documents_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    CONSTRAINT fk_documents_app FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps (tenant_id, app_id) ON DELETE RESTRICT,
    CONSTRAINT fk_documents_kb FOREIGN KEY (tenant_id, kb_id) REFERENCES knowledge_bases (tenant_id, kb_id) ON DELETE RESTRICT
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Chunk metadata in SQL: the citation text comes from here, so a vector
-- payload that has been tampered with (or is simply a stale generation)
-- cannot put words into a user's answer.
CREATE TABLE IF NOT EXISTS document_chunks (
    chunk_row       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id       VARCHAR(64)     NOT NULL,
    doc_id          BIGINT UNSIGNED NOT NULL,
    generation      INT UNSIGNED    NOT NULL,
    chunk_ord       INT UNSIGNED    NOT NULL COMMENT '0-based position in the document',
    page            INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'source page where the format has one; derived otherwise',
    text            MEDIUMTEXT      NOT NULL,
    embedding_version INT UNSIGNED  NOT NULL DEFAULT 1,
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (chunk_row),
    UNIQUE KEY uk_chunks_locator (tenant_id, doc_id, generation, chunk_ord),
    KEY idx_chunks_doc (tenant_id, doc_id, generation),
    CONSTRAINT fk_chunks_doc FOREIGN KEY (tenant_id, doc_id) REFERENCES documents (tenant_id, doc_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Memory is explicit-only (the plan refuses automatic extraction): a row
-- exists because a user or tool said "remember this". Qdrant indexes it;
-- deleting the row tombstones the index entry the same way documents do.
CREATE TABLE IF NOT EXISTS memory_entries (
    memory_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id       VARCHAR(64)     NOT NULL,
    user_key        VARCHAR(255)    NOT NULL COMMENT 'the session actor_key: memory is per user, never per group',
    text            MEDIUMTEXT      NOT NULL,
    status          ENUM('pending', 'ready', 'failed', 'deleting', 'deleted') NOT NULL DEFAULT 'pending',
    embedding_model VARCHAR(128)    NOT NULL DEFAULT '',
    embedding_dim   INT UNSIGNED    NOT NULL DEFAULT 0,
    error           VARCHAR(512)    NOT NULL DEFAULT '',
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (memory_id),
    KEY idx_memory_scope (tenant_id, user_key, status),
    CONSTRAINT fk_memory_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Artifacts: files a tool produced during an execution. staged → ready
-- happens in the execution's own commit transaction, which is what makes
-- "a user can only download what was committed" true without a second
-- state machine.
CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id       VARCHAR(64)     NOT NULL,
    execution_id    CHAR(36)        NOT NULL,
    session_pk      BIGINT UNSIGNED NOT NULL,
    public_id       CHAR(36)        NOT NULL,
    name            VARCHAR(255)    NOT NULL DEFAULT '',
    mime            VARCHAR(128)    NOT NULL DEFAULT 'application/octet-stream',
    size_bytes      BIGINT UNSIGNED NOT NULL DEFAULT 0,
    content_sha256  CHAR(64)        NOT NULL DEFAULT '',
    object_key      VARCHAR(255)    NOT NULL,
    status          ENUM('staged', 'ready', 'deleting', 'deleted', 'failed') NOT NULL DEFAULT 'staged',
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (artifact_id),
    UNIQUE KEY uk_artifacts_public (tenant_id, public_id),
    KEY idx_artifacts_execution (tenant_id, execution_id, status),
    KEY idx_artifacts_session (tenant_id, session_pk, updated_at),
    CONSTRAINT fk_artifacts_execution FOREIGN KEY (execution_id) REFERENCES executions (execution_id) ON DELETE CASCADE,
    CONSTRAINT fk_artifacts_session FOREIGN KEY (tenant_id, session_pk) REFERENCES sessions (tenant_id, session_pk) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
