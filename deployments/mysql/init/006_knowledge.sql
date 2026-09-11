-- =============================================================================
-- 006_knowledge.sql — Knowledge bases & documents (Milvus + MinIO backed)
--
-- Knowledge is tenant-level shared infrastructure: multiple agents may reference
-- the same knowledge_base (via agent_versions.runtime_profile.kb_ids).
-- Chunks and vectors live in Milvus; raw files live in MinIO; this table holds
-- metadata only. collection_name maps to the Milvus collection.
-- =============================================================================

CREATE TABLE IF NOT EXISTS knowledge_bases (
    kb_id           VARCHAR(36)  NOT NULL,
    tenant_id       VARCHAR(36)  NOT NULL,
    name            VARCHAR(128) NOT NULL,
    embedding_model VARCHAR(128) NOT NULL,
    vector_store    VARCHAR(32)  NOT NULL DEFAULT 'milvus',
    collection_name VARCHAR(128) NULL                COMMENT 'Milvus collection; default = {tenant_id}_{kb_id}',
    dimension       INT          NOT NULL DEFAULT 1536 COMMENT 'embedding dimension; must match the Milvus collection',
    created_by      VARCHAR(64)  NULL                COMMENT 'authoring member id (weak ref); NULL = pre-authorship row, treated as tenant-shared',
    visibility      ENUM('private','shared') NOT NULL DEFAULT 'private'
                                                     COMMENT 'private = author + tenant managers only; shared = tenant-readable (others read-only)',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted      TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (kb_id),
    KEY idx_kb_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='knowledge base metadata; vectors in Milvus, raw files in MinIO';

CREATE TABLE IF NOT EXISTS knowledge_documents (
    doc_id      VARCHAR(36)  NOT NULL,
    kb_id       VARCHAR(36)  NOT NULL,
    title       VARCHAR(256) NULL,
    source_uri  VARCHAR(512) NULL                COMMENT 'MinIO object path of the raw file',
    chunk_count INT          NOT NULL DEFAULT 0,
    status      ENUM('ingesting','ready','failed') NOT NULL DEFAULT 'ingesting',
    error       VARCHAR(512) NULL,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (doc_id),
    KEY idx_doc_kb (kb_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='knowledge document ingestion state';
