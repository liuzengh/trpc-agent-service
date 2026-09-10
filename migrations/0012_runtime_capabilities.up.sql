-- Issue #75: tenant-scoped Memory, Summary, Knowledge, Artifact, Audit,
-- vector-index and object-storage metadata.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_memory (
    tenant_id  TEXT NOT NULL,
    memory_id  TEXT NOT NULL CHECK (length(btrim(memory_id)) BETWEEN 1 AND 256),
    user_id    TEXT NOT NULL CHECK (length(btrim(user_id)) BETWEEN 1 AND 256),
    session_id TEXT NOT NULL DEFAULT '' CHECK (length(session_id) <= 256),
    content    TEXT NOT NULL CHECK (length(btrim(content)) > 0),
    topics     JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(topics) = 'array'),
    metadata   JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    embedding  JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(embedding) = 'array'),
    version    BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, memory_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);
CREATE INDEX runtime_memory_user_idx ON public.runtime_memory (tenant_id, user_id, updated_at DESC) WHERE deleted_at IS NULL;

CREATE TABLE public.runtime_summary (
    tenant_id  TEXT NOT NULL,
    session_id TEXT NOT NULL,
    filter_key TEXT NOT NULL DEFAULT '' CHECK (length(filter_key) <= 256),
    text       TEXT NOT NULL CHECK (length(btrim(text)) > 0),
    event_seq  BIGINT NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    version    BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id, filter_key),
    FOREIGN KEY (tenant_id, session_id) REFERENCES public.runtime_session(tenant_id, session_id) ON DELETE CASCADE
);

CREATE TABLE public.runtime_knowledge (
    tenant_id   TEXT NOT NULL,
    document_id TEXT NOT NULL CHECK (length(btrim(document_id)) BETWEEN 1 AND 256),
    content     TEXT NOT NULL CHECK (length(btrim(content)) > 0),
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    embedding   JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(embedding) = 'array'),
    digest      TEXT NOT NULL CHECK (length(digest) <= 128),
    version     BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, document_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE public.runtime_artifact (
    tenant_id  TEXT NOT NULL,
    artifact_id TEXT NOT NULL CHECK (length(btrim(artifact_id)) BETWEEN 1 AND 256),
    session_id TEXT NOT NULL DEFAULT '' CHECK (length(session_id) <= 256),
    name       TEXT NOT NULL DEFAULT '' CHECK (length(name) <= 512),
    mime_type  TEXT NOT NULL DEFAULT '' CHECK (length(mime_type) <= 256),
    content    BYTEA NOT NULL,
    version    BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, artifact_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);
CREATE INDEX runtime_artifact_session_idx ON public.runtime_artifact (tenant_id, session_id, artifact_id);

CREATE TABLE public.runtime_audit_log (
    tenant_id  TEXT NOT NULL,
    audit_id   TEXT NOT NULL CHECK (length(btrim(audit_id)) BETWEEN 1 AND 256),
    event_type TEXT NOT NULL CHECK (length(btrim(event_type)) BETWEEN 1 AND 128),
    payload    JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, audit_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);
CREATE INDEX runtime_audit_time_idx ON public.runtime_audit_log (tenant_id, occurred_at, audit_id);

CREATE TABLE public.runtime_vector_index (
    tenant_id   TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'generic' CHECK (length(btrim(source)) BETWEEN 1 AND 128),
    document_id TEXT NOT NULL CHECK (length(btrim(document_id)) BETWEEN 1 AND 256),
    content     TEXT NOT NULL DEFAULT '',
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    embedding   JSONB NOT NULL CHECK (jsonb_typeof(embedding) = 'array'),
    version     BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source, document_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE public.runtime_object (
    tenant_id    TEXT NOT NULL,
    object_key   TEXT NOT NULL CHECK (length(btrim(object_key)) BETWEEN 1 AND 1024),
    content_type TEXT NOT NULL DEFAULT '' CHECK (length(content_type) <= 256),
    content      BYTEA NOT NULL,
    size         BIGINT NOT NULL CHECK (size >= 0),
    etag         TEXT NOT NULL CHECK (length(etag) <= 128),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, object_key),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

REVOKE ALL ON TABLE public.runtime_memory, public.runtime_summary, public.runtime_knowledge, public.runtime_artifact, public.runtime_audit_log, public.runtime_vector_index, public.runtime_object FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_memory, public.runtime_summary, public.runtime_knowledge, public.runtime_artifact, public.runtime_audit_log, public.runtime_vector_index, public.runtime_object TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_memory, public.runtime_summary, public.runtime_knowledge, public.runtime_artifact, public.runtime_audit_log, public.runtime_vector_index, public.runtime_object TO migration_owner;
