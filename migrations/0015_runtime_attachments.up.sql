-- Issue #98: tenant-scoped attachment metadata and event ownership.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_attachment (
    tenant_id   TEXT NOT NULL,
    attachment_id TEXT NOT NULL CHECK (length(btrim(attachment_id)) BETWEEN 1 AND 256),
    kind        TEXT NOT NULL CHECK (kind IN ('image', 'video', 'audio', 'document')),
    mime_type   TEXT NOT NULL CHECK (length(btrim(mime_type)) BETWEEN 1 AND 256),
    name        TEXT NOT NULL DEFAULT '' CHECK (length(name) <= 512),
    size        BIGINT NOT NULL CHECK (size > 0 AND size <= 67108864),
    sha256      TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    provider    TEXT NOT NULL DEFAULT '' CHECK (length(provider) <= 64),
    provider_id TEXT NOT NULL DEFAULT '' CHECK (length(provider_id) <= 512),
    event_id    TEXT,
    expires_at  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, attachment_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id),
    FOREIGN KEY (tenant_id, attachment_id)
        REFERENCES public.runtime_object(tenant_id, object_key)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, event_id)
        REFERENCES public.message_event(tenant_id, event_id)
        ON DELETE SET NULL (event_id)
);

CREATE INDEX runtime_attachment_cleanup_idx
    ON public.runtime_attachment (tenant_id, expires_at)
    WHERE event_id IS NULL;

REVOKE ALL ON TABLE public.runtime_attachment FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_attachment TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_attachment TO migration_owner;
