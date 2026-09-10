-- Issue #98: durable media reply descriptors with deterministic text fallback.
SET LOCAL search_path = pg_catalog, public, pg_temp;

ALTER TABLE public.reply_outbox
    ADD COLUMN reply_kind TEXT NOT NULL DEFAULT 'text'
        CHECK (reply_kind IN ('text', 'image', 'video', 'audio', 'document')),
    ADD COLUMN attachment_id TEXT NOT NULL DEFAULT ''
        CHECK (length(attachment_id) <= 256),
    ADD COLUMN attachment_kind TEXT NOT NULL DEFAULT ''
        CHECK (attachment_kind IN ('', 'image', 'video', 'audio', 'document')),
    ADD COLUMN attachment_mime_type TEXT NOT NULL DEFAULT ''
        CHECK (length(attachment_mime_type) <= 256),
    ADD COLUMN attachment_name TEXT NOT NULL DEFAULT ''
        CHECK (length(attachment_name) <= 512),
    ADD COLUMN attachment_size BIGINT NOT NULL DEFAULT 0
        CHECK (attachment_size >= 0 AND attachment_size <= 67108864),
    ADD COLUMN attachment_sha256 TEXT NOT NULL DEFAULT ''
        CHECK (attachment_sha256 = '' OR attachment_sha256 ~ '^[0-9a-f]{64}$'),
    ADD COLUMN attachment_provider TEXT NOT NULL DEFAULT ''
        CHECK (length(attachment_provider) <= 64),
    ADD COLUMN attachment_provider_id TEXT NOT NULL DEFAULT ''
        CHECK (length(attachment_provider_id) <= 512),
    ADD COLUMN fallback TEXT NOT NULL DEFAULT '',
    ADD CONSTRAINT reply_outbox_media_contract CHECK (
        (
            reply_kind = 'text'
            AND attachment_id = ''
            AND attachment_kind = ''
            AND attachment_mime_type = ''
            AND attachment_name = ''
            AND attachment_size = 0
            AND attachment_sha256 = ''
            AND attachment_provider = ''
            AND attachment_provider_id = ''
            AND fallback = ''
        )
        OR (
            reply_kind <> 'text'
            AND reply_kind = attachment_kind
            AND btrim(attachment_id) <> ''
            AND btrim(attachment_mime_type) <> ''
            AND attachment_size > 0
            AND attachment_sha256 ~ '^[0-9a-f]{64}$'
            AND btrim(fallback) <> ''
        )
    );
