-- Issue #48: tenant-scoped runtime Session, inbound event and reply outbox facts.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_session (
    tenant_id   TEXT NOT NULL,
    session_id  TEXT NOT NULL CHECK (length(btrim(session_id)) BETWEEN 1 AND 256),
    status      TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'closed')),
    version     BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    state       JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(state) = 'object'),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE public.message_event (
    tenant_id            TEXT NOT NULL,
    event_id             TEXT NOT NULL CHECK (length(btrim(event_id)) BETWEEN 1 AND 256),
    session_id           TEXT NOT NULL,
    binding_id           TEXT NOT NULL,
    external_message_id  TEXT NOT NULL CHECK (length(btrim(external_message_id)) BETWEEN 1 AND 512),
    idempotency_key      TEXT NOT NULL DEFAULT '' CHECK (length(idempotency_key) <= 512),
    event_seq            BIGINT NOT NULL CHECK (event_seq >= 1),
    status               TEXT NOT NULL DEFAULT 'received'
                         CHECK (status IN ('received', 'running', 'completed', 'execution_reconciling', 'reply_pending', 'replied', 'failed')),
    fencing_token        BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_owner          TEXT NOT NULL DEFAULT '' CHECK (length(lease_owner) <= 256),
    lease_expires_at     TIMESTAMPTZ,
    reply_id             TEXT NOT NULL DEFAULT '',
    segment_count        INT NOT NULL DEFAULT 0 CHECK (segment_count >= 0),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, event_id),
    UNIQUE (tenant_id, session_id, event_seq),
    UNIQUE (tenant_id, binding_id, external_message_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES public.runtime_session(tenant_id, session_id),
    FOREIGN KEY (tenant_id, binding_id) REFERENCES public.channel_binding(tenant_id, binding_id)
);

CREATE INDEX message_event_pending_idx
    ON public.message_event (tenant_id, status, updated_at)
    WHERE status IN ('received', 'running', 'execution_reconciling', 'reply_pending');

CREATE TABLE public.reply_outbox (
    tenant_id           TEXT NOT NULL,
    reply_id            TEXT NOT NULL CHECK (length(btrim(reply_id)) BETWEEN 1 AND 256),
    event_id            TEXT NOT NULL,
    segment_index       INT NOT NULL CHECK (segment_index >= 0),
    segment_count       INT NOT NULL CHECK (segment_count > segment_index),
    payload             TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'sending', 'sent', 'retryable', 'dead_letter')),
    attempts            INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    fencing_token       BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_owner         TEXT NOT NULL DEFAULT '' CHECK (length(lease_owner) <= 256),
    lease_expires_at    TIMESTAMPTZ,
    provider_message_id TEXT NOT NULL DEFAULT '' CHECK (length(provider_message_id) <= 512),
    last_error_class    TEXT NOT NULL DEFAULT '' CHECK (length(last_error_class) <= 128),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, reply_id, segment_index),
    UNIQUE (tenant_id, event_id, reply_id, segment_index),
    FOREIGN KEY (tenant_id, event_id) REFERENCES public.message_event(tenant_id, event_id)
);

CREATE INDEX reply_outbox_delivery_idx
    ON public.reply_outbox (tenant_id, status, updated_at)
    WHERE status IN ('pending', 'retryable', 'sending');

REVOKE ALL ON TABLE public.runtime_session, public.message_event, public.reply_outbox FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_session, public.message_event, public.reply_outbox TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_session, public.message_event, public.reply_outbox TO migration_owner;
