CREATE TABLE knowledge_document (
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id),
    app_id TEXT NOT NULL,
    document_id TEXT NOT NULL,
    name TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256)=64),
    content_bytes BIGINT NOT NULL CHECK (content_bytes BETWEEN 1 AND 1048576),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    operation TEXT NOT NULL CHECK (operation IN ('upsert','delete')),
    job_id TEXT NOT NULL DEFAULT '',
    chunks INTEGER NOT NULL DEFAULT 0 CHECK (chunks >= 0),
    state TEXT NOT NULL CHECK (state IN ('pending','running','ready','deleting','failed')),
    version BIGINT NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,app_id,document_id),
    FOREIGN KEY (tenant_id,app_id) REFERENCES agent_app(tenant_id,app_id),
    FOREIGN KEY (tenant_id,revision_id) REFERENCES agent_revision(tenant_id,revision_id)
);

CREATE INDEX knowledge_document_updated
    ON knowledge_document(tenant_id,app_id,updated_at DESC,document_id DESC);

CREATE FUNCTION platform_knowledge_document_job_state() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    IF NEW.job_type = 'knowledge_upsert' THEN
        UPDATE knowledge_document
        SET state = CASE NEW.status WHEN 'completed' THEN 'ready' WHEN 'dead' THEN 'failed' WHEN 'running' THEN 'running' ELSE 'pending' END,
            version = version + 1, updated_at = now()
        WHERE tenant_id = NEW.tenant_id AND job_id = NEW.job_id;
    ELSIF NEW.job_type = 'knowledge_delete' THEN
        IF NEW.status = 'completed' THEN
            DELETE FROM knowledge_document
            WHERE tenant_id = NEW.tenant_id AND job_id = NEW.job_id;
        ELSE
            UPDATE knowledge_document
            SET state = CASE NEW.status WHEN 'dead' THEN 'failed' ELSE 'deleting' END,
                version = version + 1, updated_at = now()
            WHERE tenant_id = NEW.tenant_id AND job_id = NEW.job_id;
        END IF;
    END IF;
    RETURN NEW;
END $$;

DO $$ BEGIN
 EXECUTE format('ALTER FUNCTION %I.platform_knowledge_document_job_state() SET search_path = %I, pg_temp',current_schema(),current_schema());
END $$;

CREATE TRIGGER knowledge_document_job_state
AFTER UPDATE OF status ON background_job
FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION platform_knowledge_document_job_state();

REVOKE ALL ON knowledge_document FROM PUBLIC;
REVOKE ALL ON FUNCTION platform_knowledge_document_job_state() FROM PUBLIC;
