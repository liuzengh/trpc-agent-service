-- Keep existing connection IDs, destinations and ciphertext intact. Published
-- Agent revisions continue to point to the same immutable execution settings.
ALTER TABLE model_connection
    ADD COLUMN root_connection_id TEXT,
    ADD COLUMN config_version BIGINT NOT NULL DEFAULT 1 CHECK (config_version > 0),
    ADD COLUMN credential_version BIGINT NOT NULL DEFAULT 1 CHECK (credential_version > 0),
    ADD COLUMN version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    ADD COLUMN superseded_by TEXT,
    ADD COLUMN updated_by TEXT NOT NULL DEFAULT '',
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE model_connection SET root_connection_id=connection_id,
    updated_by=created_by, updated_at=created_at;
ALTER TABLE model_connection ALTER COLUMN root_connection_id SET NOT NULL;
ALTER TABLE model_connection
    ADD CONSTRAINT model_connection_root_fk FOREIGN KEY (tenant_id,root_connection_id)
        REFERENCES model_connection(tenant_id,connection_id),
    ADD CONSTRAINT model_connection_successor_fk FOREIGN KEY (tenant_id,superseded_by)
        REFERENCES model_connection(tenant_id,connection_id),
    ADD CONSTRAINT model_connection_config_version_unique UNIQUE (tenant_id,root_connection_id,config_version);

CREATE FUNCTION platform_model_connection_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.tenant_id,NEW.connection_id,NEW.root_connection_id,NEW.config_version,
           NEW.model_name,NEW.base_url,NEW.key_id,NEW.created_by,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.tenant_id,OLD.connection_id,OLD.root_connection_id,OLD.config_version,
           OLD.model_name,OLD.base_url,OLD.key_id,OLD.created_by,OLD.created_at) THEN
        RAISE EXCEPTION 'model execution settings are immutable' USING ERRCODE='23514';
    END IF;
    IF NEW.version<>OLD.version+1 OR
       (OLD.superseded_by IS NOT NULL AND NEW.superseded_by IS DISTINCT FROM OLD.superseded_by) THEN
        RAISE EXCEPTION 'invalid model connection revision' USING ERRCODE='23514';
    END IF;
    IF NEW.encrypted_key IS DISTINCT FROM OLD.encrypted_key THEN
        IF NEW.credential_version<>OLD.credential_version+1 THEN
            RAISE EXCEPTION 'credential revision must advance' USING ERRCODE='23514';
        END IF;
    ELSIF NEW.credential_version<>OLD.credential_version THEN
        RAISE EXCEPTION 'credential revision cannot change without a key update' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER model_connection_guard BEFORE UPDATE ON model_connection
FOR EACH ROW EXECUTE FUNCTION platform_model_connection_guard();
