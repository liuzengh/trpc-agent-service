CREATE TABLE backend_connection (
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id),
    connection_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    resource_type TEXT NOT NULL CHECK (resource_type IN ('session','memory','knowledge','artifact')),
    backend_type TEXT NOT NULL CHECK (backend_type IN ('inmemory','redis','postgres','qdrant','s3')),
    config JSONB NOT NULL,
    settings JSONB NOT NULL,
    credential_ref TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, connection_id)
);

CREATE TABLE skill_bundle (
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id),
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    checksum TEXT NOT NULL CHECK (length(checksum)=64),
    description TEXT NOT NULL,
    markdown TEXT NOT NULL CHECK (octet_length(markdown) BETWEEN 1 AND 65536),
    script TEXT NOT NULL CHECK (octet_length(script) BETWEEN 0 AND 65536),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','revoked')),
    revision BIGINT NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL,
    reviewed_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,name,version)
);

-- Published references bind the complete content; edits must use a new version.
CREATE FUNCTION platform_skill_bundle_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.tenant_id,NEW.name,NEW.version,NEW.checksum,NEW.description,NEW.markdown,NEW.script,NEW.created_by,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.tenant_id,OLD.name,OLD.version,OLD.checksum,OLD.description,OLD.markdown,OLD.script,OLD.created_by,OLD.created_at) THEN
        RAISE EXCEPTION 'skill content is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER skill_bundle_immutable BEFORE UPDATE ON skill_bundle
FOR EACH ROW EXECUTE FUNCTION platform_skill_bundle_immutable();

REVOKE ALL ON backend_connection,skill_bundle FROM PUBLIC;
