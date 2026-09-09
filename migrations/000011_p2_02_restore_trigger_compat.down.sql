-- Restore the original 000009 guard body: every insert must start as draft.
CREATE OR REPLACE FUNCTION tenant_config_revision_insert_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.status <> 'draft' THEN
        RAISE EXCEPTION 'tenant_config_version revisions must start as draft';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
