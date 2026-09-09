-- P2-02: restore-compatible config revision insert guard.
--
-- 000009 installed tenant_config_revision_insert_guard(), a BEFORE INSERT
-- trigger that forces every tenant_config_version row to enter as 'draft'.
-- The P2-02 data-only logical restore re-inserts rows in their FINAL state
-- (for example 'published') with plain COPY, which the original guard
-- always rejects. The restore protocol deliberately never disables
-- constraints or triggers, so the guard itself must understand the one
-- legitimate non-tenant path: the explicitly privileged offline recovery
-- owner.
--
-- This migration replaces ONLY the guard function:
--   * rows inserted with tenant context (every runtime/application path,
--     which P2-01 already forces through tenant transactions) keep the
--     exact 000009 behavior: a non-draft revision is still rejected;
--   * the runtime role cannot write any tenant row without tenant context
--     (FORCE ROW LEVEL SECURITY rejects it before this trigger runs);
--   * sessions without tenant context are the migration/recovery owner, the
--     same explicitly privileged offline path that runs migrations.
-- The update guard and every status transition rule of 000009 are unchanged.
CREATE OR REPLACE FUNCTION tenant_config_revision_insert_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.status <> 'draft'
        AND current_setting('trpc.tenant_id', true) IS NOT NULL THEN
        RAISE EXCEPTION 'tenant_config_version revisions must start as draft';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
