BEGIN;
DROP SCHEMA public CASCADE;
DO $$
BEGIN
  DROP ROLE IF EXISTS audit_retention_purger;
EXCEPTION WHEN dependent_objects_still_exist THEN
  NULL;
END
$$;
CREATE SCHEMA public;
GRANT USAGE ON SCHEMA public TO PUBLIC;
CREATE TABLE public.schema_migrations(version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());
COMMIT;
