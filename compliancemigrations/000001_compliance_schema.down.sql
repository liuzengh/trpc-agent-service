BEGIN;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM compliance.audit_purge_batch WHERE state = 'completed') THEN
    RAISE EXCEPTION 'cannot roll back compliance retention after purge execution' USING ERRCODE = '55000';
  END IF;
END
$$;
DROP SCHEMA compliance CASCADE;
DO $$
BEGIN
  DROP ROLE IF EXISTS compliance_purger;
END
$$;
CREATE SCHEMA compliance;
CREATE TABLE compliance.schema_migrations(version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp());
COMMIT;
