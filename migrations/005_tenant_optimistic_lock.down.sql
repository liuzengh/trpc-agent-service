-- Compatibility migration only. Migration 001 is the authoritative owner of
-- config_version for fresh installations, so rolling back 005 must not remove
-- a column still required by the schema represented by 001. Rolling back 001
-- later removes the tenants table as a whole.
SELECT 1;
