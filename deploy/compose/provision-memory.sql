-- Add Memory to the same database without changing existing schema ownership.
\getenv migrator_password MEMORY_MIGRATOR_PASSWORD
\getenv runtime_password MEMORY_RUNTIME_PASSWORD
BEGIN;
SELECT pg_advisory_xact_lock(731004280);
DO $guard$
DECLARE r record;
BEGIN
 FOR r IN SELECT * FROM pg_roles WHERE rolname IN ('memory_migrator','memory_runtime') LOOP
  IF r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls
     OR EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid)
     OR EXISTS(SELECT 1 FROM pg_database WHERE datdba=r.oid) THEN
   RAISE EXCEPTION 'Memory role has unexpected privileges: %', r.rolname;
  END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='runtime_memory' AND pg_get_userbyid(nspowner)<>'memory_migrator') THEN
  RAISE EXCEPTION 'Memory schema owner mismatch';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='runtime_memory' AND pg_get_userbyid(c.relowner)<>'memory_migrator') THEN
  RAISE EXCEPTION 'Memory object owner mismatch';
 END IF;
END $guard$;
SELECT format('CREATE ROLE memory_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'migrator_password') WHERE NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='memory_migrator') \gexec
SELECT format('CREATE ROLE memory_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'runtime_password') WHERE NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='memory_runtime') \gexec
SELECT 'CREATE SCHEMA runtime_memory AUTHORIZATION memory_migrator' WHERE NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='runtime_memory') \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM memory_migrator,memory_runtime',current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO memory_migrator,memory_runtime',current_database()) \gexec
SELECT format('ALTER ROLE memory_migrator IN DATABASE %I SET search_path TO runtime_memory,pg_temp',current_database()) \gexec
SELECT format('ALTER ROLE memory_runtime IN DATABASE %I SET search_path TO runtime_memory,pg_temp',current_database()) \gexec
REVOKE ALL ON SCHEMA runtime_memory FROM PUBLIC,control_runtime,control_migrator,gateway_runtime,gateway_migrator,worker_runtime,worker_migrator,session_runtime,session_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA runtime_memory FROM PUBLIC,control_runtime,control_migrator,gateway_runtime,gateway_migrator,worker_runtime,worker_migrator,session_runtime,session_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA runtime_memory FROM PUBLIC;
GRANT USAGE ON SCHEMA runtime_memory TO memory_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE memory_migrator REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
COMMIT;
SELECT 'MEMORY_SCHEMA_PREPARE=PASS' AS result;
