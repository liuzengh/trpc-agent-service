-- One database, fixed schemas and independent non-admin identities.
-- Credentials are read from the process environment, never echoed by this file.
\getenv control_migrator_password CONTROL_MIGRATOR_PASSWORD
\getenv control_runtime_password CONTROL_RUNTIME_PASSWORD
\getenv gateway_migrator_password GATEWAY_MIGRATOR_PASSWORD
\getenv gateway_runtime_password GATEWAY_RUNTIME_PASSWORD
\getenv worker_migrator_password WORKER_MIGRATOR_PASSWORD
\getenv worker_runtime_password WORKER_RUNTIME_PASSWORD
\getenv session_migrator_password SESSION_MIGRATOR_PASSWORD
\getenv session_runtime_password SESSION_RUNTIME_PASSWORD
BEGIN;
SELECT pg_advisory_xact_lock(731004280);
-- Guard first: an old public-schema deployment must not become an empty V1 app.
DO $guard$
DECLARE item record;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','f')) THEN
    RAISE EXCEPTION 'Legacy public objects found; perform an explicit offline data migration before schema provisioning';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_database WHERE datname='channel_gateway') THEN
    RAISE EXCEPTION 'Legacy channel_gateway database found; reconcile existing Gateway data before schema provisioning';
  END IF;
  FOR item IN SELECT r.* FROM pg_roles r WHERE r.rolname IN (
    'control_migrator','control_runtime','gateway_migrator','gateway_runtime',
    'worker_migrator','worker_runtime','session_migrator','session_runtime') LOOP
    IF item.rolsuper OR item.rolcreatedb OR item.rolcreaterole OR item.rolreplication OR item.rolbypassrls THEN
      RAISE EXCEPTION 'Existing application role has elevated attributes: %',item.rolname;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_auth_members WHERE member=item.oid) THEN
      RAISE EXCEPTION 'Application role has unexpected membership: %',item.rolname;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_database WHERE datdba=item.oid) THEN
      RAISE EXCEPTION 'Application role must not own a database: %',item.rolname;
    END IF;
  END LOOP;
  FOR item IN SELECT * FROM (VALUES
    ('control','control_migrator'),('gateway','gateway_migrator'),
    ('worker','worker_migrator'),('runtime_session','session_migrator')) AS t(schema_name,owner_name) LOOP
    IF EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname=item.schema_name
               AND pg_get_userbyid(n.nspowner)<>item.owner_name) THEN
      RAISE EXCEPTION 'Existing schema owner differs: %',item.schema_name;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname=item.schema_name AND pg_get_userbyid(c.relowner)<>item.owner_name)
       OR EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                  WHERE n.nspname=item.schema_name AND pg_get_userbyid(p.proowner)<>item.owner_name) THEN
      RAISE EXCEPTION 'Existing object owner differs: %',item.schema_name;
    END IF;
  END LOOP;
END
$guard$;
SELECT format('CREATE ROLE control_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'control_migrator_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='control_migrator') \gexec
SELECT format('CREATE ROLE control_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'control_runtime_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='control_runtime') \gexec
SELECT format('CREATE ROLE gateway_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'gateway_migrator_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='gateway_migrator') \gexec
SELECT format('CREATE ROLE gateway_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'gateway_runtime_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='gateway_runtime') \gexec
SELECT format('CREATE ROLE worker_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'worker_migrator_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='worker_migrator') \gexec
SELECT format('CREATE ROLE worker_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'worker_runtime_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='worker_runtime') \gexec
SELECT format('CREATE ROLE session_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'session_migrator_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='session_migrator') \gexec
SELECT format('CREATE ROLE session_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L', :'session_runtime_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='session_runtime') \gexec
-- No password reset, ownership takeover, or silent data movement on replay.
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC',current_database()) \gexec
REVOKE ALL ON SCHEMA public FROM PUBLIC;
SELECT 'CREATE SCHEMA control AUTHORIZATION control_migrator' WHERE NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='control') \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM control_migrator',current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO control_migrator',current_database()) \gexec
SELECT format('ALTER ROLE control_migrator IN DATABASE %I SET search_path TO control, pg_temp',current_database()) \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM control_runtime',current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO control_runtime',current_database()) \gexec
SELECT format('ALTER ROLE control_runtime IN DATABASE %I SET search_path TO control, pg_temp',current_database()) \gexec
REVOKE ALL ON SCHEMA control FROM PUBLIC;
REVOKE ALL ON SCHEMA control FROM control_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA control FROM control_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA control FROM control_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM control_runtime;
REVOKE ALL ON SCHEMA control FROM gateway_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA control FROM gateway_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA control FROM gateway_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM gateway_migrator;
REVOKE ALL ON SCHEMA control FROM gateway_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA control FROM gateway_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA control FROM gateway_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM gateway_runtime;
REVOKE ALL ON SCHEMA control FROM worker_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA control FROM worker_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA control FROM worker_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM worker_migrator;
REVOKE ALL ON SCHEMA control FROM worker_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA control FROM worker_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA control FROM worker_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM worker_runtime;
REVOKE ALL ON SCHEMA control FROM session_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA control FROM session_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA control FROM session_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM session_migrator;
REVOKE ALL ON SCHEMA control FROM session_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA control FROM session_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA control FROM session_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM session_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA control FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA control FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA control FROM PUBLIC;
GRANT USAGE ON SCHEMA control TO control_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA control TO control_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA control TO control_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE control_migrator REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE control_migrator IN SCHEMA control REVOKE ALL ON TABLES FROM control_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE control_migrator IN SCHEMA control GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO control_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE control_migrator IN SCHEMA control GRANT USAGE, SELECT ON SEQUENCES TO control_runtime;
SELECT 'REVOKE ALL ON TABLE control.control_schema_migrations FROM control_runtime, PUBLIC'
WHERE to_regclass('control.control_schema_migrations') IS NOT NULL \gexec
SELECT 'CREATE SCHEMA gateway AUTHORIZATION gateway_migrator' WHERE NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='gateway') \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM gateway_migrator',current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO gateway_migrator',current_database()) \gexec
SELECT format('ALTER ROLE gateway_migrator IN DATABASE %I SET search_path TO gateway, pg_temp',current_database()) \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM gateway_runtime',current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO gateway_runtime',current_database()) \gexec
SELECT format('ALTER ROLE gateway_runtime IN DATABASE %I SET search_path TO gateway, pg_temp',current_database()) \gexec
REVOKE ALL ON SCHEMA gateway FROM PUBLIC;
REVOKE ALL ON SCHEMA gateway FROM control_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA gateway FROM control_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA gateway FROM control_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA gateway FROM control_migrator;
REVOKE ALL ON SCHEMA gateway FROM control_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA gateway FROM control_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA gateway FROM control_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA gateway FROM control_runtime;
REVOKE ALL ON SCHEMA gateway FROM gateway_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA gateway FROM gateway_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA gateway FROM gateway_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA gateway FROM gateway_runtime;
REVOKE ALL ON SCHEMA gateway FROM worker_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA gateway FROM worker_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA gateway FROM worker_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA gateway FROM worker_migrator;
REVOKE ALL ON SCHEMA gateway FROM worker_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA gateway FROM worker_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA gateway FROM worker_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA gateway FROM worker_runtime;
REVOKE ALL ON SCHEMA gateway FROM session_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA gateway FROM session_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA gateway FROM session_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA gateway FROM session_migrator;
REVOKE ALL ON SCHEMA gateway FROM session_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA gateway FROM session_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA gateway FROM session_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA gateway FROM session_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA gateway FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA gateway FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA gateway FROM PUBLIC;
GRANT USAGE ON SCHEMA gateway TO gateway_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gateway TO gateway_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gateway TO gateway_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE gateway_migrator REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE gateway_migrator IN SCHEMA gateway REVOKE ALL ON TABLES FROM gateway_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE gateway_migrator IN SCHEMA gateway GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO gateway_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE gateway_migrator IN SCHEMA gateway GRANT USAGE, SELECT ON SEQUENCES TO gateway_runtime;
SELECT 'REVOKE ALL ON TABLE gateway.gateway_schema_migrations FROM gateway_runtime, PUBLIC'
WHERE to_regclass('gateway.gateway_schema_migrations') IS NOT NULL \gexec
SELECT 'CREATE SCHEMA worker AUTHORIZATION worker_migrator' WHERE NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='worker') \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM worker_migrator',current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO worker_migrator',current_database()) \gexec
SELECT format('ALTER ROLE worker_migrator IN DATABASE %I SET search_path TO worker, pg_temp',current_database()) \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM worker_runtime',current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO worker_runtime',current_database()) \gexec
SELECT format('ALTER ROLE worker_runtime IN DATABASE %I SET search_path TO worker, pg_temp',current_database()) \gexec
REVOKE ALL ON SCHEMA worker FROM PUBLIC;
REVOKE ALL ON SCHEMA worker FROM control_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA worker FROM control_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA worker FROM control_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA worker FROM control_migrator;
REVOKE ALL ON SCHEMA worker FROM control_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA worker FROM control_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA worker FROM control_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA worker FROM control_runtime;
REVOKE ALL ON SCHEMA worker FROM gateway_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA worker FROM gateway_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA worker FROM gateway_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA worker FROM gateway_migrator;
REVOKE ALL ON SCHEMA worker FROM gateway_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA worker FROM gateway_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA worker FROM gateway_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA worker FROM gateway_runtime;
REVOKE ALL ON SCHEMA worker FROM worker_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA worker FROM worker_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA worker FROM worker_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA worker FROM worker_runtime;
REVOKE ALL ON SCHEMA worker FROM session_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA worker FROM session_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA worker FROM session_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA worker FROM session_migrator;
REVOKE ALL ON SCHEMA worker FROM session_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA worker FROM session_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA worker FROM session_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA worker FROM session_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA worker FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA worker FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA worker FROM PUBLIC;
GRANT USAGE ON SCHEMA worker TO worker_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA worker TO worker_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA worker TO worker_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE worker_migrator REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE worker_migrator IN SCHEMA worker REVOKE ALL ON TABLES FROM worker_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE worker_migrator IN SCHEMA worker GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO worker_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE worker_migrator IN SCHEMA worker GRANT USAGE, SELECT ON SEQUENCES TO worker_runtime;
SELECT 'REVOKE ALL ON TABLE worker.worker_schema_migrations FROM worker_runtime, PUBLIC'
WHERE to_regclass('worker.worker_schema_migrations') IS NOT NULL \gexec
SELECT 'CREATE SCHEMA runtime_session AUTHORIZATION session_migrator' WHERE NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='runtime_session') \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM session_migrator',current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO session_migrator',current_database()) \gexec
SELECT format('ALTER ROLE session_migrator IN DATABASE %I SET search_path TO runtime_session, pg_temp',current_database()) \gexec
SELECT format('REVOKE ALL ON DATABASE %I FROM session_runtime',current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO session_runtime',current_database()) \gexec
SELECT format('ALTER ROLE session_runtime IN DATABASE %I SET search_path TO runtime_session, pg_temp',current_database()) \gexec
REVOKE ALL ON SCHEMA runtime_session FROM PUBLIC;
REVOKE ALL ON SCHEMA runtime_session FROM control_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA runtime_session FROM control_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA runtime_session FROM control_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA runtime_session FROM control_migrator;
REVOKE ALL ON SCHEMA runtime_session FROM control_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA runtime_session FROM control_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA runtime_session FROM control_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA runtime_session FROM control_runtime;
REVOKE ALL ON SCHEMA runtime_session FROM gateway_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA runtime_session FROM gateway_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA runtime_session FROM gateway_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA runtime_session FROM gateway_migrator;
REVOKE ALL ON SCHEMA runtime_session FROM gateway_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA runtime_session FROM gateway_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA runtime_session FROM gateway_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA runtime_session FROM gateway_runtime;
REVOKE ALL ON SCHEMA runtime_session FROM worker_migrator;
REVOKE ALL ON ALL TABLES IN SCHEMA runtime_session FROM worker_migrator;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA runtime_session FROM worker_migrator;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA runtime_session FROM worker_migrator;
REVOKE ALL ON SCHEMA runtime_session FROM worker_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA runtime_session FROM worker_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA runtime_session FROM worker_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA runtime_session FROM worker_runtime;
REVOKE ALL ON SCHEMA runtime_session FROM session_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA runtime_session FROM session_runtime;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA runtime_session FROM session_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA runtime_session FROM session_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA runtime_session FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA runtime_session FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA runtime_session FROM PUBLIC;
GRANT USAGE ON SCHEMA runtime_session TO session_runtime;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA runtime_session TO session_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA runtime_session TO session_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE session_migrator REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE session_migrator IN SCHEMA runtime_session REVOKE ALL ON TABLES FROM session_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE session_migrator IN SCHEMA runtime_session GRANT SELECT, INSERT ON TABLES TO session_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE session_migrator IN SCHEMA runtime_session GRANT USAGE, SELECT ON SEQUENCES TO session_runtime;
SELECT 'REVOKE ALL ON TABLE runtime_session.session_schema_migrations FROM session_runtime, PUBLIC'
WHERE to_regclass('runtime_session.session_schema_migrations') IS NOT NULL \gexec
COMMIT;
SELECT 'V1 schema provisioning complete' AS result;
