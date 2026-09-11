#!/usr/bin/env python3
"""Real V1 PostgreSQL ACL/bootstrap gate; creates and removes its own Docker instance."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    root = args.root.resolve()
    name = 'v1-schema-test-' + uuid.uuid4().hex[:12]
    db = 'v1_schema_test'
    schemas = {'control': 'control', 'gateway': 'gateway', 'worker': 'worker', 'runtime_session': 'session'}
    roles = [prefix + suffix for prefix in schemas.values() for suffix in ('_migrator', '_runtime')]
    passwords = {role.upper() + '_PASSWORD': secrets.token_urlsafe(24) for role in roles}
    checks = 0

    def command(argv, *, stdin=None, env=None, ok=True):
        result = subprocess.run(argv, input=stdin, text=True, capture_output=True, env=env)
        if ok and result.returncode:
            # Provisioner does not echo passwords. Still scrub environment values on failure.
            out = result.stdout + result.stderr
            for secret in passwords.values():
                out = out.replace(secret, '[redacted]')
            raise RuntimeError(f'command failed (exit {result.returncode}): {argv[0]}\n{out}')
        return result

    def sql(statement, role='v1_admin', database=db, ok=True):
        return command(['docker', 'exec', '-i', name, 'psql', '--no-psqlrc', '-qAt',
                        '--set=ON_ERROR_STOP=1', '-U', role, '-d', database], stdin=statement, ok=ok)

    def expect(statement, expected, role='v1_admin', database=db):
        nonlocal checks
        actual = sql(statement, role, database).stdout.strip()
        if actual != expected:
            raise AssertionError(f'query result mismatch for {role}: {actual!r} != {expected!r}')
        checks += 1

    def denied(statement, role):
        nonlocal checks
        result = sql('\\set VERBOSITY verbose\n' + statement, role, ok=False)
        if result.returncode == 0 or '42501' not in result.stderr:
            raise AssertionError(f'expected insufficient_privilege for {role}: {result.stderr}')
        checks += 1

    def provision(database=db, ok=True):
        env = dict(os.environ, **passwords)
        argv = ['docker', 'exec', '-e', 'PGUSER=v1_admin', '-e', 'PGDATABASE=' + database]
        for key in passwords:
            argv.extend(['-e', key])
        argv.extend([name, 'sh', '/v1/provision-schemas.sh'])
        return command(argv, env=env, ok=ok)

    started = False
    nats_started = False
    nats_name = name + '-nats'
    try:
        command(['docker', 'run', '--rm', '--detach', '--name', name,
                 '--env', 'POSTGRES_USER=v1_admin', '--env', 'POSTGRES_DB=' + db,
                 # Disposable, loopback-only acceptance fixture; production Compose requires passwords.
                 '--env', 'POSTGRES_HOST_AUTH_METHOD=trust', '--publish', '127.0.0.1::5432',
                 '--volume', str(root / 'deploy/compose') + ':/v1:ro', 'postgres:17.6-alpine'])
        started = True
        for _ in range(120):
            if command(['docker', 'exec', name, 'pg_isready', '-h', '127.0.0.1', '-U', 'v1_admin', '-d', db], ok=False).returncode == 0:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError('disposable PostgreSQL did not become ready')
        provision()
        role_hash = sql("SELECT string_agg(rolname || ':' || rolpassword, ',' ORDER BY rolname) FROM pg_authid WHERE rolname LIKE '%_runtime' OR rolname LIKE '%_migrator'").stdout
        provision()
        assert sql("SELECT string_agg(rolname || ':' || rolpassword, ',' ORDER BY rolname) FROM pg_authid WHERE rolname LIKE '%_runtime' OR rolname LIKE '%_migrator'").stdout == role_hash
        checks += 1
        expect("SELECT count(*) FROM pg_namespace WHERE nspname IN ('control','gateway','worker','runtime_session')", '4')
        for schema, prefix in schemas.items():
            runtime = prefix + '_runtime'
            migrator = prefix + '_migrator'
            expect('SELECT current_schema()', schema, runtime)
            expect('SELECT current_schema()', schema, migrator)
            expect("SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls FROM pg_roles WHERE rolname=current_user", 'f', runtime)
            expect("SELECT has_database_privilege(current_user,current_database(),'CREATE') OR has_database_privilege(current_user,current_database(),'TEMP')", 'f', runtime)
            expect("SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)", '0', runtime)
            sql('CREATE TABLE isolation_probe (id bigserial PRIMARY KEY, value text NOT NULL)', migrator)
            expect("INSERT INTO isolation_probe(value) VALUES ('initial') RETURNING id", '1', runtime)
            expect('SELECT value FROM isolation_probe', 'initial', runtime)
            if schema == 'runtime_session':
                denied("UPDATE isolation_probe SET value='changed'", runtime)
                denied('DELETE FROM isolation_probe', runtime)
            else:
                expect("UPDATE isolation_probe SET value='changed' RETURNING value", 'changed', runtime)
                expect('DELETE FROM isolation_probe RETURNING id', '1', runtime)
                sql("INSERT INTO isolation_probe(value) VALUES ('retained')", runtime)
            denied('CREATE TABLE illegal_ddl (id int)', runtime)
            denied('CREATE TEMP TABLE illegal_temp (id int)', runtime)
            denied('CREATE SCHEMA illegal_schema', runtime)
            denied('TRUNCATE isolation_probe', runtime)
            denied('SET ROLE ' + migrator, runtime)
        for schema, prefix in schemas.items():
            for other in schemas:
                if other != schema:
                    denied(f'SELECT * FROM {other}.isolation_probe', prefix + '_runtime')
                    denied(f'INSERT INTO {other}.isolation_probe(value) VALUES (\'cross\')', prefix + '_runtime')
                    denied(f'SET search_path TO {other}; SELECT * FROM {other}.isolation_probe', prefix + '_runtime')
        # Later objects inherit the same ACLs, including sequences, but not PUBLIC functions.
        sql('CREATE TABLE next_object (id bigserial PRIMARY KEY); CREATE FUNCTION probe_function() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;', 'worker_migrator')
        expect('INSERT INTO next_object DEFAULT VALUES RETURNING id', '1', 'worker_runtime')
        denied('SELECT worker.probe_function()', 'worker_runtime')
        print(f'ACL PASS: {checks} live PostgreSQL assertions', flush=True)

        port = command(['docker', 'port', name, '5432/tcp']).stdout.strip().rsplit(':', 1)[1]
        dsn = lambda role: f'postgres://{role}@127.0.0.1:{port}/{db}?sslmode=disable'
        env = dict(os.environ,
                   CONTROL_TEST_DATABASE_URL=dsn('v1_admin'),
                   CONTROL_DB_CONTRACT_ADMIN_URL=dsn('v1_admin'),
                   CONTROL_DB_CONTRACT_MIGRATION_URL=dsn('control_migrator'),
                   CONTROL_DB_CONTRACT_RUNTIME_URL=dsn('control_runtime'),
                   GATEWAY_V1_TEST_ADMIN_DATABASE_URL=dsn('v1_admin'),
                   GATEWAY_V1_TEST_DATABASE_URL=dsn('gateway_runtime'),
                   GATEWAY_V1_TEST_MIGRATION_DATABASE_URL=dsn('gateway_migrator'))
        runs = [
            ['./services/control-api/internal/bootstrap', '-run', '^TestV1DatabaseContractControl'],
            ['./services/channel-gateway/internal/bootstrap', '-run', '^TestGatewayV1'],
        ]
        for targets in runs:
            result = command(['go', '-C', str(root), 'test', '-count=1', '-v', *targets], env=env)
            if '--- SKIP:' in result.stdout or '[no tests to run]' in result.stdout:
                raise RuntimeError('database contract gate must execute, not skip\n' + result.stdout)
            print(result.stdout, end='', flush=True)
        gate = command(['bash', str(root / 'scripts/test-control-integration.sh')], env=env)
        if 'CONTROL_INTEGRATION_GATE=PASS zero skipped tests' not in gate.stdout:
            raise RuntimeError('full Control integration gate did not complete')
        print('CONTROL_INTEGRATION_GATE=PASS zero skipped tests', flush=True)
        # Exercise the changed real Gateway App bootstraps, not only its DB helper.
        command(['docker', 'run', '--rm', '--detach', '--name', nats_name,
                 '--publish', '127.0.0.1::4222', 'nats:2.11.8-alpine', '--jetstream'])
        nats_started = True
        nats_port = command(['docker', 'port', nats_name, '4222/tcp']).stdout.strip().rsplit(':', 1)[1]
        env.update(GATEWAY_TEST_ALLOW_NATS_RESET='1',
                   GATEWAY_TEST_DATABASE_URL=dsn('v1_admin'),
                   GATEWAY_TEST_NATS_URL='nats://127.0.0.1:' + nats_port)
        gateway = command(['go', '-C', str(root), 'test', '-count=1', '-json',
                           './services/channel-gateway/internal/bootstrap',
                           './services/channel-gateway/migrations'], env=env)
        events = [json.loads(line) for line in gateway.stdout.splitlines() if line.startswith('{')]
        if any(event.get('Action') == 'skip' for event in events):
            raise RuntimeError('Gateway runtime gate skipped an integration case')
        passes = sum(event.get('Action') == 'pass' and 'Test' in event for event in events)
        if passes == 0:
            raise RuntimeError('Gateway runtime gate did not execute tests')
        print(f'GATEWAY_RUNTIME_GATE=PASS {passes} tests/subtests; real PostgreSQL/NATS; zero skipped tests', flush=True)
        # Provision after real service migrations must not re-grant ledger write permission.
        provision()
        for schema, prefix, ledger in [('control', 'control', 'control_schema_migrations'),
                                       ('gateway', 'gateway', 'gateway_schema_migrations')]:
            for operation in ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER', 'MAINTAIN'):
                expect(f"SELECT has_table_privilege(current_user,'{schema}.{ledger}','{operation}')", 'f', prefix + '_runtime')
            denied(f'DELETE FROM {schema}.{ledger}', prefix + '_runtime')
        expect('SELECT value FROM worker.isolation_probe', 'retained', 'worker_runtime')
        print('REPLAY PASS: credentials/data preserved; migration ledgers remain protected', flush=True)

        # Legacy guard runs before creating/granting anything in the selected database.
        sql('CREATE DATABASE v1_legacy_guard')
        sql('CREATE TABLE public.old_application (id int)', database='v1_legacy_guard')
        result = provision('v1_legacy_guard', ok=False)
        assert result.returncode != 0 and 'Legacy public objects found' in result.stderr
        expect("SELECT count(*) FROM pg_namespace WHERE nspname IN ('control','gateway','worker','runtime_session')", '0', database='v1_legacy_guard')
        checks += 1
        sql('ALTER SCHEMA worker OWNER TO v1_admin')
        result = provision(ok=False)
        assert result.returncode != 0 and 'Existing schema owner differs' in result.stderr
        sql('ALTER SCHEMA worker OWNER TO worker_migrator')
        sql('ALTER TABLE worker.isolation_probe OWNER TO v1_admin')
        result = provision(ok=False)
        assert result.returncode != 0 and 'Existing object owner differs' in result.stderr
        sql('ALTER TABLE worker.isolation_probe OWNER TO worker_migrator')
        checks += 2
        # Elevated existing identities fail instead of being silently reused/demoted.
        sql('ALTER ROLE worker_runtime CREATEROLE')
        result = provision(ok=False)
        assert result.returncode != 0 and 'elevated attributes' in result.stderr
        sql('ALTER ROLE worker_runtime NOCREATEROLE')
        sql('GRANT worker_migrator TO worker_runtime')
        result = provision(ok=False)
        assert result.returncode != 0 and 'unexpected membership' in result.stderr
        sql('REVOKE worker_migrator FROM worker_runtime')
        checks += 2
        print('GUARDS PASS: legacy public objects, schema/object owners, elevated roles, role membership rejected before provisioning', flush=True)
        print(f'V1 DATABASE ISOLATION PASS: {checks} SQL assertions + Control/Gateway real migration/runtime tests; no skipped contract tests', flush=True)
        return 0
    finally:
        if nats_started:
            command(['docker', 'rm', '--force', '--volumes', nats_name], ok=False)
        if started:
            command(['docker', 'rm', '--force', '--volumes', name], ok=False)
            print('CLEANUP PASS: disposable PostgreSQL/NATS instances removed', flush=True)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, AssertionError) as exc:
        print('V1 DATABASE ISOLATION FAIL: ' + str(exc), file=sys.stderr)
        sys.exit(1)
