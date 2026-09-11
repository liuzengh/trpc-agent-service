#!/usr/bin/env python3
"""Worker V1 executable integration gate with disposable PostgreSQL/NATS fixtures.

This gate includes real SDK/Session/transport/transactions and explicit HTTP
model/Profile fixtures. It is not the live model + Telegram completion gate.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import uuid


def published_port(command, name, number, *, timeout=10):
    """Wait only for a requested port on a live, starting private fixture.

    Inspect state and publication together: an exited image/config is not a
    transient missing-port observation. Keep diagnostics until caller cleanup.
    """
    key = str(number) + '/tcp'
    deadline = time.monotonic() + timeout
    template = '{"state":{{json .State}},"ports":{{json .NetworkSettings.Ports}},"requested":{{json .HostConfig.PortBindings}}}'
    while True:
        observed = json.loads(command(['docker', 'inspect', '--format', template, name]))
        state = observed['state']
        status = state.get('Status', 'unknown')
        def fail(reason):
            logs = command(['docker', 'logs', '--tail', '30', name]).strip()
            raise RuntimeError(f'{name} {key}: {reason}; state={status}; exit={state.get("ExitCode")}; logs={logs}')
        if status not in ('created', 'running') or state.get('Paused') or state.get('Restarting'):
            fail('container ' + status)
        if key not in (observed.get('requested') or {}):
            fail('port binding was not requested')
        bindings = (observed.get('ports') or {}).get(key) or []
        if status == 'running' and bindings:
            if len(bindings) != 1 or bindings[0].get('HostIp') != '127.0.0.1':
                fail('unexpected non-loopback or multiple port bindings')
            port = bindings[0].get('HostPort', '')
            if not port.isdigit() or not 1 <= int(port) <= 65535:
                fail('invalid published port')
            return port
        if time.monotonic() >= deadline:
            fail('port publication timeout')
        time.sleep(0.1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    root = args.root.resolve()
    prefix = 'worker-v1-gate-' + uuid.uuid4().hex[:10]
    pg, broker, auth_broker = prefix + '-pg', prefix + '-nats', prefix + '-acl'
    started = []
    passwords = {name: secrets.token_urlsafe(24) for name in
                 [p + s for p in ('CONTROL', 'GATEWAY', 'WORKER', 'SESSION')
                  for s in ('_MIGRATOR_PASSWORD', '_RUNTIME_PASSWORD')] +
                 ['NATS_' + p + '_PASSWORD' for p in ('CONTROL', 'GATEWAY', 'WORKER', 'RECONCILER')]}

    def command(argv, *, env=None):
        result = subprocess.run(argv, cwd=root, env=env, text=True, capture_output=True)
        output = result.stdout + result.stderr
        stdout = result.stdout
        for value in passwords.values():
            output = output.replace(value, '[redacted]')
            stdout = stdout.replace(value, '[redacted]')
        if result.returncode:
            raise RuntimeError(f'{argv[0]} failed with exit {result.returncode}\n{output}')
        return output if argv[:2] == ['docker', 'logs'] else stdout

    def start(name, argv):
        command(['docker', 'run', '-d', '--name', name, *argv])
        started.append(name)

    def port(name, number):
        return published_port(command, name, number)

    def test(package, pattern, required, env):
        argv = ['go', 'test', '-count=1', '-json']
        if args.race:
            argv.append('-race')
        output = command([*argv, package, '-run', pattern], env=env)
        entries = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
        if any(row.get('Action') == 'skip' for row in entries):
            raise RuntimeError(f'{package}: required integration skipped')
        passed = {row.get('Test') for row in entries if row.get('Action') == 'pass'}
        if not set(required).issubset(passed):
            raise RuntimeError(f'{package}: expected tests did not run: {set(required) - passed}')
        for row in entries:
            line = row.get('Output', '').strip()
            if any(marker in line for marker in ('=PASS', '_PASS:', 'WORKER_', 'SESSION_CANDIDATE')):
                print(line, flush=True)
        print(f'GATE PASS: {package}; {len(passed - {None})} tests/subtests; zero skips', flush=True)

    try:
        start(pg, ['-e', 'POSTGRES_USER=platform_admin', '-e', 'POSTGRES_DB=agent_platform',
                   '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', '-p', '127.0.0.1::5432',
                   '-v', str(root / 'deploy/compose') + ':/provision:ro', 'postgres:17.6-alpine'])
        for _ in range(120):
            result = subprocess.run(['docker', 'exec', pg, 'pg_isready', '-h', '127.0.0.1',
                                     '-U', 'platform_admin', '-d', 'agent_platform'], capture_output=True)
            if result.returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError('disposable PostgreSQL readiness timeout')
        env = dict(os.environ, **passwords)
        provision = ['docker', 'exec', '-e', 'PGUSER=platform_admin', '-e', 'PGDATABASE=agent_platform']
        for name in passwords:
            if not name.startswith('NATS_'):
                provision += ['-e', name]
        command([*provision, pg, 'sh', '/provision/provision-schemas.sh'], env=env)
        db_port = port(pg, 5432)
        dsn = lambda role: f'postgres://{role}:fixture-only@127.0.0.1:{db_port}/agent_platform?sslmode=disable'
        env.update(WORKER_TEST_ADMIN_URL=dsn('platform_admin'),
                   WORKER_TEST_MIGRATION_URL=dsn('worker_migrator'),
                   WORKER_TEST_RUNTIME_URL=dsn('worker_runtime'),
                   WORKER_PROCESSOR_TEST_RUNTIME_URL=dsn('worker_runtime'),
                   WORKER_TEST_ALLOW_RESET='1', WORKER_SESSION_TEST_ALLOW_RESET='1',
                   WORKER_SESSION_TEST_ADMIN_URL=dsn('platform_admin'),
                   WORKER_SESSION_TEST_MIGRATION_URL=dsn('session_migrator'),
                   WORKER_SESSION_TEST_RUNTIME_URL=dsn('session_runtime'),
                   CONTROL_TEST_DATABASE_URL=dsn('platform_admin'),
                   GATEWAY_REPLY_TEST_DATABASE_URL=dsn('platform_admin'))
        start(broker, ['-p', '127.0.0.1::4222', 'nats:2.11.8-alpine', '-js', '-sd', '/data'])
        url = 'nats://127.0.0.1:' + port(broker, 4222)
        env.update(WORKER_TEST_NATS_URL=url, GATEWAY_REPLY_TEST_NATS_URL=url)
        # These schema-resetting tests intentionally run serially.
        gates = [
            ('./services/agent-worker/internal/execution/adapter/outbound/sessionstore', '^TestSessionCandidatePostgresContract$', ['TestSessionCandidatePostgresContract']),
            ('./services/agent-worker/internal/execution/adapter/outbound/postgresadapter', '^TestWorker(LedgerV1|RetainedRunCapacity)Postgres$', ['TestWorkerLedgerV1Postgres', 'TestWorkerRetainedRunCapacityPostgres']),
            ('./services/agent-worker/internal/execution/application', '^TestProcessorPostgres', ['TestProcessorPostgresCommitResponseLossAfterRenewalCancellation', 'TestProcessorPostgresPreparationErrorIsExplicit']),
            ('./services/agent-worker/internal/manifest/adapter/outbound/postgresadapter', '^TestManifestProjectionV1Postgres$', ['TestManifestProjectionV1Postgres']),
            ('./services/agent-worker/integration', '^TestWorker(Vertical|ManifestRejection|ReplyCapacity)PGNATSSDK$', ['TestWorkerVerticalPGNATSSDK', 'TestWorkerManifestRejectionPGNATSSDK', 'TestWorkerReplyCapacityPGNATSSDK']),
            ('./services/channel-gateway/internal/delivery/adapter/inbound/nats', '^TestReplyHandoffPostgresNATSIntegration$', ['TestReplyHandoffPostgresNATSIntegration']),
            ('./services/control-api/internal/deployment/adapter/outbound/postgres', '^Test(ManifestDistributionAgainstPostgreSQL|WorkerV1PublicationTransactionAgainstPostgreSQL)$', ['TestManifestDistributionAgainstPostgreSQL', 'TestWorkerV1PublicationTransactionAgainstPostgreSQL']),
        ]
        for package, pattern, required in gates:
            test(package, pattern, required, env)
        auth_args = ['-p', '127.0.0.1::4222', '-v', str(root / 'deploy/nats/server.conf') + ':/etc/nats/server.conf:ro']
        for key in passwords:
            if key.startswith('NATS_'):
                auth_args += ['-e', key]
        # docker reads -e NAME from this process; generated fixture values never
        # become repository files or command-line password arguments.
        command(['docker', 'run', '-d', '--name', auth_broker,
                 *auth_args, 'nats:2.11.8-alpine', '-c', '/etc/nats/server.conf'], env=env)
        started.append(auth_broker)
        auth_url = 'nats://127.0.0.1:' + port(auth_broker, 4222)
        env.update(GATEWAY_NATS_URL=auth_url, GATEWAY_NATS_USER='reconciler',
                   GATEWAY_NATS_PASSWORD=passwords['NATS_RECONCILER_PASSWORD'],
                   GATEWAY_NATS_CA_FILE='', GATEWAY_NATS_TOPOLOGY_FILE=str(root / 'deploy/nats/streams.yaml'),
                   GATEWAY_TEST_AUTH_NATS_URL=auth_url, GATEWAY_TEST_TOPOLOGY_FILE=str(root / 'deploy/nats/streams.yaml'),
                   WORKER_TEST_NATS_URL=auth_url)
        print(command(['go', 'run', './services/channel-gateway/cmd/channel-gateway', 'reconcile'], env=env).strip(), flush=True)
        test('./services/agent-worker/internal/infra/natsadapter', '^TestWorkerNATSRuntimeHandoffAndReplyReplayIntegration$',
             ['TestWorkerNATSRuntimeHandoffAndReplyReplayIntegration'], env)
        test('./services/channel-gateway/internal/infra/nats', '^TestWorkerBrokerPermissionsIntegration$',
             ['TestWorkerBrokerPermissionsIntegration'], env)
        env['WORKER_BOOTSTRAP_TEST_NATS_URL'] = auth_url
        test('./services/agent-worker/internal/bootstrap', '^TestWorkerBootstrapRealPGNATSMTLSAndDrain$',
             ['TestWorkerBootstrapRealPGNATSMTLSAndDrain'], env)
        print('WORKER_V1_FIXTURE_GATE=PASS real PostgreSQL/NATS/SDK/Session/ACL; live model and Telegram acceptance remain separate', flush=True)
        return 0
    finally:
        for name in reversed(started):
            removed = subprocess.run(['docker', 'rm', '-f', name], text=True, capture_output=True)
            remains = subprocess.run(['docker', 'inspect', name], capture_output=True)
            if removed.returncode or remains.returncode == 0:
                raise RuntimeError(f'fixture cleanup failed: {name}')
        if started:
            print('WORKER_V1_CLEANUP=PASS disposable fixture instances removed', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
