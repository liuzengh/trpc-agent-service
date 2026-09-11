#!/usr/bin/env python3
"""Disposable real Collector/Tempo/Grafana gate; never starts business services."""
from __future__ import annotations
import argparse
import base64
import json
import os
import re
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return str(s.getsockname()[1])


def request(url, *, headers=None, body=None, context=None):
    req = urllib.request.Request(url, data=body, headers=headers or {})
    handler = urllib.request.HTTPSHandler(context=context) if context else urllib.request.HTTPHandler()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), handler)
    try:
        response = opener.open(req, timeout=3)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, response.read(1 << 20)


def wait(check, timeout=100):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if check():
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    raise RuntimeError('bounded fixture readiness timed out')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--keep-running', action='store_true')
    args = parser.parse_args()
    root, artifacts = args.root.resolve(), args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='tracing-stack-private-'))
    project = 'trace-gate-' + uuid.uuid4().hex[:10]
    password = secrets.token_urlsafe(32)
    password_file = work / 'grafana-password'
    password_file.write_text(password)
    password_file.chmod(0o644)  # Grafana uid reads only this bind-mounted leaf.
    env = {k: os.environ[k] for k in ('PATH', 'HOME', 'TMPDIR', 'GOCACHE', 'GOPATH', 'GOROOT') if k in os.environ}
    env['TRACING_GRAFANA_ADMIN_PASSWORD_FILE'] = str(password_file)
    for name in ('OTLP', 'TEMPO', 'GRAFANA', 'HEALTH', 'METRICS'):
        env['TRACING_' + name + '_PORT'] = port()
    compose = ['docker', 'compose', '-p', project, '-f', str(root / 'deploy/compose/compose.tracing.yaml'), '--profile', 'tracing']
    records, tls_container = [], None
    started, passed = False, False

    def run(command, timeout=180):
        p = subprocess.run(command, cwd=root, env=env, text=True, capture_output=True, timeout=timeout)
        record = {'command': command, 'cwd': str(root), 'stdout': p.stdout.replace(password, '[redacted]'), 'stderr': p.stderr.replace(password, '[redacted]'), 'exit': p.returncode}
        records.append(record)
        (artifacts / 'commands.json').write_text(json.dumps(records, indent=2) + '\n')
        if p.returncode:
            raise RuntimeError('fixture command failed: ' + command[0])
        return p.stdout

    def url(name, suffix=''):
        return 'http://127.0.0.1:' + env['TRACING_' + name + '_PORT'] + suffix

    try:
        topology = json.loads(run(compose + ['config', '--format', 'json']))
        assert len(topology['services']) == 3
        for service in topology['services'].values():
            assert '@sha256:' in service['image'] and service['read_only']
            assert service['cap_drop'] == ['ALL']
            for binding in service.get('ports', []):
                assert binding['host_ip'] == '127.0.0.1'
        collector_image = topology['services']['trace-collector']['image']
        run(['docker', 'run', '--rm', '-v', str(root / 'deploy/compose/tracing') + ':/etc/config:ro', collector_image, 'validate', '--config=/etc/config/collector.yaml'])
        started = True
        run(compose + ['up', '-d'])
        for name, suffix in [('HEALTH', '/'), ('TEMPO', '/ready'), ('GRAFANA', '/api/health')]:
            wait(lambda: request(url(name, suffix))[0] == 200)
        env.update(TRACING_STACK_OTLP_ENDPOINT=url('OTLP', '/v1/traces'), TRACING_STACK_TEMPO_URL=url('TEMPO'), TRACING_STACK_EVIDENCE_DIR=str(artifacts))
        result = run(['go', 'test', '-race', '-count=1', '-v', './platform/telemetrytrace', '-run', '^TestCollectorTempoRoundTrip$'])
        assert 'COLLECTOR_TEMPO=PASS' in result and 'SKIP' not in result
        print(result, end='', flush=True)
        # Anonymous query denied; the provisioned datasource works via Grafana's
        # authenticated proxy, not merely a health page or static YAML check.
        assert request(url('GRAFANA', '/api/datasources/uid/im-runtime-tempo'))[0] == 401
        auth = {'Authorization': 'Basic ' + base64.b64encode(('trace-admin:' + password).encode()).decode()}
        code, data = request(url('GRAFANA', '/api/datasources/uid/im-runtime-tempo'), headers=auth)
        assert code == 200 and json.loads(data)['url'] == 'http://trace-tempo:3200'
        trace_id = re.search(r'trace_id=([0-9a-f]{32})', result).group(1)
        proxy_path = '/api/datasources/proxy/uid/im-runtime-tempo/api/traces/' + trace_id
        code, data = request(url('GRAFANA', proxy_path), headers=dict(auth, Accept='application/json'))
        assert code == 200 and b'worker.session.commit' in data and b'STACK_SECRET_CANARY' not in data
        # Validate and exercise the TLS overlay on an additional owned collector.
        tls = work / 'tls'
        tls.mkdir()
        run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(tls / 'server-key.pem'), '-out', str(tls / 'server.pem'), '-days', '2', '-subj', '/CN=trace-collector', '-addext', 'subjectAltName=DNS:trace-collector,IP:127.0.0.1'])
        (tls / 'server-key.pem').chmod(0o644)
        env['TRACING_TLS_DIR'] = str(tls)
        tls_compose = compose[:-2] + ['-f', str(root / 'deploy/compose/compose.tracing.tls.yaml'), '--profile', 'tracing']
        tls_topology = json.loads(run(tls_compose + ['config', '--format', 'json']))
        assert len(tls_topology['services']['trace-collector']['command']) == 2
        mounts = ['-v', str(root / 'deploy/compose/tracing') + ':/etc/config:ro', '-v', str(tls) + ':/run/tracing-tls:ro']
        configs = ['--config=/etc/config/collector.yaml', '--config=/etc/config/collector-tls.yaml']
        run(['docker', 'run', '--rm'] + mounts + [collector_image, 'validate'] + configs)
        tls_container = project + '-tls-probe'
        tls_port = port()
        run(['docker', 'run', '--rm', '-d', '--name', tls_container, '--network', project + '_tracing', '-p', '127.0.0.1:' + tls_port + ':4318', '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges:true', '--memory=192m'] + mounts + [collector_image] + configs)
        tls_url = 'https://127.0.0.1:' + tls_port + '/v1/traces'
        trusted = ssl.create_default_context(cafile=str(tls / 'server.pem'))
        trusted.minimum_version = ssl.TLSVersion.TLSv1_3
        wait(lambda: request(tls_url, headers={'Content-Type': 'application/x-protobuf'}, body=b'', context=trusted)[0] == 200, 15)
        for context in [ssl.create_default_context(), ssl.create_default_context(cafile=str(tls / 'server.pem'))]:
            if context is not None and context.get_ca_certs() == trusted.get_ca_certs():
                context.maximum_version = ssl.TLSVersion.TLSv1_2
            try:
                request(tls_url, headers={'Content-Type': 'application/x-protobuf'}, body=b'', context=context)
            except (OSError, urllib.error.URLError):
                pass
            else:
                raise AssertionError('untrusted CA or TLS 1.2 was accepted')
        (artifacts / 'stack-result.json').write_text(json.dumps({'result': 'PASS', 'project': project, 'ports': {k: v for k, v in env.items() if k.endswith('_PORT')}, 'private_directory': str(work) if args.keep_running else 'removed after cleanup', 'grafana_anonymous': 401, 'grafana_datasource_query': 200, 'trace_id': trace_id, 'tls13_trusted': 200, 'untrusted_ca': 'REJECTED', 'tls12': 'REJECTED', 'images': {k: v['image'] for k, v in topology['services'].items()}, 'evidence_scope': 'real observation services; synthetic application spans; not external IM acceptance'}, indent=2) + '\n')
        passed = True
        print('TRACING_STACK=PASS collector_tempo_query=true grafana_authenticated=true tls13=true', flush=True)
    finally:
        if tls_container:
            run(['docker', 'rm', '-f', tls_container])
        if not (passed and args.keep_running):
            if started:
                run(compose + ['down', '--volumes'])
            shutil.rmtree(work)


if __name__ == '__main__':
    main()
