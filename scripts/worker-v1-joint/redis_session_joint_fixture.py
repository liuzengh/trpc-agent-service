"""Managed Redis Session fixture; Summary uses the exact existing SDK model fixture."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import signal
import subprocess

from harness import Harness, _http_status
from redis_memory_joint_fixture import REDIS_IMAGE, redis_command

# Load the existing Summary fixture without executing its command-line entry.
_source = Path(__file__).resolve().parents[1] / 'test-worker-summary-joint.py'
_spec = importlib.util.spec_from_file_location('worker_existing_summary_joint', _source)
_summary = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_summary)
SummaryModelFixture, CANARY = _summary.SummaryModelFixture, _summary.CANARY
BACKEND_ID = 'joint-session-redis'


class RedisSessionHarness(_summary.SummaryHarness):
    session_backend_id = BACKEND_ID

    def spawn(self, name, argv, env):
        if name == 'control-api':
            self.control_env = dict(env)
        return super().spawn(name, argv, env)

    def record(self, name, value):
        path = self.artifacts / name
        raw = self.redact(json.dumps(value, indent=2, ensure_ascii=False)) + '\n'
        path.write_text(raw)
        assert path.read_text() == raw

    def provision(self):
        # The ordinary PG Session schema remains deliberately empty: a Redis
        # selected Manifest must never silently fall back to that available DB.
        Harness.provision(self)
        self.session_password = self.secret()
        self.redis_admin_password = self.secret()
        self.redis = self.prefix + '-session-redis'
        self.redis_port = self.port()
        self.quarantined = None
        acl = '\n'.join(('user default off',
            'user fixture_admin on >' + self.redis_admin_password + ' ~* +@all',
            'user session_runtime on >' + self.session_password + ' ~runtime_session:* +@connection +get +type +pttl +eval +set', ''))
        acl_file = self.write('redis-session-users.acl', acl, mode=0o600)
        self.command(['docker', 'run', '--rm', '-d', '--name', self.redis, '--user', '0:0', '-p', '127.0.0.1:' + str(self.redis_port) + ':6379', '-v', acl_file + ':/run/users.acl:ro', REDIS_IMAGE, 'redis-server', '--aclfile', '/run/users.acl', '--appendonly', 'yes', '--appendfsync', 'always', '--maxmemory-policy', 'noeviction', '--save', ''])
        self.containers.append(self.redis)
        assert int(self.command(['docker', 'port', self.redis, '6379/tcp']).strip().rsplit(':', 1)[1]) == self.redis_port
        self.wait(lambda: self.redis_admin('PING') == 'PONG', 'Redis Session readiness')
        raw = self.redis_admin('CONFIG', 'GET', 'appendonly', 'appendfsync', 'maxmemory-policy')
        config = dict(zip(raw[::2], raw[1::2]))
        assert config == {'appendonly': 'yes', 'appendfsync': 'always', 'maxmemory-policy': 'noeviction'}
        assert self.redis_admin('DBSIZE') == 0
        self.redis_config = {'container': self.redis, 'image': REDIS_IMAGE, 'image_id': self.command(['docker', 'inspect', '--format', '{{.Image}}', self.redis]).strip(), 'port': self.redis_port, 'username': 'session_runtime', 'configuration': config, 'acl_mode': oct(Path(acl_file).stat().st_mode & 0o777)}
        self.record('redis-session-dependency.json', self.redis_config)

    def redis_admin(self, *args):
        return redis_command('127.0.0.1', self.redis_port, 'fixture_admin', self.redis_admin_password, *args)

    def bind_session_catalog(self, tenant_id):
        catalog = {'version': 'v1', 'backends': [{'id': BACKEND_ID, 'revision': 1, 'label': 'Joint Redis Session', 'kind': 'redis', 'roles': ['session'], 'enabled': True, 'tenant_ids': [tenant_id]}]}
        targets = {'version': 'v1', 'backends': [{'backend_id': BACKEND_ID, 'backend_revision': 1, 'kind': 'redis', 'adapter': 'managed-redis-v1', 'isolation': 'tenant-session-v1', 'limits': {'timeout_ms': 5000, 'max_concurrency': 4, 'max_bytes': 1048576}, 'redis': {'host': '127.0.0.1', 'port': self.redis_port, 'database': 0, 'username': 'session_runtime', 'tls': False}}]}
        env = dict(self.control_env)
        for suffix, value in [('CATALOG', catalog), ('TARGETS', targets)]:
            path = Path(self.write('session-' + suffix.lower() + '.json', value))
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_FILE'] = str(path)
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_SHA256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        env.pop('CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST', None)
        self.contract_digest = self.command([self.binaries['control-api'], '--print-deployment-contract-digest'], env=env).strip()
        env['CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST'] = self.contract_digest
        self.control.send_signal(signal.SIGTERM)
        self.control.wait(timeout=25)
        assert self.control.returncode == 0
        self.control = self.spawn('control-api', [self.binaries['control-api']], env)
        self.wait(lambda: _http_status(self.urls['control'] + '/healthz', timeout=1) == 204, 'Control Session catalog restart')
        self.record('session-backend-config.json', {'catalog': catalog, 'targets': targets, 'contract_digest': self.contract_digest})

    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['config']['storage']['session'] = {'kind': 'managed_session', 'backend_id': BACKEND_ID, 'backend_revision': 1}
            body['credentials']['storage']['session'] = {'dsn_password': {'action': 'replace', 'value': self.session_password}}
        result = super().api(method, path, body, status, idem)
        if method == 'POST' and path == '/v1/admin/tenants':
            self.bind_session_catalog(result['id'])
        return result

    def session_state(self):
        cursor, keys = '0', []
        while True:
            cursor, batch = self.redis_admin('SCAN', cursor, 'MATCH', 'runtime_session:*', 'COUNT', 100)
            keys.extend(batch)
            if cursor == '0':
                break
        assert len(keys) == len(set(keys))
        result = []
        for key in sorted(keys):
            raw = self.redis_admin('GET', key)
            content = json.loads(raw)
            ttl = self.redis_admin('PTTL', key)
            assert ttl == -1, 'immutable Session candidate unexpectedly expires'
            result.append({'key': key, 'raw': raw, 'pttl': ttl, 'candidate_ref': key.rsplit(':', 1)[1], 'content_digest': 'sha256:' + hashlib.sha256(raw.encode()).hexdigest(), 'content': content, 'attempt_id': content['identity']['attempt_id'], 'run_id': content['identity']['run_id'], 'session_id': content['identity']['session_id'], 'parent_ref': content['parent']['ref'], 'parent_digest': content['parent']['digest']})
        return result

    def session_candidates(self, run_id):
        return [row for row in self.session_state() if row['run_id'] == run_id]

    def quarantine_parent(self, candidate):
        assert self.quarantined is None
        key, raw = candidate['key'], candidate['raw']
        assert self.redis_admin('GET', key) == raw
        private = 'fixture_quarantine:' + hashlib.sha256(key.encode()).hexdigest()
        assert self.redis_admin('EXISTS', private) == 0
        assert self.redis_admin('RENAME', key, private) == 'OK'
        self.quarantined = {'key': key, 'private_key': private, 'raw': raw}
        assert self.redis_admin('EXISTS', key) == 0 and self.redis_admin('GET', private) == raw
        return copy.deepcopy(self.quarantined)

    def restore_parent(self):
        if getattr(self, 'quarantined', None) is None:
            return
        value = self.quarantined
        assert self.redis_admin('EXISTS', value['key']) == 0
        assert self.redis_admin('GET', value['private_key']) == value['raw']
        assert self.redis_admin('RENAME', value['private_key'], value['key']) == 'OK'
        assert self.redis_admin('GET', value['key']) == value['raw']
        self.quarantined = None

    def close(self):
        errors = []
        try:
            self.restore_parent()
        except BaseException as exc:
            errors.append(exc)
        try:
            super().close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            name = getattr(self, 'redis', None)
            if name:
                inspection = subprocess.run(['docker', 'inspect', name], capture_output=True, text=True)
                if inspection.returncode == 0:
                    removed = subprocess.run(['docker', 'rm', '-f', name], capture_output=True)
                    if removed.returncode:
                        errors.append(RuntimeError('Redis Session removal failed'))
                    inspection = subprocess.run(['docker', 'inspect', name], capture_output=True, text=True)
                gone = inspection.returncode != 0 and ('No such object: ' + name) in inspection.stderr
                if not gone:
                    errors.append(RuntimeError('Redis Session removal unverified'))
                if gone:
                    (self.work / 'redis-session-users.acl').unlink(missing_ok=True)
                self.record('redis-session-cleanup.json', {'result': 'FAIL' if errors else 'PASS', 'removed': gone, 'private_directory_removed': not self.work.exists(), 'parent_restored': getattr(self, 'quarantined', None) is None})
        if errors:
            raise RuntimeError('; '.join(self.redact(str(error)) for error in errors))


def wait_redis_success(h, run_id):
    """Formal PG Completion/Final plus the exact Redis candidate, no PG fallback."""
    from faults import run, completions, outboxes
    h.wait(lambda: run(h, run_id)['status'] == 'SUCCEEDED', 'Redis Session successful Run', timeout=60)
    completed = completions(h, run_id)
    assert len(completed) == 1 and completed[0]['kind'] == 'ATTEMPT' and completed[0]['status'] == 'SUCCEEDED'
    final = outboxes(h, run_id)
    candidate = h.session_candidates(run_id)
    assert len(final) == 1 and final[0]['intent_id'] == completed[0]['final_intent_id']
    assert len(candidate) == 1 and candidate[0]['candidate_ref'] == completed[0]['candidate_ref'] and candidate[0]['content_digest'] == completed[0]['candidate_digest'] and candidate[0]['attempt_id'] == completed[0]['attempt_id']
    assert h.sql('SELECT count(*) FROM runtime_session.session_candidates') == [['0']], 'managed Redis Session touched PostgreSQL candidate storage'
    return {'completion': completed[0], 'final': final[0], 'candidate': candidate[0]}
