"""Redis variant of the existing real Control/SDK/Session/Channel Lab fixture.

Only dependency provisioning and read-only acceptance evidence differ. The model
fixture, public API routes, Worker executor and Gateway/Lab are unchanged.
"""
import copy
import hashlib
import json
from pathlib import Path
import signal
import socket
import subprocess

from harness import Harness, _http_status
from memory_joint_fixture import MemoryHarness, TOOLS

BACKEND_ID = 'joint-memory-redis'
REDIS_IMAGE = 'redis:7.4-alpine'
WRITE_FAILURE = 'REVOKE INSERT ON runtime_memory.memory_receipts FROM memory_runtime'
WRITE_RESTORE = 'GRANT INSERT ON runtime_memory.memory_receipts TO memory_runtime'


class RedisCommandError(RuntimeError):
    pass


def _reply(reader):
    line = reader.readline(1048577)
    if len(line) > 1048576 or not line.endswith(b'\r\n'):
        raise RuntimeError('Redis fixture malformed or oversized response')
    prefix, value = line[:1], line[1:-2]
    if prefix == b'-':
        # Do not include arbitrary server text or command arguments in errors.
        raise RedisCommandError('Redis fixture command rejected')
    if prefix == b'+':
        return value.decode()
    if prefix == b':':
        return int(value)
    if prefix == b'$':
        size = int(value)
        if size == -1:
            return None
        if not 0 <= size <= 8 * 1048576:
            raise RuntimeError('Redis fixture oversized bulk response')
        data = reader.read(size + 2)
        if len(data) != size + 2 or not data.endswith(b'\r\n'):
            raise RuntimeError('Redis fixture truncated bulk response')
        return data[:-2].decode()
    if prefix == b'*':
        count = int(value)
        if not 0 <= count <= 10000:
            raise RuntimeError('Redis fixture oversized array response')
        return [_reply(reader) for _ in range(count)]
    raise RuntimeError('Redis fixture unexpected response type')


def redis_command(host, port, username, password, *args):
    """Short-lived RESP2 fixture connection; no credentials in argv or files."""
    def send(sock, reader, values):
        parts = [str(value).encode() for value in values]
        sock.sendall(b'*' + str(len(parts)).encode() + b'\r\n' + b''.join(b'$' + str(len(value)).encode() + b'\r\n' + value + b'\r\n' for value in parts))
        return _reply(reader)
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.settimeout(5)
        with sock.makefile('rb') as reader:
            if send(sock, reader, ('AUTH', username, password)) != 'OK':
                raise RuntimeError('Redis fixture authentication did not succeed')
            return send(sock, reader, args)


class RedisMemoryHarness(MemoryHarness):
    memory_backend_id = BACKEND_ID

    def provision(self):
        # Bypass only MemoryHarness's PostgreSQL Memory provisioning. Formal
        # Worker ledger/Session/Control/Gateway still use the original PG fixture.
        Harness.provision(self)
        self.memory_password = self.secret()
        self.redis_admin_password = self.secret()
        self.redis = self.prefix + '-memory-redis'
        self.redis_faults = []
        acl = '\n'.join(('user default off',
            'user fixture_admin on >' + self.redis_admin_password + ' ~* +@all',
            'user memory_runtime on >' + self.memory_password + ' ~runtime_memory:* +@connection +mget +get +type +pttl +eval +mset', ''))
        acl_file = self.write('redis-users.acl', acl, mode=0o600)
        # Docker may assign a new dynamic published port after restart. Reserve
        # an explicit loopback port so the immutable Manifest target stays fixed.
        self.redis_port = self.port()
        self.command(['docker', 'run', '--rm', '-d', '--name', self.redis, '--user', '0:0', '-p', '127.0.0.1:' + str(self.redis_port) + ':6379', '-v', acl_file + ':/run/users.acl:ro', REDIS_IMAGE, 'redis-server', '--aclfile', '/run/users.acl', '--appendonly', 'yes', '--appendfsync', 'always', '--maxmemory-policy', 'noeviction', '--save', ''])
        self.containers.append(self.redis)
        assert int(self.command(['docker', 'port', self.redis, '6379/tcp']).strip().rsplit(':', 1)[1]) == self.redis_port
        self.wait(lambda: self.redis_admin('PING') == 'PONG', 'isolated Redis readiness')
        config = self.redis_admin('CONFIG', 'GET', 'appendonly', 'appendfsync', 'maxmemory-policy')
        config = dict(zip(config[::2], config[1::2]))
        assert config == {'appendonly': 'yes', 'appendfsync': 'always', 'maxmemory-policy': 'noeviction'}
        assert self.redis_admin('DBSIZE') == 0
        self.redis_config = {'container': self.redis, 'image': REDIS_IMAGE, 'image_id': self.command(['docker', 'inspect', '--format', '{{.Image}}', self.redis]).strip(), 'host': '127.0.0.1', 'port': self.redis_port, 'database': 0, 'username': 'memory_runtime', 'tls': False, 'configuration': config, 'acl_file_mode': oct(Path(acl_file).stat().st_mode & 0o777), 'initial_dbsize': 0}
        self._record('redis-dependency.json', self.redis_config)

    def _record(self, name, body):
        path = self.artifacts / name
        value = self.redact(json.dumps(body, indent=2, ensure_ascii=False)) + '\n'
        path.write_text(value)
        assert path.read_text() == value

    def redis_admin(self, *args):
        return redis_command('127.0.0.1', self.redis_port, 'fixture_admin', self.redis_admin_password, *args)

    def bind_memory_catalog(self, tenant_id):
        catalog = {'version': 'v1', 'backends': [{'id': BACKEND_ID, 'revision': 1, 'label': 'Joint Redis Memory', 'kind': 'redis', 'roles': ['memory'], 'enabled': True, 'tenant_ids': [tenant_id]}]}
        targets = {'version': 'v1', 'backends': [{'backend_id': BACKEND_ID, 'backend_revision': 1, 'kind': 'redis', 'adapter': 'managed-redis-v1', 'isolation': 'tenant-subject-agent-v1', 'limits': {'timeout_ms': 5000, 'max_concurrency': 4, 'max_bytes': 1048576}, 'redis': {'host': '127.0.0.1', 'port': self.redis_port, 'database': 0, 'username': 'memory_runtime', 'tls': False}}]}
        env = dict(self.control_env)
        for suffix, value in [('CATALOG', catalog), ('TARGETS', targets)]:
            path = Path(self.write('memory-' + suffix.lower() + '.json', value))
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_FILE'] = str(path)
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_SHA256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        env.pop('CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST', None)
        self.contract_digest = self.command([self.binaries['control-api'], '--print-deployment-contract-digest'], env=env).strip()
        env['CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST'] = self.contract_digest
        self.control.send_signal(signal.SIGTERM)
        self.control.wait(timeout=25)
        assert self.control.returncode == 0
        self.control = self.spawn('control-api', [self.binaries['control-api']], env)
        self.wait(lambda: _http_status(self.urls['control'] + '/healthz', timeout=1) == 204, 'Control pinned Redis Memory catalog restart')
        self._record('memory-backend-config.json', {'catalog': catalog, 'targets': targets, 'contract_digest': self.contract_digest})

    def api(self, method, path, body=None, status=200, idem=None):
        # Same public Agent/Profile API selection as MemoryHarness, with the
        # backend identifier changed, not a hand-written Manifest or runtime Plan.
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['spec']['requirements']['models']['primary']['capabilities'] = ['chat', 'tool_call']
            body['spec']['nodes']['assistant']['memory'] = {'tools': list(TOOLS), 'preload_limit': 0}
        if method == 'PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['config']['models']['primary']['capabilities'] = ['chat', 'tool_call']
            body['config']['storage']['memory'] = {'kind': 'managed_memory', 'backend_id': BACKEND_ID, 'backend_revision': 1}
            body['credentials']['storage']['memory'] = {'dsn_password': {'action': 'replace', 'value': self.memory_password}}
        result = Harness.api(self, method, path, body, status, idem)
        if method == 'POST' and path == '/v1/admin/tenants':
            self.bind_memory_catalog(result['id'])
        return result

    def admin_sql(self, statement):
        # Reuse the original deterministic model's explicit failure hook. Only
        # these two exact fault/provision statements map to a Redis ACL change.
        # All other SQL keeps the original owner and is never rewritten.
        if statement in (WRITE_FAILURE, WRITE_RESTORE):
            disabled = statement == WRITE_FAILURE
            assert self.redis_admin('ACL', 'SETUSER', 'memory_runtime', '-mset' if disabled else '+mset') == 'OK'
            self.redis_faults.append({'operation': 'disable_runtime_mset' if disabled else 'restore_runtime_mset', 'result': 'OK', 'database_state_modified': False})
            self._record('redis-acl-faults.json', self.redis_faults)
            return 'OK'
        return super().admin_sql(statement)

    def storage_state(self):
        cursor, keys = '0', []
        while True:
            cursor, batch = self.redis_admin('SCAN', cursor, 'MATCH', 'runtime_memory:*', 'COUNT', 100)
            keys.extend(batch)
            if cursor == '0':
                break
        assert len(keys) == len(set(keys))
        state = []
        for key in sorted(keys):
            raw = self.redis_admin('GET', key)
            ttl = self.redis_admin('PTTL', key)
            assert ttl == -1, 'formal Redis Memory key unexpectedly expires'
            value = json.loads(raw)
            content = json.loads(value['body'])
            assert value['digest'] == 'sha256:' + hashlib.sha256(value['body'].encode()).hexdigest()
            assert int(value['revision']) == content['base_revision'] + 1
            state.append({'key': key, 'pttl': ttl, 'raw': raw, 'record': value})
        return state

    def memory_state(self):
        rows = []
        for row in self.storage_state():
            if ':head:' not in row['key']:
                continue
            record = row['record']
            content = json.loads(record['body'])
            rows.append({'tenant_id': content['scope']['tenant_id'], 'scope_id': content['scope']['scope_id'], 'revision': int(record['revision']), 'digest': record['digest'], 'content': content})
        return sorted(rows, key=lambda row: (row['tenant_id'], row['scope_id']))

    def acl_failures(self):
        out = []
        for raw in self.redis_admin('ACL', 'LOG'):
            item = dict(zip(raw[::2], raw[1::2]))
            out.append({key: item[key] for key in ('count', 'reason', 'context', 'object', 'username')})
        return out

    def close(self):
        redis_name = getattr(self, 'redis', None)
        error = None
        try:
            super().close()
        except BaseException as exc:
            error = exc
        finally:
            if redis_name:
                check = subprocess.run(['docker', 'inspect', redis_name], text=True, capture_output=True)
                # Rescue the owned Redis even if another dependency's cleanup
                # aborted first. Do not hide that original cleanup failure.
                if check.returncode == 0:
                    removed_result = subprocess.run(['docker', 'rm', '-f', redis_name], text=True, capture_output=True)
                    if removed_result.returncode != 0 and error is None:
                        error = RuntimeError('Redis container removal failed')
                    check = subprocess.run(['docker', 'inspect', redis_name], text=True, capture_output=True)
                # A disconnected Docker daemon is not evidence of deletion.
                removed = check.returncode != 0 and ('No such object: ' + redis_name) in check.stderr
                if removed:
                    (self.work / 'redis-users.acl').unlink(missing_ok=True)
                self._record('redis-cleanup.json', {'result': 'PASS' if removed and error is None else 'FAIL', 'container': redis_name, 'removed': removed, 'private_directory_removed': not self.work.exists()})
                if not removed and error is None:
                    error = RuntimeError('Redis container cleanup unverified')
            if error is not None:
                raise error
