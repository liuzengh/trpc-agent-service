"""Private MinIO + real owner publication fixture for explicit Artifact tools.

PG/Session/Gateway/Lab lifecycle stays with the existing Harness. S3 requests
here are fixture provisioning or independent object readback, not a replacement
for the Worker artifact.Service under test.
"""
import base64
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
import signal
import subprocess
import threading
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from harness import Harness, _http_status

MINIO_IMAGE = 'minio/minio:RELEASE.2025-04-22T22-12-26Z'
BACKEND_ID = 'joint-artifact-s3'
BUCKET = 'joint-artifacts'
TOOLS = ['artifact_save', 'artifact_load', 'artifact_list', 'artifact_delete']


def signed_s3_headers(endpoint, access_key, secret_key, method, path, query, body, now=None):
    """AWS SigV4 fixture signer; credentials never enter a URL or saved artifact."""
    now = now or datetime.now(timezone.utc)
    stamp, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
    digest = hashlib.sha256(body).hexdigest()
    host = urllib.parse.urlsplit(endpoint).netloc
    canonical_headers = 'host:' + host + '\nx-amz-content-sha256:' + digest + '\nx-amz-date:' + stamp + '\n'
    names = 'host;x-amz-content-sha256;x-amz-date'
    canonical = '\n'.join((method, path, query, canonical_headers, names, digest))
    scope = day + '/us-east-1/s3/aws4_request'
    string_to_sign = '\n'.join(('AWS4-HMAC-SHA256', stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()))
    key = ('AWS4' + secret_key).encode()
    for value in (day, 'us-east-1', 's3', 'aws4_request'):
        key = hmac.new(key, value.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    return {'Host': host, 'X-Amz-Content-Sha256': digest, 'X-Amz-Date': stamp,
            'Authorization': 'AWS4-HMAC-SHA256 Credential=' + access_key + '/' + scope + ', SignedHeaders=' + names + ', Signature=' + signature}


class ArtifactHarness(Harness):
    artifact_backend_id = BACKEND_ID

    def spawn(self, name, argv, env):
        if name == 'control-api':
            self.control_env = dict(env)
        return super().spawn(name, argv, env)

    def record(self, name, value):
        path = self.artifacts / name
        raw = self.redact(json.dumps(value, indent=2, ensure_ascii=False)) + '\n'
        path.write_text(raw)
        assert path.read_text() == raw

    def s3_request(self, method, key=None, *, query=None, body=b'', expected=200):
        path = '/' + BUCKET + ('' if key is None else '/' + urllib.parse.quote(key, safe='/~'))
        encoded_query = urllib.parse.urlencode(sorted((query or {}).items()), quote_via=urllib.parse.quote)
        headers = signed_s3_headers(self.s3_endpoint, self.artifact_access_key, self.artifact_secret_key, method, path, encoded_query, body)
        request = urllib.request.Request(self.s3_endpoint + path + ('?' + encoded_query if encoded_query else ''), data=body if method in ('PUT', 'POST') else None, headers=headers, method=method)
        try:
            response = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            actual, data = response.status, response.read(8 * 1024 * 1024 + 1)
        if actual != expected:
            raise RuntimeError('fixture S3 HTTP status ' + str(actual) + ' expected ' + str(expected))
        if len(data) > 8 * 1024 * 1024:
            raise RuntimeError('fixture S3 response too large')
        return data

    def provision(self):
        super().provision()
        self.provision_minio()

    def provision_minio(self):
        self.artifact_access_key = self.secret(12)
        self.artifact_secret_key = self.secret()
        self.minio = self.prefix + '-artifact-minio'
        self.minio_port = self.port()
        self.s3_endpoint = 'http://127.0.0.1:' + str(self.minio_port)
        self.minio_stopped = False
        env = {'MINIO_ROOT_USER': self.artifact_access_key, 'MINIO_ROOT_PASSWORD': self.artifact_secret_key}
        self.command(['docker', 'run', '-d', '--name', self.minio, '-p', '127.0.0.1:' + str(self.minio_port) + ':9000', '-e', 'MINIO_ROOT_USER', '-e', 'MINIO_ROOT_PASSWORD', MINIO_IMAGE, 'server', '/data', '--address', ':9000'], env=env)
        self.containers.append(self.minio)
        self.wait(lambda: _http_status(self.s3_endpoint + '/minio/health/ready', timeout=1) == 200, 'private MinIO readiness')
        self.s3_request('PUT', expected=200)
        assert self.object_state() == []
        self.s3_config = {'container': self.minio, 'image': MINIO_IMAGE, 'image_id': self.command(['docker', 'inspect', '--format', '{{.Image}}', self.minio]).strip(), 'endpoint': self.s3_endpoint, 'bucket': BUCKET, 'region': 'us-east-1', 'path_style': True, 'versioning': 'disabled', 'initial_object_count': 0}
        self.record('artifact-s3-dependency.json', self.s3_config)

    def object_state(self):
        query = {'list-type': '2'}
        objects = []
        while True:
            xml = ET.fromstring(self.s3_request('GET', query=query))
            ns = {'s': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            for item in xml.findall('s:Contents', ns):
                key = item.findtext('s:Key', namespaces=ns)
                data = self.s3_request('GET', key)
                objects.append({'key': key, 'size': int(item.findtext('s:Size', namespaces=ns)), 'sha256': hashlib.sha256(data).hexdigest(), 'bytes_hex': data.hex()})
                assert objects[-1]['size'] == len(data)
            if xml.findtext('s:IsTruncated', namespaces=ns) != 'true':
                break
            query['continuation-token'] = xml.findtext('s:NextContinuationToken', namespaces=ns)
            assert query['continuation-token']
        return sorted(objects, key=lambda item: item['key'])

    def metadata_state(self):
        files = self.sql("SELECT row_to_json(f)::text FROM worker.worker_artifact_files f ORDER BY scope_id,filename")
        versions = self.sql("SELECT row_to_json(v)::text FROM worker.worker_artifact_versions v ORDER BY file_id,version")
        return {'files': [json.loads(row[0]) for row in files], 'versions': [json.loads(row[0]) for row in versions]}

    def stop_artifact_storage(self):
        assert not self.minio_stopped
        self.command(['docker', 'stop', self.minio])
        self.minio_stopped = True
        self.record('artifact-storage-fault.json', {'operation': 'stop_owned_minio', 'container': self.minio, 'business_rows_modified': False})

    def restore_artifact_storage(self):
        if not getattr(self, 'minio_stopped', False):
            return
        self.command(['docker', 'start', self.minio])
        self.wait(lambda: _http_status(self.s3_endpoint + '/minio/health/ready', timeout=1) == 200, 'MinIO restored at fixed published endpoint')
        self.minio_stopped = False

    def bind_artifact_catalog(self, tenant_id):
        catalog = {'version': 'v1', 'backends': [{'id': BACKEND_ID, 'revision': 1, 'label': 'Joint S3 Artifacts', 'kind': 's3', 'roles': ['artifact'], 'enabled': True, 'tenant_ids': [tenant_id]}]}
        targets = {'version': 'v1', 'backends': [{'backend_id': BACKEND_ID, 'backend_revision': 1, 'kind': 's3', 'adapter': 'managed-s3-v1', 'isolation': 'tenant-artifact-v1', 'limits': {'timeout_ms': 3000, 'max_concurrency': 4, 'max_bytes': 1048576}, 's3': {'endpoint': self.s3_endpoint, 'bucket': BUCKET, 'region': 'us-east-1', 'path_style': True, 'versioning': 'disabled'}}]}
        env = dict(self.control_env)
        for suffix, value in [('CATALOG', catalog), ('TARGETS', targets)]:
            path = Path(self.write('artifact-' + suffix.lower() + '.json', value))
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_FILE'] = str(path)
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_SHA256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        env.pop('CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST', None)
        self.contract_digest = self.command([self.binaries['control-api'], '--print-deployment-contract-digest'], env=env).strip()
        env['CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST']=self.contract_digest
        self.control.send_signal(signal.SIGTERM); self.control.wait(timeout=25)
        assert self.control.returncode == 0
        self.control = self.spawn('control-api', [self.binaries['control-api']], env)
        self.wait(lambda: _http_status(self.urls['control'] + '/healthz', timeout=1) == 204, 'Control fixed Artifact catalog restart')
        self.record('artifact-backend-config.json', {'catalog': catalog, 'targets': targets, 'contract_digest': self.contract_digest})

    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['spec']['requirements']['models']['primary']['capabilities'] = ['chat', 'tool_call']
            body['spec']['nodes']['assistant']['artifact'] = {'enabled': True}
        if method == 'PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['config']['models']['primary']['capabilities'] = ['chat', 'tool_call']
            body['config']['storage']['artifact'] = {'kind': 'managed_artifact', 'backend_id': BACKEND_ID, 'backend_revision': 1}
            body['credentials']['storage']['artifact'] = {'access_key_id': {'action': 'replace', 'value': self.artifact_access_key}, 'secret_access_key': {'action': 'replace', 'value': self.artifact_secret_key}}
        result = super().api(method, path, body, status, idem)
        if method == 'POST' and path == '/v1/admin/tenants':
            self.bind_artifact_catalog(result['id'])
        return result

    def close(self):
        errors = []
        try:
            self.restore_artifact_storage()
        except BaseException as exc:
            errors.append(exc)
        try:
            super().close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            name = getattr(self, 'minio', None)
            if name:
                inspection = subprocess.run(['docker', 'inspect', name], capture_output=True, text=True)
                if inspection.returncode == 0:
                    removed = subprocess.run(['docker', 'rm', '-f', name], capture_output=True)
                    if removed.returncode:
                        errors.append(RuntimeError('MinIO removal failed'))
                    inspection = subprocess.run(['docker', 'inspect', name], capture_output=True, text=True)
                gone = inspection.returncode != 0 and ('No such object: ' + name) in inspection.stderr
                if not gone:
                    errors.append(RuntimeError('MinIO removal unverified'))
                self.record('artifact-s3-cleanup.json', {'result': 'FAIL' if errors else 'PASS', 'removed': gone, 'private_directory_removed': not self.work.exists()})
        if errors:
            raise RuntimeError('; '.join(self.redact(str(e)) for e in errors))


CONTENT_V0 = b'Artifact report version zero\n\x00\xff'
CONTENT_V1 = 'Artifact report version one: 正式文件\n'.encode()
CONTENT_OTHER = b'isolated other Session bytes\n'
CONTENT_SAVED_BEFORE_FAILURE = b'saved before model failure: not rolled back\n'
CONTENT_RECOVERED = b'backend recovered bytes\n'


def save_args(name, content):
    return {'name': name, 'content_base64': base64.b64encode(content).decode(), 'mime_type': 'application/octet-stream'}


def assert_artifact(value, name, version, content, *, loaded=False, mime_type='application/octet-stream'):
    assert value['name'] == name and value['version'] == version
    assert value['mime_type'] == mime_type
    assert value['size_bytes'] == len(content)
    assert value['sha256'] == hashlib.sha256(content).hexdigest()
    assert value['ref'].startswith('artifact:') and value['ref'].endswith(':' + name + ':' + str(version))
    if loaded:
        assert base64.b64decode(value['content_base64'], validate=True) == content


class ArtifactModelFixture:
    """Deterministic HTTP model only; four real SDK tools perform all operations."""
    def __init__(self, h):
        self.key = h.secret()
        self.requests, self.outputs, self.errors = [], [], []
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                if self.path != '/v1/chat/completions':
                    self.send_error(404); return
                if self.headers.get('Authorization') != 'Bearer ' + owner.key:
                    self.send_error(401); return
                try:
                    self.respond()
                except (AssertionError, KeyError, ValueError, TypeError) as exc:
                    with owner.lock:
                        owner.errors.append(type(exc).__name__ + ': fixture contract mismatch')
                    self.send_error(400, 'artifact fixture contract mismatch')
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def respond(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                with owner.lock:
                    owner.requests.append(copy.deepcopy(request))
                    number = len(owner.requests)
                assert request['model'] == h.model_name
                assert sorted(t['function']['name'] for t in request.get('tools', [])) == sorted(TOOLS)
                messages = request['messages']
                start = max(i for i, m in enumerate(messages) if m.get('role') == 'user')
                mode = messages[start]['content']
                results = []
                for message in messages[start + 1:]:
                    if message.get('role') == 'tool':
                        try:
                            results.append(json.loads(message['content']))
                        except ValueError:
                            results.append({'error': message['content']})
                step = len(results)
                plans = {
                    'artifact-v0': [('artifact_save', save_args('report.bin', CONTENT_V0)), ('artifact_load', {'name': 'report.bin', 'version': 0}), ('artifact_list', {})],
                    'artifact-v1': [('artifact_save', save_args('report.bin', CONTENT_V1)), ('artifact_load', {'name': 'report.bin', 'version': 0}), ('artifact_load', {'name': 'report.bin', 'version': 1}), ('artifact_list', {})],
                    'artifact-isolated': [('artifact_list', {}), ('artifact_load', {'name': 'report.bin'}), ('artifact_save', save_args('report.bin', CONTENT_OTHER)), ('artifact_load', {'name': 'report.bin'})],
                    'artifact-model-fail': [('artifact_save', save_args('saved-before-failure.bin', CONTENT_SAVED_BEFORE_FAILURE))],
                    'artifact-read-after-failure': [('artifact_load', {'name': 'saved-before-failure.bin'})],
                    'artifact-storage-fail': [('artifact_save', save_args('must-not-save.bin', b'backend unavailable'))],
                    'artifact-recovered': [('artifact_save', save_args('recovered.bin', CONTENT_RECOVERED)), ('artifact_load', {'name': 'recovered.bin'})],
                    'artifact-delete': [('artifact_delete', {'name': 'report.bin'}), ('artifact_load', {'name': 'report.bin'}), ('artifact_list', {})],
                }
                if mode == 'artifact-gui-read':
                    expected = h.gui_artifact_expected
                    assert expected['name'] == 'gui-upload.txt' and expected['version'] == 0
                    plans[mode] = [('artifact_load', {'name': expected['name'], 'version': expected['version']})]
                assert mode in plans
                if mode == 'artifact-model-fail' and step:
                    assert_artifact(results[0], 'saved-before-failure.bin', 0, CONTENT_SAVED_BEFORE_FAILURE)
                    with owner.lock:
                        owner.outputs.append({'input': mode, 'status': 401, 'tool_results': results})
                    self.send_error(401, 'fixture model fails after actual save'); return
                if mode == 'artifact-storage-fail' and step == 0:
                    h.stop_artifact_storage()
                name, args = plans[mode][step] if step < len(plans[mode]) else (None, None)
                if not name:
                    if mode == 'artifact-v0':
                        assert_artifact(results[0], 'report.bin', 0, CONTENT_V0)
                        assert_artifact(results[1], 'report.bin', 0, CONTENT_V0, loaded=True)
                        assert results[0]['ref'] == results[1]['ref'] and results[2]['keys'] == ['report.bin']
                    elif mode == 'artifact-v1':
                        assert_artifact(results[0], 'report.bin', 1, CONTENT_V1)
                        assert_artifact(results[1], 'report.bin', 0, CONTENT_V0, loaded=True)
                        assert_artifact(results[2], 'report.bin', 1, CONTENT_V1, loaded=True)
                        assert results[0]['ref'] == results[2]['ref'] and results[3]['keys'] == ['report.bin']
                    elif mode == 'artifact-isolated':
                        assert results[0] == {'keys': []} and results[1] == {'found': False, 'name': 'report.bin'}
                        assert_artifact(results[2], 'report.bin', 0, CONTENT_OTHER)
                        assert_artifact(results[3], 'report.bin', 0, CONTENT_OTHER, loaded=True)
                    elif mode == 'artifact-read-after-failure':
                        assert_artifact(results[0], 'saved-before-failure.bin', 0, CONTENT_SAVED_BEFORE_FAILURE, loaded=True)
                    elif mode == 'artifact-recovered':
                        assert_artifact(results[0], 'recovered.bin', 0, CONTENT_RECOVERED)
                        assert_artifact(results[1], 'recovered.bin', 0, CONTENT_RECOVERED, loaded=True)
                    elif mode == 'artifact-delete':
                        assert results[0] == {'deleted': True, 'name': 'report.bin'}
                        assert results[1] == {'found': False, 'name': 'report.bin'}
                        assert sorted(results[2]['keys']) == ['recovered.bin', 'saved-before-failure.bin']
                    elif mode == 'artifact-gui-read':
                        expected = h.gui_artifact_expected
                        data = expected['content'].encode() if isinstance(expected['content'], str) else expected['content']
                        assert_artifact(results[0], expected['name'], expected['version'], data, loaded=True, mime_type=expected.get('mime_type', 'text/plain'))
                    elif mode == 'artifact-storage-fail':
                        assert results[0].get('error'), 'backend error must return a tool error, not successful metadata'
                    with owner.lock:
                        owner.outputs.append({'input': mode, 'text': 'artifact final: ' + mode, 'tool_results': results})
                if name:
                    delta = {'role': 'assistant', 'tool_calls': [{'index': 0, 'id': 'artifact-call-' + str(number), 'type': 'function', 'function': {'name': name, 'arguments': json.dumps(args)}}]}
                    finish = 'tool_calls'
                else:
                    # Backend error tries a normal model Final: sticky Worker error
                    # must reject it rather than accepting this claim of success.
                    delta, finish = {'role': 'assistant', 'content': 'artifact final: ' + mode}, 'stop'
                def chunk(delta, finish, usage=None):
                    event = {'id': 'artifact-response-' + str(number), 'object': 'chat.completion.chunk', 'created': 1, 'model': request['model'], 'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]}
                    if usage:
                        event['usage'] = usage
                    return 'data: ' + json.dumps(event) + '\n\n'
                payload = (chunk(delta, None) + chunk({}, finish, {'prompt_tokens': 7, 'completion_tokens': 3, 'total_tokens': 10}) + 'data: [DONE]\n\n').encode()
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.requests)

    def output_snapshot(self):
        with self.lock:
            return copy.deepcopy(self.outputs)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()
        assert not self.errors, self.errors
