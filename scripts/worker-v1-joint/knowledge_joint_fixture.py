"""Real scoped Qdrant fixture; deterministic embedding is explicitly not live-provider proof."""
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import threading
import urllib.error
import urllib.request

from harness import Harness, _http_status

BACKEND_ID = 'joint-knowledge-qdrant'
COLLECTION = 'joint_knowledge'
VECTOR_NAME = 'published_dense'
DIMENSIONS = 3
EMBEDDING_MODEL = 'joint-embedding-fixture'
QDRANT_IMAGE = 'qdrant/qdrant:v1.15.4'
CALLABLE_NAME = 'fn_' + hashlib.sha256(b'knowledge/docs').hexdigest()[:60]
DOCUMENT_NAME = 'orchid-reference.txt'
DOCUMENT_TEXT = 'The formal knowledge canary is ORCHID-739. The orchid is violet, and the service window starts at 09:17 UTC.'
OTHER_TEXT = 'A separate Profile knowledge canary is MARIGOLD-284. It must not replace the original orchid knowledge.'


class KnowledgeHarness(Harness):
    knowledge_backend_id = BACKEND_ID

    def spawn(self, name, argv, env):
        if name == 'control-api': self.control_env = dict(env)
        return super().spawn(name, argv, env)

    def record(self, name, value):
        raw = self.redact(json.dumps(value, indent=2, ensure_ascii=False)) + '\n'
        path = self.artifacts / name
        path.write_text(raw)
        assert path.read_text() == raw

    def qdrant_request(self, method, path, body=None, expected=200):
        request = urllib.request.Request(self.qdrant_endpoint + path, data=None if body is None else json.dumps(body).encode(), method=method, headers={'Content-Type': 'application/json', 'api-key': self.qdrant_key})
        try:
            response = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            status, raw = response.status, response.read(8 * 1024 * 1024 + 1)
        if status != expected: raise RuntimeError('fixture Qdrant HTTP status ' + str(status))
        if len(raw) > 8 * 1024 * 1024: raise RuntimeError('fixture Qdrant response exceeded readback bound')
        return json.loads(raw)

    def provision(self):
        super().provision()
        self.qdrant_key = self.secret()
        self.qdrant = self.prefix + '-knowledge-qdrant'
        self.qdrant_port = self.port()
        self.qdrant_endpoint = 'http://127.0.0.1:' + str(self.qdrant_port)
        self.qdrant_stopped = False
        self.command(['docker', 'run', '-d', '--name', self.qdrant, '-p', '127.0.0.1:' + str(self.qdrant_port) + ':6333', '-e', 'QDRANT__SERVICE__API_KEY', QDRANT_IMAGE], env={'QDRANT__SERVICE__API_KEY': self.qdrant_key})
        self.containers.append(self.qdrant)
        self.wait(lambda: self.qdrant_request('GET', '/collections'), 'private Qdrant readiness')
        self.qdrant_request('PUT', '/collections/' + COLLECTION, {'vectors': {VECTOR_NAME: {'size': DIMENSIONS, 'distance': 'Cosine'}}})
        config = self.qdrant_request('GET', '/collections/' + COLLECTION)
        assert config['result']['config']['params']['vectors'][VECTOR_NAME]['size'] == DIMENSIONS
        self.qdrant_config = {'endpoint': self.qdrant_endpoint, 'container': self.qdrant, 'image': QDRANT_IMAGE, 'image_id': self.command(['docker', 'inspect', '--format', '{{.Image}}', self.qdrant]).strip(), 'collection': COLLECTION, 'vector_name': VECTOR_NAME, 'dimensions': DIMENSIONS, 'distance': 'cosine', 'actual_collection': config}
        self.record('knowledge-qdrant-dependency.json', self.qdrant_config)

    def knowledge_state(self):
        points, offset = [], None
        while True:
            body = {'limit': 100, 'with_payload': True, 'with_vector': True}
            if offset is not None: body['offset'] = offset
            result = self.qdrant_request('POST', '/collections/' + COLLECTION + '/points/scroll', body)['result']
            points.extend(result['points'])
            offset = result.get('next_page_offset')
            if offset is None: break
        return sorted(points, key=lambda point: str(point['id']))

    def stop_knowledge_storage(self):
        assert not self.qdrant_stopped
        self.command(['docker', 'stop', self.qdrant])
        self.qdrant_stopped = True
        self.record('knowledge-qdrant-fault.json', {'operation': 'stop_owned_qdrant', 'container': self.qdrant, 'business_state_modified': False})

    def restore_knowledge_storage(self):
        if not getattr(self, 'qdrant_stopped', False): return
        self.command(['docker', 'start', self.qdrant])
        self.wait(lambda: self.qdrant_request('GET', '/collections'), 'restored fixed Qdrant endpoint')
        self.qdrant_stopped = False

    def bind_knowledge_catalog(self, tenant_id):
        catalog = {'version': 'v1', 'backends': [{'id': BACKEND_ID, 'revision': 1, 'label': 'Joint Qdrant Knowledge', 'kind': 'qdrant', 'roles': ['knowledge'], 'enabled': True, 'tenant_ids': [tenant_id]}]}
        targets = {'version': 'v1', 'backends': [{'backend_id': BACKEND_ID, 'backend_revision': 1, 'kind': 'qdrant', 'adapter': 'managed-qdrant-v1', 'isolation': 'tenant-profile-resource-v1', 'limits': {'timeout_ms': 5000, 'max_concurrency': 4, 'max_bytes': 65536}, 'qdrant': {'endpoint': self.qdrant_endpoint, 'collection': COLLECTION, 'vector_name': VECTOR_NAME, 'dimensions': DIMENSIONS, 'distance': 'cosine'}}]}
        env = dict(self.control_env)
        for suffix, value in [('CATALOG', catalog), ('TARGETS', targets)]:
            path = Path(self.write('knowledge-' + suffix.lower() + '.json', value))
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_FILE'] = str(path)
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_SHA256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        env.pop('CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST', None)
        self.contract_digest = self.command([self.binaries['control-api'], '--print-deployment-contract-digest'], env=env).strip()
        env['CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST'] = self.contract_digest
        self.control.send_signal(signal.SIGTERM); self.control.wait(timeout=25)
        assert self.control.returncode == 0
        self.control = self.spawn('control-api', [self.binaries['control-api']], env)
        self.wait(lambda: _http_status(self.urls['control'] + '/healthz', timeout=1) == 204, 'Control fixed Knowledge catalog restart')
        self.record('knowledge-backend-config.json', {'catalog': catalog, 'targets': targets, 'contract_digest': self.contract_digest})

    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['spec']['requirements']['models']['primary']['capabilities'] = ['chat', 'tool_call']
            body['spec']['requirements']['knowledge']['docs'] = {'capability': 'knowledge.search'}
            body['spec']['nodes']['assistant']['knowledge_slots'] = ['docs']
        if method == 'PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['config']['models']['primary']['capabilities'] = ['chat', 'tool_call']
            body['config']['knowledge']['docs'] = {'kind': 'managed_knowledge', 'backend_id': BACKEND_ID, 'backend_revision': 1, 'embedding': {'model': self.model.embedding_model, 'base_url': self.model.url + '/v1', 'dimensions': self.model.dimensions}}
            body['credentials'].setdefault('knowledge', {})['docs'] = {'qdrant_api_key': {'action': 'replace', 'value': self.qdrant_key}, 'embedding_api_key': {'action': 'replace', 'value': self.model.embedding_key}}
            self.last_profile_write = copy.deepcopy(body)
        result = super().api(method, path, body, status, idem)
        if method == 'POST' and path == '/v1/admin/tenants': self.bind_knowledge_catalog(result['id'])
        return result

    def import_text(self, name, text, *, revision=1, resource='docs', expected=200):
        path = '/v1/tenants/' + self.tenant_id + '/deployments/' + self.deployment_id + '/revisions/' + str(revision) + '/knowledge/' + resource + '/import'
        return self.api('POST', path, {'name': name, 'text': text}, status=expected)

    def import_failure(self, name, text, *, revision=1):
        """Same authenticated public API; keep non-2xx evidence without hiding 404/401."""
        path = '/v1/tenants/' + self.tenant_id + '/deployments/' + self.deployment_id + '/revisions/' + str(revision) + '/knowledge/docs/import'
        request = urllib.request.Request(self.urls['control'] + path, data=json.dumps({'name': name, 'text': text}).encode(), method='POST', headers={'Content-Type': 'application/json'})
        try:
            response = self.opener.open(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            status, raw = response.status, response.read(65537)
        assert len(raw) <= 65536
        assert status == 503, 'dependency failure must have the published 503 mapping, not 2xx/auth/404'
        return {'http_status': status, 'response': json.loads(self.redact(raw.decode()))}

    def close(self):
        errors = []
        try: self.restore_knowledge_storage()
        except BaseException as exc: errors.append(exc)
        try: super().close()
        except BaseException as exc: errors.append(exc)
        finally:
            name = getattr(self, 'qdrant', None)
            if name:
                inspection = subprocess.run(['docker', 'inspect', name], capture_output=True, text=True)
                if inspection.returncode == 0:
                    removed = subprocess.run(['docker', 'rm', '-f', name], capture_output=True)
                    if removed.returncode: errors.append(RuntimeError('Qdrant removal failed'))
                    inspection = subprocess.run(['docker', 'inspect', name], capture_output=True, text=True)
                gone = inspection.returncode != 0 and ('No such object: ' + name) in inspection.stderr
                if not gone: errors.append(RuntimeError('Qdrant removal unverified'))
                self.record('knowledge-qdrant-cleanup.json', {'result': 'FAIL' if errors else 'PASS', 'removed': gone, 'private_directory_removed': not self.work.exists()})
        if errors: raise RuntimeError('; '.join(self.redact(str(e)) for e in errors))


class KnowledgeModelFixture:
    """Provider fixture only: SDK owns reader/chunker/embedder/knowledge_search."""
    embedding_model = EMBEDDING_MODEL
    dimensions = DIMENSIONS

    def __init__(self, h):
        self.key, self.embedding_key = h.secret(), h.secret()
        self.requests, self.embedding_requests, self.outputs, self.errors = [], [], [], []
        self.fail_embedding = False
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                expected = owner.embedding_key if self.path == '/v1/embeddings' else owner.key
                if self.headers.get('Authorization') != 'Bearer ' + expected:
                    self.send_error(401); return
                try: self.respond()
                except (AssertionError, KeyError, ValueError, TypeError) as exc:
                    with owner.lock: owner.errors.append(type(exc).__name__ + ': fixture contract mismatch')
                    self.send_error(400, 'knowledge fixture contract mismatch')
                except (BrokenPipeError, ConnectionResetError): pass
            def respond(self):
                req = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                if self.path == '/v1/embeddings':
                    with owner.lock:
                        owner.embedding_requests.append(copy.deepcopy(req))
                        fail = owner.fail_embedding
                    assert req['model'] == owner.embedding_model and req['dimensions'] == owner.dimensions
                    value = {'object': 'list', 'model': req['model'], 'data': [{'object': 'embedding', 'index': 0, 'embedding': [] if fail else [1.0, 0.0, 0.0]}], 'usage': {'prompt_tokens': 7, 'total_tokens': 7}}
                    payload = json.dumps(value).encode()
                    self.send_response(200); self.send_header('Content-Type', 'application/json'); self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
                    return
                if self.path != '/v1/chat/completions': self.send_error(404); return
                with owner.lock:
                    owner.requests.append(copy.deepcopy(req)); number = len(owner.requests)
                assert req['model'] == h.model_name
                assert [t['function']['name'] for t in req.get('tools', [])] == [CALLABLE_NAME]
                messages = req['messages']; start = max(i for i,m in enumerate(messages) if m.get('role') == 'user'); mode = messages[start]['content']
                results = []
                for m in messages[start+1:]:
                    if m.get('role') == 'tool':
                        try: results.append(json.loads(m['content']))
                        except ValueError: results.append({'error':m['content']})
                assert mode in ('knowledge-query', 'knowledge-again', 'knowledge-model-fail', 'knowledge-empty', 'knowledge-other', 'knowledge-gui-query')
                if results:
                    if mode == 'knowledge-empty':
                        assert not results[0].get('documents')
                        assert 'ORCHID-739' not in json.dumps(results) and 'MARIGOLD-284' not in json.dumps(results)
                        final = 'knowledge final: empty profile scope'
                    elif mode == 'knowledge-gui-query':
                        expected = h.gui_knowledge_expected['text']
                        assert isinstance(expected, str) and expected.strip()
                        assert expected in [d['text'] for d in results[0]['documents']]
                        final = 'knowledge final: ' + expected
                    else:
                        expected = OTHER_TEXT if mode == 'knowledge-other' else DOCUMENT_TEXT
                        assert expected in [d['text'] for d in results[0]['documents']]
                        forbidden = 'ORCHID-739' if mode == 'knowledge-other' else 'MARIGOLD-284'
                        assert forbidden not in json.dumps(results)
                        final = 'knowledge final: ' + expected
                    if mode == 'knowledge-model-fail':
                        with owner.lock: owner.outputs.append({'input':mode,'status':401,'tool_results':results})
                        self.send_error(401, 'fixture model error after retrieval'); return
                    delta, finish = {'role':'assistant','content':final}, 'stop'
                    with owner.lock: owner.outputs.append({'input':mode,'text':final,'tool_results':results})
                else:
                    delta = {'role':'assistant','tool_calls':[{'index':0,'id':'knowledge-call-'+str(number),'type':'function','function':{'name':CALLABLE_NAME,'arguments':json.dumps({'query':'What is the formal knowledge canary and its service window?'})}}]}
                    finish = 'tool_calls'
                def chunk(delta, finish, usage=None):
                    event={'id':'knowledge-response-'+str(number),'object':'chat.completion.chunk','created':1,'model':req['model'],'choices':[{'index':0,'delta':delta,'finish_reason':finish}]}
                    if usage: event['usage']=usage
                    return 'data: '+json.dumps(event)+'\n\n'
                payload=(chunk(delta,None)+chunk({},finish,{'prompt_tokens':7,'completion_tokens':3,'total_tokens':10})+'data: [DONE]\n\n').encode()
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.url='http://127.0.0.1:'+str(self.server.server_port)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()

    def snapshot(self):
        with self.lock: return copy.deepcopy(self.requests)
    def embeddings(self):
        with self.lock: return copy.deepcopy(self.embedding_requests)
    def output_snapshot(self):
        with self.lock: return copy.deepcopy(self.outputs)
    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join(timeout=5)
        assert not self.thread.is_alive()
        assert not self.errors,self.errors
