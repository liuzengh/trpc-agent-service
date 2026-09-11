"""Parallel Artifact owner/API fixture; real SDK persists into owned MinIO + PG."""
import copy
import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from artifact_joint_fixture import ArtifactHarness, TOOLS, save_args, assert_artifact
from harness import Harness

CASES = ('parallel-artifact-independent', 'parallel-artifact-same', 'parallel-artifact-failure')
ROLES = ('a', 'b', 'aggregate')


def artifact_plan(case, role):
    assert case in CASES and role in ('a', 'b')
    name = 'shared.bin' if case == CASES[1] else ('saved-before-failure.bin' if case == CASES[2] else role + '.bin')
    return name, (case + ':' + role + ':正式bytes\n').encode() + b'\x00\xff'


def tree_spec(spec):
    spec = copy.deepcopy(spec)
    spec['root'] = 'workflow'
    spec['requirements']['models']['primary']['capabilities'] = ['chat', 'tool_call']
    spec['nodes'] = {'workflow': {'kind': 'sequence', 'children': ['parallel', 'aggregate']},
                     'parallel': {'kind': 'parallel', 'children': ['a', 'b']}}
    for role in ROLES:
        node = {'kind': 'llm', 'model_slot': 'primary', 'instruction': 'PAR_ARTIFACT_' + role.upper() + ': execute this fixed role.', 'tool_slots': [], 'knowledge_slots': []}
        if role != 'aggregate': node['artifact'] = {'enabled': True}
        spec['nodes'][role] = node
    return spec


class ParallelArtifactHarness(ArtifactHarness):
    def s3_request(self, method, key=None, *, query=None, body=b'', expected=200):
        if method == 'PUT' and key is None:
            # Private fixture provisioning only. MinIO's health endpoint may
            # report ready before authenticated S3 API initialization completes.
            def initialized():
                super(ParallelArtifactHarness, self).s3_request('GET', expected=404)
                return True
            self.wait(initialized, 'private S3 API initialized before bucket create')
        return super().s3_request(method, key, query=query, body=body, expected=expected)

    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['spec'] = tree_spec(body['spec'])
            return Harness.api(self, method, path, body, status, idem)
        return super().api(method, path, body, status, idem)


def classify(request):
    messages = request['messages']
    system = '\n'.join(m.get('content', '') for m in messages if m['role'] in ('system', 'developer'))
    roles = [r for r in ROLES if 'PAR_ARTIFACT_' + r.upper() + ':' in system]
    assert len(roles) == 1
    starts = [i for i, m in enumerate(messages) if m['role'] == 'user' and m.get('content') in CASES]
    assert starts
    start = starts[-1]
    return roles[0], messages[start]['content'], [json.loads(m['content']) for m in messages[start+1:] if m['role'] == 'tool']


def storage_state(h):
    metadata, objects = h.metadata_state(), h.object_state()
    by_key = {obj['key']: obj for obj in objects}
    for row in metadata['versions']:
        obj = by_key[row['object_key']]
        assert row['content_sha256'] == obj['sha256'] and row['content_length'] == obj['size']
        assert hashlib.sha256(bytes.fromhex(obj['bytes_hex'])).hexdigest() == row['content_sha256']
    return {'metadata': metadata, 'objects': objects}


def assert_saved(state, name, content, version):
    files = [f for f in state['metadata']['files'] if f['filename'] == name]
    assert len(files) == 1
    rows = [v for v in state['metadata']['versions'] if v['file_id'] == files[0]['file_id'] and v['version'] == version]
    assert len(rows) == 1
    objects = [o for o in state['objects'] if o['key'] == rows[0]['object_key']]
    assert len(objects) == 1 and objects[0]['bytes_hex'] == content.hex()
    assert objects[0]['sha256'] == hashlib.sha256(content).hexdigest() and objects[0]['size'] == len(content)


class ParallelArtifactModelFixture:
    def __init__(self, h):
        self.key = h.secret()
        self.requests, self.outputs, self.saves, self.errors = [], [], [], []
        self.lock = threading.Lock()
        self.arrived = {case: set() for case in CASES}
        self.barriers = {case: threading.Event() for case in CASES}
        self.overlap = {case: {"entered": {}, "released": {}} for case in CASES}
        self.saved_before_failure = threading.Event()
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                if self.path != '/v1/chat/completions' or self.headers.get('Authorization') != 'Bearer ' + owner.key:
                    self.send_error(401); return
                try:
                    request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    role, case, results = classify(request)
                    assert request['model'] == h.model_name
                    expected = TOOLS if role != 'aggregate' else []
                    assert sorted(t['function']['name'] for t in request.get('tools', [])) == sorted(expected)
                    with owner.lock:
                        owner.requests.append(copy.deepcopy(request)); number = len(owner.requests)
                    if role != 'aggregate' and not results:
                        with owner.lock:
                            assert role not in owner.arrived[case]
                            owner.arrived[case].add(role)
                            owner.overlap[case]["entered"][role] = time.monotonic_ns()
                            if len(owner.arrived[case]) == 2: owner.barriers[case].set()
                        assert owner.barriers[case].wait(15), 'parallel provider overlap missing'
                        with owner.lock: owner.overlap[case]['released'][role] = time.monotonic_ns()
                        if case == CASES[2] and role == 'b':
                            assert owner.saved_before_failure.wait(15), 'failure preceded actual save result'
                            self.send_error(401, 'controlled failure after peer save'); return
                        name, content = artifact_plan(case, role)
                        delta = {'role': 'assistant', 'tool_calls': [{'index': 0, 'id': 'parallel-artifact-' + str(number), 'type': 'function', 'function': {'name': 'artifact_save', 'arguments': json.dumps(save_args(name, content))}}]}
                        finish = 'tool_calls'
                    else:
                        if role != 'aggregate':
                            assert len(results) == 1
                            name, content = artifact_plan(case, role)
                            result = results[0]
                            assert_artifact(result, name, result['version'], content)
                            with owner.lock: owner.saves.append({'case': case, 'role': role, 'result': result, 'bytes_hex': content.hex()})
                            if case == CASES[2]: owner.saved_before_failure.set()
                            text = 'PAR_ARTIFACT_DONE_' + role + ':' + case
                        else:
                            assert case != CASES[2]
                            for branch in ('a', 'b'):
                                assert any('PAR_ARTIFACT_DONE_' + branch + ':' + case in m.get('content', '') for m in request['messages'])
                            text = 'PAR_ARTIFACT_FINAL:' + case
                            with owner.lock: owner.outputs.append({'input': case, 'text': text})
                        delta, finish = {'role': 'assistant', 'content': text}, 'stop'
                    def chunk(d, end, usage=None):
                        body = {'id': 'par-artifact-' + str(number), 'object': 'chat.completion.chunk', 'created': 1, 'model': request['model'], 'choices': [{'index': 0, 'delta': d, 'finish_reason': end}]}
                        if usage: body['usage'] = usage
                        return 'data: ' + json.dumps(body) + '\n\n'
                    payload = (chunk(delta, None) + chunk({}, finish, {'prompt_tokens': 7, 'completion_tokens': 3, 'total_tokens': 10}) + 'data: [DONE]\n\n').encode()
                    self.send_response(200); self.send_header('Content-Type', 'text/event-stream'); self.send_header('Content-Length', str(len(payload))); self.end_headers(); self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError): pass
                except Exception as exc:
                    with owner.lock: owner.errors.append(type(exc).__name__ + ': fixture assertion failed')
                    self.send_error(400, 'fixture assertion failed')
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
    def snapshot(self):
        with self.lock: return copy.deepcopy(self.requests)
    def output_snapshot(self):
        with self.lock: return copy.deepcopy(self.outputs)
    def save_snapshot(self):
        with self.lock: return copy.deepcopy(self.saves)
    def overlap_snapshot(self, case):
        with self.lock: return copy.deepcopy(self.overlap[case])
    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=5)
        assert not self.thread.is_alive() and not self.errors, self.errors
