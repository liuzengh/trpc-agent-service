"""Memory acceptance helpers: real owner APIs, isolated PG, observable SDK model HTTP."""
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import signal
import threading

from harness import Harness, _http_status
from model_fixture import ModelFixture

TOOLS = ['memory_add', 'memory_update', 'memory_delete', 'memory_clear', 'memory_search', 'memory_load']


class MemoryHarness(Harness):
    def spawn(self, name, argv, env):
        if name == 'control-api':
            self.control_env = dict(env)
        return super().spawn(name, argv, env)

    def admin_sql(self, statement):
        # Fault/provisioning boundary only, never a substitute for product writes.
        return self.command(['docker', 'exec', self.pg, 'psql', '-U', 'platform_admin', '-d', 'agent_platform', '-v', 'ON_ERROR_STOP=1', '-c', statement])

    def provision(self):
        super().provision()
        self.memory_password = self.secret()
        self.memory_migrator_password = self.secret()
        statement = "CREATE ROLE memory_migrator LOGIN PASSWORD " + self.quote(self.memory_migrator_password) + "; CREATE ROLE memory_runtime LOGIN PASSWORD " + self.quote(self.memory_password) + "; GRANT CONNECT ON DATABASE agent_platform TO memory_migrator,memory_runtime; CREATE SCHEMA runtime_memory AUTHORIZATION memory_migrator; REVOKE ALL ON SCHEMA runtime_memory FROM PUBLIC; GRANT USAGE ON SCHEMA runtime_memory TO memory_runtime; ALTER ROLE memory_migrator SET search_path=runtime_memory,pg_temp; ALTER ROLE memory_runtime SET search_path=runtime_memory,pg_temp;"
        self.admin_sql(statement)
        self.dsns['memory_migrator'] = f'postgres://memory_migrator:{self.memory_migrator_password}@127.0.0.1:{self.pg_port}/agent_platform?sslmode=disable'
        # This explicit process command owns migration. Runtime Open never DDLs.
        print(self.command([self.binaries['agent-worker'], 'prepare-memory'], env={'MEMORY_MIGRATION_DATABASE_URL': self.dsns['memory_migrator']}).strip(), flush=True)

    def bind_memory_catalog(self, tenant_id):
        catalog = {'version': 'v1', 'backends': [{'id': 'joint-memory-pg', 'revision': 1, 'label': 'Joint PostgreSQL Memory', 'kind': 'postgresql', 'roles': ['memory'], 'enabled': True, 'tenant_ids': [tenant_id]}]}
        targets = {'version': 'v1', 'backends': [{'backend_id': 'joint-memory-pg', 'backend_revision': 1, 'kind': 'postgresql', 'adapter': 'managed-postgres-v1', 'isolation': 'tenant-subject-agent-v1', 'limits': {'timeout_ms': 5000, 'max_concurrency': 4, 'max_bytes': 1048576}, 'postgresql': {'host': '127.0.0.1', 'port': self.pg_port, 'database': 'agent_platform', 'username': 'memory_runtime', 'sslmode': 'disable'}}]}
        env = dict(self.control_env)
        for suffix, value in [('CATALOG', catalog), ('TARGETS', targets)]:
            path = Path(self.write('memory-' + suffix.lower() + '.json', value))
            path.chmod(0o600)
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_FILE'] = str(path)
            env['CONTROL_PLATFORM_BACKEND_' + suffix + '_SHA256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        env.pop('CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST', None)
        self.contract_digest = self.command([self.binaries['control-api'], '--print-deployment-contract-digest'], env=env).strip()
        env['CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST'] = self.contract_digest
        self.control.send_signal(signal.SIGTERM)
        self.control.wait(timeout=25)
        self.control = self.spawn('control-api', [self.binaries['control-api']], env)
        self.wait(lambda: _http_status(self.urls['control'] + '/healthz', timeout=1) == 204, 'Control pinned Memory catalog restart')
        (self.artifacts / 'memory-backend-config.json').write_text(json.dumps({'catalog': catalog, 'targets': targets, 'contract_digest': self.contract_digest}, indent=2) + '\n')

    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['spec']['requirements']['models']['primary']['capabilities'] = ['chat', 'tool_call']
            body['spec']['nodes']['assistant']['memory'] = {'tools': list(TOOLS), 'preload_limit': 0}
        if method == 'PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['config']['models']['primary']['capabilities'] = ['chat', 'tool_call']
            body['config']['storage']['memory'] = {'kind': 'managed_memory', 'backend_id': 'joint-memory-pg', 'backend_revision': 1}
            body['credentials']['storage']['memory'] = {'dsn_password': {'action': 'replace', 'value': self.memory_password}}
        result = super().api(method, path, body, status, idem)
        if method == 'POST' and path == '/v1/admin/tenants':
            self.bind_memory_catalog(result['id'])
        return result

    def memory_state(self):
        rows = self.sql("SELECT json_build_object('tenant_id',tenant_id,'scope_id',scope_id,'revision',revision,'digest',digest,'content',convert_from(content,'UTF8')::json)::text FROM runtime_memory.memory_heads ORDER BY tenant_id,scope_id")
        return [json.loads(row[0]) for row in rows]


class MemoryModelFixture:
    """Only external model responses are synthetic; all tools execute in SDK.

    The correction scenario keeps a legitimate missing-ID business error in the
    SDK tool-result/model loop. It is not an infrastructure failure or a new
    Worker retry: the next model turn corrects its action via memory_load.
    """
    def __init__(self, h):
        self.key = h.secret()
        self.requests, self.outputs = [], []
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                if self.path != '/v1/chat/completions':
                    self.send_error(404)
                    return
                if self.headers.get('Authorization') != 'Bearer ' + owner.key:
                    self.send_error(401)
                    return
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                with owner.lock:
                    owner.requests.append(copy.deepcopy(request))
                    number = len(owner.requests)
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
                exposed = sorted(t['function']['name'] for t in request.get('tools', []))
                if exposed != sorted(TOOLS):
                    self.send_error(400, 'memory tools differ from published whitelist')
                    return
                if mode == 'memory-fail' and step:
                    self.send_error(401, 'fixture model failure after memory mutation')
                    return
                name, args = None, None
                if mode == 'memory-six':
                    plan = [('memory_add', {'memory': 'temporary tea memory', 'topics': ['drink']}), ('memory_load', {}), ('memory_update', None), ('memory_search', {'query': 'coffee'}), ('memory_delete', None), ('memory_add', {'memory': 'clear must remove this entry'}), ('memory_clear', {}), ('memory_add', {'memory': 'persistent orchid memory', 'topics': ['orchid']}), ('memory_load', {})]
                    if step < len(plan):
                        name, args = plan[step]
                        if name == 'memory_update':
                            args = {'memory_id': results[-1]['results'][0]['id'], 'memory': 'temporary coffee memory', 'topics': ['drink']}
                        if name == 'memory_delete':
                            args = {'memory_id': results[-1]['results'][0]['id']}
                elif mode in ('memory-fail', 'memory-pg-fail'):
                    if step == 0:
                        name, args = 'memory_add', {'memory': 'must not persist ' + mode}
                    elif mode == 'memory-pg-fail':
                        h.admin_sql('REVOKE INSERT ON runtime_memory.memory_receipts FROM memory_runtime')
                elif mode == 'memory-correct':
                    if step == 0:
                        name, args = 'memory_update', {'memory_id': 'missing-fixture-entry', 'memory': 'invalid correction must not persist'}
                    elif step == 1:
                        if not results[0].get('error'):
                            self.send_error(400, 'SDK business error was not returned to model')
                            return
                        name, args = 'memory_load', {}
                elif mode in ('memory-read', 'memory-after-failure'):
                    if step == 0:
                        name, args = 'memory_load', {}
                else:
                    self.send_error(400, 'unknown fixture scenario')
                    return
                text = 'memory final: ' + mode
                if name:
                    delta = {'role': 'assistant', 'tool_calls': [{'index': 0, 'id': 'memory-call-' + str(number), 'type': 'function', 'function': {'name': name, 'arguments': json.dumps(args)}}]}
                    finish = 'tool_calls'
                else:
                    delta, finish = {'role': 'assistant', 'content': text}, 'stop'
                    with owner.lock:
                        owner.outputs.append({'input': mode, 'text': text, 'tool_results': results})
                model = request['model']
                def chunk(delta, finish, usage=None):
                    event = {'id': 'memory-response-' + str(number), 'object': 'chat.completion.chunk', 'created': 1, 'model': model, 'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]}
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

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()
