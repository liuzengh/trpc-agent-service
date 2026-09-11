"""Real public parallel tree with a deterministic barrier provider or live relay.

Only fixture model responses use a barrier/opposite completion order. Live mode
observes the existing byte-transparent relay without delaying or editing bytes.
"""
import copy
import hashlib
import json
import select
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from harness import Harness
from mcp_joint_fixture import MCPHarness, ANSWER, SELECTED, tool_text, LiveRelay
from sequence_joint_fixture import text_content, all_strings, require_accepted_history

BRANCHES = ('research_a', 'research_b')
ROLES = BRANCHES + ('aggregator',)
SLOTS = {'research_a': 'primary', 'research_b': 'secondary', 'aggregator': 'aggregate'}
TOOLS = {'research_a': 'search_a', 'research_b': 'search_b'}
MODELS = {role: 'joint-parallel-' + role.replace('_', '-') for role in ROLES}
CALLABLES = {role: 'fn_' + hashlib.sha256(('tools/' + slot).encode()).hexdigest()[:60]
             for role, slot in TOOLS.items()}
INSTRUCTIONS = {
    'research_a': 'PARALLEL_RESEARCH_A: You are branch A. Actually call your only selected search tool with query=orchid. Use the real successful tool result; output PAR_BRANCH_A followed by its actual canary and 09:17 UTC service window. Do not use or quote branch B. This is an intermediate branch output, not the user-facing aggregate.',
    'research_b': 'PARALLEL_RESEARCH_B: You are branch B. Actually call your only selected search tool with query=orchid. Use the real successful tool result; output PAR_BRANCH_B followed by its actual canary and 09:17 UTC service window. Do not use or quote branch A. This is an intermediate branch output, not the user-facing aggregate.',
    'aggregator': 'PARALLEL_AGGREGATOR: You are the explicitly declared final summarizer after both parallel branches. You have no tools. Start with PAR_AGGREGATE and quote both complete current branch outputs verbatim, A then B. Preserve each actual canary and service window. Only this aggregate is delivered to the user.'}
A_FIRST = 'PAR_CASE=a_first；请两个分支分别实际搜索 query=orchid，然后后汇总节点引用两份真实结果。'
B_FIRST = 'PAR_CASE=b_first；再次请两个分支分别实际搜索 query=orchid，然后后汇总节点引用两份真实结果。'
FAILURE = 'PAR_CASE=branch_failure；请两个分支执行本轮工作，并等待明确结果。'
CASES = (A_FIRST, B_FIRST, FAILURE)


def role_of(request):
    system = '\n'.join(text_content(m.get('content')) for m in request['messages']
                       if m.get('role') in ('system', 'developer'))
    matched = [role for role in ROLES if ('PARALLEL_' + role.upper() + ':') in system]
    assert len(matched) == 1, 'request must identify precisely one parallel leaf'
    return matched[0]


def current_case(request):
    matched = [text_content(m.get('content')) for m in request['messages']
               if m.get('role') == 'user' and text_content(m.get('content')) in CASES]
    assert matched, 'actual user input absent from provider request'
    return matched[-1]


def branch_output(role, case):
    tag = {A_FIRST: 'a_first', B_FIRST: 'b_first', FAILURE: 'branch_failure'}[case]
    return 'PAR_BRANCH_' + role[-1].upper() + '_' + tag + '\n' + ANSWER


def aggregate_output(case):
    return 'PAR_AGGREGATE\n' + '\n'.join(branch_output(role, case) for role in BRANCHES)


def require_declaration(request, role):
    if role == 'aggregator':
        assert not request.get('tools'), 'aggregator acquired branch tool authority'
        assert all(m.get('role') != 'tool' and not m.get('tool_calls') for m in request['messages'])
    else:
        tools = request.get('tools', [])
        assert len(tools) == 1 and tools[0]['function']['name'] == CALLABLES[role]
        schema = tools[0]['function']['parameters']
        assert schema['type'] == 'object' and schema['required'] == ['query']
        assert set(schema['properties']) == {'query'} and schema['properties']['query']['type'] == 'string'


class ParallelHarness(MCPHarness):
    def prepare_parallel(self, env_file=None):
        self.prepare_mcp(env_file)
        if self.live:
            self.model = TimedLiveView(self.model)
        else:
            self.model.close()
            self.model = ParallelModelFixture(self)
        self.urls['model'] = self.model.url

    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            spec = body['spec']
            spec['root'] = 'workflow'
            spec['requirements']['models'] = {SLOTS[role]: {'capabilities': ['chat', 'tool_call'] if role in BRANCHES else ['chat']} for role in ROLES}
            spec['requirements']['tools'] = {slot: {'capability': 'web.search'} for slot in TOOLS.values()}
            spec['nodes'] = {
                'workflow': {'kind': 'sequence', 'children': ['research', 'aggregator']},
                'research': {'kind': 'parallel', 'children': list(BRANCHES)}}
            for role in ROLES:
                spec['nodes'][role] = {'kind': 'llm', 'instruction': INSTRUCTIONS[role],
                    'model_slot': SLOTS[role], 'tool_slots': [TOOLS[role]] if role in BRANCHES else [], 'knowledge_slots': []}
        if method == 'PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['config']['models'] = {SLOTS[role]: {'kind': 'openai_compatible',
                'model': self.model_name if self.live else MODELS[role], 'base_url': self.model.url + '/v1',
                'capabilities': ['chat', 'tool_call'] if role in BRANCHES else ['chat']} for role in ROLES}
            body['credentials']['models'] = {SLOTS[role]: {'api_key': {'action': 'replace',
                'value': self.model.key if self.live else self.model.keys[role]}} for role in ROLES}
            body['config']['tools'] = {slot: {'kind': 'mcp_streamable_http', 'server_url': self.mcp_url,
                'toolset_name': 'joint_mcp', 'tool_name': SELECTED, 'auth': {'kind': 'bearer'},
                'capability': 'web.search'} for slot in TOOLS.values()}
            body['credentials']['tools'] = {slot: {'bearer_token': {'action': 'replace', 'value': self.mcp_token}}
                                            for slot in TOOLS.values()}
        return Harness.api(self, method, path, body, status, idem)


class ParallelModelFixture:
    def __init__(self, h):
        self.keys = {role: h.secret() for role in ROLES}
        self.key = self.keys['research_a']
        self.records, self.errors = [], []
        self.lock = threading.RLock()
        self.idle = threading.Condition(self.lock)
        self.connections = set()
        self.barriers = {(case, phase): threading.Barrier(2, timeout=10)
                         for case in CASES for phase in ('tool_select', 'branch_output')}
        self.completed = {case: {role: threading.Event() for role in ROLES} for case in CASES}
        self.output_done = {case: {role: threading.Event() for role in BRANCHES} for case in CASES}
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def send_body(self, record, status, raw, mime):
                with owner.lock:
                    record.update(status=status, response_started_ns=time.monotonic_ns())
                self.send_response(status)
                self.send_header('Content-Type', mime)
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                self.wfile.flush()
                with owner.lock:
                    record.update(finished_ns=time.monotonic_ns(), complete=True, termination='response_complete')

            def do_POST(self):
                if self.path != '/v1/chat/completions':
                    self.send_error(404)
                    return
                record = None
                role, case = None, None
                with owner.idle:
                    owner.connections.add(self.connection)
                try:
                    request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    role, case = role_of(request), current_case(request)
                    if self.headers.get('Authorization') != 'Bearer ' + owner.keys[role]:
                        self.send_error(401)
                        return
                    assert request['model'] == MODELS[role]
                    require_declaration(request, role)
                    messages = request['messages']
                    last = max(i for i, m in enumerate(messages) if m.get('role') == 'user'
                               and text_content(m.get('content')) == case)
                    results = [tool_text(m.get('content')) for m in messages[last + 1:] if m.get('role') == 'tool']
                    phase = 'aggregate' if role == 'aggregator' else 'branch_output' if results else 'tool_select'
                    record = {'request': copy.deepcopy(request), 'role': role, 'phase': phase,
                              'authenticated': True, 'status': None, 'text': '', 'complete': False,
                              'started_ns': time.monotonic_ns()}
                    with owner.lock:
                        owner.records.append(record)
                        ordinal = len(owner.records)
                    if role in BRANCHES:
                        sibling = 'research_b' if role == 'research_a' else 'research_a'
                        assert not any(branch_output(sibling, case) in raw for raw in all_strings(messages)), \
                            'branch actual second request contains current sibling output'
                        owner.barriers[(case, phase)].wait()
                        if case == FAILURE:
                            assert phase == 'tool_select'
                            if role == 'research_b':
                                self.send_body(record, 401, b'{"error":{"message":"branch fixture denied","type":"authentication_error"}}', 'application/json')
                            else:
                                readable, _, _ = select.select([self.connection], [], [], 15)
                                assert readable, 'failed sibling did not cause observable model HTTP exit'
                                try:
                                    eof = self.connection.recv(1, socket.MSG_PEEK) == b''
                                except (ConnectionResetError, ConnectionAbortedError):
                                    eof = True
                                assert eof, 'model socket remained live after sibling error'
                                with owner.lock:
                                    record.update(finished_ns=time.monotonic_ns(), complete=True,
                                                  termination='client_disconnect', disconnect_observed=True)
                            return
                        if phase == 'tool_select':
                            delta = {'tool_calls': [{'index': 0, 'id': 'parallel-call-' + str(ordinal),
                                'type': 'function', 'function': {'name': CALLABLES[role],
                                'arguments': json.dumps({'query': 'orchid'})}}]}
                        else:
                            assert results == [ANSWER], 'branch must consume one actual MCP tool result'
                            first_role = 'research_a' if case == A_FIRST else 'research_b'
                            if role != first_role:
                                assert owner.output_done[case][first_role].wait(timeout=10), 'first branch output did not finish'
                            delta = {'content': branch_output(role, case)}
                    else:
                        assert case != FAILURE, 'aggregator executed after failed parallel branch'
                        assert all(any(branch_output(branch, case) in text_content(m.get('content')) for m in messages)
                                   for branch in BRANCHES), 'aggregator actual request lacks a branch output'
                        delta = {'content': aggregate_output(case)}
                    usage = {'prompt_tokens': 12, 'completion_tokens': 8, 'total_tokens': 20}
                    events = [{'id': 'parallel-fixture', 'object': 'chat.completion.chunk', 'model': request['model'],
                        'choices': [{'index': 0, 'delta': delta, 'finish_reason': 'stop' if 'content' in delta else 'tool_calls'}]},
                        {'id': 'parallel-fixture', 'object': 'chat.completion.chunk', 'model': request['model'], 'choices': [], 'usage': usage}]
                    raw = (''.join('data: ' + json.dumps(event) + '\n\n' for event in events) + 'data: [DONE]\n\n').encode()
                    with owner.lock:
                        record.update(text=delta.get('content', ''), usage=usage)
                    self.send_body(record, 200, raw, 'text/event-stream')
                    if phase == 'branch_output':
                        owner.output_done[case][role].set()
                except Exception as exc:
                    with owner.lock:
                        owner.errors.append(type(exc).__name__ + ': ' + str(exc))
                        if record is not None:
                            record.update(complete=True, finished_ns=time.monotonic_ns(), termination='fixture_error')
                    try:
                        self.send_error(400, 'parallel model fixture assertion')
                    except (OSError, ValueError):
                        pass
                finally:
                    if role is not None and case is not None and record is not None and record.get('complete'):
                        owner.completed[case][role].set()
                    with owner.idle:
                        owner.connections.discard(self.connection)
                        owner.idle.notify_all()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.records)

    def close(self):
        self.server.shutdown()
        for barrier in self.barriers.values():
            barrier.abort()
        with self.lock:
            pending = list(self.connections)
        for connection in pending:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        with self.idle:
            drained = self.idle.wait_for(lambda: not self.connections, timeout=5)
        self.server.server_close()
        self.thread.join(timeout=5)
        assert drained and not self.thread.is_alive(), 'owned model request handlers did not exit'


def _request_identity(request):
    return hashlib.sha256(json.dumps(request, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


class TimedLiveView:
    """Observe existing relay HTTP-handler intervals, never provider internals.

    A read-through tap returns the exact original body bytes to the original
    handler. Its original HTTPS forwarding and SSE writing remain untouched.
    The observer adds timestamps only after the original handler has returned.
    """
    def __init__(self, relay):
        self.relay, self.key, self.url = relay, relay.key, relay.url
        self.lock = threading.Condition()
        self.active = 0
        self.timings = []
        original_handler = relay.server.RequestHandlerClass
        owner = self

        class BodyTap:
            def __init__(self, reader):
                self.reader, self.body = reader, None
            def read(self, *args, **kwargs):
                raw = self.reader.read(*args, **kwargs)
                if self.body is None:
                    self.body = raw
                return raw
            def __getattr__(self, name):
                return getattr(self.reader, name)

        class ObservedHandler(original_handler):
            def do_POST(self):
                started = time.monotonic_ns()
                reader = self.rfile
                tapped = BodyTap(reader)
                self.rfile = tapped
                with owner.lock:
                    owner.active += 1
                try:
                    super().do_POST()
                finally:
                    ended = time.monotonic_ns()
                    self.rfile = reader
                    try:
                        request = json.loads(tapped.body) if tapped.body is not None else None
                    except (ValueError, TypeError):
                        request = None
                    with owner.lock:
                        if isinstance(request, dict):
                            owner.timings.append({'request_identity': _request_identity(request),
                                'request_bytes_sha256': hashlib.sha256(tapped.body).hexdigest(),
                                'started_ns': started, 'finished_ns': ended,
                                'interval_kind': 'relay_http_handler_monotonic'})
                        owner.active -= 1
                        owner.lock.notify_all()
        # prepare_parallel completes this before the real Profile is published.
        relay.server.RequestHandlerClass = ObservedHandler

    def snapshot(self):
        records = self.relay.snapshot()
        with self.lock:
            timings = copy.deepcopy(self.timings)
        by_request = {}
        for timing in sorted(timings, key=lambda value: value['started_ns']):
            by_request.setdefault(timing['request_identity'], []).append(timing)
        for record in records:
            matches = by_request.get(_request_identity(record['request']), [])
            if matches:
                observed = matches.pop(0)
                record.update({k: v for k, v in observed.items() if k != 'request_identity'})
        return records

    def wait_idle(self, timeout):
        with self.lock:
            return self.lock.wait_for(lambda: self.active == 0, timeout=timeout)

    def close(self):
        self.relay.close()
        assert self.wait_idle(timeout=5), 'live observation handler not reaped'


def require_exchange(calls, case, *, live, limit):
    grouped = {role: [] for role in ROLES}
    for call in calls:
        request = call['request']
        role = role_of(request)
        require_declaration(request, role)
        assert current_case(request) == case
        assert request['model'] == ('deepseek-v4-flash' if live else MODELS[role])
        assert request['max_completion_tokens'] == limit and 'max_tokens' not in request
        assert call.get('complete') and 0 < call['started_ns'] < call['finished_ns']
        grouped[role].append(call)
    for group in grouped.values():
        group.sort(key=lambda item: item['started_ns'])
    assert all(grouped[role] for role in BRANCHES)
    initial = [grouped[role][0] for role in BRANCHES]
    assert max(c['started_ns'] for c in initial) < min(c['finished_ns'] for c in initial), \
        'actual first HTTP branch request intervals do not overlap'
    if case == FAILURE:
        assert not live and len(calls) == 2 and not grouped['aggregator']
        a, b = grouped['research_a'][0], grouped['research_b'][0]
        assert a['status'] is None and a.get('termination') == 'client_disconnect' and a.get('disconnect_observed') is True
        assert b['status'] == 401 and b['termination'] == 'response_complete'
        assert a['finished_ns'] > b['response_started_ns']
        return {'branch_failure': True, 'branch_outputs': {}, 'terminal_output': None,
                'initial_intervals': [{k: c[k] for k in ('started_ns', 'finished_ns')} for c in initial],
                'sibling_http_termination': 'client_disconnect'}
    assert len(calls) == 5 and [len(grouped[role]) for role in ROLES] == [2, 2, 1]
    assert all(c['status'] == 200 and c['usage']['total_tokens'] > 0 for c in calls)
    outputs, ids = {}, {}
    for role in BRANCHES:
        last = grouped[role][-1]
        output = last['text']
        assert 'PAR_BRANCH_' + role[-1].upper() in output and 'MCP_ORCHID_627' in output and '09:17 UTC' in output
        outputs[role] = output
        messages = last['request']['messages']
        start = max(i for i, m in enumerate(messages) if m.get('role') == 'user' and text_content(m.get('content')) == case)
        current = messages[start + 1:]
        tool_messages = [m for m in current if m.get('role') == 'tool']
        assert len(tool_messages) == 1 and tool_text(tool_messages[0]['content']) == ANSWER
        ids[role] = tool_messages[0]['tool_call_id']
        declared_calls = [t for m in current for t in m.get('tool_calls', [])]
        assert len(declared_calls) == 1 and declared_calls[0]['function']['name'] == CALLABLES[role]
        assert declared_calls[0]['id'] == ids[role]
        if not live:
            assert output == branch_output(role, case)
    assert ids['research_a'] != ids['research_b'], 'sibling actual tool result IDs alias'
    for role in BRANCHES:
        sibling = 'research_b' if role == 'research_a' else 'research_a'
        assert not any(outputs[sibling] in raw for raw in all_strings(grouped[role][-1]['request']['messages'])), \
            'actual branch second request contains current sibling output'
    aggregate = grouped['aggregator'][0]
    for output in outputs.values():
        assert any(output in text_content(m.get('content')) for m in aggregate['request']['messages']), \
            'explicit aggregator actual request does not consume both branch outputs'
    terminal = aggregate['text']
    assert 'PAR_AGGREGATE' in terminal and all(output in terminal for output in outputs.values())
    assert all(terminal != output for output in outputs.values())
    order = sorted(BRANCHES, key=lambda role: grouped[role][-1]['finished_ns'])
    if not live:
        expected = list(BRANCHES) if case == A_FIRST else list(reversed(BRANCHES))
        assert order == expected
        assert grouped[order[0]][-1]['finished_ns'] < grouped[order[1]][-1]['response_started_ns']
        assert terminal == aggregate_output(case)
    return {'branch_failure': False, 'branch_outputs': outputs, 'terminal_output': terminal,
            'completion_order': order,
            'initial_intervals': [{k: c[k] for k in ('started_ns', 'finished_ns')} for c in initial]}
