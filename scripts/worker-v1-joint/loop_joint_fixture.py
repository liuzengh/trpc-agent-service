"""Bounded native SDK cycle through existing public Control/Session/Channel Lab.

No MCP server or extra capability service is started. Live mode reuses the
existing transparent DeepSeek relay and its already-tested interval observer.
"""
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import threading
import time

from harness import Harness
from combined_capabilities_fixture import LiveRelay, read_key
from parallel_joint_fixture import TimedLiveView
from sequence_joint_fixture import text_content, all_strings, require_accepted_history

MODEL = 'joint-loop-assistant'
NORMAL = 'LOOP_CASE=normal；请完成本轮两次迭代，第二次逐字引用本轮第一次输出。'
FAILURE = 'LOOP_CASE=terminal_failure；请先生成本轮初稿，再处理第二次迭代。'
RECOVER = 'LOOP_CASE=recover；沿用已接受历史，重新完成本轮两次迭代。'
INSTRUCTION = (
    'LOOP_ASSISTANT_NODE: The SDK orchestrator invokes this same node exactly twice. '
    'Each model call must perform ONLY its current iteration. Never simulate both '
    'iterations in one answer. You have no tools. Find the latest user message starting '
    'LOOP_CASE= and inspect only assistant messages AFTER that user message. '
    'If there is no such assistant message, return exactly these two lines, with no '
    'extra spaces, Markdown, quotation marks, explanations or preview of the next step:\n'
    'LOOP_ITERATION_ONE\nLOOP_ORCHID_627\n'
    'If such an assistant message already exists, return LOOP_ITERATION_TWO, then one '
    'newline, then the exact complete content of that supplied assistant message. '
    'Copy EVERY character unchanged, including whitespace and punctuation. Copy the '
    'actual assistant content, not reasoning_content, an imagined draft or a paraphrase. '
    'Do not add any explanation. The SDK, not this model call, controls iteration count.')


class LoopHarness(Harness):
    def __init__(self, *args, live=False, **kwargs):
        self.live = live
        super().__init__(*args, **kwargs)

    def record(self, name, value):
        path = self.artifacts / name
        path.write_text(self.redact(json.dumps(value, indent=2, ensure_ascii=False)) + '\n')
        path.chmod(0o600)
        json.loads(path.read_text())

    def prepare_loop(self, env_file=None):
        self.model.close()
        if self.live:
            key = read_key(env_file, 'DEEPSEEK_API_KEY')
            self.secrets.append(key)
            self.provider_base = read_key(env_file, 'DEEPSEEK_BASE_URL')
            self.model = TimedLiveView(LiveRelay(self, key, self.provider_base, self.model_name))
        else:
            self.model = LoopModelFixture(self)
        self.urls['model'] = self.model.url

    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            bound = getattr(self, 'initial_max_iterations', 2)
            if type(bound) is not int or not 1 <= bound <= 32:
                raise ValueError('initial max_iterations must match existing integer 1..32 contract')
            spec = body['spec']
            spec['root'] = 'workflow'
            spec['requirements'] = {'models': {'primary': {'capabilities': ['chat']}},
                'tools': {}, 'knowledge': {}}
            spec['nodes'] = {
                'workflow': {'kind': 'loop', 'body': 'assistant', 'max_iterations': bound},
                'assistant': {'kind': 'llm', 'instruction': INSTRUCTION, 'model_slot': 'primary',
                    'tool_slots': [], 'knowledge_slots': []}}
        return Harness.api(self, method, path, body, status, idem)

    def close(self):
        problem = None
        try:
            super().close()
        except BaseException as exc:
            problem = exc
        errors = getattr(getattr(self, 'model', None), 'errors', [])
        if errors and problem is None:
            problem = RuntimeError(str(errors))
        reaped = all(p.poll() is not None for _, p in self.processes)
        if not reaped and problem is None:
            problem = RuntimeError('owned process not reaped')
        self.record('loop-cleanup.json', {'result': 'FAIL' if problem else 'PASS',
            'mcp_started': False, 'processes_reaped': reaped})
        if problem:
            raise problem


def current_case(request):
    cases = [text_content(m.get('content')) for m in request['messages']
        if m.get('role') == 'user' and text_content(m.get('content')) in (NORMAL, FAILURE, RECOVER)]
    assert cases, 'actual model request lost its current Loop input'
    return cases[-1]


def current_outputs(request, case):
    messages = request['messages']
    start = max(i for i, message in enumerate(messages)
        if message.get('role') == 'user' and text_content(message.get('content')) == case)
    return [text_content(message.get('content')) for message in messages[start + 1:]
        if 'LOOP_ITERATION_ONE' in text_content(message.get('content'))]


def first_output(case):
    tag = {NORMAL: 'normal', FAILURE: 'terminal_failure', RECOVER: 'recover'}[case]
    return 'LOOP_ITERATION_ONE_' + tag + '\nLOOP_ORCHID_627: "兰花"; path=C:\\orchid\n本轮初稿。'


def terminal_output(case):
    return 'LOOP_ITERATION_TWO\n' + first_output(case) + '\nOnly the last iteration is delivered.'


def require_no_tools(request):
    assert not request.get('tools'), 'loop without declared tools exposed a callable'
    assert all(message.get('role') != 'tool' and not message.get('tool_calls')
        for message in request['messages']), 'loop without tools received tool protocol'
    assert any('LOOP_ASSISTANT_NODE:' in text_content(message.get('content'))
        for message in request['messages'] if message.get('role') in ('system', 'developer'))


class LoopModelFixture:
    """Two real HTTP responses selected by actual current-Run message history."""
    def __init__(self, h):
        self.key = h.secret()
        self.records, self.errors = [], []
        self.lock = threading.Condition()
        self.active = set()
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
                record = None
                with owner.lock:
                    owner.active.add(self.connection)
                try:
                    self.connection.settimeout(10)
                    request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    assert request['model'] == MODEL
                    require_no_tools(request)
                    case = current_case(request)
                    outputs = current_outputs(request, case)
                    assert outputs in ([], [first_output(case)]), 'actual loop history differs from current first output'
                    iteration = 2 if outputs else 1
                    record = {'request': copy.deepcopy(request), 'iteration': iteration,
                        'authenticated': True, 'status': None, 'text': '', 'complete': False,
                        'started_ns': time.monotonic_ns()}
                    with owner.lock:
                        prior = [r for r in owner.records if current_case(r['request']) == case]
                        assert len(prior) == iteration - 1, 'unexpected extra/reordered model call'
                        owner.records.append(record)
                    if case == FAILURE and iteration == 2:
                        raw = b'{"error":{"message":"loop final iteration rejected","type":"authentication_error"}}'
                        status, content_type, text = 401, 'application/json', ''
                        usage = None
                    else:
                        text = first_output(case) if iteration == 1 else terminal_output(case)
                        usage = {'prompt_tokens': 12, 'completion_tokens': 8, 'total_tokens': 20}
                        events = [
                            {'id': 'loop-fixture', 'object': 'chat.completion.chunk', 'model': MODEL,
                                'choices': [{'index': 0, 'delta': {'content': text}, 'finish_reason': 'stop'}]},
                            {'id': 'loop-fixture', 'object': 'chat.completion.chunk', 'model': MODEL,
                                'choices': [], 'usage': usage}]
                        raw = (''.join('data: ' + json.dumps(e) + '\n\n' for e in events) + 'data: [DONE]\n\n').encode()
                        status, content_type = 200, 'text/event-stream'
                    with owner.lock:
                        record['response_started_ns'] = time.monotonic_ns()
                    self.send_response(status)
                    self.send_header('Content-Type', content_type)
                    self.send_header('Content-Length', str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    self.wfile.flush()
                    with owner.lock:
                        record.update(status=status, text=text, usage=usage, complete=True,
                            termination='response_complete')
                except Exception as exc:
                    with owner.lock:
                        owner.errors.append(type(exc).__name__ + ': ' + str(exc))
                        if record is not None:
                            record.update(complete=False, termination='fixture_error')
                    try:
                        self.send_error(400, 'loop model fixture assertion')
                    except OSError:
                        pass
                finally:
                    with owner.lock:
                        if record is not None:
                            record['finished_ns'] = time.monotonic_ns()
                        owner.active.discard(self.connection)
                        owner.lock.notify_all()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.records)

    def wait_idle(self, timeout):
        with self.lock:
            return self.lock.wait_for(lambda: not self.active, timeout=timeout)

    def close(self):
        self.server.shutdown()
        with self.lock:
            sockets = list(self.active)
        for connection in sockets:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.server.server_close()
        self.thread.join(timeout=5)
        if self.thread.is_alive() or not self.wait_idle(5):
            # Parent Harness must still finish stopping all actual processes.
            self.errors.append('loop HTTP fixture did not stop')


def require_exchange(calls, case, *, live, limit):
    assert len(calls) == 2, 'max_iterations=2 must issue exactly two body model requests'
    assert calls[0]['started_ns'] < calls[1]['started_ns'], 'iteration HTTP order differs'
    for call in calls:
        request = call['request']
        require_no_tools(request)
        assert current_case(request) == case
        assert request['model'] == ('deepseek-v4-flash' if live else MODEL)
        assert request['max_completion_tokens'] == limit and 'max_tokens' not in request
        assert call.get('complete') is True and 0 < call['started_ns'] < call['finished_ns']
        if not live:
            assert call['authenticated'] is True and call['termination'] == 'response_complete'
    first, last = calls
    assert not current_outputs(first['request'], case), 'first iteration already contains current output'
    assert first['status'] == 200 and first['usage']['total_tokens'] > 0
    output = first['text']
    assert output.startswith('LOOP_ITERATION_ONE') and 'LOOP_ORCHID_627' in output
    assert current_outputs(last['request'], case) == [output], 'second iteration lacks exact decoded current first output'
    if case == FAILURE:
        assert not live and last['status'] == 401 and not last['text']
        terminal = None
    else:
        assert last['status'] == 200 and last['usage']['total_tokens'] > 0
        terminal = last['text']
        assert terminal.startswith('LOOP_ITERATION_TWO') and terminal != output
        assert output in terminal, 'last iteration must consume and quote the actual first output'
        if not live:
            assert output == first_output(case) and terminal == terminal_output(case)
    if not live:
        assert [call['iteration'] for call in calls] == [1, 2]
    return {'iterations': [1, 2], 'first_output': output, 'terminal_output': terminal,
        'terminal_failure': case == FAILURE, 'http_requests': len(calls)}
