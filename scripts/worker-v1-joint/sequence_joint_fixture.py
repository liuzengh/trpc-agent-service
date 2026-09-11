"""Nested sequence verification through the existing Control/MCP/Lab fixture.

The deterministic provider is a test dependency. Live mode uses the unchanged
DeepSeek byte relay and the same externally authorized key in two credential
slots; their immutable credential IDs must remain distinct at publication.
"""
import copy
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from harness import Harness
from mcp_joint_fixture import MCPHarness, SELECTED, CALLABLE, ANSWER, assert_declaration, tool_text

RESEARCH_MODEL = 'joint-sequence-research'
WRITER_MODEL = 'joint-sequence-writer'
RESEARCH_INSTRUCTION = (
    'SEQUENCE_RESEARCH_NODE: You are the first specialist in an ordered sequence. '
    'For the current user request, actually call the selected search tool with '
    'query=orchid. Use its successful real result. Your answer must start with '
    'SEQ_STAGE_ONE and include the exact canary and service window returned by the tool. '
    'Do not deliver a final user answer for the overall sequence.')
WRITER_INSTRUCTION = (
    'SEQUENCE_WRITER_NODE: You are the terminal specialist in an ordered sequence. '
    'Use the first specialist output already present in the current conversation. '
    'You have no tools and must not call any. Start with SEQ_STAGE_TWO, then quote '
    'the complete current first-specialist answer verbatim. Keep its real canary '
    'and service window. Only your terminal answer is delivered to the user.')


class SequenceHarness(MCPHarness):
    def prepare_sequence(self, env_file=None):
        self.prepare_mcp(env_file)
        if not self.live:
            self.model.close()
            self.model = SequenceModelFixture(self)
            self.urls['model'] = self.model.url

    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            spec = body['spec']
            spec['root'] = 'workflow'
            spec['requirements']['models'] = {
                'primary': {'capabilities': ['chat', 'tool_call']},
                'writer': {'capabilities': ['chat']}}
            spec['requirements']['tools'] = {'search': {'capability': 'web.search'}}
            spec['nodes'] = {
                'workflow': {'kind': 'sequence', 'children': ['prepare', 'writer']},
                'prepare': {'kind': 'sequence', 'children': ['researcher']},
                'researcher': {'kind': 'llm', 'instruction': RESEARCH_INSTRUCTION,
                    'model_slot': 'primary', 'tool_slots': ['search'], 'knowledge_slots': []},
                'writer': {'kind': 'llm', 'instruction': WRITER_INSTRUCTION,
                    'model_slot': 'writer', 'tool_slots': [], 'knowledge_slots': []}}
        if method == 'PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            primary = body['config']['models']['primary']
            primary['capabilities'] = ['chat', 'tool_call']
            body['config']['models']['writer'] = {
                'kind': 'openai_compatible',
                'model': self.model_name if self.live else WRITER_MODEL,
                'base_url': self.model.url + '/v1', 'capabilities': ['chat']}
            writer_key = self.model.key if self.live else self.model.writer_key
            body['credentials']['models']['writer'] = {
                'api_key': {'action': 'replace', 'value': writer_key}}
            body['config']['tools'] = {'search': {
                'kind': 'mcp_streamable_http', 'server_url': self.mcp_url,
                'toolset_name': 'joint_mcp', 'tool_name': SELECTED,
                'auth': {'kind': 'bearer'}, 'capability': 'web.search'}}
            body['credentials']['tools'] = {'search': {
                'bearer_token': {'action': 'replace', 'value': self.mcp_token}}}
        # MCPHarness owns lifecycle, not this graph's public draft transformation.
        return Harness.api(self, method, path, body, status, idem)


NORMAL = 'SEQ_CASE=normal；请由首节点实际搜索 query=orchid，末节点使用首节点真实结果给出最终回答。'
FAILURE = 'SEQ_CASE=terminal_failure；请由首节点实际搜索 query=orchid，末节点继续处理。'
RECOVER = 'SEQ_CASE=recover；再次由首节点实际搜索 query=orchid，末节点使用本轮结果回答并延续正式历史。'


def text_content(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return ''.join(text_content(part) for part in value)
    if isinstance(value, dict) and isinstance(value.get('text'), str):
        return value['text']
    return ''


def role_of(request):
    system = '\n'.join(text_content(m.get('content')) for m in request.get('messages', [])
                       if m.get('role') in ('system', 'developer'))
    first, last = 'SEQUENCE_RESEARCH_NODE:' in system, 'SEQUENCE_WRITER_NODE:' in system
    assert first != last, 'each request must identify precisely one instruction/leaf'
    return 'researcher' if first else 'writer'


def current_case(request):
    cases = [text_content(m.get('content')) for m in request['messages']
             if m.get('role') == 'user' and text_content(m.get('content')) in (NORMAL, FAILURE, RECOVER)]
    assert cases, 'actual request lost its current user input'
    return cases[-1]


def stage_one(text):
    tag = {NORMAL: 'normal', FAILURE: 'terminal_failure', RECOVER: 'recover'}[text]
    return 'SEQ_STAGE_ONE_' + tag + ': ' + ANSWER


def stage_two(text):
    return 'SEQ_STAGE_TWO\n' + stage_one(text) + '\nOnly this terminal leaf is delivered.'


def assert_leaf_declaration(request, role):
    if role == 'researcher':
        assert_declaration(request)
    else:
        assert not request.get('tools'), 'terminal leaf inherited another leaf tool declaration'
        assert all(m.get('role') != 'tool' and not m.get('tool_calls')
                   for m in request['messages']), 'foreign tools must use SDK text conversion'


class SequenceModelFixture:
    def __init__(self, h):
        self.key, self.writer_key = h.secret(), h.secret()
        self.records, self.errors, self.lock = [], [], threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                if self.path != '/v1/chat/completions':
                    self.send_error(404)
                    return
                record = None
                try:
                    request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    role = role_of(request)
                    expected_key = owner.key if role == 'researcher' else owner.writer_key
                    if self.headers.get('Authorization') != 'Bearer ' + expected_key:
                        self.send_error(401)
                        return
                    assert request['model'] == (RESEARCH_MODEL if role == 'researcher' else WRITER_MODEL)
                    assert_leaf_declaration(request, role)
                    text = current_case(request)
                    record = {'request': copy.deepcopy(request), 'role': role,
                              'authenticated': True, 'status': None, 'text': '', 'complete': False}
                    with owner.lock:
                        owner.records.append(record)
                        ordinal = len(owner.records)
                    if role == 'writer':
                        assert any(stage_one(text) in text_content(m.get('content'))
                                   for m in request['messages']), 'terminal actual request lacks first output'
                        if text == FAILURE:
                            raw = b'{"error":{"message":"terminal fixture rejects current request","type":"authentication_error"}}'
                            self.send_response(401)
                            self.send_header('Content-Type', 'application/json')
                            self.send_header('Content-Length', str(len(raw)))
                            self.end_headers()
                            self.wfile.write(raw)
                            self.wfile.flush()
                            with owner.lock:
                                record.update(status=401, complete=True)
                            return
                        delta = {'content': stage_two(text)}
                    else:
                        messages = request['messages']
                        last = max(i for i, m in enumerate(messages)
                                   if m.get('role') == 'user' and text_content(m.get('content')) == text)
                        results = [tool_text(m.get('content')) for m in messages[last + 1:]
                                   if m.get('role') == 'tool']
                        if not results:
                            delta = {'tool_calls': [{'index': 0, 'id': 'sequence-call-' + str(ordinal),
                                'type': 'function', 'function': {'name': CALLABLE,
                                'arguments': json.dumps({'query': 'orchid'})}}]}
                        else:
                            assert results == [ANSWER], 'first leaf lacks the actual successful MCP result'
                            delta = {'content': stage_one(text)}
                    usage = {'prompt_tokens': 12, 'completion_tokens': 8, 'total_tokens': 20}
                    events = [
                        {'id': 'sequence-fixture', 'object': 'chat.completion.chunk', 'model': request['model'],
                         'choices': [{'index': 0, 'delta': delta,
                         'finish_reason': 'stop' if 'content' in delta else 'tool_calls'}]},
                        {'id': 'sequence-fixture', 'object': 'chat.completion.chunk', 'model': request['model'],
                         'choices': [], 'usage': usage}]
                    raw = (''.join('data: ' + json.dumps(e) + '\n\n' for e in events) + 'data: [DONE]\n\n').encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/event-stream')
                    self.send_header('Content-Length', str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    self.wfile.flush()
                    with owner.lock:
                        record.update(status=200, text=delta.get('content', ''), usage=usage, complete=True)
                except Exception as exc:
                    with owner.lock:
                        owner.errors.append(type(exc).__name__ + ': ' + str(exc))
                        if record is not None:
                            record.update(status=400, complete=True)
                    self.send_error(400, 'sequence model fixture assertion')

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
        self.server.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()


def require_exchange(calls, text, *, live, limit):
    """Assert the observed provider wire; never normalize requests or responses."""
    assert len(calls) == 3, 'one real tool call requires two first-leaf requests and one terminal request'
    roles = [role_of(call['request']) for call in calls]
    assert roles == ['researcher', 'researcher', 'writer'], 'actual HTTP calls violate declared sequence order'
    for call, role in zip(calls, roles):
        request = call['request']
        assert_leaf_declaration(request, role)
        expected_model = 'deepseek-v4-flash' if live else (RESEARCH_MODEL if role == 'researcher' else WRITER_MODEL)
        assert request['model'] == expected_model
        assert request['max_completion_tokens'] == limit and 'max_tokens' not in request
        assert call.get('complete') is True
        assert current_case(request) == text
    assert all(call['status'] == 200 and call['usage']['total_tokens'] > 0 for call in calls[:2])
    assert calls[0]['text'] == '' and calls[1]['text'], 'first leaf must first use tool, then yield its own output'
    first = calls[1]['text']
    assert 'SEQ_STAGE_ONE' in first and 'MCP_ORCHID_627' in first and '09:17 UTC' in first
    values, ids = [], set()
    for call in calls[:2]:
        messages = call['request']['messages']
        last = max(i for i, m in enumerate(messages)
                   if m.get('role') == 'user' and text_content(m.get('content')) == text)
        for message in messages[last + 1:]:
            if message.get('role') == 'tool' and message['tool_call_id'] not in ids:
                ids.add(message['tool_call_id'])
                values.append(tool_text(message.get('content')))
    assert values == [ANSWER], 'actual first model input must consume exactly the real MCP answer'
    assert any(first in text_content(m.get('content')) for m in calls[-1]['request']['messages']), \
        'terminal actual request lacks the exact decoded first-leaf output'
    if text == FAILURE:
        assert not live and calls[-1]['status'] == 401 and calls[-1]['text'] == ''
        terminal = None
    else:
        assert calls[-1]['status'] == 200 and calls[-1]['usage']['total_tokens'] > 0
        terminal = calls[-1]['text']
        assert terminal != first and 'SEQ_STAGE_TWO' in terminal
        assert first in terminal, 'terminal must quote the actual first-leaf output, not invent a canary'
        if not live:
            assert first == stage_one(text) and terminal == stage_two(text)
    return {'roles': roles, 'first_output': first, 'terminal_output': terminal,
            'actual_tool_results': values, 'terminal_failure': text == FAILURE}


def require_accepted_history(previous, candidate, requests):
    assert candidate['parent_ref'] == (previous['candidate']['candidate_ref'] if previous else '')
    assert candidate['parent_digest'] == (previous['candidate']['content_digest'] if previous else '')
    if previous:
        final = previous['delivery']['final_text']
        # The earlier terminal agent is foreign to the next first leaf. Native
        # SDK conversion may render it as user context; require real text only.
        assert any(final in text_content(message.get('content')) for message in requests[0]['messages']), \
            'next first leaf lacks the prior accepted terminal Final'


def all_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for child in value:
            yield from all_strings(child)
    elif isinstance(value, dict):
        for child in value.values():
            yield from all_strings(child)
