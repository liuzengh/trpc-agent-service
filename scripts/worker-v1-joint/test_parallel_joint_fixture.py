"""Actual HTTP fixture seams for parallel execution; no service stack."""
import copy
import json
import threading
import urllib.request
import urllib.error
import socket
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
import parallel_joint_fixture as f


class ParallelFixtureTests(unittest.TestCase):
    def harness(self, live=False):
        h = f.ParallelHarness.__new__(f.ParallelHarness)
        h.live = live
        h.model_name = 'deepseek-v4-flash' if live else f.MODELS['research_a']
        h.mcp_url, h.mcp_token = 'http://127.0.0.1:19001/mcp', 'mcp-private'
        h.model = type('Model', (), {'url': 'http://127.0.0.1:19002', 'key': 'a-private',
            'keys': {'research_a': 'a-private', 'research_b': 'b-private', 'aggregator': 'c-private'}})()
        return h

    def test_public_graph_and_three_model_credentials_preserve_leaf_authority(self):
        h = self.harness()
        body = {'spec': {'requirements': {'models': {}, 'tools': {}, 'knowledge': {}}, 'nodes': {}}}
        old = copy.deepcopy(body)
        with patch.object(f.Harness, 'api', return_value={}) as api:
            h.api('PUT', '/v1/tenants/t/agents/a/draft', body)
        spec = api.call_args.args[3]['spec']
        self.assertEqual(body, old)
        self.assertEqual(spec['root'], 'workflow')
        self.assertEqual(spec['nodes']['workflow'], {'kind': 'sequence', 'children': ['research', 'aggregator']})
        self.assertEqual(spec['nodes']['research'], {'kind': 'parallel', 'children': ['research_a', 'research_b']})
        self.assertEqual(spec['nodes']['research_a']['tool_slots'], ['search_a'])
        self.assertEqual(spec['nodes']['research_b']['tool_slots'], ['search_b'])
        self.assertEqual(spec['nodes']['aggregator']['tool_slots'], [])
        body = {'config': {'models': {'primary': {'kind': 'openai_compatible', 'model': h.model_name,
            'base_url': h.model.url + '/v1'}}, 'storage': {'session': {'kind': 'postgres_state'}}},
            'credentials': {'models': {'primary': {'api_key': {'action': 'replace', 'value': h.model.key}}},
                            'storage': {'session': {'dsn': {'action': 'replace', 'value': 'dsn-private'}}}}}
        old = copy.deepcopy(body)
        with patch.object(f.Harness, 'api', return_value={}) as api:
            h.api('PUT', '/v1/tenants/t/runtime-profiles/p/draft', body)
        sent = api.call_args.args[3]
        self.assertEqual(body, old)
        self.assertEqual(set(sent['config']['models']), {'primary', 'secondary', 'aggregate'})
        self.assertEqual(len({v['api_key']['value'] for v in sent['credentials']['models'].values()}), 3)
        self.assertEqual(len({v['base_url'] for v in sent['config']['models'].values()}), 1)
        self.assertEqual(set(sent['config']['tools']), {'search_a', 'search_b'})
        self.assertEqual({v['tool_name'] for v in sent['config']['tools'].values()}, {'selected_search'})
        self.assertEqual(sent['config']['storage'], old['config']['storage'])
        self.assertEqual(sent['credentials']['storage'], old['credentials']['storage'])



    def model_harness(self):
        h = self.harness()
        keys = iter(['a-private', 'b-private', 'c-private'])
        h.secret = lambda: next(keys)
        return h

    def request_for(self, role, case, result=False):
        messages = [{'role': 'system', 'content': f.INSTRUCTIONS[role]}, {'role': 'user', 'content': case}]
        request = {'model': f.MODELS[role], 'messages': messages, 'max_completion_tokens': 16384}
        if role in f.BRANCHES:
            request['tools'] = [{'function': {'name': f.CALLABLES[role], 'parameters': {
                'type': 'object', 'properties': {'query': {'type': 'string'}}, 'required': ['query']}}}]
            if result:
                messages.append({'role': 'tool', 'tool_call_id': role, 'content': json.dumps([{'type': 'text', 'text': f.ANSWER}])})
        else:
            messages.append({'role': 'user', 'content': '\n'.join(f.branch_output(r, case) for r in f.BRANCHES)})
        return request

    def post(self, model, request):
        role = f.role_of(request)
        req = urllib.request.Request(model.url + '/v1/chat/completions', data=json.dumps(request).encode(),
            headers={'Authorization': 'Bearer ' + model.keys[role]})
        try: response = urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError as error: response = error
        with response:
            status, raw = response.status, response.read().decode()
        events = [json.loads(line[6:]) for line in raw.splitlines() if line.startswith('data: {')]
        return status, events[0]['choices'][0]['delta'] if events else None

    def test_actual_http_barrier_and_opposite_branch_completion_orders(self):
        model = f.ParallelModelFixture(self.model_harness())
        try:
            with ThreadPoolExecutor(max_workers=2) as workers:
                for case, expected in [(f.A_FIRST, list(f.BRANCHES)), (f.B_FIRST, list(reversed(f.BRANCHES)))]:
                    offset = len(model.snapshot())
                    first = [workers.submit(self.post, model, self.request_for(role, case)) for role in f.BRANCHES]
                    tool_calls = {}
                    for role, future in zip(f.BRANCHES, first):
                        code, delta = future.result(timeout=12)
                        self.assertEqual(code, 200)
                        self.assertEqual(delta['tool_calls'][0]['function']['name'], f.CALLABLES[role])
                        tool_calls[role] = delta['tool_calls'][0]
                    requests = []
                    for role in f.BRANCHES:
                        request = self.request_for(role, case, True)
                        request['messages'][-1]['tool_call_id'] = tool_calls[role]['id']
                        request['messages'].insert(-1, {'role': 'assistant', 'tool_calls': [tool_calls[role]]})
                        requests.append(request)
                    second = [workers.submit(self.post, model, request) for request in requests]
                    for role, future in zip(f.BRANCHES, second):
                        code, delta = future.result(timeout=12)
                        self.assertEqual((code, delta['content']), (200, f.branch_output(role, case)))
                    self.assertEqual(self.post(model, self.request_for('aggregator', case)), (200, {'content': f.aggregate_output(case)}))
                    records = model.snapshot()[offset:]
                    initial = [r for r in records if r['phase'] == 'tool_select']
                    self.assertEqual(len(initial), 2)
                    self.assertLess(max(r['started_ns'] for r in initial), min(r['finished_ns'] for r in initial))
                    final = sorted([r for r in records if r['phase'] == 'branch_output'], key=lambda r: r['finished_ns'])
                    self.assertEqual([r['role'] for r in final], expected)
                    self.assertLess(final[0]['finished_ns'], final[1]['response_started_ns'])
                    self.assertTrue(all(r['complete'] and r['authenticated'] for r in records))
                    exchange = f.require_exchange(records, case, live=False, limit=16384)
                    self.assertEqual(exchange['completion_order'], expected)
                    self.assertEqual(exchange['terminal_output'], f.aggregate_output(case))
                    for mutation in ('serial', 'missing_branch', 'authority_leak'):
                        wrong = copy.deepcopy(records)
                        if mutation == 'serial':
                            group = [r for r in wrong if r['phase'] == 'tool_select']
                            group[1]['started_ns'] = group[0]['finished_ns']
                        if mutation == 'missing_branch':
                            next(r for r in wrong if r['role'] == 'aggregator')['request']['messages'].pop()
                        if mutation == 'authority_leak':
                            next(r for r in wrong if r['role'] == 'aggregator')['request']['tools'] = wrong[0]['request']['tools']
                        with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                            f.require_exchange(wrong, case, live=False, limit=16384)
            self.assertEqual(model.errors, [])
        finally:
            model.close()

    def test_actual_socket_disconnect_observed_after_other_branch_401(self):
        model = f.ParallelModelFixture(self.model_harness())
        connection = socket.create_connection(('127.0.0.1', model.server.server_port), timeout=10)
        try:
            payload = json.dumps(self.request_for('research_a', f.FAILURE)).encode()
            headers = ('POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer ' +
                model.keys['research_a'] + '\r\nContent-Length: ' + str(len(payload)) + '\r\n\r\n').encode()
            connection.sendall(headers + payload)
            code, _ = self.post(model, self.request_for('research_b', f.FAILURE))
            self.assertEqual(code, 401)
            connection.close()
            self.assertTrue(model.completed[f.FAILURE]['research_a'].wait(timeout=5))
            records = model.snapshot()
            left = next(r for r in records if r['role'] == 'research_a')
            self.assertIsNone(left['status'])
            self.assertEqual(left['termination'], 'client_disconnect')
            self.assertTrue(left['complete'])
            self.assertEqual(len(records), 2)
            self.assertTrue(f.require_exchange(records, f.FAILURE, live=False, limit=16384)['branch_failure'])
            self.assertEqual(model.errors, [])
        finally:
            connection.close()
            model.close()



    def test_live_observer_preserves_actual_request_and_response_bytes(self):
        seen = []
        response_bytes = b'data: {"id":"fixture","choices":[{"delta":{"content":"line1\\nline2"}}]}\n\ndata: {"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4}}\n\ndata: [DONE]\n\n'
        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                seen.append(self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Content-Length', str(len(response_bytes)))
                self.end_headers()
                self.wfile.write(response_bytes)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        h = self.harness(live=True); h.redact = lambda value: value
        relay = f.LiveRelay(h, 'live-private', 'https://approved.example/v1', 'deepseek-v4-flash')
        observed = f.TimedLiveView(relay)
        payload = b'{ "model" : "deepseek-v4-flash", "stream":true, "messages": [{"role":"user","content":"quote \\" and path \\\\"}] }'
        # Use the actual existing LiveRelay with an actual HTTP fixture socket as
        # its test-only upstream. No external request or TLS claim in this unit.
        try:
            with patch.object(http.client, 'HTTPSConnection', side_effect=lambda *a, **kw: http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=5)):
                req = urllib.request.Request(observed.url + '/v1/chat/completions', data=payload,
                    headers={'Authorization': 'Bearer live-private', 'Content-Type': 'application/json'})
                with urllib.request.urlopen(req, timeout=5) as result:
                    self.assertEqual(result.read(), response_bytes)
            self.assertTrue(observed.wait_idle(timeout=5))
            self.assertEqual(seen, [payload])
            calls = observed.snapshot()
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]['request'], json.loads(payload))
            self.assertEqual(calls[0]['text'], 'line1\nline2')
            self.assertTrue(calls[0]['complete'])
            self.assertLess(calls[0]['started_ns'], calls[0]['finished_ns'])
            self.assertEqual(calls[0]['interval_kind'], 'relay_http_handler_monotonic')
            self.assertEqual(calls[0]['request_bytes_sha256'], f.hashlib.sha256(payload).hexdigest())
            self.assertNotIn('live-private', json.dumps(calls))
        finally:
            observed.close()
            server.shutdown(); server.server_close(); thread.join(timeout=5)



    def runner(self):
        path = Path(__file__).resolve().parents[1] / 'test-worker-parallel-joint.py'
        spec = importlib.util.spec_from_file_location('parallel_runner_for_unit', path)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        return module

    def test_only_aggregate_accepted_and_failure_never_advances_parent(self):
        runner = self.runner()
        candidate = {'candidate_ref': 'sc1_new', 'content_digest': 'sha256:new', 'attempt_id': 'att',
            'parent_ref': '', 'parent_digest': '', 'content': {'snapshot': ['branchA', 'branchB', 'aggregate actual']}}
        round_ = {'input': f.A_FIRST, 'run': {'run_id': 'run', 'attempts': 1, 'current_attempt_id': 'att', 'session_sequence': 1, 'status': 'SUCCEEDED'},
            'head_after': {'accepted_ref': 'sc1_new', 'accepted_digest': 'sha256:new', 'settled_sequence': 1},
            'head_before': None, 'attempts': [{'attempt_id': 'att', 'status': 'SUCCEEDED'}],
            'completions': [{'completion_id': 'cmp', 'kind': 'ATTEMPT', 'attempt_id': 'att', 'status': 'SUCCEEDED',
                'reply_disposition': 'FINAL', 'final_intent_id': 'fin', 'candidate_ref': 'sc1_new', 'candidate_digest': 'sha256:new'}],
            'outboxes': [{'intent_id': 'fin', 'payload': {'execution': {'completion_id': 'cmp', 'attempt_id': 'att'}, 'content': {'text': 'aggregate actual'}}}],
            'delivery': {'run_id': 'run', 'intent_id': 'fin', 'delivery_state': 'ACCEPTED', 'final_text': 'aggregate actual'},
            'exchange': {'branch_outputs': {'research_a': 'branchA', 'research_b': 'branchB'}, 'terminal_output': 'aggregate actual'},
            'candidates': [candidate], 'model_calls': []}
        self.assertEqual(runner.require_facts(round_, None), candidate)
        for mutation in ('first_final', 'missing_snapshot_output', 'duplicate_completion'):
            wrong = copy.deepcopy(round_)
            if mutation == 'first_final':
                wrong['delivery']['final_text'] = wrong['outboxes'][0]['payload']['content']['text'] = 'branchA'
            if mutation == 'missing_snapshot_output': wrong['candidates'][0]['content']['snapshot'].pop(0)
            if mutation == 'duplicate_completion': wrong['completions'] *= 2
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError): runner.require_facts(wrong, None)
        failed = copy.deepcopy(round_); failed['input'] = f.FAILURE
        failed['run']['status'] = failed['attempts'][0]['status'] = failed['completions'][0]['status'] = 'FAILED'
        failed['attempts'][0]['reason'] = failed['completions'][0]['reason'] = 'RUNTIME_FAILED'
        failed['completions'][0].update(candidate_ref='', candidate_digest='')
        failed['candidates'] = []
        failed['head_before'] = copy.deepcopy(failed['head_after'])
        failed['delivery']['final_text'] = failed['outboxes'][0]['payload']['content']['text'] = runner.FAILURE_FINAL
        failed['exchange'] = {'branch_failure': True, 'sibling_http_termination': 'client_disconnect'}
        self.assertIsNone(runner.require_facts(failed, None))
        wrong = copy.deepcopy(failed); wrong['head_after']['accepted_ref'] = 'polluted'
        with self.assertRaises(AssertionError): runner.require_facts(wrong, None)
        wrong = copy.deepcopy(failed); wrong['candidates'] = [candidate]
        with self.assertRaises(AssertionError): runner.require_facts(wrong, None)

    def test_published_manifest_requires_explicit_aggregator_and_distinct_model_ids(self):
        runner, h = self.runner(), self.harness()
        nodes = {'workflow': {'kind': 'sequence', 'children': ['research', 'aggregator']},
                 'research': {'kind': 'parallel', 'children': list(f.BRANCHES)}}
        for role in f.ROLES:
            nodes[role] = {'kind': 'llm', 'model_resource': f.SLOTS[role],
                'tool_resources': [f.TOOLS[role]] if role in f.BRANCHES else [],
                'callable_entries': ['tools/' + f.TOOLS[role]] if role in f.BRANCHES else []}
        content = {'agent_plan': {'root': 'workflow', 'nodes': nodes}, 'resources': {
            'models': {slot: {'base_url': h.model.url + '/v1', 'credential': {'credential_id': 'crd_' + slot}} for slot in f.SLOTS.values()},
            'tools': {slot: {'server_url': h.mcp_url, 'tool_name': 'selected_search', 'auth': {'kind': 'bearer',
                'credential': {'purpose': 'bearer_token'}}} for slot in f.TOOLS.values()}}}
        runner.require_manifest({'content': content}, h)
        for mutation in ('root_parallel', 'shared_model_credential', 'aggregate_tool'):
            wrong = copy.deepcopy(content)
            if mutation == 'root_parallel': wrong['agent_plan']['root'] = 'research'
            if mutation == 'shared_model_credential': wrong['resources']['models']['secondary']['credential']['credential_id'] = 'crd_primary'
            if mutation == 'aggregate_tool': wrong['agent_plan']['nodes']['aggregator']['tool_resources'] = ['search_a']
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError): runner.require_manifest({'content': wrong}, h)


if __name__ == '__main__':
    unittest.main()
