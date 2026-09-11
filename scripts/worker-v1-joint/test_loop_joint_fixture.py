"""Loop fixture public seams; no Docker, Worker or external model starts here."""
import copy
import json
import importlib.util
from pathlib import Path
import urllib.error
import urllib.request
import unittest
from unittest.mock import patch

import loop_joint_fixture as f


class LoopFixtureTests(unittest.TestCase):
    def test_public_draft_keeps_bounded_body_without_extra_capabilities(self):
        h = f.LoopHarness.__new__(f.LoopHarness)
        original = {'expected_revision': 1, 'spec': {'schema_version': 'v1',
            'root': 'assistant', 'requirements': {'models': {'primary': {'capabilities': ['chat']}},
                'tools': {}, 'knowledge': {}}, 'nodes': {'assistant': {'kind': 'llm'}}}}
        body = copy.deepcopy(original)
        with patch.object(f.Harness, 'api', return_value={}) as api:
            h.api('PUT', '/v1/tenants/t/agents/a/draft', body)
        spec = api.call_args.args[3]['spec']
        self.assertEqual(body, original)
        self.assertEqual(spec['root'], 'workflow')
        self.assertEqual(spec['nodes']['workflow'], {'kind': 'loop', 'body': 'assistant', 'max_iterations': 2})
        self.assertEqual(spec['nodes']['assistant'], {'kind': 'llm', 'instruction': f.INSTRUCTION,
            'model_slot': 'primary', 'tool_slots': [], 'knowledge_slots': []})
        self.assertEqual(spec['requirements'], original['spec']['requirements'])
        profile = {'config': {'models': {'primary': {'model': 'fixed-model'}}, 'tools': {},
            'knowledge': {}, 'storage': {'session': {'kind': 'postgres_state'}}},
            'credentials': {'models': {'primary': {'api_key': {'action': 'keep'}}}}}
        with patch.object(f.Harness, 'api', return_value={}) as api:
            h.api('PUT', '/v1/tenants/t/runtime-profiles/p/draft', profile)
        self.assertEqual(api.call_args.args[3], profile)

    def test_gui_seed_can_start_at_one_without_changing_execution_assertion(self):
        h = f.LoopHarness.__new__(f.LoopHarness)
        h.initial_max_iterations = 1
        body = {'spec': {}}
        with patch.object(f.Harness, 'api', return_value={}) as api:
            h.api('PUT', '/v1/tenants/t/agents/a/draft', body)
        self.assertEqual(api.call_args.args[3]['spec']['nodes']['workflow']['max_iterations'], 1)
        for invalid in (0, 33, True, 1.5):
            h.initial_max_iterations = invalid
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                h.api('PUT', '/v1/tenants/t/agents/a/draft', body)

    def test_real_http_two_iterations_history_and_terminal_401(self):
        h = type('Fixture', (), {'secret': lambda self: 'private-loop-key'})()
        model = f.LoopModelFixture(h)
        try:
            def post(request, key):
                request = urllib.request.Request(model.url + '/v1/chat/completions',
                    data=json.dumps(request).encode(), headers={'Authorization': 'Bearer ' + key})
                try:
                    response = urllib.request.urlopen(request, timeout=3)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    status, raw = response.status, response.read().decode()
                events = [json.loads(line[6:]) for line in raw.splitlines() if line.startswith('data: {')]
                return status, events[0]['choices'][0]['delta']['content'] if events else None
            for case in (f.NORMAL, f.FAILURE, f.RECOVER):
                request = {'model': f.MODEL, 'max_completion_tokens': 16384, 'stream': True,
                    'messages': [{'role': 'system', 'content': f.INSTRUCTION}, {'role': 'user', 'content': case}]}
                self.assertEqual(post(request, 'wrong-key')[0], 401)
                status, first = post(request, model.key)
                self.assertEqual(status, 200)
                self.assertEqual(first, f.first_output(case))
                request['messages'].append({'role': 'assistant', 'content': first})
                status, last = post(request, model.key)
                self.assertEqual(status, 401 if case == f.FAILURE else 200)
                if case != f.FAILURE:
                    self.assertEqual(last, f.terminal_output(case))
            self.assertTrue(model.wait_idle(3))
            calls = model.snapshot()
            self.assertEqual(len(calls), 6)
            self.assertEqual([c['iteration'] for c in calls], [1, 2] * 3)
            self.assertEqual([c['status'] for c in calls], [200, 200, 200, 401, 200, 200])
            for index, case in enumerate((f.NORMAL, f.FAILURE, f.RECOVER)):
                observed = calls[index * 2:index * 2 + 2]
                result = f.require_exchange(observed, case, live=False, limit=16384)
                self.assertEqual(result['first_output'], f.first_output(case))
                self.assertEqual(result['terminal_failure'], case == f.FAILURE)
            self.assertFalse(model.errors)
            self.assertNotIn(model.key, json.dumps(calls))
        finally:
            model.close()
        self.assertFalse(model.thread.is_alive())

    def runner(self):
        path = Path(__file__).resolve().parents[1] / 'test-worker-loop-joint.py'
        spec = importlib.util.spec_from_file_location('loop_runner_unit', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_accepted_facts_require_last_iteration_whole_snapshot_and_no_failed_candidate(self):
        runner = self.runner()
        previous = {'candidate': {'candidate_ref': 'sc1_prior', 'content_digest': 'sha256:prior'},
            'delivery': {'final_text': 'previous accepted final'}}
        first, last = f.first_output(f.RECOVER), f.terminal_output(f.RECOVER)
        candidate = {'candidate_ref': 'sc1_new', 'content_digest': 'sha256:new', 'attempt_id': 'attempt2',
            'parent_ref': 'sc1_prior', 'parent_digest': 'sha256:prior',
            'content': {'snapshot': {'events': [{'content': first}, {'content': last}]}}}
        state = {'input': f.RECOVER, 'run': {'run_id': 'run2', 'status': 'SUCCEEDED', 'attempts': 1,
                'current_attempt_id': 'attempt2', 'session_sequence': 2},
            'attempts': [{'attempt_id': 'attempt2', 'status': 'SUCCEEDED', 'agent_started_at': 'start', 'ended_at': 'end'}],
            'completions': [{'completion_id': 'completion2', 'kind': 'ATTEMPT', 'status': 'SUCCEEDED',
                'attempt_id': 'attempt2', 'reply_disposition': 'FINAL', 'final_intent_id': 'final2',
                'candidate_ref': 'sc1_new', 'candidate_digest': 'sha256:new'}],
            'outboxes': [{'intent_id': 'final2', 'payload': {'execution': {'completion_id': 'completion2',
                'attempt_id': 'attempt2'}, 'content': {'text': last}}}],
            'candidates': [candidate],
            'head_before': {'accepted_ref': 'sc1_prior', 'accepted_digest': 'sha256:prior', 'settled_sequence': 1},
            'head_after': {'accepted_ref': 'sc1_new', 'accepted_digest': 'sha256:new', 'settled_sequence': 2},
            'delivery': {'delivery_state': 'ACCEPTED', 'run_id': 'run2', 'intent_id': 'final2', 'final_text': last},
            'exchange': {'first_output': first, 'terminal_output': last, 'terminal_failure': False},
            'model_calls': [{'request': {'messages': [{'role': 'assistant', 'content': 'previous accepted final'}]}}]}
        self.assertEqual(runner.require_facts(state, previous), candidate)
        for mutation in ('early_final', 'missing_event', 'duplicate_candidate', 'wrong_parent', 'wrong_attempt'):
            wrong = copy.deepcopy(state)
            if mutation == 'early_final': wrong['delivery']['final_text'] = first
            if mutation == 'missing_event': wrong['candidates'][0]['content']['snapshot']['events'].pop(0)
            if mutation == 'duplicate_candidate': wrong['candidates'] *= 2
            if mutation == 'wrong_parent': wrong['candidates'][0]['parent_digest'] = 'wrong'
            if mutation == 'wrong_attempt': wrong['outboxes'][0]['payload']['execution']['attempt_id'] = 'wrong'
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                runner.require_facts(wrong, previous)

        failed = copy.deepcopy(state)
        failed.update(input=f.FAILURE, candidates=[])
        failed['run']['status'] = 'FAILED'
        failed['attempts'][0].update(status='FAILED', reason='RUNTIME_FAILED')
        failed['completions'][0].update(status='FAILED', reason='RUNTIME_FAILED', candidate_ref='', candidate_digest='')
        failed['head_after'].update(accepted_ref='sc1_prior', accepted_digest='sha256:prior')
        failed['delivery']['final_text'] = failed['outboxes'][0]['payload']['content']['text'] = runner.FAILURE_FINAL
        failed['exchange'].update(terminal_failure=True, terminal_output=None)
        self.assertIsNone(runner.require_facts(failed, previous))
        for mutation in ('candidate', 'advanced_head', 'success_final'):
            wrong = copy.deepcopy(failed)
            if mutation == 'candidate': wrong['candidates'] = [candidate]
            if mutation == 'advanced_head': wrong['head_after']['accepted_digest'] = 'sha256:new'
            if mutation == 'success_final': wrong['delivery']['final_text'] = first
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                runner.require_facts(wrong, previous)

    def test_exchange_rejects_escaped_missing_extra_or_leaked_history(self):
        first, last = f.first_output(f.NORMAL), f.terminal_output(f.NORMAL)
        calls = []
        for i, text in enumerate((first, last), 1):
            messages = [{'role': 'system', 'content': f.INSTRUCTION}, {'role': 'user', 'content': f.NORMAL}]
            if i == 2:
                messages.append({'role': 'assistant', 'content': first})
            calls.append({'request': {'model': f.MODEL, 'messages': messages, 'max_completion_tokens': 16384},
                'started_ns': i * 10, 'finished_ns': i * 10 + 1, 'authenticated': True, 'iteration': i,
                'status': 200, 'complete': True, 'termination': 'response_complete', 'text': text,
                'usage': {'total_tokens': 20}})
        f.require_exchange(calls, f.NORMAL, live=False, limit=16384)
        for mutation in ('escaped', 'missing', 'extra', 'tool', 'cap', 'first_history', 'early_final'):
            wrong = copy.deepcopy(calls)
            if mutation == 'escaped': wrong[1]['request']['messages'][-1]['content'] = json.dumps(first)
            if mutation == 'missing': wrong[1]['request']['messages'].pop()
            if mutation == 'extra': wrong.append(copy.deepcopy(calls[1]))
            if mutation == 'tool': wrong[1]['request']['tools'] = [{'function': {'name': 'unexpected'}}]
            if mutation == 'cap': wrong[1]['request']['max_completion_tokens'] = 1024
            if mutation == 'first_history': wrong[0]['request']['messages'].append({'role': 'assistant', 'content': first})
            if mutation == 'early_final': wrong[1]['text'] = first
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                f.require_exchange(wrong, f.NORMAL, live=False, limit=16384)
        calls[1]['request']['messages'][-1]['content'] = [{'type': 'text', 'text': first}, None]
        f.require_exchange(calls, f.NORMAL, live=False, limit=16384)
        live = copy.deepcopy(calls)
        for call in live:
            call['request']['model'] = 'deepseek-v4-flash'
        f.require_exchange(live, f.NORMAL, live=True, limit=16384)

    def test_manifest_keeps_exact_body_bound_and_no_tools(self):
        runner = self.runner()
        h = type('Fixture', (), {'model_name': f.MODEL, 'model': type('Model', (), {'url': 'http://127.0.0.1:12345'})()})()
        envelope = {'content': {'agent_plan': {'root': 'workflow', 'nodes': {
            'workflow': {'kind': 'loop', 'body': 'assistant', 'max_iterations': 2},
            'assistant': {'kind': 'llm', 'model_resource': 'primary', 'instruction': f.INSTRUCTION,
                'tool_resources': [], 'knowledge_resources': [], 'callable_entries': []}}},
            'resources': {'models': {'primary': {'base_url': h.model.url + '/v1', 'model': f.MODEL,
                'credential': {'credential_id': 'actual-model-id', 'purpose': 'api_key'}}}, 'tools': {}, 'knowledge': {}}}}
        runner.require_manifest(envelope, h)
        for mutation in ('bound', 'body', 'tools', 'model'):
            wrong = copy.deepcopy(envelope)
            if mutation == 'bound': wrong['content']['agent_plan']['nodes']['workflow']['max_iterations'] = 1
            if mutation == 'body': wrong['content']['agent_plan']['nodes']['workflow']['body'] = 'missing'
            if mutation == 'tools': wrong['content']['agent_plan']['nodes']['assistant']['callable_entries'] = ['tools/unexpected']
            if mutation == 'model': wrong['content']['resources']['models']['primary']['model'] = 'other-model'
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                runner.require_manifest(wrong, h)

    def test_cleanup_reports_fail_after_base_cleanup_and_unreaped_process(self):
        for base_error, model_error, live_process in ((True, False, False), (False, True, False), (False, False, True)):
            h = f.LoopHarness.__new__(f.LoopHarness)
            h.model = type('Model', (), {'errors': ['model failure'] if model_error else []})()
            h.processes = [('owned', type('Process', (), {'poll': lambda self: None if live_process else 0})())]
            records = {}
            h.record = lambda name, value: records.update({name: value})
            with patch.object(f.Harness, 'close', side_effect=RuntimeError('base cleanup failed') if base_error else None) as close:
                with self.assertRaises(RuntimeError):
                    h.close()
            close.assert_called_once()
            self.assertEqual(records['loop-cleanup.json']['result'], 'FAIL')
            self.assertEqual(records['loop-cleanup.json']['processes_reaped'], not live_process)



if __name__ == '__main__':
    unittest.main()
