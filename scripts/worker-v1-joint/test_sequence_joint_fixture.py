"""Public fixture seams; no service stack or external model is started here."""
import copy
import json
import unittest
import urllib.request
import urllib.error
import importlib.util
from pathlib import Path
from unittest.mock import patch
import sequence_joint_fixture as f


class SequenceFixtureTests(unittest.TestCase):
    def harness(self, live=False):
        h = f.SequenceHarness.__new__(f.SequenceHarness)
        h.live = live
        h.model_name = 'deepseek-v4-flash' if live else f.RESEARCH_MODEL
        h.mcp_url = 'http://127.0.0.1:19001/mcp'
        h.mcp_token = 'private-mcp'
        h.model = type('Model', (), {'url': 'http://127.0.0.1:19002',
            'key': 'research-private', 'writer_key': 'writer-private'})()
        return h

    def test_public_drafts_keep_nested_order_separate_models_and_leaf_tool_scope(self):
        h = self.harness()
        body = {'expected_revision': 1, 'spec': {'schema_version': 'v1',
            'root': 'assistant', 'requirements': {'models': {'primary': {'capabilities': ['chat']}},
            'tools': {}, 'knowledge': {}}, 'nodes': {'assistant': {'kind': 'llm'}}}}
        original = copy.deepcopy(body)
        with patch.object(f.Harness, 'api', return_value={}) as api:
            h.api('PUT', '/v1/tenants/t/agents/a/draft', body)
        sent = api.call_args.args[3]['spec']
        self.assertEqual(body, original)
        self.assertEqual(sent['root'], 'workflow')
        self.assertEqual(sent['nodes']['workflow'], {'kind': 'sequence', 'children': ['prepare', 'writer']})
        self.assertEqual(sent['nodes']['prepare'], {'kind': 'sequence', 'children': ['researcher']})
        self.assertEqual(sent['nodes']['researcher']['model_slot'], 'primary')
        self.assertEqual(sent['nodes']['researcher']['tool_slots'], ['search'])
        self.assertEqual(sent['nodes']['writer']['model_slot'], 'writer')
        self.assertEqual(sent['nodes']['writer']['tool_slots'], [])
        self.assertEqual(sent['requirements']['models']['writer'], {'capabilities': ['chat']})
        profile = {'config': {'models': {'primary': {'kind': 'openai_compatible',
            'model': h.model_name, 'base_url': h.model.url + '/v1', 'capabilities': ['chat']}},
            'tools': {}, 'storage': {'session': {'kind': 'postgres_state'}}},
            'credentials': {'models': {'primary': {'api_key': {'action': 'replace', 'value': h.model.key}}},
            'storage': {'session': {'dsn': {'action': 'replace', 'value': 'private-dsn'}}}}}
        original = copy.deepcopy(profile)
        with patch.object(f.Harness, 'api', return_value={}) as api:
            h.api('PUT', '/v1/tenants/t/runtime-profiles/p/draft', profile)
        sent = api.call_args.args[3]
        self.assertEqual(profile, original)
        self.assertEqual(sent['config']['models']['writer']['model'], f.WRITER_MODEL)
        self.assertEqual(sent['config']['models']['writer']['base_url'], h.model.url + '/v1')
        self.assertEqual(sent['credentials']['models']['writer']['api_key']['value'], 'writer-private')
        self.assertEqual(sent['credentials']['models']['primary']['api_key']['value'], 'research-private')
        self.assertEqual(sent['config']['storage'], original['config']['storage'])
        self.assertEqual(sent['credentials']['storage'], original['credentials']['storage'])
        self.assertEqual(sent['config']['tools']['search']['tool_name'], 'selected_search')


    def test_real_http_two_model_credentials_ordered_output_and_terminal_401(self):
        h = self.harness()
        keys = iter(['research-private', 'writer-private'])
        h.secret = lambda: next(keys)
        model = f.SequenceModelFixture(h)
        declarations = [{'type': 'function', 'function': {'name': f.CALLABLE,
            'parameters': {'type': 'object', 'properties': {'query': {'type': 'string'}},
            'required': ['query']}}}]
        try:
            def post(request, key):
                req = urllib.request.Request(model.url + '/v1/chat/completions',
                    data=json.dumps(request).encode(), headers={'Authorization': 'Bearer ' + key})
                try:
                    response = urllib.request.urlopen(req, timeout=3)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    status, raw = response.status, response.read().decode()
                events = [json.loads(line[6:]) for line in raw.splitlines() if line.startswith('data: {')]
                return status, events[0]['choices'][0]['delta'] if events else None
            for text in (f.NORMAL, f.FAILURE):
                messages = [{'role': 'system', 'content': f.RESEARCH_INSTRUCTION},
                    {'role': 'user', 'content': text}]
                request = {'model': f.RESEARCH_MODEL, 'messages': messages, 'tools': declarations}
                code, delta = post(request, model.key)
                self.assertEqual(code, 200)
                call = delta['tool_calls'][0]
                self.assertEqual(json.loads(call['function']['arguments']), {'query': 'orchid'})
                messages += [{'role': 'assistant', 'tool_calls': [call]}, {'role': 'tool',
                    'tool_call_id': call['id'], 'content': json.dumps([{'type': 'text', 'text': f.ANSWER}])}]
                code, delta = post(request, model.key)
                self.assertEqual(code, 200)
                first_output = delta['content']
                self.assertEqual(first_output, f.stage_one(text))
                # Native SDK foreign-agent context is plain user text, not the
                # first leaf's callable declaration/tool-role protocol.
                writer = {'model': f.WRITER_MODEL, 'messages': [
                    {'role': 'system', 'content': f.WRITER_INSTRUCTION},
                    {'role': 'user', 'content': text},
                    {'role': 'user', 'content': 'Foreign specialist said:\n' + first_output}]}
                self.assertEqual(post(writer, model.key)[0], 401)
                code, delta = post(writer, model.writer_key)
                self.assertEqual(code, 401 if text == f.FAILURE else 200)
                if text != f.FAILURE:
                    self.assertEqual(delta['content'], f.stage_two(text))
            self.assertEqual(model.errors, [])
            records = model.snapshot()
            self.assertEqual([r['role'] for r in records], ['researcher', 'researcher', 'writer'] * 2)
            self.assertEqual([r['status'] for r in records], [200, 200, 200, 200, 200, 401])
            self.assertTrue(all(r['authenticated'] and r['complete'] for r in records))
            self.assertNotIn(model.key, json.dumps(records))
            self.assertNotIn(model.writer_key, json.dumps(records))
        finally:
            model.close()



    def test_exchange_requires_actual_first_output_and_terminal_only_final(self):
        first = f.stage_one(f.NORMAL)
        calls = [
            {'request': {'model': f.RESEARCH_MODEL, 'max_completion_tokens': 16384,
                'messages': [{'role': 'system', 'content': f.RESEARCH_INSTRUCTION},
                             {'role': 'user', 'content': f.NORMAL}],
                'tools': [{'function': {'name': f.CALLABLE, 'parameters': {'type': 'object',
                    'properties': {'query': {'type': 'string'}}, 'required': ['query']}}}]},
             'status': 200, 'complete': True, 'text': '', 'usage': {'total_tokens': 20}},
            {'request': {'model': f.RESEARCH_MODEL, 'max_completion_tokens': 16384,
                'messages': [{'role': 'system', 'content': f.RESEARCH_INSTRUCTION},
                             {'role': 'user', 'content': f.NORMAL}, {'role': 'tool',
                             'tool_call_id': 'one', 'content': json.dumps([{'type': 'text', 'text': f.ANSWER}])}],
                'tools': [{'function': {'name': f.CALLABLE, 'parameters': {'type': 'object',
                    'properties': {'query': {'type': 'string'}}, 'required': ['query']}}}]},
             'status': 200, 'complete': True, 'text': first, 'usage': {'total_tokens': 20}},
            {'request': {'model': f.WRITER_MODEL, 'max_completion_tokens': 16384,
                'messages': [{'role': 'system', 'content': f.WRITER_INSTRUCTION},
                             {'role': 'user', 'content': f.NORMAL},
                             {'role': 'user', 'content': 'Foreign output:\n' + first}]},
             'status': 200, 'complete': True, 'text': f.stage_two(f.NORMAL), 'usage': {'total_tokens': 20}}]
        report = f.require_exchange(calls, f.NORMAL, live=False, limit=16384)
        self.assertEqual(report['first_output'], first)
        self.assertEqual(report['terminal_output'], f.stage_two(f.NORMAL))
        for change in ('order', 'missing', 'escaped', 'tool_leak', 'wire_limit'):
            wrong = copy.deepcopy(calls)
            if change == 'order': wrong[1], wrong[2] = wrong[2], wrong[1]
            if change == 'missing': wrong[2]['request']['messages'].pop()
            if change == 'escaped': wrong[2]['request']['messages'][-1]['content'] = json.dumps(first)
            if change == 'tool_leak': wrong[2]['request']['tools'] = wrong[0]['request']['tools']
            if change == 'wire_limit': wrong[2]['request']['max_completion_tokens'] = 1024
            with self.subTest(change=change), self.assertRaises(AssertionError):
                f.require_exchange(wrong, f.NORMAL, live=False, limit=16384)
        candidate = {'parent_ref': 'sc1_accepted', 'parent_digest': 'sha256:accepted'}
        previous = {'candidate': {'candidate_ref': 'sc1_accepted', 'content_digest': 'sha256:accepted'},
                    'delivery': {'final_text': 'prior "quoted" \\ text\n第二行'}}
        requests = [{'messages': [{'role': 'user', 'content': 'foreign context\n' + previous['delivery']['final_text']}]}]
        f.require_accepted_history(previous, candidate, requests)
        wrong = copy.deepcopy(candidate); wrong['parent_digest'] = 'sha256:wrong'
        with self.assertRaises(AssertionError): f.require_accepted_history(previous, wrong, requests)
        requests[0]['messages'][0]['content'] = json.dumps(previous['delivery']['final_text'])
        with self.assertRaises(AssertionError): f.require_accepted_history(previous, candidate, requests)
        self.assertEqual(f.text_content([{'type': 'text', 'text': '中文\n\n"quote"\ttab\\path'}, None]), '中文\n\n"quote"\ttab\\path')



    def test_live_draft_keeps_real_model_with_two_explicit_credential_slots(self):
        h = self.harness(live=True)
        body = {'config': {'models': {'primary': {'kind': 'openai_compatible',
            'model': 'deepseek-v4-flash', 'base_url': h.model.url + '/v1'}}, 'storage': {}},
            'credentials': {'models': {'primary': {'api_key': {'action': 'replace', 'value': h.model.key}}}}}
        with patch.object(f.Harness, 'api', return_value={}) as api:
            h.api('PUT', '/v1/tenants/t/runtime-profiles/p/draft', body)
        sent = api.call_args.args[3]
        self.assertEqual(set(sent['credentials']['models']), {'primary', 'writer'})
        self.assertEqual(sent['credentials']['models']['writer']['api_key']['value'], h.model.key)
        self.assertEqual([v['model'] for v in sent['config']['models'].values()],
                         ['deepseek-v4-flash', 'deepseek-v4-flash'])
        self.assertEqual(len({v['base_url'] for v in sent['config']['models'].values()}), 1)

    def runner(self):
        path = Path(__file__).resolve().parents[1] / 'test-worker-sequence-joint.py'
        spec = importlib.util.spec_from_file_location('sequence_runner_for_unit', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_accepted_facts_reject_intermediate_final_or_failed_candidate(self):
        runner = self.runner()
        before = {'accepted_ref': 'sc1_prior', 'accepted_digest': 'sha256:prior', 'settled_sequence': 1}
        previous = {'candidate': {'candidate_ref': 'sc1_prior', 'content_digest': 'sha256:prior'},
                    'delivery': {'final_text': 'previous terminal'}}
        candidate = {'candidate_ref': 'sc1_new', 'content_digest': 'sha256:new', 'attempt_id': 'attempt2',
                     'parent_ref': 'sc1_prior', 'parent_digest': 'sha256:prior'}
        round_ = {'input': f.RECOVER, 'run': {'run_id': 'run2', 'status': 'SUCCEEDED', 'attempts': 1,
                     'current_attempt_id': 'attempt2', 'session_sequence': 2},
            'attempts': [{'attempt_id': 'attempt2', 'status': 'SUCCEEDED'}],
            'completions': [{'kind': 'ATTEMPT', 'status': 'SUCCEEDED', 'attempt_id': 'attempt2',
                'reply_disposition': 'FINAL', 'final_intent_id': 'final2',
                'candidate_ref': 'sc1_new', 'candidate_digest': 'sha256:new'}],
            'outboxes': [{'intent_id': 'final2'}], 'candidates': [candidate], 'head_before': before,
            'head_after': {'accepted_ref': 'sc1_new', 'accepted_digest': 'sha256:new', 'settled_sequence': 2},
            'delivery': {'delivery_state': 'ACCEPTED', 'run_id': 'run2', 'intent_id': 'final2',
                         'final_text': 'terminal actual'},
            'exchange': {'terminal_output': 'terminal actual', 'first_output': 'intermediate actual'},
            'model_calls': [{'request': {'messages': [{'role': 'user', 'content': 'previous terminal'}]}}]}
        self.assertEqual(runner.require_facts(round_, previous), candidate)
        for mutation in ('intermediate', 'duplicate_completion', 'wrong_parent', 'wrong_attempt'):
            wrong = copy.deepcopy(round_)
            if mutation == 'intermediate': wrong['delivery']['final_text'] = 'intermediate actual'
            if mutation == 'duplicate_completion': wrong['completions'] *= 2
            if mutation == 'wrong_parent': wrong['candidates'][0]['parent_digest'] = 'sha256:wrong'
            if mutation == 'wrong_attempt': wrong['completions'][0]['attempt_id'] = 'other'
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                runner.require_facts(wrong, previous)
        failed = copy.deepcopy(round_)
        failed.update(input=f.FAILURE, candidates=[])
        failed['run']['status'] = 'FAILED'
        failed['attempts'][0].update(status='FAILED', reason='RUNTIME_FAILED', agent_started_at='started', ended_at='ended')
        failed['completions'][0].update(status='FAILED', reason='RUNTIME_FAILED', candidate_ref='', candidate_digest='')
        failed['head_after'].update(accepted_ref='sc1_prior', accepted_digest='sha256:prior')
        failed['delivery']['final_text'] = runner.FAILURE_FINAL
        self.assertIsNone(runner.require_facts(failed, previous))
        for mutation in ('candidate', 'advanced_head', 'success_text'):
            wrong = copy.deepcopy(failed)
            if mutation == 'candidate': wrong['candidates'] = [candidate]
            if mutation == 'advanced_head': wrong['head_after']['accepted_ref'] = 'sc1_new'
            if mutation == 'success_text': wrong['delivery']['final_text'] = 'intermediate actual'
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                runner.require_facts(wrong, previous)

    def test_manifest_requires_nested_order_and_distinct_credential_ids(self):
        runner, h = self.runner(), self.harness()
        envelope = {'content': {'agent_plan': {'root': 'workflow', 'nodes': {
            'workflow': {'kind': 'sequence', 'children': ['prepare', 'writer']},
            'prepare': {'kind': 'sequence', 'children': ['researcher']},
            'researcher': {'kind': 'llm', 'model_resource': 'primary', 'tool_resources': ['search'], 'callable_entries': ['tools/search']},
            'writer': {'kind': 'llm', 'model_resource': 'writer', 'tool_resources': [], 'callable_entries': []}}},
            'resources': {'models': {'primary': {'base_url': h.model.url + '/v1', 'credential': {'credential_id': 'model-one'}},
                                    'writer': {'base_url': h.model.url + '/v1', 'credential': {'credential_id': 'model-two'}}},
                'tools': {'search': {'server_url': h.mcp_url, 'tool_name': 'selected_search', 'auth': {
                    'credential': {'purpose': 'bearer_token', 'credential_id': 'mcp-one'}}}}}}}
        runner.require_manifest(envelope, h)
        for mutation in ('order', 'shared_id', 'writer_tool'):
            wrong = copy.deepcopy(envelope)
            if mutation == 'order': wrong['content']['agent_plan']['nodes']['workflow']['children'].reverse()
            if mutation == 'shared_id': wrong['content']['resources']['models']['writer']['credential']['credential_id'] = 'model-one'
            if mutation == 'writer_tool': wrong['content']['agent_plan']['nodes']['writer']['tool_resources'] = ['search']
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                runner.require_manifest(wrong, h)


if __name__ == '__main__':
    unittest.main()
