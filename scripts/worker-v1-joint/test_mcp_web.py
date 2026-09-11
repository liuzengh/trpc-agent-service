"""Pure guard sensitivity for the GUI runner; no service or GUI acceptance here."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest

spec = importlib.util.spec_from_file_location('mcp_web_runner', Path(__file__).resolve().parents[1] / 'test-worker-memory-web.py')
web = importlib.util.module_from_spec(spec)
spec.loader.exec_module(web)
from mcp_joint_fixture import ANSWER, CALLABLE, NORMAL, SELECTED, UNSELECTED


class MCPBrowserGuards(unittest.TestCase):
    def publication(self):
        marker = dict(result='PASS', deployment_id='d', revision_number=2, agent_id='a', agent_version_number=2, profile_id='p', profile_revision_number=2, profile_credential_reopen={'result': 'PASS', 'profile_id': 'p', 'profile_revision_number': 2, 'action': 'keep', 'input_empty': True, 'original_token_absent': True})
        view = dict(sources={'agent': {'agent_id': 'a', 'version_number': 2}, 'profile': {'profile_id': 'p', 'revision_number': 2}}, agent_plan={'root': 'assistant', 'nodes': {'assistant': {'tool_resources': ['search'], 'callable_entries': ['tools/search']}}}, resources={'tools': {'search': {'kind': 'mcp_streamable_http', 'server_url': 'http://127.0.0.1:4321/mcp', 'toolset_name': 'joint_mcp', 'tool_name': SELECTED, 'capability': 'mcp.search', 'auth': {'kind': 'bearer', 'credential_present': True}}}})
        return dict(deployment_id='d', revision_number=2, manifest_view=view), marker

    def test_published_fixed_source_and_general_capability(self):
        publication, marker = self.publication()
        resource = web.verify_mcp_browser_publication(publication, marker, 'http://127.0.0.1:4321/mcp')
        self.assertEqual(resource['capability'], 'mcp.search')
        for field, value in [('capability', 'web.search'), ('kind', 'other'), ('server_url', 'http://127.0.0.1:4322/mcp'), ('toolset_name', 'other'), ('tool_name', UNSELECTED), ('auth', {'kind': 'none'}), ('auth', {'kind': 'bearer', 'credential_present': False})]:
            changed = copy.deepcopy(publication)
            changed['manifest_view']['resources']['tools']['search'][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(AssertionError):
                web.verify_mcp_browser_publication(changed, marker, 'http://127.0.0.1:4321/mcp')

    def test_stale_source_or_extra_selected_tool_rejected(self):
        publication, marker = self.publication()
        for section, field, value in [('agent', 'version_number', 1), ('agent', 'agent_id', 'other'), ('profile', 'revision_number', 1), ('profile', 'profile_id', 'other')]:
            changed = copy.deepcopy(publication)
            changed['manifest_view']['sources'][section][field] = value
            with self.subTest(section=section, field=field), self.assertRaises(AssertionError):
                web.verify_mcp_browser_publication(changed, marker, 'http://127.0.0.1:4321/mcp')
        for field, value in [('tool_resources', ['search', 'unselected']), ('callable_entries', ['tools/search', 'tools/unselected'])]:
            changed = copy.deepcopy(publication)
            changed['manifest_view']['agent_plan']['nodes']['assistant'][field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                web.verify_mcp_browser_publication(changed, marker, 'http://127.0.0.1:4321/mcp')
        with self.assertRaises(AssertionError):
            web.verify_mcp_browser_publication(dict(publication, revision_number=1), marker, 'http://127.0.0.1:4321/mcp')

    def test_additional_unselected_resource_is_rejected_even_if_not_in_node(self):
        publication, marker = self.publication()
        extra = copy.deepcopy(publication['manifest_view']['resources']['tools']['search'])
        extra['tool_name'] = UNSELECTED
        publication['manifest_view']['resources']['tools']['extra'] = extra
        with self.assertRaises(AssertionError):
            web.verify_mcp_browser_publication(publication, marker, 'http://127.0.0.1:4321/mcp')

    def test_browser_must_reopen_credentials_after_publishing_not_only_redact_evidence(self):
        publication, marker = self.publication()
        for field, value in [('result', 'FAIL'), ('profile_id', 'other'), ('profile_revision_number', 1), ('action', 'replace'), ('input_empty', False), ('original_token_absent', False)]:
            changed = copy.deepcopy(marker)
            changed['profile_credential_reopen'][field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                web.verify_mcp_browser_publication(publication, changed, 'http://127.0.0.1:4321/mcp')
        del marker['profile_credential_reopen']
        with self.assertRaises((AssertionError, KeyError)):
            web.verify_mcp_browser_publication(publication, marker, 'http://127.0.0.1:4321/mcp')

    def exchange(self):
        request = dict(model='joint-mcp-fixture', max_completion_tokens=4096, messages=[{'role': 'user', 'content': NORMAL}], tools=[{'type': 'function', 'function': {'name': CALLABLE, 'parameters': {'type': 'object', 'required': ['query'], 'properties': {'query': {'type': 'string'}}}}}])
        followup = copy.deepcopy(request)
        followup['messages'].append({'role': 'tool', 'tool_call_id': 'actual-call-1', 'content': json.dumps({'content': [{'type': 'text', 'text': ANSWER}]})})
        event = dict(method='tool_execution', authenticated=True, name=SELECTED, query='orchid', is_error=False)
        return [request, followup], [event]

    def test_real_result_bytes_and_single_authenticated_selected_execution(self):
        calls, events = self.exchange()
        self.assertEqual(web.verify_mcp_browser_exchange(calls, events, ANSWER, 'joint-mcp-fixture', 4096), [ANSWER])
        for final in [ANSWER.replace('\n', '\\n'), ANSWER.replace('\n', ' '), ANSWER + '\n', '']:
            with self.subTest(final=final), self.assertRaises(AssertionError):
                web.verify_mcp_browser_exchange(calls, events, final, 'joint-mcp-fixture', 4096)
        for field, value in [('authenticated', False), ('name', UNSELECTED), ('query', 'other'), ('is_error', True)]:
            with self.subTest(field=field), self.assertRaises(AssertionError):
                web.verify_mcp_browser_exchange(calls, [dict(events[0], **{field: value})], ANSWER, 'joint-mcp-fixture', 4096)
        for changed in [[], events * 2]:
            with self.assertRaises(AssertionError):
                web.verify_mcp_browser_exchange(calls, changed, ANSWER, 'joint-mcp-fixture', 4096)

    def test_omitted_tool_result_or_provider_parameter_drift_rejected(self):
        calls, events = self.exchange()
        cases = []
        changed = copy.deepcopy(calls); changed[1]['messages'].pop(); cases.append(changed)
        changed = copy.deepcopy(calls); changed[1]['messages'][-1]['content'] = 'fabricated'; cases.append(changed)
        changed = copy.deepcopy(calls); changed[0]['tools'][0]['function']['name'] = UNSELECTED; cases.append(changed)
        changed = copy.deepcopy(calls); changed[0]['tools'] *= 2; cases.append(changed)
        changed = copy.deepcopy(calls); changed[0]['model'] = 'other'; cases.append(changed)
        changed = copy.deepcopy(calls); changed[0]['max_completion_tokens'] = 16384; cases.append(changed)
        changed = copy.deepcopy(calls); changed[0]['max_tokens'] = 4096; cases.append(changed)
        for index, changed in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(AssertionError):
                web.verify_mcp_browser_exchange(changed, events, ANSWER, 'joint-mcp-fixture', 4096)

    def test_cli_preserves_existing_scenarios_and_adds_mcp_without_starting(self):
        result = subprocess.run([sys.executable, '-B', str(Path(web.__file__)), '--help'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('{memory,session,artifact,knowledge,mcp,sequence,parallel,loop}', result.stdout)
        self.assertIn('{postgresql,redis}', result.stdout)


if __name__ == '__main__':
    unittest.main()
