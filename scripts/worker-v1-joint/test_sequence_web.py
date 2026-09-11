"""Pure GUI publication/observation guards, not a substitute for browser evidence."""
import copy
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('sequence_web_runner', Path(__file__).resolve().parents[1] / 'test-worker-memory-web.py')
web = importlib.util.module_from_spec(spec)
spec.loader.exec_module(web)
from sequence_joint_fixture import SequenceHarness
from harness import Harness


class SequenceBrowserGuards(unittest.TestCase):
    def fixture(self):
        marker = dict(result='PASS', deployment_id='d', revision_number=2, agent_id='a', agent_version_number=2, profile_id='p', profile_revision_number=2, profile_credential_reopen=dict(result='PASS', profile_id='p', profile_revision_number=2, action='keep', input_empty=True, original_token_absent=True))
        nodes = {'workflow': dict(kind='sequence', children=['prepare', 'writer']), 'prepare': dict(kind='sequence', children=['researcher']), 'researcher': dict(kind='llm', model_resource='primary', tool_resources=['search'], callable_entries=['tools/search']), 'writer': dict(kind='llm', model_resource='writer', tool_resources=[], callable_entries=[])}
        view = dict(sources=dict(agent=dict(agent_id='a', version_number=2), profile=dict(profile_id='p', revision_number=2)), agent_plan=dict(root='workflow', nodes=nodes), resources=dict(models=dict(primary={}, writer={}), tools=dict(search=dict(kind='mcp_streamable_http', capability='mcp.search', server_url='http://127.0.0.1:1234/mcp', toolset_name='joint_mcp', tool_name='selected_search', auth=dict(kind='bearer', credential_present=True)))))
        return dict(deployment_id='d', revision_number=2, manifest_view=view), marker

    def verify(self, publication, marker):
        web.verify_sequence_browser_publication(publication, marker, 'http://127.0.0.1:1234/mcp')

    def test_exact_gui_versions_nested_sequence_and_node_authority(self):
        publication, marker = self.fixture()
        self.verify(publication, marker)
        for field, value in [('agent_version_number', 1), ('profile_revision_number', 1), ('revision_number', 1), ('agent_id', 'other'), ('profile_id', 'other')]:
            with self.subTest(field=field), self.assertRaises(AssertionError):
                self.verify(publication, dict(marker, **{field: value}))
        for node, field, value in [('workflow', 'children', ['writer', 'prepare']), ('prepare', 'children', ['writer']), ('prepare', 'kind', 'parallel'), ('writer', 'model_resource', 'primary'), ('writer', 'tool_resources', ['search']), ('writer', 'callable_entries', ['tools/search'])]:
            changed = copy.deepcopy(publication)
            changed['manifest_view']['agent_plan']['nodes'][node][field] = value
            with self.subTest(node=node, field=field), self.assertRaises(AssertionError):
                self.verify(changed, marker)

    def test_extra_resources_or_wrong_capability_and_missing_reopen_rejected(self):
        publication, marker = self.fixture()
        for category in ('models', 'tools'):
            changed = copy.deepcopy(publication)
            changed['manifest_view']['resources'][category]['extra'] = {}
            with self.subTest(category=category), self.assertRaises(AssertionError):
                self.verify(changed, marker)
        changed = copy.deepcopy(publication)
        changed['manifest_view']['resources']['tools']['search']['capability'] = 'web.search'
        with self.assertRaises(AssertionError): self.verify(changed, marker)
        changed = copy.deepcopy(marker); changed['profile_credential_reopen']['input_empty'] = False
        with self.assertRaises(AssertionError): self.verify(publication, changed)

    def test_formal_candidate_contains_both_outputs_but_delivery_only_terminal(self):
        first, final = 'first\n真实工具结果', 'terminal\nfirst\n真实工具结果'
        candidate = {'content': {'snapshot': {'events': [{'content': first}, {'content': final}]}}}
        exchange, delivery = dict(first_output=first, terminal_output=final), dict(final_text=final)
        web.verify_sequence_candidate_content(candidate, exchange, delivery)
        for value in (first, final, '', final.replace('\n', '\\n')):
            with self.subTest(candidate=value), self.assertRaises(AssertionError):
                web.verify_sequence_candidate_content({'content': {'only_one': value}}, exchange, delivery)
        with self.assertRaises(AssertionError): web.verify_sequence_candidate_content(candidate, exchange, dict(final_text=first))

    def test_existing_publication_mixin_preserves_sequence_seed_hook(self):
        class GUIHarness(web.WebPublicationMixin, SequenceHarness): pass
        h = GUIHarness.__new__(GUIHarness)
        h.revision_number = 2
        with patch.object(Harness, 'api', return_value={}) as api:
            h.api('POST', '/v1/auth/login', {'username': 'joint-owner'})
            h.api('POST', '/v1/me/change-password', {'new_password': 'private-fixture-value'})
            self.assertEqual(h.owner_password, 'private-fixture-value')
            h.api('POST', '/v1/tenants/t/channel-bindings', {'target': {'revision_number': 1}})
            self.assertEqual(api.call_args.args[3]['target']['revision_number'], 2)
            h.api('PUT', '/v1/tenants/t/agents/a/draft', {'spec': {'requirements': {}, 'nodes': {}}})
            seeded = api.call_args.args[3]['spec']
            self.assertEqual(seeded['root'], 'workflow')
            self.assertEqual(set(seeded['nodes']), {'workflow', 'prepare', 'researcher', 'writer'})
            self.assertEqual(seeded['nodes']['workflow']['children'], ['prepare', 'writer'])
            self.assertNotIn('assistant', seeded['nodes'])


if __name__ == '__main__': unittest.main()
