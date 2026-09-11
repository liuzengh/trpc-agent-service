"""Pure Parallel GUI guards; actual browser/SDK evidence comes from the runner."""
import copy
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('parallel_web_runner', Path(__file__).resolve().parents[1] / 'test-worker-memory-web.py')
web = importlib.util.module_from_spec(spec)
spec.loader.exec_module(web)
from parallel_joint_fixture import ParallelHarness, BRANCHES, ROLES, SLOTS, TOOLS, MODELS
from harness import Harness


class ParallelBrowserGuards(unittest.TestCase):
    def fixture(self):
        marker = dict(result='PASS', deployment_id='d', revision_number=2, agent_id='a', agent_version_number=2, profile_id='p', profile_revision_number=2, profile_credential_reopen=dict(result='PASS', profile_id='p', profile_revision_number=2, action='keep', resources=['search_a', 'search_b'], input_empty=True, original_token_absent=True))
        nodes = {'workflow': dict(kind='sequence', children=['research', 'aggregator']), 'research': dict(kind='parallel', children=list(BRANCHES))}
        for role in ROLES:
            nodes[role] = dict(kind='llm', model_resource=SLOTS[role], instruction='Explicit instruction.' + web.PARALLEL_GUI_INSTRUCTION_SUFFIX, tool_resources=[TOOLS[role]] if role in BRANCHES else [], callable_entries=['tools/' + TOOLS[role]] if role in BRANCHES else [])
        tools = {slot: dict(kind='mcp_streamable_http', capability='mcp.search', server_url='http://127.0.0.1:1234/mcp', toolset_name='joint_mcp', tool_name='selected_search', auth=dict(kind='bearer', credential_present=True)) for slot in TOOLS.values()}
        view = dict(sources=dict(agent=dict(agent_id='a', version_number=2), profile=dict(profile_id='p', revision_number=2)), agent_plan=dict(root='workflow', nodes=nodes), resources=dict(models={SLOTS[role]: dict(model=MODELS[role]) for role in ROLES}, tools=tools))
        return dict(deployment_id='d', revision_number=2, manifest_view=view), marker

    def verify(self, publication, marker):
        web.verify_parallel_browser_publication(publication, marker, 'http://127.0.0.1:1234/mcp')

    def test_exact_ordered_parallel_and_explicit_gui_aggregator(self):
        publication, marker = self.fixture()
        self.verify(publication, marker)
        mutations = [('workflow', 'children', ['aggregator', 'research']), ('research', 'kind', 'sequence'), ('research', 'children', list(reversed(BRANCHES))), ('research_a', 'tool_resources', ['search_b']), ('research_b', 'callable_entries', ['tools/search_a']), ('aggregator', 'tool_resources', ['search_a']), ('aggregator', 'instruction', 'Unedited initial instruction')]
        for node, field, value in mutations:
            changed = copy.deepcopy(publication)
            changed['manifest_view']['agent_plan']['nodes'][node][field] = value
            with self.subTest(node=node, field=field), self.assertRaises(AssertionError):
                self.verify(changed, marker)

    def test_fixed_versions_whole_resource_closure_and_both_credentials_reopened(self):
        publication, marker = self.fixture()
        for field in ('agent_version_number', 'profile_revision_number', 'revision_number'):
            with self.subTest(field=field), self.assertRaises(AssertionError):
                self.verify(publication, dict(marker, **{field: 1}))
        for category in ('models', 'tools'):
            changed = copy.deepcopy(publication)
            changed['manifest_view']['resources'][category]['extra'] = {}
            with self.subTest(category=category), self.assertRaises(AssertionError):
                self.verify(changed, marker)
        for slot in TOOLS.values():
            changed = copy.deepcopy(publication)
            changed['manifest_view']['resources']['tools'][slot]['capability'] = 'web.search'
            with self.subTest(slot=slot), self.assertRaises(AssertionError):
                self.verify(changed, marker)
        for field, value in [('resources', ['search_a']), ('input_empty', False), ('original_token_absent', False)]:
            changed = copy.deepcopy(marker)
            changed['profile_credential_reopen'][field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                self.verify(publication, changed)

    def test_actual_candidate_has_both_branches_and_final_but_only_final_delivered(self):
        first, second = 'branch A\n原文', 'branch B\n\t"quoted"\\value'
        final = 'aggregate\n' + first + '\n' + second
        exchange = dict(branch_outputs=dict(research_a=first, research_b=second), terminal_output=final)
        delivery = dict(final_text=final)
        web.verify_parallel_candidate_content(dict(content=[first, second, final]), exchange, delivery)
        for missing in (first, second, final):
            with self.subTest(missing=missing), self.assertRaises(AssertionError):
                web.verify_parallel_candidate_content(dict(content=[value for value in (first, second, final) if value != missing]), exchange, delivery)
        for text in (first, second, final.replace('\n', '\\n')):
            with self.subTest(delivery=text), self.assertRaises(AssertionError):
                web.verify_parallel_candidate_content(dict(content=[first, second, final]), exchange, dict(final_text=text))

    def test_publication_mixin_preserves_parallel_seed_and_fixed_binding(self):
        class GUIHarness(web.WebPublicationMixin, ParallelHarness): pass
        h = GUIHarness.__new__(GUIHarness)
        h.revision_number, h.live, h.model_name = 2, False, MODELS['research_a']
        h.model = SimpleNamespace(url='http://127.0.0.1:1235', keys={role: 'private-' + role for role in ROLES})
        h.mcp_url, h.mcp_token = 'http://127.0.0.1:1234/mcp', 'private-fixture-token'
        with patch.object(Harness, 'api', return_value={}) as api:
            h.api('POST', '/v1/auth/login', {'username': 'joint-owner'})
            h.api('POST', '/v1/me/change-password', {'new_password': 'private-fixture-password'})
            self.assertEqual(h.owner_password, 'private-fixture-password')
            h.api('POST', '/v1/tenants/t/channel-bindings', {'target': {'revision_number': 1}})
            self.assertEqual(api.call_args.args[3]['target']['revision_number'], 2)
            h.api('PUT', '/v1/tenants/t/agents/a/draft', {'spec': {'requirements': {}, 'nodes': {}}})
            seeded = api.call_args.args[3]['spec']
            self.assertEqual(seeded['root'], 'workflow')
            self.assertEqual(set(seeded['nodes']), {'workflow', 'research', *ROLES})
            self.assertEqual(seeded['nodes']['workflow']['children'], ['research', 'aggregator'])
            self.assertEqual(seeded['nodes']['research']['kind'], 'parallel')
            self.assertEqual(seeded['nodes']['research']['children'], list(BRANCHES))
            self.assertEqual(seeded['nodes']['aggregator']['tool_slots'], [])
            h.api('PUT', '/v1/tenants/t/runtime-profiles/p/draft', {'config': {}, 'credentials': {}})
            profile = api.call_args.args[3]
            self.assertEqual(set(profile['config']['models']), set(SLOTS.values()))
            self.assertEqual(set(profile['config']['tools']), set(TOOLS.values()))
            self.assertEqual(set(profile['credentials']['tools']), set(TOOLS.values()))


if __name__ == '__main__': unittest.main()
