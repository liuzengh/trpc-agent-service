"""Pure Loop GUI guards; no browser or service stack starts here."""
import copy
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('loop_web_runner', Path(__file__).resolve().parents[1] / 'test-worker-memory-web.py')
web = importlib.util.module_from_spec(spec)
spec.loader.exec_module(web)
from loop_joint_fixture import LoopHarness, MODEL
from harness import Harness


class LoopBrowserGuards(unittest.TestCase):
    def fixture(self):
        marker = dict(result='PASS', deployment_id='d', revision_number=2, agent_id='a', agent_version_number=2, profile_id='p', profile_revision_number=1, initial_max_iterations=1, published_max_iterations=2, profile_credential_reopen=dict(result='PASS', profile_id='p', profile_revision_number=1, resource='primary', action='keep', input_empty=True, original_key_absent=True))
        nodes = {'workflow': dict(kind='loop', body='assistant', max_iterations=2), 'assistant': dict(kind='llm', instruction='Explicit instruction.' + web.LOOP_GUI_INSTRUCTION_SUFFIX, model_resource='primary', tool_resources=[], callable_entries=[], knowledge_resources=[])}
        view = dict(sources=dict(agent=dict(agent_id='a', version_number=2), profile=dict(profile_id='p', revision_number=1)), agent_plan=dict(root='workflow', nodes=nodes), resources=dict(models=dict(primary=dict(model=MODEL)), tools={}, knowledge={}))
        return dict(deployment_id='d', revision_number=2, manifest_view=view), marker

    def test_actual_iteration_change_explicit_body_and_original_profile(self):
        publication, marker = self.fixture()
        web.verify_loop_browser_publication(publication, marker)
        for field, value in [('agent_version_number', 1), ('profile_revision_number', 2), ('revision_number', 1), ('initial_max_iterations', 2), ('published_max_iterations', 1)]:
            with self.subTest(field=field), self.assertRaises(AssertionError):
                web.verify_loop_browser_publication(publication, dict(marker, **{field:value}))
        for field, value in [('kind', 'sequence'), ('body', 'other'), ('max_iterations', 1), ('max_iterations', 3), ('children', [])]:
            changed = copy.deepcopy(publication)
            changed['manifest_view']['agent_plan']['nodes']['workflow'][field] = value
            with self.subTest(field=field,value=value), self.assertRaises(AssertionError):
                web.verify_loop_browser_publication(changed, marker)

    def test_extra_authority_or_unedited_instruction_and_key_echo_rejected(self):
        publication, marker = self.fixture()
        for category in ('models','tools','knowledge'):
            changed = copy.deepcopy(publication)
            changed['manifest_view']['resources'][category]['extra'] = {}
            with self.subTest(category=category), self.assertRaises(AssertionError):
                web.verify_loop_browser_publication(changed, marker)
        for field,value in [('tool_resources',['search']),('callable_entries',['tools/search']),('memory',{'tools':['memory_add']}),('instruction','Original unedited instruction')]:
            changed = copy.deepcopy(publication)
            changed['manifest_view']['agent_plan']['nodes']['assistant'][field]=value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                web.verify_loop_browser_publication(changed, marker)
        changed = copy.deepcopy(marker)
        changed['profile_credential_reopen']['original_key_absent'] = False
        with self.assertRaises(AssertionError):web.verify_loop_browser_publication(publication,changed)

    def test_formal_candidate_retains_both_iterations_but_only_last_final(self):
        first,last='初稿\n"quoted"\\path','末次\n初稿\n"quoted"\\path'
        exchange,delivery=dict(first_output=first,terminal_output=last),dict(final_text=last)
        web.verify_sequence_candidate_content(dict(content=[first,last]),exchange,delivery)
        for content in ([first],[last],[first,last.replace('\n','\\n')]):
            with self.subTest(content=content),self.assertRaises(AssertionError):
                web.verify_sequence_candidate_content(dict(content=content),exchange,delivery)
        with self.assertRaises(AssertionError):web.verify_sequence_candidate_content(dict(content=[first,last]),exchange,dict(final_text=first))

    def test_mixin_changes_only_initial_seed_bound_and_retains_profile_and_binding(self):
        class GUIHarness(web.WebPublicationMixin,LoopHarness):pass
        h=GUIHarness.__new__(GUIHarness);h.revision_number=2;h.initial_max_iterations=1
        with patch.object(Harness,'api',return_value={}) as api:
            h.api('POST','/v1/auth/login',{'username':'joint-owner'})
            h.api('POST','/v1/me/change-password',{'new_password':'private-fixture-password'})
            self.assertEqual(h.owner_password,'private-fixture-password')
            h.api('POST','/v1/tenants/t/channel-bindings',{'target':{'revision_number':1}})
            self.assertEqual(api.call_args.args[3]['target']['revision_number'],2)
            h.api('PUT','/v1/tenants/t/agents/a/draft',{'spec':{'requirements':{},'nodes':{}}})
            seeded=api.call_args.args[3]['spec']
            self.assertEqual(seeded['root'],'workflow')
            self.assertEqual(seeded['nodes']['workflow'],dict(kind='loop',body='assistant',max_iterations=1))
            self.assertEqual(set(seeded['nodes']),{'workflow','assistant'})
            self.assertEqual(seeded['requirements'],dict(models={'primary':{'capabilities':['chat']}},tools={},knowledge={}))
            original=dict(config={'models':{'primary':{'model':MODEL}},'tools':{}},credentials={})
            h.api('PUT','/v1/tenants/t/runtime-profiles/p/draft',original)
            self.assertEqual(api.call_args.args[3],original)
            self.assertFalse(hasattr(h,'mcp_url'))


if __name__=='__main__':unittest.main()
