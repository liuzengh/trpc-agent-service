"""Pure helper contract tests; these do not replace the real WV-14 gate."""
import copy
import unittest
from urllib.parse import urlsplit

from credentials_scenarios import credential_update, changed_target_dsn, public_snapshot, initialized_attempt


def published_view():
    return {'profile_id': 'profile-a', 'revision_number': 7,
            'spec_digest': 'sha256:' + '1' * 64,
            'config': {'models': {'primary': {'model': 'fixture'}},
                       'storage': {'session': {'destination': {'database': 'agent_platform'}}}},
            'credential_states': {
                'models': {'primary': {'api_key': {'configured': True, 'status': 'active',
                    'credential_revision': 3, 'association_token': 'a' * 64}}},
                'storage': {'session': {'dsn': {'configured': True, 'status': 'active',
                    'credential_revision': 4, 'association_token': 'b' * 64}}}}}


class CredentialScenarioContractTests(unittest.TestCase):
    def test_live_replace_uses_current_public_association_and_cas(self):
        view = published_view()
        original = copy.deepcopy(view)
        body = credential_update(view, 'models', 'primary', 'api_key', 'replace', 'new-fixture-value')
        self.assertEqual(body, {
            'target': {'profile_revision_number': 7, 'category': 'models',
                'resource_name': 'primary', 'purpose_field': 'api_key',
                'association_token': 'a' * 64},
            'action': 'replace', 'expected_credential_revision': 3, 'value': 'new-fixture-value'})
        self.assertEqual(view, original)
        clear = credential_update(view, 'storage', 'session', 'dsn', 'clear')
        self.assertNotIn('value', clear)
        self.assertEqual(clear['expected_credential_revision'], 4)

    def test_initialized_attempt_distinguishes_run_and_attempt_states(self):
        run = {'status': 'RUNNING', 'current_attempt_id': 'attempt-a'}
        attempts = [{'attempt_id': 'attempt-a', 'status': 'EXECUTING', 'agent_started_at': '2026-09-07T00:00:00Z'}]
        self.assertEqual(initialized_attempt(run, attempts), attempts[0])
        for bad_run in [{'status': 'EXECUTING', 'current_attempt_id': 'attempt-a'},
                        {'status': 'RUNNING', 'current_attempt_id': 'attempt-b'}]:
            with self.assertRaises(AssertionError): initialized_attempt(bad_run, attempts)
        with self.assertRaises(AssertionError): initialized_attempt(run, attempts * 2)
        with self.assertRaises(AssertionError):
            initialized_attempt(run, [{'attempt_id': 'attempt-a', 'status': 'PREPARING', 'agent_started_at': None}])

    def test_changed_target_is_valid_database_change_not_password_reinterpretation(self):
        source = 'postgres://session_runtime:p%40ss%3A%2F%3F@127.0.0.1:5432/agent_platform?sslmode=disable'
        result = changed_target_dsn(source)
        self.assertEqual(result, 'postgres://session_runtime:p%40ss%3A%2F%3F@127.0.0.1:5432/agent_platform_wv14_changed?sslmode=disable')
        self.assertEqual(urlsplit(source).netloc, urlsplit(result).netloc)
        self.assertEqual(urlsplit(source).query, urlsplit(result).query)

    def test_evidence_retains_versions_but_excludes_association_capabilities(self):
        view = published_view()
        snapshot = public_snapshot(view)
        self.assertNotIn('association_token', str(snapshot))
        self.assertNotIn('a' * 64, str(snapshot))
        self.assertEqual(snapshot['credential_states']['models']['primary']['api_key'],
                         {'configured': True, 'status': 'active', 'credential_revision': 3})
        snapshot['config']['models']['primary']['model'] = 'edited'
        self.assertEqual(view['config']['models']['primary']['model'], 'fixture')

    def test_terminal_clear_and_missing_value_do_not_construct_live_replacement(self):
        view = published_view()
        view['credential_states']['models']['primary']['api_key']['status'] = 'cleared'
        with self.assertRaises(ValueError): credential_update(view, 'models', 'primary', 'api_key', 'replace', 'new')
        view = published_view()
        with self.assertRaises(ValueError): credential_update(view, 'models', 'primary', 'api_key', 'replace')
        with self.assertRaises(ValueError): credential_update(view, 'models', 'primary', 'api_key', 'clear', 'extra')


if __name__ == '__main__':
    unittest.main()
