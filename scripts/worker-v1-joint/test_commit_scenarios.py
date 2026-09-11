"""Commit acceptance receipt and immutable fixture-target guards."""
import copy
from types import SimpleNamespace
from urllib.parse import urlsplit
import unittest
from unittest.mock import patch

from commit_scenarios import install_proxy, require_commit_receipt


class CommitReceiptTest(unittest.TestCase):
    def receipt(self):
        return {'phase': 'server_commit_observed_response_dropped', 'commit_observed': True,
                'execute_observed': True, 'sync_observed': True, 'command_tag': 'INSERT 0 1',
                'ready_status': 'I', 'error_response_observed': False,
                'successful_response_bytes_forwarded': 0, 'suppressed_bytes': 29,
                'suppressed_message_types': ['2', 'C', 'Z'], 'disconnect_released_by_test': True,
                'bind': {'monotonic_ns': 1}, 'server_ready': {'monotonic_ns': 2},
                'client_disconnect': {'monotonic_ns': 3}}

    def test_execute_or_command_without_idle_commit_is_not_evidence(self):
        for changes in ({'commit_observed':False}, {'ready_status':'T'}, {'command_tag':'INSERT 0 0'},
                        {'execute_observed':False}, {'sync_observed':False}, {'error_response_observed':True},
                        {'suppressed_bytes':0}, {'successful_response_bytes_forwarded':1},
                        {'suppressed_message_types':['2','C']}, {'disconnect_released_by_test':False}):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                require_commit_receipt(dict(self.receipt(), **changes), disconnected=True)

    def test_committed_row_observation_precedes_disconnect(self):
        receipt = self.receipt()
        require_commit_receipt(receipt, disconnected=True)
        receipt.pop('client_disconnect')
        receipt['phase'] = 'server_commit_observed_before_disconnect'
        require_commit_receipt(receipt, disconnected=False)


class InstallTargetTest(unittest.TestCase):
    def fixture(self):
        return SimpleNamespace(prefix='worker-joint-unit', pg='worker-joint-unit-pg', containers=['worker-joint-unit-pg'], pg_port=5432,
                               dsns={'session_runtime':'postgres://session_runtime:special%40%3Apass@127.0.0.1:5432/agent_platform?sslmode=disable',
                                     'worker_runtime':'unchanged-worker', 'session_migrator':'unchanged-migrator'})

    def test_only_session_target_port_changes_before_control_seed(self):
        h = self.fixture(); before=copy.deepcopy(h.dsns)
        proxy = SimpleNamespace(port=19400)
        with patch('commit_scenarios.PGCommitFaultProxy') as constructor:
            constructor.return_value.start.return_value = proxy
            self.assertIs(install_proxy(h), proxy)
        actual = urlsplit(h.dsns['session_runtime']); original=urlsplit(before['session_runtime'])
        self.assertEqual(actual.username, original.username)
        self.assertEqual(actual.password, original.password)
        self.assertEqual(actual.hostname, original.hostname)
        self.assertEqual(actual.port, 19400)
        self.assertEqual(actual.path, original.path)
        self.assertEqual(actual.query, original.query)
        self.assertEqual(h.dsns['worker_runtime'], before['worker_runtime'])
        self.assertEqual(h.dsns['session_migrator'], before['session_migrator'])

    def test_already_published_profile_or_other_database_is_rejected(self):
        for name, value in [('profile_id','sealed-profile'), ('pg','other-pg')]:
            h = self.fixture(); setattr(h,name,value)
            with self.subTest(name=name), self.assertRaises(AssertionError):
                install_proxy(h)


if __name__ == '__main__':
    unittest.main()
