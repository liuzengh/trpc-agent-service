"""Pure gate validation; the --windows process gate supplies crash evidence."""
import base64
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from tracing_windows import revoke, saved_span


class TracingWindowsTests(unittest.TestCase):
    def fixture(self):
        return SimpleNamespace(pg='owned-pg', prefix='owned', containers=['owned-pg'],
                               quote=lambda s: "'" + s + "'", sql=lambda s: [['t']])

    def test_restore_after_failure(self):
        events = []
        with patch('tracing_windows.admin') as execute:
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                with revoke(self.fixture(), [('worker_runtime', 'worker.execution_attempts', 'INSERT')], events):
                    raise RuntimeError('injected')
        self.assertEqual([c.args[1] for c in execute.call_args_list], [
            'REVOKE INSERT ON worker.execution_attempts FROM worker_runtime',
            'GRANT INSERT ON worker.execution_attempts TO worker_runtime'])
        self.assertEqual([e['event'] for e in events], ['privilege_revoked', 'privilege_restored'])

    def test_reject_unowned_database(self):
        h = self.fixture()
        h.containers = []
        with patch('tracing_windows.admin') as execute:
            with self.assertRaises(AssertionError):
                with revoke(h, [('worker_runtime', 'worker.execution_attempts', 'INSERT')], []):
                    self.fail('entered')
            execute.assert_not_called()

    def test_reject_non_allowlisted_grant(self):
        with patch('tracing_windows.admin') as execute:
            with self.assertRaises(AssertionError):
                with revoke(self.fixture(), [('worker_runtime', 'worker.execution_runs', 'DELETE')], []):
                    self.fail('entered')
            execute.assert_not_called()

    def test_saved_span_matches_persisted_identity(self):
        tid, sid = '1' * 32, '2' * 16
        for value in (sid, base64.b64encode(bytes.fromhex(sid)).decode()):
            query = lambda *a: ({}, [{'name': 'handoff', 'spanId': value}])
            self.assertEqual(saved_span(query, 'backend', f'00-{tid}-{sid}-01', 'handoff'), tid)

    def test_saved_span_rejects_same_name_different_identity(self):
        query = lambda *a: ({}, [{'name': 'handoff', 'spanId': '3' * 16}])
        with self.assertRaisesRegex(AssertionError, 'persisted handoff'):
            saved_span(query, 'backend', '00-' + '1' * 32 + '-' + '2' * 16 + '-01', 'handoff')


if __name__ == '__main__':
    unittest.main()
