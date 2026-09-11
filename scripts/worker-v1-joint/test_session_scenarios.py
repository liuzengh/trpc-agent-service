"""Fixture guard tests; actual Session assertions run in the process joint gate."""
import unittest

from session_scenarios import require_partial_receipt, session_login_outage


class PartialReceiptTest(unittest.TestCase):
    def test_request_entered_is_not_partial_sse_evidence(self):
        for record in (None, {}, {'phase': 'request_entered', 'bytes': 100, 'done': False},
                       {'phase': 'partial_sse_flushed', 'bytes': 0, 'done': False},
                       {'phase': 'partial_sse_flushed', 'bytes': 100, 'done': True}):
            with self.subTest(record=record), self.assertRaises(AssertionError):
                require_partial_receipt(record)

    def test_flushed_nonterminal_bytes_are_required(self):
        record = {'phase': 'partial_sse_flushed', 'bytes': 256, 'done': False}
        self.assertEqual(require_partial_receipt(record), record)


class SessionOutageTest(unittest.TestCase):
    def test_exception_restores_login_without_writing_business_rows(self):
        class DedicatedFixture:
            prefix = 'worker-joint-unit'
            pg = prefix + '-pg'
            containers = [pg]
            statements = []
            enabled = True
            def command(self, argv, **kwargs):
                statement = argv[-1]
                self.statements.append(statement)
                if statement == 'ALTER ROLE session_runtime NOLOGIN': self.enabled = False
                if statement == 'ALTER ROLE session_runtime LOGIN': self.enabled = True
                return '1\n'
            def sql(self, query):
                assert query == "SELECT rolcanlogin FROM pg_roles WHERE rolname='session_runtime'"
                return [['t' if self.enabled else 'f']]
        fixture = DedicatedFixture()
        journal = []
        with self.assertRaisesRegex(RuntimeError, 'body failure'):
            with session_login_outage(fixture, journal):
                self.assertFalse(fixture.enabled)
                raise RuntimeError('body failure')
        self.assertTrue(fixture.enabled)
        self.assertEqual(fixture.statements[0], 'ALTER ROLE session_runtime NOLOGIN')
        self.assertEqual(fixture.statements[-1], 'ALTER ROLE session_runtime LOGIN')
        self.assertTrue(all(s.startswith(('ALTER ROLE session_runtime ', 'SELECT pg_terminate_backend(')) for s in fixture.statements))
        self.assertEqual(journal[-1]['event'], 'session_login_restored')

    def test_non_fixture_container_is_rejected(self):
        class NotFixture:
            prefix = 'worker-joint-test'
            pg = 'another-instance'
            containers = ['another-instance']
        with self.assertRaises(AssertionError):
            with session_login_outage(NotFixture(), []):
                self.fail('unbound instance accepted')


if __name__ == '__main__':
    unittest.main()
