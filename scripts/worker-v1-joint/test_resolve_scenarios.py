"""Pure assertion tests. Real owner/PG/process evidence comes from the joint gate."""
import subprocess
import sys
import unittest
from resolve_scenarios import ProfileLock, assert_resolve_once, events_for_run, require_suppressed
from resolve_fault_proxy import RESOLVE_PATH


class ResolveAssertionsTests(unittest.TestCase):
    def test_profile_lock_release_closes_all_pipes_after_early_exit(self):
        lock = ProfileLock.__new__(ProfileLock)
        lock.released_ns = None
        lock.process = subprocess.Popen([sys.executable, '-c', 'pass'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            lock.process.wait(timeout=3)
            lock.release()
            self.assertEqual(lock.exit_code, 0)
            self.assertTrue(all(getattr(lock.process, name).closed
                                for name in ('stdin', 'stdout', 'stderr')))
        finally:
            if lock.process.poll() is None:
                lock.process.kill(); lock.process.wait(timeout=3)
            for name in ('stdin', 'stdout', 'stderr'):
                getattr(lock.process, name).close()

    def test_each_attempt_has_exactly_one_resolve_not_retries_of_old_attempt(self):
        attempts = [{'attempt_id': 'old'}, {'attempt_id': 'new'}]
        proofs = [{'attempt_id': 'old', 'grant_group': 1},
                  {'attempt_id': 'new', 'grant_group': 2}]
        resolves = [{'grant_group': 1}, {'grant_group': 2, 'attempt_id': 'new'}]
        self.assertEqual(assert_resolve_once(resolves, proofs, attempts), {'old': 1, 'new': 1})
        for bad in (resolves+[resolves[0]], resolves[:1], []):
            with self.assertRaises(AssertionError):
                assert_resolve_once(bad, proofs, attempts)

    def test_denial_is_correlated_without_denial_body_identity(self):
        class Proxy:
            def events(self):
                return [{'path': RESOLVE_PATH, 'run_id': 'run', 'grant_group': 1, 'upstream_status': 200},
                        {'path': RESOLVE_PATH, 'grant_group': 1, 'upstream_status': 403},
                        {'path': RESOLVE_PATH, 'run_id': 'unrelated', 'grant_group': 2}]
        result = events_for_run(Proxy(), 'run', RESOLVE_PATH)
        self.assertEqual([e['upstream_status'] for e in result], [200, 403])

    def test_response_fault_needs_actual_owner_200_and_zero_downstream_bytes(self):
        event = {'upstream_status': 200, 'fault': 'drop_after_owner_200',
                 'owner_response_bytes': 200, 'downstream_body_bytes_forwarded': 0,
                 'downstream_action': 'owner_200_suppressed', 'finished_ns': 30,
                 'owner_response_ns': 20}
        self.assertEqual(require_suppressed(event, 'drop_after_owner_200'), event)
        for change in ({'upstream_status': 403}, {'owner_response_bytes': 0},
                       {'downstream_body_bytes_forwarded': 1}, {'downstream_status': 200},
                       {'finished_ns': 19}, {'downstream_action': 'forwarded'}):
            with self.assertRaises(AssertionError):
                require_suppressed(dict(event, **change), 'drop_after_owner_200')


if __name__ == '__main__': unittest.main()
