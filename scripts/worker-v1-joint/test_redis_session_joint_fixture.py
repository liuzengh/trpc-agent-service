"""Unit checks of dependency/admin seams; actual SDK acceptance is the joint gate."""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch, Mock

import redis_session_joint_fixture as fixture


class RedisSessionFixtureTests(unittest.TestCase):
    def test_reuses_original_summary_fixture_without_new_model(self):
        self.assertIs(fixture.SummaryModelFixture, fixture._summary.SummaryModelFixture)
        self.assertTrue(fixture._source.name == 'test-worker-summary-joint.py')

    def test_public_profile_changes_only_fixed_session_selection(self):
        h = fixture.RedisSessionHarness.__new__(fixture.RedisSessionHarness)
        h.session_password = 'secret-in-memory'
        body = {'config': {'models': {'primary': {'model': 'selected'}}, 'storage': {'session': {'kind': 'postgres_state'}}}, 'credentials': {'storage': {'session': {'dsn': 'old'}}}}
        original = copy.deepcopy(body)
        with patch.object(fixture._summary.SummaryHarness, 'api', return_value={'ok': True}) as api:
            self.assertEqual(h.api('PUT', '/v1/tenants/t/runtime-profiles/p/draft', body), {'ok': True})
        sent = api.call_args.args[2]
        self.assertEqual(body, original)
        self.assertEqual(sent['config']['storage']['session'], {'kind': 'managed_session', 'backend_id': 'joint-session-redis', 'backend_revision': 1})
        self.assertEqual(sent['credentials']['storage']['session'], {'dsn_password': {'action': 'replace', 'value': 'secret-in-memory'}})
        self.assertEqual(sent['config']['models'], original['config']['models'])

    def test_missing_parent_is_rename_and_exact_restore_never_rewritten(self):
        h = fixture.RedisSessionHarness.__new__(fixture.RedisSessionHarness)
        h.quarantined = None
        data = {'runtime_session:accepted': 'exact original bytes'}
        calls = []
        def command(*args):
            calls.append(args)
            if args[0] == 'GET': return data.get(args[1])
            if args[0] == 'EXISTS': return int(args[1] in data)
            if args[0] == 'RENAME':
                self.assertNotIn(args[2], data)
                data[args[2]] = data.pop(args[1]); return 'OK'
            self.fail('unexpected Redis mutation')
        h.redis_admin = command
        candidate = {'key': 'runtime_session:accepted', 'raw': 'exact original bytes'}
        saved = h.quarantine_parent(candidate)
        self.assertNotIn(candidate['key'], data)
        self.assertEqual(data[saved['private_key']], candidate['raw'])
        h.restore_parent()
        h.restore_parent()
        self.assertEqual(data, {candidate['key']: candidate['raw']})
        self.assertEqual([c[0] for c in calls if c[0] not in ('GET', 'EXISTS')], ['RENAME', 'RENAME'])
        self.assertIsNone(h.quarantined)

    def test_redis_candidate_readback_preserves_same_snapshot_summary(self):
        h = fixture.RedisSessionHarness.__new__(fixture.RedisSessionHarness)
        body = {'identity': {'tenant_id': 't', 'session_id': 's', 'run_id': 'r', 'attempt_id': 'a'}, 'parent': {'ref': '', 'digest': ''}, 'snapshot': {'session': {'summaries': {'': {'summary': 'exact summary version 7'}}, 'events': []}}}
        raw = json.dumps(body)
        def cmd(*args):
            return {'SCAN': ['0', ['runtime_session:{t}:s:sc1_key']], 'GET': raw, 'PTTL': -1}[args[0]]
        h.redis_admin = cmd
        row = h.session_candidates('r')[0]
        self.assertEqual(row['content'], body)
        self.assertEqual(row['raw'], raw)
        self.assertEqual(row['content']['snapshot']['session']['summaries']['']['summary'], 'exact summary version 7')
        self.assertEqual(h.session_candidates('absent'), [])

    def test_cleanup_restores_parent_then_reclaims_owned_redis_on_parent_error(self):
        h = fixture.RedisSessionHarness.__new__(fixture.RedisSessionHarness)
        h.redis, h.work, h.quarantined = 'session-owned', Path('/no-private-fixture'), None
        actions, records = [], []
        h.restore_parent = lambda: actions.append('restore')
        h.record = lambda name, value: records.append(value)
        h.redact = lambda value: value
        with patch.object(fixture._summary.SummaryHarness, 'close', side_effect=RuntimeError('parent cleanup')), patch.object(fixture.subprocess, 'run') as command:
            command.side_effect = [Mock(returncode=0, stderr=''), Mock(returncode=0), Mock(returncode=1, stderr='Error: No such object: session-owned')]
            with self.assertRaisesRegex(RuntimeError, 'parent cleanup'):
                h.close()
            self.assertEqual(command.call_args_list[1].args[0], ['docker', 'rm', '-f', 'session-owned'])
        self.assertEqual(actions, ['restore'])
        self.assertEqual(records[0]['result'], 'FAIL')
        self.assertTrue(records[0]['removed'])


if __name__ == '__main__':
    unittest.main()
