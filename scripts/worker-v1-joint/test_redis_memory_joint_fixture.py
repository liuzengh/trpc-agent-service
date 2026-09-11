"""Fixture unit checks are separate from actual Redis/Control/SDK acceptance."""
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch, Mock

from redis_memory_joint_fixture import RedisMemoryHarness, RedisCommandError, WRITE_FAILURE, WRITE_RESTORE, _reply


class RedisFixtureTests(unittest.TestCase):
    def test_resp_bulk_array_nil_and_errors(self):
        self.assertEqual(_reply(io.BytesIO(b'*3\r\n$3\r\nabc\r\n:7\r\n$-1\r\n')), ['abc', 7, None])
        with self.assertRaises(RedisCommandError) as caught:
            _reply(io.BytesIO(b'-NOPERM original private value\r\n'))
        self.assertNotIn('private', str(caught.exception))
        for value in (b'$4\r\nabc\r\n', b'$9000000\r\n', b'*10001\r\n', b'+truncated'):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                _reply(io.BytesIO(value))

    def test_only_exact_failure_sql_maps_to_acl_and_never_writes_data(self):
        h = RedisMemoryHarness.__new__(RedisMemoryHarness)
        h.redis_faults = []
        h._record = lambda *_: None
        calls = []
        h.redis_admin = lambda *args: calls.append(args) or 'OK'
        h.admin_sql(WRITE_FAILURE)
        h.admin_sql(WRITE_RESTORE)
        self.assertEqual(calls, [('ACL', 'SETUSER', 'memory_runtime', '-mset'), ('ACL', 'SETUSER', 'memory_runtime', '+mset')])
        self.assertTrue(all(not row['database_state_modified'] for row in h.redis_faults))
        with patch('memory_joint_fixture.MemoryHarness.admin_sql', return_value='PG') as pg:
            self.assertEqual(h.admin_sql('SELECT 1'), 'PG')
            pg.assert_called_once_with('SELECT 1')
        self.assertEqual(len(calls), 2)

    def test_head_readback_keeps_complete_entries_and_revision(self):
        h = RedisMemoryHarness.__new__(RedisMemoryHarness)
        body = {'scope': {'tenant_id': 't', 'scope_id': 's'}, 'base_revision': 2, 'entries': [{'memory': {'memory': str(i)}} for i in range(3)]}
        record = {'revision': '3', 'digest': 'sha256:test', 'body': json.dumps(body)}
        h.storage_state = lambda: [{'key': 'runtime_memory:{tenant}:head:x', 'record': record}, {'key': 'runtime_memory:{tenant}:receipt:y', 'record': record}]
        self.assertEqual(h.memory_state(), [{'tenant_id': 't', 'scope_id': 's', 'revision': 3, 'digest': 'sha256:test', 'content': body}])

    def test_parent_cleanup_error_still_removes_only_owned_redis(self):
        h = RedisMemoryHarness.__new__(RedisMemoryHarness)
        h.redis, h.work = 'owned-fixture', Path('/nonexistent-private-fixture')
        recorded = []
        h._record = lambda name, body: recorded.append(body)
        with patch('memory_joint_fixture.MemoryHarness.close', side_effect=RuntimeError('parent cleanup failed')), patch('redis_memory_joint_fixture.subprocess.run') as run:
            run.side_effect = [Mock(returncode=0, stderr=''), Mock(returncode=0, stderr=''), Mock(returncode=1, stderr='Error: No such object: owned-fixture')]
            with self.assertRaisesRegex(RuntimeError, 'parent cleanup failed'):
                h.close()
            self.assertEqual(run.call_args_list[1].args[0], ['docker', 'rm', '-f', 'owned-fixture'])
        self.assertEqual(recorded[0]['result'], 'FAIL')
        self.assertTrue(recorded[0]['removed'])

    def test_daemon_error_is_not_successful_cleanup(self):
        h = RedisMemoryHarness.__new__(RedisMemoryHarness)
        h.redis, h.work = 'owned-fixture', Path('/nonexistent-private-fixture')
        recorded = []
        h._record = lambda name, body: recorded.append(body)
        with patch('memory_joint_fixture.MemoryHarness.close'), patch('redis_memory_joint_fixture.subprocess.run') as run:
            run.return_value.returncode, run.return_value.stderr = 1, 'Cannot connect to Docker daemon'
            with self.assertRaisesRegex(RuntimeError, 'unverified'):
                h.close()
        self.assertEqual(recorded[0]['result'], 'FAIL')
        self.assertFalse(recorded[0]['removed'])


if __name__ == '__main__':
    unittest.main()
