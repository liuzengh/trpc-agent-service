"""Pure assertion sensitivity; the two process cases run only in the joint gate."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from reply_deadline_scenarios import (assert_reply_only_expired, require_expired_receipt,
                                      require_success_without_reply, require_zero_delivery,
                                      require_session_commit, require_ended_attempt_proof, restore_ready, run_reply_deadline)
from reply_scenarios import FINAL_PATH


def instant(second):
    return '2026-09-07T00:00:' + str(second).zfill(2) + '+00:00'


class ReplyDeadlineTests(unittest.TestCase):
    def test_reply_expiry_requires_one_same_db_instant_with_three_other_live_windows(self):
        good = {'status': 'RUNNING', 'attempt_status': 'EXECUTING', 'agent_started_at': instant(1),
                'db_now': instant(5), 'reply_deadline': instant(4), 'run_deadline': instant(40),
                'execution_deadline': instant(30), 'lease_until': instant(7),
                'reply_expired': True, 'run_expired': False, 'execution_expired': False, 'lease_live': True}
        self.assertEqual(assert_reply_only_expired(good), good)
        for change in ({'status': 'FAILED'}, {'attempt_status': 'ABORTED'}, {'agent_started_at': None},
                       {'db_now': instant(3)}, {'reply_expired': False}, {'run_expired': True},
                       {'execution_expired': True}, {'lease_live': False}, {'lease_until': instant(5)},
                       {'execution_deadline': instant(5)}, {'run_deadline': instant(5)}):
            with self.subTest(change=change), self.assertRaises(AssertionError):
                assert_reply_only_expired(dict(good, **change))

    def success(self):
        return dict(record={'status': 'SUCCEEDED', 'attempts': 1, 'session_sequence': 1,
                        'reply_deadline': instant(4), 'execution_deadline': instant(30), 'run_deadline': instant(40)},
            completion=[{'status': 'SUCCEEDED', 'kind': 'ATTEMPT', 'reply_disposition': 'NONE',
                         'reason': 'DEADLINE_EXPIRED', 'final_intent_id': '', 'attempt_id': 'attempt',
                         'candidate_ref': 'ref', 'candidate_digest': 'digest', 'completed_at': instant(5)}],
            aa=[{'status': 'SUCCEEDED', 'agent_started_at': instant(1), 'reason': '', 'attempt_id': 'attempt'}],
            cc=[{'candidate_ref': 'ref', 'content_digest': 'digest', 'attempt_id': 'attempt'}], final=[],
            accepted={'accepted_ref': 'ref', 'accepted_digest': 'digest', 'settled_sequence': 1})

    def test_success_keeps_candidate_but_has_no_final_after_independent_reply_expiry(self):
        good = self.success()
        self.assertEqual(require_success_without_reply(**good), good['completion'][0])
        mutations = [lambda v: v['record'].update(status='FAILED'),
                     lambda v: v['record'].update(attempts=2),
                     lambda v: v['completion'][0].update(kind='SYSTEM_TERMINATION'),
                     lambda v: v['completion'][0].update(reply_disposition='FINAL'),
                     lambda v: v['completion'][0].update(reason=''),
                     lambda v: v['completion'][0].update(final_intent_id='intent'),
                     lambda v: v['completion'][0].update(completed_at=instant(3)),
                     lambda v: v['completion'][0].update(completed_at=instant(30)),
                     lambda v: v['completion'][0].update(candidate_digest='wrong'),
                     lambda v: v['aa'][0].update(reason='DEADLINE_EXPIRED'),
                     lambda v: v['cc'][0].update(attempt_id='other'),
                     lambda v: v['accepted'].update(settled_sequence=0),
                     lambda v: v['final'].append({'intent_id': 'unexpected'})]
        for i, mutate in enumerate(mutations):
            value = copy.deepcopy(good)
            mutate(value)
            with self.subTest(index=i), self.assertRaises(AssertionError):
                require_success_without_reply(**value)

    def test_zero_delivery_means_no_intents_parts_attempts_or_external_message(self):
        good = {'intents': [], 'parts': [], 'attempts': [], 'external_messages': []}
        self.assertEqual(require_zero_delivery(good), good)
        for field in good:
            with self.subTest(field=field), self.assertRaises(AssertionError):
                require_zero_delivery(dict(good, **{field: [{}]}))

    def test_expired_receipt_requires_original_digest_rejection_and_db_time(self):
        good = [{'outcome': 'REJECTED', 'reason': 'EXPIRED', 'raw_digest': 'original', 'recorded_at': instant(21),
                 'stream_name': 'REPLY_INTENTS_V1', 'stream_id': instant(1), 'stream_sequence': 7}]
        self.assertEqual(require_expired_receipt(good, 'original', instant(20), instant(1), 7), good[0])
        for change in ({'outcome': 'ACCEPTED'}, {'reason': 'UNAUTHORIZED'}, {'raw_digest': 'changed'},
                       {'recorded_at': instant(19)}, {'stream_id': instant(2)},
                       {'stream_sequence': 8}, {'stream_name': 'OTHER'}):
            with self.subTest(change=change), self.assertRaises(AssertionError):
                require_expired_receipt([dict(good[0], **change)], 'original', instant(20), instant(1), 7)
        for bad in ([], good + good):
            with self.assertRaises(AssertionError):
                require_expired_receipt(bad, 'original', instant(20), instant(1), 7)


    def test_success_requires_a_real_formal_session_commit_bound_to_run_and_head(self):
        accepted = {'accepted_ref': 'ref', 'accepted_digest': 'digest'}
        commit = {'run_id': 'run', 'session_id': 'session', 'candidate_ref': 'ref', 'candidate_digest': 'digest'}
        facts = {'run': {'run_id': 'run', 'session_id': 'session'}, 'session_commits': [commit],
                 'completion': [{'candidate_ref': 'ref', 'candidate_digest': 'digest'}]}
        self.assertEqual(require_session_commit(facts, accepted), commit)
        for bad in ([], [commit, commit], [dict(commit, run_id='other')],
                    [dict(commit, session_id='other')], [dict(commit, candidate_digest='other')]):
            with self.assertRaises(AssertionError):
                require_session_commit(dict(facts, session_commits=bad), accepted)

    def test_late_scenario_or_restoration_error_marks_case_and_overall_failed(self):
        def failed(h, record):
            record.update(result='PASS', case='injected-before-cleanup')
            raise RuntimeError('restoration failed')
        with tempfile.TemporaryDirectory() as directory:
            h = SimpleNamespace(artifacts=Path(directory))
            with patch('reply_deadline_scenarios.complete_after_reply_expiry', side_effect=failed):
                with self.assertRaisesRegex(RuntimeError, 'restoration failed'):
                    run_reply_deadline(h)
            result = json.loads((h.artifacts / 'worker-reply-deadline.json').read_text())
            self.assertEqual(result['result'], 'FAIL')
            self.assertEqual(result['cases'][0]['result'], 'FAIL')


    def test_ended_attempt_needs_direct_unchanged_real_proof_after_gateway_restart(self):
        expected = {'run_id': 'run', 'attempt_id': 'attempt', 'intent_id': 'intent',
                    'completion_id': 'completion', 'digest': 'digest', 'tenant_id': 'tenant',
                    'manifest_digest': 'manifest', 'sequence': 1, 'execution_generation': 1}
        attempt = {'status': 'SUCCEEDED', 'attempt_id': 'attempt', 'ended_at': instant(5)}
        event = dict(expected, path=FINAL_PATH, upstream_status=200, downstream_status=200,
                     downstream_action='forwarded', owner_response_bytes=400, downstream_body_bytes_forwarded=400,
                     started_ns=30, owner_response_ns=35, finished_ns=40)
        self.assertEqual(require_ended_attempt_proof([event], expected, attempt, 25), [event])
        changes = [{'upstream_status': 403}, {'downstream_status': 503}, {'downstream_action': 'mutated_owner_200'},
                   {'fault': 'injected'}, {'owner_response_bytes': 0}, {'downstream_body_bytes_forwarded': 399},
                   {'finished_ns': 34}, {'started_ns': 24}]
        changes += [{key: 'other'} for key in expected]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(AssertionError):
                require_ended_attempt_proof([dict(event, **change)], expected, attempt, 25)
        for bad in (dict(attempt, status='EXECUTING'), dict(attempt, ended_at=None)):
            with self.assertRaises(AssertionError):
                require_ended_attempt_proof([event], expected, bad, 25)
        with self.assertRaises(AssertionError):
            require_ended_attempt_proof([], expected, attempt, 25)


    def test_restoration_requires_literal_ready_true_and_preserves_observation(self):
        ready = {'worker_id': 'worker-one', 'pid': 123, 'ready': True}
        record = {}
        with patch('reply_deadline_scenarios.restore_standard', return_value=ready):
            self.assertEqual(restore_ready(object(), record), ready)
        self.assertEqual(record['restoration'], ready)
        for value in (False, None, 1, 'true'):
            observed = dict(ready, ready=value)
            record = {}
            with self.subTest(value=value), patch('reply_deadline_scenarios.restore_standard', return_value=observed):
                with self.assertRaisesRegex(AssertionError, 'standard Worker exited'):
                    restore_ready(object(), record)
                self.assertEqual(record['restoration'], observed)

    def test_not_ready_follower_or_final_restore_marks_case_and_overall_failed(self):
        observed = {'worker_id': 'worker-one', 'pid': 123, 'ready': False}
        for field in ('follower_configuration', 'restoration'):
            def failed(h, record):
                record.update(result='PASS', case='injected-before-restore')
                restore_ready(h, record, field)
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                h = SimpleNamespace(artifacts=Path(directory))
                with patch('reply_deadline_scenarios.complete_after_reply_expiry', side_effect=failed), \
                     patch('reply_deadline_scenarios.restore_standard', return_value=observed):
                    with self.assertRaisesRegex(AssertionError, 'standard Worker exited'):
                        run_reply_deadline(h)
                result = json.loads((h.artifacts / 'worker-reply-deadline.json').read_text())
                self.assertEqual(result['result'], 'FAIL')
                self.assertEqual(result['cases'][0]['result'], 'FAIL')
                self.assertEqual(result['cases'][0][field], observed)


if __name__ == '__main__':
    unittest.main()
