"""Assertion-seam tests; real owner/transport evidence comes from root's gate."""
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
from unittest import mock
import unittest

from final_identity_scenarios import require_mutation, require_rejection, require_follower, run_final_identity
import final_identity_scenarios


def owner_proof():
    return {'intent_id': 'fin_'+'a'*64, 'digest': 'sha256:'+'b'*64,
            'admission_id': '1'*32, 'run_id': '2'*32, 'attempt_id': 'att_'+'3'*32,
            'completion_id': 'cmp_'+'4'*64, 'execution_generation': 1, 'sequence': 1,
            'tenant_id': 'tnt_'+'a'*24, 'manifest_digest': 'sha256:'+'c'*64}


def fault(mode='final_tenant_mismatch'):
    return dict(owner_proof(), method='POST', path='/internal/v1/execution/finals:verify',
                principal_uri='spiffe://agent-platform/channel-gateway',
                upstream_status=200, downstream_status=200, fault=mode, mutation=mode,
                mutation_field='tenant_id' if mode == 'final_tenant_mismatch' else 'manifest_digest',
                mutation_phase='complete_owner_200_response', downstream_action='mutated_owner_200',
                owner_response_bytes=780, downstream_body_bytes_forwarded=780,
                started_ns=1, owner_response_ns=2, finished_ns=3)


class FinalIdentityAssertionTests(unittest.TestCase):
    def test_identity_negative_requires_complete_real_matching_owner_proof(self):
        for mode in ('final_tenant_mismatch', 'final_manifest_mismatch'):
            value = fault(mode)
            self.assertEqual(require_mutation(value, mode, owner_proof()), value)
            for key, changed in [('upstream_status', 409), ('downstream_status', 503),
                                 ('run_id', '9'*32), ('digest', 'sha256:'+'d'*64),
                                 ('tenant_id', 'tnt_'+'b'*24), ('manifest_digest', 'sha256:'+'d'*64),
                                 ('mutation_phase', 'request_before_owner'),
                                 ('downstream_action', 'forwarded'), ('finished_ns', 0),
                                 ('downstream_body_bytes_forwarded', 0),
                                 ('principal_uri', 'spiffe://agent-platform/control-api')]:
                with self.subTest(mode=mode, key=key), self.assertRaises(AssertionError):
                    require_mutation(dict(value, **{key: changed}), mode, owner_proof())
            with self.assertRaises(AssertionError):
                require_mutation(dict(value, mutation_field='wrong'), mode, owner_proof())
        with self.assertRaises(ValueError):
            require_mutation(fault(), 'arbitrary_response_patch', owner_proof())

    def test_rejection_is_one_durable_original_position_and_exact_authorization_reason(self):
        record = {'stream_name': 'REPLY_INTENTS_V1', 'stream_id': '2026-09-06T22:00:00Z',
                  'stream_sequence': 11, 'outcome': 'REJECTED', 'reason': 'UNAUTHORIZED',
                  'raw_digest': 'sha256:'+'b'*64, 'recorded_at': '2026-09-06T22:01:00Z'}
        before = {'created': record['stream_id'], 'state': {'last_seq': 10}}
        self.assertEqual(require_rejection([record], record['raw_digest'], before), record)
        for key, value in [('outcome', 'ACCEPTED'), ('reason', 'INVALID_WIRE'), ('reason', 'CONFLICT'),
                           ('stream_id', 'new-incarnation'), ('stream_sequence', 10),
                           ('raw_digest', 'sha256:'+'a'*64)]:
            with self.subTest(key=key, value=value), self.assertRaises(AssertionError):
                require_rejection([dict(record, **{key: value})], record['raw_digest'], before)
        for values in ([], [record, record]):
            with self.assertRaises(AssertionError):
                require_rejection(values, record['raw_digest'], before)

    def test_successful_unsent_final_remains_accepted_history_for_same_session_follower(self):
        victim = {'tenant_id': 'tenant', 'session_id': 'session', 'session_sequence': 1, 'status': 'SUCCEEDED'}
        follower = dict(victim, session_sequence=2)
        accepted = {'accepted_ref': 'victim-candidate', 'accepted_digest': 'victim-digest', 'settled_sequence': 1}
        candidate = {'parent_ref': 'victim-candidate', 'parent_digest': 'victim-digest'}
        request = {'messages': [{'role': 'user', 'content': 'victim'},
                               {'role': 'assistant', 'content': 'joint answer: victim'},
                               {'role': 'user', 'content': 'follower'}]}
        require_follower(victim, follower, accepted, candidate, request, 'victim', 'follower')
        for key, value in [('session_id', 'other-session'), ('tenant_id', 'other-tenant'),
                           ('session_sequence', 3), ('status', 'FAILED')]:
            with self.subTest(key=key), self.assertRaises(AssertionError):
                require_follower(victim, dict(follower, **{key: value}), accepted, candidate,
                                 request, 'victim', 'follower')
        with self.assertRaises(AssertionError):
            require_follower(victim, follower, accepted, dict(candidate, parent_ref=''), request, 'victim', 'follower')
        for messages in ([request['messages'][-1]], request['messages'] + [request['messages'][-1]],
                         [request['messages'][0], request['messages'][-1]]):
            with self.assertRaises(AssertionError):
                require_follower(victim, follower, accepted, candidate, {'messages': messages}, 'victim', 'follower')

    def test_failed_case_keeps_partial_fail_evidence_and_releases_only_own_fault(self):
        proof = SimpleNamespace(name='worker-proof', release=mock.Mock())
        owner = SimpleNamespace(name='control-resolve', release=mock.Mock())
        with tempfile.TemporaryDirectory() as directory:
            h = SimpleNamespace(artifacts=Path(directory), resolve_proxies=[owner, proof],
                workers={'worker-one': SimpleNamespace(pid=123, poll=lambda: None)},
                model=SimpleNamespace(requests=[]),
                gateway_fixture=SimpleNamespace(lock=threading.Lock(), calls=[], snapshot=lambda: []),
                wait=lambda predicate, *args, **kwargs: predicate())
            def failed(harness, proxy, mode, record):
                self.assertIs(harness, h)
                self.assertIs(proxy, proof)
                self.assertEqual(mode, 'final_tenant_mismatch')
                record['run_id'] = 'durably-admitted-victim'
                raise AssertionError('fixture test failure')
            with mock.patch('final_identity_scenarios.consumer_info', return_value={'num_pending': 0, 'num_ack_pending': 0}), \
                 mock.patch('final_identity_scenarios._scenario', side_effect=failed), \
                 self.assertRaises(AssertionError):
                run_final_identity(h)
            saved = json.loads((Path(directory)/'worker-final-identity.json').read_text())
            self.assertEqual(saved['result'], 'FAIL')
            self.assertEqual(len(saved['cases']), 1)
            self.assertEqual(saved['cases'][0]['run_id'], 'durably-admitted-victim')
            self.assertEqual(saved['cases'][0]['result'], 'FAIL')
            proof.release.assert_called_once_with()
            owner.release.assert_not_called()

    def test_victim_waits_for_durable_worker_intake_after_gateway_admission(self):
        class ObservedIntake(Exception):
            pass
        events = []
        def send(text, conversation_id):
            events.append('gateway-admission')
            return 'victim-run'
        def sql(query):
            self.assertIn('FROM worker.execution_runs', query)
            self.assertIn("run_id='victim-run'", query)
            exists = 'worker-row-absent' in events
            events.append('worker-row-present' if exists else 'worker-row-absent')
            return [[json.dumps([{'run_id': 'victim-run'}] if exists else [])]]
        def wait(predicate, description, timeout):
            self.assertIn('durable intake', description)
            self.assertFalse(predicate())
            self.assertTrue(predicate())
        def success(h, run_id):
            self.assertEqual(run_id, 'victim-run')
            self.assertEqual(events, ['gateway-admission', 'worker-row-absent', 'worker-row-present'])
            raise ObservedIntake()
        h = SimpleNamespace(send_text=send, sql=sql, wait=wait)
        proxy = SimpleNamespace(events=lambda: [], arm=mock.Mock())
        record = {}
        with mock.patch.object(final_identity_scenarios, 'stream_info', return_value={'created': 'incarnation', 'state': {'last_seq': 7}}), \
             mock.patch.object(final_identity_scenarios, 'rows', return_value=[]), \
             mock.patch.object(final_identity_scenarios, '_send_calls', return_value=0), \
             mock.patch.object(final_identity_scenarios, 'wait_success', side_effect=success), \
             self.assertRaises(ObservedIntake):
            final_identity_scenarios._scenario(h, proxy, 'final_tenant_mismatch', record)
        self.assertEqual(record['run_id'], 'victim-run')
        proxy.arm.assert_called_once_with('final_tenant_mismatch', path=final_identity_scenarios.FINAL_PATH)


if __name__ == '__main__':
    unittest.main()
