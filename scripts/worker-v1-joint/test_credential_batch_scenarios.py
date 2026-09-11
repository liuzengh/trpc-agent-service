"""Assertion sensitivity only; real authorization evidence needs the joint gate."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from credential_batch_scenarios import (FAILURE_FINAL, MODES, require_denied,
                                        require_early_operations, require_mutation, run_credential_batch)
from resolve_fault_proxy import RESOLVE_PATH


class CredentialBatchAssertionTests(unittest.TestCase):
    def receipt(self, mode):
        return {'path': RESOLVE_PATH, 'fault': mode, 'mutation': mode,
                'upstream_status': 200, 'downstream_status': 200,
                'downstream_action': 'mutated_owner_200', 'owner_response_bytes': 700,
                'mutation_phase': 'complete_owner_200_response',
                'downstream_body_bytes_forwarded': 500, 'run_id': 'run', 'attempt_id': 'attempt',
                'credential_count_before': 2, 'credential_count_after': {
                    'missing_use': 1, 'extra_use': 3, 'duplicate_use': 2, 'identity_mismatch': 2}[mode],
                'owner_response_ns': 10, 'finished_ns': 20}

    def test_receipt_requires_real_complete_owner_response_and_target_identity(self):
        for mode in MODES:
            event = self.receipt(mode)
            self.assertEqual(require_mutation(event, mode, 'run', 'attempt'), event)
            for change in ({'upstream_status': 403}, {'downstream_status': 503},
                           {'downstream_action': 'forwarded'}, {'owner_response_bytes': 0},
                           {'downstream_body_bytes_forwarded': 0}, {'run_id': 'other'},
                           {'attempt_id': 'other'}, {'credential_count_before': 1},
                           {'credential_count_after': 9}, {'finished_ns': 9},
                           {'mutation': 'other'}, {'fault': 'other'}, {'mutation_phase': 'request'}):
                with self.subTest(mode=mode, change=change), self.assertRaises(AssertionError):
                    require_mutation(dict(event, **change), mode, 'run', 'attempt')

    def denial(self):
        return dict(state={'run_id': 'run', 'status': 'FAILED', 'attempts': 1, 'session_sequence': 2},
            attempt_facts=[{'attempt_id': 'attempt', 'status': 'FAILED', 'reason': 'CREDENTIAL_DENIED',
                            'agent_started_at': None, 'ended_at': 'ended'}],
            completion_facts=[{'attempt_id': 'attempt', 'kind': 'ATTEMPT', 'status': 'FAILED',
                'reason': 'CREDENTIAL_DENIED', 'candidate_ref': None, 'candidate_digest': None,
                'reply_disposition': 'FINAL', 'final_intent_id': 'intent'}],
            candidate_facts=[], final_facts=[{'intent_id': 'intent'}],
            before={'accepted_ref': 'ref', 'accepted_digest': 'digest', 'settled_sequence': 1},
            after={'accepted_ref': 'ref', 'accepted_digest': 'digest', 'settled_sequence': 2},
            delivery={'run_id': 'run', 'intent_id': 'intent', 'delivery_state': 'ACCEPTED', 'final_text': FAILURE_FINAL},
            sdk_calls=[], auth_events=[])

    def test_denial_requires_one_failed_attempt_fixed_final_and_unchanged_head(self):
        value = self.denial()
        self.assertEqual(require_denied(**value)['candidate_count'], 0)
        mutations = [
            lambda v: v['state'].update(status='SUCCEEDED'),
            lambda v: v['state'].update(attempts=2),
            lambda v: v['attempt_facts'][0].update(reason='DEPENDENCY_UNAVAILABLE'),
            lambda v: v['attempt_facts'][0].update(agent_started_at='started'),
            lambda v: v['attempt_facts'][0].update(ended_at=None),
            lambda v: v['completion_facts'][0].update(kind='SYSTEM_TERMINATION'),
            lambda v: v['completion_facts'][0].update(candidate_ref='unexpected'),
            lambda v: v['completion_facts'][0].update(reply_disposition='NONE'),
            lambda v: v['candidate_facts'].append({'candidate_ref': 'unexpected'}),
            lambda v: v['sdk_calls'].append({}),
            lambda v: v['auth_events'].append({'accepted': False}),
            lambda v: v['after'].update(accepted_digest='polluted'),
            lambda v: v['after'].update(settled_sequence=1),
            lambda v: v['delivery'].update(final_text='fabricated model result'),
            lambda v: v['delivery'].update(delivery_state='PENDING'),
            lambda v: v['delivery'].update(intent_id='unrelated'),
            lambda v: v['final_facts'].append({'intent_id': 'duplicate'}),
        ]
        for i, mutate in enumerate(mutations):
            value = self.denial()
            mutate(value)
            with self.subTest(index=i), self.assertRaises(AssertionError):
                require_denied(**value)

    def test_typed_records_exclude_session_initialization_and_model_execution(self):
        records = [{'operation': name, 'result': 'credential_denied', 'attempt_id': 'attempt'}
                   for name in ('credential_resolve', 'prepare')]
        frozen = copy.deepcopy(records)
        self.assertEqual(require_early_operations(records, 'attempt'), records)
        for operation in ('session_open', 'session_load', 'execute', 'session_stage', 'usage'):
            with self.subTest(operation=operation), self.assertRaises(AssertionError):
                require_early_operations(records + [{'operation': operation}], 'attempt')
        for bad in (records[:1], records + records[:1],
                    [dict(records[0], result='dependency'), records[1]],
                    [dict(records[0], attempt_id='other'), records[1]]):
            with self.assertRaises(AssertionError):
                require_early_operations(bad, 'attempt')
        self.assertEqual(records, frozen)

    def test_failed_scenario_restores_standard_worker_and_writes_failed_evidence(self):
        events = []
        class Relay:
            def release(self):
                events.append('release')
        class Harness:
            resolve_proxies = [Relay(), Relay()]
            def stop_fault_workers(self):
                events.append('stop')
            def start_worker(self, worker_id, overrides=None):
                events.append(('start', worker_id, overrides))
                return SimpleNamespace(pid=123, poll=lambda: None)
        with tempfile.TemporaryDirectory() as directory:
            h = Harness()
            h.artifacts = Path(directory)
            with patch('credential_batch_scenarios._scenario', side_effect=RuntimeError('injected assertion failure')):
                with self.assertRaisesRegex(RuntimeError, 'injected assertion failure'):
                    run_credential_batch(h)
            result = json.loads((h.artifacts / 'worker-credential-batch.json').read_text())
            self.assertEqual(result['result'], 'FAIL')
            self.assertEqual(result['cases'], [])
            self.assertEqual(result['standard_worker_restored_pid'], 123)
            self.assertEqual(events, ['stop', ('start', 'worker-one', {'policy': {'max_attempts': 3}}),
                                     'release', 'release', 'stop', ('start', 'worker-one', None)])


    def test_worker_restoration_failure_does_not_leave_artifact_marked_pass(self):
        class Harness:
            resolve_proxies = [SimpleNamespace(release=lambda: None), SimpleNamespace(release=lambda: None)]
            def stop_fault_workers(self):
                pass
            def start_worker(self, worker_id, overrides=None):
                if overrides is None:
                    raise RuntimeError('restoration failed')
                return SimpleNamespace(pid=123, poll=lambda: None)
        with tempfile.TemporaryDirectory() as directory:
            h = Harness()
            h.artifacts = Path(directory)
            with patch('credential_batch_scenarios._scenario', return_value={'result': 'PASS'}), patch('builtins.print'):
                with self.assertRaisesRegex(RuntimeError, 'restoration failed'):
                    run_credential_batch(h)
            result = json.loads((h.artifacts / 'worker-credential-batch.json').read_text())
            self.assertEqual(len(result['cases']), 4)
            self.assertEqual(result['result'], 'FAIL')
            self.assertNotIn('standard_worker_restored_pid', result)


if __name__ == '__main__':
    unittest.main()
