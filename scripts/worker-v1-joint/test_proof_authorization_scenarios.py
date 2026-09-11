"""Pure assertion tests; actual owner evidence is produced only by root's gate."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest import mock
import unittest

from proof_authorization_scenarios import assert_denied_facts, assert_empty_head_follower, correlate_exclusive_denial, run_proof_authorization


def event(**values):
    result={'grant_group':3,'upstream_status':403,'downstream_status':403,
            'downstream_action':'forwarded','started_ns':1,'owner_response_ns':3,'finished_ns':4}
    result.update(values)
    return result


class ProofAuthorizationAssertionsTests(unittest.TestCase):
    def test_denial_without_body_ids_needs_one_new_run_and_one_correlated_request(self):
        resolved=[event()]
        proof=[event(started_ns=2,owner_response_ns=2,finished_ns=3, fault='manifest_digest_mismatch',
                     mutation='manifest_digest_mismatch',mutation_phase='request_before_owner')]
        value=correlate_exclusive_denial(resolved,proof,{'run-1'},'run-1')
        self.assertEqual(value['grant_group'],3)
        # Response delivery and finally bookkeeping run on different threads.
        late_bookkeeping=[dict(proof[0],finished_ns=8)]
        self.assertEqual(correlate_exclusive_denial(resolved,late_bookkeeping,{'run-1'},'run-1')['grant_group'],3)
        for a,b,ids in [([],proof,{'run-1'}),(resolved,[],{'run-1'}),
                        (resolved*2,proof,{'run-1'}),(resolved,proof,set()),
                        (resolved,proof,{'run-1','extra'}),
                        (resolved,[dict(proof[0],grant_group=4)],{'run-1'}),
                        (resolved,[dict(proof[0],upstream_status=400)],{'run-1'}),
                        ([dict(resolved[0],upstream_status=503)],proof,{'run-1'}),
                        (resolved,[dict(proof[0],owner_response_ns=5)],{'run-1'}),
                        (resolved,[dict(proof[0],finished_ns=1)],{'run-1'})]:
            with self.assertRaises(AssertionError):correlate_exclusive_denial(a,b,ids,'run-1')

    def test_follower_is_same_empty_head_session_with_exact_history(self):
        victim={'tenant_id':'t','session_id':'s','session_sequence':1}
        follower=dict(victim,session_sequence=2)
        head={'settled_sequence':1,'accepted_ref':'','accepted_digest':''}
        candidate={'parent_ref':'','parent_digest':''}
        request={'messages':[{'role':'user','content':'follower'}]}
        assert_empty_head_follower(victim,follower,head,candidate,request,'follower')
        for bad in (dict(follower,session_id='elsewhere'),dict(follower,session_sequence=3)):
            with self.assertRaises(AssertionError):assert_empty_head_follower(victim,bad,head,candidate,request,'follower')
        with self.assertRaises(AssertionError):
            assert_empty_head_follower(victim,follower,dict(head,accepted_ref='orphan'),candidate,request,'follower')
        with self.assertRaises(AssertionError):
            assert_empty_head_follower(victim,follower,head,candidate,{'messages':[{'role':'user','content':'failed'},*request['messages']]},'follower')

    def test_restore_failure_is_not_persisted_as_success(self):
        worker=SimpleNamespace(pid=123,poll=lambda:None)
        with tempfile.TemporaryDirectory() as directory:
            h=SimpleNamespace(artifacts=Path(directory),resolve_proxies=[mock.Mock(),mock.Mock()],
                stop_fault_workers=mock.Mock(),start_worker=mock.Mock(side_effect=[worker,RuntimeError('restore failed')]))
            with mock.patch('proof_authorization_scenarios._proof_loss',return_value={}), \
                 mock.patch('proof_authorization_scenarios._proof_denial',return_value={}), \
                 mock.patch('builtins.print'):
                with self.assertRaises(RuntimeError):run_proof_authorization(h)
            evidence=json.loads((Path(directory)/'worker-proof-authorization.json').read_text())
            self.assertEqual(evidence['result'],'FAIL')
            self.assertNotIn('standard_worker_restored_pid',evidence)

    def test_failed_attempt_must_be_stable_denial_not_system_termination_or_retry(self):
        state={'status':'FAILED'}
        facts=[{'attempt_id':'attempt-1','status':'FAILED','reason':'CREDENTIAL_DENIED','agent_started_at':None}]
        completion=[{'kind':'ATTEMPT','status':'FAILED','reason':'CREDENTIAL_DENIED','attempt_id':'attempt-1',
                     'candidate_ref':'','candidate_digest':'','reply_disposition':'FINAL','final_intent_id':'final-1'}]
        final=[{'intent_id':'final-1'}]
        self.assertEqual(assert_denied_facts(state,facts,completion,[],final),facts[0])
        for key,value in [('reason','DEPENDENCY_UNAVAILABLE'),('agent_started_at','started')]:
            bad=copy.deepcopy(facts);bad[0][key]=value
            with self.assertRaises(AssertionError):assert_denied_facts(state,bad,completion,[],final)
        for key,value in [('kind','SYSTEM_TERMINATION'),('candidate_ref','orphan'),('reply_disposition','NONE')]:
            bad=copy.deepcopy(completion);bad[0][key]=value
            with self.assertRaises(AssertionError):assert_denied_facts(state,facts,bad,[],final)
        with self.assertRaises(AssertionError):assert_denied_facts(state,facts*2,completion,[],final)
        with self.assertRaises(AssertionError):assert_denied_facts(state,facts,completion,[{}],final)
        with self.assertRaises(AssertionError):assert_denied_facts(state,facts,completion,[],final*2)


if __name__=='__main__':unittest.main()
