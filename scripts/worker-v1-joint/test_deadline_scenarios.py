"""Deadline proof guards; actual scheduling is exercised by the joint runner."""
import copy
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from deadline_scenarios import (assert_fixed_window, assert_system_terminal, assert_queued_expired,
                                assert_initial_deadline, retry_fixed_deadline, retry_wait_deadline, queued_deadline)


class DeadlineEvidenceTest(unittest.TestCase):
    def record(self):
        return {'run_id':'run','session_id':'session','session_sequence':2,'accepted_at':'2026-09-07T00:00:00+00:00',
                'received_at':'2026-09-07T00:00:00+00:00','run_deadline':'2026-09-07T00:00:12+00:00',
                'execution_deadline':'2026-09-07T00:00:12+00:00','reply_deadline':'2026-09-07T00:03:00+00:00',
                'policy_json':{'MaxRunAge':12000000000,'MaxAttempts':4},'attempts':2,'status':'FAILED',
                'db_now':'2026-09-07T00:00:12.100+00:00','run_expired':True,'execution_expired':True}

    def terminal(self):
        return {'kind':'SYSTEM_TERMINATION','status':'FAILED','reply_disposition':'NONE','reason':'DEADLINE_EXPIRED',
                'candidate_ref':'','candidate_digest':'','final_intent_id':'','completed_at':'2026-09-07T00:00:12.050+00:00'}

    def test_retry_status_changes_do_not_extend_any_fixed_window(self):
        before=self.record();after=copy.deepcopy(before);after.update(status='RUNNING',attempts=3)
        assert_fixed_window(before,after)
        for field in ('accepted_at','received_at','run_deadline','execution_deadline','reply_deadline','policy_json'):
            bad=copy.deepcopy(after);bad[field]='changed'
            with self.subTest(field=field),self.assertRaises(AssertionError):assert_fixed_window(before,bad)

    def test_system_terminal_requires_failed_none_no_candidate_or_final_and_unchanged_head(self):
        run=self.record();completion=self.terminal();parent={'accepted_ref':'parent','accepted_digest':'digest','settled_sequence':1}
        head=dict(parent,settled_sequence=2)
        assert_system_terminal(run,[completion],[],[],parent,head)
        for field,value in [('kind','ATTEMPT'),('status','SUCCEEDED'),('reply_disposition','FINAL'),('candidate_ref','candidate'),('final_intent_id','final'),('reason','ATTEMPTS_EXHAUSTED'),('completed_at','2026-09-07T00:00:11+00:00')]:
            bad=dict(completion,**{field:value})
            with self.subTest(field=field),self.assertRaises(AssertionError):assert_system_terminal(run,[bad],[],[],parent,head)
        for comps,candidates,finals,after in [([completion,completion],[],[],head),([completion],[{}],[],head),([completion],[],[{}],head),([completion],[],[],dict(head,accepted_ref='polluted'))]:
            with self.assertRaises(AssertionError):assert_system_terminal(run,comps,candidates,finals,parent,after)

    def test_execution_window_uses_first_claim_and_not_retry_start(self):
        run=self.record();run['policy_json']['MaxRunAge']=150000000000
        run['run_deadline']='2026-09-07T00:02:30Z';run['execution_deadline']='2026-09-07T00:02:01Z'
        first={'created_at':'2026-09-07T00:00:01Z','lease_until':'2026-09-07T00:00:04Z'}
        assert_initial_deadline(run,first,{'max_run_seconds':120})
        with self.assertRaises(AssertionError):
            assert_initial_deadline(run,dict(first,created_at='2026-09-07T00:00:04Z'),{'max_run_seconds':120})
        with self.assertRaises(AssertionError):
            assert_initial_deadline(run,dict(first,lease_until='2026-09-07T00:02:02Z'),{'max_run_seconds':120})

    def test_every_scenario_restores_standard_worker_even_after_fixture_body_error(self):
        for scenario in (retry_fixed_deadline,retry_wait_deadline,queued_deadline):
            h=SimpleNamespace(model=SimpleNamespace(release=Mock(),hold_timeout_seconds=90))
            record={};processes={'worker-one':SimpleNamespace(pid=1),'worker-two':SimpleNamespace(pid=2)}
            with self.subTest(scenario=scenario.__name__), patch('deadline_scenarios.pair',return_value=processes), patch('deadline_scenarios.manifest_window',return_value={'max_run_seconds':120}), patch('deadline_scenarios.seed',side_effect=RuntimeError('body failure')), patch('deadline_scenarios.restore_standard',return_value={'ready':True}) as restore:
                with self.assertRaisesRegex(RuntimeError,'body failure'):scenario(h,record)
                restore.assert_called_once_with(h)
                self.assertTrue(record['restoration']['ready'])
                self.assertEqual(h.model.hold_timeout_seconds,90)

    def test_queued_expiration_requires_database_clock_and_zero_attempt(self):
        queue=self.record();queue.update(status='QUEUED',attempts=0,execution_deadline=None,execution_expired=False)
        assert_queued_expired(queue,[])
        for field,value in [('run_expired',False),('attempts',1),('execution_deadline','2026-09-07T00:00:12Z'),('status','RUNNING'),('db_now','2026-09-07T00:00:11Z')]:
            with self.subTest(field=field),self.assertRaises(AssertionError):assert_queued_expired(dict(queue,**{field:value}),[])
        with self.assertRaises(AssertionError):assert_queued_expired(queue,[{}])


if __name__=='__main__':unittest.main()
