"""WV-13 real online proof transport failure versus owner authorization denial.

Uses the pre-installed two verified-mTLS relays and a live Worker OS process.
The proof relay never manufactures an HTTP status: loss consumes a true owner
200, while mismatch changes one valid request digest and lets Worker decide.
Only root runs the actual process gate; these helpers do not provision fixtures.
"""
from __future__ import annotations

import json
from pathlib import Path
import time
import uuid

from commit_scenarios import operation_records
from faults import attempts, candidates, completions, head, model_calls, outboxes, rows, run, submit, wait_success
from resolve_fault_proxy import ATTEMPT_PATH, RESOLVE_PATH
from resolve_scenarios import require_recovery
from scope_scenarios import assert_transcript


def _requests_since(proxy, ordinal, path):
    return [e for e in proxy.events() if e['request_ordinal'] > ordinal
            and e.get('path') == path and e.get('method') == 'POST']


def _last_ordinal(proxy):
    events = proxy.events()
    return events[-1]['request_ordinal'] if events else 0


def correlate_exclusive_denial(resolve_events, proof_events, new_run_ids, run_id):
    """403 bodies carry no IDs: prove an explicit exclusive one-request interval.

    We do not call events_for_run on empty denial metadata, nor infer identity
    from a token/fingerprint. The caller owns the only input in this interval;
    durable new Run set, exact POST counts and shared ephemeral group must agree.
    """
    assert set(new_run_ids) == {run_id}, 'denial interval admitted an unrelated Run'
    assert len(resolve_events) == len(proof_events) == 1, 'denial interval was not one owner request'
    resolved, proof = resolve_events[0], proof_events[0]
    assert resolved['grant_group'] == proof['grant_group']
    assert resolved.get('upstream_status') == resolved.get('downstream_status') == 403
    assert proof.get('upstream_status') == proof.get('downstream_status') == 403
    assert proof.get('fault') == proof.get('mutation') == 'manifest_digest_mismatch'
    assert proof.get('mutation_phase') == 'request_before_owner'
    assert proof.get('downstream_action') == resolved.get('downstream_action') == 'forwarded'
    assert 'finished_ns' in proof and 'finished_ns' in resolved
    # The Control verifier may return on proof403 headers before the proof
    # relay's finally bookkeeping runs. Compare causal owner observations, not
    # scheduling-dependent completion timestamps from different Python threads.
    assert resolved['started_ns'] <= proof['started_ns'] <= proof['owner_response_ns'] <= resolved['owner_response_ns']
    assert proof['finished_ns'] >= proof['owner_response_ns']
    assert resolved['finished_ns'] >= resolved['owner_response_ns']
    return {'method': 'exclusive_one_new_Run_one_Resolve_one_proof_interval',
            'new_run_ids': list(new_run_ids), 'resolve_request_count': 1,
            'proof_request_count': 1, 'grant_group': proof['grant_group']}


def assert_denied_facts(state, facts, completion, candidate_rows, final_rows):
    assert state['status'] == 'FAILED' and len(facts) == 1
    attempt = facts[0]
    assert attempt['status'] == 'FAILED' and attempt['reason'] == 'CREDENTIAL_DENIED'
    assert attempt['agent_started_at'] is None
    assert len(completion) == 1 and completion[0]['kind'] == 'ATTEMPT'
    assert completion[0]['status'] == 'FAILED' and completion[0]['reason'] == 'CREDENTIAL_DENIED'
    assert completion[0]['attempt_id'] == attempt['attempt_id']
    assert not completion[0]['candidate_ref'] and not completion[0]['candidate_digest']
    assert not candidate_rows and len(final_rows) == 1
    assert completion[0]['reply_disposition'] == 'FINAL'
    assert completion[0]['final_intent_id'] == final_rows[0]['intent_id']
    return attempt


def assert_empty_head_follower(victim, follower, denied_head, candidate, request, text):
    assert victim['session_sequence'] == 1 and follower['session_sequence'] == 2
    assert victim['tenant_id'] == follower['tenant_id'] and victim['session_id'] == follower['session_id']
    assert denied_head['settled_sequence'] == 1
    assert denied_head['accepted_ref'] == denied_head['accepted_digest'] == ''
    assert candidate['parent_ref'] == candidate['parent_digest'] == ''
    assert_transcript(request, [text])


def _proof_loss(h, owner, proof, worker):
    text = 'proof-loss-'+uuid.uuid4().hex[:12]
    conversation = str(4100000000+int(uuid.uuid4().hex[:7],16))
    proof.arm('drop_after_owner_200', path=ATTEMPT_PATH)
    run_id = submit(h, text, conversation)
    fault = h.wait(lambda: (lambda event: event if event and event.get('finished_ns') else None)
                   (proof.fault()), 'real online proof 200 received then response dropped', timeout=15)
    assert fault['run_id'] == run_id and fault['upstream_status'] == 200
    assert fault['downstream_body_bytes_forwarded'] == 0 and 'downstream_status' not in fault
    assert fault['downstream_action'] == 'owner_200_suppressed'
    result = require_recovery(h, run_id, text, owner, proof)
    old, new = result['attempts']
    assert old['attempt_id'] == fault['attempt_id'] and old['status'] == 'FAILED'
    assert old['reason'] == 'DEPENDENCY_UNAVAILABLE'
    old_resolve = [event for event in result['resolve_events'] if event['grant_group'] == fault['grant_group']]
    assert len(old_resolve) == 1 and old_resolve[0]['upstream_status'] == old_resolve[0]['downstream_status'] == 503
    old_proof = [event for event in result['proof_events'] if event['grant_group'] == fault['grant_group']]
    assert old_proof == [fault], 'Control transparently retried the original proof'
    assert worker.poll() is None and old['worker_id'] == new['worker_id'] == 'worker-one'
    result.update(case='actual_proof_200_response_loss_maps_to_control_503', fault=fault,
        actual_control_status=503, original_attempt_reason='DEPENDENCY_UNAVAILABLE',
        worker_pid=worker.pid, worker_alive_after_case=True, process_killed=False)
    return result


def _proof_denial(h, owner, proof, worker):
    text = 'proof-digest-denial-'+uuid.uuid4().hex[:12]
    conversation = str(4200000000+int(uuid.uuid4().hex[:7],16))
    # Root executes scenarios sequentially. Assert that exclusivity from all new
    # durable Runs as well, instead of trusting temporal proximity alone.
    before_runs = {x['run_id'] for x in rows(h, 'SELECT run_id FROM worker.execution_runs')}
    owner_before, proof_before = _last_ordinal(owner), _last_ordinal(proof)
    started_ns = time.monotonic_ns()
    proof.arm('manifest_digest_mismatch', path=ATTEMPT_PATH)
    run_id = submit(h, text, conversation)
    h.wait(lambda: run(h,run_id)['status'] == 'FAILED', 'true proof 403 gives permanent credential denial',timeout=30)
    h.wait(lambda: (proof.fault() or {}).get('finished_ns'), 'proof denial relay finished',timeout=5)
    h.wait(lambda: all(e.get('finished_ns') for e in _requests_since(owner, owner_before, RESOLVE_PATH)),
           'Control 403 response completed',timeout=5)
    after_runs = {x['run_id'] for x in rows(h, 'SELECT run_id FROM worker.execution_runs')}
    resolved, proofs = _requests_since(owner, owner_before, RESOLVE_PATH), _requests_since(proof, proof_before, ATTEMPT_PATH)
    correlation = correlate_exclusive_denial(resolved, proofs, after_runs-before_runs, run_id)
    state, facts = run(h,run_id), attempts(h,run_id)
    completion, candidate_rows, final_rows = completions(h,run_id), candidates(h,run_id), outboxes(h,run_id)
    attempt = assert_denied_facts(state,facts,completion,candidate_rows,final_rows)
    assert model_calls(h,text) == [] and h.model.authentication_events(text) == []
    delivery = h.gateway.wait_delivery(run_id)
    assert delivery['delivery_state'] == 'ACCEPTED' and delivery['final_text'] == '本次执行未完成，请稍后重试。'
    assert delivery['intent_id'] == completion[0]['final_intent_id']
    assert worker.poll() is None and attempt['worker_id'] == 'worker-one'
    denied_head = head(h, state)
    # A fresh valid request on the same process succeeds: no failover, changed
    # credentials, restart or lowered attempts policy explains the first denial.
    follower_text = text+'-valid-follower'
    follower_id = submit(h,follower_text,conversation)
    follower_result = wait_success(h,follower_id)
    follower_delivery = h.wait_delivery(follower_id)
    calls = model_calls(h,follower_text)
    assert len(attempts(h,follower_id)) == len(calls) == 1
    follower_state = run(h,follower_id)
    assert_empty_head_follower(state, follower_state, denied_head,
                               follower_result['candidate'], calls[0], follower_text)
    assert worker.poll() is None
    return {'case':'actual_proof_request_digest_mismatch_maps_to_control_403',
        'run_id':run_id,'attempt':attempt,'completion':completion[0], 'model_http_attempt_count':0,
        'candidate_count':0,'final_count':1,'delivery':delivery,'proof_events':proofs,
        'session_id':state['session_id'],'session_sequence':state['session_sequence'],
        'accepted_head_after_denial':denied_head,
        'resolve_events':resolved,'correlation':correlation,
        'interval':{'started_ns':started_ns,'resolve_after_ordinal':owner_before,
                    'proof_after_ordinal':proof_before,'retained_runs_before':len(before_runs),
                    'retained_runs_after':len(after_runs)},
        'actual_control_status':403,'original_attempt_reason':'CREDENTIAL_DENIED',
        'worker_pid':worker.pid,'worker_alive_after_case':True,'process_killed':False,
        'operation_records':operation_records(h,run_id),
        'valid_follower':{'run_id':follower_id,'completion':follower_result['completion'],
                          'delivery':follower_delivery,'model_http_attempt_count':1,
                          'session_id':follower_state['session_id'], 'session_sequence':follower_state['session_sequence'],
                          'candidate_parent_ref':follower_result['candidate']['parent_ref'],
                          'candidate_parent_digest':follower_result['candidate']['parent_digest'],
                          'exact_history_contains_only_follower':True}}


def run_proof_authorization(h):
    proxies = getattr(h,'resolve_proxies',None)
    assert proxies and len(proxies) == 2, 'install mTLS relays before Control starts'
    owner,proof = proxies
    result = {'version':'worker-real-proof-authorization/v1','invariant_slice':'WV-13',
        'actual_control_and_worker_owners':True,'mtls_both_legs_verified':True,
        'synthetic_http_statuses':False,'business_sql_writes':False,
        'capabilities_or_credential_bodies_recorded':False,'cases':[],'result':'FAIL'}
    path=Path(h.artifacts)/'worker-proof-authorization.json'
    try:
        h.stop_fault_workers()
        worker=h.start_worker('worker-one', {'policy':{'max_attempts':3,'retry_backoff':'300ms'}})
        result['cases'].append(_proof_loss(h,owner,proof,worker))
        print('WORKER_PROOF_DEPENDENCY=PASS true Worker proof200 dropped; real Control503; new Attempt succeeds on same alive Worker; Delivery ACCEPTED',flush=True)
        result['cases'].append(_proof_denial(h,owner,proof,worker))
        print('WORKER_PROOF_UNAUTHORIZED=PASS request digest mismatch; true Worker403/Control403; one CREDENTIAL_DENIED Attempt; zero SDK/candidate; fixed failed Final ACCEPTED; same Worker valid follower succeeds',flush=True)
        result['result']='PASS'
        return result
    finally:
        proof.release()
        try:
            h.stop_fault_workers()
            restored = h.start_worker('worker-one')
            assert restored.poll() is None
            result['standard_worker_restored_pid'] = restored.pid
        except BaseException:
            result['result'] = 'FAIL'
            raise
        finally:
            path.write_text(json.dumps(result,indent=2)+'\n')
            assert json.loads(path.read_text()) == result
