"""WV-11/13: real Control batches corrupted only by a labelled test relay.

The upstream public runtime API must really return HTTP 200. The relay mutates
that completed response, never fabricates an authorization or writes business
SQL. Real Worker rejection, fixed failure Final, and a fresh same-Session Run
are observed through OS processes, PostgreSQL, SDK HTTP, and Gateway Delivery.
Call run_credential_batch(h) before live credential clear, with installed relays.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
import uuid

from commit_scenarios import operation_records
from faults import (attempts, candidates, completions, head, model_calls,
                    outboxes, run, submit, wait_success)
from resolve_fault_proxy import ATTEMPT_PATH, RESOLVE_PATH
from resolve_scenarios import assert_resolve_once, events_for_run
from scope_scenarios import assert_transcript


MODES = ('missing_use', 'extra_use', 'duplicate_use', 'identity_mismatch')
FAILURE_FINAL = '本次执行未完成，请稍后重试。'


def require_mutation(event, mode, run_id, attempt_id):
    """Require completed real-owner evidence, not just a fixture arm request."""
    assert mode in MODES
    assert event and event.get('path') == RESOLVE_PATH
    assert event.get('fault') == mode and event.get('mutation') == mode
    assert event.get('upstream_status') == event.get('downstream_status') == 200
    assert event.get('downstream_action') == 'mutated_owner_200'
    assert event.get('mutation_phase') == 'complete_owner_200_response'
    assert event.get('owner_response_bytes', 0) > 0
    assert event.get('downstream_body_bytes_forwarded', 0) > 0
    assert event.get('run_id') == run_id and event.get('attempt_id') == attempt_id
    assert event.get('credential_count_before') == 2
    assert event.get('credential_count_after') == {
        'missing_use': 1, 'extra_use': 3, 'duplicate_use': 2, 'identity_mismatch': 2}[mode]
    assert event.get('owner_response_ns', 0) > 0
    assert event.get('finished_ns', 0) >= event['owner_response_ns']
    return copy.deepcopy(event)


def require_denied(state, attempt_facts, completion_facts, candidate_facts,
                   final_facts, before, after, delivery, *, sdk_calls, auth_events):
    """Reject early, accept one immutable failed outcome, preserve accepted head."""
    assert state['status'] == 'FAILED' and state['attempts'] == 1
    assert len(attempt_facts) == 1
    attempt = attempt_facts[0]
    assert attempt['status'] == 'FAILED' and attempt['reason'] == 'CREDENTIAL_DENIED'
    assert attempt['agent_started_at'] is None and attempt['ended_at'] is not None
    assert sdk_calls == [] and auth_events == [], 'denied batch reached external model HTTP'
    assert candidate_facts == [], 'denied batch wrote a Session candidate'
    assert len(completion_facts) == len(final_facts) == 1
    completion, final = completion_facts[0], final_facts[0]
    assert completion['kind'] == 'ATTEMPT' and completion['status'] == 'FAILED'
    assert completion['reason'] == 'CREDENTIAL_DENIED'
    assert completion['attempt_id'] == attempt['attempt_id']
    assert not completion['candidate_ref'] and not completion['candidate_digest']
    assert completion['reply_disposition'] == 'FINAL'
    assert completion['final_intent_id'] == final['intent_id'] == delivery['intent_id']
    assert delivery['run_id'] == state['run_id']
    assert delivery['delivery_state'] == 'ACCEPTED' and delivery['final_text'] == FAILURE_FINAL
    assert before['accepted_ref'] and before['accepted_digest'], 'seed must establish formal history'
    for field in ('accepted_ref', 'accepted_digest'):
        assert before[field] == after[field], 'denial changed accepted Session history'
    assert after['settled_sequence'] == before['settled_sequence'] + 1 == state['session_sequence']
    return {'run': copy.deepcopy(state), 'attempts': copy.deepcopy(attempt_facts),
            'completion': copy.deepcopy(completion), 'final': copy.deepcopy(final),
            'candidate_count': 0, 'sdk_call_count': 0, 'model_authentication_count': 0,
            'accepted_head_before': copy.deepcopy(before), 'accepted_head_after': copy.deepcopy(after),
            'delivery': copy.deepcopy(delivery)}


def require_early_operations(records, attempt_id):
    """Actual typed logs corroborate no Session initialization or SDK execution."""
    for name in ('credential_resolve', 'prepare'):
        matches = [r for r in records if r.get('operation') == name]
        assert len(matches) == 1 and matches[0]['result'] == 'credential_denied'
        assert matches[0]['attempt_id'] == attempt_id
    assert not any(r.get('operation') in ('session_open', 'session_load', 'execute',
                                          'session_stage', 'usage') for r in records)
    return copy.deepcopy(records)


def _scenario(h, owner, proof, mode, worker):
    marker = 'batch-' + mode + '-' + uuid.uuid4().hex[:10]
    conversation = str(4500000000 + int(uuid.uuid4().hex[:7], 16))
    seed, victim, follower = (marker + suffix for suffix in ('-seed', '-denied', '-follower'))
    seed_run = submit(h, seed, conversation)
    seed_result = wait_success(h, seed_run)
    seed_delivery = h.wait_delivery(seed_run)
    seed_state = run(h, seed_run)
    before = head(h, seed_state)
    assert len(model_calls(h, seed)) == 1
    assert_transcript(model_calls(h, seed)[0], [seed])
    assert worker.poll() is None, 'real Worker process stopped before batch fault'

    owner.arm(mode)
    victim_run = submit(h, victim, conversation)
    h.wait(lambda: run(h, victim_run)['status'] in ('FAILED', 'SUCCEEDED'),
           'actual Worker classifies mutated credential batch', timeout=30)
    victim_state, attempt_facts = run(h, victim_run), attempts(h, victim_run)
    assert victim_state['session_id'] == seed_state['session_id']
    assert victim_state['session_sequence'] == seed_state['session_sequence'] + 1
    assert len(attempt_facts) == 1
    attempt_id = attempt_facts[0]['attempt_id']
    h.wait(lambda: (owner.fault() or {}).get('finished_ns'), 'mutated real Control response settled', timeout=10)
    mutation = require_mutation(owner.fault(), mode, victim_run, attempt_id)
    assert mutation['worker_id'] == attempt_facts[0]['worker_id']
    assert mutation['lease_epoch'] == attempt_facts[0]['lease_epoch']
    # Harness.wait_delivery expects success; the Gateway helper correctly accepts
    # a real Worker-owned failed Final as well as a successful model Final.
    delivery = h.gateway.wait_delivery(victim_run)
    denied = require_denied(victim_state, attempt_facts, completions(h, victim_run),
        candidates(h, victim_run), outboxes(h, victim_run), before, head(h, victim_state), delivery,
        sdk_calls=model_calls(h, victim), auth_events=h.model.authentication_events(victim))
    resolves = events_for_run(owner, victim_run, RESOLVE_PATH)
    proofs = events_for_run(proof, victim_run, ATTEMPT_PATH)
    counts = assert_resolve_once(resolves, proofs, attempt_facts)
    assert len(resolves) == 1 and resolves[0] == mutation
    assert len(proofs) == 2 and all(e.get('upstream_status') == e.get('downstream_status') == 200 for e in proofs)
    assert all(e.get('attempt_id') == attempt_id for e in proofs)
    h.wait(lambda: any(r.get('operation') == 'prepare' for r in operation_records(h, victim_run)),
           'typed rejected batch Prepare record', timeout=10)
    observations = require_early_operations(operation_records(h, victim_run), attempt_id)

    follower_run = submit(h, follower, conversation)
    follower_result = wait_success(h, follower_run)
    follower_delivery = h.wait_delivery(follower_run)
    follower_state, follower_attempts = run(h, follower_run), attempts(h, follower_run)
    assert follower_state['session_id'] == seed_state['session_id']
    assert follower_state['session_sequence'] == victim_state['session_sequence'] + 1
    assert len(follower_attempts) == 1 and follower_attempts[0]['attempt_id'] != attempt_id
    assert follower_result['candidate']['parent_ref'] == before['accepted_ref']
    assert follower_result['candidate']['parent_digest'] == before['accepted_digest']
    request = model_calls(h, follower)
    assert len(request) == 1
    assert_transcript(request[0], [seed, follower])
    assert FAILURE_FINAL not in json.dumps(request[0]['messages'], ensure_ascii=False)
    assert attempts(h, victim_run) == attempt_facts, 'failed Attempt was reused by the follower'
    assert completions(h, victim_run) == [denied['completion']]
    assert outboxes(h, victim_run) == [denied['final']] and candidates(h, victim_run) == []
    assert model_calls(h, victim) == [] and h.model.authentication_events(victim) == []
    assert len(events_for_run(owner, victim_run, RESOLVE_PATH)) == 1, 'same Attempt resolved twice'
    assert worker.poll() is None, 'same real Worker must remain alive through recovery'
    return {'case': mode, 'result': 'PASS', 'worker_pid': worker.pid,
            'seed_run_id': seed_run, 'seed_candidate': seed_result['candidate'], 'seed_delivery': seed_delivery,
            'denied_run_id': victim_run, 'mutation_receipt': mutation, 'proof_events': proofs,
            'resolve_calls_per_attempt': counts, 'denied': denied, 'typed_operations': observations,
            'follower_run_id': follower_run, 'follower_attempt': follower_attempts[0],
            'follower_candidate': follower_result['candidate'], 'follower_completion': follower_result['completion'],
            'follower_delivery': follower_delivery, 'follower_request': request[0],
            'failed_attempt_and_outcome_unchanged_after_follower': True}


def run_credential_batch(h):
    proxies = getattr(h, 'resolve_proxies', None)
    assert proxies and len(proxies) == 2, 'install real-owner mTLS relays before Control startup'
    owner, proof = proxies
    path = Path(h.artifacts) / 'worker-credential-batch.json'
    evidence = {'version': 'worker-real-credential-batch/v1', 'invariants': ['WV-11', 'WV-13'],
                'fixture_response_mutation_only': True, 'business_sql_writes': False,
                'credential_bodies_or_capabilities_recorded': False, 'cases': [], 'result': 'FAIL'}
    try:
        h.stop_fault_workers()
        worker = h.start_worker('worker-one', {'policy': {'max_attempts': 3}})
        for mode in MODES:
            result = _scenario(h, owner, proof, mode, worker)
            evidence['cases'].append(result)
            # Persist completed real evidence even if a later case fails.
            path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + '\n')
            print('WORKER_CREDENTIAL_BATCH_' + mode.upper() + '=PASS real Control 200 mutated; '
                  'one CREDENTIAL_DENIED Attempt; zero SDK/candidate; fixed failed Final ACCEPTED; '
                  'fresh same-Session follower retains only accepted history', flush=True)
        evidence['result'] = 'PASS'
        return evidence
    finally:
        owner.release()
        proof.release()
        try:
            h.stop_fault_workers()
            restored = h.start_worker('worker-one')
            assert restored.poll() is None
            evidence['standard_worker_restored_pid'] = restored.pid
        except BaseException:
            evidence['result'] = 'FAIL'
            raise
        finally:
            path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + '\n')
            assert json.loads(path.read_text()) == evidence
