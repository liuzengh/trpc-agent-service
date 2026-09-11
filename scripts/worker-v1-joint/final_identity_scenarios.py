"""WV-26: successful execution is distinct from Final delivery authorization.

The real Worker produces a complete committed proof. A verified-mTLS fixture
relay changes exactly one legal response identity before actual Gateway sees it.
No business row is written by this helper, no owner proof/status is fabricated,
and a permanently rejected original wire is never republished as recovery.
Root runs this before retained unproved Reply negatives and credential clear.
"""
from __future__ import annotations

import json
from pathlib import Path
import uuid

from faults import (attempts, candidates, completions, head, model_calls, one,
                    outboxes, quote, rows, run, submit, wait_success)
from reply_scenarios import consumer_info, proof_request, retained, source_final, stream_info
from resolve_fault_proxy import FINAL_PATH
from scope_scenarios import assert_transcript

MODES = {'final_tenant_mismatch': 'tenant_id',
         'final_manifest_mismatch': 'manifest_digest'}


def require_mutation(event, mode, expected):
    """Match all original owner fields, not merely the fault's temporal vicinity."""
    if mode not in MODES:
        raise ValueError('unknown closed Final identity negative')
    assert event['method'] == 'POST' and event['path'] == FINAL_PATH
    assert event['principal_uri'] == 'spiffe://agent-platform/channel-gateway'
    assert event.get('upstream_status') == event.get('downstream_status') == 200
    assert event.get('fault') == event.get('mutation') == mode
    assert event.get('mutation_field') == MODES[mode]
    assert event.get('mutation_phase') == 'complete_owner_200_response'
    assert event.get('downstream_action') == 'mutated_owner_200'
    assert event.get('owner_response_bytes', 0) > 0
    assert event.get('downstream_body_bytes_forwarded', 0) > 0
    assert event['started_ns'] <= event['owner_response_ns'] <= event['finished_ns']
    for key, value in expected.items():
        assert event.get(key) == value, 'relay original owner proof differs from durable Final/Admission: '+key
    return event


def require_rejection(records, raw_digest, before_stream):
    assert len(records) == 1, 'one original Reply must have one durable transport receipt'
    result = records[0]
    assert result['stream_name'] == 'REPLY_INTENTS_V1'
    assert result['stream_id'] == before_stream['created']
    assert result['stream_sequence'] > before_stream['state']['last_seq']
    assert result['outcome'] == 'REJECTED' and result['reason'] == 'UNAUTHORIZED'
    assert result['raw_digest'] == raw_digest and result['recorded_at']
    return result


def require_follower(victim, follower, accepted, candidate, request, old_text, new_text):
    assert victim['status'] == follower['status'] == 'SUCCEEDED'
    assert victim['tenant_id'] == follower['tenant_id']
    assert victim['session_id'] == follower['session_id']
    assert victim['session_sequence'] == 1 and follower['session_sequence'] == 2
    assert accepted['settled_sequence'] == victim['session_sequence']
    assert accepted['accepted_ref'] and accepted['accepted_digest']
    assert candidate['parent_ref'] == accepted['accepted_ref']
    assert candidate['parent_digest'] == accepted['accepted_digest']
    assert_transcript(request, [old_text, new_text])


def _worker_facts(h, run_id):
    return {'run': run(h, run_id), 'attempts': attempts(h, run_id),
            'completions': completions(h, run_id), 'candidates': candidates(h, run_id),
            'outbox': outboxes(h, run_id)}


def _send_calls(h):
    with h.gateway_fixture.lock:
        return sum(c['method'] == 'sendMessage' for c in h.gateway_fixture.calls)


def _zero_delivery(h, run_id, intent_id):
    result = one(h, 'SELECT '
        '(SELECT count(*) FROM gateway.gateway_delivery_intents WHERE run_id='+quote(run_id)+') AS intents,'
        '(SELECT count(*) FROM gateway.gateway_delivery_parts WHERE intent_id='+quote(intent_id)+') AS parts,'
        '(SELECT count(*) FROM gateway.gateway_delivery_attempts a JOIN gateway.gateway_delivery_parts p '
        'USING(part_id) WHERE p.intent_id='+quote(intent_id)+') AS attempts')
    update = json.loads(h.gateway.updates[run_id])['message']
    bot_id = h.gateway.update_bots[run_id]
    result['external_messages'] = len([m for m in h.gateway_fixture.snapshot()
        if m['bot_id'] == bot_id and m['chat_id'] == str(update['chat']['id'])
        and m['source_message_id'] == str(update['message_id'])])
    assert result == {'intents': 0, 'parts': 0, 'attempts': 0, 'external_messages': 0}
    return result


def _transport(h, raw_digest):
    # Rejected transport records deliberately need not carry a Run/Intent ID.
    # Correlate the original canonical outbox digest and exact broker incarnation.
    return rows(h, 'SELECT stream_name,stream_id,stream_sequence,raw_digest,outcome,reason,recorded_at '
        "FROM gateway.gateway_reply_transport_receipts WHERE stream_name='REPLY_INTENTS_V1' "
        'AND raw_digest='+quote(raw_digest)+' ORDER BY stream_sequence')


def _scenario(h, proof, mode, record):
    marker = 'final-identity-'+uuid.uuid4().hex[:12]
    text, follower_text = marker+'-unsent', marker+'-follower'
    conversation = str(5600000000+int(uuid.uuid4().hex[:7], 16))
    before_stream = stream_info(h)
    before_runs = {r['run_id'] for r in rows(h, 'SELECT run_id FROM worker.execution_runs')}
    before_ordinal = max((e['request_ordinal'] for e in proof.events()), default=0)
    before_sends = _send_calls(h)
    record.update(conversation_id=conversation, before_stream_created=before_stream['created'],
                  before_stream_last_sequence=before_stream['state']['last_seq'])
    proof.arm(mode, path=FINAL_PATH)
    run_id = submit(h, text, conversation)
    record['run_id'] = run_id
    result = wait_success(h, run_id)  # Worker completion, not Gateway Delivery.
    source = h.wait(lambda: source_final(h, run_id), 'original committed Final PubAck', timeout=20)
    assert source['event'] == result['final']['payload']
    target = one(h, "SELECT tenant_id,route->>'manifest_digest' AS manifest_digest "
        'FROM gateway.gateway_admissions WHERE admission_id='+quote(source['event']['admission_id'])+
        ' AND run_id='+quote(run_id))
    expected = dict(proof_request(source['event']), **target)
    mutation = h.wait(lambda: (lambda e: e if e and e.get('finished_ns') else None)(proof.fault()),
                      'complete real Worker proof identity mutation forwarded', timeout=15)
    require_mutation(mutation, mode, expected)
    observed = [e for e in proof.events() if e['request_ordinal'] > before_ordinal
                and e.get('path') == FINAL_PATH and e.get('method') == 'POST']
    assert observed == [mutation], 'exclusive victim interval included another Final proof request'
    after_runs = {r['run_id'] for r in rows(h, 'SELECT run_id FROM worker.execution_runs')}
    assert after_runs-before_runs == {run_id}, 'fault interval admitted an unrelated Run'
    received = h.wait(lambda: _transport(h, source['digest']),
                      'durable Gateway UNAUTHORIZED rejection of original Final', timeout=30)
    rejection = require_rejection(received, source['digest'], before_stream)
    record.update(original_owner_proof=expected, mutation=mutation,
                  original_reply=source['event'], original_reply_digest=source['digest'],
                  rejected_transport_receipt=rejection)
    h.wait(lambda: retained(h, rejection['stream_sequence']) is None,
           'original Reply ACK after durable UNAUTHORIZED receipt', timeout=15)
    assert stream_info(h)['created'] == before_stream['created']
    record['broker_acked_after_durable_rejection'] = True
    record['victim_gateway'] = _zero_delivery(h, run_id, expected['intent_id'])
    assert _send_calls(h) == before_sends, 'rejected Final invoked external Telegram sendMessage'

    facts, accepted = _worker_facts(h, run_id), head(h, run(h, run_id))
    state, attempt = facts['run'], facts['attempts']
    assert state['status'] == 'SUCCEEDED' and state['attempts'] == 1 and state['session_sequence'] == 1
    assert source['tenant_id'] == target['tenant_id'] == state['tenant_id']
    assert len(attempt) == 1 and attempt[0]['status'] == 'SUCCEEDED'
    assert attempt[0]['attempt_id'] == expected['attempt_id']
    assert attempt[0]['generation'] == expected['execution_generation']
    assert facts['completions'] == [result['completion']]
    assert not result['completion']['reason'] and not attempt[0]['reason']
    assert facts['candidates'] == [result['candidate']] and facts['outbox'] == [result['final']]
    assert accepted['accepted_ref'] == result['candidate']['candidate_ref'] == result['completion']['candidate_ref']
    assert accepted['accepted_digest'] == result['candidate']['content_digest'] == result['completion']['candidate_digest']
    assert accepted['settled_sequence'] == state['session_sequence']
    calls = model_calls(h, text)
    assert len(calls) == 1
    assert_transcript(calls[0], [text])
    assert source['event']['content']['text'] == 'joint answer: '+text
    record.update(worker_after_rejection=facts, accepted_head_after_rejection=accepted,
                  victim_model_request=calls[0])

    # A new valid Run, not replay of the permanently rejected original message.
    follower_id = submit(h, follower_text, conversation)
    follower_result = wait_success(h, follower_id)
    delivery = h.wait_delivery(follower_id)
    follower_state, follower_attempts = run(h, follower_id), attempts(h, follower_id)
    follower_calls = model_calls(h, follower_text)
    assert len(follower_attempts) == len(follower_calls) == 1
    assert follower_attempts[0]['attempt_id'] != expected['attempt_id']
    require_follower(state, follower_state, accepted, follower_result['candidate'],
                     follower_calls[0], text, follower_text)
    assert delivery['delivery_state'] == 'ACCEPTED'
    assert delivery['intent_id'] == follower_result['completion']['final_intent_id']
    assert delivery['final_text'] == 'joint answer: '+follower_text
    follower_head = head(h, follower_state)
    assert follower_head['settled_sequence'] == follower_state['session_sequence']
    assert follower_head['accepted_ref'] == follower_result['candidate']['candidate_ref']
    assert follower_head['accepted_digest'] == follower_result['candidate']['content_digest']
    assert _worker_facts(h, run_id) == facts, 'delivery rejection or follower changed committed execution'
    assert model_calls(h, text) == calls and _transport(h, source['digest']) == received
    assert _zero_delivery(h, run_id, expected['intent_id']) == record['victim_gateway']
    assert _send_calls(h) == before_sends+1, 'only the fresh follower may send'
    assert stream_info(h)['created'] == before_stream['created']
    assert retained(h, rejection['stream_sequence']) is None
    assert [e for e in proof.events() if e.get('path') == FINAL_PATH and e.get('run_id') == run_id] == [mutation]
    record.update(result='PASS', original_wire_republished=False, execution_unchanged_after_follower=True,
        follower={'run': follower_state, 'attempt': follower_attempts[0], 'candidate': follower_result['candidate'],
                  'completion': follower_result['completion'], 'delivery': delivery,
                  'model_request': follower_calls[0], 'accepted_head': follower_head,
                  'history_includes_unsent_accepted_final': True},
        external_send_delta=1, original_final_external_send_count=0, deny_delete=True)


def run_final_identity(h):
    proofs = [p for p in getattr(h, 'resolve_proxies', []) if p.name == 'worker-proof']
    assert len(proofs) == 1, 'install the real-owner verified-mTLS proof relay before startup'
    proof = proofs[0]
    live = {name: p.pid for name, p in h.workers.items() if p.poll() is None}
    assert live, 'root must start a healthy Worker before this exclusive Final gate'
    path = Path(h.artifacts)/'worker-final-identity.json'
    evidence = {'version': 'worker-real-final-identity/v1', 'invariants': ['WV-26'],
        'actual_gateway_and_worker_owners': True, 'mtls_both_legs_verified': True,
        'mutation_boundary': 'complete real Worker 200 response; one legal identity field only',
        'synthetic_owner_proof_or_status': False, 'business_sql_writes': False,
        'original_wire_republished': False, 'worker_pids': live, 'cases': [], 'result': 'FAIL'}
    before_sends, before_messages = _send_calls(h), len(h.gateway_fixture.snapshot())
    before_models = len(h.model.requests)
    try:
        h.wait(lambda: (lambda c: c['num_pending'] == c['num_ack_pending'] == 0)(consumer_info(h)),
               'Final identity gate precedes retained unproved Reply negatives', timeout=30)
        for mode in MODES:
            record = {'case': mode, 'result': 'FAIL'}
            evidence['cases'].append(record)
            _scenario(h, proof, mode, record)
            path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2)+'\n')
            print('WORKER_FINAL_IDENTITY_'+mode.upper()+'=PASS complete actual Worker proof200; '
                  'one legal identity mismatch; durable UNAUTHORIZED rejection before ACK; '
                  'zero victim Delivery/send; committed Session retained by valid new follower', flush=True)
        assert _send_calls(h)-before_sends == len(h.gateway_fixture.snapshot())-before_messages == 2
        assert len(h.model.requests)-before_models == 4
        assert all(h.workers[name].pid == pid and h.workers[name].poll() is None for name, pid in live.items())
        evidence.update(result='PASS', worker_processes_unchanged=True,
                        model_http_request_delta=4, accepted_delivery_delta=2, external_send_delta=2,
                        rejected_original_final_count=2,
                        execution_and_delivery_are_independent=True)
        return evidence
    finally:
        proof.release()
        path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2)+'\n')
        assert json.loads(path.read_text()) == evidence
