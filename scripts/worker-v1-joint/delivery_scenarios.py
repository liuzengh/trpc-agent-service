"""Worker Final accepted externally, with its HTTP response deliberately lost.

Real Gateway persists UNKNOWN, not NOT_SENT or ACCEPTED. Reply transport ACK
means durable handoff and does not certify provider delivery. No automatic retry,
manual state repair or exactly-once guarantee is introduced. The explicit fake
external platform knows it accepted the message; the actual Gateway does not.
"""
import hashlib
import json
from pathlib import Path
import time
import uuid

from faults import attempts, candidates, completions, model_calls, rows, run, submit, wait_success
from gateway_fixture import _publish_reply
from reply_scenarios import receipt, retained, source_final, stream_info, verify_proof
from scope_scenarios import assert_transcript, binding_path, scope_round


def delivery_snapshot(h, run_id):
    quoted = h.quote(run_id)
    parts = rows(h, "SELECT i.intent_id,i.run_id,i.target,p.part_id,p.body,p.state,p.attempt_number,"
                 "p.next_attempt_at,p.current_attempt_id,a.result,a.finished_at "
                 "FROM gateway.gateway_delivery_intents i JOIN gateway.gateway_delivery_parts p USING(intent_id) "
                 "LEFT JOIN gateway.gateway_delivery_attempts a ON a.attempt_id=p.current_attempt_id "
                 "WHERE i.run_id=" + quoted + " ORDER BY p.part_index")
    delivery_attempts = rows(h, "SELECT a.attempt_id,a.attempt_number,a.result FROM gateway.gateway_delivery_attempts a "
                            "JOIN gateway.gateway_delivery_parts p USING(part_id) JOIN gateway.gateway_delivery_intents i USING(intent_id) "
                            "WHERE i.run_id=" + quoted + " ORDER BY a.attempt_number")
    candidate = [{key: value[key] for key in ('candidate_ref', 'attempt_id', 'parent_ref', 'parent_digest', 'content_digest')}
                 for value in candidates(h, run_id)]
    return {'worker_run': run(h, run_id), 'worker_attempts': attempts(h, run_id),
            'completions': completions(h, run_id), 'candidates': candidate,
            'parts': parts, 'delivery_attempts': delivery_attempts}


def assert_unknown(snapshot):
    assert snapshot['worker_run']['status'] == 'SUCCEEDED' and snapshot['worker_run']['attempts'] == 1
    assert len(snapshot['worker_attempts']) == len(snapshot['completions']) == len(snapshot['candidates']) == 1
    assert len(snapshot['parts']) == len(snapshot['delivery_attempts']) == 1
    part = snapshot['parts'][0]
    assert part['state'] == 'UNKNOWN' and part['attempt_number'] == 1 and part['next_attempt_at'] is None
    assert part['finished_at'] is not None
    assert part['result'] == {'Certainty': 'UNKNOWN', 'ErrorClass': 'temporary', 'ProviderMessageID': ''}
    assert snapshot['delivery_attempts'][0]['attempt_id'] == part['current_attempt_id']
    assert snapshot['delivery_attempts'][0]['result'] == part['result']


def matching_messages(h, text, conversation, bot_id):
    return [m for m in h.gateway_fixture.snapshot()
            if m['text'] == text and m['chat_id'] == conversation and m['bot_id'] == bot_id]


def run_delivery(h):
    live = {name: process.pid for name, process in h.workers.items() if process.poll() is None}
    if not live:
        process = h.start_worker()
        live = {'worker-one': process.pid}
    marker = 'delivery-loss-' + uuid.uuid4().hex[:12]
    text, follower_text = marker + '-first', marker + '-follower'
    final = 'joint answer: ' + text
    conversation = str(3100000000 + int(uuid.uuid4().hex[:7], 16))
    bot_id = h.gateway.bot_id
    path = Path(h.artifacts) / 'worker-delivery-uncertainty.json'
    evidence = {'version': 'worker-v1-delivery-response-loss/v1', 'result': 'FAIL',
                'criteria': ['WV-26', 'WV-27', 'WV-29', 'WV-30'],
                'external_fixture': 'Telegram accepts and records one exact message, then closes TCP before any HTTP response',
                'product_sql_writes': 0, 'exactly_once_claim': False,
                'transport_receipt_meaning': 'ACCEPTED means durable Gateway handoff, not provider acknowledgement',
                'expected_gateway_outcome': 'UNKNOWN; no automatic retry without new authoritative delivery evidence',
                'coverage_limits': ['no exactly-once guarantee or automatic reconciliation from external fixture knowledge',
                                    'authenticated proof-response identity mismatch remains a separate branch'],
                'conversation_id': conversation, 'bot_id': bot_id, 'worker_pids': live}
    disconnected = False
    try:
        h.gateway_fixture.lose_response_once(final, conversation, bot_id=bot_id)
        run_id = submit(h, text, conversation)
        evidence['run_id'] = run_id
        outcome = wait_success(h, run_id)
        h.wait(lambda: (lambda items: len(items) == 1 and items[0]['state'] == 'UNKNOWN')(
            rows(h, "SELECT p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=" + h.quote(run_id))),
            'actual Gateway persists UNKNOWN after external acceptance and lost HTTP response', timeout=30)
        before = delivery_snapshot(h, run_id)
        assert_unknown(before)
        assert before['parts'][0]['intent_id'] == outcome['completion']['final_intent_id']
        assert len(model_calls(h, text)) == 1
        assert_transcript(model_calls(h, text)[0], [text])
        external = matching_messages(h, final, conversation, bot_id)
        assert len(external) == 1
        update = json.loads(h.gateway.updates[run_id])['message']
        assert external[0]['source_message_id'] == str(update['message_id'])
        losses = [loss for loss in h.gateway_fixture.response_loss_snapshot() if loss['message'] == external[0]]
        assert len(losses) == 1 and not losses[0]['http_response_written']
        source = h.wait(lambda: source_final(h, run_id), 'original Worker Final outbox PubAck', timeout=20)
        verified = verify_proof(h, source['event'])
        status, proof = verified['status'], verified['body']
        assert status == 200, 'actual Worker must own the committed Final before replay'
        assert proof['tenant_id'] == h.tenant_id and proof['manifest_digest'] == before['parts'][0]['target']['ManifestDigest']
        initial_receipts = h.wait(lambda: rows(h, "SELECT stream_sequence,raw_digest,outcome,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id=" + h.quote(run_id)),
                                  'durable Reply handoff exists despite UNKNOWN provider outcome', timeout=30)
        assert len(initial_receipts) == 1 and initial_receipts[0]['outcome'] == 'ACCEPTED'
        assert initial_receipts[0]['raw_digest'] == source['digest'] and initial_receipts[0]['intent_id'] == outcome['completion']['final_intent_id']
        h.wait(lambda: retained(h, initial_receipts[0]['stream_sequence']) is None,
               'original Reply is ACKed only after durable Gateway handoff', timeout=20)
        time.sleep(2)  # multiple existing sender scans; never authorizes a retry
        assert delivery_snapshot(h, run_id) == before and matching_messages(h, final, conversation, bot_id) == external
        killed_pid = h.gateway.process.pid
        h.gateway.stop(kill=True)
        assert h.gateway.process.returncode == -9
        for name in live:
            h.proof_switch.remove(name)
        disconnected = True
        acknowledgement = _publish_reply(h, source['raw'])
        sequence = int(acknowledgement['seq'])
        assert sequence != initial_receipts[0]['stream_sequence']
        h.gateway.start()
        replayed = h.wait(lambda: receipt(h, sequence), 'original Final replay receipt while Worker proof is offline', timeout=30)
        assert replayed == [['ACCEPTED', '', source['digest']]]
        h.wait(lambda: retained(h, sequence) is None, 'replayed original Final handoff is ACKed', timeout=20)
        assert stream_info(h)['config']['deny_delete'] is True
        time.sleep(2)
        after_replay = delivery_snapshot(h, run_id)
        assert_unknown(after_replay)
        assert after_replay == before and matching_messages(h, final, conversation, bot_id) == external
        assert len(model_calls(h, text)) == 1
        assert all(h.workers[name].pid == pid and h.workers[name].poll() is None for name, pid in live.items())
        for name in live:
            h.proof_switch.add(name, '127.0.0.1', h.worker_ports[name]['proof'])
        disconnected = False
        previous = {'session_id': before['worker_run']['session_id'], 'session_sequence': before['worker_run']['session_sequence'], 'completion': outcome['completion']}
        follower = scope_round(h, follower_text, conversation, h.api('GET', binding_path(h)), [text, follower_text], previous)
        assert follower['delivery']['delivery_state'] == 'ACCEPTED'
        assert delivery_snapshot(h, run_id) == before and matching_messages(h, final, conversation, bot_id) == external
        evidence.update(result='PASS', worker_final=outcome['completion'], source_reply_sha256=hashlib.sha256(source['raw']).hexdigest(),
                        source_reply_digest=source['digest'], actual_worker_proof=proof, actual_worker_proof_status=status,
                        first_transport_receipt=initial_receipts[0], provider_response_loss=losses[0],
                        before_restart=before, after_restart_replay=after_replay,
                        replay={'stream_sequence': sequence, 'transport_receipt': replayed, 'original_bytes': True,
                                'broker_acked': True, 'proof_backends_offline': True, 'deny_delete': True},
                        gateway_sigkill={'pid': killed_pid, 'exit_status': -9, 'new_pid': h.gateway.process.pid},
                        worker_processes_unchanged=True, worker_model_calls=1,
                        external_victim_send_count=1, gateway_victim_delivery_attempts=1,
                        gateway_victim_state='UNKNOWN', follower=follower,
                        unknown_does_not_erase_accepted_worker_session=True)
        print('WORKER_DELIVERY_UNCERTAINTY=PASS external message accepted then HTTP response lost; durable UNKNOWN/no retry; Gateway SIGKILL and original Reply replay ACK with proof offline; one Worker execution/send; follower uses committed Session', flush=True)
        return evidence
    finally:
        if disconnected:
            for name in live:
                h.proof_switch.add(name, '127.0.0.1', h.worker_ports[name]['proof'])
        path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
        assert json.loads(path.read_text()) == evidence
