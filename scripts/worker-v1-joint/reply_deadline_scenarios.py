"""WV-31: independent Reply deadlines never undo successful execution/history.

All time advances naturally in the disposable PostgreSQL fixture. Real public
Gateway inputs admit immutable Runs; no test writes business clocks or facts.
The second case stops the real Gateway before model release and observes the
original Worker outbox bytes retained by NATS until after their Reply deadline.
"""
from __future__ import annotations

from datetime import timedelta
import hashlib
import json
from pathlib import Path
import time
import uuid

from commit_scenarios import operation_records
from deadline_scenarios import (assert_fixed_window, assert_initial_deadline,
                                completion_facts, fresh_follower, manifest_window,
                                restore_standard, run_fact, wait_status)
from faults import attempts, candidates, head, model_calls, one, outboxes, parse_time, rows, run, submit
from reply_scenarios import FINAL_PATH, messages, proof_request, receipt, retained, source_final, stream_info
from resolve_scenarios import events_for_run
from scope_scenarios import assert_transcript


def assert_reply_only_expired(observation):
    assert observation['status'] == 'RUNNING' and observation['attempt_status'] == 'EXECUTING'
    assert observation['agent_started_at'] is not None
    now = parse_time(observation['db_now'])
    assert observation['reply_expired'] is True and now >= parse_time(observation['reply_deadline'])
    assert observation['run_expired'] is False and now < parse_time(observation['run_deadline'])
    assert observation['execution_expired'] is False and now < parse_time(observation['execution_deadline'])
    assert observation['lease_live'] is True and now < parse_time(observation['lease_until'])
    return observation


def live_expiry(h, run_id):
    # One materialized database instant determines every predicate. Do not use a
    # Python sleep or independently sampled clocks to claim live authorization.
    return one(h, "WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS db_now) "
        "SELECT r.run_id,r.status,a.attempt_id,a.status AS attempt_status,a.agent_started_at,"
        "r.run_deadline,r.execution_deadline,r.reply_deadline,a.lease_until,observed.db_now,"
        "r.reply_deadline<=observed.db_now AS reply_expired,"
        "r.run_deadline<=observed.db_now AS run_expired,"
        "r.execution_deadline<=observed.db_now AS execution_expired,"
        "a.lease_until>observed.db_now AS lease_live FROM worker.execution_runs r "
        "JOIN worker.execution_attempts a ON a.tenant_id=r.tenant_id AND a.attempt_id=r.current_attempt_id "
        "CROSS JOIN observed WHERE r.run_id=" + h.quote(run_id))


def require_success_without_reply(record, completion, aa, cc, final, accepted):
    assert record['status'] == 'SUCCEEDED' and record['attempts'] == 1
    assert len(completion) == len(aa) == len(cc) == 1
    outcome, attempt, candidate = completion[0], aa[0], cc[0]
    assert attempt['status'] == 'SUCCEEDED' and attempt['agent_started_at'] is not None
    assert not attempt['reason'], 'successful execution must not become a deadline failure'
    assert outcome['status'] == 'SUCCEEDED' and outcome['kind'] == 'ATTEMPT'
    assert outcome['reply_disposition'] == 'NONE' and outcome['reason'] == 'DEADLINE_EXPIRED'
    assert not outcome['final_intent_id'] and final == []
    assert outcome['attempt_id'] == attempt['attempt_id'] == candidate['attempt_id']
    assert outcome['candidate_ref'] == candidate['candidate_ref'] == accepted['accepted_ref']
    assert outcome['candidate_digest'] == candidate['content_digest'] == accepted['accepted_digest']
    assert accepted['settled_sequence'] == record['session_sequence']
    completed = parse_time(outcome['completed_at'])
    assert parse_time(record['reply_deadline']) <= completed
    assert completed < min(parse_time(record['execution_deadline']), parse_time(record['run_deadline']))
    return outcome


def delivery_facts(h, run_id):
    return {'intents': rows(h, "SELECT intent_id FROM gateway.gateway_delivery_intents WHERE run_id=" + h.quote(run_id)),
            'parts': rows(h, "SELECT p.part_id,p.state FROM gateway.gateway_delivery_parts p JOIN "
                "gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=" + h.quote(run_id)),
            'attempts': rows(h, "SELECT a.attempt_id FROM gateway.gateway_delivery_attempts a JOIN "
                "gateway.gateway_delivery_parts p USING(part_id) JOIN gateway.gateway_delivery_intents i USING(intent_id) "
                "WHERE i.run_id=" + h.quote(run_id)), 'external_messages': messages(h, run_id)}


def require_zero_delivery(facts):
    assert facts == {'intents': [], 'parts': [], 'attempts': [], 'external_messages': []}
    return facts


def worker_facts(h, run_id):
    return {'run': run(h, run_id), 'attempts': attempts(h, run_id),
            'completion': completion_facts(h, run_id), 'candidates': candidates(h, run_id),
            'outbox': outboxes(h, run_id),
            'session_commits': rows(h, 'SELECT run_id,session_id,candidate_ref,candidate_digest FROM '
                'worker.execution_session_commits WHERE run_id=' + h.quote(run_id))}


def require_session_commit(facts, accepted):
    assert len(facts['session_commits']) == 1
    commit = facts['session_commits'][0]
    assert commit == {'run_id': facts['run']['run_id'], 'session_id': facts['run']['session_id'],
                      'candidate_ref': accepted['accepted_ref'], 'candidate_digest': accepted['accepted_digest']}
    assert facts['completion'][0]['candidate_ref'] == commit['candidate_ref']
    assert facts['completion'][0]['candidate_digest'] == commit['candidate_digest']
    return commit


def require_expired_receipt(value, digest, deadline, stream_id, sequence):
    assert len(value) == 1
    result = value[0]
    assert result['outcome'] == 'REJECTED' and result['reason'] == 'EXPIRED'
    assert result['raw_digest'] == digest
    assert result['stream_name'] == 'REPLY_INTENTS_V1'
    assert result['stream_id'] == stream_id and result['stream_sequence'] == sequence
    assert parse_time(result['recorded_at']) >= parse_time(deadline)
    return result


def require_ended_attempt_proof(events, expected, attempt, after_ns):
    assert attempt['status'] == 'SUCCEEDED' and attempt['ended_at'] is not None
    assert expected['attempt_id'] == attempt['attempt_id']
    assert events, 'expired Final needs a directly observed real Worker proof'
    for event in events:
        assert event['path'] == FINAL_PATH
        assert event.get('upstream_status') == event.get('downstream_status') == 200
        assert event.get('downstream_action') == 'forwarded' and 'fault' not in event
        assert event.get('owner_response_bytes', 0) > 0
        assert event.get('downstream_body_bytes_forwarded') == event['owner_response_bytes']
        assert event.get('finished_ns', 0) >= event.get('owner_response_ns', 0) >= event['started_ns'] >= after_ns
        assert all(event.get(key) == value for key, value in expected.items())
    return events


def configure(h, reply_age):
    h.stop_fault_workers()
    worker = h.start_worker('worker-one', {'policy': {'max_run_age': '150s', 'max_reply_age': reply_age}})
    assert worker.poll() is None
    return worker


def assert_window(record, attempt, manifest, reply_seconds):
    assert manifest['max_run_seconds'] == 120, 'published execution window must remain the real 120s contract'
    assert record['policy_json']['MaxRunAge'] == 150000000000
    assert record['policy_json']['MaxReplyAge'] == reply_seconds * 1000000000
    assert parse_time(record['reply_deadline']) == parse_time(record['received_at']) + timedelta(seconds=reply_seconds)
    assert_initial_deadline(record, attempt, manifest)
    assert parse_time(record['execution_deadline']) < parse_time(record['run_deadline'])


def restore_ready(h, record, field='restoration'):
    # Preserve the observed restoration result before validating it, so a Worker
    # that exits immediately after readiness cannot leave this case marked PASS.
    restored = restore_standard(h)
    record[field] = restored
    assert restored.get('ready') is True, 'standard Worker exited during restoration'
    return restored


def finish_follower(h, record, text, conversation, old_input, parent):
    # A fresh admission gets a fresh Reply window; never enqueue the follower
    # before waiting for the older Run's short window to expire.
    restore_ready(h, record, 'follower_configuration')
    record['follower'] = fresh_follower(h, text, conversation, [old_input], parent)
    assert record['follower']['run']['session_id'] == record['success']['run']['session_id']
    assert record['follower']['run']['session_sequence'] == record['success']['run']['session_sequence'] + 1


def complete_after_reply_expiry(h, record):
    marker = 'reply-only-expiry-' + uuid.uuid4().hex[:10]
    text, follower = marker + '-successful-unsent', marker + '-follower'
    chat = str(5000000000 + int(uuid.uuid4().hex[:7], 16))
    try:
        worker = configure(h, '4s')
        record.update(case='reply-expires-during-successful-execution', worker_pid=worker.pid)
        h.model.hold(text)
        run_id = submit(h, text, chat)
        h.model.wait_entered(text)
        initial = run_fact(h, run_id)
        initial_attempts = attempts(h, run_id)
        assert len(initial_attempts) == 1
        manifest = manifest_window(h)
        assert_window(initial, initial_attempts[0], manifest, 4)
        record.update(run_id=run_id, initial=initial, manifest=manifest, initial_attempt=initial_attempts[0])
        observed = h.wait(lambda: (lambda v: v if v['reply_expired'] and v['lease_live'] else None)(live_expiry(h, run_id)),
                          'DB Reply deadline expired while execution lease remains live', timeout=15)
        record['reply_expiry_while_live'] = assert_reply_only_expired(observed)
        assert completions_empty(h, run_id) and candidates(h, run_id) == []
        h.model.release(text)
        succeeded = wait_status(h, run_id, 'SUCCEEDED')
        accepted = head(h, succeeded)
        facts = worker_facts(h, run_id)
        require_session_commit(facts, accepted)
        outcome = require_success_without_reply(succeeded, facts['completion'], facts['attempts'],
                                                facts['candidates'], facts['outbox'], accepted)
        assert_fixed_window(initial, succeeded)
        assert facts['attempts'][0]['attempt_id'] == observed['attempt_id']
        calls = model_calls(h, text)
        assert len(calls) == 1
        assert_transcript(calls[0], [text])
        record.update(success=facts, final_run=succeeded, accepted_head=accepted, model_request=calls[0],
                      gateway=require_zero_delivery(delivery_facts(h, run_id)))
        assert rows(h, "SELECT stream_sequence FROM gateway.gateway_reply_transport_receipts WHERE run_id=" + h.quote(run_id)) == []
        h.wait(lambda: any(e.get('operation') == 'complete' and e.get('result') == 'ok'
                          for e in operation_records(h, run_id)), 'typed success after Reply-only expiry')
        logs = operation_records(h, run_id)
        for name in ('execute', 'session_stage', 'complete'):
            matches = [e for e in logs if e.get('operation') == name]
            assert len(matches) == 1 and matches[0]['result'] == 'ok'
            assert matches[0]['attempt_id'] == outcome['attempt_id']
        record['typed_operations'] = logs
        finish_follower(h, record, follower, chat, text, accepted)
        assert worker_facts(h, run_id) == facts
        require_zero_delivery(delivery_facts(h, run_id))
        assert len(model_calls(h, text)) == 1
        record['result'] = 'PASS'
    finally:
        h.model.release(text)
        restore_ready(h, record)


def completions_empty(h, run_id):
    return completion_facts(h, run_id) == [] and outboxes(h, run_id) == []


def final_arrives_after_reply_expiry(h, record):
    marker = 'reply-broker-expiry-' + uuid.uuid4().hex[:10]
    text, follower = marker + '-queued-final', marker + '-follower'
    chat = str(5300000000 + int(uuid.uuid4().hex[:7], 16))
    gateway_stopped = False
    try:
        worker = configure(h, '20s')
        record.update(case='original-committed-final-arrives-after-reply-deadline', worker_pid=worker.pid)
        h.model.hold(text)
        run_id = submit(h, text, chat)
        h.model.wait_entered(text)
        initial = run_fact(h, run_id)
        aa = attempts(h, run_id)
        assert len(aa) == 1
        manifest = manifest_window(h)
        assert_window(initial, aa[0], manifest, 20)
        record.update(run_id=run_id, initial=initial, manifest=manifest)
        gateway_pid = h.gateway.process.pid
        h.gateway.stop()
        gateway_stopped = True
        assert h.gateway.process.returncode == 0
        record['gateway_stopped'] = {'pid': gateway_pid, 'exit_code': h.gateway.process.returncode}
        original_stream = stream_info(h)
        stream_id = original_stream['created']
        first_sequence = original_stream['state']['last_seq'] + 1
        record['broker_stream_id'] = stream_id
        h.model.release(text)
        succeeded = wait_status(h, run_id, 'SUCCEEDED')
        source = h.wait(lambda: source_final(h, run_id), 'actual Worker Final source PubAck while Gateway stopped', timeout=15)
        accepted = head(h, succeeded)
        facts = worker_facts(h, run_id)
        require_session_commit(facts, accepted)
        assert len(facts['completion']) == len(facts['attempts']) == len(facts['candidates']) == len(facts['outbox']) == 1
        outcome = facts['completion'][0]
        assert outcome['status'] == 'SUCCEEDED' and outcome['reply_disposition'] == 'FINAL' and not outcome['reason']
        assert parse_time(outcome['completed_at']) < parse_time(initial['reply_deadline'])
        assert facts['attempts'][0]['status'] == 'SUCCEEDED' and facts['attempts'][0]['attempt_id'] == aa[0]['attempt_id']
        assert outcome['candidate_ref'] == accepted['accepted_ref'] == facts['candidates'][0]['candidate_ref']
        assert outcome['candidate_digest'] == accepted['accepted_digest'] == facts['candidates'][0]['content_digest']
        assert outcome['kind'] == 'ATTEMPT' and outcome['attempt_id'] == aa[0]['attempt_id']
        assert facts['candidates'][0]['attempt_id'] == aa[0]['attempt_id']
        assert source['event']['intent_id'] == facts['outbox'][0]['intent_id'] == outcome['final_intent_id']
        assert source['event'] == facts['outbox'][0]['payload']
        assert parse_time(source['event']['deadline']) == parse_time(initial['reply_deadline'])
        published_stream = stream_info(h)
        assert published_stream['created'] == stream_id, 'broker incarnation changed during original Final publication'
        last_sequence = published_stream['state']['last_seq']
        assert first_sequence <= last_sequence < first_sequence + 16, 'bounded original-source broker observation'
        found = [n for n in range(first_sequence, last_sequence + 1) if retained(h, n) == source['raw']]
        assert len(found) == 1, 'original outbox bytes must be present once in the real broker'
        sequence = found[0]
        assert receipt(h, sequence) == []
        require_zero_delivery(delivery_facts(h, run_id))
        record.update(success=facts, accepted_head=accepted, source_reply_digest=source['digest'],
                      source_reply_sha256=hashlib.sha256(source['raw']).hexdigest(), broker_sequence=sequence,
                      original_reply_retained_before_expiry=True)
        expired = h.wait(lambda: (lambda r: r if parse_time(r['db_now']) >= parse_time(r['reply_deadline']) else None)(run_fact(h, run_id)),
                         'DB Reply deadline passes with original Final retained in broker', timeout=30)
        assert not expired['run_expired'] and not expired['execution_expired']
        assert_fixed_window(initial, expired)
        assert retained(h, sequence) == source['raw'] and receipt(h, sequence) == []
        assert worker_facts(h, run_id) == facts and head(h, expired) == accepted
        record['reply_expired_before_gateway_restart'] = expired
        assert facts['attempts'][0]['ended_at'] is not None
        assert stream_info(h)['created'] == stream_id
        restart_ns = time.monotonic_ns()
        record['gateway_restart_started_ns'] = restart_ns
        h.gateway.start()
        gateway_stopped = False
        record['gateway_restarted_pid'] = h.gateway.process.pid
        assert h.gateway.process.pid != gateway_pid and h.gateway.process.poll() is None
        received = h.wait(lambda: rows(h, "SELECT stream_name,stream_id,stream_sequence,outcome,reason,raw_digest,recorded_at FROM gateway.gateway_reply_transport_receipts "
                           "WHERE stream_name='REPLY_INTENTS_V1' AND stream_id=" + h.quote(stream_id) + " AND stream_sequence=" + str(sequence)),
                          'real Gateway durable expired rejection of original committed Final', timeout=35)
        record['expired_transport_receipt'] = require_expired_receipt(received, source['digest'], initial['reply_deadline'], stream_id, sequence)
        assert stream_info(h)['created'] == stream_id
        proof_proxy = h.resolve_proxies[1]
        proof_events = h.wait(lambda: (lambda events: events if events and all(e.get('finished_ns') for e in events) else None)(
            events_for_run(proof_proxy, run_id, FINAL_PATH)), 'direct ended-Attempt Final proof response settled', timeout=10)
        expected_proof = proof_request(source['event']) | {'tenant_id': h.tenant_id, 'manifest_digest': h.manifest_digest}
        record['ended_attempt_final_proof_events'] = require_ended_attempt_proof(
            proof_events, expected_proof, facts['attempts'][0], restart_ns)
        record['ended_attempt_observed_before_gateway_restart'] = facts['attempts'][0]
        h.wait(lambda: retained(h, sequence) is None, 'expired Final ACK after durable transport rejection', timeout=15)
        record['acked_after_durable_expired_receipt'] = True
        record['gateway'] = require_zero_delivery(delivery_facts(h, run_id))
        assert worker_facts(h, run_id) == facts and head(h, expired) == accepted
        calls = model_calls(h, text)
        assert len(calls) == 1
        assert_transcript(calls[0], [text])
        record['model_request'] = calls[0]
        finish_follower(h, record, follower, chat, text, accepted)
        assert worker_facts(h, run_id) == facts
        require_zero_delivery(delivery_facts(h, run_id))
        assert len(model_calls(h, text)) == 1
        record['result'] = 'PASS'
    finally:
        h.model.release(text)
        try:
            if gateway_stopped:
                h.gateway.start()
        finally:
            restore_ready(h, record)


def run_reply_deadline(h):
    path = Path(h.artifacts) / 'worker-reply-deadline.json'
    evidence = {'version': 'worker-independent-reply-deadline/v1', 'invariants': ['WV-31', 'WV-25'],
                'business_sql_writes': False, 'manifest_or_token_modified': False,
                'deadline_clock': 'actual PostgreSQL clock_timestamp()', 'cases': [], 'result': 'FAIL'}
    try:
        for scenario in (complete_after_reply_expiry, final_arrives_after_reply_expiry):
            record = {'result': 'FAIL'}
            evidence['cases'].append(record)
            try:
                scenario(h, record)
            except BaseException:
                record['result'] = 'FAIL'
                raise
            print('WORKER_REPLY_DEADLINE=PASS ' + record['case'] +
                  '; execution and Session succeeded; zero victim send; fresh follower reuses successful unsent history', flush=True)
        evidence['result'] = 'PASS'
        return evidence
    finally:
        path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
        assert json.loads(path.read_text()) == evidence
