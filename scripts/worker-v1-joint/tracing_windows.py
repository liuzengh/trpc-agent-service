"""Exact durable handoff windows in a private real-process tracing fixture.

The test temporarily revokes only existing runtime table privileges, never writes
business rows or adds production pause flags. Independent SQL/broker reads prove
the committed boundary before SIGKILL; privileges are restored before restart.
"""
from contextlib import contextmanager
import base64
import json
import time
import uuid

from intake_scenarios import admin, consumer_info, stream_info
from faults import model_calls


ALLOWED = {
    ("session_runtime", "runtime_session.session_candidates", "INSERT"),
    ("worker_runtime", "worker.execution_session_commits", "INSERT"),
    ('gateway_runtime', 'gateway.gateway_outbox', 'UPDATE'),
    ('worker_runtime', 'worker.execution_attempts', 'INSERT'),
    ('worker_runtime', 'worker.execution_reply_outbox', 'SELECT'),
    ('gateway_runtime', 'gateway.gateway_reply_transport_receipts', 'INSERT'),
    ('gateway_runtime', 'gateway.gateway_delivery_parts', 'UPDATE'),
}


@contextmanager
def revoke(h, grants, events):
    assert h.pg == h.prefix + '-pg' and h.pg in h.containers
    assert grants and all(tuple(g) in ALLOWED for g in grants)
    restored = []
    try:
        for role, table, privilege in grants:
            assert h.sql("SELECT has_table_privilege(" + h.quote(role) + ',' + h.quote(table) + ',' + h.quote(privilege) + ')') == [['t']]
            admin(h, f'REVOKE {privilege} ON {table} FROM {role}')
            restored.append((role, table, privilege))
            events.append({'event': 'privilege_revoked', 'role': role, 'table': table, 'privilege': privilege})
        yield
    finally:
        for role, table, privilege in reversed(restored):
            admin(h, f'GRANT {privilege} ON {table} TO {role}')
            assert h.sql("SELECT has_table_privilege(" + h.quote(role) + ',' + h.quote(table) + ',' + h.quote(privilege) + ')') == [['t']]
            events.append({'event': 'privilege_restored', 'role': role, 'table': table, 'privilege': privilege})


def marker(case):
    return 'TRACE_BODY_CANARY-' + case + '-' + uuid.uuid4().hex[:10], str(4000000000 + int(uuid.uuid4().hex[:7], 16))


def snapshot(h, run):
    q = h.quote(run)
    return {
        'worker': h.sql("SELECT status,traceparent FROM worker.execution_runs WHERE run_id=" + q),
        'attempts': h.sql("SELECT attempt_id,status FROM worker.execution_attempts WHERE run_id=" + q),
        'completion': h.sql("SELECT completion_id,status FROM worker.execution_completions WHERE run_id=" + q),
        'reply': h.sql("SELECT intent_id,digest,encode(payload,'hex'),COALESCE(traceparent,''),(published_at IS NOT NULL)::text FROM worker.execution_reply_outbox WHERE run_id=" + q),
        'delivery': h.sql("SELECT intent_id,COALESCE(traceparent,'') FROM gateway.gateway_delivery_intents WHERE run_id=" + q),
        'receipt': h.sql("SELECT outcome FROM gateway.gateway_reply_transport_receipts WHERE run_id=" + q),
        'parts': h.sql("SELECT state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i ON i.intent_id=p.intent_id WHERE i.run_id=" + q),
    }


def saved_span(query_trace, backend, carrier, name):
    fields = carrier.split('-')
    assert len(fields) == 4 and len(fields[1]) == 32 and len(fields[2]) == 16
    # Wait for the already-ended handoff span to reach the real backend before
    # killing its process; this is observation, not a guessed sleep boundary.
    document, spans = query_trace(backend, fields[1], {name})
    def span_id(value):
        try:
            return bytes.fromhex(value).hex() if len(value) == 16 else base64.b64decode(value).hex()
        except ValueError:
            return ""
    assert any(s["name"] == name and span_id(s["spanId"]) == fields[2] for s in spans), "persisted handoff Span not exported"
    return fields[1]


def assert_post_restart(h, backend, verify_run, run, text, before, restart_ns, role):
    h.wait_delivery(run)
    evidence = verify_run(h, backend, run, allow_missing_parents=True)
    after = snapshot(h, run)
    assert len(after['attempts']) == 1 and len(after['completion']) == 1 and len(after['reply']) == 1
    assert len(model_calls(h, text)) == 1, 'restart repeated Runner execution'
    for field, index in [('worker', 1), ('reply', 3), ('delivery', 1)]:
        if before[field]:
            assert before[field][0][index] == after[field][0][index], 'durable context changed: ' + field
    if before['reply']:
        assert before['reply'][0][:4] == after['reply'][0][:4], 'Reply payload/digest/identity changed'
    # The normal verifier has checked all actual trace IDs and carrier identities.
    # Now require a real child started after this process restart boundary.
    document = json.loads((h.artifacts / (run + '-trace.json')).read_text())
    def spans(value):
        if isinstance(value, dict):
            if 'spanId' in value and 'name' in value:
                yield value
            for v in value.values():
                yield from spans(v)
        elif isinstance(value, list):
            for v in value:
                yield from spans(v)
    name = 'worker.run.attempt' if role == 'claim' else 'publish execution.reply-intent.v1' if role == 'reply' else 'gateway.reply.deliver' if role == 'delivery' else 'publish execution.run-requested.v1'
    observed = [s for s in spans(document) if s['name'] == name and int(s['startTimeUnixNano']) >= restart_ns]
    assert observed, 'no actual post-restart span for ' + name
    return {'run_id': run, 'trace': evidence, 'before_kill': before, 'after_restart': after, 'restart_at_unix_nano': restart_ns, 'post_restart_span': name, 'model_calls': 1}


def run_windows(h, backend, verify_run, query_trace):
    events, results = [], {}
    # T06: Admission INSERT can commit; the first Outbox claim UPDATE is denied.
    text, conversation = marker('T06')
    with revoke(h, [('gateway_runtime', 'gateway.gateway_outbox', 'UPDATE')], events):
        run = h.send_text(text, conversation)
        outbox = h.sql("SELECT o.traceparent,(o.published_at IS NOT NULL)::text,o.attempts FROM gateway.gateway_outbox o JOIN gateway.gateway_admissions a ON a.admission_id=o.event_id WHERE a.run_id=" + h.quote(run))
        assert len(outbox) == 1 and outbox[0][1:] == ['false', '0']
        before = snapshot(h, run)
        assert before['worker'] == [] and model_calls(h, text) == []
        saved_span(query_trace, backend, outbox[0][0], 'create execution.run-requested.v1')
        pid = h.gateway.process.pid
        h.gateway.stop(kill=True)
        assert h.gateway.process.returncode == -9
    restart = time.time_ns()
    h.gateway.start()
    results['T06'] = assert_post_restart(h, backend, verify_run, run, text, before, restart, 'admission')
    results['T06'].update(killed_pid=pid, gateway_outbox_before=outbox)

    # T07: intake commits and broker confirms ACK, but the first Attempt INSERT
    # cannot commit. This proves zero durable Claims and zero model calls.
    text, conversation = marker('T07')
    preceding = stream_info(h)['state']['last_seq']
    with revoke(h, [('worker_runtime', 'worker.execution_attempts', 'INSERT')], events):
        run = h.send_text(text, conversation)
        h.wait(lambda: h.sql("SELECT count(*) FROM worker.execution_receipts WHERE run_id=" + h.quote(run)) == [['1']], 'Run intake committed')
        h.wait(lambda: consumer_info(h)['ack_floor']['stream_seq'] >= preceding + 1, 'real broker ACK floor advances before Claim')
        before = snapshot(h, run)
        assert len(before['worker']) == 1 and before['attempts'] == [] and model_calls(h, text) == []
        saved_span(query_trace, backend, before['worker'][0][1], 'process execution.run-requested.v1')
        pid = h.worker.pid
        h.stop_worker(kill=True)
        assert h.worker.returncode == -9
    restart = time.time_ns()
    h.start_worker()
    results['T07'] = assert_post_restart(h, backend, verify_run, run, text, before, restart, 'claim')
    results['T07'].update(killed_pid=pid, preceding_broker_sequence=preceding, ack_confirmed_before_kill=True)

    # T10: Completion and Reply INSERT commit; Relay SELECT alone is denied.
    text, conversation = marker('T10')
    with revoke(h, [('worker_runtime', 'worker.execution_reply_outbox', 'SELECT')], events):
        run = h.send_text(text, conversation)
        h.wait(lambda: h.sql("SELECT status FROM worker.execution_completions WHERE run_id=" + h.quote(run)) == [['SUCCEEDED']], 'Completion committed before Reply publish')
        before = snapshot(h, run)
        assert len(before['reply']) == 1 and before['reply'][0][4] == 'false' and before['delivery'] == []
        assert len(model_calls(h, text)) == 1
        saved_span(query_trace, backend, before['reply'][0][3], 'create execution.reply-intent.v1')
        pid = h.worker.pid
        h.stop_worker(kill=True)
        assert h.worker.returncode == -9
    restart = time.time_ns()
    h.start_worker()
    results['T10'] = assert_post_restart(h, backend, verify_run, run, text, before, restart, 'reply')
    results['T10']['killed_pid'] = pid

    # T11: Delivery Intent+parts commit, second receipt INSERT fails. Blocking
    # part UPDATE independently prevents any send before the Gateway restart.
    text, conversation = marker('T11')
    with revoke(h, [('gateway_runtime', 'gateway.gateway_reply_transport_receipts', 'INSERT'), ('gateway_runtime', 'gateway.gateway_delivery_parts', 'UPDATE')], events):
        run = h.send_text(text, conversation)
        h.wait(lambda: h.sql("SELECT count(*) FROM gateway.gateway_delivery_intents WHERE run_id=" + h.quote(run)) == [['1']], 'Delivery first transaction committed')
        before = snapshot(h, run)
        assert len(before['delivery']) == 1 and before['receipt'] == [] and before['parts'] == [['PENDING']]
        saved_span(query_trace, backend, before['delivery'][0][1], 'process execution.reply-intent.v1')
        pid = h.gateway.process.pid
        h.gateway.stop(kill=True)
        assert h.gateway.process.returncode == -9
    restart = time.time_ns()
    h.gateway.start()
    results['T11'] = assert_post_restart(h, backend, verify_run, run, text, before, restart, 'delivery')
    h.wait(lambda: h.sql("SELECT outcome FROM gateway.gateway_reply_transport_receipts WHERE run_id=" + h.quote(run)) == [['ACCEPTED']], 'replayed second transaction committed')
    results['T11']['killed_pid'] = pid
    result = {'result': 'PASS', 'fault': 'fixture-only privilege revocation; SQL facts committed before SIGKILL', 'business_row_writes': False, 'events': events, 'windows': results}
    (h.artifacts / 'tracing-recovery-windows.json').write_text(json.dumps(result, indent=2) + '\n')
    print('TRACING_WINDOWS=PASS T06=true T07=true T10=true T11=true same_durable_carrier=true one_model_call_each=true', flush=True)
    return result
