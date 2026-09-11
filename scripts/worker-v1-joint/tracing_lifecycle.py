"""T13: real SIGTERM cancellation, live-process fencing and stream deadline."""
import json
import signal
import time
import uuid
from deadline_scenarios import pair, policy, restore_standard, seed, terminal_snapshot, fresh_follower
from faults import attempts, candidates, completions, model_calls, outboxes, run, submit
from session_scenarios import require_partial_receipt
from tracing_delivery import attributes, read_spans
from tracing_retry import carrier, completed_trace


def ended_attempt(spans, attempt_id):
    found = [s for s in spans if s['name'] == 'worker.run.attempt' and attributes(s).get('app.attempt.id') == attempt_id]
    assert len(found) == 1
    assert attributes(found[0]).get('app.outcome') in ('cancelled', 'deadline', 'fenced'), 'stale/cancelled Attempt reported success'
    assert int(found[0]['endTimeUnixNano']) >= int(found[0]['startTimeUnixNano'])
    return found[0]


def wait_ended(h, backend, query, run_id, attempt_id):
    trace_id = carrier(h, run_id).split('-')[1]
    end = time.monotonic() + 35
    while True:
        document, spans = query(backend, trace_id, {'worker.run.attempt'})
        try:
            found = ended_attempt(spans, attempt_id)
            return document, spans, found
        except AssertionError:
            if time.monotonic() >= end:
                raise
            time.sleep(.2)


def interrupted_stream(h, backend, verify_run, query, mode):
    h.stop_fault_workers()
    first = h.start_worker('worker-one', {'policy': {'max_attempts': 3}})
    marker = 'TRACE_BODY_CANARY lifecycle-' + mode + '-' + uuid.uuid4().hex[:8]
    conversation = str(8100000000 + int(uuid.uuid4().hex[:7], 16))
    accepted = seed(h, marker + '-seed', conversation)
    text, poison = marker + '-victim', marker + '-UNACCEPTED-PARTIAL'
    h.model.hold_partial(text, poison)
    victim = submit(h, text, conversation)
    partial = require_partial_receipt(h.model.wait_partial(text))
    original = attempts(h, victim)[0]
    first_carrier = carrier(h, victim)
    assert original['worker_id'] == 'worker-one' and original['status'] == 'EXECUTING'
    assert not candidates(h, victim) and not completions(h, victim)
    suspended = False
    try:
        if mode == 'shutdown':
            start = time.monotonic()
            h.stop_worker()
            elapsed = time.monotonic() - start
            assert first.returncode == 0 and elapsed < 20
            _, _, ended = wait_ended(h, backend, query, victim, original['attempt_id'])
            assert candidates(h, victim) == [] and completions(h, victim) == []
            h.start_worker('worker-one', {'policy': {'max_attempts': 3}})
        else:
            # A living but stopped OS process cannot renew. No DB timestamp or
            # lease is rewritten. Its original TLS/SDK connection remains real.
            assert first.poll() is None
            h.proof_switch.remove('worker-one')
            disconnected_proof_sockets = h.proof_switch.disconnect()
            first.send_signal(signal.SIGSTOP)
            suspended = True
            survivor = h.start_worker('worker-two', {'policy': {'max_attempts': 3}})
            h.wait(lambda: len(model_calls(h, text)) == 2, 'survivor enters SDK after original lease expires', timeout=15)
            renewed = attempts(h, victim)
            assert len(renewed) == 2 and renewed[0]['status'] == 'ABORTED' and renewed[0]['reason'] == 'LEASE_EXPIRED'
            assert renewed[1]['worker_id'] == 'worker-two' and renewed[1]['generation'] == original['generation'] + 1
            assert renewed[1]['lease_epoch'] == original['lease_epoch'] + 1
            assert first.poll() is None and survivor.poll() is None
            first.send_signal(signal.SIGCONT)
            suspended = False
            _, _, ended = wait_ended(h, backend, query, victim, original['attempt_id'])
            h.proof_switch.add('worker-one', '127.0.0.1', h.worker_ports['worker-one']['proof'])
            assert candidates(h, victim) == [] and completions(h, victim) == []
            elapsed = None
        h.model.release(text)
        h.wait_delivery(victim)
        actual = attempts(h, victim)
        assert len(actual) == 2 and actual[-1]['status'] == 'SUCCEEDED'
        assert len(model_calls(h, text)) == 2
        assert len(candidates(h, victim)) == len(completions(h, victim)) == len(outboxes(h, victim)) == 1
        assert candidates(h, victim)[0]['attempt_id'] != original['attempt_id']
        assert poison not in json.dumps(candidates(h, victim))
        trace, spans = completed_trace(h, backend, verify_run, victim, lambda ss: ended_attempt(ss, original['attempt_id']))
        assert carrier(h, victim) == first_carrier
        follower_text = marker + '-follower'
        follower = submit(h, follower_text, conversation)
        h.wait_delivery(follower)
        request = json.dumps(model_calls(h, follower_text)[0])
        assert text in request and poison not in request
        return {'mode': mode, 'run_id': victim, 'partial_receipt': partial, 'original_pid': first.pid, 'shutdown_exit': first.returncode if mode == 'shutdown' else None, 'shutdown_seconds': elapsed, 'disconnected_proof_sockets': disconnected_proof_sockets if mode != 'shutdown' else 0, 'attempts': actual, 'original_attempt_span': ended, 'trace': trace, 'model_calls': 2, 'follower_run_id': follower}
    finally:
        if suspended and first.poll() is None:
            first.send_signal(signal.SIGCONT)
        h.model.release(text)
        restore_standard(h)


def stream_deadline(h, backend, query, identifier):
    pair(h, policy('12s'))
    marker = 'TRACE_BODY_CANARY stream-deadline-' + uuid.uuid4().hex[:8]
    conversation = str(8200000000 + int(uuid.uuid4().hex[:7], 16))
    accepted = seed(h, marker + '-seed', conversation)
    text = marker + '-victim'
    h.model.hold_partial(text, marker + '-UNACCEPTED')
    try:
        victim = submit(h, text, conversation)
        partial = require_partial_receipt(h.model.wait_partial(text))
        original = attempts(h, victim)[0]
        first_carrier = carrier(h, victim)
        terminal = terminal_snapshot(h, victim, accepted['head'])
        assert len(model_calls(h, text)) == 1 and len(terminal['attempts']) == 1
        document, spans, ended = wait_ended(h, backend, query, victim, original['attempt_id'])
        end = time.monotonic() + 35
        while True:
            document, spans = query(backend, first_carrier.split('-')[1], {'worker.run.terminalize', 'worker.runner.run'})
            ids = {identifier(s['spanId']) for s in spans}
            missing = [s for s in spans if s.get('parentSpanId') and identifier(s['parentSpanId']) != '0000000000000000' and identifier(s['parentSpanId']) not in ids]
            if not missing:
                break
            assert time.monotonic() < end, 'deadline Trace missing actual causal parents'
            time.sleep(.2)
        assert all(identifier(s['traceId']) == first_carrier.split('-')[1] for s in spans)
        assert not any(s['name'] in ('worker.session.stage', 'worker.session.commit', 'create execution.reply-intent.v1', 'gateway.im.send') for s in spans)
        assert len([s for s in spans if s['name'] == 'worker.run.terminalize']) == 1
        serialized = json.dumps(document)
        assert 'TRACE_BODY_CANARY' not in serialized and all(secret not in serialized for secret in h.secrets)
        assert carrier(h, victim) == first_carrier
        (h.artifacts / (victim + '-trace.json')).write_text(json.dumps(document, indent=2) + '\n')
        h.model.release(text)
        follower = fresh_follower(h, marker + '-follower', conversation, [marker + '-seed'], accepted['head'])
        return {'run_id': victim, 'partial_receipt': partial, 'terminal': terminal, 'trace_id': first_carrier.split('-')[1], 'span_count': len(spans), 'original_attempt_span': ended, 'follower': follower, 'reply': 'NONE'}
    finally:
        h.model.release(text)
        restore_standard(h)


def run_traced_lifecycle(h, backend, verify_run, query, identifier):
    results = {}
    results['shutdown'] = interrupted_stream(h, backend, verify_run, query, 'shutdown')
    results['live_fence'] = interrupted_stream(h, backend, verify_run, query, 'live_fence')
    results['deadline'] = stream_deadline(h, backend, query, identifier)
    result = {'result': 'PASS', 'scenarios': results, 'business_row_writes': False, 'manifest_changes': False}
    (h.artifacts / 'tracing-lifecycle-matrix.json').write_text(json.dumps(result, indent=2) + '\n')
    print('TRACING_LIFECYCLE=PASS graceful_stream_cancel=true live_lease_fence=true stream_deadline=true stale_candidate_absent=true real_span_end=true deadline_reply_NONE=true', flush=True)
    return result
