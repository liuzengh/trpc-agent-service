"""T08: same broker sequence redelivery and original Worker retry policy."""
import json
import time
import uuid
from faults import attempts, candidates, completions, model_calls, outboxes, submit
from intake_scenarios import after_commit_ack_failure
from tracing_delivery import attributes, read_spans


def unique_spans(spans, name, minimum=1):
    found = [s for s in spans if s['name'] == name]
    assert len(found) >= minimum and len({s['spanId'] for s in found}) == len(found), 'missing or reused Span identity: ' + name
    return found


def carrier(h, run):
    return h.sql('SELECT traceparent FROM worker.execution_runs WHERE run_id=' + h.quote(run))[0][0]


def completed_trace(h, backend, verify_run, run, predicate):
    end = time.monotonic() + 35
    while True:
        try:
            result = verify_run(h, backend, run)
            spans = read_spans(h, run)
            predicate(spans)
            return result, spans
        except AssertionError:
            if time.monotonic() >= end:
                raise
            time.sleep(.2)


def run_traced_retry(h, backend, verify_run, query, identifier):
    saved, events = {}, []
    def before_crash(run):
        saved['carrier'] = carrier(h, run)
        _, trace_id, span_id, _ = saved['carrier'].split('-')
        end = time.monotonic() + 35
        while True:
            _, spans = query(backend, trace_id, {'process execution.run-requested.v1'})
            if any(identifier(s['spanId']) == span_id for s in spans):
                break
            assert time.monotonic() < end, 'first persisted process Span not exported before crash'
            time.sleep(.2)
    business = after_commit_ack_failure(h, events, before_crash=before_crash)
    run = business['run_id']
    trace, spans = completed_trace(h, backend, verify_run, run, lambda ss: unique_spans(ss, 'process execution.run-requested.v1', 2))
    consumers = unique_spans(spans, 'process execution.run-requested.v1', 2)
    assert len({s['parentSpanId'] for s in consumers}) == 1, 'redelivery lost original creation parent'
    assert carrier(h, run) == saved['carrier']
    assert all(attributes(s)['app.run.id'] == run for s in consumers)
    assert len(attempts(h, run)) == len(candidates(h, run)) == len(completions(h, run)) == len(outboxes(h, run)) == 1
    replay = h.gateway.verify_webhook_auth_and_replay(run)
    assert len(attempts(h, run)) == 1
    redelivery = {'business': business, 'trace': trace, 'first_carrier': saved['carrier'], 'consumer_span_ids': [s['spanId'] for s in consumers], 'events': events, 'same_webhook_replay': replay}

    h.stop_fault_workers()
    h.start_worker('worker-one', {'policy': {'max_attempts': 3, 'retry_backoff': '1s'}})
    text = 'TRACE_BODY_CANARY model-retry-' + uuid.uuid4().hex[:8]
    h.model.fail_once(text)
    run = submit(h, text, str(7600000000 + int(uuid.uuid4().hex[:7], 16)))
    first_carrier = carrier(h, run)
    h.wait_delivery(run)
    actual = attempts(h, run)
    (h.artifacts / 'model-retry-attempts.json').write_text(json.dumps(actual, indent=2) + '\n')
    assert len(actual) == 2 and actual[0]['status'] == 'FAILED' and actual[1]['status'] == 'SUCCEEDED'
    assert len(model_calls(h, text)) == 2
    assert len(candidates(h, run)) == len(completions(h, run)) == len(outboxes(h, run)) == 1
    def retry_spans(ss):
        found = unique_spans(ss, 'worker.run.attempt', 2)
        assert len(found) == 2 and {attributes(s)['app.attempt.id'] for s in found} == {a['attempt_id'] for a in actual}
        unique_spans(ss, 'worker.runner.run', 2)
        chats = [s for s in ss if s['name'].startswith('chat ')]
        assert len(chats) == 2 and len({s['spanId'] for s in chats}) == 2
        assert any(s.get('status', {}).get('code') in (2, 'STATUS_CODE_ERROR') for s in chats), 'failed model Span absent'
    retry_trace, spans = completed_trace(h, backend, verify_run, run, retry_spans)
    assert carrier(h, run) == first_carrier
    failures = h.model.failure_snapshot()
    assert failures == [{'text': text, 'status': 503, 'response': 'no SSE/candidate'}]
    model_retry = {'run_id': run, 'attempts': actual, 'model_calls': 2, 'trace': retry_trace, 'first_carrier': first_carrier, 'fault': failures}
    result = {'result': 'PASS', 'redelivery': redelivery, 'model_retry': model_retry, 'policy_changed': False, 'business_row_writes': False}
    (h.artifacts / 'tracing-retry-matrix.json').write_text(json.dumps(result, indent=2) + '\n')
    print('TRACING_RETRY=PASS broker_original_sequence=true distinct_consumer_spans=true immutable_carrier=true model_503_retry=true attempts=2 completion=1', flush=True)
    return result
