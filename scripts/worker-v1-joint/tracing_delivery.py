"""T12: actual SDK sends, durable delivery facts, and exported Span identities."""
import json
import time
import uuid
from unittest.mock import patch

from delivery_scenarios import run_delivery, delivery_snapshot
from faults import model_calls, wait_success


def attributes(span):
    return {a['key']: next(iter(a['value'].values())) for a in span.get('attributes', [])}


def read_spans(h, run):
    document = json.loads((h.artifacts / (run + '-trace.json')).read_text())
    def walk(v):
        if isinstance(v, dict):
            if 'spanId' in v and 'name' in v:
                yield v
            for child in v.values():
                yield from walk(child)
        elif isinstance(v, list):
            for child in v:
                yield from walk(child)
    return list(walk(document))


def check_sends(spans, outcomes):
    sends = [s for s in spans if s['name'] == 'gateway.im.send']
    assert sorted(attributes(s).get('app.outcome') for s in sends) == sorted(outcomes)
    assert len({s['spanId'] for s in sends}) == len(sends)
    delivers = {s['spanId']: s for s in spans if s['name'] == 'gateway.reply.deliver'}
    for send in sends:
        assert send['parentSpanId'] in delivers, 'send is not a child of its actual part dispatch'
        assert attributes(send).get('app.attempt.id'), 'send has no actual Attempt identity'
        assert int(attributes(send)['app.retry.number']) >= 0
    return sends, delivers


def verify_delivery_trace(h, backend, verify_run, run, outcomes, preparation=False):
    # Tempo may return HTTP 200 before the last batch/part has arrived. Keep
    # the complete count/parent predicates; do not accept the first partial 200.
    deadline = time.monotonic() + 35
    while True:
        try:
            trace = verify_run(h, backend, run)
            spans = read_spans(h, run)
            sends, delivers = check_sends(spans, outcomes)
            if preparation:
                assert any(attributes(s).get('app.outcome') == 'NOT_SENT' for s in delivers.values())
            return trace, sends, delivers
        except AssertionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.2)


def run_traced_delivery(h, backend, verify_run):
    results = {}
    # Existing uncertainty gate asserts durable UNKNOWN, restart/replay with
    # proof offline, exactly one external acceptance, and continuing Session.
    unknown = run_delivery(h)
    run = unknown['run_id']
    trace, _, _ = verify_delivery_trace(h, backend, verify_run, run, ['UNKNOWN'])
    results['unknown'] = {'business': unknown, 'trace': trace}

    # Multi-part text is emitted by the external model fixture through actual SSE.
    text = 'TRACE_BODY_CANARY long-final-' + uuid.uuid4().hex[:8]
    final = '分片🙂' * 2200
    conversation = str(6000000000 + int(uuid.uuid4().hex[:7], 16))
    original = h.model.final
    def model_final(value, *, model='joint-fixture'):
        return h.model.delta(final, model=model) + h.model.delta('', 'stop', model=model) + 'data: [DONE]\n\n' if value == text else original(value, model=model)
    with patch.object(h.model, 'final', model_final):
        run = h.send_text(text, conversation)
        delivery = h.wait_delivery(run)
    state = delivery_snapshot(h, run)
    count = len(state['parts'])
    assert count > 1 and delivery['final_text'] == final
    trace, sends, delivers = verify_delivery_trace(h, backend, verify_run, run, ['ACCEPTED'] * count)
    indices = [int(attributes(delivers[s['parentSpanId']])['app.part.index']) for s in sorted(sends, key=lambda s: int(s['startTimeUnixNano']))]
    assert indices == list(range(count)), 'parts were not sent in order'
    assert all(int(attributes(s)['app.retry.number']) == 0 for s in sends)
    results['multipart'] = {'run_id': run, 'parts': count, 'part_order': indices, 'trace': trace, 'model_calls': len(model_calls(h, text))}

    # A real SDK getMe 429 occurs before SendFinal, proving NOT_SENT preparation.
    # The existing preparation budget retries; it must not invent a send Span.
    text = 'TRACE_BODY_CANARY prepare-retry-' + uuid.uuid4().hex[:8]
    conversation = str(int(conversation) + 1)
    h.gateway_fixture.reject_once('getMe')
    run = h.send_text(text, conversation)
    h.wait_delivery(run)
    trace, sends, delivers = verify_delivery_trace(h, backend, verify_run, run, ['ACCEPTED'], preparation=True)
    assert any(attributes(s).get('app.outcome') == 'NOT_SENT' for s in delivers.values())
    failed = {sid for sid, s in delivers.items() if attributes(s).get('app.outcome') == 'NOT_SENT'}
    assert all(s.get('parentSpanId') not in failed for s in sends)
    state = delivery_snapshot(h, run)
    assert len(state['delivery_attempts']) == 1 and len(model_calls(h, text)) == 1
    preparation = h.sql("SELECT p.preparation_attempts,p.preparation_result FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=" + h.quote(run))
    assert len(preparation) == 1 and preparation[0][0] == '1'
    assert json.loads(preparation[0][1])['Certainty'] == 'NOT_SENT'
    results['preparation_retry'] = {'run_id': run, 'state': state, 'trace': trace, 'send_spans': 1, 'not_sent_preparation_spans': len(failed), 'preparation': preparation}

    # A typed sendMessage 429 is REJECTED/rate_limited under the current contract,
    # not NOT_SENT. Preserve the existing terminal semantics rather than retry it.
    text = 'TRACE_BODY_CANARY rejected-send-' + uuid.uuid4().hex[:8]
    conversation = str(int(conversation) + 1)
    final = 'joint answer: ' + text
    h.gateway_fixture.reject_once('sendMessage', text=final, conversation_id=conversation)
    run = h.send_text(text, conversation)
    h.wait(lambda: h.sql("SELECT count(*) FROM worker.execution_runs WHERE run_id=" + h.quote(run)) == [["1"]], "Worker intake before terminal observation")
    wait_success(h, run)
    h.wait(lambda: h.sql("SELECT p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=" + h.quote(run)) == [['REJECTED']], 'typed 429 durably REJECTED')
    before = delivery_snapshot(h, run)
    assert before['parts'][0]['result']['Certainty'] == 'REJECTED'
    assert before['parts'][0]['result']['ErrorClass'] == 'rate_limited'
    trace, _, _ = verify_delivery_trace(h, backend, verify_run, run, ['REJECTED'])
    time.sleep(2)
    assert delivery_snapshot(h, run) == before
    assert not [m for m in h.gateway_fixture.snapshot() if m['chat_id'] == conversation]
    assert len(model_calls(h, text)) == 1
    results['send_429'] = {'run_id': run, 'state': before, 'trace': trace, 'external_acceptances': 0, 'no_retry': True}
    rejections = h.gateway_fixture.rejection_snapshot()
    assert len(rejections) == 2 and {x['method'] for x in rejections} == {'getMe', 'sendMessage'}
    result = {'result': 'PASS', 'scenarios': results, 'typed_rejections': rejections, 'production_policy_changed': False}
    (h.artifacts / 'tracing-delivery-matrix.json').write_text(json.dumps(result, indent=2) + '\n')
    print('TRACING_DELIVERY=PASS multipart_order=true preparation_NOT_SENT_retry=true send_429_REJECTED=true UNKNOWN_no_retry=true actual_span_parentage=true', flush=True)
    return result
