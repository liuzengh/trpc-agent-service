"""T09: candidate Stage failures are distinct from accepted Session commits."""
import json
import time
import uuid
from commit_scenarios import run_commit
from faults import candidates, completions, head, model_calls, outboxes, run, submit
from tracing_delivery import attributes, read_spans
from tracing_windows import revoke


def selected(spans, name, outcome=None):
    return [s for s in spans if s['name'] == name and (outcome is None or attributes(s).get('app.outcome') == outcome)]


def no_accepted_commit(spans):
    assert not selected(spans, 'worker.session.commit', 'accepted'), 'candidate or failed transaction reported accepted'


def final_trace(h, backend, verify_run, run_id, failed_name=None, failed_outcome=None):
    end = time.monotonic() + 35
    while True:
        try:
            trace = verify_run(h, backend, run_id)
            spans = read_spans(h, run_id)
            commits = selected(spans, 'worker.session.commit', 'accepted')
            assert len(commits) == 1
            assert selected(spans, 'worker.session.stage', 'candidate')
            if failed_name:
                assert selected(spans, failed_name, failed_outcome)
            assert all(attributes(s).get('app.outcome') != 'accepted' for s in selected(spans, 'worker.session.stage'))
            return trace, spans
        except AssertionError:
            if time.monotonic() >= end:
                raise
            time.sleep(.2)


def denied_write(h, backend, verify_run, query, kind):
    h.stop_fault_workers()
    h.start_worker('worker-one', {'policy': {'max_attempts': 8, 'retry_backoff': '2s'}})
    marker = 'TRACE_BODY_CANARY session-' + kind + '-' + uuid.uuid4().hex[:8]
    conversation = str(7100000000 + int(uuid.uuid4().hex[:7], 16))
    seed = submit(h, marker + '-seed', conversation)
    h.wait_delivery(seed)
    before = head(h, run(h, seed))
    text = marker + '-victim'
    h.model.hold(text)
    victim = submit(h, text, conversation)
    h.model.wait_entered(text)
    carrier = h.sql('SELECT traceparent FROM worker.execution_runs WHERE run_id=' + h.quote(victim))[0][0]
    trace_id = carrier.split('-')[1]
    grant = ('session_runtime', 'runtime_session.session_candidates', 'INSERT') if kind == 'stage' else ('worker_runtime', 'worker.execution_session_commits', 'INSERT')
    name, outcome = ('worker.session.stage', 'UNKNOWN') if kind == 'stage' else ('worker.session.commit', 'failed')
    events = []
    try:
        with revoke(h, [grant], events):
            h.model.release(text)
            # Wait for a real failed operation, not merely for request admission.
            end = time.monotonic() + 20
            while True:
                document, spans = query(backend, trace_id, {name})
                if selected(spans, name, outcome):
                    break
                assert time.monotonic() < end, 'no failed Session operation exported'
                time.sleep(.2)
            no_accepted_commit(spans)
            observed = head(h, run(h, victim))
            assert (observed['accepted_ref'], observed['accepted_digest']) == (before['accepted_ref'], before['accepted_digest'])
            assert completions(h, victim) == [] and outboxes(h, victim) == []
            staged = candidates(h, victim)
            assert (not staged) if kind == 'stage' else bool(staged)
            assert h.sql('SELECT count(*) FROM worker.execution_session_commits WHERE run_id=' + h.quote(victim)) == [['0']]
            (h.artifacts / (victim + '-before-restoration-trace.json')).write_text(json.dumps(document, indent=2) + '\n')
        h.wait_delivery(victim)
        trace, spans = final_trace(h, backend, verify_run, victim, name, outcome)
        accepted = head(h, run(h, victim))
        completed = completions(h, victim)
        assert len(completed) == 1 and accepted['accepted_ref'] == completed[0]['candidate_ref']
        assert accepted['accepted_digest'] == completed[0]['candidate_digest']
        assert h.sql('SELECT count(*) FROM worker.execution_session_commits WHERE run_id=' + h.quote(victim)) == [['1']]
        assert h.sql('SELECT traceparent FROM worker.execution_runs WHERE run_id=' + h.quote(victim)) == [[carrier]]
        follower_text = marker + '-follower'
        follower = submit(h, follower_text, conversation)
        h.wait_delivery(follower)
        assert text in json.dumps(model_calls(h, follower_text)[0]), 'accepted victim missing from follower Session'
        return {'kind': kind, 'run_id': victim, 'before_head': before, 'failure_head': observed, 'candidates_before_restoration': len(staged), 'accepted_head': accepted, 'completion': completed, 'model_calls': len(model_calls(h, text)), 'trace': trace, 'events': events, 'follower_run_id': follower}
    finally:
        h.model.release(text)


def run_traced_session(h, backend, verify_run, query):
    results = {}
    results['stage_denied'] = denied_write(h, backend, verify_run, query, 'stage')
    results['complete_denied'] = denied_write(h, backend, verify_run, query, 'complete')
    recovered = run_commit(h)
    victim = recovered['victim_run_id']
    trace, spans = final_trace(h, backend, verify_run, victim)
    stage = selected(spans, 'worker.session.stage', 'candidate')
    verify = selected(spans, 'worker.session.verify')
    assert len(stage) == 1 and len(verify) == 1 and verify[0]['parentSpanId'] == stage[0]['spanId']
    assert recovered['sdk_call_count'] == 1 and recovered['candidate_count'] == recovered['completion_count'] == 1
    results['candidate_response_loss'] = {'business': recovered, 'trace': trace, 'exact_readback_child': True}
    result = {'result': 'PASS', 'scenarios': results, 'business_row_writes': False, 'accepted_is_not_candidate': True}
    (h.artifacts / 'tracing-session-matrix.json').write_text(json.dumps(result, indent=2) + '\n')
    print('TRACING_SESSION=PASS Stage_failure_unaccepted=true Complete_rollback_unaccepted=true candidate_response_loss_readback=true accepted_commit_exactly_once=true follower_history=true', flush=True)
    return result
