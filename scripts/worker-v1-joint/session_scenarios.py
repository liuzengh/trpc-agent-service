"""Real Session acceptance cases composed by the single joint-process runner.

Only the public Gateway path creates Runs. SDK traffic is real HTTP/SSE and all
business-table access here is read-only. The external model fixture must expose
hold_partial(text, poison) and wait_partial(text, timeout=30); the latter reports
only after a nonterminal SSE delta has actually been written and flushed.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import time
import uuid

from faults import (assert_history, assert_pair_and_queue, attempts, candidates,
                    completions, head, kill_holder, model_calls, outboxes,
                    parse_time, run, submit, wait_success)


def require_partial_receipt(record):
    assert isinstance(record, dict), 'model fixture returned no partial-SSE receipt'
    assert record.get('phase') == 'partial_sse_flushed', 'request admission is not partial SSE'
    assert type(record.get('bytes')) is int and record['bytes'] > 0, 'no partial SSE bytes flushed'
    assert record.get('done') is False, 'complete response is not an interrupted partial response'
    return record


def partial_sse_sigkill(h, journal):
    if not callable(getattr(h.model, 'hold_partial', None)) or not callable(getattr(h.model, 'wait_partial', None)):
        raise AssertionError('Session acceptance requires real partial-SSE fixture hooks')
    workers = h.start_fault_workers(max_attempts=3)
    marker = 'session-partial-' + uuid.uuid4().hex[:12]
    conversation = str(1300000000 + int(uuid.uuid4().hex[:7], 16))
    seed, victim, follower = marker + '-seed', marker + '-victim', marker + '-follower'
    poison = marker + '-UNACCEPTED-PARTIAL'
    try:
        seed_run = submit(h, seed, conversation)
        seed_result = wait_success(h, seed_run)
        seed_delivery = h.wait_delivery(seed_run)
        seed_answer = seed_result['final']['payload']['content']['text']
        accepted_before = head(h, run(h, seed_run))
        h.model.hold_partial(victim, poison)
        h.model.hold(follower)
        victim_run = submit(h, victim, conversation)
        partial = require_partial_receipt(h.model.wait_partial(victim, timeout=30))
        journal.append({'event': 'partial_sse_flushed', 'run_id': victim_run, 'receipt': partial})
        # Leave the live SDK stream blocked across scheduler ticks after flush,
        # rather than killing merely when the HTTP request reaches the fixture.
        time.sleep(.2)
        assert len(model_calls(h, victim)) == 1
        follower_run = submit(h, follower, conversation)
        holder = assert_pair_and_queue(h, workers, victim_run, follower_run)
        assert candidates(h, victim_run) == [] and completions(h, victim_run) == []
        observed_head = head(h, run(h, victim_run))
        assert observed_head['accepted_ref'] == accepted_before['accepted_ref']
        assert observed_head['accepted_digest'] == accepted_before['accepted_digest']
        killed = kill_holder(h, workers, victim_run, holder)
        journal.append({'event': 'os_sigkill_after_partial', **killed})
        print('WORKER_SESSION_PARTIAL_SIGKILL=' + json.dumps(killed, sort_keys=True), flush=True)
        h.model.release(victim)
        # The queued follower remains held, giving a stable observation point
        # after predecessor acceptance and before a second head advancement.
        h.model.wait_entered(follower)
        victim_result = wait_success(h, victim_run)
        recovered = attempts(h, victim_run)
        assert len(recovered) == 2
        old, new = recovered
        assert old['attempt_id'] == killed['attempt_id'] and old['status'] == 'ABORTED'
        assert new['worker_id'] == killed['survivor_worker_id'] and new['status'] == 'SUCCEEDED'
        assert new['lease_epoch'] == old['lease_epoch'] + 1 and new['generation'] == old['generation'] + 1
        assert parse_time(new['created_at']) >= parse_time(killed['lease_until_at_process_exit'])
        assert victim_result['candidate']['parent_ref'] == accepted_before['accepted_ref']
        assert victim_result['candidate']['parent_digest'] == accepted_before['accepted_digest']
        assert poison not in json.dumps(victim_result['candidate']['content'])
        retry_calls = model_calls(h, victim)
        assert len(retry_calls) == 2
        assert_history(retry_calls[1], seed_answer, victim, poison=poison)
        follower_calls = model_calls(h, follower)
        assert len(follower_calls) == 1
        assert_history(follower_calls[0], seed_answer, follower, poison=poison)
        victim_delivery = h.wait_delivery(victim_run)
        assert poison not in victim_delivery['final_text']
        accepted_retry = head(h, run(h, victim_run))
        assert accepted_retry['accepted_ref'] == victim_result['candidate']['candidate_ref']
        h.model.release(follower)
        follower_result = wait_success(h, follower_run)
        follower_delivery = h.wait_delivery(follower_run)
        assert poison not in json.dumps(follower_result['candidate']['content'])
        assert follower_result['candidate']['parent_ref'] == victim_result['candidate']['candidate_ref']
        return {'scenario': 'partial-sse-sigkill-history', 'invariants': ['WV-16', 'WV-23'],
                'blocked_model_phase': 'after_partial_sse', 'partial_receipt': partial,
                'kill': killed, 'attempts': recovered, 'seed_run_id': seed_run,
                'victim_run_id': victim_run, 'follower_run_id': follower_run,
                'seed_delivery': seed_delivery, 'victim_delivery': victim_delivery,
                'follower_delivery': follower_delivery, 'accepted_head_before': accepted_before,
                'accepted_retry_head': accepted_retry, 'completion': victim_result['completion'],
                'final_count': len(outboxes(h, victim_run)),
                'candidate_count': len(candidates(h, victim_run)), 'result': 'PASS'}
    finally:
        h.model.release(victim)
        h.model.release(follower)
        h.stop_fault_workers()



def _session_admin(h, statement):
    # This path changes only the login capability of a role in the harness-owned
    # disposable container. All Run/Attempt/Session/Completion rows stay untouched.
    return h.command(['docker', 'exec', h.pg, 'psql', '-X', '-A', '-t',
                      '-v', 'ON_ERROR_STOP=1', '-U', 'platform_admin',
                      '-d', 'agent_platform', '-c', statement])


@contextmanager
def session_login_outage(h, journal):
    assert h.pg == h.prefix + '-pg' and h.pg in h.containers, 'Session outage requires the dedicated harness container'
    assert h.sql("SELECT rolcanlogin FROM pg_roles WHERE rolname='session_runtime'") == [['t']]
    try:
        _session_admin(h, 'ALTER ROLE session_runtime NOLOGIN')
        # Commit NOLOGIN before terminating connections so no new runtime pool
        # can replace a terminated connection during the outage window.
        _session_admin(h, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='session_runtime' AND pid<>pg_backend_pid()")
        assert h.sql("SELECT rolcanlogin FROM pg_roles WHERE rolname='session_runtime'") == [['f']]
        journal.append({'event': 'session_login_unavailable', 'role': 'session_runtime',
                        'business_rows_modified': False})
        yield
    finally:
        # Restore even after an uncertain administration response or test failure.
        _session_admin(h, 'ALTER ROLE session_runtime LOGIN')
        assert h.sql("SELECT rolcanlogin FROM pg_roles WHERE rolname='session_runtime'") == [['t']]
        journal.append({'event': 'session_login_restored', 'role': 'session_runtime'})


def accepted_store_outage(h, journal):
    h.stop_fault_workers()
    worker = h.start_worker('worker-one', {'policy': {'max_attempts': 8, 'retry_backoff': '1s'}})
    marker = 'session-outage-' + uuid.uuid4().hex[:12]
    conversation = str(1600000000 + int(uuid.uuid4().hex[:7], 16))
    seed, next_text = marker + '-accepted', marker + '-next'
    try:
        seed_run = submit(h, seed, conversation)
        seed_result = wait_success(h, seed_run)
        seed_delivery = h.wait_delivery(seed_run)
        seed_answer = seed_result['final']['payload']['content']['text']
        accepted_before = head(h, run(h, seed_run))
        before_completion = completions(h, seed_run)
        before_attempts = attempts(h, seed_run)
        before_final = outboxes(h, seed_run)
        h.model.hold(next_text)
        with session_login_outage(h, journal):
            # Exercise the actual password/TCP login while only Session is down.
            probe = h.command(['docker', 'exec', '-e', 'PGPASSWORD', h.pg, 'sh', '-c',
                               "if psql -X -h 127.0.0.1 -U session_runtime -d agent_platform -c 'SELECT 1' >/dev/null 2>&1; then printf CONNECTED; else printf REJECTED; fi"],
                              env={'PGPASSWORD': h.env['SESSION_RUNTIME_PASSWORD']}).strip()
            assert probe == 'REJECTED', 'runtime Session login was not interrupted'
            assert head(h, run(h, seed_run)) == accepted_before
            assert completions(h, seed_run) == before_completion
            assert outboxes(h, seed_run) == before_final
            # A completed receipt remains authoritative while its Session store
            # is unreadable; replay is byte-identical and does not rerun the model.
            replay = h.gateway.restart_and_replay(seed_run)
            assert attempts(h, seed_run) == before_attempts
            assert len(model_calls(h, seed)) == 1
            assert replay['external_send_count_unchanged'] and replay['delivery_intent_count'] == 1
            next_run = submit(h, next_text, conversation)
            failed = h.wait(lambda: next((a for a in attempts(h, next_run)
                                         if a['status'] == 'FAILED' and a['reason'] == 'DEPENDENCY_UNAVAILABLE'), None),
                            'Session unavailable produces a retryable preparation fact', timeout=15)
            assert model_calls(h, next_text) == [], 'model ran without the accepted Session'
            assert completions(h, next_run) == [] and candidates(h, next_run) == []
            assert outboxes(h, next_run) == []
            during = head(h, run(h, next_run))
            assert during['accepted_ref'] == accepted_before['accepted_ref']
            assert during['accepted_digest'] == accepted_before['accepted_digest']
            assert during['settled_sequence'] == accepted_before['settled_sequence']
            journal.append({'event': 'session_prepare_failed_without_model', 'run_id': next_run,
                            'attempt_id': failed['attempt_id'], 'reason': failed['reason']})
        # Role LOGIN is now restored. Only a fresh Attempt may initialize another
        # batch; it must load the exact formal predecessor instead of empty history.
        h.model.wait_entered(next_text)
        retry_calls = model_calls(h, next_text)
        assert len(retry_calls) == 1
        assert_history(retry_calls[0], seed_answer, next_text)
        h.model.release(next_text)
        next_result = wait_success(h, next_run)
        next_delivery = h.wait_delivery(next_run)
        recovered = attempts(h, next_run)
        assert len(recovered) >= 2 and recovered[0]['attempt_id'] == failed['attempt_id']
        assert recovered[-1]['status'] == 'SUCCEEDED'
        assert next_result['candidate']['parent_ref'] == accepted_before['accepted_ref']
        assert next_result['candidate']['parent_digest'] == accepted_before['accepted_digest']
        assert completions(h, seed_run) == before_completion and attempts(h, seed_run) == before_attempts
        assert outboxes(h, seed_run) == before_final and len(model_calls(h, seed)) == 1
        return {'scenario': 'accepted-store-unreadable-then-recovered',
                'invariants': ['WV-18', 'WV-23'], 'worker_pid': worker.pid,
                'seed_run_id': seed_run, 'next_run_id': next_run, 'seed_delivery': seed_delivery,
                'replayed_final': replay, 'store_login_probe': probe,
                'accepted_head_before': accepted_before, 'accepted_head_during_outage': during,
                'completed_run_completion_unchanged': True, 'completed_run_model_calls': 1,
                'new_run_model_calls_during_outage': 0, 'new_run_attempts': recovered,
                'new_run_completion': next_result['completion'], 'new_run_delivery': next_delivery,
                'business_rows_modified_for_fault': False, 'session_login_restored': True,
                'result': 'PASS'}
    finally:
        h.model.release(next_text)
        h.stop_fault_workers()

def run_session(h):
    evidence = {'version': 'worker-v1-session-process/v1', 'scenarios': [], 'events': []}
    path = Path(h.artifacts) / 'worker-session-scenarios.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        evidence['scenarios'].append(partial_sse_sigkill(h, evidence['events']))
        evidence['scenarios'].append(accepted_store_outage(h, evidence['events']))
        evidence['result'] = 'PASS'
        print('WORKER_SESSION_PARTIAL=PASS partial SSE flushed; real holder SIGKILL; clean accepted retry and follower', flush=True)
        print('WORKER_SESSION_STORE_OUTAGE=PASS accepted facts unchanged; old Final replayed; fresh Attempt recovers formal history after LOGIN restoration', flush=True)
        return evidence
    except BaseException:
        evidence['result'] = 'FAIL'
        raise
    finally:
        path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
        if json.loads(path.read_text()) != evidence:
            raise AssertionError('Session evidence readback mismatch')
