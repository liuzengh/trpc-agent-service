"""WV-17/23: real candidate commit, lost PG response, exact-identity readback.

Install the protocol relay after provision but BEFORE Control seed/publication.
Only the published Session target uses it. All other service connections and
read-only audit connections remain direct to the private PostgreSQL fixture.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
import uuid

from faults import (attempts, candidates, completions, head, model_calls,
                    outboxes, run, submit, wait_success)
from pg_fault_proxy import PGCommitFaultProxy
from scope_scenarios import assert_transcript


def install_proxy(h):
    assert h.pg == h.prefix + '-pg' and h.pg in h.containers, 'commit proxy requires the dedicated disposable PostgreSQL fixture'
    assert not hasattr(h, 'profile_id'), 'Session target must be fixed before real Control publication'
    assert not hasattr(h, 'session_fault_proxy'), 'Session proxy is installed once'
    original = urlsplit(h.dsns['session_runtime'])
    assert original.hostname == '127.0.0.1' and original.port == h.pg_port
    assert original.username == 'session_runtime' and original.query == 'sslmode=disable'
    proxy = PGCommitFaultProxy('127.0.0.1', h.pg_port).start()
    try:
        # Preserve credential bytes verbatim. Only this fixture-owned endpoint's
        # port changes, before the real Control API seals and publishes it.
        auth = original.netloc.rsplit('@', 1)[0]
        h.dsns['session_runtime'] = urlunsplit(original._replace(netloc=auth+'@127.0.0.1:'+str(proxy.port)))
        h.session_port = proxy.port
        h.session_fault_proxy = proxy
        return proxy
    except BaseException:
        proxy.close()
        raise


def require_commit_receipt(receipt, *, disconnected):
    assert isinstance(receipt, dict) and receipt.get('commit_observed') is True
    assert receipt.get('execute_observed') is True and receipt.get('sync_observed') is True
    assert receipt.get('command_tag') == 'INSERT 0 1'
    assert receipt.get('ready_status') == 'I' and receipt.get('error_response_observed') is False
    assert receipt.get('successful_response_bytes_forwarded') == 0
    assert type(receipt.get('suppressed_bytes')) is int and receipt['suppressed_bytes'] > 0
    assert receipt.get('suppressed_message_types', [])[-1:] == ['Z']
    assert receipt['server_ready']['monotonic_ns'] >= receipt['bind']['monotonic_ns']
    if disconnected:
        assert receipt.get('phase') == 'server_commit_observed_response_dropped'
        assert receipt.get('disconnect_released_by_test') is True
        assert receipt['client_disconnect']['monotonic_ns'] >= receipt['server_ready']['monotonic_ns']
    else:
        assert receipt.get('phase') == 'server_commit_observed_before_disconnect'
        assert 'client_disconnect' not in receipt
    return receipt


def operation_records(h, run_id):
    records = []
    for log in sorted(Path(h.artifacts).glob('worker-*.log')):
        for line in log.read_text(errors='replace').splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if value.get('msg') == 'worker.operation' and value.get('run_id') == run_id:
                records.append(value)
    return records


def run_commit(h):
    proxy = getattr(h, 'session_fault_proxy', None)
    assert proxy is not None and h.session_port == proxy.port, 'seed must publish the fixed Session proxy destination'
    evidence = {'version': 'worker-v1-session-commit-loss/v1', 'invariants': ['WV-17', 'WV-23'],
                'fault': 'successful candidate INSERT response withheld after real PostgreSQL idle ReadyForQuery',
                'business_state_writes_from_test': False, 'raw_protocol_payloads_recorded': False}
    path = Path(h.artifacts) / 'worker-session-commit-loss.json'
    marker = 'session-commit-' + uuid.uuid4().hex[:12]
    conversation = str(2300000000 + int(uuid.uuid4().hex[:7], 16))
    seed, victim, follower = marker+'-seed', marker+'-lost-response', marker+'-follower'
    try:
        h.stop_fault_workers()
        worker = h.start_worker('worker-one', {'policy': {'max_attempts': 3}})
        seed_run = submit(h, seed, conversation)
        seed_result = wait_success(h, seed_run)
        seed_delivery = h.wait_delivery(seed_run)
        before = head(h, run(h, seed_run))
        h.model.hold(victim)
        victim_run = submit(h, victim, conversation)
        h.model.wait_entered(victim)
        initial_attempts = attempts(h, victim_run)
        assert len(initial_attempts) == 1 and initial_attempts[0]['status'] == 'EXECUTING'
        attempt_id = initial_attempts[0]['attempt_id']
        assert candidates(h, victim_run) == [] and completions(h, victim_run) == []
        assert len(model_calls(h, victim)) == 1
        proxy.arm_next_insert(pause_before_disconnect=True)
        h.model.release(victim)
        before_disconnect = require_commit_receipt(proxy.wait_server_commit(timeout=30), disconnected=False)
        evidence['server_commit_before_disconnect'] = before_disconnect
        # The socket is still open and zero targeted response bytes were sent.
        # Independently visible row + unchanged Completion/accepted head confirms
        # the durable candidate exists before any adapter uncertainty readback.
        committed_rows = candidates(h, victim_run)
        assert len(committed_rows) == 1 and committed_rows[0]['attempt_id'] == attempt_id
        assert committed_rows[0]['parent_ref'] == before['accepted_ref']
        assert committed_rows[0]['parent_digest'] == before['accepted_digest']
        assert completions(h, victim_run) == [] and outboxes(h, victim_run) == []
        pre_drop_head = head(h, run(h, victim_run))
        assert pre_drop_head['accepted_ref'] == before['accepted_ref']
        assert pre_drop_head['accepted_digest'] == before['accepted_digest']
        assert proxy.evidence()['fault'] is None, 'observation barrier expired before independent committed-row read'
        evidence['committed_candidate_before_response_loss'] = committed_rows[0]
        evidence['pre_drop_completion_count'] = 0
        evidence['pre_drop_final_count'] = 0
        evidence['pre_drop_head'] = pre_drop_head
        proxy.release_drop()
        dropped = require_commit_receipt(proxy.wait_fault(timeout=5), disconnected=True)
        evidence['response_loss'] = dropped
        result = wait_success(h, victim_run)
        delivery = h.wait_delivery(victim_run)
        succeeded_attempts = attempts(h, victim_run)
        assert len(succeeded_attempts) == 1 and succeeded_attempts[0]['attempt_id'] == attempt_id
        assert succeeded_attempts[0]['status'] == 'SUCCEEDED'
        assert len(model_calls(h, victim)) == 1, 'committed candidate loss reran the SDK/model'
        assert result['candidate'] == committed_rows[0]
        assert result['completion']['candidate_digest'] == committed_rows[0]['content_digest']
        accepted = head(h, run(h, victim_run))
        assert accepted['accepted_ref'] == committed_rows[0]['candidate_ref']
        assert accepted['accepted_digest'] == committed_rows[0]['content_digest']
        assert accepted['settled_sequence'] == run(h, victim_run)['session_sequence']
        # Recovery must issue a real read through the proxy on a NEW socket,
        # not synthesize success from the bytes lost in the failed pool Exec.
        post_drop_reads = [record for record in proxy.evidence()['candidate_reads']
                           if record['monotonic_ns'] > dropped['disconnect_started']['monotonic_ns']]
        assert post_drop_reads and all(record['connection_id'] != dropped['connection_id'] for record in post_drop_reads)
        assert_transcript(model_calls(h, victim)[0], [seed, victim])
        follower_run = submit(h, follower, conversation)
        follower_result = wait_success(h, follower_run)
        follower_delivery = h.wait_delivery(follower_run)
        assert len(model_calls(h, follower)) == 1
        assert_transcript(model_calls(h, follower)[0], [seed, victim, follower])
        assert follower_result['candidate']['parent_ref'] == committed_rows[0]['candidate_ref']
        assert follower_result['candidate']['parent_digest'] == committed_rows[0]['content_digest']
        h.wait(lambda: any(record.get('operation') == 'complete' and record.get('result') == 'ok'
                           for record in operation_records(h, victim_run)), 'typed completion log after commit-response reconciliation')
        observations = operation_records(h, victim_run)
        for operation in ('session_stage', 'complete'):
            matches = [record for record in observations if record.get('operation') == operation]
            assert len(matches) == 1 and matches[0]['result'] == 'ok' and matches[0]['attempt_id'] == attempt_id
        evidence.update(result='PASS', worker_pid=worker.pid, seed_run_id=seed_run,
                        victim_run_id=victim_run, follower_run_id=follower_run,
                        seed_delivery=seed_delivery, victim_delivery=delivery, follower_delivery=follower_delivery,
                        initial_attempts=initial_attempts, final_attempts=succeeded_attempts,
                        accepted_head_before=before, accepted_head_after=accepted,
                        completion=result['completion'], final=result['final'],
                        candidate_count=len(candidates(h, victim_run)), completion_count=len(completions(h, victim_run)),
                        final_count=len(outboxes(h, victim_run)), sdk_call_count=1,
                        recovery_candidate_reads=post_drop_reads, typed_operations=observations,
                        follower_candidate=follower_result['candidate'], follower_request=model_calls(h, follower)[0])
        print('WORKER_SESSION_COMMIT_LOSS=PASS server INSERT 0 1 + ReadyForQuery I; row independently visible before response loss; exact candidate readback on new socket; one Attempt/SDK/Candidate/Completion/Final; follower history exact', flush=True)
        return evidence
    except BaseException:
        evidence['result'] = 'FAIL'
        raise
    finally:
        proxy.release_drop()
        h.model.release(victim)
        evidence['proxy'] = proxy.evidence()
        path.write_text(json.dumps(evidence, indent=2)+'\n')
        assert json.loads(path.read_text()) == evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    from harness import Harness
    import gateway_fixture
    h = Harness(args.root, args.artifacts, args.race)
    proxy = None
    try:
        h.provision()
        proxy = install_proxy(h)
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        run_commit(h)
    finally:
        try:
            h.close()
        finally:
            if proxy is not None:
                proxy.close()
                closed = proxy.evidence()
                assert closed['closed'] and closed['live_connections'] == 0
                path = Path(h.artifacts) / 'session-commit-proxy-cleanup.json'
                path.write_text(json.dumps(closed, indent=2)+'\n')
                assert json.loads(path.read_text()) == closed


if __name__ == '__main__':
    main()
