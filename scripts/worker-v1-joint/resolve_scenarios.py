"""WV-11/12/13 uncertainty slice using real owner HTTP and PostgreSQL locks.

Install the two mTLS relays after private provision and BEFORE Control starts.
Run these three cases before credentials are cleared. They do not cover the
entire uses-negative matrix or verifier transport-error classification. The
response-drop/timeout cases preserve real upstream status and never replay a
request. The lock case retains an in-flight owner request across a killed
Worker downstream and observes a new online proof after the Profile lock.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import select
import signal
import subprocess
import time
import uuid

from commit_scenarios import operation_records
from faults import (attempts, candidates, completions, model_calls, outboxes,
                    quote, rows, run, submit, wait_success)
from resolve_fault_proxy import (ATTEMPT_PATH, RESOLVE_PATH, GrantGroups,
                                 ResolveFaultProxy)


def install_resolve_proxies(h):
    assert h.pg == h.prefix+'-pg' and h.pg in h.containers
    assert not hasattr(h, 'profile_id') and not hasattr(h, 'control')
    assert not hasattr(h, 'resolve_proxies')
    groups = GrantGroups()
    proxies = []
    try:
        owner = ResolveFaultProxy(h.urls['control_runtime'], h.certs, {
            'spiffe://agent-platform/worker/one': 'worker',
            'spiffe://agent-platform/worker/two': 'worker_two'}, groups, 'control-resolve').start()
        proxies.append(owner)
        proof = ResolveFaultProxy(h.urls['worker'], h.certs, {
            'spiffe://agent-platform/control-api': 'control',
            'spiffe://agent-platform/channel-gateway': 'gateway'}, groups, 'worker-proof').start()
        proxies.append(proof)
        h.resolve_proxies = proxies
        h.urls['control_runtime'] = owner.url
        h.urls['worker'] = proof.url
        return proxies
    except BaseException:
        for proxy in reversed(proxies):
            proxy.close()
        raise


class ProfileLock:
    """One private PostgreSQL transaction, locks only and always ROLLBACK."""
    def __init__(self, h):
        assert h.pg == h.prefix+'-pg' and h.pg in h.containers
        self.h, self.process = h, None
        self.acquired_ns = self.release_started_ns = self.released_ns = None
        self.exit_code = None

    def acquire(self):
        if self.process is not None:
            raise RuntimeError('Profile lock is acquired once')
        # This pipe is not part of artifact logging. It contains no credential or
        # business writes. The ownership guard forbids another fixture's PG.
        argv = ['docker', 'exec', '-i', self.h.pg, 'psql', '-X', '-A', '-t',
                '-v', 'ON_ERROR_STOP=1', '-U', 'platform_admin', '-d', 'agent_platform']
        self.process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=self.h.env, cwd=self.h.root)
        sql = ("BEGIN; SET LOCAL lock_timeout='5s'; "
               "SET LOCAL idle_in_transaction_session_timeout='20s'; "
               "SELECT 'WV_RESOLVE_LOCKED' FROM control.runtime_profiles WHERE tenant_id="+
               quote(self.h.tenant_id)+" AND id="+quote(self.h.profile_id)+" FOR UPDATE;\n")
        self.process.stdin.write(sql.encode()); self.process.stdin.flush()
        buffered = b''
        deadline = time.monotonic()+8
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError('Profile lock fixture exited before acquisition')
            readable, _, _ = select.select([self.process.stdout], [], [], .1)
            if readable:
                data = os.read(self.process.stdout.fileno(), 4096)
                if not data:
                    raise RuntimeError('Profile lock output ended')
                buffered += data
                if b'WV_RESOLVE_LOCKED\n' in buffered:
                    self.acquired_ns = time.monotonic_ns()
                    return self
                if len(buffered) > 8192:
                    raise RuntimeError('Profile lock output bound')
        raise RuntimeError('Profile lock acquisition deadline')

    def release(self):
        if self.process is None or self.released_ns is not None:
            return
        self.release_started_ns = time.monotonic_ns()
        try:
            if self.process.poll() is None:
                self.process.communicate(b'ROLLBACK;\n\\q\n', timeout=5)
            else:
                # communicate owns all pipes even when the child failed before
                # lock acquisition (poll/wait alone do not close them).
                self.process.communicate(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            if self.process.poll() is None:
                self.process.kill()
            self.process.communicate(timeout=5)
        finally:
            # A second communicate can itself fail; close every parent handle
            # without turning a failed cleanup into a claimed successful one.
            for name in ('stdin', 'stdout', 'stderr'):
                stream = getattr(self.process, name)
                if stream is not None:
                    stream.close()
            self.exit_code = self.process.returncode
            self.released_ns = time.monotonic_ns()

    def evidence(self):
        return {'acquired_ns': self.acquired_ns, 'release_started_ns': self.release_started_ns,
                'released_ns': self.released_ns,
                'process_exit': self.exit_code, 'business_rows_changed': False,
                'release_command': 'ROLLBACK', 'table': 'control.runtime_profiles'}


def lock_waiters(h):
    return rows(h, "SELECT pid,usename,state,wait_event_type,wait_event FROM pg_stat_activity "
                  "WHERE datname='agent_platform' AND usename='control_runtime' "
                  "AND wait_event_type='Lock' AND query LIKE '%runtime_profiles%FOR UPDATE%'")


def events_for_run(proxy, run_id, path):
    # Denial responses lack identities. Correlate only through process-local
    # equality labels that were first bound to a real owner identity response.
    events = proxy.events()
    groups = {e['grant_group'] for e in events
              if e.get('run_id') == run_id and 'grant_group' in e}
    return [e for e in events if e.get('path') == path and
            (e.get('run_id') == run_id or e.get('grant_group') in groups)]


def assert_resolve_once(resolve_events, proof_events, attempt_facts):
    allowed = {a['attempt_id'] for a in attempt_facts}
    proof_groups = {e['grant_group']: e['attempt_id'] for e in proof_events
                    if e.get('attempt_id') in allowed and 'grant_group' in e}
    counts = {attempt_id: 0 for attempt_id in allowed}
    for event in resolve_events:
        attempt_id = event.get('attempt_id', proof_groups.get(event.get('grant_group')))
        if attempt_id in counts:
            counts[attempt_id] += 1
    assert counts and all(count == 1 for count in counts.values()), \
        'original Attempt retried Resolve or an Attempt had no owner Resolve'
    return counts


def require_recovery(h, run_id, text, resolve_proxy, proof_proxy):
    result = wait_success(h, run_id)
    delivery = h.wait_delivery(run_id)
    facts = attempts(h, run_id)
    assert len(facts) == 2 and facts[0]['attempt_id'] != facts[1]['attempt_id']
    assert facts[1]['generation'] == facts[0]['generation']+1
    assert facts[1]['status'] == 'SUCCEEDED'
    assert facts[0]['agent_started_at'] is None, 'uncertain Prepare reached SDK'
    assert len(model_calls(h, text)) == 1, 'uncertain preparation duplicated model execution'
    assert len(candidates(h, run_id)) == len(completions(h, run_id)) == len(outboxes(h, run_id)) == 1
    assert result['candidate']['attempt_id'] == facts[1]['attempt_id']
    assert result['completion']['attempt_id'] == facts[1]['attempt_id']
    # Let pending relay responses finish before counting owner calls.
    proof_events = events_for_run(proof_proxy, run_id, ATTEMPT_PATH)
    groups = {e['grant_group'] for e in proof_events}
    def settled():
        values = [e for e in resolve_proxy.events() if e.get('path') == RESOLVE_PATH
                  and e.get('grant_group') in groups]
        return values if len(values) == len(facts) and all('finished_ns' in e for e in values) else None
    resolve_events = h.wait(settled, 'all real Resolve responses settled', timeout=12)
    counts = assert_resolve_once(resolve_events, proof_events, facts)
    return {'run_id': run_id, 'attempts': facts, 'resolve_events': resolve_events,
            'proof_events': proof_events, 'resolve_calls_per_attempt': counts,
            'model_calls': 1, 'candidate_count': 1, 'completion_count': 1, 'final_count': 1,
            'candidate_ref': result['candidate']['candidate_ref'],
            'candidate_attempt_id': result['candidate']['attempt_id'],
            'completion': result['completion'], 'delivery': delivery,
            'operation_records': operation_records(h, run_id)}


def require_suppressed(event, mode):
    assert event and event.get('upstream_status') == 200
    assert event.get('fault') == mode
    assert event.get('owner_response_bytes', 0) > 0
    assert event.get('downstream_body_bytes_forwarded') == 0
    assert 'downstream_status' not in event
    assert event.get('downstream_action') == 'owner_200_suppressed'
    assert event.get('finished_ns', 0) >= event['owner_response_ns']
    return event


def _uncertain_response(h, resolve_proxy, proof_proxy, mode):
    h.stop_fault_workers()
    # The deadline is explicit fixture behavior, not a hidden model policy.
    worker = h.start_worker('worker-one', {
        'policy': {'max_attempts': 3, 'retry_backoff': '300ms'},
        'timing': {'request_timeout': '800ms'}})
    text = 'resolve-'+mode+'-'+uuid.uuid4().hex[:10]
    conversation = str(3700000000+int(uuid.uuid4().hex[:7], 16))
    resolve_proxy.arm(mode)
    run_id = submit(h, text, conversation)
    try:
        first = h.wait(lambda: (lambda e: e if e and e.get('upstream_status') == 200 else None)
                       (resolve_proxy.fault()), 'true Control 200 before response fault', timeout=15)
        assert first.get('run_id') == run_id
        original = first['attempt_id']
        if mode == 'hold_after_owner_200':
            h.wait(lambda: any(a['attempt_id'] == original and a['status'] == 'FAILED'
                               for a in attempts(h, run_id)), 'Worker resolve deadline settles original Attempt', timeout=8)
            held = resolve_proxy.fault()
            assert 'finished_ns' not in held and held['downstream_body_bytes_forwarded'] == 0
            resolve_proxy.release()
        h.wait(lambda: bool((resolve_proxy.fault() or {}).get('finished_ns')),
               'response fault completed', timeout=5)
        fault = require_suppressed(resolve_proxy.fault(), mode)
        result = require_recovery(h, run_id, text, resolve_proxy, proof_proxy)
        assert result['attempts'][0]['attempt_id'] == original
        assert result['attempts'][0]['status'] == 'FAILED'
        assert result['attempts'][0]['reason'] == 'DEPENDENCY_UNAVAILABLE'
        result.update(case=mode, fault=fault, worker_pid=worker.pid,
                      request_timeout='800ms', retry_backoff='300ms')
        if mode == 'hold_after_owner_200':
            # This is response withholding until the real caller deadline, not a
            # fabricated 504 or an assertion that packets disappeared on a wire.
            assert fault['released_by_test'] is True
            result['original_attempt_failed_while_response_still_held'] = True
        return result
    finally:
        resolve_proxy.release()


def _profile_lock_fence(h, resolve_proxy, proof_proxy):
    workers = h.start_fault_workers(max_attempts=3)
    assert len(workers) == 2 and all(p.poll() is None for p in workers.values())
    lock = ProfileLock(h)
    text = 'resolve-profile-lock-'+uuid.uuid4().hex[:10]
    conversation = str(3900000000+int(uuid.uuid4().hex[:7], 16))
    killed = None
    try:
        lock.acquire()
        run_id = submit(h, text, conversation)
        initial = h.wait(lambda: next((e for e in events_for_run(proof_proxy, run_id, ATTEMPT_PATH)
            if e.get('upstream_status') == 200), None), 'first real online authorization before Profile lock', timeout=5)
        waiting = h.wait(lambda: lock_waiters(h), 'real Control PG Profile FOR UPDATE wait', timeout=3)
        original_facts = attempts(h, run_id)
        assert len(original_facts) == 1 and original_facts[0]['status'] == 'PREPARING'
        original = original_facts[0]
        assert initial['attempt_id'] == original['attempt_id']
        assert len(model_calls(h, text)) == 0 and candidates(h, run_id) == []
        assert workers[original['worker_id']].poll() is None
        killed = workers[original['worker_id']]
        killed_ns = time.monotonic_ns()
        h.stop_worker(kill=True, worker_id=original['worker_id'])
        assert killed.returncode == -signal.SIGKILL
        survivor_id = next(w for w in workers if w != original['worker_id'])
        assert workers[survivor_id].poll() is None
        expired = h.wait(lambda: rows(h,
            'SELECT attempt_id,lease_until,clock_timestamp() AS observed_at FROM worker.execution_attempts '
            'WHERE attempt_id='+quote(original['attempt_id'])+' AND lease_until<=clock_timestamp()'),
            'durable original Attempt lease expiry before Profile unlock', timeout=5)
        lock.release()
        assert lock.exit_code == 0, 'Profile lock did not ROLLBACK cleanly'
        denied = h.wait(lambda: next((e for e in proof_proxy.events()
            if e.get('path') == ATTEMPT_PATH and e.get('grant_group') == initial['grant_group']
            and e.get('upstream_status') == 403 and e.get('downstream_status') == 403), None),
            'same original grant is explicitly denied by surviving real Worker after lock', timeout=5)
        assert denied['started_ns'] >= lock.release_started_ns
        result = require_recovery(h, run_id, text, resolve_proxy, proof_proxy)
        old_resolve = next(e for e in result['resolve_events']
                           if e['grant_group'] == initial['grant_group'])
        assert old_resolve['upstream_status'] == 403, 'Control lock recheck did not produce stable denial'
        assert result['attempts'][0]['attempt_id'] == original['attempt_id']
        assert result['attempts'][1]['worker_id'] == survivor_id
        original_proofs = [e for e in result['proof_events'] if e.get('grant_group') == initial['grant_group']]
        assert [e.get('upstream_status') for e in original_proofs] == [200, 403]
        result.update(case='profile_lock_then_expired_grant_recheck', profile_lock=lock.evidence(),
            pg_waiters_before_kill=waiting, lease_expiry=expired[0],
            killed_worker_id=original['worker_id'], killed_pid=killed.pid,
            killed_exit_code=killed.returncode, killed_ns=killed_ns,
            surviving_worker_id=survivor_id, surviving_pid=workers[survivor_id].pid,
            original_proof_statuses=[200,403], original_control_status=403,
            lock_wait_owner_request_survived_downstream_worker_exit=True,
            original_model_calls=0, original_candidate_count=0)
        return result
    finally:
        lock.release()


def run_resolve(h):
    proxies = getattr(h, 'resolve_proxies', None)
    assert proxies and len(proxies) == 2, 'install mTLS relays before real Control starts'
    resolve_proxy, proof_proxy = proxies
    evidence = {'version': 'worker-real-resolve-uncertainty/v1', 'invariant_slices': ['WV-11','WV-12','WV-13'],
        'actual_owners': ['Control binary runtime HTTP', 'Worker binary proof HTTP', 'Control PostgreSQL'],
        'mtls_both_legs_verified': True, 'owner_requests_retried_by_proxy': False,
        'credential_bodies_or_capabilities_recorded': False, 'business_sql_writes': False,
        'not_covered': ['complete credential uses negative matrix', 'proof transport-error classification'],
        'cases': []}
    path = Path(h.artifacts)/'worker-resolve-uncertainty.json'
    try:
        for mode in ('drop_after_owner_200', 'hold_after_owner_200'):
            result = _uncertain_response(h, resolve_proxy, proof_proxy, mode)
            evidence['cases'].append(result)
            print('JOINT_RESOLVE_'+('LOST_RESPONSE' if mode.startswith('drop') else 'RESPONSE_TIMEOUT')+
                  '=PASS real Control 200 suppressed; new Attempt; one model/candidate/Final; Delivery ACCEPTED', flush=True)
        result = _profile_lock_fence(h, resolve_proxy, proof_proxy)
        evidence['cases'].append(result)
        print('JOINT_RESOLVE_PROFILE_LOCK=PASS real PG wait; Worker -9; expired grant proof 200 then 403; new Attempt Delivery ACCEPTED', flush=True)
        evidence['result'] = 'PASS'
        return evidence
    finally:
        resolve_proxy.release()
        path.write_text(json.dumps(evidence, indent=2)+'\n')
        assert json.loads(path.read_text()) == evidence
