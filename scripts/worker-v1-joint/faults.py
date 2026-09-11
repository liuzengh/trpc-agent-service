"""Real Worker OS-process failover checks, composed by test-worker-v1-joint.py.

Harness boundary (fixtures are owned and cleaned up by the caller):
* start_fault_workers(max_attempts: int) -> {worker_id: subprocess.Popen}; each
  process has its own listener and identity and is ready before this returns.
* stop_fault_workers() stops/reaps this set, including already-SIGKILLed children.
* send_text(text, conversation_id=...) -> immutable RunID through real Gateway.
* wait_delivery(run_id) -> real Gateway durable/external-fixture delivery evidence.
* sql(query) -> psql rows (list[list[str]]), wait(predicate, label, timeout=30).
* model.hold(text), release(text), wait_entered(text), requests (OpenAI objects).
* artifacts: pathlib.Path. No credentials or execution tokens enter our evidence.

A stable, test-owned L4 proof relay may route TLS bytes to either live Worker.
The real Worker still owns TLS authentication and the online SQL proof.
"""
from __future__ import annotations

import copy
from datetime import datetime
import json
import os
from pathlib import Path
import signal
import select
import socket
import socketserver
import threading
import time
import uuid



class ProofSwitch:
    """Fixture-only L4 relay: TLS and proof bytes stay end-to-end Worker owned.

    The relay has no certificates, identity parser, HTTP parser, cached proof,
    retry of in-flight bytes, or synthesized success. A new TCP connection may
    select any live backend because all Workers query the same execution ledger.
    """
    def __init__(self, host='127.0.0.1', port=0):
        self.host, self.port = host, port
        self._lock = threading.Lock()
        self._backends = {}
        self._sockets = set()
        self._next = 0
        self._server = None
        self._thread = None

    def start(self):
        if self._server is not None:
            raise RuntimeError('proof relay already started')
        owner = self

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True
            block_on_close = False

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                owner._relay(self.request)

        self._server = Server((self.host, self.port), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        kwargs={'poll_interval': 0.05}, daemon=True)
        self._thread.start()
        return self

    def add(self, worker_id, host, port):
        with self._lock:
            self._backends[worker_id] = (host, int(port))

    def remove(self, worker_id):
        with self._lock:
            self._backends.pop(worker_id, None)

    def disconnect(self):
        """Drop owned TCP streams, retaining listener and backend configuration.

        Used when a stopped process leaves pooled proof connections open. This
        preserves EOF/transport failure; it does not synthesize a proof response.
        """
        with self._lock:
            connections = list(self._sockets)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        return len(connections)

    def _relay(self, client):
        upstream = None
        with self._lock:
            targets = list(self._backends.values())
            if targets:
                offset = self._next % len(targets)
                self._next += 1
                targets = targets[offset:] + targets[:offset]
            self._sockets.add(client)
        try:
            for target in targets:
                try:
                    upstream = socket.create_connection(target, timeout=2)
                    break
                except OSError:
                    continue
            if upstream is None:
                return
            client.settimeout(2)
            upstream.settimeout(2)
            with self._lock:
                self._sockets.add(upstream)
            while True:
                readable, _, _ = select.select([client, upstream], [], [], 0.5)
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    target = upstream if source is client else client
                    target.sendall(data)
        except (OSError, ValueError):
            # Connection loss is preserved as connection loss, never a proof.
            return
        finally:
            with self._lock:
                self._sockets.discard(client)
                if upstream is not None:
                    self._sockets.discard(upstream)
            if upstream is not None:
                upstream.close()

    def close(self):
        server, self._server = self._server, None
        if server is not None:
            server.shutdown()
            server.server_close()
        with self._lock:
            sockets = list(self._sockets)
            self._backends.clear()
        for connection in sockets:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        if self._thread is not None:
            self._thread.join(timeout=3)
            if self._thread.is_alive():
                raise RuntimeError('proof relay did not stop')
            self._thread = None


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def rows(h, query: str) -> list[dict]:
    values = h.sql("SELECT COALESCE(json_agg(row_to_json(fault_q)), '[]'::json)::text "
                   "FROM (" + query + ") fault_q")
    if len(values) != 1 or len(values[0]) != 1:
        raise AssertionError("fault observation must return one JSON aggregate")
    result = json.loads(values[0][0])
    if not isinstance(result, list):
        raise AssertionError("fault observation is not a row list")
    return result


def one(h, query: str) -> dict:
    values = rows(h, query)
    if len(values) != 1:
        raise AssertionError("fault observation expected exactly one immutable fact")
    return values[0]


def run(h, run_id: str) -> dict:
    return one(h, "SELECT tenant_id,run_id,session_id,session_sequence,status,attempts,"
               "current_attempt_id,policy_json,execution_deadline FROM worker.execution_runs "
               "WHERE run_id=" + quote(run_id))


def attempts(h, run_id: str) -> list[dict]:
    return rows(h, "SELECT attempt_id,worker_id,generation,lease_epoch,status,lease_until,"
                "parent_ref,parent_digest,created_at,agent_started_at,ended_at,reason "
                "FROM worker.execution_attempts WHERE run_id=" + quote(run_id) + " ORDER BY generation")


def head(h, state: dict) -> dict:
    return one(h, "SELECT accepted_ref,accepted_digest,settled_sequence,next_sequence "
               "FROM worker.execution_sessions WHERE tenant_id=" + quote(state['tenant_id']) +
               " AND session_id=" + quote(state['session_id']))


def completions(h, run_id: str) -> list[dict]:
    return rows(h, "SELECT completion_id,attempt_id,kind,status,candidate_ref,candidate_digest,"
                "final_intent_id,reply_disposition,reason FROM worker.execution_completions "
                "WHERE run_id=" + quote(run_id))


def outboxes(h, run_id: str) -> list[dict]:
    return rows(h, "SELECT intent_id,convert_from(payload,'UTF8')::json AS payload "
                "FROM worker.execution_reply_outbox WHERE run_id=" + quote(run_id))


def candidates(h, run_id: str) -> list[dict]:
    return rows(h, "SELECT candidate_ref,attempt_id,parent_ref,parent_digest,content_digest,"
                "convert_from(content,'UTF8')::json AS content FROM runtime_session.session_candidates "
                "WHERE run_id=" + quote(run_id))


def request_text(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return ''.join(item.get('text', '') for item in value if isinstance(item, dict))
    return ''


def model_calls(h, text: str) -> list[dict]:
    # Take an owned snapshot; the HTTP fixture records before signalling entered.
    result = []
    for request in copy.deepcopy(list(h.model.requests)):
        users = [request_text(m.get('content')) for m in request.get('messages', [])
                 if m.get('role') == 'user']
        if users and users[-1] == text:
            result.append(request)
    return result


def assert_history(request: dict, seed_answer: str, current_input: str,
                   forbidden_input: str | None = None, poison: str | None = None) -> None:
    messages = request.get('messages', [])
    assistants = [request_text(m.get('content')) for m in messages if m.get('role') == 'assistant']
    users = [request_text(m.get('content')) for m in messages if m.get('role') == 'user']
    assert seed_answer in assistants, 'accepted seed answer did not reach real SDK HTTP request'
    assert users.count(current_input) == 1, 'current input duplicated across failed Attempt overlay'
    if forbidden_input:
        assert forbidden_input not in users, 'failed Run user input polluted later accepted history'
    if poison:
        assert poison not in json.dumps(messages), 'killed Attempt partial response polluted history'


def submit(h, text: str, conversation_id: str) -> str:
    run_id = h.send_text(text, conversation_id=conversation_id)
    # Gateway Admission precedes NATS transfer and Worker intake. Wait for the
    # actual durable Worker fact instead of assuming an immediate local row.
    h.wait(lambda: bool(rows(h, "SELECT run_id FROM worker.execution_runs WHERE run_id=" + quote(run_id))),
           'Worker durable intake after Gateway Admission ' + run_id, timeout=30)
    return run_id


def wait_success(h, run_id: str) -> dict:
    h.wait(lambda: run(h, run_id)['status'] == 'SUCCEEDED',
           'Worker process succeeds immutable Run ' + run_id, timeout=40)
    completion = completions(h, run_id)
    assert len(completion) == 1 and completion[0]['kind'] == 'ATTEMPT'
    assert completion[0]['status'] == 'SUCCEEDED' and completion[0]['reply_disposition'] == 'FINAL'
    final = outboxes(h, run_id)
    assert len(final) == 1 and completion[0]['final_intent_id'] == final[0]['intent_id']
    candidate = candidates(h, run_id)
    assert len(candidate) == 1 and candidate[0]['candidate_ref'] == completion[0]['candidate_ref']
    assert candidate[0]['attempt_id'] == completion[0]['attempt_id']
    return {'completion': completion[0], 'final': final[0], 'candidate': candidate[0]}


def hold(h, text: str, poison: str | None = None) -> None:
    # Optional stronger fixture method emits a partial SSE response before block.
    if poison and hasattr(h.model, 'hold_partial'):
        h.model.hold_partial(text, poison)
    else:
        h.model.hold(text)


def assert_pair_and_queue(h, workers: dict, victim_run: str, follower_run: str) -> dict:
    assert len(workers) == 2 and len({p.pid for p in workers.values()}) == 2
    assert all(p.poll() is None for p in workers.values()), 'both Worker OS replicas must remain alive'
    victim, follower = run(h, victim_run), run(h, follower_run)
    assert victim['session_id'] == follower['session_id']
    assert follower['session_sequence'] == victim['session_sequence'] + 1
    assert follower['status'] == 'QUEUED' and follower['attempts'] == 0
    a = attempts(h, victim_run)
    assert len(a) == 1 and a[0]['status'] == 'EXECUTING' and a[0]['worker_id'] in workers
    # Observe across multiple scheduler ticks, not merely one racing row read.
    for _ in range(4):
        active = one(h, "SELECT count(*) AS n FROM worker.execution_attempts a JOIN "
                     "worker.execution_runs r ON a.tenant_id=r.tenant_id AND a.run_id=r.run_id "
                     "WHERE r.tenant_id=" + quote(victim['tenant_id']) + " AND r.session_id=" +
                     quote(victim['session_id']) + " AND a.status IN ('PREPARING','EXECUTING') "
                     "AND a.lease_until>clock_timestamp()")
        assert active['n'] == 1, 'two processes obtained concurrent live same-Session execution'
        assert run(h, follower_run)['attempts'] == 0, 'follower ran before queue head terminalized'
        time.sleep(0.08)
    return a[0]


def kill_holder(h, workers: dict, run_id: str, observed: dict) -> dict:
    worker_id = observed['worker_id']
    process = workers[worker_id]
    survivor_id = next(key for key in workers if key != worker_id)
    assert workers[survivor_id].poll() is None
    pid = process.pid
    os.kill(pid, signal.SIGKILL)
    status = process.wait(timeout=5)
    assert status == -signal.SIGKILL, 'Worker must actually terminate from SIGKILL, not graceful cancel'
    after = attempts(h, run_id)
    killed = next(a for a in after if a['attempt_id'] == observed['attempt_id'])
    # Re-read after process exit: any in-flight renewal has either committed or
    # rolled back. Recovery must not precede this final durable lease timestamp.
    return {'killed_worker_id': worker_id, 'killed_pid': pid, 'signal': 'SIGKILL',
            'returncode': status, 'survivor_worker_id': survivor_id,
            'survivor_pid': workers[survivor_id].pid, 'attempt_id': killed['attempt_id'],
            'lease_until_at_process_exit': killed['lease_until']}


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def scenario(h, *, max_attempts: int, suffix: str, journal: list) -> dict:
    workers = h.start_fault_workers(max_attempts=max_attempts)
    journal.append({'event': 'replicas_started', 'max_attempts': max_attempts,
                    'processes': {key: process.pid for key, process in workers.items()}})
    marker = 'fault-' + suffix
    conversation = str(900000000 + int(uuid.uuid4().hex[:7], 16))
    seed, victim, follower = marker + '-seed', marker + '-victim', marker + '-follower'
    poison = marker + '-UNACCEPTED-PARTIAL'
    try:
        seed_run = submit(h, seed, conversation)
        seed_result = wait_success(h, seed_run)
        h.wait_delivery(seed_run)
        seed_answer = seed_result['final']['payload']['content']['text']
        accepted_before = head(h, run(h, seed_run))
        hold(h, victim, poison)
        hold(h, follower)
        victim_run = submit(h, victim, conversation)
        h.model.wait_entered(victim)
        assert len(model_calls(h, victim)) == 1
        follower_run = submit(h, follower, conversation)
        holder = assert_pair_and_queue(h, workers, victim_run, follower_run)
        assert run(h, victim_run)['policy_json']['MaxAttempts'] == max_attempts
        journal.append({'event': 'single_live_same_session', 'victim_run_id': victim_run,
                        'follower_run_id': follower_run, 'attempt': holder})
        assert head(h, run(h, victim_run))['accepted_ref'] == accepted_before['accepted_ref']
        assert candidates(h, victim_run) == [] and completions(h, victim_run) == []
        killed = kill_holder(h, workers, victim_run, holder)
        journal.append({'event': 'os_sigkill_reaped', **killed})
        print('WORKER_OS_SIGKILL: ' + json.dumps(killed, sort_keys=True), flush=True)
        assert head(h, run(h, victim_run))['accepted_ref'] == accepted_before['accepted_ref']
        h.model.release(victim)

        # Keep follower's real model HTTP call held, so formal head/settled
        # sequence can be inspected after predecessor recovery and before it advances.
        h.model.wait_entered(follower)
        a = attempts(h, victim_run)
        assert a[0]['attempt_id'] == killed['attempt_id'] and a[0]['status'] == 'ABORTED'
        recovered_head = head(h, run(h, victim_run))
        record = {'scenario': 'retry-after-sigkill' if max_attempts > 1 else 'last-attempt-sigkill',
                  'max_attempts': max_attempts,
                  'blocked_model_phase': 'after_partial_sse' if hasattr(h.model, 'hold_partial') else 'before_response',
                  'processes': {k: p.pid for k, p in workers.items()},
                  'seed_run_id': seed_run, 'victim_run_id': victim_run,
                  'follower_run_id': follower_run, 'session_id': run(h, victim_run)['session_id'],
                  'kill': killed, 'attempts': a}
        if max_attempts > 1:
            victim_result = wait_success(h, victim_run)
            assert len(a) == 2 and a[1]['worker_id'] == killed['survivor_worker_id']
            assert a[1]['generation'] == a[0]['generation'] + 1
            assert a[1]['lease_epoch'] == a[0]['lease_epoch'] + 1
            assert parse_time(a[1]['created_at']) >= parse_time(killed['lease_until_at_process_exit']), 'early takeover before durable lease expiry'
            assert victim_result['candidate']['parent_ref'] == accepted_before['accepted_ref']
            assert recovered_head['accepted_ref'] == victim_result['candidate']['candidate_ref']
            assert poison not in json.dumps(victim_result['candidate']['content'])
            calls = model_calls(h, victim)
            assert len(calls) == 2, 'retry must use exactly one new real SDK HTTP call'
            assert_history(calls[1], seed_answer, victim, poison=poison)
            record['completion'] = victim_result['completion']
            record['delivery'] = h.wait_delivery(victim_run)
            record['final_count'] = len(outboxes(h, victim_run))
            record['candidate_count'] = len(candidates(h, victim_run))
        else:
            assert len(a) == 1 and len(model_calls(h, victim)) == 1, 'exhausted Run reran model'
            c = completions(h, victim_run)
            assert len(c) == 1 and c[0]['kind'] == 'SYSTEM_TERMINATION'
            assert c[0]['status'] == 'FAILED' and c[0]['reply_disposition'] == 'NONE'
            assert c[0]['reason'] == 'ATTEMPTS_EXHAUSTED' and c[0]['final_intent_id'] == ''
            assert candidates(h, victim_run) == [] and outboxes(h, victim_run) == []
            assert recovered_head['accepted_ref'] == accepted_before['accepted_ref']
            assert recovered_head['accepted_digest'] == accepted_before['accepted_digest']
            assert recovered_head['settled_sequence'] == run(h, victim_run)['session_sequence']
            calls = model_calls(h, follower)
            assert len(calls) == 1
            assert_history(calls[0], seed_answer, follower, forbidden_input=victim, poison=poison)
            record['completion'] = c[0]
            record['final_count'], record['candidate_count'] = 0, 0
        h.model.release(follower)
        follower_result = wait_success(h, follower_run)
        follower_delivery = h.wait_delivery(follower_run)
        assert run(h, follower_run)['current_attempt_id'] == follower_result['completion']['attempt_id']
        final_head = head(h, run(h, follower_run))
        assert final_head['settled_sequence'] == run(h, follower_run)['session_sequence']
        assert final_head['accepted_ref'] == follower_result['candidate']['candidate_ref']
        assert poison not in json.dumps(follower_result['candidate']['content'])
        record.update(follower_completion=follower_result['completion'],
                      follower_delivery=follower_delivery,
                      accepted_head_before=accepted_before, accepted_head_after=final_head,
                      victim_model_calls=len(model_calls(h, victim)), result='PASS')
        return record
    finally:
        # Release local fixture handlers even when an assertion fails; the
        # orchestrator remains responsible for process/container/certificate cleanup.
        h.model.release(victim)
        h.model.release(follower)
        h.stop_fault_workers()


def run_faults(h) -> dict:
    evidence = {'version': 'worker-v1-os-faults/v1', 'scenarios': [], 'events': []}
    path = Path(h.artifacts) / 'worker-process-faults.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        for max_attempts in (3, 1):
            item = scenario(h, max_attempts=max_attempts, suffix=uuid.uuid4().hex[:12],
                            journal=evidence['events'])
            evidence['scenarios'].append(item)
            print('WORKER_OS_FAULT_PASS: ' + item['scenario'] +
                  '; SIGKILL returncode=-9; killed PID=' + str(item['kill']['killed_pid']) +
                  '; survivor PID=' + str(item['kill']['survivor_pid']), flush=True)
        evidence['result'] = 'PASS'
        return evidence
    except BaseException:
        evidence['result'] = 'FAIL'
        raise
    finally:
        path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
        # Reopen exactly the evidence artifact written by this helper.
        if json.loads(path.read_text()) != evidence:
            raise AssertionError('process-fault evidence readback mismatch')
