"""WV-03 actual intake transaction crash and unaccepted ACK recovery.

Runs are created only through Gateway HTTP. The first fault delays the real
receipt INSERT before its enclosing transaction commits; the second removes
only the private broker's Worker Run-ACK permission. No business row is edited,
no stream is purged/deleted, and all fixture configuration changes are restored.
The ACK case proves server rejection of the ACK channel, not packet interception.
"""
from contextlib import contextmanager
import base64
import hashlib
import json
from pathlib import Path
import socket
import ssl
import time
from urllib.parse import urlparse
import uuid

from faults import attempts, candidates, completions, model_calls, outboxes, rows, run, wait_success
from reply_scenarios import _read_response

STREAM = 'RUN_REQUESTS_V1'
DURABLE = 'agent-worker-runs-v1'
SUBJECT = 'execution.run-requested.v1'
ACK_PERMISSION = '$JS.ACK.' + STREAM + '.' + DURABLE + '.>'


def without_run_ack(source):
    """Narrow removal from the generated ACL; never expand other permissions."""
    target = ', "' + ACK_PERMISSION + '"'
    if source.count(target) != 1:
        raise ValueError('private broker Run ACK permission is not unique')
    return source.replace(target, '', 1)


def broker_request(h, operation, body=None):
    subjects = {'stream': '$JS.API.STREAM.INFO.' + STREAM,
                'consumer': '$JS.API.CONSUMER.INFO.' + STREAM + '.' + DURABLE,
                'message': '$JS.API.STREAM.MSG.GET.' + STREAM}
    if operation not in subjects:
        raise ValueError('intake observer only permits INFO and MSG.GET')
    url = urlparse(h.nats_url)
    if url.scheme != 'tls' or url.hostname not in ('127.0.0.1', '::1'):
        raise ValueError('intake observer requires the owned TLS broker')
    sock = socket.create_connection((url.hostname, url.port), timeout=8)
    reader = None
    try:
        reader = sock.makefile('rb')
        if not reader.readline(4097).startswith(b'INFO '):
            raise RuntimeError('intake observer broker greeting missing')
        reader.close(); reader = None
        context = ssl.create_default_context(cafile=str(h.certs['ca']))
        sock = context.wrap_socket(sock, server_hostname=url.hostname)
        reader = sock.makefile('rb')
        inbox = '_INBOX.reconciler.intake.' + uuid.uuid4().hex
        connect = {'verbose': False, 'pedantic': True, 'tls_required': True,
                   'user': 'reconciler', 'pass': h.env['NATS_RECONCILER_PASSWORD'],
                   'name': 'joint-intake-observer', 'lang': 'python', 'version': '1', 'protocol': 1}
        payload = json.dumps(body or {}).encode()
        sock.sendall(b'CONNECT ' + json.dumps(connect).encode() + b'\r\nSUB ' + inbox.encode() +
                     b' 1\r\nPUB ' + subjects[operation].encode() + b' ' + inbox.encode() + b' ' +
                     str(len(payload)).encode() + b'\r\n' + payload + b'\r\nPING\r\n')
        return _read_response(reader, sock)
    finally:
        if reader is not None: reader.close()
        sock.close()


def stream_info(h):
    result = broker_request(h, 'stream')
    assert 'error' not in result and result['config']['name'] == STREAM
    assert result['config']['deny_delete'] and result['config']['deny_purge']
    return result


def consumer_info(h):
    result = broker_request(h, 'consumer')
    assert 'error' not in result and result['name'] == DURABLE
    return {key: result[key] for key in ('delivered', 'ack_floor', 'num_ack_pending', 'num_redelivered', 'num_pending')}


def retained(h, sequence):
    result = broker_request(h, 'message', {'seq': int(sequence)})
    if 'error' in result:
        assert result['error'].get('err_code') == 10037
        return None
    message = result['message']
    assert message['seq'] == int(sequence) and message['subject'] == SUBJECT
    return base64.b64decode(message['data'], validate=True)


def locate_run_message(h, run_id, preceding_sequence):
    info = stream_info(h)
    result = []
    for sequence in range(preceding_sequence + 1, info['state']['last_seq'] + 1):
        raw = retained(h, sequence)
        if raw is not None and json.loads(raw)['run_id'] == run_id:
            result.append({'sequence': sequence, 'raw': raw})
    assert len(result) <= 1, 'Gateway published the original Run more than once'
    return result[0] if result else None


def receipt(h, run_id):
    return rows(h, 'SELECT event_id,event_digest,run_id,outcome FROM worker.execution_receipts WHERE run_id=' + h.quote(run_id))


def gateway_publication(h, run_id):
    return h.sql('SELECT o.published_at IS NOT NULL,o.attempts FROM gateway.gateway_outbox o '
                 'JOIN gateway.gateway_admissions a ON a.admission_id=o.event_id WHERE a.run_id=' + h.quote(run_id))


def admin(h, statement):
    assert h.pg == h.prefix + '-pg' and h.pg in h.containers
    return h.command(['docker','exec',h.pg,'psql','-X','-A','-t','-v','ON_ERROR_STOP=1',
                      '-U','platform_admin','-d','agent_platform','-c',statement])


@contextmanager
def before_commit_delay(h, marker, events):
    """Delay only a matching real receipt INSERT, after other intake writes."""
    name = 'joint_intake_' + uuid.uuid4().hex[:12]
    sql = ("CREATE FUNCTION worker." + name + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
           "IF EXISTS (SELECT 1 FROM worker.execution_runs WHERE run_id=NEW.run_id "
           "AND request_json->'Input'->>'Text'=" + h.quote(marker) + ") THEN "
           "PERFORM pg_sleep(8); END IF; RETURN NEW; END $$; "
           "CREATE TRIGGER " + name + " AFTER INSERT ON worker.execution_receipts FOR EACH ROW "
           "EXECUTE FUNCTION worker." + name + "();")
    try:
        admin(h, sql)
        events.append({'event': 'receipt_before_commit_delay_installed', 'trigger': name,
                       'business_rows_modified': False})
        yield name
    finally:
        # Only the matching test transaction can briefly hold this DDL lock.
        admin(h, 'DROP TRIGGER IF EXISTS ' + name + ' ON worker.execution_receipts; '
                 'DROP FUNCTION IF EXISTS worker.' + name + '();')
        assert h.sql("SELECT tgname FROM pg_trigger WHERE tgname=" + h.quote(name)) == []
        events.append({'event': 'receipt_delay_removed', 'trigger': name})


def pending_before_commit(h, sequence, raw, run_id):
    backend = rows(h, "SELECT pid,state,wait_event FROM pg_stat_activity WHERE usename='worker_runtime' "
                   "AND state='active' AND wait_event='PgSleep' AND query LIKE 'INSERT INTO execution_receipts%'")
    if len(backend) != 1: return None
    broker = consumer_info(h)
    assert broker['num_ack_pending'] >= 1
    assert broker['delivered']['stream_seq'] >= sequence
    assert retained(h, sequence) == raw
    assert receipt(h, run_id) == []
    assert h.sql('SELECT run_id FROM worker.execution_runs WHERE run_id=' + h.quote(run_id)) == []
    return {'backend': backend[0], 'consumer': broker, 'run_rows': 0, 'receipt_rows': 0}


def before_commit_crash(h, events):
    h.stop_fault_workers()
    victim = h.start_worker('worker-one', {'timing': {'operation_timeout': '15s'}})
    marker = 'intake-precommit-' + uuid.uuid4().hex[:12]
    conversation = str(2100000000 + int(uuid.uuid4().hex[:7], 16))
    before = stream_info(h)
    with before_commit_delay(h, marker, events):
        run_id = h.send_text(marker, conversation_id=conversation)
        source = h.wait(lambda: locate_run_message(h, run_id, before['state']['last_seq']),
                        'original Run publication remains before intake commit', timeout=10)
        observed = h.wait(lambda: pending_before_commit(h, source['sequence'], source['raw'], run_id),
                          'actual receipt INSERT sleeps inside uncommitted intake transaction', timeout=4)
        assert model_calls(h, marker) == []
        h.stop_worker(kill=True)
        assert victim.returncode == -9
        events.append({'event': 'worker_sigkill_before_intake_commit', 'pid': victim.pid,
                       'returncode': victim.returncode, 'run_id': run_id, 'pg_backend_pid': observed['backend']['pid']})
    # Trigger removal waits for the dead client's uncommitted transaction to end.
    assert receipt(h, run_id) == []
    assert h.sql('SELECT run_id FROM worker.execution_runs WHERE run_id=' + h.quote(run_id)) == []
    assert retained(h, source['sequence']) == source['raw']
    published = gateway_publication(h, run_id)
    assert len(published) == 1 and published[0][0] == 't'
    survivor = h.start_worker('worker-two')
    h.wait(lambda: bool(receipt(h, run_id)), 'same Run broker sequence is durably accepted after crash', timeout=60)
    result = wait_success(h, run_id)
    delivery = h.wait_delivery(run_id)
    h.wait(lambda: retained(h, source['sequence']) is None, 'recovered intake has broker ACK', timeout=30)
    assert len(receipt(h, run_id)) == len(attempts(h, run_id)) == 1
    assert len(model_calls(h, marker)) == 1 and run(h, run_id)['session_sequence'] == 1
    assert gateway_publication(h, run_id) == published
    return {'scenario': 'intake-crash-before-commit', 'result': 'PASS', 'run_id': run_id,
            'stream_sequence': source['sequence'], 'raw_sha256': hashlib.sha256(source['raw']).hexdigest(),
            'before_crash': observed, 'victim_pid': victim.pid, 'victim_exit': victim.returncode,
            'survivor_pid': survivor.pid, 'receipt': receipt(h, run_id),
            'attempts': attempts(h, run_id), 'completion': result['completion'], 'delivery': delivery,
            'gateway_publication_unchanged': True, 'model_calls': 1}


@contextmanager
def ack_channel_failure(h, events):
    assert h.broker == h.prefix + '-nats' and h.broker in h.containers
    config = Path(h.nats_config_dir) / 'server.conf'
    assert config.parent.parent == Path(h.work), 'ACK fault must use private fixture configuration'
    original = config.read_text()
    assert original == (Path(h.root) / 'deploy/nats/server.conf').read_text()
    try:
        config.write_text(without_run_ack(original))
        h.command(['docker','kill','--signal=HUP',h.broker])
        # The broker reload emits this explicit acknowledgement. A later negative
        # publish log for the exact Run ACK confirms the permission is effective.
        h.wait(lambda: 'Reloaded: authorization users' in broker_log(h), 'private NATS reload applies ACK outage', timeout=10)
        events.append({'event': 'run_ack_publish_permission_removed', 'stream_topology_changed': False})
        yield
    finally:
        config.write_text(original)
        h.command(['docker','kill','--signal=HUP',h.broker])
        assert config.read_text() == original
        events.append({'event': 'run_ack_publish_permission_restored', 'original_sha256': hashlib.sha256(original.encode()).hexdigest()})


def broker_log(h):
    # docker logs emits normal NATS logs on stderr, unlike Harness.command.
    import subprocess
    p = subprocess.run(['docker','logs',h.broker],capture_output=True,text=True,env=h.env,timeout=10)
    if p.returncode: raise RuntimeError('private NATS log observation failed')
    return h.redact(p.stdout + p.stderr)


def ack_rejection(h, sequence):
    needle = '$JS.ACK.' + STREAM + '.' + DURABLE + '.'
    for line in broker_log(h).splitlines():
        if 'Publish Violation' in line and needle in line:
            subject = line.split(needle, 1)[1].split('"', 1)[0]
            parts = subject.split('.')
            # Ack subject fields after durable: delivered.stream.consumer.time.pending.
            if len(parts) >= 5 and parts[1] == str(sequence):
                return {'result': 'server_rejected_run_ack', 'stream_sequence': sequence}
    return None


def after_commit_ack_failure(h, events, before_crash=None):
    h.stop_fault_workers()
    h.start_worker('worker-one')
    marker = 'intake-ack-' + uuid.uuid4().hex[:12]
    conversation = str(2200000000 + int(uuid.uuid4().hex[:7], 16))
    before = stream_info(h)
    with ack_channel_failure(h, events):
        run_id = h.send_text(marker, conversation_id=conversation)
        source = h.wait(lambda: locate_run_message(h, run_id, before['state']['last_seq']),
                        'Run source remains while ACK publish is unavailable', timeout=10)
        rejected = h.wait(lambda: ack_rejection(h, source['sequence']),
                          'actual broker rejects ACK for exact Run stream sequence', timeout=10)
        accepted = h.wait(lambda: receipt(h, run_id), 'Run and receipt committed before failed ACK', timeout=10)
        result = wait_success(h, run_id)
        delivery = h.wait_delivery(run_id)
        original_attempts = attempts(h, run_id)
        original_completion = completions(h, run_id)
        original_final = outboxes(h, run_id)
        original_candidate = candidates(h, run_id)
        assert retained(h, source['sequence']) == source['raw']
        info = consumer_info(h)
        assert info['num_ack_pending'] >= 1
        published = gateway_publication(h, run_id)
        if before_crash is not None:
            before_crash(run_id)
        victim = h.workers['worker-one']
        h.stop_worker(kill=True)
        assert victim.returncode == -9
        events.append({'event': 'worker_sigkill_after_commit_before_accepted_ack',
                       'pid': victim.pid, 'returncode': victim.returncode, 'run_id': run_id})
    survivor = h.start_worker('worker-two')
    h.wait(lambda: retained(h, source['sequence']) is None, 'original Run receipt replays and ACKs after restart', timeout=60)
    assert receipt(h, run_id) == accepted and attempts(h, run_id) == original_attempts
    assert completions(h, run_id) == original_completion and outboxes(h, run_id) == original_final
    assert candidates(h, run_id) == original_candidate and len(model_calls(h, marker)) == 1
    assert gateway_publication(h, run_id) == published and run(h, run_id)['session_sequence'] == 1
    assert h.wait_delivery(run_id) == delivery
    return {'scenario': 'intake-committed-ack-channel-failure', 'result': 'PASS', 'run_id': run_id,
            'stream_sequence': source['sequence'], 'raw_sha256': hashlib.sha256(source['raw']).hexdigest(),
            'ack_rejection': rejected, 'consumer_before_crash': info, 'victim_pid': victim.pid,
            'victim_exit': victim.returncode, 'survivor_pid': survivor.pid, 'receipt': accepted,
            'attempts': original_attempts, 'completion': result['completion'], 'delivery': delivery,
            'model_calls': 1, 'completion_and_final_unchanged': True, 'gateway_publication_unchanged': True}


def run_intake(h):
    evidence = {'version': 'worker-v1-intake-recovery/v1', 'result': 'FAIL', 'events': [], 'scenarios': []}
    path = Path(h.artifacts) / 'intake-scenarios.json'
    try:
        evidence['scenarios'].append(before_commit_crash(h, evidence['events']))
        evidence['scenarios'].append(after_commit_ack_failure(h, evidence['events']))
        evidence['result'] = 'PASS'
        print('WORKER_INTAKE_RECOVERY=PASS real pre-commit SIGKILL rolls back intake; committed Run survives rejected ACK and restart; same broker sequences recover with one model/Session/Final', flush=True)
        return evidence
    finally:
        h.stop_fault_workers()
        path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
        assert json.loads(path.read_text()) == evidence
