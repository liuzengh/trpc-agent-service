"""WV-06: a real missing Manifest increment, rebuilt only from its Control owner.

The private fixture destroys exactly one stream resource and recreates it using
unchanged production topology. MaxAge=0, DiscardNew, DenyDelete/DenyPurge stay
intact. This is Manifest-source loss, NOT timed retention or whole broker loss.
Other streams, services and durable PG facts survive. SQL is read-only evidence.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
import socket
import ssl
import time
from urllib.parse import urlparse
import uuid

from deadline_scenarios import restore_standard
from faults import (attempts, candidates, completions, head, model_calls, one,
                    outboxes, parse_time, run, submit, wait_success)
from manifest_scenarios import (STREAM, DURABLE, SUBJECT, EXPORT, broker_state,
    change_binding, owner_export, projection, publish, receipts, rows)

OTHER_SOURCES = {'CHANNEL_ROUTES_V1': 'channel-gateway-routes-v1',
                 'RUN_REQUESTS_V1': 'agent-worker-runs-v1',
                 'REPLY_INTENTS_V1': 'channel-gateway-replies-v1'}
DELETE = '$JS.API.STREAM.DELETE.' + STREAM


def lifecycle_request(h, subject, body):
    """Only one resource-delete target exists; no arbitrary mutation API."""
    allowed = {DELETE, '$JS.API.STREAM.INFO.'+STREAM, '$JS.API.STREAM.MSG.GET.'+STREAM,
               '$JS.API.CONSUMER.INFO.'+STREAM+'.'+DURABLE}
    for stream, durable in OTHER_SOURCES.items():
        allowed.update({'$JS.API.STREAM.INFO.'+stream, '$JS.API.CONSUMER.INFO.'+stream+'.'+durable})
    if subject not in allowed or (subject == DELETE and body != {}):
        raise ValueError('only the fixed private Manifest stream lifecycle is writable')
    assert h.broker == h.prefix+'-nats' and h.broker in h.containers
    url = urlparse(h.nats_url)
    assert url.scheme == 'tls' and url.hostname in ('127.0.0.1', '::1')
    sock = socket.create_connection((url.hostname, url.port), timeout=8)
    reader = None
    try:
        reader = sock.makefile('rb')
        assert reader.readline(4097).startswith(b'INFO ')
        reader.close(); reader = None
        sock = ssl.create_default_context(cafile=h.certs['ca']).wrap_socket(sock, server_hostname=url.hostname)
        reader = sock.makefile('rb')
        inbox = '_INBOX.reconciler.manifest-gap.'+uuid.uuid4().hex
        connect = {'verbose': False, 'pedantic': True, 'tls_required': True, 'user': 'reconciler',
                   'pass': h.env['NATS_RECONCILER_PASSWORD'], 'name': 'manifest-gap-fixture',
                   'lang': 'python', 'version': '1', 'protocol': 1}
        payload = json.dumps(body).encode()
        sock.sendall(b'CONNECT '+json.dumps(connect).encode()+b'\r\nSUB '+inbox.encode()+b' 1\r\nPUB '+
                     subject.encode()+b' '+inbox.encode()+b' '+str(len(payload)).encode()+b'\r\n'+payload+b'\r\nPING\r\n')
        from reply_scenarios import _read_response
        return _read_response(reader, sock)
    finally:
        if reader is not None: reader.close()
        sock.close()


def source(h):
    observed = broker_state(h)  # Includes assertions for all fixed protective flags.
    stream = lifecycle_request(h, '$JS.API.STREAM.INFO.'+STREAM, {})
    durable = lifecycle_request(h, '$JS.API.CONSUMER.INFO.'+STREAM+'.'+DURABLE, {})
    observed.update(config=stream['config'], consumer_config=durable['config'])
    return observed


def other_sources(h):
    result = {}
    for stream, durable in OTHER_SOURCES.items():
        s = lifecycle_request(h, '$JS.API.STREAM.INFO.'+stream, {})
        c = lifecycle_request(h, '$JS.API.CONSUMER.INFO.'+stream+'.'+durable, {})
        result[stream] = {'stream_created': s['created'], 'durable_created': c['created'],
                          'config': s['config'], 'consumer_config': c['config']}
    return result


def assert_recreated(before, after):
    assert before['messages'] > 0
    assert before['stream_created'] != after['stream_created']
    assert before['durable_created'] != after['durable_created']
    assert before['config'] == after['config'], 'stream loss must not weaken production topology'
    assert before['consumer_config'] == after['consumer_config']
    assert after['messages'] == after['pending'] == after['ack_pending'] == 0


def assert_worker_export(events, ordinal, started_ns):
    assert events, 'new Worker must actually call the mTLS Control owner export'
    for event in events:
        assert event['method'] == 'GET' and event['path'] == EXPORT
        assert event['principal_uri'] == 'spiffe://agent-platform/worker/one'
        assert event['request_ordinal'] > ordinal and event['started_ns'] >= started_ns
        assert event.get('finished_ns') and event['finished_ns'] >= event['started_ns']
        assert event.get('upstream_status') == event.get('downstream_status') == 200
        assert event.get('downstream_action') == 'forwarded'
        assert event['owner_response_bytes'] > 0
        assert event['owner_response_bytes'] == event['downstream_body_bytes_forwarded']


def start_with_export(h, relay):
    ordinal = max((e['request_ordinal'] for e in relay.events()), default=0)
    started_ns = time.monotonic_ns()
    process = h.start_worker()
    def observed():
        return [e for e in relay.events() if e['request_ordinal'] > ordinal and e['path'] == EXPORT]
    h.wait(lambda: bool(observed()) and all(e.get('finished_ns') for e in observed()),
           'new Worker actual Owner export response finished', timeout=10)
    events = observed()
    assert_worker_export(events, ordinal, started_ns)
    return process, {'started_ns': started_ns, 'previous_request_ordinal': ordinal, 'events': events}


def run_record(h, run_id):
    return one(h, 'SELECT tenant_id,run_id,admission_id,request_digest,request_json,session_id,session_sequence,'
               'status,attempts,current_attempt_id,policy_json,accepted_at,run_deadline,reply_deadline,execution_deadline '
               'FROM worker.execution_runs WHERE run_id='+h.quote(run_id))


def outbox_record(h, deployment_id):
    return rows(h, 'SELECT id AS event_id,status,published_at,attempt_count,payload_digest FROM control.control_outbox '
                'WHERE aggregate_id='+h.quote(deployment_id))


def assert_fixed_run(before, after):
    for key in ('tenant_id','run_id','admission_id','request_digest','request_json','session_id','session_sequence',
                'policy_json','accepted_at','run_deadline','reply_deadline','execution_deadline'):
        assert before[key] == after[key], 'source loss changed durable Run field '+key


def _scenario(h, evidence):
    relay = h.resolve_proxies[0]
    assert relay.name == 'control-resolve'
    marker = 'manifest-gap-'+uuid.uuid4().hex[:10]
    conversation = str(1900000000+int(uuid.uuid4().hex[:6],16))
    text, fresh = marker+'-durable-unfinished', marker+'-new-manifest'
    original = h.api('GET','/v1/tenants/'+h.tenant_id+'/channel-bindings/'+h.gateway.binding_id)['binding']['target']
    h.stop_fault_workers()
    initial = h.start_worker()
    control_pid, gateway_pid = h.control.pid, h.gateway.process.pid
    unchanged = other_sources(h)
    # Retain all pre-existing completed facts, including the two baseline Runs.
    settled_before = rows(h, 'SELECT row_to_json(c) AS completion FROM worker.execution_completions c ORDER BY run_id')
    model_before = len(h.model.requests)
    sends_before = len(h.gateway_fixture.snapshot())
    sessions_before = rows(h, 'SELECT row_to_json(s) AS session FROM worker.execution_sessions s ORDER BY tenant_id,session_id')
    before_projection, before_receipts = projection(h), receipts(h)
    before_source = source(h)
    assert before_source['pending'] == before_source['ack_pending'] == 0
    h.model.hold(text)
    try:
        run_id = submit(h,text,conversation)
        evidence['unfinished_run_id'] = run_id
        h.model.wait_entered(text)
        h.wait(lambda: len(attempts(h,run_id)) == 1 and attempts(h,run_id)[0]['status'] == 'EXECUTING',
               'actual unfinished Attempt executing before source loss')
        durable_before = run_record(h,run_id)
        old_attempt = attempts(h,run_id)[0]
        accepted_before = head(h,run(h,run_id))
        assert old_attempt['worker_id'] == 'worker-one' and len(model_calls(h,text)) == 1
        assert candidates(h,run_id) == completions(h,run_id) == outboxes(h,run_id) == []
        h.stop_worker(kill=True)
        assert initial.poll() == -9
        killed_at_ns = time.monotonic_ns()
        stopped_run = run_record(h,run_id)
        stopped_attempts = attempts(h,run_id)
        evidence['killed_worker'] = {'pid':initial.pid,'exit_code':initial.poll(),'signal':'SIGKILL','observed_exit_ns':killed_at_ns,
                                     'attempt':stopped_attempts[0]}
        assert stopped_run == durable_before and stopped_run['status'] == 'RUNNING'
        publication = publish(h,marker,0)
        published = outbox_record(h,publication['deployment_id'])
        assert len(published) == 1 and published[0]['status'] == 'PUBLISHED' and published[0]['published_at']
        retained = source(h)
        assert retained['last_sequence'] == before_source['last_sequence']+1 and retained['pending'] == 1
        sequence = retained['last_sequence']
        message = lifecycle_request(h,'$JS.API.STREAM.MSG.GET.'+STREAM,{'seq':sequence})['message']
        raw = base64.b64decode(message['data'],validate=True)
        event = json.loads(raw)
        raw_path = Path(h.artifacts)/'manifest-gap-lost-event.json'
        raw_path.write_bytes(raw)
        assert raw_path.read_bytes() == raw
        assert message['seq'] == sequence and message['subject'] == SUBJECT
        assert event['event_id'] == published[0]['event_id']
        assert event['manifest']['manifest_id'] == publication['manifest_id']
        assert event['manifest']['content_digest'] == publication['manifest_digest']
        assert projection(h) == before_projection and receipts(h) == before_receipts
        evidence.update(publication=publication,control_outbox_published=published,
            old_source=retained,lost_increment={'sequence':sequence,'event_id':event['event_id'],
            'manifest_id':publication['manifest_id'],'content_digest':publication['manifest_digest'],
            'raw_sha256':hashlib.sha256(raw).hexdigest(),'raw_bytes':len(raw),'raw_artifact':str(raw_path)},other_sources_before=unchanged,
            durable_run_before_loss=stopped_run,projection_before=before_projection,receipts_before=before_receipts)
        result = lifecycle_request(h,DELETE,{})
        assert result.get('success') is True and 'error' not in result
        missing = lifecycle_request(h,'$JS.API.STREAM.INFO.'+STREAM,{})
        assert missing.get('error',{}).get('err_code') == 10059  # JSStreamNotFoundErr
        evidence['resource_loss'] = {'subject':DELETE,'response':result,'missing_source_response':missing}
        h.wait(h._reconcile,'recreate only Manifest source with unchanged production topology',timeout=20)
        recreated = source(h)
        assert_recreated(retained,recreated)
        absent = lifecycle_request(h,'$JS.API.STREAM.MSG.GET.'+STREAM,{'seq':sequence})
        assert absent.get('error',{}).get('err_code') == 10037  # JSNoMessageFoundErr
        assert other_sources(h) == unchanged
        assert run_record(h,run_id) == stopped_run and attempts(h,run_id) == stopped_attempts
        assert head(h,run(h,run_id)) == accepted_before
        assert outbox_record(h,publication['deployment_id']) == published
        # This direct audit is before the ordinal/start barrier; it cannot be
        # mistaken for the subsequent real Worker's startup GETs.
        export_audit, events = owner_export(h)
        assert events[event['event_id']] == event
        assert projection(h) == before_projection and receipts(h) == before_receipts
        evidence.update(recreated_source=recreated,old_sequence_after_loss=absent,
                        durable_run_after_loss=run_record(h,run_id),owner_export_audit=export_audit)
        recovered, startup_export = start_with_export(h,relay)
        projected, received = projection(h), receipts(h)
        assert len(projected) == len(before_projection)+1 and len(received) == len(before_receipts)+1
        assert all(p in projected for p in before_projection) and all(r in received for r in before_receipts)
        added = [p for p in projected if p['manifest_id'] == publication['manifest_id']]
        assert len(added) == 1 and not added[0]['conflicted'] and added[0]['content_digest'] == publication['manifest_digest']
        assert len([r for r in received if r['event_id'] == event['event_id'] and r['manifest_id'] == publication['manifest_id']
                    and r['event_digest'] == published[0]['payload_digest']]) == 1
        assert source(h)['messages'] == 0, 'rebuild must not be explained by any retained or republished increment'
        h.wait(lambda: len(model_calls(h,text)) == 2,'fresh actual SDK request for durable Run recovery',timeout=25)
        current = attempts(h,run_id)
        assert len(current) == 2 and current[0]['attempt_id'] != current[1]['attempt_id']
        assert parse_time(current[1]['created_at']) >= parse_time(stopped_attempts[0]['lease_until'])
        assert current[1]['status'] == 'EXECUTING' and recovered.pid != initial.pid
        assert_fixed_run(stopped_run,run_record(h,run_id))
        h.model.release(text)
        recovered_result = wait_success(h,run_id)
        old_delivery = h.wait_delivery(run_id)
        assert old_delivery['delivery_state'] == 'ACCEPTED'
        assert len(model_calls(h,text)) == 2
        accepted_after = head(h,run(h,run_id))
        assert accepted_after['accepted_ref'] == recovered_result['candidate']['candidate_ref']
        assert accepted_after['accepted_digest'] == recovered_result['candidate']['content_digest']
        evidence.update(accepted_before=accepted_before,accepted_after=accepted_after,owner_rebuild_startup=startup_export,worker_recovery_pid=recovered.pid,
                        projection_rebuilt=projected,receipts_rebuilt=received,
                        recovered_run=run_record(h,run_id),recovered_attempts=attempts(h,run_id),
                        recovered_result=recovered_result,recovered_delivery=old_delivery)
        target = change_binding(h,publication,marker+'-target')
        fresh_id = submit(h,fresh,conversation)
        fresh_result = wait_success(h,fresh_id)
        fresh_delivery = h.wait_delivery(fresh_id)
        assert fresh_delivery['delivery_state'] == 'ACCEPTED' and len(model_calls(h,fresh)) == 1
        assert h.sql("SELECT request_json->'Route'->>'ManifestRef' FROM worker.execution_runs WHERE run_id="+h.quote(fresh_id)) == [[target['manifest_ref']]]
        # Read the actual Admission route as independent public-path confirmation.
        assert h.sql("SELECT route->>'manifest_ref' FROM gateway.gateway_admissions WHERE run_id="+h.quote(fresh_id)) == [[publication['manifest_id']]]
        settled_after = rows(h, 'SELECT row_to_json(c) AS completion FROM worker.execution_completions c ORDER BY run_id')
        assert all(old in settled_after for old in settled_before) and len(settled_after) == len(settled_before)+2
        facts = {'old':run_record(h,run_id),'new':run_record(h,fresh_id),
                 'old_completion':completions(h,run_id),'new_completion':completions(h,fresh_id)}
        h.stop_worker()
        assert recovered.poll() == 0
        second, second_export = start_with_export(h,relay)
        second_projection, second_receipts = projection(h), receipts(h)
        second_facts = {'old':run_record(h,run_id),'new':run_record(h,fresh_id),
                        'old_completion':completions(h,run_id),'new_completion':completions(h,fresh_id)}
        assert second.pid != recovered.pid and second_projection == projected and second_receipts == received
        assert facts == second_facts
        final_source = source(h)
        assert_recreated(retained,final_source)
        assert final_source['stream_created'] == recreated['stream_created']
        assert final_source['durable_created'] == recreated['durable_created']
        published_after = outbox_record(h,publication['deployment_id'])
        assert published_after == published and other_sources(h) == unchanged
        assert h.control.pid == control_pid and h.control.poll() is None
        assert h.gateway.process.pid == gateway_pid and h.gateway.ready()
        assert len(h.model.requests)-model_before == 3
        assert len(h.gateway_fixture.snapshot())-sends_before == 2
        sessions_after = rows(h, 'SELECT row_to_json(s) AS session FROM worker.execution_sessions s ORDER BY tenant_id,session_id')
        assert all(old in sessions_after for old in sessions_before)
        evidence.update(fresh_run_id=fresh_id,fresh_run=facts['new'],fresh_result=fresh_result,fresh_delivery=fresh_delivery,
                        second_startup_export=second_export,second_worker_pid=second.pid,new_source_final=final_source,
                        second_restart_projection=second_projection,second_restart_receipts=second_receipts,
                        second_restart_before_run_facts=facts,second_restart_facts=second_facts,
                        other_sources_after=other_sources(h),preexisting_completions_before=settled_before,
                        preexisting_completions_after=settled_after,
                        preexisting_completions_unchanged=True,preexisting_sessions_before=sessions_before,
                        preexisting_sessions_after=sessions_after,
                        preexisting_sessions_unchanged=True,control_outbox_after=published_after,
                        control_pid=control_pid,gateway_pid=gateway_pid,model_http_delta=3,
                        model_requests_added=copy.deepcopy(h.model.requests[model_before:]),
                        unique_new_completions=2,accepted_deliveries=2,external_send_delta=2,projection_rows_added=1,receipts_added=1)
    finally:
        h.model.release(text)
        change_binding(h,original,marker+'-restore')
        evidence['binding_restored'] = True


def restore_ready(h, evidence):
    evidence['restoration'] = restore_standard(h)
    assert evidence['restoration']['ready'] is True, 'standard Worker restoration did not remain ready'


def run_manifest_gap(h):
    evidence = {'criterion':'WV-06','version':'worker-v1-manifest-gap-rebuild/v1','result':'PENDING',
        'coverage':{'missing_increment_owner_rebuild':True,'whole_broker_loss':False,'timed_retention_expiry':False,
                    'product_business_sql_mutation':False,'production_topology_changed':False}}
    path = Path(h.artifacts)/'manifest-gap-rebuild.json'
    try:
        _scenario(h,evidence)
        evidence['result'] = 'PASS'
    except BaseException:
        evidence['result'] = 'FAIL'
        raise
    finally:
        try:
            restore_ready(h,evidence)
        except BaseException:
            evidence['result'] = 'FAIL'
            raise
        finally:
            path.write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+'\n')
            assert json.loads(path.read_text()) == evidence
    print('WORKER_MANIFEST_GAP=PASS real lost Manifest source + owner-only rebuild + durable Run recovery + idempotent restart',flush=True)
    return evidence
