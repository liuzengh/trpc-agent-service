"""WV-06 owner export, retained increment overlap, empty and offline recovery.

run_manifest(h, phase='empty') runs after Control starts and BEFORE seed().
run_manifest(h) runs after seed(), with Gateway available and credentials active.
Both use real binaries, public Control writes, actual mTLS owner export and the
unchanged production NATS topology. SQL is evidence-only. Broker APIs are reads.
MaxAge=0/DiscardNew means there is no time-expiring retention window here; this
scenario explicitly does not claim broker-loss or beyond-retention recovery.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import socket
import ssl
import sys
import time
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
import uuid

sys.dont_write_bytecode = True
STREAM = 'RUNTIME_MANIFESTS_V1'
DURABLE = 'worker-manifests-v1'
SUBJECT = 'control.runtime-manifest.published.v1'
EXPORT = '/internal/v1/runtime-manifests/export'


def validate_page(page, upper=None, first=True):
    assert set(page) == {'schema_version','snapshot_upper','events','next_cursor','complete'}
    assert page['schema_version']=='v1' and isinstance(page['events'],list) and len(page['events'])<=4
    assert type(page['complete']) is bool and isinstance(page['next_cursor'],str)
    current=page['snapshot_upper']
    if not first:assert current==upper, 'owner export snapshot upper changed during pagination'
    if current is None:
        assert page['complete'] and page['events']==[] and page['next_cursor']==''
    else:
        assert set(current)=={'created_at','tenant_id','event_id'} and all(current.values())
        if page['complete']:assert page['next_cursor']==''
        else:assert page['next_cursor'] and page['events']
    return current


def assert_same_incarnation(before, after):
    for key in ('stream_created','durable_created'):
        assert before[key]==after[key], 'recovery replaced retained broker identity'


def broker_request(h, subject, body):
    allowed={'$JS.API.STREAM.INFO.'+STREAM,
             '$JS.API.CONSUMER.INFO.'+STREAM+'.'+DURABLE,
             '$JS.API.STREAM.MSG.GET.'+STREAM}
    if subject not in allowed:raise ValueError('Manifest fixture broker access is read-only')
    url=urlparse(h.nats_url)
    assert url.scheme=='tls' and url.hostname in ('127.0.0.1','::1')
    sock=socket.create_connection((url.hostname,url.port),timeout=8)
    reader=None
    try:
        reader=sock.makefile('rb')
        assert reader.readline(4097).startswith(b'INFO ')
        reader.close();reader=None
        sock=ssl.create_default_context(cafile=h.certs['ca']).wrap_socket(sock,server_hostname=url.hostname)
        reader=sock.makefile('rb')
        inbox='_INBOX.reconciler.manifest-scenarios.'+uuid.uuid4().hex
        connect={'verbose':False,'pedantic':True,'tls_required':True,'user':'reconciler',
                 'pass':h.env['NATS_RECONCILER_PASSWORD'],'name':'manifest-readback',
                 'lang':'python','version':'1','protocol':1}
        payload=json.dumps(body).encode()
        sock.sendall(b'CONNECT '+json.dumps(connect).encode()+b'\r\nSUB '+inbox.encode()+b' 1\r\nPUB '+
                     subject.encode()+b' '+inbox.encode()+b' '+str(len(payload)).encode()+b'\r\n'+payload+b'\r\nPING\r\n')
        # Shared framing helper only; this module's allowlist prevents mutation.
        from reply_scenarios import _read_response
        result=_read_response(reader,sock)
        assert 'error' not in result, 'Manifest observation API returned an error'
        return result
    finally:
        if reader is not None:reader.close()
        sock.close()


def broker_state(h):
    stream=broker_request(h,'$JS.API.STREAM.INFO.'+STREAM,{})
    consumer=broker_request(h,'$JS.API.CONSUMER.INFO.'+STREAM+'.'+DURABLE,{})
    config=stream['config']
    assert config['name']==STREAM and config['retention']=='limits'
    assert config['discard']=='new' and config.get('max_age',0)==0
    assert config['deny_delete'] is True and config['deny_purge'] is True
    assert consumer['name']==DURABLE
    return {'stream_created':stream['created'],'durable_created':consumer['created'],
            'max_age_ns':config.get('max_age',0),'max_bytes':config['max_bytes'],
            'discard':config['discard'],'deny_delete':config['deny_delete'],'deny_purge':config['deny_purge'],
            'messages':stream['state']['messages'],'first_sequence':stream['state']['first_seq'],
            'last_sequence':stream['state']['last_seq'],'pending':consumer['num_pending'],
            'ack_pending':consumer['num_ack_pending'],'delivered':consumer['delivered'],
            'ack_floor':consumer['ack_floor']}


def export_page(h,cursor=''):
    context=ssl.create_default_context(cafile=h.certs['ca'])
    context.load_cert_chain(h.certs['worker_cert'],h.certs['worker_key'])
    query={'limit':4}
    if cursor:query['cursor']=cursor
    request=Request(h.urls['control_runtime']+EXPORT+'?'+urlencode(query))
    with urlopen(request,context=context,timeout=10) as response:
        assert response.status==200 and response.headers.get_content_type()=='application/json'
        raw=response.read(5*1024*1024+1)
    assert len(raw)<=5*1024*1024
    return json.loads(raw)


def owner_export(h,after_first_page=None):
    cursor='';seen=set();upper=None;events={};pages=[]
    for index in range(128):
        page=export_page(h,cursor)
        upper=validate_page(page,upper,first=index==0)
        identities=[]
        for event in page['events']:
            event_id=event['event_id']
            assert event_id not in events, 'owner export duplicated an immutable event'
            events[event_id]=event
            identities.append({'event_id':event_id,'manifest_id':event['manifest']['manifest_id'],
                               'content_digest':event['manifest']['content_digest']})
        pages.append({'http_status':200,'snapshot_upper':upper,'events':identities,
                      'next_cursor_present':bool(page['next_cursor']),'complete':page['complete']})
        if index==0 and after_first_page is not None:after_first_page()
        if page['complete']:return {'pages':pages,'event_count':len(events),'complete':True},events
        next_cursor=page['next_cursor']
        assert next_cursor!=cursor and next_cursor not in seen, 'owner export cursor failed to advance'
        seen.add(next_cursor);cursor=next_cursor
    raise AssertionError('bounded Manifest export did not complete')


def rows(h,query):
    result=h.sql("SELECT COALESCE(json_agg(row_to_json(manifest_q)), '[]'::json)::text FROM ("+query+') manifest_q')
    assert len(result)==1 and len(result[0])==1
    return json.loads(result[0][0])


def projection(h):
    return rows(h,'SELECT tenant_id,manifest_id,deployment_revision_id,content_digest,envelope_digest,conflicted '
                 'FROM worker.runtime_manifests ORDER BY tenant_id,manifest_id')


def receipts(h):
    return rows(h,'SELECT event_id,event_digest,tenant_id,manifest_id FROM worker.manifest_receipts ORDER BY event_id')


def publish(h,marker,index):
    base='/v1/tenants/'+h.tenant_id+'/deployments'
    created=h.api('POST',base,{'name':'Manifest offline '+marker+' '+str(index)},status=201,
                  idem=marker+'-create-'+str(index))['deployment']
    source={'schema_version':'v1','agent':{'agent_id':h.agent_id,'version_number':1},
            'profile':{'profile_id':h.profile_id,'revision_number':1}}
    assert h.api('POST',base+'/'+created['id']+'/validate',source)['valid'] is True
    publication=h.api('POST',base+'/'+created['id']+'/revisions',
                      {'expected_latest_revision_number':None,'input':source},status=201,
                      idem=marker+'-publish-'+str(index))
    h.wait(lambda:h.sql("SELECT status FROM control.control_outbox WHERE aggregate_id="+h.quote(created['id']))==[['PUBLISHED']],
           'actual offline Manifest relay PubAck',timeout=30)
    return publication['revision']


def change_binding(h,target,marker):
    base='/v1/tenants/'+h.tenant_id+'/channel-bindings/'+h.gateway.binding_id
    current=h.api('GET',base)['binding']
    result=h.api('POST',base+'/target',{'expected_binding_revision':current['binding_revision'],
                 'target':{'deployment_id':target['deployment_id'],'revision_number':target['revision_number']}},idem=marker)
    expected=result['binding']['target']
    h.wait(lambda:h.sql("SELECT snapshot->>'manifest_ref' FROM gateway.gateway_route_projections WHERE provider='telegram' AND account_id="+
                        h.quote(h.gateway.account_id))==[[expected['manifest_ref']]],'Gateway applies restored/new fixed Manifest route')
    return expected


def run_manifest(h,phase='offline'):
    if phase not in ('empty','offline'):raise ValueError('unknown Manifest scenario phase')
    evidence={'criterion':'WV-06','phase':phase,'version':'worker-v1-manifest-recovery/v1',
              'coverage':{'empty_collection':phase=='empty','owner_export_increment_overlap':phase=='offline',
                          'offline_retained_source_recovery':phase=='offline','offline_beyond_retention':False,
                          'broker_loss_rebuild':False,
                          'retention_note':'Production requires MaxAge=0, DiscardNew, DenyDelete/DenyPurge; no timed-retention expiry was induced.'}}
    path=Path(h.artifacts)/('manifest-empty.json' if phase=='empty' else 'manifest-recovery.json')
    original=None;started=time.monotonic();marker='manifest-'+uuid.uuid4().hex[:10]
    try:
        if phase=='empty':
            assert not hasattr(h,'tenant_id'), 'empty phase must run before seed/publication'
            before=broker_state(h)
            assert before['messages']==before['pending']==before['ack_pending']==0
            transcript,events=owner_export(h)
            assert transcript['event_count']==0 and events=={}
            assert transcript['pages']==[{'http_status':200,'snapshot_upper':None,'events':[],
                                         'next_cursor_present':False,'complete':True}]
            process=h.start_worker()
            assert process.poll() is None and projection(h)==[] and receipts(h)==[]
            after=broker_state(h);assert_same_incarnation(before,after)
            evidence.update(owner_export=transcript,broker_before=before,broker_after=after,
                            worker_pid=process.pid,worker_ready=True,projection_count=0,receipt_count=0)
            h.stop_worker()
        else:
            original=h.api('GET','/v1/tenants/'+h.tenant_id+'/channel-bindings/'+h.gateway.binding_id)['binding']['target']
            h.stop_fault_workers()
            stopped={name:{'pid':p.pid,'exit_code':p.poll()} for name,p in h.workers.items()}
            assert all(item['exit_code'] is not None for item in stopped.values())
            before=broker_state(h);before_projection=projection(h);before_receipts=receipts(h)
            assert before['pending']==before['ack_pending']==0
            model_before=len(h.model.requests)
            publications=[publish(h,marker,i) for i in range(5)]
            # A true publication commits between owner-export pages. The first
            # snapshot must not expand its upper; the increment durable retains it.
            transcript,events=owner_export(h,lambda:publications.append(publish(h,marker,5)))
            assert len(transcript['pages'])>=2
            during=publications[-1]
            assert during['manifest_id'] not in {event['manifest']['manifest_id'] for event in events.values()}
            full_transcript,full_events=owner_export(h)
            assert {r['manifest_id'] for r in publications}.issubset({e['manifest']['manifest_id'] for e in full_events.values()})
            pending=broker_state(h);assert_same_incarnation(before,pending)
            assert pending['pending']==6 and pending['delivered']==before['delivered']
            assert projection(h)==before_projection and receipts(h)==before_receipts
            retained=[]
            for sequence in range(before['last_sequence']+1,pending['last_sequence']+1):
                message=broker_request(h,'$JS.API.STREAM.MSG.GET.'+STREAM,{'seq':sequence})['message']
                assert message['subject']==SUBJECT
                event=json.loads(base64.b64decode(message['data'],validate=True))
                assert full_events[event['event_id']]==event
                retained.append({'sequence':sequence,'event_id':event['event_id'],'manifest_id':event['manifest']['manifest_id']})
            assert len(retained)==6
            target=change_binding(h,{'deployment_id':during['deployment_id'],'revision_number':during['revision_number']},marker+'-route')
            text=marker+'-queued-while-worker-offline'
            run_id=h.send_text(text,conversation_id=str(1700000000+int(uuid.uuid4().hex[:6],16)))
            h.wait(lambda:h.sql('SELECT o.published_at IS NOT NULL FROM gateway.gateway_outbox o JOIN gateway.gateway_admissions a ON a.admission_id=o.event_id WHERE a.run_id='+h.quote(run_id))==[['t']],
                   'offline Run source PubAck')
            assert h.sql('SELECT count(*) FROM worker.execution_runs WHERE run_id='+h.quote(run_id))==[['0']]
            assert len(h.model.requests)==model_before
            offline_seconds=time.monotonic()-started
            process=h.start_worker()
            recovered=broker_state(h);assert_same_incarnation(before,recovered)
            assert recovered['pending']==recovered['ack_pending']==0
            assert recovered['ack_floor']['stream_seq']>=pending['last_sequence']
            assert recovered['delivered']['consumer_seq']==before['delivered']['consumer_seq']+6
            projected=projection(h);received=receipts(h)
            assert len(projected)==len(before_projection)+6 and len(received)==len(before_receipts)+6
            for publication in publications:
                matches=[row for row in projected if row['manifest_id']==publication['manifest_id']]
                assert len(matches)==1 and matches[0]['content_digest']==publication['manifest_digest'] and not matches[0]['conflicted']
                assert len([row for row in received if row['manifest_id']==publication['manifest_id']])==1
            delivery=h.wait_delivery(run_id)
            assert delivery['delivery_state']=='ACCEPTED'
            assert h.sql("SELECT route->>'manifest_ref' FROM gateway.gateway_admissions WHERE run_id="+h.quote(run_id))==[[target['manifest_ref']]]
            assert len(h.model.requests)==model_before+1
            facts=rows(h,'SELECT r.run_id,r.status,r.attempts,c.completion_id,c.final_intent_id FROM worker.execution_runs r JOIN worker.execution_completions c USING(tenant_id,run_id) WHERE r.run_id='+h.quote(run_id))
            assert len(facts)==1 and facts[0]['status']=='SUCCEEDED' and facts[0]['attempts']==1
            # A second actual process restart repeats owner export over the same
            # retained/ACKed events. Stable receipts/projection/Completion survive.
            h.stop_worker();restarted=h.start_worker()
            after_restart=broker_state(h);assert_same_incarnation(recovered,after_restart)
            assert projection(h)==projected and receipts(h)==received
            assert len(h.model.requests)==model_before+1
            assert rows(h,'SELECT r.run_id,r.status,r.attempts,c.completion_id,c.final_intent_id FROM worker.execution_runs r JOIN worker.execution_completions c USING(tenant_id,run_id) WHERE r.run_id='+h.quote(run_id))==facts
            assert h.gateway.wait_delivery(run_id)==delivery
            evidence.update(stopped_workers=stopped,offline_seconds=offline_seconds,publications=publications,
                            owner_export_overlapping_publication=transcript,owner_export_after_publication=full_transcript,
                            during_export_manifest_id=during['manifest_id'],retained_overlap=retained,
                            broker_before=before,broker_offline=pending,broker_recovered=recovered,broker_second_restart=after_restart,
                            worker_recovery_pid=process.pid,worker_second_restart_pid=restarted.pid,
                            projection_rows_added=6,receipts_added=6,duplicate_projection_rows=0,duplicate_receipts=0,
                            run_facts=facts,model_calls=1,delivery=delivery)
            change_binding(h,original,marker+'-restore');original=None
        evidence['result']='PASS'
        print('WORKER_MANIFEST_'+phase.upper()+'=PASS actual owner export/durable/Worker '+
              ('empty initialization' if phase=='empty' else 'offline overlap and idempotent restart; no timed-retention-expiry claim'),flush=True)
        return evidence
    except BaseException:
        evidence['result']='FAIL';raise
    finally:
        if original is not None:change_binding(h,original,marker+'-restore-after-failure')
        path.write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+'\n')
        assert json.loads(path.read_text())==evidence


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--artifacts',type=Path,required=True)
    parser.add_argument('--race',action='store_true')
    args=parser.parse_args()
    from harness import Harness
    import gateway_fixture
    h=Harness(args.root,args.artifacts,args.race)
    try:
        h.provision();h.control_start(gateway_fixture.prepare(h))
        run_manifest(h,'empty')
        h.seed();h.start_worker();h.gateway=gateway_fixture.start(h)
        run_manifest(h)
        return 0
    finally:h.close()


if __name__=='__main__':raise SystemExit(main())
