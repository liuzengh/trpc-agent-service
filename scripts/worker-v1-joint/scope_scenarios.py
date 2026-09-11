"""WV-09/10: public Revision/Binding changes and actual SessionScope execution.

All business writes use Control HTTP and Gateway webhooks. SQL only observes
published projections, frozen Run scopes and immutable Session candidates.
Call before live credential clear. Existing h identity/target fields are retained.
"""
from __future__ import annotations
import argparse
import copy
import json
from pathlib import Path
import uuid
import time

from faults import one, rows, run, model_calls, request_text, wait_success


def assert_transcript(request, users):
    messages=request.get('messages', [])
    actual_users=[request_text(m.get('content')) for m in messages if m.get('role')=='user']
    actual_assistants=[request_text(m.get('content')) for m in messages if m.get('role')=='assistant']
    assert actual_users==users, 'SDK user history crossed or lost a SessionScope'
    assert actual_assistants==['joint answer: '+text for text in users[:-1]], 'SDK assistant history crossed or lost a SessionScope'


def binding_path(h):
    return '/v1/tenants/'+h.tenant_id+'/channel-bindings/'+h.gateway.binding_id


def await_route(h, view):
    binding=view['binding']; expected=view['route_generation']
    def applied():
        result=rows(h,"SELECT generation,enabled,snapshot FROM gateway.gateway_route_projections WHERE provider='telegram' AND account_id="+h.quote(h.gateway.account_id))
        if len(result)!=1 or result[0]['generation']!=expected or result[0]['enabled']!=binding['enabled']:
            return False
        if binding['enabled']:
            target=binding['target']; actual=result[0]['snapshot']
            if actual.get('binding_id')!=binding['binding_id'] or actual.get('deployment_revision_id')!=target['deployment_revision_id'] or actual.get('manifest_digest')!=target['manifest_digest']:
                return False
        return result[0]
    return h.wait(applied, 'actual Gateway route projection matches public Binding revision',timeout=40)


def change_binding(h, *, revision=None, enabled=None):
    view=h.api('GET',binding_path(h)); binding=view['binding']
    if (revision is None)==(enabled is None):raise ValueError('one Binding change required')
    if revision is not None:
        action='target'; body={'expected_binding_revision':binding['binding_revision'],
            'target':{'deployment_id':binding['target']['deployment_id'],'revision_number':revision}}
    else:
        action='enabled'; body={'expected_binding_revision':binding['binding_revision'],'enabled':enabled}
    changed=h.api('POST',binding_path(h)+'/'+action,body,idem='scope-'+uuid.uuid4().hex)
    await_route(h,changed)
    return changed


def scope_round(h, text, conversation, view, users, previous=None, *, thread_id=None, chat_type='private'):
    run_id=h.gateway.send_text(text,conversation_id=conversation,thread_id=thread_id,chat_type=chat_type)
    h.wait(lambda: bool(rows(h,'SELECT run_id FROM worker.execution_runs WHERE run_id='+h.quote(run_id))), 'scope Worker intake',timeout=30)
    result=wait_success(h,run_id); delivery=h.wait_delivery(run_id)
    fact=one(h,'SELECT r.request_json,r.session_id,r.session_sequence,s.scope_json FROM worker.execution_runs r JOIN worker.execution_sessions s USING(tenant_id,session_id) WHERE r.run_id='+h.quote(run_id))
    target=view['binding']['target']; expected_scope=[h.tenant_id,'telegram',h.gateway.account_id,str(conversation),'' if thread_id is None else str(thread_id),h.gateway.binding_id,target['deployment_revision_id']]
    assert fact['scope_json']==expected_scope
    route=fact['request_json']['Route']
    assert route['Generation']==view['route_generation'] and route['ManifestRef']==target['manifest_ref'] and route['ManifestDigest']==target['manifest_digest']
    calls=model_calls(h,text); assert len(calls)==1
    assert_transcript(calls[0],users)
    candidate=result['candidate']
    if previous is None:
        assert fact['session_sequence']==1 and candidate['parent_ref']=='' and candidate['parent_digest']==''
    else:
        assert fact['session_id']==previous['session_id'] and fact['session_sequence']==previous['session_sequence']+1
        assert candidate['parent_ref']==previous['completion']['candidate_ref'] and candidate['parent_digest']==previous['completion']['candidate_digest']
    assert delivery['delivery_state']=='ACCEPTED' and delivery['final_text']=='joint answer: '+text
    snapshot=json.dumps(candidate['content'],ensure_ascii=False)
    for expected in users:assert expected in snapshot, 'formal snapshot omitted accepted history'
    assert delivery['intent_id']==result['completion']['final_intent_id']
    return {'run_id':run_id,'session_id':fact['session_id'],'session_sequence':fact['session_sequence'],
            'scope':fact['scope_json'],'route':route,'sdk_request':calls[0],
            'candidate_parent_ref':candidate['parent_ref'],'candidate_parent_digest':candidate['parent_digest'],
            'completion':result['completion'],'delivery':delivery}



def ignored_topic(h, text, conversation, thread_id):
    # Current Gateway V1 admits only actual private-chat text. A genuine forum
    # topic is sent as a forum topic and must not be disguised as private input.
    event=str(time.time_ns()//1000000)
    message={'message_id':int(event)%1000000000+1,'date':int(time.time()),
             'chat':{'id':int(conversation),'type':'supergroup','is_forum':True},
             'from':{'id':100,'is_bot':False},'text':text,
             'message_thread_id':thread_id,'is_topic_message':True}
    raw=json.dumps({'update_id':int(event),'message':message}).encode()
    code,_=h.gateway._webhook(raw,h.gateway_fixture.secret)
    assert code==200
    receipt=one(h,"SELECT receipt FROM gateway.gateway_inbox WHERE provider='telegram' AND account_id="+h.quote(h.gateway.account_id)+' AND event_id='+h.quote(event))['receipt']
    assert receipt['decision']=='ignore' and not receipt.get('run_id') and not receipt.get('admission_id')
    assert h.sql('SELECT count(*) FROM gateway.gateway_admissions WHERE account_id='+h.quote(h.gateway.account_id)+' AND event_id='+h.quote(event))==[['0']]
    assert h.sql("SELECT count(*) FROM worker.execution_runs WHERE request_json->'Input'->>'Text'="+h.quote(text))==[['0']]
    assert model_calls(h,text)==[]
    assert h.gateway._webhook(raw,h.gateway_fixture.secret)[0]==200
    again=one(h,"SELECT receipt FROM gateway.gateway_inbox WHERE provider='telegram' AND account_id="+h.quote(h.gateway.account_id)+' AND event_id='+h.quote(event))['receipt']
    assert again==receipt
    return {'event_id':event,'chat_type':'supergroup','conversation_id':conversation,
            'thread_id':str(thread_id),'http_status':code,'receipt':receipt,
            'admission_count':0,'worker_run_count':0,'sdk_calls':0,'duplicate_receipt_unchanged':True,
            'result':'IGNORED_BY_CURRENT_GATEWAY_V1'}

def run_scope(h):
    evidence={'version':'worker-v1-session-scope/v1','rounds':[],'route_changes':[],
              'coverage':['WV-09 same private chat / different DeploymentRevision','WV-10 revision switch-back and RouteGeneration-only refresh','real forum-topic ingress is durably ignored by the current Gateway V1'],
              'not_exercised':['different Tenant','different Account: existing fixture has one authenticated bot identity','different Thread actual Session execution: Gateway V1 intentionally admits private-chat text only']}
    path=Path(h.artifacts)/'worker-session-scope.json'
    original=h.api('GET',binding_path(h)); binding=original['binding']; original_target=copy.deepcopy(binding['target'])
    assert binding['enabled'] is True and original_target['deployment_revision_id']==h.revision_id
    fields={name:getattr(h,name) for name in ('tenant_id','deployment_id','revision_id','revision_number','manifest_id','manifest_digest')}
    marker='scope-'+uuid.uuid4().hex[:12]
    private=str(1900000000+int(uuid.uuid4().hex[:7],16))
    group=str(-1000000000000-int(uuid.uuid4().hex[:7],16))
    try:
        await_route(h,original)
        seed=scope_round(h,marker+'-rev1',private,original,[marker+'-rev1']); evidence['rounds'].append(seed)
        base='/v1/tenants/'+h.tenant_id+'/deployments/'+h.deployment_id
        old_revision=h.api('GET',base+'/revisions/'+str(original_target['revision_number']))
        source=old_revision['input']
        publication=h.api('POST',base+'/revisions',{'expected_latest_revision_number':original_target['revision_number'],'input':source},status=201,idem=marker+'-publish-rev2')
        revision=publication['revision']; assert revision['revision_number']==original_target['revision_number']+1 and revision['id']!=h.revision_id
        evidence['publication']=publication
        h.wait(lambda: h.sql('SELECT content_digest FROM worker.runtime_manifests WHERE tenant_id='+h.quote(h.tenant_id)+' AND manifest_id='+h.quote(revision['manifest_id']))==[[revision['manifest_digest']]],'real Manifest Relay delivers second published revision',timeout=40)
        switched=change_binding(h,revision=revision['revision_number']); evidence['route_changes'].append(switched)
        new=scope_round(h,marker+'-rev2',private,switched,[marker+'-rev2']); evidence['rounds'].append(new)
        assert new['session_id']!=seed['session_id']
        back=change_binding(h,revision=original_target['revision_number']); evidence['route_changes'].append(back)
        resumed=scope_round(h,marker+'-rev1-return',private,back,[marker+'-rev1',marker+'-rev1-return'],seed); evidence['rounds'].append(resumed)
        disabled=change_binding(h,enabled=False); evidence['route_changes'].append(disabled)
        refreshed=change_binding(h,enabled=True); evidence['route_changes'].append(refreshed)
        assert refreshed['route_generation']>back['route_generation']>switched['route_generation']>original['route_generation']
        same=scope_round(h,marker+'-generation-refresh',private,refreshed,[marker+'-rev1',marker+'-rev1-return',marker+'-generation-refresh'],resumed); evidence['rounds'].append(same)
        evidence['thread_ingress_boundary']=[ignored_topic(h,marker+'-topic41',group,41),ignored_topic(h,marker+'-topic42',group,42)]
        assert same['session_id']==resumed['session_id']==seed['session_id'] and new['session_id']!=seed['session_id']
        evidence['result']='PASS'
        print('WORKER_SESSION_SCOPE=PASS real revision 1->2->1 history isolation/reuse; RouteGeneration refresh retains history; genuine forum topics durably ignored with zero Run/SDK',flush=True)
        return evidence
    except BaseException:
        evidence['result']='FAIL'
        raise
    finally:
        try:
            current=h.api('GET',binding_path(h))
            if current['binding']['target']!=original_target:
                current=change_binding(h,revision=original_target['revision_number'])
            if current['binding']['enabled']!=binding['enabled']:
                current=change_binding(h,enabled=binding['enabled'])
            await_route(h,current)
            assert current['binding']['target']==original_target and current['binding']['enabled']==binding['enabled']
            assert all(getattr(h,key)==value for key,value in fields.items())
            evidence['restoration']={'original_target':original_target,'restored_target':current['binding']['target'],'enabled':current['binding']['enabled'],'route_generation':current['route_generation'],'h_fields_unchanged':True}
        finally:
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
        h.provision();h.control_start(gateway_fixture.prepare(h));h.seed();h.start_worker();h.verify_dependencies()
        h.gateway=gateway_fixture.start(h)
        run_scope(h)
    finally:h.close()


if __name__=='__main__':main()
