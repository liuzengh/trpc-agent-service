#!/usr/bin/env python3
"""Actual public text import, SDK embedding/retrieval, Qdrant and Channel Lab.

This deterministic provider gate is not real external-embedding acceptance.
"""
import argparse
import copy
import json
from pathlib import Path
import sys
import tempfile
import traceback

sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent/'worker-v1-joint'))
from knowledge_joint_fixture import KnowledgeHarness,KnowledgeModelFixture,BACKEND_ID,DOCUMENT_NAME,DOCUMENT_TEXT,OTHER_TEXT,VECTOR_NAME
from harness import assert_model_contract
from faults import run,head,candidates,completions,wait_success,submit
from scope_scenarios import change_binding,binding_path
import channel_lab_fixture as gateway_fixture


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);parser.add_argument('--artifacts',type=Path);parser.add_argument('--race',action='store_true')
    args=parser.parse_args()
    h=KnowledgeHarness(args.root,args.artifacts or Path(tempfile.mkdtemp(prefix='worker-knowledge-joint-')),args.race)
    evidence={'result':'PENDING','embedding_provider':'DETERMINISTIC_HTTP_FIXTURE_NOT_REAL_EXTERNAL_PROVIDER','im':'REAL_CHANNEL_LAB','rounds':[],'imports':[],'failures':[],'isolation':'Tenant/Profile/Resource fixed scope; different Session alone is not Knowledge isolation'}
    print('KNOWLEDGE_JOINT_ARTIFACTS='+str(h.artifacts),flush=True)
    def save():h.record('knowledge-joint.json',evidence)
    def success(text,previous=None):
        before=h.knowledge_state();offset=len(h.model.snapshot());embed_offset=len(h.model.embeddings());out_offset=len(h.model.output_snapshot())
        run_id=submit(h,text,'42');delivery=h.wait_delivery(run_id);result=wait_success(h,run_id)
        actual=run(h,run_id);accepted=head(h,actual)
        assert accepted['accepted_ref']==result['candidate']['candidate_ref'] and accepted['accepted_digest']==result['candidate']['content_digest']
        assert result['candidate']['parent_ref']==(previous['candidate']['candidate_ref'] if previous else '')
        assert result['candidate']['parent_digest']==(previous['candidate']['content_digest'] if previous else '')
        after=h.knowledge_state();assert after==before,'knowledge search mutated Qdrant'
        outputs=h.model.output_snapshot()[out_offset:];assert len(outputs)==1 and outputs[0]['input']==text
        assert delivery['final_text']==outputs[0]['text']
        route=json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id='+h.quote(run_id))[0][0])['Route']
        assert route['ManifestRef']==h.manifest_id and route['ManifestDigest']==h.manifest_digest
        calls=h.model.snapshot()[offset:];embeddings=h.model.embeddings()[embed_offset:]
        assert len(calls)==2 and len(embeddings)==1
        return {'input':text,'run_id':run_id,'run':actual,'head':accepted,'candidate':result['candidate'],'completion':result['completion'],'route':route,'delivery':delivery,'calls':calls,'embedding_requests':embeddings,'tool_results':outputs[0]['tool_results'],'knowledge_before':before,'knowledge_after':after}
    def ingest(name,text,revision=1):
        offset=len(h.model.embeddings());before=h.knowledge_state()
        result=h.import_text(name,text,revision=revision);after=h.knowledge_state();calls=h.model.embeddings()[offset:]
        assert result['documents']==1 and len(calls)==1
        value={'name':name,'text':text,'revision':revision,'response':result,'embedding_requests':calls,'before':before,'after':after}
        evidence['imports'].append(value);save();return value
    try:
        h.provision();h.model.close();h.model=KnowledgeModelFixture(h);h.urls['model']=h.model.url
        h.control_start(gateway_fixture.prepare(h));h.seed();h.start_worker();h.verify_dependencies();h.gateway=gateway_fixture.start(h)
        publication=h.api('GET','/v1/tenants/'+h.tenant_id+'/deployments/'+h.deployment_id+'/revisions/1')
        envelope=json.loads(h.sql("SELECT convert_from(envelope,'UTF8') FROM worker.runtime_manifests WHERE manifest_id="+h.quote(h.manifest_id))[0][0])
        content=envelope['content'];node=content['agent_plan']['nodes'][content['agent_plan']['root']]
        assert node['knowledge_resources']==['docs']
        resource=content['resources']['knowledge']['docs'];assert resource['kind']=='managed_knowledge' and resource['backend']['backend_id']==BACKEND_ID
        assert resource['credential']['purpose']=='qdrant_api_key' and resource['embedding']['credential']['purpose']=='embedding_api_key'
        assert resource['credential']['credential_id']!=resource['embedding']['credential']['credential_id']
        assert resource['backend']['qdrant']['vector_name']==VECTOR_NAME and resource['embedding']['dimensions']==h.model.dimensions
        evidence.update(publication=publication,manifest=envelope,dependency=h.qdrant_config,initial_knowledge=h.knowledge_state())
        assert evidence['initial_knowledge']==[]
        imported=ingest(DOCUMENT_NAME,DOCUMENT_TEXT)
        assert len(imported['after'])==1
        assert DOCUMENT_TEXT in json.dumps(imported['after'][0]['payload'])
        assert imported['after'][0]['vector']=={VECTOR_NAME:[1,0,0]}
        previous=success('knowledge-query');evidence['rounds'].append(previous);save()
        repeated=ingest(DOCUMENT_NAME,DOCUMENT_TEXT)
        assert [p['id'] for p in repeated['after']]==[p['id'] for p in imported['after']]
        previous=success('knowledge-again',previous);evidence['rounds'].append(previous);save()
        before=h.knowledge_state();calls_before=len(h.model.embeddings());h.model.fail_embedding=True
        try:bad=h.import_failure('invalid-empty-vector.txt','this input must not become a Qdrant point')
        finally:h.model.fail_embedding=False
        after=h.knowledge_state();assert before==after and len(h.model.embeddings())==calls_before+1
        evidence['failures'].append({'case':'empty_embedding_rejected','public_response':bad,'before':before,'after':after,'embedding_requests':h.model.embeddings()[calls_before:]});save()
        h.stop_knowledge_storage()
        try:unavailable=h.import_failure('backend-unavailable.txt','unavailable backend must not report import success')
        finally:h.restore_knowledge_storage()
        after=h.knowledge_state();assert after==before
        evidence['failures'].append({'case':'qdrant_unavailable_import_rejected','public_response':unavailable,'before':before,'after':after});save()
        call_offset=len(h.model.snapshot());run_id=submit(h,'knowledge-model-fail','42')
        h.wait(lambda:run(h,run_id)['status']=='FAILED','model failure after actual Knowledge retrieval',timeout=90)
        delivery=h.gateway.wait_delivery(run_id);failed=completions(h,run_id);accepted=head(h,run(h,run_id))
        assert len(failed)==1 and failed[0]['reason']=='RUNTIME_FAILED' and candidates(h,run_id)==[]
        assert accepted['accepted_ref']==previous['candidate']['candidate_ref'] and accepted['accepted_digest']==previous['candidate']['content_digest']
        assert delivery['final_text']=='本次执行未完成，请稍后重试。' and h.knowledge_state()==before
        evidence['failures'].append({'case':'model_failure_after_retrieval','run_id':run_id,'completion':failed,'head':accepted,'delivery':delivery,'candidate_rows':[],'calls':h.model.snapshot()[call_offset:],'knowledge_before':before,'knowledge_after':h.knowledge_state()});save()
        previous=success('knowledge-again',previous)
        assert 'knowledge-model-fail' not in [m.get('content') for m in previous['calls'][0]['messages'] if m.get('role')=='user']
        evidence['rounds'].append(previous);save()
        # A distinct Profile, not just a new Session, selects a distinct fixed scope.
        base='/v1/tenants/'+h.tenant_id
        p2=h.api('POST',base+'/runtime-profiles',{'name':'Isolated Knowledge Profile'},status=201)['profile']
        write=copy.deepcopy(h.last_profile_write);write['expected_draft_revision']=1
        h.api('PUT',base+'/runtime-profiles/'+p2['id']+'/draft',write,idem='knowledge-other-profile-draft')
        h.api('POST',base+'/runtime-profiles/'+p2['id']+'/revisions',{'expected_revision':2},status=201)
        source=copy.deepcopy(publication['input']);source['profile']={'profile_id':p2['id'],'revision_number':1}
        second=h.api('POST',base+'/deployments/'+h.deployment_id+'/revisions',{'expected_latest_revision_number':1,'input':source},status=201,idem='knowledge-other-profile-deployment')
        revision=second['revision'];h.wait(lambda:h.sql('SELECT content_digest FROM worker.runtime_manifests WHERE manifest_id='+h.quote(revision['manifest_id']))==[[revision['manifest_digest']]],'second Profile immutable Knowledge Manifest')
        original=(h.manifest_id,h.manifest_digest);original_binding=h.api('GET',binding_path(h))
        try:
            changed=change_binding(h,revision=2);h.manifest_id,h.manifest_digest=revision['manifest_id'],revision['manifest_digest']
            empty=success('knowledge-empty');assert empty['run']['session_id']!=previous['run']['session_id'];evidence['rounds'].append(empty);save()
            other_import=ingest('marigold-reference.txt',OTHER_TEXT,revision=2)
            assert len(other_import['after'])==2 and len({p['payload']['worker_scope'] for p in other_import['after']})==2
            other=success('knowledge-other',empty);evidence['rounds'].append(other)
            evidence.update(second_profile=p2,second_publication=second,changed_binding=changed);save()
        finally:
            h.manifest_id,h.manifest_digest=original
            restored=change_binding(h,revision=1);assert restored['binding']['target']==original_binding['binding']['target'];evidence['binding_restoration']=restored
        returned=success('knowledge-query',previous);evidence['rounds'].append(returned)
        assert returned['run']['session_id']==previous['run']['session_id']
        assert len(h.knowledge_state())==2
        evidence.update(result='PASS',final_knowledge=h.knowledge_state(),model_requests=h.model.snapshot(),embedding_requests=h.model.embeddings(),model_outputs=h.model.output_snapshot(),model_contract=assert_model_contract(h.model.snapshot(),publication['manifest_view'],h.model_name),external_embedding_verified=False)
        save()
    except BaseException as exc:
        frame=traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL',error=h.redact(str(exc)),error_type=type(exc).__name__,error_location={'file':Path(frame.filename).name,'line':frame.lineno},model_requests=h.model.snapshot() if isinstance(getattr(h,'model',None),KnowledgeModelFixture) else [],embedding_requests=h.model.embeddings() if isinstance(getattr(h,'model',None),KnowledgeModelFixture) else [])
        save();raise RuntimeError(evidence['error']) from None
    finally:
        try:h.close();evidence['cleanup']='PASS'
        except BaseException as exc:evidence.update(result='FAIL',cleanup_error=h.redact(str(exc)));raise
        finally:
            save();leaked=[str(path) for path in h.artifacts.rglob('*') if path.is_file() and any(s.encode() in path.read_bytes() for s in h.secrets if s)]
            evidence['secret_scan']={'result':'FAIL' if leaked else 'PASS','files':leaked}
            if leaked:evidence['result']='FAIL'
            save()
            if evidence['result']!='PASS':raise RuntimeError('Knowledge joint did not pass; inspect redacted evidence')
    print('WORKER_KNOWLEDGE_JOINT=PASS real public import + SDK text/embed/search + Qdrant named vector + Lab; deterministic embedding fixture only, external_embedding_verified=false',flush=True)


if __name__=='__main__':main()
