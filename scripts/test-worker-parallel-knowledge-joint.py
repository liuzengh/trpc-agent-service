#!/usr/bin/env python3
"""Real parallel Knowledge SDK retrieval from two scoped equal-vector resources.

One successful Run through Control/Gateway/Worker, Qdrant and Channel Lab.
Cross-Tenant negative uses owner HTTP denial and read-only fixed-scope Qdrant
probes, not a second Tenant SDK execution. No external embedding/live-model claim.
"""
import argparse
import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import traceback

sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent/'worker-v1-joint'))
from parallel_knowledge_joint_fixture import (ParallelKnowledgeHarness,BRANCHES,ROLES,SLOTS,RESOURCES,MODELS,
    BACKEND_ID,COLLECTION,VECTOR_NAME,TEXTS,NORMAL,QUERY,scope_for,require_points,require_exchange,tenant_controls)
from sequence_joint_fixture import require_accepted_history,all_strings
from faults import submit,run,head,rows,attempts,candidates,completions,outboxes
import channel_lab_fixture as lab

_spec=importlib.util.spec_from_file_location('sequence_delivery_predicate',Path(__file__).with_name('test-worker-sequence-joint.py'))
_sequence=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_sequence)
require_delivery_wire=_sequence.require_delivery_wire


def require_manifest(envelope,h):
    content=envelope['content'];nodes=content['agent_plan']['nodes']
    assert envelope['manifest_id']==h.manifest_id and envelope['content_digest']==h.manifest_digest
    assert content['tenant_id']==h.tenant_id and content['sources']['profile']['profile_id']==h.profile_id
    assert content['agent_plan']['root']=='workflow' and set(nodes)=={'workflow','research',*ROLES}
    assert nodes['workflow']=={'kind':'sequence','children':['research','aggregator']}
    assert nodes['research']=={'kind':'parallel','children':list(BRANCHES)}
    resources=content['resources'];models=resources['models']
    assert set(models)==set(SLOTS.values()) and resources['tools']=={} and set(resources['knowledge'])==set(RESOURCES.values())
    assert len({model['credential']['credential_id'] for model in models.values()})==3
    scopes={}
    for role in ROLES:
        node=nodes[role];model=models[SLOTS[role]]
        assert node['kind']=='llm' and node['model_resource']==SLOTS[role]
        assert model['model']==MODELS[role] and model['base_url']==h.model.url+'/v1'
        assert node['tool_resources']==[]
        if role in BRANCHES:
            resource_id=RESOURCES[role];resource=resources['knowledge'][resource_id]
            assert node['knowledge_resources']==[resource_id] and node['callable_entries']==['knowledge/'+resource_id]
            assert resource['kind']=='managed_knowledge' and resource['capability']=='knowledge.search'
            assert resource['backend']['tenant_id']==h.tenant_id and resource['backend']['backend_id']==BACKEND_ID
            assert resource['backend']['backend_revision']==1
            assert resource['backend']['qdrant']=={'endpoint':h.qdrant_endpoint,'collection':COLLECTION,'vector_name':VECTOR_NAME,'dimensions':3,'distance':'cosine'}
            assert resource['credential']['purpose']=='qdrant_api_key'
            assert resource['embedding']['credential']['purpose']=='embedding_api_key'
            assert resource['credential']['credential_id']!=resource['embedding']['credential']['credential_id']
            assert resource['embedding']['model']==h.model.embedding_model and resource['embedding']['base_url']==h.model.embedding_url+'/v1'
            assert resource['embedding']['dimensions']==3
            scopes[role]=scope_for(h.tenant_id,h.profile_id,resource_id,resource)
        else:
            assert node['knowledge_resources']==node['callable_entries']==[]
    assert resources['knowledge']['docs_a']['backend']==resources['knowledge']['docs_b']['backend']
    assert scopes['research_a']!=scopes['research_b']
    return content,scopes


def require_facts(round_):
    state=round_['run'];attempt_rows=round_['attempts'];comp=round_['completions'];finals=round_['outboxes']
    assert state['attempts']==len(attempt_rows)==len(comp)==len(finals)==1
    assert len(round_['candidates'])==1
    candidate=round_['candidates'][0];completion=comp[0];attempt=attempt_rows[0];final=finals[0]
    assert state['status']==attempt['status']==completion['status']=='SUCCEEDED'
    assert completion['attempt_id']==attempt['attempt_id']==state['current_attempt_id']==candidate['attempt_id']
    assert completion['kind']=='ATTEMPT' and completion['reply_disposition']=='FINAL'
    assert completion['candidate_ref']==candidate['candidate_ref']==round_['head']['accepted_ref']
    assert completion['candidate_digest']==candidate['content_digest']==round_['head']['accepted_digest']
    assert state['session_sequence']==round_['head']['settled_sequence']==1
    assert not candidate['parent_ref'] and not candidate['parent_digest']
    assert completion['final_intent_id']==final['intent_id']==round_['delivery']['intent_id']
    assert final['payload']['execution']['completion_id']==completion['completion_id']
    assert final['payload']['execution']['attempt_id']==attempt['attempt_id']
    assert final['payload']['content']['text']==round_['exchange']['terminal_output']==round_['delivery']['final_text']
    require_accepted_history(None,candidate,[c['request'] for c in round_['model_calls']])
    stored=list(all_strings(candidate['content']['snapshot']))
    for text in [*round_['exchange']['branch_outputs'].values(),round_['exchange']['terminal_output']]:assert text in stored
    assert all(round_['delivery']['final_text']!=text for text in round_['exchange']['branch_outputs'].values())
    require_delivery_wire(round_)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts',type=Path)
    parser.add_argument('--race',action='store_true')
    args=parser.parse_args()
    h=ParallelKnowledgeHarness(args.root,args.artifacts or Path(tempfile.mkdtemp(prefix='parallel-knowledge-')),args.race,model_name=MODELS['research_a'])
    evidence={'result':'PENDING','model_provider':'DETERMINISTIC_HTTP_FIXTURE','embedding_provider':'DETERMINISTIC_HTTP_FIXTURE_EQUAL_VECTORS',
        'external_semantics_verified':False,'im':'REAL_CHANNEL_LAB_SIMULATOR','business_sql_writes':False,'second_tenant_sdk_run':False,'imports':[]}
    def save():h.record('parallel-knowledge-joint.json',evidence)
    print('PARALLEL_KNOWLEDGE_ARTIFACTS='+str(h.artifacts),flush=True)
    try:
        h.provision();h.prepare_parallel_knowledge()
        h.control_start(lab.prepare(h));h.seed(extra_tenants=1)
        h.start_worker();h.verify_dependencies();h.gateway=lab.start(h)
        envelope=rows(h,"SELECT convert_from(envelope,'UTF8')::json AS manifest FROM worker.runtime_manifests WHERE manifest_id="+h.quote(h.manifest_id))[0]['manifest']
        content,scopes=require_manifest(envelope,h)
        evidence.update(manifest=envelope,scopes=scopes,dependency=h.qdrant_config,initial_knowledge=h.knowledge_state())
        assert evidence['initial_knowledge']==[]
        for role in BRANCHES:
            offset=len(h.model.embeddings());before=h.knowledge_state()
            response=h.import_text('parallel-'+RESOURCES[role]+'.txt',TEXTS[role],resource=RESOURCES[role])
            after=h.knowledge_state();embedding_calls=h.model.embeddings()[offset:]
            value={'resource':RESOURCES[role],'text':TEXTS[role],'response':response,'before':before,'after':after,'embedding_requests':embedding_calls}
            evidence['imports'].append(value);save()
            assert response['documents']==1 and len(embedding_calls)==1 and len(after)==len(before)+1
        points=h.knowledge_state();require_points(points,scopes)
        evidence['knowledge_before_run']=points;save()
        offset=len(h.model.snapshot());embedding_offset=len(h.model.embeddings())
        rid=submit(h,NORMAL,'42')
        h.wait(lambda:run(h,rid)['status'] in ('SUCCEEDED','FAILED'),'parallel Knowledge Run settles',timeout=90)
        state=run(h,rid)
        round_={'run_id':rid,'run':state,'head':head(h,state),'attempts':attempts(h,rid),'completions':completions(h,rid),
                'candidates':candidates(h,rid),'outboxes':outboxes(h,rid),'result':'PENDING'}
        evidence['round']=round_;save()
        round_['delivery']=h.gateway.wait_delivery(rid)
        h.wait(lambda:len(h.model.snapshot()[offset:])==5 and all(c.get('complete') for c in h.model.snapshot()[offset:]),'five complete real SDK model HTTP records',timeout=20)
        round_['model_calls']=h.model.snapshot()[offset:]
        round_['embedding_calls']=h.model.embeddings()[embedding_offset:]
        round_['route']=rows(h,"SELECT request_json->'Route' AS route FROM worker.execution_runs WHERE run_id="+h.quote(rid))[0]['route']
        round_['gateway_receipts']=rows(h,'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id='+h.quote(rid))
        round_['gateway_parts']=rows(h,'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id='+h.quote(rid)+' ORDER BY p.part_index')
        round_['knowledge_after']=h.knowledge_state();save()
        h.model.assert_healthy()
        assert len(round_['embedding_calls'])==2 and all(c['input']==QUERY or c['input']==[QUERY] for c in round_['embedding_calls'])
        assert round_['knowledge_after']==points
        route=round_['route'];assert route['ManifestRef']==h.manifest_id and route['ManifestDigest']==h.manifest_digest and route['DeploymentRevisionID']==h.revision_id
        round_['exchange']=require_exchange(round_['model_calls'],content['execution']['max_output_tokens'])
        require_facts(round_);round_['result']='PASS';save()
        evidence['tenant_controls']=tenant_controls(h,content,points)
        assert run(h,rid)==state and candidates(h,rid)==round_['candidates']
        evidence.update(result='PASS',final_knowledge=h.knowledge_state(),model_requests=h.model.snapshot(),embedding_requests=h.model.embeddings())
        assert len(evidence['embedding_requests'])==4
        save()
    except BaseException as exc:
        frame=traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL',error=h.redact(str(exc)),error_type=type(exc).__name__,location={'file':Path(frame.filename).name,'line':frame.lineno})
        if hasattr(getattr(h,'model',None),'snapshot'):evidence['model_calls_at_failure']=h.model.snapshot()
        save();raise
    finally:
        try:h.close();evidence['cleanup']='PASS'
        except BaseException as exc:evidence.update(result='FAIL',cleanup_error=h.redact(str(exc)));raise
        finally:
            save()
            leaks=[str(path) for path in h.artifacts.rglob('*') if path.is_file() and any(secret.encode() in path.read_bytes() for secret in h.secrets if secret)]
            evidence['secret_scan']={'result':'FAIL' if leaks else 'PASS','files':leaks}
            if leaks:evidence['result']='FAIL'
            save()
            if evidence['result']!='PASS':raise RuntimeError('Parallel Knowledge joint did not pass; inspect retained evidence')
    print('WORKER_PARALLEL_KNOWLEDGE_JOINT=PASS actual SDK parallel + scoped Qdrant equal vectors + explicit aggregate + Session + Lab; crossTenant=storage-control-only; external_semantics=false',flush=True)


if __name__=='__main__':main()
