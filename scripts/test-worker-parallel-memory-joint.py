#!/usr/bin/env python3
"""Parallel real SDK Memory -> PG or Redis accepted snapshot -> Session -> Lab.

Three Runs per backend: parallel add, formal persisted read, private add then
sibling model401. No live model/GUI and no product state writes through SQL.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import traceback
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent/'worker-v1-joint'))
from parallel_memory_joint_fixture import (ParallelMemoryHarness,ParallelRedisMemoryHarness,
    ROLES,BRANCHES,SLOTS,MODELS,ALLOWED,MUTATE,READ,FAILURE,POISON,VALUES,
    require_exchange,require_memory_state,require_accepted_history,all_strings)
from faults import submit,run,head,rows,attempts,candidates,completions,outboxes
import channel_lab_fixture as lab
_spec=importlib.util.spec_from_file_location('parallel_memory_delivery',Path(__file__).with_name('test-worker-sequence-joint.py'))
_sequence=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_sequence)
require_delivery_wire,FAILURE_FINAL=_sequence.require_delivery_wire,_sequence.FAILURE_FINAL

def require_manifest(envelope,h):
    content=envelope['content'];plan=content['agent_plan'];nodes=plan['nodes']
    assert plan['root']=='workflow' and nodes['workflow']['children']==['parallel','aggregator']
    assert nodes['workflow']['kind']=='sequence' and nodes['parallel']['kind']=='parallel' and nodes['parallel']['children']==list(BRANCHES)
    models=content['resources']['models'];assert set(models)==set(SLOTS.values())
    assert len({value['credential']['credential_id'] for value in models.values()})==3
    memory_resources=set()
    for role in ROLES:
        node=nodes[role];assert node['kind']=='llm' and node['model_resource']==SLOTS[role]
        assert not node['tool_resources'] and not node['knowledge_resources']
        assert sorted(node['memory']['tools'])==sorted(ALLOWED[role]) and node['memory']['preload_limit']==0
        memory_resources.add(node['memory']['resource'])
        model=models[SLOTS[role]];assert model['model']==MODELS[role] and model['base_url']==h.model.url+'/v1'
    assert len(memory_resources)==1
    memory=content['resources']['storage'][next(iter(memory_resources))]
    assert memory['kind']=='managed_memory' and memory['backend']['backend_id']==h.memory_backend_id
    assert memory['backend']['kind']==('postgresql' if h.backend=='postgres' else 'redis')
    assert memory['backend']['tenant_id']==h.tenant_id and memory['credential']['purpose']=='dsn_password'
    return content

def memory_receipt(h,round_):
    if h.backend=='postgres':
        selected=[row for row in round_['backend_after']['receipts'] if row['run_id']==round_['run_id']]
        assert len(selected)==1
        r=selected[0]
        return {'completion_id':r['completion_id'],'attempt_id':r['attempt_id'],'run_id':r['run_id'],'digest':r['candidate_digest'],'revision':r['applied_revision'],'body':json.loads(r['raw_content'])}
    selected=[row for row in round_['backend_after'] if ':receipt:' in row['key'] and row['record']['run_id']==round_['run_id']]
    assert len(selected)==1
    r=selected[0]['record']
    return {'completion_id':r['completion_id'],'attempt_id':r['attempt_id'],'run_id':r['run_id'],'digest':r['digest'],'revision':int(r['revision']),'body':json.loads(r['body'])}

def require_facts(round_,previous):
    state=round_['run'];after=round_['head_after'];delivery=round_['delivery'];exchange=round_['exchange']
    assert state['attempts']==len(round_['attempts'])==len(round_['completions'])==len(round_['outboxes'])==len(round_['memory_completion'])==1
    a,c,o,m=round_['attempts'][0],round_['completions'][0],round_['outboxes'][0],round_['memory_completion'][0]
    assert c['attempt_id']==a['attempt_id']==state['current_attempt_id']
    assert c['kind']=='ATTEMPT' and c['reply_disposition']=='FINAL' and state['status']==a['status']==c['status']
    assert c['final_intent_id']==o['intent_id']==delivery['intent_id']
    assert o['payload']['execution']['completion_id']==c['completion_id'] and o['payload']['execution']['attempt_id']==a['attempt_id']
    assert o['payload']['content']['text']==delivery['final_text'] and delivery['delivery_state']=='ACCEPTED'
    assert delivery['run_id']==state['run_id'] and after['settled_sequence']==state['session_sequence']
    if round_['input']==FAILURE:
        assert state['status']=='FAILED' and c['reason']==a['reason']=='RUNTIME_FAILED'
        assert not round_['candidates'] and not c['candidate_ref'] and not c['candidate_digest']
        assert m['memory_status']==m['memory_digest']==''
        assert all(after[k]==round_['head_before'][k] for k in ('accepted_ref','accepted_digest'))
        assert round_['backend_after']==round_['backend_before'] and round_['memory_after']==round_['memory_before']
        assert delivery['final_text']==FAILURE_FINAL and exchange['failed']
        return None
    assert state['status']=='SUCCEEDED' and m['memory_status']=='APPLIED' and m['memory_digest'].startswith('sha256:')
    assert len(round_['candidates'])==1 and not exchange['failed']
    candidate=round_['candidates'][0]
    assert candidate['candidate_ref']==c['candidate_ref']==after['accepted_ref']
    assert candidate['content_digest']==c['candidate_digest']==after['accepted_digest']
    assert candidate['attempt_id']==a['attempt_id']
    assert delivery['final_text']==exchange['terminal_output'] and delivery['final_text'] not in exchange['branch_outputs'].values()
    require_accepted_history(previous,candidate,[call['request'] for call in round_['model_calls']])
    stored=list(all_strings(candidate['content']['snapshot']))
    assert all(value in stored for value in [*exchange['branch_outputs'].values(),exchange['terminal_output']])
    return candidate

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);parser.add_argument('--artifacts',type=Path);parser.add_argument('--race',action='store_true');parser.add_argument('--backend',choices=['postgres','redis'],required=True);args=parser.parse_args()
    cls=ParallelMemoryHarness if args.backend=='postgres' else ParallelRedisMemoryHarness
    h=cls(args.root,args.artifacts or Path(tempfile.mkdtemp(prefix='parallel-memory-'+args.backend+'-')),args.race,model_name=MODELS['writer_a'])
    evidence={'result':'PENDING','backend':args.backend,'model':'DETERMINISTIC_HTTP_FIXTURE','im':'REAL_CHANNEL_LAB','business_sql_writes':False,'rounds':[]}
    def save():h.record('parallel-memory-joint.json',evidence)
    try:
        h.provision();h.prepare_parallel_memory();h.control_start(lab.prepare(h));h.seed();h.start_worker();h.verify_dependencies();h.gateway=lab.start(h)
        envelope=rows(h,"SELECT convert_from(envelope,'UTF8')::json AS manifest FROM worker.runtime_manifests WHERE manifest_id="+h.quote(h.manifest_id))[0]['manifest']
        content=require_manifest(envelope,h);evidence.update(manifest=envelope,agent_id=h.agent_id,initial_memory=h.memory_state(),initial_backend=h.backend_state());assert evidence['initial_memory']==[];save()
        previous=None;revision=0;accepted_entries=None
        for case in (MUTATE,READ,FAILURE):
            offset=len(h.model.snapshot());before=head(h,previous['run']) if previous else None
            mem_before=h.memory_state();backend_before=h.backend_state();rid=submit(h,case,'42')
            h.wait(lambda:run(h,rid)['status'] in ('SUCCEEDED','FAILED'),'Parallel Memory Run terminal',timeout=90)
            state=run(h,rid)
            r={'input':case,'run_id':rid,'run':state,'attempts':attempts(h,rid),'completions':completions(h,rid),'outboxes':outboxes(h,rid),'candidates':candidates(h,rid),'head_before':before,'head_after':head(h,state),'memory_before':mem_before,'backend_before':backend_before,'result':'PENDING'}
            evidence['rounds'].append(r);save()
            h.wait(lambda:bool(h.model.snapshot()[offset:]) and all(c.get('complete') and c.get('finished_ns') for c in h.model.snapshot()[offset:]),'all real model handlers exited',timeout=20);assert h.model.wait_idle(5)
            r['model_calls']=h.model.snapshot()[offset:]
            # Delivery visibility waits for post-accept Memory finalization.
            r['delivery']=h.gateway.wait_delivery(rid)
            r['gateway_receipts']=rows(h,'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id='+h.quote(rid))
            r['gateway_parts']=rows(h,'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id='+h.quote(rid)+' ORDER BY p.part_index')
            r['request']=rows(h,'SELECT request_json AS request FROM worker.execution_runs WHERE run_id='+h.quote(rid))[0]['request']
            r['memory_completion']=rows(h,'SELECT memory_status,memory_digest FROM worker.execution_completions WHERE run_id='+h.quote(rid))
            r['memory_after']=h.memory_state();r['backend_after']=h.backend_state();save()
            r['exchange']=require_exchange(r['model_calls'],case,content['execution']['max_output_tokens'])
            route=r['request']['Route'];assert route['ManifestRef']==h.manifest_id and route['ManifestDigest']==h.manifest_digest and route['DeploymentRevisionID']==h.revision_id
            candidate=require_facts(r,previous);require_delivery_wire(r)
            if previous:assert state['session_id']==previous['run']['session_id'] and state['session_sequence']==previous['run']['session_sequence']+1
            if case==FAILURE:
                r['facts_after_http_exit']={'candidates':candidates(h,rid),'completions':completions(h,rid),'outboxes':outboxes(h,rid),'head_after':head(h,state),'memory_after':h.memory_state(),'backend_after':h.backend_state()}
                r['old_candidate_after_failure']=candidates(h,previous['run_id']);save()
                assert all(r['facts_after_http_exit'][k]==r[k] for k in r['facts_after_http_exit'])
                assert r['old_candidate_after_failure']==[previous['candidate']]
            else:
                revision+=1;entries=require_memory_state(r['memory_after'],r['request'],h.agent_id,revision)
                receipt=memory_receipt(h,r);r['accepted_memory_receipt']=receipt
                c=r['completions'][0];assert receipt['completion_id']==c['completion_id'] and receipt['run_id']==rid and receipt['attempt_id']==c['attempt_id']
                assert receipt['digest']==r['memory_completion'][0]['memory_digest']==r['memory_after'][0]['digest'] and receipt['revision']==revision
                assert receipt['body']==r['memory_after'][0]['content']
                if case==READ:assert entries==accepted_entries,'read-only follower mutated stored entries'
                accepted_entries=entries;previous={'run':state,'run_id':rid,'candidate':candidate,'delivery':r['delivery']}
            r['result']='PASS';save()
        evidence.update(result='PASS',model_calls=h.model.snapshot(),final_memory=h.memory_state(),final_backend=h.backend_state());assert len(evidence['model_calls'])==15;save()
    except BaseException as exc:
        frame=traceback.extract_tb(exc.__traceback__)[-1];evidence.update(result='FAIL',error=h.redact(str(exc)),error_type=type(exc).__name__,location={'file':Path(frame.filename).name,'line':frame.lineno})
        if hasattr(getattr(h,'model',None),'snapshot'):evidence['model_calls_at_failure']=h.model.snapshot()
        save();raise
    finally:
        try:h.close();evidence['cleanup']='PASS'
        except BaseException as exc:evidence.update(result='FAIL',cleanup_error=h.redact(str(exc)));raise
        finally:
            save();leaks=[str(p) for p in h.artifacts.rglob('*') if p.is_file() and any(s.encode() in p.read_bytes() for s in h.secrets if s)]
            evidence['secret_scan']={'result':'FAIL' if leaks else 'PASS','files':leaks}
            if leaks:evidence['result']='FAIL'
            save()
            if evidence['result']!='PASS':raise RuntimeError('Parallel Memory joint gate did not pass; inspect retained evidence')
    print('WORKER_PARALLEL_MEMORY_JOINT=PASS backend='+args.backend+' runs=3 memoryApplied=true failureUnchanged=true formalSession=true lab=true cleanup=PASS',flush=True)

if __name__=='__main__':main()
