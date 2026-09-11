#!/usr/bin/env python3
"""Three normal Runs with Memory + Session Summary + Artifact + Knowledge.

--live forwards main and summary model bytes unchanged to the already configured
DeepSeek endpoint. Embeddings remain an explicitly labelled deterministic fixture.
"""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import traceback

sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent/'worker-v1-joint'))
from combined_capabilities_fixture import CombinedHarness,round_inputs,has_summary_text,MEMORY_TEXT,FILE_NAME,FILE_BYTES,DOCUMENT_TEXT,TOOLS,FINAL_TEXT,CALLABLE_NAME
from knowledge_joint_fixture import DOCUMENT_NAME
from faults import run,head,wait_success,submit
import channel_lab_fixture as gateway_fixture


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);parser.add_argument('--artifacts',type=Path);parser.add_argument('--race',action='store_true');parser.add_argument('--live',action='store_true');parser.add_argument('--env-file',type=Path,default=Path('/Users/jfs/Projects/trpc-agent-service/.env'))
    args=parser.parse_args()
    h=CombinedHarness(args.root,args.artifacts or Path(tempfile.mkdtemp(prefix='worker-capabilities-combined-')),args.race,live=args.live,model_name='deepseek-v4-flash' if args.live else 'joint-fixture')
    evidence={'result':'PENDING','model_execution':'REAL_DEEPSEEK_UNMODIFIED_BYTES' if args.live else 'DETERMINISTIC_MODEL_FIXTURE','embedding_execution':'DETERMINISTIC_FIXTURE','real_external_embedding_verified':False,'im':'REAL_CHANNEL_LAB','rounds':[],'matrix':'three normal Runs only; standalone fault matrices are separately verified'}
    print('CAPABILITIES_JOINT_ARTIFACTS='+str(h.artifacts),flush=True)
    current_run_id=None
    def save():h.record('capabilities-joint.json',evidence)
    def primary_records():
        return [r for r in h.model.snapshot() if r['request'].get('stream')] if h.live else [{'request':r} for r in h.model.snapshot()]
    def summary_records():
        return [r for r in h.model.snapshot() if not r['request'].get('stream')] if h.live else [{'request':r,'text':text} for r,text in zip(h.summary_provider.snapshot(),h.summary_provider.summary_outputs()) if r['model']=='joint-summary']
    def tool_observations(calls):
        seen_calls={};seen_results={}
        for call in calls:
            for msg in call['request']['messages']:
                for tc in msg.get('tool_calls',[]):seen_calls[tc['id']]=tc['function']
                if msg.get('role')=='tool':
                    try:value=json.loads(msg['content'])
                    except ValueError:value={'error':msg['content']}
                    seen_results[msg['tool_call_id']]=value
        return [{'id':id,'name':function['name'],'arguments':json.loads(function['arguments']),'result':seen_results.get(id)} for id,function in seen_calls.items() if id in seen_results]
    try:
        h.provision();h.configure_providers(env_file=args.env_file if args.live else None)
        h.control_start(gateway_fixture.prepare(h));h.seed();h.start_worker();h.verify_dependencies();h.gateway=gateway_fixture.start(h)
        imported=h.import_text(DOCUMENT_NAME,DOCUMENT_TEXT);assert imported=={'documents':1}
        knowledge=h.knowledge_state();assert len(knowledge)==1
        envelope=json.loads(h.sql("SELECT convert_from(envelope,'UTF8') FROM worker.runtime_manifests WHERE manifest_id="+h.quote(h.manifest_id))[0][0]);content=envelope['content'];node=content['agent_plan']['nodes'][content['agent_plan']['root']]
        assert node['memory']['tools'] and node['artifact']['resource'] and node['knowledge_resources']==['docs'] and node['add_session_summary']
        assert content['runtime']['summary']['enabled'] and content['resources']['models'][content['runtime']['summary']['model_resource']]['model']==('deepseek-v4-flash' if h.live else 'joint-summary')
        assert len(content['resources']['models'])==2 and h.catalog_restarts==1
        evidence.update(manifest=envelope,import_response=imported,initial_knowledge=knowledge,provider_base=getattr(h,'provider_base',None))
        previous=None;previous_summary=None
        for index,text in enumerate(round_inputs()):
            offset=len(primary_records());summary_offset=len(summary_records());embedding_offset=len(h.embedding_provider.embeddings())
            run_id=submit(h,text,'42');current_run_id=run_id;delivery=h.wait_delivery(run_id);result=wait_success(h,run_id);actual=run(h,run_id);accepted=head(h,actual)
            assert accepted['accepted_ref']==result['candidate']['candidate_ref'] and accepted['accepted_digest']==result['candidate']['content_digest']
            assert result['candidate']['parent_ref']==(previous['candidate']['candidate_ref'] if previous else '')
            assert result['candidate']['parent_digest']==(previous['candidate']['content_digest'] if previous else '')
            if h.live:h.wait(lambda:all(r.get('complete') for r in h.model.snapshot()),'actual DeepSeek relay completed records',timeout=30)
            primary=primary_records()[offset:];summaries=summary_records()[summary_offset:]
            assert primary
            for call in primary:
                req=call['request'];assert req['model']==h.model_name and sorted(t['function']['name'] for t in req['tools'])==sorted(TOOLS)
                assert req['max_completion_tokens']==content['execution']['max_output_tokens'] and 'max_tokens' not in req
            for call in summaries:
                assert call['request']['model']==('deepseek-v4-flash' if h.live else 'joint-summary')
                assert call['request']['max_completion_tokens']==content['execution']['max_output_tokens'] and 'max_tokens' not in call['request']
            if h.live:
                assert all(r['status']==200 and r.get('usage',{}).get('total_tokens',0)>0 for r in primary+summaries)
                assert delivery['final_text']==primary[-1]['text'],'Lab differs from actual live model Final'
            else:assert delivery['final_text']==FINAL_TEXT
            if previous_summary:
                assert has_summary_text(primary[0]['request']['messages'],previous_summary),'next primary did not consume exact formally accepted summary'
            stored=result['candidate']['content']['snapshot']['session'].get('summaries',{})
            if summaries:
                assert len(stored)==1 and next(iter(stored.values()))['summary']==summaries[-1]['text'],'formal summary differs from actual summary response'
                previous_summary=summaries[-1]['text']
            if index>0:assert previous_summary and summaries,'later normal Runs must generate and consume formal Summary'
            # Deduplicate full accepted history, then restrict to this current input.
            current_calls=[]
            for call in primary:
                copied=dict(call);request=dict(call['request']);messages=request['messages'];start=max(i for i,m in enumerate(messages) if m.get('role')=='user' and m.get('content')==text);request['messages']=messages[start:];copied['request']=request;current_calls.append(copied)
            observed=tool_observations(current_calls);names=[row['name'] for row in observed]
            assert 'memory_load' in names and 'artifact_load' in names and CALLABLE_NAME in names
            if index==0:assert names.count('memory_add')==1 and names.count('artifact_save')==1
            else:assert 'memory_add' not in names and 'artifact_save' not in names
            loaded_memory=next(row['result'] for row in observed if row['name']=='memory_load')
            assert any(x['memory']==MEMORY_TEXT for x in loaded_memory['results'])
            loaded_file=next(row['result'] for row in observed if row['name']=='artifact_load')
            from artifact_joint_fixture import assert_artifact
            assert_artifact(loaded_file,FILE_NAME,0,FILE_BYTES,loaded=True,mime_type='text/plain')
            docs=next(row['result'] for row in observed if row['name']==CALLABLE_NAME)
            assert DOCUMENT_TEXT in [x['text'] for x in docs['documents']]
            memory_status=h.sql('SELECT memory_status FROM worker.execution_completions WHERE run_id='+h.quote(run_id))
            assert memory_status==[['APPLIED']]
            route=json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id='+h.quote(run_id))[0][0])['Route']
            assert route['ManifestRef']==h.manifest_id and route['ManifestDigest']==h.manifest_digest
            assert actual['attempts']==1,'normal combination must not hide a retried attempt'
            memory=h.memory_state();assert len(memory)==1 and memory[0]['revision']==index+1
            assert [e['memory']['memory'] for e in memory[0]['content']['entries']]==[MEMORY_TEXT]
            objects=h.object_state();metadata=h.metadata_state();assert len(objects)==1 and objects[0]['bytes_hex']==FILE_BYTES.hex() and len(metadata['files'])==1 and len(metadata['versions'])==1
            assert metadata['versions'][0]['object_key']==objects[0]['key'] and metadata['versions'][0]['content_sha256']==objects[0]['sha256']
            assert h.knowledge_state()==knowledge
            previous={'run_id':run_id,'input':text,'run':actual,'head':accepted,'candidate':result['candidate'],'completion':result['completion'],'memory_status':memory_status,'actual_route':route,'delivery':delivery,'primary_calls':primary,'summary_calls':summaries,'formal_summary':previous_summary,'tool_observations':observed,'memory':memory,'artifact_metadata':metadata,'s3_objects':objects,'knowledge':h.knowledge_state(),'embedding_requests':h.embedding_provider.embeddings()[embedding_offset:]}
            previous['durable_snapshot']=h.failure_snapshot(run_id)
            evidence['rounds'].append(previous);save()
        assert previous_summary and len(evidence['rounds'])==3
        evidence.update(result='PASS',catalog_restarts=h.catalog_restarts,all_primary_calls=primary_records(),all_summary_calls=summary_records(),all_embedding_requests=h.embedding_provider.embeddings(),final_memory=h.memory_state(),final_artifact_metadata=h.metadata_state(),final_s3=h.object_state(),final_knowledge=h.knowledge_state());save()
    except BaseException as exc:
        frame=traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL',error=h.redact(str(exc)),error_type=type(exc).__name__,error_location={'file':Path(frame.filename).name,'line':frame.lineno})
        evidence['failure_snapshot']=h.failure_snapshot(current_run_id)
        if hasattr(h,'embedding_provider'):evidence['embedding_requests']=h.embedding_provider.embeddings()
        if hasattr(h,'summary_provider'):evidence.update(primary_calls=primary_records(),summary_calls=summary_records())
        save();raise RuntimeError(evidence['error']) from None
    finally:
        try:h.close();evidence['cleanup']='PASS'
        except BaseException as exc:evidence.update(result='FAIL',cleanup_error=h.redact(str(exc)));raise
        finally:
            save();leaked=[str(path) for path in h.artifacts.rglob('*') if path.is_file() and any(secret.encode() in path.read_bytes() for secret in h.secrets if secret)];evidence['secret_scan']={'result':'FAIL' if leaked else 'PASS','files':leaked}
            if leaked:evidence['result']='FAIL'
            save()
            if evidence['result']!='PASS':raise RuntimeError('Combined capability gate did not pass; inspect evidence')
    print('WORKER_CAPABILITIES_JOINT=PASS mode='+('REAL_DEEPSEEK' if h.live else 'FIXTURE')+' sameManifest=true normalRuns=3 memory+summary+artifact+knowledge=true embeddingFixture=true cleanup=PASS',flush=True)


if __name__=='__main__':main()
