#!/usr/bin/env python3
"""Published selected MCP tool -> real MCP server -> formal Session -> Channel Lab.

Default: two deterministic model Runs including correctable MCP IsError.
--live: one actual DeepSeek selected tool/result-use Run, no Embedding involved.
"""
import argparse,json,sys,tempfile,traceback
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent/'worker-v1-joint'))
from mcp_joint_fixture import MCPHarness,CALLABLE,SELECTED,UNSELECTED,NORMAL,REPAIR,ANSWER,CORRECTION,assert_declaration,assert_session_continuity,tool_text
from faults import submit,run,head,wait_success,rows
import channel_lab_fixture as lab


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--artifacts',type=Path);p.add_argument('--race',action='store_true');p.add_argument('--live',action='store_true');p.add_argument('--env-file',type=Path,default=Path('/Users/jfs/Projects/trpc-agent-service/.env'));a=p.parse_args()
 h=MCPHarness(a.root,a.artifacts or Path(tempfile.mkdtemp(prefix='worker-mcp-joint-')),a.race,live=a.live,model_name='deepseek-v4-flash' if a.live else 'joint-mcp-fixture')
 evidence={'result':'PENDING','model':'REAL_DEEPSEEK_UNMODIFIED_BYTES' if a.live else 'DETERMINISTIC_HTTP_FIXTURE','mcp_server':'ACTUAL_TRPC_MCP_GO_V0.0.10','im':'REAL_CHANNEL_LAB','rounds':[],'embedding':'NOT_USED'}
 def save():h.record('mcp-joint.json',evidence)
 try:
  h.provision();h.prepare_mcp(a.env_file if a.live else None);h.control_start(lab.prepare(h));h.seed();h.start_worker();h.verify_dependencies();h.gateway=lab.start(h)
  envelope=json.loads(h.sql("SELECT convert_from(envelope,'UTF8') FROM worker.runtime_manifests WHERE manifest_id="+h.quote(h.manifest_id))[0][0]);content=envelope['content'];node=content['agent_plan']['nodes'][content['agent_plan']['root']];resource=content['resources']['tools']['search']
  assert node['tool_resources']==['search'] and node['callable_entries']==['tools/search'];assert resource['server_url']==h.mcp_url and resource['tool_name']==SELECTED and resource['auth']['credential']['purpose']=='bearer_token'
  assert h.mcp_state()['registered_tools']==[SELECTED,UNSELECTED];evidence['manifest']=envelope;previous=None
  for text in ([NORMAL] if a.live else [NORMAL,REPAIR]):
   offset=len(h.model.snapshot());server_offset=len(h.mcp_state()['events']);rid=submit(h,text,'42');delivery=h.wait_delivery(rid);result=wait_success(h,rid);actual=run(h,rid);accepted=head(h,actual)
   assert actual['attempts']==1 and accepted['accepted_ref']==result['candidate']['candidate_ref'] and accepted['accepted_digest']==result['candidate']['content_digest']
   if a.live:h.wait(lambda:all(x.get('complete') for x in h.model.snapshot()),'real MCP live model response completion')
   calls=h.model.snapshot()[offset:];requests=[x['request'] for x in calls] if a.live else calls
   route=rows(h,"SELECT request_json->'Route' AS route FROM worker.execution_runs WHERE run_id="+h.quote(rid))[0]['route']
   assert_session_continuity(previous,result['candidate'],requests,route,h.manifest_id,h.manifest_digest)
   for request in requests:
    assert_declaration(request);assert request['model']==h.model_name and request['max_completion_tokens']==content['execution']['max_output_tokens'] and 'max_tokens' not in request
   values=[];ids=set()
   for request in requests:
    messages=request['messages'];start=max(i for i,m in enumerate(messages) if m.get('role')=='user' and m.get('content')==text)
    for message in messages[start+1:]:
     if message.get('role')=='tool' and message['tool_call_id'] not in ids:ids.add(message['tool_call_id']);values.append(tool_text(message['content']))
   assert ANSWER in values
   if text==REPAIR:assert CORRECTION in values
   events=h.mcp_state()['events'][server_offset:];executed=[e for e in events if e['method']=='tool_execution']
   assert executed and all(e['authenticated'] and e['name']==SELECTED for e in executed)
   assert [e['query'] for e in executed]==(['correct-me','orchid'] if text==REPAIR else ['orchid'])
   assert [e.get('is_error',False) for e in executed]==([True,False] if text==REPAIR else [False])
   if a.live:
    assert all(c['status']==200 and c.get('complete') and c.get('usage',{}).get('total_tokens',0)>0 for c in calls);assert delivery['final_text']==calls[-1]['text'] and 'MCP_ORCHID_627' in delivery['final_text']
   else:assert delivery['final_text']==ANSWER
   receipts=rows(h,"SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id="+h.quote(rid));parts=rows(h,"SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id="+h.quote(rid)+" ORDER BY p.part_index")
   assert len(receipts)==1 and receipts[0]['outcome']=='ACCEPTED' and all(p['state']=='ACCEPTED' for p in parts) and ''.join(p['body'] for p in parts)==delivery['final_text']
   previous={'input':text,'run_id':rid,'run':actual,'head':accepted,'candidate':result['candidate'],'completion':result['completion'],'delivery':delivery,'model_calls':calls,'tool_results':values,'mcp_events':events,'gateway_receipts':receipts,'gateway_parts':parts,'route':route};evidence['rounds'].append(previous);save()
  evidence.update(result='PASS',mcp_state=h.mcp_state(),provider_base=getattr(h,'provider_base',None));save()
 except BaseException as exc:
  frame=traceback.extract_tb(exc.__traceback__)[-1];evidence.update(result='FAIL',error=h.redact(str(exc)),error_type=type(exc).__name__,location={'file':Path(frame.filename).name,'line':frame.lineno})
  if hasattr(h,'mcp_url'):
   try:evidence['mcp_state']=h.mcp_state();evidence['model_calls']=h.model.snapshot()
   except Exception:pass
  save();raise
 finally:
  try:h.close();evidence['cleanup']='PASS'
  except BaseException as exc:evidence.update(result='FAIL',cleanup_error=h.redact(str(exc)));raise
  finally:
   save();leaks=[str(p) for p in h.artifacts.rglob('*') if p.is_file() and any(secret.encode() in p.read_bytes() for secret in h.secrets if secret)];evidence['secret_scan']={'result':'FAIL' if leaks else 'PASS','files':leaks}
   if leaks:evidence['result']='FAIL'
   save()
   if evidence['result']!='PASS':raise RuntimeError('MCP joint gate did not pass; inspect retained evidence')
 print('WORKER_MCP_JOINT=PASS mode='+('REAL_DEEPSEEK' if a.live else 'FIXTURE')+' selectedOnly=true formalSession=true lab=true cleanup=PASS')
if __name__=='__main__':main()
