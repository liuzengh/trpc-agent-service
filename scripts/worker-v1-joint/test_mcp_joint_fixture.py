import copy,json,unittest,urllib.request
from unittest.mock import Mock,patch
import mcp_joint_fixture as f

class MCPFixtureTests(unittest.TestCase):
 def test_record_is_available_on_base_harness_and_reopens_redacted_json(self):
  import tempfile
  from pathlib import Path
  with tempfile.TemporaryDirectory() as directory:
   h=self.harness();h.artifacts=Path(directory);h.secrets=['actual-private-value']
   h.record('receipt.json',{'text':'multiline\nactual-private-value\t中文'})
   path=h.artifacts/'receipt.json';self.assertEqual(json.loads(path.read_text()),{'text':'multiline\n[redacted]\t中文'});self.assertEqual(path.stat().st_mode&0o777,0o600)

 def test_session_continuity_requires_parent_digest_real_history_and_route(self):
  final='原文 "quote" \\ path\n\n下一行\ttab';previous={'candidate':{'candidate_ref':'sc1_parent','content_digest':'sha256:parent'},'delivery':{'final_text':final}}
  candidate={'parent_ref':'sc1_parent','parent_digest':'sha256:parent'};route={'ManifestRef':'manifest','ManifestDigest':'sha256:manifest'}
  for content in (final,[{'type':'text','text':final}]):
   f.assert_session_continuity(previous,candidate,[{'messages':[{'role':'assistant','content':content}]}],route,'manifest','sha256:manifest')
  for wrong in ({'parent_ref':'other','parent_digest':'sha256:parent'},{'parent_ref':'sc1_parent','parent_digest':'sha256:other'}):
   with self.assertRaises(AssertionError):f.assert_session_continuity(previous,wrong,[{'messages':[{'role':'assistant','content':final}]}],route,'manifest','sha256:manifest')
  for message in ({'role':'assistant','content':json.dumps(final)},{'role':'user','content':final},{'role':'assistant','content':None}):
   with self.assertRaises(AssertionError):f.assert_session_continuity(previous,candidate,[{'messages':[message]}],route,'manifest','sha256:manifest')
  with self.assertRaises(AssertionError):f.assert_session_continuity(None,{'parent_ref':'','parent_digest':''},[],route,'other','sha256:manifest')
  f.assert_session_continuity(None,{'parent_ref':'','parent_digest':''},[],route,'manifest','sha256:manifest')

 def harness(self):
  h=f.MCPHarness.__new__(f.MCPHarness);h.mcp_url='http://127.0.0.1:19001/mcp';h.mcp_token='private-mcp-value';return h
 def test_public_agent_and_profile_hooks_preserve_existing_fields(self):
  h=self.harness();agent={'spec':{'requirements':{'models':{'primary':{'capabilities':['chat']}},'tools':{},'knowledge':{}},'nodes':{'assistant':{'model_slot':'primary','tool_slots':[]}}}}
  original=copy.deepcopy(agent)
  with patch.object(f.Harness,'api',return_value={}) as api:h.api('PUT','/v1/tenants/t/agents/a/draft',agent)
  sent=api.call_args.args[2];self.assertEqual(agent,original);self.assertEqual(sent['spec']['requirements']['tools'],{'search':{'capability':'web.search'}});self.assertEqual(sent['spec']['nodes']['assistant']['tool_slots'],['search']);self.assertEqual(sent['spec']['nodes']['assistant']['model_slot'],'primary')
  body={'config':{'models':{'primary':{'capabilities':['chat']}},'tools':{},'storage':{'session':{'kind':'postgres_state'}}},'credentials':{'models':{'primary':{'api_key':{'action':'replace','value':'model-key'}}}}};old=copy.deepcopy(body)
  with patch.object(f.Harness,'api',return_value={}) as api:h.api('PUT','/v1/tenants/t/runtime-profiles/p/draft',body)
  sent=api.call_args.args[2];self.assertEqual(body,old);self.assertEqual(sent['config']['tools']['search'],{'kind':'mcp_streamable_http','server_url':h.mcp_url,'toolset_name':'joint_mcp','tool_name':f.SELECTED,'auth':{'kind':'bearer'},'capability':'web.search'});self.assertEqual(sent['credentials']['tools']['search']['bearer_token']['value'],h.mcp_token);self.assertEqual(sent['credentials']['models'],old['credentials']['models']);self.assertEqual(sent['config']['storage'],old['config']['storage'])
 def test_hashed_declaration_and_schema_exclude_unselected(self):
  import hashlib
  self.assertEqual(f.CALLABLE,'fn_'+hashlib.sha256(b'tools/search').hexdigest()[:60])
  valid={'tools':[{'type':'function','function':{'name':f.CALLABLE,'parameters':{'type':'object','required':['query'],'properties':{'query':{'type':'string'}}}}}]};f.assert_declaration(valid)
  invalid=copy.deepcopy(valid);invalid['tools'].append({'function':{'name':f.UNSELECTED}})
  with self.assertRaises(AssertionError):f.assert_declaration(invalid)
  invalid=copy.deepcopy(valid);invalid['tools'][0]['function']['name']=f.SELECTED
  with self.assertRaises(AssertionError):f.assert_declaration(invalid)
 def test_decoded_content_preserves_multiline_and_ignores_nontext(self):
  text='真实文本\n\n引号 " \\ 路径\ttab'
  self.assertEqual(f.tool_text(json.dumps([{'type':'text','text':text}])),text)
  self.assertEqual(f.tool_text({'content':[{'type':'text','text':text}]}),text)
  self.assertEqual(f.tool_text([None,{'image_url':'untrusted'},123]),'')
 def test_model_real_http_normal_and_error_correction(self):
  h=Mock(model_name='joint-mcp-fixture');h.secret.return_value='model-private';model=f.MCPModelFixture(h)
  try:
   for text,expected in [(f.NORMAL,['orchid']),(f.REPAIR,['correct-me','orchid'])]:
    messages=[{'role':'user','content':text}]
    def post():
     body={'model':h.model_name,'messages':messages,'tools':[{'type':'function','function':{'name':f.CALLABLE,'parameters':{'type':'object','required':['query'],'properties':{'query':{'type':'string'}}}}}]}
     request=urllib.request.Request(model.url+'/v1/chat/completions',data=json.dumps(body).encode(),headers={'Authorization':'Bearer model-private'})
     with urllib.request.urlopen(request) as response:
      events=[json.loads(line[6:]) for line in response.read().decode().splitlines() if line.startswith('data: {')]
     return events[0]['choices'][0]['delta']
    for query in expected:
     call=post()['tool_calls'][0];self.assertEqual(json.loads(call['function']['arguments']),{'query':query});self.assertEqual(call['function']['name'],f.CALLABLE)
     messages.append({'role':'assistant','tool_calls':[call]});messages.append({'role':'tool','tool_call_id':call['id'],'content':json.dumps([{'type':'text','text':f.ANSWER if query=='orchid' else f.CORRECTION}])})
    self.assertEqual(post()['content'],f.ANSWER)
   self.assertEqual(model.errors,[]);self.assertEqual(len(model.snapshot()),5)
  finally:model.close()
 def test_close_reports_fixture_error_only_after_parent_cleanup(self):
  h=self.harness();h.model=Mock();h.model.errors=['actual fixture assertion'];order=[];h.record=lambda name,value:order.append(value['result'])
  with patch.object(f.Harness,'close',side_effect=lambda:order.append('parent_closed')):
   with self.assertRaisesRegex(RuntimeError,'actual fixture assertion'):h.close()
  self.assertEqual(order,['parent_closed','FAIL'])

if __name__=='__main__':unittest.main()
