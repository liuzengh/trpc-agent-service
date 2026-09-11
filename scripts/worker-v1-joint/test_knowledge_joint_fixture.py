import copy
import io
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import urllib.error
import urllib.request
import knowledge_joint_fixture as f


class KnowledgeFixtureTests(unittest.TestCase):
    def test_provider_name_matches_frozen_consumer_golden(self):
        path=Path(__file__).resolve().parents[2]/'api/schemas/deployment/v1/callable-name-v1.json'
        golden=json.loads(path.read_text())['golden']
        expected=next(row['provider_name'] for row in golden if row['entry_id']=='knowledge/docs')
        self.assertEqual(f.CALLABLE_NAME,expected)

    def test_profile_uses_managed_target_and_distinct_embedding_slot(self):
        h=f.KnowledgeHarness.__new__(f.KnowledgeHarness)
        h.qdrant_key='qdrant-private';h.model=Mock(url='http://127.0.0.1:19001',embedding_model=f.EMBEDDING_MODEL,dimensions=3,embedding_key='embedding-private')
        body={'config':{'models':{'primary':{}},'knowledge':{}},'credentials':{}}
        before=copy.deepcopy(body)
        with patch.object(f.Harness,'api',return_value={} ) as api:
            h.api('PUT','/v1/tenants/t/runtime-profiles/p/draft',body)
        sent=api.call_args.args[2]
        self.assertEqual(before,body)
        self.assertEqual(sent['config']['knowledge']['docs'],{'kind':'managed_knowledge','backend_id':f.BACKEND_ID,'backend_revision':1,'embedding':{'model':f.EMBEDDING_MODEL,'base_url':'http://127.0.0.1:19001/v1','dimensions':3}})
        self.assertEqual(sent['credentials']['knowledge']['docs'],{'qdrant_api_key':{'action':'replace','value':'qdrant-private'},'embedding_api_key':{'action':'replace','value':'embedding-private'}})

    def test_scroll_retains_real_named_vectors_and_pages(self):
        h=f.KnowledgeHarness.__new__(f.KnowledgeHarness);calls=[]
        def api(method,path,body):
            calls.append(copy.deepcopy(body))
            if body.get('offset'):
                return {'result':{'points':[{'id':'b','vector':{f.VECTOR_NAME:[1,0,0]},'payload':{'worker_scope':'scope'}}],'next_page_offset':None}}
            return {'result':{'points':[{'id':'a','vector':{f.VECTOR_NAME:[1,0,0]},'payload':{'worker_scope':'scope'}}],'next_page_offset':'next'}}
        h.qdrant_request=api
        self.assertEqual([p['id'] for p in h.knowledge_state()],['a','b'])
        self.assertEqual(calls[1],{'limit':100,'with_payload':True,'with_vector':True,'offset':'next'})

    def test_qdrant_error_does_not_expose_body_and_closes(self):
        h=f.KnowledgeHarness.__new__(f.KnowledgeHarness);h.qdrant_endpoint='http://127.0.0.1:19000';h.qdrant_key='private'
        body=io.BytesIO(b'private provider message')
        error=urllib.error.HTTPError(h.qdrant_endpoint,401,'denied',{},body)
        with patch.object(urllib.request,'urlopen',side_effect=error):
            with self.assertRaisesRegex(RuntimeError,'^fixture Qdrant HTTP status 401$'):h.qdrant_request('GET','/collections')
        self.assertTrue(body.closed)

    def test_real_embedding_http_uses_separate_auth_and_invalid_vector_failure(self):
        h=Mock();h.secret.side_effect=['model-private','embedding-private'];h.model_name='joint-fixture'
        model=f.KnowledgeModelFixture(h)
        try:
            def post(path,body,key):
                req=urllib.request.Request(model.url+path,data=json.dumps(body).encode(),headers={'Authorization':'Bearer '+key})
                with urllib.request.urlopen(req) as r:return json.loads(r.read())
            body={'model':f.EMBEDDING_MODEL,'dimensions':3,'input':['knowledge text']}
            result=post('/v1/embeddings',body,'embedding-private')
            self.assertEqual(result['data'][0]['embedding'],[1,0,0])
            model.fail_embedding=True
            self.assertEqual(post('/v1/embeddings',body,'embedding-private')['data'][0]['embedding'],[])
            with self.assertRaises(urllib.error.HTTPError) as caught:post('/v1/embeddings',body,'model-private')
            caught.exception.close()
            self.assertEqual(model.embeddings(),[body,body])
        finally:model.close()

    def test_real_model_final_comes_from_sdk_search_documents(self):
        h=Mock();h.secret.side_effect=['model-private','embedding-private'];h.model_name='joint-fixture'
        model=f.KnowledgeModelFixture(h)
        try:
            body={'model':'joint-fixture','messages':[{'role':'user','content':'knowledge-query'}],'tools':[{'function':{'name':f.CALLABLE_NAME}}]}
            def post():
                req=urllib.request.Request(model.url+'/v1/chat/completions',data=json.dumps(body).encode(),headers={'Authorization':'Bearer model-private'})
                with urllib.request.urlopen(req) as r:
                    return [json.loads(line[6:]) for line in r.read().decode().splitlines() if line.startswith('data: {')][0]['choices'][0]['delta']
            self.assertEqual(post()['tool_calls'][0]['function']['name'],f.CALLABLE_NAME)
            body['messages'].append({'role':'tool','content':json.dumps({'documents':[{'id':'actual-point','text':f.DOCUMENT_TEXT,'score':1}]})})
            self.assertEqual(post()['content'],'knowledge final: '+f.DOCUMENT_TEXT)
        finally:model.close()

    def test_gui_canary_is_read_from_real_tool_result(self):
        h=Mock();h.secret.side_effect=['model-private','embedding-private'];h.model_name='joint-fixture'
        h.gui_knowledge_expected={'name':'gui-reference.txt','text':'GUI imported canary LILY-526.'}
        model=f.KnowledgeModelFixture(h)
        try:
            body={'model':'joint-fixture','messages':[{'role':'user','content':'knowledge-gui-query'}],'tools':[{'function':{'name':f.CALLABLE_NAME}}]}
            def post():
                req=urllib.request.Request(model.url+'/v1/chat/completions',data=json.dumps(body).encode(),headers={'Authorization':'Bearer model-private'})
                with urllib.request.urlopen(req) as r:
                    return [json.loads(line[6:]) for line in r.read().decode().splitlines() if line.startswith('data: {')][0]['choices'][0]['delta']
            self.assertEqual(post()['tool_calls'][0]['function']['name'],f.CALLABLE_NAME)
            value={'documents':[{'id':'actual-qdrant-point','text':h.gui_knowledge_expected['text'],'score':1}]}
            body['messages'].append({'role':'tool','content':json.dumps(value)})
            self.assertEqual(post()['content'],'knowledge final: '+h.gui_knowledge_expected['text'])
            self.assertEqual(model.output_snapshot()[0]['tool_results'],[value])
        finally:model.close()

    def test_cleanup_requires_confirmed_container_removal(self):
        h=f.KnowledgeHarness.__new__(f.KnowledgeHarness);h.qdrant='owned';h.work=Path('/absent-private-fixture');h.restore_knowledge_storage=Mock();h.redact=lambda s:s
        records=[];h.record=lambda name,value:records.append(value)
        with patch.object(f.Harness,'close'),patch.object(f.subprocess,'run',return_value=Mock(returncode=1,stderr='daemon gone')):
            with self.assertRaisesRegex(RuntimeError,'removal unverified'):h.close()
        self.assertEqual(records[0]['result'],'FAIL');self.assertFalse(records[0]['removed'])


if __name__=='__main__':unittest.main()
