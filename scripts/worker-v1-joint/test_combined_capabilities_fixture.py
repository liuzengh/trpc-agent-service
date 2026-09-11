"""Composition seam checks only; real four-capability proof is the process gate."""
import copy
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import Mock,patch
import urllib.request
import combined_capabilities_fixture as f


class CombinedFixtureTests(unittest.TestCase):
    def test_summary_match_preserves_actual_content_bytes(self):
        summary='摘要 "quote" \\ path\n\n下一行\ttab'
        for content in ('prefix\n'+summary+'\nsuffix',[{'type':'text','text':'prefix '+summary}],[None,[{'type':'text','text':summary}]]):
            with self.subTest(content=content):self.assertTrue(f.has_summary_text([{'role':'system','content':content}],summary))
        for content in (None,123,{'image_url':summary},[{'type':'image_url','image_url':summary}],json.dumps(summary),summary.replace('\n',' ')):
            with self.subTest(content=content):self.assertFalse(f.has_summary_text([{'role':'system','content':content}],summary))
        self.assertFalse(f.has_summary_text([None,{'content':'text'}],None))
        self.assertFalse(f.has_summary_text([{'content':'text'}],''))

    def harness(self,live=False):
        h=f.CombinedHarness.__new__(f.CombinedHarness);h.live=live;h.model_name='deepseek-v4-flash' if live else 'joint-fixture'
        h.model=Mock(url='http://127.0.0.1:19001',key='main-private',summary_key='summary-private',embedding_key='embedding-private',embedding_model='joint-embedding-fixture',dimensions=3)
        h.embedding_provider=Mock(url='http://127.0.0.1:19002');h.summary_provider=Mock(url='http://127.0.0.1:19003')
        h.qdrant_key='qdrant-private';h.memory_password='memory-private';h.artifact_access_key='access-private';h.artifact_secret_key='secret-private'
        return h

    def test_same_agent_combines_all_four_existing_hooks(self):
        h=self.harness()
        body={'spec':{'requirements':{'models':{'primary':{'capabilities':['chat']}},'knowledge':{}},'nodes':{'assistant':{'knowledge_slots':[]}}}}
        before=copy.deepcopy(body)
        with patch.object(f.Harness,'api',return_value={}) as api:h.api('PUT','/v1/tenants/t/agents/a/draft',body)
        sent=api.call_args.args[2]['spec'];node=sent['nodes']['assistant']
        self.assertEqual(body,before);self.assertEqual(node['memory']['tools'],f.MEMORY_TOOLS);self.assertTrue(node['artifact']['enabled']);self.assertEqual(node['knowledge_slots'],['docs']);self.assertTrue(node['add_session_summary']);self.assertEqual(sent['runtime']['summary'],{'enabled':True,'model_slot':'summarizer','event_threshold':1})

    def test_same_profile_merges_fixed_backends_and_distinct_credentials(self):
        h=self.harness()
        body={'config':{'models':{'primary':{}},'knowledge':{},'storage':{'session':{'kind':'postgres_state'}}},'credentials':{'models':{},'storage':{'session':{'dsn':'existing'}}}}
        original=copy.deepcopy(body)
        with patch.object(f.Harness,'api',return_value={}) as api:h.api('PUT','/v1/tenants/t/runtime-profiles/p/draft',body)
        sent=api.call_args.args[2]
        self.assertEqual(body,original)
        self.assertEqual(set(sent['config']['storage']),{'session','memory','artifact'})
        self.assertEqual(sent['config']['knowledge']['docs']['embedding']['base_url'],h.embedding_provider.url+'/v1')
        self.assertEqual(sent['config']['models']['summarizer']['base_url'],h.summary_provider.url+'/v1')
        self.assertEqual(sent['config']['models']['summarizer']['model'],'joint-summary')
        self.assertEqual(sent['credentials']['storage']['memory']['dsn_password']['value'],'memory-private')
        self.assertEqual(sent['credentials']['storage']['artifact']['access_key_id']['value'],'access-private')
        self.assertEqual(sent['credentials']['knowledge']['docs']['embedding_api_key']['value'],'embedding-private')

    def test_live_leaf_keeps_exact_deepseek_for_summary_not_fixture_model(self):
        h=self.harness(live=True)
        body={'config':{'models':{'primary':{}},'knowledge':{},'storage':{}},'credentials':{'models':{},'storage':{}}}
        with patch.object(f.Harness,'api',return_value={}) as api:h.api('PUT','/v1/tenants/t/runtime-profiles/p/draft',body)
        self.assertEqual(api.call_args.args[2]['config']['models']['summarizer']['model'],'deepseek-v4-flash')
        self.assertEqual(api.call_args.args[2]['config']['knowledge']['docs']['embedding']['base_url'],h.embedding_provider.url+'/v1')

    def test_tenant_creation_calls_only_merged_catalog_once(self):
        h=self.harness();h.bind_combined_catalog=Mock()
        with patch.object(f.Harness,'api',return_value={'id':'real-owner-tenant'}):h.api('POST','/v1/admin/tenants',{})
        h.bind_combined_catalog.assert_called_once_with('real-owner-tenant')

    def test_live_configuration_reuses_existing_byte_relay_and_in_memory_key(self):
        h=self.harness(live=True);h.secrets=[];h.urls={};old=h.model
        embedding=Mock(embedding_key='embedding-private',embedding_model='joint-embedding-fixture',dimensions=3)
        relay=Mock(url='http://127.0.0.1:19500')
        with patch.object(f,'KnowledgeModelFixture',return_value=embedding),patch.object(f,'read_key',side_effect=['private-deepseek-key','https://api.deepseek.com/v1']) as read,patch.object(f,'LiveRelay',return_value=relay) as create:
            h.configure_providers(env_file=Path('/private/mock.env'))
        old.close.assert_called_once();self.assertEqual([x.args[1] for x in read.call_args_list],['DEEPSEEK_API_KEY','DEEPSEEK_BASE_URL'])
        create.assert_called_once_with(h,'private-deepseek-key','https://api.deepseek.com/v1','deepseek-v4-flash')
        self.assertIs(h.model,h.summary_provider);self.assertEqual(h.model.embedding_key,'embedding-private');self.assertIn('private-deepseek-key',h.secrets)

    def test_cleanup_closes_extra_providers_even_on_parent_error(self):
        h=self.harness();h.redact=lambda s:s;records=[];h.record=lambda n,v:records.append(v)
        with patch.object(f.ArtifactHarness,'close',side_effect=RuntimeError('parent failed')):
            with self.assertRaisesRegex(RuntimeError,'parent failed'):h.close()
        h.summary_provider.close.assert_called_once();h.embedding_provider.close.assert_called_once();self.assertEqual(records[0]['result'],'FAIL')

    def test_failure_snapshot_uses_json_rows_and_survives_read_errors(self):
        h=self.harness();queries=[];value=[{'body':'line one\n\nline two\ttab'}]
        def sql(query):
            queries.append(query)
            self.assertTrue(query.startswith("SELECT COALESCE(json_agg(row_to_json(fact))"))
            if 'worker.execution_attempts' in query:raise RuntimeError('private dependency detail')
            return [[json.dumps(value)]]
        h.sql=sql
        for name in ('memory_state','metadata_state','object_state','knowledge_state'):setattr(h,name,Mock(return_value=[]))
        result=h.failure_snapshot("run'quoted")
        self.assertEqual(len(queries),10);self.assertIn("run''quoted",queries[0])
        self.assertEqual(result['queries']['delivery_parts'],value)
        self.assertEqual(result['queries']['attempts'],{'read_error_type':'RuntimeError'})
        self.assertNotIn('private dependency detail',json.dumps(result));self.assertNotIn('token_hash',' '.join(queries));self.assertNotIn('credential_ref',' '.join(queries))
        self.assertEqual(h.failure_snapshot(None)['queries'],{})

    def test_minio_bootstrap_retries_only_bucket_503(self):
        h=self.harness();h._initializing_dependencies=True;h._s3_bootstrap_retries=0
        with patch.object(f.ArtifactHarness,'s3_request',side_effect=[RuntimeError('fixture S3 HTTP status 503 expected 200'),b'ready']) as request,patch.object(f.time,'sleep'):
            self.assertEqual(h.s3_request('PUT'),b'ready')
        self.assertEqual(request.call_count,2);self.assertEqual(h._s3_bootstrap_retries,1)
        for initializing,key,error in ((False,None,'fixture S3 HTTP status 503 expected 200'),(True,'actual-object','fixture S3 HTTP status 503 expected 200'),(True,None,'fixture S3 HTTP status 403 expected 200')):
            h._initializing_dependencies=initializing
            with patch.object(f.ArtifactHarness,'s3_request',side_effect=RuntimeError(error)) as request:
                with self.assertRaisesRegex(RuntimeError,error):h.s3_request('GET',key)
            self.assertEqual(request.call_count,1)

    def test_fixture_assertions_are_raised_after_parent_cleanup(self):
        h=self.harness();h.redact=lambda s:s;events=[];h.record=lambda n,v:events.append(v['result'])
        h.model.errors=['observed fixture mismatch']
        def cleanup():
            events.append('owned_parent_resources_closed')
        with patch.object(f.ArtifactHarness,'close',side_effect=cleanup):
            with self.assertRaisesRegex(RuntimeError,'observed fixture mismatch'):h.close()
        self.assertEqual(events,['owned_parent_resources_closed','FAIL'])
        h.summary_provider.close.assert_called_once();h.embedding_provider.close.assert_called_once()

    def test_real_fixture_http_calls_all_three_tool_families_before_final(self):
        h=Mock();h.secret.return_value='fixture-private';h.model_name='joint-fixture';m=f.CombinedModelFixture(h)
        try:
            body={'model':'joint-fixture','messages':[{'role':'user','content':f.round_inputs()[0]}],'tools':[{'function':{'name':name}} for name in f.TOOLS]}
            def post():
                req=urllib.request.Request(m.url+'/v1/chat/completions',data=json.dumps(body).encode(),headers={'Authorization':'Bearer fixture-private'})
                with urllib.request.urlopen(req) as r:return [json.loads(x[6:]) for x in r.read().decode().splitlines() if x.startswith('data: {')][0]['choices'][0]['delta']
            metadata={'name':f.FILE_NAME,'version':0,'ref':'artifact:session:'+f.FILE_NAME+':0','mime_type':'text/plain','size_bytes':len(f.FILE_BYTES),'sha256':hashlib.sha256(f.FILE_BYTES).hexdigest()}
            results=[{'id':'memory-entry'},{'results':[{'memory':f.MEMORY_TEXT}]},metadata,dict(metadata,content_base64=f.base64.b64encode(f.FILE_BYTES).decode()),{'documents':[{'text':f.DOCUMENT_TEXT}]}]
            expected=['memory_add','memory_load','artifact_save','artifact_load',f.CALLABLE_NAME]
            for name,result in zip(expected,results):
                call=post()['tool_calls'][0]
                self.assertEqual(call['function']['name'],name)
                body['messages'].append({'role':'tool','tool_call_id':call['id'],'content':json.dumps(result)})
                # Real SDK Summary keeps a bounded recent tool-history window.
                # Provider fixture must acknowledge actual IDs, not count window rows.
                body['messages']=body['messages'][:1]+body['messages'][1:][-2:]
            self.assertEqual(post()['content'],f.FINAL_TEXT);self.assertEqual(m.outputs[0]['plan'],expected)
        finally:m.close()


if __name__=='__main__':unittest.main()
