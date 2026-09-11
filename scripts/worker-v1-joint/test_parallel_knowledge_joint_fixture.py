import copy
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import Mock, patch
import urllib.error
import urllib.request

import parallel_knowledge_joint_fixture as f


class ParallelKnowledgeTests(unittest.TestCase):
    def test_public_tree_and_profile_have_exact_two_knowledge_resources(self):
        h=f.ParallelKnowledgeHarness.__new__(f.ParallelKnowledgeHarness)
        h.model=Mock(url='http://127.0.0.1:19101',embedding_url='http://127.0.0.1:19102',
                     embedding_model=f.EMBEDDING_MODEL,dimensions=3,embedding_key='embed-private')
        h.model.keys={role:role+'-private' for role in f.ROLES}; h.qdrant_key='qdrant-private'
        agent={'spec':{'requirements':{},'nodes':{'assistant':{'kind':'llm'}}},'expected_revision':1}
        profile={'config':{'storage':{'session':{'kind':'postgres_state'}}},'credentials':{'storage':{'session':{'dsn':{'action':'keep'}}}}}
        originals=copy.deepcopy((agent,profile))
        with patch.object(f.Harness,'api',return_value={}) as api:
            h.api('PUT','/v1/tenants/t/agents/a/draft',agent); a=api.call_args.args[3]
            h.api('PUT','/v1/tenants/t/runtime-profiles/p/draft',profile); p=api.call_args.args[3]
        self.assertEqual((agent,profile),originals)
        self.assertEqual(a['spec']['nodes']['research']['children'],list(f.BRANCHES))
        self.assertEqual(set(a['spec']['requirements']['knowledge']),{'docs_a','docs_b'})
        self.assertEqual(a['spec']['requirements']['tools'],{})
        self.assertNotIn('assistant',a['spec']['nodes'])
        for role in f.BRANCHES:
            self.assertEqual(a['spec']['nodes'][role]['knowledge_slots'],[f.RESOURCES[role]])
        self.assertEqual(a['spec']['nodes']['aggregator']['knowledge_slots'],[])
        self.assertEqual(set(p['config']['knowledge']),{'docs_a','docs_b'})
        self.assertEqual(p['config']['storage'],profile['config']['storage'])
        self.assertEqual(p['credentials']['storage'],profile['credentials']['storage'])
        for resource in p['config']['knowledge'].values():
            self.assertEqual(resource['backend_id'],f.BACKEND_ID)
            self.assertEqual(resource['embedding']['base_url'],h.model.embedding_url+'/v1')
        self.assertEqual(len({v['api_key']['value'] for v in p['credentials']['models'].values()}),3)
        self.assertEqual(set(p['credentials']['knowledge']),{'docs_a','docs_b'})

    def test_extra_tenant_does_not_replace_primary_catalog(self):
        h=f.ParallelKnowledgeHarness.__new__(f.ParallelKnowledgeHarness)
        with patch.object(f.Harness,'api',side_effect=[{'id':'tenant-one'},{'id':'tenant-two'}]), \
             patch.object(f.KnowledgeHarness,'bind_knowledge_catalog') as bind:
            h.api('POST','/v1/admin/tenants',{})
            h.api('POST','/v1/admin/tenants',{})
        bind.assert_called_once_with('tenant-one')

    def test_scope_matches_actual_store_known_json_hash_and_changes_tenant(self):
        backend={'adapter':'managed-qdrant-v1','backend_id':'joint-knowledge-qdrant','backend_revision':1,
                 'isolation':'tenant-profile-resource-v1','kind':'qdrant','limits':{'max_bytes':65536,'max_concurrency':4,'timeout_ms':5000},
                 'qdrant':{'collection':'joint_knowledge','dimensions':3,'distance':'cosine','endpoint':'http://127.0.0.1:61833','vector_name':'published_dense'},
                 'schema_version':'v1','tenant_id':'tnt_tEuv3QNK_7TnYqouV8sSFwOa'}
        resource={'backend':backend,'credential':{'audience_digest':'sha256:2bdb446562572c275b6e20a2bd594539e7168926e684895a192c6a75aec343bf'},
                  'embedding':{'base_url':'http://127.0.0.1:61921/v1','model':'joint-embedding-fixture','dimensions':3}}
        scope=f.scope_for(backend['tenant_id'],'rpf_PtwopBFNIC6IpFSZZaBrFrAK','docs',resource)
        self.assertEqual(scope,'e5721290740703ae1bcb03a6f35173ed1007835982ddbd21d3711275a6fb9680')
        self.assertNotEqual(scope,f.scope_for('other-tenant','rpf_PtwopBFNIC6IpFSZZaBrFrAK','docs',resource))
        self.assertEqual(backend['tenant_id'],'tnt_tEuv3QNK_7TnYqouV8sSFwOa')

    def test_points_require_scope_canary_and_equal_vectors(self):
        scopes={role:'scope-'+role for role in f.BRANCHES}
        points=[{'id':role,'payload':{'worker_scope':scopes[role],'document':{'content':f.TEXTS[role]}},'vector':{f.VECTOR_NAME:[1,0,0]}} for role in f.BRANCHES]
        f.require_points(points,scopes)
        for mutation in ('scope','canary','vector','extra'):
            bad=copy.deepcopy(points)
            if mutation=='scope':bad[1]['payload']['worker_scope']=scopes[f.BRANCHES[0]]
            if mutation=='canary':bad[1]['payload']['document']['content']=f.TEXTS[f.BRANCHES[0]]
            if mutation=='vector':bad[1]['vector'][f.VECTOR_NAME]=[0,1,0]
            if mutation=='extra':bad.append(copy.deepcopy(points[0]))
            with self.subTest(mutation=mutation), self.assertRaises(AssertionError):f.require_points(bad,scopes)

    def test_tool_allowlist_and_results_reject_sibling_leak(self):
        request=self.request('research_a')
        f.require_declaration(request,'research_a')
        bad=copy.deepcopy(request);bad['tools'][0]['function']['name']=f.CALLABLES['research_b']
        with self.assertRaises(AssertionError):f.require_declaration(bad,'research_a')
        result={'documents':[{'text':f.TEXTS['research_a'],'id':'real-doc','score':1}]}
        self.assertEqual(f.branch_output('research_a',result),'KNOWLEDGE_BRANCH_A\n'+f.TEXTS['research_a'])
        result['documents'].append({'text':f.TEXTS['research_b'],'id':'other','score':1})
        with self.assertRaises(AssertionError):f.branch_output('research_a',result)

    @staticmethod
    def request(role):
        body={'model':f.MODELS[role],'max_completion_tokens':4096,'messages':[
            {'role':'system','content':f.INSTRUCTIONS[role]},{'role':'user','content':f.NORMAL}]}
        if role in f.BRANCHES:
            body['tools']=[{'type':'function','function':{'name':f.CALLABLES[role],
                'parameters':{'type':'object','required':['query'],'properties':{'query':{'type':'string'}}}}}]
        return body

    def test_actual_http_parallel_barrier_results_and_cleanup(self):
        h=Mock();h.secret.side_effect=['embed-model-key','embedding-key','a-key','b-key','agg-key']
        model=f.ParallelKnowledgeModelFixture(h);port=model.server.server_port
        def post(path,body,key):
            req=urllib.request.Request(model.url+path,data=json.dumps(body).encode(),headers={'Authorization':'Bearer '+key})
            try:response=urllib.request.urlopen(req,timeout=12)
            except urllib.error.HTTPError as error:
                error.close();raise
            with response as r:
                raw=r.read().decode()
                return next(json.loads(line[6:])['choices'][0]['delta'] for line in raw.splitlines() if line.startswith('data: {'))
        try:
            requests={role:self.request(role) for role in f.ROLES}
            with ThreadPoolExecutor(max_workers=2) as pool:
                pending={role:pool.submit(post,'/v1/chat/completions',requests[role],model.keys[role]) for role in f.BRANCHES}
                calls={role:job.result() for role,job in pending.items()}
            for role in f.BRANCHES:
                self.assertEqual(calls[role]['tool_calls'][0]['function']['name'],f.CALLABLES[role])
                requests[role]['messages'].append({'role':'tool','content':json.dumps({'documents':[{'text':f.TEXTS[role],'id':'actual-'+role,'score':1}]})})
                output=post('/v1/chat/completions',requests[role],model.keys[role])['content']
                requests['aggregator']['messages'].append({'role':'user','content':output})
            final=post('/v1/chat/completions',requests['aggregator'],model.keys['aggregator'])['content']
            exchange=f.require_exchange(model.snapshot(),4096)
            self.assertEqual(final,exchange['terminal_output'])
            self.assertGreater(exchange['initial_http_overlap_ns'],0)
            for change in ('extra','lost_branch','no_overlap'):
                bad=copy.deepcopy(model.snapshot())
                if change=='extra':bad.append(copy.deepcopy(bad[-1]))
                if change=='lost_branch':bad[-1]['request']['messages'].pop()
                if change=='no_overlap':bad[0]['finished_ns']=bad[1]['started_ns']-1
                with self.subTest(change=change),self.assertRaises(AssertionError):f.require_exchange(bad,4096)
            late=copy.deepcopy(model.snapshot())
            aggregate=next(c for c in late if c['role']=='aggregator')
            for c in late:
                if c['phase']=='result':c['finished_ns']=aggregate['started_ns']+100
            f.require_exchange(late,4096)
            late[0]['response_started_ns']=aggregate['started_ns']+100
            with self.assertRaises(AssertionError):f.require_exchange(late,4096)
            wrong=self.request('research_a')
            with self.assertRaises(urllib.error.HTTPError) as error:post('/v1/chat/completions',wrong,model.keys['research_b'])
            error.exception.close();self.assertEqual(error.exception.code,401)
            model.assert_healthy()
        finally:model.close()
        with socket.socket() as s:self.assertNotEqual(s.connect_ex(('127.0.0.1',port)),0)
        self.assertFalse(model.thread.is_alive())
        self.assertFalse(model.embedding.thread.is_alive())

    def test_tenant_controls_hold_profile_resource_and_read_real_filters(self):
        h=Mock();h.extra_tenant_ids=['tenant-b'];h.deployment_id='deployment-a'
        backend={'schema_version':'v1','tenant_id':'tenant-a','backend_id':f.BACKEND_ID,'backend_revision':1,
            'kind':'qdrant','adapter':'managed-qdrant-v1','isolation':'tenant-profile-resource-v1',
            'limits':{'timeout_ms':5000,'max_concurrency':4,'max_bytes':65536},
            'qdrant':{'endpoint':'http://127.0.0.1:19103','collection':f.COLLECTION,'vector_name':f.VECTOR_NAME,'dimensions':3,'distance':'cosine'}}
        resource={'backend':backend,'credential':{'audience_digest':f.backend_digest(backend)},
            'embedding':{'model':f.EMBEDDING_MODEL,'base_url':'http://127.0.0.1:19102/v1','dimensions':3}}
        content={'tenant_id':'tenant-a','sources':{'profile':{'profile_id':'profile-a'}},
            'resources':{'knowledge':{slot:copy.deepcopy(resource) for slot in f.RESOURCES.values()}}}
        scopes={role:f.scope_for('tenant-a','profile-a',slot,resource) for role,slot in f.RESOURCES.items()}
        points=[{'id':role,'payload':{'worker_scope':scopes[role],'document':{'content':f.TEXTS[role]}},
                 'vector':{f.VECTOR_NAME:[1,0,0]}} for role in f.BRANCHES]
        calls=[]
        def search(method,path,body):
            calls.append(copy.deepcopy(body))
            scope=body['filter']['must'][0]['match']['value']
            return {'result':[{'id':p['id'],'payload':copy.deepcopy(p['payload'])} for p in points if p['payload']['worker_scope']==scope]}
        h.qdrant_request.side_effect=search;h.knowledge_state.return_value=points;h.api.return_value={'code':'NOT_FOUND'}
        result=f.tenant_controls(h,content,points)
        self.assertEqual(result['result'],'PASS');self.assertEqual(len(calls),4)
        for role,offset in zip(f.BRANCHES,(0,2)):
            self.assertEqual(calls[offset]['vector'],calls[offset+1]['vector'])
            self.assertNotEqual(calls[offset]['filter'],calls[offset+1]['filter'])
            self.assertEqual(calls[offset+1]['filter']['must'][0]['match']['value'],
                f.scope_for('tenant-b','profile-a',f.RESOURCES[role],resource))
        self.assertEqual(h.api.call_args.args[:2],('POST','/v1/tenants/tenant-b/deployments/deployment-a/revisions/1/knowledge/docs_a/import'))
        self.assertEqual(h.api.call_args.kwargs,{'status':404})
        # A broad/no-op filter that leaks a real original point must fail.
        h.qdrant_request.side_effect=lambda *_:{'result':[{'id':points[0]['id'],'payload':points[0]['payload']}]}
        with self.assertRaisesRegex(AssertionError,'foreign Tenant'):f.tenant_controls(h,content,points)

    def test_cleanup_runs_parent_even_when_provider_close_raises(self):
        h=f.ParallelKnowledgeHarness.__new__(f.ParallelKnowledgeHarness)
        h.model=Mock();original=h.model;h.model.close.side_effect=RuntimeError('fixture close failure')
        h.record=Mock();h.redact=lambda s:s
        with patch.object(f.KnowledgeHarness,'close') as close:
            with self.assertRaisesRegex(RuntimeError,'fixture close failure'):h.close()
        close.assert_called_once();self.assertIs(h.model,original)
        self.assertEqual(h.record.call_args.args[1]['result'],'FAIL')


if __name__=='__main__':unittest.main()
