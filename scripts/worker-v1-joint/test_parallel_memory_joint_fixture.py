"""Public fixture seams; no service stack or external model starts in units."""
import copy
import importlib.util
from pathlib import Path
import json
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch
import parallel_memory_joint_fixture as f

class ParallelMemoryTests(unittest.TestCase):
    def test_public_graph_credentials_whitelists_and_backend_choice(self):
        for cls, backend in ((f.ParallelMemoryHarness, 'joint-memory-pg'), (f.ParallelRedisMemoryHarness, 'joint-memory-redis')):
            h=cls.__new__(cls);h.model=type('Model',(),{'url':'http://127.0.0.1:12345','keys':dict(zip(f.ROLES,['a-key','b-key','c-key']))})();h.memory_password='memory-private'
            body={'spec':{'schema_version':'v1'}}
            with patch.object(f.Harness,'api',return_value={}) as api:h.api('PUT','/v1/tenants/t/agents/a/draft',body)
            spec=api.call_args.args[3]['spec'];self.assertEqual(body,{'spec':{'schema_version':'v1'}})
            self.assertEqual(spec['nodes']['workflow'],{'kind':'sequence','children':['parallel','aggregator']})
            self.assertEqual(spec['nodes']['parallel'],{'kind':'parallel','children':['writer_a','writer_b']})
            for role in f.ROLES:self.assertEqual(spec['nodes'][role]['memory'],{'tools':f.ALLOWED[role],'preload_limit':0})
            profile={'config':{'storage':{'session':{'kind':'postgres_state'}}},'credentials':{'storage':{'session':{'dsn':{'action':'keep'}}}}}
            with patch.object(f.Harness,'api',return_value={}) as api:h.api('PUT','/v1/tenants/t/runtime-profiles/p/draft',profile)
            sent=api.call_args.args[3];self.assertEqual(sent['config']['storage']['memory']['backend_id'],backend)
            self.assertEqual([v['api_key']['value'] for v in sent['credentials']['models'].values()],['a-key','b-key','c-key'])
            self.assertEqual(sent['credentials']['storage']['memory']['dsn_password']['value'],'memory-private')
            self.assertEqual(sent['config']['storage']['session'],profile['config']['storage']['session'])
            self.assertEqual(set(sent['config']['models']),set(f.SLOTS.values()))

    def test_actual_http_parallel_tool_results_union_and_private_add_before_401(self):
        keys=iter(['a-key','b-key','c-key']);model=f.ParallelMemoryModel(type('H',(),{'secret':lambda self:next(keys)})())
        def request(role,case):
            return {'model':f.MODELS[role],'stream':True,'max_completion_tokens':4096,
                'messages':[{'role':'system','content':f.instruction(role)},{'role':'user','content':case}],
                'tools':[{'type':'function','function':{'name':name,'parameters':{'type':'object'}}} for name in f.ALLOWED[role]]}
        def post(role,q):
            req=urllib.request.Request(model.url+'/v1/chat/completions',data=json.dumps(q).encode(),headers={'Authorization':'Bearer '+model.keys[role]})
            try:r=urllib.request.urlopen(req,timeout=15)
            except urllib.error.HTTPError as error:r=error
            with r:code,raw=r.status,r.read().decode()
            chunks=[json.loads(x[6:]) for x in raw.splitlines() if x.startswith('data: {')]
            return code,chunks[0]['choices'][0]['delta'] if chunks else None
        loaded={'count':2,'results':[{'id':role,'memory':value} for role,value in f.VALUES.items()]}
        def follow(q,delta,result):
            call=delta['tool_calls'][0];q['messages'] += [{'role':'assistant','tool_calls':[call]}, {'role':'tool','tool_call_id':call['id'],'content':json.dumps(result)}]
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                for case in (f.MUTATE,f.READ,f.FAILURE):
                    offset=len(model.snapshot());qa,qb=request('writer_a',case),request('writer_b',case)
                    a=pool.submit(post,'writer_a',qa);b=pool.submit(post,'writer_b',qb)
                    code,da=a.result(12);self.assertEqual(code,200)
                    result=loaded if case==f.READ else {'message':'Memory added successfully','memory':f.POISON if case==f.FAILURE else f.VALUES['writer_a'],'topics':[]}
                    follow(qa,da,result);self.assertEqual(post('writer_a',qa)[0],200)
                    code,db=b.result(12)
                    if case==f.FAILURE:self.assertEqual(code,401)
                    else:
                        self.assertEqual(code,200);follow(qb,db,loaded if case==f.READ else {'message':'Memory added successfully','memory':f.VALUES['writer_b'],'topics':[]});self.assertEqual(post('writer_b',qb)[0],200)
                        q=request('aggregator',case);q['messages'] += [{'role':'user','content':f.branch_output(role,case)} for role in f.BRANCHES]
                        code,delta=post('aggregator',q);self.assertEqual(code,200);follow(q,delta,loaded);self.assertEqual(post('aggregator',q)[0],200)
                    self.assertTrue(model.wait_idle(3));calls=model.snapshot()[offset:];report=f.require_exchange(calls,case,4096)
                    self.assertEqual(report['failed'],case==f.FAILURE)
            self.assertFalse(model.errors)
        finally:model.close()

    def runner(self):
        spec=importlib.util.spec_from_file_location('parallel_memory_unit',Path(__file__).resolve().parents[1]/'test-worker-parallel-memory-joint.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

    def test_exact_scope_revision_entries_and_no_duplicate_or_poison(self):
        q={'Route':{'TenantID':'tenant','Provider':'telegram','AccountID':'account'},'Input':{'SenderID':'100'}}
        scope=f.expected_scope(q,'agent')
        entries=[{'id':role,'app_name':'tenant','user_id':scope,'memory':{'memory':value}} for role,value in f.VALUES.items()]
        state=[{'tenant_id':'tenant','scope_id':scope,'revision':2,'content':{'scope':{'tenant_id':'tenant','scope_id':scope},'base_revision':1,'entries':entries}}]
        self.assertEqual(f.require_memory_state(state,q,'agent',2),entries)
        for mutation in ('tenant','scope','revision','base','missing','duplicate','poison'):
            wrong=copy.deepcopy(state)
            if mutation=='tenant':wrong[0]['tenant_id']='other'
            if mutation=='scope':wrong[0]['content']['entries'][0]['user_id']='other'
            if mutation=='revision':wrong[0]['revision']=3
            if mutation=='base':wrong[0]['content']['base_revision']=0
            if mutation=='missing':wrong[0]['content']['entries'].pop()
            if mutation=='duplicate':wrong[0]['content']['entries'][1]['id']=wrong[0]['content']['entries'][0]['id']
            if mutation=='poison':wrong[0]['content']['entries'][0]['memory']['memory']=f.POISON
            with self.subTest(mutation=mutation),self.assertRaises(AssertionError):f.require_memory_state(wrong,q,'agent',2)
        for change in ('tenant','account','sender','agent'):
            other=copy.deepcopy(q);agent='agent'
            if change=='tenant':other['Route']['TenantID']='other'
            if change=='account':other['Route']['AccountID']='other'
            if change=='sender':other['Input']['SenderID']='101'
            if change=='agent':agent='other'
            self.assertNotEqual(f.expected_scope(other,agent),scope)

    def test_formal_memory_receipt_maps_both_backend_wire_without_reconstruction(self):
        runner=self.runner();body={'scope':{'tenant_id':'tenant','scope_id':'scope'},'base_revision':1,'entries':[]}
        pg={'tenant_id':'tenant','completion_id':'completion','run_id':'run','attempt_id':'attempt','scope_id':'scope','candidate_digest':'sha256:fixed','applied_revision':2,'raw_content':json.dumps(body)}
        redis={'completion_id':'completion','run_id':'run','attempt_id':'attempt','digest':'sha256:fixed','revision':'2','body':json.dumps(body)}
        expected={'completion_id':'completion','run_id':'run','attempt_id':'attempt','digest':'sha256:fixed','revision':2,'body':body}
        for backend,after in [('postgres',{'receipts':[pg]}),('redis',[{'key':'runtime_memory:{tenant}:receipt:key','record':redis}])]:
            h=type('H',(),{'backend':backend})();r={'run_id':'run','backend_after':after}
            self.assertEqual(runner.memory_receipt(h,r),expected)
            wrong=copy.deepcopy(r)
            if backend=='postgres':wrong['backend_after']['receipts']*=2
            else:wrong['backend_after']*=2
            with self.assertRaises(AssertionError):runner.memory_receipt(h,wrong)

    def test_failed_private_add_cannot_advance_any_formal_memory_or_session_fact(self):
        runner=self.runner();before={'accepted_ref':'old','accepted_digest':'sha256:old','settled_sequence':2}
        state={'input':f.FAILURE,'run':{'run_id':'run','status':'FAILED','attempts':1,'session_sequence':3,'current_attempt_id':'attempt'},
            'attempts':[{'attempt_id':'attempt','status':'FAILED','reason':'RUNTIME_FAILED'}],
            'completions':[{'completion_id':'completion','attempt_id':'attempt','status':'FAILED','reason':'RUNTIME_FAILED','kind':'ATTEMPT','reply_disposition':'FINAL','final_intent_id':'final','candidate_ref':'','candidate_digest':''}],
            'outboxes':[{'intent_id':'final','payload':{'execution':{'completion_id':'completion','attempt_id':'attempt'},'content':{'text':runner.FAILURE_FINAL}}}],
            'memory_completion':[{'memory_status':'','memory_digest':''}],'candidates':[],
            'head_before':before,'head_after':dict(before,settled_sequence=3),'memory_before':[{'revision':2,'content':{'entries':['accepted']}}],
            'memory_after':[{'revision':2,'content':{'entries':['accepted']}}],'backend_before':['actual immutable raw bytes'],'backend_after':['actual immutable raw bytes'],
            'delivery':{'run_id':'run','intent_id':'final','delivery_state':'ACCEPTED','final_text':runner.FAILURE_FINAL},'exchange':{'failed':True}}
        self.assertIsNone(runner.require_facts(state,None))
        for mutation in ('candidate','head','revision','raw_bytes','memory_gate','success_final'):
            wrong=copy.deepcopy(state)
            if mutation=='candidate':wrong['candidates']=[{}]
            if mutation=='head':wrong['head_after']['accepted_digest']='new'
            if mutation=='revision':wrong['memory_after'][0]['revision']=3
            if mutation=='raw_bytes':wrong['backend_after']=['changed']
            if mutation=='memory_gate':wrong['memory_completion'][0]['memory_status']='APPLIED'
            if mutation=='success_final':wrong['delivery']['final_text']='private add succeeded'
            with self.subTest(mutation=mutation),self.assertRaises(AssertionError):runner.require_facts(wrong,None)

    def test_cleanup_failure_remains_failure_after_base_cleanup(self):
        h=f.ParallelMemoryHarness.__new__(f.ParallelMemoryHarness);h.processes=[];h.model=type('Model',(),{'errors':['fixture error']})();saved={}
        h.record=lambda name,value:saved.update({name:value})
        with patch.object(f.MemoryHarness,'close') as cleanup:
            with self.assertRaises(RuntimeError):h.close()
        cleanup.assert_called_once();self.assertEqual(saved['parallel-memory-cleanup.json']['result'],'FAIL')

if __name__=='__main__':unittest.main()
