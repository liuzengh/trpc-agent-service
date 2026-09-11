"""Parallel SDK Memory normal-path acceptance against existing PG/Redis adapters.

The model is a deterministic HTTP dependency; tool results come from the actual
SDK. Backend provisioning, catalog, lifecycle and Lab reuse existing helpers.
"""
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import threading
import time
from harness import Harness
from memory_joint_fixture import MemoryHarness
from redis_memory_joint_fixture import RedisMemoryHarness
from sequence_joint_fixture import text_content, all_strings, require_accepted_history
from faults import rows

ROLES = ('writer_a','writer_b','aggregator')
BRANCHES = ROLES[:2]
SLOTS = dict(zip(ROLES,('primary','secondary','aggregate')))
MODELS = {role:'parallel-memory-'+role for role in ROLES}
ALLOWED = {role:['memory_add','memory_load'] if role in BRANCHES else ['memory_load'] for role in ROLES}
MUTATE = 'PARALLEL_MEMORY_CASE=mutate；两个分支分别保存独立内容，汇总读取两条实际记忆。'
READ = 'PARALLEL_MEMORY_CASE=read；所有节点只读取正式持久化记忆，不再新增。'
FAILURE = 'PARALLEL_MEMORY_CASE=failure；A私有写成功后B返回模型错误，整个Run不接受。'
VALUES = {'writer_a':'PAR_MEMORY_A: orchid is violet.\n独立甲 "A"',
          'writer_b':'PAR_MEMORY_B: tea is jasmine.\n独立乙 \\B'}
POISON = 'PAR_MEMORY_FAILED_PRIVATE_ADD: this must never become accepted.'

def instruction(role):
    return 'PAR_MEMORY_NODE='+role+': Follow the current test request using only the selected Memory tools. Use actual tool results. No preload. Only the explicit aggregator produces the final user answer.'

def branch_output(role, case):
    return 'PAR_MEMORY_BRANCH_'+role+': '+(POISON if case==FAILURE else VALUES[role])

def final_output(case):
    return 'PAR_MEMORY_FINAL '+('mutate' if case==MUTATE else 'read')+'\n'+VALUES['writer_a']+'\n'+VALUES['writer_b']

class _ParallelMemory:
    def prepare_parallel_memory(self):
        self.model.close();self.model=ParallelMemoryModel(self);self.urls['model']=self.model.url
    def record(self,name,value):
        path=self.artifacts/name;path.write_text(self.redact(json.dumps(value,ensure_ascii=False,indent=2))+'\n');path.chmod(0o600);json.loads(path.read_text())
    def api(self,method,path,body=None,status=200,idem=None):
        if method=='PUT' and '/agents/' in path and path.endswith('/draft'):
            body=copy.deepcopy(body);spec=body['spec'];spec['root']='workflow'
            spec['requirements']={'models':{slot:{'capabilities':['chat','tool_call']} for slot in SLOTS.values()},'tools':{},'knowledge':{}}
            spec['nodes']={'workflow':{'kind':'sequence','children':['parallel','aggregator']},'parallel':{'kind':'parallel','children':list(BRANCHES)}}
            for role in ROLES:spec['nodes'][role]={'kind':'llm','instruction':instruction(role),'model_slot':SLOTS[role],'tool_slots':[],'knowledge_slots':[],'memory':{'tools':list(ALLOWED[role]),'preload_limit':0}}
        if method=='PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body=copy.deepcopy(body);body['config']['models']={SLOTS[role]:{'kind':'openai_compatible','model':MODELS[role],'base_url':self.model.url+'/v1','capabilities':['chat','tool_call']} for role in ROLES}
            body['credentials']['models']={SLOTS[role]:{'api_key':{'action':'replace','value':self.model.keys[role]}} for role in ROLES}
            body['config']['storage']['memory']={'kind':'managed_memory','backend_id':self.memory_backend_id,'backend_revision':1}
            body['credentials']['storage']['memory']={'dsn_password':{'action':'replace','value':self.memory_password}}
        # Retain existing catalog/restart behavior without the single-node hook.
        result=Harness.api(self,method,path,body,status,idem)
        if method=='POST' and path=='/v1/admin/tenants':self.bind_memory_catalog(result['id'])
        return result
    def backend_state(self):
        if self.backend=='redis':return RedisMemoryHarness.storage_state(self)
        return {'heads':rows(self,"SELECT tenant_id,scope_id,revision,digest,convert_from(content,'UTF8') AS raw_content FROM runtime_memory.memory_heads ORDER BY tenant_id,scope_id"),
            'receipts':rows(self,"SELECT tenant_id,completion_id,run_id,attempt_id,scope_id,candidate_digest,applied_revision,convert_from(content,'UTF8') AS raw_content FROM runtime_memory.memory_receipts ORDER BY tenant_id,completion_id")}
    def close(self):
        error=None
        try:super().close()
        except BaseException as exc:error=exc
        if getattr(getattr(self,'model',None),'errors',[]) and error is None:error=RuntimeError(str(self.model.errors))
        reaped=all(p.poll() is not None for _,p in self.processes)
        if not reaped and error is None:error=RuntimeError('owned process still active')
        self.record('parallel-memory-cleanup.json',{'result':'FAIL' if error else 'PASS','backend':self.backend,'processes_reaped':reaped})
        if error:raise error

class ParallelMemoryHarness(_ParallelMemory, MemoryHarness):
    backend='postgres'
    memory_backend_id='joint-memory-pg'

class ParallelRedisMemoryHarness(_ParallelMemory, RedisMemoryHarness):
    backend='redis'
    memory_backend_id='joint-memory-redis'

def role_of(request):
    system='\n'.join(text_content(m.get('content')) for m in request['messages'] if m.get('role') in ('system','developer'))
    roles=[role for role in ROLES if 'PAR_MEMORY_NODE='+role+':' in system]
    assert len(roles)==1,'model request lost node identity'
    return roles[0]

def current_case(request):
    choices=[text_content(m.get('content')) for m in request['messages'] if m.get('role')=='user' and text_content(m.get('content')) in (MUTATE,READ,FAILURE)]
    assert choices,'current parallel Memory input absent'
    return choices[-1]

def tool_results(request,case):
    messages=request['messages'];start=max(i for i,m in enumerate(messages) if m.get('role')=='user' and text_content(m.get('content'))==case)
    current=messages[start+1:];calls={call['id']:call['function'] for m in current for call in m.get('tool_calls',[])}
    results=[]
    for message in current:
        if message.get('role')=='tool':
            assert message['tool_call_id'] in calls,'unmatched SDK tool result'
            value=json.loads(message['content']);assert isinstance(value,dict) and not value.get('error')
            results.append({'id':message['tool_call_id'],'name':calls[message['tool_call_id']]['name'],'arguments':json.loads(calls[message['tool_call_id']]['arguments']),'result':value})
    return results

def require_load(value):
    assert value['count']==2 and len(value['results'])==2
    assert {row['memory'] for row in value['results']}==set(VALUES.values())
    assert len({row['id'] for row in value['results']})==2

def require_declaration(request,role):
    assert sorted(t['function']['name'] for t in request.get('tools',[]))==sorted(ALLOWED[role])
    assert request['model']==MODELS[role]

class ParallelMemoryModel:
    def __init__(self,h):
        self.keys={role:h.secret() for role in ROLES};self.key=self.keys['writer_a']
        self.records=[];self.errors=[];self.lock=threading.Condition();self.active=set();self.barriers={};self.private_add_finished=threading.Event();owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                if self.path!='/v1/chat/completions':self.send_error(404);return
                record=None;case=None;role=None;results=[]
                with owner.lock:owner.active.add(self.connection)
                try:
                    self.connection.settimeout(15)
                    q=json.loads(self.rfile.read(int(self.headers['Content-Length'])));role=role_of(q)
                    if self.headers.get('Authorization')!='Bearer '+owner.keys[role]:self.send_error(401);return
                    require_declaration(q,role);case=current_case(q);results=tool_results(q,case);assert len(results)<=1
                    record={'request':copy.deepcopy(q),'role':role,'phase':'output' if results else 'tool_select','authenticated':True,'started_ns':time.monotonic_ns(),'status':None,'text':'','complete':False,'tool_results':copy.deepcopy(results)}
                    with owner.lock:
                        owner.records.append(record);ordinal=len(owner.records)
                        barrier=owner.barriers.setdefault(case,threading.Barrier(2))
                    if role in BRANCHES and not results:barrier.wait(timeout=10)
                    if case==FAILURE and role=='writer_b':
                        assert not results and owner.private_add_finished.wait(timeout=10),'A did not finish after real private add result'
                        with owner.lock:record['private_add_precedes_rejection']=True
                        payload=b'{"error":{"message":"branch B rejected after A private add","type":"authentication_error"}}';status=401;ctype='application/json';text='';usage=None
                    else:
                        if role=='aggregator':
                            assert case!=FAILURE
                            for branch in BRANCHES:assert any(branch_output(branch,case) in text_content(m.get('content')) for m in q['messages'])
                        if not results:
                            name='memory_load' if role=='aggregator' or case==READ else 'memory_add'
                            args={} if name=='memory_load' else {'memory':POISON if case==FAILURE else VALUES[role],'topics':['parallel-fixture']}
                            delta={'role':'assistant','tool_calls':[{'index':0,'id':'parallel-memory-call-'+str(ordinal),'type':'function','function':{'name':name,'arguments':json.dumps(args)}}]};text='';finish='tool_calls'
                        else:
                            result=results[0]
                            if role=='aggregator' or case==READ:
                                assert result['name']=='memory_load' and result['arguments']=={};require_load(result['result'])
                            else:
                                expected=POISON if case==FAILURE else VALUES[role]
                                assert result['name']=='memory_add' and result['arguments']['memory']==expected
                                assert result['result']['message']=='Memory added successfully' and result['result']['memory']==expected
                                if case==FAILURE:
                                    with owner.lock:record['private_add_observed_ns']=time.monotonic_ns()
                            text=final_output(case) if role=='aggregator' else branch_output(role,case);delta={'role':'assistant','content':text};finish='stop'
                        usage={'prompt_tokens':12,'completion_tokens':8,'total_tokens':20}
                        chunks=[{'id':'parallel-memory-response-'+str(ordinal),'object':'chat.completion.chunk','model':q['model'],'choices':[{'index':0,'delta':delta,'finish_reason':finish}]}, {'id':'parallel-memory-response-'+str(ordinal),'object':'chat.completion.chunk','model':q['model'],'choices':[],'usage':usage}]
                        payload=(''.join('data: '+json.dumps(chunk)+'\n\n' for chunk in chunks)+'data: [DONE]\n\n').encode();status=200;ctype='text/event-stream'
                    with owner.lock:record['response_started_ns']=time.monotonic_ns()
                    self.send_response(status);self.send_header('Content-Type',ctype);self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload);self.wfile.flush()
                    with owner.lock:record.update(status=status,text=text,usage=usage,complete=True,termination='response_complete')
                except Exception as exc:
                    with owner.lock:
                        owner.errors.append(type(exc).__name__+': '+str(exc))
                        if record is not None:record.update(complete=False,termination='fixture_error')
                    try:self.send_error(400,'parallel Memory fixture assertion')
                    except OSError:pass
                finally:
                    with owner.lock:
                        if record is not None:record['finished_ns']=time.monotonic_ns()
                        owner.active.discard(self.connection);owner.lock.notify_all()
                        if case==FAILURE and role=='writer_a' and results and record and record.get('complete'):owner.private_add_finished.set()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.server.daemon_threads=True;self.url='http://127.0.0.1:'+str(self.server.server_port)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
    def snapshot(self):
        with self.lock:return copy.deepcopy(self.records)
    def wait_idle(self,timeout):
        with self.lock:return self.lock.wait_for(lambda:not self.active,timeout=timeout)
    def close(self):
        for barrier in self.barriers.values():barrier.abort()
        self.server.shutdown()
        with self.lock:sockets=list(self.active)
        for connection in sockets:
            try:connection.shutdown(socket.SHUT_RDWR)
            except OSError:pass
        self.server.server_close();self.thread.join(timeout=5)
        if self.thread.is_alive() or not self.wait_idle(15):self.errors.append('model handlers not reaped')

def require_exchange(calls,case,limit):
    grouped={role:[] for role in ROLES}
    for call in calls:
        q=call['request'];role=role_of(q);require_declaration(q,role);assert current_case(q)==case
        assert q['max_completion_tokens']==limit and 'max_tokens' not in q
        assert call['authenticated'] and call['complete'] and call['termination']=='response_complete' and 0<call['started_ns']<call['finished_ns']
        grouped[role].append(call)
    for group in grouped.values():group.sort(key=lambda c:c['started_ns'])
    a,b=grouped['writer_a'],grouped['writer_b'];assert a and b
    assert max(a[0]['started_ns'],b[0]['started_ns'])<min(a[0]['finished_ns'],b[0]['finished_ns']),'initial branches did not overlap'
    assert len(a)==2 and all(call['status']==200 and call['usage']['total_tokens']>0 for call in a)
    if case==FAILURE:
        assert len(calls)==3 and len(b)==1 and not grouped['aggregator']
        assert b[0]['status']==401 and b[0]['private_add_precedes_rejection']
        assert a[-1]['private_add_observed_ns']<a[-1]['finished_ns']<b[0]['response_started_ns']
        assert a[-1]['tool_results'][0]['result']['memory']==POISON
    else:
        assert len(calls)==6 and len(b)==len(grouped['aggregator'])==2
        assert all(c['status']==200 and c['usage']['total_tokens']>0 for c in calls)
    for role in BRANCHES:
        group=grouped[role]
        if len(group)<2:continue
        results=tool_results(group[-1]['request'],case);assert len(results)==1
        if case==READ:assert results[0]['name']=='memory_load';require_load(results[0]['result'])
        else:
            expected=POISON if case==FAILURE else VALUES[role]
            assert results[0]['name']=='memory_add' and results[0]['result']['memory']==expected and results[0]['result']['message']=='Memory added successfully'
        assert group[-1]['text']==branch_output(role,case)
    if case==FAILURE:return {'failed':True,'private_add_result':a[-1]['tool_results'][0],'terminal_output':None,'branch_outputs':{'writer_a':a[-1]['text']}}
    aggregate=grouped['aggregator'];loaded=tool_results(aggregate[-1]['request'],case);assert len(loaded)==1 and loaded[0]['name']=='memory_load';require_load(loaded[0]['result'])
    for role in BRANCHES:assert any(branch_output(role,case) in text_content(m.get('content')) for m in aggregate[0]['request']['messages'])
    assert aggregate[-1]['text']==final_output(case)
    return {'failed':False,'branch_outputs':{role:grouped[role][-1]['text'] for role in BRANCHES},'terminal_output':aggregate[-1]['text'],'aggregate_load':loaded[0]['result']}

def expected_scope(request,agent_id):
    route,source=request['Route'],request['Input']
    def stable(prefix,parts):
        return prefix+'_'+hashlib.sha256(json.dumps(parts,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
    social=stable('soc',[route['TenantID'],route['Provider'],route['AccountID'],source['SenderID']])
    return stable('mem',['worker-memory-scope/v1',route['TenantID'],social,agent_id])

def require_memory_state(state,request,agent_id,revision):
    assert len(state)==1
    memory=state[0];scope=expected_scope(request,agent_id);tenant=request['Route']['TenantID']
    assert memory['tenant_id']==tenant and memory['scope_id']==scope and memory['revision']==revision
    body=memory['content'];assert body['scope']=={'tenant_id':tenant,'scope_id':scope} and body['base_revision']==revision-1
    entries=body['entries'];assert len(entries)==2 and len({entry['id'] for entry in entries})==2
    assert {entry['memory']['memory'] for entry in entries}==set(VALUES.values())
    assert all(entry['app_name']==tenant and entry['user_id']==scope for entry in entries)
    return entries
