"""Published parallel Knowledge scope proof with real Qdrant, SDK and Lab.

Model/embedding responses are deterministic HTTP fixtures. Equal vectors prevent
similarity from masking resource isolation. Cross-Tenant controls are direct
read-only Qdrant scope probes plus owner HTTP denial, not a second SDK Run.
"""
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import threading
import time

from harness import Harness
from knowledge_joint_fixture import (KnowledgeHarness, KnowledgeModelFixture,
    BACKEND_ID, COLLECTION, VECTOR_NAME, EMBEDDING_MODEL, DIMENSIONS)
from sequence_joint_fixture import all_strings

BRANCHES=('research_a','research_b')
ROLES=BRANCHES+('aggregator',)
SLOTS=dict(zip(ROLES,('primary','secondary','aggregate')))
RESOURCES=dict(zip(BRANCHES,('docs_a','docs_b')))
MODELS={role:'joint-parallel-knowledge-'+role.replace('_','-') for role in ROLES}
CALLABLES={role:'fn_'+hashlib.sha256(('knowledge/'+resource).encode()).hexdigest()[:60]
           for role,resource in RESOURCES.items()}
TEXTS={'research_a':'Resource A contains IRIS-614. Only the A branch may retrieve this canary.',
       'research_b':'Resource B contains LILY-925. Only the B branch may retrieve this canary.'}
NORMAL='PARALLEL_KNOWLEDGE_NORMAL: independently search each selected resource and aggregate both actual results.'
QUERY='What is the fixed knowledge canary?'
INSTRUCTIONS={role:'PARALLEL_KNOWLEDGE_'+role.upper()+': '+(
    'Use only your selected Knowledge tool to search for the fixed canary. Report its actual document verbatim; never use another branch result.'
    if role in BRANCHES else 'You have no tools. Quote both actual branch outputs in declared A then B order. Only your final output is delivered.') for role in ROLES}


def role_of(request):
    text='\n'.join(m.get('content','') for m in request['messages'] if m.get('role') in ('system','developer'))
    matches=[role for role in ROLES if 'PARALLEL_KNOWLEDGE_'+role.upper()+':' in text]
    assert len(matches)==1,'one explicit provider node required'
    return matches[0]


def require_declaration(request,role):
    if role=='aggregator':
        assert not request.get('tools'),'aggregator acquired Knowledge tool'
        assert all(m.get('role')!='tool' and not m.get('tool_calls') for m in request['messages'])
        return
    tools=request.get('tools',[])
    assert len(tools)==1 and tools[0]['function']['name']==CALLABLES[role],'branch callable isolation'
    schema=tools[0]['function']['parameters']
    assert schema['type']=='object' and schema['required']==['query']
    assert set(schema['properties'])=={'query'} and schema['properties']['query']['type']=='string'


def branch_output(role,result):
    docs=result.get('documents')
    assert isinstance(docs,list) and len(docs)==1,'one actual matching document required'
    assert docs[0]['text']==TEXTS[role],'retrieved document crosses resource'
    assert all(text not in raw for other,text in TEXTS.items() if other!=role for raw in all_strings(result))
    return 'KNOWLEDGE_BRANCH_'+role[-1].upper()+'\n'+docs[0]['text']


class ParallelKnowledgeHarness(KnowledgeHarness):
    def prepare_parallel_knowledge(self):
        self.model.close()
        self.model=ParallelKnowledgeModelFixture(self)
        self.urls['model']=self.model.url

    def api(self,method,path,body=None,status=200,idem=None):
        if method=='PUT' and '/agents/' in path and path.endswith('/draft'):
            body=copy.deepcopy(body);spec=body['spec']
            spec['root']='workflow'
            spec['requirements']={'models':{SLOTS[role]:{'capabilities':['chat','tool_call'] if role in BRANCHES else ['chat']} for role in ROLES},
                'tools':{},'knowledge':{resource:{'capability':'knowledge.search'} for resource in RESOURCES.values()}}
            spec['nodes']={'workflow':{'kind':'sequence','children':['research','aggregator']},
                           'research':{'kind':'parallel','children':list(BRANCHES)}}
            for role in ROLES:
                spec['nodes'][role]={'kind':'llm','instruction':INSTRUCTIONS[role],'model_slot':SLOTS[role],
                    'tool_slots':[],'knowledge_slots':[RESOURCES[role]] if role in BRANCHES else []}
        if method=='PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body=copy.deepcopy(body)
            body['config']['models']={SLOTS[role]:{'kind':'openai_compatible','model':MODELS[role],
                'base_url':self.model.url+'/v1','capabilities':['chat','tool_call'] if role in BRANCHES else ['chat']} for role in ROLES}
            body['config']['tools']={}
            body['config']['knowledge']={resource:{'kind':'managed_knowledge','backend_id':BACKEND_ID,'backend_revision':1,
                'embedding':{'model':self.model.embedding_model,'base_url':self.model.embedding_url+'/v1','dimensions':self.model.dimensions}}
                for resource in RESOURCES.values()}
            body['credentials']['models']={SLOTS[role]:{'api_key':{'action':'replace','value':self.model.keys[role]}} for role in ROLES}
            body['credentials']['knowledge']={resource:{'qdrant_api_key':{'action':'replace','value':self.qdrant_key},
                'embedding_api_key':{'action':'replace','value':self.model.embedding_key}} for resource in RESOURCES.values()}
            self.last_profile_write=copy.deepcopy(body)
        # Do not invoke KnowledgeHarness.api: its single assistant/docs seed hook
        # would erase this public tree or add an undeclared resource.
        result=Harness.api(self,method,path,body,status,idem)
        if method=='POST' and path=='/v1/admin/tenants' and not getattr(self,'catalog_tenant',None):
            self.bind_knowledge_catalog(result['id']);self.catalog_tenant=result['id']
        return result

    def close(self):
        # Harness.close calls model.close before reaping services. A fixture
        # assertion must not abort that resource cleanup on the error path.
        errors=[];model=getattr(self,'model',None)
        try:
            if model is not None:
                try:model.close()
                except BaseException as exc:errors.append(exc)
                del self.model
            try:super().close()
            except BaseException as exc:errors.append(exc)
        finally:
            if model is not None:self.model=model
            self.record('parallel-knowledge-provider-cleanup.json',{'result':'FAIL' if errors else 'PASS',
                'model_closed':model is None or bool(getattr(model,'closed',False)),
                'errors':[self.redact(str(e)) for e in errors]})
        if errors:raise RuntimeError('; '.join(self.redact(str(e)) for e in errors))


class ParallelKnowledgeModelFixture:
    """Actual HTTP provider boundary; never fabricates SDK tool return records."""
    embedding_model=EMBEDDING_MODEL
    dimensions=DIMENSIONS

    def __init__(self,h):
        self.embedding=KnowledgeModelFixture(h)
        self.embedding_key=self.embedding.embedding_key
        self.embedding_url=self.embedding.url
        self.keys={role:h.secret() for role in ROLES};self.key=self.keys['research_a']
        self.records=[];self.errors=[];self.lock=threading.RLock()
        self.idle=threading.Condition(self.lock);self.connections=set();self.closed=False
        self.barrier=threading.Barrier(2,timeout=10)
        owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                record=None
                with owner.idle:owner.connections.add(self.connection)
                try:
                    if self.path!='/v1/chat/completions':self.send_error(404);return
                    req=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    role=role_of(req)
                    if self.headers.get('Authorization')!='Bearer '+owner.keys[role]:self.send_error(401);return
                    assert req['model']==MODELS[role]
                    require_declaration(req,role)
                    # SDK foreign branch messages may be represented as user messages.
                    # Locate the exact submitted input, not the last arbitrary user.
                    users=[i for i,m in enumerate(req['messages']) if m.get('role')=='user' and m.get('content')==NORMAL]
                    assert users,'actual original submitted input absent'
                    results=[json.loads(m['content']) for m in req['messages'][users[-1]+1:] if m.get('role')=='tool']
                    phase='aggregate' if role=='aggregator' else 'result' if results else 'select'
                    record={'request':copy.deepcopy(req),'role':role,'phase':phase,'authenticated':True,
                        'started_ns':time.monotonic_ns(),'complete':False,'status':None,'text':'','tool_results':copy.deepcopy(results)}
                    with owner.lock:owner.records.append(record);ordinal=len(owner.records)
                    if role in BRANCHES:
                        sibling=next(r for r in BRANCHES if r!=role)
                        assert not any(TEXTS[sibling] in s for s in all_strings(req['messages'])),'sibling canary entered branch request'
                        if phase=='select':
                            owner.barrier.wait()
                            delta={'tool_calls':[{'index':0,'id':'parallel-knowledge-'+str(ordinal),'type':'function',
                                'function':{'name':CALLABLES[role],'arguments':json.dumps({'query':QUERY})}}]}
                        else:
                            assert len(results)==1
                            delta={'content':branch_output(role,results[0])}
                    else:
                        with owner.lock:
                            outputs={r['role']:r['text'] for r in owner.records if r['role'] in BRANCHES and r['phase']=='result' and r.get('response_started_ns')}
                        assert set(outputs)==set(BRANCHES),'aggregate started without both actual branch responses'
                        assert all(any(output in s for s in all_strings(req['messages'])) for output in outputs.values())
                        delta={'content':'KNOWLEDGE_AGGREGATE\n'+'\n'.join(outputs[role] for role in BRANCHES)}
                    usage={'prompt_tokens':12,'completion_tokens':8,'total_tokens':20}
                    events=[{'id':'parallel-knowledge','object':'chat.completion.chunk','model':req['model'],
                        'choices':[{'index':0,'delta':delta,'finish_reason':'stop' if 'content' in delta else 'tool_calls'}]},
                        {'id':'parallel-knowledge','object':'chat.completion.chunk','model':req['model'],'choices':[],'usage':usage}]
                    raw=(''.join('data: '+json.dumps(ev)+'\n\n' for ev in events)+'data: [DONE]\n\n').encode()
                    with owner.lock:record.update(status=200,text=delta.get('content',''),usage=usage,response_started_ns=time.monotonic_ns())
                    self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(raw)));self.end_headers()
                    self.wfile.write(raw);self.wfile.flush()
                    with owner.lock:record.update(complete=True,finished_ns=time.monotonic_ns())
                except Exception as exc:
                    with owner.lock:
                        owner.errors.append(type(exc).__name__+': '+str(exc))
                        if record is not None:record.update(complete=True,finished_ns=time.monotonic_ns(),error='fixture contract failed')
                    try:self.send_error(400,'parallel Knowledge fixture contract mismatch')
                    except (OSError,ValueError):pass
                finally:
                    with owner.idle:owner.connections.discard(self.connection);owner.idle.notify_all()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.server.daemon_threads=True
        self.url='http://127.0.0.1:'+str(self.server.server_port)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()

    def snapshot(self):
        with self.lock:return copy.deepcopy(self.records)
    def embeddings(self):return self.embedding.embeddings()
    def assert_healthy(self):
        with self.lock:assert not self.errors,self.errors
        assert not self.embedding.errors,self.embedding.errors
    def close(self):
        if self.closed:return
        self.closed=True;errors=[]
        self.barrier.abort()
        try:
            self.server.shutdown()
            with self.idle:
                for connection in list(self.connections):
                    try:connection.shutdown(socket.SHUT_RDWR)
                    except OSError:pass
                if not self.idle.wait_for(lambda:not self.connections,timeout=5):errors.append('model HTTP handlers did not exit')
            self.server.server_close();self.thread.join(timeout=5)
            if self.thread.is_alive():errors.append('model provider thread did not exit')
        except BaseException as exc:errors.append(str(exc))
        try:self.embedding.close()
        except BaseException as exc:errors.append(str(exc))
        try:self.assert_healthy()
        except BaseException as exc:errors.append(str(exc))
        if errors:raise RuntimeError('; '.join(errors))


def require_exchange(calls,limit):
    assert len(calls)==5,'two select/two result/one aggregator requests required'
    assert all(c.get('complete') and c.get('finished_ns') and c['status']==200 and c['authenticated'] and not c.get('error') for c in calls)
    grouped={role:sorted([c for c in calls if c['role']==role],key=lambda c:c['started_ns']) for role in ROLES}
    assert [len(grouped[r]) for r in ROLES]==[2,2,1]
    for role in ROLES:
        for call in grouped[role]:
            assert role_of(call['request'])==role
            require_declaration(call['request'],role)
            assert call['request']['model']==MODELS[role]
            assert call['request']['max_completion_tokens']==limit and 'max_tokens' not in call['request']
    first=[grouped[r][0] for r in BRANCHES]
    overlap=min(c['finished_ns'] for c in first)-max(c['started_ns'] for c in first)
    assert overlap>0,'actual branch HTTP intervals did not overlap'
    outputs={}
    for role in BRANCHES:
        select,result=grouped[role]
        assert select['phase']=='select' and result['phase']=='result'
        assert select['response_started_ns']<result['started_ns']
        assert len(result['tool_results'])==1
        outputs[role]=branch_output(role,result['tool_results'][0]);assert result['text']==outputs[role]
        sibling=next(r for r in BRANCHES if r!=role)
        assert not any(TEXTS[sibling] in s for s in all_strings(result['request']['messages']))
    aggregate=grouped['aggregator'][0]
    assert aggregate['phase']=='aggregate'
    # The SDK may consume complete response bytes before the server thread
    # records its post-flush finished timestamp. Compare response causality,
    # not cross-thread finally ordering; all finished records are still required.
    assert aggregate['started_ns']>max(grouped[r][-1]['response_started_ns'] for r in BRANCHES)
    assert all(any(value in s for s in all_strings(aggregate['request']['messages'])) for value in outputs.values())
    final='KNOWLEDGE_AGGREGATE\n'+'\n'.join(outputs[r] for r in BRANCHES)
    assert aggregate['text']==final
    return {'branch_outputs':outputs,'terminal_output':final,'initial_http_overlap_ns':overlap,
        'http_requests':5,'initial_http_intervals':[{k:c[k] for k in ('started_ns','finished_ns')} for c in first]}


def backend_digest(backend):
    # Fixture snapshots have ASCII keys, integer values and no floats; this is
    # byte-identical to Snapshot.Canonical JCS for this deliberately closed shape.
    return 'sha256:'+hashlib.sha256(json.dumps(backend,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def scope_for(tenant_id,profile_id,resource_id,resource):
    backend=copy.deepcopy(resource['backend']);backend['tenant_id']=tenant_id
    digest=backend_digest(backend)
    if tenant_id==resource['backend']['tenant_id']:
        assert digest==resource['credential']['audience_digest'],'actual published backend digest mismatch'
    embedding=resource['embedding']
    parts=[tenant_id,profile_id,resource_id,digest,embedding['model'],embedding['base_url'],str(embedding['dimensions'])]
    return hashlib.sha256(json.dumps(parts,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def require_points(points,scopes):
    assert len(points)==2 and len({p['id'] for p in points})==2
    assert len(set(scopes.values()))==2
    for role in BRANCHES:
        selected=[p for p in points if p['payload']['worker_scope']==scopes[role]]
        assert len(selected)==1 and selected[0]['payload']['document']['content']==TEXTS[role]
        assert selected[0]['vector']=={VECTOR_NAME:[1,0,0]},'both resources must have the exact same vector'


def tenant_controls(h,content,points):
    tenant=content['tenant_id'];other=h.extra_tenant_ids[0];profile=content['sources']['profile']['profile_id']
    assert tenant!=other
    result={'result':'PENDING','kind':'actual Qdrant fixed-scope read control and public owner wrong-Tenant denial; not a second SDK Run',
        'tenant_id':tenant,'other_tenant_id':other,'profile_id_held_constant':profile,'probes':[]}
    for role in BRANCHES:
        resource_id=RESOURCES[role];resource=content['resources']['knowledge'][resource_id]
        positive=scope_for(tenant,profile,resource_id,resource);negative=scope_for(other,profile,resource_id,resource)
        assert positive!=negative
        def query(scope):
            body={'vector':{'name':VECTOR_NAME,'vector':[1,0,0]},'with_payload':True,'limit':10,
                'filter':{'must':[{'key':'worker_scope','match':{'value':scope}}]}}
            response=h.qdrant_request('POST','/collections/'+COLLECTION+'/points/search',body)
            return body,response['result']
        positive_request,yes=query(positive);negative_request,no=query(negative)
        assert len(yes)==1 and yes[0]['payload']['worker_scope']==positive
        assert yes[0]['payload']['document']['content']==TEXTS[role]
        assert {str(x['id']) for x in yes}=={str(x['id']) for x in points if x['payload']['worker_scope']==positive}
        assert no==[],'foreign Tenant fixed scope returned a private point'
        result['probes'].append({'resource':resource_id,'positive_request':positive_request,'positive':yes,'negative_request':negative_request,'negative':no})
    path='/v1/tenants/'+other+'/deployments/'+h.deployment_id+'/revisions/1/knowledge/docs_a/import'
    response=h.api('POST',path,{'name':'wrong-tenant.txt','text':'This denied request must never write a point.'},status=404)
    assert h.knowledge_state()==points,'read controls or denied owner import changed Qdrant'
    result.update(result='PASS',wrong_tenant_public_request={'method':'POST','path':path,'expected_status':404,'response':response},points_unchanged=True)
    return result
