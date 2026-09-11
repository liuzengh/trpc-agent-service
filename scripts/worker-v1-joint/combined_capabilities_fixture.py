"""Three normal Runs through the existing four capability implementations.

Only provider fixtures are synthetic. Catalog merger is fixture composition,
not a new runtime platform. Live mode reuses the unchanged-byte LiveRelay.
"""
import base64
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import importlib.util
import json
from pathlib import Path
import signal
import threading
import time

from harness import Harness, _http_status
from artifact_joint_fixture import ArtifactHarness, BACKEND_ID as ARTIFACT_BACKEND, BUCKET, TOOLS as ARTIFACT_TOOLS, assert_artifact
from knowledge_joint_fixture import KnowledgeHarness, KnowledgeModelFixture, BACKEND_ID as KNOWLEDGE_BACKEND, COLLECTION, VECTOR_NAME, DIMENSIONS, CALLABLE_NAME, DOCUMENT_TEXT
from memory_joint_fixture import MemoryHarness, TOOLS as MEMORY_TOOLS
from redis_session_joint_fixture import _summary

_live_path=Path(__file__).resolve().parents[1]/'test-worker-summary-live.py'
_spec=importlib.util.spec_from_file_location('worker_existing_summary_live',_live_path)
_live=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_live)
LiveRelay,read_key=_live.LiveRelay,_live.read_key
MEMORY_TEXT='COMBINED_MEMORY_CANARY: the selected tea is jasmine.'
FILE_NAME='combined-report.txt'
FILE_TEXT='COMBINED_ARTIFACT_CANARY: orchid report bytes.\n'
FILE_BYTES=FILE_TEXT.encode()
TOOLS=MEMORY_TOOLS+ARTIFACT_TOOLS+[CALLABLE_NAME]
FINAL_TEXT='Combined verified: '+MEMORY_TEXT+' '+FILE_TEXT.strip()+' '+DOCUMENT_TEXT


def has_summary_text(messages,summary):
    """Compare actual content text, not JSON's escaped representation."""
    if not isinstance(summary,str) or not summary:return False
    def texts(value):
        if isinstance(value,str):yield value
        elif isinstance(value,list):
            for part in value:yield from texts(part)
        elif isinstance(value,dict) and isinstance(value.get('text'),str):yield value['text']
    return any(summary in text for message in messages if isinstance(message,dict) for text in texts(message.get('content')))


def round_inputs():
    return [
        '组合验收第一轮。必须实际调用工具完成：1) memory_add 将完整字符串 '+MEMORY_TEXT+' 保存一次；2) memory_load 读回；3) artifact_save 保存 name='+FILE_NAME+'，mime_type=text/plain，content_base64='+base64.b64encode(FILE_BYTES).decode()+'，只保存一次；4) artifact_load 读取该文件 version=0；5) 调用知识检索工具查询 orchid canary 与 service window。依据实际工具结果给简短最终确认，包含知识中的 canary。不要声称未调用的操作成功。',
        '组合验收第二轮。请读取已接受的摘要上下文，并实际调用 memory_load、artifact_load(name='+FILE_NAME+',version=0) 和知识检索工具。不要新增或修改Memory/文件，依据真实返回确认记忆、文件和知识内容。',
        '组合验收第三轮。请继续消费正式Session摘要，实际调用 memory_load、artifact_load(name='+FILE_NAME+',version=0) 和知识检索工具交叉核验。不要新增或修改Memory/文件，依据真实返回给最终确认。',
    ]


class _CombinedLeaf(Harness):
    """Runs after the existing public-input hooks, immediately before HTTP."""
    def api(self,method,path,body=None,status=200,idem=None):
        if method=='PUT' and '/agents/' in path and path.endswith('/draft'):
            body=copy.deepcopy(body)
            body['spec']['nodes']['assistant']['instruction']='Execute the user requested verification using the available tools. Never invent tool outputs. Preserve exact file bytes and remembered content. Use the supplied Session summary as historical context, not as evidence that this Run called a tool.'
        if method=='PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body=copy.deepcopy(body)
            body['config']['knowledge']['docs']['embedding']['base_url']=self.embedding_provider.url+'/v1'
            body['config']['models']['summarizer']['base_url']=self.summary_provider.url+'/v1'
            body['config']['models']['summarizer']['model']=self.model_name if self.live else 'joint-summary'
        return super().api(method,path,body,status,idem)


class CombinedHarness(ArtifactHarness,KnowledgeHarness,MemoryHarness,_summary.SummaryHarness,_CombinedLeaf):
    def __init__(self,*args,live=False,**kwargs):
        self.live=live;self.catalog_restarts=0
        super().__init__(*args,**kwargs)
    def failure_snapshot(self,run_id):
        """Read only owned Run facts before cleanup; JSON protects multiline text."""
        if not run_id:return {'run_id':None,'queries':{},'reason':'no Run submitted'}
        rid=self.quote(run_id)
        queries={
            'run':"SELECT clock_timestamp() AS observed_at,tenant_id,run_id,session_id,status,wait_reason,attempts,current_attempt_id,accepted_at,execution_deadline,run_deadline,reply_deadline FROM worker.execution_runs WHERE run_id="+rid,
            'head':"SELECT s.tenant_id,s.session_id,s.accepted_ref,s.accepted_digest,s.settled_sequence,s.next_sequence FROM worker.execution_sessions s JOIN worker.execution_runs r USING(tenant_id,session_id) WHERE r.run_id="+rid,
            'completion':"SELECT run_id,completion_id,attempt_id,status,candidate_ref,candidate_digest,reply_disposition,reason,memory_status,completed_at FROM worker.execution_completions WHERE run_id="+rid,
            'attempts':"SELECT attempt_id,status,reason,worker_id,lease_epoch,parent_ref,parent_digest,created_at,ended_at FROM worker.execution_attempts WHERE run_id="+rid+" ORDER BY created_at",
            'final_outbox':"SELECT intent_id,run_id,digest,ready,published_at,convert_from(payload,'UTF8')::json AS payload FROM worker.execution_reply_outbox WHERE run_id="+rid,
            'reply_transport':"SELECT stream_name,stream_id,stream_sequence,raw_digest,outcome,reason,intent_id,run_id,recorded_at FROM gateway.gateway_reply_transport_receipts WHERE run_id="+rid+" OR run_id='' ORDER BY stream_sequence",
            'delivery_intents':"SELECT intent_id,run_id,digest,intent,deadline,part_count,created_at FROM gateway.gateway_delivery_intents WHERE run_id="+rid,
            'delivery_parts':"SELECT p.part_id,p.intent_id,p.part_index,p.body,p.state,p.attempt_number,p.current_attempt_id,p.updated_at FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id="+rid+" ORDER BY p.part_index",
            'delivery_attempts':"SELECT a.attempt_id,a.part_id,a.attempt_number,a.result,a.created_at,a.finished_at FROM gateway.gateway_delivery_attempts a JOIN gateway.gateway_delivery_parts p USING(part_id) JOIN gateway.gateway_delivery_intents i ON i.intent_id=p.intent_id WHERE i.run_id="+rid+" ORDER BY a.created_at",
            'gateway_owners':"SELECT account_id,revision,enabled,instance_id,epoch,lease_until,blocked_revision FROM gateway.gateway_connection_accounts",
        }
        result={'run_id':run_id,'queries':{}}
        for name,query in queries.items():
            try:
                rows=self.sql("SELECT COALESCE(json_agg(row_to_json(fact)),'[]'::json)::text FROM ("+query+") fact")
                if len(rows)!=1 or len(rows[0])!=1:raise ValueError('JSON evidence row shape')
                result['queries'][name]=json.loads(rows[0][0])
            except Exception as exc:result['queries'][name]={'read_error_type':type(exc).__name__}
        for name,read in (('memory',self.memory_state),('artifact_metadata',self.metadata_state),('s3_objects',self.object_state),('knowledge',self.knowledge_state)):
            try:result[name]=read()
            except Exception as exc:result[name]={'read_error_type':type(exc).__name__}
        return result
    def provision(self):
        self._initializing_dependencies=True;self._s3_bootstrap_retries=0
        try:
            super().provision()
            self.record('combined-dependency-readiness.json',{'bucket_bootstrap_503_retries':self._s3_bootstrap_retries,'worker_save_load_retry_changed':False})
        finally:self._initializing_dependencies=False
    def s3_request(self,method,key=None,**kwargs):
        # MinIO's health endpoint can turn ready before its bucket API. This is
        # owned empty-fixture provisioning only, never Worker Save/Load policy.
        bootstrap=getattr(self,'_initializing_dependencies',False) and key is None and method in ('PUT','GET')
        deadline=time.monotonic()+30
        while True:
            try:return super().s3_request(method,key,**kwargs)
            except RuntimeError as exc:
                if not bootstrap or not str(exc).startswith('fixture S3 HTTP status 503 ') or time.monotonic()>=deadline:raise
                self._s3_bootstrap_retries+=1;time.sleep(.1)
    # Existing seeding hooks call these on the way back from owner Tenant HTTP.
    # The outermost API method merges all descriptors exactly once instead.
    def bind_artifact_catalog(self,tenant_id):pass
    def bind_knowledge_catalog(self,tenant_id):pass
    def bind_memory_catalog(self,tenant_id):pass
    def api(self,method,path,body=None,status=200,idem=None):
        result=super().api(method,path,body,status,idem)
        if method=='POST' and path=='/v1/admin/tenants':self.bind_combined_catalog(result['id'])
        return result
    def bind_combined_catalog(self,tenant):
        assert self.catalog_restarts==0,'combined fixture must publish one merged catalog'
        catalog={'version':'v1','backends':[{'id':id,'revision':1,'label':label,'kind':kind,'roles':[role],'enabled':True,'tenant_ids':[tenant]} for id,label,kind,role in [('joint-memory-pg','Combined PostgreSQL Memory','postgresql','memory'),(ARTIFACT_BACKEND,'Combined S3 Artifact','s3','artifact'),(KNOWLEDGE_BACKEND,'Combined Qdrant Knowledge','qdrant','knowledge')]]}
        targets={'version':'v1','backends':[
            {'backend_id':'joint-memory-pg','backend_revision':1,'kind':'postgresql','adapter':'managed-postgres-v1','isolation':'tenant-subject-agent-v1','limits':{'timeout_ms':5000,'max_concurrency':4,'max_bytes':1048576},'postgresql':{'host':'127.0.0.1','port':self.pg_port,'database':'agent_platform','username':'memory_runtime','sslmode':'disable'}},
            {'backend_id':ARTIFACT_BACKEND,'backend_revision':1,'kind':'s3','adapter':'managed-s3-v1','isolation':'tenant-artifact-v1','limits':{'timeout_ms':3000,'max_concurrency':4,'max_bytes':1048576},'s3':{'endpoint':self.s3_endpoint,'bucket':BUCKET,'region':'us-east-1','path_style':True,'versioning':'disabled'}},
            {'backend_id':KNOWLEDGE_BACKEND,'backend_revision':1,'kind':'qdrant','adapter':'managed-qdrant-v1','isolation':'tenant-profile-resource-v1','limits':{'timeout_ms':5000,'max_concurrency':4,'max_bytes':65536},'qdrant':{'endpoint':self.qdrant_endpoint,'collection':COLLECTION,'vector_name':VECTOR_NAME,'dimensions':DIMENSIONS,'distance':'cosine'}},
        ]}
        env=dict(self.control_env)
        for suffix,value in [('CATALOG',catalog),('TARGETS',targets)]:
            path=Path(self.write('combined-'+suffix.lower()+'.json',value));env['CONTROL_PLATFORM_BACKEND_'+suffix+'_FILE']=str(path);env['CONTROL_PLATFORM_BACKEND_'+suffix+'_SHA256']=hashlib.sha256(path.read_bytes()).hexdigest()
        env.pop('CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST',None)
        self.contract_digest=self.command([self.binaries['control-api'],'--print-deployment-contract-digest'],env=env).strip();env['CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST']=self.contract_digest
        self.control.send_signal(signal.SIGTERM);self.control.wait(timeout=25);assert self.control.returncode==0
        self.control=self.spawn('control-api',[self.binaries['control-api']],env)
        self.wait(lambda:_http_status(self.urls['control']+'/healthz',timeout=1)==204,'one merged four-capability Control catalog')
        self.catalog_restarts+=1;self.record('combined-backend-config.json',{'catalog':catalog,'targets':targets,'contract_digest':self.contract_digest,'catalog_restarts':self.catalog_restarts})
    def configure_providers(self,*,env_file=None):
        self.model.close()
        self.embedding_provider=KnowledgeModelFixture(self)
        if self.live:
            key=read_key(env_file,'DEEPSEEK_API_KEY');base=read_key(env_file,'DEEPSEEK_BASE_URL')
            self.secrets.append(key);self.provider_base=base
            self.model=LiveRelay(self,key,base,self.model_name);self.summary_provider=self.model;self.model.summary_key=key
        else:
            self.summary_provider=_summary.SummaryModelFixture(self)
            self.model=CombinedModelFixture(self);self.model.summary_key=self.summary_provider.summary_key
        self.model.embedding_key=self.embedding_provider.embedding_key
        self.model.embedding_model=self.embedding_provider.embedding_model
        self.model.dimensions=self.embedding_provider.dimensions
        self.urls['model']=self.model.url
    def close(self):
        errors=[]
        try:super().close()
        except BaseException as exc:errors.append(exc)
        finally:
            fixture_errors=getattr(getattr(self,'model',None),'errors',[])
            if isinstance(fixture_errors,list) and fixture_errors:errors.append(RuntimeError(str(fixture_errors)))
            for name in ('summary_provider','embedding_provider'):
                provider=getattr(self,name,None)
                if provider is not None and provider is not getattr(self,'model',None):
                    try:provider.close()
                    except BaseException as exc:errors.append(exc)
            self.record('combined-provider-cleanup.json',{'result':'FAIL' if errors else 'PASS','additional_providers_closed':True})
        if errors:raise RuntimeError('; '.join(self.redact(str(e)) for e in errors))


class CombinedModelFixture:
    """Synthetic LLM only. Calls existing Memory/Artifact/SDK Knowledge tools."""
    def __init__(self,h):
        self.key=h.secret();self.requests=[];self.outputs=[];self.errors=[];self.progress={};self.lock=threading.Lock();owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                if self.path!='/v1/chat/completions':self.send_error(404);return
                if self.headers.get('Authorization')!='Bearer '+owner.key:self.send_error(401);return
                try:self.respond()
                except (AssertionError,KeyError,ValueError,TypeError) as exc:
                    owner.errors.append(type(exc).__name__+': combined fixture mismatch');self.send_error(400,'combined fixture mismatch')
                except (BrokenPipeError,ConnectionResetError):pass
            def respond(self):
                req=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                with owner.lock:owner.requests.append(copy.deepcopy(req));number=len(owner.requests)
                assert req['model']==h.model_name and sorted(t['function']['name'] for t in req['tools'])==sorted(TOOLS)
                messages=req['messages'];start=max(i for i,m in enumerate(messages) if m.get('role')=='user');text=messages[start]['content'];round_index=round_inputs().index(text)
                state=owner.progress.setdefault(round_index,{'calls':[],'results':{}})
                tool_messages=[m for m in messages[start+1:] if m.get('role')=='tool']
                ids={call['id'] for call in state['calls']}
                if ids and not any(m.get('tool_call_id') in ids for m in tool_messages):
                    # A fresh SDK invocation cannot inherit this fixture's prior
                    # observed attempt results just because the input repeats.
                    state={'calls':[],'results':{}};owner.progress[round_index]=state;ids=set()
                for message in tool_messages:
                    if message.get('tool_call_id') in ids:
                        try:value=json.loads(message['content'])
                        except ValueError:value={'error':message['content']}
                        state['results'][message['tool_call_id']]=value
                assert all(call['id'] in state['results'] for call in state['calls']), 'previous actual tool result not observed'
                results=[state['results'][call['id']] for call in state['calls']]
                plan=[]
                if round_index==0:plan.append(('memory_add',{'memory':MEMORY_TEXT}))
                plan.append(('memory_load',{}))
                if round_index==0:plan.append(('artifact_save',{'name':FILE_NAME,'content_base64':base64.b64encode(FILE_BYTES).decode(),'mime_type':'text/plain'}))
                plan.extend([('artifact_load',{'name':FILE_NAME,'version':0}),(CALLABLE_NAME,{'query':'What is the orchid knowledge canary and service window?'})])
                step=len(results)
                if step<len(plan):
                    name,args=plan[step];call_id='combined-call-'+str(number)
                    state['calls'].append({'id':call_id,'name':name})
                    delta={'role':'assistant','tool_calls':[{'index':0,'id':call_id,'type':'function','function':{'name':name,'arguments':json.dumps(args)}}]};finish='tool_calls'
                else:
                    by_name={name:value for (name,_),value in zip(plan,results)}
                    assert any(r['memory']==MEMORY_TEXT for r in by_name['memory_load']['results'])
                    assert_artifact(by_name['artifact_load'],FILE_NAME,0,FILE_BYTES,loaded=True,mime_type='text/plain')
                    assert DOCUMENT_TEXT in [r['text'] for r in by_name[CALLABLE_NAME]['documents']]
                    delta={'role':'assistant','content':FINAL_TEXT};finish='stop'
                    with owner.lock:owner.outputs.append({'round':round_index+1,'tool_results':results,'plan':[name for name,_ in plan],'text':FINAL_TEXT})
                def chunk(delta,finish,usage=None):
                    event={'id':'combined-response-'+str(number),'object':'chat.completion.chunk','created':1,'model':req['model'],'choices':[{'index':0,'delta':delta,'finish_reason':finish}]}
                    if usage:event['usage']=usage
                    return 'data: '+json.dumps(event)+'\n\n'
                payload=(chunk(delta,None)+chunk({},finish,{'prompt_tokens':7,'completion_tokens':3,'total_tokens':10})+'data: [DONE]\n\n').encode()
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.url='http://127.0.0.1:'+str(self.server.server_port);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
    def snapshot(self):
        with self.lock:return copy.deepcopy(self.requests)
    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join(timeout=5)
        assert not self.thread.is_alive()
        # Harness checks fixture assertions after all owned process resources close.
