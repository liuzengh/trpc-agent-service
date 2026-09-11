"""Actual trpc-mcp-go server + existing Control/Worker/Gateway/Channel Lab.

Only the deterministic model is synthetic; --live reuses the existing byte relay.
No shared Harness, production protocol, policy, or generation changes.
"""
import copy
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
from pathlib import Path
import threading
import urllib.request

from harness import Harness,_http_status
from combined_capabilities_fixture import LiveRelay,read_key

SLOT='search'
SELECTED='selected_search'
UNSELECTED='unselected_secret'
CALLABLE='fn_'+hashlib.sha256(('tools/'+SLOT).encode()).hexdigest()[:60]
ANSWER='MCP_ORCHID_627: the orchid is violet.\nThe checked service window is 09:17 UTC.'
CORRECTION='QUERY_REQUIRES_ORCHID: retry the selected tool with query=orchid.'
NORMAL='MCP正常验收：实际调用已选搜索工具，参数query=orchid；用真实返回说明canary和服务窗口，不要猜测或仅承诺调用。'
REPAIR='MCP纠错验收：先实际调用已选搜索工具，参数query=correct-me；根据工具返回的业务错误纠正参数并再次实际调用，最终使用真实成功结果回答。'


def tool_text(raw):
    """Read actual SDK-decoded MCP content without JSON-string comparisons."""
    if isinstance(raw,str):
        try:raw=json.loads(raw)
        except ValueError:return raw
    if isinstance(raw,str):return raw
    if isinstance(raw,list):return ''.join(tool_text(v) for v in raw)
    if isinstance(raw,dict):
        if isinstance(raw.get('text'),str):return raw['text']
        if 'content' in raw:return tool_text(raw['content'])
    return ''


def assert_declaration(request):
    declarations=request.get('tools',[])
    assert len(declarations)==1 and declarations[0]['function']['name']==CALLABLE
    schema=declarations[0]['function']['parameters']
    assert schema['type']=='object' and schema['required']==['query']
    assert set(schema['properties'])=={'query'} and schema['properties']['query']['type']=='string'


def assert_session_continuity(previous,candidate,requests,route,manifest_id,manifest_digest):
    assert candidate['parent_ref']==(previous['candidate']['candidate_ref'] if previous else '')
    assert candidate['parent_digest']==(previous['candidate']['content_digest'] if previous else '')
    assert route['ManifestRef']==manifest_id and route['ManifestDigest']==manifest_digest
    if previous:
        def content_text(value):
            if isinstance(value,str):return value
            if isinstance(value,list):return ''.join(content_text(part) for part in value)
            if isinstance(value,dict) and isinstance(value.get('text'),str):return value['text']
            return ''
        assert requests and any(message.get('role')=='assistant' and content_text(message.get('content'))==previous['delivery']['final_text'] for message in requests[0]['messages']),'next actual SDK request lacks prior accepted Final'


class MCPHarness(Harness):
    def __init__(self,*args,live=False,**kwargs):
        self.live=live
        super().__init__(*args,**kwargs)
    def record(self,name,value):
        path=self.artifacts/name
        path.write_text(self.redact(json.dumps(value,indent=2,ensure_ascii=False))+'\n')
        path.chmod(0o600)
        json.loads(path.read_text())
    def prepare_mcp(self,env_file=None):
        self.mcp_token=self.secret();port=self.port();self.mcp_url='http://127.0.0.1:'+str(port)+'/mcp'
        binary=self.work/'mcp-fixture-server'
        self.command(['go','build',*(['-race'] if self.race else []),'-o',str(binary),'./scripts/worker-v1-joint/mcpserver'])
        self.mcp_process=self.spawn('mcp-server',[str(binary),'--addr','127.0.0.1:'+str(port)],{'MCP_FIXTURE_TOKEN':self.mcp_token})
        self.wait(lambda:_http_status(self.mcp_url.removesuffix('/mcp')+'/healthz',1)==204,'real MCP dependency readiness')
        self.record('mcp-dependency.json',{'server_library':'trpc-mcp-go','version':'v0.0.10','url':self.mcp_url,'pid':self.mcp_process.pid,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'registered_tools':[SELECTED,UNSELECTED]})
        self.model.close()
        if self.live:
            key=read_key(env_file,'DEEPSEEK_API_KEY');base=read_key(env_file,'DEEPSEEK_BASE_URL');self.secrets.append(key);self.provider_base=base
            self.model=LiveRelay(self,key,base,self.model_name)
        else:self.model=MCPModelFixture(self)
        self.urls['model']=self.model.url
    def mcp_state(self):
        with urllib.request.urlopen(self.mcp_url.removesuffix('/mcp')+'/fixture/state',timeout=3) as r:return json.loads(r.read())
    def api(self,method,path,body=None,status=200,idem=None):
        if method=='PUT' and '/agents/' in path and path.endswith('/draft'):
            body=copy.deepcopy(body);spec=body['spec'];spec['requirements']['models']['primary']['capabilities']=['chat','tool_call'];spec['requirements']['tools']={SLOT:{'capability':'web.search'}}
            spec['nodes']['assistant']['tool_slots']=[SLOT];spec['nodes']['assistant']['instruction']='Use the selected external search tool when requested. Treat a tool business error as a request to correct its arguments. Base the Final on the successful actual tool result; do not invent it.'
        if method=='PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body=copy.deepcopy(body);body['config']['models']['primary']['capabilities']=['chat','tool_call']
            body['config']['tools']={SLOT:{'kind':'mcp_streamable_http','server_url':self.mcp_url,'toolset_name':'joint_mcp','tool_name':SELECTED,'auth':{'kind':'bearer'},'capability':'web.search'}}
            body['credentials']['tools']={SLOT:{'bearer_token':{'action':'replace','value':self.mcp_token}}}
        return super().api(method,path,body,status,idem)
    def close(self):
        # Model assertion reporting must occur only after all real processes stop.
        problem=None
        try:super().close()
        except BaseException as exc:problem=exc
        errors=getattr(getattr(self,'model',None),'errors',[])
        if isinstance(errors,list) and errors and problem is None:problem=RuntimeError(str(errors))
        self.record('mcp-cleanup.json',{'result':'FAIL' if problem else 'PASS','actual_server_reaped':not hasattr(self,'mcp_process') or self.mcp_process.poll() is not None})
        if problem:raise problem


class MCPModelFixture:
    def __init__(self,h):
        self.key=h.secret();self.records=[];self.errors=[];self.lock=threading.Lock();owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                if self.path!='/v1/chat/completions':self.send_error(404);return
                if self.headers.get('Authorization')!='Bearer '+owner.key:self.send_error(401);return
                try:
                    request=json.loads(self.rfile.read(int(self.headers['Content-Length'])));assert_declaration(request)
                    with owner.lock:owner.records.append(copy.deepcopy(request));ordinal=len(owner.records)
                    messages=request['messages'];last=max(i for i,m in enumerate(messages) if m.get('role')=='user');text=messages[last]['content'];assert text in (NORMAL,REPAIR)
                    results=[tool_text(m['content']) for m in messages[last+1:] if m.get('role')=='tool']
                    if text==REPAIR and not results:query='correct-me'
                    elif not results or (text==REPAIR and results==[CORRECTION]):query='orchid'
                    else:
                        assert results==([ANSWER] if text==NORMAL else [CORRECTION,ANSWER]),'actual MCP tool result differs'
                        query=None
                    delta={'content':ANSWER} if query is None else {'tool_calls':[{'index':0,'id':'mcp-call-'+str(ordinal),'type':'function','function':{'name':CALLABLE,'arguments':json.dumps({'query':query})}}]}
                    events=[{'id':'mcp-model-fixture','object':'chat.completion.chunk','model':h.model_name,'choices':[{'index':0,'delta':delta,'finish_reason':'stop' if query is None else 'tool_calls'}]}, {'id':'mcp-model-fixture','object':'chat.completion.chunk','model':h.model_name,'choices':[],'usage':{'prompt_tokens':12,'completion_tokens':8,'total_tokens':20}}]
                    raw=(''.join('data: '+json.dumps(e)+'\n\n' for e in events)+'data: [DONE]\n\n').encode();self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
                except Exception as exc:
                    with owner.lock:owner.errors.append(type(exc).__name__+': '+str(exc))
                    self.send_error(400,'model fixture assertion')
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.server.daemon_threads=True;self.url='http://127.0.0.1:'+str(self.server.server_port);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
    def snapshot(self):
        with self.lock:return copy.deepcopy(self.records)
    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join(timeout=5);assert not self.thread.is_alive()
