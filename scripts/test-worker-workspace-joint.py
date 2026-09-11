#!/usr/bin/env python3
"""Real Linux SDK workspace -> PG/S3 -> accepted Final -> Lab multipart gate.

Control and Gateway are real host processes; Worker is an isolated Linux
container. Fixture-only TCP relays preserve the published loopback endpoints and
end-to-end TLS. They neither parse nor authorize requests. Only model responses
are deterministic; Bash, SDK tools, bytes, services and persistence are real.
"""
import argparse
import copy
import hashlib
import json
import os
import subprocess
from pathlib import Path
import shlex
import ssl
import sys
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.parse import urlsplit
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent/'worker-v1-joint'))
from artifact_joint_fixture import ArtifactHarness
from parallel_artifact_joint_fixture import ParallelArtifactHarness
from harness import Harness
from faults import submit, wait_success, run, head, completions, outboxes, rows
import channel_lab_fixture as lab

CASES = {
 'workspace-txt': ('report.txt', b'workspace first text\n'),
 'workspace-csv': ('report.csv', b'name,value\nalpha,7\nbeta,9\n'),
 'workspace-txt-v2': ('report.txt', b'workspace second text\n'),
 'workspace-text-only': (None,b''),
}

class WorkspaceModel:
 def __init__(self,h):
  self.key=h.secret();self.requests=[];self.errors=[];self.lock=threading.Lock();owner=self
  class Handler(BaseHTTPRequestHandler):
   def log_message(self,*_):pass
   def do_POST(self):
    try:
     assert self.path=='/v1/chat/completions'
     assert self.headers.get('Authorization')=='Bearer '+owner.key
     size=int(self.headers.get('Content-Length','0'));assert 0<size<4*1024*1024
     req=json.loads(self.rfile.read(size));assert req['model']=='joint-fixture'
     with owner.lock:owner.requests.append(copy.deepcopy(req))
     msgs=req['messages'];index=max(i for i,m in enumerate(msgs) if m['role']=='user')
     case=msgs[index]['content'];name,data=CASES[case]
     tools={x['function']['name'] for x in req['tools']}
     assert {'workspace_exec','workspace_save_artifact'}<=tools
     tail=msgs[index+1:];results=[m for m in tail if m['role']=='tool']
     if name is None:
      assert not results;tool=None
     elif not results:
      tool='workspace_exec';args={'command':'mkdir -p out; printf %s '+shlex.quote(data.decode())+' > '+shlex.quote('out/'+name)}
     elif len(results)==1:
      value=json.loads(results[0]['content']);assert value.get('exit_code')==0, value
      tool='workspace_save_artifact';args={'path':'out/'+name}
     else:
      assert len(results)==2
      saved=json.loads(results[-1]['content']);assert saved['path']=='out/'+name
      assert saved['size_bytes']==len(data) and saved['version']>=0 and '/' not in saved['saved_as']
      tool=None
     if tool:
      delta={'role':'assistant','tool_calls':[{'index':0,'id':case+'-'+str(len(results)),'type':'function','function':{'name':tool,'arguments':json.dumps(args)}}]};finish='tool_calls'
     else:delta={'role':'assistant','content':'WORKSPACE_FINAL:'+case};finish='stop'
     def chunk(d,f):return 'data: '+json.dumps({'id':'workspace-model','object':'chat.completion.chunk','created':1,'model':'joint-fixture','choices':[{'index':0,'delta':d,'finish_reason':f}]})+'\n\n'
     payload=(chunk(delta,None)+chunk({},finish)+'data: [DONE]\n\n').encode()
     self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
    except (BrokenPipeError,ConnectionResetError):pass
    except Exception as e:
     with owner.lock:owner.errors.append(type(e).__name__+': '+str(e))
     self.send_error(400,'fixture assertion failed')
  self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.url='http://127.0.0.1:'+str(self.server.server_port)
  self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
 def close(self):self.server.shutdown();self.server.server_close();self.thread.join(5)

class WorkspaceHarness(ParallelArtifactHarness):
 def api(self,method,path,body=None,status=200,idem=None):
  if method=='PUT' and '/agents/' in path and path.endswith('/draft'):
   body=copy.deepcopy(body);spec=body['spec'];spec['requirements']['executors']={'shell':{'capability':'workspace'}}
   spec['nodes']['assistant']['workspace']={'executor_slot':'shell','tools':['workspace_exec','workspace_save_artifact']}
  if method=='PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
   body=copy.deepcopy(body);body['config']['executors']={'shell':{'kind':'sdk_sandbox'}}
  # Deliberately skip the Parallel subclass's graph transformation.
  return ArtifactHarness.api(self,method,path,body,status,idem)
 def spawn(self,name,argv,env):
  if not name.startswith('worker-'):return ArtifactHarness.spawn(self,name,argv,env)
  configpath=Path(env['WORKER_CONFIG_FILE']);config=json.loads(configpath.read_text())
  config['internal_address']=config['internal_address'].replace('127.0.0.1:','0.0.0.0:')
  config['health_address']=config['health_address'].replace('127.0.0.1:','0.0.0.0:')
  # All Worker private files are mounted beneath the sandbox-denied /run.
  for file in self.work.glob('*.json'):
   file.write_text(file.read_text().replace(str(self.work),'/run/fixture'))
  configpath.write_text(json.dumps(config).replace(str(self.work),'/run/fixture'))
  ports=sorted({self.pg_port,self.minio_port,self.model.server.server_port,self.ports['control_runtime'],urlsplit(self.nats_url).port})
  container=self.prefix+'-workspace';self.workspace_container=container
  args=['docker','run','--rm','--name',container,'--read-only','--user',str(os.getuid())+':'+str(os.getgid()),'--cap-drop','ALL','--security-opt','seccomp='+str(self.root/'deploy/compose/workspace/seccomp.json'),'--security-opt','systempaths=unconfined','--tmpfs','/workspace:rw,uid='+str(os.getuid())+',gid='+str(os.getgid())+',mode=0700','--tmpfs','/tmp:rw,mode=1777','--mount','type=bind,src='+str(self.work)+',dst=/run/fixture,readonly']
  for port in self.worker_ports[name].values():args+=['-p','127.0.0.1:'+str(port)+':'+str(port)]
  for key in env:args+=['-e',key]
  args+=['--entrypoint','/fixture-proxy',self.workspace_image,'--ports',','.join(map(str,ports))]
  env={k:v.replace(str(self.work),'/run/fixture') for k,v in env.items()}
  return Harness.spawn(self,name,args,env)
 def close(self):
  error=None
  try:super().close()
  except BaseException as e:error=e
  container=getattr(self,'workspace_container',None)
  if container:
   inspection=subprocess.run(['docker','inspect',container],capture_output=True,text=True)
   if inspection.returncode==0:subprocess.run(['docker','rm','-f',container],capture_output=True,check=True)
   inspection=subprocess.run(['docker','inspect',container],capture_output=True,text=True)
   if inspection.returncode==0 or 'No such object' not in inspection.stderr:raise RuntimeError('owned Worker container cleanup unverified')
   self.record('workspace-container-cleanup.json',{'removed':True,'container':container})
  if error:raise error
 def build_linux(self):
  context=self.work/'linux-build';context.mkdir()
  arch=self.command(['docker','info','--format','{{.Architecture}}']).strip();arch={'aarch64':'arm64','arm64':'arm64','x86_64':'amd64','amd64':'amd64'}[arch]
  self.command(['go','build','-o',context/'binary','./services/agent-worker/cmd/agent-worker'],env={'CGO_ENABLED':'0','GOOS':'linux','GOARCH':arch},timeout=300)
  self.command(['go','build','-o',context/'proxy','./scripts/worker-v1-joint/workspaceproxy'],env={'CGO_ENABLED':'0','GOOS':'linux','GOARCH':arch},timeout=300)
  (context/'Dockerfile').write_text((self.root/'deploy/compose/Dockerfile.worker-workspace').read_text()+'\n# Test-only L4 relay; absent from product runtime image.\nCOPY proxy /fixture-proxy\n')
  self.workspace_image='worker-workspace-joint:'+self.prefix
  self.command(['docker','build','--build-arg','WORKSPACE_BASE_IMAGE='+self.base_image,'-t',self.workspace_image,context],timeout=300)
  self.record('linux-worker-build.json',{'binary_sha256':hashlib.sha256((context/'binary').read_bytes()).hexdigest(),'image':self.workspace_image,'image_id':self.command(['docker','image','inspect','--format','{{.Id}}',self.workspace_image]).strip(),'base_image_id':self.command(['docker','image','inspect','--format','{{.Id}}',self.base_image]).strip(),'fixture_transport':'Go L4 relay only, no request parsing','sandbox':'approved scoped seccomp + systempaths + read-only root + private workspace tmpfs'})

def artifact_http(h,body,principal='gateway'):
 ctx=ssl.create_default_context(cafile=h.certs['ca']);ctx.load_cert_chain(h.certs[principal+'_cert'],h.certs[principal+'_key'])
 request=Request(h.urls['worker']+'/internal/v1/reply-artifacts',data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
 try:r=urlopen(request,context=ctx,timeout=15)
 except HTTPError as e:r=e
 with r:return r.status,dict(r.headers),r.read()

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--base-image',default='alpine:3.22',help='explicit Linux base; cached preflight image may be selected for offline reproduction');parser.add_argument('--artifacts',type=Path,default=Path('/private/tmp/worker-workspace-artifact-20260909/joint'));args=parser.parse_args()
 h=WorkspaceHarness(Path(__file__).resolve().parents[1],args.artifacts);h.base_image=args.base_image
 evidence={'result':'PENDING','model':'deterministic HTTP fixture','runtime':'real Linux SDK sandbox','storage':'real isolated PostgreSQL and MinIO','im':'real Channel Lab HTTP simulator, not Telegram','rounds':[]}
 try:
  h.provision();h.model.close();h.model=WorkspaceModel(h);h.urls['model']=h.model.url
  h.build_linux();h.control_start(lab.prepare(h));h.seed();h.start_worker();h.verify_dependencies();h.gateway=lab.start(h)
  evidence['manifest']=h.api('GET','/v1/tenants/'+h.tenant_id+'/deployments/'+h.deployment_id+'/revisions/1')['manifest_view']
  first_request=None
  for case,(logical,data) in CASES.items():
   rid=submit(h,case,'42');accepted=wait_success(h,rid);delivery=h.wait_delivery(rid)
   finals=outboxes(h,rid);completion=completions(h,rid);assert len(finals)==len(completion)==1
   payload=json.loads(h.sql("SELECT convert_from(payload,'UTF8') FROM worker.execution_reply_outbox WHERE run_id="+h.quote(rid))[0][0])
   # Confirm the persisted envelope, not an attachment reconstructed from text.
   content=payload['content']
   if logical is None:
    assert not content.get('attachments') and all('document' not in m for m in delivery['outgoing_added'])
    evidence['rounds'].append({'case':case,'run_id':rid,'completion':completion[0],'intent_id':completion[0]['final_intent_id'],'delivery':delivery,'text_only':True});continue
   attachments=content['attachments'];assert len(attachments)==1;a=attachments[0]
   assert a['name'].endswith('-'+logical) and a['size_bytes']==len(data) and a['sha256']==hashlib.sha256(data).hexdigest()
   assert a['version']==(1 if case.endswith('v2') else 0)
   outgoing=delivery['outgoing_added'];docs=[m for m in outgoing if 'document' in m];assert len(docs)==1
   assert docs[0]['chat_id']=='42' and docs[0]['bot_id']==h.gateway_fixture.bot_id
   doc=docs[0]['document'];assert doc['file_name']==a['name'] and doc['file_size']==len(data) and doc['sha256']==a['sha256']
   with h.gateway_fixture.opener.open(h.gateway_fixture.url+'/lab/documents/'+doc['file_id'],timeout=10) as r:received=r.read()
   assert received==data
   request={'intent_id':completion[0]['final_intent_id'],'run_id':rid,'completion_id':completion[0]['completion_id'],'name':a['name'],'version':a['version']}
   if first_request is None:first_request=copy.deepcopy(request)
   status,headers,raw=artifact_http(h,request);assert status==200 and raw==data and headers.get('X-Content-Sha256',headers.get('X-Content-SHA256'))==a['sha256']
   negative=[]
   for name,change in [('version',{'version':a['version']+10}),('completion',{'completion_id':'cmp_'+'0'*64}),('extra_scope',{'tenant_id':'untrusted'})]:
    status,_,raw=artifact_http(h,{**request,**change});assert status in (400,403,404), (name,status)
    negative.append({'case':name,'status':status})
   status,_,_=artifact_http(h,request,'control');assert status==403;negative.append({'case':'control_download_principal','status':status})
   objects=h.object_state();assert any(x['sha256']==a['sha256'] and bytes.fromhex(x['bytes_hex'])==data for x in objects)
   evidence['rounds'].append({'case':case,'run_id':rid,'completion':completion[0],'intent_id':request['intent_id'],'attachment':a,'accepted_head':head(h,run(h,rid)),'delivery':delivery,'negative':negative,'metadata':h.metadata_state(),'objects':objects,'accepted_snapshot':accepted['candidate']})
   h.record('workspace-joint.json',evidence)
  status,headers,raw=artifact_http(h,first_request)
  assert status==200 and raw==CASES['workspace-txt'][1] and first_request['version']==0
  status,_,_=artifact_http(h,{**first_request,'run_id':evidence['rounds'][1]['run_id']});assert status in (403,404)
  evidence['old_final_after_v2']={'version':0,'sha256':hashlib.sha256(raw).hexdigest(),'fixed_version_preserved':True,'mismatched_run_status':status}
  assert not h.model.errors,h.model.errors
  evidence.update(result='PASS',run_count=4,paid_model_calls=0)
 except BaseException as e:
  frame=traceback.extract_tb(e.__traceback__)[-1];evidence.update(result='FAIL',error=h.redact(str(e)),location={'file':Path(frame.filename).name,'line':frame.lineno});raise
 finally:
  try:h.close();evidence['cleanup']='PASS'
  except BaseException as e:evidence.update(result='FAIL',cleanup='FAIL',cleanup_error=h.redact(str(e)));raise
  finally:h.record('workspace-joint.json',evidence)
 print('WORKSPACE_JOINT=PASS linux_sdk=true pg_s3=true accepted_final=true lab_document_bytes=true runs=4')
if __name__=='__main__':main()
