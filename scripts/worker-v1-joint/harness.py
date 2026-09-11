"""Private disposable process fixture. All product state is seeded through HTTP."""
from __future__ import annotations
import base64
import http.cookiejar
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


from model_fixture import ModelFixture


def assert_model_contract(requests, manifest_view, expected_model):
    """Compare actual SDK JSON with the public immutable single-LLM contract."""
    plan=manifest_view['agent_plan']; node_id=plan['root']; node=plan['nodes'][node_id]
    assert node['kind']=='llm' and len(plan['nodes'])==1
    resource=node['model_resource']; model=manifest_view['resources']['models'][resource]['model']
    assert model==expected_model, 'Control publication changed the explicitly selected model'
    policy=manifest_view['execution']['max_output_tokens']
    node_max=(node.get('generation') or {}).get('max_output_tokens')
    assert type(policy) is int and policy>0
    assert node_max is None or (type(node_max) is int and 0<node_max<=policy)
    effective=node_max if node_max is not None else policy
    assert requests, 'no actual SDK model requests were recorded'
    parameters=[]
    for request in requests:
        assert request.get('model')==model, 'SDK model differs from the published model'
        assert type(request.get('max_completion_tokens')) is int and request['max_completion_tokens']==effective, 'SDK output cap differs from the published effective max_output_tokens'
        assert 'max_tokens' not in request, 'SDK emitted an extra competing output-token field'
        parameters.append({'model':request['model'],'max_completion_tokens':request['max_completion_tokens']})
    return {'result':'PASS','model_name':model,'node_id':node_id,'model_resource':resource,
            'execution_max_output_tokens':policy,'node_max_output_tokens':node_max,
            'effective_max_output_tokens':effective,'request_parameters':parameters}


def _http_status(url, timeout):
    """A polling response is owned even when urllib raises HTTPError."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status
    except urllib.error.HTTPError as error:
        error.close()
        raise


class Harness:
    def __init__(self, root, artifacts, race=False, *, model_name='joint-fixture'):
        if not isinstance(model_name,str) or not model_name or model_name.strip()!=model_name:
            raise ValueError('model_name must be an explicit nonempty name')
        self.model_name=model_name
        self.root = Path(root).resolve(); self.artifacts = Path(artifacts).resolve()
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.work = Path(tempfile.mkdtemp(prefix='worker-joint-private-'))
        self.race = race
        self.prefix = 'worker-joint-' + uuid.uuid4().hex[:10]
        self.env = {k: os.environ[k] for k in ('PATH','HOME','TMPDIR','GOCACHE','GOPATH','GOROOT','CGO_ENABLED') if k in os.environ}
        self.secrets = []; self.containers = []; self.processes = []; self.handles = []
        self.binaries = {}; self.certs = {}; self.dsns = {}; self.worker = None; self.workers = {}
        self.worker_ports = {name:{"proof":self.port(),"health":self.port()} for name in ("worker-one","worker-two")}
        self.ports = {name:self.port() for name in ('control','control_channel','control_runtime','worker','worker_health','gateway','gateway_health')}
        self.urls = {name:('https' if name in ('control_channel','control_runtime','worker') else 'http')+'://127.0.0.1:'+str(port) for name,port in self.ports.items()}
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    def secret(self, n=24):
        value=secrets.token_urlsafe(n); self.secrets.append(value); return value
    @staticmethod
    def port():
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0)); return sock.getsockname()[1]
    @staticmethod
    def quote(value): return "'" + str(value).replace("'", "''") + "'"
    def redact(self, value):
        for secret in self.secrets: value=value.replace(secret,'[redacted]')
        return value
    def command(self, argv, env=None, timeout=180):
        result=subprocess.run([str(x) for x in argv],cwd=self.root,env=dict(self.env,**(env or {})),text=True,capture_output=True,timeout=timeout)
        if result.returncode:
            raise RuntimeError(self.redact(f'{argv[0]} exit {result.returncode}\n{result.stdout}{result.stderr}'))
        return result.stdout
    def spawn(self, name, argv, env):
        log=open(self.artifacts/(name+'.log'),'a')
        self.handles.append(log)
        p=subprocess.Popen([str(x) for x in argv],cwd=self.root,env=dict(self.env,**env),stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        self.processes.append((name,p))
        with open(self.artifacts/'process-events.jsonl','a') as record:
            record.write(json.dumps({'event':'started','name':name,'pid':p.pid,'argv':[str(x) for x in argv]})+'\n')
        return p
    def wait(self, predicate, description, timeout=30):
        deadline=time.monotonic()+timeout; last=None
        while time.monotonic()<deadline:
            try:
                value=predicate()
                if value: return value
            except (OSError,urllib.error.URLError,RuntimeError) as exc: last=exc
            time.sleep(.05)
        stopped=[f'{name}:{p.returncode}' for name,p in self.processes if p.poll() is not None]
        raise RuntimeError(self.redact(f'{description}: deadline; stopped={stopped}; last={last}'))
    def write(self, name, value, mode=0o600):
        path=self.work/name
        path.write_text(json.dumps(value,indent=2)+'\n' if not isinstance(value,str) else value)
        path.chmod(mode); return str(path)
    def sql(self, query):
        # Evidence reads only. Product writes always go through actual service HTTP.
        if not query.lstrip().upper().startswith(('SELECT ', 'WITH ')):
            raise ValueError('joint evidence SQL is read-only')
        output=self.command(['docker','exec','-e','PGOPTIONS=-c default_transaction_read_only=on',self.pg,'psql','-X','-A','-t','-F','\t','-v','ON_ERROR_STOP=1','-U','platform_admin','-d','agent_platform','-c',query])
        return [line.split('\t') for line in output.splitlines() if line]
    def api(self, method, path, body=None, status=200, idem=None):
        request=urllib.request.Request(self.urls['control']+path,data=None if body is None else json.dumps(body).encode(),method=method,headers={'Content-Type':'application/json',**({'Idempotency-Key':idem} if idem else {})})
        try: response=self.opener.open(request,timeout=10)
        except urllib.error.HTTPError as exc: response=exc
        with response:
            data=response.read(); actual=response.status
        with open(self.artifacts/'control-http.jsonl','a') as record: record.write(json.dumps({'method':method,'path':path,'status':actual})+'\n')
        if actual != status: raise RuntimeError(self.redact(f'Control {method} {path}: {actual} expected {status}: {data.decode()}'))
        return json.loads(data) if data else None
    def provision(self):
        for role in ('CONTROL','GATEWAY','WORKER','SESSION'):
            for kind in ('MIGRATOR','RUNTIME'): self.env[role+'_'+kind+'_PASSWORD']=self.secret()
        for role in ('CONTROL','GATEWAY','WORKER','RECONCILER'): self.env['NATS_'+role+'_PASSWORD']='nats_'+self.secret()
        self.env['POSTGRES_PASSWORD']=self.secret()
        self.pg=self.prefix+'-pg'
        self.command(['docker','run','--rm','-d','--name',self.pg,'-e','POSTGRES_USER=platform_admin','-e','POSTGRES_DB=agent_platform','-e','POSTGRES_PASSWORD','-p','127.0.0.1::5432','-v',str(self.root/'deploy/compose')+':/provision:ro','postgres:17.6-alpine'])
        self.containers.append(self.pg)
        self.wait(lambda:subprocess.run(['docker','exec',self.pg,'pg_isready','-h','127.0.0.1','-U','platform_admin','-d','agent_platform'],capture_output=True).returncode==0,'PostgreSQL readiness')
        args=['docker','exec','-e','PGUSER=platform_admin','-e','PGDATABASE=agent_platform']
        for name in self.env:
            if name.endswith(('_MIGRATOR_PASSWORD','_RUNTIME_PASSWORD')): args+=['-e',name]
        self.command([*args,self.pg,'sh','/provision/provision-schemas.sh'])
        self.pg_port=int(self.command(['docker','port',self.pg,'5432/tcp']).strip().rsplit(':',1)[1])
        self.session_port=self.pg_port
        for role in ('control','gateway','worker','session'):
            for kind in ('migrator','runtime'):
                name=role+'_'+kind
                self.dsns[name]=f'postgres://{name}:{self.env[name.upper()+"_PASSWORD"]}@127.0.0.1:{self.pg_port}/agent_platform?sslmode=disable'
        self.make_pki()
        self.broker=self.prefix+'-nats'
        # NATS runs as root inside its disposable fixture so the host owner's
        # private cert key can remain mode 0600. The application files stay private.
        tlsdir=self.work/'nats-tls'; tlsdir.mkdir(); tlsdir.chmod(0o755)
        for src,dst in (('ca','ca.pem'),('server_cert','server.pem'),('server_key','server-key.pem')):
            shutil.copyfile(self.certs[src],tlsdir/dst)
            (tlsdir/dst).chmod(0o600 if src=='server_key' else 0o644)
        # A byte-identical private configuration copy permits reversible fault
        # injection without editing the checkout or another fixture's broker.
        self.nats_config_dir=self.work/'nats-config'
        shutil.copytree(self.root/'deploy/nats',self.nats_config_dir)
        args=['docker','run','--rm','-d','--name',self.broker,'--user','0:0','-p','127.0.0.1::4222','-v',str(self.nats_config_dir)+':/etc/nats:ro','-v',str(tlsdir)+':/run/nats-tls:ro']
        for role in ('CONTROL','GATEWAY','WORKER','RECONCILER'): args+=['-e','NATS_'+role+'_PASSWORD']
        self.command([*args,'nats:2.11.8-alpine','-c','/etc/nats/server-tls.conf']);self.containers.append(self.broker)
        port=int(self.command(['docker','port',self.broker,'4222/tcp']).strip().rsplit(':',1)[1]);self.nats_url=f'tls://127.0.0.1:{port}'
        for name,path in (('control-api','./services/control-api/cmd/control-api'),('agent-worker','./services/agent-worker/cmd/agent-worker'),('channel-gateway','./services/channel-gateway/cmd/channel-gateway')):
            out=self.work/name
            self.command(['go','build',*(['-race'] if self.race else []),'-o',out,path],timeout=300);self.binaries[name]=str(out)
        (self.artifacts/'binary-build.json').write_text(json.dumps({'go_version':self.command(['go','version']).strip(),'race':self.race,'sha256':{name:hashlib.sha256(Path(path).read_bytes()).hexdigest() for name,path in self.binaries.items()}},indent=2)+'\n')
        self.wait(lambda:self._reconcile(),'NATS TLS topology reconciliation')
        print(self.command([self.binaries['agent-worker'],'prepare-session'],env={'SESSION_MIGRATION_DATABASE_URL':self.dsns['session_migrator']}).strip(),flush=True)
        self.model=ModelFixture(self);self.secrets.append(self.model.key)
        self.urls['model']=self.model.url
        (self.artifacts/'fixture-resources.json').write_text(json.dumps({'postgres':self.pg,'nats':self.broker,'pg_port':self.pg_port,'urls':self.urls,'nats_url':self.nats_url},indent=2)+'\n')
    def _reconcile(self):
        try:
            output=self.command([self.binaries['channel-gateway'],'reconcile'],env={'GATEWAY_NATS_URL':self.nats_url,'GATEWAY_NATS_USER':'reconciler','GATEWAY_NATS_PASSWORD':self.env['NATS_RECONCILER_PASSWORD'],'GATEWAY_NATS_CA_FILE':self.certs['ca'],'GATEWAY_NATS_TOPOLOGY_FILE':str(self.root/'deploy/nats/streams.yaml')})
            (self.artifacts/'nats-reconcile.log').write_text(output);return True
        except RuntimeError:return False
    def make_pki(self):
        ca_key=self.work/'ca.key';ca=self.work/'ca.pem'
        ca_config=self.write('ca.cnf','[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=ca_ext\n[dn]\nCN=Worker joint fixture CA\n[ca_ext]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n')
        self.command(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',ca_key,'-out',ca,'-config',ca_config,'-days','1'])
        ca_key.chmod(0o600);self.certs['ca']=str(ca)
        for name,identity,usage in [('server','', 'serverAuth'),('worker','spiffe://agent-platform/worker/one','clientAuth'),('worker_two','spiffe://agent-platform/worker/two','clientAuth'),('control','spiffe://agent-platform/control-api','clientAuth'),('gateway','spiffe://agent-platform/channel-gateway','clientAuth')]:
            key=self.work/(name+'.key');csr=self.work/(name+'.csr');cert=self.work/(name+'.pem')
            self.command(['openssl','req','-new','-newkey','rsa:2048','-nodes','-keyout',key,'-out',csr,'-subj','/CN='+name])
            ext=self.write(name+'.ext','basicConstraints=critical,CA:FALSE\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage='+usage+'\nsubjectAltName='+('URI:'+identity if identity else 'IP:127.0.0.1,DNS:localhost,DNS:nats')+'\n')
            self.command(['openssl','x509','-req','-in',csr,'-CA',ca,'-CAkey',ca_key,'-CAcreateserial','-out',cert,'-days','1','-extfile',ext])
            key.chmod(0o600);self.certs[name+'_cert']=str(cert);self.certs[name+'_key']=str(key)
    def control_start(self, channel_env):
        env={'CONTROL_DATABASE_URL':self.dsns['control_runtime'],'CONTROL_MIGRATION_DATABASE_URL':self.dsns['control_migrator'],'CONTROL_PROFILE_CREDENTIAL_KEY':base64.b64encode(secrets.token_bytes(32)).decode(),'CONTROL_HTTP_ADDRESS':'127.0.0.1:'+str(self.ports['control']),'CONTROL_SESSION_COOKIE_SECURE':'false','CONTROL_BOOTSTRAP_MODE':'auto','CONTROL_BOOTSTRAP_USERNAME':'joint-admin','CONTROL_BOOTSTRAP_PASSWORD':self.secret(),'CONTROL_BOOTSTRAP_DISPLAY_NAME':'Joint fixture admin',**channel_env}
        self.secrets.append(env['CONTROL_PROFILE_CREDENTIAL_KEY']);self.admin_password=env['CONTROL_BOOTSTRAP_PASSWORD']
        self.contract_digest=self.command([self.binaries['control-api'],'--print-deployment-contract-digest'],env=env).strip()
        env['CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST']=self.contract_digest
        nats_file=self.write('control-runtime-nats.json',{'url':self.nats_url,'user':'control','password':self.env['NATS_CONTROL_PASSWORD'],'ca_file':self.certs['ca']})
        runtime={'internal_address':'127.0.0.1:'+str(self.ports['control_runtime']),'tls_cert_file':self.certs['server_cert'],'tls_key_file':self.certs['server_key'],'client_ca_file':self.certs['ca'],'execution_url':self.urls['worker'],'execution_ca_file':self.certs['ca'],'execution_cert_file':self.certs['control_cert'],'execution_key_file':self.certs['control_key'],'manifest_nats_file':nats_file,'workers':[{'principal_uri':'spiffe://agent-platform/worker/one','worker_id':'worker-one'},{'principal_uri':'spiffe://agent-platform/worker/two','worker_id':'worker-two'}]}
        env['CONTROL_RUNTIME_CONFIG_FILE']=self.write('control-runtime.json',runtime)
        from faults import ProofSwitch
        self.proof_switch=ProofSwitch(host='127.0.0.1',port=self.ports['worker']).start()
        self.control=self.spawn('control-api',[self.binaries['control-api']],env)
        self.wait(lambda:_http_status(self.urls['control']+'/healthz',timeout=1)==204,'Control process readiness')
    def seed(self, extra_tenants=0):
        if type(extra_tenants) is not int or extra_tenants < 0:
            raise ValueError('extra_tenants must be a non-negative integer')
        self.extra_tenant_ids=[]
        self.api('POST','/v1/auth/login',{'username':'joint-admin','password':self.admin_password})
        self.api('POST','/v1/me/change-password',{'current_password':self.admin_password,'new_password':self.secret()},status=204)
        password=self.secret()
        user=self.api('POST','/v1/admin/users',{'username':'joint-owner','display_name':'Joint Owner','temporary_password':password},status=201)
        tenant=self.api('POST','/v1/admin/tenants',{'slug':'joint-fixture','name':'Joint Fixture','owner_user_id':user['id']},status=201);self.tenant_id=tenant['id']
        for index in range(extra_tenants):
            tenant=self.api('POST','/v1/admin/tenants',{'slug':'joint-extra-'+str(index+1),'name':'Joint Extra '+str(index+1),'owner_user_id':user['id']},status=201)
            self.extra_tenant_ids.append(tenant['id'])
        self.api('POST','/v1/auth/login',{'username':'joint-owner','password':password})
        self.api('POST','/v1/me/change-password',{'current_password':password,'new_password':self.secret()},status=204)
        base='/v1/tenants/'+self.tenant_id
        agent=self.api('POST',base+'/agents',{'name':'Joint Worker V1 Agent'},status=201)['agent'];self.agent_id=agent['id']
        spec={'schema_version':'v1','root':'assistant','requirements':{'models':{'primary':{'capabilities':['chat']}},'tools':{},'knowledge':{}},'nodes':{'assistant':{'kind':'llm','instruction':'Reply with a short final answer.','model_slot':'primary','tool_slots':[],'knowledge_slots':[]}}}
        self.api('PUT',base+'/agents/'+self.agent_id+'/draft',{'expected_revision':1,'spec':spec})
        self.api('POST',base+'/agents/'+self.agent_id+'/versions',{'expected_revision':2},status=201)
        profile=self.api('POST',base+'/runtime-profiles',{'name':'Joint Worker V1 Profile'},status=201)['profile'];self.profile_id=profile['id']
        config={'models':{'primary':{'kind':'openai_compatible','model':self.model_name,'base_url':self.model.url+'/v1','capabilities':['chat']}},'tools':{},'knowledge':{},'storage':{'session':{'kind':'postgres_state','destination':{'host':'127.0.0.1','port':self.session_port,'database':'agent_platform','username':'session_runtime','sslmode':'disable'}}}}
        credentials={'models':{'primary':{'api_key':{'action':'replace','value':self.model.key}}},'storage':{'session':{'dsn':{'action':'replace','value':self.dsns['session_runtime']}}}}
        self.api('PUT',base+'/runtime-profiles/'+self.profile_id+'/draft',{'expected_draft_revision':1,'credential_protocol_version':'v1','config':config,'credentials':credentials},idem='joint-profile-draft')
        self.api('POST',base+'/runtime-profiles/'+self.profile_id+'/revisions',{'expected_revision':2},status=201)
        deployment=self.api('POST',base+'/deployments',{'name':'Joint Worker V1'},status=201,idem='joint-deployment-create')['deployment'];self.deployment_id=deployment['id']
        source={'schema_version':'v1','agent':{'agent_id':self.agent_id,'version_number':1},'profile':{'profile_id':self.profile_id,'revision_number':1}}
        result=self.api('POST',base+'/deployments/'+self.deployment_id+'/validate',source)
        if not result['valid']:raise RuntimeError('actual Control Worker V1 validation failed: '+json.dumps(result))
        publication=self.api('POST',base+'/deployments/'+self.deployment_id+'/revisions',{'expected_latest_revision_number':None,'input':source},status=201,idem='joint-deployment-publish')
        self.revision_id=publication['revision']['id'];self.revision_number=1
        self.manifest_id=publication['revision']['manifest_id'];self.manifest_digest=publication['revision']['manifest_digest']
        (self.artifacts/'control-publication.json').write_text(json.dumps(publication,indent=2)+'\n')
        self.wait(lambda:self.sql("SELECT status FROM control.control_outbox WHERE aggregate_id="+self.quote(self.deployment_id))==[['PUBLISHED']],'actual Control Manifest Relay PubAck')
        print('JOINT_CONTROL_HTTP_PUBLICATION=PASS AgentVersion + ProfileRevision + DeploymentRevision + Manifest Relay',flush=True)
    def start_worker(self, worker_id='worker-one', overrides=None):
        if worker_id not in self.worker_ports: raise RuntimeError('unmapped Worker identity')
        if worker_id in self.workers and self.workers[worker_id].poll() is None: raise RuntimeError('Worker already running')
        ports=self.worker_ports[worker_id]
        certname='worker' if worker_id=='worker-one' else 'worker_two'
        config=json.loads((self.root/'services/agent-worker/internal/bootstrap/example.json').read_text())
        config.update(worker_id=worker_id,platform_contract_digest=self.contract_digest,health_address='127.0.0.1:'+str(ports['health']),internal_address='127.0.0.1:'+str(ports['proof']),control_url=self.urls['control_runtime'],control_tls={'cert_file':self.certs[certname+'_cert'],'key_file':self.certs[certname+'_key'],'ca_file':self.certs['ca']},proof_tls={'cert_file':self.certs['server_cert'],'key_file':self.certs['server_key'],'client_ca_file':self.certs['ca']})
        config['nats_file']=self.write('worker-nats.json',{'url':self.nats_url,'user':'worker','password':self.env['NATS_WORKER_PASSWORD'],'ca_file':self.certs['ca']})
        config['policy'].update(lease_ttl='3s',renewal_interval='1s',retry_backoff='200ms',max_run_age='3m',max_reply_age='3m')
        config['timing'].update(poll_interval='20ms',health_interval='200ms',shutdown_drain_timeout='8s')
        def merge(a,b):
            for k,v in b.items():
                if isinstance(v,dict):merge(a[k],v)
                else:a[k]=v
        if overrides:merge(config,overrides)
        env={'WORKER_CONFIG_FILE':self.write(worker_id+'.json',config),'WORKER_DATABASE_URL':self.dsns['worker_runtime'],'WORKER_MIGRATION_DATABASE_URL':self.dsns['worker_migrator']}
        process=self.spawn(worker_id,[self.binaries['agent-worker']],env)
        self.workers[worker_id]=process
        if worker_id=='worker-one': self.worker=process
        self.proof_switch.add(worker_id,'127.0.0.1',ports['proof'])
        self.wait(lambda:_http_status('http://127.0.0.1:'+str(ports['health'])+'/readyz',timeout=1)==204,'Worker owner export/catch-up readiness')
        return process
    def verify_dependencies(self):
        """Real mTLS negative proof proves caller/route/owner connectivity, not a Grant."""
        attempt={'workload_identity':'worker-one','execution_token':'joint-preflight-not-a-grant','manifest_id':self.manifest_id,'manifest_digest':self.manifest_digest}
        checks=[]
        for name,origin,path,client,body in [
            ('Worker live proof',self.urls['worker'],'/internal/v1/execution/attempts:verify','control',attempt),
            ('Control online callback',self.urls['control_runtime'],'/internal/v1/runtime-profiles/credentials/resolve','worker',{'execution_token':attempt['execution_token'],'manifest_id':self.manifest_id,'manifest_digest':self.manifest_digest,'uses':[]})]:
            context=ssl.create_default_context(cafile=self.certs['ca'])
            context.load_cert_chain(self.certs[client+'_cert'],self.certs[client+'_key'])
            request=urllib.request.Request(origin+path,data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
            try: response=urllib.request.urlopen(request,context=context,timeout=8)
            except urllib.error.HTTPError as error: response=error
            with response:
                raw=json.loads(response.read()); status=response.status
            checks.append({'name':name,'status':status,'response':raw})
            if status!=403: raise RuntimeError('joint mTLS preflight '+json.dumps(checks))
        out=self.command(['docker','exec','-e','PGPASSWORD',self.pg,'psql','-X','-A','-t','-h','127.0.0.1','-U','session_runtime','-d','agent_platform','-c',"SELECT current_schema(),current_user,has_table_privilege(current_user,'runtime_session.session_candidates','SELECT') AND has_table_privilege(current_user,'runtime_session.session_candidates','INSERT') AND NOT has_table_privilege(current_user,'runtime_session.session_candidates','UPDATE,DELETE,TRUNCATE,TRIGGER,REFERENCES,MAINTAIN')"],env={'PGPASSWORD':self.env['SESSION_RUNTIME_PASSWORD']})
        if out.strip()!='runtime_session|session_runtime|t':raise RuntimeError('Session target preflight differs')
        (self.artifacts/'dependency-preflight.json').write_text(json.dumps({'mtls':checks,'session':out.strip()},indent=2)+'\n')
    def stop_worker(self, kill=False, worker_id='worker-one'):
        process=self.workers.get(worker_id)
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            process.wait(timeout=25)
        self.proof_switch.remove(worker_id)
    def start_fault_workers(self, max_attempts=2):
        self.stop_fault_workers()
        for worker_id in ('worker-one','worker-two'):
            self.start_worker(worker_id,{'policy':{'max_attempts':max_attempts}})
        return dict(self.workers)
    def stop_fault_workers(self):
        for worker_id in list(self.workers): self.stop_worker(worker_id=worker_id)
    def send_text(self, text, conversation_id='42', update_id=None):return self.gateway.send_text(text,conversation_id,update_id)
    def wait_delivery(self, run_id):
        query="SELECT status,COALESCE(wait_reason,'') FROM worker.execution_runs WHERE run_id="+self.quote(run_id)
        terminal=self.wait(lambda:(lambda rows:rows if rows and rows[0][0] in ('SUCCEEDED','FAILED') else None)(self.sql(query)), 'Worker terminal outcome before Gateway Delivery', timeout=90)
        if terminal[0][0]!='SUCCEEDED':
            facts=self.sql("SELECT status,reason,reply_disposition FROM worker.execution_completions WHERE run_id="+self.quote(run_id))
            (self.artifacts/'failed-execution.json').write_text(json.dumps({'run_id':run_id,'state':terminal,'completion':facts},indent=2)+'\n')
            raise RuntimeError('actual Worker ended before expected Final: '+json.dumps(facts))
        return self.gateway.wait_delivery(run_id)
    def close(self):
        errors=[]
        try:
            if hasattr(self,'gateway'):self.gateway.close()
            else:
                if hasattr(self,'gateway_ingress'):self.gateway_ingress.close()
                if hasattr(self,'gateway_fixture'):self.gateway_fixture.close()
        except Exception as exc:errors.append(str(exc))
        if hasattr(self,'model'):self.model.close()
        for name,p in reversed(self.processes):
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
                try:p.wait(timeout=20)
                except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5)
        if hasattr(self,'session_fault_proxy'):
            self.session_fault_proxy.close()
            closed=self.session_fault_proxy.evidence()
            if not closed['closed'] or closed['live_connections'] != 0:errors.append('Session proxy cleanup')
            path=self.artifacts/'session-commit-proxy-cleanup.json'
            path.write_text(json.dumps(closed,indent=2)+'\n')
            if json.loads(path.read_text()) != closed:errors.append('Session proxy cleanup evidence readback')
        if hasattr(self,'resolve_proxies'):
            snapshots=[]
            for proxy in reversed(self.resolve_proxies):
                proxy.close()
                snapshot=proxy.evidence()
                if not snapshot['closed'] or snapshot['live_connections'] != 0:errors.append('Resolve proxy cleanup')
                snapshots.append(snapshot)
            path=self.artifacts/'resolve-proxies-cleanup.json'
            path.write_text(json.dumps(snapshots,indent=2)+'\n')
            if json.loads(path.read_text()) != snapshots:errors.append('Resolve proxy cleanup evidence readback')
        if hasattr(self,'proof_switch'):self.proof_switch.close()
        for handle in self.handles:handle.close()
        with open(self.artifacts/'process-events.jsonl','a') as record:
            for name,process in self.processes:
                record.write(json.dumps({'event':'exited','name':name,'pid':process.pid,'exit_code':process.returncode})+'\n')
                if process.returncode not in (0,-signal.SIGKILL): errors.append('process exit '+name+':'+str(process.returncode))
        for log in self.artifacts.glob('*.log'):
            value=self.redact(log.read_text(errors='replace'));log.write_text(value)
            if 'WARNING: DATA RACE' in value: errors.append('race detector '+log.name)
        for name in reversed(self.containers):
            result=subprocess.run(['docker','rm','-f',name],capture_output=True)
            if result.returncode or subprocess.run(['docker','inspect',name],capture_output=True).returncode==0:errors.append('container cleanup '+name)
        shutil.rmtree(self.work)
        if errors:raise RuntimeError('; '.join(errors))
        print('WORKER_JOINT_CLEANUP=PASS processes stopped; containers removed; private fixture files removed',flush=True)
