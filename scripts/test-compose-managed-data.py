#!/usr/bin/env python3
"""Operator-controlled Compose smoke: bootstrap, before, operator restart, after.

Never starts/stops/restarts the stack. Creates only a dedicated owner/tenant and
unpublished Profile draft; no Agent, Deployment, Bot or model call is made.
Backend durability probes use reserved infrastructure names, not runtime data.
"""
import argparse
import hashlib
import http.cookiejar
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys
import urllib.error
import urllib.request
from urllib.parse import urlparse


class Failure(Exception):pass


INIT_JOBS={'database-schemas','session-prepare','nats-reconcile','backend-init','memory-schema','memory-prepare'}


def private_json(path):
    path=Path(path)
    if stat.S_IMODE(path.stat().st_mode)&0o077:raise Failure('private state file permissions')
    return json.loads(path.read_text())


def save_private(path,value):
    path=Path(path);temporary=path.with_name(path.name+'.tmp')
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,'w') as out:json.dump(value,out);out.write('\n')
    os.chmod(temporary,0o600);os.replace(temporary,path)


def secret(path):
    p=Path(path)
    if stat.S_IMODE(p.stat().st_mode)&0o077:raise Failure('secret file permissions')
    value=p.read_text().removesuffix('\n')
    if not value or '\n' in value or '\r' in value:raise Failure('invalid secret file')
    return value


def assert_catalog(view,catalog,tenant):
    assert set(view)=={'items'} and isinstance(view['items'],list)
    expected=[]
    for entry in catalog['backends']:
        if tenant in entry['tenant_ids']:
            expected.append({'id':entry['id'],'revision':entry['revision'],'label':entry['label'],'kind':entry['kind'],
                'roles':sorted(entry['roles']),'available':entry['enabled']})
    assert view['items']==sorted(expected,key=lambda x:x['id']),'actual public catalog differs from explicit grant'
    for item in view['items']:assert set(item)=={'id','revision','label','kind','roles','available'}
    return view['items']


def assert_profile_read(value,config,expected_slots,secret_values):
    raw=json.dumps(value,ensure_ascii=False)
    assert all(not s or s not in raw for s in secret_values),'Profile response echoed secret'
    assert value['config']==config and value['credential_protocol_version']=='v1'
    assert 'spec' not in value and 'credentials' not in value
    assert 'credential_id' not in raw and 'ciphertext' not in raw
    states=value['credential_states']
    actual={(category,resource,purpose) for category,resources in states.items() for resource,purposes in resources.items() for purpose in purposes}
    assert actual==set(expected_slots),'Profile credential slots changed'
    for category,resource,purpose in expected_slots:
        state=states[category][resource][purpose]
        assert set(state)<={'configured','status','credential_revision','association_token'}
        assert state['configured'] is True and state['status']=='active' and state['credential_revision']==1
        assert isinstance(state.get('association_token'),str) and state['association_token']


def validate_config(config,project):
    assert config['name']==project and project not in ('trpc-agent-latest','channel-lab-dev')
    services=config['services']
    required={'postgres','nats','control-api','channel-gateway','agent-worker','web','redis','qdrant','minio','backend-tools'}
    assert required|INIT_JOBS<=set(services),'missing complete-stack services or initialization jobs'
    for name,service in services.items():
        assert not service.get('container_name'),'explicit container names cross project isolation'
        assert not service.get('network_mode') in ('host','service:trpc-agent-latest')
        for port in service.get('ports',[]):
            assert name not in ('postgres','nats','redis','qdrant','minio'),'backend must not have host ports'
            assert port.get('host_ip') in ('127.0.0.1','::1'),'published application port is not loopback'
    for volume in config.get('volumes',{}).values():
        assert not volume.get('external'),'external persistent volume'
        assert volume.get('name','').startswith(project+'_'),'volume name crosses project'
    for network in config.get('networks',{}).values():assert not network.get('external'),'external network'
    for name in ('postgres','redis','qdrant','minio'):
        assert any(v['type']=='volume' for v in services[name].get('volumes',[])),'backend lacks persistent named volume'
    return sorted(required)


def validate_containers(records,project,required,jobs=()):
    observations={}
    for obj in records:
        labels=obj['Config']['Labels'];assert labels.get('com.docker.compose.project')==project
        service=labels['com.docker.compose.service'];state=obj['State']
        assert service not in observations,'ambiguous duplicate service containers'
        if state['Status']=='exited':assert state['ExitCode']==0,'initialization job failed'
        else:
            assert state['Running'] is True and state['Status']=='running'
            health=state.get('Health',{}).get('Status')
            if health is not None:assert health=='healthy','service has not reached health'
        observations[service]={'status':state['Status'],'health':state.get('Health',{}).get('Status','NOT_DEFINED'),
            'exit_code':state['ExitCode'],'container_id':obj['Id'],'started_at':state['StartedAt']}
    assert set(required)<=set(observations)
    assert all(observations[name]['status']=='running' for name in required)
    assert set(jobs)<=set(observations),'missing initialization job evidence'
    assert all(observations[name]['status']=='exited' and observations[name]['exit_code']==0 for name in jobs),'initialization job not completed'
    return observations


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*_):return None


class HTTP:
    def __init__(self,base):
        p=urlparse(base)
        if p.scheme!='http' or p.hostname not in ('127.0.0.1','localhost') or p.username or p.password or p.query or p.fragment:raise Failure('expected loopback Control origin')
        self.base=base.rstrip('/');self.jar=http.cookiejar.CookieJar()
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPCookieProcessor(self.jar),NoRedirect())
        self.observations=[]
    def call(self,method,path,body=None,expected=200):
        req=urllib.request.Request(self.base+path,data=None if body is None else json.dumps(body).encode(),method=method,headers={'Content-Type':'application/json','Idempotency-Key':'smoke-'+secrets.token_hex(16)})
        try:response=self.opener.open(req,timeout=20)
        except urllib.error.HTTPError as error:response=error
        except (OSError,urllib.error.URLError):raise Failure('Control HTTP dependency') from None
        with response:status=response.status;raw=response.read(2*1024*1024+1)
        self.observations.append({'method':method,'path':path,'status':status})
        if status!=expected:raise Failure('Control '+method+' '+path+' status '+str(status)+' expected '+str(expected))
        if len(raw)>2*1024*1024:raise Failure('Control response bound')
        return json.loads(raw) if raw else None
    def login(self,username,password):self.call('POST','/v1/auth/login',{'username':username,'password':password})


class Smoke:
    def __init__(self,args):
        self.args=args;self.meta=json.loads(args.metadata.read_text())
        if self.meta['schema_version']!='managed-compose-v1':raise Failure('metadata schema')
        self.project=self.meta['project'];self.state_dir=Path(self.meta['state_dir'])
        self.state_path=self.state_dir/'smoke-private.json';self.secrets_dir=Path(self.meta['files']['secrets_dir'])
        self.artifacts=args.artifacts;self.artifacts.mkdir(parents=True,exist_ok=True)
        self.base=['docker','compose','--project-name',self.project,'--env-file',self.meta['files']['compose_env']]
        for path in args.compose_file:self.base+=['-f',str(path.resolve())]
        self.api=HTTP(self.meta['urls']['control']);self.report={'phase':args.phase,'result':'PENDING','project':self.project,'model_calls':0,'agent_deployments':0,'im_registrations':0}
        self.known_secrets=[secret(path) for path in self.secrets_dir.iterdir() if path.is_file()]
    def command(self,argv,*,input=None):
        environment=None
        if list(argv[:2])==['docker','compose']:
            envfile=Path(argv[argv.index('--env-file')+1]);values={}
            # The generator writes plain single-line values (no interpolation).
            # Compose gives ambient variables precedence over --env-file, so
            # explicitly overlay this state's values while keeping CLI context.
            for line in envfile.read_text().splitlines():
                if not line or line.startswith('#'):continue
                key,value=line.split('=',1);values[key]=value
            environment={**os.environ,**values}
        result=subprocess.run(argv,input=input,text=True,capture_output=True,timeout=120,env=environment)
        if result.returncode:raise Failure('local deployment command failed exit='+str(result.returncode))
        return result.stdout
    def compose(self,*argv,input=None):return self.command(self.base+list(argv),input=input)
    def tool(self,*argv):
        output=self.compose('exec','-T','backend-tools','python3','/tooling/backendctl.py',*argv)
        if '=PASS' not in output:raise Failure('backend tool missing completed PASS')
        return output.strip()
    def save(self):
        self.report['http']=self.api.observations
        raw=json.dumps(self.report,indent=2,ensure_ascii=False)+'\n'
        for value in self.known_secrets:
            if value and value in raw:raise Failure('secret in report')
        path=self.artifacts/('compose-'+self.args.phase+'.json');path.write_text(raw)
        assert path.read_text()==raw
    def bootstrap(self):
        if self.state_path.exists():raise Failure('bootstrap state exists; do not repeat account creation')
        username=self.meta['bootstrap']['username'];password_file=self.meta['bootstrap']['password_file'];password=secret(password_file)
        self.api.login(username,password)
        me=self.api.call('GET','/v1/me')
        # A fresh bootstrap administrator must rotate its temporary password.
        # Preserve the current login value only in smoke-private.json; the
        # generator's bootstrap secret remains its initial provisioning value.
        if me.get('password_change_required') or me.get('restricted') or me.get('must_change_password'):
            new=secrets.token_urlsafe(32);self.known_secrets.append(new)
            self.api.call('POST','/v1/me/change-password',{'current_password':password,'new_password':new},expected=204)
            password=new
        progress={'project':self.project,'bootstrap_complete':False,'admin_username':username,'admin_password':password}
        save_private(self.state_path,progress)
        owner='managed-smoke-'+secrets.token_hex(5);temporary=secrets.token_urlsafe(32)
        self.known_secrets.append(temporary)
        user=self.api.call('POST','/v1/admin/users',{'username':owner,'display_name':'Managed deployment smoke owner','temporary_password':temporary},expected=201)
        tenant=self.api.call('POST','/v1/admin/tenants',{'slug':owner,'name':'Managed deployment smoke','owner_user_id':user['id']},expected=201)
        foreign=self.api.call('POST','/v1/admin/tenants',{'slug':owner+'-empty','name':'Ungrant managed smoke','owner_user_id':user['id']},expected=201)
        self.api.login(owner,temporary);current=secrets.token_urlsafe(32);self.known_secrets.append(current)
        self.api.call('POST','/v1/me/change-password',{'current_password':temporary,'new_password':current},expected=204)
        state={**progress,'bootstrap_complete':True,'tenant_id':tenant['id'],'ungranted_tenant_id':foreign['id'],'owner_username':owner,'owner_password':current,'probe_id':secrets.token_hex(8)}
        save_private(self.state_path,state)
        self.report.update(result='PASS',tenant_id=tenant['id'],ungranted_tenant_id=foreign['id'],next='operator grant tenant, repin and restart owned Control/Worker; then before')
    def inspect(self):
        config=json.loads(self.compose('config','--format','json'));required=validate_config(config,self.project)
        ids=self.compose('ps','--all','--quiet').split()
        if not ids:raise Failure('no owned stack containers')
        containers=json.loads(self.command(['docker','inspect',*ids]))
        states=validate_containers(containers,self.project,required,INIT_JOBS)
        self.report['services']=states
        self.report['compose_config_sha256']=hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest()
        web=urllib.request.Request(self.meta['urls']['web'].rstrip('/')+'/login')
        try:
            with urllib.request.urlopen(web,timeout=15) as response:
                assert response.status==200;response.read(2*1024*1024)
        except urllib.error.HTTPError as error:
            error.close();raise Failure('Web actual HTTP login page unavailable') from None
        except (OSError,urllib.error.URLError):raise Failure('Web actual HTTP login page unavailable') from None
        self.report['web_http']={'status':200,'path':'/login'}
        return states
    def pg(self,sql):
        command='export PGPASSWORD="$POSTGRES_PASSWORD"; exec psql -X -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
        return self.compose('exec','-T','postgres','sh','-ec',command,input=sql+'\n').strip()
    def pg_check(self):
        owners={'control':'control_migrator','gateway':'gateway_migrator','worker':'worker_migrator','runtime_session':'session_migrator','runtime_memory':'memory_migrator'}
        schema_names=','.join("'"+name+"'" for name in owners)
        expected_roles={name+'_'+kind for name in ('control','gateway','worker','session','memory') for kind in ('migrator','runtime')}
        role_names=','.join("'"+name+"'" for name in sorted(expected_roles))
        schemas=json.loads(self.pg('SELECT json_object_agg(nspname,pg_get_userbyid(nspowner)) FROM pg_namespace WHERE nspname IN ('+schema_names+');'))
        assert schemas==owners,'five schema ownership mismatch'
        roles=json.loads(self.pg("SELECT json_agg(json_build_object('name',rolname,'super',rolsuper,'createdb',rolcreatedb,'createrole',rolcreaterole,'replication',rolreplication,'bypassrls',rolbypassrls,'memberships',(SELECT count(*) FROM pg_auth_members WHERE member=r.oid),'owned_databases',(SELECT count(*) FROM pg_database WHERE datdba=r.oid))) FROM pg_roles r WHERE rolname IN ("+role_names+');'))
        assert len(roles)==10 and {row['name'] for row in roles}==expected_roles
        assert all(not row[k] for row in roles for k in ('super','createdb','createrole','replication','bypassrls','memberships','owned_databases')),'unexpected role privileges'
        runtime_rows=','.join("('"+owner.removesuffix('_migrator')+"_runtime','"+schema+"')" for schema,owner in owners.items())
        schema_rows=','.join("('"+name+"')" for name in owners)
        cross=int(self.pg("SELECT count(*) FROM (VALUES "+runtime_rows+") AS r(role_name,own_schema) CROSS JOIN (VALUES "+schema_rows+") AS s(schema_name) WHERE r.own_schema<>s.schema_name AND has_schema_privilege(r.role_name,s.schema_name,'USAGE');"))
        assert cross==0,'unexpected runtime cross-schema grant'
        tables=json.loads(self.pg("SELECT json_object_agg(c.relname,json_build_object('owner',pg_get_userbyid(c.relowner),'privileges',(SELECT coalesce(json_agg(priv ORDER BY priv),'[]'::json) FROM unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN']) AS p(priv) WHERE has_table_privilege('memory_runtime',c.oid,priv)))) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='runtime_memory' AND c.relkind='r' AND c.relname IN ('memory_heads','memory_receipts','memory_schema_migrations');"))
        expected_tables={name:{'owner':'memory_migrator','privileges':privileges} for name,privileges in [('memory_heads',['INSERT','SELECT','UPDATE']),('memory_receipts',['INSERT','SELECT']),('memory_schema_migrations',['SELECT'])]}
        assert tables==expected_tables,'prepared Memory tables or exact runtime grants differ'
        self.report['postgres']={'schemas':schemas,'least_privileged_roles':10,'runtime_cross_schema_checks':20,'cross_schema_usage_grants':0,'memory_tables':tables}
    def backend_args(self,service):
        if service=='redis':return ['--host','redis','--password-file','/run/secrets/redis_admin']
        if service=='qdrant':return ['--endpoint','http://qdrant:6333','--api-key-file','/run/secrets/qdrant_api_key']
        targets=json.loads(Path(self.meta['files']['targets']).read_text())['backends']
        values=[x['s3'] for x in targets if x['kind']=='s3'];assert len(values)==1
        return ['--endpoint',values[0]['endpoint'],'--bucket',values[0]['bucket'],'--region',values[0]['region'],
                '--access-key-file','/run/secrets/minio_app_user','--secret-key-file','/run/secrets/minio_app_password']
    def acl_check(self):
        code="""import argparse,json,sys
sys.path.insert(0,'/tooling');import backendctl as b
a=argparse.Namespace(host='redis',port=6379,tls=False,timeout=5,username='deployment_admin',password_file='/run/secrets/redis_admin')
def raw(*cmd):return b.redis(a,*cmd)
def deny(*cmd):
 try:raw(*cmd)
 except b.Failure as error:
  if str(error)=='Redis command rejected':return True
  raise
 return False
result={}
default=raw('ACL','GETUSER','default');default=dict(zip(default[::2],default[1::2]));assert 'off' in default['flags']
for role in ('memory','session'):
 user=role+'_runtime';rule=raw('ACL','GETUSER',user);rule=dict(zip(rule[::2],rule[1::2]));assert 'on' in rule['flags']
 assert rule['keys']=='~runtime_'+role+':*'
 command='MGET' if role=='memory' else 'GET'
 assert raw('ACL','DRYRUN',user,command,'runtime_'+role+':deployment_probe')=='OK'
 assert raw('ACL','DRYRUN',user,'GET','deployment_smoke:outside')=="User "+user+" has no permissions to access the 'deployment_smoke:outside' key"
 assert raw('ACL','DRYRUN',user,'FLUSHALL')=="User "+user+" has no permissions to run the 'flushall' command"
 a.username=user;a.password_file='/run/secrets/redis_'+role
 assert raw('PING')=='PONG';raw(command,'runtime_'+role+':deployment_probe')
 assert deny('GET','deployment_smoke:outside')
 a.username='deployment_admin';a.password_file='/run/secrets/redis_admin'
 result[user]={'authenticated':True,'own_namespace':True,'foreign_namespace_denied':True,'flush_denied':True}
print(json.dumps({'default_off':True,'users':result}))"""
        self.report['redis_acl']=json.loads(self.compose('exec','-T','backend-tools','python3','-c',code))
    def profile(self,state,catalog):
        tenant=state['tenant_id'];base='/v1/tenants/'+tenant+'/runtime-profiles'
        entries=[x for x in catalog['backends'] if tenant in x['tenant_ids'] and x['enabled']]
        def select(kind,role):
            found=[x for x in entries if x['kind']==kind and role in x['roles']]
            assert len(found)==1,'exact available backend selection required';return found[0]
        config={'models':{},'tools':{},'knowledge':{},'storage':{}};credentials={'storage':{}};slots=[]
        def value(name):
            v=secret(self.secrets_dir/name);self.known_secrets.append(v);return v
        for role in ('session','memory'):
            backend=select('redis',role);config['storage'][role]={'kind':'managed_'+role,'backend_id':backend['id'],'backend_revision':backend['revision']}
            credentials['storage'][role]={'dsn_password':{'action':'replace','value':value('redis_'+role)}};slots.append(('storage',role,'dsn_password'))
        backend=select('s3','artifact');config['storage']['artifact']={'kind':'managed_artifact','backend_id':backend['id'],'backend_revision':backend['revision']}
        credentials['storage']['artifact']={purpose:{'action':'replace','value':value(file)} for purpose,file in [('access_key_id','minio_app_user'),('secret_access_key','minio_app_password')]}
        slots += [('storage','artifact','access_key_id'),('storage','artifact','secret_access_key')]
        knowledge=self.meta['knowledge']
        if knowledge['enabled']:
            backend=select('qdrant','knowledge');dimensions=knowledge['dimensions'];assert type(dimensions) is int and dimensions>0
            config['knowledge']['docs']={'kind':'managed_knowledge','backend_id':backend['id'],'backend_revision':backend['revision'],
                'embedding':{'model':'deployment-smoke-unpublished','base_url':'http://127.0.0.1:9/v1','dimensions':dimensions}}
            embed=secrets.token_urlsafe(32);self.known_secrets.append(embed)
            credentials['knowledge']={'docs':{'qdrant_api_key':{'action':'replace','value':value('qdrant_api_key')},'embedding_api_key':{'action':'replace','value':embed}}}
            slots += [('knowledge','docs','qdrant_api_key'),('knowledge','docs','embedding_api_key')]
        if state.get('profile_id'):raise Failure('Profile smoke already started; inspect existing draft rather than repeat writes')
        result=self.api.call('POST',base,{'name':'Managed deployment smoke draft'},expected=201)
        profile=result['profile']['id'];state['profile_id']=profile;save_private(self.state_path,state)
        draft=self.api.call('PUT',base+'/'+profile+'/draft',{'expected_draft_revision':1,'credential_protocol_version':'v1','config':config,'credentials':credentials})
        read=self.api.call('GET',base+'/'+profile+'/draft')
        assert_profile_read(read,config,slots,self.known_secrets)
        state['profile_config']=config;state['profile_slots']=slots;save_private(self.state_path,state)
        self.report['profile']={'id':profile,'draft_revision':read['draft_revision'],'published':False,'write_only_reopen':'PASS','slots':[list(x) for x in slots]}
    def verify(self):
        if self.meta['status']!='PINNED':raise Failure('operator must pin exact generated configuration')
        state=private_json(self.state_path);assert state['project']==self.project
        self.known_secrets.extend([state['owner_password'],state.get('admin_password','')]);self.api.login(state['owner_username'],state['owner_password'])
        catalog=json.loads(Path(self.meta['files']['catalog']).read_text());tenant=state['tenant_id']
        items=assert_catalog(self.api.call('GET','/v1/tenants/'+tenant+'/runtime-backends'),catalog,tenant)
        assert items,'operator grant and restart required'
        assert_catalog(self.api.call('GET','/v1/tenants/'+state['ungranted_tenant_id']+'/runtime-backends'),catalog,state['ungranted_tenant_id'])
        assert not any(state['ungranted_tenant_id'] in e['tenant_ids'] for e in catalog['backends'])
        self.report['catalog']={'tenant_id':tenant,'items':items,'ungranted_tenant_items':0}
        self.report['business_knowledge']='CONFIGURED_EXPLICIT_DIMENSIONS' if self.meta['knowledge']['enabled'] else 'NOT_CONFIGURED'
        current=self.inspect();self.pg_check();self.acl_check()
        probe=state['probe_id'];assert re.fullmatch('[0-9a-f]{16}',probe)
        table='worker.deployment_smoke_'+probe;marker='deployment-persistence-probe-v1:'+probe
        if self.args.phase=='before':
            if state.get('before_complete'):raise Failure('before already completed; use after, do not repeat writes')
            self.profile(state,catalog)
            self.pg("SET ROLE worker_migrator; CREATE TABLE IF NOT EXISTS "+table+" (id integer PRIMARY KEY CHECK(id=1), marker text NOT NULL); INSERT INTO "+table+" VALUES (1,'"+marker+"') ON CONFLICT DO NOTHING; RESET ROLE;")
            assert self.pg('SELECT marker FROM '+table+' WHERE id=1;')==marker
            self.report['durability_writes']={name:self.tool('smoke','--service',name,'--action','write','--probe-id',probe,*self.backend_args(name)) for name in ('redis','minio','qdrant')}
            state.update(before_complete=True,services_before=current);save_private(self.state_path,state)
        else:
            assert state.get('before_complete'),'missing completed before checkpoint'
            before=state['services_before']
            for name in ('postgres','redis','minio','qdrant'):
                assert current[name]['started_at']!=before[name]['started_at'],'operator did not restart backend '+name
            assert self.pg('SELECT marker FROM '+table+' WHERE id=1;')==marker
            self.report['durability_reads']={name:self.tool('smoke','--service',name,'--action','read','--probe-id',probe,*self.backend_args(name)) for name in ('redis','minio','qdrant')}
            read=self.api.call('GET','/v1/tenants/'+tenant+'/runtime-profiles/'+state['profile_id']+'/draft')
            assert_profile_read(read,state['profile_config'],[tuple(x) for x in state['profile_slots']],self.known_secrets)
            self.report['profile_after_restart']={'write_only_reopen':'PASS','draft_revision':read['draft_revision']}
            self.report['probe_cleanup']={name:self.tool('smoke','--service',name,'--action','delete','--probe-id',probe,*self.backend_args(name)) for name in ('redis','minio','qdrant')}
            self.pg('SET ROLE worker_migrator; DROP TABLE '+table+'; RESET ROLE;')
            self.report['postgres_probe_removed']=True
            state['after_complete']=True;save_private(self.state_path,state)
        self.report.update(result='PASS',probe_id=probe,qdrant_probe={'dimensions':1,'collection':'deployment_smoke_'+probe,'business_embedding':False})


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--metadata',type=Path,required=True)
    parser.add_argument('--compose-file',type=Path,action='append',required=True)
    parser.add_argument('--artifacts',type=Path,required=True)
    parser.add_argument('--phase',choices=['bootstrap','before','after'],required=True)
    args=parser.parse_args();smoke=None
    try:
        smoke=Smoke(args)
        if args.phase=='bootstrap':smoke.bootstrap()
        else:smoke.verify()
        smoke.save();print('COMPOSE_MANAGED_SMOKE=PASS phase='+args.phase)
        return 0
    except Exception as exc:
        if smoke:
            smoke.report.update(result='FAIL',error_type=type(exc).__name__)
            if isinstance(exc,Failure):smoke.report['reason']=str(exc)
            smoke.save()
        print('COMPOSE_MANAGED_SMOKE=FAIL phase='+args.phase,file=sys.stderr)
        return 1


if __name__=='__main__':sys.exit(main())
