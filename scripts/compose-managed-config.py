#!/usr/bin/env python3
"""Generate private, stable configuration for one independent managed Compose.

This command never starts services, calls tenant APIs, or enables Agent features.
init -> actual Control image --print-deployment-contract-digest -> pin -> startup.
An explicit grant changes only the release-owned tenant allowlist, invalidates the
pin, and requires the operator's repin/restart. PostgreSQL stays one database.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys
import tempfile
import uuid
from urllib.parse import quote

ROOT=Path(__file__).resolve().parents[1]
DEFAULT_STATE=Path.home()/'.local/share/trpc-agent-service/managed-local'
VERSION='managed-compose-v1'
CLIENT_IDENTITIES={'control-client':'spiffe://agent-platform/control-api','worker-client':'spiffe://agent-platform/worker/one','gateway-client':'spiffe://agent-platform/channel-gateway'}
IDENTIFIER=re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$')
NAME=re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$')
DIGEST=re.compile(r'^sha256:[0-9a-f]{64}$')
PG_ROLES=[f'{service}_{role}' for service in ('control','gateway','worker','session','memory') for role in ('migrator','runtime')]
class ConfigError(RuntimeError):pass

def read_json(path):
    if path.is_symlink():raise ConfigError('configuration symlink rejected')
    try:return json.loads(path.read_text())
    except (OSError,ValueError):raise ConfigError('configuration file unavailable or invalid') from None

def private_dir(path):
    if path.is_symlink():raise ConfigError('state directory symlink rejected')
    path.mkdir(parents=True,exist_ok=True,mode=0o700)
    if not path.is_dir() or path.stat().st_uid!=os.getuid():raise ConfigError('state directory must be owned by the current user')
    path.chmod(0o700)

def write(path,value):
    private_dir(path.parent)
    if path.is_symlink():raise ConfigError('configuration symlink rejected')
    raw=(json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2)+'\n').encode() if not isinstance(value,(bytes,str)) else value.encode() if isinstance(value,str) else value
    if path.exists() and path.read_bytes()==raw:
        path.chmod(0o600);return
    fd,temporary=tempfile.mkstemp(prefix='.'+path.name+'.',dir=path.parent)
    try:
        os.fchmod(fd,0o600)
        with os.fdopen(fd,'wb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
    finally:
        if os.path.exists(temporary):os.unlink(temporary)

def command(argv):
    try:result=subprocess.run([str(v) for v in argv],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=45,check=False)
    except (OSError,subprocess.TimeoutExpired):raise ConfigError('local configuration tool failed') from None
    if result.returncode:raise ConfigError('local configuration tool rejected configuration')
    return result.stdout

def validate_knowledge(value):
    d=value['dimensions']
    if d is not None and (type(d) is not int or not 1<=d<=65536):raise ConfigError('explicit Knowledge dimensions must be 1..65536')
    if not NAME.fullmatch(value['collection']) or not NAME.fullmatch(value['vector_name']) or value['distance'] not in ('cosine','dot','euclid'):raise ConfigError('Knowledge named vector configuration invalid')

def initial_settings(project='trpc-agent-managed-local',ports=None,knowledge=None):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,62}',project):raise ConfigError('Compose project name invalid')
    values={'web':23000,'control':28080,'gateway':28090,'gateway_admin':28091,'worker':28083};values.update(ports or {})
    if any(type(v) is not int or not 1024<=v<=65535 for v in values.values()) or len(set(values.values()))!=5:raise ConfigError('host ports must be distinct unprivileged ports')
    know={'dimensions':None,'collection':'worker_knowledge','vector_name':'published_dense','distance':'cosine'};know.update(knowledge or {});validate_knowledge(know)
    return {'project':project,'ports':values,'knowledge':know,'database':'agent_platform','bootstrap_username':'managed-admin','scope_id':'managed-local','worker_id':'worker-one','gateway_instance_id':'gateway-1','runtime_uid':os.getuid(),'runtime_gid':os.getgid()}

def make_pki(state):
    directory=state/'pki';private_dir(directory)
    if (directory/'complete.json').exists():
        for name in ['ca',*CLIENT_IDENTITIES,'control-server','worker-server','nats-server']:
            for suffix in ['.pem','.key']:
                p=directory/(name+suffix)
                if p.is_symlink() or not p.is_file() or stat.S_IMODE(p.stat().st_mode)!=0o600:raise ConfigError('existing TLS material is missing or has incorrect permissions')
        return
    if any(directory.iterdir()):raise ConfigError('partial TLS generation requires explicit removal of that unused state directory')
    write(directory/'ca.cnf','[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=ca_ext\n[dn]\nCN=Managed local platform CA\n[ca_ext]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n')
    command(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',directory/'ca.key','-out',directory/'ca.pem','-config',directory/'ca.cnf','-days','365'])
    for name in ['ca']:
        for suffix in ['.key','.pem']:(directory/(name+suffix)).chmod(0o600)
    servers={'control-server':'control-api','worker-server':'agent-worker','nats-server':'nats'}
    for name in [*CLIENT_IDENTITIES,*servers]:
        command(['openssl','req','-new','-newkey','rsa:2048','-nodes','-keyout',directory/(name+'.key'),'-out',directory/(name+'.csr'),'-subj','/CN='+name])
        san='URI:'+CLIENT_IDENTITIES[name] if name in CLIENT_IDENTITIES else 'DNS:'+servers[name]+',DNS:localhost,IP:127.0.0.1'
        usage='clientAuth' if name in CLIENT_IDENTITIES else 'serverAuth'
        write(directory/(name+'.ext'),'basicConstraints=critical,CA:FALSE\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage='+usage+'\nsubjectAltName='+san+'\n')
        command(['openssl','x509','-req','-in',directory/(name+'.csr'),'-CA',directory/'ca.pem','-CAkey',directory/'ca.key','-set_serial','0x'+secrets.token_hex(16),'-out',directory/(name+'.pem'),'-days','365','-extfile',directory/(name+'.ext')])
        for suffix in ['.key','.csr','.pem']:(directory/(name+suffix)).chmod(0o600)
    write(directory/'complete.json',{'version':VERSION,'days':365})

def initialize(state,settings=None):
    state=Path(state).absolute()
    private_dir(state)
    path=state/'state.json'
    if path.exists():
        data=read_json(path)
        if data.get('version')!=VERSION:raise ConfigError('state version mismatch')
        if settings is not None and data['settings']!=settings:raise ConfigError('existing state settings differ; no implicit secret or identity rotation')
    else:
        if any(state.iterdir()):raise ConfigError('new state directory must be empty')
        settings=settings or initial_settings()
        values={role:secrets.token_urlsafe(32) for role in PG_ROLES}
        values.update({name:secrets.token_urlsafe(32) for name in ['pg_admin','bootstrap_password','redis_admin','redis_memory','redis_session','minio_password','minio_app_password','qdrant_api_key','nats_control','nats_gateway','nats_worker','nats_reconciler']})
        values.update(minio_user='root-'+secrets.token_hex(8),minio_app_user='artifact-'+secrets.token_hex(8),profile_key=base64.b64encode(secrets.token_bytes(32)).decode(),channel_encryption=base64.b64encode(secrets.token_bytes(32)).decode(),channel_mac=base64.b64encode(secrets.token_bytes(32)).decode())
        data={'version':VERSION,'settings':settings,'tenant_ids':[],'source_epoch':str(uuid.uuid4()),'secrets':values,'pin':None}
        write(path,data)
    make_pki(state)
    return render(state)

def catalog(settings,tenants):
    if not tenants:return {'version':'v1','backends':[]},{'version':'v1','backends':[]}
    entries=[];targets=[]
    def add(id,label,kind,role,adapter,isolation,target,enabled=True,capacity=1048576):
        entries.append({'id':id,'revision':1,'label':label,'kind':kind,'roles':[role],'enabled':enabled,'tenant_ids':tenants})
        if target is not None:targets.append({'backend_id':id,'backend_revision':1,'kind':kind,'adapter':adapter,'isolation':isolation,'limits':{'timeout_ms':5000,'max_concurrency':4,'max_bytes':capacity},kind:target})
    add('memory-pg','PostgreSQL Memory','postgresql','memory','managed-postgres-v1','tenant-subject-agent-v1',{'host':'postgres','port':5432,'database':settings['database'],'username':'memory_runtime','sslmode':'disable'})
    add('memory-redis','Redis Memory','redis','memory','managed-redis-v1','tenant-subject-agent-v1',{'host':'redis','port':6379,'database':0,'username':'memory_runtime','tls':False})
    add('session-redis','Redis Session and Summary','redis','session','managed-redis-v1','tenant-session-v1',{'host':'redis','port':6379,'database':0,'username':'session_runtime','tls':False},capacity=4194304)
    add('artifact-s3','S3 Artifacts','s3','artifact','managed-s3-v1','tenant-artifact-v1',{'endpoint':'http://minio:9000','bucket':'worker-artifacts','region':'us-east-1','path_style':True,'versioning':'disabled'},capacity=10485760)
    k=settings['knowledge'];enabled=k['dimensions'] is not None
    add('knowledge-qdrant','Qdrant Knowledge' if enabled else 'Qdrant Knowledge (embedding dimensions not configured)','qdrant','knowledge','managed-qdrant-v1','tenant-profile-resource-v1',{'endpoint':'http://qdrant:6333',**k} if enabled else None,enabled=enabled)
    return {'version':'v1','backends':entries},{'version':'v1','backends':targets}

def env_text(values):
    for value in values.values():
        if any(c in str(value) for c in ('\n','\r','\x00',"'",'$', '#')):raise ConfigError('environment value requires unsupported interpolation')
    return ''.join(k+'='+str(v)+'\n' for k,v in sorted(values.items()))

def render(state):
    state=Path(state).absolute();private_dir(state);data=read_json(state/'state.json');s=data['settings'];v=data['secrets']
    if data['version']!=VERSION:raise ConfigError('state version mismatch')
    private_dir(state/'redis')
    cat,targets=catalog(s,data['tenant_ids']);write(state/'managed/catalog.json',cat);write(state/'managed/targets.json',targets)
    sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
    catalog_sha,targets_sha=sha(state/'managed/catalog.json'),sha(state/'managed/targets.json')
    digest_env={'CONTROL_PLATFORM_BACKEND_CATALOG_SHA256':catalog_sha,'CONTROL_PLATFORM_BACKEND_TARGETS_SHA256':targets_sha}
    fingerprint=hashlib.sha256(env_text(digest_env).encode()).hexdigest();pinned=data.get('pin') or {};digest=pinned.get('digest','') if pinned.get('inputs_sha256')==fingerprint else ''
    secret_map={name:name for name in ['bootstrap_password','redis_admin','redis_memory','redis_session','minio_user','minio_password','minio_app_user','minio_app_password','qdrant_api_key']}
    secret_map.update(pg_session='session_runtime',pg_memory='memory_runtime')
    for file,key in secret_map.items():write(state/'secrets'/file,v[key]+'\n')
    # Reuse the existing deployment-only ACL generator; no Redis connection.
    command([sys.executable,ROOT/'deploy/compose/data-backends/backendctl.py','redis-acl','--admin-password-file',state/'secrets/redis_admin','--memory-password-file',state/'secrets/redis_memory','--session-password-file',state/'secrets/redis_session','--output',state/'redis/users.acl'])
    def copy(source,destination):write(state/destination,(state/'pki'/source).read_bytes())
    for directory,server in [('control-runtime','control-server'),('control-channel','control-server'),('worker-runtime','worker-server')]:
        copy('ca.pem',directory+'/ca.pem');copy(server+'.pem',directory+'/server.pem');copy(server+'.key',directory+'/server-key.pem')
    for directory,client in [('control-runtime','control-client'),('worker-runtime','worker-client'),('gateway-control','gateway-client'),('gateway-worker','gateway-client')]:
        copy('ca.pem',directory+'/ca.pem');copy(client+'.pem',directory+'/client.pem');copy(client+'.key',directory+'/client-key.pem')
    for filename,source in [('ca.pem','ca.pem'),('server.pem','nats-server.pem'),('server-key.pem','nats-server.key')]:copy(source,'nats-tls/'+filename)
    for directory,user in [('control-runtime','control'),('control-channel','control'),('worker-runtime','worker')]:
        write(state/directory/'nats.json',{'url':'tls://nats:4222','user':user,'password':v['nats_'+user],'ca_file':'/run/nats-tls/ca.pem'})
    rt='/run/control-runtime';ch='/run/control-channel';wr='/run/worker-runtime'
    write(state/'control-runtime/config.json',{'internal_address':':8082','tls_cert_file':rt+'/server.pem','tls_key_file':rt+'/server-key.pem','client_ca_file':rt+'/ca.pem','execution_url':'https://agent-worker:8082','execution_ca_file':rt+'/ca.pem','execution_cert_file':rt+'/client.pem','execution_key_file':rt+'/client-key.pem','manifest_nats_file':rt+'/nats.json','workers':[{'principal_uri':CLIENT_IDENTITIES['worker-client'],'worker_id':s['worker_id']}]})
    write(state/'control-channel/keyring.json',{'active_key_id':'managed-v1','keys':{'managed-v1':{'encryption_key':v['channel_encryption'],'mac_key':v['channel_mac']}}})
    write(state/'control-channel/config.json',{'route_nats_file':ch+'/nats.json','scope_id':s['scope_id'],'source_epoch':data['source_epoch'],'internal_address':':8081','tls_cert_file':ch+'/server.pem','tls_key_file':ch+'/server-key.pem','client_ca_file':ch+'/ca.pem','credential_keys_file':ch+'/keyring.json','max_tenant_accounts':100,'workloads':[{'principal_id':CLIENT_IDENTITIES['gateway-client'],'instance_id':s['gateway_instance_id'],'scope_id':s['scope_id'],'audience':'control-channel-v1','consumers':['telegram_registration','telegram_receiver','telegram_webhook','telegram_delivery','telegram_preflight','wecom_connection','wecom_preflight']}]})
    worker=read_json(ROOT/'services/agent-worker/internal/bootstrap/example.json')
    worker.update(worker_id=s['worker_id'],platform_contract_digest=digest,health_address=':8083',internal_address=':8082',control_url='https://control-api:8082',control_tls={'cert_file':wr+'/client.pem','key_file':wr+'/client-key.pem','ca_file':wr+'/ca.pem'},proof_tls={'cert_file':wr+'/server.pem','key_file':wr+'/server-key.pem','client_ca_file':wr+'/ca.pem'},control_principals=[CLIENT_IDENTITIES['control-client']],gateway_principals=[CLIENT_IDENTITIES['gateway-client']],nats_file=wr+'/nats.json')
    write(state/'worker-runtime/config.json',worker)
    def dsn(role):return 'postgres://'+role+':'+quote(v[role],safe='')+'@postgres:5432/'+s['database']+'?sslmode=disable'
    ports=s['ports']
    env={**digest_env,'COMPOSE_PROJECT_NAME':s['project'],'MANAGED_STATE_DIR':str(state),'MANAGED_RUNTIME_UID':s['runtime_uid'],'MANAGED_RUNTIME_GID':s['runtime_gid'],'WEB_HTTP_PORT':ports['web'],'CONTROL_API_HTTP_PORT':ports['control'],'GATEWAY_HTTP_PORT':ports['gateway'],'GATEWAY_ADMIN_PORT':ports['gateway_admin'],'WORKER_HTTP_PORT':ports['worker'],'WORKER_HEALTH_PORT':ports['worker'],'WORKER_ADMIN_PORT':ports['worker'],'PLATFORM_POSTGRES_DB':s['database'],'PLATFORM_POSTGRES_USER':'platform_admin','PLATFORM_POSTGRES_PASSWORD':v['pg_admin'],'CONTROL_PROFILE_CREDENTIAL_KEY':v['profile_key'],'CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST':digest,'CONTROL_PLATFORM_BACKEND_CATALOG_FILE':'/run/managed/catalog.json','CONTROL_PLATFORM_BACKEND_TARGETS_FILE':'/run/managed/targets.json','CONTROL_RUNTIME_CONFIG_DIR':str(state/'control-runtime'),'CONTROL_CHANNEL_CONFIG_DIR':str(state/'control-channel'),'WORKER_RUNTIME_CONFIG_DIR':str(state/'worker-runtime'),'GATEWAY_CONTROL_CERTS_DIR':str(state/'gateway-control'),'GATEWAY_WORKER_CERTS_DIR':str(state/'gateway-worker'),'NATS_TLS_DIR':str(state/'nats-tls'),'CONTROL_SESSION_COOKIE_SECURE':'false','CONTROL_SESSION_COOKIE_NAME':'control_'+s['project'].replace('-','_'),'CONTROL_BOOTSTRAP_MODE':'auto','CONTROL_BOOTSTRAP_USERNAME':s['bootstrap_username'],'CONTROL_BOOTSTRAP_DISPLAY_NAME':'Managed Local Operator','CONTROL_BOOTSTRAP_PASSWORD':v['bootstrap_password'],'CONTROL_SESSION_LIFETIME':'24h','GATEWAY_ACCOUNT_SOURCE':'control','GATEWAY_INSTANCE_ID':s['gateway_instance_id'],'GATEWAY_CONTROL_URL':'https://control-api:8081','GATEWAY_CONTROL_SCOPE_ID':s['scope_id'],'GATEWAY_CONTROL_SOURCE_EPOCH':data['source_epoch'],'GATEWAY_PUBLIC_ORIGIN':'','GATEWAY_WORKER_URL':'https://agent-worker:8082','GATEWAY_NATS_URL':'tls://nats:4222','GATEWAY_NATS_CA_FILE':'/run/nats-tls/ca.pem','GATEWAY_WORKER_CA_FILE':'/run/gateway-worker/ca.pem','GATEWAY_WORKER_CERT_FILE':'/run/gateway-worker/client.pem','GATEWAY_WORKER_KEY_FILE':'/run/gateway-worker/client-key.pem','CONTROL_API_BASE':'http://control-api:8080','RUNTIME_API_BASE':'http://channel-gateway:8090','WORKER_STOP_GRACE_PERIOD':'60s','MINIO_ROOT_USER_FILE':'/run/secrets/minio_user','MINIO_ROOT_PASSWORD_FILE':'/run/secrets/minio_password','MANAGED_S3_BUCKET':'worker-artifacts','MANAGED_S3_REGION':'us-east-1','KNOWLEDGE_DIMENSIONS':s['knowledge']['dimensions'] or '','KNOWLEDGE_COLLECTION':s['knowledge']['collection'],'KNOWLEDGE_VECTOR_NAME':s['knowledge']['vector_name'],'KNOWLEDGE_DISTANCE':{'cosine':'Cosine','dot':'Dot','euclid':'Euclid'}[s['knowledge']['distance']]}
    for role in PG_ROLES:env[role.upper()+'_PASSWORD']=v[role]
    for name in ('control','gateway','worker','session','memory'):
        env[name.upper()+'_DATABASE_URL']=dsn(name+'_runtime');env[name.upper()+'_MIGRATION_DATABASE_URL']=dsn(name+'_migrator')
    for name in ('control','gateway','worker','reconciler'):env['NATS_'+name.upper()+'_PASSWORD']=v['nats_'+name]
    env.update(CONTROL_API_IMAGE='trpc-agent-service/control-api:managed-cfce361',CHANNEL_GATEWAY_IMAGE='trpc-agent-service/channel-gateway:managed-cfce361',AGENT_WORKER_IMAGE='trpc-agent-service/agent-worker:managed-cfce361',WEB_IMAGE='trpc-agent-service/web:managed-cfce361',BACKEND_TOOLS_IMAGE='trpc-agent-service/backend-tools:managed-local')
    write(state/'compose.env',env_text(env));write(state/'digest.env',env_text(digest_env));write(state/'contract.env',env_text(digest_env))
    paths={'compose_env':state/'compose.env','digest_env':state/'digest.env','contract_env':state/'contract.env','catalog':state/'managed/catalog.json','targets':state/'managed/targets.json','control_runtime':state/'control-runtime/config.json','control_channel':state/'control-channel/config.json','worker':state/'worker-runtime/config.json','secrets_dir':state/'secrets'}
    meta={'schema_version':VERSION,'project':s['project'],'state_dir':str(state),'status':'PINNED' if digest else 'UNPINNED','ports':ports,'urls':{k:'http://127.0.0.1:'+str(port) for k,port in ports.items()},'tenant_ids':data['tenant_ids'],'scope_id':s['scope_id'],'source_epoch':data['source_epoch'],'worker_id':s['worker_id'],'gateway_instance_id':s['gateway_instance_id'],'knowledge':{**s['knowledge'],'enabled':s['knowledge']['dimensions'] is not None},'catalog_sha256':catalog_sha,'targets_sha256':targets_sha,'platform_contract_digest':digest,'inputs_sha256':fingerprint,'files':{k:str(path) for k,path in paths.items()},'bootstrap':{'username':s['bootstrap_username'],'password_file':str(state/'secrets/bootstrap_password')},'runtime_uid':s['runtime_uid'],'runtime_gid':s['runtime_gid']}
    write(state/'metadata.json',meta)
    return meta

def grant(state,tenant_ids,knowledge=None):
    state=Path(state).absolute();data=read_json(state/'state.json')
    if not tenant_ids or any(not isinstance(t,str) or not IDENTIFIER.fullmatch(t) for t in tenant_ids):raise ConfigError('explicit valid tenant ID required')
    data['tenant_ids']=sorted(set(data['tenant_ids']+tenant_ids))
    if knowledge is not None:
        validate_knowledge(knowledge)
        old=data['settings']['knowledge']
        if old['dimensions'] is not None and old!=knowledge:
            raise ConfigError('enabled Knowledge backend revision is immutable; configuration was not changed')
        data['settings']['knowledge']=knowledge
    write(state/'state.json',data)
    return render(state)

def pin(state,digest):
    if not DIGEST.fullmatch(digest):raise ConfigError('Control release digest must be sha256 and 64 lowercase hex digits')
    state=Path(state).absolute();meta=render(state);data=read_json(state/'state.json')
    data['pin']={'digest':digest,'inputs_sha256':meta['inputs_sha256']};write(state/'state.json',data)
    return render(state)

def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='action',required=True)
    for name in ('init','grant','pin','status'):
        p=sub.add_parser(name);p.add_argument('--state-dir',type=Path,default=DEFAULT_STATE)
        if name=='init':
            p.add_argument('--project');p.add_argument('--tenant-id',action='append')
            for key in ('web','control','gateway','gateway-admin','worker'):p.add_argument('--'+key+'-port',type=int)
        if name in ('init','grant'):
            if name=='grant':p.add_argument('--tenant-id',action='append',required=True)
            p.add_argument('--knowledge-dimensions',type=int);p.add_argument('--knowledge-collection',default='worker_knowledge');p.add_argument('--knowledge-vector-name',default='published_dense');p.add_argument('--knowledge-distance',choices=('cosine','dot','euclid'),default='cosine')
        if name=='pin':p.add_argument('--digest',required=True)
    args=parser.parse_args()
    try:
        state=args.state_dir.absolute()
        knowledge={'dimensions':args.knowledge_dimensions,'collection':args.knowledge_collection,'vector_name':args.knowledge_vector_name,'distance':args.knowledge_distance} if args.action in ('init','grant') and args.knowledge_dimensions is not None else None
        if args.action=='init':
            existing=(state/'state.json').exists();overrides=any(getattr(args,k+'_port') is not None for k in ('web','control','gateway','gateway_admin','worker')) or args.project is not None or knowledge is not None
            settings=None if existing and not overrides else initial_settings(args.project or 'trpc-agent-managed-local',{k:getattr(args,k+'_port') for k in ('web','control','gateway','gateway_admin','worker') if getattr(args,k+'_port') is not None},knowledge)
            meta=initialize(state,settings)
            if args.tenant_id:meta=grant(state,args.tenant_id,knowledge)
        elif args.action=='grant':meta=grant(state,args.tenant_id,knowledge)
        elif args.action=='pin':meta=pin(state,args.digest)
        else:meta=read_json(state/'metadata.json')
        print('MANAGED_CONFIG='+meta['status']);print('MANAGED_METADATA='+str(state/'metadata.json'))
        return 0
    except (ConfigError,OSError,ValueError,KeyError):
        print('MANAGED_CONFIG=FAIL configuration validation or local generation failed',file=sys.stderr)
        return 1
if __name__=='__main__':sys.exit(main())
