import copy
import contextlib
import types
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock,patch
import urllib.error

spec=importlib.util.spec_from_file_location('compose_smoke',Path(__file__).resolve().parents[1]/'test-compose-managed-data.py')
f=importlib.util.module_from_spec(spec);spec.loader.exec_module(f)


class ManagedSmokeTests(unittest.TestCase):
    def test_catalog_is_exact_authorized_projection_not_target(self):
        entry={'id':'redis','revision':1,'label':'Redis','kind':'redis','roles':['session','memory'],'enabled':True,'tenant_ids':['a']}
        catalog={'backends':[entry]};view={'items':[{'id':'redis','revision':1,'label':'Redis','kind':'redis','roles':['memory','session'],'available':True}]}
        self.assertEqual(f.assert_catalog(view,catalog,'a'),view['items'])
        self.assertEqual(f.assert_catalog({'items':[]},catalog,'b'),[])
        for field,value in [('tenant_ids',['a']),('password','leak'),('host','redis')]:
            bad=copy.deepcopy(view);bad['items'][0][field]=value
            with self.subTest(field=field),self.assertRaises(AssertionError):f.assert_catalog(bad,catalog,'a')
        with self.assertRaises(AssertionError):f.assert_catalog(view,catalog,'b')

    def test_disabled_unconfigured_qdrant_stays_unavailable(self):
        catalog={'backends':[{'id':'q','revision':1,'label':'Unconfigured','kind':'qdrant','roles':['knowledge'],'enabled':False,'tenant_ids':['a']}]}
        view={'items':[{'id':'q','revision':1,'label':'Unconfigured','kind':'qdrant','roles':['knowledge'],'available':False}]}
        f.assert_catalog(view,catalog,'a')
        view['items'][0]['available']=True
        with self.assertRaises(AssertionError):f.assert_catalog(view,catalog,'a')

    def test_profile_exact_slots_and_no_plaintext_or_internal_id(self):
        config={'models':{},'tools':{},'knowledge':{},'storage':{'memory':{'kind':'managed_memory','backend_id':'m','backend_revision':1}}}
        value={'config':config,'credential_protocol_version':'v1','credential_states':{'storage':{'memory':{'dsn_password':{'configured':True,'status':'active','credential_revision':1,'association_token':'binding-mac'}}}}}
        slots=[('storage','memory','dsn_password')]
        f.assert_profile_read(value,config,slots,['private-canary'])
        for mode in ('plaintext','id','missing','inactive','extra'):
            bad=copy.deepcopy(value)
            if mode=='plaintext':bad['password']='private-canary'
            elif mode=='id':bad['credential_id']='crd_private'
            elif mode=='missing':bad['credential_states']={}
            elif mode=='inactive':bad['credential_states']['storage']['memory']['dsn_password']['configured']=False
            else:bad['credential_states']['storage']['memory']['dsn_password']['value']='unexpected'
            with self.subTest(mode=mode),self.assertRaises(AssertionError):f.assert_profile_read(bad,config,slots,['private-canary'])

    @staticmethod
    def config():
        names=['postgres','nats','control-api','channel-gateway','agent-worker','web','redis','qdrant','minio','backend-tools','database-schemas','session-prepare','nats-reconcile','backend-init','memory-schema','memory-prepare']
        value={'name':'trpc-agent-managed-local','services':{name:{} for name in names},'volumes':{},'networks':{'default':{}}}
        for name in ('postgres','redis','qdrant','minio'):
            value['services'][name]['volumes']=[{'type':'volume','source':name,'target':'/data'}]
            value['volumes'][name]={'name':'trpc-agent-managed-local_'+name}
        value['services']['web']['ports']=[{'host_ip':'127.0.0.1','published':'23000','target':3000}]
        return value

    def test_complete_stack_dedicated_network_volumes_and_loopback(self):
        value=self.config();f.validate_config(value,value['name'])
        for mode in ('backend-port','external-volume','foreign-volume','container-name','public-web','missing'):
            bad=copy.deepcopy(value)
            if mode=='backend-port':bad['services']['redis']['ports']=[{'host_ip':'127.0.0.1','target':6379}]
            elif mode=='external-volume':bad['volumes']['redis']['external']=True
            elif mode=='foreign-volume':bad['volumes']['redis']['name']='channel-lab-dev-data'
            elif mode=='container-name':bad['services']['redis']['container_name']='trpc-agent-latest'
            elif mode=='public-web':bad['services']['web']['ports'][0]['host_ip']='0.0.0.0'
            else:del bad['services']['agent-worker']
            with self.subTest(mode=mode),self.assertRaises(AssertionError):f.validate_config(bad,value['name'])

    def test_container_health_jobs_exit_and_project_identity(self):
        obj={'Id':'abc','Config':{'Labels':{'com.docker.compose.project':'project','com.docker.compose.service':'redis'}},'State':{'Status':'running','Running':True,'ExitCode':0,'StartedAt':'time1','Health':{'Status':'healthy'}}}
        f.validate_containers([obj],'project',['redis'])
        for mode in ('unhealthy','project','missing','duplicate','exited'):
            bad=copy.deepcopy(obj);items=[bad]
            if mode=='unhealthy':bad['State']['Health']['Status']='starting'
            elif mode=='project':bad['Config']['Labels']['com.docker.compose.project']='channel-lab-dev'
            elif mode=='missing':items=[]
            elif mode=='duplicate':items.append(copy.deepcopy(bad))
            else:bad['State'].update(Status='exited',Running=False,ExitCode=1)
            with self.subTest(mode=mode),self.assertRaises(AssertionError):f.validate_containers(items,'project',['redis'])
        job=copy.deepcopy(obj);job['Config']['Labels']['com.docker.compose.service']='init';job['State'].update(Status='exited',Running=False,ExitCode=0)
        f.validate_containers([obj,job],'project',['redis'])

    def test_managed_initialization_jobs_required_and_completed(self):
        value=self.config()
        for name in ('backend-init','memory-schema','memory-prepare'):
            bad=copy.deepcopy(value);del bad['services'][name]
            with self.subTest(name=name),self.assertRaises(AssertionError):f.validate_config(bad,value['name'])
        obj={'Id':'abc','Config':{'Labels':{'com.docker.compose.project':'project','com.docker.compose.service':'memory-prepare'}},'State':{'Status':'exited','Running':False,'ExitCode':0,'StartedAt':'time1'}}
        f.validate_containers([obj],'project',[],['memory-prepare'])
        for mode in ('absent','running','failed'):
            bad=copy.deepcopy(obj)
            if mode=='running':bad['State'].update(Status='running',Running=True)
            if mode=='failed':bad['State']['ExitCode']=1
            with self.subTest(mode=mode),self.assertRaises(AssertionError):f.validate_containers([] if mode=='absent' else [bad],'project',[],['memory-prepare'])

    @staticmethod
    def postgres_responses():
        schemas={k:v+'_migrator' for k,v in [('control','control'),('gateway','gateway'),('worker','worker'),('runtime_session','session'),('runtime_memory','memory')]}
        roles=[dict(name=role+'_'+kind,super=False,createdb=False,createrole=False,replication=False,bypassrls=False,memberships=0,owned_databases=0) for role in ('control','gateway','worker','session','memory') for kind in ('migrator','runtime')]
        tables={name:{'owner':'memory_migrator','privileges':privileges} for name,privileges in [('memory_heads',['INSERT','SELECT','UPDATE']),('memory_receipts',['INSERT','SELECT']),('memory_schema_migrations',['SELECT'])]}
        return [schemas,roles,0,tables]

    def test_postgres_five_schemas_ten_roles_and_prepared_memory_permissions(self):
        def check(values):
            h=f.Smoke.__new__(f.Smoke);h.report={};h.pg=Mock(side_effect=[json.dumps(x) for x in values]);h.pg_check();return h
        h=check(self.postgres_responses())
        self.assertEqual(h.report['postgres']['least_privileged_roles'],10)
        self.assertEqual(len(h.report['postgres']['schemas']),5)
        self.assertEqual(h.report['postgres']['runtime_cross_schema_checks'],20)
        self.assertEqual(set(h.report['postgres']['memory_tables']),{'memory_heads','memory_receipts','memory_schema_migrations'})
        for mode in ('missing-schema','missing-role','role-membership','role-db-owner','cross-schema','missing-table','wrong-table-owner','receipt-update','migration-write'):
            bad=self.postgres_responses()
            if mode=='missing-schema':del bad[0]['runtime_memory']
            elif mode=='missing-role':bad[1].pop()
            elif mode=='role-membership':bad[1][-1]['memberships']=1
            elif mode=='role-db-owner':bad[1][-1]['owned_databases']=1
            elif mode=='cross-schema':bad[2]=1
            elif mode=='missing-table':del bad[3]['memory_receipts']
            elif mode=='wrong-table-owner':bad[3]['memory_heads']['owner']='memory_runtime'
            elif mode=='receipt-update':bad[3]['memory_receipts']['privileges'].append('UPDATE')
            else:bad[3]['memory_schema_migrations']['privileges'].append('INSERT')
            with self.subTest(mode=mode),self.assertRaises(AssertionError):check(bad)

    def test_web_http_error_closes_response(self):
        h=f.Smoke.__new__(f.Smoke);h.report={};h.project='project';h.meta={'urls':{'web':'http://127.0.0.1:23000'}}
        h.compose=Mock(side_effect=['{}','cid']);h.command=Mock(return_value='[]')
        body=io.BytesIO(b'private-body');error=urllib.error.HTTPError('http://127.0.0.1:23000/login',503,'failed',{},body)
        with patch.object(f,'validate_config',return_value=[]),patch.object(f,'validate_containers',return_value={}),patch.object(f.urllib.request,'urlopen',side_effect=error):
            with self.assertRaises(f.Failure):h.inspect()
        self.assertTrue(body.closed)

    def test_private_state_reopens_and_refuses_world_readable(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'state.json';f.save_private(p,{'credential':'private'})
            self.assertEqual(p.stat().st_mode&0o777,0o600)
            self.assertEqual(f.private_json(p),{'credential':'private'})
            p.chmod(0o644)
            with self.assertRaises(f.Failure):f.private_json(p)

    def test_http_error_response_closed_body_not_in_error(self):
        http=f.HTTP('http://127.0.0.1:28080');body=io.BytesIO(b'private-password')
        error=urllib.error.HTTPError(http.base,503,'failed',{},body)
        http.opener=Mock();http.opener.open.side_effect=error
        with self.assertRaises(f.Failure) as raised:http.call('GET','/v1/me')
        self.assertTrue(body.closed);self.assertNotIn('private-password',str(raised.exception))
        self.assertEqual(http.observations,[{'method':'GET','path':'/v1/me','status':503}])

    def test_http_loopback_only(self):
        for base in ['https://remote.invalid','http://127.0.0.1@evil.invalid','http://localhost?token=x']:
            with self.subTest(base=base),self.assertRaises(f.Failure):f.HTTP(base)

    def test_acl_dryrun_denials_are_bulk_strings_not_resp_errors(self):
        calls=[]
        class BackendFailure(Exception):pass
        def redis(args,*cmd):
            calls.append((args.username,cmd))
            if cmd[:2]==('ACL','GETUSER'):
                user=cmd[2]
                return ['flags',['off' if user=='default' else 'on'],'keys','~runtime_'+user.removesuffix('_runtime')+':*']
            if cmd[:2]==('ACL','DRYRUN'):
                user,verb=cmd[2:4]
                if verb=='FLUSHALL':return "User "+user+" has no permissions to run the 'flushall' command"
                if cmd[4]=='deployment_smoke:outside':return "User "+user+" has no permissions to access the 'deployment_smoke:outside' key"
                return 'OK'
            if cmd==('PING',):return 'PONG'
            if cmd==('GET','deployment_smoke:outside'):raise BackendFailure('Redis command rejected')
            return [None] if cmd[0]=='MGET' else None
        module=types.ModuleType('backendctl');module.redis=redis;module.Failure=BackendFailure
        h=f.Smoke.__new__(f.Smoke);h.report={}
        def compose(*argv,**_):
            self.assertEqual(argv[:5],('exec','-T','backend-tools','python3','-c'))
            output=io.StringIO()
            with patch.dict('sys.modules',{'backendctl':module}),contextlib.redirect_stdout(output):exec(argv[5],{})
            return output.getvalue()
        h.compose=compose;h.acl_check()
        self.assertTrue(h.report['redis_acl']['default_off'])
        self.assertEqual(set(h.report['redis_acl']['users']),{'memory_runtime','session_runtime'})
        self.assertTrue(all(row['foreign_namespace_denied'] for row in h.report['redis_acl']['users'].values()))
        self.assertFalse(any(cmd[0] in ('SET','MSET','EVAL','FLUSHALL') for _,cmd in calls))
        original=module.redis
        def dependency(args,*cmd):
            if args.username=='memory_runtime' and cmd==('GET','deployment_smoke:outside'):raise BackendFailure('Redis dependency unavailable')
            return original(args,*cmd)
        module.redis=dependency
        with self.assertRaises(BackendFailure):h.acl_check()

    def test_backend_command_secrets_are_paths_and_no_restart(self):
        h=f.Smoke.__new__(f.Smoke);h.compose=Mock(return_value='DATA_BACKEND_SMOKE=PASS service=redis action=write\n')
        result=h.tool('smoke','--service','redis','--action','write','--probe-id','abc',*h.backend_args('redis'))
        self.assertIn('=PASS',result)
        self.assertEqual(h.compose.call_args.args[:5],('exec','-T','backend-tools','python3','/tooling/backendctl.py'))
        self.assertNotIn('restart',h.compose.call_args.args)
        self.assertIn('/run/secrets/redis_admin',h.compose.call_args.args)

    def test_compose_state_env_overrides_ambient_credentials_preserving_docker_and_path(self):
        with tempfile.TemporaryDirectory() as d:
            envfile=Path(d)/'compose.env';envfile.write_text('# generated plain values\nCONTROL_DATABASE_URL=postgres://owned/private\nUNQUOTED_VALUE=contains space\n');envfile.chmod(0o600)
            h=f.Smoke.__new__(f.Smoke);h.base=['docker','compose','--project-name','owned','--env-file',str(envfile)]
            with patch.dict(f.os.environ,{'CONTROL_DATABASE_URL':'postgres://old/foreign','PATH':'preserved-path','DOCKER_HOST':'unix:///owned.sock'},clear=True),patch.object(f.subprocess,'run',return_value=Mock(returncode=0,stdout='{}')) as run:
                self.assertEqual(h.compose('config','--format','json'),'{}')
                actual=run.call_args.kwargs['env']
                self.assertEqual(actual,{'CONTROL_DATABASE_URL':'postgres://owned/private','PATH':'preserved-path','DOCKER_HOST':'unix:///owned.sock','UNQUOTED_VALUE':'contains space'})
                self.assertNotIn('postgres://owned/private',repr(run.call_args.args))
                h.command(['docker','inspect','owned-container'])
                self.assertIsNone(run.call_args.kwargs['env'])

    def test_bootstrap_requires_real_password_rotation_and_stores_private_only(self):
        with tempfile.TemporaryDirectory() as d:
            h=f.Smoke.__new__(f.Smoke);h.state_path=Path(d)/'state.json';h.project='project';h.known_secrets=[];h.report={}
            password=Path(d)/'initial';password.write_text('original\n');password.chmod(0o600)
            h.meta={'bootstrap':{'username':'admin','password_file':str(password)}};h.api=Mock()
            h.api.call.side_effect=[{'user':{'id':'a'},'password_change_required':True},None,{'id':'owner'}, {'id':'tenant'}, {'id':'empty-tenant'},None]
            h.bootstrap()
            state=f.private_json(h.state_path)
            self.assertTrue(state['bootstrap_complete']);self.assertEqual(state['tenant_id'],'tenant')
            self.assertNotEqual(state['admin_password'],'original');self.assertEqual(password.read_text(),'original\n')
            self.assertEqual(h.report['result'],'PASS')
            paths=[call.args[1] for call in h.api.call.call_args_list]
            self.assertNotIn('/agents',' '.join(paths));self.assertNotIn('/channel-accounts',' '.join(paths))
            with self.assertRaises(f.Failure):h.bootstrap()


if __name__=='__main__':unittest.main()
