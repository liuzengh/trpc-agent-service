import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('backendctl', Path(__file__).parents[1] / 'backendctl.py')
b = importlib.util.module_from_spec(spec); spec.loader.exec_module(b)

class BackendTests(unittest.TestCase):
    def test_acl_hash_namespace_and_atomic_idempotence(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);args=argparse.Namespace(output=str(root/'users.acl'))
            for role in ['admin','memory','session']:
                p=root/role;p.write_text(role+'-secret\n');setattr(args,role+'_password_file',str(p))
            with patch('sys.stdout',new=io.StringIO()):b.redis_acl(args)
            raw=Path(args.output).read_text()
            for role in ['admin','memory','session']:
                self.assertNotIn(role+'-secret',raw);self.assertIn('#'+hashlib.sha256((role+'-secret').encode()).hexdigest(),raw)
            self.assertIn('~runtime_memory:*',raw);self.assertIn('~runtime_session:*',raw)
            self.assertEqual(Path(args.output).stat().st_mode & 0o777,0o600)
            with patch('sys.stdout',new=io.StringIO()):b.redis_acl(args)
            self.assertEqual(Path(args.output).read_text(),raw)

    def test_qdrant_without_dimensions_never_calls_network(self):
        args=argparse.Namespace(dimensions=None)
        with patch.object(b,'qdrant') as network,patch('sys.stdout',new=io.StringIO()) as out:
            b.qdrant_init(args);network.assert_not_called();self.assertIn('SKIPPED',out.getvalue())

    def qargs(self):return argparse.Namespace(dimensions=3,collection='knowledge',vector_name='dense',distance='Cosine',wait_seconds=1)
    def config(self,dim=3):return json.dumps({'result':{'config':{'params':{'vectors':{'dense':{'size':dim,'distance':'Cosine'}}}}}}).encode()

    def test_qdrant_existing_conflict_no_write(self):
        with patch.object(b,'qdrant',side_effect=[(200,b''),(200,self.config(4))]) as q:
            with self.assertRaises(b.Failure):b.qdrant_init(self.qargs())
        self.assertEqual([x.args[1] for x in q.call_args_list],['GET','GET'])

    def test_qdrant_missing_created_named_vector(self):
        with patch.object(b,'qdrant',side_effect=[(200,b''),(404,b''),(200,b''),(200,self.config())]) as q,patch('sys.stdout',new=io.StringIO()):
            b.qdrant_init(self.qargs())
        self.assertEqual(q.call_args_list[2].args[3],{'vectors':{'dense':{'size':3,'distance':'Cosine'}}})

    def test_existing_bucket_not_overwritten(self):
        args=argparse.Namespace(wait_seconds=1,region='us-east-1')
        with patch.object(b,'s3',side_effect=[(200,b''),(200,b''),(200,b''),(200,b'<VersioningConfiguration/>')]) as s,patch('sys.stdout',new=io.StringIO()):b.minio_init(args)
        self.assertNotIn('PUT',[x.args[1] for x in s.call_args_list])

    def test_bucket_initializes_once_and_rejects_versioning(self):
        args=argparse.Namespace(wait_seconds=1,region='us-east-1')
        with patch.object(b,'s3',side_effect=[(404,b''),(404,b''),(200,b''),(200,b''),(200,b'<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>')]) as s:
            with self.assertRaises(b.Failure):b.minio_init(args)
        self.assertEqual([x.args[1] for x in s.call_args_list].count('PUT'),1)

    def test_http_error_body_never_read_or_returned(self):
        response=argparse.Namespace(status=403)
        class Response:
            status=403
            def __enter__(self):return self
            def __exit__(self,*_):pass
            def read(self,*_):raise AssertionError('must not read credential echo')
        opener=argparse.Namespace(open=lambda *a,**kw:Response())
        with patch.object(b.urllib.request,'build_opener',return_value=opener):self.assertEqual(b.http('http://example.test'),(403,b''))

    def test_endpoint_rejects_credentials_queries_and_redirects(self):
        for url in ['http://user:key@host','http://host?secret=x','ftp://host','http://host/path']:
            with self.assertRaises(b.Failure):b.endpoint(url)
        self.assertIsNone(b.NoRedirect().redirect_request(None,None,None,None,None,None))

    def test_readiness_handles_transient_network_then_ready(self):
        with patch('time.sleep'),patch.object(b.time,'monotonic',side_effect=[0,0,0,0]):
            values=iter([b.Failure('network'),True])
            def ready():
                value=next(values)
                if isinstance(value,Exception):raise value
                return value
            b.wait_ready(ready,1)

    def test_redis_smoke_rejects_collision_before_delete(self):
        args=argparse.Namespace(service='redis',probe_id='probe',action='delete')
        with patch.object(b,'redis',side_effect=[['appendonly','yes','appendfsync','always','maxmemory-policy','noeviction'],'foreign']) as r:
            with self.assertRaises(b.Failure):b.smoke(args)
        self.assertNotIn('DEL',[x.args[1] for x in r.call_args_list])

    def test_smoke_read_must_find_persisted_bytes(self):
        args=argparse.Namespace(service='minio',probe_id='probe',action='read')
        with patch.object(b,'s3',return_value=(200,b'wrong')):
            with self.assertRaises(b.Failure):b.smoke(args)

if __name__=='__main__':unittest.main()

class AccountTests(unittest.TestCase):
    def test_invalid_acl_owner_rejected_before_tempfile(self):
        args=argparse.Namespace(admin_password_file='a',memory_password_file='b',session_password_file='c',owner_uid=1,owner_gid=None,output='/unused')
        with patch.object(b,'secret',side_effect=['a','b','c']),patch.object(b.tempfile,'mkstemp') as create:
            with self.assertRaises(b.Failure):b.redis_acl(args)
            create.assert_not_called()

    def test_minio_account_admin_protocol_and_bucket_scope(self):
        args=argparse.Namespace(access_key_file='root',secret_key_file='rootpass',app_access_key_file='app',app_secret_key_file='apppass',bucket='worker-artifacts',endpoint='http://minio:9000',mc_binary='mc',timeout=3)
        with patch.object(b,'secret',side_effect=['root-user','root-password','app-user','app-password']),patch.object(b.subprocess,'run',return_value=argparse.Namespace(returncode=0)) as run,patch('sys.stdout',new=io.StringIO()) as out:
            b.minio_account_init(args)
        calls=[c.args[0][4:] for c in run.call_args_list]
        self.assertEqual(calls[1],['admin','user','add','owned','app-user','app-password'])
        self.assertEqual(calls[-1],['stat','application/worker-artifacts'])
        for call in run.call_args_list:
            self.assertEqual(call.kwargs['timeout'],3)
            self.assertEqual(call.kwargs['stderr'],b.subprocess.DEVNULL)
        self.assertNotIn('app-password',out.getvalue())
        policy=b.artifact_policy('worker-artifacts')
        self.assertEqual(policy['Statement'][1]['Resource'],['arn:aws:s3:::worker-artifacts/*'])
        self.assertNotIn('s3:*',json.dumps(policy))

    def test_minio_account_admin_failure_stops_before_app_success(self):
        args=argparse.Namespace(access_key_file='root',secret_key_file='rootpass',app_access_key_file='app',app_secret_key_file='apppass',bucket='worker-artifacts',endpoint='http://minio:9000',mc_binary='mc',timeout=3)
        with patch.object(b,'secret',side_effect=['root-user','root-password','app-user','app-password']),patch.object(b.subprocess,'run',return_value=argparse.Namespace(returncode=1)) as run:
            with self.assertRaises(b.Failure):b.minio_account_init(args)
        self.assertEqual(run.call_count,1)

class EntryTests(unittest.TestCase):
    def run_entry(self, entry, dimensions='', distance='Cosine'):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); log=root/'calls'
            python=root/'python3'
            python.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALL_LOG"\n')
            python.chmod(0o700)
            env=dict(b.os.environ,PATH=directory+':'+b.os.environ['PATH'],CALL_LOG=str(log),MANAGED_QDRANT_DIMENSIONS=dimensions,MANAGED_QDRANT_DISTANCE=distance)
            for key in ['MANAGED_S3_BUCKET','MANAGED_QDRANT_COLLECTION','MANAGED_QDRANT_VECTOR']:env.pop(key,None)
            result=b.subprocess.run(['sh',str(Path(__file__).parents[1]/entry)],env=env,capture_output=True)
            self.assertEqual(result.returncode,0)
            return log.read_text().splitlines()

    def test_init_omits_unconfigured_dimensions(self):
        calls=self.run_entry('init-all.sh')
        self.assertEqual(len(calls),3)
        self.assertIn('--bucket agent-artifacts',calls[0])
        self.assertIn('minio-account-init',calls[1])
        self.assertIn('--app-access-key-file /run/secrets/minio_app_user',calls[1])
        self.assertIn('--collection agent-knowledge --vector-name embedding',calls[2])
        self.assertNotIn('--dimensions',calls[2])

    def test_init_preserves_explicit_dimensions(self):
        self.assertIn('--dimensions 3072',self.run_entry('init-all.sh','3072')[2])

    def test_health_checks_all_services_in_order(self):
        calls=self.run_entry('health-all.sh')
        self.assertEqual(len(calls),3)
        for call,service in zip(calls,['redis','minio','qdrant']):
            self.assertIn('/tooling/backendctl.py health --service '+service,call)
            self.assertIn('--timeout 5 --wait-seconds 15',call)

    def test_init_preserves_noncosine_distance(self):
        for distance in ['Dot', 'Euclid']:
            self.assertIn('--distance '+distance, self.run_entry('init-all.sh','3072',distance)[2])
