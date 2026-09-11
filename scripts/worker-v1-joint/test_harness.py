"""Focused orchestration contract tests; process integration is a separate gate."""
import json
import io
import importlib.util
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock
from urllib.error import HTTPError
from urllib.request import Request,urlopen
from harness import Harness,ModelFixture
import harness


class HarnessTests(unittest.TestCase):
    def test_model_name_default_and_explicit_selection_remain_fixture_options(self):
        with tempfile.TemporaryDirectory() as evidence:
            for options, expected in (({},'joint-fixture'),({'model_name':'gpt-4o'},'gpt-4o')):
                h=Harness(Path(__file__).resolve().parents[2],evidence,**options)
                try:self.assertEqual(h.model_name,expected)
                finally:h.close()

    def test_extra_tenant_count_rejects_invalid_input_before_any_api_call(self):
        h=object.__new__(Harness)
        h.api=mock.Mock()
        for value in (-1, True, 1.5, None, '1'):
            with self.subTest(value=value),self.assertRaises(ValueError):h.seed(extra_tenants=value)
        h.api.assert_not_called()

    def test_session_proxy_closes_after_service_processes(self):
        with tempfile.TemporaryDirectory() as evidence:
            h=Harness(Path(__file__).resolve().parents[2],evidence)
            order=[]
            process=mock.Mock(pid=1,returncode=0)
            process.poll.return_value=None
            process.wait.side_effect=lambda **kwargs:order.append('worker_stopped')
            h.processes=[('fixture-worker',process)]
            h.session_fault_proxy=SimpleNamespace(close=lambda:order.append('session_proxy_closed'),evidence=lambda:{'closed':True,'live_connections':0})
            h.close()
            self.assertEqual(order,['worker_stopped','session_proxy_closed'])

    def test_resolve_relays_close_after_services_before_proof_switch(self):
        with tempfile.TemporaryDirectory() as evidence:
            h=Harness(Path(__file__).resolve().parents[2],evidence)
            order=[]
            process=mock.Mock(pid=1,returncode=0)
            process.poll.return_value=None
            process.wait.side_effect=lambda **kwargs:order.append('worker_stopped')
            h.processes=[('fixture-worker',process)]
            proxy=lambda name:SimpleNamespace(close=lambda:order.append(name),evidence=lambda:{'closed':True,'live_connections':0,'name':name})
            h.resolve_proxies=[proxy('control_relay'),proxy('proof_relay')]
            h.proof_switch=SimpleNamespace(close=lambda:order.append('proof_switch'))
            h.close()
            self.assertEqual(order,['worker_stopped','proof_relay','control_relay','proof_switch'])
            snapshots=json.loads((Path(evidence)/'resolve-proxies-cleanup.json').read_text())
            self.assertEqual([s['name'] for s in snapshots],['proof_relay','control_relay'])

    def test_extra_tenants_share_owner_and_are_created_before_owner_login(self):
        with tempfile.TemporaryDirectory() as evidence:
            for count,model_name in ((0,'joint-fixture'),(1,'gpt-4o')):
                h=object.__new__(Harness)
                h.model_name=model_name
                h.admin_password='fixture-admin'
                h.secret=lambda:'fixture-secret'
                h.artifacts=Path(evidence)
                h.session_port=23456
                h.pg_port=5432
                h.dsns={'session_runtime':'fixture-session-proxy'}
                h.model=SimpleNamespace(url='http://127.0.0.1:1',key='fixture-model-key')
                h.wait=lambda *args,**kwargs:None
                calls=[]
                def api(method,path,body,**kwargs):
                    calls.append((method,path,body))
                    if path=='/v1/admin/users':return {'id':'same-owner'}
                    if path=='/v1/admin/tenants':return {'id':body['slug']}
                    if path.endswith('/agents'):return {'agent':{'id':'agent'}}
                    if path.endswith('/runtime-profiles'):return {'profile':{'id':'profile'}}
                    if path.endswith('/deployments'):return {'deployment':{'id':'deployment'}}
                    if path.endswith('/validate'):return {'valid':True}
                    if path.endswith('/deployments/deployment/revisions'):
                        return {'revision':{'id':'revision','manifest_id':'manifest','manifest_digest':'digest'}}
                    return {}
                h.api=api
                h.seed(extra_tenants=count)
                tenant_calls=[(i,b) for i,(_,p,b) in enumerate(calls) if p=='/v1/admin/tenants']
                owner_login=next(i for i,(_,p,b) in enumerate(calls) if p=='/v1/auth/login' and b['username']=='joint-owner')
                self.assertEqual(len(tenant_calls),count+1)
                self.assertTrue(all(i<owner_login and b['owner_user_id']=='same-owner' for i,b in tenant_calls))
                self.assertEqual(h.extra_tenant_ids,['joint-extra-1'] if count else [])
                profile=next(b for _,p,b in calls if p.endswith('/runtime-profiles/profile/draft'))
                self.assertEqual(profile['config']['storage']['session']['destination']['port'],23456)
                self.assertEqual(profile['config']['models']['primary']['model'],model_name)
                self.assertEqual(profile['credentials']['storage']['session']['dsn']['value'],'fixture-session-proxy')

    def test_does_not_inherit_service_credentials_and_reaps_private_files(self):
        with tempfile.TemporaryDirectory() as evidence:
            with mock.patch.dict('os.environ', {'CONTROL_DATABASE_URL':'not-a-fixture','OPENAI_API_KEY':'not-a-fixture'}):
                h=Harness(Path(__file__).resolve().parents[2],evidence)
            self.assertNotIn('CONTROL_DATABASE_URL',h.env)
            self.assertNotIn('OPENAI_API_KEY',h.env)
            path=h.work
            h.write('private.json',{'fixture':h.secret()})
            self.assertEqual((path/'private.json').stat().st_mode & 0o777,0o600)
            with self.assertRaises(ValueError):h.sql('UPDATE product SET value=1')
            h.close()
            self.assertFalse(path.exists())
    def test_explicit_model_fixture_holds_records_and_releases(self):
        fixture=ModelFixture(None)
        try:
            fixture.hold('held question')
            request={'model':'fixture','messages':[{'role':'user','content':'held question'}],'stream':True}
            result=[]
            def run():
                with urlopen(Request(fixture.url+'/v1/chat/completions',data=json.dumps(request).encode(),headers={'Authorization':'Bearer '+fixture.key,'Content-Type':'application/json'}),timeout=5) as response:
                    result.append(response.read().decode())
            thread=threading.Thread(target=run);thread.start()
            fixture.wait_entered('held question',timeout=2)
            self.assertEqual(fixture.requests,[request]);self.assertEqual(result,[])
            fixture.release('held question');thread.join(3)
            self.assertFalse(thread.is_alive());self.assertIn('joint answer: held question',result[0])
        finally:fixture.close()

    def test_fixture_hold_timeout_is_explicit_and_not_a_product_deadline(self):
        fixture=ModelFixture(None)
        try:
            self.assertEqual(fixture.hold_timeout_seconds,90)
            fixture.hold_timeout_seconds=150
            self.assertEqual(fixture.hold_timeout_seconds,150)
            for value in (True,False,None,'150',0,-1,float('inf'),float('nan')):
                with self.subTest(value=value),self.assertRaises(ValueError):
                    fixture.hold_timeout_seconds=value
            self.assertEqual(fixture.hold_timeout_seconds,150)
        finally:fixture.close()


class ModelPublicationAssertionsTest(unittest.TestCase):
    def test_actual_sdk_parameters_must_equal_published_model_and_effective_output(self):
        for model, policy, node_max in [('joint-fixture',4096,None),('gpt-4o',20000,None),('gpt-4o',20000,18000)]:
            node={'kind':'llm','model_resource':'primary'}
            if node_max is not None:node['generation']={'max_output_tokens':node_max}
            view={'agent_plan':{'root':'assistant','nodes':{'assistant':node}},
                  'resources':{'models':{'primary':{'model':model}}},'execution':{'max_output_tokens':policy}}
            expected=node_max if node_max is not None else policy
            request={'model':model,'max_completion_tokens':expected,'messages':[]}
            result=harness.assert_model_contract([request,dict(request)],view,model)
            self.assertEqual(result['effective_max_output_tokens'],expected)
            self.assertEqual(result['model_name'],model)
            for field,value in [('model','silently-replaced'),('max_completion_tokens',16384),('max_tokens',expected)]:
                with self.subTest(model=model,node=node_max,field=field),self.assertRaises(AssertionError):
                    harness.assert_model_contract([dict(request,**{field:value})],view,model)

    def test_joint_cli_passes_default_or_explicit_model_without_starting_services(self):
        spec=importlib.util.spec_from_file_location('joint_runner_model_test',Path(__file__).resolve().parents[1]/'test-worker-v1-joint.py')
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        class StopBeforeProvision(Exception):pass
        with tempfile.TemporaryDirectory() as directory:
            for extra,expected in (([],'joint-fixture'),(['--model-name','gpt-4o'],'gpt-4o')):
                with mock.patch.object(runner,'Harness',side_effect=StopBeforeProvision) as factory, mock.patch('sys.argv',['joint','--artifacts',directory,*extra]), self.assertRaises(StopBeforeProvision):
                    runner.main()
                self.assertEqual(factory.call_args.kwargs,{'model_name':expected})

class Response(io.BytesIO):
    def __init__(self, body=b'{}', status=200, read_error=None):
        super().__init__(body)
        self.status=status
        self.read_error=read_error
    def read(self, *args):
        if self.read_error: raise self.read_error
        return super().read(*args)


def lifecycle_harness(artifacts):
    # No processes, sockets or fixture directories are allocated in these tests.
    h=object.__new__(Harness)
    h.root=Path(__file__).resolve().parents[2]
    h.artifacts=Path(artifacts)
    h.secrets=[]
    h.urls={key:'https://127.0.0.1:1' for key in ('control','control_runtime','worker')}
    h.dsns={key:'fixture-only' for key in ('control_runtime','control_migrator','worker_runtime','worker_migrator')}
    h.env={'NATS_CONTROL_PASSWORD':'fixture','NATS_WORKER_PASSWORD':'fixture','SESSION_RUNTIME_PASSWORD':'fixture'}
    h.ports={'control':1,'control_runtime':2,'worker':3}
    h.nats_url='tls://127.0.0.1:4'
    h.certs={key:'fixture-only' for key in ('ca','server_cert','server_key','control_cert','control_key','worker_cert','worker_key')}
    h.worker_ports={'worker-one':{'proof':5,'health':6}}
    h.workers={}
    h.proof_switch=mock.Mock()
    h.binaries={'control-api':'fixture-control','agent-worker':'fixture-worker'}
    h.contract_digest='sha256:'+'a'*64
    h.manifest_id='manifest'
    h.manifest_digest=h.contract_digest
    h.pg='fixture-only'
    h.write=mock.Mock(return_value='fixture-only.json')
    h.command=mock.Mock(return_value='runtime_session|session_runtime|t\n')
    h.spawn=mock.Mock(return_value=SimpleNamespace(poll=lambda:None))
    h.wait=lambda predicate,description,**kwargs:predicate()
    return h


class ResponseLifecycleTest(unittest.TestCase):
    def test_api_closes_success_error_parse_failure_and_read_failure(self):
        scenarios=[(Response(b'{"ok":true}'),False,None),
                   (Response(b'not json'),False,json.JSONDecodeError),
                   (Response(read_error=OSError('read failed')),False,OSError),
                   (Response(b'{"code":"denied"}',403),True,RuntimeError)]
        with tempfile.TemporaryDirectory() as directory:
            h=lifecycle_harness(directory)
            for response,is_http_error,expected_error in scenarios:
                with self.subTest(http_error=is_http_error,expected_error=expected_error):
                    error=HTTPError(h.urls['control'],403,'Forbidden',{},response) if is_http_error else None
                    h.opener=SimpleNamespace(open=mock.Mock(side_effect=error) if error else mock.Mock(return_value=response))
                    try:
                        if expected_error:
                            with self.assertRaises(expected_error):h.api('GET','/test')
                        else:self.assertEqual(h.api('GET','/test'),{'ok':True})
                        self.assertTrue(response.closed,'API response resource must close on every outcome')
                    finally:
                        if error:error.close()
                        response.close()

    def test_startup_health_checks_close_http_errors_and_success(self):
        with tempfile.TemporaryDirectory() as directory:
            for method in ('control_start','start_worker'):
                for status in (204,503):
                    with self.subTest(method=method,status=status):
                        h=lifecycle_harness(directory)
                        response=Response(b'',status)
                        error=HTTPError(h.urls['control'],status,'Unavailable',{},response) if status==503 else None
                        opener=mock.Mock(side_effect=error) if error else mock.Mock(return_value=response)
                        try:
                            with mock.patch('harness.urllib.request.urlopen',opener),mock.patch('faults.ProofSwitch'):
                                if error:
                                    with self.assertRaises(HTTPError):
                                        getattr(h,method)(*([{}] if method=='control_start' else []))
                                else:getattr(h,method)(*([{}] if method=='control_start' else []))
                            self.assertTrue(response.closed,'startup health response must be explicitly closed')
                        finally:
                            if error:error.close()
                            response.close()

    def test_dependency_preflight_closes_expected_403_responses(self):
        with tempfile.TemporaryDirectory() as directory:
            h=lifecycle_harness(directory)
            responses=[Response(b'{"code":"DENIED"}',403),Response(b'{"code":"DENIED"}',403)]
            errors=[HTTPError(h.urls['worker'],403,'Forbidden',{},response) for response in responses]
            try:
                with mock.patch('harness.ssl.create_default_context'),mock.patch('harness.urllib.request.urlopen',side_effect=errors):
                    h.verify_dependencies()
                self.assertTrue(all(response.closed for response in responses),'all expected-denial response bodies must close')
            finally:
                for error in errors:error.close()
                for response in responses:response.close()

    def test_dependency_preflight_closes_unexpected_and_bad_json_responses(self):
        with tempfile.TemporaryDirectory() as directory:
            for response,expected_error in ((Response(b'{}',200),RuntimeError),(Response(b'not json',403),json.JSONDecodeError)):
                h=lifecycle_harness(directory)
                try:
                    with mock.patch('harness.ssl.create_default_context'),mock.patch('harness.urllib.request.urlopen',return_value=response):
                        with self.assertRaises(expected_error):h.verify_dependencies()
                    self.assertTrue(response.closed,'preflight must close unexpected/error response before raising')
                finally:response.close()


if __name__=='__main__':unittest.main()
