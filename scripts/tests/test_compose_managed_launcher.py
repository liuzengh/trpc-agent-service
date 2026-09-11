"""Launcher unit acceptance. Mock process boundaries; never contacts Docker."""
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock,patch
import urllib.error

SPEC=importlib.util.spec_from_file_location('managed_launcher',Path(__file__).resolve().parents[1]/'compose-managed.py')
f=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(f)

class LauncherTests(unittest.TestCase):
    def fixture(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);state=Path(temp.name)
        values={'COMPOSE_PROJECT_NAME':'unit-managed','CONTROL_API_IMAGE':'control:test','CHANNEL_GATEWAY_IMAGE':'gateway:test','AGENT_WORKER_IMAGE':'worker:test','WEB_IMAGE':'web:test','BACKEND_TOOLS_IMAGE':'tools:test','CONTROL_PLATFORM_BACKEND_CATALOG_SHA256':'a'*64,'CONTROL_PLATFORM_BACKEND_TARGETS_SHA256':'b'*64,'CONTROL_DATABASE_URL':'postgres://private-state-only','CONTROL_BOOTSTRAP_DISPLAY_NAME':'Managed Local Operator'}
        (state/'compose.env').write_text(''.join(k+'='+v+'\n' for k,v in values.items()))
        return state,values
    def main(self,args,runner=None):
        with patch.object(f.sys,'argv',['compose-managed.py',*args]),patch.object(f,'run',side_effect=runner) as run,patch.object(f,'ready') as ready,contextlib.redirect_stdout(io.StringIO()):f.main()
        return run,ready
    def fake_runner(self,events):
        def run(command,**kw):
            cmd=[str(v) for v in command];events.append((cmd,kw))
            return 'aarch64\n' if cmd[:2]==['docker','info'] else 'sha256:'+'c'*64+'\n' if '--print-deployment-contract-digest' in cmd else ''
        return run
    def test_all_compose_calls_name_complete_independent_project(self):
        state,values=self.fixture();cmd=[str(v) for v in f.compose(state,values)]
        self.assertEqual(cmd[:6],['docker','compose','--project-name','unit-managed','--env-file',str(state/'compose.env')])
        self.assertEqual([Path(cmd[i+1]).name for i,v in enumerate(cmd) if v=='-f'],['compose.yaml','compose.local.yaml','compose.worker-v1.yaml','compose.data-backends.yaml'])
        self.assertNotIn('--profile',cmd)
        for name in ['trpc-agent-latest','channel-lab-dev']:
            with self.subTest(name=name),self.assertRaises(RuntimeError):f.compose(state,{**values,'COMPOSE_PROJECT_NAME':name})
    def test_repeated_up_preserves_existing_secret_files_and_skips_init(self):
        state,_=self.fixture();secret=state/'retained-secret';secret.write_bytes(b'actual-stable-bytes');before=secret.read_bytes();events=[]
        for _ in range(2):self.main(['up','--state-dir',str(state),'--skip-build'],self.fake_runner(events))
        self.assertEqual(secret.read_bytes(),before)
        self.assertFalse(any('init' in cmd or 'build' in cmd for cmd,_ in events))
        self.assertEqual(sum('--print-deployment-contract-digest' in cmd for cmd,_ in events),2)
        ups=[cmd for cmd,_ in events if cmd[:2]==['docker','compose'] and 'up' in cmd]
        self.assertEqual(len(ups),2);self.assertTrue(all('--wait' in cmd and '--no-build' in cmd for cmd in ups))
        self.assertFalse(any(set(cmd)&{'down','--volumes','-v','prune'} for cmd,_ in events))
    def test_builds_exact_five_images_before_pin(self):
        state,_=self.fixture();events=[];self.main(['up','--state-dir',str(state)],self.fake_runner(events))
        builds=[cmd for cmd,_ in events if cmd[:2]==['docker','build']]
        self.assertEqual(len(builds),5)
        self.assertEqual({cmd[cmd.index('-t')+1] for cmd in builds},{'control:test','gateway:test','worker:test','web:test','tools:test'})
        self.assertIn('deploy/compose/Dockerfile.web-managed',builds[3])
        self.assertLess(max(i for i,(cmd,_) in enumerate(events) if cmd[:2]==['docker','build']),next(i for i,(cmd,_) in enumerate(events) if '--print-deployment-contract-digest' in cmd))
    def test_pin_uses_only_named_digest_environment_no_secrets_in_argv(self):
        state,values=self.fixture();events=[]
        with patch.object(f,'run',side_effect=self.fake_runner(events)):f.pin(state,values)
        cmd,kw=events[0]
        self.assertEqual(cmd[-2:],['control:test','--print-deployment-contract-digest'])
        self.assertEqual([cmd[i+1] for i,v in enumerate(cmd) if v=='-e'],['CONTROL_PLATFORM_BACKEND_CATALOG_SHA256','CONTROL_PLATFORM_BACKEND_TARGETS_SHA256'])
        self.assertNotIn(values['CONTROL_DATABASE_URL'],' '.join(cmd))
        self.assertEqual(kw['env']['CONTROL_PLATFORM_BACKEND_TARGETS_SHA256'],values['CONTROL_PLATFORM_BACKEND_TARGETS_SHA256'])
    def test_malformed_digest_stops_before_generator_pin(self):
        state,values=self.fixture()
        with patch.object(f,'run',return_value='private-failure-body') as run:
            with self.assertRaises(RuntimeError) as raised:f.pin(state,values)
        self.assertEqual(run.call_count,1);self.assertNotIn('private-failure-body',str(raised.exception))
    def test_explicit_grant_only_recreates_control_and_worker(self):
        state,_=self.fixture();events=[]
        self.main(['grant','--state-dir',str(state),'--tenant-id','actual-tenant'],self.fake_runner(events))
        grant=events[0][0];self.assertEqual(grant[-2:],['--tenant-id','actual-tenant'])
        restarts=[cmd for cmd,_ in events if '--force-recreate' in cmd]
        self.assertEqual(len(restarts),1);self.assertEqual(restarts[0][-2:],['control-api','agent-worker']);self.assertIn('--no-deps',restarts[0])
        with self.assertRaises(RuntimeError):self.main(['grant','--state-dir',str(state)],self.fake_runner([]))
    def test_state_env_wins_over_exported_other_project_database(self):
        state,values=self.fixture();result=subprocess.CompletedProcess(['docker'],0,'','')
        with patch.dict(os.environ,{'CONTROL_DATABASE_URL':'postgres://old-project-secret','DOCKER_HOST':'unix:///private-docker.sock'}),patch.object(f.subprocess,'run',return_value=result) as child:
            f.run(f.compose(state,values)+['config','--quiet'])
        kw=child.call_args.kwargs
        self.assertEqual(kw['env']['CONTROL_DATABASE_URL'],values['CONTROL_DATABASE_URL'])
        self.assertEqual(kw['env']['DOCKER_HOST'],'unix:///private-docker.sock')
    def test_generated_unquoted_environment_preserves_space(self):
        state,values=self.fixture();self.assertEqual(f.read_env(state),values)
    def test_failed_config_never_starts_services(self):
        state,_=self.fixture();events=[]
        def run(cmd,**kw):
            text=[str(v) for v in cmd];events.append(text)
            if text[-2:]==['config','--quiet']:raise RuntimeError('config failed')
            return 'sha256:'+'c'*64+'\n' if '--print-deployment-contract-digest' in text else ''
        with self.assertRaises(RuntimeError):self.main(['up','--state-dir',str(state),'--skip-build'],run)
        self.assertFalse(any(cmd[:2]==['docker','compose'] and 'up' in cmd for cmd in events))
    def test_captured_child_failure_does_not_echo_expanded_secret(self):
        result=subprocess.CompletedProcess(['docker'],1,'private-password','private-dsn')
        with patch.object(f.subprocess,'run',return_value=result):
            with self.assertRaises(RuntimeError) as raised:f.run(['docker','config'],capture=True)
        self.assertEqual(str(raised.exception),'docker failed exit=1')
    def test_readiness_accepts_actual_204_and_closes_success(self):
        responses=[]
        def opened(url,timeout):
            value=Mock();value.status=200 if url.endswith(':23000/') else 204
            cm=Mock();cm.__enter__=Mock(return_value=value);cm.__exit__=Mock(return_value=False);responses.append(cm);return cm
        opener=Mock();opener.open.side_effect=opened
        with patch.object(f.urllib.request,'build_opener',return_value=opener),patch.object(f.time,'sleep') as sleep,contextlib.redirect_stdout(io.StringIO()):f.ready({})
        self.assertEqual(len(responses),4);self.assertTrue(all(r.__exit__.call_count==1 for r in responses));sleep.assert_not_called()
    def test_failed_readiness_closes_http_error_responses(self):
        bodies=[]
        def opened(url,timeout):
            body=io.BytesIO(b'private upstream failure');bodies.append(body)
            raise urllib.error.HTTPError(url,503,'unavailable',{},body)
        opener=Mock();opener.open.side_effect=opened
        with patch.object(f.urllib.request,'build_opener',return_value=opener),patch.object(f.time,'monotonic',side_effect=[0,0,999]),patch.object(f.time,'sleep'):
            with self.assertRaises(RuntimeError) as raised:f.ready({})
        self.assertEqual(len(bodies),4);self.assertTrue(all(b.closed for b in bodies));self.assertNotIn('private',str(raised.exception))

if __name__=='__main__':unittest.main()
