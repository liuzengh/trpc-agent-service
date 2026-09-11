#!/usr/bin/env python3
"""Pure generator and actual local OpenSSL tests; no Docker or services."""
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest

SPEC=importlib.util.spec_from_file_location('managed_config',Path(__file__).with_name('compose-managed-config.py'))
config=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(config)

class ManagedConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.state=Path(cls.temp.name)/'state'
        cls.settings=config.initial_settings(project='unit-managed')
        config.initialize(cls.state,cls.settings)
    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()
    def clone(self):
        import shutil
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        state=Path(temp.name)/'state';shutil.copytree(self.state,state)
        return state
    def test_empty_catalog_no_invented_embedding_or_tenant(self):
        s=self.clone();m=config.render(s)
        self.assertEqual(config.read_json(s/'managed/catalog.json')['backends'],[])
        self.assertEqual(config.read_json(s/'managed/targets.json')['backends'],[])
        self.assertEqual(m['status'],'UNPINNED');self.assertIsNone(m['knowledge']['dimensions'])
        self.assertEqual(m['ports'],{'web':23000,'control':28080,'gateway':28090,'gateway_admin':28091,'worker':28083})
    def test_idempotence_preserves_every_secret_identity_and_tls(self):
        s=self.clone();config.render(s)
        before={p.relative_to(s):p.read_bytes() for p in s.rglob('*') if p.is_file()}
        config.initialize(s,self.settings)
        after={p.relative_to(s):p.read_bytes() for p in s.rglob('*') if p.is_file()}
        self.assertEqual(before,after)
    def test_grant_is_explicit_separate_targets_and_disabled_knowledge(self):
        s=self.clone();config.grant(s,['tenant-real']);m=config.read_json(s/'metadata.json')
        cat=config.read_json(s/'managed/catalog.json')['backends'];targets=config.read_json(s/'managed/targets.json')['backends']
        self.assertEqual(len(cat),5);self.assertEqual(len(targets),4)
        self.assertTrue(all(e['tenant_ids']==['tenant-real'] for e in cat))
        self.assertFalse(next(e for e in cat if e['kind']=='qdrant')['enabled'])
        redis=[e for e in targets if e['kind']=='redis']
        self.assertEqual({e['redis']['username'] for e in redis},{'session_runtime','memory_runtime'})
        self.assertEqual({e['redis']['host'] for e in redis},{'redis'})
        pg=next(e['postgresql'] for e in targets if e['kind']=='postgresql')
        self.assertEqual((pg['host'],pg['database'],pg['username']),('postgres','agent_platform','memory_runtime'))
        self.assertEqual(m['status'],'UNPINNED')
    def test_pin_matches_both_configs_and_grant_invalidates_pin(self):
        s=self.clone();digest='sha256:'+'a'*64;config.pin(s,digest)
        self.assertEqual(config.read_json(s/'worker-runtime/config.json')['platform_contract_digest'],digest)
        self.assertIn('CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST='+digest,(s/'compose.env').read_text())
        self.assertEqual(config.render(s)['status'],'PINNED')
        config.grant(s,['tenant-other']);self.assertEqual(config.read_json(s/'metadata.json')['status'],'UNPINNED')
        self.assertEqual(config.read_json(s/'worker-runtime/config.json')['platform_contract_digest'],'')
        with self.assertRaises(config.ConfigError):config.pin(s,'bad')
    def test_known_dimensions_only_when_explicit(self):
        s=self.clone();config.grant(s,['tenant-real'],knowledge={'dimensions':768,'collection':'actual_docs','vector_name':'dense','distance':'cosine'})
        target=next(e for e in config.read_json(s/'managed/targets.json')['backends'] if e['kind']=='qdrant')
        self.assertEqual(target['qdrant'],{'endpoint':'http://qdrant:6333','collection':'actual_docs','vector_name':'dense','dimensions':768,'distance':'cosine'})
        for distance,expected in [('cosine','Cosine'),('dot','Dot'),('euclid','Euclid')]:
            s=self.clone()
            config.grant(s,['tenant-real'],knowledge={'dimensions':768,'collection':'actual_docs','vector_name':'dense','distance':distance})
            self.assertIn('KNOWLEDGE_DISTANCE='+expected,(s/'compose.env').read_text())
        for bad in [0,-1,65537,True]:
            with self.assertRaises(config.ConfigError):config.validate_knowledge({'dimensions':bad,'collection':'x','vector_name':'dense','distance':'cosine'})
    def test_enabled_knowledge_revision_is_immutable_and_same_value_idempotent(self):
        s=self.clone();fixed={'dimensions':768,'collection':'actual_docs','vector_name':'dense','distance':'dot'}
        config.grant(s,['tenant-real'],knowledge=fixed)
        before={p.relative_to(s):p.read_bytes() for p in s.rglob('*') if p.is_file()}
        config.grant(s,['tenant-real'],knowledge=dict(fixed))
        self.assertEqual(before,{p.relative_to(s):p.read_bytes() for p in s.rglob('*') if p.is_file()})
        for field,value in [('dimensions',1536),('collection','other'),('vector_name','other'),('distance','cosine')]:
            with self.subTest(field=field),self.assertRaises(config.ConfigError):
                config.grant(s,['new-tenant'],knowledge={**fixed,field:value})
            self.assertEqual(before,{p.relative_to(s):p.read_bytes() for p in s.rglob('*') if p.is_file()})
    def test_pki_actual_san_and_client_roles(self):
        for name,dns in [('control-server','control-api'),('worker-server','agent-worker'),('nats-server','nats')]:
            out=subprocess.run(['openssl','x509','-in',str(self.state/'pki'/f'{name}.pem'),'-noout','-text'],capture_output=True,text=True,check=True).stdout
            self.assertIn('DNS:'+dns,out);self.assertIn('TLS Web Server Authentication',out)
        for name,uri in config.CLIENT_IDENTITIES.items():
            out=subprocess.run(['openssl','x509','-in',str(self.state/'pki'/f'{name}.pem'),'-noout','-text'],capture_output=True,text=True,check=True).stdout
            self.assertIn('URI:'+uri,out);self.assertIn('TLS Web Client Authentication',out)
        for name in config.CLIENT_IDENTITIES:
            subprocess.run(['openssl','verify','-CAfile',str(self.state/'pki/ca.pem'),str(self.state/'pki'/f'{name}.pem')],capture_output=True,check=True)
    def test_secret_permissions_and_no_stdout_secret(self):
        s=self.clone();result=config.render(s);secret_values=config.read_json(s/'state.json')['secrets']
        for path in s.rglob('*'):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode),0o700 if path.is_dir() else 0o600,str(path))
        self.assertFalse(any(value in json.dumps(result) for value in secret_values.values()))
        self.assertEqual(len(set(secret_values.values())),len(secret_values))
        env=(s/'compose.env').read_text();self.assertIn('MANAGED_RUNTIME_UID='+str(os.getuid()),env)
        self.assertIn('CONTROL_API_BASE=http://control-api:8080',env)
        self.assertIn('CONTROL_PLATFORM_BACKEND_CATALOG_FILE=/run/managed/catalog.json',env)
        self.assertIn('CONTROL_PLATFORM_BACKEND_TARGETS_FILE=/run/managed/targets.json',env)
        self.assertTrue((s/'secrets/minio_app_password').is_file())
        self.assertEqual((s/'contract.env').read_bytes(),(s/'digest.env').read_bytes())
        self.assertIn('AGENT_WORKER_IMAGE=trpc-agent-service/agent-worker:managed-cfce361',env)
    def test_unsafe_state_symlink_and_mismatched_settings_rejected(self):
        s=self.clone();(s/'metadata.json').unlink();(s/'metadata.json').symlink_to(s/'state.json')
        with self.assertRaises(config.ConfigError):config.render(s)
        s=self.clone()
        with self.assertRaises(config.ConfigError):config.initialize(s,config.initial_settings(project='another'))
        with self.assertRaises(config.ConfigError):config.grant(s,['bad/tenant'])
        with self.assertRaises(config.ConfigError):config.initial_settings(project='bad\nproject')
    def test_redis_acl_uses_hashes_and_exact_scopes(self):
        s=self.clone();config.render(s);raw=(s/'redis/users.acl').read_text();secrets=config.read_json(s/'state.json')['secrets']
        self.assertIn('user deployment_admin on #',raw)
        self.assertIn('~runtime_memory:* +@connection +mget +get +type +pttl +eval +mset',raw)
        self.assertIn('~runtime_session:* +@connection +get +type +pttl +eval +set',raw)
        self.assertFalse(any(value in raw for value in secrets.values()))

if __name__=='__main__':unittest.main()
