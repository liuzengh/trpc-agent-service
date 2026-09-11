import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from live_trace_verify import external_evidence, ReadOnlyDatabase, verify_business, credential_values


class LiveEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.files = {
            'deployment.json': {'real_provider': 'https://api.example.test', 'real_telegram': True, 'model': 'test-model'},
            'provider-requests.json': [{'model': 'test-model', 'stream': True, 'messages': [{'role':'user','content':'unique input'}]}],
            'provider-responses.json': [{'http_status':200,'bytes':100,'sha256':'a'*64,'final_text':'final response','usage':{'prompt_tokens':10,'completion_tokens':3,'total_tokens':13}}],
            'telegram-ingress.json': [{'gateway_status':200,'raw_sha256':'b'*64,'update':{'update_id':1,'message':{'message_id':2,'text':'unique input'}}}],
        }

    def write(self):
        for name,value in self.files.items():
            (self.root/name).write_text(json.dumps(value))

    def test_correlates_unique_input_and_redacts_output(self):
        self.write()
        result, final, _ = external_evidence(self.root,'unique input')
        self.assertEqual(final,'final response')
        self.assertEqual(result['provider_call_count'],1)
        self.assertNotIn('unique input',json.dumps(result))
        self.assertNotIn('final response',json.dumps(result))

    def test_failed_or_incomplete_provider_is_not_live_success(self):
        for change in ('inflight','http_error','empty_final'):
            with self.subTest(change=change):
                original=json.loads(json.dumps(self.files))
                if change=='inflight':self.files['provider-responses.json']=[]
                elif change=='http_error':self.files['provider-responses.json'][0]['http_status']=503
                else:self.files['provider-responses.json'][0]['final_text']=''
                self.write()
                with self.assertRaises(AssertionError):external_evidence(self.root,'unique input')
                self.files=original

    def test_rejects_missing_or_ambiguous_external_input(self):
        self.write()
        with self.assertRaises(AssertionError):external_evidence(self.root,'other input')
        self.files['provider-requests.json'] *= 2
        self.files['provider-responses.json'] *= 2
        self.write()
        with self.assertRaises(AssertionError):external_evidence(self.root,'unique input')

    def test_rejects_fixture_origin_and_nonaccepted_ingress(self):
        self.files['deployment.json']['real_provider']='http://127.0.0.1:1234'
        self.write()
        with self.assertRaises(AssertionError):external_evidence(self.root,'unique input')
        self.files['deployment.json']['real_provider']='https://api.example.test'
        self.files['telegram-ingress.json'][0]['gateway_status']=401
        self.write()
        with self.assertRaises(AssertionError):external_evidence(self.root,'unique input')

    def test_credentials_are_read_without_shell_evaluation(self):
        path=self.root/'private.env'
        path.write_text("export API_KEY='sensitive-value'\nMODEL=model-name\nDEEPSEEKAPI=another-sensitive-value\n")
        self.assertEqual(credential_values(path),['sensitive-value','another-sensitive-value'])

    def test_database_writes_rejected_before_subprocess(self):
        db=ReadOnlyDatabase('explicit-container',self.root,[])
        with patch('live_trace_verify.subprocess.run') as run:
            for query in ['UPDATE worker.execution_runs SET status=\'SUCCEEDED\'','DELETE FROM x','WITH x AS (DELETE FROM x) SELECT 1']:
                with self.assertRaises(ValueError):db.sql(query)
            run.assert_not_called()

    def test_business_checks_formal_head_digest_and_delivery(self):
        body='final\nresponse\t测试'; sha='a'*64
        responses=[[['run','SUCCEEDED']],[['ref','sha256:'+sha,'ref','sha256:'+sha,'ATTEMPT','SUCCEEDED','FINAL',sha,'sha256:'+sha]],[['1','1','1']],[['ACCEPTED',body.encode().hex(),'ACCEPTED','123']]]
        db=ReadOnlyDatabase('explicit-container',self.root,[])
        with patch.object(db,'sql',side_effect=responses):
            run,result=verify_business(db,'unique input',body)
        self.assertEqual(run,'run');self.assertEqual(result['provider_message_ids'],['123'])
        responses[1][0][2]='other-head'
        with patch.object(db,'sql',side_effect=responses):
            with self.assertRaises(AssertionError):verify_business(db,'unique input',body)


if __name__=='__main__':
    unittest.main()
