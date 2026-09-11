"""Unit seams only; these do not claim real backend acceptance."""
import copy
import hashlib
import unittest
from unittest.mock import patch
import parallel_artifact_joint_fixture as f

class ParallelArtifactTests(unittest.TestCase):
    def test_bucket_waits_for_authenticated_readiness_before_single_put(self):
        h=f.ParallelArtifactHarness.__new__(f.ParallelArtifactHarness)
        def wait(predicate, description):
            with self.assertRaisesRegex(RuntimeError, '503'): predicate()
            self.assertTrue(predicate())
        h.wait=wait
        with patch.object(f.ArtifactHarness,'s3_request',side_effect=[RuntimeError('503'),b'NoSuchBucket',b'created']) as request:
            self.assertEqual(h.s3_request('PUT'),b'created')
        self.assertEqual([call.args[0] for call in request.call_args_list],['GET','GET','PUT'])
        self.assertEqual(request.call_args_list[0].kwargs,{'expected':404})

    def test_tree_explicit_authority_no_mutation(self):
        spec={'root':'assistant','requirements':{'models':{'primary':{'capabilities':['chat']}}},'nodes':{'assistant':{'kind':'llm'}}}
        original=copy.deepcopy(spec);tree=f.tree_spec(spec)
        self.assertEqual(spec,original)
        self.assertEqual(tree['nodes']['parallel']['children'],['a','b'])
        self.assertNotIn('artifact',tree['nodes']['aggregate'])
        self.assertEqual(tree['nodes']['a']['artifact'],{'enabled':True})

    def test_draft_bypasses_single_leaf_override(self):
        h=f.ParallelArtifactHarness.__new__(f.ParallelArtifactHarness)
        body={'spec':{'requirements':{'models':{'primary':{}}}}}
        with patch.object(f.Harness,'api',return_value={'ok':True}) as api:
            h.api('PUT','/v1/tenants/t/agents/a/draft',body)
        self.assertEqual(api.call_args.args[3]['spec']['nodes']['parallel']['kind'],'parallel')
        self.assertNotIn('nodes',body['spec'])

    def test_version_content_mapping_not_arrival_order(self):
        first=b'a';second=b'b'
        state={'metadata':{'files':[{'filename':'shared.bin','file_id':'f'}],'versions':[{'file_id':'f','version':0,'object_key':'second'},{'file_id':'f','version':1,'object_key':'first'}]},'objects':[{'key':key,'bytes_hex':data.hex(),'sha256':hashlib.sha256(data).hexdigest(),'size':len(data)} for key,data in [('first',first),('second',second)]]}
        f.assert_saved(state,'shared.bin',first,1);f.assert_saved(state,'shared.bin',second,0)
        with self.assertRaises(AssertionError):f.assert_saved(state,'shared.bin',first,0)

    def test_role_and_current_case_not_foreign_user_context(self):
        request={'messages':[{'role':'system','content':'PAR_ARTIFACT_B: task'},{'role':'user','content':f.CASES[0]},{'role':'user','content':'[a] said: prior result'},{'role':'tool','content':'{"version":0}'}]}
        self.assertEqual(f.classify(request),('b',f.CASES[0],[{'version':0}]))

    def test_same_name_different_full_bytes(self):
        a=f.artifact_plan(f.CASES[1],'a');b=f.artifact_plan(f.CASES[1],'b')
        self.assertEqual(a[0],b[0]);self.assertNotEqual(a[1],b[1]);self.assertTrue(a[1].endswith(b'\x00\xff'))

if __name__=='__main__':unittest.main()
