"""Evidence guard tests; the process gate supplies actual broker and owner facts."""
import copy
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock

from manifest_gap_scenarios import (STREAM, assert_recreated, assert_worker_export,
    lifecycle_request, restore_ready, run_manifest_gap)


class ManifestGapTests(unittest.TestCase):
    def test_recreation_requires_gap_new_incarnations_and_equal_complete_config(self):
        before={'stream_created':'old','durable_created':'old-c','config':{'max_age':0,'deny_delete':True},
                'consumer_config':{'ack_policy':'explicit'},'messages':3}
        after={**before,'stream_created':'new','durable_created':'new-c','messages':0,'pending':0,'ack_pending':0}
        assert_recreated(before,after)
        for field,value in [('stream_created','old'),('durable_created','old-c'),('messages',1),('pending',1),('ack_pending',1),
                            ('config',{'max_age':1}),('consumer_config',{'ack_policy':'none'})]:
            with self.subTest(field=field),self.assertRaises(AssertionError):assert_recreated(before,{**after,field:value})

    def test_lifecycle_whitelist_rejects_other_streams_and_message_mutations_before_network(self):
        h=SimpleNamespace(broker='fixture-nats',prefix='fixture',containers=['fixture-nats'],nats_url='tls://127.0.0.1:4222')
        for subject in ['$JS.API.STREAM.DELETE.RUN_REQUESTS_V1','$JS.API.STREAM.PURGE.'+STREAM,
                        '$JS.API.STREAM.MSG.DELETE.'+STREAM,'$JS.API.STREAM.UPDATE.'+STREAM]:
            with self.subTest(subject=subject),self.assertRaises(ValueError):lifecycle_request(h,subject,{})
        with self.assertRaises(ValueError):lifecycle_request(h,'$JS.API.STREAM.DELETE.'+STREAM,{'seq':1})
        with self.assertRaises(AssertionError):lifecycle_request(SimpleNamespace(**{**vars(h),'broker':'unrelated'}),'$JS.API.STREAM.DELETE.'+STREAM,{})

    def test_owner_evidence_requires_actual_complete_unmodified_worker_get_after_start(self):
        event={'method':'GET','path':'/internal/v1/runtime-manifests/export','principal_uri':'spiffe://agent-platform/worker/one',
               'request_ordinal':4,'started_ns':100,'finished_ns':120,'upstream_status':200,'downstream_status':200,
               'owner_response_bytes':100,'downstream_body_bytes_forwarded':100,'downstream_action':'forwarded'}
        assert_worker_export([event],3,99)
        for field,value in [('request_ordinal',3),('started_ns',98),('finished_ns',None),('upstream_status',403),
                            ('downstream_status',403),('downstream_action','mutated_owner_200'),('downstream_body_bytes_forwarded',99),
                            ('principal_uri','spiffe://agent-platform/worker/two')]:
            with self.subTest(field=field),self.assertRaises(AssertionError):assert_worker_export([{**event,field:value}],3,99)
        with self.assertRaises(AssertionError):assert_worker_export([],3,99)

    def test_restore_records_false_readiness_before_failure(self):
        record={}
        with mock.patch('manifest_gap_scenarios.restore_standard',return_value={'ready':False,'pid':12}):
            with self.assertRaises(AssertionError):restore_ready(object(),record)
        self.assertEqual(record,{'restoration':{'ready':False,'pid':12}})

    def test_cleanup_failure_never_leaves_pass_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            h=SimpleNamespace(artifacts=Path(directory))
            with mock.patch('manifest_gap_scenarios._scenario',return_value={'result':'PASS'}), \
                 mock.patch('manifest_gap_scenarios.restore_standard',return_value={'ready':False,'pid':12}):
                with self.assertRaises(AssertionError):run_manifest_gap(h)
            evidence=json.loads((Path(directory)/'manifest-gap-rebuild.json').read_text())
            self.assertEqual(evidence['result'],'FAIL')
            self.assertFalse(evidence['restoration']['ready'])

if __name__=='__main__':unittest.main()
