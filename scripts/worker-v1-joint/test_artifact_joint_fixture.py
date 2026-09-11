"""Fixture seam checks; separate joint gate proves product behavior."""
import copy
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import urllib.error
import urllib.request
import artifact_joint_fixture as f


class ArtifactFixtureTests(unittest.TestCase):
    def test_signer_binds_request(self):
        args = ('http://127.0.0.1:19000', 'access', 'private', 'PUT', '/joint-artifacts/example.bin', '', b'bytes')
        now = datetime(2026, 9, 9, tzinfo=timezone.utc)
        signed = f.signed_s3_headers(*args, now=now)
        self.assertEqual(signed, f.signed_s3_headers(*args, now=now))
        self.assertEqual(signed['X-Amz-Date'], '20260909T000000Z')
        self.assertEqual(signed['X-Amz-Content-Sha256'], hashlib.sha256(b'bytes').hexdigest())
        self.assertNotIn('private', str(signed))
        for index, replacement in ((0, 'http://127.0.0.1:19001'), (3, 'GET'), (4, '/joint-artifacts/other'), (5, 'list-type=2'), (6, b'different')):
            changed = list(args); changed[index] = replacement
            self.assertNotEqual(signed['Authorization'], f.signed_s3_headers(*changed, now=now)['Authorization'])

    def test_profile_two_credential_slots_no_input_mutation(self):
        h = f.ArtifactHarness.__new__(f.ArtifactHarness)
        h.artifact_access_key, h.artifact_secret_key = 'fixture-key', 'fixture-value'
        body = {'config': {'models': {'primary': {}}, 'storage': {}}, 'credentials': {'storage': {}}}
        before = copy.deepcopy(body)
        with patch.object(f.Harness, 'api', return_value={'ok': True}) as api:
            self.assertEqual(h.api('PUT', '/v1/tenants/t/runtime-profiles/p/draft', body), {'ok': True})
        actual = api.call_args.args[2]
        self.assertEqual(body, before)
        self.assertEqual(actual['config']['storage']['artifact'], {'kind': 'managed_artifact', 'backend_id': f.BACKEND_ID, 'backend_revision': 1})
        self.assertEqual(actual['credentials']['storage']['artifact'], {'access_key_id': {'action': 'replace', 'value': 'fixture-key'}, 'secret_access_key': {'action': 'replace', 'value': 'fixture-value'}})

    def test_s3_http_error_closed_and_redacted(self):
        h = f.ArtifactHarness.__new__(f.ArtifactHarness)
        h.s3_endpoint, h.artifact_access_key, h.artifact_secret_key = 'http://127.0.0.1:19000', 'access', 'secret'
        body = io.BytesIO(b'private server detail')
        response = urllib.error.HTTPError(h.s3_endpoint, 403, 'forbidden', {}, body)
        with patch.object(urllib.request, 'urlopen', side_effect=response):
            with self.assertRaisesRegex(RuntimeError, '^fixture S3 HTTP status 403 expected 200$'):
                h.s3_request('GET')
        self.assertTrue(body.closed)

    def test_object_actual_bytes_and_pagination(self):
        h = f.ArtifactHarness.__new__(f.ArtifactHarness)
        calls = []
        def s3(method, key=None, query=None):
            calls.append((method, key, copy.deepcopy(query)))
            if key: return {'one': b'a', 'two': b'bc'}[key]
            key = 'two' if query.get('continuation-token') else 'one'
            return ('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Contents><Key>' + key + '</Key><Size>' + str(1 if key == 'one' else 2) + '</Size></Contents><IsTruncated>' + ('true' if key == 'one' else 'false') + '</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>').encode()
        h.s3_request = s3
        result = h.object_state()
        self.assertEqual([(v['key'], v['size'], v['bytes_hex']) for v in result], [('one', 1, '61'), ('two', 2, '6263')])
        self.assertEqual(calls[2], ('GET', None, {'list-type': '2', 'continuation-token': 'next'}))

    def cleanup_fixture(self):
        h = f.ArtifactHarness.__new__(f.ArtifactHarness)
        h.minio, h.work = 'only-owned-minio', Path('/absent-private-fixture')
        h.restore_artifact_storage = Mock(); h.redact = lambda s: s
        records = []; h.record = lambda name, value: records.append(value)
        return h, records

    def test_cleanup_daemon_outage_is_not_removed(self):
        h, records = self.cleanup_fixture()
        with patch.object(f.Harness, 'close'), patch.object(f.subprocess, 'run', return_value=Mock(returncode=1, stderr='daemon disconnected')):
            with self.assertRaisesRegex(RuntimeError, 'removal unverified'): h.close()
        self.assertEqual(records[0]['result'], 'FAIL')
        self.assertFalse(records[0]['removed'])

    def test_cleanup_reclaims_dependency_on_parent_error(self):
        h, records = self.cleanup_fixture()
        with patch.object(f.Harness, 'close', side_effect=RuntimeError('parent failed')), patch.object(f.subprocess, 'run') as command:
            command.side_effect = [Mock(returncode=0), Mock(returncode=0), Mock(returncode=1, stderr='Error: No such object: only-owned-minio')]
            with self.assertRaisesRegex(RuntimeError, 'parent failed'): h.close()
        self.assertEqual(records[0]['result'], 'FAIL')
        self.assertTrue(records[0]['removed'])

    def test_actual_http_model_requires_byte_exact_load(self):
        h = Mock(); h.secret.return_value = 'fixture-only-key'; h.model_name = 'joint-fixture'
        model = f.ArtifactModelFixture(h)
        try:
            request = {'model': 'joint-fixture', 'messages': [{'role': 'user', 'content': 'artifact-v0'}], 'tools': [{'function': {'name': name}} for name in f.TOOLS]}
            def post():
                req = urllib.request.Request(model.url + '/v1/chat/completions', data=json.dumps(request).encode(), headers={'Authorization': 'Bearer fixture-only-key', 'Content-Type': 'application/json'})
                with urllib.request.urlopen(req) as response:
                    return [json.loads(line[6:]) for line in response.read().decode().splitlines() if line.startswith('data: {')][0]['choices'][0]['delta']
            self.assertEqual(post()['tool_calls'][0]['function']['name'], 'artifact_save')
            metadata = {'name': 'report.bin', 'version': 0, 'ref': 'artifact:scope:report.bin:0', 'mime_type': 'application/octet-stream', 'size_bytes': len(f.CONTENT_V0), 'sha256': hashlib.sha256(f.CONTENT_V0).hexdigest()}
            request['messages'].append({'role': 'tool', 'content': json.dumps(metadata)})
            self.assertEqual(post()['tool_calls'][0]['function']['name'], 'artifact_load')
            request['messages'].append({'role': 'tool', 'content': json.dumps(dict(metadata, content_base64=f.save_args('x', f.CONTENT_V0)['content_base64']))})
            self.assertEqual(post()['tool_calls'][0]['function']['name'], 'artifact_list')
            request['messages'].append({'role': 'tool', 'content': '{"keys":["report.bin"]}'})
            self.assertEqual(post()['content'], 'artifact final: artifact-v0')
            self.assertEqual(len(model.snapshot()), 4)
        finally:
            model.close()

    def test_gui_read_uses_real_tool_result_and_text_mime(self):
        h = Mock(); h.secret.return_value = 'fixture-key'; h.model_name = 'joint-fixture'
        h.gui_artifact_expected = {'name': 'gui-upload.txt', 'version': 0, 'content': 'artifact GUI accepted bytes\n', 'mime_type': 'text/plain'}
        model = f.ArtifactModelFixture(h)
        try:
            body = {'model': 'joint-fixture', 'messages': [{'role': 'user', 'content': 'artifact-gui-read'}], 'tools': [{'function': {'name': name}} for name in f.TOOLS]}
            def post():
                req = urllib.request.Request(model.url + '/v1/chat/completions', data=json.dumps(body).encode(), headers={'Authorization': 'Bearer fixture-key'})
                with urllib.request.urlopen(req) as response:
                    return [json.loads(line[6:]) for line in response.read().decode().splitlines() if line.startswith('data: {')][0]['choices'][0]['delta']
            first = post()['tool_calls'][0]['function']
            self.assertEqual(first['name'], 'artifact_load')
            self.assertEqual(json.loads(first['arguments']), {'name': 'gui-upload.txt', 'version': 0})
            data = h.gui_artifact_expected['content'].encode()
            value = {'name': 'gui-upload.txt', 'version': 0, 'mime_type': 'text/plain', 'size_bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(), 'ref': 'artifact:scope:gui-upload.txt:0', 'content_base64': f.save_args('x', data)['content_base64']}
            body['messages'].append({'role': 'tool', 'content': json.dumps(value)})
            self.assertEqual(post()['content'], 'artifact final: artifact-gui-read')
            self.assertEqual(model.output_snapshot()[0]['tool_results'], [value])
        finally:
            model.close()


if __name__ == '__main__': unittest.main()
