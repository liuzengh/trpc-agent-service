#!/usr/bin/env python3
"""Real Control Manifest -> Worker Artifact/S3 + PG metadata -> Channel Lab."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import traceback

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from artifact_joint_fixture import ArtifactHarness, ArtifactModelFixture, BACKEND_ID, CONTENT_V0, CONTENT_V1, CONTENT_OTHER, CONTENT_SAVED_BEFORE_FAILURE, CONTENT_RECOVERED
from harness import assert_model_contract
import channel_lab_fixture as gateway_fixture
from faults import run, head, candidates, completions, attempts, wait_success, submit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    h = ArtifactHarness(args.root, args.artifacts or Path(tempfile.mkdtemp(prefix='worker-artifact-joint-')), args.race)
    evidence = {'result': 'PENDING', 'im': 'REAL_CHANNEL_LAB', 'model': 'DETERMINISTIC_HTTP_FIXTURE_REAL_SDK_TOOLS', 'rounds': [], 'failures': [], 'side_effect_boundary': 'Save immediately persists S3 bytes + Worker PG metadata; later model failure does not roll back the saved version. Delete withdraws metadata versions but retains S3 bytes.', 'cross_tenant_boundary': 'single-tenant joint; tenant rejection belongs to independent store/HTTP tests'}
    print('ARTIFACT_JOINT_ARTIFACTS=' + str(h.artifacts), flush=True)
    def save(): h.record('artifact-joint.json', evidence)
    def state():
        metadata, objects = h.metadata_state(), h.object_state()
        by_key = {obj['key']: obj for obj in objects}
        for version in metadata['versions']:
            obj = by_key[version['object_key']]
            assert version['content_sha256'] == obj['sha256'] and version['content_length'] == obj['size']
        return {'metadata': metadata, 'objects': objects}
    def success(text, previous=None, chat='42'):
        offset, outputs = len(h.model.snapshot()), len(h.model.output_snapshot())
        run_id = submit(h, text, chat)
        delivery = h.wait_delivery(run_id)
        result = wait_success(h, run_id)
        actual = run(h, run_id)
        accepted = head(h, actual)
        assert accepted['accepted_ref'] == result['candidate']['candidate_ref'] and accepted['accepted_digest'] == result['candidate']['content_digest']
        assert result['candidate']['parent_ref'] == (previous['candidate']['candidate_ref'] if previous else '')
        assert result['candidate']['parent_digest'] == (previous['candidate']['content_digest'] if previous else '')
        actual_outputs = h.model.output_snapshot()[outputs:]
        assert len(actual_outputs) == 1 and actual_outputs[0]['input'] == text
        assert delivery['final_text'] == 'artifact final: ' + text
        calls = h.model.snapshot()[offset:]
        route = json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))[0][0])['Route']
        assert route['ManifestRef'] == h.manifest_id and route['ManifestDigest'] == h.manifest_digest
        return {'input': text, 'chat': chat, 'run_id': run_id, 'run': actual, 'route': route, 'head': accepted, 'candidate': result['candidate'], 'completion': result['completion'], 'delivery': delivery, 'calls': calls, 'tool_results': actual_outputs[0]['tool_results'], 'storage': state()}
    def failed(text, previous):
        before = state()
        offset = len(h.model.snapshot())
        run_id = submit(h, text, '42')
        try:
            h.wait(lambda: run(h, run_id)['status'] == 'FAILED', 'Artifact failure is terminal, not successful Final', timeout=90)
            delivery = h.gateway.wait_delivery(run_id)
        finally:
            h.restore_artifact_storage()
        actual, completion = run(h, run_id), completions(h, run_id)
        assert len(completion) == 1 and completion[0]['status'] == 'FAILED'
        assert completion[0]['reason'] == 'RUNTIME_FAILED'
        assert completion[0]['candidate_ref'] == '' and completion[0]['candidate_digest'] == ''
        assert candidates(h, run_id) == []
        accepted = head(h, actual)
        assert accepted['accepted_ref'] == previous['candidate']['candidate_ref'] and accepted['accepted_digest'] == previous['candidate']['content_digest']
        assert delivery['final_text'] == '本次执行未完成，请稍后重试。'
        after = state()
        if text == 'artifact-storage-fail':
            assert after == before, 'unavailable storage falsely published a metadata version'
            assert h.minio_stopped is False
        else:
            assert len(after['metadata']['versions']) == len(before['metadata']['versions']) + 1
            assert len(after['objects']) == len(before['objects']) + 1
            assert any(o['bytes_hex'] == CONTENT_SAVED_BEFORE_FAILURE.hex() for o in after['objects'])
        return {'input': text, 'run_id': run_id, 'run': actual, 'attempts': attempts(h, run_id), 'completion': completion, 'head': accepted, 'candidate_rows': [], 'delivery': delivery, 'calls': h.model.snapshot()[offset:], 'storage_before': before, 'storage_after': after}
    try:
        h.provision()
        h.model.close()
        h.model = ArtifactModelFixture(h)
        h.urls['model'] = h.model.url
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        publication = h.api('GET', '/v1/tenants/' + h.tenant_id + '/deployments/' + h.deployment_id + '/revisions/1')
        envelope = json.loads(h.sql("SELECT convert_from(envelope,'UTF8') FROM worker.runtime_manifests WHERE manifest_id=" + h.quote(h.manifest_id))[0][0])
        content = envelope['content']
        node = content['agent_plan']['nodes'][content['agent_plan']['root']]
        resource = content['resources']['storage'][node['artifact']['resource']]
        assert resource['kind'] == 'managed_artifact' and resource['backend']['backend_id'] == BACKEND_ID
        assert resource['backend']['s3'] == {'endpoint': h.s3_endpoint, 'bucket': 'joint-artifacts', 'region': 'us-east-1', 'path_style': True, 'versioning': 'disabled'}
        uses = resource['credentials']
        assert set(uses) == {'access_key_id', 'secret_access_key'}
        assert uses['access_key_id']['credential_id'] != uses['secret_access_key']['credential_id']
        assert uses['access_key_id']['purpose'] == 'access_key_id' and uses['secret_access_key']['purpose'] == 'secret_access_key'
        evidence.update(publication=publication, manifest=envelope, dependency=h.s3_config, initial_storage=state())
        assert evidence['initial_storage'] == {'metadata': {'files': [], 'versions': []}, 'objects': []}
        previous = success('artifact-v0')
        evidence['rounds'].append(previous); save()
        previous = success('artifact-v1', previous)
        evidence['rounds'].append(previous); save()
        isolated = success('artifact-isolated', chat='43')
        assert isolated['run']['session_id'] != previous['run']['session_id']
        assert isolated['tool_results'][2]['ref'] != evidence['rounds'][0]['tool_results'][0]['ref']
        assert len({f['scope_id'] for f in isolated['storage']['metadata']['files']}) == 2
        evidence['rounds'].append(isolated); save()
        failure = failed('artifact-model-fail', previous)
        evidence['failures'].append(failure); save()
        previous = success('artifact-read-after-failure', previous)
        assert 'artifact-model-fail' not in [m.get('content') for m in previous['calls'][0]['messages'] if m.get('role') == 'user']
        evidence['rounds'].append(previous); save()
        evidence['failures'].append(failed('artifact-storage-fail', previous)); save()
        previous = success('artifact-recovered', previous)
        assert 'artifact-storage-fail' not in [m.get('content') for m in previous['calls'][0]['messages'] if m.get('role') == 'user']
        evidence['rounds'].append(previous); save()
        before_delete = state()
        previous = success('artifact-delete', previous)
        assert before_delete['objects'] == previous['storage']['objects'], 'logical delete claimed physical erasure'
        assert len(previous['storage']['metadata']['versions']) == len(before_delete['metadata']['versions']) - 2
        # The second Session's report version remains visible in metadata.
        other_file = next(f for f in isolated['storage']['metadata']['files'] if f['file_id'] not in {f['file_id'] for f in evidence['rounds'][1]['storage']['metadata']['files']})
        assert any(v['file_id'] == other_file['file_id'] for v in previous['storage']['metadata']['versions'])
        evidence['rounds'].append(previous)
        final = state()
        assert sorted(o['bytes_hex'] for o in final['objects']) == sorted(b.hex() for b in (CONTENT_V0, CONTENT_V1, CONTENT_OTHER, CONTENT_SAVED_BEFORE_FAILURE, CONTENT_RECOVERED))
        assert len(final['metadata']['versions']) == 3 and len(final['metadata']['files']) == 4
        model_parameters = assert_model_contract(h.model.snapshot(), publication['manifest_view'], h.model_name)
        evidence.update(result='PASS', final_storage=final, model_parameters=model_parameters, model_requests=h.model.snapshot(), model_outputs=h.model.output_snapshot(), run_count=8, success_count=6, failed_count=2)
        save()
    except BaseException as exc:
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL', error=h.redact(str(exc)), error_type=type(exc).__name__, error_location={'file': Path(frame.filename).name, 'line': frame.lineno}, model_requests=h.model.snapshot() if isinstance(getattr(h, 'model', None), ArtifactModelFixture) else [])
        save()
        raise RuntimeError(evidence['error']) from None
    finally:
        try:
            h.close()
            evidence['cleanup'] = 'PASS'
        except BaseException as exc:
            evidence.update(result='FAIL', cleanup_error=h.redact(str(exc)))
            raise
        finally:
            save()
            leaked = [str(path) for path in h.artifacts.rglob('*') if path.is_file() and any(secret.encode() in path.read_bytes() for secret in h.secrets if secret)]
            evidence['secret_scan'] = {'result': 'FAIL' if leaked else 'PASS', 'files': leaked}
            if leaked: evidence['result'] = 'FAIL'
            save()
            if evidence['result'] != 'PASS': raise RuntimeError('Artifact joint did not pass; inspect redacted evidence')
    print('WORKER_ARTIFACT_JOINT=PASS actual Control + SDK 4 tools + S3 bytes + Worker PG metadata + Session + Lab; immediate-save and logical-delete boundaries verified', flush=True)


if __name__ == '__main__': main()
