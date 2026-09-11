#!/usr/bin/env python3
"""Owned MinIO + Worker PG: real Parallel Artifact SDK operations and acceptance."""
import argparse
import json
from pathlib import Path
import sys
import traceback
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from parallel_artifact_joint_fixture import ParallelArtifactHarness, ParallelArtifactModelFixture, CASES, artifact_plan, storage_state, assert_saved
import channel_lab_fixture as lab
from faults import run, head, candidates, completions, wait_success, submit, rows, attempts, outboxes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path, default=Path('/private/tmp/worker-parallel-data-artifact-20260909'))
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    h = ParallelArtifactHarness(args.root, args.artifacts, args.race)
    evidence = {'result': 'PENDING', 'model': 'DETERMINISTIC_HTTP_FIXTURE_REAL_SDK', 'backend': 'OWNED_MINIO_AND_WORKER_POSTGRES', 'im': 'REAL_CHANNEL_LAB', 'rounds': [], 'boundary': 'Artifact Save immediately persists; a failed parallel peer does not roll it back. No cross-branch transaction or GC is claimed.'}
    def save(): h.record('parallel-artifact-joint.json', evidence)
    try:
        h.provision(); h.model.close(); h.model = ParallelArtifactModelFixture(h); h.urls['model'] = h.model.url
        h.control_start(lab.prepare(h)); h.seed(); h.start_worker(); h.verify_dependencies(); h.gateway = lab.start(h)
        publication = h.api('GET', '/v1/tenants/' + h.tenant_id + '/deployments/' + h.deployment_id + '/revisions/1')
        evidence['manifest'] = publication['manifest_view']
        nodes = evidence['manifest']['agent_plan']['nodes']
        assert nodes['parallel']['kind'] == 'parallel' and nodes['parallel']['children'] == ['a', 'b']
        previous = None
        for case in CASES:
            before = storage_state(h); offset = len(h.model.snapshot())
            run_id = submit(h, case, '42')
            if case != CASES[2]:
                result = wait_success(h, run_id); delivery = h.wait_delivery(run_id)
                actual = run(h, run_id); accepted = head(h, actual)
                candidate = result['candidate']
                assert accepted['accepted_ref'] == candidate['candidate_ref'] and accepted['accepted_digest'] == candidate['content_digest']
                assert candidate['parent_ref'] == (previous['candidate_ref'] if previous else '')
                assert candidate['parent_digest'] == (previous['content_digest'] if previous else '')
                assert delivery['final_text'] == 'PAR_ARTIFACT_FINAL:' + case
                previous = candidate
            else:
                h.wait(lambda: run(h, run_id)['status'] == 'FAILED', 'parallel failure after actual Artifact save', timeout=90)
                delivery = h.gateway.wait_delivery(run_id); actual = run(h, run_id); accepted = head(h, actual)
                completion = completions(h, run_id)
                assert len(completion) == 1 and completion[0]['status'] == 'FAILED' and completion[0]['reason'] == 'RUNTIME_FAILED'
                assert completion[0]['candidate_ref'] == '' and completion[0]['candidate_digest'] == '' and candidates(h, run_id) == []
                assert accepted['accepted_ref'] == previous['candidate_ref'] and accepted['accepted_digest'] == previous['content_digest']
                assert delivery['final_text'] == '本次执行未完成，请稍后重试。'
                assert h.model.saved_before_failure.is_set()
                result = {'completion': completion, 'candidate': None}
            after = storage_state(h)
            saved = [item for item in h.model.save_snapshot() if item['case'] == case]
            expected_roles = {'a'} if case == CASES[2] else {'a', 'b'}
            assert {item['role'] for item in saved} == expected_roles and len(saved) == len(expected_roles)
            for item in saved:
                name, content = artifact_plan(case, item['role'])
                assert_saved(after, name, content, item['result']['version'])
            if case == CASES[1]: assert {item['result']['version'] for item in saved} == {0, 1}
            else: assert {item['result']['version'] for item in saved} == {0}
            assert len(after['metadata']['versions']) == len(before['metadata']['versions']) + len(expected_roles)
            assert len(after['objects']) == len(before['objects']) + len(expected_roles)
            overlap = h.model.overlap_snapshot(case)
            assert set(overlap['entered']) == set(overlap['released']) == {'a', 'b'}
            assert max(overlap['entered'].values()) <= min(overlap['released'].values()), 'responses released before both actual provider requests arrived'
            calls = h.model.snapshot()[offset:]
            assert len(calls) == (3 if case == CASES[2] else 5)
            route = json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))[0][0])['Route']
            assert route['ManifestRef'] == h.manifest_id and route['ManifestDigest'] == h.manifest_digest
            record = {'case': case, 'run_id': run_id, 'run': actual, 'result': result, 'accepted_head': accepted, 'route': route, 'delivery': delivery, 'saves': saved, 'provider_calls': calls, 'actual_provider_barrier': overlap, 'storage_before': before, 'storage_after': after}
            record['attempts'] = attempts(h, run_id)
            record['completions'] = completions(h, run_id)
            record['outboxes'] = outboxes(h, run_id)
            record['candidates'] = candidates(h, run_id)
            record['gateway_receipts'] = rows(h, 'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id=' + h.quote(run_id))
            record['gateway_parts'] = rows(h, 'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=' + h.quote(run_id) + ' ORDER BY p.part_index')
            comp, finals, receipts, parts = record['completions'], record['outboxes'], record['gateway_receipts'], record['gateway_parts']
            assert len(comp) == len(finals) == len(receipts) == 1
            assert comp[0]['final_intent_id'] == finals[0]['intent_id'] == receipts[0]['intent_id'] == delivery['intent_id']
            assert receipts[0]['outcome'] == 'ACCEPTED' and receipts[0]['reason'] == '' and receipts[0]['run_id'] == run_id
            assert parts and all(p['state'] == 'ACCEPTED' for p in parts)
            assert ''.join(p['body'] for p in parts) == delivery['final_text']
            assert len(delivery['outgoing_added']) == len(parts)
            assert ''.join(m['text'] for m in delivery['outgoing_added']) == delivery['final_text']
            evidence['rounds'].append(record)
            save()
        assert len(h.model.output_snapshot()) == 2 and not h.model.errors
        evidence.update(result='PASS', run_count=3, success_count=2, failed_count=1)
        save()
    except BaseException as exc:
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL', error=h.redact(str(exc)), error_type=type(exc).__name__, location={'file': Path(frame.filename).name, 'line': frame.lineno})
        save()
        raise
    finally:
        try: h.close(); evidence['cleanup'] = 'PASS'
        except BaseException as exc: evidence.update(result='FAIL', cleanup_error=h.redact(str(exc))); raise
        finally:
            save()
            leaked = [str(p) for p in h.artifacts.rglob('*') if p.is_file() and any(s.encode() in p.read_bytes() for s in h.secrets if s)]
            evidence['secret_scan'] = {'result': 'FAIL' if leaked else 'PASS', 'files': leaked}
            if leaked: evidence['result'] = 'FAIL'
            save()
            if evidence['result'] != 'PASS': raise RuntimeError('Parallel Artifact gate failed; inspect redacted evidence')
    print('WORKER_PARALLEL_ARTIFACT_JOINT=PASS runs=3 real_sdk=true pg_metadata=true minio_bytes=true lab=true', flush=True)

if __name__ == '__main__': main()
