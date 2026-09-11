#!/usr/bin/env python3
"""Real managed Redis Session + same-candidate Summary + Channel Lab acceptance."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import traceback

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from redis_session_joint_fixture import RedisSessionHarness, SummaryModelFixture, CANARY, BACKEND_ID, wait_redis_success
import channel_lab_fixture as gateway_fixture
from faults import run, head, completions, attempts
from scope_scenarios import change_binding, binding_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    h = RedisSessionHarness(args.root, args.artifacts or Path(tempfile.mkdtemp(prefix='worker-session-redis-joint-')), args.race)
    evidence = {'result': 'PENDING', 'model_fixture': 'EXISTING_SUMMARY_MODEL_FIXTURE', 'im': 'REAL_CHANNEL_LAB', 'rounds': [], 'failures': [], 'scope_rounds': [], 'overlay_toggle_boundary': 'existing SDK same-snapshot tests; a published new revision is explicitly a new SessionScope'}
    def save():
        h.record('session-redis-joint.json', evidence)
    def success(text, previous=None):
        start = len(h.model.snapshot())
        prior_summary = None
        if previous:
            summaries = previous['candidate']['content']['snapshot']['session'].get('summaries', {})
            if summaries:
                prior_summary = next(iter(summaries.values()))['summary']
        run_id = h.send_text(text)
        delivery = h.wait_delivery(run_id)
        result = wait_redis_success(h, run_id)
        actual = run(h, run_id)
        route = json.loads(h.sql("SELECT (request_json->'Route')::text FROM worker.execution_runs WHERE run_id=" + h.quote(run_id))[0][0])
        accepted = head(h, actual)
        c = result['candidate']
        assert accepted['accepted_ref'] == c['candidate_ref'] and accepted['accepted_digest'] == c['content_digest']
        assert c['parent_ref'] == (previous['candidate']['candidate_ref'] if previous else '')
        assert c['parent_digest'] == (previous['candidate']['content_digest'] if previous else '')
        calls = h.model.snapshot()[start:]
        primary = [call for call in calls if call['model'] == 'joint-fixture']
        summaries = [call for call in calls if call['model'] == 'joint-summary']
        assert len(primary) == 1
        for call in calls:
            assert call['max_completion_tokens'] == evidence['manifest']['content']['execution']['max_output_tokens']
        if prior_summary:
            assert prior_summary in json.dumps(primary[0]['messages']), 'accepted summary missing from next real SDK input'
        if summaries:
            stored = c['content']['snapshot']['session']['summaries']
            assert len(stored) == 1 and next(iter(stored.values()))['summary'] == h.model.summary_outputs()[-1], 'candidate did not contain this exact summary result'
        usage = []
        for line in (h.artifacts / 'worker-one.log').read_text().splitlines():
            observation = json.loads(line)
            if observation.get('operation') == 'usage' and observation.get('run_id') == run_id:
                usage.append({k: observation[k] for k in ('input_tokens', 'output_tokens', 'total_tokens')})
        assert usage == [{'input_tokens': 3 + 11 * len(summaries), 'output_tokens': 2 + 7 * len(summaries), 'total_tokens': 5 + 18 * len(summaries)}]
        assert delivery['final_text'] == 'joint answer: ' + text
        return {'run_id': run_id, 'input': text, 'run': actual, 'route': route, 'head': accepted, 'candidate': c, 'completion': result['completion'], 'calls': calls, 'usage': usage, 'delivery': delivery}
    def failed(text, reason, no_model=False):
        storage_before = h.session_state()
        offset = len(h.model.snapshot())
        run_id = h.send_text(text)
        h.wait(lambda: h.sql('SELECT status FROM worker.execution_runs WHERE run_id=' + h.quote(run_id)) == [['FAILED']], 'failed Redis Session Run', timeout=90)
        delivery = h.gateway.wait_delivery(run_id)
        rows = completions(h, run_id)
        assert len(rows) == 1 and rows[0]['status'] == 'FAILED' and rows[0]['reason'] == reason
        assert rows[0]['candidate_ref'] == '' and rows[0]['candidate_digest'] == ''
        candidate_rows = h.session_candidates(run_id)
        assert candidate_rows == []
        storage_after = h.session_state()
        assert storage_before == storage_after, 'failed Attempt changed immutable Redis candidates'
        calls = h.model.snapshot()[offset:]
        if no_model:
            assert calls == [], 'missing fixed Parent reached SDK instead of failing closed'
        else:
            assert any(call['model'] == 'joint-summary' for call in calls), 'summary failure did not reach actual summary model'
        assert delivery['final_text'] == '本次执行未完成，请稍后重试。'
        assert h.sql('SELECT count(*) FROM runtime_session.session_candidates') == [['0']]
        return {'run_id': run_id, 'reason': reason, 'run': run(h, run_id), 'attempts': attempts(h, run_id), 'completion': rows, 'candidate_rows': candidate_rows, 'calls': calls, 'delivery': delivery, 'storage_before': storage_before, 'storage_after': storage_after}
    try:
        h.provision()
        h.model.close()
        h.model = SummaryModelFixture(h)
        h.urls['model'] = h.model.url
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        envelope = json.loads(h.sql("SELECT convert_from(envelope,'UTF8') FROM worker.runtime_manifests WHERE manifest_id=" + h.quote(h.manifest_id))[0][0])
        content = envelope['content']
        resource = content['resources']['storage'][content['storage_roles']['session']]
        assert resource['kind'] == 'managed_session' and resource['backend']['kind'] == 'redis' and resource['backend']['backend_id'] == BACKEND_ID
        assert resource['backend']['redis']['port'] == h.redis_port and resource['credential']['purpose'] == 'dsn_password'
        assert content['runtime']['summary']['enabled'] and content['agent_plan']['nodes'][content['agent_plan']['root']]['add_session_summary']
        evidence.update(manifest=envelope, dependency=h.redis_config, initial_redis=h.session_state())
        assert evidence['initial_redis'] == []
        previous = None
        for text in ('redis summary seed', 'redis summary generate', 'redis summary consume'):
            previous = success(text, previous)
            evidence['rounds'].append(previous)
            save()
        original_session = previous['run']['session_id']
        before = previous['head']
        h.model.fail_summary = True
        try:
            failure = failed('redis SUMMARY_FAILURE_NOT_ACCEPTED', 'RUNTIME_FAILED')
        finally:
            h.model.fail_summary = False
        after = head(h, failure['run'])
        assert (after['accepted_ref'], after['accepted_digest']) == (before['accepted_ref'], before['accepted_digest'])
        failure.update(head_before=before, head_after=after)
        evidence['failures'].append(failure)
        previous = success('redis summary after failure', previous)
        assert 'SUMMARY_FAILURE_NOT_ACCEPTED' not in json.dumps(previous['calls'])
        evidence['rounds'].append(previous)
        save()

        original_candidate = previous['candidate']
        original_storage = h.session_state()
        quarantine = h.quarantine_parent(original_candidate)
        try:
            missing = failed('redis MISSING_PARENT_NOT_ACCEPTED', 'SESSION_INVALID', no_model=True)
            after = head(h, missing['run'])
            assert after['accepted_ref'] == original_candidate['candidate_ref'] and after['accepted_digest'] == original_candidate['content_digest']
            assert all(a['parent_ref'] == original_candidate['candidate_ref'] for a in missing['attempts'])
            missing.update(head_before=previous['head'], head_after=after, quarantined=quarantine)
            evidence['failures'].append(missing)
        finally:
            h.restore_parent()
        restored = h.session_state()
        assert restored == original_storage
        evidence['missing_parent_restoration'] = {'before': original_storage, 'after': restored}
        previous = success('redis summary after exact parent restore', previous)
        assert 'MISSING_PARENT_NOT_ACCEPTED' not in json.dumps(previous['calls'])
        evidence['rounds'].append(previous)
        save()

        # Same published configuration in a NEW revision is not an overlay toggle.
        base = '/v1/tenants/' + h.tenant_id + '/deployments/' + h.deployment_id
        old_revision = h.api('GET', base + '/revisions/1')
        publication = h.api('POST', base + '/revisions', {'expected_latest_revision_number': 1, 'input': old_revision['input']}, status=201, idem='redis-session-revision-two')
        revision = publication['revision']
        h.wait(lambda: h.sql('SELECT content_digest FROM worker.runtime_manifests WHERE manifest_id=' + h.quote(revision['manifest_id'])) == [[revision['manifest_digest']]], 'Redis Session second immutable Manifest')
        original_binding = h.api('GET', binding_path(h))
        try:
            changed = change_binding(h, revision=2)
            new = success('redis revision two starts isolated')
            assert new['run']['session_id'] != original_session and new['run']['session_sequence'] == 1
            assert new['route']['DeploymentRevisionID'] == revision['id'] and new['route']['ManifestRef'] == revision['manifest_id'] and new['route']['ManifestDigest'] == revision['manifest_digest']
            first_model = next(call for call in new['calls'] if call['model'] == 'joint-fixture')
            assert CANARY not in json.dumps(first_model['messages']) and 'redis summary' not in json.dumps(first_model['messages'])
            next_new = success('redis revision two consumes its own summary', new)
            assert next_new['run']['session_id'] == new['run']['session_id'] and next_new['route'] == new['route']
            evidence['scope_rounds'] = [new, next_new]
            evidence.update(second_publication=publication, changed_binding=changed)
        finally:
            restored_binding = change_binding(h, revision=1)
            assert restored_binding['binding']['target'] == original_binding['binding']['target']
            evidence['binding_restoration'] = restored_binding
        state = h.session_state()
        assert len(state) == len(evidence['rounds']) + len(evidence['scope_rounds']) == 7
        evidence.update(result='PASS', final_redis=state, generated_summaries=h.model.summary_outputs(), all_model_requests=h.model.snapshot(), postgres_candidate_count=0)
        save()
    except BaseException as exc:
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL', error=h.redact(str(exc)), error_type=type(exc).__name__, error_location={'file': Path(frame.filename).name, 'line': frame.lineno})
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
            if leaked:
                evidence['result'] = 'FAIL'
            save()
            if evidence['result'] != 'PASS':
                raise RuntimeError('Redis Session joint did not pass; inspect redacted evidence')
    print('WORKER_SESSION_REDIS_JOINT=PASS actual Control + Redis immutable Session/Summary + PG accepted head + Lab + missing-parent fail-closed + new revision isolation', flush=True)


if __name__ == '__main__':
    main()
