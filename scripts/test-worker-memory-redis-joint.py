#!/usr/bin/env python3
"""Real Control -> Manifest -> Worker/Redis -> Channel Lab Memory acceptance.

Reuse the PostgreSQL Memory suite's model fixture and all production services.
Only the Memory dependency is Redis; Session and execution facts remain in PG.
"""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import traceback

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from redis_memory_joint_fixture import RedisMemoryHarness, BACKEND_ID, WRITE_RESTORE
from memory_joint_fixture import MemoryModelFixture, TOOLS
import channel_lab_fixture as gateway_fixture
from faults import run, head, candidates, completions, wait_success


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    h = RedisMemoryHarness(args.root, args.artifacts or Path(tempfile.mkdtemp(prefix='worker-memory-redis-joint-')), args.race)
    evidence = {'result': 'PENDING', 'model': 'REUSED_DETERMINISTIC_HTTP_FIXTURE', 'im': 'REAL_ISOLATED_CHANNEL_LAB', 'memory_backend': 'redis', 'rounds': [], 'failures': [], 'scope_checks': []}
    target = h.artifacts / 'memory-redis-joint.json'
    print('MEMORY_REDIS_JOINT_ARTIFACTS=' + str(h.artifacts), flush=True)

    def save():
        h._record(target.name, evidence)

    def success(text, chat='42', sender=100):
        offset = len(h.model.outputs)
        run_id = h.gateway.send_text(text, conversation_id=chat, sender_id=sender)
        delivery = h.wait_delivery(run_id)
        result = wait_success(h, run_id)
        actual_run = run(h, run_id)
        accepted = head(h, actual_run)
        assert accepted['accepted_ref'] == result['candidate']['candidate_ref']
        assert delivery['final_text'] == 'memory final: ' + text
        assert h.sql('SELECT memory_status FROM worker.execution_completions WHERE run_id=' + h.quote(run_id)) == [['APPLIED']]
        outputs = [row for row in h.model.outputs[offset:] if row['input'] == text]
        assert len(outputs) == 1
        request = json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))[0][0])
        assert request['Route']['ManifestRef'] == h.manifest_id and request['Route']['ManifestDigest'] == h.manifest_digest
        return {'run_id': run_id, 'input': text, 'chat': chat, 'sender': sender, 'request_route': request['Route'], 'request_input': request['Input'], 'delivery': delivery, 'session_head': accepted, 'completion': result['completion'], 'memory': h.memory_state(), 'tool_results': outputs[0]['tool_results']}

    try:
        h.provision()
        h.model.close()
        h.model = MemoryModelFixture(h)
        h.urls['model'] = h.model.url
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        publication = h.api('GET', '/v1/tenants/' + h.tenant_id + '/deployments/' + h.deployment_id + '/revisions/1')
        view = publication['manifest_view']
        node = view['agent_plan']['nodes'][view['agent_plan']['root']]
        resource = view['resources']['storage'][node['memory']['resource']]
        backend = resource['backend']
        assert sorted(node['memory']['tools']) == sorted(TOOLS)
        assert backend['kind'] == 'redis' and backend['backend_id'] == BACKEND_ID
        # Public views deliberately redact target/credential metadata. Read the
        # actual immutable Worker projection, not a hand-constructed Manifest.
        envelope = json.loads(h.sql("SELECT convert_from(envelope,'UTF8') FROM worker.runtime_manifests WHERE tenant_id=" + h.quote(h.tenant_id) + ' AND manifest_id=' + h.quote(h.manifest_id))[0][0])
        assert envelope['manifest_id'] == h.manifest_id and envelope['content_digest'] == h.manifest_digest
        runtime_resource = envelope['content']['resources']['storage'][node['memory']['resource']]
        assert runtime_resource['backend']['redis'] == {'host': '127.0.0.1', 'port': h.redis_port, 'database': 0, 'username': 'memory_runtime', 'tls': False}
        assert runtime_resource['credential']['purpose'] == 'dsn_password'
        evidence.update(publication=publication, worker_manifest=envelope, dependency=h.redis_config, initial_storage=h.storage_state())
        assert evidence['initial_storage'] == []

        for text in ('memory-six', 'memory-read', 'memory-correct'):
            row = success(text)
            assert len(row['memory']) == 1 and row['memory'][0]['revision'] == len(evidence['rounds']) + 1
            entries = row['memory'][0]['content']['entries']
            assert len(entries) == 1 and entries[0]['memory']['memory'] == 'persistent orchid memory'
            assert row['tool_results'][-1]['results'][0]['memory'] == 'persistent orchid memory'
            evidence['rounds'].append(row)
            save()

        for text in ('memory-fail', 'memory-pg-fail'):
            before = h.storage_state()
            run_id = h.send_text(text)
            expected_status = 'FAILED' if text == 'memory-fail' else 'SUCCEEDED'
            h.wait(lambda: h.sql('SELECT status FROM worker.execution_runs WHERE run_id=' + h.quote(run_id)) == [[expected_status]], 'Redis Memory failure terminal', timeout=90)
            delivery = h.gateway.wait_delivery(run_id)
            assert delivery['final_text'] == '本次执行未完成，请稍后重试。'
            after = h.storage_state()
            assert after == before, 'failed execution/apply modified formal Redis keys'
            failure = completions(h, run_id)
            assert len(failure) == 1 and failure[0]['status'] == expected_status
            memory_status = h.sql("SELECT COALESCE(NULLIF(memory_status,''),'NONE') FROM worker.execution_completions WHERE run_id=" + h.quote(run_id))
            acl_failures = h.acl_failures()
            if text == 'memory-pg-fail':
                assert memory_status == [['FAILED']]
                assert any(item['username'] == 'memory_runtime' and item['object'] == 'mset' and item['context'] == 'lua' and item['reason'] == 'command' for item in acl_failures), 'real Apply did not hit the intended Redis MSET ACL denial'
            else:
                assert candidates(h, run_id) == [], 'failed SDK execution staged a Session candidate'
                assert memory_status == [['NONE']], 'failed SDK execution acquired a Memory finalization gate'
            evidence['failures'].append({'run_id': run_id, 'scenario': 'redis_mset_acl_denied' if text == 'memory-pg-fail' else 'model_failure_after_local_tool_write', 'reused_model_input': text, 'delivery': delivery, 'completion': failure, 'memory_status': memory_status, 'storage_before': before, 'storage_after': after, 'acl_failures': acl_failures})
            h.admin_sql(WRITE_RESTORE)
            save()

        follower = success('memory-after-failure')
        assert follower['memory'][0]['revision'] == 4
        assert follower['tool_results'][-1]['results'][0]['memory'] == 'persistent orchid memory'
        evidence['post_failure_follower'] = follower

        primary = follower['memory'][0]
        separate = success('memory-read', chat='43', sender=101)
        assert separate['tool_results'][-1].get('count', 0) == 0 and not separate['tool_results'][-1].get('results')
        assert len(separate['memory']) == 2
        assert primary in separate['memory'], 'other subject changed original accepted Memory'
        new_scope = next(row for row in separate['memory'] if row['scope_id'] != primary['scope_id'])
        assert new_scope['tenant_id'] == h.tenant_id and new_scope['revision'] == 1 and new_scope['content']['entries'] == []
        evidence['scope_checks'].append({'case': 'different_real_lab_sender_and_private_chat_starts_empty', 'original_scope': primary, 'other_subject': separate})
        save()

        before_restart = h.storage_state()
        h.command(['docker', 'restart', h.redis])
        restarted_port = int(h.command(['docker', 'port', h.redis, '6379/tcp']).strip().rsplit(':', 1)[1])
        assert restarted_port == h.redis_port, 'Redis lifecycle changed the published fixed target'
        h.wait(lambda: h.redis_admin('PING') == 'PONG', 'Redis AOF restart readiness')
        after_restart = h.storage_state()
        assert before_restart == after_restart, 'AOF restart lost or altered formal Memory'
        returned = success('memory-read')
        assert returned['tool_results'][-1]['results'][0]['memory'] == 'persistent orchid memory'
        original_now = next(row for row in returned['memory'] if row['scope_id'] == primary['scope_id'])
        assert original_now['revision'] == 5 and new_scope in returned['memory']
        evidence['scope_checks'].append({'case': 'return_to_original_subject_after_redis_restart', 'run': returned})
        evidence['redis_restart'] = {'published_port_before': h.redis_port, 'published_port_after': restarted_port, 'storage_before': before_restart, 'storage_after': after_restart, 'exact_equal': True}
        final_storage = h.storage_state()
        # Six applied Runs: five in the original scope and one in a fresh scope.
        assert len([row for row in final_storage if ':head:' in row['key']]) == 2
        assert len([row for row in final_storage if ':receipt:' in row['key']]) == 6
        assert len([row for row in final_storage if ':attempt:' in row['key']]) == 6
        evidence.update(result='PASS', requests=h.model.snapshot(), final_memory=h.memory_state(), final_storage=final_storage, cross_tenant_boundary='covered by separate real Redis store contract tests, not claimed by this single-tenant Lab fixture')
        save()
    except BaseException as exc:
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL', error=h.redact(str(exc)), error_type=type(exc).__name__, error_location={'file': Path(frame.filename).name, 'line': frame.lineno}, requests=h.model.snapshot() if isinstance(getattr(h, 'model', None), MemoryModelFixture) else [])
        save()
        raise RuntimeError(evidence['error']) from None
    finally:
        try:
            worker_log = h.artifacts / 'worker-one.log'
            if worker_log.exists():
                log = worker_log.read_text()
                values = ('temporary tea memory', 'temporary coffee memory', 'clear must remove this entry', 'persistent orchid memory', 'must not persist memory-fail', 'must not persist memory-pg-fail', 'invalid correction must not persist')
                evidence['worker_log_memory_redaction'] = 'PASS' if not any(text in log for text in values) else 'FAIL'
                if evidence['worker_log_memory_redaction'] != 'PASS':
                    evidence['result'] = 'FAIL'
            h.close()
            evidence['cleanup'] = 'PASS'
        except BaseException as exc:
            evidence.update(result='FAIL', cleanup_error=h.redact(str(exc)))
            raise
        finally:
            save()
            leaked = [str(path) for path in h.artifacts.rglob('*') if path.is_file() and any(secret.encode() in path.read_bytes() for secret in h.secrets if secret)]
            evidence['all_fixture_secrets_scan'] = {'result': 'FAIL' if leaked else 'PASS', 'files': leaked}
            if leaked:
                evidence['result'] = 'FAIL'
            save()
            if evidence['result'] != 'PASS':
                raise RuntimeError('Redis Memory joint did not pass; inspect redacted evidence')
    print('WORKER_MEMORY_REDIS_JOINT=PASS', flush=True)


if __name__ == '__main__':
    main()
