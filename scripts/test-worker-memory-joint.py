#!/usr/bin/env python3
"""Real Control/Worker/Memory PostgreSQL/Lab acceptance; model HTTP is deterministic."""
import argparse
import json
from pathlib import Path
import sys
import tempfile

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from memory_joint_fixture import MemoryHarness, MemoryModelFixture, TOOLS
import channel_lab_fixture as gateway_fixture
from faults import run, head, candidates, completions, wait_success


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    h = MemoryHarness(args.root, args.artifacts or Path(tempfile.mkdtemp(prefix='worker-memory-joint-')), args.race)
    evidence = {'result': 'PENDING', 'model': 'DETERMINISTIC_HTTP_FIXTURE', 'im': 'REAL_ISOLATED_CHANNEL_LAB', 'rounds': [], 'failures': []}
    target = h.artifacts / 'memory-joint.json'
    print('MEMORY_JOINT_ARTIFACTS=' + str(h.artifacts), flush=True)
    def save():
        target.write_text(h.redact(json.dumps(evidence, indent=2)) + '\n')
        assert json.loads(target.read_text())['result'] == evidence['result']
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
        evidence['manifest_view'] = publication['manifest_view']
        assert sorted(evidence['manifest_view']['agent_plan']['nodes']['assistant']['memory']['tools']) == sorted(TOOLS)
        for text in ('memory-six', 'memory-read', 'memory-correct'):
            run_id = h.send_text(text)
            delivery = h.wait_delivery(run_id)
            result = wait_success(h, run_id)
            accepted = head(h, run(h, run_id))
            assert accepted['accepted_ref'] == result['candidate']['candidate_ref']
            assert delivery['final_text'] == 'memory final: ' + text
            state = h.memory_state()
            assert len(state) == 1 and state[0]['revision'] == len(evidence['rounds']) + 1
            entries = state[0]['content']['entries']
            assert len(entries) == 1 and entries[0]['memory']['memory'] == 'persistent orchid memory'
            output = next(x for x in h.model.outputs if x['input'] == text)
            assert output['tool_results'][-1]['results'][0]['memory'] == 'persistent orchid memory'
            evidence['rounds'].append({'run_id': run_id, 'delivery': delivery, 'session_head': accepted, 'memory': state, 'tool_results': output['tool_results']})
            save()
        for text in ('memory-fail', 'memory-pg-fail'):
            before = h.memory_state()
            run_id = h.send_text(text)
            expected_status = 'FAILED' if text == 'memory-fail' else 'SUCCEEDED'
            h.wait(lambda: h.sql('SELECT status FROM worker.execution_runs WHERE run_id=' + h.quote(run_id)) == [[expected_status]], 'Memory failure terminal', timeout=90)
            delivery = h.gateway.wait_delivery(run_id)
            assert 'memory final:' not in delivery['final_text']
            assert h.memory_state() == before
            failure = completions(h, run_id)
            assert failure and failure[-1]['status'] == expected_status
            memory_status = h.sql('SELECT memory_status FROM worker.execution_completions WHERE run_id=' + h.quote(run_id))
            if text == 'memory-pg-fail':
                assert memory_status == [['FAILED']]
                assert delivery['final_text'] == '本次执行未完成，请稍后重试。'
            if text == 'memory-fail':
                assert candidates(h, run_id) == []
            evidence['failures'].append({'run_id': run_id, 'scenario': text, 'delivery': delivery, 'completion': failure, 'memory_status': memory_status, 'memory_unchanged': True})
            h.admin_sql('GRANT INSERT ON runtime_memory.memory_receipts TO memory_runtime')
            save()
        run_id = h.send_text('memory-after-failure')
        delivery = h.wait_delivery(run_id)
        wait_success(h, run_id)
        assert delivery['final_text'] == 'memory final: memory-after-failure'
        evidence.update(result='PASS', next_run_unblocked=run_id, requests=h.model.snapshot(), final_memory=h.memory_state())
        save()
    except BaseException as exc:
        evidence.update(result='FAIL', error=h.redact(str(exc)), requests=h.model.snapshot() if isinstance(getattr(h, 'model', None), MemoryModelFixture) else [])
        save()
        raise RuntimeError(evidence['error']) from None
    finally:
        try:
            worker_log = h.artifacts / 'worker-one.log'
            if worker_log.exists():
                log = worker_log.read_text()
                memory_texts = ('temporary tea memory', 'temporary coffee memory', 'clear must remove this entry', 'persistent orchid memory', 'must not persist memory-fail', 'must not persist memory-pg-fail', 'invalid correction must not persist')
                evidence['worker_log_memory_redaction'] = 'PASS' if not any(text in log for text in memory_texts) else 'FAIL'
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
                raise RuntimeError('Memory joint did not pass; inspect redacted evidence')
    print('WORKER_MEMORY_JOINT=PASS', flush=True)


if __name__ == '__main__':
    main()
