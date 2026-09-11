#!/usr/bin/env python3
"""Real model Memory write, accepted PostgreSQL persistence, later SDK load and Lab Final."""
import argparse
import importlib.util
import json
from pathlib import Path
import secrets
import sys
import tempfile

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from memory_joint_fixture import MemoryHarness, TOOLS
import channel_lab_fixture as gateway_fixture
from faults import wait_success

# Reuse the transparent, byte-preserving provider observation relay unchanged.
_spec = importlib.util.spec_from_file_location('worker_summary_live', Path(__file__).with_name('test-worker-summary-live.py'))
_live = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_live)
LiveRelay, read_key = _live.LiveRelay, _live.read_key


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--key-name', default='deepseekapi')
    parser.add_argument('--model', required=True)
    parser.add_argument('--provider-base', required=True)
    args = parser.parse_args()
    key = read_key(args.env_file, args.key_name)
    h = MemoryHarness(args.root, args.artifacts or Path(tempfile.mkdtemp(prefix='worker-memory-live-evidence-')), model_name=args.model)
    h.secrets.append(key)
    evidence = {'result': 'PENDING', 'provider_base': args.provider_base, 'model': args.model,
                'model_execution': 'REAL_EXTERNAL_PROVIDER', 'im': 'isolated Channel Lab HTTP',
                'real_telegram': 'NOT_RUN', 'web_gui': 'NOT_RUN', 'rounds': []}
    target = h.artifacts / 'memory-live.json'
    print('MEMORY_LIVE_ARTIFACTS=' + str(h.artifacts), flush=True)

    def save():
        target.write_text(h.redact(json.dumps(evidence, indent=2, ensure_ascii=False)) + '\n')
        assert json.loads(target.read_text())['result'] == evidence['result']

    try:
        h.provision()
        h.model.close()
        h.model = LiveRelay(h, key, args.provider_base, args.model)
        h.urls['model'] = h.model.url
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        view = h.api('GET', '/v1/tenants/' + h.tenant_id + '/deployments/' + h.deployment_id + '/revisions/1')['manifest_view']
        assert sorted(view['agent_plan']['nodes']['assistant']['memory']['tools']) == sorted(TOOLS)
        assert view['resources']['storage']['memory']['backend']['kind'] == 'postgresql'
        evidence['manifest_view'] = view
        canary = 'memory-orchid-' + secrets.token_hex(6)
        evidence['canary'] = canary
        prompts = [
            'Use the memory_add tool now to save this exact personal preference: "My preferred travel label is ' + canary + '". After the tool succeeds, acknowledge briefly. Do not merely repeat it without calling the tool.',
            'Use the memory_load tool now to read my stored memories. Then tell me my preferred travel label from the tool result. You must call memory_load even if conversation history contains the label.'
        ]
        for index, text in enumerate(prompts):
            offset = len(h.model.snapshot())
            run_id = h.send_text(text)
            delivery = h.wait_delivery(run_id)
            result = wait_success(h, run_id)
            h.wait(lambda: all(c.get('complete') for c in h.model.snapshot()[offset:]), 'real provider response observation complete')
            calls = h.model.snapshot()[offset:]
            assert len(calls) >= 2, 'provider did not complete a tool-to-final cycle'
            assert all(c['status'] == 200 and c.get('complete') for c in calls)
            assert all(c['request']['max_completion_tokens'] == view['execution']['max_output_tokens'] for c in calls)
            # Only current Run messages after the last user turn prove actual
            # tool execution; historical tool output cannot satisfy this gate.
            current = []
            for call in calls:
                messages = call['request']['messages']
                start = max(i for i, m in enumerate(messages) if m.get('role') == 'user')
                current.extend(messages[start + 1:])
            expected_tool = 'memory_add' if index == 0 else 'memory_load'
            invoked = {tool['id'] for message in current for tool in message.get('tool_calls', []) if tool['function']['name'] == expected_tool}
            tool_results = [m for m in current if m.get('role') == 'tool' and m.get('tool_call_id') in invoked]
            assert invoked and tool_results, 'current run did not execute required SDK tool'
            if index == 1:
                assert any(canary in m.get('content', '') for m in tool_results), 'memory_load did not return persisted canary'
            state = h.memory_state()
            assert len(state) == 1 and canary in json.dumps(state[0]['content']), 'real model write did not reach formal Memory'
            status = h.sql('SELECT memory_status FROM worker.execution_completions WHERE run_id=' + h.quote(run_id))
            assert status == [['APPLIED']], status
            assert delivery['final_text'] == calls[-1]['text'], 'Lab Final differs from actual provider Final'
            if index == 1:
                assert canary in delivery['final_text']
            evidence['rounds'].append({'run_id': run_id, 'input': text, 'calls': calls, 'delivery': delivery,
                                      'completion': result['completion'], 'memory_status': status[0][0], 'memory': state, 'required_tool': expected_tool})
            save()
        evidence.update(result='PASS', real_sdk_write=True, formal_pg_persistence=True, later_run_sdk_load=True,
                        final_after_memory_applied=True, final_matches_live_response=True)
        save()
    except BaseException as exc:
        evidence.update(result='FAIL', error=h.redact(str(exc)), calls=h.model.snapshot() if isinstance(getattr(h, 'model', None), LiveRelay) else [])
        save()
        raise RuntimeError(evidence['error']) from None
    finally:
        try:
            h.close()
            evidence['cleanup'] = 'PASS'
        except BaseException as exc:
            evidence.update(result='FAIL', cleanup_error=h.redact(str(exc)))
            raise RuntimeError(evidence['cleanup_error']) from None
        finally:
            save()
            leaked = [str(path) for path in h.artifacts.rglob('*') if path.is_file() and any(secret.encode() in path.read_bytes() for secret in h.secrets if secret)]
            evidence['credential_leak_scan'] = {'result': 'FAIL' if leaked else 'PASS', 'files': leaked}
            if leaked:
                evidence['result'] = 'FAIL'
            save()
            if leaked:
                raise RuntimeError('API key leak found in artifact paths: ' + ', '.join(leaked))
    print('WORKER_MEMORY_LIVE=PASS', flush=True)


if __name__ == '__main__':
    main()
