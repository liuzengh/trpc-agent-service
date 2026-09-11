#!/usr/bin/env python3
"""Published bounded Loop -> real SDK iterations -> accepted Session -> Channel Lab.

Fixture: max=2 success, second-iteration HTTP401, then same-Session recovery.
Live: one max=2 Run against the unchanged DeepSeek byte relay. No MCP/embedding.
The only product writes are normal Control and Channel Lab public HTTP requests.
"""
import argparse
import copy
import importlib.util
from pathlib import Path
import sys
import tempfile
import traceback

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from loop_joint_fixture import (LoopHarness, MODEL, NORMAL, FAILURE, RECOVER,
    first_output, require_exchange, require_accepted_history, all_strings)
from faults import submit, run, head, rows, attempts, candidates, completions, outboxes
import channel_lab_fixture as lab

# Keep the already verified JSON-safe Gateway receipt/part/Lab predicate.
_spec = importlib.util.spec_from_file_location('loop_delivery_predicate', Path(__file__).with_name('test-worker-sequence-joint.py'))
_sequence = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_sequence)
require_delivery_wire, FAILURE_FINAL = _sequence.require_delivery_wire, _sequence.FAILURE_FINAL


def require_manifest(envelope, h):
    content = envelope['content']
    plan, resources = content['agent_plan'], content['resources']
    nodes = plan['nodes']
    assert plan['root'] == 'workflow' and set(nodes) == {'workflow', 'assistant'}
    loop, body = nodes['workflow'], nodes['assistant']
    assert loop['kind'] == 'loop' and loop['body'] == 'assistant' and loop['max_iterations'] == 2
    assert not loop.get('children')
    assert body['kind'] == 'llm' and body['model_resource'] == 'primary'
    assert 'LOOP_ASSISTANT_NODE:' in body['instruction']
    for field in ('tool_resources', 'knowledge_resources', 'callable_entries', 'memory', 'artifact', 'add_session_summary'):
        assert not body.get(field), 'Loop fixture unexpectedly enabled another capability'
    assert set(resources['models']) == {'primary'}
    model = resources['models']['primary']
    assert model['base_url'] == h.model.url + '/v1' and model['model'] == h.model_name
    assert model['credential']['purpose'] == 'api_key' and model['credential']['credential_id']
    assert not resources.get('tools') and not resources.get('knowledge')
    return content


def require_facts(round_, previous):
    state, after = round_['run'], round_['head_after']
    comps, attempt_rows, finals = round_['completions'], round_['attempts'], round_['outboxes']
    delivery, exchange = round_['delivery'], round_['exchange']
    assert state['attempts'] == len(attempt_rows) == len(comps) == len(finals) == 1
    completion, attempt, final = comps[0], attempt_rows[0], finals[0]
    assert completion['kind'] == 'ATTEMPT' and completion['reply_disposition'] == 'FINAL'
    assert completion['attempt_id'] == attempt['attempt_id'] == state['current_attempt_id']
    assert attempt['agent_started_at'] is not None and attempt['ended_at'] is not None
    assert completion['final_intent_id'] == final['intent_id'] == delivery['intent_id']
    assert final['payload']['execution']['completion_id'] == completion['completion_id']
    assert final['payload']['execution']['attempt_id'] == attempt['attempt_id']
    assert final['payload']['content']['text'] == delivery['final_text']
    assert delivery['delivery_state'] == 'ACCEPTED' and delivery['run_id'] == state['run_id']
    assert after['settled_sequence'] == state['session_sequence']
    if round_['input'] == FAILURE:
        assert state['status'] == attempt['status'] == completion['status'] == 'FAILED'
        assert attempt['reason'] == completion['reason'] == 'RUNTIME_FAILED'
        assert not round_['candidates'] and not completion['candidate_ref'] and not completion['candidate_digest']
        assert delivery['final_text'] == FAILURE_FINAL and exchange['terminal_failure']
        assert exchange['terminal_output'] is None
        for field in ('accepted_ref', 'accepted_digest'):
            assert after[field] == round_['head_before'][field]
        assert after['settled_sequence'] == round_['head_before']['settled_sequence'] + 1
        return None
    assert state['status'] == attempt['status'] == completion['status'] == 'SUCCEEDED'
    assert not exchange['terminal_failure'] and len(round_['candidates']) == 1
    candidate = round_['candidates'][0]
    assert candidate['candidate_ref'] == completion['candidate_ref'] == after['accepted_ref']
    assert candidate['content_digest'] == completion['candidate_digest'] == after['accepted_digest']
    assert candidate['attempt_id'] == attempt['attempt_id']
    assert delivery['final_text'] == exchange['terminal_output'] != exchange['first_output']
    require_accepted_history(previous, candidate, [c['request'] for c in round_['model_calls']])
    stored = list(all_strings(candidate['content']['snapshot']))
    assert exchange['first_output'] in stored and exchange['terminal_output'] in stored
    return candidate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--race', action='store_true')
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--env-file', type=Path, default=Path('/Users/jfs/Projects/trpc-agent-service/.env'))
    args = parser.parse_args()
    h = LoopHarness(args.root, args.artifacts or Path(tempfile.mkdtemp(prefix='worker-loop-joint-')),
        args.race, live=args.live, model_name='deepseek-v4-flash' if args.live else MODEL)
    evidence = {'result': 'PENDING', 'model': 'REAL_DEEPSEEK_UNMODIFIED_BYTES' if args.live else 'DETERMINISTIC_HTTP_FIXTURE',
        'mcp': 'NOT_USED', 'embedding': 'NOT_USED', 'im': 'REAL_CHANNEL_LAB',
        'business_sql_writes': False, 'rounds': []}
    def save():
        h.record('loop-joint.json', evidence)
    try:
        h.provision()
        h.prepare_loop(args.env_file if args.live else None)
        h.control_start(lab.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = lab.start(h)
        envelope = rows(h, "SELECT convert_from(envelope,'UTF8')::json AS manifest FROM worker.runtime_manifests WHERE manifest_id=" + h.quote(h.manifest_id))[0]['manifest']
        content = require_manifest(envelope, h)
        evidence['manifest'] = envelope
        previous, prior_state, failed_round = None, None, None
        for case in ([NORMAL] if args.live else [NORMAL, FAILURE, RECOVER]):
            offset = len(h.model.snapshot())
            before = head(h, prior_state) if prior_state else None
            rid = submit(h, case, '42')
            h.wait(lambda: run(h, rid)['status'] in ('SUCCEEDED', 'FAILED'), 'Loop Run reaches formal terminal outcome', timeout=90)
            state = run(h, rid)
            round_ = {'input': case, 'run_id': rid, 'run': state, 'attempts': attempts(h, rid),
                'completions': completions(h, rid), 'outboxes': outboxes(h, rid), 'candidates': candidates(h, rid),
                'head_before': before, 'head_after': head(h, state), 'result': 'PENDING'}
            evidence['rounds'].append(round_)
            save()
            # Concrete response/handler observations, not a delay interpreted as
            # cancellation. The SDK cancellation submatrix is a separate test.
            h.wait(lambda: bool(h.model.snapshot()[offset:]) and all(c.get('complete') and c.get('finished_ns')
                for c in h.model.snapshot()[offset:]), 'all observed iteration HTTP handlers exited', timeout=20)
            assert h.model.wait_idle(5), 'model handler remains active after terminal Run'
            round_['model_calls'] = h.model.snapshot()[offset:]
            round_['route'] = rows(h, "SELECT request_json->'Route' AS route FROM worker.execution_runs WHERE run_id=" + h.quote(rid))[0]['route']
            save()
            limit = (content['agent_plan']['nodes']['assistant'].get('generation') or {}).get('max_output_tokens', content['execution']['max_output_tokens'])
            route = round_['route']
            assert route['ManifestRef'] == h.manifest_id and route['ManifestDigest'] == h.manifest_digest
            assert route['DeploymentRevisionID'] == h.revision_id
            # FAILED still traverses the real fixed-failure Final and Gateway.
            round_['delivery'] = h.gateway.wait_delivery(rid)
            round_['gateway_receipts'] = rows(h, 'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id=' + h.quote(rid))
            round_['gateway_parts'] = rows(h, 'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=' + h.quote(rid) + ' ORDER BY p.part_index')
            save()
            # Capture real delivery even when the Provider later fails this
            # scenario's extra verbatim-copy instruction. Keep that assertion
            # strict; it is distinct from the generic Loop execution contract.
            round_['exchange'] = require_exchange(round_['model_calls'], case, live=args.live, limit=limit)
            candidate = require_facts(round_, previous)
            require_delivery_wire(round_)
            if prior_state:
                assert state['session_id'] == prior_state['session_id']
                assert state['session_sequence'] == prior_state['session_sequence'] + 1
            if case == FAILURE:
                late = {'run': run(h, rid), 'attempts': attempts(h, rid), 'completions': completions(h, rid),
                    'outboxes': outboxes(h, rid), 'candidates': candidates(h, rid), 'head_after': head(h, state)}
                old = candidates(h, previous['run_id'])
                round_.update(facts_after_all_model_http_exit=late, prior_accepted_candidate_after_failure=old)
                save()
                assert all(late[key] == round_[key] for key in late)
                assert not late['candidates'] and old == [previous['candidate']]
            else:
                if case == RECOVER:
                    for raw in all_strings([round_['model_calls'][0]['request']['messages'], candidate['content']]):
                        assert FAILURE not in raw and first_output(FAILURE) not in raw and FAILURE_FINAL not in raw
                    failed = {'attempts': attempts(h, failed_round['run_id']), 'completions': completions(h, failed_round['run_id']),
                        'outboxes': outboxes(h, failed_round['run_id']), 'candidates': candidates(h, failed_round['run_id'])}
                    round_['failed_facts_after_recovery'] = failed
                    save()
                    assert all(failed[key] == failed_round[key] for key in failed)
                previous = {'run': state, 'run_id': rid, 'candidate': candidate, 'delivery': round_['delivery']}
            prior_state = state
            round_['result'] = 'PASS'
            if case == FAILURE:
                failed_round = copy.deepcopy(round_)
            save()
        assert len(h.model.snapshot()) == (2 if args.live else 6), 'late/unplanned model call'
        evidence.update(result='PASS', provider_base=getattr(h, 'provider_base', None), model_calls=h.model.snapshot())
        save()
    except BaseException as exc:
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL', error=h.redact(str(exc)), error_type=type(exc).__name__,
            location={'file': Path(frame.filename).name, 'line': frame.lineno})
        try:
            evidence['model_calls_at_failure'] = h.model.snapshot()
        except Exception:
            pass
        save()
        raise
    finally:
        try:
            h.close()
            evidence['cleanup'] = 'PASS'
        except BaseException as exc:
            evidence.update(result='FAIL', cleanup_error=h.redact(str(exc)))
            raise
        finally:
            save()
            leaks = [str(path) for path in h.artifacts.rglob('*') if path.is_file()
                and any(secret.encode() in path.read_bytes() for secret in h.secrets if secret)]
            evidence['secret_scan'] = {'result': 'FAIL' if leaks else 'PASS', 'files': leaks}
            if leaks:
                evidence['result'] = 'FAIL'
            save()
            if evidence['result'] != 'PASS':
                raise RuntimeError('Loop joint gate did not pass; inspect retained evidence')
    print('WORKER_LOOP_JOINT=PASS mode=' + ('REAL_DEEPSEEK' if args.live else 'FIXTURE') +
        ' maxIterations=2 lastIterationOnly=true formalSession=true lab=true cleanup=PASS', flush=True)


if __name__ == '__main__':
    main()
