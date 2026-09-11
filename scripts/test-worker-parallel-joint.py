#!/usr/bin/env python3
"""Published parallel branches plus explicit aggregator through real SDK and Lab.

Fixture: two observed overlapping Runs with opposite branch completion order,
then branch401 plus observed sibling HTTP cancellation and settled fact readback.
Live: one unchanged DeepSeek sequence, observed overlap only (no live barrier).
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
from parallel_joint_fixture import (ParallelHarness, BRANCHES, ROLES, SLOTS, TOOLS, MODELS,
    A_FIRST, B_FIRST, FAILURE, require_exchange, require_accepted_history, all_strings)
from faults import submit, run, head, rows, attempts, candidates, completions, outboxes
import channel_lab_fixture as lab

# Reuse the already verified raw Gateway receipt/part/Lab equality predicate.
_spec = importlib.util.spec_from_file_location('sequence_delivery_predicate', Path(__file__).with_name('test-worker-sequence-joint.py'))
_sequence = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_sequence)
require_delivery_wire = _sequence.require_delivery_wire
FAILURE_FINAL = _sequence.FAILURE_FINAL


def require_manifest(envelope, h):
    content = envelope['content']; plan = content['agent_plan']; nodes = plan['nodes']
    assert plan['root'] == 'workflow'
    assert nodes['workflow']['kind'] == 'sequence' and nodes['workflow']['children'] == ['research', 'aggregator']
    assert nodes['research']['kind'] == 'parallel' and nodes['research']['children'] == list(BRANCHES)
    models = content['resources']['models']
    assert set(models) == set(SLOTS.values())
    assert len({model['credential']['credential_id'] for model in models.values()}) == 3
    assert {model['base_url'] for model in models.values()} == {h.model.url + '/v1'}
    for role in ROLES:
        node = nodes[role]
        assert node['kind'] == 'llm' and node['model_resource'] == SLOTS[role]
        if role in BRANCHES:
            assert node['tool_resources'] == [TOOLS[role]] and node['callable_entries'] == ['tools/' + TOOLS[role]]
        else:
            assert not node.get('tool_resources') and not node.get('callable_entries')
    resources = content['resources']['tools']
    assert set(resources) == set(TOOLS.values())
    for resource in resources.values():
        assert resource['server_url'] == h.mcp_url and resource['tool_name'] == 'selected_search'
        assert resource['auth']['kind'] == 'bearer' and resource['auth']['credential']['purpose'] == 'bearer_token'
    return content


def require_facts(round_, previous):
    state, after = round_['run'], round_['head_after']
    comp, attempt_rows, finals = round_['completions'], round_['attempts'], round_['outboxes']
    delivery, exchange = round_['delivery'], round_['exchange']
    assert state['attempts'] == len(attempt_rows) == len(comp) == len(finals) == 1
    completion, attempt, final = comp[0], attempt_rows[0], finals[0]
    assert completion['kind'] == 'ATTEMPT' and completion['reply_disposition'] == 'FINAL'
    assert completion['attempt_id'] == attempt['attempt_id'] == state['current_attempt_id']
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
        assert delivery['final_text'] == FAILURE_FINAL
        for field in ('accepted_ref', 'accepted_digest'):
            assert after[field] == round_['head_before'][field]
        assert exchange['branch_failure'] and exchange['sibling_http_termination'] == 'client_disconnect'
        return None
    assert state['status'] == attempt['status'] == completion['status'] == 'SUCCEEDED'
    assert len(round_['candidates']) == 1
    candidate = round_['candidates'][0]
    assert candidate['candidate_ref'] == completion['candidate_ref'] == after['accepted_ref']
    assert candidate['content_digest'] == completion['candidate_digest'] == after['accepted_digest']
    assert candidate['attempt_id'] == attempt['attempt_id']
    assert delivery['final_text'] == exchange['terminal_output']
    assert all(delivery['final_text'] != text for text in exchange['branch_outputs'].values())
    require_accepted_history(previous, candidate, [c['request'] for c in round_['model_calls']])
    stored = list(all_strings(candidate['content']['snapshot']))
    assert all(value in stored for value in [*exchange['branch_outputs'].values(), exchange['terminal_output']])
    return candidate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--race', action='store_true')
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--env-file', type=Path, default=Path('/Users/jfs/Projects/trpc-agent-service/.env'))
    args = parser.parse_args()
    h = ParallelHarness(args.root, args.artifacts or Path(tempfile.mkdtemp(prefix='worker-parallel-joint-')),
        args.race, live=args.live, model_name='deepseek-v4-flash' if args.live else MODELS['research_a'])
    evidence = {'result': 'PENDING', 'model': 'REAL_DEEPSEEK_UNMODIFIED_BYTES' if args.live else 'DETERMINISTIC_HTTP_FIXTURE',
        'mcp_server': 'ACTUAL_TRPC_MCP_GO_V0.0.10', 'im': 'REAL_CHANNEL_LAB', 'embedding': 'NOT_USED',
        'live_timing_boundary': 'relay_http_handler_monotonic intervals, not provider internal inference',
        'fixture_barriers_only': True, 'business_sql_writes': False, 'rounds': []}
    def save(): h.record('parallel-joint.json', evidence)
    try:
        h.provision(); h.prepare_parallel(args.env_file if args.live else None)
        h.control_start(lab.prepare(h)); h.seed(); h.start_worker(); h.verify_dependencies(); h.gateway = lab.start(h)
        envelope = rows(h, "SELECT convert_from(envelope,'UTF8')::json AS manifest FROM worker.runtime_manifests WHERE manifest_id=" + h.quote(h.manifest_id))[0]['manifest']
        content = require_manifest(envelope, h); evidence['manifest'] = envelope
        assert h.mcp_state()['registered_tools'] == ['selected_search', 'unselected_secret']
        previous = None
        for case in ([A_FIRST] if args.live else [A_FIRST, B_FIRST, FAILURE]):
            offset, server_offset = len(h.model.snapshot()), len(h.mcp_state()['events'])
            before = head(h, previous['run']) if previous else None
            rid = submit(h, case, '42')
            h.wait(lambda: run(h, rid)['status'] in ('SUCCEEDED', 'FAILED'), 'parallel Run reaches formal terminal outcome', timeout=90)
            state = run(h, rid)
            round_ = {'input': case, 'run_id': rid, 'run': state, 'attempts': attempts(h, rid),
                'completions': completions(h, rid), 'outboxes': outboxes(h, rid), 'candidates': candidates(h, rid),
                'head_before': before, 'head_after': head(h, state), 'result': 'PENDING'}
            evidence['rounds'].append(round_); save()
            # This waits for the concrete observed request records, not a grace
            # sleep. On failure the sibling must have really closed its socket.
            h.wait(lambda: bool(h.model.snapshot()[offset:]) and all(c.get('complete') and c.get('finished_ns')
                for c in h.model.snapshot()[offset:]), 'all observed model HTTP requests exited', timeout=20)
            round_['model_calls'] = h.model.snapshot()[offset:]
            round_['mcp_events'] = h.mcp_state()['events'][server_offset:]
            round_['route'] = rows(h, "SELECT request_json->'Route' AS route FROM worker.execution_runs WHERE run_id=" + h.quote(rid))[0]['route']
            save()
            round_['exchange'] = require_exchange(round_['model_calls'], case, live=args.live, limit=content['execution']['max_output_tokens'])
            executed = [event for event in round_['mcp_events'] if event['method'] == 'tool_execution']
            assert len(executed) == (0 if case == FAILURE else 2)
            assert all(event['authenticated'] is True and event['name'] == 'selected_search'
                and event['query'] == 'orchid' and not event.get('is_error') for event in executed)
            route = round_['route']
            assert route['ManifestRef'] == h.manifest_id and route['ManifestDigest'] == h.manifest_digest
            assert route['DeploymentRevisionID'] == h.revision_id
            round_['delivery'] = h.gateway.wait_delivery(rid)
            round_['gateway_receipts'] = rows(h, 'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id=' + h.quote(rid))
            round_['gateway_parts'] = rows(h, 'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=' + h.quote(rid) + ' ORDER BY p.part_index')
            save()
            candidate = require_facts(round_, previous); require_delivery_wire(round_)
            if previous:
                assert state['session_id'] == previous['run']['session_id']
                assert state['session_sequence'] == previous['run']['session_sequence'] + 1
            if case == FAILURE:
                late = {'run': run(h, rid), 'attempts': attempts(h, rid), 'completions': completions(h, rid),
                        'outboxes': outboxes(h, rid), 'candidates': candidates(h, rid), 'head_after': head(h, state)}
                assert all(late[key] == round_[key] for key in late)
                assert not late['candidates']
                old = candidates(h, previous['run_id'])
                assert old == [previous['candidate']]
                round_.update(facts_after_all_model_http_exit=late, prior_accepted_candidate_after_failure=old)
            else:
                previous = {'run': state, 'run_id': rid, 'candidate': candidate, 'delivery': round_['delivery']}
            round_['result'] = 'PASS'; save()
        evidence.update(result='PASS', mcp_state=h.mcp_state(), provider_base=getattr(h, 'provider_base', None)); save()
    except BaseException as exc:
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL', error=h.redact(str(exc)), error_type=type(exc).__name__,
            location={'file': Path(frame.filename).name, 'line': frame.lineno})
        try:
            evidence['model_calls_at_failure'] = h.model.snapshot()
            if hasattr(h, 'mcp_url'): evidence['mcp_state_at_failure'] = h.mcp_state()
        except Exception: pass
        save(); raise
    finally:
        try:
            h.close(); evidence['cleanup'] = 'PASS'
        except BaseException as exc:
            evidence.update(result='FAIL', cleanup_error=h.redact(str(exc))); raise
        finally:
            save()
            leaks = [str(path) for path in h.artifacts.rglob('*') if path.is_file()
                     and any(secret.encode() in path.read_bytes() for secret in h.secrets if secret)]
            evidence['secret_scan'] = {'result': 'FAIL' if leaks else 'PASS', 'files': leaks}
            if leaks: evidence['result'] = 'FAIL'
            save()
            if evidence['result'] != 'PASS': raise RuntimeError('Parallel joint gate did not pass; inspect retained evidence')
    print('WORKER_PARALLEL_JOINT=PASS mode=' + ('REAL_DEEPSEEK' if args.live else 'FIXTURE') +
        ' overlap=true explicitAggregator=true terminalOnly=true formalSession=true lab=true cleanup=PASS', flush=True)


if __name__ == '__main__': main()
