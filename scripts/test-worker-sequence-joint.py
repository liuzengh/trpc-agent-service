#!/usr/bin/env python3
"""Real public nested sequence -> per-leaf SDK -> accepted Session -> Channel Lab.

Default: normal, terminal HTTP 401, then same-Session recovery (three Runs).
--live: one real DeepSeek Run, using the existing unmodified-byte relay. Only
fixture-owned services are started; this command never changes production SQL.
"""
import argparse
import copy
import json
from pathlib import Path
import sys
import tempfile
import traceback

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from sequence_joint_fixture import (SequenceHarness, RESEARCH_MODEL, NORMAL, FAILURE,
    RECOVER, SELECTED, stage_one, require_exchange, require_accepted_history, all_strings)
from faults import submit, run, head, rows, attempts, candidates, completions, outboxes
import channel_lab_fixture as lab

FAILURE_FINAL = '本次执行未完成，请稍后重试。'


def require_manifest(envelope, h):
    content = envelope['content']
    plan = content['agent_plan']
    nodes = plan['nodes']
    assert plan['root'] == 'workflow'
    assert nodes['workflow']['kind'] == nodes['prepare']['kind'] == 'sequence'
    assert nodes['workflow']['children'] == ['prepare', 'writer']
    assert nodes['prepare']['children'] == ['researcher']
    assert nodes['researcher']['kind'] == nodes['writer']['kind'] == 'llm'
    assert nodes['researcher']['model_resource'] == 'primary'
    assert nodes['writer']['model_resource'] == 'writer'
    assert nodes['researcher']['tool_resources'] == ['search']
    assert nodes['researcher']['callable_entries'] == ['tools/search']
    assert not nodes['writer'].get('tool_resources') and not nodes['writer'].get('callable_entries')
    models = content['resources']['models']
    assert set(models) == {'primary', 'writer'}
    assert models['primary']['base_url'] == models['writer']['base_url'] == h.model.url + '/v1'
    assert models['primary']['credential']['credential_id'] != models['writer']['credential']['credential_id']
    resource = content['resources']['tools']['search']
    assert resource['server_url'] == h.mcp_url and resource['tool_name'] == SELECTED
    assert resource['auth']['credential']['purpose'] == 'bearer_token'
    assert resource['auth']['credential']['credential_id'] not in {
        value['credential']['credential_id'] for value in models.values()}
    return content


def require_facts(round_, previous):
    state, attempt_rows = round_['run'], round_['attempts']
    completion_rows, finals, candidate_rows = round_['completions'], round_['outboxes'], round_['candidates']
    delivery, after = round_['delivery'], round_['head_after']
    assert state['attempts'] == 1 and len(attempt_rows) == 1
    assert len(completion_rows) == len(finals) == 1
    completion, attempt = completion_rows[0], attempt_rows[0]
    assert completion['kind'] == 'ATTEMPT' and completion['reply_disposition'] == 'FINAL'
    assert completion['attempt_id'] == attempt['attempt_id'] == state['current_attempt_id']
    assert completion['final_intent_id'] == finals[0]['intent_id'] == delivery['intent_id']
    assert delivery['delivery_state'] == 'ACCEPTED' and delivery['run_id'] == state['run_id']
    assert after['settled_sequence'] == state['session_sequence']
    if round_['input'] == FAILURE:
        assert state['status'] == attempt['status'] == completion['status'] == 'FAILED'
        assert attempt['reason'] == completion['reason'] == 'RUNTIME_FAILED'
        assert attempt['agent_started_at'] is not None and attempt['ended_at'] is not None
        assert not candidate_rows and not completion['candidate_ref'] and not completion['candidate_digest']
        assert delivery['final_text'] == FAILURE_FINAL
        assert round_['head_before'] is not None
        for field in ('accepted_ref', 'accepted_digest'):
            assert after[field] == round_['head_before'][field]
        assert round_['head_before']['settled_sequence'] + 1 == after['settled_sequence']
        return None
    assert state['status'] == attempt['status'] == completion['status'] == 'SUCCEEDED'
    assert len(candidate_rows) == 1
    candidate = candidate_rows[0]
    assert candidate['attempt_id'] == attempt['attempt_id']
    assert candidate['candidate_ref'] == completion['candidate_ref'] == after['accepted_ref']
    assert candidate['content_digest'] == completion['candidate_digest'] == after['accepted_digest']
    assert delivery['final_text'] == round_['exchange']['terminal_output']
    assert delivery['final_text'] != round_['exchange']['first_output']
    require_accepted_history(previous, candidate, [c['request'] for c in round_['model_calls']])
    return candidate


def require_delivery_wire(round_):
    receipts, parts, delivery = round_['gateway_receipts'], round_['gateway_parts'], round_['delivery']
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt['outcome'] == 'ACCEPTED' and receipt['reason'] == ''
    assert receipt['run_id'] == round_['run_id'] and receipt['intent_id'] == delivery['intent_id']
    assert parts and all(part['state'] == 'ACCEPTED' for part in parts)
    assert ''.join(part['body'] for part in parts) == delivery['final_text']
    assert len(delivery['outgoing_added']) == len(parts)
    assert ''.join(message['text'] for message in delivery['outgoing_added']) == delivery['final_text']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--race', action='store_true')
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--env-file', type=Path, default=Path('/Users/jfs/Projects/trpc-agent-service/.env'))
    args = parser.parse_args()
    h = SequenceHarness(args.root, args.artifacts or Path(tempfile.mkdtemp(prefix='worker-sequence-joint-')),
        args.race, live=args.live, model_name='deepseek-v4-flash' if args.live else RESEARCH_MODEL)
    evidence = {'result': 'PENDING', 'model': 'REAL_DEEPSEEK_UNMODIFIED_BYTES' if args.live else 'DETERMINISTIC_HTTP_FIXTURE',
        'mcp_server': 'ACTUAL_TRPC_MCP_GO_V0.0.10', 'im': 'REAL_CHANNEL_LAB',
        'embedding': 'NOT_USED', 'business_sql_writes': False, 'rounds': []}
    def save():
        h.record('sequence-joint.json', evidence)
    try:
        h.provision()
        h.prepare_sequence(args.env_file if args.live else None)
        h.control_start(lab.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = lab.start(h)
        envelope = rows(h, "SELECT convert_from(envelope,'UTF8')::json AS manifest FROM worker.runtime_manifests WHERE manifest_id=" + h.quote(h.manifest_id))[0]['manifest']
        content = require_manifest(envelope, h)
        assert h.mcp_state()['registered_tools'] == ['selected_search', 'unselected_secret']
        evidence['manifest'] = envelope
        previous, failed_round = None, None
        for text in ([NORMAL] if args.live else [NORMAL, FAILURE, RECOVER]):
            offset, server_offset = len(h.model.snapshot()), len(h.mcp_state()['events'])
            before = head(h, previous['run']) if previous else None
            rid = submit(h, text, '42')
            h.wait(lambda: run(h, rid)['status'] in ('SUCCEEDED', 'FAILED'), 'formal sequence terminal Run', timeout=90)
            state = run(h, rid)
            round_ = {'input': text, 'run_id': rid, 'run': state, 'attempts': attempts(h, rid),
                'head_before': before, 'head_after': head(h, state), 'completions': completions(h, rid),
                'outboxes': outboxes(h, rid), 'candidates': candidates(h, rid), 'result': 'PENDING'}
            evidence['rounds'].append(round_)
            save()
            h.wait(lambda: all(c.get('complete') for c in h.model.snapshot()[offset:]), 'provider observation completion')
            round_['model_calls'] = h.model.snapshot()[offset:]
            round_['mcp_events'] = h.mcp_state()['events'][server_offset:]
            round_['route'] = rows(h, "SELECT request_json->'Route' AS route FROM worker.execution_runs WHERE run_id=" + h.quote(rid))[0]['route']
            save()
            round_['exchange'] = require_exchange(round_['model_calls'], text, live=args.live, limit=content['execution']['max_output_tokens'])
            executed = [event for event in round_['mcp_events'] if event['method'] == 'tool_execution']
            assert len(executed) == 1 and executed[0]['name'] == SELECTED
            assert executed[0]['authenticated'] is True and executed[0]['query'] == 'orchid' and not executed[0].get('is_error')
            route = round_['route']
            assert route['ManifestRef'] == h.manifest_id and route['ManifestDigest'] == h.manifest_digest
            assert route['DeploymentRevisionID'] == h.revision_id
            # A failed Run still has a real, fixed failed Final; bypass only the
            # base helper's success expectation, not Gateway delivery checking.
            round_['delivery'] = h.gateway.wait_delivery(rid)
            round_['gateway_receipts'] = rows(h, 'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id=' + h.quote(rid))
            round_['gateway_parts'] = rows(h, 'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=' + h.quote(rid) + ' ORDER BY p.part_index')
            save()
            candidate = require_facts(round_, previous)
            require_delivery_wire(round_)
            if previous:
                assert state['session_id'] == previous['run']['session_id']
            if text == FAILURE:
                assert state['session_sequence'] == previous['run']['session_sequence'] + 1
                failed_round = copy.deepcopy(round_)
                # Read accepted immutable bytes again; no failed leaf output can
                # enter the previously accepted snapshot.
                old = candidates(h, previous['run_id'])
                assert old == [previous['candidate']]
                round_['prior_accepted_candidate_after_failure'] = old
            else:
                if text == RECOVER:
                    assert state['session_sequence'] == failed_round['run']['session_sequence'] + 1
                    for raw in all_strings([round_['model_calls'][0]['request']['messages'], candidate['content']]):
                        assert FAILURE not in raw and stage_one(FAILURE) not in raw and FAILURE_FINAL not in raw
                    settled_failed = {'attempts': attempts(h, failed_round['run_id']),
                        'completions': completions(h, failed_round['run_id']),
                        'outboxes': outboxes(h, failed_round['run_id']), 'candidates': candidates(h, failed_round['run_id'])}
                    assert all(settled_failed[key] == failed_round[key] for key in settled_failed)
                    round_['failed_facts_after_recovery'] = settled_failed
                previous = {'run': state, 'run_id': rid, 'candidate': candidate, 'delivery': round_['delivery']}
            round_['result'] = 'PASS'
            save()
        evidence.update(result='PASS', mcp_state=h.mcp_state(), provider_base=getattr(h, 'provider_base', None))
        save()
    except BaseException as exc:
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        evidence.update(result='FAIL', error=h.redact(str(exc)), error_type=type(exc).__name__,
                        location={'file': Path(frame.filename).name, 'line': frame.lineno})
        try:
            evidence['model_calls_at_failure'] = h.model.snapshot()
            if hasattr(h, 'mcp_url'):
                evidence['mcp_state_at_failure'] = h.mcp_state()
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
                raise RuntimeError('Sequence joint gate did not pass; inspect retained evidence')
    print('WORKER_SEQUENCE_JOINT=PASS mode=' + ('REAL_DEEPSEEK' if args.live else 'FIXTURE') +
          ' nested=true ordered=true terminalOnly=true formalSession=true lab=true cleanup=PASS', flush=True)


if __name__ == '__main__':
    main()
