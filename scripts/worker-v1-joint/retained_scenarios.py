"""Bound retained Runs, then recover the original unacknowledged broker message.

Capacity is an explicit deployment setting, not a Manifest or Token policy.
This gate never removes history or writes SQL business rows to create a slot.
"""
import hashlib
import json
from pathlib import Path
import time
import uuid

from capacity_scenarios import capacity_observation
from faults import attempts, candidates, completions, model_calls, run, wait_success
from intake_scenarios import (consumer_info, gateway_publication, locate_run_message,
                             receipt, retained, stream_info)


def limits_for_retained(count):
    if type(count) is not int or count < 1:
        raise ValueError('retained fixture requires existing durable Run history')
    return {'limits': {'max_retained_runs': count}}


def run_retained(h):
    h.stop_fault_workers()
    assert h.sql("SELECT count(*) FROM worker.execution_runs WHERE status IN ('QUEUED','RUNNING','RETRY_WAIT')") == [['0']]
    count = int(h.sql('SELECT count(*) FROM worker.execution_runs')[0][0])
    settings = limits_for_retained(count)
    evidence = {'version': 'worker-v1-retained-process/v1', 'result': 'FAIL',
                'limit_applies_to': 'all-status execution_runs only',
                'auxiliary_evidence_tables_bounded': False, 'business_rows_modified_by_test': False}
    path = Path(h.artifacts) / 'worker-retained-scenarios.json'
    marker = 'retained-' + uuid.uuid4().hex[:12]
    conversation = str(2900000000 + int(uuid.uuid4().hex[:7], 16))
    try:
        first = h.start_worker(overrides=settings)
        before = stream_info(h)
        history = h.sql('SELECT tenant_id,run_id,status FROM worker.execution_runs ORDER BY tenant_id,run_id')
        sessions = h.sql('SELECT tenant_id,session_id,next_sequence,settled_sequence FROM worker.execution_sessions ORDER BY tenant_id,session_id')
        run_id = h.send_text(marker, conversation_id=conversation)
        source = h.wait(lambda: locate_run_message(h, run_id, before['state']['last_seq']),
                        'original Run publication before retained intake capacity recovery', timeout=10)
        observation = h.wait(lambda: capacity_observation(h.artifacts / 'worker-one.log', run_id),
                             'all-status retained Run capacity rejects new intake despite empty active queue', timeout=15)
        publication = gateway_publication(h, run_id)
        assert len(publication) == 1 and publication[0][0] == 't'
        for _ in range(3):
            assert h.sql('SELECT tenant_id,run_id,status FROM worker.execution_runs ORDER BY tenant_id,run_id') == history
            assert h.sql('SELECT tenant_id,session_id,next_sequence,settled_sequence FROM worker.execution_sessions ORDER BY tenant_id,session_id') == sessions
            assert receipt(h, run_id) == [] and model_calls(h, marker) == []
            assert retained(h, source['sequence']) == source['raw']
            time.sleep(.1)
        pending = consumer_info(h)
        assert pending['num_ack_pending'] >= 1 and pending['delivered']['stream_seq'] >= source['sequence']
        # A deployment config increase makes room. No history cleanup, new
        # webhook or broker republish is involved in this recovery.
        h.stop_worker()
        second = h.start_worker(overrides=limits_for_retained(count + 1))
        outcome = wait_success(h, run_id)
        delivery = h.wait_delivery(run_id)
        h.wait(lambda: retained(h, source['sequence']) is None,
               'original retained Run sequence ACKs after durable acceptance', timeout=30)
        assert gateway_publication(h, run_id) == publication
        assert h.sql('SELECT count(*) FROM worker.execution_runs') == [[str(count + 1)]]
        assert h.sql('SELECT tenant_id,run_id,status FROM worker.execution_runs WHERE run_id<>' + h.quote(run_id) + ' ORDER BY tenant_id,run_id') == history
        assert len(receipt(h, run_id)) == len(attempts(h, run_id)) == len(candidates(h, run_id)) == len(completions(h, run_id)) == len(model_calls(h, marker)) == 1
        assert run(h, run_id)['session_sequence'] == 1
        evidence.update(result='PASS', invariants=['WV-32'], run_id=run_id,
                        retained_before=count, retained_after=count + 1, active_before=0,
                        configured_limit_before=count, configured_limit_after=count + 1,
                        blocked_observation=observation, original_stream_sequence=source['sequence'],
                        raw_sha256=hashlib.sha256(source['raw']).hexdigest(), pending_consumer=pending,
                        first_worker_pid=first.pid, recovery_worker_pid=second.pid,
                        gateway_publication_before=publication, gateway_publication_after=gateway_publication(h, run_id),
                        completion=outcome['completion'], delivery=delivery,
                        original_history_preserved=True, same_broker_sequence_recovered=True,
                        model_calls=1, webhook_count=1)
        print('WORKER_RETAINED_CAPACITY=PASS active=0; retained history rejects new Run without intake side effects; explicit limit increase recovers original broker sequence once; history preserved; auxiliary evidence caps remain deferred', flush=True)
        return evidence
    finally:
        path.write_text(json.dumps(evidence, indent=2) + '\n')
        assert json.loads(path.read_text()) == evidence
        h.stop_fault_workers()
        # Restore the default explicit fixture configuration for later gates.
        h.start_worker()
