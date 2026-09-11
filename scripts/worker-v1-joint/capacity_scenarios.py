"""Actual Worker admission backpressure and recovery, without SQL state writes."""
import json
from pathlib import Path
import time
import uuid
from faults import attempts, candidates, completions, model_calls, run, submit, wait_success


def capacity_observation(path, run_id):
    if not Path(path).is_file():
        return None
    for line in Path(path).read_text().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get('operation') == 'intake' and row.get('result') == 'capacity' and row.get('run_id') == run_id:
            return {k: row[k] for k in ('operation', 'result', 'run_id')}
    return None


def run_capacity(h):
    h.stop_fault_workers()
    h.start_worker('worker-one', {'limits': {'max_queued_runs': 1, 'max_active_attempts': 1, 'scan_batch': 1}})
    marker = 'capacity-' + uuid.uuid4().hex[:12]
    first_text, second_text = marker + '-holder', marker + '-pending'
    conversation = str(1800000000 + int(uuid.uuid4().hex[:7], 16))
    evidence = {'version': 'worker-v1-capacity-process/v1', 'result': 'FAIL'}
    path = Path(h.artifacts) / 'worker-capacity-scenarios.json'
    try:
        h.model.hold(first_text)
        h.model.hold(second_text)
        first = submit(h, first_text, conversation)
        h.model.wait_entered(first_text)
        # Gateway Admission is durable even though Worker has no free intake slot.
        second = h.send_text(second_text, conversation_id=conversation)
        observation = h.wait(lambda: capacity_observation(h.artifacts / 'worker-one.log', second),
                             'actual Worker rejects intake while configured capacity is full', timeout=15)
        published = h.sql("SELECT o.published_at IS NOT NULL,o.attempts FROM gateway.gateway_outbox o JOIN gateway.gateway_admissions a ON a.admission_id=o.event_id WHERE a.run_id=" + h.quote(second))
        assert len(published) == 1 and published[0][0] == 't'
        for _ in range(3):
            assert h.sql("SELECT run_id FROM worker.execution_runs WHERE run_id=" + h.quote(second)) == []
            assert len(model_calls(h, first_text)) == 1 and model_calls(h, second_text) == []
            assert h.sql("SELECT count(*) FROM worker.execution_runs WHERE status IN ('QUEUED','RUNNING','RETRY_WAIT')") == [['1']]
            time.sleep(.15)
        # No new webhook, no new publication and no product-state repair: release
        # the held real SDK call, and require the already-published Run to recover.
        h.model.release(first_text)
        first_result = wait_success(h, first)
        first_delivery = h.wait_delivery(first)
        h.model.wait_entered(second_text)
        assert run(h, second)['session_sequence'] == run(h, first)['session_sequence'] + 1
        assert len(attempts(h, second)) == 1
        assert h.sql("SELECT count(*) FROM worker.execution_runs WHERE status IN ('QUEUED','RUNNING','RETRY_WAIT')") == [['1']]
        later_published = h.sql("SELECT o.published_at IS NOT NULL,o.attempts FROM gateway.gateway_outbox o JOIN gateway.gateway_admissions a ON a.admission_id=o.event_id WHERE a.run_id=" + h.quote(second))
        assert later_published == published, 'Gateway republished instead of broker preserving unaccepted intake'
        h.model.release(second_text)
        second_result = wait_success(h, second)
        second_delivery = h.wait_delivery(second)
        assert len(model_calls(h, first_text)) == len(model_calls(h, second_text)) == 1
        assert len(completions(h, first)) == len(completions(h, second)) == 1
        assert len(candidates(h, first)) == len(candidates(h, second)) == 1
        evidence.update(result='PASS', invariants=['WV-32'], first_run_id=first, second_run_id=second,
                        configured_limits={'max_queued_runs': 1, 'max_active_attempts': 1, 'scan_batch': 1},
                        blocked_intake=observation, gateway_outbox_before=published,
                        gateway_outbox_after=later_published, second_webhook_count=1,
                        first_completion=first_result['completion'], second_completion=second_result['completion'],
                        first_delivery=first_delivery, second_delivery=second_delivery,
                        model_calls=2, rows_modified_by_test=False)
        print('WORKER_CAPACITY_RECOVERY=PASS configured intake/active/scan limits=1; no Run or SDK before slot release; original published request recovers without republish', flush=True)
        return evidence
    finally:
        h.model.release(first_text)
        h.model.release(second_text)
        h.stop_fault_workers()
        path.write_text(json.dumps(evidence, indent=2) + '\n')
        assert json.loads(path.read_text()) == evidence
