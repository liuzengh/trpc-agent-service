"""Run explicitly selected old binaries against retained tracing columns.

Only the harness-owned database, processes and temporary build directory change.
The source repository is read via git archive; no checkout/reset is performed.
"""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

from faults import model_calls
from tracing_windows import revoke, snapshot


def build_old(h, source, ref):
    source = Path(source).resolve()
    commit = subprocess.check_output(['git', '-C', str(source), 'rev-parse', '--verify', ref + '^{commit}'], text=True).strip()
    assert len(commit) == 40 and all(c in '0123456789abcdef' for c in commit)
    archive = subprocess.check_output(['git', '-C', str(source), 'archive', '--format=tar', commit])
    target = (h.work / 'rollback-source').resolve()
    target.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        # Reject links as well as traversal; source/build paths stay private.
        members = tar.getmembers()
        for item in members:
            path = (target / item.name).resolve()
            assert path.is_relative_to(target) and (item.isfile() or item.isdir())
        tar.extractall(target, members=members, filter='data')
    binaries, builds = {}, []
    for name in ('agent-worker', 'channel-gateway'):
        out = h.work / ('old-' + name)
        cmd = ['go', 'build', *(['-race'] if h.race else []), '-o', str(out), './services/' + name + '/cmd/' + name]
        p = subprocess.run(cmd, cwd=target, env=h.env, text=True, capture_output=True, timeout=300)
        assert p.returncode == 0, h.redact(p.stderr)
        binaries[name] = str(out)
        builds.append({'command': cmd, 'cwd': str(target), 'exit': p.returncode, 'sha256': hashlib.sha256(out.read_bytes()).hexdigest()})
    return binaries, {'commit': commit, 'source': str(source), 'archive_sha256': hashlib.sha256(archive).hexdigest(), 'builds': builds}


def check_config(h, binary, config, expected):
    path = h.write('rollback-config-check.json', config)
    cmd = [binary, '--check-config']
    p = subprocess.run(cmd, cwd=h.root, env=dict(h.env, WORKER_CONFIG_FILE=path, WORKER_DATABASE_URL=h.dsns["worker_runtime"], WORKER_MIGRATION_DATABASE_URL=h.dsns["worker_migrator"]), text=True, capture_output=True, timeout=15)
    if expected:
        assert p.returncode == 0 and 'WORKER_CONFIG=PASS' in p.stdout, h.redact(p.stdout + p.stderr)
    else:
        assert p.returncode != 0 and p.stderr.strip() == 'worker configuration fields are invalid', h.redact(p.stdout + p.stderr)
    return {'command': cmd, 'input': 'private fixture Worker config; tracing=' + str('tracing' in config), 'stdout': h.redact(p.stdout), 'stderr': h.redact(p.stderr), 'exit': p.returncode}


def columns(h):
    rows = h.sql("SELECT table_schema,table_name,column_name,is_nullable FROM information_schema.columns WHERE (table_schema,table_name) IN (('worker','execution_runs'),('worker','execution_reply_outbox'),('gateway','gateway_outbox'),('gateway','gateway_delivery_intents')) AND column_name IN ('traceparent','tracestate') ORDER BY 1,2,3")
    assert len(rows) == 8 and all(row[3] == 'YES' for row in rows)
    return rows


def run_rollback(h, source, ref, backend, verify_run):
    old, build = build_old(h, source, ref)
    current = dict(h.binaries)
    before_columns = columns(h)
    config = json.loads((h.work / 'worker-one.json').read_text())
    rejected = check_config(h, old['agent-worker'], config, False)
    config.pop('tracing')
    accepted = check_config(h, old['agent-worker'], config, True)
    events = []
    # Leave a real new-version Reply pending, not a hand-authored old payload.
    text = 'TRACE_BODY_CANARY rollback pending'
    with revoke(h, [('worker_runtime', 'worker.execution_reply_outbox', 'SELECT')], events):
        pending = h.send_text(text)
        h.wait(lambda: h.sql("SELECT status FROM worker.execution_completions WHERE run_id=" + h.quote(pending)) == [['SUCCEEDED']], 'new Completion committed before binary rollback')
        before = snapshot(h, pending)
        assert len(before['reply']) == 1 and before['reply'][0][4] == 'false' and before['delivery'] == []
        h.stop_worker()
        h.gateway.stop()
    try:
        h.trace_enabled = False
        h.binaries.update(old)
        h.start_worker()
        h.gateway.start()
        old_pids = {'worker': h.worker.pid, 'gateway': h.gateway.process.pid}
        h.wait_delivery(pending)
        after = snapshot(h, pending)
        assert h.sql("SELECT count(*) FROM worker.execution_session_commits WHERE run_id=" + h.quote(pending)) == [['1']]
        assert before['reply'][0][:4] == after['reply'][0][:4]
        assert before['worker'][0][1] == after['worker'][0][1]
        assert len(model_calls(h, text)) == 1, 'old Worker reran completed Runner'
        assert after['delivery'][0][1] == '', 'old Gateway unexpectedly stored tracing'
        old_text = 'TRACE_BODY_CANARY old binary roundtrip'
        old_run = h.send_text(old_text)
        h.wait_delivery(old_run)
        old_state = snapshot(h, old_run)
        assert h.sql("SELECT count(*) FROM worker.execution_session_commits WHERE run_id=" + h.quote(old_run)) == [['1']]
        assert old_state['worker'][0][1] == '' and old_state['reply'][0][3] == '' and old_state['delivery'][0][1] == ''
        assert len(old_state['completion']) == 1 and len(model_calls(h, old_text)) == 1
        request = model_calls(h, old_text)[0]
        assert text in json.dumps(request) and 'TRACE_BODY_CANARY round 0' in json.dumps(request), 'old Worker lost accepted Session history'
        # Old parser/receipt path consumes a replay of the exact new Reply bytes.
        replay = h.gateway.restart_and_replay(pending)
        assert columns(h) == before_columns
    finally:
        h.stop_worker()
        h.gateway.stop()
        h.binaries = current
        h.trace_enabled = True
        h.start_worker()
        h.gateway.start()
    forward_text = 'TRACE_BODY_CANARY forward after rollback'
    forward = h.send_text(forward_text)
    h.wait_delivery(forward)
    trace = verify_run(h, backend, forward)
    request = model_calls(h, forward_text)[0]
    assert old_text in json.dumps(request), 'forward version lost old-version Session history'
    assert columns(h) == before_columns
    result = {'result': 'PASS', 'build': build, 'old_config_with_tracing': rejected, 'old_config_without_tracing': accepted, 'old_process_pids': old_pids, 'retained_columns': before_columns, 'privilege_events': events, 'pending_new_reply': {'run_id': pending, 'before': before, 'after': after, 'model_calls': 1}, 'old_roundtrip': {'run_id': old_run, 'state': old_state, 'accepted_history': True}, 'old_replay': replay, 'forward_roundtrip': trace, 'business_row_writes': False, 'down_migration': False}
    (h.artifacts / 'tracing-binary-rollback.json').write_text(json.dumps(result, indent=2) + '\n')
    print('TRACING_BINARY_ROLLBACK=PASS old_config_rejected=true stripped_config_accepted=true retained_columns=8 pending_new_reply=true old_roundtrip=true old_replay=true forward_trace=true session_history=true', flush=True)
    return result
