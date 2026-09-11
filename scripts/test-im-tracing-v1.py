#!/usr/bin/env python3
"""Actual Control/Gateway/Worker trace gate; only external model/IM are fixtures."""
from __future__ import annotations
import argparse
import base64
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from harness import Harness, assert_model_contract
import gateway_fixture


class TracedHarness(Harness):
    def __init__(self, root, artifacts, endpoint, **kwargs):
        super().__init__(root, artifacts, **kwargs)
        self.tracing = {'traces_endpoint': endpoint, 'sampling_ratio': 1.0, 'export_timeout': '2s', 'batch_timeout': '200ms', 'max_queue_size': 2048, 'max_export_batch_size': 512}
        self.trace_config = self.write('gateway-tracing.json', self.tracing)
        self.trace_enabled = True
        self.env['DEPLOYMENT_ENVIRONMENT'] = 'tracing-fixture'

    def spawn(self, name, argv, env):
        env = dict(env)
        if 'WORKER_CONFIG_FILE' in env:
            path = Path(env['WORKER_CONFIG_FILE'])
            config = json.loads(path.read_text())
            if self.trace_enabled:
                config['tracing'] = self.tracing
            else:
                config.pop('tracing', None)
            path.write_text(json.dumps(config))
        if 'GATEWAY_DATABASE_URL' in env:
            if self.trace_enabled:
                env['GATEWAY_TRACING_CONFIG_FILE'] = self.trace_config
            else:
                env.pop('GATEWAY_TRACING_CONFIG_FILE', None)
        return super().spawn(name, argv, env)


def identifier(value):
    if len(value) in (16, 32):
        try:
            bytes.fromhex(value)
            return value.lower()
        except ValueError:
            pass
    return base64.b64decode(value).hex()


def flatten(document):
    spans = []
    def walk(value):
        if isinstance(value, dict):
            if 'spanId' in value and 'name' in value:
                spans.append(value)
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    walk(document)
    return spans


def query(backend, trace_id, required, timeout=35):
    end = time.monotonic() + timeout
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while time.monotonic() < end:
        request = urllib.request.Request(backend + '/api/traces/' + trace_id, headers={'Accept': 'application/json'})
        try:
            with opener.open(request, timeout=3) as response:
                document = json.loads(response.read(4 << 20))
            spans = flatten(document)
            if required.issubset({s['name'] for s in spans}):
                return document, spans
        except urllib.error.HTTPError as error:
            error.close()
        except (OSError, ValueError):
            pass
        time.sleep(0.2)
    raise AssertionError('actual process trace did not contain all required phases')


# Required causal ancestry, not a required immediate parent: SDK and storage
# adapters may legitimately add intermediate spans without changing this contract.
CAUSAL_ANCESTORS = {
    'gateway.run.admit': 'gateway.im.callback',
    'create execution.run-requested.v1': 'gateway.run.admit',
    'worker.run.attempt': 'process execution.run-requested.v1',
    'worker.runner.run': 'worker.run.attempt',
    'worker.session.load': 'worker.run.attempt',
    'worker.session.stage': 'worker.run.attempt',
    'worker.run.complete': 'worker.run.attempt',
    'worker.session.commit': 'worker.run.complete',
    'create execution.reply-intent.v1': 'worker.run.complete',
    'gateway.reply.verify': 'process execution.reply-intent.v1',
    'worker.reply.verify': 'gateway.reply.verify',
    'gateway.reply.deliver': 'process execution.reply-intent.v1',
    'gateway.im.send': 'gateway.reply.deliver',
}


def verify_parentage(spans, trace_id, *, allow_missing_parents=False):
    """Validate observed edges; crash mode permits absent parents, never wrong roots.

    Missing ancestors can only be excused by an actual missing parent ID on that
    span's own path. Cycles, foreign traces, duplicate IDs and incorrect observed
    messaging edges/Links remain errors even in crash mode.
    """
    by_id = {}
    parents = {}
    missing = []
    roots = []
    for span in spans:
        sid = identifier(span['spanId'])
        assert len(sid) == 16 and sid != '0' * 16 and sid not in by_id, 'invalid/duplicate span ID'
        assert identifier(span['traceId']) == trace_id, 'foreign trace span'
        by_id[sid] = span
        raw_parent = span.get('parentSpanId')
        parent = identifier(raw_parent) if raw_parent else '0' * 16
        assert len(parent) == 16, 'invalid parent ID'
        parents[sid] = parent if parent != '0' * 16 else None
        if parents[sid] is None:
            roots.append(span)
    assert all(s['name'] == 'gateway.im.callback' for s in roots), 'unexpected diagnostic/disconnected root'
    assert len(roots) <= 1 and (allow_missing_parents or len(roots) == 1), 'expected single callback root'

    for sid, span in by_id.items():
        seen = {sid}
        ancestor_names = set()
        parent = parents[sid]
        gap = False
        while parent is not None:
            assert parent not in seen, 'cyclic parent chain'
            seen.add(parent)
            if parent not in by_id:
                gap = True
                break
            ancestor_names.add(by_id[parent]['name'])
            parent = parents[parent]
        direct_parent = parents[sid]
        if direct_parent is not None and direct_parent not in by_id:
            missing.append({'span': span['name'], 'parent_span_id': direct_parent})
        assert not gap or allow_missing_parents, 'missing actual causal parent: ' + span['name']
        expected = CAUSAL_ANCESTORS.get(span['name'])
        if span['name'].startswith(('invoke_agent ', 'chat ', 'execute_tool ', 'session.overlay.')):
            expected = 'worker.runner.run'
        if expected:
            assert expected in ancestor_names or (gap and allow_missing_parents), 'wrong causal ancestry: ' + span['name']
        for subject in ('execution.run-requested.v1', 'execution.reply-intent.v1'):
            if span['name'] not in ('publish ' + subject, 'process ' + subject):
                continue
            assert direct_parent is not None, 'missing message creation parent'
            if direct_parent in by_id:
                assert by_id[direct_parent]['name'] == 'create ' + subject, 'wrong message creation parent'
            assert any(identifier(link['traceId']) == trace_id and identifier(link['spanId']) == direct_parent
                       for link in span.get('links', [])), 'missing creation Link: ' + span['name']
    return missing


def verify_run(h, backend, run, *, allow_missing_parents=False):
    rows = h.sql("SELECT r.status,c.kind,o.intent_id,r.traceparent,o.traceparent FROM worker.execution_runs r JOIN worker.execution_completions c USING(tenant_id,run_id) JOIN worker.execution_reply_outbox o USING(tenant_id,run_id) WHERE r.run_id=" + h.quote(run))
    assert len(rows) == 1 and rows[0][:2] == ['SUCCEEDED', 'ATTEMPT']
    delivery = h.sql("SELECT traceparent FROM gateway.gateway_delivery_intents WHERE run_id=" + h.quote(run))
    assert len(delivery) == 1
    carriers = [rows[0][3], rows[0][4], delivery[0][0]]
    trace_id = carriers[0].split('-')[1]
    assert all(c.split('-')[1] == trace_id for c in carriers)
    required = {'gateway.im.callback', 'gateway.run.admit', 'create execution.run-requested.v1', 'publish execution.run-requested.v1', 'process execution.run-requested.v1', 'worker.run.attempt', 'worker.runner.run', 'worker.session.load', 'worker.session.stage', 'worker.session.commit', 'worker.run.complete', 'create execution.reply-intent.v1', 'publish execution.reply-intent.v1', 'process execution.reply-intent.v1', 'gateway.reply.verify', 'worker.reply.verify', 'gateway.reply.deliver', 'gateway.im.send'}
    document, spans = query(backend, trace_id, required)
    names = {s['name'] for s in spans}
    assert any(n.startswith('chat ') for n in names) and any(n.startswith('invoke_agent ') for n in names)
    by_id = {identifier(s['spanId']): s for s in spans}
    for carrier, name in zip(carriers, ['process execution.run-requested.v1', 'create execution.reply-intent.v1', 'process execution.reply-intent.v1']):
        assert by_id[carrier.split('-')[2]]['name'] == name
    missing_parents = verify_parentage(spans, trace_id, allow_missing_parents=allow_missing_parents)
    serialized = json.dumps(document)
    assert 'TRACE_BODY_CANARY' not in serialized
    assert all(secret not in serialized for secret in h.secrets)
    assert not any('memory.' in n for n in names), 'Memory is deferred, not a placeholder span'
    (h.artifacts / (run + '-trace.json')).write_text(json.dumps(document, indent=2) + '\n')
    return {'run_id': run, 'trace_id': trace_id, 'span_count': len(spans), 'names': sorted(names), 'durable_carriers': carriers, 'completion': rows[0][:3], 'parentage': 'PASS' if not missing_parents else 'post-crash gaps recorded', 'missing_parent_spans': missing_parents, 'body_canary': 'ABSENT'}


def verify_replay_link(h, backend, prior):
    run, original = prior['run_id'], prior['trace_id']
    delivery = h.sql("SELECT traceparent FROM gateway.gateway_delivery_intents WHERE run_id=" + h.quote(run))
    assert delivery == [[prior['durable_carriers'][2]]], 'replay overwrote first accepted context'
    carrier = prior['durable_carriers'][2].split('-')
    end = time.monotonic() + 30
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    url = backend + '/api/search?q=' + urllib.parse.quote('{ span.app.run.id = "' + run + '" }')
    while time.monotonic() < end:
        with opener.open(url, timeout=3) as response:
            results = json.loads(response.read(1 << 20))
        for item in results.get('traces', []):
            trace_id = item.get('traceID', item.get('traceId'))
            if not trace_id or trace_id == original:
                continue
            document, spans = query(backend, trace_id, {'process execution.reply-intent.v1'}, timeout=3)
            for span in spans:
                for link in span.get('links', []):
                    if identifier(link['traceId']) == original and identifier(link['spanId']) == carrier[2]:
                        (h.artifacts / 'gateway-replay-trace.json').write_text(json.dumps(document, indent=2) + '\n')
                        return {'diagnostic_trace_id': trace_id, 'original_trace_id': original, 'first_process_span_id': carrier[2], 'creation_header': 'absent in explicit replay fixture', 'durable_link': 'PASS'}
        time.sleep(0.2)
    raise AssertionError('replayed diagnostic trace did not link durable first acceptance')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    p.add_argument('--artifacts', type=Path, required=True)
    p.add_argument('--export-matrix', action='store_true', help='verify real OTLP outage, queue pressure and blocked stdout')
    p.add_argument('--lifecycle-matrix', action='store_true', help='verify stream shutdown, live fencing and deadline traces')
    p.add_argument('--retry-matrix', action='store_true', help='verify actual broker redelivery and model retry traces')
    p.add_argument('--session-matrix', action='store_true', help='verify candidate and accepted Session fault traces')
    p.add_argument('--delivery-matrix', action='store_true', help='verify actual multi-part and delivery fault traces')
    p.add_argument('--windows', action='store_true', help='verify exact durable handoff crash windows on owned fixtures')
    p.add_argument('--rollback-source', type=Path, help='read-only Git repository for old binary build')
    p.add_argument('--rollback-ref', help='explicit old commit/ref; requires --rollback-source')
    p.add_argument('--otlp-endpoint', required=True)
    p.add_argument('--tempo-url', required=True)
    args = p.parse_args()
    if bool(args.rollback_source) != bool(args.rollback_ref):
        p.error("--rollback-source and --rollback-ref must be provided together")
    for value in (args.otlp_endpoint, args.tempo_url):
        url = urllib.parse.urlparse(value)
        assert url.scheme == 'http' and url.hostname == '127.0.0.1' and not url.username and not url.query
    h = TracedHarness(args.root, args.artifacts, args.otlp_endpoint, race=True, model_name='gpt-4o')
    try:
        h.provision()
        if args.session_matrix:
            from commit_scenarios import install_proxy
            install_proxy(h)
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        results = []
        for n in range(2):
            run = h.send_text('TRACE_BODY_CANARY round ' + str(n))
            h.wait_delivery(run)
            results.append(verify_run(h, args.tempo_url, run))
        assert len(h.model.requests) == 2
        assert 'TRACE_BODY_CANARY round 0' in json.dumps(h.model.requests[1]), 'accepted Session missing from second model call'
        published = h.api('GET', '/v1/tenants/' + h.tenant_id + '/deployments/' + h.deployment_id + '/revisions/' + str(h.revision_number))
        contract = assert_model_contract(h.model.requests, published['manifest_view'], h.model_name)
        export_matrix = None
        if args.export_matrix:
            from tracing_export import run_traced_export
            export_matrix = run_traced_export(h, args.tempo_url, verify_run)
        lifecycle_matrix = None
        if args.lifecycle_matrix:
            from tracing_lifecycle import run_traced_lifecycle
            lifecycle_matrix = run_traced_lifecycle(h, args.tempo_url, verify_run, query, identifier)
        retry_matrix = None
        if args.retry_matrix:
            from tracing_retry import run_traced_retry
            retry_matrix = run_traced_retry(h, args.tempo_url, verify_run, query, identifier)
        session_matrix = None
        if args.session_matrix:
            from tracing_session import run_traced_session
            session_matrix = run_traced_session(h, args.tempo_url, verify_run, query)
        delivery_matrix = None
        if args.delivery_matrix:
            from tracing_delivery import run_traced_delivery
            delivery_matrix = run_traced_delivery(h, args.tempo_url, verify_run)
        windows = None
        if args.windows:
            from tracing_windows import run_windows
            windows = run_windows(h, args.tempo_url, verify_run, query)
        rollback = None
        if args.rollback_source:
            from tracing_rollback import run_rollback
            rollback = run_rollback(h, args.rollback_source, args.rollback_ref, args.tempo_url, verify_run)
        model_count = len(h.model.requests)
        h.stop_worker()
        replay = h.gateway.restart_and_replay(results[-1]['run_id'])
        assert len(h.model.requests) == model_count
        replay['trace_link'] = verify_replay_link(h, args.tempo_url, results[-1])
        result = {'result': 'PASS', 'runs': results, 'model_contract': contract, 'gateway_restart_replay': replay, 'recovery_windows': windows, 'export_matrix': export_matrix, 'lifecycle_matrix': lifecycle_matrix, 'retry_matrix': retry_matrix, 'session_matrix': session_matrix, 'delivery_matrix': delivery_matrix, 'binary_rollback': rollback, 'external_fixtures': ['model', 'Telegram Bot API', 'HTTPS webhook ingress'], 'real_processes': ['Control', 'Gateway', 'Worker'], 'memory': 'DEFERRED'}
        (h.artifacts / 'tracing-process-result.json').write_text(json.dumps(result, indent=2) + '\n')
        print('TRACING_PROCESS=PASS runs=2 real_sdk=true formal_session=true reply=true parentage=true gateway_restart_replay=true', flush=True)
    finally:
        h.close()


if __name__ == '__main__':
    main()
