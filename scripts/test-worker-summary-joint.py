#!/usr/bin/env python3
"""Disposable real Control/Gateway/Worker Summary acceptance; external APIs are fixtures."""
import argparse
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from harness import Harness
from model_fixture import ModelFixture
import channel_lab_fixture as gateway_fixture
from faults import run, head, candidates, completions, wait_success

CANARY = 'SUMMARY_ONLY_CANARY: the chosen drink is green tea.'


class SummaryModelFixture:
    """Only HTTP model traffic is simulated; never writes product persistence."""
    def __init__(self, h):
        self.key = h.secret()
        self.summary_key = h.secret()
        self.requests = []
        self.generated = []
        self.lock = threading.Lock()
        self.fail_summary = False
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                if self.path != '/v1/chat/completions':
                    self.send_error(404)
                    return
                try:
                    size = int(self.headers.get('Content-Length', '0'))
                    if not 0 < size < 4 * 1024 * 1024:
                        raise ValueError('body size')
                    request = json.loads(self.rfile.read(size))
                    is_summary = request['model'] == 'joint-summary'
                    if request['model'] not in ('joint-fixture', 'joint-summary'):
                        raise ValueError('unpublished model')
                except (ValueError, KeyError):
                    self.send_error(400)
                    return
                key = owner.summary_key if is_summary else owner.key
                if self.headers.get('Authorization') != 'Bearer ' + key:
                    self.send_error(401)
                    return
                with owner.lock:
                    owner.requests.append(copy.deepcopy(request))
                    fail = is_summary and owner.fail_summary
                    summary_text = None
                    if is_summary and not fail:
                        summary_text = CANARY + ' summary_version=' + str(len(owner.generated) + 1)
                        owner.generated.append(summary_text)
                if fail:
                    payload = json.dumps({'error': {'message': 'fixture summary unavailable', 'type': 'invalid_api_key'}}).encode()
                    self.send_response(401)
                    self.send_header('Content-Type', 'application/json')
                elif is_summary:
                    if request.get('stream'):
                        self.send_error(400, 'summary must be nonstreaming')
                        return
                    payload = json.dumps({'id': 'summary', 'object': 'chat.completion', 'model': 'joint-summary',
                        'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': summary_text}, 'finish_reason': 'stop'}],
                        'usage': {'prompt_tokens': 11, 'completion_tokens': 7, 'total_tokens': 18}}).encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                else:
                    text = ModelFixture.input_text(request)
                    payload = (ModelFixture.delta('joint answer: ' + text) +
                        'data: ' + json.dumps({'id': 'joint-fixture', 'created': 1, 'object': 'chat.completion.chunk', 'model': 'joint-fixture', 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}],
                          'usage': {'prompt_tokens': 3, 'completion_tokens': 2, 'total_tokens': 5}}) + '\n\ndata: [DONE]\n\n').encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                try:
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.requests)

    def summary_outputs(self):
        with self.lock:
            return list(self.generated)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()


class SummaryHarness(Harness):
    def api(self, method, path, body=None, status=200, idem=None):
        # Reuse owner HTTP seeding, changing only authored immutable inputs.
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            spec = body['spec']
            spec['requirements']['models']['summarizer'] = {'capabilities': ['chat']}
            spec['runtime'] = {'summary': {'enabled': True, 'model_slot': 'summarizer', 'event_threshold': 1}}
            spec['nodes']['assistant']['add_session_summary'] = True
        if method == 'PUT' and '/runtime-profiles/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['config']['models']['summarizer'] = {'kind': 'openai_compatible', 'model': 'joint-summary',
                'base_url': self.model.url + '/v1', 'capabilities': ['chat']}
            body['credentials']['models']['summarizer'] = {'api_key': {'action': 'replace', 'value': self.model.summary_key}}
        return super().api(method, path, body, status, idem)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--race', action='store_true')
    args = parser.parse_args()
    artifacts = args.artifacts or Path(tempfile.mkdtemp(prefix='worker-summary-joint-evidence-'))
    h = SummaryHarness(args.root, artifacts, args.race)
    evidence = {'result': 'PENDING', 'external_fixtures': ['deterministic model HTTP', 'real Channel Lab Telegram-protocol simulator'],
                'channel_lab_http': 'PENDING', 'channel_lab_gui': 'NOT_RUN', 'real_telegram': 'NOT_RUN'}
    target = h.artifacts / 'summary-joint.json'
    print('SUMMARY_JOINT_ARTIFACTS=' + str(h.artifacts), flush=True)
    try:
        h.provision()
        h.model.close()
        h.model = SummaryModelFixture(h)
        h.urls['model'] = h.model.url
        resource_path = h.artifacts / 'fixture-resources.json'
        resources = json.loads(resource_path.read_text())
        resources['urls']['model'] = h.model.url
        resource_path.write_text(json.dumps(resources, indent=2) + '\n')
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        publication = h.api('GET', '/v1/tenants/' + h.tenant_id + '/deployments/' + h.deployment_id + '/revisions/1')
        view = publication['manifest_view']
        summary = view['runtime']['summary']
        assert summary['enabled'] and summary['event_threshold'] == 1
        assert view['resources']['models'][summary['model_resource']]['model'] == 'joint-summary'
        assert view['agent_plan']['nodes']['assistant']['add_session_summary'] is True
        evidence['manifest_view'] = view
        evidence['rounds'] = []
        # First turn may not exceed SDK summary threshold; later turns must.
        for text in ('summary seed turn', 'summary generate turn', 'summary consume turn'):
            before = len(h.model.snapshot())
            previous_summaries = h.model.summary_outputs()
            run_id = h.send_text(text)
            h.wait_delivery(run_id)
            result = wait_success(h, run_id)
            state = run(h, run_id)
            accepted = head(h, state)
            assert accepted['accepted_ref'] == result['candidate']['candidate_ref']
            assert accepted['accepted_digest'] == result['candidate']['content_digest']
            calls = h.model.snapshot()[before:]
            primary = [c for c in calls if c['model'] == 'joint-fixture']
            summaries = [c for c in calls if c['model'] == 'joint-summary']
            assert len(primary) == 1
            for call in calls:
                assert call['max_completion_tokens'] == view['execution']['max_output_tokens']
            usage = []
            for line in (h.artifacts / 'worker-one.log').read_text().splitlines():
                try:
                    observation = json.loads(line)
                except ValueError:
                    continue
                if observation.get('operation') == 'usage' and observation.get('run_id') == run_id:
                    usage.append({k: observation[k] for k in ('input_tokens', 'output_tokens', 'total_tokens')})
            assert usage == [{'input_tokens': 3 + 11 * len(summaries), 'output_tokens': 2 + 7 * len(summaries), 'total_tokens': 5 + 18 * len(summaries)}], usage
            if summaries:
                stored = result['candidate']['content']['snapshot']['session']['summaries']
                assert len(stored) == 1 and next(iter(stored.values()))['summary'] == h.model.summary_outputs()[-1], 'formal candidate did not persist this exact summary output'
            if text == 'summary consume turn':
                assert previous_summaries and previous_summaries[-1] in json.dumps(primary[0]['messages']), 'next real SDK input lacks exact previously accepted summary'
            evidence['rounds'].append({'run_id': run_id, 'head': accepted, 'usage': usage, 'calls': calls, 'completion': result['completion'], 'candidate': result['candidate']})
        assert sum(c['model'] == 'joint-summary' for c in h.model.snapshot()) > 0
        before = head(h, state)
        with h.model.lock:
            h.model.fail_summary = True
        failure_request_offset = len(h.model.snapshot())
        failure = h.send_text('summary failure must not become accepted')
        h.wait(lambda: h.sql('SELECT status FROM worker.execution_runs WHERE run_id=' + h.quote(failure)) == [['FAILED']], 'summary failure terminal', timeout=90)
        after = head(h, run(h, failure))
        assert after['accepted_ref'] == before['accepted_ref'] and after['accepted_digest'] == before['accepted_digest']
        assert candidates(h, failure) == [], 'failed summary staged candidate'
        failed = completions(h, failure)
        assert len(failed) == 1 and failed[0]['status'] == 'FAILED'
        assert failed[0]['candidate_ref'] == '' and failed[0]['candidate_digest'] == ''
        assert failed[0]['reason'] == 'RUNTIME_FAILED', failed
        failure_delivery = h.gateway.wait_delivery(failure)
        assert 'joint answer: summary failure' not in failure_delivery['final_text']
        assert CANARY not in failure_delivery['final_text']
        assert 'fixture summary unavailable' not in failure_delivery['final_text']
        evidence.update(accepted_before_failure=before, accepted_after_failure=after, failure_model_calls=h.model.snapshot()[failure_request_offset:])
        evidence.update(result='PASS', generated_summaries=h.model.summary_outputs(), failed_run=failure, failed_completion=failed, failure_delivery=failure_delivery, accepted_head_unchanged=True,
                        channel_lab_http='PASS', formal_session_summary=True, next_im_consumes=True, independent_summary_usage=True)
        target.write_text(json.dumps(evidence, indent=2) + '\n')
        assert json.loads(target.read_text()) == evidence
    except BaseException as exc:
        evidence.update(result='FAIL', error=h.redact(str(exc)), model_requests=h.model.snapshot() if isinstance(getattr(h, 'model', None), SummaryModelFixture) else [])
        target.write_text(json.dumps(evidence, indent=2) + '\n')
        raise
    finally:
        try:
            h.close()
        except BaseException as exc:
            evidence.update(result='FAIL', cleanup_error=h.redact(str(exc)))
            target.write_text(json.dumps(evidence, indent=2) + '\n')
            raise
        if evidence['result'] == 'PASS':
            evidence['cleanup'] = 'PASS'
            target.write_text(json.dumps(evidence, indent=2) + '\n')
            assert json.loads(target.read_text()) == evidence
            print('WORKER_SUMMARY_JOINT=PASS actual Control publication + credential resolve + Channel Lab HTTP + Gateway IM + Worker SDK + accepted Session summary + next IM consumption + Final + usage + failed summary head unchanged', flush=True)


if __name__ == '__main__':
    main()
