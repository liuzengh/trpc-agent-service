#!/usr/bin/env python3
"""Disposable Control/Lab/Gateway/Worker acceptance against a real external LLM."""
import argparse
import copy
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import secrets
import sys
import tempfile
import threading
from urllib.parse import urlsplit

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from harness import Harness
import channel_lab_fixture as gateway_fixture
from faults import run, head, wait_success


def read_key(path, name):
    for line in path.read_text().splitlines():
        key, sep, value = line.strip().removeprefix('export ').partition('=')
        if sep and key.strip() == name:
            value = value.strip().strip('\"\'')
            if value:
                return value
    raise ValueError('requested API key variable is absent or empty')


class LiveRelay:
    """Loopback observation only: forwards request payload and SSE bytes unchanged."""
    def __init__(self, h, key, base, model):
        self.key, self.model = key, model
        self.records, self.lock = [], threading.Lock()
        upstream = urlsplit(base)
        if upstream.scheme != 'https' or not upstream.hostname or upstream.username or upstream.query or upstream.fragment:
            raise ValueError('provider-base must be an HTTPS origin/path')
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                if self.path != '/v1/chat/completions':
                    self.send_error(404)
                    return
                if self.headers.get('Authorization') != 'Bearer ' + owner.key:
                    self.send_error(401)
                    return
                payload = self.rfile.read(int(self.headers['Content-Length']))
                request = json.loads(payload)
                if request.get('model') != owner.model:
                    self.send_error(400)
                    return
                record = {'request': request, 'status': None, 'text': '', 'usage': None}
                with owner.lock:
                    owner.records.append(record)
                connection = http.client.HTTPSConnection(upstream.hostname, upstream.port, timeout=90)
                chunks = []
                try:
                    connection.request('POST', upstream.path.rstrip('/') + '/chat/completions', body=payload,
                        headers={'Authorization': self.headers['Authorization'], 'Content-Type': self.headers.get('Content-Type', 'application/json')})
                    response = connection.getresponse()
                    record['status'] = response.status
                    self.send_response(response.status)
                    self.send_header('Content-Type', response.getheader('Content-Type', 'application/json'))
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    while True:
                        chunk = response.read1(65536)
                        if not chunk:
                            break
                        chunks.append(chunk)
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    raw = b''.join(chunks).decode('utf-8')
                    if request.get('stream'):
                        events = []
                        for line in raw.splitlines():
                            if line.startswith('data:') and line[5:].strip() != '[DONE]':
                                events.append(json.loads(line[5:]))
                        record['text'] = ''.join(c.get('delta', {}).get('content') or '' for event in events for c in event.get('choices', []))
                        record['usage'] = next((event['usage'] for event in reversed(events) if event.get('usage')), None)
                    else:
                        result = json.loads(raw)
                        record['text'] = ''.join(c.get('message', {}).get('content') or '' for c in result.get('choices', []))
                        record['usage'] = result.get('usage')
                    record['complete'] = True
                except Exception as exc:
                    record['error'] = h.redact(str(exc))
                    self.close_connection = True
                finally:
                    connection.close()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.records)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()


class LiveHarness(Harness):
    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'PUT' and '/agents/' in path and path.endswith('/draft'):
            body = copy.deepcopy(body)
            body['spec']['runtime'] = {'summary': {'enabled': True, 'model_slot': 'primary', 'event_threshold': 1}}
            body['spec']['nodes']['assistant']['add_session_summary'] = True
        return super().api(method, path, body, status, idem)


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
    h = LiveHarness(args.root, args.artifacts or Path(tempfile.mkdtemp(prefix='worker-summary-live-evidence-')), model_name=args.model)
    h.secrets.append(key)
    evidence = {'result': 'PENDING', 'provider_base': args.provider_base, 'model': args.model,
                'model_execution': 'REAL_EXTERNAL_PROVIDER', 'im': 'isolated Channel Lab HTTP', 'real_telegram': 'NOT_RUN', 'rounds': []}
    target = h.artifacts / 'summary-live.json'
    print('SUMMARY_LIVE_ARTIFACTS=' + str(h.artifacts), flush=True)
    def save():
        target.write_text(h.redact(json.dumps(evidence, indent=2, ensure_ascii=False)) + '\n')
        assert json.loads(target.read_text())['result'] == evidence['result']
    try:
        h.provision()
        h.model.close()
        h.model = LiveRelay(h, key, args.provider_base, args.model)
        h.urls['model'] = h.model.url
        resources = h.artifacts / 'fixture-resources.json'
        doc = json.loads(resources.read_text())
        doc['urls']['model'] = h.model.url
        resources.write_text(json.dumps(doc, indent=2) + '\n')
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        view = h.api('GET', '/v1/tenants/' + h.tenant_id + '/deployments/' + h.deployment_id + '/revisions/1')['manifest_view']
        assert len(view['resources']['models']) == 1
        assert view['runtime']['summary']['model_resource'] == 'primary'
        assert view['resources']['models']['primary']['model'] == args.model
        evidence['manifest_view'] = view
        canary = 'orchid-' + secrets.token_hex(6)
        evidence['canary'] = canary
        previous_summary = None
        for text in ('Remember my verification code: ' + canary + '. Acknowledge briefly.', 'Please continue briefly.', 'What verification code did I ask you to remember?'):
            offset = len(h.model.snapshot())
            run_id = h.send_text(text)
            delivery = h.wait_delivery(run_id)
            result = wait_success(h, run_id)
            accepted = head(h, run(h, run_id))
            assert accepted['accepted_ref'] == result['candidate']['candidate_ref']
            assert accepted['accepted_digest'] == result['candidate']['content_digest']
            h.wait(lambda: all(c.get('complete') for c in h.model.snapshot()[offset:]), 'relay completed response observations')
            calls = h.model.snapshot()[offset:]
            primary = [c for c in calls if c['request'].get('stream')]
            summaries = [c for c in calls if not c['request'].get('stream')]
            assert len(primary) == 1
            for call in calls:
                assert 200 <= call['status'] < 300 and call['usage'] and call['usage']['total_tokens'] > 0
                assert call['request']['max_completion_tokens'] == view['execution']['max_output_tokens']
            assert delivery['final_text'] == primary[0]['text'], 'Lab Final differs from actual provider output'
            if previous_summary is not None:
                assert any(previous_summary in message.get('content', '') for message in primary[0]['request']['messages'] if isinstance(message.get('content'), str)), 'next primary lacks previous accepted summary'
            stored = result['candidate']['content']['snapshot']['session'].get('summaries', {})
            if summaries:
                assert len(stored) == 1
                previous_summary = next(iter(stored.values()))['summary']
                assert previous_summary == summaries[-1]['text'], 'formal summary differs from actual provider response'
            evidence['rounds'].append({'run_id': run_id, 'input': text, 'head': accepted, 'calls': calls, 'delivery': delivery, 'candidate': result['candidate'], 'completion': result['completion']})
            save()
        assert previous_summary and any(not c['request'].get('stream') for c in h.model.snapshot())
        evidence.update(result='PASS', formal_summary_matches_live_response=True, next_primary_consumes_accepted_summary=True, final_matches_live_response=True)
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
            leaked = [str(path) for path in h.artifacts.rglob('*') if path.is_file() and key.encode() in path.read_bytes()]
            evidence['api_key_leak_scan'] = {'result': 'FAIL' if leaked else 'PASS', 'files': leaked}
            if leaked:
                evidence['result'] = 'FAIL'
            save()
            if leaked:
                raise RuntimeError('API key leak found in artifact paths: ' + ', '.join(leaked))
    print('WORKER_SUMMARY_LIVE=PASS', flush=True)


if __name__ == '__main__':
    main()
