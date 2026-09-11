"""Explicit external-model fixture with observable auth and streaming boundaries."""
from __future__ import annotations

import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import secrets
import threading


class ModelFixture:
    """Serve actual SDK HTTP requests; never impersonate Control or Worker proof.

    Keys remain private fixture values. Evidence records only generations and
    auth outcomes. A held request keeps the auth decision made at its entry.
    """

    def __init__(self, harness=None):
        self.h = harness
        self.requests = []
        self.blocks = {}
        self.entered = {}
        self.lock = threading.Lock()
        self._hold_timeout_seconds = 90
        self.key = secrets.token_urlsafe(24)
        self._generation = 1
        self._authentication = {}
        self._fail_once = {}
        self._failures = []
        self._partial = {}
        self._partial_events = {}
        self._partial_receipts = {}
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                if self.path != '/v1/chat/completions':
                    self.send_error(404)
                    return
                try:
                    length = int(self.headers.get('Content-Length', '0'))
                    if length <= 0 or length > 4 * 1024 * 1024:
                        raise ValueError('fixture body size')
                    request = json.loads(self.rfile.read(length))
                    text = fixture.input_text(request)
                    model_name = request.get('model', 'joint-fixture')
                except (ValueError, TypeError, AttributeError):
                    self.send_error(400)
                    return
                with fixture.lock:
                    accepted = self.headers.get('Authorization') == 'Bearer ' + fixture.key
                    generation = fixture._generation
                    fixture._authentication.setdefault(text, []).append({
                        'accepted': accepted, 'key_generation': generation,
                    })
                    if accepted:
                        fixture.requests.append(request)
                        entered = fixture.entered.setdefault(text, threading.Event())
                        block = fixture.blocks.get(text)
                        hold_timeout = fixture._hold_timeout_seconds
                        failure = fixture._fail_once.pop(text, None)
                        if failure is not None:
                            fixture._failures.append({"text": text, "status": failure, "response": "no SSE/candidate"})
                        partial = fixture._partial.pop(text, None)
                        if partial is None:
                            entered.set()
                if not accepted:
                    self.send_error(403)
                    return
                try:
                    if failure is not None:
                        raw = json.dumps({'error': {'message': 'TRACE_BODY_CANARY model temporarily unavailable', 'type': 'server_error'}}).encode()
                        self.send_response(failure)
                        self.send_header('Content-Type', 'application/json')
                        self.send_header('Content-Length', str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                        return
                    if partial is not None:
                        self.send_response(200)
                        self.send_header('Content-Type', 'text/event-stream')
                        self.send_header('Connection', 'close')
                        self.end_headers()
                        first = fixture.delta(partial, model=model_name).encode()
                        self.wfile.write(first)
                        self.wfile.flush()
                        with fixture.lock:
                            fixture._partial_receipts[text] = {
                                'phase': 'partial_sse_flushed', 'bytes': len(first),
                                'done': False, 'key_generation': generation,
                            }
                            fixture._partial_events[text].set()
                            entered.set()
                        if block is not None and not block.wait(hold_timeout):
                            return
                        self.wfile.write(fixture.final(text, model=model_name).encode())
                        self.wfile.flush()
                    else:
                        if block is not None and not block.wait(hold_timeout):
                            self.send_error(504)
                            return
                        body = fixture.final(text, model=model_name).encode()
                        self.send_response(200)
                        self.send_header('Content-Type', 'text/event-stream')
                        self.send_header('Content-Length', str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    # Preserve the killed caller's lost connection; a retry is
                    # a new independently authenticated actual SDK request.
                    pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @staticmethod
    def input_text(request):
        text = next((m.get('content', '') for m in reversed(request.get('messages', []))
                     if m.get('role') == 'user'), '')
        if isinstance(text, str):
            return text
        if isinstance(text, list):
            return ''.join(m.get('text', '') for m in text if isinstance(m, dict))
        return json.dumps(text, sort_keys=True)

    @staticmethod
    def delta(text, finish=None, *, model='joint-fixture'):
        choice = {'index': 0, 'delta': {'role': 'assistant', 'content': text}
                  if text else {}, 'finish_reason': finish}
        return 'data: ' + json.dumps({'id': 'joint-fixture', 'object': 'chat.completion.chunk',
                                      'created': 1, 'model': model,
                                      'choices': [choice]}) + '\n\n'

    @classmethod
    def final(cls, text, *, model='joint-fixture'):
        return cls.delta('joint answer: ' + text, model=model) + cls.delta('', 'stop', model=model) + 'data: [DONE]\n\n'

    def fail_once(self, text, status=503):
        if not isinstance(text, str) or not text or type(status) is not int or status not in (500, 503):
            raise ValueError('model fault requires exact text and transient HTTP status')
        with self.lock:
            if text in self._fail_once or text in self.blocks or text in self._partial:
                raise ValueError('model input already armed')
            self._fail_once[text] = status

    def failure_snapshot(self):
        with self.lock:
            return copy.deepcopy(self._failures)

    @property
    def hold_timeout_seconds(self):
        with self.lock:
            return self._hold_timeout_seconds

    @hold_timeout_seconds.setter
    def hold_timeout_seconds(self, value):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError('model fixture hold timeout must be finite and positive')
        with self.lock:
            self._hold_timeout_seconds = value

    def hold(self, text):
        with self.lock:
            self.blocks[text] = threading.Event()
            self.entered[text] = threading.Event()

    def hold_partial(self, text, poison):
        if not isinstance(poison, str) or not poison:
            raise ValueError('partial fixture text must be nonempty')
        with self.lock:
            self.blocks[text] = threading.Event()
            self.entered[text] = threading.Event()
            self._partial[text] = poison
            self._partial_events[text] = threading.Event()
            self._partial_receipts.pop(text, None)

    def wait_partial(self, text, timeout=30):
        with self.lock:
            event = self._partial_events.get(text)
        if event is None or not event.wait(timeout):
            raise RuntimeError('partial SSE flush deadline')
        with self.lock:
            return copy.deepcopy(self._partial_receipts[text])

    def release(self, text):
        with self.lock:
            if text in self.blocks:
                self.blocks[text].set()

    def wait_entered(self, text, timeout=30):
        with self.lock:
            event = self.entered.setdefault(text, threading.Event())
        if not event.wait(timeout):
            raise RuntimeError('model fixture request deadline')

    def rotate_key(self, new_key):
        if not isinstance(new_key, str) or not new_key:
            raise ValueError('fixture key required')
        with self.lock:
            self.key = new_key
            self._generation += 1
        if self.h is not None:
            self.h.secrets.append(new_key)

    def authentication_events(self, text):
        with self.lock:
            return copy.deepcopy(self._authentication.get(text, []))

    def close(self):
        with self.lock:
            for event in self.blocks.values():
                event.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)
        if self.thread.is_alive():
            raise RuntimeError('model fixture did not stop')
