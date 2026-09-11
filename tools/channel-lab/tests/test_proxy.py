import json
import pathlib
import sys
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from app import Lab, Error, server


class ProxyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(pathlib.Path(self.tmp.name)/'lab.db')
        self.lab = Lab(self.path, 'http://gateway:8090')
        self.seen = []
        self.release = threading.Event()
        outer = self

        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                outer.seen.append((self.path, dict(self.headers), body))
                if body.get('seed') == 1:
                    self.send_response(401); self.end_headers()
                    self.wfile.write(b'upstream-secret-must-not-leak'); return
                if body.get('seed') == 2:
                    self.send_response(307); self.send_header('Location', outer.url+'/v1/chat/completions'); self.end_headers(); return
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream' if body.get('stream') else 'application/json')
                self.end_headers()
                if body.get('stream'):
                    self.wfile.write(b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n'); self.wfile.flush()
                    outer.release.wait(3)
                    self.wfile.write(b'data: [DONE]\n\n')
                else:
                    self.wfile.write(json.dumps({'model': body['model'], 'choices': [{'message': {'role': 'assistant', 'content': 'real answer'}}]}).encode())
        self.up = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        self.http = server(self.lab, ('127.0.0.1', 0))
        self.url = 'http://127.0.0.1:'+str(self.up.server_port)
        self.local = 'http://127.0.0.1:'+str(self.http.server_port)
        self.threads = [threading.Thread(target=s.serve_forever) for s in (self.up, self.http)]
        for t in self.threads: t.start()
        self.key = self.lab.snapshot()['model_key']
        self.lab.configure_model({'mode': 'proxy', 'base_url': self.url+'/v1', 'model': 'real-model', 'api_key': 'upstream-secret'})

    def tearDown(self):
        self.release.set()
        for s in (self.http, self.up): s.shutdown(); s.server_close()
        for t in self.threads: t.join()
        self.lab.close(); self.lab.db.close(); self.tmp.cleanup()

    def request(self, **kw):
        body = {'model': 'lab-echo', 'messages': [{'role': 'user', 'content': 'test'}], **kw}
        return urllib.request.urlopen(urllib.request.Request(self.local+'/v1/chat/completions', data=json.dumps(body).encode(), headers={'Content-Type': 'application/json', 'Authorization': 'Bearer '+self.key}), timeout=5)

    def test_json_maps_model_and_replaces_auth_preserves_parameters(self):
        with self.request(temperature=0.3, max_tokens=123) as r:
            self.assertEqual(json.load(r)['choices'][0]['message']['content'], 'real answer')
        path, headers, body = self.seen[0]
        self.assertEqual(path, '/v1/chat/completions')
        self.assertEqual(headers['Authorization'], 'Bearer upstream-secret')
        self.assertEqual(body['model'], 'real-model')
        self.assertEqual(body['max_tokens'], 123)
        self.assertEqual(body['temperature'], 0.3)
        self.assertEqual(body['messages'][0]['content'], 'test')

    def test_sse_is_incremental_not_buffered(self):
        with self.request(stream=True) as r:
            self.assertEqual(r.headers.get_content_type(), 'text/event-stream')
            self.assertIn(b'hello', r.readline())
            self.assertFalse(self.release.is_set())
            self.release.set()
            self.assertIn(b'[DONE]', r.read())

    def test_errors_sanitized_redirect_not_followed(self):
        for seed in [1, 2]:
            with self.assertRaises(urllib.error.HTTPError) as e: self.request(seed=seed)
            body = e.exception.read().decode()
            self.assertEqual(e.exception.code, 502)
            self.assertNotIn('upstream-secret', body)
        self.assertEqual(len(self.seen), 2)

    def test_settings_private_persistent_and_key_not_reused_for_changed_host(self):
        self.assertNotIn('upstream-secret', json.dumps(self.lab.snapshot()))
        self.lab.configure_model({'mode': 'proxy', 'api_key': ''})
        with self.assertRaises(Error):
            self.lab.configure_model({'mode': 'proxy', 'base_url': 'https://another.example/v1'})
        for base in ['https://user:pass@example.com/v1', 'https://example.com/v1?key=secret', 'file:///tmp/x', 'https://example.com/v1/chat/completions']:
            with self.assertRaises(Error): self.lab.configure_model({'mode': 'proxy', 'base_url': base})
        other = Lab(self.path, 'http://gateway:8090')
        try: self.assertEqual(other.model_config(private=True)['api_key'], 'upstream-secret')
        finally: other.close(); other.db.close()
        self.lab.configure_model({'mode': 'echo', 'clear_key': True})
        self.assertFalse(self.lab.model_config()['key_configured'])
        with self.request() as r: self.assertEqual(json.load(r)['choices'][0]['message']['content'], 'lab echo: test')

    def test_config_requires_same_origin_and_bad_model_auth_never_calls_upstream(self):
        for path, headers in [('/lab/model', {}), ('/v1/chat/completions', {'Authorization': 'Bearer wrong'})]:
            with self.assertRaises(urllib.error.HTTPError):
                urllib.request.urlopen(urllib.request.Request(self.local+path, data=b'{}', headers={'Content-Type': 'application/json', **headers}))
        self.assertEqual(self.seen, [])
        with urllib.request.urlopen(urllib.request.Request(self.local+'/lab/model', data=b'{"mode":"echo"}', headers={'Content-Type': 'application/json', 'Origin': self.local})) as r:
            result=json.load(r);self.assertEqual(result['mode'], 'echo');self.assertNotIn('api_key',result)
