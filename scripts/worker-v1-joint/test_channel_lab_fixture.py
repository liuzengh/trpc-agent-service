"""Channel Lab helper tests: actual Lab subprocess/HTTP, not three-service E2E."""
import json
from pathlib import Path
import ssl
import sys
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from channel_lab_fixture import ChannelLab, LabGateway

ROOT = Path(__file__).resolve().parents[2]


def wait(predicate, description, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.03)
    raise AssertionError(description)


class ChannelLabTest(unittest.TestCase):
    def test_http_200_nonready_response_has_a_real_deadline_and_no_hot_loop(self):
        lab = ChannelLab.__new__(ChannelLab)
        lab.process = Mock()
        lab.process.poll.return_value = None
        lab.request = Mock(return_value={'status': 'wrong service'})
        with patch('channel_lab_fixture.time.monotonic', side_effect=[0, 1, 11]), patch('channel_lab_fixture.time.sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError, 'readiness deadline'):
                lab._wait_ready(timeout=10)
        lab.request.assert_called_once_with('/healthz')
        sleep.assert_called_once_with(.03)

    def test_real_lab_chat_posts_tls_webhook_and_records_only_actual_out_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            ca, key = path / 'ca.pem', path / 'key.pem'
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1', '-keyout', str(key), '-out', str(ca)], check=True, capture_output=True)
            received = []
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *_):
                    pass
                def do_POST(self):
                    received.append({'path': self.path, 'secret': self.headers.get('X-Telegram-Bot-Api-Secret-Token'), 'body': json.loads(self.rfile.read(int(self.headers['Content-Length'])))})
                    self.send_response(200)
                    self.send_header('Content-Length', '0')
                    self.end_headers()
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(ca, key)
            server.socket = context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            h = SimpleNamespace(root=ROOT, work=path, artifacts=path, urls={'gateway_ingress': 'https://127.0.0.1:' + str(server.server_port)}, certs={'ca': ca}, secrets=[])
            lab = None
            try:
                lab = ChannelLab(h)
                self.assertIsNone(lab.process.poll())
                lab.request('/bot' + lab.token + '/setWebhook', {'url': h.urls['gateway_ingress'] + '/v1/telegram/test-account', 'secret_token': lab.secret})
                update = lab.chat('actual IM input', '42', sender_id=100)
                wait(lambda: received, 'real Lab HTTPS webhook')
                wait(lambda: lab.update_state(update['update_id'])['ack'] == 1, 'Lab records actual HTTP ACK')
                self.assertEqual(received[0], {'path': '/v1/telegram/test-account', 'secret': lab.secret, 'body': update})
                reply = lab.request('/bot' + lab.token + '/sendMessage', {'chat_id': 42, 'text': 'actual Lab output', 'reply_parameters': {'message_id': update['message']['message_id']}})
                self.assertTrue(reply['ok'])
                messages = lab.snapshot()
                self.assertEqual(len(messages), 1)
                self.assertEqual(messages[0]['text'], 'actual Lab output')
                self.assertEqual(messages[0]['chat_id'], '42')
                self.assertNotIn('source_message_id', messages[0])
                self.assertNotIn('reply_parameters', messages[0])
                with self.assertRaisesRegex(RuntimeError, 'Channel Lab HTTP 400'):
                    lab.request('/bot' + lab.token + '/sendMessage', {'chat_id': 42, 'text': 'invalid source', 'reply_parameters': {'message_id': 999999}})
                self.assertEqual(lab.snapshot(), messages)
                lab.close()
                cleanup = json.loads((path / 'channel-lab-cleanup.json').read_text())
                self.assertEqual(cleanup['result'], 'PASS')
                self.assertEqual(cleanup['exit_code'], -15)
                self.assertTrue(cleanup['reaped'])
                for artifact in path.glob('channel-lab-*.json*'):
                    raw = artifact.read_text()
                    self.assertNotIn(lab.token, raw)
                    self.assertNotIn(lab.secret, raw)
                lab.close()
            finally:
                if lab:
                    lab.close()
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())


class LabGatewayTest(unittest.TestCase):
    def fixture(self, directory, final_text="fixed failure Final"):
        lab = Mock()
        lab.bot_id = 11
        lab.snapshot.return_value = [{'row_id': 1, 'bot_id': 11, 'chat_id': '42', 'message_id': 101, 'text': 'previous Final'}]
        lab.chat.return_value = {'update_id': 9, 'message': {'message_id': 9, 'chat': {'id': 42, 'type': 'private'}, 'text': 'real IM'}}
        lab.update_state.return_value = {'id': 9, 'bot': 11, 'attempts': 1, 'status': 'HTTP 200', 'ack': 1}
        h = SimpleNamespace(gateway_fixture=lab, artifacts=Path(directory), wait=wait)
        statements = []
        def sql(query):
            statements.append(query)
            if 'gateway_admissions' in query:
                return [['run-one']]
            if 'gateway_delivery_intents' in query:
                # Reproduce Harness.sql's actual psql -At transport, including its
                # row/column splitting. JSON is SQL framing, not altered Final text.
                field = json.dumps(final_text, ensure_ascii=False) if 'to_json(string_agg(' in query else final_text
                raw = 'intent-one\t' + field + '\ttrue\t1\n'
                return [line.split('\t') for line in raw.splitlines() if line]
            if 'gateway_reply_transport_receipts' in query:
                return [['1']]
            raise AssertionError(query)
        h.sql = sql
        real = SimpleNamespace(h=h, account_id='account-one', updates={}, update_bots={}, stop=Mock(), _webhook=Mock(side_effect=AssertionError('direct webhook forbidden')))
        return LabGateway(real), lab, statements

    def test_durable_final_with_newlines_blank_lines_and_tabs_round_trips_exactly(self):
        for text in ('first line\nsecond line', 'first\n\nlast\n', 'column one\tcolumn two', '中文 `code`\n\n\tindent\r\n"quoted" \\end'):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as directory:
                gateway, lab, statements = self.fixture(directory, final_text=text)
                run_id = gateway.send_text('real IM')
                added = {'row_id': 2, 'bot_id': 11, 'chat_id': '42', 'message_id': 102, 'text': text}
                lab.snapshot.return_value += [added]
                # This is an already durable row, not a time-dependent service.
                # A false predicate detects the framing bug without a 90s sleep.
                def immediate(predicate, description, timeout=10):
                    self.assertTrue(predicate(), description)
                gateway.h.wait = immediate
                result = gateway.wait_delivery(run_id)
                self.assertEqual(result['final_text'], text)
                self.assertEqual(result['outgoing_added'], [added])
                self.assertEqual(gateway.wait_delivery(run_id), result)
                stored = json.loads((Path(directory) / ('gateway-delivery-' + run_id + '.json')).read_text())
                self.assertEqual(stored['final_text'], text)
                self.assertEqual(result['parts'], 1)
                self.assertTrue(all(query.startswith('SELECT ') for query in statements))

    def test_sql_null_missing_receipt_and_actual_empty_string_are_distinct(self):
        for mode in ('sql_null', 'json_null', 'not_received', 'empty_string'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                gateway, lab, _ = self.fixture(directory, final_text='')
                run_id = gateway.send_text('real IM')
                original_sql = gateway.h.sql
                def sql(query):
                    if 'gateway_delivery_intents' in query:
                        if mode == 'sql_null': return [['intent-one', '', 'true', '1']]
                        if mode == 'json_null': return [['intent-one', 'null', 'true', '1']]
                        if mode == 'not_received': return []
                    return original_sql(query)
                gateway.h.sql = sql
                def immediate(predicate, description, timeout=10):
                    self.assertTrue(predicate(), description)
                gateway.h.wait = immediate
                if mode == 'empty_string':
                    lab.snapshot.return_value += [{'row_id': 2, 'bot_id': 11, 'chat_id': '42', 'message_id': 102, 'text': ''}]
                    self.assertEqual(gateway.wait_delivery(run_id)['final_text'], '')
                else:
                    with self.assertRaises((AssertionError, ValueError)): gateway.wait_delivery(run_id)
                    self.assertNotIn('delivery', gateway.rounds[run_id])
                    self.assertFalse((Path(directory) / ('gateway-delivery-' + run_id + '.json')).exists())

    def test_only_lab_chat_ingress_and_formal_admission_then_delta_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway, lab, statements = self.fixture(directory)
            run_id = gateway.send_text('real IM')
            self.assertEqual(run_id, 'run-one')
            lab.chat.assert_called_once_with('real IM', '42', sender_id=100)
            gateway._webhook.assert_not_called()
            self.assertEqual(json.loads(gateway.updates[run_id]), lab.chat.return_value)
            with self.assertRaisesRegex(AssertionError, 'one unfinished'):
                gateway.send_text('concurrent same chat')
            added = {'row_id': 2, 'bot_id': 11, 'chat_id': '42', 'message_id': 102, 'text': 'fixed failure Final'}
            unrelated = {'row_id': 3, 'bot_id': 11, 'chat_id': '99', 'message_id': 103, 'text': 'unrelated'}
            lab.snapshot.return_value += [added, unrelated]
            result = gateway.wait_delivery(run_id)
            self.assertEqual(result['delivery_state'], 'ACCEPTED')
            self.assertEqual(result['outgoing_added'], [added])
            self.assertEqual(result['outgoing_before'][0]['text'], 'previous Final')
            self.assertIn('not exposed', result['correlation'])
            self.assertNotIn('source_message_id', result['outgoing_added'][0])
            self.assertEqual(gateway.wait_delivery(run_id), result)
            self.assertTrue(all(query.startswith('SELECT ') for query in statements))
            self.assertTrue((Path(directory) / 'channel-lab-input-run-one.json').exists())
            self.assertTrue((Path(directory) / 'gateway-delivery-run-one.json').exists())

    def test_duplicate_output_or_wrong_text_never_marks_delivery_accepted(self):
        for texts in (['fixed failure Final', 'fixed failure Final'], ['different output']):
            with self.subTest(texts=texts), tempfile.TemporaryDirectory() as directory:
                gateway, lab, _ = self.fixture(directory)
                run_id = gateway.send_text('real IM')
                lab.snapshot.return_value += [{'row_id': 2 + i, 'bot_id': 11, 'chat_id': '42', 'message_id': 102 + i, 'text': text} for i, text in enumerate(texts)]
                with self.assertRaises(AssertionError):
                    gateway.wait_delivery(run_id)
                self.assertNotIn('delivery', gateway.rounds[run_id])
                self.assertFalse((Path(directory) / 'gateway-delivery-run-one.json').exists())

    def test_late_duplicate_is_rejected_by_next_send_and_repeated_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway, lab, _ = self.fixture(directory)
            run_id = gateway.send_text('real IM')
            lab.snapshot.return_value += [{'row_id': 2, 'bot_id': 11, 'chat_id': '42', 'message_id': 102, 'text': 'fixed failure Final'}]
            gateway.wait_delivery(run_id)
            lab.snapshot.return_value += [{'row_id': 3, 'bot_id': 11, 'chat_id': '42', 'message_id': 103, 'text': 'fixed failure Final'}]
            with self.assertRaisesRegex(AssertionError, 'late duplicate'):
                gateway.wait_delivery(run_id)
            with self.assertRaisesRegex(AssertionError, 'late duplicate'):
                gateway.send_text('next IM')
            lab.chat.assert_called_once()
            gateway.gateway.close = Mock()
            with self.assertRaisesRegex(AssertionError, 'late duplicate'):
                gateway.close()
            gateway.gateway.close.assert_called_once()
            result = json.loads((Path(directory) / 'channel-lab-outgoing-final.json').read_text())
            self.assertEqual(result['result'], 'FAIL')

    def test_successful_close_preserves_exact_union_of_observed_output(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway, lab, _ = self.fixture(directory)
            run_id = gateway.send_text('real IM')
            lab.snapshot.return_value += [{'row_id': 2, 'bot_id': 11, 'chat_id': '42', 'message_id': 102, 'text': 'fixed failure Final'}]
            gateway.wait_delivery(run_id)
            gateway.gateway.close = Mock()
            gateway.close()
            gateway.gateway.close.assert_called_once()
            result = json.loads((Path(directory) / 'channel-lab-outgoing-final.json').read_text())
            self.assertEqual(result['result'], 'PASS')
            self.assertEqual(result['chats'][0]['actual'], result['chats'][0]['expected'])
            self.assertEqual(len(result['chats'][0]['actual']), 2)

    def test_caller_cannot_invent_lab_update_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            gateway, lab, statements = self.fixture(directory)
            with self.assertRaisesRegex(ValueError, 'Lab owns update IDs'):
                gateway.send_text('real IM', update_id=99)
            lab.chat.assert_not_called()
            self.assertEqual(statements, [])

    def test_early_child_exit_is_reaped_but_cleanup_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            lab = ChannelLab.__new__(ChannelLab)
            lab.h = SimpleNamespace(artifacts=Path(directory))
            lab.closed = False
            lab.log = open(Path(directory) / 'child.log', 'w')
            lab.process = subprocess.Popen([sys.executable, '-B', '-c', 'pass'], stdout=lab.log, stderr=subprocess.STDOUT)
            lab.process.wait(timeout=10)
            with self.assertRaisesRegex(RuntimeError, 'cleanup failed'):
                lab.close()
            result = json.loads((Path(directory) / 'channel-lab-cleanup.json').read_text())
            self.assertEqual(result['result'], 'FAIL')
            self.assertEqual(result['exit_code'], 0)
            self.assertTrue(result['reaped'])
            self.assertTrue(lab.log.closed)


if __name__ == '__main__':
    unittest.main()
