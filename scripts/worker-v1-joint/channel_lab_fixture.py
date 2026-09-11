"""Real Channel Lab IM process adapter for the disposable joint acceptance.

Only the existing Lab's public HTTP API is used. The Lab replaces Telegram's
external HTTP service; Control, Gateway, Worker and their persistence stay real.
Lab outgoing rows lack reply_parameters: delivery evidence correlates a serialized
per-chat outgoing delta, never an invented source_message_id observation.
"""
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

import gateway_fixture as original


def _write(path, value):
    path = Path(path)
    path.write_text(json.dumps(value, indent=2) + '\n')
    path.chmod(0o600)
    if json.loads(path.read_text()) != value:
        raise AssertionError('Channel Lab evidence readback')


class ChannelLab:
    """Own one isolated real app.py process and expose secret-free observations."""
    def __init__(self, h):
        self.h = h
        self.closed = False
        self.process = None
        self.token = self.secret = ''
        self.opener = build_opener(ProxyHandler({}))
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        self.url = 'http://127.0.0.1:' + str(port)
        directory = Path(h.work) / 'channel-lab-joint'
        directory.mkdir(mode=0o700)
        self.database = directory / 'channel-lab.sqlite'
        app = Path(h.root) / 'tools/channel-lab/app.py'
        self.log = open(directory / 'process.log', 'w')
        os.chmod(directory / 'process.log', 0o600)
        env = dict(getattr(h, 'env', os.environ), SSL_CERT_FILE=str(h.certs['ca']), PYTHONDONTWRITEBYTECODE='1')
        try:
            # This unmodified Python app exits -SIGTERM. Do not register it in
            # Harness.processes, whose service-specific contract is Go exit 0/-9.
            self.process = subprocess.Popen([sys.executable, '-B', str(app), '--host', '127.0.0.1', '--port', str(port), '--database', str(self.database), '--webhook-origin', h.urls['gateway_ingress']], cwd=h.root, env=env, stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True)
            self._wait_ready()
            bot = self.request('/lab/bots', {'name': 'Worker Summary joint IM'})
            self.bot_id, self.token, self.secret = int(bot['id']), bot['token'], bot['secret']
            h.secrets.extend([self.token, self.secret])
            _write(Path(h.artifacts) / 'channel-lab-process.json', {'implementation': str(app), 'implementation_sha256': hashlib.sha256(app.read_bytes()).hexdigest(), 'pid': self.process.pid, 'url': self.url, 'bot_id': self.bot_id, 'database': str(self.database), 'webhook_origin': h.urls['gateway_ingress'], 'ready': True, 'external_platform': 'existing Channel Lab local Telegram simulator', 'shared_18090_used': False})
        except BaseException:
            self.close()
            raise

    def _wait_ready(self, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError('Channel Lab exited before readiness')
            try:
                if self.request('/healthz').get('status') == 'ok':
                    return
            except RuntimeError:
                pass
            # A successful HTTP response with a wrong status is not readiness.
            # Bound every unsuccessful iteration, not only network exceptions.
            time.sleep(.03)
        raise RuntimeError('Channel Lab readiness deadline')

    def request(self, path, body=None):
        # Never surface URL/body-bearing HTTPError objects: bot tokens are in
        # Telegram protocol paths and /lab/state contains private credentials.
        payload = None if body is None else json.dumps(body).encode()
        request = Request(self.url + path, data=payload, headers={'Origin': self.url, 'Content-Type': 'application/json'}, method='GET' if body is None else 'POST')
        try:
            with self.opener.open(request, timeout=10) as response:
                data = response.read(8 * 1024 * 1024 + 1)
                if len(data) > 8 * 1024 * 1024:
                    raise RuntimeError('Channel Lab response size')
                return json.loads(data)
        except HTTPError as error:
            code = error.code
            error.close()
            raise RuntimeError('Channel Lab HTTP ' + str(code)) from None
        except (URLError, TimeoutError, OSError):
            raise RuntimeError('Channel Lab HTTP dependency') from None
        except ValueError:
            raise RuntimeError('Channel Lab malformed JSON') from None

    def chat(self, text, conversation_id='42', *, sender_id=100):
        if int(conversation_id) <= 0 or int(sender_id) <= 0:
            raise ValueError('Channel Lab joint input requires a positive private chat/user')
        return self.request('/lab/chat', {'bot': self.bot_id, 'user': int(sender_id), 'chat': int(conversation_id), 'text': text})

    def update_state(self, update_id):
        rows = [row for row in self.request('/lab/state')['updates'] if row['bot'] == self.bot_id and row['id'] == int(update_id)]
        if len(rows) != 1:
            raise AssertionError('Channel Lab update identity')
        return rows[0]

    def snapshot(self):
        messages = []
        for row in self.request('/lab/state')['messages']:
            if row['direction'] != 'out':
                continue
            body = json.loads(row['body'])
            messages.append({'row_id': row['id'], 'bot_id': row['bot'], 'chat_id': str(body['chat']['id']), 'message_id': body['message_id'], 'text': body.get('text', ''), **({'document': body['document']} if 'document' in body else {})})
        return messages

    def close(self):
        if self.closed:
            return
        self.closed = True
        expected = False
        forced = False
        try:
            if self.process is not None:
                expected = self.process.poll() is None
                if expected:
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        forced = True
                        self.process.kill()
                        self.process.wait(timeout=5)
                else:
                    self.process.wait(timeout=5)
        finally:
            self.log.close()
        result = {'result': 'PASS' if self.process is not None and expected and not forced and self.process.returncode == -signal.SIGTERM else 'FAIL', 'pid': self.process.pid if self.process else None, 'exit_code': self.process.returncode if self.process else None, 'termination': 'SIGTERM', 'expected_python_signal_exit': -signal.SIGTERM, 'forced_kill': forced, 'reaped': self.process is not None and self.process.poll() is not None, 'closed': True}
        _write(Path(self.h.artifacts) / 'channel-lab-cleanup.json', result)
        if result['result'] != 'PASS':
            raise RuntimeError('Channel Lab process cleanup failed')


class LabGateway:
    """Keep real Gateway lifecycle, replacing only external IM send/read helpers."""
    def __init__(self, gateway):
        self.gateway = gateway
        self.rounds = {}

    def __getattr__(self, name):
        return getattr(self.gateway, name)

    def send_text(self, text, conversation_id='42', update_id=None, *, sender_id=100):
        if update_id is not None:
            raise ValueError('Channel Lab owns update IDs; replay requires its explicit API')
        chat_id = str(int(conversation_id))
        if any(item['chat_id'] == chat_id and 'delivery' not in item for item in self.rounds.values()):
            raise AssertionError('Channel Lab delta correlation requires one unfinished input per chat')
        self._verify_chat(chat_id)
        lab, h = self.h.gateway_fixture, self.h
        before = [m for m in lab.snapshot() if m['bot_id'] == lab.bot_id and m['chat_id'] == chat_id]
        update = lab.chat(text, chat_id, sender_id=sender_id)
        assert update['message']['chat']['id'] == int(chat_id) and update['message']['text'] == text
        h.wait(lambda: lab.update_state(update['update_id'])['ack'] == 1, 'actual Channel Lab webhook acknowledged by Gateway', timeout=45)
        rows = []
        query = 'SELECT run_id FROM gateway.gateway_admissions WHERE account_id=' + original._literal(self.account_id) + ' AND event_id=' + original._literal(update['update_id'])
        def admitted():
            rows[:] = h.sql(query)
            return len(rows) == 1
        h.wait(admitted, 'actual Channel Lab IM created Gateway Admission', timeout=30)
        run_id = rows[0][0]
        # Lab serializes this exact returned update with json.dumps for webhook.
        self.updates[run_id] = json.dumps(update).encode()
        self.update_bots[run_id] = lab.bot_id
        self.rounds[run_id] = {'run_id': run_id, 'bot_id': lab.bot_id, 'chat_id': chat_id, 'update': update, 'outgoing_before': before, 'lab_update': lab.update_state(update['update_id']), 'ingress': 'POST /lab/chat -> real Lab HTTPS webhook -> Gateway Admission'}
        _write(Path(h.artifacts) / ('channel-lab-input-' + run_id + '.json'), self.rounds[run_id])
        return run_id

    def wait_delivery(self, run_id):
        round_ = self.rounds[run_id]
        h, lab = self.h, self.h.gateway_fixture
        result = []
        query = "SELECT i.intent_id,to_json(string_agg(CASE WHEN p.part_index < i.part_count - COALESCE(jsonb_array_length(i.intent->'Attachments'),0) THEN p.body ELSE '' END,'' ORDER BY p.part_index))::text,bool_and(p.state='ACCEPTED')::text,count(*)::text FROM gateway.gateway_delivery_intents i JOIN gateway.gateway_delivery_parts p ON p.intent_id=i.intent_id WHERE i.run_id=" + original._literal(run_id) + ' GROUP BY i.intent_id'
        def accepted():
            result[:] = h.sql(query)
            return len(result) == 1 and result[0][2] == 'true'
        h.wait(accepted, 'real Gateway Delivery ACCEPTED by Channel Lab', timeout=90)
        # Harness.sql splits psql rows and columns on newlines/tabs. Keep the
        # durable Final in JSON framing across that boundary, then recover it
        # byte-for-byte as text; NULL is not an empty accepted Final.
        result[0][1] = json.loads(result[0][1])
        assert isinstance(result[0][1], str), 'durable Final must be a JSON string, not SQL NULL'
        if 'delivery' in round_:
            self._verify_chat(round_['chat_id'])
            evidence = round_['delivery']
            assert result[0] == [evidence['intent_id'], evidence['final_text'], 'true', str(evidence['parts'])]
            selected = [m for m in lab.snapshot() if m['row_id'] in {row['row_id'] for row in evidence['outgoing_added']}]
            assert selected == evidence['outgoing_added'], 'Channel Lab original accepted output changed'
            return evidence
        before_ids = {m['row_id'] for m in round_['outgoing_before']}
        added = [m for m in lab.snapshot() if m['bot_id'] == round_['bot_id'] and m['chat_id'] == round_['chat_id'] and m['row_id'] not in before_ids]
        assert ''.join(m['text'] for m in added) == result[0][1], 'Channel Lab output differs from durable Final'
        assert len(added) == int(result[0][3]), 'unexpected duplicate or missing Channel Lab send'
        h.wait(lambda: h.sql("SELECT count(*) FROM gateway.gateway_reply_transport_receipts WHERE run_id=" + original._literal(run_id) + " AND outcome='ACCEPTED'")[0][0] != '0', 'Gateway durable Reply transport receipt for Lab', timeout=30)
        evidence = {'run_id': run_id, 'intent_id': result[0][0], 'final_text': result[0][1], 'parts': int(result[0][3]), 'delivery_state': 'ACCEPTED', 'external_platform': 'existing Channel Lab local Telegram simulator', 'correlation': 'serialized per-bot/chat outgoing delta after POST /lab/chat; source_message_id is not exposed by Lab', 'lab_update_id': round_['update']['update_id'], 'outgoing_before': round_['outgoing_before'], 'outgoing_added': added}
        _write(Path(h.artifacts) / ('gateway-delivery-' + run_id + '.json'), evidence)
        round_['delivery'] = evidence
        return evidence


    def _chat_observation(self, chat_id):
        rounds = [item for item in self.rounds.values() if item['chat_id'] == chat_id]
        if not rounds or any('delivery' not in item for item in rounds):
            return None
        expected = list(rounds[0]['outgoing_before'])
        for item in rounds:
            expected.extend(item['delivery']['outgoing_added'])
        actual = [m for m in self.h.gateway_fixture.snapshot() if m['bot_id'] == rounds[0]['bot_id'] and m['chat_id'] == chat_id]
        return {'chat_id': chat_id, 'bot_id': rounds[0]['bot_id'], 'run_ids': [item['run_id'] for item in rounds], 'expected': expected, 'actual': actual}

    def _verify_chat(self, chat_id):
        observation = self._chat_observation(chat_id)
        if observation:
            assert observation['actual'] == observation['expected'], 'Channel Lab late duplicate or changed output between serialized inputs'

    def close(self):
        # Stop the real Gateway before the last observation so it cannot send
        # after our outgoing audit. Keep Lab and ingress live until readback.
        evidence = {'result': 'PENDING', 'chats': []}
        try:
            self.gateway.stop()
            for chat_id in sorted({item['chat_id'] for item in self.rounds.values()}):
                observation = self._chat_observation(chat_id)
                if observation:
                    evidence['chats'].append(observation)
                    assert observation['actual'] == observation['expected'], 'Channel Lab late duplicate or changed output at shutdown'
            evidence['result'] = 'PASS'
        except BaseException:
            evidence['result'] = 'FAIL'
            raise
        finally:
            try:
                self.gateway.close()
            except BaseException:
                evidence['result'] = 'FAIL'
                raise
            finally:
                _write(Path(self.h.artifacts) / 'channel-lab-outgoing-final.json', evidence)


def prepare(h):
    """Compatibility entry: preserve existing ingress/Control config preparation."""
    env = original.prepare(h)
    h.gateway_fixture.close()
    h.gateway_fixture = ChannelLab(h)
    h.urls['telegram'] = h.gateway_fixture.url
    path = Path(h.artifacts) / 'fixture-resources.json'
    if path.exists():
        resources = json.loads(path.read_text())
        resources.setdefault('urls', {})['telegram'] = h.gateway_fixture.url
        resources['channel_lab'] = {'pid': h.gateway_fixture.process.pid, 'source': 'tools/channel-lab/app.py', 'isolated': True}
        _write(path, resources)
    return env


def start(h):
    """Use unchanged public Control Account/Binding and real Gateway binary."""
    gateway = LabGateway(original.start(h))
    h.gateway = gateway
    return gateway
