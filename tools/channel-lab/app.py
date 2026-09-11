#!/usr/bin/env python3
"""Local Telegram protocol laboratory. No product database access."""
import argparse
import hashlib
import http.client
import json
import os
import secrets
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Error(Exception):
    def __init__(self, code, description):
        self.code, self.description = code, description


class Lab:
    def __init__(self, path, webhook_origin):
        self.origin = webhook_origin.rstrip('/')
        u = urllib.parse.urlsplit(self.origin)
        if u.scheme not in ('http', 'https') or not u.hostname or u.path or u.query or u.fragment or u.username:
            raise ValueError('webhook origin must be an explicit HTTP(S) origin')
        self.cv = threading.Condition(threading.RLock())
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS bots(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, token TEXT UNIQUE NOT NULL, secret TEXT NOT NULL, webhook TEXT NOT NULL DEFAULT '', webhook_secret TEXT NOT NULL DEFAULT '', allowed TEXT NOT NULL DEFAULT '[]');
        CREATE TABLE IF NOT EXISTS updates(id INTEGER PRIMARY KEY AUTOINCREMENT, bot INTEGER NOT NULL, body TEXT NOT NULL, ack INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, retry REAL NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'queued');
        CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY AUTOINCREMENT, bot INTEGER NOT NULL, direction TEXT NOT NULL, body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS documents(file_id TEXT PRIMARY KEY,bot INTEGER NOT NULL,name TEXT NOT NULL,mime_type TEXT NOT NULL,sha256 TEXT NOT NULL,body BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS model_requests(id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        ''')
        self.db.execute('INSERT OR IGNORE INTO settings VALUES (?,?)', ('model_key', secrets.token_urlsafe(32)))
        self.db.commit()
        self.stop = threading.Event()
        self.pollers = set()

    def bot(self, token):
        b = self.db.execute('SELECT * FROM bots WHERE token=?', (token,)).fetchone()
        if b is None:
            raise Error(401, 'invalid laboratory bot token')
        return b

    def create(self, name):
        with self.cv:
            # Allocate the Bot ID before forming a syntactically valid token.
            c = self.db.execute('INSERT INTO bots(name,token,secret) VALUES(?,?,?)', (name[:80], secrets.token_urlsafe(32), secrets.token_urlsafe(24)))
            ident = c.lastrowid
            self.db.execute('UPDATE bots SET token=? WHERE id=?', (str(ident)+':'+secrets.token_urlsafe(32), ident))
            self.db.commit()
            return dict(self.db.execute('SELECT * FROM bots WHERE id=?', (ident,)).fetchone())

    def chat(self, bot, user, chat, text):
        if not text or len(text) > 4096 or user <= 0 or chat == 0:
            raise Error(400, 'invalid text or user/chat ID')
        with self.cv:
            if not self.db.execute('SELECT 1 FROM bots WHERE id=?', (bot,)).fetchone():
                raise Error(404, 'bot not found')
            c = self.db.execute('INSERT INTO updates(bot,body) VALUES(?,?)', (bot, '{}'))
            ident = c.lastrowid
            msg = {'message_id': ident, 'date': int(time.time()), 'from': {'id': user, 'is_bot': False, 'first_name': 'User '+str(user)}, 'chat': {'id': chat, 'type': 'private' if chat > 0 else 'supergroup'}, 'text': text}
            update = {'update_id': ident, 'message': msg}
            self.db.execute('UPDATE updates SET body=? WHERE id=?', (json.dumps(update), ident))
            self.db.execute('INSERT INTO messages(bot,direction,body) VALUES(?,?,?)', (bot, 'in', json.dumps(msg)))
            self.db.commit()
            self.cv.notify_all()
            return update

    def replay(self, bot, ident):
        with self.cv:
            c = self.db.execute("UPDATE updates SET ack=0,attempts=0,retry=0,status='replay' WHERE id=? AND bot=?", (ident, bot))
            self.db.commit()
            if c.rowcount != 1:
                raise Error(404, 'update not found')
            self.cv.notify_all()
            return True

    def api(self, token, method, data):
        with self.cv:
            b = self.bot(token)
            bid = b['id']
            if method == 'getMe':
                return {'id': bid, 'is_bot': True, 'first_name': b['name'], 'username': 'lab_'+str(bid)+'_bot'}
            if method == 'getWebhookInfo':
                count = self.db.execute('SELECT count(*) FROM updates WHERE bot=? AND ack=0', (bid,)).fetchone()[0]
                return {'url': b['webhook'], 'has_custom_certificate': False, 'pending_update_count': count}
            if method in ('setWebhook', 'deleteWebhook'):
                url = data.get('url', '') if method == 'setWebhook' else ''
                parsed = urllib.parse.urlsplit(url)
                if url and (not url.startswith(self.origin+'/v1/telegram/') or parsed.query or parsed.fragment or parsed.username or not parsed.path.removeprefix('/v1/telegram/') or '/' in parsed.path.removeprefix('/v1/telegram/')):
                    raise Error(400, 'webhook must use configured Gateway origin and account route')
                allowed = data.get('allowed_updates', [])
                if isinstance(allowed, str):
                    allowed = json.loads(allowed)
                if allowed and allowed != ['message'] and set(allowed) != {'message', 'callback_query'}:
                    raise Error(400, 'laboratory supports text message updates only')
                self.db.execute('UPDATE bots SET webhook=?,webhook_secret=?,allowed=? WHERE id=?', (url, data.get('secret_token', '') if url else '', json.dumps(allowed), bid))
                if data.get('drop_pending_updates') in (True, 'true'):
                    self.db.execute("UPDATE updates SET ack=1,status='dropped' WHERE bot=? AND ack=0", (bid,))
                self.db.commit()
                self.cv.notify_all()
                return True
            if method == 'getUpdates':
                if b['webhook']:
                    raise Error(409, 'webhook active')
                offset, limit, timeout = int(data.get('offset', 0)), int(data.get('limit', 100)), int(data.get('timeout', 0))
                if not 1 <= limit <= 100 or not 0 <= timeout <= 50 or offset < 0:
                    raise Error(400, 'supported: offset >= 0, limit 1..100, timeout 0..50')
                if bid in self.pollers:
                    raise Error(409, 'another laboratory polling request is active')
                self.pollers.add(bid)
                try:
                    self.db.execute("UPDATE updates SET ack=1,status='confirmed' WHERE bot=? AND id<?", (bid, offset))
                    self.db.commit()
                    deadline = time.monotonic()+timeout
                    while True:
                        if self.bot(token)['webhook']:
                            raise Error(409, 'webhook active')
                        rows = self.db.execute('SELECT body FROM updates WHERE bot=? AND ack=0 AND id>=? ORDER BY id LIMIT ?', (bid, offset, limit)).fetchall()
                        if rows or time.monotonic() >= deadline or self.stop.is_set():
                            return [json.loads(r[0]) for r in rows]
                        self.cv.wait(min(deadline-time.monotonic(), 1))
                finally:
                    self.pollers.discard(bid)
            if method in ('sendMessage', 'sendDocument'):
                document = data.get('document') if method == 'sendDocument' else None
                if method == 'sendDocument' and (not isinstance(document, dict) or not isinstance(document.get('bytes'), bytes) or not document.get('filename')):
                    raise Error(400, 'document must be an actual multipart upload')
                chat, text = int(data['chat_id']), data.get('caption', '') if document else data['text']
                if not document and (not text or len(text)>4096):
                    raise Error(400, 'text must contain 1..4096 characters')
                known = self.db.execute("SELECT body FROM messages WHERE bot=? AND direction='in'", (bid,)).fetchall()
                source = [json.loads(r[0]) for r in known]
                if not any(m['chat']['id'] == chat for m in source):
                    raise Error(400, 'unknown laboratory chat')
                reply = data.get('reply_parameters', {})
                if isinstance(reply, str):
                    reply = json.loads(reply)
                if reply and not any(m['chat']['id']==chat and m['message_id']==int(reply['message_id']) for m in source):
                    raise Error(400, 'reply source not found')
                c = self.db.execute('INSERT INTO messages(bot,direction,body) VALUES(?,?,?)', (bid, 'out', '{}'))
                msg = {'message_id': 1000000000+c.lastrowid, 'date': int(time.time()), 'chat': {'id': chat, 'type': 'private' if chat > 0 else 'supergroup'}, 'text': text}
                if document:
                    body = document['bytes']
                    digest = hashlib.sha256(body).hexdigest()
                    file_id = 'lab_document_'+str(c.lastrowid)
                    mime = document['mime_type']
                    self.db.execute('INSERT INTO documents VALUES(?,?,?,?,?,?)', (file_id,bid,document['filename'],mime,digest,body))
                    msg.pop('text', None)
                    msg['document'] = {'file_id':file_id,'file_unique_id':digest,'file_name':document['filename'],'mime_type':mime,'file_size':len(body),'sha256':digest}
                    if text: msg['caption'] = text
                if data.get('message_thread_id'): msg['message_thread_id'] = int(data['message_thread_id'])
                if reply: msg['reply_to_message'] = next(m for m in source if m['chat']['id']==chat and m['message_id']==int(reply['message_id']))
                self.db.execute('UPDATE messages SET body=? WHERE id=?', (json.dumps(msg), c.lastrowid))
                self.db.commit()
                return msg
            raise Error(404, 'unsupported laboratory method')

    def deliver_once(self):
        with self.cv:
            r = self.db.execute('SELECT u.*, b.webhook,b.webhook_secret FROM updates u JOIN bots b ON b.id=u.bot WHERE u.ack=0 AND u.attempts<5 AND u.retry<=? AND b.webhook<>\'\' ORDER BY u.id LIMIT 1', (time.time(),)).fetchone()
            if r is None:
                return False
            item = dict(r)
        req = urllib.request.Request(item['webhook'], data=item['body'].encode(), headers={'Content-Type': 'application/json', 'X-Telegram-Bot-Api-Secret-Token': item['webhook_secret']})
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args):
                return None
        try:
            with urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(req, timeout=5) as res:
                status = res.status
        except urllib.error.HTTPError as exc:
            status = exc.code
            exc.close()
        except (OSError, urllib.error.URLError):
            status = 0
        with self.cv:
            # Do not apply a stale in-flight ACK after a configuration change.
            b = self.db.execute('SELECT webhook,webhook_secret FROM bots WHERE id=?', (item['bot'],)).fetchone()
            if b and (b['webhook'],b['webhook_secret']) == (item['webhook'],item['webhook_secret']):
                self.db.execute('UPDATE updates SET ack=?,attempts=attempts+1,retry=?,status=? WHERE id=?', (int(200<=status<300), time.time()+min(30,2**item['attempts']), 'HTTP '+str(status), item['id']))
                self.db.commit()
        return True

    def snapshot(self):
        with self.cv:
            return {'bots': [dict(r) for r in self.db.execute('SELECT * FROM bots')], 'messages': [dict(r) for r in self.db.execute('SELECT * FROM messages')], 'updates': [dict(r) for r in self.db.execute('SELECT id,bot,attempts,status,ack FROM updates')], 'model_requests': [dict(r) for r in self.db.execute('SELECT * FROM model_requests ORDER BY id DESC LIMIT 20')], 'model_proxy': self.model_config(), 'model_key': self.db.execute("SELECT value FROM settings WHERE key='model_key'").fetchone()[0]}

    def model_config(self, private=False):
        with self.cv:
            row = self.db.execute("SELECT value FROM settings WHERE key='model_proxy'").fetchone()
            config = json.loads(row[0]) if row else {'mode': 'echo', 'base_url': '', 'model': '', 'api_key': ''}
            if private:
                return config
            return {k: v for k, v in config.items() if k != 'api_key'} | {'key_configured': bool(config.get('api_key'))}

    def configure_model(self, data):
        if data.get('mode') not in ('echo', 'proxy'):
            raise Error(400, 'mode must be echo or proxy')
        with self.cv:
            old = self.model_config(private=True)
            base = data.get('base_url', old['base_url'])
            model = data.get('model', old['model'])
            key = data.get('api_key', '')
            if not all(isinstance(v, str) for v in (base, model, key)):
                raise Error(400, 'invalid upstream settings')
            base, model = base.strip().rstrip('/'), model.strip()
            if base:
                u = urllib.parse.urlsplit(base)
                if u.scheme not in ('http', 'https') or not u.hostname or u.username or u.password or u.query or u.fragment or any(c.isspace() for c in base):
                    raise Error(400, 'base URL must be an HTTP(S) API base without credentials or query')
                if base.endswith('/chat/completions'):
                    raise Error(400, 'enter API base URL, not /chat/completions')
            if len(base) > 2048 or len(model) > 256 or len(key) > 8192 or any(c in key for c in ('\r', '\n')):
                raise Error(400, 'invalid upstream settings')
            if data.get('clear_key') is True:
                key = ''
            elif not key:
                # A changed destination must not silently receive the old key.
                key = old['api_key'] if base == old['base_url'] else ''
            if data['mode'] == 'proxy' and not (base and model and key):
                raise Error(400, 'proxy requires base URL, model and upstream API key')
            config = {'mode': data['mode'], 'base_url': base, 'model': model, 'api_key': key}
            self.db.execute('INSERT OR REPLACE INTO settings VALUES (?,?)', ('model_proxy', json.dumps(config)))
            self.db.commit()
            return self.model_config()

    def close(self):
        self.stop.set()
        with self.cv:
            self.cv.notify_all()


def server(lab, address):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass  # Bot token is part of protocol URLs.

        def respond(self, code, body):
            raw = json.dumps(body).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(raw)

        def proxy_model(self, data, config):
            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, *args, **kwargs):
                    return None
            payload = dict(data, model=config['model'])
            request = urllib.request.Request(config['base_url']+'/chat/completions',
                data=json.dumps(payload).encode(), headers={
                    'Content-Type': 'application/json',
                    'Accept': 'text/event-stream' if data.get('stream') else 'application/json',
                    'Authorization': 'Bearer '+config['api_key'],
                })
            try:
                upstream = urllib.request.build_opener(NoRedirect()).open(request, timeout=30)
            except urllib.error.HTTPError as e:
                status = e.code
                e.close()
                raise Error(502, 'upstream HTTP '+str(status))
            except (TimeoutError, socket.timeout):
                raise Error(504, 'upstream timeout')
            except (urllib.error.URLError, OSError, ValueError):
                raise Error(502, 'upstream connection failed')
            with upstream:
                if not data.get('stream'):
                    try:
                        body = upstream.read(8*1024*1024+1)
                        if len(body) > 8*1024*1024:
                            raise Error(502, 'upstream response too large')
                        result = json.loads(body)
                        if not isinstance(result, dict) or not isinstance(result.get('choices'), list):
                            raise Error(502, 'invalid upstream completion')
                    except (ValueError, OSError, http.client.HTTPException):
                        raise Error(502, 'invalid upstream response')
                    return self.respond(200, result)
                if upstream.headers.get_content_type() != 'text/event-stream':
                    raise Error(502, 'upstream did not return SSE')
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Cache-Control', 'no-store')
                self.send_header('X-Accel-Buffering', 'no')
                self.send_header('Connection', 'close')
                self.end_headers()
                self.close_connection = True
                total, deadline = 0, time.monotonic()+120
                try:
                    while time.monotonic() < deadline:
                        chunk = upstream.read1(65536)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > 8*1024*1024:
                            break
                        self.wfile.write(chunk)
                        self.wfile.flush()
                except (OSError, TimeoutError, http.client.HTTPException):
                    pass  # Close incomplete streams; never invent a successful [DONE].

        def do_GET(self):
            if self.path == '/healthz':
                return self.respond(200, {'status': 'ok', 'mode': 'simulated'})
            if self.path.startswith('/lab/documents/'):
                file_id = self.path.removeprefix('/lab/documents/')
                with lab.cv:
                    item = lab.db.execute('SELECT body,sha256 FROM documents WHERE file_id=?', (file_id,)).fetchone()
                if item is None: return self.respond(404, {'error':'document not found'})
                self.send_response(200)
                self.send_header('Content-Type','application/octet-stream')
                self.send_header('Content-Disposition','attachment')
                self.send_header('X-Content-Type-Options','nosniff')
                self.send_header('X-Content-SHA256',item['sha256'])
                self.send_header('Cache-Control','no-store')
                self.send_header('Content-Length',str(len(item['body'])))
                self.end_headers()
                self.wfile.write(item['body'])
                return
            if self.path == '/lab/state':
                return self.respond(200, lab.snapshot())
            if self.path != '/':
                return self.respond(404, {'error': 'not found'})
            raw = (Path(__file__).parent/'web/index.html').read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            try:
                if self.path.startswith('/lab/') and (self.headers.get('Origin') != 'http://'+self.headers.get('Host','') or self.headers.get('Content-Type') != 'application/json'):
                    raise Error(403, 'same-origin JSON required')
                limit = 51*1024*1024 if self.path.startswith('/bot') and self.path.endswith('/sendDocument') else 1024*1024
                size = int(self.headers.get('Content-Length', 0))
                if not 0 <= size <= limit:
                    raise Error(413, 'request too large')
                if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
                    chunks, total = [], 0
                    while True:
                        line = self.rfile.readline(128)
                        count = int(line.strip().split(b';')[0],16)
                        if count == 0:
                            if self.rfile.readline(2) != b'\r\n':
                                raise Error(400, 'unsupported trailers')
                            break
                        total += count
                        if total > limit:
                            raise Error(413, 'request too large')
                        chunk = self.rfile.read(count)
                        if len(chunk) != count or self.rfile.read(2) != b'\r\n':
                            raise Error(400, 'invalid chunk')
                        chunks.append(chunk)
                    raw = b''.join(chunks)
                else:
                    raw = self.rfile.read(size)
                content_type = self.headers.get('Content-Type', '')
                if content_type.startswith('application/json'):
                    data = json.loads(raw or b'{}')
                elif content_type.startswith('multipart/form-data'):
                    from email.parser import BytesParser
                    from email.policy import default
                    mail = BytesParser(policy=default).parsebytes(b'Content-Type: '+content_type.encode()+b'\r\nMIME-Version: 1.0\r\n\r\n'+raw)
                    data = {}
                    for part in mail.iter_parts():
                        name = part.get_param('name',header='content-disposition')
                        if name in data: raise Error(400, 'duplicate multipart field')
                        body = part.get_payload(decode=True)
                        data[name] = {'filename':part.get_filename(),'mime_type':part.get_content_type(),'bytes':body} if part.get_filename() is not None else body.decode()
                else:
                    data = {k:v[0] for k,v in urllib.parse.parse_qs(raw.decode()).items()}
                if self.path == '/v1/chat/completions':
                    with lab.cv:
                        key = lab.db.execute("SELECT value FROM settings WHERE key='model_key'").fetchone()[0]
                        if not secrets.compare_digest(self.headers.get('Authorization', ''), 'Bearer '+key):
                            raise Error(401, 'invalid laboratory model key')
                        if data.get('model') != 'lab-echo' or not isinstance(data.get('messages'),list):
                            raise Error(400, 'model must be lab-echo with messages')
                        if not all(isinstance(m, dict) for m in data['messages']):
                            raise Error(400, 'messages must contain objects')
                        config = lab.model_config(private=True)
                        texts = [m.get('content','') for m in data['messages'] if m.get('role')=='user']
                        if config['mode'] == 'echo' and (not texts or not isinstance(texts[-1],str)):
                            raise Error(400, 'text user message required')
                        lab.db.execute('INSERT INTO model_requests(body) VALUES(?)', (json.dumps(data),))
                        lab.db.commit()
                    if config['mode'] == 'proxy':
                        return self.proxy_model(data, config)
                    answer = 'lab echo: '+texts[-1]
                    result = {'id':'chatcmpl-lab-'+secrets.token_hex(8),'object':'chat.completion','created':int(time.time()),'model':'lab-echo','choices':[{'index':0,'message':{'role':'assistant','content':answer},'finish_reason':'stop'}]}
                    if not data.get('stream'):
                        return self.respond(200,result)
                    result['object']='chat.completion.chunk'
                    events=[]
                    for delta, finish in [({'role':'assistant','content':answer},None),({},'stop')]:
                        result['choices']=[{'index':0,'delta':delta,'finish_reason':finish}]
                        events.append('data: '+json.dumps(result)+'\n\n')
                    raw=(''.join(events)+'data: [DONE]\n\n').encode()
                    self.send_response(200)
                    self.send_header('Content-Type','text/event-stream')
                    self.send_header('Content-Length',str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                if self.path.startswith('/bot'):
                    token, method = self.path[4:].split('/',1)
                    return self.respond(200, {'ok': True, 'result': lab.api(token,method,data)})
                if self.path == '/lab/model':
                    return self.respond(200, lab.configure_model(data))
                if self.path == '/lab/bots':
                    return self.respond(201, lab.create(data['name']))
                if self.path == '/lab/chat':
                    return self.respond(201, lab.chat(int(data['bot']),int(data['user']),int(data['chat']),data['text']))
                if self.path == '/lab/replay':
                    return self.respond(200, lab.replay(int(data['bot']),int(data['update'])))
                raise Error(404, 'not found')
            except Error as e:
                self.respond(e.code, {'ok': False, 'error_code': e.code, 'description': e.description})
            except (ValueError, KeyError, TypeError):
                self.respond(400, {'ok': False, 'error_code': 400, 'description': 'invalid request'})
    return ThreadingHTTPServer(address, Handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', default=8080,type=int)
    parser.add_argument('--database', default='channel-lab.sqlite')
    parser.add_argument('--webhook-origin', default='http://channel-gateway:8090')
    a = parser.parse_args()
    os.umask(0o077)
    lab = Lab(a.database,a.webhook_origin)
    def delivery():
        while not lab.stop.is_set():
            lab.deliver_once()
            lab.stop.wait(.2)
    threading.Thread(target=delivery,daemon=True).start()
    http = server(lab,(a.host,a.port))
    try:
        http.serve_forever()
    finally:
        lab.close()
        http.server_close()

if __name__ == '__main__':
    main()
