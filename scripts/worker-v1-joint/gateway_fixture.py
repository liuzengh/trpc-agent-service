"""Real Gateway process helper; only the external Telegram API/HTTPS ingress are fixtures.

Harness contract is intentionally small: prepare(h) before Control starts, then
start(h) after the owner-authenticated deployment seed. No SQL writes seed or
repair application facts. Telegram secrets remain in Control's credential API.
"""
from __future__ import annotations

from contextlib import ExitStack
import base64
import json
import secrets
import socket
import ssl
import threading
import time
import uuid
from email.parser import BytesParser
from email.policy import default
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

SCOPE = "joint-pool"
EPOCH = "00000000-0000-4000-8000-000000000001"
INSTANCE = "gateway-joint"
PRINCIPAL = "spiffe://agent-platform/channel-gateway"


def _literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def _write(path, value, private=False):
    path = Path(path)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    path.chmod(0o600 if private else 0o644)
    return str(path)


def _form(handler):
    if handler.headers.get("Transfer-Encoding", "").lower() == "chunked":
        chunks = []
        total = 0
        while True:
            line = handler.rfile.readline(128)
            if not line.endswith(b"\r\n"):
                raise ValueError("invalid chunk framing")
            size = int(line.strip().split(b";", 1)[0], 16)
            if size == 0:
                # SDK emits no trailers; consume the terminating CRLF only.
                if handler.rfile.readline(2) != b"\r\n":
                    raise ValueError("unexpected chunk trailer")
                break
            total += size
            if total > 1024 * 1024:
                raise ValueError("oversized external API request")
            chunk = handler.rfile.read(size)
            if len(chunk) != size or handler.rfile.read(2) != b"\r\n":
                raise ValueError("truncated chunk")
            chunks.append(chunk)
        raw = b"".join(chunks)
    else:
        size = int(handler.headers.get("Content-Length", "0"))
        if size < 0 or size > 1024 * 1024:
            raise ValueError("oversized external API request")
        raw = handler.rfile.read(size)
    content_type = handler.headers.get("Content-Type", "")
    if content_type.startswith("application/json"):
        return json.loads(raw or b"{}")
    if content_type.startswith("multipart/form-data"):
        envelope = b"Content-Type: " + content_type.encode() + b"\r\nMIME-Version: 1.0\r\n\r\n" + raw
        parsed = BytesParser(policy=default).parsebytes(envelope)
        return {part.get_param("name", header="content-disposition"): part.get_payload(decode=True).decode() for part in parsed.iter_parts()}
    return {key: values[0] for key, values in parse_qs(raw.decode()).items()}


class TelegramFixture:
    """Explicit fake external platform. Real SDK HTTP calls still authenticate."""
    def __init__(self, artifacts):
        self.artifacts = Path(artifacts)
        self.bot_id = 987654
        self.token = "987654:joint-synthetic-bot-token"
        self.secret = "joint_webhook_" + secrets.token_hex(16)
        self.lock = threading.Lock()
        self._bots = {self.token: {"bot_id": self.bot_id, "token": self.token, "secret": self.secret}}
        self.calls = []
        self.messages = []
        self.registrations = []
        self._reject_once = set()
        self._rejections = []
        self._response_loss = set()
        self._response_losses = []
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass  # token-bearing SDK URL must not enter logs

            def do_POST(self):
                token, separator, method = self.path.removeprefix("/bot").partition("/")
                with fixture.lock:
                    bot = fixture._bots.get(token) if self.path.startswith("/bot") and separator else None
                if bot is None:
                    self.send_error(401)
                    return
                try:
                    values = _form(self)
                    rejection = (method, bot["bot_id"], str(values.get("chat_id", "")), values.get("text", ""))
                    with fixture.lock:
                        reject = rejection in fixture._reject_once
                        if reject:
                            fixture._reject_once.remove(rejection)
                            fixture._rejections.append({"method": method, "bot_id": bot["bot_id"], "chat_id": rejection[2], "text": rejection[3], "status": 429, "accepted": False})
                            fixture.calls.append({"bot_id": bot["bot_id"], "method": method, "result": "rate_limited"})
                            fixture._save()
                    if reject:
                        raw = json.dumps({"ok": False, "error_code": 429, "description": "fixture rate limit", "parameters": {"retry_after": 1}}).encode()
                        self.send_response(429)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(raw)))
                        self.end_headers()
                        self.wfile.write(raw)
                        return
                    drop_response = False
                    with fixture.lock:
                        if method == "getMe":
                            result = {"id": bot["bot_id"], "is_bot": True, "first_name": "Joint fixture", "username": "joint_fixture_bot" if bot["bot_id"] == fixture.bot_id else "joint_fixture_bot_" + str(bot["bot_id"])}
                        elif method == "getWebhookInfo":
                            result = {"url": bot.get("webhook_url", ""), "pending_update_count": 0}
                        elif method == "setWebhook":
                            if values.get("secret_token") != bot["secret"] or not values.get("url", "").startswith("https://127.0.0.1:"):
                                raise ValueError("registration target or managed secret mismatch")
                            bot["webhook_url"] = values["url"]
                            fixture.registrations.append({"bot_id": bot["bot_id"], "url": values["url"], "allowed_updates": values.get("allowed_updates")})
                            result = True
                        elif method == "sendMessage":
                            chat = int(values["chat_id"])
                            reply = json.loads(values.get("reply_parameters", "{}"))
                            if chat == 0 or int(reply.get("message_id", 0)) <= 0 or not values.get("text"):
                                raise ValueError("original reply context missing")
                            result = {"message_id": 1000 + len(fixture.messages), "date": int(time.time()), "chat": {"id": chat, "type": "supergroup" if chat < 0 else "private"}, "text": values["text"]}
                            record = {"bot_id": bot["bot_id"], "chat_id": str(chat), "source_message_id": str(reply["message_id"]), "text": values["text"], "message_id": result["message_id"]}
                            if values.get("message_thread_id"):
                                thread = int(values["message_thread_id"])
                                if thread <= 0 or chat > 0:
                                    raise ValueError("invalid synthetic forum topic")
                                result["message_thread_id"] = thread
                                record["message_thread_id"] = str(thread)
                            fixture.messages.append(record)
                            fault = (bot["bot_id"], str(chat), values["text"])
                            if fault in fixture._response_loss:
                                fixture._response_loss.remove(fault)
                                fixture._response_losses.append({"phase": "accepted_before_http_response_loss",
                                    "http_response_written": False, "message": dict(record)})
                                drop_response = True
                        else:
                            raise ValueError("unexpected external API method")
                        fixture.calls.append({"bot_id": bot["bot_id"], "method": method, "result": "accepted"})
                        fixture._save()
                    if drop_response:
                        # The provider has already accepted/recorded the message.
                        # Emit neither an HTTP status nor a body; the real client
                        # observes connection loss, not proof of non-transmission.
                        self.close_connection = True
                        try:
                            self.connection.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        return
                    raw = json.dumps({"ok": True, "result": result}).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                except (ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
                    with fixture.lock:
                        fixture.calls.append({"bot_id": bot["bot_id"], "method": method, "result": "rejected", "error_type": type(error).__name__, "body_transport": self.headers.get("Transfer-Encoding", "content-length")})
                        fixture._save()
                    self.send_error(400, "invalid synthetic platform operation")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = "http://127.0.0.1:" + str(self.server.server_port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def _save(self):
        _write(self.artifacts / "telegram-external-fixture.json", {"external_platform": "synthetic Telegram HTTP", "calls": self.calls, "registrations": self.registrations, "messages": self.messages, "response_losses": self._response_losses})

    def lose_response_once(self, text, conversation_id, *, bot_id=None):
        """Arm one exact private bot/chat/Final, without changing other calls."""
        bot_id = self.bot_id if bot_id is None else bot_id
        if type(bot_id) is not int or not isinstance(text, str) or not text or not str(conversation_id).isdigit() or int(conversation_id) <= 0:
            raise ValueError("response-loss fixture requires an exact private message")
        with self.lock:
            if self._response_loss or not any(bot["bot_id"] == bot_id for bot in self._bots.values()):
                raise ValueError("response-loss fixture is armed or bot is absent")
            self._response_loss.add((bot_id, str(int(conversation_id)), text))

    def reject_once(self, method, *, text='', conversation_id='', bot_id=None):
        """One typed 429 before any external acceptance; getMe is preparation."""
        bot_id = self.bot_id if bot_id is None else bot_id
        if method not in ('getMe', 'sendMessage') or type(bot_id) is not int:
            raise ValueError('unsupported rejection fixture')
        if (method == 'getMe' and (text or conversation_id)) or (method == 'sendMessage' and (not text or not str(conversation_id).isdigit())):
            raise ValueError('rejection requires exact method input')
        with self.lock:
            if self._reject_once or not any(bot['bot_id'] == bot_id for bot in self._bots.values()):
                raise ValueError('rejection fixture is armed or bot is absent')
            self._reject_once.add((method, bot_id, str(conversation_id), text))

    def rejection_snapshot(self):
        with self.lock:
            return json.loads(json.dumps(self._rejections))

    def response_loss_snapshot(self):
        with self.lock:
            return json.loads(json.dumps(self._response_losses))

    def add_bot(self, bot_id):
        """Add one private-fixture identity; never mutate the default bot fields."""
        if type(bot_id) is not int or bot_id <= 0:
            raise ValueError("synthetic bot identity must be positive")
        with self.lock:
            if len(self._bots) >= 3 or any(bot["bot_id"] == bot_id for bot in self._bots.values()):
                raise ValueError("synthetic bot identity duplicates or exceeds fixture capacity")
            bot = {"bot_id": bot_id, "token": str(bot_id) + ":joint-" + secrets.token_urlsafe(24),
                   "secret": "joint_webhook_" + secrets.token_hex(16)}
            self._bots[bot["token"]] = bot
            return dict(bot)

    def remove_bot(self, bot_id):
        if bot_id == self.bot_id:
            raise ValueError("default fixture bot must remain available")
        with self.lock:
            tokens = [token for token, bot in self._bots.items() if bot["bot_id"] == bot_id]
            if len(tokens) != 1:
                raise ValueError("synthetic bot identity is not registered")
            del self._bots[tokens[0]]

    def bot_ids(self):
        with self.lock:
            return sorted(bot["bot_id"] for bot in self._bots.values())

    def snapshot(self):
        with self.lock:
            return json.loads(json.dumps(self.messages))

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class IngressFixture:
    """TLS termination fixture forwarding only the actual Gateway webhook route."""
    def __init__(self, h):
        target = h.urls["gateway"].rstrip("/")

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                if not self.path.startswith("/v1/telegram/") or "?" in self.path:
                    self.send_error(404)
                    return
                size = int(self.headers.get("Content-Length", "0"))
                if size > 1024 * 1024:
                    self.send_error(413)
                    return
                request = Request(target + self.path, data=self.rfile.read(size), headers={"Content-Type": "application/json", "X-Telegram-Bot-Api-Secret-Token": self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")}, method="POST")
                try:
                    response = urlopen(request, timeout=10)
                except HTTPError as error:
                    response = error
                except URLError:
                    self.send_error(503)
                    return
                with response:
                    raw = response.read()
                    self.send_response(response.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(h.certs["server_cert"], h.certs["server_key"])
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.url = "https://127.0.0.1:" + str(self.server.server_port)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def prepare(h):
    """Return Control Channel configuration env before Control binary starts."""
    directory = Path(h.work) / "gateway-joint"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    h.gateway_fixture = TelegramFixture(h.artifacts)
    h.secrets.extend([h.gateway_fixture.token, h.gateway_fixture.secret])
    h.gateway_ingress = IngressFixture(h)
    h.urls["telegram"] = h.gateway_fixture.url
    h.urls["gateway_ingress"] = h.gateway_ingress.url
    encryption_key = base64.b64encode(secrets.token_bytes(32)).decode()
    mac_key = base64.b64encode(secrets.token_bytes(32)).decode()
    h.secrets.extend([encryption_key, mac_key])
    keyring = _write(directory / "channel-keys.json", {"active_key_id": "joint-key", "keys": {"joint-key": {"encryption_key": encryption_key, "mac_key": mac_key}}}, True)
    nats = _write(directory / "control-nats.json", {"url": h.nats_url, "user": "control", "password": h.env["NATS_CONTROL_PASSWORD"], "ca_file": str(h.certs["ca"])}, True)
    address = urlparse(h.urls["control_channel"]).netloc
    config = _write(directory / "control-channel.json", {"route_nats_file": nats, "scope_id": SCOPE, "source_epoch": EPOCH, "internal_address": address, "tls_cert_file": str(h.certs["server_cert"]), "tls_key_file": str(h.certs["server_key"]), "client_ca_file": str(h.certs["ca"]), "credential_keys_file": keyring, "max_tenant_accounts": 100, "workloads": [{"principal_id": PRINCIPAL, "instance_id": INSTANCE, "scope_id": SCOPE, "audience": "control-channel-v1", "consumers": ["telegram_registration", "telegram_receiver", "telegram_webhook", "telegram_delivery"]}]})
    return {"CONTROL_CHANNEL_CONFIG_FILE": config}


class Gateway:
    def __init__(self, h, account_id, binding_id):
        self.h = h
        self.account_id = account_id
        self.binding_id = binding_id
        self.bot_id = h.gateway_fixture.bot_id
        self.webhook_secret = h.gateway_fixture.secret
        self.process = None
        self.process_number = 0
        self.updates = {}
        self.update_bots = {}
        self.ssl = ssl.create_default_context(cafile=str(h.certs["ca"]))

    def start(self):
        h = self.h
        if self.process is not None and self.process.poll() is None:
            raise RuntimeError("Gateway is already running")
        env = dict(h.env)
        env.update({"GATEWAY_ACCOUNT_SOURCE": "control", "GATEWAY_INSTANCE_ID": INSTANCE,
            "GATEWAY_HTTP_ADDRESS": urlparse(h.urls["gateway"]).netloc,
            "GATEWAY_ADMIN_ADDRESS": urlparse(h.urls["gateway_health"]).netloc,
            "GATEWAY_DATABASE_URL": h.dsns["gateway_runtime"], "GATEWAY_MIGRATION_DATABASE_URL": h.dsns["gateway_migrator"],
            "GATEWAY_NATS_URL": h.nats_url, "GATEWAY_NATS_USER": "gateway", "GATEWAY_NATS_PASSWORD": h.env["NATS_GATEWAY_PASSWORD"],
            "GATEWAY_NATS_CA_FILE": str(h.certs["ca"]), "GATEWAY_NATS_TOPOLOGY_FILE": str(Path(h.root) / "deploy/nats/streams.yaml"),
            "GATEWAY_CONTROL_URL": h.urls["control_channel"], "GATEWAY_CONTROL_SCOPE_ID": SCOPE, "GATEWAY_CONTROL_SOURCE_EPOCH": EPOCH,
            "GATEWAY_CONTROL_CA_FILE": str(h.certs["ca"]), "GATEWAY_CONTROL_CERT_FILE": str(h.certs["gateway_cert"]), "GATEWAY_CONTROL_KEY_FILE": str(h.certs["gateway_key"]),
            "GATEWAY_WORKER_URL": h.urls["worker"], "GATEWAY_WORKER_CA_FILE": str(h.certs["ca"]), "GATEWAY_WORKER_CERT_FILE": str(h.certs["gateway_cert"]), "GATEWAY_WORKER_KEY_FILE": str(h.certs["gateway_key"]),
            "GATEWAY_PUBLIC_ORIGIN": h.urls["gateway_ingress"], "GATEWAY_TELEGRAM_API_URL": h.urls["telegram"]})
        self.process_number += 1
        self.process = h.spawn("gateway-" + str(self.process_number), [str(h.binaries["channel-gateway"])], env)
        h.wait(self.ready, "real Gateway catalog/route/Telegram registration readiness", timeout=90)
        return self

    def ready(self):
        if self.process is None or self.process.poll() is not None:
            return False
        try:
            with urlopen(self.h.urls["gateway_health"] + "/readyz", timeout=2) as response:
                return response.status == 204
        except HTTPError as error:
            error.close()
            return False
        except (URLError, TimeoutError):
            return False

    def stop(self, kill=False):
        if self.process is not None and self.process.poll() is None:
            if kill:
                self.process.kill()
            else:
                self.process.terminate()
            self.process.wait(timeout=45)

    def _webhook(self, raw, secret):
        request = Request(self.h.urls["gateway_ingress"] + "/v1/telegram/" + self.account_id, data=raw, headers={"Content-Type": "application/json", "X-Telegram-Bot-Api-Secret-Token": secret}, method="POST")
        try:
            with urlopen(request, context=self.ssl, timeout=10) as response:
                return response.status, response.read()
        except HTTPError as error:
            with error:
                return error.code, error.read()

    def send_text(self, text, conversation_id="42", update_id=None, *, thread_id=None, chat_type="private", sender_id=100):
        update_id = int(update_id if update_id is not None else time.time_ns() // 1000000)
        message = {"message_id": update_id % 1000000000 + 1, "date": int(time.time()), "chat": {"id": int(conversation_id), "type": chat_type}, "from": {"id": int(sender_id), "is_bot": False}, "text": text}
        if thread_id is not None:
            if int(thread_id) <= 0 or chat_type != "supergroup" or int(conversation_id) >= 0:
                raise ValueError("forum input requires a negative supergroup chat and positive topic")
            message["message_thread_id"] = int(thread_id)
            message["is_topic_message"] = True
            message["chat"]["is_forum"] = True
        raw = json.dumps({"update_id": update_id, "message": message}).encode()
        def accepted():
            code, _ = self._webhook(raw, self.webhook_secret)
            if code not in (200, 503):
                raise AssertionError("unexpected real Gateway webhook status " + str(code))
            return code == 200
        self.h.wait(accepted, "Gateway durable webhook HTTP ACK", timeout=45)
        query = "SELECT run_id FROM gateway.gateway_admissions WHERE account_id=" + _literal(self.account_id) + " AND event_id=" + _literal(update_id)
        result = []
        def admitted():
            result[:] = self.h.sql(query)
            return len(result) == 1
        self.h.wait(admitted, "Gateway Admission persisted from authenticated ingress", timeout=30)
        run_id = result[0][0]
        self.updates[run_id] = raw
        self.update_bots[run_id] = self.bot_id
        return run_id

    def verify_webhook_auth_and_replay(self, run_id):
        raw = self.updates[run_id]
        assert self._webhook(raw, "wrong_synthetic_secret")[0] == 401
        assert self._webhook(raw, self.webhook_secret)[0] == 200
        event = json.loads(raw)["update_id"]
        assert self.h.sql("SELECT count(*) FROM gateway.gateway_admissions WHERE account_id=" + _literal(self.account_id) + " AND event_id=" + _literal(event)) == [["1"]]
        return {"wrong_secret_status": 401, "duplicate_status": 200, "admission_count": 1}

    def wait_delivery(self, run_id):
        result = []
        query = "SELECT i.intent_id,string_agg(p.body,'' ORDER BY p.part_index),bool_and(p.state='ACCEPTED')::text,count(*)::text FROM gateway.gateway_delivery_intents i JOIN gateway.gateway_delivery_parts p ON p.intent_id=i.intent_id WHERE i.run_id=" + _literal(run_id) + " GROUP BY i.intent_id"
        def accepted():
            result[:] = self.h.sql(query)
            return len(result) == 1 and result[0][2] == "true"
        self.h.wait(accepted, "real Gateway Delivery ACCEPTED after Worker proof", timeout=90)
        raw = json.loads(self.updates[run_id]) if run_id in self.updates else None
        if raw:
            bot_id = self.update_bots.get(run_id, self.h.gateway_fixture.bot_id)
            messages = [m for m in self.h.gateway_fixture.snapshot() if m["bot_id"] == bot_id and m["chat_id"] == str(raw["message"]["chat"]["id"]) and m["source_message_id"] == str(raw["message"]["message_id"])]
            assert "".join(m["text"] for m in messages) == result[0][1], "external response differs from durable Final"
            assert len(messages) == int(result[0][3]), "unexpected duplicate external send"
            if raw["message"].get("message_thread_id") is not None:
                assert all(m.get("message_thread_id") == str(raw["message"]["message_thread_id"]) for m in messages), "external topic differs from original input"
        self.h.wait(lambda: self.h.sql("SELECT count(*) FROM gateway.gateway_reply_transport_receipts WHERE run_id=" + _literal(run_id) + " AND outcome='ACCEPTED'")[0][0] != "0", "Gateway durable Reply transport receipt", timeout=30)
        evidence = {"run_id": run_id, "intent_id": result[0][0], "final_text": result[0][1], "parts": int(result[0][3]), "delivery_state": "ACCEPTED", "external_platform": "synthetic Telegram HTTP"}
        _write(Path(self.h.artifacts) / ("gateway-delivery-" + run_id + ".json"), evidence)
        return evidence

    def restart_and_replay(self, run_id):
        """Crash Gateway after a completed Final, replay original bytes on a new broker sequence.

        Caller may stop Worker first: immutable Delivery receipt must precede proof
        access. A fresh test publish MsgID deliberately avoids broker deduplication;
        payload/IntentID remain byte-identical and originate from Worker outbox.
        """
        self.wait_delivery(run_id)
        before = self.h.gateway_fixture.snapshot()
        rows = self.h.sql("SELECT intent_id,encode(payload,'hex') FROM worker.execution_reply_outbox WHERE run_id=" + _literal(run_id))
        assert len(rows) == 1
        self.stop(kill=True)
        ack = _publish_reply(self.h, bytes.fromhex(rows[0][1]))
        self.start()
        sequence = int(ack["seq"])
        query = "SELECT outcome,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE stream_sequence=" + str(sequence)
        self.h.wait(lambda: self.h.sql(query) == [["ACCEPTED", rows[0][0], run_id]], "replayed Reply durable transport receipt after process restart", timeout=45)
        time.sleep(2)  # at least two sender scans; terminal parts must stay quiet
        assert self.h.gateway_fixture.snapshot() == before, "completed Final resent after Gateway restart"
        assert self.h.sql("SELECT count(*) FROM gateway.gateway_delivery_intents WHERE run_id=" + _literal(run_id)) == [["1"]]
        result = {"run_id": run_id, "intent_id": rows[0][0], "replay_broker_sequence": sequence, "gateway_process_starts": self.process_number, "delivery_intent_count": 1, "external_send_count_unchanged": True}
        _write(Path(self.h.artifacts) / "gateway-restart-reply-replay.json", result)
        return result

    def close(self):
        self.stop()
        self.h.gateway_ingress.close()
        self.h.gateway_fixture.close()


def start(h):
    fixture = h.gateway_fixture
    base = "/v1/tenants/" + h.tenant_id
    created = h.api("POST", base + "/channel-accounts", {"provider": "telegram", "provider_account_id": str(fixture.bot_id), "name": "Joint external Telegram fixture", "config": {"receive_mode": "webhook"}, "credentials": {"telegram.bot_token": {"action": "replace", "value": fixture.token}, "telegram.webhook_secret": {"action": "replace", "value": fixture.secret}}}, status=201, idem="joint-channel-account")
    account_id = created["account"]["account_id"]
    binding = h.api("POST", base + "/channel-bindings", {"account_id": account_id, "target": {"deployment_id": h.deployment_id, "revision_number": 1}}, status=201, idem="joint-channel-binding")
    binding_id = binding["binding"]["binding_id"]
    target = binding["binding"]["target"]
    assert target["deployment_revision_id"] == h.revision_id and target["manifest_digest"] == h.manifest_digest
    h.api("POST", base + "/channel-accounts/" + account_id + "/enabled", {"expected_account_revision": 1, "enabled": True}, idem="joint-channel-account-enable")
    route = h.api("POST", base + "/channel-bindings/" + binding_id + "/enabled", {"expected_binding_revision": 1, "enabled": True}, idem="joint-channel-route-enable")
    assert route["event_id"] and route["distribution"] == "PENDING"
    _write(Path(h.artifacts) / "gateway-channel-publication.json", {"account_id": account_id, "binding_id": binding_id, "route_event_id": route["event_id"], "target": target})
    gateway = Gateway(h, account_id, binding_id)
    h.gateway = gateway
    return gateway.start()


def _publish_reply(h, raw):
    """Narrow TLS NATS client using Worker ACL, requiring actual JetStream PubAck."""
    url = urlparse(h.nats_url)
    assert url.scheme == "tls" and url.hostname in ("127.0.0.1", "::1")
    with ExitStack() as stack:
        sock = stack.enter_context(socket.create_connection((url.hostname, url.port), timeout=10))
        initial = stack.enter_context(sock.makefile("rb"))
        line = initial.readline()
        assert line.startswith(b"INFO "), "missing NATS greeting"
        initial.close()
        context = ssl.create_default_context(cafile=str(h.certs["ca"]))
        sock = stack.enter_context(context.wrap_socket(sock, server_hostname=url.hostname))
        reader = stack.enter_context(sock.makefile("rb"))
        inbox = "_INBOX.worker.joint." + uuid.uuid4().hex
        header = ("NATS/1.0\r\nNats-Msg-Id: joint-replay-" + uuid.uuid4().hex + "\r\n\r\n").encode()
        connect = {"verbose": False, "pedantic": True, "tls_required": True, "user": "worker", "pass": h.env["NATS_WORKER_PASSWORD"], "name": "joint-reply-replay", "lang": "python", "version": "1", "protocol": 1, "headers": True}
        sock.sendall(b"CONNECT " + json.dumps(connect).encode() + b"\r\nSUB " + inbox.encode() + b" 1\r\nHPUB execution.reply-intent.v1 " + inbox.encode() + b" " + str(len(header)).encode() + b" " + str(len(header) + len(raw)).encode() + b"\r\n" + header + raw + b"\r\nPING\r\n")
        while True:
            line = reader.readline()
            if line == b"PING\r\n":
                sock.sendall(b"PONG\r\n")
            elif line.startswith(b"MSG "):
                size = int(line.split()[-1])
                payload = reader.read(size)
                assert reader.read(2) == b"\r\n"
                ack = json.loads(payload)
                assert "error" not in ack and ack["stream"] == "REPLY_INTENTS_V1" and ack["seq"] > 0, ack
                return ack
            elif line.startswith(b"-ERR") or not line:
                raise RuntimeError("NATS replay publication rejected")
