"""WV-26/27/29 Reply checks against real Gateway/Worker/NATS processes.

Only model and Telegram remain external fixtures. All source Finals come from
the actual Worker outbox. Product SQL is read-only. Negative events use the
Worker broker identity; a reconciler identity only inspects broker retention.
Unproved negative events stay retained under the real deny_delete topology;
Harness.close eventually removes the entire private fixture broker.

Coverage is deliberately precise: Tenant/Manifest are not Reply wire fields,
so their injection tests the strict codec, not a forged successful proof. A new
Intent has no committed Worker proof and must remain retryable (404); this does
not claim to reach the later SQL UNIQUE(run_id) barrier. Rejected-receipt write
failure and authenticated proof-response identity mismatch remain separate gates.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
import socket
import ssl
import uuid
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from gateway_fixture import _literal, _publish_reply

STREAM = "REPLY_INTENTS_V1"
DURABLE = "channel-gateway-replies-v1"
SUBJECT = "execution.reply-intent.v1"
FINAL_PATH = "/internal/v1/execution/finals:verify"


def canonical_reply(value):
    """RFC8785-compatible serialization for this schema's ASCII keys/integers.

    Reject unsupported numeric/key types rather than pretending to be a general
    JCS library. The real outbox digest is also checked before deriving negatives.
    """
    def check(node):
        if isinstance(node, dict):
            for key, item in node.items():
                if not isinstance(key, str) or not key.isascii():
                    raise ValueError("Reply fixture expects ASCII schema keys")
                check(item)
        elif isinstance(node, list):
            for item in node:
                check(item)
        elif isinstance(node, bool) or node is None or isinstance(node, str):
            return
        elif isinstance(node, int) and abs(node) <= 9007199254740991:
            return
        else:
            raise ValueError("Reply fixture expects exact JSON integers")
    check(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def mutations(event):
    """Independent mutations, retaining the original Intent except WV-27."""
    result = {}
    body = copy.deepcopy(event)
    body["content"]["text"] += " [uncommitted mutation]"
    result["final_text"] = body
    generation = copy.deepcopy(event)
    generation["execution"]["generation"] += 1
    result["execution_generation"] = generation
    completion = copy.deepcopy(event)
    completion["execution"]["completion_id"] = "joint-uncommitted-" + uuid.uuid4().hex
    result["completion_id"] = completion
    tenant = copy.deepcopy(event)
    tenant["tenant_id"] = "joint-other-tenant"
    result["tenant_wire_injection"] = tenant
    manifest = copy.deepcopy(event)
    manifest["manifest_digest"] = "sha256:" + "f" * 64
    result["manifest_wire_injection"] = manifest
    for name, changed_generation in (("new_intent", False), ("new_intent_generation", True)):
        candidate = copy.deepcopy(event)
        candidate["intent_id"] = "joint-unproved-" + uuid.uuid4().hex
        if changed_generation:
            candidate["execution"]["generation"] += 1
        result[name] = candidate
    return result


def proof_request(event):
    return {"intent_id": event["intent_id"], "digest": digest(canonical_reply(event)),
            "admission_id": event["admission_id"], "run_id": event["run_id"],
            "attempt_id": event["execution"]["attempt_id"],
            "completion_id": event["execution"]["completion_id"],
            "execution_generation": event["execution"]["generation"],
            "sequence": event["sequence"]}


def verify_proof(h, event):
    """Actual Gateway mTLS identity to actual immutable Worker proof handler."""
    context = ssl.create_default_context(cafile=str(h.certs["ca"]))
    context.load_cert_chain(h.certs["gateway_cert"], h.certs["gateway_key"])
    body = proof_request(event)
    request = Request(h.urls["worker"].rstrip("/") + FINAL_PATH,
                      data=json.dumps(body).encode(), method="POST",
                      headers={"Content-Type": "application/json"})
    try:
        response = urlopen(request, context=context, timeout=8)
    except HTTPError as error:
        response = error
    with response:
        raw = response.read(4097)
        if len(raw) > 4096:
            raise AssertionError("actual Worker proof exceeded its protocol bound")
        value = json.loads(raw)
        if response.status == 200:
            assert all(value.get(key) == item for key, item in body.items()), "Worker proof differs from request"
        return {"status": response.status, "body": value}


def _read_response(reader, sock):
    """Read one bounded NATS request response; never expose auth/URL on failure."""
    while True:
        line = reader.readline(4097)
        if not line or len(line) > 4096:
            raise RuntimeError("joint broker response ended or exceeded framing bound")
        if line == b"PING\r\n":
            sock.sendall(b"PONG\r\n")
        elif line.startswith(b"MSG "):
            size = int(line.split()[-1])
            if size < 0 or size > 2 * 1024 * 1024:
                raise RuntimeError("joint broker response exceeded payload bound")
            raw = reader.read(size)
            if len(raw) != size or reader.read(2) != b"\r\n":
                raise RuntimeError("joint broker response was truncated")
            return json.loads(raw)
        elif line.startswith(b"-ERR"):
            raise RuntimeError("joint broker request rejected")


def broker_request(h, subject, body):
    allowed = {"$JS.API.STREAM.INFO." + STREAM,
               "$JS.API.CONSUMER.INFO." + STREAM + "." + DURABLE,
               "$JS.API.STREAM.MSG.GET." + STREAM}
    if subject not in allowed:
        raise ValueError("Reply helper broker operation is not allowlisted")
    url = urlparse(h.nats_url)
    if url.scheme != "tls" or url.hostname not in ("127.0.0.1", "::1"):
        raise ValueError("Reply helper requires its private TLS broker")
    sock = socket.create_connection((url.hostname, url.port), timeout=8)
    reader = None
    try:
        reader = sock.makefile("rb")
        if not reader.readline(4097).startswith(b"INFO "):
            raise RuntimeError("joint broker greeting missing")
        reader.close()
        reader = None
        context = ssl.create_default_context(cafile=str(h.certs["ca"]))
        sock = context.wrap_socket(sock, server_hostname=url.hostname)
        reader = sock.makefile("rb")
        inbox = "_INBOX.reconciler.reply-scenarios." + uuid.uuid4().hex
        connect = {"verbose": False, "pedantic": True, "tls_required": True,
                   "user": "reconciler", "pass": h.env["NATS_RECONCILER_PASSWORD"],
                   "name": "joint-reply-observation", "lang": "python", "version": "1",
                   "protocol": 1}
        payload = json.dumps(body).encode()
        sock.sendall(b"CONNECT " + json.dumps(connect).encode() + b"\r\nSUB " + inbox.encode() +
                     b" 1\r\nPUB " + subject.encode() + b" " + inbox.encode() + b" " +
                     str(len(payload)).encode() + b"\r\n" + payload + b"\r\nPING\r\n")
        return _read_response(reader, sock)
    finally:
        if reader is not None:
            reader.close()
        sock.close()


def stream_info(h):
    value = broker_request(h, "$JS.API.STREAM.INFO." + STREAM, {})
    assert "error" not in value and value["config"]["name"] == STREAM, "Reply stream INFO failed"
    assert value["config"].get("deny_delete") is True, "Reply source delete protection changed"
    return value


def consumer_info(h):
    value = broker_request(h, "$JS.API.CONSUMER.INFO." + STREAM + "." + DURABLE, {})
    assert "error" not in value and value["name"] == DURABLE, "Reply durable INFO failed"
    return {key: value[key] for key in ("delivered", "num_ack_pending", "num_redelivered", "num_pending")}


def retained(h, sequence):
    value = broker_request(h, "$JS.API.STREAM.MSG.GET." + STREAM, {"seq": int(sequence)})
    if "error" in value:
        assert value["error"].get("err_code") == 10037, "Reply MSG.GET unexpected error"
        return None
    message = value["message"]
    assert message["seq"] == int(sequence) and message["subject"] == SUBJECT
    return base64.b64decode(message["data"], validate=True)


def receipt(h, sequence):
    return h.sql("SELECT outcome,reason,raw_digest FROM gateway.gateway_reply_transport_receipts "
                 "WHERE stream_name='REPLY_INTENTS_V1' AND stream_sequence=" + str(int(sequence)))


def delivery_count(h, run_id):
    return int(h.sql("SELECT count(*) FROM gateway.gateway_delivery_intents WHERE run_id=" + _literal(run_id))[0][0])


def messages(h, run_id):
    update = json.loads(h.gateway.updates[run_id])["message"]
    return [message for message in h.gateway_fixture.snapshot()
            if message["chat_id"] == str(update["chat"]["id"])
            and message["source_message_id"] == str(update["message_id"])]


def pending_evidence(h, sequence, raw, run_id, expected_delivery_count, baseline_delivered, expected_pending=1):
    current = consumer_info(h)
    # This gate starts empty and tracks every pending negative it introduces.
    # Require all expected messages to be redelivered, so repeated deliveries of
    # the first unknown Intent cannot masquerade as consuming the second one.
    if (current["delivered"]["consumer_seq"] < baseline_delivered + 2
            or current["num_redelivered"] < expected_pending
            or current["num_ack_pending"] < expected_pending):
        return None
    assert retained(h, sequence) == raw, "Reply ACKed/lost while proof unavailable"
    assert receipt(h, sequence) == [], "unproved Reply was recorded terminal"
    assert delivery_count(h, run_id) == expected_delivery_count, "unproved Reply advanced Delivery"
    return current


def source_final(h, run_id):
    found = h.sql("SELECT tenant_id,digest,encode(payload,'hex'),published_at IS NOT NULL "
                  "FROM worker.execution_reply_outbox WHERE run_id=" + _literal(run_id))
    if not found or found[0][3] != "t":
        return None
    assert len(found) == 1, "one Run acquired multiple Worker Final outbox rows"
    raw = bytes.fromhex(found[0][2])
    event = json.loads(raw)
    assert event["run_id"] == run_id and canonical_reply(event) == raw
    assert digest(raw) == found[0][1], "source outbox canonical digest differs"
    return {"raw": raw, "event": event, "tenant_id": found[0][0], "digest": found[0][1]}


def run_reply(h):
    """One recovered real Final, five rejected mutations, two retained negatives.

    Call after the baseline two rounds and before unrelated fault injections.
    Gateway is restarted once to remove pre-existing proof keepalive sockets;
    Workers remain running. The L4 relay never terminates or fabricates TLS proof.
    """
    live = [name for name, process in h.workers.items() if process.poll() is None]
    if not live:
        h.start_worker()
        live = ["worker-one"]
    h.wait(lambda: (lambda info: info["num_pending"] == 0 and info["num_ack_pending"] == 0)(consumer_info(h)),
           "Reply matrix begins with settled existing durable", timeout=30)
    suffix = uuid.uuid4().hex[:12]
    text = "joint Reply proof recovery " + suffix
    chat = str(700000000 + int(suffix[:7], 16))
    h.model.hold(text)
    blocked = False
    evidence = {"gate": "WV-26/WV-27/WV-29", "external_fixtures": ["model", "Telegram"],
                "real_processes": ["Control", "Worker", "Gateway", "NATS", "PostgreSQL"],
                "coverage_limits": ["Tenant/Manifest injection is strict-wire rejection, not authenticated proof-response identity mismatch",
                                    "unknown Intent remains 404-retryable; SQL UNIQUE(run_id) branch is not reached",
                                    "transport rejection receipt write failure is not injected here"],
                "negatives": [], "retained_synthetic_negatives": [],
                "cleanup": "Keep deny_delete=true; Harness.close removes the entire private broker after all gates"}
    path = Path(h.artifacts) / "reply-scenarios.json"

    def save():
        path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n")

    def restore_proofs():
        for name in live:
            h.proof_switch.add(name, "127.0.0.1", h.worker_ports[name]["proof"])

    try:
        run_id = h.send_text(text, conversation_id=chat)
        h.model.wait_entered(text)
        # The model is already entered: Control's active proof/credential batch
        # finished. Disconnecting only future proof sockets now targets Final.
        h.gateway.stop()
        before_stream = stream_info(h)
        baseline = consumer_info(h)["delivered"]["consumer_seq"]
        for name in live:
            h.proof_switch.remove(name)
        blocked = True
        h.gateway.start()
        assert delivery_count(h, run_id) == 0 and messages(h, run_id) == []
        h.model.release(text)
        source = h.wait(lambda: source_final(h, run_id), "actual Worker committed/published Reply while proof disconnected", timeout=45)
        sequence = None
        after_stream = stream_info(h)
        assert after_stream["created"] == before_stream["created"], "Reply source incarnation changed"
        first, last = before_stream["state"]["last_seq"] + 1, after_stream["state"]["last_seq"]
        assert 0 < last - first + 1 <= 64, "unexpected concurrent Reply volume"
        for candidate in range(first, last + 1):
            if retained(h, candidate) == source["raw"]:
                assert sequence is None, "original Reply has multiple source publications"
                sequence = candidate
        assert sequence is not None, "original Reply disappeared before dependency recovery"
        pending = h.wait(lambda: pending_evidence(h, sequence, source["raw"], run_id, 0, baseline),
                         "real Reply redelivery without premature ACK or terminal receipt", timeout=30)
        assert messages(h, run_id) == [], "Final sent without available committed proof"
        evidence["dependency_outage"] = {"run_id": run_id, "stream_sequence": sequence,
                                         "source_digest": source["digest"], "consumer": pending,
                                         "message_retained": True, "transport_receipts": 0,
                                         "delivery_intents": 0, "external_sends": 0}
        save()
        restore_proofs()
        blocked = False
        delivered = h.wait_delivery(run_id)
        expected = [["ACCEPTED", "", source["digest"]]]
        h.wait(lambda: receipt(h, sequence) == expected, "same Reply sequence accepted after proof recovery", timeout=30)
        h.wait(lambda: retained(h, sequence) is None, "recovered Reply broker ACK removed work item", timeout=30)
        assert delivery_count(h, run_id) == 1 and len(messages(h, run_id)) == 1
        evidence["dependency_recovery"] = {"stream_sequence": sequence, "intent_id": delivered["intent_id"],
                                           "delivery_intents": 1, "external_sends": 1,
                                           "terminal_receipt": "ACCEPTED", "broker_message_removed_after_receipt": True}
        original_proof = verify_proof(h, source["event"])
        assert original_proof["status"] == 200
        assert original_proof["body"]["tenant_id"] == source["tenant_id"]
        assert original_proof["body"]["manifest_digest"] == h.manifest_digest
        evidence["actual_immutable_proof"] = original_proof
        baseline_messages = messages(h, run_id)
        for name, mutation in mutations(source["event"]).items():
            raw = canonical_reply(mutation)
            proof = None
            if name in ("final_text", "execution_generation", "completion_id"):
                proof = verify_proof(h, mutation)
                assert proof["status"] == 409, "actual Worker accepted altered immutable Final"
            elif name.startswith("new_intent"):
                proof = verify_proof(h, mutation)
                assert proof["status"] == 404, "new Intent unexpectedly has committed evidence"
            before = consumer_info(h)["delivered"]["consumer_seq"]
            ack = _publish_reply(h, raw)
            seq = int(ack["seq"])
            case = {"case": name, "run_id": run_id, "intent_id": mutation["intent_id"],
                    "stream_sequence": seq, "raw_digest": digest(raw), "published_as": "worker",
                    "actual_worker_proof": proof}
            if name.startswith("new_intent"):
                evidence["retained_synthetic_negatives"].append({"stream_sequence": seq, "raw_digest": digest(raw),
                                                                 "run_id": run_id, "intent_id": mutation["intent_id"]})
                save()
                expected_pending = len(evidence["retained_synthetic_negatives"])
                observed = h.wait(lambda: pending_evidence(h, seq, raw, run_id, 1, before, expected_pending),
                                  "same Run unproved Intent stays retained and produces no second Final", timeout=30)
                case.update(outcome="RETRY_PENDING", consumer=observed, message_retained=True,
                            sql_unique_branch_exercised=False, transport_receipts=0)
                case["cleanup"] = "retained_until_private_broker_teardown"
            else:
                reason = "INVALID_WIRE" if name.endswith("wire_injection") else "CONFLICT"
                h.wait(lambda: receipt(h, seq) == [["REJECTED", reason, digest(raw)]],
                       "negative Reply durable terminal rejection", timeout=30)
                h.wait(lambda: retained(h, seq) is None, "negative Reply ACK follows durable rejection", timeout=30)
                case.update(outcome="REJECTED", reason=reason, broker_message_removed=True)
            assert delivery_count(h, run_id) == 1, "Reply mutation crossed one-Final outcome boundary"
            assert messages(h, run_id) == baseline_messages, "Reply mutation caused an extra Telegram send"
            case.update(delivery_intents=1, additional_external_sends=0)
            evidence["negatives"].append(case)
            save()
        evidence["result"] = "PASS"
        save()
        print("WORKER_JOINT_REPLY_MATRIX=PASS actual mTLS immutable proof mismatches; strict Tenant/Manifest wire rejection; new Intent retained without duplicate Final; proof outage retains and redelivers same broker sequence after recovery", flush=True)
        return evidence
    finally:
        if blocked:
            restore_proofs()
        h.model.release(text)
        save()
