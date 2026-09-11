"""WV-29: actual rejection-receipt storage failure, broker retry and recovery.

The only fault is a short-lived PostgreSQL trigger in the harness-owned database.
It rejects INSERT for one exact synthetic raw digest and REJECTED outcome. No
Run, Admission, Final, Delivery or receipt is inserted/updated by test code.
The original committed Final and mTLS proof come from the actual Worker; the
negative is published using the real Worker NATS ACL. deny_delete stays enabled.

Tenant/Manifest remain immutable proof-response fields, not legal Reply inputs.
This case does not fabricate a successful proof or claim that identity-mismatch
branch; its concrete new evidence is failure to persist a permanent rejection.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
import json
from pathlib import Path
import re
import subprocess
import tempfile
import uuid

from gateway_fixture import _literal, _publish_reply
from reply_scenarios import (canonical_reply, consumer_info, delivery_count,
                             digest, messages, receipt, retained, source_final,
                             stream_info, verify_proof)

TABLE = "gateway.gateway_reply_transport_receipts"


def _fixture(h):
    assert h.pg == h.prefix + "-pg" and h.pg in h.containers, "receipt fault requires its dedicated harness database"


def _admin(h, statement):
    _fixture(h)
    return h.command(["docker", "exec", h.pg, "psql", "-X", "-A", "-t",
                      "-v", "ON_ERROR_STOP=1", "-U", "platform_admin",
                      "-d", "agent_platform", "-c", statement])


def _fault_objects(h, name):
    return h.sql("SELECT (SELECT count(*) FROM pg_trigger WHERE tgrelid=" +
                 _literal(TABLE) + "::regclass AND tgname=" + _literal(name) +
                 "),(SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                 "WHERE n.nspname='gateway' AND p.proname=" + _literal(name) + ")")


@contextmanager
def rejection_storage_fault(h, raw_digest, journal):
    """Scoped storage fault; restore after body failure or uncertain DDL response."""
    _fixture(h)
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", raw_digest), "receipt fault digest malformed"
    suffix = uuid.uuid4().hex
    name = "joint_receipt_fault_" + suffix
    marker = "joint-receipt-storage-fault-" + suffix
    assert _fault_objects(h, name) == [["0", "0"]], "receipt fault object identity already exists"
    # The function uses no SECURITY DEFINER and modifies no row. A 53100 error
    # models storage exhaustion at the exact RecordTransportReceipt operation.
    install = """BEGIN;
SET LOCAL lock_timeout='5s';
CREATE FUNCTION gateway.%s() RETURNS trigger LANGUAGE plpgsql AS $joint$
BEGIN
  IF NEW.raw_digest = %s AND NEW.outcome = 'REJECTED' THEN
    RAISE EXCEPTION USING ERRCODE='53100', MESSAGE=%s;
  END IF;
  RETURN NEW;
END
$joint$;
CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION gateway.%s();
COMMIT;""" % (name, _literal(raw_digest), _literal(marker), name, TABLE, name)
    remove = ("BEGIN; SET LOCAL lock_timeout='5s'; DROP TRIGGER IF EXISTS " + name + " ON " + TABLE +
              "; DROP FUNCTION IF EXISTS gateway." + name + "(); COMMIT;")
    try:
        _admin(h, install)
        assert _fault_objects(h, name) == [["1", "1"]], "receipt storage fault not installed"
        fault = {"marker": marker, "object_name": name, "raw_digest": raw_digest,
                 "sqlstate": "53100", "scope": "REJECTED INSERT for one raw_digest",
                 "product_rows_written_by_test": 0}
        journal.append({"event": "rejection_storage_fault_installed", **fault})
        yield fault
    finally:
        _admin(h, remove)
        assert _fault_objects(h, name) == [["0", "0"]], "receipt storage fault not restored"
        journal.append({"event": "rejection_storage_fault_removed", "object_name": name,
                        "remaining_triggers": 0, "remaining_functions": 0})


def storage_failure_hits(h, marker):
    """Read only matching PostgreSQL ERROR lines; never save unrelated DB logs."""
    _fixture(h)
    assert re.fullmatch(r"joint-receipt-storage-fault-[0-9a-f]{32}", marker)
    command = ["docker", "logs", h.pg]
    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise RuntimeError("read dedicated PostgreSQL fault log failed")
    lines = (result.stdout + result.stderr).splitlines()
    errors = [line for line in lines if "ERROR:" in line and marker in line]
    return {"matching_error_count": len(errors), "marker": marker,
            "source": "actual PostgreSQL ERROR log", "command_exit": result.returncode}


def blocked_receipt(h, sequence, raw, run_id, original_messages, marker, baseline_consumer):
    """Two actual failed INSERTs prove this exact message was consumed/retried."""
    hits = storage_failure_hits(h, marker)
    if hits["matching_error_count"] < 2:
        return None
    current = consumer_info(h)
    assert current["delivered"]["consumer_seq"] >= baseline_consumer + 2, "receipt fault lacked broker redelivery"
    assert retained(h, sequence) == raw, "failed rejection receipt was ACKed or lost"
    assert receipt(h, sequence) == [], "failed rejection acquired premature terminal receipt"
    assert delivery_count(h, run_id) == 1, "negative Reply changed the committed Delivery barrier"
    assert messages(h, run_id) == original_messages, "negative Reply caused an extra external send"
    return {"postgres_fault": hits, "consumer": current, "message_retained": True,
            "terminal_receipts": 0, "delivery_intents": 1, "additional_external_sends": 0}


def run_receipt(h):
    """Use any running Worker; all assertions are keyed to this Run/sequence."""
    if not any(process.poll() is None for process in h.workers.values()):
        h.start_worker()
    marker = uuid.uuid4().hex[:12]
    conversation = str(2100000000 + int(marker[:7], 16))
    run_id = h.send_text("receipt recovery " + marker, conversation_id=conversation)
    delivery = h.wait_delivery(run_id)
    source = h.wait(lambda: source_final(h, run_id), "actual committed Reply outbox source", timeout=30)
    original_proof = verify_proof(h, source["event"])
    assert original_proof["status"] == 200
    assert original_proof["body"]["tenant_id"] == source["tenant_id"]
    altered = copy.deepcopy(source["event"])
    altered["content"]["text"] += " [uncommitted receipt-store negative]"
    altered_proof = verify_proof(h, altered)
    assert altered_proof == {"status": 409, "body": {"code": "FINAL_MISMATCH"}}
    raw = canonical_reply(altered)
    raw_digest = digest(raw)
    original_messages = messages(h, run_id)
    assert len(original_messages) == delivery["parts"] == 1
    before = stream_info(h)
    evidence = {"version": "worker-v1-receipt-storage/v1", "criterion": "WV-29",
                "run_id": run_id, "intent_id": delivery["intent_id"],
                "source_digest": source["digest"], "negative_raw_digest": raw_digest,
                "actual_original_mtls_proof": original_proof,
                "actual_altered_mtls_proof": altered_proof,
                "original_delivery": delivery, "published_as": "worker",
                "deny_delete": before["config"]["deny_delete"],
                "external_fixtures": ["model", "Telegram"], "events": [],
                "coverage_limits": ["same accepted Intent mutation rejects receipt-first as CONFLICT; separate real Worker proof reports FINAL_MISMATCH",
                                    "successful proof-response Tenant/Manifest mismatch is not manufactured by this test"],
                "product_rows_written_by_test": 0}
    path = Path(h.artifacts) / "receipt-scenarios.json"

    def save():
        path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")

    try:
        with rejection_storage_fault(h, raw_digest, evidence["events"]) as fault:
            baseline_consumer = consumer_info(h)["delivered"]["consumer_seq"]
            ack = _publish_reply(h, raw)
            sequence = int(ack["seq"])
            evidence["broker_sequence"] = sequence
            save()
            evidence["during_outage"] = h.wait(
                lambda: blocked_receipt(h, sequence, raw, run_id, original_messages, fault["marker"], baseline_consumer),
                "actual receipt INSERT fails twice, no ACK, same Reply retained", timeout=30)
            save()
        # Unchanged event bytes are already on the existing durable. No publish,
        # resubscribe/reset, table repair or manual ACK occurs during recovery.
        h.wait(lambda: receipt(h, sequence) == [["REJECTED", "CONFLICT", raw_digest]],
               "same Reply sequence rejection durable after storage restoration", timeout=30)
        h.wait(lambda: retained(h, sequence) is None,
               "same Reply ACK follows restored durable rejection receipt", timeout=30)
        after = stream_info(h)
        assert after["created"] == before["created"] and after["config"]["deny_delete"] is True
        assert messages(h, run_id) == original_messages and delivery_count(h, run_id) == 1
        proof_after = verify_proof(h, source["event"])
        assert proof_after == original_proof, "receipt-storage fault changed committed Worker proof"
        evidence["after_restoration"] = {"broker_sequence": sequence,
                                          "terminal_receipt": {"outcome": "REJECTED", "reason": "CONFLICT", "raw_digest": raw_digest},
                                          "broker_message_removed_after_durable_receipt": True,
                                          "delivery_intents": 1, "additional_external_sends": 0,
                                          "original_proof_unchanged": True, "deny_delete": True}
        evidence["result"] = "PASS"
        print("WORKER_RECEIPT_STORAGE_RECOVERY=PASS actual PG rejection INSERT failed twice; same broker sequence retained without ACK; storage restored then durable CONFLICT before ACK; zero extra sends", flush=True)
        return evidence
    finally:
        save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--race", action="store_true")
    args = parser.parse_args()
    from harness import Harness
    import gateway_fixture
    artifacts = args.artifacts or Path(tempfile.mkdtemp(prefix="worker-receipt-evidence-"))
    h = Harness(args.root, artifacts, args.race)
    print("RECEIPT_ARTIFACTS=" + str(h.artifacts), flush=True)
    try:
        h.provision()
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        h.gateway = gateway_fixture.start(h)
        run_receipt(h)
        return 0
    finally:
        h.close()


if __name__ == "__main__":
    raise SystemExit(main())
