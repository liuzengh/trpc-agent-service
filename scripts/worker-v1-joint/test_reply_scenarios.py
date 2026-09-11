"""Fast helper tests; no service/process gate is represented as unit evidence."""
import copy
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import reply_scenarios as reply


def event():
    fixture = Path(__file__).resolve().parents[2] / "api/events/execution/v1/fixtures/reply-intent-final.valid.json"
    value = json.loads(fixture.read_text())
    value["sequence"] = 1
    return value


class ReplyMutationTest(unittest.TestCase):
    def test_changes_are_independent_and_do_not_mutate_source(self):
        source = event()
        before = copy.deepcopy(source)
        cases = reply.mutations(source)
        self.assertEqual(source, before)
        self.assertEqual(set(cases), {"final_text", "execution_generation", "completion_id",
                                     "tenant_wire_injection", "manifest_wire_injection",
                                     "new_intent", "new_intent_generation"})
        for name, value in cases.items():
            self.assertEqual(value["run_id"], source["run_id"])
            self.assertEqual(value["admission_id"], source["admission_id"])
            self.assertNotEqual(reply.digest(reply.canonical_reply(value)),
                                reply.digest(reply.canonical_reply(source)))
            self.assertEqual(value["intent_id"] == source["intent_id"], not name.startswith("new_intent"))
        self.assertEqual(cases["execution_generation"]["execution"]["generation"], 2)
        self.assertEqual(cases["new_intent"]["execution"], source["execution"])
        self.assertEqual(cases["new_intent_generation"]["execution"]["generation"], 2)

    def test_canonical_digest_is_explicit_not_python_general_json_guessing(self):
        self.assertEqual(reply.canonical_reply({"z": "中文\n", "a": 1}), b'{"a":1,"z":"\xe4\xb8\xad\xe6\x96\x87\\n"}')
        for value in ({"a": 1.25}, {"a": 9007199254740992}, {"非schema": 1}):
            with self.assertRaises(ValueError):
                reply.canonical_reply(value)
        value = event()
        request = reply.proof_request(value)
        self.assertEqual(request["digest"], reply.digest(reply.canonical_reply(value)))
        self.assertEqual(request["execution_generation"], value["execution"]["generation"])
        self.assertNotIn("tenant_id", request)
        self.assertNotIn("manifest_digest", request)
        self.assertEqual(set(request), {"intent_id", "digest", "admission_id", "run_id", "attempt_id",
                                       "completion_id", "execution_generation", "sequence"})


class BrokerProtocolTest(unittest.TestCase):
    def test_handles_ping_and_reads_exact_response(self):
        class Socket:
            data = b""
            def sendall(self, data):
                self.data += data
        sock = Socket()
        raw = b'{"ok":true}'
        wire = b'PING\r\nPONG\r\nMSG _INBOX.reconciler.test 1 ' + str(len(raw)).encode() + b'\r\n' + raw + b'\r\n'
        self.assertEqual(reply._read_response(io.BytesIO(wire), sock), {"ok": True})
        self.assertEqual(sock.data, b"PONG\r\n")

    def test_rejects_auth_errors_truncation_and_oversize(self):
        for wire in (b'-ERR secret-not-to-copy\r\n', b'MSG inbox 1 3\r\n{}',
                     b'MSG inbox 1 2097153\r\n', b''):
            with self.assertRaises(RuntimeError) as error:
                reply._read_response(io.BytesIO(wire), None)
            self.assertNotIn("secret-not-to-copy", str(error.exception))

    def test_broker_operations_are_allowlisted_before_connect(self):
        with self.assertRaises(ValueError), patch.object(reply.socket, "create_connection") as connect:
            reply.broker_request(None, "$JS.API.STREAM.PURGE.REPLY_INTENTS_V1", {})
        connect.assert_not_called()
        with self.assertRaises(ValueError), patch.object(reply.socket, "create_connection") as connect:
            reply.broker_request(None, "$JS.API.STREAM.MSG.DELETE.REPLY_INTENTS_V1", {"seq": 1})
        connect.assert_not_called()

    def test_only_missing_message_is_interpreted_as_acknowledged(self):
        with patch.object(reply, "broker_request", return_value={"error": {"err_code": 10037}}):
            self.assertIsNone(reply.retained(None, 7))
        with patch.object(reply, "broker_request", return_value={"error": {"err_code": 10059}}):
            with self.assertRaises(AssertionError):
                reply.retained(None, 7)



class PendingInvariantTest(unittest.TestCase):
    def test_checks_real_redelivery_before_accepting_pending_evidence(self):
        current = {"delivered": {"consumer_seq": 21}, "num_redelivered": 0, "num_ack_pending": 1}
        with patch.object(reply, "consumer_info", return_value=current), \
                patch.object(reply, "retained") as retained:
            self.assertIsNone(reply.pending_evidence(None, 5, b"raw", "run", 0, 20))
        retained.assert_not_called()

    def test_second_negative_requires_both_messages_actually_redelivered(self):
        current = {"delivered": {"consumer_seq": 29}, "num_redelivered": 1, "num_ack_pending": 2}
        with patch.object(reply, "consumer_info", return_value=current), \
                patch.object(reply, "retained") as retained:
            self.assertIsNone(reply.pending_evidence(None, 6, b"raw", "run", 1, 24, expected_pending=2))
        retained.assert_not_called()

    def test_zero_receipts_and_delivery_are_required_while_work_is_retained(self):
        current = {"delivered": {"consumer_seq": 22}, "num_redelivered": 1, "num_ack_pending": 1}
        with patch.object(reply, "consumer_info", return_value=current), \
                patch.object(reply, "retained", return_value=b"raw"), \
                patch.object(reply, "receipt", return_value=[]), \
                patch.object(reply, "delivery_count", return_value=0):
            self.assertEqual(reply.pending_evidence(None, 5, b"raw", "run", 0, 20), current)
        for raw, receipt, count in ((None, [], 0), (b"raw", [["REJECTED"]], 0), (b"raw", [], 1)):
            with patch.object(reply, "consumer_info", return_value=current), \
                    patch.object(reply, "retained", return_value=raw), \
                    patch.object(reply, "receipt", return_value=receipt), \
                    patch.object(reply, "delivery_count", return_value=count):
                with self.assertRaises(AssertionError):
                    reply.pending_evidence(None, 5, b"raw", "run", 0, 20)


if __name__ == "__main__":
    unittest.main()
