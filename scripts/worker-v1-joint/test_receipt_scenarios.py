"""Fast fixture restoration/invariant checks, separate from the actual gate."""
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import receipt_scenarios as receipt


class Fixture:
    prefix = "worker-joint-unit"
    pg = prefix + "-pg"
    containers = [pg]

    def __init__(self):
        self.installed = False
        self.statements = []
        self.uncertain_install = False

    def command(self, argv, **kwargs):
        statement = argv[-1]
        self.statements.append(statement)
        if "CREATE TRIGGER" in statement:
            self.installed = True
            if self.uncertain_install:
                raise RuntimeError("uncertain install response")
        if "DROP TRIGGER" in statement:
            self.installed = False
        return "COMMIT\n"

    def sql(self, query):
        assert query.startswith("SELECT (SELECT count(*) FROM pg_trigger")
        return [["1", "1"]] if self.installed else [["0", "0"]]


class RejectionStorageFaultTest(unittest.TestCase):
    def test_scoped_trigger_restores_after_body_error(self):
        fixture = Fixture()
        journal = []
        raw_digest = "sha256:" + "a" * 64
        with self.assertRaisesRegex(RuntimeError, "body failed"):
            with receipt.rejection_storage_fault(fixture, raw_digest, journal) as fault:
                self.assertTrue(fixture.installed)
                self.assertEqual(fault["sqlstate"], "53100")
                raise RuntimeError("body failed")
        self.assertFalse(fixture.installed)
        install, remove = fixture.statements
        self.assertIn("NEW.raw_digest = '" + raw_digest + "' AND NEW.outcome = 'REJECTED'", install)
        self.assertIn("BEFORE INSERT ON gateway.gateway_reply_transport_receipts", install)
        self.assertIn("DROP TRIGGER IF EXISTS", remove)
        self.assertIn("DROP FUNCTION IF EXISTS", remove)
        self.assertNotIn("SECURITY DEFINER", install)
        self.assertNotIn("INSERT INTO", install)
        self.assertEqual(journal[-1]["event"], "rejection_storage_fault_removed")

    def test_uncertain_install_still_drops_owned_objects(self):
        fixture = Fixture()
        fixture.uncertain_install = True
        with self.assertRaisesRegex(RuntimeError, "uncertain install"):
            with receipt.rejection_storage_fault(fixture, "sha256:" + "b" * 64, []):
                self.fail("uncertain install entered fault body")
        self.assertFalse(fixture.installed)
        self.assertIn("DROP TRIGGER IF EXISTS", fixture.statements[-1])

    def test_non_fixture_and_malformed_digest_never_touch_database(self):
        fixture = Fixture()
        for target, digest in ((SimpleNamespace(pg="unrelated", prefix="worker-joint", containers=["unrelated"]), "sha256:" + "a" * 64),
                               (fixture, "sha256:'; DROP TABLE something; --")):
            with self.assertRaises(AssertionError):
                with receipt.rejection_storage_fault(target, digest, []):
                    self.fail("invalid fault binding entered")
        self.assertEqual(fixture.statements, [])

    def test_pg_errors_are_counted_without_copying_unrelated_logs(self):
        marker = "joint-receipt-storage-fault-" + "d" * 32
        result = SimpleNamespace(returncode=0, stdout="unrelated secret\n", stderr="ERROR: " + marker + "\nCONTEXT: ignore\nERROR: " + marker + "\n")
        with patch.object(receipt.subprocess, "run", return_value=result):
            value = receipt.storage_failure_hits(Fixture(), marker)
        self.assertEqual(value["matching_error_count"], 2)
        self.assertNotIn("secret", str(value))
        self.assertNotIn("CONTEXT", str(value))


class BlockedReceiptTest(unittest.TestCase):
    def test_zero_actual_storage_failures_is_not_a_pending_pass(self):
        with patch.object(receipt, "storage_failure_hits", return_value={"matching_error_count": 1}), \
                patch.object(receipt, "consumer_info") as consumer:
            self.assertIsNone(receipt.blocked_receipt(None, 5, b"raw", "run", [], "marker", 10))
        consumer.assert_not_called()

    def test_premature_ack_receipt_or_send_fails(self):
        for raw, records, count, sent in ((None, [], 1, []), (b"raw", [["REJECTED"]], 1, []),
                                          (b"raw", [], 2, []), (b"raw", [], 1, ["extra"])):
            with patch.object(receipt, "storage_failure_hits", return_value={"matching_error_count": 2}), \
                    patch.object(receipt, "consumer_info", return_value={"delivered": {"consumer_seq": 12}}), \
                    patch.object(receipt, "retained", return_value=raw), \
                    patch.object(receipt, "receipt", return_value=records), \
                    patch.object(receipt, "delivery_count", return_value=count), \
                    patch.object(receipt, "messages", return_value=sent):
                with self.assertRaises(AssertionError):
                    receipt.blocked_receipt(None, 5, b"raw", "run", [], "marker", 10)

    def test_actual_repeated_storage_errors_require_same_raw_and_no_receipt(self):
        with patch.object(receipt, "storage_failure_hits", return_value={"matching_error_count": 2}), \
                patch.object(receipt, "consumer_info", return_value={"delivered": {"consumer_seq": 12}}), \
                patch.object(receipt, "retained", return_value=b"raw"), \
                patch.object(receipt, "receipt", return_value=[]), \
                patch.object(receipt, "delivery_count", return_value=1), \
                patch.object(receipt, "messages", return_value=[]):
            result = receipt.blocked_receipt(None, 5, b"raw", "run", [], "marker", 10)
        self.assertTrue(result["message_retained"])
        self.assertEqual(result["terminal_receipts"], 0)


if __name__ == "__main__":
    unittest.main()
