import json
import tempfile
import unittest
from pathlib import Path

import ci_evidence


class EvidenceContractTests(unittest.TestCase):
    def test_duplicate_filenames_are_rejected_before_flattening(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "ci-evidence-go" / "shared.log"
            second = root / "ci-evidence-vulnerability" / "shared.log"
            first.parent.mkdir()
            second.parent.mkdir()
            first.write_text("go", encoding="utf-8")
            second.write_text("vulnerability", encoding="utf-8")
            with self.assertRaises(ValueError):
                ci_evidence.require_unique_filenames([first, second])

    def test_valid_test_log_requires_complete_package_results(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.jsonl"
            path.write_text(
                json.dumps({"Action": "start", "Package": "example/pkg"})
                + "\n"
                + json.dumps({"Action": "pass", "Package": "example/pkg", "Test": "TestOne"})
                + "\n"
                + json.dumps({"Action": "pass", "Package": "example/pkg"})
                + "\n",
                encoding="utf-8",
            )
            result = ci_evidence.validate_test_log(path, allow_skips=False)
            self.assertEqual(result["packages"], 1)
            self.assertEqual(result["passed_test_results"], 1)

    def test_incomplete_test_log_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.jsonl"
            path.write_text(json.dumps({"Action": "start", "Package": "example/pkg"}) + "\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                ci_evidence.validate_test_log(path, allow_skips=False)

    def test_failed_test_log_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test.jsonl"
            path.write_text(
                json.dumps({"Action": "fail", "Package": "example/pkg", "Test": "TestOne"}) + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                ci_evidence.validate_test_log(path, allow_skips=False)


if __name__ == "__main__":
    unittest.main()
