"""Pure browser-coordination guards; no service startup or fake Artifact writes."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

spec = importlib.util.spec_from_file_location('artifact_web_runner', Path(__file__).resolve().parents[1] / 'test-worker-memory-web.py')
web = importlib.util.module_from_spec(spec)
spec.loader.exec_module(web)


class ArtifactBrowserGuards(unittest.TestCase):
    def marker(self, root, data=None):
        download = root / 'actual-download.txt'
        download.write_bytes(web.ARTIFACT_GUI_BYTES if data is None else data)
        return {'result': 'PASS', 'run_id': 'run-fixture', 'name': web.ARTIFACT_GUI_NAME, 'version': 0, 'size_bytes': len(web.ARTIFACT_GUI_BYTES), 'sha256': web.hashlib.sha256(web.ARTIFACT_GUI_BYTES).hexdigest(), 'download_file': str(download)}

    def test_actual_download_bytes_and_version_zero_are_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = self.marker(root)
            self.assertEqual(web.verify_artifact_browser_download(marker, root, 'run-fixture').read_bytes(), web.ARTIFACT_GUI_BYTES)
            (root / 'actual-download.txt').write_bytes(b'wrong real bytes')
            with self.assertRaisesRegex(AssertionError, 'download bytes differ'):
                web.verify_artifact_browser_download(marker, root, 'run-fixture')

    def test_wrong_run_version_name_or_hash_never_counts_as_gui_readback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = self.marker(root)
            for key, value in [('run_id', 'other'), ('name', 'other.txt'), ('version', 1), ('sha256', '0' * 64), ('size_bytes', 0)]:
                with self.subTest(key=key), self.assertRaises(AssertionError):
                    web.verify_artifact_browser_download(dict(original, **{key: value}), root, 'run-fixture')

    def test_browser_download_cannot_reference_an_outside_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory);owned = root / 'owned';owned.mkdir()
            marker = self.marker(root)
            with self.assertRaisesRegex(AssertionError, 'owned evidence file'):
                web.verify_artifact_browser_download(marker, owned, 'run-fixture')

    def test_failed_publication_marker_and_wait_timeout_are_not_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'marker.json'
            process = Mock();process.poll.return_value = None
            with self.assertRaisesRegex(RuntimeError, 'marker timeout'):
                web.wait_browser_marker(path, process, 0)
            path.write_text(json.dumps({'result': 'FAIL'}))
            with self.assertRaises(AssertionError):web.wait_browser_marker(path, process, 1)
            path.write_text(json.dumps({'result': 'PASS', 'run_id': 'r'}))
            self.assertEqual(web.wait_browser_marker(path, process, 1)['run_id'], 'r')


if __name__ == '__main__': unittest.main()
