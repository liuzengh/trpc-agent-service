"""Validation of rollback gate predicates; real binaries are tested separately."""
import io
import tarfile
import tempfile
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from tracing_rollback import build_old, check_config, columns


class TracingRollbackTests(unittest.TestCase):
    def harness(self):
        return SimpleNamespace(write=lambda *args: '/private/fixture.json', root='/private/fixture', env={}, dsns={'worker_runtime': 'fixture', 'worker_migrator': 'fixture-migrator'}, redact=lambda s: s)

    def test_build_accepts_resolved_private_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            real = Path(directory) / 'real'
            real.mkdir()
            alias = Path(directory) / 'alias'
            alias.symlink_to(real, target_is_directory=True)
            archive = io.BytesIO()
            with tarfile.open(fileobj=archive, mode='w') as tar:
                member = tarfile.TarInfo('go.mod')
                member.size = 6
                tar.addfile(member, io.BytesIO(b'module'))
            h = SimpleNamespace(work=alias, race=False, env={}, redact=lambda s: s)
            def build(cmd, **kwargs):
                Path(cmd[cmd.index('-o') + 1]).write_bytes(b'fixture-binary')
                return SimpleNamespace(returncode=0, stderr='')
            with patch('tracing_rollback.subprocess.check_output', side_effect=['a' * 40, archive.getvalue()]), patch('tracing_rollback.subprocess.run', side_effect=build):
                binaries, result = build_old(h, directory, 'explicit-ref')
            self.assertEqual(len(binaries), 2)
            self.assertEqual(result['commit'], 'a' * 40)
            self.assertEqual((real / 'rollback-source/go.mod').read_bytes(), b'module')

    def test_archive_traversal_is_rejected_before_extraction(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = io.BytesIO()
            with tarfile.open(fileobj=archive, mode='w') as tar:
                tar.addfile(tarfile.TarInfo('../escaped'))
            h = SimpleNamespace(work=Path(directory))
            with patch('tracing_rollback.subprocess.check_output', side_effect=['a' * 40, archive.getvalue()]), self.assertRaises(AssertionError):
                build_old(h, directory, 'explicit-ref')
            self.assertFalse((Path(directory) / 'escaped').exists())

    def test_expected_config_rejection(self):
        with patch('tracing_rollback.subprocess.run', return_value=SimpleNamespace(returncode=1, stdout='', stderr='worker configuration fields are invalid\n')):
            result = check_config(self.harness(), '/old/worker', {'tracing': {}}, False)
        self.assertEqual(result['exit'], 1)

    def test_different_failure_does_not_prove_tracing_rejection(self):
        with patch('tracing_rollback.subprocess.run', return_value=SimpleNamespace(returncode=1, stdout='', stderr='missing credentials')):
            with self.assertRaises(AssertionError):
                check_config(self.harness(), '/old/worker', {'tracing': {}}, False)

    def test_accepted_config_must_report_pass(self):
        with patch('tracing_rollback.subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='', stderr='')):
            with self.assertRaises(AssertionError):
                check_config(self.harness(), '/old/worker', {}, True)

    def test_config_environment_contains_private_database_roles(self):
        with patch('tracing_rollback.subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='WORKER_CONFIG=PASS\n', stderr='')) as run:
            check_config(self.harness(), '/old/worker', {}, True)
            self.assertEqual(run.call_args.kwargs['env']['WORKER_DATABASE_URL'], 'fixture')
            self.assertEqual(run.call_args.kwargs['env']['WORKER_MIGRATION_DATABASE_URL'], 'fixture-migrator')

    def test_missing_or_nonnullable_columns_fail(self):
        for rows in ([], [['worker','runs','traceparent','NO']] * 8):
            with self.assertRaises(AssertionError):
                columns(SimpleNamespace(sql=lambda q: rows))


if __name__ == '__main__':
    unittest.main()
