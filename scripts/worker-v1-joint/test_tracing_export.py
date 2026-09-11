import unittest
import urllib.request
import urllib.error
from types import SimpleNamespace
from unittest.mock import patch
from tracing_export import FullPipe, ExportFault, blocked_stdout


class ExportFixtureTests(unittest.TestCase):
    def test_pipe_remains_full_without_a_reader(self):
        pipe = FullPipe()
        try:
            first = pipe.snapshot()
            self.assertGreater(first['prefilled_bytes'], 0)
            self.assertEqual(first['unread_bytes'], first['prefilled_bytes'])
            self.assertEqual(pipe.snapshot(), first)
            self.assertFalse(first['reader_drained'])
        finally:
            pipe.close()

    def test_503_records_digest_not_payload(self):
        fault = ExportFault(SimpleNamespace(secrets=['private-secret']), '503')
        try:
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(urllib.request.Request(fault.endpoint, data=b'opaque protobuf'), timeout=2)
            self.assertEqual(error.exception.code, 503)
            error.exception.close()
            records = fault.snapshot()
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]['bytes'], 15)
            self.assertFalse(records[0]['canary_present'])
            self.assertNotIn('raw', records[0])
            self.assertEqual(len(records[0]['sha256']), 64)
        finally:
            fault.close()

    def test_canary_is_detected_without_retaining_body(self):
        fault = ExportFault(SimpleNamespace(secrets=['private-secret']), '503')
        try:
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(urllib.request.Request(fault.endpoint, data=b'private-secret'), timeout=2)
            error.exception.close()
            self.assertTrue(fault.snapshot()[0]['canary_present'])
            self.assertNotIn('private-secret', str(fault.snapshot()))
        finally:
            fault.close()

    def test_failed_assertion_stops_children_before_closing_pipe(self):
        events = []
        process = SimpleNamespace(poll=lambda: None)
        h = SimpleNamespace(worker=process, gateway=SimpleNamespace(process=process, stop=lambda: events.append('gateway')), stop_worker=lambda: events.append('worker'))
        with patch('tracing_export.FullPipe.close', lambda self: events.append('close')):
            with self.assertRaisesRegex(AssertionError, 'injected'):
                with blocked_stdout(h) as sinks:
                    sinks['test'] = SimpleNamespace(close=lambda: events.append('close'))
                    raise AssertionError('injected')
        self.assertEqual(events, ['worker', 'gateway', 'close'])


if __name__ == '__main__':
    unittest.main()
