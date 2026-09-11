import unittest
from unittest.mock import Mock, patch
from tracing_retry import completed_trace, unique_spans


class RetryTraceTests(unittest.TestCase):
    def test_distinct_consumer_spans(self):
        spans = [{'name': 'process', 'spanId': 'first'}, {'name': 'process', 'spanId': 'second'}]
        self.assertEqual(len(unique_spans(spans, 'process', 2)), 2)

    def test_missing_or_reused_identity_fails(self):
        span = {'name': 'process', 'spanId': 'same'}
        for spans in ([span], [span, span]):
            with self.assertRaises(AssertionError):
                unique_spans(spans, 'process', 2)

    def test_waits_for_redelivered_consumer_batch(self):
        first = [{'name': 'process', 'spanId': 'first'}]
        both = first + [{'name': 'process', 'spanId': 'second'}]
        verify = Mock(return_value={'trace_id': 'actual'})
        with patch('tracing_retry.read_spans', side_effect=[first, both]), patch('tracing_retry.time.sleep'):
            _, spans = completed_trace(None, 'backend', verify, 'run', lambda ss: unique_spans(ss, 'process', 2))
        self.assertEqual(verify.call_count, 2)
        self.assertEqual(spans, both)


if __name__ == '__main__':
    unittest.main()
