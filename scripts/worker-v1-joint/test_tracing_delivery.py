import unittest
from unittest.mock import Mock, patch
from tracing_delivery import check_sends, verify_delivery_trace


def attr(key, value):
    return {'key': key, 'value': {'stringValue': value}}


class DeliveryTraceTests(unittest.TestCase):
    def spans(self):
        return [{'name': 'gateway.reply.deliver', 'spanId': 'parent', 'attributes': [attr('app.part.index', '0')]},
                {'name': 'gateway.im.send', 'spanId': 'send', 'parentSpanId': 'parent', 'attributes': [attr('app.outcome', 'UNKNOWN'), attr('app.attempt.id', 'real-attempt'), attr('app.retry.number', '0')]}]

    def test_partial_backend_response_waits_for_all_parts(self):
        first = self.spans()
        complete = first + [dict(first[-1], spanId='send2')]
        verify = Mock(return_value={'trace_id': 'actual'})
        with patch('tracing_delivery.read_spans', side_effect=[first, complete]), patch('tracing_delivery.time.sleep'):
            trace, sends, parents = verify_delivery_trace(None, 'backend', verify, 'run', ['UNKNOWN', 'UNKNOWN'])
        self.assertEqual(verify.call_count, 2)
        self.assertEqual(len(sends), 2)

    def test_actual_unknown_send_parent(self):
        sends, parents = check_sends(self.spans(), ['UNKNOWN'])
        self.assertEqual(len(sends), 1)
        self.assertIn('parent', parents)

    def test_unknown_is_not_accepted(self):
        with self.assertRaises(AssertionError):
            check_sends(self.spans(), ['ACCEPTED'])

    def test_actual_parent_required(self):
        spans = self.spans()
        spans[1]['parentSpanId'] = 'unrelated'
        with self.assertRaises(AssertionError):
            check_sends(spans, ['UNKNOWN'])

    def test_duplicate_send_span_identity_rejected(self):
        spans = self.spans()
        spans.append(spans[-1].copy())
        with self.assertRaises(AssertionError):
            check_sends(spans, ['UNKNOWN', 'UNKNOWN'])


if __name__ == '__main__':
    unittest.main()
