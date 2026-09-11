import unittest
from tracing_lifecycle import ended_attempt


def span(outcome, attempt='attempt'):
    return {'name': 'worker.run.attempt', 'startTimeUnixNano': '10', 'endTimeUnixNano': '20', 'attributes': [
        {'key': 'app.attempt.id', 'value': {'stringValue': attempt}},
        {'key': 'app.outcome', 'value': {'stringValue': outcome}}]}


class LifecycleTraceTests(unittest.TestCase):
    def test_closed_cancellation_deadline_and_fence(self):
        for outcome in ('cancelled', 'deadline', 'fenced'):
            self.assertEqual(ended_attempt([span(outcome)], 'attempt'), span(outcome))

    def test_success_is_not_stale_attempt_evidence(self):
        with self.assertRaises(AssertionError):
            ended_attempt([span('ok')], 'attempt')

    def test_wrong_attempt_or_duplicate_end_is_rejected(self):
        for spans in ([span('fenced','other')], [span('fenced'),span('fenced')]):
            with self.assertRaises(AssertionError):
                ended_attempt(spans, 'attempt')

    def test_impossible_end_time_is_rejected(self):
        item = span('cancelled');item['endTimeUnixNano']='9'
        with self.assertRaises(AssertionError):
            ended_attempt([item], 'attempt')


if __name__ == '__main__':
    unittest.main()
