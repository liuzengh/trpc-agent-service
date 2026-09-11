import unittest
from unittest.mock import Mock, patch
from tracing_session import final_trace, no_accepted_commit, selected


def span(name, outcome):
    return {'name': name, 'spanId': name, 'attributes': [{'key': 'app.outcome', 'value': {'stringValue': outcome}}]}


class SessionTraceTests(unittest.TestCase):
    def test_candidate_and_failed_are_not_accepted(self):
        no_accepted_commit([span('worker.session.stage', 'candidate'), span('worker.session.commit', 'failed')])

    def test_early_accepted_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, 'reported accepted'):
            no_accepted_commit([span('worker.session.commit', 'accepted')])

    def test_selection_requires_both_operation_and_outcome(self):
        values = [span('worker.session.stage', 'candidate'), span('worker.session.commit', 'failed')]
        self.assertEqual(selected(values, 'worker.session.stage', 'accepted'), [])
        self.assertEqual(len(selected(values, 'worker.session.commit', 'failed')), 1)

    def test_final_trace_waits_for_failed_and_accepted_evidence(self):
        partial = [span('worker.session.stage', 'candidate'), span('worker.session.commit', 'accepted')]
        complete = partial + [span('worker.session.commit', 'failed')]
        verify = Mock(return_value={'trace_id': 'actual'})
        with patch('tracing_session.read_spans', side_effect=[partial, complete]), patch('tracing_session.time.sleep'):
            _, spans = final_trace(None, 'backend', verify, 'run', 'worker.session.commit', 'failed')
        self.assertEqual(verify.call_count, 2)
        self.assertEqual(spans, complete)


if __name__ == '__main__':
    unittest.main()
