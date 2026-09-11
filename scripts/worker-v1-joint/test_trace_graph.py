"""Negative coverage for the actual trace acceptance graph validator."""
import copy
import importlib.util
from pathlib import Path
import sys
import unittest

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('trace_graph_gate', Path(__file__).resolve().parents[1] / 'test-im-tracing-v1.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
TRACE = '1' * 32


def fixture():
    edges = dict(gate.CAUSAL_ANCESTORS)
    edges['gateway.im.callback'] = None
    for subject in ('execution.run-requested.v1', 'execution.reply-intent.v1'):
        edges['publish ' + subject] = 'create ' + subject
        edges['process ' + subject] = 'create ' + subject
    edges['invoke_agent assistant'] = 'worker.runner.run'
    edges['chat fixture'] = 'invoke_agent assistant'
    ids = {name: format(i + 1, '016x') for i, name in enumerate(edges)}
    return [{'name': name, 'spanId': ids[name], 'traceId': TRACE,
             'parentSpanId': ids[parent] if parent else '0' * 16,
             'links': [{'spanId': ids[parent], 'traceId': TRACE}] if name.startswith(('publish ', 'process ')) else []}
            for name, parent in edges.items()]


class TraceGraphTest(unittest.TestCase):
    def setUp(self):
        self.spans = fixture()
        self.by_name = {s['name']: s for s in self.spans}

    def check(self, crash=False):
        return gate.verify_parentage(self.spans, TRACE, allow_missing_parents=crash)

    def test_valid_graph(self):
        self.assertEqual(self.check(), [])

    def test_all_disconnected_roots_rejected_even_in_crash_mode(self):
        for s in self.spans:
            s['parentSpanId'] = '0' * 16
        for crash in (False, True):
            with self.assertRaises(AssertionError): self.check(crash)

    def test_wrong_existing_parent(self):
        self.by_name['gateway.im.send']['parentSpanId'] = self.by_name['worker.runner.run']['spanId']
        for crash in (False, True):
            with self.assertRaises(AssertionError): self.check(crash)

    def test_cycle(self):
        a = self.by_name['worker.run.attempt']
        a['parentSpanId'] = self.by_name['worker.runner.run']['spanId']
        with self.assertRaisesRegex(AssertionError, 'cyclic'): self.check()

    def test_missing_creation_link(self):
        self.by_name['process execution.run-requested.v1']['links'] = []
        with self.assertRaisesRegex(AssertionError, 'creation Link'): self.check()

    def test_foreign_creation_link(self):
        self.by_name['publish execution.reply-intent.v1']['links'][0]['traceId'] = '2' * 32
        with self.assertRaisesRegex(AssertionError, 'creation Link'): self.check()

    def test_crash_gap_is_explicit_and_not_pass(self):
        removed = self.by_name['worker.run.attempt']
        self.spans.remove(removed)
        with self.assertRaisesRegex(AssertionError, 'missing actual causal parent'): self.check()
        missing = self.check(True)
        self.assertTrue(missing)
        self.assertTrue(all(x['parent_span_id'] == removed['spanId'] for x in missing))

    def test_duplicate_id(self):
        self.spans.append(copy.deepcopy(self.spans[0]))
        with self.assertRaisesRegex(AssertionError, 'duplicate'): self.check()

    def test_foreign_trace(self):
        self.spans[0]['traceId'] = '2' * 32
        with self.assertRaisesRegex(AssertionError, 'foreign'): self.check()


if __name__ == '__main__':
    unittest.main()
