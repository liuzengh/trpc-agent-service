"""Unit checks of external HTTP fixture sequencing, not product acceptance."""
import json
import unittest
import urllib.error
import urllib.request
from memory_joint_fixture import MemoryModelFixture, TOOLS


class FakeHarness:
    def __init__(self):
        self.sql_calls = []

    def secret(self):
        return 'fixture-only-key'

    def admin_sql(self, statement):
        self.sql_calls.append(statement)


class MemoryModelFixtureTests(unittest.TestCase):
    def setUp(self):
        self.h = FakeHarness()
        self.fixture = MemoryModelFixture(self.h)
        self.addCleanup(self.fixture.close)

    def call(self, mode, results, tools=TOOLS):
        messages = [{'role': 'user', 'content': mode}]
        messages += [{'role': 'tool', 'content': json.dumps(result)} for result in results]
        body = {'model': 'unit-fixture', 'messages': messages, 'tools': [{'type': 'function', 'function': {'name': name}} for name in tools]}
        request = urllib.request.Request(self.fixture.url + '/v1/chat/completions', data=json.dumps(body).encode(), headers={'Authorization': 'Bearer ' + self.fixture.key, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=3) as response:
            return [json.loads(line[5:]) for line in response.read().decode().splitlines() if line.startswith('data:') and line[5:].strip() != '[DONE]']

    def test_all_six_tools_and_result_derived_ids(self):
        results = []
        names = []
        responses = [{'message': 'added'}, {'results': [{'id': 'old-id'}]}, {'memory_id': 'updated-id'}, {'results': [{'id': 'updated-id'}]}, {}, {}, {}, {}, {'results': [{'id': 'kept-id', 'memory': 'persistent orchid memory'}]}]
        for reply in responses:
            events = self.call('memory-six', results)
            function = events[0]['choices'][0]['delta']['tool_calls'][0]['function']
            names.append(function['name'])
            args = json.loads(function['arguments'])
            if function['name'] == 'memory_update':
                self.assertEqual(args['memory_id'], 'old-id')
            if function['name'] == 'memory_delete':
                self.assertEqual(args['memory_id'], 'updated-id')
            results.append(reply)
        self.assertEqual(set(names), set(TOOLS))
        final = self.call('memory-six', results)
        self.assertEqual(final[0]['choices'][0]['delta']['content'], 'memory final: memory-six')
        self.assertEqual(final[-1]['usage']['total_tokens'], 10)

    def test_business_error_is_corrected_by_the_next_model_turn(self):
        first = self.call('memory-correct', [])
        self.assertEqual(first[0]['choices'][0]['delta']['tool_calls'][0]['function']['name'], 'memory_update')
        error = {'error': 'memory with id missing-fixture-entry not found'}
        correction = self.call('memory-correct', [error])
        self.assertEqual(correction[0]['choices'][0]['delta']['tool_calls'][0]['function']['name'], 'memory_load')
        final = self.call('memory-correct', [error, {'results': [{'memory': 'persistent orchid memory'}]}])
        self.assertEqual(final[0]['choices'][0]['delta']['content'], 'memory final: memory-correct')

    def test_missing_published_tools_rejected(self):
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.call('memory-read', [], ['memory_load'])
        self.assertEqual(caught.exception.code, 400)
        caught.exception.close()

    def test_failure_is_after_tool_result_not_before(self):
        first = self.call('memory-fail', [])
        self.assertEqual(first[0]['choices'][0]['delta']['tool_calls'][0]['function']['name'], 'memory_add')
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.call('memory-fail', [{'message': 'added'}])
        self.assertEqual(caught.exception.code, 401)
        caught.exception.close()
        self.assertEqual(self.h.sql_calls, [])

    def test_pg_fault_only_changes_receipt_acl_before_final(self):
        self.call('memory-pg-fail', [])
        self.assertEqual(self.h.sql_calls, [])
        final = self.call('memory-pg-fail', [{'message': 'added'}])
        self.assertEqual(self.h.sql_calls, ['REVOKE INSERT ON runtime_memory.memory_receipts FROM memory_runtime'])
        self.assertEqual(final[0]['choices'][0]['delta']['content'], 'memory final: memory-pg-fail')


if __name__ == '__main__':
    unittest.main()
