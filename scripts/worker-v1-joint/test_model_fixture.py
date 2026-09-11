import json
import threading
import unittest
import urllib.error
from urllib.request import Request, urlopen
from model_fixture import ModelFixture


class ModelFixtureTests(unittest.TestCase):
    def setUp(self):
        self.fixture = ModelFixture()
        self.addCleanup(self.fixture.close)

    def request(self, text, key):
        request = {'model': 'fixture', 'messages': [{'role': 'user', 'content': text}], 'stream': True}
        return urlopen(Request(self.fixture.url + '/v1/chat/completions',
                               data=json.dumps(request).encode(),
                               headers={'Content-Type': 'application/json',
                                        'Authorization': 'Bearer ' + key}), timeout=5)

    def test_transient_failure_is_authenticated_one_shot(self):
        self.fixture.fail_once('retry')
        with self.assertRaises(urllib.error.HTTPError) as wrong_key:
            self.request('retry', 'wrong')
        self.assertEqual(wrong_key.exception.code, 403)
        wrong_key.exception.close()
        self.assertEqual(self.fixture.failure_snapshot(), [])
        with self.assertRaises(urllib.error.HTTPError) as failed:
            self.request('retry', self.fixture.key)
        self.assertEqual(failed.exception.code, 503)
        self.assertIn('error', json.load(failed.exception))
        failed.exception.close()
        with self.request('retry', self.fixture.key) as response:
            self.assertIn('[DONE]', response.read().decode())
        self.assertEqual(len(self.fixture.requests), 2)
        self.assertEqual(len(self.fixture.failure_snapshot()), 1)

    def test_failure_rejects_nontransient_and_conflicting_faults(self):
        for code in (400, True, 429):
            with self.assertRaises(ValueError):
                self.fixture.fail_once('retry', code)
        self.fixture.hold('held')
        with self.assertRaises(ValueError):
            self.fixture.fail_once('held')

    def test_partial_is_flushed_without_done_and_only_first_call_is_poisoned(self):
        self.fixture.hold_partial('streaming', 'unaccepted-partial')
        with self.request('streaming', self.fixture.key) as response:
            first = response.readline().decode()
            receipt = self.fixture.wait_partial('streaming', timeout=1)
            self.assertIn('unaccepted-partial', first)
            self.assertEqual(json.loads(first[6:])['model'],'fixture')
            self.assertNotIn('[DONE]', first)
            self.assertGreater(receipt['bytes'], 0)
            self.assertFalse(receipt['done'])
            self.assertEqual(receipt['phase'], 'partial_sse_flushed')
            self.fixture.release('streaming')
            self.assertIn('[DONE]', response.read().decode())
        with self.request('streaming', self.fixture.key) as response:
            body = response.read().decode()
            self.assertNotIn('unaccepted-partial', body)
            self.assertIn('joint answer: streaming', body)

    def test_response_chunks_echo_the_exact_requested_model_and_keep_parameters(self):
        for model_name in ('joint-fixture', 'gpt-4o'):
            request = {'model': model_name, 'max_completion_tokens': 20000,
                       'messages': [{'role': 'user', 'content': 'explicit ' + model_name}], 'stream': True}
            with urlopen(Request(self.fixture.url + '/v1/chat/completions',
                                 data=json.dumps(request).encode(), headers={'Content-Type': 'application/json',
                                 'Authorization': 'Bearer ' + self.fixture.key}), timeout=5) as response:
                chunks = [json.loads(line[6:]) for line in response.read().decode().splitlines()
                          if line.startswith('data: ') and line != 'data: [DONE]']
            self.assertEqual({chunk['model'] for chunk in chunks}, {model_name})
            self.assertEqual(self.fixture.requests[-1], request)

    def test_rotation_does_not_reauthenticate_an_already_held_call(self):
        old = self.fixture.key
        self.fixture.hold('old in flight')
        result = []
        def read():
            with self.request('old in flight', old) as response:
                result.append(response.read().decode())
        thread = threading.Thread(target=read)
        thread.start()
        try:
            self.fixture.wait_entered('old in flight', timeout=1)
            self.fixture.rotate_key('new-fixture-key')
            self.fixture.release('old in flight')
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertIn('joint answer: old in flight', result[0])
            with self.assertRaises(urllib.error.HTTPError) as error:
                self.request('new attempt', old)
            error.exception.close()
            self.assertEqual(error.exception.code, 403)
            with self.request('new attempt', self.fixture.key) as response:
                response.read()
            self.assertEqual(self.fixture.authentication_events('old in flight'),
                             [{'accepted': True, 'key_generation': 1}])
            events = self.fixture.authentication_events('new attempt')
            self.assertEqual(events, [{'accepted': False, 'key_generation': 2},
                                      {'accepted': True, 'key_generation': 2}])
            evidence = json.dumps(events)
            self.assertNotIn(old, evidence)
            self.assertNotIn(self.fixture.key, evidence)
            self.assertEqual(len(self.fixture.requests), 2)
        finally:
            self.fixture.release('old in flight')
            thread.join(3)

if __name__ == '__main__':
    unittest.main()
