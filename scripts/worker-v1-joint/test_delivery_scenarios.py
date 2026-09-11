"""Exact external acceptance/response-loss seam; real Gateway is a separate gate."""
import http.client
import copy
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from urllib.request import Request, urlopen

from gateway_fixture import TelegramFixture
from delivery_scenarios import assert_unknown
import delivery_scenarios


class TelegramResponseLossTest(unittest.TestCase):
    def test_accept_before_response_loss_matches_exact_bot_chat_and_text_once(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TelegramFixture(directory)
            try:
                second = fixture.add_bot(987655)
                fixture.lose_response_once('lost-final', '42', bot_id=987655)
                def send(token, chat, text):
                    body = {'chat_id': chat, 'text': text, 'reply_parameters': json.dumps({'message_id': 7})}
                    request = Request(fixture.url + '/bot' + token + '/sendMessage',
                                      data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
                    with urlopen(request, timeout=3) as response:
                        return json.load(response)
                send(fixture.token, '42', 'lost-final')
                send(second['token'], '43', 'lost-final')
                send(second['token'], '42', 'another-final')
                with self.assertRaises(http.client.RemoteDisconnected):
                    send(second['token'], '42', 'lost-final')
                messages = fixture.snapshot()
                self.assertEqual(len(messages), 4)
                losses = fixture.response_loss_snapshot()
                self.assertEqual(len(losses), 1)
                self.assertEqual(losses[0]['message'], messages[-1])
                self.assertEqual(losses[0]['phase'], 'accepted_before_http_response_loss')
                self.assertFalse(losses[0]['http_response_written'])
                # Explicit external-fixture retry proves the fault is one-shot;
                # the real Gateway scenario must instead prove zero retries.
                send(second['token'], '42', 'lost-final')
                self.assertEqual(len(fixture.response_loss_snapshot()), 1)
                saved = (Path(directory) / 'telegram-external-fixture.json').read_text()
                for secret in (fixture.token, fixture.secret, second['token'], second['secret']):
                    self.assertNotIn(secret, saved)
            finally:
                fixture.close()

    def test_response_loss_is_bounded_to_one_valid_private_message(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TelegramFixture(directory)
            try:
                for text, chat, bot in [('', '42', 987654), ('final', '-42', 987654), ('final', '42', 123)]:
                    with self.assertRaises(ValueError):
                        fixture.lose_response_once(text, chat, bot_id=bot)
                fixture.lose_response_once('exact', '42')
                with self.assertRaises(ValueError):
                    fixture.lose_response_once('another', '42')
                self.assertEqual(fixture.response_loss_snapshot(), [])
            finally:
                fixture.close()


class UnknownOutcomeAssertionsTest(unittest.TestCase):
    def test_unknown_never_means_accepted_retryable_or_worker_reexecution(self):
        result = {'Certainty': 'UNKNOWN', 'ErrorClass': 'temporary', 'ProviderMessageID': ''}
        snapshot = {'worker_run': {'status': 'SUCCEEDED', 'attempts': 1}, 'worker_attempts': [{}],
                    'completions': [{}], 'candidates': [{}],
                    'parts': [{'state': 'UNKNOWN', 'attempt_number': 1, 'next_attempt_at': None,
                               'finished_at': '2026-09-07T01:00:00Z', 'current_attempt_id': 'one', 'result': result}],
                    'delivery_attempts': [{'attempt_id': 'one', 'result': result}]}
        assert_unknown(snapshot)
        mutations = [('parts', 'state', 'ACCEPTED'), ('parts', 'next_attempt_at', '2026-09-07T01:00:01Z'),
                     ('parts', 'attempt_number', 2), ('worker_run', 'attempts', 2)]
        for key, field, value in mutations:
            changed = copy.deepcopy(snapshot)
            target = changed[key] if key == 'worker_run' else changed[key][0]
            target[field] = value
            with self.subTest(key=key, field=field), self.assertRaises(AssertionError):
                assert_unknown(changed)


class DeliveryIntakeOrderingTest(unittest.TestCase):
    def test_gateway_admission_waits_for_zero_then_durable_worker_row(self):
        class StopAfterIntake(Exception):
            pass
        events = []
        with tempfile.TemporaryDirectory() as directory:
            h = SimpleNamespace(artifacts=Path(directory), gateway=SimpleNamespace(bot_id=987654),
                                workers={'worker-one': SimpleNamespace(pid=123, poll=lambda: None)},
                                gateway_fixture=SimpleNamespace(lose_response_once=lambda *args, **kwargs: None))
            def send(text, conversation_id):
                events.append('gateway-admission')
                return 'run-intake-order'
            def sql(query):
                self.assertIn('FROM worker.execution_runs', query)
                self.assertIn("run_id='run-intake-order'", query)
                exists = 'worker-row-absent' in events
                events.append('worker-row-present' if exists else 'worker-row-absent')
                return [[json.dumps([{'run_id': 'run-intake-order'}] if exists else [])]]
            def wait(predicate, description, timeout):
                self.assertIn('durable intake', description)
                self.assertFalse(predicate())
                self.assertTrue(predicate())
            def success(harness, run_id):
                self.assertEqual(events, ['gateway-admission', 'worker-row-absent', 'worker-row-present'])
                self.assertEqual(run_id, 'run-intake-order')
                raise StopAfterIntake()
            h.send_text, h.sql, h.wait = send, sql, wait
            with patch.object(delivery_scenarios, 'wait_success', success), self.assertRaises(StopAfterIntake):
                delivery_scenarios.run_delivery(h)
            self.assertEqual(json.loads((Path(directory) / 'worker-delivery-uncertainty.json').read_text())['run_id'], 'run-intake-order')


if __name__ == '__main__':
    unittest.main()
