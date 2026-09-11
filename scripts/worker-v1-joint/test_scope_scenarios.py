"""Fast public external-Telegram and Session history assertion checks."""
import io
import socket
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError
import json
import tempfile
import unittest
from urllib.request import Request, urlopen
from gateway_fixture import TelegramFixture, Gateway, _publish_reply
from scope_scenarios import assert_transcript, change_binding


class ThreadFixtureTests(unittest.TestCase):
    def test_negative_supergroup_topic_is_echoed_and_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TelegramFixture(directory)
            try:
                body = {'chat_id': '-100123', 'message_thread_id': 41, 'text': 'topic answer',
                        'reply_parameters': json.dumps({'message_id': 7})}
                with urlopen(Request(fixture.url+'/bot'+fixture.token+'/sendMessage',
                                     data=json.dumps(body).encode(), headers={'Content-Type':'application/json'}),timeout=3) as response:
                    result=json.load(response)['result']
                self.assertEqual(result['chat']['id'], -100123)
                self.assertEqual(result['chat']['type'], 'supergroup')
                self.assertEqual(result['message_thread_id'], 41)
                self.assertEqual(fixture.snapshot()[0]['message_thread_id'], '41')
            finally:
                fixture.close()


class ScopeContractTests(unittest.TestCase):
    def test_exact_history_rejects_cross_scope_and_silent_reset(self):
        good={'messages':[{'role':'system','content':'instruction'}, {'role':'user','content':'first'}, {'role':'assistant','content':'joint answer: first'}, {'role':'user','content':'second'}]}
        assert_transcript(good,['first','second'])
        for messages in ([{'role':'user','content':'second'}],good['messages']+[{'role':'user','content':'other revision'}]):
            with self.assertRaises(AssertionError):assert_transcript({'messages':messages},['first','second'])


class GatewayResourceTests(unittest.TestCase):
    def test_http_error_response_is_closed_in_ready_and_webhook(self):
        gateway=object.__new__(Gateway)
        gateway.h=SimpleNamespace(urls={'gateway_health':'http://fixture.invalid','gateway_ingress':'https://fixture.invalid'})
        gateway.process=SimpleNamespace(poll=lambda:None)
        gateway.account_id='account';gateway.ssl=None
        for method in ('ready','webhook'):
            stream=io.BytesIO(b'error')
            error=HTTPError('https://fixture.invalid',503,'fixture outage',{},stream)
            try:
                with patch('gateway_fixture.urlopen',side_effect=error):
                    if method=='ready':self.assertFalse(gateway.ready())
                    else:self.assertEqual(gateway._webhook(b'{}','fixture-secret'),(503,b'error'))
                self.assertTrue(stream.closed, 'HTTPError response was leaked')
            finally:error.close()

    def test_bad_nats_greeting_closes_socket_before_tls(self):
        client,peer=socket.socketpair();peer.sendall(b'INVALID GREETING\r\n')
        try:
            with patch('gateway_fixture.socket.create_connection',return_value=client):
                with self.assertRaises(AssertionError):_publish_reply(SimpleNamespace(nats_url='tls://127.0.0.1:4222'),b'{}')
            self.assertEqual(client.fileno(),-1,'pre-TLS failure leaked connection')
        finally:client.close();peer.close()


if __name__ == '__main__':
    unittest.main()
