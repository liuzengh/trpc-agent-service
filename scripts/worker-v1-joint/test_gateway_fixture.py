"""Unit verification of the explicit external platform substitute, not product E2E."""
import json
import http.client
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from gateway_fixture import TelegramFixture, _literal


class TelegramFixtureTest(unittest.TestCase):
    def test_sdk_protocol_original_reply_and_redacted_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TelegramFixture(directory)
            try:
                def request(method, payload):
                    with urlopen(Request(fixture.url + "/bot" + fixture.token + "/" + method, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}), timeout=3) as response:
                        return json.load(response)
                self.assertEqual(request("getMe", {})["result"]["id"], 987654)
                self.assertEqual(request("getWebhookInfo", {})["result"], {"url": "", "pending_update_count": 0})
                self.assertTrue(request("setWebhook", {"url": "https://127.0.0.1:8080/v1/telegram/account", "secret_token": fixture.secret, "drop_pending_updates": False})["result"])
                self.assertEqual(request("getWebhookInfo", {})["result"], {"url": "https://127.0.0.1:8080/v1/telegram/account", "pending_update_count": 0})
                self.assertTrue(request("sendMessage", {"chat_id": "123", "text": "final fixture", "reply_parameters": '{"message_id":7}'})["ok"])
                self.assertEqual(fixture.snapshot()[0]["source_message_id"], "7")
                with self.assertRaises(HTTPError) as missing_reply:
                    request("sendMessage", {"chat_id": "123", "text": "missing context"})
                self.assertEqual(missing_reply.exception.code, 400)
                missing_reply.exception.close()
                with self.assertRaises(HTTPError) as wrong_secret:
                    request("setWebhook", {"url": "https://127.0.0.1:8080/v1/telegram/account", "secret_token": "wrong"})
                self.assertEqual(wrong_secret.exception.code, 400)
                wrong_secret.exception.close()
                raw = (Path(directory) / "telegram-external-fixture.json").read_text()
                self.assertNotIn(fixture.token, raw)
                self.assertNotIn(fixture.secret, raw)
                self.assertEqual(len(json.loads(raw)["messages"]), 1)
            finally:
                fixture.close()


    def test_sdk_chunked_multipart_registration(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TelegramFixture(directory)
            try:
                boundary = "joint-sdk-boundary"
                raw = ""
                for key, value in {"url": "https://127.0.0.1:8080/v1/telegram/account", "secret_token": fixture.secret}.items():
                    raw += "--" + boundary + '\r\nContent-Disposition: form-data; name="' + key + '"\r\n\r\n' + value + "\r\n"
                raw += "--" + boundary + "--\r\n"
                connection = http.client.HTTPConnection("127.0.0.1", fixture.server.server_port, timeout=3)
                try:
                    connection.request("POST", "/bot" + fixture.token + "/setWebhook", body=iter([raw[:20].encode(), raw[20:].encode()]), headers={"Content-Type": "multipart/form-data; boundary=" + boundary}, encode_chunked=True)
                    response = connection.getresponse()
                    self.assertEqual(response.status, 200)
                    self.assertTrue(json.loads(response.read())["result"])
                    self.assertEqual(len(fixture.registrations), 1)
                finally:
                    connection.close()
            finally:
                fixture.close()

    def test_typed_rejection_is_one_shot_and_never_accepts(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TelegramFixture(directory)
            try:
                payload = {"chat_id": "123", "text": "rate fixture", "reply_parameters": '{"message_id":7}'}
                fixture.reject_once('sendMessage', text=payload['text'], conversation_id='123')
                request = Request(fixture.url + '/bot' + fixture.token + '/sendMessage', data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
                with self.assertRaises(HTTPError) as rejected:
                    urlopen(request, timeout=3)
                self.assertEqual(rejected.exception.code, 429)
                self.assertEqual(json.load(rejected.exception)['error_code'], 429)
                rejected.exception.close()
                self.assertEqual(fixture.snapshot(), [])
                self.assertFalse(fixture.rejection_snapshot()[0]['accepted'])
                with urlopen(request, timeout=3) as response:
                    self.assertTrue(json.load(response)['ok'])
                self.assertEqual(len(fixture.snapshot()), 1)
                self.assertEqual(len(fixture.rejection_snapshot()), 1)
            finally:
                fixture.close()

    def test_preparation_rejection_does_not_record_a_message(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = TelegramFixture(directory)
            try:
                fixture.reject_once('getMe')
                with self.assertRaises(ValueError):
                    fixture.reject_once('getMe')
                request = Request(fixture.url + '/bot' + fixture.token + '/getMe', data=b'{}', headers={'Content-Type': 'application/json'})
                with self.assertRaises(HTTPError) as rejected:
                    urlopen(request, timeout=3)
                self.assertEqual(rejected.exception.code, 429)
                rejected.exception.close()
                self.assertEqual(fixture.snapshot(), [])
                with urlopen(request, timeout=3) as response:
                    self.assertTrue(json.load(response)['ok'])
            finally:
                fixture.close()

    def test_sql_literal_is_quoted(self):
        self.assertEqual(_literal("a'b"), "'a''b'")


if __name__ == "__main__":
    unittest.main()
