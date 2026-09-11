"""External fixture identity checks; real SessionScope evidence is a separate gate."""
import json
import copy
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from gateway_fixture import TelegramFixture
from identity_scenarios import assert_identity_isolation, await_identity
from types import SimpleNamespace
from unittest.mock import patch
from gateway_fixture import _literal


class MultiBotFixtureTest(unittest.TestCase):
    def test_three_authenticated_bots_cannot_exchange_managed_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture=TelegramFixture(directory)
            try:
                default=(fixture.bot_id,fixture.token,fixture.secret)
                bots=[{'bot_id':fixture.bot_id,'token':fixture.token,'secret':fixture.secret},
                      fixture.add_bot(987655),fixture.add_bot(987656)]
                self.assertEqual((fixture.bot_id,fixture.token,fixture.secret),default)
                def call(bot,method,body,status=200):
                    request=Request(fixture.url+'/bot'+bot['token']+'/'+method,data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
                    try:response=urlopen(request,timeout=3)
                    except HTTPError as error:response=error
                    with response:
                        self.assertEqual(response.status,status)
                        return json.load(response) if status==200 else response.read()
                for index,bot in enumerate(bots):
                    self.assertEqual(call(bot,'getMe',{})['result']['id'],bot['bot_id'])
                    self.assertEqual(call(bot,'getWebhookInfo',{})['result']['url'],'')
                    call(bot,'setWebhook',{'url':'https://127.0.0.1:8080/v1/telegram/account-'+str(index),'secret_token':bots[(index+1)%3]['secret']},400)
                    call(bot,'setWebhook',{'url':'https://127.0.0.1:8080/v1/telegram/account-'+str(index),'secret_token':bot['secret']})
                    self.assertEqual(call(bot,'getWebhookInfo',{})['result']['url'],'https://127.0.0.1:8080/v1/telegram/account-'+str(index))
                    call(bot,'sendMessage',{'chat_id':'42','reply_parameters':'{"message_id":7}','text':'bot '+str(index)})
                for index,bot in enumerate(bots):
                    self.assertEqual(call(bot,'getWebhookInfo',{})['result']['url'],'https://127.0.0.1:8080/v1/telegram/account-'+str(index))
                messages=fixture.snapshot()
                self.assertEqual([m['bot_id'] for m in messages],[987654,987655,987656])
                self.assertEqual(len({(m['bot_id'],m['chat_id'],m['source_message_id']) for m in messages}),3)
                saved=(Path(directory)/'telegram-external-fixture.json').read_text()
                for bot in bots:
                    self.assertNotIn(bot['token'],saved)
                    self.assertNotIn(bot['secret'],saved)
                fixture.remove_bot(987655)
                call(bots[1],'getMe',{},401)
                self.assertEqual(call(bots[0],'getMe',{})['result']['id'],987654)
            finally:fixture.close()

    def test_registry_is_bounded_and_default_identity_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture=TelegramFixture(directory)
            try:
                for invalid in (0,-1,True,'987655'):
                    with self.assertRaises(ValueError):fixture.add_bot(invalid)
                with self.assertRaises(ValueError):fixture.add_bot(987654)
                with self.assertRaises(ValueError):fixture.remove_bot(987654)
                fixture.add_bot(987655);fixture.add_bot(987656)
                with self.assertRaises(ValueError):fixture.add_bot(987657)
                self.assertEqual(fixture.bot_ids(),[987654,987655,987656])
            finally:fixture.close()


class ReceiverReadinessTest(unittest.TestCase):
    def test_identity_waits_for_current_receiver_not_legacy_registration(self):
        queries = []
        identity = {"account_id": "account", "tenant_id": "tenant", "bot_id": 987654}
        view = {"binding": {"binding_id": "binding"}}
        def sql(query):
            queries.append(query)
            if "gateway_telegram_receivers" in query:
                return [['[ {"owner_epoch": 2} ]']] if len([q for q in queries if "gateway_telegram_receivers" in q]) > 1 else [["[]"]]
            return [["987654", "tenant", "t", "t"]]
        waits = []
        def wait(predicate, label, timeout):
            waits.append(label)
            if "receiver" in label:
                self.assertFalse(predicate(), "missing current receiver passed readiness")
            self.assertTrue(predicate())
        h = SimpleNamespace(api=lambda *args: view, sql=sql, wait=wait, quote=_literal,
                            urls={"gateway_ingress": "https://127.0.0.1:8080/"})
        with patch("identity_scenarios.binding_path", return_value="/binding"), \
             patch("identity_scenarios.await_route") as route:
            self.assertIs(await_identity(h, identity), view)
            route.assert_called_once_with(h, view)
        query = queries[-1]
        for guard in ("d.source_epoch=r.source_epoch", "d.connection_revision=r.connection_revision",
                      "r.managed_revision=r.connection_revision", "r.managed_url='https://127.0.0.1:8080/v1/telegram/account'",
                      "r.owner_epoch>0", "r.lease_until>clock_timestamp()", "r.last_reason='NONE'",
                      "d.enabled AND d.present", "q.instance_epoch=r.instance_epoch",
                      "q.enabled AND q.valid_until>clock_timestamp()"):
            self.assertIn(guard, query)
        self.assertNotIn("gateway_telegram_registration", query)
        self.assertEqual(len(waits), 2)


class IsolationAssertionsTest(unittest.TestCase):
    def test_rejects_cross_identity_session_and_wrong_parent(self):
        rounds=[]
        for index in range(6):
            group=index%3
            rounds.append({'session_id':'session-'+str(group),'session_sequence':index//3+1,
                           'identity':{'account_id':'account-'+str(group),'tenant_id':'a' if group<2 else 'b','manifest_digest':'m-a' if group<2 else 'm-b'},
                           'candidate_parent_ref':'' if index<3 else 'ref-'+str(group),
                           'candidate_parent_digest':'' if index<3 else 'digest-'+str(group),
                           'completion':{'candidate_ref':'ref-'+str(index),'candidate_digest':'digest-'+str(index)},
                           'run_id':'run-'+str(index),'delivery':{'intent_id':'intent-'+str(index)}})
        assert_identity_isolation(rounds)
        for index,key,value in [(1,'session_id','session-0'),(3,'candidate_parent_ref','ref-1'),
                                (4,'candidate_parent_digest','digest-0'),(5,'session_sequence',1),
                                (2,'run_id','run-0')]:
            changed=copy.deepcopy(rounds);changed[index][key]=value
            with self.subTest(index=index,key=key),self.assertRaises(AssertionError):
                assert_identity_isolation(changed)


if __name__=='__main__':unittest.main()
