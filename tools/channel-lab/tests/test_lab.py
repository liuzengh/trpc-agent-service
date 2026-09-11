import concurrent.futures
import json
import pathlib
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from app import Lab,Error,server

class LabTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=str(pathlib.Path(self.tmp.name)/'lab.db')
        self.lab=Lab(self.path,'http://127.0.0.1:9999')
        self.b=self.lab.create('test')
    def tearDown(self):
        self.lab.close();self.lab.db.close();self.tmp.cleanup()
    def call(self,m,**data):return self.lab.api(self.b['token'],m,data)
    def chat(self):return self.lab.chat(self.b['id'],100,100,'hello')
    def test_poll_confirm_reply_and_replay(self):
        u=self.chat();self.assertEqual(self.call('getUpdates'),[u]);self.assertEqual(self.call('getUpdates'),[u])
        r=self.call('sendMessage',chat_id='100',text='world',reply_parameters=json.dumps({'message_id':u['message']['message_id']}))
        self.assertEqual(r['chat']['id'],100)
        self.assertEqual(self.call('getUpdates',offset=u['update_id']+1),[])
        self.lab.replay(self.b['id'],u['update_id']);self.assertEqual(self.call('getUpdates'),[u])
    def test_long_poll_wakes(self):
        with concurrent.futures.ThreadPoolExecutor() as pool:
            f=pool.submit(self.call,'getUpdates',timeout=2)
            time.sleep(.1);u=self.chat();self.assertEqual(f.result(timeout=2),[u])
    def test_webhook_mutual_exclusion_and_target(self):
        with self.assertRaises(Error):self.call('setWebhook',url='http://evil.example/v1/telegram/a')
        self.call('setWebhook',url=self.lab.origin+'/v1/telegram/cha_a',secret_token='abc')
        with self.assertRaises(Error) as c:self.call('getUpdates')
        self.assertEqual(c.exception.code,409)
        self.call('deleteWebhook');self.assertEqual(self.call('getWebhookInfo')['url'],'')
    def test_bot_isolation_and_restart(self):
        u=self.chat();other=self.lab.create('other')
        self.assertEqual(self.lab.api(other['token'],'getUpdates',{}),[])
        with self.assertRaises(Error):self.lab.api(other['token'],'sendMessage',{'chat_id':100,'text':'wrong'})
        self.lab.db.close();self.lab=Lab(self.path,'http://127.0.0.1:9999')
        self.assertEqual(self.call('getUpdates'),[u]);self.assertEqual(self.call('getMe')['id'],self.b['id'])
    def test_http_and_csrf(self):
        s=server(self.lab,('127.0.0.1',0));t=threading.Thread(target=s.serve_forever);t.start()
        try:
            url='http://127.0.0.1:'+str(s.server_port)
            req=urllib.request.Request(url+'/bot'+self.b['token']+'/getMe',data=b'{}',headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(req) as r:self.assertTrue(json.load(r)['ok'])
            req=urllib.request.Request(url+'/lab/bots',data=b'{"name":"x"}',headers={'Content-Type':'application/json'})
            with self.assertRaises(urllib.error.HTTPError) as c:urllib.request.urlopen(req)
            self.assertEqual(c.exception.code,403);c.exception.close()
        finally:s.shutdown();s.server_close();t.join()
    def test_model_http_and_stream(self):
        s=server(self.lab,('127.0.0.1',0));t=threading.Thread(target=s.serve_forever);t.start()
        try:
            url='http://127.0.0.1:'+str(s.server_port)+'/v1/chat/completions'
            for stream in [False,True]:
                body=json.dumps({'model':'lab-echo','messages':[{'role':'user','content':'first'},{'role':'assistant','content':'old'},{'role':'user','content':'next'}],'stream':stream}).encode()
                req=urllib.request.Request(url,data=body,headers={'Content-Type':'application/json','Authorization':'Bearer '+self.lab.snapshot()['model_key']})
                with urllib.request.urlopen(req) as r:
                    raw=r.read().decode();self.assertIn('lab echo: next',raw)
                    if stream:self.assertIn('data: [DONE]',raw)
            self.assertEqual(len(self.lab.snapshot()['model_requests']),2)
        finally:s.shutdown();s.server_close();t.join()
    def test_sdk_chunked_form(self):
        import http.client
        s=server(self.lab,('127.0.0.1',0));t=threading.Thread(target=s.serve_forever);t.start()
        try:
            self.chat()
            c=http.client.HTTPConnection('127.0.0.1',s.server_port)
            c.request('POST','/bot'+self.b['token']+'/sendMessage',body=iter([b'chat_id=100&',b'text=hello']),headers={'Content-Type':'application/x-www-form-urlencoded'},encode_chunked=True)
            r=c.getresponse();self.assertEqual(r.status,200);self.assertTrue(json.loads(r.read())['ok']);c.close()
        finally:s.shutdown();s.server_close();t.join()
    def test_webhook_real_http_delivery_and_retry(self):
        seen=[]
        class H(BaseHTTPRequestHandler):
            def log_message(self,*a):pass
            def do_POST(self):
                seen.append((self.path,self.headers.get('X-Telegram-Bot-Api-Secret-Token'),json.loads(self.rfile.read(int(self.headers['Content-Length'])))))
                self.send_response(503 if len(seen)==1 else 200);self.end_headers()
        s=ThreadingHTTPServer(('127.0.0.1',0),H);t=threading.Thread(target=s.serve_forever);t.start()
        try:
            self.lab.origin='http://127.0.0.1:'+str(s.server_port)
            self.call('setWebhook',url=self.lab.origin+'/v1/telegram/cha_test',secret_token='secret')
            u=self.chat();self.lab.deliver_once();self.assertEqual(self.lab.snapshot()['updates'][0]['ack'],0)
            with self.lab.cv:self.lab.db.execute('UPDATE updates SET retry=0');self.lab.db.commit()
            self.lab.deliver_once();self.assertEqual(self.lab.snapshot()['updates'][0]['ack'],1)
            self.assertEqual(seen[0],('/v1/telegram/cha_test','secret',u));self.assertEqual(len(seen),2)
        finally:s.shutdown();s.server_close();t.join()

if __name__=='__main__':unittest.main()
