import hashlib
import json
import pathlib
import sys
import tempfile
import threading
import unittest
import urllib.request
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from app import Lab,Error,server

class DocumentsTest(unittest.TestCase):
    def test_actual_multipart_binary_receipt_and_download(self):
        with tempfile.TemporaryDirectory() as tmp:
            lab=Lab(str(pathlib.Path(tmp)/'lab.db'),'http://127.0.0.1:9999')
            bot=lab.create('documents'); update=lab.chat(bot['id'],100,100,'make a file')
            s=server(lab,('127.0.0.1',0));thread=threading.Thread(target=s.serve_forever);thread.start()
            try:
                data=b'\x00\xffbinary\r\nactual file\n'; boundary='document-boundary'
                fields={'chat_id':'100','message_thread_id':'7','reply_parameters':json.dumps({'message_id':update['message']['message_id']})}
                raw=b''.join(('--'+boundary+'\r\nContent-Disposition: form-data; name="'+k+'"\r\n\r\n'+v+'\r\n').encode() for k,v in fields.items())
                raw+=('--'+boundary+'\r\nContent-Disposition: form-data; name="document"; filename="report.bin"\r\nContent-Type: application/octet-stream\r\n\r\n').encode()+data+('\r\n--'+boundary+'--\r\n').encode()
                base='http://127.0.0.1:'+str(s.server_port)
                req=urllib.request.Request(base+'/bot'+bot['token']+'/sendDocument',data=raw,headers={'Content-Type':'multipart/form-data; boundary='+boundary})
                with urllib.request.urlopen(req) as response: result=json.load(response)['result']
                doc=result['document'];self.assertEqual(doc['file_size'],len(data));self.assertEqual(doc['sha256'],hashlib.sha256(data).hexdigest());self.assertEqual(result['reply_to_message']['message_id'],update['message']['message_id']);self.assertEqual(result['message_thread_id'],7)
                with urllib.request.urlopen(base+'/lab/documents/'+doc['file_id']) as response:
                    self.assertEqual(response.read(),data);self.assertEqual(response.headers['X-Content-SHA256'],doc['sha256'])
                row=lab.db.execute('SELECT body FROM documents WHERE file_id=?',(doc['file_id'],)).fetchone();self.assertEqual(row['body'],data)
                with self.assertRaises(Error):lab.api(bot['token'],'sendDocument',{'chat_id':'100','document':'https://example.invalid/file'})
                count=lab.db.execute("SELECT count(*) FROM messages WHERE direction='out'").fetchone()[0];self.assertEqual(count,1)
            finally:s.shutdown();s.server_close();thread.join();lab.close();lab.db.close()
