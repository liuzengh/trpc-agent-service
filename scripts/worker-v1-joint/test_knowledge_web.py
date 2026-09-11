"""Pure marker checks; actual import and SDK evidence are owned by the GUI runner."""
import copy
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('knowledge_web_runner',Path(__file__).resolve().parents[1]/'test-worker-memory-web.py')
web=importlib.util.module_from_spec(spec);spec.loader.exec_module(web)
class KnowledgeBrowserGuards(unittest.TestCase):
 def test_exact_import_identity_count_and_text_hash(self):
  publication={'deployment_id':'d','revision_number':2,'manifest_id':'m'}
  marker=dict(result='PASS',http_status=200,**publication,resource='docs',name=web.KNOWLEDGE_GUI_NAME,text_sha256=web.hashlib.sha256(web.KNOWLEDGE_GUI_TEXT.encode()).hexdigest(),documents=1)
  web.verify_knowledge_browser_import(marker,publication)
  for field,value in [('result','FAIL'),('http_status',503),('deployment_id','other'),('revision_number',1),('manifest_id','other'),('resource','other'),('name','other.txt'),('text_sha256','0'*64),('documents',0),('documents',True)]:
   with self.subTest(field=field,value=value),self.assertRaises(AssertionError):web.verify_knowledge_browser_import(dict(marker,**{field:value}),publication)
 def test_missing_real_receipt_is_not_accepted(self):
  publication={'deployment_id':'d','revision_number':2,'manifest_id':'m'}
  with self.assertRaises(KeyError):web.verify_knowledge_browser_import({'result':'PASS'},publication)
if __name__=='__main__':unittest.main()
