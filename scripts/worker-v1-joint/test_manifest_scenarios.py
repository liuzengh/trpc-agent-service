"""Pure owner-export transcript checks; actual transport runs separately."""
import unittest
from manifest_scenarios import validate_page, assert_same_incarnation


class ManifestTranscriptTests(unittest.TestCase):
    def test_empty_export_requires_explicit_complete_and_null_upper(self):
        page={'schema_version':'v1','snapshot_upper':None,'events':[],'next_cursor':'','complete':True}
        self.assertIsNone(validate_page(page))
        for key,value in [('complete',False),('next_cursor','cursor'),('events',[{}])]:
            bad=dict(page);bad[key]=value
            with self.assertRaises(AssertionError):validate_page(bad)
    def test_nonempty_export_upper_stays_fixed_and_cursor_is_required(self):
        upper={'created_at':'2026-09-07T00:00:00Z','tenant_id':'tenant','event_id':'event'}
        page={'schema_version':'v1','snapshot_upper':upper,'events':[{}],'next_cursor':'cursor','complete':False}
        self.assertEqual(validate_page(page),upper)
        with self.assertRaises(AssertionError):validate_page(dict(page,next_cursor=''))
        with self.assertRaises(AssertionError):validate_page(dict(page,events=[]))
        with self.assertRaises(AssertionError):validate_page(page,dict(upper,event_id='other'),first=False)
    def test_recovery_keeps_stream_and_durable_incarnations(self):
        first={'stream_created':'A','durable_created':'B'}
        assert_same_incarnation(first,dict(first))
        with self.assertRaises(AssertionError):assert_same_incarnation(first,dict(first,durable_created='C'))

if __name__=='__main__':unittest.main()
